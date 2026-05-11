use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use bytes::BytesMut;
use seaweed_rdma::{NeedleSource, RdmaReadRequest, RdmaReadResponse};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

use crate::rdma::parse_fid::parse_fid;
use crate::storage::types::{DATA_SIZE_SIZE, NEEDLE_HEADER_SIZE};

const REQUEST_WIRE_SIZE: usize = 64;
const MAX_PREAD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct ListenerConfig {
    pub addr: String,
    pub max_inflight: usize,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:0".to_string(),
            max_inflight: 256,
        }
    }
}

pub struct Listener<S: NeedleSource + Send + Sync + 'static> {
    source: Arc<S>,
    config: ListenerConfig,
    inflight: Arc<Semaphore>,
}

impl<S: NeedleSource + Send + Sync + 'static> Listener<S> {
    pub fn new(source: Arc<S>, config: ListenerConfig) -> Self {
        let inflight = Arc::new(Semaphore::new(config.max_inflight));
        Self {
            source,
            config,
            inflight,
        }
    }

    pub async fn bind(self) -> std::io::Result<BoundListener<S>> {
        let tcp = TcpListener::bind(&self.config.addr).await?;
        let bound = tcp.local_addr()?;
        info!(addr = %bound, "seaweed-volume RDMA TCP listener bound");
        Ok(BoundListener {
            tcp,
            source: self.source,
            inflight: self.inflight,
            bound,
        })
    }
}

pub struct BoundListener<S: NeedleSource + Send + Sync + 'static> {
    tcp: TcpListener,
    source: Arc<S>,
    inflight: Arc<Semaphore>,
    pub bound: std::net::SocketAddr,
}

impl<S: NeedleSource + Send + Sync + 'static> BoundListener<S> {
    pub async fn serve(self) {
        loop {
            match self.tcp.accept().await {
                Ok((stream, peer)) => {
                    let source = self.source.clone();
                    let inflight = self.inflight.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, source, inflight).await {
                            warn!(error = %e, peer = %peer, "RDMA TCP session ended");
                        }
                    });
                }
                Err(e) => {
                    error!(error = %e, "RDMA TCP accept failed");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
    }
}

async fn handle_connection<S: NeedleSource + Send + Sync + 'static>(
    mut stream: TcpStream,
    source: Arc<S>,
    inflight: Arc<Semaphore>,
) -> std::io::Result<()> {
    let mut req_buf = [0u8; REQUEST_WIRE_SIZE];
    loop {
        match stream.read_exact(&mut req_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        }

        let req = match RdmaReadRequest::from_bytes(&req_buf) {
            Some(req) => req,
            None => return Ok(()),
        };
        let request_id = req.request_id;
        let permit = match inflight.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                let resp = RdmaReadResponse::busy(request_id);
                stream.write_all(resp.as_bytes()).await?;
                continue;
            }
        };

        let fid = req.fid_str();
        match source.locate(fid) {
            Some(loc) => {
                let dat_path = match loc.dat_path.as_deref() {
                    Some(dat_path) => dat_path.to_string(),
                    None => {
                        let resp = RdmaReadResponse::error(request_id);
                        stream.write_all(resp.as_bytes()).await?;
                        drop(permit);
                        continue;
                    }
                };
                let data_offset = loc.offset;
                let data_size = loc.length;
                let chunk_offset = req.offset;
                let want_length = req.length;
                let expected = match parse_fid(fid).ok() {
                    Some((_, needle_id, cookie)) => (needle_id, cookie),
                    None => {
                        let resp = RdmaReadResponse::not_found(request_id);
                        stream.write_all(resp.as_bytes()).await?;
                        drop(permit);
                        continue;
                    }
                };
                let read_result = tokio::task::spawn_blocking(move || {
                    pread_data_with_cookie(
                        &dat_path,
                        data_offset,
                        data_size,
                        chunk_offset,
                        want_length,
                        expected.0,
                        expected.1,
                    )
                })
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

                match read_result {
                    Ok(PreadOutcome { data }) => {
                        stream.write_all(&data).await?;
                        let resp = RdmaReadResponse::ok(request_id, data.len() as u32);
                        stream.write_all(resp.as_bytes()).await?;
                        debug!(request_id, bytes = data.len(), "served RDMA TCP read");
                    }
                    Err(PreadError::CookieMismatch { expected, actual }) => {
                        warn!(
                            request_id,
                            expected = format_args!("{:08x}", expected),
                            actual = format_args!("{:08x}", actual),
                            "RDMA read cookie mismatch"
                        );
                        let resp = RdmaReadResponse::cookie_mismatch(request_id);
                        stream.write_all(resp.as_bytes()).await?;
                    }
                    Err(PreadError::RangeInvalid {
                        data_size,
                        requested_offset,
                        requested_length,
                    }) => {
                        warn!(
                            request_id,
                            data_size,
                            requested_offset,
                            requested_length,
                            "RDMA read range is outside needle payload"
                        );
                        let resp = RdmaReadResponse::range_invalid(request_id);
                        stream.write_all(resp.as_bytes()).await?;
                    }
                    Err(PreadError::Io(e)) => {
                        warn!(request_id, error = %e, "pread failed");
                        let resp = RdmaReadResponse::error(request_id);
                        stream.write_all(resp.as_bytes()).await?;
                    }
                }
            }
            None => {
                let resp = RdmaReadResponse::not_found(request_id);
                stream.write_all(resp.as_bytes()).await?;
            }
        }
        drop(permit);
    }
}

struct PreadOutcome {
    data: Vec<u8>,
}

#[derive(Debug)]
enum PreadError {
    Io(std::io::Error),
    CookieMismatch {
        expected: u32,
        actual: u32,
    },
    RangeInvalid {
        data_size: u64,
        requested_offset: u64,
        requested_length: u64,
    },
}

fn pread_data_with_cookie(
    dat_path: &str,
    data_offset: u64,
    data_size: u64,
    chunk_offset: u64,
    want_length: u64,
    expected_needle_id: u64,
    expected_cookie: u32,
) -> Result<PreadOutcome, PreadError> {
    let file = File::open(Path::new(dat_path)).map_err(PreadError::Io)?;
    let on_disk_cookie = read_cookie_for_payload(&file, data_offset, expected_needle_id)?;

    if data_size == 0 {
        return Err(PreadError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "needle has zero data_size",
        )));
    }
    if on_disk_cookie != expected_cookie {
        return Err(PreadError::CookieMismatch {
            expected: expected_cookie,
            actual: on_disk_cookie,
        });
    }

    if want_length == 0 {
        if chunk_offset >= data_size {
            return Err(PreadError::RangeInvalid {
                data_size,
                requested_offset: chunk_offset,
                requested_length: 0,
            });
        }
    } else if chunk_offset
        .checked_add(want_length)
        .map(|end| end > data_size)
        .unwrap_or(true)
    {
        return Err(PreadError::RangeInvalid {
            data_size,
            requested_offset: chunk_offset,
            requested_length: want_length,
        });
    }

    let available = data_size - chunk_offset;
    let read_len_u64 = if want_length == 0 {
        available
    } else {
        want_length
    };
    if read_len_u64 > MAX_PREAD_BYTES as u64 {
        return Err(PreadError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "requested read exceeds MAX_PREAD_BYTES",
        )));
    }

    let mut buf = BytesMut::zeroed(read_len_u64 as usize);
    read_exact_at(&file, &mut buf, data_offset + chunk_offset).map_err(PreadError::Io)?;
    Ok(PreadOutcome { data: buf.to_vec() })
}

fn read_cookie_for_payload(
    file: &File,
    data_offset: u64,
    expected_needle_id: u64,
) -> Result<u32, PreadError> {
    let candidates = [
        data_offset.checked_sub((NEEDLE_HEADER_SIZE + DATA_SIZE_SIZE) as u64),
        data_offset.checked_sub(NEEDLE_HEADER_SIZE as u64),
    ];
    for candidate in candidates.into_iter().flatten() {
        let mut header = [0u8; NEEDLE_HEADER_SIZE];
        if read_exact_at(file, &mut header, candidate).is_err() {
            continue;
        }
        let needle_id = u64::from_be_bytes([
            header[4], header[5], header[6], header[7], header[8], header[9], header[10],
            header[11],
        ]);
        if needle_id == expected_needle_id {
            return Ok(u32::from_be_bytes([
                header[0], header[1], header[2], header[3],
            ]));
        }
    }
    Err(PreadError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "could not find matching needle header before payload",
    )))
}

#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut filled = 0;
    while filled < buf.len() {
        let n = file.seek_read(&mut buf[filled..], offset)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected EOF in seek_read",
            ));
        }
        filled += n;
        offset += n as u64;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use seaweed_rdma::{
        NeedleLocation, NeedleSource, RdmaReadRequest, RdmaReadResponse,
        RDMA_STATUS_COOKIE_MISMATCH, RDMA_STATUS_NOT_FOUND, RDMA_STATUS_OK,
        RDMA_STATUS_RANGE_INVALID,
    };
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    use super::{Listener, ListenerConfig};
    use crate::config::MinFreeSpace;
    use crate::rdma::needle_source::locate_in_store;
    use crate::storage::needle::needle::Needle;
    use crate::storage::needle_map::NeedleMapKind;
    use crate::storage::store::Store;
    use crate::storage::types::{Cookie, DiskType, NeedleId, Version, VolumeId, VERSION_1};

    struct StoreBackedSource {
        store: Arc<RwLock<Store>>,
    }

    impl NeedleSource for StoreBackedSource {
        fn locate(&self, id: &str) -> Option<NeedleLocation> {
            let store = self.store.read().ok()?;
            locate_in_store(&store, id)
        }
    }

    fn make_store_with_needle(data: &[u8], cookie: u32) -> (TempDir, Arc<RwLock<Store>>, String) {
        make_store_with_needle_version(data, cookie, Version::current())
    }

    fn make_store_with_needle_version(
        data: &[u8],
        cookie: u32,
        version: Version,
    ) -> (TempDir, Arc<RwLock<Store>>, String) {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir,
                dir,
                10,
                DiskType::HardDrive,
                MinFreeSpace::Percent(0.0),
                Vec::new(),
            )
            .unwrap();
        store
            .add_volume(VolumeId(3), "", None, None, 0, DiskType::HardDrive, version)
            .unwrap();

        let mut needle = Needle {
            id: NeedleId(1),
            cookie: Cookie(cookie),
            data: data.to_vec(),
            data_size: data.len() as u32,
            ..Needle::default()
        };
        store.write_volume_needle(VolumeId(3), &mut needle).unwrap();

        let fid = format!("3,{:02x}{:08x}", 1u64, cookie);
        (tmp, Arc::new(RwLock::new(store)), fid)
    }

    async fn spawn_listener(store: Arc<RwLock<Store>>) -> SocketAddr {
        let source = Arc::new(StoreBackedSource { store });
        let listener = Listener::new(
            source,
            ListenerConfig {
                addr: "127.0.0.1:0".to_string(),
                max_inflight: 8,
            },
        );
        let bound = listener.bind().await.unwrap();
        let addr = bound.bound;
        tokio::spawn(bound.serve());
        addr
    }

    async fn send_request(
        addr: SocketAddr,
        req: RdmaReadRequest,
        data_len: usize,
    ) -> (Vec<u8>, RdmaReadResponse) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(req.as_bytes()).await.unwrap();

        let mut data = vec![0u8; data_len];
        if data_len > 0 {
            tokio::time::timeout(Duration::from_secs(2), stream.read_exact(&mut data))
                .await
                .unwrap()
                .unwrap();
        }
        let mut resp_buf = [0u8; 32];
        tokio::time::timeout(Duration::from_secs(2), stream.read_exact(&mut resp_buf))
            .await
            .unwrap()
            .unwrap();
        let resp = RdmaReadResponse::from_bytes(&resp_buf).unwrap();
        (data, resp)
    }

    async fn send_error_request(addr: SocketAddr, req: RdmaReadRequest) -> RdmaReadResponse {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(req.as_bytes()).await.unwrap();

        let mut resp_buf = [0u8; 32];
        tokio::time::timeout(Duration::from_secs(2), stream.read_exact(&mut resp_buf))
            .await
            .unwrap()
            .unwrap();
        RdmaReadResponse::from_bytes(&resp_buf).unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn listener_serves_full_needle_from_real_store() {
        let (_tmp, store, fid) = make_store_with_needle(b"abcdefgh", 0x89b26a98);
        let addr = spawn_listener(store).await;

        let (data, resp) = send_request(addr, RdmaReadRequest::new(7, &fid, 0, 0), 8).await;

        assert_eq!(data, b"abcdefgh");
        assert_eq!(resp.request_id, 7);
        assert_eq!(resp.status, RDMA_STATUS_OK);
        assert_eq!(resp.bytes_transferred, 8);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn listener_honors_request_offset_and_length() {
        let (_tmp, store, fid) = make_store_with_needle(b"abcdefgh", 0x89b26a98);
        let addr = spawn_listener(store).await;

        let (data, resp) = send_request(addr, RdmaReadRequest::new(8, &fid, 3, 4), 4).await;

        assert_eq!(data, b"defg");
        assert_eq!(resp.status, RDMA_STATUS_OK);
        assert_eq!(resp.bytes_transferred, 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn listener_length_zero_returns_remainder_from_offset() {
        let (_tmp, store, fid) = make_store_with_needle(b"abcdefgh", 0x89b26a98);
        let addr = spawn_listener(store).await;

        let (data, resp) = send_request(addr, RdmaReadRequest::new(9, &fid, 5, 0), 3).await;

        assert_eq!(data, b"fgh");
        assert_eq!(resp.status, RDMA_STATUS_OK);
        assert_eq!(resp.bytes_transferred, 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn listener_returns_cookie_mismatch_on_stale_fid() {
        let (_tmp, store, _fid) = make_store_with_needle(b"abcdefgh", 0x89b26a98);
        let addr = spawn_listener(store).await;

        let resp = send_error_request(addr, RdmaReadRequest::new(10, "3,01cafebabe", 0, 8)).await;

        assert_eq!(resp.request_id, 10);
        assert_eq!(resp.status, RDMA_STATUS_COOKIE_MISMATCH);
        assert_eq!(resp.bytes_transferred, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn listener_checks_cookie_before_range_validation() {
        let (_tmp, store, _fid) = make_store_with_needle(b"abcdefgh", 0x89b26a98);
        let addr = spawn_listener(store).await;

        let resp = send_error_request(addr, RdmaReadRequest::new(13, "3,01cafebabe", 6, 4)).await;

        assert_eq!(resp.request_id, 13);
        assert_eq!(resp.status, RDMA_STATUS_COOKIE_MISMATCH);
        assert_eq!(resp.bytes_transferred, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn listener_returns_range_invalid_when_request_overshoots_data() {
        let (_tmp, store, fid) = make_store_with_needle(b"abcdefgh", 0x89b26a98);
        let addr = spawn_listener(store).await;

        let resp = send_error_request(addr, RdmaReadRequest::new(11, &fid, 6, 4)).await;

        assert_eq!(resp.request_id, 11);
        assert_eq!(resp.status, RDMA_STATUS_RANGE_INVALID);
        assert_eq!(resp.bytes_transferred, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn listener_returns_range_invalid_for_length_zero_at_eof() {
        let (_tmp, store, fid) = make_store_with_needle(b"abcdefgh", 0x89b26a98);
        let addr = spawn_listener(store).await;

        let resp = send_error_request(addr, RdmaReadRequest::new(14, &fid, 8, 0)).await;

        assert_eq!(resp.request_id, 14);
        assert_eq!(resp.status, RDMA_STATUS_RANGE_INVALID);
        assert_eq!(resp.bytes_transferred, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn listener_rejects_version_one_volume_until_storage_semantics_are_settled() {
        let (_tmp, store, fid) = make_store_with_needle_version(b"abcdefgh", 0x89b26a98, VERSION_1);
        let addr = spawn_listener(store).await;

        let resp = send_error_request(addr, RdmaReadRequest::new(15, &fid, 2, 3)).await;

        assert_eq!(resp.status, RDMA_STATUS_NOT_FOUND);
        assert_eq!(resp.bytes_transferred, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn listener_returns_not_found_for_missing_needle() {
        let (_tmp, store, _fid) = make_store_with_needle(b"abcdefgh", 0x89b26a98);
        let addr = spawn_listener(store).await;

        let resp = send_error_request(addr, RdmaReadRequest::new(12, "3,0289b26a98", 0, 8)).await;

        assert_eq!(resp.request_id, 12);
        assert_eq!(resp.status, RDMA_STATUS_NOT_FOUND);
        assert_eq!(resp.bytes_transferred, 0);
    }
}
