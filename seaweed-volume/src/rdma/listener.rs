use std::sync::Arc;

use seaweed_rdma::{
    RdmaOpenError, RdmaReadError, RdmaReadHandle, RdmaReadRequest, RdmaReadResponse,
    RdmaReadableSource,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Semaphore};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::rdma::parse_fid::parse_fid;

const REQUEST_WIRE_SIZE: usize = 64;
const MAX_READ_BYTES: usize = 64 * 1024 * 1024;

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

pub struct Listener<S: RdmaReadableSource + Send + Sync + 'static> {
    source: Arc<S>,
    config: ListenerConfig,
    inflight: Arc<Semaphore>,
}

impl<S: RdmaReadableSource + Send + Sync + 'static> Listener<S> {
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

pub struct BoundListener<S: RdmaReadableSource + Send + Sync + 'static> {
    tcp: TcpListener,
    source: Arc<S>,
    inflight: Arc<Semaphore>,
    pub bound: std::net::SocketAddr,
}

impl<S: RdmaReadableSource + Send + Sync + 'static> BoundListener<S> {
    pub async fn serve(self) {
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        self.serve_until(shutdown_rx).await;
        drop(shutdown_tx);
    }

    pub async fn serve_until(self, mut shutdown: broadcast::Receiver<()>) {
        let (connection_shutdown_tx, _) = broadcast::channel(1);
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("RDMA TCP listener shutting down");
                    let _ = connection_shutdown_tx.send(());
                    break;
                }
                accepted = self.tcp.accept() => match accepted {
                    Ok((stream, peer)) => {
                        let source = self.source.clone();
                        let inflight = self.inflight.clone();
                        let connection_shutdown = connection_shutdown_tx.subscribe();
                        connections.spawn(async move {
                            if let Err(e) = handle_connection(stream, source, inflight, connection_shutdown).await {
                                warn!(error = %e, peer = %peer, "RDMA TCP session ended");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "RDMA TCP accept failed");
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                },
                joined = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(e)) = joined {
                        warn!(error = %e, "RDMA TCP session task failed");
                    }
                }
            }
        }

        while connections.join_next().await.is_some() {}
    }
}

async fn handle_connection<S: RdmaReadableSource + Send + Sync + 'static>(
    mut stream: TcpStream,
    source: Arc<S>,
    inflight: Arc<Semaphore>,
    mut shutdown: broadcast::Receiver<()>,
) -> std::io::Result<()> {
    let mut req_buf = [0u8; REQUEST_WIRE_SIZE];
    loop {
        tokio::select! {
            _ = shutdown.recv() => return Ok(()),
            read = stream.read_exact(&mut req_buf) => match read {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e),
            }
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
                tokio::select! {
                    _ = shutdown.recv() => return Ok(()),
                    written = stream.write_all(resp.as_bytes()) => written?,
                }
                continue;
            }
        };

        let result = process_request(source.clone(), req);
        tokio::select! {
            _ = shutdown.recv() => return Ok(()),
            written = write_request_result(&mut stream, request_id, result) => written?,
        }
        drop(permit);
    }
}
fn process_request<S: RdmaReadableSource + Send + Sync + 'static>(
    source: Arc<S>,
    req: RdmaReadRequest,
) -> Result<Vec<u8>, ReadFailure> {
    let fid = req.fid_str();
    let (_vid, _needle_id, expected_cookie) = parse_fid(fid).map_err(|_| ReadFailure::NotFound)?;
    let handle = source.open(fid).map_err(ReadFailure::Open)?;

    if handle.cookie() != expected_cookie {
        return Err(ReadFailure::CookieMismatch {
            expected: expected_cookie,
            actual: handle.cookie(),
        });
    }

    read_payload(handle, req.offset, req.length)
}

fn read_payload(
    mut handle: Box<dyn RdmaReadHandle>,
    offset: u64,
    length: u64,
) -> Result<Vec<u8>, ReadFailure> {
    handle
        .read_at(offset, length, MAX_READ_BYTES)
        .map_err(ReadFailure::Read)
}

async fn write_request_result(
    stream: &mut TcpStream,
    request_id: u64,
    result: Result<Vec<u8>, ReadFailure>,
) -> std::io::Result<()> {
    match result {
        Ok(data) => {
            stream.write_all(&data).await?;
            let resp = RdmaReadResponse::ok(request_id, data.len() as u32);
            stream.write_all(resp.as_bytes()).await?;
            debug!(request_id, bytes = data.len(), "served RDMA TCP read");
        }
        Err(ReadFailure::NotFound) | Err(ReadFailure::Open(RdmaOpenError::NotFound)) => {
            let resp = RdmaReadResponse::not_found(request_id);
            stream.write_all(resp.as_bytes()).await?;
        }
        Err(ReadFailure::CookieMismatch { expected, actual })
        | Err(ReadFailure::Read(RdmaReadError::CookieMismatch { expected, actual })) => {
            warn!(
                request_id,
                expected = format_args!("{:08x}", expected),
                actual = format_args!("{:08x}", actual),
                "RDMA read cookie mismatch"
            );
            let resp = RdmaReadResponse::cookie_mismatch(request_id);
            stream.write_all(resp.as_bytes()).await?;
        }
        Err(ReadFailure::Read(RdmaReadError::RangeInvalid {
            len,
            offset,
            requested,
        })) => {
            warn!(
                request_id,
                len, offset, requested, "RDMA read range is outside needle payload"
            );
            let resp = RdmaReadResponse::range_invalid(request_id);
            stream.write_all(resp.as_bytes()).await?;
        }
        Err(ReadFailure::Read(RdmaReadError::NotFound)) => {
            let resp = RdmaReadResponse::not_found(request_id);
            stream.write_all(resp.as_bytes()).await?;
        }
        Err(err) => {
            warn!(request_id, error = %err, "RDMA read failed");
            let resp = RdmaReadResponse::error(request_id);
            stream.write_all(resp.as_bytes()).await?;
        }
    }
    Ok(())
}

#[derive(Debug)]
enum ReadFailure {
    NotFound,
    Open(RdmaOpenError),
    Read(RdmaReadError),
    CookieMismatch { expected: u32, actual: u32 },
}

impl std::fmt::Display for ReadFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadFailure::NotFound => write!(f, "not found"),
            ReadFailure::Open(e) => write!(f, "open failed: {e}"),
            ReadFailure::Read(e) => write!(f, "read failed: {e}"),
            ReadFailure::CookieMismatch { expected, actual } => {
                write!(
                    f,
                    "cookie mismatch expected={expected:#x} actual={actual:#x}"
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use seaweed_rdma::{
        RdmaReadRequest, RdmaReadResponse, RDMA_STATUS_COOKIE_MISMATCH, RDMA_STATUS_ERROR,
        RDMA_STATUS_NOT_FOUND, RDMA_STATUS_OK, RDMA_STATUS_RANGE_INVALID,
    };
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::broadcast;

    use super::{Listener, ListenerConfig};
    use crate::config::MinFreeSpace;
    use crate::rdma::needle_source::StoreNeedleSource;
    use crate::storage::needle::needle::Needle;
    use crate::storage::needle_map::NeedleMapKind;
    use crate::storage::store::Store;
    use crate::storage::types::{Cookie, DiskType, NeedleId, Version, VolumeId, VERSION_1};

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
        let source = Arc::new(StoreNeedleSource::from_store_for_tests(store));
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn listener_shutdown_aborts_open_connection_tasks() {
        let (_tmp, store, _fid) = make_store_with_needle(b"abcdefgh", 0x89b26a98);
        let source = Arc::new(StoreNeedleSource::from_store_for_tests(store));
        let listener = Listener::new(
            source,
            ListenerConfig {
                addr: "127.0.0.1:0".to_string(),
                max_inflight: 8,
            },
        );
        let bound = listener.bind().await.unwrap();
        let addr = bound.bound;
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let serve_task = tokio::spawn(bound.serve_until(shutdown_rx));

        let _stream = TcpStream::connect(addr).await.unwrap();
        shutdown_tx.send(()).unwrap();

        tokio::time::timeout(Duration::from_secs(2), serve_task)
            .await
            .unwrap()
            .unwrap();
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

        assert_eq!(resp.status, RDMA_STATUS_ERROR);
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
