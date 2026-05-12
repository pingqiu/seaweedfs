//! Adapter from the Rust volume server storage state to the SRA substrate.

use std::sync::Arc;

use seaweed_rdma::{
    NeedleLocation, NeedleSource, RdmaOpenError, RdmaReadError, RdmaReadHandle, RdmaReadableSource,
    ReadSegment,
};

use crate::rdma::parse_fid::parse_fid;
use crate::server::volume_server::VolumeServerState;
use crate::storage::needle::needle::Needle;
use crate::storage::store::Store;
use crate::storage::types::{NeedleId, VolumeId, VERSION_1};
use crate::storage::volume::{NeedleStreamInfo, NeedleStreamSource, VolumeError};

#[derive(Clone)]
pub struct StoreNeedleSource {
    access: StoreAccess,
}

#[derive(Clone)]
enum StoreAccess {
    VolumeServer(Arc<VolumeServerState>),
    #[cfg(test)]
    Direct(Arc<std::sync::RwLock<Store>>),
}

impl StoreNeedleSource {
    pub fn new(state: Arc<VolumeServerState>) -> Self {
        Self {
            access: StoreAccess::VolumeServer(state),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_store_for_tests(store: Arc<std::sync::RwLock<Store>>) -> Self {
        Self {
            access: StoreAccess::Direct(store),
        }
    }
}

impl NeedleSource for StoreNeedleSource {
    fn locate(&self, id: &str) -> Option<NeedleLocation> {
        self.access
            .with_read_option(|store| locate_in_store(store, id))
    }
}

impl RdmaReadableSource for StoreNeedleSource {
    fn open(&self, id: &str) -> Result<Box<dyn RdmaReadHandle>, RdmaOpenError> {
        let (vid, needle_id, _cookie) = parse_fid(id).map_err(|_| RdmaOpenError::NotFound)?;
        let volume_id = VolumeId(vid);
        let needle_id = NeedleId(needle_id);
        let access = self.access.clone();
        let handle = access.with_read_open(|store| {
            RustVolumeReadHandle::open_from_store(access.clone(), store, volume_id, needle_id)
        })?;
        Ok(Box::new(handle))
    }
}

impl StoreAccess {
    fn with_read_option<T>(&self, f: impl FnOnce(&Store) -> Option<T>) -> Option<T> {
        match self {
            StoreAccess::VolumeServer(state) => {
                let store = state.store.read().ok()?;
                f(&store)
            }
            #[cfg(test)]
            StoreAccess::Direct(store) => {
                let store = store.read().ok()?;
                f(&store)
            }
        }
    }

    fn with_read_open<T>(
        &self,
        f: impl FnOnce(&Store) -> Result<T, RdmaOpenError>,
    ) -> Result<T, RdmaOpenError> {
        match self {
            StoreAccess::VolumeServer(state) => {
                let store = state
                    .store
                    .read()
                    .map_err(|_| RdmaOpenError::Other("store read lock poisoned".to_string()))?;
                f(&store)
            }
            #[cfg(test)]
            StoreAccess::Direct(store) => {
                let store = store
                    .read()
                    .map_err(|_| RdmaOpenError::Other("store read lock poisoned".to_string()))?;
                f(&store)
            }
        }
    }

    fn with_read<T>(
        &self,
        f: impl FnOnce(&Store) -> Result<T, RdmaReadError>,
    ) -> Result<T, RdmaReadError> {
        match self {
            StoreAccess::VolumeServer(state) => {
                let store = state
                    .store
                    .read()
                    .map_err(|_| RdmaReadError::Other("store read lock poisoned".to_string()))?;
                f(&store)
            }
            #[cfg(test)]
            StoreAccess::Direct(store) => {
                let store = store
                    .read()
                    .map_err(|_| RdmaReadError::Other("store read lock poisoned".to_string()))?;
                f(&store)
            }
        }
    }
}

struct RustVolumeReadHandle {
    access: StoreAccess,
    source: Option<NeedleStreamSource>,
    volume_id: VolumeId,
    needle_id: NeedleId,
    data_file_offset: u64,
    data_size: u64,
    cookie: u32,
    compaction_revision: u16,
}

impl RustVolumeReadHandle {
    fn open_from_store(
        access: StoreAccess,
        store: &Store,
        volume_id: VolumeId,
        needle_id: NeedleId,
    ) -> Result<Self, RdmaOpenError> {
        let mut handle = Self {
            access,
            source: None,
            volume_id,
            needle_id,
            data_file_offset: 0,
            data_size: 0,
            cookie: 0,
            compaction_revision: 0,
        };
        handle
            .refresh_from_store(store)
            .map_err(read_to_open_error)?;
        Ok(handle)
    }

    fn refresh_from_store(&mut self, store: &Store) -> Result<(), RdmaReadError> {
        let (_, volume) = store
            .find_volume(self.volume_id)
            .ok_or(RdmaReadError::NotFound)?;
        if volume.version() == VERSION_1 {
            return Err(RdmaReadError::Unsupported(
                "VERSION_1 volumes are not supported by RDMA read handle".to_string(),
            ));
        }

        let mut needle = Needle {
            id: self.needle_id,
            ..Needle::default()
        };
        let info = store
            .read_volume_needle_stream_info(self.volume_id, &mut needle, false)
            .map_err(read_error_from_volume)?;
        self.apply_stream_info(info, needle.cookie.0);
        Ok(())
    }

    fn apply_stream_info(&mut self, info: NeedleStreamInfo, cookie: u32) {
        self.source = Some(info.source);
        self.volume_id = info.volume_id;
        self.needle_id = info.needle_id;
        self.data_file_offset = info.data_file_offset;
        self.data_size = info.data_size as u64;
        self.cookie = cookie;
        self.compaction_revision = info.compaction_revision;
    }

    fn refresh_for_read(
        &mut self,
        store: &Store,
        expected_cookie: u32,
    ) -> Result<crate::storage::volume::DataFileReadLease, RdmaReadError> {
        let (_, volume) = store
            .find_volume(self.volume_id)
            .ok_or(RdmaReadError::NotFound)?;
        if volume.version() == VERSION_1 {
            return Err(RdmaReadError::Unsupported(
                "VERSION_1 volumes are not supported by RDMA read handle".to_string(),
            ));
        }

        let mut needle = Needle {
            id: self.needle_id,
            ..Needle::default()
        };
        let (info, lease) = store
            .read_volume_needle_stream_info_with_lease(self.volume_id, &mut needle, false)
            .map_err(read_error_from_volume)?;
        if needle.cookie.0 != expected_cookie {
            return Err(RdmaReadError::CookieMismatch {
                expected: expected_cookie,
                actual: needle.cookie.0,
            });
        }
        self.apply_stream_info(info, needle.cookie.0);
        Ok(lease)
    }

    fn read_with_current_source(&self, offset: u64, buf: &mut [u8]) -> Result<(), RdmaReadError> {
        let source = self
            .source
            .as_ref()
            .ok_or_else(|| RdmaReadError::Other("read handle has no stream source".to_string()))?;
        let physical_offset = self
            .data_file_offset
            .checked_add(offset)
            .ok_or_else(|| RdmaReadError::Other("physical offset overflow".to_string()))?;
        source
            .read_exact_at(buf, physical_offset)
            .map_err(RdmaReadError::Io)
    }
}

impl RdmaReadHandle for RustVolumeReadHandle {
    fn len(&self) -> u64 {
        self.data_size
    }

    fn cookie(&self) -> u32 {
        self.cookie
    }

    fn supports_direct_slot_reads(&self) -> bool {
        true
    }

    fn validate_read_range(&mut self, offset: u64, length: u64) -> Result<(), RdmaReadError> {
        let access = self.access.clone();
        access.with_read(|store| {
            // Refresh under the backend guard before slot allocation so range
            // and cookie failures keep their wire status precedence over BUSY.
            let _lease = self.refresh_for_read(store, self.cookie)?;
            validate_range(self.data_size, offset, length)
        })
    }

    fn read_exact_at_segments(
        &mut self,
        offset: u64,
        segments: &mut [ReadSegment],
    ) -> Result<(), RdmaReadError> {
        let requested = segments.iter().try_fold(0u64, |acc, segment| {
            acc.checked_add(segment.length as u64)
                .ok_or_else(|| RdmaReadError::Other("segment length overflow".to_string()))
        })?;
        let access = self.access.clone();
        access.with_read(|store| {
            // Hold one store read guard and one data-file read lease across the
            // entire logical request so multi-slot RDMA reads cannot mix
            // snapshots across compaction or same-cookie rewrites.
            let _lease = self.refresh_for_read(store, self.cookie)?;
            validate_range(self.data_size, offset, requested)?;
            for segment in segments {
                let segment_offset = offset
                    .checked_add(segment.request_offset)
                    .ok_or_else(|| RdmaReadError::Other("segment offset overflow".to_string()))?;
                let buf =
                    unsafe { std::slice::from_raw_parts_mut(segment.slot.ptr, segment.length) };
                self.read_with_current_source(segment_offset, buf)?;
            }
            Ok(())
        })
    }

    fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), RdmaReadError> {
        let requested = buf.len() as u64;
        let access = self.access.clone();
        access.with_read(|store| {
            // Lock order matches storage: store read guard first, then
            // data-file read lease. Holding both through the read blocks
            // compaction commit and writers without inverting the writer path.
            let _lease = self.refresh_for_read(store, self.cookie)?;
            validate_range(self.data_size, offset, requested)?;
            self.read_with_current_source(offset, buf)
        })
    }

    fn read_at(
        &mut self,
        offset: u64,
        length: u64,
        max_len: usize,
    ) -> Result<Vec<u8>, RdmaReadError> {
        let access = self.access.clone();
        access.with_read(|store| {
            // Refresh before resolving `length=0` so same-cookie overwrites
            // between open and read use the current payload length.
            let _lease = self.refresh_for_read(store, self.cookie)?;
            let read_len = resolve_read_len(self.data_size, offset, length)?;
            if read_len > max_len as u64 {
                return Err(RdmaReadError::Other(format!(
                    "requested read exceeds max_len: {read_len}"
                )));
            }
            let mut buf = vec![0u8; read_len as usize];
            self.read_with_current_source(offset, &mut buf)?;
            Ok(buf)
        })
    }
}

pub(crate) fn locate_in_store(store: &Store, id: &str) -> Option<NeedleLocation> {
    let (vid, needle_id, _cookie) = parse_fid(id).ok()?;
    let volume_id = VolumeId(vid);
    let mut needle = Needle {
        id: NeedleId(needle_id),
        ..Needle::default()
    };

    let info = store
        .read_volume_needle_stream_info(volume_id, &mut needle, false)
        .ok()?;
    let (_, volume) = store.find_volume(volume_id)?;
    if volume.version() == VERSION_1 {
        return None;
    }

    Some(NeedleLocation {
        volume_id: vid,
        offset: info.data_file_offset,
        length: info.data_size as u64,
        dat_path: Some(volume.file_name(".dat")),
    })
}

fn validate_range(len: u64, offset: u64, requested: u64) -> Result<(), RdmaReadError> {
    let _ = resolve_read_len(len, offset, requested)?;
    Ok(())
}

fn resolve_read_len(len: u64, offset: u64, requested: u64) -> Result<u64, RdmaReadError> {
    if len == 0 || offset >= len {
        return Err(RdmaReadError::RangeInvalid {
            len,
            offset,
            requested,
        });
    }
    if requested == 0 {
        return Ok(len - offset);
    }
    if offset
        .checked_add(requested)
        .map(|end| end <= len)
        .unwrap_or(false)
    {
        Ok(requested)
    } else {
        Err(RdmaReadError::RangeInvalid {
            len,
            offset,
            requested,
        })
    }
}

fn read_error_from_volume(err: VolumeError) -> RdmaReadError {
    match err {
        VolumeError::NotFound | VolumeError::Deleted => RdmaReadError::NotFound,
        VolumeError::UnsupportedVersion(version) => {
            RdmaReadError::Unsupported(format!("unsupported volume version {version}"))
        }
        VolumeError::StreamingUnsupported => {
            RdmaReadError::Unsupported("volume cannot provide a streaming source".to_string())
        }
        VolumeError::CookieMismatch(actual) => RdmaReadError::CookieMismatch {
            expected: 0,
            actual,
        },
        VolumeError::Io(e) => RdmaReadError::Io(e),
        other => RdmaReadError::Other(other.to_string()),
    }
}

fn read_to_open_error(err: RdmaReadError) -> RdmaOpenError {
    match err {
        RdmaReadError::NotFound => RdmaOpenError::NotFound,
        RdmaReadError::Unsupported(msg) => RdmaOpenError::Unsupported(msg),
        RdmaReadError::Io(e) => RdmaOpenError::Other(e.to_string()),
        other => RdmaOpenError::Other(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{locate_in_store, StoreNeedleSource};
    use crate::config::MinFreeSpace;
    use crate::storage::needle::needle::Needle;
    use crate::storage::needle_map::NeedleMapKind;
    use crate::storage::store::Store;
    use crate::storage::types::{Cookie, DiskType, NeedleId, Version, VolumeId};
    use seaweed_rdma::buffer_pool::{BufferPool, BufferPoolConfig};
    use seaweed_rdma::{RdmaReadError, RdmaReadableSource, SlotReadError, SlotReader};
    use std::fs::File;
    use std::sync::{Arc, RwLock};
    use tempfile::TempDir;

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

    fn make_store_with_needle(data: &[u8], cookie: u32) -> (TempDir, Store, String) {
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
            .add_volume(
                VolumeId(3),
                "",
                None,
                None,
                0,
                DiskType::HardDrive,
                Version::current(),
            )
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
        (tmp, store, fid)
    }

    #[test]
    fn locate_hit_uses_stream_info_and_volume_dat_path() {
        let (_tmp, store, fid) = make_store_with_needle(b"hello rdma", 0x89b26a98);

        let loc = locate_in_store(&store, &fid).unwrap();

        assert_eq!(loc.volume_id, 3);
        assert_eq!(loc.length, 10);
        assert!(loc.dat_path.as_deref().unwrap().ends_with("3.dat"));
        assert!(loc.offset > 0);

        let mut needle = Needle {
            id: NeedleId(1),
            ..Needle::default()
        };
        let info = store
            .read_volume_needle_stream_info(VolumeId(3), &mut needle, false)
            .unwrap();
        assert_eq!(loc.offset, info.data_file_offset);
    }

    #[test]
    fn locate_offset_points_at_payload_bytes_not_record_header() {
        let (_tmp, store, fid) = make_store_with_needle(b"hello rdma", 0x89b26a98);

        let loc = locate_in_store(&store, &fid).unwrap();
        let file = File::open(loc.dat_path.as_deref().unwrap()).unwrap();
        let mut payload = vec![0u8; loc.length as usize];
        read_exact_at(&file, &mut payload, loc.offset).unwrap();

        assert_eq!(payload, b"hello rdma");
    }

    #[test]
    fn locate_missing_volume_returns_none() {
        let (_tmp, store, _fid) = make_store_with_needle(b"hello rdma", 0x89b26a98);
        assert!(locate_in_store(&store, "999,189b26a98").is_none());
    }

    #[test]
    fn locate_missing_key_returns_none() {
        let (_tmp, store, _fid) = make_store_with_needle(b"hello rdma", 0x89b26a98);
        assert!(locate_in_store(&store, "3,0289b26a98").is_none());
    }

    #[test]
    fn read_handle_reports_cookie_and_serves_payload_offset() {
        let (_tmp, store, fid) = make_store_with_needle(b"hello rdma", 0x89b26a98);
        let source = StoreNeedleSource::from_store_for_tests(Arc::new(RwLock::new(store)));

        let mut handle = source.open(&fid).unwrap();
        let mut payload = vec![0u8; 4];
        handle.read_exact_at(6, &mut payload).unwrap();

        assert_eq!(handle.len(), 10);
        assert_eq!(handle.cookie(), 0x89b26a98);
        assert_eq!(payload, b"rdma");
    }

    #[test]
    fn store_read_handle_advertises_direct_slot_reads() {
        let (_tmp, store, fid) = make_store_with_needle(b"hello rdma", 0x89b26a98);
        let source = StoreNeedleSource::from_store_for_tests(Arc::new(RwLock::new(store)));

        let handle = source.open(&fid).unwrap();

        assert!(handle.supports_direct_slot_reads());
    }

    #[test]
    fn store_read_handle_fills_rdma_buffer_slots() {
        let (_tmp, store, fid) = make_store_with_needle(b"abcdefghijklmnopqrstuvwxyz", 0x89b26a98);
        let source = StoreNeedleSource::from_store_for_tests(Arc::new(RwLock::new(store)));
        let mut handle = source.open(&fid).unwrap();
        let pool = Arc::new(BufferPool::new(BufferPoolConfig {
            size_bytes: 32,
            slot_size: 8,
            aligned: false,
            prefer_hugepage: false,
        }));
        let reader = SlotReader::new(pool.clone());

        let batch = reader
            .read_handle_to_slots(7, handle.as_mut(), 5, 17)
            .unwrap();
        let segments = batch.segments();

        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].request_offset, 0);
        assert_eq!(segments[0].length, 8);
        assert_eq!(segments[1].request_offset, 8);
        assert_eq!(segments[1].length, 8);
        assert_eq!(segments[2].request_offset, 16);
        assert_eq!(segments[2].length, 1);

        let mut payload = Vec::new();
        for segment in segments {
            let bytes = unsafe {
                std::slice::from_raw_parts(segment.slot.ptr as *const u8, segment.length)
            };
            payload.extend_from_slice(bytes);
        }
        assert_eq!(payload, b"fghijklmnopqrstuv");

        assert_eq!(batch.deallocate(), 3);
        assert_eq!(pool.stats().allocated_slots, 0);
    }

    #[test]
    fn direct_slot_read_revalidates_shorter_same_cookie_rewrite() {
        let (_tmp, store, fid) = make_store_with_needle(b"abcdefgh", 0x89b26a98);
        let store = Arc::new(RwLock::new(store));
        let source = StoreNeedleSource::from_store_for_tests(store.clone());
        let mut handle = source.open(&fid).unwrap();
        let pool = Arc::new(BufferPool::new(BufferPoolConfig {
            size_bytes: 16,
            slot_size: 8,
            aligned: false,
            prefer_hugepage: false,
        }));
        let reader = SlotReader::new(pool.clone());

        {
            let mut store = store.write().unwrap();
            let mut needle = Needle {
                id: NeedleId(1),
                cookie: Cookie(0x89b26a98),
                data: b"abcd".to_vec(),
                data_size: 4,
                ..Needle::default()
            };
            store.write_volume_needle(VolumeId(3), &mut needle).unwrap();
        }

        let err = reader
            .read_handle_to_slots(9, handle.as_mut(), 0, 8)
            .unwrap_err();

        assert!(matches!(
            err,
            SlotReadError::Read(RdmaReadError::RangeInvalid {
                len: 4,
                offset: 0,
                requested: 8
            })
        ));
        assert_eq!(pool.stats().allocated_slots, 0);
    }

    #[test]
    fn direct_slot_range_invalid_precedes_buffer_exhaustion() {
        let (_tmp, store, fid) = make_store_with_needle(b"abcd", 0x89b26a98);
        let source = StoreNeedleSource::from_store_for_tests(Arc::new(RwLock::new(store)));
        let mut handle = source.open(&fid).unwrap();
        let pool = Arc::new(BufferPool::new(BufferPoolConfig {
            size_bytes: 4,
            slot_size: 4,
            aligned: false,
            prefer_hugepage: false,
        }));
        let reader = SlotReader::new(pool.clone());

        let err = reader
            .read_handle_to_slots(11, handle.as_mut(), 0, 8)
            .unwrap_err();

        assert!(matches!(
            err,
            SlotReadError::Read(RdmaReadError::RangeInvalid {
                len: 4,
                offset: 0,
                requested: 8
            })
        ));
        assert_eq!(pool.stats().allocated_slots, 0);
    }

    #[test]
    fn direct_slot_read_accepts_longer_same_cookie_rewrite() {
        let (_tmp, store, fid) = make_store_with_needle(b"abcd", 0x89b26a98);
        let store = Arc::new(RwLock::new(store));
        let source = StoreNeedleSource::from_store_for_tests(store.clone());
        let mut handle = source.open(&fid).unwrap();
        let pool = Arc::new(BufferPool::new(BufferPoolConfig {
            size_bytes: 16,
            slot_size: 4,
            aligned: false,
            prefer_hugepage: false,
        }));
        let reader = SlotReader::new(pool.clone());

        {
            let mut store = store.write().unwrap();
            let mut needle = Needle {
                id: NeedleId(1),
                cookie: Cookie(0x89b26a98),
                data: b"abcdefgh".to_vec(),
                data_size: 8,
                ..Needle::default()
            };
            store.write_volume_needle(VolumeId(3), &mut needle).unwrap();
        }

        let batch = reader
            .read_handle_to_slots(10, handle.as_mut(), 0, 8)
            .unwrap();

        let mut payload = Vec::new();
        for segment in batch.segments() {
            let bytes = unsafe {
                std::slice::from_raw_parts(segment.slot.ptr as *const u8, segment.length)
            };
            payload.extend_from_slice(bytes);
        }
        assert_eq!(payload, b"abcdefgh");
        assert_eq!(batch.deallocate(), 2);
        assert_eq!(pool.stats().allocated_slots, 0);
    }

    #[test]
    fn read_handle_rejects_out_of_range_read() {
        let (_tmp, store, fid) = make_store_with_needle(b"hello rdma", 0x89b26a98);
        let source = StoreNeedleSource::from_store_for_tests(Arc::new(RwLock::new(store)));

        let mut handle = source.open(&fid).unwrap();
        let mut payload = vec![0u8; 4];
        let err = handle.read_exact_at(8, &mut payload).unwrap_err();

        assert!(matches!(
            err,
            RdmaReadError::RangeInvalid {
                len: 10,
                offset: 8,
                requested: 4
            }
        ));
    }

    #[test]
    fn open_read_handle_does_not_block_concurrent_store_write() {
        let (_tmp, store, fid) = make_store_with_needle(b"hello rdma", 0x89b26a98);
        let store = Arc::new(RwLock::new(store));
        let source = StoreNeedleSource::from_store_for_tests(store.clone());
        let _handle = source.open(&fid).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            let result = {
                let mut store = store.write().unwrap();
                let mut needle = Needle {
                    id: NeedleId(2),
                    cookie: Cookie(0x01020304),
                    data: b"second".to_vec(),
                    data_size: 6,
                    ..Needle::default()
                };
                store
                    .write_volume_needle(VolumeId(3), &mut needle)
                    .map(|_| ())
            };
            tx.send(result).unwrap();
        });

        rx.recv_timeout(std::time::Duration::from_secs(2))
            .expect("writer blocked while RDMA handle was open")
            .unwrap();
    }

    #[test]
    fn read_at_resolves_length_zero_after_same_cookie_rewrite() {
        let (_tmp, store, fid) = make_store_with_needle(b"short", 0x89b26a98);
        let store = Arc::new(RwLock::new(store));
        let source = StoreNeedleSource::from_store_for_tests(store.clone());
        let mut handle = source.open(&fid).unwrap();

        {
            let mut store = store.write().unwrap();
            let mut needle = Needle {
                id: NeedleId(1),
                cookie: Cookie(0x89b26a98),
                data: b"longer payload".to_vec(),
                data_size: 14,
                ..Needle::default()
            };
            store.write_volume_needle(VolumeId(3), &mut needle).unwrap();
        }

        let payload = handle.read_at(0, 0, 1024).unwrap();

        assert_eq!(payload, b"longer payload");
    }
}
