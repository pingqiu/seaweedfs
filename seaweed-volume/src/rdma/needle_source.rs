//! Adapter from the Rust volume server storage state to the SRA substrate.

use std::sync::Arc;

use seaweed_rdma::{NeedleLocation, NeedleSource};

use crate::rdma::parse_fid::parse_fid;
use crate::server::volume_server::VolumeServerState;
use crate::storage::needle::needle::Needle;
use crate::storage::store::Store;
use crate::storage::types::{
    NeedleId, VolumeId, DATA_SIZE_SIZE, NEEDLE_HEADER_SIZE, VERSION_1,
};

#[derive(Clone)]
pub struct StoreNeedleSource {
    state: Arc<VolumeServerState>,
}

impl StoreNeedleSource {
    pub fn new(state: Arc<VolumeServerState>) -> Self {
        Self { state }
    }
}

impl NeedleSource for StoreNeedleSource {
    fn locate(&self, id: &str) -> Option<NeedleLocation> {
        let store = self.state.store.read().ok()?;
        locate_in_store(&store, id)
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
    let record_prefix = if volume.version() == VERSION_1 {
        NEEDLE_HEADER_SIZE
    } else {
        NEEDLE_HEADER_SIZE + DATA_SIZE_SIZE
    };
    let record_offset = info.data_file_offset.checked_sub(record_prefix as u64)?;

    Some(NeedleLocation {
        volume_id: vid,
        offset: record_offset,
        length: info.data_size as u64,
        dat_path: Some(volume.file_name(".dat")),
    })
}

#[cfg(test)]
mod tests {
    use super::locate_in_store;
    use crate::config::MinFreeSpace;
    use crate::storage::needle::needle::Needle;
    use crate::storage::needle_map::NeedleMapKind;
    use crate::storage::store::Store;
    use crate::storage::types::{
        Cookie, DiskType, NeedleId, Version, VolumeId, DATA_SIZE_SIZE, NEEDLE_HEADER_SIZE,
    };
    use tempfile::TempDir;

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

        let fid = format!("3,{:x}{:08x}", 1u64, cookie);
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
        assert_eq!(
            loc.offset + (NEEDLE_HEADER_SIZE + DATA_SIZE_SIZE) as u64,
            info.data_file_offset
        );
    }

    #[test]
    fn locate_missing_volume_returns_none() {
        let (_tmp, store, _fid) = make_store_with_needle(b"hello rdma", 0x89b26a98);
        assert!(locate_in_store(&store, "999,189b26a98").is_none());
    }

    #[test]
    fn locate_missing_key_returns_none() {
        let (_tmp, store, _fid) = make_store_with_needle(b"hello rdma", 0x89b26a98);
        assert!(locate_in_store(&store, "3,289b26a98").is_none());
    }
}
