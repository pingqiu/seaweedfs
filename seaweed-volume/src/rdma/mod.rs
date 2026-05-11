//! RDMA read path for the Rust volume server.
//!
//! This module is built around the canonical SRA substrate in
//! `C:\work\rdma\sra\seaweed-rdma`. The default build keeps this module
//! TCP/mock-test capable without requiring libibverbs; the `rdma` feature
//! enables real RDMA through the substrate.

pub mod needle_source;
pub mod parse_fid;

pub use needle_source::StoreNeedleSource;
pub use parse_fid::{parse_fid, ParseFidError};
