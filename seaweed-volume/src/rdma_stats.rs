//! Runtime RDMA stats exposed through `/sra/rdma/stats`.

use std::sync::{Arc, OnceLock};

use seaweed_rdma::buffer_pool::{BufferPool, BufferPoolStats};
use serde_json::{json, Value};

static RC_BUFFER_POOL: OnceLock<Arc<BufferPool>> = OnceLock::new();

pub fn set_rc_buffer_pool(pool: Arc<BufferPool>) {
    let _ = RC_BUFFER_POOL.set(pool);
}

pub fn rdma_stats_json() -> Value {
    match RC_BUFFER_POOL.get() {
        Some(pool) => {
            let stats = pool.stats();
            json!({
                "schema_version": 1,
                "service": "weed-volume",
                "kind": "rdma_listener",
                "available": true,
                "transport": "rc",
                "buffer_pool": buffer_pool_stats_json(stats),
            })
        }
        None => json!({
            "schema_version": 1,
            "service": "weed-volume",
            "kind": "rdma_listener",
            "available": false,
            "transport": null,
            "buffer_pool": null,
        }),
    }
}

fn buffer_pool_stats_json(stats: BufferPoolStats) -> Value {
    json!({
        "total_slots": stats.total_slots,
        "free_slots": stats.free_slots,
        "allocated_slots": stats.allocated_slots,
        "total_allocations": stats.total_allocations,
        "exhausted_events": stats.exhausted_events,
        "wait_events": stats.wait_events,
        "wait_timeout_events": stats.wait_timeout_events,
        "wait_total_micros": stats.wait_total_micros,
        "request_slot_observations": stats.request_slot_observations,
        "request_slots_total": stats.request_slots_total,
        "request_slots_max": stats.request_slots_max,
        "quarantined_slots_total": stats.quarantined_slots_total,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rdma_stats_json_is_stable_when_unavailable() {
        let value = rdma_stats_json();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["service"], "weed-volume");
        assert_eq!(value["kind"], "rdma_listener");
        assert_eq!(value["available"], false);
        assert!(value["buffer_pool"].is_null());
    }
}
