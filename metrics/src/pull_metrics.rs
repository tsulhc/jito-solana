//! Lock-free, pull-oriented metrics used by the validator's private metrics route.
//!
//! Updates only touch numeric atomics.  The Prometheus-compatible text is built by
//! [`PullMetrics::exposition`] when a scrape is requested.

use std::sync::atomic::{AtomicU64, Ordering};

/// The number of method slots reserved for RPC instrumentation.
pub const RPC_METHOD_SLOTS: usize = 64;

const RPC_METHOD_LABELS: [&str; RPC_METHOD_SLOTS] = [
    "method_0",
    "method_1",
    "method_2",
    "method_3",
    "method_4",
    "method_5",
    "method_6",
    "method_7",
    "method_8",
    "method_9",
    "method_10",
    "method_11",
    "method_12",
    "method_13",
    "method_14",
    "method_15",
    "method_16",
    "method_17",
    "method_18",
    "method_19",
    "method_20",
    "method_21",
    "method_22",
    "method_23",
    "method_24",
    "method_25",
    "method_26",
    "method_27",
    "method_28",
    "method_29",
    "method_30",
    "method_31",
    "method_32",
    "method_33",
    "method_34",
    "method_35",
    "method_36",
    "method_37",
    "method_38",
    "method_39",
    "method_40",
    "method_41",
    "method_42",
    "method_43",
    "method_44",
    "method_45",
    "method_46",
    "method_47",
    "method_48",
    "method_49",
    "method_50",
    "method_51",
    "method_52",
    "method_53",
    "method_54",
    "method_55",
    "method_56",
    "method_57",
    "method_58",
    "method_59",
    "method_60",
    "method_61",
    "method_62",
    "method_63",
];

pub struct PullMetrics {
    pub accounts_index_bytes: AtomicU64,
    pub accounts_index_entries: AtomicU64,
    pub accounts_scan_total: AtomicU64,
    pub accounts_scan_in_flight: AtomicU64,
    pub jemalloc_allocated_bytes: AtomicU64,
    pub jemalloc_resident_bytes: AtomicU64,
    pub jemalloc_active_bytes: AtomicU64,
    pub jemalloc_metadata_bytes: AtomicU64,
    pub rpc_in_flight: AtomicU64,
    rpc_calls: [AtomicU64; RPC_METHOD_SLOTS],
    rpc_in_flight_by_method: [AtomicU64; RPC_METHOD_SLOTS],
}

impl Default for PullMetrics {
    fn default() -> Self {
        Self {
            accounts_index_bytes: AtomicU64::new(0),
            accounts_index_entries: AtomicU64::new(0),
            accounts_scan_total: AtomicU64::new(0),
            accounts_scan_in_flight: AtomicU64::new(0),
            jemalloc_allocated_bytes: AtomicU64::new(0),
            jemalloc_resident_bytes: AtomicU64::new(0),
            jemalloc_active_bytes: AtomicU64::new(0),
            jemalloc_metadata_bytes: AtomicU64::new(0),
            rpc_in_flight: AtomicU64::new(0),
            rpc_calls: std::array::from_fn(|_| AtomicU64::new(0)),
            rpc_in_flight_by_method: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl PullMetrics {
    /// Increment a bounded RPC method counter. Out-of-range slots are ignored.
    pub fn increment_rpc_method(&self, slot: usize) {
        if let Some(counter) = self.rpc_calls.get(slot) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Adjust the bounded in-flight count for an RPC method.
    pub fn set_rpc_method_in_flight(&self, slot: usize, value: u64) {
        if let Some(counter) = self.rpc_in_flight_by_method.get(slot) {
            counter.store(value, Ordering::Relaxed);
        }
    }

    /// Return a complete scrape without taking a lock on producer state.
    pub fn exposition(&self) -> String {
        let mut output = String::new();
        macro_rules! gauge {
            ($name:literal, $value:expr) => {
                output.push_str(concat!($name, " "));
                output.push_str(&$value.load(Ordering::Relaxed).to_string());
                output.push('\n');
            };
        }
        gauge!("solana_accounts_index_bytes", self.accounts_index_bytes);
        gauge!("solana_accounts_index_entries", self.accounts_index_entries);
        gauge!("solana_accounts_scan_total", self.accounts_scan_total);
        gauge!(
            "solana_accounts_scan_in_flight",
            self.accounts_scan_in_flight
        );
        gauge!(
            "solana_jemalloc_allocated_bytes",
            self.jemalloc_allocated_bytes
        );
        gauge!(
            "solana_jemalloc_resident_bytes",
            self.jemalloc_resident_bytes
        );
        gauge!("solana_jemalloc_active_bytes", self.jemalloc_active_bytes);
        gauge!(
            "solana_jemalloc_metadata_bytes",
            self.jemalloc_metadata_bytes
        );
        gauge!("solana_rpc_in_flight", self.rpc_in_flight);
        for (slot, label) in RPC_METHOD_LABELS.iter().enumerate() {
            output.push_str("solana_rpc_calls_total{method=\"");
            output.push_str(label);
            output.push_str("\"} ");
            output.push_str(&self.rpc_calls[slot].load(Ordering::Relaxed).to_string());
            output.push('\n');
            output.push_str("solana_rpc_in_flight{method=\"");
            output.push_str(label);
            output.push_str("\"} ");
            output.push_str(
                &self.rpc_in_flight_by_method[slot]
                    .load(Ordering::Relaxed)
                    .to_string(),
            );
            output.push('\n');
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn updates_are_reflected_at_scrape_time() {
        let metrics = PullMetrics::default();
        metrics.accounts_index_bytes.store(42, Ordering::Relaxed);
        metrics.increment_rpc_method(3);
        metrics.set_rpc_method_in_flight(3, 2);
        metrics.increment_rpc_method(RPC_METHOD_SLOTS);
        let output = metrics.exposition();
        assert!(output.contains("solana_accounts_index_bytes 42\n"));
        assert!(output.contains("solana_rpc_calls_total{method=\"method_3\"} 1\n"));
        assert!(output.contains("solana_rpc_in_flight{method=\"method_3\"} 2\n"));
        assert!(!output.contains("method_64"));
    }
}
