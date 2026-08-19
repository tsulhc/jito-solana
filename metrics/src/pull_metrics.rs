//! Lock-free, pull-oriented metrics used by the validator's private metrics route.
//!
//! Updates only touch numeric atomics.  The Prometheus-compatible text is built by
//! [`PullMetrics::exposition`] when a scrape is requested.

use std::sync::atomic::{AtomicU64, Ordering};

/// The number of method slots reserved for RPC instrumentation.
pub const RPC_METHOD_SLOTS: usize = 54;

const RPC_METHOD_LABELS: [&str; RPC_METHOD_SLOTS] = [
    "getBalance", "getEpochInfo", "getGenesisHash", "getHealth", "getIdentity", "getSlot",
    "getBlockHeight", "getHighestSnapshotSlot", "getTransactionCount", "getVersion",
    "getVoteAccounts", "getLeaderSchedule", "getMinimumBalanceForRentExemption",
    "getInflationGovernor", "getInflationRate", "getEpochSchedule", "getSlotLeader",
    "getSlotLeaders", "getBlockProduction", "getAccountInfo", "getMultipleAccounts",
    "getBlockCommitment", "getTokenAccountBalance", "getTokenSupply", "getProgramAccounts",
    "getLargestAccounts", "getSupply", "getTokenLargestAccounts", "getTokenAccountsByOwner",
    "getTokenAccountsByDelegate", "getInflationReward", "getClusterNodes",
    "getRecentPerformanceSamples", "getSignatureStatuses", "getMaxRetransmitSlot",
    "getMaxShredInsertSlot", "requestAirdrop", "sendTransaction", "simulateTransaction",
    "simulateBundle", "minimumLedgerSlot", "getBlock", "getBlockTime", "getBlocks",
    "getBlocksWithLimit", "getTransaction", "getSignaturesForAddress", "getFirstAvailableBlock",
    "getLatestBlockhash", "isBlockhashValid", "getFeeForMessage", "getStakeMinimumDelegation",
    "getRecentPrioritizationFees", "other",
];

pub fn rpc_method_slot(method: &str) -> usize {
    RPC_METHOD_LABELS[..RPC_METHOD_SLOTS - 1]
        .iter()
        .position(|known| *known == method)
        .unwrap_or(RPC_METHOD_SLOTS - 1)
}

pub struct PullMetrics {
    pub accounts_index_bytes: AtomicU64,
    pub accounts_index_entries: AtomicU64,
    pub accounts_scan_total: AtomicU64,
    pub accounts_scan_in_flight: AtomicU64,
    pub jemalloc_allocated_bytes: AtomicU64,
    pub jemalloc_resident_bytes: AtomicU64,
    pub jemalloc_active_bytes: AtomicU64,
    pub jemalloc_metadata_bytes: AtomicU64,
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
            rpc_calls: std::array::from_fn(|_| AtomicU64::new(0)),
            rpc_in_flight_by_method: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl PullMetrics {
    pub fn record_rpc_request(&self, slot: usize) {
        if let Some(counter) = self.rpc_calls.get(slot) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(counter) = self.rpc_in_flight_by_method.get(slot) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn finish_rpc_request(&self, slot: usize) {
        if let Some(counter) = self.rpc_in_flight_by_method.get(slot) {
            counter.fetch_sub(1, Ordering::Relaxed);
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
        gauge!("agave_accounts_index_bytes", self.accounts_index_bytes);
        gauge!("agave_accounts_index_entries", self.accounts_index_entries);
        gauge!("agave_accounts_scan_total", self.accounts_scan_total);
        gauge!(
            "agave_accounts_scan_in_flight",
            self.accounts_scan_in_flight
        );
        gauge!(
            "agave_jemalloc_allocated_bytes",
            self.jemalloc_allocated_bytes
        );
        gauge!(
            "agave_jemalloc_resident_bytes",
            self.jemalloc_resident_bytes
        );
        gauge!("agave_jemalloc_active_bytes", self.jemalloc_active_bytes);
        gauge!(
            "agave_jemalloc_metadata_bytes",
            self.jemalloc_metadata_bytes
        );
        for (slot, label) in RPC_METHOD_LABELS.iter().enumerate() {
            output.push_str("agave_rpc_requests_total{method=\"");
            output.push_str(label);
            output.push_str("\"} ");
            output.push_str(&self.rpc_calls[slot].load(Ordering::Relaxed).to_string());
            output.push('\n');
            output.push_str("agave_rpc_in_flight{method=\"");
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
        metrics.record_rpc_request(3);
        let output = metrics.exposition();
        assert!(output.contains("agave_accounts_index_bytes 42\n"));
        assert!(output.contains("agave_rpc_requests_total{method=\"getHealth\"} 1\n"));
        assert!(output.contains("agave_rpc_in_flight{method=\"getHealth\"} 1\n"));
    }

    #[test]
    fn totals_and_in_flight_are_independent() {
        let metrics = PullMetrics::default();
        let slot = rpc_method_slot("getBalance");
        metrics.record_rpc_request(slot);
        metrics.record_rpc_request(slot);
        metrics.finish_rpc_request(slot);
        let output = metrics.exposition();
        assert!(output.contains("agave_rpc_requests_total{method=\"getBalance\"} 2\n"));
        assert!(output.contains("agave_rpc_in_flight{method=\"getBalance\"} 1\n"));
    }

    #[test]
    fn unknown_methods_use_the_bounded_other_slot() {
        let metrics = PullMetrics::default();
        let slot = rpc_method_slot("notRegistered");
        assert_eq!(slot, RPC_METHOD_SLOTS - 1);
        metrics.record_rpc_request(slot);
        metrics.finish_rpc_request(slot);
        let output = metrics.exposition();
        assert!(output.contains("agave_rpc_requests_total{method=\"other\"} 1\n"));
        assert!(output.contains("agave_rpc_in_flight{method=\"other\"} 0\n"));
        assert!(!output.contains("method_"));
    }

    #[test]
    fn catalog_cardinality_is_bounded() {
        assert!(RPC_METHOD_SLOTS <= 128);
        assert_eq!(RPC_METHOD_SLOTS, RPC_METHOD_LABELS.len());
    }
}
