//! Lock-free, pull-oriented metrics used by the validator's private metrics route.
//!
//! Updates only touch numeric atomics. The Prometheus-compatible text is built by
//! [`PullMetrics::exposition`] when a scrape is requested.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

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
    pub accounts_index_count_in_mem: AtomicU64,
    pub accounts_index_capacity_in_mem: AtomicU64,
    pub accounts_index_estimate_mem_bytes: AtomicU64,
    /// Legacy sampled value written by `clean_accounts`. Phase 1.1 exports
    /// `accounts_scan_active_live` instead so clean sampling cannot race with
    /// ScanGuard lifecycle updates.
    pub accounts_scan_active: AtomicU64,
    accounts_scan_active_live: AtomicU64,
    accounts_scan_started_total: AtomicU64,
    accounts_scan_completed_total: AtomicU64,
    pub accounts_scan_max_root_distance: AtomicU64,
    pub jemalloc_allocated_bytes: AtomicU64,
    pub jemalloc_resident_bytes: AtomicU64,
    pub jemalloc_active_bytes: AtomicU64,
    pub jemalloc_retained_bytes: AtomicU64,
    rpc_calls: [AtomicU64; RPC_METHOD_SLOTS],
    rpc_responses_success: [AtomicU64; RPC_METHOD_SLOTS],
    rpc_responses_error: [AtomicU64; RPC_METHOD_SLOTS],
    rpc_duration_micros: [AtomicU64; RPC_METHOD_SLOTS],
    rpc_in_flight_by_method: [AtomicU64; RPC_METHOD_SLOTS],
}

impl Default for PullMetrics {
    fn default() -> Self {
        Self {
            accounts_index_count_in_mem: AtomicU64::new(0),
            accounts_index_capacity_in_mem: AtomicU64::new(0),
            accounts_index_estimate_mem_bytes: AtomicU64::new(0),
            accounts_scan_active: AtomicU64::new(0),
            accounts_scan_active_live: AtomicU64::new(0),
            accounts_scan_started_total: AtomicU64::new(0),
            accounts_scan_completed_total: AtomicU64::new(0),
            accounts_scan_max_root_distance: AtomicU64::new(0),
            jemalloc_allocated_bytes: AtomicU64::new(0),
            jemalloc_resident_bytes: AtomicU64::new(0),
            jemalloc_active_bytes: AtomicU64::new(0),
            jemalloc_retained_bytes: AtomicU64::new(0),
            rpc_calls: std::array::from_fn(|_| AtomicU64::new(0)),
            rpc_responses_success: std::array::from_fn(|_| AtomicU64::new(0)),
            rpc_responses_error: std::array::from_fn(|_| AtomicU64::new(0)),
            rpc_duration_micros: std::array::from_fn(|_| AtomicU64::new(0)),
            rpc_in_flight_by_method: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl PullMetrics {
    pub fn record_accounts_scan_start(&self) {
        self.accounts_scan_active_live
            .fetch_add(1, Ordering::Relaxed);
        self.accounts_scan_started_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_accounts_scan_complete(&self) {
        let _ = self.accounts_scan_active_live.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |value| value.checked_sub(1),
        );
        self.accounts_scan_completed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_rpc_request(&self, slot: usize) {
        if let Some(counter) = self.rpc_calls.get(slot) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(counter) = self.rpc_in_flight_by_method.get(slot) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_rpc_completion(&self, slot: usize, duration: Duration, success: Option<bool>) {
        if let Some(counter) = self.rpc_duration_micros.get(slot) {
            let micros = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
            let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_add(micros))
            });
        }
        let response_counters = match success {
            Some(true) => Some(&self.rpc_responses_success),
            Some(false) => Some(&self.rpc_responses_error),
            None => None,
        };
        if let Some(counter) = response_counters.and_then(|counters| counters.get(slot)) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn finish_rpc_request(&self, slot: usize) {
        if let Some(counter) = self.rpc_in_flight_by_method.get(slot) {
            // Keep the gauge valid even if a caller accidentally finishes a
            // request more than once. In particular, never wrap to u64::MAX.
            let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_sub(1)
            });
        }
    }

    /// Publish one complete allocator sample. Callers should only invoke this
    /// after all four values have been read successfully.
    pub fn store_jemalloc_stats(&self, allocated: u64, active: u64, resident: u64, retained: u64) {
        self.jemalloc_allocated_bytes
            .store(allocated, Ordering::Relaxed);
        self.jemalloc_active_bytes.store(active, Ordering::Relaxed);
        self.jemalloc_resident_bytes.store(resident, Ordering::Relaxed);
        self.jemalloc_retained_bytes.store(retained, Ordering::Relaxed);
    }

    fn push_micros_as_seconds(output: &mut String, micros: u64) {
        output.push_str(&(micros as f64 / 1_000_000.0).to_string());
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
        gauge!(
            "agave_accounts_index_count_in_mem",
            self.accounts_index_count_in_mem
        );
        gauge!(
            "agave_accounts_index_capacity_in_mem",
            self.accounts_index_capacity_in_mem
        );
        gauge!(
            "agave_accounts_index_estimate_mem_bytes",
            self.accounts_index_estimate_mem_bytes
        );
        gauge!(
            "agave_accounts_scan_active",
            self.accounts_scan_active_live
        );
        gauge!(
            "agave_accounts_scan_started_total",
            self.accounts_scan_started_total
        );
        gauge!(
            "agave_accounts_scan_completed_total",
            self.accounts_scan_completed_total
        );
        gauge!(
            "agave_accounts_scan_max_root_distance",
            self.accounts_scan_max_root_distance
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
            "agave_jemalloc_retained_bytes",
            self.jemalloc_retained_bytes
        );
        for (slot, label) in RPC_METHOD_LABELS.iter().enumerate() {
            output.push_str("agave_rpc_requests_total{method=\"");
            output.push_str(label);
            output.push_str("\"} ");
            output.push_str(&self.rpc_calls[slot].load(Ordering::Relaxed).to_string());
            output.push('\n');
            output.push_str("agave_rpc_responses_total{method=\"");
            output.push_str(label);
            output.push_str("\",outcome=\"success\"} ");
            output.push_str(
                &self.rpc_responses_success[slot]
                    .load(Ordering::Relaxed)
                    .to_string(),
            );
            output.push('\n');
            output.push_str("agave_rpc_responses_total{method=\"");
            output.push_str(label);
            output.push_str("\",outcome=\"error\"} ");
            output.push_str(
                &self.rpc_responses_error[slot]
                    .load(Ordering::Relaxed)
                    .to_string(),
            );
            output.push('\n');
            output.push_str("agave_rpc_duration_seconds_total{method=\"");
            output.push_str(label);
            output.push_str("\"} ");
            Self::push_micros_as_seconds(
                &mut output,
                self.rpc_duration_micros[slot].load(Ordering::Relaxed),
            );
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
        metrics
            .accounts_index_count_in_mem
            .store(42, Ordering::Relaxed);
        metrics
            .accounts_index_capacity_in_mem
            .store(84, Ordering::Relaxed);
        metrics
            .accounts_index_estimate_mem_bytes
            .store(168, Ordering::Relaxed);
        metrics.accounts_scan_active.store(99, Ordering::Relaxed);
        metrics.record_accounts_scan_start();
        metrics
            .accounts_scan_max_root_distance
            .store(7, Ordering::Relaxed);
        metrics.store_jemalloc_stats(10, 20, 30, 40);
        metrics.record_rpc_request(3);
        metrics.record_rpc_completion(3, Duration::from_micros(1_500_000), Some(true));
        let output = metrics.exposition();
        assert!(output.contains("agave_accounts_index_count_in_mem 42\n"));
        assert!(output.contains("agave_accounts_index_capacity_in_mem 84\n"));
        assert!(output.contains("agave_accounts_index_estimate_mem_bytes 168\n"));
        assert!(output.contains("agave_accounts_scan_active 1\n"));
        assert!(output.contains("agave_accounts_scan_started_total 1\n"));
        assert!(output.contains("agave_accounts_scan_completed_total 0\n"));
        assert!(output.contains("agave_accounts_scan_max_root_distance 7\n"));
        assert!(output.contains("agave_jemalloc_allocated_bytes 10\n"));
        assert!(output.contains("agave_jemalloc_active_bytes 20\n"));
        assert!(output.contains("agave_jemalloc_resident_bytes 30\n"));
        assert!(output.contains("agave_jemalloc_retained_bytes 40\n"));
        assert!(!output.contains("agave_jemalloc_metadata_bytes"));
        assert!(output.contains("agave_rpc_requests_total{method=\"getHealth\"} 1\n"));
        assert!(output.contains(
            "agave_rpc_responses_total{method=\"getHealth\",outcome=\"success\"} 1\n"
        ));
        assert!(output.contains(
            "agave_rpc_responses_total{method=\"getHealth\",outcome=\"error\"} 0\n"
        ));
        assert!(output.contains(
            "agave_rpc_duration_seconds_total{method=\"getHealth\"} 1.5\n"
        ));
        assert!(output.contains("agave_rpc_in_flight{method=\"getHealth\"} 1\n"));
    }

    #[test]
    fn scan_lifecycle_is_live_and_balanced() {
        let metrics = PullMetrics::default();
        metrics.record_accounts_scan_start();
        metrics.record_accounts_scan_start();
        metrics.record_accounts_scan_complete();
        let output = metrics.exposition();
        assert!(output.contains("agave_accounts_scan_active 1\n"));
        assert!(output.contains("agave_accounts_scan_started_total 2\n"));
        assert!(output.contains("agave_accounts_scan_completed_total 1\n"));
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
    fn response_outcomes_are_bounded_and_duration_is_cumulative() {
        let metrics = PullMetrics::default();
        let slot = rpc_method_slot("getProgramAccounts");
        metrics.record_rpc_completion(slot, Duration::from_millis(250), Some(true));
        metrics.record_rpc_completion(slot, Duration::from_millis(750), Some(false));
        metrics.record_rpc_completion(slot, Duration::from_millis(500), None);
        let output = metrics.exposition();
        assert!(output.contains(
            "agave_rpc_responses_total{method=\"getProgramAccounts\",outcome=\"success\"} 1\n"
        ));
        assert!(output.contains(
            "agave_rpc_responses_total{method=\"getProgramAccounts\",outcome=\"error\"} 1\n"
        ));
        assert!(output.contains(
            "agave_rpc_duration_seconds_total{method=\"getProgramAccounts\"} 1.5\n"
        ));
    }

    #[test]
    fn unknown_methods_use_the_bounded_other_slot() {
        let metrics = PullMetrics::default();
        let slot = rpc_method_slot("notRegistered");
        assert_eq!(slot, RPC_METHOD_SLOTS - 1);
        metrics.record_rpc_request(slot);
        metrics.record_rpc_completion(slot, Duration::from_micros(10), Some(false));
        metrics.finish_rpc_request(slot);
        let output = metrics.exposition();
        assert!(output.contains("agave_rpc_requests_total{method=\"other\"} 1\n"));
        assert!(output.contains("agave_rpc_in_flight{method=\"other\"} 0\n"));
        assert!(output.contains(
            "agave_rpc_responses_total{method=\"other\",outcome=\"error\"} 1\n"
        ));
        assert!(!output.contains("method_"));
    }

    #[test]
    fn finishing_without_an_in_flight_request_does_not_underflow() {
        let metrics = PullMetrics::default();
        metrics.finish_rpc_request(rpc_method_slot("getHealth"));
        let output = metrics.exposition();
        assert!(output.contains("agave_rpc_in_flight{method=\"getHealth\"} 0\n"));
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn catalog_cardinality_is_bounded() {
        assert!(RPC_METHOD_SLOTS <= 128);
        assert_eq!(RPC_METHOD_SLOTS, RPC_METHOD_LABELS.len());
    }
}
