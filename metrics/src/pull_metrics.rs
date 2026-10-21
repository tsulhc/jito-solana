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

/// Static semantic origins for account scans.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanOrigin {
    GetProgramAccounts,
    GetLargestAccounts,
    GetSupply,
    GetTokenAccountsByOwner,
    GetTokenAccountsByDelegate,
    GetTokenLargestAccounts,
    CirculatingSupply,
    AccountsDataSizeCalculation,
    LedgerToolScanAll,
    LedgerToolGetProgramAccounts,
    Other,
}

pub const SCAN_ORIGIN_SLOTS: usize = 11;
pub const SCAN_ORIGIN_LABELS: [&str; SCAN_ORIGIN_SLOTS] = [
    "get_program_accounts",
    "get_largest_accounts",
    "get_supply",
    "get_token_accounts_by_owner",
    "get_token_accounts_by_delegate",
    "get_token_largest_accounts",
    "circulating_supply",
    "accounts_data_size_calculation",
    "ledger_tool_scan_all",
    "ledger_tool_get_program_accounts",
    "other",
];

impl ScanOrigin {
    pub const fn slot(self) -> usize {
        match self {
            Self::GetProgramAccounts => 0,
            Self::GetLargestAccounts => 1,
            Self::GetSupply => 2,
            Self::GetTokenAccountsByOwner => 3,
            Self::GetTokenAccountsByDelegate => 4,
            Self::GetTokenLargestAccounts => 5,
            Self::CirculatingSupply => 6,
            Self::AccountsDataSizeCalculation => 7,
            Self::LedgerToolScanAll => 8,
            Self::LedgerToolGetProgramAccounts => 9,
            Self::Other => 10,
        }
    }
}

const RPC_METHOD_LABELS: [&str; RPC_METHOD_SLOTS] = [
    "getBalance",
    "getEpochInfo",
    "getGenesisHash",
    "getHealth",
    "getIdentity",
    "getSlot",
    "getBlockHeight",
    "getHighestSnapshotSlot",
    "getTransactionCount",
    "getVersion",
    "getVoteAccounts",
    "getLeaderSchedule",
    "getMinimumBalanceForRentExemption",
    "getInflationGovernor",
    "getInflationRate",
    "getEpochSchedule",
    "getSlotLeader",
    "getSlotLeaders",
    "getBlockProduction",
    "getAccountInfo",
    "getMultipleAccounts",
    "getBlockCommitment",
    "getTokenAccountBalance",
    "getTokenSupply",
    "getProgramAccounts",
    "getLargestAccounts",
    "getSupply",
    "getTokenLargestAccounts",
    "getTokenAccountsByOwner",
    "getTokenAccountsByDelegate",
    "getInflationReward",
    "getClusterNodes",
    "getRecentPerformanceSamples",
    "getSignatureStatuses",
    "getMaxRetransmitSlot",
    "getMaxShredInsertSlot",
    "requestAirdrop",
    "sendTransaction",
    "simulateTransaction",
    "simulateBundle",
    "minimumLedgerSlot",
    "getBlock",
    "getBlockTime",
    "getBlocks",
    "getBlocksWithLimit",
    "getTransaction",
    "getSignaturesForAddress",
    "getFirstAvailableBlock",
    "getLatestBlockhash",
    "isBlockhashValid",
    "getFeeForMessage",
    "getStakeMinimumDelegation",
    "getRecentPrioritizationFees",
    "other",
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
    accounts_scan_active: AtomicU64,
    accounts_scan_started_total: AtomicU64,
    accounts_scan_completed_total: AtomicU64,
    accounts_scan_max_root_distance: AtomicU64,
    scan_origin_active: [AtomicU64; SCAN_ORIGIN_SLOTS],
    scan_origin_started: [AtomicU64; SCAN_ORIGIN_SLOTS],
    scan_origin_completed: [AtomicU64; SCAN_ORIGIN_SLOTS],
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
            accounts_scan_started_total: AtomicU64::new(0),
            accounts_scan_completed_total: AtomicU64::new(0),
            accounts_scan_max_root_distance: AtomicU64::new(0),
            scan_origin_active: std::array::from_fn(|_| AtomicU64::new(0)),
            scan_origin_started: std::array::from_fn(|_| AtomicU64::new(0)),
            scan_origin_completed: std::array::from_fn(|_| AtomicU64::new(0)),
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
    pub fn record_accounts_scan_start(&self, origin: ScanOrigin) {
        self.accounts_scan_active.fetch_add(1, Ordering::Relaxed);
        self.accounts_scan_started_total
            .fetch_add(1, Ordering::Relaxed);
        self.scan_origin_active[origin.slot()].fetch_add(1, Ordering::Relaxed);
        self.scan_origin_started[origin.slot()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_accounts_scan_complete(&self, origin: ScanOrigin) {
        let _ =
            self.accounts_scan_active
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    value.checked_sub(1)
                });
        self.accounts_scan_completed_total
            .fetch_add(1, Ordering::Relaxed);
        let _ = self.scan_origin_active[origin.slot()].fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |value| value.checked_sub(1),
        );
        self.scan_origin_completed[origin.slot()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_accounts_scan_root_distance(&self, distance: u64) {
        self.accounts_scan_max_root_distance
            .fetch_max(distance, Ordering::Relaxed);
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
            let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_sub(1)
            });
        }
    }

    /// Publish one complete allocator sample. Callers should only invoke this after all
    /// four values have been read successfully.
    pub fn store_jemalloc_stats(&self, allocated: u64, active: u64, resident: u64, retained: u64) {
        self.jemalloc_allocated_bytes
            .store(allocated, Ordering::Relaxed);
        self.jemalloc_active_bytes.store(active, Ordering::Relaxed);
        self.jemalloc_resident_bytes
            .store(resident, Ordering::Relaxed);
        self.jemalloc_retained_bytes
            .store(retained, Ordering::Relaxed);
    }

    fn push_micros_as_seconds(output: &mut String, micros: u64) {
        output.push_str(&(micros as f64 / 1_000_000.0).to_string());
    }

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
        gauge!("agave_accounts_scan_active", self.accounts_scan_active);
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
        for (slot, label) in SCAN_ORIGIN_LABELS.iter().enumerate() {
            output.push_str("agave_accounts_scan_origin_active{origin=\"");
            output.push_str(label);
            output.push_str("\"} ");
            output.push_str(
                &self.scan_origin_active[slot]
                    .load(Ordering::Relaxed)
                    .to_string(),
            );
            output.push('\n');

            output.push_str("agave_accounts_scan_origin_started_total{origin=\"");
            output.push_str(label);
            output.push_str("\"} ");
            output.push_str(
                &self.scan_origin_started[slot]
                    .load(Ordering::Relaxed)
                    .to_string(),
            );
            output.push('\n');

            output.push_str("agave_accounts_scan_origin_completed_total{origin=\"");
            output.push_str(label);
            output.push_str("\"} ");
            output.push_str(
                &self.scan_origin_completed[slot]
                    .load(Ordering::Relaxed)
                    .to_string(),
            );
            output.push('\n');
        }
        gauge!(
            "agave_jemalloc_allocated_bytes",
            self.jemalloc_allocated_bytes
        );
        gauge!("agave_jemalloc_active_bytes", self.jemalloc_active_bytes);
        gauge!(
            "agave_jemalloc_resident_bytes",
            self.jemalloc_resident_bytes
        );
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
    fn scan_lifecycle_is_live_and_balanced() {
        let metrics = PullMetrics::default();
        metrics.record_accounts_scan_start(ScanOrigin::GetProgramAccounts);
        metrics.record_accounts_scan_start(ScanOrigin::GetLargestAccounts);
        metrics.record_accounts_scan_root_distance(7);
        metrics.record_accounts_scan_complete(ScanOrigin::GetProgramAccounts);
        let output = metrics.exposition();
        assert!(output.contains("agave_accounts_scan_active 1\n"));
        assert!(output.contains("agave_accounts_scan_started_total 2\n"));
        assert!(output.contains("agave_accounts_scan_completed_total 1\n"));
        assert!(output.contains("agave_accounts_scan_max_root_distance 7\n"));
        assert!(
            output
                .contains("agave_accounts_scan_origin_active{origin=\"get_program_accounts\"} 0\n")
        );
        assert!(output.contains(
            "agave_accounts_scan_origin_started_total{origin=\"get_largest_accounts\"} 1\n"
        ));
        assert!(output.contains(
            "agave_accounts_scan_origin_completed_total{origin=\"get_program_accounts\"} 1\n"
        ));
    }

    #[test]
    fn scan_origin_aggregates_are_balanced_and_bounded() {
        let metrics = PullMetrics::default();
        metrics.record_accounts_scan_start(ScanOrigin::GetProgramAccounts);
        metrics.record_accounts_scan_start(ScanOrigin::GetLargestAccounts);
        metrics.record_accounts_scan_complete(ScanOrigin::GetProgramAccounts);

        assert_eq!(metrics.accounts_scan_active.load(Ordering::Relaxed), 1);
        assert_eq!(
            metrics.accounts_scan_started_total.load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            metrics
                .accounts_scan_completed_total
                .load(Ordering::Relaxed),
            1
        );
        let active_sum: u64 = metrics
            .scan_origin_active
            .iter()
            .map(|value| value.load(Ordering::Relaxed))
            .sum();
        let started_sum: u64 = metrics
            .scan_origin_started
            .iter()
            .map(|value| value.load(Ordering::Relaxed))
            .sum();
        let completed_sum: u64 = metrics
            .scan_origin_completed
            .iter()
            .map(|value| value.load(Ordering::Relaxed))
            .sum();
        assert_eq!(
            active_sum,
            metrics.accounts_scan_active.load(Ordering::Relaxed)
        );
        assert_eq!(
            started_sum,
            metrics.accounts_scan_started_total.load(Ordering::Relaxed)
        );
        assert_eq!(
            completed_sum,
            metrics
                .accounts_scan_completed_total
                .load(Ordering::Relaxed)
        );
        assert_eq!(
            metrics.scan_origin_active[ScanOrigin::GetProgramAccounts.slot()]
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            metrics.scan_origin_active[ScanOrigin::GetLargestAccounts.slot()]
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(SCAN_ORIGIN_SLOTS, SCAN_ORIGIN_LABELS.len());
        assert_eq!(
            SCAN_ORIGIN_LABELS
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            SCAN_ORIGIN_SLOTS
        );
    }

    #[test]
    fn scan_origin_completion_does_not_underflow() {
        let metrics = PullMetrics::default();
        metrics.record_accounts_scan_complete(ScanOrigin::Other);
        assert_eq!(metrics.accounts_scan_active.load(Ordering::Relaxed), 0);
        assert_eq!(
            metrics.scan_origin_active[ScanOrigin::Other.slot()].load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            metrics
                .accounts_scan_completed_total
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            metrics.scan_origin_completed[ScanOrigin::Other.slot()].load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn response_outcomes_are_bounded_and_duration_is_cumulative() {
        let metrics = PullMetrics::default();
        let slot = rpc_method_slot("getProgramAccounts");
        metrics.record_rpc_request(slot);
        metrics.record_rpc_completion(slot, Duration::from_millis(250), Some(true));
        metrics.record_rpc_completion(slot, Duration::from_millis(750), Some(false));
        metrics.record_rpc_completion(slot, Duration::from_millis(500), None);
        metrics.finish_rpc_request(slot);
        let output = metrics.exposition();
        assert!(output.contains(
            "agave_rpc_responses_total{method=\"getProgramAccounts\",outcome=\"success\"} 1\n"
        ));
        assert!(output.contains(
            "agave_rpc_responses_total{method=\"getProgramAccounts\",outcome=\"error\"} 1\n"
        ));
        assert!(
            output
                .contains("agave_rpc_duration_seconds_total{method=\"getProgramAccounts\"} 1.5\n")
        );
        assert!(output.contains("agave_rpc_in_flight{method=\"getProgramAccounts\"} 0\n"));
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
