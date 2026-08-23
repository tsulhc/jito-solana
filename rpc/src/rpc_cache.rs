use {
    solana_rpc_client_api::{config::RpcLargestAccountsFilter, response::RpcAccountBalance},
    std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        time::{Duration, SystemTime},
    },
    tokio::sync::watch,
};

#[derive(Debug, Clone)]
pub struct LargestAccountsCache {
    duration: u64,
    cache: HashMap<Option<RpcLargestAccountsFilter>, LargestAccountsCacheValue>,
}

#[derive(Debug, Clone)]
struct LargestAccountsCacheValue {
    accounts: Vec<RpcAccountBalance>,
    slot: u64,
    cached_time: SystemTime,
}

impl LargestAccountsCache {
    pub(crate) fn new(duration: u64) -> Self {
        Self {
            duration,
            cache: HashMap::new(),
        }
    }

    pub(crate) fn get_largest_accounts(
        &self,
        filter: &Option<RpcLargestAccountsFilter>,
    ) -> Option<(u64, Vec<RpcAccountBalance>)> {
        self.cache.get(filter).and_then(|value| {
            if let Ok(elapsed) = value.cached_time.elapsed()
                && elapsed < Duration::from_secs(self.duration)
            {
                return Some((value.slot, value.accounts.clone()));
            }
            None
        })
    }

    pub(crate) fn set_largest_accounts(
        &mut self,
        filter: &Option<RpcLargestAccountsFilter>,
        slot: u64,
        accounts: &[RpcAccountBalance],
    ) {
        self.cache.insert(
            filter.clone(),
            LargestAccountsCacheValue {
                accounts: accounts.to_owned(),
                slot,
                cached_time: SystemTime::now(),
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn set_largest_accounts_with_time(
        &mut self,
        filter: &Option<RpcLargestAccountsFilter>,
        slot: u64,
        accounts: &[RpcAccountBalance],
        cached_time: SystemTime,
    ) {
        self.cache.insert(
            filter.clone(),
            LargestAccountsCacheValue {
                accounts: accounts.to_owned(),
                slot,
                cached_time,
            },
        );
    }
}

/// Bounded coordinator for exactly the three filter-only keys:
/// `None`, `Some(Circulating)`, `Some(NonCirculating)`.
///
/// Only the filter is the key; commitment is not added.
/// Distinct keys can have three simultaneous producers; only a short
/// admission/state lock is held and never across computation.
pub(crate) struct LargestAccountsSingleflight {
    pub(crate) inner: Mutex<HashMap<Option<RpcLargestAccountsFilter>, Arc<InflightEntry>>>,
}

pub(crate) struct InflightEntry {
    pub(crate) sender: watch::Sender<Option<Result<(u64, Vec<RpcAccountBalance>), String>>>,
    // Keep one receiver alive so `sender.send` always updates the stored value even if
    // waiters have not yet subscribed. Without this, `watch::Sender::send` fails when
    // receiver count is 0 and the value is not updated, causing deadlock.
    _receiver: watch::Receiver<Option<Result<(u64, Vec<RpcAccountBalance>), String>>>,
}

impl LargestAccountsSingleflight {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Attempt to join or become producer for `key`.
    /// Returns `(is_producer, entry)` where `is_producer==true` means caller
    /// must spawn the owned producer task. Lock is held only for admission.
    pub(crate) fn get_or_create(
        &self,
        key: &Option<RpcLargestAccountsFilter>,
    ) -> (bool, Arc<InflightEntry>) {
        let mut map = self.inner.lock().unwrap();
        if let Some(entry) = map.get(key) {
            (false, Arc::clone(entry))
        } else {
            let (tx, rx) = watch::channel(None);
            let entry = Arc::new(InflightEntry {
                sender: tx,
                _receiver: rx,
            });
            map.insert(key.clone(), Arc::clone(&entry));
            (true, entry)
        }
    }

    #[cfg(test)]
    pub(crate) fn is_inflight(&self, key: &Option<RpcLargestAccountsFilter>) -> bool {
        self.inner.lock().unwrap().contains_key(key)
    }

    #[cfg(test)]
    pub(crate) fn inflight_count(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }
}

/// RAII lease for a singleflight producer. Constructed synchronously before `runtime.spawn`
/// so Drop runs even if the spawned future is never polled (aborted before first poll or runtime shutdown).
/// On Drop, if not marked completed, publishes terminal error and removes only the matching entry (generation check).
pub(crate) struct SingleflightLease {
    coordinator: Arc<LargestAccountsSingleflight>,
    key: Option<RpcLargestAccountsFilter>,
    entry: Arc<InflightEntry>,
    completed: AtomicBool,
}

impl SingleflightLease {
    pub(crate) fn new(
        coordinator: Arc<LargestAccountsSingleflight>,
        key: Option<RpcLargestAccountsFilter>,
        entry: Arc<InflightEntry>,
    ) -> Self {
        Self {
            coordinator,
            key,
            entry,
            completed: AtomicBool::new(false),
        }
    }

    pub(crate) fn mark_completed(&self) {
        self.completed.store(true, Ordering::SeqCst);
    }
}

impl Drop for SingleflightLease {
    fn drop(&mut self) {
        if self.completed.load(Ordering::SeqCst) {
            return;
        }
        // Abnormal termination: wake waiters and clean only matching generation.
        let _ = self
            .entry
            .sender
            .send(Some(Err("producer task aborted".to_string())));
        let mut map = self.coordinator.inner.lock().unwrap();
        if let Some(existing) = map.get(&self.key) {
            if Arc::ptr_eq(existing, &self.entry) {
                map.remove(&self.key);
            }
        }
    }
}

impl InflightEntry {
    /// Multi-waiter completion primitive. Waiter cancellation (dropping this future)
    /// does not affect producer or other waiters. If the watch channel is closed
    /// (abnormal producer termination), returns an error that is retryable for
    /// future requests and does not leave stale in-flight state.
    pub(crate) async fn wait(&self) -> Result<(u64, Vec<RpcAccountBalance>), String> {
        let mut rx = self.sender.subscribe();
        if let Some(res) = &*rx.borrow() {
            return res.clone();
        }
        loop {
            match rx.changed().await {
                Ok(()) => {
                    if let Some(res) = &*rx.borrow() {
                        return res.clone();
                    }
                }
                Err(_) => {
                    // Sender dropped without publishing -> abnormal termination.
                    // No deadlock; future requests can elect a new producer.
                    return Err("producer task aborted".to_string());
                }
            }
        }
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn test_old_entries_expire() {
        let mut cache = LargestAccountsCache::new(1);

        let filter = Some(RpcLargestAccountsFilter::Circulating);

        let accounts: Vec<RpcAccountBalance> = Vec::new();

        cache.set_largest_accounts(&filter, 1000, &accounts);
        std::thread::sleep(Duration::from_secs(1));
        assert_eq!(cache.get_largest_accounts(&filter), None);
    }

    // Additional coordinator tests are in rpc.rs integration tests; keep cache test here.
    #[tokio::test]
    async fn test_singleflight_coalesces_same_key() {
        let sf = LargestAccountsSingleflight::new();
        let key = Some(RpcLargestAccountsFilter::Circulating);
        let (is_prod1, e1) = sf.get_or_create(&key);
        assert!(is_prod1);
        assert_eq!(sf.inflight_count(), 1);
        let (is_prod2, e2) = sf.get_or_create(&key);
        assert!(!is_prod2);
        assert!(Arc::ptr_eq(&e1, &e2));

        // Simulate producer publish
        let accounts = vec![RpcAccountBalance {
            address: "A".to_string(),
            lamports: 1,
        }];
        let _ = e1.sender.send(Some(Ok((100, accounts.clone()))));

        // Waiters get same result
        let r1 = e1.wait().await.unwrap();
        let r2 = e2.wait().await.unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r1.0, 100);
    }

    #[tokio::test]
    async fn test_singleflight_distinct_keys_simultaneous() {
        let sf = LargestAccountsSingleflight::new();
        let k_none: Option<RpcLargestAccountsFilter> = None;
        let k_circ = Some(RpcLargestAccountsFilter::Circulating);
        let k_non = Some(RpcLargestAccountsFilter::NonCirculating);
        let (p1, _) = sf.get_or_create(&k_none);
        let (p2, _) = sf.get_or_create(&k_circ);
        let (p3, _) = sf.get_or_create(&k_non);
        assert!(p1 && p2 && p3);
        assert_eq!(sf.inflight_count(), 3);
        // Same keys should be waiters, not new producers
        let (p1b, _) = sf.get_or_create(&k_none);
        assert!(!p1b);
        assert_eq!(sf.inflight_count(), 3);
    }

    #[tokio::test]
    async fn test_singleflight_abnormal_closes() {
        let sf = Arc::new(LargestAccountsSingleflight::new());
        let key: Option<RpcLargestAccountsFilter> = None;
        let (is_prod, entry) = sf.get_or_create(&key);
        assert!(is_prod);
        assert!(sf.is_inflight(&key));
        // Simulate abnormal termination: drop sender without send + remove map (RAII)
        {
            let sf2 = Arc::clone(&sf);
            let ent2 = Arc::clone(&entry);
            // Mimic Guard drop: send abort error then remove
            let _ = ent2.sender.send(Some(Err("producer task aborted".to_string())));
            sf2.inner.lock().unwrap().remove(&key);
            // drop sender by not holding? keep entry alive for waiter
        }
        // Waiter should not deadlock, gets error
        let res = entry.wait().await;
        assert!(res.is_err());
        assert!(!sf.is_inflight(&key));
        // Future request can retry
        let (is_prod2, _) = sf.get_or_create(&key);
        assert!(is_prod2);
    }
}
