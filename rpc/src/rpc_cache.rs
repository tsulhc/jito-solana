use {
    solana_rpc_client_api::{config::RpcLargestAccountsFilter, response::RpcAccountBalance},
    std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum GenerationState {
    Active = 0,
    Aborting = 1,
    Completing = 2,
    Terminal = 3,
}

pub(crate) struct InflightEntry {
    pub(crate) sender: watch::Sender<Option<Result<(u64, Vec<RpcAccountBalance>), String>>>,
    // Keep one receiver alive so `sender.send` always updates the stored value even if
    // waiters have not yet subscribed. Without this, `watch::Sender::send` fails when
    // receiver count is 0 and the value is not updated, causing deadlock.
    _receiver: watch::Receiver<Option<Result<(u64, Vec<RpcAccountBalance>), String>>>,
    abort: Arc<AtomicBool>,
    state: AtomicU8,
    waiters: AtomicUsize,
}

pub(crate) struct SingleflightAdmission {
    pub(crate) is_producer: bool,
    pub(crate) entry: Arc<InflightEntry>,
    pub(crate) waiter: SingleflightWaiterLease,
}

/// RAII ownership for one real RPC caller waiting on a generation.
pub(crate) struct SingleflightWaiterLease {
    coordinator: Arc<LargestAccountsSingleflight>,
    key: Option<RpcLargestAccountsFilter>,
    entry: Arc<InflightEntry>,
}

impl InflightEntry {
    fn state(&self) -> GenerationState {
        match self.state.load(Ordering::SeqCst) {
            0 => GenerationState::Active,
            1 => GenerationState::Aborting,
            2 => GenerationState::Completing,
            3 => GenerationState::Terminal,
            value => panic!("invalid largest-accounts generation state: {value}"),
        }
    }

    pub(crate) fn abort_token(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.abort)
    }
}

impl LargestAccountsSingleflight {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Admit one real RPC caller to an active generation or return a retiring
    /// generation that must terminate before a new same-key producer is admitted.
    pub(crate) fn get_or_create_with_waiter(
        self: &Arc<Self>,
        key: &Option<RpcLargestAccountsFilter>,
    ) -> Result<SingleflightAdmission, Arc<InflightEntry>> {
        let mut map = self.inner.lock().unwrap();
        loop {
            if let Some(entry) = map.get(key).cloned() {
                match entry.state() {
                    GenerationState::Active => {
                        entry.waiters.fetch_add(1, Ordering::SeqCst);
                        return Ok(SingleflightAdmission {
                            is_producer: false,
                            entry,
                            waiter: SingleflightWaiterLease {
                                coordinator: Arc::clone(self),
                                key: key.clone(),
                                entry: map.get(key).unwrap().clone(),
                            },
                        });
                    }
                    GenerationState::Aborting | GenerationState::Completing => {
                        return Err(entry);
                    }
                    GenerationState::Terminal => {
                        map.remove(key);
                    }
                }
            } else {
                let (tx, rx) = watch::channel(None);
                let entry = Arc::new(InflightEntry {
                    sender: tx,
                    _receiver: rx,
                    abort: Arc::new(AtomicBool::new(false)),
                    state: AtomicU8::new(GenerationState::Active as u8),
                    waiters: AtomicUsize::new(1),
                });
                map.insert(key.clone(), Arc::clone(&entry));
                return Ok(SingleflightAdmission {
                    is_producer: true,
                    entry: Arc::clone(&entry),
                    waiter: SingleflightWaiterLease {
                        coordinator: Arc::clone(self),
                        key: key.clone(),
                        entry,
                    },
                });
            }
        }
    }

    /// Untracked admission retained only for deterministic unit tests that manually
    /// model completion. Production callers must use `get_or_create_with_waiter`.
    #[cfg(test)]
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
                abort: Arc::new(AtomicBool::new(false)),
                state: AtomicU8::new(GenerationState::Active as u8),
                waiters: AtomicUsize::new(0),
            });
            map.insert(key.clone(), Arc::clone(&entry));
            (true, entry)
        }
    }

    fn try_commit_success(
        &self,
        key: &Option<RpcLargestAccountsFilter>,
        entry: &Arc<InflightEntry>,
    ) -> bool {
        let map = self.inner.lock().unwrap();
        let Some(existing) = map.get(key) else {
            return false;
        };
        if !Arc::ptr_eq(existing, entry) {
            return false;
        }
        existing
            .state
            .compare_exchange(
                GenerationState::Active as u8,
                GenerationState::Completing as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }

    fn publish_and_remove(
        &self,
        key: &Option<RpcLargestAccountsFilter>,
        entry: &Arc<InflightEntry>,
        result: Result<(u64, Vec<RpcAccountBalance>), String>,
    ) {
        let mut map = self.inner.lock().unwrap();
        if let Some(existing) = map.get(key)
            && Arc::ptr_eq(existing, entry)
        {
            existing
                .state
                .store(GenerationState::Terminal as u8, Ordering::SeqCst);
            let _ = existing.sender.send(Some(result));
            map.remove(key);
        } else {
            let _ = entry.sender.send(Some(result));
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

    pub(crate) fn try_commit_success(&self) -> bool {
        self.coordinator.try_commit_success(&self.key, &self.entry)
    }

    pub(crate) fn publish_and_remove(&self, result: Result<(u64, Vec<RpcAccountBalance>), String>) {
        self.coordinator
            .publish_and_remove(&self.key, &self.entry, result);
    }
}

impl Drop for SingleflightLease {
    fn drop(&mut self) {
        if self.completed.load(Ordering::SeqCst) {
            return;
        }
        // Abnormal termination: wake waiters and clean only matching generation.
        self.publish_and_remove(Err("producer task aborted".to_string()));
    }
}

impl Drop for SingleflightWaiterLease {
    fn drop(&mut self) {
        let map = self.coordinator.inner.lock().unwrap();
        let Some(existing) = map.get(&self.key) else {
            return;
        };
        if !Arc::ptr_eq(existing, &self.entry) {
            return;
        }
        let previous = self.entry.waiters.fetch_sub(1, Ordering::SeqCst);
        if previous == 1
            && self
                .entry
                .state
                .compare_exchange(
                    GenerationState::Active as u8,
                    GenerationState::Aborting as u8,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
        {
            self.entry.abort.store(true, Ordering::SeqCst);
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
            let _ = ent2
                .sender
                .send(Some(Err("producer task aborted".to_string())));
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

    #[test]
    fn test_waiter_drop_aborts_only_after_last_real_waiter() {
        let coordinator = Arc::new(LargestAccountsSingleflight::new());
        let key = Some(RpcLargestAccountsFilter::Circulating);
        let producer = coordinator
            .get_or_create_with_waiter(&key)
            .unwrap_or_else(|_| panic!("initial admission must succeed"));
        let waiter = coordinator
            .get_or_create_with_waiter(&key)
            .unwrap_or_else(|_| panic!("waiter admission must succeed"));
        let entry = Arc::clone(&producer.entry);

        drop(producer);
        assert!(!entry.abort_token().load(Ordering::SeqCst));
        drop(waiter);
        assert!(entry.abort_token().load(Ordering::SeqCst));
        assert_eq!(entry.state(), GenerationState::Aborting);

        let Err(retiring) = coordinator.get_or_create_with_waiter(&key) else {
            panic!("an aborting generation must not admit a new producer");
        };
        assert!(Arc::ptr_eq(&retiring, &entry));
        coordinator.publish_and_remove(&key, &entry, Err("aborted".to_string()));

        let replacement = coordinator
            .get_or_create_with_waiter(&key)
            .unwrap_or_else(|_| panic!("replacement admission must succeed"));
        assert!(replacement.is_producer);
        let replacement_entry = Arc::clone(&replacement.entry);
        drop(replacement);
        coordinator.publish_and_remove(&key, &replacement_entry, Err("test cleanup".to_string()));
    }

    #[tokio::test]
    async fn test_new_caller_waits_for_aborting_generation_to_terminate() {
        let coordinator = Arc::new(LargestAccountsSingleflight::new());
        let key = Some(RpcLargestAccountsFilter::NonCirculating);
        let first = coordinator
            .get_or_create_with_waiter(&key)
            .unwrap_or_else(|_| panic!("initial admission must succeed"));
        let entry = Arc::clone(&first.entry);
        drop(first);
        assert!(entry.abort_token().load(Ordering::SeqCst));

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let coordinator_clone = Arc::clone(&coordinator);
        let key_clone = key.clone();
        let caller = tokio::spawn(async move {
            started_tx.send(()).unwrap();
            let Err(retiring) = coordinator_clone.get_or_create_with_waiter(&key_clone) else {
                panic!("caller must wait for the retiring generation");
            };
            retiring.wait().await.unwrap_err();
            coordinator_clone
                .get_or_create_with_waiter(&key_clone)
                .unwrap_or_else(|_| panic!("replacement admission must succeed"))
        });

        started_rx.await.unwrap();
        tokio::task::yield_now().await;
        assert!(!caller.is_finished());

        coordinator.publish_and_remove(&key, &entry, Err("aborted".to_string()));
        let replacement = caller.await.unwrap();
        assert!(replacement.is_producer);
        let replacement_entry = Arc::clone(&replacement.entry);
        drop(replacement);
        coordinator.publish_and_remove(&key, &replacement_entry, Err("test cleanup".to_string()));
    }

    #[test]
    fn test_completion_wins_over_waiter_drop() {
        let coordinator = Arc::new(LargestAccountsSingleflight::new());
        let key = Some(RpcLargestAccountsFilter::Circulating);
        let producer = coordinator
            .get_or_create_with_waiter(&key)
            .unwrap_or_else(|_| panic!("initial admission must succeed"));
        let waiter = coordinator
            .get_or_create_with_waiter(&key)
            .unwrap_or_else(|_| panic!("waiter admission must succeed"));
        let entry = Arc::clone(&producer.entry);
        let lease =
            SingleflightLease::new(Arc::clone(&coordinator), key.clone(), Arc::clone(&entry));

        assert!(lease.try_commit_success());
        drop(waiter);
        assert!(!entry.abort_token().load(Ordering::SeqCst));
        assert_eq!(entry.state(), GenerationState::Completing);

        lease.publish_and_remove(Ok((1, vec![])));
        lease.mark_completed();
        drop(producer);
        assert!(coordinator.is_empty());
    }
}
