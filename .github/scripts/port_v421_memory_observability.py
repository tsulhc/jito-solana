#!/usr/bin/env python3
from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    if new in text:
        return
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected exactly one anchor, found {count}: {old[:120]!r}")
    p.write_text(text.replace(old, new, 1))


# Accounts scan lifecycle: keep the native v4.2 ScanGuard and add only lock-free hooks.
replace_once(
    "accounts-db/src/accounts_scan.rs",
    """                scan_tracker
                    .max_distance_to_min_scan_slot
                    .fetch_max(current, Ordering::Relaxed);
""",
    """                scan_tracker
                    .max_distance_to_min_scan_slot
                    .fetch_max(current, Ordering::Relaxed);
                solana_metrics::pull_metrics().record_accounts_scan_root_distance(current);
""",
)
replace_once(
    "accounts-db/src/accounts_scan.rs",
    """        scan_tracker.active_scans.fetch_add(1, Ordering::Relaxed);
        Some(Self {
""",
    """        scan_tracker.active_scans.fetch_add(1, Ordering::Relaxed);
        solana_metrics::pull_metrics().record_accounts_scan_start();
        Some(Self {
""",
)
replace_once(
    "accounts-db/src/accounts_scan.rs",
    """        self.scan_tracker
            .active_scans
            .fetch_sub(1, Ordering::Relaxed);
        let mut ongoing_scan_roots = self.scan_tracker.ongoing_scan_roots.write().unwrap();
""",
    """        self.scan_tracker
            .active_scans
            .fetch_sub(1, Ordering::Relaxed);
        solana_metrics::pull_metrics().record_accounts_scan_complete();
        let mut ongoing_scan_roots = self.scan_tracker.ongoing_scan_roots.write().unwrap();
""",
)

# AccountsIndex: publish the values already calculated by the native 10s stats report.
replace_once(
    "accounts-db/src/accounts_index/stats.rs",
    """        let count_in_mem = self.count_in_mem.load(Ordering::Relaxed);
        let capacity_in_mem = self.capacity_in_mem.load(Ordering::Relaxed);

        // sum of elapsed time in each thread
""",
    """        let count_in_mem = self.count_in_mem.load(Ordering::Relaxed);
        let capacity_in_mem = self.capacity_in_mem.load(Ordering::Relaxed);
        let pull_metrics = solana_metrics::pull_metrics();
        pull_metrics
            .accounts_index_count_in_mem
            .store(count_in_mem as u64, Ordering::Relaxed);
        pull_metrics
            .accounts_index_capacity_in_mem
            .store(capacity_in_mem as u64, Ordering::Relaxed);

        // sum of elapsed time in each thread
""",
)
replace_once(
    "accounts-db/src/accounts_index/stats.rs",
    """                    * size_of::<SlotListItem<T>>() // <-- size of one slot list entry
                    * 2; // <-- and assume there are two entries
            datapoint_info!(
""",
    """                    * size_of::<SlotListItem<T>>() // <-- size of one slot list entry
                    * 2; // <-- and assume there are two entries
            pull_metrics
                .accounts_index_estimate_mem_bytes
                .store(estimate_mem_bytes as u64, Ordering::Relaxed);
            datapoint_info!(
""",
)
replace_once(
    "accounts-db/src/accounts_index/stats.rs",
    """        } else {
            datapoint_info!(
                datapoint_name,
                (
                    \"estimate_mem_bytes\",
                    (
                        // hash map mem usage is based on capacity, and the footprint of a KV-pair
                        // (we ignore other hash map details, such as load factor)
                        capacity_in_mem * InMemAccountsIndex::<T, U>::size_of_uninitialized()
                        // each value in use we assume has a single entry in the slot list
                        + count_in_mem * InMemAccountsIndex::<T, U>::size_of_single_entry()
                    ),
                    i64
                ),
""",
    """        } else {
            let estimate_mem_bytes =
                // hash map mem usage is based on capacity, and the footprint of a KV-pair
                // (we ignore other hash map details, such as load factor)
                capacity_in_mem * InMemAccountsIndex::<T, U>::size_of_uninitialized()
                // each value in use we assume has a single entry in the slot list
                + count_in_mem * InMemAccountsIndex::<T, U>::size_of_single_entry();
            pull_metrics
                .accounts_index_estimate_mem_bytes
                .store(estimate_mem_bytes as u64, Ordering::Relaxed);
            datapoint_info!(
                datapoint_name,
                (\"estimate_mem_bytes\", estimate_mem_bytes, i64),
""",
)

# RPC crate gets jemalloc access only on platforms where the allocator telemetry exists.
replace_once(
    "rpc/Cargo.toml",
    """wincode = { workspace = true }

[dev-dependencies]
""",
    """wincode = { workspace = true }

[target.'cfg(not(any(target_env = \"msvc\", target_os = \"freebsd\")))'.dependencies]
jemalloc-ctl = { workspace = true }

[dev-dependencies]
""",
)

# RPC imports and metrics middleware. jsonrpc-core remains 18.0.0 in v4.2.1.
replace_once(
    "rpc/src/rpc_service.rs",
    """    jsonrpc_core::{MetaIoHandler, futures::prelude::*},
""",
    """    jsonrpc_core::{Call, MetaIoHandler, futures::prelude::*, middleware::Middleware},
""",
)
replace_once(
    "rpc/src/rpc_service.rs",
    """};

const FULL_SNAPSHOT_REQUEST_PATH: &str = \"/snapshot.tar.bz2\";
""",
    """};

#[derive(Clone, Default)]
struct RpcMetricsMiddleware;

struct RpcInFlightGuard {
    slot: usize,
}

impl Drop for RpcInFlightGuard {
    fn drop(&mut self) {
        solana_metrics::pull_metrics().finish_rpc_request(self.slot);
    }
}

impl Middleware<JsonRpcRequestProcessor> for RpcMetricsMiddleware {
    type Future = Pin<Box<dyn Future<Output = Option<jsonrpc_core::Response>> + Send>>;
    type CallFuture = Pin<Box<dyn Future<Output = Option<jsonrpc_core::Output>> + Send>>;

    fn on_call<F, X>(
        &self,
        call: Call,
        meta: JsonRpcRequestProcessor,
        next: F,
    ) -> jsonrpc_core::futures::future::Either<Self::CallFuture, X>
    where
        F: Fn(Call, JsonRpcRequestProcessor) -> X + Send + Sync,
        X: Future<Output = Option<jsonrpc_core::Output>> + Send + 'static,
    {
        let method = match &call {
            Call::MethodCall(call) => call.method.as_str(),
            Call::Notification(call) => call.method.as_str(),
            Call::Invalid { .. } => \"other\",
        };
        let slot = solana_metrics::pull_metrics::rpc_method_slot(method);
        solana_metrics::pull_metrics().record_rpc_request(slot);
        let guard = RpcInFlightGuard { slot };
        let started_at = Instant::now();
        let result = next(call, meta);
        jsonrpc_core::futures::future::Either::Left(Box::pin(async move {
            let result = result.await;
            let success = result.as_ref().map(|output| match output {
                jsonrpc_core::Output::Success(_) => true,
                jsonrpc_core::Output::Failure(_) => false,
            });
            solana_metrics::pull_metrics().record_rpc_completion(
                slot,
                started_at.elapsed(),
                success,
            );
            drop(guard);
            result
        }))
    }
}

const FULL_SNAPSHOT_REQUEST_PATH: &str = \"/snapshot.tar.bz2\";
""",
)

# Service lifecycle gains a separate optional metrics listener.
replace_once(
    "rpc/src/rpc_service.rs",
    """pub struct JsonRpcService {
    thread_hdl: JoinHandle<()>,

    #[cfg(test)]
""",
    """pub struct JsonRpcService {
    thread_hdl: JoinHandle<()>,
    metrics_thread_hdl: Option<JoinHandle<()>>,

    #[cfg(test)]
""",
)
replace_once(
    "rpc/src/rpc_service.rs",
    """    close_handle: Option<CloseHandle>,

    client_updater: Arc<dyn NotifyKeyUpdate + Send + Sync>,
""",
    """    close_handle: Option<CloseHandle>,
    metrics_close_handle: Option<CloseHandle>,

    client_updater: Arc<dyn NotifyKeyUpdate + Send + Sync>,
""",
)

metrics_middleware = r'''
struct MetricsRequestMiddleware;

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
fn refresh_jemalloc_metrics() -> Result<(), String> {
    use jemalloc_ctl::{epoch, stats};

    epoch::mib()
        .map_err(|error| error.to_string())?
        .advance()
        .map_err(|error| error.to_string())?;
    let allocated = u64::try_from(
        stats::allocated::mib()
            .map_err(|error| error.to_string())?
            .read()
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let active = u64::try_from(
        stats::active::mib()
            .map_err(|error| error.to_string())?
            .read()
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let resident = u64::try_from(
        stats::resident::mib()
            .map_err(|error| error.to_string())?
            .read()
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let retained = u64::try_from(
        stats::retained::mib()
            .map_err(|error| error.to_string())?
            .read()
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    solana_metrics::pull_metrics().store_jemalloc_stats(allocated, active, resident, retained);
    Ok(())
}

#[cfg(any(target_env = "msvc", target_os = "freebsd"))]
fn refresh_jemalloc_metrics() -> Result<(), String> {
    Ok(())
}

impl MetricsRequestMiddleware {
    fn response(status: hyper::StatusCode) -> RequestMiddlewareAction {
        RequestMiddlewareAction::Respond {
            should_validate_hosts: false,
            response: Box::pin(async move {
                Ok(hyper::Response::builder()
                    .status(status)
                    .body(hyper::Body::empty())
                    .unwrap())
            }),
        }
    }
}

impl RequestMiddleware for MetricsRequestMiddleware {
    fn on_request(&self, request: hyper::Request<hyper::Body>) -> RequestMiddlewareAction {
        if request.method() == hyper::Method::GET && request.uri().path() == "/metrics" {
            RequestMiddlewareAction::Respond {
                should_validate_hosts: false,
                response: Box::pin(async {
                    if let Err(error) = refresh_jemalloc_metrics() {
                        warn!("read jemalloc stats: {error}");
                    }
                    Ok(hyper::Response::builder()
                        .status(hyper::StatusCode::OK)
                        .header(hyper::header::CONTENT_TYPE, "text/plain; version=0.0.4")
                        .header(hyper::header::CACHE_CONTROL, "no-store")
                        .body(hyper::Body::from(solana_metrics::pull_metrics_exposition()))
                        .unwrap())
                }),
            }
        } else if request.uri().path() == "/metrics" {
            Self::response(hyper::StatusCode::METHOD_NOT_ALLOWED)
        } else {
            Self::response(hyper::StatusCode::NOT_FOUND)
        }
    }
}

'''
replace_once(
    "rpc/src/rpc_service.rs",
    """fn match_supply_path(path: &str) -> Option<&str> {
""",
    metrics_middleware + "fn match_supply_path(path: &str) -> Option<&str> {\n",
)

replace_once(
    "rpc/src/rpc_service.rs",
    """pub struct JsonRpcServiceConfig<'a> {
    pub rpc_addr: SocketAddr,
    pub rpc_config: JsonRpcConfig,
""",
    """pub struct JsonRpcServiceConfig<'a> {
    pub rpc_addr: SocketAddr,
    pub metrics_addr: Option<SocketAddr>,
    pub rpc_config: JsonRpcConfig,
""",
)
replace_once(
    "rpc/src/rpc_service.rs",
    """        let json_rpc_service = Self::new(
            config.rpc_addr,
            config.rpc_config.clone(),
""",
    """        let json_rpc_service = Self::new(
            config.rpc_addr,
            config.metrics_addr,
            config.rpc_config.clone(),
""",
)
replace_once(
    "rpc/src/rpc_service.rs",
    """    >(
        rpc_addr: SocketAddr,
        config: JsonRpcConfig,
""",
    """    >(
        rpc_addr: SocketAddr,
        metrics_addr: Option<SocketAddr>,
        config: JsonRpcConfig,
""",
)
replace_once(
    "rpc/src/rpc_service.rs",
    """                let mut io = MetaIoHandler::default();
""",
    """                let mut io = MetaIoHandler::with_middleware(RpcMetricsMiddleware);
""",
)

replace_once(
    "rpc/src/rpc_service.rs",
    """        let close_handle = close_handle_receiver.recv().unwrap()?;
        let close_handle_ = close_handle.clone();
        validator_exit
            .write()
            .unwrap()
            .register_exit(Box::new(move || {
                close_handle_.close();
            }));
        Ok(Self {
            thread_hdl,
            #[cfg(test)]
            request_processor: test_request_processor,
            close_handle: Some(close_handle),
            client_updater: Arc::new(client) as Arc<dyn NotifyKeyUpdate + Send + Sync>,
        })
""",
    """        let close_handle = close_handle_receiver.recv().unwrap()?;
        let (metrics_thread_hdl, metrics_close_handle) = if let Some(metrics_addr) = metrics_addr {
            let (sender, receiver) = unbounded();
            let metrics_thread_hdl = Builder::new()
                .name(\"solMetricsSvc\".to_string())
                .spawn(move || {
                    let server = ServerBuilder::<()>::new(MetaIoHandler::default())
                        .threads(1)
                        .request_middleware(MetricsRequestMiddleware)
                        .start_http(&metrics_addr);
                    match server {
                        Ok(server) => {
                            sender.send(Ok(server.close_handle())).unwrap();
                            server.wait();
                        }
                        Err(error) => {
                            sender.send(Err(error.to_string())).unwrap();
                        }
                    }
                })
                .unwrap();
            match receiver.recv().unwrap() {
                Ok(close_handle) => (Some(metrics_thread_hdl), Some(close_handle)),
                Err(error) => {
                    close_handle.clone().close();
                    metrics_thread_hdl.join().unwrap();
                    thread_hdl.join().unwrap();
                    return Err(error);
                }
            }
        } else {
            (None, None)
        };
        let close_handle_ = close_handle.clone();
        let metrics_close_handle_ = metrics_close_handle.clone();
        validator_exit
            .write()
            .unwrap()
            .register_exit(Box::new(move || {
                close_handle_.close();
                if let Some(close_handle) = &metrics_close_handle_ {
                    close_handle.clone().close();
                }
            }));
        Ok(Self {
            thread_hdl,
            metrics_thread_hdl,
            #[cfg(test)]
            request_processor: test_request_processor,
            close_handle: Some(close_handle),
            metrics_close_handle,
            client_updater: Arc::new(client) as Arc<dyn NotifyKeyUpdate + Send + Sync>,
        })
""",
)
replace_once(
    "rpc/src/rpc_service.rs",
    """    pub fn exit(&mut self) {
        if let Some(c) = self.close_handle.take() {
            c.close()
        }
    }

    pub fn join(mut self) -> thread::Result<()> {
        self.exit();
        self.thread_hdl.join()
    }
""",
    """    pub fn exit(&mut self) {
        if let Some(c) = self.close_handle.take() {
            c.close()
        }
        if let Some(c) = self.metrics_close_handle.take() {
            c.close()
        }
    }

    pub fn join(mut self) -> thread::Result<()> {
        self.exit();
        self.thread_hdl.join()?;
        if let Some(thread_hdl) = self.metrics_thread_hdl {
            thread_hdl.join()?;
        }
        Ok(())
    }
""",
)
replace_once(
    "rpc/src/rpc_service.rs",
    """        let mut rpc_service = JsonRpcService::new(
            rpc_addr,
            json_rpc_config,
""",
    """        let mut rpc_service = JsonRpcService::new(
            rpc_addr,
            None,
            json_rpc_config,
""",
)

# Add request middleware tests before create_bank_forks.
metrics_tests = r'''
    #[test]
    fn test_json_rpc_listener_does_not_serve_metrics() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(Blockstore::open(ledger_path.path()).unwrap());
        let bank_forks = create_bank_forks();
        let optimistically_confirmed_bank =
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks);
        let middleware = RpcRequestMiddleware::new(
            ledger_path.path().to_path_buf(),
            None,
            bank_forks,
            RpcHealth::stub(optimistically_confirmed_bank, blockstore),
        );
        let request = hyper::Request::builder()
            .method(hyper::Method::GET)
            .uri("/metrics")
            .body(hyper::Body::empty())
            .unwrap();
        assert!(matches!(
            middleware.on_request(request),
            RequestMiddlewareAction::Proceed { .. }
        ));
    }

    #[test]
    fn test_metrics_listener_serves_only_get_metrics() {
        let runtime = Runtime::new().unwrap();
        let middleware = MetricsRequestMiddleware;
        let get = hyper::Request::builder()
            .method(hyper::Method::GET)
            .uri("/metrics")
            .body(hyper::Body::empty())
            .unwrap();
        let response = match middleware.on_request(get) {
            RequestMiddlewareAction::Respond { response, .. } => runtime.block_on(response).unwrap(),
            RequestMiddlewareAction::Proceed { .. } => panic!("metrics request proceeded"),
        };
        assert_eq!(response.status(), hyper::StatusCode::OK);

        for (method, path, status) in [
            (hyper::Method::POST, "/metrics", hyper::StatusCode::METHOD_NOT_ALLOWED),
            (hyper::Method::GET, "/", hyper::StatusCode::NOT_FOUND),
        ] {
            let request = hyper::Request::builder()
                .method(method)
                .uri(path)
                .body(hyper::Body::empty())
                .unwrap();
            let response = match middleware.on_request(request) {
                RequestMiddlewareAction::Respond { response, .. } => runtime.block_on(response).unwrap(),
                RequestMiddlewareAction::Proceed { .. } => panic!("metrics request proceeded"),
            };
            assert_eq!(response.status(), status);
        }
    }

'''
replace_once(
    "rpc/src/rpc_service.rs",
    """    fn create_bank_forks() -> Arc<RwLock<BankForks>> {
""",
    metrics_tests + "    fn create_bank_forks() -> Arc<RwLock<BankForks>> {\n",
)

# Validator config plumbing.
replace_once(
    "core/src/validator.rs",
    """    pub rpc_addrs: Option<(SocketAddr, SocketAddr)>, // (JsonRpc, JsonRpcPubSub)
    pub pubsub_config: PubSubConfig,
""",
    """    pub rpc_addrs: Option<(SocketAddr, SocketAddr)>, // (JsonRpc, JsonRpcPubSub)
    pub metrics_addr: Option<SocketAddr>,
    pub pubsub_config: PubSubConfig,
""",
)
replace_once(
    "core/src/validator.rs",
    """            rpc_addrs: None,
            pubsub_config: PubSubConfig::default_for_tests(),
""",
    """            rpc_addrs: None,
            metrics_addr: None,
            pubsub_config: PubSubConfig::default_for_tests(),
""",
)
replace_once(
    "core/src/validator.rs",
    """            let rpc_svc_config = JsonRpcServiceConfig {
                rpc_addr,
                rpc_config: config.rpc_config.clone(),
""",
    """            let rpc_svc_config = JsonRpcServiceConfig {
                rpc_addr,
                metrics_addr: config.metrics_addr,
                rpc_config: config.rpc_config.clone(),
""",
)
replace_once(
    "local-cluster/src/validator_configs.rs",
    """        rpc_addrs: config.rpc_addrs,
        pubsub_config: config.pubsub_config.clone(),
""",
    """        rpc_addrs: config.rpc_addrs,
        metrics_addr: config.metrics_addr,
        pubsub_config: config.pubsub_config.clone(),
""",
)

# CLI plumbing, preserving the v4.2 RunArgs structure.
replace_once(
    "validator/src/commands/run/args.rs",
    """    clap::{App, Arg, ArgMatches, values_t},
""",
    """    clap::{App, Arg, ArgMatches, value_t, values_t},
""",
)
replace_once(
    "validator/src/commands/run/args.rs",
    """    pub send_transaction_service_config: SendTransactionServiceConfig,
    pub filter_keys: HashSet<Pubkey>,
}
""",
    """    pub send_transaction_service_config: SendTransactionServiceConfig,
    pub filter_keys: HashSet<Pubkey>,
    pub metrics_addr: Option<SocketAddr>,
}
""",
)
replace_once(
    "validator/src/commands/run/args.rs",
    """            filter_keys: if matches.is_present(\"filter_keys\") {
                values_t!(matches, \"filter_keys\", Pubkey)?
                    .into_iter()
                    .collect()
            } else {
                HashSet::new()
            },
        })
""",
    """            filter_keys: if matches.is_present(\"filter_keys\") {
                values_t!(matches, \"filter_keys\", Pubkey)?
                    .into_iter()
                    .collect()
            } else {
                HashSet::new()
            },
            metrics_addr: if matches.is_present(\"metrics_bind_address\") {
                Some(value_t!(matches, \"metrics_bind_address\", SocketAddr)?)
            } else {
                None
            },
        })
""",
)
replace_once(
    "validator/src/commands/run/args.rs",
    """    .arg(
        Arg::with_name(\"private_rpc\")
            .long(\"private-rpc\")
            .takes_value(false)
            .help(\"Do not publish the RPC port for use by others\"),
    )
    .arg(
        Arg::with_name(\"no_port_check\")
""",
    """    .arg(
        Arg::with_name(\"private_rpc\")
            .long(\"private-rpc\")
            .takes_value(false)
            .help(\"Do not publish the RPC port for use by others\"),
    )
    .arg(
        Arg::with_name(\"metrics_bind_address\")
            .long(\"metrics-bind-address\")
            .value_name(\"HOST:PORT\")
            .takes_value(true)
            .validator(|value| {
                value
                    .parse::<SocketAddr>()
                    .map(|_| ())
                    .map_err(|err| format!(\"invalid metrics bind address: {err}\"))
            })
            .requires(\"rpc_port\")
            .help(
                \"Enable the Prometheus metrics listener on the given HOST:PORT. The listener \\
                 has no authentication and must only be bound to loopback or other private, \\
                 trusted interfaces.\",
            ),
    )
    .arg(
        Arg::with_name(\"no_port_check\")
""",
)

# Production config gets the parsed address. The rpc_addrs block remains native v4.2.
replace_once(
    "validator/src/commands/run/execute.rs",
    """        rpc_config: run_args.json_rpc_config,
        on_start_geyser_plugin_config_files,
""",
    """        rpc_config: run_args.json_rpc_config,
        metrics_addr: run_args.metrics_addr,
        on_start_geyser_plugin_config_files,
""",
)

print("v4.2.1 observability port applied")
