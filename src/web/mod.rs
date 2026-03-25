// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, bail};
use axum::{
    Router,
    routing::{get, post},
};
use chrono::Utc;
use log::{info, warn};
use std::{net::SocketAddr, path::PathBuf, sync::Arc, thread, time::Instant};

use crate::DEFAULT_STDLIB_FN_TIMELINE_ROUTE;
use crate::app;
use crate::query::{
    build_stdlib_file_action_graph_dataset, build_stdlib_g8r_vs_yosys_dataset,
    build_unprocessed_version_rows, load_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index,
    load_ir_fn_corpus_g8r_vs_yosys_dataset_index, load_stdlib_fn_version_timeline_dataset_index,
    load_stdlib_fns_trend_dataset_index, load_stdlib_g8r_vs_yosys_dataset_index,
    load_versions_cards_index, parse_stdlib_fn_route_selector,
    rebuild_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index,
    rebuild_ir_fn_corpus_g8r_vs_yosys_dataset_index,
    rebuild_stdlib_fn_version_timeline_dataset_index, rebuild_stdlib_fns_trend_dataset_index,
    rebuild_stdlib_g8r_vs_yosys_dataset_index, rebuild_versions_cards_index,
};
use crate::queue::{list_queue_files, reclaim_expired_running_leases};
use crate::service::default_worker_id;
use crate::snapshot::load_static_snapshot_manifest;
use crate::store::ArtifactStore;
use crate::view::{QueueLiveStatusView, StdlibMetric, StdlibTrendKind};

pub(crate) mod cache;
mod render;
mod routes_action;
mod routes_api;
mod routes_corpus;
mod routes_db;
mod routes_dslx;
mod routes_versions;
pub(crate) mod types;

use crate::web::cache::WebUiCache;
use render::{
    render_stdlib_file_action_graph_html, render_stdlib_fn_version_timeline_html,
    render_stdlib_fns_trend_html, render_versions_html,
};
use routes_action::{web_action_detail, web_action_output_file};
use routes_api::web_jsonrpc;
use routes_corpus::{
    web_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc,
    web_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_redirect, web_ir_fn_corpus_g8r_vs_yosys_abc,
    web_ir_fn_corpus_g8r_vs_yosys_abc_redirect, web_ir_fn_corpus_k3_losses_vs_crate_version,
    web_ir_fn_corpus_k3_losses_vs_crate_version_redirect, web_ir_fn_corpus_structural,
    web_ir_fn_corpus_structural_download, web_ir_fn_corpus_structural_group,
    web_ir_fn_corpus_structural_redirect,
};
use routes_db::{web_db_size, web_db_size_redirect};
use routes_dslx::{
    web_stdlib_file_action_graph, web_stdlib_file_action_graph_redirect,
    web_stdlib_fn_version_history, web_stdlib_fn_version_history_root_redirect, web_stdlib_fns_g8r,
    web_stdlib_fns_g8r_redirect, web_stdlib_fns_g8r_vs_yosys_abc,
    web_stdlib_fns_g8r_vs_yosys_abc_redirect, web_stdlib_fns_yosys_abc,
    web_stdlib_fns_yosys_abc_redirect, web_stdlib_sample_details,
};
use routes_versions::{
    web_enqueue_crate_version, web_favicon_ico_redirect, web_favicon_svg, web_queue_status,
    web_root, web_runner_pause, web_runner_resume, web_versions, web_versions_redirect,
};
use types::{WebRunnerConfig, WebRunnerControl, WebUiState};

fn env_flag_enabled(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(raw) => {
            let lowered = raw.trim().to_ascii_lowercase();
            !matches!(lowered.as_str(), "0" | "false" | "no" | "off")
        }
        Err(_) => default,
    }
}

fn env_usize_at_least_one(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn start_embedded_web_runner(
    store: Arc<ArtifactStore>,
    repo_root: PathBuf,
    base_worker_id: &str,
    control: Arc<WebRunnerControl>,
    runner_config: &WebRunnerConfig,
) -> Result<()> {
    if runner_config.lease_seconds <= 0 {
        bail!(
            "--runner-lease-seconds must be > 0, got {}",
            runner_config.lease_seconds
        );
    }
    if runner_config.drain_batch_size == 0 {
        bail!("--runner-batch-size must be > 0");
    }
    if runner_config.poll_interval.is_zero() {
        bail!("--runner-poll-millis must be > 0");
    }
    if runner_config.worker_count == 0 {
        bail!("--runner-workers must be > 0");
    }

    for worker_index in 0..runner_config.worker_count {
        let store = store.clone();
        let repo_root = repo_root.clone();
        let control = control.clone();
        let worker_id = format!("{}:web-runner-{}", base_worker_id, worker_index);
        let lease_seconds = runner_config.lease_seconds;
        let poll_interval = runner_config.poll_interval;
        let drain_batch_size = runner_config.drain_batch_size;
        thread::spawn(move || {
            loop {
                if control.is_paused() {
                    if let Err(err) = reclaim_expired_running_leases(&store) {
                        control.mark_sync_error(format!(
                            "failed reclaiming expired running leases during pause: {:#}",
                            err
                        ));
                    }
                    match list_queue_files(&store.queue_running_dir()) {
                        Ok(running) if running.is_empty() => {
                            if control.is_sync_pending() {
                                match store.flush_durable() {
                                    Ok(()) => control.mark_sync_success(),
                                    Err(err) => control.mark_sync_error(format!("{:#}", err)),
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(err) => control.mark_sync_error(format!(
                            "failed checking running queue during pause: {:#}",
                            err
                        )),
                    }
                    thread::sleep(poll_interval);
                    continue;
                }
                match app::drain_queue(
                    &store,
                    &repo_root,
                    Some(drain_batch_size),
                    &worker_id,
                    lease_seconds,
                    true,
                    Some(control.pause_flag()),
                ) {
                    Ok(0) => thread::sleep(poll_interval),
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("embedded queue runner ({}) error: {:#}", worker_id, err);
                        thread::sleep(poll_interval);
                    }
                }
            }
        });
    }
    Ok(())
}

pub(crate) fn serve_web_ui(
    shared_store: Arc<ArtifactStore>,
    repo_root: PathBuf,
    _artifacts_sled_db_path: PathBuf,
    bind: &str,
    runner_config: WebRunnerConfig,
) -> Result<()> {
    let addr: SocketAddr = bind
        .parse()
        .with_context(|| format!("parsing --bind socket address: {}", bind))?;
    shared_store.ensure_layout()?;
    let snapshot_manifest = if shared_store.is_snapshot_backend() {
        let snapshot_dir = shared_store
            .snapshot_dir()
            .ok_or_else(|| anyhow::anyhow!("snapshot backend without snapshot_dir"))?;
        Some(
            load_static_snapshot_manifest(&snapshot_dir).with_context(|| {
                format!(
                    "loading static snapshot manifest from {}",
                    snapshot_dir.display()
                )
            })?,
        )
    } else {
        None
    };
    if snapshot_manifest.is_some() && runner_config.enabled {
        bail!("snapshot web mode does not support embedded queue runner");
    }
    let shared_cache = Arc::new(WebUiCache::new(runner_config.enabled));
    let heavy_route_limit = Arc::new(tokio::sync::Semaphore::new(env_usize_at_least_one(
        "BVC_WEB_HEAVY_MAX_INFLIGHT",
        2,
    )));
    let runner_control = if runner_config.enabled {
        Some(Arc::new(WebRunnerControl::new()))
    } else {
        None
    };
    let runner_owner_prefix = if runner_config.enabled {
        let base_worker_id = default_worker_id();
        let control = runner_control
            .clone()
            .expect("runner control should be present when runner enabled");
        start_embedded_web_runner(
            shared_store.clone(),
            repo_root.clone(),
            &base_worker_id,
            control,
            &runner_config,
        )?;
        Some(format!("{}:web-runner-", base_worker_id))
    } else {
        None
    };
    let state = WebUiState {
        store: shared_store,
        cache: shared_cache,
        heavy_route_limit,
        repo_root,
        snapshot_manifest,
        runner_enabled: runner_config.enabled,
        runner_owner_prefix,
        runner_control,
    };
    if !runner_config.enabled
        && state.snapshot_manifest.is_none()
        && env_flag_enabled("BVC_WEB_PREWARM", true)
    {
        let prewarm_store = state.store.clone();
        let prewarm_repo_root = state.repo_root.clone();
        let prewarm_cache = state.cache.clone();
        thread::spawn(move || {
            let versions_started = Instant::now();
            match (|| -> Result<()> {
                let report = if let Some(indexed) = load_versions_cards_index(&prewarm_store)? {
                    indexed
                } else {
                    let summary = rebuild_versions_cards_index(&prewarm_store, &prewarm_repo_root)?;
                    info!(
                        "web cache prewarm /versions rebuilt versions summary index cards={} unattributed={} index_bytes={} elapsed_ms={}",
                        summary.card_count,
                        summary.unattributed_actions,
                        summary.index_bytes,
                        summary.elapsed_ms
                    );
                    load_versions_cards_index(&prewarm_store)?.ok_or_else(|| {
                        anyhow::anyhow!(
                            "versions summary index rebuild completed but index remained unavailable"
                        )
                    })?
                };
                let unprocessed = build_unprocessed_version_rows(
                    &prewarm_store,
                    &prewarm_repo_root,
                    &report.cards,
                )?;
                let live_status = QueueLiveStatusView {
                    updated_utc: Utc::now(),
                    pending: 0,
                    pending_expanders: 0,
                    pending_non_expanders: 0,
                    pending_is_lower_bound: false,
                    running: 0,
                    running_expanders: 0,
                    running_non_expanders: 0,
                    running_owned: 0,
                    running_foreign: 0,
                    runner_enabled: false,
                    runner_paused: false,
                    runner_drained: false,
                    runner_sync_pending: false,
                    runner_last_sync_utc: None,
                    runner_last_sync_error: None,
                    failed: 0,
                    canceled: 0,
                    done: 0,
                    running_actions: Vec::new(),
                };
                let db_size_bytes = prewarm_store.artifacts_db_size_bytes().ok();
                let html = render_versions_html(
                    &report.cards,
                    &report.unattributed_actions,
                    &unprocessed,
                    &live_status,
                    false,
                    !prewarm_store.is_snapshot_backend(),
                    db_size_bytes,
                    Utc::now(),
                );
                prewarm_cache.put_page("page:/versions/".to_string(), html);
                Ok(())
            })() {
                Ok(()) => info!(
                    "web cache prewarm /versions elapsed_ms={}",
                    versions_started.elapsed().as_millis()
                ),
                Err(err) => warn!("web cache prewarm /versions failed: {:#}", err),
            }
            let g8r_trend_started = Instant::now();
            match (|| -> Result<_> {
                if let Some(indexed) = load_stdlib_fns_trend_dataset_index(
                    &prewarm_store,
                    StdlibTrendKind::G8r,
                    false,
                    None,
                )? {
                    return Ok(indexed);
                }
                let summary = rebuild_stdlib_fns_trend_dataset_index(
                    &prewarm_store,
                    StdlibTrendKind::G8r,
                    false,
                )?;
                info!(
                    "web cache prewarm /dslx-fns-g8r rebuilt stdlib trend index kind={} fraig={} series={} points={} index_bytes={} elapsed_ms={}",
                    summary.kind_path,
                    summary.fraig,
                    summary.series_count,
                    summary.point_count,
                    summary.index_bytes,
                    summary.elapsed_ms
                );
                load_stdlib_fns_trend_dataset_index(
                    &prewarm_store,
                    StdlibTrendKind::G8r,
                    false,
                    None,
                )?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "stdlib trend index rebuild completed but index remained unavailable"
                    )
                })
            })() {
                Ok(dataset) => {
                    let html =
                        render_stdlib_fns_trend_html(&dataset, StdlibMetric::AndNodes, Utc::now());
                    prewarm_cache.put_page(
                        "page:/dslx-fns-g8r/?metric=and_nodes&fraig=false&file=".to_string(),
                        html,
                    );
                    info!(
                        "web cache prewarm /dslx-fns-g8r elapsed_ms={}",
                        g8r_trend_started.elapsed().as_millis()
                    );
                }
                Err(err) => warn!("web cache prewarm /dslx-fns-g8r failed: {:#}", err),
            }
            let yosys_trend_started = Instant::now();
            match (|| -> Result<_> {
                if let Some(indexed) = load_stdlib_fns_trend_dataset_index(
                    &prewarm_store,
                    StdlibTrendKind::YosysAbc,
                    false,
                    None,
                )? {
                    return Ok(indexed);
                }
                let summary = rebuild_stdlib_fns_trend_dataset_index(
                    &prewarm_store,
                    StdlibTrendKind::YosysAbc,
                    false,
                )?;
                info!(
                    "web cache prewarm /dslx-fns-yosys-abc rebuilt stdlib trend index kind={} fraig={} series={} points={} index_bytes={} elapsed_ms={}",
                    summary.kind_path,
                    summary.fraig,
                    summary.series_count,
                    summary.point_count,
                    summary.index_bytes,
                    summary.elapsed_ms
                );
                load_stdlib_fns_trend_dataset_index(
                    &prewarm_store,
                    StdlibTrendKind::YosysAbc,
                    false,
                    None,
                )?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "stdlib trend index rebuild completed but index remained unavailable"
                    )
                })
            })() {
                Ok(dataset) => {
                    let html =
                        render_stdlib_fns_trend_html(&dataset, StdlibMetric::AndNodes, Utc::now());
                    prewarm_cache.put_page(
                        "page:/dslx-fns-yosys-abc/?metric=and_nodes&fraig=false&file=".to_string(),
                        html,
                    );
                    info!(
                        "web cache prewarm /dslx-fns-yosys-abc elapsed_ms={}",
                        yosys_trend_started.elapsed().as_millis()
                    );
                }
                Err(err) => warn!("web cache prewarm /dslx-fns-yosys-abc failed: {:#}", err),
            }
            let timeline_started = Instant::now();
            match (|| -> Result<()> {
                let route_selector = DEFAULT_STDLIB_FN_TIMELINE_ROUTE
                    .trim_start_matches("/dslx-fns/")
                    .trim_matches('/');
                let (file_selector, dslx_fn_name) = parse_stdlib_fn_route_selector(route_selector)?;
                let delay_model = "asap7";
                let dataset = if let Some(indexed) = load_stdlib_fn_version_timeline_dataset_index(
                    &prewarm_store,
                    &file_selector,
                    &dslx_fn_name,
                    false,
                    delay_model,
                )? {
                    indexed
                } else {
                    let summary = rebuild_stdlib_fn_version_timeline_dataset_index(&prewarm_store)?;
                    info!(
                        "web cache prewarm /dslx-fns/{{file_and_fn}} rebuilt timeline index files={} functions={} index_bytes={} elapsed_ms={}",
                        summary.file_count,
                        summary.fn_count,
                        summary.index_bytes,
                        summary.elapsed_ms
                    );
                    load_stdlib_fn_version_timeline_dataset_index(
                        &prewarm_store,
                        &file_selector,
                        &dslx_fn_name,
                        false,
                        delay_model,
                    )?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "stdlib fn timeline index rebuild completed but index remained unavailable"
                        )
                    })?
                };
                let html = render_stdlib_fn_version_timeline_html(&dataset, Utc::now());
                prewarm_cache.put_page(
                    format!(
                        "page:/dslx-fns/{}/?fraig=false&delay_model={}",
                        route_selector, delay_model
                    ),
                    html,
                );
                info!(
                    "web cache prewarm /dslx-fns/{{file_and_fn}} elapsed_ms={} points={} matched_files={} selected_file_present={}",
                    timeline_started.elapsed().as_millis(),
                    dataset.points.len(),
                    dataset.matched_files.len(),
                    dataset.dslx_file.is_some(),
                );
                Ok(())
            })() {
                Ok(()) => {}
                Err(err) => warn!(
                    "web cache prewarm /dslx-fns/{{file_and_fn}} failed: {:#}",
                    err
                ),
            }
            let graph_started = Instant::now();
            let graph_dataset_cache_key = String::from(
                "dataset:dslx-file-action-graph:crate_version=;file=;fn_name=;action_id=;include_k3_descendants=false",
            );
            match prewarm_cache.get_or_compute_stdlib_file_action_graph_dataset(
                &graph_dataset_cache_key,
                || {
                    build_stdlib_file_action_graph_dataset(
                        &prewarm_store,
                        &prewarm_repo_root,
                        None,
                        None,
                        None,
                        None,
                        false,
                    )
                },
            ) {
                Ok(dataset) => {
                    let html = render_stdlib_file_action_graph_html(dataset.as_ref(), Utc::now());
                    prewarm_cache.put_page(
                        "page:/dslx-file-action-graph/?crate_version=&file=&fn_name=&action_id=&include_k3_descendants=false"
                            .to_string(),
                        html,
                    );
                    info!(
                        "web cache prewarm /dslx-file-action-graph elapsed_ms={} roots={} nodes={} edges={}",
                        graph_started.elapsed().as_millis(),
                        dataset.root_action_ids.len(),
                        dataset.nodes.len(),
                        dataset.edges.len(),
                    );
                }
                Err(err) => warn!(
                    "web cache prewarm /dslx-file-action-graph failed: {:#}",
                    err
                ),
            }
            let started = Instant::now();
            match prewarm_cache.get_or_compute_stdlib_g8r_vs_yosys_dataset(false, || {
                if let Some(indexed) = load_stdlib_g8r_vs_yosys_dataset_index(&prewarm_store, false)?
                {
                    return Ok(indexed);
                }
                let summary = rebuild_stdlib_g8r_vs_yosys_dataset_index(&prewarm_store, false)?;
                info!(
                    "web cache prewarm stdlib g8r-vs-yosys rebuilt index fraig={} samples={} versions={} index_bytes={} elapsed_ms={}",
                    summary.fraig,
                    summary.sample_count,
                    summary.crate_versions,
                    summary.index_bytes,
                    summary.elapsed_ms
                );
                if let Some(indexed) = load_stdlib_g8r_vs_yosys_dataset_index(&prewarm_store, false)?
                {
                    return Ok(indexed);
                }
                build_stdlib_g8r_vs_yosys_dataset(&prewarm_store, false)
            }) {
                Ok(dataset) => info!(
                    "web cache prewarm stdlib g8r-vs-yosys samples={} elapsed_ms={}",
                    dataset.samples.len(),
                    started.elapsed().as_millis()
                ),
                Err(err) => warn!("web cache prewarm stdlib g8r-vs-yosys failed: {:#}", err),
            }
            let corpus_started = Instant::now();
            match prewarm_cache.get_or_compute_ir_fn_corpus_g8r_vs_yosys_dataset(|| {
                if let Some(indexed) = load_ir_fn_corpus_g8r_vs_yosys_dataset_index(&prewarm_store)?
                {
                    return Ok(indexed);
                }
                let summary = rebuild_ir_fn_corpus_g8r_vs_yosys_dataset_index(
                    &prewarm_store,
                    &prewarm_repo_root,
                )?;
                info!(
                    "web cache prewarm ir-fn-corpus rebuilt index samples={} versions={}",
                    summary.sample_count, summary.crate_versions
                );
                load_ir_fn_corpus_g8r_vs_yosys_dataset_index(&prewarm_store)?.ok_or_else(|| {
                    anyhow::anyhow!(
                        "ir-fn-corpus index rebuild completed but index remained unavailable"
                    )
                })
            }) {
                Ok(dataset) => info!(
                    "web cache prewarm ir-fn-corpus g8r-vs-yosys samples={} elapsed_ms={}",
                    dataset.samples.len(),
                    corpus_started.elapsed().as_millis()
                ),
                Err(err) => warn!(
                    "web cache prewarm ir-fn-corpus g8r-vs-yosys failed: {:#}",
                    err
                ),
            }
            let frontend_compare_started = Instant::now();
            match prewarm_cache
                .get_or_compute_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset(|| {
                    if let Some(indexed) =
                        load_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index(
                            &prewarm_store,
                        )?
                    {
                        return Ok(indexed);
                    }
                    let summary = rebuild_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index(
                        &prewarm_store,
                        &prewarm_repo_root,
                    )?;
                    info!(
                        "web cache prewarm ir-fn frontend-compare rebuilt index samples={} versions={}",
                        summary.sample_count, summary.crate_versions
                    );
                    load_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index(&prewarm_store)?
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "ir-fn frontend-compare index rebuild completed but index remained unavailable"
                            )
                        })
                })
            {
                Ok(dataset) => info!(
                    "web cache prewarm ir-fn-corpus g8r-abc-vs-codegen-yosys-abc samples={} elapsed_ms={}",
                    dataset.samples.len(),
                    frontend_compare_started.elapsed().as_millis()
                ),
                Err(err) => warn!(
                    "web cache prewarm ir-fn-corpus g8r-abc-vs-codegen-yosys-abc failed: {:#}",
                    err
                ),
            }
        });
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building tokio runtime for web UI")?;
    runtime.block_on(async move {
        let mut app = Router::new()
            .route("/", get(web_root))
            .route("/versions", get(web_versions_redirect))
            .route("/versions/", get(web_versions))
            .route("/db-size", get(web_db_size_redirect))
            .route("/db-size/", get(web_db_size))
            .route("/versions/enqueue-crate", post(web_enqueue_crate_version))
            .route("/action/{action_id}", get(web_action_detail))
            .route("/action/{action_id}/", get(web_action_detail))
            .route(
                "/action/{action_id}/output-file",
                get(web_action_output_file),
            )
            .route(
                "/action/{action_id}/output-file/",
                get(web_action_output_file),
            )
            .route("/dslx-fns-g8r", get(web_stdlib_fns_g8r_redirect))
            .route("/dslx-fns-g8r/", get(web_stdlib_fns_g8r))
            .route(
                "/dslx-fns",
                get(web_stdlib_fn_version_history_root_redirect),
            )
            .route(
                "/dslx-fns/",
                get(web_stdlib_fn_version_history_root_redirect),
            )
            .route("/dslx-fns/{file_and_fn}", get(web_stdlib_fn_version_history))
            .route(
                "/dslx-fns/{file_and_fn}/",
                get(web_stdlib_fn_version_history),
            )
            .route("/dslx-fns-yosys-abc", get(web_stdlib_fns_yosys_abc_redirect))
            .route("/dslx-fns-yosys-abc/", get(web_stdlib_fns_yosys_abc))
            .route(
                "/dslx-fns-g8r-vs-yosys-abc",
                get(web_stdlib_fns_g8r_vs_yosys_abc_redirect),
            )
            .route(
                "/dslx-fns-g8r-vs-yosys-abc/",
                get(web_stdlib_fns_g8r_vs_yosys_abc),
            )
            .route(
                "/ir-fn-corpus-g8r-vs-yosys-abc",
                get(web_ir_fn_corpus_g8r_vs_yosys_abc_redirect),
            )
            .route(
                "/ir-fn-corpus-g8r-vs-yosys-abc/",
                get(web_ir_fn_corpus_g8r_vs_yosys_abc),
            )
            .route(
                "/ir-fn-courpus-g8r-vs-yosys-abc",
                get(web_ir_fn_corpus_g8r_vs_yosys_abc_redirect),
            )
            .route(
                "/ir-fn-courpus-g8r-vs-yosys-abc/",
                get(web_ir_fn_corpus_g8r_vs_yosys_abc),
            )
            .route(
                "/ir-fn-g8r-abc-vs-codegen-yosys-abc",
                get(web_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_redirect),
            )
            .route(
                "/ir-fn-g8r-abc-vs-codegen-yosys-abc/",
                get(web_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc),
            )
            .route(
                "/ir-fn-corpus-k3-losses-vs-crate-version",
                get(web_ir_fn_corpus_k3_losses_vs_crate_version_redirect),
            )
            .route(
                "/ir-fn-corpus-k3-losses-vs-crate-version/",
                get(web_ir_fn_corpus_k3_losses_vs_crate_version),
            )
            .route(
                "/dslx-file-action-graph",
                get(web_stdlib_file_action_graph_redirect),
            )
            .route("/dslx-file-action-graph/", get(web_stdlib_file_action_graph))
            .route(
                "/ir-fn-corpus-structural",
                get(web_ir_fn_corpus_structural_redirect),
            )
            .route("/ir-fn-corpus-structural/", get(web_ir_fn_corpus_structural))
            .route(
                "/ir-fn-corpus-structural/group/{structural_hash}",
                get(web_ir_fn_corpus_structural_group),
            )
            .route(
                "/ir-fn-corpus-structural/group/{structural_hash}/",
                get(web_ir_fn_corpus_structural_group),
            )
            .route(
                "/ir-fn-corpus-structural/download.tar.zst",
                get(web_ir_fn_corpus_structural_download),
            )
            .route("/favicon.svg", get(web_favicon_svg))
            .route("/favicon.ico", get(web_favicon_ico_redirect))
            .route("/api/jsonrpc", post(web_jsonrpc))
            .route("/api/jsonrpc/", post(web_jsonrpc))
            .route("/api/dslx-sample-details", get(web_stdlib_sample_details))
            .route("/api/queue-status", get(web_queue_status));
        if runner_config.enabled {
            app = app
                .route("/api/runner-pause", post(web_runner_pause))
                .route("/api/runner-resume", post(web_runner_resume));
        }
        let heavy_route_limit = state.heavy_route_limit.available_permits();
        let snapshot_status = state
            .snapshot_manifest
            .as_ref()
            .map(|snapshot| {
                format!(
                    "static snapshot mode enabled (snapshot_id={} generated_utc={} dataset_files={})",
                    snapshot.snapshot_id,
                    snapshot.generated_utc.to_rfc3339(),
                    snapshot.dataset_files.len()
                )
            });
        let app = app.with_state(state);
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("binding web UI listener on {}", addr))?;
        println!("web UI listening at http://{}/versions/", addr);
        if runner_config.enabled {
            println!(
                "embedded queue runner enabled (workers={}, lease_seconds={}, poll_millis={}, batch_size={})",
                runner_config.worker_count,
                runner_config.lease_seconds,
                runner_config.poll_interval.as_millis(),
                runner_config.drain_batch_size
            );
        } else {
            println!("embedded queue runner disabled (--no-runner)");
        }
        if let Some(snapshot_status) = snapshot_status {
            println!("{}", snapshot_status);
        }
        println!(
            "heavy web route concurrency limit={}",
            heavy_route_limit
        );
        axum::serve(listener, app)
            .await
            .context("serving web UI failed")
    })
}
