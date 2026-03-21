use super::types::*;
use axum::{
    Json,
    extract::{Path as AxumPath, Query, State},
    response::{Html, IntoResponse, Redirect},
};
use chrono::Utc;
use log::info;
use std::time::Instant;

use crate::DEFAULT_STDLIB_FN_TIMELINE_ROUTE;
use crate::query::{
    build_stdlib_file_action_graph_dataset, build_stdlib_fn_version_timeline_dataset,
    build_stdlib_g8r_vs_yosys_dataset, build_stdlib_sample_details, clamp_ir_node_limit,
    is_valid_action_id_for_route, load_stdlib_fn_version_timeline_dataset_index,
    load_stdlib_fns_trend_dataset_index, load_stdlib_g8r_vs_yosys_dataset_index,
    parse_stdlib_fn_route_selector, rebuild_stdlib_fn_version_timeline_dataset_index,
    rebuild_stdlib_fns_trend_dataset_index, rebuild_stdlib_g8r_vs_yosys_dataset_index,
};
use crate::versioning::normalize_tag_version;
use crate::view::{G8rVsYosysViewScope, StdlibMetric, StdlibTrendKind};
use crate::web::render::{
    inject_server_timing_badge, render_stdlib_file_action_graph_html,
    render_stdlib_fn_version_timeline_html, render_stdlib_fns_trend_html,
    render_stdlib_g8r_vs_yosys_html,
};

pub(super) async fn web_stdlib_fns_g8r_redirect() -> Redirect {
    Redirect::temporary("/dslx-fns-g8r/")
}

pub(super) async fn web_stdlib_fn_version_history_root_redirect() -> Redirect {
    Redirect::temporary(DEFAULT_STDLIB_FN_TIMELINE_ROUTE)
}

pub(super) async fn web_stdlib_fns_yosys_abc_redirect() -> Redirect {
    Redirect::temporary("/dslx-fns-yosys-abc/")
}

pub(super) async fn web_stdlib_fns_g8r_vs_yosys_abc_redirect() -> Redirect {
    Redirect::temporary("/dslx-fns-g8r-vs-yosys-abc/")
}

pub(super) async fn web_stdlib_file_action_graph_redirect() -> Redirect {
    Redirect::temporary("/dslx-file-action-graph/")
}

pub(super) async fn web_stdlib_fns_g8r(
    State(state): State<WebUiState>,
    Query(query): Query<StdlibFnsTrendQuery>,
) -> impl IntoResponse {
    web_stdlib_fns_trend_impl(state, query, StdlibTrendKind::G8r).await
}

pub(super) async fn web_stdlib_fns_yosys_abc(
    State(state): State<WebUiState>,
    Query(query): Query<StdlibFnsTrendQuery>,
) -> impl IntoResponse {
    web_stdlib_fns_trend_impl(state, query, StdlibTrendKind::YosysAbc).await
}

pub(super) async fn web_stdlib_fns_g8r_vs_yosys_abc(
    State(state): State<WebUiState>,
    Query(query): Query<StdlibFnsG8rVsYosysAbcQuery>,
) -> impl IntoResponse {
    let request_started = Instant::now();
    let cache = state.cache.clone();
    let store = state.store.clone();
    let fraig = query.fraig.unwrap_or(false);
    let losses_only = query.losses_only.unwrap_or(false);
    let snapshot_mode = state.snapshot_manifest.is_some();
    let requested_max_ir_nodes = query.max_ir_nodes;
    let requested_crate_version = query
        .crate_version
        .map(|v| normalize_tag_version(v.trim()).to_string())
        .filter(|v| !v.is_empty());
    let cache_key = format!(
        "page:/dslx-fns-g8r-vs-yosys-abc/?fraig={}&max_ir_nodes={}&crate_version={}&losses_only={}",
        fraig,
        requested_max_ir_nodes
            .map(|v| v.to_string())
            .unwrap_or_default(),
        requested_crate_version.as_deref().unwrap_or(""),
        losses_only
    );
    if let Some(html) = cache.get_page(&cache_key) {
        return Html(inject_server_timing_badge(
            &html,
            request_started.elapsed().as_millis(),
        ))
        .into_response();
    }
    let permit = match state.heavy_route_limit.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            info!("web throttle route=/dslx-fns-g8r-vs-yosys-abc");
            return (
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                "too many in-flight heavy requests; retry shortly".to_string(),
            )
                .into_response();
        }
    };
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let started = Instant::now();
        let dataset = cache.get_or_compute_stdlib_g8r_vs_yosys_dataset(fraig, || {
            if let Some(indexed) = load_stdlib_g8r_vs_yosys_dataset_index(&store, fraig)? {
                return Ok(indexed);
            }
            if snapshot_mode {
                return Err(anyhow::anyhow!(
                    "stdlib g8r-vs-yosys dataset index missing in static snapshot mode (fraig={})",
                    fraig
                ));
            }
            let summary = rebuild_stdlib_g8r_vs_yosys_dataset_index(&store, fraig)?;
            info!(
                "web /dslx-fns-g8r-vs-yosys-abc rebuilt stdlib g8r-vs-yosys index fraig={} samples={} versions={} index_bytes={} elapsed_ms={}",
                summary.fraig,
                summary.sample_count,
                summary.crate_versions,
                summary.index_bytes,
                summary.elapsed_ms
            );
            if let Some(indexed) = load_stdlib_g8r_vs_yosys_dataset_index(&store, fraig)? {
                return Ok(indexed);
            }
            build_stdlib_g8r_vs_yosys_dataset(&store, fraig)
        })?;
        let after_build = Instant::now();
        let max_ir_nodes = clamp_ir_node_limit(
            requested_max_ir_nodes,
            dataset.min_ir_nodes,
            dataset.max_ir_nodes,
        );
        let selected_crate_version = requested_crate_version
            .filter(|v| dataset.available_crate_versions.binary_search(v).is_ok())
            .or_else(|| dataset.available_crate_versions.last().cloned());
        let html = render_stdlib_g8r_vs_yosys_html(
            &dataset,
            max_ir_nodes,
            selected_crate_version.as_deref(),
            losses_only,
            Utc::now(),
            G8rVsYosysViewScope::Stdlib,
        );
        cache.put_page(cache_key, html.clone());
        info!(
            "web /dslx-fns-g8r-vs-yosys-abc dataset_build_ms={} render_ms={} total_ms={} samples={} paired_scope_versions={} losses_only={} fraig={}",
            (after_build - started).as_millis(),
            after_build.elapsed().as_millis(),
            started.elapsed().as_millis(),
            dataset.samples.len(),
            dataset.available_crate_versions.len(),
            losses_only,
            fraig,
        );
        Ok::<String, anyhow::Error>(html)
    })
    .await
    {
        Ok(Ok(html)) => Html(inject_server_timing_badge(
            &html,
            request_started.elapsed().as_millis(),
        ))
        .into_response(),
        Ok(Err(err)) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "failed building /dslx-fns-g8r-vs-yosys-abc/ view: {:#}",
                err
            ),
        )
            .into_response(),
        Err(join_err) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("web handler join error: {}", join_err),
        )
            .into_response(),
    }
}

pub(super) async fn web_stdlib_file_action_graph(
    State(state): State<WebUiState>,
    Query(query): Query<StdlibFileActionGraphQuery>,
) -> impl IntoResponse {
    if state.snapshot_manifest.is_some() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "/dslx-file-action-graph is unavailable in static snapshot mode".to_string(),
        )
            .into_response();
    }
    let request_started = Instant::now();
    let cache = state.cache.clone();
    let store = state.store.clone();
    let repo_root = state.repo_root.clone();
    let requested_crate_version = query
        .crate_version
        .map(|v| normalize_tag_version(v.trim()).to_string())
        .filter(|v| !v.is_empty());
    let requested_file = query
        .file
        .map(|v| v.trim().replace('\\', "/"))
        .filter(|v| !v.is_empty());
    let requested_fn_name = query
        .fn_name
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let requested_action_id = query
        .action_id
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty());
    let include_k3_descendants = query.include_k3_descendants.unwrap_or(false);
    if let Some(action_id) = requested_action_id.as_deref()
        && !is_valid_action_id_for_route(action_id)
    {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            format!(
                "invalid action id `{}`: expected 64 hex characters",
                action_id
            ),
        )
            .into_response();
    }
    let cache_key = format!(
        "page:/dslx-file-action-graph/?crate_version={}&file={}&fn_name={}&action_id={}&include_k3_descendants={}",
        requested_crate_version.as_deref().unwrap_or(""),
        requested_file.as_deref().unwrap_or(""),
        requested_fn_name.as_deref().unwrap_or(""),
        requested_action_id.as_deref().unwrap_or(""),
        include_k3_descendants,
    );
    let dataset_cache_key = format!(
        "dataset:dslx-file-action-graph:crate_version={};file={};fn_name={};action_id={};include_k3_descendants={}",
        requested_crate_version.as_deref().unwrap_or(""),
        requested_file.as_deref().unwrap_or(""),
        requested_fn_name.as_deref().unwrap_or(""),
        requested_action_id.as_deref().unwrap_or(""),
        include_k3_descendants,
    );
    if let Some(html) = cache.get_page(&cache_key) {
        return Html(inject_server_timing_badge(
            &html,
            request_started.elapsed().as_millis(),
        ))
        .into_response();
    }
    let permit = match state.heavy_route_limit.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            info!("web throttle route=/dslx-file-action-graph");
            return (
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                "too many in-flight heavy requests; retry shortly".to_string(),
            )
                .into_response();
        }
    };
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let started = Instant::now();
        let dataset = cache.get_or_compute_stdlib_file_action_graph_dataset(
            &dataset_cache_key,
            || {
                build_stdlib_file_action_graph_dataset(
                    &store,
                    &repo_root,
                    requested_crate_version.as_deref(),
                    requested_file.as_deref(),
                    requested_fn_name.as_deref(),
                    requested_action_id.as_deref(),
                    include_k3_descendants,
                )
            },
        )?;
        let after_build = Instant::now();
        let html = render_stdlib_file_action_graph_html(dataset.as_ref(), Utc::now());
        cache.put_page(cache_key, html.clone());
        info!(
            "web /dslx-file-action-graph dataset_build_ms={} render_ms={} total_ms={} crate_actions={} roots={} nodes={} edges={} action_focus={} include_k3_descendants={}",
            (after_build - started).as_millis(),
            after_build.elapsed().as_millis(),
            started.elapsed().as_millis(),
            dataset.total_actions_for_crate,
            dataset.root_action_ids.len(),
            dataset.nodes.len(),
            dataset.edges.len(),
            dataset.selected_action_id.as_deref().unwrap_or(""),
            dataset.include_k3_descendants,
        );
        Ok::<String, anyhow::Error>(html)
    })
    .await
    {
        Ok(Ok(html)) => Html(inject_server_timing_badge(
            &html,
            request_started.elapsed().as_millis(),
        ))
        .into_response(),
        Ok(Err(err)) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed building /dslx-file-action-graph/ view: {:#}", err),
        )
            .into_response(),
        Err(join_err) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("web handler join error: {}", join_err),
        )
            .into_response(),
    }
}

pub(super) async fn web_stdlib_sample_details(
    State(state): State<WebUiState>,
    Query(query): Query<StdlibSampleDetailsQuery>,
) -> impl IntoResponse {
    if state.snapshot_manifest.is_some() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "dslx sample details are unavailable in static snapshot mode".to_string(),
        )
            .into_response();
    }
    let store = state.store.clone();
    let ir_action_id = query.ir_action_id.trim().to_string();
    let ir_top = query
        .ir_top
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned);
    if ir_action_id.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "missing ir_action_id query parameter".to_string(),
        )
            .into_response();
    }
    match tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let details = build_stdlib_sample_details(&store, &ir_action_id, ir_top.as_deref())?;
        info!(
            "web /api/dslx-sample-details ir_action_id={} has_ir_top={} elapsed_ms={}",
            ir_action_id,
            details.ir_top_fn_name.is_some(),
            started.elapsed().as_millis(),
        );
        Ok::<_, anyhow::Error>(details)
    })
    .await
    {
        Ok(Ok(view)) => Json(view).into_response(),
        Ok(Err(err)) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "failed resolving sample details for `{}`: {:#}",
                query.ir_action_id, err
            ),
        )
            .into_response(),
        Err(join_err) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("web handler join error: {}", join_err),
        )
            .into_response(),
    }
}

pub(super) async fn web_stdlib_fn_version_history(
    State(state): State<WebUiState>,
    AxumPath(file_and_fn): AxumPath<String>,
    Query(query): Query<StdlibFnVersionHistoryQuery>,
) -> impl IntoResponse {
    let request_started = Instant::now();
    let (file_selector, dslx_fn_name) = match parse_stdlib_fn_route_selector(&file_and_fn) {
        Ok(v) => v,
        Err(err) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                format!(
                    "invalid DSLX function selector `{}` (expected `<file>:<fn>`): {:#}",
                    file_and_fn, err
                ),
            )
                .into_response();
        }
    };
    let fraig = query.fraig.unwrap_or(false);
    let delay_model = query
        .delay_model
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("asap7")
        .to_string();
    let snapshot_mode = state.snapshot_manifest.is_some();
    let cache = state.cache.clone();
    let store = state.store.clone();
    let cache_key = format!(
        "page:/dslx-fns/{}/?fraig={}&delay_model={}",
        file_and_fn, fraig, delay_model
    );
    if let Some(html) = cache.get_page(&cache_key) {
        return Html(inject_server_timing_badge(
            &html,
            request_started.elapsed().as_millis(),
        ))
        .into_response();
    }
    let permit = match state.heavy_route_limit.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            info!("web throttle route=/dslx-fns/{file_and_fn}");
            return (
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                "too many in-flight heavy requests; retry shortly".to_string(),
            )
                .into_response();
        }
    };
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let started = Instant::now();
        let dataset = if let Some(indexed) = load_stdlib_fn_version_timeline_dataset_index(
            &store,
            &file_selector,
            &dslx_fn_name,
            fraig,
            &delay_model,
        )? {
            indexed
        } else {
            if snapshot_mode {
                return Err(anyhow::anyhow!(
                    "stdlib fn timeline dataset index missing in static snapshot mode for {}:{}",
                    file_selector,
                    dslx_fn_name
                ));
            }
            let summary = rebuild_stdlib_fn_version_timeline_dataset_index(&store)?;
            info!(
                "web /dslx-fns/{{file_and_fn}} rebuilt stdlib timeline index files={} functions={} index_bytes={} elapsed_ms={}",
                summary.file_count,
                summary.fn_count,
                summary.index_bytes,
                summary.elapsed_ms
            );
            load_stdlib_fn_version_timeline_dataset_index(
                &store,
                &file_selector,
                &dslx_fn_name,
                fraig,
                &delay_model,
            )?
            .unwrap_or(build_stdlib_fn_version_timeline_dataset(
                &store,
                &file_selector,
                &dslx_fn_name,
                fraig,
                &delay_model,
            )?)
        };
        let after_build = Instant::now();
        let html = render_stdlib_fn_version_timeline_html(&dataset, Utc::now());
        cache.put_page(cache_key, html.clone());
        info!(
            "web /dslx-fns/{{file_and_fn}} dataset_build_ms={} render_ms={} total_ms={} points={} matched_files={} selected_file_present={}",
            (after_build - started).as_millis(),
            after_build.elapsed().as_millis(),
            started.elapsed().as_millis(),
            dataset.points.len(),
            dataset.matched_files.len(),
            dataset.dslx_file.is_some(),
        );
        Ok::<String, anyhow::Error>(html)
    })
    .await
    {
        Ok(Ok(html)) => Html(inject_server_timing_badge(
            &html,
            request_started.elapsed().as_millis(),
        ))
        .into_response(),
        Ok(Err(err)) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed building /dslx-fns/{}/ view: {:#}", file_and_fn, err),
        )
            .into_response(),
        Err(join_err) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("web handler join error: {}", join_err),
        )
            .into_response(),
    }
}

async fn web_stdlib_fns_trend_impl(
    state: WebUiState,
    query: StdlibFnsTrendQuery,
    kind: StdlibTrendKind,
) -> impl IntoResponse {
    let request_started = Instant::now();
    let cache = state.cache.clone();
    let store = state.store.clone();
    let snapshot_mode = state.snapshot_manifest.is_some();
    let metric = StdlibMetric::from_query(query.metric.as_deref());
    let fraig = if kind.supports_fraig() {
        query.fraig.unwrap_or(false)
    } else {
        false
    };
    let selected_file = query.file.and_then(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });
    let cache_key = format!(
        "page:{}?metric={}&fraig={}&file={}",
        kind.view_path(),
        metric.as_query_value(),
        fraig,
        selected_file.as_deref().unwrap_or(""),
    );
    if let Some(html) = cache.get_page(&cache_key) {
        return Html(inject_server_timing_badge(
            &html,
            request_started.elapsed().as_millis(),
        ))
        .into_response();
    }
    match tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let dataset = if let Some(indexed) =
            load_stdlib_fns_trend_dataset_index(&store, kind, fraig, selected_file.as_deref())?
        {
            indexed
        } else {
            if snapshot_mode {
                return Err(anyhow::anyhow!(
                    "stdlib trend dataset index missing in static snapshot mode (kind={} fraig={})",
                    kind.view_path(),
                    fraig
                ));
            }
            let summary = rebuild_stdlib_fns_trend_dataset_index(&store, kind, fraig)?;
            info!(
                "web {} rebuilt stdlib trend index kind={} fraig={} series={} points={} index_bytes={} elapsed_ms={}",
                kind.view_path(),
                summary.kind_path,
                summary.fraig,
                summary.series_count,
                summary.point_count,
                summary.index_bytes,
                summary.elapsed_ms
            );
            load_stdlib_fns_trend_dataset_index(&store, kind, fraig, selected_file.as_deref())?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "stdlib trend index rebuild completed but index remained unavailable"
                    )
                })?
        };
        let after_build = Instant::now();
        let html = render_stdlib_fns_trend_html(&dataset, metric, Utc::now());
        cache.put_page(cache_key, html.clone());
        info!(
            "web {} dataset_build_ms={} render_ms={} total_ms={} series={} points={} fraig={} metric={}",
            kind.view_path(),
            (after_build - started).as_millis(),
            after_build.elapsed().as_millis(),
            started.elapsed().as_millis(),
            dataset.series.len(),
            dataset.total_points,
            fraig,
            metric.as_query_value(),
        );
        Ok::<String, anyhow::Error>(html)
    })
    .await
    {
        Ok(Ok(html)) => Html(inject_server_timing_badge(
            &html,
            request_started.elapsed().as_millis(),
        ))
        .into_response(),
        Ok(Err(err)) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed building {} view: {:#}", kind.view_path(), err),
        )
            .into_response(),
        Err(join_err) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("web handler join error: {}", join_err),
        )
            .into_response(),
    }
}
