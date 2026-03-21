use super::types::*;
use axum::body::Body;
use axum::{
    extract::{Path as AxumPath, Query, State},
    http::{
        HeaderValue,
        header::{CONTENT_DISPOSITION, CONTENT_TYPE},
    },
    response::{Html, IntoResponse, Redirect},
};
use chrono::Utc;
use log::info;
use std::time::Instant;

use crate::query::{
    build_ir_fn_corpus_k3_loss_by_version_dataset, build_ir_fn_corpus_structural_coverage,
    build_ir_fn_corpus_structural_member_views, clamp_ir_node_limit,
    ensure_ir_fn_corpus_structural_archive, html_escape,
    ir_fn_corpus_structural_archive_download_filename, is_valid_action_id_for_route,
    load_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index,
    load_ir_fn_corpus_g8r_vs_yosys_dataset_index, load_ir_fn_corpus_structural_group,
    load_ir_fn_corpus_structural_manifest, lookup_ir_fn_corpus_by_fn_name,
    rebuild_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index,
    rebuild_ir_fn_corpus_g8r_vs_yosys_dataset_index,
};
use crate::versioning::normalize_tag_version;
use crate::view::G8rVsYosysViewScope;
use crate::web::render::{
    inject_server_timing_badge, render_ir_fn_corpus_k3_losses_vs_crate_version_html,
    render_ir_fn_corpus_structural_group_html, render_ir_fn_corpus_structural_html,
    render_stdlib_g8r_vs_yosys_html,
};

pub(super) async fn web_ir_fn_corpus_g8r_vs_yosys_abc_redirect() -> Redirect {
    Redirect::temporary("/ir-fn-corpus-g8r-vs-yosys-abc/")
}

pub(super) async fn web_ir_fn_corpus_k3_losses_vs_crate_version_redirect() -> Redirect {
    Redirect::temporary("/ir-fn-corpus-k3-losses-vs-crate-version/")
}

pub(super) async fn web_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_redirect() -> Redirect {
    Redirect::temporary("/ir-fn-g8r-abc-vs-codegen-yosys-abc/")
}

pub(super) async fn web_ir_fn_corpus_structural_redirect() -> Redirect {
    Redirect::temporary("/ir-fn-corpus-structural/")
}

pub(super) async fn web_ir_fn_corpus_structural(
    State(state): State<WebUiState>,
    Query(query): Query<IrFnCorpusStructuralQuery>,
) -> impl IntoResponse {
    let request_started = Instant::now();
    let requested_fn_name = query
        .fn_name
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let cache_key = format!(
        "page:/ir-fn-corpus-structural/?fn_name={}",
        requested_fn_name.as_deref().unwrap_or("")
    );
    if let Some(html) = state.cache.get_page(&cache_key) {
        return Html(inject_server_timing_badge(
            &html,
            request_started.elapsed().as_millis(),
        ))
        .into_response();
    }
    let store = state.store.clone();
    let cache = state.cache.clone();
    let requested_fn_name_for_lookup = requested_fn_name.clone();
    match tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let manifest = load_ir_fn_corpus_structural_manifest(&store).ok();
        let coverage = load_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store)
            .ok()
            .flatten()
            .map(|dataset| build_ir_fn_corpus_structural_coverage(&dataset))
            .unwrap_or_default();
        let lookup_rows = if let (Some(manifest), Some(fn_name)) =
            (manifest.as_ref(), requested_fn_name_for_lookup.as_deref())
        {
            lookup_ir_fn_corpus_by_fn_name(&store, manifest, fn_name, 256)?
        } else {
            Vec::new()
        };
        let group_rows = manifest
            .as_ref()
            .map(|manifest| {
                manifest
                    .groups
                    .iter()
                    .map(|group| {
                        let g8r_loss_sample_count = coverage
                            .g8r_loss_sample_count_by_structural_hash
                            .get(&group.structural_hash)
                            .copied()
                            .unwrap_or(0);
                        let ir_node_count = group.ir_node_count.or_else(|| {
                            coverage
                                .ir_node_count_by_structural_hash
                                .get(&group.structural_hash)
                                .copied()
                        });
                        crate::view::IrFnCorpusStructuralGroupRowView {
                            structural_hash: group.structural_hash.clone(),
                            member_count: group.member_count,
                            ir_node_count,
                            g8r_loss_sample_count,
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let html = render_ir_fn_corpus_structural_html(
            manifest.as_ref(),
            &group_rows,
            requested_fn_name_for_lookup.as_deref(),
            &lookup_rows,
            Utc::now(),
        );
        cache.put_page(cache_key, html.clone());
        let group_count = manifest.as_ref().map(|m| m.groups.len()).unwrap_or(0);
        info!(
            "web /ir-fn-corpus-structural groups={} lookup_fn={} lookup_matches={} elapsed_ms={}",
            group_count,
            requested_fn_name
                .as_deref()
                .map(html_escape)
                .unwrap_or_else(|| "-".to_string()),
            lookup_rows.len(),
            started.elapsed().as_millis()
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
            format!("failed building /ir-fn-corpus-structural/ view: {:#}", err),
        )
            .into_response(),
        Err(join_err) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("web handler join error: {}", join_err),
        )
            .into_response(),
    }
}

pub(super) async fn web_ir_fn_corpus_structural_group(
    State(state): State<WebUiState>,
    AxumPath(structural_hash): AxumPath<String>,
) -> impl IntoResponse {
    let request_started = Instant::now();
    if !is_valid_action_id_for_route(&structural_hash) {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            format!(
                "invalid structural hash `{}`: expected 64 hex characters",
                structural_hash
            ),
        )
            .into_response();
    }
    let cache_key = format!("page:/ir-fn-corpus-structural/group/{}/", structural_hash);
    if let Some(html) = state.cache.get_page(&cache_key) {
        return Html(inject_server_timing_badge(
            &html,
            request_started.elapsed().as_millis(),
        ))
        .into_response();
    }
    let cache = state.cache.clone();
    let store = state.store.clone();
    let requested_hash = structural_hash.clone();
    match tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let group = load_ir_fn_corpus_structural_group(&store, &requested_hash)?;
        let members = build_ir_fn_corpus_structural_member_views(&store, &group);
        let html = render_ir_fn_corpus_structural_group_html(&requested_hash, &members, Utc::now());
        cache.put_page(cache_key, html.clone());
        info!(
            "web /ir-fn-corpus-structural/group members={} elapsed_ms={}",
            members.len(),
            started.elapsed().as_millis()
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
            axum::http::StatusCode::NOT_FOUND,
            format!(
                "failed building /ir-fn-corpus-structural/group/{}/ view: {:#}",
                html_escape(&structural_hash),
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

pub(super) async fn web_ir_fn_corpus_structural_download(
    State(state): State<WebUiState>,
) -> impl IntoResponse {
    let request_started = Instant::now();
    let permit = match state.heavy_route_limit.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            info!("web throttle route=/ir-fn-corpus-structural/download.tar.zst");
            return (
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                "too many in-flight heavy requests; retry shortly".to_string(),
            )
                .into_response();
        }
    };
    let store = state.store.clone();
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let started = Instant::now();
        let payload = ensure_ir_fn_corpus_structural_archive(&store)?;
        info!(
            "web /ir-fn-corpus-structural/download.tar.zst cache_hit={} functions={} bytes={} elapsed_ms={}",
            payload.cache_hit,
            payload.function_count,
            payload.bytes.len(),
            started.elapsed().as_millis()
        );
        Ok::<_, anyhow::Error>(payload)
    })
    .await
    {
        Ok(Ok(payload)) => {
            let filename = ir_fn_corpus_structural_archive_download_filename();
            let disposition_value = format!("attachment; filename=\"{}\"", filename);
            let mut response = axum::response::Response::new(Body::from(payload.bytes));
            *response.status_mut() = axum::http::StatusCode::OK;
            response
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/zstd"));
            if let Ok(value) = HeaderValue::from_str(&disposition_value) {
                response.headers_mut().insert(CONTENT_DISPOSITION, value);
            }
            let elapsed_ms = request_started.elapsed().as_millis().to_string();
            if let Ok(value) = HeaderValue::from_str(&elapsed_ms) {
                response.headers_mut().insert("x-bvc-server-ms", value);
            }
            response.into_response()
        }
        Ok(Err(err)) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "failed building /ir-fn-corpus-structural/download.tar.zst: {:#}",
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

pub(super) async fn web_ir_fn_corpus_g8r_vs_yosys_abc(
    State(state): State<WebUiState>,
    Query(query): Query<IrFnCorpusG8rVsYosysAbcQuery>,
) -> impl IntoResponse {
    let request_started = Instant::now();
    let cache = state.cache.clone();
    let store = state.store.clone();
    let repo_root = state.repo_root.clone();
    let requested_max_ir_nodes = query.max_ir_nodes;
    let losses_only = query.losses_only.unwrap_or(false);
    let snapshot_mode = state.snapshot_manifest.is_some();
    let requested_crate_version = query
        .crate_version
        .map(|v| normalize_tag_version(v.trim()).to_string())
        .filter(|v| !v.is_empty());
    let cache_key = format!(
        "page:/ir-fn-corpus-g8r-vs-yosys-abc/?max_ir_nodes={}&crate_version={}&losses_only={}",
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
            info!("web throttle route=/ir-fn-corpus-g8r-vs-yosys-abc");
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
        let dataset = cache.get_or_compute_ir_fn_corpus_g8r_vs_yosys_dataset(|| {
            if let Some(indexed) = load_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store)? {
                return Ok(indexed);
            }
            if snapshot_mode {
                return Err(anyhow::anyhow!(
                    "ir-fn-corpus g8r-vs-yosys dataset index missing in static snapshot mode"
                ));
            }
            let summary = rebuild_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store, &repo_root)?;
            info!(
                "web /ir-fn-corpus-g8r-vs-yosys-abc rebuilt index samples={} versions={}",
                summary.sample_count, summary.crate_versions
            );
            load_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store)?.ok_or_else(|| {
                anyhow::anyhow!(
                    "ir-fn-corpus index rebuild completed but index remained unavailable"
                )
            })
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
            G8rVsYosysViewScope::IrFnCorpus,
        );
        cache.put_page(cache_key, html.clone());
        info!(
            "web /ir-fn-corpus-g8r-vs-yosys-abc dataset_build_ms={} render_ms={} total_ms={} samples={} versions={} losses_only={}",
            (after_build - started).as_millis(),
            after_build.elapsed().as_millis(),
            started.elapsed().as_millis(),
            dataset.samples.len(),
            dataset.available_crate_versions.len(),
            losses_only,
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
                "failed building /ir-fn-corpus-g8r-vs-yosys-abc/ view: {:#}",
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

pub(super) async fn web_ir_fn_corpus_k3_losses_vs_crate_version(
    State(state): State<WebUiState>,
    Query(query): Query<IrFnCorpusK3LossesVsCrateVersionQuery>,
) -> impl IntoResponse {
    let request_started = Instant::now();
    let cache = state.cache.clone();
    let store = state.store.clone();
    let repo_root = state.repo_root.clone();
    let requested_max_ir_nodes = query
        .max_ir_nodes
        .or_else(|| crate::default_k_bool_cone_max_ir_ops_for_k(3));
    let show_same = query.show_same.unwrap_or(false);
    let snapshot_mode = state.snapshot_manifest.is_some();
    let requested_crate_version = query
        .crate_version
        .map(|v| normalize_tag_version(v.trim()).to_string())
        .filter(|v| !v.is_empty());
    let cache_key = format!(
        "page:/ir-fn-corpus-k3-losses-vs-crate-version/?max_ir_nodes={}&crate_version={}&show_same={}",
        requested_max_ir_nodes
            .map(|v| v.to_string())
            .unwrap_or_default(),
        requested_crate_version.as_deref().unwrap_or(""),
        show_same,
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
            info!("web throttle route=/ir-fn-corpus-k3-losses-vs-crate-version");
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
        let dataset = cache.get_or_compute_ir_fn_corpus_g8r_vs_yosys_dataset(|| {
            if let Some(indexed) = load_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store)? {
                return Ok(indexed);
            }
            if snapshot_mode {
                return Err(anyhow::anyhow!(
                    "ir-fn-corpus g8r-vs-yosys dataset index missing in static snapshot mode"
                ));
            }
            let summary = rebuild_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store, &repo_root)?;
            info!(
                "web /ir-fn-corpus-k3-losses-vs-crate-version rebuilt index samples={} versions={}",
                summary.sample_count, summary.crate_versions
            );
            load_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store)?.ok_or_else(|| {
                anyhow::anyhow!(
                    "ir-fn-corpus index rebuild completed but index remained unavailable"
                )
            })
        })?;
        let after_build = Instant::now();
        let view = build_ir_fn_corpus_k3_loss_by_version_dataset(
            &dataset,
            requested_crate_version.as_deref(),
            requested_max_ir_nodes,
            show_same,
        );
        let html = render_ir_fn_corpus_k3_losses_vs_crate_version_html(&view, Utc::now());
        cache.put_page(cache_key, html.clone());
        info!(
            "web /ir-fn-corpus-k3-losses-vs-crate-version dataset_build_ms={} render_ms={} total_ms={} rows={} compared_versions={} selected_crate={} show_same={}",
            (after_build - started).as_millis(),
            after_build.elapsed().as_millis(),
            started.elapsed().as_millis(),
            view.rows.len(),
            view.compared_crate_versions.len(),
            view.selected_crate_version.as_deref().unwrap_or("-"),
            show_same,
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
                "failed building /ir-fn-corpus-k3-losses-vs-crate-version/ view: {:#}",
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

pub(super) async fn web_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc(
    State(state): State<WebUiState>,
    Query(query): Query<IrFnCorpusG8rVsYosysAbcQuery>,
) -> impl IntoResponse {
    let request_started = Instant::now();
    let cache = state.cache.clone();
    let store = state.store.clone();
    let repo_root = state.repo_root.clone();
    let requested_max_ir_nodes = query.max_ir_nodes;
    let losses_only = query.losses_only.unwrap_or(false);
    let snapshot_mode = state.snapshot_manifest.is_some();
    let requested_crate_version = query
        .crate_version
        .map(|v| normalize_tag_version(v.trim()).to_string())
        .filter(|v| !v.is_empty());
    let cache_key = format!(
        "page:/ir-fn-g8r-abc-vs-codegen-yosys-abc/?max_ir_nodes={}&crate_version={}&losses_only={}",
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
    let _permit = match state.heavy_route_limit.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            info!("web throttle route=/ir-fn-g8r-abc-vs-codegen-yosys-abc");
            return (
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                "too many in-flight heavy requests; retry shortly".to_string(),
            )
                .into_response();
        }
    };
    match tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let dataset = cache.get_or_compute_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset(
            || {
                if let Some(indexed) =
                    load_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index(&store)?
                {
                    return Ok(indexed);
                }
                if snapshot_mode {
                    return Err(anyhow::anyhow!(
                        "ir-fn-corpus g8r-abc-vs-codegen-yosys dataset index missing in static snapshot mode"
                    ));
                }
                let summary = rebuild_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index(
                    &store, &repo_root,
                )?;
                info!(
                    "web /ir-fn-g8r-abc-vs-codegen-yosys-abc rebuilt index samples={} versions={}",
                    summary.sample_count, summary.crate_versions
                );
                load_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index(&store)?.ok_or_else(
                    || {
                        anyhow::anyhow!(
                            "ir-fn-corpus frontend-compare index rebuild completed but index remained unavailable"
                        )
                    },
                )
            },
        )?;
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
            G8rVsYosysViewScope::IrFnCorpusG8rAbcVsCodegenYosysAbc,
        );
        cache.put_page(cache_key, html.clone());
        info!(
            "web /ir-fn-g8r-abc-vs-codegen-yosys-abc dataset_build_ms={} render_ms={} total_ms={} samples={} versions={} losses_only={}",
            (after_build - started).as_millis(),
            after_build.elapsed().as_millis(),
            started.elapsed().as_millis(),
            dataset.samples.len(),
            dataset.available_crate_versions.len(),
            losses_only,
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
                "failed building /ir-fn-g8r-abc-vs-codegen-yosys-abc/ view: {:#}",
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
