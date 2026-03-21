use super::types::*;
use axum::{
    Json,
    extract::{Form, State},
    http::{HeaderValue, header::CONTENT_TYPE},
    response::{Html, IntoResponse, Redirect},
};
use chrono::Utc;
use log::info;
use std::fs;
use std::time::Instant;

use crate::DASHBOARD_FAVICON_SVG;
use crate::query::{
    build_queue_live_status, build_unprocessed_version_rows, enqueue_processing_for_crate_version,
    load_versions_cards_index, rebuild_versions_cards_index,
};
use crate::view::QueueLiveStatusView;
use crate::web::render::{inject_server_timing_badge, render_versions_html};

fn apply_runner_control_status(
    mut status: QueueLiveStatusView,
    runner_enabled: bool,
    control: Option<&WebRunnerControl>,
) -> QueueLiveStatusView {
    status.runner_enabled = runner_enabled;
    if !runner_enabled {
        status.runner_paused = false;
        status.runner_drained = false;
        status.runner_sync_pending = false;
        status.runner_last_sync_utc = None;
        status.runner_last_sync_error = None;
        return status;
    }
    if let Some(control) = control {
        let snapshot = control.snapshot();
        status.runner_paused = snapshot.paused;
        status.runner_drained = snapshot.paused && status.running == 0;
        status.runner_sync_pending = snapshot.sync_pending;
        status.runner_last_sync_utc = snapshot.last_sync_utc;
        status.runner_last_sync_error = snapshot.last_sync_error;
    } else {
        status.runner_paused = false;
        status.runner_drained = false;
        status.runner_sync_pending = false;
        status.runner_last_sync_utc = None;
        status.runner_last_sync_error =
            Some("runner enabled but runner control state is unavailable".to_string());
    }
    status
}

fn process_rss_mib() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kib = rest.split_whitespace().next()?.parse::<u64>().ok()?;
            return Some(kib / 1024);
        }
    }
    None
}

pub(super) async fn web_root() -> Redirect {
    Redirect::temporary("/versions/")
}

pub(super) async fn web_versions_redirect() -> Redirect {
    Redirect::temporary("/versions/")
}

pub(super) async fn web_favicon_svg() -> impl IntoResponse {
    (
        [(
            CONTENT_TYPE,
            HeaderValue::from_static("image/svg+xml; charset=utf-8"),
        )],
        DASHBOARD_FAVICON_SVG,
    )
}

pub(super) async fn web_favicon_ico_redirect() -> Redirect {
    Redirect::permanent("/favicon.svg")
}

pub(super) async fn web_versions(State(state): State<WebUiState>) -> impl IntoResponse {
    let request_started = Instant::now();
    let cache_key = "page:/versions/".to_string();
    if let Some(html) = state.cache.get_page(&cache_key) {
        return Html(inject_server_timing_badge(
            &html,
            request_started.elapsed().as_millis(),
        ))
        .into_response();
    }
    let store = state.store.clone();
    let cache = state.cache.clone();
    let repo_root = state.repo_root.clone();
    let runner_owner_prefix = state.runner_owner_prefix.clone();
    let runner_control = state.runner_control.clone();
    let show_db_size_link = state.snapshot_manifest.is_none();
    let show_live_queue = state.runner_enabled;
    match tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let rss_mib_start = process_rss_mib().unwrap_or(0);
        let report = if let Some(indexed) = load_versions_cards_index(&store)? {
            indexed
        } else {
            let summary = rebuild_versions_cards_index(&store, &repo_root)?;
            info!(
                "web /versions rebuilt versions summary index cards={} unattributed={} index_bytes={} elapsed_ms={}",
                summary.card_count,
                summary.unattributed_actions,
                summary.index_bytes,
                summary.elapsed_ms
            );
            load_versions_cards_index(&store)?.ok_or_else(|| {
                anyhow::anyhow!(
                    "versions summary index rebuild completed but index remained unavailable"
                )
            })?
        };
        let after_cards = Instant::now();
        let unprocessed = build_unprocessed_version_rows(&store, &repo_root, &report.cards)?;
        let after_unprocessed = Instant::now();
        let db_size_bytes = store.artifacts_db_size_bytes().ok();
        let live_status = if show_live_queue {
            let status = build_queue_live_status(&store, &repo_root, runner_owner_prefix.as_deref())?;
            apply_runner_control_status(status, true, runner_control.as_deref())
        } else {
            QueueLiveStatusView {
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
            }
        };
        let after_queue = Instant::now();
        let html = render_versions_html(
            &report.cards,
            &report.unattributed_actions,
            &unprocessed,
            &live_status,
            show_live_queue,
            show_db_size_link,
            db_size_bytes,
            Utc::now(),
        );
        cache.put_page(cache_key, html.clone());
        let rss_mib_end = process_rss_mib().unwrap_or(0);
        info!(
            "web /versions build_cards_ms={} build_unprocessed_ms={} build_queue_ms={} render_ms={} total_ms={} cards={} unattributed={} unprocessed={} rss_mib_start={} rss_mib_end={}",
            (after_cards - started).as_millis(),
            (after_unprocessed - after_cards).as_millis(),
            (after_queue - after_unprocessed).as_millis(),
            after_queue.elapsed().as_millis(),
            started.elapsed().as_millis(),
            report.cards.len(),
            report.unattributed_actions.len(),
            unprocessed.len(),
            rss_mib_start,
            rss_mib_end,
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
            format!("failed building /versions/ view: {:#}", err),
        )
            .into_response(),
        Err(join_err) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("web handler join error: {}", join_err),
        )
            .into_response(),
    }
}

pub(super) async fn web_queue_status(State(state): State<WebUiState>) -> impl IntoResponse {
    let store = state.store.clone();
    let repo_root = state.repo_root.clone();
    let runner_owner_prefix = state.runner_owner_prefix.clone();
    let runner_control = state.runner_control.clone();
    let runner_enabled = state.runner_enabled;
    match tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let base = build_queue_live_status(&store, &repo_root, runner_owner_prefix.as_deref())?;
        let status = apply_runner_control_status(base, runner_enabled, runner_control.as_deref());
        info!(
            "web /api/queue-status pending={} pending_expanders={} pending_non_expanders={} lower_bound={} running={} running_expanders={} running_non_expanders={} running_owned={} running_foreign={} paused={} drained={} sync_pending={} failed={} canceled={} done={} elapsed_ms={}",
            status.pending,
            status.pending_expanders,
            status.pending_non_expanders,
            status.pending_is_lower_bound,
            status.running,
            status.running_expanders,
            status.running_non_expanders,
            status.running_owned,
            status.running_foreign,
            status.runner_paused,
            status.runner_drained,
            status.runner_sync_pending,
            status.failed,
            status.canceled,
            status.done,
            started.elapsed().as_millis()
        );
        Ok::<_, anyhow::Error>(status)
    })
    .await
    {
        Ok(Ok(status)) => Json(status).into_response(),
        Ok(Err(err)) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed building queue status: {:#}", err),
        )
            .into_response(),
        Err(join_err) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("web handler join error: {}", join_err),
        )
            .into_response(),
    }
}

pub(super) async fn web_runner_pause(State(state): State<WebUiState>) -> impl IntoResponse {
    web_runner_set_paused(state, true).await
}

pub(super) async fn web_runner_resume(State(state): State<WebUiState>) -> impl IntoResponse {
    web_runner_set_paused(state, false).await
}

async fn web_runner_set_paused(state: WebUiState, paused: bool) -> axum::response::Response {
    let pause_state_label = if paused { "paused" } else { "active" };
    if !state.runner_enabled {
        return (
            axum::http::StatusCode::NOT_FOUND,
            "runner control endpoints are unavailable on this instance",
        )
            .into_response();
    }
    let Some(control) = state.runner_control.clone() else {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "runner control state is unavailable on this instance",
        )
            .into_response();
    };

    if paused {
        control.request_pause();
    } else {
        control.request_resume();
    }

    let store = state.store.clone();
    let repo_root = state.repo_root.clone();
    let runner_owner_prefix = state.runner_owner_prefix.clone();
    let runner_enabled = state.runner_enabled;
    match tokio::task::spawn_blocking(move || {
        let base = build_queue_live_status(&store, &repo_root, runner_owner_prefix.as_deref())?;
        let status = apply_runner_control_status(base, runner_enabled, Some(control.as_ref()));
        Ok::<_, anyhow::Error>(status)
    })
    .await
    {
        Ok(Ok(status)) => Json(status).into_response(),
        Ok(Err(err)) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "failed applying runner state change to {}: {:#}",
                pause_state_label, err
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

pub(super) async fn web_enqueue_crate_version(
    State(state): State<WebUiState>,
    Form(form): Form<EnqueueCrateVersionForm>,
) -> impl IntoResponse {
    if state.snapshot_manifest.is_some() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "enqueue is disabled in static snapshot mode",
        )
            .into_response();
    }
    if !state.runner_enabled {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "enqueue is disabled: embedded queue runner is not enabled for this web instance",
        )
            .into_response();
    }
    let store = state.store.clone();
    let repo_root = state.repo_root.clone();
    let requested_crate = form.crate_version;
    let requested_priority = form.priority;
    let requested_crate_for_worker = requested_crate.clone();
    match tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let result = enqueue_processing_for_crate_version(
            &store,
            &repo_root,
            &requested_crate_for_worker,
            requested_priority,
        );
        if result.is_ok() {
            info!(
                "web /versions/enqueue-crate crate={} priority={} elapsed_ms={}",
                requested_crate_for_worker,
                requested_priority,
                started.elapsed().as_millis()
            );
        }
        result
    })
    .await
    {
        Ok(Ok(())) => Redirect::to("/versions/").into_response(),
        Ok(Err(err)) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "failed enqueueing crate version `{}`: {:#}",
                requested_crate, err
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
