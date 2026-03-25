// SPDX-License-Identifier: Apache-2.0

use super::types::*;
use axum::{
    extract::{Query, State},
    response::{Html, IntoResponse, Redirect},
};
use chrono::Utc;
use log::info;
use std::time::Instant;

use crate::sled_space::analyze_sled_space;
use crate::web::render::{inject_server_timing_badge, render_db_size_html};

const DB_SIZE_DEFAULT_TOP: usize = 25;
const DB_SIZE_DEFAULT_SAMPLE: usize = 40;
const DB_SIZE_MAX_TOP: usize = 250;
const DB_SIZE_MAX_SAMPLE: usize = 250;

fn clamp_query_usize(raw: Option<usize>, default: usize, max: usize) -> usize {
    raw.unwrap_or(default).max(1).min(max)
}

pub(super) async fn web_db_size_redirect() -> Redirect {
    Redirect::temporary("/db-size/")
}

pub(super) async fn web_db_size(
    State(state): State<WebUiState>,
    Query(query): Query<DbSizeQuery>,
) -> impl IntoResponse {
    if state.snapshot_manifest.is_some() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "/db-size is unavailable in static snapshot mode".to_string(),
        )
            .into_response();
    }
    let request_started = Instant::now();
    let top = clamp_query_usize(query.top, DB_SIZE_DEFAULT_TOP, DB_SIZE_MAX_TOP);
    let sample = clamp_query_usize(query.sample, DB_SIZE_DEFAULT_SAMPLE, DB_SIZE_MAX_SAMPLE);
    let cache_key = format!("page:/db-size/?top={top}&sample={sample}");

    if let Some(html) = state.cache.get_page(&cache_key) {
        return Html(inject_server_timing_badge(
            &html,
            request_started.elapsed().as_millis(),
        ))
        .into_response();
    }

    let _permit = match state.heavy_route_limit.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            info!("web throttle route=/db-size/");
            return (
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                "too many in-flight heavy requests; retry shortly".to_string(),
            )
                .into_response();
        }
    };

    let store = state.store.clone();
    let cache = state.cache.clone();
    match tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let db_path = store.artifacts_sled_db_path();
        let report = analyze_sled_space(&db_path, top, sample)?;
        let html = render_db_size_html(&report, top, sample, Utc::now());
        cache.put_page(cache_key, html.clone());
        info!(
            "web /db-size top={} sample={} trees={} total_entries={} total_logical_bytes={} elapsed_ms={}",
            top,
            sample,
            report.tree_count,
            report.total_entries,
            report.total_logical_bytes,
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
            format!("failed building /db-size/ view: {:#}", err),
        )
            .into_response(),
        Err(join_err) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("web handler join error: {}", join_err),
        )
            .into_response(),
    }
}
