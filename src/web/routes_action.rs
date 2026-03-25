// SPDX-License-Identifier: Apache-2.0

use super::types::*;
use anyhow::{Context, Result, bail};
use axum::{
    body::Body,
    extract::{Path as AxumPath, Query, State},
    http::{
        HeaderValue,
        header::{CONTENT_DISPOSITION, CONTENT_TYPE},
    },
    response::{Html, IntoResponse},
};
use log::info;
use sha2::{Digest, Sha256};
use std::{fs, path::Path, time::Instant};

use crate::query::{html_escape, is_valid_action_id_for_route, normalize_action_output_relpath};
use crate::web::render::{
    inject_server_timing_badge, render_action_detail_html, render_action_not_found_html,
};

pub(super) async fn web_action_detail(
    State(state): State<WebUiState>,
    AxumPath(action_id): AxumPath<String>,
) -> impl IntoResponse {
    if state.snapshot_manifest.is_some() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "action detail is unavailable in static snapshot mode",
        )
            .into_response();
    }
    let request_started = Instant::now();
    if !is_valid_action_id_for_route(&action_id) {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            format!(
                "invalid action id `{}`: expected 64 hex characters",
                action_id
            ),
        )
            .into_response();
    }

    let cache_key = format!("page:/action/{}/", action_id);
    if let Some(html) = state.cache.get_page(&cache_key) {
        return Html(inject_server_timing_badge(
            &html,
            request_started.elapsed().as_millis(),
        ))
        .into_response();
    }
    let cache = state.cache.clone();
    let store = state.store.clone();
    let repo_root = state.repo_root.clone();
    let requested_action_id = action_id.clone();
    match tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let html = render_action_detail_html(&store, &repo_root, &requested_action_id)?;
        info!(
            "web /action/{{id}} action_id={} found={} elapsed_ms={}",
            requested_action_id,
            html.is_some(),
            started.elapsed().as_millis(),
        );
        if let Some(ref rendered) = html {
            cache.put_page(cache_key, rendered.clone());
        }
        Ok::<_, anyhow::Error>(html)
    })
    .await
    {
        Ok(Ok(Some(html))) => Html(inject_server_timing_badge(
            &html,
            request_started.elapsed().as_millis(),
        ))
        .into_response(),
        Ok(Ok(None)) => (
            axum::http::StatusCode::NOT_FOUND,
            Html(inject_server_timing_badge(
                &render_action_not_found_html(&action_id),
                request_started.elapsed().as_millis(),
            )),
        )
            .into_response(),
        Ok(Err(err)) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "failed building /action/{}/ view: {:#}",
                html_escape(&action_id),
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

pub(super) async fn web_action_output_file(
    State(state): State<WebUiState>,
    AxumPath(action_id): AxumPath<String>,
    Query(query): Query<ActionOutputFileQuery>,
) -> impl IntoResponse {
    if state.snapshot_manifest.is_some() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "action output-file download is unavailable in static snapshot mode",
        )
            .into_response();
    }
    if !is_valid_action_id_for_route(&action_id) {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            format!(
                "invalid action id `{}`: expected 64 hex characters",
                action_id
            ),
        )
            .into_response();
    }
    let Some(requested_relpath) = normalize_action_output_relpath(&query.path) else {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            format!("invalid output file path `{}`", query.path),
        )
            .into_response();
    };

    let store = state.store.clone();
    let requested_action_id = action_id.clone();
    let requested_relpath_for_task = requested_relpath.clone();
    match tokio::task::spawn_blocking(move || -> Result<Option<(Vec<u8>, bool, String)>> {
        let started = Instant::now();
        if !store.action_exists(&requested_action_id) {
            info!(
                "web /action/{{id}}/output-file action_id={} path={} found=false elapsed_ms={}",
                requested_action_id,
                requested_relpath_for_task,
                started.elapsed().as_millis(),
            );
            return Ok(None);
        }
        let provenance = store.load_provenance(&requested_action_id)?;
        let Some(manifest_file) = provenance
            .output_files
            .iter()
            .find(|file| file.path.replace('\\', "/") == requested_relpath_for_task)
        else {
            return Ok(None);
        };

        let payload_root = store
            .materialize_action_dir(&requested_action_id)?
            .join("payload");
        let file_path = payload_root.join(&requested_relpath_for_task);
        if !file_path.is_file() {
            return Ok(None);
        }

        let bytes = fs::read(&file_path)
            .with_context(|| format!("reading output file: {}", file_path.display()))?;
        let actual_sha = {
            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            hex::encode(hasher.finalize())
        };
        if actual_sha != manifest_file.sha256 {
            bail!(
                "output file sha256 mismatch for action {} path {}: expected {} got {}",
                requested_action_id,
                requested_relpath_for_task,
                manifest_file.sha256,
                actual_sha
            );
        }
        let is_utf8 = std::str::from_utf8(&bytes).is_ok();
        info!(
            "web /action/{{id}}/output-file action_id={} path={} bytes={} utf8={} elapsed_ms={}",
            requested_action_id,
            requested_relpath_for_task,
            bytes.len(),
            is_utf8,
            started.elapsed().as_millis(),
        );

        Ok(Some((bytes, is_utf8, requested_relpath_for_task)))
    })
    .await
    {
        Ok(Ok(Some((bytes, is_utf8, relpath)))) => {
            let content_type = if is_utf8 {
                "text/plain; charset=utf-8"
            } else {
                "application/octet-stream"
            };
            let filename = Path::new(&relpath)
                .file_name()
                .and_then(|f| f.to_str())
                .unwrap_or("output.bin");
            let disposition_value = format!(
                "inline; filename=\"{}\"",
                filename.replace('"', "").replace('\\', "_")
            );
            let mut response = axum::response::Response::new(Body::from(bytes));
            *response.status_mut() = axum::http::StatusCode::OK;
            response
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
            if let Ok(value) = HeaderValue::from_str(&disposition_value) {
                response.headers_mut().insert(CONTENT_DISPOSITION, value);
            }
            response.into_response()
        }
        Ok(Ok(None)) => (
            axum::http::StatusCode::NOT_FOUND,
            format!(
                "output file `{}` not found for action `{}`",
                requested_relpath, action_id
            ),
        )
            .into_response(),
        Ok(Err(err)) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "failed reading output file for /action/{}/output-file: {:#}",
                html_escape(&action_id),
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
