use super::types::*;
use axum::{Json, extract::State, response::IntoResponse};
use chrono::Utc;
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};
use std::time::Instant;

use crate::model::{ActionSpec, CommandTrace};
use crate::query::{
    build_associated_ir_view, filter_ir_fn_corpus_g8r_vs_yosys_samples,
    load_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index,
    load_ir_fn_corpus_g8r_vs_yosys_dataset_index,
    rebuild_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index,
    rebuild_ir_fn_corpus_g8r_vs_yosys_dataset_index,
};
use crate::queue::action_dependency_action_ids;
use crate::versioning::normalize_tag_version;
use crate::view::{AssociatedIrView, StdlibG8rVsYosysSample};

const JSONRPC_VERSION: &str = "2.0";
const METHOD_QUERY_IR_FN_CORPUS_G8R_VS_YOSYS: &str = "query.ir_fn_corpus_g8r_vs_yosys_abc_samples";
const METHOD_QUERY_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS: &str =
    "query.ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_samples";
const METHOD_QUERY_IR_FN_SAMPLE_EXPLAIN: &str = "query.ir_fn_sample_explain";
const DEFAULT_RESULT_LIMIT: usize = 100;
const MAX_RESULT_LIMIT: usize = 1000;
const DEFAULT_RESULT_CURSOR: usize = 0;

#[derive(Debug, Clone, Deserialize)]
pub(super) struct JsonRpcRequest {
    jsonrpc: Option<String>,
    method: String,
    #[serde(default)]
    params: Option<Value>,
    #[serde(default)]
    id: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
    id: Value,
}

#[derive(Debug, Clone, Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Clone, Deserialize)]
struct QueryIrFnCorpusG8rVsYosysParams {
    crate_version: String,
    yosys_levels_lt: f64,
    ir_node_count_lt: u64,
    sort_key: IrFnCorpusSampleSortKey,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IrFnCorpusSampleSortKey {
    G8rProductOverYosysProductDesc,
    G8rProductLossDesc,
    YosysLevelsAscIrNodesAsc,
}

#[derive(Debug, Clone, Serialize)]
struct QueryIrFnCorpusG8rVsYosysResult {
    generated_utc: chrono::DateTime<Utc>,
    crate_version: String,
    yosys_levels_lt: f64,
    ir_node_count_lt: u64,
    sort_key: IrFnCorpusSampleSortKey,
    total_matched: usize,
    returned_count: usize,
    samples: Vec<IrFnCorpusSampleWithAssociatedIr>,
}

#[derive(Debug, Clone, Deserialize)]
struct QueryIrFnCorpusG8rAbcVsCodegenYosysParams {
    crate_version: String,
    #[serde(default)]
    max_ir_nodes: Option<u64>,
    #[serde(default)]
    losses_only: bool,
    #[serde(default)]
    strict_loss_only: bool,
    sort_key: IrFnCorpusSampleSortKey,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    cursor: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
struct QueryIrFnCorpusG8rAbcVsCodegenYosysResult {
    generated_utc: chrono::DateTime<Utc>,
    crate_version: String,
    max_ir_nodes: u64,
    losses_only: bool,
    strict_loss_only: bool,
    sort_key: IrFnCorpusSampleSortKey,
    total_matched: usize,
    returned_count: usize,
    cursor: usize,
    next_cursor: Option<usize>,
    samples: Vec<IrFnCorpusFrontendCompareSampleWithAssociatedIr>,
}

#[derive(Debug, Clone, Serialize)]
struct IrFnCorpusLossSummary {
    g8r_nodes_minus_rhs_nodes: f64,
    g8r_levels_minus_rhs_levels: f64,
    g8r_product_minus_rhs_product: f64,
    strict_loss: bool,
    loss_class: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct IrFnCorpusFrontendCompareSampleWithAssociatedIr {
    sample: StdlibG8rVsYosysSample,
    associated_ir: AssociatedIrView,
    loss_summary: IrFnCorpusLossSummary,
}

#[derive(Debug, Clone, Serialize)]
struct IrFnCorpusSampleWithAssociatedIr {
    sample: StdlibG8rVsYosysSample,
    associated_ir: AssociatedIrView,
}

#[derive(Debug, Clone, Deserialize)]
struct QueryIrFnSampleExplainParams {
    g8r_stats_action_id: String,
    yosys_abc_stats_action_id: String,
    #[serde(default)]
    max_upstream_actions_per_branch: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
struct QueryIrFnSampleExplainResult {
    generated_utc: chrono::DateTime<Utc>,
    g8r_stats_action_id: String,
    yosys_abc_stats_action_id: String,
    loss_summary: Option<IrFnCorpusLossSummary>,
    associated_ir: Option<AssociatedIrView>,
    g8r_branch_upstream: Vec<ExplainActionRow>,
    yosys_abc_branch_upstream: Vec<ExplainActionRow>,
    common_upstream_action_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ExplainActionRow {
    depth_hops: usize,
    action_id: String,
    action: ActionSpec,
    created_utc: chrono::DateTime<Utc>,
    output_artifact: crate::model::ArtifactRef,
    dependency_action_ids: Vec<String>,
    commands: Vec<CommandTrace>,
    details: Value,
}

fn clamp_result_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(DEFAULT_RESULT_LIMIT)
        .clamp(1, MAX_RESULT_LIMIT)
}

fn g8r_over_yosys_product_ratio(sample: &StdlibG8rVsYosysSample) -> f64 {
    let numerator = sample.g8r_product;
    let denominator = sample.yosys_abc_product;
    if denominator == 0.0 {
        if numerator > 0.0 {
            f64::INFINITY
        } else if numerator < 0.0 {
            f64::NEG_INFINITY
        } else {
            1.0
        }
    } else {
        numerator / denominator
    }
}

fn stable_sample_tie_break(a: &StdlibG8rVsYosysSample, b: &StdlibG8rVsYosysSample) -> Ordering {
    a.fn_key
        .cmp(&b.fn_key)
        .then(a.ir_action_id.cmp(&b.ir_action_id))
        .then(
            a.ir_top
                .as_deref()
                .unwrap_or("")
                .cmp(b.ir_top.as_deref().unwrap_or("")),
        )
        .then(a.g8r_stats_action_id.cmp(&b.g8r_stats_action_id))
        .then(
            a.yosys_abc_stats_action_id
                .cmp(&b.yosys_abc_stats_action_id),
        )
}

fn sample_loss_summary(sample: &StdlibG8rVsYosysSample) -> IrFnCorpusLossSummary {
    let nodes_delta = sample.g8r_nodes - sample.yosys_abc_nodes;
    let levels_delta = sample.g8r_levels - sample.yosys_abc_levels;
    let product_delta = sample.g8r_product - sample.yosys_abc_product;
    let strict_loss = nodes_delta > 0.0 && levels_delta > 0.0;
    let strict_win = nodes_delta < 0.0 && levels_delta < 0.0;
    let loss_class = if strict_loss {
        "strict_loss"
    } else if strict_win {
        "strict_win"
    } else if nodes_delta == 0.0 && levels_delta == 0.0 {
        "tie"
    } else {
        "mixed"
    };
    IrFnCorpusLossSummary {
        g8r_nodes_minus_rhs_nodes: nodes_delta,
        g8r_levels_minus_rhs_levels: levels_delta,
        g8r_product_minus_rhs_product: product_delta,
        strict_loss,
        loss_class,
    }
}

fn sample_matches_frontend_compare_filters(
    sample: &StdlibG8rVsYosysSample,
    crate_version: &str,
    max_ir_nodes: u64,
    losses_only: bool,
    strict_loss_only: bool,
) -> bool {
    if normalize_tag_version(&sample.crate_version) != crate_version {
        return false;
    }
    if sample.ir_node_count > max_ir_nodes {
        return false;
    }
    let summary = sample_loss_summary(sample);
    if strict_loss_only && !summary.strict_loss {
        return false;
    }
    if losses_only
        && !(summary.g8r_product_minus_rhs_product.is_finite()
            && summary.g8r_product_minus_rhs_product > 0.0)
    {
        return false;
    }
    true
}

fn sort_ir_fn_corpus_samples(
    samples: &mut [StdlibG8rVsYosysSample],
    sort_key: IrFnCorpusSampleSortKey,
) {
    match sort_key {
        IrFnCorpusSampleSortKey::G8rProductOverYosysProductDesc => {
            samples.sort_by(|a, b| {
                let ratio_a = g8r_over_yosys_product_ratio(a);
                let ratio_b = g8r_over_yosys_product_ratio(b);
                ratio_b
                    .total_cmp(&ratio_a)
                    .then(b.g8r_product_loss.total_cmp(&a.g8r_product_loss))
                    .then(b.g8r_product.total_cmp(&a.g8r_product))
                    .then(a.yosys_abc_product.total_cmp(&b.yosys_abc_product))
                    .then(stable_sample_tie_break(a, b))
            });
        }
        IrFnCorpusSampleSortKey::G8rProductLossDesc => {
            samples.sort_by(|a, b| {
                b.g8r_product_loss
                    .total_cmp(&a.g8r_product_loss)
                    .then(b.g8r_product.total_cmp(&a.g8r_product))
                    .then(a.yosys_abc_product.total_cmp(&b.yosys_abc_product))
                    .then(stable_sample_tie_break(a, b))
            });
        }
        IrFnCorpusSampleSortKey::YosysLevelsAscIrNodesAsc => {
            samples.sort_by(|a, b| {
                a.yosys_abc_levels
                    .total_cmp(&b.yosys_abc_levels)
                    .then(a.ir_node_count.cmp(&b.ir_node_count))
                    .then(stable_sample_tie_break(a, b))
            });
        }
    }
}

fn jsonrpc_ok(id: Value, result: Value) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: JSONRPC_VERSION,
        result: Some(result),
        error: None,
        id,
    }
}

fn jsonrpc_error(id: Value, code: i64, message: impl Into<String>) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: JSONRPC_VERSION,
        result: None,
        error: Some(JsonRpcError {
            code,
            message: message.into(),
        }),
        id,
    }
}

fn clamp_result_cursor(cursor: Option<usize>) -> usize {
    cursor.unwrap_or(DEFAULT_RESULT_CURSOR)
}

fn extract_ir_context_from_stats_action(
    store: &crate::store::ArtifactStore,
    stats_action_id: &str,
) -> Option<(String, Option<String>)> {
    let stats_prov = store.load_provenance(stats_action_id).ok()?;
    let ActionSpec::DriverAigToStats { aig_action_id, .. } = stats_prov.action else {
        return None;
    };
    let producer = store.load_provenance(&aig_action_id).ok()?;
    match producer.action {
        ActionSpec::AigToYosysAbcAig {
            aig_action_id: upstream_aig_action_id,
            ..
        } => {
            let g8r = store.load_provenance(&upstream_aig_action_id).ok()?;
            let ActionSpec::DriverIrToG8rAig {
                ir_action_id,
                top_fn_name,
                ..
            } = g8r.action
            else {
                return None;
            };
            Some((ir_action_id, top_fn_name))
        }
        ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id, ..
        } => {
            let verilog = store.load_provenance(&verilog_action_id).ok()?;
            let ActionSpec::IrFnToCombinationalVerilog {
                ir_action_id,
                top_fn_name,
                ..
            } = verilog.action
            else {
                return None;
            };
            Some((ir_action_id, top_fn_name))
        }
        ActionSpec::DriverIrToG8rAig {
            ir_action_id,
            top_fn_name,
            ..
        } => Some((ir_action_id, top_fn_name)),
        _ => None,
    }
}

fn build_upstream_actions(
    store: &crate::store::ArtifactStore,
    start_action_id: &str,
    max_nodes: usize,
) -> Vec<ExplainActionRow> {
    let mut seen = HashSet::<String>::new();
    let mut queue = VecDeque::<(String, usize)>::new();
    let mut rows = Vec::<ExplainActionRow>::new();
    queue.push_back((start_action_id.to_string(), 0));
    while let Some((action_id, depth_hops)) = queue.pop_front() {
        if rows.len() >= max_nodes || !seen.insert(action_id.clone()) {
            continue;
        }
        let Ok(provenance) = store.load_provenance(&action_id) else {
            continue;
        };
        let dependency_action_ids: Vec<String> = action_dependency_action_ids(&provenance.action)
            .into_iter()
            .map(|v| v.to_string())
            .collect();
        rows.push(ExplainActionRow {
            depth_hops,
            action_id: provenance.action_id.clone(),
            action: provenance.action.clone(),
            created_utc: provenance.created_utc,
            output_artifact: provenance.output_artifact.clone(),
            dependency_action_ids: dependency_action_ids.clone(),
            commands: provenance.commands.clone(),
            details: provenance.details.clone(),
        });
        for dep in dependency_action_ids {
            queue.push_back((dep, depth_hops + 1));
        }
    }
    rows.sort_by(|a, b| {
        a.depth_hops
            .cmp(&b.depth_hops)
            .then(a.action_id.cmp(&b.action_id))
    });
    rows
}

pub(super) async fn web_jsonrpc(
    State(state): State<WebUiState>,
    Json(request): Json<JsonRpcRequest>,
) -> impl IntoResponse {
    let request_started = Instant::now();
    let id = request.id.unwrap_or(Value::Null);
    if request.jsonrpc.as_deref() != Some(JSONRPC_VERSION) {
        return Json(jsonrpc_error(
            id,
            -32600,
            "invalid request: expected jsonrpc=\"2.0\"",
        ))
        .into_response();
    }
    if state.snapshot_manifest.is_some() {
        return Json(jsonrpc_error(
            id,
            -32010,
            "jsonrpc queries are unavailable in static snapshot mode",
        ))
        .into_response();
    }

    let response = match request.method.as_str() {
        METHOD_QUERY_IR_FN_CORPUS_G8R_VS_YOSYS => {
            let raw_params = request.params.unwrap_or(Value::Null);
            let params: QueryIrFnCorpusG8rVsYosysParams = match serde_json::from_value(raw_params) {
                Ok(params) => params,
                Err(err) => {
                    return Json(jsonrpc_error(
                        id,
                        -32602,
                        format!("invalid params for method `{}`: {}", request.method, err),
                    ))
                    .into_response();
                }
            };
            if !params.yosys_levels_lt.is_finite() {
                return Json(jsonrpc_error(
                    id,
                    -32602,
                    "invalid params: yosys_levels_lt must be a finite number",
                ))
                .into_response();
            }
            let crate_version = normalize_tag_version(params.crate_version.trim()).to_string();
            if crate_version.is_empty() {
                return Json(jsonrpc_error(
                    id,
                    -32602,
                    "invalid params: crate_version must be non-empty",
                ))
                .into_response();
            }

            let limit = clamp_result_limit(params.limit);
            let cache = state.cache.clone();
            let store = state.store.clone();
            let repo_root = state.repo_root.clone();
            let _permit = match state.heavy_route_limit.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    return Json(jsonrpc_error(
                        id,
                        -32001,
                        "too many in-flight heavy requests; retry shortly",
                    ))
                    .into_response();
                }
            };
            match tokio::task::spawn_blocking(move || {
                let started = Instant::now();
                let dataset = cache.get_or_compute_ir_fn_corpus_g8r_vs_yosys_dataset(|| {
                    if let Some(indexed) = load_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store)? {
                        return Ok(indexed);
                    }
                    let summary = rebuild_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store, &repo_root)?;
                    info!(
                        "web /api/jsonrpc rebuilt ir-fn-corpus g8r-vs-yosys index samples={} versions={}",
                        summary.sample_count, summary.crate_versions
                    );
                    load_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store)?.ok_or_else(|| {
                        anyhow::anyhow!(
                            "ir-fn-corpus index rebuild completed but index remained unavailable"
                        )
                    })
                })?;

                if dataset.available_crate_versions.binary_search(&crate_version).is_err() {
                    anyhow::bail!(
                        "crate version v{} is unavailable; available versions: {}",
                        crate_version,
                        dataset.available_crate_versions.join(", ")
                    );
                }

                let matched = filter_ir_fn_corpus_g8r_vs_yosys_samples(
                    dataset.as_ref(),
                    &crate_version,
                    params.yosys_levels_lt,
                    params.ir_node_count_lt,
                );
                let mut matched = matched;
                let total_matched = matched.len();
                sort_ir_fn_corpus_samples(&mut matched, params.sort_key);
                let mut rows = Vec::new();
                for sample in matched.into_iter().take(limit) {
                    let associated_ir = build_associated_ir_view(
                        &store,
                        &sample.ir_action_id,
                        sample.ir_top.as_deref(),
                    )?;
                    rows.push(IrFnCorpusSampleWithAssociatedIr {
                        sample,
                        associated_ir,
                    });
                }
                info!(
                    "web /api/jsonrpc method={} crate_version=v{} yosys_levels_lt={} ir_node_count_lt={} total_matched={} returned={} elapsed_ms={}",
                    METHOD_QUERY_IR_FN_CORPUS_G8R_VS_YOSYS,
                    crate_version,
                    params.yosys_levels_lt,
                    params.ir_node_count_lt,
                    total_matched,
                    rows.len(),
                    started.elapsed().as_millis()
                );
                Ok::<QueryIrFnCorpusG8rVsYosysResult, anyhow::Error>(QueryIrFnCorpusG8rVsYosysResult {
                    generated_utc: Utc::now(),
                    crate_version: format!("v{}", crate_version),
                    yosys_levels_lt: params.yosys_levels_lt,
                    ir_node_count_lt: params.ir_node_count_lt,
                    sort_key: params.sort_key,
                    total_matched,
                    returned_count: rows.len(),
                    samples: rows,
                })
            })
            .await
            {
                Ok(Ok(result)) => match serde_json::to_value(result) {
                    Ok(value) => Json(jsonrpc_ok(id, value)).into_response(),
                    Err(err) => Json(jsonrpc_error(
                        id,
                        -32603,
                        format!("internal error serializing result: {}", err),
                    ))
                    .into_response(),
                },
                Ok(Err(err)) => {
                    Json(jsonrpc_error(id, -32000, format!("query failed: {:#}", err)))
                        .into_response()
                }
                Err(join_err) => Json(jsonrpc_error(
                    id,
                    -32603,
                    format!("internal join error: {}", join_err),
                ))
                .into_response(),
            }
        }
        METHOD_QUERY_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS => {
            let raw_params = request.params.unwrap_or(Value::Null);
            let params: QueryIrFnCorpusG8rAbcVsCodegenYosysParams =
                match serde_json::from_value(raw_params) {
                    Ok(params) => params,
                    Err(err) => {
                        return Json(jsonrpc_error(
                            id,
                            -32602,
                            format!("invalid params for method `{}`: {}", request.method, err),
                        ))
                        .into_response();
                    }
                };
            let crate_version = normalize_tag_version(params.crate_version.trim()).to_string();
            if crate_version.is_empty() {
                return Json(jsonrpc_error(
                    id,
                    -32602,
                    "invalid params: crate_version must be non-empty",
                ))
                .into_response();
            }

            let limit = clamp_result_limit(params.limit);
            let cursor = clamp_result_cursor(params.cursor);
            let cache = state.cache.clone();
            let store = state.store.clone();
            let repo_root = state.repo_root.clone();
            let _permit = match state.heavy_route_limit.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    return Json(jsonrpc_error(
                        id,
                        -32001,
                        "too many in-flight heavy requests; retry shortly",
                    ))
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
                        let summary = rebuild_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index(
                            &store,
                            &repo_root,
                        )?;
                        info!(
                            "web /api/jsonrpc rebuilt ir-fn-corpus g8r+abc-vs-codegen+yosys index samples={} versions={}",
                            summary.sample_count, summary.crate_versions
                        );
                        load_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index(&store)?
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "ir-fn-corpus g8r+abc-vs-codegen+yosys index rebuild completed but index remained unavailable"
                                )
                            })
                    },
                )?;

                if dataset.available_crate_versions.binary_search(&crate_version).is_err() {
                    anyhow::bail!(
                        "crate version v{} is unavailable; available versions: {}",
                        crate_version,
                        dataset.available_crate_versions.join(", ")
                    );
                }

                let max_ir_nodes = params.max_ir_nodes.unwrap_or(dataset.max_ir_nodes);
                let mut matched: Vec<StdlibG8rVsYosysSample> = dataset
                    .samples
                    .iter()
                    .filter(|sample| {
                        sample_matches_frontend_compare_filters(
                            sample,
                            &crate_version,
                            max_ir_nodes,
                            params.losses_only,
                            params.strict_loss_only,
                        )
                    })
                    .cloned()
                    .collect();
                let total_matched = matched.len();
                sort_ir_fn_corpus_samples(&mut matched, params.sort_key);
                let start = cursor.min(total_matched);
                let mut rows = Vec::new();
                for sample in matched.into_iter().skip(start).take(limit) {
                    let associated_ir = build_associated_ir_view(
                        &store,
                        &sample.ir_action_id,
                        sample.ir_top.as_deref(),
                    )?;
                    rows.push(IrFnCorpusFrontendCompareSampleWithAssociatedIr {
                        loss_summary: sample_loss_summary(&sample),
                        sample,
                        associated_ir,
                    });
                }
                let returned_count = rows.len();
                let next_cursor = (start + returned_count < total_matched)
                    .then_some(start + returned_count);
                info!(
                    "web /api/jsonrpc method={} crate_version=v{} max_ir_nodes={} losses_only={} strict_loss_only={} total_matched={} returned={} cursor={} next_cursor={:?} elapsed_ms={}",
                    METHOD_QUERY_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS,
                    crate_version,
                    max_ir_nodes,
                    params.losses_only,
                    params.strict_loss_only,
                    total_matched,
                    returned_count,
                    start,
                    next_cursor,
                    started.elapsed().as_millis()
                );
                Ok::<QueryIrFnCorpusG8rAbcVsCodegenYosysResult, anyhow::Error>(
                    QueryIrFnCorpusG8rAbcVsCodegenYosysResult {
                        generated_utc: Utc::now(),
                        crate_version: format!("v{}", crate_version),
                        max_ir_nodes,
                        losses_only: params.losses_only,
                        strict_loss_only: params.strict_loss_only,
                        sort_key: params.sort_key,
                        total_matched,
                        returned_count,
                        cursor: start,
                        next_cursor,
                        samples: rows,
                    },
                )
            })
            .await
            {
                Ok(Ok(result)) => match serde_json::to_value(result) {
                    Ok(value) => Json(jsonrpc_ok(id, value)).into_response(),
                    Err(err) => Json(jsonrpc_error(
                        id,
                        -32603,
                        format!("internal error serializing result: {}", err),
                    ))
                    .into_response(),
                },
                Ok(Err(err)) => {
                    Json(jsonrpc_error(id, -32000, format!("query failed: {:#}", err)))
                        .into_response()
                }
                Err(join_err) => Json(jsonrpc_error(
                    id,
                    -32603,
                    format!("internal join error: {}", join_err),
                ))
                .into_response(),
            }
        }
        METHOD_QUERY_IR_FN_SAMPLE_EXPLAIN => {
            let raw_params = request.params.unwrap_or(Value::Null);
            let params: QueryIrFnSampleExplainParams = match serde_json::from_value(raw_params) {
                Ok(params) => params,
                Err(err) => {
                    return Json(jsonrpc_error(
                        id,
                        -32602,
                        format!("invalid params for method `{}`: {}", request.method, err),
                    ))
                    .into_response();
                }
            };
            let g8r_stats_action_id = params.g8r_stats_action_id.trim().to_string();
            let yosys_abc_stats_action_id = params.yosys_abc_stats_action_id.trim().to_string();
            if g8r_stats_action_id.is_empty() || yosys_abc_stats_action_id.is_empty() {
                return Json(jsonrpc_error(
                    id,
                    -32602,
                    "invalid params: g8r_stats_action_id and yosys_abc_stats_action_id must be non-empty",
                ))
                .into_response();
            }
            let max_upstream_actions_per_branch = params
                .max_upstream_actions_per_branch
                .unwrap_or(64)
                .clamp(1, 512);
            let cache = state.cache.clone();
            let store = state.store.clone();
            let repo_root = state.repo_root.clone();
            let _permit = match state.heavy_route_limit.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    return Json(jsonrpc_error(
                        id,
                        -32001,
                        "too many in-flight heavy requests; retry shortly",
                    ))
                    .into_response();
                }
            };
            match tokio::task::spawn_blocking(move || {
                let started = Instant::now();
                if !store.action_exists(&g8r_stats_action_id) {
                    anyhow::bail!("g8r_stats_action_id not found: {}", g8r_stats_action_id);
                }
                if !store.action_exists(&yosys_abc_stats_action_id) {
                    anyhow::bail!(
                        "yosys_abc_stats_action_id not found: {}",
                        yosys_abc_stats_action_id
                    );
                }

                let dataset_frontend =
                    cache.get_or_compute_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset(
                        || {
                            if let Some(indexed) =
                                load_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index(
                                    &store,
                                )?
                            {
                                return Ok(indexed);
                            }
                            rebuild_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index(
                                &store, &repo_root,
                            )?;
                            load_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index(&store)?
                                .ok_or_else(|| {
                                    anyhow::anyhow!(
                                        "frontend compare index rebuild completed but index remained unavailable"
                                    )
                                })
                        },
                    )?;
                let mut sample = dataset_frontend
                    .samples
                    .iter()
                    .find(|s| {
                        s.g8r_stats_action_id == g8r_stats_action_id
                            && s.yosys_abc_stats_action_id == yosys_abc_stats_action_id
                    })
                    .cloned();
                if sample.is_none() {
                    let dataset_default = cache.get_or_compute_ir_fn_corpus_g8r_vs_yosys_dataset(
                        || {
                            if let Some(indexed) =
                                load_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store)?
                            {
                                return Ok(indexed);
                            }
                            rebuild_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store, &repo_root)?;
                            load_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store)?.ok_or_else(
                                || {
                                    anyhow::anyhow!(
                                        "ir-fn-corpus g8r-vs-yosys index rebuild completed but index remained unavailable"
                                    )
                                },
                            )
                        },
                    )?;
                    sample = dataset_default
                        .samples
                        .iter()
                        .find(|s| {
                            s.g8r_stats_action_id == g8r_stats_action_id
                                && s.yosys_abc_stats_action_id == yosys_abc_stats_action_id
                        })
                        .cloned();
                }

                let associated_ir = if let Some(sample) = sample.as_ref() {
                    Some(build_associated_ir_view(
                        &store,
                        &sample.ir_action_id,
                        sample.ir_top.as_deref(),
                    )?)
                } else if let Some((ir_action_id, ir_top)) =
                    extract_ir_context_from_stats_action(&store, &g8r_stats_action_id)
                {
                    Some(build_associated_ir_view(
                        &store,
                        &ir_action_id,
                        ir_top.as_deref(),
                    )?)
                } else if let Some((ir_action_id, ir_top)) =
                    extract_ir_context_from_stats_action(&store, &yosys_abc_stats_action_id)
                {
                    Some(build_associated_ir_view(
                        &store,
                        &ir_action_id,
                        ir_top.as_deref(),
                    )?)
                } else {
                    None
                };

                let g8r_branch = build_upstream_actions(
                    &store,
                    &g8r_stats_action_id,
                    max_upstream_actions_per_branch,
                );
                let yosys_branch = build_upstream_actions(
                    &store,
                    &yosys_abc_stats_action_id,
                    max_upstream_actions_per_branch,
                );
                let g8r_ids: HashSet<String> =
                    g8r_branch.iter().map(|row| row.action_id.clone()).collect();
                let yosys_ids: HashSet<String> = yosys_branch
                    .iter()
                    .map(|row| row.action_id.clone())
                    .collect();
                let mut common_upstream_action_ids: Vec<String> =
                    g8r_ids.intersection(&yosys_ids).cloned().collect();
                common_upstream_action_ids.sort();

                let result = QueryIrFnSampleExplainResult {
                    generated_utc: Utc::now(),
                    g8r_stats_action_id: g8r_stats_action_id.clone(),
                    yosys_abc_stats_action_id: yosys_abc_stats_action_id.clone(),
                    loss_summary: sample.as_ref().map(sample_loss_summary),
                    associated_ir,
                    g8r_branch_upstream: g8r_branch,
                    yosys_abc_branch_upstream: yosys_branch,
                    common_upstream_action_ids,
                };
                info!(
                    "web /api/jsonrpc method={} g8r_stats_action_id={} yosys_abc_stats_action_id={} sample_found={} g8r_branch={} yosys_branch={} elapsed_ms={}",
                    METHOD_QUERY_IR_FN_SAMPLE_EXPLAIN,
                    g8r_stats_action_id,
                    yosys_abc_stats_action_id,
                    sample.is_some(),
                    result.g8r_branch_upstream.len(),
                    result.yosys_abc_branch_upstream.len(),
                    started.elapsed().as_millis()
                );
                Ok::<QueryIrFnSampleExplainResult, anyhow::Error>(result)
            })
            .await
            {
                Ok(Ok(result)) => match serde_json::to_value(result) {
                    Ok(value) => Json(jsonrpc_ok(id, value)).into_response(),
                    Err(err) => Json(jsonrpc_error(
                        id,
                        -32603,
                        format!("internal error serializing result: {}", err),
                    ))
                    .into_response(),
                },
                Ok(Err(err)) => {
                    Json(jsonrpc_error(id, -32000, format!("query failed: {:#}", err)))
                        .into_response()
                }
                Err(join_err) => Json(jsonrpc_error(
                    id,
                    -32603,
                    format!("internal join error: {}", join_err),
                ))
                .into_response(),
            }
        }
        _ => Json(jsonrpc_error(
            id,
            -32601,
            format!("method not found: {}", request.method),
        ))
        .into_response(),
    };
    info!(
        "web /api/jsonrpc total_ms={}",
        request_started.elapsed().as_millis()
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(
        fn_key: &str,
        ir_action_id: &str,
        g8r_product: f64,
        yosys_product: f64,
        yosys_levels: f64,
        ir_node_count: u64,
    ) -> StdlibG8rVsYosysSample {
        StdlibG8rVsYosysSample {
            fn_key: fn_key.to_string(),
            crate_version: "0.33.0".to_string(),
            dso_version: "0.35.0".to_string(),
            ir_action_id: ir_action_id.to_string(),
            ir_top: None,
            structural_hash: None,
            ir_node_count,
            g8r_nodes: 0.0,
            g8r_levels: 0.0,
            yosys_abc_nodes: 0.0,
            yosys_abc_levels: yosys_levels,
            g8r_product,
            yosys_abc_product: yosys_product,
            g8r_product_loss: g8r_product - yosys_product,
            g8r_stats_action_id: "a".repeat(64),
            yosys_abc_stats_action_id: "b".repeat(64),
        }
    }

    #[test]
    fn sort_ir_fn_corpus_samples_ratio_desc_is_deterministic() {
        let mut rows = vec![
            sample("f2", &"2".repeat(64), 100.0, 10.0, 9.0, 10), // ratio 10
            sample("f1", &"1".repeat(64), 50.0, 10.0, 8.0, 2),   // ratio 5
            sample("f3", &"3".repeat(64), 50.0, 10.0, 7.0, 1),   // ratio 5; tie by fn_key
        ];
        sort_ir_fn_corpus_samples(
            &mut rows,
            IrFnCorpusSampleSortKey::G8rProductOverYosysProductDesc,
        );
        assert_eq!(rows[0].fn_key, "f2");
        assert_eq!(rows[1].fn_key, "f1");
        assert_eq!(rows[2].fn_key, "f3");
    }

    #[test]
    fn sort_ir_fn_corpus_samples_levels_then_nodes() {
        let mut rows = vec![
            sample("f3", &"3".repeat(64), 1.0, 1.0, 2.0, 50),
            sample("f1", &"1".repeat(64), 1.0, 1.0, 1.0, 99),
            sample("f2", &"2".repeat(64), 1.0, 1.0, 1.0, 3),
        ];
        sort_ir_fn_corpus_samples(&mut rows, IrFnCorpusSampleSortKey::YosysLevelsAscIrNodesAsc);
        assert_eq!(rows[0].fn_key, "f2");
        assert_eq!(rows[1].fn_key, "f1");
        assert_eq!(rows[2].fn_key, "f3");
    }

    #[test]
    fn sort_ir_fn_corpus_samples_loss_desc_orders_by_largest_loss() {
        let mut rows = vec![
            sample("f3", &"3".repeat(64), 13.0, 10.0, 2.0, 50), // loss 3
            sample("f1", &"1".repeat(64), 41.0, 10.0, 1.0, 99), // loss 31
            sample("f2", &"2".repeat(64), 22.0, 10.0, 1.0, 3),  // loss 12
        ];
        sort_ir_fn_corpus_samples(&mut rows, IrFnCorpusSampleSortKey::G8rProductLossDesc);
        assert_eq!(rows[0].fn_key, "f1");
        assert_eq!(rows[1].fn_key, "f2");
        assert_eq!(rows[2].fn_key, "f3");
    }

    #[test]
    fn sample_matches_frontend_compare_filters_strict_loss_behavior() {
        let strict_loss = StdlibG8rVsYosysSample {
            fn_key: "strict_loss".to_string(),
            crate_version: "0.33.0".to_string(),
            dso_version: "0.35.0".to_string(),
            ir_action_id: "a".repeat(64),
            ir_top: Some("__top".to_string()),
            structural_hash: Some("1".repeat(64)),
            ir_node_count: 8,
            g8r_nodes: 11.0,
            g8r_levels: 7.0,
            yosys_abc_nodes: 10.0,
            yosys_abc_levels: 6.0,
            g8r_product: 77.0,
            yosys_abc_product: 60.0,
            g8r_product_loss: 17.0,
            g8r_stats_action_id: "b".repeat(64),
            yosys_abc_stats_action_id: "c".repeat(64),
        };
        let mixed = StdlibG8rVsYosysSample {
            fn_key: "mixed".to_string(),
            crate_version: "0.33.0".to_string(),
            dso_version: "0.35.0".to_string(),
            ir_action_id: "d".repeat(64),
            ir_top: Some("__top".to_string()),
            structural_hash: Some("2".repeat(64)),
            ir_node_count: 8,
            g8r_nodes: 11.0,
            g8r_levels: 5.0,
            yosys_abc_nodes: 10.0,
            yosys_abc_levels: 6.0,
            g8r_product: 55.0,
            yosys_abc_product: 60.0,
            g8r_product_loss: -5.0,
            g8r_stats_action_id: "e".repeat(64),
            yosys_abc_stats_action_id: "f".repeat(64),
        };
        assert!(sample_matches_frontend_compare_filters(
            &strict_loss,
            "0.33.0",
            8,
            true,
            true
        ));
        assert!(!sample_matches_frontend_compare_filters(
            &mixed, "0.33.0", 8, true, true
        ));
    }

    #[test]
    fn query_params_require_sort_key() {
        let raw = serde_json::json!({
            "crate_version": "v0.33.0",
            "yosys_levels_lt": 12.0,
            "ir_node_count_lt": 64,
            "limit": 10
        });
        let parsed: Result<QueryIrFnCorpusG8rVsYosysParams, _> = serde_json::from_value(raw);
        assert!(parsed.is_err());
        let err = parsed.unwrap_err().to_string();
        assert!(err.contains("sort_key"), "unexpected parse error: {err}");
    }

    #[test]
    fn frontend_compare_query_params_require_sort_key() {
        let raw = serde_json::json!({
            "crate_version": "v0.33.0",
            "max_ir_nodes": 64,
            "losses_only": true
        });
        let parsed: Result<QueryIrFnCorpusG8rAbcVsCodegenYosysParams, _> =
            serde_json::from_value(raw);
        assert!(parsed.is_err());
        let err = parsed.unwrap_err().to_string();
        assert!(err.contains("sort_key"), "unexpected parse error: {err}");
    }
}
