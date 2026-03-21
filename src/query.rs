use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::fs;
use std::io::Read;
use std::path::{Component, Path};
use std::time::{Duration, Instant};

use crate::app;
use crate::executor::{
    compute_action_id, discover_dslx_fn_to_ir_suggestions, extract_ir_fn_block_by_name,
};
use crate::model::*;
use crate::queue::*;
use crate::runtime::*;
use crate::service::*;
use crate::store::ArtifactStore;
use crate::versioning::*;
use crate::view::*;
use crate::{
    DEFAULT_SUGGESTED_ENQUEUE_MAX_DEPTH, INPUT_IR_FN_STRUCTURAL_HASH_DETAILS_KEY,
    MODULE_SUBTREE_ROOT_PATHS, VERSION_COMPAT_PATH,
    WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME,
    WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_SCHEMA_VERSION,
    WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME,
    WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION,
    WEB_STDLIB_FILE_ACTION_GRAPH_INDEX_FILENAME, WEB_STDLIB_FILE_ACTION_GRAPH_INDEX_SCHEMA_VERSION,
    WEB_STDLIB_FN_TIMELINE_INDEX_FILENAME, WEB_STDLIB_FN_TIMELINE_INDEX_SCHEMA_VERSION,
    WEB_STDLIB_FNS_TREND_G8R_FRAIG_FALSE_INDEX_FILENAME,
    WEB_STDLIB_FNS_TREND_G8R_FRAIG_TRUE_INDEX_FILENAME, WEB_STDLIB_FNS_TREND_INDEX_SCHEMA_VERSION,
    WEB_STDLIB_FNS_TREND_YOSYS_ABC_INDEX_FILENAME,
    WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME,
    WEB_STDLIB_G8R_VS_YOSYS_FRAIG_TRUE_INDEX_FILENAME,
    WEB_STDLIB_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION, WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
    WEB_VERSIONS_SUMMARY_INDEX_SCHEMA_VERSION,
};

mod corpus_structural;
pub(crate) use corpus_structural::*;

type ProvenanceLookup<'a> = BTreeMap<&'a str, &'a Provenance>;

fn build_provenance_lookup<'a>(provenances: &'a [Provenance]) -> ProvenanceLookup<'a> {
    provenances
        .iter()
        .map(|provenance| (provenance.action_id.as_str(), provenance))
        .collect()
}

pub(crate) fn build_stdlib_fns_trend_dataset(
    store: &ArtifactStore,
    kind: StdlibTrendKind,
    fraig: bool,
    selected_file: Option<&str>,
) -> Result<StdlibFnsTrendDataset> {
    let started = Instant::now();
    let provenances = store.list_provenances_shared()?;
    let provenance_by_action_id = build_provenance_lookup(provenances.as_ref());

    let selected_file = selected_file
        .map(|v| v.trim().replace('\\', "/"))
        .filter(|v| !v.is_empty());
    let mut by_fn_and_crate: BTreeMap<(String, String), BTreeMap<String, StdlibFnTrendPoint>> =
        BTreeMap::new();
    let mut available_file_set = BTreeSet::new();

    for provenance in provenances.iter() {
        let (aig_action_id, dso_version) = match &provenance.action {
            ActionSpec::DriverAigToStats {
                aig_action_id,
                version,
                ..
            } => (
                aig_action_id.as_str(),
                normalize_tag_version(version).to_string(),
            ),
            _ => continue,
        };

        let Some(source_ctx) = extract_stdlib_trend_source_context(
            &provenance_by_action_id,
            kind,
            aig_action_id,
            fraig,
        ) else {
            continue;
        };

        let Some((dslx_file, dslx_fn_name)) = resolve_direct_dslx_origin_from_opt_ir_action(
            &provenance_by_action_id,
            &source_ctx.ir_action_id,
        ) else {
            continue;
        };
        let dslx_file = dslx_file.replace('\\', "/");
        available_file_set.insert(dslx_file.clone());
        if let Some(target_file) = selected_file.as_deref()
            && dslx_file != target_file
        {
            continue;
        }

        let stats_path = store.resolve_artifact_ref_path(&provenance.output_artifact);
        let stats_text = match fs::read_to_string(&stats_path) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let stats_json: serde_json::Value = match serde_json::from_str(&stats_text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(and_nodes) = parse_aig_stats_metric(&stats_json, "and_nodes") else {
            continue;
        };
        let Some(depth) = parse_aig_stats_metric(&stats_json, "depth") else {
            continue;
        };

        let crate_version = source_ctx.crate_version;
        let dso_version = if dso_version.is_empty() {
            source_ctx.dso_version
        } else {
            dso_version
        };
        let point = StdlibFnTrendPoint {
            crate_version: crate_version.clone(),
            dso_version,
            and_nodes,
            depth,
            created_utc: provenance.created_utc,
            stats_action_id: provenance.action_id.clone(),
        };
        let per_crate = by_fn_and_crate
            .entry((dslx_file.clone(), dslx_fn_name.clone()))
            .or_default();
        match per_crate.entry(crate_version) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(point);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let existing = entry.get();
                let should_replace = point.created_utc > existing.created_utc
                    || (point.created_utc == existing.created_utc
                        && point.stats_action_id > existing.stats_action_id);
                if should_replace {
                    entry.insert(point);
                }
            }
        }
    }

    let available_files: Vec<String> = available_file_set.into_iter().collect();
    let selected_file = selected_file.filter(|v| available_files.binary_search(v).is_ok());
    let mut crate_versions = BTreeSet::new();
    let mut series = Vec::new();
    for ((dslx_file, dslx_fn_name), by_crate) in by_fn_and_crate {
        let mut points: Vec<StdlibFnTrendPoint> = by_crate.into_values().collect();
        points.sort_by(|a, b| {
            cmp_dotted_numeric_version(&a.crate_version, &b.crate_version)
                .then(a.created_utc.cmp(&b.created_utc))
                .then(a.stats_action_id.cmp(&b.stats_action_id))
        });
        if points.is_empty() {
            continue;
        }
        for point in &points {
            crate_versions.insert(point.crate_version.clone());
        }
        let fn_key = format!("{dslx_file}::{dslx_fn_name}");
        series.push(StdlibFnTrendSeries { fn_key, points });
    }
    series.sort_by(|a, b| a.fn_key.cmp(&b.fn_key));

    let mut crate_versions: Vec<String> = crate_versions.into_iter().collect();
    crate_versions.sort_by(|a, b| cmp_dotted_numeric_version(a, b));
    let total_points = series.iter().map(|s| s.points.len()).sum();
    let selected_file_label = selected_file.clone().unwrap_or_else(|| "<all>".to_string());
    let dataset = StdlibFnsTrendDataset {
        kind,
        fraig,
        crate_versions,
        series,
        total_points,
        available_files,
        selected_file,
    };
    info!(
        "query build_stdlib_fns_trend_dataset kind={} fraig={} selected_file={} provenances={} series={} points={} elapsed_ms={}",
        kind.view_path(),
        fraig,
        selected_file_label,
        provenances.len(),
        dataset.series.len(),
        dataset.total_points,
        started.elapsed().as_millis()
    );
    Ok(dataset)
}

fn stdlib_fns_trend_index_key(kind: StdlibTrendKind, fraig: bool) -> &'static str {
    match kind {
        StdlibTrendKind::G8r => {
            if fraig {
                WEB_STDLIB_FNS_TREND_G8R_FRAIG_TRUE_INDEX_FILENAME
            } else {
                WEB_STDLIB_FNS_TREND_G8R_FRAIG_FALSE_INDEX_FILENAME
            }
        }
        StdlibTrendKind::YosysAbc => WEB_STDLIB_FNS_TREND_YOSYS_ABC_INDEX_FILENAME,
    }
}

fn apply_selected_file_to_stdlib_fns_trend_dataset(
    dataset: &StdlibFnsTrendDataset,
    selected_file: Option<&str>,
) -> StdlibFnsTrendDataset {
    let requested = selected_file
        .map(|v| v.trim().replace('\\', "/"))
        .filter(|v| !v.is_empty());
    let selected_file = requested
        .as_deref()
        .filter(|v| {
            dataset
                .available_files
                .binary_search_by(|f| f.as_str().cmp(v))
                .is_ok()
        })
        .map(str::to_string);
    let Some(selected_file_ref) = selected_file.as_deref() else {
        let mut unfiltered = dataset.clone();
        unfiltered.selected_file = None;
        return unfiltered;
    };

    let mut crate_versions = BTreeSet::new();
    let mut series = Vec::new();
    let mut total_points = 0usize;
    for entry in &dataset.series {
        let matches_selected_file = entry
            .fn_key
            .split_once("::")
            .map(|(file, _)| file == selected_file_ref)
            .unwrap_or(false);
        if !matches_selected_file {
            continue;
        }
        total_points += entry.points.len();
        for point in &entry.points {
            crate_versions.insert(point.crate_version.clone());
        }
        series.push(entry.clone());
    }
    let mut crate_versions: Vec<String> = crate_versions.into_iter().collect();
    crate_versions.sort_by(|a, b| cmp_dotted_numeric_version(a, b));
    StdlibFnsTrendDataset {
        kind: dataset.kind,
        fraig: dataset.fraig,
        crate_versions,
        series,
        total_points,
        available_files: dataset.available_files.clone(),
        selected_file,
    }
}

pub(crate) fn build_stdlib_fn_version_timeline_dataset(
    store: &ArtifactStore,
    file_selector: &str,
    dslx_fn_name: &str,
    fraig: bool,
    delay_model: &str,
) -> Result<StdlibFnVersionTimelineDataset> {
    let started = Instant::now();
    let provenances = store.list_provenances_shared()?;
    let provenance_by_action_id = build_provenance_lookup(provenances.as_ref());

    let file_selector = file_selector.trim().replace('\\', "/");
    let dslx_fn_name = dslx_fn_name.trim().to_string();
    let delay_model = delay_model.trim().to_string();

    let mut functions_by_file: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for provenance in provenances.iter() {
        let ActionSpec::DriverDslxFnToIr {
            dslx_file,
            dslx_fn_name,
            ..
        } = &provenance.action
        else {
            continue;
        };
        let dslx_file = dslx_file.replace('\\', "/");
        functions_by_file
            .entry(dslx_file)
            .or_default()
            .insert(dslx_fn_name.clone());
    }

    let mut available_files: Vec<String> = functions_by_file.keys().cloned().collect();
    available_files.sort();
    let mut matched_files: Vec<String> = available_files
        .iter()
        .filter(|dslx_file| stdlib_file_selector_matches(&file_selector, dslx_file))
        .cloned()
        .collect();
    matched_files.sort();
    let matched_file_set: BTreeSet<String> = matched_files.iter().cloned().collect();

    let dslx_file = matched_files.first().cloned();
    let available_functions_for_file = dslx_file
        .as_deref()
        .and_then(|file| functions_by_file.get(file))
        .map(|fns| fns.iter().cloned().collect())
        .unwrap_or_else(Vec::new);

    let mut g8r_by_crate: BTreeMap<String, StdlibFnTrendPoint> = BTreeMap::new();
    let mut yosys_by_crate: BTreeMap<String, StdlibFnTrendPoint> = BTreeMap::new();
    let mut delay_by_crate: BTreeMap<String, StdlibFnDelayPoint> = BTreeMap::new();

    for provenance in provenances.iter() {
        if let ActionSpec::DriverAigToStats {
            aig_action_id,
            version,
            ..
        } = &provenance.action
        {
            let stats_dso_version = normalize_tag_version(version).to_string();
            let mut source_ctx_and_kind: Option<(StdlibTrendSourceContext, bool)> = None;
            if let Some(source_ctx) = extract_stdlib_trend_source_context(
                &provenance_by_action_id,
                StdlibTrendKind::G8r,
                aig_action_id,
                fraig,
            ) {
                if let Some((dslx_file, source_fn)) = resolve_direct_dslx_origin_from_opt_ir_action(
                    &provenance_by_action_id,
                    &source_ctx.ir_action_id,
                ) {
                    let dslx_file = dslx_file.replace('\\', "/");
                    if source_fn == dslx_fn_name && matched_file_set.contains(&dslx_file) {
                        source_ctx_and_kind = Some((source_ctx, true));
                    }
                }
            } else if let Some(source_ctx) = extract_stdlib_trend_source_context(
                &provenance_by_action_id,
                StdlibTrendKind::YosysAbc,
                aig_action_id,
                false,
            ) && let Some((dslx_file, source_fn)) =
                resolve_direct_dslx_origin_from_opt_ir_action(
                    &provenance_by_action_id,
                    &source_ctx.ir_action_id,
                )
            {
                let dslx_file = dslx_file.replace('\\', "/");
                if source_fn == dslx_fn_name && matched_file_set.contains(&dslx_file) {
                    source_ctx_and_kind = Some((source_ctx, false));
                }
            }
            let Some((source_ctx, is_g8r)) = source_ctx_and_kind else {
                continue;
            };

            let stats_path = store.resolve_artifact_ref_path(&provenance.output_artifact);
            let stats_text = match fs::read_to_string(&stats_path) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let stats_json: serde_json::Value = match serde_json::from_str(&stats_text) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let Some(and_nodes) = parse_aig_stats_metric(&stats_json, "and_nodes")
                .or_else(|| parse_aig_stats_metric(&stats_json, "live_nodes"))
            else {
                continue;
            };
            let Some(depth) = parse_aig_stats_metric(&stats_json, "depth")
                .or_else(|| parse_aig_stats_metric(&stats_json, "deepest_path"))
            else {
                continue;
            };

            let point = StdlibFnTrendPoint {
                crate_version: source_ctx.crate_version.clone(),
                dso_version: if stats_dso_version.is_empty() {
                    source_ctx.dso_version
                } else {
                    stats_dso_version
                },
                and_nodes,
                depth,
                created_utc: provenance.created_utc,
                stats_action_id: provenance.action_id.clone(),
            };
            if is_g8r {
                upsert_stdlib_fn_trend_point_by_crate(&mut g8r_by_crate, point);
            } else {
                upsert_stdlib_fn_trend_point_by_crate(&mut yosys_by_crate, point);
            }
            continue;
        }

        let ActionSpec::DriverIrToDelayInfo {
            ir_action_id,
            delay_model: action_delay_model,
            version,
            runtime,
            ..
        } = &provenance.action
        else {
            continue;
        };
        if !action_delay_model.eq_ignore_ascii_case(&delay_model) {
            continue;
        }
        let Some((dslx_file, source_fn)) =
            resolve_direct_dslx_origin_from_opt_ir_action(&provenance_by_action_id, ir_action_id)
        else {
            continue;
        };
        let dslx_file = dslx_file.replace('\\', "/");
        if source_fn != dslx_fn_name || !matched_file_set.contains(&dslx_file) {
            continue;
        }
        let delay_info_path = store.resolve_artifact_ref_path(&provenance.output_artifact);
        let delay_info_text = match fs::read_to_string(&delay_info_path) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(delay_ps) = parse_delay_info_total_delay_ps(&delay_info_text) else {
            continue;
        };
        let delay_point = StdlibFnDelayPoint {
            crate_version: normalize_tag_version(&runtime.driver_version).to_string(),
            dso_version: normalize_tag_version(version).to_string(),
            delay_ps,
            created_utc: provenance.created_utc,
            action_id: provenance.action_id.clone(),
        };
        upsert_delay_point_by_crate(&mut delay_by_crate, delay_point);
    }

    let mut crate_versions = BTreeSet::new();
    crate_versions.extend(g8r_by_crate.keys().cloned());
    crate_versions.extend(yosys_by_crate.keys().cloned());
    crate_versions.extend(delay_by_crate.keys().cloned());
    let mut crate_versions: Vec<String> = crate_versions.into_iter().collect();
    crate_versions.sort_by(|a, b| cmp_dotted_numeric_version(a, b));

    let mut points = Vec::new();
    for crate_version in &crate_versions {
        let g8r = g8r_by_crate.get(crate_version);
        let yosys = yosys_by_crate.get(crate_version);
        let delay = delay_by_crate.get(crate_version);
        points.push(StdlibFnVersionTimelinePoint {
            crate_version: crate_version.clone(),
            g8r_dso_version: g8r.map(|p| p.dso_version.clone()),
            yosys_abc_dso_version: yosys.map(|p| p.dso_version.clone()),
            ir_delay_dso_version: delay.map(|p| p.dso_version.clone()),
            g8r_and_nodes: g8r.map(|p| p.and_nodes),
            g8r_levels: g8r.map(|p| p.depth),
            g8r_stats_action_id: g8r.map(|p| p.stats_action_id.clone()),
            yosys_abc_and_nodes: yosys.map(|p| p.and_nodes),
            yosys_abc_levels: yosys.map(|p| p.depth),
            yosys_abc_stats_action_id: yosys.map(|p| p.stats_action_id.clone()),
            ir_delay_ps: delay.map(|p| p.delay_ps),
            ir_delay_action_id: delay.map(|p| p.action_id.clone()),
        });
    }

    let dataset = StdlibFnVersionTimelineDataset {
        file_selector,
        dslx_file,
        dslx_fn_name,
        fraig,
        delay_model,
        points,
        crate_versions,
        matched_files,
        available_files,
        available_functions_for_file,
    };
    info!(
        "query build_stdlib_fn_version_timeline_dataset file_selector={} fn={} fraig={} delay_model={} provenances={} points={} matched_files={} elapsed_ms={}",
        dataset.file_selector,
        dataset.dslx_fn_name,
        dataset.fraig,
        dataset.delay_model,
        provenances.len(),
        dataset.points.len(),
        dataset.matched_files.len(),
        started.elapsed().as_millis(),
    );
    Ok(dataset)
}

pub(crate) fn clamp_ir_node_limit(
    requested: Option<u64>,
    min_ir_nodes: u64,
    max_ir_nodes: u64,
) -> u64 {
    if max_ir_nodes < min_ir_nodes {
        return min_ir_nodes;
    }
    // Treat stale `max_ir_nodes=0` query params as "unset" so old links do not
    // collapse the view after new data (with min_ir_nodes > 0) is materialized.
    if requested == Some(0) {
        return max_ir_nodes;
    }
    requested
        .unwrap_or(max_ir_nodes)
        .clamp(min_ir_nodes, max_ir_nodes)
}

pub(crate) fn build_stdlib_g8r_vs_yosys_dataset(
    store: &ArtifactStore,
    fraig: bool,
) -> Result<StdlibG8rVsYosysDataset> {
    let started = Instant::now();
    let provenances = store.list_provenances_shared()?;
    let provenance_by_action_id = build_provenance_lookup(provenances.as_ref());

    let mut g8r_by_ir: BTreeMap<(String, String), StdlibAigStatsPoint> = BTreeMap::new();
    let mut yosys_by_ir: BTreeMap<(String, String), StdlibAigStatsPoint> = BTreeMap::new();

    for provenance in provenances.iter() {
        let (aig_action_id, stats_dso_version) = match &provenance.action {
            ActionSpec::DriverAigToStats {
                aig_action_id,
                version,
                ..
            } => (
                aig_action_id.as_str(),
                normalize_tag_version(version).to_string(),
            ),
            _ => continue,
        };

        let stats_path = store.resolve_artifact_ref_path(&provenance.output_artifact);
        let stats_text = match fs::read_to_string(&stats_path) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let stats_json: serde_json::Value = match serde_json::from_str(&stats_text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(and_nodes) = parse_aig_stats_metric(&stats_json, "and_nodes")
            .or_else(|| parse_aig_stats_metric(&stats_json, "live_nodes"))
        else {
            continue;
        };
        let Some(depth) = parse_aig_stats_metric(&stats_json, "depth")
            .or_else(|| parse_aig_stats_metric(&stats_json, "deepest_path"))
        else {
            continue;
        };

        if let Some(source_ctx) = extract_stdlib_trend_source_context(
            &provenance_by_action_id,
            StdlibTrendKind::G8r,
            aig_action_id,
            fraig,
        ) {
            if let Some(point) = make_stdlib_aig_stats_point(
                &provenance_by_action_id,
                source_ctx,
                &provenance.action_id,
                provenance.created_utc,
                &stats_dso_version,
                and_nodes,
                depth,
            ) {
                let key = (point.ir_action_id.clone(), point.crate_version.clone());
                upsert_aig_stats_point_by_key(&mut g8r_by_ir, key, point);
            }
            continue;
        }

        if let Some(source_ctx) = extract_stdlib_trend_source_context(
            &provenance_by_action_id,
            StdlibTrendKind::YosysAbc,
            aig_action_id,
            false,
        ) && let Some(point) = make_stdlib_aig_stats_point(
            &provenance_by_action_id,
            source_ctx,
            &provenance.action_id,
            provenance.created_utc,
            &stats_dso_version,
            and_nodes,
            depth,
        ) {
            let key = (point.ir_action_id.clone(), point.crate_version.clone());
            upsert_aig_stats_point_by_key(&mut yosys_by_ir, key, point);
        }
    }

    let paired_ir_count = g8r_by_ir
        .keys()
        .filter(|key| yosys_by_ir.contains_key(*key))
        .count();
    let g8r_only_count = g8r_by_ir.len().saturating_sub(paired_ir_count);
    let yosys_only_count = yosys_by_ir.len().saturating_sub(paired_ir_count);

    let mut samples = Vec::new();
    let mut crate_versions = BTreeSet::new();
    let mut ir_node_count_cache: BTreeMap<(String, Option<String>), u64> = BTreeMap::new();
    for ((ir_action_id, crate_version), g8r) in &g8r_by_ir {
        let key = (ir_action_id.clone(), crate_version.clone());
        let Some(yosys) = yosys_by_ir.get(&key) else {
            continue;
        };
        let ir_node_count = resolve_ir_node_count_cached(
            store,
            ir_action_id,
            g8r.ir_top.as_deref(),
            &mut ir_node_count_cache,
        )
        .unwrap_or(0);
        let g8r_product = g8r.and_nodes * g8r.depth;
        let yosys_abc_product = yosys.and_nodes * yosys.depth;
        samples.push(StdlibG8rVsYosysSample {
            fn_key: g8r.fn_key.clone(),
            crate_version: g8r.crate_version.clone(),
            dso_version: if g8r.dso_version.is_empty() {
                yosys.dso_version.clone()
            } else {
                g8r.dso_version.clone()
            },
            ir_action_id: ir_action_id.clone(),
            ir_top: g8r.ir_top.clone().or_else(|| yosys.ir_top.clone()),
            structural_hash: None,
            ir_node_count,
            g8r_nodes: g8r.and_nodes,
            g8r_levels: g8r.depth,
            yosys_abc_nodes: yosys.and_nodes,
            yosys_abc_levels: yosys.depth,
            g8r_product,
            yosys_abc_product,
            g8r_product_loss: g8r_product - yosys_abc_product,
            g8r_stats_action_id: g8r.stats_action_id.clone(),
            yosys_abc_stats_action_id: yosys.stats_action_id.clone(),
        });
        crate_versions.insert(g8r.crate_version.clone());
    }

    samples.sort_by(|a, b| {
        cmp_dotted_numeric_version(&a.crate_version, &b.crate_version)
            .then(a.fn_key.cmp(&b.fn_key))
            .then(a.ir_action_id.cmp(&b.ir_action_id))
    });

    let min_ir_nodes = samples.iter().map(|s| s.ir_node_count).min().unwrap_or(0);
    let max_ir_nodes = samples.iter().map(|s| s.ir_node_count).max().unwrap_or(0);
    let mut available_crate_versions: Vec<String> = crate_versions.into_iter().collect();
    available_crate_versions.sort_by(|a, b| cmp_dotted_numeric_version(a, b));

    let dataset = StdlibG8rVsYosysDataset {
        fraig,
        samples,
        min_ir_nodes,
        max_ir_nodes,
        g8r_only_count,
        yosys_only_count,
        available_crate_versions,
    };
    info!(
        "query build_stdlib_g8r_vs_yosys_dataset fraig={} provenances={} g8r_candidates={} yosys_candidates={} paired_samples={} elapsed_ms={}",
        fraig,
        provenances.len(),
        g8r_by_ir.len(),
        yosys_by_ir.len(),
        dataset.samples.len(),
        started.elapsed().as_millis()
    );
    Ok(dataset)
}

pub(crate) fn filter_ir_fn_corpus_g8r_vs_yosys_samples(
    dataset: &StdlibG8rVsYosysDataset,
    crate_version: &str,
    yosys_levels_lt: f64,
    ir_node_count_lt: u64,
) -> Vec<StdlibG8rVsYosysSample> {
    let target_crate_version = normalize_tag_version(crate_version).to_string();
    dataset
        .samples
        .iter()
        .filter(|sample| {
            normalize_tag_version(&sample.crate_version) == target_crate_version
                && sample.yosys_abc_levels < yosys_levels_lt
                && sample.ir_node_count < ir_node_count_lt
        })
        .cloned()
        .collect()
}

fn sample_is_k3_cone(sample: &StdlibG8rVsYosysSample) -> bool {
    sample
        .ir_top
        .as_deref()
        .is_some_and(|ir_top| ir_top.starts_with("__k3_cone_"))
}

fn make_k3_loss_by_version_sample_ref(sample: &StdlibG8rVsYosysSample) -> K3LossByVersionSampleRef {
    K3LossByVersionSampleRef {
        crate_version: sample.crate_version.clone(),
        g8r_product_loss: sample.g8r_product_loss,
        g8r_nodes: sample.g8r_nodes,
        g8r_levels: sample.g8r_levels,
        yosys_abc_nodes: sample.yosys_abc_nodes,
        yosys_abc_levels: sample.yosys_abc_levels,
        g8r_stats_action_id: sample.g8r_stats_action_id.clone(),
        yosys_abc_stats_action_id: sample.yosys_abc_stats_action_id.clone(),
    }
}

#[derive(Debug, Default)]
struct K3LossByVersionRowBuilder {
    fn_key: String,
    ir_top: Option<String>,
    structural_hash: String,
    ir_node_count: u64,
    samples_by_version: BTreeMap<String, K3LossByVersionSampleRef>,
}

fn determine_k3_loss_change_status(
    current: Option<&K3LossByVersionSampleRef>,
    previous: Option<&K3LossByVersionSampleRef>,
) -> Option<(K3LossChangeStatus, Option<f64>)> {
    let current_loss = current.map(|sample| sample.g8r_product_loss);
    let previous_loss = previous.map(|sample| sample.g8r_product_loss);
    let delta_vs_previous = match (current_loss, previous_loss) {
        (Some(current_loss), Some(previous_loss)) => Some(current_loss - previous_loss),
        _ => None,
    };
    match (current_loss, previous_loss) {
        (Some(current_loss), Some(previous_loss)) if current_loss > 0.0 && previous_loss > 0.0 => {
            if (current_loss - previous_loss).abs() < 1e-9 {
                Some((K3LossChangeStatus::Same, delta_vs_previous))
            } else if current_loss < previous_loss {
                Some((K3LossChangeStatus::Improved, delta_vs_previous))
            } else {
                Some((K3LossChangeStatus::Regressed, delta_vs_previous))
            }
        }
        (Some(current_loss), Some(previous_loss)) if current_loss > 0.0 && previous_loss <= 0.0 => {
            Some((K3LossChangeStatus::NewlyLossy, delta_vs_previous))
        }
        (Some(current_loss), Some(previous_loss)) if current_loss <= 0.0 && previous_loss > 0.0 => {
            Some((K3LossChangeStatus::Resolved, delta_vs_previous))
        }
        (Some(current_loss), None) if current_loss > 0.0 => {
            Some((K3LossChangeStatus::NewlyLossy, None))
        }
        (None, Some(previous_loss)) if previous_loss > 0.0 => {
            Some((K3LossChangeStatus::MissingCurrent, None))
        }
        _ => None,
    }
}

pub(crate) fn build_ir_fn_corpus_k3_loss_by_version_dataset(
    dataset: &StdlibG8rVsYosysDataset,
    requested_crate_version: Option<&str>,
    requested_max_ir_nodes: Option<u64>,
    show_same: bool,
) -> K3LossByVersionDataset {
    let mut available_crate_versions = dataset
        .samples
        .iter()
        .filter(|sample| sample_is_k3_cone(sample))
        .map(|sample| sample.crate_version.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    available_crate_versions.sort_by(|a, b| cmp_dotted_numeric_version(a, b));

    let selected_crate_version = requested_crate_version
        .map(|version| normalize_tag_version(version).to_string())
        .filter(|version| {
            available_crate_versions
                .iter()
                .any(|available| available == version)
        })
        .or_else(|| available_crate_versions.last().cloned());

    let compared_crate_versions = if let Some(selected) = selected_crate_version.as_ref() {
        available_crate_versions
            .iter()
            .filter(|version| {
                cmp_dotted_numeric_version(version, selected) != std::cmp::Ordering::Greater
            })
            .cloned()
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let compared_versions_set = compared_crate_versions
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();

    let filtered_samples = dataset
        .samples
        .iter()
        .filter(|sample| {
            sample_is_k3_cone(sample) && compared_versions_set.contains(&sample.crate_version)
        })
        .collect::<Vec<_>>();

    let min_ir_nodes = filtered_samples
        .iter()
        .map(|sample| sample.ir_node_count)
        .min()
        .unwrap_or(0);
    let max_ir_nodes = filtered_samples
        .iter()
        .map(|sample| sample.ir_node_count)
        .max()
        .unwrap_or(0);
    let applied_max_ir_nodes = if filtered_samples.is_empty() {
        0
    } else {
        clamp_ir_node_limit(requested_max_ir_nodes, min_ir_nodes, max_ir_nodes)
    };

    let mut rows_by_structural_hash: BTreeMap<String, K3LossByVersionRowBuilder> = BTreeMap::new();
    for sample in filtered_samples
        .into_iter()
        .filter(|sample| sample.ir_node_count <= applied_max_ir_nodes)
    {
        let structural_hash = sample
            .structural_hash
            .clone()
            .unwrap_or_else(|| format!("{}:{}", sample.fn_key, sample.ir_action_id));
        let row = rows_by_structural_hash
            .entry(structural_hash.clone())
            .or_insert_with(|| K3LossByVersionRowBuilder {
                fn_key: sample.fn_key.clone(),
                ir_top: sample.ir_top.clone(),
                structural_hash: structural_hash.clone(),
                ir_node_count: sample.ir_node_count,
                samples_by_version: BTreeMap::new(),
            });
        if row.fn_key.is_empty() {
            row.fn_key = sample.fn_key.clone();
        }
        if row.ir_top.is_none() {
            row.ir_top = sample.ir_top.clone();
        }
        row.ir_node_count = row.ir_node_count.max(sample.ir_node_count);
        row.samples_by_version.insert(
            sample.crate_version.clone(),
            make_k3_loss_by_version_sample_ref(sample),
        );
    }

    let selected_index = selected_crate_version.as_ref().and_then(|selected| {
        compared_crate_versions
            .iter()
            .position(|version| version == selected)
    });
    let mut status_counts = K3LossByVersionStatusCounts::default();
    let mut rows = rows_by_structural_hash
        .into_values()
        .filter_map(|row| {
            let version_losses = compared_crate_versions
                .iter()
                .map(|version| {
                    row.samples_by_version
                        .get(version)
                        .map(|sample| sample.g8r_product_loss)
                })
                .collect::<Vec<_>>();
            let current_sample = selected_crate_version
                .as_ref()
                .and_then(|selected| row.samples_by_version.get(selected.as_str()).cloned());
            let previous_sample = selected_index.and_then(|selected_index| {
                compared_crate_versions[..selected_index]
                    .iter()
                    .rev()
                    .find_map(|version| row.samples_by_version.get(version).cloned())
            });
            let Some((status, delta_vs_previous)) =
                determine_k3_loss_change_status(current_sample.as_ref(), previous_sample.as_ref())
            else {
                return None;
            };
            status_counts.increment(status);
            if !show_same && status == K3LossChangeStatus::Same {
                return None;
            }
            Some(K3LossByVersionRow {
                fn_key: row.fn_key,
                ir_top: row.ir_top,
                structural_hash: row.structural_hash,
                ir_node_count: row.ir_node_count,
                status,
                version_losses,
                current_sample,
                previous_sample,
                delta_vs_previous,
            })
        })
        .collect::<Vec<_>>();

    rows.sort_by(|a, b| {
        let a_mag = a
            .current_sample
            .as_ref()
            .map(|sample| sample.g8r_product_loss.abs())
            .or_else(|| {
                a.previous_sample
                    .as_ref()
                    .map(|sample| sample.g8r_product_loss.abs())
            })
            .unwrap_or(0.0);
        let b_mag = b
            .current_sample
            .as_ref()
            .map(|sample| sample.g8r_product_loss.abs())
            .or_else(|| {
                b.previous_sample
                    .as_ref()
                    .map(|sample| sample.g8r_product_loss.abs())
            })
            .unwrap_or(0.0);
        a.status
            .cmp(&b.status)
            .then_with(|| {
                b_mag
                    .partial_cmp(&a_mag)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| a.fn_key.cmp(&b.fn_key))
            .then_with(|| a.structural_hash.cmp(&b.structural_hash))
    });

    K3LossByVersionDataset {
        available_crate_versions,
        compared_crate_versions,
        selected_crate_version,
        show_same,
        min_ir_nodes,
        max_ir_nodes,
        applied_max_ir_nodes,
        rows,
        status_counts,
    }
}

pub(crate) fn load_ir_fn_corpus_entity_maps(
    store: &ArtifactStore,
) -> Result<(
    BTreeMap<(String, String), String>,
    BTreeMap<String, String>,
    BTreeMap<String, String>,
    BTreeMap<String, u64>,
    BTreeMap<String, BTreeMap<String, u64>>,
    BTreeMap<String, u64>,
)> {
    let manifest = load_ir_fn_corpus_structural_manifest(store)?;
    let mut structural_hash_by_ir_action_and_top: BTreeMap<(String, String), String> =
        BTreeMap::new();
    let mut structural_hashes_by_ir_action: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut label_by_structural_hash: BTreeMap<String, String> = BTreeMap::new();
    let mut ir_node_count_by_structural_hash: BTreeMap<String, u64> = BTreeMap::new();
    let mut ir_node_count_by_source_ir_action_and_top: BTreeMap<String, BTreeMap<String, u64>> =
        BTreeMap::new();
    let mut ir_node_counts_by_source_ir_action: BTreeMap<String, BTreeSet<u64>> = BTreeMap::new();

    for group_meta in manifest.groups {
        let structural_hash = group_meta.structural_hash;
        if let Some(ir_node_count) = group_meta.ir_node_count {
            ir_node_count_by_structural_hash.insert(structural_hash.clone(), ir_node_count);
        }
        let group = load_ir_fn_corpus_structural_group(store, &structural_hash)?;
        let mut preferred_label: Option<String> = None;
        for member in group.members {
            structural_hash_by_ir_action_and_top.insert(
                (member.opt_ir_action_id.clone(), member.ir_top.clone()),
                structural_hash.clone(),
            );
            structural_hashes_by_ir_action
                .entry(member.opt_ir_action_id.clone())
                .or_default()
                .insert(structural_hash.clone());
            if let Some(ir_node_count) = member.ir_op_count {
                ir_node_count_by_source_ir_action_and_top
                    .entry(member.source_ir_action_id.clone())
                    .or_default()
                    .entry(member.ir_top.clone())
                    .or_insert(ir_node_count);
                ir_node_counts_by_source_ir_action
                    .entry(member.source_ir_action_id.clone())
                    .or_default()
                    .insert(ir_node_count);
            }
            if preferred_label.is_none()
                && let Some(sig) = member.ir_fn_signature.as_deref()
            {
                let trimmed = sig.trim();
                if !trimmed.is_empty() {
                    preferred_label = Some(trimmed.to_string());
                }
            }
            if preferred_label.is_none() {
                let trimmed = member.ir_top.trim();
                if !trimmed.is_empty() {
                    preferred_label = Some(trimmed.to_string());
                }
            }
        }
        let short_len = std::cmp::min(12, structural_hash.len());
        let short_hash = &structural_hash[..short_len];
        let label = match preferred_label {
            Some(label) => format!("{label} [{short_hash}]"),
            None => format!("hash:{short_hash}"),
        };
        label_by_structural_hash
            .entry(structural_hash.clone())
            .or_insert(label);
    }

    let mut unique_structural_hash_by_ir_action: BTreeMap<String, String> = BTreeMap::new();
    for (ir_action_id, hashes) in structural_hashes_by_ir_action {
        if hashes.len() == 1
            && let Some(structural_hash) = hashes.iter().next()
        {
            unique_structural_hash_by_ir_action.insert(ir_action_id, structural_hash.clone());
        }
    }
    let mut unique_ir_node_count_by_source_ir_action: BTreeMap<String, u64> = BTreeMap::new();
    for (ir_action_id, counts) in ir_node_counts_by_source_ir_action {
        if counts.len() == 1
            && let Some(ir_node_count) = counts.iter().next().copied()
        {
            unique_ir_node_count_by_source_ir_action.insert(ir_action_id, ir_node_count);
        }
    }

    Ok((
        structural_hash_by_ir_action_and_top,
        unique_structural_hash_by_ir_action,
        label_by_structural_hash,
        ir_node_count_by_structural_hash,
        ir_node_count_by_source_ir_action_and_top,
        unique_ir_node_count_by_source_ir_action,
    ))
}

pub(crate) fn resolve_ir_corpus_structural_hash_for_source(
    structural_hash_by_ir_action_and_top: &BTreeMap<(String, String), String>,
    unique_structural_hash_by_ir_action: &BTreeMap<String, String>,
    ir_action_id: &str,
    ir_top: Option<&str>,
) -> Option<String> {
    if let Some(ir_top) = ir_top
        && let Some(hash) = structural_hash_by_ir_action_and_top
            .get(&(ir_action_id.to_string(), ir_top.to_string()))
    {
        return Some(hash.clone());
    }
    unique_structural_hash_by_ir_action
        .get(ir_action_id)
        .cloned()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IrFnCorpusEntityPoint {
    structural_hash: String,
    crate_version: String,
    point: StdlibAigStatsPoint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IrFnCorpusIncrementalDeltaRow {
    source_kind: IrFnCorpusStatsSourceKind,
    row: IrFnCorpusEntityPoint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IrFnCorpusG8rVsYosysIndexFile {
    schema_version: u32,
    generated_utc: DateTime<Utc>,
    dataset: StdlibG8rVsYosysDataset,
    #[serde(default)]
    g8r_points: Vec<IrFnCorpusEntityPoint>,
    #[serde(default)]
    yosys_points: Vec<IrFnCorpusEntityPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VersionsSummaryIndexFile {
    schema_version: u32,
    generated_utc: DateTime<Utc>,
    report: VersionCardsReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StdlibFnsTrendIndexFile {
    schema_version: u32,
    generated_utc: DateTime<Utc>,
    dataset: StdlibFnsTrendDataset,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StdlibG8rVsYosysIndexFile {
    schema_version: u32,
    generated_utc: DateTime<Utc>,
    dataset: StdlibG8rVsYosysDataset,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
struct StdlibFileActionGraphTargetEdge {
    target: String,
    role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StdlibFileActionGraphIndexState {
    action_specs: BTreeMap<String, ActionSpec>,
    action_ids_with_provenance: BTreeSet<String>,
    crate_by_action_id: BTreeMap<String, String>,
    action_ids_by_crate: BTreeMap<String, Vec<String>>,
    files_by_crate_version: BTreeMap<String, Vec<String>>,
    functions_by_crate_file: BTreeMap<String, BTreeMap<String, Vec<String>>>,
    roots_by_crate_file_fn: BTreeMap<String, BTreeMap<String, BTreeMap<String, Vec<String>>>>,
    dependency_edges_by_source: BTreeMap<String, Vec<StdlibFileActionGraphTargetEdge>>,
    suggested_edges_by_source: BTreeMap<String, Vec<StdlibFileActionGraphTargetEdge>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StdlibFileActionGraphIndexFile {
    schema_version: u32,
    generated_utc: DateTime<Utc>,
    state: StdlibFileActionGraphIndexState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StdlibFnTimelineIndexEntry {
    dslx_file: String,
    dslx_fn_name: String,
    g8r_fraig_false_by_crate: BTreeMap<String, StdlibFnTrendPoint>,
    g8r_fraig_true_by_crate: BTreeMap<String, StdlibFnTrendPoint>,
    yosys_by_crate: BTreeMap<String, StdlibFnTrendPoint>,
    delay_by_model_and_crate: BTreeMap<String, BTreeMap<String, StdlibFnDelayPoint>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StdlibFnTimelineIndexFile {
    schema_version: u32,
    generated_utc: DateTime<Utc>,
    available_files: Vec<String>,
    functions_by_file: BTreeMap<String, Vec<String>>,
    entries_by_fn_key: BTreeMap<String, StdlibFnTimelineIndexEntry>,
}

#[derive(Debug, Clone)]
struct IrFnCorpusG8rVsYosysIndexState {
    dataset: StdlibG8rVsYosysDataset,
    g8r_by_entity: BTreeMap<(String, String), StdlibAigStatsPoint>,
    yosys_by_entity: BTreeMap<(String, String), StdlibAigStatsPoint>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IrFnCorpusStatsSourceKind {
    G8r,
    YosysAbc,
}

#[derive(Debug, Clone)]
struct IrFnCorpusStatsUpsertPoint {
    source_kind: IrFnCorpusStatsSourceKind,
    structural_hash: String,
    crate_version: String,
    point: StdlibAigStatsPoint,
}

fn ir_fn_corpus_incremental_delta_prefix(index_key: &str) -> String {
    format!("{index_key}/incremental-delta/")
}

fn ir_fn_corpus_incremental_delta_key(
    index_key: &str,
    source_kind: IrFnCorpusStatsSourceKind,
    structural_hash: &str,
    crate_version: &str,
) -> String {
    let source = match source_kind {
        IrFnCorpusStatsSourceKind::G8r => "g8r",
        IrFnCorpusStatsSourceKind::YosysAbc => "yosys_abc",
    };
    let crate_component = crate_version.replace('/', "_");
    format!(
        "{}/{}:{}:{}",
        ir_fn_corpus_incremental_delta_prefix(index_key),
        source,
        crate_component,
        structural_hash
    )
}

fn normalize_structural_hash_hex(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.len() != 64 {
        return None;
    }
    if !trimmed.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(trimmed.to_ascii_lowercase())
}

fn structural_hash_from_details(details: &serde_json::Value) -> Option<String> {
    details
        .get(INPUT_IR_FN_STRUCTURAL_HASH_DETAILS_KEY)
        .and_then(|v| v.as_str())
        .and_then(normalize_structural_hash_hex)
}

fn short_structural_hash(hash: &str) -> &str {
    hash.get(..12).unwrap_or(hash)
}

fn entity_map_to_points(
    map: &BTreeMap<(String, String), StdlibAigStatsPoint>,
) -> Vec<IrFnCorpusEntityPoint> {
    map.iter()
        .map(
            |((structural_hash, crate_version), point)| IrFnCorpusEntityPoint {
                structural_hash: structural_hash.clone(),
                crate_version: crate_version.clone(),
                point: point.clone(),
            },
        )
        .collect()
}

fn entity_points_to_map(
    points: &[IrFnCorpusEntityPoint],
) -> BTreeMap<(String, String), StdlibAigStatsPoint> {
    let mut map = BTreeMap::new();
    for row in points {
        let key = (row.structural_hash.clone(), row.crate_version.clone());
        upsert_aig_stats_point_by_key(&mut map, key, row.point.clone());
    }
    map
}

fn paired_entity_maps_from_dataset(
    dataset: &StdlibG8rVsYosysDataset,
) -> (
    BTreeMap<(String, String), StdlibAigStatsPoint>,
    BTreeMap<(String, String), StdlibAigStatsPoint>,
) {
    let mut g8r_by_entity = BTreeMap::new();
    let mut yosys_by_entity = BTreeMap::new();
    for sample in &dataset.samples {
        let Some(structural_hash) = sample
            .structural_hash
            .as_deref()
            .and_then(normalize_structural_hash_hex)
        else {
            continue;
        };
        let key = (structural_hash, sample.crate_version.clone());
        let g8r = StdlibAigStatsPoint {
            fn_key: sample.fn_key.clone(),
            ir_action_id: sample.ir_action_id.clone(),
            ir_top: sample.ir_top.clone(),
            crate_version: sample.crate_version.clone(),
            dso_version: sample.dso_version.clone(),
            and_nodes: sample.g8r_nodes,
            depth: sample.g8r_levels,
            created_utc: DateTime::<Utc>::UNIX_EPOCH,
            stats_action_id: sample.g8r_stats_action_id.clone(),
        };
        let yosys = StdlibAigStatsPoint {
            fn_key: sample.fn_key.clone(),
            ir_action_id: sample.ir_action_id.clone(),
            ir_top: sample.ir_top.clone(),
            crate_version: sample.crate_version.clone(),
            dso_version: sample.dso_version.clone(),
            and_nodes: sample.yosys_abc_nodes,
            depth: sample.yosys_abc_levels,
            created_utc: DateTime::<Utc>::UNIX_EPOCH,
            stats_action_id: sample.yosys_abc_stats_action_id.clone(),
        };
        upsert_aig_stats_point_by_key(&mut g8r_by_entity, key.clone(), g8r);
        upsert_aig_stats_point_by_key(&mut yosys_by_entity, key, yosys);
    }
    (g8r_by_entity, yosys_by_entity)
}

pub(crate) fn seed_ir_node_count_cache_from_dataset(
    dataset: &StdlibG8rVsYosysDataset,
) -> BTreeMap<(String, Option<String>), u64> {
    let mut cache = BTreeMap::new();
    for sample in &dataset.samples {
        if sample.ir_node_count == 0 {
            continue;
        }
        cache
            .entry((sample.ir_action_id.clone(), sample.ir_top.clone()))
            .or_insert(sample.ir_node_count);
    }
    cache
}

fn resolve_ir_node_count_from_source_maps(
    ir_node_count_by_source_ir_action_and_top: Option<&BTreeMap<String, BTreeMap<String, u64>>>,
    unique_ir_node_count_by_source_ir_action: Option<&BTreeMap<String, u64>>,
    ir_action_id: &str,
    ir_top: Option<&str>,
) -> Option<u64> {
    if let Some(ir_top) = ir_top
        && let Some(ir_node_count) = ir_node_count_by_source_ir_action_and_top
            .and_then(|by_source| by_source.get(ir_action_id))
            .and_then(|by_top| by_top.get(ir_top))
            .copied()
    {
        return Some(ir_node_count);
    }
    unique_ir_node_count_by_source_ir_action.and_then(|counts| counts.get(ir_action_id).copied())
}

fn build_ir_fn_corpus_dataset_from_entity_maps(
    store: &ArtifactStore,
    g8r_by_entity: &BTreeMap<(String, String), StdlibAigStatsPoint>,
    yosys_by_entity: &BTreeMap<(String, String), StdlibAigStatsPoint>,
    ir_node_count_by_structural_hash: Option<&BTreeMap<String, u64>>,
    ir_node_count_by_source_ir_action_and_top: Option<&BTreeMap<String, BTreeMap<String, u64>>>,
    unique_ir_node_count_by_source_ir_action: Option<&BTreeMap<String, u64>>,
    seed_ir_node_count_cache: Option<BTreeMap<(String, Option<String>), u64>>,
) -> StdlibG8rVsYosysDataset {
    let paired_entity_count = g8r_by_entity
        .keys()
        .filter(|key| yosys_by_entity.contains_key(*key))
        .count();
    let g8r_only_count = g8r_by_entity.len().saturating_sub(paired_entity_count);
    let yosys_only_count = yosys_by_entity.len().saturating_sub(paired_entity_count);

    let mut samples = Vec::new();
    let mut crate_versions = BTreeSet::new();
    let mut ir_node_count_cache = seed_ir_node_count_cache.unwrap_or_default();
    for (entity_key, g8r) in g8r_by_entity {
        let Some(yosys) = yosys_by_entity.get(entity_key) else {
            continue;
        };
        let ir_node_count = ir_node_count_by_structural_hash
            .and_then(|counts| counts.get(&entity_key.0).copied())
            .or_else(|| {
                resolve_ir_node_count_from_source_maps(
                    ir_node_count_by_source_ir_action_and_top,
                    unique_ir_node_count_by_source_ir_action,
                    &g8r.ir_action_id,
                    g8r.ir_top.as_deref(),
                )
            })
            .or_else(|| {
                resolve_ir_node_count_from_source_maps(
                    ir_node_count_by_source_ir_action_and_top,
                    unique_ir_node_count_by_source_ir_action,
                    &yosys.ir_action_id,
                    yosys.ir_top.as_deref(),
                )
            })
            .or_else(|| {
                resolve_ir_node_count_cached(
                    store,
                    &g8r.ir_action_id,
                    g8r.ir_top.as_deref(),
                    &mut ir_node_count_cache,
                )
            })
            .or_else(|| {
                resolve_ir_node_count_cached(
                    store,
                    &yosys.ir_action_id,
                    yosys.ir_top.as_deref(),
                    &mut ir_node_count_cache,
                )
            })
            .unwrap_or(0);
        let g8r_product = g8r.and_nodes * g8r.depth;
        let yosys_abc_product = yosys.and_nodes * yosys.depth;
        samples.push(StdlibG8rVsYosysSample {
            fn_key: g8r.fn_key.clone(),
            crate_version: g8r.crate_version.clone(),
            dso_version: if g8r.dso_version.is_empty() {
                yosys.dso_version.clone()
            } else {
                g8r.dso_version.clone()
            },
            ir_action_id: g8r.ir_action_id.clone(),
            ir_top: g8r.ir_top.clone().or_else(|| yosys.ir_top.clone()),
            structural_hash: Some(entity_key.0.clone()),
            ir_node_count,
            g8r_nodes: g8r.and_nodes,
            g8r_levels: g8r.depth,
            yosys_abc_nodes: yosys.and_nodes,
            yosys_abc_levels: yosys.depth,
            g8r_product,
            yosys_abc_product,
            g8r_product_loss: g8r_product - yosys_abc_product,
            g8r_stats_action_id: g8r.stats_action_id.clone(),
            yosys_abc_stats_action_id: yosys.stats_action_id.clone(),
        });
        crate_versions.insert(g8r.crate_version.clone());
    }

    samples.sort_by(|a, b| {
        cmp_dotted_numeric_version(&a.crate_version, &b.crate_version)
            .then(a.fn_key.cmp(&b.fn_key))
            .then(a.ir_action_id.cmp(&b.ir_action_id))
    });

    let min_ir_nodes = samples.iter().map(|s| s.ir_node_count).min().unwrap_or(0);
    let max_ir_nodes = samples.iter().map(|s| s.ir_node_count).max().unwrap_or(0);
    let mut available_crate_versions: Vec<String> = crate_versions.into_iter().collect();
    available_crate_versions.sort_by(|a, b| cmp_dotted_numeric_version(a, b));

    StdlibG8rVsYosysDataset {
        fraig: false,
        samples,
        min_ir_nodes,
        max_ir_nodes,
        g8r_only_count,
        yosys_only_count,
        available_crate_versions,
    }
}

fn load_ir_fn_corpus_incremental_delta_rows(
    store: &ArtifactStore,
    index_key: &str,
) -> Result<Vec<IrFnCorpusIncrementalDeltaRow>> {
    let prefix = ir_fn_corpus_incremental_delta_prefix(index_key);
    let rows = store.list_web_index_entries_with_prefix(&prefix)?;
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for (key, bytes) in rows {
        match serde_json::from_slice::<IrFnCorpusIncrementalDeltaRow>(&bytes) {
            Ok(row) => out.push(row),
            Err(err) => warn!(
                "query skipping malformed ir-fn-corpus incremental delta row key={} error={:#}",
                key, err
            ),
        }
    }
    Ok(out)
}

fn build_ir_fn_corpus_state_from_incremental_deltas(
    store: &ArtifactStore,
    deltas: &[IrFnCorpusIncrementalDeltaRow],
) -> Option<IrFnCorpusG8rVsYosysIndexState> {
    if deltas.is_empty() {
        return None;
    }
    let mut g8r_by_entity: BTreeMap<(String, String), StdlibAigStatsPoint> = BTreeMap::new();
    let mut yosys_by_entity: BTreeMap<(String, String), StdlibAigStatsPoint> = BTreeMap::new();
    for delta in deltas {
        let key = (
            delta.row.structural_hash.clone(),
            delta.row.crate_version.clone(),
        );
        match delta.source_kind {
            IrFnCorpusStatsSourceKind::G8r => {
                upsert_aig_stats_point_by_key(&mut g8r_by_entity, key, delta.row.point.clone());
            }
            IrFnCorpusStatsSourceKind::YosysAbc => {
                upsert_aig_stats_point_by_key(&mut yosys_by_entity, key, delta.row.point.clone());
            }
        }
    }
    let dataset = build_ir_fn_corpus_dataset_from_entity_maps(
        store,
        &g8r_by_entity,
        &yosys_by_entity,
        None,
        None,
        None,
        None,
    );
    Some(IrFnCorpusG8rVsYosysIndexState {
        dataset,
        g8r_by_entity,
        yosys_by_entity,
    })
}

fn write_ir_fn_corpus_incremental_delta_row(
    store: &ArtifactStore,
    index_key: &str,
    point: &IrFnCorpusStatsUpsertPoint,
) -> Result<()> {
    let index_delta_key = ir_fn_corpus_incremental_delta_key(
        index_key,
        point.source_kind,
        &point.structural_hash,
        &point.crate_version,
    );
    let row = IrFnCorpusIncrementalDeltaRow {
        source_kind: point.source_kind,
        row: IrFnCorpusEntityPoint {
            structural_hash: point.structural_hash.clone(),
            crate_version: point.crate_version.clone(),
            point: point.point.clone(),
        },
    };
    let bytes =
        serde_json::to_vec(&row).context("serializing ir-fn-corpus incremental delta row")?;
    store
        .write_web_index_bytes(&index_delta_key, &bytes)
        .with_context(|| {
            format!(
                "writing ir-fn-corpus incremental delta row: {}",
                store.web_index_location(&index_delta_key)
            )
        })?;
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct IrFnCorpusG8rVsYosysIndexSummary {
    pub(crate) generated_utc: DateTime<Utc>,
    pub(crate) index_path: String,
    pub(crate) sample_count: usize,
    pub(crate) crate_versions: usize,
    pub(crate) index_bytes: u64,
    pub(crate) elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VersionsSummaryIndexSummary {
    pub(crate) generated_utc: DateTime<Utc>,
    pub(crate) index_path: String,
    pub(crate) card_count: usize,
    pub(crate) unattributed_actions: usize,
    pub(crate) index_bytes: u64,
    pub(crate) elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct StdlibFnsTrendIndexSummary {
    pub(crate) kind_path: String,
    pub(crate) fraig: bool,
    pub(crate) generated_utc: DateTime<Utc>,
    pub(crate) index_path: String,
    pub(crate) series_count: usize,
    pub(crate) point_count: usize,
    pub(crate) file_count: usize,
    pub(crate) index_bytes: u64,
    pub(crate) elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct StdlibFnTimelineIndexSummary {
    pub(crate) generated_utc: DateTime<Utc>,
    pub(crate) index_path: String,
    pub(crate) file_count: usize,
    pub(crate) fn_count: usize,
    pub(crate) index_bytes: u64,
    pub(crate) elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct StdlibG8rVsYosysIndexSummary {
    pub(crate) fraig: bool,
    pub(crate) generated_utc: DateTime<Utc>,
    pub(crate) index_path: String,
    pub(crate) sample_count: usize,
    pub(crate) crate_versions: usize,
    pub(crate) index_bytes: u64,
    pub(crate) elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct StdlibFileActionGraphIndexSummary {
    pub(crate) generated_utc: DateTime<Utc>,
    pub(crate) index_path: String,
    pub(crate) action_count: usize,
    pub(crate) crate_versions: usize,
    pub(crate) dependency_edges: usize,
    pub(crate) suggested_edges: usize,
    pub(crate) index_bytes: u64,
    pub(crate) elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WebIndicesRebuildSummary {
    pub(crate) generated_utc: DateTime<Utc>,
    pub(crate) versions_summary: VersionsSummaryIndexSummary,
    pub(crate) stdlib_file_action_graph: StdlibFileActionGraphIndexSummary,
    pub(crate) stdlib_fns_trend: Vec<StdlibFnsTrendIndexSummary>,
    pub(crate) stdlib_fn_timeline: StdlibFnTimelineIndexSummary,
    pub(crate) stdlib_g8r_vs_yosys: Vec<StdlibG8rVsYosysIndexSummary>,
    pub(crate) ir_fn_corpus_g8r_vs_yosys: IrFnCorpusG8rVsYosysIndexSummary,
    pub(crate) ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc: IrFnCorpusG8rVsYosysIndexSummary,
}

fn load_ir_fn_corpus_index_state(
    store: &ArtifactStore,
    index_key: &str,
    expected_schema_version: u32,
) -> Result<Option<IrFnCorpusG8rVsYosysIndexState>> {
    let location = store.web_index_location(index_key);
    let mut state = if let Some(bytes) = store
        .load_web_index_bytes(index_key)
        .with_context(|| format!("reading ir-fn-corpus web index: {}", location))?
    {
        let index_file: IrFnCorpusG8rVsYosysIndexFile = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing ir-fn-corpus web index: {}", location))?;
        if index_file.schema_version != expected_schema_version {
            info!(
                "query ir-fn-corpus web index schema mismatch location={} expected={} got={}",
                location, expected_schema_version, index_file.schema_version
            );
            None
        } else {
            let mut g8r_by_entity = entity_points_to_map(&index_file.g8r_points);
            let mut yosys_by_entity = entity_points_to_map(&index_file.yosys_points);
            if g8r_by_entity.is_empty() && yosys_by_entity.is_empty() {
                let (g8r_fallback, yosys_fallback) =
                    paired_entity_maps_from_dataset(&index_file.dataset);
                g8r_by_entity = g8r_fallback;
                yosys_by_entity = yosys_fallback;
            }
            info!(
                "query ir-fn-corpus web index hit location={} generated_utc={} samples={} g8r_points={} yosys_points={}",
                location,
                index_file.generated_utc,
                index_file.dataset.samples.len(),
                g8r_by_entity.len(),
                yosys_by_entity.len()
            );
            Some(IrFnCorpusG8rVsYosysIndexState {
                dataset: index_file.dataset,
                g8r_by_entity,
                yosys_by_entity,
            })
        }
    } else {
        None
    };

    let deltas = load_ir_fn_corpus_incremental_delta_rows(store, index_key)?;
    if deltas.is_empty() {
        return Ok(state);
    }

    if state.is_none() {
        return Ok(build_ir_fn_corpus_state_from_incremental_deltas(
            store, &deltas,
        ));
    }

    if let Some(state_ref) = state.as_mut() {
        for delta in deltas {
            let key = (
                delta.row.structural_hash.clone(),
                delta.row.crate_version.clone(),
            );
            match delta.source_kind {
                IrFnCorpusStatsSourceKind::G8r => upsert_aig_stats_point_by_key(
                    &mut state_ref.g8r_by_entity,
                    key,
                    delta.row.point.clone(),
                ),
                IrFnCorpusStatsSourceKind::YosysAbc => upsert_aig_stats_point_by_key(
                    &mut state_ref.yosys_by_entity,
                    key,
                    delta.row.point.clone(),
                ),
            };
        }
        let seed_ir_node_count_cache = seed_ir_node_count_cache_from_dataset(&state_ref.dataset);
        state_ref.dataset = build_ir_fn_corpus_dataset_from_entity_maps(
            store,
            &state_ref.g8r_by_entity,
            &state_ref.yosys_by_entity,
            None,
            None,
            None,
            Some(seed_ir_node_count_cache),
        );
    }
    Ok(state)
}

fn load_ir_fn_corpus_g8r_vs_yosys_index_state(
    store: &ArtifactStore,
) -> Result<Option<IrFnCorpusG8rVsYosysIndexState>> {
    load_ir_fn_corpus_index_state(
        store,
        WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME,
        WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION,
    )
}

fn load_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_index_state(
    store: &ArtifactStore,
) -> Result<Option<IrFnCorpusG8rVsYosysIndexState>> {
    load_ir_fn_corpus_index_state(
        store,
        WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME,
        WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_SCHEMA_VERSION,
    )
}

fn write_ir_fn_corpus_index_state(
    store: &ArtifactStore,
    index_key: &str,
    schema_version: u32,
    dataset: &StdlibG8rVsYosysDataset,
    g8r_by_entity: &BTreeMap<(String, String), StdlibAigStatsPoint>,
    yosys_by_entity: &BTreeMap<(String, String), StdlibAigStatsPoint>,
) -> Result<(String, u64, DateTime<Utc>)> {
    let location = store.web_index_location(index_key);
    let (bytes, generated_utc) = serialize_ir_fn_corpus_index_state(
        schema_version,
        dataset,
        g8r_by_entity,
        yosys_by_entity,
    )?;
    store
        .write_web_index_bytes(index_key, &bytes)
        .with_context(|| format!("writing ir-fn-corpus web index: {}", location))?;
    let cleared_delta_rows = store
        .delete_web_index_keys_with_prefix(&ir_fn_corpus_incremental_delta_prefix(index_key))
        .with_context(|| {
            format!(
                "clearing ir-fn-corpus incremental delta rows for {}",
                location
            )
        })?;
    info!(
        "query ir-fn-corpus web index write location={} bytes={} samples={} g8r_points={} yosys_points={} cleared_delta_rows={}",
        location,
        bytes.len(),
        dataset.samples.len(),
        g8r_by_entity.len(),
        yosys_by_entity.len(),
        cleared_delta_rows,
    );
    Ok((location, bytes.len() as u64, generated_utc))
}

fn serialize_ir_fn_corpus_index_state(
    schema_version: u32,
    dataset: &StdlibG8rVsYosysDataset,
    g8r_by_entity: &BTreeMap<(String, String), StdlibAigStatsPoint>,
    yosys_by_entity: &BTreeMap<(String, String), StdlibAigStatsPoint>,
) -> Result<(Vec<u8>, DateTime<Utc>)> {
    let generated_utc = Utc::now();
    let payload = IrFnCorpusG8rVsYosysIndexFile {
        schema_version,
        generated_utc,
        dataset: dataset.clone(),
        g8r_points: entity_map_to_points(g8r_by_entity),
        yosys_points: entity_map_to_points(yosys_by_entity),
    };
    let bytes = serde_json::to_vec(&payload).context("serializing ir-fn-corpus web index")?;
    Ok((bytes, generated_utc))
}

fn write_ir_fn_corpus_g8r_vs_yosys_index_state(
    store: &ArtifactStore,
    dataset: &StdlibG8rVsYosysDataset,
    g8r_by_entity: &BTreeMap<(String, String), StdlibAigStatsPoint>,
    yosys_by_entity: &BTreeMap<(String, String), StdlibAigStatsPoint>,
) -> Result<(String, u64, DateTime<Utc>)> {
    write_ir_fn_corpus_index_state(
        store,
        WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME,
        WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION,
        dataset,
        g8r_by_entity,
        yosys_by_entity,
    )
}

fn write_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_index_state(
    store: &ArtifactStore,
    dataset: &StdlibG8rVsYosysDataset,
    g8r_by_entity: &BTreeMap<(String, String), StdlibAigStatsPoint>,
    yosys_by_entity: &BTreeMap<(String, String), StdlibAigStatsPoint>,
) -> Result<(String, u64, DateTime<Utc>)> {
    write_ir_fn_corpus_index_state(
        store,
        WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME,
        WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_SCHEMA_VERSION,
        dataset,
        g8r_by_entity,
        yosys_by_entity,
    )
}

pub(crate) fn load_ir_fn_corpus_g8r_vs_yosys_dataset_index(
    store: &ArtifactStore,
) -> Result<Option<StdlibG8rVsYosysDataset>> {
    Ok(load_ir_fn_corpus_g8r_vs_yosys_index_state(store)?.map(|state| state.dataset))
}

pub(crate) fn load_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index(
    store: &ArtifactStore,
) -> Result<Option<StdlibG8rVsYosysDataset>> {
    Ok(
        load_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_index_state(store)?
            .map(|state| state.dataset),
    )
}

pub(crate) fn rebuild_ir_fn_corpus_g8r_vs_yosys_dataset_index(
    store: &ArtifactStore,
    repo_root: &Path,
) -> Result<IrFnCorpusG8rVsYosysIndexSummary> {
    let started = Instant::now();
    let state = build_ir_fn_corpus_g8r_vs_yosys_build_state(store, repo_root)?;
    let (index_location, index_bytes, generated_utc) = write_ir_fn_corpus_g8r_vs_yosys_index_state(
        store,
        &state.dataset,
        &state.g8r_by_entity,
        &state.yosys_by_entity,
    )?;
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok(IrFnCorpusG8rVsYosysIndexSummary {
        generated_utc,
        index_path: index_location,
        sample_count: state.dataset.samples.len(),
        crate_versions: state.dataset.available_crate_versions.len(),
        index_bytes,
        elapsed_ms,
    })
}

pub(crate) fn build_ir_fn_corpus_g8r_vs_yosys_dataset_index_bytes(
    store: &ArtifactStore,
    repo_root: &Path,
) -> Result<(
    IrFnCorpusG8rVsYosysIndexSummary,
    Vec<u8>,
    BTreeMap<(String, Option<String>), u64>,
)> {
    let started = Instant::now();
    let state = build_ir_fn_corpus_g8r_vs_yosys_build_state(store, repo_root)?;
    let seed_ir_node_count_cache = seed_ir_node_count_cache_from_dataset(&state.dataset);
    let (bytes, generated_utc) = serialize_ir_fn_corpus_index_state(
        WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION,
        &state.dataset,
        &state.g8r_by_entity,
        &state.yosys_by_entity,
    )?;
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok((
        IrFnCorpusG8rVsYosysIndexSummary {
            generated_utc,
            index_path: WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME.to_string(),
            sample_count: state.dataset.samples.len(),
            crate_versions: state.dataset.available_crate_versions.len(),
            index_bytes: bytes.len() as u64,
            elapsed_ms,
        },
        bytes,
        seed_ir_node_count_cache,
    ))
}

const WEB_INDEX_REBUILD_PROGRESS_EVERY_ACTIONS: usize = 10_000;
const WEB_INDEX_REBUILD_PROGRESS_MIN_INTERVAL_SECS: u64 = 5;

fn format_progress_duration(seconds: f64) -> String {
    if !seconds.is_finite() {
        return "unknown".to_string();
    }
    let total_secs = seconds.max(0.0).round() as u64;
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    if hours > 0 {
        return format!("{hours}h{minutes:02}m{secs:02}s");
    }
    if minutes > 0 {
        return format!("{minutes}m{secs:02}s");
    }
    format!("{secs}s")
}

fn maybe_log_web_index_rebuild_progress(
    phase: &str,
    scanned: usize,
    total: usize,
    stats_actions_seen: usize,
    lhs_candidates: usize,
    rhs_candidates: usize,
    started: Instant,
    last_progress_log_at: &mut Instant,
    force: bool,
) {
    if total == 0 {
        return;
    }
    let done = scanned >= total;
    let count_boundary = scanned.is_multiple_of(WEB_INDEX_REBUILD_PROGRESS_EVERY_ACTIONS);
    let time_boundary = last_progress_log_at.elapsed()
        >= Duration::from_secs(WEB_INDEX_REBUILD_PROGRESS_MIN_INTERVAL_SECS);
    if !force && !done && !(count_boundary && time_boundary) {
        return;
    }
    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    let scan_rate_per_sec = scanned as f64 / elapsed;
    let remaining = total.saturating_sub(scanned);
    let eta_secs = if done || scan_rate_per_sec <= 0.0 {
        0.0
    } else {
        remaining as f64 / scan_rate_per_sec
    };
    let pct = (scanned as f64 / total as f64) * 100.0;
    warn!(
        "query progress phase={} scanned={}/{} ({:.1}%) stats_actions_seen={} lhs_candidates={} rhs_candidates={} elapsed={} scan_rate_per_sec={:.1} eta={}",
        phase,
        scanned,
        total,
        pct,
        stats_actions_seen,
        lhs_candidates,
        rhs_candidates,
        format_progress_duration(elapsed),
        scan_rate_per_sec,
        format_progress_duration(eta_secs),
    );
    *last_progress_log_at = Instant::now();
}

pub(crate) fn rebuild_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index(
    store: &ArtifactStore,
    repo_root: &Path,
) -> Result<IrFnCorpusG8rVsYosysIndexSummary> {
    let started = Instant::now();
    let state = build_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_build_state_with_seed(
        store, repo_root, None,
    )?;
    let (index_location, index_bytes, generated_utc) =
        write_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_index_state(
            store,
            &state.dataset,
            &state.g8r_by_entity,
            &state.yosys_by_entity,
        )?;
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok(IrFnCorpusG8rVsYosysIndexSummary {
        generated_utc,
        index_path: index_location,
        sample_count: state.dataset.samples.len(),
        crate_versions: state.dataset.available_crate_versions.len(),
        index_bytes,
        elapsed_ms,
    })
}

pub(crate) fn build_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index_bytes(
    store: &ArtifactStore,
    repo_root: &Path,
    seed_ir_node_count_cache: Option<BTreeMap<(String, Option<String>), u64>>,
) -> Result<(IrFnCorpusG8rVsYosysIndexSummary, Vec<u8>)> {
    let started = Instant::now();
    let state = build_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_build_state_with_seed(
        store,
        repo_root,
        seed_ir_node_count_cache,
    )?;
    let (bytes, generated_utc) = serialize_ir_fn_corpus_index_state(
        WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_SCHEMA_VERSION,
        &state.dataset,
        &state.g8r_by_entity,
        &state.yosys_by_entity,
    )?;
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok((
        IrFnCorpusG8rVsYosysIndexSummary {
            generated_utc,
            index_path: WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME.to_string(),
            sample_count: state.dataset.samples.len(),
            crate_versions: state.dataset.available_crate_versions.len(),
            index_bytes: bytes.len() as u64,
            elapsed_ms,
        },
        bytes,
    ))
}

pub(crate) fn rebuild_web_indices(
    store: &ArtifactStore,
    repo_root: &Path,
) -> Result<WebIndicesRebuildSummary> {
    let started = Instant::now();
    warn!("rebuild-web-indices start");
    let generated_utc = Utc::now();
    let phase_versions_started = Instant::now();
    warn!("rebuild-web-indices phase=versions-summary begin");
    let versions_summary = rebuild_versions_cards_index(store, repo_root)?;
    warn!(
        "rebuild-web-indices phase=versions-summary done cards={} unattributed={} elapsed={}",
        versions_summary.card_count,
        versions_summary.unattributed_actions,
        format_progress_duration(phase_versions_started.elapsed().as_secs_f64())
    );
    let phase_file_graph_started = Instant::now();
    warn!("rebuild-web-indices phase=stdlib-file-action-graph begin");
    let stdlib_file_action_graph =
        rebuild_stdlib_file_action_graph_dataset_index(store, repo_root)?;
    warn!(
        "rebuild-web-indices phase=stdlib-file-action-graph done actions={} crate_versions={} dependency_edges={} suggested_edges={} elapsed={}",
        stdlib_file_action_graph.action_count,
        stdlib_file_action_graph.crate_versions,
        stdlib_file_action_graph.dependency_edges,
        stdlib_file_action_graph.suggested_edges,
        format_progress_duration(phase_file_graph_started.elapsed().as_secs_f64())
    );
    let mut stdlib_fns_trend = Vec::new();
    for (kind, fraig) in [
        (StdlibTrendKind::G8r, false),
        (StdlibTrendKind::YosysAbc, false),
    ] {
        let phase_started = Instant::now();
        warn!(
            "rebuild-web-indices phase=stdlib-fns-trend begin kind={} fraig={}",
            kind.view_path(),
            fraig
        );
        let summary = rebuild_stdlib_fns_trend_dataset_index(store, kind, fraig)?;
        warn!(
            "rebuild-web-indices phase=stdlib-fns-trend done kind={} fraig={} series={} points={} elapsed={}",
            summary.kind_path,
            summary.fraig,
            summary.series_count,
            summary.point_count,
            format_progress_duration(phase_started.elapsed().as_secs_f64())
        );
        stdlib_fns_trend.push(summary);
    }
    let phase_timeline_started = Instant::now();
    warn!("rebuild-web-indices phase=stdlib-fn-timeline begin");
    let stdlib_fn_timeline = rebuild_stdlib_fn_version_timeline_dataset_index(store)?;
    warn!(
        "rebuild-web-indices phase=stdlib-fn-timeline done files={} functions={} elapsed={}",
        stdlib_fn_timeline.file_count,
        stdlib_fn_timeline.fn_count,
        format_progress_duration(phase_timeline_started.elapsed().as_secs_f64())
    );
    let mut stdlib_g8r_vs_yosys = Vec::new();
    for fraig in [false, true] {
        let phase_started = Instant::now();
        warn!(
            "rebuild-web-indices phase=stdlib-g8r-vs-yosys begin fraig={}",
            fraig
        );
        let summary = rebuild_stdlib_g8r_vs_yosys_dataset_index(store, fraig)?;
        warn!(
            "rebuild-web-indices phase=stdlib-g8r-vs-yosys done fraig={} samples={} versions={} elapsed={}",
            summary.fraig,
            summary.sample_count,
            summary.crate_versions,
            format_progress_duration(phase_started.elapsed().as_secs_f64())
        );
        stdlib_g8r_vs_yosys.push(summary);
    }
    let phase_g8r_started = Instant::now();
    warn!("rebuild-web-indices phase=ir-fn-corpus-g8r-vs-yosys-abc begin");
    let ir_fn_corpus_g8r_vs_yosys =
        rebuild_ir_fn_corpus_g8r_vs_yosys_dataset_index(store, repo_root)?;
    warn!(
        "rebuild-web-indices phase=ir-fn-corpus-g8r-vs-yosys-abc done samples={} versions={} elapsed={}",
        ir_fn_corpus_g8r_vs_yosys.sample_count,
        ir_fn_corpus_g8r_vs_yosys.crate_versions,
        format_progress_duration(phase_g8r_started.elapsed().as_secs_f64())
    );
    let phase_frontend_started = Instant::now();
    warn!("rebuild-web-indices phase=ir-fn-g8r-abc-vs-codegen-yosys-abc begin");
    let ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc =
        rebuild_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index(store, repo_root)?;
    warn!(
        "rebuild-web-indices phase=ir-fn-g8r-abc-vs-codegen-yosys-abc done samples={} versions={} elapsed={}",
        ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc.sample_count,
        ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc.crate_versions,
        format_progress_duration(phase_frontend_started.elapsed().as_secs_f64())
    );
    warn!(
        "rebuild-web-indices done elapsed={}",
        format_progress_duration(started.elapsed().as_secs_f64())
    );
    Ok(WebIndicesRebuildSummary {
        generated_utc,
        versions_summary,
        stdlib_file_action_graph,
        stdlib_fns_trend,
        stdlib_fn_timeline,
        stdlib_g8r_vs_yosys,
        ir_fn_corpus_g8r_vs_yosys,
        ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc,
    })
}

fn build_ir_fn_corpus_g8r_vs_yosys_build_state(
    store: &ArtifactStore,
    _repo_root: &Path,
) -> Result<IrFnCorpusG8rVsYosysIndexState> {
    let started = Instant::now();
    let provenances = store.list_provenances_shared()?;
    let provenance_by_action_id = build_provenance_lookup(provenances.as_ref());

    let (
        structural_hash_by_ir_action_and_top,
        unique_structural_hash_by_ir_action,
        label_by_structural_hash,
        ir_node_count_by_structural_hash,
        _ir_node_count_by_source_ir_action_and_top,
        _unique_ir_node_count_by_source_ir_action,
    ) = load_ir_fn_corpus_entity_maps(store)?;

    let mut g8r_by_entity: BTreeMap<(String, String), StdlibAigStatsPoint> = BTreeMap::new();
    let mut yosys_by_entity: BTreeMap<(String, String), StdlibAigStatsPoint> = BTreeMap::new();
    let total_provenances = provenances.len();
    let mut stats_actions_seen = 0_usize;
    let mut last_progress_log_at = Instant::now();

    for (index, provenance) in provenances.iter().enumerate() {
        let scanned = index + 1;
        maybe_log_web_index_rebuild_progress(
            "ir-fn-corpus-g8r-vs-yosys-abc",
            scanned,
            total_provenances,
            stats_actions_seen,
            g8r_by_entity.len(),
            yosys_by_entity.len(),
            started,
            &mut last_progress_log_at,
            false,
        );
        let (aig_action_id, stats_dso_version) = match &provenance.action {
            ActionSpec::DriverAigToStats {
                aig_action_id,
                version,
                ..
            } => (
                aig_action_id.as_str(),
                normalize_tag_version(version).to_string(),
            ),
            _ => continue,
        };
        stats_actions_seen += 1;

        let stats_path = store.resolve_artifact_ref_path(&provenance.output_artifact);
        let stats_text = match fs::read_to_string(&stats_path) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let stats_json: serde_json::Value = match serde_json::from_str(&stats_text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(and_nodes) = parse_aig_stats_metric(&stats_json, "and_nodes")
            .or_else(|| parse_aig_stats_metric(&stats_json, "live_nodes"))
        else {
            continue;
        };
        let Some(depth) = parse_aig_stats_metric(&stats_json, "depth")
            .or_else(|| parse_aig_stats_metric(&stats_json, "deepest_path"))
        else {
            continue;
        };

        if let Some(source_ctx) = extract_stdlib_trend_source_context(
            &provenance_by_action_id,
            StdlibTrendKind::G8r,
            aig_action_id,
            false,
        ) {
            let Some(structural_hash) = resolve_ir_corpus_structural_hash_for_source(
                &structural_hash_by_ir_action_and_top,
                &unique_structural_hash_by_ir_action,
                &source_ctx.ir_action_id,
                source_ctx.ir_top.as_deref(),
            ) else {
                continue;
            };
            let crate_version = source_ctx.crate_version.clone();
            let short_len = std::cmp::min(12, structural_hash.len());
            let entity_label = label_by_structural_hash
                .get(&structural_hash)
                .cloned()
                .unwrap_or_else(|| format!("hash:{}", &structural_hash[..short_len]));
            let point = StdlibAigStatsPoint {
                fn_key: entity_label,
                ir_action_id: source_ctx.ir_action_id,
                ir_top: source_ctx.ir_top.clone(),
                crate_version: crate_version.clone(),
                dso_version: if stats_dso_version.is_empty() {
                    source_ctx.dso_version
                } else {
                    stats_dso_version.clone()
                },
                and_nodes,
                depth,
                created_utc: provenance.created_utc,
                stats_action_id: provenance.action_id.clone(),
            };
            upsert_aig_stats_point_by_key(
                &mut g8r_by_entity,
                (structural_hash, crate_version),
                point,
            );
            continue;
        }

        if let Some(source_ctx) = extract_stdlib_trend_source_context(
            &provenance_by_action_id,
            StdlibTrendKind::YosysAbc,
            aig_action_id,
            false,
        ) {
            let Some(structural_hash) = resolve_ir_corpus_structural_hash_for_source(
                &structural_hash_by_ir_action_and_top,
                &unique_structural_hash_by_ir_action,
                &source_ctx.ir_action_id,
                source_ctx.ir_top.as_deref(),
            ) else {
                continue;
            };
            let crate_version = source_ctx.crate_version.clone();
            let short_len = std::cmp::min(12, structural_hash.len());
            let entity_label = label_by_structural_hash
                .get(&structural_hash)
                .cloned()
                .unwrap_or_else(|| format!("hash:{}", &structural_hash[..short_len]));
            let point = StdlibAigStatsPoint {
                fn_key: entity_label,
                ir_action_id: source_ctx.ir_action_id,
                ir_top: source_ctx.ir_top.clone(),
                crate_version: crate_version.clone(),
                dso_version: if stats_dso_version.is_empty() {
                    source_ctx.dso_version
                } else {
                    stats_dso_version.clone()
                },
                and_nodes,
                depth,
                created_utc: provenance.created_utc,
                stats_action_id: provenance.action_id.clone(),
            };
            upsert_aig_stats_point_by_key(
                &mut yosys_by_entity,
                (structural_hash, crate_version),
                point,
            );
        }
    }
    maybe_log_web_index_rebuild_progress(
        "ir-fn-corpus-g8r-vs-yosys-abc",
        total_provenances,
        total_provenances,
        stats_actions_seen,
        g8r_by_entity.len(),
        yosys_by_entity.len(),
        started,
        &mut last_progress_log_at,
        true,
    );

    let dataset = build_ir_fn_corpus_dataset_from_entity_maps(
        store,
        &g8r_by_entity,
        &yosys_by_entity,
        Some(&ir_node_count_by_structural_hash),
        None,
        None,
        None,
    );
    info!(
        "query build_ir_fn_corpus_g8r_vs_yosys_dataset provenances={} g8r_candidates={} yosys_candidates={} paired_samples={} elapsed_ms={}",
        provenances.len(),
        g8r_by_entity.len(),
        yosys_by_entity.len(),
        dataset.samples.len(),
        started.elapsed().as_millis()
    );
    Ok(IrFnCorpusG8rVsYosysIndexState {
        dataset,
        g8r_by_entity,
        yosys_by_entity,
    })
}

fn extract_ir_fn_corpus_g8r_abc_vs_codegen_source_context(
    provenance_by_action_id: &ProvenanceLookup<'_>,
    aig_action_id: &str,
) -> Option<(
    IrFnCorpusStatsSourceKind,
    StdlibTrendSourceContext,
    Option<String>,
)> {
    let producer = provenance_by_action_id.get(aig_action_id)?;
    match &producer.action {
        ActionSpec::AigToYosysAbcAig {
            aig_action_id: upstream_aig_action_id,
            yosys_script_ref,
            ..
        } => {
            if !is_canonical_yosys_script_ref(yosys_script_ref) {
                return None;
            }
            let g8r_provenance = provenance_by_action_id.get(upstream_aig_action_id.as_str())?;
            let ActionSpec::DriverIrToG8rAig {
                ir_action_id,
                top_fn_name,
                fraig,
                lowering_mode,
                version,
                runtime,
                ..
            } = &g8r_provenance.action
            else {
                return None;
            };
            if *fraig || *lowering_mode != G8rLoweringMode::FrontendNoPrepRewrite {
                return None;
            }
            let structural_hash_hint = structural_hash_from_details(&g8r_provenance.details)
                .or_else(|| structural_hash_from_details(&producer.details));
            Some((
                IrFnCorpusStatsSourceKind::G8r,
                StdlibTrendSourceContext {
                    ir_action_id: ir_action_id.clone(),
                    ir_top: top_fn_name.clone().or_else(|| {
                        g8r_provenance
                            .details
                            .get("ir_top")
                            .and_then(|v| v.as_str())
                            .map(|v| v.to_string())
                    }),
                    crate_version: normalize_tag_version(&runtime.driver_version).to_string(),
                    dso_version: normalize_tag_version(version).to_string(),
                },
                structural_hash_hint,
            ))
        }
        ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id,
            yosys_script_ref,
            ..
        } => {
            if !is_canonical_yosys_script_ref(yosys_script_ref) {
                return None;
            }
            let verilog_provenance = provenance_by_action_id.get(verilog_action_id.as_str())?;
            let ActionSpec::IrFnToCombinationalVerilog {
                ir_action_id,
                top_fn_name,
                version,
                runtime,
                ..
            } = &verilog_provenance.action
            else {
                return None;
            };
            let structural_hash_hint = structural_hash_from_details(&verilog_provenance.details)
                .or_else(|| structural_hash_from_details(&producer.details));
            Some((
                IrFnCorpusStatsSourceKind::YosysAbc,
                StdlibTrendSourceContext {
                    ir_action_id: ir_action_id.clone(),
                    ir_top: top_fn_name.clone().or_else(|| {
                        verilog_provenance
                            .details
                            .get("ir_top")
                            .and_then(|v| v.as_str())
                            .map(|v| v.to_string())
                    }),
                    crate_version: normalize_tag_version(&runtime.driver_version).to_string(),
                    dso_version: normalize_tag_version(version).to_string(),
                },
                structural_hash_hint,
            ))
        }
        _ => None,
    }
}

fn build_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_build_state_with_seed(
    store: &ArtifactStore,
    _repo_root: &Path,
    seed_ir_node_count_cache: Option<BTreeMap<(String, Option<String>), u64>>,
) -> Result<IrFnCorpusG8rVsYosysIndexState> {
    let started = Instant::now();
    let provenances = store.list_provenances_shared()?;
    let provenance_by_action_id = build_provenance_lookup(provenances.as_ref());

    let (
        structural_hash_by_ir_action_and_top,
        unique_structural_hash_by_ir_action,
        label_by_structural_hash,
        ir_node_count_by_structural_hash,
        ir_node_count_by_source_ir_action_and_top,
        unique_ir_node_count_by_source_ir_action,
    ) = load_ir_fn_corpus_entity_maps(store)?;

    let mut g8r_by_entity: BTreeMap<(String, String), StdlibAigStatsPoint> = BTreeMap::new();
    let mut yosys_by_entity: BTreeMap<(String, String), StdlibAigStatsPoint> = BTreeMap::new();
    let total_provenances = provenances.len();
    let mut stats_actions_seen = 0_usize;
    let mut last_progress_log_at = Instant::now();

    for (index, stats_provenance) in provenances.iter().enumerate() {
        let scanned = index + 1;
        maybe_log_web_index_rebuild_progress(
            "ir-fn-g8r-abc-vs-codegen-yosys-abc",
            scanned,
            total_provenances,
            stats_actions_seen,
            g8r_by_entity.len(),
            yosys_by_entity.len(),
            started,
            &mut last_progress_log_at,
            false,
        );
        let (aig_action_id, stats_dso_version) = match &stats_provenance.action {
            ActionSpec::DriverAigToStats {
                aig_action_id,
                version,
                ..
            } => (
                aig_action_id.as_str(),
                normalize_tag_version(version).to_string(),
            ),
            _ => continue,
        };
        stats_actions_seen += 1;

        let stats_path = store.resolve_artifact_ref_path(&stats_provenance.output_artifact);
        let stats_text = match fs::read_to_string(&stats_path) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let stats_json: serde_json::Value = match serde_json::from_str(&stats_text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(and_nodes) = parse_aig_stats_metric(&stats_json, "and_nodes")
            .or_else(|| parse_aig_stats_metric(&stats_json, "live_nodes"))
        else {
            continue;
        };
        let Some(depth) = parse_aig_stats_metric(&stats_json, "depth")
            .or_else(|| parse_aig_stats_metric(&stats_json, "deepest_path"))
        else {
            continue;
        };

        let Some((source_kind, source_ctx, structural_hash_hint)) =
            extract_ir_fn_corpus_g8r_abc_vs_codegen_source_context(
                &provenance_by_action_id,
                aig_action_id,
            )
        else {
            continue;
        };

        let structural_hash = structural_hash_hint.or_else(|| {
            resolve_ir_corpus_structural_hash_for_source(
                &structural_hash_by_ir_action_and_top,
                &unique_structural_hash_by_ir_action,
                &source_ctx.ir_action_id,
                source_ctx.ir_top.as_deref(),
            )
        });
        let Some(structural_hash) = structural_hash else {
            continue;
        };

        let crate_version = source_ctx.crate_version.clone();
        let short_len = std::cmp::min(12, structural_hash.len());
        let entity_label = label_by_structural_hash
            .get(&structural_hash)
            .cloned()
            .unwrap_or_else(|| format!("hash:{}", &structural_hash[..short_len]));
        let point = StdlibAigStatsPoint {
            fn_key: entity_label,
            ir_action_id: source_ctx.ir_action_id,
            ir_top: source_ctx.ir_top.clone(),
            crate_version: crate_version.clone(),
            dso_version: if stats_dso_version.is_empty() {
                source_ctx.dso_version
            } else {
                stats_dso_version.clone()
            },
            and_nodes,
            depth,
            created_utc: stats_provenance.created_utc,
            stats_action_id: stats_provenance.action_id.clone(),
        };
        let target = match source_kind {
            IrFnCorpusStatsSourceKind::G8r => &mut g8r_by_entity,
            IrFnCorpusStatsSourceKind::YosysAbc => &mut yosys_by_entity,
        };
        upsert_aig_stats_point_by_key(target, (structural_hash, crate_version), point);
    }
    maybe_log_web_index_rebuild_progress(
        "ir-fn-g8r-abc-vs-codegen-yosys-abc",
        total_provenances,
        total_provenances,
        stats_actions_seen,
        g8r_by_entity.len(),
        yosys_by_entity.len(),
        started,
        &mut last_progress_log_at,
        true,
    );

    let dataset = build_ir_fn_corpus_dataset_from_entity_maps(
        store,
        &g8r_by_entity,
        &yosys_by_entity,
        Some(&ir_node_count_by_structural_hash),
        Some(&ir_node_count_by_source_ir_action_and_top),
        Some(&unique_ir_node_count_by_source_ir_action),
        seed_ir_node_count_cache,
    );
    info!(
        "query build_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset provenances={} g8r_abc_candidates={} codegen_yosys_candidates={} paired_samples={} elapsed_ms={}",
        provenance_by_action_id.len(),
        g8r_by_entity.len(),
        yosys_by_entity.len(),
        dataset.samples.len(),
        started.elapsed().as_millis()
    );
    Ok(IrFnCorpusG8rVsYosysIndexState {
        dataset,
        g8r_by_entity,
        yosys_by_entity,
    })
}

fn compute_ir_fn_corpus_stats_upsert_point(
    store: &ArtifactStore,
    _repo_root: &Path,
    stats_provenance: &Provenance,
) -> Result<Option<IrFnCorpusStatsUpsertPoint>> {
    let ActionSpec::DriverAigToStats {
        aig_action_id,
        version,
        ..
    } = &stats_provenance.action
    else {
        return Ok(None);
    };
    let stats_path = store.resolve_artifact_ref_path(&stats_provenance.output_artifact);
    let stats_text = match fs::read_to_string(&stats_path) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let stats_json: serde_json::Value = match serde_json::from_str(&stats_text) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let Some(and_nodes) = parse_aig_stats_metric(&stats_json, "and_nodes")
        .or_else(|| parse_aig_stats_metric(&stats_json, "live_nodes"))
    else {
        return Ok(None);
    };
    let Some(depth) = parse_aig_stats_metric(&stats_json, "depth")
        .or_else(|| parse_aig_stats_metric(&stats_json, "deepest_path"))
    else {
        return Ok(None);
    };

    let producer = store.load_provenance(aig_action_id)?;
    let stats_dso_version = normalize_tag_version(version).to_string();

    let (source_kind, ir_action_id, ir_top, crate_version, source_dso_version, mut structural_hash) =
        match &producer.action {
            ActionSpec::DriverIrToG8rAig {
                ir_action_id,
                top_fn_name,
                fraig,
                lowering_mode,
                version,
                runtime,
                ..
            } => {
                if *fraig || *lowering_mode != G8rLoweringMode::Default {
                    return Ok(None);
                }
                (
                    IrFnCorpusStatsSourceKind::G8r,
                    ir_action_id.clone(),
                    top_fn_name.clone().or_else(|| {
                        producer
                            .details
                            .get("ir_top")
                            .and_then(|v| v.as_str())
                            .map(|v| v.to_string())
                    }),
                    normalize_tag_version(&runtime.driver_version).to_string(),
                    normalize_tag_version(version).to_string(),
                    structural_hash_from_details(&producer.details),
                )
            }
            ActionSpec::ComboVerilogToYosysAbcAig {
                verilog_action_id,
                yosys_script_ref,
                ..
            } => {
                if !is_canonical_yosys_script_ref(yosys_script_ref) {
                    return Ok(None);
                }
                let verilog_provenance = store.load_provenance(verilog_action_id)?;
                let ActionSpec::IrFnToCombinationalVerilog {
                    ir_action_id,
                    top_fn_name,
                    version,
                    runtime,
                    ..
                } = &verilog_provenance.action
                else {
                    return Ok(None);
                };
                (
                    IrFnCorpusStatsSourceKind::YosysAbc,
                    ir_action_id.clone(),
                    top_fn_name.clone().or_else(|| {
                        verilog_provenance
                            .details
                            .get("ir_top")
                            .and_then(|v| v.as_str())
                            .map(|v| v.to_string())
                    }),
                    normalize_tag_version(&runtime.driver_version).to_string(),
                    normalize_tag_version(version).to_string(),
                    structural_hash_from_details(&verilog_provenance.details)
                        .or_else(|| structural_hash_from_details(&producer.details)),
                )
            }
            _ => return Ok(None),
        };

    if structural_hash.is_none() {
        let (structural_hash_by_ir_action_and_top, unique_structural_hash_by_ir_action, _, _, _, _) =
            load_ir_fn_corpus_entity_maps(store)?;
        structural_hash = resolve_ir_corpus_structural_hash_for_source(
            &structural_hash_by_ir_action_and_top,
            &unique_structural_hash_by_ir_action,
            &ir_action_id,
            ir_top.as_deref(),
        );
    }
    let Some(structural_hash) = structural_hash else {
        return Ok(None);
    };

    let fn_key = if let Some(ir_top) = ir_top.as_deref() {
        format!("{} [{}]", ir_top, short_structural_hash(&structural_hash))
    } else {
        format!("hash:{}", short_structural_hash(&structural_hash))
    };

    Ok(Some(IrFnCorpusStatsUpsertPoint {
        source_kind,
        structural_hash: structural_hash.clone(),
        crate_version: crate_version.clone(),
        point: StdlibAigStatsPoint {
            fn_key,
            ir_action_id,
            ir_top,
            crate_version,
            dso_version: if stats_dso_version.is_empty() {
                source_dso_version
            } else {
                stats_dso_version
            },
            and_nodes,
            depth,
            created_utc: stats_provenance.created_utc,
            stats_action_id: stats_provenance.action_id.clone(),
        },
    }))
}

pub(crate) fn maybe_upsert_ir_fn_corpus_g8r_vs_yosys_index_for_completed_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action_id: &str,
) -> Result<bool> {
    let provenance = match store.load_provenance(action_id) {
        Ok(p) => p,
        Err(_) => return Ok(false),
    };
    if !matches!(provenance.action, ActionSpec::DriverAigToStats { .. }) {
        return Ok(false);
    }
    let Some(mut upsert_point) =
        compute_ir_fn_corpus_stats_upsert_point(store, repo_root, &provenance)?
    else {
        return Ok(false);
    };
    let delta_key = ir_fn_corpus_incremental_delta_key(
        WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME,
        upsert_point.source_kind,
        &upsert_point.structural_hash,
        &upsert_point.crate_version,
    );
    if let Some(existing_bytes) = store.load_web_index_bytes(&delta_key)? {
        if let Ok(existing_row) =
            serde_json::from_slice::<IrFnCorpusIncrementalDeltaRow>(&existing_bytes)
            && existing_row.row.point.stats_action_id == upsert_point.point.stats_action_id
            && existing_row.row.point.created_utc == upsert_point.point.created_utc
        {
            return Ok(false);
        }
        // Preserve existing fn label when a previous row already established a curated one.
        if let Ok(existing_row) =
            serde_json::from_slice::<IrFnCorpusIncrementalDeltaRow>(&existing_bytes)
            && !existing_row.row.point.fn_key.trim().is_empty()
        {
            upsert_point.point.fn_key = existing_row.row.point.fn_key;
        }
    }
    write_ir_fn_corpus_incremental_delta_row(
        store,
        WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME,
        &upsert_point,
    )?;
    Ok(true)
}

fn compute_ir_fn_corpus_g8r_abc_vs_codegen_stats_upsert_point(
    store: &ArtifactStore,
    _repo_root: &Path,
    stats_provenance: &Provenance,
) -> Result<Option<IrFnCorpusStatsUpsertPoint>> {
    let ActionSpec::DriverAigToStats {
        aig_action_id,
        version,
        ..
    } = &stats_provenance.action
    else {
        return Ok(None);
    };
    let stats_path = store.resolve_artifact_ref_path(&stats_provenance.output_artifact);
    let stats_text = match fs::read_to_string(&stats_path) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let stats_json: serde_json::Value = match serde_json::from_str(&stats_text) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let Some(and_nodes) = parse_aig_stats_metric(&stats_json, "and_nodes")
        .or_else(|| parse_aig_stats_metric(&stats_json, "live_nodes"))
    else {
        return Ok(None);
    };
    let Some(depth) = parse_aig_stats_metric(&stats_json, "depth")
        .or_else(|| parse_aig_stats_metric(&stats_json, "deepest_path"))
    else {
        return Ok(None);
    };

    let producer = store.load_provenance(aig_action_id)?;
    let stats_dso_version = normalize_tag_version(version).to_string();

    let (source_kind, ir_action_id, ir_top, crate_version, source_dso_version, mut structural_hash) =
        match &producer.action {
            ActionSpec::AigToYosysAbcAig {
                aig_action_id: upstream_aig_action_id,
                yosys_script_ref,
                ..
            } => {
                if !is_canonical_yosys_script_ref(yosys_script_ref) {
                    return Ok(None);
                }
                let g8r_provenance = store.load_provenance(upstream_aig_action_id)?;
                let ActionSpec::DriverIrToG8rAig {
                    ir_action_id,
                    top_fn_name,
                    fraig,
                    lowering_mode,
                    version,
                    runtime,
                    ..
                } = &g8r_provenance.action
                else {
                    return Ok(None);
                };
                if *fraig || *lowering_mode != G8rLoweringMode::FrontendNoPrepRewrite {
                    return Ok(None);
                }
                (
                    IrFnCorpusStatsSourceKind::G8r,
                    ir_action_id.clone(),
                    top_fn_name.clone().or_else(|| {
                        g8r_provenance
                            .details
                            .get("ir_top")
                            .and_then(|v| v.as_str())
                            .map(|v| v.to_string())
                    }),
                    normalize_tag_version(&runtime.driver_version).to_string(),
                    normalize_tag_version(version).to_string(),
                    structural_hash_from_details(&g8r_provenance.details)
                        .or_else(|| structural_hash_from_details(&producer.details)),
                )
            }
            ActionSpec::ComboVerilogToYosysAbcAig {
                verilog_action_id,
                yosys_script_ref,
                ..
            } => {
                if !is_canonical_yosys_script_ref(yosys_script_ref) {
                    return Ok(None);
                }
                let verilog_provenance = store.load_provenance(verilog_action_id)?;
                let ActionSpec::IrFnToCombinationalVerilog {
                    ir_action_id,
                    top_fn_name,
                    version,
                    runtime,
                    ..
                } = &verilog_provenance.action
                else {
                    return Ok(None);
                };
                (
                    IrFnCorpusStatsSourceKind::YosysAbc,
                    ir_action_id.clone(),
                    top_fn_name.clone().or_else(|| {
                        verilog_provenance
                            .details
                            .get("ir_top")
                            .and_then(|v| v.as_str())
                            .map(|v| v.to_string())
                    }),
                    normalize_tag_version(&runtime.driver_version).to_string(),
                    normalize_tag_version(version).to_string(),
                    structural_hash_from_details(&verilog_provenance.details)
                        .or_else(|| structural_hash_from_details(&producer.details)),
                )
            }
            _ => return Ok(None),
        };

    if structural_hash.is_none() {
        let (structural_hash_by_ir_action_and_top, unique_structural_hash_by_ir_action, _, _, _, _) =
            load_ir_fn_corpus_entity_maps(store)?;
        structural_hash = resolve_ir_corpus_structural_hash_for_source(
            &structural_hash_by_ir_action_and_top,
            &unique_structural_hash_by_ir_action,
            &ir_action_id,
            ir_top.as_deref(),
        );
    }
    let Some(structural_hash) = structural_hash else {
        return Ok(None);
    };

    let fn_key = if let Some(ir_top) = ir_top.as_deref() {
        format!("{} [{}]", ir_top, short_structural_hash(&structural_hash))
    } else {
        format!("hash:{}", short_structural_hash(&structural_hash))
    };

    Ok(Some(IrFnCorpusStatsUpsertPoint {
        source_kind,
        structural_hash: structural_hash.clone(),
        crate_version: crate_version.clone(),
        point: StdlibAigStatsPoint {
            fn_key,
            ir_action_id,
            ir_top,
            crate_version,
            dso_version: if stats_dso_version.is_empty() {
                source_dso_version
            } else {
                stats_dso_version
            },
            and_nodes,
            depth,
            created_utc: stats_provenance.created_utc,
            stats_action_id: stats_provenance.action_id.clone(),
        },
    }))
}

pub(crate) fn maybe_upsert_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_index_for_completed_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action_id: &str,
) -> Result<bool> {
    let provenance = match store.load_provenance(action_id) {
        Ok(p) => p,
        Err(_) => return Ok(false),
    };
    if !matches!(provenance.action, ActionSpec::DriverAigToStats { .. }) {
        return Ok(false);
    }
    let Some(mut upsert_point) =
        compute_ir_fn_corpus_g8r_abc_vs_codegen_stats_upsert_point(store, repo_root, &provenance)?
    else {
        return Ok(false);
    };
    let delta_key = ir_fn_corpus_incremental_delta_key(
        WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME,
        upsert_point.source_kind,
        &upsert_point.structural_hash,
        &upsert_point.crate_version,
    );
    if let Some(existing_bytes) = store.load_web_index_bytes(&delta_key)? {
        if let Ok(existing_row) =
            serde_json::from_slice::<IrFnCorpusIncrementalDeltaRow>(&existing_bytes)
            && existing_row.row.point.stats_action_id == upsert_point.point.stats_action_id
            && existing_row.row.point.created_utc == upsert_point.point.created_utc
        {
            return Ok(false);
        }
        if let Ok(existing_row) =
            serde_json::from_slice::<IrFnCorpusIncrementalDeltaRow>(&existing_bytes)
            && !existing_row.row.point.fn_key.trim().is_empty()
        {
            upsert_point.point.fn_key = existing_row.row.point.fn_key;
        }
    }
    write_ir_fn_corpus_incremental_delta_row(
        store,
        WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME,
        &upsert_point,
    )?;
    Ok(true)
}

pub(crate) fn action_kind_short_label(action: &ActionSpec) -> &'static str {
    match action {
        ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { .. } => "stdlib",
        ActionSpec::DownloadAndExtractXlsynthSourceSubtree { .. } => "src-subtree",
        ActionSpec::DriverDslxFnToIr { .. } => "dslx2ir",
        ActionSpec::DriverIrToOpt { .. } => "ir2opt",
        ActionSpec::DriverIrToDelayInfo { .. } => "ir-delay",
        ActionSpec::DriverIrEquiv { .. } => "ir-equiv",
        ActionSpec::DriverIrAigEquiv { .. } => "ir-aig-equiv",
        ActionSpec::DriverIrToG8rAig { .. } => "ir2g8r",
        ActionSpec::IrFnToCombinationalVerilog { .. } => "ir2combo",
        ActionSpec::IrFnToKBoolConeCorpus { .. } => "ir2kcone",
        ActionSpec::ComboVerilogToYosysAbcAig { .. } => "yosys-abc",
        ActionSpec::AigToYosysAbcAig { .. } => "aig2abc",
        ActionSpec::DriverAigToStats { .. } => "aig-stats",
        ActionSpec::AigStatDiff { .. } => "aig-diff",
    }
}

pub(crate) fn script_ref_short_label(script_ref: &ScriptRef) -> String {
    let normalized = script_ref.path.replace('\\', "/");
    Path::new(&normalized)
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or(normalized)
}

pub(crate) fn action_graph_node_label(action: &ActionSpec) -> String {
    match action {
        ActionSpec::DriverDslxFnToIr { dslx_fn_name, .. } => {
            format!("dslx2ir\n{}", dslx_fn_name)
        }
        ActionSpec::DriverIrToOpt {
            top_fn_name: Some(top),
            ..
        } => {
            format!("ir2opt\n{}", top)
        }
        ActionSpec::DriverIrToOpt {
            top_fn_name: None, ..
        } => action_kind_short_label(action).to_string(),
        ActionSpec::DriverIrToDelayInfo {
            top_fn_name: Some(top),
            ..
        } => {
            format!("ir-delay\n{}", top)
        }
        ActionSpec::DriverIrToDelayInfo {
            top_fn_name: None, ..
        } => action_kind_short_label(action).to_string(),
        ActionSpec::DriverIrToG8rAig {
            top_fn_name: Some(top),
            fraig,
            lowering_mode,
            ..
        } => {
            let mode_suffix = if *lowering_mode == G8rLoweringMode::Default {
                ""
            } else {
                "+front"
            };
            format!(
                "ir2g8r{}{}\n{}",
                if *fraig { "+fraig" } else { "" },
                mode_suffix,
                top
            )
        }
        ActionSpec::DriverIrToG8rAig {
            top_fn_name: None,
            fraig,
            lowering_mode,
            ..
        } => {
            let mode_suffix = if *lowering_mode == G8rLoweringMode::Default {
                ""
            } else {
                "+front"
            };
            format!(
                "ir2g8r{}{}",
                if *fraig { "+fraig" } else { "" },
                mode_suffix
            )
        }
        ActionSpec::IrFnToCombinationalVerilog {
            top_fn_name: Some(top),
            ..
        } => {
            format!("ir2combo\n{}", top)
        }
        ActionSpec::IrFnToCombinationalVerilog {
            top_fn_name: None, ..
        } => action_kind_short_label(action).to_string(),
        ActionSpec::IrFnToKBoolConeCorpus {
            top_fn_name: Some(top),
            k,
            max_ir_ops,
            ..
        } => {
            let cfg = max_ir_ops
                .map(|v| format!("k={k},ops<={v}"))
                .unwrap_or_else(|| format!("k={k}"));
            format!("ir2kcone({})\n{}", cfg, top)
        }
        ActionSpec::IrFnToKBoolConeCorpus {
            top_fn_name: None,
            k,
            max_ir_ops,
            ..
        } => {
            let cfg = max_ir_ops
                .map(|v| format!("k={k},ops<={v}"))
                .unwrap_or_else(|| format!("k={k}"));
            format!("ir2kcone({})", cfg)
        }
        ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_top_module_name: Some(top),
            yosys_script_ref,
            ..
        } => {
            format!(
                "yosys-abc\n{}\n{}",
                top,
                script_ref_short_label(yosys_script_ref)
            )
        }
        ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_top_module_name: None,
            yosys_script_ref,
            ..
        } => format!(
            "{}\n{}",
            action_kind_short_label(action),
            script_ref_short_label(yosys_script_ref)
        ),
        ActionSpec::AigToYosysAbcAig {
            yosys_script_ref, ..
        } => format!(
            "{}\n{}",
            action_kind_short_label(action),
            script_ref_short_label(yosys_script_ref)
        ),
        ActionSpec::DriverAigToStats { .. } => action_kind_short_label(action).to_string(),
        ActionSpec::DriverIrEquiv {
            top_fn_name: Some(top),
            ..
        } => {
            format!("ir-equiv\n{}", top)
        }
        ActionSpec::DriverIrEquiv {
            top_fn_name: None, ..
        } => action_kind_short_label(action).to_string(),
        ActionSpec::DriverIrAigEquiv {
            top_fn_name: Some(top),
            ..
        } => {
            format!("ir-aig-equiv\n{}", top)
        }
        ActionSpec::DriverIrAigEquiv {
            top_fn_name: None, ..
        } => action_kind_short_label(action).to_string(),
        ActionSpec::AigStatDiff { .. } => action_kind_short_label(action).to_string(),
        ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { .. } => {
            action_kind_short_label(action).to_string()
        }
        ActionSpec::DownloadAndExtractXlsynthSourceSubtree { subtree, .. } => {
            format!("src-subtree\n{}", subtree)
        }
    }
}

fn action_is_k_bool_cone_corpus(action: &ActionSpec) -> bool {
    matches!(action, ActionSpec::IrFnToKBoolConeCorpus { .. })
}

pub(crate) fn action_is_expander(action: &ActionSpec) -> bool {
    matches!(
        action,
        ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { .. }
            | ActionSpec::DownloadAndExtractXlsynthSourceSubtree { .. }
            | ActionSpec::IrFnToKBoolConeCorpus { .. }
    )
}

pub(crate) fn build_stdlib_file_action_graph_dataset(
    store: &ArtifactStore,
    repo_root: &Path,
    requested_crate_version: Option<&str>,
    requested_file: Option<&str>,
    requested_fn_name: Option<&str>,
    requested_action_id: Option<&str>,
    include_k3_descendants: bool,
) -> Result<StdlibFileActionGraphDataset> {
    let fast_path_started = Instant::now();
    if let Ok(Some(index_state)) = load_stdlib_file_action_graph_dataset_index(store) {
        match build_stdlib_file_action_graph_dataset_from_index_state(
            store,
            repo_root,
            &index_state,
            requested_crate_version,
            requested_file,
            requested_fn_name,
            requested_action_id,
            include_k3_descendants,
        ) {
            Ok(dataset) => {
                info!(
                    "query build_stdlib_file_action_graph_dataset source=index elapsed_ms={} nodes={} edges={}",
                    fast_path_started.elapsed().as_millis(),
                    dataset.nodes.len(),
                    dataset.edges.len()
                );
                return Ok(dataset);
            }
            Err(err) => warn!(
                "query build_stdlib_file_action_graph_dataset index path failed; falling back to live scan: {:#}",
                err
            ),
        }
    }
    match rebuild_stdlib_file_action_graph_dataset_index(store, repo_root) {
        Ok(summary) => {
            info!(
                "query build_stdlib_file_action_graph_dataset rebuilt index actions={} crate_versions={} index_bytes={} elapsed_ms={}",
                summary.action_count,
                summary.crate_versions,
                summary.index_bytes,
                summary.elapsed_ms
            );
            if let Ok(Some(index_state)) = load_stdlib_file_action_graph_dataset_index(store) {
                match build_stdlib_file_action_graph_dataset_from_index_state(
                    store,
                    repo_root,
                    &index_state,
                    requested_crate_version,
                    requested_file,
                    requested_fn_name,
                    requested_action_id,
                    include_k3_descendants,
                ) {
                    Ok(dataset) => {
                        info!(
                            "query build_stdlib_file_action_graph_dataset source=index-after-rebuild elapsed_ms={} nodes={} edges={}",
                            fast_path_started.elapsed().as_millis(),
                            dataset.nodes.len(),
                            dataset.edges.len()
                        );
                        return Ok(dataset);
                    }
                    Err(err) => warn!(
                        "query build_stdlib_file_action_graph_dataset rebuilt index path failed; falling back to live scan: {:#}",
                        err
                    ),
                }
            }
        }
        Err(err) => warn!(
            "query build_stdlib_file_action_graph_dataset index rebuild failed; falling back to live scan: {:#}",
            err
        ),
    }

    let started = Instant::now();
    let provenances = store.list_provenances_shared()?;
    let provenance_by_action_id = build_provenance_lookup(provenances.as_ref());

    let mut action_specs: BTreeMap<String, ActionSpec> = BTreeMap::new();
    let mut suggested_edges: Vec<RawActionGraphEdge> = Vec::new();
    for provenance in provenances.iter() {
        action_specs.insert(provenance.action_id.clone(), provenance.action.clone());
        for suggested in &provenance.suggested_next_actions {
            action_specs
                .entry(suggested.action_id.clone())
                .or_insert_with(|| suggested.action.clone());
            suggested_edges.push(RawActionGraphEdge {
                source: provenance.action_id.clone(),
                target: suggested.action_id.clone(),
                edge_kind: "suggested".to_string(),
                role: suggested.reason.clone(),
            });
        }
    }

    for queue_path in list_queue_files(&store.queue_pending_dir())? {
        let text = match fs::read_to_string(&queue_path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("reading pending queue record: {}", queue_path.display())
                });
            }
        };
        let (action_id, _, _, action) = match parse_queue_work_item(&text, &queue_path) {
            Ok(item) => item,
            Err(err) => {
                eprintln!(
                    "warning: skipping malformed pending queue record {}: {:#}",
                    queue_path.display(),
                    err
                );
                continue;
            }
        };
        action_specs.entry(action_id).or_insert(action);
    }
    for queue_path in list_queue_files(&store.queue_running_dir())? {
        let text = match fs::read_to_string(&queue_path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("reading running queue record: {}", queue_path.display())
                });
            }
        };
        let (action_id, _, _, action) = match parse_queue_work_item(&text, &queue_path) {
            Ok(item) => item,
            Err(err) => {
                eprintln!(
                    "warning: skipping malformed running queue record {}: {:#}",
                    queue_path.display(),
                    err
                );
                continue;
            }
        };
        action_specs.entry(action_id).or_insert(action);
    }
    for failed in load_failed_queue_records(store)? {
        action_specs
            .entry(failed.action_id)
            .or_insert(failed.action);
    }
    for queue_path in list_queue_files(&store.queue_canceled_dir())? {
        let text = match fs::read_to_string(&queue_path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("reading canceled queue record: {}", queue_path.display())
                });
            }
        };
        let canceled: QueueCanceled = match serde_json::from_str(&text)
            .with_context(|| format!("parsing canceled queue record: {}", queue_path.display()))
        {
            Ok(record) => record,
            Err(err) => {
                eprintln!(
                    "warning: skipping malformed canceled queue record {}: {:#}",
                    queue_path.display(),
                    err
                );
                continue;
            }
        };
        action_specs
            .entry(canceled.action_id)
            .or_insert(canceled.action);
    }

    let compat_by_dso = load_compat_by_dso(repo_root);
    let mut crate_by_action_id: BTreeMap<String, String> = BTreeMap::new();
    let mut action_ids_by_crate: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (action_id, action) in &action_specs {
        if let Some(crate_version) = infer_crate_version_for_action(store, action, &compat_by_dso) {
            let normalized = normalize_tag_version(&crate_version).to_string();
            crate_by_action_id.insert(action_id.clone(), normalized.clone());
            action_ids_by_crate
                .entry(normalized)
                .or_default()
                .insert(action_id.clone());
        }
    }

    let mut files_by_crate_version: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut functions_by_crate_file: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    let mut roots_by_crate_file_fn: BTreeMap<(String, String, String), Vec<String>> =
        BTreeMap::new();
    for (action_id, action) in &action_specs {
        let ActionSpec::DriverDslxFnToIr {
            dslx_file,
            dslx_fn_name,
            ..
        } = action
        else {
            continue;
        };
        let Some(crate_version) = crate_by_action_id.get(action_id) else {
            continue;
        };
        let normalized_file = dslx_file.replace('\\', "/");
        files_by_crate_version
            .entry(crate_version.clone())
            .or_default()
            .insert(normalized_file.clone());
        functions_by_crate_file
            .entry((crate_version.clone(), normalized_file.clone()))
            .or_default()
            .insert(dslx_fn_name.clone());
        roots_by_crate_file_fn
            .entry((crate_version.clone(), normalized_file, dslx_fn_name.clone()))
            .or_default()
            .push(action_id.clone());
    }
    for roots in roots_by_crate_file_fn.values_mut() {
        roots.sort();
        roots.dedup();
    }

    let mut dependency_edges_by_source: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    for (target_action_id, action) in &action_specs {
        for (role, dependency_action_id) in action_dependency_role_action_ids(action) {
            dependency_edges_by_source
                .entry(dependency_action_id.to_string())
                .or_default()
                .push((target_action_id.clone(), role.to_string()));
        }
    }
    for edges in dependency_edges_by_source.values_mut() {
        edges.sort();
        edges.dedup();
    }
    let mut suggested_edges_by_source: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    for edge in &suggested_edges {
        suggested_edges_by_source
            .entry(edge.source.clone())
            .or_default()
            .push((edge.target.clone(), edge.role.clone()));
    }
    for edges in suggested_edges_by_source.values_mut() {
        edges.sort();
        edges.dedup();
    }

    let mut available_crate_versions: Vec<String> =
        files_by_crate_version.keys().cloned().collect();
    available_crate_versions.sort_by(|a, b| cmp_dotted_numeric_version(a, b));
    let selected_action_id = requested_action_id
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty());
    let action_focus_found = selected_action_id
        .as_ref()
        .map(|v| action_specs.contains_key(v))
        .unwrap_or(false);
    let action_focus_crate_version = selected_action_id
        .as_ref()
        .and_then(|action_id| action_specs.get(action_id))
        .and_then(|action| infer_crate_version_for_action(store, action, &compat_by_dso))
        .map(|v| normalize_tag_version(&v).to_string());
    let selected_crate_version = requested_crate_version
        .map(|v| normalize_tag_version(v.trim()).to_string())
        .filter(|v| available_crate_versions.binary_search(v).is_ok())
        .or_else(|| {
            action_focus_crate_version.filter(|v| available_crate_versions.binary_search(v).is_ok())
        })
        .or_else(|| available_crate_versions.last().cloned());
    let available_files: Vec<String> = selected_crate_version
        .as_ref()
        .and_then(|v| files_by_crate_version.get(v))
        .map(|files| files.iter().cloned().collect())
        .unwrap_or_default();
    let selected_file = requested_file
        .map(|v| v.trim().replace('\\', "/"))
        .filter(|v| !v.is_empty())
        .filter(|v| available_files.binary_search(v).is_ok());

    if selected_crate_version.is_none() && !action_focus_found {
        let dataset = StdlibFileActionGraphDataset {
            available_crate_versions,
            selected_crate_version,
            available_files,
            selected_file,
            available_functions: Vec::new(),
            selected_function: None,
            include_k3_descendants,
            selected_action_id,
            action_focus_found,
            root_action_ids: Vec::new(),
            total_actions_for_crate: 0,
            nodes: Vec::new(),
            edges: Vec::new(),
        };
        info!(
            "query build_stdlib_file_action_graph_dataset selected_crate=<none> provenances={} actions={} nodes={} edges={} elapsed_ms={}",
            provenances.len(),
            action_specs.len(),
            dataset.nodes.len(),
            dataset.edges.len(),
            started.elapsed().as_millis()
        );
        return Ok(dataset);
    }

    let selected_action_ids: HashSet<String> = selected_crate_version
        .as_ref()
        .and_then(|v| action_ids_by_crate.get(v))
        .map(|ids| ids.iter().cloned().collect())
        .unwrap_or_default();
    let available_functions: Vec<String> = selected_crate_version
        .as_ref()
        .and_then(|crate_version| {
            selected_file.as_ref().and_then(|file| {
                functions_by_crate_file
                    .get(&(crate_version.clone(), file.clone()))
                    .map(|fns| fns.iter().cloned().collect())
            })
        })
        .unwrap_or_default();
    let selected_function = requested_fn_name
        .map(|v| v.trim().to_string())
        .filter(|v| available_functions.binary_search(v).is_ok())
        .or_else(|| available_functions.first().cloned());
    let mut root_action_ids = Vec::new();
    if action_focus_found {
        if let Some(action_id) = selected_action_id.as_ref() {
            root_action_ids.push(action_id.clone());
        }
    } else if let (Some(crate_version), Some(file), Some(fn_name)) = (
        selected_crate_version.as_ref(),
        selected_file.as_ref(),
        selected_function.as_ref(),
    ) && let Some(roots) =
        roots_by_crate_file_fn.get(&(crate_version.clone(), file.clone(), fn_name.clone()))
    {
        root_action_ids.extend(roots.iter().cloned());
    }
    root_action_ids.sort();
    root_action_ids.dedup();

    let mut visited: BTreeSet<String> = BTreeSet::new();
    let mut edge_set: BTreeSet<RawActionGraphEdge> = BTreeSet::new();
    let mut dependency_pairs: HashSet<(String, String)> = HashSet::new();
    if action_focus_found {
        if let Some(root_action_id) = root_action_ids.first() {
            visited.insert(root_action_id.clone());
            for (action_id, _) in
                collect_transitive_upstream_action_ids(root_action_id, &action_specs)
            {
                visited.insert(action_id);
            }
            let reverse_dependency_index = build_reverse_dependency_index(&action_specs);
            for (action_id, _) in
                collect_transitive_downstream_action_ids(root_action_id, &reverse_dependency_index)
            {
                visited.insert(action_id);
            }
        }

        for action_id in &visited {
            let Some(action) = action_specs.get(action_id) else {
                continue;
            };
            for (role, dependency_action_id) in action_dependency_role_action_ids(action) {
                if visited.contains(dependency_action_id) {
                    dependency_pairs.insert((dependency_action_id.to_string(), action_id.clone()));
                    edge_set.insert(RawActionGraphEdge {
                        source: dependency_action_id.to_string(),
                        target: action_id.clone(),
                        edge_kind: "dependency".to_string(),
                        role: role.to_string(),
                    });
                }
            }
        }
        for edge in &suggested_edges {
            if dependency_pairs.contains(&(edge.source.clone(), edge.target.clone())) {
                continue;
            }
            if visited.contains(&edge.source) && visited.contains(&edge.target) {
                edge_set.insert(edge.clone());
            }
        }
    } else {
        let mut queue: VecDeque<String> = VecDeque::new();
        for root_action_id in &root_action_ids {
            if visited.insert(root_action_id.clone()) {
                queue.push_back(root_action_id.clone());
            }
        }
        while let Some(current) = queue.pop_front() {
            let Some(current_action) = action_specs.get(&current) else {
                continue;
            };
            let descend = include_k3_descendants || !action_is_k_bool_cone_corpus(current_action);
            if !descend {
                continue;
            }

            if let Some(dependency_targets) = dependency_edges_by_source.get(&current) {
                for (target, role) in dependency_targets {
                    if !selected_action_ids.contains(&current)
                        || !selected_action_ids.contains(target)
                    {
                        continue;
                    }
                    dependency_pairs.insert((current.clone(), target.clone()));
                    edge_set.insert(RawActionGraphEdge {
                        source: current.clone(),
                        target: target.clone(),
                        edge_kind: "dependency".to_string(),
                        role: role.clone(),
                    });
                    if visited.insert(target.clone()) {
                        queue.push_back(target.clone());
                    }
                }
            }
            if let Some(suggested_targets) = suggested_edges_by_source.get(&current) {
                for (target, role) in suggested_targets {
                    if !selected_action_ids.contains(&current)
                        || !selected_action_ids.contains(target)
                    {
                        continue;
                    }
                    if dependency_pairs.contains(&(current.clone(), target.clone())) {
                        continue;
                    }
                    edge_set.insert(RawActionGraphEdge {
                        source: current.clone(),
                        target: target.clone(),
                        edge_kind: "suggested".to_string(),
                        role: role.clone(),
                    });
                    if visited.insert(target.clone()) {
                        queue.push_back(target.clone());
                    }
                }
            }
        }
    }

    let root_set: HashSet<&str> = root_action_ids.iter().map(String::as_str).collect();
    let mut nodes = Vec::new();
    for action_id in &visited {
        let Some(action) = action_specs.get(action_id) else {
            continue;
        };
        let state = if store.action_exists(action_id) {
            QueueState::Done
        } else {
            queue_state_for_action(store, action_id)
        };
        nodes.push(StdlibFileActionGraphNode {
            action_id: action_id.clone(),
            label: action_graph_node_label(action),
            kind: action_kind_label(action).to_string(),
            subject: action_subject(action),
            state_label: queue_state_display_label(&state).to_string(),
            state_key: queue_state_key(&state).to_string(),
            has_provenance: provenance_by_action_id.contains_key(action_id.as_str()),
            is_root: root_set.contains(action_id.as_str()),
        });
    }

    let mut edges = Vec::new();
    for edge in edge_set {
        if !visited.contains(&edge.source) || !visited.contains(&edge.target) {
            continue;
        }
        edges.push(StdlibFileActionGraphEdge {
            source: edge.source,
            target: edge.target,
            edge_kind: edge.edge_kind,
            role: edge.role,
        });
    }

    let dataset = StdlibFileActionGraphDataset {
        available_crate_versions,
        selected_crate_version,
        available_files,
        selected_file,
        available_functions,
        selected_function,
        include_k3_descendants,
        selected_action_id,
        action_focus_found,
        root_action_ids,
        total_actions_for_crate: selected_action_ids.len(),
        nodes,
        edges,
    };
    info!(
        "query build_stdlib_file_action_graph_dataset selected_crate={} selected_file={} selected_fn={} selected_action_id={} action_focus_found={} include_k3_descendants={} provenances={} actions={} nodes={} edges={} elapsed_ms={}",
        dataset
            .selected_crate_version
            .as_deref()
            .unwrap_or("<none>"),
        dataset.selected_file.as_deref().unwrap_or("<none>"),
        dataset.selected_function.as_deref().unwrap_or("<none>"),
        dataset.selected_action_id.as_deref().unwrap_or("<none>"),
        dataset.action_focus_found,
        dataset.include_k3_descendants,
        provenances.len(),
        action_specs.len(),
        dataset.nodes.len(),
        dataset.edges.len(),
        started.elapsed().as_millis()
    );
    Ok(dataset)
}

fn insert_sorted_unique_string(values: &mut Vec<String>, value: String) {
    match values.binary_search(&value) {
        Ok(_) => {}
        Err(index) => values.insert(index, value),
    }
}

fn insert_sorted_unique_edge(
    values: &mut Vec<StdlibFileActionGraphTargetEdge>,
    value: StdlibFileActionGraphTargetEdge,
) {
    match values.binary_search(&value) {
        Ok(_) => {}
        Err(index) => values.insert(index, value),
    }
}

fn collect_runtime_action_specs_for_stdlib_file_action_graph(
    store: &ArtifactStore,
) -> Result<Vec<(String, ActionSpec)>> {
    let mut action_specs_by_id: BTreeMap<String, ActionSpec> = BTreeMap::new();
    for queue_path in list_queue_files(&store.queue_pending_dir())? {
        let text = match fs::read_to_string(&queue_path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("reading pending queue record: {}", queue_path.display())
                });
            }
        };
        let (action_id, _, _, action) = match parse_queue_work_item(&text, &queue_path) {
            Ok(item) => item,
            Err(err) => {
                eprintln!(
                    "warning: skipping malformed pending queue record {}: {:#}",
                    queue_path.display(),
                    err
                );
                continue;
            }
        };
        action_specs_by_id.entry(action_id).or_insert(action);
    }
    for queue_path in list_queue_files(&store.queue_running_dir())? {
        let text = match fs::read_to_string(&queue_path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("reading running queue record: {}", queue_path.display())
                });
            }
        };
        let (action_id, _, _, action) = match parse_queue_work_item(&text, &queue_path) {
            Ok(item) => item,
            Err(err) => {
                eprintln!(
                    "warning: skipping malformed running queue record {}: {:#}",
                    queue_path.display(),
                    err
                );
                continue;
            }
        };
        action_specs_by_id.entry(action_id).or_insert(action);
    }
    for failed in load_failed_queue_records(store)? {
        action_specs_by_id
            .entry(failed.action_id)
            .or_insert(failed.action);
    }
    for queue_path in list_queue_files(&store.queue_canceled_dir())? {
        let text = match fs::read_to_string(&queue_path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("reading canceled queue record: {}", queue_path.display())
                });
            }
        };
        let canceled: QueueCanceled = match serde_json::from_str(&text)
            .with_context(|| format!("parsing canceled queue record: {}", queue_path.display()))
        {
            Ok(record) => record,
            Err(err) => {
                eprintln!(
                    "warning: skipping malformed canceled queue record {}: {:#}",
                    queue_path.display(),
                    err
                );
                continue;
            }
        };
        action_specs_by_id
            .entry(canceled.action_id)
            .or_insert(canceled.action);
    }
    Ok(action_specs_by_id.into_iter().collect())
}

fn apply_runtime_action_specs_to_stdlib_file_action_graph_index_state(
    store: &ArtifactStore,
    repo_root: &Path,
    state: &mut StdlibFileActionGraphIndexState,
) -> Result<()> {
    let compat_by_dso = load_compat_by_dso(repo_root);
    let runtime_action_specs = collect_runtime_action_specs_for_stdlib_file_action_graph(store)?;
    for (action_id, action) in runtime_action_specs {
        if state.action_specs.contains_key(&action_id) {
            continue;
        }
        state.action_specs.insert(action_id.clone(), action.clone());

        if let Some(crate_version) = infer_crate_version_for_action(store, &action, &compat_by_dso)
        {
            let normalized = normalize_tag_version(&crate_version).to_string();
            state
                .crate_by_action_id
                .insert(action_id.clone(), normalized.clone());
            insert_sorted_unique_string(
                state
                    .action_ids_by_crate
                    .entry(normalized.clone())
                    .or_default(),
                action_id.clone(),
            );
            if let ActionSpec::DriverDslxFnToIr {
                dslx_file,
                dslx_fn_name,
                ..
            } = &action
            {
                let normalized_file = dslx_file.replace('\\', "/");
                insert_sorted_unique_string(
                    state
                        .files_by_crate_version
                        .entry(normalized.clone())
                        .or_default(),
                    normalized_file.clone(),
                );
                let functions_by_file = state
                    .functions_by_crate_file
                    .entry(normalized.clone())
                    .or_default();
                insert_sorted_unique_string(
                    functions_by_file
                        .entry(normalized_file.clone())
                        .or_default(),
                    dslx_fn_name.clone(),
                );
                let roots_by_file = state.roots_by_crate_file_fn.entry(normalized).or_default();
                let roots_by_fn = roots_by_file.entry(normalized_file).or_default();
                insert_sorted_unique_string(
                    roots_by_fn.entry(dslx_fn_name.clone()).or_default(),
                    action_id.clone(),
                );
            }
        }

        for (role, dependency_action_id) in action_dependency_role_action_ids(&action) {
            insert_sorted_unique_edge(
                state
                    .dependency_edges_by_source
                    .entry(dependency_action_id.to_string())
                    .or_default(),
                StdlibFileActionGraphTargetEdge {
                    target: action_id.clone(),
                    role: role.to_string(),
                },
            );
        }
    }
    Ok(())
}

fn build_stdlib_file_action_graph_dataset_from_index_state(
    store: &ArtifactStore,
    repo_root: &Path,
    index_state: &StdlibFileActionGraphIndexState,
    requested_crate_version: Option<&str>,
    requested_file: Option<&str>,
    requested_fn_name: Option<&str>,
    requested_action_id: Option<&str>,
    include_k3_descendants: bool,
) -> Result<StdlibFileActionGraphDataset> {
    let started = Instant::now();
    let mut state = index_state.clone();
    apply_runtime_action_specs_to_stdlib_file_action_graph_index_state(
        store, repo_root, &mut state,
    )?;

    let mut available_crate_versions: Vec<String> = state
        .action_ids_by_crate
        .keys()
        .chain(state.files_by_crate_version.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    available_crate_versions.sort_by(|a, b| cmp_dotted_numeric_version(a, b));
    let selected_action_id = requested_action_id
        .map(|v| v.trim().to_ascii_lowercase())
        .filter(|v| !v.is_empty());
    let action_focus_found = selected_action_id
        .as_ref()
        .map(|v| state.action_specs.contains_key(v))
        .unwrap_or(false);
    let action_focus_crate_version = selected_action_id
        .as_ref()
        .and_then(|action_id| state.crate_by_action_id.get(action_id))
        .cloned();
    let selected_crate_version = requested_crate_version
        .map(|v| normalize_tag_version(v.trim()).to_string())
        .filter(|v| available_crate_versions.binary_search(v).is_ok())
        .or_else(|| {
            action_focus_crate_version.filter(|v| available_crate_versions.binary_search(v).is_ok())
        })
        .or_else(|| available_crate_versions.last().cloned());
    let available_files: Vec<String> = selected_crate_version
        .as_ref()
        .and_then(|v| state.files_by_crate_version.get(v))
        .cloned()
        .unwrap_or_default();
    let selected_file = requested_file
        .map(|v| v.trim().replace('\\', "/"))
        .filter(|v| !v.is_empty())
        .filter(|v| available_files.binary_search(v).is_ok());

    if selected_crate_version.is_none() && !action_focus_found {
        let dataset = StdlibFileActionGraphDataset {
            available_crate_versions,
            selected_crate_version,
            available_files,
            selected_file,
            available_functions: Vec::new(),
            selected_function: None,
            include_k3_descendants,
            selected_action_id,
            action_focus_found,
            root_action_ids: Vec::new(),
            total_actions_for_crate: 0,
            nodes: Vec::new(),
            edges: Vec::new(),
        };
        info!(
            "query build_stdlib_file_action_graph_dataset_from_index_state selected_crate=<none> actions={} nodes={} edges={} elapsed_ms={}",
            state.action_specs.len(),
            dataset.nodes.len(),
            dataset.edges.len(),
            started.elapsed().as_millis()
        );
        return Ok(dataset);
    }

    let selected_action_ids: HashSet<String> = selected_crate_version
        .as_ref()
        .and_then(|v| state.action_ids_by_crate.get(v))
        .map(|ids| ids.iter().cloned().collect())
        .unwrap_or_default();
    let available_functions: Vec<String> = selected_crate_version
        .as_ref()
        .and_then(|crate_version| {
            selected_file.as_ref().and_then(|file| {
                state
                    .functions_by_crate_file
                    .get(crate_version)
                    .and_then(|by_file| by_file.get(file))
                    .cloned()
            })
        })
        .unwrap_or_default();
    let selected_function = requested_fn_name
        .map(|v| v.trim().to_string())
        .filter(|v| available_functions.binary_search(v).is_ok())
        .or_else(|| available_functions.first().cloned());
    let mut root_action_ids = Vec::new();
    if action_focus_found {
        if let Some(action_id) = selected_action_id.as_ref() {
            root_action_ids.push(action_id.clone());
        }
    } else if let (Some(crate_version), Some(file), Some(fn_name)) = (
        selected_crate_version.as_ref(),
        selected_file.as_ref(),
        selected_function.as_ref(),
    ) && let Some(roots) = state
        .roots_by_crate_file_fn
        .get(crate_version)
        .and_then(|by_file| by_file.get(file))
        .and_then(|by_fn| by_fn.get(fn_name))
    {
        root_action_ids.extend(roots.iter().cloned());
    }
    root_action_ids.sort();
    root_action_ids.dedup();

    let mut visited: BTreeSet<String> = BTreeSet::new();
    let mut edge_set: BTreeSet<RawActionGraphEdge> = BTreeSet::new();
    let mut dependency_pairs: HashSet<(String, String)> = HashSet::new();
    if action_focus_found {
        if let Some(root_action_id) = root_action_ids.first() {
            visited.insert(root_action_id.clone());
            for (action_id, _) in
                collect_transitive_upstream_action_ids(root_action_id, &state.action_specs)
            {
                visited.insert(action_id);
            }
            let reverse_dependency_index = build_reverse_dependency_index(&state.action_specs);
            for (action_id, _) in
                collect_transitive_downstream_action_ids(root_action_id, &reverse_dependency_index)
            {
                visited.insert(action_id);
            }
        }

        for action_id in &visited {
            let Some(action) = state.action_specs.get(action_id) else {
                continue;
            };
            for (role, dependency_action_id) in action_dependency_role_action_ids(action) {
                if visited.contains(dependency_action_id) {
                    dependency_pairs.insert((dependency_action_id.to_string(), action_id.clone()));
                    edge_set.insert(RawActionGraphEdge {
                        source: dependency_action_id.to_string(),
                        target: action_id.clone(),
                        edge_kind: "dependency".to_string(),
                        role: role.to_string(),
                    });
                }
            }
        }
        for source in &visited {
            let Some(targets) = state.suggested_edges_by_source.get(source) else {
                continue;
            };
            for target in targets {
                if dependency_pairs.contains(&(source.clone(), target.target.clone())) {
                    continue;
                }
                if visited.contains(source) && visited.contains(&target.target) {
                    edge_set.insert(RawActionGraphEdge {
                        source: source.clone(),
                        target: target.target.clone(),
                        edge_kind: "suggested".to_string(),
                        role: target.role.clone(),
                    });
                }
            }
        }
    } else {
        let mut queue: VecDeque<String> = VecDeque::new();
        for root_action_id in &root_action_ids {
            if visited.insert(root_action_id.clone()) {
                queue.push_back(root_action_id.clone());
            }
        }
        while let Some(current) = queue.pop_front() {
            let Some(current_action) = state.action_specs.get(&current) else {
                continue;
            };
            let descend = include_k3_descendants || !action_is_k_bool_cone_corpus(current_action);
            if !descend {
                continue;
            }

            if let Some(dependency_targets) = state.dependency_edges_by_source.get(&current) {
                for target in dependency_targets {
                    if !selected_action_ids.contains(&current)
                        || !selected_action_ids.contains(&target.target)
                    {
                        continue;
                    }
                    dependency_pairs.insert((current.clone(), target.target.clone()));
                    edge_set.insert(RawActionGraphEdge {
                        source: current.clone(),
                        target: target.target.clone(),
                        edge_kind: "dependency".to_string(),
                        role: target.role.clone(),
                    });
                    if visited.insert(target.target.clone()) {
                        queue.push_back(target.target.clone());
                    }
                }
            }
            if let Some(suggested_targets) = state.suggested_edges_by_source.get(&current) {
                for target in suggested_targets {
                    if !selected_action_ids.contains(&current)
                        || !selected_action_ids.contains(&target.target)
                    {
                        continue;
                    }
                    if dependency_pairs.contains(&(current.clone(), target.target.clone())) {
                        continue;
                    }
                    edge_set.insert(RawActionGraphEdge {
                        source: current.clone(),
                        target: target.target.clone(),
                        edge_kind: "suggested".to_string(),
                        role: target.role.clone(),
                    });
                    if visited.insert(target.target.clone()) {
                        queue.push_back(target.target.clone());
                    }
                }
            }
        }
    }

    let root_set: HashSet<&str> = root_action_ids.iter().map(String::as_str).collect();
    let mut nodes = Vec::new();
    for action_id in &visited {
        let Some(action) = state.action_specs.get(action_id) else {
            continue;
        };
        let state = if store.action_exists(action_id) {
            QueueState::Done
        } else {
            queue_state_for_action(store, action_id)
        };
        nodes.push(StdlibFileActionGraphNode {
            action_id: action_id.clone(),
            label: action_graph_node_label(action),
            kind: action_kind_label(action).to_string(),
            subject: action_subject(action),
            state_label: queue_state_display_label(&state).to_string(),
            state_key: queue_state_key(&state).to_string(),
            has_provenance: index_state
                .action_ids_with_provenance
                .contains(action_id.as_str()),
            is_root: root_set.contains(action_id.as_str()),
        });
    }

    let mut edges = Vec::new();
    for edge in edge_set {
        if !visited.contains(&edge.source) || !visited.contains(&edge.target) {
            continue;
        }
        edges.push(StdlibFileActionGraphEdge {
            source: edge.source,
            target: edge.target,
            edge_kind: edge.edge_kind,
            role: edge.role,
        });
    }

    let dataset = StdlibFileActionGraphDataset {
        available_crate_versions,
        selected_crate_version,
        available_files,
        selected_file,
        available_functions,
        selected_function,
        include_k3_descendants,
        selected_action_id,
        action_focus_found,
        root_action_ids,
        total_actions_for_crate: selected_action_ids.len(),
        nodes,
        edges,
    };
    info!(
        "query build_stdlib_file_action_graph_dataset_from_index_state selected_crate={} selected_file={} selected_fn={} selected_action_id={} action_focus_found={} include_k3_descendants={} actions={} nodes={} edges={} elapsed_ms={}",
        dataset
            .selected_crate_version
            .as_deref()
            .unwrap_or("<none>"),
        dataset.selected_file.as_deref().unwrap_or("<none>"),
        dataset.selected_function.as_deref().unwrap_or("<none>"),
        dataset.selected_action_id.as_deref().unwrap_or("<none>"),
        dataset.action_focus_found,
        dataset.include_k3_descendants,
        state.action_specs.len(),
        dataset.nodes.len(),
        dataset.edges.len(),
        started.elapsed().as_millis()
    );
    Ok(dataset)
}

fn build_stdlib_file_action_graph_index_state(
    store: &ArtifactStore,
    repo_root: &Path,
) -> Result<StdlibFileActionGraphIndexState> {
    let started = Instant::now();
    let provenances = store.list_provenances_shared()?;

    let mut action_specs: BTreeMap<String, ActionSpec> = BTreeMap::new();
    let mut action_ids_with_provenance: BTreeSet<String> = BTreeSet::new();
    let mut suggested_edges: Vec<RawActionGraphEdge> = Vec::new();
    for provenance in provenances.iter() {
        action_ids_with_provenance.insert(provenance.action_id.clone());
        action_specs.insert(provenance.action_id.clone(), provenance.action.clone());
        for suggested in &provenance.suggested_next_actions {
            action_specs
                .entry(suggested.action_id.clone())
                .or_insert_with(|| suggested.action.clone());
            suggested_edges.push(RawActionGraphEdge {
                source: provenance.action_id.clone(),
                target: suggested.action_id.clone(),
                edge_kind: "suggested".to_string(),
                role: suggested.reason.clone(),
            });
        }
    }

    let compat_by_dso = load_compat_by_dso(repo_root);
    let mut crate_by_action_id: BTreeMap<String, String> = BTreeMap::new();
    let mut action_ids_by_crate_set: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (action_id, action) in &action_specs {
        if let Some(crate_version) = infer_crate_version_for_action(store, action, &compat_by_dso) {
            let normalized = normalize_tag_version(&crate_version).to_string();
            crate_by_action_id.insert(action_id.clone(), normalized.clone());
            action_ids_by_crate_set
                .entry(normalized)
                .or_default()
                .insert(action_id.clone());
        }
    }

    let mut files_by_crate_version_set: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut functions_by_crate_file_set: BTreeMap<String, BTreeMap<String, BTreeSet<String>>> =
        BTreeMap::new();
    let mut roots_by_crate_file_fn_set: BTreeMap<
        String,
        BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    > = BTreeMap::new();
    for (action_id, action) in &action_specs {
        let ActionSpec::DriverDslxFnToIr {
            dslx_file,
            dslx_fn_name,
            ..
        } = action
        else {
            continue;
        };
        let Some(crate_version) = crate_by_action_id.get(action_id) else {
            continue;
        };
        let normalized_file = dslx_file.replace('\\', "/");
        files_by_crate_version_set
            .entry(crate_version.clone())
            .or_default()
            .insert(normalized_file.clone());
        functions_by_crate_file_set
            .entry(crate_version.clone())
            .or_default()
            .entry(normalized_file.clone())
            .or_default()
            .insert(dslx_fn_name.clone());
        roots_by_crate_file_fn_set
            .entry(crate_version.clone())
            .or_default()
            .entry(normalized_file)
            .or_default()
            .entry(dslx_fn_name.clone())
            .or_default()
            .insert(action_id.clone());
    }

    let mut dependency_edges_by_source_set: BTreeMap<
        String,
        BTreeSet<StdlibFileActionGraphTargetEdge>,
    > = BTreeMap::new();
    for (target_action_id, action) in &action_specs {
        for (role, dependency_action_id) in action_dependency_role_action_ids(action) {
            dependency_edges_by_source_set
                .entry(dependency_action_id.to_string())
                .or_default()
                .insert(StdlibFileActionGraphTargetEdge {
                    target: target_action_id.clone(),
                    role: role.to_string(),
                });
        }
    }
    let mut suggested_edges_by_source_set: BTreeMap<
        String,
        BTreeSet<StdlibFileActionGraphTargetEdge>,
    > = BTreeMap::new();
    for edge in &suggested_edges {
        suggested_edges_by_source_set
            .entry(edge.source.clone())
            .or_default()
            .insert(StdlibFileActionGraphTargetEdge {
                target: edge.target.clone(),
                role: edge.role.clone(),
            });
    }

    let action_ids_by_crate = action_ids_by_crate_set
        .into_iter()
        .map(|(crate_version, ids)| (crate_version, ids.into_iter().collect()))
        .collect();
    let files_by_crate_version = files_by_crate_version_set
        .into_iter()
        .map(|(crate_version, files)| (crate_version, files.into_iter().collect()))
        .collect();
    let functions_by_crate_file = functions_by_crate_file_set
        .into_iter()
        .map(|(crate_version, by_file)| {
            let by_file_vecs = by_file
                .into_iter()
                .map(|(file, fns)| (file, fns.into_iter().collect()))
                .collect();
            (crate_version, by_file_vecs)
        })
        .collect();
    let roots_by_crate_file_fn = roots_by_crate_file_fn_set
        .into_iter()
        .map(|(crate_version, by_file)| {
            let by_file_vecs = by_file
                .into_iter()
                .map(|(file, by_fn)| {
                    let by_fn_vecs = by_fn
                        .into_iter()
                        .map(|(fn_name, roots)| (fn_name, roots.into_iter().collect()))
                        .collect();
                    (file, by_fn_vecs)
                })
                .collect();
            (crate_version, by_file_vecs)
        })
        .collect();
    let dependency_edges_by_source = dependency_edges_by_source_set
        .into_iter()
        .map(|(source, targets)| (source, targets.into_iter().collect()))
        .collect();
    let suggested_edges_by_source = suggested_edges_by_source_set
        .into_iter()
        .map(|(source, targets)| (source, targets.into_iter().collect()))
        .collect();
    let state = StdlibFileActionGraphIndexState {
        action_specs,
        action_ids_with_provenance,
        crate_by_action_id,
        action_ids_by_crate,
        files_by_crate_version,
        functions_by_crate_file,
        roots_by_crate_file_fn,
        dependency_edges_by_source,
        suggested_edges_by_source,
    };
    info!(
        "query build_stdlib_file_action_graph_index_state provenances={} actions={} crate_versions={} dependency_edges={} suggested_edges={} elapsed_ms={}",
        provenances.len(),
        state.action_specs.len(),
        state.action_ids_by_crate.len(),
        state
            .dependency_edges_by_source
            .values()
            .map(Vec::len)
            .sum::<usize>(),
        state
            .suggested_edges_by_source
            .values()
            .map(Vec::len)
            .sum::<usize>(),
        started.elapsed().as_millis()
    );
    Ok(state)
}

fn load_stdlib_file_action_graph_dataset_index(
    store: &ArtifactStore,
) -> Result<Option<StdlibFileActionGraphIndexState>> {
    let key = WEB_STDLIB_FILE_ACTION_GRAPH_INDEX_FILENAME;
    let location = store.web_index_location(key);
    let Some(bytes) = store
        .load_web_index_bytes(key)
        .with_context(|| format!("reading stdlib file action graph web index: {}", location))?
    else {
        return Ok(None);
    };
    let index_file: StdlibFileActionGraphIndexFile = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing stdlib file action graph web index: {}", location))?;
    if index_file.schema_version != WEB_STDLIB_FILE_ACTION_GRAPH_INDEX_SCHEMA_VERSION {
        info!(
            "query stdlib file action graph web index schema mismatch location={} expected={} got={}",
            location, WEB_STDLIB_FILE_ACTION_GRAPH_INDEX_SCHEMA_VERSION, index_file.schema_version
        );
        return Ok(None);
    }
    info!(
        "query stdlib file action graph web index hit location={} generated_utc={} actions={} crate_versions={} dependency_edges={} suggested_edges={}",
        location,
        index_file.generated_utc,
        index_file.state.action_specs.len(),
        index_file.state.action_ids_by_crate.len(),
        index_file
            .state
            .dependency_edges_by_source
            .values()
            .map(Vec::len)
            .sum::<usize>(),
        index_file
            .state
            .suggested_edges_by_source
            .values()
            .map(Vec::len)
            .sum::<usize>()
    );
    Ok(Some(index_file.state))
}

fn write_stdlib_file_action_graph_dataset_index(
    store: &ArtifactStore,
    state: &StdlibFileActionGraphIndexState,
) -> Result<(String, u64, DateTime<Utc>)> {
    let key = WEB_STDLIB_FILE_ACTION_GRAPH_INDEX_FILENAME;
    let location = store.web_index_location(key);
    let generated_utc = Utc::now();
    let payload = StdlibFileActionGraphIndexFile {
        schema_version: WEB_STDLIB_FILE_ACTION_GRAPH_INDEX_SCHEMA_VERSION,
        generated_utc,
        state: state.clone(),
    };
    let bytes =
        serde_json::to_vec(&payload).context("serializing stdlib file action graph web index")?;
    store
        .write_web_index_bytes(key, &bytes)
        .with_context(|| format!("writing stdlib file action graph web index: {}", location))?;
    info!(
        "query stdlib file action graph web index write location={} actions={} crate_versions={} bytes={}",
        location,
        state.action_specs.len(),
        state.action_ids_by_crate.len(),
        bytes.len()
    );
    Ok((location, bytes.len() as u64, generated_utc))
}

pub(crate) fn rebuild_stdlib_file_action_graph_dataset_index(
    store: &ArtifactStore,
    repo_root: &Path,
) -> Result<StdlibFileActionGraphIndexSummary> {
    let started = Instant::now();
    let state = build_stdlib_file_action_graph_index_state(store, repo_root)?;
    let action_count = state.action_specs.len();
    let crate_versions = state.action_ids_by_crate.len();
    let dependency_edges = state
        .dependency_edges_by_source
        .values()
        .map(Vec::len)
        .sum();
    let suggested_edges = state.suggested_edges_by_source.values().map(Vec::len).sum();
    let (index_location, index_bytes, generated_utc) =
        write_stdlib_file_action_graph_dataset_index(store, &state)?;
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok(StdlibFileActionGraphIndexSummary {
        generated_utc,
        index_path: index_location,
        action_count,
        crate_versions,
        dependency_edges,
        suggested_edges,
        index_bytes,
        elapsed_ms,
    })
}

pub(crate) fn make_stdlib_aig_stats_point(
    provenance_by_action_id: &ProvenanceLookup<'_>,
    source_ctx: StdlibTrendSourceContext,
    stats_action_id: &str,
    created_utc: DateTime<Utc>,
    stats_dso_version: &str,
    and_nodes: f64,
    depth: f64,
) -> Option<StdlibAigStatsPoint> {
    let (dslx_file, dslx_fn_name) = resolve_direct_dslx_origin_from_opt_ir_action(
        provenance_by_action_id,
        &source_ctx.ir_action_id,
    )?;
    let dslx_file = dslx_file.replace('\\', "/");
    Some(StdlibAigStatsPoint {
        fn_key: format!("{dslx_file}::{dslx_fn_name}"),
        ir_action_id: source_ctx.ir_action_id,
        ir_top: source_ctx.ir_top,
        crate_version: source_ctx.crate_version,
        dso_version: if stats_dso_version.is_empty() {
            source_ctx.dso_version
        } else {
            stats_dso_version.to_string()
        },
        and_nodes,
        depth,
        created_utc,
        stats_action_id: stats_action_id.to_string(),
    })
}

pub(crate) fn upsert_aig_stats_point_by_key<K: Ord>(
    map: &mut BTreeMap<K, StdlibAigStatsPoint>,
    key: K,
    point: StdlibAigStatsPoint,
) {
    match map.entry(key) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(point);
        }
        std::collections::btree_map::Entry::Occupied(mut entry) => {
            let existing = entry.get();
            let should_replace = point.created_utc > existing.created_utc
                || (point.created_utc == existing.created_utc
                    && point.stats_action_id > existing.stats_action_id);
            if should_replace {
                entry.insert(point);
            }
        }
    }
}

pub(crate) fn upsert_stdlib_fn_trend_point_by_crate(
    map: &mut BTreeMap<String, StdlibFnTrendPoint>,
    point: StdlibFnTrendPoint,
) {
    match map.entry(point.crate_version.clone()) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(point);
        }
        std::collections::btree_map::Entry::Occupied(mut entry) => {
            let existing = entry.get();
            let should_replace = point.created_utc > existing.created_utc
                || (point.created_utc == existing.created_utc
                    && point.stats_action_id > existing.stats_action_id);
            if should_replace {
                entry.insert(point);
            }
        }
    }
}

pub(crate) fn upsert_delay_point_by_crate(
    map: &mut BTreeMap<String, StdlibFnDelayPoint>,
    point: StdlibFnDelayPoint,
) {
    match map.entry(point.crate_version.clone()) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(point);
        }
        std::collections::btree_map::Entry::Occupied(mut entry) => {
            let existing = entry.get();
            let should_replace = point.created_utc > existing.created_utc
                || (point.created_utc == existing.created_utc
                    && point.action_id > existing.action_id);
            if should_replace {
                entry.insert(point);
            }
        }
    }
}

pub(crate) fn resolve_ir_node_count_cached(
    store: &ArtifactStore,
    ir_action_id: &str,
    ir_top: Option<&str>,
    cache: &mut BTreeMap<(String, Option<String>), u64>,
) -> Option<u64> {
    let cache_key = (
        ir_action_id.to_string(),
        ir_top.map(|value| value.to_string()),
    );
    if let Some(cached) = cache.get(&cache_key) {
        return Some(*cached);
    }
    let ir_dep = load_dependency_of_type(store, ir_action_id, ArtifactType::IrPackageFile).ok()?;
    let ir_path = store.resolve_artifact_ref_path(&ir_dep);
    let ir_text = fs::read_to_string(&ir_path).ok()?;
    let count = if let Some(ir_top) = ir_top {
        parse_ir_fn_node_count_by_name(&ir_text, ir_top)
            .or_else(|| parse_ir_top_node_count(&ir_text))
    } else {
        parse_ir_top_node_count(&ir_text)
    }?;
    cache.insert(cache_key, count);
    Some(count)
}

pub(crate) fn build_stdlib_sample_details(
    store: &ArtifactStore,
    ir_action_id: &str,
    requested_ir_top: Option<&str>,
) -> Result<StdlibSampleDetailsView> {
    let ir_dep = load_dependency_of_type(store, ir_action_id, ArtifactType::IrPackageFile)?;
    let ir_path = store.resolve_artifact_ref_path(&ir_dep);
    let ir_text = fs::read_to_string(&ir_path)
        .with_context(|| format!("reading IR artifact text: {}", ir_path.display()))?;

    let ir_top_fn_name = requested_ir_top
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| infer_first_ir_function_name(&ir_text));
    let ir_top_fn_text = ir_top_fn_name
        .as_deref()
        .and_then(|fn_name| extract_ir_fn_block_by_name(&ir_text, fn_name).ok());

    let mut dslx_file = None;
    let mut dslx_fn_name = None;
    let mut dslx_subtree_action_id = None;
    let mut dslx_text = None;
    if let Some(origin) = resolve_dslx_origin_action_context_from_ir_action(store, ir_action_id)? {
        let normalized_dslx_file = origin.dslx_file.replace('\\', "/");
        validate_relative_subpath(&normalized_dslx_file)?;
        let subtree_dep = load_dependency_of_type(
            store,
            &origin.dslx_subtree_action_id,
            ArtifactType::DslxFileSubtree,
        )?;
        let subtree_root = store.resolve_artifact_ref_path(&subtree_dep);
        let dslx_path = subtree_root.join(&normalized_dslx_file);
        if dslx_path.exists() && dslx_path.is_file() {
            let text = fs::read_to_string(&dslx_path)
                .with_context(|| format!("reading DSLX source text: {}", dslx_path.display()))?;
            dslx_text = Some(text);
        }
        dslx_file = Some(normalized_dslx_file);
        dslx_fn_name = Some(origin.dslx_fn_name);
        dslx_subtree_action_id = Some(origin.dslx_subtree_action_id);
    }

    Ok(StdlibSampleDetailsView {
        ir_action_id: ir_action_id.to_string(),
        ir_artifact_relpath: ir_dep.relpath,
        dslx_file,
        dslx_fn_name,
        dslx_subtree_action_id,
        ir_top_fn_name,
        ir_top_fn_text,
        ir_text,
        dslx_text,
    })
}

pub(crate) fn build_associated_ir_view(
    store: &ArtifactStore,
    ir_action_id: &str,
    requested_ir_top: Option<&str>,
) -> Result<AssociatedIrView> {
    let ir_dep = load_dependency_of_type(store, ir_action_id, ArtifactType::IrPackageFile)?;
    let ir_path = store.resolve_artifact_ref_path(&ir_dep);
    let ir_text = fs::read_to_string(&ir_path)
        .with_context(|| format!("reading IR artifact text: {}", ir_path.display()))?;

    let ir_top_fn_name = requested_ir_top
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| infer_first_ir_function_name(&ir_text));
    let ir_top_fn_text = ir_top_fn_name
        .as_deref()
        .and_then(|fn_name| extract_ir_fn_block_by_name(&ir_text, fn_name).ok());

    Ok(AssociatedIrView {
        ir_action_id: ir_action_id.to_string(),
        ir_artifact_relpath: ir_dep.relpath,
        ir_top_fn_name,
        ir_top_fn_text,
        ir_text,
    })
}

fn infer_first_ir_function_name(ir_text: &str) -> Option<String> {
    for line in ir_text.lines() {
        let trimmed = line.trim_start();
        let rest = if let Some(rest) = trimmed.strip_prefix("top fn ") {
            rest
        } else if let Some(rest) = trimmed.strip_prefix("fn ") {
            rest
        } else {
            continue;
        };
        let Some(open_paren) = rest.find('(') else {
            continue;
        };
        let candidate_name = rest[..open_paren].trim();
        if !candidate_name.is_empty() {
            return Some(candidate_name.to_string());
        }
    }
    None
}

pub(crate) struct StdlibTrendSourceContext {
    pub(crate) ir_action_id: String,
    pub(crate) ir_top: Option<String>,
    pub(crate) crate_version: String,
    pub(crate) dso_version: String,
}

pub(crate) fn extract_stdlib_trend_source_context(
    provenance_by_action_id: &ProvenanceLookup<'_>,
    kind: StdlibTrendKind,
    aig_action_id: &str,
    fraig: bool,
) -> Option<StdlibTrendSourceContext> {
    let producer = *provenance_by_action_id.get(aig_action_id)?;
    match (&kind, &producer.action) {
        (
            StdlibTrendKind::G8r,
            ActionSpec::DriverIrToG8rAig {
                ir_action_id,
                top_fn_name,
                fraig: producer_fraig,
                lowering_mode,
                version,
                runtime,
                ..
            },
        ) => {
            if *producer_fraig != fraig || *lowering_mode != G8rLoweringMode::Default {
                return None;
            }
            Some(StdlibTrendSourceContext {
                ir_action_id: ir_action_id.clone(),
                ir_top: top_fn_name.clone().or_else(|| {
                    producer
                        .details
                        .get("ir_top")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                }),
                crate_version: normalize_tag_version(&runtime.driver_version).to_string(),
                dso_version: normalize_tag_version(version).to_string(),
            })
        }
        (
            StdlibTrendKind::YosysAbc,
            ActionSpec::ComboVerilogToYosysAbcAig {
                verilog_action_id,
                yosys_script_ref,
                ..
            },
        ) => {
            if !is_canonical_yosys_script_ref(yosys_script_ref) {
                return None;
            }
            let verilog_provenance = *provenance_by_action_id.get(verilog_action_id.as_str())?;
            let ActionSpec::IrFnToCombinationalVerilog {
                ir_action_id,
                top_fn_name,
                version,
                runtime,
                ..
            } = &verilog_provenance.action
            else {
                return None;
            };
            Some(StdlibTrendSourceContext {
                ir_action_id: ir_action_id.clone(),
                ir_top: top_fn_name.clone().or_else(|| {
                    verilog_provenance
                        .details
                        .get("ir_top")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                }),
                crate_version: normalize_tag_version(&runtime.driver_version).to_string(),
                dso_version: normalize_tag_version(version).to_string(),
            })
        }
        _ => None,
    }
}

pub(crate) fn resolve_direct_dslx_origin_from_opt_ir_action(
    provenance_by_action_id: &ProvenanceLookup<'_>,
    opt_ir_action_id: &str,
) -> Option<(String, String)> {
    let opt_provenance = *provenance_by_action_id.get(opt_ir_action_id)?;
    let ActionSpec::DriverIrToOpt { ir_action_id, .. } = &opt_provenance.action else {
        return None;
    };
    let source_provenance = *provenance_by_action_id.get(ir_action_id.as_str())?;
    let ActionSpec::DriverDslxFnToIr {
        dslx_file,
        dslx_fn_name,
        ..
    } = &source_provenance.action
    else {
        return None;
    };
    Some((dslx_file.clone(), dslx_fn_name.clone()))
}

pub(crate) fn parse_aig_stats_metric(stats_json: &serde_json::Value, key: &str) -> Option<f64> {
    let value = stats_json.get(key)?;
    match value {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

pub(crate) fn parse_delay_info_total_delay_ps(delay_info_textproto: &str) -> Option<f64> {
    let mut in_critical_path = false;
    let mut brace_depth = 0_i64;
    let mut first_total_delay_anywhere = None;
    for line in delay_info_textproto.lines() {
        let trimmed = line.trim();
        if let Some(value_text) = trimmed.strip_prefix("total_delay_ps:") {
            let value = value_text.trim().parse::<f64>().ok();
            if first_total_delay_anywhere.is_none() {
                first_total_delay_anywhere = value;
            }
            if in_critical_path && value.is_some() {
                return value;
            }
        }
        if !in_critical_path && trimmed.starts_with("combinational_critical_path") {
            in_critical_path = true;
            brace_depth = 0;
        }
        if in_critical_path {
            brace_depth += line.bytes().filter(|b| *b == b'{').count() as i64;
            brace_depth -= line.bytes().filter(|b| *b == b'}').count() as i64;
            if brace_depth <= 0 {
                in_critical_path = false;
            }
        }
    }
    first_total_delay_anywhere
}

pub(crate) fn ensure_aiger_input_header(path: &Path) -> Result<()> {
    let mut file = fs::File::open(path)
        .with_context(|| format!("opening AIG input file: {}", path.display()))?;
    let mut header = [0_u8; 16];
    let bytes_read = file
        .read(&mut header)
        .with_context(|| format!("reading AIG input header: {}", path.display()))?;
    if bytes_read < 3 {
        bail!(
            "input artifact is too short to be AIGER: {} ({} bytes)",
            path.display(),
            bytes_read
        );
    }
    let prefix = &header[..3];
    if prefix == b"aig" || prefix == b"aag" {
        return Ok(());
    }
    let shown = format_binary_prefix(&header[..bytes_read]);
    bail!(
        "input artifact is not AIGER (header_prefix={}); likely legacy ir2g8r --bin-out payload rather than AIGER",
        shown
    );
}

pub(crate) fn format_binary_prefix(bytes: &[u8]) -> String {
    let mut out = String::new();
    for b in bytes.iter().take(16) {
        if b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b' ') {
            out.push(*b as char);
        } else {
            out.push_str(&format!("\\x{:02x}", b));
        }
    }
    out
}

pub(crate) fn normalize_legacy_g8r_stats_payload(
    raw_path: &Path,
    output_path: &Path,
) -> Result<()> {
    let text = fs::read_to_string(raw_path)
        .with_context(|| format!("reading legacy g8r stats JSON: {}", raw_path.display()))?;
    let mut value: serde_json::Value = serde_json::from_str(&text)
        .with_context(|| format!("parsing legacy g8r stats JSON: {}", raw_path.display()))?;

    let and_nodes = parse_aig_stats_metric(&value, "and_nodes")
        .or_else(|| parse_aig_stats_metric(&value, "live_nodes"));
    let depth = parse_aig_stats_metric(&value, "depth")
        .or_else(|| parse_aig_stats_metric(&value, "deepest_path"));
    let object = value.as_object_mut().ok_or_else(|| {
        anyhow!(
            "legacy g8r stats payload is not a JSON object: {}",
            raw_path.display()
        )
    })?;
    if object.get("and_nodes").is_none()
        && let Some(v) = and_nodes
    {
        object.insert("and_nodes".to_string(), json!(v));
    }
    if object.get("depth").is_none()
        && let Some(v) = depth
    {
        object.insert("depth".to_string(), json!(v));
    }

    fs::write(
        output_path,
        serde_json::to_string_pretty(&value).context("serializing normalized legacy g8r stats")?,
    )
    .with_context(|| {
        format!(
            "writing normalized legacy g8r stats: {}",
            output_path.display()
        )
    })?;
    Ok(())
}

pub(crate) fn query_component_escape(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{:02X}", byte)),
        }
    }
    out
}

pub(crate) fn path_component_escape(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b':' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{:02X}", byte)),
        }
    }
    out
}

pub(crate) fn parse_stdlib_fn_route_selector(raw: &str) -> Result<(String, String)> {
    let trimmed = raw.trim().trim_matches('/');
    let Some((file_selector, dslx_fn_name)) = trimmed.rsplit_once(':') else {
        bail!("missing `:` separator");
    };
    let file_selector = file_selector.trim().replace('\\', "/");
    let dslx_fn_name = dslx_fn_name.trim().to_string();
    if file_selector.is_empty() {
        bail!("missing file selector");
    }
    if dslx_fn_name.is_empty() {
        bail!("missing function name");
    }
    Ok((file_selector, dslx_fn_name))
}

pub(crate) fn stdlib_file_selector_matches(file_selector: &str, dslx_file: &str) -> bool {
    let selector = file_selector.trim().replace('\\', "/");
    if selector.is_empty() {
        return false;
    }
    let file = dslx_file.replace('\\', "/");
    if selector == file {
        return true;
    }
    if let Some(basename) = Path::new(&file).file_name().and_then(|v| v.to_str())
        && selector == basename
    {
        return true;
    }
    if !selector.contains('/') {
        return file == format!("xls/dslx/stdlib/{selector}");
    }
    false
}

pub(crate) fn stdlib_fn_history_view_url(
    file_selector: &str,
    dslx_fn_name: &str,
    fraig: bool,
    delay_model: &str,
) -> String {
    let file_selector = file_selector.trim().replace('\\', "/");
    let file_component = Path::new(&file_selector)
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or(file_selector.as_str());
    let selector = format!("{}:{}", file_component.trim(), dslx_fn_name.trim());
    let mut url = format!("/dslx-fns/{}", path_component_escape(&selector));
    let mut first = true;
    if fraig {
        url.push(if first { '?' } else { '&' });
        url.push_str("fraig=true");
        first = false;
    }
    if !delay_model.trim().is_empty() && !delay_model.trim().eq_ignore_ascii_case("asap7") {
        url.push(if first { '?' } else { '&' });
        url.push_str("delay_model=");
        url.push_str(&query_component_escape(delay_model.trim()));
    }
    url
}

pub(crate) fn stdlib_fns_trend_view_url(
    kind: StdlibTrendKind,
    metric: StdlibMetric,
    fraig: bool,
    file: Option<&str>,
) -> String {
    let mut url = format!("{}?metric={}", kind.view_path(), metric.as_query_value());
    if kind.supports_fraig() {
        url.push_str("&fraig=");
        url.push_str(if fraig { "true" } else { "false" });
    }
    if let Some(file) = file
        && !file.is_empty()
    {
        url.push_str("&file=");
        url.push_str(&query_component_escape(file));
    }
    url
}

pub(crate) fn stdlib_g8r_vs_yosys_view_url(
    fraig: bool,
    max_ir_nodes: Option<u64>,
    crate_version: Option<&str>,
) -> String {
    let mut url = format!(
        "/dslx-fns-g8r-vs-yosys-abc/?fraig={}",
        if fraig { "true" } else { "false" }
    );
    if let Some(max_ir_nodes) = max_ir_nodes {
        url.push_str("&max_ir_nodes=");
        url.push_str(&max_ir_nodes.to_string());
    }
    if let Some(crate_version) = crate_version
        && !crate_version.is_empty()
    {
        url.push_str("&crate_version=");
        url.push_str(&query_component_escape(crate_version));
    }
    url
}

pub(crate) fn ir_fn_corpus_g8r_vs_yosys_view_url(
    max_ir_nodes: Option<u64>,
    crate_version: Option<&str>,
) -> String {
    let mut url = String::from("/ir-fn-corpus-g8r-vs-yosys-abc/");
    let mut first = true;
    if let Some(max_ir_nodes) = max_ir_nodes {
        url.push(if first { '?' } else { '&' });
        first = false;
        url.push_str("max_ir_nodes=");
        url.push_str(&max_ir_nodes.to_string());
    }
    if let Some(crate_version) = crate_version
        && !crate_version.is_empty()
    {
        url.push(if first { '?' } else { '&' });
        url.push_str("crate_version=");
        url.push_str(&query_component_escape(crate_version));
    }
    url
}

pub(crate) fn ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_view_url(
    max_ir_nodes: Option<u64>,
    crate_version: Option<&str>,
) -> String {
    let mut url = String::from("/ir-fn-g8r-abc-vs-codegen-yosys-abc/");
    let mut first = true;
    if let Some(max_ir_nodes) = max_ir_nodes {
        url.push(if first { '?' } else { '&' });
        first = false;
        url.push_str("max_ir_nodes=");
        url.push_str(&max_ir_nodes.to_string());
    }
    if let Some(crate_version) = crate_version
        && !crate_version.is_empty()
    {
        url.push(if first { '?' } else { '&' });
        url.push_str("crate_version=");
        url.push_str(&query_component_escape(crate_version));
    }
    url
}

pub(crate) fn ir_fn_corpus_k3_losses_vs_crate_version_view_url(
    max_ir_nodes: Option<u64>,
    crate_version: Option<&str>,
    show_same: bool,
) -> String {
    let mut url = String::from("/ir-fn-corpus-k3-losses-vs-crate-version/");
    let mut first = true;
    if let Some(max_ir_nodes) = max_ir_nodes {
        url.push(if first { '?' } else { '&' });
        first = false;
        url.push_str("max_ir_nodes=");
        url.push_str(&max_ir_nodes.to_string());
    }
    if let Some(crate_version) = crate_version
        && !crate_version.is_empty()
    {
        url.push(if first { '?' } else { '&' });
        first = false;
        url.push_str("crate_version=");
        url.push_str(&query_component_escape(crate_version));
    }
    if show_same {
        url.push(if first { '?' } else { '&' });
        url.push_str("show_same=true");
    }
    url
}

pub(crate) fn stdlib_file_action_graph_view_url(
    crate_version: Option<&str>,
    file: Option<&str>,
    fn_name: Option<&str>,
    action_id: Option<&str>,
    include_k3_descendants: bool,
) -> String {
    let mut url = String::from("/dslx-file-action-graph/");
    let mut first = true;
    if let Some(crate_version) = crate_version
        && !crate_version.is_empty()
    {
        url.push(if first { '?' } else { '&' });
        url.push_str("crate_version=");
        url.push_str(&query_component_escape(crate_version));
        first = false;
    }
    if let Some(file) = file
        && !file.is_empty()
    {
        url.push(if first { '?' } else { '&' });
        url.push_str("file=");
        url.push_str(&query_component_escape(file));
        first = false;
    }
    if let Some(fn_name) = fn_name
        && !fn_name.is_empty()
    {
        url.push(if first { '?' } else { '&' });
        url.push_str("fn_name=");
        url.push_str(&query_component_escape(fn_name));
        first = false;
    }
    if let Some(action_id) = action_id
        && !action_id.is_empty()
    {
        url.push(if first { '?' } else { '&' });
        url.push_str("action_id=");
        url.push_str(&query_component_escape(action_id));
        first = false;
    }
    if include_k3_descendants {
        url.push(if first { '?' } else { '&' });
        url.push_str("include_k3_descendants=true");
    }
    url
}

fn stdlib_g8r_vs_yosys_index_key(fraig: bool) -> &'static str {
    if fraig {
        WEB_STDLIB_G8R_VS_YOSYS_FRAIG_TRUE_INDEX_FILENAME
    } else {
        WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME
    }
}

pub(crate) fn load_stdlib_g8r_vs_yosys_dataset_index(
    store: &ArtifactStore,
    fraig: bool,
) -> Result<Option<StdlibG8rVsYosysDataset>> {
    let key = stdlib_g8r_vs_yosys_index_key(fraig);
    let location = store.web_index_location(key);
    let Some(bytes) = store
        .load_web_index_bytes(key)
        .with_context(|| format!("reading stdlib g8r-vs-yosys web index: {}", location))?
    else {
        return Ok(None);
    };
    let index_file: StdlibG8rVsYosysIndexFile = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing stdlib g8r-vs-yosys web index: {}", location))?;
    if index_file.schema_version != WEB_STDLIB_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION {
        info!(
            "query stdlib g8r-vs-yosys web index schema mismatch location={} expected={} got={}",
            location, WEB_STDLIB_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION, index_file.schema_version
        );
        return Ok(None);
    }
    if index_file.dataset.fraig != fraig {
        info!(
            "query stdlib g8r-vs-yosys web index payload mismatch location={} expected_fraig={} got_fraig={}",
            location, fraig, index_file.dataset.fraig
        );
        return Ok(None);
    }
    info!(
        "query stdlib g8r-vs-yosys web index hit location={} generated_utc={} fraig={} samples={} versions={}",
        location,
        index_file.generated_utc,
        fraig,
        index_file.dataset.samples.len(),
        index_file.dataset.available_crate_versions.len(),
    );
    Ok(Some(index_file.dataset))
}

fn write_stdlib_g8r_vs_yosys_dataset_index(
    store: &ArtifactStore,
    dataset: &StdlibG8rVsYosysDataset,
) -> Result<(String, u64, DateTime<Utc>)> {
    let key = stdlib_g8r_vs_yosys_index_key(dataset.fraig);
    let location = store.web_index_location(key);
    let generated_utc = Utc::now();
    let payload = StdlibG8rVsYosysIndexFile {
        schema_version: WEB_STDLIB_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION,
        generated_utc,
        dataset: dataset.clone(),
    };
    let bytes =
        serde_json::to_vec(&payload).context("serializing stdlib g8r-vs-yosys web index")?;
    store
        .write_web_index_bytes(key, &bytes)
        .with_context(|| format!("writing stdlib g8r-vs-yosys web index: {}", location))?;
    info!(
        "query stdlib g8r-vs-yosys web index write location={} fraig={} samples={} versions={} bytes={}",
        location,
        dataset.fraig,
        dataset.samples.len(),
        dataset.available_crate_versions.len(),
        bytes.len(),
    );
    Ok((location, bytes.len() as u64, generated_utc))
}

pub(crate) fn rebuild_stdlib_g8r_vs_yosys_dataset_index(
    store: &ArtifactStore,
    fraig: bool,
) -> Result<StdlibG8rVsYosysIndexSummary> {
    let started = Instant::now();
    let dataset = build_stdlib_g8r_vs_yosys_dataset(store, fraig)?;
    let sample_count = dataset.samples.len();
    let crate_versions = dataset.available_crate_versions.len();
    let (index_location, index_bytes, generated_utc) =
        write_stdlib_g8r_vs_yosys_dataset_index(store, &dataset)?;
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok(StdlibG8rVsYosysIndexSummary {
        fraig,
        generated_utc,
        index_path: index_location,
        sample_count,
        crate_versions,
        index_bytes,
        elapsed_ms,
    })
}

fn stdlib_fn_timeline_entry_key(dslx_file: &str, dslx_fn_name: &str) -> String {
    format!(
        "{}::{}",
        dslx_file.trim().replace('\\', "/"),
        dslx_fn_name.trim()
    )
}

fn build_stdlib_fn_timeline_index_file(store: &ArtifactStore) -> Result<StdlibFnTimelineIndexFile> {
    let started = Instant::now();
    let provenances = store.list_provenances_shared()?;
    let provenance_by_action_id = build_provenance_lookup(provenances.as_ref());

    let mut functions_by_file_set: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut entries_by_fn_key: BTreeMap<String, StdlibFnTimelineIndexEntry> = BTreeMap::new();
    let mut stats_rows = 0usize;
    let mut delay_rows = 0usize;

    for provenance in provenances.iter() {
        let ActionSpec::DriverDslxFnToIr {
            dslx_file,
            dslx_fn_name,
            ..
        } = &provenance.action
        else {
            continue;
        };
        let dslx_file = dslx_file.replace('\\', "/");
        functions_by_file_set
            .entry(dslx_file)
            .or_default()
            .insert(dslx_fn_name.clone());
    }

    for provenance in provenances.iter() {
        if let ActionSpec::DriverAigToStats {
            aig_action_id,
            version,
            ..
        } = &provenance.action
        {
            let stats_dso_version = normalize_tag_version(version).to_string();
            let mut source_ctx_and_kind: Option<(StdlibTrendSourceContext, Option<bool>)> = None;
            if let Some(source_ctx) = extract_stdlib_trend_source_context(
                &provenance_by_action_id,
                StdlibTrendKind::G8r,
                aig_action_id,
                false,
            ) {
                source_ctx_and_kind = Some((source_ctx, Some(false)));
            } else if let Some(source_ctx) = extract_stdlib_trend_source_context(
                &provenance_by_action_id,
                StdlibTrendKind::G8r,
                aig_action_id,
                true,
            ) {
                source_ctx_and_kind = Some((source_ctx, Some(true)));
            } else if let Some(source_ctx) = extract_stdlib_trend_source_context(
                &provenance_by_action_id,
                StdlibTrendKind::YosysAbc,
                aig_action_id,
                false,
            ) {
                source_ctx_and_kind = Some((source_ctx, None));
            }
            let Some((source_ctx, g8r_fraig)) = source_ctx_and_kind else {
                continue;
            };
            let Some((dslx_file, dslx_fn_name)) = resolve_direct_dslx_origin_from_opt_ir_action(
                &provenance_by_action_id,
                &source_ctx.ir_action_id,
            ) else {
                continue;
            };
            let dslx_file = dslx_file.replace('\\', "/");
            let key = stdlib_fn_timeline_entry_key(&dslx_file, &dslx_fn_name);
            let entry =
                entries_by_fn_key
                    .entry(key)
                    .or_insert_with(|| StdlibFnTimelineIndexEntry {
                        dslx_file: dslx_file.clone(),
                        dslx_fn_name: dslx_fn_name.clone(),
                        g8r_fraig_false_by_crate: BTreeMap::new(),
                        g8r_fraig_true_by_crate: BTreeMap::new(),
                        yosys_by_crate: BTreeMap::new(),
                        delay_by_model_and_crate: BTreeMap::new(),
                    });

            let stats_path = store.resolve_artifact_ref_path(&provenance.output_artifact);
            let stats_text = match fs::read_to_string(&stats_path) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let stats_json: serde_json::Value = match serde_json::from_str(&stats_text) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let Some(and_nodes) = parse_aig_stats_metric(&stats_json, "and_nodes")
                .or_else(|| parse_aig_stats_metric(&stats_json, "live_nodes"))
            else {
                continue;
            };
            let Some(depth) = parse_aig_stats_metric(&stats_json, "depth")
                .or_else(|| parse_aig_stats_metric(&stats_json, "deepest_path"))
            else {
                continue;
            };

            let point = StdlibFnTrendPoint {
                crate_version: source_ctx.crate_version.clone(),
                dso_version: if stats_dso_version.is_empty() {
                    source_ctx.dso_version
                } else {
                    stats_dso_version
                },
                and_nodes,
                depth,
                created_utc: provenance.created_utc,
                stats_action_id: provenance.action_id.clone(),
            };
            match g8r_fraig {
                Some(true) => {
                    upsert_stdlib_fn_trend_point_by_crate(
                        &mut entry.g8r_fraig_true_by_crate,
                        point,
                    );
                }
                Some(false) => {
                    upsert_stdlib_fn_trend_point_by_crate(
                        &mut entry.g8r_fraig_false_by_crate,
                        point,
                    );
                }
                None => {
                    upsert_stdlib_fn_trend_point_by_crate(&mut entry.yosys_by_crate, point);
                }
            }
            stats_rows += 1;
            continue;
        }

        let ActionSpec::DriverIrToDelayInfo {
            ir_action_id,
            delay_model,
            version,
            runtime,
            ..
        } = &provenance.action
        else {
            continue;
        };
        let Some((dslx_file, dslx_fn_name)) =
            resolve_direct_dslx_origin_from_opt_ir_action(&provenance_by_action_id, ir_action_id)
        else {
            continue;
        };
        let dslx_file = dslx_file.replace('\\', "/");
        let key = stdlib_fn_timeline_entry_key(&dslx_file, &dslx_fn_name);
        let entry = entries_by_fn_key
            .entry(key)
            .or_insert_with(|| StdlibFnTimelineIndexEntry {
                dslx_file: dslx_file.clone(),
                dslx_fn_name: dslx_fn_name.clone(),
                g8r_fraig_false_by_crate: BTreeMap::new(),
                g8r_fraig_true_by_crate: BTreeMap::new(),
                yosys_by_crate: BTreeMap::new(),
                delay_by_model_and_crate: BTreeMap::new(),
            });
        let delay_info_path = store.resolve_artifact_ref_path(&provenance.output_artifact);
        let delay_info_text = match fs::read_to_string(&delay_info_path) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(delay_ps) = parse_delay_info_total_delay_ps(&delay_info_text) else {
            continue;
        };
        let delay_model = delay_model.trim().to_ascii_lowercase();
        if delay_model.is_empty() {
            continue;
        }
        let point = StdlibFnDelayPoint {
            crate_version: normalize_tag_version(&runtime.driver_version).to_string(),
            dso_version: normalize_tag_version(version).to_string(),
            delay_ps,
            created_utc: provenance.created_utc,
            action_id: provenance.action_id.clone(),
        };
        upsert_delay_point_by_crate(
            entry
                .delay_by_model_and_crate
                .entry(delay_model)
                .or_default(),
            point,
        );
        delay_rows += 1;
    }

    let mut available_files: Vec<String> = functions_by_file_set.keys().cloned().collect();
    available_files.sort();
    let mut functions_by_file: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (dslx_file, functions) in functions_by_file_set {
        let mut functions: Vec<String> = functions.into_iter().collect();
        functions.sort();
        functions_by_file.insert(dslx_file, functions);
    }

    let index = StdlibFnTimelineIndexFile {
        schema_version: WEB_STDLIB_FN_TIMELINE_INDEX_SCHEMA_VERSION,
        generated_utc: Utc::now(),
        available_files,
        functions_by_file,
        entries_by_fn_key,
    };
    info!(
        "query build_stdlib_fn_timeline_index_file provenances={} files={} indexed_functions={} stats_rows={} delay_rows={} elapsed_ms={}",
        provenances.len(),
        index.available_files.len(),
        index.entries_by_fn_key.len(),
        stats_rows,
        delay_rows,
        started.elapsed().as_millis(),
    );
    Ok(index)
}

pub(crate) fn load_stdlib_fn_version_timeline_dataset_index(
    store: &ArtifactStore,
    file_selector: &str,
    dslx_fn_name: &str,
    fraig: bool,
    delay_model: &str,
) -> Result<Option<StdlibFnVersionTimelineDataset>> {
    let key = WEB_STDLIB_FN_TIMELINE_INDEX_FILENAME;
    let location = store.web_index_location(key);
    let Some(bytes) = store
        .load_web_index_bytes(key)
        .with_context(|| format!("reading stdlib fn timeline web index: {}", location))?
    else {
        return Ok(None);
    };
    let index_file: StdlibFnTimelineIndexFile = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing stdlib fn timeline web index: {}", location))?;
    if index_file.schema_version != WEB_STDLIB_FN_TIMELINE_INDEX_SCHEMA_VERSION {
        info!(
            "query stdlib fn timeline web index schema mismatch location={} expected={} got={}",
            location, WEB_STDLIB_FN_TIMELINE_INDEX_SCHEMA_VERSION, index_file.schema_version
        );
        return Ok(None);
    }

    let file_selector = file_selector.trim().replace('\\', "/");
    let dslx_fn_name = dslx_fn_name.trim().to_string();
    let delay_model = delay_model.trim().to_string();
    let delay_model_key = delay_model.to_ascii_lowercase();
    let mut matched_files: Vec<String> = index_file
        .available_files
        .iter()
        .filter(|dslx_file| stdlib_file_selector_matches(&file_selector, dslx_file))
        .cloned()
        .collect();
    matched_files.sort();
    let dslx_file = matched_files.first().cloned();
    let available_functions_for_file = dslx_file
        .as_deref()
        .and_then(|file| index_file.functions_by_file.get(file))
        .cloned()
        .unwrap_or_default();

    let mut g8r_by_crate: BTreeMap<String, StdlibFnTrendPoint> = BTreeMap::new();
    let mut yosys_by_crate: BTreeMap<String, StdlibFnTrendPoint> = BTreeMap::new();
    let mut delay_by_crate: BTreeMap<String, StdlibFnDelayPoint> = BTreeMap::new();
    for matched_file in &matched_files {
        let key = stdlib_fn_timeline_entry_key(matched_file, &dslx_fn_name);
        let Some(entry) = index_file.entries_by_fn_key.get(&key) else {
            continue;
        };
        let g8r_by_crate_for_fraig = if fraig {
            &entry.g8r_fraig_true_by_crate
        } else {
            &entry.g8r_fraig_false_by_crate
        };
        for point in g8r_by_crate_for_fraig.values() {
            upsert_stdlib_fn_trend_point_by_crate(&mut g8r_by_crate, point.clone());
        }
        for point in entry.yosys_by_crate.values() {
            upsert_stdlib_fn_trend_point_by_crate(&mut yosys_by_crate, point.clone());
        }
        if let Some(delay_for_model) = entry.delay_by_model_and_crate.get(&delay_model_key) {
            for point in delay_for_model.values() {
                upsert_delay_point_by_crate(&mut delay_by_crate, point.clone());
            }
        }
    }

    let mut crate_versions = BTreeSet::new();
    crate_versions.extend(g8r_by_crate.keys().cloned());
    crate_versions.extend(yosys_by_crate.keys().cloned());
    crate_versions.extend(delay_by_crate.keys().cloned());
    let mut crate_versions: Vec<String> = crate_versions.into_iter().collect();
    crate_versions.sort_by(|a, b| cmp_dotted_numeric_version(a, b));

    let mut points = Vec::new();
    for crate_version in &crate_versions {
        let g8r = g8r_by_crate.get(crate_version);
        let yosys = yosys_by_crate.get(crate_version);
        let delay = delay_by_crate.get(crate_version);
        points.push(StdlibFnVersionTimelinePoint {
            crate_version: crate_version.clone(),
            g8r_dso_version: g8r.map(|p| p.dso_version.clone()),
            yosys_abc_dso_version: yosys.map(|p| p.dso_version.clone()),
            ir_delay_dso_version: delay.map(|p| p.dso_version.clone()),
            g8r_and_nodes: g8r.map(|p| p.and_nodes),
            g8r_levels: g8r.map(|p| p.depth),
            g8r_stats_action_id: g8r.map(|p| p.stats_action_id.clone()),
            yosys_abc_and_nodes: yosys.map(|p| p.and_nodes),
            yosys_abc_levels: yosys.map(|p| p.depth),
            yosys_abc_stats_action_id: yosys.map(|p| p.stats_action_id.clone()),
            ir_delay_ps: delay.map(|p| p.delay_ps),
            ir_delay_action_id: delay.map(|p| p.action_id.clone()),
        });
    }

    let dataset = StdlibFnVersionTimelineDataset {
        file_selector,
        dslx_file,
        dslx_fn_name,
        fraig,
        delay_model,
        points,
        crate_versions,
        matched_files,
        available_files: index_file.available_files,
        available_functions_for_file,
    };
    info!(
        "query stdlib fn timeline web index hit location={} generated_utc={} file_selector={} fn={} fraig={} delay_model={} points={} matched_files={}",
        location,
        index_file.generated_utc,
        dataset.file_selector,
        dataset.dslx_fn_name,
        dataset.fraig,
        dataset.delay_model,
        dataset.points.len(),
        dataset.matched_files.len(),
    );
    Ok(Some(dataset))
}

fn write_stdlib_fn_version_timeline_dataset_index(
    store: &ArtifactStore,
    index_file: &StdlibFnTimelineIndexFile,
) -> Result<(String, u64, DateTime<Utc>)> {
    let key = WEB_STDLIB_FN_TIMELINE_INDEX_FILENAME;
    let location = store.web_index_location(key);
    let mut payload = index_file.clone();
    payload.generated_utc = Utc::now();
    payload.schema_version = WEB_STDLIB_FN_TIMELINE_INDEX_SCHEMA_VERSION;
    let bytes = serde_json::to_vec(&payload).context("serializing stdlib fn timeline web index")?;
    store
        .write_web_index_bytes(key, &bytes)
        .with_context(|| format!("writing stdlib fn timeline web index: {}", location))?;
    info!(
        "query stdlib fn timeline web index write location={} files={} indexed_functions={} bytes={}",
        location,
        payload.available_files.len(),
        payload.entries_by_fn_key.len(),
        bytes.len(),
    );
    Ok((location, bytes.len() as u64, payload.generated_utc))
}

pub(crate) fn rebuild_stdlib_fn_version_timeline_dataset_index(
    store: &ArtifactStore,
) -> Result<StdlibFnTimelineIndexSummary> {
    let started = Instant::now();
    let index_file = build_stdlib_fn_timeline_index_file(store)?;
    let file_count = index_file.available_files.len();
    let fn_count = index_file.entries_by_fn_key.len();
    let (index_location, index_bytes, generated_utc) =
        write_stdlib_fn_version_timeline_dataset_index(store, &index_file)?;
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok(StdlibFnTimelineIndexSummary {
        generated_utc,
        index_path: index_location,
        file_count,
        fn_count,
        index_bytes,
        elapsed_ms,
    })
}

pub(crate) fn load_stdlib_fns_trend_dataset_index(
    store: &ArtifactStore,
    kind: StdlibTrendKind,
    fraig: bool,
    selected_file: Option<&str>,
) -> Result<Option<StdlibFnsTrendDataset>> {
    let fraig = kind.supports_fraig() && fraig;
    let key = stdlib_fns_trend_index_key(kind, fraig);
    let location = store.web_index_location(key);
    let Some(bytes) = store
        .load_web_index_bytes(key)
        .with_context(|| format!("reading stdlib trend web index: {}", location))?
    else {
        return Ok(None);
    };
    let index_file: StdlibFnsTrendIndexFile = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing stdlib trend web index: {}", location))?;
    if index_file.schema_version != WEB_STDLIB_FNS_TREND_INDEX_SCHEMA_VERSION {
        info!(
            "query stdlib trend web index schema mismatch location={} expected={} got={}",
            location, WEB_STDLIB_FNS_TREND_INDEX_SCHEMA_VERSION, index_file.schema_version
        );
        return Ok(None);
    }
    if index_file.dataset.kind != kind || index_file.dataset.fraig != fraig {
        info!(
            "query stdlib trend web index payload mismatch location={} expected_kind={} expected_fraig={} got_kind={} got_fraig={}",
            location,
            kind.view_path(),
            fraig,
            index_file.dataset.kind.view_path(),
            index_file.dataset.fraig,
        );
        return Ok(None);
    }
    let filtered =
        apply_selected_file_to_stdlib_fns_trend_dataset(&index_file.dataset, selected_file);
    info!(
        "query stdlib trend web index hit location={} generated_utc={} kind={} fraig={} selected_file={} series={} points={}",
        location,
        index_file.generated_utc,
        kind.view_path(),
        fraig,
        filtered.selected_file.as_deref().unwrap_or("<all>"),
        filtered.series.len(),
        filtered.total_points
    );
    Ok(Some(filtered))
}

fn write_stdlib_fns_trend_dataset_index(
    store: &ArtifactStore,
    dataset: &StdlibFnsTrendDataset,
) -> Result<(String, u64, DateTime<Utc>)> {
    let key = stdlib_fns_trend_index_key(dataset.kind, dataset.fraig);
    let location = store.web_index_location(key);
    let generated_utc = Utc::now();
    let payload = StdlibFnsTrendIndexFile {
        schema_version: WEB_STDLIB_FNS_TREND_INDEX_SCHEMA_VERSION,
        generated_utc,
        dataset: dataset.clone(),
    };
    let bytes = serde_json::to_vec(&payload).context("serializing stdlib trend web index")?;
    store
        .write_web_index_bytes(key, &bytes)
        .with_context(|| format!("writing stdlib trend web index: {}", location))?;
    info!(
        "query stdlib trend web index write location={} kind={} fraig={} series={} points={} bytes={}",
        location,
        dataset.kind.view_path(),
        dataset.fraig,
        dataset.series.len(),
        dataset.total_points,
        bytes.len(),
    );
    Ok((location, bytes.len() as u64, generated_utc))
}

pub(crate) fn rebuild_stdlib_fns_trend_dataset_index(
    store: &ArtifactStore,
    kind: StdlibTrendKind,
    fraig: bool,
) -> Result<StdlibFnsTrendIndexSummary> {
    let fraig = kind.supports_fraig() && fraig;
    let started = Instant::now();
    let dataset = build_stdlib_fns_trend_dataset(store, kind, fraig, None)?;
    let (index_location, index_bytes, generated_utc) =
        write_stdlib_fns_trend_dataset_index(store, &dataset)?;
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok(StdlibFnsTrendIndexSummary {
        kind_path: kind.view_path().to_string(),
        fraig,
        generated_utc,
        index_path: index_location,
        series_count: dataset.series.len(),
        point_count: dataset.total_points,
        file_count: dataset.available_files.len(),
        index_bytes,
        elapsed_ms,
    })
}

pub(crate) fn load_versions_cards_index(
    store: &ArtifactStore,
) -> Result<Option<VersionCardsReport>> {
    let key = WEB_VERSIONS_SUMMARY_INDEX_FILENAME;
    let location = store.web_index_location(key);
    let Some(bytes) = store
        .load_web_index_bytes(key)
        .with_context(|| format!("reading versions summary web index: {}", location))?
    else {
        return Ok(None);
    };
    let index_file: VersionsSummaryIndexFile = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing versions summary web index: {}", location))?;
    if index_file.schema_version != WEB_VERSIONS_SUMMARY_INDEX_SCHEMA_VERSION {
        info!(
            "query versions summary web index schema mismatch location={} expected={} got={}",
            location, WEB_VERSIONS_SUMMARY_INDEX_SCHEMA_VERSION, index_file.schema_version
        );
        return Ok(None);
    }
    info!(
        "query versions summary web index hit location={} generated_utc={} cards={} unattributed={}",
        location,
        index_file.generated_utc,
        index_file.report.cards.len(),
        index_file.report.unattributed_actions.len()
    );
    Ok(Some(index_file.report))
}

fn write_versions_cards_index(
    store: &ArtifactStore,
    report: &VersionCardsReport,
) -> Result<(String, u64, DateTime<Utc>)> {
    let key = WEB_VERSIONS_SUMMARY_INDEX_FILENAME;
    let location = store.web_index_location(key);
    let generated_utc = Utc::now();
    let payload = VersionsSummaryIndexFile {
        schema_version: WEB_VERSIONS_SUMMARY_INDEX_SCHEMA_VERSION,
        generated_utc,
        report: report.clone(),
    };
    let bytes = serde_json::to_vec(&payload).context("serializing versions summary web index")?;
    store
        .write_web_index_bytes(key, &bytes)
        .with_context(|| format!("writing versions summary web index: {}", location))?;
    info!(
        "query versions summary web index write location={} cards={} unattributed={} bytes={}",
        location,
        report.cards.len(),
        report.unattributed_actions.len(),
        bytes.len(),
    );
    Ok((location, bytes.len() as u64, generated_utc))
}

pub(crate) fn rebuild_versions_cards_index(
    store: &ArtifactStore,
    repo_root: &Path,
) -> Result<VersionsSummaryIndexSummary> {
    let started = Instant::now();
    let report = build_versions_cards(store, repo_root)?;
    let (index_location, index_bytes, generated_utc) = write_versions_cards_index(store, &report)?;
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok(VersionsSummaryIndexSummary {
        generated_utc,
        index_path: index_location,
        card_count: report.cards.len(),
        unattributed_actions: report.unattributed_actions.len(),
        index_bytes,
        elapsed_ms,
    })
}

pub(crate) fn build_versions_cards(
    store: &ArtifactStore,
    repo_root: &Path,
) -> Result<VersionCardsReport> {
    let started = Instant::now();
    let compat = load_version_compat_map(repo_root)?;
    let compat_by_dso = load_compat_by_dso(repo_root);
    let crate_release_utc_by_crate = load_crate_release_datetime_utc_map(repo_root);
    let mut versions: BTreeMap<String, VersionAggregate> = BTreeMap::new();
    let mut unattributed_actions: Vec<UnattributedActionView> = Vec::new();

    let provenances = store.list_provenances_shared()?;
    let provenance_by_action_id: BTreeMap<&str, &Provenance> = provenances
        .iter()
        .map(|provenance| (provenance.action_id.as_str(), provenance))
        .collect();
    let mut crate_version_inference_cache: BTreeMap<String, Option<String>> = BTreeMap::new();
    for provenance in provenances.iter() {
        let inferred = infer_crate_version_for_provenance_with_lookup(
            provenance,
            &compat_by_dso,
            &provenance_by_action_id,
            &mut crate_version_inference_cache,
        );
        let Some(crate_version) = inferred else {
            unattributed_actions.push(UnattributedActionView {
                source: UnattributedActionSource::Materialized,
                action_id: provenance.action_id.clone(),
                action_kind: action_kind_label(&provenance.action).to_string(),
                dso_version: action_dso_version(&provenance.action)
                    .map(|v| normalize_tag_version(v).to_string()),
                reason: classify_crate_version_inference_miss(
                    store,
                    &provenance.action,
                    &compat_by_dso,
                ),
            });
            continue;
        };
        let agg = versions.entry(crate_version).or_default();
        agg.total_materialized += 1;
        if let Some(dso_version) = action_dso_version(&provenance.action) {
            agg.dso_versions.insert(dso_version.to_string());
        }
    }

    for failed in load_failed_queue_records(store)? {
        let inferred = infer_crate_version_for_action_with_lookup(
            &failed.action,
            &compat_by_dso,
            &provenance_by_action_id,
            &mut crate_version_inference_cache,
        );
        let Some(crate_version) = inferred else {
            unattributed_actions.push(UnattributedActionView {
                source: UnattributedActionSource::Failed,
                action_id: failed.action_id.clone(),
                action_kind: action_kind_label(&failed.action).to_string(),
                dso_version: action_dso_version(&failed.action)
                    .map(|v| normalize_tag_version(v).to_string()),
                reason: classify_crate_version_inference_miss(
                    store,
                    &failed.action,
                    &compat_by_dso,
                ),
            });
            continue;
        };
        let agg = versions.entry(crate_version).or_default();
        agg.failed_total += 1;
        if let Some(dso_version) = action_dso_version(&failed.action) {
            agg.dso_versions.insert(dso_version.to_string());
        }
        let is_timeout = is_timeout_error(&failed.error);
        let kind = action_kind_label(&failed.action).to_string();
        let failed_kind = agg.failed_by_kind.entry(kind.clone()).or_default();
        failed_kind.count += 1;
        if is_timeout {
            failed_kind.timeout_count += 1;
        }
        agg.failures.push(FailedActionRowView {
            action_id: failed.action_id,
            failed_utc: failed.failed_utc,
            action_kind: kind,
            dso_version: action_dso_version(&failed.action).map(|v| v.to_string()),
            subject: action_subject(&failed.action),
            error_summary: summarize_error(&failed.error),
        });
    }

    let mut cards: Vec<VersionCardView> = versions
        .into_iter()
        .map(|(crate_version, mut agg)| {
            let mut failed_by_kind: Vec<FailedKindView> = agg
                .failed_by_kind
                .into_iter()
                .map(|(kind, counts)| FailedKindView {
                    kind,
                    count: counts.count,
                    timeout_count: counts.timeout_count,
                })
                .collect();
            failed_by_kind.sort_by(|a, b| b.count.cmp(&a.count).then(a.kind.cmp(&b.kind)));

            let mut dso_versions: Vec<String> = agg.dso_versions.into_iter().collect();
            dso_versions.sort_by(|a, b| {
                cmp_dotted_numeric_version(normalize_tag_version(a), normalize_tag_version(b))
                    .reverse()
            });

            agg.failures.sort_by(|a, b| {
                b.failed_utc
                    .cmp(&a.failed_utc)
                    .then(a.action_id.cmp(&b.action_id))
            });
            let crate_release_datetime = compat
                .get(&crate_version)
                .map(|entry| entry.crate_release_datetime.clone());

            VersionCardView {
                stdlib_enumeration: build_stdlib_enumeration_status(
                    store,
                    repo_root,
                    &crate_version,
                    &compat,
                ),
                crate_version,
                crate_release_datetime,
                total_materialized: agg.total_materialized,
                failed_total: agg.failed_total,
                dso_versions,
                failed_by_kind,
                failures: agg.failures,
            }
        })
        .collect();

    cards.sort_by(|a, b| {
        cmp_crate_versions_by_release_datetime(
            &a.crate_version,
            &b.crate_version,
            &crate_release_utc_by_crate,
        )
    });

    unattributed_actions.sort_by(|a, b| {
        a.action_id
            .cmp(&b.action_id)
            .then(a.source.as_label().cmp(b.source.as_label()))
    });
    let invariant_miss_count = unattributed_actions
        .iter()
        .filter(|row| row.reason.is_invariant_failure())
        .count();
    if !unattributed_actions.is_empty() {
        eprintln!(
            "warning: suppressed {} unattributed action(s) from /versions/ cards (invariant_failures={})",
            unattributed_actions.len(),
            invariant_miss_count
        );
    }

    let report = VersionCardsReport {
        cards,
        unattributed_actions,
    };
    info!(
        "query build_versions_cards cards={} unattributed={} elapsed_ms={}",
        report.cards.len(),
        report.unattributed_actions.len(),
        started.elapsed().as_millis(),
    );
    Ok(report)
}

pub(crate) fn build_stdlib_enumeration_status(
    store: &ArtifactStore,
    repo_root: &Path,
    crate_version: &str,
    compat: &BTreeMap<String, VersionCompatEntry>,
) -> StdlibEnumerationStatusView {
    let Some(compat_entry) = compat.get(crate_version) else {
        return StdlibEnumerationStatusView {
            badge_class: "enum-unknown".to_string(),
            badge_label: "unknown".to_string(),
            summary: "crate missing in compatibility map".to_string(),
        };
    };

    let dso_version = if compat_entry.xlsynth_release_version.starts_with('v') {
        compat_entry.xlsynth_release_version.clone()
    } else {
        format!("v{}", compat_entry.xlsynth_release_version)
    };

    let runtime =
        match explicit_driver_runtime_for_crate_version(repo_root, crate_version, &dso_version) {
            Ok(runtime) => runtime,
            Err(err) => {
                return StdlibEnumerationStatusView {
                    badge_class: "enum-unknown".to_string(),
                    badge_label: "unknown".to_string(),
                    summary: summarize_error(&format!("runtime setup failed: {:#}", err)),
                };
            }
        };
    let root_action = ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
        version: dso_version.clone(),
        discovery_runtime: Some(runtime),
    };
    let root_action_id = match compute_action_id(&root_action) {
        Ok(action_id) => action_id,
        Err(err) => {
            return StdlibEnumerationStatusView {
                badge_class: "enum-unknown".to_string(),
                badge_label: "unknown".to_string(),
                summary: summarize_error(&format!("failed computing canonical root id: {:#}", err)),
            };
        }
    };
    if !store.action_exists(&root_action_id) {
        return StdlibEnumerationStatusView {
            badge_class: "enum-missing".to_string(),
            badge_label: "not run".to_string(),
            summary: format!(
                "canonical root stdlib action not materialized for dso:{}",
                normalize_tag_version(&dso_version)
            ),
        };
    }

    let provenance = match store.load_provenance(&root_action_id) {
        Ok(p) => p,
        Err(err) => {
            return StdlibEnumerationStatusView {
                badge_class: "enum-unknown".to_string(),
                badge_label: "unknown".to_string(),
                summary: summarize_error(&format!("failed loading root provenance: {:#}", err)),
            };
        }
    };

    stdlib_enumeration_status_from_provenance(&provenance)
}

pub(crate) fn stdlib_enumeration_status_from_provenance(
    provenance: &Provenance,
) -> StdlibEnumerationStatusView {
    let details = provenance.details.as_object();

    if let Some(error_value) = details.and_then(|d| d.get("dslx_list_fns_discovery_error")) {
        return StdlibEnumerationStatusView {
            badge_class: "enum-failed".to_string(),
            badge_label: "failed".to_string(),
            summary: summarize_error(&format!(
                "enumeration error: {}",
                json_value_compact(error_value)
            )),
        };
    }

    let discovery = details
        .and_then(|d| d.get("dslx_list_fns_discovery"))
        .and_then(|v| v.as_object());
    let Some(discovery) = discovery else {
        if !provenance.suggested_next_actions.is_empty() {
            return StdlibEnumerationStatusView {
                badge_class: "enum-partial".to_string(),
                badge_label: "partial".to_string(),
                summary: format!(
                    "suggestions={} (discovery metadata missing)",
                    provenance.suggested_next_actions.len()
                ),
            };
        }
        return StdlibEnumerationStatusView {
            badge_class: "enum-failed".to_string(),
            badge_label: "failed".to_string(),
            summary: "no discovery metadata and no suggested actions".to_string(),
        };
    };

    let scanned_files = discovery
        .get("scanned_dslx_files")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let failed_files = discovery
        .get("failed_dslx_files_count")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let concrete_functions = discovery
        .get("concrete_functions")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let suggested_actions = discovery
        .get("suggested_actions")
        .and_then(|v| v.as_u64())
        .unwrap_or(provenance.suggested_next_actions.len() as u64);
    let summary = format!(
        "concrete={} suggested={} failed_files={}/{}",
        concrete_functions, suggested_actions, failed_files, scanned_files
    );

    let all_files_failed = scanned_files > 0 && failed_files >= scanned_files;
    let no_concrete_outputs = concrete_functions == 0 && suggested_actions == 0;
    if all_files_failed || (failed_files > 0 && no_concrete_outputs) {
        return StdlibEnumerationStatusView {
            badge_class: "enum-failed".to_string(),
            badge_label: "failed".to_string(),
            summary,
        };
    }
    if failed_files > 0 || no_concrete_outputs {
        return StdlibEnumerationStatusView {
            badge_class: "enum-partial".to_string(),
            badge_label: "partial".to_string(),
            summary,
        };
    }
    StdlibEnumerationStatusView {
        badge_class: "enum-ok".to_string(),
        badge_label: "ok".to_string(),
        summary,
    }
}

pub(crate) fn json_value_compact(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        _ => serde_json::to_string(value)
            .unwrap_or_else(|_| "<json serialization error>".to_string()),
    }
}

pub(crate) fn build_unprocessed_version_rows(
    store: &ArtifactStore,
    repo_root: &Path,
    cards: &[VersionCardView],
) -> Result<Vec<UnprocessedVersionRowView>> {
    let compat = load_version_compat_map(repo_root)?;
    let compat_by_dso = load_compat_by_dso(repo_root);

    let mut materialized_by_crate: BTreeMap<String, usize> = BTreeMap::new();
    for card in cards {
        materialized_by_crate.insert(card.crate_version.clone(), card.total_materialized);
    }

    let mut active_queue_by_crate: BTreeMap<String, usize> = BTreeMap::new();
    for queue_dir in [store.queue_pending_dir(), store.queue_running_dir()] {
        for path in list_queue_files(&queue_dir)? {
            let text = match fs::read_to_string(&path) {
                Ok(text) => text,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("reading queue item: {}", path.display()));
                }
            };
            let (_, _, _, action) = match parse_queue_work_item(&text, &path) {
                Ok(item) => item,
                Err(err) => {
                    eprintln!(
                        "warning: skipping malformed queue item {} while building unprocessed versions: {:#}",
                        path.display(),
                        err
                    );
                    continue;
                }
            };
            let crate_version = infer_crate_version_for_action(store, &action, &compat_by_dso)
                .unwrap_or_else(|| "unknown".to_string());
            *active_queue_by_crate.entry(crate_version).or_insert(0) += 1;
        }
    }

    let mut rows = Vec::new();
    for (crate_version, entry) in compat {
        let dso = if entry.xlsynth_release_version.starts_with('v') {
            entry.xlsynth_release_version
        } else {
            format!("v{}", entry.xlsynth_release_version)
        };
        let materialized_actions = *materialized_by_crate.get(&crate_version).unwrap_or(&0);
        if materialized_actions > 0 {
            continue;
        }

        let runtime = explicit_driver_runtime_for_crate_version(repo_root, &crate_version, &dso)?;
        let root_action = ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
            version: dso.clone(),
            discovery_runtime: Some(runtime),
        };
        let root_action_id = compute_action_id(&root_action)?;
        let root_queue_state = queue_state_for_action(store, &root_action_id);
        let active_queue_actions = *active_queue_by_crate.get(&crate_version).unwrap_or(&0);
        rows.push(UnprocessedVersionRowView {
            crate_version,
            crate_release_datetime: entry.crate_release_datetime,
            dso_version: normalize_tag_version(&dso).to_string(),
            materialized_actions,
            active_queue_actions,
            root_queue_state_key: queue_state_key(&root_queue_state).to_string(),
            root_queue_state_label: queue_state_display_label(&root_queue_state).to_string(),
        });
    }

    rows.sort_by(|a, b| {
        let a_release_utc = parse_compat_release_datetime_utc(&a.crate_release_datetime);
        let b_release_utc = parse_compat_release_datetime_utc(&b.crate_release_datetime);
        match (a_release_utc, b_release_utc) {
            (Some(a_dt), Some(b_dt)) => b_dt
                .cmp(&a_dt)
                .then(cmp_dotted_numeric_version(&a.crate_version, &b.crate_version).reverse()),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => {
                cmp_dotted_numeric_version(&a.crate_version, &b.crate_version).reverse()
            }
        }
    });
    Ok(rows)
}

pub(crate) fn build_queue_live_status(
    store: &ArtifactStore,
    repo_root: &Path,
    runner_owner_prefix: Option<&str>,
) -> Result<QueueLiveStatusView> {
    let pending_paths = list_queue_files(&store.queue_pending_dir())?;
    let pending = pending_paths.len();
    let mut pending_expanders = 0_usize;
    for path in pending_paths {
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("reading pending queue record: {}", path.display()));
            }
        };
        let action = match parse_queue_work_item(&text, &path) {
            Ok((_action_id, _enqueued_utc, _priority, action)) => action,
            Err(err) => {
                eprintln!(
                    "warning: skipping malformed pending queue record {} while building queue live status: {:#}",
                    path.display(),
                    err
                );
                continue;
            }
        };
        if action_is_expander(&action) {
            pending_expanders += 1;
        }
    }
    let pending_non_expanders = pending.saturating_sub(pending_expanders);
    let failed = load_failed_queue_records(store)?.len();
    let canceled = list_queue_files(&store.queue_canceled_dir())?.len();
    let done = list_queue_files(&store.queue_done_dir())?.len();

    let compat_by_dso = load_compat_by_dso(repo_root);
    let mut running_actions = Vec::new();
    let mut running_owned = 0_usize;
    let mut running_expanders = 0_usize;
    let runner_owner_prefix = runner_owner_prefix
        .map(str::trim)
        .filter(|prefix| !prefix.is_empty());
    for path in list_queue_files(&store.queue_running_dir())? {
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("reading running queue record: {}", path.display()));
            }
        };
        let running: QueueRunning = match serde_json::from_str(&text)
            .with_context(|| format!("parsing running queue record: {}", path.display()))
        {
            Ok(record) => record,
            Err(err) => {
                eprintln!(
                    "warning: skipping malformed running queue record {} while building queue live status: {:#}",
                    path.display(),
                    err
                );
                continue;
            }
        };
        let crate_version = infer_crate_version_for_action(store, &running.action, &compat_by_dso)
            .unwrap_or_else(|| "unknown".to_string());
        let owned_by_this_runner = runner_owner_prefix
            .map(|prefix| running.lease_owner.starts_with(prefix))
            .unwrap_or(false);
        if owned_by_this_runner {
            running_owned += 1;
        }
        if action_is_expander(&running.action) {
            running_expanders += 1;
        }
        running_actions.push(RunningActionLiveView {
            action_id: running.action_id,
            action_kind: action_kind_label(&running.action).to_string(),
            subject: action_subject(&running.action),
            crate_version,
            dso_version: action_dso_version(&running.action)
                .map(|v| normalize_tag_version(v).to_string()),
            lease_owner: running.lease_owner,
            owned_by_this_runner,
            lease_expires_utc: running.lease_expires_utc,
        });
    }
    running_actions.sort_by(|a, b| {
        a.lease_expires_utc
            .cmp(&b.lease_expires_utc)
            .then(a.action_id.cmp(&b.action_id))
    });

    let running = running_actions.len();
    let running_non_expanders = running.saturating_sub(running_expanders);
    let running_foreign = running.saturating_sub(running_owned);
    Ok(QueueLiveStatusView {
        updated_utc: Utc::now(),
        pending,
        pending_expanders,
        pending_non_expanders,
        pending_is_lower_bound: pending_expanders > 0 || running_expanders > 0,
        running,
        running_expanders,
        running_non_expanders,
        running_owned,
        running_foreign,
        runner_enabled: false,
        runner_paused: false,
        runner_drained: false,
        runner_sync_pending: false,
        runner_last_sync_utc: None,
        runner_last_sync_error: None,
        failed,
        canceled,
        done,
        running_actions,
    })
}

pub(crate) fn canonical_root_actions_for_crate_version(
    repo_root: &Path,
    crate_version: &str,
    dso_version: &str,
) -> Result<Vec<ActionSpec>> {
    let runtime = explicit_driver_runtime_for_crate_version(repo_root, crate_version, dso_version)?;
    let mut roots = Vec::with_capacity(1 + MODULE_SUBTREE_ROOT_PATHS.len());
    roots.push(ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
        version: dso_version.to_string(),
        discovery_runtime: Some(runtime.clone()),
    });
    for subtree in MODULE_SUBTREE_ROOT_PATHS {
        roots.push(ActionSpec::DownloadAndExtractXlsynthSourceSubtree {
            version: dso_version.to_string(),
            subtree: normalize_subtree_path(subtree)?,
            discovery_runtime: Some(runtime.clone()),
        });
    }
    Ok(roots)
}

pub(crate) fn enqueue_processing_for_crate_version(
    store: &ArtifactStore,
    repo_root: &Path,
    requested_crate_version: &str,
    priority: i32,
) -> Result<()> {
    let compat = load_version_compat_map(repo_root)?;
    let crate_version = normalize_tag_version(requested_crate_version).to_string();
    let entry = compat.get(&crate_version).ok_or_else(|| {
        anyhow!(
            "crate version `v{}` not found in {}",
            crate_version,
            VERSION_COMPAT_PATH
        )
    })?;
    let dso = if entry.xlsynth_release_version.starts_with('v') {
        entry.xlsynth_release_version.clone()
    } else {
        format!("v{}", entry.xlsynth_release_version)
    };

    let roots = canonical_root_actions_for_crate_version(repo_root, &crate_version, &dso)?;
    for root_action in roots {
        let root_action_id = compute_action_id(&root_action)?;
        store.delete_failed_action_record(&root_action_id)?;
        for terminal_path in [store.canceled_queue_path(&root_action_id)] {
            if terminal_path.exists() {
                fs::remove_file(&terminal_path).with_context(|| {
                    format!(
                        "removing terminal queue record for retry: {}",
                        terminal_path.display()
                    )
                })?;
            }
        }

        enqueue_action_with_priority(store, root_action.clone(), priority)?;
        if store.action_exists(&root_action_id) {
            maybe_refresh_dslx_root_for_suggestion_discovery(store, repo_root, &root_action_id)?;
            let _ = app::enqueue_suggested_actions(
                store,
                repo_root,
                &root_action_id,
                true,
                DEFAULT_SUGGESTED_ENQUEUE_MAX_DEPTH,
                priority,
            )?;
        }
    }
    Ok(())
}

pub(crate) fn maybe_refresh_dslx_root_for_suggestion_discovery(
    store: &ArtifactStore,
    repo_root: &Path,
    root_action_id: &str,
) -> Result<()> {
    let mut provenance = store.load_provenance(root_action_id)?;
    if !dslx_root_provenance_needs_discovery_refresh(&provenance) {
        ensure_dslx_root_has_suggested_actions(&provenance)?;
        return Ok(());
    }

    eprintln!(
        "refreshing dslx discovery suggestions for root action {}",
        root_action_id
    );

    let (dso_version, discovery_runtime) = match &provenance.action {
        ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
            version,
            discovery_runtime,
        }
        | ActionSpec::DownloadAndExtractXlsynthSourceSubtree {
            version,
            discovery_runtime,
            ..
        } => (version.clone(), discovery_runtime.clone()),
        _ => {
            return Ok(());
        }
    };
    let payload_dir = store.resolve_artifact_ref_path(&provenance.output_artifact);
    if !payload_dir.is_dir() {
        bail!(
            "cannot refresh dslx discovery suggestions; payload directory missing: {}",
            payload_dir.display()
        );
    }

    let discovery = discover_dslx_fn_to_ir_suggestions(
        store,
        repo_root,
        root_action_id,
        &payload_dir,
        &dso_version,
        discovery_runtime.as_ref(),
    )?;
    provenance.commands = discovery.commands;
    provenance.suggested_next_actions = discovery.suggested_next_actions;
    provenance.created_utc = Utc::now();
    let mut details = provenance.details.as_object().cloned().unwrap_or_default();
    details.remove("dslx_list_fns_discovery_error");
    details.insert(
        "dslx_list_fns_discovery".to_string(),
        json!({
            "driver_runtime": discovery.source_runtime,
            "enumeration_runtime": discovery.discovery_runtime,
            "enumeration_runtime_xlsynth_version": discovery.discovery_runtime_xlsynth_version,
            "enumeration_runtime_overrides_source": !same_driver_runtime(&discovery.source_runtime, &discovery.discovery_runtime),
            "crate_version_label": version_label("crate", &discovery.source_runtime.driver_version),
            "dso_version_label": version_label("dso", &dso_version),
            "enumeration_runtime_dso_version_label": version_label("dso", &discovery.discovery_runtime_xlsynth_version),
            "dslx_path": discovery.import_context.dslx_path,
            "dslx_stdlib_path": discovery.import_context.dslx_stdlib_path,
            "stdlib_source": discovery.import_context.stdlib_source,
            "scanned_dslx_files": discovery.scanned_dslx_files,
            "listed_functions": discovery.listed_functions,
            "concrete_functions": discovery.concrete_functions,
            "failed_dslx_files_count": discovery.failed_dslx_files.len(),
            "failed_dslx_files": discovery.failed_dslx_files,
            "suggested_actions": provenance.suggested_next_actions.len(),
        }),
    );
    provenance.details = serde_json::Value::Object(details);
    let provenance_path = store.provenance_path(root_action_id);
    fs::write(
        &provenance_path,
        serde_json::to_string_pretty(&provenance).context("serializing refreshed provenance")?,
    )
    .with_context(|| {
        format!(
            "writing refreshed provenance: {}",
            provenance_path.display()
        )
    })?;

    provenance = store.load_provenance(root_action_id)?;
    if dslx_root_provenance_needs_discovery_refresh(&provenance) {
        let detail = provenance
            .details
            .as_object()
            .and_then(|o| o.get("dslx_list_fns_discovery_error"))
            .cloned()
            .unwrap_or_else(|| json!("dslx discovery did not produce suggestions"));
        bail!(
            "dslx discovery suggestions still missing after refresh for action {}: {}",
            root_action_id,
            detail
        );
    }
    ensure_dslx_root_has_suggested_actions(&provenance)?;
    Ok(())
}

pub(crate) fn dslx_root_provenance_needs_discovery_refresh(provenance: &Provenance) -> bool {
    if !matches!(
        provenance.action,
        ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { .. }
            | ActionSpec::DownloadAndExtractXlsynthSourceSubtree { .. }
    ) {
        return false;
    }
    if provenance.suggested_next_actions.is_empty() {
        return true;
    }
    false
}

pub(crate) fn ensure_dslx_root_has_suggested_actions(provenance: &Provenance) -> Result<()> {
    if !matches!(
        provenance.action,
        ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { .. }
            | ActionSpec::DownloadAndExtractXlsynthSourceSubtree { .. }
    ) {
        return Ok(());
    }
    if !provenance.suggested_next_actions.is_empty() {
        return Ok(());
    }
    let detail = provenance
        .details
        .as_object()
        .and_then(|o| o.get("dslx_list_fns_discovery").cloned())
        .or_else(|| {
            provenance
                .details
                .as_object()
                .and_then(|o| o.get("dslx_list_fns_discovery_error").cloned())
        })
        .unwrap_or_else(|| json!("dslx discovery produced zero suggestions"));
    bail!(
        "dslx discovery produced zero suggested actions for action {}: {}",
        provenance.action_id,
        detail
    );
}

pub(crate) fn load_action_detail_records(
    store: &ArtifactStore,
    action_id: &str,
) -> Result<ActionDetailRecords> {
    let provenance = if store.action_exists(action_id) {
        Some(store.load_provenance(action_id)?)
    } else {
        None
    };
    Ok(ActionDetailRecords {
        provenance,
        pending: load_queue_pending_record(store, action_id)?,
        running: load_queue_running_record(store, action_id)?,
        done: load_queue_done_record(store, action_id)?,
        failed: load_queue_failed_record(store, action_id)?,
        canceled: load_queue_canceled_record(store, action_id)?,
    })
}

pub(crate) fn action_detail_has_any_records(records: &ActionDetailRecords) -> bool {
    records.provenance.is_some()
        || records.pending.is_some()
        || records.running.is_some()
        || records.done.is_some()
        || records.failed.is_some()
        || records.canceled.is_some()
}

pub(crate) fn action_spec_from_records(records: &ActionDetailRecords) -> Option<&ActionSpec> {
    records
        .failed
        .as_ref()
        .map(|r| &r.action)
        .or_else(|| records.running.as_ref().map(|r| &r.action))
        .or_else(|| records.pending.as_ref().map(|r| &r.action))
        .or_else(|| records.canceled.as_ref().map(|r| &r.action))
        .or_else(|| records.provenance.as_ref().map(|p| &p.action))
}

pub(crate) fn build_action_related_actions_view(
    store: &ArtifactStore,
    action_id: &str,
    current_action: Option<&ActionSpec>,
) -> Result<ActionRelatedActionsView> {
    let (mut action_specs, provenance_by_action_id) =
        collect_known_action_specs_and_provenances(store)?;
    if let Some(action) = current_action
        && !action_specs.contains_key(action_id)
    {
        action_specs.insert(action_id.to_string(), action.clone());
    }

    let upstream_ids = collect_transitive_upstream_action_ids(action_id, &action_specs);
    let reverse_dependency_index = build_reverse_dependency_index(&action_specs);
    let downstream_ids =
        collect_transitive_downstream_action_ids(action_id, &reverse_dependency_index);

    Ok(ActionRelatedActionsView {
        upstream: build_related_action_rows(
            store,
            &action_specs,
            &provenance_by_action_id,
            upstream_ids,
        ),
        downstream: build_related_action_rows(
            store,
            &action_specs,
            &provenance_by_action_id,
            downstream_ids,
        ),
    })
}

fn collect_known_action_specs_and_provenances(
    store: &ArtifactStore,
) -> Result<(BTreeMap<String, ActionSpec>, BTreeMap<String, Provenance>)> {
    let mut action_specs: BTreeMap<String, ActionSpec> = BTreeMap::new();
    let mut provenance_by_action_id: BTreeMap<String, Provenance> = BTreeMap::new();
    let provenances = store.list_provenances_shared()?;
    for provenance in provenances.iter() {
        action_specs.insert(provenance.action_id.clone(), provenance.action.clone());
        for suggested in &provenance.suggested_next_actions {
            action_specs
                .entry(suggested.action_id.clone())
                .or_insert_with(|| suggested.action.clone());
        }
        provenance_by_action_id.insert(provenance.action_id.clone(), provenance.clone());
    }

    for queue_path in list_queue_files(&store.queue_pending_dir())? {
        let text = match fs::read_to_string(&queue_path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("reading pending queue record: {}", queue_path.display())
                });
            }
        };
        let (queue_action_id, _, _, queue_action) = match parse_queue_work_item(&text, &queue_path)
        {
            Ok(item) => item,
            Err(err) => {
                warn!(
                    "skipping malformed pending queue record while building related actions path={} error={:#}",
                    queue_path.display(),
                    err
                );
                continue;
            }
        };
        action_specs.entry(queue_action_id).or_insert(queue_action);
    }

    for queue_path in list_queue_files(&store.queue_running_dir())? {
        let text = match fs::read_to_string(&queue_path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("reading running queue record: {}", queue_path.display())
                });
            }
        };
        let (queue_action_id, _, _, queue_action) = match parse_queue_work_item(&text, &queue_path)
        {
            Ok(item) => item,
            Err(err) => {
                warn!(
                    "skipping malformed running queue record while building related actions path={} error={:#}",
                    queue_path.display(),
                    err
                );
                continue;
            }
        };
        action_specs.entry(queue_action_id).or_insert(queue_action);
    }

    for failed in load_failed_queue_records(store)? {
        action_specs
            .entry(failed.action_id)
            .or_insert(failed.action);
    }

    for queue_path in list_queue_files(&store.queue_canceled_dir())? {
        let text = match fs::read_to_string(&queue_path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("reading canceled queue record: {}", queue_path.display())
                });
            }
        };
        let canceled: QueueCanceled = match serde_json::from_str(&text)
            .with_context(|| format!("parsing canceled queue record: {}", queue_path.display()))
        {
            Ok(record) => record,
            Err(err) => {
                warn!(
                    "skipping malformed canceled queue record while building related actions path={} error={:#}",
                    queue_path.display(),
                    err
                );
                continue;
            }
        };
        action_specs
            .entry(canceled.action_id)
            .or_insert(canceled.action);
    }

    Ok((action_specs, provenance_by_action_id))
}

fn sorted_unique_dependency_ids(action: &ActionSpec) -> Vec<String> {
    let mut deps: Vec<String> = action_dependency_action_ids(action)
        .into_iter()
        .map(ToOwned::to_owned)
        .collect();
    deps.sort();
    deps.dedup();
    deps
}

fn build_reverse_dependency_index(
    action_specs: &BTreeMap<String, ActionSpec>,
) -> BTreeMap<String, Vec<String>> {
    let mut dependents_by_dependency: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (candidate_action_id, candidate_action) in action_specs {
        for dependency_action_id in sorted_unique_dependency_ids(candidate_action) {
            dependents_by_dependency
                .entry(dependency_action_id)
                .or_default()
                .push(candidate_action_id.clone());
        }
    }
    for dependents in dependents_by_dependency.values_mut() {
        dependents.sort();
        dependents.dedup();
    }
    dependents_by_dependency
}

fn collect_transitive_upstream_action_ids(
    root_action_id: &str,
    action_specs: &BTreeMap<String, ActionSpec>,
) -> Vec<(String, usize)> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<(String, usize)> = VecDeque::new();
    let mut ordered = Vec::new();
    visited.insert(root_action_id.to_string());

    if let Some(root_action) = action_specs.get(root_action_id) {
        for dep in sorted_unique_dependency_ids(root_action) {
            queue.push_back((dep, 1));
        }
    }

    while let Some((current, distance_hops)) = queue.pop_front() {
        if !visited.insert(current.clone()) {
            continue;
        }
        ordered.push((current.clone(), distance_hops));
        if let Some(current_action) = action_specs.get(&current) {
            for next in sorted_unique_dependency_ids(current_action) {
                if !visited.contains(&next) {
                    queue.push_back((next, distance_hops + 1));
                }
            }
        }
    }

    ordered
}

fn collect_transitive_downstream_action_ids(
    root_action_id: &str,
    dependents_by_dependency: &BTreeMap<String, Vec<String>>,
) -> Vec<(String, usize)> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<(String, usize)> = VecDeque::new();
    let mut ordered = Vec::new();
    visited.insert(root_action_id.to_string());

    if let Some(first_hop) = dependents_by_dependency.get(root_action_id) {
        for next in first_hop {
            queue.push_back((next.clone(), 1));
        }
    }

    while let Some((current, distance_hops)) = queue.pop_front() {
        if !visited.insert(current.clone()) {
            continue;
        }
        ordered.push((current.clone(), distance_hops));
        if let Some(next_hop) = dependents_by_dependency.get(&current) {
            for next in next_hop {
                if !visited.contains(next) {
                    queue.push_back((next.clone(), distance_hops + 1));
                }
            }
        }
    }

    ordered
}

fn build_related_action_rows(
    store: &ArtifactStore,
    action_specs: &BTreeMap<String, ActionSpec>,
    provenance_by_action_id: &BTreeMap<String, Provenance>,
    related_action_ids: Vec<(String, usize)>,
) -> Vec<RelatedActionRowView> {
    related_action_ids
        .into_iter()
        .map(|(related_action_id, distance_hops)| {
            let queue_state = if store.action_exists(&related_action_id) {
                QueueState::Done
            } else {
                queue_state_for_action(store, &related_action_id)
            };
            let output_artifact = provenance_by_action_id.get(&related_action_id).map(|p| {
                format!(
                    "{} {}",
                    artifact_type_label(&p.output_artifact.artifact_type),
                    p.output_artifact.relpath
                )
            });
            let action = action_specs.get(&related_action_id);
            RelatedActionRowView {
                action_id: related_action_id,
                distance_hops,
                queue_state_key: queue_state_key(&queue_state).to_string(),
                queue_state_label: queue_state_display_label(&queue_state).to_string(),
                action_kind: action
                    .map(action_kind_label)
                    .unwrap_or("unknown")
                    .to_string(),
                subject: action
                    .map(action_subject)
                    .unwrap_or_else(|| "(not available)".to_string()),
                output_artifact,
            }
        })
        .collect()
}

pub(crate) fn action_dso_version(action: &ActionSpec) -> Option<&str> {
    match action {
        ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { version, .. }
        | ActionSpec::DownloadAndExtractXlsynthSourceSubtree { version, .. }
        | ActionSpec::DriverDslxFnToIr { version, .. }
        | ActionSpec::DriverIrToOpt { version, .. }
        | ActionSpec::DriverIrToDelayInfo { version, .. }
        | ActionSpec::DriverIrEquiv { version, .. }
        | ActionSpec::DriverIrAigEquiv { version, .. }
        | ActionSpec::DriverIrToG8rAig { version, .. }
        | ActionSpec::IrFnToCombinationalVerilog { version, .. }
        | ActionSpec::IrFnToKBoolConeCorpus { version, .. }
        | ActionSpec::DriverAigToStats { version, .. } => Some(version.as_str()),
        ActionSpec::ComboVerilogToYosysAbcAig { .. }
        | ActionSpec::AigToYosysAbcAig { .. }
        | ActionSpec::AigStatDiff { .. } => None,
    }
}

pub(crate) fn action_driver_version(action: &ActionSpec) -> Option<&str> {
    match action {
        ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
            discovery_runtime, ..
        }
        | ActionSpec::DownloadAndExtractXlsynthSourceSubtree {
            discovery_runtime, ..
        } => discovery_runtime
            .as_ref()
            .map(|runtime| runtime.driver_version.as_str()),
        ActionSpec::DriverDslxFnToIr { runtime, .. }
        | ActionSpec::DriverIrToOpt { runtime, .. }
        | ActionSpec::DriverIrToDelayInfo { runtime, .. }
        | ActionSpec::DriverIrEquiv { runtime, .. }
        | ActionSpec::DriverIrAigEquiv { runtime, .. }
        | ActionSpec::DriverIrToG8rAig { runtime, .. }
        | ActionSpec::IrFnToCombinationalVerilog { runtime, .. }
        | ActionSpec::IrFnToKBoolConeCorpus { runtime, .. }
        | ActionSpec::DriverAigToStats { runtime, .. } => Some(runtime.driver_version.as_str()),
        ActionSpec::ComboVerilogToYosysAbcAig { .. }
        | ActionSpec::AigToYosysAbcAig { .. }
        | ActionSpec::AigStatDiff { .. } => None,
    }
}

pub(crate) fn load_compat_by_dso(repo_root: &Path) -> BTreeMap<String, Vec<String>> {
    let compat = match load_version_compat_map(repo_root) {
        Ok(v) => v,
        Err(_) => return BTreeMap::new(),
    };
    let mut by_dso: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (crate_version, entry) in compat {
        let dso = normalize_tag_version(&entry.xlsynth_release_version).to_string();
        by_dso.entry(dso).or_default().push(crate_version);
    }
    for versions in by_dso.values_mut() {
        versions.sort_by(|a, b| cmp_dotted_numeric_version(a, b).reverse());
        versions.dedup();
    }
    by_dso
}

pub(crate) fn fallback_crate_for_dso(
    action: &ActionSpec,
    compat_by_dso: &BTreeMap<String, Vec<String>>,
) -> Option<String> {
    let dso = normalize_tag_version(action_dso_version(action)?).to_string();
    compat_by_dso.get(&dso).and_then(|v| v.first().cloned())
}

fn infer_crate_version_for_action_with_lookup(
    action: &ActionSpec,
    compat_by_dso: &BTreeMap<String, Vec<String>>,
    provenance_by_action_id: &BTreeMap<&str, &Provenance>,
    cache: &mut BTreeMap<String, Option<String>>,
) -> Option<String> {
    fn infer_by_action_id(
        action_id: &str,
        compat_by_dso: &BTreeMap<String, Vec<String>>,
        provenance_by_action_id: &BTreeMap<&str, &Provenance>,
        cache: &mut BTreeMap<String, Option<String>>,
    ) -> Option<String> {
        if let Some(cached) = cache.get(action_id) {
            return cached.clone();
        }
        let inferred = provenance_by_action_id
            .get(action_id)
            .and_then(|provenance| {
                infer_crate_version_for_action_with_lookup(
                    &provenance.action,
                    compat_by_dso,
                    provenance_by_action_id,
                    cache,
                )
            });
        cache.insert(action_id.to_string(), inferred.clone());
        inferred
    }

    if let ActionSpec::DriverAigToStats {
        aig_action_id,
        runtime,
        ..
    } = action
    {
        if let Some(v) =
            infer_by_action_id(aig_action_id, compat_by_dso, provenance_by_action_id, cache)
        {
            return Some(v);
        }
        return Some(runtime.driver_version.clone());
    }

    if let Some(v) = action_driver_version(action) {
        return Some(v.to_string());
    }

    match action {
        ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id, ..
        } => infer_by_action_id(
            verilog_action_id,
            compat_by_dso,
            provenance_by_action_id,
            cache,
        ),
        ActionSpec::AigToYosysAbcAig { aig_action_id, .. } => {
            infer_by_action_id(aig_action_id, compat_by_dso, provenance_by_action_id, cache)
        }
        ActionSpec::AigStatDiff {
            g8r_aig_stats_action_id,
            yosys_abc_aig_stats_action_id,
            ..
        } => {
            for action_id in [g8r_aig_stats_action_id, yosys_abc_aig_stats_action_id] {
                if let Some(v) =
                    infer_by_action_id(action_id, compat_by_dso, provenance_by_action_id, cache)
                {
                    return Some(v);
                }
            }
            None
        }
        _ => fallback_crate_for_dso(action, compat_by_dso),
    }
}

fn infer_crate_version_for_provenance_with_lookup(
    provenance: &Provenance,
    compat_by_dso: &BTreeMap<String, Vec<String>>,
    provenance_by_action_id: &BTreeMap<&str, &Provenance>,
    cache: &mut BTreeMap<String, Option<String>>,
) -> Option<String> {
    if let Some(v) = cache.get(provenance.action_id.as_str()) {
        return v.clone();
    }
    let inferred = if let Some(v) = infer_crate_version_for_action_with_lookup(
        &provenance.action,
        compat_by_dso,
        provenance_by_action_id,
        cache,
    ) {
        Some(v)
    } else {
        provenance
            .suggested_next_actions
            .iter()
            .find_map(|suggested| action_driver_version(&suggested.action).map(|v| v.to_string()))
            .or_else(|| fallback_crate_for_dso(&provenance.action, compat_by_dso))
    };
    cache.insert(provenance.action_id.clone(), inferred.clone());
    inferred
}

pub(crate) fn infer_crate_version_for_action(
    store: &ArtifactStore,
    action: &ActionSpec,
    compat_by_dso: &BTreeMap<String, Vec<String>>,
) -> Option<String> {
    if let ActionSpec::DriverAigToStats {
        aig_action_id,
        runtime,
        ..
    } = action
    {
        if let Ok(provenance) = store.load_provenance(aig_action_id)
            && let Some(v) =
                infer_crate_version_for_action(store, &provenance.action, compat_by_dso)
        {
            return Some(v);
        }
        return Some(runtime.driver_version.clone());
    }

    if let Some(v) = action_driver_version(action) {
        return Some(v.to_string());
    }
    match action {
        ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id, ..
        } => infer_verilog_driver_context(store, verilog_action_id)
            .ok()
            .flatten()
            .map(|ctx| ctx.runtime.driver_version),
        ActionSpec::AigToYosysAbcAig { aig_action_id, .. } => {
            if let Ok(provenance) = store.load_provenance(aig_action_id) {
                infer_crate_version_for_action(store, &provenance.action, compat_by_dso)
            } else {
                None
            }
        }
        ActionSpec::AigStatDiff {
            g8r_aig_stats_action_id,
            yosys_abc_aig_stats_action_id,
            ..
        } => {
            for action_id in [g8r_aig_stats_action_id, yosys_abc_aig_stats_action_id] {
                if let Ok(provenance) = store.load_provenance(action_id)
                    && let Some(v) =
                        infer_crate_version_for_action(store, &provenance.action, compat_by_dso)
                {
                    return Some(v);
                }
            }
            None
        }
        _ => fallback_crate_for_dso(action, compat_by_dso),
    }
}

pub(crate) fn classify_crate_version_inference_miss(
    store: &ArtifactStore,
    action: &ActionSpec,
    compat_by_dso: &BTreeMap<String, Vec<String>>,
) -> CrateVersionInferenceMissReason {
    if action_driver_version(action).is_some() {
        return CrateVersionInferenceMissReason::InvariantDriverVersionPresent;
    }
    if let Some(dso_version) = action_dso_version(action) {
        let dso = normalize_tag_version(dso_version).to_string();
        if compat_by_dso.contains_key(&dso) {
            return CrateVersionInferenceMissReason::InvariantMappedDsoPresent;
        }
        return CrateVersionInferenceMissReason::UnmappedDsoVersion;
    }
    match action {
        ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id, ..
        } => {
            if store.action_exists(verilog_action_id) {
                CrateVersionInferenceMissReason::MissingDependencyCrateContext
            } else {
                CrateVersionInferenceMissReason::MissingDependencyProvenance
            }
        }
        ActionSpec::AigToYosysAbcAig { aig_action_id, .. } => {
            if store.action_exists(aig_action_id) {
                CrateVersionInferenceMissReason::MissingDependencyCrateContext
            } else {
                CrateVersionInferenceMissReason::MissingDependencyProvenance
            }
        }
        ActionSpec::AigStatDiff {
            g8r_aig_stats_action_id,
            yosys_abc_aig_stats_action_id,
            ..
        } => {
            let has_dependency = store.action_exists(g8r_aig_stats_action_id)
                || store.action_exists(yosys_abc_aig_stats_action_id);
            if has_dependency {
                CrateVersionInferenceMissReason::MissingDependencyCrateContext
            } else {
                CrateVersionInferenceMissReason::MissingDependencyProvenance
            }
        }
        _ => CrateVersionInferenceMissReason::NoVersionContext,
    }
}

pub(crate) fn action_kind_label(action: &ActionSpec) -> &'static str {
    match action {
        ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { .. } => {
            "download_and_extract_xlsynth_release_stdlib_tarball"
        }
        ActionSpec::DownloadAndExtractXlsynthSourceSubtree { .. } => {
            "download_and_extract_xlsynth_source_subtree"
        }
        ActionSpec::DriverDslxFnToIr { .. } => "driver_dslx_fn_to_ir",
        ActionSpec::DriverIrToOpt { .. } => "driver_ir_to_opt",
        ActionSpec::DriverIrToDelayInfo { .. } => "driver_ir_to_delay_info",
        ActionSpec::DriverIrEquiv { .. } => "driver_ir_equiv",
        ActionSpec::DriverIrAigEquiv { .. } => "driver_ir_aig_equiv",
        ActionSpec::DriverIrToG8rAig { .. } => "driver_ir_to_g8r_aig",
        ActionSpec::IrFnToCombinationalVerilog { .. } => "ir_fn_to_combinational_verilog",
        ActionSpec::IrFnToKBoolConeCorpus { .. } => "ir_fn_to_k_bool_cone_corpus",
        ActionSpec::ComboVerilogToYosysAbcAig { .. } => "combo_verilog_to_yosys_abc_aig",
        ActionSpec::AigToYosysAbcAig { .. } => "aig_to_yosys_abc_aig",
        ActionSpec::DriverAigToStats { .. } => "driver_aig_to_stats",
        ActionSpec::AigStatDiff { .. } => "aig_stat_diff",
    }
}

pub(crate) fn action_subject(action: &ActionSpec) -> String {
    match action {
        ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { version, .. } => {
            format!("download-stdlib {}", version)
        }
        ActionSpec::DownloadAndExtractXlsynthSourceSubtree {
            version, subtree, ..
        } => {
            format!("download-src-subtree {} {}", version, subtree)
        }
        ActionSpec::DriverDslxFnToIr {
            dslx_file,
            dslx_fn_name,
            ..
        } => format!("{}::{}", dslx_file, dslx_fn_name),
        ActionSpec::DriverIrToOpt {
            ir_action_id,
            top_fn_name,
            ..
        } => format!(
            "ir={} top={}",
            short_id(ir_action_id),
            top_fn_name.as_deref().unwrap_or("<auto>")
        ),
        ActionSpec::DriverIrToDelayInfo {
            ir_action_id,
            top_fn_name,
            delay_model,
            ..
        } => format!(
            "ir={} top={} model={}",
            short_id(ir_action_id),
            top_fn_name.as_deref().unwrap_or("<auto>"),
            delay_model
        ),
        ActionSpec::DriverIrEquiv {
            lhs_ir_action_id,
            rhs_ir_action_id,
            top_fn_name,
            ..
        } => format!(
            "lhs={} rhs={} top={}",
            short_id(lhs_ir_action_id),
            short_id(rhs_ir_action_id),
            top_fn_name.as_deref().unwrap_or("<auto>")
        ),
        ActionSpec::DriverIrAigEquiv {
            ir_action_id,
            aig_action_id,
            top_fn_name,
            ..
        } => format!(
            "ir={} aig={} top={}",
            short_id(ir_action_id),
            short_id(aig_action_id),
            top_fn_name.as_deref().unwrap_or("<auto>")
        ),
        ActionSpec::DriverIrToG8rAig {
            ir_action_id,
            top_fn_name,
            fraig,
            lowering_mode,
            ..
        } => format!(
            "ir={} top={} fraig={} mode={}",
            short_id(ir_action_id),
            top_fn_name.as_deref().unwrap_or("<auto>"),
            fraig,
            match lowering_mode {
                G8rLoweringMode::Default => "default",
                G8rLoweringMode::FrontendNoPrepRewrite => "frontend_no_prep_rewrite",
            }
        ),
        ActionSpec::IrFnToCombinationalVerilog {
            ir_action_id,
            top_fn_name,
            use_system_verilog,
            ..
        } => format!(
            "ir={} top={} system_verilog={}",
            short_id(ir_action_id),
            top_fn_name.as_deref().unwrap_or("<auto>"),
            use_system_verilog
        ),
        ActionSpec::IrFnToKBoolConeCorpus {
            ir_action_id,
            top_fn_name,
            k,
            max_ir_ops,
            ..
        } => format!(
            "ir={} top={} k={} max_ir_ops={}",
            short_id(ir_action_id),
            top_fn_name.as_deref().unwrap_or("<auto>"),
            k,
            max_ir_ops
                .map(|v| v.to_string())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id,
            verilog_top_module_name,
            yosys_script_ref,
            ..
        } => format!(
            "verilog={} top={} script={}",
            short_id(verilog_action_id),
            verilog_top_module_name.as_deref().unwrap_or("<auto>"),
            script_ref_short_label(yosys_script_ref)
        ),
        ActionSpec::AigToYosysAbcAig {
            aig_action_id,
            yosys_script_ref,
            ..
        } => format!(
            "aig={} script={}",
            short_id(aig_action_id),
            script_ref_short_label(yosys_script_ref)
        ),
        ActionSpec::DriverAigToStats { aig_action_id, .. } => {
            format!("aig={}", short_id(aig_action_id))
        }
        ActionSpec::AigStatDiff {
            opt_ir_action_id,
            g8r_aig_stats_action_id,
            yosys_abc_aig_stats_action_id,
        } => format!(
            "opt_ir={} g8r={} yosys={}",
            short_id(opt_ir_action_id),
            short_id(g8r_aig_stats_action_id),
            short_id(yosys_abc_aig_stats_action_id)
        ),
    }
}

pub(crate) fn is_timeout_error(error: &str) -> bool {
    error
        .lines()
        .map(str::trim_start)
        .find(|line| !line.trim().is_empty())
        .map(|line| line.starts_with("TIMEOUT("))
        .unwrap_or(false)
}

pub(crate) fn html_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

pub(crate) fn is_valid_action_id_for_route(action_id: &str) -> bool {
    action_id.len() == 64 && action_id.bytes().all(|b| b.is_ascii_hexdigit())
}

pub(crate) fn normalize_action_output_relpath(path: &str) -> Option<String> {
    let normalized = path.trim().replace('\\', "/");
    if normalized.is_empty() {
        return None;
    }
    let candidate = Path::new(&normalized);
    if candidate.is_absolute() {
        return None;
    }
    if !candidate
        .components()
        .all(|c| matches!(c, Component::Normal(_)))
    {
        return None;
    }
    Some(normalized)
}

pub(crate) fn artifact_type_label(artifact_type: &ArtifactType) -> &'static str {
    match artifact_type {
        ArtifactType::DslxFileSubtree => "dslx_file_subtree",
        ArtifactType::IrPackageFile => "ir_package_file",
        ArtifactType::IrDelayInfoFile => "ir_delay_info_file",
        ArtifactType::AigFile => "aig_file",
        ArtifactType::VerilogFile => "verilog_file",
        ArtifactType::AigStatsFile => "aig_stats_file",
        ArtifactType::AigStatDiffFile => "aig_stat_diff_file",
        ArtifactType::EquivReportFile => "equiv_report_file",
    }
}

pub(crate) fn repo_relative_display_path(repo_root: &Path, path: &Path) -> String {
    path.strip_prefix(repo_root)
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| path.display().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::fs;
    use std::io::Read;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_runtime() -> DriverRuntimeSpec {
        DriverRuntimeSpec {
            driver_version: "0.31.0".to_string(),
            release_platform: "x64-linux".to_string(),
            docker_image: "ubuntu:24.04".to_string(),
            dockerfile: "FROM ubuntu:24.04".to_string(),
        }
    }

    fn make_test_store(prefix: &str) -> (ArtifactStore, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-query-{prefix}-{}-{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).expect("create test store root");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure test store layout");
        (store, root)
    }

    fn make_ir_fn_corpus_sample(
        structural_hash: &str,
        crate_version: &str,
        ir_top: &str,
        fn_key: &str,
        ir_node_count: u64,
        g8r_product_loss: f64,
    ) -> StdlibG8rVsYosysSample {
        StdlibG8rVsYosysSample {
            fn_key: fn_key.to_string(),
            crate_version: crate_version.to_string(),
            dso_version: "0.39.0".to_string(),
            ir_action_id: format!("{structural_hash}:{crate_version}:{ir_top}"),
            ir_top: Some(ir_top.to_string()),
            structural_hash: Some(structural_hash.to_string()),
            ir_node_count,
            g8r_nodes: 10.0,
            g8r_levels: 10.0,
            yosys_abc_nodes: 10.0,
            yosys_abc_levels: 10.0,
            g8r_product: 100.0 + g8r_product_loss,
            yosys_abc_product: 100.0,
            g8r_product_loss,
            g8r_stats_action_id: format!("g8r:{structural_hash}:{crate_version}"),
            yosys_abc_stats_action_id: format!("yabc:{structural_hash}:{crate_version}"),
        }
    }

    fn materialize_test_provenance(
        store: &ArtifactStore,
        action_id: &str,
        action: ActionSpec,
        artifact_type: ArtifactType,
        relpath: &str,
    ) {
        let staging_dir = store.staging_dir().join(format!("{action_id}-stage"));
        let output_path = staging_dir.join(relpath);
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).expect("create test output parent");
        }
        fs::write(&output_path, format!("artifact for {action_id}")).expect("write test output");
        let bytes = fs::metadata(&output_path).expect("stat test output").len();
        let provenance = Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: action_id.to_string(),
            created_utc: Utc::now(),
            action,
            dependencies: Vec::new(),
            output_artifact: ArtifactRef {
                action_id: action_id.to_string(),
                artifact_type,
                relpath: relpath.to_string(),
            },
            output_files: vec![OutputFile {
                path: relpath.to_string(),
                bytes,
                sha256: "0".repeat(64),
            }],
            commands: Vec::new(),
            details: json!({"synthetic": true}),
            suggested_next_actions: Vec::new(),
        };
        fs::write(
            staging_dir.join("provenance.json"),
            serde_json::to_string_pretty(&provenance).expect("serialize test provenance"),
        )
        .expect("write test provenance");
        store
            .promote_staging_action_dir(action_id, &staging_dir)
            .expect("promote test provenance");
    }

    fn overwrite_test_artifact(
        store: &ArtifactStore,
        action_id: &str,
        artifact_type: ArtifactType,
        relpath: &str,
        content: &str,
    ) {
        let artifact_ref = ArtifactRef {
            action_id: action_id.to_string(),
            artifact_type,
            relpath: relpath.to_string(),
        };
        let path = store.resolve_artifact_ref_path(&artifact_ref);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create artifact parent");
        }
        fs::write(path, content).expect("overwrite test artifact");
    }

    fn synthetic_provenance(
        action_id: &str,
        action: ActionSpec,
        artifact_type: ArtifactType,
    ) -> Provenance {
        Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: action_id.to_string(),
            created_utc: Utc::now(),
            action,
            dependencies: Vec::new(),
            output_artifact: ArtifactRef {
                action_id: action_id.to_string(),
                artifact_type,
                relpath: "payload/out".to_string(),
            },
            output_files: Vec::new(),
            commands: Vec::new(),
            details: json!({"synthetic": true}),
            suggested_next_actions: Vec::new(),
        }
    }

    fn borrowed_lookup<'a>(
        provenance_by_action_id: &'a BTreeMap<String, Provenance>,
    ) -> ProvenanceLookup<'a> {
        provenance_by_action_id
            .iter()
            .map(|(action_id, provenance)| (action_id.as_str(), provenance))
            .collect()
    }

    #[test]
    fn transitive_dependency_traversal_reports_expected_hops() {
        let runtime = test_runtime();
        let mut action_specs = BTreeMap::new();
        action_specs.insert(
            "subtree".to_string(),
            ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                version: "0.35.0".to_string(),
                discovery_runtime: None,
            },
        );
        action_specs.insert(
            "ir".to_string(),
            ActionSpec::DriverDslxFnToIr {
                dslx_subtree_action_id: "subtree".to_string(),
                dslx_file: "xls/dslx/stdlib/float32.x".to_string(),
                dslx_fn_name: "add".to_string(),
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
        );
        action_specs.insert(
            "opt".to_string(),
            ActionSpec::DriverIrToOpt {
                ir_action_id: "ir".to_string(),
                top_fn_name: Some("__float32__add".to_string()),
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
        );
        action_specs.insert(
            "g8r".to_string(),
            ActionSpec::DriverIrToG8rAig {
                ir_action_id: "ir".to_string(),
                top_fn_name: Some("__float32__add".to_string()),
                fraig: false,
                lowering_mode: G8rLoweringMode::Default,
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
        );
        action_specs.insert(
            "stats".to_string(),
            ActionSpec::DriverAigToStats {
                aig_action_id: "g8r".to_string(),
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
        );
        action_specs.insert(
            "equiv".to_string(),
            ActionSpec::DriverIrEquiv {
                lhs_ir_action_id: "opt".to_string(),
                rhs_ir_action_id: "g8r".to_string(),
                top_fn_name: Some("__float32__add".to_string()),
                version: "0.35.0".to_string(),
                runtime,
            },
        );

        let upstream = collect_transitive_upstream_action_ids("equiv", &action_specs);
        assert_eq!(
            upstream,
            vec![
                ("g8r".to_string(), 1),
                ("opt".to_string(), 1),
                ("ir".to_string(), 2),
                ("subtree".to_string(), 3)
            ]
        );

        let reverse = build_reverse_dependency_index(&action_specs);
        let downstream = collect_transitive_downstream_action_ids("ir", &reverse);
        assert_eq!(
            downstream,
            vec![
                ("g8r".to_string(), 1),
                ("opt".to_string(), 1),
                ("equiv".to_string(), 2),
                ("stats".to_string(), 2)
            ]
        );
    }

    #[test]
    fn transitive_dependency_traversal_handles_cycles_without_looping() {
        let runtime = test_runtime();
        let mut action_specs = BTreeMap::new();
        action_specs.insert(
            "a".to_string(),
            ActionSpec::DriverIrToOpt {
                ir_action_id: "b".to_string(),
                top_fn_name: None,
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
        );
        action_specs.insert(
            "b".to_string(),
            ActionSpec::DriverIrToOpt {
                ir_action_id: "a".to_string(),
                top_fn_name: None,
                version: "0.35.0".to_string(),
                runtime,
            },
        );

        let upstream = collect_transitive_upstream_action_ids("a", &action_specs);
        assert_eq!(upstream, vec![("b".to_string(), 1)]);

        let reverse = build_reverse_dependency_index(&action_specs);
        let downstream = collect_transitive_downstream_action_ids("a", &reverse);
        assert_eq!(downstream, vec![("b".to_string(), 1)]);
    }

    #[test]
    fn stdlib_fn_history_view_url_handles_default_and_non_default_delay_model() {
        let default_url = stdlib_fn_history_view_url("float32.x", "add", false, "asap7");
        assert_eq!(default_url, "/dslx-fns/float32.x:add");

        let non_default_url = stdlib_fn_history_view_url("float32.x", "add", true, "sky130");
        assert_eq!(
            non_default_url,
            "/dslx-fns/float32.x:add?fraig=true&delay_model=sky130"
        );
    }

    #[test]
    fn stdlib_g8r_vs_yosys_view_url_keeps_scope_and_encodes_crate_version() {
        let url = stdlib_g8r_vs_yosys_view_url(true, Some(4096), Some("0.31.0+meta"));
        assert_eq!(
            url,
            "/dslx-fns-g8r-vs-yosys-abc/?fraig=true&max_ir_nodes=4096&crate_version=0.31.0%2Bmeta"
        );
    }

    #[test]
    fn ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_view_url_encodes_crate_version() {
        let url =
            ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_view_url(Some(4096), Some("0.31.0+meta"));
        assert_eq!(
            url,
            "/ir-fn-g8r-abc-vs-codegen-yosys-abc/?max_ir_nodes=4096&crate_version=0.31.0%2Bmeta"
        );
    }

    #[test]
    fn ir_fn_corpus_k3_losses_vs_crate_version_view_url_encodes_crate_version() {
        let url = ir_fn_corpus_k3_losses_vs_crate_version_view_url(
            Some(4096),
            Some("0.31.0+meta"),
            false,
        );
        assert_eq!(
            url,
            "/ir-fn-corpus-k3-losses-vs-crate-version/?max_ir_nodes=4096&crate_version=0.31.0%2Bmeta"
        );
        let show_same_url =
            ir_fn_corpus_k3_losses_vs_crate_version_view_url(Some(4096), Some("0.31.0+meta"), true);
        assert_eq!(
            show_same_url,
            "/ir-fn-corpus-k3-losses-vs-crate-version/?max_ir_nodes=4096&crate_version=0.31.0%2Bmeta&show_same=true"
        );
    }

    #[test]
    fn format_progress_duration_formats_seconds_minutes_and_hours() {
        assert_eq!(format_progress_duration(7.0), "7s");
        assert_eq!(format_progress_duration(67.0), "1m07s");
        assert_eq!(format_progress_duration(3667.0), "1h01m07s");
    }

    #[test]
    fn format_progress_duration_handles_non_finite() {
        assert_eq!(format_progress_duration(f64::NAN), "unknown");
        assert_eq!(format_progress_duration(f64::INFINITY), "unknown");
    }

    #[test]
    fn clamp_ir_node_limit_treats_zero_requested_as_unset() {
        assert_eq!(clamp_ir_node_limit(Some(0), 1, 1434), 1434);
    }

    #[test]
    fn clamp_ir_node_limit_still_clamps_nonzero_requested_values() {
        assert_eq!(clamp_ir_node_limit(Some(1), 1, 1434), 1);
        assert_eq!(clamp_ir_node_limit(Some(2000), 1, 1434), 1434);
    }

    #[test]
    fn stdlib_file_action_graph_view_url_includes_action_id() {
        let url = stdlib_file_action_graph_view_url(
            Some("0.31.0"),
            Some("xls/dslx/stdlib/float32.x"),
            Some("add"),
            Some("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
            false,
        );
        assert_eq!(
            url,
            "/dslx-file-action-graph/?crate_version=0.31.0&file=xls%2Fdslx%2Fstdlib%2Ffloat32.x&fn_name=add&action_id=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
    }

    #[test]
    fn stdlib_file_action_graph_view_url_includes_k3_toggle_when_enabled() {
        let url = stdlib_file_action_graph_view_url(
            Some("0.31.0"),
            Some("xls/dslx/stdlib/float32.x"),
            Some("add"),
            None,
            true,
        );
        assert_eq!(
            url,
            "/dslx-file-action-graph/?crate_version=0.31.0&file=xls%2Fdslx%2Fstdlib%2Ffloat32.x&fn_name=add&include_k3_descendants=true"
        );
    }

    #[test]
    fn build_queue_live_status_splits_owned_and_foreign_running_leases() {
        let (store, root) = make_test_store("queue-live-owned-vs-foreign");
        let now = Utc::now();
        let runner_prefix = "host-a:999:web-runner-";

        let owned = QueueRunning {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: "a".repeat(64),
            enqueued_utc: now,
            priority: crate::DEFAULT_QUEUE_PRIORITY,
            action: ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                version: "v0.35.0".to_string(),
                discovery_runtime: None,
            },
            lease_owner: format!("{runner_prefix}3"),
            lease_acquired_utc: now,
            lease_expires_utc: now + chrono::Duration::seconds(60),
        };
        let foreign = QueueRunning {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: "b".repeat(64),
            enqueued_utc: now,
            priority: crate::DEFAULT_QUEUE_PRIORITY,
            action: ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                version: "v0.35.0".to_string(),
                discovery_runtime: None,
            },
            lease_owner: "other-host:42:web-runner-7".to_string(),
            lease_acquired_utc: now,
            lease_expires_utc: now + chrono::Duration::seconds(120),
        };

        for running in [&owned, &foreign] {
            let path = store.running_queue_path(&running.action_id);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("create running queue parent");
            }
            fs::write(
                &path,
                serde_json::to_string_pretty(running).expect("serialize running record"),
            )
            .expect("write running queue record");
        }

        let status = build_queue_live_status(&store, &root, Some(runner_prefix))
            .expect("build queue live status");
        assert_eq!(status.pending, 0);
        assert_eq!(status.pending_expanders, 0);
        assert_eq!(status.pending_non_expanders, 0);
        assert_eq!(status.running, 2);
        assert_eq!(status.running_expanders, 2);
        assert_eq!(status.running_non_expanders, 0);
        assert_eq!(status.running_owned, 1);
        assert_eq!(status.running_foreign, 1);
        assert!(status.pending_is_lower_bound);
        assert_eq!(status.running_actions.len(), 2);
        assert!(
            status
                .running_actions
                .iter()
                .any(|row| row.owned_by_this_runner),
            "expected at least one owned running row"
        );
        assert!(
            status
                .running_actions
                .iter()
                .any(|row| !row.owned_by_this_runner),
            "expected at least one foreign running row"
        );

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn build_queue_live_status_counts_pending_expanders() {
        let (store, root) = make_test_store("queue-live-pending-expanders");
        let now = Utc::now();
        let driver_runtime = DriverRuntimeSpec {
            driver_version: "0.33.0".to_string(),
            release_platform: "ubuntu2004".to_string(),
            docker_image: "xlsynth-bvc-driver:0.33.0".to_string(),
            dockerfile: "docker/xlsynth-driver.Dockerfile".to_string(),
        };

        let expander_item = QueueItem {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: "c".repeat(64),
            enqueued_utc: now,
            priority: crate::DEFAULT_QUEUE_PRIORITY,
            action: ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                version: "v0.35.0".to_string(),
                discovery_runtime: None,
            },
        };
        let non_expander_item = QueueItem {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: "d".repeat(64),
            enqueued_utc: now,
            priority: crate::DEFAULT_QUEUE_PRIORITY,
            action: ActionSpec::DriverAigToStats {
                aig_action_id: "e".repeat(64),
                version: "v0.35.0".to_string(),
                runtime: driver_runtime,
            },
        };

        for item in [&expander_item, &non_expander_item] {
            let path = store.pending_queue_path(&item.action_id);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("create pending queue parent");
            }
            fs::write(
                &path,
                serde_json::to_string_pretty(item).expect("serialize pending record"),
            )
            .expect("write pending queue record");
        }

        let status = build_queue_live_status(&store, &root, None).expect("build queue live status");
        assert_eq!(status.pending, 2);
        assert_eq!(status.pending_expanders, 1);
        assert_eq!(status.pending_non_expanders, 1);
        assert!(status.pending_is_lower_bound);
        assert_eq!(status.running, 0);
        assert_eq!(status.running_expanders, 0);
        assert_eq!(status.running_non_expanders, 0);

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn action_subject_for_yosys_abc_includes_script_label() {
        let action = ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id: "f".repeat(64),
            verilog_top_module_name: Some("__float32__add".to_string()),
            yosys_script_ref: ScriptRef {
                path: "flows/yosys_to_aig.ys".to_string(),
                sha256: "0".repeat(64),
            },
            runtime: crate::runtime::default_yosys_runtime(),
        };
        assert_eq!(
            action_subject(&action),
            "verilog=ffffffffffff top=__float32__add script=yosys_to_aig.ys"
        );
    }

    #[test]
    fn action_graph_node_label_for_yosys_abc_includes_script_label() {
        let action = ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id: "f".repeat(64),
            verilog_top_module_name: Some("__float32__add".to_string()),
            yosys_script_ref: ScriptRef {
                path: "flows/ablate_abc_fast.ys".to_string(),
                sha256: "0".repeat(64),
            },
            runtime: crate::runtime::default_yosys_runtime(),
        };
        assert_eq!(
            action_graph_node_label(&action),
            "yosys-abc\n__float32__add\nablate_abc_fast.ys"
        );
    }

    #[test]
    fn extract_stdlib_trend_source_context_yosys_rejects_noncanonical_script() {
        let runtime = test_runtime();
        let ir_action_id = "a".repeat(64);
        let verilog_action_id = "b".repeat(64);
        let yosys_action_id = "c".repeat(64);
        let mut provenance_by_action_id = BTreeMap::new();
        provenance_by_action_id.insert(
            verilog_action_id.clone(),
            synthetic_provenance(
                &verilog_action_id,
                ActionSpec::IrFnToCombinationalVerilog {
                    ir_action_id: ir_action_id.clone(),
                    top_fn_name: Some("__float32__add".to_string()),
                    use_system_verilog: false,
                    version: "0.35.0".to_string(),
                    runtime: runtime.clone(),
                },
                ArtifactType::VerilogFile,
            ),
        );
        provenance_by_action_id.insert(
            yosys_action_id.clone(),
            synthetic_provenance(
                &yosys_action_id,
                ActionSpec::ComboVerilogToYosysAbcAig {
                    verilog_action_id: verilog_action_id.clone(),
                    verilog_top_module_name: Some("__float32__add".to_string()),
                    yosys_script_ref: ScriptRef {
                        path: "flows/ablate_abc_fast.ys".to_string(),
                        sha256: "0".repeat(64),
                    },
                    runtime: crate::runtime::default_yosys_runtime(),
                },
                ArtifactType::AigFile,
            ),
        );
        let lookup = borrowed_lookup(&provenance_by_action_id);
        let ctx = extract_stdlib_trend_source_context(
            &lookup,
            StdlibTrendKind::YosysAbc,
            &yosys_action_id,
            false,
        );
        assert!(ctx.is_none());
    }

    #[test]
    fn extract_stdlib_trend_source_context_yosys_accepts_canonical_script() {
        let runtime = test_runtime();
        let ir_action_id = "d".repeat(64);
        let verilog_action_id = "e".repeat(64);
        let yosys_action_id = "f".repeat(64);
        let mut provenance_by_action_id = BTreeMap::new();
        provenance_by_action_id.insert(
            verilog_action_id.clone(),
            synthetic_provenance(
                &verilog_action_id,
                ActionSpec::IrFnToCombinationalVerilog {
                    ir_action_id: ir_action_id.clone(),
                    top_fn_name: Some("__float32__add".to_string()),
                    use_system_verilog: false,
                    version: "0.35.0".to_string(),
                    runtime: runtime.clone(),
                },
                ArtifactType::VerilogFile,
            ),
        );
        provenance_by_action_id.insert(
            yosys_action_id.clone(),
            synthetic_provenance(
                &yosys_action_id,
                ActionSpec::ComboVerilogToYosysAbcAig {
                    verilog_action_id: verilog_action_id.clone(),
                    verilog_top_module_name: Some("__float32__add".to_string()),
                    yosys_script_ref: ScriptRef {
                        path: crate::DEFAULT_YOSYS_FLOW_SCRIPT.to_string(),
                        sha256: "0".repeat(64),
                    },
                    runtime: crate::runtime::default_yosys_runtime(),
                },
                ArtifactType::AigFile,
            ),
        );
        let lookup = borrowed_lookup(&provenance_by_action_id);
        let ctx = extract_stdlib_trend_source_context(
            &lookup,
            StdlibTrendKind::YosysAbc,
            &yosys_action_id,
            false,
        )
        .expect("canonical yosys script should be accepted");
        assert_eq!(ctx.ir_action_id, ir_action_id);
        assert_eq!(ctx.ir_top.as_deref(), Some("__float32__add"));
        assert_eq!(ctx.crate_version, "0.31.0");
        assert_eq!(ctx.dso_version, "0.35.0");
    }

    #[test]
    fn stdlib_file_action_graph_hides_k3_descendants_by_default() {
        let (store, root) = make_test_store("k3-prune");
        let runtime = test_runtime();
        let ir_id = "1".repeat(64);
        let opt_id = "2".repeat(64);
        let direct_aig_id = "3".repeat(64);
        let k3_id = "4".repeat(64);
        let k3_child_id = "5".repeat(64);

        materialize_test_provenance(
            &store,
            &ir_id,
            ActionSpec::DriverDslxFnToIr {
                dslx_subtree_action_id: "f".repeat(64),
                dslx_file: "xls/dslx/stdlib/float32.x".to_string(),
                dslx_fn_name: "add".to_string(),
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
            ArtifactType::IrPackageFile,
            "payload/ir.ir",
        );
        materialize_test_provenance(
            &store,
            &opt_id,
            ActionSpec::DriverIrToOpt {
                ir_action_id: ir_id.clone(),
                top_fn_name: Some("__float32__add".to_string()),
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
            ArtifactType::IrPackageFile,
            "payload/opt.ir",
        );
        materialize_test_provenance(
            &store,
            &direct_aig_id,
            ActionSpec::DriverIrToG8rAig {
                ir_action_id: opt_id.clone(),
                top_fn_name: Some("__float32__add".to_string()),
                fraig: false,
                lowering_mode: G8rLoweringMode::Default,
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
            ArtifactType::AigFile,
            "payload/direct.aig",
        );
        materialize_test_provenance(
            &store,
            &k3_id,
            ActionSpec::IrFnToKBoolConeCorpus {
                ir_action_id: opt_id.clone(),
                top_fn_name: Some("__float32__add".to_string()),
                k: 3,
                max_ir_ops: Some(16),
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
            ArtifactType::IrPackageFile,
            "payload/k3.ir",
        );
        materialize_test_provenance(
            &store,
            &k3_child_id,
            ActionSpec::DriverIrToG8rAig {
                ir_action_id: k3_id.clone(),
                top_fn_name: Some("__k3_cone_deadbeef".to_string()),
                fraig: false,
                lowering_mode: G8rLoweringMode::Default,
                version: "0.35.0".to_string(),
                runtime,
            },
            ArtifactType::AigFile,
            "payload/k3_child.aig",
        );

        let pruned = build_stdlib_file_action_graph_dataset(
            &store,
            &root,
            Some("0.31.0"),
            Some("xls/dslx/stdlib/float32.x"),
            Some("add"),
            None,
            false,
        )
        .expect("build pruned graph dataset");
        let expanded = build_stdlib_file_action_graph_dataset(
            &store,
            &root,
            Some("0.31.0"),
            Some("xls/dslx/stdlib/float32.x"),
            Some("add"),
            None,
            true,
        )
        .expect("build expanded graph dataset");

        let pruned_ids: BTreeSet<String> =
            pruned.nodes.iter().map(|n| n.action_id.clone()).collect();
        let expanded_ids: BTreeSet<String> =
            expanded.nodes.iter().map(|n| n.action_id.clone()).collect();
        assert!(pruned_ids.contains(&k3_id), "k3 root should remain visible");
        assert!(
            !pruned_ids.contains(&k3_child_id),
            "k3 descendants should be pruned when include_k3_descendants=false"
        );
        assert!(
            expanded_ids.contains(&k3_child_id),
            "k3 descendants should be included when include_k3_descendants=true"
        );

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn stdlib_file_action_graph_index_overlay_includes_runtime_queue_actions() {
        let (store, root) = make_test_store("file-graph-index-overlay");
        let runtime = test_runtime();
        let ir_id = "1".repeat(64);
        let opt_id = "2".repeat(64);
        let ir_top = "__float32__add";

        materialize_test_provenance(
            &store,
            &ir_id,
            ActionSpec::DriverDslxFnToIr {
                dslx_subtree_action_id: "f".repeat(64),
                dslx_file: "xls/dslx/stdlib/float32.x".to_string(),
                dslx_fn_name: "add".to_string(),
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
            ArtifactType::IrPackageFile,
            "payload/ir.ir",
        );
        materialize_test_provenance(
            &store,
            &opt_id,
            ActionSpec::DriverIrToOpt {
                ir_action_id: ir_id.clone(),
                top_fn_name: Some(ir_top.to_string()),
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
            ArtifactType::IrPackageFile,
            "payload/opt.ir",
        );

        let summary = rebuild_stdlib_file_action_graph_dataset_index(&store, &root)
            .expect("rebuild file graph index");
        assert!(
            summary.action_count >= 2,
            "index should include materialized actions"
        );

        let pending_action = ActionSpec::DriverIrToG8rAig {
            ir_action_id: opt_id.clone(),
            top_fn_name: Some(ir_top.to_string()),
            fraig: false,
            lowering_mode: G8rLoweringMode::Default,
            version: "0.35.0".to_string(),
            runtime: runtime.clone(),
        };
        let pending_action_id =
            compute_action_id(&pending_action).expect("compute pending action id");
        enqueue_action_with_priority(&store, pending_action, 0).expect("enqueue pending action");

        let focused = build_stdlib_file_action_graph_dataset(
            &store,
            &root,
            None,
            None,
            None,
            Some(&pending_action_id),
            false,
        )
        .expect("build focused graph from indexed state with queue overlay");
        let focused_ids: BTreeSet<String> =
            focused.nodes.iter().map(|n| n.action_id.clone()).collect();
        assert!(
            focused.action_focus_found,
            "focused pending action should be found via queue overlay"
        );
        assert!(
            focused_ids.contains(&pending_action_id),
            "focused pending action should be present in graph nodes"
        );
        assert!(
            focused_ids.contains(&opt_id),
            "focused pending action should include upstream dependencies"
        );

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn stdlib_fn_timeline_index_roundtrip_supports_fraig_toggle() {
        let (store, root) = make_test_store("timeline-index");
        let runtime = test_runtime();
        let yosys_runtime = default_yosys_runtime();
        let dslx_id = "1".repeat(64);
        let opt_id = "2".repeat(64);
        let g8r_aig_false_id = "3".repeat(64);
        let g8r_stats_false_id = "4".repeat(64);
        let g8r_aig_true_id = "5".repeat(64);
        let g8r_stats_true_id = "6".repeat(64);
        let ir2combo_id = "7".repeat(64);
        let yosys_aig_id = "8".repeat(64);
        let yosys_stats_id = "9".repeat(64);
        let delay_id = "a".repeat(64);
        let dslx_file = "xls/dslx/stdlib/float32.x";
        let dslx_fn_name = "eq_2";
        let ir_top = "__float32__eq_2";

        materialize_test_provenance(
            &store,
            &dslx_id,
            ActionSpec::DriverDslxFnToIr {
                dslx_subtree_action_id: "f".repeat(64),
                dslx_file: dslx_file.to_string(),
                dslx_fn_name: dslx_fn_name.to_string(),
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
            ArtifactType::IrPackageFile,
            "payload/dslx.ir",
        );
        materialize_test_provenance(
            &store,
            &opt_id,
            ActionSpec::DriverIrToOpt {
                ir_action_id: dslx_id.clone(),
                top_fn_name: Some(ir_top.to_string()),
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
            ArtifactType::IrPackageFile,
            "payload/opt.ir",
        );
        materialize_test_provenance(
            &store,
            &g8r_aig_false_id,
            ActionSpec::DriverIrToG8rAig {
                ir_action_id: opt_id.clone(),
                top_fn_name: Some(ir_top.to_string()),
                fraig: false,
                lowering_mode: G8rLoweringMode::Default,
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
            ArtifactType::AigFile,
            "payload/g8r_false.aig",
        );
        materialize_test_provenance(
            &store,
            &g8r_stats_false_id,
            ActionSpec::DriverAigToStats {
                aig_action_id: g8r_aig_false_id.clone(),
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
            ArtifactType::AigStatsFile,
            "payload/g8r_false.stats.json",
        );
        overwrite_test_artifact(
            &store,
            &g8r_stats_false_id,
            ArtifactType::AigStatsFile,
            "payload/g8r_false.stats.json",
            r#"{"and_nodes": 42, "depth": 6}"#,
        );

        materialize_test_provenance(
            &store,
            &g8r_aig_true_id,
            ActionSpec::DriverIrToG8rAig {
                ir_action_id: opt_id.clone(),
                top_fn_name: Some(ir_top.to_string()),
                fraig: true,
                lowering_mode: G8rLoweringMode::Default,
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
            ArtifactType::AigFile,
            "payload/g8r_true.aig",
        );
        materialize_test_provenance(
            &store,
            &g8r_stats_true_id,
            ActionSpec::DriverAigToStats {
                aig_action_id: g8r_aig_true_id.clone(),
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
            ArtifactType::AigStatsFile,
            "payload/g8r_true.stats.json",
        );
        overwrite_test_artifact(
            &store,
            &g8r_stats_true_id,
            ArtifactType::AigStatsFile,
            "payload/g8r_true.stats.json",
            r#"{"and_nodes": 40, "depth": 5}"#,
        );

        materialize_test_provenance(
            &store,
            &ir2combo_id,
            ActionSpec::IrFnToCombinationalVerilog {
                ir_action_id: opt_id.clone(),
                top_fn_name: Some(ir_top.to_string()),
                use_system_verilog: false,
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
            ArtifactType::VerilogFile,
            "payload/combo.v",
        );
        materialize_test_provenance(
            &store,
            &yosys_aig_id,
            ActionSpec::ComboVerilogToYosysAbcAig {
                verilog_action_id: ir2combo_id.clone(),
                verilog_top_module_name: Some(ir_top.to_string()),
                yosys_script_ref: ScriptRef {
                    path: crate::DEFAULT_YOSYS_FLOW_SCRIPT.to_string(),
                    sha256: "0".repeat(64),
                },
                runtime: yosys_runtime,
            },
            ArtifactType::AigFile,
            "payload/yosys.aig",
        );
        materialize_test_provenance(
            &store,
            &yosys_stats_id,
            ActionSpec::DriverAigToStats {
                aig_action_id: yosys_aig_id.clone(),
                version: "0.35.0".to_string(),
                runtime: runtime.clone(),
            },
            ArtifactType::AigStatsFile,
            "payload/yosys.stats.json",
        );
        overwrite_test_artifact(
            &store,
            &yosys_stats_id,
            ArtifactType::AigStatsFile,
            "payload/yosys.stats.json",
            r#"{"and_nodes": 55, "depth": 7}"#,
        );

        materialize_test_provenance(
            &store,
            &delay_id,
            ActionSpec::DriverIrToDelayInfo {
                ir_action_id: opt_id.clone(),
                top_fn_name: Some(ir_top.to_string()),
                delay_model: "asap7".to_string(),
                output_format: crate::DELAY_INFO_OUTPUT_FORMAT_TEXTPROTO_V1.to_string(),
                version: "0.35.0".to_string(),
                runtime,
            },
            ArtifactType::IrDelayInfoFile,
            "payload/delay.textproto",
        );
        overwrite_test_artifact(
            &store,
            &delay_id,
            ArtifactType::IrDelayInfoFile,
            "payload/delay.textproto",
            "total_delay_ps: 123.5\n",
        );

        let summary =
            rebuild_stdlib_fn_version_timeline_dataset_index(&store).expect("rebuild timeline");
        assert!(
            summary.file_count >= 1,
            "expected at least one indexed file"
        );
        assert!(
            summary.fn_count >= 1,
            "expected at least one indexed function"
        );

        let dataset = load_stdlib_fn_version_timeline_dataset_index(
            &store,
            "float32.x",
            dslx_fn_name,
            false,
            "asap7",
        )
        .expect("load timeline index")
        .expect("timeline index dataset should exist");
        assert_eq!(dataset.dslx_file.as_deref(), Some(dslx_file));
        assert_eq!(dataset.points.len(), 1);
        assert_eq!(dataset.points[0].g8r_and_nodes, Some(42.0));
        assert_eq!(dataset.points[0].yosys_abc_and_nodes, Some(55.0));
        assert_eq!(dataset.points[0].ir_delay_ps, Some(123.5));

        let dataset_fraig_true = load_stdlib_fn_version_timeline_dataset_index(
            &store,
            "float32.x",
            dslx_fn_name,
            true,
            "asap7",
        )
        .expect("load timeline index with fraig")
        .expect("timeline index dataset should exist for fraig");
        assert_eq!(dataset_fraig_true.points.len(), 1);
        assert_eq!(dataset_fraig_true.points[0].g8r_and_nodes, Some(40.0));
        assert_eq!(dataset_fraig_true.points[0].yosys_abc_and_nodes, Some(55.0));

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn stdlib_g8r_vs_yosys_index_roundtrip() {
        let (store, root) = make_test_store("stdlib-g8r-vs-yosys-index");
        let dataset = StdlibG8rVsYosysDataset {
            fraig: false,
            samples: vec![StdlibG8rVsYosysSample {
                fn_key: "xls/dslx/stdlib/float32.x::eq_2".to_string(),
                crate_version: "0.31.0".to_string(),
                dso_version: "0.35.0".to_string(),
                ir_action_id: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string(),
                ir_top: Some("__float32__eq_2".to_string()),
                structural_hash: None,
                ir_node_count: 10,
                g8r_nodes: 42.0,
                g8r_levels: 6.0,
                yosys_abc_nodes: 55.0,
                yosys_abc_levels: 7.0,
                g8r_product: 252.0,
                yosys_abc_product: 385.0,
                g8r_product_loss: -133.0,
                g8r_stats_action_id:
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                yosys_abc_stats_action_id:
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            }],
            min_ir_nodes: 10,
            max_ir_nodes: 10,
            g8r_only_count: 0,
            yosys_only_count: 0,
            available_crate_versions: vec!["0.31.0".to_string()],
        };

        let (location, _, _) =
            write_stdlib_g8r_vs_yosys_dataset_index(&store, &dataset).expect("write index");
        assert!(!location.is_empty(), "index location should be non-empty");
        let loaded = load_stdlib_g8r_vs_yosys_dataset_index(&store, false)
            .expect("load index")
            .expect("index should exist");
        assert_eq!(loaded.fraig, false);
        assert_eq!(loaded.samples.len(), 1);
        assert_eq!(
            loaded.samples[0].fn_key,
            "xls/dslx/stdlib/float32.x::eq_2".to_string()
        );

        let missing_fraig_true =
            load_stdlib_g8r_vs_yosys_dataset_index(&store, true).expect("load other fraig");
        assert!(
            missing_fraig_true.is_none(),
            "fraig=true index should be absent when only fraig=false was written"
        );

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn filter_ir_fn_corpus_g8r_vs_yosys_samples_applies_thresholds() {
        let dataset = StdlibG8rVsYosysDataset {
            fraig: false,
            samples: vec![
                StdlibG8rVsYosysSample {
                    fn_key: "f::a".to_string(),
                    crate_version: "0.33.0".to_string(),
                    dso_version: "0.35.0".to_string(),
                    ir_action_id: "a".repeat(64),
                    ir_top: Some("__a".to_string()),
                    structural_hash: Some("0".repeat(64)),
                    ir_node_count: 15,
                    g8r_nodes: 10.0,
                    g8r_levels: 3.0,
                    yosys_abc_nodes: 12.0,
                    yosys_abc_levels: 5.0,
                    g8r_product: 30.0,
                    yosys_abc_product: 60.0,
                    g8r_product_loss: -30.0,
                    g8r_stats_action_id: "b".repeat(64),
                    yosys_abc_stats_action_id: "c".repeat(64),
                },
                StdlibG8rVsYosysSample {
                    fn_key: "f::b".to_string(),
                    crate_version: "0.33.0".to_string(),
                    dso_version: "0.35.0".to_string(),
                    ir_action_id: "d".repeat(64),
                    ir_top: Some("__b".to_string()),
                    structural_hash: Some("1".repeat(64)),
                    ir_node_count: 99,
                    g8r_nodes: 10.0,
                    g8r_levels: 3.0,
                    yosys_abc_nodes: 12.0,
                    yosys_abc_levels: 4.0,
                    g8r_product: 30.0,
                    yosys_abc_product: 48.0,
                    g8r_product_loss: -18.0,
                    g8r_stats_action_id: "e".repeat(64),
                    yosys_abc_stats_action_id: "f".repeat(64),
                },
                StdlibG8rVsYosysSample {
                    fn_key: "f::c".to_string(),
                    crate_version: "0.32.0".to_string(),
                    dso_version: "0.35.0".to_string(),
                    ir_action_id: "0".repeat(64),
                    ir_top: Some("__c".to_string()),
                    structural_hash: Some("2".repeat(64)),
                    ir_node_count: 5,
                    g8r_nodes: 10.0,
                    g8r_levels: 3.0,
                    yosys_abc_nodes: 12.0,
                    yosys_abc_levels: 3.0,
                    g8r_product: 30.0,
                    yosys_abc_product: 36.0,
                    g8r_product_loss: -6.0,
                    g8r_stats_action_id: "1".repeat(64),
                    yosys_abc_stats_action_id: "2".repeat(64),
                },
            ],
            min_ir_nodes: 5,
            max_ir_nodes: 99,
            g8r_only_count: 0,
            yosys_only_count: 0,
            available_crate_versions: vec!["0.32.0".to_string(), "0.33.0".to_string()],
        };

        let filtered = filter_ir_fn_corpus_g8r_vs_yosys_samples(&dataset, "v0.33.0", 6.0, 50);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].fn_key, "f::a");
    }

    #[test]
    fn build_ir_fn_corpus_dataset_uses_source_ir_node_counts_before_ir_reads() {
        let (store, root) = make_test_store("ir-fn-corpus-source-ir-node-counts");
        let structural_hash = "a".repeat(64);
        let crate_version = "0.34.0".to_string();
        let g8r_ir_action_id = "1".repeat(64);
        let yosys_ir_action_id = "2".repeat(64);
        let ir_top = Some("__foo".to_string());

        let g8r_by_entity = BTreeMap::from([(
            (structural_hash.clone(), crate_version.clone()),
            StdlibAigStatsPoint {
                fn_key: "foo".to_string(),
                ir_action_id: g8r_ir_action_id.clone(),
                ir_top: ir_top.clone(),
                crate_version: crate_version.clone(),
                dso_version: "0.35.0".to_string(),
                and_nodes: 10.0,
                depth: 2.0,
                created_utc: DateTime::<Utc>::UNIX_EPOCH,
                stats_action_id: "3".repeat(64),
            },
        )]);
        let yosys_by_entity = BTreeMap::from([(
            (structural_hash, crate_version.clone()),
            StdlibAigStatsPoint {
                fn_key: "foo".to_string(),
                ir_action_id: yosys_ir_action_id.clone(),
                ir_top: ir_top.clone(),
                crate_version: crate_version.clone(),
                dso_version: "0.35.0".to_string(),
                and_nodes: 8.0,
                depth: 2.0,
                created_utc: DateTime::<Utc>::UNIX_EPOCH,
                stats_action_id: "4".repeat(64),
            },
        )]);
        let ir_node_count_by_source_ir_action_and_top = BTreeMap::from([
            (
                g8r_ir_action_id,
                BTreeMap::from([("__foo".to_string(), 123_u64)]),
            ),
            (
                yosys_ir_action_id,
                BTreeMap::from([("__foo".to_string(), 123_u64)]),
            ),
        ]);

        let dataset = build_ir_fn_corpus_dataset_from_entity_maps(
            &store,
            &g8r_by_entity,
            &yosys_by_entity,
            None,
            Some(&ir_node_count_by_source_ir_action_and_top),
            None,
            None,
        );

        assert_eq!(dataset.samples.len(), 1);
        assert_eq!(dataset.samples[0].crate_version, crate_version);
        assert_eq!(dataset.samples[0].ir_node_count, 123);

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn build_ir_fn_corpus_k3_loss_by_version_dataset_tracks_statuses() {
        let improved_hash = "a".repeat(64);
        let same_hash = "b".repeat(64);
        let newly_lossy_hash = "c".repeat(64);
        let resolved_hash = "d".repeat(64);
        let missing_current_hash = "e".repeat(64);
        let regressed_hash = "f".repeat(64);
        let dataset = StdlibG8rVsYosysDataset {
            fraig: false,
            samples: vec![
                make_ir_fn_corpus_sample(
                    &improved_hash,
                    "0.35.0",
                    "__k3_cone_improved",
                    "float64::fma",
                    6,
                    12.0,
                ),
                make_ir_fn_corpus_sample(
                    &improved_hash,
                    "0.39.0",
                    "__k3_cone_improved",
                    "float64::fma",
                    6,
                    4.0,
                ),
                make_ir_fn_corpus_sample(
                    &same_hash,
                    "0.34.0",
                    "__k3_cone_same",
                    "float32::fma",
                    7,
                    9.0,
                ),
                make_ir_fn_corpus_sample(
                    &same_hash,
                    "0.39.0",
                    "__k3_cone_same",
                    "float32::fma",
                    7,
                    9.0,
                ),
                make_ir_fn_corpus_sample(
                    &newly_lossy_hash,
                    "0.35.0",
                    "__k3_cone_new",
                    "float32::fast_rsqrt",
                    8,
                    0.0,
                ),
                make_ir_fn_corpus_sample(
                    &newly_lossy_hash,
                    "0.39.0",
                    "__k3_cone_new",
                    "float32::fast_rsqrt",
                    8,
                    5.0,
                ),
                make_ir_fn_corpus_sample(
                    &resolved_hash,
                    "0.35.0",
                    "__k3_cone_resolved",
                    "bfloat16::fma",
                    9,
                    11.0,
                ),
                make_ir_fn_corpus_sample(
                    &resolved_hash,
                    "0.39.0",
                    "__k3_cone_resolved",
                    "bfloat16::fma",
                    9,
                    -2.0,
                ),
                make_ir_fn_corpus_sample(
                    &missing_current_hash,
                    "0.34.0",
                    "__k3_cone_missing",
                    "float64::mul",
                    10,
                    7.0,
                ),
                make_ir_fn_corpus_sample(
                    &regressed_hash,
                    "0.35.0",
                    "__k3_cone_regressed",
                    "float64::sub",
                    11,
                    3.0,
                ),
                make_ir_fn_corpus_sample(
                    &regressed_hash,
                    "0.39.0",
                    "__k3_cone_regressed",
                    "float64::sub",
                    11,
                    8.0,
                ),
                make_ir_fn_corpus_sample(
                    &"g".repeat(64),
                    "0.39.0",
                    "__not_k3",
                    "non_k3::ignored",
                    5,
                    100.0,
                ),
            ],
            min_ir_nodes: 5,
            max_ir_nodes: 11,
            g8r_only_count: 0,
            yosys_only_count: 0,
            available_crate_versions: vec![
                "0.34.0".to_string(),
                "0.35.0".to_string(),
                "0.39.0".to_string(),
            ],
        };

        let hidden_same_view = build_ir_fn_corpus_k3_loss_by_version_dataset(
            &dataset,
            Some("v0.39.0"),
            Some(11),
            false,
        );
        assert_eq!(
            hidden_same_view.selected_crate_version.as_deref(),
            Some("0.39.0")
        );
        assert_eq!(hidden_same_view.status_counts.same, 1);
        assert!(
            hidden_same_view
                .rows
                .iter()
                .all(|row| row.status != K3LossChangeStatus::Same)
        );

        let view = build_ir_fn_corpus_k3_loss_by_version_dataset(
            &dataset,
            Some("v0.39.0"),
            Some(11),
            true,
        );
        assert_eq!(view.selected_crate_version.as_deref(), Some("0.39.0"));
        assert_eq!(
            view.compared_crate_versions,
            vec![
                "0.34.0".to_string(),
                "0.35.0".to_string(),
                "0.39.0".to_string()
            ]
        );
        assert_eq!(view.status_counts.regressed, 1);
        assert_eq!(view.status_counts.newly_lossy, 1);
        assert_eq!(view.status_counts.same, 1);
        assert_eq!(view.status_counts.improved, 1);
        assert_eq!(view.status_counts.resolved, 1);
        assert_eq!(view.status_counts.missing_current, 1);
        let status_by_hash = view
            .rows
            .iter()
            .map(|row| (row.structural_hash.clone(), row.status))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            status_by_hash.get(&improved_hash),
            Some(&K3LossChangeStatus::Improved)
        );
        assert_eq!(
            status_by_hash.get(&same_hash),
            Some(&K3LossChangeStatus::Same)
        );
        assert_eq!(
            status_by_hash.get(&newly_lossy_hash),
            Some(&K3LossChangeStatus::NewlyLossy)
        );
        assert_eq!(
            status_by_hash.get(&resolved_hash),
            Some(&K3LossChangeStatus::Resolved)
        );
        assert_eq!(
            status_by_hash.get(&missing_current_hash),
            Some(&K3LossChangeStatus::MissingCurrent)
        );
        assert_eq!(
            status_by_hash.get(&regressed_hash),
            Some(&K3LossChangeStatus::Regressed)
        );
        let same_row = view
            .rows
            .iter()
            .find(|row| row.structural_hash == same_hash)
            .expect("same row");
        assert_eq!(
            same_row
                .previous_sample
                .as_ref()
                .map(|sample| sample.crate_version.as_str()),
            Some("0.34.0")
        );
        assert_eq!(
            same_row
                .current_sample
                .as_ref()
                .map(|sample| sample.crate_version.as_str()),
            Some("0.39.0")
        );
        assert!(view.show_same);
        assert!(!hidden_same_view.show_same);
    }

    #[test]
    fn build_ir_fn_corpus_structural_coverage_tracks_losses_and_ir_nodes() {
        let structural_hash_a = "a".repeat(64);
        let structural_hash_b = "b".repeat(64);
        let dataset = StdlibG8rVsYosysDataset {
            fraig: false,
            samples: vec![
                StdlibG8rVsYosysSample {
                    fn_key: "f::a".to_string(),
                    crate_version: "0.33.0".to_string(),
                    dso_version: "0.35.0".to_string(),
                    ir_action_id: "0".repeat(64),
                    ir_top: Some("__a".to_string()),
                    structural_hash: Some(structural_hash_a.clone()),
                    ir_node_count: 17,
                    g8r_nodes: 10.0,
                    g8r_levels: 5.0,
                    yosys_abc_nodes: 8.0,
                    yosys_abc_levels: 4.0,
                    g8r_product: 50.0,
                    yosys_abc_product: 32.0,
                    g8r_product_loss: 18.0,
                    g8r_stats_action_id: "1".repeat(64),
                    yosys_abc_stats_action_id: "2".repeat(64),
                },
                StdlibG8rVsYosysSample {
                    fn_key: "f::a2".to_string(),
                    crate_version: "0.32.0".to_string(),
                    dso_version: "0.35.0".to_string(),
                    ir_action_id: "3".repeat(64),
                    ir_top: Some("__a2".to_string()),
                    structural_hash: Some(structural_hash_a.clone()),
                    ir_node_count: 17,
                    g8r_nodes: 4.0,
                    g8r_levels: 3.0,
                    yosys_abc_nodes: 5.0,
                    yosys_abc_levels: 3.0,
                    g8r_product: 12.0,
                    yosys_abc_product: 15.0,
                    g8r_product_loss: -3.0,
                    g8r_stats_action_id: "4".repeat(64),
                    yosys_abc_stats_action_id: "5".repeat(64),
                },
                StdlibG8rVsYosysSample {
                    fn_key: "f::b".to_string(),
                    crate_version: "0.33.0".to_string(),
                    dso_version: "0.35.0".to_string(),
                    ir_action_id: "6".repeat(64),
                    ir_top: Some("__b".to_string()),
                    structural_hash: Some(structural_hash_b.clone()),
                    ir_node_count: 0,
                    g8r_nodes: 4.0,
                    g8r_levels: 4.0,
                    yosys_abc_nodes: 4.0,
                    yosys_abc_levels: 4.0,
                    g8r_product: 16.0,
                    yosys_abc_product: 16.0,
                    g8r_product_loss: 0.0,
                    g8r_stats_action_id: "7".repeat(64),
                    yosys_abc_stats_action_id: "8".repeat(64),
                },
            ],
            min_ir_nodes: 0,
            max_ir_nodes: 17,
            g8r_only_count: 0,
            yosys_only_count: 0,
            available_crate_versions: vec!["0.32.0".to_string(), "0.33.0".to_string()],
        };

        let coverage = build_ir_fn_corpus_structural_coverage(&dataset);
        assert_eq!(
            coverage
                .g8r_loss_sample_count_by_structural_hash
                .get(&structural_hash_a)
                .copied(),
            Some(1)
        );
        assert!(
            !coverage
                .g8r_loss_sample_count_by_structural_hash
                .contains_key(&structural_hash_b)
        );
        assert_eq!(
            coverage
                .ir_node_count_by_structural_hash
                .get(&structural_hash_a)
                .copied(),
            Some(17)
        );
        assert!(
            !coverage
                .ir_node_count_by_structural_hash
                .contains_key(&structural_hash_b)
        );
    }

    #[test]
    fn normalize_action_output_relpath_rejects_invalid_paths() {
        assert_eq!(
            normalize_action_output_relpath("payload/result.ir"),
            Some("payload/result.ir".to_string())
        );
        assert_eq!(
            normalize_action_output_relpath("payload\\result.ir"),
            Some("payload/result.ir".to_string())
        );
        assert_eq!(normalize_action_output_relpath("../escape"), None);
        assert_eq!(normalize_action_output_relpath("/abs/path"), None);
        assert_eq!(normalize_action_output_relpath(""), None);
    }

    #[test]
    fn is_valid_action_id_for_route_requires_64_hex_chars() {
        assert!(is_valid_action_id_for_route(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ));
        assert!(!is_valid_action_id_for_route("0123"));
        assert!(!is_valid_action_id_for_route(
            "zzzz456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ));
    }

    #[test]
    fn infer_first_ir_function_name_prefers_top_fn() {
        let ir = r#"package p

top fn foo(x: bits[1] id=1) -> bits[1] {
  ret x: bits[1] = identity(x, id=2)
}

fn bar(y: bits[1] id=3) -> bits[1] {
  ret y: bits[1] = identity(y, id=4)
}
"#;
        assert_eq!(infer_first_ir_function_name(ir).as_deref(), Some("foo"));
    }

    #[test]
    fn infer_first_ir_function_name_handles_non_top_packages() {
        let ir = r#"package p

fn only(z: bits[1] id=1) -> bits[1] {
  ret z: bits[1] = identity(z, id=2)
}
"#;
        assert_eq!(infer_first_ir_function_name(ir).as_deref(), Some("only"));
    }

    #[test]
    fn ir_fn_corpus_web_index_roundtrip() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-query-index-test-{}-{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).expect("create temp store root");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure store layout");

        let dataset = StdlibG8rVsYosysDataset {
            fraig: false,
            samples: vec![StdlibG8rVsYosysSample {
                fn_key: "float32.x:add".to_string(),
                crate_version: "0.31.0".to_string(),
                dso_version: "0.35.0".to_string(),
                ir_action_id: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string(),
                ir_top: Some("__float32__add".to_string()),
                structural_hash: Some(
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                ),
                ir_node_count: 123,
                g8r_nodes: 45.0,
                g8r_levels: 10.0,
                yosys_abc_nodes: 47.0,
                yosys_abc_levels: 11.0,
                g8r_product: 450.0,
                yosys_abc_product: 517.0,
                g8r_product_loss: -67.0,
                g8r_stats_action_id:
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                yosys_abc_stats_action_id:
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            }],
            min_ir_nodes: 123,
            max_ir_nodes: 123,
            g8r_only_count: 0,
            yosys_only_count: 0,
            available_crate_versions: vec!["0.31.0".to_string()],
        };

        let (g8r_points, yosys_points) = paired_entity_maps_from_dataset(&dataset);
        let (location, _, _) = write_ir_fn_corpus_g8r_vs_yosys_index_state(
            &store,
            &dataset,
            &g8r_points,
            &yosys_points,
        )
        .expect("write web index");
        assert!(!location.is_empty(), "index location should be non-empty");
        let loaded = load_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store)
            .expect("load web index")
            .expect("index should be present");
        assert_eq!(loaded.samples.len(), 1);
        assert_eq!(loaded.samples[0].fn_key, "float32.x:add");
        assert_eq!(loaded.available_crate_versions, vec!["0.31.0".to_string()]);

        fs::remove_dir_all(&root).expect("remove temp store root");
    }

    #[test]
    fn ir_fn_corpus_web_index_delta_overlay_applies() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-query-index-delta-overlay-test-{}-{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).expect("create temp store root");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure store layout");

        let structural_hash =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string();
        let dataset = StdlibG8rVsYosysDataset {
            fraig: false,
            samples: vec![StdlibG8rVsYosysSample {
                fn_key: "fn [0123456789ab]".to_string(),
                crate_version: "0.31.0".to_string(),
                dso_version: "0.35.0".to_string(),
                ir_action_id: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string(),
                ir_top: Some("__float32__add".to_string()),
                structural_hash: Some(structural_hash.clone()),
                ir_node_count: 123,
                g8r_nodes: 45.0,
                g8r_levels: 10.0,
                yosys_abc_nodes: 47.0,
                yosys_abc_levels: 11.0,
                g8r_product: 450.0,
                yosys_abc_product: 517.0,
                g8r_product_loss: -67.0,
                g8r_stats_action_id:
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                yosys_abc_stats_action_id:
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            }],
            min_ir_nodes: 123,
            max_ir_nodes: 123,
            g8r_only_count: 0,
            yosys_only_count: 0,
            available_crate_versions: vec!["0.31.0".to_string()],
        };

        let (g8r_points, yosys_points) = paired_entity_maps_from_dataset(&dataset);
        write_ir_fn_corpus_g8r_vs_yosys_index_state(&store, &dataset, &g8r_points, &yosys_points)
            .expect("write web index");

        let delta = IrFnCorpusStatsUpsertPoint {
            source_kind: IrFnCorpusStatsSourceKind::G8r,
            structural_hash: structural_hash.clone(),
            crate_version: "0.31.0".to_string(),
            point: StdlibAigStatsPoint {
                fn_key: "curated fn label".to_string(),
                ir_action_id: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string(),
                ir_top: Some("__float32__add".to_string()),
                crate_version: "0.31.0".to_string(),
                dso_version: "0.35.0".to_string(),
                and_nodes: 40.0,
                depth: 9.0,
                created_utc: Utc::now(),
                stats_action_id: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    .to_string(),
            },
        };
        write_ir_fn_corpus_incremental_delta_row(
            &store,
            WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME,
            &delta,
        )
        .expect("write incremental delta");

        let loaded = load_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store)
            .expect("load web index")
            .expect("index should be present");
        assert_eq!(loaded.samples.len(), 1);
        assert_eq!(loaded.samples[0].g8r_nodes, 40.0);
        assert_eq!(loaded.samples[0].g8r_levels, 9.0);
        assert_eq!(loaded.samples[0].yosys_abc_nodes, 47.0);
        assert_eq!(loaded.samples[0].yosys_abc_levels, 11.0);

        fs::remove_dir_all(&root).expect("remove temp store root");
    }

    #[test]
    fn ir_fn_corpus_web_index_can_load_from_delta_only_rows() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-query-index-delta-only-test-{}-{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).expect("create temp store root");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure store layout");

        let structural_hash =
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_string();
        let crate_version = "0.32.0".to_string();
        let ir_action_id =
            "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210".to_string();
        let ir_top = Some("__float32__mul".to_string());
        let dso_version = "0.38.0".to_string();

        let g8r_delta = IrFnCorpusStatsUpsertPoint {
            source_kind: IrFnCorpusStatsSourceKind::G8r,
            structural_hash: structural_hash.clone(),
            crate_version: crate_version.clone(),
            point: StdlibAigStatsPoint {
                fn_key: "__float32__mul [abcdef012345]".to_string(),
                ir_action_id: ir_action_id.clone(),
                ir_top: ir_top.clone(),
                crate_version: crate_version.clone(),
                dso_version: dso_version.clone(),
                and_nodes: 100.0,
                depth: 20.0,
                created_utc: Utc::now(),
                stats_action_id: "1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
            },
        };
        let yosys_delta = IrFnCorpusStatsUpsertPoint {
            source_kind: IrFnCorpusStatsSourceKind::YosysAbc,
            structural_hash: structural_hash.clone(),
            crate_version: crate_version.clone(),
            point: StdlibAigStatsPoint {
                fn_key: "__float32__mul [abcdef012345]".to_string(),
                ir_action_id: ir_action_id.clone(),
                ir_top: ir_top.clone(),
                crate_version: crate_version.clone(),
                dso_version: dso_version.clone(),
                and_nodes: 120.0,
                depth: 22.0,
                created_utc: Utc::now(),
                stats_action_id: "2222222222222222222222222222222222222222222222222222222222222222"
                    .to_string(),
            },
        };

        write_ir_fn_corpus_incremental_delta_row(
            &store,
            WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME,
            &g8r_delta,
        )
        .expect("write g8r delta");
        write_ir_fn_corpus_incremental_delta_row(
            &store,
            WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME,
            &yosys_delta,
        )
        .expect("write yosys delta");

        let loaded = load_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store)
            .expect("load web index")
            .expect("index should be present");
        assert_eq!(loaded.samples.len(), 1);
        let sample = &loaded.samples[0];
        assert_eq!(sample.crate_version, crate_version);
        assert_eq!(
            sample.structural_hash.as_deref(),
            Some(structural_hash.as_str())
        );
        assert_eq!(sample.g8r_nodes, 100.0);
        assert_eq!(sample.yosys_abc_nodes, 120.0);

        fs::remove_dir_all(&root).expect("remove temp store root");
    }

    #[test]
    fn ir_fn_corpus_web_index_load_returns_none_when_missing() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-query-index-missing-test-{}-{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).expect("create temp store root");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure store layout");

        let loaded =
            load_ir_fn_corpus_g8r_vs_yosys_dataset_index(&store).expect("load missing web index");
        assert!(loaded.is_none());

        fs::remove_dir_all(&root).expect("remove temp store root");
    }

    #[test]
    fn lookup_ir_fn_corpus_by_fn_name_returns_match_with_ir_block() {
        let (store, root) = make_test_store("ir-fn-corpus-lookup");
        let structural_hash = "f".repeat(64);
        let opt_ir_action_id = "1".repeat(64);
        let source_ir_action_id = "2".repeat(64);
        let ir_top = "__k3_cone_f139d8559988ff4e".to_string();
        let artifact_ref = ArtifactRef {
            action_id: opt_ir_action_id.clone(),
            artifact_type: ArtifactType::IrPackageFile,
            relpath: "payload/k3cones.ir".to_string(),
        };
        let artifact_path = store.resolve_artifact_ref_path(&artifact_ref);
        if let Some(parent) = artifact_path.parent() {
            fs::create_dir_all(parent).expect("create artifact parent");
        }
        let ir_text = format!(
            "package p\n\nfn helper(x: bits[1] id=1) -> bits[1] {{\n  ret x: bits[1] = identity(x, id=2)\n}}\n\nfn {ir_top}(x: bits[1] id=3) -> bits[1] {{\n  ret result: bits[1] = not(x, id=4)\n}}\n"
        );
        fs::write(&artifact_path, &ir_text).expect("write sample IR");
        let artifact_bytes = fs::metadata(&artifact_path).expect("stat sample IR").len();

        let member = IrFnCorpusStructuralMember {
            opt_ir_action_id: opt_ir_action_id.clone(),
            source_ir_action_id: source_ir_action_id.clone(),
            ir_top: ir_top.clone(),
            ir_fn_signature: None,
            ir_op_count: None,
            crate_version: "0.31.0".to_string(),
            dso_version: "0.35.0".to_string(),
            created_utc: Utc::now(),
            output_artifact: artifact_ref,
            output_file_sha256: "0".repeat(64),
            output_file_bytes: artifact_bytes,
            hash_source: "hint".to_string(),
            hash_hint_source_action_ids: vec![],
            dslx_origin: None,
            producer_action_kind: Some("ir_fn_to_k_bool_cone_corpus".to_string()),
        };
        let group = IrFnCorpusStructuralGroupFile {
            schema_version: crate::IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION,
            structural_hash: structural_hash.clone(),
            members: vec![member],
        };

        let group_relpath = hash_group_relpath(&structural_hash);
        let group_key = ir_fn_corpus_structural_group_index_key(&structural_hash);
        store
            .write_web_index_bytes(
                &group_key,
                &serde_json::to_vec_pretty(&group).expect("serialize group"),
            )
            .expect("write group key");

        let manifest = IrFnCorpusStructuralManifest {
            schema_version: crate::IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION,
            generated_utc: Utc::now(),
            store_root: root.display().to_string(),
            output_dir: ir_fn_corpus_structural_index_location(
                &ir_fn_corpus_structural_index_prefix(),
            ),
            recompute_missing_hashes: false,
            total_actions_scanned: 1,
            total_driver_ir_to_opt_actions: 1,
            total_ir_fn_to_k_bool_cone_corpus_actions: 1,
            indexed_actions: 1,
            indexed_k_bool_cone_members: 1,
            distinct_structural_hashes: 1,
            hash_from_dependency_hint_count: 1,
            hash_recomputed_count: 0,
            hash_hint_conflict_count: 0,
            skipped_missing_output_count: 0,
            skipped_missing_ir_top_count: 0,
            skipped_missing_hash_hint_count: 0,
            skipped_hash_error_count: 0,
            skipped_k_bool_cone_manifest_errors: 0,
            skipped_k_bool_cone_empty_count: 0,
            source_action_set_sha256: None,
            groups: vec![IrFnCorpusStructuralManifestGroup {
                structural_hash: structural_hash.clone(),
                member_count: 1,
                relpath: group_relpath,
                ir_node_count: Some(1),
            }],
        };
        store
            .write_web_index_bytes(
                ir_fn_corpus_structural_manifest_index_key(),
                &serde_json::to_vec_pretty(&manifest).expect("serialize manifest"),
            )
            .expect("write manifest key");

        let loaded_manifest =
            load_ir_fn_corpus_structural_manifest(&store).expect("load written manifest");
        let rows = lookup_ir_fn_corpus_by_fn_name(
            &store,
            &loaded_manifest,
            "__k3_cone_f139d8559988ff4e",
            256,
        )
        .expect("lookup rows");
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.structural_hash, structural_hash);
        assert_eq!(row.member.member.opt_ir_action_id, opt_ir_action_id);
        assert_eq!(row.member.member.source_ir_action_id, source_ir_action_id);
        assert_eq!(row.member.member.ir_top, ir_top);
        let ir_fn_text = row
            .ir_fn_text
            .as_deref()
            .expect("lookup should include IR fn");
        assert!(
            ir_fn_text.contains("fn __k3_cone_f139d8559988ff4e("),
            "missing function declaration in extracted IR block: {}",
            ir_fn_text
        );
        assert!(
            ir_fn_text.contains("ret result: bits[1] = not(x, id=4)"),
            "missing function body in extracted IR block: {}",
            ir_fn_text
        );

        let missing_rows =
            lookup_ir_fn_corpus_by_fn_name(&store, &loaded_manifest, "__k3_cone_missing", 256)
                .expect("lookup missing rows");
        assert!(missing_rows.is_empty());

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn structural_archive_is_built_and_then_served_from_cache() {
        let (store, root) = make_test_store("ir-fn-corpus-archive");
        let structural_hash = "e".repeat(64);
        let opt_ir_action_id = "1".repeat(64);
        let source_ir_action_id = "2".repeat(64);
        let ir_top = "__float32__add".to_string();
        let artifact_ref = ArtifactRef {
            action_id: opt_ir_action_id.clone(),
            artifact_type: ArtifactType::IrPackageFile,
            relpath: "payload/opt.ir".to_string(),
        };
        let artifact_path = store.resolve_artifact_ref_path(&artifact_ref);
        if let Some(parent) = artifact_path.parent() {
            fs::create_dir_all(parent).expect("create artifact parent");
        }
        let ir_text = format!(
            "package p\n\nfn helper(x: bits[1] id=1) -> bits[1] {{\n  ret x: bits[1] = identity(x, id=2)\n}}\n\ntop fn {ir_top}(x: bits[1] id=3) -> bits[1] {{\n  ret y: bits[1] = not(x, id=4)\n}}\n"
        );
        fs::write(&artifact_path, &ir_text).expect("write sample IR");
        let artifact_bytes = fs::metadata(&artifact_path).expect("stat sample IR").len();

        let group_relpath = hash_group_relpath(&structural_hash);
        let group = IrFnCorpusStructuralGroupFile {
            schema_version: crate::IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION,
            structural_hash: structural_hash.clone(),
            members: vec![IrFnCorpusStructuralMember {
                opt_ir_action_id: opt_ir_action_id.clone(),
                source_ir_action_id: source_ir_action_id.clone(),
                ir_top: ir_top.clone(),
                ir_fn_signature: Some("__float32__add(x: bits[1]) -> bits[1]".to_string()),
                ir_op_count: Some(1),
                crate_version: "0.33.0".to_string(),
                dso_version: "0.37.0".to_string(),
                created_utc: Utc::now(),
                output_artifact: artifact_ref,
                output_file_sha256: "0".repeat(64),
                output_file_bytes: artifact_bytes,
                hash_source: "hint".to_string(),
                hash_hint_source_action_ids: vec![],
                dslx_origin: Some(IrFnCorpusStructuralDslxOrigin {
                    dslx_subtree_action_id: "3".repeat(64),
                    dslx_file: "xls/dslx/stdlib/float32.x".to_string(),
                    dslx_fn_name: "add".to_string(),
                }),
                producer_action_kind: Some("driver_ir_to_opt".to_string()),
            }],
        };
        let group_key = ir_fn_corpus_structural_group_index_key(&structural_hash);
        store
            .write_web_index_bytes(
                &group_key,
                &serde_json::to_vec_pretty(&group).expect("serialize group"),
            )
            .expect("write group key");

        let manifest = IrFnCorpusStructuralManifest {
            schema_version: crate::IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION,
            generated_utc: Utc::now(),
            store_root: root.display().to_string(),
            output_dir: ir_fn_corpus_structural_index_location(
                &ir_fn_corpus_structural_index_prefix(),
            ),
            recompute_missing_hashes: false,
            total_actions_scanned: 1,
            total_driver_ir_to_opt_actions: 1,
            total_ir_fn_to_k_bool_cone_corpus_actions: 0,
            indexed_actions: 1,
            indexed_k_bool_cone_members: 0,
            distinct_structural_hashes: 1,
            hash_from_dependency_hint_count: 1,
            hash_recomputed_count: 0,
            hash_hint_conflict_count: 0,
            skipped_missing_output_count: 0,
            skipped_missing_ir_top_count: 0,
            skipped_missing_hash_hint_count: 0,
            skipped_hash_error_count: 0,
            skipped_k_bool_cone_manifest_errors: 0,
            skipped_k_bool_cone_empty_count: 0,
            source_action_set_sha256: Some("abc123".to_string()),
            groups: vec![IrFnCorpusStructuralManifestGroup {
                structural_hash: structural_hash.clone(),
                member_count: 1,
                relpath: group_relpath,
                ir_node_count: Some(1),
            }],
        };
        store
            .write_web_index_bytes(
                ir_fn_corpus_structural_manifest_index_key(),
                &serde_json::to_vec_pretty(&manifest).expect("serialize manifest"),
            )
            .expect("write manifest key");

        let first = ensure_ir_fn_corpus_structural_archive(&store).expect("build archive");
        assert!(!first.cache_hit, "first archive request should build");
        assert!(!first.bytes.is_empty(), "archive bytes should be non-empty");

        let second = ensure_ir_fn_corpus_structural_archive(&store).expect("load archive cache");
        assert!(second.cache_hit, "second archive request should hit cache");
        assert_eq!(first.bytes, second.bytes, "cached archive should be stable");

        let decoder = zstd::stream::read::Decoder::new(std::io::Cursor::new(second.bytes))
            .expect("create zstd decoder");
        let mut archive = tar::Archive::new(decoder);
        let mut saw_manifest = false;
        let mut saw_index = false;
        let mut saw_function = false;
        for entry in archive.entries().expect("read tar entries") {
            let mut entry = entry.expect("read tar entry");
            let path = entry
                .path()
                .expect("tar entry path")
                .to_string_lossy()
                .to_string();
            let mut content = String::new();
            entry
                .read_to_string(&mut content)
                .expect("read tar entry content");
            if path.ends_with("/manifest.json") {
                saw_manifest = true;
            }
            if path.ends_with("/functions/index.json") {
                saw_index = true;
                assert!(
                    content.contains(&structural_hash),
                    "index should include structural hash"
                );
            }
            if path.ends_with(&format!("/functions/by-hash/ee/ee/{structural_hash}.ir")) {
                saw_function = true;
                assert!(
                    content.contains("top fn __float32__add"),
                    "function tar entry should contain selected top fn"
                );
                assert!(
                    !content.contains("fn helper("),
                    "function tar entry should include only the selected function block"
                );
            }
        }
        assert!(saw_manifest, "archive should contain manifest");
        assert!(saw_index, "archive should contain function index");
        assert!(saw_function, "archive should contain function IR payload");

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn resolve_direct_dslx_origin_from_opt_ir_action_rejects_k_bool_cone_ir() {
        let runtime = test_runtime();
        let dslx_ir_id = "1".repeat(64);
        let opt_ir_id = "2".repeat(64);
        let kcone_ir_id = "3".repeat(64);
        let mut provenance_by_action_id = BTreeMap::new();
        provenance_by_action_id.insert(
            dslx_ir_id.clone(),
            synthetic_provenance(
                &dslx_ir_id,
                ActionSpec::DriverDslxFnToIr {
                    dslx_subtree_action_id: "4".repeat(64),
                    dslx_file: "xls/dslx/stdlib/float32.x".to_string(),
                    dslx_fn_name: "add".to_string(),
                    version: "0.35.0".to_string(),
                    runtime: runtime.clone(),
                },
                ArtifactType::IrPackageFile,
            ),
        );
        provenance_by_action_id.insert(
            opt_ir_id.clone(),
            synthetic_provenance(
                &opt_ir_id,
                ActionSpec::DriverIrToOpt {
                    ir_action_id: dslx_ir_id.clone(),
                    top_fn_name: Some("__float32__add".to_string()),
                    version: "0.35.0".to_string(),
                    runtime: runtime.clone(),
                },
                ArtifactType::IrPackageFile,
            ),
        );
        provenance_by_action_id.insert(
            kcone_ir_id.clone(),
            synthetic_provenance(
                &kcone_ir_id,
                ActionSpec::IrFnToKBoolConeCorpus {
                    ir_action_id: opt_ir_id.clone(),
                    top_fn_name: Some("__float32__add".to_string()),
                    k: 3,
                    max_ir_ops: Some(16),
                    version: "0.35.0".to_string(),
                    runtime,
                },
                ArtifactType::IrPackageFile,
            ),
        );
        let lookup = borrowed_lookup(&provenance_by_action_id);

        assert_eq!(
            resolve_direct_dslx_origin_from_opt_ir_action(&lookup, &opt_ir_id),
            Some(("xls/dslx/stdlib/float32.x".to_string(), "add".to_string()))
        );
        assert_eq!(
            resolve_direct_dslx_origin_from_opt_ir_action(&lookup, &kcone_ir_id),
            None
        );
    }

    #[test]
    fn make_stdlib_aig_stats_point_rejects_k_bool_cone_derived_ir() {
        let runtime = test_runtime();
        let dslx_ir_id = "5".repeat(64);
        let opt_ir_id = "6".repeat(64);
        let kcone_ir_id = "7".repeat(64);
        let mut provenance_by_action_id = BTreeMap::new();
        provenance_by_action_id.insert(
            dslx_ir_id.clone(),
            synthetic_provenance(
                &dslx_ir_id,
                ActionSpec::DriverDslxFnToIr {
                    dslx_subtree_action_id: "8".repeat(64),
                    dslx_file: "xls/dslx/stdlib/float32.x".to_string(),
                    dslx_fn_name: "add".to_string(),
                    version: "0.35.0".to_string(),
                    runtime: runtime.clone(),
                },
                ArtifactType::IrPackageFile,
            ),
        );
        provenance_by_action_id.insert(
            opt_ir_id.clone(),
            synthetic_provenance(
                &opt_ir_id,
                ActionSpec::DriverIrToOpt {
                    ir_action_id: dslx_ir_id,
                    top_fn_name: Some("__float32__add".to_string()),
                    version: "0.35.0".to_string(),
                    runtime: runtime.clone(),
                },
                ArtifactType::IrPackageFile,
            ),
        );
        provenance_by_action_id.insert(
            kcone_ir_id.clone(),
            synthetic_provenance(
                &kcone_ir_id,
                ActionSpec::IrFnToKBoolConeCorpus {
                    ir_action_id: opt_ir_id.clone(),
                    top_fn_name: Some("__float32__add".to_string()),
                    k: 3,
                    max_ir_ops: Some(16),
                    version: "0.35.0".to_string(),
                    runtime,
                },
                ArtifactType::IrPackageFile,
            ),
        );
        let lookup = borrowed_lookup(&provenance_by_action_id);

        let direct_stats_action_id = "9".repeat(64);
        let direct_point = make_stdlib_aig_stats_point(
            &lookup,
            StdlibTrendSourceContext {
                ir_action_id: opt_ir_id,
                ir_top: Some("__float32__add".to_string()),
                crate_version: "0.31.0".to_string(),
                dso_version: "0.35.0".to_string(),
            },
            &direct_stats_action_id,
            Utc::now(),
            "0.35.0",
            100.0,
            10.0,
        );
        assert!(direct_point.is_some());

        let kcone_stats_action_id = "a".repeat(64);
        let kcone_point = make_stdlib_aig_stats_point(
            &lookup,
            StdlibTrendSourceContext {
                ir_action_id: kcone_ir_id,
                ir_top: Some("__k3_cone_deadbeef".to_string()),
                crate_version: "0.31.0".to_string(),
                dso_version: "0.35.0".to_string(),
            },
            &kcone_stats_action_id,
            Utc::now(),
            "0.35.0",
            100.0,
            10.0,
        );
        assert!(kcone_point.is_none());
    }

    #[test]
    fn apply_selected_file_to_stdlib_trend_dataset_filters_series_and_versions() {
        let base = StdlibFnsTrendDataset {
            kind: StdlibTrendKind::G8r,
            fraig: false,
            crate_versions: vec!["0.30.0".to_string(), "0.31.0".to_string()],
            series: vec![
                StdlibFnTrendSeries {
                    fn_key: "xls/dslx/stdlib/float32.x::add".to_string(),
                    points: vec![
                        StdlibFnTrendPoint {
                            crate_version: "0.30.0".to_string(),
                            dso_version: "0.35.0".to_string(),
                            and_nodes: 100.0,
                            depth: 10.0,
                            created_utc: Utc::now(),
                            stats_action_id: "a".repeat(64),
                        },
                        StdlibFnTrendPoint {
                            crate_version: "0.31.0".to_string(),
                            dso_version: "0.35.0".to_string(),
                            and_nodes: 90.0,
                            depth: 9.0,
                            created_utc: Utc::now(),
                            stats_action_id: "b".repeat(64),
                        },
                    ],
                },
                StdlibFnTrendSeries {
                    fn_key: "xls/dslx/stdlib/apfloat_fadd.x::next_pow2".to_string(),
                    points: vec![StdlibFnTrendPoint {
                        crate_version: "0.31.0".to_string(),
                        dso_version: "0.35.0".to_string(),
                        and_nodes: 200.0,
                        depth: 20.0,
                        created_utc: Utc::now(),
                        stats_action_id: "c".repeat(64),
                    }],
                },
            ],
            total_points: 3,
            available_files: vec![
                "xls/dslx/stdlib/apfloat_fadd.x".to_string(),
                "xls/dslx/stdlib/float32.x".to_string(),
            ],
            selected_file: None,
        };

        let filtered = apply_selected_file_to_stdlib_fns_trend_dataset(
            &base,
            Some("xls/dslx/stdlib/float32.x"),
        );
        assert_eq!(
            filtered.selected_file.as_deref(),
            Some("xls/dslx/stdlib/float32.x")
        );
        assert_eq!(filtered.series.len(), 1);
        assert_eq!(filtered.series[0].fn_key, "xls/dslx/stdlib/float32.x::add");
        assert_eq!(filtered.total_points, 2);
        assert_eq!(filtered.crate_versions, vec!["0.30.0", "0.31.0"]);

        let missing = apply_selected_file_to_stdlib_fns_trend_dataset(
            &base,
            Some("xls/dslx/stdlib/does_not_exist.x"),
        );
        assert!(missing.selected_file.is_none());
        assert_eq!(missing.series.len(), 2);
        assert_eq!(missing.total_points, 3);
    }
}
