use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Digest;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

use crate::cli::{CorpusExecutionMode, CorpusRecipePreset, CorpusTopFnPolicy, DriverCli, YosysCli};
use crate::executor::{compute_action_id, execute_action};
use crate::model::{
    ActionSpec, ArtifactRef, ArtifactType, DriverRuntimeSpec, Provenance, YosysRuntimeSpec,
};
use crate::queue::{
    enqueue_action_with_priority, load_queue_canceled_record, load_queue_done_record,
    load_queue_failed_record, queue_state_for_action, suggested_action_queue_priority,
};
use crate::runtime::resolve_driver_runtime_for_aig_stats;
use crate::service::{
    collect_output_files, infer_ir_top_function, make_script_ref, sha256_file, summarize_error,
};
use crate::store::ArtifactStore;

const IR_DIR_CORPUS_MANIFEST_SCHEMA_VERSION: u32 = 2;
const IR_DIR_CORPUS_MANIFEST_FILENAME: &str = "manifest.json";
const IR_DIR_CORPUS_SAMPLES_FILENAME: &str = "samples.jsonl";
const IR_DIR_CORPUS_SUMMARY_FILENAME: &str = "summary.json";
const IR_DIR_CORPUS_JOINED_DIR: &str = "joined";
const IR_DIR_CORPUS_EXPORTED_ARTIFACTS_DIR: &str = "artifacts";
const IR_DIR_CORPUS_INTERNAL_DIR: &str = ".bvc";
const IR_DIR_CORPUS_INTERNAL_STORE_DIR: &str = "bvc-artifacts";
const IR_DIR_CORPUS_INTERNAL_SLED_FILENAME: &str = "artifacts.sled";
const IMPORTED_IR_RELPATH: &str = "payload/input.ir";
const G8R_AIG_RELPATH: &str = "payload/result.aig";
const G8R_STATS_RELPATH: &str = "payload/stats.json";
const COMBO_VERILOG_RELPATH: &str = "payload/result.v";
const YOSYS_ABC_AIG_RELPATH: &str = "payload/result.aig";
const YOSYS_ABC_STATS_RELPATH: &str = "payload/stats.json";
const AIG_STAT_DIFF_RELPATH: &str = "payload/aig_stat_diff.json";
const IR_DIR_CORPUS_REFRESH_SEMANTICS: &str = "rerun_same_command_refreshes_exports";
const IR_DIR_CORPUS_SAMPLE_ID_SCHEME: &str = "basename-plus-normalized-source-relpath-sha256-12-v1";

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RunIrDirCorpusSummary {
    pub(crate) output_dir: String,
    pub(crate) input_dir: String,
    pub(crate) workspace_dir: String,
    pub(crate) workspace_store_dir: String,
    pub(crate) workspace_artifacts_via_sled: String,
    pub(crate) manifest_path: String,
    pub(crate) samples_path: String,
    pub(crate) summary_path: String,
    pub(crate) joined_jsonl_path: String,
    pub(crate) joined_csv_path: String,
    pub(crate) recipe_preset: String,
    pub(crate) execution_mode: String,
    pub(crate) refresh_semantics: String,
    pub(crate) sample_id_scheme: String,
    pub(crate) sample_count: usize,
    pub(crate) completed_samples: usize,
    pub(crate) enqueued_actions: usize,
    pub(crate) executed_actions: usize,
    pub(crate) status_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IrDirCorpusManifest {
    schema_version: u32,
    generated_utc: DateTime<Utc>,
    input_dir: String,
    output_dir: String,
    workspace_dir: String,
    workspace_store_dir: String,
    workspace_artifacts_via_sled: String,
    recipe_preset: String,
    execution_mode: String,
    refresh_semantics: String,
    sample_id_scheme: String,
    top_fn_policy: String,
    top_fn_name: Option<String>,
    fraig: bool,
    dso_version: String,
    driver_runtime: DriverRuntimeSpec,
    stats_runtime: DriverRuntimeSpec,
    yosys_runtime: YosysRuntimeSpec,
    yosys_script: String,
    yosys_script_sha256: String,
    samples: Vec<IrDirCorpusSampleRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IrDirCorpusSampleRecord {
    sample_id: String,
    logical_name: String,
    source_relpath: String,
    source_sha256: String,
    top_fn_policy: String,
    top_fn_name: String,
    fraig: bool,
    dso_version: String,
    driver_crate_version: String,
    stats_driver_crate_version: String,
    yosys_script: String,
    yosys_script_sha256: String,
    preset: String,
    import_ir_action_id: String,
    import_ir_status: String,
    g8r_aig_action_id: String,
    g8r_aig_status: String,
    g8r_stats_action_id: String,
    g8r_stats_status: String,
    combo_verilog_action_id: String,
    combo_verilog_status: String,
    yosys_abc_aig_action_id: String,
    yosys_abc_aig_status: String,
    yosys_abc_stats_action_id: String,
    yosys_abc_stats_status: String,
    aig_stat_diff_action_id: String,
    aig_stat_diff_status: String,
    status: String,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct IrDirCorpusSummaryFile {
    schema_version: u32,
    generated_utc: DateTime<Utc>,
    output_dir: String,
    workspace_store_dir: String,
    workspace_artifacts_via_sled: String,
    recipe_preset: String,
    execution_mode: String,
    refresh_semantics: String,
    sample_id_scheme: String,
    total_samples: usize,
    completed_samples: usize,
    status_counts: BTreeMap<String, usize>,
    enqueued_actions: usize,
    executed_actions: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct IrDirCorpusStatusReport {
    pub(crate) output_dir: String,
    pub(crate) manifest_path: String,
    pub(crate) samples_path: String,
    pub(crate) summary_path: String,
    pub(crate) joined_jsonl_path: String,
    pub(crate) joined_csv_path: String,
    pub(crate) export_root: String,
    pub(crate) workspace_store_dir: String,
    pub(crate) workspace_artifacts_via_sled: String,
    pub(crate) refreshed_public_outputs: bool,
    pub(crate) generated_utc: DateTime<Utc>,
    pub(crate) sample_counts: BTreeMap<String, usize>,
    pub(crate) action_counts: BTreeMap<String, usize>,
    pub(crate) throughput_window_seconds: i64,
    pub(crate) sample_throughput_basis: String,
    pub(crate) sample_throughput_per_hour: Option<f64>,
    pub(crate) action_throughput_basis: String,
    pub(crate) action_throughput_per_hour: Option<f64>,
    pub(crate) eta_scope: String,
    pub(crate) eta_seconds: Option<i64>,
    pub(crate) eta_human: Option<String>,
    pub(crate) ready_output_counts: BTreeMap<String, usize>,
    pub(crate) public_outputs_present: BTreeMap<String, bool>,
    pub(crate) exported_sample_dirs_on_disk: usize,
    pub(crate) failed_sample_examples: Vec<IrDirCorpusFailureExample>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct IrDirCorpusFailureExample {
    pub(crate) sample_id: String,
    pub(crate) source_relpath: String,
    pub(crate) error: String,
}

#[derive(Debug, Clone, Serialize)]
struct IrDirCorpusJoinedRow {
    sample_id: String,
    logical_name: String,
    source_relpath: String,
    source_sha256: String,
    top_fn_policy: String,
    top_fn_name: String,
    fraig: bool,
    dso_version: String,
    driver_crate_version: String,
    stats_driver_crate_version: String,
    yosys_script: String,
    yosys_script_sha256: String,
    import_ir_action_id: String,
    g8r_aig_action_id: String,
    g8r_stats_action_id: String,
    combo_verilog_action_id: String,
    yosys_abc_aig_action_id: String,
    yosys_abc_stats_action_id: String,
    aig_stat_diff_action_id: String,
    g8r_and_nodes: Option<f64>,
    g8r_depth: Option<f64>,
    g8r_product: Option<f64>,
    yosys_abc_and_nodes: Option<f64>,
    yosys_abc_depth: Option<f64>,
    yosys_abc_product: Option<f64>,
    g8r_product_loss: Option<f64>,
    delta_and_nodes_yosys_minus_g8r: Option<f64>,
    delta_depth_yosys_minus_g8r: Option<f64>,
}

#[derive(Debug, Clone)]
struct CorpusSampleSpec {
    sample_id: String,
    logical_name: String,
    source_path: PathBuf,
    source_relpath: String,
    source_sha256: String,
    top_fn_name: String,
}

#[derive(Debug, Clone)]
struct CorpusActionPlan {
    import_action: ActionSpec,
    g8r_aig_action: ActionSpec,
    g8r_stats_action: ActionSpec,
    combo_verilog_action: ActionSpec,
    yosys_abc_aig_action: ActionSpec,
    yosys_abc_stats_action: ActionSpec,
    aig_stat_diff_action: ActionSpec,
    import_ir_action_id: String,
    g8r_aig_action_id: String,
    g8r_stats_action_id: String,
    combo_verilog_action_id: String,
    yosys_abc_aig_action_id: String,
    yosys_abc_stats_action_id: String,
    aig_stat_diff_action_id: String,
}

#[derive(Debug, Default)]
struct ExecutionCounters {
    enqueued_actions: usize,
    executed_actions: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CorpusStatusQueryMode {
    LiveStore,
    QueueFilesOnly,
}

pub(crate) fn run_ir_dir_corpus(
    repo_root: &Path,
    input_dir: &Path,
    output_dir: &Path,
    recipe_preset: CorpusRecipePreset,
    execution_mode: CorpusExecutionMode,
    top_fn_policy: CorpusTopFnPolicy,
    top_fn_name: Option<&str>,
    fraig: bool,
    version: &str,
    yosys_script: &str,
    priority: i32,
    driver: DriverCli,
    yosys: YosysCli,
) -> Result<RunIrDirCorpusSummary> {
    if !input_dir.exists() {
        bail!("input dir does not exist: {}", input_dir.display());
    }
    if !input_dir.is_dir() {
        bail!("input dir is not a directory: {}", input_dir.display());
    }
    if input_dir == output_dir {
        bail!("input dir and output dir must differ");
    }
    if let CorpusTopFnPolicy::Explicit = top_fn_policy {
        if top_fn_name.is_none() {
            bail!("--top-fn-name is required when --top-fn-policy=explicit");
        }
    }

    fs::create_dir_all(output_dir)
        .with_context(|| format!("creating output dir: {}", output_dir.display()))?;
    let (workspace_dir, workspace_store_dir, workspace_artifacts_via_sled) =
        corpus_workspace_paths(output_dir);
    fs::create_dir_all(&workspace_dir)
        .with_context(|| format!("creating workspace dir: {}", workspace_dir.display()))?;
    let store = ArtifactStore::new_with_sled(
        workspace_store_dir.clone(),
        workspace_artifacts_via_sled.clone(),
    );
    store.ensure_layout()?;

    let driver_runtime = driver.into_runtime(repo_root, version)?;
    let stats_runtime = resolve_driver_runtime_for_aig_stats(repo_root, &driver_runtime)
        .unwrap_or_else(|_| driver_runtime.clone());
    let yosys_runtime = yosys.into_runtime();
    let yosys_script_ref = make_script_ref(repo_root, yosys_script)?;
    let samples = discover_corpus_samples(input_dir, output_dir, top_fn_policy, top_fn_name)?;
    if samples.is_empty() {
        bail!("no .ir files found under {}", input_dir.display());
    }

    let mut counters = ExecutionCounters::default();
    let mut run_errors: BTreeMap<String, String> = BTreeMap::new();
    let mut sample_records = Vec::with_capacity(samples.len());
    let now = Utc::now();

    for sample in &samples {
        let plan = build_action_plan(
            sample,
            fraig,
            version,
            &driver_runtime,
            &stats_runtime,
            &yosys_runtime,
            &yosys_script_ref,
        )?;
        ensure_imported_ir_action(&store, sample, &plan.import_action)?;
        match execution_mode {
            CorpusExecutionMode::Enqueue => {
                counters.enqueued_actions += enqueue_plan(&store, &plan, priority)?;
            }
            CorpusExecutionMode::Run => {
                execute_plan(&store, &plan, &mut counters)
                    .map_err(|err| {
                        run_errors.insert(sample.sample_id.clone(), format!("{:#}", err));
                        err
                    })
                    .ok();
            }
        }
        sample_records.push(build_sample_record(
            &store,
            sample,
            &plan,
            &run_errors,
            top_fn_policy,
            fraig,
            version,
            &driver_runtime,
            &stats_runtime,
            &yosys_script_ref,
        ));
    }

    let joined_rows = build_joined_rows(&store, &sample_records)?;
    export_leaf_artifacts(&store, output_dir, &sample_records)?;

    let manifest = IrDirCorpusManifest {
        schema_version: IR_DIR_CORPUS_MANIFEST_SCHEMA_VERSION,
        generated_utc: now,
        input_dir: input_dir.display().to_string(),
        output_dir: output_dir.display().to_string(),
        workspace_dir: workspace_dir.display().to_string(),
        workspace_store_dir: workspace_store_dir.display().to_string(),
        workspace_artifacts_via_sled: workspace_artifacts_via_sled.display().to_string(),
        recipe_preset: recipe_preset_label(recipe_preset).to_string(),
        execution_mode: execution_mode_label(execution_mode).to_string(),
        refresh_semantics: refresh_semantics_label().to_string(),
        sample_id_scheme: sample_id_scheme_label().to_string(),
        top_fn_policy: top_fn_policy_label(top_fn_policy).to_string(),
        top_fn_name: top_fn_name.map(ToOwned::to_owned),
        fraig,
        dso_version: version.to_string(),
        driver_runtime: driver_runtime.clone(),
        stats_runtime: stats_runtime.clone(),
        yosys_runtime: yosys_runtime.clone(),
        yosys_script: yosys_script_ref.path.clone(),
        yosys_script_sha256: yosys_script_ref.sha256.clone(),
        samples: sample_records.clone(),
    };
    let status_counts = count_statuses(&sample_records);
    let summary_file = IrDirCorpusSummaryFile {
        schema_version: IR_DIR_CORPUS_MANIFEST_SCHEMA_VERSION,
        generated_utc: now,
        output_dir: output_dir.display().to_string(),
        workspace_store_dir: workspace_store_dir.display().to_string(),
        workspace_artifacts_via_sled: workspace_artifacts_via_sled.display().to_string(),
        recipe_preset: recipe_preset_label(recipe_preset).to_string(),
        execution_mode: execution_mode_label(execution_mode).to_string(),
        refresh_semantics: refresh_semantics_label().to_string(),
        sample_id_scheme: sample_id_scheme_label().to_string(),
        total_samples: sample_records.len(),
        completed_samples: sample_records
            .iter()
            .filter(|sample| sample.status == "done")
            .count(),
        status_counts: status_counts.clone(),
        enqueued_actions: counters.enqueued_actions,
        executed_actions: counters.executed_actions,
    };

    let manifest_path = output_dir.join(IR_DIR_CORPUS_MANIFEST_FILENAME);
    let samples_path = output_dir.join(IR_DIR_CORPUS_SAMPLES_FILENAME);
    let summary_path = output_dir.join(IR_DIR_CORPUS_SUMMARY_FILENAME);
    let joined_dir = output_dir.join(IR_DIR_CORPUS_JOINED_DIR);
    fs::create_dir_all(&joined_dir)
        .with_context(|| format!("creating joined dir: {}", joined_dir.display()))?;
    let joined_jsonl_path =
        joined_dir.join(format!("{}.jsonl", recipe_preset_label(recipe_preset)));
    let joined_csv_path = joined_dir.join(format!("{}.csv", recipe_preset_label(recipe_preset)));

    fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest).context("serializing corpus manifest")?,
    )
    .with_context(|| format!("writing manifest: {}", manifest_path.display()))?;
    write_samples_jsonl(&samples_path, &sample_records)?;
    fs::write(
        &summary_path,
        serde_json::to_string_pretty(&summary_file).context("serializing corpus summary")?,
    )
    .with_context(|| format!("writing summary: {}", summary_path.display()))?;
    write_joined_jsonl(&joined_jsonl_path, &joined_rows)?;
    write_joined_csv(&joined_csv_path, &joined_rows)?;

    Ok(RunIrDirCorpusSummary {
        output_dir: output_dir.display().to_string(),
        input_dir: input_dir.display().to_string(),
        workspace_dir: workspace_dir.display().to_string(),
        workspace_store_dir: workspace_store_dir.display().to_string(),
        workspace_artifacts_via_sled: workspace_artifacts_via_sled.display().to_string(),
        manifest_path: manifest_path.display().to_string(),
        samples_path: samples_path.display().to_string(),
        summary_path: summary_path.display().to_string(),
        joined_jsonl_path: joined_jsonl_path.display().to_string(),
        joined_csv_path: joined_csv_path.display().to_string(),
        recipe_preset: recipe_preset_label(recipe_preset).to_string(),
        execution_mode: execution_mode_label(execution_mode).to_string(),
        refresh_semantics: refresh_semantics_label().to_string(),
        sample_id_scheme: sample_id_scheme_label().to_string(),
        sample_count: sample_records.len(),
        completed_samples: summary_file.completed_samples,
        enqueued_actions: counters.enqueued_actions,
        executed_actions: counters.executed_actions,
        status_counts,
    })
}

pub(crate) fn show_ir_dir_corpus_progress(
    output_dir: &Path,
    throughput_window_seconds: i64,
    failed_sample_examples: usize,
) -> Result<IrDirCorpusStatusReport> {
    build_ir_dir_corpus_status_report(
        output_dir,
        false,
        throughput_window_seconds,
        failed_sample_examples,
    )
}

pub(crate) fn refresh_ir_dir_corpus_status(
    output_dir: &Path,
    throughput_window_seconds: i64,
    failed_sample_examples: usize,
) -> Result<IrDirCorpusStatusReport> {
    build_ir_dir_corpus_status_report(
        output_dir,
        true,
        throughput_window_seconds,
        failed_sample_examples,
    )
}

fn build_ir_dir_corpus_status_report(
    output_dir: &Path,
    refresh_public_outputs: bool,
    throughput_window_seconds: i64,
    failed_sample_examples: usize,
) -> Result<IrDirCorpusStatusReport> {
    if throughput_window_seconds <= 0 {
        bail!(
            "--throughput-window-seconds must be > 0, got {}",
            throughput_window_seconds
        );
    }

    let manifest_path = output_dir.join(IR_DIR_CORPUS_MANIFEST_FILENAME);
    let manifest_text = fs::read_to_string(&manifest_path)
        .with_context(|| format!("reading corpus manifest: {}", manifest_path.display()))?;
    let manifest: IrDirCorpusManifest = serde_json::from_str(&manifest_text)
        .with_context(|| format!("parsing corpus manifest: {}", manifest_path.display()))?;

    let (workspace_dir, workspace_store_dir, workspace_artifacts_via_sled) =
        corpus_workspace_paths(output_dir);
    if !workspace_artifacts_via_sled.exists() {
        bail!(
            "corpus workspace sled db does not exist: {}",
            workspace_artifacts_via_sled.display()
        );
    }
    let store = ArtifactStore::new_with_sled(
        workspace_store_dir.clone(),
        workspace_artifacts_via_sled.clone(),
    );
    store.ensure_layout()?;
    let status_query_mode = match store.load_failed_action_records() {
        Ok(_) => CorpusStatusQueryMode::LiveStore,
        Err(err) => {
            if is_sled_lock_error(&err) {
                if refresh_public_outputs {
                    bail!(
                        "corpus workspace sled db is locked by another process: {} (stop the active worker/service first, then retry)",
                        workspace_store_dir.display()
                    );
                }
                CorpusStatusQueryMode::QueueFilesOnly
            } else {
                return Err(err);
            }
        }
    };

    let top_fn_policy = parse_top_fn_policy_label(&manifest.top_fn_policy)?;
    let yosys_script_ref = crate::model::ScriptRef {
        path: manifest.yosys_script.clone(),
        sha256: manifest.yosys_script_sha256.clone(),
    };
    let samples = sample_specs_from_manifest(&manifest);
    let run_errors = BTreeMap::new();
    let mut sample_records = Vec::with_capacity(samples.len());
    let mut unique_action_ids = BTreeSet::new();
    let mut action_counts = empty_action_counts();
    let mut action_completion_times = Vec::new();

    for (sample, persisted_sample) in samples.iter().zip(manifest.samples.iter()) {
        let plan = build_action_plan(
            sample,
            manifest.fraig,
            &manifest.dso_version,
            &manifest.driver_runtime,
            &manifest.stats_runtime,
            &manifest.yosys_runtime,
            &yosys_script_ref,
        )?;
        let mut sample_record = match status_query_mode {
            CorpusStatusQueryMode::LiveStore => build_sample_record(
                &store,
                sample,
                &plan,
                &run_errors,
                top_fn_policy,
                manifest.fraig,
                &manifest.dso_version,
                &manifest.driver_runtime,
                &manifest.stats_runtime,
                &yosys_script_ref,
            ),
            CorpusStatusQueryMode::QueueFilesOnly => build_sample_record_from_queue_state(
                &store,
                sample,
                persisted_sample,
                &plan,
                top_fn_policy,
                manifest.fraig,
                &manifest.dso_version,
                &manifest.driver_runtime,
                &manifest.stats_runtime,
                &yosys_script_ref,
            ),
        };
        preserve_persisted_run_failure(
            &manifest.execution_mode,
            persisted_sample,
            &mut sample_record,
        );
        for (action_id, status) in corpus_sample_action_statuses(&sample_record) {
            if !unique_action_ids.insert(action_id.to_string()) {
                continue;
            }
            *action_counts
                .get_mut(status)
                .expect("known corpus action status key") += 1;
            if status == "done" {
                if let Some(completed_utc) =
                    action_completion_utc(&store, action_id, status_query_mode)?
                {
                    action_completion_times.push(completed_utc);
                }
            }
        }
        sample_records.push(sample_record);
    }
    action_counts.insert("planned_unique".to_string(), unique_action_ids.len());

    let mut sample_counts = count_statuses(&sample_records);
    sample_counts.insert("total".to_string(), sample_records.len());
    ensure_sample_count_key(&mut sample_counts, "done");
    ensure_sample_count_key(&mut sample_counts, "running");
    ensure_sample_count_key(&mut sample_counts, "pending");
    ensure_sample_count_key(&mut sample_counts, "failed");
    ensure_sample_count_key(&mut sample_counts, "missing");

    let now = Utc::now();
    let sample_completion_times =
        collect_sample_completion_times(&store, &sample_records, status_query_mode)?;
    let sample_rate =
        completion_rate_per_hour(&sample_completion_times, now, throughput_window_seconds);
    let action_rate =
        completion_rate_per_hour(&action_completion_times, now, throughput_window_seconds);
    let active_remaining_samples = sample_counts.get("running").copied().unwrap_or(0)
        + sample_counts.get("pending").copied().unwrap_or(0);
    let eta_seconds = estimate_eta_seconds(active_remaining_samples, sample_rate.per_hour);

    let joined_jsonl_path = joined_output_path(output_dir, &manifest.recipe_preset, "jsonl");
    let joined_csv_path = joined_output_path(output_dir, &manifest.recipe_preset, "csv");
    if refresh_public_outputs {
        let joined_rows = build_joined_rows(&store, &sample_records)?;
        export_leaf_artifacts(&store, output_dir, &sample_records)?;

        let refreshed_manifest = IrDirCorpusManifest {
            schema_version: IR_DIR_CORPUS_MANIFEST_SCHEMA_VERSION,
            generated_utc: now,
            input_dir: manifest.input_dir.clone(),
            output_dir: output_dir.display().to_string(),
            workspace_dir: workspace_dir.display().to_string(),
            workspace_store_dir: workspace_store_dir.display().to_string(),
            workspace_artifacts_via_sled: workspace_artifacts_via_sled.display().to_string(),
            recipe_preset: manifest.recipe_preset.clone(),
            execution_mode: manifest.execution_mode.clone(),
            refresh_semantics: manifest.refresh_semantics.clone(),
            sample_id_scheme: manifest.sample_id_scheme.clone(),
            top_fn_policy: manifest.top_fn_policy.clone(),
            top_fn_name: manifest.top_fn_name.clone(),
            fraig: manifest.fraig,
            dso_version: manifest.dso_version.clone(),
            driver_runtime: manifest.driver_runtime.clone(),
            stats_runtime: manifest.stats_runtime.clone(),
            yosys_runtime: manifest.yosys_runtime.clone(),
            yosys_script: manifest.yosys_script.clone(),
            yosys_script_sha256: manifest.yosys_script_sha256.clone(),
            samples: sample_records.clone(),
        };
        let refreshed_summary = IrDirCorpusSummaryFile {
            schema_version: IR_DIR_CORPUS_MANIFEST_SCHEMA_VERSION,
            generated_utc: now,
            output_dir: output_dir.display().to_string(),
            workspace_store_dir: workspace_store_dir.display().to_string(),
            workspace_artifacts_via_sled: workspace_artifacts_via_sled.display().to_string(),
            recipe_preset: manifest.recipe_preset.clone(),
            execution_mode: manifest.execution_mode.clone(),
            refresh_semantics: manifest.refresh_semantics.clone(),
            sample_id_scheme: manifest.sample_id_scheme.clone(),
            total_samples: sample_records.len(),
            completed_samples: sample_counts.get("done").copied().unwrap_or(0),
            status_counts: count_statuses(&sample_records),
            enqueued_actions: 0,
            executed_actions: 0,
        };
        let samples_path = output_dir.join(IR_DIR_CORPUS_SAMPLES_FILENAME);
        let summary_path = output_dir.join(IR_DIR_CORPUS_SUMMARY_FILENAME);
        let joined_dir = output_dir.join(IR_DIR_CORPUS_JOINED_DIR);
        fs::create_dir_all(&joined_dir)
            .with_context(|| format!("creating joined dir: {}", joined_dir.display()))?;
        fs::write(
            &manifest_path,
            serde_json::to_string_pretty(&refreshed_manifest)
                .context("serializing refreshed corpus manifest")?,
        )
        .with_context(|| format!("writing manifest: {}", manifest_path.display()))?;
        write_samples_jsonl(&samples_path, &sample_records)?;
        fs::write(
            &summary_path,
            serde_json::to_string_pretty(&refreshed_summary)
                .context("serializing refreshed corpus summary")?,
        )
        .with_context(|| format!("writing summary: {}", summary_path.display()))?;
        write_joined_jsonl(&joined_jsonl_path, &joined_rows)?;
        write_joined_csv(&joined_csv_path, &joined_rows)?;
    }

    let public_outputs_present = public_outputs_present(
        &manifest_path,
        &output_dir.join(IR_DIR_CORPUS_SAMPLES_FILENAME),
        &output_dir.join(IR_DIR_CORPUS_SUMMARY_FILENAME),
        &joined_jsonl_path,
        &joined_csv_path,
    );

    Ok(IrDirCorpusStatusReport {
        output_dir: output_dir.display().to_string(),
        manifest_path: manifest_path.display().to_string(),
        samples_path: output_dir
            .join(IR_DIR_CORPUS_SAMPLES_FILENAME)
            .display()
            .to_string(),
        summary_path: output_dir
            .join(IR_DIR_CORPUS_SUMMARY_FILENAME)
            .display()
            .to_string(),
        joined_jsonl_path: joined_jsonl_path.display().to_string(),
        joined_csv_path: joined_csv_path.display().to_string(),
        export_root: output_dir
            .join(IR_DIR_CORPUS_EXPORTED_ARTIFACTS_DIR)
            .display()
            .to_string(),
        workspace_store_dir: workspace_store_dir.display().to_string(),
        workspace_artifacts_via_sled: workspace_artifacts_via_sled.display().to_string(),
        refreshed_public_outputs: refresh_public_outputs,
        generated_utc: now,
        sample_counts,
        action_counts,
        throughput_window_seconds,
        sample_throughput_basis: sample_rate.basis,
        sample_throughput_per_hour: sample_rate.per_hour,
        action_throughput_basis: action_rate.basis,
        action_throughput_per_hour: action_rate.per_hour,
        eta_scope: "running_plus_pending_samples".to_string(),
        eta_seconds,
        eta_human: eta_seconds.map(format_eta_seconds),
        ready_output_counts: ready_output_counts(&sample_records),
        public_outputs_present,
        exported_sample_dirs_on_disk: exported_sample_dirs_on_disk(output_dir)?,
        failed_sample_examples: failed_sample_examples_for(&sample_records, failed_sample_examples),
    })
}

fn sample_specs_from_manifest(manifest: &IrDirCorpusManifest) -> Vec<CorpusSampleSpec> {
    manifest
        .samples
        .iter()
        .map(|sample| CorpusSampleSpec {
            sample_id: sample.sample_id.clone(),
            logical_name: sample.logical_name.clone(),
            source_path: Path::new(&manifest.input_dir).join(&sample.source_relpath),
            source_relpath: sample.source_relpath.clone(),
            source_sha256: sample.source_sha256.clone(),
            top_fn_name: sample.top_fn_name.clone(),
        })
        .collect()
}

fn corpus_workspace_paths(output_dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let workspace_dir = output_dir.join(IR_DIR_CORPUS_INTERNAL_DIR);
    let workspace_store_dir = workspace_dir.join(IR_DIR_CORPUS_INTERNAL_STORE_DIR);
    let workspace_artifacts_via_sled = workspace_dir.join(IR_DIR_CORPUS_INTERNAL_SLED_FILENAME);
    (
        workspace_dir,
        workspace_store_dir,
        workspace_artifacts_via_sled,
    )
}

fn preserve_persisted_run_failure(
    execution_mode: &str,
    persisted_sample: &IrDirCorpusSampleRecord,
    live_sample: &mut IrDirCorpusSampleRecord,
) {
    if execution_mode != execution_mode_label(CorpusExecutionMode::Run) {
        return;
    }
    if live_sample.status != "missing" || live_sample.error.is_some() {
        return;
    }
    if persisted_sample.status != "failed" {
        return;
    }
    let Some(error) = persisted_sample.error.clone() else {
        return;
    };
    live_sample.status = "failed".to_string();
    live_sample.error = Some(error);
}

fn parse_top_fn_policy_label(label: &str) -> Result<CorpusTopFnPolicy> {
    match label {
        "infer_single_package" => Ok(CorpusTopFnPolicy::InferSinglePackage),
        "explicit" => Ok(CorpusTopFnPolicy::Explicit),
        "from_filename" => Ok(CorpusTopFnPolicy::FromFilename),
        other => bail!("unsupported corpus top-fn policy in manifest: {}", other),
    }
}

fn empty_action_counts() -> BTreeMap<String, usize> {
    [
        "planned_unique",
        "done",
        "running",
        "pending",
        "failed",
        "canceled",
        "missing",
        "done_missing_artifact",
    ]
    .into_iter()
    .map(|key| (key.to_string(), 0))
    .collect()
}

fn ensure_sample_count_key(counts: &mut BTreeMap<String, usize>, key: &str) {
    counts.entry(key.to_string()).or_insert(0);
}

fn joined_output_path(output_dir: &Path, recipe_preset: &str, extension: &str) -> PathBuf {
    output_dir
        .join(IR_DIR_CORPUS_JOINED_DIR)
        .join(format!("{}.{}", recipe_preset, extension))
}

fn is_sled_lock_error(err: &anyhow::Error) -> bool {
    format!("{:#}", err).contains("could not acquire lock")
}

fn action_completion_utc(
    store: &ArtifactStore,
    action_id: &str,
    status_query_mode: CorpusStatusQueryMode,
) -> Result<Option<DateTime<Utc>>> {
    if let Some(done) = load_queue_done_record(store, action_id)? {
        return Ok(Some(done.completed_utc));
    }
    if status_query_mode == CorpusStatusQueryMode::QueueFilesOnly {
        return Ok(None);
    }
    Ok(Some(
        store
            .load_provenance(action_id)
            .with_context(|| format!("loading action provenance for {}", action_id))?
            .created_utc,
    ))
}

fn collect_sample_completion_times(
    store: &ArtifactStore,
    sample_records: &[IrDirCorpusSampleRecord],
    status_query_mode: CorpusStatusQueryMode,
) -> Result<Vec<DateTime<Utc>>> {
    let mut out = Vec::new();
    for sample in sample_records {
        if sample.status != "done" {
            continue;
        }
        if let Some(completed_utc) =
            action_completion_utc(store, &sample.aig_stat_diff_action_id, status_query_mode)?
        {
            out.push(completed_utc);
        }
    }
    Ok(out)
}

#[derive(Debug, Clone)]
struct CompletionRateEstimate {
    per_hour: Option<f64>,
    basis: String,
}

fn completion_rate_per_hour(
    completion_times: &[DateTime<Utc>],
    now: DateTime<Utc>,
    throughput_window_seconds: i64,
) -> CompletionRateEstimate {
    if completion_times.is_empty() {
        return CompletionRateEstimate {
            per_hour: None,
            basis: "no_completed_history".to_string(),
        };
    }
    let window = throughput_window_seconds.max(1);
    let window_start = now - ChronoDuration::seconds(window);
    let recent_count = completion_times
        .iter()
        .filter(|completed_utc| **completed_utc >= window_start)
        .count();
    if recent_count > 0 {
        return CompletionRateEstimate {
            per_hour: Some(recent_count as f64 * 3600.0 / window as f64),
            basis: format!("recent_window_{}s", window),
        };
    }

    let mut sorted = completion_times.to_vec();
    sorted.sort();
    if sorted.len() < 2 {
        return CompletionRateEstimate {
            per_hour: None,
            basis: "insufficient_completed_history".to_string(),
        };
    }
    let elapsed_seconds = (sorted.last().copied().expect("last")
        - sorted.first().copied().expect("first"))
    .num_seconds()
    .max(1);
    CompletionRateEstimate {
        per_hour: Some(sorted.len() as f64 * 3600.0 / elapsed_seconds as f64),
        basis: "overall_completed_history".to_string(),
    }
}

fn estimate_eta_seconds(remaining_samples: usize, rate_per_hour: Option<f64>) -> Option<i64> {
    if remaining_samples == 0 {
        return Some(0);
    }
    let rate_per_hour = rate_per_hour?;
    if rate_per_hour <= 0.0 {
        return None;
    }
    Some(((remaining_samples as f64 / rate_per_hour) * 3600.0).ceil() as i64)
}

fn format_eta_seconds(total_seconds: i64) -> String {
    if total_seconds <= 0 {
        return "0s".to_string();
    }
    let mut remaining = total_seconds;
    let days = remaining / 86_400;
    remaining %= 86_400;
    let hours = remaining / 3_600;
    remaining %= 3_600;
    let minutes = remaining / 60;
    let seconds = remaining % 60;
    let mut parts = Vec::new();
    if days > 0 {
        parts.push(format!("{}d", days));
    }
    if hours > 0 {
        parts.push(format!("{}h", hours));
    }
    if minutes > 0 {
        parts.push(format!("{}m", minutes));
    }
    if seconds > 0 || parts.is_empty() {
        parts.push(format!("{}s", seconds));
    }
    parts.join("")
}

fn ready_output_counts(sample_records: &[IrDirCorpusSampleRecord]) -> BTreeMap<String, usize> {
    let mut counts: BTreeMap<String, usize> = [
        "input_ir",
        "g8r_aig",
        "g8r_stats",
        "combo_verilog",
        "yosys_abc_aig",
        "yosys_abc_stats",
        "aig_stat_diff",
    ]
    .into_iter()
    .map(|key| (key.to_string(), 0))
    .collect();

    for sample in sample_records {
        let checks = [
            ("input_ir", sample.import_ir_status.as_str()),
            ("g8r_aig", sample.g8r_aig_status.as_str()),
            ("g8r_stats", sample.g8r_stats_status.as_str()),
            ("combo_verilog", sample.combo_verilog_status.as_str()),
            ("yosys_abc_aig", sample.yosys_abc_aig_status.as_str()),
            ("yosys_abc_stats", sample.yosys_abc_stats_status.as_str()),
            ("aig_stat_diff", sample.aig_stat_diff_status.as_str()),
        ];
        for (key, status) in checks {
            if status_has_ready_output(status) {
                *counts.get_mut(key).expect("known ready output key") += 1;
            }
        }
    }

    counts
}

fn status_has_ready_output(status: &str) -> bool {
    matches!(status, "done" | "done_missing_artifact")
}

fn public_outputs_present(
    manifest_path: &Path,
    samples_path: &Path,
    summary_path: &Path,
    joined_jsonl_path: &Path,
    joined_csv_path: &Path,
) -> BTreeMap<String, bool> {
    [
        ("manifest_json", manifest_path.exists()),
        ("samples_jsonl", samples_path.exists()),
        ("summary_json", summary_path.exists()),
        ("joined_jsonl", joined_jsonl_path.exists()),
        ("joined_csv", joined_csv_path.exists()),
    ]
    .into_iter()
    .map(|(key, present)| (key.to_string(), present))
    .collect()
}

fn exported_sample_dirs_on_disk(output_dir: &Path) -> Result<usize> {
    let export_root = output_dir.join(IR_DIR_CORPUS_EXPORTED_ARTIFACTS_DIR);
    if !export_root.exists() {
        return Ok(0);
    }
    let mut count = 0_usize;
    for entry in fs::read_dir(&export_root)
        .with_context(|| format!("reading export root: {}", export_root.display()))?
    {
        let entry =
            entry.with_context(|| format!("reading export entry in {}", export_root.display()))?;
        if entry
            .file_type()
            .with_context(|| format!("reading file type for {}", entry.path().display()))?
            .is_dir()
        {
            count += 1;
        }
    }
    Ok(count)
}

fn failed_sample_examples_for(
    sample_records: &[IrDirCorpusSampleRecord],
    limit: usize,
) -> Vec<IrDirCorpusFailureExample> {
    sample_records
        .iter()
        .filter_map(|sample| {
            let error = sample.error.as_ref()?;
            Some(IrDirCorpusFailureExample {
                sample_id: sample.sample_id.clone(),
                source_relpath: sample.source_relpath.clone(),
                error: error.clone(),
            })
        })
        .take(limit)
        .collect()
}

fn discover_corpus_samples(
    input_dir: &Path,
    output_dir: &Path,
    top_fn_policy: CorpusTopFnPolicy,
    top_fn_name: Option<&str>,
) -> Result<Vec<CorpusSampleSpec>> {
    fn is_hidden_internal_dir(entry: &DirEntry) -> bool {
        entry.file_type().is_dir()
            && entry.file_name().to_string_lossy() == IR_DIR_CORPUS_INTERNAL_DIR
    }

    let mut samples = Vec::new();
    let walker = WalkDir::new(input_dir)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|entry| {
            if is_hidden_internal_dir(entry) {
                return false;
            }
            !entry.path().starts_with(output_dir)
        });
    for entry in walker {
        let entry = entry.context("walking input dir")?;
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if !file_name.ends_with(".ir") {
            continue;
        }
        let rel = path
            .strip_prefix(input_dir)
            .with_context(|| format!("stripping input dir prefix: {}", path.display()))?;
        let source_relpath = rel.to_string_lossy().replace('\\', "/");
        let logical_name = source_relpath.clone();
        let source_sha256 = sha256_file(path)?;
        let resolved_top = resolve_top_fn_name(path, top_fn_policy, top_fn_name)?;
        let sample_id = sample_id_for_relpath(&source_relpath);
        samples.push(CorpusSampleSpec {
            sample_id,
            logical_name,
            source_path: path.to_path_buf(),
            source_relpath,
            source_sha256,
            top_fn_name: resolved_top,
        });
    }
    samples.sort_by(|a, b| a.source_relpath.cmp(&b.source_relpath));
    Ok(samples)
}

fn resolve_top_fn_name(
    source_path: &Path,
    policy: CorpusTopFnPolicy,
    explicit_top_fn_name: Option<&str>,
) -> Result<String> {
    match policy {
        CorpusTopFnPolicy::InferSinglePackage => infer_ir_top_function(source_path).with_context(
            || {
                format!(
                    "infer-single-package expects one unambiguous top function in {}; use --top-fn-policy explicit or from-filename for multi-function corpora",
                    source_path.display()
                )
            },
        ),
        CorpusTopFnPolicy::Explicit => Ok(explicit_top_fn_name
            .ok_or_else(|| anyhow!("--top-fn-name is required for explicit top-fn policy"))?
            .to_string()),
        CorpusTopFnPolicy::FromFilename => top_fn_name_from_filename(source_path),
    }
}

fn top_fn_name_from_filename(source_path: &Path) -> Result<String> {
    let file_name = source_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("non-utf8 input filename: {}", source_path.display()))?;
    let stem = file_name
        .strip_suffix(".opt.ir")
        .or_else(|| file_name.strip_suffix(".ir"))
        .unwrap_or(file_name)
        .trim();
    if stem.is_empty() {
        bail!(
            "could not derive top function name from filename: {}",
            source_path.display()
        );
    }
    Ok(stem.to_string())
}

fn sample_id_for_relpath(source_relpath: &str) -> String {
    let digest = sha2::Sha256::digest(source_relpath.as_bytes());
    let hash = hex::encode(digest);
    let readable = Path::new(source_relpath)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(source_relpath)
        .trim_end_matches(".opt.ir")
        .trim_end_matches(".ir")
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' => ch.to_ascii_lowercase(),
            _ => '-',
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    let readable = if readable.is_empty() {
        "sample".to_string()
    } else {
        readable
    };
    format!("{}-{}", readable, &hash[..12])
}

fn build_action_plan(
    sample: &CorpusSampleSpec,
    fraig: bool,
    version: &str,
    driver_runtime: &DriverRuntimeSpec,
    stats_runtime: &DriverRuntimeSpec,
    yosys_runtime: &YosysRuntimeSpec,
    yosys_script_ref: &crate::model::ScriptRef,
) -> Result<CorpusActionPlan> {
    let import_action = ActionSpec::ImportIrPackageFile {
        source_sha256: sample.source_sha256.clone(),
        top_fn_name: Some(sample.top_fn_name.clone()),
    };
    let import_ir_action_id = compute_action_id(&import_action)?;
    let g8r_aig_action = ActionSpec::DriverIrToG8rAig {
        ir_action_id: import_ir_action_id.clone(),
        top_fn_name: Some(sample.top_fn_name.clone()),
        fraig,
        lowering_mode: crate::model::G8rLoweringMode::Default,
        version: version.to_string(),
        runtime: driver_runtime.clone(),
    };
    let g8r_aig_action_id = compute_action_id(&g8r_aig_action)?;
    let g8r_stats_action = ActionSpec::DriverAigToStats {
        aig_action_id: g8r_aig_action_id.clone(),
        version: version.to_string(),
        runtime: stats_runtime.clone(),
    };
    let g8r_stats_action_id = compute_action_id(&g8r_stats_action)?;
    let combo_verilog_action = ActionSpec::IrFnToCombinationalVerilog {
        ir_action_id: import_ir_action_id.clone(),
        top_fn_name: Some(sample.top_fn_name.clone()),
        use_system_verilog: false,
        version: version.to_string(),
        runtime: driver_runtime.clone(),
    };
    let combo_verilog_action_id = compute_action_id(&combo_verilog_action)?;
    let yosys_abc_aig_action = ActionSpec::ComboVerilogToYosysAbcAig {
        verilog_action_id: combo_verilog_action_id.clone(),
        verilog_top_module_name: Some(sample.top_fn_name.clone()),
        yosys_script_ref: yosys_script_ref.clone(),
        runtime: yosys_runtime.clone(),
    };
    let yosys_abc_aig_action_id = compute_action_id(&yosys_abc_aig_action)?;
    let yosys_abc_stats_action = ActionSpec::DriverAigToStats {
        aig_action_id: yosys_abc_aig_action_id.clone(),
        version: version.to_string(),
        runtime: stats_runtime.clone(),
    };
    let yosys_abc_stats_action_id = compute_action_id(&yosys_abc_stats_action)?;
    let aig_stat_diff_action = ActionSpec::AigStatDiff {
        opt_ir_action_id: import_ir_action_id.clone(),
        g8r_aig_stats_action_id: g8r_stats_action_id.clone(),
        yosys_abc_aig_stats_action_id: yosys_abc_stats_action_id.clone(),
    };
    let aig_stat_diff_action_id = compute_action_id(&aig_stat_diff_action)?;

    Ok(CorpusActionPlan {
        import_action,
        g8r_aig_action,
        g8r_stats_action,
        combo_verilog_action,
        yosys_abc_aig_action,
        yosys_abc_stats_action,
        aig_stat_diff_action,
        import_ir_action_id,
        g8r_aig_action_id,
        g8r_stats_action_id,
        combo_verilog_action_id,
        yosys_abc_aig_action_id,
        yosys_abc_stats_action_id,
        aig_stat_diff_action_id,
    })
}

fn ensure_imported_ir_action(
    store: &ArtifactStore,
    sample: &CorpusSampleSpec,
    import_action: &ActionSpec,
) -> Result<()> {
    let action_id = compute_action_id(import_action)?;
    if store.action_exists(&action_id) {
        return Ok(());
    }

    let staging_dir = store
        .staging_dir()
        .join(format!("{action_id}-corpus-import"));
    if staging_dir.exists() {
        fs::remove_dir_all(&staging_dir).with_context(|| {
            format!(
                "removing stale import staging dir: {}",
                staging_dir.display()
            )
        })?;
    }
    let payload_dir = staging_dir.join("payload");
    fs::create_dir_all(&payload_dir)
        .with_context(|| format!("creating payload dir: {}", payload_dir.display()))?;
    let imported_path = payload_dir.join("input.ir");
    fs::copy(&sample.source_path, &imported_path).with_context(|| {
        format!(
            "copying imported IR {} -> {}",
            sample.source_path.display(),
            imported_path.display()
        )
    })?;
    let output_files = collect_output_files(&payload_dir)?;
    let provenance = Provenance {
        schema_version: crate::ACTION_SCHEMA_VERSION,
        action_id: action_id.clone(),
        created_utc: Utc::now(),
        action: import_action.clone(),
        dependencies: Vec::new(),
        output_artifact: ArtifactRef {
            action_id: action_id.clone(),
            artifact_type: ArtifactType::IrPackageFile,
            relpath: IMPORTED_IR_RELPATH.to_string(),
        },
        output_files,
        commands: Vec::new(),
        details: json!({
            "source_sha256": sample.source_sha256,
            "ir_top": sample.top_fn_name,
            "import_kind": "local_ir_file",
        }),
        suggested_next_actions: Vec::new(),
    };
    let provenance_path = staging_dir.join("provenance.json");
    fs::write(
        &provenance_path,
        serde_json::to_string_pretty(&provenance).context("serializing imported IR provenance")?,
    )
    .with_context(|| format!("writing provenance: {}", provenance_path.display()))?;
    store.promote_staging_action_dir(&action_id, &staging_dir)?;
    Ok(())
}

fn enqueue_plan(
    store: &ArtifactStore,
    plan: &CorpusActionPlan,
    base_priority: i32,
) -> Result<usize> {
    let actions = [
        &plan.g8r_aig_action,
        &plan.g8r_stats_action,
        &plan.combo_verilog_action,
        &plan.yosys_abc_aig_action,
        &plan.yosys_abc_stats_action,
        &plan.aig_stat_diff_action,
    ];
    let mut enqueued = 0_usize;
    for action in actions {
        let action_id = compute_action_id(action)?;
        let was_known = store.action_exists(&action_id)
            || !matches!(
                queue_state_for_action(store, &action_id),
                crate::queue::QueueState::None
            );
        enqueue_action_with_priority(
            store,
            action.clone(),
            suggested_action_queue_priority(base_priority, action),
        )?;
        if !was_known && !store.action_exists(&action_id) {
            enqueued += 1;
        }
    }
    Ok(enqueued)
}

fn execute_plan(
    store: &ArtifactStore,
    plan: &CorpusActionPlan,
    counters: &mut ExecutionCounters,
) -> Result<()> {
    let actions = [
        &plan.g8r_aig_action,
        &plan.g8r_stats_action,
        &plan.combo_verilog_action,
        &plan.yosys_abc_aig_action,
        &plan.yosys_abc_stats_action,
        &plan.aig_stat_diff_action,
    ];
    for action in actions {
        let action_id = compute_action_id(action)?;
        let existed = store.action_exists(&action_id);
        let _ = execute_action(store, action.clone())?;
        if !existed {
            counters.executed_actions += 1;
        }
    }
    Ok(())
}

fn build_sample_record(
    store: &ArtifactStore,
    sample: &CorpusSampleSpec,
    plan: &CorpusActionPlan,
    run_errors: &BTreeMap<String, String>,
    top_fn_policy: CorpusTopFnPolicy,
    fraig: bool,
    dso_version: &str,
    driver_runtime: &DriverRuntimeSpec,
    stats_runtime: &DriverRuntimeSpec,
    yosys_script_ref: &crate::model::ScriptRef,
) -> IrDirCorpusSampleRecord {
    let import_ir_status = action_status_label(store, &plan.import_ir_action_id);
    let g8r_aig_status = action_status_label(store, &plan.g8r_aig_action_id);
    let g8r_stats_status = action_status_label(store, &plan.g8r_stats_action_id);
    let combo_verilog_status = action_status_label(store, &plan.combo_verilog_action_id);
    let yosys_abc_aig_status = action_status_label(store, &plan.yosys_abc_aig_action_id);
    let yosys_abc_stats_status = action_status_label(store, &plan.yosys_abc_stats_action_id);
    let aig_stat_diff_status = action_status_label(store, &plan.aig_stat_diff_action_id);

    let terminal_error = action_error_summary(store, &plan.aig_stat_diff_action_id)
        .or_else(|| action_error_summary(store, &plan.yosys_abc_stats_action_id))
        .or_else(|| action_error_summary(store, &plan.yosys_abc_aig_action_id))
        .or_else(|| action_error_summary(store, &plan.combo_verilog_action_id))
        .or_else(|| action_error_summary(store, &plan.g8r_stats_action_id))
        .or_else(|| action_error_summary(store, &plan.g8r_aig_action_id))
        .or_else(|| {
            run_errors
                .get(&sample.sample_id)
                .map(|s| summarize_error(s))
        });

    build_sample_record_with_statuses(
        sample,
        plan,
        top_fn_policy,
        fraig,
        dso_version,
        driver_runtime,
        stats_runtime,
        yosys_script_ref,
        import_ir_status,
        g8r_aig_status,
        g8r_stats_status,
        combo_verilog_status,
        yosys_abc_aig_status,
        yosys_abc_stats_status,
        aig_stat_diff_status,
        terminal_error,
    )
}

fn build_sample_record_from_queue_state(
    store: &ArtifactStore,
    sample: &CorpusSampleSpec,
    persisted_sample: &IrDirCorpusSampleRecord,
    plan: &CorpusActionPlan,
    top_fn_policy: CorpusTopFnPolicy,
    fraig: bool,
    dso_version: &str,
    driver_runtime: &DriverRuntimeSpec,
    stats_runtime: &DriverRuntimeSpec,
    yosys_script_ref: &crate::model::ScriptRef,
) -> IrDirCorpusSampleRecord {
    let import_ir_status = queue_or_persisted_action_status_label(
        store,
        &plan.import_ir_action_id,
        &persisted_sample.import_ir_status,
    );
    let g8r_aig_status = queue_or_persisted_action_status_label(
        store,
        &plan.g8r_aig_action_id,
        &persisted_sample.g8r_aig_status,
    );
    let g8r_stats_status = queue_or_persisted_action_status_label(
        store,
        &plan.g8r_stats_action_id,
        &persisted_sample.g8r_stats_status,
    );
    let combo_verilog_status = queue_or_persisted_action_status_label(
        store,
        &plan.combo_verilog_action_id,
        &persisted_sample.combo_verilog_status,
    );
    let yosys_abc_aig_status = queue_or_persisted_action_status_label(
        store,
        &plan.yosys_abc_aig_action_id,
        &persisted_sample.yosys_abc_aig_status,
    );
    let yosys_abc_stats_status = queue_or_persisted_action_status_label(
        store,
        &plan.yosys_abc_stats_action_id,
        &persisted_sample.yosys_abc_stats_status,
    );
    let aig_stat_diff_status = queue_or_persisted_action_status_label(
        store,
        &plan.aig_stat_diff_action_id,
        &persisted_sample.aig_stat_diff_status,
    );

    let statuses = [
        aig_stat_diff_status.as_str(),
        yosys_abc_stats_status.as_str(),
        yosys_abc_aig_status.as_str(),
        g8r_stats_status.as_str(),
        g8r_aig_status.as_str(),
        combo_verilog_status.as_str(),
    ];
    let terminal_error = action_error_summary(store, &plan.aig_stat_diff_action_id)
        .or_else(|| action_error_summary(store, &plan.yosys_abc_stats_action_id))
        .or_else(|| action_error_summary(store, &plan.yosys_abc_aig_action_id))
        .or_else(|| action_error_summary(store, &plan.combo_verilog_action_id))
        .or_else(|| action_error_summary(store, &plan.g8r_stats_action_id))
        .or_else(|| action_error_summary(store, &plan.g8r_aig_action_id))
        .or_else(|| {
            if summarize_sample_status(&statuses, false) == "failed" {
                persisted_sample.error.clone()
            } else {
                None
            }
        });

    build_sample_record_with_statuses(
        sample,
        plan,
        top_fn_policy,
        fraig,
        dso_version,
        driver_runtime,
        stats_runtime,
        yosys_script_ref,
        import_ir_status,
        g8r_aig_status,
        g8r_stats_status,
        combo_verilog_status,
        yosys_abc_aig_status,
        yosys_abc_stats_status,
        aig_stat_diff_status,
        terminal_error,
    )
}

fn build_sample_record_with_statuses(
    sample: &CorpusSampleSpec,
    plan: &CorpusActionPlan,
    top_fn_policy: CorpusTopFnPolicy,
    fraig: bool,
    dso_version: &str,
    driver_runtime: &DriverRuntimeSpec,
    stats_runtime: &DriverRuntimeSpec,
    yosys_script_ref: &crate::model::ScriptRef,
    import_ir_status: String,
    g8r_aig_status: String,
    g8r_stats_status: String,
    combo_verilog_status: String,
    yosys_abc_aig_status: String,
    yosys_abc_stats_status: String,
    aig_stat_diff_status: String,
    terminal_error: Option<String>,
) -> IrDirCorpusSampleRecord {
    let statuses = [
        aig_stat_diff_status.as_str(),
        yosys_abc_stats_status.as_str(),
        yosys_abc_aig_status.as_str(),
        g8r_stats_status.as_str(),
        g8r_aig_status.as_str(),
        combo_verilog_status.as_str(),
    ];
    let overall_status = summarize_sample_status(&statuses, terminal_error.is_some());

    IrDirCorpusSampleRecord {
        sample_id: sample.sample_id.clone(),
        logical_name: sample.logical_name.clone(),
        source_relpath: sample.source_relpath.clone(),
        source_sha256: sample.source_sha256.clone(),
        top_fn_policy: top_fn_policy_label(top_fn_policy).to_string(),
        top_fn_name: sample.top_fn_name.clone(),
        fraig,
        dso_version: dso_version.to_string(),
        driver_crate_version: driver_runtime.driver_version.clone(),
        stats_driver_crate_version: stats_runtime.driver_version.clone(),
        yosys_script: yosys_script_ref.path.clone(),
        yosys_script_sha256: yosys_script_ref.sha256.clone(),
        preset: recipe_preset_label(CorpusRecipePreset::G8rVsYabcAigDiff).to_string(),
        import_ir_action_id: plan.import_ir_action_id.clone(),
        import_ir_status,
        g8r_aig_action_id: plan.g8r_aig_action_id.clone(),
        g8r_aig_status,
        g8r_stats_action_id: plan.g8r_stats_action_id.clone(),
        g8r_stats_status,
        combo_verilog_action_id: plan.combo_verilog_action_id.clone(),
        combo_verilog_status,
        yosys_abc_aig_action_id: plan.yosys_abc_aig_action_id.clone(),
        yosys_abc_aig_status,
        yosys_abc_stats_action_id: plan.yosys_abc_stats_action_id.clone(),
        yosys_abc_stats_status,
        aig_stat_diff_action_id: plan.aig_stat_diff_action_id.clone(),
        aig_stat_diff_status,
        status: overall_status,
        error: terminal_error,
    }
}

fn queue_or_persisted_action_status_label(
    store: &ArtifactStore,
    action_id: &str,
    persisted_status: &str,
) -> String {
    match queue_state_for_action(store, action_id) {
        crate::queue::QueueState::Pending => "pending".to_string(),
        crate::queue::QueueState::Running { .. } => "running".to_string(),
        crate::queue::QueueState::Done => "done".to_string(),
        crate::queue::QueueState::Failed => "failed".to_string(),
        crate::queue::QueueState::Canceled => "canceled".to_string(),
        crate::queue::QueueState::None => persisted_status.to_string(),
    }
}

fn corpus_sample_action_statuses(sample: &IrDirCorpusSampleRecord) -> [(&str, &str); 7] {
    [
        (&sample.import_ir_action_id, &sample.import_ir_status),
        (&sample.g8r_aig_action_id, &sample.g8r_aig_status),
        (&sample.g8r_stats_action_id, &sample.g8r_stats_status),
        (
            &sample.combo_verilog_action_id,
            &sample.combo_verilog_status,
        ),
        (
            &sample.yosys_abc_aig_action_id,
            &sample.yosys_abc_aig_status,
        ),
        (
            &sample.yosys_abc_stats_action_id,
            &sample.yosys_abc_stats_status,
        ),
        (
            &sample.aig_stat_diff_action_id,
            &sample.aig_stat_diff_status,
        ),
    ]
}

fn action_status_label(store: &ArtifactStore, action_id: &str) -> String {
    if store.action_exists(action_id) {
        return "done".to_string();
    }
    match queue_state_for_action(store, action_id) {
        crate::queue::QueueState::Pending => "pending".to_string(),
        crate::queue::QueueState::Running { .. } => "running".to_string(),
        crate::queue::QueueState::Done => "done_missing_artifact".to_string(),
        crate::queue::QueueState::Failed => "failed".to_string(),
        crate::queue::QueueState::Canceled => "canceled".to_string(),
        crate::queue::QueueState::None => "missing".to_string(),
    }
}

fn action_error_summary(store: &ArtifactStore, action_id: &str) -> Option<String> {
    if let Ok(Some(failed)) = load_queue_failed_record(store, action_id) {
        return Some(summarize_error(&failed.error));
    }
    if let Ok(Some(canceled)) = load_queue_canceled_record(store, action_id) {
        return Some(summarize_error(&canceled.reason));
    }
    None
}

fn summarize_sample_status(statuses: &[&str], has_error: bool) -> String {
    if statuses.first().copied() == Some("done") {
        return "done".to_string();
    }
    if has_error
        || statuses
            .iter()
            .any(|status| matches!(*status, "failed" | "canceled"))
    {
        return "failed".to_string();
    }
    if statuses.iter().any(|status| *status == "running") {
        return "running".to_string();
    }
    if statuses.iter().any(|status| *status == "pending") {
        return "pending".to_string();
    }
    "missing".to_string()
}

fn build_joined_rows(
    store: &ArtifactStore,
    sample_records: &[IrDirCorpusSampleRecord],
) -> Result<Vec<IrDirCorpusJoinedRow>> {
    let mut rows = Vec::new();
    for sample in sample_records {
        if sample.status != "done" {
            continue;
        }
        let diff_provenance = store
            .load_provenance(&sample.aig_stat_diff_action_id)
            .with_context(|| {
                format!(
                    "loading aig-stat-diff provenance for sample {}",
                    sample.sample_id
                )
            })?;
        let diff_path = store.resolve_artifact_ref_path(&diff_provenance.output_artifact);
        let diff_json: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&diff_path)
                .with_context(|| format!("reading diff JSON: {}", diff_path.display()))?,
        )
        .with_context(|| format!("parsing diff JSON: {}", diff_path.display()))?;
        let g8r_stats = diff_json
            .get("g8r_stats")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let yosys_stats = diff_json
            .get("yosys_abc_stats")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let g8r_and_nodes = stats_metric(&g8r_stats, "and_nodes")
            .or_else(|| stats_metric(&g8r_stats, "live_nodes"));
        let g8r_depth = stats_metric(&g8r_stats, "depth");
        let g8r_product = product_metric(g8r_and_nodes, g8r_depth);
        let yosys_abc_and_nodes = stats_metric(&yosys_stats, "and_nodes")
            .or_else(|| stats_metric(&yosys_stats, "live_nodes"));
        let yosys_abc_depth = stats_metric(&yosys_stats, "depth");
        let yosys_abc_product = product_metric(yosys_abc_and_nodes, yosys_abc_depth);
        rows.push(IrDirCorpusJoinedRow {
            sample_id: sample.sample_id.clone(),
            logical_name: sample.logical_name.clone(),
            source_relpath: sample.source_relpath.clone(),
            source_sha256: sample.source_sha256.clone(),
            top_fn_policy: sample.top_fn_policy.clone(),
            top_fn_name: sample.top_fn_name.clone(),
            fraig: sample.fraig,
            dso_version: sample.dso_version.clone(),
            driver_crate_version: sample.driver_crate_version.clone(),
            stats_driver_crate_version: sample.stats_driver_crate_version.clone(),
            yosys_script: sample.yosys_script.clone(),
            yosys_script_sha256: sample.yosys_script_sha256.clone(),
            import_ir_action_id: sample.import_ir_action_id.clone(),
            g8r_aig_action_id: sample.g8r_aig_action_id.clone(),
            g8r_stats_action_id: sample.g8r_stats_action_id.clone(),
            combo_verilog_action_id: sample.combo_verilog_action_id.clone(),
            yosys_abc_aig_action_id: sample.yosys_abc_aig_action_id.clone(),
            yosys_abc_stats_action_id: sample.yosys_abc_stats_action_id.clone(),
            aig_stat_diff_action_id: sample.aig_stat_diff_action_id.clone(),
            g8r_and_nodes,
            g8r_depth,
            g8r_product,
            yosys_abc_and_nodes,
            yosys_abc_depth,
            yosys_abc_product,
            g8r_product_loss: match (g8r_product, yosys_abc_product) {
                (Some(g8r), Some(yosys)) => Some(g8r - yosys),
                _ => None,
            },
            delta_and_nodes_yosys_minus_g8r: diff_numeric_delta(&diff_json, "and_nodes")
                .or_else(|| diff_numeric_delta(&diff_json, "live_nodes")),
            delta_depth_yosys_minus_g8r: diff_numeric_delta(&diff_json, "depth"),
        });
    }
    rows.sort_by(|a, b| a.source_relpath.cmp(&b.source_relpath));
    Ok(rows)
}

fn stats_metric(value: &serde_json::Value, key: &str) -> Option<f64> {
    match value.get(key)? {
        serde_json::Value::Number(number) => number.as_f64(),
        serde_json::Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn diff_numeric_delta(value: &serde_json::Value, metric: &str) -> Option<f64> {
    let deltas = value.get("numeric_deltas_yosys_minus_g8r")?.as_array()?;
    for delta in deltas {
        if delta.get("metric").and_then(|v| v.as_str()) != Some(metric) {
            continue;
        }
        return match delta.get("delta_yosys_minus_g8r")? {
            serde_json::Value::Number(number) => number.as_f64(),
            serde_json::Value::String(text) => text.parse::<f64>().ok(),
            _ => None,
        };
    }
    None
}

fn product_metric(and_nodes: Option<f64>, depth: Option<f64>) -> Option<f64> {
    Some(and_nodes? * depth?)
}

fn export_leaf_artifacts(
    store: &ArtifactStore,
    output_dir: &Path,
    sample_records: &[IrDirCorpusSampleRecord],
) -> Result<()> {
    let export_root = output_dir.join(IR_DIR_CORPUS_EXPORTED_ARTIFACTS_DIR);
    fs::create_dir_all(&export_root)
        .with_context(|| format!("creating export root: {}", export_root.display()))?;
    let done_sample_ids: BTreeSet<&str> = sample_records
        .iter()
        .filter(|sample| sample.status == "done")
        .map(|sample| sample.sample_id.as_str())
        .collect();
    for entry in fs::read_dir(&export_root)
        .with_context(|| format!("reading export root: {}", export_root.display()))?
    {
        let entry =
            entry.with_context(|| format!("reading export entry in {}", export_root.display()))?;
        if !entry
            .file_type()
            .with_context(|| format!("reading file type for {}", entry.path().display()))?
            .is_dir()
        {
            continue;
        }
        let entry_name = entry.file_name();
        let entry_name = entry_name.to_string_lossy();
        if done_sample_ids.contains(entry_name.as_ref()) {
            continue;
        }
        fs::remove_dir_all(entry.path()).with_context(|| {
            format!(
                "removing stale sample export dir: {}",
                entry.path().display()
            )
        })?;
    }
    for sample in sample_records {
        let sample_dir = export_root.join(&sample.sample_id);
        if sample_dir.exists() {
            fs::remove_dir_all(&sample_dir).with_context(|| {
                format!(
                    "removing stale sample export dir before refresh: {}",
                    sample_dir.display()
                )
            })?;
        }
        if sample.status != "done" {
            continue;
        }
        fs::create_dir_all(&sample_dir)
            .with_context(|| format!("creating sample export dir: {}", sample_dir.display()))?;
        copy_artifact_if_present(
            store,
            &ArtifactRef {
                action_id: sample.import_ir_action_id.clone(),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: IMPORTED_IR_RELPATH.to_string(),
            },
            &sample_dir.join("input.ir"),
        )?;
        copy_artifact_if_present(
            store,
            &ArtifactRef {
                action_id: sample.g8r_aig_action_id.clone(),
                artifact_type: ArtifactType::AigFile,
                relpath: G8R_AIG_RELPATH.to_string(),
            },
            &sample_dir.join("g8r.aig"),
        )?;
        copy_artifact_if_present(
            store,
            &ArtifactRef {
                action_id: sample.g8r_stats_action_id.clone(),
                artifact_type: ArtifactType::AigStatsFile,
                relpath: G8R_STATS_RELPATH.to_string(),
            },
            &sample_dir.join("g8r_stats.json"),
        )?;
        copy_artifact_if_present(
            store,
            &ArtifactRef {
                action_id: sample.combo_verilog_action_id.clone(),
                artifact_type: ArtifactType::VerilogFile,
                relpath: COMBO_VERILOG_RELPATH.to_string(),
            },
            &sample_dir.join("combo.v"),
        )?;
        copy_artifact_if_present(
            store,
            &ArtifactRef {
                action_id: sample.yosys_abc_aig_action_id.clone(),
                artifact_type: ArtifactType::AigFile,
                relpath: YOSYS_ABC_AIG_RELPATH.to_string(),
            },
            &sample_dir.join("yosys_abc.aig"),
        )?;
        copy_artifact_if_present(
            store,
            &ArtifactRef {
                action_id: sample.yosys_abc_stats_action_id.clone(),
                artifact_type: ArtifactType::AigStatsFile,
                relpath: YOSYS_ABC_STATS_RELPATH.to_string(),
            },
            &sample_dir.join("yosys_abc_stats.json"),
        )?;
        copy_artifact_if_present(
            store,
            &ArtifactRef {
                action_id: sample.aig_stat_diff_action_id.clone(),
                artifact_type: ArtifactType::AigStatDiffFile,
                relpath: AIG_STAT_DIFF_RELPATH.to_string(),
            },
            &sample_dir.join("aig_stat_diff.json"),
        )?;
    }
    Ok(())
}

fn copy_artifact_if_present(
    store: &ArtifactStore,
    artifact_ref: &ArtifactRef,
    dst: &Path,
) -> Result<()> {
    let src = store.resolve_artifact_ref_path(artifact_ref);
    if !src.exists() {
        return Ok(());
    }
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating export parent: {}", parent.display()))?;
    }
    fs::copy(&src, dst)
        .with_context(|| format!("copying artifact {} -> {}", src.display(), dst.display()))?;
    Ok(())
}

fn write_samples_jsonl(path: &Path, samples: &[IrDirCorpusSampleRecord]) -> Result<()> {
    let mut text = String::new();
    for sample in samples {
        text.push_str(
            &serde_json::to_string(sample).context("serializing corpus sample JSONL row")?,
        );
        text.push('\n');
    }
    fs::write(path, text).with_context(|| format!("writing samples jsonl: {}", path.display()))
}

fn write_joined_jsonl(path: &Path, rows: &[IrDirCorpusJoinedRow]) -> Result<()> {
    let mut text = String::new();
    for row in rows {
        text.push_str(&serde_json::to_string(row).context("serializing joined JSONL row")?);
        text.push('\n');
    }
    fs::write(path, text).with_context(|| format!("writing joined jsonl: {}", path.display()))
}

fn write_joined_csv(path: &Path, rows: &[IrDirCorpusJoinedRow]) -> Result<()> {
    let mut text = String::new();
    text.push_str(
        "sample_id,logical_name,source_relpath,source_sha256,top_fn_policy,top_fn_name,fraig,dso_version,driver_crate_version,stats_driver_crate_version,yosys_script,yosys_script_sha256,import_ir_action_id,g8r_aig_action_id,g8r_stats_action_id,combo_verilog_action_id,yosys_abc_aig_action_id,yosys_abc_stats_action_id,aig_stat_diff_action_id,g8r_and_nodes,g8r_depth,g8r_product,yosys_abc_and_nodes,yosys_abc_depth,yosys_abc_product,g8r_product_loss,delta_and_nodes_yosys_minus_g8r,delta_depth_yosys_minus_g8r\n",
    );
    for row in rows {
        let fields = [
            csv_escape(&row.sample_id),
            csv_escape(&row.logical_name),
            csv_escape(&row.source_relpath),
            csv_escape(&row.source_sha256),
            csv_escape(&row.top_fn_policy),
            csv_escape(&row.top_fn_name),
            csv_escape(&row.fraig.to_string()),
            csv_escape(&row.dso_version),
            csv_escape(&row.driver_crate_version),
            csv_escape(&row.stats_driver_crate_version),
            csv_escape(&row.yosys_script),
            csv_escape(&row.yosys_script_sha256),
            csv_escape(&row.import_ir_action_id),
            csv_escape(&row.g8r_aig_action_id),
            csv_escape(&row.g8r_stats_action_id),
            csv_escape(&row.combo_verilog_action_id),
            csv_escape(&row.yosys_abc_aig_action_id),
            csv_escape(&row.yosys_abc_stats_action_id),
            csv_escape(&row.aig_stat_diff_action_id),
            csv_escape(&optional_f64(row.g8r_and_nodes)),
            csv_escape(&optional_f64(row.g8r_depth)),
            csv_escape(&optional_f64(row.g8r_product)),
            csv_escape(&optional_f64(row.yosys_abc_and_nodes)),
            csv_escape(&optional_f64(row.yosys_abc_depth)),
            csv_escape(&optional_f64(row.yosys_abc_product)),
            csv_escape(&optional_f64(row.g8r_product_loss)),
            csv_escape(&optional_f64(row.delta_and_nodes_yosys_minus_g8r)),
            csv_escape(&optional_f64(row.delta_depth_yosys_minus_g8r)),
        ];
        text.push_str(&fields.join(","));
        text.push('\n');
    }
    fs::write(path, text).with_context(|| format!("writing joined csv: {}", path.display()))
}

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn optional_f64(value: Option<f64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn count_statuses(samples: &[IrDirCorpusSampleRecord]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for sample in samples {
        *counts.entry(sample.status.clone()).or_insert(0) += 1;
    }
    counts
}

fn recipe_preset_label(recipe_preset: CorpusRecipePreset) -> &'static str {
    match recipe_preset {
        CorpusRecipePreset::G8rVsYabcAigDiff => "g8r-vs-yabc-aig-diff",
    }
}

fn execution_mode_label(execution_mode: CorpusExecutionMode) -> &'static str {
    match execution_mode {
        CorpusExecutionMode::Enqueue => "enqueue",
        CorpusExecutionMode::Run => "run",
    }
}

fn top_fn_policy_label(policy: CorpusTopFnPolicy) -> &'static str {
    match policy {
        CorpusTopFnPolicy::InferSinglePackage => "infer_single_package",
        CorpusTopFnPolicy::Explicit => "explicit",
        CorpusTopFnPolicy::FromFilename => "from_filename",
    }
}

fn refresh_semantics_label() -> &'static str {
    IR_DIR_CORPUS_REFRESH_SEMANTICS
}

fn sample_id_scheme_label() -> &'static str {
    IR_DIR_CORPUS_SAMPLE_ID_SCHEME
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration as ChronoDuration, Utc};
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-corpus-test-{}-{}-{}",
            prefix,
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).expect("create temp dir");
        root
    }

    fn sample_driver_runtime() -> DriverRuntimeSpec {
        DriverRuntimeSpec {
            driver_version: "0.34.0".to_string(),
            release_platform: crate::DEFAULT_RELEASE_PLATFORM.to_string(),
            docker_image: crate::runtime::default_driver_image("0.34.0"),
            dockerfile: crate::DEFAULT_DOCKERFILE.to_string(),
        }
    }

    fn sample_stats_runtime() -> DriverRuntimeSpec {
        sample_driver_runtime()
    }

    fn sample_yosys_runtime() -> YosysRuntimeSpec {
        YosysRuntimeSpec {
            docker_image: crate::DEFAULT_YOSYS_DOCKER_IMAGE.to_string(),
            dockerfile: crate::DEFAULT_YOSYS_DOCKERFILE.to_string(),
        }
    }

    fn sample_yosys_script_ref() -> crate::model::ScriptRef {
        crate::model::ScriptRef {
            path: crate::DEFAULT_YOSYS_FLOW_SCRIPT.to_string(),
            sha256: "a".repeat(64),
        }
    }

    fn stage_provenance_record(
        store: &ArtifactStore,
        action: ActionSpec,
        output_artifact: ArtifactRef,
        created_utc: DateTime<Utc>,
        details: serde_json::Value,
        staged_files: Vec<(String, Vec<u8>)>,
    ) -> Provenance {
        let action_id = compute_action_id(&action).expect("compute action id");
        let provenance = Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: action_id.clone(),
            created_utc,
            action,
            dependencies: Vec::new(),
            output_artifact,
            output_files: staged_files
                .iter()
                .map(|(path, bytes)| crate::model::OutputFile {
                    path: path.clone(),
                    bytes: bytes.len() as u64,
                    sha256: format!("{:064x}", bytes.len()),
                })
                .collect(),
            commands: Vec::new(),
            details,
            suggested_next_actions: Vec::new(),
        };
        let staging_dir = store.staging_dir().join(format!("{}-staged", action_id));
        fs::create_dir_all(&staging_dir).expect("create staging dir");
        for (relpath, bytes) in &staged_files {
            let path = staging_dir.join(relpath);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("create staged parent");
            }
            fs::write(path, bytes).expect("write staged file");
        }
        fs::write(
            staging_dir.join("provenance.json"),
            serde_json::to_string_pretty(&provenance).expect("serialize provenance"),
        )
        .expect("write staged provenance");
        store
            .promote_staging_action_dir(&action_id, &staging_dir)
            .expect("promote staged action");
        store
            .load_provenance(&action_id)
            .expect("reload staged provenance")
    }

    fn seed_manifest_sample_record(
        sample: &CorpusSampleSpec,
        plan: &CorpusActionPlan,
        driver_runtime: &DriverRuntimeSpec,
        stats_runtime: &DriverRuntimeSpec,
        yosys_script_ref: &crate::model::ScriptRef,
    ) -> IrDirCorpusSampleRecord {
        IrDirCorpusSampleRecord {
            sample_id: sample.sample_id.clone(),
            logical_name: sample.logical_name.clone(),
            source_relpath: sample.source_relpath.clone(),
            source_sha256: sample.source_sha256.clone(),
            top_fn_policy: top_fn_policy_label(CorpusTopFnPolicy::FromFilename).to_string(),
            top_fn_name: sample.top_fn_name.clone(),
            fraig: false,
            dso_version: "v0.39.0".to_string(),
            driver_crate_version: driver_runtime.driver_version.clone(),
            stats_driver_crate_version: stats_runtime.driver_version.clone(),
            yosys_script: yosys_script_ref.path.clone(),
            yosys_script_sha256: yosys_script_ref.sha256.clone(),
            preset: recipe_preset_label(CorpusRecipePreset::G8rVsYabcAigDiff).to_string(),
            import_ir_action_id: plan.import_ir_action_id.clone(),
            import_ir_status: "done".to_string(),
            g8r_aig_action_id: plan.g8r_aig_action_id.clone(),
            g8r_aig_status: "pending".to_string(),
            g8r_stats_action_id: plan.g8r_stats_action_id.clone(),
            g8r_stats_status: "pending".to_string(),
            combo_verilog_action_id: plan.combo_verilog_action_id.clone(),
            combo_verilog_status: "pending".to_string(),
            yosys_abc_aig_action_id: plan.yosys_abc_aig_action_id.clone(),
            yosys_abc_aig_status: "pending".to_string(),
            yosys_abc_stats_action_id: plan.yosys_abc_stats_action_id.clone(),
            yosys_abc_stats_status: "pending".to_string(),
            aig_stat_diff_action_id: plan.aig_stat_diff_action_id.clone(),
            aig_stat_diff_status: "pending".to_string(),
            status: "pending".to_string(),
            error: None,
        }
    }

    struct StatusFixture {
        root: PathBuf,
        output_dir: PathBuf,
        store: ArtifactStore,
        samples: Vec<CorpusSampleSpec>,
        plans: Vec<CorpusActionPlan>,
    }

    fn make_status_fixture() -> StatusFixture {
        let root = make_temp_dir("status-report");
        let output_dir = root.join("out");
        let input_dir = root.join("input");
        fs::create_dir_all(&output_dir).expect("create output dir");
        fs::create_dir_all(&input_dir).expect("create input dir");

        let workspace_dir = output_dir.join(IR_DIR_CORPUS_INTERNAL_DIR);
        let workspace_store_dir = workspace_dir.join(IR_DIR_CORPUS_INTERNAL_STORE_DIR);
        let workspace_artifacts_via_sled = workspace_dir.join(IR_DIR_CORPUS_INTERNAL_SLED_FILENAME);
        let store = ArtifactStore::new_with_sled(
            workspace_store_dir.clone(),
            workspace_artifacts_via_sled.clone(),
        );
        store.ensure_layout().expect("ensure store layout");

        let driver_runtime = sample_driver_runtime();
        let stats_runtime = sample_stats_runtime();
        let yosys_runtime = sample_yosys_runtime();
        let yosys_script_ref = sample_yosys_script_ref();
        let samples = vec![
            CorpusSampleSpec {
                sample_id: "done-sample".to_string(),
                logical_name: "done.ir".to_string(),
                source_path: input_dir.join("done.ir"),
                source_relpath: "done.ir".to_string(),
                source_sha256: "1".repeat(64),
                top_fn_name: "done_top".to_string(),
            },
            CorpusSampleSpec {
                sample_id: "pending-sample".to_string(),
                logical_name: "pending.ir".to_string(),
                source_path: input_dir.join("pending.ir"),
                source_relpath: "pending.ir".to_string(),
                source_sha256: "2".repeat(64),
                top_fn_name: "pending_top".to_string(),
            },
            CorpusSampleSpec {
                sample_id: "failed-sample".to_string(),
                logical_name: "failed.ir".to_string(),
                source_path: input_dir.join("failed.ir"),
                source_relpath: "failed.ir".to_string(),
                source_sha256: "3".repeat(64),
                top_fn_name: "failed_top".to_string(),
            },
        ];
        let plans: Vec<_> = samples
            .iter()
            .map(|sample| {
                build_action_plan(
                    sample,
                    false,
                    "v0.39.0",
                    &driver_runtime,
                    &stats_runtime,
                    &yosys_runtime,
                    &yosys_script_ref,
                )
                .expect("build action plan")
            })
            .collect();

        let manifest = IrDirCorpusManifest {
            schema_version: IR_DIR_CORPUS_MANIFEST_SCHEMA_VERSION,
            generated_utc: Utc::now(),
            input_dir: input_dir.display().to_string(),
            output_dir: output_dir.display().to_string(),
            workspace_dir: workspace_dir.display().to_string(),
            workspace_store_dir: workspace_store_dir.display().to_string(),
            workspace_artifacts_via_sled: workspace_artifacts_via_sled.display().to_string(),
            recipe_preset: recipe_preset_label(CorpusRecipePreset::G8rVsYabcAigDiff).to_string(),
            execution_mode: execution_mode_label(CorpusExecutionMode::Enqueue).to_string(),
            refresh_semantics: refresh_semantics_label().to_string(),
            sample_id_scheme: sample_id_scheme_label().to_string(),
            top_fn_policy: top_fn_policy_label(CorpusTopFnPolicy::FromFilename).to_string(),
            top_fn_name: None,
            fraig: false,
            dso_version: "v0.39.0".to_string(),
            driver_runtime: driver_runtime.clone(),
            stats_runtime: stats_runtime.clone(),
            yosys_runtime: yosys_runtime.clone(),
            yosys_script: yosys_script_ref.path.clone(),
            yosys_script_sha256: yosys_script_ref.sha256.clone(),
            samples: samples
                .iter()
                .zip(plans.iter())
                .map(|(sample, plan)| {
                    seed_manifest_sample_record(
                        sample,
                        plan,
                        &driver_runtime,
                        &stats_runtime,
                        &yosys_script_ref,
                    )
                })
                .collect(),
        };
        fs::write(
            output_dir.join(IR_DIR_CORPUS_MANIFEST_FILENAME),
            serde_json::to_string_pretty(&manifest).expect("serialize manifest"),
        )
        .expect("write manifest");

        StatusFixture {
            root,
            output_dir,
            store,
            samples,
            plans,
        }
    }

    fn read_status_manifest(output_dir: &Path) -> IrDirCorpusManifest {
        let manifest_path = output_dir.join(IR_DIR_CORPUS_MANIFEST_FILENAME);
        let manifest_text = fs::read_to_string(&manifest_path).expect("read manifest");
        serde_json::from_str(&manifest_text).expect("parse manifest")
    }

    fn write_status_manifest(output_dir: &Path, manifest: &IrDirCorpusManifest) {
        fs::write(
            output_dir.join(IR_DIR_CORPUS_MANIFEST_FILENAME),
            serde_json::to_string_pretty(manifest).expect("serialize manifest"),
        )
        .expect("write manifest");
    }

    fn read_samples_jsonl(output_dir: &Path) -> Vec<IrDirCorpusSampleRecord> {
        fs::read_to_string(output_dir.join(IR_DIR_CORPUS_SAMPLES_FILENAME))
            .expect("read samples jsonl")
            .lines()
            .map(|line| serde_json::from_str(line).expect("parse samples row"))
            .collect()
    }

    fn read_summary_json(output_dir: &Path) -> serde_json::Value {
        let summary_path = output_dir.join(IR_DIR_CORPUS_SUMMARY_FILENAME);
        let summary_text = fs::read_to_string(&summary_path).expect("read summary");
        serde_json::from_str(&summary_text).expect("parse summary")
    }

    fn seed_done_sample(
        store: &ArtifactStore,
        sample: &CorpusSampleSpec,
        plan: &CorpusActionPlan,
        created_utc: DateTime<Utc>,
    ) {
        stage_provenance_record(
            store,
            plan.import_action.clone(),
            ArtifactRef {
                action_id: plan.import_ir_action_id.clone(),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: IMPORTED_IR_RELPATH.to_string(),
            },
            created_utc,
            json!({
                "source_sha256": sample.source_sha256,
                "ir_top": sample.top_fn_name,
            }),
            vec![(
                IMPORTED_IR_RELPATH.to_string(),
                format!("package {}\n", sample.top_fn_name).into_bytes(),
            )],
        );
        stage_provenance_record(
            store,
            plan.g8r_aig_action.clone(),
            ArtifactRef {
                action_id: plan.g8r_aig_action_id.clone(),
                artifact_type: ArtifactType::AigFile,
                relpath: G8R_AIG_RELPATH.to_string(),
            },
            created_utc,
            json!({}),
            vec![(G8R_AIG_RELPATH.to_string(), b"aig".to_vec())],
        );
        stage_provenance_record(
            store,
            plan.g8r_stats_action.clone(),
            ArtifactRef {
                action_id: plan.g8r_stats_action_id.clone(),
                artifact_type: ArtifactType::AigStatsFile,
                relpath: G8R_STATS_RELPATH.to_string(),
            },
            created_utc,
            json!({}),
            vec![(
                G8R_STATS_RELPATH.to_string(),
                serde_json::to_vec_pretty(&json!({"and_nodes": 11, "depth": 7}))
                    .expect("serialize g8r stats"),
            )],
        );
        stage_provenance_record(
            store,
            plan.combo_verilog_action.clone(),
            ArtifactRef {
                action_id: plan.combo_verilog_action_id.clone(),
                artifact_type: ArtifactType::VerilogFile,
                relpath: COMBO_VERILOG_RELPATH.to_string(),
            },
            created_utc,
            json!({}),
            vec![(
                COMBO_VERILOG_RELPATH.to_string(),
                b"module done; endmodule\n".to_vec(),
            )],
        );
        stage_provenance_record(
            store,
            plan.yosys_abc_aig_action.clone(),
            ArtifactRef {
                action_id: plan.yosys_abc_aig_action_id.clone(),
                artifact_type: ArtifactType::AigFile,
                relpath: YOSYS_ABC_AIG_RELPATH.to_string(),
            },
            created_utc,
            json!({}),
            vec![(YOSYS_ABC_AIG_RELPATH.to_string(), b"yosys-aig".to_vec())],
        );
        stage_provenance_record(
            store,
            plan.yosys_abc_stats_action.clone(),
            ArtifactRef {
                action_id: plan.yosys_abc_stats_action_id.clone(),
                artifact_type: ArtifactType::AigStatsFile,
                relpath: YOSYS_ABC_STATS_RELPATH.to_string(),
            },
            created_utc,
            json!({}),
            vec![(
                YOSYS_ABC_STATS_RELPATH.to_string(),
                serde_json::to_vec_pretty(&json!({"and_nodes": 10, "depth": 6}))
                    .expect("serialize yosys stats"),
            )],
        );
        stage_provenance_record(
            store,
            plan.aig_stat_diff_action.clone(),
            ArtifactRef {
                action_id: plan.aig_stat_diff_action_id.clone(),
                artifact_type: ArtifactType::AigStatDiffFile,
                relpath: AIG_STAT_DIFF_RELPATH.to_string(),
            },
            created_utc,
            json!({}),
            vec![(
                AIG_STAT_DIFF_RELPATH.to_string(),
                serde_json::to_vec_pretty(&json!({
                    "g8r_stats": {"and_nodes": 11, "depth": 7},
                    "yosys_abc_stats": {"and_nodes": 10, "depth": 6},
                    "numeric_deltas_yosys_minus_g8r": [
                        {"metric": "and_nodes", "delta_yosys_minus_g8r": -1},
                        {"metric": "depth", "delta_yosys_minus_g8r": -1}
                    ]
                }))
                .expect("serialize diff"),
            )],
        );
    }

    #[test]
    fn top_fn_name_from_filename_strips_opt_ir_suffix() {
        let path = Path::new("/tmp/example.opt.ir");
        assert_eq!(
            super::top_fn_name_from_filename(path).expect("derive top fn"),
            "example"
        );
    }

    #[test]
    fn sample_id_is_relpath_stable_and_content_independent() {
        let lhs = super::sample_id_for_relpath("dir/sample.opt.ir");
        let rhs = super::sample_id_for_relpath("dir/sample.opt.ir");
        let other = super::sample_id_for_relpath("other-dir/sample.opt.ir");

        assert_eq!(lhs, rhs);
        assert_ne!(lhs, other);
        assert!(lhs.starts_with("sample-"));
    }

    #[test]
    fn ensure_imported_ir_action_is_idempotent() {
        let root = make_temp_dir("import-idempotent");
        let store = ArtifactStore::new_with_sled(root.join("store"), root.join("artifacts.sled"));
        store.ensure_layout().expect("ensure store layout");
        let source_path = root.join("sample.ir");
        fs::write(
            &source_path,
            "package p\n\ntop fn foo(x: bits[1]) -> bits[1] {\n  ret x: bits[1] = identity(x)\n}\n",
        )
        .expect("write source ir");
        let sample = CorpusSampleSpec {
            sample_id: "sample-1".to_string(),
            logical_name: "sample.ir".to_string(),
            source_path: source_path.clone(),
            source_relpath: "sample.ir".to_string(),
            source_sha256: sha256_file(&source_path).expect("sha256"),
            top_fn_name: "foo".to_string(),
        };
        let action = ActionSpec::ImportIrPackageFile {
            source_sha256: sample.source_sha256.clone(),
            top_fn_name: Some(sample.top_fn_name.clone()),
        };

        ensure_imported_ir_action(&store, &sample, &action).expect("first import");
        ensure_imported_ir_action(&store, &sample, &action).expect("second import");

        let action_id = compute_action_id(&action).expect("compute import action id");
        let provenance = store
            .load_provenance(&action_id)
            .expect("load imported provenance");
        assert_eq!(provenance.output_artifact.relpath, IMPORTED_IR_RELPATH);
        assert_eq!(
            provenance
                .details
                .get("source_sha256")
                .and_then(|v| v.as_str()),
            Some(sample.source_sha256.as_str())
        );
        assert!(
            store
                .resolve_artifact_ref_path(&provenance.output_artifact)
                .exists()
        );

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn export_leaf_artifacts_prunes_stale_sample_dirs() {
        let root = make_temp_dir("export-prune");
        let store = ArtifactStore::new_with_sled(root.join("store"), root.join("artifacts.sled"));
        store.ensure_layout().expect("ensure store layout");
        let export_root = root.join(IR_DIR_CORPUS_EXPORTED_ARTIFACTS_DIR);
        let stale_removed = export_root.join("removed-sample");
        let stale_pending = export_root.join("pending-sample");
        fs::create_dir_all(&stale_removed).expect("create removed sample dir");
        fs::create_dir_all(&stale_pending).expect("create pending sample dir");
        fs::write(stale_removed.join("stale.txt"), "stale")
            .expect("write removed sample stale file");
        fs::write(stale_pending.join("stale.txt"), "stale")
            .expect("write pending sample stale file");

        let pending_sample = IrDirCorpusSampleRecord {
            sample_id: "pending-sample".to_string(),
            logical_name: "pending.ir".to_string(),
            source_relpath: "pending.ir".to_string(),
            source_sha256: "sha".to_string(),
            top_fn_policy: "explicit".to_string(),
            top_fn_name: "foo".to_string(),
            fraig: false,
            dso_version: "v0.39.0".to_string(),
            driver_crate_version: "0.34.0".to_string(),
            stats_driver_crate_version: "0.39.0".to_string(),
            yosys_script: "flows/yosys_to_aig.ys".to_string(),
            yosys_script_sha256: "scriptsha".to_string(),
            preset: recipe_preset_label(CorpusRecipePreset::G8rVsYabcAigDiff).to_string(),
            import_ir_action_id: "import".to_string(),
            import_ir_status: "done".to_string(),
            g8r_aig_action_id: "g8r".to_string(),
            g8r_aig_status: "pending".to_string(),
            g8r_stats_action_id: "g8rstats".to_string(),
            g8r_stats_status: "pending".to_string(),
            combo_verilog_action_id: "combo".to_string(),
            combo_verilog_status: "pending".to_string(),
            yosys_abc_aig_action_id: "yosysaig".to_string(),
            yosys_abc_aig_status: "pending".to_string(),
            yosys_abc_stats_action_id: "yosysstats".to_string(),
            yosys_abc_stats_status: "pending".to_string(),
            aig_stat_diff_action_id: "diff".to_string(),
            aig_stat_diff_status: "pending".to_string(),
            status: "pending".to_string(),
            error: None,
        };

        export_leaf_artifacts(&store, &root, &[pending_sample]).expect("export leaf artifacts");

        assert!(!stale_removed.exists());
        assert!(!stale_pending.exists());

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn show_ir_dir_corpus_progress_reports_live_counts_eta_and_failures() {
        let fixture = make_status_fixture();
        let done_at = Utc::now() - ChronoDuration::minutes(5);
        seed_done_sample(
            &fixture.store,
            &fixture.samples[0],
            &fixture.plans[0],
            done_at,
        );
        stage_provenance_record(
            &fixture.store,
            fixture.plans[1].import_action.clone(),
            ArtifactRef {
                action_id: fixture.plans[1].import_ir_action_id.clone(),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: IMPORTED_IR_RELPATH.to_string(),
            },
            done_at,
            json!({
                "source_sha256": fixture.samples[1].source_sha256,
                "ir_top": fixture.samples[1].top_fn_name,
            }),
            vec![(
                IMPORTED_IR_RELPATH.to_string(),
                b"package pending\n".to_vec(),
            )],
        );
        stage_provenance_record(
            &fixture.store,
            fixture.plans[2].import_action.clone(),
            ArtifactRef {
                action_id: fixture.plans[2].import_ir_action_id.clone(),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: IMPORTED_IR_RELPATH.to_string(),
            },
            done_at,
            json!({
                "source_sha256": fixture.samples[2].source_sha256,
                "ir_top": fixture.samples[2].top_fn_name,
            }),
            vec![(
                IMPORTED_IR_RELPATH.to_string(),
                b"package failed\n".to_vec(),
            )],
        );
        enqueue_action_with_priority(&fixture.store, fixture.plans[1].g8r_aig_action.clone(), 0)
            .expect("enqueue pending sample action");
        fixture
            .store
            .write_failed_action_record(&crate::model::QueueFailed {
                schema_version: crate::ACTION_SCHEMA_VERSION,
                action_id: fixture.plans[2].g8r_aig_action_id.clone(),
                enqueued_utc: done_at,
                failed_utc: done_at,
                failed_by: "test".to_string(),
                action: fixture.plans[2].g8r_aig_action.clone(),
                error: "synthetic failure".to_string(),
            })
            .expect("write failed action record");
        let output_dir = fixture.output_dir.clone();
        let root = fixture.root.clone();
        let failed_sample_id = fixture.samples[2].sample_id.clone();
        drop(fixture.store);

        let report = show_ir_dir_corpus_progress(&output_dir, 1800, 10).expect("show progress");
        assert_eq!(report.sample_counts.get("total"), Some(&3));
        assert_eq!(report.sample_counts.get("done"), Some(&1));
        assert_eq!(report.sample_counts.get("pending"), Some(&1));
        assert_eq!(report.sample_counts.get("failed"), Some(&1));
        assert_eq!(report.action_counts.get("planned_unique"), Some(&21));
        assert_eq!(report.ready_output_counts.get("input_ir"), Some(&3));
        assert_eq!(report.ready_output_counts.get("g8r_aig"), Some(&1));
        assert_eq!(report.eta_seconds, Some(1800));
        assert_eq!(report.failed_sample_examples.len(), 1);
        assert_eq!(report.failed_sample_examples[0].sample_id, failed_sample_id);
        assert_eq!(
            report.public_outputs_present.get("manifest_json"),
            Some(&true)
        );
        assert_eq!(
            report.public_outputs_present.get("summary_json"),
            Some(&false)
        );

        fs::remove_dir_all(root).expect("cleanup fixture");
    }

    #[test]
    fn show_ir_dir_corpus_progress_does_not_materialize_ready_outputs() {
        let fixture = make_status_fixture();
        let done_at = Utc::now() - ChronoDuration::minutes(5);
        seed_done_sample(
            &fixture.store,
            &fixture.samples[0],
            &fixture.plans[0],
            done_at,
        );
        let output_dir = fixture.output_dir.clone();
        let root = fixture.root.clone();
        let materialized_actions_root = fixture.store.artifacts_dir();
        let materialized_before = WalkDir::new(&materialized_actions_root)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.depth() > 0)
            .count();
        assert_eq!(materialized_before, 0);
        drop(fixture.store);

        let report = show_ir_dir_corpus_progress(&output_dir, 1800, 10).expect("show progress");
        assert_eq!(report.ready_output_counts.get("input_ir"), Some(&1));
        assert_eq!(report.ready_output_counts.get("g8r_aig"), Some(&1));
        assert_eq!(report.ready_output_counts.get("aig_stat_diff"), Some(&1));

        let materialized_after = WalkDir::new(&materialized_actions_root)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.depth() > 0)
            .count();
        assert_eq!(materialized_after, 0);

        fs::remove_dir_all(root).expect("cleanup fixture");
    }

    #[test]
    fn show_ir_dir_corpus_progress_succeeds_while_sled_db_is_locked() {
        let fixture = make_status_fixture();
        let done_at = Utc::now() - ChronoDuration::minutes(5);
        stage_provenance_record(
            &fixture.store,
            fixture.plans[1].import_action.clone(),
            ArtifactRef {
                action_id: fixture.plans[1].import_ir_action_id.clone(),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: IMPORTED_IR_RELPATH.to_string(),
            },
            done_at,
            json!({
                "source_sha256": fixture.samples[1].source_sha256,
                "ir_top": fixture.samples[1].top_fn_name,
            }),
            vec![(
                IMPORTED_IR_RELPATH.to_string(),
                b"package pending\n".to_vec(),
            )],
        );
        enqueue_action_with_priority(&fixture.store, fixture.plans[1].g8r_aig_action.clone(), 0)
            .expect("enqueue pending sample action");
        fixture
            .store
            .write_failed_action_record(&crate::model::QueueFailed {
                schema_version: crate::ACTION_SCHEMA_VERSION,
                action_id: fixture.plans[2].g8r_aig_action_id.clone(),
                enqueued_utc: done_at,
                failed_utc: done_at,
                failed_by: "test".to_string(),
                action: fixture.plans[2].g8r_aig_action.clone(),
                error: "synthetic failure while locked".to_string(),
            })
            .expect("write failed action record");
        let output_dir = fixture.output_dir.clone();
        let root = fixture.root.clone();
        let failed_sample_id = fixture.samples[2].sample_id.clone();
        let lock_err = sled::Config::new()
            .path(
                output_dir
                    .join(IR_DIR_CORPUS_INTERNAL_DIR)
                    .join(IR_DIR_CORPUS_INTERNAL_SLED_FILENAME),
            )
            .open()
            .expect_err("second sled open should hit the exclusive lock");
        assert!(lock_err.to_string().contains("could not acquire lock"));

        let report =
            show_ir_dir_corpus_progress(&output_dir, 1800, 10).expect("show progress while locked");
        assert_eq!(report.sample_counts.get("pending"), Some(&2));
        assert_eq!(report.sample_counts.get("failed"), Some(&1));
        assert_eq!(report.sample_counts.get("missing"), Some(&0));
        assert_eq!(report.failed_sample_examples.len(), 1);
        assert_eq!(report.failed_sample_examples[0].sample_id, failed_sample_id);
        assert_eq!(
            report.failed_sample_examples[0].error,
            "synthetic failure while locked"
        );

        drop(fixture.store);
        fs::remove_dir_all(root).expect("cleanup fixture");
    }
    fn show_ir_dir_corpus_progress_uses_output_dir_workspace_paths() {
        let fixture = make_status_fixture();
        let output_dir = fixture.output_dir.clone();
        let root = fixture.root.clone();
        let workspace_dir = output_dir.join(IR_DIR_CORPUS_INTERNAL_DIR);
        let workspace_store_dir = workspace_dir.join(IR_DIR_CORPUS_INTERNAL_STORE_DIR);
        let workspace_artifacts_via_sled = workspace_dir.join(IR_DIR_CORPUS_INTERNAL_SLED_FILENAME);
        let mut manifest = read_status_manifest(&output_dir);
        manifest.workspace_dir = Path::new("out")
            .join(IR_DIR_CORPUS_INTERNAL_DIR)
            .display()
            .to_string();
        manifest.workspace_store_dir = Path::new("out")
            .join(IR_DIR_CORPUS_INTERNAL_DIR)
            .join(IR_DIR_CORPUS_INTERNAL_STORE_DIR)
            .display()
            .to_string();
        manifest.workspace_artifacts_via_sled = Path::new("out")
            .join(IR_DIR_CORPUS_INTERNAL_DIR)
            .join(IR_DIR_CORPUS_INTERNAL_SLED_FILENAME)
            .display()
            .to_string();
        write_status_manifest(&output_dir, &manifest);
        drop(fixture.store);

        let report = show_ir_dir_corpus_progress(&output_dir, 1800, 10).expect("show progress");
        assert_eq!(report.sample_counts.get("total"), Some(&3));
        assert_eq!(
            report.workspace_store_dir,
            workspace_store_dir.display().to_string()
        );
        assert_eq!(
            report.workspace_artifacts_via_sled,
            workspace_artifacts_via_sled.display().to_string()
        );

        fs::remove_dir_all(root).expect("cleanup fixture");
    }

    #[test]
    fn refresh_ir_dir_corpus_status_preserves_run_mode_failures_from_manifest() {
        let fixture = make_status_fixture();
        let failed_at = Utc::now() - ChronoDuration::minutes(2);
        stage_provenance_record(
            &fixture.store,
            fixture.plans[2].import_action.clone(),
            ArtifactRef {
                action_id: fixture.plans[2].import_ir_action_id.clone(),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: IMPORTED_IR_RELPATH.to_string(),
            },
            failed_at,
            json!({
                "source_sha256": fixture.samples[2].source_sha256,
                "ir_top": fixture.samples[2].top_fn_name,
            }),
            vec![(
                IMPORTED_IR_RELPATH.to_string(),
                b"package failed\n".to_vec(),
            )],
        );
        let output_dir = fixture.output_dir.clone();
        let root = fixture.root.clone();
        let failed_sample_id = fixture.samples[2].sample_id.clone();
        let failed_error = "synthetic run failure".to_string();
        let mut manifest = read_status_manifest(&output_dir);
        manifest.execution_mode = execution_mode_label(CorpusExecutionMode::Run).to_string();
        let failed_sample = manifest
            .samples
            .iter_mut()
            .find(|sample| sample.sample_id == failed_sample_id)
            .expect("find failed sample in manifest");
        failed_sample.import_ir_status = "done".to_string();
        failed_sample.g8r_aig_status = "missing".to_string();
        failed_sample.g8r_stats_status = "missing".to_string();
        failed_sample.combo_verilog_status = "missing".to_string();
        failed_sample.yosys_abc_aig_status = "missing".to_string();
        failed_sample.yosys_abc_stats_status = "missing".to_string();
        failed_sample.aig_stat_diff_status = "missing".to_string();
        failed_sample.status = "failed".to_string();
        failed_sample.error = Some(failed_error.clone());
        write_status_manifest(&output_dir, &manifest);
        drop(fixture.store);

        let report = refresh_ir_dir_corpus_status(&output_dir, 1800, 10).expect("refresh status");
        assert_eq!(report.sample_counts.get("failed"), Some(&1));
        assert_eq!(report.failed_sample_examples.len(), 1);
        assert_eq!(report.failed_sample_examples[0].sample_id, failed_sample_id);
        assert_eq!(
            report.failed_sample_examples[0].error,
            failed_error.as_str()
        );

        let refreshed_manifest = read_status_manifest(&output_dir);
        let refreshed_failed_sample = refreshed_manifest
            .samples
            .iter()
            .find(|sample| sample.sample_id == failed_sample_id)
            .expect("find failed sample in refreshed manifest");
        assert_eq!(refreshed_failed_sample.status, "failed");
        assert_eq!(
            refreshed_failed_sample.error.as_deref(),
            Some("synthetic run failure")
        );

        let refreshed_samples = read_samples_jsonl(&output_dir);
        let refreshed_failed_sample = refreshed_samples
            .iter()
            .find(|sample| sample.sample_id == failed_sample_id)
            .expect("find failed sample in refreshed samples jsonl");
        assert_eq!(refreshed_failed_sample.status, "failed");
        assert_eq!(
            refreshed_failed_sample.error.as_deref(),
            Some("synthetic run failure")
        );

        fs::remove_dir_all(root).expect("cleanup fixture");
    }

    #[test]
    fn refresh_ir_dir_corpus_status_prefers_live_state_over_stale_run_failure_manifest() {
        let fixture = make_status_fixture();
        let done_at = Utc::now() - ChronoDuration::minutes(4);
        seed_done_sample(
            &fixture.store,
            &fixture.samples[0],
            &fixture.plans[0],
            done_at,
        );
        stage_provenance_record(
            &fixture.store,
            fixture.plans[1].import_action.clone(),
            ArtifactRef {
                action_id: fixture.plans[1].import_ir_action_id.clone(),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: IMPORTED_IR_RELPATH.to_string(),
            },
            done_at,
            json!({
                "source_sha256": fixture.samples[1].source_sha256,
                "ir_top": fixture.samples[1].top_fn_name,
            }),
            vec![(
                IMPORTED_IR_RELPATH.to_string(),
                b"package pending\n".to_vec(),
            )],
        );
        enqueue_action_with_priority(&fixture.store, fixture.plans[1].g8r_aig_action.clone(), 0)
            .expect("enqueue pending sample action");
        let output_dir = fixture.output_dir.clone();
        let root = fixture.root.clone();
        let done_sample_id = fixture.samples[0].sample_id.clone();
        let pending_sample_id = fixture.samples[1].sample_id.clone();
        let mut manifest = read_status_manifest(&output_dir);
        manifest.execution_mode = execution_mode_label(CorpusExecutionMode::Run).to_string();
        for sample_id in [&done_sample_id, &pending_sample_id] {
            let sample = manifest
                .samples
                .iter_mut()
                .find(|sample| sample.sample_id == *sample_id)
                .expect("find stale failed sample in manifest");
            sample.status = "failed".to_string();
            sample.error = Some("stale run failure".to_string());
            sample.g8r_aig_status = "missing".to_string();
            sample.g8r_stats_status = "missing".to_string();
            sample.combo_verilog_status = "missing".to_string();
            sample.yosys_abc_aig_status = "missing".to_string();
            sample.yosys_abc_stats_status = "missing".to_string();
            sample.aig_stat_diff_status = "missing".to_string();
        }
        write_status_manifest(&output_dir, &manifest);
        drop(fixture.store);

        let report = refresh_ir_dir_corpus_status(&output_dir, 1800, 10).expect("refresh status");
        assert_eq!(report.sample_counts.get("done"), Some(&1));
        assert_eq!(report.sample_counts.get("pending"), Some(&1));
        assert_eq!(report.sample_counts.get("failed"), Some(&0));
        assert!(report.failed_sample_examples.is_empty());

        let refreshed_manifest = read_status_manifest(&output_dir);
        let refreshed_done_sample = refreshed_manifest
            .samples
            .iter()
            .find(|sample| sample.sample_id == done_sample_id)
            .expect("find done sample in refreshed manifest");
        assert_eq!(refreshed_done_sample.status, "done");
        assert_eq!(refreshed_done_sample.error, None);
        let refreshed_pending_sample = refreshed_manifest
            .samples
            .iter()
            .find(|sample| sample.sample_id == pending_sample_id)
            .expect("find pending sample in refreshed manifest");
        assert_eq!(refreshed_pending_sample.status, "pending");
        assert_eq!(refreshed_pending_sample.error, None);

        let refreshed_samples = read_samples_jsonl(&output_dir);
        let refreshed_done_sample = refreshed_samples
            .iter()
            .find(|sample| sample.sample_id == done_sample_id)
            .expect("find done sample in refreshed samples jsonl");
        assert_eq!(refreshed_done_sample.status, "done");
        assert_eq!(refreshed_done_sample.error, None);
        let refreshed_pending_sample = refreshed_samples
            .iter()
            .find(|sample| sample.sample_id == pending_sample_id)
            .expect("find pending sample in refreshed samples jsonl");
        assert_eq!(refreshed_pending_sample.status, "pending");
        assert_eq!(refreshed_pending_sample.error, None);

        fs::remove_dir_all(root).expect("cleanup fixture");
    }

    #[test]
    fn refresh_ir_dir_corpus_status_rewrites_workspace_paths_under_output_dir() {
        let fixture = make_status_fixture();
        let output_dir = fixture.output_dir.clone();
        let root = fixture.root.clone();
        let workspace_dir = output_dir.join(IR_DIR_CORPUS_INTERNAL_DIR);
        let workspace_store_dir = workspace_dir.join(IR_DIR_CORPUS_INTERNAL_STORE_DIR);
        let workspace_artifacts_via_sled = workspace_dir.join(IR_DIR_CORPUS_INTERNAL_SLED_FILENAME);
        let mut manifest = read_status_manifest(&output_dir);
        manifest.workspace_dir = Path::new("out")
            .join(IR_DIR_CORPUS_INTERNAL_DIR)
            .display()
            .to_string();
        manifest.workspace_store_dir = Path::new("out")
            .join(IR_DIR_CORPUS_INTERNAL_DIR)
            .join(IR_DIR_CORPUS_INTERNAL_STORE_DIR)
            .display()
            .to_string();
        manifest.workspace_artifacts_via_sled = Path::new("out")
            .join(IR_DIR_CORPUS_INTERNAL_DIR)
            .join(IR_DIR_CORPUS_INTERNAL_SLED_FILENAME)
            .display()
            .to_string();
        write_status_manifest(&output_dir, &manifest);
        drop(fixture.store);

        let report = refresh_ir_dir_corpus_status(&output_dir, 1800, 10).expect("refresh status");
        assert_eq!(
            report.workspace_store_dir,
            workspace_store_dir.display().to_string()
        );
        assert_eq!(
            report.workspace_artifacts_via_sled,
            workspace_artifacts_via_sled.display().to_string()
        );

        let refreshed_manifest = read_status_manifest(&output_dir);
        assert_eq!(
            refreshed_manifest.workspace_dir,
            workspace_dir.display().to_string()
        );
        assert_eq!(
            refreshed_manifest.workspace_store_dir,
            workspace_store_dir.display().to_string()
        );
        assert_eq!(
            refreshed_manifest.workspace_artifacts_via_sled,
            workspace_artifacts_via_sled.display().to_string()
        );

        let refreshed_summary = read_summary_json(&output_dir);
        assert_eq!(
            refreshed_summary.get("output_dir").and_then(|v| v.as_str()),
            Some(output_dir.display().to_string().as_str())
        );
        assert_eq!(
            refreshed_summary
                .get("workspace_store_dir")
                .and_then(|v| v.as_str()),
            Some(workspace_store_dir.display().to_string().as_str())
        );
        assert_eq!(
            refreshed_summary
                .get("workspace_artifacts_via_sled")
                .and_then(|v| v.as_str()),
            Some(workspace_artifacts_via_sled.display().to_string().as_str())
        );

        fs::remove_dir_all(root).expect("cleanup fixture");
    }

    #[test]
    fn refresh_ir_dir_corpus_status_rewrites_public_outputs_from_live_state() {
        let fixture = make_status_fixture();
        let done_at = Utc::now() - ChronoDuration::minutes(3);
        seed_done_sample(
            &fixture.store,
            &fixture.samples[0],
            &fixture.plans[0],
            done_at,
        );
        stage_provenance_record(
            &fixture.store,
            fixture.plans[1].import_action.clone(),
            ArtifactRef {
                action_id: fixture.plans[1].import_ir_action_id.clone(),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: IMPORTED_IR_RELPATH.to_string(),
            },
            done_at,
            json!({
                "source_sha256": fixture.samples[1].source_sha256,
                "ir_top": fixture.samples[1].top_fn_name,
            }),
            vec![(
                IMPORTED_IR_RELPATH.to_string(),
                b"package pending\n".to_vec(),
            )],
        );
        fixture
            .store
            .write_failed_action_record(&crate::model::QueueFailed {
                schema_version: crate::ACTION_SCHEMA_VERSION,
                action_id: fixture.plans[2].g8r_aig_action_id.clone(),
                enqueued_utc: done_at,
                failed_utc: done_at,
                failed_by: "test".to_string(),
                action: fixture.plans[2].g8r_aig_action.clone(),
                error: "synthetic failure".to_string(),
            })
            .expect("write failed action record");
        let output_dir = fixture.output_dir.clone();
        let root = fixture.root.clone();
        let done_sample_id = fixture.samples[0].sample_id.clone();
        let pending_sample_id = fixture.samples[1].sample_id.clone();
        drop(fixture.store);

        let report = refresh_ir_dir_corpus_status(&output_dir, 1800, 10).expect("refresh status");
        assert!(report.refreshed_public_outputs);
        assert_eq!(report.sample_counts.get("done"), Some(&1));
        assert_eq!(report.sample_counts.get("failed"), Some(&1));
        assert_eq!(report.exported_sample_dirs_on_disk, 1);
        assert_eq!(
            report.public_outputs_present.get("summary_json"),
            Some(&true)
        );
        assert!(Path::new(&report.joined_jsonl_path).exists());
        assert!(Path::new(&report.joined_csv_path).exists());
        assert!(
            output_dir
                .join(IR_DIR_CORPUS_EXPORTED_ARTIFACTS_DIR)
                .join(&done_sample_id)
                .join("aig_stat_diff.json")
                .exists()
        );
        assert!(
            !output_dir
                .join(IR_DIR_CORPUS_EXPORTED_ARTIFACTS_DIR)
                .join(&pending_sample_id)
                .exists()
        );

        fs::remove_dir_all(root).expect("cleanup fixture");
    }
}
