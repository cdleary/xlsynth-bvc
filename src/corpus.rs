use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Digest;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

use crate::cli::{CorpusExecutionMode, CorpusRecipePreset, CorpusTopFnPolicy, DriverCli, YosysCli};
use crate::executor::{compute_action_id, execute_action};
use crate::model::{
    ActionSpec, ArtifactRef, ArtifactType, DriverRuntimeSpec, Provenance, YosysRuntimeSpec,
};
use crate::queue::{
    enqueue_action_with_priority, load_queue_canceled_record, load_queue_failed_record,
    queue_state_for_action, suggested_action_queue_priority,
};
use crate::runtime::resolve_driver_runtime_for_aig_stats;
use crate::service::{
    collect_output_files, infer_ir_top_function, make_script_ref, sha256_file, summarize_error,
};
use crate::store::ArtifactStore;

const IR_DIR_CORPUS_MANIFEST_SCHEMA_VERSION: u32 = 1;
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
    top_fn_policy: String,
    top_fn_name: Option<String>,
    fraig: bool,
    dso_version: String,
    driver_runtime: DriverRuntimeSpec,
    stats_runtime: DriverRuntimeSpec,
    yosys_runtime: YosysRuntimeSpec,
    yosys_script: String,
    samples: Vec<IrDirCorpusSampleRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IrDirCorpusSampleRecord {
    sample_id: String,
    logical_name: String,
    source_relpath: String,
    source_sha256: String,
    top_fn_name: String,
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
    total_samples: usize,
    completed_samples: usize,
    status_counts: BTreeMap<String, usize>,
    enqueued_actions: usize,
    executed_actions: usize,
}

#[derive(Debug, Clone, Serialize)]
struct IrDirCorpusJoinedRow {
    sample_id: String,
    logical_name: String,
    source_relpath: String,
    top_fn_name: String,
    import_ir_action_id: String,
    g8r_aig_action_id: String,
    g8r_stats_action_id: String,
    combo_verilog_action_id: String,
    yosys_abc_aig_action_id: String,
    yosys_abc_stats_action_id: String,
    aig_stat_diff_action_id: String,
    g8r_and_nodes: Option<f64>,
    g8r_depth: Option<f64>,
    yosys_abc_and_nodes: Option<f64>,
    yosys_abc_depth: Option<f64>,
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
    let workspace_dir = output_dir.join(IR_DIR_CORPUS_INTERNAL_DIR);
    let workspace_store_dir = workspace_dir.join(IR_DIR_CORPUS_INTERNAL_STORE_DIR);
    let workspace_artifacts_via_sled = workspace_dir.join(IR_DIR_CORPUS_INTERNAL_SLED_FILENAME);
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
        sample_records.push(build_sample_record(&store, sample, &plan, &run_errors));
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
        top_fn_policy: top_fn_policy_label(top_fn_policy).to_string(),
        top_fn_name: top_fn_name.map(ToOwned::to_owned),
        fraig,
        dso_version: version.to_string(),
        driver_runtime: driver_runtime.clone(),
        stats_runtime: stats_runtime.clone(),
        yosys_runtime: yosys_runtime.clone(),
        yosys_script: yosys_script_ref.path.clone(),
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
        sample_count: sample_records.len(),
        completed_samples: summary_file.completed_samples,
        enqueued_actions: counters.enqueued_actions,
        executed_actions: counters.executed_actions,
        status_counts,
    })
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
        CorpusTopFnPolicy::InferSinglePackage => infer_ir_top_function(source_path),
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
        top_fn_name: sample.top_fn_name.clone(),
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
        rows.push(IrDirCorpusJoinedRow {
            sample_id: sample.sample_id.clone(),
            logical_name: sample.logical_name.clone(),
            source_relpath: sample.source_relpath.clone(),
            top_fn_name: sample.top_fn_name.clone(),
            import_ir_action_id: sample.import_ir_action_id.clone(),
            g8r_aig_action_id: sample.g8r_aig_action_id.clone(),
            g8r_stats_action_id: sample.g8r_stats_action_id.clone(),
            combo_verilog_action_id: sample.combo_verilog_action_id.clone(),
            yosys_abc_aig_action_id: sample.yosys_abc_aig_action_id.clone(),
            yosys_abc_stats_action_id: sample.yosys_abc_stats_action_id.clone(),
            aig_stat_diff_action_id: sample.aig_stat_diff_action_id.clone(),
            g8r_and_nodes: stats_metric(&g8r_stats, "and_nodes")
                .or_else(|| stats_metric(&g8r_stats, "live_nodes")),
            g8r_depth: stats_metric(&g8r_stats, "depth"),
            yosys_abc_and_nodes: stats_metric(&yosys_stats, "and_nodes")
                .or_else(|| stats_metric(&yosys_stats, "live_nodes")),
            yosys_abc_depth: stats_metric(&yosys_stats, "depth"),
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

fn export_leaf_artifacts(
    store: &ArtifactStore,
    output_dir: &Path,
    sample_records: &[IrDirCorpusSampleRecord],
) -> Result<()> {
    let export_root = output_dir.join(IR_DIR_CORPUS_EXPORTED_ARTIFACTS_DIR);
    fs::create_dir_all(&export_root)
        .with_context(|| format!("creating export root: {}", export_root.display()))?;
    for sample in sample_records {
        if sample.status != "done" {
            continue;
        }
        let sample_dir = export_root.join(&sample.sample_id);
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
        "sample_id,logical_name,source_relpath,top_fn_name,import_ir_action_id,g8r_aig_action_id,g8r_stats_action_id,combo_verilog_action_id,yosys_abc_aig_action_id,yosys_abc_stats_action_id,aig_stat_diff_action_id,g8r_and_nodes,g8r_depth,yosys_abc_and_nodes,yosys_abc_depth,delta_and_nodes_yosys_minus_g8r,delta_depth_yosys_minus_g8r\n",
    );
    for row in rows {
        let fields = [
            csv_escape(&row.sample_id),
            csv_escape(&row.logical_name),
            csv_escape(&row.source_relpath),
            csv_escape(&row.top_fn_name),
            csv_escape(&row.import_ir_action_id),
            csv_escape(&row.g8r_aig_action_id),
            csv_escape(&row.g8r_stats_action_id),
            csv_escape(&row.combo_verilog_action_id),
            csv_escape(&row.yosys_abc_aig_action_id),
            csv_escape(&row.yosys_abc_stats_action_id),
            csv_escape(&row.aig_stat_diff_action_id),
            csv_escape(&optional_f64(row.g8r_and_nodes)),
            csv_escape(&optional_f64(row.g8r_depth)),
            csv_escape(&optional_f64(row.yosys_abc_and_nodes)),
            csv_escape(&optional_f64(row.yosys_abc_depth)),
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

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn top_fn_name_from_filename_strips_opt_ir_suffix() {
        let path = Path::new("/tmp/example.opt.ir");
        assert_eq!(
            super::top_fn_name_from_filename(path).expect("derive top fn"),
            "example"
        );
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
}
