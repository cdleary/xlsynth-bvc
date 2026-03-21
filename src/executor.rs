use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use reqwest::blocking::Client;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use walkdir::WalkDir;

use crate::model::*;
use crate::query::{
    ensure_aiger_input_header,
    maybe_upsert_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_index_for_completed_action,
    maybe_upsert_ir_fn_corpus_g8r_vs_yosys_index_for_completed_action,
    normalize_legacy_g8r_stats_payload,
};
use crate::runtime::*;
use crate::service::*;
use crate::store::ArtifactStore;
use crate::versioning::*;
use crate::{
    ACTION_SCHEMA_VERSION, DEFAULT_YOSYS_FLOW_SCRIPT, DELAY_INFO_OUTPUT_FORMAT_TEXTPROTO_V1,
    DSLX_PATH_LIST_SEPARATOR, INPUT_IR_FN_STRUCTURAL_HASH_DETAILS_KEY, LEGACY_G8R_STATS_RELPATH,
    OUTPUT_IR_OP_COUNT_DETAILS_KEY, XLSYNTH_SOURCE_ARCHIVE_URL_PREFIX,
    default_k_bool_cone_max_ir_ops_for_k, driver_ir_aig_equiv_enabled,
    incremental_ir_corpus_upsert_enabled,
};

type DriverSubcommandSupportCacheKey = (String, String, String, String);
static DRIVER_SUBCOMMAND_SUPPORT_CACHE: OnceLock<
    Mutex<BTreeMap<DriverSubcommandSupportCacheKey, bool>>,
> = OnceLock::new();
type DriverSubcommandHelpTokenCacheKey = (String, String, String, String, String);
static DRIVER_SUBCOMMAND_HELP_TOKEN_CACHE: OnceLock<
    Mutex<BTreeMap<DriverSubcommandHelpTokenCacheKey, bool>>,
> = OnceLock::new();

pub(crate) fn execute_action(
    store: &ArtifactStore,
    action: ActionSpec,
) -> Result<(String, ArtifactRef)> {
    let action_id = compute_action_id(&action)?;

    if store.action_exists(&action_id) {
        let provenance = store.load_provenance(&action_id)?;
        return Ok((action_id, provenance.output_artifact));
    }

    let staging_dir = make_staging_dir(store, &action_id)?;
    let payload_dir = staging_dir.join("payload");
    fs::create_dir_all(&payload_dir)
        .with_context(|| format!("creating payload dir: {}", payload_dir.display()))?;

    let repo_root = std::env::current_dir().context("getting current directory")?;

    let outcome = match &action {
        ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
            version,
            discovery_runtime,
        } => run_download_stdlib_action(
            store,
            &repo_root,
            &action_id,
            version,
            discovery_runtime.as_ref(),
            &payload_dir,
        )?,
        ActionSpec::DownloadAndExtractXlsynthSourceSubtree {
            version,
            subtree,
            discovery_runtime,
        } => run_download_source_subtree_action(
            store,
            &repo_root,
            &action_id,
            version,
            subtree,
            discovery_runtime.as_ref(),
            &payload_dir,
        )?,
        ActionSpec::DriverDslxFnToIr {
            dslx_subtree_action_id,
            dslx_file,
            dslx_fn_name,
            version,
            runtime,
        } => run_driver_dslx_fn_to_ir_action(
            store,
            &repo_root,
            &action_id,
            dslx_subtree_action_id,
            dslx_file,
            dslx_fn_name,
            version,
            runtime,
            &payload_dir,
        )?,
        ActionSpec::DriverIrToOpt {
            ir_action_id,
            top_fn_name,
            version,
            runtime,
        } => run_driver_ir_to_opt_action(
            store,
            &repo_root,
            &action_id,
            ir_action_id,
            top_fn_name.as_deref(),
            version,
            runtime,
            &payload_dir,
        )?,
        ActionSpec::DriverIrToDelayInfo {
            ir_action_id,
            top_fn_name,
            delay_model,
            output_format,
            version,
            runtime,
        } => run_driver_ir_to_delay_info_action(
            store,
            &repo_root,
            &action_id,
            ir_action_id,
            top_fn_name.as_deref(),
            delay_model,
            output_format,
            version,
            runtime,
            &payload_dir,
        )?,
        ActionSpec::DriverIrEquiv {
            lhs_ir_action_id,
            rhs_ir_action_id,
            top_fn_name,
            version,
            runtime,
        } => run_driver_ir_equiv_action(
            store,
            &repo_root,
            &action_id,
            lhs_ir_action_id,
            rhs_ir_action_id,
            top_fn_name.as_deref(),
            version,
            runtime,
            &payload_dir,
        )?,
        ActionSpec::DriverIrAigEquiv {
            ir_action_id,
            aig_action_id,
            top_fn_name,
            version,
            runtime,
        } => run_driver_ir_aig_equiv_action(
            store,
            &repo_root,
            &action_id,
            ir_action_id,
            aig_action_id,
            top_fn_name.as_deref(),
            version,
            runtime,
            &payload_dir,
        )?,
        ActionSpec::DriverIrToG8rAig {
            ir_action_id,
            top_fn_name,
            fraig,
            lowering_mode,
            version,
            runtime,
        } => run_driver_ir_to_g8r_aig_action(
            store,
            &repo_root,
            &action_id,
            ir_action_id,
            top_fn_name.as_deref(),
            *fraig,
            lowering_mode,
            version,
            runtime,
            &payload_dir,
        )?,
        ActionSpec::IrFnToCombinationalVerilog {
            ir_action_id,
            top_fn_name,
            use_system_verilog,
            version,
            runtime,
        } => run_ir_fn_to_combinational_verilog_action(
            store,
            &repo_root,
            &action_id,
            ir_action_id,
            top_fn_name.as_deref(),
            *use_system_verilog,
            version,
            runtime,
            &payload_dir,
        )?,
        ActionSpec::IrFnToKBoolConeCorpus {
            ir_action_id,
            top_fn_name,
            k,
            max_ir_ops,
            version,
            runtime,
        } => run_ir_fn_to_k_bool_cone_corpus_action(
            store,
            &repo_root,
            &action_id,
            ir_action_id,
            top_fn_name.as_deref(),
            *k,
            *max_ir_ops,
            version,
            runtime,
            &payload_dir,
        )?,
        ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id,
            verilog_top_module_name,
            yosys_script_ref,
            runtime,
        } => run_combo_verilog_to_yosys_abc_aig_action(
            store,
            &repo_root,
            &action_id,
            verilog_action_id,
            verilog_top_module_name.as_deref(),
            yosys_script_ref,
            runtime,
            &payload_dir,
        )?,
        ActionSpec::AigToYosysAbcAig {
            aig_action_id,
            yosys_script_ref,
            runtime,
        } => run_aig_to_yosys_abc_aig_action(
            store,
            &repo_root,
            &action_id,
            aig_action_id,
            yosys_script_ref,
            runtime,
            &payload_dir,
        )?,
        ActionSpec::DriverAigToStats {
            aig_action_id,
            version,
            runtime,
        } => run_driver_aig_to_stats_action(
            store,
            &repo_root,
            &action_id,
            aig_action_id,
            version,
            runtime,
            &payload_dir,
        )?,
        ActionSpec::AigStatDiff {
            opt_ir_action_id,
            g8r_aig_stats_action_id,
            yosys_abc_aig_stats_action_id,
        } => run_aig_stat_diff_action(
            store,
            &action_id,
            opt_ir_action_id,
            g8r_aig_stats_action_id,
            yosys_abc_aig_stats_action_id,
            &payload_dir,
        )?,
    };

    finish_action_execution(
        store,
        &repo_root,
        action,
        action_id,
        staging_dir,
        payload_dir,
        outcome,
    )
}

fn finish_action_execution(
    store: &ArtifactStore,
    repo_root: &Path,
    action: ActionSpec,
    action_id: String,
    staging_dir: PathBuf,
    payload_dir: PathBuf,
    outcome: ActionOutcome,
) -> Result<(String, ArtifactRef)> {
    let output_files = collect_output_files(&payload_dir)?;
    let provenance = Provenance {
        schema_version: ACTION_SCHEMA_VERSION,
        action_id: action_id.clone(),
        created_utc: Utc::now(),
        action,
        dependencies: outcome.dependencies,
        output_artifact: outcome.output_artifact.clone(),
        output_files,
        commands: outcome.commands,
        details: outcome.details,
        suggested_next_actions: outcome.suggested_next_actions,
    };

    let provenance_path = staging_dir.join("provenance.json");
    let serialized = serde_json::to_string_pretty(&provenance).context("serializing provenance")?;
    fs::write(&provenance_path, serialized)
        .with_context(|| format!("writing provenance file: {}", provenance_path.display()))?;

    store.promote_staging_action_dir(&action_id, &staging_dir)?;

    if incremental_ir_corpus_upsert_enabled()
        && let Err(err) = maybe_upsert_ir_fn_corpus_g8r_vs_yosys_index_for_completed_action(
            store, repo_root, &action_id,
        )
    {
        eprintln!(
            "warning: unable to incrementally upsert ir-fn-corpus g8r-vs-yosys index for action {}: {:#}",
            action_id, err
        );
    }
    if incremental_ir_corpus_upsert_enabled()
        && let Err(err) =
            maybe_upsert_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_index_for_completed_action(
                store, repo_root, &action_id,
            )
    {
        eprintln!(
            "warning: unable to incrementally upsert ir-fn-corpus g8r-abc-vs-codegen-yosys-abc index for action {}: {:#}",
            action_id, err
        );
    }

    Ok((action_id, provenance.output_artifact))
}

#[derive(Debug)]
pub(crate) struct BatchActionExecutionResult {
    pub(crate) action_id: String,
    pub(crate) output_artifact: Option<ArtifactRef>,
    pub(crate) error: Option<String>,
}

impl BatchActionExecutionResult {
    fn success(action_id: String, output_artifact: ArtifactRef) -> Self {
        Self {
            action_id,
            output_artifact: Some(output_artifact),
            error: None,
        }
    }

    fn failure(action_id: String, error: impl Into<String>) -> Self {
        Self {
            action_id,
            output_artifact: None,
            error: Some(error.into()),
        }
    }
}

pub(crate) fn execute_action_batch(
    store: &ArtifactStore,
    actions: Vec<ActionSpec>,
) -> Result<Vec<BatchActionExecutionResult>> {
    if actions.len() <= 1 {
        return execute_actions_individually(store, actions);
    }
    let Some(batch_key) = action_batch_key(&actions[0]) else {
        return execute_actions_individually(store, actions);
    };
    if actions
        .iter()
        .any(|action| action_batch_key(action).as_ref() != Some(&batch_key))
    {
        return execute_actions_individually(store, actions);
    }

    match batch_key {
        ActionBatchKey::DriverIrToG8rAig {
            version,
            runtime,
            fraig,
            lowering_mode,
        } => execute_driver_ir_to_g8r_aig_batch(
            store,
            actions,
            &version,
            &runtime,
            fraig,
            &lowering_mode,
        ),
    }
}

fn execute_actions_individually(
    store: &ArtifactStore,
    actions: Vec<ActionSpec>,
) -> Result<Vec<BatchActionExecutionResult>> {
    let mut results = Vec::with_capacity(actions.len());
    for action in actions {
        let action_id = compute_action_id(&action)?;
        match execute_action(store, action) {
            Ok((_action_id, output_artifact)) => {
                results.push(BatchActionExecutionResult::success(
                    action_id,
                    output_artifact,
                ));
            }
            Err(err) => {
                results.push(BatchActionExecutionResult::failure(
                    action_id,
                    format!("{:#}", err),
                ));
            }
        }
    }
    Ok(results)
}

struct DriverIrToG8rBatchRunnable {
    action: ActionSpec,
    action_id: String,
    staging_dir: PathBuf,
    payload_dir: PathBuf,
    dependency: ArtifactRef,
    output_artifact: ArtifactRef,
    commands: Vec<CommandTrace>,
    details: serde_json::Map<String, serde_json::Value>,
    suggested_next_actions: Vec<SuggestedAction>,
    ir_input_path: PathBuf,
    explicit_ir_top: Option<String>,
    use_legacy_ir2g8r_cli: bool,
    capture_prepared_ir: bool,
}

enum PreparedDriverIrToG8rBatchAction {
    Immediate {
        action: ActionSpec,
        action_id: String,
        staging_dir: PathBuf,
        payload_dir: PathBuf,
        outcome: ActionOutcome,
    },
    Runnable(DriverIrToG8rBatchRunnable),
    Fallback(ActionSpec),
}

fn execute_driver_ir_to_g8r_aig_batch(
    store: &ArtifactStore,
    actions: Vec<ActionSpec>,
    version: &str,
    runtime: &DriverRuntimeSpec,
    fraig: bool,
    lowering_mode: &G8rLoweringMode,
) -> Result<Vec<BatchActionExecutionResult>> {
    let repo_root = std::env::current_dir().context("getting current directory")?;
    let mut prep_commands = Vec::new();
    ensure_driver_runtime_prepared(store, &repo_root, version, runtime, &mut prep_commands)?;

    let mut results = Vec::with_capacity(actions.len());
    let mut runnable = Vec::new();
    for action in actions {
        let action_id = compute_action_id(&action)?;
        if store.action_exists(&action_id) {
            let provenance = store.load_provenance(&action_id)?;
            results.push(BatchActionExecutionResult::success(
                action_id,
                provenance.output_artifact,
            ));
            continue;
        }
        match prepare_driver_ir_to_g8r_batch_action(
            store,
            &repo_root,
            action,
            version,
            runtime,
            fraig,
            lowering_mode,
            &prep_commands,
        )? {
            PreparedDriverIrToG8rBatchAction::Immediate {
                action,
                action_id,
                staging_dir,
                payload_dir,
                outcome,
            } => match finish_action_execution(
                store,
                &repo_root,
                action,
                action_id.clone(),
                staging_dir,
                payload_dir,
                outcome,
            ) {
                Ok((_done_id, output_artifact)) => {
                    results.push(BatchActionExecutionResult::success(
                        action_id,
                        output_artifact,
                    ));
                }
                Err(err) => {
                    results.push(BatchActionExecutionResult::failure(
                        action_id,
                        format!("{:#}", err),
                    ));
                }
            },
            PreparedDriverIrToG8rBatchAction::Runnable(member) => runnable.push(member),
            PreparedDriverIrToG8rBatchAction::Fallback(action) => {
                let action_id = compute_action_id(&action)?;
                match execute_action(store, action) {
                    Ok((_done_id, output_artifact)) => results.push(
                        BatchActionExecutionResult::success(action_id, output_artifact),
                    ),
                    Err(err) => results.push(BatchActionExecutionResult::failure(
                        action_id,
                        format!("{:#}", err),
                    )),
                }
            }
        }
    }

    if !runnable.is_empty() {
        results.extend(run_driver_ir_to_g8r_batch_runnable(
            store,
            &repo_root,
            version,
            runtime,
            fraig,
            lowering_mode,
            runnable,
        )?);
    }

    Ok(results)
}

#[cfg(test)]
pub(crate) fn promote_staging_action_dir(staging_dir: &Path, final_dir: &Path) -> Result<()> {
    let final_parent = final_dir
        .parent()
        .ok_or_else(|| anyhow!("final action dir had no parent"))?;
    fs::create_dir_all(final_parent)
        .with_context(|| format!("creating final shard path: {}", final_parent.display()))?;

    match fs::rename(staging_dir, final_dir) {
        Ok(()) => Ok(()),
        Err(first_err) => {
            if !final_dir.exists() {
                return Err(first_err).with_context(|| {
                    format!(
                        "moving staged artifact into place: {} -> {}",
                        staging_dir.display(),
                        final_dir.display()
                    )
                });
            }

            let final_provenance = final_dir.join("provenance.json");
            if final_provenance.exists() {
                fs::remove_dir_all(staging_dir).ok();
                return Ok(());
            }

            fs::remove_dir_all(final_dir).with_context(|| {
                format!(
                    "removing failed-only final action dir before promotion: {}",
                    final_dir.display()
                )
            })?;
            fs::rename(staging_dir, final_dir).with_context(|| {
                format!(
                    "moving staged artifact into place after removing failed-only dir: {} -> {}",
                    staging_dir.display(),
                    final_dir.display()
                )
            })?;
            Ok(())
        }
    }
}

pub(crate) fn compute_action_id(action: &ActionSpec) -> Result<String> {
    let payload = json!({
        "schema_version": ACTION_SCHEMA_VERSION,
        "action": action,
    });
    let bytes = serde_json::to_vec(&payload).context("serializing action fingerprint")?;
    let digest = Sha256::digest(bytes);
    Ok(hex::encode(digest))
}

pub(crate) fn make_staging_dir(store: &ArtifactStore, action_id: &str) -> Result<PathBuf> {
    for attempt in 0..1000_u32 {
        let pid = std::process::id();
        let ts = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let path =
            store
                .staging_dir()
                .join(format!("{}-{}-{}", action_id, pid, ts + i64::from(attempt)));
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("creating staging directory: {}", path.display()));
            }
        }
    }
    bail!("unable to create unique staging directory for action {action_id}")
}

pub(crate) fn run_download_stdlib_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action_id: &str,
    version: &str,
    discovery_runtime: Option<&DriverRuntimeSpec>,
    payload_dir: &Path,
) -> Result<ActionOutcome> {
    let client = Client::builder()
        .build()
        .context("creating reqwest client")?;
    let base_url = format!("https://github.com/xlsynth/xlsynth/releases/download/{version}");
    let tarball_url = format!("{base_url}/dslx_stdlib.tar.gz");
    let sha_url = format!("{tarball_url}.sha256");

    let tarball_path = payload_dir
        .parent()
        .ok_or_else(|| anyhow!("payload path missing parent"))?
        .join("dslx_stdlib.tar.gz");

    download_to_file(&client, &tarball_url, &tarball_path)?;
    let sha_text = client
        .get(&sha_url)
        .send()
        .with_context(|| format!("downloading sha256 file: {sha_url}"))?
        .error_for_status()
        .with_context(|| format!("status check for sha256 URL: {sha_url}"))?
        .text()
        .context("reading sha256 response body")?;

    let expected_sha = parse_sha256_text(&sha_text)?;
    let actual_sha = sha256_file(&tarball_path)?;

    if expected_sha != actual_sha {
        bail!(
            "downloaded tarball checksum mismatch; expected {} got {}",
            expected_sha,
            actual_sha
        );
    }

    extract_tar_gz_safely(&tarball_path, payload_dir)?;
    fs::remove_file(&tarball_path).ok();

    let output_artifact = ArtifactRef {
        action_id: action_id.to_string(),
        artifact_type: ArtifactType::DslxFileSubtree,
        relpath: "payload".to_string(),
    };

    let mut commands = Vec::new();
    let mut suggested_next_actions = Vec::new();
    let mut details = serde_json::Map::new();
    details.insert(
        "download".to_string(),
        json!({
            "tarball_url": tarball_url,
            "sha256_url": sha_url,
            "expected_sha256": expected_sha,
            "actual_sha256": actual_sha,
        }),
    );

    match discover_dslx_fn_to_ir_suggestions(
        store,
        repo_root,
        action_id,
        payload_dir,
        version,
        discovery_runtime,
    ) {
        Ok(discovery) => {
            commands.extend(discovery.commands);
            suggested_next_actions = discovery.suggested_next_actions;
            details.insert(
                "dslx_list_fns_discovery".to_string(),
                json!({
                    "driver_runtime": discovery.source_runtime,
                    "enumeration_runtime": discovery.discovery_runtime,
                    "enumeration_runtime_xlsynth_version": discovery.discovery_runtime_xlsynth_version,
                    "enumeration_runtime_overrides_source": !same_driver_runtime(&discovery.source_runtime, &discovery.discovery_runtime),
                    "crate_version_label": version_label("crate", &discovery.source_runtime.driver_version),
                    "dso_version_label": version_label("dso", version),
                    "enumeration_runtime_dso_version_label": version_label("dso", &discovery.discovery_runtime_xlsynth_version),
                    "dslx_path": discovery.import_context.dslx_path,
                    "dslx_stdlib_path": discovery.import_context.dslx_stdlib_path,
                    "stdlib_source": discovery.import_context.stdlib_source,
                    "scanned_dslx_files": discovery.scanned_dslx_files,
                    "listed_functions": discovery.listed_functions,
                    "concrete_functions": discovery.concrete_functions,
                    "failed_dslx_files_count": discovery.failed_dslx_files.len(),
                    "failed_dslx_files": discovery.failed_dslx_files,
                    "suggested_actions": suggested_next_actions.len(),
                }),
            );
        }
        Err(err) => {
            details.insert(
                "dslx_list_fns_discovery_error".to_string(),
                json!(format!("{:#}", err)),
            );
        }
    }

    Ok(ActionOutcome {
        dependencies: Vec::new(),
        output_artifact,
        commands,
        details: serde_json::Value::Object(details),
        suggested_next_actions,
    })
}

pub(crate) fn run_download_source_subtree_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action_id: &str,
    version: &str,
    subtree: &str,
    discovery_runtime: Option<&DriverRuntimeSpec>,
    payload_dir: &Path,
) -> Result<ActionOutcome> {
    let normalized_subtree = normalize_subtree_path(subtree)?;
    let client = Client::builder()
        .build()
        .context("creating reqwest client")?;
    let tarball_url = format!("{XLSYNTH_SOURCE_ARCHIVE_URL_PREFIX}/{version}.tar.gz");
    let tarball_path = payload_dir
        .parent()
        .ok_or_else(|| anyhow!("payload path missing parent"))?
        .join("xlsynth-source.tar.gz");
    download_to_file(&client, &tarball_url, &tarball_path)?;
    let source_tarball_sha256 = sha256_file(&tarball_path)?;
    let extracted_file_count =
        extract_tar_gz_subtree_safely(&tarball_path, payload_dir, &normalized_subtree)?;
    fs::remove_file(&tarball_path).ok();

    let output_artifact = ArtifactRef {
        action_id: action_id.to_string(),
        artifact_type: ArtifactType::DslxFileSubtree,
        relpath: "payload".to_string(),
    };

    let mut commands = Vec::new();
    let mut suggested_next_actions = Vec::new();
    let mut details = serde_json::Map::new();
    details.insert(
        "download".to_string(),
        json!({
            "source_archive_url": tarball_url,
            "source_archive_sha256": source_tarball_sha256,
        }),
    );
    details.insert("subtree".to_string(), json!(normalized_subtree.clone()));
    details.insert(
        "extracted_file_count".to_string(),
        json!(extracted_file_count),
    );

    match discover_dslx_fn_to_ir_suggestions(
        store,
        repo_root,
        action_id,
        payload_dir,
        version,
        discovery_runtime,
    ) {
        Ok(discovery) => {
            commands.extend(discovery.commands);
            suggested_next_actions = discovery.suggested_next_actions;
            details.insert(
                "dslx_list_fns_discovery".to_string(),
                json!({
                    "driver_runtime": discovery.source_runtime,
                    "enumeration_runtime": discovery.discovery_runtime,
                    "enumeration_runtime_xlsynth_version": discovery.discovery_runtime_xlsynth_version,
                    "enumeration_runtime_overrides_source": !same_driver_runtime(&discovery.source_runtime, &discovery.discovery_runtime),
                    "crate_version_label": version_label("crate", &discovery.source_runtime.driver_version),
                    "dso_version_label": version_label("dso", version),
                    "enumeration_runtime_dso_version_label": version_label("dso", &discovery.discovery_runtime_xlsynth_version),
                    "dslx_path": discovery.import_context.dslx_path,
                    "dslx_stdlib_path": discovery.import_context.dslx_stdlib_path,
                    "stdlib_source": discovery.import_context.stdlib_source,
                    "scanned_dslx_files": discovery.scanned_dslx_files,
                    "listed_functions": discovery.listed_functions,
                    "concrete_functions": discovery.concrete_functions,
                    "failed_dslx_files_count": discovery.failed_dslx_files.len(),
                    "failed_dslx_files": discovery.failed_dslx_files,
                    "suggested_actions": suggested_next_actions.len(),
                }),
            );
        }
        Err(err) => {
            details.insert(
                "dslx_list_fns_discovery_error".to_string(),
                json!(format!("{:#}", err)),
            );
        }
    }

    Ok(ActionOutcome {
        dependencies: Vec::new(),
        output_artifact,
        commands,
        details: serde_json::Value::Object(details),
        suggested_next_actions,
    })
}

pub(crate) fn determine_dslx_import_context(subtree_root: &Path) -> DslxImportContext {
    let subtree_stdlib = subtree_root.join("xls/dslx/stdlib");
    if subtree_stdlib.is_dir() {
        return DslxImportContext {
            dslx_path: ["/inputs/subtree", "/inputs/subtree/xls/dslx/stdlib"]
                .join(DSLX_PATH_LIST_SEPARATOR),
            dslx_stdlib_path: "/inputs/subtree/xls/dslx/stdlib".to_string(),
            stdlib_source: "subtree".to_string(),
        };
    }
    DslxImportContext {
        dslx_path: [
            "/inputs/subtree",
            "/tmp/xlsynth-release",
            "/tmp/xlsynth-release/xls/dslx/stdlib",
        ]
        .join(DSLX_PATH_LIST_SEPARATOR),
        dslx_stdlib_path: "/tmp/xlsynth-release/xls/dslx/stdlib".to_string(),
        stdlib_source: "toolchain_release".to_string(),
    }
}

pub(crate) fn discover_dslx_fn_to_ir_suggestions(
    store: &ArtifactStore,
    repo_root: &Path,
    dslx_subtree_action_id: &str,
    subtree_root: &Path,
    version: &str,
    discovery_runtime: Option<&DriverRuntimeSpec>,
) -> Result<DslxFnDiscovery> {
    let dslx_files = collect_dslx_files(subtree_root)?;
    let import_context = determine_dslx_import_context(subtree_root);
    let source_runtime = discovery_runtime
        .cloned()
        .unwrap_or(default_driver_runtime_for_version(repo_root, version)?);
    let discovery_runtime = resolve_driver_runtime_for_dslx_list_fns(repo_root, &source_runtime)
        .unwrap_or_else(|_| source_runtime.clone());
    let discovery_runtime_xlsynth_version =
        resolve_xlsynth_version_for_driver(repo_root, &discovery_runtime.driver_version)?;

    if dslx_files.is_empty() {
        return Ok(DslxFnDiscovery {
            commands: Vec::new(),
            source_runtime,
            discovery_runtime,
            discovery_runtime_xlsynth_version,
            import_context,
            scanned_dslx_files: 0,
            listed_functions: 0,
            concrete_functions: 0,
            failed_dslx_files: Vec::new(),
            suggested_next_actions: Vec::new(),
        });
    }

    let mut commands = Vec::new();
    ensure_driver_runtime_prepared(
        store,
        repo_root,
        &discovery_runtime_xlsynth_version,
        &discovery_runtime,
        &mut commands,
    )?;

    let scratch_root = subtree_root
        .parent()
        .ok_or_else(|| anyhow!("subtree root missing parent"))?;
    let scratch_dir = scratch_root.join("dslx-list-fns-discovery");
    let result = (|| -> Result<DslxFnDiscovery> {
        if scratch_dir.exists() {
            fs::remove_dir_all(&scratch_dir).with_context(|| {
                format!(
                    "removing previous dslx-list-fns scratch directory: {}",
                    scratch_dir.display()
                )
            })?;
        }
        fs::create_dir_all(&scratch_dir).with_context(|| {
            format!(
                "creating dslx-list-fns scratch directory: {}",
                scratch_dir.display()
            )
        })?;

        let list_path = scratch_dir.join("dslx_files.txt");
        let mut list_text = String::new();
        for dslx_file in &dslx_files {
            list_text.push_str(dslx_file);
            list_text.push('\n');
        }
        fs::write(&list_path, list_text)
            .with_context(|| format!("writing dslx file list: {}", list_path.display()))?;

        let output_jsonl_path = scratch_dir.join("dslx_list_fns.jsonl");
        let output_errors_path = scratch_dir.join("dslx_list_fns_errors.jsonl");
        let script = driver_script(
            r#"
cat > /tmp/xlsynth-toolchain.toml <<'TOML'
[toolchain]
tool_path = "/tmp/xlsynth-release"
TOML
python3 - <<'PY'
import os
import json
import subprocess
from pathlib import Path

files = [line.strip() for line in Path("/scratch/dslx_files.txt").read_text(encoding="utf-8").splitlines() if line.strip()]
out_path = Path("/scratch/dslx_list_fns.jsonl")
err_path = Path("/scratch/dslx_list_fns_errors.jsonl")
dslx_path = os.environ["DSLX_PATH"]
stdlib_path = os.environ["DSLX_STDLIB_PATH"]
def _tail(text: str, max_chars: int = 2000) -> str:
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]

with out_path.open("w", encoding="utf-8") as out, err_path.open("w", encoding="utf-8") as err:
    for rel in files:
        cmd = [
            "xlsynth-driver",
            "--toolchain",
            "/tmp/xlsynth-toolchain.toml",
            "dslx-list-fns",
            "--dslx_input_file",
            f"/inputs/subtree/{rel}",
            "--dslx_path",
            dslx_path,
            "--dslx_stdlib_path",
            stdlib_path,
            "--format",
            "json",
        ]
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
        if proc.returncode != 0:
            err.write(json.dumps({
                "dslx_file": rel,
                "returncode": proc.returncode,
                "stdout_tail": _tail(proc.stdout),
                "stderr_tail": _tail(proc.stderr),
            }, sort_keys=True))
            err.write("\n")
            continue
        try:
            entries = json.loads(proc.stdout)
        except Exception as e:
            err.write(json.dumps({
                "dslx_file": rel,
                "returncode": proc.returncode,
                "parse_error": str(e),
                "stdout_tail": _tail(proc.stdout),
                "stderr_tail": _tail(proc.stderr),
            }, sort_keys=True))
            err.write("\n")
            continue
        if not isinstance(entries, list):
            err.write(json.dumps({
                "dslx_file": rel,
                "returncode": proc.returncode,
                "parse_error": f"dslx-list-fns expected list output, got {type(entries)}",
                "stdout_tail": _tail(proc.stdout),
                "stderr_tail": _tail(proc.stderr),
            }, sort_keys=True))
            err.write("\n")
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            entry["dslx_file"] = rel
            out.write(json.dumps(entry, sort_keys=True))
            out.write("\n")
PY
test -f /scratch/dslx_list_fns.jsonl
test -f /scratch/dslx_list_fns_errors.jsonl
"#,
        );

        let mut env = BTreeMap::new();
        env.insert(
            "XLSYNTH_VERSION".to_string(),
            discovery_runtime_xlsynth_version.to_string(),
        );
        env.insert(
            "XLSYNTH_PLATFORM".to_string(),
            discovery_runtime.release_platform.to_string(),
        );
        env.insert("DSLX_PATH".to_string(), import_context.dslx_path.clone());
        env.insert(
            "DSLX_STDLIB_PATH".to_string(),
            import_context.dslx_stdlib_path.clone(),
        );
        let mounts = vec![
            DockerMount::read_only(subtree_root, "/inputs/subtree")?,
            DockerMount::read_write(&scratch_dir, "/scratch")?,
            driver_cache_mount(store)?,
        ];
        let run_trace = run_docker_script(
            &discovery_runtime.docker_image,
            &mounts,
            &env,
            &script,
            dslx_subtree_action_id,
        )?;
        commands.push(run_trace);

        let jsonl_text = fs::read_to_string(&output_jsonl_path).with_context(|| {
            format!(
                "reading dslx-list-fns output file: {}",
                output_jsonl_path.display()
            )
        })?;
        let mut discovered = parse_discovered_dslx_functions(&jsonl_text)?;
        discovered.sort_by(|a, b| {
            a.dslx_file
                .cmp(&b.dslx_file)
                .then(a.fn_name.cmp(&b.fn_name))
                .then(a.is_parametric.cmp(&b.is_parametric))
        });
        let errors_jsonl = fs::read_to_string(&output_errors_path).with_context(|| {
            format!(
                "reading dslx-list-fns error output file: {}",
                output_errors_path.display()
            )
        })?;
        let mut failed_dslx_files = parse_dslx_list_fns_failed_files(&errors_jsonl)?;
        failed_dslx_files.sort();
        failed_dslx_files.dedup();

        let listed_functions = discovered.len();
        let mut concrete_functions = 0_usize;
        let mut suggested_next_actions = Vec::new();
        let mut seen_action_ids = HashSet::new();

        for record in discovered {
            if record.is_parametric {
                continue;
            }
            concrete_functions += 1;
            if let Ok(next) = build_suggestion(
                format!(
                    "Convert concrete DSLX function `{}` in `{}` to IR with DriverDslxFnToIr",
                    record.fn_name, record.dslx_file
                ),
                ActionSpec::DriverDslxFnToIr {
                    dslx_subtree_action_id: dslx_subtree_action_id.to_string(),
                    dslx_file: record.dslx_file,
                    dslx_fn_name: record.fn_name,
                    version: version.to_string(),
                    runtime: source_runtime.clone(),
                },
            ) && seen_action_ids.insert(next.action_id.clone())
            {
                suggested_next_actions.push(next);
            }
        }

        Ok(DslxFnDiscovery {
            commands,
            source_runtime,
            discovery_runtime,
            discovery_runtime_xlsynth_version,
            import_context,
            scanned_dslx_files: dslx_files.len(),
            listed_functions,
            concrete_functions,
            failed_dslx_files,
            suggested_next_actions,
        })
    })();
    fs::remove_dir_all(&scratch_dir).ok();
    result
}

pub(crate) fn collect_dslx_files(subtree_root: &Path) -> Result<Vec<String>> {
    let mut files = Vec::new();
    for entry in WalkDir::new(subtree_root).sort_by_file_name() {
        let entry = entry.context("walking DSLX subtree")?;
        if !entry.file_type().is_file() {
            continue;
        }
        if entry.path().extension().and_then(|s| s.to_str()) != Some("x") {
            continue;
        }
        let rel = entry
            .path()
            .strip_prefix(subtree_root)
            .with_context(|| format!("stripping subtree prefix for {}", entry.path().display()))?;
        let rel = rel.to_string_lossy().replace('\\', "/");
        validate_relative_subpath(&rel)?;
        files.push(rel);
    }
    files.sort();
    Ok(files)
}

pub(crate) fn parse_discovered_dslx_functions(
    jsonl_text: &str,
) -> Result<Vec<DiscoveredDslxFunction>> {
    let mut out = Vec::new();
    for (idx, line) in jsonl_text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(line)
            .with_context(|| format!("parsing dslx-list-fns jsonl line {}", idx + 1))?;
        let dslx_file = value
            .get("dslx_file")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("dslx-list-fns line {} missing string dslx_file", idx + 1))?
            .to_string();
        validate_relative_subpath(&dslx_file)?;
        let fn_name = value
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("dslx-list-fns line {} missing string name", idx + 1))?
            .to_string();
        if fn_name.is_empty() {
            bail!("dslx-list-fns line {} had empty function name", idx + 1);
        }
        let is_parametric = value
            .get("is_parametric")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        out.push(DiscoveredDslxFunction {
            dslx_file,
            fn_name,
            is_parametric,
        });
    }
    Ok(out)
}

pub(crate) fn parse_dslx_list_fns_failed_files(jsonl_text: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for (idx, line) in jsonl_text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(line)
            .with_context(|| format!("parsing dslx-list-fns error jsonl line {}", idx + 1))?;
        let dslx_file = value
            .get("dslx_file")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                anyhow!(
                    "dslx-list-fns error line {} missing string dslx_file",
                    idx + 1
                )
            })?
            .to_string();
        validate_relative_subpath(&dslx_file)?;
        out.push(dslx_file);
    }
    Ok(out)
}

pub(crate) fn download_to_file(client: &Client, url: &str, path: &Path) -> Result<()> {
    let mut response = client
        .get(url)
        .send()
        .with_context(|| format!("downloading URL: {url}"))?
        .error_for_status()
        .with_context(|| format!("status check failed for URL: {url}"))?;

    let mut out = fs::File::create(path)
        .with_context(|| format!("creating output file: {}", path.display()))?;
    std::io::copy(&mut response, &mut out)
        .with_context(|| format!("streaming URL response into file: {}", path.display()))?;
    Ok(())
}

pub(crate) fn parse_sha256_text(text: &str) -> Result<String> {
    let first = text
        .split_whitespace()
        .next()
        .ok_or_else(|| anyhow!("sha256 file did not contain checksum text"))?;
    if first.len() != 64 || !first.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("invalid sha256 checksum format in sha256 file: {first}");
    }
    Ok(first.to_ascii_lowercase())
}

pub(crate) fn extract_tar_gz_safely(tarball_path: &Path, destination: &Path) -> Result<()> {
    let tar_gz = fs::File::open(tarball_path)
        .with_context(|| format!("opening tarball: {}", tarball_path.display()))?;
    let decoder = flate2::read::GzDecoder::new(tar_gz);
    let mut archive = tar::Archive::new(decoder);
    for entry in archive.entries().context("iterating tar entries")? {
        let mut entry = entry.context("reading tar entry")?;
        entry
            .unpack_in(destination)
            .with_context(|| format!("unpacking entry into {}", destination.display()))?;
    }
    Ok(())
}

pub(crate) fn extract_tar_gz_subtree_safely(
    tarball_path: &Path,
    destination: &Path,
    subtree: &str,
) -> Result<usize> {
    let subtree = normalize_subtree_path(subtree)?;
    let subtree_path = Path::new(&subtree);
    let tar_gz = fs::File::open(tarball_path)
        .with_context(|| format!("opening source tarball: {}", tarball_path.display()))?;
    let decoder = flate2::read::GzDecoder::new(tar_gz);
    let mut archive = tar::Archive::new(decoder);
    let mut extracted_file_count = 0_usize;
    for entry in archive
        .entries()
        .context("iterating source tarball entries")?
    {
        let mut entry = entry.context("reading source tarball entry")?;
        let entry_path = entry.path().context("reading source tarball entry path")?;
        let mut normalized_components: Vec<String> = Vec::new();
        for comp in entry_path.components() {
            match comp {
                Component::Normal(part) => {
                    let part = part
                        .to_str()
                        .ok_or_else(|| anyhow!("archive entry path component was not utf-8"))?;
                    normalized_components.push(part.to_string());
                }
                Component::CurDir => {}
                Component::ParentDir => {
                    bail!(
                        "archive entry contained parent dir component: {:?}",
                        entry_path
                    )
                }
                Component::Prefix(_) | Component::RootDir => {
                    bail!(
                        "archive entry contained absolute/prefixed path: {:?}",
                        entry_path
                    )
                }
            }
        }
        if normalized_components.is_empty() {
            continue;
        }

        let mut candidates = Vec::with_capacity(2);
        candidates.push(normalized_components.join("/"));
        if normalized_components.len() > 1 {
            candidates.push(normalized_components[1..].join("/"));
        }

        let matched_relpath = candidates
            .into_iter()
            .find(|candidate| {
                let candidate_path = Path::new(candidate);
                candidate_path == subtree_path || candidate_path.starts_with(subtree_path)
            })
            .map(|v| normalize_subtree_path(&v))
            .transpose()?;
        let Some(relpath) = matched_relpath else {
            continue;
        };

        let target_path = destination.join(&relpath);
        let entry_type = entry.header().entry_type();
        if entry_type.is_dir() {
            fs::create_dir_all(&target_path).with_context(|| {
                format!("creating extracted directory: {}", target_path.display())
            })?;
            continue;
        }
        if entry_type.is_symlink() || entry_type.is_hard_link() {
            bail!(
                "source archive subtree contains unsupported link entry at {}",
                relpath
            );
        }
        if !entry_type.is_file() {
            continue;
        }
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "creating parent directory for extracted file: {}",
                    parent.display()
                )
            })?;
        }
        entry
            .unpack(&target_path)
            .with_context(|| format!("extracting file into {}", target_path.display()))?;
        extracted_file_count += 1;
    }
    if extracted_file_count == 0 {
        bail!(
            "source archive did not contain any files under requested subtree `{}`",
            subtree
        );
    }
    Ok(extracted_file_count)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_driver_dslx_fn_to_ir_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action_id: &str,
    dslx_subtree_action_id: &str,
    dslx_file: &str,
    dslx_fn_name: &str,
    version: &str,
    runtime: &DriverRuntimeSpec,
    payload_dir: &Path,
) -> Result<ActionOutcome> {
    validate_relative_subpath(dslx_file)?;

    let dep =
        load_dependency_of_type(store, dslx_subtree_action_id, ArtifactType::DslxFileSubtree)?;
    let subtree_root = store.resolve_artifact_ref_path(&dep);
    let input_dslx_path = subtree_root.join(dslx_file);
    if !input_dslx_path.exists() {
        bail!(
            "dslx input file does not exist in subtree: {}",
            input_dslx_path.display()
        );
    }

    let mut commands = Vec::new();
    ensure_driver_runtime_prepared(store, repo_root, version, runtime, &mut commands)?;
    let import_context = determine_dslx_import_context(&subtree_root);

    let script = driver_script(
        r#"
cat > /tmp/xlsynth-toolchain.toml <<'TOML'
[toolchain]
tool_path = "/tmp/xlsynth-release"
TOML
xlsynth-driver --toolchain /tmp/xlsynth-toolchain.toml dslx2ir \
  --dslx_input_file "/inputs/subtree/${DSLX_FILE}" \
  --dslx_top "${DSLX_FN_NAME}" \
  --dslx_path "${DSLX_PATH}" \
  --dslx_stdlib_path "${DSLX_STDLIB_PATH}" \
  > /outputs/package.ir
"#,
    );

    let mut env = BTreeMap::new();
    env.insert("XLSYNTH_VERSION".to_string(), version.to_string());
    env.insert(
        "XLSYNTH_PLATFORM".to_string(),
        runtime.release_platform.to_string(),
    );
    env.insert("DSLX_FILE".to_string(), dslx_file.to_string());
    env.insert("DSLX_FN_NAME".to_string(), dslx_fn_name.to_string());
    env.insert("DSLX_PATH".to_string(), import_context.dslx_path.clone());
    env.insert(
        "DSLX_STDLIB_PATH".to_string(),
        import_context.dslx_stdlib_path.clone(),
    );

    let mounts = vec![
        DockerMount::read_only(&subtree_root, "/inputs/subtree")?,
        DockerMount::read_write(payload_dir, "/outputs")?,
        driver_cache_mount(store)?,
    ];

    let run_trace = run_docker_script(&runtime.docker_image, &mounts, &env, &script, action_id)?;
    commands.push(run_trace);

    let ir_output_path = payload_dir.join("package.ir");
    let ir_top = match infer_ir_top_function(&ir_output_path) {
        Ok(top) => top,
        Err(_) => {
            let module_name = dslx_module_name_from_path(dslx_file)?;
            dslx_to_mangled_ir_fn_name(&module_name, dslx_fn_name)
        }
    };
    let next = build_suggestion(
        "Optimize generated IR package with DriverIrToOpt",
        ActionSpec::DriverIrToOpt {
            ir_action_id: action_id.to_string(),
            top_fn_name: Some(ir_top.clone()),
            version: version.to_string(),
            runtime: runtime.clone(),
        },
    )?;

    let output_artifact = ArtifactRef {
        action_id: action_id.to_string(),
        artifact_type: ArtifactType::IrPackageFile,
        relpath: "payload/package.ir".to_string(),
    };

    Ok(ActionOutcome {
        dependencies: vec![dep],
        output_artifact,
        commands,
        details: json!({
            "driver_runtime": runtime,
            "xlsynth_version": version,
            "crate_version_label": version_label("crate", &runtime.driver_version),
            "dso_version_label": version_label("dso", version),
            "dslx_file": dslx_file,
            "dslx_fn_name": dslx_fn_name,
            "ir_top": ir_top,
            "dslx_path": import_context.dslx_path,
            "dslx_stdlib_path": import_context.dslx_stdlib_path,
            "stdlib_source": import_context.stdlib_source,
        }),
        suggested_next_actions: vec![next],
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_driver_ir_to_opt_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action_id: &str,
    ir_action_id: &str,
    top_fn_name: Option<&str>,
    version: &str,
    runtime: &DriverRuntimeSpec,
    payload_dir: &Path,
) -> Result<ActionOutcome> {
    let dep = load_dependency_of_type(store, ir_action_id, ArtifactType::IrPackageFile)?;
    let ir_input_path = store.resolve_artifact_ref_path(&dep);
    if !ir_input_path.exists() {
        bail!("input IR path does not exist: {}", ir_input_path.display());
    }

    let ir_top = match top_fn_name {
        Some(top) => top.to_string(),
        None => infer_ir_top_function(&ir_input_path)?,
    };

    let mut commands = Vec::new();
    ensure_driver_runtime_prepared(store, repo_root, version, runtime, &mut commands)?;

    let next_no_fraig = build_suggestion(
        "Convert optimized IR to AIG with DriverIrToG8rAig (fraig=false)",
        ActionSpec::DriverIrToG8rAig {
            ir_action_id: action_id.to_string(),
            top_fn_name: None,
            fraig: false,
            lowering_mode: G8rLoweringMode::Default,
            version: version.to_string(),
            runtime: runtime.clone(),
        },
    )?;
    let next_fraig = build_suggestion(
        "Convert optimized IR to AIG with DriverIrToG8rAig (fraig=true)",
        ActionSpec::DriverIrToG8rAig {
            ir_action_id: action_id.to_string(),
            top_fn_name: None,
            fraig: true,
            lowering_mode: G8rLoweringMode::Default,
            version: version.to_string(),
            runtime: runtime.clone(),
        },
    )?;
    let next_frontend_no_rewrite = build_suggestion(
        "Convert optimized IR to frontend-only g8r AIG (fraig=false, prep rewrites off)",
        ActionSpec::DriverIrToG8rAig {
            ir_action_id: action_id.to_string(),
            top_fn_name: None,
            fraig: false,
            lowering_mode: G8rLoweringMode::FrontendNoPrepRewrite,
            version: version.to_string(),
            runtime: runtime.clone(),
        },
    )?;
    let next_combo = build_suggestion(
        "Convert optimized IR to combinational Verilog with IrFnToCombinationalVerilog",
        ActionSpec::IrFnToCombinationalVerilog {
            ir_action_id: action_id.to_string(),
            top_fn_name: Some(ir_top.clone()),
            use_system_verilog: false,
            version: version.to_string(),
            runtime: runtime.clone(),
        },
    )?;
    let next_ir_equiv = build_suggestion(
        "Check unoptimized vs optimized IR equivalence with DriverIrEquiv",
        ActionSpec::DriverIrEquiv {
            lhs_ir_action_id: ir_action_id.to_string(),
            rhs_ir_action_id: action_id.to_string(),
            top_fn_name: Some(ir_top.clone()),
            version: version.to_string(),
            runtime: runtime.clone(),
        },
    )?;
    let next_delay_info = build_suggestion(
        "Compute optimized IR delay info with DriverIrToDelayInfo",
        ActionSpec::DriverIrToDelayInfo {
            ir_action_id: action_id.to_string(),
            top_fn_name: Some(ir_top.clone()),
            delay_model: "asap7".to_string(),
            output_format: DELAY_INFO_OUTPUT_FORMAT_TEXTPROTO_V1.to_string(),
            version: version.to_string(),
            runtime: runtime.clone(),
        },
    )?;

    let output_artifact = ArtifactRef {
        action_id: action_id.to_string(),
        artifact_type: ArtifactType::IrPackageFile,
        relpath: "payload/package.opt.ir".to_string(),
    };

    let mut details = serde_json::Map::new();
    details.insert("driver_runtime".to_string(), json!(runtime));
    details.insert("xlsynth_version".to_string(), json!(version));
    insert_driver_version_labels(&mut details, version, runtime);
    details.insert("ir_top".to_string(), json!(ir_top.clone()));
    let ir_aig_equiv_runtime = resolve_driver_runtime_for_aig_stats(repo_root, runtime)
        .unwrap_or_else(|_| runtime.clone());
    if ir_aig_equiv_runtime != *runtime {
        details.insert(
            "driver_ir_aig_equiv_runtime".to_string(),
            json!(ir_aig_equiv_runtime.clone()),
        );
    }
    let mut opt_ir_aig_equiv_suggestions = Vec::new();
    if !driver_ir_aig_equiv_enabled() {
        details.insert("driver_ir_aig_equiv_supported".to_string(), json!(false));
        details.insert("driver_ir_aig_equiv_quarantined".to_string(), json!(true));
    } else {
        match resolve_driver_ir_aig_equiv_supported(
            store,
            repo_root,
            version,
            &ir_aig_equiv_runtime,
        ) {
            Ok(true) => {
                details.insert("driver_ir_aig_equiv_supported".to_string(), json!(true));
                details.insert(
                    "driver_ir_aig_equiv_mode".to_string(),
                    json!("aig2ir_then_ir_equiv"),
                );
                match build_opt_ir_aig_equiv_suggestions(
                    action_id,
                    &ir_top,
                    &next_no_fraig.action_id,
                    &next_fraig.action_id,
                    version,
                    &ir_aig_equiv_runtime,
                ) {
                    Ok(next) => opt_ir_aig_equiv_suggestions = next,
                    Err(err) => {
                        details.insert(
                            "driver_ir_aig_equiv_suggestions_error".to_string(),
                            json!(format!("{:#}", err)),
                        );
                    }
                }
            }
            Ok(false) => {
                details.insert("driver_ir_aig_equiv_supported".to_string(), json!(false));
            }
            Err(err) => {
                details.insert(
                    "driver_ir_aig_equiv_probe_error".to_string(),
                    json!(format!("{:#}", err)),
                );
            }
        }
    }

    let mut input_ir_structural_hash = None;
    match compute_ir_fn_structural_hash(
        store,
        repo_root,
        &ir_input_path,
        Some(&ir_top),
        version,
        runtime,
    ) {
        Ok((hash, trace)) => {
            commands.push(trace);
            details.insert(
                INPUT_IR_FN_STRUCTURAL_HASH_DETAILS_KEY.to_string(),
                json!(hash.clone()),
            );
            input_ir_structural_hash = Some(hash);
        }
        Err(err) => {
            details.insert(
                "input_ir_fn_structural_hash_error".to_string(),
                json!(format!("{:#}", err)),
            );
        }
    }

    let mut reused_payload = false;
    let mut reused_output_ir_structural_hash: Option<String> = None;
    if let Some(hash) = input_ir_structural_hash.as_deref() {
        let reusable = find_semantic_reuse_candidate(store, hash, |provenance| {
            if provenance.action_id == action_id {
                return false;
            }
            let matches_action = matches!(
                &provenance.action,
                ActionSpec::DriverIrToOpt {
                    version: candidate_version,
                    runtime: candidate_runtime,
                    ..
                } if candidate_version == version && same_driver_runtime(candidate_runtime, runtime)
            );
            if !matches_action {
                return false;
            }
            provenance.details.get("ir_top").and_then(|v| v.as_str()) == Some(ir_top.as_str())
        })?;
        if let Some(source) = reusable {
            reuse_payload_from_provenance(store, &source, payload_dir)?;
            details.insert(
                "semantic_cache_hit".to_string(),
                json!({
                    "from_action_id": source.action_id,
                    "key": "ir_fn_structural_hash",
                }),
            );
            reused_payload = true;
            if let Some(raw_hash) = source
                .details
                .get("output_ir_fn_structural_hash")
                .and_then(|v| v.as_str())
                && let Some(normalized) = normalized_structural_hash(raw_hash)
            {
                reused_output_ir_structural_hash = Some(normalized);
            }
        }
    }

    if !reused_payload {
        let script = driver_script(
            r#"
cat > /tmp/xlsynth-toolchain.toml <<'TOML'
[toolchain]
tool_path = "/tmp/xlsynth-release"
TOML
xlsynth-driver --toolchain /tmp/xlsynth-toolchain.toml ir2opt /inputs/input.ir --top "${IR_TOP}" > /outputs/package.opt.ir
"#,
        );

        let mut env = BTreeMap::new();
        env.insert("XLSYNTH_VERSION".to_string(), version.to_string());
        env.insert(
            "XLSYNTH_PLATFORM".to_string(),
            runtime.release_platform.to_string(),
        );
        env.insert("IR_TOP".to_string(), ir_top.clone());

        let mounts = vec![
            DockerMount::read_only(&ir_input_path, "/inputs/input.ir")?,
            DockerMount::read_write(payload_dir, "/outputs")?,
            driver_cache_mount(store)?,
        ];

        let run_trace =
            run_docker_script(&runtime.docker_image, &mounts, &env, &script, action_id)?;
        commands.push(run_trace);
    }

    let output_ir_path = payload_dir.join("package.opt.ir");
    let _output_ir_op_count = match parse_ir_fn_op_count_from_file(&output_ir_path, &ir_top) {
        Ok(Some(count)) => {
            details.insert(OUTPUT_IR_OP_COUNT_DETAILS_KEY.to_string(), json!(count));
            Some(count)
        }
        Ok(None) => {
            details.insert(
                "output_ir_op_count_error".to_string(),
                json!("unable to determine op count for optimized IR top"),
            );
            None
        }
        Err(err) => {
            details.insert(
                "output_ir_op_count_error".to_string(),
                json!(format!("{:#}", err)),
            );
            None
        }
    };
    let k_bool_cone_max_ir_ops = default_k_bool_cone_max_ir_ops_for_k(3);
    details.insert(
        "k_bool_cone_max_output_ir_ops".to_string(),
        json!(k_bool_cone_max_ir_ops),
    );
    let mut output_ir_structural_hash = reused_output_ir_structural_hash;
    if output_ir_structural_hash.is_none() {
        match compute_ir_fn_structural_hash(
            store,
            repo_root,
            &output_ir_path,
            Some(&ir_top),
            version,
            runtime,
        ) {
            Ok((hash, trace)) => {
                commands.push(trace);
                output_ir_structural_hash = Some(hash.clone());
                details.insert("output_ir_fn_structural_hash".to_string(), json!(hash));
            }
            Err(err) => {
                details.insert(
                    "output_ir_fn_structural_hash_error".to_string(),
                    json!(format!("{:#}", err)),
                );
            }
        }
    } else if let Some(hash) = output_ir_structural_hash.clone() {
        details.insert("output_ir_fn_structural_hash".to_string(), json!(hash));
    }

    let mut suggested_next_actions = vec![
        next_ir_equiv,
        next_delay_info,
        next_no_fraig,
        next_fraig,
        next_frontend_no_rewrite,
        next_combo,
    ];
    suggested_next_actions.extend(opt_ir_aig_equiv_suggestions);
    let mut k_bool_cone_reason = "missing_structural_hash";
    let mut k_bool_cone_eligible = false;
    if !reused_payload {
        if let Some(hash) = output_ir_structural_hash.as_deref() {
            if is_first_opt_action_for_output_structural_hash(store, action_id, hash)? {
                k_bool_cone_reason = "eligible";
                k_bool_cone_eligible = true;
            } else {
                k_bool_cone_reason = "not_first_structural_owner";
            }
        }
    } else {
        k_bool_cone_reason = "semantic_cache_hit";
    }
    details.insert(
        "k_bool_cone_suggestion_eligible".to_string(),
        json!(k_bool_cone_eligible),
    );
    details.insert(
        "k_bool_cone_suggestion_reason".to_string(),
        json!(k_bool_cone_reason),
    );
    if k_bool_cone_eligible {
        let next_k_bool_cones = build_suggestion(
            "Extract k=3 bool-cone corpus from structurally unique optimized IR and keep cones with <=16 IR ops",
            ActionSpec::IrFnToKBoolConeCorpus {
                ir_action_id: action_id.to_string(),
                top_fn_name: Some(ir_top.clone()),
                k: 3,
                max_ir_ops: k_bool_cone_max_ir_ops,
                version: version.to_string(),
                runtime: runtime.clone(),
            },
        )?;
        suggested_next_actions.push(next_k_bool_cones);
    }

    Ok(ActionOutcome {
        dependencies: vec![dep],
        output_artifact,
        commands,
        details: serde_json::Value::Object(details),
        suggested_next_actions,
    })
}

pub(crate) fn build_opt_ir_aig_equiv_suggestions(
    opt_ir_action_id: &str,
    ir_top: &str,
    g8r_no_fraig_action_id: &str,
    g8r_fraig_action_id: &str,
    version: &str,
    runtime: &DriverRuntimeSpec,
) -> Result<Vec<SuggestedAction>> {
    let no_fraig = build_suggestion(
        "Check optimized IR and fraig=false g8r AIG equivalence with DriverIrAigEquiv",
        ActionSpec::DriverIrAigEquiv {
            ir_action_id: opt_ir_action_id.to_string(),
            aig_action_id: g8r_no_fraig_action_id.to_string(),
            top_fn_name: Some(ir_top.to_string()),
            version: version.to_string(),
            runtime: runtime.clone(),
        },
    )?;
    let fraig = build_suggestion(
        "Check optimized IR and fraig=true g8r AIG equivalence with DriverIrAigEquiv",
        ActionSpec::DriverIrAigEquiv {
            ir_action_id: opt_ir_action_id.to_string(),
            aig_action_id: g8r_fraig_action_id.to_string(),
            top_fn_name: Some(ir_top.to_string()),
            version: version.to_string(),
            runtime: runtime.clone(),
        },
    )?;
    Ok(vec![no_fraig, fraig])
}

pub(crate) fn resolve_driver_ir_aig_equiv_supported(
    store: &ArtifactStore,
    repo_root: &Path,
    version: &str,
    runtime: &DriverRuntimeSpec,
) -> Result<bool> {
    let supports_aig2ir =
        driver_supports_subcommand_cached(store, repo_root, version, runtime, "aig2ir")?;
    let supports_ir_equiv =
        driver_supports_subcommand_cached(store, repo_root, version, runtime, "ir-equiv")?;
    Ok(supports_aig2ir && supports_ir_equiv)
}

pub(crate) fn resolve_driver_ir2gates_prepared_ir_supported(
    store: &ArtifactStore,
    repo_root: &Path,
    version: &str,
    runtime: &DriverRuntimeSpec,
) -> Result<bool> {
    let supports_ir2gates =
        driver_supports_subcommand_cached(store, repo_root, version, runtime, "ir2gates")?;
    if !supports_ir2gates {
        return Ok(false);
    }
    driver_subcommand_help_has_token_cached(
        store,
        repo_root,
        version,
        runtime,
        "ir2gates",
        "--prepared-ir-out",
    )
}

pub(crate) fn is_first_opt_action_for_output_structural_hash(
    store: &ArtifactStore,
    action_id: &str,
    structural_hash: &str,
) -> Result<bool> {
    let mut seen_prior = false;
    store.for_each_provenance(|provenance| {
        if provenance.action_id == action_id {
            return Ok(std::ops::ControlFlow::Continue(()));
        }
        if !matches!(provenance.action, ActionSpec::DriverIrToOpt { .. }) {
            return Ok(std::ops::ControlFlow::Continue(()));
        }
        let Some(existing_hash) = provenance
            .details
            .get("output_ir_fn_structural_hash")
            .and_then(|v| v.as_str())
            .and_then(normalized_structural_hash)
        else {
            return Ok(std::ops::ControlFlow::Continue(()));
        };
        if existing_hash == structural_hash {
            seen_prior = true;
            return Ok(std::ops::ControlFlow::Break(()));
        }
        Ok(std::ops::ControlFlow::Continue(()))
    })?;
    Ok(!seen_prior)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_driver_ir_to_delay_info_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action_id: &str,
    ir_action_id: &str,
    top_fn_name: Option<&str>,
    delay_model: &str,
    output_format: &str,
    version: &str,
    runtime: &DriverRuntimeSpec,
    payload_dir: &Path,
) -> Result<ActionOutcome> {
    let dep = load_dependency_of_type(store, ir_action_id, ArtifactType::IrPackageFile)?;
    let ir_input_path = store.resolve_artifact_ref_path(&dep);
    if !ir_input_path.exists() {
        bail!("input IR path does not exist: {}", ir_input_path.display());
    }

    let ir_top = match top_fn_name {
        Some(top) => Some(top.to_string()),
        None => infer_ir_top_function(&ir_input_path).ok(),
    };

    let mut commands = Vec::new();
    ensure_driver_runtime_prepared(store, repo_root, version, runtime, &mut commands)?;

    if output_format != DELAY_INFO_OUTPUT_FORMAT_TEXTPROTO_V1 {
        bail!("unsupported delay info output format: {}", output_format);
    }
    let output_relpath = "payload/delay_info.textproto".to_string();

    let output_artifact = ArtifactRef {
        action_id: action_id.to_string(),
        artifact_type: ArtifactType::IrDelayInfoFile,
        relpath: output_relpath.clone(),
    };

    let mut details = serde_json::Map::new();
    details.insert("driver_runtime".to_string(), json!(runtime));
    details.insert("xlsynth_version".to_string(), json!(version));
    insert_driver_version_labels(&mut details, version, runtime);
    details.insert("ir_top".to_string(), json!(ir_top.clone()));
    details.insert("delay_model".to_string(), json!(delay_model));
    details.insert("output_format".to_string(), json!(output_format));
    details.insert("driver_subcommand".to_string(), json!("delay_info_main"));
    details.insert(
        "delay_info_proto_schema_urls".to_string(),
        json!({
            "delay_info_proto": format!(
                "https://raw.githubusercontent.com/xlsynth/xlsynth/{}/xls/estimators/delay_model/delay_info.proto",
                version
            ),
            "op_proto": format!(
                "https://raw.githubusercontent.com/xlsynth/xlsynth/{}/xls/ir/op.proto",
                version
            ),
        }),
    );

    let mut input_ir_structural_hash = None;
    match compute_ir_fn_structural_hash(
        store,
        repo_root,
        &ir_input_path,
        ir_top.as_deref(),
        version,
        runtime,
    ) {
        Ok((hash, trace)) => {
            commands.push(trace);
            details.insert(
                INPUT_IR_FN_STRUCTURAL_HASH_DETAILS_KEY.to_string(),
                json!(hash.clone()),
            );
            input_ir_structural_hash = Some(hash);
        }
        Err(err) => {
            details.insert(
                "input_ir_fn_structural_hash_error".to_string(),
                json!(format!("{:#}", err)),
            );
        }
    }

    if let Some(hash) = input_ir_structural_hash.as_deref() {
        let reusable = find_semantic_reuse_candidate(store, hash, |provenance| {
            if provenance.action_id == action_id {
                return false;
            }
            let matches_action = matches!(
                &provenance.action,
                ActionSpec::DriverIrToDelayInfo {
                    delay_model: candidate_delay_model,
                    output_format: candidate_output_format,
                    version: candidate_version,
                    runtime: candidate_runtime,
                    ..
                } if candidate_delay_model == delay_model
                    && candidate_output_format == output_format
                    && candidate_version == version
                    && same_driver_runtime(candidate_runtime, runtime)
            );
            if !matches_action {
                return false;
            }
            provenance.details.get("ir_top").and_then(|v| v.as_str()) == ir_top.as_deref()
                && provenance.output_artifact.relpath == output_relpath
        })?;
        if let Some(source) = reusable {
            reuse_payload_from_provenance(store, &source, payload_dir)?;
            details.insert(
                "semantic_cache_hit".to_string(),
                json!({
                    "from_action_id": source.action_id,
                    "key": "ir_fn_structural_hash",
                }),
            );
            return Ok(ActionOutcome {
                dependencies: vec![dep],
                output_artifact,
                commands,
                details: serde_json::Value::Object(details),
                suggested_next_actions: Vec::new(),
            });
        }
    }

    let script = if ir_top.is_some() {
        driver_script(
            r#"
/tmp/xlsynth-release/delay_info_main --delay_model "${DELAY_MODEL}" --top "${IR_TOP}" --proto_out /tmp/delay_info.pb /inputs/input.ir > /tmp/delay_info_stdout.txt
protoc -I /tmp/xlsynth-release/protos --decode=xls.DelayInfoProto /tmp/xlsynth-release/protos/xls/estimators/delay_model/delay_info.proto < /tmp/delay_info.pb > /outputs/delay_info.textproto
test -s /outputs/delay_info.textproto
"#,
        )
    } else {
        driver_script(
            r#"
/tmp/xlsynth-release/delay_info_main --delay_model "${DELAY_MODEL}" --proto_out /tmp/delay_info.pb /inputs/input.ir > /tmp/delay_info_stdout.txt
protoc -I /tmp/xlsynth-release/protos --decode=xls.DelayInfoProto /tmp/xlsynth-release/protos/xls/estimators/delay_model/delay_info.proto < /tmp/delay_info.pb > /outputs/delay_info.textproto
test -s /outputs/delay_info.textproto
"#,
        )
    };

    let mut env = BTreeMap::new();
    env.insert("XLSYNTH_VERSION".to_string(), version.to_string());
    env.insert(
        "XLSYNTH_PLATFORM".to_string(),
        runtime.release_platform.to_string(),
    );
    env.insert("DELAY_MODEL".to_string(), delay_model.to_string());
    if let Some(top) = &ir_top {
        env.insert("IR_TOP".to_string(), top.clone());
    }

    let mounts = vec![
        DockerMount::read_only(&ir_input_path, "/inputs/input.ir")?,
        DockerMount::read_write(payload_dir, "/outputs")?,
        driver_cache_mount(store)?,
    ];

    let run_trace = run_docker_script(&runtime.docker_image, &mounts, &env, &script, action_id)?;
    commands.push(run_trace);

    Ok(ActionOutcome {
        dependencies: vec![dep],
        output_artifact,
        commands,
        details: serde_json::Value::Object(details),
        suggested_next_actions: Vec::new(),
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_driver_ir_equiv_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action_id: &str,
    lhs_ir_action_id: &str,
    rhs_ir_action_id: &str,
    top_fn_name: Option<&str>,
    version: &str,
    runtime: &DriverRuntimeSpec,
    payload_dir: &Path,
) -> Result<ActionOutcome> {
    let lhs_dep = load_dependency_of_type(store, lhs_ir_action_id, ArtifactType::IrPackageFile)?;
    let rhs_dep = load_dependency_of_type(store, rhs_ir_action_id, ArtifactType::IrPackageFile)?;
    let lhs_input_path = store.resolve_artifact_ref_path(&lhs_dep);
    let rhs_input_path = store.resolve_artifact_ref_path(&rhs_dep);
    if !lhs_input_path.exists() {
        bail!("LHS IR path does not exist: {}", lhs_input_path.display());
    }
    if !rhs_input_path.exists() {
        bail!("RHS IR path does not exist: {}", rhs_input_path.display());
    }

    let ir_top = match top_fn_name {
        Some(top) => Some(top.to_string()),
        None => infer_ir_top_function(&rhs_input_path)
            .ok()
            .or_else(|| infer_ir_top_function(&lhs_input_path).ok()),
    };

    let mut commands = Vec::new();
    ensure_driver_runtime_prepared(store, repo_root, version, runtime, &mut commands)?;

    let script = if ir_top.is_some() {
        driver_script(
            r#"
cat > /tmp/xlsynth-toolchain.toml <<'TOML'
[toolchain]
tool_path = "/tmp/xlsynth-release"
TOML
xlsynth-driver --toolchain /tmp/xlsynth-toolchain.toml ir-equiv /inputs/lhs.ir /inputs/rhs.ir --top "${IR_TOP}" --solver auto --output_json /outputs/ir_equiv.json
test -s /outputs/ir_equiv.json
"#,
        )
    } else {
        driver_script(
            r#"
cat > /tmp/xlsynth-toolchain.toml <<'TOML'
[toolchain]
tool_path = "/tmp/xlsynth-release"
TOML
xlsynth-driver --toolchain /tmp/xlsynth-toolchain.toml ir-equiv /inputs/lhs.ir /inputs/rhs.ir --solver auto --output_json /outputs/ir_equiv.json
test -s /outputs/ir_equiv.json
"#,
        )
    };

    let mut env = BTreeMap::new();
    env.insert("XLSYNTH_VERSION".to_string(), version.to_string());
    env.insert(
        "XLSYNTH_PLATFORM".to_string(),
        runtime.release_platform.to_string(),
    );
    if let Some(top) = &ir_top {
        env.insert("IR_TOP".to_string(), top.clone());
    }
    let mounts = vec![
        DockerMount::read_only(&lhs_input_path, "/inputs/lhs.ir")?,
        DockerMount::read_only(&rhs_input_path, "/inputs/rhs.ir")?,
        DockerMount::read_write(payload_dir, "/outputs")?,
        driver_cache_mount(store)?,
    ];
    let run_trace = run_docker_script(&runtime.docker_image, &mounts, &env, &script, action_id)?;
    commands.push(run_trace);

    let output_artifact = ArtifactRef {
        action_id: action_id.to_string(),
        artifact_type: ArtifactType::EquivReportFile,
        relpath: "payload/ir_equiv.json".to_string(),
    };

    Ok(ActionOutcome {
        dependencies: vec![lhs_dep, rhs_dep],
        output_artifact,
        commands,
        details: json!({
            "driver_runtime": runtime,
            "xlsynth_version": version,
            "crate_version_label": version_label("crate", &runtime.driver_version),
            "dso_version_label": version_label("dso", version),
            "ir_top": ir_top,
            "driver_subcommand": "ir-equiv",
        }),
        suggested_next_actions: Vec::new(),
    })
}

pub(crate) fn driver_supports_subcommand_cached(
    store: &ArtifactStore,
    repo_root: &Path,
    version: &str,
    runtime: &DriverRuntimeSpec,
    subcommand: &str,
) -> Result<bool> {
    let key: DriverSubcommandSupportCacheKey = (
        normalize_tag_version(version).to_string(),
        runtime.driver_version.clone(),
        runtime.release_platform.clone(),
        subcommand.to_string(),
    );
    if let Some(cache) = DRIVER_SUBCOMMAND_SUPPORT_CACHE.get()
        && let Ok(guard) = cache.lock()
        && let Some(value) = guard.get(&key)
    {
        return Ok(*value);
    }

    let supported =
        probe_driver_subcommand_support(store, repo_root, runtime, version, subcommand)?;
    let cache = DRIVER_SUBCOMMAND_SUPPORT_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()));
    if let Ok(mut guard) = cache.lock() {
        guard.insert(key, supported);
    }
    Ok(supported)
}

pub(crate) fn probe_driver_subcommand_support(
    store: &ArtifactStore,
    repo_root: &Path,
    runtime: &DriverRuntimeSpec,
    version: &str,
    subcommand: &str,
) -> Result<bool> {
    let scratch = make_temp_work_dir("driver-subcommand-probe")?;
    let outputs_dir = scratch.join("outputs");
    fs::create_dir_all(&outputs_dir).with_context(|| {
        format!(
            "creating subcommand probe outputs: {}",
            outputs_dir.display()
        )
    })?;
    let result = (|| -> Result<bool> {
        let effective_version = effective_xlsynth_version_for_runtime(repo_root, version, runtime);
        let script = driver_script(
            r#"
if xlsynth-driver --help | grep -Eq "^[[:space:]]+${DRIVER_SUBCOMMAND}[[:space:]]"; then
  printf "true\n" > /outputs/supported.txt
else
  printf "false\n" > /outputs/supported.txt
fi
"#,
        );
        let mut env = BTreeMap::new();
        env.insert("XLSYNTH_VERSION".to_string(), effective_version);
        env.insert(
            "XLSYNTH_PLATFORM".to_string(),
            runtime.release_platform.to_string(),
        );
        env.insert("DRIVER_SUBCOMMAND".to_string(), subcommand.to_string());
        let mounts = vec![
            DockerMount::read_write(&outputs_dir, "/outputs")?,
            driver_cache_mount(store)?,
        ];
        run_docker_script(
            &runtime.docker_image,
            &mounts,
            &env,
            &script,
            "driver-subcommand-probe",
        )?;
        let supported_path = outputs_dir.join("supported.txt");
        let text = fs::read_to_string(&supported_path)
            .with_context(|| format!("reading probe output: {}", supported_path.display()))?;
        Ok(text.trim().eq_ignore_ascii_case("true"))
    })();
    fs::remove_dir_all(&scratch).ok();
    result
}

pub(crate) fn driver_subcommand_help_has_token_cached(
    store: &ArtifactStore,
    repo_root: &Path,
    version: &str,
    runtime: &DriverRuntimeSpec,
    subcommand: &str,
    token: &str,
) -> Result<bool> {
    let key: DriverSubcommandHelpTokenCacheKey = (
        normalize_tag_version(version).to_string(),
        runtime.driver_version.clone(),
        runtime.release_platform.clone(),
        subcommand.to_string(),
        token.to_string(),
    );
    if let Some(cache) = DRIVER_SUBCOMMAND_HELP_TOKEN_CACHE.get()
        && let Ok(guard) = cache.lock()
        && let Some(value) = guard.get(&key)
    {
        return Ok(*value);
    }

    let supported =
        probe_driver_subcommand_help_token(store, repo_root, runtime, version, subcommand, token)?;
    let cache = DRIVER_SUBCOMMAND_HELP_TOKEN_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()));
    if let Ok(mut guard) = cache.lock() {
        guard.insert(key, supported);
    }
    Ok(supported)
}

pub(crate) fn probe_driver_subcommand_help_token(
    store: &ArtifactStore,
    repo_root: &Path,
    runtime: &DriverRuntimeSpec,
    version: &str,
    subcommand: &str,
    token: &str,
) -> Result<bool> {
    let scratch = make_temp_work_dir("driver-help-token-probe")?;
    let outputs_dir = scratch.join("outputs");
    fs::create_dir_all(&outputs_dir).with_context(|| {
        format!(
            "creating help-token probe outputs: {}",
            outputs_dir.display()
        )
    })?;
    let result = (|| -> Result<bool> {
        let effective_version = effective_xlsynth_version_for_runtime(repo_root, version, runtime);
        let script = driver_script(
            r#"
if xlsynth-driver "${DRIVER_SUBCOMMAND}" --help 2>/dev/null | grep -Fq -- "${DRIVER_HELP_TOKEN}"; then
  printf "true\n" > /outputs/supported.txt
else
  printf "false\n" > /outputs/supported.txt
fi
"#,
        );
        let mut env = BTreeMap::new();
        env.insert("XLSYNTH_VERSION".to_string(), effective_version);
        env.insert(
            "XLSYNTH_PLATFORM".to_string(),
            runtime.release_platform.to_string(),
        );
        env.insert("DRIVER_SUBCOMMAND".to_string(), subcommand.to_string());
        env.insert("DRIVER_HELP_TOKEN".to_string(), token.to_string());
        let mounts = vec![
            DockerMount::read_write(&outputs_dir, "/outputs")?,
            driver_cache_mount(store)?,
        ];
        run_docker_script(
            &runtime.docker_image,
            &mounts,
            &env,
            &script,
            "driver-help-token-probe",
        )?;
        let supported_path = outputs_dir.join("supported.txt");
        let text = fs::read_to_string(&supported_path)
            .with_context(|| format!("reading probe output: {}", supported_path.display()))?;
        Ok(text.trim().eq_ignore_ascii_case("true"))
    })();
    fs::remove_dir_all(&scratch).ok();
    result
}

fn effective_xlsynth_version_for_runtime(
    repo_root: &Path,
    requested_version: &str,
    runtime: &DriverRuntimeSpec,
) -> String {
    resolve_xlsynth_version_for_driver(repo_root, &runtime.driver_version)
        .unwrap_or_else(|_| requested_version.to_string())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_driver_ir_aig_equiv_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action_id: &str,
    ir_action_id: &str,
    aig_action_id: &str,
    top_fn_name: Option<&str>,
    version: &str,
    runtime: &DriverRuntimeSpec,
    payload_dir: &Path,
) -> Result<ActionOutcome> {
    let ir_dep = load_dependency_of_type(store, ir_action_id, ArtifactType::IrPackageFile)?;
    let aig_dep = load_dependency_of_type(store, aig_action_id, ArtifactType::AigFile)?;
    let ir_input_path = store.resolve_artifact_ref_path(&ir_dep);
    let aig_input_path = store.resolve_artifact_ref_path(&aig_dep);
    if !ir_input_path.exists() {
        bail!("IR path does not exist: {}", ir_input_path.display());
    }
    if !aig_input_path.exists() {
        bail!("AIG path does not exist: {}", aig_input_path.display());
    }

    let ir_top = match top_fn_name {
        Some(top) => Some(top.to_string()),
        None => infer_ir_top_function(&ir_input_path).ok(),
    };

    let effective_version = effective_xlsynth_version_for_runtime(repo_root, version, runtime);
    let mut commands = Vec::new();
    ensure_driver_runtime_prepared(store, repo_root, &effective_version, runtime, &mut commands)?;
    if !resolve_driver_ir_aig_equiv_supported(store, repo_root, version, runtime)? {
        bail!(
            "driver runtime does not support IR<->AIG equivalence flow (requires aig2ir+ir-equiv): crate:{} dso:{}",
            normalize_tag_version(&runtime.driver_version),
            normalize_tag_version(version)
        );
    }
    let script = if ir_top.is_some() {
        driver_script(
            r#"
cat > /tmp/xlsynth-toolchain.toml <<'TOML'
[toolchain]
tool_path = "/tmp/xlsynth-release"
TOML
xlsynth-driver aig2ir /inputs/input.aig > /outputs/input_from_aig.ir
test -s /outputs/input_from_aig.ir
xlsynth-driver --toolchain /tmp/xlsynth-toolchain.toml ir-equiv /inputs/input.ir /outputs/input_from_aig.ir --top "${IR_TOP}" --solver auto --output_json /outputs/ir_aig_equiv.json
test -s /outputs/ir_aig_equiv.json
"#,
        )
    } else {
        driver_script(
            r#"
cat > /tmp/xlsynth-toolchain.toml <<'TOML'
[toolchain]
tool_path = "/tmp/xlsynth-release"
TOML
xlsynth-driver aig2ir /inputs/input.aig > /outputs/input_from_aig.ir
test -s /outputs/input_from_aig.ir
xlsynth-driver --toolchain /tmp/xlsynth-toolchain.toml ir-equiv /inputs/input.ir /outputs/input_from_aig.ir --solver auto --output_json /outputs/ir_aig_equiv.json
test -s /outputs/ir_aig_equiv.json
"#,
        )
    };

    let mut env = BTreeMap::new();
    env.insert("XLSYNTH_VERSION".to_string(), effective_version);
    env.insert(
        "XLSYNTH_PLATFORM".to_string(),
        runtime.release_platform.to_string(),
    );
    if let Some(top) = &ir_top {
        env.insert("IR_TOP".to_string(), top.clone());
    }
    let mounts = vec![
        DockerMount::read_only(&ir_input_path, "/inputs/input.ir")?,
        DockerMount::read_only(&aig_input_path, "/inputs/input.aig")?,
        DockerMount::read_write(payload_dir, "/outputs")?,
        driver_cache_mount(store)?,
    ];
    let run_trace = run_docker_script(&runtime.docker_image, &mounts, &env, &script, action_id)?;
    commands.push(run_trace);

    let output_artifact = ArtifactRef {
        action_id: action_id.to_string(),
        artifact_type: ArtifactType::EquivReportFile,
        relpath: "payload/ir_aig_equiv.json".to_string(),
    };

    Ok(ActionOutcome {
        dependencies: vec![ir_dep, aig_dep],
        output_artifact,
        commands,
        details: json!({
            "driver_runtime": runtime,
            "xlsynth_version": version,
            "crate_version_label": version_label("crate", &runtime.driver_version),
            "dso_version_label": version_label("dso", version),
            "ir_top": ir_top,
            "driver_subcommand": "aig2ir+ir-equiv",
            "driver_ir_aig_equiv_mode": "aig2ir_then_ir_equiv",
        }),
        suggested_next_actions: Vec::new(),
    })
}

#[allow(clippy::too_many_arguments)]
fn g8r_lowering_mode_label(mode: &G8rLoweringMode) -> &'static str {
    match mode {
        G8rLoweringMode::Default => "default",
        G8rLoweringMode::FrontendNoPrepRewrite => "frontend_no_prep_rewrite",
    }
}

fn g8r_lowering_mode_extra_flags(mode: &G8rLoweringMode) -> &'static str {
    match mode {
        G8rLoweringMode::Default => "",
        // Keep folding/hash on, but disable prep-for-gatify rewrites so this acts as a
        // frontend AIG generator prior to running the canonical Yosys/ABC flow.
        G8rLoweringMode::FrontendNoPrepRewrite => {
            "--fold=true --hash=true --enable-rewrite-carry-out=false --enable-rewrite-prio-encode=false"
        }
    }
}

fn record_input_ir_structural_hash(
    store: &ArtifactStore,
    repo_root: &Path,
    ir_action_id: &str,
    ir_input_path: &Path,
    ir_top: Option<&str>,
    version: &str,
    runtime: &DriverRuntimeSpec,
    commands: &mut Vec<CommandTrace>,
    details: &mut serde_json::Map<String, serde_json::Value>,
    allow_compute_fallback: bool,
) -> Option<String> {
    match resolve_known_input_ir_structural_hash(store, ir_action_id, ir_top) {
        Ok(Some(known)) => {
            details.insert(
                INPUT_IR_FN_STRUCTURAL_HASH_DETAILS_KEY.to_string(),
                json!(known.hash.clone()),
            );
            details.insert(
                "input_ir_fn_structural_hash_source".to_string(),
                json!(known.source),
            );
            return Some(known.hash);
        }
        Ok(None) => {}
        Err(err) => {
            details.insert(
                "input_ir_fn_structural_hash_resolve_error".to_string(),
                json!(format!("{:#}", err)),
            );
        }
    }

    if !allow_compute_fallback {
        return None;
    }

    match compute_ir_fn_structural_hash(store, repo_root, ir_input_path, ir_top, version, runtime) {
        Ok((hash, trace)) => {
            commands.push(trace);
            details.insert(
                INPUT_IR_FN_STRUCTURAL_HASH_DETAILS_KEY.to_string(),
                json!(hash.clone()),
            );
            details.insert(
                "input_ir_fn_structural_hash_source".to_string(),
                json!("computed"),
            );
            Some(hash)
        }
        Err(err) => {
            details.insert(
                "input_ir_fn_structural_hash_error".to_string(),
                json!(format!("{:#}", err)),
            );
            None
        }
    }
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[allow(clippy::too_many_arguments)]
fn prepare_driver_ir_to_g8r_batch_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action: ActionSpec,
    version: &str,
    runtime: &DriverRuntimeSpec,
    fraig: bool,
    lowering_mode: &G8rLoweringMode,
    prep_commands: &[CommandTrace],
) -> Result<PreparedDriverIrToG8rBatchAction> {
    let ActionSpec::DriverIrToG8rAig {
        ir_action_id,
        top_fn_name,
        ..
    } = &action
    else {
        return Ok(PreparedDriverIrToG8rBatchAction::Fallback(action));
    };

    let action_id = compute_action_id(&action)?;
    let dep = load_dependency_of_type(store, ir_action_id, ArtifactType::IrPackageFile)?;
    let ir_input_path = store.resolve_artifact_ref_path(&dep);
    if !ir_input_path.exists() {
        bail!("input IR path does not exist: {}", ir_input_path.display());
    }

    let explicit_ir_top = top_fn_name.clone();
    let inferred_ir_top = explicit_ir_top
        .clone()
        .or_else(|| infer_ir_top_function(&ir_input_path).ok());

    let output_artifact = ArtifactRef {
        action_id: action_id.to_string(),
        artifact_type: ArtifactType::AigFile,
        relpath: "payload/result.aig".to_string(),
    };

    let use_legacy_ir2g8r_cli =
        cmp_dotted_numeric_version(&runtime.driver_version, "0.24.0") == std::cmp::Ordering::Less;
    let prepared_ir_relpath = "prep_for_gatify.ir";
    let mut suggested_next_actions = Vec::new();
    let stats_runtime = resolve_driver_runtime_for_aig_stats(repo_root, runtime)
        .unwrap_or_else(|_| runtime.clone());
    if let Ok(next) = build_suggestion(
        "Compute AIG stats with DriverAigToStats",
        ActionSpec::DriverAigToStats {
            aig_action_id: action_id.to_string(),
            version: version.to_string(),
            runtime: stats_runtime,
        },
    ) {
        suggested_next_actions.push(next);
    }
    if matches!(lowering_mode, G8rLoweringMode::FrontendNoPrepRewrite)
        && !fraig
        && let Ok(yosys_script_ref) = make_script_ref(repo_root, DEFAULT_YOSYS_FLOW_SCRIPT)
        && let Ok(next) = build_suggestion(
            "Run canonical Yosys/ABC over g8r-frontend AIG with AigToYosysAbcAig",
            ActionSpec::AigToYosysAbcAig {
                aig_action_id: action_id.to_string(),
                yosys_script_ref,
                runtime: default_yosys_runtime(),
            },
        )
    {
        suggested_next_actions.push(next);
    }

    let mut details = serde_json::Map::new();
    details.insert("driver_runtime".to_string(), json!(runtime));
    details.insert("xlsynth_version".to_string(), json!(version));
    insert_driver_version_labels(&mut details, version, runtime);
    details.insert("ir_top".to_string(), json!(inferred_ir_top.clone()));
    details.insert(
        "ir_top_explicit".to_string(),
        json!(explicit_ir_top.clone()),
    );
    details.insert("fraig".to_string(), json!(fraig));
    details.insert(
        "g8r_lowering_mode".to_string(),
        json!(g8r_lowering_mode_label(lowering_mode)),
    );
    details.insert(
        "g8r_lowering_mode_flags".to_string(),
        json!(g8r_lowering_mode_extra_flags(lowering_mode)),
    );
    details.insert("driver_subcommand".to_string(), json!("ir2g8r"));
    let mut capture_prepared_ir = false;
    if use_legacy_ir2g8r_cli {
        details.insert(
            "driver_ir2g8r_prepared_ir_supported".to_string(),
            json!(false),
        );
        details.insert(
            "driver_ir2g8r_prepared_ir_reason".to_string(),
            json!("legacy_cli_unsupported"),
        );
    } else {
        match resolve_driver_ir2gates_prepared_ir_supported(store, repo_root, version, runtime) {
            Ok(supported) => {
                capture_prepared_ir = supported;
                details.insert(
                    "driver_ir2g8r_prepared_ir_supported".to_string(),
                    json!(supported),
                );
            }
            Err(err) => {
                details.insert(
                    "driver_ir2g8r_prepared_ir_supported".to_string(),
                    json!(false),
                );
                details.insert(
                    "driver_ir2g8r_prepared_ir_probe_error".to_string(),
                    json!(format!("{:#}", err)),
                );
            }
        }
    }
    if capture_prepared_ir {
        details.insert(
            "driver_ir2g8r_prepared_ir_relpath".to_string(),
            json!(format!("payload/{prepared_ir_relpath}")),
        );
    }

    let ir_aig_equiv_runtime = resolve_driver_runtime_for_aig_stats(repo_root, runtime)
        .unwrap_or_else(|_| runtime.clone());
    if ir_aig_equiv_runtime != *runtime {
        details.insert(
            "driver_ir_aig_equiv_runtime".to_string(),
            json!(ir_aig_equiv_runtime.clone()),
        );
    }
    if !driver_ir_aig_equiv_enabled() {
        details.insert("driver_ir_aig_equiv_supported".to_string(), json!(false));
        details.insert("driver_ir_aig_equiv_quarantined".to_string(), json!(true));
    } else {
        match resolve_driver_ir_aig_equiv_supported(
            store,
            repo_root,
            version,
            &ir_aig_equiv_runtime,
        ) {
            Ok(true) => {
                details.insert("driver_ir_aig_equiv_supported".to_string(), json!(true));
                details.insert(
                    "driver_ir_aig_equiv_mode".to_string(),
                    json!("aig2ir_then_ir_equiv"),
                );
                if let Ok(next) = build_suggestion(
                    "Check optimized IR and emitted AIG equivalence with DriverIrAigEquiv",
                    ActionSpec::DriverIrAigEquiv {
                        ir_action_id: ir_action_id.to_string(),
                        aig_action_id: action_id.to_string(),
                        top_fn_name: inferred_ir_top.clone(),
                        version: version.to_string(),
                        runtime: ir_aig_equiv_runtime.clone(),
                    },
                ) {
                    suggested_next_actions.push(next);
                }
            }
            Ok(false) => {
                details.insert("driver_ir_aig_equiv_supported".to_string(), json!(false));
            }
            Err(err) => {
                details.insert(
                    "driver_ir_aig_equiv_probe_error".to_string(),
                    json!(format!("{:#}", err)),
                );
            }
        }
    }

    let mut commands = prep_commands.to_vec();
    let Some(input_ir_structural_hash) = record_input_ir_structural_hash(
        store,
        repo_root,
        ir_action_id,
        &ir_input_path,
        inferred_ir_top.as_deref(),
        version,
        runtime,
        &mut commands,
        &mut details,
        false,
    ) else {
        return Ok(PreparedDriverIrToG8rBatchAction::Fallback(action));
    };

    let staging_dir = make_staging_dir(store, &action_id)?;
    let payload_dir = staging_dir.join("payload");
    fs::create_dir_all(&payload_dir)
        .with_context(|| format!("creating payload dir: {}", payload_dir.display()))?;

    if let Some(source) =
        find_semantic_reuse_candidate(store, &input_ir_structural_hash, |provenance| {
            if provenance.action_id == action_id {
                return false;
            }
            let matches_action = matches!(
                &provenance.action,
                ActionSpec::DriverIrToG8rAig {
                    fraig: candidate_fraig,
                    lowering_mode: candidate_lowering_mode,
                    version: candidate_version,
                    runtime: candidate_runtime,
                    ..
                } if *candidate_fraig == fraig
                    && candidate_lowering_mode == lowering_mode
                    && candidate_version == version
                    && same_driver_runtime(candidate_runtime, runtime)
            );
            if !matches_action {
                return false;
            }
            if capture_prepared_ir
                && !provenance
                    .output_files
                    .iter()
                    .any(|file| file.path == prepared_ir_relpath)
            {
                return false;
            }
            provenance.details.get("ir_top").and_then(|v| v.as_str()) == inferred_ir_top.as_deref()
        })?
    {
        reuse_payload_from_provenance(store, &source, &payload_dir)?;
        details.insert(
            "semantic_cache_hit".to_string(),
            json!({
                "from_action_id": source.action_id,
                "key": "ir_fn_structural_hash",
            }),
        );
        let outcome = ActionOutcome {
            dependencies: vec![dep],
            output_artifact,
            commands,
            details: serde_json::Value::Object(details),
            suggested_next_actions,
        };
        return Ok(PreparedDriverIrToG8rBatchAction::Immediate {
            action,
            action_id,
            staging_dir,
            payload_dir,
            outcome,
        });
    }

    details.insert(
        "driver_ir2g8r_cli_mode".to_string(),
        json!(if use_legacy_ir2g8r_cli {
            "legacy_bin_out"
        } else {
            "modern_aiger_out"
        }),
    );
    if use_legacy_ir2g8r_cli {
        details.insert(
            "driver_ir2g8r_output_kind".to_string(),
            json!("g8rbin_with_stats_sidecar"),
        );
        details.insert(
            "legacy_g8r_stats_relpath".to_string(),
            json!(LEGACY_G8R_STATS_RELPATH),
        );
    }
    if !use_legacy_ir2g8r_cli && explicit_ir_top.is_some() {
        details.insert("driver_ir2g8r_passed_top".to_string(), json!(true));
    } else if use_legacy_ir2g8r_cli && explicit_ir_top.is_some() {
        details.insert("driver_ir2g8r_passed_top".to_string(), json!(false));
        details.insert(
            "driver_ir2g8r_top_ignored_reason".to_string(),
            json!("legacy_cli_unsupported"),
        );
    }

    Ok(PreparedDriverIrToG8rBatchAction::Runnable(
        DriverIrToG8rBatchRunnable {
            action,
            action_id,
            staging_dir,
            payload_dir,
            dependency: dep,
            output_artifact,
            commands,
            details,
            suggested_next_actions,
            ir_input_path,
            explicit_ir_top,
            use_legacy_ir2g8r_cli,
            capture_prepared_ir,
        },
    ))
}

#[allow(clippy::too_many_arguments)]
fn run_driver_ir_to_g8r_batch_runnable(
    store: &ArtifactStore,
    repo_root: &Path,
    version: &str,
    runtime: &DriverRuntimeSpec,
    fraig: bool,
    lowering_mode: &G8rLoweringMode,
    runnable: Vec<DriverIrToG8rBatchRunnable>,
) -> Result<Vec<BatchActionExecutionResult>> {
    if runnable.is_empty() {
        return Ok(Vec::new());
    }

    let batch_root = make_temp_work_dir("driver-ir-to-g8r-batch")?;
    let batch_result = (|| -> Result<Vec<BatchActionExecutionResult>> {
        let inputs_dir = batch_root.join("inputs");
        let outputs_dir = batch_root.join("outputs");
        fs::create_dir_all(&inputs_dir)
            .with_context(|| format!("creating batch inputs dir: {}", inputs_dir.display()))?;
        fs::create_dir_all(&outputs_dir)
            .with_context(|| format!("creating batch outputs dir: {}", outputs_dir.display()))?;

        let mut script_body = String::from(
            "cat > /tmp/xlsynth-toolchain.toml <<'TOML'\n[toolchain]\ntool_path = \"/tmp/xlsynth-release\"\nTOML\n",
        );
        for (index, member) in runnable.iter().enumerate() {
            let input_path = inputs_dir.join(format!("{index}.ir"));
            fs::copy(&member.ir_input_path, &input_path).with_context(|| {
                format!(
                    "copying batch IR input {} -> {}",
                    member.ir_input_path.display(),
                    input_path.display()
                )
            })?;
            let output_dir = outputs_dir.join(index.to_string());
            fs::create_dir_all(&output_dir)
                .with_context(|| format!("creating batch output dir: {}", output_dir.display()))?;

            let top_flag = member
                .explicit_ir_top
                .as_ref()
                .map(|top| format!(" --top {}", shell_single_quote(top)))
                .unwrap_or_default();
            let input_ref = format!("/batch/inputs/{index}.ir");
            let output_ref = format!("/batch/outputs/{index}");
            script_body.push_str(&format!("mkdir -p {output_ref}\nif "));
            if member.use_legacy_ir2g8r_cli {
                script_body.push_str(&format!(
                    "xlsynth-driver ir2g8r ${{G8R_EXTRA_FLAGS}} --fraig=\"${{FRAIG}}\" --bin-out {output_ref}/result.aig --stats-out {output_ref}/result.g8r_stats.json {input_ref} > /dev/null"
                ));
            } else if member.capture_prepared_ir {
                script_body.push_str(&format!(
                    "xlsynth-driver ir2g8r ${{G8R_EXTRA_FLAGS}} {input_ref}{top_flag} --fraig=\"${{FRAIG}}\" --aiger-out {output_ref}/result.aig > /dev/null && xlsynth-driver ir2gates ${{G8R_EXTRA_FLAGS}} {input_ref}{top_flag} --fraig=\"${{FRAIG}}\" --prepared-ir-out {output_ref}/prep_for_gatify.ir > /dev/null && test -s {output_ref}/prep_for_gatify.ir"
                ));
            } else {
                script_body.push_str(&format!(
                    "xlsynth-driver ir2g8r ${{G8R_EXTRA_FLAGS}} {input_ref}{top_flag} --fraig=\"${{FRAIG}}\" --aiger-out {output_ref}/result.aig > /dev/null"
                ));
            }
            script_body.push_str(&format!(
                "; then\n  printf 'ok\\n' > {output_ref}/status.txt\nelse\n  code=$?\n  printf 'exit=%s\\n' \"$code\" > {output_ref}/status.txt\nfi\n"
            ));
        }

        let mut env = BTreeMap::new();
        env.insert("XLSYNTH_VERSION".to_string(), version.to_string());
        env.insert(
            "XLSYNTH_PLATFORM".to_string(),
            runtime.release_platform.to_string(),
        );
        env.insert(
            "FRAIG".to_string(),
            if fraig { "true" } else { "false" }.to_string(),
        );
        env.insert(
            "G8R_EXTRA_FLAGS".to_string(),
            g8r_lowering_mode_extra_flags(lowering_mode).to_string(),
        );

        let mounts = vec![
            DockerMount::read_write(&batch_root, "/batch")?,
            driver_cache_mount(store)?,
        ];
        let run_trace = run_docker_script(
            &runtime.docker_image,
            &mounts,
            &env,
            &driver_script(&script_body),
            "driver-ir-to-g8r-batch",
        )?;

        let batch_size = runnable.len();
        let mut results = Vec::with_capacity(batch_size);
        for (index, mut member) in runnable.into_iter().enumerate() {
            let batch_output_dir = outputs_dir.join(index.to_string());
            let status_text = fs::read_to_string(batch_output_dir.join("status.txt"))
                .unwrap_or_else(|_| "missing_status".to_string());
            if status_text.trim() != "ok" {
                let error = format!(
                    "batched driver_ir_to_g8r_aig member failed (status={} action_id={})",
                    status_text.trim(),
                    member.action_id
                );
                results.push(BatchActionExecutionResult::failure(member.action_id, error));
                continue;
            }
            copy_directory_contents(&batch_output_dir, &member.payload_dir)?;
            member.commands.push(run_trace.clone());
            member.details.insert(
                "batch_execution".to_string(),
                json!({
                    "kind": "driver_ir_to_g8r_aig",
                    "batch_size": batch_size,
                    "member_index": index,
                }),
            );
            let outcome = ActionOutcome {
                dependencies: vec![member.dependency],
                output_artifact: member.output_artifact,
                commands: member.commands,
                details: serde_json::Value::Object(member.details),
                suggested_next_actions: member.suggested_next_actions,
            };
            match finish_action_execution(
                store,
                repo_root,
                member.action,
                member.action_id.clone(),
                member.staging_dir,
                member.payload_dir,
                outcome,
            ) {
                Ok((_done_id, output_artifact)) => results.push(
                    BatchActionExecutionResult::success(member.action_id, output_artifact),
                ),
                Err(err) => results.push(BatchActionExecutionResult::failure(
                    member.action_id,
                    format!("{:#}", err),
                )),
            }
        }
        Ok(results)
    })();
    fs::remove_dir_all(&batch_root).ok();
    batch_result
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_driver_ir_to_g8r_aig_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action_id: &str,
    ir_action_id: &str,
    top_fn_name: Option<&str>,
    fraig: bool,
    lowering_mode: &G8rLoweringMode,
    version: &str,
    runtime: &DriverRuntimeSpec,
    payload_dir: &Path,
) -> Result<ActionOutcome> {
    let dep = load_dependency_of_type(store, ir_action_id, ArtifactType::IrPackageFile)?;
    let ir_input_path = store.resolve_artifact_ref_path(&dep);
    if !ir_input_path.exists() {
        bail!("input IR path does not exist: {}", ir_input_path.display());
    }

    let explicit_ir_top = top_fn_name.map(|top| top.to_string());
    let inferred_ir_top = explicit_ir_top
        .clone()
        .or_else(|| infer_ir_top_function(&ir_input_path).ok());

    let mut commands = Vec::new();
    ensure_driver_runtime_prepared(store, repo_root, version, runtime, &mut commands)?;

    let output_artifact = ArtifactRef {
        action_id: action_id.to_string(),
        artifact_type: ArtifactType::AigFile,
        relpath: "payload/result.aig".to_string(),
    };

    let use_legacy_ir2g8r_cli =
        cmp_dotted_numeric_version(&runtime.driver_version, "0.24.0") == std::cmp::Ordering::Less;
    let prepared_ir_relpath = "prep_for_gatify.ir";
    let mut suggested_next_actions = Vec::new();
    let stats_runtime = resolve_driver_runtime_for_aig_stats(repo_root, runtime)
        .unwrap_or_else(|_| runtime.clone());
    if let Ok(next) = build_suggestion(
        "Compute AIG stats with DriverAigToStats",
        ActionSpec::DriverAigToStats {
            aig_action_id: action_id.to_string(),
            version: version.to_string(),
            runtime: stats_runtime,
        },
    ) {
        suggested_next_actions.push(next);
    }
    if matches!(lowering_mode, G8rLoweringMode::FrontendNoPrepRewrite)
        && !fraig
        && let Ok(yosys_script_ref) = make_script_ref(repo_root, DEFAULT_YOSYS_FLOW_SCRIPT)
        && let Ok(next) = build_suggestion(
            "Run canonical Yosys/ABC over g8r-frontend AIG with AigToYosysAbcAig",
            ActionSpec::AigToYosysAbcAig {
                aig_action_id: action_id.to_string(),
                yosys_script_ref,
                runtime: default_yosys_runtime(),
            },
        )
    {
        suggested_next_actions.push(next);
    }

    let mut details = serde_json::Map::new();
    details.insert("driver_runtime".to_string(), json!(runtime));
    details.insert("xlsynth_version".to_string(), json!(version));
    insert_driver_version_labels(&mut details, version, runtime);
    details.insert("ir_top".to_string(), json!(inferred_ir_top.clone()));
    details.insert(
        "ir_top_explicit".to_string(),
        json!(explicit_ir_top.clone()),
    );
    details.insert("fraig".to_string(), json!(fraig));
    details.insert(
        "g8r_lowering_mode".to_string(),
        json!(g8r_lowering_mode_label(lowering_mode)),
    );
    details.insert(
        "g8r_lowering_mode_flags".to_string(),
        json!(g8r_lowering_mode_extra_flags(lowering_mode)),
    );
    details.insert("driver_subcommand".to_string(), json!("ir2g8r"));
    let mut capture_prepared_ir = false;
    if use_legacy_ir2g8r_cli {
        details.insert(
            "driver_ir2g8r_prepared_ir_supported".to_string(),
            json!(false),
        );
        details.insert(
            "driver_ir2g8r_prepared_ir_reason".to_string(),
            json!("legacy_cli_unsupported"),
        );
    } else {
        match resolve_driver_ir2gates_prepared_ir_supported(store, repo_root, version, runtime) {
            Ok(supported) => {
                capture_prepared_ir = supported;
                details.insert(
                    "driver_ir2g8r_prepared_ir_supported".to_string(),
                    json!(supported),
                );
            }
            Err(err) => {
                details.insert(
                    "driver_ir2g8r_prepared_ir_supported".to_string(),
                    json!(false),
                );
                details.insert(
                    "driver_ir2g8r_prepared_ir_probe_error".to_string(),
                    json!(format!("{:#}", err)),
                );
            }
        }
    }
    if capture_prepared_ir {
        details.insert(
            "driver_ir2g8r_prepared_ir_relpath".to_string(),
            json!(format!("payload/{prepared_ir_relpath}")),
        );
    }

    let ir_aig_equiv_runtime = resolve_driver_runtime_for_aig_stats(repo_root, runtime)
        .unwrap_or_else(|_| runtime.clone());
    if ir_aig_equiv_runtime != *runtime {
        details.insert(
            "driver_ir_aig_equiv_runtime".to_string(),
            json!(ir_aig_equiv_runtime.clone()),
        );
    }
    if !driver_ir_aig_equiv_enabled() {
        details.insert("driver_ir_aig_equiv_supported".to_string(), json!(false));
        details.insert("driver_ir_aig_equiv_quarantined".to_string(), json!(true));
    } else {
        match resolve_driver_ir_aig_equiv_supported(
            store,
            repo_root,
            version,
            &ir_aig_equiv_runtime,
        ) {
            Ok(true) => {
                details.insert("driver_ir_aig_equiv_supported".to_string(), json!(true));
                details.insert(
                    "driver_ir_aig_equiv_mode".to_string(),
                    json!("aig2ir_then_ir_equiv"),
                );
                if let Ok(next) = build_suggestion(
                    "Check optimized IR and emitted AIG equivalence with DriverIrAigEquiv",
                    ActionSpec::DriverIrAigEquiv {
                        ir_action_id: ir_action_id.to_string(),
                        aig_action_id: action_id.to_string(),
                        top_fn_name: inferred_ir_top.clone(),
                        version: version.to_string(),
                        runtime: ir_aig_equiv_runtime.clone(),
                    },
                ) {
                    suggested_next_actions.push(next);
                }
            }
            Ok(false) => {
                details.insert("driver_ir_aig_equiv_supported".to_string(), json!(false));
            }
            Err(err) => {
                details.insert(
                    "driver_ir_aig_equiv_probe_error".to_string(),
                    json!(format!("{:#}", err)),
                );
            }
        }
    }

    let input_ir_structural_hash = record_input_ir_structural_hash(
        store,
        repo_root,
        ir_action_id,
        &ir_input_path,
        inferred_ir_top.as_deref(),
        version,
        runtime,
        &mut commands,
        &mut details,
        true,
    );

    if let Some(hash) = input_ir_structural_hash.as_deref() {
        let reusable = find_semantic_reuse_candidate(store, hash, |provenance| {
            if provenance.action_id == action_id {
                return false;
            }
            let matches_action = matches!(
                &provenance.action,
                ActionSpec::DriverIrToG8rAig {
                    fraig: candidate_fraig,
                    lowering_mode: candidate_lowering_mode,
                    version: candidate_version,
                    runtime: candidate_runtime,
                    ..
                } if *candidate_fraig == fraig
                    && candidate_lowering_mode == lowering_mode
                    && candidate_version == version
                    && same_driver_runtime(candidate_runtime, runtime)
            );
            if !matches_action {
                return false;
            }
            if capture_prepared_ir
                && !provenance
                    .output_files
                    .iter()
                    .any(|file| file.path == prepared_ir_relpath)
            {
                return false;
            }
            provenance.details.get("ir_top").and_then(|v| v.as_str()) == inferred_ir_top.as_deref()
        })?;
        if let Some(source) = reusable {
            reuse_payload_from_provenance(store, &source, payload_dir)?;
            details.insert(
                "semantic_cache_hit".to_string(),
                json!({
                    "from_action_id": source.action_id,
                    "key": "ir_fn_structural_hash",
                }),
            );
            return Ok(ActionOutcome {
                dependencies: vec![dep],
                output_artifact,
                commands,
                details: serde_json::Value::Object(details),
                suggested_next_actions,
            });
        }
    }

    details.insert(
        "driver_ir2g8r_cli_mode".to_string(),
        json!(if use_legacy_ir2g8r_cli {
            "legacy_bin_out"
        } else {
            "modern_aiger_out"
        }),
    );
    if use_legacy_ir2g8r_cli {
        details.insert(
            "driver_ir2g8r_output_kind".to_string(),
            json!("g8rbin_with_stats_sidecar"),
        );
        details.insert(
            "legacy_g8r_stats_relpath".to_string(),
            json!(LEGACY_G8R_STATS_RELPATH),
        );
    }

    let script = if use_legacy_ir2g8r_cli {
        driver_script(
            r#"
xlsynth-driver ir2g8r ${G8R_EXTRA_FLAGS} --fraig="${FRAIG}" --bin-out /outputs/result.aig --stats-out /outputs/result.g8r_stats.json /inputs/input.ir > /dev/null
test -s /outputs/result.g8r_stats.json
"#,
        )
    } else if explicit_ir_top.is_some() && capture_prepared_ir {
        driver_script(
            r#"
xlsynth-driver ir2g8r ${G8R_EXTRA_FLAGS} /inputs/input.ir --top "${IR_TOP}" --fraig="${FRAIG}" --aiger-out /outputs/result.aig > /dev/null
xlsynth-driver ir2gates ${G8R_EXTRA_FLAGS} /inputs/input.ir --top "${IR_TOP}" --fraig="${FRAIG}" --prepared-ir-out /outputs/prep_for_gatify.ir > /dev/null
test -s /outputs/prep_for_gatify.ir
"#,
        )
    } else if explicit_ir_top.is_some() {
        driver_script(
            r#"
xlsynth-driver ir2g8r ${G8R_EXTRA_FLAGS} /inputs/input.ir --top "${IR_TOP}" --fraig="${FRAIG}" --aiger-out /outputs/result.aig > /dev/null
"#,
        )
    } else if capture_prepared_ir {
        driver_script(
            r#"
xlsynth-driver ir2g8r ${G8R_EXTRA_FLAGS} /inputs/input.ir --fraig="${FRAIG}" --aiger-out /outputs/result.aig > /dev/null
xlsynth-driver ir2gates ${G8R_EXTRA_FLAGS} /inputs/input.ir --fraig="${FRAIG}" --prepared-ir-out /outputs/prep_for_gatify.ir > /dev/null
test -s /outputs/prep_for_gatify.ir
"#,
        )
    } else {
        driver_script(
            r#"
xlsynth-driver ir2g8r ${G8R_EXTRA_FLAGS} /inputs/input.ir --fraig="${FRAIG}" --aiger-out /outputs/result.aig > /dev/null
"#,
        )
    };

    let mut env = BTreeMap::new();
    env.insert("XLSYNTH_VERSION".to_string(), version.to_string());
    env.insert(
        "XLSYNTH_PLATFORM".to_string(),
        runtime.release_platform.to_string(),
    );
    env.insert(
        "FRAIG".to_string(),
        if fraig { "true" } else { "false" }.to_string(),
    );
    env.insert(
        "G8R_EXTRA_FLAGS".to_string(),
        g8r_lowering_mode_extra_flags(lowering_mode).to_string(),
    );
    if !use_legacy_ir2g8r_cli && explicit_ir_top.is_some() {
        details.insert("driver_ir2g8r_passed_top".to_string(), json!(true));
    } else if use_legacy_ir2g8r_cli && explicit_ir_top.is_some() {
        details.insert("driver_ir2g8r_passed_top".to_string(), json!(false));
        details.insert(
            "driver_ir2g8r_top_ignored_reason".to_string(),
            json!("legacy_cli_unsupported"),
        );
    }
    if !use_legacy_ir2g8r_cli && let Some(top) = &explicit_ir_top {
        env.insert("IR_TOP".to_string(), top.clone());
    }

    let mounts = vec![
        DockerMount::read_only(&ir_input_path, "/inputs/input.ir")?,
        DockerMount::read_write(payload_dir, "/outputs")?,
        driver_cache_mount(store)?,
    ];

    let run_trace = run_docker_script(&runtime.docker_image, &mounts, &env, &script, action_id)?;
    commands.push(run_trace);

    Ok(ActionOutcome {
        dependencies: vec![dep],
        output_artifact,
        commands,
        details: serde_json::Value::Object(details),
        suggested_next_actions,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_ir_fn_to_combinational_verilog_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action_id: &str,
    ir_action_id: &str,
    top_fn_name: Option<&str>,
    use_system_verilog: bool,
    version: &str,
    runtime: &DriverRuntimeSpec,
    payload_dir: &Path,
) -> Result<ActionOutcome> {
    let dep = load_dependency_of_type(store, ir_action_id, ArtifactType::IrPackageFile)?;
    let ir_input_path = store.resolve_artifact_ref_path(&dep);
    if !ir_input_path.exists() {
        bail!("input IR path does not exist: {}", ir_input_path.display());
    }

    let ir_top = match top_fn_name {
        Some(top) => Some(top.to_string()),
        None => infer_ir_top_function(&ir_input_path).ok(),
    };

    let mut commands = Vec::new();
    ensure_driver_runtime_prepared(store, repo_root, version, runtime, &mut commands)?;

    let output_relpath = if use_system_verilog {
        "payload/result.sv".to_string()
    } else {
        "payload/result.v".to_string()
    };

    let output_artifact = ArtifactRef {
        action_id: action_id.to_string(),
        artifact_type: ArtifactType::VerilogFile,
        relpath: output_relpath,
    };

    let mut suggested_next_actions = Vec::new();
    if let Ok(yosys_script_ref) = make_script_ref(repo_root, DEFAULT_YOSYS_FLOW_SCRIPT)
        && let Ok(next) = build_suggestion(
            "Convert combinational Verilog to AIG with ComboVerilogToYosysAbcAig",
            ActionSpec::ComboVerilogToYosysAbcAig {
                verilog_action_id: action_id.to_string(),
                verilog_top_module_name: ir_top.clone(),
                yosys_script_ref,
                runtime: default_yosys_runtime(),
            },
        )
    {
        suggested_next_actions.push(next);
    }

    let mut details = serde_json::Map::new();
    details.insert("driver_runtime".to_string(), json!(runtime));
    details.insert("xlsynth_version".to_string(), json!(version));
    insert_driver_version_labels(&mut details, version, runtime);
    details.insert("ir_top".to_string(), json!(ir_top.clone()));
    details.insert("use_system_verilog".to_string(), json!(use_system_verilog));
    details.insert("delay_model".to_string(), json!("unit"));
    details.insert("driver_subcommand".to_string(), json!("ir2combo"));

    let mut input_ir_structural_hash = None;
    match compute_ir_fn_structural_hash(
        store,
        repo_root,
        &ir_input_path,
        ir_top.as_deref(),
        version,
        runtime,
    ) {
        Ok((hash, trace)) => {
            commands.push(trace);
            details.insert(
                INPUT_IR_FN_STRUCTURAL_HASH_DETAILS_KEY.to_string(),
                json!(hash.clone()),
            );
            input_ir_structural_hash = Some(hash);
        }
        Err(err) => {
            details.insert(
                "input_ir_fn_structural_hash_error".to_string(),
                json!(format!("{:#}", err)),
            );
        }
    }

    if let Some(hash) = input_ir_structural_hash.as_deref() {
        let reusable = find_semantic_reuse_candidate(store, hash, |provenance| {
            if provenance.action_id == action_id {
                return false;
            }
            let matches_action = matches!(
                &provenance.action,
                ActionSpec::IrFnToCombinationalVerilog {
                    use_system_verilog: candidate_use_system_verilog,
                    version: candidate_version,
                    runtime: candidate_runtime,
                    ..
                } if *candidate_use_system_verilog == use_system_verilog
                    && candidate_version == version
                    && same_driver_runtime(candidate_runtime, runtime)
            );
            if !matches_action {
                return false;
            }
            provenance.details.get("ir_top").and_then(|v| v.as_str()) == ir_top.as_deref()
        })?;
        if let Some(source) = reusable {
            reuse_payload_from_provenance(store, &source, payload_dir)?;
            details.insert(
                "semantic_cache_hit".to_string(),
                json!({
                    "from_action_id": source.action_id,
                    "key": "ir_fn_structural_hash",
                }),
            );
            return Ok(ActionOutcome {
                dependencies: vec![dep],
                output_artifact,
                commands,
                details: serde_json::Value::Object(details),
                suggested_next_actions,
            });
        }
    }

    let mut docker_ir_input_path = ir_input_path.clone();
    let mut rewrite_work_dir = None;
    if let Some(top) = ir_top.as_deref() {
        let original_ir_text = fs::read_to_string(&ir_input_path)
            .with_context(|| format!("reading IR input text: {}", ir_input_path.display()))?;
        let rewritten_ir_text = rewrite_ir_package_top_function(&original_ir_text, top)?;
        let work_dir = make_temp_work_dir("ir2combo-top")?;
        let rewritten_path = work_dir.join("input.with_top.ir");
        fs::write(&rewritten_path, rewritten_ir_text).with_context(|| {
            format!(
                "writing rewritten IR input with explicit top: {}",
                rewritten_path.display()
            )
        })?;
        docker_ir_input_path = rewritten_path;
        rewrite_work_dir = Some(work_dir);
        details.insert("driver_ir2combo_top_rewritten".to_string(), json!(true));
    } else {
        details.insert("driver_ir2combo_top_rewritten".to_string(), json!(false));
    }

    let script = if ir_top.is_some() {
        driver_script(
            r#"
cat > /tmp/xlsynth-toolchain.toml <<'TOML'
[toolchain]
tool_path = "/tmp/xlsynth-release"
TOML
xlsynth-driver --toolchain /tmp/xlsynth-toolchain.toml ir2combo /inputs/input.ir --delay_model "${DELAY_MODEL}" --top "${IR_TOP}" --use_system_verilog "${USE_SYSTEM_VERILOG}" > "/outputs/result.${VERILOG_EXT}"
"#,
        )
    } else {
        driver_script(
            r#"
cat > /tmp/xlsynth-toolchain.toml <<'TOML'
[toolchain]
tool_path = "/tmp/xlsynth-release"
TOML
xlsynth-driver --toolchain /tmp/xlsynth-toolchain.toml ir2combo /inputs/input.ir --delay_model "${DELAY_MODEL}" --use_system_verilog "${USE_SYSTEM_VERILOG}" > "/outputs/result.${VERILOG_EXT}"
"#,
        )
    };

    let mut env = BTreeMap::new();
    env.insert("XLSYNTH_VERSION".to_string(), version.to_string());
    env.insert(
        "XLSYNTH_PLATFORM".to_string(),
        runtime.release_platform.to_string(),
    );
    env.insert(
        "USE_SYSTEM_VERILOG".to_string(),
        if use_system_verilog {
            "true".to_string()
        } else {
            "false".to_string()
        },
    );
    env.insert("DELAY_MODEL".to_string(), "unit".to_string());
    env.insert(
        "VERILOG_EXT".to_string(),
        if use_system_verilog {
            "sv".to_string()
        } else {
            "v".to_string()
        },
    );
    if let Some(top) = &ir_top {
        env.insert("IR_TOP".to_string(), top.clone());
    }
    let mounts = vec![
        DockerMount::read_only(&docker_ir_input_path, "/inputs/input.ir")?,
        DockerMount::read_write(payload_dir, "/outputs")?,
        driver_cache_mount(store)?,
    ];

    let run_trace =
        (|| run_docker_script(&runtime.docker_image, &mounts, &env, &script, action_id))();
    if let Some(dir) = rewrite_work_dir {
        fs::remove_dir_all(&dir).ok();
    }
    let run_trace = run_trace?;
    commands.push(run_trace);

    Ok(ActionOutcome {
        dependencies: vec![dep],
        output_artifact,
        commands,
        details: serde_json::Value::Object(details),
        suggested_next_actions,
    })
}

pub(crate) fn load_raw_bool_cone_manifest_lines(
    path: &Path,
) -> Result<Vec<RawBoolConeManifestLine>> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("reading ir-bool-cones manifest jsonl: {}", path.display()))?;
    let mut rows = Vec::new();
    for (line_no, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let row: RawBoolConeManifestLine = serde_json::from_str(trimmed).with_context(|| {
            format!(
                "parsing ir-bool-cones manifest row {} from {}",
                line_no + 1,
                path.display()
            )
        })?;
        rows.push(row);
    }
    Ok(rows)
}

pub(crate) fn extract_single_ir_fn_block_with_name(
    ir_text: &str,
    new_fn_name: &str,
) -> Result<String> {
    let lines: Vec<&str> = ir_text.lines().collect();
    let mut decl_index = None;
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("top fn ") || trimmed.starts_with("fn ") {
            decl_index = Some(i);
            break;
        }
    }
    let Some(decl_index) = decl_index else {
        bail!("unable to locate function declaration in cone IR package");
    };

    let decl_line = lines[decl_index];
    let trimmed = decl_line.trim_start();
    let leading = &decl_line[..decl_line.len() - trimmed.len()];
    let rest = if let Some(rest) = trimmed.strip_prefix("top fn ") {
        rest
    } else if let Some(rest) = trimmed.strip_prefix("fn ") {
        rest
    } else {
        bail!("unsupported function declaration line in cone IR package");
    };
    let Some(open_paren) = rest.find('(') else {
        bail!("function declaration missing `(` in cone IR package");
    };
    let decl_suffix = &rest[open_paren..];
    let new_decl = format!("{leading}fn {new_fn_name}{decl_suffix}");

    let mut block_lines = vec![new_decl];
    let mut brace_depth: i64 = decl_line.bytes().filter(|b| *b == b'{').count() as i64
        - decl_line.bytes().filter(|b| *b == b'}').count() as i64;
    let mut saw_open = decl_line.bytes().any(|b| b == b'{');
    if saw_open && brace_depth <= 0 {
        return Ok(block_lines.join("\n"));
    }

    for line in lines.iter().skip(decl_index + 1) {
        block_lines.push((*line).to_string());
        if line.bytes().any(|b| b == b'{') {
            saw_open = true;
        }
        brace_depth += line.bytes().filter(|b| *b == b'{').count() as i64;
        brace_depth -= line.bytes().filter(|b| *b == b'}').count() as i64;
        if saw_open && brace_depth <= 0 {
            return Ok(block_lines.join("\n"));
        }
    }

    bail!("unterminated function body in cone IR package")
}

pub(crate) fn rewrite_ir_package_top_function(ir_text: &str, top_fn_name: &str) -> Result<String> {
    let mut found_top = false;
    let mut rewritten_lines = Vec::new();
    for line in ir_text.lines() {
        let trimmed = line.trim_start();
        let indent_len = line.len().saturating_sub(trimmed.len());
        let indent = &line[..indent_len];

        let mut maybe_rewritten = None;
        let mut handled_decl = false;
        if let Some(rest) = trimmed.strip_prefix("top fn ") {
            handled_decl = true;
            let maybe_name = rest.find('(').map(|idx| rest[..idx].trim());
            if maybe_name == Some(top_fn_name) {
                found_top = true;
                maybe_rewritten = Some(format!("{indent}top fn {rest}"));
            } else {
                // Keep exactly one top function by demoting non-selected declarations.
                maybe_rewritten = Some(format!("{indent}fn {rest}"));
            }
        } else if let Some(rest) = trimmed.strip_prefix("fn ") {
            handled_decl = true;
            let maybe_name = rest.find('(').map(|idx| rest[..idx].trim());
            if maybe_name == Some(top_fn_name) {
                found_top = true;
                maybe_rewritten = Some(format!("{indent}top fn {rest}"));
            }
        }

        if handled_decl {
            rewritten_lines.push(maybe_rewritten.unwrap_or_else(|| line.to_string()));
        } else {
            rewritten_lines.push(line.to_string());
        }
    }

    if !found_top {
        bail!(
            "unable to mark requested IR top function as top fn: {}",
            top_fn_name
        );
    }

    Ok(rewritten_lines.join("\n"))
}

pub(crate) fn extract_ir_fn_block_by_name(ir_text: &str, ir_fn_name: &str) -> Result<String> {
    let lines: Vec<&str> = ir_text.lines().collect();
    let mut decl_index = None;
    for (i, line) in lines.iter().enumerate() {
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
        if candidate_name == ir_fn_name {
            decl_index = Some(i);
            break;
        }
    }
    let Some(decl_index) = decl_index else {
        bail!("unable to locate function declaration for `{ir_fn_name}`");
    };

    let decl_line = lines[decl_index];
    let mut block_lines = vec![decl_line.to_string()];
    let mut brace_depth: i64 = decl_line.bytes().filter(|b| *b == b'{').count() as i64
        - decl_line.bytes().filter(|b| *b == b'}').count() as i64;
    let mut saw_open = decl_line.bytes().any(|b| b == b'{');
    if saw_open && brace_depth <= 0 {
        return Ok(block_lines.join("\n"));
    }

    for line in lines.iter().skip(decl_index + 1) {
        block_lines.push((*line).to_string());
        if line.bytes().any(|b| b == b'{') {
            saw_open = true;
        }
        brace_depth += line.bytes().filter(|b| *b == b'{').count() as i64;
        brace_depth -= line.bytes().filter(|b| *b == b'}').count() as i64;
        if saw_open && brace_depth <= 0 {
            return Ok(block_lines.join("\n"));
        }
    }

    bail!("unterminated function body for `{ir_fn_name}`")
}

fn append_ir_text_ids_from_line(line: &str, ids: &mut Vec<u64>) {
    let bytes = line.as_bytes();
    let mut i = 0usize;
    while i + 3 <= bytes.len() {
        if bytes[i] == b'i' && bytes[i + 1] == b'd' && bytes[i + 2] == b'=' {
            let mut j = i + 3;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 3 {
                if let Ok(value) = line[i + 3..j].parse::<u64>() {
                    ids.push(value);
                }
                i = j;
                continue;
            }
        }
        i += 1;
    }
}

pub(crate) fn find_duplicate_ir_text_ids(ir_fn_text: &str) -> Vec<u64> {
    let mut ids = Vec::new();
    for line in ir_fn_text.lines() {
        append_ir_text_ids_from_line(line, &mut ids);
    }
    let mut seen = HashSet::new();
    let mut duplicates = BTreeSet::new();
    for id in ids {
        if !seen.insert(id) {
            duplicates.insert(id);
        }
    }
    duplicates.into_iter().collect()
}

fn max_ir_text_id(ir_text: &str) -> u64 {
    let mut ids = Vec::new();
    for line in ir_text.lines() {
        append_ir_text_ids_from_line(line, &mut ids);
    }
    ids.into_iter().max().unwrap_or(0)
}

pub(crate) fn rewrite_ir_node_name_suffixes_with_offset(
    ir_text: &str,
    offset: u64,
) -> Result<String> {
    if offset == 0 {
        return Ok(ir_text.to_string());
    }
    fn is_ident_byte(b: u8) -> bool {
        b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$' | b'\\')
    }

    let bytes = ir_text.as_bytes();
    let mut out = String::with_capacity(ir_text.len() + 64);
    let mut chunk_start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'.' {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 {
                let mut token_start = i;
                while token_start > 0 && is_ident_byte(bytes[token_start - 1]) {
                    token_start -= 1;
                }
                if token_start < i {
                    let token = &ir_text[token_start..i];
                    let token_first = token.as_bytes().first().copied().unwrap_or(b'\0');
                    let token_ok = token_first.is_ascii_alphabetic()
                        || matches!(token_first, b'_' | b'$' | b'\\');
                    if token_ok {
                        let value = ir_text[i + 1..j].parse::<u64>().with_context(|| {
                            format!(
                                "parsing node-name id suffix in IR snippet: `{}`",
                                &ir_text[token_start..j]
                            )
                        })?;
                        let shifted = value.checked_add(offset).ok_or_else(|| {
                            anyhow!(
                                "node-name id suffix offset overflow: suffix={} offset={}",
                                value,
                                offset
                            )
                        })?;
                        out.push_str(&ir_text[chunk_start..i + 1]);
                        out.push_str(&shifted.to_string());
                        chunk_start = j;
                        i = j;
                        continue;
                    }
                }
            }
        }
        i += 1;
    }
    out.push_str(&ir_text[chunk_start..]);
    Ok(out)
}

pub(crate) fn find_ir_node_name_id_suffix_mismatches(ir_fn_text: &str) -> Vec<String> {
    let mut mismatches = Vec::new();
    for line in ir_fn_text.lines() {
        let trimmed = line.trim_start();
        if !trimmed.contains(" = ") || !trimmed.contains("id=") {
            continue;
        }
        let lhs_region = if let Some(rest) = trimmed.strip_prefix("ret ") {
            rest
        } else {
            trimmed
        };
        let Some(colon_idx) = lhs_region.find(':') else {
            continue;
        };
        let lhs_name = lhs_region[..colon_idx].trim();
        let Some(dot_idx) = lhs_name.rfind('.') else {
            continue;
        };
        let Ok(name_suffix) = lhs_name[dot_idx + 1..].parse::<u64>() else {
            continue;
        };

        let mut ids = Vec::new();
        append_ir_text_ids_from_line(line, &mut ids);
        let Some(id_attr) = ids.first().copied() else {
            continue;
        };
        if id_attr != name_suffix {
            mismatches.push(format!(
                "{} has name-suffix {} but id attribute {}",
                lhs_name, name_suffix, id_attr
            ));
        }
    }
    mismatches
}

pub(crate) fn rewrite_ir_text_ids_with_offset(ir_text: &str, offset: u64) -> Result<String> {
    if offset == 0 {
        return Ok(ir_text.to_string());
    }
    let bytes = ir_text.as_bytes();
    let mut out = String::with_capacity(ir_text.len() + 64);
    let mut chunk_start = 0usize;
    let mut i = 0usize;
    while i + 3 <= bytes.len() {
        if bytes[i] == b'i' && bytes[i + 1] == b'd' && bytes[i + 2] == b'=' {
            let mut j = i + 3;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 3 {
                let value = ir_text[i + 3..j].parse::<u64>().with_context(|| {
                    format!("parsing text id in IR snippet: `{}`", &ir_text[i..j])
                })?;
                let shifted = value.checked_add(offset).ok_or_else(|| {
                    anyhow!(
                        "text-id offset overflow while rewriting IR ids: id={} offset={}",
                        value,
                        offset
                    )
                })?;
                out.push_str(&ir_text[chunk_start..i + 3]);
                out.push_str(&shifted.to_string());
                chunk_start = j;
                i = j;
                continue;
            }
        }
        i += 1;
    }
    out.push_str(&ir_text[chunk_start..]);
    Ok(out)
}

pub(crate) fn validate_k_bool_cone_package_text_ids(
    package_text: &str,
    entries: &[KBoolConeCorpusEntry],
) -> Result<()> {
    let package_duplicate_ids = find_duplicate_ir_text_ids(package_text);
    if !package_duplicate_ids.is_empty() {
        let sample: Vec<u64> = package_duplicate_ids.into_iter().take(16).collect();
        bail!(
            "k-bool-cone IR package has duplicate text ids at package scope; sample ids: {:?}",
            sample
        );
    }

    let mut sample_failures = Vec::new();
    let mut total_failed = 0usize;
    let mut mismatch_failures = Vec::new();
    let mut total_mismatch_failed = 0usize;
    for entry in entries {
        let fn_block =
            extract_ir_fn_block_by_name(package_text, &entry.fn_name).with_context(|| {
                format!(
                    "extracting function `{}` for text-id validation",
                    entry.fn_name
                )
            })?;
        let duplicate_ids = find_duplicate_ir_text_ids(&fn_block);
        if !duplicate_ids.is_empty() {
            total_failed = total_failed.saturating_add(1);
            if sample_failures.len() < 8 {
                sample_failures.push(format!("{}={:?}", entry.fn_name, duplicate_ids));
            }
        }
        let mismatches = find_ir_node_name_id_suffix_mismatches(&fn_block);
        if !mismatches.is_empty() {
            total_mismatch_failed = total_mismatch_failed.saturating_add(1);
            if mismatch_failures.len() < 8 {
                mismatch_failures.push(format!("{}={}", entry.fn_name, mismatches.join("; ")));
            }
        }
    }
    if total_failed > 0 {
        let sample = sample_failures.join(", ");
        bail!(
            "k-bool-cone IR package failed text-id validation in {} function(s); sample: {}",
            total_failed,
            sample
        );
    }
    if total_mismatch_failed > 0 {
        let sample = mismatch_failures.join(", ");
        bail!(
            "k-bool-cone IR package failed node-name/id validation in {} function(s); sample: {}",
            total_mismatch_failed,
            sample
        );
    }
    Ok(())
}

pub(crate) fn build_k_bool_cone_corpus_outputs(
    raw_output_dir: &Path,
    output_ir_path: &Path,
    output_manifest_path: &Path,
    source_ir_action_id: &str,
    source_ir_top: &str,
    k: u32,
    max_ir_ops: Option<u64>,
) -> Result<KBoolConeCorpusManifest> {
    let manifest_jsonl_path = raw_output_dir.join("manifest.jsonl");
    let raw_rows = load_raw_bool_cone_manifest_lines(&manifest_jsonl_path)?;

    #[derive(Debug, Clone)]
    struct TempEntry {
        source_index: usize,
        structural_hash: String,
        fn_name: String,
        fn_block: String,
        ir_fn_signature: Option<String>,
        ir_op_count: Option<u64>,
        sink_node_index: u64,
        frontier_leaf_indices: Vec<u64>,
        frontier_non_literal_count: u64,
        included_node_count: u64,
    }

    let mut by_hash: BTreeMap<String, TempEntry> = BTreeMap::new();
    for (index, row) in raw_rows.iter().enumerate() {
        let Some(structural_hash) = normalized_structural_hash(&row.sha256) else {
            bail!(
                "invalid ir-bool-cones manifest sha256 at row {}: {}",
                index + 1,
                row.sha256
            );
        };
        if by_hash.contains_key(&structural_hash) {
            continue;
        }
        let cone_ir_path = raw_output_dir.join(format!("{structural_hash}.ir"));
        if !cone_ir_path.exists() {
            bail!(
                "manifest referenced missing cone IR file: {}",
                cone_ir_path.display()
            );
        }
        let cone_ir_text = fs::read_to_string(&cone_ir_path)
            .with_context(|| format!("reading cone IR file: {}", cone_ir_path.display()))?;
        let fn_name = format!("__k{}_cone_{}", k, &structural_hash[..16]);
        let fn_block =
            extract_single_ir_fn_block_with_name(&cone_ir_text, &fn_name).with_context(|| {
                format!(
                    "extracting function block from cone IR: {}",
                    cone_ir_path.display()
                )
            })?;
        let ir_fn_signature = parse_ir_function_signature(&fn_block, &fn_name);
        let ir_op_count = parse_ir_fn_node_count_by_name(&fn_block, &fn_name);
        let duplicate_ids = find_duplicate_ir_text_ids(&fn_block);
        if !duplicate_ids.is_empty() {
            bail!(
                "invalid ir-bool-cones output for `{}` from {}: duplicate text ids {:?}",
                fn_name,
                cone_ir_path.display(),
                duplicate_ids
            );
        }
        by_hash.insert(
            structural_hash.clone(),
            TempEntry {
                source_index: index,
                structural_hash,
                fn_name,
                fn_block,
                ir_fn_signature,
                ir_op_count,
                sink_node_index: row.sink_node_index,
                frontier_leaf_indices: row.frontier_leaf_indices.clone(),
                frontier_non_literal_count: row.frontier_non_literal_count,
                included_node_count: row.included_node_count,
            },
        );
    }

    let mut package_text = format!("package k_bool_cones_k{k}\n\n");
    let mut emitted_entries: Vec<TempEntry> = Vec::new();
    let mut filtered_out_ir_op_count = 0_usize;
    let mut next_id_offset = 0u64;
    for entry in by_hash.values() {
        if let Some(max_ops) = max_ir_ops {
            match entry.ir_op_count {
                Some(op_count) if op_count <= max_ops => {}
                _ => {
                    filtered_out_ir_op_count = filtered_out_ir_op_count.saturating_add(1);
                    continue;
                }
            }
        }
        let rewritten_fn_block = rewrite_ir_text_ids_with_offset(&entry.fn_block, next_id_offset)
            .and_then(|text| rewrite_ir_node_name_suffixes_with_offset(&text, next_id_offset))
            .with_context(|| {
                format!(
                    "rewriting text ids and node-name suffixes with offset {} for function `{}`",
                    next_id_offset, entry.fn_name
                )
            })?;
        package_text.push_str(&rewritten_fn_block);
        package_text.push_str("\n\n");
        let max_id = max_ir_text_id(&entry.fn_block);
        next_id_offset = next_id_offset.saturating_add(max_id.saturating_add(1));
        emitted_entries.push(entry.clone());
    }
    fs::write(output_ir_path, &package_text).with_context(|| {
        format!(
            "writing merged k-bool cone IR package: {}",
            output_ir_path.display()
        )
    })?;

    let output_ir_relpath = format!(
        "payload/{}",
        output_ir_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("k_bool_cones.ir")
    );

    let mut entries = Vec::new();
    for entry in emitted_entries {
        entries.push(KBoolConeCorpusEntry {
            structural_hash: entry.structural_hash,
            fn_name: entry.fn_name,
            source_index: entry.source_index,
            sink_node_index: entry.sink_node_index,
            frontier_leaf_indices: entry.frontier_leaf_indices,
            frontier_non_literal_count: entry.frontier_non_literal_count,
            included_node_count: entry.included_node_count,
            ir_fn_signature: entry.ir_fn_signature,
            ir_op_count: entry.ir_op_count,
        });
    }
    entries.sort_by(|a, b| {
        a.structural_hash
            .cmp(&b.structural_hash)
            .then(a.fn_name.cmp(&b.fn_name))
    });

    let manifest = KBoolConeCorpusManifest {
        schema_version: 1,
        source_ir_action_id: source_ir_action_id.to_string(),
        source_ir_top: source_ir_top.to_string(),
        k,
        max_ir_ops,
        total_manifest_rows: raw_rows.len(),
        emitted_cone_files: entries.len(),
        deduped_unique_cones: entries.len(),
        filtered_out_ir_op_count,
        output_ir_relpath,
        entries,
    };
    write_json_pretty_file(output_manifest_path, &manifest)?;
    Ok(manifest)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_ir_fn_to_k_bool_cone_corpus_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action_id: &str,
    ir_action_id: &str,
    top_fn_name: Option<&str>,
    k: u32,
    max_ir_ops: Option<u64>,
    version: &str,
    runtime: &DriverRuntimeSpec,
    payload_dir: &Path,
) -> Result<ActionOutcome> {
    if k == 0 {
        bail!("k must be > 0 for IrFnToKBoolConeCorpus");
    }
    let max_ir_ops = max_ir_ops.or_else(|| default_k_bool_cone_max_ir_ops_for_k(k));
    let dep = load_dependency_of_type(store, ir_action_id, ArtifactType::IrPackageFile)?;
    let ir_input_path = store.resolve_artifact_ref_path(&dep);
    if !ir_input_path.exists() {
        bail!("input IR path does not exist: {}", ir_input_path.display());
    }

    let ir_top = match top_fn_name {
        Some(top) => top.to_string(),
        None => infer_ir_top_function(&ir_input_path)?,
    };

    let output_ir_name = format!("k_bool_cones_k{k}.ir");
    let output_manifest_name = format!("k_bool_cones_k{k}_manifest.json");
    let output_ir_relpath = format!("payload/{output_ir_name}");
    let output_manifest_relpath = format!("payload/{output_manifest_name}");
    let output_ir_path = payload_dir.join(&output_ir_name);
    let output_manifest_path = payload_dir.join(&output_manifest_name);

    let output_artifact = ArtifactRef {
        action_id: action_id.to_string(),
        artifact_type: ArtifactType::IrPackageFile,
        relpath: output_ir_relpath.clone(),
    };

    let mut commands = Vec::new();
    ensure_driver_runtime_prepared(store, repo_root, version, runtime, &mut commands)?;

    let mut details = serde_json::Map::new();
    details.insert("driver_runtime".to_string(), json!(runtime));
    details.insert("xlsynth_version".to_string(), json!(version));
    insert_driver_version_labels(&mut details, version, runtime);
    details.insert("ir_top".to_string(), json!(ir_top.clone()));
    details.insert("k".to_string(), json!(k));
    details.insert("max_ir_ops".to_string(), json!(max_ir_ops));
    details.insert("driver_subcommand".to_string(), json!("ir-bool-cones"));
    details.insert(
        "output_ir_relpath".to_string(),
        json!(output_ir_relpath.clone()),
    );
    details.insert(
        "output_manifest_relpath".to_string(),
        json!(output_manifest_relpath.clone()),
    );

    let mut input_ir_structural_hash = None;
    match compute_ir_fn_structural_hash(
        store,
        repo_root,
        &ir_input_path,
        Some(&ir_top),
        version,
        runtime,
    ) {
        Ok((hash, trace)) => {
            commands.push(trace);
            details.insert(
                INPUT_IR_FN_STRUCTURAL_HASH_DETAILS_KEY.to_string(),
                json!(hash.clone()),
            );
            input_ir_structural_hash = Some(hash);
        }
        Err(err) => {
            details.insert(
                "input_ir_fn_structural_hash_error".to_string(),
                json!(format!("{:#}", err)),
            );
        }
    }

    if let Some(hash) = input_ir_structural_hash.as_deref() {
        let reusable = find_semantic_reuse_candidate(store, hash, |provenance| {
            if provenance.action_id == action_id {
                return false;
            }
            let matches_action = matches!(
                &provenance.action,
                ActionSpec::IrFnToKBoolConeCorpus {
                    k: candidate_k,
                    max_ir_ops: candidate_max_ir_ops,
                    version: candidate_version,
                    runtime: candidate_runtime,
                    ..
                } if *candidate_k == k
                    && *candidate_max_ir_ops == max_ir_ops
                    && candidate_version == version
                    && same_driver_runtime(candidate_runtime, runtime)
            );
            if !matches_action {
                return false;
            }
            provenance.details.get("ir_top").and_then(|v| v.as_str()) == Some(ir_top.as_str())
        })?;
        if let Some(source) = reusable {
            reuse_payload_from_provenance(store, &source, payload_dir)?;
            let mut suggested_next_actions = Vec::new();
            match fs::read_to_string(&output_manifest_path)
                .ok()
                .and_then(|text| serde_json::from_str::<KBoolConeCorpusManifest>(&text).ok())
            {
                Some(manifest) => {
                    let output_ir_text =
                        fs::read_to_string(&output_ir_path).with_context(|| {
                            format!(
                                "reading merged k-bool-cone IR package from semantic cache hit: {}",
                                output_ir_path.display()
                            )
                        })?;
                    validate_k_bool_cone_package_text_ids(&output_ir_text, &manifest.entries)
                        .with_context(|| {
                            format!(
                                "validating semantic cache hit payload from action {}",
                                source.action_id
                            )
                        })?;
                    suggested_next_actions = build_k_bool_cone_corpus_suggested_actions(
                        action_id, &manifest, version, runtime,
                    );
                    details.insert(
                        "suggested_actions".to_string(),
                        json!(suggested_next_actions.len()),
                    );
                    details.insert("text_id_validation".to_string(), json!("ok"));
                }
                None => {
                    details.insert(
                        "suggested_actions_error".to_string(),
                        json!(format!(
                            "unable to read/parse manifest after semantic cache hit: {}",
                            output_manifest_path.display()
                        )),
                    );
                }
            }
            details.insert(
                "semantic_cache_hit".to_string(),
                json!({
                    "from_action_id": source.action_id,
                    "key": "ir_fn_structural_hash",
                }),
            );
            return Ok(ActionOutcome {
                dependencies: vec![dep],
                output_artifact,
                commands,
                details: serde_json::Value::Object(details),
                suggested_next_actions,
            });
        }
    }

    let raw_output_dir = payload_dir.join("k_bool_cones_raw");
    fs::create_dir_all(&raw_output_dir).with_context(|| {
        format!(
            "creating raw k-bool-cones output directory: {}",
            raw_output_dir.display()
        )
    })?;
    let script = driver_script(
        r#"
cat > /tmp/xlsynth-toolchain.toml <<'TOML'
[toolchain]
tool_path = "/tmp/xlsynth-release"
TOML
mkdir -p /outputs/raw
xlsynth-driver --toolchain /tmp/xlsynth-toolchain.toml ir-bool-cones \
  --k "${K}" \
  --top "${IR_TOP}" \
  --output_dir /outputs/raw \
  --manifest_jsonl /outputs/raw/manifest.jsonl \
  /inputs/input.ir
test -f /outputs/raw/manifest.jsonl
"#,
    );
    let mut env = BTreeMap::new();
    env.insert("XLSYNTH_VERSION".to_string(), version.to_string());
    env.insert(
        "XLSYNTH_PLATFORM".to_string(),
        runtime.release_platform.to_string(),
    );
    env.insert("IR_TOP".to_string(), ir_top.clone());
    env.insert("K".to_string(), k.to_string());

    let mounts = vec![
        DockerMount::read_only(&ir_input_path, "/inputs/input.ir")?,
        DockerMount::read_write(&raw_output_dir, "/outputs/raw")?,
        driver_cache_mount(store)?,
    ];
    let run_trace = run_docker_script(&runtime.docker_image, &mounts, &env, &script, action_id)?;
    commands.push(run_trace);

    let manifest = build_k_bool_cone_corpus_outputs(
        &raw_output_dir,
        &output_ir_path,
        &output_manifest_path,
        ir_action_id,
        &ir_top,
        k,
        max_ir_ops,
    )?;
    let output_ir_text = fs::read_to_string(&output_ir_path).with_context(|| {
        format!(
            "reading merged k-bool-cone IR package: {}",
            output_ir_path.display()
        )
    })?;
    validate_k_bool_cone_package_text_ids(&output_ir_text, &manifest.entries).with_context(
        || {
            format!(
                "validating merged k-bool-cone IR package: {}",
                output_ir_path.display()
            )
        },
    )?;
    details.insert(
        "total_manifest_rows".to_string(),
        json!(manifest.total_manifest_rows),
    );
    details.insert(
        "emitted_cone_files".to_string(),
        json!(manifest.emitted_cone_files),
    );
    details.insert(
        "deduped_unique_cones".to_string(),
        json!(manifest.deduped_unique_cones),
    );
    details.insert(
        "filtered_out_ir_op_count".to_string(),
        json!(manifest.filtered_out_ir_op_count),
    );
    details.insert(
        "k_bool_cone_entry_count".to_string(),
        json!(manifest.entries.len()),
    );
    details.insert("text_id_validation".to_string(), json!("ok"));
    let suggested_next_actions =
        build_k_bool_cone_corpus_suggested_actions(action_id, &manifest, version, runtime);
    details.insert(
        "suggested_actions".to_string(),
        json!(suggested_next_actions.len()),
    );

    fs::remove_dir_all(&raw_output_dir).ok();

    Ok(ActionOutcome {
        dependencies: vec![dep],
        output_artifact,
        commands,
        details: serde_json::Value::Object(details),
        suggested_next_actions,
    })
}

pub(crate) fn build_k_bool_cone_corpus_suggested_actions(
    action_id: &str,
    manifest: &KBoolConeCorpusManifest,
    version: &str,
    runtime: &DriverRuntimeSpec,
) -> Vec<SuggestedAction> {
    build_k_bool_cone_corpus_suggested_actions_for_entries(
        action_id,
        &manifest.entries,
        version,
        runtime,
    )
}

pub(crate) fn build_k_bool_cone_corpus_suggested_actions_for_entries(
    action_id: &str,
    entries: &[KBoolConeCorpusEntry],
    version: &str,
    runtime: &DriverRuntimeSpec,
) -> Vec<SuggestedAction> {
    let mut suggested = Vec::new();
    let mut seen_action_ids = HashSet::new();
    for entry in entries {
        let top = Some(entry.fn_name.clone());
        let g8r = ActionSpec::DriverIrToG8rAig {
            ir_action_id: action_id.to_string(),
            top_fn_name: top.clone(),
            fraig: false,
            lowering_mode: G8rLoweringMode::Default,
            version: version.to_string(),
            runtime: runtime.clone(),
        };
        if let Ok(next) = build_suggestion(
            "Convert k-bool-cone IR function to AIG with DriverIrToG8rAig (fraig=false)",
            g8r,
        ) && seen_action_ids.insert(next.action_id.clone())
        {
            suggested.push(next);
        }
        let frontend_g8r = ActionSpec::DriverIrToG8rAig {
            ir_action_id: action_id.to_string(),
            top_fn_name: top.clone(),
            fraig: false,
            lowering_mode: G8rLoweringMode::FrontendNoPrepRewrite,
            version: version.to_string(),
            runtime: runtime.clone(),
        };
        if let Ok(next) = build_suggestion(
            "Convert k-bool-cone IR function to AIG with DriverIrToG8rAig (frontend_no_prep_rewrite)",
            frontend_g8r,
        ) && seen_action_ids.insert(next.action_id.clone())
        {
            suggested.push(next);
        }

        let combo = ActionSpec::IrFnToCombinationalVerilog {
            ir_action_id: action_id.to_string(),
            top_fn_name: top,
            use_system_verilog: false,
            version: version.to_string(),
            runtime: runtime.clone(),
        };
        if let Ok(next) = build_suggestion(
            "Convert k-bool-cone IR function to combinational Verilog with IrFnToCombinationalVerilog",
            combo,
        ) && seen_action_ids.insert(next.action_id.clone())
        {
            suggested.push(next);
        }
    }
    suggested
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_combo_verilog_to_yosys_abc_aig_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action_id: &str,
    verilog_action_id: &str,
    verilog_top_module_name: Option<&str>,
    yosys_script_ref: &ScriptRef,
    runtime: &YosysRuntimeSpec,
    payload_dir: &Path,
) -> Result<ActionOutcome> {
    let dep = load_dependency_of_type(store, verilog_action_id, ArtifactType::VerilogFile)?;
    let verilog_input_path = store.resolve_artifact_ref_path(&dep);
    if !verilog_input_path.exists() {
        bail!(
            "input Verilog path does not exist: {}",
            verilog_input_path.display()
        );
    }

    let yosys_script_path = resolve_script_ref(repo_root, yosys_script_ref)?;

    let mut commands = Vec::new();
    let build_trace = ensure_yosys_image(repo_root, runtime)?;
    if let Some(trace) = build_trace {
        commands.push(trace);
    }

    let script = r#"
set -euo pipefail
{
  echo "read_verilog ${READ_VERILOG_FLAGS} /inputs/input.v"
  if [ -n "${VERILOG_TOP_MODULE_NAME:-}" ]; then
    echo "hierarchy -check -top ${VERILOG_TOP_MODULE_NAME}"
  fi
  echo "script /inputs/flow.ys"
  echo "write_aiger /outputs/result.aig"
} > /tmp/run.ys
yosys -s /tmp/run.ys
test -s /outputs/result.aig
"#;

    let mut env = BTreeMap::new();
    env.insert(
        "READ_VERILOG_FLAGS".to_string(),
        if dep.relpath.ends_with(".sv") {
            "-sv".to_string()
        } else {
            "".to_string()
        },
    );
    if let Some(top) = verilog_top_module_name {
        env.insert("VERILOG_TOP_MODULE_NAME".to_string(), top.to_string());
    }

    let mounts = vec![
        DockerMount::read_only(&verilog_input_path, "/inputs/input.v")?,
        DockerMount::read_only(&yosys_script_path, "/inputs/flow.ys")?,
        DockerMount::read_write(payload_dir, "/outputs")?,
    ];

    let run_trace = run_docker_script(&runtime.docker_image, &mounts, &env, script, action_id)?;
    commands.push(run_trace);

    let output_artifact = ArtifactRef {
        action_id: action_id.to_string(),
        artifact_type: ArtifactType::AigFile,
        relpath: "payload/result.aig".to_string(),
    };

    let mut suggested_next_actions = Vec::new();
    if let Ok(Some((xlsynth_version, driver_runtime))) =
        infer_driver_context_from_verilog_action(store, verilog_action_id)
    {
        let stats_runtime = resolve_driver_runtime_for_aig_stats(repo_root, &driver_runtime)
            .unwrap_or(driver_runtime.clone());
        if let Ok(next) = build_suggestion(
            "Compute AIG stats with DriverAigToStats",
            ActionSpec::DriverAigToStats {
                aig_action_id: action_id.to_string(),
                version: xlsynth_version,
                runtime: stats_runtime,
            },
        ) {
            suggested_next_actions.push(next);
        }
    }

    Ok(ActionOutcome {
        dependencies: vec![dep],
        output_artifact,
        commands,
        details: json!({
            "yosys_runtime": runtime,
            "verilog_top_module_name": verilog_top_module_name,
            "yosys_script_ref": yosys_script_ref,
            "flow": "yosys_script_to_aiger",
        }),
        suggested_next_actions,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_aig_to_yosys_abc_aig_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action_id: &str,
    aig_action_id: &str,
    yosys_script_ref: &ScriptRef,
    runtime: &YosysRuntimeSpec,
    payload_dir: &Path,
) -> Result<ActionOutcome> {
    let dep = load_dependency_of_type(store, aig_action_id, ArtifactType::AigFile)?;
    let aig_input_path = store.resolve_artifact_ref_path(&dep);
    if !aig_input_path.exists() {
        bail!(
            "input AIG path does not exist: {}",
            aig_input_path.display()
        );
    }
    ensure_aiger_input_header(&aig_input_path)?;

    let yosys_script_path = resolve_script_ref(repo_root, yosys_script_ref)?;

    let mut commands = Vec::new();
    let build_trace = ensure_yosys_image(repo_root, runtime)?;
    if let Some(trace) = build_trace {
        commands.push(trace);
    }

    let script = r#"
set -euo pipefail
{
  echo "read_aiger /inputs/input.aig"
  echo "script /inputs/flow.ys"
  echo "write_aiger /outputs/result.aig"
} > /tmp/run.ys
yosys -s /tmp/run.ys
test -s /outputs/result.aig
"#;

    let mounts = vec![
        DockerMount::read_only(&aig_input_path, "/inputs/input.aig")?,
        DockerMount::read_only(&yosys_script_path, "/inputs/flow.ys")?,
        DockerMount::read_write(payload_dir, "/outputs")?,
    ];
    let env = BTreeMap::new();
    let run_trace = run_docker_script(&runtime.docker_image, &mounts, &env, script, action_id)?;
    commands.push(run_trace);

    let output_artifact = ArtifactRef {
        action_id: action_id.to_string(),
        artifact_type: ArtifactType::AigFile,
        relpath: "payload/result.aig".to_string(),
    };

    let mut suggested_next_actions = Vec::new();
    if let Ok(Some((xlsynth_version, driver_runtime))) =
        infer_driver_context_from_aig_action(store, aig_action_id)
    {
        let stats_runtime = resolve_driver_runtime_for_aig_stats(repo_root, &driver_runtime)
            .unwrap_or(driver_runtime.clone());
        if let Ok(next) = build_suggestion(
            "Compute AIG stats with DriverAigToStats",
            ActionSpec::DriverAigToStats {
                aig_action_id: action_id.to_string(),
                version: xlsynth_version,
                runtime: stats_runtime,
            },
        ) {
            suggested_next_actions.push(next);
        }
    }

    Ok(ActionOutcome {
        dependencies: vec![dep],
        output_artifact,
        commands,
        details: json!({
            "yosys_runtime": runtime,
            "yosys_script_ref": yosys_script_ref,
            "flow": "aiger_script_to_aiger",
        }),
        suggested_next_actions,
    })
}

pub(crate) fn run_driver_aig_to_stats_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action_id: &str,
    aig_action_id: &str,
    version: &str,
    runtime: &DriverRuntimeSpec,
    payload_dir: &Path,
) -> Result<ActionOutcome> {
    let dep = load_dependency_of_type(store, aig_action_id, ArtifactType::AigFile)?;
    let aig_input_path = store.resolve_artifact_ref_path(&dep);
    if !aig_input_path.exists() {
        bail!(
            "input AIG path does not exist: {}",
            aig_input_path.display()
        );
    }

    let mut commands = Vec::new();
    let mut dependencies = vec![dep];
    let runtime_xlsynth_version =
        resolve_xlsynth_version_for_driver(repo_root, &runtime.driver_version)?;
    let mut details = serde_json::Map::new();
    details.insert("driver_runtime".to_string(), json!(runtime));
    details.insert("xlsynth_version".to_string(), json!(version));
    details.insert(
        "runtime_xlsynth_version".to_string(),
        json!(runtime_xlsynth_version),
    );
    details.insert(
        "stats_runtime_crate_version_label".to_string(),
        json!(version_label("crate", &runtime.driver_version)),
    );
    details.insert(
        "stats_runtime_dso_version_label".to_string(),
        json!(version_label("dso", &runtime_xlsynth_version)),
    );
    details.insert(
        "dso_version_label".to_string(),
        json!(version_label("dso", version)),
    );
    details.insert("driver_subcommand".to_string(), json!("aig-stats"));
    if let Some(legacy_ctx) = infer_legacy_g8r_stats_context(store, aig_action_id)? {
        details.insert(
            "legacy_g8r_source".to_string(),
            json!({
                "ir_action_id": legacy_ctx.ir_action_id,
                "fraig": legacy_ctx.fraig,
                "source_runtime": legacy_ctx.runtime,
                "source_xlsynth_version": legacy_ctx.version,
            }),
        );
        let legacy_sidecar = legacy_ctx.stats_relpath.as_deref().map(|relpath| {
            let sidecar = store
                .materialize_action_dir(aig_action_id)
                .map(|dir| dir.join(relpath))
                .unwrap_or_else(|_| store.action_dir(aig_action_id).join(relpath));
            (relpath.to_string(), sidecar)
        });
        if let Some((relpath, sidecar_path)) = legacy_sidecar.as_ref() {
            if sidecar_path.exists() {
                normalize_legacy_g8r_stats_payload(sidecar_path, &payload_dir.join("stats.json"))?;
                details.insert(
                    "aig_stats_mode".to_string(),
                    json!("legacy_g8r_stats_sidecar"),
                );
                details.insert("legacy_g8r_stats_relpath".to_string(), json!(relpath));
            } else {
                details.insert(
                    "legacy_g8r_stats_sidecar_missing".to_string(),
                    json!(relpath),
                );
            }
        }
        if details
            .get("aig_stats_mode")
            .and_then(|v| v.as_str())
            .is_none()
        {
            ensure_driver_runtime_prepared(
                store,
                repo_root,
                &legacy_ctx.version,
                &legacy_ctx.runtime,
                &mut commands,
            )?;
            let ir_dep = load_dependency_of_type(
                store,
                &legacy_ctx.ir_action_id,
                ArtifactType::IrPackageFile,
            )?;
            let ir_input_path = store.resolve_artifact_ref_path(&ir_dep);
            if !ir_input_path.exists() {
                bail!("input IR path does not exist: {}", ir_input_path.display());
            }
            let script = driver_script(
                r#"
xlsynth-driver ir2g8r --fraig="${FRAIG}" --stats-out /outputs/stats.raw.json /inputs/input.ir > /dev/null
test -s /outputs/stats.raw.json
"#,
            );
            let mut env = BTreeMap::new();
            env.insert("XLSYNTH_VERSION".to_string(), legacy_ctx.version.clone());
            env.insert(
                "XLSYNTH_PLATFORM".to_string(),
                legacy_ctx.runtime.release_platform.to_string(),
            );
            env.insert("FRAIG".to_string(), legacy_ctx.fraig.to_string());
            let mounts = vec![
                DockerMount::read_only(&ir_input_path, "/inputs/input.ir")?,
                DockerMount::read_write(payload_dir, "/outputs")?,
                driver_cache_mount(store)?,
            ];
            let run_trace = run_docker_script(
                &legacy_ctx.runtime.docker_image,
                &mounts,
                &env,
                &script,
                action_id,
            )?;
            commands.push(run_trace);
            dependencies.push(ir_dep);
            normalize_legacy_g8r_stats_payload(
                &payload_dir.join("stats.raw.json"),
                &payload_dir.join("stats.json"),
            )?;
            details.insert(
                "aig_stats_mode".to_string(),
                json!("legacy_ir2g8r_stats_out_recompute"),
            );
        }
    } else {
        ensure_aiger_input_header(&aig_input_path)?;
        if let Some(trace) = ensure_driver_image(repo_root, runtime)? {
            commands.push(trace);
        }
        if let Some(trace) = ensure_driver_release_cache(
            store,
            repo_root,
            &runtime_xlsynth_version,
            &runtime.release_platform,
        )? {
            commands.push(trace);
        }
        let script = driver_script(
            r#"
xlsynth-driver aig-stats /inputs/input.aig --output_json /outputs/stats.json
test -s /outputs/stats.json
"#,
        );
        let mut env = BTreeMap::new();
        env.insert(
            "XLSYNTH_VERSION".to_string(),
            runtime_xlsynth_version.clone(),
        );
        env.insert(
            "XLSYNTH_PLATFORM".to_string(),
            runtime.release_platform.to_string(),
        );
        let mounts = vec![
            DockerMount::read_only(&aig_input_path, "/inputs/input.aig")?,
            DockerMount::read_write(payload_dir, "/outputs")?,
            driver_cache_mount(store)?,
        ];
        let run_trace =
            run_docker_script(&runtime.docker_image, &mounts, &env, &script, action_id)?;
        commands.push(run_trace);
        details.insert("aig_stats_mode".to_string(), json!("aiger_aig_stats"));
    }

    let output_artifact = ArtifactRef {
        action_id: action_id.to_string(),
        artifact_type: ArtifactType::AigStatsFile,
        relpath: "payload/stats.json".to_string(),
    };

    let mut suggested_next_actions = Vec::new();
    if let Ok(Some(next)) =
        build_aig_stat_diff_suggestion_for_stats_action(store, repo_root, action_id, aig_action_id)
    {
        suggested_next_actions.push(next);
    }
    if let Ok(Some((source_version, source_runtime))) =
        infer_driver_context_from_aig_action(store, aig_action_id)
    {
        details.insert(
            "source_aig_crate_version_label".to_string(),
            json!(version_label("crate", &source_runtime.driver_version)),
        );
        details.insert(
            "source_aig_dso_version_label".to_string(),
            json!(version_label("dso", &source_version)),
        );
        details.insert(
            "source_aig_driver_runtime".to_string(),
            json!(source_runtime),
        );
        details.insert(
            "crate_version_label".to_string(),
            json!(version_label("crate", &source_runtime.driver_version)),
        );
    } else {
        details.insert(
            "crate_version_label".to_string(),
            json!(version_label("crate", &runtime.driver_version)),
        );
    }

    Ok(ActionOutcome {
        dependencies,
        output_artifact,
        commands,
        details: serde_json::Value::Object(details),
        suggested_next_actions,
    })
}

pub(crate) fn run_aig_stat_diff_action(
    store: &ArtifactStore,
    action_id: &str,
    opt_ir_action_id: &str,
    g8r_aig_stats_action_id: &str,
    yosys_abc_aig_stats_action_id: &str,
    payload_dir: &Path,
) -> Result<ActionOutcome> {
    let g8r_dep =
        load_dependency_of_type(store, g8r_aig_stats_action_id, ArtifactType::AigStatsFile)?;
    let yosys_dep = load_dependency_of_type(
        store,
        yosys_abc_aig_stats_action_id,
        ArtifactType::AigStatsFile,
    )?;
    let g8r_stats_path = store.resolve_artifact_ref_path(&g8r_dep);
    let yosys_stats_path = store.resolve_artifact_ref_path(&yosys_dep);
    let g8r_stats_value: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(&g8r_stats_path)
            .with_context(|| format!("reading g8r stats JSON: {}", g8r_stats_path.display()))?,
    )
    .with_context(|| format!("parsing g8r stats JSON: {}", g8r_stats_path.display()))?;
    let yosys_stats_value: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&yosys_stats_path).with_context(|| {
            format!(
                "reading yosys/abc stats JSON: {}",
                yosys_stats_path.display()
            )
        })?)
        .with_context(|| {
            format!(
                "parsing yosys/abc stats JSON: {}",
                yosys_stats_path.display()
            )
        })?;

    let g8r_numeric = collect_numeric_leaf_values(&g8r_stats_value);
    let yosys_numeric = collect_numeric_leaf_values(&yosys_stats_value);
    let mut keys = std::collections::BTreeSet::new();
    keys.extend(g8r_numeric.keys().cloned());
    keys.extend(yosys_numeric.keys().cloned());
    let mut numeric_deltas = Vec::new();
    for key in keys {
        if let (Some(lhs), Some(rhs)) = (g8r_numeric.get(&key), yosys_numeric.get(&key)) {
            numeric_deltas.push(json!({
                "metric": key,
                "g8r": lhs,
                "yosys_abc": rhs,
                "delta_yosys_minus_g8r": rhs - lhs,
            }));
        }
    }

    let diff = json!({
        "opt_ir_action_id": opt_ir_action_id,
        "g8r_aig_stats_action_id": g8r_aig_stats_action_id,
        "yosys_abc_aig_stats_action_id": yosys_abc_aig_stats_action_id,
        "g8r_stats": g8r_stats_value,
        "yosys_abc_stats": yosys_stats_value,
        "numeric_deltas_yosys_minus_g8r": numeric_deltas,
    });
    let output_path = payload_dir.join("aig_stat_diff.json");
    fs::write(
        &output_path,
        serde_json::to_string_pretty(&diff).context("serializing AIG stats diff JSON")?,
    )
    .with_context(|| format!("writing AIG stats diff file: {}", output_path.display()))?;

    let output_artifact = ArtifactRef {
        action_id: action_id.to_string(),
        artifact_type: ArtifactType::AigStatDiffFile,
        relpath: "payload/aig_stat_diff.json".to_string(),
    };
    Ok(ActionOutcome {
        dependencies: vec![g8r_dep, yosys_dep],
        output_artifact,
        commands: Vec::new(),
        details: json!({
            "opt_ir_action_id": opt_ir_action_id,
            "g8r_aig_stats_action_id": g8r_aig_stats_action_id,
            "yosys_abc_aig_stats_action_id": yosys_abc_aig_stats_action_id,
            "numeric_delta_count": diff["numeric_deltas_yosys_minus_g8r"].as_array().map(|v| v.len()).unwrap_or(0),
        }),
        suggested_next_actions: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn compute_action_id_matches_golden_for_aig_stat_diff() {
        let action = ActionSpec::AigStatDiff {
            opt_ir_action_id: "1".repeat(64),
            g8r_aig_stats_action_id: "2".repeat(64),
            yosys_abc_aig_stats_action_id: "3".repeat(64),
        };
        let action_id = compute_action_id(&action).expect("compute action id");
        assert_eq!(
            action_id,
            "2541d69cb429497c67d00151da6a85074c72bf702a12e280490e040aff9dd946"
        );
    }

    #[test]
    fn compute_action_id_changes_when_action_changes() {
        let a = ActionSpec::AigStatDiff {
            opt_ir_action_id: "1".repeat(64),
            g8r_aig_stats_action_id: "2".repeat(64),
            yosys_abc_aig_stats_action_id: "3".repeat(64),
        };
        let b = ActionSpec::AigStatDiff {
            opt_ir_action_id: "1".repeat(64),
            g8r_aig_stats_action_id: "2".repeat(64),
            yosys_abc_aig_stats_action_id: "4".repeat(64),
        };
        let id_a = compute_action_id(&a).expect("compute id a");
        let id_b = compute_action_id(&b).expect("compute id b");
        assert_ne!(id_a, id_b);
    }

    #[test]
    fn compute_action_id_is_stable_for_equivalent_action_value() {
        let action = ActionSpec::AigStatDiff {
            opt_ir_action_id: "a".repeat(64),
            g8r_aig_stats_action_id: "b".repeat(64),
            yosys_abc_aig_stats_action_id: "c".repeat(64),
        };
        let id_a = compute_action_id(&action).expect("compute id a");
        let id_b = compute_action_id(&action.clone()).expect("compute id b");
        assert_eq!(id_a, id_b);
    }

    #[test]
    fn promote_staging_action_dir_replaces_failed_only_final_dir() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-promote-replace-{}-{}",
            std::process::id(),
            nanos
        ));
        let staging_dir = root.join("staging").join("action");
        let final_dir = root.join("artifacts").join("aa").join("bb").join("action");
        fs::create_dir_all(&staging_dir).expect("create staging dir");
        fs::create_dir_all(&final_dir).expect("create final dir");
        fs::write(staging_dir.join("provenance.json"), "{\"ok\":true}")
            .expect("write staged provenance");
        fs::write(final_dir.join("failed.json"), "{\"error\":\"boom\"}")
            .expect("write failed marker");

        promote_staging_action_dir(&staging_dir, &final_dir).expect("promote staging");

        assert!(final_dir.join("provenance.json").exists());
        assert!(!final_dir.join("failed.json").exists());
        assert!(!staging_dir.exists());
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn promote_staging_action_dir_preserves_existing_completed_final_dir() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-promote-preserve-{}-{}",
            std::process::id(),
            nanos
        ));
        let staging_dir = root.join("staging").join("action");
        let final_dir = root.join("artifacts").join("aa").join("bb").join("action");
        fs::create_dir_all(&staging_dir).expect("create staging dir");
        fs::create_dir_all(&final_dir).expect("create final dir");
        fs::write(staging_dir.join("provenance.json"), "{\"ok\":\"staged\"}")
            .expect("write staged provenance");
        fs::write(final_dir.join("provenance.json"), "{\"ok\":\"final\"}")
            .expect("write final provenance");

        promote_staging_action_dir(&staging_dir, &final_dir).expect("promote staging");

        let final_text =
            fs::read_to_string(final_dir.join("provenance.json")).expect("read final provenance");
        assert_eq!(final_text, "{\"ok\":\"final\"}");
        assert!(!staging_dir.exists());
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn extract_single_ir_fn_block_with_name_renames_top_fn() {
        let input = r#"package bool_cone

top fn cone(leaf_2: bits[8] id=1) -> bits[1] {
  literal.2: bits[8] = literal(value=0, id=2)
  ret eq.3: bits[1] = eq(leaf_2, literal.2, id=3)
}"#;
        let block =
            extract_single_ir_fn_block_with_name(input, "__k3_cone_deadbeef").expect("extract");
        assert!(block.starts_with("fn __k3_cone_deadbeef("));
        assert!(!block.contains("top fn"));
        assert!(block.contains("ret eq.3"));
    }

    #[test]
    fn find_duplicate_ir_text_ids_reports_duplicates() {
        let input = r#"fn __k3_cone_test(x: bits[8] id=1) -> bits[1] {
  literal.1: bits[8] = literal(value=0, id=1)
  ret eq.2: bits[1] = eq(x, literal.1, id=2)
}"#;
        let duplicates = find_duplicate_ir_text_ids(input);
        assert_eq!(duplicates, vec![1]);
    }

    #[test]
    fn extract_ir_fn_block_by_name_finds_named_function() {
        let input = r#"package k_bool_cones

fn __k3_cone_aaaa(x: bits[1] id=1) -> bits[1] {
  ret x.2: bits[1] = identity(x, id=2)
}

fn __k3_cone_bbbb(y: bits[1] id=1) -> bits[1] {
  ret y.2: bits[1] = not(y, id=2)
}"#;
        let block = extract_ir_fn_block_by_name(input, "__k3_cone_bbbb").expect("extract by name");
        assert!(block.starts_with("fn __k3_cone_bbbb("));
        assert!(block.contains("ret y.2"));
        assert!(!block.contains("__k3_cone_aaaa"));
    }

    #[test]
    fn rewrite_ir_package_top_function_promotes_target_and_demotes_other_top() {
        let input = r#"package p

top fn old_top(x: bits[1] id=1) -> bits[1] {
  ret x.2: bits[1] = not(x, id=2)
}

fn wanted(y: bits[1] id=3) -> bits[1] {
  ret y.4: bits[1] = identity(y, id=4)
}"#;
        let rewritten = rewrite_ir_package_top_function(input, "wanted").expect("rewrite top");
        assert!(rewritten.contains("fn old_top("));
        assert!(!rewritten.contains("top fn old_top("));
        assert!(rewritten.contains("top fn wanted("));
    }

    #[test]
    fn rewrite_ir_package_top_function_errors_when_target_missing() {
        let input = r#"package p

fn a(x: bits[1] id=1) -> bits[1] {
  ret x.2: bits[1] = not(x, id=2)
}"#;
        let err = rewrite_ir_package_top_function(input, "missing")
            .expect_err("missing target should error");
        assert!(format!("{err:#}").contains("unable to mark requested IR top function"));
    }

    #[test]
    fn rewrite_ir_text_ids_with_offset_shifts_all_ids() {
        let input = "fn f(x: bits[1] id=1) -> bits[1] {\n  ret y.2: bits[1] = not(x, id=2)\n}";
        let rewritten = rewrite_ir_text_ids_with_offset(input, 10).expect("rewrite ids");
        assert!(rewritten.contains("id=11"));
        assert!(rewritten.contains("id=12"));
        assert!(!rewritten.contains("id=1)"));
        assert!(!rewritten.contains("id=2)"));
    }

    #[test]
    fn rewrite_ir_node_name_suffixes_with_offset_shifts_suffixes() {
        let input =
            "fn f(x: bits[1] id=1) -> bits[1] {\n  ret bit_slice.2: bits[1] = not(x, id=2)\n}";
        let rewritten =
            rewrite_ir_node_name_suffixes_with_offset(input, 10).expect("rewrite suffixes");
        assert!(rewritten.contains("bit_slice.12"));
        assert!(!rewritten.contains("bit_slice.2"));
    }

    #[test]
    fn find_ir_node_name_id_suffix_mismatches_reports_mismatch() {
        let input =
            "fn f(x: bits[1] id=1) -> bits[1] {\n  ret bit_slice.2: bits[1] = not(x, id=7)\n}";
        let mismatches = find_ir_node_name_id_suffix_mismatches(input);
        assert_eq!(mismatches.len(), 1);
        assert!(mismatches[0].contains("bit_slice.2"));
        assert!(mismatches[0].contains("id attribute 7"));
    }

    #[test]
    fn build_k_bool_cone_corpus_outputs_dedupes_by_structural_hash() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-kbool-test-{}-{}",
            std::process::id(),
            nanos
        ));
        let raw_dir = root.join("raw");
        std::fs::create_dir_all(&raw_dir).expect("create raw dir");
        let hash_a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let hash_b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        std::fs::write(
            raw_dir.join(format!("{hash_a}.ir")),
            r#"package bool_cone

top fn cone(leaf_2: bits[8] id=1) -> bits[1] {
  literal.2: bits[8] = literal(value=0, id=2)
  ret eq.3: bits[1] = eq(leaf_2, literal.2, id=3)
}"#,
        )
        .expect("write cone a");
        std::fs::write(
            raw_dir.join(format!("{hash_b}.ir")),
            r#"package bool_cone

top fn cone(leaf_7: bits[1] id=1) -> bits[1] {
  ret not.2: bits[1] = not(leaf_7, id=2)
}"#,
        )
        .expect("write cone b");
        std::fs::write(
            raw_dir.join("manifest.jsonl"),
            format!(
                "{{\"sha256\":\"{hash_a}\",\"sink_node_index\":1,\"frontier_leaf_indices\":[2],\"frontier_non_literal_count\":1,\"included_node_count\":2}}\n\
                 {{\"sha256\":\"{hash_a}\",\"sink_node_index\":1,\"frontier_leaf_indices\":[2],\"frontier_non_literal_count\":1,\"included_node_count\":2}}\n\
                 {{\"sha256\":\"{hash_b}\",\"sink_node_index\":3,\"frontier_leaf_indices\":[7],\"frontier_non_literal_count\":1,\"included_node_count\":1}}\n"
            ),
        )
        .expect("write manifest");

        let output_ir_path = root.join("k_bool_cones_k3.ir");
        let output_manifest_path = root.join("k_bool_cones_k3_manifest.json");
        let manifest = build_k_bool_cone_corpus_outputs(
            &raw_dir,
            &output_ir_path,
            &output_manifest_path,
            "f".repeat(64).as_str(),
            "__foo",
            3,
            None,
        )
        .expect("build corpus outputs");

        assert_eq!(manifest.total_manifest_rows, 3);
        assert_eq!(manifest.deduped_unique_cones, 2);
        assert_eq!(manifest.entries.len(), 2);
        let merged = std::fs::read_to_string(&output_ir_path).expect("read merged package");
        assert!(merged.starts_with("package k_bool_cones_k3"));
        assert!(merged.contains("fn __k3_cone_aaaaaaaaaaaaaaaa("));
        assert!(merged.contains("fn __k3_cone_bbbbbbbbbbbbbbbb("));
        assert!(
            find_duplicate_ir_text_ids(&merged).is_empty(),
            "merged package should not contain duplicate text ids"
        );
        assert!(
            find_ir_node_name_id_suffix_mismatches(&merged).is_empty(),
            "merged package should keep node-name suffixes aligned with id attributes"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn build_k_bool_cone_corpus_outputs_filters_by_ir_op_count() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-kbool-filter-test-{}-{}",
            std::process::id(),
            nanos
        ));
        let raw_dir = root.join("raw");
        std::fs::create_dir_all(&raw_dir).expect("create raw dir");
        let hash_a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let hash_b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        std::fs::write(
            raw_dir.join(format!("{hash_a}.ir")),
            r#"package bool_cone

top fn cone(leaf_2: bits[8] id=1) -> bits[1] {
  literal.2: bits[8] = literal(value=0, id=2)
  ret eq.3: bits[1] = eq(leaf_2, literal.2, id=3)
}"#,
        )
        .expect("write cone a");
        std::fs::write(
            raw_dir.join(format!("{hash_b}.ir")),
            r#"package bool_cone

top fn cone(leaf_7: bits[1] id=1) -> bits[1] {
  ret not.2: bits[1] = not(leaf_7, id=2)
}"#,
        )
        .expect("write cone b");
        std::fs::write(
            raw_dir.join("manifest.jsonl"),
            format!(
                "{{\"sha256\":\"{hash_a}\",\"sink_node_index\":1,\"frontier_leaf_indices\":[2],\"frontier_non_literal_count\":1,\"included_node_count\":2}}\n\
                 {{\"sha256\":\"{hash_b}\",\"sink_node_index\":3,\"frontier_leaf_indices\":[7],\"frontier_non_literal_count\":1,\"included_node_count\":1}}\n"
            ),
        )
        .expect("write manifest");

        let output_ir_path = root.join("k_bool_cones_k3.ir");
        let output_manifest_path = root.join("k_bool_cones_k3_manifest.json");
        let manifest = build_k_bool_cone_corpus_outputs(
            &raw_dir,
            &output_ir_path,
            &output_manifest_path,
            "f".repeat(64).as_str(),
            "__foo",
            3,
            Some(1),
        )
        .expect("build corpus outputs");

        assert_eq!(manifest.total_manifest_rows, 2);
        assert_eq!(manifest.filtered_out_ir_op_count, 1);
        assert_eq!(manifest.entries.len(), 1);
        assert_eq!(manifest.entries[0].structural_hash, hash_b);
        assert_eq!(manifest.entries[0].ir_op_count, Some(1));
        let merged = std::fs::read_to_string(&output_ir_path).expect("read merged package");
        assert!(!merged.contains("__k3_cone_aaaaaaaaaaaaaaaa"));
        assert!(merged.contains("__k3_cone_bbbbbbbbbbbbbbbb"));

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn build_k_bool_cone_corpus_outputs_rejects_duplicate_text_ids() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-kbool-dup-id-test-{}-{}",
            std::process::id(),
            nanos
        ));
        let raw_dir = root.join("raw");
        std::fs::create_dir_all(&raw_dir).expect("create raw dir");
        let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        std::fs::write(
            raw_dir.join(format!("{hash}.ir")),
            r#"package bool_cone

top fn cone(leaf_2: bits[8] id=1) -> bits[1] {
  literal.2: bits[8] = literal(value=0, id=1)
  ret eq.3: bits[1] = eq(leaf_2, literal.2, id=3)
}"#,
        )
        .expect("write cone");
        std::fs::write(
            raw_dir.join("manifest.jsonl"),
            format!(
                "{{\"sha256\":\"{hash}\",\"sink_node_index\":1,\"frontier_leaf_indices\":[2],\"frontier_non_literal_count\":1,\"included_node_count\":2}}\n"
            ),
        )
        .expect("write manifest");

        let output_ir_path = root.join("k_bool_cones_k3.ir");
        let output_manifest_path = root.join("k_bool_cones_k3_manifest.json");
        let err = build_k_bool_cone_corpus_outputs(
            &raw_dir,
            &output_ir_path,
            &output_manifest_path,
            "f".repeat(64).as_str(),
            "__foo",
            3,
            None,
        )
        .expect_err("expected duplicate text-id validation failure");
        let err_text = format!("{err:#}");
        assert!(err_text.contains("duplicate text ids"));

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn build_k_bool_cone_corpus_suggested_actions_builds_g8r_frontend_and_combo_per_entry() {
        let manifest = KBoolConeCorpusManifest {
            schema_version: 1,
            source_ir_action_id: "a".repeat(64),
            source_ir_top: "__top".to_string(),
            k: 3,
            max_ir_ops: Some(16),
            total_manifest_rows: 2,
            emitted_cone_files: 2,
            deduped_unique_cones: 2,
            filtered_out_ir_op_count: 0,
            output_ir_relpath: "payload/k_bool_cones_k3.ir".to_string(),
            entries: vec![
                KBoolConeCorpusEntry {
                    structural_hash:
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_string(),
                    fn_name: "__k3_cone_aaaaaaaaaaaaaaaa".to_string(),
                    source_index: 0,
                    sink_node_index: 1,
                    frontier_leaf_indices: vec![1, 2],
                    frontier_non_literal_count: 2,
                    included_node_count: 4,
                    ir_fn_signature: None,
                    ir_op_count: None,
                },
                KBoolConeCorpusEntry {
                    structural_hash:
                        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                            .to_string(),
                    fn_name: "__k3_cone_bbbbbbbbbbbbbbbb".to_string(),
                    source_index: 1,
                    sink_node_index: 2,
                    frontier_leaf_indices: vec![3],
                    frontier_non_literal_count: 1,
                    included_node_count: 2,
                    ir_fn_signature: None,
                    ir_op_count: None,
                },
            ],
        };
        let runtime = DriverRuntimeSpec {
            driver_version: "0.31.0".to_string(),
            release_platform: "ubuntu2004".to_string(),
            docker_image: "xlsynth-bvc-driver:0.31.0".to_string(),
            dockerfile: "docker/xlsynth-driver.Dockerfile".to_string(),
        };

        let suggestions = build_k_bool_cone_corpus_suggested_actions(
            "f1a545045a06d81c95bd6d70447918805d408b02d4f262b73cf625a4e5feb4ac",
            &manifest,
            "v0.35.0",
            &runtime,
        );
        assert_eq!(suggestions.len(), 6);
        assert!(suggestions.iter().any(|s| {
            matches!(
                s.action,
                ActionSpec::DriverIrToG8rAig {
                    ref top_fn_name,
                    fraig: false,
                    lowering_mode: G8rLoweringMode::Default,
                    ..
                } if top_fn_name.as_deref() == Some("__k3_cone_aaaaaaaaaaaaaaaa")
            )
        }));
        assert!(suggestions.iter().any(|s| {
            matches!(
                s.action,
                ActionSpec::DriverIrToG8rAig {
                    ref top_fn_name,
                    fraig: false,
                    lowering_mode: G8rLoweringMode::FrontendNoPrepRewrite,
                    ..
                } if top_fn_name.as_deref() == Some("__k3_cone_aaaaaaaaaaaaaaaa")
            )
        }));
        assert!(suggestions.iter().any(|s| {
            matches!(
                s.action,
                ActionSpec::IrFnToCombinationalVerilog {
                    ref top_fn_name,
                    use_system_verilog: false,
                    ..
                } if top_fn_name.as_deref() == Some("__k3_cone_bbbbbbbbbbbbbbbb")
            )
        }));
    }

    #[test]
    fn build_opt_ir_aig_equiv_suggestions_targets_both_fraig_variants() {
        let runtime = DriverRuntimeSpec {
            driver_version: "0.31.0".to_string(),
            release_platform: "ubuntu2004".to_string(),
            docker_image: "xlsynth-bvc-driver:0.31.0".to_string(),
            dockerfile: "docker/xlsynth-driver.Dockerfile".to_string(),
        };
        let opt_ir_action_id = "f1a545045a06d81c95bd6d70447918805d408b02d4f262b73cf625a4e5feb4ac";
        let no_fraig_aig_action_id =
            "ba8c7276254f8f0862fa65f5f0814be8c9f48d5f6d89ac78cbf2982f8af2f5d2";
        let fraig_aig_action_id =
            "8b4de63af847a6076be3f93243cb3211b4de96f4dc8f95b04ea20ff317b6fbf5";
        let suggestions = build_opt_ir_aig_equiv_suggestions(
            opt_ir_action_id,
            "__float32__add",
            no_fraig_aig_action_id,
            fraig_aig_action_id,
            "v0.35.0",
            &runtime,
        )
        .expect("build suggestions");

        assert_eq!(suggestions.len(), 2);
        assert!(suggestions.iter().any(|s| {
            matches!(
                s.action,
                ActionSpec::DriverIrAigEquiv {
                    ref ir_action_id,
                    ref aig_action_id,
                    ref top_fn_name,
                    ..
                } if ir_action_id == opt_ir_action_id
                    && aig_action_id == no_fraig_aig_action_id
                    && top_fn_name.as_deref() == Some("__float32__add")
            )
        }));
        assert!(suggestions.iter().any(|s| {
            matches!(
                s.action,
                ActionSpec::DriverIrAigEquiv {
                    ref ir_action_id,
                    ref aig_action_id,
                    ref top_fn_name,
                    ..
                } if ir_action_id == opt_ir_action_id
                    && aig_action_id == fraig_aig_action_id
                    && top_fn_name.as_deref() == Some("__float32__add")
            )
        }));
    }
}
