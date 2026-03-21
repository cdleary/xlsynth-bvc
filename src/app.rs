use anyhow::{Context, Result, bail};
use clap::Parser;
use serde_json::json;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::Path;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use crate::DELAY_INFO_OUTPUT_FORMAT_TEXTPROTO_V1;
use crate::cli::{Cli, RunAction, TopCommand};
use crate::driver_ir_aig_equiv_enabled;
use crate::executor::{
    build_k_bool_cone_corpus_suggested_actions_for_entries, build_opt_ir_aig_equiv_suggestions,
    compute_action_id, execute_action, execute_action_batch, resolve_driver_ir_aig_equiv_supported,
};
use crate::model::*;
use crate::query::{enqueue_processing_for_crate_version, rebuild_web_indices};
use crate::queue::*;
use crate::queue_only_previous_loss_k_cones_enabled;
use crate::runtime::*;
use crate::service::*;
use crate::sled_space::analyze_sled_space;
use crate::snapshot::{BuildStaticSnapshotOptions, build_static_snapshot, verify_static_snapshot};
use crate::store::{
    ArtifactStore, backfill_sled_action_file_compression, compact_sled_db,
    ingest_legacy_failed_records, prune_sled_actions_by_relpath_size,
};
use crate::versioning::*;
use crate::web::{self, types::WebRunnerConfig};

const DEFAULT_QUEUE_RUNTIME_PREPARE_MIN_INTERVAL_SECS: u64 = 30;
const MAX_COMPATIBLE_BATCH_SIZE: usize = 4;
static QUEUE_RUNTIME_PREPARE_AT: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
static K3_CONE_CANONICAL_CACHE: OnceLock<Mutex<K3ConeCanonicalCache>> = OnceLock::new();
static K_BOOL_PREVIOUS_LOSS_CACHE: OnceLock<Mutex<KBoolPreviousLossCache>> = OnceLock::new();

#[derive(Debug, Clone, Copy, Default)]
struct SuggestedEnqueuePolicy {
    only_previous_loss_k_cones: bool,
}

impl SuggestedEnqueuePolicy {
    fn from_env() -> Self {
        Self {
            only_previous_loss_k_cones: queue_only_previous_loss_k_cones_enabled(),
        }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct K3ConeCanonicalKey {
    structural_hash: String,
    version: String,
    runtime: DriverRuntimeSpec,
}

#[derive(Debug, Default)]
struct K3ConeCanonicalCache {
    // (k-bool action id, cone top fn) -> full structural hash
    cone_hash_by_action_and_top: HashMap<(String, String), String>,
    // (structural hash + runtime tuple) -> canonical k-bool action id
    canonical_ir_action_by_key: HashMap<K3ConeCanonicalKey, String>,
}

#[derive(Debug, Default)]
struct KBoolPreviousLossCache {
    store_identity: Option<std::path::PathBuf>,
    complete_by_k: HashMap<u32, HashSet<String>>,
}

fn queue_runtime_prepare_min_interval() -> Duration {
    let secs = std::env::var("BVC_QUEUE_RUNTIME_PREPARE_MIN_INTERVAL_SECS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_QUEUE_RUNTIME_PREPARE_MIN_INTERVAL_SECS);
    Duration::from_secs(secs.max(1))
}

fn maybe_prepare_queue_runtime_environment(store: &ArtifactStore, repo_root: &Path) -> Result<()> {
    let min_interval = queue_runtime_prepare_min_interval();
    let now = Instant::now();
    let state = QUEUE_RUNTIME_PREPARE_AT.get_or_init(|| Mutex::new(None));

    {
        let guard = state
            .lock()
            .map_err(|_| anyhow::anyhow!("queue runtime prepare state lock poisoned"))?;
        if let Some(last) = *guard
            && now.duration_since(last) < min_interval
        {
            return Ok(());
        }
    }

    let mut guard = state
        .lock()
        .map_err(|_| anyhow::anyhow!("queue runtime prepare state lock poisoned"))?;
    if let Some(last) = *guard
        && now.duration_since(last) < min_interval
    {
        return Ok(());
    }

    prepare_queue_runtime_environment(store, repo_root)?;
    *guard = Some(Instant::now());
    Ok(())
}

fn compatible_batch_claim_limit(max_extra: usize) -> usize {
    max_extra.min(MAX_COMPATIBLE_BATCH_SIZE.saturating_sub(1))
}

pub(crate) fn run() -> Result<()> {
    let Cli {
        store_dir,
        artifacts_via_sled,
        command,
    } = Cli::parse();
    if let TopCommand::AnalyzeSledSpace { top, sample } = &command {
        let summary = analyze_sled_space(&artifacts_via_sled, *top, *sample)?;
        println!(
            "{}",
            serde_json::to_string_pretty(&summary)
                .expect("serializing sled space analysis summary")
        );
        return Ok(());
    }
    if let TopCommand::CompactSledDb {
        output_path,
        replace_source,
    } = &command
    {
        let summary =
            compact_sled_db(&artifacts_via_sled, output_path.as_deref(), *replace_source)?;
        println!(
            "{}",
            serde_json::to_string_pretty(&summary).expect("serializing sled compaction summary")
        );
        return Ok(());
    }
    if let TopCommand::BackfillSledActionFileCompression {
        dry_run,
        limit,
        target_relpath,
    } = &command
    {
        let summary = backfill_sled_action_file_compression(
            &artifacts_via_sled,
            *limit,
            *dry_run,
            target_relpath.as_deref(),
        )?;
        println!(
            "{}",
            serde_json::to_string_pretty(&summary)
                .expect("serializing sled action-file compression backfill summary")
        );
        return Ok(());
    }
    if let TopCommand::PruneSledActionsByRelpathSize {
        relpath,
        min_bytes,
        dry_run,
        no_downstream,
    } = &command
    {
        let summary = prune_sled_actions_by_relpath_size(
            &store_dir,
            &artifacts_via_sled,
            relpath,
            *min_bytes,
            !*no_downstream,
            *dry_run,
        )?;
        println!(
            "{}",
            serde_json::to_string_pretty(&summary)
                .expect("serializing sled relpath-size prune summary")
        );
        return Ok(());
    }
    let repo_root = std::env::current_dir().context("getting current directory")?;
    if let TopCommand::VerifyStaticSnapshot { snapshot_dir } = &command {
        let summary = verify_static_snapshot(snapshot_dir)?;
        println!(
            "{}",
            serde_json::to_string_pretty(&summary)
                .expect("serializing verify static snapshot summary")
        );
        return Ok(());
    }
    if let TopCommand::ServeWeb {
        bind,
        no_runner,
        snapshot_dir: Some(snapshot_dir),
        runner_lease_seconds,
        runner_poll_millis,
        runner_batch_size,
        runner_workers,
    } = &command
    {
        let runner_config = WebRunnerConfig {
            enabled: !*no_runner,
            lease_seconds: *runner_lease_seconds,
            poll_interval: Duration::from_millis(*runner_poll_millis),
            drain_batch_size: *runner_batch_size,
            worker_count: *runner_workers,
        };
        if runner_config.enabled {
            bail!("--snapshot-dir requires --no-runner");
        }
        let snapshot_store = ArtifactStore::new_with_snapshot(store_dir, snapshot_dir.clone());
        snapshot_store.ensure_layout()?;
        web::serve_web_ui(
            Arc::new(snapshot_store),
            repo_root.clone(),
            artifacts_via_sled.clone(),
            bind,
            runner_config,
        )?;
        return Ok(());
    }
    let store = ArtifactStore::new_with_sled(store_dir, artifacts_via_sled.clone());
    store.ensure_layout()?;

    match command {
        TopCommand::Run { action } => {
            let action_spec = run_action_to_spec(&repo_root, action)?;
            let (action_id, artifact_ref) = execute_action(&store, action_spec)?;
            println!(
                "{}\n{}",
                action_id,
                serde_json::to_string_pretty(&artifact_ref).expect("serializing ArtifactRef")
            );
        }
        TopCommand::Enqueue { priority, action } => {
            let action_spec = run_action_to_spec(&repo_root, action)?;
            let action_id = enqueue_action_with_priority(&store, action_spec, priority)?;
            println!("{}", action_id);
        }
        TopCommand::EnqueueCrateVersion {
            crate_version,
            priority,
        } => {
            enqueue_processing_for_crate_version(&store, &repo_root, &crate_version, priority)?;
            println!("{}", version_label("crate", &crate_version));
        }
        TopCommand::DrainQueue {
            limit,
            worker_id,
            lease_seconds,
            no_reclaim_expired,
        } => {
            let worker_id = worker_id.unwrap_or_else(default_worker_id);
            let drained = drain_queue(
                &store,
                &repo_root,
                limit,
                &worker_id,
                lease_seconds,
                !no_reclaim_expired,
                None,
            )?;
            println!("{}", drained);
        }
        TopCommand::ServeWeb {
            bind,
            no_runner,
            snapshot_dir: _,
            runner_lease_seconds,
            runner_poll_millis,
            runner_batch_size,
            runner_workers,
        } => {
            let runner_config = WebRunnerConfig {
                enabled: !no_runner,
                lease_seconds: runner_lease_seconds,
                poll_interval: Duration::from_millis(runner_poll_millis),
                drain_batch_size: runner_batch_size,
                worker_count: runner_workers,
            };
            web::serve_web_ui(
                Arc::new(store),
                repo_root.clone(),
                artifacts_via_sled.clone(),
                &bind,
                runner_config,
            )?;
        }
        TopCommand::DiscoverReleases {
            after,
            max_pages,
            dry_run,
        } => {
            let summary = discover_releases(&store, &repo_root, &after, max_pages, !dry_run)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&summary).expect("serializing discover summary")
            );
        }
        TopCommand::ShowSuggested {
            action_id,
            recursive,
            max_depth,
        } => {
            let report = build_suggested_report(&store, &action_id, recursive, max_depth)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("serializing suggestion report")
            );
        }
        TopCommand::EnqueueSuggested {
            action_id,
            recursive,
            max_depth,
            priority,
        } => {
            let report = enqueue_suggested_actions(
                &store, &repo_root, &action_id, recursive, max_depth, priority,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .expect("serializing enqueue suggested report")
            );
        }
        TopCommand::AuditSuggested { include_completed } => {
            let report = build_suggested_audit_report(&store, include_completed)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("serializing suggestion audit report")
            );
        }
        TopCommand::FindAigStatDiffs { opt_ir_action_id } => {
            let report = find_aig_stat_diffs_for_opt_ir(&store, &opt_ir_action_id)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("serializing aig stat diff report")
            );
        }
        TopCommand::PopulateIrFnCorpusStructural {
            output_dir,
            recompute_missing_hashes,
            threads,
        } => {
            let summary = populate_ir_fn_corpus_structural_index(
                &store,
                &repo_root,
                &output_dir,
                recompute_missing_hashes,
                threads,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .expect("serializing IR corpus structural index summary")
            );
        }
        TopCommand::CheckIrFnCorpusStructuralFreshness => {
            let summary = check_ir_fn_corpus_structural_freshness(&store)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .expect("serializing IR corpus structural freshness summary")
            );
            if !summary.up_to_date {
                bail!(
                    "IR fn corpus structural index is stale; run `populate-ir-fn-corpus-structural` and retry"
                );
            }
        }
        TopCommand::RebuildWebIndices => {
            let summary = rebuild_web_indices(&store, &repo_root)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .expect("serializing rebuild web indices summary")
            );
        }
        TopCommand::BuildStaticSnapshot {
            out_dir,
            overwrite,
            skip_rebuild_web_indices,
        } => {
            let summary = build_static_snapshot(
                &store,
                &repo_root,
                &BuildStaticSnapshotOptions {
                    out_dir,
                    overwrite,
                    skip_rebuild_web_indices,
                },
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .expect("serializing build static snapshot summary")
            );
        }
        TopCommand::VerifyStaticSnapshot { .. } => {
            unreachable!("verify-static-snapshot handled before store initialization")
        }
        TopCommand::AnalyzeSledSpace { .. } => {
            unreachable!("analyze-sled-space handled before store initialization")
        }
        TopCommand::CompactSledDb { .. } => {
            unreachable!("compact-sled-db handled before store initialization")
        }
        TopCommand::BackfillSledActionFileCompression { .. } => {
            unreachable!(
                "backfill-sled-action-file-compression handled before store initialization"
            )
        }
        TopCommand::PruneSledActionsByRelpathSize { .. } => {
            unreachable!("prune-sled-actions-by-relpath-size handled before store initialization")
        }
        TopCommand::EnqueueStructuralOptIrG8r {
            crate_version,
            fraig,
            recompute_missing_hashes,
            dry_run,
        } => {
            let summary = enqueue_structural_opt_ir_g8r_actions(
                &store,
                &repo_root,
                &crate_version,
                fraig.as_bool(),
                recompute_missing_hashes,
                dry_run,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .expect("serializing enqueue structural opt ir g8r summary")
            );
        }
        TopCommand::EnqueueStructuralOptIrKBoolCones {
            crate_version,
            k,
            only_previous_losses,
            recompute_missing_hashes,
            dry_run,
        } => {
            let summary = enqueue_structural_opt_ir_k_bool_cone_actions(
                &store,
                &repo_root,
                &crate_version,
                k,
                only_previous_losses,
                recompute_missing_hashes,
                dry_run,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .expect("serializing enqueue structural opt ir k-bool-cone summary")
            );
        }
        TopCommand::BackfillKBoolConeSuggestions { enqueue, dry_run } => {
            let summary = backfill_k_bool_cone_suggestions(&store, &repo_root, enqueue, dry_run)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .expect("serializing backfill k-bool-cone suggestions summary")
            );
        }
        TopCommand::BackfillStdlibOptIrAigEquivSuggestions { enqueue, dry_run } => {
            let summary =
                backfill_stdlib_opt_ir_aig_equiv_suggestions(&store, &repo_root, enqueue, dry_run)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .expect("serializing backfill stdlib opt-IR AIG equiv suggestions summary")
            );
        }
        TopCommand::BackfillOptIrFrontendCompareSuggestions {
            enqueue,
            dry_run,
            priority,
            crate_version,
        } => {
            let summary = backfill_opt_ir_frontend_compare_suggestions(
                &store,
                enqueue,
                dry_run,
                priority,
                crate_version.as_deref(),
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .expect("serializing backfill opt-IR frontend compare suggestions summary")
            );
        }
        TopCommand::RepairQueue {
            apply,
            reenqueue_missing_suggested,
        } => {
            let queue_repair = repair_corrupt_queue_records(&store, apply)?;
            let reenqueue_summary = if reenqueue_missing_suggested {
                Some(enqueue_missing_suggested_actions(
                    &store, &repo_root, !apply,
                )?)
            } else {
                None
            };
            let summary = json!({
                "dry_run": !apply,
                "queue_repair": queue_repair,
                "reenqueue_missing_suggested": reenqueue_summary,
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&summary).expect("serializing repair-queue summary")
            );
        }
        TopCommand::IngestLegacyFailedRecords {
            dry_run,
            keep_legacy_files,
        } => {
            let summary = ingest_legacy_failed_records(&store, dry_run, keep_legacy_files)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .expect("serializing legacy failed-record ingest summary")
            );
        }
        TopCommand::RefreshVersionCompat => {
            let summary = refresh_version_compat_json()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&summary)
                    .expect("serializing version compat refresh summary")
            );
        }
        TopCommand::DslxToMangledIrFnName {
            dslx_module_name,
            dslx_fn_name,
        } => {
            println!(
                "{}",
                dslx_to_mangled_ir_fn_name(&dslx_module_name, &dslx_fn_name)
            );
        }
        TopCommand::Rematerialize { action_id } => {
            let provenance = store.load_provenance(&action_id)?;
            let (computed, artifact_ref) = execute_action(&store, provenance.action.clone())?;
            if computed != action_id {
                bail!(
                    "action id mismatch when rematerializing; stored={} computed={}",
                    action_id,
                    computed
                );
            }
            println!(
                "{}\n{}",
                computed,
                serde_json::to_string_pretty(&artifact_ref).expect("serializing ArtifactRef")
            );
        }
        TopCommand::ShowProvenance { action_id } => {
            let provenance = store.load_provenance(&action_id)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&provenance).expect("serializing provenance")
            );
        }
        TopCommand::Resolve { action_id } => {
            let provenance = store.load_provenance(&action_id)?;
            let path = store.resolve_artifact_ref_path(&provenance.output_artifact);
            println!("{}", path.display());
        }
    }

    Ok(())
}

pub(crate) fn run_action_to_spec(repo_root: &Path, action: RunAction) -> Result<ActionSpec> {
    match action {
        RunAction::DownloadStdlib { version } => {
            let runtime = canonical_stdlib_discovery_runtime_for_version(repo_root, &version)?;
            Ok(ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                version,
                discovery_runtime: Some(runtime),
            })
        }
        RunAction::DownloadSourceSubtree { version, subtree } => {
            validate_relative_subpath(&subtree)?;
            let runtime = canonical_stdlib_discovery_runtime_for_version(repo_root, &version)?;
            Ok(ActionSpec::DownloadAndExtractXlsynthSourceSubtree {
                version,
                subtree,
                discovery_runtime: Some(runtime),
            })
        }
        RunAction::DslxFnToIr {
            dslx_subtree_action_id,
            dslx_file,
            dslx_fn_name,
            version,
            driver,
        } => Ok(ActionSpec::DriverDslxFnToIr {
            dslx_subtree_action_id,
            dslx_file,
            dslx_fn_name,
            runtime: driver.into_runtime(repo_root, &version)?,
            version,
        }),
        RunAction::IrToOpt {
            ir_action_id,
            top_fn_name,
            version,
            driver,
        } => Ok(ActionSpec::DriverIrToOpt {
            ir_action_id,
            top_fn_name,
            runtime: driver.into_runtime(repo_root, &version)?,
            version,
        }),
        RunAction::IrToDelayInfo {
            ir_action_id,
            top_fn_name,
            delay_model,
            version,
            driver,
        } => Ok(ActionSpec::DriverIrToDelayInfo {
            ir_action_id,
            top_fn_name,
            delay_model,
            output_format: DELAY_INFO_OUTPUT_FORMAT_TEXTPROTO_V1.to_string(),
            runtime: driver.into_runtime(repo_root, &version)?,
            version,
        }),
        RunAction::IrEquiv {
            lhs_ir_action_id,
            rhs_ir_action_id,
            top_fn_name,
            version,
            driver,
        } => Ok(ActionSpec::DriverIrEquiv {
            lhs_ir_action_id,
            rhs_ir_action_id,
            top_fn_name,
            version: version.clone(),
            runtime: driver.into_runtime(repo_root, &version)?,
        }),
        RunAction::IrAigEquiv {
            ir_action_id,
            aig_action_id,
            top_fn_name,
            version,
            driver,
        } => Ok(ActionSpec::DriverIrAigEquiv {
            ir_action_id,
            aig_action_id,
            top_fn_name,
            version: version.clone(),
            runtime: driver.into_runtime(repo_root, &version)?,
        }),
        RunAction::IrToG8rAig {
            ir_action_id,
            top_fn_name,
            fraig,
            version,
            driver,
        } => Ok(ActionSpec::DriverIrToG8rAig {
            ir_action_id,
            top_fn_name,
            fraig,
            lowering_mode: crate::model::G8rLoweringMode::Default,
            runtime: driver.into_runtime(repo_root, &version)?,
            version,
        }),
        RunAction::IrToComboVerilog {
            ir_action_id,
            top_fn_name,
            use_system_verilog,
            version,
            driver,
        } => Ok(ActionSpec::IrFnToCombinationalVerilog {
            ir_action_id,
            top_fn_name,
            use_system_verilog,
            runtime: driver.into_runtime(repo_root, &version)?,
            version,
        }),
        RunAction::IrToKBoolConeCorpus {
            ir_action_id,
            top_fn_name,
            k,
            max_ir_ops,
            version,
            driver,
        } => Ok(ActionSpec::IrFnToKBoolConeCorpus {
            ir_action_id,
            top_fn_name,
            k,
            max_ir_ops: max_ir_ops.or_else(|| crate::default_k_bool_cone_max_ir_ops_for_k(k)),
            runtime: driver.into_runtime(repo_root, &version)?,
            version,
        }),
        RunAction::ComboVerilogToYosysAbcAig {
            verilog_action_id,
            verilog_top_module_name,
            yosys_script,
            yosys,
        } => Ok(ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id,
            verilog_top_module_name,
            yosys_script_ref: make_script_ref(repo_root, &yosys_script)?,
            runtime: yosys.into_runtime(),
        }),
        RunAction::AigToYosysAbcAig {
            aig_action_id,
            yosys_script,
            yosys,
        } => Ok(ActionSpec::AigToYosysAbcAig {
            aig_action_id,
            yosys_script_ref: make_script_ref(repo_root, &yosys_script)?,
            runtime: yosys.into_runtime(),
        }),
        RunAction::AigToStats {
            aig_action_id,
            version,
            driver,
        } => {
            let source_runtime = driver.into_runtime(repo_root, &version)?;
            let runtime = resolve_driver_runtime_for_aig_stats(repo_root, &source_runtime)
                .unwrap_or(source_runtime);
            Ok(ActionSpec::DriverAigToStats {
                aig_action_id,
                version: version.clone(),
                runtime,
            })
        }
        RunAction::AigStatDiff {
            opt_ir_action_id,
            g8r_aig_stats_action_id,
            yosys_abc_aig_stats_action_id,
        } => Ok(ActionSpec::AigStatDiff {
            opt_ir_action_id,
            g8r_aig_stats_action_id,
            yosys_abc_aig_stats_action_id,
        }),
    }
}

pub(crate) fn drain_queue(
    store: &ArtifactStore,
    repo_root: &Path,
    limit: Option<usize>,
    worker_id: &str,
    lease_seconds: i64,
    reclaim_expired: bool,
    pause_claiming: Option<&AtomicBool>,
) -> Result<usize> {
    if lease_seconds <= 0 {
        bail!("--lease-seconds must be > 0, got {}", lease_seconds);
    }
    maybe_prepare_queue_runtime_environment(store, repo_root)?;
    if reclaim_expired {
        reclaim_expired_running_leases(store)?;
    }

    let mut drained = 0_usize;
    let mut failed = 0_usize;
    let mut canceled = 0_usize;
    loop {
        if let Some(limit) = limit
            && drained >= limit
        {
            break;
        }
        if let Some(flag) = pause_claiming
            && flag.load(Ordering::Relaxed)
        {
            break;
        }

        let claimed = claim_next_pending_item(store, worker_id, lease_seconds)?;
        let Some(running) = claimed else {
            break;
        };

        let mut running_batch = vec![running];
        if let Some(batch_key) = action_batch_key(running_batch[0].action()) {
            let max_extra = limit
                .map(|max| max.saturating_sub(drained.saturating_add(1)))
                .unwrap_or(0);
            let compatible_limit = compatible_batch_claim_limit(max_extra);
            if compatible_limit > 0 {
                running_batch.extend(claim_compatible_pending_items(
                    store,
                    worker_id,
                    lease_seconds,
                    &batch_key,
                    running_batch[0].priority(),
                    compatible_limit,
                )?);
            }
        }

        if running_batch.len() > 1 {
            let batch_actions = running_batch
                .iter()
                .map(|running| running.action().clone())
                .collect();
            let batch_results = match execute_action_batch(store, batch_actions) {
                Ok(results) => results,
                Err(err) => {
                    let error_text = format!("{:#}", err);
                    for running in &running_batch {
                        write_failed_record(store, running, worker_id, &error_text)?;
                        remove_file_if_exists(&running.path)?;
                        let canceled_now = cancel_downstream_pending_actions(
                            store,
                            running.action_id(),
                            worker_id,
                        )?;
                        failed += 1;
                        canceled += canceled_now;
                    }
                    continue;
                }
            };
            let result_by_action_id: HashMap<_, _> = batch_results
                .into_iter()
                .map(|result| (result.action_id.clone(), result))
                .collect();
            for running in running_batch {
                let Some(result) = result_by_action_id.get(running.action_id()) else {
                    let error_text = format!(
                        "batch execution omitted result for action {}",
                        running.action_id()
                    );
                    write_failed_record(store, &running, worker_id, &error_text)?;
                    remove_file_if_exists(&running.path)?;
                    let canceled_now =
                        cancel_downstream_pending_actions(store, running.action_id(), worker_id)?;
                    failed += 1;
                    canceled += canceled_now;
                    continue;
                };
                if let Some(output_artifact) = result.output_artifact.clone() {
                    finalize_successful_queue_action(
                        store,
                        &running,
                        output_artifact,
                        worker_id,
                        || {
                            if let Err(err) = note_completed_action_for_previous_loss_k_cone_policy(
                                store,
                                running.action_id(),
                            ) {
                                eprintln!(
                                    "queue action {} succeeded but failed to refresh k-bool loss cache: {:#}",
                                    running.action_id(),
                                    err
                                );
                            }
                            if let Err(err) = enqueue_suggested_actions(
                                store,
                                repo_root,
                                running.action_id(),
                                false,
                                1,
                                running.priority(),
                            ) {
                                eprintln!(
                                    "queue action {} succeeded but failed to enqueue suggested actions: {:#}",
                                    running.action_id(),
                                    err
                                );
                            }
                        },
                    )?;
                    drained += 1;
                } else {
                    let error_text = result
                        .error
                        .as_deref()
                        .unwrap_or("batched execution failed without an error payload")
                        .to_string();
                    write_failed_record(store, &running, worker_id, &error_text)?;
                    remove_file_if_exists(&running.path)?;
                    let canceled_now =
                        cancel_downstream_pending_actions(store, running.action_id(), worker_id)?;
                    failed += 1;
                    canceled += canceled_now;
                }
            }
            continue;
        }

        let action_result = execute_action(store, running_batch[0].action().clone());
        match action_result {
            Ok((_action_id, output_artifact)) => {
                finalize_successful_queue_action(
                    store,
                    &running_batch[0],
                    output_artifact,
                    worker_id,
                    || {
                        if let Err(err) = note_completed_action_for_previous_loss_k_cone_policy(
                            store,
                            running_batch[0].action_id(),
                        ) {
                            eprintln!(
                                "queue action {} succeeded but failed to refresh k-bool loss cache: {:#}",
                                running_batch[0].action_id(),
                                err
                            );
                        }
                        if let Err(err) = enqueue_suggested_actions(
                            store,
                            repo_root,
                            running_batch[0].action_id(),
                            false,
                            1,
                            running_batch[0].priority(),
                        ) {
                            eprintln!(
                                "queue action {} succeeded but failed to enqueue suggested actions: {:#}",
                                running_batch[0].action_id(),
                                err
                            );
                        }
                    },
                )?;
                drained += 1;
            }
            Err(err) => {
                let error_text = format!("{:#}", err);
                write_failed_record(store, &running_batch[0], worker_id, &error_text)?;
                remove_file_if_exists(&running_batch[0].path)?;
                let canceled_now = cancel_downstream_pending_actions(
                    store,
                    running_batch[0].action_id(),
                    worker_id,
                )?;
                failed += 1;
                canceled += canceled_now;
                eprintln!(
                    "queue action {} failed; marked failed and canceled {} downstream action(s)",
                    running_batch[0].action_id(),
                    canceled_now
                );
            }
        }
    }

    if failed > 0 {
        eprintln!(
            "drain-queue summary: successes={}, failed={}, canceled={}",
            drained, failed, canceled
        );
    }
    Ok(drained)
}

fn finalize_successful_queue_action<F>(
    store: &ArtifactStore,
    running: &QueueRunningWithPath,
    output_artifact: ArtifactRef,
    worker_id: &str,
    before_mark_done: F,
) -> Result<()>
where
    F: FnOnce(),
{
    // Keep the running lease visible until post-success bookkeeping finishes so
    // queue status does not briefly report the worker as idle while it is still
    // expanding follow-up work.
    before_mark_done();
    write_done_record(store, running, output_artifact, worker_id)?;
    remove_file_if_exists(&running.path)?;
    Ok(())
}

pub(crate) fn build_suggested_report(
    store: &ArtifactStore,
    root_action_id: &str,
    recursive: bool,
    max_depth: u32,
) -> Result<SuggestedReport> {
    let _root = store.load_provenance(root_action_id).with_context(|| {
        format!(
            "cannot build suggested report; root action provenance missing: {}",
            root_action_id
        )
    })?;

    let mut nodes = Vec::new();
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back((root_action_id.to_string(), 0_u32));

    while let Some((action_id, depth)) = queue.pop_front() {
        if !visited.insert(action_id.clone()) {
            continue;
        }
        let provenance = store.load_provenance(&action_id)?;
        let mut statuses = Vec::new();
        for suggested in &provenance.suggested_next_actions {
            let completed = store.action_exists(&suggested.action_id);
            let queue_state = queue_state_for_action(store, &suggested.action_id).as_label();
            statuses.push(SuggestedStatus {
                reason: suggested.reason.clone(),
                action_id: suggested.action_id.clone(),
                completed,
                queue_state,
                action: suggested.action.clone(),
            });
            if recursive && depth < max_depth && completed {
                queue.push_back((suggested.action_id.clone(), depth + 1));
            }
        }
        nodes.push(SuggestedNode {
            source_action_id: action_id,
            depth,
            suggestions: statuses,
        });
    }

    Ok(SuggestedReport {
        root_action_id: root_action_id.to_string(),
        recursive,
        max_depth,
        nodes,
    })
}

pub(crate) fn build_suggested_audit_report(
    store: &ArtifactStore,
    include_completed: bool,
) -> Result<SuggestedAuditReport> {
    let mut entries = Vec::new();
    let mut total_sources = 0_usize;
    let mut total_suggestions = 0_usize;
    let mut completed_suggestions = 0_usize;

    store.for_each_provenance(|provenance| {
        total_sources += 1;
        for suggested in &provenance.suggested_next_actions {
            total_suggestions += 1;
            let completed = store.action_exists(&suggested.action_id);
            if completed {
                completed_suggestions += 1;
            }
            if include_completed || !completed {
                entries.push(SuggestedAuditEntry {
                    source_action_id: provenance.action_id.clone(),
                    reason: suggested.reason.clone(),
                    action_id: suggested.action_id.clone(),
                    completed,
                    queue_state: queue_state_for_action(store, &suggested.action_id).as_label(),
                    action: suggested.action.clone(),
                });
            }
        }
        Ok(std::ops::ControlFlow::Continue(()))
    })?;
    entries.sort_by(|a, b| {
        a.source_action_id
            .cmp(&b.source_action_id)
            .then(a.action_id.cmp(&b.action_id))
    });
    let missing_suggestions = total_suggestions.saturating_sub(completed_suggestions);
    Ok(SuggestedAuditReport {
        total_source_actions: total_sources,
        total_suggestions,
        completed_suggestions,
        missing_suggestions,
        entries,
    })
}

pub(crate) fn enqueue_suggested_actions(
    store: &ArtifactStore,
    repo_root: &Path,
    root_action_id: &str,
    recursive: bool,
    max_depth: u32,
    priority: i32,
) -> Result<EnqueueSuggestedSummary> {
    enqueue_suggested_actions_with_policy(
        store,
        repo_root,
        root_action_id,
        recursive,
        max_depth,
        priority,
        SuggestedEnqueuePolicy::from_env(),
    )
}

fn enqueue_suggested_actions_with_policy(
    store: &ArtifactStore,
    repo_root: &Path,
    root_action_id: &str,
    recursive: bool,
    max_depth: u32,
    priority: i32,
    policy: SuggestedEnqueuePolicy,
) -> Result<EnqueueSuggestedSummary> {
    let _root = store.load_provenance(root_action_id).with_context(|| {
        format!(
            "cannot enqueue suggestions; root action provenance missing: {}",
            root_action_id
        )
    })?;

    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back((root_action_id.to_string(), 0_u32));

    let mut total_suggestions = 0_usize;
    let mut enqueued_count = 0_usize;
    let mut already_done_count = 0_usize;
    let mut already_pending_count = 0_usize;
    let mut already_running_count = 0_usize;
    let mut already_failed_count = 0_usize;
    let mut already_canceled_count = 0_usize;
    let mut skipped_blocked_count = 0_usize;
    let mut skipped_not_previously_lossy_k_bool_count = 0_usize;
    let unknown_queue_state_count = 0_usize;

    while let Some((source_action_id, depth)) = queue.pop_front() {
        if !visited.insert(source_action_id.clone()) {
            continue;
        }
        let provenance = store.load_provenance(&source_action_id)?;
        for suggested in &provenance.suggested_next_actions {
            total_suggestions += 1;
            let action = canonicalize_suggested_action(store, repo_root, &suggested.action);
            let suggested_priority = suggested_action_queue_priority(priority, &action);
            match classify_suggested_action_skip_reason(store, &action, policy)? {
                Some(SuggestedActionSkipReason::DisabledDriverIrAigEquiv) => {
                    skipped_blocked_count += 1;
                    continue;
                }
                Some(SuggestedActionSkipReason::NotPreviouslyLossyKBool) => {
                    skipped_not_previously_lossy_k_bool_count += 1;
                    continue;
                }
                None => {}
            }
            let action_id = compute_action_id(&action)?;
            if store.action_exists(&action_id) {
                already_done_count += 1;
            } else {
                match queue_state_for_action(store, &action_id) {
                    QueueState::Pending => {
                        enqueue_action_with_priority(store, action.clone(), suggested_priority)?;
                        already_pending_count += 1;
                    }
                    QueueState::Running { .. } => already_running_count += 1,
                    QueueState::Failed => already_failed_count += 1,
                    QueueState::Canceled => already_canceled_count += 1,
                    QueueState::None => match classify_action_readiness(store, &action)? {
                        ActionReadiness::Blocked { .. } => skipped_blocked_count += 1,
                        ActionReadiness::Ready | ActionReadiness::NotReady => {
                            enqueue_action_with_priority(store, action, suggested_priority)?;
                            enqueued_count += 1;
                        }
                    },
                    QueueState::Done => already_done_count += 1,
                }
            }

            if recursive
                && depth < max_depth
                && let Some(next_source_action_id) = recursive_source_action_id_for_suggestion(
                    store,
                    &action_id,
                    &suggested.action_id,
                )
            {
                queue.push_back((next_source_action_id, depth + 1));
            }
        }
    }

    Ok(EnqueueSuggestedSummary {
        root_action_id: root_action_id.to_string(),
        recursive,
        max_depth,
        visited_source_actions: visited.len(),
        total_suggestions,
        enqueued_count,
        already_done_count,
        already_pending_count,
        already_running_count,
        already_failed_count,
        already_canceled_count,
        skipped_blocked_count,
        skipped_not_previously_lossy_k_bool_count,
        unknown_queue_state_count,
    })
}

pub(crate) fn enqueue_missing_suggested_actions(
    store: &ArtifactStore,
    repo_root: &Path,
    dry_run: bool,
) -> Result<serde_json::Value> {
    enqueue_missing_suggested_actions_with_policy(
        store,
        repo_root,
        dry_run,
        SuggestedEnqueuePolicy::from_env(),
    )
}

fn enqueue_missing_suggested_actions_with_policy(
    store: &ArtifactStore,
    repo_root: &Path,
    dry_run: bool,
    policy: SuggestedEnqueuePolicy,
) -> Result<serde_json::Value> {
    let audit = build_suggested_audit_report(store, false)?;
    let mut total_missing_entries = 0_usize;
    let mut visited_action_ids = HashSet::new();
    let mut unique_missing_actions = 0_usize;
    let mut duplicate_missing_entries = 0_usize;
    let mut enqueued_count = 0_usize;
    let mut already_done_count = 0_usize;
    let mut already_pending_count = 0_usize;
    let mut already_running_count = 0_usize;
    let mut already_failed_count = 0_usize;
    let mut already_canceled_count = 0_usize;
    let mut skipped_blocked_count = 0_usize;
    let mut skipped_not_previously_lossy_k_bool_count = 0_usize;

    for entry in audit.entries {
        if entry.completed {
            continue;
        }
        total_missing_entries += 1;
        let action = canonicalize_suggested_action(store, repo_root, &entry.action);
        match classify_suggested_action_skip_reason(store, &action, policy)? {
            Some(SuggestedActionSkipReason::DisabledDriverIrAigEquiv) => {
                skipped_blocked_count += 1;
                continue;
            }
            Some(SuggestedActionSkipReason::NotPreviouslyLossyKBool) => {
                skipped_not_previously_lossy_k_bool_count += 1;
                continue;
            }
            None => {}
        }
        let action_id = compute_action_id(&action)?;
        if !visited_action_ids.insert(action_id.clone()) {
            duplicate_missing_entries += 1;
            continue;
        }
        unique_missing_actions += 1;
        if store.action_exists(&action_id) {
            already_done_count += 1;
            continue;
        }
        match queue_state_for_action(store, &action_id) {
            QueueState::Pending => {
                already_pending_count += 1;
            }
            QueueState::Running { .. } => {
                already_running_count += 1;
            }
            QueueState::Done => {
                already_done_count += 1;
            }
            QueueState::Failed => {
                already_failed_count += 1;
            }
            QueueState::Canceled => {
                already_canceled_count += 1;
            }
            QueueState::None => match classify_action_readiness(store, &action)? {
                ActionReadiness::Blocked { .. } => {
                    skipped_blocked_count += 1;
                }
                ActionReadiness::Ready | ActionReadiness::NotReady => {
                    if !dry_run {
                        enqueue_action(store, action)?;
                    }
                    enqueued_count += 1;
                }
            },
        }
    }

    Ok(json!({
        "dry_run": dry_run,
        "missing_entries": total_missing_entries,
        "unique_missing_actions": unique_missing_actions,
        "duplicate_missing_entries": duplicate_missing_entries,
        "enqueued_count": enqueued_count,
        "already_done_count": already_done_count,
        "already_pending_count": already_pending_count,
        "already_running_count": already_running_count,
        "already_failed_count": already_failed_count,
        "already_canceled_count": already_canceled_count,
        "skipped_blocked_count": skipped_blocked_count,
        "skipped_not_previously_lossy_k_bool_count": skipped_not_previously_lossy_k_bool_count
    }))
}

pub(crate) fn recursive_source_action_id_for_suggestion(
    store: &ArtifactStore,
    canonicalized_action_id: &str,
    original_action_id: &str,
) -> Option<String> {
    if store.action_exists(canonicalized_action_id) {
        return Some(canonicalized_action_id.to_string());
    }
    if canonicalized_action_id != original_action_id && store.action_exists(original_action_id) {
        return Some(original_action_id.to_string());
    }
    None
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SuggestedActionSkipReason {
    DisabledDriverIrAigEquiv,
    NotPreviouslyLossyKBool,
}

fn classify_suggested_action_skip_reason(
    store: &ArtifactStore,
    action: &ActionSpec,
    policy: SuggestedEnqueuePolicy,
) -> Result<Option<SuggestedActionSkipReason>> {
    if should_skip_suggested_action(action) {
        return Ok(Some(SuggestedActionSkipReason::DisabledDriverIrAigEquiv));
    }
    if should_skip_not_previously_lossy_k_bool_suggested_action(store, action, policy)? {
        return Ok(Some(SuggestedActionSkipReason::NotPreviouslyLossyKBool));
    }
    Ok(None)
}

fn should_skip_suggested_action(action: &ActionSpec) -> bool {
    matches!(action, ActionSpec::DriverIrAigEquiv { .. }) && !driver_ir_aig_equiv_enabled()
}

fn should_skip_not_previously_lossy_k_bool_suggested_action(
    store: &ArtifactStore,
    action: &ActionSpec,
    policy: SuggestedEnqueuePolicy,
) -> Result<bool> {
    if !policy.only_previous_loss_k_cones {
        return Ok(false);
    }

    let ActionSpec::IrFnToKBoolConeCorpus {
        ir_action_id, k, ..
    } = action
    else {
        return Ok(false);
    };
    let source_provenance = match store.load_provenance(ir_action_id) {
        Ok(provenance) => provenance,
        Err(err) => {
            eprintln!(
                "warning: skipping k-bool suggestion {}; unable to load source opt provenance {}: {:#}",
                compute_action_id(action).unwrap_or_else(|_| "<unknown>".to_string()),
                ir_action_id,
                err
            );
            return Ok(true);
        }
    };
    let Some(source_structural_hash) = source_provenance
        .details
        .get("output_ir_fn_structural_hash")
        .and_then(|v| v.as_str())
        .and_then(normalized_structural_hash)
    else {
        return Ok(true);
    };

    let lossy_hashes = load_complete_previously_lossy_k_bool_source_hashes(store, *k)?;
    Ok(!lossy_hashes.contains(&source_structural_hash))
}

fn with_k_bool_previous_loss_cache_for_store<T>(
    store: &ArtifactStore,
    f: impl FnOnce(&mut KBoolPreviousLossCache) -> T,
) -> Result<T> {
    let cache_mutex =
        K_BOOL_PREVIOUS_LOSS_CACHE.get_or_init(|| Mutex::new(KBoolPreviousLossCache::default()));
    let mut cache = cache_mutex
        .lock()
        .map_err(|_| anyhow::anyhow!("k-bool previous loss cache lock poisoned"))?;
    let store_identity = store.queue_root();
    if cache.store_identity.as_ref() != Some(&store_identity) {
        cache.store_identity = Some(store_identity);
        cache.complete_by_k.clear();
    }
    Ok(f(&mut cache))
}

fn load_complete_previously_lossy_k_bool_source_hashes(
    store: &ArtifactStore,
    k: u32,
) -> Result<HashSet<String>> {
    if let Some(cached) = with_k_bool_previous_loss_cache_for_store(store, |cache| {
        cache.complete_by_k.get(&k).cloned()
    })? {
        return Ok(cached);
    }

    let lossy_hashes = load_previously_lossy_k_bool_source_structural_hashes(store, k)?;
    with_k_bool_previous_loss_cache_for_store(store, |cache| {
        cache
            .complete_by_k
            .entry(k)
            .or_insert_with(|| lossy_hashes.clone());
    })?;
    Ok(lossy_hashes)
}

fn note_completed_action_for_previous_loss_k_cone_policy(
    store: &ArtifactStore,
    action_id: &str,
) -> Result<()> {
    if K_BOOL_PREVIOUS_LOSS_CACHE.get().is_none() && !queue_only_previous_loss_k_cones_enabled() {
        return Ok(());
    }
    let Some(provenance) = store.load_provenance(action_id).ok() else {
        return Ok(());
    };
    let Some((k, structural_hash)) =
        lossy_k_bool_source_structural_hash_for_aig_stat_diff(store, &provenance)
    else {
        return Ok(());
    };
    with_k_bool_previous_loss_cache_for_store(store, |cache| {
        if let Some(lossy_hashes) = cache.complete_by_k.get_mut(&k) {
            lossy_hashes.insert(structural_hash);
        }
    })?;
    Ok(())
}

pub(crate) fn canonicalize_suggested_action(
    store: &ArtifactStore,
    repo_root: &Path,
    action: &ActionSpec,
) -> ActionSpec {
    let upgraded = match action {
        ActionSpec::DriverAigToStats {
            aig_action_id,
            version,
            runtime,
        } => {
            let runtime = resolve_driver_runtime_for_aig_stats(repo_root, runtime)
                .unwrap_or_else(|_| runtime.clone());
            ActionSpec::DriverAigToStats {
                aig_action_id: aig_action_id.clone(),
                version: version.clone(),
                runtime,
            }
        }
        ActionSpec::DriverIrAigEquiv {
            ir_action_id,
            aig_action_id,
            top_fn_name,
            version,
            runtime,
        } => {
            let runtime = resolve_driver_runtime_for_aig_stats(repo_root, runtime)
                .unwrap_or_else(|_| runtime.clone());
            ActionSpec::DriverIrAigEquiv {
                ir_action_id: ir_action_id.clone(),
                aig_action_id: aig_action_id.clone(),
                top_fn_name: top_fn_name.clone(),
                version: version.clone(),
                runtime,
            }
        }
        _ => action.clone(),
    };
    canonicalize_k3_cone_suggested_action(store, &upgraded).unwrap_or(upgraded)
}

fn canonicalize_k3_cone_suggested_action(
    store: &ArtifactStore,
    action: &ActionSpec,
) -> Result<ActionSpec> {
    fn resolve_candidate_fields(
        action: &ActionSpec,
    ) -> Option<(String, String, String, DriverRuntimeSpec)> {
        match action {
            ActionSpec::DriverIrToG8rAig {
                ir_action_id,
                top_fn_name,
                version,
                runtime,
                ..
            } => top_fn_name.as_ref().map(|top| {
                (
                    ir_action_id.clone(),
                    top.clone(),
                    version.clone(),
                    runtime.clone(),
                )
            }),
            ActionSpec::IrFnToCombinationalVerilog {
                ir_action_id,
                top_fn_name,
                version,
                runtime,
                ..
            } => top_fn_name.as_ref().map(|top| {
                (
                    ir_action_id.clone(),
                    top.clone(),
                    version.clone(),
                    runtime.clone(),
                )
            }),
            _ => None,
        }
    }

    let Some((source_ir_action_id, top_fn_name, version, runtime)) =
        resolve_candidate_fields(action)
    else {
        return Ok(action.clone());
    };
    if !top_fn_name.starts_with("__k3_cone_") {
        return Ok(action.clone());
    }

    let cache_mutex =
        K3_CONE_CANONICAL_CACHE.get_or_init(|| Mutex::new(K3ConeCanonicalCache::default()));

    for attempt in 0..2 {
        let mut cache = cache_mutex
            .lock()
            .map_err(|_| anyhow::anyhow!("k3 cone canonical cache lock poisoned"))?;
        if attempt == 0 && cache.cone_hash_by_action_and_top.is_empty() {
            *cache = build_k3_cone_canonical_cache(store)?;
        } else if attempt == 1 {
            *cache = build_k3_cone_canonical_cache(store)?;
        }

        let hash_key = (source_ir_action_id.clone(), top_fn_name.clone());
        let Some(structural_hash) = cache.cone_hash_by_action_and_top.get(&hash_key).cloned()
        else {
            continue;
        };
        let key = K3ConeCanonicalKey {
            structural_hash,
            version: version.clone(),
            runtime: runtime.clone(),
        };
        let Some(canonical_ir_action_id) = cache.canonical_ir_action_by_key.get(&key).cloned()
        else {
            continue;
        };
        drop(cache);

        if canonical_ir_action_id == source_ir_action_id {
            return Ok(action.clone());
        }

        let rewritten = match action {
            ActionSpec::DriverIrToG8rAig {
                top_fn_name,
                fraig,
                lowering_mode,
                version,
                runtime,
                ..
            } => ActionSpec::DriverIrToG8rAig {
                ir_action_id: canonical_ir_action_id,
                top_fn_name: top_fn_name.clone(),
                fraig: *fraig,
                lowering_mode: lowering_mode.clone(),
                version: version.clone(),
                runtime: runtime.clone(),
            },
            ActionSpec::IrFnToCombinationalVerilog {
                top_fn_name,
                use_system_verilog,
                version,
                runtime,
                ..
            } => ActionSpec::IrFnToCombinationalVerilog {
                ir_action_id: canonical_ir_action_id,
                top_fn_name: top_fn_name.clone(),
                use_system_verilog: *use_system_verilog,
                version: version.clone(),
                runtime: runtime.clone(),
            },
            _ => action.clone(),
        };
        return Ok(rewritten);
    }

    Ok(action.clone())
}

fn build_k3_cone_canonical_cache(store: &ArtifactStore) -> Result<K3ConeCanonicalCache> {
    let provenances = store.list_provenances()?;
    let mut cone_hash_by_action_and_top = HashMap::new();
    let mut canonical_ir_action_by_key = HashMap::new();

    for provenance in provenances {
        let ActionSpec::IrFnToKBoolConeCorpus {
            k,
            version,
            runtime,
            ..
        } = &provenance.action
        else {
            continue;
        };
        if *k != 3 {
            continue;
        }

        let default_manifest_relpath = format!("payload/k_bool_cones_k{}_manifest.json", k);
        let manifest_relpath = provenance
            .details
            .get("output_manifest_relpath")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or(default_manifest_relpath);
        let manifest_ref = ArtifactRef {
            action_id: provenance.action_id.clone(),
            artifact_type: ArtifactType::IrPackageFile,
            relpath: manifest_relpath,
        };
        let manifest_path = store.resolve_artifact_ref_path(&manifest_ref);
        if !manifest_path.exists() || !manifest_path.is_file() {
            continue;
        }
        let Ok(manifest_text) = fs::read_to_string(&manifest_path) else {
            continue;
        };
        let Ok(manifest) = serde_json::from_str::<KBoolConeCorpusManifest>(&manifest_text) else {
            continue;
        };

        for entry in manifest.entries {
            let Some(structural_hash) = normalized_structural_hash(&entry.structural_hash) else {
                continue;
            };
            let top = entry.fn_name.trim().to_string();
            if top.is_empty() {
                continue;
            }
            cone_hash_by_action_and_top.insert(
                (provenance.action_id.clone(), top.clone()),
                structural_hash.clone(),
            );

            let key = K3ConeCanonicalKey {
                structural_hash,
                version: version.clone(),
                runtime: runtime.clone(),
            };
            match canonical_ir_action_by_key.get(&key) {
                Some(existing) => {
                    if provenance.action_id < *existing {
                        canonical_ir_action_by_key.insert(key, provenance.action_id.clone());
                    }
                }
                None => {
                    canonical_ir_action_by_key.insert(key, provenance.action_id.clone());
                }
            }
        }
    }

    Ok(K3ConeCanonicalCache {
        cone_hash_by_action_and_top,
        canonical_ir_action_by_key,
    })
}

pub(crate) fn find_aig_stat_diffs_for_opt_ir(
    store: &ArtifactStore,
    opt_ir_action_id: &str,
) -> Result<AigStatDiffQueryReport> {
    let mut diffs = Vec::new();
    store.for_each_provenance(|provenance| {
        if let ActionSpec::AigStatDiff {
            opt_ir_action_id: diff_opt_ir_action_id,
            ..
        } = &provenance.action
            && diff_opt_ir_action_id == opt_ir_action_id
        {
            diffs.push(AigStatDiffQueryItem {
                action_id: provenance.action_id,
                output_artifact: provenance.output_artifact,
            });
        }
        Ok(std::ops::ControlFlow::Continue(()))
    })?;
    diffs.sort_by(|a, b| a.action_id.cmp(&b.action_id));
    Ok(AigStatDiffQueryReport {
        opt_ir_action_id: opt_ir_action_id.to_string(),
        count: diffs.len(),
        diffs,
    })
}

pub(crate) fn backfill_stdlib_opt_ir_aig_equiv_suggestions(
    store: &ArtifactStore,
    repo_root: &Path,
    enqueue: bool,
    dry_run: bool,
) -> Result<serde_json::Value> {
    #[derive(Clone)]
    struct StdlibOptBackfillItem {
        provenance: Provenance,
        candidates: Vec<SuggestedAction>,
    }

    let mut scanned_opt_actions = 0_usize;
    let mut scoped_stdlib_opt_actions = 0_usize;
    let mut skipped_non_stdlib_parent = 0_usize;
    let mut skipped_missing_parent = 0_usize;
    let mut skipped_missing_ir_top = 0_usize;
    let mut ir_aig_equiv_quarantined = 0_usize;
    let mut ir_aig_equiv_supported = 0_usize;
    let mut ir_aig_equiv_unsupported = 0_usize;
    let mut ir_aig_equiv_probe_errors = 0_usize;
    let mut ir_aig_equiv_runtime_upgraded = 0_usize;
    let mut updated_provenance_count = 0_usize;
    let mut total_candidate_suggestions = 0_usize;
    let mut total_added_suggestions = 0_usize;
    let mut total_removed_suggestions = 0_usize;
    let mut backfill_items = Vec::new();
    let ir_aig_equiv_enabled = driver_ir_aig_equiv_enabled();

    store.for_each_provenance(|provenance| {
        let (ir_action_id, top_fn_name, version, runtime) = match &provenance.action {
            ActionSpec::DriverIrToOpt {
                ir_action_id,
                top_fn_name,
                version,
                runtime,
                ..
            } => (
                ir_action_id.clone(),
                top_fn_name.clone(),
                version.clone(),
                runtime.clone(),
            ),
            _ => return Ok(std::ops::ControlFlow::Continue(())),
        };
        scanned_opt_actions += 1;

        let parent = match store.load_provenance(&ir_action_id) {
            Ok(parent) => parent,
            Err(_) => {
                skipped_missing_parent += 1;
                return Ok(std::ops::ControlFlow::Continue(()));
            }
        };

        let dslx_file = match parent.action {
            ActionSpec::DriverDslxFnToIr { dslx_file, .. } => dslx_file,
            _ => {
                skipped_non_stdlib_parent += 1;
                return Ok(std::ops::ControlFlow::Continue(()));
            }
        };
        if !dslx_file.starts_with("xls/dslx/stdlib/") {
            skipped_non_stdlib_parent += 1;
            return Ok(std::ops::ControlFlow::Continue(()));
        }
        scoped_stdlib_opt_actions += 1;

        let ir_top = provenance
            .details
            .get("ir_top")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or(top_fn_name);
        let Some(ir_top) = ir_top else {
            skipped_missing_ir_top += 1;
            return Ok(std::ops::ControlFlow::Continue(()));
        };

        let candidates = if ir_aig_equiv_enabled {
            let equiv_runtime = resolve_driver_runtime_for_aig_stats(repo_root, &runtime)
                .unwrap_or_else(|_| runtime.clone());
            if equiv_runtime != runtime {
                ir_aig_equiv_runtime_upgraded += 1;
            }

            let mut supported = false;
            match resolve_driver_ir_aig_equiv_supported(store, repo_root, &version, &equiv_runtime)
            {
                Ok(true) => {
                    ir_aig_equiv_supported += 1;
                    supported = true;
                }
                Ok(false) => {
                    ir_aig_equiv_unsupported += 1;
                }
                Err(_) => {
                    ir_aig_equiv_probe_errors += 1;
                    return Ok(std::ops::ControlFlow::Continue(()));
                }
            }

            if !supported {
                Vec::new()
            } else {
                let no_fraig_g8r = ActionSpec::DriverIrToG8rAig {
                    ir_action_id: provenance.action_id.clone(),
                    top_fn_name: None,
                    fraig: false,
                    lowering_mode: crate::model::G8rLoweringMode::Default,
                    version: version.clone(),
                    runtime: runtime.clone(),
                };
                let fraig_g8r = ActionSpec::DriverIrToG8rAig {
                    ir_action_id: provenance.action_id.clone(),
                    top_fn_name: None,
                    fraig: true,
                    lowering_mode: crate::model::G8rLoweringMode::Default,
                    version: version.clone(),
                    runtime: runtime.clone(),
                };
                let no_fraig_g8r_action_id = compute_action_id(&no_fraig_g8r)?;
                let fraig_g8r_action_id = compute_action_id(&fraig_g8r)?;
                build_opt_ir_aig_equiv_suggestions(
                    &provenance.action_id,
                    &ir_top,
                    &no_fraig_g8r_action_id,
                    &fraig_g8r_action_id,
                    &version,
                    &equiv_runtime,
                )?
            }
        } else {
            ir_aig_equiv_quarantined += 1;
            Vec::new()
        };

        backfill_items.push(StdlibOptBackfillItem {
            provenance,
            candidates,
        });
        Ok(std::ops::ControlFlow::Continue(()))
    })?;

    backfill_items.sort_by(|a, b| {
        a.provenance
            .created_utc
            .cmp(&b.provenance.created_utc)
            .then(a.provenance.action_id.cmp(&b.provenance.action_id))
    });

    let mut enqueued_count = 0_usize;
    let mut already_done_count = 0_usize;
    let mut already_pending_count = 0_usize;
    let mut already_running_count = 0_usize;
    let mut already_failed_count = 0_usize;
    let mut already_canceled_count = 0_usize;
    let mut deduped_candidate_action_ids = HashSet::new();

    for item in backfill_items {
        let candidates = item.candidates.clone();
        total_candidate_suggestions += candidates.len();

        let mut updated = item.provenance.clone();
        let before_len = updated.suggested_next_actions.len();
        updated.suggested_next_actions.retain(|suggested| {
            !matches!(
                &suggested.action,
                ActionSpec::DriverIrAigEquiv { ir_action_id, .. }
                    if ir_action_id == &item.provenance.action_id
            )
        });
        let removed = before_len.saturating_sub(updated.suggested_next_actions.len());
        let mut existing_ids: HashSet<String> = updated
            .suggested_next_actions
            .iter()
            .map(|s| s.action_id.clone())
            .collect();
        let mut added = 0_usize;
        for candidate in &candidates {
            if existing_ids.insert(candidate.action_id.clone()) {
                updated.suggested_next_actions.push(candidate.clone());
                added += 1;
            }
        }
        if removed > 0 || added > 0 {
            total_added_suggestions += added;
            total_removed_suggestions += removed;
            updated_provenance_count += 1;
            if !dry_run {
                store.write_provenance(&updated)?;
            }
        }

        if !enqueue {
            continue;
        }

        for candidate in candidates {
            if !deduped_candidate_action_ids.insert(candidate.action_id.clone()) {
                continue;
            }
            if store.action_exists(&candidate.action_id) {
                already_done_count += 1;
                continue;
            }
            match queue_state_for_action(store, &candidate.action_id) {
                QueueState::Pending => {
                    already_pending_count += 1;
                }
                QueueState::Running { .. } => {
                    already_running_count += 1;
                }
                QueueState::Done => {
                    already_done_count += 1;
                }
                QueueState::Failed => {
                    already_failed_count += 1;
                }
                QueueState::Canceled => {
                    already_canceled_count += 1;
                }
                QueueState::None => {
                    if !dry_run {
                        enqueue_action(store, candidate.action)?;
                    }
                    enqueued_count += 1;
                }
            }
        }
    }

    Ok(json!({
        "dry_run": dry_run,
        "enqueue": enqueue,
        "driver_ir_aig_equiv_enabled": ir_aig_equiv_enabled,
        "scanned_opt_actions": scanned_opt_actions,
        "scoped_stdlib_opt_actions": scoped_stdlib_opt_actions,
        "skipped_non_stdlib_parent": skipped_non_stdlib_parent,
        "skipped_missing_parent": skipped_missing_parent,
        "skipped_missing_ir_top": skipped_missing_ir_top,
        "ir_aig_equiv_quarantined": ir_aig_equiv_quarantined,
        "ir_aig_equiv_supported": ir_aig_equiv_supported,
        "ir_aig_equiv_unsupported": ir_aig_equiv_unsupported,
        "ir_aig_equiv_probe_errors": ir_aig_equiv_probe_errors,
        "ir_aig_equiv_runtime_upgraded": ir_aig_equiv_runtime_upgraded,
        "updated_provenance_count": updated_provenance_count,
        "total_candidate_suggestions": total_candidate_suggestions,
        "total_added_suggestions": total_added_suggestions,
        "total_removed_suggestions": total_removed_suggestions,
        "enqueued_count": enqueued_count,
        "already_done_count": already_done_count,
        "already_pending_count": already_pending_count,
        "already_running_count": already_running_count,
        "already_failed_count": already_failed_count,
        "already_canceled_count": already_canceled_count
    }))
}

pub(crate) fn backfill_opt_ir_frontend_compare_suggestions(
    store: &ArtifactStore,
    enqueue: bool,
    dry_run: bool,
    priority: i32,
    crate_version_filter: Option<&str>,
) -> Result<serde_json::Value> {
    let normalized_filter = crate_version_filter
        .map(normalize_tag_version)
        .map(|v| v.to_string());

    let mut scanned_opt_actions = 0_usize;
    let mut scoped_opt_actions = 0_usize;
    let mut skipped_crate_version_filter = 0_usize;
    let mut updated_provenance_count = 0_usize;
    let mut total_candidate_suggestions = 0_usize;
    let mut total_added_suggestions = 0_usize;
    let mut total_removed_suggestions = 0_usize;

    let mut enqueued_count = 0_usize;
    let mut already_done_count = 0_usize;
    let mut already_pending_count = 0_usize;
    let mut already_running_count = 0_usize;
    let mut already_failed_count = 0_usize;
    let mut already_canceled_count = 0_usize;
    let mut deduped_candidate_action_ids = HashSet::new();

    store.for_each_provenance(|provenance| {
        let (version, runtime) = match &provenance.action {
            ActionSpec::DriverIrToOpt {
                version, runtime, ..
            } => (version.clone(), runtime.clone()),
            _ => return Ok(std::ops::ControlFlow::Continue(())),
        };
        scanned_opt_actions += 1;

        if let Some(filter) = normalized_filter.as_deref() {
            let action_crate_version = normalize_tag_version(&runtime.driver_version);
            if action_crate_version != filter {
                skipped_crate_version_filter += 1;
                return Ok(std::ops::ControlFlow::Continue(()));
            }
        }
        scoped_opt_actions += 1;

        let candidate_action = ActionSpec::DriverIrToG8rAig {
            ir_action_id: provenance.action_id.clone(),
            top_fn_name: None,
            fraig: false,
            lowering_mode: G8rLoweringMode::FrontendNoPrepRewrite,
            version,
            runtime,
        };
        let candidate_action_id = compute_action_id(&candidate_action)?;
        let candidate = SuggestedAction {
            reason: "Run frontend-isolated g8r+ABC path (fraig=false, no prep rewrite)".to_string(),
            action_id: candidate_action_id.clone(),
            action: candidate_action,
        };
        total_candidate_suggestions += 1;

        let mut updated = provenance.clone();
        let before_len = updated.suggested_next_actions.len();
        updated.suggested_next_actions.retain(|suggested| {
            !matches!(
                &suggested.action,
                ActionSpec::DriverIrToG8rAig {
                    ir_action_id,
                    fraig,
                    lowering_mode,
                    ..
                } if ir_action_id == &provenance.action_id
                    && !fraig
                    && *lowering_mode == G8rLoweringMode::FrontendNoPrepRewrite
            )
        });
        let removed = before_len.saturating_sub(updated.suggested_next_actions.len());
        let exists = updated
            .suggested_next_actions
            .iter()
            .any(|suggested| suggested.action_id == candidate_action_id);
        let mut added = 0_usize;
        if !exists {
            updated.suggested_next_actions.push(candidate.clone());
            added = 1;
        }
        if removed > 0 || added > 0 {
            updated_provenance_count += 1;
            total_added_suggestions += added;
            total_removed_suggestions += removed;
            if !dry_run {
                store.write_provenance(&updated)?;
            }
        }

        if enqueue && deduped_candidate_action_ids.insert(candidate_action_id.clone()) {
            if store.action_exists(&candidate_action_id) {
                already_done_count += 1;
            } else {
                match queue_state_for_action(store, &candidate_action_id) {
                    QueueState::Pending => {
                        already_pending_count += 1;
                        if !dry_run {
                            enqueue_action_with_priority(
                                store,
                                candidate.action.clone(),
                                priority,
                            )?;
                        }
                    }
                    QueueState::Running { .. } => already_running_count += 1,
                    QueueState::Done => already_done_count += 1,
                    QueueState::Failed => already_failed_count += 1,
                    QueueState::Canceled => already_canceled_count += 1,
                    QueueState::None => {
                        if !dry_run {
                            enqueue_action_with_priority(
                                store,
                                candidate.action.clone(),
                                priority,
                            )?;
                        }
                        enqueued_count += 1;
                    }
                }
            }
        }
        Ok(std::ops::ControlFlow::Continue(()))
    })?;

    Ok(json!({
        "dry_run": dry_run,
        "enqueue": enqueue,
        "priority": priority,
        "crate_version_filter": normalized_filter,
        "scanned_opt_actions": scanned_opt_actions,
        "scoped_opt_actions": scoped_opt_actions,
        "skipped_crate_version_filter": skipped_crate_version_filter,
        "updated_provenance_count": updated_provenance_count,
        "total_candidate_suggestions": total_candidate_suggestions,
        "total_added_suggestions": total_added_suggestions,
        "total_removed_suggestions": total_removed_suggestions,
        "enqueued_count": enqueued_count,
        "already_done_count": already_done_count,
        "already_pending_count": already_pending_count,
        "already_running_count": already_running_count,
        "already_failed_count": already_failed_count,
        "already_canceled_count": already_canceled_count
    }))
}

pub(crate) fn backfill_k_bool_cone_suggestions(
    store: &ArtifactStore,
    _repo_root: &Path,
    enqueue: bool,
    dry_run: bool,
) -> Result<serde_json::Value> {
    #[derive(Clone)]
    struct KBoolBackfillItem {
        provenance: Provenance,
        version: String,
        runtime: DriverRuntimeSpec,
        manifest: KBoolConeCorpusManifest,
    }

    let mut scanned_k_bool_actions = 0_usize;
    let mut updated_provenance_count = 0_usize;
    let mut manifest_error_count = 0_usize;
    let mut total_suggested_actions = 0_usize;
    let mut total_unique_cone_entries = 0_usize;
    let mut enqueued_count = 0_usize;
    let mut already_done_count = 0_usize;
    let mut already_pending_count = 0_usize;
    let mut already_running_count = 0_usize;
    let mut already_failed_count = 0_usize;
    let mut already_canceled_count = 0_usize;
    let mut backfill_items = Vec::new();

    store.for_each_provenance(|provenance| {
        let (k, version, runtime) = match &provenance.action {
            ActionSpec::IrFnToKBoolConeCorpus {
                k,
                version,
                runtime,
                ..
            } => (*k, version.clone(), runtime.clone()),
            _ => return Ok(std::ops::ControlFlow::Continue(())),
        };
        scanned_k_bool_actions += 1;

        let default_manifest_relpath = format!("payload/k_bool_cones_k{}_manifest.json", k);
        let manifest_relpath = provenance
            .details
            .get("output_manifest_relpath")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or(default_manifest_relpath);
        let manifest_ref = ArtifactRef {
            action_id: provenance.action_id.clone(),
            artifact_type: ArtifactType::IrPackageFile,
            relpath: manifest_relpath,
        };
        let manifest_path = store.resolve_artifact_ref_path(&manifest_ref);
        let manifest_text = match fs::read_to_string(&manifest_path) {
            Ok(text) => text,
            Err(_) => {
                manifest_error_count += 1;
                return Ok(std::ops::ControlFlow::Continue(()));
            }
        };
        let manifest: KBoolConeCorpusManifest = match serde_json::from_str(&manifest_text) {
            Ok(parsed) => parsed,
            Err(_) => {
                manifest_error_count += 1;
                return Ok(std::ops::ControlFlow::Continue(()));
            }
        };
        backfill_items.push(KBoolBackfillItem {
            provenance,
            version,
            runtime,
            manifest,
        });
        Ok(std::ops::ControlFlow::Continue(()))
    })?;

    backfill_items.sort_by(|a, b| {
        a.provenance
            .created_utc
            .cmp(&b.provenance.created_utc)
            .then(a.provenance.action_id.cmp(&b.provenance.action_id))
    });

    let mut seen_structural_hashes = HashSet::new();
    for item in backfill_items {
        let mut owned_entries = Vec::new();
        for entry in &item.manifest.entries {
            let hash = entry.structural_hash.trim().to_ascii_lowercase();
            if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
                if seen_structural_hashes.insert(hash) {
                    owned_entries.push(entry.clone());
                }
            } else {
                manifest_error_count += 1;
            }
        }
        total_unique_cone_entries += owned_entries.len();

        let suggestions = build_k_bool_cone_corpus_suggested_actions_for_entries(
            &item.provenance.action_id,
            &owned_entries,
            &item.version,
            &item.runtime,
        );
        total_suggested_actions += suggestions.len();

        let existing_ids: HashSet<String> = item
            .provenance
            .suggested_next_actions
            .iter()
            .map(|s| s.action_id.clone())
            .collect();
        let new_ids: HashSet<String> = suggestions.iter().map(|s| s.action_id.clone()).collect();
        if existing_ids != new_ids {
            updated_provenance_count += 1;
            if !dry_run {
                let mut updated = item.provenance.clone();
                updated.suggested_next_actions = suggestions.clone();
                store.write_provenance(&updated)?;
            }
        }

        if !enqueue {
            continue;
        }
        for suggested in suggestions {
            if store.action_exists(&suggested.action_id) {
                already_done_count += 1;
                continue;
            }
            match queue_state_for_action(store, &suggested.action_id) {
                QueueState::Pending => {
                    already_pending_count += 1;
                }
                QueueState::Running { .. } => {
                    already_running_count += 1;
                }
                QueueState::Done => {
                    already_done_count += 1;
                }
                QueueState::Failed => {
                    already_failed_count += 1;
                }
                QueueState::Canceled => {
                    already_canceled_count += 1;
                }
                QueueState::None => {
                    if !dry_run {
                        enqueue_action(store, suggested.action)?;
                    }
                    enqueued_count += 1;
                }
            }
        }
    }

    Ok(json!({
        "dry_run": dry_run,
        "enqueue": enqueue,
        "scanned_k_bool_actions": scanned_k_bool_actions,
        "updated_provenance_count": updated_provenance_count,
        "manifest_error_count": manifest_error_count,
        "total_unique_cone_entries": total_unique_cone_entries,
        "total_suggested_actions": total_suggested_actions,
        "enqueued_count": enqueued_count,
        "already_done_count": already_done_count,
        "already_pending_count": already_pending_count,
        "already_running_count": already_running_count,
        "already_failed_count": already_failed_count,
        "already_canceled_count": already_canceled_count,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_test_store(prefix: &str) -> (ArtifactStore, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-app-test-{}-{}-{}",
            prefix,
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).expect("create temp store root");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure store layout");
        (store, root)
    }

    fn test_runtime() -> DriverRuntimeSpec {
        DriverRuntimeSpec {
            driver_version: "0.34.0".to_string(),
            release_platform: crate::DEFAULT_RELEASE_PLATFORM.to_string(),
            docker_image: default_driver_image("0.34.0"),
            dockerfile: crate::DEFAULT_DOCKERFILE.to_string(),
        }
    }

    fn make_artifact(action_id: &str, artifact_type: ArtifactType, relpath: &str) -> ArtifactRef {
        ArtifactRef {
            action_id: action_id.to_string(),
            artifact_type,
            relpath: relpath.to_string(),
        }
    }

    fn write_provenance_record(
        store: &ArtifactStore,
        action: ActionSpec,
        output_artifact: ArtifactRef,
        details: serde_json::Value,
        suggested_next_actions: Vec<SuggestedAction>,
        staged_files: Vec<(String, Vec<u8>)>,
    ) -> Provenance {
        let action_id = compute_action_id(&action).expect("compute action id");
        let provenance = Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id,
            created_utc: Utc::now(),
            action,
            dependencies: Vec::new(),
            output_artifact,
            output_files: staged_files
                .iter()
                .map(|(relpath, bytes)| OutputFile {
                    path: relpath.clone(),
                    bytes: bytes.len() as u64,
                    sha256: format!("{:x}", Sha256::digest(bytes)),
                })
                .collect(),
            commands: Vec::new(),
            details,
            suggested_next_actions,
        };
        let staging_dir = store
            .staging_dir()
            .join(format!("{}-staged", provenance.action_id));
        fs::create_dir_all(&staging_dir).expect("create staging dir");
        for (relpath, bytes) in &staged_files {
            let path = staging_dir.join(relpath);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("create staged file parent");
            }
            fs::write(path, bytes).expect("write staged file");
        }
        fs::write(
            staging_dir.join("provenance.json"),
            serde_json::to_string_pretty(&provenance).expect("serialize provenance"),
        )
        .expect("write staged provenance");
        store
            .promote_staging_action_dir(&provenance.action_id, &staging_dir)
            .expect("promote staged action");
        store
            .load_provenance(&provenance.action_id)
            .expect("reload written provenance")
    }

    fn make_opt_action(ir_action_id: &str, runtime: &DriverRuntimeSpec) -> ActionSpec {
        ActionSpec::DriverIrToOpt {
            ir_action_id: ir_action_id.to_string(),
            top_fn_name: Some("foo".to_string()),
            version: "v0.39.0".to_string(),
            runtime: runtime.clone(),
        }
    }

    fn make_k_bool_action(source_opt_action_id: &str, runtime: &DriverRuntimeSpec) -> ActionSpec {
        ActionSpec::IrFnToKBoolConeCorpus {
            ir_action_id: source_opt_action_id.to_string(),
            top_fn_name: Some("foo".to_string()),
            k: 3,
            max_ir_ops: crate::default_k_bool_cone_max_ir_ops_for_k(3),
            version: "v0.39.0".to_string(),
            runtime: runtime.clone(),
        }
    }

    fn make_k_bool_suggestion(
        source_opt_action_id: &str,
        runtime: &DriverRuntimeSpec,
    ) -> SuggestedAction {
        let action = make_k_bool_action(source_opt_action_id, runtime);
        SuggestedAction {
            reason: "test k-bool suggestion".to_string(),
            action_id: compute_action_id(&action).expect("compute suggested action id"),
            action,
        }
    }

    fn write_loss_history_for_hash(
        store: &ArtifactStore,
        runtime: &DriverRuntimeSpec,
        structural_hash: &str,
        seed: &str,
    ) -> Provenance {
        let source_opt = write_provenance_record(
            store,
            make_opt_action(&format!("src-ir-{seed}"), runtime),
            make_artifact(
                &compute_action_id(&make_opt_action(&format!("src-ir-{seed}"), runtime))
                    .expect("compute source opt action id"),
                ArtifactType::IrPackageFile,
                "payload/opt.ir",
            ),
            json!({
                "ir_top": "foo",
                "output_ir_fn_structural_hash": structural_hash,
            }),
            Vec::new(),
            Vec::new(),
        );
        let k_bool_action = make_k_bool_action(&source_opt.action_id, runtime);
        let k_bool_action_id = compute_action_id(&k_bool_action).expect("compute k-bool action id");
        write_provenance_record(
            store,
            k_bool_action,
            make_artifact(
                &k_bool_action_id,
                ArtifactType::IrPackageFile,
                "payload/k3.ir",
            ),
            json!({}),
            Vec::new(),
            Vec::new(),
        );

        let g8r_stats_action = ActionSpec::DriverAigToStats {
            aig_action_id: format!("g8r-aig-{seed}"),
            version: "v0.39.0".to_string(),
            runtime: runtime.clone(),
        };
        let g8r_stats_action_id =
            compute_action_id(&g8r_stats_action).expect("compute g8r stats action id");
        let g8r_stats_artifact = make_artifact(
            &g8r_stats_action_id,
            ArtifactType::AigStatsFile,
            "payload/g8r_stats.json",
        );
        write_provenance_record(
            store,
            g8r_stats_action,
            g8r_stats_artifact,
            json!({}),
            Vec::new(),
            vec![(
                "payload/g8r_stats.json".to_string(),
                serde_json::to_vec_pretty(&json!({"and_nodes": 11, "depth": 7}))
                    .expect("serialize g8r stats"),
            )],
        );

        let yosys_stats_action = ActionSpec::DriverAigToStats {
            aig_action_id: format!("yosys-aig-{seed}"),
            version: "v0.39.0".to_string(),
            runtime: runtime.clone(),
        };
        let yosys_stats_action_id =
            compute_action_id(&yosys_stats_action).expect("compute yosys stats action id");
        let yosys_stats_artifact = make_artifact(
            &yosys_stats_action_id,
            ArtifactType::AigStatsFile,
            "payload/yosys_stats.json",
        );
        write_provenance_record(
            store,
            yosys_stats_action,
            yosys_stats_artifact,
            json!({}),
            Vec::new(),
            vec![(
                "payload/yosys_stats.json".to_string(),
                serde_json::to_vec_pretty(&json!({"and_nodes": 10, "depth": 6}))
                    .expect("serialize yosys stats"),
            )],
        );

        let diff_action = ActionSpec::AigStatDiff {
            opt_ir_action_id: k_bool_action_id,
            g8r_aig_stats_action_id: g8r_stats_action_id,
            yosys_abc_aig_stats_action_id: yosys_stats_action_id,
        };
        let diff_action_id = compute_action_id(&diff_action).expect("compute diff action id");
        write_provenance_record(
            store,
            diff_action,
            make_artifact(
                &diff_action_id,
                ArtifactType::AigStatDiffFile,
                "payload/aig_stat_diff.json",
            ),
            json!({}),
            Vec::new(),
            Vec::new(),
        )
    }

    fn write_current_opt_with_k_bool_suggestion(
        store: &ArtifactStore,
        runtime: &DriverRuntimeSpec,
        structural_hash: &str,
        seed: &str,
    ) -> (Provenance, SuggestedAction) {
        let current_opt_action = make_opt_action(&format!("current-ir-{seed}"), runtime);
        let current_opt_action_id =
            compute_action_id(&current_opt_action).expect("compute current opt action id");
        let suggestion = make_k_bool_suggestion(&current_opt_action_id, runtime);
        let provenance = write_provenance_record(
            store,
            current_opt_action,
            make_artifact(
                &current_opt_action_id,
                ArtifactType::IrPackageFile,
                "payload/current_opt.ir",
            ),
            json!({
                "ir_top": "foo",
                "output_ir_fn_structural_hash": structural_hash,
            }),
            vec![suggestion.clone()],
            Vec::new(),
        );
        (provenance, suggestion)
    }

    #[test]
    fn finalize_successful_queue_action_keeps_running_visible_until_done_write() {
        let (store, root) = make_test_store("finalize-success-order");
        let runtime = test_runtime();
        let action = make_opt_action("finalize-success-order-ir", &runtime);
        let action_id = compute_action_id(&action).expect("compute queue action id");
        let now = Utc::now();
        let running = QueueRunningWithPath {
            running: QueueRunning {
                schema_version: crate::ACTION_SCHEMA_VERSION,
                action_id: action_id.clone(),
                enqueued_utc: now,
                priority: crate::DEFAULT_QUEUE_PRIORITY,
                action,
                lease_owner: "worker-1".to_string(),
                lease_acquired_utc: now,
                lease_expires_utc: now,
            },
            path: store.running_queue_path(&action_id),
        };
        if let Some(parent) = running.path.parent() {
            fs::create_dir_all(parent).expect("create running queue dir");
        }
        fs::write(
            &running.path,
            serde_json::to_string_pretty(&running.running).expect("serialize running queue record"),
        )
        .expect("write running queue record");

        let mut before_mark_done_called = false;
        finalize_successful_queue_action(
            &store,
            &running,
            make_artifact(
                &action_id,
                ArtifactType::IrPackageFile,
                "payload/finalized.ir",
            ),
            "worker-1",
            || {
                before_mark_done_called = true;
                assert!(running.path.exists());
                assert!(!store.done_queue_path(&action_id).exists());
            },
        )
        .expect("finalize successful queue action");

        assert!(before_mark_done_called);
        assert!(!running.path.exists());
        let done = load_queue_done_record(&store, &action_id)
            .expect("load done queue record")
            .expect("done queue record exists");
        assert_eq!(done.completed_by, "worker-1");
        assert_eq!(done.output_artifact.relpath, "payload/finalized.ir");

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn enqueue_suggested_actions_skips_k_bool_without_previous_loss_history() {
        let (store, root) = make_test_store("skip-kbool-without-history");
        let runtime = test_runtime();
        let (current_opt, suggestion) =
            write_current_opt_with_k_bool_suggestion(&store, &runtime, &"a".repeat(64), "skip");

        let summary = enqueue_suggested_actions_with_policy(
            &store,
            &root,
            &current_opt.action_id,
            false,
            1,
            crate::DEFAULT_QUEUE_PRIORITY,
            SuggestedEnqueuePolicy {
                only_previous_loss_k_cones: true,
            },
        )
        .expect("enqueue suggested actions");

        assert_eq!(summary.enqueued_count, 0);
        assert_eq!(summary.skipped_not_previously_lossy_k_bool_count, 1);
        assert!(!store.pending_queue_path(&suggestion.action_id).exists());

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn enqueue_suggested_actions_promotes_pending_items_with_stage_bonus() {
        let (store, root) = make_test_store("promote-pending-suggestion-priority");
        let runtime = test_runtime();
        let (current_opt, suggestion) =
            write_current_opt_with_k_bool_suggestion(&store, &runtime, &"c".repeat(64), "promote");

        enqueue_action_with_priority(
            &store,
            suggestion.action.clone(),
            crate::DEFAULT_QUEUE_PRIORITY,
        )
        .expect("enqueue pending suggestion");
        let expected_priority =
            suggested_action_queue_priority(crate::DEFAULT_QUEUE_PRIORITY, &suggestion.action);

        let summary = enqueue_suggested_actions_with_policy(
            &store,
            &root,
            &current_opt.action_id,
            false,
            1,
            crate::DEFAULT_QUEUE_PRIORITY,
            SuggestedEnqueuePolicy::default(),
        )
        .expect("enqueue suggested actions");

        assert_eq!(summary.enqueued_count, 0);
        assert_eq!(summary.already_pending_count, 1);
        let pending = load_queue_pending_record(&store, &suggestion.action_id)
            .expect("load pending queue record")
            .expect("pending queue record exists");
        assert_eq!(pending.priority, expected_priority);

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn compatible_batch_claim_limit_caps_extra_claims() {
        assert_eq!(compatible_batch_claim_limit(0), 0);
        assert_eq!(compatible_batch_claim_limit(1), 1);
        assert_eq!(compatible_batch_claim_limit(3), 3);
        assert_eq!(compatible_batch_claim_limit(30), 3);
    }

    #[test]
    fn completed_lossy_aig_stat_diff_updates_k_bool_enqueue_filter_cache() {
        let (store, root) = make_test_store("update-kbool-loss-cache");
        let runtime = test_runtime();
        let structural_hash = "b".repeat(64);
        let (current_opt, suggestion) =
            write_current_opt_with_k_bool_suggestion(&store, &runtime, &structural_hash, "update");

        let first = enqueue_suggested_actions_with_policy(
            &store,
            &root,
            &current_opt.action_id,
            false,
            1,
            crate::DEFAULT_QUEUE_PRIORITY,
            SuggestedEnqueuePolicy {
                only_previous_loss_k_cones: true,
            },
        )
        .expect("enqueue suggested actions first pass");
        assert_eq!(first.enqueued_count, 0);
        assert_eq!(first.skipped_not_previously_lossy_k_bool_count, 1);

        let diff = write_loss_history_for_hash(&store, &runtime, &structural_hash, "update");
        let ActionSpec::AigStatDiff {
            opt_ir_action_id,
            g8r_aig_stats_action_id,
            yosys_abc_aig_stats_action_id,
        } = &diff.action
        else {
            panic!("expected aig-stat-diff action");
        };
        let k_bool_provenance = store
            .load_provenance(opt_ir_action_id)
            .expect("load k-bool provenance");
        let ActionSpec::IrFnToKBoolConeCorpus {
            ir_action_id, k, ..
        } = &k_bool_provenance.action
        else {
            panic!("expected k-bool provenance");
        };
        assert_eq!(*k, 3);
        let source_opt = store
            .load_provenance(ir_action_id)
            .expect("load source opt provenance");
        assert_eq!(
            source_opt
                .details
                .get("output_ir_fn_structural_hash")
                .and_then(|v| v.as_str()),
            Some(structural_hash.as_str())
        );
        let g8r_stats = store
            .load_provenance(g8r_aig_stats_action_id)
            .expect("load g8r stats provenance");
        let yosys_stats = store
            .load_provenance(yosys_abc_aig_stats_action_id)
            .expect("load yosys stats provenance");
        assert!(
            store
                .resolve_artifact_ref_path(&g8r_stats.output_artifact)
                .exists()
        );
        assert!(
            store
                .resolve_artifact_ref_path(&yosys_stats.output_artifact)
                .exists()
        );
        assert_eq!(
            lossy_k_bool_source_structural_hash_for_aig_stat_diff(&store, &diff),
            Some((3, structural_hash.clone()))
        );
        note_completed_action_for_previous_loss_k_cone_policy(&store, &diff.action_id)
            .expect("refresh k-bool loss cache");
        assert!(
            load_complete_previously_lossy_k_bool_source_hashes(&store, 3)
                .expect("load cached lossy k-bool source hashes")
                .contains(&structural_hash)
        );

        let second = enqueue_suggested_actions_with_policy(
            &store,
            &root,
            &current_opt.action_id,
            false,
            1,
            crate::DEFAULT_QUEUE_PRIORITY,
            SuggestedEnqueuePolicy {
                only_previous_loss_k_cones: true,
            },
        )
        .expect("enqueue suggested actions second pass");
        assert_eq!(second.enqueued_count, 1);
        assert_eq!(second.skipped_not_previously_lossy_k_bool_count, 0);
        assert!(store.pending_queue_path(&suggestion.action_id).exists());

        fs::remove_dir_all(root).expect("cleanup temp store");
    }
}
