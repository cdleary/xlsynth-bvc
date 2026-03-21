use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct DriverRuntimeSpec {
    pub(crate) driver_version: String,
    pub(crate) release_platform: String,
    pub(crate) docker_image: String,
    pub(crate) dockerfile: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct YosysRuntimeSpec {
    pub(crate) docker_image: String,
    pub(crate) dockerfile: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum G8rLoweringMode {
    #[default]
    Default,
    FrontendNoPrepRewrite,
}

fn is_default_g8r_lowering_mode(mode: &G8rLoweringMode) -> bool {
    matches!(mode, G8rLoweringMode::Default)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArtifactType {
    DslxFileSubtree,
    IrPackageFile,
    IrDelayInfoFile,
    AigFile,
    VerilogFile,
    AigStatsFile,
    AigStatDiffFile,
    EquivReportFile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ArtifactRef {
    pub(crate) action_id: String,
    pub(crate) artifact_type: ArtifactType,
    pub(crate) relpath: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ScriptRef {
    pub(crate) path: String,
    pub(crate) sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub(crate) enum ActionSpec {
    DownloadAndExtractXlsynthReleaseStdlibTarball {
        version: String,
        discovery_runtime: Option<DriverRuntimeSpec>,
    },
    DownloadAndExtractXlsynthSourceSubtree {
        version: String,
        subtree: String,
        discovery_runtime: Option<DriverRuntimeSpec>,
    },
    DriverDslxFnToIr {
        dslx_subtree_action_id: String,
        dslx_file: String,
        dslx_fn_name: String,
        version: String,
        runtime: DriverRuntimeSpec,
    },
    DriverIrToOpt {
        ir_action_id: String,
        top_fn_name: Option<String>,
        version: String,
        runtime: DriverRuntimeSpec,
    },
    DriverIrToDelayInfo {
        ir_action_id: String,
        top_fn_name: Option<String>,
        delay_model: String,
        output_format: String,
        version: String,
        runtime: DriverRuntimeSpec,
    },
    DriverIrEquiv {
        lhs_ir_action_id: String,
        rhs_ir_action_id: String,
        top_fn_name: Option<String>,
        version: String,
        runtime: DriverRuntimeSpec,
    },
    DriverIrAigEquiv {
        ir_action_id: String,
        aig_action_id: String,
        top_fn_name: Option<String>,
        version: String,
        runtime: DriverRuntimeSpec,
    },
    DriverIrToG8rAig {
        ir_action_id: String,
        top_fn_name: Option<String>,
        fraig: bool,
        #[serde(default, skip_serializing_if = "is_default_g8r_lowering_mode")]
        lowering_mode: G8rLoweringMode,
        version: String,
        runtime: DriverRuntimeSpec,
    },
    IrFnToCombinationalVerilog {
        ir_action_id: String,
        top_fn_name: Option<String>,
        use_system_verilog: bool,
        version: String,
        runtime: DriverRuntimeSpec,
    },
    IrFnToKBoolConeCorpus {
        ir_action_id: String,
        top_fn_name: Option<String>,
        k: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_ir_ops: Option<u64>,
        version: String,
        runtime: DriverRuntimeSpec,
    },
    ComboVerilogToYosysAbcAig {
        verilog_action_id: String,
        verilog_top_module_name: Option<String>,
        yosys_script_ref: ScriptRef,
        runtime: YosysRuntimeSpec,
    },
    AigToYosysAbcAig {
        aig_action_id: String,
        yosys_script_ref: ScriptRef,
        runtime: YosysRuntimeSpec,
    },
    DriverAigToStats {
        aig_action_id: String,
        version: String,
        runtime: DriverRuntimeSpec,
    },
    AigStatDiff {
        opt_ir_action_id: String,
        g8r_aig_stats_action_id: String,
        yosys_abc_aig_stats_action_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum ActionBatchKey {
    DriverIrToG8rAig {
        version: String,
        runtime: DriverRuntimeSpec,
        fraig: bool,
        lowering_mode: G8rLoweringMode,
    },
}

pub(crate) fn action_batch_key(action: &ActionSpec) -> Option<ActionBatchKey> {
    match action {
        ActionSpec::DriverIrToG8rAig {
            version,
            runtime,
            fraig,
            lowering_mode,
            ..
        } => Some(ActionBatchKey::DriverIrToG8rAig {
            version: version.clone(),
            runtime: runtime.clone(),
            fraig: *fraig,
            lowering_mode: lowering_mode.clone(),
        }),
        _ => None,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OutputFile {
    pub(crate) path: String,
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CommandTrace {
    pub(crate) argv: Vec<String>,
    pub(crate) exit_code: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Provenance {
    pub(crate) schema_version: u32,
    pub(crate) action_id: String,
    pub(crate) created_utc: DateTime<Utc>,
    pub(crate) action: ActionSpec,
    pub(crate) dependencies: Vec<ArtifactRef>,
    pub(crate) output_artifact: ArtifactRef,
    pub(crate) output_files: Vec<OutputFile>,
    pub(crate) commands: Vec<CommandTrace>,
    pub(crate) details: serde_json::Value,
    #[serde(default)]
    pub(crate) suggested_next_actions: Vec<SuggestedAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SuggestedAction {
    pub(crate) reason: String,
    pub(crate) action_id: String,
    pub(crate) action: ActionSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueItem {
    pub(crate) schema_version: u32,
    pub(crate) action_id: String,
    pub(crate) enqueued_utc: DateTime<Utc>,
    #[serde(default = "default_queue_priority")]
    pub(crate) priority: i32,
    pub(crate) action: ActionSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueDone {
    pub(crate) schema_version: u32,
    pub(crate) action_id: String,
    pub(crate) completed_utc: DateTime<Utc>,
    #[serde(default = "crate::service::default_completed_by")]
    pub(crate) completed_by: String,
    pub(crate) output_artifact: ArtifactRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueFailed {
    pub(crate) schema_version: u32,
    pub(crate) action_id: String,
    pub(crate) enqueued_utc: DateTime<Utc>,
    pub(crate) failed_utc: DateTime<Utc>,
    pub(crate) failed_by: String,
    pub(crate) action: ActionSpec,
    pub(crate) error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueCanceled {
    pub(crate) schema_version: u32,
    pub(crate) action_id: String,
    pub(crate) enqueued_utc: DateTime<Utc>,
    pub(crate) canceled_utc: DateTime<Utc>,
    pub(crate) canceled_by: String,
    pub(crate) canceled_due_to_action_id: String,
    pub(crate) root_failed_action_id: String,
    pub(crate) action: ActionSpec,
    pub(crate) reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueRunning {
    pub(crate) schema_version: u32,
    pub(crate) action_id: String,
    pub(crate) enqueued_utc: DateTime<Utc>,
    #[serde(default = "default_queue_priority")]
    pub(crate) priority: i32,
    pub(crate) action: ActionSpec,
    pub(crate) lease_owner: String,
    pub(crate) lease_acquired_utc: DateTime<Utc>,
    pub(crate) lease_expires_utc: DateTime<Utc>,
}

fn default_queue_priority() -> i32 {
    crate::DEFAULT_QUEUE_PRIORITY
}

#[derive(Debug, Clone)]
pub(crate) struct QueueRunningWithPath {
    pub(crate) running: QueueRunning,
    pub(crate) path: PathBuf,
}

impl QueueRunningWithPath {
    pub(crate) fn action_id(&self) -> &str {
        &self.running.action_id
    }

    pub(crate) fn enqueued_utc(&self) -> &DateTime<Utc> {
        &self.running.enqueued_utc
    }

    pub(crate) fn action(&self) -> &ActionSpec {
        &self.running.action
    }

    pub(crate) fn priority(&self) -> i32 {
        self.running.priority
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SuggestedReport {
    pub(crate) root_action_id: String,
    pub(crate) recursive: bool,
    pub(crate) max_depth: u32,
    pub(crate) nodes: Vec<SuggestedNode>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SuggestedNode {
    pub(crate) source_action_id: String,
    pub(crate) depth: u32,
    pub(crate) suggestions: Vec<SuggestedStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SuggestedStatus {
    pub(crate) reason: String,
    pub(crate) action_id: String,
    pub(crate) completed: bool,
    pub(crate) queue_state: String,
    pub(crate) action: ActionSpec,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SuggestedAuditReport {
    pub(crate) total_source_actions: usize,
    pub(crate) total_suggestions: usize,
    pub(crate) completed_suggestions: usize,
    pub(crate) missing_suggestions: usize,
    pub(crate) entries: Vec<SuggestedAuditEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SuggestedAuditEntry {
    pub(crate) source_action_id: String,
    pub(crate) reason: String,
    pub(crate) action_id: String,
    pub(crate) completed: bool,
    pub(crate) queue_state: String,
    pub(crate) action: ActionSpec,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EnqueueSuggestedSummary {
    pub(crate) root_action_id: String,
    pub(crate) recursive: bool,
    pub(crate) max_depth: u32,
    pub(crate) visited_source_actions: usize,
    pub(crate) total_suggestions: usize,
    pub(crate) enqueued_count: usize,
    pub(crate) already_done_count: usize,
    pub(crate) already_pending_count: usize,
    pub(crate) already_running_count: usize,
    pub(crate) already_failed_count: usize,
    pub(crate) already_canceled_count: usize,
    pub(crate) skipped_blocked_count: usize,
    pub(crate) skipped_not_previously_lossy_k_bool_count: usize,
    pub(crate) unknown_queue_state_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct AigStatDiffQueryReport {
    pub(crate) opt_ir_action_id: String,
    pub(crate) count: usize,
    pub(crate) diffs: Vec<AigStatDiffQueryItem>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct AigStatDiffQueryItem {
    pub(crate) action_id: String,
    pub(crate) output_artifact: ArtifactRef,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PopulateIrFnCorpusStructuralSummary {
    pub(crate) output_dir: String,
    pub(crate) manifest_path: String,
    pub(crate) generated_utc: DateTime<Utc>,
    pub(crate) recompute_missing_hashes: bool,
    pub(crate) threads: usize,
    pub(crate) total_actions_scanned: usize,
    pub(crate) total_driver_ir_to_opt_actions: usize,
    pub(crate) total_ir_fn_to_k_bool_cone_corpus_actions: usize,
    pub(crate) indexed_actions: usize,
    pub(crate) indexed_k_bool_cone_members: usize,
    pub(crate) distinct_structural_hashes: usize,
    pub(crate) hash_from_dependency_hint_count: usize,
    pub(crate) hash_recomputed_count: usize,
    pub(crate) hash_hint_conflict_count: usize,
    pub(crate) skipped_missing_output_count: usize,
    pub(crate) skipped_missing_ir_top_count: usize,
    pub(crate) skipped_missing_hash_hint_count: usize,
    pub(crate) skipped_hash_error_count: usize,
    pub(crate) skipped_k_bool_cone_manifest_errors: usize,
    pub(crate) skipped_k_bool_cone_empty_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CheckIrFnCorpusStructuralFreshnessSummary {
    pub(crate) up_to_date: bool,
    pub(crate) stale_reasons: Vec<String>,
    pub(crate) manifest_present: bool,
    pub(crate) manifest_generated_utc: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) manifest_source_action_set_sha256: Option<String>,
    pub(crate) current_source_action_set_sha256: String,
    pub(crate) manifest_total_driver_ir_to_opt_actions: Option<usize>,
    pub(crate) current_total_driver_ir_to_opt_actions: usize,
    pub(crate) manifest_total_ir_fn_to_k_bool_cone_corpus_actions: Option<usize>,
    pub(crate) current_total_ir_fn_to_k_bool_cone_corpus_actions: usize,
    pub(crate) latest_relevant_action_created_utc: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EnqueueStructuralOptIrG8rSummary {
    pub(crate) crate_version: String,
    pub(crate) dso_version: String,
    pub(crate) fraig: bool,
    pub(crate) dry_run: bool,
    pub(crate) recompute_missing_hashes: bool,
    pub(crate) total_actions_scanned: usize,
    pub(crate) total_driver_ir_to_opt_actions: usize,
    pub(crate) hashed_opt_ir_actions: usize,
    pub(crate) unique_structural_hashes: usize,
    pub(crate) hash_from_dependency_hint_count: usize,
    pub(crate) hash_recomputed_count: usize,
    pub(crate) hash_hint_conflict_count: usize,
    pub(crate) skipped_missing_output_count: usize,
    pub(crate) skipped_missing_ir_top_count: usize,
    pub(crate) skipped_missing_hash_hint_count: usize,
    pub(crate) skipped_hash_error_count: usize,
    pub(crate) existing_done_count: usize,
    pub(crate) existing_pending_count: usize,
    pub(crate) existing_running_count: usize,
    pub(crate) existing_failed_count: usize,
    pub(crate) existing_canceled_count: usize,
    pub(crate) already_done_action_id_count: usize,
    pub(crate) already_pending_action_id_count: usize,
    pub(crate) already_running_action_id_count: usize,
    pub(crate) already_failed_action_id_count: usize,
    pub(crate) already_canceled_action_id_count: usize,
    pub(crate) enqueued_count: usize,
    pub(crate) enqueued_samples: Vec<EnqueuedStructuralOptIrG8rSample>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EnqueuedStructuralOptIrG8rSample {
    pub(crate) structural_hash: String,
    pub(crate) opt_ir_action_id: String,
    pub(crate) action_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EnqueueStructuralOptIrKBoolConeSummary {
    pub(crate) crate_version: String,
    pub(crate) dso_version: String,
    pub(crate) k: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) max_ir_ops: Option<u64>,
    pub(crate) dry_run: bool,
    pub(crate) only_previous_losses: bool,
    pub(crate) recompute_missing_hashes: bool,
    pub(crate) total_actions_scanned: usize,
    pub(crate) total_driver_ir_to_opt_actions: usize,
    pub(crate) hashed_opt_ir_actions: usize,
    pub(crate) unique_structural_hashes: usize,
    pub(crate) hash_from_dependency_hint_count: usize,
    pub(crate) hash_recomputed_count: usize,
    pub(crate) hash_hint_conflict_count: usize,
    pub(crate) skipped_missing_output_count: usize,
    pub(crate) skipped_missing_ir_top_count: usize,
    pub(crate) skipped_missing_hash_hint_count: usize,
    pub(crate) skipped_hash_error_count: usize,
    pub(crate) historical_k_bool_aig_stat_diffs_scanned: usize,
    pub(crate) historical_k_bool_aig_stat_diffs_considered: usize,
    pub(crate) historical_k_bool_loss_samples: usize,
    pub(crate) historical_lossy_source_structural_hashes: usize,
    pub(crate) skipped_not_previously_lossy_structural_hashes: usize,
    pub(crate) existing_done_count: usize,
    pub(crate) existing_pending_count: usize,
    pub(crate) existing_running_count: usize,
    pub(crate) existing_failed_count: usize,
    pub(crate) existing_canceled_count: usize,
    pub(crate) already_done_action_id_count: usize,
    pub(crate) already_pending_action_id_count: usize,
    pub(crate) already_running_action_id_count: usize,
    pub(crate) already_failed_action_id_count: usize,
    pub(crate) already_canceled_action_id_count: usize,
    pub(crate) enqueued_count: usize,
    pub(crate) enqueued_samples: Vec<EnqueuedStructuralOptIrKBoolConeSample>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EnqueuedStructuralOptIrKBoolConeSample {
    pub(crate) structural_hash: String,
    pub(crate) opt_ir_action_id: String,
    pub(crate) ir_top: String,
    pub(crate) action_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct StructuralOptIrCandidate {
    pub(crate) structural_hash: String,
    pub(crate) opt_ir_action_id: String,
    pub(crate) ir_top: String,
    pub(crate) crate_version: String,
    pub(crate) created_utc: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum StructuralHashCoverageState {
    Pending,
    Running,
    Done,
    Failed,
    Canceled,
}

#[derive(Debug, Clone)]
pub(crate) struct StructuralHashCoverageRecord {
    pub(crate) state: StructuralHashCoverageState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct IrFnCorpusStructuralManifest {
    pub(crate) schema_version: u32,
    pub(crate) generated_utc: DateTime<Utc>,
    pub(crate) store_root: String,
    pub(crate) output_dir: String,
    pub(crate) recompute_missing_hashes: bool,
    pub(crate) total_actions_scanned: usize,
    pub(crate) total_driver_ir_to_opt_actions: usize,
    #[serde(default)]
    pub(crate) total_ir_fn_to_k_bool_cone_corpus_actions: usize,
    pub(crate) indexed_actions: usize,
    #[serde(default)]
    pub(crate) indexed_k_bool_cone_members: usize,
    pub(crate) distinct_structural_hashes: usize,
    pub(crate) hash_from_dependency_hint_count: usize,
    pub(crate) hash_recomputed_count: usize,
    pub(crate) hash_hint_conflict_count: usize,
    pub(crate) skipped_missing_output_count: usize,
    pub(crate) skipped_missing_ir_top_count: usize,
    pub(crate) skipped_missing_hash_hint_count: usize,
    pub(crate) skipped_hash_error_count: usize,
    #[serde(default)]
    pub(crate) skipped_k_bool_cone_manifest_errors: usize,
    #[serde(default)]
    pub(crate) skipped_k_bool_cone_empty_count: usize,
    #[serde(default)]
    pub(crate) source_action_set_sha256: Option<String>,
    pub(crate) groups: Vec<IrFnCorpusStructuralManifestGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct IrFnCorpusStructuralManifestGroup {
    pub(crate) structural_hash: String,
    pub(crate) member_count: usize,
    pub(crate) relpath: String,
    #[serde(default)]
    pub(crate) ir_node_count: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct IrFnCorpusStructuralGroupFile {
    pub(crate) schema_version: u32,
    pub(crate) structural_hash: String,
    pub(crate) members: Vec<IrFnCorpusStructuralMember>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct IrFnCorpusStructuralMember {
    pub(crate) opt_ir_action_id: String,
    pub(crate) source_ir_action_id: String,
    pub(crate) ir_top: String,
    #[serde(default)]
    pub(crate) ir_fn_signature: Option<String>,
    #[serde(default)]
    pub(crate) ir_op_count: Option<u64>,
    pub(crate) crate_version: String,
    pub(crate) dso_version: String,
    pub(crate) created_utc: DateTime<Utc>,
    pub(crate) output_artifact: ArtifactRef,
    pub(crate) output_file_sha256: String,
    pub(crate) output_file_bytes: u64,
    pub(crate) hash_source: String,
    pub(crate) hash_hint_source_action_ids: Vec<String>,
    pub(crate) dslx_origin: Option<IrFnCorpusStructuralDslxOrigin>,
    #[serde(default)]
    pub(crate) producer_action_kind: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct IrFnCorpusStructuralDslxOrigin {
    pub(crate) dslx_subtree_action_id: String,
    pub(crate) dslx_file: String,
    pub(crate) dslx_fn_name: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct IrStructuralHashHint {
    pub(crate) hashes: BTreeSet<String>,
    pub(crate) source_action_ids: BTreeSet<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct StructuralHashRecomputeJob {
    pub(crate) member: IrFnCorpusStructuralMember,
    pub(crate) output_path: PathBuf,
    pub(crate) dso_version: String,
    pub(crate) runtime: DriverRuntimeSpec,
}

#[derive(Debug, Clone)]
pub(crate) struct StructuralHashRecomputeResult {
    pub(crate) member: IrFnCorpusStructuralMember,
    pub(crate) structural_hash: Option<String>,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DiscoverReleasesSummary {
    pub(crate) after: String,
    pub(crate) max_pages: u32,
    pub(crate) inspected_releases: usize,
    pub(crate) considered_releases: usize,
    pub(crate) enqueued_count: usize,
    pub(crate) reached_after: bool,
    pub(crate) releases: Vec<DiscoveredRelease>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DiscoveredRelease {
    pub(crate) version: String,
    pub(crate) action_id: String,
    pub(crate) enqueued: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RefreshVersionCompatSummary {
    pub(crate) output_path: String,
    pub(crate) source_url: String,
    pub(crate) bytes: usize,
    pub(crate) sha256: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct VersionCompatEntry {
    pub(crate) xlsynth_release_version: String,
    #[allow(dead_code)]
    pub(crate) crate_release_datetime: String,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct ReleaseTag {
    pub(crate) major: u32,
    pub(crate) minor: u32,
    pub(crate) patch: u32,
    pub(crate) patch2: u32,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GithubRelease {
    pub(crate) tag_name: String,
    pub(crate) draft: bool,
    pub(crate) prerelease: bool,
    pub(crate) assets: Vec<GithubReleaseAsset>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GithubReleaseAsset {
    pub(crate) name: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct IrFnStructuralHashResponse {
    pub(crate) structural_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct KBoolConeCorpusManifest {
    pub(crate) schema_version: u32,
    pub(crate) source_ir_action_id: String,
    pub(crate) source_ir_top: String,
    pub(crate) k: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) max_ir_ops: Option<u64>,
    pub(crate) total_manifest_rows: usize,
    pub(crate) emitted_cone_files: usize,
    pub(crate) deduped_unique_cones: usize,
    #[serde(default)]
    pub(crate) filtered_out_ir_op_count: usize,
    pub(crate) output_ir_relpath: String,
    pub(crate) entries: Vec<KBoolConeCorpusEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct KBoolConeCorpusEntry {
    pub(crate) structural_hash: String,
    pub(crate) fn_name: String,
    pub(crate) source_index: usize,
    pub(crate) sink_node_index: u64,
    pub(crate) frontier_leaf_indices: Vec<u64>,
    pub(crate) frontier_non_literal_count: u64,
    pub(crate) included_node_count: u64,
    #[serde(default)]
    pub(crate) ir_fn_signature: Option<String>,
    #[serde(default)]
    pub(crate) ir_op_count: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RawBoolConeManifestLine {
    pub(crate) sha256: String,
    pub(crate) sink_node_index: u64,
    #[serde(default)]
    pub(crate) frontier_leaf_indices: Vec<u64>,
    pub(crate) frontier_non_literal_count: u64,
    pub(crate) included_node_count: u64,
}

#[derive(Debug)]
pub(crate) struct ActionOutcome {
    pub(crate) dependencies: Vec<ArtifactRef>,
    pub(crate) output_artifact: ArtifactRef,
    pub(crate) commands: Vec<CommandTrace>,
    pub(crate) details: serde_json::Value,
    pub(crate) suggested_next_actions: Vec<SuggestedAction>,
}

#[derive(Debug)]
pub(crate) struct DslxFnDiscovery {
    pub(crate) commands: Vec<CommandTrace>,
    pub(crate) source_runtime: DriverRuntimeSpec,
    pub(crate) discovery_runtime: DriverRuntimeSpec,
    pub(crate) discovery_runtime_xlsynth_version: String,
    pub(crate) import_context: DslxImportContext,
    pub(crate) scanned_dslx_files: usize,
    pub(crate) listed_functions: usize,
    pub(crate) concrete_functions: usize,
    pub(crate) failed_dslx_files: Vec<String>,
    pub(crate) suggested_next_actions: Vec<SuggestedAction>,
}

#[derive(Debug, Clone)]
pub(crate) struct DslxImportContext {
    pub(crate) dslx_path: String,
    pub(crate) dslx_stdlib_path: String,
    pub(crate) stdlib_source: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct DiscoveredDslxFunction {
    pub(crate) dslx_file: String,
    pub(crate) fn_name: String,
    pub(crate) is_parametric: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_runtime() -> DriverRuntimeSpec {
        DriverRuntimeSpec {
            driver_version: "0.31.0".to_string(),
            release_platform: "ubuntu2004".to_string(),
            docker_image: "xlsynth-bvc-driver:0.31.0".to_string(),
            dockerfile: "docker/xlsynth-driver.Dockerfile".to_string(),
        }
    }

    fn sample_action() -> ActionSpec {
        ActionSpec::DriverIrToOpt {
            ir_action_id: "a".repeat(64),
            top_fn_name: Some("__foo".to_string()),
            version: "v0.35.0".to_string(),
            runtime: sample_runtime(),
        }
    }

    #[test]
    fn serde_roundtrip_queue_failed_preserves_action_and_error() {
        let record = QueueFailed {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: "b".repeat(64),
            enqueued_utc: Utc::now(),
            failed_utc: Utc::now(),
            failed_by: "worker-a".to_string(),
            action: sample_action(),
            error: "boom".to_string(),
        };
        let text = serde_json::to_string_pretty(&record).expect("serialize QueueFailed");
        let parsed: QueueFailed = serde_json::from_str(&text).expect("deserialize QueueFailed");
        assert_eq!(parsed.schema_version, crate::ACTION_SCHEMA_VERSION);
        assert_eq!(parsed.action_id, record.action_id);
        assert_eq!(parsed.failed_by, record.failed_by);
        assert_eq!(parsed.error, record.error);
        assert_eq!(
            serde_json::to_value(&parsed.action).expect("serialize parsed action"),
            serde_json::to_value(&record.action).expect("serialize expected action")
        );
    }

    #[test]
    fn serde_roundtrip_provenance_preserves_suggested_actions() {
        let record = Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: "c".repeat(64),
            created_utc: Utc::now(),
            action: sample_action(),
            dependencies: vec![ArtifactRef {
                action_id: "d".repeat(64),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: "payload/main.ir".to_string(),
            }],
            output_artifact: ArtifactRef {
                action_id: "c".repeat(64),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: "payload/result.ir".to_string(),
            },
            output_files: vec![OutputFile {
                path: "payload/result.ir".to_string(),
                bytes: 12,
                sha256: "e".repeat(64),
            }],
            commands: vec![CommandTrace {
                argv: vec!["xlsynth-driver".to_string(), "ir2opt".to_string()],
                exit_code: 0,
            }],
            details: json!({"example":"value"}),
            suggested_next_actions: vec![SuggestedAction {
                reason: "opt->g8r".to_string(),
                action_id: "f".repeat(64),
                action: ActionSpec::DriverIrToG8rAig {
                    ir_action_id: "c".repeat(64),
                    top_fn_name: Some("__foo".to_string()),
                    fraig: false,
                    lowering_mode: G8rLoweringMode::Default,
                    version: "v0.35.0".to_string(),
                    runtime: sample_runtime(),
                },
            }],
        };

        let text = serde_json::to_string_pretty(&record).expect("serialize Provenance");
        let parsed: Provenance = serde_json::from_str(&text).expect("deserialize Provenance");
        assert_eq!(parsed.action_id, record.action_id);
        assert_eq!(
            serde_json::to_value(&parsed.action).expect("serialize parsed action"),
            serde_json::to_value(&record.action).expect("serialize expected action")
        );
        assert_eq!(parsed.output_artifact.relpath, "payload/result.ir");
        assert_eq!(parsed.suggested_next_actions.len(), 1);
        assert_eq!(
            parsed.suggested_next_actions[0].action_id,
            record.suggested_next_actions[0].action_id
        );
    }

    #[test]
    fn queue_done_deserialize_defaults_completed_by_when_missing() {
        let raw = json!({
            "schema_version": crate::ACTION_SCHEMA_VERSION,
            "action_id": "9".repeat(64),
            "completed_utc": Utc::now(),
            "output_artifact": {
                "action_id": "9".repeat(64),
                "artifact_type": "ir_package_file",
                "relpath": "payload/result.ir"
            }
        });
        let parsed: QueueDone =
            serde_json::from_value(raw).expect("deserialize QueueDone with missing completed_by");
        assert_eq!(parsed.completed_by, crate::service::default_completed_by());
    }
}
