// SPDX-License-Identifier: Apache-2.0

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use crate::model::{Provenance, QueueCanceled, QueueDone, QueueFailed, QueueItem, QueueRunning};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct VersionCardView {
    pub(crate) crate_version: String,
    pub(crate) crate_release_datetime: Option<String>,
    pub(crate) total_materialized: usize,
    pub(crate) failed_total: usize,
    pub(crate) dso_versions: Vec<String>,
    pub(crate) stdlib_enumeration: StdlibEnumerationStatusView,
    pub(crate) failed_by_kind: Vec<FailedKindView>,
    pub(crate) failures: Vec<FailedActionRowView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FailedKindView {
    pub(crate) kind: String,
    pub(crate) count: usize,
    pub(crate) timeout_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StdlibEnumerationStatusView {
    pub(crate) badge_class: String,
    pub(crate) badge_label: String,
    pub(crate) summary: String,
}

#[derive(Debug, Clone)]
pub(crate) struct UnprocessedVersionRowView {
    pub(crate) crate_version: String,
    pub(crate) crate_release_datetime: String,
    pub(crate) dso_version: String,
    pub(crate) materialized_actions: usize,
    pub(crate) active_queue_actions: usize,
    pub(crate) root_queue_state_key: String,
    pub(crate) root_queue_state_label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FailedActionRowView {
    pub(crate) action_id: String,
    pub(crate) failed_utc: DateTime<Utc>,
    pub(crate) action_kind: String,
    pub(crate) dso_version: Option<String>,
    pub(crate) subject: String,
    pub(crate) error_summary: String,
}

#[derive(Debug, Default)]
pub(crate) struct VersionAggregate {
    pub(crate) total_materialized: usize,
    pub(crate) failed_total: usize,
    pub(crate) dso_versions: BTreeSet<String>,
    pub(crate) failed_by_kind: BTreeMap<String, FailedKindAggregate>,
    pub(crate) failures: Vec<FailedActionRowView>,
}

#[derive(Debug, Default)]
pub(crate) struct FailedKindAggregate {
    pub(crate) count: usize,
    pub(crate) timeout_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct VersionCardsReport {
    pub(crate) cards: Vec<VersionCardView>,
    pub(crate) unattributed_actions: Vec<UnattributedActionView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct UnattributedActionView {
    pub(crate) source: UnattributedActionSource,
    pub(crate) action_id: String,
    pub(crate) action_kind: String,
    pub(crate) dso_version: Option<String>,
    pub(crate) reason: CrateVersionInferenceMissReason,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum UnattributedActionSource {
    Materialized,
    Failed,
}

impl UnattributedActionSource {
    pub(crate) fn as_label(self) -> &'static str {
        match self {
            Self::Materialized => "materialized",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CrateVersionInferenceMissReason {
    UnmappedDsoVersion,
    MissingDependencyProvenance,
    MissingDependencyCrateContext,
    InvariantDriverVersionPresent,
    InvariantMappedDsoPresent,
    NoVersionContext,
}

impl CrateVersionInferenceMissReason {
    pub(crate) fn as_label(self) -> &'static str {
        match self {
            Self::UnmappedDsoVersion => "unmapped_dso_version",
            Self::MissingDependencyProvenance => "missing_dependency_provenance",
            Self::MissingDependencyCrateContext => "missing_dependency_crate_context",
            Self::InvariantDriverVersionPresent => "invariant_driver_version_present",
            Self::InvariantMappedDsoPresent => "invariant_mapped_dso_present",
            Self::NoVersionContext => "no_version_context",
        }
    }

    pub(crate) fn is_invariant_failure(self) -> bool {
        matches!(
            self,
            Self::InvariantDriverVersionPresent | Self::InvariantMappedDsoPresent
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct QueueLiveStatusView {
    pub(crate) updated_utc: DateTime<Utc>,
    pub(crate) pending: usize,
    pub(crate) pending_expanders: usize,
    pub(crate) pending_non_expanders: usize,
    pub(crate) pending_is_lower_bound: bool,
    pub(crate) running: usize,
    pub(crate) running_expanders: usize,
    pub(crate) running_non_expanders: usize,
    pub(crate) running_owned: usize,
    pub(crate) running_foreign: usize,
    pub(crate) runner_enabled: bool,
    pub(crate) runner_paused: bool,
    pub(crate) runner_drained: bool,
    pub(crate) runner_sync_pending: bool,
    pub(crate) runner_last_sync_utc: Option<DateTime<Utc>>,
    pub(crate) runner_last_sync_error: Option<String>,
    pub(crate) failed: usize,
    pub(crate) canceled: usize,
    pub(crate) done: usize,
    pub(crate) running_actions: Vec<RunningActionLiveView>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RunningActionLiveView {
    pub(crate) action_id: String,
    pub(crate) action_kind: String,
    pub(crate) subject: String,
    pub(crate) crate_version: String,
    pub(crate) dso_version: Option<String>,
    pub(crate) lease_owner: String,
    pub(crate) owned_by_this_runner: bool,
    pub(crate) lease_expires_utc: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(crate) struct ActionDetailRecords {
    pub(crate) provenance: Option<Provenance>,
    pub(crate) pending: Option<QueueItem>,
    pub(crate) running: Option<QueueRunning>,
    pub(crate) done: Option<QueueDone>,
    pub(crate) failed: Option<QueueFailed>,
    pub(crate) canceled: Option<QueueCanceled>,
}

#[derive(Debug, Clone)]
pub(crate) struct RelatedActionRowView {
    pub(crate) action_id: String,
    pub(crate) distance_hops: usize,
    pub(crate) queue_state_key: String,
    pub(crate) queue_state_label: String,
    pub(crate) action_kind: String,
    pub(crate) subject: String,
    pub(crate) output_artifact: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ActionRelatedActionsView {
    pub(crate) upstream: Vec<RelatedActionRowView>,
    pub(crate) downstream: Vec<RelatedActionRowView>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct StdlibSampleDetailsView {
    pub(crate) ir_action_id: String,
    pub(crate) ir_artifact_relpath: String,
    pub(crate) dslx_file: Option<String>,
    pub(crate) dslx_fn_name: Option<String>,
    pub(crate) dslx_subtree_action_id: Option<String>,
    pub(crate) ir_top_fn_name: Option<String>,
    pub(crate) ir_top_fn_text: Option<String>,
    pub(crate) ir_text: String,
    pub(crate) dslx_text: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct AssociatedIrView {
    pub(crate) ir_action_id: String,
    pub(crate) ir_artifact_relpath: String,
    pub(crate) ir_top_fn_name: Option<String>,
    pub(crate) ir_top_fn_text: Option<String>,
    pub(crate) ir_text: String,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StdlibMetric {
    AndNodes,
    Depth,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StdlibTrendKind {
    G8r,
    YosysAbc,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum G8rVsYosysViewScope {
    Stdlib,
    IrFnCorpus,
    IrFnCorpusG8rAbcVsCodegenYosysAbc,
}

impl G8rVsYosysViewScope {
    pub(crate) fn title(self) -> &'static str {
        match self {
            Self::Stdlib => "DSLX Fn G8r vs Yosys/ABC",
            Self::IrFnCorpus => "IR Fn Corpus G8r vs Yosys/ABC",
            Self::IrFnCorpusG8rAbcVsCodegenYosysAbc => "IR Fn Corpus G8r+ABC vs Codegen+Yosys/ABC",
        }
    }

    pub(crate) fn route_label(self) -> &'static str {
        match self {
            Self::Stdlib => "/dslx-fns-g8r-vs-yosys-abc/",
            Self::IrFnCorpus => "/ir-fn-corpus-g8r-vs-yosys-abc/",
            Self::IrFnCorpusG8rAbcVsCodegenYosysAbc => "/ir-fn-g8r-abc-vs-codegen-yosys-abc/",
        }
    }

    pub(crate) fn supports_fraig_toggle(self) -> bool {
        matches!(self, Self::Stdlib)
    }

    pub(crate) fn lhs_label(self) -> &'static str {
        match self {
            Self::Stdlib | Self::IrFnCorpus => "g8r",
            Self::IrFnCorpusG8rAbcVsCodegenYosysAbc => "g8r+abc",
        }
    }

    pub(crate) fn rhs_label(self) -> &'static str {
        match self {
            Self::Stdlib | Self::IrFnCorpus => "yabc",
            Self::IrFnCorpusG8rAbcVsCodegenYosysAbc => "codegen+yabc",
        }
    }
}

impl StdlibTrendKind {
    pub(crate) fn view_path(self) -> &'static str {
        match self {
            Self::G8r => "/dslx-fns-g8r/",
            Self::YosysAbc => "/dslx-fns-yosys-abc/",
        }
    }

    pub(crate) fn title(self) -> &'static str {
        match self {
            Self::G8r => "DSLX Fn G8r Trends",
            Self::YosysAbc => "DSLX Fn Yosys/ABC Trends",
        }
    }

    pub(crate) fn no_data_message(self) -> &'static str {
        match self {
            Self::G8r => {
                "No matching g8r AIG stats found yet for this filter. \
Run more queued actions or adjust `fraig` / file filter."
            }
            Self::YosysAbc => {
                "No matching yosys/abc AIG stats found yet for this filter. \
Run more queued actions or adjust file filter."
            }
        }
    }

    pub(crate) fn supports_fraig(self) -> bool {
        matches!(self, Self::G8r)
    }
}

impl StdlibMetric {
    pub(crate) fn from_query(value: Option<&str>) -> Self {
        match value.unwrap_or("and_nodes") {
            "depth" => Self::Depth,
            _ => Self::AndNodes,
        }
    }

    pub(crate) fn as_query_value(self) -> &'static str {
        match self {
            Self::AndNodes => "and_nodes",
            Self::Depth => "depth",
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::AndNodes => "AIG and_nodes",
            Self::Depth => "AIG depth",
        }
    }

    pub(crate) fn value(self, point: &StdlibFnTrendPoint) -> f64 {
        match self {
            Self::AndNodes => point.and_nodes,
            Self::Depth => point.depth,
        }
    }

    pub(crate) fn lower_is_better(self) -> bool {
        match self {
            Self::AndNodes | Self::Depth => true,
        }
    }

    pub(crate) fn delta_class(self, delta: f64) -> &'static str {
        if delta > 0.0 {
            if self.lower_is_better() { "down" } else { "up" }
        } else if delta < 0.0 {
            if self.lower_is_better() { "up" } else { "down" }
        } else {
            "flat"
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StdlibFnTrendPoint {
    pub(crate) crate_version: String,
    pub(crate) dso_version: String,
    pub(crate) and_nodes: f64,
    pub(crate) depth: f64,
    pub(crate) created_utc: DateTime<Utc>,
    pub(crate) stats_action_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StdlibFnTrendSeries {
    pub(crate) fn_key: String,
    pub(crate) points: Vec<StdlibFnTrendPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StdlibFnsTrendDataset {
    pub(crate) kind: StdlibTrendKind,
    pub(crate) fraig: bool,
    pub(crate) crate_versions: Vec<String>,
    pub(crate) series: Vec<StdlibFnTrendSeries>,
    pub(crate) total_points: usize,
    pub(crate) available_files: Vec<String>,
    pub(crate) selected_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StdlibG8rVsYosysDataset {
    pub(crate) fraig: bool,
    pub(crate) samples: Vec<StdlibG8rVsYosysSample>,
    pub(crate) min_ir_nodes: u64,
    pub(crate) max_ir_nodes: u64,
    pub(crate) g8r_only_count: usize,
    pub(crate) yosys_only_count: usize,
    pub(crate) available_crate_versions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StdlibFnVersionTimelinePoint {
    pub(crate) crate_version: String,
    pub(crate) g8r_dso_version: Option<String>,
    pub(crate) yosys_abc_dso_version: Option<String>,
    pub(crate) ir_delay_dso_version: Option<String>,
    pub(crate) g8r_and_nodes: Option<f64>,
    pub(crate) g8r_levels: Option<f64>,
    pub(crate) g8r_stats_action_id: Option<String>,
    pub(crate) yosys_abc_and_nodes: Option<f64>,
    pub(crate) yosys_abc_levels: Option<f64>,
    pub(crate) yosys_abc_stats_action_id: Option<String>,
    pub(crate) ir_delay_ps: Option<f64>,
    pub(crate) ir_delay_action_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StdlibFnVersionTimelineDataset {
    pub(crate) file_selector: String,
    pub(crate) dslx_file: Option<String>,
    pub(crate) dslx_fn_name: String,
    pub(crate) fraig: bool,
    pub(crate) delay_model: String,
    pub(crate) points: Vec<StdlibFnVersionTimelinePoint>,
    pub(crate) crate_versions: Vec<String>,
    pub(crate) matched_files: Vec<String>,
    pub(crate) available_files: Vec<String>,
    pub(crate) available_functions_for_file: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StdlibG8rVsYosysSample {
    pub(crate) fn_key: String,
    pub(crate) crate_version: String,
    pub(crate) dso_version: String,
    pub(crate) ir_action_id: String,
    pub(crate) ir_top: Option<String>,
    #[serde(default)]
    pub(crate) structural_hash: Option<String>,
    pub(crate) ir_node_count: u64,
    pub(crate) g8r_nodes: f64,
    pub(crate) g8r_levels: f64,
    pub(crate) yosys_abc_nodes: f64,
    pub(crate) yosys_abc_levels: f64,
    pub(crate) g8r_product: f64,
    pub(crate) yosys_abc_product: f64,
    pub(crate) g8r_product_loss: f64,
    pub(crate) g8r_stats_action_id: String,
    pub(crate) yosys_abc_stats_action_id: String,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum K3LossChangeStatus {
    Regressed,
    NewlyLossy,
    Same,
    Improved,
    Resolved,
    MissingCurrent,
}

impl K3LossChangeStatus {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Regressed => "regressed",
            Self::NewlyLossy => "newly_lossy",
            Self::Same => "same",
            Self::Improved => "improved",
            Self::Resolved => "resolved",
            Self::MissingCurrent => "missing_current",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct K3LossByVersionSampleRef {
    pub(crate) crate_version: String,
    pub(crate) g8r_product_loss: f64,
    pub(crate) g8r_nodes: f64,
    pub(crate) g8r_levels: f64,
    pub(crate) yosys_abc_nodes: f64,
    pub(crate) yosys_abc_levels: f64,
    pub(crate) g8r_stats_action_id: String,
    pub(crate) yosys_abc_stats_action_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct K3LossByVersionRow {
    pub(crate) fn_key: String,
    pub(crate) ir_top: Option<String>,
    pub(crate) structural_hash: String,
    pub(crate) ir_node_count: u64,
    pub(crate) status: K3LossChangeStatus,
    pub(crate) version_losses: Vec<Option<f64>>,
    pub(crate) current_sample: Option<K3LossByVersionSampleRef>,
    pub(crate) previous_sample: Option<K3LossByVersionSampleRef>,
    pub(crate) delta_vs_previous: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct K3LossByVersionStatusCounts {
    pub(crate) regressed: usize,
    pub(crate) newly_lossy: usize,
    pub(crate) same: usize,
    pub(crate) improved: usize,
    pub(crate) resolved: usize,
    pub(crate) missing_current: usize,
}

impl K3LossByVersionStatusCounts {
    pub(crate) fn increment(&mut self, status: K3LossChangeStatus) {
        match status {
            K3LossChangeStatus::Regressed => self.regressed += 1,
            K3LossChangeStatus::NewlyLossy => self.newly_lossy += 1,
            K3LossChangeStatus::Same => self.same += 1,
            K3LossChangeStatus::Improved => self.improved += 1,
            K3LossChangeStatus::Resolved => self.resolved += 1,
            K3LossChangeStatus::MissingCurrent => self.missing_current += 1,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct K3LossByVersionDataset {
    pub(crate) available_crate_versions: Vec<String>,
    pub(crate) compared_crate_versions: Vec<String>,
    pub(crate) selected_crate_version: Option<String>,
    pub(crate) show_same: bool,
    pub(crate) min_ir_nodes: u64,
    pub(crate) max_ir_nodes: u64,
    pub(crate) applied_max_ir_nodes: u64,
    pub(crate) rows: Vec<K3LossByVersionRow>,
    pub(crate) status_counts: K3LossByVersionStatusCounts,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct StdlibFileActionGraphNode {
    pub(crate) action_id: String,
    pub(crate) label: String,
    pub(crate) kind: String,
    pub(crate) subject: String,
    pub(crate) state_label: String,
    pub(crate) state_key: String,
    pub(crate) has_provenance: bool,
    pub(crate) is_root: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct StdlibFileActionGraphEdge {
    pub(crate) source: String,
    pub(crate) target: String,
    pub(crate) edge_kind: String,
    pub(crate) role: String,
}

#[derive(Debug, Clone)]
pub(crate) struct StdlibFileActionGraphDataset {
    pub(crate) available_crate_versions: Vec<String>,
    pub(crate) selected_crate_version: Option<String>,
    pub(crate) available_files: Vec<String>,
    pub(crate) selected_file: Option<String>,
    pub(crate) available_functions: Vec<String>,
    pub(crate) selected_function: Option<String>,
    pub(crate) include_k3_descendants: bool,
    pub(crate) selected_action_id: Option<String>,
    pub(crate) action_focus_found: bool,
    pub(crate) root_action_ids: Vec<String>,
    pub(crate) total_actions_for_crate: usize,
    pub(crate) nodes: Vec<StdlibFileActionGraphNode>,
    pub(crate) edges: Vec<StdlibFileActionGraphEdge>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct RawActionGraphEdge {
    pub(crate) source: String,
    pub(crate) target: String,
    pub(crate) edge_kind: String,
    pub(crate) role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StdlibAigStatsPoint {
    pub(crate) fn_key: String,
    pub(crate) ir_action_id: String,
    pub(crate) ir_top: Option<String>,
    pub(crate) crate_version: String,
    pub(crate) dso_version: String,
    pub(crate) and_nodes: f64,
    pub(crate) depth: f64,
    pub(crate) created_utc: DateTime<Utc>,
    pub(crate) stats_action_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StdlibFnDelayPoint {
    pub(crate) crate_version: String,
    pub(crate) dso_version: String,
    pub(crate) delay_ps: f64,
    pub(crate) created_utc: DateTime<Utc>,
    pub(crate) action_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct IrFnCorpusStructuralGroupRowView {
    pub(crate) structural_hash: String,
    pub(crate) member_count: usize,
    pub(crate) ir_node_count: Option<u64>,
    pub(crate) g8r_loss_sample_count: usize,
}
