// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, bail};
use chrono::Utc;
use prost::Message;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};
use walkdir::WalkDir;

use crate::model::{ActionSpec, QueueCanceled, QueueCancellationKind};
use crate::proto::v1 as pb;
use crate::proto::{
    DEFAULT_RELEASE_CAMPAIGN, action_id_to_hex, action_id_to_proto, action_spec_from_proto,
    action_spec_to_proto, compute_model_action_id_v2, driver_runtime_from_proto,
    driver_runtime_to_proto, timestamp_from_proto, timestamp_to_proto,
};
use crate::query::{
    canonical_root_actions_for_crate_version, canonical_root_actions_for_runtime,
    enqueue_processing_for_root_actions, is_timeout_error, load_stdlib_g8r_vs_yosys_dataset_index,
    load_versions_cards_index, stdlib_enumeration_status_from_provenance,
};
use crate::queue::{
    QueueState, action_dependency_action_ids, load_queue_canceled_record, queue_state_for_action,
};
use crate::runtime::{
    explicit_driver_runtime_for_crate_version, explicit_driver_runtime_recipe_for_crate_version,
};
use crate::store::ArtifactStore;
use crate::versioning::{
    cmp_dotted_numeric_version, load_version_compat_map, normalize_tag_version,
    resolve_xlsynth_version_for_driver,
};
use crate::view::{StdlibEnumerationState, StdlibEnumerationStatusView, StdlibG8rVsYosysDataset};
use crate::{
    WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME, WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
};

const CAMPAIGN_IDENTITY_VERSION: u32 = 1;
const CAMPAIGN_RUN_IDENTITY_VERSION: u32 = 1;
const CAMPAIGN_RUN_RECORD_VERSION: u32 = 1;
const CAMPAIGN_ID_DOMAIN: &[u8] = b"xlsynth-bvc/campaign/v1\0";
const CAMPAIGN_RUN_ID_DOMAIN: &[u8] = b"xlsynth-bvc/campaign-run/v1\0";
const WORK_POLICY_RULE_FINGERPRINT_DOMAIN: &[u8] = b"xlsynth-bvc/work-policy-rule/v1\0";
pub(crate) const CAMPAIGN_RUN_MANIFEST_FILENAME: &str = "run-manifest.pb";
pub(crate) const CAMPAIGN_ANALYSIS_FILENAME: &str = "analysis.pb";

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CampaignRunSummary {
    pub(crate) campaign_id: String,
    pub(crate) run_id: String,
    pub(crate) campaign_name: String,
    pub(crate) crate_version: String,
    pub(crate) dso_version: String,
    pub(crate) status: String,
    pub(crate) root_action_count: u64,
    pub(crate) completed_root_count: u64,
    pub(crate) pending_count: u64,
    pub(crate) running_count: u64,
    pub(crate) failed_count: u64,
    pub(crate) canceled_count: u64,
    pub(crate) missing_outputs: Vec<String>,
    pub(crate) failed_samples: Vec<String>,
    pub(crate) intentionally_skipped_samples: Vec<String>,
    pub(crate) manifest_path: String,
    pub(crate) persisted: bool,
}

fn required<'a, T>(value: &'a Option<T>, field: &str) -> Result<&'a T> {
    value
        .as_ref()
        .with_context(|| format!("missing required protobuf field {field}"))
}

fn validate_nonempty(value: &str, field: &str) -> Result<()> {
    if value.is_empty() || value.trim() != value || value.contains('\0') {
        bail!("{field} must be nonempty, trimmed, and contain no NUL");
    }
    Ok(())
}

fn normalize_version(value: &str, field: &str) -> Result<String> {
    validate_nonempty(value, field)?;
    let normalized = normalize_tag_version(value);
    let (base, suffix) = normalized
        .split_once('-')
        .map_or((normalized, None), |(base, suffix)| (base, Some(suffix)));
    let parts: Vec<&str> = base.split('.').collect();
    if parts.len() != 3
        || parts
            .iter()
            .any(|part| part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()))
    {
        bail!("{field} must be a numeric X.Y.Z version, got {value:?}");
    }
    if let Some(suffix) = suffix
        && (suffix.is_empty() || !suffix.bytes().all(|b| b.is_ascii_digit()))
    {
        bail!("{field} suffix must be numeric, got {value:?}");
    }
    Ok(normalized.to_string())
}

fn validate_digest(value: &pb::Sha256Digest, field: &str) -> Result<()> {
    if value.value.len() != 32 {
        bail!("{field} must contain exactly 32 bytes");
    }
    Ok(())
}

fn digest_hex(value: &pb::Sha256Digest, field: &str) -> Result<String> {
    validate_digest(value, field)?;
    Ok(hex::encode(&value.value))
}

fn domain_hash(domain: &[u8], payload: &[u8]) -> pb::Sha256Digest {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(payload);
    pb::Sha256Digest {
        value: hasher.finalize().to_vec(),
    }
}

fn normalize_campaign(mut campaign: pb::CampaignSpec) -> Result<pb::CampaignSpec> {
    validate_nonempty(&campaign.campaign_name, "campaign.campaign_name")?;
    validate_nonempty(&campaign.corpus_source, "campaign.corpus_source")?;
    if campaign.semantic_version == 0
        || campaign.analysis_algorithm_version == 0
        || campaign.publication_policy_version == 0
    {
        bail!("campaign semantic, analysis, and publication versions must be nonzero");
    }
    match pb::CampaignRootPolicy::try_from(campaign.root_policy)
        .context("campaign.root_policy is unknown")?
    {
        pb::CampaignRootPolicy::ReleaseStdlibAndModules => {}
        pb::CampaignRootPolicy::Unspecified => bail!("campaign.root_policy must be specified"),
    }
    required(&campaign.failure_policy, "campaign.failure_policy")?;
    if campaign.required_outputs.is_empty() {
        bail!("campaign.required_outputs must not be empty");
    }
    campaign.required_outputs.sort_unstable();
    let mut previous = None;
    for raw in &campaign.required_outputs {
        let kind = pb::RequiredOutputKind::try_from(*raw)
            .context("campaign.required_outputs contains an unknown value")?;
        if kind == pb::RequiredOutputKind::Unspecified {
            bail!("campaign.required_outputs must not contain UNSPECIFIED");
        }
        if previous == Some(*raw) {
            bail!("campaign.required_outputs contains duplicate {kind:?}");
        }
        previous = Some(*raw);
    }
    let work_policy = campaign
        .work_policy
        .as_mut()
        .context("campaign.work_policy is required")?;
    work_policy.rules.sort_by(|a, b| a.rule_id.cmp(&b.rule_id));
    let mut previous_rule_id = None;
    for rule in &mut work_policy.rules {
        validate_nonempty(&rule.rule_id, "campaign.work_policy.rules.rule_id")?;
        validate_nonempty(&rule.top_name, "campaign.work_policy.rules.top_name")?;
        validate_nonempty(&rule.reason, "campaign.work_policy.rules.reason")?;
        match pb::CampaignWorkDecision::try_from(rule.decision)
            .context("campaign work rule decision is unknown")?
        {
            pb::CampaignWorkDecision::Exclude => {}
            pb::CampaignWorkDecision::Unspecified => {
                bail!("campaign work rule decision must be specified")
            }
        }
        if previous_rule_id.as_deref() == Some(rule.rule_id.as_str()) {
            bail!(
                "campaign work policy contains duplicate rule id {:?}",
                rule.rule_id
            );
        }
        previous_rule_id = Some(rule.rule_id.clone());
        if rule.action_kinds.is_empty() {
            bail!("campaign work rule action_kinds must not be empty");
        }
        rule.action_kinds.sort_unstable();
        let mut previous_kind = None;
        for raw in &rule.action_kinds {
            let kind = pb::CampaignActionKind::try_from(*raw)
                .context("campaign work rule action kind is unknown")?;
            if kind == pb::CampaignActionKind::Unspecified {
                bail!("campaign work rule action kind must be specified");
            }
            if previous_kind == Some(*raw) {
                bail!("campaign work rule contains duplicate action kind {kind:?}");
            }
            previous_kind = Some(*raw);
        }
    }
    Ok(campaign)
}

pub(crate) fn load_default_campaign() -> Result<pb::CampaignSpec> {
    let campaign = pb::CampaignSpec::decode(DEFAULT_RELEASE_CAMPAIGN)
        .context("decoding embedded release campaign protobuf")?;
    normalize_campaign(campaign)
}

pub(crate) fn matching_work_policy_exclusion<'a>(
    action: &ActionSpec,
    work_policy: &'a pb::CampaignWorkPolicy,
) -> Result<Option<&'a pb::CampaignWorkRule>> {
    let (action_kind, top_name) = match action {
        ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_top_module_name: Some(top_name),
            ..
        } => (
            pb::CampaignActionKind::ComboVerilogToYosysAbcAig,
            top_name.as_str(),
        ),
        _ => return Ok(None),
    };

    for rule in &work_policy.rules {
        let decision = pb::CampaignWorkDecision::try_from(rule.decision)
            .context("campaign work rule decision is unknown")?;
        if decision == pb::CampaignWorkDecision::Exclude
            && rule
                .action_kinds
                .iter()
                .any(|raw| *raw == action_kind as i32)
            && rule.top_name == top_name
        {
            return Ok(Some(rule));
        }
    }
    Ok(None)
}

pub(crate) fn work_policy_rule_fingerprint(rule: &pb::CampaignWorkRule) -> Result<String> {
    let mut normalized = rule.clone();
    validate_nonempty(&normalized.rule_id, "campaign work rule rule_id")?;
    validate_nonempty(&normalized.top_name, "campaign work rule top_name")?;
    validate_nonempty(&normalized.reason, "campaign work rule reason")?;
    if pb::CampaignWorkDecision::try_from(normalized.decision)
        .context("campaign work rule decision is unknown")?
        != pb::CampaignWorkDecision::Exclude
    {
        bail!("campaign work rule fingerprint requires an exclusion decision");
    }
    if normalized.action_kinds.is_empty() {
        bail!("campaign work rule action_kinds must not be empty");
    }
    normalized.action_kinds.sort_unstable();
    let mut previous = None;
    for raw in &normalized.action_kinds {
        let kind = pb::CampaignActionKind::try_from(*raw)
            .context("campaign work rule action kind is unknown")?;
        if kind == pb::CampaignActionKind::Unspecified || previous == Some(*raw) {
            bail!("campaign work rule fingerprint requires unique concrete action kinds");
        }
        previous = Some(*raw);
    }
    digest_hex(
        &domain_hash(
            WORK_POLICY_RULE_FINGERPRINT_DOMAIN,
            &normalized.encode_to_vec(),
        ),
        "campaign work rule fingerprint",
    )
}

fn current_work_policy_exclusion<'a>(
    canceled: &QueueCanceled,
    work_policy: &'a pb::CampaignWorkPolicy,
) -> Result<Option<&'a pb::CampaignWorkRule>> {
    if canceled.cancellation_kind != QueueCancellationKind::WorkPolicyExcluded {
        return Ok(None);
    }
    let Some(rule) = matching_work_policy_exclusion(&canceled.action, work_policy)? else {
        return Ok(None);
    };
    let fingerprint = work_policy_rule_fingerprint(rule)?;
    if canceled.work_policy_rule_id.as_deref() == Some(rule.rule_id.as_str())
        && canceled.work_policy_rule_fingerprint.as_deref() == Some(fingerprint.as_str())
        && canceled.reason == rule.reason
    {
        Ok(Some(rule))
    } else {
        Ok(None)
    }
}

pub(crate) fn compute_campaign_id(campaign: &pb::CampaignSpec) -> Result<pb::Sha256Digest> {
    let campaign = normalize_campaign(campaign.clone())?;
    let identity = pb::CampaignIdentity {
        identity_version: CAMPAIGN_IDENTITY_VERSION,
        campaign: Some(campaign),
    };
    Ok(domain_hash(CAMPAIGN_ID_DOMAIN, &identity.encode_to_vec()))
}

fn compute_run_id(identity: &pb::CampaignRunIdentity) -> Result<pb::Sha256Digest> {
    if identity.identity_version != CAMPAIGN_RUN_IDENTITY_VERSION {
        bail!(
            "unsupported campaign run identity version {}",
            identity.identity_version
        );
    }
    validate_digest(
        required(&identity.campaign_id, "run_identity.campaign_id")?,
        "run_identity.campaign_id",
    )?;
    normalize_version(
        &required(&identity.crate_version, "run_identity.crate_version")?.value,
        "run_identity.crate_version",
    )?;
    normalize_version(
        &required(&identity.dso_version, "run_identity.dso_version")?.value,
        "run_identity.dso_version",
    )?;
    driver_runtime_from_proto(
        required(&identity.driver_runtime, "run_identity.driver_runtime")?,
        "run_identity.driver_runtime",
    )?;
    Ok(domain_hash(
        CAMPAIGN_RUN_ID_DOMAIN,
        &identity.encode_to_vec(),
    ))
}

pub(crate) fn campaign_run_path(
    store: &ArtifactStore,
    run_id: &pb::Sha256Digest,
) -> Result<PathBuf> {
    let run_id = digest_hex(run_id, "run_id")?;
    Ok(store
        .campaign_runs_dir()
        .join(&run_id[0..2])
        .join(&run_id[2..4])
        .join(run_id)
        .join(CAMPAIGN_RUN_MANIFEST_FILENAME))
}

pub(crate) fn campaign_analysis_path(
    store: &ArtifactStore,
    run_id: &pb::Sha256Digest,
) -> Result<PathBuf> {
    Ok(campaign_run_path(store, run_id)?
        .parent()
        .expect("campaign manifest has a parent")
        .join(CAMPAIGN_ANALYSIS_FILENAME))
}

fn canonical_roots(
    repo_root: &Path,
    crate_version: &str,
    dso_version: &str,
) -> Result<Vec<pb::CampaignRootAction>> {
    let roots = canonical_root_actions_for_crate_version(repo_root, crate_version, dso_version)?;
    let mut result = Vec::with_capacity(roots.len());
    for action in roots {
        let action_id = compute_model_action_id_v2(&action)?.to_hex();
        result.push(pb::CampaignRootAction {
            action_id: Some(action_id_to_proto(
                &action_id,
                "campaign.root_action.action_id",
            )?),
            action: Some(action_spec_to_proto(&action)?),
        });
    }
    result.sort_by(|a, b| {
        a.action_id
            .as_ref()
            .map(|id| id.value.as_slice())
            .cmp(&b.action_id.as_ref().map(|id| id.value.as_slice()))
    });
    Ok(result)
}

fn canonical_roots_for_runtime(
    dso_version: &str,
    runtime: &crate::model::DriverRuntimeSpec,
) -> Result<Vec<pb::CampaignRootAction>> {
    let roots = canonical_root_actions_for_runtime(dso_version, runtime)?;
    let mut result = Vec::with_capacity(roots.len());
    for action in roots {
        let action_id = compute_model_action_id_v2(&action)?.to_hex();
        result.push(pb::CampaignRootAction {
            action_id: Some(action_id_to_proto(
                &action_id,
                "campaign.root_action.action_id",
            )?),
            action: Some(action_spec_to_proto(&action)?),
        });
    }
    result.sort_by(|a, b| {
        a.action_id
            .as_ref()
            .map(|id| id.value.as_slice())
            .cmp(&b.action_id.as_ref().map(|id| id.value.as_slice()))
    });
    Ok(result)
}

fn same_driver_runtime_recipe(
    lhs: &crate::model::DriverRuntimeSpec,
    rhs: &crate::model::DriverRuntimeSpec,
) -> bool {
    lhs.driver_version == rhs.driver_version
        && lhs.release_platform == rhs.release_platform
        && lhs.docker_image == rhs.docker_image
        && lhs.dockerfile == rhs.dockerfile
        && lhs.dockerfile_sha256 == rhs.dockerfile_sha256
        && lhs.release_cache_input_sha256 == rhs.release_cache_input_sha256
}

fn new_manifest(
    repo_root: &Path,
    requested_crate_version: &str,
) -> Result<pb::CampaignRunManifest> {
    let campaign = load_default_campaign()?;
    let campaign_id = compute_campaign_id(&campaign)?;
    let crate_version = normalize_version(requested_crate_version, "crate_version")?;
    let dso_with_v = resolve_xlsynth_version_for_driver(repo_root, &crate_version)?;
    let dso_version = normalize_version(&dso_with_v, "dso_version")?;
    let runtime =
        explicit_driver_runtime_for_crate_version(repo_root, &crate_version, &dso_version)?;
    let runtime_pb = driver_runtime_to_proto(&runtime, "driver_runtime")?;
    let identity = pb::CampaignRunIdentity {
        identity_version: CAMPAIGN_RUN_IDENTITY_VERSION,
        campaign_id: Some(campaign_id.clone()),
        crate_version: Some(pb::CrateVersion {
            value: crate_version.clone(),
        }),
        dso_version: Some(pb::DsoVersion {
            value: dso_version.clone(),
        }),
        driver_runtime: Some(runtime_pb.clone()),
    };
    let run_id = compute_run_id(&identity)?;
    let now = Utc::now();
    let root_actions = canonical_roots(repo_root, &crate_version, &dso_version)?;
    let root_action_count = root_actions.len() as u64;
    Ok(pb::CampaignRunManifest {
        record_version: CAMPAIGN_RUN_RECORD_VERSION,
        campaign_id: Some(campaign_id),
        run_id: Some(run_id),
        campaign: Some(campaign),
        crate_version: Some(pb::CrateVersion {
            value: crate_version,
        }),
        dso_version: Some(pb::DsoVersion { value: dso_version }),
        driver_runtime: Some(runtime_pb),
        root_actions,
        status: pb::CampaignRunStatus::Building as i32,
        created_at: Some(timestamp_to_proto(&now)),
        updated_at: Some(timestamp_to_proto(&now)),
        completion: Some(pb::CompletionReport {
            status: pb::CampaignRunStatus::Building as i32,
            root_action_count,
            ..Default::default()
        }),
    })
}

fn action_id_from_root(root: &pb::CampaignRootAction) -> Result<String> {
    action_id_to_hex(
        required(&root.action_id, "campaign.root_actions.action_id")?,
        "campaign.root_actions.action_id",
    )
}

fn validate_manifest(manifest: &pb::CampaignRunManifest) -> Result<()> {
    if manifest.record_version != CAMPAIGN_RUN_RECORD_VERSION {
        bail!(
            "unsupported campaign run record version {}; expected {}",
            manifest.record_version,
            CAMPAIGN_RUN_RECORD_VERSION
        );
    }
    let campaign =
        normalize_campaign(required(&manifest.campaign, "campaign_run.campaign")?.clone())?;
    let expected_campaign_id = compute_campaign_id(&campaign)?;
    let campaign_id = required(&manifest.campaign_id, "campaign_run.campaign_id")?;
    validate_digest(campaign_id, "campaign_run.campaign_id")?;
    if campaign_id != &expected_campaign_id {
        bail!("campaign_run.campaign_id does not match campaign contents");
    }
    let crate_version = required(&manifest.crate_version, "campaign_run.crate_version")?;
    let dso_version = required(&manifest.dso_version, "campaign_run.dso_version")?;
    if normalize_version(&crate_version.value, "campaign_run.crate_version")? != crate_version.value
        || normalize_version(&dso_version.value, "campaign_run.dso_version")? != dso_version.value
    {
        bail!("campaign run versions must use canonical values without a leading v");
    }
    let runtime = required(&manifest.driver_runtime, "campaign_run.driver_runtime")?;
    let runtime_model = driver_runtime_from_proto(runtime, "campaign_run.driver_runtime")?;
    if normalize_version(
        &runtime_model.driver_version,
        "driver_runtime.driver_version",
    )? != crate_version.value
    {
        bail!("campaign run runtime crate version does not match crate_version");
    }
    let identity = pb::CampaignRunIdentity {
        identity_version: CAMPAIGN_RUN_IDENTITY_VERSION,
        campaign_id: Some(campaign_id.clone()),
        crate_version: Some(crate_version.clone()),
        dso_version: Some(dso_version.clone()),
        driver_runtime: Some(runtime.clone()),
    };
    let expected_run_id = compute_run_id(&identity)?;
    let run_id = required(&manifest.run_id, "campaign_run.run_id")?;
    validate_digest(run_id, "campaign_run.run_id")?;
    if run_id != &expected_run_id {
        bail!("campaign_run.run_id does not match run identity");
    }
    if manifest.root_actions.is_empty() {
        bail!("campaign_run.root_actions must not be empty");
    }
    let mut prior: Option<Vec<u8>> = None;
    for root in &manifest.root_actions {
        let id = action_id_from_root(root)?;
        let action_pb = required(&root.action, "campaign_run.root_actions.action")?;
        let action = action_spec_from_proto(action_pb)?;
        let expected_id = compute_model_action_id_v2(&action)?.to_hex();
        if id != expected_id {
            bail!("root action id {id} does not match its protobuf ActionSpec");
        }
        let bytes = root.action_id.as_ref().expect("validated").value.clone();
        if let Some(prior) = &prior
            && prior >= &bytes
        {
            bail!("campaign_run.root_actions must be strictly sorted by action id");
        }
        prior = Some(bytes);
    }
    let expected_roots = canonical_roots_for_runtime(&dso_version.value, &runtime_model)
        .context("deriving canonical campaign roots from the embedded runtime")?;
    if manifest.root_actions != expected_roots {
        bail!("campaign_run.root_actions do not match the canonical runtime root set");
    }
    let status = pb::CampaignRunStatus::try_from(manifest.status)
        .context("campaign_run.status is unknown")?;
    if status == pb::CampaignRunStatus::Unspecified {
        bail!("campaign_run.status must be specified");
    }
    let created = timestamp_from_proto(&manifest.created_at, "campaign_run.created_at")?;
    let updated = timestamp_from_proto(&manifest.updated_at, "campaign_run.updated_at")?;
    if updated < created {
        bail!("campaign_run.updated_at precedes created_at");
    }
    let completion = required(&manifest.completion, "campaign_run.completion")?;
    let completion_status = pb::CampaignRunStatus::try_from(completion.status)
        .context("campaign_run.completion.status is unknown")?;
    if completion_status != status {
        bail!("campaign run status disagrees with completion report status");
    }
    if completion.root_action_count != manifest.root_actions.len() as u64
        || completion.completed_root_count > completion.root_action_count
    {
        bail!("campaign run completion root counts are inconsistent");
    }
    for missing in &completion.missing_outputs {
        let kind = pb::RequiredOutputKind::try_from(missing.kind)
            .context("completion.missing_outputs.kind is unknown")?;
        if kind == pb::RequiredOutputKind::Unspecified || missing.reason.trim().is_empty() {
            bail!("completion missing outputs require a kind and reason");
        }
        if let Some(action_id) = &missing.action_id {
            action_id_to_hex(action_id, "completion.missing_outputs.action_id")?;
        }
    }
    for failed in &completion.failed_samples {
        action_id_to_hex(
            required(&failed.action_id, "completion.failed_samples.action_id")?,
            "completion.failed_samples.action_id",
        )?;
        validate_nonempty(&failed.error, "completion.failed_samples.error")?;
    }
    for skipped in &completion.intentionally_skipped_samples {
        action_id_to_hex(
            required(
                &skipped.action_id,
                "completion.intentionally_skipped_samples.action_id",
            )?,
            "completion.intentionally_skipped_samples.action_id",
        )?;
        validate_nonempty(
            &skipped.rule_id,
            "completion.intentionally_skipped_samples.rule_id",
        )?;
        validate_nonempty(
            &skipped.reason,
            "completion.intentionally_skipped_samples.reason",
        )?;
    }
    Ok(())
}

fn load_manifest(path: &Path) -> Result<pb::CampaignRunManifest> {
    let bytes = fs::read(path)
        .with_context(|| format!("reading campaign run manifest: {}", path.display()))?;
    let manifest = pb::CampaignRunManifest::decode(bytes.as_slice())
        .with_context(|| format!("decoding campaign run manifest: {}", path.display()))?;
    validate_manifest(&manifest)
        .with_context(|| format!("validating campaign run manifest: {}", path.display()))?;
    Ok(manifest)
}

pub(crate) fn load_campaign_run_file(path: &Path) -> Result<pb::CampaignRunManifest> {
    load_manifest(path)
}

pub(crate) fn list_campaign_runs(store: &ArtifactStore) -> Result<Vec<pb::CampaignRunManifest>> {
    let root = store.campaign_runs_dir();
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut manifests = Vec::new();
    let mut run_ids = BTreeSet::new();
    for entry in WalkDir::new(&root).sort_by_file_name() {
        let entry = entry.with_context(|| format!("walking campaign runs: {}", root.display()))?;
        if !entry.file_type().is_file() && !entry.file_type().is_dir() {
            bail!(
                "campaign record tree contains a symlink or special filesystem node: {}",
                entry.path().display()
            );
        }
        if !entry.file_type().is_file()
            || entry.file_name().to_string_lossy() != CAMPAIGN_RUN_MANIFEST_FILENAME
        {
            continue;
        }
        let manifest = load_manifest(entry.path())?;
        let run_id = required(&manifest.run_id, "campaign_run.run_id")?;
        let expected_path = campaign_run_path(store, run_id)?;
        if entry.path() != expected_path {
            bail!(
                "campaign run path does not match its embedded run_id: {}",
                entry.path().display()
            );
        }
        let run_id = digest_hex(run_id, "campaign_run.run_id")?;
        if !run_ids.insert(run_id.clone()) {
            bail!("campaign record tree contains duplicate run_id {run_id}");
        }
        manifests.push(manifest);
    }
    manifests.sort_by(|a, b| {
        a.run_id
            .as_ref()
            .map(|id| id.value.as_slice())
            .cmp(&b.run_id.as_ref().map(|id| id.value.as_slice()))
    });
    Ok(manifests)
}

pub(crate) fn list_finalized_campaign_runs(
    store: &ArtifactStore,
) -> Result<Vec<pb::CampaignRunManifest>> {
    list_campaign_runs(store).map(|manifests| {
        manifests
            .into_iter()
            .filter(|manifest| {
                matches!(
                    pb::CampaignRunStatus::try_from(manifest.status),
                    Ok(pb::CampaignRunStatus::Complete | pb::CampaignRunStatus::Degraded)
                )
            })
            .collect()
    })
}

pub(crate) fn load_campaign_run_by_id(
    store: &ArtifactStore,
    run_id: &str,
) -> Result<pb::CampaignRunManifest> {
    let bytes = hex::decode(run_id).context("decoding campaign run id as hex")?;
    let digest = pb::Sha256Digest { value: bytes };
    validate_digest(&digest, "campaign run id")?;
    let path = campaign_run_path(store, &digest)?;
    load_manifest(&path)
}

pub(crate) fn pending_campaign_versions(
    store: &ArtifactStore,
    repo_root: &Path,
) -> Result<Vec<String>> {
    let campaign_id = compute_campaign_id(&load_default_campaign()?)?;
    let finalized = list_finalized_campaign_runs(store)?;
    let mut pending = Vec::new();
    for version in load_version_compat_map(repo_root)?.into_keys() {
        let dso_with_v = resolve_xlsynth_version_for_driver(repo_root, &version)?;
        let dso_version = normalize_version(&dso_with_v, "dso_version")?;
        let mut recipe =
            explicit_driver_runtime_recipe_for_crate_version(repo_root, &version, &dso_version)?;
        recipe.release_cache_input_sha256 = crate::service::driver_release_cache_input_sha256(
            repo_root,
            &dso_version,
            &recipe.release_platform,
        )?;
        let is_finalized = finalized.iter().any(|manifest| {
            let Some(manifest_runtime_pb) = manifest.driver_runtime.as_ref() else {
                return false;
            };
            let Ok(manifest_runtime) =
                driver_runtime_from_proto(manifest_runtime_pb, "campaign.driver_runtime")
            else {
                return false;
            };
            let Ok(expected_roots) = canonical_roots_for_runtime(&dso_version, &manifest_runtime)
            else {
                return false;
            };
            manifest.campaign_id.as_ref() == Some(&campaign_id)
                && manifest
                    .crate_version
                    .as_ref()
                    .is_some_and(|value| value.value == version)
                && manifest
                    .dso_version
                    .as_ref()
                    .is_some_and(|value| value.value == dso_version)
                && same_driver_runtime_recipe(&manifest_runtime, &recipe)
                && manifest.root_actions == expected_roots
        });
        if !is_finalized {
            pending.push(version);
        }
    }
    pending.sort_by(|a, b| cmp_dotted_numeric_version(a, b));
    Ok(pending)
}

pub(crate) fn load_finalized_campaign_run_for_version(
    store: &ArtifactStore,
    _repo_root: &Path,
    crate_version: &str,
) -> Result<pb::CampaignRunManifest> {
    let crate_version = normalize_version(crate_version, "crate_version")?;
    let campaign_id = compute_campaign_id(&load_default_campaign()?)?;
    let mut candidates = list_finalized_campaign_runs(store)?
        .into_iter()
        .filter(|manifest| {
            manifest.campaign_id.as_ref() == Some(&campaign_id)
                && manifest
                    .crate_version
                    .as_ref()
                    .is_some_and(|version| version.value == crate_version)
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        bail!(
            "campaign run for crate version {} has not been reconciled/finalized",
            crate_version
        );
    }
    if candidates.len() > 1 {
        let ids = candidates
            .iter()
            .filter_map(|manifest| manifest.run_id.as_ref())
            .map(|id| digest_hex(id, "campaign_run.run_id"))
            .collect::<Result<Vec<_>>>()?;
        bail!(
            "multiple finalized campaign runs exist for crate version {}; select an exact run id: {}",
            crate_version,
            ids.join(", ")
        );
    }
    Ok(candidates.pop().expect("nonempty candidates"))
}

fn load_existing_manifest(
    store: &ArtifactStore,
    planned: &pb::CampaignRunManifest,
) -> Result<Option<pb::CampaignRunManifest>> {
    let path = campaign_run_path(
        store,
        required(&planned.run_id, "planned_campaign_run.run_id")?,
    )?;
    if !path.exists() {
        return Ok(None);
    }
    let existing = load_manifest(&path)?;
    if existing.campaign_id != planned.campaign_id
        || existing.run_id != planned.run_id
        || existing.campaign != planned.campaign
        || existing.crate_version != planned.crate_version
        || existing.dso_version != planned.dso_version
        || existing.driver_runtime != planned.driver_runtime
        || existing.root_actions != planned.root_actions
    {
        bail!(
            "existing campaign run manifest identity differs from current campaign plan: {}",
            path.display()
        );
    }
    Ok(Some(existing))
}

fn write_manifest(store: &ArtifactStore, manifest: &pb::CampaignRunManifest) -> Result<PathBuf> {
    validate_manifest(manifest)?;
    let path = campaign_run_path(store, required(&manifest.run_id, "campaign_run.run_id")?)?;
    store.write_record_atomic("campaign", &path, &manifest.encode_to_vec())?;
    Ok(path)
}

fn canonical_version_matches(actual: &str, expected: &str) -> bool {
    normalize_tag_version(actual) == normalize_tag_version(expected)
}

fn add_missing_dataset_if(
    kind: pb::RequiredOutputKind,
    index_key: &str,
    crate_version: &str,
    present: bool,
    missing: &mut Vec<pb::MissingOutput>,
) {
    if !present {
        missing.push(pb::MissingOutput {
            kind: kind as i32,
            action_id: None,
            reason: format!(
                "required dataset {index_key} does not contain crate version {crate_version}"
            ),
        });
    }
}

fn stdlib_enumeration_is_complete(status: &StdlibEnumerationStatusView) -> bool {
    status.state == StdlibEnumerationState::Ok
        && status.scanned_files > 0
        && status.failed_files == 0
        && status.concrete_functions > 0
        && status.suggested_actions > 0
}

fn versions_summary_contains_crate(
    report: &crate::view::VersionCardsReport,
    version: &str,
) -> bool {
    report.cards.iter().any(|card| {
        canonical_version_matches(&card.crate_version, version)
            && stdlib_enumeration_is_complete(&card.stdlib_enumeration)
    })
}

fn stdlib_dataset_contains_crate(dataset: &StdlibG8rVsYosysDataset, version: &str) -> bool {
    dataset
        .samples
        .iter()
        .any(|sample| canonical_version_matches(&sample.crate_version, version))
}

fn action_descends_from_root<F>(
    start_action_id: &str,
    root_action_id: &str,
    mut load_action: F,
) -> Result<bool>
where
    F: FnMut(&str) -> Result<Option<ActionSpec>>,
{
    let mut visited = BTreeSet::new();
    let mut queue = VecDeque::from([start_action_id.to_string()]);
    while let Some(action_id) = queue.pop_front() {
        if action_id == root_action_id {
            return Ok(true);
        }
        if !visited.insert(action_id.clone()) {
            continue;
        }
        let Some(action) = load_action(&action_id)? else {
            continue;
        };
        for dependency in action_dependency_action_ids(&action) {
            queue.push_back(dependency.to_string());
        }
    }
    Ok(false)
}

pub(crate) fn stored_action_descends_from_root(
    store: &ArtifactStore,
    start_action_id: &str,
    root_action_id: &str,
) -> Result<bool> {
    action_descends_from_root(start_action_id, root_action_id, |action_id| {
        if !store.action_exists(action_id) {
            return Ok(None);
        }
        Ok(Some(store.load_provenance(action_id)?.action))
    })
}

fn stdlib_dataset_has_root_lineage(
    store: &ArtifactStore,
    dataset: &StdlibG8rVsYosysDataset,
    version: &str,
    root_action_id: &str,
) -> Result<bool> {
    let matching = dataset
        .samples
        .iter()
        .filter(|sample| canonical_version_matches(&sample.crate_version, version))
        .collect::<Vec<_>>();
    if matching.is_empty() {
        return Ok(false);
    }
    for sample in matching {
        for action_id in [
            &sample.ir_action_id,
            &sample.g8r_stats_action_id,
            &sample.yosys_abc_stats_action_id,
        ] {
            if !stored_action_descends_from_root(store, action_id, root_action_id)? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

pub(crate) fn stdlib_root_action_id(manifest: &pb::CampaignRunManifest) -> Result<Option<String>> {
    let mut roots = Vec::new();
    for root in &manifest.root_actions {
        let action =
            action_spec_from_proto(required(&root.action, "campaign_run.root_actions.action")?)?;
        if matches!(
            action,
            ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { .. }
        ) {
            roots.push(action_id_from_root(root)?);
        }
    }
    if roots.len() > 1 {
        bail!("campaign run contains more than one stdlib root action");
    }
    Ok(roots.pop())
}

fn normalize_completion_error(error: &str, fallback: &str) -> String {
    let escaped = error.replace('\0', "\\0");
    let trimmed = escaped.trim();
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed.to_string()
    }
}

fn failure_error(store: &ArtifactStore, action_id: &str) -> Result<String> {
    Ok(store
        .load_failed_action_record(action_id)?
        .map(|failed| {
            if is_timeout_error(&failed.error) {
                "timeout"
            } else {
                "failed"
            }
        })
        .unwrap_or("failed")
        .to_string())
}

fn evaluate_completion(
    store: &ArtifactStore,
    repo_root: &Path,
    manifest: &pb::CampaignRunManifest,
) -> Result<pb::CompletionReport> {
    let campaign = required(&manifest.campaign, "campaign_run.campaign")?;
    let failure_policy = required(&campaign.failure_policy, "campaign.failure_policy")?;
    let work_policy = required(&campaign.work_policy, "campaign.work_policy")?;
    let root_ids: BTreeSet<String> = manifest
        .root_actions
        .iter()
        .map(action_id_from_root)
        .collect::<Result<_>>()?;
    let mut discovered = root_ids.clone();
    let mut queue: VecDeque<String> = root_ids.iter().cloned().collect();
    while let Some(action_id) = queue.pop_front() {
        if !store.action_exists(&action_id) {
            continue;
        }
        let provenance = store.load_provenance(&action_id)?;
        for suggestion in provenance.suggested_next_actions {
            if discovered.insert(suggestion.action_id.clone()) {
                queue.push_back(suggestion.action_id);
            }
        }
    }

    let mut completed_root_count = 0_u64;
    let mut pending_count = 0_u64;
    let mut running_count = 0_u64;
    let mut failed_count = 0_u64;
    let mut canceled_count = 0_u64;
    let mut root_terminal_failure = false;
    let mut missing_outputs = Vec::new();
    let mut failed_samples = Vec::new();
    let mut intentionally_skipped_samples = Vec::new();

    for action_id in &discovered {
        let is_root = root_ids.contains(action_id);
        if store.action_exists(action_id) {
            if is_root {
                completed_root_count += 1;
            }
            continue;
        }
        match queue_state_for_action(store, action_id) {
            QueueState::Pending | QueueState::None => pending_count += 1,
            QueueState::Running { .. } => running_count += 1,
            QueueState::Done => {
                failed_count += 1;
                if !is_root {
                    failed_samples.push(pb::FailedSample {
                        action_id: Some(action_id_to_proto(action_id, "failed_sample.action_id")?),
                        error: "queue says done but the output artifact is absent".to_string(),
                    });
                }
                root_terminal_failure |= is_root;
            }
            QueueState::Failed => {
                failed_count += 1;
                let error = failure_error(store, action_id)?;
                if !is_root {
                    failed_samples.push(pb::FailedSample {
                        action_id: Some(action_id_to_proto(action_id, "failed_sample.action_id")?),
                        error,
                    });
                }
                root_terminal_failure |= is_root;
            }
            QueueState::Canceled => {
                let canceled = load_queue_canceled_record(store, action_id)?;
                let error = canceled
                    .as_ref()
                    .map(|record| {
                        normalize_completion_error(
                            &record.reason,
                            "canceled action reason is unavailable",
                        )
                    })
                    .unwrap_or_else(|| "canceled action record is unavailable".to_string());
                let current_rule = match canceled.as_ref() {
                    Some(record) => current_work_policy_exclusion(record, work_policy)?,
                    None => None,
                };
                if let (Some(_), Some(rule)) = (canceled.as_ref(), current_rule) {
                    canceled_count += 1;
                    intentionally_skipped_samples.push(pb::IntentionallySkippedSample {
                        action_id: Some(action_id_to_proto(
                            action_id,
                            "intentionally_skipped_sample.action_id",
                        )?),
                        rule_id: rule.rule_id.clone(),
                        reason: rule.reason.clone(),
                    });
                } else if canceled.as_ref().is_some_and(|record| {
                    record.cancellation_kind == QueueCancellationKind::WorkPolicyExcluded
                }) {
                    pending_count += 1;
                } else {
                    canceled_count += 1;
                    if !is_root {
                        failed_samples.push(pb::FailedSample {
                            action_id: Some(action_id_to_proto(
                                action_id,
                                "failed_sample.action_id",
                            )?),
                            error,
                        });
                    }
                    root_terminal_failure |= is_root;
                }
            }
        }
        if is_root {
            missing_outputs.push(pb::MissingOutput {
                kind: pb::RequiredOutputKind::RootArtifacts as i32,
                action_id: Some(action_id_to_proto(action_id, "missing_output.action_id")?),
                reason: format!(
                    "root action artifact is absent; queue state is {}",
                    queue_state_for_action(store, action_id).key()
                ),
            });
        }
    }

    let crate_version = &required(&manifest.crate_version, "campaign_run.crate_version")?.value;
    let stdlib_root_action_id = stdlib_root_action_id(manifest)?;
    let stdlib_enumeration_status = match stdlib_root_action_id.as_deref() {
        Some(action_id) if store.action_exists(action_id) => Some(
            stdlib_enumeration_status_from_provenance(&store.load_provenance(action_id)?),
        ),
        _ => None,
    };
    for raw in &campaign.required_outputs {
        match pb::RequiredOutputKind::try_from(*raw)
            .context("campaign.required_outputs contains unknown value")?
        {
            pb::RequiredOutputKind::RootArtifacts => {}
            pb::RequiredOutputKind::VersionsSummaryDataset => {
                let present = load_versions_cards_index(store, repo_root)?
                    .as_ref()
                    .is_some_and(|report| versions_summary_contains_crate(report, crate_version));
                add_missing_dataset_if(
                    pb::RequiredOutputKind::VersionsSummaryDataset,
                    WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
                    crate_version,
                    present,
                    &mut missing_outputs,
                );
            }
            pb::RequiredOutputKind::StdlibG8rVsYosysDataset => {
                let dataset = load_stdlib_g8r_vs_yosys_dataset_index(store, false)?;
                let present = match (dataset.as_ref(), stdlib_root_action_id.as_deref()) {
                    (Some(dataset), Some(root_action_id)) => {
                        stdlib_dataset_contains_crate(dataset, crate_version)
                            && stdlib_dataset_has_root_lineage(
                                store,
                                dataset,
                                crate_version,
                                root_action_id,
                            )?
                    }
                    _ => false,
                };
                if !present {
                    missing_outputs.push(pb::MissingOutput {
                        kind: pb::RequiredOutputKind::StdlibG8rVsYosysDataset as i32,
                        action_id: stdlib_root_action_id
                            .as_deref()
                            .map(|id| action_id_to_proto(id, "missing_output.action_id"))
                            .transpose()?,
                        reason: format!(
                            "required dataset {} lacks non-empty crate samples with declared stdlib-root lineage for {}",
                            WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME,
                            crate_version
                        ),
                    });
                }
            }
            pb::RequiredOutputKind::StdlibEnumeration => {
                if !stdlib_enumeration_status
                    .as_ref()
                    .is_some_and(stdlib_enumeration_is_complete)
                {
                    missing_outputs.push(pb::MissingOutput {
                        kind: pb::RequiredOutputKind::StdlibEnumeration as i32,
                        action_id: stdlib_root_action_id
                            .as_deref()
                            .map(|id| action_id_to_proto(id, "missing_output.action_id"))
                            .transpose()?,
                        reason: "stdlib enumeration is absent, partial, failed, or empty"
                            .to_string(),
                    });
                }
            }
            pb::RequiredOutputKind::Unspecified => unreachable!("campaign validated"),
        }
    }
    missing_outputs.sort_by(|a, b| {
        a.kind
            .cmp(&b.kind)
            .then(
                a.action_id
                    .as_ref()
                    .map(|id| &id.value)
                    .cmp(&b.action_id.as_ref().map(|id| &id.value)),
            )
            .then(a.reason.cmp(&b.reason))
    });
    failed_samples.sort_by(|a, b| {
        a.action_id
            .as_ref()
            .map(|id| &id.value)
            .cmp(&b.action_id.as_ref().map(|id| &id.value))
    });
    intentionally_skipped_samples.sort_by(|a, b| {
        a.action_id
            .as_ref()
            .map(|id| &id.value)
            .cmp(&b.action_id.as_ref().map(|id| &id.value))
            .then(a.rule_id.cmp(&b.rule_id))
    });

    let active = pending_count > 0 || running_count > 0;
    let sample_failure = !failed_samples.is_empty();
    let status = if root_terminal_failure && failure_policy.root_action_failure_is_terminal {
        pb::CampaignRunStatus::Failed
    } else if active || completed_root_count < root_ids.len() as u64 {
        pb::CampaignRunStatus::Building
    } else if sample_failure && !failure_policy.allow_sample_action_failures {
        pb::CampaignRunStatus::Failed
    } else if root_terminal_failure || sample_failure || !missing_outputs.is_empty() {
        pb::CampaignRunStatus::Degraded
    } else {
        pb::CampaignRunStatus::Complete
    };
    Ok(pb::CompletionReport {
        status: status as i32,
        root_action_count: root_ids.len() as u64,
        completed_root_count,
        pending_count,
        running_count,
        failed_count,
        canceled_count,
        missing_outputs,
        failed_samples,
        intentionally_skipped_samples,
    })
}

fn evaluated_manifest(
    store: &ArtifactStore,
    repo_root: &Path,
    crate_version: &str,
) -> Result<pb::CampaignRunManifest> {
    let planned = new_manifest(repo_root, crate_version)?;
    let existing = load_existing_manifest(store, &planned)?;
    evaluate_stored_manifest(store, repo_root, existing.unwrap_or(planned))
}

fn evaluate_stored_manifest(
    store: &ArtifactStore,
    repo_root: &Path,
    mut manifest: pb::CampaignRunManifest,
) -> Result<pb::CampaignRunManifest> {
    validate_manifest(&manifest)?;
    let previous_status = manifest.status;
    let previous_completion = manifest.completion.clone();
    let completion = evaluate_completion(store, repo_root, &manifest)?;
    manifest.status = completion.status;
    manifest.completion = Some(completion);
    if manifest.status != previous_status || manifest.completion != previous_completion {
        manifest.updated_at = Some(timestamp_to_proto(&Utc::now()));
    }
    validate_manifest(&manifest)?;
    Ok(manifest)
}

fn status_label(raw: i32) -> Result<String> {
    let status = pb::CampaignRunStatus::try_from(raw).context("campaign status is unknown")?;
    Ok(status
        .as_str_name()
        .trim_start_matches("CAMPAIGN_RUN_STATUS_")
        .to_ascii_lowercase())
}

fn summary(
    manifest: &pb::CampaignRunManifest,
    manifest_path: &Path,
    persisted: bool,
) -> Result<CampaignRunSummary> {
    let completion = required(&manifest.completion, "campaign_run.completion")?;
    let missing_outputs = completion
        .missing_outputs
        .iter()
        .map(|missing| {
            let kind = pb::RequiredOutputKind::try_from(missing.kind)
                .map(|kind| kind.as_str_name().to_string())
                .unwrap_or_else(|_| format!("UNKNOWN_{}", missing.kind));
            let action = missing
                .action_id
                .as_ref()
                .and_then(|id| action_id_to_hex(id, "missing_output.action_id").ok())
                .map(|id| format!(" action_id={id}"))
                .unwrap_or_default();
            format!("{kind}{action}: {}", missing.reason)
        })
        .collect();
    let failed_samples = completion
        .failed_samples
        .iter()
        .map(|failed| {
            let id = failed
                .action_id
                .as_ref()
                .and_then(|id| action_id_to_hex(id, "failed_sample.action_id").ok())
                .unwrap_or_else(|| "unknown".to_string());
            format!("{id}: {}", failed.error)
        })
        .collect();
    let intentionally_skipped_samples = completion
        .intentionally_skipped_samples
        .iter()
        .map(|skipped| {
            let id = skipped
                .action_id
                .as_ref()
                .and_then(|id| action_id_to_hex(id, "intentionally_skipped_sample.action_id").ok())
                .unwrap_or_else(|| "unknown".to_string());
            format!("{id} ({}): {}", skipped.rule_id, skipped.reason)
        })
        .collect();
    Ok(CampaignRunSummary {
        campaign_id: digest_hex(
            required(&manifest.campaign_id, "campaign_run.campaign_id")?,
            "campaign_run.campaign_id",
        )?,
        run_id: digest_hex(
            required(&manifest.run_id, "campaign_run.run_id")?,
            "campaign_run.run_id",
        )?,
        campaign_name: required(&manifest.campaign, "campaign_run.campaign")?
            .campaign_name
            .clone(),
        crate_version: required(&manifest.crate_version, "campaign_run.crate_version")?
            .value
            .clone(),
        dso_version: required(&manifest.dso_version, "campaign_run.dso_version")?
            .value
            .clone(),
        status: status_label(manifest.status)?,
        root_action_count: completion.root_action_count,
        completed_root_count: completion.completed_root_count,
        pending_count: completion.pending_count,
        running_count: completion.running_count,
        failed_count: completion.failed_count,
        canceled_count: completion.canceled_count,
        missing_outputs,
        failed_samples,
        intentionally_skipped_samples,
        manifest_path: manifest_path.display().to_string(),
        persisted,
    })
}

pub(crate) fn plan_campaign_run(
    store: &ArtifactStore,
    repo_root: &Path,
    crate_version: &str,
) -> Result<CampaignRunSummary> {
    let manifest = evaluated_manifest(store, repo_root, crate_version)?;
    let path = campaign_run_path(store, required(&manifest.run_id, "campaign_run.run_id")?)?;
    summary(&manifest, &path, false)
}

pub(crate) fn summarize_campaign_run(
    store: &ArtifactStore,
    manifest: &pb::CampaignRunManifest,
    persisted: bool,
) -> Result<CampaignRunSummary> {
    let path = campaign_run_path(store, required(&manifest.run_id, "campaign_run.run_id")?)?;
    summary(manifest, &path, persisted)
}

pub(crate) fn persist_campaign_run_plan(
    store: &ArtifactStore,
    repo_root: &Path,
    crate_version: &str,
) -> Result<pb::CampaignRunManifest> {
    let manifest = evaluated_manifest(store, repo_root, crate_version)?;
    write_manifest(store, &manifest)?;
    Ok(manifest)
}

pub(crate) fn reconcile_stored_campaign_run(
    store: &ArtifactStore,
    repo_root: &Path,
    manifest: &pb::CampaignRunManifest,
    priority: i32,
) -> Result<CampaignRunSummary> {
    validate_manifest(manifest)?;
    let roots = manifest
        .root_actions
        .iter()
        .map(|root| {
            action_spec_from_proto(required(&root.action, "campaign_run.root_actions.action")?)
        })
        .collect::<Result<Vec<_>>>()?;
    enqueue_processing_for_root_actions(store, repo_root, roots, priority)?;
    let evaluated = evaluate_stored_manifest(store, repo_root, manifest.clone())?;
    let path = write_manifest(store, &evaluated)?;
    summary(&evaluated, &path, true)
}

pub(crate) fn finalize_stored_campaign_run(
    store: &ArtifactStore,
    repo_root: &Path,
    manifest: &pb::CampaignRunManifest,
) -> Result<CampaignRunSummary> {
    let evaluated = evaluate_stored_manifest(store, repo_root, manifest.clone())?;
    let path = write_manifest(store, &evaluated)?;
    summary(&evaluated, &path, true)
}

pub(crate) fn reconcile_campaign_run(
    store: &ArtifactStore,
    repo_root: &Path,
    crate_version: &str,
    priority: i32,
) -> Result<CampaignRunSummary> {
    let planned = new_manifest(repo_root, crate_version)?;
    let manifest = load_existing_manifest(store, &planned)?.unwrap_or(planned);
    reconcile_stored_campaign_run(store, repo_root, &manifest, priority)
}

pub(crate) fn finalize_campaign_run(
    store: &ArtifactStore,
    repo_root: &Path,
    crate_version: &str,
) -> Result<CampaignRunSummary> {
    let manifest = evaluated_manifest(store, repo_root, crate_version)?;
    let path = write_manifest(store, &manifest)?;
    summary(&manifest, &path, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::list_queue_files;
    use crate::versioning::load_version_compat_map;
    use crate::view::{
        StdlibEnumerationStatusView, StdlibG8rVsYosysDataset, StdlibG8rVsYosysSample,
        VersionCardView, VersionCardsReport,
    };
    use std::collections::BTreeMap;

    fn temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "xlsynth-bvc-campaign-{label}-{}-{nanos}",
            std::process::id()
        ))
    }

    fn known_crate_version(repo_root: &Path) -> String {
        load_version_compat_map(repo_root)
            .expect("load version map")
            .keys()
            .next()
            .expect("nonempty version map")
            .clone()
    }

    fn make_mutable_resource_root(label: &str) -> PathBuf {
        let source_root = std::env::current_dir().expect("current dir");
        let root = temp_path(label);
        for relpath in [
            crate::VERSION_COMPAT_PATH,
            crate::DEFAULT_DOCKERFILE,
            crate::VENDORED_DOWNLOAD_RELEASE_SCRIPT,
        ] {
            let target = root.join(relpath);
            fs::create_dir_all(target.parent().expect("resource parent"))
                .expect("create resource parent");
            fs::copy(source_root.join(relpath), &target).expect("copy resource input");
        }
        root
    }

    #[cfg(unix)]
    fn make_read_only_resource_root() -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let source_root = std::env::current_dir().expect("current dir");
        let root = temp_path("read-only-resources");
        let compat_path = root.join(crate::VERSION_COMPAT_PATH);
        fs::create_dir_all(compat_path.parent().expect("compat parent"))
            .expect("create resource directories");
        fs::copy(source_root.join(crate::VERSION_COMPAT_PATH), &compat_path)
            .expect("copy compatibility map");
        let dockerfile_path = root.join(crate::DEFAULT_DOCKERFILE);
        fs::create_dir_all(dockerfile_path.parent().expect("Dockerfile parent"))
            .expect("create Dockerfile directory");
        fs::copy(
            source_root.join(crate::DEFAULT_DOCKERFILE),
            &dockerfile_path,
        )
        .expect("copy runtime Dockerfile");
        let cache_script_path = root.join(crate::VENDORED_DOWNLOAD_RELEASE_SCRIPT);
        fs::create_dir_all(cache_script_path.parent().expect("cache script parent"))
            .expect("create cache script directory");
        fs::copy(
            source_root.join(crate::VENDORED_DOWNLOAD_RELEASE_SCRIPT),
            &cache_script_path,
        )
        .expect("copy cache setup script");
        fs::set_permissions(&dockerfile_path, fs::Permissions::from_mode(0o444))
            .expect("make runtime Dockerfile read-only");
        fs::set_permissions(
            dockerfile_path.parent().expect("Dockerfile parent"),
            fs::Permissions::from_mode(0o555),
        )
        .expect("make Dockerfile directory read-only");
        fs::set_permissions(&compat_path, fs::Permissions::from_mode(0o444))
            .expect("make compatibility map read-only");
        fs::set_permissions(&cache_script_path, fs::Permissions::from_mode(0o444))
            .expect("make cache setup script read-only");
        let mut current = cache_script_path.parent();
        while let Some(dir) = current {
            if !dir.starts_with(&root) {
                break;
            }
            fs::set_permissions(dir, fs::Permissions::from_mode(0o555))
                .expect("make cache script directory read-only");
            if dir == root {
                break;
            }
            current = dir.parent();
        }
        let mut current = compat_path.parent();
        while let Some(dir) = current {
            if !dir.starts_with(&root) {
                break;
            }
            fs::set_permissions(dir, fs::Permissions::from_mode(0o555))
                .expect("make resource directory read-only");
            if dir == root {
                break;
            }
            current = dir.parent();
        }
        root
    }

    #[cfg(unix)]
    fn remove_read_only_resource_root(root: &Path) {
        use std::os::unix::fs::PermissionsExt;

        let cache_script_path = root.join(crate::VENDORED_DOWNLOAD_RELEASE_SCRIPT);
        let mut current = cache_script_path.parent();
        while let Some(dir) = current {
            if !dir.starts_with(root) {
                break;
            }
            fs::set_permissions(dir, fs::Permissions::from_mode(0o755))
                .expect("restore cache script directory permissions");
            if dir == root {
                break;
            }
            current = dir.parent();
        }
        fs::set_permissions(&cache_script_path, fs::Permissions::from_mode(0o644))
            .expect("restore cache setup script permissions");
        let compat_path = root.join(crate::VERSION_COMPAT_PATH);
        let mut current = compat_path.parent();
        while let Some(dir) = current {
            if !dir.starts_with(root) {
                break;
            }
            fs::set_permissions(dir, fs::Permissions::from_mode(0o755))
                .expect("restore resource directory permissions");
            if dir == root {
                break;
            }
            current = dir.parent();
        }
        fs::set_permissions(&compat_path, fs::Permissions::from_mode(0o644))
            .expect("restore compatibility map permissions");
        let dockerfile_path = root.join(crate::DEFAULT_DOCKERFILE);
        fs::set_permissions(
            dockerfile_path.parent().expect("Dockerfile parent"),
            fs::Permissions::from_mode(0o755),
        )
        .expect("restore Dockerfile directory permissions");
        fs::set_permissions(&dockerfile_path, fs::Permissions::from_mode(0o644))
            .expect("restore Dockerfile permissions");
        fs::remove_dir_all(root).expect("remove resource root");
    }

    #[test]
    fn campaign_id_is_deterministic_and_semantic() {
        let campaign = load_default_campaign().expect("load campaign");
        let first = compute_campaign_id(&campaign).expect("campaign id");
        let second = compute_campaign_id(&campaign).expect("campaign id");
        assert_eq!(first, second);
        let mut changed = campaign;
        changed.analysis_algorithm_version += 1;
        assert_ne!(
            first,
            compute_campaign_id(&changed).expect("changed campaign id")
        );
    }

    #[test]
    fn work_policy_cancellation_must_match_current_rule_and_action() {
        let campaign = load_default_campaign().expect("default campaign");
        let policy = campaign.work_policy.expect("work policy");
        let rule = policy.rules.first().expect("checked-in exclusion rule");
        let action = ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id: "a".repeat(64),
            verilog_top_module_name: Some(rule.top_name.clone()),
            frontend: crate::model::YosysVerilogFrontend::Builtin,
            yosys_script_ref: crate::model::ScriptRef {
                path: "flows/yosys_to_aig.ys".to_string(),
                sha256: "b".repeat(64),
            },
            runtime: crate::runtime::test_yosys_runtime(),
        };
        let action_id = compute_model_action_id_v2(&action)
            .expect("action id")
            .to_hex();
        let rule_fingerprint = work_policy_rule_fingerprint(rule).expect("rule fingerprint");
        let now = Utc::now();
        let mut canceled = QueueCanceled {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: action_id.clone(),
            enqueued_utc: now,
            canceled_utc: now,
            canceled_by: "campaign-work-policy".to_string(),
            canceled_due_to_action_id: action_id.clone(),
            root_failed_action_id: action_id,
            action,
            reason: rule.reason.clone(),
            cancellation_kind: QueueCancellationKind::WorkPolicyExcluded,
            work_policy_rule_id: Some(rule.rule_id.clone()),
            work_policy_rule_fingerprint: Some(rule_fingerprint),
        };

        assert_eq!(
            current_work_policy_exclusion(&canceled, &policy)
                .expect("match current policy")
                .map(|matched| matched.rule_id.as_str()),
            Some(rule.rule_id.as_str())
        );
        assert!(
            current_work_policy_exclusion(&canceled, &pb::CampaignWorkPolicy::default())
                .expect("removed policy")
                .is_none()
        );
        let mut changed_policy = policy.clone();
        changed_policy.rules[0].reason = "updated reviewed reason".to_string();
        assert!(
            current_work_policy_exclusion(&canceled, &changed_policy)
                .expect("changed policy")
                .is_none()
        );
        canceled.reason = changed_policy.rules[0].reason.clone();
        canceled.work_policy_rule_fingerprint = Some(
            work_policy_rule_fingerprint(&changed_policy.rules[0])
                .expect("changed rule fingerprint"),
        );
        assert!(
            current_work_policy_exclusion(&canceled, &changed_policy)
                .expect("refreshed policy evidence")
                .is_some()
        );
        canceled.work_policy_rule_id = Some("obsolete-rule".to_string());
        assert!(
            current_work_policy_exclusion(&canceled, &policy)
                .expect("obsolete rule")
                .is_none()
        );
    }

    #[test]
    fn pending_versions_compare_current_run_identity() {
        let repo_root = std::env::current_dir().expect("current dir");
        let root = temp_path("pending-current-campaign");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let version = known_crate_version(&repo_root);

        let current = new_manifest(&repo_root, &version).expect("current manifest");
        let mut older_campaign = current.clone();
        let campaign = older_campaign.campaign.as_mut().expect("campaign");
        campaign.semantic_version += 1;
        let campaign_id = compute_campaign_id(campaign).expect("older campaign id");
        older_campaign.campaign_id = Some(campaign_id.clone());
        older_campaign.run_id = Some(
            compute_run_id(&pb::CampaignRunIdentity {
                identity_version: CAMPAIGN_RUN_IDENTITY_VERSION,
                campaign_id: Some(campaign_id),
                crate_version: older_campaign.crate_version.clone(),
                dso_version: older_campaign.dso_version.clone(),
                driver_runtime: older_campaign.driver_runtime.clone(),
            })
            .expect("older run id"),
        );
        older_campaign.status = pb::CampaignRunStatus::Complete as i32;
        older_campaign.completion = Some(pb::CompletionReport {
            status: pb::CampaignRunStatus::Complete as i32,
            root_action_count: older_campaign.root_actions.len() as u64,
            completed_root_count: older_campaign.root_actions.len() as u64,
            ..Default::default()
        });
        write_manifest(&store, &older_campaign).expect("write older finalized campaign");

        assert!(
            pending_campaign_versions(&store, &repo_root)
                .expect("pending under current campaign")
                .contains(&version)
        );

        let mut current_finalized = current;
        current_finalized.status = pb::CampaignRunStatus::Complete as i32;
        current_finalized.completion = Some(pb::CompletionReport {
            status: pb::CampaignRunStatus::Complete as i32,
            root_action_count: current_finalized.root_actions.len() as u64,
            completed_root_count: current_finalized.root_actions.len() as u64,
            ..Default::default()
        });
        write_manifest(&store, &current_finalized).expect("write current finalized campaign");
        assert!(
            !pending_campaign_versions(&store, &repo_root)
                .expect("not pending after current finalization")
                .contains(&version)
        );

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn pending_versions_detect_release_cache_input_change() {
        let repo_root = make_mutable_resource_root("pending-cache-input-change");
        let store_root = temp_path("pending-cache-input-store");
        let store = ArtifactStore::new(store_root.clone());
        store.ensure_layout().expect("layout");
        let version = known_crate_version(&repo_root);

        let mut finalized = new_manifest(&repo_root, &version).expect("current manifest");
        finalized.status = pb::CampaignRunStatus::Complete as i32;
        finalized.completion = Some(pb::CompletionReport {
            status: pb::CampaignRunStatus::Complete as i32,
            root_action_count: finalized.root_actions.len() as u64,
            completed_root_count: finalized.root_actions.len() as u64,
            ..Default::default()
        });
        write_manifest(&store, &finalized).expect("write finalized manifest");
        assert!(
            !pending_campaign_versions(&store, &repo_root)
                .expect("current generation is finalized")
                .contains(&version)
        );

        let setup_script = repo_root.join(crate::VENDORED_DOWNLOAD_RELEASE_SCRIPT);
        let mut bytes = fs::read(&setup_script).expect("read setup script");
        bytes.extend_from_slice(b"\n# changed cache input\n");
        fs::write(&setup_script, bytes).expect("change cache setup script");
        assert!(
            pending_campaign_versions(&store, &repo_root)
                .expect("changed cache input is pending")
                .contains(&version)
        );

        drop(store);
        fs::remove_dir_all(store_root).expect("cleanup store");
        fs::remove_dir_all(repo_root).expect("cleanup resources");
    }

    #[test]
    fn plan_is_read_only_and_run_id_is_stable() {
        let repo_root = std::env::current_dir().expect("current dir");
        let root = temp_path("plan");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let version = known_crate_version(&repo_root);
        let first = plan_campaign_run(&store, &repo_root, &version).expect("first plan");
        let second = plan_campaign_run(&store, &repo_root, &version).expect("second plan");
        assert_eq!(first.run_id, second.run_id);
        assert!(!Path::new(&first.manifest_path).exists());
        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[cfg(unix)]
    #[test]
    fn planning_uses_a_read_only_resource_root_and_unknown_versions_fail_closed() {
        let repo_root = make_read_only_resource_root();
        let compat_path = repo_root.join(crate::VERSION_COMPAT_PATH);
        let original_compat = fs::read(&compat_path).expect("read compatibility map");
        let store_root = temp_path("read-only-store");
        let store = ArtifactStore::new(store_root.clone());
        store.ensure_layout().expect("layout");

        let version = known_crate_version(&repo_root);
        plan_campaign_run(&store, &repo_root, &version).expect("plan from read-only resources");
        let error = plan_campaign_run(&store, &repo_root, "999.0.0")
            .expect_err("unknown deployed version should fail");
        assert!(error.to_string().contains("update it out of band"));
        assert_eq!(
            fs::read(&compat_path).expect("reread compatibility map"),
            original_compat
        );

        drop(store);
        fs::remove_dir_all(store_root).expect("remove store");
        remove_read_only_resource_root(&repo_root);
    }

    #[test]
    fn reconcile_is_idempotent_and_finalize_explains_incomplete_run() {
        let repo_root = std::env::current_dir().expect("current dir");
        let root = temp_path("reconcile");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let version = known_crate_version(&repo_root);
        let first = reconcile_campaign_run(&store, &repo_root, &version, 0).expect("reconcile");
        let first_manifest = fs::read(&first.manifest_path).expect("first manifest bytes");
        let pending_once = list_queue_files(&store.queue_pending_dir())
            .expect("pending files")
            .len();
        let second = reconcile_campaign_run(&store, &repo_root, &version, 0).expect("reconcile");
        let second_manifest = fs::read(&second.manifest_path).expect("second manifest bytes");
        let pending_twice = list_queue_files(&store.queue_pending_dir())
            .expect("pending files")
            .len();
        assert_eq!(first.run_id, second.run_id);
        assert_eq!(first_manifest, second_manifest);
        assert_eq!(pending_once, pending_twice);
        assert_eq!(pending_once as u64, first.root_action_count);
        let finalized = finalize_campaign_run(&store, &repo_root, &version).expect("finalize");
        assert_eq!(finalized.status, "building");
        assert_eq!(finalized.pending_count, finalized.root_action_count);
        assert!(
            finalized
                .missing_outputs
                .iter()
                .any(|reason| reason.contains("root action artifact is absent"))
        );
        assert!(Path::new(&finalized.manifest_path).exists());
        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn malformed_manifest_is_rejected() {
        let manifest = pb::CampaignRunManifest::default();
        let error = validate_manifest(&manifest).expect_err("invalid manifest");
        assert!(error.to_string().contains("record version"));
    }

    #[test]
    fn manifest_requires_the_exact_canonical_runtime_root_set() {
        let repo_root = std::env::current_dir().expect("current dir");
        let version = known_crate_version(&repo_root);
        let mut manifest = new_manifest(&repo_root, &version).expect("manifest");
        assert!(manifest.root_actions.len() > 1, "test needs multiple roots");
        manifest.root_actions.remove(0);
        manifest
            .completion
            .as_mut()
            .expect("completion")
            .root_action_count = manifest.root_actions.len() as u64;

        let error = validate_manifest(&manifest).expect_err("partial root set must fail");
        assert!(
            format!("{error:#}").contains("canonical runtime root set"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn completion_dataset_checks_use_exact_crate_fields() {
        let mut versions = VersionCardsReport {
            cards: vec![VersionCardView {
                crate_version: "0.40.0".to_string(),
                crate_release_datetime: None,
                total_materialized: 1,
                failed_total: 0,
                dso_versions: vec!["0.39.0".to_string()],
                stdlib_enumeration: StdlibEnumerationStatusView {
                    state: crate::view::StdlibEnumerationState::Ok,
                    reason: crate::view::StdlibEnumerationReason::DiscoveryCounts,
                    scanned_files: 1,
                    failed_files: 0,
                    concrete_functions: 1,
                    suggested_actions: 1,
                },
                failed_by_kind: Vec::new(),
                failures: Vec::new(),
            }],
            unattributed_actions: Vec::new(),
            releases: Vec::new(),
            repository_head_observation: None,
        };
        assert!(versions_summary_contains_crate(&versions, "v0.40.0"));
        assert!(!versions_summary_contains_crate(&versions, "0.41.0"));
        versions.cards[0].stdlib_enumeration.state = crate::view::StdlibEnumerationState::Partial;
        assert!(!versions_summary_contains_crate(&versions, "0.40.0"));

        let dataset = StdlibG8rVsYosysDataset {
            fraig: false,
            samples: vec![StdlibG8rVsYosysSample {
                fn_key: "foo".to_string(),
                crate_version: "0.40.0".to_string(),
                dso_version: "0.39.0".to_string(),
                stdlib_root_action_id: None,
                ir_action_id: "ir".to_string(),
                ir_top: None,
                structural_hash: None,
                ir_node_count: 1,
                g8r_nodes: 1.0,
                g8r_levels: 1.0,
                yosys_abc_nodes: 1.0,
                yosys_abc_levels: 1.0,
                g8r_product: 1.0,
                yosys_abc_product: 1.0,
                g8r_product_loss: 0.0,
                g8r_stats_action_id: "g8r".to_string(),
                yosys_abc_stats_action_id: "yosys".to_string(),
            }],
            min_ir_nodes: 1,
            max_ir_nodes: 1,
            g8r_only_count: 0,
            yosys_only_count: 0,
            available_crate_versions: vec!["0.40.0".to_string()],
        };
        assert!(stdlib_dataset_contains_crate(&dataset, "0.40.0"));
        assert!(!stdlib_dataset_contains_crate(&dataset, "0.41.0"));
        let mut advertised_but_empty = dataset.clone();
        advertised_but_empty.samples.clear();
        assert!(!stdlib_dataset_contains_crate(
            &advertised_but_empty,
            "0.40.0"
        ));
    }

    #[test]
    fn stdlib_completeness_requires_nonempty_success_and_declared_root_lineage() {
        let complete = StdlibEnumerationStatusView {
            state: crate::view::StdlibEnumerationState::Ok,
            reason: crate::view::StdlibEnumerationReason::DiscoveryCounts,
            scanned_files: 2,
            failed_files: 0,
            concrete_functions: 2,
            suggested_actions: 2,
        };
        assert!(stdlib_enumeration_is_complete(&complete));
        let mut empty = complete.clone();
        empty.scanned_files = 0;
        assert!(!stdlib_enumeration_is_complete(&empty));
        let mut partial = complete;
        partial.state = crate::view::StdlibEnumerationState::Partial;
        assert!(!stdlib_enumeration_is_complete(&partial));

        let mut actions = BTreeMap::new();
        actions.insert(
            "leaf".to_string(),
            ActionSpec::AigStatDiff {
                opt_ir_action_id: "middle".to_string(),
                g8r_aig_stats_action_id: "unrelated".to_string(),
                yosys_abc_aig_stats_action_id: "unrelated".to_string(),
            },
        );
        actions.insert(
            "middle".to_string(),
            ActionSpec::AigStatDiff {
                opt_ir_action_id: "root".to_string(),
                g8r_aig_stats_action_id: "unrelated".to_string(),
                yosys_abc_aig_stats_action_id: "unrelated".to_string(),
            },
        );
        assert!(
            action_descends_from_root("leaf", "root", |id| { Ok(actions.get(id).cloned()) })
                .expect("lineage")
        );
        assert!(
            !action_descends_from_root("leaf", "other-root", |id| { Ok(actions.get(id).cloned()) })
                .expect("unrelated lineage")
        );
    }

    #[test]
    fn completion_errors_are_trimmed_and_nul_escaped() {
        assert_eq!(
            normalize_completion_error("  timed out\n", "fallback"),
            "timed out"
        );
        assert_eq!(
            normalize_completion_error("\0oops\0", "fallback"),
            "\\0oops\\0"
        );
        assert_eq!(normalize_completion_error(" \n\t", "fallback"), "fallback");
    }

    #[test]
    fn campaign_failed_samples_use_fixed_public_classes() {
        let root = temp_path("private-failure");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let action = ActionSpec::ImportIrPackageFile {
            source_sha256: "ab".repeat(32),
            top_fn_name: Some("main".to_string()),
        };
        let action_id = compute_model_action_id_v2(&action)
            .expect("action id")
            .to_hex();
        let now = Utc::now();
        store
            .write_failed_action_record(&crate::model::QueueFailed {
                schema_version: crate::ACTION_SCHEMA_VERSION,
                action_id: action_id.clone(),
                enqueued_utc: now,
                failed_utc: now,
                failed_by: "test-worker".to_string(),
                action,
                error: format!("private path {} token=do-not-publish", root.display()),
            })
            .expect("write failed action");
        assert_eq!(
            failure_error(&store, &action_id).expect("public class"),
            "failed"
        );

        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }
}
