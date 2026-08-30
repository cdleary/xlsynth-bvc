// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use prost::Message;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::campaign::{
    campaign_analysis_path, list_finalized_campaign_runs, load_campaign_run_by_id,
    load_finalized_campaign_run_for_version, stdlib_root_action_id,
    stored_action_descends_from_root,
};
use crate::proto::v1 as pb;
use crate::proto::{action_id_to_hex, action_id_to_proto, timestamp_to_proto};
use crate::query::load_stdlib_g8r_vs_yosys_dataset_index;
use crate::store::ArtifactStore;
use crate::versioning::{cmp_dotted_numeric_version, normalize_tag_version};
use crate::view::StdlibG8rVsYosysSample;

const ANALYSIS_RECORD_VERSION: u32 = 1;
const FINDING_IDENTITY_VERSION: u32 = 1;
const FINDING_ID_DOMAIN: &[u8] = b"xlsynth-bvc/finding/v1\0";
const METRIC_SCALE: f64 = 1_000_000.0;
const CHANGE_THRESHOLD_MICRO: i64 = 50_000;
const OUTLIER_THRESHOLD_MICRO: i64 = 50_000;
static WRITE_NONCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Serialize)]
pub(crate) struct AnalysisSummary {
    pub(crate) run_id: String,
    pub(crate) crate_version: String,
    pub(crate) baseline_run_id: Option<String>,
    pub(crate) baseline_crate_version: Option<String>,
    pub(crate) finding_count: usize,
    pub(crate) findings_by_kind: BTreeMap<String, usize>,
    pub(crate) analysis_path: String,
}

fn required<'a, T>(value: &'a Option<T>, field: &str) -> Result<&'a T> {
    value
        .as_ref()
        .with_context(|| format!("missing required protobuf field {field}"))
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

fn fixed_metric(value: f64, field: &str) -> Result<i64> {
    if !value.is_finite() {
        bail!("{field} must be finite");
    }
    let scaled = (value * METRIC_SCALE).round();
    if scaled < i64::MIN as f64 || scaled > i64::MAX as f64 {
        bail!("{field} is outside fixed-point range");
    }
    Ok(scaled as i64)
}

fn finding_id(identity: &pb::FindingIdentity) -> Result<pb::Sha256Digest> {
    if identity.identity_version != FINDING_IDENTITY_VERSION {
        bail!(
            "unsupported finding identity version {}",
            identity.identity_version
        );
    }
    validate_digest(
        required(&identity.campaign_id, "finding.campaign_id")?,
        "finding.campaign_id",
    )?;
    validate_digest(
        required(&identity.run_id, "finding.run_id")?,
        "finding.run_id",
    )?;
    if let Some(baseline) = &identity.baseline_run_id {
        validate_digest(baseline, "finding.baseline_run_id")?;
    }
    let kind = pb::FindingKind::try_from(identity.kind).context("finding kind is unknown")?;
    if kind == pb::FindingKind::Unspecified
        || identity.subject_key.trim().is_empty()
        || identity.analysis_algorithm_version == 0
    {
        bail!("finding identity is incomplete");
    }
    let metric = required(&identity.metric, "finding.metric")?;
    if metric.name.trim().is_empty() || metric.unit.trim().is_empty() {
        bail!("finding metric name and unit must be nonempty");
    }
    let mut hasher = Sha256::new();
    hasher.update(FINDING_ID_DOMAIN);
    hasher.update(identity.encode_to_vec());
    Ok(pb::Sha256Digest {
        value: hasher.finalize().to_vec(),
    })
}

fn artifact_digest(store: &ArtifactStore, action_id: &str) -> Result<pb::Sha256Digest> {
    let provenance = store
        .load_provenance(action_id)
        .with_context(|| format!("loading analysis evidence action {action_id}"))?;
    let output = provenance
        .output_files
        .first()
        .with_context(|| format!("analysis evidence action {action_id} has no output files"))?;
    let bytes = hex::decode(&output.sha256)
        .with_context(|| format!("decoding analysis evidence digest for action {action_id}"))?;
    if bytes.len() != 32 {
        bail!("analysis evidence digest for action {action_id} is not SHA-256");
    }
    Ok(pb::Sha256Digest { value: bytes })
}

fn evidence(store: &ArtifactStore, role: &str, action_id: &str) -> Result<pb::FindingEvidence> {
    Ok(pb::FindingEvidence {
        role: role.to_string(),
        action_id: Some(action_id_to_proto(action_id, "finding.evidence.action_id")?),
        artifact_sha256: Some(artifact_digest(store, action_id)?),
    })
}

fn structural_digest(value: Option<&str>) -> Option<pb::Sha256Digest> {
    let bytes = hex::decode(value?).ok()?;
    (bytes.len() == 32).then_some(pb::Sha256Digest { value: bytes })
}

fn subject_key(sample: &StdlibG8rVsYosysSample) -> String {
    format!(
        "{}#{}",
        sample.fn_key,
        sample.ir_top.as_deref().unwrap_or("<default>")
    )
}

fn make_finding(
    store: &ArtifactStore,
    campaign_id: &pb::Sha256Digest,
    run_id: &pb::Sha256Digest,
    baseline_run_id: Option<&pb::Sha256Digest>,
    algorithm_version: u32,
    kind: pb::FindingKind,
    current: &StdlibG8rVsYosysSample,
    baseline: Option<&StdlibG8rVsYosysSample>,
) -> Result<pb::AnalysisFinding> {
    let metric = pb::FindingMetric {
        name: "g8r_product_loss".to_string(),
        baseline_microunits: baseline
            .map(|sample| fixed_metric(sample.g8r_product_loss, "baseline product loss"))
            .transpose()?,
        current_microunits: Some(fixed_metric(
            current.g8r_product_loss,
            "current product loss",
        )?),
        unit: "ratio".to_string(),
    };
    let identity = pb::FindingIdentity {
        identity_version: FINDING_IDENTITY_VERSION,
        campaign_id: Some(campaign_id.clone()),
        run_id: Some(run_id.clone()),
        baseline_run_id: baseline_run_id.cloned(),
        kind: kind as i32,
        subject_key: subject_key(current),
        metric: Some(metric),
        analysis_algorithm_version: algorithm_version,
    };
    let id = finding_id(&identity)?;
    let mut evidence_refs = vec![
        evidence(store, "current_ir", &current.ir_action_id)?,
        evidence(store, "current_g8r_stats", &current.g8r_stats_action_id)?,
        evidence(
            store,
            "current_yosys_abc_stats",
            &current.yosys_abc_stats_action_id,
        )?,
    ];
    if let Some(baseline) = baseline {
        evidence_refs.push(evidence(
            store,
            "baseline_g8r_stats",
            &baseline.g8r_stats_action_id,
        )?);
        evidence_refs.push(evidence(
            store,
            "baseline_yosys_abc_stats",
            &baseline.yosys_abc_stats_action_id,
        )?);
    }
    evidence_refs.sort_by(|a, b| a.role.cmp(&b.role));
    Ok(pb::AnalysisFinding {
        finding_id: Some(id),
        identity: Some(identity),
        structural_hash: structural_digest(current.structural_hash.as_deref()),
        evidence: evidence_refs,
    })
}

fn unique_finalized_run_for_version(
    store: &ArtifactStore,
    campaign_id: &pb::Sha256Digest,
    version: &str,
) -> Result<pb::CampaignRunManifest> {
    let mut candidates = list_finalized_campaign_runs(store)?
        .into_iter()
        .filter(|manifest| {
            manifest.campaign_id.as_ref() == Some(campaign_id)
                && manifest
                    .crate_version
                    .as_ref()
                    .is_some_and(|candidate| candidate.value == version)
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        bail!("no comparable finalized campaign run exists for crate version {version}");
    }
    if candidates.len() > 1 {
        let ids = candidates
            .iter()
            .map(|manifest| {
                digest_hex(
                    required(&manifest.run_id, "campaign_run.run_id")?,
                    "campaign_run.run_id",
                )
            })
            .collect::<Result<Vec<_>>>()?;
        bail!(
            "multiple comparable finalized campaign runs exist for crate version {version}: {}",
            ids.join(", ")
        );
    }
    Ok(candidates.pop().expect("nonempty candidates"))
}

fn previous_finalized_run(
    store: &ArtifactStore,
    current: &pb::CampaignRunManifest,
) -> Result<Option<pb::CampaignRunManifest>> {
    let current_version = &required(&current.crate_version, "campaign_run.crate_version")?.value;
    let campaign_id = required(&current.campaign_id, "campaign_run.campaign_id")?;
    let mut versions = list_finalized_campaign_runs(store)?
        .into_iter()
        .filter(|manifest| manifest.campaign_id.as_ref() == Some(campaign_id))
        .filter_map(|manifest| manifest.crate_version.map(|version| version.value))
        .filter(|version| cmp_dotted_numeric_version(version, current_version).is_lt())
        .collect::<Vec<_>>();
    versions.sort_by(|a, b| cmp_dotted_numeric_version(a, b));
    versions.dedup();
    versions
        .pop()
        .map(|version| unique_finalized_run_for_version(store, campaign_id, &version))
        .transpose()
}

fn sample_has_root_lineage(
    store: &ArtifactStore,
    sample: &StdlibG8rVsYosysSample,
    root_action_id: &str,
) -> Result<bool> {
    for action_id in [
        &sample.ir_action_id,
        &sample.g8r_stats_action_id,
        &sample.yosys_abc_stats_action_id,
    ] {
        if !stored_action_descends_from_root(store, action_id, root_action_id)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn samples_for_exact_run<'a>(
    store: &ArtifactStore,
    manifest: &pb::CampaignRunManifest,
    samples: &'a [StdlibG8rVsYosysSample],
) -> Result<BTreeMap<String, &'a StdlibG8rVsYosysSample>> {
    let version = &required(&manifest.crate_version, "campaign_run.crate_version")?.value;
    let root_action_id = stdlib_root_action_id(manifest)?
        .context("campaign run has no exact stdlib root for analysis")?;
    let mut selected = BTreeMap::new();
    for sample in samples
        .iter()
        .filter(|sample| normalize_tag_version(&sample.crate_version) == version)
    {
        if !sample_has_root_lineage(store, sample, &root_action_id)? {
            continue;
        }
        let key = subject_key(sample);
        if selected.insert(key.clone(), sample).is_some() {
            bail!(
                "campaign run contains duplicate exact-lineage analysis samples for subject {key}"
            );
        }
    }
    if selected.is_empty() {
        let run_id = digest_hex(
            required(&manifest.run_id, "campaign_run.run_id")?,
            "campaign_run.run_id",
        )?;
        bail!(
            "refusing analysis: run {run_id} has no dataset samples descended from its exact stdlib root {root_action_id}"
        );
    }
    Ok(selected)
}

fn build_analysis_report(
    store: &ArtifactStore,
    current: &pb::CampaignRunManifest,
    baseline: Option<&pb::CampaignRunManifest>,
    samples: &[StdlibG8rVsYosysSample],
    generated_at: prost_types::Timestamp,
) -> Result<pb::AnalysisReport> {
    let current_by_subject = samples_for_exact_run(store, current, samples)?;
    let baseline_by_subject = baseline
        .map(|manifest| samples_for_exact_run(store, manifest, samples))
        .transpose()?
        .unwrap_or_default();
    let campaign = required(&current.campaign, "campaign_run.campaign")?;
    let algorithm_version = campaign.analysis_algorithm_version;
    let campaign_id = required(&current.campaign_id, "campaign_run.campaign_id")?;
    let run_id = required(&current.run_id, "campaign_run.run_id")?;
    let baseline_run_id = baseline.and_then(|manifest| manifest.run_id.as_ref());
    let mut findings = Vec::new();
    for (key, current_sample) in current_by_subject {
        let current_loss = fixed_metric(current_sample.g8r_product_loss, "current product loss")?;
        if let Some(baseline_sample) = baseline_by_subject.get(&key) {
            let baseline_loss =
                fixed_metric(baseline_sample.g8r_product_loss, "baseline product loss")?;
            let delta = current_loss.saturating_sub(baseline_loss);
            if delta > CHANGE_THRESHOLD_MICRO {
                findings.push(make_finding(
                    store,
                    campaign_id,
                    run_id,
                    baseline_run_id,
                    algorithm_version,
                    pb::FindingKind::Regression,
                    current_sample,
                    Some(baseline_sample),
                )?);
            } else if delta < -CHANGE_THRESHOLD_MICRO {
                findings.push(make_finding(
                    store,
                    campaign_id,
                    run_id,
                    baseline_run_id,
                    algorithm_version,
                    pb::FindingKind::Improvement,
                    current_sample,
                    Some(baseline_sample),
                )?);
            }
            if current_loss > OUTLIER_THRESHOLD_MICRO && baseline_loss > OUTLIER_THRESHOLD_MICRO {
                findings.push(make_finding(
                    store,
                    campaign_id,
                    run_id,
                    baseline_run_id,
                    algorithm_version,
                    pb::FindingKind::PersistentOutlier,
                    current_sample,
                    Some(baseline_sample),
                )?);
            }
        }
        if current_loss > OUTLIER_THRESHOLD_MICRO && current_sample.structural_hash.is_some() {
            findings.push(make_finding(
                store,
                campaign_id,
                run_id,
                baseline_run_id,
                algorithm_version,
                pb::FindingKind::StructuralHashLoss,
                current_sample,
                baseline_by_subject.get(&key).copied(),
            )?);
        }
    }
    findings.sort_by(|a, b| {
        a.finding_id
            .as_ref()
            .map(|id| id.value.as_slice())
            .cmp(&b.finding_id.as_ref().map(|id| id.value.as_slice()))
    });
    Ok(pb::AnalysisReport {
        record_version: ANALYSIS_RECORD_VERSION,
        campaign_id: Some(campaign_id.clone()),
        run_id: Some(run_id.clone()),
        baseline_run_id: baseline_run_id.cloned(),
        crate_version: current.crate_version.clone(),
        baseline_crate_version: baseline.and_then(|manifest| manifest.crate_version.clone()),
        analysis_algorithm_version: algorithm_version,
        generated_at: Some(generated_at),
        findings,
    })
}

fn validate_report(report: &pb::AnalysisReport) -> Result<()> {
    if report.record_version != ANALYSIS_RECORD_VERSION || report.analysis_algorithm_version == 0 {
        bail!("unsupported or incomplete analysis report version");
    }
    for (digest, field) in [
        (&report.campaign_id, "analysis.campaign_id"),
        (&report.run_id, "analysis.run_id"),
    ] {
        validate_digest(required(digest, field)?, field)?;
    }
    if let Some(id) = &report.baseline_run_id {
        validate_digest(id, "analysis.baseline_run_id")?;
    }
    if required(&report.crate_version, "analysis.crate_version")?
        .value
        .is_empty()
    {
        bail!("analysis crate version must not be empty");
    }
    let generated = required(&report.generated_at, "analysis.generated_at")?;
    if !(0..1_000_000_000).contains(&generated.nanos)
        || chrono::DateTime::from_timestamp(generated.seconds, generated.nanos as u32).is_none()
    {
        bail!("analysis.generated_at is invalid");
    }
    let mut previous: Option<Vec<u8>> = None;
    for finding in &report.findings {
        let identity = required(&finding.identity, "analysis.finding.identity")?;
        crate::query::validate_safe_public_text(
            "analysis.finding.subject_key",
            &identity.subject_key,
            512,
        )?;
        if identity.campaign_id != report.campaign_id
            || identity.run_id != report.run_id
            || identity.baseline_run_id != report.baseline_run_id
            || identity.analysis_algorithm_version != report.analysis_algorithm_version
        {
            bail!("finding identity disagrees with analysis report identity");
        }
        let expected = finding_id(identity)?;
        let actual = required(&finding.finding_id, "analysis.finding.finding_id")?;
        if actual != &expected {
            bail!("analysis finding id does not match finding identity");
        }
        if let Some(hash) = &finding.structural_hash {
            validate_digest(hash, "analysis.finding.structural_hash")?;
        }
        let metric = required(&identity.metric, "analysis.finding.metric")?;
        crate::query::validate_safe_public_text("analysis.finding.metric.name", &metric.name, 128)?;
        crate::query::validate_safe_public_text("analysis.finding.metric.unit", &metric.unit, 128)?;
        let mut prior_role: Option<&str> = None;
        for evidence in &finding.evidence {
            crate::query::validate_safe_public_text(
                "analysis.finding.evidence.role",
                &evidence.role,
                128,
            )?;
            if evidence.role.trim().is_empty()
                || prior_role.is_some_and(|prior| prior >= evidence.role.as_str())
            {
                bail!("analysis evidence roles must be nonempty and strictly sorted");
            }
            let action = required(&evidence.action_id, "analysis.finding.evidence.action_id")?;
            if action.value.len() != 32 {
                bail!("analysis evidence action id must contain exactly 32 bytes");
            }
            if let Some(digest) = &evidence.artifact_sha256 {
                validate_digest(digest, "analysis.finding.evidence.artifact_sha256")?;
            }
            prior_role = Some(&evidence.role);
        }
        if previous
            .as_ref()
            .is_some_and(|prior| prior >= &actual.value)
        {
            bail!("analysis findings must be strictly sorted by finding id");
        }
        previous = Some(actual.value.clone());
    }
    Ok(())
}

fn write_report(path: &Path, report: &pb::AnalysisReport) -> Result<()> {
    validate_report(report)?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("analysis path has no parent"))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("creating analysis directory: {}", parent.display()))?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let nonce = WRITE_NONCE.fetch_add(1, Ordering::Relaxed);
    let temp = parent.join(format!(
        ".analysis.pb.tmp-{}-{timestamp}-{nonce}",
        std::process::id()
    ));
    fs::write(&temp, report.encode_to_vec())
        .with_context(|| format!("writing analysis temp file: {}", temp.display()))?;
    fs::rename(&temp, path).with_context(|| {
        format!(
            "atomically promoting analysis report: {} -> {}",
            temp.display(),
            path.display()
        )
    })
}

pub(crate) fn decode_analysis_report(bytes: &[u8]) -> Result<pb::AnalysisReport> {
    let report = pb::AnalysisReport::decode(bytes).context("decoding AnalysisReport")?;
    validate_report(&report)?;
    Ok(report)
}

fn require_finalized_manifest(manifest: &pb::CampaignRunManifest, label: &str) -> Result<()> {
    let status = pb::CampaignRunStatus::try_from(manifest.status)
        .with_context(|| format!("{label} campaign run status is unknown"))?;
    if !matches!(
        status,
        pb::CampaignRunStatus::Complete | pb::CampaignRunStatus::Degraded
    ) {
        bail!("{label} campaign run is not finalized: {status:?}");
    }
    Ok(())
}

pub(crate) fn validate_analysis_report_against_store(
    store: &ArtifactStore,
    report: &pb::AnalysisReport,
) -> Result<()> {
    validate_report(report)?;
    let run_id = digest_hex(
        required(&report.run_id, "analysis.run_id")?,
        "analysis.run_id",
    )?;
    let current = load_campaign_run_by_id(store, &run_id)
        .with_context(|| format!("loading analysis campaign run {run_id}"))?;
    require_finalized_manifest(&current, "analysis current")?;
    if current.run_id != report.run_id
        || current.campaign_id != report.campaign_id
        || current.crate_version != report.crate_version
    {
        bail!("analysis identity does not match its current campaign run {run_id}");
    }
    let current_campaign = required(&current.campaign, "campaign_run.campaign")?;
    if current_campaign.analysis_algorithm_version != report.analysis_algorithm_version {
        bail!("analysis algorithm version does not match its current campaign run");
    }
    let current_root = stdlib_root_action_id(&current)?
        .context("analysis current campaign run has no exact stdlib root")?;

    let baseline = match (&report.baseline_run_id, &report.baseline_crate_version) {
        (Some(baseline_run_id), Some(baseline_version)) => {
            let baseline_id = digest_hex(baseline_run_id, "analysis.baseline_run_id")?;
            let manifest = load_campaign_run_by_id(store, &baseline_id)
                .with_context(|| format!("loading analysis baseline campaign run {baseline_id}"))?;
            require_finalized_manifest(&manifest, "analysis baseline")?;
            if manifest.run_id.as_ref() != Some(baseline_run_id)
                || manifest.campaign_id != report.campaign_id
                || manifest.crate_version.as_ref() != Some(baseline_version)
            {
                bail!("analysis baseline identity is not campaign-compatible");
            }
            Some(manifest)
        }
        (None, None) => None,
        _ => bail!("analysis baseline run id and crate version must be present together"),
    };
    let baseline_root = match baseline.as_ref() {
        Some(manifest) => Some(
            stdlib_root_action_id(manifest)?
                .context("analysis baseline campaign run has no exact stdlib root")?,
        ),
        None => None,
    };

    for finding in &report.findings {
        let identity = required(&finding.identity, "analysis.finding.identity")?;
        let kind =
            pb::FindingKind::try_from(identity.kind).context("analysis finding kind is unknown")?;
        let metric = required(&identity.metric, "analysis.finding.metric")?;
        if metric.current_microunits.is_none() {
            bail!("analysis finding is missing its current metric value");
        }
        let mut current_ir = false;
        let mut current_g8r = false;
        let mut current_yosys = false;
        let mut baseline_g8r = false;
        let mut baseline_yosys = false;
        for evidence in &finding.evidence {
            let action_id = action_id_to_hex(
                required(&evidence.action_id, "analysis.finding.evidence.action_id")?,
                "analysis.finding.evidence.action_id",
            )?;
            if !store.action_exists(&action_id) {
                bail!(
                    "analysis evidence action does not exist: role={} action_id={action_id}",
                    evidence.role
                );
            }
            let declared = required(
                &evidence.artifact_sha256,
                "analysis.finding.evidence.artifact_sha256",
            )?;
            let actual = artifact_digest(store, &action_id)?;
            if declared != &actual {
                bail!(
                    "analysis evidence digest disagrees with stored provenance: role={} action_id={action_id}",
                    evidence.role
                );
            }
            let lineage_root = match evidence.role.as_str() {
                "current_ir" => {
                    current_ir = true;
                    &current_root
                }
                "current_g8r_stats" => {
                    current_g8r = true;
                    &current_root
                }
                "current_yosys_abc_stats" => {
                    current_yosys = true;
                    &current_root
                }
                "baseline_g8r_stats" => {
                    baseline_g8r = true;
                    baseline_root
                        .as_ref()
                        .context("baseline evidence exists without a baseline campaign run")?
                }
                "baseline_yosys_abc_stats" => {
                    baseline_yosys = true;
                    baseline_root
                        .as_ref()
                        .context("baseline evidence exists without a baseline campaign run")?
                }
                role => bail!("analysis evidence has unsupported role {role}"),
            };
            if !stored_action_descends_from_root(store, &action_id, lineage_root)? {
                bail!(
                    "analysis evidence action is outside exact run lineage: role={} action_id={action_id} root_action_id={lineage_root}",
                    evidence.role
                );
            }
        }
        if !(current_ir && current_g8r && current_yosys) {
            bail!("analysis finding is missing required current evidence roles");
        }
        if baseline_g8r != baseline_yosys {
            bail!("analysis finding baseline evidence roles must be present together");
        }
        if metric.baseline_microunits.is_some() != baseline_g8r {
            bail!("analysis finding baseline metric and evidence must be present together");
        }
        if matches!(
            kind,
            pb::FindingKind::Regression
                | pb::FindingKind::Improvement
                | pb::FindingKind::PersistentOutlier
        ) && !baseline_g8r
        {
            bail!("comparative analysis finding is missing baseline evidence");
        }
    }
    let dataset = load_stdlib_g8r_vs_yosys_dataset_index(store, false)?
        .context("stdlib g8r-vs-yosys dataset is required to validate analysis semantics")?;
    let expected = build_analysis_report(
        store,
        &current,
        baseline.as_ref(),
        &dataset.samples,
        *required(&report.generated_at, "analysis.generated_at")?,
    )?;
    if &expected != report {
        bail!("analysis report semantics do not match exact campaign datasets and manifests");
    }
    Ok(())
}

fn preserve_generated_at_if_unchanged(
    existing: &pb::AnalysisReport,
    mut candidate: pb::AnalysisReport,
) -> pb::AnalysisReport {
    let candidate_generated_at = candidate.generated_at.clone();
    candidate.generated_at = existing.generated_at.clone();
    if &candidate == existing {
        existing.clone()
    } else {
        candidate.generated_at = candidate_generated_at;
        candidate
    }
}

pub(crate) fn analyze_campaign_run(
    store: &ArtifactStore,
    repo_root: &Path,
    crate_version: &str,
    exact_run_id: Option<&str>,
    baseline_crate_version: Option<&str>,
) -> Result<AnalysisSummary> {
    let current = match exact_run_id {
        Some(run_id) => {
            let manifest = load_campaign_run_by_id(store, run_id)?;
            let status = pb::CampaignRunStatus::try_from(manifest.status)
                .context("campaign run status is unknown")?;
            if !matches!(
                status,
                pb::CampaignRunStatus::Complete | pb::CampaignRunStatus::Degraded
            ) {
                bail!("campaign run {run_id} is not finalized: {status:?}");
            }
            manifest
        }
        None => load_finalized_campaign_run_for_version(store, repo_root, crate_version)?,
    };
    let current_version = required(&current.crate_version, "campaign_run.crate_version")?
        .value
        .clone();
    if normalize_tag_version(crate_version) != current_version {
        bail!(
            "selected campaign run crate version {} does not match requested {}",
            current_version,
            crate_version
        );
    }
    let campaign_id = required(&current.campaign_id, "campaign_run.campaign_id")?;
    let baseline = match baseline_crate_version {
        Some(version) => Some(unique_finalized_run_for_version(
            store,
            campaign_id,
            normalize_tag_version(version),
        )?),
        None => previous_finalized_run(store, &current)?,
    };
    let baseline_version = baseline
        .as_ref()
        .and_then(|manifest| manifest.crate_version.as_ref())
        .map(|version| version.value.clone());
    let dataset = load_stdlib_g8r_vs_yosys_dataset_index(store, false)?
        .context("stdlib g8r-vs-yosys dataset must be rebuilt before analysis")?;
    let run_id = required(&current.run_id, "campaign_run.run_id")?;
    let baseline_run_id = baseline
        .as_ref()
        .and_then(|manifest| manifest.run_id.as_ref());
    let mut report = build_analysis_report(
        store,
        &current,
        baseline.as_ref(),
        &dataset.samples,
        timestamp_to_proto(&Utc::now()),
    )?;
    let path = campaign_analysis_path(store, run_id)?;
    if path.exists() {
        let bytes = fs::read(&path)
            .with_context(|| format!("reading existing campaign analysis: {}", path.display()))?;
        let existing = decode_analysis_report(&bytes)?;
        report = preserve_generated_at_if_unchanged(&existing, report);
    }
    validate_analysis_report_against_store(store, &report)?;
    write_report(&path, &report)?;
    let mut by_kind = BTreeMap::new();
    for finding in &report.findings {
        let kind = pb::FindingKind::try_from(required(&finding.identity, "finding.identity")?.kind)
            .context("finding kind is unknown")?;
        *by_kind.entry(kind.as_str_name().to_string()).or_insert(0) += 1;
    }
    Ok(AnalysisSummary {
        run_id: digest_hex(run_id, "run_id")?,
        crate_version: current_version,
        baseline_run_id: baseline_run_id
            .map(|id| digest_hex(id, "baseline_run_id"))
            .transpose()?,
        baseline_crate_version: baseline_version,
        finding_count: report.findings.len(),
        findings_by_kind: by_kind,
        analysis_path: path.display().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::campaign::{campaign_run_path, persist_campaign_run_plan};
    use crate::model::{ActionSpec, ArtifactRef, ArtifactType, OutputFile, Provenance};
    use crate::proto::{
        action_id_to_hex, action_id_to_proto, action_spec_from_proto, action_spec_to_proto,
        driver_runtime_from_proto,
    };
    use serde_json::json;

    fn digest(byte: u8) -> pb::Sha256Digest {
        pb::Sha256Digest {
            value: vec![byte; 32],
        }
    }

    fn test_runtime() -> crate::model::DriverRuntimeSpec {
        crate::model::DriverRuntimeSpec {
            driver_version: "0.31.0".to_string(),
            release_platform: "test".to_string(),
            docker_image: "test:image".to_string(),
            dockerfile: "docker/test.Dockerfile".to_string(),
            docker_image_id: "a".repeat(64),
            dockerfile_sha256: "b".repeat(64),
            release_cache_input_sha256: "c".repeat(64),
        }
    }

    fn promote_test_action(store: &ArtifactStore, action: ActionSpec) -> String {
        let action_id = crate::executor::compute_action_id(&action).expect("action id");
        let bytes = action_id.as_bytes();
        let staging = store
            .staging_dir()
            .join(format!("{action_id}-analysis-test"));
        fs::create_dir_all(staging.join("payload")).expect("create staging payload");
        fs::write(staging.join("payload/result"), bytes).expect("write result");
        let provenance = Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: action_id.clone(),
            created_utc: Utc::now(),
            action,
            dependencies: Vec::new(),
            output_artifact: ArtifactRef {
                action_id: action_id.clone(),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: "payload/result".to_string(),
            },
            output_files: vec![OutputFile {
                path: "payload/result".to_string(),
                bytes: bytes.len() as u64,
                sha256: hex::encode(Sha256::digest(bytes)),
            }],
            commands: Vec::new(),
            details: json!({"test": true}),
            suggested_next_actions: Vec::new(),
        };
        fs::write(
            staging.join("provenance.pb"),
            crate::proto::encode_provenance(&provenance).expect("encode provenance"),
        )
        .expect("write provenance");
        store
            .promote_staging_action_dir(&action_id, &staging)
            .expect("promote action");
        action_id
    }

    fn test_lineage(
        store: &ArtifactStore,
        root_seed: u8,
    ) -> (pb::CampaignRunManifest, StdlibG8rVsYosysSample) {
        let root_action = ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
            version: "v0.30.0".to_string(),
            discovery_runtime: Some(test_runtime()),
            stdlib_tarball_sha256: format!("{root_seed:064x}"),
        };
        let root_id = promote_test_action(store, root_action.clone());
        let ir_id = promote_test_action(
            store,
            ActionSpec::DriverDslxFnToIr {
                dslx_subtree_action_id: root_id.clone(),
                dslx_file: "xls/dslx/stdlib/test.x".to_string(),
                dslx_fn_name: "subject".to_string(),
                version: "v0.30.0".to_string(),
                runtime: test_runtime(),
            },
        );
        let g8r_id = promote_test_action(
            store,
            ActionSpec::DriverIrToOpt {
                ir_action_id: ir_id.clone(),
                top_fn_name: Some("g8r".to_string()),
                version: "v0.30.0".to_string(),
                runtime: test_runtime(),
            },
        );
        let yosys_id = promote_test_action(
            store,
            ActionSpec::DriverIrToOpt {
                ir_action_id: ir_id.clone(),
                top_fn_name: Some("yosys".to_string()),
                version: "v0.30.0".to_string(),
                runtime: test_runtime(),
            },
        );
        let manifest = pb::CampaignRunManifest {
            run_id: Some(digest(root_seed)),
            crate_version: Some(pb::CrateVersion {
                value: "0.31.0".to_string(),
            }),
            root_actions: vec![pb::CampaignRootAction {
                action_id: Some(action_id_to_proto(&root_id, "root id").expect("root proto")),
                action: Some(action_spec_to_proto(&root_action).expect("root action proto")),
            }],
            ..Default::default()
        };
        let sample = StdlibG8rVsYosysSample {
            fn_key: "stdlib::subject".to_string(),
            crate_version: "0.31.0".to_string(),
            dso_version: "0.30.0".to_string(),
            ir_action_id: ir_id,
            ir_top: None,
            structural_hash: None,
            ir_node_count: 1,
            g8r_nodes: 1.0,
            g8r_levels: 1.0,
            yosys_abc_nodes: 1.0,
            yosys_abc_levels: 1.0,
            g8r_product: 1.0,
            yosys_abc_product: 1.0,
            g8r_product_loss: root_seed as f64,
            g8r_stats_action_id: g8r_id,
            yosys_abc_stats_action_id: yosys_id,
        };
        (manifest, sample)
    }

    #[test]
    fn exact_run_samples_reject_same_version_rows_from_another_root() {
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-analysis-lineage-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let (old_manifest, old_sample) = test_lineage(&store, 1);
        let (new_manifest, new_sample) = test_lineage(&store, 2);
        let samples = vec![old_sample.clone(), new_sample.clone()];

        let selected =
            samples_for_exact_run(&store, &new_manifest, &samples).expect("new exact lineage");
        assert_eq!(selected.len(), 1);
        assert_eq!(
            selected["stdlib::subject#<default>"].ir_action_id,
            new_sample.ir_action_id
        );
        let old_selected =
            samples_for_exact_run(&store, &old_manifest, &samples).expect("old exact lineage");
        assert_eq!(
            old_selected["stdlib::subject#<default>"].ir_action_id,
            old_sample.ir_action_id
        );

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn persisted_analysis_evidence_must_match_digest_and_exact_run_lineage() {
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-analysis-persisted-lineage-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let repo_root = std::env::current_dir().expect("repo root");
        let version = crate::versioning::load_version_compat_map(&repo_root)
            .expect("version map")
            .into_keys()
            .next()
            .expect("known version");
        let mut manifest =
            persist_campaign_run_plan(&store, &repo_root, &version).expect("persist campaign plan");
        let root_action_id = stdlib_root_action_id(&manifest)
            .expect("stdlib root lookup")
            .expect("stdlib root");
        let root_action = manifest
            .root_actions
            .iter()
            .find(|root| {
                root.action_id.as_ref().is_some_and(|id| {
                    action_id_to_hex(id, "test root id").is_ok_and(|value| value == root_action_id)
                })
            })
            .and_then(|root| root.action.as_ref())
            .map(action_spec_from_proto)
            .transpose()
            .expect("decode root action")
            .expect("root action spec");
        assert_eq!(promote_test_action(&store, root_action), root_action_id);

        let runtime = driver_runtime_from_proto(
            manifest
                .driver_runtime
                .as_ref()
                .expect("manifest driver runtime"),
            "test driver runtime",
        )
        .expect("decode driver runtime");
        let action_version = format!("v{version}");
        let ir_action_id = promote_test_action(
            &store,
            ActionSpec::DriverDslxFnToIr {
                dslx_subtree_action_id: root_action_id,
                dslx_file: "xls/dslx/stdlib/test.x".to_string(),
                dslx_fn_name: "subject".to_string(),
                version: action_version.clone(),
                runtime: runtime.clone(),
            },
        );
        let g8r_action_id = promote_test_action(
            &store,
            ActionSpec::DriverIrToOpt {
                ir_action_id: ir_action_id.clone(),
                top_fn_name: Some("g8r".to_string()),
                version: action_version.clone(),
                runtime: runtime.clone(),
            },
        );
        let yosys_action_id = promote_test_action(
            &store,
            ActionSpec::DriverIrToOpt {
                ir_action_id: ir_action_id.clone(),
                top_fn_name: Some("yosys".to_string()),
                version: action_version,
                runtime,
            },
        );
        let sample = StdlibG8rVsYosysSample {
            fn_key: "stdlib::subject".to_string(),
            crate_version: version.clone(),
            dso_version: manifest
                .dso_version
                .as_ref()
                .expect("dso version")
                .value
                .clone(),
            ir_action_id,
            ir_top: None,
            structural_hash: Some("a".repeat(64)),
            ir_node_count: 1,
            g8r_nodes: 2.0,
            g8r_levels: 2.0,
            yosys_abc_nodes: 1.0,
            yosys_abc_levels: 1.0,
            g8r_product: 4.0,
            yosys_abc_product: 1.0,
            g8r_product_loss: 3.0,
            g8r_stats_action_id: g8r_action_id,
            yosys_abc_stats_action_id: yosys_action_id,
        };
        crate::query::write_stdlib_g8r_vs_yosys_dataset_index(
            &store,
            &crate::view::StdlibG8rVsYosysDataset {
                fraig: false,
                samples: vec![sample.clone()],
                min_ir_nodes: 1,
                max_ir_nodes: 1,
                g8r_only_count: 0,
                yosys_only_count: 0,
                available_crate_versions: vec![version.clone()],
            },
        )
        .expect("write semantic validation dataset");

        manifest.status = pb::CampaignRunStatus::Complete as i32;
        let root_count = manifest.root_actions.len() as u64;
        let completion = manifest.completion.as_mut().expect("completion");
        completion.status = pb::CampaignRunStatus::Complete as i32;
        completion.root_action_count = root_count;
        completion.completed_root_count = root_count;
        let manifest_path =
            campaign_run_path(&store, manifest.run_id.as_ref().expect("manifest run id"))
                .expect("manifest path");
        fs::write(&manifest_path, manifest.encode_to_vec()).expect("finalize manifest fixture");

        let campaign_id = manifest.campaign_id.as_ref().expect("campaign id");
        let run_id = manifest.run_id.as_ref().expect("run id");
        let finding = make_finding(
            &store,
            campaign_id,
            run_id,
            None,
            manifest
                .campaign
                .as_ref()
                .expect("campaign")
                .analysis_algorithm_version,
            pb::FindingKind::StructuralHashLoss,
            &sample,
            None,
        )
        .expect("finding");
        let report = pb::AnalysisReport {
            record_version: ANALYSIS_RECORD_VERSION,
            campaign_id: Some(campaign_id.clone()),
            run_id: Some(run_id.clone()),
            baseline_run_id: None,
            crate_version: manifest.crate_version.clone(),
            baseline_crate_version: None,
            analysis_algorithm_version: manifest
                .campaign
                .as_ref()
                .expect("campaign")
                .analysis_algorithm_version,
            generated_at: Some(timestamp_to_proto(&Utc::now())),
            findings: vec![finding],
        };
        validate_analysis_report_against_store(&store, &report)
            .expect("exact-lineage report validates");

        let (_, foreign_sample) = test_lineage(&store, 9);
        let mut wrong_lineage = report.clone();
        let current_ir = wrong_lineage.findings[0]
            .evidence
            .iter_mut()
            .find(|evidence| evidence.role == "current_ir")
            .expect("current IR evidence");
        *current_ir =
            evidence(&store, "current_ir", &foreign_sample.ir_action_id).expect("foreign evidence");
        let error = validate_analysis_report_against_store(&store, &wrong_lineage)
            .expect_err("foreign lineage must fail");
        assert!(format!("{error:#}").contains("outside exact run lineage"));

        let mut wrong_digest = report.clone();
        wrong_digest.findings[0].evidence[0]
            .artifact_sha256
            .as_mut()
            .expect("evidence digest")
            .value[0] ^= 1;
        let error = validate_analysis_report_against_store(&store, &wrong_digest)
            .expect_err("wrong evidence digest must fail");
        assert!(format!("{error:#}").contains("disagrees with stored provenance"));

        let mut fabricated_metric = report.clone();
        let identity = fabricated_metric.findings[0]
            .identity
            .as_mut()
            .expect("finding identity");
        identity
            .metric
            .as_mut()
            .expect("finding metric")
            .current_microunits = Some(123);
        fabricated_metric.findings[0].finding_id =
            Some(finding_id(identity).expect("recompute self-consistent finding id"));
        let error = validate_analysis_report_against_store(&store, &fabricated_metric)
            .expect_err("fabricated metric must fail semantic recomputation");
        assert!(format!("{error:#}").contains("semantics do not match"));

        let mut private_subject = report;
        let identity = private_subject.findings[0]
            .identity
            .as_mut()
            .expect("finding identity");
        identity.subject_key = "/srv/private/build/input.ir#main".to_string();
        private_subject.findings[0].finding_id =
            Some(finding_id(identity).expect("recompute private finding id"));
        let error = validate_analysis_report_against_store(&store, &private_subject)
            .expect_err("private absolute path must fail public-text validation");
        assert!(format!("{error:#}").contains("absolute host path"));

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn finding_identity_is_deterministic_and_metric_sensitive() {
        let identity = pb::FindingIdentity {
            identity_version: FINDING_IDENTITY_VERSION,
            campaign_id: Some(digest(1)),
            run_id: Some(digest(2)),
            baseline_run_id: Some(digest(3)),
            kind: pb::FindingKind::Regression as i32,
            subject_key: "stdlib::f32::add#main".to_string(),
            metric: Some(pb::FindingMetric {
                name: "g8r_product_loss".to_string(),
                baseline_microunits: Some(100_000),
                current_microunits: Some(200_000),
                unit: "ratio".to_string(),
            }),
            analysis_algorithm_version: 1,
        };
        let first = finding_id(&identity).expect("finding id");
        assert_eq!(first, finding_id(&identity).expect("finding id"));
        let mut changed = identity;
        changed.metric.as_mut().unwrap().current_microunits = Some(200_001);
        assert_ne!(first, finding_id(&changed).expect("changed finding id"));
    }

    #[test]
    fn malformed_analysis_report_is_rejected() {
        let error = validate_report(&pb::AnalysisReport::default()).expect_err("invalid");
        assert!(error.to_string().contains("version"));
    }

    #[test]
    fn unchanged_analysis_preserves_generation_timestamp() {
        let mut existing = pb::AnalysisReport {
            record_version: 1,
            generated_at: Some(prost_types::Timestamp {
                seconds: 100,
                nanos: 0,
            }),
            ..Default::default()
        };
        let mut candidate = existing.clone();
        candidate.generated_at = Some(prost_types::Timestamp {
            seconds: 200,
            nanos: 0,
        });
        assert_eq!(
            preserve_generated_at_if_unchanged(&existing, candidate.clone()),
            existing
        );

        existing.analysis_algorithm_version = 1;
        let changed = preserve_generated_at_if_unchanged(&existing, candidate);
        assert_eq!(
            changed.generated_at.expect("candidate timestamp").seconds,
            200
        );
    }
}
