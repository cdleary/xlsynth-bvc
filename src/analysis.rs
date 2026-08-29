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
    campaign_analysis_path, list_finalized_campaign_runs, load_finalized_campaign_run_for_version,
};
use crate::proto::v1 as pb;
use crate::proto::{action_id_to_proto, timestamp_to_proto};
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

fn artifact_digest(store: &ArtifactStore, action_id: &str) -> Option<pb::Sha256Digest> {
    let provenance = store.load_provenance(action_id).ok()?;
    let output = provenance.output_files.first()?;
    let bytes = hex::decode(&output.sha256).ok()?;
    (bytes.len() == 32).then_some(pb::Sha256Digest { value: bytes })
}

fn evidence(store: &ArtifactStore, role: &str, action_id: &str) -> Result<pb::FindingEvidence> {
    Ok(pb::FindingEvidence {
        role: role.to_string(),
        action_id: Some(action_id_to_proto(action_id, "finding.evidence.action_id")?),
        artifact_sha256: artifact_digest(store, action_id),
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

fn previous_finalized_run(
    store: &ArtifactStore,
    current_version: &str,
) -> Result<Option<pb::CampaignRunManifest>> {
    let mut candidates = list_finalized_campaign_runs(store)?
        .into_iter()
        .filter(|manifest| {
            manifest.crate_version.as_ref().is_some_and(|version| {
                cmp_dotted_numeric_version(&version.value, current_version).is_lt()
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|a, b| {
        cmp_dotted_numeric_version(
            &a.crate_version.as_ref().expect("filtered").value,
            &b.crate_version.as_ref().expect("filtered").value,
        )
    });
    Ok(candidates.pop())
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
        let mut prior_role: Option<&str> = None;
        for evidence in &finding.evidence {
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

pub(crate) fn analyze_campaign_run(
    store: &ArtifactStore,
    repo_root: &Path,
    crate_version: &str,
    baseline_crate_version: Option<&str>,
) -> Result<AnalysisSummary> {
    let current = load_finalized_campaign_run_for_version(store, repo_root, crate_version)?;
    let current_version = required(&current.crate_version, "campaign_run.crate_version")?
        .value
        .clone();
    let baseline = match baseline_crate_version {
        Some(version) => Some(load_finalized_campaign_run_for_version(
            store, repo_root, version,
        )?),
        None => previous_finalized_run(store, &current_version)?,
    };
    let baseline_version = baseline
        .as_ref()
        .and_then(|manifest| manifest.crate_version.as_ref())
        .map(|version| version.value.clone());
    let dataset = load_stdlib_g8r_vs_yosys_dataset_index(store, false)?
        .context("stdlib g8r-vs-yosys dataset must be rebuilt before analysis")?;
    let current_by_subject = dataset
        .samples
        .iter()
        .filter(|sample| normalize_tag_version(&sample.crate_version) == current_version)
        .map(|sample| (subject_key(sample), sample))
        .collect::<BTreeMap<_, _>>();
    let baseline_by_subject = baseline_version
        .as_deref()
        .map(|version| {
            dataset
                .samples
                .iter()
                .filter(|sample| normalize_tag_version(&sample.crate_version) == version)
                .map(|sample| (subject_key(sample), sample))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let campaign = required(&current.campaign, "campaign_run.campaign")?;
    let algorithm_version = campaign.analysis_algorithm_version;
    let campaign_id = required(&current.campaign_id, "campaign_run.campaign_id")?;
    let run_id = required(&current.run_id, "campaign_run.run_id")?;
    let baseline_run_id = baseline
        .as_ref()
        .and_then(|manifest| manifest.run_id.as_ref());
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
    let report = pb::AnalysisReport {
        record_version: ANALYSIS_RECORD_VERSION,
        campaign_id: Some(campaign_id.clone()),
        run_id: Some(run_id.clone()),
        baseline_run_id: baseline_run_id.cloned(),
        crate_version: Some(pb::CrateVersion {
            value: current_version.clone(),
        }),
        baseline_crate_version: baseline_version
            .clone()
            .map(|value| pb::CrateVersion { value }),
        analysis_algorithm_version: algorithm_version,
        generated_at: Some(timestamp_to_proto(&Utc::now())),
        findings,
    };
    let path = campaign_analysis_path(store, run_id)?;
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

    fn digest(byte: u8) -> pb::Sha256Digest {
        pb::Sha256Digest {
            value: vec![byte; 32],
        }
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
}
