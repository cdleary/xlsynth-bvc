// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, bail};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::cli::CorpusSchedulingPolicyPreset;
use crate::proto::{RELEASE_PROGRESSION_IR_SCHEDULING_POLICY, v1 as pb};

const SCHEDULING_POLICY_SCHEMA_VERSION: u32 = 1;
const SCHEDULING_POLICY_MARKER_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CorpusPriorityTierRecord {
    pub(crate) tier_name: String,
    pub(crate) queue_priority_boost: i32,
    pub(crate) structural_hashes: Vec<String>,
    pub(crate) reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CorpusSchedulingPolicyRecord {
    pub(crate) schema_version: u32,
    pub(crate) policy_name: String,
    pub(crate) semantic_version: u32,
    pub(crate) config_sha256: String,
    pub(crate) expected_corpus_sample_count: u64,
    pub(crate) expected_corpus_artifact_manifest_sha256: String,
    pub(crate) priority_tiers: Vec<CorpusPriorityTierRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CorpusSchedulingPolicyIdentity {
    pub(crate) schema_version: u32,
    pub(crate) policy_name: String,
    pub(crate) semantic_version: u32,
    pub(crate) config_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CorpusSchedulingArtifact {
    pub(crate) source_relpath: String,
    pub(crate) source_sha256: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedCorpusSchedulingPolicy {
    pub(crate) record: CorpusSchedulingPolicyRecord,
    priority_boost_by_structural_hash: BTreeMap<String, i32>,
}

fn validate_text(value: &str, field: &str) -> Result<()> {
    if value.is_empty() || value.trim() != value || value.contains('\0') {
        bail!("{field} must be nonempty, trimmed, and contain no NUL");
    }
    Ok(())
}

fn is_lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn digest_to_hex(digest: Option<&pb::Sha256Digest>, field: &str) -> Result<String> {
    let digest = digest.with_context(|| format!("{field} must be present"))?;
    if digest.value.len() != 32 {
        bail!("{field} must contain exactly 32 bytes");
    }
    Ok(hex::encode(&digest.value))
}

fn structural_hash_from_source_relpath(source_relpath: &str) -> Result<&str> {
    if source_relpath.contains('/') || source_relpath.contains('\\') {
        bail!(
            "scheduling policy requires root-level <structural-hash>.ir inputs; got {:?}",
            source_relpath
        );
    }
    let structural_hash = source_relpath.strip_suffix(".ir").with_context(|| {
        format!(
            "scheduling policy requires <structural-hash>.ir inputs; got {:?}",
            source_relpath
        )
    })?;
    if !is_lower_hex_sha256(structural_hash) {
        bail!(
            "scheduling policy requires a lowercase SHA-256 filename; got {:?}",
            source_relpath
        );
    }
    Ok(structural_hash)
}

pub(crate) fn resolve(
    preset: Option<CorpusSchedulingPolicyPreset>,
    source_artifacts: &[CorpusSchedulingArtifact],
) -> Result<Option<ResolvedCorpusSchedulingPolicy>> {
    let Some(preset) = preset else {
        return Ok(None);
    };
    let policy_bytes = match preset {
        CorpusSchedulingPolicyPreset::ReleaseProgressionIrV1 => {
            RELEASE_PROGRESSION_IR_SCHEDULING_POLICY
        }
    };
    let policy = pb::IrDirCorpusSchedulingPolicy::decode(policy_bytes)
        .context("decoding embedded IR directory corpus scheduling policy")?;
    if policy.schema_version != SCHEDULING_POLICY_SCHEMA_VERSION {
        bail!(
            "unsupported corpus scheduling policy schema version {}; expected {}",
            policy.schema_version,
            SCHEDULING_POLICY_SCHEMA_VERSION
        );
    }
    validate_text(&policy.policy_name, "scheduling_policy.policy_name")?;
    if policy.policy_name != "release-progression-ir" {
        bail!(
            "scheduling policy preset does not match embedded policy name {:?}",
            policy.policy_name
        );
    }
    if policy.semantic_version == 0 {
        bail!("scheduling_policy.semantic_version must be nonzero");
    }
    if policy.expected_corpus_sample_count == 0 {
        bail!("scheduling_policy.expected_corpus_sample_count must be nonzero");
    }
    let expected_corpus_artifact_manifest_sha256 = digest_to_hex(
        policy.expected_corpus_artifact_manifest_sha256.as_ref(),
        "scheduling_policy.expected_corpus_artifact_manifest_sha256",
    )?;
    if source_artifacts.len() as u64 != policy.expected_corpus_sample_count {
        bail!(
            "scheduling policy {:?} expects {} corpus samples, got {}",
            policy.policy_name,
            policy.expected_corpus_sample_count,
            source_artifacts.len()
        );
    }

    let mut corpus_artifacts = Vec::with_capacity(source_artifacts.len());
    for artifact in source_artifacts {
        let structural_hash = structural_hash_from_source_relpath(&artifact.source_relpath)?;
        if !is_lower_hex_sha256(&artifact.source_sha256) {
            bail!(
                "scheduling policy requires lowercase SHA-256 source digests; got {:?} for {:?}",
                artifact.source_sha256,
                artifact.source_relpath
            );
        }
        corpus_artifacts.push((structural_hash.to_string(), artifact.source_sha256.clone()));
    }
    corpus_artifacts.sort_by(|lhs, rhs| lhs.0.cmp(&rhs.0));
    if corpus_artifacts
        .windows(2)
        .any(|pair| pair[0].0 == pair[1].0)
    {
        bail!("scheduling-policy corpus contains duplicate structural hashes");
    }
    let mut corpus_artifact_manifest_hasher = Sha256::new();
    corpus_artifact_manifest_hasher.update(b"xlsynth-bvc/ir-dir-corpus-artifact-manifest/v1\0");
    for (structural_hash, source_sha256) in &corpus_artifacts {
        corpus_artifact_manifest_hasher.update(
            hex::decode(structural_hash).expect("validated structural SHA-256 hexadecimal"),
        );
        corpus_artifact_manifest_hasher
            .update(hex::decode(source_sha256).expect("validated source SHA-256 hexadecimal"));
    }
    let actual_corpus_artifact_manifest_sha256 =
        hex::encode(corpus_artifact_manifest_hasher.finalize());
    if actual_corpus_artifact_manifest_sha256 != expected_corpus_artifact_manifest_sha256 {
        bail!(
            "scheduling policy {:?} corpus artifact manifest mismatch: expected {}, got {}",
            policy.policy_name,
            expected_corpus_artifact_manifest_sha256,
            actual_corpus_artifact_manifest_sha256
        );
    }

    if policy.priority_tiers.is_empty() {
        bail!("scheduling_policy.priority_tiers must not be empty");
    }
    let corpus_hash_set: BTreeSet<_> = corpus_artifacts
        .into_iter()
        .map(|(structural_hash, _)| structural_hash)
        .collect();
    let mut tier_names = BTreeSet::new();
    let mut priority_boost_by_structural_hash = BTreeMap::new();
    let mut priority_tiers = Vec::with_capacity(policy.priority_tiers.len());
    for tier in policy.priority_tiers {
        validate_text(&tier.tier_name, "scheduling_policy.priority_tier.tier_name")?;
        validate_text(&tier.reason, "scheduling_policy.priority_tier.reason")?;
        if !tier_names.insert(tier.tier_name.clone()) {
            bail!(
                "scheduling policy contains duplicate priority tier {:?}",
                tier.tier_name
            );
        }
        if tier.queue_priority_boost <= 0 {
            bail!(
                "scheduling policy priority tier {:?} must have a positive queue priority boost",
                tier.tier_name
            );
        }
        if tier.structural_hashes.is_empty() {
            bail!(
                "scheduling policy priority tier {:?} must contain structural hashes",
                tier.tier_name
            );
        }

        let mut previous_hash: Option<String> = None;
        let mut structural_hashes = Vec::with_capacity(tier.structural_hashes.len());
        for structural_hash_digest in &tier.structural_hashes {
            let structural_hash = digest_to_hex(
                Some(structural_hash_digest),
                "scheduling_policy.priority_tier.structural_hash",
            )?;
            if previous_hash
                .as_deref()
                .is_some_and(|previous| previous >= structural_hash.as_str())
            {
                bail!(
                    "scheduling policy priority tier {:?} structural hashes must be sorted and unique",
                    tier.tier_name
                );
            }
            previous_hash = Some(structural_hash.clone());
            if !corpus_hash_set.contains(&structural_hash) {
                bail!(
                    "scheduling policy priority tier {:?} hash {} is not in the corpus",
                    tier.tier_name,
                    structural_hash
                );
            }
            if priority_boost_by_structural_hash
                .insert(structural_hash.clone(), tier.queue_priority_boost)
                .is_some()
            {
                bail!(
                    "scheduling policy structural hash {} appears in multiple priority tiers",
                    structural_hash
                );
            }
            structural_hashes.push(structural_hash);
        }
        priority_tiers.push(CorpusPriorityTierRecord {
            tier_name: tier.tier_name,
            queue_priority_boost: tier.queue_priority_boost,
            structural_hashes,
            reason: tier.reason,
        });
    }

    let config_sha256 = hex::encode(Sha256::digest(policy_bytes));
    Ok(Some(ResolvedCorpusSchedulingPolicy {
        record: CorpusSchedulingPolicyRecord {
            schema_version: policy.schema_version,
            policy_name: policy.policy_name,
            semantic_version: policy.semantic_version,
            config_sha256,
            expected_corpus_sample_count: policy.expected_corpus_sample_count,
            expected_corpus_artifact_manifest_sha256,
            priority_tiers,
        },
        priority_boost_by_structural_hash,
    }))
}

pub(crate) fn scheduling_policy_identity(
    record: &CorpusSchedulingPolicyRecord,
) -> CorpusSchedulingPolicyIdentity {
    CorpusSchedulingPolicyIdentity {
        schema_version: record.schema_version,
        policy_name: record.policy_name.clone(),
        semantic_version: record.semantic_version,
        config_sha256: record.config_sha256.clone(),
    }
}

pub(crate) fn encode_scheduling_policy_marker(
    record: &CorpusSchedulingPolicyRecord,
) -> Result<Vec<u8>> {
    if !is_lower_hex_sha256(&record.config_sha256) {
        bail!("scheduling policy record config_sha256 must be a lowercase SHA-256");
    }
    Ok(pb::IrDirCorpusSchedulingPolicyMarker {
        marker_schema_version: SCHEDULING_POLICY_MARKER_SCHEMA_VERSION,
        policy_schema_version: record.schema_version,
        policy_name: record.policy_name.clone(),
        policy_semantic_version: record.semantic_version,
        policy_config_sha256: Some(pb::Sha256Digest {
            value: hex::decode(&record.config_sha256)
                .expect("validated scheduling policy config SHA-256 hexadecimal"),
        }),
    }
    .encode_to_vec())
}

pub(crate) fn decode_scheduling_policy_marker(
    bytes: &[u8],
) -> Result<CorpusSchedulingPolicyIdentity> {
    let marker = pb::IrDirCorpusSchedulingPolicyMarker::decode(bytes)
        .context("decoding corpus scheduling policy marker")?;
    if marker.marker_schema_version != SCHEDULING_POLICY_MARKER_SCHEMA_VERSION {
        bail!(
            "unsupported corpus scheduling policy marker schema version {}; expected {}",
            marker.marker_schema_version,
            SCHEDULING_POLICY_MARKER_SCHEMA_VERSION
        );
    }
    if marker.policy_schema_version != SCHEDULING_POLICY_SCHEMA_VERSION {
        bail!(
            "unsupported marked corpus scheduling policy schema version {}; expected {}",
            marker.policy_schema_version,
            SCHEDULING_POLICY_SCHEMA_VERSION
        );
    }
    validate_text(&marker.policy_name, "scheduling_policy_marker.policy_name")?;
    if marker.policy_semantic_version == 0 {
        bail!("scheduling_policy_marker.policy_semantic_version must be nonzero");
    }
    let config_sha256 = digest_to_hex(
        marker.policy_config_sha256.as_ref(),
        "scheduling_policy_marker.policy_config_sha256",
    )?;
    Ok(CorpusSchedulingPolicyIdentity {
        schema_version: marker.policy_schema_version,
        policy_name: marker.policy_name,
        semantic_version: marker.policy_semantic_version,
        config_sha256,
    })
}

pub(crate) fn priority_boost(
    policy: Option<&ResolvedCorpusSchedulingPolicy>,
    source_relpath: &str,
) -> Result<i32> {
    let Some(policy) = policy else {
        return Ok(0);
    };
    let structural_hash = structural_hash_from_source_relpath(source_relpath)?;
    Ok(policy
        .priority_boost_by_structural_hash
        .get(structural_hash)
        .copied()
        .unwrap_or(0))
}

pub(crate) fn prioritized_sample_count(policy: Option<&CorpusSchedulingPolicyRecord>) -> usize {
    policy
        .map(|policy| {
            policy
                .priority_tiers
                .iter()
                .map(|tier| tier.structural_hashes.len())
                .sum()
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canonical_source_artifacts() -> Vec<CorpusSchedulingArtifact> {
        include_str!("site_assets/release_progression_ir_artifacts.tsv")
            .lines()
            .map(|line| {
                let (structural_hash, source_sha256) = line
                    .split_once('\t')
                    .expect("artifact manifest line must be tab-separated");
                CorpusSchedulingArtifact {
                    source_relpath: format!("{structural_hash}.ir"),
                    source_sha256: source_sha256.to_string(),
                }
            })
            .collect()
    }

    #[test]
    fn release_progression_policy_matches_pinned_corpus() {
        let source_artifacts = canonical_source_artifacts();
        let structural_hashes = source_artifacts
            .iter()
            .map(|artifact| artifact.source_relpath.trim_end_matches(".ir"))
            .collect::<Vec<_>>();
        assert_eq!(
            structural_hashes,
            include_str!("site_assets/release_progression_ir_hashes.txt")
                .lines()
                .collect::<Vec<_>>()
        );
        let policy = resolve(
            Some(CorpusSchedulingPolicyPreset::ReleaseProgressionIrV1),
            &source_artifacts,
        )
        .expect("resolve policy")
        .expect("selected policy");

        assert_eq!(policy.record.policy_name, "release-progression-ir");
        assert_eq!(prioritized_sample_count(Some(&policy.record)), 3);
        assert_eq!(
            priority_boost(
                Some(&policy),
                "c8d78323bfef010613b71cae233b0ab7835ffb4132794e9a0036254891b6a814.ir"
            )
            .expect("priority boost"),
            100
        );
        assert_eq!(
            priority_boost(
                Some(&policy),
                "02e46faeb2d2b8a51e331e763a76adc98ab76ea374a5b33523f30fbf6509a5e0.ir"
            )
            .expect("default priority"),
            0
        );
    }

    #[test]
    fn release_progression_policy_rejects_a_different_corpus() {
        let error = resolve(
            Some(CorpusSchedulingPolicyPreset::ReleaseProgressionIrV1),
            &[CorpusSchedulingArtifact {
                source_relpath: "0".repeat(64) + ".ir",
                source_sha256: "0".repeat(64),
            }],
        )
        .expect_err("different corpus must be rejected");

        assert!(error.to_string().contains("expects 187 corpus samples"));
    }

    #[test]
    fn release_progression_policy_rejects_changed_ir_bytes() {
        let mut source_artifacts = canonical_source_artifacts();
        source_artifacts[0].source_sha256 = "0".repeat(64);

        let error = resolve(
            Some(CorpusSchedulingPolicyPreset::ReleaseProgressionIrV1),
            &source_artifacts,
        )
        .expect_err("changed IR bytes must be rejected");

        assert!(
            error
                .to_string()
                .contains("corpus artifact manifest mismatch")
        );
    }

    #[test]
    fn scheduling_policy_marker_round_trips_validated_identity() {
        let policy = resolve(
            Some(CorpusSchedulingPolicyPreset::ReleaseProgressionIrV1),
            &canonical_source_artifacts(),
        )
        .expect("resolve policy")
        .expect("selected policy");
        let encoded =
            encode_scheduling_policy_marker(&policy.record).expect("encode policy marker");
        let decoded = decode_scheduling_policy_marker(&encoded).expect("decode policy marker");

        assert_eq!(decoded, scheduling_policy_identity(&policy.record));
    }
}
