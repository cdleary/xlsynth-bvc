// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, bail};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::cli::CorpusSchedulingPolicyPreset;
use crate::proto::{RELEASE_PROGRESSION_IR_SCHEDULING_POLICY, v1 as pb};

const SCHEDULING_POLICY_SCHEMA_VERSION: u32 = 1;

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
    pub(crate) expected_corpus_hash_manifest_sha256: String,
    pub(crate) priority_tiers: Vec<CorpusPriorityTierRecord>,
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
    source_relpaths: &[String],
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
    if !is_lower_hex_sha256(&policy.expected_corpus_hash_manifest_sha256) {
        bail!("scheduling_policy.expected_corpus_hash_manifest_sha256 must be a lowercase SHA-256");
    }
    if source_relpaths.len() as u64 != policy.expected_corpus_sample_count {
        bail!(
            "scheduling policy {:?} expects {} corpus samples, got {}",
            policy.policy_name,
            policy.expected_corpus_sample_count,
            source_relpaths.len()
        );
    }

    let mut corpus_hashes = Vec::with_capacity(source_relpaths.len());
    for source_relpath in source_relpaths {
        corpus_hashes.push(structural_hash_from_source_relpath(source_relpath)?.to_string());
    }
    corpus_hashes.sort();
    if corpus_hashes.windows(2).any(|pair| pair[0] == pair[1]) {
        bail!("scheduling-policy corpus contains duplicate structural hashes");
    }
    let corpus_hash_manifest = corpus_hashes
        .iter()
        .map(|hash| format!("{hash}\n"))
        .collect::<String>();
    let actual_corpus_hash_manifest_sha256 =
        hex::encode(Sha256::digest(corpus_hash_manifest.as_bytes()));
    if actual_corpus_hash_manifest_sha256 != policy.expected_corpus_hash_manifest_sha256 {
        bail!(
            "scheduling policy {:?} corpus hash manifest mismatch: expected {}, got {}",
            policy.policy_name,
            policy.expected_corpus_hash_manifest_sha256,
            actual_corpus_hash_manifest_sha256
        );
    }

    if policy.priority_tiers.is_empty() {
        bail!("scheduling_policy.priority_tiers must not be empty");
    }
    let corpus_hash_set: BTreeSet<_> = corpus_hashes.into_iter().collect();
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

        let mut previous_hash: Option<&str> = None;
        for structural_hash in &tier.structural_hashes {
            if !is_lower_hex_sha256(structural_hash) {
                bail!(
                    "scheduling policy priority tier {:?} contains invalid structural hash {:?}",
                    tier.tier_name,
                    structural_hash
                );
            }
            if previous_hash.is_some_and(|previous| previous >= structural_hash.as_str()) {
                bail!(
                    "scheduling policy priority tier {:?} structural hashes must be sorted and unique",
                    tier.tier_name
                );
            }
            previous_hash = Some(structural_hash);
            if !corpus_hash_set.contains(structural_hash) {
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
        }
        priority_tiers.push(CorpusPriorityTierRecord {
            tier_name: tier.tier_name,
            queue_priority_boost: tier.queue_priority_boost,
            structural_hashes: tier.structural_hashes,
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
            expected_corpus_hash_manifest_sha256: policy.expected_corpus_hash_manifest_sha256,
            priority_tiers,
        },
        priority_boost_by_structural_hash,
    }))
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

    fn canonical_source_relpaths() -> Vec<String> {
        include_str!("site_assets/release_progression_ir_hashes.txt")
            .lines()
            .map(|hash| format!("{hash}.ir"))
            .collect()
    }

    #[test]
    fn release_progression_policy_matches_pinned_corpus() {
        let policy = resolve(
            Some(CorpusSchedulingPolicyPreset::ReleaseProgressionIrV1),
            &canonical_source_relpaths(),
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
            &["0".repeat(64) + ".ir"],
        )
        .expect_err("different corpus must be rejected");

        assert!(error.to_string().contains("expects 187 corpus samples"));
    }
}
