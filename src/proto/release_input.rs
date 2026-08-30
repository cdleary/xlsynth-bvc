// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::sync::OnceLock;

use anyhow::{Context, Result, anyhow, bail};
use prost::Message;

use crate::proto::{DEFAULT_RELEASE_INPUTS, v1 as pb};
use crate::versioning::{cmp_dotted_numeric_version, normalize_tag_version};

const RELEASE_INPUT_LOCK_RECORD_VERSION: u32 = 1;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ReleaseInput {
    pub(crate) stdlib_tarball_sha256: String,
    pub(crate) source_commit: String,
}

fn decode_lock() -> Result<BTreeMap<String, ReleaseInput>> {
    let lock = pb::ReleaseInputLock::decode(DEFAULT_RELEASE_INPUTS)
        .context("decoding compiled release-input lock")?;
    if lock.record_version != RELEASE_INPUT_LOCK_RECORD_VERSION {
        bail!(
            "unsupported release-input lock record version {}; expected {}",
            lock.record_version,
            RELEASE_INPUT_LOCK_RECORD_VERSION
        );
    }
    if lock.releases.is_empty() {
        bail!("compiled release-input lock is empty");
    }

    let mut result = BTreeMap::new();
    let mut previous: Option<String> = None;
    for entry in lock.releases {
        let version = entry
            .dso_version
            .context("release input missing dso_version")?
            .value;
        if version.is_empty() || version != normalize_tag_version(&version) {
            bail!("release input has non-canonical DSO version {version:?}");
        }
        if previous.as_deref().is_some_and(|value| {
            cmp_dotted_numeric_version(value, &version) != std::cmp::Ordering::Less
        }) {
            bail!("release inputs must be strictly sorted by DSO version");
        }
        previous = Some(version.clone());

        let digest = entry
            .stdlib_tarball_sha256
            .context("release input missing stdlib_tarball_sha256")?;
        if digest.value.len() != 32 {
            bail!(
                "release input for v{version} has {}-byte stdlib SHA-256",
                digest.value.len()
            );
        }
        let source_commit = entry.source_commit;
        if source_commit.len() != 40
            || !source_commit.bytes().all(|byte| byte.is_ascii_hexdigit())
            || source_commit.to_ascii_lowercase() != source_commit
        {
            bail!("release input for v{version} has invalid source commit");
        }
        let input = ReleaseInput {
            stdlib_tarball_sha256: hex::encode(digest.value),
            source_commit,
        };
        if result.insert(version.clone(), input).is_some() {
            bail!("duplicate release input for v{version}");
        }
    }
    Ok(result)
}

pub(crate) fn release_input_for_dso_version(version: &str) -> Result<ReleaseInput> {
    static INPUTS: OnceLock<Result<BTreeMap<String, ReleaseInput>, String>> = OnceLock::new();
    let inputs = INPUTS.get_or_init(|| decode_lock().map_err(|error| format!("{error:#}")));
    let inputs = inputs.as_ref().map_err(|error| anyhow!(error.clone()))?;
    let normalized = normalize_tag_version(version);
    inputs.get(normalized).cloned().ok_or_else(|| {
        anyhow!(
            "DSO version v{} is not in the compiled release-input lock; refresh it out of band and redeploy",
            normalized
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiled_release_input_lock_is_valid_and_queryable() {
        let inputs = decode_lock().expect("valid release-input lock");
        assert!(inputs.len() >= 70);
        let latest = release_input_for_dso_version("v0.45.0").expect("known release");
        assert_eq!(latest.stdlib_tarball_sha256.len(), 64);
        assert_eq!(latest.source_commit.len(), 40);
    }

    #[test]
    fn unknown_release_requires_out_of_band_refresh() {
        let error = release_input_for_dso_version("v999.0.0").expect_err("unknown release");
        assert!(error.to_string().contains("refresh it out of band"));
    }
}
