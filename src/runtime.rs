// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, anyhow, bail};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use std::thread;

use crate::model::{DriverRuntimeSpec, YosysRuntimeSpec};

pub(crate) fn default_driver_image(driver_version: &str) -> String {
    let mut tag = driver_version.replace(
        |c: char| !c.is_ascii_alphanumeric() && c != '.' && c != '_' && c != '-',
        "-",
    );
    if tag.is_empty() {
        tag = "unknown".to_string();
    }
    format!("{}:{}", crate::DEFAULT_DOCKER_IMAGE_PREFIX, tag)
}

fn sha256_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub(crate) fn runtime_dockerfile_sha256(repo_root: &Path, dockerfile: &str) -> Result<String> {
    let path = Path::new(dockerfile);
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        repo_root.join(path)
    };
    let bytes = fs::read(&path)
        .with_context(|| format!("reading runtime Dockerfile: {}", path.display()))?;
    Ok(sha256_bytes(&bytes))
}

pub(crate) fn default_yosys_runtime(repo_root: &Path) -> Result<YosysRuntimeSpec> {
    crate::service::bind_yosys_runtime_image(
        repo_root,
        YosysRuntimeSpec {
            docker_image: crate::DEFAULT_YOSYS_DOCKER_IMAGE.to_string(),
            dockerfile: crate::DEFAULT_YOSYS_DOCKERFILE.to_string(),
            dockerfile_sha256: sha256_bytes(include_bytes!("../docker/yosys-abc.Dockerfile")),
            docker_image_id: String::new(),
            upstream_commit: Some(crate::DEFAULT_YOSYS_UPSTREAM_COMMIT.to_string()),
        },
    )
}

#[cfg(test)]
pub(crate) fn test_yosys_runtime() -> YosysRuntimeSpec {
    YosysRuntimeSpec {
        docker_image: crate::DEFAULT_YOSYS_DOCKER_IMAGE.to_string(),
        dockerfile: crate::DEFAULT_YOSYS_DOCKERFILE.to_string(),
        dockerfile_sha256: sha256_bytes(include_bytes!("../docker/yosys-abc.Dockerfile")),
        docker_image_id: "e".repeat(64),
        upstream_commit: Some(crate::DEFAULT_YOSYS_UPSTREAM_COMMIT.to_string()),
    }
}

pub(crate) fn default_web_runner_workers() -> usize {
    let cpu_count = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    cpu_count
        .saturating_sub(1)
        .clamp(1, crate::DEFAULT_WEB_RUNNER_MAX_WORKERS)
}

pub(crate) fn default_structural_index_threads() -> usize {
    let cpu_count = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    cpu_count
        .saturating_sub(1)
        .clamp(1, crate::DEFAULT_STRUCTURAL_INDEX_MAX_THREADS)
}

pub(crate) fn resolve_driver_runtime_for_aig_stats(
    repo_root: &Path,
    source_runtime: &DriverRuntimeSpec,
) -> Result<DriverRuntimeSpec> {
    let source_driver_version =
        crate::versioning::normalize_tag_version(&source_runtime.driver_version);
    let latest_driver_version = crate::versioning::latest_known_driver_version(repo_root)?;
    if crate::versioning::cmp_dotted_numeric_version(&latest_driver_version, source_driver_version)
        != std::cmp::Ordering::Greater
    {
        return Ok(source_runtime.clone());
    }

    bound_driver_runtime_for_driver_version(
        repo_root,
        &latest_driver_version,
        &source_runtime.release_platform,
    )
}

pub(crate) fn resolve_driver_runtime_for_dslx_list_fns(
    repo_root: &Path,
    source_runtime: &DriverRuntimeSpec,
) -> Result<DriverRuntimeSpec> {
    let source_driver_version =
        crate::versioning::normalize_tag_version(&source_runtime.driver_version);
    if crate::versioning::cmp_dotted_numeric_version(
        source_driver_version,
        crate::DSLX_LIST_FNS_LEGACY_SOURCE_DRIVER_MAX,
    ) == std::cmp::Ordering::Greater
    {
        return Ok(source_runtime.clone());
    }

    let latest_driver_version = crate::versioning::latest_known_driver_version(repo_root)?;
    if crate::versioning::cmp_dotted_numeric_version(&latest_driver_version, source_driver_version)
        != std::cmp::Ordering::Greater
    {
        return Ok(source_runtime.clone());
    }

    bound_driver_runtime_for_driver_version(
        repo_root,
        &latest_driver_version,
        &source_runtime.release_platform,
    )
}

fn bound_driver_runtime_for_driver_version(
    repo_root: &Path,
    driver_version: &str,
    release_platform: &str,
) -> Result<DriverRuntimeSpec> {
    let xlsynth_version =
        crate::versioning::resolve_xlsynth_version_for_driver(repo_root, driver_version)?;
    crate::service::bind_driver_runtime_image(
        repo_root,
        DriverRuntimeSpec {
            driver_version: driver_version.to_string(),
            release_platform: release_platform.to_string(),
            docker_image: default_driver_image(driver_version),
            dockerfile: crate::DEFAULT_DOCKERFILE.to_string(),
            dockerfile_sha256: runtime_dockerfile_sha256(repo_root, crate::DEFAULT_DOCKERFILE)?,
            docker_image_id: String::new(),
            release_cache_input_sha256: crate::service::driver_release_cache_input_sha256(
                repo_root,
                &xlsynth_version,
                release_platform,
            )?,
        },
    )
}

pub(crate) fn canonical_stdlib_discovery_runtime_for_version(
    repo_root: &Path,
    requested_xlsynth_version: &str,
) -> Result<DriverRuntimeSpec> {
    match default_driver_runtime_for_version(repo_root, requested_xlsynth_version) {
        Ok(runtime) => Ok(runtime),
        Err(_) => {
            // Stdlib extraction still works without a direct compat entry; keep
            // discovery/runtime explicit by using the latest known driver runtime.
            let latest_driver_version = crate::versioning::latest_known_driver_version(repo_root)?;
            bound_driver_runtime_for_driver_version(
                repo_root,
                &latest_driver_version,
                crate::DEFAULT_RELEASE_PLATFORM,
            )
        }
    }
}

pub(crate) fn resolve_driver_version(
    repo_root: &Path,
    requested_driver_version: Option<&str>,
    requested_xlsynth_version: &str,
) -> Result<String> {
    let compat = crate::versioning::load_version_compat_map(repo_root)?;
    let requested_xlsynth = crate::versioning::normalize_tag_version(requested_xlsynth_version);

    let mut compatible_versions: Vec<String> = compat
        .iter()
        .filter_map(|(driver_version, entry)| {
            if crate::versioning::normalize_tag_version(&entry.xlsynth_release_version)
                == requested_xlsynth
            {
                Some(driver_version.clone())
            } else {
                None
            }
        })
        .collect();
    compatible_versions
        .sort_by(|a, b| crate::versioning::cmp_dotted_numeric_version(a, b).reverse());

    let Some(first_compatible) = compatible_versions.first() else {
        let newest_mapped_xlsynth = compat
            .values()
            .map(|v| v.xlsynth_release_version.as_str())
            .max_by(|a, b| crate::versioning::cmp_dotted_numeric_version(a, b))
            .unwrap_or("unknown");
        bail!(
            "no compatible driver crate version is known for {}; newest mapped xlsynth version in {} is v{} (update the map out of band with `scripts/sync-version-compat.sh` and redeploy if needed)",
            requested_xlsynth_version,
            crate::VERSION_COMPAT_PATH,
            newest_mapped_xlsynth
        );
    };

    match requested_driver_version {
        None => Ok(first_compatible.clone()),
        Some("latest") => Ok(first_compatible.clone()),
        Some(explicit) => Ok(crate::versioning::normalize_tag_version(explicit).to_string()),
    }
}

pub(crate) fn default_driver_runtime_for_version(
    repo_root: &Path,
    requested_xlsynth_version: &str,
) -> Result<DriverRuntimeSpec> {
    let driver_version = resolve_driver_version(repo_root, None, requested_xlsynth_version)?;
    let runtime = crate::service::bind_driver_runtime_image(
        repo_root,
        DriverRuntimeSpec {
            driver_version: driver_version.clone(),
            release_platform: crate::DEFAULT_RELEASE_PLATFORM.to_string(),
            docker_image: default_driver_image(&driver_version),
            dockerfile: crate::DEFAULT_DOCKERFILE.to_string(),
            dockerfile_sha256: runtime_dockerfile_sha256(repo_root, crate::DEFAULT_DOCKERFILE)?,
            docker_image_id: String::new(),
            release_cache_input_sha256: crate::service::driver_release_cache_input_sha256(
                repo_root,
                requested_xlsynth_version,
                crate::DEFAULT_RELEASE_PLATFORM,
            )?,
        },
    )?;
    ensure_driver_runtime_compatibility(repo_root, &runtime, requested_xlsynth_version)?;
    Ok(runtime)
}

pub(crate) fn explicit_driver_runtime_for_crate_version(
    repo_root: &Path,
    crate_version: &str,
    requested_xlsynth_version: &str,
) -> Result<DriverRuntimeSpec> {
    let mut runtime = explicit_driver_runtime_recipe_for_crate_version(
        repo_root,
        crate_version,
        requested_xlsynth_version,
    )?;
    runtime.release_cache_input_sha256 = crate::service::driver_release_cache_input_sha256(
        repo_root,
        requested_xlsynth_version,
        &runtime.release_platform,
    )?;
    crate::service::bind_driver_runtime_image(repo_root, runtime)
}

pub(crate) fn explicit_driver_runtime_recipe_for_crate_version(
    repo_root: &Path,
    crate_version: &str,
    requested_xlsynth_version: &str,
) -> Result<DriverRuntimeSpec> {
    let driver_version = crate::versioning::normalize_tag_version(crate_version).to_string();
    let runtime = DriverRuntimeSpec {
        driver_version: driver_version.clone(),
        release_platform: crate::DEFAULT_RELEASE_PLATFORM.to_string(),
        docker_image: default_driver_image(&driver_version),
        dockerfile: crate::DEFAULT_DOCKERFILE.to_string(),
        dockerfile_sha256: runtime_dockerfile_sha256(repo_root, crate::DEFAULT_DOCKERFILE)?,
        docker_image_id: String::new(),
        release_cache_input_sha256: String::new(),
    };
    ensure_driver_runtime_compatibility(repo_root, &runtime, requested_xlsynth_version)?;
    Ok(runtime)
}

pub(crate) fn ensure_driver_runtime_compatibility(
    repo_root: &Path,
    runtime: &DriverRuntimeSpec,
    requested_xlsynth_version: &str,
) -> Result<()> {
    let compat = crate::versioning::load_version_compat_map(repo_root)?;
    let requested_xlsynth = crate::versioning::normalize_tag_version(requested_xlsynth_version);

    if runtime.driver_version == "latest" {
        let latest_known = compat
            .keys()
            .max_by(|a, b| crate::versioning::cmp_dotted_numeric_version(a, b));
        let Some(latest_known) = latest_known else {
            bail!(
                "version compatibility map is empty at {}",
                crate::VERSION_COMPAT_PATH
            );
        };
        let latest_entry = compat
            .get(latest_known.as_str())
            .ok_or_else(|| anyhow!("failed to resolve latest compatibility entry"))?;
        if crate::versioning::normalize_tag_version(&latest_entry.xlsynth_release_version)
            != requested_xlsynth
        {
            bail!(
                "driver version `latest` currently maps to xlsynth v{}, but action requested {}; pass --driver-version <crate-version> compatible with {}",
                latest_entry.xlsynth_release_version,
                requested_xlsynth_version,
                requested_xlsynth_version
            );
        }
        return Ok(());
    }

    let driver_version = crate::versioning::normalize_tag_version(&runtime.driver_version);
    let entry = compat.get(driver_version).ok_or_else(|| {
        anyhow!(
            "driver crate version `{}` was not found in deployed compatibility map {}; update it out of band with `scripts/sync-version-compat.sh` and redeploy, or choose a known version",
            runtime.driver_version,
            crate::VERSION_COMPAT_PATH
        )
    })?;

    if crate::versioning::normalize_tag_version(&entry.xlsynth_release_version) == requested_xlsynth
    {
        return Ok(());
    }

    let mut matching_driver_versions: Vec<String> = compat
        .iter()
        .filter_map(|(driver, compat)| {
            if crate::versioning::normalize_tag_version(&compat.xlsynth_release_version)
                == requested_xlsynth
            {
                Some(driver.clone())
            } else {
                None
            }
        })
        .collect();
    matching_driver_versions
        .sort_by(|a, b| crate::versioning::cmp_dotted_numeric_version(a, b).reverse());

    if matching_driver_versions.is_empty() {
        let newest_mapped_xlsynth = compat
            .values()
            .map(|v| v.xlsynth_release_version.as_str())
            .max_by(|a, b| crate::versioning::cmp_dotted_numeric_version(a, b))
            .unwrap_or("unknown");
        bail!(
            "driver crate version `{}` is mapped to xlsynth v{}, but action requested {}; no compatible crate version is known in {} (newest mapped xlsynth version is v{})",
            runtime.driver_version,
            entry.xlsynth_release_version,
            requested_xlsynth_version,
            crate::VERSION_COMPAT_PATH,
            newest_mapped_xlsynth
        );
    }

    bail!(
        "driver crate version `{}` is mapped to xlsynth v{}, but action requested {}; compatible driver crate version(s): {}",
        runtime.driver_version,
        entry.xlsynth_release_version,
        requested_xlsynth_version,
        matching_driver_versions.join(", ")
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oldest_and_latest_driver_versions(repo_root: &Path) -> (String, String) {
        let compat = crate::versioning::load_version_compat_map(repo_root).expect("compat map");
        let oldest = compat
            .keys()
            .min_by(|a, b| crate::versioning::cmp_dotted_numeric_version(a, b))
            .expect("oldest driver")
            .clone();
        let latest = compat
            .keys()
            .max_by(|a, b| crate::versioning::cmp_dotted_numeric_version(a, b))
            .expect("latest driver")
            .clone();
        assert_ne!(oldest, latest, "fixture needs multiple driver generations");
        (oldest, latest)
    }

    fn source_runtime(repo_root: &Path, driver_version: &str) -> DriverRuntimeSpec {
        let dso = crate::versioning::resolve_xlsynth_version_for_driver(repo_root, driver_version)
            .expect("source DSO");
        DriverRuntimeSpec {
            driver_version: driver_version.to_string(),
            release_platform: crate::DEFAULT_RELEASE_PLATFORM.to_string(),
            docker_image: default_driver_image(driver_version),
            dockerfile: "old/deployment.Dockerfile".to_string(),
            dockerfile_sha256: "a".repeat(64),
            docker_image_id: "b".repeat(64),
            release_cache_input_sha256: crate::service::driver_release_cache_input_sha256(
                repo_root,
                &dso,
                crate::DEFAULT_RELEASE_PLATFORM,
            )
            .expect("source cache digest"),
        }
    }

    fn assert_substitute_runtime_is_self_consistent(
        repo_root: &Path,
        source: &DriverRuntimeSpec,
        resolved: &DriverRuntimeSpec,
        latest: &str,
    ) {
        assert_eq!(resolved.driver_version, latest);
        assert_eq!(resolved.dockerfile, crate::DEFAULT_DOCKERFILE);
        assert_ne!(resolved.dockerfile_sha256, source.dockerfile_sha256);
        let resolved_dso = crate::versioning::resolve_xlsynth_version_for_driver(
            repo_root,
            &resolved.driver_version,
        )
        .expect("resolved DSO");
        let expected_digest = crate::service::driver_release_cache_input_sha256(
            repo_root,
            &resolved_dso,
            &resolved.release_platform,
        )
        .expect("resolved cache digest");
        assert_eq!(resolved.release_cache_input_sha256, expected_digest);
        assert_ne!(
            resolved.release_cache_input_sha256,
            source.release_cache_input_sha256
        );
    }

    #[test]
    fn substituted_driver_runtimes_rebind_recipe_and_release_cache_identity() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let (oldest, latest) = oldest_and_latest_driver_versions(repo_root);
        let source = source_runtime(repo_root, &oldest);

        let stats = resolve_driver_runtime_for_aig_stats(repo_root, &source)
            .expect("substitute AIG stats runtime");
        assert_substitute_runtime_is_self_consistent(repo_root, &source, &stats, &latest);

        let list_fns = resolve_driver_runtime_for_dslx_list_fns(repo_root, &source)
            .expect("substitute list-fns runtime");
        assert_substitute_runtime_is_self_consistent(repo_root, &source, &list_fns, &latest);
    }

    #[test]
    fn unknown_stdlib_discovery_version_uses_latest_drivers_own_cache_identity() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let runtime = canonical_stdlib_discovery_runtime_for_version(repo_root, "v999.0.0")
            .expect("fallback discovery runtime");
        let runtime_dso = crate::versioning::resolve_xlsynth_version_for_driver(
            repo_root,
            &runtime.driver_version,
        )
        .expect("runtime DSO");
        let expected_digest = crate::service::driver_release_cache_input_sha256(
            repo_root,
            &runtime_dso,
            &runtime.release_platform,
        )
        .expect("runtime cache digest");
        assert_eq!(runtime.release_cache_input_sha256, expected_digest);
    }
}
