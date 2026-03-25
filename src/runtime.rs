// SPDX-License-Identifier: Apache-2.0

use anyhow::{Result, anyhow, bail};
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

pub(crate) fn default_yosys_runtime() -> YosysRuntimeSpec {
    YosysRuntimeSpec {
        docker_image: crate::DEFAULT_YOSYS_DOCKER_IMAGE.to_string(),
        dockerfile: crate::DEFAULT_YOSYS_DOCKERFILE.to_string(),
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

    Ok(DriverRuntimeSpec {
        driver_version: latest_driver_version.clone(),
        release_platform: source_runtime.release_platform.clone(),
        docker_image: default_driver_image(&latest_driver_version),
        dockerfile: source_runtime.dockerfile.clone(),
    })
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

    Ok(DriverRuntimeSpec {
        driver_version: latest_driver_version.clone(),
        release_platform: source_runtime.release_platform.clone(),
        docker_image: default_driver_image(&latest_driver_version),
        dockerfile: source_runtime.dockerfile.clone(),
    })
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
            Ok(DriverRuntimeSpec {
                driver_version: latest_driver_version.clone(),
                release_platform: crate::DEFAULT_RELEASE_PLATFORM.to_string(),
                docker_image: default_driver_image(&latest_driver_version),
                dockerfile: crate::DEFAULT_DOCKERFILE.to_string(),
            })
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
            "no compatible driver crate version is known for {}; newest mapped xlsynth version in {} is v{} (run `refresh-version-compat` if needed)",
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
    let runtime = DriverRuntimeSpec {
        driver_version: driver_version.clone(),
        release_platform: crate::DEFAULT_RELEASE_PLATFORM.to_string(),
        docker_image: default_driver_image(&driver_version),
        dockerfile: crate::DEFAULT_DOCKERFILE.to_string(),
    };
    ensure_driver_runtime_compatibility(repo_root, &runtime, requested_xlsynth_version)?;
    Ok(runtime)
}

pub(crate) fn explicit_driver_runtime_for_crate_version(
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
            "driver crate version `{}` was not found in compatibility map {}; run `refresh-version-compat` or choose a known version",
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
