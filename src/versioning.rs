// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone, Utc};
use reqwest::blocking::Client;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::model::{
    ActionSpec, DiscoverReleasesSummary, DiscoveredRelease, GithubRelease,
    RefreshVersionCompatSummary, ReleaseTag, VersionCompatEntry,
};
use crate::store::ArtifactStore;

pub(crate) fn refresh_version_compat_json() -> Result<RefreshVersionCompatSummary> {
    let client = Client::builder()
        .user_agent(format!("xlsynth-bvc/{}", env!("CARGO_PKG_VERSION")))
        .build()
        .context("creating compatibility refresh client")?;
    let body = client
        .get(crate::VERSION_COMPAT_MAIN_URL)
        .send()
        .context("fetching generated_version_compat.json from main")?
        .error_for_status()
        .context("status check for generated_version_compat.json download")?
        .bytes()
        .context("reading generated_version_compat.json response body")?;

    let output_path = PathBuf::from(crate::VERSION_COMPAT_PATH);
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "creating parent directory for version compat file: {}",
                parent.display()
            )
        })?;
    }
    fs::write(&output_path, &body).with_context(|| {
        format!(
            "writing refreshed version compatibility file: {}",
            output_path.display()
        )
    })?;
    let sha256 = {
        let digest = Sha256::digest(&body);
        hex::encode(digest)
    };
    Ok(RefreshVersionCompatSummary {
        output_path: output_path.display().to_string(),
        source_url: crate::VERSION_COMPAT_MAIN_URL.to_string(),
        bytes: body.len(),
        sha256,
    })
}

pub(crate) fn normalize_tag_version(version: &str) -> &str {
    version.strip_prefix('v').unwrap_or(version)
}

pub(crate) fn version_label(kind: &str, version: &str) -> String {
    format!("{kind}:v{}", normalize_tag_version(version))
}

fn parse_dotted_numeric_version(version: &str) -> Option<Vec<u32>> {
    version
        .split('.')
        .map(|part| part.parse::<u32>().ok())
        .collect::<Option<Vec<u32>>>()
}

pub(crate) fn cmp_dotted_numeric_version(a: &str, b: &str) -> std::cmp::Ordering {
    match (
        parse_dotted_numeric_version(a),
        parse_dotted_numeric_version(b),
    ) {
        (Some(mut av), Some(mut bv)) => {
            let len = av.len().max(bv.len());
            av.resize(len, 0);
            bv.resize(len, 0);
            av.cmp(&bv)
        }
        _ => a.cmp(b),
    }
}

fn timezone_abbrev_utc_offset_seconds(abbrev: &str) -> Option<i32> {
    match abbrev {
        "UTC" | "GMT" => Some(0),
        "PST" => Some(-8 * 60 * 60),
        "PDT" => Some(-7 * 60 * 60),
        "MST" => Some(-7 * 60 * 60),
        "MDT" => Some(-6 * 60 * 60),
        "CST" => Some(-6 * 60 * 60),
        "CDT" => Some(-5 * 60 * 60),
        "EST" => Some(-5 * 60 * 60),
        "EDT" => Some(-4 * 60 * 60),
        _ => None,
    }
}

pub(crate) fn parse_compat_release_datetime_utc(value: &str) -> Option<DateTime<Utc>> {
    let (naive_part, tz_part) = value.rsplit_once(' ')?;
    let naive = NaiveDateTime::parse_from_str(naive_part, "%Y-%m-%d %H:%M:%S").ok()?;
    let offset = FixedOffset::east_opt(timezone_abbrev_utc_offset_seconds(tz_part)?)?;
    let dt = offset.from_local_datetime(&naive).single()?;
    Some(dt.with_timezone(&Utc))
}

pub(crate) fn load_crate_release_datetime_utc_map(
    repo_root: &Path,
) -> BTreeMap<String, DateTime<Utc>> {
    let compat = match load_version_compat_map(repo_root) {
        Ok(v) => v,
        Err(_) => return BTreeMap::new(),
    };
    let mut by_crate = BTreeMap::new();
    for (crate_version, entry) in compat {
        if let Some(dt) = parse_compat_release_datetime_utc(&entry.crate_release_datetime) {
            by_crate.insert(crate_version, dt);
        }
    }
    by_crate
}

pub(crate) fn cmp_crate_versions_by_release_datetime(
    crate_a: &str,
    crate_b: &str,
    release_utc_by_crate: &BTreeMap<String, DateTime<Utc>>,
) -> std::cmp::Ordering {
    match (
        release_utc_by_crate.get(crate_a),
        release_utc_by_crate.get(crate_b),
    ) {
        (Some(a_dt), Some(b_dt)) => b_dt
            .cmp(a_dt)
            .then(cmp_dotted_numeric_version(crate_a, crate_b).reverse()),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => cmp_dotted_numeric_version(crate_a, crate_b).reverse(),
    }
}

pub(crate) fn load_version_compat_map(
    repo_root: &Path,
) -> Result<BTreeMap<String, VersionCompatEntry>> {
    let path = repo_root.join(crate::VERSION_COMPAT_PATH);
    let text = fs::read_to_string(&path).with_context(|| {
        format!(
            "reading version compatibility map: {} (run `refresh-version-compat` if missing)",
            path.display()
        )
    })?;
    let map =
        serde_json::from_str::<BTreeMap<String, VersionCompatEntry>>(&text).with_context(|| {
            format!(
                "parsing version compatibility map JSON at {}",
                path.display()
            )
        })?;
    Ok(map)
}

pub(crate) fn latest_known_driver_version(repo_root: &Path) -> Result<String> {
    let compat = load_version_compat_map(repo_root)?;
    compat
        .keys()
        .max_by(|a, b| cmp_dotted_numeric_version(a, b))
        .cloned()
        .ok_or_else(|| {
            anyhow!(
                "version compatibility map is empty at {}",
                crate::VERSION_COMPAT_PATH
            )
        })
}

pub(crate) fn resolve_xlsynth_version_for_driver(
    repo_root: &Path,
    driver_version: &str,
) -> Result<String> {
    let compat = load_version_compat_map(repo_root)?;
    let key = normalize_tag_version(driver_version);
    let entry = compat.get(key).ok_or_else(|| {
        anyhow!(
            "driver crate version `{}` was not found in compatibility map {}; run `refresh-version-compat` or choose a known version",
            driver_version,
            crate::VERSION_COMPAT_PATH
        )
    })?;
    if entry.xlsynth_release_version.starts_with('v') {
        Ok(entry.xlsynth_release_version.clone())
    } else {
        Ok(format!("v{}", entry.xlsynth_release_version))
    }
}

pub(crate) fn discover_releases(
    store: &ArtifactStore,
    repo_root: &Path,
    after: &str,
    max_pages: u32,
    enqueue: bool,
) -> Result<DiscoverReleasesSummary> {
    if max_pages == 0 {
        bail!("--max-pages must be > 0");
    }
    let after_tag = parse_release_tag(after)
        .with_context(|| format!("--after must be in vX.Y.Z or vX.Y.Z-N form, got `{after}`"))?;

    let client = Client::builder()
        .user_agent(format!("xlsynth-bvc/{}", env!("CARGO_PKG_VERSION")))
        .build()
        .context("creating github API client")?;

    let mut inspected = 0_usize;
    let mut considered = 0_usize;
    let mut reached_after = false;
    let mut discovered = Vec::new();

    for page in 1..=max_pages {
        let url = format!("{}?per_page=100&page={page}", crate::GITHUB_RELEASES_API);
        let releases: Vec<GithubRelease> = client
            .get(&url)
            .send()
            .with_context(|| format!("fetching github releases page {}", page))?
            .error_for_status()
            .with_context(|| format!("github status check for {}", url))?
            .json()
            .with_context(|| format!("decoding github releases response for page {}", page))?;
        if releases.is_empty() {
            break;
        }

        for release in releases {
            inspected += 1;
            if release.draft || release.prerelease {
                continue;
            }
            if !release_has_stdlib_assets(&release) {
                continue;
            }

            let parsed = match parse_release_tag(&release.tag_name) {
                Ok(p) => p,
                Err(_) => continue,
            };
            if release.tag_name == after || parsed <= after_tag {
                reached_after = true;
                break;
            }
            considered += 1;

            let runtime = crate::runtime::canonical_stdlib_discovery_runtime_for_version(
                repo_root,
                &release.tag_name,
            )?;
            let action = ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                version: release.tag_name.clone(),
                discovery_runtime: Some(runtime),
            };
            let action_id = crate::executor::compute_action_id(&action)?;
            let was_known = store.action_exists(&action_id)
                || store.pending_queue_path(&action_id).exists()
                || store.running_queue_path(&action_id).exists()
                || store.done_queue_path(&action_id).exists()
                || store.failed_action_record_exists(&action_id)
                || store.canceled_queue_path(&action_id).exists();
            if enqueue {
                crate::queue::enqueue_action(store, action)?;
            }
            discovered.push(DiscoveredRelease {
                version: release.tag_name,
                action_id,
                enqueued: enqueue && !was_known,
            });
        }

        if reached_after {
            break;
        }
    }

    let enqueued_count = discovered.iter().filter(|r| r.enqueued).count();
    Ok(DiscoverReleasesSummary {
        after: after.to_string(),
        max_pages,
        inspected_releases: inspected,
        considered_releases: considered,
        enqueued_count,
        reached_after,
        releases: discovered,
    })
}

fn release_has_stdlib_assets(release: &GithubRelease) -> bool {
    let mut has_tar = false;
    let mut has_sha = false;
    for asset in &release.assets {
        if asset.name == "dslx_stdlib.tar.gz" {
            has_tar = true;
        } else if asset.name == "dslx_stdlib.tar.gz.sha256" {
            has_sha = true;
        }
    }
    has_tar && has_sha
}

pub(crate) fn parse_release_tag(tag: &str) -> Result<ReleaseTag> {
    let stripped = tag
        .strip_prefix('v')
        .ok_or_else(|| anyhow!("release tag must start with `v`: {}", tag))?;
    let (base, patch2) = match stripped.split_once('-') {
        Some((base, patch2)) => {
            let parsed_patch2 = patch2
                .parse::<u32>()
                .with_context(|| format!("patch2 in tag is not numeric: {}", tag))?;
            (base, parsed_patch2)
        }
        None => (stripped, 0),
    };
    let mut parts = base.split('.');
    let major = parts
        .next()
        .ok_or_else(|| anyhow!("missing major in tag: {}", tag))?
        .parse::<u32>()
        .with_context(|| format!("major in tag is not numeric: {}", tag))?;
    let minor = parts
        .next()
        .ok_or_else(|| anyhow!("missing minor in tag: {}", tag))?
        .parse::<u32>()
        .with_context(|| format!("minor in tag is not numeric: {}", tag))?;
    let patch = parts
        .next()
        .ok_or_else(|| anyhow!("missing patch in tag: {}", tag))?
        .parse::<u32>()
        .with_context(|| format!("patch in tag is not numeric: {}", tag))?;
    if parts.next().is_some() {
        bail!("too many components in release tag: {}", tag);
    }
    Ok(ReleaseTag {
        major,
        minor,
        patch,
        patch2,
    })
}
