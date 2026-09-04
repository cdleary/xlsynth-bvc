// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone, Utc};
use reqwest::blocking::Client;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use crate::model::{
    ActionSpec, DiscoverReleasesSummary, DiscoveredRelease, GithubRelease, ReleaseTag,
    VersionCompatEntry,
};
use crate::store::ArtifactStore;
use crate::view::RepositoryHeadObservationView;

pub(crate) fn normalize_tag_version(version: &str) -> &str {
    version.strip_prefix('v').unwrap_or(version)
}

pub(crate) fn xlsynth_release_tag(version: &str) -> String {
    format!("v{}", normalize_tag_version(version))
}

pub(crate) fn version_label(kind: &str, version: &str) -> String {
    format!("{kind}:{}", xlsynth_release_tag(version))
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

pub(crate) fn repository_comparison_status(
    commits_ahead: u64,
    commits_behind: u64,
) -> &'static str {
    match (commits_ahead > 0, commits_behind > 0) {
        (true, true) => "diverged",
        (true, false) => "ahead",
        (false, true) => "behind",
        (false, false) => "identical",
    }
}

fn parse_repository_utc_timestamp(value: &str) -> Option<DateTime<Utc>> {
    if value.as_bytes().get(10) != Some(&b'T') || !value.ends_with('Z') {
        return None;
    }
    DateTime::parse_from_rfc3339(value)
        .ok()
        .filter(|timestamp| timestamp.offset().local_minus_utc() == 0)
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

pub(crate) fn validate_repository_head_observation(
    observation: &RepositoryHeadObservationView,
) -> Result<()> {
    if observation.schema_version != 2
        || observation.repository != "xlsynth/xlsynth-crate"
        || observation.head_ref != "main"
    {
        bail!("repository observation identity is invalid");
    }
    if observation.version_compat_sha256.len() != 64
        || !observation
            .version_compat_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("repository observation version_compat_sha256 is not a lowercase SHA-256 digest");
    }
    for (label, commit) in [
        ("head_commit", observation.head_commit.as_str()),
        (
            "latest_release_commit",
            observation.latest_release_commit.as_str(),
        ),
    ] {
        if commit.len() != 40
            || !commit
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            bail!("repository observation {label} is not a lowercase full commit");
        }
    }
    let Some(observed_at) = parse_repository_utc_timestamp(&observation.observed_at_utc) else {
        bail!("repository observation observed_at_utc is not an RFC 3339 UTC date-time");
    };
    let Some(head_committed_at) =
        parse_repository_utc_timestamp(&observation.head_committed_at_utc)
    else {
        bail!("repository observation head_committed_at_utc is not an RFC 3339 UTC date-time");
    };
    let Some(release_committed_at) =
        parse_repository_utc_timestamp(&observation.latest_release_committed_at_utc)
    else {
        bail!(
            "repository observation latest_release_committed_at_utc is not an RFC 3339 UTC date-time"
        );
    };
    if observed_at < head_committed_at || observed_at < release_committed_at {
        bail!("repository observation time predates an observed commit");
    }
    if observation.latest_release_tag
        != format!(
            "v{}",
            normalize_tag_version(&observation.latest_crate_version)
        )
    {
        bail!("repository observation latest release tag/version disagree");
    }
    let expected_status =
        repository_comparison_status(observation.commits_ahead, observation.commits_behind);
    if observation.comparison_status != expected_status {
        bail!(
            "repository observation comparison status is {}; expected {expected_status} from ahead/behind counts",
            observation.comparison_status
        );
    }
    let commits_match = observation.head_commit == observation.latest_release_commit;
    let zero_distance = observation.commits_ahead == 0 && observation.commits_behind == 0;
    if commits_match != zero_distance {
        bail!("repository observation commit equality disagrees with ahead/behind distance");
    }
    Ok(())
}

pub(crate) fn load_version_compat_map(
    repo_root: &Path,
) -> Result<BTreeMap<String, VersionCompatEntry>> {
    let path = repo_root.join(crate::VERSION_COMPAT_PATH);
    let text = fs::read_to_string(&path).with_context(|| {
        format!(
            "reading deployed version compatibility map: {} (update it out of band with `scripts/sync_version_compat.py` and redeploy)",
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

pub(crate) fn load_xlsynth_crate_repository_head_observation(
    repo_root: &Path,
) -> Result<Option<RepositoryHeadObservationView>> {
    let path = repo_root.join(crate::XLSYNTH_CRATE_REPOSITORY_OBSERVATION_PATH);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "reading xlsynth-crate repository observation: {}",
                    path.display()
                )
            });
        }
    };
    let observation: RepositoryHeadObservationView = serde_json::from_str(&text)
        .with_context(|| format!("parsing repository observation JSON at {}", path.display()))?;
    validate_repository_head_observation(&observation)
        .with_context(|| format!("validating repository observation at {}", path.display()))?;
    let compat_path = repo_root.join(crate::VERSION_COMPAT_PATH);
    let compat_bytes = fs::read(&compat_path).with_context(|| {
        format!(
            "reading compatibility map while validating repository observation: {}",
            compat_path.display()
        )
    })?;
    let compat_sha256 = hex::encode(Sha256::digest(&compat_bytes));
    if observation.version_compat_sha256 != compat_sha256 {
        bail!(
            "repository observation belongs to compatibility map {}, but deployed map is {}; rerun scripts/sync_version_compat.py",
            observation.version_compat_sha256,
            compat_sha256
        );
    }
    Ok(Some(observation))
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
            "driver crate version `{}` was not found in deployed compatibility map {}; update it out of band with `scripts/sync_version_compat.py` and redeploy, or choose a known version",
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
            let release_input = crate::proto::release_input_for_dso_version(&release.tag_name)?;
            let action = ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                version: release.tag_name.clone(),
                discovery_runtime: Some(runtime),
                stdlib_tarball_sha256: release_input.stdlib_tarball_sha256,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xlsynth_release_tag_restores_external_tag_spelling() {
        assert_eq!(xlsynth_release_tag("0.45.0"), "v0.45.0");
        assert_eq!(xlsynth_release_tag("v0.45.0"), "v0.45.0");
        assert_eq!(xlsynth_release_tag("0.45.0-1"), "v0.45.0-1");
    }

    #[test]
    fn repository_comparison_status_is_derived_from_both_counts() {
        assert_eq!(repository_comparison_status(0, 0), "identical");
        assert_eq!(repository_comparison_status(1, 0), "ahead");
        assert_eq!(repository_comparison_status(0, 1), "behind");
        assert_eq!(repository_comparison_status(1, 1), "diverged");
        assert_eq!(repository_comparison_status(u64::MAX, 0), "ahead");
        assert_eq!(repository_comparison_status(u64::MAX, u64::MAX), "diverged");
    }

    fn repository_observation() -> RepositoryHeadObservationView {
        RepositoryHeadObservationView {
            schema_version: 2,
            repository: "xlsynth/xlsynth-crate".to_string(),
            version_compat_sha256: "c".repeat(64),
            observed_at_utc: "2026-09-03T22:34:04Z".to_string(),
            head_ref: "main".to_string(),
            head_commit: "a".repeat(40),
            head_committed_at_utc: "2026-09-03T17:53:02Z".to_string(),
            latest_crate_version: "0.67.1".to_string(),
            latest_release_tag: "v0.67.1".to_string(),
            latest_release_commit: "b".repeat(40),
            latest_release_committed_at_utc: "2026-09-03T16:00:00Z".to_string(),
            comparison_status: "ahead".to_string(),
            commits_ahead: 2,
            commits_behind: 0,
        }
    }

    #[test]
    fn repository_observation_requires_rfc3339_utc_date_times() {
        let mut observation = repository_observation();
        observation.observed_at_utc = "2026-09-03Z".to_string();
        let error = validate_repository_head_observation(&observation)
            .expect_err("date-only timestamps must be rejected");
        assert!(format!("{error:#}").contains("RFC 3339 UTC date-time"));

        let mut observation = repository_observation();
        observation.observed_at_utc = "2026-09-03T15:00:00Z".to_string();
        let error = validate_repository_head_observation(&observation)
            .expect_err("observation time must follow commit times");
        assert!(format!("{error:#}").contains("predates"));
    }

    #[test]
    fn repository_observation_binds_commit_equality_to_distance() {
        let mut observation = repository_observation();
        observation.comparison_status = "identical".to_string();
        observation.commits_ahead = 0;
        let error = validate_repository_head_observation(&observation)
            .expect_err("distinct commits cannot be identical");
        assert!(format!("{error:#}").contains("commit equality"));

        let mut observation = repository_observation();
        observation.latest_release_commit = observation.head_commit.clone();
        let error = validate_repository_head_observation(&observation)
            .expect_err("equal commits cannot have nonzero distance");
        assert!(format!("{error:#}").contains("commit equality"));
    }
}
