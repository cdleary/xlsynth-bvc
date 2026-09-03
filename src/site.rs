// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use prost::Message;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

use crate::analysis::decode_analysis_report;
use crate::proto::v1 as pb;
use crate::snapshot::{
    load_static_snapshot_manifest, should_include_snapshot_index_key, verify_static_snapshot,
};
use crate::versioning::cmp_dotted_numeric_version;
use crate::view::{StdlibEnumerationState, StdlibG8rVsYosysDataset, VersionCardsReport};

pub(crate) const STATIC_SITE_RECORD_VERSION: u32 = 1;
pub(crate) const STATIC_SITE_MANIFEST_FILENAME: &str = "site_manifest.v1.pb";
const PLOTLY_ASSET_NAME: &str = "plotly-2.35.2.min.js";
const PLOTLY_JS: &[u8] = include_bytes!("../third_party/plotly/plotly-2.35.2.min.js");
const PLOTLY_LICENSE_ASSET_NAME: &str = "plotly-2.35.2.LICENSE.txt";
const PLOTLY_LICENSE: &[u8] = include_bytes!("../third_party/plotly/LICENSE");
const PLOTLY_NOTICE_ASSET_NAME: &str = "plotly-2.35.2.min.js.LICENSE.txt";
const PLOTLY_NOTICE: &[u8] =
    include_bytes!("../third_party/plotly/plotly-2.35.2.min.js.LICENSE.txt");

const STYLE_CSS: &str = include_str!("site_assets/style.css");
const APP_JS: &str = include_str!("site_assets/app.js");
const BROWSER_CATALOG_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone)]
pub(crate) struct BuildStaticSiteOptions {
    pub(crate) snapshot_dir: PathBuf,
    pub(crate) out_dir: PathBuf,
    pub(crate) base_url: String,
    pub(crate) overwrite: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BuildStaticSiteSummary {
    pub(crate) out_dir: String,
    pub(crate) snapshot_id: String,
    pub(crate) base_url: String,
    pub(crate) dataset_count: usize,
    pub(crate) file_count: usize,
    pub(crate) total_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VerifyStaticSiteSummary {
    pub(crate) site_dir: String,
    pub(crate) snapshot_id: String,
    pub(crate) base_url: String,
    pub(crate) file_count: usize,
    pub(crate) total_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SmokeStaticSiteSummary {
    pub(crate) site_dir: String,
    pub(crate) base_url: String,
    pub(crate) browser: String,
    pub(crate) pages_checked: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrowserCatalog {
    schema_version: u32,
    snapshot_id: String,
    base_url: String,
    datasets: Vec<BrowserDataset>,
    runs: Vec<BrowserRun>,
    progression: BrowserProgressionCatalog,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrowserDataset {
    logical_key: String,
    url: String,
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrowserRun {
    campaign_id: String,
    run_id: String,
    campaign_name: String,
    campaign_semantic_version: u32,
    crate_version: String,
    dso_version: String,
    status: String,
    updated_utc: String,
    root_action_ids: Vec<String>,
    completed_root_count: u64,
    failed_count: u64,
    canceled_count: u64,
    missing_output_count: u64,
    failed_sample_count: u64,
    intentionally_skipped_samples: Vec<BrowserIntentionalSkip>,
    protobuf_url: String,
    page_url: String,
    findings_protobuf_url: Option<String>,
    findings: Vec<BrowserFinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrowserIntentionalSkip {
    action_id: String,
    rule_id: String,
    reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrowserFinding {
    finding_id: String,
    kind: String,
    subject_key: String,
    metric_name: String,
    baseline_value: Option<f64>,
    current_value: Option<f64>,
    unit: String,
    structural_hash: Option<String>,
    evidence_action_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct BrowserProgressionCatalog {
    dataset_key: String,
    cohort_subject_count: u64,
    cohort_subject_sha256: Option<String>,
    cohort_complete_generation_count: u64,
    generations: Vec<BrowserProgressionGeneration>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum BrowserProgressionCoverage {
    CohortComplete,
    Partial,
    Incompatible,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct BrowserProgressionRunRef {
    run_id: String,
    status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct BrowserProgressionGeneration {
    generation_id: String,
    crate_version: String,
    dso_version: String,
    stdlib_root_action_id: String,
    coverage: BrowserProgressionCoverage,
    observed_subject_count: u64,
    cohort_subject_count: u64,
    missing_cohort_subject_count: u64,
    extra_subject_count: u64,
    enumeration_status: Option<String>,
    enumerated_subject_count: Option<u64>,
    unmeasured_enumerated_subject_count: Option<u64>,
    campaign_runs: Vec<BrowserProgressionRunRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProgressionComparisonIndexFile {
    schema_version: u32,
    generated_utc: chrono::DateTime<chrono::Utc>,
    dataset: StdlibG8rVsYosysDataset,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProgressionVersionsIndexFile {
    schema_version: u32,
    generated_utc: chrono::DateTime<chrono::Utc>,
    report: VersionCardsReport,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
struct ProgressionSubject {
    fn_key: String,
    ir_top: Option<String>,
}

#[derive(Debug)]
struct ProgressionGenerationSource {
    crate_version: String,
    dso_version: String,
    stdlib_root_action_id: String,
    subjects: Vec<ProgressionSubject>,
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn encode_browser_catalog(catalog: &BrowserCatalog) -> Result<Vec<u8>> {
    serde_json::to_vec_pretty(catalog).context("serializing canonical browser catalog")
}

fn decode_canonical_browser_catalog(bytes: &[u8]) -> Result<BrowserCatalog> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).context("decoding browser catalog JSON value")?;
    let catalog: BrowserCatalog =
        serde_json::from_value(value.clone()).context("decoding typed browser catalog")?;
    let projected =
        serde_json::to_value(&catalog).context("projecting typed browser catalog to JSON")?;
    if value != projected {
        bail!("browser catalog contains values outside its typed public schema");
    }
    if encode_browser_catalog(&catalog)? != bytes {
        bail!("browser catalog is not canonically encoded");
    }
    Ok(catalog)
}

fn progression_subjects_sha256(subjects: &[ProgressionSubject]) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"xlsynth-bvc/release-progression-cohort/v1\0");
    hasher.update(
        serde_json::to_vec(subjects).context("encoding release progression cohort identity")?,
    );
    Ok(hex::encode(hasher.finalize()))
}

fn progression_generation_id(
    crate_version: &str,
    dso_version: &str,
    stdlib_root_action_id: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"xlsynth-bvc/release-progression-generation/v1\0");
    for value in [crate_version, dso_version, stdlib_root_action_id] {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    hex::encode(hasher.finalize())
}

fn enumeration_state_label(state: StdlibEnumerationState) -> &'static str {
    match state {
        StdlibEnumerationState::Unknown => "unknown",
        StdlibEnumerationState::Missing => "missing",
        StdlibEnumerationState::Failed => "failed",
        StdlibEnumerationState::Partial => "partial",
        StdlibEnumerationState::Ok => "ok",
    }
}

fn empty_browser_progression_catalog() -> BrowserProgressionCatalog {
    BrowserProgressionCatalog {
        dataset_key: crate::WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME.to_string(),
        cohort_subject_count: 0,
        cohort_subject_sha256: None,
        cohort_complete_generation_count: 0,
        generations: Vec::new(),
    }
}

fn build_browser_progression_catalog(
    dataset: &StdlibG8rVsYosysDataset,
    versions: Option<&VersionCardsReport>,
    runs: &[BrowserRun],
) -> Result<BrowserProgressionCatalog> {
    if dataset
        .samples
        .iter()
        .any(|sample| sample.stdlib_root_action_id.is_none())
    {
        return Ok(empty_browser_progression_catalog());
    }
    let mut grouped = BTreeMap::<(String, String, String), Vec<_>>::new();
    let mut cohort_populations = BTreeMap::<(String, String), BTreeSet<_>>::new();
    for sample in &dataset.samples {
        let root = sample
            .stdlib_root_action_id
            .as_ref()
            .expect("lineage presence was checked above");
        let subject = ProgressionSubject {
            fn_key: sample.fn_key.clone(),
            ir_top: sample.ir_top.clone(),
        };
        cohort_populations
            .entry((sample.crate_version.clone(), root.clone()))
            .or_default()
            .insert(subject);
        grouped
            .entry((
                sample.crate_version.clone(),
                sample.dso_version.clone(),
                root.clone(),
            ))
            .or_default()
            .push(sample);
    }

    let mut sources = Vec::with_capacity(grouped.len());
    let mut population_frequency = BTreeMap::<Vec<ProgressionSubject>, u64>::new();
    for subjects in cohort_populations.into_values() {
        *population_frequency
            .entry(subjects.into_iter().collect())
            .or_default() += 1;
    }
    for ((crate_version, dso_version, stdlib_root_action_id), samples) in grouped {
        let mut subjects = BTreeSet::new();
        for sample in samples {
            let subject = ProgressionSubject {
                fn_key: sample.fn_key.clone(),
                ir_top: sample.ir_top.clone(),
            };
            if !subjects.insert(subject) {
                bail!(
                    "release progression generation contains a duplicate subject: crate={} root={}",
                    crate_version,
                    stdlib_root_action_id
                );
            }
        }
        let subjects = subjects.into_iter().collect::<Vec<_>>();
        sources.push(ProgressionGenerationSource {
            crate_version,
            dso_version,
            stdlib_root_action_id,
            subjects,
        });
    }

    let mut populations = population_frequency.into_iter().collect::<Vec<_>>();
    populations.sort_by(
        |(left_subjects, left_count), (right_subjects, right_count)| {
            right_count
                .cmp(left_count)
                .then(right_subjects.len().cmp(&left_subjects.len()))
                .then_with(|| {
                    progression_subjects_sha256(left_subjects)
                        .expect("cohort encoding is infallible")
                        .cmp(
                            &progression_subjects_sha256(right_subjects)
                                .expect("cohort encoding is infallible"),
                        )
                })
        },
    );
    let cohort = populations
        .first()
        .map(|(subjects, _)| subjects.clone())
        .unwrap_or_default();
    let cohort_set = cohort.iter().cloned().collect::<BTreeSet<_>>();
    let cohort_subject_sha256 = if cohort.is_empty() {
        None
    } else {
        Some(progression_subjects_sha256(&cohort)?)
    };

    let mut enumeration_by_version = BTreeMap::new();
    if let Some(versions) = versions {
        for card in &versions.cards {
            if enumeration_by_version
                .insert(
                    card.crate_version.clone(),
                    (
                        enumeration_state_label(card.stdlib_enumeration.state).to_string(),
                        matches!(
                            card.stdlib_enumeration.state,
                            StdlibEnumerationState::Partial | StdlibEnumerationState::Ok
                        )
                        .then_some(card.stdlib_enumeration.concrete_functions),
                    ),
                )
                .is_some()
            {
                bail!(
                    "version summary contains duplicate cards for {}",
                    card.crate_version
                );
            }
        }
    }

    let cohort_subject_count = u64::try_from(cohort.len()).context("cohort size exceeds u64")?;
    let mut generations = Vec::with_capacity(sources.len());
    for source in sources {
        let subject_set = source.subjects.iter().cloned().collect::<BTreeSet<_>>();
        let missing_cohort_subject_count =
            u64::try_from(cohort_set.difference(&subject_set).count())
                .context("missing cohort subject count exceeds u64")?;
        let extra_subject_count = u64::try_from(subject_set.difference(&cohort_set).count())
            .context("extra subject count exceeds u64")?;
        let coverage = if subject_set == cohort_set {
            BrowserProgressionCoverage::CohortComplete
        } else if extra_subject_count == 0 {
            BrowserProgressionCoverage::Partial
        } else {
            BrowserProgressionCoverage::Incompatible
        };
        let observed_subject_count =
            u64::try_from(source.subjects.len()).context("subject count exceeds u64")?;
        let (enumeration_status, enumerated_subject_count) = enumeration_by_version
            .get(&source.crate_version)
            .map(|(status, count)| (Some(status.clone()), *count))
            .unwrap_or((None, None));
        let unmeasured_enumerated_subject_count =
            enumerated_subject_count.map(|count| count.saturating_sub(observed_subject_count));
        let mut campaign_runs = runs
            .iter()
            .filter(|run| {
                run.crate_version == source.crate_version
                    && run.dso_version == source.dso_version
                    && run.root_action_ids.contains(&source.stdlib_root_action_id)
            })
            .map(|run| BrowserProgressionRunRef {
                run_id: run.run_id.clone(),
                status: run.status.clone(),
            })
            .collect::<Vec<_>>();
        campaign_runs.sort_by(|left, right| left.run_id.cmp(&right.run_id));
        generations.push(BrowserProgressionGeneration {
            generation_id: progression_generation_id(
                &source.crate_version,
                &source.dso_version,
                &source.stdlib_root_action_id,
            ),
            crate_version: source.crate_version,
            dso_version: source.dso_version,
            stdlib_root_action_id: source.stdlib_root_action_id,
            coverage,
            observed_subject_count,
            cohort_subject_count,
            missing_cohort_subject_count,
            extra_subject_count,
            enumeration_status,
            enumerated_subject_count,
            unmeasured_enumerated_subject_count,
            campaign_runs,
        });
    }
    generations.sort_by(|left, right| {
        cmp_dotted_numeric_version(&left.crate_version, &right.crate_version)
            .then_with(|| cmp_dotted_numeric_version(&left.dso_version, &right.dso_version))
            .then(left.stdlib_root_action_id.cmp(&right.stdlib_root_action_id))
    });
    let cohort_complete_generation_count = u64::try_from(
        generations
            .iter()
            .filter(|generation| generation.coverage == BrowserProgressionCoverage::CohortComplete)
            .count(),
    )
    .context("complete generation count exceeds u64")?;
    Ok(BrowserProgressionCatalog {
        dataset_key: crate::WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME.to_string(),
        cohort_subject_count,
        cohort_subject_sha256,
        cohort_complete_generation_count,
        generations,
    })
}

fn build_browser_progression_catalog_from_site(
    site_dir: &Path,
    datasets: &[BrowserDataset],
    runs: &[BrowserRun],
) -> Result<BrowserProgressionCatalog> {
    let Some(comparison_entry) = datasets.iter().find(|dataset| {
        dataset.logical_key == crate::WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME
    }) else {
        return Ok(empty_browser_progression_catalog());
    };
    let comparison_bytes = fs::read(site_dir.join(&comparison_entry.url)).with_context(|| {
        format!(
            "reading comparison dataset for release progression: {}",
            comparison_entry.url
        )
    })?;
    let comparison: ProgressionComparisonIndexFile = serde_json::from_slice(&comparison_bytes)
        .context("decoding typed release progression comparison dataset")?;
    if comparison.schema_version != crate::WEB_STDLIB_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION
        || comparison.dataset.fraig
    {
        bail!("release progression comparison dataset has the wrong schema or fraig mode");
    }
    let versions = datasets
        .iter()
        .find(|dataset| dataset.logical_key == crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME)
        .map(|entry| {
            let bytes = fs::read(site_dir.join(&entry.url)).with_context(|| {
                format!(
                    "reading versions dataset for release progression: {}",
                    entry.url
                )
            })?;
            let versions: ProgressionVersionsIndexFile = serde_json::from_slice(&bytes)
                .context("decoding typed release progression versions dataset")?;
            if versions.schema_version != crate::WEB_VERSIONS_SUMMARY_INDEX_SCHEMA_VERSION {
                bail!("release progression versions dataset has the wrong schema");
            }
            Ok(versions)
        })
        .transpose()?;
    build_browser_progression_catalog(
        &comparison.dataset,
        versions.as_ref().map(|versions| &versions.report),
        runs,
    )
}

fn normalize_base_url(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() || !value.starts_with('/') {
        bail!("base URL must begin with '/': {value:?}");
    }
    if value.contains('?') || value.contains('#') || value.contains('\\') {
        bail!("base URL must be a path without query, fragment, or backslash");
    }
    if value.split('/').any(|part| part == "." || part == "..") {
        bail!("base URL must not contain '.' or '..' path components");
    }
    Ok(if value.ends_with('/') {
        value.to_string()
    } else {
        format!("{value}/")
    })
}

fn normalized_relpath(value: &str) -> Result<String> {
    if value.is_empty() || value.starts_with('/') {
        bail!("site relpath must be nonempty and relative: {value:?}");
    }
    let mut parts = Vec::new();
    for component in Path::new(value).components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().to_string()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("site relpath is not normalized: {value:?}")
            }
        }
    }
    let normalized = parts.join("/");
    if normalized != value.replace('\\', "/") {
        bail!("site relpath is not normalized: {value:?}");
    }
    Ok(normalized)
}

fn ensure_empty_output_dir(path: &Path, overwrite: bool) -> Result<()> {
    if path.exists() {
        if !overwrite {
            bail!(
                "static site output directory already exists; rerun with --overwrite: {}",
                path.display()
            );
        }
        fs::remove_dir_all(path)
            .with_context(|| format!("removing existing static site: {}", path.display()))?;
    }
    fs::create_dir_all(path)
        .with_context(|| format!("creating static site directory: {}", path.display()))
}

fn normalized_absolute_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("getting current directory for path validation")?
            .join(path)
    };
    let mut existing = absolute.as_path();
    while !existing.exists() {
        existing = existing
            .parent()
            .with_context(|| format!("path has no existing ancestor: {}", absolute.display()))?;
    }
    let mut resolved = fs::canonicalize(existing)
        .with_context(|| format!("canonicalizing path ancestor: {}", existing.display()))?;
    for component in absolute
        .strip_prefix(existing)
        .context("resolving output path suffix")?
        .components()
    {
        match component {
            Component::Normal(part) => resolved.push(part),
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            Component::RootDir | Component::Prefix(_) => {
                bail!("unexpected absolute component in path suffix")
            }
        }
    }
    Ok(resolved)
}

fn reject_site_output_overlap(
    out_dir: &Path,
    snapshot_dir: &Path,
    protected_roots: &[(&str, &Path)],
) -> Result<()> {
    let output = normalized_absolute_path(out_dir)?;
    let snapshot = normalized_absolute_path(snapshot_dir)?;
    if output.starts_with(&snapshot) || snapshot.starts_with(&output) {
        bail!(
            "static site output must not overlap source snapshot: output={} snapshot={}",
            output.display(),
            snapshot.display()
        );
    }
    for (label, root) in protected_roots {
        let protected = normalized_absolute_path(root)?;
        if output.starts_with(&protected) || protected.starts_with(&output) {
            bail!(
                "static site output must not overlap {label}: output={} protected={}",
                output.display(),
                protected.display()
            );
        }
    }
    Ok(())
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn html_shell(
    title: &str,
    site_root_url: &str,
    body: &str,
    css_name: &str,
    js_name: Option<&str>,
) -> String {
    let script = js_name
        .map(|name| format!(r#"<script defer src="{site_root_url}assets/{name}"></script>"#))
        .unwrap_or_default();
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta http-equiv=\"Content-Security-Policy\" content=\"default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'\"><meta name=\"bvc-site-root\" content=\"{site_root_url}\"><title>{}</title><link rel=\"stylesheet\" href=\"{site_root_url}assets/{css_name}\">{script}</head><body>{body}</body></html>",
        escape_html(title)
    )
}

fn html_shell_with_plotly(
    title: &str,
    site_root_url: &str,
    body: &str,
    css_name: &str,
    js_name: &str,
) -> String {
    let plotly =
        format!(r#"<script defer src="{site_root_url}assets/{PLOTLY_ASSET_NAME}"></script>"#);
    let script = format!(r#"<script defer src="{site_root_url}assets/{js_name}"></script>"#);
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta http-equiv=\"Content-Security-Policy\" content=\"default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'\"><meta name=\"bvc-site-root\" content=\"{site_root_url}\"><title>{}</title><link rel=\"stylesheet\" href=\"{site_root_url}assets/{css_name}\">{plotly}{script}</head><body>{body}</body></html>",
        escape_html(title)
    )
}

fn site_root_url(page_relpath: &str) -> Result<String> {
    let page_relpath = normalized_relpath(page_relpath)?;
    if !page_relpath.ends_with(".html") {
        bail!("site page relpath must end in .html: {page_relpath}");
    }
    let depth = Path::new(&page_relpath)
        .parent()
        .into_iter()
        .flat_map(Path::components)
        .filter(|component| matches!(component, Component::Normal(_)))
        .count();
    Ok(if depth == 0 {
        "./".to_string()
    } else {
        "../".repeat(depth)
    })
}

fn resolve_site_link(page_relpath: &str, url: &str) -> Result<String> {
    if url.starts_with('/') {
        bail!("site link must be relative so publication is relocatable: {url}");
    }
    let path = url.split(['?', '#']).next().unwrap_or("");
    let page_dir = Path::new(page_relpath).parent().unwrap_or(Path::new(""));
    let joined = page_dir.join(path);
    let mut parts = Vec::new();
    for component in joined.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().to_string()),
            Component::CurDir => {}
            Component::ParentDir => {
                if parts.pop().is_none() {
                    bail!("site link escapes the immutable site root: {page_relpath} -> {url}");
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                bail!("site link must be relative: {page_relpath} -> {url}")
            }
        }
    }
    if path.ends_with('/') || parts.is_empty() {
        parts.push("index.html".to_string());
    }
    Ok(parts.join("/"))
}

fn media_type(relpath: &str) -> &'static str {
    if relpath.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if relpath.ends_with(".css") {
        "text/css; charset=utf-8"
    } else if relpath.ends_with(".js") {
        "text/javascript; charset=utf-8"
    } else if relpath.ends_with(".json") {
        "application/json"
    } else if relpath.ends_with(".pb") {
        "application/x-protobuf"
    } else {
        "application/octet-stream"
    }
}

fn publication_file(out_dir: &Path, relpath: &str) -> Result<pb::PublicationFile> {
    let relpath = normalized_relpath(relpath)?;
    let bytes = fs::read(out_dir.join(&relpath))
        .with_context(|| format!("reading generated site file: {relpath}"))?;
    Ok(pb::PublicationFile {
        logical_key: relpath.clone(),
        relpath: Some(pb::NormalizedRelpath {
            value: relpath.clone(),
        }),
        bytes: bytes.len() as u64,
        sha256: Some(pb::Sha256Digest {
            value: hex::decode(sha256_hex(&bytes)).expect("sha256 hex decodes"),
        }),
        media_type: media_type(&relpath).to_string(),
    })
}

fn write_file(out_dir: &Path, relpath: &str, bytes: &[u8]) -> Result<()> {
    let relpath = normalized_relpath(relpath)?;
    let path = out_dir.join(relpath);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating static site parent: {}", parent.display()))?;
    }
    fs::write(&path, bytes).with_context(|| format!("writing static site file: {}", path.display()))
}

fn static_site_asset_names() -> (String, String) {
    let css_hash = &sha256_hex(STYLE_CSS.as_bytes())[..16];
    let js_hash = &sha256_hex(APP_JS.as_bytes())[..16];
    (
        format!("site-{css_hash}.css"),
        format!("explorer-{js_hash}.js"),
    )
}

fn insert_unique_site_relpath(paths: &mut BTreeSet<String>, relpath: String) -> Result<()> {
    normalized_relpath(&relpath)?;
    if !paths.insert(relpath.clone()) {
        bail!("static site topology contains duplicate path: {relpath}");
    }
    Ok(())
}

fn expected_catalog_site_relpaths(
    catalog: &BrowserCatalog,
) -> Result<(BTreeSet<String>, BTreeSet<String>)> {
    let (css_name, js_name) = static_site_asset_names();
    let mut all = [
        "catalog.json".to_string(),
        "snapshot_manifest.v1.pb".to_string(),
        "index.html".to_string(),
        "runs.html".to_string(),
        "progression.html".to_string(),
        "mffc-discrepancies.html".to_string(),
        "ir-fn-corpus-g8r-vs-yosys-abc/index.html".to_string(),
        "ir-fn-g8r-abc-vs-codegen-yosys-abc/index.html".to_string(),
        "dataset.html".to_string(),
        format!("assets/{css_name}"),
        format!("assets/{js_name}"),
        format!("assets/{PLOTLY_ASSET_NAME}"),
        format!("assets/{PLOTLY_LICENSE_ASSET_NAME}"),
        format!("assets/{PLOTLY_NOTICE_ASSET_NAME}"),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    let mut data = BTreeSet::new();

    for dataset in &catalog.datasets {
        let expected_url = format!("data/{}", dataset.logical_key);
        if dataset.url != expected_url {
            bail!(
                "browser catalog dataset URL is not canonical: expected {expected_url}, got {}",
                dataset.url
            );
        }
        insert_unique_site_relpath(&mut data, dataset.url.clone())?;
    }

    for run in &catalog.runs {
        let expected_page_url = format!("runs/{}/", run.run_id);
        let expected_page_relpath = format!("runs/{}/index.html", run.run_id);
        let expected_protobuf_url = format!("data/runs/{}/run.pb", run.run_id);
        if run.page_url != expected_page_url || run.protobuf_url != expected_protobuf_url {
            bail!(
                "browser catalog run paths are not canonical for {}",
                run.run_id
            );
        }
        insert_unique_site_relpath(&mut all, expected_page_relpath)?;
        insert_unique_site_relpath(&mut data, run.protobuf_url.clone())?;
        if let Some(findings_url) = &run.findings_protobuf_url {
            let expected_findings_url = format!("data/runs/{}/findings.pb", run.run_id);
            if findings_url != &expected_findings_url {
                bail!(
                    "browser catalog findings path is not canonical for {}",
                    run.run_id
                );
            }
            insert_unique_site_relpath(&mut data, findings_url.clone())?;
        }
    }

    for relpath in &data {
        insert_unique_site_relpath(&mut all, relpath.clone())?;
    }
    Ok((all, data))
}

fn expected_snapshot_site_data_relpaths(
    snapshot: &crate::snapshot::StaticSnapshotManifest,
) -> Result<BTreeSet<String>> {
    let mut expected = BTreeSet::new();
    for entry in &snapshot.dataset_files {
        let relpath = if entry.relpath.ends_with(".json") {
            let suffix = entry
                .relpath
                .strip_prefix("web_index/")
                .unwrap_or(&entry.relpath);
            format!("data/{suffix}")
        } else if entry.index_key.starts_with("runs/")
            && (entry.relpath.ends_with("/run.pb") || entry.relpath.ends_with("/findings.pb"))
        {
            format!("data/{}", entry.relpath)
        } else {
            bail!(
                "source snapshot contains unsupported static-site dataset: {}",
                entry.relpath
            );
        };
        insert_unique_site_relpath(&mut expected, relpath)?;
    }
    Ok(expected)
}

fn actual_site_relpaths(site_dir: &Path) -> Result<BTreeSet<String>> {
    let mut found = BTreeSet::new();
    for entry in WalkDir::new(site_dir).sort_by_file_name() {
        let entry = entry.context("walking static site")?;
        if entry.file_type().is_dir() {
            continue;
        }
        if !entry.file_type().is_file() {
            bail!(
                "static site contains a symlink or special filesystem node: {}",
                entry.path().display()
            );
        }
        let relpath = entry
            .path()
            .strip_prefix(site_dir)
            .context("stripping static site root")?
            .to_string_lossy()
            .replace('\\', "/");
        if relpath != STATIC_SITE_MANIFEST_FILENAME {
            insert_unique_site_relpath(&mut found, relpath)?;
        }
    }
    Ok(found)
}

fn progression_body(root_site_url: &str) -> String {
    format!(
        "<header><p><a href=\"{root_site_url}\">← Results</a></p><h1>Release progression</h1><p class=\"meta\">Signed G8r − Yosys/ABC nodes × levels product loss across the canonical measured stdlib cohort; negative means G8r is better</p><p id=\"error\" role=\"alert\"></p></header><main id=\"progression\" data-dataset-key=\"{}\"><div class=\"toolbar\"><label>Baseline <select id=\"baseline-version\" aria-label=\"Baseline crate release\"></select></label><label>Current <select id=\"current-version\" aria-label=\"Current crate release\"></select></label><label><input id=\"include-incomplete\" type=\"checkbox\"> Include incomplete generations</label></div><p id=\"progression-status\" class=\"meta\" aria-live=\"polite\">Loading release data…</p><section id=\"progression-summary\" class=\"grid\" aria-live=\"polite\"></section><h2>Distribution progression</h2><section id=\"progression-chart\" class=\"progression-chart\" aria-live=\"polite\"><p class=\"muted\">Loading release data…</p></section><h2>Generation coverage</h2><section id=\"progression-inventory\" aria-live=\"polite\"></section><section id=\"progression-table\" aria-live=\"polite\"></section></main>",
        crate::WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME,
    )
}

fn comparison_plots_body(
    root_site_url: &str,
    title: &str,
    dataset_key: &str,
    lhs_label: &str,
    rhs_label: &str,
) -> String {
    format!(
        "<header class=\"comparison-header\"><p><a href=\"{root_site_url}\">← Results</a></p><h1>{}</h1><p class=\"meta\">The same interactive paired-synthesis view as the dynamic site: log/log levels and nodes, four-quadrant deltas, and strict product loss versus IR size</p><nav class=\"comparison-nav\"><a href=\"{root_site_url}ir-fn-corpus-g8r-vs-yosys-abc/\">G8r vs Yosys/ABC</a><a href=\"{root_site_url}ir-fn-g8r-abc-vs-codegen-yosys-abc/\">G8r+ABC vs codegen+Yosys/ABC</a><a href=\"{root_site_url}mffc-discrepancies.html\">MFFC discrepancies</a></nav><p id=\"error\" role=\"alert\"></p></header><main id=\"comparison-plots\" class=\"comparison-page\" data-dataset-key=\"{}\" data-ir-dataset-key=\"{}\" data-lhs-label=\"{}\" data-rhs-label=\"{}\"><section class=\"comparison-controls\"><div class=\"toolbar\"><label>Crate version scope <select id=\"comparison-crate-version\" aria-label=\"Comparison crate version\"></select></label><label>Sample mode <select id=\"comparison-sample-mode\" aria-label=\"Comparison sample mode\"><option value=\"all\">all samples</option><option value=\"losses_only\">show losses only</option></select></label><label>Maximum IR nodes <input id=\"comparison-max-ir-nodes\" class=\"comparison-range\" type=\"range\" step=\"1\" aria-label=\"Maximum IR nodes\"></label><output id=\"comparison-max-ir-nodes-value\"></output></div><div class=\"comparison-status meta\"><span>scope: <strong id=\"comparison-scope-label\">loading…</strong></span><span>filtered: <strong id=\"comparison-filtered-count\">0</strong> / <strong id=\"comparison-total-count\">0</strong></span><span>strict positive-loss plot: <strong id=\"loss-positive-count\">0</strong></span></div><p id=\"comparison-no-data\" class=\"muted\" hidden>No paired samples match the current filters.</p></section><section class=\"plot-grid\"><article class=\"plot-panel\"><h2 class=\"plot-title\">{} levels vs {} levels</h2><div id=\"plot-levels\" class=\"plotly-host\"></div></article><article class=\"plot-panel\"><h2 class=\"plot-title\">{} nodes vs {} nodes</h2><div id=\"plot-nodes\" class=\"plotly-host\"></div></article><article class=\"plot-panel\"><h2 class=\"plot-title\">Quadrant deltas: levels vs nodes</h2><div id=\"plot-delta-quadrant\" class=\"plotly-host\"></div></article><article class=\"plot-panel\"><h2 class=\"plot-title\">{} product loss amount vs IR node count</h2><div id=\"plot-loss-vs-ir\" class=\"plotly-host\"></div></article></section><p class=\"meta\">Scroll to zoom, drag to pan, and click any point for its exact XLS IR, sample identity, and structural evidence. Zeroes on the log/log pair plots are displayed at 1 so they remain visible.</p><section class=\"card comparison-detail\" aria-live=\"polite\"><h2>Selected sample</h2><p id=\"comparison-detail-empty\" class=\"muted\">Click any point in any plot to inspect it.</p><p id=\"comparison-detail-evidence\" class=\"meta\"></p><section id=\"comparison-detail-ir\" class=\"comparison-ir-detail\" hidden></section><details id=\"comparison-detail-raw\" class=\"comparison-raw-detail\" hidden><summary>Raw sample metadata</summary><pre id=\"comparison-detail-json\"></pre></details></section><p class=\"meta\">Raw exported data: <a href=\"{root_site_url}dataset.html?key={}\">dataset explorer</a>.</p></main>",
        escape_html(title),
        escape_html(dataset_key),
        crate::WEB_IR_FN_CORPUS_IR_INDEX_FILENAME,
        escape_html(lhs_label),
        escape_html(rhs_label),
        escape_html(lhs_label),
        escape_html(rhs_label),
        escape_html(lhs_label),
        escape_html(rhs_label),
        escape_html(lhs_label),
        url_encode(dataset_key),
    )
}

fn mffc_discrepancies_body(root_site_url: &str) -> String {
    format!(
        "<header><p><a href=\"{root_site_url}\">← Results</a></p><h1>MFFC discrepancies</h1><p class=\"meta\">Paired MFFCs lowered through G8r+ABC and codegen+Yosys/ABC, ranked by signed nodes × levels product loss; positive means G8r is worse</p><p id=\"error\" role=\"alert\"></p></header><main id=\"mffc-discrepancies\" data-dataset-key=\"{}\" data-ir-dataset-key=\"{}\"><div class=\"toolbar\"><label>Crate release <select id=\"mffc-crate-version\" aria-label=\"MFFC crate release\"></select></label><label>Maximum IR nodes <input id=\"mffc-max-ir-nodes\" type=\"number\" min=\"1\" inputmode=\"numeric\" placeholder=\"all\" aria-label=\"Maximum MFFC IR nodes\"></label><label>Samples <select id=\"mffc-sample-mode\" aria-label=\"MFFC sample mode\"><option value=\"losses_only\">G8r losses only</option><option value=\"all\">all paired MFFCs</option></select></label></div><section id=\"mffc-summary\" class=\"grid\" aria-live=\"polite\"></section><section id=\"mffc-detail\" class=\"sample-detail\" aria-live=\"polite\"><h2>Selected MFFC</h2><p class=\"muted\">Click an MFFC name in the chart or table to inspect its origin, exact XLS IR, and synthesis evidence.</p></section><section id=\"mffc-chart\" aria-live=\"polite\"><p class=\"muted\">Loading paired MFFC data…</p></section><section id=\"mffc-table\" aria-live=\"polite\"></section><p class=\"meta\">Raw exported data: <a href=\"{root_site_url}dataset.html?key={}\">dataset explorer</a>.</p></main>",
        crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME,
        crate::WEB_IR_FN_CORPUS_IR_INDEX_FILENAME,
        url_encode(crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME),
    )
}

fn homepage_body(
    root_site_url: &str,
    catalog: &BrowserCatalog,
    snapshot: &crate::snapshot::StaticSnapshotManifest,
) -> String {
    let generated_utc = snapshot.generated_utc.to_rfc3339();
    let generated_date = generated_utc.split('T').next().unwrap_or(&generated_utc);
    let snapshot_short = snapshot
        .snapshot_id
        .get(..12)
        .unwrap_or(&snapshot.snapshot_id);
    format!(
        r#"<header class="science-header"><div class="science-topline"><div><p class="science-label">xlsynth-bvc / static result corpus</p><h1>Boolean synthesis comparison</h1></div><nav class="science-nav" aria-label="Primary navigation"><a href="{root_site_url}ir-fn-g8r-abc-vs-codegen-yosys-abc/">QoR explorer</a><a href="{root_site_url}progression.html">Release history</a><a href="{root_site_url}runs.html">Runs</a><a href="{root_site_url}dataset.html">Data</a></nav></div><p class="science-abstract">Paired measurements of XLS IR through G8r+ABC and codegen+Yosys/ABC. Both paths share ABC downstream, so the overview isolates frontend structure; the full explorers provide sample-level links to immutable exported evidence.</p><dl class="science-meta"><div><dt>snapshot</dt><dd><code title="{}">{}</code></dd></div><div><dt>verified runs</dt><dd>{}</dd></div><div><dt>datasets</dt><dd>{}</dd></div><div><dt>generated</dt><dd>{}</dd></div></dl><p id="error" role="alert"></p></header><main class="science-main"><section id="home-overview" data-dataset-key="{}" data-lhs-label="G8r+ABC" data-rhs-label="codegen+Yosys/ABC" aria-labelledby="overview-title"><div class="overview-heading"><div><p class="science-label">latest crate release / G8r+ABC vs codegen+Yosys/ABC</p><h2 id="overview-title">Corpus overview</h2><p id="home-overview-status" class="meta" aria-live="polite">Loading paired synthesis measurements…</p></div><a id="home-full-explorer-link" class="text-link" href="{root_site_url}ir-fn-g8r-abc-vs-codegen-yosys-abc/">Open the full interactive view →</a></div><p id="home-coverage-warning" class="coverage-warning" hidden role="status"></p><dl class="metric-row"><div><dt>crate release</dt><dd id="home-overview-release">—</dd></div><div><dt>paired IR samples</dt><dd id="home-sample-count">—</dd></div><div><dt>Q1 pure wins</dt><dd id="home-pure-win-count">—</dd></div><div><dt>Q3 strict losses</dt><dd id="home-strict-loss-count">—</dd></div><div><dt>median signed product Δ</dt><dd id="home-median-loss">—</dd></div></dl><div class="home-plot-grid"><article class="home-chart-panel"><div class="home-chart-header"><h3>Logic levels</h3><span>log / log · y=x · zero→1 disclosed</span></div><div id="home-plot-levels" class="home-plot" aria-label="G8r plus ABC versus codegen plus Yosys ABC logic levels"></div></article><article class="home-chart-panel"><div class="home-chart-header"><h3>Logic nodes</h3><span>log / log · y=x · zero→1 disclosed</span></div><div id="home-plot-nodes" class="home-plot" aria-label="G8r plus ABC versus codegen plus Yosys ABC logic nodes"></div></article><article class="home-chart-panel"><div class="home-chart-header"><h3>Node / level delta quadrants</h3><span>positive = G8r+ABC better</span></div><div id="home-plot-deltas" class="home-plot" aria-label="Four quadrant node and level delta plot"></div></article><article class="home-chart-panel"><div class="home-chart-header"><h3>Strict product losses vs IR size</h3><span>Q3 positive only · click to inspect</span></div><div id="home-plot-loss" class="home-plot" aria-label="G8r plus ABC strict product loss versus IR size"></div></article></div></section><section class="analysis-index" aria-labelledby="analysis-title"><div class="analysis-index-header"><h2 id="analysis-title">Analysis views</h2><p>Focused views of the same exported corpus</p></div><nav class="analysis-list" aria-label="Analysis views"><a class="analysis-link" href="{root_site_url}ir-fn-g8r-abc-vs-codegen-yosys-abc/"><span>01</span><span><strong>G8r+ABC vs codegen+Yosys/ABC</strong><small>Frontend structure comparison with a shared downstream optimizer.</small></span><span>→</span></a><a class="analysis-link" href="{root_site_url}ir-fn-corpus-g8r-vs-yosys-abc/"><span>02</span><span><strong>G8r vs Yosys/ABC</strong><small>Direct backend comparison across releases, filters, quadrants, and sample evidence.</small></span><span>→</span></a><a class="analysis-link" href="{root_site_url}mffc-discrepancies.html"><span>03</span><span><strong>MFFC discrepancies</strong><small>Local cone ranking with paired IR and synthesis evidence.</small></span><span>→</span></a><a class="analysis-link" href="{root_site_url}progression.html"><span>04</span><span><strong>Release progression</strong><small>Median and tail product-loss changes across verified runs.</small></span><span>→</span></a></nav></section><footer class="publication-bar"><span>Self-contained static publication · no live database at request time</span><nav aria-label="Publication details"><a href="{root_site_url}runs.html">Campaign runs</a><a href="{root_site_url}dataset.html">Raw datasets</a></nav></footer></main>"#,
        escape_html(&snapshot.snapshot_id),
        escape_html(snapshot_short),
        catalog.runs.len(),
        catalog.datasets.len(),
        escape_html(generated_date),
        crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME,
    )
}

fn expected_fixed_site_files(
    catalog: &BrowserCatalog,
    snapshot: &crate::snapshot::StaticSnapshotManifest,
) -> Result<BTreeMap<String, Vec<u8>>> {
    let (css_name, js_name) = static_site_asset_names();
    let root_site_url = site_root_url("index.html")?;
    let mut files = BTreeMap::new();
    files.insert(format!("assets/{css_name}"), STYLE_CSS.as_bytes().to_vec());
    files.insert(format!("assets/{js_name}"), APP_JS.as_bytes().to_vec());
    files.insert(format!("assets/{PLOTLY_ASSET_NAME}"), PLOTLY_JS.to_vec());
    files.insert(
        format!("assets/{PLOTLY_LICENSE_ASSET_NAME}"),
        PLOTLY_LICENSE.to_vec(),
    );
    files.insert(
        format!("assets/{PLOTLY_NOTICE_ASSET_NAME}"),
        PLOTLY_NOTICE.to_vec(),
    );
    files.insert("catalog.json".to_string(), encode_browser_catalog(catalog)?);
    files.insert(
        "snapshot_manifest.v1.pb".to_string(),
        crate::snapshot::encode_static_snapshot_manifest(snapshot)?,
    );

    files.insert(
        "progression.html".to_string(),
        html_shell(
            "xlsynth-bvc release progression",
            &root_site_url,
            &progression_body(&root_site_url),
            &css_name,
            Some(&js_name),
        )
        .into_bytes(),
    );
    for (page_relpath, page_title, dataset_key, lhs_label, rhs_label) in [
        (
            "ir-fn-corpus-g8r-vs-yosys-abc/index.html",
            "IR corpus: G8r vs Yosys/ABC",
            crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME,
            "G8r",
            "Yosys/ABC",
        ),
        (
            "ir-fn-g8r-abc-vs-codegen-yosys-abc/index.html",
            "IR corpus: G8r+ABC vs codegen+Yosys/ABC",
            crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME,
            "G8r+ABC",
            "codegen+Yosys/ABC",
        ),
    ] {
        let page_site_root_url = site_root_url(page_relpath)?;
        files.insert(
            page_relpath.to_string(),
            html_shell_with_plotly(
                &format!("xlsynth-bvc {page_title}"),
                &page_site_root_url,
                &comparison_plots_body(
                    &page_site_root_url,
                    page_title,
                    dataset_key,
                    lhs_label,
                    rhs_label,
                ),
                &css_name,
                &js_name,
            )
            .into_bytes(),
        );
    }

    files.insert(
        "mffc-discrepancies.html".to_string(),
        html_shell(
            "xlsynth-bvc MFFC discrepancies",
            &root_site_url,
            &mffc_discrepancies_body(&root_site_url),
            &css_name,
            Some(&js_name),
        )
        .into_bytes(),
    );

    for run in &catalog.runs {
        let page_relpath = format!("runs/{}/index.html", run.run_id);
        let run_site_root_url = site_root_url(&page_relpath)?;
        let root_actions = run
            .root_action_ids
            .iter()
            .map(|id| format!("<li><code>{}</code></li>", escape_html(id)))
            .collect::<String>();
        let finding_rows = run
            .findings
            .iter()
            .map(|finding| {
                let baseline = finding
                    .baseline_value
                    .map(|value| format!("{value:.6}"))
                    .unwrap_or_else(|| "—".to_string());
                let current = finding
                    .current_value
                    .map(|value| format!("{value:.6}"))
                    .unwrap_or_else(|| "—".to_string());
                let structural = finding
                    .structural_hash
                    .as_deref()
                    .map(escape_html)
                    .unwrap_or_else(|| "—".to_string());
                format!(
                    "<tr><td>{}</td><td>{}</td><td>{baseline}</td><td>{current}</td><td><code>{structural}</code></td><td>{}</td></tr>",
                    escape_html(&finding.kind),
                    escape_html(&finding.subject_key),
                    finding.evidence_action_ids.len(),
                )
            })
            .collect::<String>();
        let findings_download = run
            .findings_protobuf_url
            .as_deref()
            .map(|url| {
                format!(
                    "<p><a href=\"{run_site_root_url}{url}\">Download canonical findings protobuf</a></p>"
                )
            })
            .unwrap_or_default();
        let intentional_skip_items = if run.intentionally_skipped_samples.is_empty() {
            "<li>None</li>".to_string()
        } else {
            run.intentionally_skipped_samples
                .iter()
                .map(|skipped| {
                    format!(
                        "<li><code>{}</code> via <strong>{}</strong>: {}</li>",
                        escape_html(&skipped.action_id),
                        escape_html(&skipped.rule_id),
                        escape_html(&skipped.reason),
                    )
                })
                .collect::<String>()
        };
        let intentional_skip_label = if run.intentionally_skipped_samples.len() == 1 {
            "intentional skip"
        } else {
            "intentional skips"
        };
        let body = format!(
            "<header><p><a href=\"{run_site_root_url}runs.html\">← Runs</a></p><h1>{} crate v{}</h1><p class=\"meta\">Campaign {} v{} · DSO v{} · status <strong>{}</strong></p></header><main><div class=\"grid\"><article class=\"card\"><h2>Completion</h2><p>{} roots complete · {} failed · {} canceled</p><p>{} missing outputs · {} failed samples · {} {intentional_skip_label}</p></article><article class=\"card\"><h2>Identity</h2><p>Run <code>{}</code></p><p>Campaign <code>{}</code></p><p><a href=\"{run_site_root_url}{}\">Download public run protobuf</a></p>{findings_download}</article></div><h2>Intentional skips</h2><ul>{intentional_skip_items}</ul><h2>Findings</h2><div class=\"table-wrap\"><table><thead><tr><th>Kind</th><th>Subject</th><th>Baseline loss</th><th>Current loss</th><th>Structural hash</th><th>Evidence actions</th></tr></thead><tbody>{finding_rows}</tbody></table></div><h2>Root actions</h2><ul>{root_actions}</ul><h2>Results</h2><p><a href=\"{run_site_root_url}ir-fn-corpus-g8r-vs-yosys-abc/\">Open interactive QoR plots</a> · <a href=\"{run_site_root_url}progression.html\">View release progression</a> · <a href=\"{run_site_root_url}dataset.html?key={}\">Open g8r versus Yosys/ABC dataset</a></p></main>",
            escape_html(&run.campaign_name),
            escape_html(&run.crate_version),
            escape_html(&run.campaign_name),
            run.campaign_semantic_version,
            escape_html(&run.dso_version),
            escape_html(&run.status),
            run.completed_root_count,
            run.failed_count,
            run.canceled_count,
            run.missing_output_count,
            run.failed_sample_count,
            run.intentionally_skipped_samples.len(),
            run.run_id,
            run.campaign_id,
            run.protobuf_url,
            url_encode(crate::WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME),
        );
        files.insert(
            page_relpath,
            html_shell(
                &format!("{} v{}", run.campaign_name, run.crate_version),
                &run_site_root_url,
                &body,
                &css_name,
                None,
            )
            .into_bytes(),
        );
    }

    let run_cards = catalog
        .runs
        .iter()
        .map(|run| {
            format!(
                "<article class=\"card\"><h2><a href=\"{root_site_url}{}\">{} v{}</a></h2><p>Status <strong>{}</strong> · DSO v{}</p><code>{}</code></article>",
                run.page_url,
                escape_html(&run.campaign_name),
                escape_html(&run.crate_version),
                escape_html(&run.status),
                escape_html(&run.dso_version),
                run.run_id,
            )
        })
        .collect::<String>();
    let runs_body = format!(
        "<header><p><a href=\"{root_site_url}\">← Results</a></p><h1>Campaign runs</h1><p class=\"meta\">{} verified public runs</p></header><main><div class=\"grid\">{run_cards}</div></main>",
        catalog.runs.len()
    );
    files.insert(
        "runs.html".to_string(),
        html_shell(
            "xlsynth-bvc campaign runs",
            &root_site_url,
            &runs_body,
            &css_name,
            None,
        )
        .into_bytes(),
    );

    let index_body = homepage_body(&root_site_url, catalog, snapshot);
    files.insert(
        "index.html".to_string(),
        html_shell_with_plotly(
            "xlsynth-bvc results",
            &root_site_url,
            &index_body,
            &css_name,
            &js_name,
        )
        .into_bytes(),
    );
    let explorer_body = format!(
        "<header><p><a href=\"{root_site_url}\">← Results</a></p><h1>Dataset explorer</h1><div class=\"toolbar\"><label>Dataset <select id=\"dataset\"></select></label><span id=\"dataset-meta\" class=\"meta\"></span></div><p id=\"error\"></p></header><main><section id=\"plot\"></section><section id=\"table\"></section><h2>Raw JSON</h2><pre id=\"raw\">Loading…</pre></main>"
    );
    files.insert(
        "dataset.html".to_string(),
        html_shell(
            "xlsynth-bvc dataset explorer",
            &root_site_url,
            &explorer_body,
            &css_name,
            Some(&js_name),
        )
        .into_bytes(),
    );
    Ok(files)
}

fn verify_exact_fixed_site_files(
    site_dir: &Path,
    catalog: &BrowserCatalog,
    snapshot: &crate::snapshot::StaticSnapshotManifest,
) -> Result<()> {
    for (relpath, expected) in expected_fixed_site_files(catalog, snapshot)? {
        let actual = fs::read(site_dir.join(&relpath))
            .with_context(|| format!("reading fixed generated site file: {relpath}"))?;
        if actual != expected {
            bail!("fixed generated site file differs from deterministic rendering: {relpath}");
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn build_static_site(
    options: &BuildStaticSiteOptions,
) -> Result<BuildStaticSiteSummary> {
    build_static_site_with_protected_roots(options, &[])
}

pub(crate) fn build_static_site_with_protected_roots(
    options: &BuildStaticSiteOptions,
    protected_roots: &[(&str, &Path)],
) -> Result<BuildStaticSiteSummary> {
    verify_static_snapshot(&options.snapshot_dir).context("verifying source snapshot")?;
    let snapshot = load_static_snapshot_manifest(&options.snapshot_dir)?;
    let base_url = normalize_base_url(&options.base_url)?;
    let root_site_url = site_root_url("index.html")?;
    reject_site_output_overlap(&options.out_dir, &options.snapshot_dir, protected_roots)?;
    ensure_empty_output_dir(&options.out_dir, options.overwrite)?;

    let (css_name, js_name) = static_site_asset_names();
    write_file(
        &options.out_dir,
        &format!("assets/{css_name}"),
        STYLE_CSS.as_bytes(),
    )?;
    write_file(
        &options.out_dir,
        &format!("assets/{js_name}"),
        APP_JS.as_bytes(),
    )?;
    write_file(
        &options.out_dir,
        &format!("assets/{PLOTLY_ASSET_NAME}"),
        PLOTLY_JS,
    )?;
    write_file(
        &options.out_dir,
        &format!("assets/{PLOTLY_LICENSE_ASSET_NAME}"),
        PLOTLY_LICENSE,
    )?;
    write_file(
        &options.out_dir,
        &format!("assets/{PLOTLY_NOTICE_ASSET_NAME}"),
        PLOTLY_NOTICE,
    )?;

    let mut datasets = Vec::new();
    for entry in &snapshot.dataset_files {
        if !entry.relpath.ends_with(".json") {
            continue;
        }
        let source = options.snapshot_dir.join(&entry.relpath);
        let suffix = entry
            .relpath
            .strip_prefix("web_index/")
            .unwrap_or(&entry.relpath);
        let target_relpath = format!("data/{suffix}");
        let bytes = fs::read(&source)
            .with_context(|| format!("reading snapshot dataset: {}", source.display()))?;
        if sha256_hex(&bytes) != entry.sha256 {
            bail!(
                "snapshot dataset changed after verification: {}",
                entry.relpath
            );
        }
        write_file(&options.out_dir, &target_relpath, &bytes)?;
        datasets.push(BrowserDataset {
            logical_key: entry.index_key.clone(),
            url: target_relpath,
            bytes: entry.bytes,
            sha256: entry.sha256.clone(),
        });
    }
    datasets.sort_by(|a, b| a.logical_key.cmp(&b.logical_key));
    let mut runs = Vec::new();
    for entry in &snapshot.dataset_files {
        if !entry.index_key.starts_with("runs/") || !entry.relpath.ends_with("/run.pb") {
            continue;
        }
        let source = options.snapshot_dir.join(&entry.relpath);
        let bytes = fs::read(&source)
            .with_context(|| format!("reading public run protobuf: {}", source.display()))?;
        if sha256_hex(&bytes) != entry.sha256 {
            bail!(
                "public run changed after snapshot verification: {}",
                entry.relpath
            );
        }
        let target_relpath = format!("data/{}", entry.relpath);
        write_file(&options.out_dir, &target_relpath, &bytes)?;
        let run = pb::PublicCampaignRun::decode(bytes.as_slice())
            .with_context(|| format!("decoding public run protobuf: {}", entry.relpath))?;
        runs.push(public_run_to_browser(&run, target_relpath)?);
    }
    runs.sort_by(|a, b| {
        a.crate_version
            .cmp(&b.crate_version)
            .then(a.run_id.cmp(&b.run_id))
    });
    for entry in &snapshot.dataset_files {
        if !entry.index_key.starts_with("runs/") || !entry.relpath.ends_with("/findings.pb") {
            continue;
        }
        let source = options.snapshot_dir.join(&entry.relpath);
        let bytes = fs::read(&source)
            .with_context(|| format!("reading findings protobuf: {}", source.display()))?;
        if sha256_hex(&bytes) != entry.sha256 {
            bail!(
                "findings changed after snapshot verification: {}",
                entry.relpath
            );
        }
        let target_relpath = format!("data/{}", entry.relpath);
        write_file(&options.out_dir, &target_relpath, &bytes)?;
        let report = decode_analysis_report(&bytes)?;
        let (run_id, findings) = analysis_to_browser(&report)?;
        let run = runs
            .iter_mut()
            .find(|run| run.run_id == run_id)
            .with_context(|| format!("findings reference unpublished run {run_id}"))?;
        run.findings_protobuf_url = Some(target_relpath);
        run.findings = findings;
    }
    let progression =
        build_browser_progression_catalog_from_site(&options.out_dir, &datasets, &runs)?;
    let catalog = BrowserCatalog {
        schema_version: BROWSER_CATALOG_SCHEMA_VERSION,
        snapshot_id: snapshot.snapshot_id.clone(),
        base_url: base_url.clone(),
        datasets,
        runs,
        progression,
    };
    write_file(
        &options.out_dir,
        "catalog.json",
        &encode_browser_catalog(&catalog)?,
    )?;
    write_file(
        &options.out_dir,
        "snapshot_manifest.v1.pb",
        &crate::snapshot::encode_static_snapshot_manifest(&snapshot)?,
    )?;
    write_file(
        &options.out_dir,
        "progression.html",
        html_shell(
            "xlsynth-bvc release progression",
            &root_site_url,
            &progression_body(&root_site_url),
            &css_name,
            Some(&js_name),
        )
        .as_bytes(),
    )?;
    for (page_relpath, page_title, dataset_key, lhs_label, rhs_label) in [
        (
            "ir-fn-corpus-g8r-vs-yosys-abc/index.html",
            "IR corpus: G8r vs Yosys/ABC",
            crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME,
            "G8r",
            "Yosys/ABC",
        ),
        (
            "ir-fn-g8r-abc-vs-codegen-yosys-abc/index.html",
            "IR corpus: G8r+ABC vs codegen+Yosys/ABC",
            crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME,
            "G8r+ABC",
            "codegen+Yosys/ABC",
        ),
    ] {
        let page_site_root_url = site_root_url(page_relpath)?;
        write_file(
            &options.out_dir,
            page_relpath,
            html_shell_with_plotly(
                &format!("xlsynth-bvc {page_title}"),
                &page_site_root_url,
                &comparison_plots_body(
                    &page_site_root_url,
                    page_title,
                    dataset_key,
                    lhs_label,
                    rhs_label,
                ),
                &css_name,
                &js_name,
            )
            .as_bytes(),
        )?;
    }

    write_file(
        &options.out_dir,
        "mffc-discrepancies.html",
        html_shell(
            "xlsynth-bvc MFFC discrepancies",
            &root_site_url,
            &mffc_discrepancies_body(&root_site_url),
            &css_name,
            Some(&js_name),
        )
        .as_bytes(),
    )?;

    for run in &catalog.runs {
        let page_relpath = format!("runs/{}/index.html", run.run_id);
        let run_site_root_url = site_root_url(&page_relpath)?;
        let root_actions = run
            .root_action_ids
            .iter()
            .map(|id| format!("<li><code>{}</code></li>", escape_html(id)))
            .collect::<String>();
        let finding_rows = run
            .findings
            .iter()
            .map(|finding| {
                let baseline = finding
                    .baseline_value
                    .map(|value| format!("{value:.6}"))
                    .unwrap_or_else(|| "—".to_string());
                let current = finding
                    .current_value
                    .map(|value| format!("{value:.6}"))
                    .unwrap_or_else(|| "—".to_string());
                let structural = finding
                    .structural_hash
                    .as_deref()
                    .map(escape_html)
                    .unwrap_or_else(|| "—".to_string());
                format!(
                    "<tr><td>{}</td><td>{}</td><td>{baseline}</td><td>{current}</td><td><code>{structural}</code></td><td>{}</td></tr>",
                    escape_html(&finding.kind),
                    escape_html(&finding.subject_key),
                    finding.evidence_action_ids.len(),
                )
            })
            .collect::<String>();
        let findings_download = run
            .findings_protobuf_url
            .as_deref()
            .map(|url| {
                format!(
                    "<p><a href=\"{run_site_root_url}{url}\">Download canonical findings protobuf</a></p>"
                )
            })
            .unwrap_or_default();
        let intentional_skip_items = if run.intentionally_skipped_samples.is_empty() {
            "<li>None</li>".to_string()
        } else {
            run.intentionally_skipped_samples
                .iter()
                .map(|skipped| {
                    format!(
                        "<li><code>{}</code> via <strong>{}</strong>: {}</li>",
                        escape_html(&skipped.action_id),
                        escape_html(&skipped.rule_id),
                        escape_html(&skipped.reason),
                    )
                })
                .collect::<String>()
        };
        let intentional_skip_label = if run.intentionally_skipped_samples.len() == 1 {
            "intentional skip"
        } else {
            "intentional skips"
        };
        let body = format!(
            "<header><p><a href=\"{run_site_root_url}runs.html\">← Runs</a></p><h1>{} crate v{}</h1><p class=\"meta\">Campaign {} v{} · DSO v{} · status <strong>{}</strong></p></header><main><div class=\"grid\"><article class=\"card\"><h2>Completion</h2><p>{} roots complete · {} failed · {} canceled</p><p>{} missing outputs · {} failed samples · {} {intentional_skip_label}</p></article><article class=\"card\"><h2>Identity</h2><p>Run <code>{}</code></p><p>Campaign <code>{}</code></p><p><a href=\"{run_site_root_url}{}\">Download public run protobuf</a></p>{findings_download}</article></div><h2>Intentional skips</h2><ul>{intentional_skip_items}</ul><h2>Findings</h2><div class=\"table-wrap\"><table><thead><tr><th>Kind</th><th>Subject</th><th>Baseline loss</th><th>Current loss</th><th>Structural hash</th><th>Evidence actions</th></tr></thead><tbody>{finding_rows}</tbody></table></div><h2>Root actions</h2><ul>{root_actions}</ul><h2>Results</h2><p><a href=\"{run_site_root_url}ir-fn-corpus-g8r-vs-yosys-abc/\">Open interactive QoR plots</a> · <a href=\"{run_site_root_url}progression.html\">View release progression</a> · <a href=\"{run_site_root_url}dataset.html?key={}\">Open g8r versus Yosys/ABC dataset</a></p></main>",
            escape_html(&run.campaign_name),
            escape_html(&run.crate_version),
            escape_html(&run.campaign_name),
            run.campaign_semantic_version,
            escape_html(&run.dso_version),
            escape_html(&run.status),
            run.completed_root_count,
            run.failed_count,
            run.canceled_count,
            run.missing_output_count,
            run.failed_sample_count,
            run.intentionally_skipped_samples.len(),
            run.run_id,
            run.campaign_id,
            run.protobuf_url,
            url_encode(crate::WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME),
        );
        write_file(
            &options.out_dir,
            &page_relpath,
            html_shell(
                &format!("{} v{}", run.campaign_name, run.crate_version),
                &run_site_root_url,
                &body,
                &css_name,
                None,
            )
            .as_bytes(),
        )?;
    }

    let run_cards = catalog
        .runs
        .iter()
        .map(|run| {
            format!(
                "<article class=\"card\"><h2><a href=\"{root_site_url}{}\">{} v{}</a></h2><p>Status <strong>{}</strong> · DSO v{}</p><code>{}</code></article>",
                run.page_url,
                escape_html(&run.campaign_name),
                escape_html(&run.crate_version),
                escape_html(&run.status),
                escape_html(&run.dso_version),
                run.run_id,
            )
        })
        .collect::<String>();
    let runs_body = format!(
        "<header><p><a href=\"{root_site_url}\">← Results</a></p><h1>Campaign runs</h1><p class=\"meta\">{} verified public runs</p></header><main><div class=\"grid\">{run_cards}</div></main>",
        catalog.runs.len()
    );
    write_file(
        &options.out_dir,
        "runs.html",
        html_shell(
            "xlsynth-bvc campaign runs",
            &root_site_url,
            &runs_body,
            &css_name,
            None,
        )
        .as_bytes(),
    )?;

    let index_body = homepage_body(&root_site_url, &catalog, &snapshot);
    write_file(
        &options.out_dir,
        "index.html",
        html_shell_with_plotly(
            "xlsynth-bvc results",
            &root_site_url,
            &index_body,
            &css_name,
            &js_name,
        )
        .as_bytes(),
    )?;
    let explorer_body = format!(
        "<header><p><a href=\"{root_site_url}\">← Results</a></p><h1>Dataset explorer</h1><div class=\"toolbar\"><label>Dataset <select id=\"dataset\"></select></label><span id=\"dataset-meta\" class=\"meta\"></span></div><p id=\"error\"></p></header><main><section id=\"plot\"></section><section id=\"table\"></section><h2>Raw JSON</h2><pre id=\"raw\">Loading…</pre></main>"
    );
    write_file(
        &options.out_dir,
        "dataset.html",
        html_shell(
            "xlsynth-bvc dataset explorer",
            &root_site_url,
            &explorer_body,
            &css_name,
            Some(&js_name),
        )
        .as_bytes(),
    )?;

    let (relpaths, catalog_data_relpaths) = expected_catalog_site_relpaths(&catalog)?;
    let snapshot_data_relpaths = expected_snapshot_site_data_relpaths(&snapshot)?;
    if catalog_data_relpaths != snapshot_data_relpaths {
        bail!("static site catalog does not exactly project the source snapshot");
    }
    let found = actual_site_relpaths(&options.out_dir)?;
    if found != relpaths {
        let unexpected = found.difference(&relpaths).collect::<Vec<_>>();
        let missing = relpaths.difference(&found).collect::<Vec<_>>();
        bail!(
            "generated static site does not match its allowlisted topology; unexpected={unexpected:?} missing={missing:?}"
        );
    }
    let files = relpaths
        .iter()
        .map(|path| publication_file(&options.out_dir, path))
        .collect::<Result<Vec<_>>>()?;
    let total_bytes = files.iter().map(|file| file.bytes).sum();
    let manifest = pb::StaticSiteManifest {
        record_version: STATIC_SITE_RECORD_VERSION,
        source_snapshot_id: Some(pb::Sha256Digest {
            value: hex::decode(&snapshot.snapshot_id).context("decoding snapshot id")?,
        }),
        base_url: base_url.clone(),
        files,
    };
    write_file(
        &options.out_dir,
        STATIC_SITE_MANIFEST_FILENAME,
        &manifest.encode_to_vec(),
    )?;
    verify_static_site(&options.out_dir).context("verifying generated static site")?;

    Ok(BuildStaticSiteSummary {
        out_dir: options.out_dir.display().to_string(),
        snapshot_id: snapshot.snapshot_id,
        base_url,
        dataset_count: catalog.datasets.len(),
        file_count: manifest.files.len(),
        total_bytes,
    })
}

fn url_encode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

fn digest_hex(value: &Option<pb::Sha256Digest>, field: &str) -> Result<String> {
    let value = value.as_ref().with_context(|| format!("missing {field}"))?;
    if value.value.len() != 32 {
        bail!("{field} must contain exactly 32 bytes");
    }
    Ok(hex::encode(&value.value))
}

fn public_run_to_browser(run: &pb::PublicCampaignRun, protobuf_url: String) -> Result<BrowserRun> {
    if run.record_version != 1 {
        bail!(
            "unsupported public run record version {}",
            run.record_version
        );
    }
    let run_id = digest_hex(&run.run_id, "public_run.run_id")?;
    let campaign_id = digest_hex(&run.campaign_id, "public_run.campaign_id")?;
    let status =
        pb::CampaignRunStatus::try_from(run.status).context("public run status is unknown")?;
    if !matches!(
        status,
        pb::CampaignRunStatus::Complete | pb::CampaignRunStatus::Degraded
    ) {
        bail!("static site only accepts complete or degraded public runs");
    }
    let updated = run
        .updated_at
        .as_ref()
        .context("public run missing updated_at")?;
    let updated = chrono::DateTime::from_timestamp(updated.seconds, updated.nanos as u32)
        .context("public run updated_at is invalid")?;
    let root_action_ids = run
        .root_action_ids
        .iter()
        .map(|id| {
            if id.value.len() != 32 {
                bail!("public run root action id must contain 32 bytes");
            }
            Ok(hex::encode(&id.value))
        })
        .collect::<Result<Vec<_>>>()?;
    let intentionally_skipped_samples = run
        .intentionally_skipped_samples
        .iter()
        .map(|skipped| {
            let action_id = skipped
                .action_id
                .as_ref()
                .context("public intentional skip is missing action id")?;
            if action_id.value.len() != 32 {
                bail!("public intentional skip action id must contain 32 bytes");
            }
            if skipped.rule_id.trim().is_empty() || skipped.reason.trim().is_empty() {
                bail!("public intentional skip requires a rule id and reason");
            }
            Ok(BrowserIntentionalSkip {
                action_id: hex::encode(&action_id.value),
                rule_id: skipped.rule_id.clone(),
                reason: skipped.reason.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BrowserRun {
        campaign_id,
        run_id: run_id.clone(),
        campaign_name: run.campaign_name.clone(),
        campaign_semantic_version: run.campaign_semantic_version,
        crate_version: run
            .crate_version
            .as_ref()
            .context("public run missing crate_version")?
            .value
            .clone(),
        dso_version: run
            .dso_version
            .as_ref()
            .context("public run missing dso_version")?
            .value
            .clone(),
        status: status
            .as_str_name()
            .trim_start_matches("CAMPAIGN_RUN_STATUS_")
            .to_ascii_lowercase(),
        updated_utc: updated.to_rfc3339(),
        root_action_ids,
        completed_root_count: run.completed_root_count,
        failed_count: run.failed_count,
        canceled_count: run.canceled_count,
        missing_output_count: run.missing_output_count,
        failed_sample_count: run.failed_sample_count,
        intentionally_skipped_samples,
        protobuf_url,
        page_url: format!("runs/{run_id}/"),
        findings_protobuf_url: None,
        findings: Vec::new(),
    })
}

fn analysis_to_browser(report: &pb::AnalysisReport) -> Result<(String, Vec<BrowserFinding>)> {
    let run_id = digest_hex(&report.run_id, "analysis.run_id")?;
    let mut findings = Vec::with_capacity(report.findings.len());
    for finding in &report.findings {
        let identity = finding
            .identity
            .as_ref()
            .context("analysis finding missing identity")?;
        let metric = identity
            .metric
            .as_ref()
            .context("analysis finding missing metric")?;
        let kind =
            pb::FindingKind::try_from(identity.kind).context("analysis finding kind is unknown")?;
        let structural_hash = finding
            .structural_hash
            .as_ref()
            .map(|digest| {
                if digest.value.len() != 32 {
                    bail!("analysis structural hash must contain 32 bytes");
                }
                Ok(hex::encode(&digest.value))
            })
            .transpose()?;
        let evidence_action_ids = finding
            .evidence
            .iter()
            .map(|evidence| {
                let id = evidence
                    .action_id
                    .as_ref()
                    .context("analysis evidence missing action_id")?;
                if id.value.len() != 32 {
                    bail!("analysis evidence action id must contain 32 bytes");
                }
                Ok(hex::encode(&id.value))
            })
            .collect::<Result<Vec<_>>>()?;
        findings.push(BrowserFinding {
            finding_id: digest_hex(&finding.finding_id, "finding.finding_id")?,
            kind: kind
                .as_str_name()
                .trim_start_matches("FINDING_KIND_")
                .to_ascii_lowercase(),
            subject_key: identity.subject_key.clone(),
            metric_name: metric.name.clone(),
            baseline_value: metric
                .baseline_microunits
                .map(|value| value as f64 / 1_000_000.0),
            current_value: metric
                .current_microunits
                .map(|value| value as f64 / 1_000_000.0),
            unit: metric.unit.clone(),
            structural_hash,
            evidence_action_ids,
        });
    }
    Ok((run_id, findings))
}

pub(crate) fn verify_static_site(site_dir: &Path) -> Result<VerifyStaticSiteSummary> {
    let manifest_path = site_dir.join(STATIC_SITE_MANIFEST_FILENAME);
    let manifest_bytes = fs::read(&manifest_path)
        .with_context(|| format!("reading site manifest: {}", manifest_path.display()))?;
    let manifest = pb::StaticSiteManifest::decode(manifest_bytes.as_slice())
        .context("decoding protobuf site manifest")?;
    if manifest.encode_to_vec() != manifest_bytes {
        bail!("static site manifest is not canonically encoded");
    }
    if manifest.record_version != STATIC_SITE_RECORD_VERSION {
        bail!(
            "unsupported site manifest version {}; expected {}",
            manifest.record_version,
            STATIC_SITE_RECORD_VERSION
        );
    }
    let base_url = normalize_base_url(&manifest.base_url)?;
    if base_url != manifest.base_url {
        bail!("site manifest base_url is not normalized");
    }
    let snapshot_id = digest_hex(&manifest.source_snapshot_id, "source_snapshot_id")?;
    let trusted_plotly_relpath = format!("assets/{PLOTLY_ASSET_NAME}");
    let mut declared = BTreeMap::new();
    let mut total_bytes = 0_u64;
    for file in &manifest.files {
        let relpath = file
            .relpath
            .as_ref()
            .context("site file missing relpath")?
            .value
            .clone();
        normalized_relpath(&relpath)?;
        if file.logical_key != relpath {
            bail!("site file logical_key must equal relpath: {relpath}");
        }
        if file.media_type != media_type(&relpath) {
            bail!("site file media type mismatch: {relpath}");
        }
        if declared.insert(relpath.clone(), file).is_some() {
            bail!("duplicate site file relpath: {relpath}");
        }
        let bytes = fs::read(site_dir.join(&relpath))
            .with_context(|| format!("reading declared site file: {relpath}"))?;
        if bytes.len() as u64 != file.bytes {
            bail!("site file size mismatch: {relpath}");
        }
        if sha256_hex(&bytes) != digest_hex(&file.sha256, "site_file.sha256")? {
            bail!("site file sha256 mismatch: {relpath}");
        }
        if matches!(
            media_type(&relpath),
            "text/html; charset=utf-8" | "text/javascript; charset=utf-8"
        ) {
            let text = std::str::from_utf8(&bytes)
                .with_context(|| format!("site text file is not UTF-8: {relpath}"))?;
            if relpath != trusted_plotly_relpath && text.contains("/api/") {
                bail!("static site file contains forbidden /api/ request path: {relpath}");
            }
        }
        total_bytes += file.bytes;
    }

    let declared_relpaths = declared.keys().cloned().collect::<BTreeSet<_>>();
    let found = actual_site_relpaths(site_dir)?;
    if found != declared_relpaths {
        let undeclared = found.difference(&declared_relpaths).collect::<Vec<_>>();
        let missing = declared_relpaths.difference(&found).collect::<Vec<_>>();
        bail!("site file closure mismatch; undeclared={undeclared:?} missing={missing:?}");
    }

    let attr_re = Regex::new(r#"(?:href|src)=\"([^\"]+)\""#).expect("valid regex");
    for relpath in declared.keys().filter(|path| path.ends_with(".html")) {
        let html = fs::read_to_string(site_dir.join(relpath))?;
        if !html.to_ascii_lowercase().contains("<!doctype html>") {
            bail!("HTML file lacks doctype: {relpath}");
        }
        let expected_site_root = site_root_url(relpath)?;
        let expected_meta = format!("name=\"bvc-site-root\" content=\"{expected_site_root}\"");
        if !html.contains(&expected_meta) {
            bail!("HTML file has wrong site-root metadata in {relpath}");
        }
        for captures in attr_re.captures_iter(&html) {
            let url = &captures[1];
            if url.starts_with("http:") || url.starts_with("https:") || url.starts_with('#') {
                continue;
            }
            let local = resolve_site_link(relpath, url)?;
            if !declared.contains_key(&local) {
                bail!("broken static link in {relpath}: {url} -> {local}");
            }
        }
    }
    let catalog_bytes =
        fs::read(site_dir.join("catalog.json")).context("reading browser catalog")?;
    let catalog = decode_canonical_browser_catalog(&catalog_bytes)?;
    if catalog.schema_version != BROWSER_CATALOG_SCHEMA_VERSION
        || catalog.snapshot_id != snapshot_id
        || catalog.base_url != base_url
    {
        bail!("browser catalog does not match protobuf site manifest");
    }
    let (expected_relpaths, catalog_data_relpaths) = expected_catalog_site_relpaths(&catalog)?;
    if declared_relpaths != expected_relpaths {
        let unexpected = declared_relpaths
            .difference(&expected_relpaths)
            .collect::<Vec<_>>();
        let missing = expected_relpaths
            .difference(&declared_relpaths)
            .collect::<Vec<_>>();
        bail!(
            "site manifest does not match the allowlisted topology; unexpected={unexpected:?} missing={missing:?}"
        );
    }

    let source_snapshot = load_static_snapshot_manifest(site_dir)
        .context("validating embedded source snapshot manifest")?;
    if source_snapshot.snapshot_id != snapshot_id {
        bail!("embedded source snapshot identity disagrees with site manifest");
    }
    verify_exact_fixed_site_files(site_dir, &catalog, &source_snapshot)?;
    let snapshot_data_relpaths = expected_snapshot_site_data_relpaths(&source_snapshot)?;
    if catalog_data_relpaths != snapshot_data_relpaths {
        bail!("static site catalog does not exactly project the embedded source snapshot");
    }
    for entry in &source_snapshot.dataset_files {
        let relpath = if entry.relpath.ends_with(".json") {
            let suffix = entry
                .relpath
                .strip_prefix("web_index/")
                .unwrap_or(&entry.relpath);
            format!("data/{suffix}")
        } else {
            format!("data/{}", entry.relpath)
        };
        let bytes = fs::read(site_dir.join(&relpath))
            .with_context(|| format!("reading snapshot-bound site dataset: {relpath}"))?;
        if bytes.len() as u64 != entry.bytes || sha256_hex(&bytes) != entry.sha256 {
            bail!("site dataset does not match embedded source snapshot: {relpath}");
        }
    }

    let (css_name, js_name) = static_site_asset_names();
    for (relpath, expected_bytes) in [
        (format!("assets/{css_name}"), STYLE_CSS.as_bytes()),
        (format!("assets/{js_name}"), APP_JS.as_bytes()),
        (format!("assets/{PLOTLY_ASSET_NAME}"), PLOTLY_JS),
        (
            format!("assets/{PLOTLY_LICENSE_ASSET_NAME}"),
            PLOTLY_LICENSE,
        ),
        (format!("assets/{PLOTLY_NOTICE_ASSET_NAME}"), PLOTLY_NOTICE),
    ] {
        let actual = fs::read(site_dir.join(&relpath))
            .with_context(|| format!("reading trusted static asset: {relpath}"))?;
        if actual.as_slice() != expected_bytes {
            bail!("static asset does not match the compiled allowlisted content: {relpath}");
        }
    }

    let declared_dataset_urls = declared
        .keys()
        .filter(|relpath| relpath.starts_with("data/") && relpath.ends_with(".json"))
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut catalog_dataset_keys = BTreeSet::new();
    let mut catalog_dataset_urls = BTreeSet::new();
    for dataset in &catalog.datasets {
        if !should_include_snapshot_index_key(&dataset.logical_key) {
            bail!(
                "browser catalog contains a non-public dataset key: {}",
                dataset.logical_key
            );
        }
        let expected_url = format!("data/{}", dataset.logical_key);
        if dataset.url != expected_url || !declared.contains_key(&dataset.url) {
            bail!(
                "browser catalog dataset path is missing or inconsistent: {}",
                dataset.url
            );
        }
        if !catalog_dataset_keys.insert(dataset.logical_key.clone())
            || !catalog_dataset_urls.insert(dataset.url.clone())
        {
            bail!("browser catalog contains a duplicate dataset");
        }
        let bytes = fs::read(site_dir.join(&dataset.url))?;
        if bytes.len() as u64 != dataset.bytes || sha256_hex(&bytes) != dataset.sha256 {
            bail!(
                "browser catalog dataset metadata mismatch: {}",
                dataset.logical_key
            );
        }
        let canonical =
            crate::query::canonicalize_public_web_index_json(&dataset.logical_key, &bytes)
                .with_context(|| {
                    format!(
                        "validating typed browser dataset during site verification: {}",
                        dataset.logical_key
                    )
                })?;
        if canonical != bytes {
            bail!(
                "browser catalog dataset is not canonically encoded: {}",
                dataset.logical_key
            );
        }
    }
    if catalog_dataset_urls != declared_dataset_urls {
        bail!("browser catalog datasets do not exactly match declared public JSON files");
    }
    let expected_progression =
        build_browser_progression_catalog_from_site(site_dir, &catalog.datasets, &catalog.runs)?;
    if catalog.progression != expected_progression {
        bail!("browser release progression projection disagrees with source datasets");
    }
    if !catalog.runs.windows(2).all(|pair| {
        (&pair[0].crate_version, &pair[0].run_id) < (&pair[1].crate_version, &pair[1].run_id)
    }) {
        bail!("browser catalog runs are not strictly sorted by version and run id");
    }
    for run in &catalog.runs {
        if run.page_url != format!("runs/{}/", run.run_id)
            || !declared.contains_key(&format!("runs/{}/index.html", run.run_id))
            || !declared.contains_key(&run.protobuf_url)
        {
            bail!(
                "browser catalog run paths are missing or inconsistent: {}",
                run.run_id
            );
        }
        let public_bytes = fs::read(site_dir.join(&run.protobuf_url))?;
        let public = pb::PublicCampaignRun::decode(public_bytes.as_slice())
            .context("decoding catalog public run protobuf")?;
        if public.encode_to_vec() != public_bytes {
            bail!("catalog public run protobuf is not canonically encoded");
        }
        let mut expected = public_run_to_browser(&public, run.protobuf_url.clone())?;
        if let Some(findings_url) = &run.findings_protobuf_url {
            if !declared.contains_key(findings_url) {
                bail!("run findings protobuf is undeclared: {findings_url}");
            }
            let findings_bytes = fs::read(site_dir.join(findings_url))?;
            let report = decode_analysis_report(&findings_bytes)?;
            if report.encode_to_vec() != findings_bytes {
                bail!("run findings protobuf is not canonically encoded: {findings_url}");
            }
            let (finding_run_id, findings) = analysis_to_browser(&report)?;
            if finding_run_id != run.run_id {
                bail!("run findings identity mismatch for {}", run.run_id);
            }
            expected.findings_protobuf_url = Some(findings_url.clone());
            expected.findings = findings;
        }
        if serde_json::to_value(&expected)? != serde_json::to_value(run)? {
            bail!(
                "browser run projection disagrees with protobuf for {}",
                run.run_id
            );
        }
    }

    Ok(VerifyStaticSiteSummary {
        site_dir: site_dir.display().to_string(),
        snapshot_id,
        base_url,
        file_count: manifest.files.len(),
        total_bytes,
    })
}

fn browser_path(explicit: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        if !path.is_file() {
            bail!("headless browser does not exist: {}", path.display());
        }
        return Ok(path.to_path_buf());
    }
    for candidate in [
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
    ] {
        let path = PathBuf::from(candidate);
        if path.is_file() {
            return Ok(path);
        }
    }
    bail!("no Chrome/Chromium binary found; pass --browser PATH")
}

fn start_plain_static_server(directory: &Path) -> Result<(Child, std::net::SocketAddr)> {
    let port_probe =
        TcpListener::bind(("127.0.0.1", 0)).context("selecting local static smoke HTTP port")?;
    let address = port_probe
        .local_addr()
        .context("reading local static smoke HTTP port")?;
    drop(port_probe);
    let mut child = Command::new("python3")
        .args([
            "-m",
            "http.server",
            &address.port().to_string(),
            "--bind",
            "127.0.0.1",
            "--directory",
        ])
        .arg(directory)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("launching plain Python static HTTP server")?;
    let started = Instant::now();
    loop {
        if TcpStream::connect(address).is_ok() {
            return Ok((child, address));
        }
        if let Some(status) = child
            .try_wait()
            .context("checking plain static HTTP server")?
        {
            bail!("plain static HTTP server exited before startup: {status}");
        }
        if started.elapsed() >= Duration::from_secs(5) {
            let _ = child.kill();
            let _ = child.wait();
            bail!("plain static HTTP server did not start within 5s");
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn run_browser_page(
    browser: &Path,
    profile_dir: &Path,
    url: &str,
    expected_text: &str,
    rendered_markers: &[(String, usize)],
    timeout: Duration,
) -> Result<()> {
    let response = reqwest::blocking::get(url)
        .with_context(|| format!("fetching static smoke page: {url}"))?
        .error_for_status()
        .with_context(|| format!("checking static smoke page status: {url}"))?
        .text()
        .with_context(|| format!("reading static smoke page: {url}"))?;
    if !response.contains(expected_text) {
        bail!("static HTTP response for {url} did not contain {expected_text:?}");
    }
    let screenshot_name = format!("{}.png", &sha256_hex(url.as_bytes())[..16]);
    let screenshot_path = profile_dir.join(screenshot_name);
    let mut command = Command::new(browser);
    command.args([
        "--headless=new",
        "--disable-gpu",
        "--disable-dev-shm-usage",
        "--no-sandbox",
        "--no-first-run",
        "--disable-features=NetworkServiceSandbox",
        "--disable-background-networking",
        "--disable-component-update",
        "--disable-default-apps",
        "--disable-sync",
        "--window-size=1280,900",
    ]);
    if !rendered_markers.is_empty() {
        command.args(["--dump-dom", "--virtual-time-budget=12000"]);
    }
    let mut child = command
        .args([
            &format!("--screenshot={}", screenshot_path.display()),
            &format!("--user-data-dir={}", profile_dir.display()),
            url,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("launching headless browser: {}", browser.display()))?;
    let mut stdout = child
        .stdout
        .take()
        .context("capturing headless browser stdout")?;
    let mut stderr = child
        .stderr
        .take()
        .context("capturing headless browser stderr")?;
    let stdout_reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = stdout.read_to_end(&mut bytes);
        (result, bytes)
    });
    let stderr_reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = stderr.read_to_end(&mut bytes);
        (result, bytes)
    });
    let started = Instant::now();
    let status = loop {
        if rendered_markers.is_empty()
            && screenshot_path
                .metadata()
                .is_ok_and(|metadata| metadata.len() > 0)
        {
            let _ = child.kill();
            break child
                .wait()
                .context("waiting for rendered headless browser")?;
        }
        if let Some(status) = child.try_wait().context("waiting for headless browser")? {
            break status;
        }
        if started.elapsed() >= timeout {
            child.kill().context("killing timed-out headless browser")?;
            let _ = child.wait();
            bail!(
                "headless browser timed out after {}s for {url}",
                timeout.as_secs()
            );
        }
        thread::sleep(Duration::from_millis(50));
    };
    let (stdout_result, stdout) = stdout_reader
        .join()
        .map_err(|_| anyhow::anyhow!("headless browser stdout reader panicked"))?;
    stdout_result.context("reading headless browser stdout")?;
    let (stderr_result, stderr) = stderr_reader
        .join()
        .map_err(|_| anyhow::anyhow!("headless browser stderr reader panicked"))?;
    stderr_result.context("reading headless browser stderr")?;
    if !screenshot_path
        .metadata()
        .is_ok_and(|metadata| metadata.len() > 0)
    {
        bail!(
            "headless browser did not render a screenshot for {url} (status {status}): {}",
            String::from_utf8_lossy(&stderr)
        );
    }
    if !rendered_markers.is_empty() {
        if !status.success() {
            bail!(
                "headless browser failed for {url} with {status}: {}",
                String::from_utf8_lossy(&stderr)
            );
        }
        let rendered = String::from_utf8_lossy(&stdout);
        for (marker, expected_count) in rendered_markers {
            let actual_count = rendered.matches(marker).count();
            if actual_count < *expected_count {
                bail!(
                    "headless browser rendered {actual_count} instances of {marker:?}; expected at least {expected_count} for {url}: {}",
                    String::from_utf8_lossy(&stderr)
                );
            }
        }
    }
    Ok(())
}

pub(crate) fn smoke_static_site(
    site_dir: &Path,
    explicit_browser: Option<&Path>,
    timeout_seconds: u64,
) -> Result<SmokeStaticSiteSummary> {
    if timeout_seconds == 0 {
        bail!("browser smoke timeout must be nonzero");
    }
    let verified = verify_static_site(site_dir)?;
    let catalog = decode_canonical_browser_catalog(
        &fs::read(site_dir.join("catalog.json")).context("reading browser smoke catalog")?,
    )?;
    let mut complete_versions = catalog
        .progression
        .generations
        .iter()
        .filter(|generation| generation.coverage == BrowserProgressionCoverage::CohortComplete)
        .map(|generation| generation.crate_version.clone())
        .collect::<Vec<_>>();
    complete_versions.sort_by(|left, right| cmp_dotted_numeric_version(left, right));
    complete_versions.dedup();
    let progression_markers = if complete_versions.len() >= 2 {
        vec![
            ("data-progression-rendered=\"true\"".to_string(), 1),
            (
                format!(
                    "data-progression-baseline-version=\"{}\"",
                    complete_versions[complete_versions.len() - 2]
                ),
                1,
            ),
            (
                format!(
                    "data-progression-current-version=\"{}\"",
                    complete_versions[complete_versions.len() - 1]
                ),
                1,
            ),
        ]
    } else {
        Vec::new()
    };
    let browser = browser_path(explicit_browser)?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let site_dir = fs::canonicalize(site_dir).context("canonicalizing static site directory")?;
    let mut temporary_server_root = None;
    let server_root = if verified.base_url == "/" {
        site_dir.clone()
    } else {
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-static-server-{}-{nonce}",
            std::process::id()
        ));
        let mount = root.join(verified.base_url.trim_matches('/'));
        fs::create_dir_all(
            mount
                .parent()
                .context("static smoke base URL mount has no parent")?,
        )
        .context("creating static smoke base URL mount")?;
        std::os::unix::fs::symlink(&site_dir, &mount)
            .context("mounting site below static smoke base URL")?;
        temporary_server_root = Some(root.clone());
        root
    };
    let (mut server, address) = start_plain_static_server(&server_root)?;
    let profile_dir =
        std::env::temp_dir().join(format!("xlsynth-bvc-chrome-{}-{nonce}", std::process::id()));
    fs::create_dir_all(&profile_dir).context("creating temporary Chrome profile")?;
    let origin = format!("http://{address}");
    let timeout = Duration::from_secs(timeout_seconds);
    let pages = vec![
        ("", "xlsynth-bvc results", Vec::new()),
        ("runs.html", "Campaign runs", Vec::new()),
        (
            "progression.html",
            "Release progression",
            progression_markers,
        ),
        ("mffc-discrepancies.html", "MFFC discrepancies", Vec::new()),
        (
            "ir-fn-corpus-g8r-vs-yosys-abc/",
            "IR corpus: G8r vs Yosys/ABC",
            vec![("class=\"plotly-host js-plotly-plot\"".to_string(), 4)],
        ),
        (
            "ir-fn-g8r-abc-vs-codegen-yosys-abc/",
            "IR corpus: G8r+ABC vs codegen+Yosys/ABC",
            vec![("class=\"plotly-host js-plotly-plot\"".to_string(), 4)],
        ),
        ("dataset.html", "Dataset explorer", Vec::new()),
    ];
    let result = pages
        .iter()
        .try_for_each(|(path, expected, rendered_markers)| {
            run_browser_page(
                &browser,
                &profile_dir,
                &format!("{origin}{}{path}", verified.base_url),
                expected,
                rendered_markers,
                timeout,
            )
        });
    let _ = server.kill();
    let _ = server.wait();
    fs::remove_dir_all(&profile_dir).ok();
    if let Some(root) = temporary_server_root {
        fs::remove_dir_all(root).ok();
    }
    result?;
    Ok(SmokeStaticSiteSummary {
        site_dir: site_dir.display().to_string(),
        base_url: verified.base_url,
        browser: browser.display().to_string(),
        pages_checked: pages
            .iter()
            .map(|(path, _, _)| (*path).to_string())
            .collect(),
    })
}

#[cfg(test)]
mod tests;
