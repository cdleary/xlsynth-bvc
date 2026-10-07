// SPDX-License-Identifier: Apache-2.0

//! Verified composition: history and full-corpus evidence keep separate roots
//! and populations. Only their final, reproducible browser summary is combined.
use super::*;
use site_corpus::wire;

pub(super) const MANIFEST: &str = "dashboard.pb";
const HTML: &str = include_str!("site_assets/dashboard.html");
const JS: &str = include_str!("site_assets/dashboard.js");
const CSS: &str = include_str!("site_assets/dashboard.css");

fn fixed_files() -> [(&'static str, &'static [u8]); 3] {
    [
        ("index.html", HTML.as_bytes()),
        ("assets/dashboard.js", JS.as_bytes()),
        ("assets/dashboard.css", CSS.as_bytes()),
    ]
}

#[derive(Default, Serialize)]
struct Version {
    crate_version: String,
    full_count: u64,
    historical_abc_count: usize,
    historical_raw_count: usize,
    historical_abc_measurements: usize,
    historical_raw_measurements: usize,
    cohorts: Vec<Coverage>,
}

#[derive(Serialize)]
struct Coverage {
    id: String,
    label: String,
    measured: u64,
    total: u64,
    complete: bool,
    url: String,
}

#[derive(Serialize)]
struct Point {
    version: String,
    url: String,
    cost: f64,
    lower: usize,
    equal: usize,
    higher: usize,
}

#[derive(Serialize)]
struct Trend {
    id: String,
    label: String,
    count: usize,
    points: Vec<Point>,
}

#[derive(Serialize)]
struct Dashboard {
    schema_version: u32,
    versions: Vec<Version>,
    trends: Vec<Trend>,
}

fn point(
    version: &str,
    url: String,
    baseline: &BTreeMap<String, f64>,
    now: &BTreeMap<String, f64>,
) -> Result<Point> {
    if baseline.keys().ne(now.keys()) || now.is_empty() {
        bail!("dashboard trend populations differ");
    }
    if baseline
        .values()
        .chain(now.values())
        .any(|v| !v.is_finite() || *v < 0.0)
    {
        bail!("dashboard cost must be finite and nonnegative");
    }
    let mut result = Point {
        version: version.into(),
        url,
        cost: now.values().sum(),
        lower: 0,
        equal: 0,
        higher: 0,
    };
    for (key, value) in now {
        match value.total_cmp(&baseline[key]) {
            std::cmp::Ordering::Less => result.lower += 1,
            std::cmp::Ordering::Equal => result.equal += 1,
            std::cmp::Ordering::Greater => result.higher += 1,
        }
    }
    Ok(result)
}

fn progression_url(cohort: &str, generation: &str) -> String {
    format!("history/progression.html?cohort={cohort}&current={generation}")
}

fn projection(out: &Path) -> Result<Vec<u8>> {
    // Both child projections have already been verified against their protobuf
    // evidence. They are read only to create the final dashboard web projection.
    let history = out.join("history");
    let catalog = decode_canonical_browser_catalog(&fs::read(history.join("catalog.json"))?)?;
    let mut versions: BTreeMap<String, Version> = BTreeMap::new();
    for (key, raw) in [
        (
            crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME,
            false,
        ),
        (crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME, true),
    ] {
        if let Some(entry) = catalog.datasets.iter().find(|d| d.logical_key == key) {
            let manifest: StaticComparisonManifest =
                serde_json::from_slice(&fs::read(history.join(&entry.url))?)?;
            // Include versions with only one synthesis path measured, even when
            // they cannot yet supply a paired comparison or a trend point.
            for shard in manifest
                .g8r_point_shards
                .iter()
                .chain(&manifest.yosys_point_shards)
            {
                let entry = catalog
                    .datasets
                    .iter()
                    .find(|d| d.logical_key == shard.index_key)
                    .context("missing measurement shard")?;
                let shard: StaticComparisonEntityShard =
                    serde_json::from_slice(&fs::read(history.join(&entry.url))?)?;
                for row in shard.rows {
                    let key = normalize_tag_version(&row.entity.crate_version).to_string();
                    let version = versions.entry(key.clone()).or_insert_with(|| Version {
                        crate_version: key,
                        ..Default::default()
                    });
                    if raw {
                        version.historical_raw_measurements += 1;
                    } else {
                        version.historical_abc_measurements += 1;
                    }
                }
            }
            for shard in manifest.sample_shards {
                let entry = catalog
                    .datasets
                    .iter()
                    .find(|d| d.logical_key == shard.index_key)
                    .context("missing sample shard")?;
                let shard: StaticComparisonSampleShard =
                    serde_json::from_slice(&fs::read(history.join(&entry.url))?)?;
                for row in shard.rows {
                    let key = normalize_tag_version(&row.sample.crate_version).to_string();
                    let version = versions.entry(key.clone()).or_insert_with(|| Version {
                        crate_version: key,
                        ..Default::default()
                    });
                    if raw {
                        version.historical_raw_count += 1;
                    } else {
                        version.historical_abc_count += 1;
                    }
                }
            }
        }
    }
    let dataset = load_progression_comparison_dataset_from_site(&history, &catalog.datasets)?;
    let mut trends = Vec::new();
    for cohort in &catalog.progression.cohorts {
        let mut trend = Trend {
            id: cohort.cohort_id.clone(),
            label: cohort.display_label.clone(),
            count: cohort.cohort_ir_count as usize,
            points: vec![],
        };
        let mut baseline = None;
        let mut baseline_id = None;
        let mut generations = cohort
            .generations
            .iter()
            .filter(|g| {
                g.origin == BrowserProgressionOrigin::CrateRelease && g.observed_ir_count > 0
            })
            .collect::<Vec<_>>();
        generations.sort_by(|a, b| {
            cmp_dotted_numeric_version(
                a.crate_version.as_deref().unwrap_or(""),
                b.crate_version.as_deref().unwrap_or(""),
            )
        });
        for g in generations {
            let version = g
                .crate_version
                .as_ref()
                .context("release without version")?;
            let version = normalize_tag_version(version).to_string();
            let complete = g.coverage == BrowserProgressionCoverage::CohortComplete;
            let entry = versions.entry(version.clone()).or_insert_with(|| Version {
                crate_version: version.clone(),
                ..Default::default()
            });
            entry.cohorts.push(Coverage {
                id: cohort.cohort_id.clone(),
                label: cohort.display_label.clone(),
                measured: g.observed_ir_count.saturating_sub(g.extra_ir_count),
                total: g.cohort_ir_count,
                complete,
                url: progression_url(&cohort.cohort_id, &g.generation_id),
            });
            if !complete {
                continue;
            }
            let samples = progression_baseline_samples(
                dataset.as_ref().context("missing progression dataset")?,
                cohort,
                progression_cohort(&cohort.cohort_id)?,
                &version,
                &g.dso_version,
            )?;
            let now = samples
                .into_iter()
                .map(|(key, sample)| (key, sample.g8r_product))
                .collect::<BTreeMap<_, _>>();
            if now.len() != trend.count {
                bail!("complete dashboard cohort has missing samples");
            }
            let first = baseline.get_or_insert_with(|| now.clone());
            let first_id = baseline_id.get_or_insert_with(|| g.generation_id.clone());
            trend.points.push(point(
                &version,
                format!(
                    "{}&baseline={first_id}",
                    progression_url(&cohort.cohort_id, &g.generation_id)
                ),
                first,
                &now,
            )?);
        }
        if !trend.points.is_empty() {
            trends.push(trend);
        }
    }
    let corpus = out.join("corpus");
    let site = wire::CorpusSite::decode(fs::read(corpus.join(site_corpus::MANIFEST))?.as_slice())?;
    let mut trend = Trend {
        id: "full-corpus".into(),
        label: "Full frozen corpus".into(),
        count: site.generations[0].sample_count as usize,
        points: vec![],
    };
    let mut baseline = None;
    let baseline_version = site.generations[0].crate_version.clone();
    for generation in site.generations {
        let version = generation.crate_version;
        versions
            .entry(version.clone())
            .or_insert_with(|| Version {
                crate_version: version.clone(),
                ..Default::default()
            })
            .full_count = generation.sample_count;
        let mut now = BTreeMap::new();
        for shard in generation.shards {
            let path = shard
                .evidence
                .context("missing corpus evidence")?
                .relpath
                .context("missing evidence path")?
                .value;
            let evidence = wire::EvidenceShard::decode(fs::read(corpus.join(path))?.as_slice())?;
            for sample in evidence.samples {
                let stats = sample.g8r_abc.context("missing G8r stats")?;
                now.insert(
                    sample.sample_id,
                    stats.and_nodes as f64 * stats.depth as f64,
                );
            }
        }
        let first = baseline.get_or_insert_with(|| now.clone());
        trend.points.push(point(
            &version,
            format!("corpus/?release={version}&reference=release&baseline={baseline_version}"),
            first,
            &now,
        )?);
    }
    trends.insert(0, trend);
    let mut versions = versions.into_values().collect::<Vec<_>>();
    versions.sort_by(|a, b| cmp_dotted_numeric_version(&a.crate_version, &b.crate_version));
    Ok(serde_json::to_vec(&Dashboard {
        schema_version: 1,
        versions,
        trends,
    })?)
}

pub(crate) fn build(
    options: &BuildStaticSiteOptions,
    protected: &[(&str, &Path)],
    progression: &[PathBuf],
    corpus: &[PathBuf],
    target: usize,
) -> Result<BuildStaticSiteSummary> {
    reject_site_output_overlap(&options.out_dir, &options.snapshot_dir, protected)?;
    preflight_progression_run_dirs(progression)?;
    if corpus.is_empty() || target == 0 || target > 16 * 1024 * 1024 {
        bail!("dashboard requires corpus inputs and shard target in 1..=16 MiB");
    }
    for run in progression.iter().chain(corpus) {
        reject_site_output_overlap(&options.out_dir, run, protected)?;
    }
    let base_url = normalize_base_url(&options.base_url)?;
    build_static_site_atomically(options, |staging| {
        let child = |name: &str| BuildStaticSiteOptions {
            snapshot_dir: options.snapshot_dir.clone(),
            out_dir: staging.out_dir.join(name),
            base_url: format!("{base_url}{name}/"),
            overwrite: false,
        };
        build_static_site_with_progression_runs_in_place(
            &child("history"),
            protected,
            progression,
        )?;
        site_corpus::build_in_place(&child("corpus"), corpus, target)?;
        finish(&staging.out_dir, &base_url)
    })
}

fn finish(out: &Path, base_url: &str) -> Result<BuildStaticSiteSummary> {
    let dashboard = wire::DashboardSite {
        record_version: 1,
        history_manifest: Some(publication_file(
            out,
            &format!("history/{STATIC_SITE_MANIFEST_FILENAME}"),
        )?),
        corpus_manifest: Some(publication_file(
            out,
            &format!("corpus/{STATIC_SITE_MANIFEST_FILENAME}"),
        )?),
    };
    let bytes = dashboard.encode_to_vec();
    let snapshot_id = sha256_hex(&bytes);
    write_file(out, MANIFEST, &bytes)?;
    for (path, bytes) in fixed_files() {
        write_file(out, path, bytes)?;
    }
    write_file(out, "dashboard.json", &projection(out)?)?;
    let files = actual_site_relpaths(out)?
        .iter()
        .map(|path| publication_file(out, path))
        .collect::<Result<Vec<_>>>()?;
    let total_bytes = files.iter().map(|f| f.bytes).sum();
    let manifest = pb::StaticSiteManifest {
        record_version: STATIC_SITE_RECORD_VERSION,
        source_snapshot_id: Some(crate::proto::digest_from_hex(
            &snapshot_id,
            "dashboard identity",
        )?),
        base_url: base_url.into(),
        files,
    };
    write_file(
        out,
        STATIC_SITE_MANIFEST_FILENAME,
        &manifest.encode_to_vec(),
    )?;
    verify_static_site(out)?;
    Ok(BuildStaticSiteSummary {
        out_dir: out.display().to_string(),
        snapshot_id,
        base_url: base_url.into(),
        dataset_count: 2,
        file_count: manifest.files.len(),
        total_bytes,
    })
}

pub(super) fn verify_projection(out: &Path, manifest: &pb::StaticSiteManifest) -> Result<()> {
    let bytes = fs::read(out.join(MANIFEST))?;
    let dashboard = wire::DashboardSite::decode(bytes.as_slice())?;
    if dashboard.record_version != 1
        || dashboard.encode_to_vec() != bytes
        || sha256_hex(&bytes) != digest_hex(&manifest.source_snapshot_id, "dashboard identity")?
    {
        bail!("invalid dashboard identity");
    }
    let mut expected = BTreeSet::from([MANIFEST.into(), "dashboard.json".into()]);
    for (name, record, corpus) in [
        ("history", dashboard.history_manifest, false),
        ("corpus", dashboard.corpus_manifest, true),
    ] {
        let path = format!("{name}/{STATIC_SITE_MANIFEST_FILENAME}");
        if record.as_ref() != Some(&publication_file(out, &path)?) {
            bail!("dashboard child manifest mismatch");
        }
        let root = out.join(name);
        // Restrict recursion to the two supported leaf site types.
        if root.join(MANIFEST).exists() || root.join(site_corpus::MANIFEST).exists() != corpus {
            bail!("unexpected dashboard child kind");
        }
        let verified = verify_static_site(&root)?;
        if verified.base_url != format!("{}{name}/", manifest.base_url) {
            bail!("dashboard child base URL mismatch");
        }
        expected.insert(path);
        expected.extend(
            actual_site_relpaths(&root)?
                .into_iter()
                .map(|p| format!("{name}/{p}")),
        );
    }
    for (path, bytes) in fixed_files() {
        expected.insert(path.into());
        if fs::read(out.join(path))? != bytes {
            bail!("dashboard template mismatch");
        }
    }
    if actual_site_relpaths(out)? != expected
        || fs::read(out.join("dashboard.json"))? != projection(out)?
    {
        bail!("dashboard projection or topology mismatch");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dashboard_browser_semantics() {
        let output = Command::new("node")
            .arg("testdata/dashboard_test.js")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    #[test]
    fn trend_requires_identical_populations_and_counts_all_changes() {
        let baseline = BTreeMap::from([("a".into(), 2.), ("b".into(), 0.), ("c".into(), 1.)]);
        let now = BTreeMap::from([("a".into(), 1.), ("b".into(), 0.), ("c".into(), 3.)]);
        let p = point("1.0.0", String::new(), &baseline, &now).unwrap();
        assert_eq!((p.cost, p.lower, p.equal, p.higher), (4., 1, 1, 1));
        assert!(
            point(
                "1.0.0",
                String::new(),
                &baseline,
                &BTreeMap::from([("d".into(), 4.)])
            )
            .is_err()
        );
    }
}
