// SPDX-License-Identifier: Apache-2.0

//! Canonical technology-mapping runs -> reproducible, two-dimensional web view.
use super::*;
use crate::techmap_proto::wire::{SampleStatus, TechmapRun, TechmapSample};

pub(super) const MANIFEST: &str = "techmap-site.pb";
pub(super) const ROOT: &str = "tech-mapped-asap7";
const HTML: &[u8] = include_bytes!("site_assets/techmap.html");
const JS: &[u8] = include_bytes!("site_assets/techmap.js");
const CSS: &[u8] = include_bytes!("site_assets/techmap.css");

fn fixed_files() -> Vec<(&'static str, &'static [u8])> {
    vec![
        ("index.html", HTML),
        ("assets/techmap.js", JS),
        ("assets/techmap.css", CSS),
        ("assets/plotly-strict-2.35.2.min.js", PLOTLY_STRICT_JS),
        ("assets/plotly-2.35.2.LICENSE.txt", PLOTLY_LICENSE),
        (
            "assets/plotly-strict-2.35.2.min.js.LICENSE.txt",
            PLOTLY_STRICT_NOTICE,
        ),
    ]
}

fn sha256(bytes: &[u8]) -> Vec<u8> {
    Sha256::digest(bytes).to_vec()
}

fn valid_version(version: &str) -> bool {
    let parts = version.split('.').collect::<Vec<_>>();
    parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|c| c.is_ascii_digit()))
}

fn native_goals(version: &str) -> &'static [&'static str] {
    if cmp_dotted_numeric_version(version, "0.76.0").is_lt() {
        &["area"]
    } else {
        &["area", "delay"]
    }
}

fn abc_goals(version: &str, record_version: u32) -> &'static [&'static str] {
    if record_version == 1 && cmp_dotted_numeric_version(version, "0.76.0").is_lt() {
        &["area"]
    } else {
        &["area", "delay"]
    }
}

fn validate_sample(
    sample: &TechmapSample,
    native_goals: &[&str],
    abc_goals: &[&str],
) -> Result<()> {
    if sample.sample_id.is_empty()
        || sample.sample_id.len() > 256
        || sample.sample_id.chars().any(char::is_control)
        || sample.source_ir_sha256.len() != 32
        || sample.top_fn_name.is_empty()
    {
        bail!("invalid techmap sample identity");
    }
    normalized_relpath(&sample.source_relpath)?;
    let mut unsigned = sample.clone();
    unsigned.record_sha256.clear();
    if sample.record_sha256 != sha256(&unsigned.encode_to_vec()) {
        bail!(
            "techmap sample record digest mismatch: {}",
            sample.sample_id
        );
    }
    let status = SampleStatus::try_from(sample.status).context("unknown mapping sample status")?;
    let expected = native_goals
        .iter()
        .map(|goal| ("g8r", *goal))
        .chain(abc_goals.iter().map(|goal| ("abc", *goal)))
        .collect::<BTreeSet<_>>();
    let mut keys = BTreeSet::new();
    for point in &sample.points {
        let key = (point.tool.as_str(), point.goal.as_str());
        if !expected.contains(&key)
            || !keys.insert(key)
            || !point.area.is_finite()
            || point.area < 0.0
            || !point.delay_ps.is_finite()
            || point.delay_ps < 0.0
            || point.mapped_netlist_sha256.len() != 32
            || point.policy.is_empty()
        {
            bail!("invalid technology mapping point for {}", sample.sample_id);
        }
    }
    match status {
        SampleStatus::Done => {
            if keys != expected
                || sample.g8r_aig_sha256.len() != 32
                || sample.abc_aig_sha256.len() != 32
                || !sample.error.is_empty()
            {
                bail!(
                    "completed techmap sample has missing results: {}",
                    sample.sample_id
                );
            }
        }
        SampleStatus::Failed => {
            if sample.error.is_empty() {
                bail!("failed techmap sample lacks an error");
            }
        }
        SampleStatus::Pending => {
            if !sample.points.is_empty() {
                bail!("pending techmap sample has measured points");
            }
        }
        SampleStatus::Unspecified => bail!("mapping sample status is unspecified"),
    }
    Ok(())
}

fn decode_run(bytes: &[u8]) -> Result<TechmapRun> {
    let run = TechmapRun::decode(bytes).context("decoding technology-mapping run")?;
    if run.encode_to_vec() != bytes {
        bail!("technology-mapping run is not canonically encoded");
    }
    if ![1, 2].contains(&run.record_version)
        || (run.record_version == 2 && native_goals(&run.crate_version).contains(&"delay"))
        || !valid_version(&run.crate_version)
        || !valid_version(&run.dso_version)
        || cmp_dotted_numeric_version(&run.crate_version, "0.75.0").is_lt()
        || run.release_cache_input_sha256.len() != 32
        || run.source_recipe_revision != 1
        || !valid_version(&run.evaluator_crate_version)
        || run.evaluator_release_cache_input_sha256.len() != 32
        || !is_canonical_lower_hex(
            run.evaluator_image_id.strip_prefix("sha256:").unwrap_or(""),
            64,
        )
        || !["full-corpus", "whole-functions-v1", "mffc-v1"].contains(&run.cohort_id.as_str())
        || run.expected_sample_count == 0
        || run.samples.len() as u64 > run.expected_sample_count
        || run.corpus_sha256.len() != 32
        || run.liberty_bundle_sha256.len() != 32
        || run.preparation_script_sha256.len() != 32
        || !is_canonical_lower_hex(
            run.driver_image_id.strip_prefix("sha256:").unwrap_or(""),
            64,
        )
        || !is_canonical_lower_hex(run.abc_image_id.strip_prefix("sha256:").unwrap_or(""), 64)
        || run.primary_input_transition_ps != 10.0
        || run.module_output_load_ff != 1.0
        || run.source_lowering_mode.is_empty()
    {
        bail!("technology-mapping run violates the ASAP7 comparison contract");
    }
    let mut unsigned = run.clone();
    unsigned.samples.clear();
    unsigned.run_spec_sha256.clear();
    unsigned.complete = false;
    if run.run_spec_sha256 != sha256(&unsigned.encode_to_vec()) {
        bail!("technology-mapping run specification digest mismatch");
    }
    let native_goals = native_goals(&run.crate_version);
    let abc_goals = abc_goals(&run.crate_version, run.record_version);
    let mut sample_ids = BTreeSet::new();
    let mut source_ids = BTreeSet::new();
    for sample in &run.samples {
        validate_sample(sample, native_goals, abc_goals)?;
        if !sample_ids.insert(&sample.sample_id)
            || !source_ids.insert((&sample.source_relpath, &sample.top_fn_name))
        {
            bail!("duplicate technology-mapping input");
        }
    }
    let complete = run.samples.len() as u64 == run.expected_sample_count
        && run
            .samples
            .iter()
            .all(|s| s.status == SampleStatus::Done as i32);
    if run.complete != complete {
        bail!("technology-mapping run completion flag disagrees with its measurements");
    }
    Ok(run)
}

fn read_runs(run_dirs: &[PathBuf]) -> Result<Vec<TechmapRun>> {
    let mut runs = Vec::new();
    for dir in run_dirs {
        let path = dir.join("manifest.pb");
        let bytes = fs::read(&path)
            .with_context(|| format!("reading mapper manifest: {}", path.display()))?;
        runs.push(decode_run(&bytes)?);
    }
    validate_run_set(&mut runs)?;
    Ok(runs)
}

fn validate_run_set(runs: &mut [TechmapRun]) -> Result<()> {
    runs.sort_by(|a, b| {
        cmp_dotted_numeric_version(&a.crate_version, &b.crate_version)
            .then(a.cohort_id.cmp(&b.cohort_id))
    });
    let mut keys = BTreeSet::new();
    let mut cohorts = BTreeMap::new();
    let mut libraries = BTreeSet::new();
    let mut evaluators = BTreeSet::new();
    let mut abc_images = BTreeSet::new();
    for run in runs {
        if !keys.insert((run.crate_version.clone(), run.cohort_id.clone())) {
            bail!("duplicate technology-mapping release/cohort");
        }
        evaluators.insert((
            run.evaluator_image_id.clone(),
            run.evaluator_crate_version.clone(),
            run.evaluator_release_cache_input_sha256.clone(),
        ));
        abc_images.insert(run.abc_image_id.clone());
        libraries.insert((
            run.liberty_bundle_sha256.clone(),
            run.preparation_script_sha256.clone(),
        ));
        let identity = (run.corpus_sha256.clone(), run.expected_sample_count);
        if cohorts
            .insert(run.cohort_id.clone(), identity.clone())
            .is_some_and(|old| old != identity)
        {
            bail!("technology-mapping cohort population changes across releases");
        }
    }
    if evaluators.len() > 1 || abc_images.len() > 1 {
        bail!("technology mapping runs require one common evaluator and ABC runtime");
    }
    if libraries.len() > 1 {
        bail!("technology-mapping runs use different libraries or preparation scripts");
    }
    Ok(())
}

const SHARD_TARGET: usize = 2 * 1024 * 1024;

#[derive(Serialize)]
struct Projection {
    schema_version: u32,
    library: &'static str,
    method: &'static str,
    runs: Vec<RunView>,
}
#[derive(Serialize)]
struct RunView {
    version: String,
    cohort_id: String,
    cohort_label: &'static str,
    expected_count: u64,
    sample_count: usize,
    done_count: u64,
    complete: bool,
    run_spec_sha256: String,
    supported_goals: Vec<&'static str>,
    native_goals: Vec<&'static str>,
    input_transition_ps: f64,
    output_load_ff: f64,
    source_lowering_mode: String,
    evaluator_version: String,
    evaluator_image_id: String,
    evaluator_release_cache_sha256: String,
    liberty_sha256: String,
    preparation_sha256: String,
    metric_shards: Vec<String>,
}
#[derive(Serialize)]
struct SampleView {
    id: String,
    label: String,
    source_relpath: String,
    source_sha256: String,
    status: &'static str,
    points: Vec<PointView>,
}
#[derive(Serialize)]
struct PointView {
    tool: String,
    goal: String,
    area: f64,
    delay_ps: f64,
    cell_count: u64,
}
#[derive(Serialize)]
struct SampleShard<'a> {
    schema_version: u32,
    run_spec_sha256: &'a str,
    samples: &'a [SampleView],
}

fn run_view(run: &TechmapRun, metric_shards: Vec<String>) -> RunView {
    RunView {
        version: run.crate_version.clone(),
        cohort_id: run.cohort_id.clone(),
        cohort_label: match run.cohort_id.as_str() {
            "full-corpus" => "Full frozen corpus",
            "whole-functions-v1" => "Whole functions",
            "mffc-v1" => "MFFC cones",
            _ => unreachable!(),
        },
        expected_count: run.expected_sample_count,
        sample_count: run.samples.len(),
        done_count: run
            .samples
            .iter()
            .filter(|s| s.status == SampleStatus::Done as i32)
            .count() as u64,
        complete: run.complete,
        run_spec_sha256: hex::encode(&run.run_spec_sha256),
        supported_goals: abc_goals(&run.crate_version, run.record_version).to_vec(),
        native_goals: native_goals(&run.crate_version).to_vec(),
        input_transition_ps: run.primary_input_transition_ps,
        output_load_ff: run.module_output_load_ff,
        source_lowering_mode: run.source_lowering_mode.clone(),
        evaluator_version: run.evaluator_crate_version.clone(),
        evaluator_image_id: run.evaluator_image_id.clone(),
        evaluator_release_cache_sha256: hex::encode(&run.evaluator_release_cache_input_sha256),
        liberty_sha256: hex::encode(&run.liberty_bundle_sha256),
        preparation_sha256: hex::encode(&run.preparation_script_sha256),
        metric_shards,
    }
}

fn sample_views(run: &TechmapRun) -> Vec<SampleView> {
    let mut samples = run
        .samples
        .iter()
        .map(|sample| {
            let mut points = sample
                .points
                .iter()
                .map(|point| PointView {
                    tool: point.tool.clone(),
                    goal: point.goal.clone(),
                    area: point.area,
                    delay_ps: point.delay_ps,
                    cell_count: point.cell_count,
                })
                .collect::<Vec<_>>();
            points.sort_by(|a, b| a.tool.cmp(&b.tool).then(a.goal.cmp(&b.goal)));
            SampleView {
                id: sample.sample_id.clone(),
                label: sample.top_fn_name.clone(),
                source_relpath: sample.source_relpath.clone(),
                source_sha256: hex::encode(&sample.source_ir_sha256),
                status: match SampleStatus::try_from(sample.status)
                    .expect("validated sample status")
                {
                    SampleStatus::Done => "done",
                    SampleStatus::Failed => "failed",
                    _ => "pending",
                },
                points,
            }
        })
        .collect::<Vec<_>>();
    samples.sort_by(|a, b| a.label.cmp(&b.label).then(a.id.cmp(&b.id)));
    samples
}

#[derive(Deserialize)]
struct CoverageCatalog {
    runs: Vec<PublishedCoverage>,
}
#[derive(Deserialize)]
pub(super) struct PublishedCoverage {
    pub(super) version: String,
    pub(super) cohort_id: String,
    pub(super) cohort_label: String,
    pub(super) expected_count: u64,
    pub(super) done_count: u64,
    pub(super) complete: bool,
}

pub(super) fn published_coverage(out: &Path) -> Result<Vec<PublishedCoverage>> {
    let root = out.join(ROOT);
    if !root.join(MANIFEST).is_file() {
        return Ok(Vec::new());
    }
    // As with the other dashboard inputs, this final browser projection has
    // already been verified against canonical protobuf evidence.
    let catalog: CoverageCatalog = serde_json::from_slice(&fs::read(root.join("results.json"))?)?;
    Ok(catalog.runs)
}

fn run_header(run: &TechmapRun) -> TechmapRun {
    let mut header = run.clone();
    header.samples.clear();
    header.complete = false;
    header
}

fn seal_shard(shard: &mut TechmapRun) {
    shard.complete = shard.samples.len() as u64 == shard.expected_sample_count
        && shard
            .samples
            .iter()
            .all(|s| s.status == SampleStatus::Done as i32);
}

fn evidence_shards(run: &TechmapRun) -> Result<Vec<Vec<u8>>> {
    let header = run_header(run);
    let mut shard = header.clone();
    let mut estimated = shard.encoded_len();
    let mut shards = Vec::new();
    for sample in &run.samples {
        let size = sample.encoded_len() + 10;
        if size + header.encoded_len() > SHARD_TARGET {
            bail!("technology mapping sample exceeds shard budget");
        }
        if estimated + size > SHARD_TARGET && !shard.samples.is_empty() {
            seal_shard(&mut shard);
            shards.push(shard.encode_to_vec());
            shard = header.clone();
            estimated = shard.encoded_len();
        }
        shard.samples.push(sample.clone());
        estimated += size;
    }
    if !shard.samples.is_empty() || shards.is_empty() {
        seal_shard(&mut shard);
        shards.push(shard.encode_to_vec());
    }
    Ok(shards)
}

fn file_record(path: &str, bytes: &[u8]) -> pb::PublicationFile {
    pb::PublicationFile {
        logical_key: path.into(),
        relpath: Some(pb::NormalizedRelpath { value: path.into() }),
        bytes: bytes.len() as u64,
        sha256: Some(pb::Sha256Digest {
            value: sha256(bytes),
        }),
        media_type: media_type(path).into(),
    }
}

fn generated_files(runs: &[TechmapRun]) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut files = BTreeMap::new();
    let mut publications = Vec::new();
    let mut catalog = Vec::new();
    for run in runs {
        let id = hex::encode(&run.run_spec_sha256);
        let mut records = Vec::new();
        for (index, bytes) in evidence_shards(run)?.into_iter().enumerate() {
            let path = format!("data/{id}/{index:05}.pb");
            records.push(file_record(&path, &bytes));
            files.insert(path, bytes);
        }
        publications.push(site_corpus::wire::TechmapRunPublication {
            source_manifest_sha256: sha256(&run.encode_to_vec()),
            shards: records,
        });
        let samples = sample_views(run);
        let mut paths = Vec::new();
        let mut start = 0;
        let mut estimated = 256;
        for index in 0..=samples.len() {
            let size = if index == samples.len() {
                0
            } else {
                serde_json::to_vec(&samples[index])?.len() + 1
            };
            if size + 256 > SHARD_TARGET {
                bail!("technology mapping browser sample exceeds shard budget");
            }
            if (estimated + size > SHARD_TARGET && index > start) || index == samples.len() {
                let path = format!("metrics/{id}/{:05}.json", paths.len());
                let bytes = serde_json::to_vec(&SampleShard {
                    schema_version: 1,
                    run_spec_sha256: &id,
                    samples: &samples[start..index],
                })?;
                paths.push(path.clone());
                files.insert(path, bytes);
                start = index;
                estimated = 256;
            }
            estimated += size;
        }
        catalog.push(run_view(run, paths));
    }
    let evidence = site_corpus::wire::TechmapSite {
        record_version: 1,
        runs: publications,
    }
    .encode_to_vec();
    files.insert(MANIFEST.into(), evidence.clone());
    files.insert("evidence.pb".into(), evidence);
    files.insert("results.json".into(),serde_json::to_vec(&Projection {
        schema_version:1,library:"ASAP7 · RVT · SS · 0.63 V · 100 °C",
        method:"Native g8r area and delay objectives are compared with ABC map -S 10 (and -a for area). All mapped netlists share one pinned gv-stats area and timing evaluator and the same pinned library. Zero wire delay; area uses the library's cell-area units.",runs:catalog,
    })?);
    for (path, bytes) in fixed_files() {
        files.insert(path.into(), bytes.to_vec());
    }
    if files.values().any(|bytes| bytes.len() > 25 * 1024 * 1024) {
        bail!("technology mapping site exceeds asset budget");
    }
    Ok(files)
}

pub(super) fn build_leaf(out: &Path, base_url: &str, run_dirs: &[PathBuf]) -> Result<()> {
    let runs = read_runs(run_dirs)?;
    if runs.is_empty() {
        bail!("technology mapping publication requires at least one run");
    }
    let files = generated_files(&runs)?;
    let identity = sha256(files.get(MANIFEST).expect("generated manifest"));
    for (path, bytes) in &files {
        write_file(out, path, bytes)?;
    }
    let files = files
        .keys()
        .map(|p| publication_file(out, p))
        .collect::<Result<Vec<_>>>()?;
    write_file(
        out,
        STATIC_SITE_MANIFEST_FILENAME,
        &pb::StaticSiteManifest {
            record_version: STATIC_SITE_RECORD_VERSION,
            source_snapshot_id: Some(pb::Sha256Digest { value: identity }),
            base_url: normalize_base_url(base_url)?,
            files,
        }
        .encode_to_vec(),
    )
}

pub(super) fn verify_projection(out: &Path, manifest: &pb::StaticSiteManifest) -> Result<()> {
    let bytes = fs::read(out.join(MANIFEST))?;
    let site = site_corpus::wire::TechmapSite::decode(bytes.as_slice())?;
    if site.record_version != 1
        || site.encode_to_vec() != bytes
        || digest_hex(&manifest.source_snapshot_id, "techmap identity")? != sha256_hex(&bytes)
    {
        bail!("invalid technology-mapping site identity");
    }
    let mut runs = Vec::new();
    for publication in site.runs {
        if publication.source_manifest_sha256.len() != 32 || publication.shards.is_empty() {
            bail!("invalid mapping source manifest binding");
        }
        let mut merged = None;
        for (index, record) in publication.shards.iter().enumerate() {
            let path = &record
                .relpath
                .as_ref()
                .context("missing techmap evidence path")?
                .value;
            normalized_relpath(path)?;
            if record.bytes > SHARD_TARGET as u64 || record != &publication_file(out, path)? {
                bail!("invalid mapping evidence shard binding");
            }
            let shard = decode_run(&fs::read(out.join(path))?)?;
            if path != &format!("data/{}/{index:05}.pb", hex::encode(&shard.run_spec_sha256)) {
                bail!("noncanonical mapping evidence path");
            }
            let run = merged.get_or_insert_with(|| run_header(&shard));
            if run_header(&shard) != run_header(run) {
                bail!("technology mapping shard run specifications differ");
            }
            run.samples.extend(shard.samples);
        }
        let mut run = merged.context("missing mapping run")?;
        seal_shard(&mut run);
        let bytes = run.encode_to_vec();
        if sha256(&bytes) != publication.source_manifest_sha256 {
            bail!("technology mapping shards do not reconstruct their source manifest");
        }
        runs.push(decode_run(&bytes)?);
    }
    if runs.is_empty() {
        bail!("empty technology mapping site");
    }
    validate_run_set(&mut runs)?;
    let files = generated_files(&runs)?;
    if actual_site_relpaths(out)? != files.keys().cloned().collect() {
        bail!("technology mapping site topology mismatch");
    }
    for (path, bytes) in files {
        if fs::read(out.join(&path))? != bytes {
            bail!("technology mapping projection differs from canonical evidence: {path}");
        }
    }
    Ok(())
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::techmap_proto::wire::TechmapPoint;

    pub(crate) fn fixture(version: &str) -> TechmapRun {
        fixture_with_record_version(version, 1)
    }

    fn fixture_with_record_version(version: &str, record_version: u32) -> TechmapRun {
        let mut sample = TechmapSample {
            sample_id: "fixture-input".into(),
            logical_name: "fixture".into(),
            source_relpath: "fixture.ir".into(),
            source_ir_sha256: vec![1; 32],
            top_fn_name: "fixture".into(),
            g8r_aig_sha256: vec![2; 32],
            abc_aig_sha256: vec![3; 32],
            status: SampleStatus::Done as i32,
            ..Default::default()
        };
        for (tool, goals) in [
            ("g8r", native_goals(version)),
            ("abc", abc_goals(version, record_version)),
        ] {
            for goal in goals {
                sample.points.push(TechmapPoint {
                    tool: tool.into(),
                    goal: (*goal).into(),
                    area: 5.0,
                    delay_ps: 10.0,
                    cell_count: 3,
                    mapped_netlist_sha256: vec![4; 32],
                    mapped_netlist_relpath: "cache/fixture.v".into(),
                    policy: (*goal).into(),
                });
            }
        }
        sample.record_sha256 = sha256(&sample.encode_to_vec());
        let mut run = TechmapRun {
            record_version,
            crate_version: version.into(),
            dso_version: "0.71.0".into(),
            cohort_id: "whole-functions-v1".into(),
            expected_sample_count: 1,
            corpus_sha256: vec![5; 32],
            liberty_bundle_sha256: vec![6; 32],
            preparation_script_sha256: vec![7; 32],
            driver_image_id: format!("sha256:{}", "8".repeat(64)),
            abc_image_id: format!("sha256:{}", "9".repeat(64)),
            primary_input_transition_ps: 10.0,
            module_output_load_ff: 1.0,
            source_lowering_mode: "default".into(),
            release_cache_input_sha256: vec![10; 32],
            source_recipe_revision: 1,
            evaluator_image_id: format!("sha256:{}", "b".repeat(64)),
            evaluator_crate_version: "0.79.0".into(),
            evaluator_release_cache_input_sha256: vec![11; 32],
            ..Default::default()
        };
        run.run_spec_sha256 = sha256(&run.encode_to_vec());
        run.samples.push(sample);
        run.complete = true;
        run
    }

    #[test]
    fn verifies_both_native_objectives_and_rejects_forged_completion() {
        let run = fixture("0.76.0");
        assert_eq!(decode_run(&run.encode_to_vec()).unwrap(), run);
        let mut forged = run.clone();
        forged.samples[0].points.pop();
        forged.samples[0].record_sha256.clear();
        forged.samples[0].record_sha256 = sha256(&forged.samples[0].encode_to_vec());
        assert!(decode_run(&forged.encode_to_vec()).is_err());
        let older = fixture("0.75.0");
        assert_eq!(older.samples[0].points.len(), 2);
        assert_eq!(decode_run(&older.encode_to_vec()).unwrap(), older);
    }

    #[test]
    fn older_native_mapper_can_share_the_abc_delay_reference() {
        for (record_version, expected_count, expected_references) in
            [(1, 2, vec!["area"]), (2, 3, vec!["area", "delay"])]
        {
            let run = fixture_with_record_version("0.75.0", record_version);
            assert_eq!(decode_run(&run.encode_to_vec()).unwrap(), run);
            assert_eq!(run.samples[0].points.len(), expected_count);
            assert_eq!(
                run.samples[0]
                    .points
                    .iter()
                    .filter(|p| p.tool == "g8r")
                    .map(|p| p.goal.as_str())
                    .collect::<Vec<_>>(),
                vec!["area"]
            );
            let files = generated_files(&[run]).unwrap();
            let catalog: serde_json::Value =
                serde_json::from_slice(&files["results.json"]).unwrap();
            assert_eq!(
                catalog["runs"][0]["supported_goals"],
                serde_json::json!(expected_references)
            );
            assert_eq!(
                catalog["runs"][0]["native_goals"],
                serde_json::json!(["area"])
            );
        }
        let mut invalid = fixture_with_record_version("0.75.0", 2);
        invalid.samples[0]
            .points
            .retain(|p| !(p.tool == "abc" && p.goal == "delay"));
        invalid.samples[0].record_sha256.clear();
        invalid.samples[0].record_sha256 = sha256(&invalid.samples[0].encode_to_vec());
        assert!(decode_run(&invalid.encode_to_vec()).is_err());
        assert!(decode_run(&fixture_with_record_version("0.76.0", 2).encode_to_vec()).is_err());
    }

    #[test]
    fn final_projection_preserves_separate_area_and_delay_points() {
        let run = fixture("0.76.0");
        let files = generated_files(&[run]).unwrap();
        let data: serde_json::Value = serde_json::from_slice(&files["results.json"]).unwrap();
        assert_eq!(
            data["runs"][0]["supported_goals"],
            serde_json::json!(["area", "delay"])
        );
        let path = data["runs"][0]["metric_shards"][0].as_str().unwrap();
        let shard: serde_json::Value = serde_json::from_slice(&files[path]).unwrap();
        let points = shard["samples"][0]["points"].as_array().unwrap();
        assert_eq!(points.len(), 4);
        assert_eq!(
            points
                .iter()
                .map(|p| (p["tool"].as_str().unwrap(), p["goal"].as_str().unwrap()))
                .collect::<Vec<_>>(),
            vec![
                ("abc", "area"),
                ("abc", "delay"),
                ("g8r", "area"),
                ("g8r", "delay")
            ]
        );
    }

    #[test]
    fn verifies_leaf_against_canonical_evidence_and_rejects_projection_edit() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "bvc-techmap-site-test-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(root.join("run")).unwrap();
        fs::write(
            root.join("run/manifest.pb"),
            fixture("0.76.0").encode_to_vec(),
        )
        .unwrap();
        let out = root.join(ROOT);
        build_leaf(&out, "/nested/tech-mapped-asap7/", &[root.join("run")]).unwrap();
        verify_static_site(&out).unwrap();
        fs::write(out.join("results.json"), b"{}").unwrap();
        // Rehash the outer publication. Reproducibility still binds the web view
        // to the canonical mapper evidence.
        let mut manifest = pb::StaticSiteManifest::decode(
            fs::read(out.join(STATIC_SITE_MANIFEST_FILENAME))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        manifest.files = actual_site_relpaths(&out)
            .unwrap()
            .iter()
            .map(|p| publication_file(&out, p).unwrap())
            .collect();
        fs::write(
            out.join(STATIC_SITE_MANIFEST_FILENAME),
            manifest.encode_to_vec(),
        )
        .unwrap();
        assert!(verify_static_site(&out).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn bounds_evidence_and_browser_shards_for_large_populations() {
        let mut run = fixture("0.76.0");
        let template = run.samples.remove(0);
        run.expected_sample_count = 8_000;
        for index in 0..run.expected_sample_count {
            let mut sample = template.clone();
            sample.sample_id = format!("sample-{index:05}");
            sample.source_relpath = format!("sample-{index:05}.ir");
            sample.record_sha256.clear();
            sample.record_sha256 = sha256(&sample.encode_to_vec());
            run.samples.push(sample);
        }
        run.run_spec_sha256.clear();
        run.run_spec_sha256 = sha256(&run_header(&run).encode_to_vec());
        assert!(run.encoded_len() > SHARD_TARGET);
        assert!(decode_run(&run.encode_to_vec()).is_ok());
        let files = generated_files(&[run]).unwrap();
        let evidence = files
            .iter()
            .filter(|(p, _)| p.starts_with("data/"))
            .collect::<Vec<_>>();
        let metrics = files
            .iter()
            .filter(|(p, _)| p.starts_with("metrics/"))
            .collect::<Vec<_>>();
        assert!(evidence.len() > 1);
        assert!(metrics.len() > 1);
        assert!(
            evidence
                .iter()
                .chain(&metrics)
                .all(|(_, bytes)| bytes.len() <= SHARD_TARGET)
        );
        let samples = evidence
            .iter()
            .map(|(_, bytes)| decode_run(bytes).unwrap().samples.len())
            .sum::<usize>();
        assert_eq!(samples, 8_000);
    }

    #[test]
    fn javascript_preserves_two_dimensional_tradeoffs() {
        let status = Command::new("node")
            .arg("testdata/techmap_site_test.js")
            .status()
            .unwrap();
        assert!(status.success());
    }
}
