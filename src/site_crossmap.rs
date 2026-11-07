// SPDX-License-Identifier: Apache-2.0

//! Controlled same-AIG mapper evidence and its deterministic public projection.

use anyhow::{Context, Result, bail};
use prost::Message;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use crate::crossmap_proto::wire::{ExperimentRun, ExperimentSpec, PublicCrossmapRun, SampleResult};
use crate::techmap_proto::wire::{PublicTechmapRun, SampleStatus, TechmapRun, TechmapSample};

pub(super) const RESULTS: &str = "crossmap-results.json";
const MAX_BYTES: usize = 2 * 1024 * 1024;
const PIPELINE: &str = "xls-ir/xls-combinational-verilog/yosys/abc-and-aig";
const PROOF_METHOD: &str = "abc-cec-positional-fraig-sat";

fn digest(bytes: &[u8]) -> Vec<u8> {
    Sha256::digest(bytes).to_vec()
}

fn sample_digest(sample: &SampleResult) -> Vec<u8> {
    let mut unsigned = sample.clone();
    unsigned.record_sha256.clear();
    digest(&unsigned.encode_to_vec())
}

fn run_digest(run: &ExperimentRun) -> Vec<u8> {
    let mut unsigned = run.clone();
    unsigned.record_sha256.clear();
    digest(&unsigned.encode_to_vec())
}

fn baseline_sample_digest(sample: &TechmapSample) -> Vec<u8> {
    let mut unsigned = sample.clone();
    unsigned.record_sha256.clear();
    digest(&unsigned.encode_to_vec())
}

fn public_baseline_sample(sample: &TechmapSample) -> TechmapSample {
    let mut public = sample.clone();
    for point in &mut public.points {
        point.mapped_netlist_relpath.clear();
    }
    public.record_sha256 = baseline_sample_digest(&public);
    public
}

fn public_id(source_digest: &[u8]) -> String {
    format!("crossmap-{}", hex::encode(source_digest))
}

fn image_id(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn baselines<'a>(
    runs: &'a [PublicTechmapRun],
    selected: &BTreeSet<Vec<u8>>,
) -> Result<BTreeMap<Vec<u8>, &'a TechmapRun>> {
    let mut result = BTreeMap::new();
    let mut identities = BTreeSet::new();
    for public in runs {
        if !selected.contains(&public.source_run_sha256) {
            continue;
        }
        super::site_techmap::decode_public_run(&public.encode_to_vec())?;
        let run = public
            .measurements
            .as_ref()
            .context("missing baseline measurements")?;
        if result
            .insert(public.source_run_sha256.clone(), run)
            .is_some()
            || !identities.insert((&run.crate_version, &run.cohort_id))
        {
            bail!("duplicate same-AIG baseline identity");
        }
    }
    Ok(result)
}

fn validate_spec(spec: &ExperimentSpec, baseline: &TechmapRun) -> Result<()> {
    let library = crate::asap7_library::summary()?;
    if spec.record_version != 1
        || spec.experiment_id.is_empty()
        || spec.experiment_id.len() > 256
        || spec.experiment_id.chars().any(char::is_control)
        || spec.source_pipeline != PIPELINE
        || spec.mapper != "g8r"
        || spec.goals != ["area", "delay"]
        || !image_id(&spec.driver_image_id)
        || !image_id(&spec.evaluator_image_id)
        || !image_id(&spec.equivalence_image_id)
        || spec.driver_crate_version != baseline.crate_version
        || spec.driver_image_id != baseline.driver_image_id
        || spec.evaluator_image_id != baseline.evaluator_image_id
        || spec.equivalence_image_id != baseline.abc_image_id
        || spec.equivalence_method != PROOF_METHOD
        || spec.primary_input_transition_ps != 10.0
        || spec.module_output_load_ff != 1.0
        || spec.cohort_id != baseline.cohort_id
        || spec.expected_samples != baseline.expected_sample_count
        || spec.corpus_sha256 != baseline.corpus_sha256
        || spec.baseline_run_spec_sha256 != baseline.run_spec_sha256
        || spec.library_bundle_sha256 != baseline.liberty_bundle_sha256
        || spec.library_bundle_sha256 != library.archive_sha256
        || spec.merged_liberty_sha256 != library.merged_liberty_sha256
        || baseline.preparation_script_sha256 != library.preparation_script_sha256
        || !baseline.complete
        || baseline.primary_input_transition_ps != 10.0
        || baseline.module_output_load_ff != 1.0
    {
        bail!("same-AIG experiment violates the mapper, baseline, library, or evaluator contract");
    }
    for value in [
        &spec.baseline_manifest_sha256,
        &spec.release_ready_sha256,
        &spec.merged_liberty_sha256,
        &spec.liberty_proto_sha256,
        &spec.liberty_proto_record_sha256,
        &spec.library_bundle_sha256,
        &spec.baseline_run_spec_sha256,
        &spec.corpus_sha256,
        &spec.schema_descriptor_sha256,
        &spec.runner_sha256,
        &spec.prior_diagnostic_spec_sha256,
    ] {
        if value.len() != 32 {
            bail!("same-AIG experiment has an invalid input commitment");
        }
    }
    Ok(())
}

fn validate_run(
    run: &ExperimentRun,
    baseline_runs: &BTreeMap<Vec<u8>, &TechmapRun>,
    public: bool,
) -> Result<()> {
    let spec = run
        .spec
        .as_ref()
        .context("same-AIG experiment has no specification")?;
    let baseline = baseline_runs
        .get(&spec.baseline_manifest_sha256)
        .context("same-AIG experiment has no matching original baseline manifest commitment")?;
    validate_spec(spec, baseline)?;
    if run.record_version != 1
        || !run.complete
        || run.samples.len() as u64 != spec.expected_samples
        || run.samples.len() != baseline.samples.len()
        || run.record_sha256 != run_digest(run)
    {
        bail!("same-AIG experiment is incomplete or its run digest is invalid");
    }
    let expected = baseline
        .samples
        .iter()
        .map(|sample| (&sample.sample_id, sample))
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeSet::new();
    let mut sources = BTreeSet::new();
    let spec_digest = digest(&spec.encode_to_vec());
    for sample in &run.samples {
        let retained = sample
            .baseline
            .as_ref()
            .context("same-AIG sample has no baseline")?;
        let matched = expected
            .get(&sample.sample_id)
            .context("same-AIG sample does not belong to its baseline")?;
        if sample.record_version != 1
            || sample.status != "done"
            || !sample.error.is_empty()
            || !seen.insert(&sample.sample_id)
            || !sources.insert((&retained.source_relpath, &retained.top_fn_name))
            || retained.sample_id != sample.sample_id
            || retained.status != SampleStatus::Done as i32
            || retained.record_sha256 != baseline_sample_digest(retained)
            || sample.record_sha256 != sample_digest(sample)
            || sample.experiment_spec_sha256 != spec_digest
            || sample.input_aig_sha256 != retained.abc_aig_sha256
            || sample.input_aig_sha256.len() != 32
            || sample.input_source_record_sha256.len() != 32
            || public_baseline_sample(retained) != **matched
        {
            bail!("same-AIG sample identity, baseline measurements, or input digest mismatch");
        }
        let expected_baseline = BTreeSet::from([
            ("g8r", "area"),
            ("g8r", "delay"),
            ("abc", "area"),
            ("abc", "delay"),
        ]);
        if retained
            .points
            .iter()
            .map(|point| (point.tool.as_str(), point.goal.as_str()))
            .collect::<BTreeSet<_>>()
            != expected_baseline
        {
            bail!("same-AIG sample requires all four original baseline endpoints");
        }
        let mut goals = BTreeSet::new();
        for point in &sample.points {
            if point.mapper != "g8r"
                || point.source_pipeline != PIPELINE
                || !["area", "delay"].contains(&point.goal.as_str())
                || !goals.insert(point.goal.as_str())
                || point.input_aig_sha256 != sample.input_aig_sha256
                || point.status != "done"
                || !point.error.is_empty()
                || point.equivalence_status != "proved"
                || !point.cell_area.is_finite()
                || point.cell_area < 0.0
                || !point.delay_ps.is_finite()
                || point.delay_ps < 0.0
            {
                bail!(
                    "same-AIG endpoint has invalid metrics, provenance, completion, or proof status"
                );
            }
            for value in [
                &point.mapped_netlist_sha256,
                &point.external_stats_sha256,
                &point.external_map_metrics_sha256,
                &point.roundtrip_aig_sha256,
                &point.equivalence_report_sha256,
            ] {
                if value.len() != 32 {
                    bail!("same-AIG endpoint has an invalid artifact or proof commitment");
                }
            }
            if public && !point.attempt_relpath.is_empty() {
                bail!("public same-AIG evidence contains a private attempt locator");
            }
        }
        if goals != BTreeSet::from(["area", "delay"]) {
            bail!("same-AIG sample is missing a mapping goal");
        }
        if public
            && retained
                .points
                .iter()
                .any(|point| !point.mapped_netlist_relpath.is_empty())
        {
            bail!("public same-AIG evidence contains a private baseline netlist locator");
        }
    }
    Ok(())
}

fn project_run(run: &ExperimentRun) -> PublicCrossmapRun {
    let source_run_sha256 = digest(&run.encode_to_vec());
    let mut measurements = run.clone();
    let spec = measurements
        .spec
        .as_mut()
        .expect("validated same-AIG specification");
    spec.experiment_id = public_id(&source_run_sha256);
    let spec_digest = digest(&spec.encode_to_vec());
    for sample in &mut measurements.samples {
        sample.experiment_spec_sha256 = spec_digest.clone();
        sample.baseline = sample.baseline.as_ref().map(public_baseline_sample);
        for point in &mut sample.points {
            point.attempt_relpath.clear();
        }
        sample.record_sha256 = sample_digest(sample);
    }
    measurements.record_sha256 = run_digest(&measurements);
    PublicCrossmapRun {
        record_version: 1,
        source_run_sha256,
        measurements: Some(measurements),
    }
}

#[derive(Serialize)]
struct Projection {
    schema_version: u32,
    runs: Vec<RunView>,
}
#[derive(Serialize)]
struct RunView {
    id: String,
    version: String,
    cohort_id: String,
    expected_count: u64,
    sample_count: usize,
    verified_points: usize,
    input_transition_ps: f64,
    output_load_ff: f64,
    samples: Vec<SampleView>,
}
#[derive(Serialize)]
struct SampleView {
    id: String,
    label: String,
    points: Vec<PointView>,
}
#[derive(Serialize)]
struct PointView {
    source: &'static str,
    mapper: String,
    goal: String,
    area: f64,
    delay_ps: f64,
    cell_count: u64,
    verified: bool,
}

fn generated_files(
    mut public_runs: Vec<PublicCrossmapRun>,
    baseline_runs: &[PublicTechmapRun],
) -> Result<BTreeMap<String, Vec<u8>>> {
    let selected = public_runs
        .iter()
        .filter_map(|public| {
            public
                .measurements
                .as_ref()?
                .spec
                .as_ref()
                .map(|spec| spec.baseline_manifest_sha256.clone())
        })
        .collect();
    let baseline_runs = baselines(baseline_runs, &selected)?;
    let mut ids = BTreeSet::new();
    let mut identities = BTreeSet::new();
    let mut compatible_inputs = BTreeSet::new();
    let mut output = BTreeMap::new();
    let mut views = Vec::new();
    public_runs.sort_by(|a, b| a.source_run_sha256.cmp(&b.source_run_sha256));
    for public in public_runs {
        if public.record_version != 1 || public.source_run_sha256.len() != 32 {
            bail!("invalid public same-AIG evidence wrapper");
        }
        let run = public
            .measurements
            .as_ref()
            .context("public same-AIG evidence lacks measurements")?;
        let spec = run
            .spec
            .as_ref()
            .context("public same-AIG evidence lacks specification")?;
        let id = public_id(&public.source_run_sha256);
        if spec.experiment_id != id
            || !ids.insert(id.clone())
            || !identities.insert((spec.driver_crate_version.clone(), spec.cohort_id.clone()))
        {
            bail!("duplicate or invalid public same-AIG experiment identity");
        }
        compatible_inputs.insert((
            spec.evaluator_image_id.clone(),
            spec.library_bundle_sha256.clone(),
            spec.merged_liberty_sha256.clone(),
            spec.liberty_proto_sha256.clone(),
            spec.equivalence_image_id.clone(),
        ));
        validate_run(run, &baseline_runs, true)?;
        let bytes = public.encode_to_vec();
        if bytes.len() > MAX_BYTES {
            bail!(
                "same-AIG public experiment exceeds the 2 MiB evidence budget; sharding is required"
            );
        }
        output.insert(format!("crossmap/{id}.pb"), bytes);
        let mut samples = Vec::new();
        for sample in &run.samples {
            let baseline = sample.baseline.as_ref().expect("validated baseline");
            let mut points = baseline
                .points
                .iter()
                .map(|point| PointView {
                    source: if point.tool == "g8r" {
                        "native-g8r"
                    } else {
                        "yosys-abc"
                    },
                    mapper: point.tool.clone(),
                    goal: point.goal.clone(),
                    area: point.area,
                    delay_ps: point.delay_ps,
                    cell_count: point.cell_count,
                    verified: false,
                })
                .chain(sample.points.iter().map(|point| PointView {
                    source: "yosys-abc",
                    mapper: point.mapper.clone(),
                    goal: point.goal.clone(),
                    area: point.cell_area,
                    delay_ps: point.delay_ps,
                    cell_count: point.cell_count,
                    verified: true,
                }))
                .collect::<Vec<_>>();
            points.sort_by(|a, b| {
                a.source
                    .cmp(b.source)
                    .then(a.mapper.cmp(&b.mapper))
                    .then(a.goal.cmp(&b.goal))
            });
            samples.push(SampleView {
                id: sample.sample_id.clone(),
                label: baseline.top_fn_name.clone(),
                points,
            });
        }
        samples.sort_by(|a, b| a.label.cmp(&b.label).then(a.id.cmp(&b.id)));
        views.push(RunView {
            id,
            version: spec.driver_crate_version.clone(),
            cohort_id: spec.cohort_id.clone(),
            expected_count: spec.expected_samples,
            sample_count: samples.len(),
            verified_points: samples.len() * 2,
            input_transition_ps: spec.primary_input_transition_ps,
            output_load_ff: spec.module_output_load_ff,
            samples,
        });
    }
    if compatible_inputs.len() > 1 {
        bail!("same-AIG experiments require one common library, evaluator, and proof runtime");
    }
    views.sort_by(|a, b| {
        crate::versioning::cmp_dotted_numeric_version(&a.version, &b.version)
            .then(a.cohort_id.cmp(&b.cohort_id))
            .then(a.id.cmp(&b.id))
    });
    let json = serde_json::to_vec(&Projection {
        schema_version: 1,
        runs: views,
    })?;
    if json.len() > MAX_BYTES {
        bail!("same-AIG browser projection exceeds the 2 MiB budget; sharding is required");
    }
    output.insert(RESULTS.into(), json);
    Ok(output)
}

/// Read canonical execution records and create sanitized public evidence plus browser data.
pub(super) fn generate(
    input_paths: &[PathBuf],
    baseline_public_runs: &[PublicTechmapRun],
) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut runs = Vec::new();
    for input in input_paths {
        let path = if input.is_dir() {
            input.join("manifest.pb")
        } else {
            input.clone()
        };
        if fs::metadata(&path)?.len() > MAX_BYTES as u64 {
            bail!(
                "same-AIG input experiment exceeds the 2 MiB evidence budget; sharding is required"
            );
        }
        let bytes = fs::read(&path)
            .with_context(|| format!("reading same-AIG experiment {}", path.display()))?;
        if bytes.len() > MAX_BYTES {
            bail!(
                "same-AIG input experiment exceeds the 2 MiB evidence budget; sharding is required"
            );
        }
        let run =
            ExperimentRun::decode(bytes.as_slice()).context("decoding same-AIG experiment")?;
        if run.encode_to_vec() != bytes {
            bail!("same-AIG experiment is not canonically encoded");
        }
        runs.push(run);
    }
    let selected = runs
        .iter()
        .filter_map(|run| {
            run.spec
                .as_ref()
                .map(|spec| spec.baseline_manifest_sha256.clone())
        })
        .collect();
    let baselines = baselines(baseline_public_runs, &selected)?;
    let mut public = Vec::new();
    for run in runs {
        validate_run(&run, &baselines, false)?;
        public.push(project_run(&run));
    }
    generated_files(public, baseline_public_runs)
}

/// Validate published protobuf evidence and reconstruct every derived publication byte.
pub(super) fn regenerate_public(
    evidence: &BTreeMap<String, Vec<u8>>,
    baseline_public_runs: &[PublicTechmapRun],
) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut public = Vec::new();
    for (path, bytes) in evidence {
        if bytes.len() > MAX_BYTES {
            bail!("same-AIG public experiment exceeds the 2 MiB evidence budget");
        }
        let run = PublicCrossmapRun::decode(bytes.as_slice())
            .context("decoding public same-AIG experiment")?;
        if run.encode_to_vec() != *bytes
            || *path != format!("crossmap/{}.pb", public_id(&run.source_run_sha256))
        {
            bail!("same-AIG public evidence has a noncanonical encoding or path");
        }
        public.push(run);
    }
    generated_files(public, baseline_public_runs)
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::crossmap_proto::wire::Metric;
    use crate::techmap_proto::wire::TechmapPoint;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct FixtureDir(PathBuf);
    impl FixtureDir {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "bvc-crossmap-test-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for FixtureDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    pub(in crate::site) fn fixturepair() -> (TechmapRun, ExperimentRun) {
        let library = crate::asap7_library::summary().unwrap();
        let mut sample = TechmapSample {
            sample_id: "fixture-input".into(),
            logical_name: "fixture".into(),
            source_relpath: "fixture.ir".into(),
            source_ir_sha256: vec![1; 32],
            top_fn_name: "fixture_top".into(),
            g8r_aig_sha256: vec![2; 32],
            abc_aig_sha256: vec![3; 32],
            status: SampleStatus::Done as i32,
            ..Default::default()
        };
        for (tool, goal) in [
            ("g8r", "area"),
            ("abc", "area"),
            ("g8r", "delay"),
            ("abc", "delay"),
        ] {
            sample.points.push(TechmapPoint {
                tool: tool.into(),
                goal: goal.into(),
                area: 5.0,
                delay_ps: 10.0,
                cell_count: 3,
                mapped_netlist_sha256: vec![4; 32],
                mapped_netlist_relpath: "../../private/fictional-operator/cache/mapped.v".into(),
                policy: format!("{tool}-{goal}"),
            });
        }
        sample.record_sha256 = baseline_sample_digest(&sample);
        let mut baseline = TechmapRun {
            record_version: 1,
            crate_version: "0.79.0".into(),
            dso_version: "0.61.0".into(),
            cohort_id: "whole-functions-v1".into(),
            expected_sample_count: 1,
            corpus_sha256: vec![5; 32],
            liberty_bundle_sha256: library.archive_sha256.clone(),
            preparation_script_sha256: library.preparation_script_sha256.clone(),
            driver_image_id: format!("sha256:{}", "a".repeat(64)),
            abc_image_id: format!("sha256:{}", "b".repeat(64)),
            evaluator_image_id: format!("sha256:{}", "a".repeat(64)),
            evaluator_crate_version: "0.79.0".into(),
            evaluator_release_cache_input_sha256: vec![6; 32],
            primary_input_transition_ps: 10.0,
            module_output_load_ff: 1.0,
            source_lowering_mode: "default".into(),
            release_cache_input_sha256: vec![6; 32],
            source_recipe_revision: 1,
            ..Default::default()
        };
        baseline.run_spec_sha256 = crate::techmap_corpus::run_spec_digest(&baseline);
        baseline.samples.push(sample.clone());
        baseline.complete = true;
        let spec = ExperimentSpec {
            record_version: 1,
            experiment_id: "fictional-local-attempt".into(),
            baseline_manifest_sha256: digest(&baseline.encode_to_vec()),
            source_pipeline: PIPELINE.into(),
            mapper: "g8r".into(),
            goals: vec!["area".into(), "delay".into()],
            driver_image_id: baseline.driver_image_id.clone(),
            evaluator_image_id: baseline.evaluator_image_id.clone(),
            release_ready_sha256: vec![7; 32],
            merged_liberty_sha256: library.merged_liberty_sha256,
            liberty_proto_sha256: vec![8; 32],
            liberty_proto_record_sha256: vec![9; 32],
            library_bundle_sha256: baseline.liberty_bundle_sha256.clone(),
            primary_input_transition_ps: 10.0,
            module_output_load_ff: 1.0,
            expected_samples: 1,
            driver_crate_version: "0.79.0".into(),
            cohort_id: baseline.cohort_id.clone(),
            baseline_run_spec_sha256: baseline.run_spec_sha256.clone(),
            corpus_sha256: baseline.corpus_sha256.clone(),
            schema_descriptor_sha256: vec![10; 32],
            runner_sha256: vec![11; 32],
            equivalence_method: PROOF_METHOD.into(),
            equivalence_image_id: baseline.abc_image_id.clone(),
            prior_diagnostic_spec_sha256: vec![12; 32],
        };
        let mut result = SampleResult {
            record_version: 1,
            sample_id: sample.sample_id.clone(),
            experiment_spec_sha256: digest(&spec.encode_to_vec()),
            baseline: Some(sample.clone()),
            input_aig_sha256: sample.abc_aig_sha256.clone(),
            input_source_record_sha256: vec![13; 32],
            status: "done".into(),
            ..Default::default()
        };
        for goal in ["area", "delay"] {
            result.points.push(Metric {
                mapper: "g8r".into(),
                goal: goal.into(),
                source_pipeline: PIPELINE.into(),
                input_aig_sha256: sample.abc_aig_sha256.clone(),
                cell_area: 0.0,
                delay_ps: 0.0,
                cell_count: 0,
                mapped_netlist_sha256: vec![14; 32],
                external_stats_sha256: vec![15; 32],
                external_map_metrics_sha256: vec![16; 32],
                roundtrip_aig_sha256: vec![17; 32],
                equivalence_status: "proved".into(),
                equivalence_report_sha256: vec![18; 32],
                attempt_relpath: "private/fictional-operator/attempt".into(),
                status: "done".into(),
                error: String::new(),
            });
        }
        result.record_sha256 = sample_digest(&result);
        let mut run = ExperimentRun {
            record_version: 1,
            spec: Some(spec),
            samples: vec![result],
            complete: true,
            record_sha256: Vec::new(),
        };
        run.record_sha256 = run_digest(&run);
        (baseline, run)
    }

    fn public_baseline(run: &TechmapRun) -> PublicTechmapRun {
        let mut measurements = run.clone();
        measurements.samples = measurements
            .samples
            .iter()
            .map(public_baseline_sample)
            .collect();
        PublicTechmapRun {
            record_version: 1,
            source_run_sha256: digest(&run.encode_to_vec()),
            measurements: Some(measurements),
        }
    }
    fn reseal(run: &mut ExperimentRun) {
        let spec_digest = digest(&run.spec.as_ref().unwrap().encode_to_vec());
        for sample in &mut run.samples {
            sample.experiment_spec_sha256 = spec_digest.clone();
            sample.record_sha256 = sample_digest(sample);
        }
        run.record_sha256 = run_digest(run);
    }
    fn evidence(files: &BTreeMap<String, Vec<u8>>) -> BTreeMap<String, Vec<u8>> {
        files
            .iter()
            .filter(|(path, _)| path.starts_with("crossmap/"))
            .map(|(a, b)| (a.clone(), b.clone()))
            .collect()
    }
    fn validate_fixture(run: &ExperimentRun, baseline: &TechmapRun) -> Result<()> {
        let public = [public_baseline(baseline)];
        let selected = BTreeSet::from([digest(&baseline.encode_to_vec())]);
        validate_run(run, &baselines(&public, &selected)?, false)
    }

    #[test]
    fn projects_all_six_endpoints_and_redacts_every_private_locator() {
        let (baseline, run) = fixturepair();
        let dir = FixtureDir::new();
        let path = dir.0.join("manifest.pb");
        fs::write(&path, run.encode_to_vec()).unwrap();
        let retained = fs::read(&path).unwrap();
        let baselines = [public_baseline(&baseline)];
        let files = generate(&[path.clone()], &baselines).unwrap();
        assert_eq!(fs::read(path).unwrap(), retained);
        let public =
            PublicCrossmapRun::decode(evidence(&files).into_values().next().unwrap().as_slice())
                .unwrap();
        assert_eq!(public.source_run_sha256, digest(&retained));
        let projected = public.measurements.as_ref().unwrap();
        assert_eq!(
            projected.spec.as_ref().unwrap().experiment_id,
            public_id(&public.source_run_sha256)
        );
        assert_ne!(projected.record_sha256, run.record_sha256);
        for sample in &projected.samples {
            assert!(
                sample
                    .points
                    .iter()
                    .all(|point| point.attempt_relpath.is_empty())
            );
            assert!(
                sample
                    .baseline
                    .as_ref()
                    .unwrap()
                    .points
                    .iter()
                    .all(|point| point.mapped_netlist_relpath.is_empty())
            );
        }
        assert_eq!(
            regenerate_public(&evidence(&files), &baselines).unwrap(),
            files
        );
        let json: serde_json::Value = serde_json::from_slice(&files[RESULTS]).unwrap();
        assert_eq!(json["runs"][0]["verified_points"], 2);
        let points = json["runs"][0]["samples"][0]["points"].as_array().unwrap();
        assert_eq!(points.len(), 6);
        assert_eq!(points.iter().filter(|p| p["verified"] == true).count(), 2);
        assert_eq!(
            points
                .iter()
                .filter(|p| p["verified"] == true)
                .map(|p| (
                    p["source"].as_str().unwrap(),
                    p["mapper"].as_str().unwrap(),
                    p["goal"].as_str().unwrap(),
                    p["area"].as_f64().unwrap(),
                    p["delay_ps"].as_f64().unwrap()
                ))
                .collect::<Vec<_>>(),
            vec![
                ("yosys-abc", "g8r", "area", 0.0, 0.0),
                ("yosys-abc", "g8r", "delay", 0.0, 0.0)
            ]
        );
    }

    #[test]
    fn rejects_stale_digests_and_rehashed_baseline_or_input_changes() {
        let (baseline, run) = fixturepair();
        assert!(validate_fixture(&run, &baseline).is_ok());
        let mut bad = run.clone();
        bad.samples[0].points[0].cell_area = 1.0;
        assert!(validate_fixture(&bad, &baseline).is_err());
        let mut bad = run.clone();
        bad.samples[0].baseline.as_mut().unwrap().points[0].area = 1.0;
        let sample = bad.samples[0].baseline.as_mut().unwrap();
        sample.record_sha256 = baseline_sample_digest(sample);
        reseal(&mut bad);
        assert!(validate_fixture(&bad, &baseline).is_err());
        let mut bad = run.clone();
        bad.samples[0].input_aig_sha256 = vec![99; 32];
        reseal(&mut bad);
        assert!(validate_fixture(&bad, &baseline).is_err());
        let mut bad = run.clone();
        bad.spec.as_mut().unwrap().baseline_manifest_sha256 = vec![99; 32];
        reseal(&mut bad);
        assert!(validate_fixture(&bad, &baseline).is_err());
        let mut bad = run.clone();
        bad.samples.push(bad.samples[0].clone());
        reseal(&mut bad);
        assert!(validate_fixture(&bad, &baseline).is_err());
    }

    #[test]
    fn rejects_rehashed_incomplete_proofs_bad_metrics_and_incompatible_inputs() {
        let (baseline, run) = fixturepair();
        for change in 0..13 {
            let mut bad = run.clone();
            match change {
                0 => bad.complete = false,
                1 => bad.samples[0].status = "failed".into(),
                2 => bad.samples[0].points[0].equivalence_status = "unknown".into(),
                3 => bad.samples[0].points[0].equivalence_report_sha256.clear(),
                4 => bad.samples[0].points[0].cell_area = f64::NAN,
                5 => bad.samples[0].points[0].delay_ps = -1.0,
                6 => bad.spec.as_mut().unwrap().primary_input_transition_ps = 20.0,
                7 => {
                    bad.spec.as_mut().unwrap().evaluator_image_id =
                        format!("sha256:{}", "c".repeat(64))
                }
                8 => bad.spec.as_mut().unwrap().merged_liberty_sha256 = vec![99; 32],
                9 => bad.spec.as_mut().unwrap().mapper = "abc".into(),
                10 => bad.spec.as_mut().unwrap().source_pipeline = "yosys-without-abc".into(),
                11 => bad.samples[0].points[0].goal = "delay".into(),
                12 => bad.samples[0].points[0].error = "tool failed".into(),
                _ => unreachable!(),
            }
            reseal(&mut bad);
            assert!(
                validate_fixture(&bad, &baseline).is_err(),
                "mutation {change}"
            );
        }
    }

    #[test]
    fn public_verifier_rejects_rehashed_private_locators_and_noncanonical_bytes() {
        let (baseline, run) = fixturepair();
        let baselines = [public_baseline(&baseline)];
        let public = project_run(&run);
        let path = format!("crossmap/{}.pb", public_id(&public.source_run_sha256));
        for attempt in [false, true] {
            let mut bad = public.clone();
            let measurements = bad.measurements.as_mut().unwrap();
            if attempt {
                measurements.samples[0].points[0].attempt_relpath =
                    "private/fictional-operator".into();
            } else {
                let baseline = measurements.samples[0].baseline.as_mut().unwrap();
                baseline.points[0].mapped_netlist_relpath = "private/fictional-operator".into();
                baseline.record_sha256 = baseline_sample_digest(baseline);
            }
            reseal(measurements);
            assert!(
                regenerate_public(
                    &BTreeMap::from([(path.clone(), bad.encode_to_vec())]),
                    &baselines
                )
                .is_err()
            );
        }
        let mut noncanonical = public.encode_to_vec();
        noncanonical.extend_from_slice(&[0x98, 0x06, 0x01]);
        assert!(
            regenerate_public(&BTreeMap::from([(path.clone(), noncanonical)]), &baselines).is_err()
        );
        assert!(
            regenerate_public(
                &BTreeMap::from([("crossmap/other.pb".into(), public.encode_to_vec())]),
                &baselines
            )
            .is_err()
        );
        assert!(generated_files(vec![public.clone(), public], &baselines).is_err());
    }

    #[test]
    fn empty_supplement_does_not_decode_unrelated_baseline_samples() {
        let empty = generate(&[], &[PublicTechmapRun::default()]).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&empty[RESULTS]).unwrap();
        assert_eq!(value, serde_json::json!({"schema_version":1,"runs":[]}));
        assert_eq!(empty.len(), 1);
    }
    #[test]
    fn rejects_noncanonical_source_records_and_oversize_evidence() {
        let (baseline, run) = fixturepair();
        let dir = FixtureDir::new();
        let path = dir.0.join("manifest.pb");
        let baselines = [public_baseline(&baseline)];
        let mut bytes = run.encode_to_vec();
        bytes.extend_from_slice(&[0x98, 0x06, 0x01]);
        fs::write(&path, bytes).unwrap();
        assert!(generate(&[path.clone()], &baselines).is_err());
        fs::write(&path, vec![0; MAX_BYTES + 1]).unwrap();
        assert_eq!(
            generate(&[path], &baselines).unwrap_err().to_string(),
            "same-AIG input experiment exceeds the 2 MiB evidence budget; sharding is required"
        );
        assert!(
            regenerate_public(
                &BTreeMap::from([("crossmap/oversize.pb".into(), vec![0; MAX_BYTES + 1])]),
                &baselines
            )
            .is_err()
        );
    }
}
