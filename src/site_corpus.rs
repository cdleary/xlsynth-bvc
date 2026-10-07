// SPDX-License-Identifier: Apache-2.0

//! Completed corpus workspaces -> verified protobuf evidence -> bounded web assets.
//! This mode does not enqueue, refresh, or execute any evaluation work.

use super::*;

mod wire {
    include!(concat!(env!("OUT_DIR"), "/xlsynth.bvc.corpus_site.v1.rs"));
}

pub(super) const MANIFEST: &str = "corpus-site.pb";
const MAX_ASSET_BYTES: u64 = 25 * 1024 * 1024;
// Reserve room for the publication pointer and wrapper files.
const MAX_SITE_FILES: usize = 19_990;
const MAX_SHARD_TARGET: usize = 16 * 1024 * 1024;
const JS: &str = include_str!("site_assets/corpus.js");
const CSS: &str = include_str!("site_assets/corpus.css");
const HTML: &str = include_str!("site_assets/corpus.html");

#[derive(Deserialize)]
struct DiffManifest {
    samples: Vec<DiffInput>,
}

#[derive(Deserialize)]
struct DiffInput {
    sample_id: String,
    aig_stat_diff_action_id: String,
    aig_stat_diff_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Metric {
    sample_id: String,
    source_relpath: String,
    source_sha256: String,
    top_fn_name: String,
    ir_node_count: u64,
    g8r_nodes: u64,
    g8r_depth: u64,
    g8r_le: Option<f64>,
    yosys_nodes: u64,
    yosys_depth: u64,
    yosys_le: Option<f64>,
    raw_nodes: u64,
    raw_depth: u64,
    raw_le: Option<f64>,
}

#[derive(Debug, Serialize)]
struct Catalog<'a> {
    schema_version: u32,
    cohort_sha256: String,
    sample_count: u64,
    generations: Vec<GenerationView<'a>>,
}

#[derive(Debug, Serialize)]
struct GenerationView<'a> {
    crate_version: &'a str,
    dso_version: &'a str,
    shards: Vec<ShardView>,
}

#[derive(Debug, Serialize)]
struct ShardView {
    metrics: String,
    ir: String,
    sample_count: u64,
}

fn required<'a, T>(value: &'a Option<T>, label: &str) -> Result<&'a T> {
    value
        .as_ref()
        .with_context(|| format!("missing corpus evidence {label}"))
}

fn id(action: &ActionSpec) -> Result<String> {
    crate::executor::compute_action_id(action)
}

fn canonical_version(version: &str) -> Result<()> {
    if !Regex::new(r"^[0-9]+\.[0-9]+\.[0-9]+$")?.is_match(version) {
        bail!("corpus site requires a canonical numeric release version");
    }
    Ok(())
}

fn input_identity(row: &ReleaseSampleInput) -> Result<wire::InputIdentity> {
    normalized_relpath(&row.source_relpath)?;
    normalized_relpath(&row.sample_id)?;
    if row.sample_id != crate::corpus::sample_id_for_relpath(&row.source_relpath)
        || row.top_fn_name.is_empty()
    {
        bail!("corpus sample identity does not match its relative path/top function");
    }
    Ok(wire::InputIdentity {
        source_relpath: row.source_relpath.clone(),
        source_sha256: crate::proto::digest_from_hex(&row.source_sha256, "source_sha256")?.value,
        top_fn_name: row.top_fn_name.clone(),
    })
}

fn cohort_identity(inputs: impl IntoIterator<Item = wire::InputIdentity>) -> Result<Vec<u8>> {
    let mut inputs: Vec<_> = inputs.into_iter().collect();
    inputs.sort_by(|a, b| a.source_relpath.cmp(&b.source_relpath));
    if inputs.is_empty()
        || inputs
            .windows(2)
            .any(|w| w[0].source_relpath == w[1].source_relpath)
    {
        bail!("corpus inputs must be nonempty and have unique relative paths");
    }
    let bytes = wire::CohortIdentity { inputs }.encode_to_vec();
    Ok(
        Sha256::digest([b"xlsynth-bvc/corpus-site/cohort/v1\0".as_slice(), &bytes].concat())
            .to_vec(),
    )
}

struct Actions {
    ancestors: Vec<ActionSpec>,
    stats: [ActionSpec; 3],
}

fn actions(
    input: &wire::InputIdentity,
    dso: &str,
    driver: &DriverRuntimeSpec,
    stats_driver: &DriverRuntimeSpec,
    yosys: &YosysRuntimeSpec,
    script: &ScriptRef,
) -> Result<Actions> {
    let import = ActionSpec::ImportIrPackageFile {
        source_sha256: hex::encode(&input.source_sha256),
        top_fn_name: Some(input.top_fn_name.clone()),
    };
    let lowering = ActionSpec::DriverIrToG8rAig {
        ir_action_id: id(&import)?,
        top_fn_name: Some(input.top_fn_name.clone()),
        fraig: false,
        lowering_mode: G8rLoweringMode::FrontendNoPrepRewrite,
        execution_recipe_revision: crate::versioning::driver_ir2g8r_execution_recipe_revision(
            &driver.driver_version,
        ),
        version: dso.to_string(),
        runtime: driver.clone(),
    };
    let abc = ActionSpec::AigToYosysAbcAig {
        aig_action_id: id(&lowering)?,
        yosys_script_ref: script.clone(),
        runtime: yosys.clone(),
    };
    let combo = ActionSpec::IrFnToCombinationalVerilog {
        ir_action_id: id(&import)?,
        top_fn_name: Some(input.top_fn_name.clone()),
        use_system_verilog: false,
        version: dso.to_string(),
        runtime: driver.clone(),
    };
    let reference = ActionSpec::ComboVerilogToYosysAbcAig {
        verilog_action_id: id(&combo)?,
        verilog_top_module_name: Some(input.top_fn_name.clone()),
        frontend: YosysVerilogFrontend::Builtin,
        yosys_script_ref: script.clone(),
        runtime: yosys.clone(),
    };
    let stats = [&abc, &reference, &lowering]
        .map(|a| {
            Ok(ActionSpec::DriverAigToStats {
                aig_action_id: id(a)?,
                version: dso.to_string(),
                runtime: stats_driver.clone(),
            })
        })
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    let diff = ActionSpec::AigStatDiff {
        opt_ir_action_id: id(&import)?,
        g8r_aig_stats_action_id: id(&stats[0])?,
        yosys_abc_aig_stats_action_id: id(&stats[1])?,
    };
    Ok(Actions {
        ancestors: vec![import, lowering, abc, combo, reference, diff],
        stats: stats.try_into().expect("three stats actions"),
    })
}

fn checked_provenance(
    store: &ArtifactStore,
    action: &ActionSpec,
) -> Result<crate::model::Provenance> {
    let expected = id(action)?;
    let p = store.load_provenance(&expected)?;
    if p.action_id != expected
        || id(&p.action)? != expected
        || p.output_artifact.action_id != expected
    {
        bail!("corpus action provenance does not match its content-addressed identity");
    }
    Ok(p)
}

fn stats_from_export(
    run: &Path,
    store: &ArtifactStore,
    sample_id: &str,
    action: &ActionSpec,
    name: &str,
) -> Result<wire::Stats> {
    let (_, s) = read_verified_progression_stats_with_evidence(
        run,
        store,
        sample_id,
        &id(action)?,
        action,
        name,
    )?;
    for n in [s.and_nodes, s.depth] {
        // JavaScript plots must retain exact integer counts.
        if n.fract() != 0.0 || !(0.0..=9_007_199_254_740_991.0).contains(&n) {
            bail!("corpus metric is not an exactly representable nonnegative integer");
        }
    }
    Ok(wire::Stats {
        and_nodes: s.and_nodes as u64,
        depth: s.depth as u64,
        graph_logical_effort: s.graph_logical_effort,
        output_bytes: s.source_output_bytes,
        output_sha256: hex::decode(s.source_output_sha256)?,
        action: Some(crate::proto::action_spec_to_proto(action)?),
    })
}

fn ingest(run: &Path) -> Result<(wire::Generation, Vec<wire::Sample>)> {
    let manifest_bytes = fs::read(run.join("manifest.json"))?;
    let manifest: ReleaseCorpusManifestInput = serde_json::from_slice(&manifest_bytes)?;
    let diffs: DiffManifest = serde_json::from_slice(&manifest_bytes)?;
    if manifest.schema_version != 5
        || manifest.recipe_preset != "g8r-abc-vs-yabc-aig-diff"
        || manifest.fraig
        || manifest.candidate_run.is_some()
        || manifest.samples.is_empty()
    {
        bail!("--corpus-run-dir requires a complete released g8r-abc-vs-yabc-aig-diff corpus");
    }
    crate::corpus::validate_candidate_run_marker_provenance(run, None)?;
    canonical_version(&manifest.driver_runtime.driver_version)?;
    validate_release_progression_driver_runtime(&manifest.driver_runtime)?;
    validate_release_progression_driver_runtime(&manifest.stats_runtime)?;
    validate_release_progression_yosys_runtime(&manifest.yosys_runtime)?;
    let dso = normalize_tag_version(&manifest.dso_version).to_string();
    canonical_version(&dso)?;
    if manifest.yosys_script != crate::DEFAULT_YOSYS_FLOW_SCRIPT {
        bail!("corpus run has an unsupported Yosys script");
    }
    let script = ScriptRef {
        path: manifest.yosys_script.clone(),
        sha256: manifest.yosys_script_sha256.clone(),
    };
    crate::proto::digest_from_hex(&script.sha256, "Yosys script")?;
    let inputs = manifest
        .samples
        .iter()
        .map(input_identity)
        .collect::<Result<Vec<_>>>()?;
    let cohort_sha256 = cohort_identity(inputs.iter().cloned())?;
    // Opening the canonical store fails if its worker still owns the DB. No
    // queue records, store markers, or materialization caches are rewritten.
    let store = ArtifactStore::new_with_sled(
        run.join(".bvc/bvc-artifacts"),
        run.join(".bvc/artifacts.sled"),
    );
    let mut samples = Vec::with_capacity(inputs.len());
    for ((row, input), diff) in manifest.samples.iter().zip(inputs).zip(diffs.samples) {
        if row.status != "done"
            || row.import_ir_status != "done"
            || row.g8r_aig_status != "done"
            || row.g8r_abc_aig_status.as_deref() != Some("done")
            || row.g8r_stats_status != "done"
            || row.g8r_raw_stats_status.as_deref() != Some("done")
            || row.combo_verilog_status != "done"
            || row.yosys_abc_aig_status != "done"
            || row.yosys_abc_stats_status != "done"
            || diff.aig_stat_diff_status != "done"
            || diff.sample_id != row.sample_id
            || row.fraig
            || normalize_tag_version(&row.dso_version) != dso
            || row.driver_crate_version != manifest.driver_runtime.driver_version
            || row.stats_driver_crate_version != manifest.stats_runtime.driver_version
        {
            bail!(
                "corpus sample {} is incomplete or has inconsistent release identity; finalize the run first",
                row.sample_id
            );
        }
        let graph = actions(
            &input,
            &dso,
            &manifest.driver_runtime,
            &manifest.stats_runtime,
            &manifest.yosys_runtime,
            &script,
        )?;
        let expected_ids = [
            row.import_ir_action_id.as_str(),
            row.g8r_aig_action_id.as_str(),
            row.g8r_abc_aig_action_id
                .as_deref()
                .context("missing G8r ABC action")?,
            row.combo_verilog_action_id.as_str(),
            row.yosys_abc_aig_action_id.as_str(),
            diff.aig_stat_diff_action_id.as_str(),
        ];
        for (a, expected) in graph.ancestors.iter().zip(expected_ids) {
            if id(a)? != expected {
                bail!("corpus action graph does not match its manifest");
            }
            checked_provenance(&store, a)?;
        }
        for (a, expected) in graph.stats.iter().zip([
            row.g8r_stats_action_id.as_str(),
            row.yosys_abc_stats_action_id.as_str(),
            row.g8r_raw_stats_action_id
                .as_deref()
                .context("missing raw G8r stats")?,
        ]) {
            if id(a)? != expected {
                bail!("corpus stats action identity mismatch");
            }
        }
        let input_ir =
            fs::read_to_string(run.join("artifacts").join(&row.sample_id).join("input.ir"))?;
        if Sha256::digest(input_ir.as_bytes()).as_slice() != input.source_sha256 {
            bail!("exported corpus input IR checksum mismatch");
        }
        let sample = wire::Sample {
            input: Some(input),
            sample_id: row.sample_id.clone(),
            input_ir,
            g8r_abc: Some(stats_from_export(
                run,
                &store,
                &row.sample_id,
                &graph.stats[0],
                "g8r_stats.json",
            )?),
            yosys_abc: Some(stats_from_export(
                run,
                &store,
                &row.sample_id,
                &graph.stats[1],
                "yosys_abc_stats.json",
            )?),
            g8r_raw: Some(stats_from_export(
                run,
                &store,
                &row.sample_id,
                &graph.stats[2],
                "g8r_raw_stats.json",
            )?),
            actions: graph
                .ancestors
                .iter()
                .map(crate::proto::action_spec_to_proto)
                .collect::<Result<_>>()?,
        };
        validate_sample(&sample, &manifest.driver_runtime.driver_version, &dso)?;
        samples.push(sample);
    }
    samples.sort_by(|a, b| a.sample_id.cmp(&b.sample_id));
    if samples.len() != manifest.samples.len()
        || samples.windows(2).any(|w| w[0].sample_id == w[1].sample_id)
    {
        bail!("corpus sample population is incomplete or duplicated");
    }
    let generation = wire::Generation {
        crate_version: manifest.driver_runtime.driver_version,
        dso_version: dso,
        cohort_sha256,
        sample_count: samples.len() as u64,
        shards: vec![],
    };
    Ok((generation, samples))
}

fn validate_sample(
    sample: &wire::Sample,
    version: &str,
    dso: &str,
) -> Result<ProgressionRuntimeIdentity> {
    let input = required(&sample.input, "input")?;
    normalized_relpath(&input.source_relpath)?;
    if sample.sample_id != crate::corpus::sample_id_for_relpath(&input.source_relpath)
        || input.top_fn_name.is_empty()
        || input.source_sha256.len() != 32
        || Sha256::digest(sample.input_ir.as_bytes()).as_slice() != input.source_sha256
        || sample.actions.len() != 6
    {
        bail!("invalid corpus sample/input evidence");
    }
    let actual = sample
        .actions
        .iter()
        .map(crate::proto::action_spec_from_proto)
        .collect::<Result<Vec<_>>>()?;
    let driver = match &actual[1] {
        ActionSpec::DriverIrToG8rAig { runtime, .. } => runtime,
        _ => bail!("corpus evidence is missing G8r lowering"),
    };
    let (yosys, script) = match &actual[2] {
        ActionSpec::AigToYosysAbcAig {
            runtime,
            yosys_script_ref,
            ..
        } => (runtime, yosys_script_ref),
        _ => bail!("corpus evidence is missing G8r ABC"),
    };
    let stats = [
        required(&sample.g8r_abc, "G8r stats")?,
        required(&sample.yosys_abc, "Yosys stats")?,
        required(&sample.g8r_raw, "raw G8r stats")?,
    ];
    let stats_action = required_progression_action(&stats[0].action, "stats action")?;
    let stats_driver = match &stats_action {
        ActionSpec::DriverAigToStats { runtime, .. } => runtime,
        _ => bail!("corpus evidence is missing its stats runtime"),
    };
    validate_release_progression_driver_runtime(driver)?;
    validate_release_progression_driver_runtime(stats_driver)?;
    validate_release_progression_yosys_runtime(yosys)?;
    if driver.driver_version != version || script.path != crate::DEFAULT_YOSYS_FLOW_SCRIPT {
        bail!("corpus evidence runtime does not match its generation");
    }
    crate::proto::digest_from_hex(&script.sha256, "Yosys script")?;
    let expected = actions(input, dso, driver, stats_driver, yosys, script)?;
    for (a, b) in actual.iter().zip(&expected.ancestors) {
        if id(a)? != id(b)? {
            bail!("corpus evidence has an invalid action graph");
        }
    }
    for (s, a) in stats.iter().zip(&expected.stats) {
        if s.output_bytes == 0
            || s.output_sha256.len() != 32
            || s.and_nodes > 9_007_199_254_740_991
            || s.depth > 9_007_199_254_740_991
            || s.and_nodes
                .checked_mul(s.depth)
                .is_none_or(|n| n > 9_007_199_254_740_991)
            || s.graph_logical_effort
                .is_some_and(|v| !v.is_finite() || v < 0.0)
            || id(&required_progression_action(&s.action, "stats action")?)? != id(a)?
        {
            bail!("corpus evidence has invalid statistics");
        }
    }
    Ok(ProgressionRuntimeIdentity {
        stats_runtime: stats_driver.clone(),
        yosys_runtime: yosys.clone(),
        yosys_script: script.path.clone(),
        yosys_script_sha256: script.sha256.clone(),
    })
}

fn project(samples: &[wire::Sample]) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut metrics = Vec::new();
    let mut ir = BTreeMap::new();
    for sample in samples {
        let input = required(&sample.input, "input")?;
        let g = required(&sample.g8r_abc, "G8r stats")?;
        let y = required(&sample.yosys_abc, "Yosys stats")?;
        let r = required(&sample.g8r_raw, "raw stats")?;
        let nodes =
            crate::service::parse_ir_fn_node_count_by_name(&sample.input_ir, &input.top_fn_name)
                .context("corpus input does not contain its declared top function")?;
        metrics.push(Metric {
            sample_id: sample.sample_id.clone(),
            source_relpath: input.source_relpath.clone(),
            source_sha256: hex::encode(&input.source_sha256),
            top_fn_name: input.top_fn_name.clone(),
            ir_node_count: nodes,
            g8r_nodes: g.and_nodes,
            g8r_depth: g.depth,
            g8r_le: g.graph_logical_effort,
            yosys_nodes: y.and_nodes,
            yosys_depth: y.depth,
            yosys_le: y.graph_logical_effort,
            raw_nodes: r.and_nodes,
            raw_depth: r.depth,
            raw_le: r.graph_logical_effort,
        });
        if ir.insert(&sample.sample_id, &sample.input_ir).is_some() {
            bail!("duplicate sample ID in corpus shard");
        }
    }
    Ok((serde_json::to_vec(&metrics)?, serde_json::to_vec(&ir)?))
}

fn fixed_files() -> Vec<(&'static str, &'static [u8])> {
    vec![
        ("index.html", HTML.as_bytes()),
        ("assets/corpus.js", JS.as_bytes()),
        ("assets/corpus.css", CSS.as_bytes()),
        ("assets/plotly-2.35.2.min.js", PLOTLY_JS),
        ("assets/plotly-2.35.2.LICENSE.txt", PLOTLY_LICENSE),
        ("assets/plotly-2.35.2.min.js.LICENSE.txt", PLOTLY_NOTICE),
    ]
}

fn write_shards(
    out: &Path,
    generation: &mut wire::Generation,
    samples: &[wire::Sample],
    target: usize,
) -> Result<()> {
    let evidence = wire::EvidenceShard {
        samples: samples.to_vec(),
    }
    .encode_to_vec();
    let (metrics, ir) = project(samples)?;
    if [evidence.len(), metrics.len(), ir.len()]
        .into_iter()
        .any(|n| n > target)
    {
        if samples.len() <= 1 {
            bail!("one corpus sample exceeds the shard byte target; increase --corpus-shard-bytes");
        }
        let (left, right) = samples.split_at(samples.len() / 2);
        write_shards(out, generation, left, target)?;
        return write_shards(out, generation, right, target);
    }
    let prefix = format!(
        "data/{}/{}",
        generation.crate_version,
        generation.shards.len()
    );
    let files = [("pb", evidence), ("json", metrics), ("ir.json", ir)]
        .into_iter()
        .map(|(suffix, bytes)| {
            let relpath = format!("{prefix}.{suffix}");
            write_file(out, &relpath, &bytes)?;
            publication_file(out, &relpath)
        })
        .collect::<Result<Vec<_>>>()?;
    generation.shards.push(wire::Shard {
        sample_count: samples.len() as u64,
        evidence: Some(files[0].clone()),
        metrics: Some(files[1].clone()),
        ir: Some(files[2].clone()),
    });
    Ok(())
}

fn path(file: &Option<pb::PublicationFile>) -> Result<String> {
    let file = required(file, "shard file")?;
    let relpath = &required(&file.relpath, "shard path")?.value;
    normalized_relpath(relpath)?;
    Ok(relpath.clone())
}

fn catalog(site: &wire::CorpusSite) -> Result<Vec<u8>> {
    let first = site.generations.first().context("empty corpus site")?;
    let generations = site
        .generations
        .iter()
        .map(|g| {
            Ok(GenerationView {
                crate_version: &g.crate_version,
                dso_version: &g.dso_version,
                shards: g
                    .shards
                    .iter()
                    .map(|s| {
                        Ok(ShardView {
                            metrics: path(&s.metrics)?,
                            ir: path(&s.ir)?,
                            sample_count: s.sample_count,
                        })
                    })
                    .collect::<Result<_>>()?,
            })
        })
        .collect::<Result<_>>()?;
    Ok(serde_json::to_vec(&Catalog {
        schema_version: 1,
        cohort_sha256: hex::encode(&first.cohort_sha256),
        sample_count: first.sample_count,
        generations,
    })?)
}

pub(crate) fn build(
    options: &BuildStaticSiteOptions,
    protected: &[(&str, &Path)],
    runs: &[PathBuf],
    target: usize,
) -> Result<BuildStaticSiteSummary> {
    if runs.is_empty() || target == 0 || target > MAX_SHARD_TARGET {
        bail!("provide completed --corpus-run-dir inputs and a shard target in 1..=16 MiB");
    }
    for run in runs {
        reject_site_output_overlap(&options.out_dir, run, protected)?;
    }
    normalize_base_url(&options.base_url)?;
    build_static_site_atomically(options, |staging| build_in_place(staging, runs, target))
}

fn build_in_place(
    options: &BuildStaticSiteOptions,
    runs: &[PathBuf],
    target: usize,
) -> Result<BuildStaticSiteSummary> {
    let mut site = wire::CorpusSite {
        record_version: 1,
        generations: vec![],
        shard_target_bytes: target as u64,
    };
    for run in runs {
        eprintln!("Validating completed corpus run {}", run.display());
        let (mut generation, samples) = ingest(run)?;
        if site
            .generations
            .iter()
            .any(|g| g.crate_version == generation.crate_version)
        {
            bail!("duplicate corpus release input");
        }
        if site
            .generations
            .first()
            .is_some_and(|g| g.cohort_sha256 != generation.cohort_sha256)
        {
            bail!("corpus releases do not contain the same exact inputs");
        }
        write_shards(&options.out_dir, &mut generation, &samples, target)?;
        site.generations.push(generation);
    }
    site.generations
        .sort_by(|a, b| cmp_dotted_numeric_version(&a.crate_version, &b.crate_version));
    for (name, bytes) in fixed_files() {
        write_file(&options.out_dir, name, bytes)?;
    }
    write_file(&options.out_dir, "catalog.json", &catalog(&site)?)?;
    let bytes = site.encode_to_vec();
    let snapshot_id = sha256_hex(&bytes);
    write_file(&options.out_dir, MANIFEST, &bytes)?;
    let files = actual_site_relpaths(&options.out_dir)?
        .iter()
        .map(|p| publication_file(&options.out_dir, p))
        .collect::<Result<Vec<_>>>()?;
    let total_bytes = files.iter().map(|f| f.bytes).sum();
    let manifest = pb::StaticSiteManifest {
        record_version: STATIC_SITE_RECORD_VERSION,
        source_snapshot_id: Some(crate::proto::digest_from_hex(
            &snapshot_id,
            "corpus_site_id",
        )?),
        base_url: normalize_base_url(&options.base_url)?,
        files,
    };
    write_file(
        &options.out_dir,
        STATIC_SITE_MANIFEST_FILENAME,
        &manifest.encode_to_vec(),
    )?;
    verify_static_site(&options.out_dir)?;
    Ok(BuildStaticSiteSummary {
        out_dir: options.out_dir.display().to_string(),
        snapshot_id,
        base_url: manifest.base_url,
        dataset_count: site.generations.len(),
        file_count: manifest.files.len(),
        total_bytes,
    })
}

pub(super) fn verify_hosting_budget(site_dir: &Path) -> Result<()> {
    let mut count = 0;
    for entry in WalkDir::new(site_dir).follow_links(false) {
        let entry = entry?;
        if !entry.file_type().is_dir() && !entry.file_type().is_file() {
            bail!("static site contains a symlink or special file");
        }
        if entry.file_type().is_file() {
            count += 1;
            if entry.metadata()?.len() > MAX_ASSET_BYTES {
                bail!(
                    "site asset exceeds the 25 MiB hosting budget: {}",
                    entry.path().display()
                );
            }
        }
    }
    if count > MAX_SITE_FILES {
        bail!("site exceeds the {MAX_SITE_FILES}-file hosting budget");
    }
    Ok(())
}

pub(super) fn verify_projection(site_dir: &Path, manifest: &pb::StaticSiteManifest) -> Result<()> {
    let bytes = fs::read(site_dir.join(MANIFEST))?;
    let site = wire::CorpusSite::decode(bytes.as_slice())?;
    if site.encode_to_vec() != bytes
        || site.record_version != 1
        || site.generations.is_empty()
        || site.shard_target_bytes == 0
        || site.shard_target_bytes > MAX_SHARD_TARGET as u64
        || sha256_hex(&bytes) != digest_hex(&manifest.source_snapshot_id, "corpus site ID")?
    {
        bail!("invalid corpus site protobuf identity");
    }
    let mut expected = BTreeSet::from([MANIFEST.to_string(), "catalog.json".to_string()]);
    for (name, contents) in fixed_files() {
        expected.insert(name.to_string());
        if fs::read(site_dir.join(name))? != contents {
            bail!("corpus static asset differs from its compiled template");
        }
    }
    if fs::read(site_dir.join("catalog.json"))? != catalog(&site)? {
        bail!("corpus browser catalog does not match protobuf evidence");
    }
    let mut versions = BTreeSet::new();
    let mut common_runtime: Option<ProgressionRuntimeIdentity> = None;
    for generation in &site.generations {
        canonical_version(&generation.crate_version)?;
        canonical_version(&generation.dso_version)?;
        if !versions.insert(&generation.crate_version)
            || generation.cohort_sha256 != site.generations[0].cohort_sha256
        {
            bail!("duplicate release or mismatched corpus");
        }
        let mut inputs = Vec::new();
        let mut prior_id = None;
        let mut generation_driver = None;
        for (ordinal, shard) in generation.shards.iter().enumerate() {
            let mut file_bytes = Vec::new();
            for (record, suffix) in [
                (&shard.evidence, "pb"),
                (&shard.metrics, "json"),
                (&shard.ir, "ir.json"),
            ] {
                let record = required(record, "shard")?;
                let relpath = required(&record.relpath, "shard path")?.value.clone();
                if relpath != format!("data/{}/{ordinal}.{suffix}", generation.crate_version)
                    || record.bytes > site.shard_target_bytes
                    || !expected.insert(relpath.clone())
                    || !manifest.files.iter().any(|file| file == record)
                {
                    bail!("invalid corpus shard reference");
                }
                file_bytes.push(fs::read(site_dir.join(relpath))?);
            }
            let evidence = wire::EvidenceShard::decode(file_bytes[0].as_slice())?;
            if evidence.encode_to_vec() != file_bytes[0]
                || evidence.samples.is_empty()
                || evidence.samples.len() as u64 != shard.sample_count
            {
                bail!("invalid corpus shard protobuf");
            }
            for sample in &evidence.samples {
                if prior_id.as_ref().is_some_and(|id| id >= &sample.sample_id) {
                    bail!("corpus samples are duplicated or unsorted");
                }
                prior_id = Some(sample.sample_id.clone());
                let runtime =
                    validate_sample(sample, &generation.crate_version, &generation.dso_version)?;
                let ActionSpec::DriverIrToG8rAig {
                    runtime: driver, ..
                } = crate::proto::action_spec_from_proto(&sample.actions[1])?
                else {
                    bail!("missing corpus lowering runtime");
                };
                if generation_driver
                    .as_ref()
                    .is_some_and(|prior| prior != &driver)
                {
                    bail!("corpus generation mixes lowering runtimes");
                }
                generation_driver = Some(driver);
                if common_runtime.as_ref().is_some_and(|r| r != &runtime) {
                    bail!(
                        "corpus comparisons use inconsistent stats estimators or Yosys runtimes/scripts"
                    );
                }
                common_runtime = Some(runtime);
                inputs.push(required(&sample.input, "input")?.clone());
            }
            let (metrics, ir) = project(&evidence.samples)?;
            if metrics != file_bytes[1] || ir != file_bytes[2] {
                bail!("corpus browser projection differs from protobuf evidence");
            }
        }
        if inputs.len() as u64 != generation.sample_count
            || cohort_identity(inputs)? != generation.cohort_sha256
        {
            bail!("corpus evidence does not cover its complete input population");
        }
    }
    if !site
        .generations
        .windows(2)
        .all(|w| cmp_dotted_numeric_version(&w[0].crate_version, &w[1].crate_version).is_lt())
    {
        bail!("corpus generations are unsorted");
    }
    if actual_site_relpaths(site_dir)? != expected {
        bail!("corpus site has unexpected or missing files");
    }
    Ok(())
}

#[cfg(test)]
#[path = "site_corpus/tests.rs"]
mod tests;
