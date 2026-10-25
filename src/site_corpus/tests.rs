// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::model::{ArtifactRef, OutputFile, Provenance};
use serde_json::json;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "bvc-corpus-site-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        Self(root)
    }
    fn options(&self) -> BuildStaticSiteOptions {
        BuildStaticSiteOptions {
            snapshot_dir: PathBuf::new(),
            out_dir: self.0.join("site"),
            base_url: "/nested/".into(),
            overwrite: false,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn driver(version: &str) -> DriverRuntimeSpec {
    DriverRuntimeSpec {
        driver_version: version.into(),
        source_revision: None,
        release_platform: crate::DEFAULT_RELEASE_PLATFORM.into(),
        docker_image: crate::runtime::default_driver_image(version),
        dockerfile: crate::DEFAULT_DOCKERFILE.into(),
        docker_image_id: "a".repeat(64),
        dockerfile_sha256: "b".repeat(64),
        release_cache_input_sha256: "c".repeat(64),
    }
}

fn fixture_run(root: &Path, version: &str, count: usize) -> PathBuf {
    let run = root.join(version);
    let store = ArtifactStore::new_with_sled(
        run.join(".bvc/bvc-artifacts"),
        run.join(".bvc/artifacts.sled"),
    );
    store.ensure_layout().unwrap();
    let lowering = driver(version);
    let estimator = driver("0.74.0");
    let yosys = YosysRuntimeSpec {
        docker_image: crate::DEFAULT_YOSYS_DOCKER_IMAGE.into(),
        dockerfile: crate::DEFAULT_YOSYS_DOCKERFILE.into(),
        docker_image_id: "d".repeat(64),
        dockerfile_sha256: "e".repeat(64),
        upstream_commit: Some(crate::DEFAULT_YOSYS_UPSTREAM_COMMIT.into()),
        slang_commit: None,
    };
    let script = ScriptRef {
        path: crate::DEFAULT_YOSYS_FLOW_SCRIPT.into(),
        sha256: "f".repeat(64),
    };
    let mut rows = Vec::new();
    for n in 0..count {
        let ir = format!(
            "package test\ntop fn f{n}(x: bits[1] id=1) -> bits[1] {{\n ret not.2: bits[1] = not(x, id=2)\n}}\n"
        );
        let input = wire::InputIdentity {
            source_relpath: format!("input{n}.ir"),
            source_sha256: Sha256::digest(ir.as_bytes()).to_vec(),
            top_fn_name: format!("f{n}"),
        };
        let sample_id = crate::corpus::sample_id_for_relpath(&input.source_relpath);
        let graph = actions(&input, "0.59.0", &lowering, &estimator, &yosys, &script).unwrap();
        let directory = run.join("artifacts").join(&sample_id);
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("input.ir"), &ir).unwrap();
        for (i, action) in graph.ancestors.iter().chain(&graph.stats).enumerate() {
            let stats = serde_json::to_vec(&json!({"and_nodes": n + i + 1, "depth":2, "graph_logical_effort_worst_case_delay":3.25})).unwrap();
            let action_id = id(action).unwrap();
            store
                .write_provenance(&Provenance {
                    schema_version: crate::ACTION_SCHEMA_VERSION,
                    action_id: action_id.clone(),
                    created_utc: chrono::Utc::now(),
                    action: action.clone(),
                    dependencies: vec![],
                    output_artifact: ArtifactRef {
                        action_id,
                        artifact_type: ArtifactType::AigStatsFile,
                        relpath: crate::corpus::G8R_STATS_RELPATH.into(),
                    },
                    output_files: vec![OutputFile {
                        path: "stats.json".into(),
                        bytes: stats.len() as u64,
                        sha256: sha256_hex(&stats),
                    }],
                    commands: vec![],
                    details: json!({}),
                    suggested_next_actions: vec![],
                })
                .unwrap();
            if i >= 6 {
                fs::write(
                    directory.join(
                        [
                            "g8r_stats.json",
                            "yosys_abc_stats.json",
                            "g8r_raw_stats.json",
                        ][i - 6],
                    ),
                    &stats,
                )
                .unwrap();
            }
        }
        let ancestors = graph
            .ancestors
            .iter()
            .map(|a| id(a).unwrap())
            .collect::<Vec<_>>();
        let stats = graph
            .stats
            .iter()
            .map(|a| id(a).unwrap())
            .collect::<Vec<_>>();
        rows.push(json!({
            "sample_id":sample_id, "source_relpath":input.source_relpath, "source_sha256":hex::encode(input.source_sha256), "top_fn_name":input.top_fn_name,
            "fraig":false,"dso_version":"v0.59.0","driver_crate_version":version,"stats_driver_crate_version":"0.74.0","status":"done",
            "import_ir_action_id":ancestors[0],"import_ir_status":"done",
            "g8r_aig_action_id":ancestors[1],"g8r_aig_status":"done",
            "g8r_abc_aig_action_id":ancestors[2],"g8r_abc_aig_status":"done",
            "combo_verilog_action_id":ancestors[3],"combo_verilog_status":"done",
            "yosys_abc_aig_action_id":ancestors[4],"yosys_abc_aig_status":"done",
            "aig_stat_diff_action_id":ancestors[5],"aig_stat_diff_status":"done",
            "g8r_stats_action_id":stats[0],"g8r_stats_status":"done",
            "yosys_abc_stats_action_id":stats[1],"yosys_abc_stats_status":"done",
            "g8r_raw_stats_action_id":stats[2],"g8r_raw_stats_status":"done"
        }));
    }
    fs::write(run.join("manifest.json"), serde_json::to_vec(&json!({
        "schema_version":5,"recipe_preset":"g8r-abc-vs-yabc-aig-diff","fraig":false,"dso_version":"v0.59.0",
        "driver_runtime":lowering,"stats_runtime":estimator,"yosys_runtime":yosys,"yosys_script":script.path,"yosys_script_sha256":script.sha256,"samples":rows
    })).unwrap()).unwrap();
    run
}

fn mutate_manifest(run: &Path, change: impl FnOnce(&mut serde_json::Value)) {
    let path = run.join("manifest.json");
    let mut value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    change(&mut value);
    fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
}

#[test]
fn builds_complete_matching_releases_with_bounded_deterministic_shards() {
    let root = Fixture::new();
    let a = fixture_run(&root.0, "0.73.0", 4);
    let b = fixture_run(&root.0, "0.74.0", 4);
    let (_, samples) = ingest(&a).unwrap();
    let target = wire::EvidenceShard {
        samples: samples[..1].to_vec(),
    }
    .encoded_len()
        + 512;
    let mut options = root.options();
    let built = build(&options, &[], &[b.clone(), a.clone()], &[], target).unwrap();
    let verified = verify_static_site(&options.out_dir).unwrap();
    assert_eq!(built.snapshot_id, verified.snapshot_id);
    assert_eq!(built.dataset_count, 2);
    let site =
        wire::CorpusSite::decode(fs::read(options.out_dir.join(MANIFEST)).unwrap().as_slice())
            .unwrap();
    assert_eq!(
        site.generations
            .iter()
            .map(|g| g.crate_version.as_str())
            .collect::<Vec<_>>(),
        ["0.73.0", "0.74.0"]
    );
    for g in &site.generations {
        assert_eq!(g.sample_count, 4);
        assert_eq!(g.shards.len(), 4);
        for shard in &g.shards {
            for f in [&shard.evidence, &shard.metrics, &shard.ir] {
                assert!(f.as_ref().unwrap().bytes <= target as u64);
            }
        }
    }
    options.overwrite = true;
    assert_eq!(
        build(&options, &[], &[a, b], &[], target)
            .unwrap()
            .snapshot_id,
        built.snapshot_id
    );
}

#[test]
fn rejects_incomplete_or_forged_operational_inputs() {
    let root = Fixture::new();
    let run = fixture_run(&root.0, "0.74.0", 1);
    let original = fs::read(run.join("manifest.json")).unwrap();
    for field in [
        "status",
        "import_ir_status",
        "g8r_aig_status",
        "g8r_abc_aig_status",
        "g8r_stats_status",
        "g8r_raw_stats_status",
        "combo_verilog_status",
        "yosys_abc_aig_status",
        "yosys_abc_stats_status",
        "aig_stat_diff_status",
    ] {
        mutate_manifest(&run, |m| m["samples"][0][field] = json!("pending"));
        assert!(ingest(&run).is_err(), "accepted incomplete {field}");
        fs::write(run.join("manifest.json"), &original).unwrap();
    }
    for field in [
        "import_ir_action_id",
        "g8r_aig_action_id",
        "g8r_abc_aig_action_id",
        "combo_verilog_action_id",
        "yosys_abc_aig_action_id",
        "aig_stat_diff_action_id",
        "g8r_stats_action_id",
        "g8r_raw_stats_action_id",
        "yosys_abc_stats_action_id",
        "source_sha256",
    ] {
        mutate_manifest(&run, |m| m["samples"][0][field] = json!("0".repeat(64)));
        assert!(ingest(&run).is_err(), "accepted forged {field}");
        fs::write(run.join("manifest.json"), &original).unwrap();
    }
    let sample_id = crate::corpus::sample_id_for_relpath("input0.ir");
    for name in [
        "input.ir",
        "g8r_stats.json",
        "g8r_raw_stats.json",
        "yosys_abc_stats.json",
    ] {
        let path = run.join("artifacts").join(&sample_id).join(name);
        let bytes = fs::read(&path).unwrap();
        fs::write(&path, b"{}").unwrap();
        assert!(ingest(&run).is_err(), "accepted corrupt {name}");
        fs::write(path, bytes).unwrap();
    }
}

#[test]
fn failed_build_preserves_previous_site_and_rejects_unsafe_inputs() {
    let root = Fixture::new();
    let a = fixture_run(&root.0, "0.73.0", 2);
    let b = fixture_run(&root.0, "0.74.0", 1);
    let mut options = root.options();
    build(&options, &[], std::slice::from_ref(&a), &[], 100_000).unwrap();
    let before = fs::read(options.out_dir.join(STATIC_SITE_MANIFEST_FILENAME)).unwrap();
    options.overwrite = true;
    for runs in [vec![a.clone(), b], vec![a.clone(), a.clone()]] {
        assert!(build(&options, &[], &runs, &[], 100_000).is_err());
        assert_eq!(
            fs::read(options.out_dir.join(STATIC_SITE_MANIFEST_FILENAME)).unwrap(),
            before
        );
    }
    for size in [0, 1, MAX_SHARD_TARGET + 1] {
        assert!(build(&options, &[], std::slice::from_ref(&a), &[], size).is_err());
    }
    options.out_dir = a.join("site");
    assert!(build(&options, &[], &[a], &[], 100_000).is_err());
}

#[test]
fn corpus_build_and_publication_allow_siblings_but_protect_the_checkout() {
    let root = Fixture::new();
    let checkout = root.0.join("checkout");
    let run = fixture_run(&checkout, "0.74.0", 1);
    let protected = [("resource checkout", checkout.as_path())];
    let mut options = root.options();
    options.out_dir = checkout.join("site");
    assert!(
        build(
            &options,
            &protected,
            std::slice::from_ref(&run),
            &[],
            100_000
        )
        .is_err()
    );
    assert!(!options.out_dir.exists());

    options.out_dir = checkout.join("../bvc-site");
    let summary = build(&options, &protected, &[run], &[], 100_000).unwrap();
    let unsafe_publication = checkout.join("publication");
    assert!(
        crate::publish::publish_static_site_with_protected_roots(
            &options.out_dir,
            &unsafe_publication,
            &protected
        )
        .is_err()
    );
    assert!(!unsafe_publication.exists());

    let publication = checkout.join("../bvc-publication");
    let published = crate::publish::publish_static_site_with_protected_roots(
        &options.out_dir,
        &publication,
        &protected,
    )
    .unwrap();
    assert_eq!(published.snapshot_id, summary.snapshot_id);
    assert_eq!(
        crate::publish::verify_published_site(&publication)
            .unwrap()
            .site_id,
        published.site_id
    );
}

#[test]
fn verifier_rejects_projection_tampering_even_with_updated_file_hashes() {
    let root = Fixture::new();
    let a = fixture_run(&root.0, "0.74.0", 1);
    let options = root.options();
    build(&options, &[], &[a], &[], 100_000).unwrap();
    let mut site =
        wire::CorpusSite::decode(fs::read(options.out_dir.join(MANIFEST)).unwrap().as_slice())
            .unwrap();
    let relative = path(&site.generations[0].shards[0].metrics).unwrap();
    let mut metrics: Vec<Metric> =
        serde_json::from_slice(&fs::read(options.out_dir.join(&relative)).unwrap()).unwrap();
    metrics[0].g8r_nodes += 1;
    write_file(
        &options.out_dir,
        &relative,
        &serde_json::to_vec(&metrics).unwrap(),
    )
    .unwrap();
    site.generations[0].shards[0].metrics =
        Some(publication_file(&options.out_dir, &relative).unwrap());
    let bytes = site.encode_to_vec();
    write_file(&options.out_dir, MANIFEST, &bytes).unwrap();
    let manifest = pb::StaticSiteManifest {
        record_version: STATIC_SITE_RECORD_VERSION,
        source_snapshot_id: Some(
            crate::proto::digest_from_hex(&sha256_hex(&bytes), "site").unwrap(),
        ),
        base_url: "/nested/".into(),
        files: actual_site_relpaths(&options.out_dir)
            .unwrap()
            .iter()
            .map(|p| publication_file(&options.out_dir, p).unwrap())
            .collect(),
    };
    write_file(
        &options.out_dir,
        STATIC_SITE_MANIFEST_FILENAME,
        &manifest.encode_to_vec(),
    )
    .unwrap();
    assert!(verify_static_site(&options.out_dir).is_err());
}

#[test]
fn hosting_budget_rejects_large_files_and_symlinks() {
    let root = Fixture::new();
    let asset = root.0.join("large.pb");
    let file = fs::File::create(&asset).unwrap();
    file.set_len(MAX_ASSET_BYTES + 1).unwrap();
    assert!(verify_hosting_budget(&root.0).is_err());
    file.set_len(MAX_ASSET_BYTES).unwrap();
    verify_hosting_budget(&root.0).unwrap();
    std::os::unix::fs::symlink(&asset, root.0.join("link")).unwrap();
    assert!(verify_hosting_budget(&root.0).is_err());
}

#[test]
fn corpus_site_cli_supports_composed_and_standalone_sources() {
    use crate::cli::Cli;
    use clap::Parser;
    let parse = |args: &[&str]| {
        Cli::try_parse_from(
            ["bvc", "build-static-site", "--out-dir", "site"]
                .into_iter()
                .chain(args.iter().copied()),
        )
    };
    assert!(parse(&[]).is_err());
    assert!(parse(&["--snapshot-dir", "snapshot"]).is_ok());
    assert!(parse(&["--corpus-run-dir", "a", "--corpus-run-dir", "b"]).is_ok());
    assert!(parse(&["--corpus-run-dir", "a", "--snapshot-dir", "snapshot"]).is_ok());
    let evidence_only =
        parse(&["--corpus-evidence-dir", "a", "--corpus-evidence-dir", "b"]).unwrap();
    let crate::cli::TopCommand::BuildStaticSite {
        corpus_evidence_dirs,
        corpus_run_dirs,
        snapshot_dir,
        ..
    } = evidence_only.command
    else {
        panic!("wrong command")
    };
    assert_eq!(
        corpus_evidence_dirs,
        [PathBuf::from("a"), PathBuf::from("b")]
    );
    assert!(corpus_run_dirs.is_empty());
    assert!(snapshot_dir.is_none());
    assert!(parse(&["--corpus-evidence-dir", "a", "--corpus-run-dir", "b"]).is_ok());
    assert!(parse(&["--corpus-evidence-dir", "a", "--snapshot-dir", "snapshot"]).is_ok());
    assert!(
        parse(&[
            "--corpus-run-dir",
            "a",
            "--snapshot-dir",
            "snapshot",
            "--progression-run-dir",
            "b"
        ])
        .is_ok()
    );
}

#[test]
fn dashboard_composes_verifies_and_publishes_all_versions() {
    let root = Fixture::new();
    super::super::tests::build_historical_candidate_site_fixture(&root.0);
    let run = fixture_run(&root.0, "0.74.0", 2);
    let options = BuildStaticSiteOptions {
        snapshot_dir: root.0.join("snapshot"),
        out_dir: root.0.join("dashboard"),
        base_url: "/nested/".into(),
        overwrite: false,
    };
    let summary = super::super::site_dashboard::build(
        &options,
        &[],
        &[root.0.join("candidate")],
        &[run],
        &[],
        100_000,
    )
    .unwrap();
    let data: serde_json::Value =
        serde_json::from_slice(&fs::read(options.out_dir.join("dashboard.json")).unwrap()).unwrap();
    assert_eq!(
        data["versions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["crate_version"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["0.66.0", "0.74.0"]
    );
    assert_eq!(data["versions"][1]["full_count"], 2);
    assert_eq!(data["trends"][0]["count"], 2);
    assert_eq!(data["schema_version"], 2);
    let stages = &data["trends"][0]["stages"];
    assert_eq!(stages["abc"]["points"][0]["equal"], 2);
    assert_eq!(stages["raw"]["points"][0]["equal"], 2);
    assert_eq!(stages["abc"]["points"][0]["cost"], 30.0);
    assert_eq!(stages["raw"]["points"][0]["cost"], 38.0);
    assert_eq!(
        stages["raw"]["points"][0]["url"],
        "corpus/?release=0.74.0&reference=release&baseline=0.74.0&stage=raw"
    );
    let mut reused_options = options.clone();
    reused_options.out_dir = root.0.join("reused-dashboard");
    let reused = super::super::site_dashboard::build(
        &reused_options,
        &[],
        &[root.0.join("candidate")],
        &[],
        &[options.out_dir.join("corpus")],
        100_000,
    )
    .unwrap();
    assert_eq!(reused.snapshot_id, summary.snapshot_id);
    let published =
        crate::publish::publish_static_site(&options.out_dir, &root.0.join("publication")).unwrap();
    assert_eq!(published.snapshot_id, summary.snapshot_id);
    assert_eq!(
        crate::publish::verify_published_site(&root.0.join("publication"))
            .unwrap()
            .site_id,
        published.site_id
    );
    // A rehashed child page cannot restore a competing overview or change its
    // redirect. Navigation is reconstructed from the composition, not trusted HTML.
    let child = options.out_dir.join("history");
    let original_page = fs::read(child.join("index.html")).unwrap();
    let original_manifest = fs::read(child.join(STATIC_SITE_MANIFEST_FILENAME)).unwrap();
    fs::write(child.join("index.html"), br#"<!doctype html><html><head><meta name="bvc-site-root" content="./"></head><body><h1>Another homepage</h1></body></html>"#).unwrap();
    let mut child_manifest = pb::StaticSiteManifest::decode(original_manifest.as_slice()).unwrap();
    *child_manifest
        .files
        .iter_mut()
        .find(|f| f.logical_key == "index.html")
        .unwrap() = publication_file(&child, "index.html").unwrap();
    fs::write(
        child.join(STATIC_SITE_MANIFEST_FILENAME),
        child_manifest.encode_to_vec(),
    )
    .unwrap();
    assert!(verify_static_site(&child).is_err());
    fs::write(child.join("index.html"), original_page).unwrap();
    fs::write(child.join(STATIC_SITE_MANIFEST_FILENAME), original_manifest).unwrap();
    // Rehashing modified browser data must not detach it from the child evidence.
    fs::write(options.out_dir.join("dashboard.json"), b"{}").unwrap();
    let mut manifest = pb::StaticSiteManifest::decode(
        fs::read(options.out_dir.join(STATIC_SITE_MANIFEST_FILENAME))
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    *manifest
        .files
        .iter_mut()
        .find(|f| f.logical_key == "dashboard.json")
        .unwrap() = publication_file(&options.out_dir, "dashboard.json").unwrap();
    fs::write(
        options.out_dir.join(STATIC_SITE_MANIFEST_FILENAME),
        manifest.encode_to_vec(),
    )
    .unwrap();
    assert!(verify_static_site(&options.out_dir).is_err());
}

#[test]
fn corpus_browser_semantics() {
    let result = Command::new("node")
        .arg("testdata/corpus_site_test.js")
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

fn evidence_index(directory: &Path) -> wire::CorpusSite {
    wire::CorpusSite::decode(fs::read(directory.join(MANIFEST)).unwrap().as_slice()).unwrap()
}

// Deliberately rebind a test bundle, so rejection must be semantic rather than
// merely a stale checksum. This helper never touches an evaluation store.
fn rebind_evidence(directory: &Path, site: &wire::CorpusSite) {
    let bytes = site.encode_to_vec();
    write_file(directory, MANIFEST, &bytes).unwrap();
    let manifest = pb::StaticSiteManifest {
        record_version: STATIC_SITE_RECORD_VERSION,
        source_snapshot_id: Some(
            crate::proto::digest_from_hex(&sha256_hex(&bytes), "site").unwrap(),
        ),
        base_url: "/nested/".into(),
        files: actual_site_relpaths(directory)
            .unwrap()
            .iter()
            .map(|p| publication_file(directory, p).unwrap())
            .collect(),
    };
    write_file(
        directory,
        STATIC_SITE_MANIFEST_FILENAME,
        &manifest.encode_to_vec(),
    )
    .unwrap();
}

#[test]
fn evidence_rebuild_is_identical_without_evaluation_stores_or_old_web_assets() {
    let root = Fixture::new();
    let a = fixture_run(&root.0, "0.73.0", 4);
    let b = fixture_run(&root.0, "0.74.0", 4);
    let mut options = root.options();
    let original = build(&options, &[], &[a.clone(), b.clone()], &[], 100_000).unwrap();
    let source = options.out_dir.clone();
    fs::rename(a, root.0.join("retired-a")).unwrap();
    fs::rename(b, root.0.join("retired-b")).unwrap();
    // No old browser projection or template is an internal source of truth.
    for path in actual_site_relpaths(&source).unwrap() {
        if !path.ends_with(".pb") {
            fs::remove_file(source.join(path)).unwrap();
        }
    }
    options.out_dir = root.0.join("rebuilt");
    let rebuilt = build(&options, &[], &[], &[source], 100_000).unwrap();
    assert_eq!(rebuilt.snapshot_id, original.snapshot_id);
    assert_eq!(rebuilt.file_count, original.file_count);
    assert_eq!(rebuilt.total_bytes, original.total_bytes);
    assert_eq!(
        verify_static_site(&options.out_dir).unwrap().snapshot_id,
        original.snapshot_id
    );
}

#[test]
fn mixes_multiple_evidence_sources_with_new_runs_and_reshards_deterministically() {
    let root = Fixture::new();
    let a = fixture_run(&root.0, "0.72.0", 4);
    let b = fixture_run(&root.0, "0.73.0", 4);
    let c = fixture_run(&root.0, "0.74.0", 4);
    let (_, samples) = ingest(&a).unwrap();
    let target = wire::EvidenceShard {
        samples: samples[..1].to_vec(),
    }
    .encoded_len()
        + 512;
    let mut options = root.options();
    let direct = build(
        &options,
        &[],
        &[a.clone(), b.clone(), c.clone()],
        &[],
        target,
    )
    .unwrap();
    let mut sources = Vec::new();
    for (name, run) in [("older-a", a), ("older-b", b)] {
        options.out_dir = root.0.join(name);
        build(&options, &[], std::slice::from_ref(&run), &[], 100_000).unwrap();
        sources.push(options.out_dir.clone());
        fs::rename(run, root.0.join(format!("retired-{name}"))).unwrap();
    }
    options.out_dir = root.0.join("mixed");
    let mixed = build(&options, &[], &[c], &sources, target).unwrap();
    assert_eq!(mixed.snapshot_id, direct.snapshot_id);
    let site = evidence_index(&options.out_dir);
    assert_eq!(
        site.generations
            .iter()
            .map(|g| (g.crate_version.as_str(), g.sample_count, g.shards.len()))
            .collect::<Vec<_>>(),
        [("0.72.0", 4, 4), ("0.73.0", 4, 4), ("0.74.0", 4, 4)]
    );
}

#[test]
fn evidence_reuse_rejects_corruption_and_preserves_existing_output() {
    let root = Fixture::new();
    let run = fixture_run(&root.0, "0.74.0", 2);
    let mut options = root.options();
    build(&options, &[], &[run], &[], 100_000).unwrap();
    let source = options.out_dir.clone();
    options.out_dir = root.0.join("destination");
    let before = build(&options, &[], &[], std::slice::from_ref(&source), 100_000).unwrap();
    options.overwrite = true;
    let original_index = evidence_index(&source);
    let relative = path(&original_index.generations[0].shards[0].evidence).unwrap();
    let original_bytes = fs::read(source.join(&relative)).unwrap();
    // Missing, corrupt, and symlinked evidence must all fail closed.
    fs::rename(source.join(&relative), source.join("saved.pb")).unwrap();
    assert!(build(&options, &[], &[], std::slice::from_ref(&source), 100_000).is_err());
    std::os::unix::fs::symlink(source.join("saved.pb"), source.join(&relative)).unwrap();
    assert!(build(&options, &[], &[], std::slice::from_ref(&source), 100_000).is_err());
    fs::remove_file(source.join(&relative)).unwrap();
    fs::rename(source.join("saved.pb"), source.join(&relative)).unwrap();
    fs::write(source.join(&relative), b"invalid protobuf").unwrap();
    assert!(build(&options, &[], &[], std::slice::from_ref(&source), 100_000).is_err());
    fs::write(source.join(&relative), &original_bytes).unwrap();
    // Rehashed but invalid action evidence also fails the semantic verifier.
    let mut shard = wire::EvidenceShard::decode(original_bytes.as_slice()).unwrap();
    shard.samples[0].actions.clear();
    fs::write(source.join(&relative), shard.encode_to_vec()).unwrap();
    let mut changed = original_index.clone();
    changed.generations[0].shards[0].evidence = Some(publication_file(&source, &relative).unwrap());
    rebind_evidence(&source, &changed);
    assert!(build(&options, &[], &[], std::slice::from_ref(&source), 100_000).is_err());
    fs::write(source.join(&relative), &original_bytes).unwrap();
    // A forged population or escaping shard reference cannot reuse a valid bundle.
    for change in [0, 1, 2] {
        let mut changed = original_index.clone();
        match change {
            0 => changed.generations[0].sample_count += 1,
            1 => changed.generations[0].cohort_sha256[0] ^= 1,
            _ => {
                changed.generations[0].shards[0]
                    .evidence
                    .as_mut()
                    .unwrap()
                    .relpath
                    .as_mut()
                    .unwrap()
                    .value = "../escaped.pb".into()
            }
        }
        rebind_evidence(&source, &changed);
        assert!(build(&options, &[], &[], std::slice::from_ref(&source), 100_000).is_err());
    }
    assert_eq!(
        verify_static_site(&options.out_dir).unwrap().snapshot_id,
        before.snapshot_id
    );
}

#[test]
fn evidence_reuse_rejects_duplicate_releases_mixed_populations_and_overlap() {
    let root = Fixture::new();
    let a = fixture_run(&root.0, "0.73.0", 2);
    let b = fixture_run(&root.0, "0.74.0", 1);
    let mut options = root.options();
    build(&options, &[], std::slice::from_ref(&a), &[], 100_000).unwrap();
    let source = options.out_dir.clone();
    options.overwrite = true;
    assert!(build(&options, &[], &[], std::slice::from_ref(&source), 100_000).is_err());
    options.out_dir = root.0.join("destination");
    assert!(
        build(
            &options,
            &[],
            &[],
            &[source.clone(), source.clone()],
            100_000
        )
        .is_err()
    );
    assert!(build(&options, &[], &[a], std::slice::from_ref(&source), 100_000).is_err());
    assert!(build(&options, &[], &[b], &[source], 100_000).is_err());
    assert!(!options.out_dir.exists());
}

#[test]
fn evidence_reuse_preserves_aliases_or_refreshes_them_from_an_explicit_snapshot() {
    let root = Fixture::new();
    let run = fixture_run(&root.0, "0.74.0", 1);
    let mut options = root.options();
    build(&options, &[], &[run], &[], 100_000).unwrap();
    let source = options.out_dir.clone();
    let mut site = evidence_index(&source);
    let mut samples = evidence::Source::open(&source)
        .unwrap()
        .samples(&site.generations[0])
        .unwrap();
    let sample = &mut samples[0];
    let top = &sample.input.as_ref().unwrap().top_fn_name;
    sample.provenance = Some(wire::InputProvenance {
        source_snapshot_sha256: vec![1; 32],
        structural_hashes: vec![vec![2; 32]],
        function_sha256: Sha256::digest(
            crate::executor::extract_ir_fn_block_by_name(&sample.input_ir, top)
                .unwrap()
                .as_bytes(),
        )
        .to_vec(),
        origins: vec![wire::FunctionOrigin {
            ir_top: top.clone(),
            kind: wire::InputKind::Whole as i32,
            ir_action_id: vec![3; 32],
            ..Default::default()
        }],
    });
    site.generations[0].shards.clear();
    write_shards(&source, &mut site.generations[0], &samples, 100_000).unwrap();
    rebind_evidence(&source, &site);
    let frozen = verify_static_site(&source).unwrap();
    options.out_dir = root.0.join("retained");
    let rebuilt = build(&options, &[], &[], std::slice::from_ref(&source), 100_000).unwrap();
    assert_eq!(rebuilt.snapshot_id, frozen.snapshot_id);
    let retained = evidence::Source::open(&options.out_dir).unwrap();
    assert_eq!(
        retained.samples(&retained.site.generations[0]).unwrap(),
        samples
    );

    // The supplied snapshot lacks this function. Its verified index must replace
    // the old alias binding rather than silently retaining stale classifications.
    let historical = Fixture::new();
    super::super::tests::build_historical_candidate_site_fixture(&historical.0);
    options.snapshot_dir = historical.0.join("snapshot");
    options.out_dir = root.0.join("refreshed");
    build(&options, &[], &[], std::slice::from_ref(&source), 100_000).unwrap();
    samples[0].provenance = None;
    let refreshed = evidence::Source::open(&options.out_dir).unwrap();
    assert_eq!(
        refreshed.samples(&refreshed.site.generations[0]).unwrap(),
        samples
    );

    // A new snapshot cannot launder an invalid old binding before validation.
    samples[0].provenance = Some(wire::InputProvenance::default());
    site.generations[0].shards.clear();
    write_shards(&source, &mut site.generations[0], &samples, 100_000).unwrap();
    rebind_evidence(&source, &site);
    options.out_dir = root.0.join("invalid-binding");
    assert!(build(&options, &[], &[], &[source], 100_000).is_err());
    assert!(!options.out_dir.exists());
}

#[test]
fn evidence_reuse_rejects_incompatible_measurement_runtimes() {
    let root = Fixture::new();
    let mut options = root.options();
    let mut sources = Vec::new();
    for version in ["0.73.0", "0.74.0"] {
        let run = fixture_run(&root.0, version, 1);
        options.out_dir = root.0.join(format!("site-{version}"));
        build(&options, &[], &[run], &[], 100_000).unwrap();
        sources.push(options.out_dir.clone());
    }
    let source = &sources[1];
    let mut site = evidence_index(source);
    let relative = path(&site.generations[0].shards[0].evidence).unwrap();
    let mut shard =
        wire::EvidenceShard::decode(fs::read(source.join(&relative)).unwrap().as_slice()).unwrap();
    let sample = &mut shard.samples[0];
    let ActionSpec::DriverIrToG8rAig {
        runtime: driver, ..
    } = crate::proto::action_spec_from_proto(&sample.actions[1]).unwrap()
    else {
        panic!("missing lowering")
    };
    let ActionSpec::AigToYosysAbcAig {
        runtime: yosys,
        yosys_script_ref: script,
        ..
    } = crate::proto::action_spec_from_proto(&sample.actions[2]).unwrap()
    else {
        panic!("missing ABC")
    };
    let ActionSpec::DriverAigToStats {
        runtime: mut estimator,
        ..
    } = required_progression_action(&sample.g8r_abc.as_ref().unwrap().action, "stats").unwrap()
    else {
        panic!("missing estimator")
    };
    estimator.docker_image_id = "9".repeat(64);
    let graph = actions(
        sample.input.as_ref().unwrap(),
        &site.generations[0].dso_version,
        &driver,
        &estimator,
        &yosys,
        &script,
    )
    .unwrap();
    sample.actions = graph
        .ancestors
        .iter()
        .map(crate::proto::action_spec_to_proto)
        .collect::<Result<_>>()
        .unwrap();
    for (stats, action) in [
        &mut sample.g8r_abc,
        &mut sample.yosys_abc,
        &mut sample.g8r_raw,
    ]
    .into_iter()
    .zip(&graph.stats)
    {
        stats.as_mut().unwrap().action = Some(crate::proto::action_spec_to_proto(action).unwrap());
    }
    fs::write(source.join(&relative), shard.encode_to_vec()).unwrap();
    site.generations[0].shards[0].evidence = Some(publication_file(source, &relative).unwrap());
    rebind_evidence(source, &site);
    // Each bundle is valid independently, but they cannot be compared together.
    verify_static_site(source).unwrap();
    options.out_dir = root.0.join("incompatible");
    assert!(build(&options, &[], &[], &sources, 100_000).is_err());
    assert!(!options.out_dir.exists());
}

#[test]
fn corpus_site_works_with_immutable_publication() {
    let root = Fixture::new();
    let run = fixture_run(&root.0, "0.74.0", 1);
    let options = root.options();
    let site = build(&options, &[], &[run], &[], 100_000).unwrap();
    let publication = root.0.join("published");
    let first = crate::publish::publish_static_site(&options.out_dir, &publication).unwrap();
    assert_eq!(first.snapshot_id, site.snapshot_id);
    assert_eq!(
        crate::publish::verify_published_site(&publication)
            .unwrap()
            .site_id,
        first.site_id
    );
    assert!(
        crate::publish::publish_static_site(&options.out_dir, &publication)
            .unwrap()
            .reused_immutable_site
    );
}

#[test]
fn cohort_identity_is_order_independent_and_content_bound() {
    let a = wire::InputIdentity {
        source_relpath: "a.ir".into(),
        source_sha256: vec![1; 32],
        top_fn_name: "a".into(),
    };
    let b = wire::InputIdentity {
        source_relpath: "b.ir".into(),
        source_sha256: vec![2; 32],
        top_fn_name: "b".into(),
    };
    assert_eq!(
        cohort_identity([a.clone(), b.clone()]).unwrap(),
        cohort_identity([b.clone(), a.clone()]).unwrap()
    );
    assert!(cohort_identity([a.clone(), a.clone()]).is_err());
    let mut changed = b.clone();
    changed.source_sha256[0] = 3;
    assert_ne!(
        cohort_identity([a.clone(), b]).unwrap(),
        cohort_identity([a, changed]).unwrap()
    );
}

#[test]
fn publication_schema_does_not_change_the_evaluation_store_descriptor() {
    assert_eq!(
        sha256_hex(crate::proto::FILE_DESCRIPTOR_SET),
        "50c5524c7dc7d6a45a9e0bf35722867585cd81e3451ba36b6622913245c2c548"
    );
}
