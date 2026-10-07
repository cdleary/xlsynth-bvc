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
    let built = build(&options, &[], &[b.clone(), a.clone()], target).unwrap();
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
        build(&options, &[], &[a, b], target).unwrap().snapshot_id,
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
    build(&options, &[], std::slice::from_ref(&a), 100_000).unwrap();
    let before = fs::read(options.out_dir.join(STATIC_SITE_MANIFEST_FILENAME)).unwrap();
    options.overwrite = true;
    for runs in [vec![a.clone(), b], vec![a.clone(), a.clone()]] {
        assert!(build(&options, &[], &runs, 100_000).is_err());
        assert_eq!(
            fs::read(options.out_dir.join(STATIC_SITE_MANIFEST_FILENAME)).unwrap(),
            before
        );
    }
    for size in [0, 1, MAX_SHARD_TARGET + 1] {
        assert!(build(&options, &[], std::slice::from_ref(&a), size).is_err());
    }
    options.out_dir = a.join("site");
    assert!(build(&options, &[], &[a], 100_000).is_err());
}

#[test]
fn corpus_build_and_publication_allow_siblings_but_protect_the_checkout() {
    let root = Fixture::new();
    let checkout = root.0.join("checkout");
    let run = fixture_run(&checkout, "0.74.0", 1);
    let protected = [("resource checkout", checkout.as_path())];
    let mut options = root.options();
    options.out_dir = checkout.join("site");
    assert!(build(&options, &protected, std::slice::from_ref(&run), 100_000).is_err());
    assert!(!options.out_dir.exists());

    options.out_dir = checkout.join("../bvc-site");
    let summary = build(&options, &protected, &[run], 100_000).unwrap();
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
    build(&options, &[], &[a], 100_000).unwrap();
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
fn corpus_site_cli_requires_exactly_one_source_mode() {
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
    assert!(parse(&["--corpus-run-dir", "a", "--snapshot-dir", "snapshot"]).is_err());
    assert!(parse(&["--corpus-run-dir", "a", "--progression-run-dir", "b"]).is_err());
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

#[test]
fn corpus_site_works_with_immutable_publication() {
    let root = Fixture::new();
    let run = fixture_run(&root.0, "0.74.0", 1);
    let options = root.options();
    let site = build(&options, &[], &[run], 100_000).unwrap();
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
