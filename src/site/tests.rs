// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::campaign::finalize_campaign_run;
use crate::executor::compute_action_id;
use crate::model::{
    ActionSpec, ArtifactRef, ArtifactType, DriverSourceRevision, OutputFile, Provenance,
    QueueFailed,
};
use crate::query::{canonical_root_actions_for_crate_version, rebuild_versions_cards_index};
use crate::snapshot::{
    BuildStaticSnapshotOptions, STATIC_SNAPSHOT_MANIFEST_FILENAME, build_static_snapshot,
};
use crate::store::ArtifactStore;
use crate::versioning::{
    load_version_compat_map, load_xlsynth_crate_repository_head_observation,
    resolve_xlsynth_version_for_driver,
};
use chrono::Utc;
use serde_json::json;
use std::io::Write as _;

fn temp_root() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("xlsynth-bvc-site-{}-{nanos}", std::process::id()))
}

fn test_repo_root() -> PathBuf {
    std::env::current_dir().expect("current directory")
}

fn candidate_driver_runtime(commit: &str, source: bool) -> DriverRuntimeSpec {
    DriverRuntimeSpec {
        driver_version: "0.68.0".to_string(),
        source_revision: source.then(|| DriverSourceRevision {
            repository: crate::XLSYNTH_CRATE_GIT_REPOSITORY.to_string(),
            commit: commit.to_string(),
        }),
        release_platform: crate::DEFAULT_RELEASE_PLATFORM.to_string(),
        docker_image: if source {
            crate::runtime::git_driver_image(commit)
                .unwrap_or_else(|_| "xlsynth-bvc-driver:invalid-git-commit".to_string())
        } else {
            crate::runtime::default_driver_image("0.68.0")
        },
        dockerfile: if source {
            crate::DEFAULT_GIT_DOCKERFILE.to_string()
        } else {
            crate::DEFAULT_DOCKERFILE.to_string()
        },
        docker_image_id: "a".repeat(64),
        dockerfile_sha256: if source {
            sha256_hex(include_bytes!("../../docker/xlsynth-driver-git.Dockerfile"))
        } else {
            "b".repeat(64)
        },
        release_cache_input_sha256: "c".repeat(64),
    }
}

fn candidate_manifest_input(commit: &str) -> CandidateCorpusManifestInput {
    let driver_runtime = candidate_driver_runtime(commit, true);
    let stats_runtime = candidate_driver_runtime(commit, false);
    let abc_runtime = YosysRuntimeSpec {
        docker_image: crate::DEFAULT_YOSYS_DOCKER_IMAGE.to_string(),
        dockerfile: crate::DEFAULT_YOSYS_DOCKERFILE.to_string(),
        dockerfile_sha256: "d".repeat(64),
        docker_image_id: "e".repeat(64),
        upstream_commit: Some(crate::DEFAULT_YOSYS_UPSTREAM_COMMIT.to_string()),
        slang_commit: None,
    };
    let samples = release_progression_ir_artifacts()
        .expect("fixed artifact manifest")
        .into_iter()
        .enumerate()
        .map(|(index, (structural_hash, source_sha256))| {
            let source_relpath = format!("{structural_hash}.ir");
            CandidateSampleInput {
                sample_id: crate::corpus::sample_id_for_relpath(&source_relpath),
                source_relpath,
                source_sha256,
                top_fn_name: format!("top_{index}"),
                fraig: false,
                dso_version: "v0.54.7".to_string(),
                import_ir_action_id: format!("{:064x}", index + 1),
                g8r_aig_action_id: format!("{:064x}", index + 1001),
                g8r_stats_action_id: format!("{:064x}", index + 2001),
                yosys_abc_aig_action_id: format!("{:064x}", index + 3001),
            }
        })
        .collect::<Vec<_>>();
    let action_manifest_sha256 =
        candidate_action_manifest_sha256(&samples).expect("candidate action manifest");
    let candidate_run = CandidateRunInput {
        schema_version: 4,
        candidate_run_id: String::new(),
        requested_ref: None,
        observed_at_utc: "2026-09-08T20:01:53Z".to_string(),
        candidate_committed_at_utc: Some("2026-09-08T19:59:42Z".to_string()),
        candidate: CandidateGitRevisionInput {
            kind: "git_revision".to_string(),
            repository: crate::XLSYNTH_CRATE_GIT_REPOSITORY.to_string(),
            commit: commit.to_string(),
        },
        baseline: CandidateReleaseInput {
            kind: "crate_release".to_string(),
            crate_version: "0.68.0".to_string(),
            release_tag: "v0.68.0".to_string(),
            commit: "e".repeat(40),
        },
        dso_version: "0.54.7".to_string(),
        lowering_mode: "frontend_no_prep_rewrite".to_string(),
        fraig: false,
        execution_recipe_revision: 0,
        driver_runtime: driver_runtime.clone(),
        abc_runtime: abc_runtime.clone(),
        stats_runtime: stats_runtime.clone(),
        yosys_script: crate::DEFAULT_YOSYS_FLOW_SCRIPT.to_string(),
        yosys_script_sha256: "1".repeat(64),
        action_manifest_sha256,
        cohort_sample_count: RELEASE_PROGRESSION_IR_COUNT as u64,
        cohort_artifact_manifest_sha256: RELEASE_PROGRESSION_ARTIFACT_MANIFEST_SHA256.to_string(),
    };
    let mut manifest = CandidateCorpusManifestInput {
        schema_version: 5,
        recipe_preset: "g8r-abc-stats".to_string(),
        fraig: false,
        dso_version: "v0.54.7".to_string(),
        driver_runtime,
        stats_runtime,
        yosys_runtime: abc_runtime,
        yosys_script: crate::DEFAULT_YOSYS_FLOW_SCRIPT.to_string(),
        yosys_script_sha256: "1".repeat(64),
        candidate_run: Some(candidate_run),
        samples,
    };
    let candidate_run = manifest.candidate_run.as_mut().expect("candidate run");
    candidate_run.candidate_run_id =
        candidate_run_identity_sha256(candidate_run).expect("candidate run identity");
    manifest
}

fn write_candidate_run_marker(run_dir: &Path, manifest: &CandidateCorpusManifestInput) {
    let candidate_run = manifest.candidate_run.as_ref().expect("candidate run");
    let mut marker = serde_json::to_value(candidate_run).expect("serialize candidate marker");
    let marker_object = marker.as_object_mut().expect("candidate marker object");
    marker_object.insert(
        "scheduling_policy_name".to_string(),
        serde_json::json!("release-progression-ir-v1"),
    );
    marker_object.insert(
        "scheduling_policy_config_sha256".to_string(),
        serde_json::json!("9".repeat(64)),
    );
    marker_object.insert(
        "planned_candidate_actions".to_string(),
        serde_json::json!(561),
    );
    marker_object.insert(
        "reused_or_already_queued_actions".to_string(),
        serde_json::json!(561),
    );
    marker_object.insert(
        "newly_enqueued_candidate_actions".to_string(),
        serde_json::json!(0),
    );
    let marker_path = run_dir
        .join(".bvc/bvc-artifacts/corpus")
        .join("corpus-candidate-run.json");
    fs::create_dir_all(marker_path.parent().expect("candidate marker parent"))
        .expect("create candidate marker parent");
    fs::write(
        marker_path,
        serde_json::to_vec_pretty(&marker).expect("encode candidate marker"),
    )
    .expect("write candidate marker");
}

#[test]
fn candidate_baseline_is_bound_to_checked_latest_release_observation() {
    let observation = load_xlsynth_crate_repository_head_observation(&test_repo_root())
        .expect("load repository observation")
        .expect("repository observation");
    let mut manifest = candidate_manifest_input(&"8".repeat(40));
    {
        let candidate_run = manifest.candidate_run.as_mut().expect("candidate run");
        candidate_run.baseline.crate_version = observation.latest_crate_version.clone();
        candidate_run.baseline.release_tag = observation.latest_release_tag.clone();
        candidate_run.baseline.commit = observation.latest_release_commit.clone();
        candidate_run.driver_runtime.driver_version = observation.latest_crate_version.clone();
        candidate_run.stats_runtime.driver_version = observation.latest_crate_version.clone();
        candidate_run.stats_runtime.docker_image =
            crate::runtime::default_driver_image(&observation.latest_crate_version);
        candidate_run.candidate_run_id =
            candidate_run_identity_sha256(candidate_run).expect("candidate run identity");
    }
    manifest.driver_runtime = manifest
        .candidate_run
        .as_ref()
        .expect("candidate run")
        .driver_runtime
        .clone();
    manifest.stats_runtime = manifest
        .candidate_run
        .as_ref()
        .expect("candidate run")
        .stats_runtime
        .clone();
    validate_candidate_corpus_manifest_input(&manifest).expect("valid candidate manifest");
    assert!(
        validate_candidate_baseline_release_observation(
            manifest.candidate_run.as_ref().expect("candidate run"),
            Some(&observation),
        )
        .expect("matching checked release observation")
    );

    let candidate_run = manifest.candidate_run.as_mut().expect("candidate run");
    candidate_run.baseline.commit = "d".repeat(40);
    candidate_run.candidate_run_id =
        candidate_run_identity_sha256(candidate_run).expect("candidate run identity");
    validate_candidate_corpus_manifest_input(&manifest)
        .expect("self-consistent candidate manifest");
    let error = validate_candidate_baseline_release_observation(
        manifest.candidate_run.as_ref().expect("candidate run"),
        Some(&observation),
    )
    .expect_err("self-asserted release commit must be rejected");
    assert!(format!("{error:#}").contains("checked latest-release observation"));

    let historical_run = manifest.candidate_run.as_mut().expect("candidate run");
    historical_run.baseline.crate_version = "0.67.0".to_string();
    historical_run.baseline.release_tag = "v0.67.0".to_string();
    assert!(
        !validate_candidate_baseline_release_observation(historical_run, Some(&observation),)
            .expect("historical baseline remains loadable without publishing an unverified commit")
    );
}

#[test]
fn candidate_requested_ref_is_only_published_when_observation_binds_it() {
    let observation = load_xlsynth_crate_repository_head_observation(&test_repo_root())
        .expect("load repository observation")
        .expect("repository observation");
    let mut manifest = candidate_manifest_input(&observation.head_commit);
    let candidate_run = manifest.candidate_run.as_mut().expect("candidate run");
    candidate_run.requested_ref = Some(observation.head_ref.clone());
    candidate_run.candidate_committed_at_utc = Some(observation.head_committed_at_utc.clone());
    assert_eq!(
        verified_candidate_requested_ref(candidate_run, Some(&observation))
            .expect("verified requested ref"),
        Some(observation.head_ref.clone())
    );

    candidate_run.candidate_committed_at_utc = Some("2000-01-01T00:00:00Z".to_string());
    let error = verified_candidate_requested_ref(candidate_run, Some(&observation))
        .expect_err("current-head timestamp must match the checked observation");
    assert!(
        error
            .to_string()
            .contains("commit timestamp does not match")
    );
    candidate_run.candidate_committed_at_utc = Some(observation.head_committed_at_utc.clone());

    candidate_run.candidate.commit = "8".repeat(40);
    assert_eq!(
        verified_candidate_requested_ref(candidate_run, Some(&observation))
            .expect("historical candidate ref"),
        None,
        "an old commit must not retain a floating main label"
    );

    candidate_run.candidate.commit = observation.head_commit.clone();
    candidate_run.requested_ref = None;
    assert_eq!(
        verified_candidate_requested_ref(candidate_run, Some(&observation))
            .expect("commit-only candidate ref"),
        None,
        "a commit-addressed run must remain labeled as a Git revision"
    );
}

#[test]
fn candidate_manifest_identity_binds_source_driver_runtime() {
    let commit = "8".repeat(40);
    let mut manifest = candidate_manifest_input(&commit);
    validate_candidate_corpus_manifest_input(&manifest).expect("valid candidate manifest");

    manifest
        .candidate_run
        .as_mut()
        .expect("candidate run")
        .driver_runtime
        .docker_image_id = "2".repeat(64);
    manifest.driver_runtime.docker_image_id = "2".repeat(64);
    let error = validate_candidate_corpus_manifest_input(&manifest)
        .expect_err("runtime mutation must invalidate the run identity");
    assert!(error.to_string().contains("mismatched candidate_run_id"));
}

#[test]
fn candidate_manifest_rejects_a_custom_source_build_recipe() {
    let commit = "8".repeat(40);
    let mut manifest = candidate_manifest_input(&commit);
    let candidate_run = manifest.candidate_run.as_mut().expect("candidate run");
    candidate_run.driver_runtime.dockerfile = "custom.Dockerfile".to_string();
    candidate_run.driver_runtime.dockerfile_sha256 = "9".repeat(64);
    manifest.driver_runtime = candidate_run.driver_runtime.clone();
    candidate_run.candidate_run_id =
        candidate_run_identity_sha256(candidate_run).expect("candidate run identity");

    let error = validate_candidate_corpus_manifest_input(&manifest)
        .expect_err("custom source build recipe must fail closed");
    assert!(
        error
            .to_string()
            .contains("canonical source-build Dockerfile")
    );
}

#[test]
fn candidate_manifest_rejects_private_runtime_identifiers() {
    let commit = "8".repeat(40);
    let mut manifest = candidate_manifest_input(&commit);
    manifest.stats_runtime.dockerfile = "/srv/private/xlsynth-driver.Dockerfile".to_string();
    manifest
        .candidate_run
        .as_mut()
        .expect("candidate run")
        .stats_runtime
        .dockerfile = manifest.stats_runtime.dockerfile.clone();
    {
        let candidate_run = manifest.candidate_run.as_mut().expect("candidate run");
        candidate_run.candidate_run_id =
            candidate_run_identity_sha256(candidate_run).expect("candidate run identity");
    }

    let error = validate_candidate_corpus_manifest_input(&manifest)
        .expect_err("private runtime path must not enter public candidate evidence");
    assert!(
        error
            .to_string()
            .contains("canonical public runtime identifiers")
    );
}

#[test]
fn candidate_manifest_identity_binds_commit_timestamp() {
    let commit = "8".repeat(40);
    let mut manifest = candidate_manifest_input(&commit);
    validate_candidate_corpus_manifest_input(&manifest).expect("valid candidate manifest");

    manifest
        .candidate_run
        .as_mut()
        .expect("candidate run")
        .candidate_committed_at_utc = Some("2026-09-08T20:00:00Z".to_string());
    let error = validate_candidate_corpus_manifest_input(&manifest)
        .expect_err("commit-time mutation must invalidate the run identity");
    assert!(error.to_string().contains("mismatched candidate_run_id"));
}

#[test]
fn candidate_manifest_identity_binds_action_graph() {
    let commit = "8".repeat(40);
    let mut manifest = candidate_manifest_input(&commit);
    validate_candidate_corpus_manifest_input(&manifest).expect("valid candidate manifest");

    manifest.samples[0].g8r_aig_action_id = "9".repeat(64);
    let error = validate_candidate_corpus_manifest_input(&manifest)
        .expect_err("action mutation must invalidate the action manifest");
    assert!(format!("{error:#}").contains("action manifest"));
}

#[test]
fn candidate_stats_are_read_from_digest_verified_provenance() {
    let root = temp_root();
    let run_dir = root.join("candidate");
    let store = ArtifactStore::new_with_sled(
        run_dir.join(".bvc/bvc-artifacts"),
        run_dir.join(".bvc/artifacts.sled"),
    );
    store.ensure_layout().expect("candidate store layout");

    let expected_action = ActionSpec::DriverAigToStats {
        aig_action_id: "7".repeat(64),
        version: "v0.54.7".to_string(),
        runtime: candidate_driver_runtime(&"8".repeat(40), false),
    };
    let action_id = compute_action_id(&expected_action).expect("stats action ID");
    let sample = CandidateSampleInput {
        sample_id: "sample".to_string(),
        source_relpath: "sample.ir".to_string(),
        source_sha256: "1".repeat(64),
        top_fn_name: "top".to_string(),
        fraig: false,
        dso_version: "v0.54.7".to_string(),
        import_ir_action_id: "2".repeat(64),
        g8r_aig_action_id: "3".repeat(64),
        g8r_stats_action_id: action_id.clone(),
        yosys_abc_aig_action_id: "4".repeat(64),
    };
    let stats_bytes = br#"{"and_nodes":3,"depth":4,"private_path":"/srv/private/customer-a"}"#;
    store
        .write_provenance(&Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: action_id.clone(),
            created_utc: Utc::now(),
            action: expected_action.clone(),
            dependencies: Vec::new(),
            output_artifact: ArtifactRef {
                action_id,
                artifact_type: ArtifactType::AigStatsFile,
                relpath: crate::corpus::G8R_STATS_RELPATH.to_string(),
            },
            output_files: vec![OutputFile {
                path: "stats.json".to_string(),
                bytes: stats_bytes.len() as u64,
                sha256: sha256_hex(stats_bytes),
            }],
            commands: Vec::new(),
            details: json!({}),
            suggested_next_actions: Vec::new(),
        })
        .expect("write stats provenance");
    let stats_dir = run_dir.join("artifacts/sample");
    fs::create_dir_all(&stats_dir).expect("create exported stats directory");
    let stats_path = stats_dir.join("g8r_stats.json");
    fs::write(&stats_path, stats_bytes).expect("write exported stats");

    let (stats, evidence) =
        read_verified_candidate_stats_with_evidence(&run_dir, &store, &sample, &expected_action)
            .expect("read verified stats");
    assert_eq!(stats["and_nodes"], 3);
    let evidence_json = String::from_utf8(
        encode_candidate_progression_evidence(&CandidateProgressionEvidence {
            schema_version: CANDIDATE_PROGRESSION_EVIDENCE_SCHEMA_VERSION,
            manifest: candidate_manifest_input(&"8".repeat(40)),
            stats: vec![evidence],
        })
        .expect("encode candidate evidence"),
    )
    .expect("UTF-8 evidence");
    assert!(!evidence_json.contains("/srv/private/customer-a"));
    fs::write(&stats_path, br#"{"and_nodes":9,"depth":9}"#).expect("tamper stats");
    let error =
        read_verified_candidate_stats_with_evidence(&run_dir, &store, &sample, &expected_action)
            .expect_err("tampered exported stats must fail provenance verification");
    assert!(format!("{error:#}").contains("immutable provenance digest"));

    drop(store);
    fs::remove_dir_all(root).expect("cleanup candidate fixture");
}

fn build_historical_candidate_site_fixture(root: &Path) -> (PathBuf, String) {
    let baseline_version = "0.66.0";
    let dso_version = "v0.54.7";
    let candidate_commit = "8".repeat(40);
    let mut candidate_runtime = candidate_driver_runtime(&candidate_commit, true);
    candidate_runtime.driver_version = baseline_version.to_string();
    let mut stats_runtime = candidate_driver_runtime(&candidate_commit, false);
    stats_runtime.driver_version = baseline_version.to_string();
    stats_runtime.docker_image = crate::runtime::default_driver_image(baseline_version);
    let abc_runtime = YosysRuntimeSpec {
        docker_image: crate::DEFAULT_YOSYS_DOCKER_IMAGE.to_string(),
        dockerfile: crate::DEFAULT_YOSYS_DOCKERFILE.to_string(),
        dockerfile_sha256: "d".repeat(64),
        docker_image_id: "e".repeat(64),
        upstream_commit: Some(crate::DEFAULT_YOSYS_UPSTREAM_COMMIT.to_string()),
        slang_commit: None,
    };
    let script_ref = ScriptRef {
        path: crate::DEFAULT_YOSYS_FLOW_SCRIPT.to_string(),
        sha256: "1".repeat(64),
    };

    let run_dir = root.join("candidate");
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("source store layout");

    let candidate_store = ArtifactStore::new_with_sled(
        run_dir.join(".bvc/bvc-artifacts"),
        run_dir.join(".bvc/artifacts.sled"),
    );
    candidate_store
        .ensure_layout()
        .expect("candidate store layout");
    let mut release_samples = Vec::new();
    let mut candidate_samples = Vec::new();
    let mut g8r_points = Vec::new();
    let mut yosys_points = Vec::new();
    for (index, (structural_hash, source_sha256)) in release_progression_ir_artifacts()
        .expect("fixed artifact manifest")
        .into_iter()
        .enumerate()
    {
        let source_relpath = format!("{structural_hash}.ir");
        let sample_id = crate::corpus::sample_id_for_relpath(&source_relpath);
        let top_fn_name = format!("top_{index}");
        let import_action = ActionSpec::ImportIrPackageFile {
            source_sha256: source_sha256.clone(),
            top_fn_name: Some(top_fn_name.clone()),
        };
        let import_action_id = compute_action_id(&import_action).expect("import action ID");
        let ir_text = format!(
            "package candidate_fixture\n\nfn {top_fn_name}(x: bits[1]) -> bits[1] {{\n  ret identity.1: bits[1] = identity(x, id=1)\n}}\n"
        );
        let ir_bytes = ir_text.as_bytes();
        let import_artifact = ArtifactRef {
            action_id: import_action_id.clone(),
            artifact_type: ArtifactType::IrPackageFile,
            relpath: "payload/input.ir".to_string(),
        };
        store
            .write_provenance(&Provenance {
                schema_version: crate::ACTION_SCHEMA_VERSION,
                action_id: import_action_id.clone(),
                created_utc: Utc::now(),
                action: import_action,
                dependencies: Vec::new(),
                output_artifact: import_artifact.clone(),
                output_files: vec![OutputFile {
                    path: "input.ir".to_string(),
                    bytes: ir_bytes.len() as u64,
                    sha256: sha256_hex(ir_bytes),
                }],
                commands: Vec::new(),
                details: json!({}),
                suggested_next_actions: Vec::new(),
            })
            .expect("write imported IR provenance");
        let import_path = store.resolve_artifact_ref_path(&import_artifact);
        fs::create_dir_all(import_path.parent().expect("import artifact parent"))
            .expect("create imported IR directory");
        fs::write(import_path, ir_bytes).expect("write imported IR artifact");

        let candidate_g8r_action = ActionSpec::DriverIrToG8rAig {
            ir_action_id: import_action_id.clone(),
            top_fn_name: Some(top_fn_name.clone()),
            fraig: false,
            lowering_mode: G8rLoweringMode::FrontendNoPrepRewrite,
            execution_recipe_revision: 0,
            version: dso_version.to_string(),
            runtime: candidate_runtime.clone(),
        };
        let candidate_g8r_action_id =
            compute_action_id(&candidate_g8r_action).expect("candidate G8r action ID");
        let candidate_abc_action = ActionSpec::AigToYosysAbcAig {
            aig_action_id: candidate_g8r_action_id.clone(),
            yosys_script_ref: script_ref.clone(),
            runtime: abc_runtime.clone(),
        };
        let candidate_abc_action_id =
            compute_action_id(&candidate_abc_action).expect("candidate ABC action ID");
        let candidate_stats_action = ActionSpec::DriverAigToStats {
            aig_action_id: candidate_abc_action_id.clone(),
            version: dso_version.to_string(),
            runtime: stats_runtime.clone(),
        };
        let candidate_stats_action_id =
            compute_action_id(&candidate_stats_action).expect("candidate stats action ID");

        let baseline_g8r_action = ActionSpec::DriverIrToG8rAig {
            ir_action_id: import_action_id.clone(),
            top_fn_name: Some(top_fn_name.clone()),
            fraig: false,
            lowering_mode: G8rLoweringMode::FrontendNoPrepRewrite,
            execution_recipe_revision: 0,
            version: dso_version.to_string(),
            runtime: stats_runtime.clone(),
        };
        let baseline_g8r_action_id =
            compute_action_id(&baseline_g8r_action).expect("baseline G8r action ID");
        let baseline_abc_action = ActionSpec::AigToYosysAbcAig {
            aig_action_id: baseline_g8r_action_id,
            yosys_script_ref: script_ref.clone(),
            runtime: abc_runtime.clone(),
        };
        let baseline_abc_action_id =
            compute_action_id(&baseline_abc_action).expect("baseline ABC action ID");
        let baseline_stats_action = ActionSpec::DriverAigToStats {
            aig_action_id: baseline_abc_action_id,
            version: dso_version.to_string(),
            runtime: stats_runtime.clone(),
        };
        let baseline_stats_action_id =
            compute_action_id(&baseline_stats_action).expect("baseline stats action ID");

        let candidate_nodes = 20 + index as u64;
        let candidate_depth = 3 + (index % 3) as u64;
        let stats_bytes = serde_json::to_vec(&json!({
            "and_nodes": candidate_nodes,
            "depth": candidate_depth,
        }))
        .expect("serialize candidate stats");
        candidate_store
            .write_provenance(&Provenance {
                schema_version: crate::ACTION_SCHEMA_VERSION,
                action_id: candidate_stats_action_id.clone(),
                created_utc: Utc::now(),
                action: candidate_stats_action,
                dependencies: Vec::new(),
                output_artifact: ArtifactRef {
                    action_id: candidate_stats_action_id.clone(),
                    artifact_type: ArtifactType::AigStatsFile,
                    relpath: crate::corpus::G8R_STATS_RELPATH.to_string(),
                },
                output_files: vec![OutputFile {
                    path: "stats.json".to_string(),
                    bytes: stats_bytes.len() as u64,
                    sha256: sha256_hex(&stats_bytes),
                }],
                commands: Vec::new(),
                details: json!({}),
                suggested_next_actions: Vec::new(),
            })
            .expect("write candidate stats provenance");
        let stats_dir = run_dir.join("artifacts").join(&sample_id);
        fs::create_dir_all(&stats_dir).expect("create candidate stats directory");
        fs::write(stats_dir.join("g8r_stats.json"), &stats_bytes).expect("write candidate stats");

        let baseline_g8r_nodes = (24 + index as u64) as f64;
        let baseline_g8r_levels = 4.0;
        let reference_nodes = (18 + index as u64) as f64;
        let reference_levels = 3.0;
        let reference_stats_action_id = sha256_hex(format!("reference-{index}").as_bytes());
        let release_sample = crate::view::StdlibG8rVsYosysSample {
            fn_key: format!("fixed/{index}"),
            crate_version: baseline_version.to_string(),
            dso_version: dso_version.to_string(),
            stdlib_root_action_id: None,
            ir_action_id: import_action_id.clone(),
            ir_top: Some(top_fn_name.clone()),
            structural_hash: Some(structural_hash.clone()),
            ir_node_count: (index + 1) as u64,
            g8r_nodes: baseline_g8r_nodes,
            g8r_levels: baseline_g8r_levels,
            yosys_abc_nodes: reference_nodes,
            yosys_abc_levels: reference_levels,
            g8r_product: baseline_g8r_nodes * baseline_g8r_levels,
            yosys_abc_product: reference_nodes * reference_levels,
            g8r_product_loss: baseline_g8r_nodes * baseline_g8r_levels
                - reference_nodes * reference_levels,
            g8r_stats_action_id: baseline_stats_action_id.clone(),
            yosys_abc_stats_action_id: reference_stats_action_id.clone(),
        };
        g8r_points.push(StaticCorpusEntityPoint {
            structural_hash: structural_hash.clone(),
            crate_version: baseline_version.to_string(),
            point: StdlibAigStatsPoint {
                fn_key: format!("g8r/{index}"),
                ir_action_id: import_action_id.clone(),
                ir_top: Some(top_fn_name.clone()),
                crate_version: baseline_version.to_string(),
                dso_version: dso_version.to_string(),
                and_nodes: baseline_g8r_nodes,
                depth: baseline_g8r_levels,
                created_utc: Utc::now(),
                stats_action_id: baseline_stats_action_id,
            },
        });
        yosys_points.push(StaticCorpusEntityPoint {
            structural_hash,
            crate_version: baseline_version.to_string(),
            point: StdlibAigStatsPoint {
                fn_key: format!("yosys/{index}"),
                ir_action_id: import_action_id.clone(),
                ir_top: Some(top_fn_name.clone()),
                crate_version: baseline_version.to_string(),
                dso_version: dso_version.to_string(),
                and_nodes: reference_nodes,
                depth: reference_levels,
                created_utc: Utc::now(),
                stats_action_id: reference_stats_action_id,
            },
        });
        release_samples.push(release_sample);
        candidate_samples.push(CandidateSampleInput {
            sample_id,
            source_relpath,
            source_sha256,
            top_fn_name,
            fraig: false,
            dso_version: dso_version.to_string(),
            import_ir_action_id: import_action_id,
            g8r_aig_action_id: candidate_g8r_action_id,
            g8r_stats_action_id: candidate_stats_action_id,
            yosys_abc_aig_action_id: candidate_abc_action_id,
        });
    }
    drop(candidate_store);

    let action_manifest_sha256 =
        candidate_action_manifest_sha256(&candidate_samples).expect("candidate action manifest");
    let mut candidate_run = CandidateRunInput {
        schema_version: 4,
        candidate_run_id: String::new(),
        requested_ref: Some("main".to_string()),
        observed_at_utc: "2026-09-01T12:00:00Z".to_string(),
        candidate_committed_at_utc: Some("2026-08-31T12:00:00Z".to_string()),
        candidate: CandidateGitRevisionInput {
            kind: "git_revision".to_string(),
            repository: crate::XLSYNTH_CRATE_GIT_REPOSITORY.to_string(),
            commit: candidate_commit,
        },
        baseline: CandidateReleaseInput {
            kind: "crate_release".to_string(),
            crate_version: baseline_version.to_string(),
            release_tag: format!("v{baseline_version}"),
            commit: "7".repeat(40),
        },
        dso_version: normalize_tag_version(dso_version).to_string(),
        lowering_mode: "frontend_no_prep_rewrite".to_string(),
        fraig: false,
        execution_recipe_revision: 0,
        driver_runtime: candidate_runtime.clone(),
        abc_runtime: abc_runtime.clone(),
        stats_runtime: stats_runtime.clone(),
        yosys_script: crate::DEFAULT_YOSYS_FLOW_SCRIPT.to_string(),
        yosys_script_sha256: "1".repeat(64),
        action_manifest_sha256,
        cohort_sample_count: RELEASE_PROGRESSION_IR_COUNT as u64,
        cohort_artifact_manifest_sha256: RELEASE_PROGRESSION_ARTIFACT_MANIFEST_SHA256.to_string(),
    };
    candidate_run.candidate_run_id =
        candidate_run_identity_sha256(&candidate_run).expect("candidate run identity");
    let generation_id = candidate_run.candidate_run_id.clone();
    let manifest = CandidateCorpusManifestInput {
        schema_version: 5,
        recipe_preset: "g8r-abc-stats".to_string(),
        fraig: false,
        dso_version: dso_version.to_string(),
        driver_runtime: candidate_runtime,
        stats_runtime,
        yosys_runtime: abc_runtime,
        yosys_script: crate::DEFAULT_YOSYS_FLOW_SCRIPT.to_string(),
        yosys_script_sha256: "1".repeat(64),
        candidate_run: Some(candidate_run),
        samples: candidate_samples,
    };
    fs::write(
        run_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).expect("serialize candidate manifest"),
    )
    .expect("write candidate manifest");
    write_candidate_run_marker(&run_dir, &manifest);

    let comparison = ComparisonSourceIndex {
        schema_version: crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_SCHEMA_VERSION,
        generated_utc: Utc::now(),
        dataset: StdlibG8rVsYosysDataset {
            fraig: false,
            samples: release_samples,
            min_ir_nodes: 1,
            max_ir_nodes: RELEASE_PROGRESSION_IR_COUNT as u64,
            g8r_only_count: 0,
            yosys_only_count: 0,
            available_crate_versions: vec![baseline_version.to_string()],
        },
        g8r_points,
        yosys_points,
    };
    let frontend_bytes =
        serde_json::to_vec(&comparison).expect("serialize frontend comparison source");
    let mut direct_comparison = comparison.clone();
    direct_comparison.schema_version = crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION;
    let direct_bytes =
        serde_json::to_vec(&direct_comparison).expect("serialize direct comparison source");
    store
        .write_web_index_bytes(
            crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME,
            &frontend_bytes,
        )
        .expect("write frontend comparison source");
    store
        .write_web_index_bytes(
            crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME,
            &direct_bytes,
        )
        .expect("write direct comparison source");
    let ir_index = crate::query::build_ir_fn_corpus_ir_index_bytes_for_paired_indices(
        &store,
        &[
            (
                crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME,
                direct_bytes.as_slice(),
                crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION,
            ),
            (
                crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME,
                frontend_bytes.as_slice(),
                crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_SCHEMA_VERSION,
            ),
        ],
    )
    .expect("build paired IR index");
    store
        .write_web_index_bytes(
            crate::WEB_IR_FN_CORPUS_IR_INDEX_FILENAME,
            &ir_index.manifest_bytes,
        )
        .expect("write paired IR manifest");
    for (index_key, bytes) in ir_index.shard_files {
        store
            .write_web_index_bytes(&index_key, &bytes)
            .expect("write paired IR shard");
    }
    let snapshot_dir = root.join("snapshot");
    rebuild_versions_cards_index(&store, &test_repo_root())
        .expect("rebuild versions dataset after source artifacts");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build source snapshot");
    drop(store);

    let site_dir = root.join("site");
    build_static_site_with_candidate_runs(
        &BuildStaticSiteOptions {
            snapshot_dir,
            out_dir: site_dir.clone(),
            base_url: "/".into(),
            overwrite: false,
        },
        &[],
        &[run_dir],
    )
    .expect("build historical candidate site");
    (site_dir, generation_id)
}

#[test]
fn site_verifier_reconstructs_historical_candidate_from_published_evidence() {
    let root = temp_root();
    let (site_dir, generation_id) = build_historical_candidate_site_fixture(&root);
    verify_static_site(&site_dir).expect("verify historical candidate site");
    let catalog_path = site_dir.join("catalog.json");
    let catalog_bytes = fs::read(&catalog_path).expect("read candidate browser catalog");
    let mut catalog =
        decode_canonical_browser_catalog(&catalog_bytes).expect("decode candidate browser catalog");
    assert_eq!(catalog.candidate_evidence.len(), 1);
    assert_eq!(catalog.candidate_evidence[0].generation_id, generation_id);
    assert!(site_dir.join(&catalog.candidate_evidence[0].url).is_file());
    let evidence_url = catalog.candidate_evidence[0].url.clone();
    let candidate = catalog
        .progression
        .generations
        .iter_mut()
        .find(|generation| generation.generation_id == generation_id)
        .expect("candidate generation");
    match &candidate.origin {
        BrowserProgressionOrigin::GitRevision {
            requested_ref,
            baseline_crate_version,
            baseline_release_commit,
            ..
        } => {
            assert_eq!(requested_ref, &None);
            assert_eq!(baseline_crate_version, "0.66.0");
            assert_eq!(baseline_release_commit, &None);
        }
        BrowserProgressionOrigin::CrateRelease => panic!("expected Git candidate"),
    }

    let sample = candidate
        .candidate_samples
        .first_mut()
        .expect("candidate sample");
    sample.g8r_nodes += 1.0;
    sample.g8r_product = sample.g8r_nodes * sample.g8r_levels;
    sample.g8r_product_loss = sample.g8r_product - sample.yosys_abc_product;
    fs::write(
        &catalog_path,
        encode_browser_catalog(&catalog).expect("encode tampered catalog"),
    )
    .expect("write self-consistent tampered catalog");
    refresh_site_manifest_entry(&site_dir, "catalog.json");
    let error = verify_static_site(&site_dir)
        .expect_err("catalog candidate metrics must be reconstructed from evidence");
    assert!(
        format!("{error:#}").contains("release progression projection disagrees"),
        "unexpected error: {error:#}"
    );
    fs::write(&catalog_path, &catalog_bytes).expect("restore candidate browser catalog");
    refresh_site_manifest_entry(&site_dir, "catalog.json");
    let evidence_path = site_dir.join(&evidence_url);
    let mut evidence = decode_canonical_candidate_progression_evidence(
        &fs::read(&evidence_path).expect("read candidate evidence"),
    )
    .expect("decode candidate evidence");
    evidence.stats[0].and_nodes += 1.0;
    let tampered_evidence_bytes =
        encode_candidate_progression_evidence(&evidence).expect("encode tampered evidence");
    fs::write(&evidence_path, &tampered_evidence_bytes).expect("write tampered candidate evidence");
    let mut evidence_catalog =
        decode_canonical_browser_catalog(&catalog_bytes).expect("decode restored browser catalog");
    let evidence_ref = evidence_catalog
        .candidate_evidence
        .iter_mut()
        .find(|candidate| candidate.url == evidence_url)
        .expect("candidate evidence catalog reference");
    evidence_ref.bytes = tampered_evidence_bytes.len() as u64;
    evidence_ref.sha256 = sha256_hex(&tampered_evidence_bytes);
    fs::write(
        &catalog_path,
        encode_browser_catalog(&evidence_catalog).expect("encode evidence catalog"),
    )
    .expect("write evidence catalog");
    refresh_site_manifest_entry(&site_dir, "catalog.json");
    refresh_site_manifest_entry(&site_dir, &evidence_url);
    let error = verify_static_site(&site_dir)
        .expect_err("candidate metrics must match their public projection digest");
    assert!(
        format!("{error:#}").contains("metric projection digest"),
        "unexpected error: {error:#}"
    );
    fs::remove_dir_all(root).expect("cleanup candidate site fixture");
}

#[test]
fn candidate_preflight_rejects_malformed_commit_without_panicking() {
    let root = temp_root();
    let run_dir = root.join("candidate");
    fs::create_dir_all(&run_dir).expect("create candidate run directory");
    let manifest = candidate_manifest_input("short");
    fs::write(
        run_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).expect("serialize candidate manifest"),
    )
    .expect("write candidate manifest");
    write_candidate_run_marker(&run_dir, &manifest);

    let error = preflight_candidate_run_dirs(&[run_dir])
        .expect_err("malformed commit must fail candidate preflight");
    assert!(
        error
            .chain()
            .any(|cause| cause.to_string().contains("driver Git commit must be"))
    );
    fs::remove_dir_all(root).expect("cleanup candidate fixture");
}

#[test]
fn candidate_preflight_requires_matching_durable_marker() {
    let root = temp_root();
    let run_dir = root.join("candidate");
    fs::create_dir_all(&run_dir).expect("create candidate run directory");
    let manifest = candidate_manifest_input(&"8".repeat(40));
    fs::write(
        run_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).expect("serialize candidate manifest"),
    )
    .expect("write candidate manifest");

    let missing_error = preflight_candidate_run_dirs(std::slice::from_ref(&run_dir))
        .expect_err("candidate manifest without marker must fail");
    assert!(
        format!("{missing_error:#}").contains("no durable workspace marker"),
        "unexpected error: {missing_error:#}"
    );

    let other_manifest = candidate_manifest_input(&"7".repeat(40));
    write_candidate_run_marker(&run_dir, &other_manifest);
    let mismatch_error = preflight_candidate_run_dirs(std::slice::from_ref(&run_dir))
        .expect_err("candidate manifest and marker identities must match");
    assert!(
        format!("{mismatch_error:#}").contains("identity does not match"),
        "unexpected error: {mismatch_error:#}"
    );
    fs::remove_dir_all(root).expect("cleanup candidate marker fixture");
}

fn empty_versions_index_bytes() -> Vec<u8> {
    let root = temp_root().join("versions-fixture-store");
    let store = ArtifactStore::new(root.clone());
    store.ensure_layout().expect("versions fixture layout");
    rebuild_versions_cards_index(&store, &test_repo_root()).expect("build versions fixture");
    let bytes = store
        .load_web_index_bytes(crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME)
        .expect("load versions fixture")
        .expect("versions fixture exists");
    drop(store);
    fs::remove_dir_all(root).expect("cleanup versions fixture");
    bytes
}

fn refresh_site_manifest_entry(site_dir: &Path, relpath: &str) {
    let manifest_path = site_dir.join(STATIC_SITE_MANIFEST_FILENAME);
    let mut manifest = pb::StaticSiteManifest::decode(
        fs::read(&manifest_path)
            .expect("read site manifest")
            .as_slice(),
    )
    .expect("decode site manifest");
    let replacement = publication_file(site_dir, relpath).expect("describe changed site file");
    let slot = manifest
        .files
        .iter_mut()
        .find(|file| {
            file.relpath
                .as_ref()
                .is_some_and(|value| value.value == relpath)
        })
        .expect("declared site file");
    *slot = replacement;
    fs::write(manifest_path, manifest.encode_to_vec()).expect("rewrite site manifest");
}

#[test]
fn generated_site_links_are_relocatable() {
    assert_eq!(site_root_url("index.html").expect("root URL"), "./");
    assert_eq!(
        site_root_url("runs/abc/index.html").expect("nested root URL"),
        "../../"
    );
    assert_eq!(
        resolve_site_link("runs/abc/index.html", "../../assets/site.css").expect("nested asset"),
        "assets/site.css"
    );
    assert_eq!(
        resolve_site_link("runs/abc/index.html", "../../").expect("root page"),
        "index.html"
    );
    assert!(resolve_site_link("index.html", "/xlsynth-bvc/assets/site.css").is_err());
}

#[test]
fn site_versions_loader_rejects_truncated_release_ledger() {
    let root = temp_root();
    let dataset_relpath = format!("data/{}", crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME);
    let dataset_path = root.join(&dataset_relpath);
    fs::create_dir_all(dataset_path.parent().expect("dataset parent"))
        .expect("create dataset parent");

    let mut versions: crate::query::VersionsSummaryIndexFile =
        serde_json::from_slice(&empty_versions_index_bytes()).expect("decode versions fixture");
    versions
        .report
        .releases
        .pop()
        .expect("remove an older release row");
    let bytes = serde_json::to_vec(&versions).expect("encode truncated versions fixture");
    fs::write(&dataset_path, &bytes).expect("write truncated versions fixture");

    let datasets = vec![BrowserDataset {
        logical_key: crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME.to_string(),
        url: dataset_relpath,
        bytes: bytes.len() as u64,
        sha256: sha256_hex(&bytes),
    }];
    let error = load_versions_report_from_site(&root, &datasets)
        .expect_err("truncated release ledger must fail site loading");
    assert!(
        format!("{error:#}").contains("embedded compatibility map has"),
        "unexpected error: {error:#}"
    );

    fs::remove_dir_all(root).expect("cleanup site fixture");
}

#[test]
fn structural_static_manifest_advertises_resolvable_shards() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write versions dataset");

    let structural_hash = "7".repeat(64);
    let group = crate::model::IrFnCorpusStructuralGroupFile {
        schema_version: crate::IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION,
        structural_hash: structural_hash.clone(),
        members: vec![crate::model::IrFnCorpusStructuralMember {
            opt_ir_action_id: "1".repeat(64),
            source_ir_action_id: "2".repeat(64),
            ir_top: "__sample".to_string(),
            ir_fn_signature: Some("fn __sample(x: bits[1]) -> bits[1]".to_string()),
            ir_op_count: Some(1),
            crate_version: "0.68.0".to_string(),
            dso_version: "0.68.0".to_string(),
            created_utc: Utc::now(),
            output_artifact: crate::model::ArtifactRef {
                action_id: "1".repeat(64),
                artifact_type: crate::model::ArtifactType::IrPackageFile,
                relpath: "payload/result.ir".to_string(),
            },
            output_file_sha256: "3".repeat(64),
            output_file_bytes: 42,
            hash_source: "dependency_hint".to_string(),
            hash_hint_source_action_ids: vec!["4".repeat(64)],
            dslx_origin: None,
            producer_action_kind: Some("driver_ir_to_opt".to_string()),
        }],
    };
    let group_bytes = serde_json::to_vec_pretty(&group).expect("serialize structural group");
    let group_key = format!(
        "{}/{}",
        crate::WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_NAMESPACE,
        crate::service::hash_group_relpath(&structural_hash)
    );
    store
        .write_web_index_bytes(&group_key, &group_bytes)
        .expect("write structural group");
    let source_manifest = crate::model::IrFnCorpusStructuralManifest {
        schema_version: crate::IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION,
        generated_utc: Utc::now(),
        recompute_missing_hashes: false,
        total_actions_scanned: 1,
        total_driver_ir_to_opt_actions: 1,
        total_ir_fn_to_k_bool_cone_corpus_actions: 0,
        indexed_actions: 1,
        indexed_k_bool_cone_members: 0,
        distinct_structural_hashes: 1,
        hash_from_dependency_hint_count: 1,
        hash_recomputed_count: 0,
        hash_hint_conflict_count: 0,
        skipped_missing_output_count: 0,
        skipped_missing_ir_top_count: 0,
        skipped_missing_hash_hint_count: 0,
        skipped_hash_error_count: 0,
        skipped_k_bool_cone_manifest_errors: 0,
        skipped_k_bool_cone_empty_count: 0,
        source_action_set_sha256: Some("5".repeat(64)),
        groups: vec![crate::model::IrFnCorpusStructuralManifestGroup {
            structural_hash: structural_hash.clone(),
            member_count: 1,
            relpath: crate::service::hash_group_relpath(&structural_hash),
            content_sha256: sha256_hex(&group_bytes),
            ir_node_count: Some(1),
        }],
    };
    store
        .write_web_index_bytes(
            crate::WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY,
            &serde_json::to_vec_pretty(&source_manifest).expect("serialize source manifest"),
        )
        .expect("write structural manifest");

    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let site_dir = root.join("site");
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/".into(),
        overwrite: false,
    })
    .expect("build site");

    let manifest_path = site_dir
        .join("data")
        .join(crate::WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY);
    let static_manifest: StaticStructuralManifest =
        serde_json::from_slice(&fs::read(&manifest_path).expect("read static manifest"))
            .expect("decode static manifest");
    assert_eq!(
        static_manifest.source.logical_key,
        crate::WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY
    );
    assert_eq!(
        static_manifest.source.manifest.distinct_structural_hashes,
        1
    );
    assert_eq!(static_manifest.shards.len(), 1);
    let shard = &static_manifest.shards[0];
    assert_eq!(shard.prefix, "77");
    assert_eq!(shard.group_count, 1);
    assert_eq!(shard.member_count, 1);
    assert!(site_dir.join("data").join(&shard.index_key).is_file());
    assert!(
        !site_dir.join("data").join(&group_key).exists(),
        "per-hash compatibility files would exceed the hosting file limit"
    );
    verify_static_site(&site_dir).expect("verify site");
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_build_and_verify_supports_subdirectory_base() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write dataset");
    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let site_dir = root.join("site");
    let summary = build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/xlsynth-bvc/".into(),
        overwrite: false,
    })
    .expect("build site");
    assert_eq!(summary.dataset_count, 1);
    let verified = verify_static_site(&site_dir).expect("verify site");
    assert_eq!(verified.base_url, "/xlsynth-bvc/");
    let catalog: BrowserCatalog = serde_json::from_slice(
        &fs::read(site_dir.join("catalog.json")).expect("read browser catalog"),
    )
    .expect("decode browser catalog");
    assert_eq!(catalog.schema_version, BROWSER_CATALOG_SCHEMA_VERSION);
    let index_html = fs::read_to_string(site_dir.join("index.html")).expect("read homepage HTML");
    assert!(index_html.contains("Boolean synthesis comparison"));
    assert!(index_html.contains("id=\"home-overview\""));
    assert!(index_html.contains("paired IR samples"));
    assert!(
        index_html.contains(crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME)
    );
    assert_eq!(index_html.matches("class=\"analysis-link\"").count(), 4);
    assert!(index_html.contains("Raw datasets"));
    assert!(index_html.contains("Campaign runs"));
    assert!(index_html.contains(PLOTLY_ASSET_NAME));
    let (_, js_name) = static_site_asset_names();
    assert!(index_html.contains(&js_name));
    assert!(!index_html.contains("<h2>Datasets</h2>"));
    assert!(!index_html.contains("class=\"feature-card\""));
    assert!(!index_html.contains(crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME));
    assert!(
        !fs::read_to_string(site_dir.join("dataset.html"))
            .expect("read HTML")
            .contains("/api/")
    );
    let progression_html =
        fs::read_to_string(site_dir.join("progression.html")).expect("read progression HTML");
    assert!(
        progression_html
            .contains(crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME)
    );
    assert!(progression_html.contains("Quality versus distribution"));
    assert!(progression_html.contains("id=\"include-incomplete\""));
    assert!(progression_html.contains("id=\"progression-inventory\""));
    let releases_html =
        fs::read_to_string(site_dir.join("releases.html")).expect("read releases HTML");
    assert!(releases_html.contains("Crate release processing"));
    assert!(releases_html.contains("Release ledger"));
    assert!(releases_html.contains("not processed"));
    assert!(index_html.contains("Processing status"));
    assert!(APP_JS.contains("Progression data is not available in this snapshot."));
    assert!(APP_JS.contains("At least two cohort-complete generations are needed"));
    assert!(APP_JS.contains("Aggregate quality"));
    assert!(APP_JS.contains("sample.structural_hash"));
    assert!(APP_JS.contains("contains duplicate fixed IR"));
    assert!(APP_JS.contains("query.get('all_versions')==='true'"));
    assert!(APP_JS.contains("Median paired per-artifact change"));
    assert!(APP_JS.contains("Current-only and baseline-only"));
    assert!(APP_JS.contains("Largest per-artifact changes: ${comparisonHeading}"));
    assert!(APP_JS.contains("Change<br>${esc(currentLabel)} − ${esc(baselineLabel)}"));
    assert!(APP_JS.contains("completeGenerations=generations.filter"));
    assert!(APP_JS.contains("Incomplete generations are never selected by default or plotted"));
    assert!(APP_JS.contains("Incomplete fixed-IR warning"));
    assert!(!APP_JS.contains("Degraded runs are never selected by default or plotted"));
    assert!(!APP_JS.contains("Selected campaign generation is unavailable"));
    let comparison_html =
        fs::read_to_string(site_dir.join("ir-fn-corpus-g8r-vs-yosys-abc/index.html"))
            .expect("read comparison HTML");
    assert!(comparison_html.contains(crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME));
    assert!(comparison_html.contains("../assets/plotly-2.35.2.min.js"));
    assert!(!comparison_html.contains("cdn.plot.ly"));
    assert!(comparison_html.contains("script-src 'self';"));
    assert!(comparison_html.contains("style-src 'self' 'unsafe-inline';"));
    assert!(comparison_html.contains("../assets/"));
    assert_eq!(
        fs::read(site_dir.join(format!("assets/{PLOTLY_ASSET_NAME}")))
            .expect("read local Plotly asset"),
        PLOTLY_JS
    );
    assert!(
        site_dir
            .join(format!("assets/{PLOTLY_LICENSE_ASSET_NAME}"))
            .is_file()
    );
    assert!(
        site_dir
            .join(format!("assets/{PLOTLY_NOTICE_ASSET_NAME}"))
            .is_file()
    );

    let frontend_comparison_html =
        fs::read_to_string(site_dir.join("ir-fn-g8r-abc-vs-codegen-yosys-abc/index.html"))
            .expect("read frontend comparison HTML");
    assert!(
        frontend_comparison_html
            .contains(crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME)
    );
    assert!(frontend_comparison_html.contains(crate::WEB_IR_FN_CORPUS_IR_INDEX_FILENAME));
    assert!(frontend_comparison_html.contains("id=\"comparison-detail-ir\""));
    assert!(frontend_comparison_html.contains("id=\"comparison-detail-raw\""));
    assert!(frontend_comparison_html.contains("Raw sample metadata"));
    assert!(APP_JS.contains("loadIrFnCorpusIrEntry"));
    assert!(APP_JS.contains("data-copy-comparison-ir"));
    assert!(APP_JS.contains("Loading exact XLS IR"));
    assert!(frontend_comparison_html.contains("G8r+ABC vs codegen+Yosys/ABC"));
    assert!(APP_JS.contains("renderComparisonQuadrants"));
    assert!(APP_JS.contains("renderComparisonLoss"));
    assert!(!APP_JS.contains("scattergl"));
    assert!(!STYLE_CSS.contains(".table-wrap{max-height:60vh;overflow:auto}svg{"));
    assert!(STYLE_CSS.contains(".progression-chart-grid{display:grid"));
    assert!(APP_JS.contains("losses_only"));
    assert!(APP_JS.contains("comparisonSelectionKey"));
    let mffc_html = fs::read_to_string(site_dir.join("mffc-discrepancies.html"))
        .expect("read MFFC discrepancies HTML");
    assert!(mffc_html.contains("MFFC discrepancies"));
    assert!(
        mffc_html.contains(crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME)
    );
    assert!(mffc_html.contains(crate::WEB_IR_FN_CORPUS_IR_INDEX_FILENAME));
    assert!(mffc_html.contains("positive means G8r is worse"));
    assert!(APP_JS.contains("No paired MFFC samples are available in this snapshot."));
    assert!(APP_JS.contains("Largest G8r product losses"));
    assert!(APP_JS.contains("Both evidence paths reference this same action and top."));
    assert!(APP_JS.contains("entry.g8r_stats_action_id===sample.g8r_stats_action_id"));
    assert!(APP_JS.contains("entry.yosys_abc_stats_action_id===sample.yosys_abc_stats_action_id"));
    assert!(APP_JS.contains("mffcIrPanel('G8r'"));
    assert!(APP_JS.contains("mffcIrPanel('Yosys/ABC'"));
    assert!(APP_JS.contains("Representative subject"));
    assert!(APP_JS.contains("Yosys/ABC IR action"));
    let initial_render = APP_JS
        .split_once("async function mffcDiscrepancies")
        .expect("MFFC page initializer")
        .1
        .split_once("async function datasetExplorer")
        .expect("next page initializer")
        .0;
    assert!(!initial_render.contains("loadMffcIrIndex"));
    assert!(initial_render.contains("mffcDetailGeneration++"));
    let detail_render = APP_JS
        .split_once("async function showMffcDetail")
        .expect("MFFC detail renderer")
        .1
        .split_once("function mffcUnavailable")
        .expect("end of MFFC detail renderer")
        .0;
    assert!(detail_render.contains("loadMffcIrEntry"));
    assert_eq!(
        detail_render
            .matches("generation!==mffcDetailGeneration")
            .count(),
        2
    );
    assert!(APP_JS.contains("rankMffcSamples"));
    assert!(!APP_JS.contains("absolute difference"));
    assert!(!APP_JS.contains("after?.median??0"));
    assert!(
        fs::read_to_string(site_dir.join("index.html"))
            .expect("read index HTML")
            .contains("progression.html")
    );
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn homepage_javascript_selects_latest_published_crate_release() {
    const SCRIPT: &str = r#"
const fs = require('fs');
let capturedLayout = null;
let capturedTraces = null;
global.document = {
  querySelector: () => ({content: ''}),
  getElementById: () => null,
};
global.Plotly = {
  react: (_id, traces, layout) => {
capturedTraces = traces;
capturedLayout = layout;
return Promise.resolve();
  },
};
const app = fs.readFileSync(0, 'utf8');
const prefix = app.slice(0, app.indexOf('async function main()'));
const api = new Function(prefix + '\nreturn {homepageSummary,homepageExplorerHref,homepagePairPlot,renderComparisonPair};')();
const sample = (crate_version, fn_key, g8r_nodes, g8r_levels, yosys_abc_nodes, yosys_abc_levels, g8r_product_loss, ir_top = fn_key) => ({
  crate_version,
  dso_version: crate_version,
  fn_key,
  ir_top,
  ir_node_count: 20,
  g8r_nodes,
  g8r_levels,
  yosys_abc_nodes,
  yosys_abc_levels,
  g8r_product_loss,
});
const samples = [
  sample('0.66.0', 'win', 10, 5, 12, 7, -34),
  sample('0.66.0', 'loss', 12, 8, 10, 6, 36),
  sample('0.66.0', 'tie', 1, 1, 1, 1, 0),
  sample('0.68.0', 'partial', 10, 5, 12, 7, -34),
  ...Array.from({length: 4}, (_, i) => sample('0.68.0', `mffc-${i}`, 1, 1, 1, 1, 0, `__mffc_${i}`)),
  ...Array.from({length: 4}, (_, i) => sample('0.68.0', `k3-${i}`, 1, 1, 1, 1, 0, `__k3_cone_${i}`)),
  {...sample('0.68.0', 'stale-dso', 100, 100, 1, 1, 9999), dso_version: '0.67.0'},
];
const summary = api.homepageSummary(samples);
if (JSON.stringify(summary.versions) !== JSON.stringify(['0.66.0', '0.68.0']) || summary.latestVersion !== '0.68.0' || summary.samples.length !== 9) {
  throw new Error(`unexpected latest-release summary: ${JSON.stringify(summary)}`);
}
if (summary.selection !== 'latest crate release v0.68.0 · DSO v0.68.0' || summary.wholeFunctionCount !== 1 || summary.mffcCount !== 4 || summary.k3Count !== 4) {
  throw new Error(`unexpected latest-release policy: ${JSON.stringify(summary)}`);
}
if (summary.pureWins !== 1 || summary.strictLosses !== 0 || summary.medianLoss !== 0) {
  throw new Error(`unexpected quadrant summary: ${JSON.stringify(summary)}`);
}
const semanticSummary = api.homepageSummary([
  sample('0.9.0', 'older', 1, 1, 1, 1, 0),
  sample('0.10.0', 'newer', 1, 1, 1, 1, 0),
]);
if (semanticSummary.latestVersion !== '0.10.0' || semanticSummary.samples.length !== 1 || semanticSummary.samples[0].fn_key !== 'newer') {
  throw new Error(`latest release was not selected semantically: ${JSON.stringify(semanticSummary)}`);
}
api.homepagePairPlot('test-plot', [sample('0.66.0', 'zero', 0, 0, 0, 0, 0)], 'lhs', 'rhs', value => value.g8r_nodes, value => value.yosys_abc_nodes, {lhs: 'lhs', rhs: 'rhs'});
if (!capturedLayout?.annotations?.[0]?.text.includes('1 zero-valued pair plotted at 1 for log scale')) {
  throw new Error(`missing zero-clamp disclosure: ${JSON.stringify(capturedLayout)}`);
}
const largeSamples = Array.from({length: 70000}, (_, i) => sample('0.68.0', 'large-' + i, i + 1, i + 1, i + 2, i + 2, 0));
api.homepagePairPlot('large-test-plot', largeSamples, 'lhs', 'rhs', value => value.g8r_nodes, value => value.yosys_abc_nodes, {lhs: 'lhs', rhs: 'rhs'});
const referenceLine = capturedTraces.at(-1);
if (JSON.stringify(referenceLine.x) !== JSON.stringify([1, 70001]) || JSON.stringify(referenceLine.y) !== JSON.stringify([1, 70001])) {
  throw new Error('unexpected large-corpus bounds: ' + JSON.stringify(referenceLine));
}

api.renderComparisonPair('large-explorer-plot', 'large explorer', 'lhs', 'rhs', largeSamples, value => value.g8r_nodes, value => value.yosys_abc_nodes, {lhs: 'lhs', rhs: 'rhs'});
const explorerReferenceLine = capturedTraces.at(-1);
if (JSON.stringify(explorerReferenceLine.x) !== JSON.stringify([1, 70001]) || JSON.stringify(explorerReferenceLine.y) !== JSON.stringify([1, 70001])) {
  throw new Error('unexpected large-explorer bounds: ' + JSON.stringify(explorerReferenceLine));
}

const href = api.homepageExplorerHref({
  crate_version: '0.68.0',
  g8r_stats_action_id: 'g8r-action',
  yosys_abc_stats_action_id: 'yosys-action',
}, true);
if (href !== 'ir-fn-g8r-abc-vs-codegen-yosys-abc/?crate_version=0.68.0&losses_only=true&sample=g8r-action%3Ayosys-action') {
  throw new Error(`unexpected homepage explorer link: ${href}`);
}
"#;
    let mut child = match Command::new("node")
        .arg("-e")
        .arg(SCRIPT)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => panic!("spawn node: {error}"),
    };
    child
        .stdin
        .as_mut()
        .expect("node stdin")
        .write_all(APP_JS.as_bytes())
        .expect("write app js");
    let output = child.wait_with_output().expect("wait for node");
    assert!(
        output.status.success(),
        "node failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn progression_catalog_uses_fixed_ir_structural_hash_population() {
    fn sample(
        crate_version: &str,
        structural_hash: &str,
        fn_key: &str,
    ) -> crate::view::StdlibG8rVsYosysSample {
        crate::view::StdlibG8rVsYosysSample {
            fn_key: fn_key.to_string(),
            crate_version: crate_version.to_string(),
            dso_version: "0.1.0".to_string(),
            stdlib_root_action_id: None,
            ir_action_id: format!("ir-{crate_version}-{fn_key}"),
            ir_top: Some(format!("top_{fn_key}")),
            structural_hash: Some(structural_hash.to_string()),
            ir_node_count: 1,
            g8r_nodes: 1.0,
            g8r_levels: 1.0,
            yosys_abc_nodes: 1.0,
            yosys_abc_levels: 1.0,
            g8r_product: 1.0,
            yosys_abc_product: 1.0,
            g8r_product_loss: 0.0,
            g8r_stats_action_id: sha256_hex(format!("g8r-{crate_version}-{fn_key}").as_bytes()),
            yosys_abc_stats_action_id: sha256_hex(
                format!("yosys-{crate_version}-{fn_key}").as_bytes(),
            ),
        }
    }

    let hashes = release_progression_ir_hashes().expect("pinned fixed IR hashes");
    assert_eq!(hashes.len(), RELEASE_PROGRESSION_IR_COUNT);
    let extra_hash = "0".repeat(64);
    assert!(!hashes.contains(&extra_hash));
    let mut samples = Vec::new();
    for (index, hash) in hashes.iter().enumerate() {
        samples.push(sample("0.1.0", hash, &format!("original-{index}")));
        samples.push(sample("0.2.0", hash, &format!("renamed-{index}")));
    }
    for (index, hash) in hashes[..hashes.len() - 1].iter().enumerate() {
        samples.push(sample("0.3.0", hash, &format!("partial-{index}")));
        samples.push(sample("0.4.0", hash, &format!("changed-{index}")));
    }
    samples.push(sample("0.2.0", &extra_hash, "unrelated-complete-extra"));
    samples.push(sample("0.4.0", &extra_hash, "extra"));
    let mut generated = sample("0.4.0", &extra_hash, "generated");
    generated.ir_top = Some("__k3_cone_generated".to_string());
    samples.push(generated);
    let dataset = StdlibG8rVsYosysDataset {
        fraig: false,
        samples,
        min_ir_nodes: 1,
        max_ir_nodes: 1,
        g8r_only_count: 0,
        yosys_only_count: 0,
        available_crate_versions: vec![
            "0.1.0".to_string(),
            "0.2.0".to_string(),
            "0.3.0".to_string(),
            "0.4.0".to_string(),
        ],
    };

    let catalog = build_browser_progression_catalog(&dataset).expect("build progression catalog");
    assert_eq!(
        catalog.dataset_key,
        crate::WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME
    );
    assert_eq!(catalog.cohort_ir_count, RELEASE_PROGRESSION_IR_COUNT as u64);
    assert_eq!(&catalog.cohort_ir_hashes, &hashes);
    assert_eq!(
        catalog.cohort_ir_sha256.as_deref(),
        Some(
            progression_ir_sha256(&hashes)
                .expect("cohort digest")
                .as_str()
        )
    );
    assert_eq!(catalog.cohort_complete_generation_count, 2);
    assert_eq!(catalog.generations.len(), 4);

    let generation = |version: &str| {
        catalog
            .generations
            .iter()
            .find(|generation| generation.crate_version.as_deref() == Some(version))
            .expect("generation")
    };
    assert_eq!(
        generation("0.1.0").coverage,
        BrowserProgressionCoverage::CohortComplete
    );
    assert_eq!(
        generation("0.2.0").coverage,
        BrowserProgressionCoverage::CohortComplete
    );
    assert_eq!(
        generation("0.2.0").observed_ir_count,
        RELEASE_PROGRESSION_IR_COUNT as u64
    );
    assert_eq!(generation("0.2.0").extra_ir_count, 1);

    let artifacts = release_progression_ir_artifacts().expect("fixed artifact manifest");
    let candidate_samples = hashes
        .iter()
        .map(|structural_hash| {
            let baseline = dataset
                .samples
                .iter()
                .find(|sample| {
                    sample.crate_version == "0.2.0"
                        && sample.structural_hash.as_ref() == Some(structural_hash)
                })
                .expect("baseline sample");
            let source_sha256 = artifacts
                .get(structural_hash)
                .expect("fixed artifact source hash")
                .clone();
            let ir_action_id = compute_action_id(&ActionSpec::ImportIrPackageFile {
                source_sha256: source_sha256.clone(),
                top_fn_name: baseline.ir_top.clone(),
            })
            .expect("candidate IR action ID");
            BrowserCandidateProgressionSample {
                fn_key: baseline.fn_key.clone(),
                structural_hash: structural_hash.clone(),
                source_sha256,
                ir_node_count: baseline.ir_node_count,
                g8r_nodes: baseline.g8r_nodes,
                g8r_levels: baseline.g8r_levels,
                yosys_abc_nodes: baseline.yosys_abc_nodes,
                yosys_abc_levels: baseline.yosys_abc_levels,
                g8r_product: baseline.g8r_product,
                yosys_abc_product: baseline.yosys_abc_product,
                g8r_product_loss: baseline.g8r_product_loss,
                ir_action_id,
                g8r_stats_action_id: sha256_hex(
                    format!("candidate-g8r-{structural_hash}").as_bytes(),
                ),
                yosys_abc_stats_action_id: baseline.yosys_abc_stats_action_id.clone(),
            }
        })
        .collect();
    let candidate = BrowserProgressionGeneration {
        generation_id: "a".repeat(64),
        origin: BrowserProgressionOrigin::GitRevision {
            repository: crate::XLSYNTH_CRATE_GIT_REPOSITORY.to_string(),
            commit: "b".repeat(40),
            requested_ref: None,
            baseline_crate_version: "0.2.0".to_string(),
            baseline_release_commit: None,
        },
        display_label: "git@bbbbbbbb".to_string(),
        event_time_utc: Some("2026-01-01T00:00:00Z".to_string()),
        crate_version: None,
        dso_version: "0.1.0".to_string(),
        baseline_generation_id: Some(generation("0.2.0").generation_id.clone()),
        coverage: BrowserProgressionCoverage::CohortComplete,
        observed_ir_count: RELEASE_PROGRESSION_IR_COUNT as u64,
        cohort_ir_count: RELEASE_PROGRESSION_IR_COUNT as u64,
        missing_cohort_ir_count: 0,
        extra_ir_count: 0,
        candidate_samples,
    };
    let observation = RepositoryHeadObservationView {
        schema_version: 2,
        repository: crate::XLSYNTH_CRATE_GIT_REPOSITORY.to_string(),
        version_compat_sha256: "c".repeat(64),
        observed_at_utc: "2026-01-02T00:00:00Z".to_string(),
        head_ref: "main".to_string(),
        head_commit: "d".repeat(40),
        head_committed_at_utc: "2026-01-02T00:00:00Z".to_string(),
        latest_crate_version: "0.4.0".to_string(),
        latest_release_tag: "v0.4.0".to_string(),
        latest_release_commit: "e".repeat(40),
        latest_release_committed_at_utc: "2025-12-31T00:00:00Z".to_string(),
        comparison_status: "ahead".to_string(),
        commits_ahead: 1,
        commits_behind: 0,
    };
    validate_candidate_progression_generation(&candidate, &catalog, &dataset, Some(&observation))
        .expect("canonical candidate accepts unrelated baseline samples");

    let partial = generation("0.3.0");
    assert_eq!(partial.coverage, BrowserProgressionCoverage::Partial);
    assert_eq!(partial.missing_cohort_ir_count, 1);
    assert_eq!(partial.extra_ir_count, 0);

    let incompatible = generation("0.4.0");
    assert_eq!(
        incompatible.coverage,
        BrowserProgressionCoverage::Incompatible
    );
    assert_eq!(
        incompatible.observed_ir_count,
        (RELEASE_PROGRESSION_IR_COUNT - 1) as u64
    );
    assert_eq!(incompatible.missing_cohort_ir_count, 1);
    assert_eq!(incompatible.extra_ir_count, 1);

    let mut mixed_dso = dataset.clone();
    for sample in mixed_dso
        .samples
        .iter_mut()
        .filter(|sample| sample.crate_version == "0.1.0")
    {
        sample.dso_version = "0.9.0".to_string();
    }
    let mut recomputed = mixed_dso.samples[0].clone();
    recomputed.dso_version = "0.10.0".to_string();
    recomputed.g8r_stats_action_id = "g8r-recomputed".to_string();
    recomputed.yosys_abc_stats_action_id = "yosys-recomputed".to_string();
    mixed_dso.samples.push(recomputed);
    let mixed_catalog = build_browser_progression_catalog(&mixed_dso)
        .expect("rolling DSO updates keep the static site publishable");
    assert_eq!(
        mixed_catalog.cohort_ir_count,
        RELEASE_PROGRESSION_IR_COUNT as u64
    );
    assert_eq!(mixed_catalog.cohort_complete_generation_count, 2);
    assert_eq!(mixed_catalog.generations.len(), 5);
    let rolling_generations = mixed_catalog
        .generations
        .iter()
        .filter(|generation| generation.crate_version.as_deref() == Some("0.1.0"))
        .collect::<Vec<_>>();
    assert_eq!(rolling_generations.len(), 2);
    assert_eq!(rolling_generations[0].dso_version, "0.9.0");
    assert_eq!(
        rolling_generations[0].observed_ir_count,
        RELEASE_PROGRESSION_IR_COUNT as u64
    );
    assert_eq!(rolling_generations[1].dso_version, "0.10.0");
    assert_eq!(rolling_generations[1].observed_ir_count, 1);
    assert_eq!(
        rolling_generations[1].coverage,
        BrowserProgressionCoverage::Partial
    );

    let mut missing_hash = dataset.clone();
    missing_hash.samples[0].structural_hash = None;
    let error = build_browser_progression_catalog(&missing_hash)
        .expect_err("whole-function fixed IR must have a structural hash");
    assert!(error.to_string().contains("has no valid structural hash"));

    let mut generated_only = dataset.clone();
    generated_only.samples.retain(|sample| {
        sample
            .ir_top
            .as_deref()
            .is_some_and(|top| top.starts_with("__k3_cone_"))
    });
    let unavailable = build_browser_progression_catalog(&generated_only)
        .expect("generated-only datasets keep the site publishable");
    assert_eq!(
        unavailable.cohort_ir_count,
        RELEASE_PROGRESSION_IR_COUNT as u64
    );
    assert_eq!(
        unavailable.cohort_ir_hashes.len(),
        RELEASE_PROGRESSION_IR_COUNT
    );
    assert_eq!(
        unavailable.cohort_ir_sha256.as_deref(),
        Some(RELEASE_PROGRESSION_IR_SHA256)
    );
    assert_eq!(unavailable.cohort_complete_generation_count, 0);
    assert!(unavailable.generations.is_empty());
}

#[test]
fn progression_javascript_uses_fixed_ir_pairing_and_aggregate_quality() {
    const SCRIPT: &str = r#"
const fs = require('fs');
global.document = {
  querySelector: () => ({content: ''}),
  getElementById: () => null,
};
const app = fs.readFileSync(0, 'utf8');
const prefix = app.slice(0, app.indexOf('async function main()'));
const api = new Function(prefix + '\nreturn {compareSamples,medianPairedProductLossChange,progressionSelection,releaseGenerations,releaseStats,renderGenerationPair};')();
const hash = value => value.repeat(64);
const sample = (fn_key, hashValue, g8r_product_loss, g8r_product = 100 + g8r_product_loss, yosys_abc_product = 100) => ({
  fn_key,
  ir_top: fn_key,
  structural_hash: hash(hashValue),
  g8r_product_loss,
  g8r_product,
  yosys_abc_product,
});
const before = [sample('old-a', 'a', 0), sample('old-b', 'b', 100), sample('old-c', 'c', 101)];
const after = [sample('new-a', 'a', 99), sample('new-b', 'b', 98), sample('new-c', 'c', 102)];
const {pairs} = api.compareSamples(before, after);
const actual = api.medianPairedProductLossChange(pairs);
if (actual !== 1 || pairs[0].key.length !== 64) {
  throw new Error(`expected structural-hash paired median delta 1, got ${actual}`);
}
const one = api.progressionSelection([{generation_id: 'only'}]);
if (one.baseline !== '' || one.current !== 'only') {
  throw new Error(`one generation must not compare to itself: ${JSON.stringify(one)}`);
}
const two = api.progressionSelection([{generation_id: 'before'}, {generation_id: 'after'}]);
if (two.baseline !== 'before' || two.current !== 'after') {
  throw new Error(`two generations must default to a distinct pair: ${JSON.stringify(two)}`);
}
const duplicate = api.progressionSelection(
  [{generation_id: 'before'}, {generation_id: 'after'}],
  'after',
  'after',
);
if (duplicate.baseline !== 'before' || duplicate.current !== 'after') {
  throw new Error(`duplicate selections must be separated: ${JSON.stringify(duplicate)}`);
}
const candidateSelection = api.progressionSelection([
  {generation_id: 'release'},
  {generation_id: 'candidate', baseline_generation_id: 'release'},
]);
if (candidateSelection.baseline !== 'release' || candidateSelection.current !== 'candidate') {
  throw new Error(`candidate must default to its captured release: ${JSON.stringify(candidateSelection)}`);
}
const candidateBeforeNewerRelease = api.progressionSelection([
  {generation_id: 'release'},
  {generation_id: 'candidate', origin: {kind: 'git_revision'}, baseline_generation_id: 'release'},
  {generation_id: 'newer-release', origin: {kind: 'crate_release'}},
]);
if (candidateBeforeNewerRelease.baseline !== 'release'
    || candidateBeforeNewerRelease.current !== 'candidate') {
  throw new Error(
    `newest Git candidate must remain the default current generation: ${JSON.stringify(candidateBeforeNewerRelease)}`,
  );
}
const direct = api.compareSamples(
  [sample('release-a', 'a', 900, 1000, 100)],
  [sample('candidate-a', 'a', 1900, 2000, 100)],
  value => value.g8r_product,
);
if (direct.pairs.length !== 1 || direct.pairs[0].delta !== 1000) {
  throw new Error(`commit-to-release comparison must use direct G8r product: ${JSON.stringify(direct)}`);
}
const generation = (generation_id, dso_version) => ({
  generation_id,
  crate_version: '1.0.0',
  dso_version,
  observed_ir_count: 1,
  coverage: 'partial',
  cohort_ir_count: 2,
  missing_cohort_ir_count: 1,
  extra_ir_count: 0,
});
const rolling = api.releaseGenerations(
  {progression: {cohort_ir_count: 2, cohort_ir_hashes: [hash('a'), hash('b')], generations: [generation('new', '0.10.0'), generation('old', '0.9.0')]}},
  [
    {...sample('a', 'a', 0), crate_version: '1.0.0', dso_version: '0.9.0'},
    {...sample('a', 'a', 0), crate_version: '1.0.0', dso_version: '0.10.0'},
    {...sample('unrelated-old', 'f', 999999), crate_version: '1.0.0', dso_version: '0.9.0'},
    {...sample('unrelated-new', 'f', -999999), crate_version: '1.0.0', dso_version: '0.10.0'},
  ],
);
if (rolling.length !== 2 || rolling.some(value => value.samples.length !== 1)
    || rolling[0].generation_id !== 'old' || rolling[1].generation_id !== 'new') {
  throw new Error(`mixed DSO populations must remain separate: ${JSON.stringify(rolling)}`);
}
const candidateRows = api.releaseGenerations(
  {progression: {cohort_ir_count: 1, cohort_ir_hashes: [hash('a')], generations: [
    {...generation('release', '0.9.0'), event_time_utc: '2026-08-29T00:00:00Z', coverage: 'cohort_complete', observed_ir_count: 1, missing_cohort_ir_count: 0},
    {generation_id: 'candidate', origin: {kind: 'git_revision', commit: hash('c')}, display_label: 'main@cccccccc',
      event_time_utc: '2026-09-07T19:26:18Z', dso_version: '0.9.0', baseline_generation_id: 'release',
      coverage: 'cohort_complete', observed_ir_count: 1, cohort_ir_count: 1, missing_cohort_ir_count: 0,
      extra_ir_count: 0, candidate_samples: [sample('candidate-a', 'a', 2, 102, 100)]},
  ]}},
  [{...sample('release-a', 'a', 0), crate_version: '1.0.0', dso_version: '0.9.0'}],
);
if (candidateRows.length !== 2 || candidateRows[1].display_label !== 'main@cccccccc'
    || candidateRows[1].baseline_generation_id !== 'release'
    || candidateRows[1].samples[0].g8r_product !== 102) {
  throw new Error(`typed Git candidate rows were not preserved: ${JSON.stringify(candidateRows)}`);
}
const rendered = {
  'progression-summary': {innerHTML: ''},
  'progression-table': {innerHTML: ''},
};
global.document.getElementById = id => rendered[id] || null;
api.renderGenerationPair([
  {
    generation_id: 'release', origin: {kind: 'crate_release'}, display_label: 'v1.0.0',
    event_time_utc: '2026-08-01T00:00:00Z', dso_version: '0.9.0', coverage: 'cohort_complete',
    samples: [sample('release-a', 'a', 0, 100, 100)],
  },
  {
    generation_id: 'candidate', origin: {kind: 'git_revision'}, display_label: 'main@cccccccc',
    event_time_utc: '2026-08-02T00:00:00Z', dso_version: '0.9.0', coverage: 'cohort_complete',
    baseline_generation_id: 'release', samples: [sample('candidate-a', 'a', 10, 110, 100)],
  },
], 'release', 'candidate');
const renderedTable = rendered['progression-table'].innerHTML;
if (!renderedTable.includes('Largest per-artifact changes: v1.0.0 → main@cccccccc')
    || !renderedTable.includes('v1.0.0<br>Baseline G8r+ABC product')
    || !renderedTable.includes('main@cccccccc<br>Current G8r+ABC product')
    || !renderedTable.includes('Change<br>main@cccccccc − v1.0.0')) {
  throw new Error(`per-artifact table does not identify both endpoints: ${renderedTable}`);
}
const renderedSummary = rendered['progression-summary'].innerHTML;
if (!renderedSummary.includes('canonical structural-hash cohort')
    || renderedSummary.includes('identical IR bytes')) {
  throw new Error(`candidate comparison overstates its pairing guarantee: ${renderedSummary}`);
}
const stats = api.releaseStats([{generation_id: 'quality', version: '1.0.0', dso_version: '1.0.0', samples: [
  sample('larger-regression', 'd', 10, 110, 100),
  sample('smaller-improvement', 'e', -5, 45, 50),
]}])[0];
if (stats.g8r_total !== 155 || stats.yosys_total !== 150 || stats.total_loss !== 5
    || Math.abs(stats.aggregate_pct - (100 / 30)) > 1e-9
    || stats.gross_regression !== 10 || stats.gross_improvement !== 5) {
  throw new Error(`unexpected aggregate quality statistics: ${JSON.stringify(stats)}`);
}
"#;
    let mut child = match Command::new("node")
        .arg("-e")
        .arg(SCRIPT)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("skipping embedded JavaScript behavior test: node is unavailable");
            return;
        }
        Err(error) => panic!("starting node: {error}"),
    };
    child
        .stdin
        .take()
        .expect("node stdin")
        .write_all(APP_JS.as_bytes())
        .expect("write embedded JavaScript");
    let output = child.wait_with_output().expect("wait for node");
    assert!(
        output.status.success(),
        "embedded JavaScript test failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn comparison_javascript_matches_dynamic_quadrants_and_loss_filter() {
    const SCRIPT: &str = r#"
const fs = require('fs');
global.document = {
  querySelector: () => ({content: ''}),
  getElementById: () => null,
};
const app = fs.readFileSync(0, 'utf8');
const prefix = app.slice(0, app.indexOf('async function main()'));
const api = new Function(prefix + '\nreturn {comparisonDefaultVersion,comparisonIsLoss,comparisonQuadrant,comparisonSelectionKey,latestDsoSamplesByCrate};')();
const state = {lhs: 'G8r', rhs: 'Yosys/ABC'};
const sample = (g8r_nodes, g8r_levels, yosys_abc_nodes, yosys_abc_levels) => ({
  g8r_nodes,
  g8r_levels,
  yosys_abc_nodes,
  yosys_abc_levels,
});
const cases = [
  [sample(10, 10, 12, 12), 'Q1 pure win', false],
  [sample(12, 8, 10, 10), 'Q2 mixed', true],
  [sample(12, 12, 10, 10), 'Q3 strict loss', true],
  [sample(8, 12, 10, 10), 'Q4 mixed', true],
  [sample(10, 10, 10, 10), 'Tie exact', false],
];
for (const [value, prefix, loss] of cases) {
  const actual = api.comparisonQuadrant(value, state).label;
  if (!actual.startsWith(prefix)) {
throw new Error(`expected ${prefix}, got ${actual}`);
  }
  if (api.comparisonIsLoss(value, state) !== loss) {
throw new Error(`unexpected loss classification for ${actual}`);
  }
}
const key = api.comparisonSelectionKey({g8r_stats_action_id: 'g', yosys_abc_stats_action_id: 'y'});
if (key !== 'g:y') {
  throw new Error(`unexpected selection key: ${key}`);
}
const versions = ['0.9.0', '0.10.0'];
if (api.comparisonDefaultVersion(versions, null) !== '0.10.0') {
  throw new Error('missing version did not default to latest');
}
if (api.comparisonDefaultVersion(versions, '0.9.0') !== '0.9.0') {
  throw new Error('valid requested version was not preserved');
}
if (api.comparisonDefaultVersion(versions, 'invalid') !== '0.10.0') {
  throw new Error('invalid requested version did not default to latest');
}
if (api.comparisonDefaultVersion([], null) !== '') {
  throw new Error('empty version list did not stay empty');
}
const latestDso = api.latestDsoSamplesByCrate([
  {crate_version: '0.25.0', dso_version: '0.9.0', fn_key: 'stale'},
  {crate_version: '0.25.0', dso_version: '0.10.0', fn_key: 'current'},
  {crate_version: '0.26.0', dso_version: '0.8.0', fn_key: 'other'},
]);
if (JSON.stringify(latestDso.map(sample => sample.fn_key)) !== JSON.stringify(['current', 'other'])) {
  throw new Error(`stale DSO generations were not removed: ${JSON.stringify(latestDso)}`);
}

"#;
    let mut child = match Command::new("node")
        .arg("-e")
        .arg(SCRIPT)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("skipping embedded JavaScript behavior test: node is unavailable");
            return;
        }
        Err(error) => panic!("starting node: {error}"),
    };
    child
        .stdin
        .take()
        .expect("node stdin")
        .write_all(APP_JS.as_bytes())
        .expect("write embedded JavaScript");
    let output = child.wait_with_output().expect("wait for node");
    assert!(
        output.status.success(),
        "embedded JavaScript test failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn comparison_javascript_ignores_stale_evidence_fetches() {
    const SCRIPT: &str = r#"
const fs = require('fs');
const elements = {
  'comparison-detail-empty': {hidden: false},
  'comparison-detail-json': {hidden: true, textContent: ''},
  'comparison-detail-raw': {hidden: true},
  'comparison-detail-evidence': {textContent: '', innerHTML: ''},
  'comparison-detail-ir': {hidden: true, innerHTML: '', querySelectorAll: () => []},
  'comparison-max-ir-nodes': {value: '100'},
  'comparison-crate-version': {value: '0.31.0'},
  'comparison-sample-mode': {value: 'all'},
};
global.document = {
  querySelector: () => ({content: ''}),
  getElementById: id => elements[id] || null,
};
global.location = {href: 'https://example.test/plots'};
global.history = {replaceState: () => {}};
const pending = new Map();
global.fetch = url => new Promise(resolve => pending.set(url, resolve));
const app = fs.readFileSync(0, 'utf8');
const prefix = app.slice(0, app.indexOf('async function main()'));
const api = new Function(prefix + '\nreturn {showComparisonDetail};')();
const firstHash = '1'.repeat(64);
const secondHash = '2'.repeat(64);
const structuralDatasetKey = hash => `ir-fn-corpus-structural.v2/by-hash-prefix/${hash.slice(0, 2)}.json`;
const irDatasetKey = hash => `ir-fn-corpus-ir.v1/by-hash-prefix/${hash.slice(0, 3)}.json`;
const state = {
  catalog: {datasets: [
{logical_key: structuralDatasetKey(firstHash), url: 'first.json'},
{logical_key: structuralDatasetKey(secondHash), url: 'second.json'},
{logical_key: irDatasetKey(firstHash), url: 'first-ir.json'},
{logical_key: irDatasetKey(secondHash), url: 'second-ir.json'},
  ]},
  lhs: 'G8r',
  rhs: 'Yosys/ABC',
  irDatasetKey: 'ir-fn-corpus-ir.v1.json',
  selectedSampleKey: null,
};
const sample = (name, hash) => ({
  fn_key: name,
  crate_version: '0.31.0',
  dso_version: '0.54.7',
  ir_node_count: 10,
  ir_action_id: `action-${name}`,
  ir_top: `top-${name}`,
  structural_hash: hash,
  g8r_stats_action_id: `g8r-${name}`,
  yosys_abc_stats_action_id: `yabc-${name}`,
});
const structuralResponse = (name, hash) => ({
  ok: true,
  json: async () => ({groups: [{structural_hash: hash, members: [{
crate_version: '0.31.0',
opt_ir_action_id: `action-${name}`,
ir_top: `top-${name}`,
source_ir_action_id: `source-${name}`,
dslx_origin: {dslx_file: `${name}.x`, dslx_fn_name: name},
  }]}]}),
});
const irResponse = (name, hash) => ({
  ok: true,
  json: async () => ({rows: [{entry: {
crate_version: '0.31.0',
structural_hash: hash,
g8r_stats_action_id: `g8r-${name}`,
yosys_abc_stats_action_id: `yabc-${name}`,
g8r: {ir_action_id: `ir-${name}`, ir_top: `top-${name}`, ir_text: `fn top-${name}() -> bits[1] { ret literal.1: bits[1] = literal(value=1, id=1) }`},
yosys_abc: {ir_action_id: `ir-${name}`, ir_top: `top-${name}`, ir_text: `fn top-${name}() -> bits[1] { ret literal.1: bits[1] = literal(value=1, id=1) }`},
  }}]}),
});
(async () => {
  const first = api.showComparisonDetail(sample('first', firstHash), 'plot-levels', state);
  const second = api.showComparisonDetail(sample('second', secondHash), 'plot-nodes', state);
  await new Promise(resolve => setImmediate(resolve));
  pending.get('second.json')(structuralResponse('second', secondHash));
  pending.get('second-ir.json')(irResponse('second', secondHash));
  await second;
  const secondHtml = elements['comparison-detail-evidence'].innerHTML;
  if (!secondHtml.includes(secondHash) || !secondHtml.includes('source-second')) {
throw new Error(`second selection did not render: ${secondHtml}`);
  }
  const secondIrHtml = elements['comparison-detail-ir'].innerHTML;
  if (!secondIrHtml.includes('Exact XLS IR') || !secondIrHtml.includes('fn top-second')) {
throw new Error(`second selection IR did not render: ${secondIrHtml}`);
  }
  pending.get('first.json')(structuralResponse('first', firstHash));
  pending.get('first-ir.json')(irResponse('first', firstHash));
  await first;
  const finalHtml = elements['comparison-detail-evidence'].innerHTML;
  if (finalHtml !== secondHtml || finalHtml.includes(firstHash) || finalHtml.includes('source-first')) {
throw new Error(`stale first selection replaced second: ${finalHtml}`);
  }
  const finalIrHtml = elements['comparison-detail-ir'].innerHTML;
  if (finalIrHtml !== secondIrHtml || finalIrHtml.includes('fn top-first')) {
throw new Error(`stale first selection replaced second IR: ${finalIrHtml}`);
  }
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
"#;
    let mut child = match Command::new("node")
        .arg("-e")
        .arg(SCRIPT)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("skipping embedded JavaScript race test: node is unavailable");
            return;
        }
        Err(error) => panic!("starting node: {error}"),
    };
    child
        .stdin
        .take()
        .expect("node stdin")
        .write_all(APP_JS.as_bytes())
        .expect("write embedded JavaScript");
    let output = child.wait_with_output().expect("wait for node");
    assert!(
        output.status.success(),
        "embedded JavaScript race test failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn mffc_javascript_filters_and_ranks_paired_samples() {
    const SCRIPT: &str = r#"
const fs = require('fs');
global.document = {
  querySelector: () => ({content: ''}),
  getElementById: () => null,
};
const app = fs.readFileSync(0, 'utf8');
const prefix = app.slice(0, app.indexOf('async function main()'));
const api = new Function(prefix + '\nreturn {mffcComparisonKey,mffcLossPresentation,mffcSamples,mffcStructuralGroupKey,irFnCorpusIrShardKey,rankMffcSamples,sameIrIdentity};')();
const sample = (ir_top, ir_node_count, g8r_product_loss) => ({
  ir_top,
  ir_node_count,
  g8r_nodes: 10,
  g8r_levels: 10,
  yosys_abc_nodes: 10,
  yosys_abc_levels: 10,
  g8r_product: 100 + g8r_product_loss,
  yosys_abc_product: 100,
  g8r_product_loss,
});
const filtered = api.mffcSamples([
  sample('__mffc_small', 4, 10),
  sample('__whole_function', 3, 1000),
  sample('__mffc_large', 20, 200),
]);
if (filtered.length !== 2) {
  throw new Error(`expected 2 MFFCs, got ${filtered.length}`);
}
const ranked = api.rankMffcSamples(filtered);
if (ranked.map(row => row.ir_top).join(',') !== '__mffc_large,__mffc_small') {
  throw new Error(`unexpected MFFC ranking: ${ranked.map(row => row.ir_top)}`);
}
const sourceHash = 'd'.repeat(64);
if (api.mffcStructuralGroupKey(sourceHash) !== 'ir-fn-corpus-structural.v2/by-hash-prefix/dd.json') {
  throw new Error('unexpected source structural group key');
}
if (api.irFnCorpusIrShardKey('ir-fn-corpus-ir.v1.json', sourceHash) !== 'ir-fn-corpus-ir.v1/by-hash-prefix/ddd.json') {
  throw new Error('unexpected IR function corpus shard key');
}
const splitIrManifest = {shards: [{structural_prefix: 'ddd', g8r_stats_prefix: 'a', index_key: 'ir-fn-corpus-ir.v1/by-hash-prefix/ddd-g8r-a.json'}]};
if (api.irFnCorpusIrShardKey('ir-fn-corpus-ir.v1.json', sourceHash, 'a'.repeat(64), splitIrManifest) !== 'ir-fn-corpus-ir.v1/by-hash-prefix/ddd-g8r-a.json') {
  throw new Error('adaptive IR function corpus shard was not selected');
}
if (api.mffcComparisonKey('0.31.0', 'c'.repeat(64)) !== `0.31.0:${'c'.repeat(64)}`) {
  throw new Error('unexpected MFFC comparison key');
}
const pairedEntry = {
  g8r: {ir_action_id: 'a', ir_top: '__mffc_left', source_ir_top: '__source_left'},
  yosys_abc: {ir_action_id: 'b', ir_top: '__mffc_right', source_ir_top: '__source_right'},
};
if (api.sameIrIdentity(pairedEntry.g8r, pairedEntry.yosys_abc)) {
  throw new Error('different backend IR identities were treated as shared');
}
const presentations = [api.mffcLossPresentation(12.5), api.mffcLossPresentation(-12.5), api.mffcLossPresentation(0)];
if (presentations.map(value => value.kind).join(',') !== 'regressed,improved,same') {
  throw new Error(`unexpected loss presentation kinds: ${presentations.map(value => value.kind)}`);
}
if (presentations.map(value => value.text).join(',') !== '12.5 units worse than,12.5 units better than,tied with') {
  throw new Error(`unexpected loss presentation text: ${presentations.map(value => value.text)}`);
}
"#;
    let mut child = match Command::new("node")
        .arg("-e")
        .arg(SCRIPT)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("skipping embedded JavaScript behavior test: node is unavailable");
            return;
        }
        Err(error) => panic!("starting node: {error}"),
    };
    child
        .stdin
        .take()
        .expect("node stdin")
        .write_all(APP_JS.as_bytes())
        .expect("write embedded JavaScript");
    let output = child.wait_with_output().expect("wait for node");
    assert!(
        output.status.success(),
        "embedded JavaScript test failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn public_site_never_contains_private_executor_error_text() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    let repo_root = std::env::current_dir().expect("current dir");
    let crate_version = load_version_compat_map(&repo_root)
        .expect("compat map")
        .into_keys()
        .next()
        .expect("known crate version");
    let dso_version = resolve_xlsynth_version_for_driver(&repo_root, &crate_version)
        .expect("resolve DSO version");
    let root_action =
        canonical_root_actions_for_crate_version(&repo_root, &crate_version, &dso_version)
            .expect("canonical roots")
            .into_iter()
            .find(|action| {
                matches!(
                    action,
                    ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { .. }
                )
            })
            .expect("stdlib root action");
    let discovery_runtime = match &root_action {
        ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
            discovery_runtime: Some(runtime),
            ..
        } => runtime.clone(),
        _ => panic!("stdlib root has discovery runtime"),
    };
    let root_action_id = compute_action_id(&root_action).expect("root action id");
    let private_path = root.join("private-build/worker.log").display().to_string();
    let private_token = "BVC_TEST_CREDENTIAL=do-not-publish";
    let private_windows_path = "C:/bvc-private/worker.log";
    let private_file_uri = "file:///srv/bvc-private/worker.log";
    let failed_action = ActionSpec::DriverIrToOpt {
        ir_action_id: "a".repeat(64),
        top_fn_name: Some(format!(
            "top={private_path}; {private_windows_path}; {private_file_uri}; {private_token}"
        )),
        version: dso_version.clone(),
        runtime: discovery_runtime,
    };
    let failed_action_id = compute_action_id(&failed_action).expect("failed action id");
    let now = Utc::now();
    store
        .write_provenance(&Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: root_action_id.clone(),
            created_utc: now,
            action: root_action,
            dependencies: Vec::new(),
            output_artifact: ArtifactRef {
                action_id: root_action_id,
                artifact_type: ArtifactType::DslxFileSubtree,
                relpath: "payload".to_string(),
            },
            output_files: Vec::new(),
            commands: Vec::new(),
            details: json!({
                "dslx_list_fns_discovery_error": format!(
                    "discovery failed at {private_path}; {private_token}"
                )
            }),
            suggested_next_actions: Vec::new(),
        })
        .expect("write private discovery failure provenance");
    store
        .write_failed_action_record(&QueueFailed {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: failed_action_id,
            enqueued_utc: now,
            failed_utc: now,
            failed_by: "test-worker".to_string(),
            action: failed_action,
            error: format!("executor failed at {private_path}; {private_token}"),
        })
        .expect("write private failure");
    rebuild_versions_cards_index(&store, &repo_root).expect("build public versions index");

    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &repo_root,
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let versions_json = fs::read_to_string(
        snapshot_dir
            .join("web_index")
            .join(crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME),
    )
    .expect("read public versions dataset");
    assert!(versions_json.contains("\"failure_class\":\"failed\""));
    assert!(versions_json.contains("\"reason\":\"discovery_failed\""));
    assert!(!versions_json.contains(&private_path));
    assert!(!versions_json.contains(private_token));
    assert!(!versions_json.contains(private_windows_path));
    assert!(!versions_json.contains(private_file_uri));

    let site_dir = root.join("site");
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/xlsynth-bvc/".into(),
        overwrite: false,
    })
    .expect("build site");
    verify_static_site(&site_dir).expect("verify site");
    let releases_html =
        fs::read_to_string(site_dir.join("releases.html")).expect("read releases page");
    assert!(releases_html.contains("Repository position at release sync"));
    assert!(releases_html.contains("Release ledger"));
    assert!(releases_html.contains("processed"));
    assert!(releases_html.contains("not processed"));
    for entry in WalkDir::new(&site_dir) {
        let entry = entry.expect("walk public site");
        if !entry.file_type().is_file()
            || !matches!(
                entry.path().extension().and_then(|value| value.to_str()),
                Some("json" | "html")
            )
        {
            continue;
        }
        let text = fs::read_to_string(entry.path()).expect("read public text file");
        assert!(!text.contains(&private_path), "{}", entry.path().display());
        assert!(!text.contains(private_token), "{}", entry.path().display());
        assert!(
            !text.contains(private_windows_path),
            "{}",
            entry.path().display()
        );
        assert!(
            !text.contains(private_file_uri),
            "{}",
            entry.path().display()
        );
    }
    drop(store);
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_verifier_detects_tamper() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write dataset");
    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let site_dir = root.join("site");
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/".into(),
        overwrite: false,
    })
    .expect("build site");
    fs::write(site_dir.join("index.html"), "tampered").expect("tamper");
    assert!(verify_static_site(&site_dir).is_err());
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_verifier_rejects_self_consistent_script_and_unknown_catalog_field() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write dataset");
    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let site_dir = root.join("site");
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/".into(),
        overwrite: false,
    })
    .expect("build site");

    let manifest_path = site_dir.join(STATIC_SITE_MANIFEST_FILENAME);
    let original_manifest = fs::read(&manifest_path).expect("read original manifest");
    let index_path = site_dir.join("index.html");
    let original_index = fs::read_to_string(&index_path).expect("read index");
    fs::write(
        &index_path,
        original_index.replace(
            "</body>",
            "<script src=\"https://attacker.example/payload.js\"></script></body>",
        ),
    )
    .expect("inject external script");
    refresh_site_manifest_entry(&site_dir, "index.html");
    let error = verify_static_site(&site_dir)
        .expect_err("self-consistent external script must fail verification");
    assert!(
        format!("{error:#}").contains("deterministic rendering"),
        "unexpected error: {error:#}"
    );

    fs::write(&index_path, original_index).expect("restore index");
    fs::write(&manifest_path, &original_manifest).expect("restore manifest");
    let catalog_path = site_dir.join("catalog.json");
    let mut catalog: serde_json::Value =
        serde_json::from_slice(&fs::read(&catalog_path).expect("read catalog"))
            .expect("decode catalog value");
    catalog.as_object_mut().expect("catalog object").insert(
        "private_path".to_string(),
        serde_json::Value::String("/srv/build/private".to_string()),
    );
    fs::write(
        &catalog_path,
        serde_json::to_vec_pretty(&catalog).expect("encode modified catalog"),
    )
    .expect("write modified catalog");
    refresh_site_manifest_entry(&site_dir, "catalog.json");
    let error = verify_static_site(&site_dir)
        .expect_err("self-consistent unknown catalog field must fail verification");
    assert!(
        format!("{error:#}").contains("unknown field `private_path`"),
        "unexpected error: {error:#}"
    );
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_verifier_binds_release_catalog_to_versions_dataset() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write dataset");
    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let site_dir = root.join("site");
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/".into(),
        overwrite: false,
    })
    .expect("build site");

    let catalog_path = site_dir.join("catalog.json");
    let mut catalog: BrowserCatalog =
        serde_json::from_slice(&fs::read(&catalog_path).expect("read catalog"))
            .expect("decode catalog");
    catalog.releases.push(CrateReleaseStatusView {
        crate_version: "0.35.0".to_string(),
        crate_release_datetime: "2026-08-28 17:52:54 PDT".to_string(),
        dso_version: "0.35.0".to_string(),
        processed: false,
        materialized_actions: 0,
        failed_actions: 0,
        stdlib_enumeration_state: "not run".to_string(),
    });
    let embedded_snapshot =
        load_static_snapshot_manifest(&site_dir).expect("load embedded snapshot");
    for (relpath, bytes) in
        expected_fixed_site_files(&catalog, &embedded_snapshot).expect("render fixed files")
    {
        fs::write(site_dir.join(&relpath), bytes).expect("rewrite fixed site file");
        refresh_site_manifest_entry(&site_dir, &relpath);
    }

    let error =
        verify_static_site(&site_dir).expect_err("catalog release rows must come from the dataset");
    assert!(
        format!("{error:#}").contains("release processing projection disagrees"),
        "unexpected error: {error:#}"
    );

    catalog.releases.clear();
    catalog.repository_head_observation = Some(RepositoryHeadObservationView {
        schema_version: 2,
        repository: "xlsynth/xlsynth-crate".to_string(),
        version_compat_sha256: "c".repeat(64),
        observed_at_utc: "2026-08-29T12:00:00Z".to_string(),
        head_ref: "main".to_string(),
        head_commit: "a".repeat(40),
        head_committed_at_utc: "2026-08-29T11:00:00Z".to_string(),
        latest_crate_version: "0.35.0".to_string(),
        latest_release_tag: "v0.35.0".to_string(),
        latest_release_commit: "b".repeat(40),
        latest_release_committed_at_utc: "2026-08-28T11:00:00Z".to_string(),
        comparison_status: "identical".to_string(),
        commits_ahead: 9,
        commits_behind: 0,
    });
    for (relpath, bytes) in
        expected_fixed_site_files(&catalog, &embedded_snapshot).expect("render fixed files")
    {
        fs::write(site_dir.join(&relpath), bytes).expect("rewrite fixed site file");
        refresh_site_manifest_entry(&site_dir, &relpath);
    }

    let error = verify_static_site(&site_dir)
        .expect_err("catalog repository observation must come from the dataset");
    assert!(
        format!("{error:#}").contains("release processing projection disagrees"),
        "unexpected error: {error:#}"
    );
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_output_rejects_protected_root_ancestors_and_descendants() {
    let root = temp_root();
    let snapshot_dir = root.join("snapshot");
    fs::create_dir_all(&snapshot_dir).expect("create snapshot placeholder");
    for label in [
        "resource checkout",
        "private store",
        "artifact backend storage",
    ] {
        let container = root.join(label.replace(' ', "-"));
        let protected = container.join("protected");
        fs::create_dir_all(&protected).expect("create protected root");

        for output in [container.clone(), protected.join("generated-site")] {
            let error =
                reject_site_output_overlap(&output, &snapshot_dir, &[(label, protected.as_path())])
                    .expect_err("bidirectional protected-root overlap must fail");
            assert!(
                format!("{error:#}").contains(&format!("must not overlap {label}")),
                "unexpected error for {label}: {error:#}"
            );
        }
    }
    fs::remove_dir_all(root).expect("cleanup");
}

#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
#[test]
fn atomic_site_exchange_never_removes_either_directory_name() {
    let root = temp_root();
    let current = root.join("site");
    let replacement = root.join("site-staging");
    fs::create_dir_all(&current).expect("create current site");
    fs::create_dir_all(&replacement).expect("create replacement site");
    fs::write(current.join("generation.txt"), "old").expect("write current generation");
    fs::write(replacement.join("generation.txt"), "new").expect("write replacement generation");

    atomic_exchange_site_directories(&current, &replacement).expect("exchange site directories");

    assert_eq!(
        fs::read_to_string(current.join("generation.txt")).expect("read installed generation"),
        "new"
    );
    assert_eq!(
        fs::read_to_string(replacement.join("generation.txt")).expect("read replaced generation"),
        "old"
    );
    fs::remove_dir_all(root).expect("cleanup");
}

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "redox",
    target_vendor = "apple"
))]
#[test]
fn atomic_site_install_does_not_replace_a_concurrent_destination() {
    let root = temp_root();
    let destination = root.join("site");
    let replacement = root.join("site-staging");
    fs::create_dir_all(&destination).expect("create competing site");
    fs::create_dir_all(&replacement).expect("create replacement site");
    fs::write(destination.join("generation.txt"), "competing").expect("write competing generation");
    fs::write(replacement.join("generation.txt"), "new").expect("write replacement generation");

    let error = atomic_install_site_directory(&replacement, &destination)
        .expect_err("no-replace install must reject a concurrent destination");

    assert!(
        format!("{error:#}").contains("without replacing an existing destination"),
        "unexpected error: {error:#}"
    );
    assert_eq!(
        fs::read_to_string(destination.join("generation.txt")).expect("read competing generation"),
        "competing"
    );
    assert_eq!(
        fs::read_to_string(replacement.join("generation.txt")).expect("read staged generation"),
        "new"
    );
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_publication_lock_excludes_a_second_publisher() {
    let root = temp_root();
    fs::create_dir_all(&root).expect("create publication parent");
    let site_dir = root.join("site");
    let first_lock =
        acquire_site_publication_lock(&site_dir).expect("acquire first publisher lock");
    let lock_path = root.join(".site.publish.lock");
    let second_lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open second publisher lock handle");

    fs2::FileExt::try_lock_exclusive(&second_lock)
        .expect_err("second publisher must be excluded while first holds the lock");
    drop(first_lock);
    fs2::FileExt::try_lock_exclusive(&second_lock)
        .expect("second publisher can acquire lock after first exits");
    fs2::FileExt::unlock(&second_lock).expect("unlock second publisher");

    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_overwrite_rejects_snapshot_ancestor_before_deletion() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write dataset");
    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let marker = snapshot_dir.join(STATIC_SNAPSHOT_MANIFEST_FILENAME);

    let error = build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: root.clone(),
        base_url: "/".into(),
        overwrite: true,
    })
    .expect_err("snapshot ancestor output must fail");
    assert!(format!("{error:#}").contains("must not overlap source snapshot"));
    assert!(marker.is_file(), "source snapshot must survive rejection");
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_overwrite_preserves_existing_site_on_late_candidate_failure() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write dataset");
    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");

    let site_dir = root.join("site");
    fs::create_dir_all(&site_dir).expect("create existing site");
    let sentinel_path = site_dir.join("keep.txt");
    fs::write(&sentinel_path, b"prior verified site").expect("write existing site sentinel");
    let candidate_dir = root.join("candidate");
    fs::create_dir_all(&candidate_dir).expect("create candidate directory");
    let candidate_manifest = candidate_manifest_input(&"8".repeat(40));
    fs::write(
        candidate_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&candidate_manifest).expect("serialize candidate manifest"),
    )
    .expect("write candidate manifest");
    write_candidate_run_marker(&candidate_dir, &candidate_manifest);

    let error = build_static_site_with_candidate_runs(
        &BuildStaticSiteOptions {
            snapshot_dir,
            out_dir: site_dir.clone(),
            base_url: "/".into(),
            overwrite: true,
        },
        &[],
        &[candidate_dir],
    )
    .expect_err("candidate without the comparison dataset must fail after staging starts");
    assert!(
        format!("{error:#}").contains("cannot add a candidate"),
        "unexpected error: {error:#}"
    );
    assert_eq!(
        fs::read(&sentinel_path).expect("read sentinel"),
        b"prior verified site"
    );

    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_verifier_rejects_symlinks() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write dataset");
    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let site_dir = root.join("site");
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/".into(),
        overwrite: false,
    })
    .expect("build site");
    std::os::unix::fs::symlink("/tmp", site_dir.join("private-link")).expect("create symlink");
    let error = verify_static_site(&site_dir).expect_err("symlink must fail");
    assert!(format!("{error:#}").contains("symlink or special"));
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_verifier_binds_dataset_bytes_to_source_snapshot() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write dataset");
    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let site_dir = root.join("site");
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/".into(),
        overwrite: false,
    })
    .expect("build site");

    let dataset_relpath = format!("data/{}", crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME);
    let dataset_path = site_dir.join(&dataset_relpath);
    let original = fs::read_to_string(&dataset_path).expect("read dataset");
    let changed = original.replacen("1970-01-01T00:00:00Z", "1971-01-01T00:00:00Z", 1);
    assert_ne!(original, changed);
    fs::write(&dataset_path, changed.as_bytes()).expect("rewrite dataset");

    let catalog_path = site_dir.join("catalog.json");
    let mut catalog: BrowserCatalog =
        serde_json::from_slice(&fs::read(&catalog_path).expect("read catalog"))
            .expect("decode catalog");
    let dataset = catalog
        .datasets
        .iter_mut()
        .find(|dataset| dataset.url == dataset_relpath)
        .expect("catalog dataset");
    dataset.bytes = changed.len() as u64;
    dataset.sha256 = sha256_hex(changed.as_bytes());
    fs::write(
        &catalog_path,
        encode_browser_catalog(&catalog).expect("encode canonical catalog"),
    )
    .expect("rewrite catalog");
    let embedded_snapshot = load_static_snapshot_manifest(&site_dir)
        .expect("load embedded snapshot for fixed-page regeneration");
    for (relpath, bytes) in
        expected_fixed_site_files(&catalog, &embedded_snapshot).expect("render fixed files")
    {
        fs::write(site_dir.join(relpath), bytes).expect("rewrite fixed site file");
    }

    let manifest_path = site_dir.join(STATIC_SITE_MANIFEST_FILENAME);
    let mut manifest = pb::StaticSiteManifest::decode(
        fs::read(&manifest_path)
            .expect("read site manifest")
            .as_slice(),
    )
    .expect("decode site manifest");
    for slot in &mut manifest.files {
        let relpath = slot
            .relpath
            .as_ref()
            .expect("declared relpath")
            .value
            .clone();
        *slot = publication_file(&site_dir, &relpath).expect("describe changed site file");
    }
    fs::write(&manifest_path, manifest.encode_to_vec()).expect("rewrite site manifest");

    let error =
        verify_static_site(&site_dir).expect_err("site data must remain bound to source snapshot");
    assert!(
        format!("{error:#}").contains("does not match snapshot source"),
        "unexpected error: {error:#}"
    );
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn site_verifier_rejects_self_consistent_non_allowlisted_files() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    store
        .write_web_index_bytes(
            crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
            &empty_versions_index_bytes(),
        )
        .expect("write dataset");
    let snapshot_dir = root.join("snapshot");
    build_static_snapshot(
        &store,
        &test_repo_root(),
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("build snapshot");
    let site_dir = root.join("site");
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/".into(),
        overwrite: false,
    })
    .expect("build site");

    let private_files: [(&str, &[u8]); 3] = [
        (
            "data/internal-build-metadata.v1.json",
            br#"{"private_path":"/srv/build/secrets"}"#,
        ),
        ("data/private-provenance.pb", b"private protobuf bytes"),
        ("worker.log", b"private worker output"),
    ];
    for (relpath, bytes) in private_files {
        let path = site_dir.join(relpath);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create private file parent");
        }
        fs::write(path, bytes).expect("write non-allowlisted site file");
    }
    let manifest_path = site_dir.join(STATIC_SITE_MANIFEST_FILENAME);
    let mut manifest = pb::StaticSiteManifest::decode(
        fs::read(&manifest_path)
            .expect("read site manifest")
            .as_slice(),
    )
    .expect("decode site manifest");
    for (relpath, _) in private_files {
        manifest
            .files
            .push(publication_file(&site_dir, relpath).expect("describe private file"));
    }
    fs::write(&manifest_path, manifest.encode_to_vec()).expect("rewrite site manifest");

    let error = verify_static_site(&site_dir)
        .expect_err("self-consistent non-allowlisted files must fail verification");
    assert!(
        format!("{error:#}").contains("allowlisted topology"),
        "unexpected error: {error:#}"
    );
    fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn empty_stdlib_evidence_is_degraded_and_rendered_as_verified_static_run_page() {
    let root = temp_root();
    let store = ArtifactStore::new(root.join("store"));
    store.ensure_layout().expect("store layout");
    let repo_root = std::env::current_dir().expect("current dir");
    let compat = load_version_compat_map(&repo_root).expect("compat map");
    let crate_version = compat.keys().next().expect("known crate version").clone();
    let dso_version =
        resolve_xlsynth_version_for_driver(&repo_root, &crate_version).expect("dso version");
    for action in canonical_root_actions_for_crate_version(&repo_root, &crate_version, &dso_version)
        .expect("root actions")
    {
        let action_id = compute_action_id(&action).expect("action id");
        let details = match &action {
            crate::model::ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { .. } => {
                json!({"download": {}})
            }
            crate::model::ActionSpec::DownloadAndExtractXlsynthSourceSubtree {
                subtree, ..
            } => json!({"download": {}, "subtree": subtree, "extracted_file_count": 1}),
            _ => unreachable!(),
        };
        store
            .write_provenance(&Provenance {
                schema_version: crate::ACTION_SCHEMA_VERSION,
                action_id: action_id.clone(),
                created_utc: Utc::now(),
                action,
                dependencies: Vec::new(),
                output_artifact: ArtifactRef {
                    action_id,
                    artifact_type: ArtifactType::DslxFileSubtree,
                    relpath: "payload".to_string(),
                },
                output_files: Vec::new(),
                commands: Vec::new(),
                details,
                suggested_next_actions: Vec::new(),
            })
            .expect("write root provenance");
    }
    let generated_utc = Utc::now();
    rebuild_versions_cards_index(&store, &repo_root).expect("versions dataset");
    let comparison_json = serde_json::to_vec(&json!({
        "schema_version": crate::WEB_STDLIB_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION,
        "generated_utc": generated_utc,
        "dataset": {
            "fraig": false,
            "samples": [],
            "min_ir_nodes": 0,
            "max_ir_nodes": 0,
            "g8r_only_count": 0,
            "yosys_only_count": 0,
            "available_crate_versions": [crate_version]
        }
    }))
    .expect("serialize comparison dataset");
    store
        .write_web_index_bytes(
            crate::WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME,
            &comparison_json,
        )
        .expect("comparison dataset");
    let finalized =
        finalize_campaign_run(&store, &repo_root, &crate_version).expect("finalize campaign");
    assert_eq!(finalized.status, "degraded");
    assert!(
        finalized
            .missing_outputs
            .iter()
            .any(|reason| reason.contains("stdlib enumeration is absent"))
    );
    assert!(
        finalized
            .missing_outputs
            .iter()
            .any(|reason| reason.contains("declared stdlib-root lineage"))
    );

    let snapshot_dir = root.join("snapshot-with-run");
    build_static_snapshot(
        &store,
        &repo_root,
        &BuildStaticSnapshotOptions {
            out_dir: snapshot_dir.clone(),
            overwrite: false,
            skip_rebuild_web_indices: true,
        },
    )
    .expect("snapshot");
    let site_dir = root.join("site-with-run");
    build_static_site(&BuildStaticSiteOptions {
        snapshot_dir,
        out_dir: site_dir.clone(),
        base_url: "/xlsynth-bvc/".to_string(),
        overwrite: false,
    })
    .expect("site");
    verify_static_site(&site_dir).expect("verify run site");
    assert!(site_dir.join("runs.html").exists());
    assert!(site_dir.join("progression.html").exists());
    assert!(site_dir.join("releases.html").exists());
    assert!(site_dir.join("mffc-discrepancies.html").exists());
    assert!(
        site_dir
            .join("runs")
            .join(finalized.run_id)
            .join("index.html")
            .exists()
    );
    drop(store);
    fs::remove_dir_all(root).expect("cleanup");
}
