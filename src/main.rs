// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
#[cfg(test)]
use chrono::Utc;
#[cfg(test)]
use std::fs;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;

mod app;
mod cli;
mod corpus;
mod executor;
mod model;
mod ops;
mod query;
mod queue;
mod runtime;
mod service;
mod sled_space;
mod snapshot;
mod store;
mod versioning;
mod view;
mod web;

use crate::cli::{DriverCli, YosysCli};
#[cfg(test)]
use crate::executor::{
    compute_action_id, determine_dslx_import_context, extract_tar_gz_subtree_safely,
    parse_discovered_dslx_functions, parse_dslx_list_fns_failed_files,
};
#[cfg(test)]
use crate::model::*;
use crate::model::{DriverRuntimeSpec, YosysRuntimeSpec};
use crate::runtime::{
    default_driver_image, ensure_driver_runtime_compatibility, resolve_driver_version,
};
#[cfg(test)]
use crate::store::ArtifactStore;

const ACTION_SCHEMA_VERSION: u32 = 1;
const DEFAULT_STORE_DIR: &str = "bvc-artifacts";
const DEFAULT_RELEASE_PLATFORM: &str = "ubuntu2004";
const DEFAULT_DOCKERFILE: &str = "docker/xlsynth-driver.Dockerfile";
const DEFAULT_DOCKER_IMAGE_PREFIX: &str = "xlsynth-bvc-driver";
const DEFAULT_YOSYS_DOCKERFILE: &str = "docker/yosys-abc.Dockerfile";
const DEFAULT_YOSYS_DOCKER_IMAGE: &str = "xlsynth-bvc-yosys-abc:ubuntu24.04-py";
const DEFAULT_YOSYS_FLOW_SCRIPT: &str = "flows/yosys_to_aig.ys";
const DEFAULT_QUEUE_LEASE_SECONDS: i64 = 900;
const DEFAULT_QUEUE_PRIORITY: i32 = 0;
const DEFAULT_ACTION_TIMEOUT_SECONDS: u64 = 300;
const DEFAULT_WEB_RUNNER_POLL_MILLIS: u64 = 1000;
const DEFAULT_WEB_RUNNER_DRAIN_BATCH_SIZE: usize = 32;
const DEFAULT_WEB_RUNNER_MAX_WORKERS: usize = 8;
const DEFAULT_STRUCTURAL_INDEX_MAX_THREADS: usize = 16;
const DEFAULT_SUGGESTED_ENQUEUE_MAX_DEPTH: u32 = 32;
const DEFAULT_STDLIB_FN_TIMELINE_ROUTE: &str = "/dslx-fns/float32.x:add";
const DSLX_PATH_LIST_SEPARATOR: &str = ":";
const IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION: u32 = 1;
const WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_NAMESPACE: &str = "ir-fn-corpus-structural.v1";
const WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY: &str =
    "ir-fn-corpus-structural.v1/manifest.json";
const WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME: &str = "ir-fn-corpus-g8r-vs-yosys-abc.v3.json";
const WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION: u32 = 3;
const WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME: &str =
    "ir-fn-corpus-g8r-abc-vs-codegen-yosys-abc.v1.json";
const WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_SCHEMA_VERSION: u32 = 1;
const WEB_VERSIONS_SUMMARY_INDEX_FILENAME: &str = "versions-summary.v1.json";
const WEB_VERSIONS_SUMMARY_INDEX_SCHEMA_VERSION: u32 = 1;
const WEB_STDLIB_FNS_TREND_G8R_FRAIG_FALSE_INDEX_FILENAME: &str =
    "stdlib-fns-trend-g8r-fraig-false.v1.json";
const WEB_STDLIB_FNS_TREND_G8R_FRAIG_TRUE_INDEX_FILENAME: &str =
    "stdlib-fns-trend-g8r-fraig-true.v1.json";
const WEB_STDLIB_FNS_TREND_YOSYS_ABC_INDEX_FILENAME: &str = "stdlib-fns-trend-yosys-abc.v1.json";
const WEB_STDLIB_FNS_TREND_INDEX_SCHEMA_VERSION: u32 = 1;
const WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME: &str =
    "stdlib-g8r-vs-yosys-fraig-false.v1.json";
const WEB_STDLIB_G8R_VS_YOSYS_FRAIG_TRUE_INDEX_FILENAME: &str =
    "stdlib-g8r-vs-yosys-fraig-true.v1.json";
const WEB_STDLIB_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION: u32 = 1;
const WEB_STDLIB_FILE_ACTION_GRAPH_INDEX_FILENAME: &str = "stdlib-file-action-graph.v1.json";
const WEB_STDLIB_FILE_ACTION_GRAPH_INDEX_SCHEMA_VERSION: u32 = 1;
const WEB_STDLIB_FN_TIMELINE_INDEX_FILENAME: &str = "stdlib-fn-timeline.v1.json";
const WEB_STDLIB_FN_TIMELINE_INDEX_SCHEMA_VERSION: u32 = 1;
const STRUCTURAL_SCAN_PROGRESS_EVERY_ACTIONS: usize = 50;
const STRUCTURAL_SCAN_PROGRESS_INTERVAL_SECS: u64 = 5;
const GITHUB_RELEASES_API: &str = "https://api.github.com/repos/xlsynth/xlsynth/releases";
const VERSION_COMPAT_PATH: &str = "third_party/xlsynth-crate/generated_version_compat.json";
const VENDORED_DOWNLOAD_RELEASE_SCRIPT: &str =
    "third_party/xlsynth-crate/v0.29.0/scripts/download_release.py";
const DRIVER_RELEASE_CACHE_DIR: &str = "driver-release-cache";
const DRIVER_RELEASE_CACHE_READY_FILE: &str = ".ready.json";
const DRIVER_RELEASE_CACHE_LOCK_FILE: &str = ".setup.lock";
const DRIVER_RELEASE_CACHE_SETUP_TIMEOUT_SECS: u64 = 300;
const DRIVER_RELEASE_CACHE_SETUP_POLL_MILLIS: u64 = 250;
const DRIVER_RELEASE_CACHE_BINARIES: &str =
    "dslx_fmt,ir_converter_main,opt_main,codegen_main,delay_info_main,check_ir_equivalence_main";
const DEFAULT_WEB_BIND: &str = "127.0.0.1:3000";
const LEGACY_G8R_STATS_RELPATH: &str = "payload/result.g8r_stats.json";
const BVC_ENABLE_DRIVER_IR_AIG_EQUIV_ENV: &str = "BVC_ENABLE_DRIVER_IR_AIG_EQUIV";
const BVC_ENABLE_INCREMENTAL_IR_CORPUS_UPSERT_ENV: &str = "BVC_ENABLE_INCREMENTAL_IR_CORPUS_UPSERT";
const BVC_QUEUE_ONLY_PREVIOUS_LOSS_K_CONES_ENV: &str = "BVC_QUEUE_ONLY_PREVIOUS_LOSS_K_CONES";
const BVC_DISABLE_AUTO_SUGGESTED_ENQUEUE_ENV: &str = "BVC_DISABLE_AUTO_SUGGESTED_ENQUEUE";
const DASHBOARD_FAVICON_SVG: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><rect width="64" height="64" rx="10" fill="#070b12"/><rect x="12" y="14" width="40" height="30" rx="4" fill="#0f1f35" stroke="#35f2b3" stroke-width="2"/><path d="M22 22l8 7-8 7" fill="none" stroke="#35f2b3" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/><rect x="34" y="34" width="10" height="3" rx="1.5" fill="#5ec9ff"/><circle cx="18" cy="50" r="2" fill="#35f2b3"/><circle cx="25" cy="50" r="2" fill="#5ec9ff"/><circle cx="32" cy="50" r="2" fill="#35f2b3"/></svg>"##;
const DRIVER_TOOLS_SETUP_FROM_CACHE_SNIPPET: &str = r#"
cache_dir="/cache/${XLSYNTH_VERSION}/${XLSYNTH_PLATFORM}"
if [ ! -d "${cache_dir}" ]; then
  echo "missing cached xlsynth release at ${cache_dir}" >&2
  exit 1
fi
rm -rf /tmp/xlsynth-release
mkdir -p /tmp/xlsynth-release
cp -a "${cache_dir}/." /tmp/xlsynth-release/
for ext in so dylib; do
  if [ -f "/tmp/xlsynth-release/libxls-${XLSYNTH_PLATFORM}.${ext}" ]; then
    ln -sf "/tmp/xlsynth-release/libxls-${XLSYNTH_PLATFORM}.${ext}" "/tmp/xlsynth-release/libxls-${XLSYNTH_VERSION}-${XLSYNTH_PLATFORM}.${ext}"
  fi
done
export XLSYNTH_TOOLS=/tmp/xlsynth-release
export LD_LIBRARY_PATH="/tmp/xlsynth-release:${LD_LIBRARY_PATH:-}"
"#;
const INPUT_IR_FN_STRUCTURAL_HASH_DETAILS_KEY: &str = "input_ir_fn_structural_hash";
const OUTPUT_IR_OP_COUNT_DETAILS_KEY: &str = "output_ir_op_count";
const DEFAULT_K_BOOL_CONE_MAX_IR_OPS: u64 = 16;
const DELAY_INFO_OUTPUT_FORMAT_TEXTPROTO_V1: &str = "textproto_v1";
const DSLX_LIST_FNS_LEGACY_SOURCE_DRIVER_MAX: &str = "0.29.0";
const XLSYNTH_SOURCE_ARCHIVE_URL_PREFIX: &str =
    "https://github.com/xlsynth/xlsynth/archive/refs/tags";
const MODULE_SUBTREE_ROOT_PATHS: &[&str] = &["xls/modules/add_dual_path"];
const VERSION_COMPAT_MAIN_URL: &str =
    "https://raw.githubusercontent.com/xlsynth/xlsynth-crate/main/generated_version_compat.json";
static DOCKER_RUN_NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) fn driver_ir_aig_equiv_enabled() -> bool {
    std::env::var(BVC_ENABLE_DRIVER_IR_AIG_EQUIV_ENV)
        .ok()
        .map(|raw| match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => false,
        })
        .unwrap_or(false)
}

pub(crate) fn queue_only_previous_loss_k_cones_enabled() -> bool {
    std::env::var(BVC_QUEUE_ONLY_PREVIOUS_LOSS_K_CONES_ENV)
        .ok()
        .map(|raw| match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => false,
        })
        .unwrap_or(false)
}

pub(crate) fn auto_suggested_enqueue_enabled() -> bool {
    !std::env::var(BVC_DISABLE_AUTO_SUGGESTED_ENQUEUE_ENV)
        .ok()
        .map(|raw| match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => false,
        })
        .unwrap_or(false)
}

pub(crate) fn incremental_ir_corpus_upsert_enabled() -> bool {
    std::env::var(BVC_ENABLE_INCREMENTAL_IR_CORPUS_UPSERT_ENV)
        .ok()
        .map(|raw| match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => true,
        })
        .unwrap_or(true)
}

pub(crate) fn default_k_bool_cone_max_ir_ops_for_k(k: u32) -> Option<u64> {
    if k == 3 {
        Some(DEFAULT_K_BOOL_CONE_MAX_IR_OPS)
    } else {
        None
    }
}

impl DriverCli {
    pub(crate) fn into_runtime(
        self,
        repo_root: &Path,
        xlsynth_version: &str,
    ) -> Result<DriverRuntimeSpec> {
        let resolved_driver_version =
            resolve_driver_version(repo_root, self.driver_version.as_deref(), xlsynth_version)?;
        let docker_image = self
            .docker_image
            .unwrap_or_else(|| default_driver_image(&resolved_driver_version));
        let runtime = DriverRuntimeSpec {
            driver_version: resolved_driver_version,
            release_platform: self.release_platform,
            docker_image,
            dockerfile: self.dockerfile.to_string_lossy().to_string(),
        };
        ensure_driver_runtime_compatibility(repo_root, &runtime, xlsynth_version)?;
        Ok(runtime)
    }
}

impl YosysCli {
    pub(crate) fn into_runtime(self) -> YosysRuntimeSpec {
        YosysRuntimeSpec {
            docker_image: self.yosys_docker_image,
            dockerfile: self.yosys_dockerfile.to_string_lossy().to_string(),
        }
    }
}

fn main() -> Result<()> {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("warn,xlsynth_bvc=info"),
    )
    .format_timestamp_millis()
    .try_init();
    app::run()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{
        canonical_root_actions_for_crate_version, enqueue_processing_for_crate_version,
    };
    use crate::queue::{
        list_queue_files, load_queue_failed_record, write_failed_action_record, write_failed_record,
    };
    use crate::service::{
        dslx_to_mangled_ir_fn_name, load_failed_queue_records, normalize_subtree_path,
        record_structural_hash_coverage, structural_hash_for_matching_target_g8r_action,
    };
    use crate::versioning::parse_release_tag;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::fs::File;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tar::Builder;

    fn make_test_store() -> (ArtifactStore, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("xlsynth-bvc-test-{}-{}", std::process::id(), nanos));
        fs::create_dir_all(&root).expect("creating temp store root");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensuring store layout");
        (store, root)
    }

    fn make_test_repo_root_with_compat(compat_json: &serde_json::Value) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-test-repo-{}-{}",
            std::process::id(),
            nanos
        ));
        let compat_path = root.join(VERSION_COMPAT_PATH);
        if let Some(parent) = compat_path.parent() {
            fs::create_dir_all(parent).expect("creating compat parent");
        }
        fs::write(
            &compat_path,
            serde_json::to_string_pretty(compat_json).expect("serialize compat"),
        )
        .expect("write compat json");
        root
    }

    fn write_test_tarball(path: &Path, entries: &[(&str, &str)]) {
        let file = File::create(path).expect("creating tarball file");
        let encoder = GzEncoder::new(file, Compression::default());
        let mut builder = Builder::new(encoder);
        for (entry_path, content) in entries {
            let bytes = content.as_bytes();
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o644);
            header.set_mtime(0);
            header.set_cksum();
            builder
                .append_data(&mut header, entry_path, bytes)
                .expect("append tar entry");
        }
        builder.finish().expect("finish tarball");
    }

    fn sample_running(action_id: &str) -> QueueRunningWithPath {
        QueueRunningWithPath {
            running: QueueRunning {
                schema_version: ACTION_SCHEMA_VERSION,
                action_id: action_id.to_string(),
                enqueued_utc: Utc::now(),
                priority: DEFAULT_QUEUE_PRIORITY,
                action: ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                    version: "v0.37.0".to_string(),
                    discovery_runtime: None,
                },
                lease_owner: "worker-test".to_string(),
                lease_acquired_utc: Utc::now(),
                lease_expires_utc: Utc::now(),
            },
            path: PathBuf::from("/tmp/running.json"),
        }
    }

    #[test]
    fn normalize_subtree_path_normalizes_and_rejects_invalid_values() {
        assert_eq!(
            normalize_subtree_path("./xls/modules/add_dual_path/").expect("normalize"),
            "xls/modules/add_dual_path"
        );
        assert_eq!(
            normalize_subtree_path("xls\\modules\\add_dual_path").expect("normalize windows-style"),
            "xls/modules/add_dual_path"
        );
        assert!(normalize_subtree_path("").is_err());
        assert!(normalize_subtree_path("/xls/modules/add_dual_path").is_err());
        assert!(normalize_subtree_path("../xls/modules/add_dual_path").is_err());
    }

    #[test]
    fn determine_dslx_import_context_switches_on_subtree_stdlib_presence() {
        let temp_root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-test-import-context-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock before epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&temp_root).expect("create temp root");

        let without_stdlib = determine_dslx_import_context(&temp_root);
        assert_eq!(without_stdlib.stdlib_source, "toolchain_release");
        assert_eq!(
            without_stdlib.dslx_stdlib_path,
            "/tmp/xlsynth-release/xls/dslx/stdlib"
        );
        assert_eq!(
            without_stdlib.dslx_path,
            [
                "/inputs/subtree",
                "/tmp/xlsynth-release",
                "/tmp/xlsynth-release/xls/dslx/stdlib",
            ]
            .join(DSLX_PATH_LIST_SEPARATOR)
        );
        assert!(without_stdlib.dslx_path.contains(DSLX_PATH_LIST_SEPARATOR));
        assert!(!without_stdlib.dslx_path.contains(';'));

        let stdlib_dir = temp_root.join("xls/dslx/stdlib");
        fs::create_dir_all(&stdlib_dir).expect("create subtree stdlib");
        let with_stdlib = determine_dslx_import_context(&temp_root);
        assert_eq!(with_stdlib.stdlib_source, "subtree");
        assert_eq!(
            with_stdlib.dslx_stdlib_path,
            "/inputs/subtree/xls/dslx/stdlib"
        );
        assert_eq!(
            with_stdlib.dslx_path,
            ["/inputs/subtree", "/inputs/subtree/xls/dslx/stdlib"].join(DSLX_PATH_LIST_SEPARATOR)
        );
        assert!(with_stdlib.dslx_path.contains(DSLX_PATH_LIST_SEPARATOR));
        assert!(!with_stdlib.dslx_path.contains(';'));

        fs::remove_dir_all(&temp_root).expect("cleanup temp root");
    }

    #[test]
    fn extract_tar_gz_subtree_safely_extracts_only_requested_subtree_files() {
        let temp_root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-test-subtree-extract-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock before epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&temp_root).expect("create temp root");
        let tarball_path = temp_root.join("source.tar.gz");
        write_test_tarball(
            &tarball_path,
            &[
                (
                    "xlsynth-0.35.0/xls/modules/add_dual_path/dual_path.x",
                    "pub fn add_dual_path_f32(x: u32, y: u32) -> u32 { x + y }",
                ),
                (
                    "xlsynth-0.35.0/xls/modules/add_dual_path/near_path.x",
                    "pub fn near_path(x: u32) -> u32 { x }",
                ),
                (
                    "xlsynth-0.35.0/xls/dslx/stdlib/std.x",
                    "pub fn fake_std() -> bool { true }",
                ),
            ],
        );
        let output = temp_root.join("out");
        fs::create_dir_all(&output).expect("create output dir");
        let extracted_count =
            extract_tar_gz_subtree_safely(&tarball_path, &output, "xls/modules/add_dual_path")
                .expect("extract subtree");
        assert_eq!(extracted_count, 2);
        let dual_path = output.join("xls/modules/add_dual_path/dual_path.x");
        let near_path = output.join("xls/modules/add_dual_path/near_path.x");
        assert!(dual_path.is_file());
        assert!(near_path.is_file());
        assert!(!output.join("xls/dslx/stdlib/std.x").exists());
        assert!(
            fs::read_to_string(&dual_path)
                .expect("read extracted dual_path")
                .contains("add_dual_path_f32")
        );

        fs::remove_dir_all(&temp_root).expect("cleanup temp root");
    }

    #[test]
    fn enqueue_processing_for_crate_version_enqueues_stdlib_and_module_roots() {
        let (store, store_root) = make_test_store();
        let repo_root = make_test_repo_root_with_compat(&json!({
            "0.31.0": {
                "xlsynth_release_version": "0.35.0",
                "crate_release_datetime": "2026-02-20 00:00:00 UTC"
            }
        }));

        enqueue_processing_for_crate_version(&store, &repo_root, "v0.31.0", DEFAULT_QUEUE_PRIORITY)
            .expect("enqueue roots");
        let roots = canonical_root_actions_for_crate_version(&repo_root, "0.31.0", "v0.35.0")
            .expect("build canonical roots");
        assert_eq!(roots.len(), 1 + MODULE_SUBTREE_ROOT_PATHS.len());
        for root in &roots {
            let action_id = compute_action_id(root).expect("compute root action id");
            assert!(
                store.pending_queue_path(&action_id).exists(),
                "expected pending queue record for root action {}",
                action_id
            );
        }
        let pending_count_first = list_queue_files(&store.queue_pending_dir())
            .expect("list pending queue")
            .len();
        assert_eq!(pending_count_first, roots.len());

        enqueue_processing_for_crate_version(&store, &repo_root, "v0.31.0", DEFAULT_QUEUE_PRIORITY)
            .expect("re-enqueue roots idempotently");
        let pending_count_second = list_queue_files(&store.queue_pending_dir())
            .expect("list pending queue second time")
            .len();
        assert_eq!(pending_count_second, roots.len());

        fs::remove_dir_all(store_root).expect("cleaning temp store");
        fs::remove_dir_all(repo_root).expect("cleaning temp repo");
    }

    #[test]
    fn parse_release_tag_handles_patch2() {
        assert_eq!(
            parse_release_tag("v0.37.0-2").expect("parse"),
            ReleaseTag {
                major: 0,
                minor: 37,
                patch: 0,
                patch2: 2
            }
        );
        assert_eq!(
            parse_release_tag("v0.37.0").expect("parse"),
            ReleaseTag {
                major: 0,
                minor: 37,
                patch: 0,
                patch2: 0
            }
        );
    }

    #[test]
    fn dslx_mangle_utility_sanitizes_components() {
        assert_eq!(
            dslx_to_mangled_ir_fn_name("my.mod", "f-step"),
            "__my_mod__f_step"
        );
    }

    #[test]
    fn parse_discovered_dslx_functions_parses_and_keeps_parametric_flag() {
        let jsonl = r#"
{"dslx_file":"xls/dslx/stdlib/acm_random.x","name":"rng_next","is_parametric":false}
{"dslx_file":"xls/dslx/stdlib/bits.x","name":"slice_update","is_parametric":true}
"#;

        let parsed = parse_discovered_dslx_functions(jsonl).expect("parse discovered functions");
        assert_eq!(
            parsed,
            vec![
                DiscoveredDslxFunction {
                    dslx_file: "xls/dslx/stdlib/acm_random.x".to_string(),
                    fn_name: "rng_next".to_string(),
                    is_parametric: false
                },
                DiscoveredDslxFunction {
                    dslx_file: "xls/dslx/stdlib/bits.x".to_string(),
                    fn_name: "slice_update".to_string(),
                    is_parametric: true
                }
            ]
        );
    }

    #[test]
    fn parse_dslx_list_fns_failed_files_extracts_paths() {
        let jsonl = r#"
{"dslx_file":"xls/dslx/stdlib/abs_diff.x","returncode":101}
{"dslx_file":"xls/dslx/stdlib/std.x","returncode":101}
"#;
        let parsed = parse_dslx_list_fns_failed_files(jsonl).expect("parse failed files");
        assert_eq!(
            parsed,
            vec![
                "xls/dslx/stdlib/abs_diff.x".to_string(),
                "xls/dslx/stdlib/std.x".to_string(),
            ]
        );
    }

    #[test]
    fn structural_hash_for_matching_target_g8r_action_uses_details_hash() {
        let runtime = DriverRuntimeSpec {
            driver_version: "0.31.0".to_string(),
            release_platform: DEFAULT_RELEASE_PLATFORM.to_string(),
            docker_image: default_driver_image("0.31.0"),
            dockerfile: DEFAULT_DOCKERFILE.to_string(),
        };
        let action = ActionSpec::DriverIrToG8rAig {
            ir_action_id: "opt-ir-action".to_string(),
            top_fn_name: None,
            fraig: true,
            lowering_mode: G8rLoweringMode::Default,
            version: "v0.35.0".to_string(),
            runtime,
        };
        let details = json!({
            INPUT_IR_FN_STRUCTURAL_HASH_DETAILS_KEY:
                "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789"
        });
        let mut by_opt_action = BTreeMap::new();
        by_opt_action.insert(
            "opt-ir-action".to_string(),
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        );
        let hash = structural_hash_for_matching_target_g8r_action(
            &action,
            Some(&details),
            "0.31.0",
            "0.35.0",
            true,
            &by_opt_action,
        )
        .expect("expected matching hash");
        assert_eq!(
            hash,
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        );
        assert!(
            structural_hash_for_matching_target_g8r_action(
                &action,
                Some(&details),
                "0.31.0",
                "0.35.0",
                false,
                &by_opt_action
            )
            .is_none()
        );
    }

    #[test]
    fn record_structural_hash_coverage_prefers_higher_priority_state() {
        let mut by_hash = BTreeMap::new();
        let hash = "f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0".to_string();

        record_structural_hash_coverage(
            &mut by_hash,
            hash.clone(),
            StructuralHashCoverageState::Failed,
        );
        record_structural_hash_coverage(
            &mut by_hash,
            hash.clone(),
            StructuralHashCoverageState::Done,
        );
        record_structural_hash_coverage(
            &mut by_hash,
            hash.clone(),
            StructuralHashCoverageState::Pending,
        );

        let record = by_hash.get(&hash).expect("coverage record present");
        assert_eq!(record.state, StructuralHashCoverageState::Done);
    }

    #[test]
    fn write_failed_record_persists_failed_action_record_without_queue_failed_copy() {
        let (store, root) = make_test_store();
        let action_id = "abcd1234";
        let running = sample_running(action_id);
        write_failed_record(&store, &running, "worker-a", "boom").expect("writing failed record");

        assert!(store.failed_action_record_exists(action_id));

        let persisted = load_queue_failed_record(&store, action_id)
            .expect("loading persisted failed record")
            .expect("failed record should exist");
        assert_eq!(persisted.error, "boom");

        fs::remove_dir_all(root).expect("cleaning temp store");
    }

    #[test]
    fn load_failed_queue_records_reads_persisted_records_only() {
        let (store, root) = make_test_store();
        let action_id = "persisted-1";
        let action = ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
            version: "v0.37.0".to_string(),
            discovery_runtime: None,
        };
        let persisted = QueueFailed {
            schema_version: ACTION_SCHEMA_VERSION,
            action_id: action_id.to_string(),
            enqueued_utc: Utc::now(),
            failed_utc: Utc::now(),
            failed_by: "persisted".to_string(),
            action: action.clone(),
            error: "persisted error".to_string(),
        };
        write_failed_action_record(&store, &persisted).expect("writing persisted failed record");

        let persisted_only_id = "persisted-only-2";
        let persisted_only = QueueFailed {
            schema_version: ACTION_SCHEMA_VERSION,
            action_id: persisted_only_id.to_string(),
            enqueued_utc: Utc::now(),
            failed_utc: Utc::now(),
            failed_by: "persisted".to_string(),
            action,
            error: "persisted only".to_string(),
        };
        write_failed_action_record(&store, &persisted_only).expect("writing persisted-only record");

        let records = load_failed_queue_records(&store).expect("loading failed queue records");
        assert_eq!(records.len(), 2);
        let by_id: BTreeMap<String, QueueFailed> = records
            .into_iter()
            .map(|record| (record.action_id.clone(), record))
            .collect();

        assert_eq!(
            by_id.get(action_id).expect("persisted record").error,
            "persisted error"
        );
        assert_eq!(
            by_id
                .get(persisted_only_id)
                .expect("persisted-only record")
                .error,
            "persisted only"
        );

        fs::remove_dir_all(root).expect("cleaning temp store");
    }
}
