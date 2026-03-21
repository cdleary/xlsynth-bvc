use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use log::warn;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Component, Path, PathBuf};

use crate::query::{
    build_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index_bytes,
    build_ir_fn_corpus_g8r_vs_yosys_dataset_index_bytes,
    rebuild_stdlib_fn_version_timeline_dataset_index, rebuild_stdlib_fns_trend_dataset_index,
    rebuild_stdlib_g8r_vs_yosys_dataset_index, rebuild_versions_cards_index,
};
use crate::store::ArtifactStore;
use crate::view::StdlibTrendKind;
use crate::{
    WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME,
    WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME,
};

pub(crate) const STATIC_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
pub(crate) const STATIC_SNAPSHOT_MANIFEST_FILENAME: &str = "snapshot_manifest.v1.json";
pub(crate) const STATIC_SNAPSHOT_WEB_INDEX_DIR: &str = "web_index";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StaticSnapshotDatasetFile {
    pub(crate) index_key: String,
    pub(crate) relpath: String,
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StaticSnapshotManifest {
    pub(crate) schema_version: u32,
    pub(crate) snapshot_id: String,
    pub(crate) generated_utc: DateTime<Utc>,
    pub(crate) git_commit: Option<String>,
    pub(crate) source_action_set_sha256: Option<String>,
    pub(crate) dataset_files: Vec<StaticSnapshotDatasetFile>,
    pub(crate) total_dataset_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BuildStaticSnapshotSummary {
    pub(crate) out_dir: String,
    pub(crate) rebuild_web_indices_ran: bool,
    pub(crate) snapshot_id: String,
    pub(crate) generated_utc: DateTime<Utc>,
    pub(crate) git_commit: Option<String>,
    pub(crate) source_action_set_sha256: Option<String>,
    pub(crate) dataset_file_count: usize,
    pub(crate) total_dataset_bytes: u64,
    pub(crate) manifest_path: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VerifyStaticSnapshotSummary {
    pub(crate) snapshot_dir: String,
    pub(crate) snapshot_id: String,
    pub(crate) generated_utc: DateTime<Utc>,
    pub(crate) dataset_file_count: usize,
    pub(crate) total_dataset_bytes: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct BuildStaticSnapshotOptions {
    pub(crate) out_dir: PathBuf,
    pub(crate) overwrite: bool,
    pub(crate) skip_rebuild_web_indices: bool,
}

fn ensure_empty_output_dir(path: &Path, overwrite: bool) -> Result<()> {
    if path.exists() {
        if !overwrite {
            bail!(
                "snapshot output directory already exists; rerun with --overwrite: {}",
                path.display()
            );
        }
        fs::remove_dir_all(path).with_context(|| {
            format!("removing existing snapshot output dir: {}", path.display())
        })?;
    }
    fs::create_dir_all(path)
        .with_context(|| format!("creating snapshot output dir: {}", path.display()))?;
    Ok(())
}

fn index_key_to_relpath(index_key: &str) -> Result<PathBuf> {
    let trimmed = index_key.trim().trim_start_matches('/');
    if trimmed.is_empty() {
        bail!("snapshot index key must not be empty");
    }
    let mut rel = PathBuf::new();
    for component in Path::new(trimmed).components() {
        match component {
            Component::Normal(part) => rel.push(part),
            Component::CurDir => {}
            Component::ParentDir => {
                bail!(
                    "snapshot index key must not contain parent traversal: {}",
                    index_key
                )
            }
            Component::RootDir | Component::Prefix(_) => {
                bail!("snapshot index key must not be absolute: {}", index_key)
            }
        }
    }
    if rel.as_os_str().is_empty() {
        bail!("snapshot index key normalized to empty path: {}", index_key);
    }
    Ok(rel)
}

fn should_include_snapshot_index_key(index_key: &str) -> bool {
    !index_key.contains("/incremental-delta/") && index_key != "stdlib-file-action-graph.v1.json"
}

fn should_copy_snapshot_store_index(index_key: &str, direct_heavy_indices_written: bool) -> bool {
    should_include_snapshot_index_key(index_key)
        && (!direct_heavy_indices_written
            || (index_key != WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME
                && index_key != WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME))
}

fn write_snapshot_dataset_entry(
    out_dir: &Path,
    index_key: &str,
    bytes: &[u8],
) -> Result<StaticSnapshotDatasetFile> {
    let disk_path = snapshot_web_index_path(out_dir, index_key)?;
    if let Some(parent) = disk_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating snapshot dataset parent: {}", parent.display()))?;
    }
    fs::write(&disk_path, bytes)
        .with_context(|| format!("writing snapshot dataset file: {}", disk_path.display()))?;
    let relpath = disk_path
        .strip_prefix(out_dir)
        .expect("snapshot dataset path should be under output dir")
        .to_string_lossy()
        .replace('\\', "/");
    Ok(StaticSnapshotDatasetFile {
        index_key: index_key.to_string(),
        relpath,
        bytes: bytes.len() as u64,
        sha256: sha256_hex(bytes),
    })
}

fn rebuild_snapshot_web_indices(
    store: &ArtifactStore,
    repo_root: &Path,
    out_dir: &Path,
) -> Result<Vec<StaticSnapshotDatasetFile>> {
    warn!("rebuild-snapshot-web-indices start");

    warn!("rebuild-snapshot-web-indices phase=versions-summary begin");
    let versions_summary = rebuild_versions_cards_index(store, repo_root)?;
    warn!(
        "rebuild-snapshot-web-indices phase=versions-summary done cards={} unattributed={}",
        versions_summary.card_count, versions_summary.unattributed_actions
    );

    for (kind, fraig) in [
        (StdlibTrendKind::G8r, false),
        (StdlibTrendKind::YosysAbc, false),
    ] {
        warn!(
            "rebuild-snapshot-web-indices phase=stdlib-fns-trend begin kind={} fraig={}",
            kind.view_path(),
            fraig
        );
        let summary = rebuild_stdlib_fns_trend_dataset_index(store, kind, fraig)?;
        warn!(
            "rebuild-snapshot-web-indices phase=stdlib-fns-trend done kind={} fraig={} series={} points={}",
            summary.kind_path, summary.fraig, summary.series_count, summary.point_count
        );
    }

    warn!("rebuild-snapshot-web-indices phase=stdlib-fn-timeline begin");
    let stdlib_fn_timeline = rebuild_stdlib_fn_version_timeline_dataset_index(store)?;
    warn!(
        "rebuild-snapshot-web-indices phase=stdlib-fn-timeline done files={} functions={}",
        stdlib_fn_timeline.file_count, stdlib_fn_timeline.fn_count
    );

    for fraig in [false, true] {
        warn!(
            "rebuild-snapshot-web-indices phase=stdlib-g8r-vs-yosys begin fraig={}",
            fraig
        );
        let summary = rebuild_stdlib_g8r_vs_yosys_dataset_index(store, fraig)?;
        warn!(
            "rebuild-snapshot-web-indices phase=stdlib-g8r-vs-yosys done fraig={} samples={} versions={}",
            summary.fraig, summary.sample_count, summary.crate_versions
        );
    }

    warn!("rebuild-snapshot-web-indices phase=ir-fn-corpus-g8r-vs-yosys-abc begin");
    let (ir_fn_corpus_g8r_vs_yosys, ir_fn_corpus_g8r_vs_yosys_bytes, seed_ir_node_count_cache) =
        build_ir_fn_corpus_g8r_vs_yosys_dataset_index_bytes(store, repo_root)?;
    let mut direct_files = Vec::with_capacity(2);
    direct_files.push(write_snapshot_dataset_entry(
        out_dir,
        WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME,
        &ir_fn_corpus_g8r_vs_yosys_bytes,
    )?);
    warn!(
        "rebuild-snapshot-web-indices phase=ir-fn-corpus-g8r-vs-yosys-abc done samples={} versions={} direct_bytes={}",
        ir_fn_corpus_g8r_vs_yosys.sample_count,
        ir_fn_corpus_g8r_vs_yosys.crate_versions,
        ir_fn_corpus_g8r_vs_yosys.index_bytes
    );

    warn!("rebuild-snapshot-web-indices phase=ir-fn-g8r-abc-vs-codegen-yosys-abc begin");
    let (
        ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc,
        ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_bytes,
    ) = build_ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_dataset_index_bytes(
        store,
        repo_root,
        Some(seed_ir_node_count_cache),
    )?;
    direct_files.push(write_snapshot_dataset_entry(
        out_dir,
        WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME,
        &ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_bytes,
    )?);
    warn!(
        "rebuild-snapshot-web-indices phase=ir-fn-g8r-abc-vs-codegen-yosys-abc done samples={} versions={} direct_bytes={}",
        ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc.sample_count,
        ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc.crate_versions,
        ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc.index_bytes
    );

    warn!("rebuild-snapshot-web-indices done");
    Ok(direct_files)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn snapshot_id_for_dataset_files(files: &[StaticSnapshotDatasetFile]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(STATIC_SNAPSHOT_SCHEMA_VERSION.to_string().as_bytes());
    for file in files {
        hasher.update(file.index_key.as_bytes());
        hasher.update([0]);
        hasher.update(file.sha256.as_bytes());
        hasher.update([0]);
        hasher.update(file.bytes.to_le_bytes());
    }
    hex::encode(hasher.finalize())
}

fn read_git_commit(repo_root: &Path) -> Option<String> {
    let output = std::process::Command::new("git")
        .arg("rev-parse")
        .arg("HEAD")
        .current_dir(repo_root)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let commit = String::from_utf8(output.stdout).ok()?;
    let commit = commit.trim();
    if commit.is_empty() {
        None
    } else {
        Some(commit.to_string())
    }
}

fn read_source_action_set_sha256(store: &ArtifactStore) -> Option<String> {
    let manifest_key = crate::service::ir_fn_corpus_structural_manifest_index_key();
    let bytes = store.load_web_index_bytes(manifest_key).ok().flatten()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    value
        .get("source_action_set_sha256")
        .and_then(|v| v.as_str())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

pub(crate) fn snapshot_web_index_path(snapshot_dir: &Path, index_key: &str) -> Result<PathBuf> {
    let rel = index_key_to_relpath(index_key)?;
    Ok(snapshot_dir.join(STATIC_SNAPSHOT_WEB_INDEX_DIR).join(rel))
}

pub(crate) fn load_static_snapshot_manifest(snapshot_dir: &Path) -> Result<StaticSnapshotManifest> {
    let manifest_path = snapshot_dir.join(STATIC_SNAPSHOT_MANIFEST_FILENAME);
    let bytes = fs::read(&manifest_path).with_context(|| {
        format!(
            "reading static snapshot manifest: {}",
            manifest_path.display()
        )
    })?;
    let manifest: StaticSnapshotManifest = serde_json::from_slice(&bytes).with_context(|| {
        format!(
            "parsing static snapshot manifest: {}",
            manifest_path.display()
        )
    })?;
    if manifest.schema_version != STATIC_SNAPSHOT_SCHEMA_VERSION {
        bail!(
            "unsupported static snapshot schema_version={} (expected {}) at {}",
            manifest.schema_version,
            STATIC_SNAPSHOT_SCHEMA_VERSION,
            manifest_path.display()
        );
    }
    Ok(manifest)
}

pub(crate) fn build_static_snapshot(
    store: &ArtifactStore,
    repo_root: &Path,
    options: &BuildStaticSnapshotOptions,
) -> Result<BuildStaticSnapshotSummary> {
    ensure_empty_output_dir(&options.out_dir, options.overwrite)?;
    fs::create_dir_all(options.out_dir.join(STATIC_SNAPSHOT_WEB_INDEX_DIR)).with_context(|| {
        format!(
            "creating static snapshot web_index directory: {}",
            options
                .out_dir
                .join(STATIC_SNAPSHOT_WEB_INDEX_DIR)
                .display()
        )
    })?;

    let mut dataset_files = Vec::new();
    let mut total_dataset_bytes = 0_u64;
    let direct_heavy_indices_written = !options.skip_rebuild_web_indices;
    if !options.skip_rebuild_web_indices {
        let direct_files = rebuild_snapshot_web_indices(store, repo_root, &options.out_dir)
            .context("rebuilding snapshot web indices before snapshot")?;
        total_dataset_bytes += direct_files.iter().map(|entry| entry.bytes).sum::<u64>();
        dataset_files.extend(direct_files);
    }

    let mut entries = store
        .list_web_index_entries_with_prefix("")
        .context("listing web index entries for static snapshot")?;
    entries.retain(|(index_key, _)| {
        should_copy_snapshot_store_index(index_key, direct_heavy_indices_written)
    });
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    for (index_key, bytes) in entries {
        let entry = write_snapshot_dataset_entry(&options.out_dir, &index_key, &bytes)?;
        total_dataset_bytes += entry.bytes;
        dataset_files.push(entry);
    }

    let snapshot_id = snapshot_id_for_dataset_files(&dataset_files);
    let manifest = StaticSnapshotManifest {
        schema_version: STATIC_SNAPSHOT_SCHEMA_VERSION,
        snapshot_id: snapshot_id.clone(),
        generated_utc: Utc::now(),
        git_commit: read_git_commit(repo_root),
        source_action_set_sha256: read_source_action_set_sha256(store),
        dataset_files,
        total_dataset_bytes,
    };
    let manifest_path = options.out_dir.join(STATIC_SNAPSHOT_MANIFEST_FILENAME);
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).context("serializing static snapshot manifest")?,
    )
    .with_context(|| {
        format!(
            "writing static snapshot manifest: {}",
            manifest_path.display()
        )
    })?;

    Ok(BuildStaticSnapshotSummary {
        out_dir: options.out_dir.display().to_string(),
        rebuild_web_indices_ran: !options.skip_rebuild_web_indices,
        snapshot_id: manifest.snapshot_id,
        generated_utc: manifest.generated_utc,
        git_commit: manifest.git_commit,
        source_action_set_sha256: manifest.source_action_set_sha256,
        dataset_file_count: manifest.dataset_files.len(),
        total_dataset_bytes: manifest.total_dataset_bytes,
        manifest_path: manifest_path.display().to_string(),
    })
}

pub(crate) fn verify_static_snapshot(snapshot_dir: &Path) -> Result<VerifyStaticSnapshotSummary> {
    let manifest = load_static_snapshot_manifest(snapshot_dir)?;

    let mut recomputed_total_bytes = 0_u64;
    for entry in &manifest.dataset_files {
        let expected_relpath = index_key_to_relpath(&entry.index_key)?;
        let expected_relpath = Path::new(STATIC_SNAPSHOT_WEB_INDEX_DIR)
            .join(expected_relpath)
            .to_string_lossy()
            .replace('\\', "/");
        if expected_relpath != entry.relpath {
            bail!(
                "snapshot dataset relpath mismatch for key {}: manifest={} expected={}",
                entry.index_key,
                entry.relpath,
                expected_relpath
            );
        }

        let disk_path = snapshot_dir.join(&entry.relpath);
        let bytes = fs::read(&disk_path)
            .with_context(|| format!("reading snapshot dataset file: {}", disk_path.display()))?;
        let actual_bytes = bytes.len() as u64;
        if actual_bytes != entry.bytes {
            bail!(
                "snapshot dataset size mismatch for {}: manifest={} actual={}",
                entry.relpath,
                entry.bytes,
                actual_bytes
            );
        }
        let actual_sha = sha256_hex(&bytes);
        if actual_sha != entry.sha256 {
            bail!(
                "snapshot dataset sha256 mismatch for {}: manifest={} actual={}",
                entry.relpath,
                entry.sha256,
                actual_sha
            );
        }
        recomputed_total_bytes += actual_bytes;
    }

    if recomputed_total_bytes != manifest.total_dataset_bytes {
        bail!(
            "snapshot total dataset bytes mismatch: manifest={} actual={}",
            manifest.total_dataset_bytes,
            recomputed_total_bytes
        );
    }

    let recomputed_snapshot_id = snapshot_id_for_dataset_files(&manifest.dataset_files);
    if recomputed_snapshot_id != manifest.snapshot_id {
        bail!(
            "snapshot id mismatch: manifest={} actual={}",
            manifest.snapshot_id,
            recomputed_snapshot_id
        );
    }

    Ok(VerifyStaticSnapshotSummary {
        snapshot_dir: snapshot_dir.display().to_string(),
        snapshot_id: manifest.snapshot_id,
        generated_utc: manifest.generated_utc,
        dataset_file_count: manifest.dataset_files.len(),
        total_dataset_bytes: manifest.total_dataset_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_temp_dir(prefix: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "xlsynth-bvc-snapshot-test-{}-{}-{}",
            prefix,
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[test]
    fn static_snapshot_build_and_verify_roundtrip() {
        let root = make_temp_dir("build-verify");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure layout");
        store
            .write_web_index_bytes("versions-summary.v1.json", br#"{"cards":[]}"#)
            .expect("write web index");
        store
            .write_web_index_bytes(
                "ir-fn-corpus-structural.v1/manifest.json",
                br#"{"source_action_set_sha256":"abc123"}"#,
            )
            .expect("write structural manifest");

        let out_dir = root.join("snapshot-out");
        let summary = build_static_snapshot(
            &store,
            &root,
            &BuildStaticSnapshotOptions {
                out_dir: out_dir.clone(),
                overwrite: false,
                skip_rebuild_web_indices: true,
            },
        )
        .expect("build snapshot");
        assert_eq!(summary.dataset_file_count, 2);

        let verify = verify_static_snapshot(&out_dir).expect("verify snapshot");
        assert_eq!(verify.dataset_file_count, 2);
    }

    #[test]
    fn static_snapshot_verify_detects_tamper() {
        let root = make_temp_dir("tamper");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure layout");
        store
            .write_web_index_bytes("versions-summary.v1.json", br#"{"cards":[]}"#)
            .expect("write web index");

        let out_dir = root.join("snapshot-out");
        build_static_snapshot(
            &store,
            &root,
            &BuildStaticSnapshotOptions {
                out_dir: out_dir.clone(),
                overwrite: false,
                skip_rebuild_web_indices: true,
            },
        )
        .expect("build snapshot");

        let tampered_path = out_dir.join("web_index").join("versions-summary.v1.json");
        fs::write(&tampered_path, b"tampered").expect("tamper file");
        let err = verify_static_snapshot(&out_dir).expect_err("verify should fail");
        assert!(
            err.to_string().contains("sha256 mismatch")
                || err.to_string().contains("size mismatch"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn static_snapshot_build_skips_incremental_delta_rows() {
        let root = make_temp_dir("skip-incremental");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure layout");
        store
            .write_web_index_bytes(
                "ir-fn-corpus-g8r-vs-yosys-abc.v3.json",
                br#"{"schema_version":3}"#,
            )
            .expect("write base index");
        store
            .write_web_index_bytes(
                "ir-fn-corpus-g8r-vs-yosys-abc.v3.json/incremental-delta/row-1.json",
                br#"{"row":1}"#,
            )
            .expect("write incremental delta row");

        let out_dir = root.join("snapshot-out");
        let summary = build_static_snapshot(
            &store,
            &root,
            &BuildStaticSnapshotOptions {
                out_dir: out_dir.clone(),
                overwrite: false,
                skip_rebuild_web_indices: true,
            },
        )
        .expect("build snapshot");

        assert_eq!(summary.dataset_file_count, 1);
        let manifest = load_static_snapshot_manifest(&out_dir).expect("load snapshot manifest");
        assert_eq!(manifest.dataset_files.len(), 1);
        assert_eq!(
            manifest.dataset_files[0].index_key,
            "ir-fn-corpus-g8r-vs-yosys-abc.v3.json"
        );
        let delta_path = out_dir
            .join("web_index")
            .join("ir-fn-corpus-g8r-vs-yosys-abc.v3.json")
            .join("incremental-delta");
        assert!(
            !delta_path.exists(),
            "incremental delta path should be skipped"
        );
    }

    #[test]
    fn static_snapshot_build_skips_stdlib_file_action_graph_index() {
        let root = make_temp_dir("skip-action-graph");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure layout");
        store
            .write_web_index_bytes("versions-summary.v1.json", br#"{"cards":[]}"#)
            .expect("write versions index");
        store
            .write_web_index_bytes(
                "stdlib-file-action-graph.v1.json",
                br#"{"schema_version":1,"nodes":[]}"#,
            )
            .expect("write file action graph index");

        let out_dir = root.join("snapshot-out");
        let summary = build_static_snapshot(
            &store,
            &root,
            &BuildStaticSnapshotOptions {
                out_dir: out_dir.clone(),
                overwrite: false,
                skip_rebuild_web_indices: true,
            },
        )
        .expect("build snapshot");

        assert_eq!(summary.dataset_file_count, 1);
        let manifest = load_static_snapshot_manifest(&out_dir).expect("load snapshot manifest");
        assert_eq!(manifest.dataset_files.len(), 1);
        assert_eq!(
            manifest.dataset_files[0].index_key,
            "versions-summary.v1.json"
        );
        assert!(
            !out_dir
                .join("web_index")
                .join("stdlib-file-action-graph.v1.json")
                .exists(),
            "file action graph should be omitted from snapshot"
        );
    }

    #[test]
    fn snapshot_web_index_path_rejects_parent_traversal() {
        let root = make_temp_dir("path");
        let err = snapshot_web_index_path(&root, "../escape.json").expect_err("must fail");
        assert!(
            err.to_string()
                .contains("must not contain parent traversal"),
            "unexpected error: {err:#}"
        );
    }
}
