// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use log::warn;
use prost::Message;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Component, Path, PathBuf};
use walkdir::WalkDir;

use crate::analysis::decode_analysis_report;
use crate::campaign::{campaign_analysis_path, list_finalized_campaign_runs};
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
    WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME, WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY,
    WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_NAMESPACE, WEB_STDLIB_FN_TIMELINE_INDEX_FILENAME,
    WEB_STDLIB_FNS_TREND_G8R_FRAIG_FALSE_INDEX_FILENAME,
    WEB_STDLIB_FNS_TREND_YOSYS_ABC_INDEX_FILENAME,
    WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME,
    WEB_STDLIB_G8R_VS_YOSYS_FRAIG_TRUE_INDEX_FILENAME, WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
};
use crate::{proto::FILE_DESCRIPTOR_SET, proto::v1 as pb};

pub(crate) const STATIC_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
pub(crate) const STATIC_SNAPSHOT_IDENTITY_VERSION: u32 = 1;
pub(crate) const PUBLICATION_POLICY_VERSION: u32 = 6;
pub(crate) const STATIC_SNAPSHOT_MANIFEST_FILENAME: &str = "snapshot_manifest.v1.pb";
pub(crate) const STATIC_SNAPSHOT_WEB_INDEX_DIR: &str = "web_index";

#[derive(Debug, Clone)]
pub(crate) struct StaticSnapshotDatasetFile {
    pub(crate) index_key: String,
    pub(crate) relpath: String,
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
}

#[derive(Debug, Clone)]
pub(crate) struct StaticSnapshotManifest {
    pub(crate) schema_version: u32,
    pub(crate) snapshot_id: String,
    pub(crate) generated_utc: DateTime<Utc>,
    pub(crate) git_commit: Option<String>,
    pub(crate) source_action_set_sha256: Option<String>,
    pub(crate) dataset_files: Vec<StaticSnapshotDatasetFile>,
    pub(crate) total_dataset_bytes: u64,
    pub(crate) campaign_ids: Vec<String>,
    pub(crate) run_ids: Vec<String>,
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
    pub(crate) run_count: usize,
    pub(crate) total_dataset_bytes: u64,
    pub(crate) manifest_path: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VerifyStaticSnapshotSummary {
    pub(crate) snapshot_dir: String,
    pub(crate) snapshot_id: String,
    pub(crate) generated_utc: DateTime<Utc>,
    pub(crate) dataset_file_count: usize,
    pub(crate) run_count: usize,
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

fn observe_generated_utc(latest: &mut Option<DateTime<Utc>>, candidate: DateTime<Utc>) {
    if latest.as_ref().is_none_or(|current| candidate > *current) {
        *latest = Some(candidate);
    }
}

fn observe_json_generated_utc(latest: &mut Option<DateTime<Utc>>, bytes: &[u8]) {
    let Some(value) = serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|value| value.get("generated_utc").cloned())
        .and_then(|value| value.as_str().map(str::to_string))
        .and_then(|value| DateTime::parse_from_rfc3339(&value).ok())
    else {
        return;
    };
    observe_generated_utc(latest, value.with_timezone(&Utc));
}

fn observe_proto_generated_utc(
    latest: &mut Option<DateTime<Utc>>,
    timestamp: Option<&prost_types::Timestamp>,
) {
    if let Some(value) = timestamp
        .filter(|timestamp| (0..1_000_000_000).contains(&timestamp.nanos))
        .and_then(|timestamp| DateTime::from_timestamp(timestamp.seconds, timestamp.nanos as u32))
    {
        observe_generated_utc(latest, value);
    }
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

fn is_public_structural_group_index_key(index_key: &str) -> bool {
    let Some(suffix) = index_key
        .strip_prefix(WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_NAMESPACE)
        .and_then(|suffix| suffix.strip_prefix("/by-hash/"))
    else {
        return false;
    };
    let mut parts = suffix.split('/');
    let (Some(first_shard), Some(second_shard), Some(filename), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    let Some(hash) = filename.strip_suffix(".json") else {
        return false;
    };
    hash.len() == 64
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && first_shard == &hash[0..2]
        && second_shard == &hash[2..4]
}

fn should_include_snapshot_index_key(index_key: &str) -> bool {
    matches!(
        index_key,
        WEB_VERSIONS_SUMMARY_INDEX_FILENAME
            | WEB_STDLIB_FNS_TREND_G8R_FRAIG_FALSE_INDEX_FILENAME
            | WEB_STDLIB_FNS_TREND_YOSYS_ABC_INDEX_FILENAME
            | WEB_STDLIB_FN_TIMELINE_INDEX_FILENAME
            | WEB_STDLIB_G8R_VS_YOSYS_FRAIG_FALSE_INDEX_FILENAME
            | WEB_STDLIB_G8R_VS_YOSYS_FRAIG_TRUE_INDEX_FILENAME
            | WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME
            | WEB_IR_FN_CORPUS_G8R_ABC_VS_CODEGEN_YOSYS_ABC_INDEX_FILENAME
            | WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY
    ) || is_public_structural_group_index_key(index_key)
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
    let bytes = crate::query::canonicalize_public_web_index_json(index_key, bytes)
        .with_context(|| format!("projecting allowlisted public dataset {index_key}"))?;
    let disk_path = snapshot_web_index_path(out_dir, index_key)?;
    if let Some(parent) = disk_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating snapshot dataset parent: {}", parent.display()))?;
    }
    fs::write(&disk_path, &bytes)
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
        sha256: sha256_hex(&bytes),
    })
}

fn write_snapshot_run_entry(
    out_dir: &Path,
    run_id: &str,
    filename: &str,
    bytes: &[u8],
) -> Result<StaticSnapshotDatasetFile> {
    let relpath = format!("runs/{run_id}/{filename}");
    index_key_to_relpath(&relpath)?;
    let disk_path = out_dir.join(&relpath);
    if let Some(parent) = disk_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating public run parent: {}", parent.display()))?;
    }
    fs::write(&disk_path, bytes)
        .with_context(|| format!("writing public run protobuf: {}", disk_path.display()))?;
    Ok(StaticSnapshotDatasetFile {
        index_key: relpath.clone(),
        relpath,
        bytes: bytes.len() as u64,
        sha256: sha256_hex(bytes),
    })
}

fn public_run_from_manifest(manifest: &pb::CampaignRunManifest) -> Result<pb::PublicCampaignRun> {
    let campaign = manifest
        .campaign
        .as_ref()
        .context("campaign run missing campaign")?;
    let completion = manifest
        .completion
        .as_ref()
        .context("campaign run missing completion")?;
    Ok(pb::PublicCampaignRun {
        record_version: 1,
        campaign_id: manifest.campaign_id.clone(),
        run_id: manifest.run_id.clone(),
        campaign_name: campaign.campaign_name.clone(),
        campaign_semantic_version: campaign.semantic_version,
        crate_version: manifest.crate_version.clone(),
        dso_version: manifest.dso_version.clone(),
        status: manifest.status,
        updated_at: manifest.updated_at,
        root_action_ids: manifest
            .root_actions
            .iter()
            .map(|root| {
                root.action_id
                    .clone()
                    .context("campaign root missing action_id")
            })
            .collect::<Result<Vec<_>>>()?,
        completed_root_count: completion.completed_root_count,
        pending_count: completion.pending_count,
        running_count: completion.running_count,
        failed_count: completion.failed_count,
        canceled_count: completion.canceled_count,
        missing_output_count: completion.missing_outputs.len() as u64,
        failed_sample_count: completion.failed_samples.len() as u64,
        intentionally_skipped_samples: completion.intentionally_skipped_samples.clone(),
    })
}

fn validate_public_run(run: &pb::PublicCampaignRun) -> Result<()> {
    if run.record_version != 1 {
        bail!(
            "unsupported public campaign run version {}",
            run.record_version
        );
    }
    for (digest, field) in [
        (&run.campaign_id, "public_run.campaign_id"),
        (&run.run_id, "public_run.run_id"),
    ] {
        let digest = digest
            .as_ref()
            .with_context(|| format!("missing {field}"))?;
        if digest.value.len() != 32 {
            bail!("{field} must contain exactly 32 bytes");
        }
    }
    if run.campaign_name.trim().is_empty()
        || run.campaign_semantic_version == 0
        || run
            .crate_version
            .as_ref()
            .is_none_or(|v| v.value.is_empty())
        || run.dso_version.as_ref().is_none_or(|v| v.value.is_empty())
    {
        bail!("public campaign run identity fields are incomplete");
    }
    let status = pb::CampaignRunStatus::try_from(run.status)
        .context("public campaign run status is unknown")?;
    if !matches!(
        status,
        pb::CampaignRunStatus::Complete | pb::CampaignRunStatus::Degraded
    ) {
        bail!("public campaign run must be complete or degraded, got {status:?}");
    }
    let timestamp = run
        .updated_at
        .as_ref()
        .context("public run missing updated_at")?;
    if !(0..1_000_000_000).contains(&timestamp.nanos)
        || DateTime::from_timestamp(timestamp.seconds, timestamp.nanos as u32).is_none()
    {
        bail!("public run updated_at is invalid");
    }
    if run.root_action_ids.is_empty() {
        bail!("public run must contain root action ids");
    }
    let mut previous: Option<&[u8]> = None;
    for action_id in &run.root_action_ids {
        if action_id.value.len() != 32 {
            bail!("public run root action id must contain exactly 32 bytes");
        }
        if previous.is_some_and(|prior| prior >= action_id.value.as_slice()) {
            bail!("public run root action ids must be strictly sorted");
        }
        previous = Some(action_id.value.as_slice());
    }
    for skipped in &run.intentionally_skipped_samples {
        let action_id = skipped
            .action_id
            .as_ref()
            .context("public intentional skip is missing action id")?;
        if action_id.value.len() != 32 {
            bail!("public intentional skip action id must contain exactly 32 bytes");
        }
        if skipped.rule_id.trim().is_empty() || skipped.reason.trim().is_empty() {
            bail!("public intentional skip requires a rule id and reason");
        }
    }
    Ok(())
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

fn digest_from_hex(value: &str, field: &str) -> Result<pb::Sha256Digest> {
    let bytes = hex::decode(value).with_context(|| format!("decoding {field} as hex"))?;
    if bytes.len() != 32 {
        bail!("{field} must contain exactly 32 bytes, got {}", bytes.len());
    }
    Ok(pb::Sha256Digest { value: bytes })
}

fn digest_to_hex(value: &pb::Sha256Digest, field: &str) -> Result<String> {
    if value.value.len() != 32 {
        bail!(
            "{field} must contain exactly 32 bytes, got {}",
            value.value.len()
        );
    }
    Ok(hex::encode(&value.value))
}

fn descriptor_sha256() -> String {
    sha256_hex(FILE_DESCRIPTOR_SET)
}

fn media_type_for_relpath(relpath: &str) -> &'static str {
    if relpath.ends_with(".json") {
        "application/json"
    } else if relpath.ends_with(".pb") {
        "application/x-protobuf"
    } else if relpath.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if relpath.ends_with(".css") {
        "text/css; charset=utf-8"
    } else if relpath.ends_with(".js") {
        "text/javascript; charset=utf-8"
    } else {
        "application/octet-stream"
    }
}

fn dataset_file_to_proto(file: &StaticSnapshotDatasetFile) -> Result<pb::PublicationFile> {
    index_key_to_relpath(&file.index_key)?;
    let relpath = index_key_to_relpath(&file.relpath)?
        .to_string_lossy()
        .replace('\\', "/");
    Ok(pb::PublicationFile {
        logical_key: file.index_key.clone(),
        relpath: Some(pb::NormalizedRelpath { value: relpath }),
        bytes: file.bytes,
        sha256: Some(digest_from_hex(&file.sha256, "publication_file.sha256")?),
        media_type: media_type_for_relpath(&file.relpath).to_string(),
    })
}

fn dataset_file_from_proto(file: &pb::PublicationFile) -> Result<StaticSnapshotDatasetFile> {
    if file.logical_key.trim().is_empty() {
        bail!("publication_file.logical_key must not be empty");
    }
    index_key_to_relpath(&file.logical_key)?;
    let relpath = file
        .relpath
        .as_ref()
        .context("publication_file.relpath is required")?
        .value
        .clone();
    let normalized = index_key_to_relpath(&relpath)?
        .to_string_lossy()
        .replace('\\', "/");
    if relpath != normalized {
        bail!(
            "publication_file.relpath is not normalized: {:?} expected {:?}",
            relpath,
            normalized
        );
    }
    if file.media_type != media_type_for_relpath(&relpath) {
        bail!(
            "publication_file.media_type mismatch for {}: got {:?} expected {:?}",
            relpath,
            file.media_type,
            media_type_for_relpath(&relpath)
        );
    }
    Ok(StaticSnapshotDatasetFile {
        index_key: file.logical_key.clone(),
        relpath,
        bytes: file.bytes,
        sha256: digest_to_hex(
            file.sha256
                .as_ref()
                .context("publication_file.sha256 is required")?,
            "publication_file.sha256",
        )?,
    })
}

fn normalized_dataset_files(
    files: &[StaticSnapshotDatasetFile],
) -> Result<Vec<pb::PublicationFile>> {
    let mut files = files
        .iter()
        .map(dataset_file_to_proto)
        .collect::<Result<Vec<_>>>()?;
    files.sort_by(|a, b| {
        a.logical_key.cmp(&b.logical_key).then_with(|| {
            a.relpath
                .as_ref()
                .map(|v| &v.value)
                .cmp(&b.relpath.as_ref().map(|v| &v.value))
        })
    });
    for pair in files.windows(2) {
        if pair[0].logical_key == pair[1].logical_key {
            bail!("duplicate publication logical key: {}", pair[0].logical_key);
        }
        if pair[0].relpath == pair[1].relpath {
            bail!(
                "duplicate publication relpath: {}",
                pair[0]
                    .relpath
                    .as_ref()
                    .map(|v| v.value.as_str())
                    .unwrap_or("<missing>")
            );
        }
    }
    Ok(files)
}

fn snapshot_id_for_dataset_files(
    files: &[StaticSnapshotDatasetFile],
    source_action_set_sha256: Option<&str>,
) -> Result<String> {
    let identity = pb::PublicationSnapshotIdentity {
        identity_version: STATIC_SNAPSHOT_IDENTITY_VERSION,
        publication_policy_version: PUBLICATION_POLICY_VERSION,
        source_action_set_sha256: source_action_set_sha256
            .map(|value| digest_from_hex(value, "source_action_set_sha256"))
            .transpose()?,
        schema_descriptor_sha256: Some(digest_from_hex(
            &descriptor_sha256(),
            "schema_descriptor_sha256",
        )?),
        dataset_files: normalized_dataset_files(files)?,
    };
    let mut hasher = Sha256::new();
    hasher.update(b"xlsynth-bvc/publication-snapshot/v1\0");
    hasher.update(identity.encode_to_vec());
    Ok(hex::encode(hasher.finalize()))
}

fn encode_static_snapshot_manifest(manifest: &StaticSnapshotManifest) -> Result<Vec<u8>> {
    if manifest.schema_version != STATIC_SNAPSHOT_SCHEMA_VERSION {
        bail!(
            "unsupported static snapshot schema_version={} expected {}",
            manifest.schema_version,
            STATIC_SNAPSHOT_SCHEMA_VERSION
        );
    }
    let expected_id = snapshot_id_for_dataset_files(
        &manifest.dataset_files,
        manifest.source_action_set_sha256.as_deref(),
    )?;
    if manifest.snapshot_id != expected_id {
        bail!(
            "snapshot id mismatch before encoding: manifest={} actual={}",
            manifest.snapshot_id,
            expected_id
        );
    }
    let total = manifest.dataset_files.iter().map(|v| v.bytes).sum::<u64>();
    if manifest.total_dataset_bytes != total {
        bail!(
            "snapshot total bytes mismatch before encoding: manifest={} actual={}",
            manifest.total_dataset_bytes,
            total
        );
    }
    let campaign_ids = manifest
        .campaign_ids
        .iter()
        .map(|value| digest_from_hex(value, "campaign_id"))
        .collect::<Result<Vec<_>>>()?;
    let run_ids = manifest
        .run_ids
        .iter()
        .map(|value| digest_from_hex(value, "run_id"))
        .collect::<Result<Vec<_>>>()?;
    if !manifest
        .campaign_ids
        .windows(2)
        .all(|pair| pair[0] < pair[1])
        || !manifest.run_ids.windows(2).all(|pair| pair[0] < pair[1])
    {
        bail!("snapshot campaign_ids and run_ids must be strictly sorted");
    }
    Ok(pb::PublicationSnapshotManifest {
        record_version: STATIC_SNAPSHOT_SCHEMA_VERSION,
        snapshot_id: Some(digest_from_hex(&manifest.snapshot_id, "snapshot_id")?),
        generated_at: Some(prost_types::Timestamp {
            seconds: manifest.generated_utc.timestamp(),
            nanos: manifest.generated_utc.timestamp_subsec_nanos() as i32,
        }),
        producing_git_commit: manifest.git_commit.clone(),
        source_action_set_sha256: manifest
            .source_action_set_sha256
            .as_deref()
            .map(|value| digest_from_hex(value, "source_action_set_sha256"))
            .transpose()?,
        schema_descriptor_sha256: Some(digest_from_hex(
            &descriptor_sha256(),
            "schema_descriptor_sha256",
        )?),
        publication_policy_version: PUBLICATION_POLICY_VERSION,
        dataset_files: normalized_dataset_files(&manifest.dataset_files)?,
        total_dataset_bytes: manifest.total_dataset_bytes,
        campaign_ids,
        run_ids,
    }
    .encode_to_vec())
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
    let wire = pb::PublicationSnapshotManifest::decode(bytes.as_slice()).with_context(|| {
        format!(
            "decoding protobuf static snapshot manifest: {}",
            manifest_path.display()
        )
    })?;
    if wire.record_version != STATIC_SNAPSHOT_SCHEMA_VERSION {
        bail!(
            "unsupported static snapshot record_version={} (expected {}) at {}",
            wire.record_version,
            STATIC_SNAPSHOT_SCHEMA_VERSION,
            manifest_path.display()
        );
    }
    if wire.publication_policy_version != PUBLICATION_POLICY_VERSION {
        bail!(
            "unsupported publication_policy_version={} (expected {}) at {}",
            wire.publication_policy_version,
            PUBLICATION_POLICY_VERSION,
            manifest_path.display()
        );
    }
    let descriptor = wire
        .schema_descriptor_sha256
        .as_ref()
        .context("snapshot manifest missing schema_descriptor_sha256")?;
    let descriptor = digest_to_hex(descriptor, "schema_descriptor_sha256")?;
    if descriptor != descriptor_sha256() {
        bail!(
            "snapshot schema descriptor mismatch at {}; expected a fresh snapshot for this binary",
            manifest_path.display()
        );
    }
    let timestamp = wire
        .generated_at
        .as_ref()
        .context("snapshot manifest missing generated_at")?;
    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        bail!(
            "snapshot generated_at nanos out of range: {}",
            timestamp.nanos
        );
    }
    let generated_utc = DateTime::from_timestamp(timestamp.seconds, timestamp.nanos as u32)
        .context("snapshot generated_at is outside chrono's supported range")?;
    let dataset_files = wire
        .dataset_files
        .iter()
        .map(dataset_file_from_proto)
        .collect::<Result<Vec<_>>>()?;
    let campaign_ids = wire
        .campaign_ids
        .iter()
        .map(|value| digest_to_hex(value, "campaign_id"))
        .collect::<Result<Vec<_>>>()?;
    let run_ids = wire
        .run_ids
        .iter()
        .map(|value| digest_to_hex(value, "run_id"))
        .collect::<Result<Vec<_>>>()?;
    if !campaign_ids.windows(2).all(|pair| pair[0] < pair[1])
        || !run_ids.windows(2).all(|pair| pair[0] < pair[1])
    {
        bail!("snapshot campaign_ids and run_ids must be strictly sorted");
    }
    let manifest = StaticSnapshotManifest {
        schema_version: wire.record_version,
        snapshot_id: digest_to_hex(
            wire.snapshot_id
                .as_ref()
                .context("snapshot manifest missing snapshot_id")?,
            "snapshot_id",
        )?,
        generated_utc,
        git_commit: wire.producing_git_commit,
        source_action_set_sha256: wire
            .source_action_set_sha256
            .as_ref()
            .map(|value| digest_to_hex(value, "source_action_set_sha256"))
            .transpose()?,
        dataset_files,
        total_dataset_bytes: wire.total_dataset_bytes,
        campaign_ids,
        run_ids,
    };
    let expected_id = snapshot_id_for_dataset_files(
        &manifest.dataset_files,
        manifest.source_action_set_sha256.as_deref(),
    )?;
    if manifest.snapshot_id != expected_id {
        bail!(
            "snapshot id mismatch at {}: manifest={} actual={}",
            manifest_path.display(),
            manifest.snapshot_id,
            expected_id
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
    let mut latest_source_generated_utc = None;
    let direct_heavy_indices_written = !options.skip_rebuild_web_indices;
    if !options.skip_rebuild_web_indices {
        let direct_files = rebuild_snapshot_web_indices(store, repo_root, &options.out_dir)
            .context("rebuilding snapshot web indices before snapshot")?;
        for entry in &direct_files {
            let path = options.out_dir.join(&entry.relpath);
            let bytes = fs::read(&path)
                .with_context(|| format!("reading rebuilt snapshot dataset: {}", path.display()))?;
            observe_json_generated_utc(&mut latest_source_generated_utc, &bytes);
        }
        total_dataset_bytes += direct_files.iter().map(|entry| entry.bytes).sum::<u64>();
        dataset_files.extend(direct_files);
    }

    let mut entries = store
        .list_web_index_entries_with_prefix("")
        .context("listing web index entries for static snapshot")?;
    crate::service::validate_ir_fn_corpus_structural_index_closure(&entries)
        .context("validating structural index closure before static snapshot")?;
    entries.retain(|(index_key, _)| {
        should_copy_snapshot_store_index(index_key, direct_heavy_indices_written)
    });
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    for (index_key, bytes) in entries {
        observe_json_generated_utc(&mut latest_source_generated_utc, &bytes);
        let entry = write_snapshot_dataset_entry(&options.out_dir, &index_key, &bytes)?;
        total_dataset_bytes += entry.bytes;
        dataset_files.push(entry);
    }

    let finalized_runs = list_finalized_campaign_runs(store)?;
    let mut campaign_ids = std::collections::BTreeSet::new();
    let mut run_ids = Vec::with_capacity(finalized_runs.len());
    for run in finalized_runs {
        let public_run = public_run_from_manifest(&run)?;
        validate_public_run(&public_run)?;
        observe_proto_generated_utc(
            &mut latest_source_generated_utc,
            public_run.updated_at.as_ref(),
        );
        let campaign_id = digest_to_hex(
            public_run
                .campaign_id
                .as_ref()
                .context("public run missing campaign_id")?,
            "public_run.campaign_id",
        )?;
        let run_id = digest_to_hex(
            public_run
                .run_id
                .as_ref()
                .context("public run missing run_id")?,
            "public_run.run_id",
        )?;
        campaign_ids.insert(campaign_id);
        run_ids.push(run_id.clone());
        let entry = write_snapshot_run_entry(
            &options.out_dir,
            &run_id,
            "run.pb",
            &public_run.encode_to_vec(),
        )?;
        total_dataset_bytes += entry.bytes;
        dataset_files.push(entry);
        let analysis_path = campaign_analysis_path(
            store,
            run.run_id.as_ref().context("campaign run missing run_id")?,
        )?;
        if analysis_path.exists() {
            let analysis_bytes = fs::read(&analysis_path).with_context(|| {
                format!("reading campaign analysis: {}", analysis_path.display())
            })?;
            let analysis = decode_analysis_report(&analysis_bytes)?;
            observe_proto_generated_utc(
                &mut latest_source_generated_utc,
                analysis.generated_at.as_ref(),
            );
            if analysis.run_id != run.run_id || analysis.campaign_id != run.campaign_id {
                bail!("analysis identity does not match campaign run {run_id}");
            }
            let entry = write_snapshot_run_entry(
                &options.out_dir,
                &run_id,
                "findings.pb",
                &analysis_bytes,
            )?;
            total_dataset_bytes += entry.bytes;
            dataset_files.push(entry);
        }
    }
    run_ids.sort();
    let campaign_ids = campaign_ids.into_iter().collect::<Vec<_>>();

    let source_action_set_sha256 = read_source_action_set_sha256(store);
    let snapshot_id =
        snapshot_id_for_dataset_files(&dataset_files, source_action_set_sha256.as_deref())?;
    let git_commit = read_git_commit(repo_root);
    let generated_utc = latest_source_generated_utc
        .unwrap_or_else(|| DateTime::from_timestamp(0, 0).expect("Unix epoch is representable"));
    let manifest = StaticSnapshotManifest {
        schema_version: STATIC_SNAPSHOT_SCHEMA_VERSION,
        snapshot_id: snapshot_id.clone(),
        generated_utc,
        git_commit,
        source_action_set_sha256,
        dataset_files,
        total_dataset_bytes,
        campaign_ids,
        run_ids,
    };
    let manifest_path = options.out_dir.join(STATIC_SNAPSHOT_MANIFEST_FILENAME);
    fs::write(
        &manifest_path,
        encode_static_snapshot_manifest(&manifest)
            .context("encoding protobuf static snapshot manifest")?,
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
        run_count: manifest.run_ids.len(),
        total_dataset_bytes: manifest.total_dataset_bytes,
        manifest_path: manifest_path.display().to_string(),
    })
}

pub(crate) fn verify_static_snapshot(snapshot_dir: &Path) -> Result<VerifyStaticSnapshotSummary> {
    let manifest = load_static_snapshot_manifest(snapshot_dir)?;

    let mut recomputed_total_bytes = 0_u64;
    let mut declared_relpaths = std::collections::BTreeSet::new();
    let mut decoded_campaign_ids = std::collections::BTreeSet::new();
    let mut decoded_run_ids = std::collections::BTreeSet::new();
    let mut structural_index_entries = Vec::new();
    declared_relpaths.insert(STATIC_SNAPSHOT_MANIFEST_FILENAME.to_string());
    for entry in &manifest.dataset_files {
        let expected_relpath = if entry.index_key.starts_with("runs/") {
            index_key_to_relpath(&entry.index_key)?
                .to_string_lossy()
                .replace('\\', "/")
        } else {
            Path::new(STATIC_SNAPSHOT_WEB_INDEX_DIR)
                .join(index_key_to_relpath(&entry.index_key)?)
                .to_string_lossy()
                .replace('\\', "/")
        };
        if expected_relpath != entry.relpath {
            bail!(
                "snapshot dataset relpath mismatch for key {}: manifest={} expected={}",
                entry.index_key,
                entry.relpath,
                expected_relpath
            );
        }
        if !declared_relpaths.insert(entry.relpath.clone()) {
            bail!("duplicate snapshot dataset relpath: {}", entry.relpath);
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
        if entry.index_key.starts_with("runs/") && entry.index_key.ends_with("/run.pb") {
            let public_run = pb::PublicCampaignRun::decode(bytes.as_slice())
                .with_context(|| format!("decoding public campaign run: {}", entry.relpath))?;
            validate_public_run(&public_run)?;
            let run_id = digest_to_hex(
                public_run
                    .run_id
                    .as_ref()
                    .context("public run missing run_id")?,
                "public_run.run_id",
            )?;
            let campaign_id = digest_to_hex(
                public_run
                    .campaign_id
                    .as_ref()
                    .context("public run missing campaign_id")?,
                "public_run.campaign_id",
            )?;
            if entry.index_key != format!("runs/{run_id}/run.pb") {
                bail!(
                    "public run path does not match encoded run id: {}",
                    entry.index_key
                );
            }
            decoded_campaign_ids.insert(campaign_id);
            decoded_run_ids.insert(run_id);
        } else if entry.index_key.starts_with("runs/") && entry.index_key.ends_with("/findings.pb")
        {
            let report = decode_analysis_report(&bytes)?;
            let run_id = digest_to_hex(
                report
                    .run_id
                    .as_ref()
                    .context("analysis report missing run_id")?,
                "analysis.run_id",
            )?;
            if entry.index_key != format!("runs/{run_id}/findings.pb") {
                bail!(
                    "analysis report path does not match encoded run id: {}",
                    entry.index_key
                );
            }
        } else if entry.index_key.starts_with("runs/") {
            bail!("unknown run publication file: {}", entry.index_key);
        }
        if entry
            .index_key
            .starts_with(WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_NAMESPACE)
        {
            structural_index_entries.push((entry.index_key.clone(), bytes));
        }
        recomputed_total_bytes += actual_bytes;
    }

    crate::service::validate_ir_fn_corpus_structural_index_closure(&structural_index_entries)
        .context("validating structural index closure in static snapshot")?;

    if decoded_campaign_ids.into_iter().collect::<Vec<_>>() != manifest.campaign_ids
        || decoded_run_ids.into_iter().collect::<Vec<_>>() != manifest.run_ids
    {
        bail!("snapshot campaign_ids/run_ids do not exactly match public run files");
    }

    if recomputed_total_bytes != manifest.total_dataset_bytes {
        bail!(
            "snapshot total dataset bytes mismatch: manifest={} actual={}",
            manifest.total_dataset_bytes,
            recomputed_total_bytes
        );
    }

    let recomputed_snapshot_id = snapshot_id_for_dataset_files(
        &manifest.dataset_files,
        manifest.source_action_set_sha256.as_deref(),
    )?;
    if recomputed_snapshot_id != manifest.snapshot_id {
        bail!(
            "snapshot id mismatch: manifest={} actual={}",
            manifest.snapshot_id,
            recomputed_snapshot_id
        );
    }

    for entry in WalkDir::new(snapshot_dir).sort_by_file_name() {
        let entry = entry.context("walking snapshot directory during verification")?;
        if !entry.file_type().is_file() {
            continue;
        }
        let relpath = entry
            .path()
            .strip_prefix(snapshot_dir)
            .context("stripping snapshot root during verification")?
            .to_string_lossy()
            .replace('\\', "/");
        if !declared_relpaths.remove(&relpath) {
            bail!("snapshot contains undeclared file: {relpath}");
        }
    }
    if !declared_relpaths.is_empty() {
        bail!(
            "snapshot manifest declares files that were not found: {:?}",
            declared_relpaths
        );
    }

    Ok(VerifyStaticSnapshotSummary {
        snapshot_dir: snapshot_dir.display().to_string(),
        snapshot_id: manifest.snapshot_id,
        generated_utc: manifest.generated_utc,
        dataset_file_count: manifest.dataset_files.len(),
        run_count: manifest.run_ids.len(),
        total_dataset_bytes: manifest.total_dataset_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_versions_index_bytes() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "schema_version": crate::WEB_VERSIONS_SUMMARY_INDEX_SCHEMA_VERSION,
            "generated_utc": Utc::now(),
            "report": {
                "cards": [],
                "unattributed_actions": []
            }
        }))
        .expect("serialize empty versions index")
    }

    fn empty_ir_corpus_index_bytes() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "schema_version": crate::WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_SCHEMA_VERSION,
            "generated_utc": Utc::now(),
            "dataset": {
                "fraig": false,
                "samples": [],
                "min_ir_nodes": 0,
                "max_ir_nodes": 0,
                "g8r_only_count": 0,
                "yosys_only_count": 0,
                "available_crate_versions": []
            },
            "g8r_points": [],
            "yosys_points": []
        }))
        .expect("serialize empty IR corpus index")
    }

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
            .write_web_index_bytes(
                WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
                &empty_versions_index_bytes(),
            )
            .expect("write web index");
        let structural_manifest = crate::model::IrFnCorpusStructuralManifest {
            schema_version: crate::IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION,
            generated_utc: Utc::now(),
            recompute_missing_hashes: false,
            total_actions_scanned: 0,
            total_driver_ir_to_opt_actions: 0,
            total_ir_fn_to_k_bool_cone_corpus_actions: 0,
            indexed_actions: 0,
            indexed_k_bool_cone_members: 0,
            distinct_structural_hashes: 0,
            hash_from_dependency_hint_count: 0,
            hash_recomputed_count: 0,
            hash_hint_conflict_count: 0,
            skipped_missing_output_count: 0,
            skipped_missing_ir_top_count: 0,
            skipped_missing_hash_hint_count: 0,
            skipped_hash_error_count: 0,
            skipped_k_bool_cone_manifest_errors: 0,
            skipped_k_bool_cone_empty_count: 0,
            source_action_set_sha256: Some("ab".repeat(32)),
            groups: Vec::new(),
        };
        store
            .write_web_index_bytes(
                WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY,
                &serde_json::to_vec_pretty(&structural_manifest)
                    .expect("serialize structural manifest"),
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

        let first_manifest =
            fs::read(out_dir.join(STATIC_SNAPSHOT_MANIFEST_FILENAME)).expect("first manifest");
        let second = build_static_snapshot(
            &store,
            &root,
            &BuildStaticSnapshotOptions {
                out_dir: out_dir.clone(),
                overwrite: true,
                skip_rebuild_web_indices: true,
            },
        )
        .expect("rebuild snapshot");
        assert_eq!(summary.snapshot_id, second.snapshot_id);
        assert_eq!(
            first_manifest,
            fs::read(out_dir.join(STATIC_SNAPSHOT_MANIFEST_FILENAME)).expect("second manifest")
        );
    }

    #[test]
    fn static_snapshot_verify_detects_tamper() {
        let root = make_temp_dir("tamper");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure layout");
        store
            .write_web_index_bytes(
                WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
                &empty_versions_index_bytes(),
            )
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

        let tampered_path = out_dir
            .join("web_index")
            .join(WEB_VERSIONS_SUMMARY_INDEX_FILENAME);
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
                &empty_ir_corpus_index_bytes(),
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
            .write_web_index_bytes(
                WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
                &empty_versions_index_bytes(),
            )
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
            WEB_VERSIONS_SUMMARY_INDEX_FILENAME
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
    fn static_snapshot_rejects_incomplete_structural_index() {
        let root = make_temp_dir("incomplete-structural-index");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure layout");
        let structural_hash = "c".repeat(64);
        let manifest = crate::model::IrFnCorpusStructuralManifest {
            schema_version: crate::IR_FN_CORPUS_STRUCTURAL_INDEX_SCHEMA_VERSION,
            generated_utc: Utc::now(),
            recompute_missing_hashes: false,
            total_actions_scanned: 0,
            total_driver_ir_to_opt_actions: 0,
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
            source_action_set_sha256: Some("ab".repeat(32)),
            groups: vec![crate::model::IrFnCorpusStructuralManifestGroup {
                structural_hash: structural_hash.clone(),
                member_count: 1,
                relpath: crate::service::hash_group_relpath(&structural_hash),
                content_sha256: "0".repeat(64),
                ir_node_count: None,
            }],
        };
        store
            .write_web_index_bytes(
                WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY,
                &serde_json::to_vec_pretty(&manifest).expect("serialize manifest"),
            )
            .expect("write structural manifest");

        let error = build_static_snapshot(
            &store,
            &root,
            &BuildStaticSnapshotOptions {
                out_dir: root.join("snapshot-out"),
                overwrite: false,
                skip_rebuild_web_indices: true,
            },
        )
        .expect_err("incomplete structural index must not be snapshotted");
        assert!(
            format!("{error:#}").contains("structural manifest group is missing"),
            "unexpected error: {error:#}"
        );

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn public_snapshot_index_allowlist_is_fail_closed() {
        assert!(should_include_snapshot_index_key(
            WEB_VERSIONS_SUMMARY_INDEX_FILENAME
        ));
        assert!(should_include_snapshot_index_key(
            WEB_IR_FN_CORPUS_G8R_VS_YOSYS_INDEX_FILENAME
        ));
        assert!(should_include_snapshot_index_key(
            WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_MANIFEST_KEY
        ));
        let hash = "ab".repeat(32);
        let group_key = format!(
            "{WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_NAMESPACE}/by-hash/{}/{}/{hash}.json",
            &hash[0..2],
            &hash[2..4]
        );
        assert!(should_include_snapshot_index_key(&group_key));

        assert!(!should_include_snapshot_index_key(
            "internal-build-metadata.v1.json"
        ));
        assert!(!should_include_snapshot_index_key(
            "stdlib-file-action-graph.v1.json"
        ));
        assert!(!should_include_snapshot_index_key(
            "ir-fn-corpus-g8r-vs-yosys-abc.v3.json/incremental-delta/row.json"
        ));
        assert!(!should_include_snapshot_index_key(&format!(
            "{WEB_IR_FN_CORPUS_STRUCTURAL_INDEX_NAMESPACE}/by-hash/ff/ff/{hash}.json"
        )));
    }

    #[test]
    fn static_snapshot_excludes_unknown_private_index_and_host_path() {
        let root = make_temp_dir("private-index");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure layout");
        store
            .write_web_index_bytes(
                WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
                &empty_versions_index_bytes(),
            )
            .expect("write public web index");
        let private_key = "internal-build-metadata.v1.json";
        let private_bytes = serde_json::to_vec_pretty(&serde_json::json!({
            "store_root": root.display().to_string()
        }))
        .expect("private JSON");
        store
            .write_web_index_bytes(private_key, &private_bytes)
            .expect("write private web index");

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
        .expect("build public snapshot");
        assert_eq!(summary.dataset_file_count, 1);
        assert!(!out_dir.join("web_index").join(private_key).exists());

        let private_path = root.display().to_string();
        for entry in WalkDir::new(&out_dir) {
            let entry = entry.expect("walk snapshot");
            if entry.file_type().is_file()
                && entry.path().extension().and_then(|value| value.to_str()) == Some("json")
            {
                let text = fs::read_to_string(entry.path()).expect("read public JSON");
                assert!(!text.contains(&private_path));
            }
        }
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
