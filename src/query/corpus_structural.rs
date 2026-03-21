use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::time::Instant;

use crate::executor::extract_ir_fn_block_by_name;
use crate::model::{
    IrFnCorpusStructuralGroupFile, IrFnCorpusStructuralManifest, IrFnCorpusStructuralMember,
};
use crate::service::{
    ir_fn_corpus_structural_group_index_key, ir_fn_corpus_structural_manifest_index_key,
    parse_ir_fn_op_count_from_file, parse_ir_function_signature_from_file,
};
use crate::store::ArtifactStore;
use crate::versioning::{cmp_dotted_numeric_version, normalize_tag_version};
use crate::view::StdlibG8rVsYosysDataset;

#[derive(Debug, Clone, Default)]
pub(crate) struct IrFnCorpusStructuralCoverage {
    pub(crate) g8r_loss_sample_count_by_structural_hash: BTreeMap<String, usize>,
    pub(crate) ir_node_count_by_structural_hash: BTreeMap<String, u64>,
}

pub(crate) fn build_ir_fn_corpus_structural_coverage(
    dataset: &StdlibG8rVsYosysDataset,
) -> IrFnCorpusStructuralCoverage {
    let mut coverage = IrFnCorpusStructuralCoverage::default();
    for sample in &dataset.samples {
        let Some(structural_hash) = sample
            .structural_hash
            .as_deref()
            .and_then(super::normalize_structural_hash_hex)
        else {
            continue;
        };
        if sample.g8r_product_loss.is_finite() && sample.g8r_product_loss > 0.0 {
            *coverage
                .g8r_loss_sample_count_by_structural_hash
                .entry(structural_hash.clone())
                .or_insert(0) += 1;
        }
        if sample.ir_node_count > 0 {
            coverage
                .ir_node_count_by_structural_hash
                .entry(structural_hash)
                .or_insert(sample.ir_node_count);
        }
    }
    coverage
}

const WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_INDEX_FILENAME: &str =
    "ir-fn-corpus-structural-export.v1.tar.zst";
const WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_METADATA_FILENAME: &str =
    "ir-fn-corpus-structural-export.v1.meta.json";
const WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_SCHEMA_VERSION: u32 = 1;
const IR_FN_CORPUS_STRUCTURAL_ARCHIVE_ZSTD_LEVEL: i32 = 19;
const IR_FN_CORPUS_STRUCTURAL_ARCHIVE_ROOT_DIR: &str = "ir-fn-corpus-structural";
const IR_FN_CORPUS_STRUCTURAL_ARCHIVE_DOWNLOAD_FILENAME: &str = "ir-fn-corpus-structural.tar.zst";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IrFnCorpusStructuralArchiveCacheMetadata {
    schema_version: u32,
    generated_utc: DateTime<Utc>,
    manifest_fingerprint: String,
    function_count: usize,
    archive_bytes: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct IrFnCorpusStructuralArchivePayload {
    pub(crate) bytes: Vec<u8>,
    pub(crate) cache_hit: bool,
    pub(crate) function_count: usize,
}

pub(crate) fn ir_fn_corpus_structural_archive_download_filename() -> &'static str {
    IR_FN_CORPUS_STRUCTURAL_ARCHIVE_DOWNLOAD_FILENAME
}

fn ir_fn_corpus_structural_manifest_fingerprint(
    manifest: &IrFnCorpusStructuralManifest,
) -> Result<String> {
    let bytes = serde_json::to_vec(manifest).context("serializing structural manifest for hash")?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(hex::encode(hasher.finalize()))
}

fn append_tar_entry<W: std::io::Write>(
    builder: &mut tar::Builder<W>,
    entry_path: &str,
    bytes: &[u8],
) -> Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o644);
    header.set_mtime(0);
    header.set_cksum();
    builder
        .append_data(&mut header, entry_path, Cursor::new(bytes))
        .with_context(|| format!("writing tar entry `{}`", entry_path))
}

fn load_ir_fn_corpus_structural_archive_cache(
    store: &ArtifactStore,
    manifest_fingerprint: &str,
) -> Result<Option<IrFnCorpusStructuralArchivePayload>> {
    let metadata_location =
        store.web_index_location(WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_METADATA_FILENAME);
    let archive_location =
        store.web_index_location(WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_INDEX_FILENAME);
    let Some(metadata_bytes) = store
        .load_web_index_bytes(WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_METADATA_FILENAME)
        .with_context(|| {
            format!(
                "reading IR corpus structural archive metadata: {}",
                metadata_location
            )
        })?
    else {
        return Ok(None);
    };
    let metadata: IrFnCorpusStructuralArchiveCacheMetadata =
        serde_json::from_slice(&metadata_bytes).with_context(|| {
            format!(
                "parsing IR corpus structural archive metadata: {}",
                metadata_location
            )
        })?;
    if metadata.schema_version != WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_SCHEMA_VERSION {
        info!(
            "query structural archive cache metadata schema mismatch location={} expected={} got={}",
            metadata_location,
            WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_SCHEMA_VERSION,
            metadata.schema_version
        );
        return Ok(None);
    }
    if metadata.manifest_fingerprint != manifest_fingerprint {
        info!(
            "query structural archive cache stale location={} expected_manifest={} got_manifest={}",
            metadata_location, manifest_fingerprint, metadata.manifest_fingerprint
        );
        return Ok(None);
    }
    let Some(bytes) = store
        .load_web_index_bytes(WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_INDEX_FILENAME)
        .with_context(|| {
            format!(
                "reading IR corpus structural archive bytes: {}",
                archive_location
            )
        })?
    else {
        return Ok(None);
    };
    info!(
        "query structural archive cache hit location={} bytes={} functions={} generated_utc={}",
        archive_location,
        bytes.len(),
        metadata.function_count,
        metadata.generated_utc
    );
    Ok(Some(IrFnCorpusStructuralArchivePayload {
        bytes,
        cache_hit: true,
        function_count: metadata.function_count,
    }))
}

fn canonical_member_from_group(
    group: &IrFnCorpusStructuralGroupFile,
) -> Option<IrFnCorpusStructuralMember> {
    group
        .members
        .iter()
        .min_by(|a, b| {
            a.opt_ir_action_id
                .cmp(&b.opt_ir_action_id)
                .then(a.ir_top.cmp(&b.ir_top))
                .then(a.source_ir_action_id.cmp(&b.source_ir_action_id))
                .then(a.crate_version.cmp(&b.crate_version))
                .then(a.dso_version.cmp(&b.dso_version))
                .then(a.created_utc.cmp(&b.created_utc))
        })
        .cloned()
}

fn rebuild_ir_fn_corpus_structural_archive_cache(
    store: &ArtifactStore,
    manifest: &IrFnCorpusStructuralManifest,
    manifest_fingerprint: &str,
) -> Result<IrFnCorpusStructuralArchivePayload> {
    let started = Instant::now();
    let mut groups = manifest.groups.clone();
    groups.sort_by(|a, b| a.structural_hash.cmp(&b.structural_hash));

    let mut exported_rows = Vec::with_capacity(groups.len());
    let encoder =
        zstd::stream::write::Encoder::new(Vec::new(), IR_FN_CORPUS_STRUCTURAL_ARCHIVE_ZSTD_LEVEL)
            .context("creating zstd encoder for structural corpus archive")?;
    let mut tar_builder = tar::Builder::new(encoder);

    for manifest_group in groups {
        let structural_hash = manifest_group.structural_hash;
        let group = load_ir_fn_corpus_structural_group(store, &structural_hash)?;
        let Some(member) = canonical_member_from_group(&group) else {
            warn!(
                "query structural archive skipping empty group structural_hash={}",
                structural_hash
            );
            continue;
        };

        let ir_text = fs::read_to_string(store.resolve_artifact_ref_path(&member.output_artifact))
            .with_context(|| {
                format!(
                    "reading canonical member IR for structural_hash={} action_id={} top={}",
                    structural_hash, member.opt_ir_action_id, member.ir_top
                )
            })?;
        let fn_text = extract_ir_fn_block_by_name(&ir_text, &member.ir_top).unwrap_or_else(|err| {
            warn!(
                "query structural archive fallback_to_full_ir structural_hash={} action_id={} top={} error={:#}",
                structural_hash, member.opt_ir_action_id, member.ir_top, err
            );
            ir_text
        });

        let function_relpath = format!(
            "{}/functions/by-hash/{}/{}/{}.ir",
            IR_FN_CORPUS_STRUCTURAL_ARCHIVE_ROOT_DIR,
            &structural_hash[0..2],
            &structural_hash[2..4],
            structural_hash
        );
        append_tar_entry(&mut tar_builder, &function_relpath, fn_text.as_bytes())?;
        exported_rows.push(json!({
            "structural_hash": structural_hash,
            "function_relpath": function_relpath,
            "ir_top": member.ir_top,
            "crate_version": member.crate_version,
            "dso_version": member.dso_version,
            "opt_ir_action_id": member.opt_ir_action_id,
            "source_ir_action_id": member.source_ir_action_id,
            "ir_op_count": member.ir_op_count,
            "ir_fn_signature": member.ir_fn_signature,
            "dslx_origin": member.dslx_origin,
            "producer_action_kind": member.producer_action_kind,
        }));
    }

    let manifest_entry_path = format!("{}/manifest.json", IR_FN_CORPUS_STRUCTURAL_ARCHIVE_ROOT_DIR);
    let manifest_bytes = serde_json::to_vec_pretty(manifest)
        .context("serializing structural manifest for archive")?;
    append_tar_entry(&mut tar_builder, &manifest_entry_path, &manifest_bytes)?;

    let index_entry_path = format!(
        "{}/functions/index.json",
        IR_FN_CORPUS_STRUCTURAL_ARCHIVE_ROOT_DIR
    );
    let index_payload = json!({
        "schema_version": WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_SCHEMA_VERSION,
        "generated_utc": Utc::now(),
        "manifest_generated_utc": manifest.generated_utc,
        "manifest_fingerprint": manifest_fingerprint,
        "function_count": exported_rows.len(),
        "functions": exported_rows,
    });
    let index_bytes = serde_json::to_vec_pretty(&index_payload)
        .context("serializing structural archive index")?;
    append_tar_entry(&mut tar_builder, &index_entry_path, &index_bytes)?;
    tar_builder
        .finish()
        .context("finishing structural corpus tar stream")?;
    let encoder = tar_builder
        .into_inner()
        .context("extracting zstd encoder for structural corpus tar stream")?;
    let archive_bytes = encoder
        .finish()
        .context("finishing structural corpus zstd stream")?;

    let metadata = IrFnCorpusStructuralArchiveCacheMetadata {
        schema_version: WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_SCHEMA_VERSION,
        generated_utc: Utc::now(),
        manifest_fingerprint: manifest_fingerprint.to_string(),
        function_count: exported_rows.len(),
        archive_bytes: archive_bytes.len() as u64,
    };
    let metadata_bytes = serde_json::to_vec_pretty(&metadata)
        .context("serializing structural archive cache metadata")?;
    store
        .write_web_index_bytes(
            WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_INDEX_FILENAME,
            &archive_bytes,
        )
        .with_context(|| {
            format!(
                "writing structural archive bytes: {}",
                store.web_index_location(WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_INDEX_FILENAME)
            )
        })?;
    store
        .write_web_index_bytes(
            WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_METADATA_FILENAME,
            &metadata_bytes,
        )
        .with_context(|| {
            format!(
                "writing structural archive metadata: {}",
                store.web_index_location(WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_METADATA_FILENAME)
            )
        })?;
    info!(
        "query structural archive cache rebuild location={} metadata={} functions={} bytes={} zstd_level={} elapsed_ms={}",
        store.web_index_location(WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_INDEX_FILENAME),
        store.web_index_location(WEB_IR_FN_CORPUS_STRUCTURAL_ARCHIVE_METADATA_FILENAME),
        metadata.function_count,
        metadata.archive_bytes,
        IR_FN_CORPUS_STRUCTURAL_ARCHIVE_ZSTD_LEVEL,
        started.elapsed().as_millis()
    );
    Ok(IrFnCorpusStructuralArchivePayload {
        bytes: archive_bytes,
        cache_hit: false,
        function_count: metadata.function_count,
    })
}

pub(crate) fn ensure_ir_fn_corpus_structural_archive(
    store: &ArtifactStore,
) -> Result<IrFnCorpusStructuralArchivePayload> {
    let manifest = load_ir_fn_corpus_structural_manifest(store)?;
    let manifest_fingerprint = ir_fn_corpus_structural_manifest_fingerprint(&manifest)?;
    if let Some(cached) = load_ir_fn_corpus_structural_archive_cache(store, &manifest_fingerprint)?
    {
        return Ok(cached);
    }
    rebuild_ir_fn_corpus_structural_archive_cache(store, &manifest, &manifest_fingerprint)
}

pub(crate) fn load_ir_fn_corpus_structural_manifest(
    store: &ArtifactStore,
) -> Result<IrFnCorpusStructuralManifest> {
    let key = ir_fn_corpus_structural_manifest_index_key();
    let location = store.web_index_location(key);
    let bytes = store
        .load_web_index_bytes(key)
        .with_context(|| format!("reading IR corpus structural manifest: {}", location))?
        .ok_or_else(|| anyhow!("IR corpus structural manifest missing at {}", location))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing IR corpus structural manifest: {}", location))
}

pub(crate) fn load_ir_fn_corpus_structural_group(
    store: &ArtifactStore,
    structural_hash: &str,
) -> Result<IrFnCorpusStructuralGroupFile> {
    let key = ir_fn_corpus_structural_group_index_key(structural_hash);
    let location = store.web_index_location(&key);
    let bytes = store
        .load_web_index_bytes(&key)
        .with_context(|| {
            format!(
                "reading IR corpus structural group for {}: {}",
                structural_hash, location
            )
        })?
        .ok_or_else(|| {
            anyhow!(
                "IR corpus structural group for {} missing at {}",
                structural_hash,
                location
            )
        })?;
    serde_json::from_slice(&bytes).with_context(|| {
        format!(
            "parsing IR corpus structural group for {}: {}",
            structural_hash, location
        )
    })
}

#[derive(Debug, Clone)]
pub(crate) struct IrFnCorpusStructuralMemberView {
    pub(crate) member: IrFnCorpusStructuralMember,
    pub(crate) fn_signature: Option<String>,
    pub(crate) ir_op_count: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct IrFnCorpusFnLookupView {
    pub(crate) structural_hash: String,
    pub(crate) member: IrFnCorpusStructuralMemberView,
    pub(crate) ir_fn_text: Option<String>,
}

pub(crate) fn infer_ir_fn_signature_for_member(
    store: &ArtifactStore,
    member: &IrFnCorpusStructuralMember,
) -> Option<String> {
    if let Some(signature) = member.ir_fn_signature.as_deref()
        && !signature.trim().is_empty()
    {
        return Some(signature.to_string());
    }
    let path = store.resolve_artifact_ref_path(&member.output_artifact);
    parse_ir_function_signature_from_file(&path, &member.ir_top)
        .ok()
        .flatten()
}

pub(crate) fn infer_ir_op_count_for_member(
    store: &ArtifactStore,
    member: &IrFnCorpusStructuralMember,
) -> Option<u64> {
    if let Some(count) = member.ir_op_count {
        return Some(count);
    }
    let path = store.resolve_artifact_ref_path(&member.output_artifact);
    parse_ir_fn_op_count_from_file(&path, &member.ir_top)
        .ok()
        .flatten()
}

pub(crate) fn build_ir_fn_corpus_structural_member_views(
    store: &ArtifactStore,
    group: &IrFnCorpusStructuralGroupFile,
) -> Vec<IrFnCorpusStructuralMemberView> {
    let mut members = group.members.clone();
    members.sort_by(|a, b| {
        a.opt_ir_action_id
            .cmp(&b.opt_ir_action_id)
            .then(a.ir_top.cmp(&b.ir_top))
    });
    members
        .into_iter()
        .map(|member| IrFnCorpusStructuralMemberView {
            fn_signature: infer_ir_fn_signature_for_member(store, &member),
            ir_op_count: infer_ir_op_count_for_member(store, &member),
            member,
        })
        .collect()
}

pub(crate) fn lookup_ir_fn_corpus_by_fn_name(
    store: &ArtifactStore,
    manifest: &IrFnCorpusStructuralManifest,
    fn_name: &str,
    max_results: usize,
) -> Result<Vec<IrFnCorpusFnLookupView>> {
    let requested_fn_name = fn_name.trim();
    if requested_fn_name.is_empty() || max_results == 0 {
        return Ok(Vec::new());
    }

    let mut matches = Vec::new();
    for manifest_group in &manifest.groups {
        let group = match load_ir_fn_corpus_structural_group(store, &manifest_group.structural_hash)
        {
            Ok(group) => group,
            Err(err) => {
                warn!(
                    "query lookup_ir_fn_corpus_by_fn_name skipping group={} fn_name={} error={:#}",
                    manifest_group.structural_hash, requested_fn_name, err
                );
                continue;
            }
        };
        for member in group.members {
            if member.ir_top.trim() != requested_fn_name {
                continue;
            }
            let ir_fn_text =
                fs::read_to_string(store.resolve_artifact_ref_path(&member.output_artifact))
                    .ok()
                    .and_then(|ir_text| {
                        extract_ir_fn_block_by_name(&ir_text, requested_fn_name).ok()
                    });
            let row = IrFnCorpusStructuralMemberView {
                fn_signature: infer_ir_fn_signature_for_member(store, &member),
                ir_op_count: infer_ir_op_count_for_member(store, &member),
                member,
            };
            matches.push(IrFnCorpusFnLookupView {
                structural_hash: group.structural_hash.clone(),
                member: row,
                ir_fn_text,
            });
            if matches.len() >= max_results {
                break;
            }
        }
        if matches.len() >= max_results {
            break;
        }
    }

    matches.sort_by(|a, b| {
        cmp_dotted_numeric_version(
            normalize_tag_version(&a.member.member.crate_version),
            normalize_tag_version(&b.member.member.crate_version),
        )
        .reverse()
        .then(
            cmp_dotted_numeric_version(
                normalize_tag_version(&a.member.member.dso_version),
                normalize_tag_version(&b.member.member.dso_version),
            )
            .reverse(),
        )
        .then(
            b.member
                .member
                .created_utc
                .cmp(&a.member.member.created_utc),
        )
        .then(
            a.member
                .member
                .opt_ir_action_id
                .cmp(&b.member.member.opt_ir_action_id),
        )
        .then(a.structural_hash.cmp(&b.structural_hash))
    });
    Ok(matches)
}
