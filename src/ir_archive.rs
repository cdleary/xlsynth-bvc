// SPDX-License-Identifier: Apache-2.0

//! Fixed, content-addressed IR download packs and verified local root imports.

use anyhow::{Context, Result, bail};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub(crate) mod wire {
    include!(concat!(env!("OUT_DIR"), "/xlsynth.bvc.ir_archive.v1.rs"));
}

pub(crate) const INDEX: &str = "ir/index.pb";
const MAX_ASSET_BYTES: u64 = 25 * 1024 * 1024;
const MAX_EXPANDED_BUCKET_BYTES: u64 = 256 * 1024 * 1024;
const MAX_TOTAL_ROOT_BYTES: u64 = 1024 * 1024 * 1024;
static IMPORT_NONCE: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
pub(crate) struct Builder {
    blobs: BTreeMap<Vec<u8>, Vec<u8>>,
    roots: BTreeMap<(String, String), wire::IrArchiveRoot>,
}

fn digest(bytes: &[u8]) -> Vec<u8> {
    Sha256::digest(bytes).to_vec()
}

fn relative(value: &str) -> Result<()> {
    if value.is_empty()
        || value
            .chars()
            .any(|c| c.is_control() || c == '\\' || c == ':')
        || value.split('/').any(|p| matches!(p, "" | "." | ".."))
    {
        bail!("IR archive contains a nonportable relative path");
    }
    Ok(())
}

fn root_key(root: &wire::IrArchiveRoot) -> (String, String) {
    (root.corpus.clone(), root.source_relpath.clone())
}

fn validate_root(root: &wire::IrArchiveRoot) -> Result<()> {
    if root.corpus.is_empty()
        || !root
            .corpus
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
        || root.source_sha256.len() != 32
        || root.top_fn_name.is_empty()
        || !root.source_relpath.ends_with(".ir")
    {
        bail!("invalid IR archive root identity");
    }
    relative(&root.source_relpath)
}

pub(crate) fn archive_path(prefix: &str) -> String {
    format!("ir/{prefix}.tar.zst")
}

fn tar_bytes(blobs: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>) -> Result<Vec<u8>> {
    let mut tar = tar::Builder::new(Vec::new());
    for (sha, bytes) in blobs {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_cksum();
        tar.append_data(
            &mut header,
            format!("{}.ir", hex::encode(sha)),
            bytes.as_slice(),
        )?;
    }
    tar.finish()?;
    Ok(tar.into_inner()?)
}

impl Builder {
    pub(crate) fn add(&mut self, root: wire::IrArchiveRoot, ir: &[u8]) -> Result<()> {
        validate_root(&root)?;
        if ir.is_empty() || ir.len() as u64 > MAX_ASSET_BYTES || digest(ir) != root.source_sha256 {
            bail!("IR archive root does not match its exact-byte SHA-256 or size bound");
        }
        let key = root_key(&root);
        if let Some(prior) = self.roots.get(&key) {
            if prior != &root || self.blobs.get(&root.source_sha256).map(Vec::as_slice) != Some(ir)
            {
                bail!("conflicting IR archive roots at the same corpus path");
            }
            return Ok(());
        }
        let text = std::str::from_utf8(ir).context("IR archive root is not UTF-8")?;
        if crate::service::parse_ir_fn_node_count_by_name(text, &root.top_fn_name).is_none() {
            bail!("IR archive root does not declare its selected top function");
        }
        if let Some(prior) = self.blobs.get(&root.source_sha256) {
            if prior != ir {
                bail!("conflicting IR bytes for the same SHA-256");
            }
        } else {
            self.blobs.insert(root.source_sha256.clone(), ir.to_vec());
        }
        self.roots.insert(key, root);
        Ok(())
    }

    fn packs(&self) -> Result<(wire::IrArchiveIndex, Vec<Vec<u8>>)> {
        let mut index = wire::IrArchiveIndex {
            record_version: 1,
            buckets: Vec::with_capacity(256),
            roots: self.roots.values().cloned().collect(),
        };
        let mut packs = Vec::with_capacity(256);
        for ordinal in 0..=255u8 {
            let blobs = self.blobs.iter().filter(|(sha, _)| sha[0] == ordinal);
            let records = blobs
                .clone()
                .map(|(sha, ir)| wire::IrArchiveBlob {
                    sha256: sha.clone(),
                    bytes: ir.len() as u64,
                })
                .collect::<Vec<_>>();
            let raw = tar_bytes(blobs.map(|(sha, ir)| (sha.clone(), ir.clone())))?;
            if raw.len() as u64 > MAX_EXPANDED_BUCKET_BYTES {
                bail!("IR bucket {ordinal:02x} exceeds the 256 MiB expanded-size bound");
            }
            let bytes = zstd::stream::encode_all(raw.as_slice(), 3)?;
            if bytes.len() as u64 > MAX_ASSET_BYTES {
                bail!("IR archive {ordinal:02x}.tar.zst exceeds the 25 MiB hosting limit");
            }
            index.buckets.push(wire::IrArchiveBucket {
                prefix: format!("{ordinal:02x}"),
                archive_bytes: bytes.len() as u64,
                archive_sha256: digest(&bytes),
                blobs: records,
            });
            packs.push(bytes);
        }
        validate_index(&index)?;
        Ok((index, packs))
    }

    pub(crate) fn write(&self, site: &Path) -> Result<()> {
        let (index, packs) = self.packs()?;
        fs::create_dir_all(site.join("ir"))?;
        fs::write(site.join(INDEX), index.encode_to_vec())?;
        for (bucket, bytes) in index.buckets.iter().zip(packs) {
            fs::write(site.join(archive_path(&bucket.prefix)), bytes)?;
        }
        Ok(())
    }

    pub(crate) fn verify(&self, site: &Path) -> Result<BTreeSet<String>> {
        let (expected, packs) = self.packs()?;
        if read_local(site, INDEX, MAX_ASSET_BYTES)? != expected.encode_to_vec() {
            bail!("IR archive index differs from the site's verified IR roots");
        }
        let mut paths = BTreeSet::from([INDEX.into()]);
        for (bucket, bytes) in expected.buckets.iter().zip(packs) {
            let path = archive_path(&bucket.prefix);
            if read_local(site, &path, MAX_ASSET_BYTES)? != bytes {
                bail!("IR archive {path} differs from the site's verified IR roots");
            }
            paths.insert(path);
        }
        Ok(paths)
    }

    pub(crate) fn extend_from_site(&mut self, site: &Path) -> Result<()> {
        let source = Source::Local(site.to_path_buf());
        let (index, blobs, _) = read_bundle(&source)?;
        for root in index.roots {
            self.add(root.clone(), &blobs[&root.source_sha256])?;
        }
        Ok(())
    }

    /// Additional bytes may come from the download itself only after their
    /// complete root identities are independently bound by progression evidence.
    pub(crate) fn verify_including(
        &self,
        site: &Path,
        additional: &[wire::IrArchiveRoot],
    ) -> Result<BTreeSet<String>> {
        let (index, blobs, _) = read_bundle(&Source::Local(site.into()))?;
        let mut expected = self.roots.clone();
        for root in additional {
            validate_root(root)?;
            if expected
                .insert(root_key(root), root.clone())
                .is_some_and(|prior| prior != *root)
            {
                bail!("conflicting expected IR archive root identities");
            }
        }
        if expected.into_values().collect::<Vec<_>>() != index.roots {
            bail!("IR archive roots differ from the full-corpus and progression evidence");
        }
        let mut verified = Builder::default();
        for root in index.roots {
            verified.add(root.clone(), &blobs[&root.source_sha256])?;
        }
        verified.verify(site)
    }
}

fn validate_index(index: &wire::IrArchiveIndex) -> Result<()> {
    if index.record_version != 1
        || index.buckets.len() != 256
        || index.encoded_len() as u64 > MAX_ASSET_BYTES
    {
        bail!("invalid IR archive index version, bucket count, or size");
    }
    let mut blobs = BTreeMap::new();
    for (ordinal, bucket) in index.buckets.iter().enumerate() {
        if bucket.prefix != format!("{ordinal:02x}")
            || bucket.archive_sha256.len() != 32
            || bucket.archive_bytes == 0
            || bucket.archive_bytes > MAX_ASSET_BYTES
        {
            bail!("invalid IR archive bucket identity or hosting size");
        }
        let mut previous = None;
        let mut expanded = 1024u64;
        for blob in &bucket.blobs {
            if blob.sha256.len() != 32
                || blob.sha256[0] as usize != ordinal
                || blob.bytes == 0
                || blob.bytes > MAX_ASSET_BYTES
                || previous.is_some_and(|p: &[u8]| p >= blob.sha256.as_slice())
                || blobs.insert(blob.sha256.clone(), blob.bytes).is_some()
            {
                bail!("invalid, misplaced, duplicate, or unsorted IR archive blob");
            }
            expanded = expanded
                .checked_add(512 + blob.bytes.div_ceil(512) * 512)
                .context("IR archive expanded size overflow")?;
            previous = Some(blob.sha256.as_slice());
        }
        if expanded > MAX_EXPANDED_BUCKET_BYTES {
            bail!("IR archive bucket exceeds the expanded-size bound");
        }
    }
    let mut previous = None;
    let mut referenced = BTreeSet::new();
    let mut root_paths = BTreeSet::new();
    let mut total_root_bytes = 0u64;
    for root in &index.roots {
        validate_root(root)?;
        let key = root_key(root);
        if previous.as_ref().is_some_and(|p| p >= &key) || !blobs.contains_key(&root.source_sha256)
        {
            bail!("duplicate, unsorted, or dangling IR archive root");
        }
        let mut ancestor = String::new();
        for part in root.source_relpath.split('/') {
            if root_paths.contains(&(root.corpus.clone(), ancestor.clone())) {
                bail!("IR archive root paths overlap as file and directory");
            }
            if !ancestor.is_empty() {
                ancestor.push('/');
            }
            ancestor.push_str(part);
        }
        root_paths.insert(key.clone());
        total_root_bytes = total_root_bytes
            .checked_add(blobs[&root.source_sha256])
            .context("IR archive root byte count overflow")?;
        if total_root_bytes > MAX_TOTAL_ROOT_BYTES {
            bail!("IR archive roots exceed the 1 GiB expanded-output bound");
        }
        referenced.insert(root.source_sha256.clone());
        previous = Some(key);
    }
    if referenced != blobs.into_keys().collect() {
        bail!("IR archive contains unreferenced blobs");
    }
    Ok(())
}

fn read_local(root: &Path, relative_path: &str, limit: u64) -> Result<Vec<u8>> {
    relative(relative_path)?;
    let mut path = root.to_path_buf();
    if !fs::symlink_metadata(&path)?.file_type().is_dir() {
        bail!("IR archive source must be a real directory");
    }
    for part in relative_path.split('/') {
        path.push(part);
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() || (!metadata.is_dir() && !metadata.is_file()) {
            bail!("IR archive source contains a symlink or special file");
        }
    }
    if !fs::symlink_metadata(&path)?.is_file() || fs::metadata(&path)?.len() > limit {
        bail!("IR archive source is not a bounded regular file");
    }
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        bail!("IR archive source exceeds its size bound");
    }
    Ok(bytes)
}

enum Source {
    Local(PathBuf),
    Remote(reqwest::blocking::Client, reqwest::Url),
}

impl Source {
    fn read(&self, path: &str, limit: u64) -> Result<Vec<u8>> {
        match self {
            Self::Local(root) => read_local(root, path, limit),
            Self::Remote(client, base) => {
                let response = client.get(base.join(path)?).send()?.error_for_status()?;
                if response.content_length().is_some_and(|n| n > limit) {
                    bail!("IR archive download exceeds its size bound");
                }
                let mut bytes = Vec::new();
                response.take(limit + 1).read_to_end(&mut bytes)?;
                if bytes.len() as u64 > limit {
                    bail!("IR archive download exceeds its size bound");
                }
                Ok(bytes)
            }
        }
    }
}

type BlobMap = BTreeMap<Vec<u8>, Vec<u8>>;
type Bundle = (wire::IrArchiveIndex, BlobMap, Vec<Vec<u8>>);

fn read_bundle(source: &Source) -> Result<Bundle> {
    let bytes = source.read(INDEX, MAX_ASSET_BYTES)?;
    let index = wire::IrArchiveIndex::decode(bytes.as_slice())?;
    if index.encode_to_vec() != bytes {
        bail!("IR archive index is not canonical protobuf");
    }
    validate_index(&index)?;
    let mut blobs = BTreeMap::new();
    let mut packs = Vec::with_capacity(256);
    for bucket in &index.buckets {
        let bytes = source.read(&archive_path(&bucket.prefix), bucket.archive_bytes)?;
        if bytes.len() as u64 != bucket.archive_bytes || digest(&bytes) != bucket.archive_sha256 {
            bail!("IR archive {} checksum or length mismatch", bucket.prefix);
        }
        let bound = 1024
            + bucket
                .blobs
                .iter()
                .map(|b| 512 + b.bytes.div_ceil(512) * 512)
                .sum::<u64>();
        let decoder = zstd::stream::read::Decoder::new(bytes.as_slice())?;
        let mut raw = Vec::new();
        decoder.take(bound + 1).read_to_end(&mut raw)?;
        if raw.len() as u64 > bound {
            bail!("IR archive exceeds its declared expanded size");
        }
        let mut tar = tar::Archive::new(Cursor::new(&raw));
        let mut entries = tar.entries()?;
        let mut bucket_blobs = Vec::new();
        for blob in &bucket.blobs {
            let mut entry = entries
                .next()
                .context("IR archive is missing a declared blob")??;
            if !entry.header().entry_type().is_file()
                || entry.path()?.as_ref() != Path::new(&format!("{}.ir", hex::encode(&blob.sha256)))
                || entry.size() != blob.bytes
            {
                bail!("IR archive contains a link, unexpected path, type, order, or size");
            }
            let mut ir = Vec::new();
            entry.read_to_end(&mut ir)?;
            if ir.len() as u64 != blob.bytes || digest(&ir) != blob.sha256 {
                bail!("IR archive blob checksum mismatch");
            }
            blobs.insert(blob.sha256.clone(), ir.clone());
            bucket_blobs.push((blob.sha256.clone(), ir));
        }
        if entries.next().is_some() || tar_bytes(bucket_blobs)? != raw {
            bail!("IR archive contains extra entries or noncanonical tar metadata");
        }
        packs.push(bytes);
    }
    // Check selected functions as well as byte identity before touching a store.
    let mut checked = Builder::default();
    for root in &index.roots {
        checked.add(root.clone(), &blobs[&root.source_sha256])?;
    }
    Ok((index, blobs, packs))
}

#[derive(Serialize)]
pub(crate) struct ImportSummary {
    output_dir: String,
    roots: usize,
    unique_ir_blobs: usize,
    imported_actions: usize,
    already_cached_actions: usize,
}

/// Operator-facing projection of the retained protobuf index, never a source
/// for cache identity or publication verification.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportedRoots {
    schema_version: u32,
    roots: Vec<ImportedRoot>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportedRoot {
    corpus: String,
    source_relpath: String,
    top_fn_name: String,
    import_action_id: String,
}

/// Only this newly-created staging directory is ever removed on failure.
struct Staging(PathBuf);
impl Drop for Staging {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub(crate) fn import(
    store: &crate::store::ArtifactStore,
    site_dir: Option<&Path>,
    url: Option<&str>,
    output: &Path,
    protected: &[(&str, &Path)],
) -> Result<ImportSummary> {
    let source = match (site_dir, url) {
        (Some(root), None) => {
            crate::site::reject_site_output_overlap(output, root, protected)?;
            Source::Local(root.to_path_buf())
        }
        (None, Some(url)) => {
            let mut base = reqwest::Url::parse(url)?;
            if !matches!(base.scheme(), "http" | "https")
                || !base.username().is_empty()
                || base.password().is_some()
                || base.query().is_some()
                || base.fragment().is_some()
            {
                bail!(
                    "provide an HTTP(S) immutable site URL without credentials, query, or fragment"
                );
            }
            if !base.path().ends_with('/') {
                base.set_path(&format!("{}/", base.path()));
            }
            let client = reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(60))
                .connect_timeout(Duration::from_secs(10))
                .redirect(reqwest::redirect::Policy::none())
                .build()?;
            for (_, root) in protected {
                crate::site::reject_site_output_overlap(output, root, &[])?;
            }
            Source::Remote(client, base)
        }
        _ => bail!("provide exactly one of --site-dir or --url"),
    };
    if output.try_exists()? {
        bail!("IR import output already exists; choose a new directory");
    }
    let (index, blobs, packs) = read_bundle(&source)?;
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let stage_path = parent.join(format!(
        ".bvc-ir-import-{}-{}",
        std::process::id(),
        IMPORT_NONCE.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&stage_path)?;
    let stage = Staging(stage_path);
    fs::create_dir(stage.0.join("ir"))?;
    fs::write(stage.0.join(INDEX), index.encode_to_vec())?;
    for (bucket, bytes) in index.buckets.iter().zip(packs) {
        fs::write(stage.0.join(archive_path(&bucket.prefix)), bytes)?;
    }
    let mut actions = BTreeSet::new();
    let mut imported = 0;
    let mut cached = 0;
    let mut imported_roots = Vec::new();
    for root in &index.roots {
        let path = stage
            .0
            .join("inputs")
            .join(&root.corpus)
            .join(&root.source_relpath);
        fs::create_dir_all(path.parent().context("IR root parent")?)?;
        fs::write(&path, &blobs[&root.source_sha256])?;
        let action = crate::model::ActionSpec::ImportIrPackageFile {
            source_sha256: hex::encode(&root.source_sha256),
            top_fn_name: Some(root.top_fn_name.clone()),
        };
        let id = crate::executor::compute_action_id(&action)?;
        imported_roots.push(ImportedRoot {
            corpus: root.corpus.clone(),
            source_relpath: root.source_relpath.clone(),
            top_fn_name: root.top_fn_name.clone(),
            import_action_id: id.clone(),
        });
        if actions.insert(id.clone()) {
            let already_cached = store.action_exists(&id);
            crate::corpus::import_ir_root(
                store,
                &path,
                &hex::encode(&root.source_sha256),
                &root.top_fn_name,
            )?;
            if already_cached {
                cached += 1;
            } else {
                imported += 1;
            }
        }
    }
    fs::write(
        stage.0.join("roots.json"),
        serde_json::to_vec_pretty(&ImportedRoots {
            schema_version: 1,
            roots: imported_roots,
        })?,
    )?;
    // No output is exposed until all downloads, roots, and canonical imports pass.
    crate::site::atomic_install_site_directory(&stage.0, output)?;
    Ok(ImportSummary {
        output_dir: output.display().to_string(),
        roots: index.roots.len(),
        unique_ir_blobs: blobs.len(),
        imported_actions: imported,
        already_cached_actions: cached,
    })
}

#[cfg(test)]
mod tests;
