// SPDX-License-Identifier: Apache-2.0

//! Read a sealed publication boundary, never a memoized view of mutable runs.
//! Only protobuf evidence is consumed; current browser projections are regenerated
//! and the assembled site receives the normal semantic and integrity verification.

use super::*;

pub(super) struct Source {
    root: PathBuf,
    files: BTreeMap<String, pb::PublicationFile>,
    pub(super) site: wire::CorpusSite,
}

// Bound reads and reject symlinks/special files, including intermediate directories.
// Evidence inputs need not retain old HTML, JavaScript, or JSON projections.
fn read(root: &Path, relative: &str, limit: u64) -> Result<Vec<u8>> {
    normalized_relpath(relative)?;
    let mut path = root.to_path_buf();
    if !fs::symlink_metadata(&path)?.file_type().is_dir() {
        bail!("corpus evidence root must be a directory");
    }
    let parts = Path::new(relative).components().collect::<Vec<_>>();
    for (index, part) in parts.iter().enumerate() {
        path.push(part.as_os_str());
        let metadata = fs::symlink_metadata(&path)?;
        if index + 1 == parts.len() {
            if !metadata.file_type().is_file() || metadata.len() > limit {
                bail!("corpus evidence file is not regular or exceeds its size bound");
            }
        } else if !metadata.file_type().is_dir() {
            bail!("corpus evidence path contains a symlink or special directory");
        }
    }
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        bail!("corpus evidence file exceeds its size bound");
    }
    Ok(bytes)
}

fn checked_file(
    root: &Path,
    file: &pb::PublicationFile,
    relative: &str,
    limit: u64,
) -> Result<Vec<u8>> {
    if file.logical_key != relative
        || file.relpath.as_ref().map(|p| p.value.as_str()) != Some(relative)
        || file.media_type != media_type(relative)
        || file.bytes > limit
    {
        bail!("invalid corpus evidence file reference");
    }
    let bytes = read(root, relative, limit)?;
    if bytes.len() as u64 != file.bytes
        || sha256_hex(&bytes) != digest_hex(&file.sha256, "corpus evidence checksum")?
    {
        bail!("corpus evidence file does not match its recorded bytes");
    }
    Ok(bytes)
}

impl Source {
    pub(super) fn open(root: &Path) -> Result<Self> {
        let bytes = read(root, STATIC_SITE_MANIFEST_FILENAME, MAX_ASSET_BYTES)?;
        let manifest = pb::StaticSiteManifest::decode(bytes.as_slice())?;
        if manifest.encode_to_vec() != bytes
            || manifest.record_version != STATIC_SITE_RECORD_VERSION
        {
            bail!("invalid corpus evidence site manifest");
        }
        let mut files = BTreeMap::new();
        for file in &manifest.files {
            let relative = required(&file.relpath, "file path")?.value.clone();
            normalized_relpath(&relative)?;
            if files.insert(relative, file.clone()).is_some() {
                bail!("duplicate corpus evidence file reference");
            }
        }
        let bytes = checked_file(
            root,
            files
                .get(MANIFEST)
                .context("missing corpus evidence index")?,
            MANIFEST,
            MAX_ASSET_BYTES,
        )?;
        if sha256_hex(&bytes)
            != digest_hex(&manifest.source_snapshot_id, "corpus evidence identity")?
        {
            bail!("corpus evidence index identity mismatch");
        }
        let site = wire::CorpusSite::decode(bytes.as_slice())?;
        if site.encode_to_vec() != bytes
            || site.record_version != 1
            || site.generations.is_empty()
            || site.shard_target_bytes == 0
            || site.shard_target_bytes > MAX_SHARD_TARGET as u64
        {
            bail!("invalid corpus evidence index");
        }
        for generation in &site.generations {
            canonical_version(&generation.crate_version)?;
            canonical_version(&generation.dso_version)?;
            if generation.sample_count == 0
                || generation.shards.is_empty()
                || generation.cohort_sha256.len() != 32
                || generation.cohort_sha256 != site.generations[0].cohort_sha256
            {
                bail!("incomplete or inconsistent corpus evidence generation");
            }
        }
        if !site
            .generations
            .windows(2)
            .all(|g| cmp_dotted_numeric_version(&g[0].crate_version, &g[1].crate_version).is_lt())
        {
            bail!("corpus evidence generations must be unique and sorted");
        }
        Ok(Self {
            root: root.into(),
            files,
            site,
        })
    }

    pub(super) fn samples(&self, generation: &wire::Generation) -> Result<Vec<wire::Sample>> {
        let mut samples = Vec::new();
        for (ordinal, shard) in generation.shards.iter().enumerate() {
            let relative = format!("data/{}/{ordinal}.pb", generation.crate_version);
            let record = required(&shard.evidence, "shard evidence")?;
            if self.files.get(&relative) != Some(record) {
                bail!("corpus evidence shard is not bound by its site manifest");
            }
            let bytes = checked_file(&self.root, record, &relative, self.site.shard_target_bytes)?;
            let evidence = wire::EvidenceShard::decode(bytes.as_slice())?;
            if evidence.encode_to_vec() != bytes
                || evidence.samples.is_empty()
                || evidence.samples.len() as u64 != shard.sample_count
            {
                bail!("invalid corpus evidence shard");
            }
            samples.extend(evidence.samples);
        }
        if samples.len() as u64 != generation.sample_count
            || !samples.windows(2).all(|s| s[0].sample_id < s[1].sample_id)
            || cohort_identity(
                samples
                    .iter()
                    .map(|s| required(&s.input, "input").cloned())
                    .collect::<Result<Vec<_>>>()?,
            )? != generation.cohort_sha256
        {
            bail!("corpus evidence does not cover its declared input population");
        }
        // Action-graph, metric, and common-runtime checks occur in the normal
        // staged-site verifier before atomic installation, just as for raw runs.
        Ok(samples)
    }
}
