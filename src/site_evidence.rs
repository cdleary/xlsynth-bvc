// SPDX-License-Identifier: Apache-2.0

//! Read retained protobuf publication evidence independently of web projections.
//! Old HTML, JavaScript, and JSON need not exist: only consumed evidence files
//! are read, bounded, and checked against their canonical publication manifest.

use super::*;

pub(super) const MAX_EVIDENCE_BYTES: u64 = 25 * 1024 * 1024;

pub(super) struct Source {
    root: PathBuf,
    manifest: pb::StaticSiteManifest,
    files: BTreeMap<String, pb::PublicationFile>,
}

fn read(root: &Path, relative: &str, limit: u64) -> Result<Vec<u8>> {
    normalized_relpath(relative)?;
    let mut path = root.to_path_buf();
    if !fs::symlink_metadata(&path)?.file_type().is_dir() {
        bail!("publication evidence root must be a directory");
    }
    let parts = Path::new(relative).components().collect::<Vec<_>>();
    for (index, part) in parts.iter().enumerate() {
        path.push(part.as_os_str());
        let metadata = fs::symlink_metadata(&path)?;
        if index + 1 == parts.len() {
            if !metadata.file_type().is_file() || metadata.len() > limit {
                bail!("publication evidence file is not regular or exceeds its size bound");
            }
        } else if !metadata.file_type().is_dir() {
            bail!("publication evidence path contains a symlink or special directory");
        }
    }
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        bail!("publication evidence file exceeds its size bound");
    }
    Ok(bytes)
}

impl Source {
    pub(super) fn open(root: &Path) -> Result<Self> {
        let bytes = read(root, STATIC_SITE_MANIFEST_FILENAME, MAX_EVIDENCE_BYTES)?;
        let manifest = pb::StaticSiteManifest::decode(bytes.as_slice())?;
        if manifest.encode_to_vec() != bytes {
            bail!("publication evidence manifest is not canonical protobuf");
        }
        Self::from_manifest(root, manifest)
    }

    pub(super) fn from_manifest(root: &Path, manifest: pb::StaticSiteManifest) -> Result<Self> {
        if manifest.record_version != STATIC_SITE_RECORD_VERSION
            || normalize_base_url(&manifest.base_url)? != manifest.base_url
        {
            bail!("invalid publication evidence manifest");
        }
        digest_hex(
            &manifest.source_snapshot_id,
            "publication evidence identity",
        )?;
        let mut files = BTreeMap::new();
        for file in &manifest.files {
            let relative = file
                .relpath
                .as_ref()
                .context("publication evidence file has no path")?
                .value
                .clone();
            normalized_relpath(&relative)?;
            if file.logical_key != relative || file.media_type != media_type(&relative) {
                bail!("invalid publication evidence file reference");
            }
            digest_hex(&file.sha256, "publication evidence file checksum")?;
            if files.insert(relative, file.clone()).is_some() {
                bail!("duplicate publication evidence file reference");
            }
        }
        Ok(Self {
            root: root.into(),
            manifest,
            files,
        })
    }

    pub(super) fn manifest(&self) -> &pb::StaticSiteManifest {
        &self.manifest
    }

    pub(super) fn paths(&self) -> impl Iterator<Item = &str> {
        self.files.keys().map(String::as_str)
    }

    pub(super) fn read(&self, relative: &str, limit: u64) -> Result<Vec<u8>> {
        let record = self
            .files
            .get(relative)
            .context("missing publication evidence file")?;
        self.read_record(record, limit)
    }

    pub(super) fn read_record(&self, record: &pb::PublicationFile, limit: u64) -> Result<Vec<u8>> {
        let relative = &record
            .relpath
            .as_ref()
            .context("evidence file has no path")?
            .value;
        if self.files.get(relative) != Some(record) || record.bytes > limit {
            bail!(
                "publication evidence file is not bound by its site manifest or exceeds its size bound"
            );
        }
        let bytes = read(&self.root, relative, limit)?;
        if bytes.len() as u64 != record.bytes
            || sha256_hex(&bytes) != digest_hex(&record.sha256, "publication evidence checksum")?
        {
            bail!("publication evidence file differs from its recorded bytes");
        }
        Ok(bytes)
    }
}

#[cfg(test)]
pub(super) fn retain_protobuf_files(root: &Path) {
    for entry in WalkDir::new(root) {
        let entry = entry.unwrap();
        if entry.file_type().is_file() && entry.path().extension().is_none_or(|ext| ext != "pb") {
            fs::remove_file(entry.path()).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "bvc-publication-evidence-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            fs::create_dir_all(root.join("data")).unwrap();
            fs::write(root.join("data/evidence.pb"), b"retained protobuf bytes").unwrap();
            fs::write(root.join("index.html"), b"earlier renderer").unwrap();
            let manifest = pb::StaticSiteManifest {
                record_version: STATIC_SITE_RECORD_VERSION,
                source_snapshot_id: Some(pb::Sha256Digest { value: vec![7; 32] }),
                base_url: "/previous/".into(),
                files: ["data/evidence.pb", "index.html"]
                    .iter()
                    .map(|path| publication_file(&root, path).unwrap())
                    .collect(),
            };
            fs::write(
                root.join(STATIC_SITE_MANIFEST_FILENAME),
                manifest.encode_to_vec(),
            )
            .unwrap();
            Self(root)
        }

        fn manifest(&self) -> pb::StaticSiteManifest {
            pb::StaticSiteManifest::decode(
                fs::read(self.0.join(STATIC_SITE_MANIFEST_FILENAME))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn retained_evidence_ignores_web_files_but_checks_consumed_bytes_and_bounds() {
        let fixture = Fixture::new();
        fs::remove_file(fixture.0.join("index.html")).unwrap();
        let source = Source::open(&fixture.0).unwrap();
        let expected = b"retained protobuf bytes";
        assert_eq!(
            source
                .read("data/evidence.pb", expected.len() as u64)
                .unwrap(),
            expected
        );
        assert!(
            source
                .read("data/evidence.pb", expected.len() as u64 - 1)
                .is_err()
        );
        let mut unbound = source.files["data/evidence.pb"].clone();
        unbound.bytes += 1;
        assert!(source.read_record(&unbound, MAX_EVIDENCE_BYTES).is_err());
        fs::write(fixture.0.join("data/evidence.pb"), vec![0; expected.len()]).unwrap();
        assert!(source.read("data/evidence.pb", MAX_EVIDENCE_BYTES).is_err());
        // A small declared size cannot authorize reading a larger file.
        fs::write(
            fixture.0.join("data/evidence.pb"),
            vec![0; expected.len() + 1],
        )
        .unwrap();
        assert!(
            source
                .read("data/evidence.pb", expected.len() as u64)
                .is_err()
        );
    }

    #[test]
    fn retained_manifest_rejects_unsafe_paths_duplicate_records_and_noncanonical_bytes() {
        let fixture = Fixture::new();
        let original = fixture.manifest();
        for path in [
            "../outside.pb",
            "/absolute.pb",
            "data/../evidence.pb",
            "data/./evidence.pb",
        ] {
            let mut manifest = original.clone();
            manifest.files[0].logical_key = path.into();
            manifest.files[0].relpath.as_mut().unwrap().value = path.into();
            assert!(Source::from_manifest(&fixture.0, manifest).is_err());
        }
        let mut duplicate = original.clone();
        duplicate.files.push(duplicate.files[0].clone());
        assert!(Source::from_manifest(&fixture.0, duplicate).is_err());
        let mut checksum = original.clone();
        checksum.files[0].sha256.as_mut().unwrap().value.pop();
        assert!(Source::from_manifest(&fixture.0, checksum).is_err());
        let mut bytes = original.encode_to_vec();
        bytes.extend([0xf8, 0x07, 0x01]); // Unknown field 127 must not survive canonical decoding.
        fs::write(fixture.0.join(STATIC_SITE_MANIFEST_FILENAME), bytes).unwrap();
        assert!(Source::open(&fixture.0).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn retained_evidence_rejects_symlink_roots_files_and_intermediate_directories() {
        use std::os::unix::fs::symlink;
        let fixture = Fixture::new();
        let root_link = fixture.0.with_extension("link");
        symlink(&fixture.0, &root_link).unwrap();
        assert!(Source::open(&root_link).is_err());
        fs::remove_file(root_link).unwrap();
        let source = Source::open(&fixture.0).unwrap();
        let data = fixture.0.join("data");
        fs::rename(data.join("evidence.pb"), fixture.0.join("retained.pb")).unwrap();
        symlink("../retained.pb", data.join("evidence.pb")).unwrap();
        assert!(source.read("data/evidence.pb", MAX_EVIDENCE_BYTES).is_err());
        fs::remove_file(data.join("evidence.pb")).unwrap();
        fs::rename(fixture.0.join("retained.pb"), data.join("evidence.pb")).unwrap();
        fs::rename(&data, fixture.0.join("retained-data")).unwrap();
        symlink("retained-data", &data).unwrap();
        assert!(source.read("data/evidence.pb", MAX_EVIDENCE_BYTES).is_err());
    }
}
