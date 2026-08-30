// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use fs2::FileExt;
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::path::{Component, Path};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use walkdir::WalkDir;

use crate::proto::v1 as pb;
use crate::proto::{timestamp_from_proto, timestamp_to_proto};
use crate::site::{STATIC_SITE_MANIFEST_FILENAME, verify_static_site};

const PUBLISHED_SITE_IDENTITY_VERSION: u32 = 1;
const PUBLISHED_SITE_RECORD_VERSION: u32 = 1;
const CURRENT_SITE_RECORD_VERSION: u32 = 1;
const PUBLISHED_SITE_ID_DOMAIN: &[u8] = b"xlsynth-bvc/published-site/v1\0";
const CURRENT_POINTER_PROTO: &str = "current.pb";
const CURRENT_POINTER_JSON: &str = "current.json";
pub(crate) const PUBLICATION_LOCK_FILENAME: &str = ".publication.lock";
const PUBLISHED_ROOT_INDEX_HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>xlsynth-bvc results</title></head>
<body><p id="status">Loading current xlsynth-bvc results…</p><script>
fetch('current.json',{cache:'no-store'}).then(response=>{if(!response.ok)throw new Error(`current.json ${response.status}`);return response.json()}).then(current=>{if(current.schema_version!==1||!/^sites\/[0-9a-f]{64}\/$/.test(current.site_url))throw new Error('invalid current site pointer');window.location.replace(current.site_url)}).catch(error=>{document.getElementById('status').textContent=`Unable to load current results: ${error.message}`});
</script></body></html>
"#;
static WRITE_NONCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PublishStaticSiteSummary {
    pub(crate) publish_root: String,
    pub(crate) site_id: String,
    pub(crate) snapshot_id: String,
    pub(crate) site_relpath: String,
    pub(crate) catalog_relpath: String,
    pub(crate) reused_immutable_site: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VerifyPublishedSiteSummary {
    pub(crate) publish_root: String,
    pub(crate) site_id: String,
    pub(crate) snapshot_id: String,
    pub(crate) site_relpath: String,
    pub(crate) catalog_relpath: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct BrowserCurrentPointer {
    schema_version: u32,
    site_id: String,
    catalog_url: String,
    site_url: String,
}

struct PublicationLock {
    file: File,
}

impl PublicationLock {
    fn acquire(publish_root: &Path) -> Result<Self> {
        fs::create_dir_all(publish_root)
            .with_context(|| format!("creating publish root: {}", publish_root.display()))?;
        let path = publish_root.join(PUBLICATION_LOCK_FILENAME);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("opening publication lock: {}", path.display()))?;
        file.try_lock_exclusive().with_context(|| {
            format!(
                "another xlsynth-bvc publisher holds the publication-root lock {}",
                path.display()
            )
        })?;
        Ok(Self { file })
    }
}

impl Drop for PublicationLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn digest(bytes: &[u8]) -> pb::Sha256Digest {
    pb::Sha256Digest {
        value: Sha256::digest(bytes).to_vec(),
    }
}

fn digest_hex(value: &pb::Sha256Digest, field: &str) -> Result<String> {
    if value.value.len() != 32 {
        bail!("{field} must contain exactly 32 bytes");
    }
    Ok(hex::encode(&value.value))
}

fn required<'a, T>(value: &'a Option<T>, field: &str) -> Result<&'a T> {
    value
        .as_ref()
        .with_context(|| format!("missing required protobuf field {field}"))
}

fn decode_canonical_protobuf<M>(bytes: &[u8], label: &str) -> Result<M>
where
    M: Message + Default,
{
    let message = M::decode(bytes).with_context(|| format!("decoding {label}"))?;
    if message.encode_to_vec() != bytes {
        bail!("{label} is not canonically encoded");
    }
    Ok(message)
}

fn encode_browser_current_pointer(pointer: &BrowserCurrentPointer) -> Result<Vec<u8>> {
    serde_json::to_vec_pretty(pointer).context("serializing canonical browser current-site pointer")
}

fn normalized_relpath(value: &str, field: &str) -> Result<String> {
    if value.is_empty() || value.starts_with('/') || value.contains('\\') {
        bail!("{field} must be a normalized relative path");
    }
    let mut parts = Vec::new();
    for component in Path::new(value).components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().to_string()),
            _ => bail!("{field} must be a normalized relative path"),
        }
    }
    let normalized = parts.join("/");
    if normalized != value {
        bail!("{field} must be a normalized relative path");
    }
    Ok(normalized)
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("atomic write path has no parent"))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("creating publication parent: {}", parent.display()))?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let nonce = WRITE_NONCE.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("publication");
    let temp = parent.join(format!(
        ".{name}.tmp-{}-{timestamp}-{nonce}",
        std::process::id()
    ));
    fs::write(&temp, bytes)
        .with_context(|| format!("writing publication temp file: {}", temp.display()))?;
    fs::rename(&temp, path).with_context(|| {
        format!(
            "atomically promoting publication file: {} -> {}",
            temp.display(),
            path.display()
        )
    })
}

fn copy_site(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)
        .with_context(|| format!("creating staged site: {}", destination.display()))?;
    for entry in WalkDir::new(source).sort_by_file_name() {
        let entry = entry.with_context(|| format!("walking source site: {}", source.display()))?;
        let relpath = entry
            .path()
            .strip_prefix(source)
            .context("stripping source site root")?;
        if relpath.as_os_str().is_empty() {
            continue;
        }
        let target = destination.join(relpath);
        if entry.file_type().is_symlink() {
            bail!(
                "static site publication refuses symlink: {}",
                entry.path().display()
            );
        } else if entry.file_type().is_dir() {
            fs::create_dir_all(&target)
                .with_context(|| format!("creating staged directory: {}", target.display()))?;
        } else if entry.file_type().is_file() {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(entry.path(), &target).with_context(|| {
                format!(
                    "copying immutable site file: {} -> {}",
                    entry.path().display(),
                    target.display()
                )
            })?;
        }
    }
    Ok(())
}

fn remove_abandoned_site_staging(staging_root: &Path, site_id: &str) -> Result<usize> {
    if !staging_root.exists() {
        return Ok(0);
    }
    let prefix = format!("site-{site_id}-");
    let mut removed = 0;
    for entry in fs::read_dir(staging_root)
        .with_context(|| format!("reading publication staging: {}", staging_root.display()))?
    {
        let entry = entry.context("reading publication staging entry")?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if !name.starts_with(&prefix) {
            continue;
        }
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("reading staging entry type: {}", path.display()))?;
        if file_type.is_dir() {
            fs::remove_dir_all(&path).with_context(|| {
                format!("removing abandoned publication staging: {}", path.display())
            })?;
        } else {
            fs::remove_file(&path).with_context(|| {
                format!(
                    "removing abandoned publication staging entry: {}",
                    path.display()
                )
            })?;
        }
        removed += 1;
    }
    Ok(removed)
}

fn site_identity(site_dir: &Path) -> Result<(pb::StaticSiteManifest, Vec<u8>, pb::Sha256Digest)> {
    verify_static_site(site_dir)?;
    let manifest_path = site_dir.join(STATIC_SITE_MANIFEST_FILENAME);
    let bytes = fs::read(&manifest_path)
        .with_context(|| format!("reading site manifest: {}", manifest_path.display()))?;
    let manifest = pb::StaticSiteManifest::decode(bytes.as_slice())
        .context("decoding static site manifest")?;
    let manifest_sha = digest(&bytes);
    let identity = pb::PublishedSiteIdentity {
        identity_version: PUBLISHED_SITE_IDENTITY_VERSION,
        source_snapshot_id: manifest.source_snapshot_id.clone(),
        site_manifest_sha256: Some(manifest_sha),
    };
    let mut hasher = Sha256::new();
    hasher.update(PUBLISHED_SITE_ID_DOMAIN);
    hasher.update(identity.encode_to_vec());
    Ok((
        manifest,
        bytes,
        pb::Sha256Digest {
            value: hasher.finalize().to_vec(),
        },
    ))
}

fn validate_catalog(catalog: &pb::PublishedSiteCatalog) -> Result<()> {
    if catalog.record_version != PUBLISHED_SITE_RECORD_VERSION {
        bail!(
            "unsupported published site catalog version {}",
            catalog.record_version
        );
    }
    for (value, field) in [
        (&catalog.site_id, "catalog.site_id"),
        (&catalog.source_snapshot_id, "catalog.source_snapshot_id"),
        (
            &catalog.site_manifest_sha256,
            "catalog.site_manifest_sha256",
        ),
    ] {
        digest_hex(required(value, field)?, field)?;
    }
    if catalog.base_url.is_empty() || !catalog.base_url.starts_with('/') {
        bail!("catalog.base_url must begin with '/'");
    }
    normalized_relpath(
        &required(&catalog.site_relpath, "catalog.site_relpath")?.value,
        "catalog.site_relpath",
    )?;
    timestamp_from_proto(&catalog.published_at, "catalog.published_at")?;
    Ok(())
}

fn validate_pointer(pointer: &pb::CurrentSitePointer) -> Result<()> {
    if pointer.record_version != CURRENT_SITE_RECORD_VERSION {
        bail!(
            "unsupported current site pointer version {}",
            pointer.record_version
        );
    }
    digest_hex(
        required(&pointer.site_id, "current.site_id")?,
        "current.site_id",
    )?;
    normalized_relpath(
        &required(&pointer.catalog_relpath, "current.catalog_relpath")?.value,
        "current.catalog_relpath",
    )?;
    timestamp_from_proto(&pointer.updated_at, "current.updated_at")?;
    Ok(())
}

fn normalized_absolute_path(path: &Path) -> Result<std::path::PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("getting current directory for publication path validation")?
            .join(path)
    };
    let mut existing = absolute.as_path();
    while !existing.exists() {
        existing = existing.parent().with_context(|| {
            format!(
                "publication path has no existing ancestor: {}",
                absolute.display()
            )
        })?;
    }
    let mut resolved = fs::canonicalize(existing)
        .with_context(|| format!("canonicalizing publication path: {}", existing.display()))?;
    for component in absolute
        .strip_prefix(existing)
        .context("resolving publication path suffix")?
        .components()
    {
        match component {
            Component::Normal(part) => resolved.push(part),
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            Component::RootDir | Component::Prefix(_) => {
                bail!("unexpected absolute component in publication path suffix")
            }
        }
    }
    Ok(resolved)
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn reject_publication_root_overlap(
    site_dir: &Path,
    publish_root: &Path,
    protected_roots: &[(&str, &Path)],
) -> Result<()> {
    let publication = normalized_absolute_path(publish_root)?;
    let source_site = normalized_absolute_path(site_dir)?;
    if paths_overlap(&publication, &source_site) {
        bail!(
            "publication root must not overlap the source site: publication={} source_site={}",
            publication.display(),
            source_site.display()
        );
    }
    for (label, root) in protected_roots {
        let protected = normalized_absolute_path(root)?;
        if paths_overlap(&publication, &protected) {
            bail!(
                "publication root must not overlap {label}: publication={} protected={}",
                publication.display(),
                protected.display()
            );
        }
    }
    Ok(())
}

fn validate_publication_top_level(publish_root: &Path) -> Result<()> {
    if !publish_root.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(publish_root)
        .with_context(|| format!("reading publication root: {}", publish_root.display()))?
    {
        let entry = entry.context("reading publication root entry")?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let file_type = entry.file_type().with_context(|| {
            format!("reading publication entry type: {}", entry.path().display())
        })?;
        let valid_type = match name.as_ref() {
            ".staging" | "sites" | "catalogs" => file_type.is_dir(),
            PUBLICATION_LOCK_FILENAME
            | "index.html"
            | CURRENT_POINTER_PROTO
            | CURRENT_POINTER_JSON => file_type.is_file(),
            _ => false,
        };
        if !valid_type {
            bail!(
                "publication root contains an unexpected top-level entry: {}",
                entry.path().display()
            );
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn publish_static_site(
    site_dir: &Path,
    publish_root: &Path,
) -> Result<PublishStaticSiteSummary> {
    publish_static_site_with_protected_roots(site_dir, publish_root, &[])
}

pub(crate) fn publish_static_site_with_protected_roots(
    site_dir: &Path,
    publish_root: &Path,
    protected_roots: &[(&str, &Path)],
) -> Result<PublishStaticSiteSummary> {
    reject_publication_root_overlap(site_dir, publish_root, protected_roots)?;
    validate_publication_top_level(publish_root)?;
    let _lock = PublicationLock::acquire(publish_root)?;
    validate_publication_top_level(publish_root)?;
    let (site_manifest, manifest_bytes, site_id_digest) = site_identity(site_dir)?;
    let site_id = digest_hex(&site_id_digest, "site_id")?;
    let snapshot_id = digest_hex(
        required(&site_manifest.source_snapshot_id, "site.source_snapshot_id")?,
        "site.source_snapshot_id",
    )?;
    atomic_write(
        &publish_root.join("index.html"),
        PUBLISHED_ROOT_INDEX_HTML.as_bytes(),
    )?;
    let site_relpath = format!("sites/{site_id}");
    let target = publish_root.join(&site_relpath);
    let staging_root = publish_root.join(".staging");
    fs::create_dir_all(&staging_root)?;
    remove_abandoned_site_staging(&staging_root, &site_id)?;
    let reused_immutable_site = if target.exists() {
        let (_, existing_manifest, existing_site_id) = site_identity(&target)?;
        if existing_manifest != manifest_bytes || existing_site_id != site_id_digest {
            bail!(
                "immutable published site id collision at {}",
                target.display()
            );
        }
        true
    } else {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let nonce = WRITE_NONCE.fetch_add(1, Ordering::Relaxed);
        let staged = staging_root.join(format!(
            "site-{site_id}-{}-{timestamp}-{nonce}",
            std::process::id()
        ));
        copy_site(site_dir, &staged)?;
        let (_, staged_manifest, staged_site_id) = site_identity(&staged)?;
        if staged_manifest != manifest_bytes || staged_site_id != site_id_digest {
            bail!("staged site verification changed site identity");
        }
        fs::create_dir_all(target.parent().expect("site target has parent"))?;
        fs::rename(&staged, &target).with_context(|| {
            format!(
                "promoting immutable site directory: {} -> {}",
                staged.display(),
                target.display()
            )
        })?;
        false
    };

    let catalog_relpath = format!("catalogs/{site_id}.pb");
    let catalog_path = publish_root.join(&catalog_relpath);
    let mut catalog = pb::PublishedSiteCatalog {
        record_version: PUBLISHED_SITE_RECORD_VERSION,
        site_id: Some(site_id_digest.clone()),
        source_snapshot_id: site_manifest.source_snapshot_id.clone(),
        site_manifest_sha256: Some(digest(&manifest_bytes)),
        base_url: site_manifest.base_url.clone(),
        site_relpath: Some(pb::NormalizedRelpath {
            value: site_relpath.clone(),
        }),
        published_at: Some(timestamp_to_proto(&Utc::now())),
    };
    if catalog_path.exists() {
        let catalog_bytes = fs::read(&catalog_path)
            .with_context(|| format!("reading immutable catalog: {}", catalog_path.display()))?;
        let existing: pb::PublishedSiteCatalog = decode_canonical_protobuf(
            &catalog_bytes,
            &format!("immutable catalog {}", catalog_path.display()),
        )?;
        validate_catalog(&existing)?;
        catalog.published_at = existing.published_at.clone();
        if existing != catalog {
            bail!(
                "immutable published catalog conflicts with site identity at {}",
                catalog_path.display()
            );
        }
    } else {
        validate_catalog(&catalog)?;
        atomic_write(&catalog_path, &catalog.encode_to_vec())?;
    }
    let pointer_path = publish_root.join(CURRENT_POINTER_PROTO);
    let mut pointer = pb::CurrentSitePointer {
        record_version: CURRENT_SITE_RECORD_VERSION,
        site_id: Some(site_id_digest),
        catalog_relpath: Some(pb::NormalizedRelpath {
            value: catalog_relpath.clone(),
        }),
        updated_at: Some(timestamp_to_proto(&Utc::now())),
    };
    if pointer_path.exists() {
        let existing_bytes = fs::read(&pointer_path).with_context(|| {
            format!(
                "reading current protobuf pointer: {}",
                pointer_path.display()
            )
        })?;
        let existing: pb::CurrentSitePointer = decode_canonical_protobuf(
            &existing_bytes,
            &format!("current protobuf pointer {}", pointer_path.display()),
        )?;
        validate_pointer(&existing)?;
        if existing.site_id == pointer.site_id
            && existing.catalog_relpath == pointer.catalog_relpath
        {
            pointer.updated_at = existing.updated_at;
        }
    }
    validate_pointer(&pointer)?;
    atomic_write(&pointer_path, &pointer.encode_to_vec())?;
    let browser_pointer = BrowserCurrentPointer {
        schema_version: 1,
        site_id: site_id.clone(),
        catalog_url: catalog_relpath.clone(),
        site_url: format!("{site_relpath}/"),
    };
    atomic_write(
        &publish_root.join(CURRENT_POINTER_JSON),
        &encode_browser_current_pointer(&browser_pointer)?,
    )?;
    verify_published_site(publish_root)?;
    Ok(PublishStaticSiteSummary {
        publish_root: publish_root.display().to_string(),
        site_id,
        snapshot_id,
        site_relpath,
        catalog_relpath,
        reused_immutable_site,
    })
}

pub(crate) fn verify_published_site(publish_root: &Path) -> Result<VerifyPublishedSiteSummary> {
    validate_publication_top_level(publish_root)?;
    let landing =
        fs::read(publish_root.join("index.html")).context("reading published root index")?;
    if landing != PUBLISHED_ROOT_INDEX_HTML.as_bytes() {
        bail!("published root index does not match the current-site loader");
    }

    let pointer_bytes = fs::read(publish_root.join(CURRENT_POINTER_PROTO))
        .context("reading current protobuf pointer")?;
    let pointer: pb::CurrentSitePointer =
        decode_canonical_protobuf(&pointer_bytes, "current protobuf pointer")?;
    validate_pointer(&pointer)?;
    let site_id = digest_hex(
        required(&pointer.site_id, "current.site_id")?,
        "current.site_id",
    )?;
    let catalog_relpath = required(&pointer.catalog_relpath, "current.catalog_relpath")?
        .value
        .clone();
    let expected_catalog_relpath = format!("catalogs/{site_id}.pb");
    if catalog_relpath != expected_catalog_relpath {
        bail!(
            "current catalog path is not canonical for site id: expected {}",
            expected_catalog_relpath
        );
    }
    let catalog_bytes = fs::read(publish_root.join(&catalog_relpath))
        .context("reading current published catalog")?;
    let catalog: pb::PublishedSiteCatalog =
        decode_canonical_protobuf(&catalog_bytes, "current published catalog")?;
    validate_catalog(&catalog)?;
    if catalog.site_id != pointer.site_id {
        bail!("current pointer site id disagrees with catalog");
    }
    let site_relpath = required(&catalog.site_relpath, "catalog.site_relpath")?
        .value
        .clone();
    let expected_site_relpath = format!("sites/{site_id}");
    if site_relpath != expected_site_relpath {
        bail!(
            "published site path is not canonical for site id: expected {}",
            expected_site_relpath
        );
    }
    let site_dir = publish_root.join(&site_relpath);
    let site_summary = verify_static_site(&site_dir)?;
    let (site_manifest, manifest_bytes, actual_site_id) = site_identity(&site_dir)?;
    if actual_site_id != *required(&catalog.site_id, "catalog.site_id")?
        || digest(&manifest_bytes)
            != *required(
                &catalog.site_manifest_sha256,
                "catalog.site_manifest_sha256",
            )?
    {
        bail!("published site content does not match catalog identity");
    }
    if catalog.source_snapshot_id != site_manifest.source_snapshot_id
        || catalog.base_url != site_manifest.base_url
    {
        bail!("published catalog metadata disagrees with site manifest");
    }
    let browser_bytes = fs::read(publish_root.join(CURRENT_POINTER_JSON))
        .context("reading browser current pointer")?;
    let browser: BrowserCurrentPointer =
        serde_json::from_slice(&browser_bytes).context("decoding browser current pointer")?;
    let expected_browser = BrowserCurrentPointer {
        schema_version: 1,
        site_id: site_id.clone(),
        catalog_url: catalog_relpath.clone(),
        site_url: format!("{site_relpath}/"),
    };
    if browser != expected_browser {
        bail!("browser current pointer disagrees with protobuf pointer/catalog");
    }
    if browser_bytes != encode_browser_current_pointer(&expected_browser)? {
        bail!("browser current pointer is not canonically encoded");
    }
    Ok(VerifyPublishedSiteSummary {
        publish_root: publish_root.display().to_string(),
        site_id,
        snapshot_id: site_summary.snapshot_id,
        site_relpath,
        catalog_relpath,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::site::{BuildStaticSiteOptions, STATIC_SITE_MANIFEST_FILENAME, build_static_site};
    use crate::snapshot::{
        BuildStaticSnapshotOptions, STATIC_SNAPSHOT_MANIFEST_FILENAME, build_static_snapshot,
    };
    use crate::store::ArtifactStore;
    use std::path::PathBuf;

    fn temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "xlsynth-bvc-publish-{label}-{}-{nanos}",
            std::process::id()
        ))
    }

    fn empty_versions_index_bytes() -> &'static [u8] {
        br#"{"schema_version":4,"generated_utc":"2026-08-29T12:00:00Z","report":{"cards":[],"unattributed_actions":[]}}"#
    }

    #[test]
    fn publication_rejects_source_and_protected_root_overlap_before_mutation() {
        let root = temp_path("overlap");
        let site_dir = root.join("site");
        fs::create_dir_all(&site_dir).expect("create source site");

        for publish_root in [&site_dir, &site_dir.join("published"), &root] {
            let error = publish_static_site(&site_dir, publish_root)
                .expect_err("source-site overlap must fail");
            assert!(
                error
                    .to_string()
                    .contains("must not overlap the source site")
            );
            assert!(!publish_root.join(PUBLICATION_LOCK_FILENAME).exists());
        }

        let protected = root.join("private-store");
        fs::create_dir_all(&protected).expect("create protected root");
        let publish_root = protected.join("published");
        let error = publish_static_site_with_protected_roots(
            &site_dir,
            &publish_root,
            &[("private store", protected.as_path())],
        )
        .expect_err("protected-root overlap must fail");
        assert!(error.to_string().contains("must not overlap private store"));
        assert!(!publish_root.exists());

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn publication_rejects_unknown_top_level_entries_before_locking() {
        let root = temp_path("top-level-closure");
        let site_dir = root.join("site");
        let publish_root = root.join("published");
        fs::create_dir_all(&site_dir).expect("create source site");
        fs::create_dir_all(&publish_root).expect("create publication root");
        fs::write(publish_root.join("private.db"), b"private")
            .expect("seed unknown publication entry");

        let error = publish_static_site(&site_dir, &publish_root)
            .expect_err("unknown top-level entry must fail");
        assert!(error.to_string().contains("unexpected top-level entry"));
        assert!(!publish_root.join(PUBLICATION_LOCK_FILENAME).exists());
        let error = verify_published_site(&publish_root)
            .expect_err("verification must reject unknown top-level entry");
        assert!(error.to_string().contains("unexpected top-level entry"));

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn publication_is_verified_and_idempotent() {
        let root = temp_path("roundtrip");
        let store = ArtifactStore::new(root.join("store"));
        store.ensure_layout().expect("layout");
        store
            .write_web_index_bytes(
                crate::WEB_VERSIONS_SUMMARY_INDEX_FILENAME,
                empty_versions_index_bytes(),
            )
            .expect("write index");
        let snapshot_dir = root.join("snapshot");
        let repo_root = std::env::current_dir().expect("current directory");
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
        let site_dir = root.join("site");
        build_static_site(&BuildStaticSiteOptions {
            snapshot_dir: snapshot_dir.clone(),
            out_dir: site_dir.clone(),
            base_url: "/xlsynth-bvc/".to_string(),
            overwrite: false,
        })
        .expect("site");
        let publish_root = root.join("published");
        let held_lock = PublicationLock::acquire(&publish_root).expect("hold publication lock");
        let error = publish_static_site(&site_dir, &publish_root)
            .expect_err("overlapping publication must fail");
        assert!(
            error
                .to_string()
                .contains("another xlsynth-bvc publisher holds")
        );
        assert!(!publish_root.join("index.html").exists());
        drop(held_lock);

        let (_, _, site_id_digest) = site_identity(&site_dir).expect("site identity");
        let site_id = digest_hex(&site_id_digest, "site id").expect("site id hex");
        let abandoned = publish_root
            .join(".staging")
            .join(format!("site-{site_id}-{}", std::process::id()));
        fs::create_dir_all(abandoned.join("partial")).expect("seed abandoned staging");
        fs::write(abandoned.join("partial/index.html"), "partial")
            .expect("write abandoned staging");

        let first = publish_static_site(&site_dir, &publish_root).expect("first publish");
        assert!(!first.reused_immutable_site);
        assert!(!abandoned.exists());
        let catalog_path = publish_root.join(&first.catalog_relpath);
        let first_catalog = fs::read(&catalog_path).expect("first catalog");
        let first_pointer =
            fs::read(publish_root.join(CURRENT_POINTER_PROTO)).expect("first current pointer");
        let first_browser_pointer =
            fs::read(publish_root.join(CURRENT_POINTER_JSON)).expect("first browser pointer");

        let first_snapshot_manifest =
            fs::read(snapshot_dir.join(STATIC_SNAPSHOT_MANIFEST_FILENAME))
                .expect("first snapshot manifest");
        let first_site_manifest =
            fs::read(site_dir.join(STATIC_SITE_MANIFEST_FILENAME)).expect("first site manifest");
        build_static_snapshot(
            &store,
            &repo_root,
            &BuildStaticSnapshotOptions {
                out_dir: snapshot_dir.clone(),
                overwrite: true,
                skip_rebuild_web_indices: true,
            },
        )
        .expect("rebuild snapshot");
        assert_eq!(
            fs::read(snapshot_dir.join(STATIC_SNAPSHOT_MANIFEST_FILENAME))
                .expect("rebuilt snapshot manifest"),
            first_snapshot_manifest
        );
        build_static_site(&BuildStaticSiteOptions {
            snapshot_dir: snapshot_dir.clone(),
            out_dir: site_dir.clone(),
            base_url: "/xlsynth-bvc/".to_string(),
            overwrite: true,
        })
        .expect("rebuild site");
        assert_eq!(
            fs::read(site_dir.join(STATIC_SITE_MANIFEST_FILENAME)).expect("rebuilt site manifest"),
            first_site_manifest
        );

        let second = publish_static_site(&site_dir, &publish_root).expect("second publish");
        assert!(second.reused_immutable_site);
        assert_eq!(first.site_id, second.site_id);
        assert_eq!(
            fs::read(publish_root.join(CURRENT_POINTER_PROTO)).expect("second current pointer"),
            first_pointer,
            "republishing the current site must preserve protobuf pointer bytes"
        );
        assert_eq!(
            fs::read(&catalog_path).expect("second catalog"),
            first_catalog,
            "republishing an immutable site must preserve catalog bytes"
        );
        let verified = verify_published_site(&publish_root).expect("verify publication");
        assert_eq!(verified.site_id, first.site_id);
        drop(store);
        let browser: BrowserCurrentPointer = serde_json::from_slice(
            &fs::read(publish_root.join(CURRENT_POINTER_JSON)).expect("read browser pointer"),
        )
        .expect("decode browser pointer");
        assert_eq!(browser.site_url, format!("{}/", first.site_relpath));
        let landing = fs::read_to_string(publish_root.join("index.html")).expect("landing");
        assert!(landing.contains("current.json"));
        let immutable_index =
            fs::read_to_string(publish_root.join(&first.site_relpath).join("index.html"))
                .expect("immutable index");
        assert!(immutable_index.contains("name=\"bvc-site-root\" content=\"./\""));
        assert!(!immutable_index.contains("/xlsynth-bvc/assets/"));

        let mut catalog_with_unknown_field = first_catalog.clone();
        catalog_with_unknown_field.extend_from_slice(&[0xfa, 0x07, 0x01, b'x']);
        fs::write(&catalog_path, &catalog_with_unknown_field).expect("write catalog unknown field");
        let error = verify_published_site(&publish_root)
            .expect_err("catalog with unknown wire field must fail");
        assert!(error.to_string().contains("not canonically encoded"));
        let error = publish_static_site(&site_dir, &publish_root)
            .expect_err("republish must reject existing catalog unknown wire field");
        assert!(error.to_string().contains("not canonically encoded"));
        fs::write(&catalog_path, &first_catalog).expect("restore canonical catalog");

        let pointer_path = publish_root.join(CURRENT_POINTER_PROTO);
        let mut pointer_with_unknown_field = first_pointer.clone();
        pointer_with_unknown_field.extend_from_slice(&[0xfa, 0x07, 0x01, b'x']);
        fs::write(&pointer_path, &pointer_with_unknown_field).expect("write pointer unknown field");
        let error = verify_published_site(&publish_root)
            .expect_err("pointer with unknown wire field must fail");
        assert!(error.to_string().contains("not canonically encoded"));
        fs::write(&pointer_path, &first_pointer).expect("restore canonical pointer");

        let browser_pointer_path = publish_root.join(CURRENT_POINTER_JSON);
        let mut browser_value: serde_json::Value =
            serde_json::from_slice(&first_browser_pointer).expect("decode browser pointer value");
        browser_value
            .as_object_mut()
            .expect("browser pointer object")
            .insert("private_path".to_string(), "/srv/build/private".into());
        fs::write(
            &browser_pointer_path,
            serde_json::to_vec_pretty(&browser_value).expect("encode browser pointer value"),
        )
        .expect("write browser pointer unknown field");
        let error = verify_published_site(&publish_root)
            .expect_err("browser pointer unknown field must fail");
        assert!(
            format!("{error:#}").contains("unknown field `private_path`"),
            "unexpected error: {error:#}"
        );
        fs::write(&browser_pointer_path, &first_browser_pointer)
            .expect("restore canonical browser pointer");

        let mut mutated_catalog =
            pb::PublishedSiteCatalog::decode(first_catalog.as_slice()).expect("decode catalog");
        mutated_catalog.source_snapshot_id = Some(digest(b"wrong snapshot"));
        fs::write(&catalog_path, mutated_catalog.encode_to_vec())
            .expect("write wrong snapshot catalog");
        let error = verify_published_site(&publish_root)
            .expect_err("catalog snapshot metadata mismatch must fail");
        assert!(
            error
                .to_string()
                .contains("catalog metadata disagrees with site manifest")
        );

        let mut mutated_catalog =
            pb::PublishedSiteCatalog::decode(first_catalog.as_slice()).expect("decode catalog");
        mutated_catalog.site_relpath = Some(pb::NormalizedRelpath {
            value: format!("alternate/{}", first.site_id),
        });
        fs::write(&catalog_path, mutated_catalog.encode_to_vec())
            .expect("write noncanonical site path catalog");
        let error = verify_published_site(&publish_root)
            .expect_err("noncanonical catalog site path must fail");
        assert!(
            error
                .to_string()
                .contains("published site path is not canonical")
        );
        fs::write(&catalog_path, &first_catalog).expect("restore canonical catalog");

        let mut mutated_pointer =
            pb::CurrentSitePointer::decode(first_pointer.as_slice()).expect("decode pointer");
        mutated_pointer.catalog_relpath = Some(pb::NormalizedRelpath {
            value: "alternate/catalog.pb".to_string(),
        });
        fs::write(
            publish_root.join(CURRENT_POINTER_PROTO),
            mutated_pointer.encode_to_vec(),
        )
        .expect("write noncanonical catalog path pointer");
        let error = verify_published_site(&publish_root)
            .expect_err("noncanonical current catalog path must fail");
        assert!(
            error
                .to_string()
                .contains("current catalog path is not canonical")
        );
        fs::write(publish_root.join(CURRENT_POINTER_PROTO), &first_pointer)
            .expect("restore canonical pointer");

        let mut conflicting =
            pb::PublishedSiteCatalog::decode(first_catalog.as_slice()).expect("decode catalog");
        conflicting.base_url = "/conflicting/".to_string();
        fs::write(&catalog_path, conflicting.encode_to_vec()).expect("write conflicting catalog");
        let error = verify_published_site(&publish_root)
            .expect_err("catalog base URL metadata mismatch must fail");
        assert!(
            error
                .to_string()
                .contains("catalog metadata disagrees with site manifest")
        );
        let error = publish_static_site(&site_dir, &publish_root)
            .expect_err("conflicting immutable catalog must fail");
        assert!(
            error
                .to_string()
                .contains("immutable published catalog conflicts")
        );
        fs::remove_dir_all(root).expect("cleanup");
    }
}
