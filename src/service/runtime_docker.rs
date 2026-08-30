// SPDX-License-Identifier: Apache-2.0

use super::*;
use prost::Message;

const RUNTIME_FINGERPRINT_LABEL: &str = "org.xlsynth-bvc.runtime-fingerprint";

fn runtime_fingerprint(kind: &str, fields: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"xlsynth-bvc/runtime-build/v2\0");
    hasher.update(kind.as_bytes());
    for field in fields {
        hasher.update([0]);
        hasher.update(field.as_bytes());
    }
    hex::encode(hasher.finalize())
}

fn driver_runtime_fingerprint(runtime: &DriverRuntimeSpec) -> Result<String> {
    Ok(runtime_fingerprint(
        "driver",
        &[
            &runtime.driver_version,
            &runtime.release_platform,
            &runtime.dockerfile,
            &runtime.dockerfile_sha256,
            &runtime.release_cache_input_sha256,
        ],
    ))
}

fn yosys_runtime_fingerprint(runtime: &YosysRuntimeSpec) -> Result<String> {
    Ok(runtime_fingerprint(
        "yosys",
        &[
            &runtime.dockerfile,
            &runtime.dockerfile_sha256,
            runtime.upstream_commit.as_deref().unwrap_or(""),
        ],
    ))
}

pub(crate) fn driver_cache_mount(
    store: &ArtifactStore,
    runtime: &DriverRuntimeSpec,
) -> Result<DockerMount> {
    DockerMount::read_only(
        &store.driver_release_cache_dir(&runtime.release_cache_input_sha256)?,
        "/cache-input",
    )
}

pub(crate) fn driver_script(body: &str) -> String {
    let mut script = String::from("set -euo pipefail\n");
    script.push_str(DRIVER_TOOLS_SETUP_FROM_CACHE_SNIPPET);
    script.push_str(body);
    if !script.ends_with('\n') {
        script.push('\n');
    }
    script
}

pub(crate) fn ensure_driver_runtime_prepared(
    store: &ArtifactStore,
    repo_root: &Path,
    version: &str,
    runtime: &DriverRuntimeSpec,
    commands: &mut Vec<CommandTrace>,
) -> Result<()> {
    if let Some(trace) = ensure_driver_image(repo_root, runtime)? {
        commands.push(trace);
    }
    if let Some(trace) = ensure_driver_release_cache(
        store,
        repo_root,
        version,
        &runtime.release_platform,
        &runtime.release_cache_input_sha256,
    )? {
        commands.push(trace);
    }
    Ok(())
}

const DRIVER_RELEASE_CACHE_MANIFEST_VERSION: u32 = 2;
const DRIVER_RELEASE_CACHE_INPUT_MANIFEST_VERSION: u32 = 1;
const RELEASE_ASSET_INPUT_KIND: i32 = 1;
const SOURCE_COMMIT_FILE_INPUT_KIND: i32 = 2;
const LOCAL_RESOURCE_FILE_INPUT_KIND: i32 = 3;
const DRIVER_RELEASE_CACHE_PROTO_RELPATHS: [&str; 2] = [
    "xls/estimators/delay_model/delay_info.proto",
    "xls/ir/op.proto",
];

fn cache_file_sha256(path: &Path) -> Result<Vec<u8>> {
    let mut file = fs::File::open(path)
        .with_context(|| format!("opening cache file for hashing: {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .with_context(|| format!("hashing cache file: {}", path.display()))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hasher.finalize().to_vec())
}

fn driver_release_cache_required_relpaths(platform: &str) -> Result<BTreeSet<String>> {
    let dso_suffix = match platform {
        "ubuntu2004" | "ubuntu2204" | "rocky8" | "x64" => ".so",
        "arm64" => ".dylib",
        _ => bail!("unsupported xlsynth release platform: {platform}"),
    };
    let mut required: BTreeSet<String> = DRIVER_RELEASE_CACHE_BINARIES
        .split(',')
        .map(ToOwned::to_owned)
        .collect();
    required.insert(format!("libxls-{platform}{dso_suffix}"));
    required.insert("dslx_stdlib.tar.gz".to_string());
    required.insert("protos/xls/estimators/delay_model/delay_info.proto".to_string());
    required.insert("protos/xls/ir/op.proto".to_string());
    Ok(required)
}

fn driver_release_cache_asset_relpaths(version: &str, platform: &str) -> Result<BTreeSet<String>> {
    let dso_suffix = match platform {
        "ubuntu2004" | "ubuntu2204" | "rocky8" | "x64" => ".so",
        "arm64" => ".dylib",
        _ => bail!("unsupported xlsynth release platform: {platform}"),
    };
    let mut assets: BTreeSet<String> = DRIVER_RELEASE_CACHE_BINARIES
        .split(',')
        .map(|binary| format!("{binary}-{platform}"))
        .collect();
    let version_without_revision = normalize_tag_version(version)
        .split('-')
        .next()
        .unwrap_or(version);
    let gzip_suffix = if cmp_dotted_numeric_version(version_without_revision, "0.0.219")
        != std::cmp::Ordering::Less
    {
        ".gz"
    } else {
        ""
    };
    assets.insert(format!("libxls-{platform}{dso_suffix}{gzip_suffix}"));
    assets.insert("dslx_stdlib.tar.gz".to_string());
    Ok(assets)
}

fn driver_release_cache_input_sha256_bytes(
    inputs: &crate::proto::v1::DriverReleaseCacheInputManifest,
) -> Vec<u8> {
    Sha256::digest(inputs.encode_to_vec()).to_vec()
}

fn driver_release_cache_input_sha256_hex(
    inputs: &crate::proto::v1::DriverReleaseCacheInputManifest,
) -> String {
    hex::encode(driver_release_cache_input_sha256_bytes(inputs))
}

fn validate_driver_release_cache_inputs(
    inputs: &crate::proto::v1::DriverReleaseCacheInputManifest,
    version: &str,
    platform: &str,
) -> Result<()> {
    if inputs.record_version != DRIVER_RELEASE_CACHE_INPUT_MANIFEST_VERSION {
        bail!("unsupported driver release cache input manifest version");
    }
    if inputs
        .dso_version
        .as_ref()
        .map(|value| value.value.as_str())
        != Some(normalize_tag_version(version))
        || inputs.platform != platform
    {
        bail!("driver release cache input manifest identity mismatch");
    }
    let release_input = crate::proto::release_input_for_dso_version(version)?;
    if inputs.source_commit != release_input.source_commit {
        bail!("driver release cache input manifest source commit mismatch");
    }

    let mut expected = BTreeSet::new();
    for relpath in driver_release_cache_asset_relpaths(version, platform)? {
        expected.insert((RELEASE_ASSET_INPUT_KIND, relpath));
    }
    for relpath in DRIVER_RELEASE_CACHE_PROTO_RELPATHS {
        expected.insert((SOURCE_COMMIT_FILE_INPUT_KIND, relpath.to_string()));
    }
    expected.insert((
        LOCAL_RESOURCE_FILE_INPUT_KIND,
        VENDORED_DOWNLOAD_RELEASE_SCRIPT.to_string(),
    ));

    let mut declared = BTreeMap::new();
    let mut previous: Option<(i32, String)> = None;
    for file in &inputs.files {
        if file.kind != RELEASE_ASSET_INPUT_KIND
            && file.kind != SOURCE_COMMIT_FILE_INPUT_KIND
            && file.kind != LOCAL_RESOURCE_FILE_INPUT_KIND
        {
            bail!("driver release cache input has unknown kind {}", file.kind);
        }
        let relpath = file
            .relpath
            .as_ref()
            .context("driver release cache input missing relpath")?
            .value
            .clone();
        validate_relative_subpath(&relpath)?;
        let key = (file.kind, relpath.clone());
        if previous.as_ref().is_some_and(|value| value >= &key) {
            bail!("driver release cache inputs are not strictly sorted");
        }
        previous = Some(key.clone());
        let digest = file
            .sha256
            .as_ref()
            .context("driver release cache input missing sha256")?;
        if digest.value.len() != 32 {
            bail!("driver release cache input has invalid sha256 length: {relpath}");
        }
        declared.insert(key, digest.value.clone());
    }
    if declared.keys().cloned().collect::<BTreeSet<_>>() != expected {
        bail!("driver release cache input manifest does not declare the exact input closure");
    }
    let stdlib_digest = declared
        .get(&(RELEASE_ASSET_INPUT_KIND, "dslx_stdlib.tar.gz".to_string()))
        .context("driver release cache input manifest missing stdlib checksum")?;
    if hex::encode(stdlib_digest) != release_input.stdlib_tarball_sha256 {
        bail!("driver release cache stdlib checksum disagrees with the release-input lock");
    }
    Ok(())
}

fn fetch_release_cache_input_manifest(
    repo_root: &Path,
    version: &str,
    platform: &str,
) -> Result<crate::proto::v1::DriverReleaseCacheInputManifest> {
    let release_input = crate::proto::release_input_for_dso_version(version)?;
    let client = Client::builder()
        .user_agent("xlsynth-bvc/release-cache-inputs")
        .build()
        .context("creating client for driver release cache input resolution")?;
    let release_tag = xlsynth_release_tag(version);
    let base_url = format!("https://github.com/xlsynth/xlsynth/releases/download/{release_tag}");
    let mut files = Vec::new();
    for relpath in driver_release_cache_asset_relpaths(version, platform)? {
        let checksum_url = format!("{base_url}/{relpath}.sha256");
        let checksum_text = client
            .get(&checksum_url)
            .send()
            .with_context(|| format!("downloading release checksum: {checksum_url}"))?
            .error_for_status()
            .with_context(|| format!("release checksum request failed: {checksum_url}"))?
            .text()
            .with_context(|| format!("reading release checksum: {checksum_url}"))?;
        let checksum = checksum_text
            .split_whitespace()
            .next()
            .context("release checksum response is empty")?
            .to_ascii_lowercase();
        if checksum.len() != 64 || !checksum.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            bail!("release checksum is not a SHA-256 digest: {checksum_url}");
        }
        files.push(crate::proto::v1::DriverReleaseCacheInputFile {
            kind: RELEASE_ASSET_INPUT_KIND,
            relpath: Some(crate::proto::v1::NormalizedRelpath { value: relpath }),
            sha256: Some(crate::proto::v1::Sha256Digest {
                value: hex::decode(checksum).context("decoding release checksum")?,
            }),
        });
    }
    for relpath in DRIVER_RELEASE_CACHE_PROTO_RELPATHS {
        let url = format!(
            "https://raw.githubusercontent.com/xlsynth/xlsynth/{}/{relpath}",
            release_input.source_commit
        );
        let bytes = client
            .get(&url)
            .send()
            .with_context(|| format!("downloading source-commit cache input: {url}"))?
            .error_for_status()
            .with_context(|| format!("source-commit cache input request failed: {url}"))?
            .bytes()
            .with_context(|| format!("reading source-commit cache input: {url}"))?;
        files.push(crate::proto::v1::DriverReleaseCacheInputFile {
            kind: SOURCE_COMMIT_FILE_INPUT_KIND,
            relpath: Some(crate::proto::v1::NormalizedRelpath {
                value: relpath.to_string(),
            }),
            sha256: Some(crate::proto::v1::Sha256Digest {
                value: Sha256::digest(&bytes).to_vec(),
            }),
        });
    }
    let script_bytes = fs::read(repo_root.join(VENDORED_DOWNLOAD_RELEASE_SCRIPT))
        .context("reading vendored release-cache setup script")?;
    files.push(crate::proto::v1::DriverReleaseCacheInputFile {
        kind: LOCAL_RESOURCE_FILE_INPUT_KIND,
        relpath: Some(crate::proto::v1::NormalizedRelpath {
            value: VENDORED_DOWNLOAD_RELEASE_SCRIPT.to_string(),
        }),
        sha256: Some(crate::proto::v1::Sha256Digest {
            value: Sha256::digest(&script_bytes).to_vec(),
        }),
    });
    files.sort_by(|a, b| {
        (a.kind, a.relpath.as_ref().map(|value| value.value.as_str()))
            .cmp(&(b.kind, b.relpath.as_ref().map(|value| value.value.as_str())))
    });
    let inputs = crate::proto::v1::DriverReleaseCacheInputManifest {
        record_version: DRIVER_RELEASE_CACHE_INPUT_MANIFEST_VERSION,
        dso_version: Some(crate::proto::v1::DsoVersion {
            value: normalize_tag_version(version).to_string(),
        }),
        platform: platform.to_string(),
        source_commit: release_input.source_commit,
        files,
    };
    validate_driver_release_cache_inputs(&inputs, version, platform)?;
    Ok(inputs)
}

fn resolve_driver_release_cache_input_manifest(
    repo_root: &Path,
    version: &str,
    platform: &str,
) -> Result<crate::proto::v1::DriverReleaseCacheInputManifest> {
    #[cfg(test)]
    {
        let release_input = crate::proto::release_input_for_dso_version(version)?;
        let normalized_version = normalize_tag_version(version);
        let mut files = Vec::new();
        for relpath in driver_release_cache_asset_relpaths(version, platform)? {
            let digest = if relpath == "dslx_stdlib.tar.gz" {
                hex::decode(&release_input.stdlib_tarball_sha256)
                    .context("decoding locked stdlib checksum")?
            } else {
                Sha256::digest(
                    format!(
                        "xlsynth-bvc/test-release-asset/v1\0{normalized_version}\0{platform}\0{relpath}"
                    )
                    .as_bytes(),
                )
                .to_vec()
            };
            files.push(crate::proto::v1::DriverReleaseCacheInputFile {
                kind: RELEASE_ASSET_INPUT_KIND,
                relpath: Some(crate::proto::v1::NormalizedRelpath { value: relpath }),
                sha256: Some(crate::proto::v1::Sha256Digest { value: digest }),
            });
        }
        for relpath in DRIVER_RELEASE_CACHE_PROTO_RELPATHS {
            files.push(crate::proto::v1::DriverReleaseCacheInputFile {
                kind: SOURCE_COMMIT_FILE_INPUT_KIND,
                relpath: Some(crate::proto::v1::NormalizedRelpath {
                    value: relpath.to_string(),
                }),
                sha256: Some(crate::proto::v1::Sha256Digest {
                    value: Sha256::digest(
                        format!(
                            "xlsynth-bvc/test-source-file/v1\0{}\0{relpath}",
                            release_input.source_commit
                        )
                        .as_bytes(),
                    )
                    .to_vec(),
                }),
            });
        }
        let script_bytes = fs::read(repo_root.join(VENDORED_DOWNLOAD_RELEASE_SCRIPT))
            .context("reading vendored release-cache setup script")?;
        files.push(crate::proto::v1::DriverReleaseCacheInputFile {
            kind: LOCAL_RESOURCE_FILE_INPUT_KIND,
            relpath: Some(crate::proto::v1::NormalizedRelpath {
                value: VENDORED_DOWNLOAD_RELEASE_SCRIPT.to_string(),
            }),
            sha256: Some(crate::proto::v1::Sha256Digest {
                value: Sha256::digest(&script_bytes).to_vec(),
            }),
        });
        files.sort_by(|a, b| {
            (a.kind, a.relpath.as_ref().map(|value| value.value.as_str()))
                .cmp(&(b.kind, b.relpath.as_ref().map(|value| value.value.as_str())))
        });
        let inputs = crate::proto::v1::DriverReleaseCacheInputManifest {
            record_version: DRIVER_RELEASE_CACHE_INPUT_MANIFEST_VERSION,
            dso_version: Some(crate::proto::v1::DsoVersion {
                value: normalize_tag_version(version).to_string(),
            }),
            platform: platform.to_string(),
            source_commit: release_input.source_commit,
            files,
        };
        validate_driver_release_cache_inputs(&inputs, version, platform)?;
        return Ok(inputs);
    }
    #[cfg(not(test))]
    {
        static CACHE: std::sync::OnceLock<
            std::sync::Mutex<
                BTreeMap<
                    (String, String, String),
                    crate::proto::v1::DriverReleaseCacheInputManifest,
                >,
            >,
        > = std::sync::OnceLock::new();
        let local_script_sha256 = hex::encode(cache_file_sha256(
            &repo_root.join(VENDORED_DOWNLOAD_RELEASE_SCRIPT),
        )?);
        let key = (
            normalize_tag_version(version).to_string(),
            platform.to_string(),
            local_script_sha256,
        );
        let cache = CACHE.get_or_init(|| std::sync::Mutex::new(BTreeMap::new()));
        if let Some(value) = cache.lock().expect("cache input lock poisoned").get(&key) {
            return Ok(value.clone());
        }
        let resolved = fetch_release_cache_input_manifest(repo_root, version, platform)?;
        cache
            .lock()
            .expect("cache input lock poisoned")
            .insert(key, resolved.clone());
        Ok(resolved)
    }
}

#[cfg(not(test))]
pub(crate) fn driver_release_cache_input_sha256(
    repo_root: &Path,
    version: &str,
    platform: &str,
) -> Result<String> {
    Ok(driver_release_cache_input_sha256_hex(
        &resolve_driver_release_cache_input_manifest(repo_root, version, platform)?,
    ))
}

#[cfg(test)]
pub(crate) fn driver_release_cache_input_sha256(
    repo_root: &Path,
    version: &str,
    platform: &str,
) -> Result<String> {
    Ok(driver_release_cache_input_sha256_hex(
        &resolve_driver_release_cache_input_manifest(repo_root, version, platform)?,
    ))
}

fn build_driver_release_cache_manifest(
    cache_dir: &Path,
    version: &str,
    platform: &str,
    inputs: crate::proto::v1::DriverReleaseCacheInputManifest,
) -> Result<crate::proto::v1::DriverReleaseCacheManifest> {
    validate_driver_release_cache_inputs(&inputs, version, platform)?;
    let mut files = Vec::new();
    for entry in WalkDir::new(cache_dir).sort_by_file_name() {
        let entry =
            entry.with_context(|| format!("walking release cache: {}", cache_dir.display()))?;
        if entry.file_type().is_dir() {
            continue;
        }
        if !entry.file_type().is_file() {
            bail!(
                "release cache contains non-regular entry: {}",
                entry.path().display()
            );
        }
        let relpath = path_to_forward_slashes(
            entry
                .path()
                .strip_prefix(cache_dir)
                .context("computing release cache relpath")?,
        );
        if relpath == DRIVER_RELEASE_CACHE_READY_FILE {
            continue;
        }
        validate_relative_subpath(&relpath)?;
        let metadata = entry
            .metadata()
            .with_context(|| format!("reading cache metadata: {}", entry.path().display()))?;
        files.push(crate::proto::v1::DriverReleaseCacheFile {
            relpath: Some(crate::proto::v1::NormalizedRelpath { value: relpath }),
            bytes: metadata.len(),
            sha256: Some(crate::proto::v1::Sha256Digest {
                value: cache_file_sha256(entry.path())?,
            }),
        });
    }
    files.sort_by(|a, b| {
        a.relpath
            .as_ref()
            .map(|p| p.value.as_str())
            .cmp(&b.relpath.as_ref().map(|p| p.value.as_str()))
    });
    Ok(crate::proto::v1::DriverReleaseCacheManifest {
        record_version: DRIVER_RELEASE_CACHE_MANIFEST_VERSION,
        dso_version: Some(crate::proto::v1::DsoVersion {
            value: normalize_tag_version(version).to_string(),
        }),
        platform: platform.to_string(),
        files,
        inputs: Some(inputs),
    })
}

fn validate_driver_release_cache(
    cache_dir: &Path,
    version: &str,
    platform: &str,
    expected_input_sha256: &str,
) -> Result<()> {
    let ready_path = cache_dir.join(DRIVER_RELEASE_CACHE_READY_FILE);
    let bytes = fs::read(&ready_path)
        .with_context(|| format!("reading cache manifest: {}", ready_path.display()))?;
    let manifest = crate::proto::v1::DriverReleaseCacheManifest::decode(bytes.as_slice())
        .with_context(|| format!("decoding cache manifest: {}", ready_path.display()))?;
    if manifest.record_version != DRIVER_RELEASE_CACHE_MANIFEST_VERSION {
        bail!("unsupported driver release cache manifest version");
    }
    if manifest.dso_version.as_ref().map(|v| v.value.as_str())
        != Some(normalize_tag_version(version))
        || manifest.platform != platform
    {
        bail!("driver release cache manifest identity mismatch");
    }
    let inputs = manifest
        .inputs
        .as_ref()
        .context("driver release cache manifest is missing its input manifest")?;
    validate_driver_release_cache_inputs(inputs, version, platform)?;
    if driver_release_cache_input_sha256_hex(inputs) != expected_input_sha256 {
        bail!("driver release cache input digest does not match the planned runtime");
    }

    let mut declared = BTreeMap::new();
    let mut previous: Option<String> = None;
    for file in &manifest.files {
        let relpath = file
            .relpath
            .as_ref()
            .context("cache manifest file missing relpath")?
            .value
            .clone();
        validate_relative_subpath(&relpath)?;
        if previous.as_deref().is_some_and(|p| p >= relpath.as_str()) {
            bail!("cache manifest files are not strictly sorted");
        }
        previous = Some(relpath.clone());
        let digest = file
            .sha256
            .as_ref()
            .context("cache manifest file missing sha256")?;
        if digest.value.len() != 32 {
            bail!("cache manifest file has invalid sha256 length: {relpath}");
        }
        declared.insert(relpath, (file.bytes, digest.value.clone()));
    }

    let required = driver_release_cache_required_relpaths(platform)?;
    for relpath in &required {
        let Some((bytes, _)) = declared.get(relpath) else {
            bail!("cache manifest is missing required file: {relpath}");
        };
        if *bytes == 0 {
            bail!("required cache file is empty: {relpath}");
        }
    }

    let mut actual = BTreeMap::new();
    for entry in WalkDir::new(cache_dir).sort_by_file_name() {
        let entry =
            entry.with_context(|| format!("walking release cache: {}", cache_dir.display()))?;
        if entry.file_type().is_dir() {
            continue;
        }
        if !entry.file_type().is_file() {
            bail!(
                "release cache contains non-regular entry: {}",
                entry.path().display()
            );
        }
        let relpath = path_to_forward_slashes(
            entry
                .path()
                .strip_prefix(cache_dir)
                .context("computing release cache relpath")?,
        );
        if relpath == DRIVER_RELEASE_CACHE_READY_FILE {
            continue;
        }
        let metadata = entry
            .metadata()
            .with_context(|| format!("reading cache metadata: {}", entry.path().display()))?;
        actual.insert(relpath, (metadata.len(), cache_file_sha256(entry.path())?));
    }
    if actual != declared {
        bail!("driver release cache contents do not match its protobuf manifest");
    }
    Ok(())
}

fn driver_release_cache_is_ready(
    cache_dir: &Path,
    version: &str,
    platform: &str,
    expected_input_sha256: &str,
) -> bool {
    if !cache_dir.join(DRIVER_RELEASE_CACHE_READY_FILE).is_file() {
        return false;
    }
    match validate_driver_release_cache(cache_dir, version, platform, expected_input_sha256) {
        Ok(()) => true,
        Err(err) => {
            eprintln!(
                "ignoring invalid driver release cache {}: {:#}",
                cache_dir.display(),
                err
            );
            false
        }
    }
}

fn write_cache_manifest_atomic(
    path: &Path,
    manifest: &crate::proto::v1::DriverReleaseCacheManifest,
) -> Result<()> {
    let tmp = path.with_file_name(format!(
        ".{}.tmp-{}-{}",
        DRIVER_RELEASE_CACHE_READY_FILE,
        std::process::id(),
        DOCKER_RUN_NAME_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    fs::write(&tmp, manifest.encode_to_vec())
        .with_context(|| format!("writing staged cache manifest: {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| {
        format!(
            "publishing staged cache manifest: {} -> {}",
            tmp.display(),
            path.display()
        )
    })
}

pub(crate) fn ensure_driver_release_cache(
    store: &ArtifactStore,
    repo_root: &Path,
    version: &str,
    platform: &str,
    expected_input_sha256: &str,
) -> Result<Option<CommandTrace>> {
    let cache_dir = store.driver_release_cache_dir(expected_input_sha256)?;
    if driver_release_cache_is_ready(&cache_dir, version, platform, expected_input_sha256) {
        return Ok(None);
    }

    let cache_root = store.driver_release_cache_root();
    fs::create_dir_all(&cache_root).with_context(|| {
        format!(
            "creating driver release cache root: {}",
            cache_root.display()
        )
    })?;
    let lock_key = expected_input_sha256.to_ascii_lowercase();
    let lock_path = cache_root.join(format!("{DRIVER_RELEASE_CACHE_LOCK_FILE}-{lock_key}"));
    let start = Instant::now();
    loop {
        if driver_release_cache_is_ready(&cache_dir, version, platform, expected_input_sha256) {
            return Ok(None);
        }
        match fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock_path)
        {
            Ok(mut lock_file) => {
                let write_result = (|| -> Result<()> {
                    let lock = crate::proto::v1::DriverReleaseCacheSetupLock {
                        record_version: 1,
                        hostname: std::env::var("HOSTNAME")
                            .unwrap_or_else(|_| "unknown-host".to_string()),
                        pid: std::process::id(),
                        process_start_ticks: process_start_ticks(std::process::id()).unwrap_or(0),
                    };
                    lock_file
                        .write_all(&lock.encode_to_vec())
                        .with_context(|| {
                            format!("writing cache setup lock: {}", lock_path.display())
                        })?;
                    lock_file.sync_all().with_context(|| {
                        format!("syncing cache setup lock: {}", lock_path.display())
                    })
                })();
                if let Err(error) = write_result {
                    drop(lock_file);
                    let _ = fs::remove_file(&lock_path);
                    return Err(error);
                }
                break;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if is_stale_cache_setup_lock(&lock_path, DRIVER_RELEASE_CACHE_SETUP_TIMEOUT_SECS) {
                    eprintln!(
                        "removing stale driver release cache lock: {}",
                        lock_path.display()
                    );
                    let _ = fs::remove_file(&lock_path);
                    continue;
                }
                if start.elapsed().as_secs() > DRIVER_RELEASE_CACHE_SETUP_TIMEOUT_SECS {
                    bail!(
                        "timed out waiting for driver release cache lock: {}",
                        lock_path.display()
                    );
                }
                thread::sleep(Duration::from_millis(
                    DRIVER_RELEASE_CACHE_SETUP_POLL_MILLIS,
                ));
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!(
                        "creating driver release cache lock file: {}",
                        lock_path.display()
                    )
                });
            }
        }
    }

    let staging_dir = cache_root.join(format!(
        ".staging-{lock_key}-{}-{}",
        std::process::id(),
        DOCKER_RUN_NAME_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let setup_result = (|| -> Result<Option<CommandTrace>> {
        if driver_release_cache_is_ready(&cache_dir, version, platform, expected_input_sha256) {
            return Ok(None);
        }
        if cache_dir.exists() {
            bail!(
                "immutable driver release cache is present but invalid at {}; remove that exact digest directory out of band before retrying",
                cache_dir.display()
            );
        }
        remove_path_if_exists(&staging_dir)?;
        fs::create_dir_all(&staging_dir)
            .with_context(|| format!("creating staged release cache: {}", staging_dir.display()))?;

        let script_path = repo_root.join(VENDORED_DOWNLOAD_RELEASE_SCRIPT);
        if !script_path.is_file() {
            bail!(
                "vendored download_release.py not found: {}",
                script_path.display()
            );
        }
        let release_tag = xlsynth_release_tag(version);
        let inputs = resolve_driver_release_cache_input_manifest(repo_root, version, platform)?;
        let actual_input_sha256 = driver_release_cache_input_sha256_hex(&inputs);
        if actual_input_sha256 != expected_input_sha256 {
            bail!(
                "driver release cache inputs changed after action planning: expected {}, resolved {}",
                expected_input_sha256,
                actual_input_sha256
            );
        }
        let args: Vec<OsString> = vec![
            script_path.into_os_string(),
            OsString::from("-v"),
            OsString::from(&release_tag),
            OsString::from("-o"),
            staging_dir.clone().into_os_string(),
            OsString::from("-p"),
            OsString::from(platform),
            OsString::from("-d"),
            OsString::from("-b"),
            OsString::from(DRIVER_RELEASE_CACHE_BINARIES),
        ];
        let mut args = args;
        for input in &inputs.files {
            if input.kind != RELEASE_ASSET_INPUT_KIND {
                continue;
            }
            let relpath = &input
                .relpath
                .as_ref()
                .context("release asset input missing relpath")?
                .value;
            let digest = input
                .sha256
                .as_ref()
                .context("release asset input missing sha256")?;
            args.push(OsString::from("--expected_sha256"));
            args.push(OsString::from(format!(
                "{}={}",
                relpath,
                hex::encode(&digest.value)
            )));
        }
        let output = Command::new("python3")
            .args(&args)
            .output()
            .context("running vendored download_release.py for driver release cache")?;
        if !output.status.success() {
            bail!(
                "driver release cache setup failed for version {} platform {} with exit code {:?}\nstdout:\n{}\nstderr:\n{}",
                version,
                platform,
                output.status.code(),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let client = Client::builder()
            .build()
            .context("creating reqwest client for delay-info proto cache")?;
        let delay_info_proto =
            staging_dir.join("protos/xls/estimators/delay_model/delay_info.proto");
        let op_proto = staging_dir.join("protos/xls/ir/op.proto");
        let delay_info_url = format!(
            "https://raw.githubusercontent.com/xlsynth/xlsynth/{}/xls/estimators/delay_model/delay_info.proto",
            inputs.source_commit
        );
        let op_url = format!(
            "https://raw.githubusercontent.com/xlsynth/xlsynth/{}/xls/ir/op.proto",
            inputs.source_commit
        );
        for path in [&delay_info_proto, &op_proto] {
            fs::create_dir_all(path.parent().context("cache proto path missing parent")?)
                .with_context(|| {
                    format!("creating proto cache directory for {}", path.display())
                })?;
        }
        download_to_file(&client, &delay_info_url, &delay_info_proto)?;
        download_to_file(&client, &op_url, &op_proto)?;
        for (relpath, path) in [
            (DRIVER_RELEASE_CACHE_PROTO_RELPATHS[0], &delay_info_proto),
            (DRIVER_RELEASE_CACHE_PROTO_RELPATHS[1], &op_proto),
        ] {
            let expected = inputs
                .files
                .iter()
                .find(|input| {
                    input.kind == SOURCE_COMMIT_FILE_INPUT_KIND
                        && input
                            .relpath
                            .as_ref()
                            .is_some_and(|value| value.value == relpath)
                })
                .and_then(|input| input.sha256.as_ref())
                .context("source-commit cache input missing expected checksum")?;
            if cache_file_sha256(path)? != expected.value {
                bail!("source-commit cache input checksum mismatch: {relpath}");
            }
        }

        let manifest =
            build_driver_release_cache_manifest(&staging_dir, version, platform, inputs)?;
        write_cache_manifest_atomic(
            &staging_dir.join(DRIVER_RELEASE_CACHE_READY_FILE),
            &manifest,
        )?;
        validate_driver_release_cache(&staging_dir, version, platform, expected_input_sha256)?;

        if let Some(parent) = cache_dir.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("creating cache promotion parent: {}", parent.display())
            })?;
        }
        fs::rename(&staging_dir, &cache_dir).with_context(|| {
            format!(
                "atomically promoting driver release cache: {} -> {}",
                staging_dir.display(),
                cache_dir.display()
            )
        })?;
        Ok(Some(CommandTrace {
            argv: os_args_to_string("python3", &args),
            exit_code: output.status.code().unwrap_or(1),
        }))
    })();

    if setup_result.is_err() {
        remove_path_if_exists(&staging_dir).ok();
    }
    fs::remove_file(&lock_path).ok();
    setup_result
}

pub(crate) fn is_stale_cache_setup_lock(lock_path: &Path, stale_after_secs: u64) -> bool {
    if let Ok(bytes) = fs::read(lock_path)
        && let Ok(lock) = crate::proto::v1::DriverReleaseCacheSetupLock::decode(bytes.as_slice())
        && lock.record_version == 1
    {
        let local_host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".to_string());
        if lock.hostname == local_host {
            return process_start_ticks(lock.pid) != Some(lock.process_start_ticks);
        }
    }

    let metadata = match fs::metadata(lock_path) {
        Ok(m) => m,
        Err(_) => return false,
    };
    let modified = match metadata.modified() {
        Ok(t) => t,
        Err(_) => return false,
    };
    match std::time::SystemTime::now().duration_since(modified) {
        Ok(age) => age.as_secs() > stale_after_secs,
        Err(_) => false,
    }
}

pub(crate) fn collect_pending_queue_actions(store: &ArtifactStore) -> Result<Vec<ActionSpec>> {
    let mut paths = list_queue_files(&store.queue_pending_dir())?;
    paths.sort();
    let mut actions = Vec::new();
    for path in paths {
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("reading pending queue record: {}", path.display()));
            }
        };
        match parse_queue_work_item(&bytes, &path) {
            Ok((_, _, _, action)) => actions.push(action),
            Err(err) => {
                quarantine_corrupt_queue_file(
                    &path,
                    &format!("parse error while preparing runtime environment: {:#}", err),
                )?;
            }
        }
    }
    Ok(actions)
}

pub(crate) fn prepare_queue_runtime_environment(
    store: &ArtifactStore,
    repo_root: &Path,
) -> Result<()> {
    let actions = collect_pending_queue_actions(store)?;
    if actions.is_empty() {
        return Ok(());
    }

    let mut driver_runtimes: HashSet<DriverRuntimeSpec> = HashSet::new();
    let mut driver_releases: HashSet<(String, String, String)> = HashSet::new();
    let mut yosys_runtimes: HashSet<YosysRuntimeSpec> = HashSet::new();

    for action in actions {
        match action {
            ActionSpec::ImportIrPackageFile { .. } => {}
            ActionSpec::DriverDslxFnToIr {
                version, runtime, ..
            }
            | ActionSpec::DriverIrToOpt {
                version, runtime, ..
            }
            | ActionSpec::DriverIrToDelayInfo {
                version, runtime, ..
            }
            | ActionSpec::DriverIrEquiv {
                version, runtime, ..
            }
            | ActionSpec::DriverIrAigEquiv {
                version, runtime, ..
            }
            | ActionSpec::DriverIrToG8rAig {
                version, runtime, ..
            }
            | ActionSpec::IrFnToCombinationalVerilog {
                version, runtime, ..
            }
            | ActionSpec::IrFnToKBoolConeCorpus {
                version, runtime, ..
            }
            | ActionSpec::IrFnToMffcCorpus {
                version, runtime, ..
            } => {
                driver_releases.insert((
                    version,
                    runtime.release_platform.clone(),
                    runtime.release_cache_input_sha256.clone(),
                ));
                driver_runtimes.insert(runtime);
            }
            ActionSpec::DriverAigToStats { runtime, .. } => {
                let release_platform = runtime.release_platform.clone();
                let driver_version = runtime.driver_version.clone();
                let release_cache_input_sha256 = runtime.release_cache_input_sha256.clone();
                driver_runtimes.insert(runtime);
                let runtime_xlsynth_version =
                    resolve_xlsynth_version_for_driver(repo_root, &driver_version)?;
                driver_releases.insert((
                    runtime_xlsynth_version,
                    release_platform,
                    release_cache_input_sha256,
                ));
            }
            ActionSpec::ComboVerilogToYosysAbcAig { runtime, .. } => {
                yosys_runtimes.insert(runtime);
            }
            ActionSpec::AigToYosysAbcAig { runtime, .. } => {
                yosys_runtimes.insert(runtime);
            }
            ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { .. }
            | ActionSpec::DownloadAndExtractXlsynthSourceSubtree { .. }
            | ActionSpec::AigStatDiff { .. } => {}
        }
    }

    for runtime in &driver_runtimes {
        ensure_driver_image(repo_root, runtime)?;
    }
    for runtime in &yosys_runtimes {
        ensure_yosys_image(repo_root, runtime)?;
    }
    for (version, platform, expected_input_sha256) in &driver_releases {
        ensure_driver_release_cache(store, repo_root, version, platform, expected_input_sha256)?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct DockerMount {
    host_path: PathBuf,
    container_path: String,
    read_only: bool,
}

impl DockerMount {
    pub(crate) fn read_only(host_path: &Path, container_path: &str) -> Result<Self> {
        let host_path = host_path
            .canonicalize()
            .with_context(|| format!("canonicalizing mount path: {}", host_path.display()))?;
        Ok(Self {
            host_path,
            container_path: container_path.to_string(),
            read_only: true,
        })
    }

    pub(crate) fn read_write(host_path: &Path, container_path: &str) -> Result<Self> {
        let host_path = host_path
            .canonicalize()
            .with_context(|| format!("canonicalizing mount path: {}", host_path.display()))?;
        Ok(Self {
            host_path,
            container_path: container_path.to_string(),
            read_only: false,
        })
    }
}

fn checked_runtime_dockerfile(
    repo_root: &Path,
    dockerfile: &str,
    expected_sha256: &str,
) -> Result<PathBuf> {
    let path = PathBuf::from(dockerfile);
    let path = if path.is_absolute() {
        path
    } else {
        repo_root.join(path)
    };
    if !path.is_file() {
        bail!("dockerfile not found: {}", path.display());
    }
    let actual_sha256 = runtime_dockerfile_sha256(repo_root, dockerfile)?;
    if actual_sha256 != expected_sha256 {
        bail!(
            "runtime Dockerfile digest mismatch for {}: expected {} got {}",
            path.display(),
            expected_sha256,
            actual_sha256
        );
    }
    Ok(path)
}

fn inspect_image_runtime_fingerprint(image: &str) -> Result<Option<String>> {
    let format = format!(
        "{{{{ index .Config.Labels \"{}\" }}}}",
        RUNTIME_FINGERPRINT_LABEL
    );
    let output = Command::new("docker")
        .args(["image", "inspect", "--format", &format, image])
        .output()
        .context("running `docker image inspect` for runtime fingerprint")?;
    if !output.status.success() {
        return Ok(None);
    }
    Ok(Some(
        String::from_utf8(output.stdout)
            .context("decoding docker runtime fingerprint label")?
            .trim()
            .to_string(),
    ))
}
fn normalize_docker_image_id(value: &str) -> Result<String> {
    let value = value.strip_prefix("sha256:").unwrap_or(value);
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("docker image ID must be sha256:<64 hexadecimal digits>; got {value}");
    }
    Ok(value.to_ascii_lowercase())
}

fn inspect_image_id(image: &str) -> Result<Option<String>> {
    let output = Command::new("docker")
        .args(["image", "inspect", "--format", "{{.Id}}", image])
        .output()
        .context("running `docker image inspect` for immutable image ID")?;
    if !output.status.success() {
        return Ok(None);
    }
    Ok(Some(normalize_docker_image_id(
        String::from_utf8(output.stdout)
            .context("decoding docker image ID")?
            .trim(),
    )?))
}

pub(crate) fn docker_image_content_ref(image_id: &str) -> Result<String> {
    Ok(format!("sha256:{}", normalize_docker_image_id(image_id)?))
}

fn require_image_runtime_fingerprint(image: &str, expected: &str) -> Result<()> {
    let actual = inspect_image_runtime_fingerprint(image)?
        .with_context(|| format!("built docker image `{image}` is missing"))?;
    if actual != expected {
        bail!(
            "docker image `{}` runtime fingerprint mismatch: expected {} got {}",
            image,
            expected,
            if actual.is_empty() || actual == "<no value>" {
                "<missing>"
            } else {
                actual.as_str()
            }
        );
    }
    Ok(())
}

fn fingerprint_qualified_image_ref(image: &str, kind: &str, fingerprint: &str) -> Result<String> {
    if image.contains('@') {
        bail!("runtime image name must be a repository or tag, not a digest reference");
    }
    let last_slash = image.rfind('/');
    let last_colon = image.rfind(':');
    let (repository, original_tag) = match (last_slash, last_colon) {
        (_, Some(colon)) if last_slash.is_none_or(|slash| colon > slash) => {
            (&image[..colon], Some(&image[colon + 1..]))
        }
        _ => (image, None),
    };
    if repository.is_empty() || original_tag.is_some_and(str::is_empty) || fingerprint.len() != 64 {
        bail!("cannot construct fingerprint-qualified runtime image reference");
    }
    let tag = original_tag
        .map(|tag| format!("{tag}-bvc-{kind}-{fingerprint}"))
        .unwrap_or_else(|| format!("bvc-{kind}-{fingerprint}"));
    Ok(format!("{repository}:{tag}"))
}

fn driver_image_build_args(
    runtime: &DriverRuntimeSpec,
    dockerfile: PathBuf,
    fingerprint: &str,
    image_ref: &str,
) -> Vec<OsString> {
    vec![
        OsString::from("build"),
        OsString::from("--file"),
        dockerfile.into_os_string(),
        OsString::from("--tag"),
        OsString::from(image_ref),
        OsString::from("--build-arg"),
        OsString::from(format!("DRIVER_CRATE_VERSION={}", runtime.driver_version)),
        OsString::from("--build-arg"),
        OsString::from(format!("BVC_RUNTIME_FINGERPRINT={fingerprint}")),
        OsString::from("."),
    ]
}

fn yosys_image_build_args(
    runtime: &YosysRuntimeSpec,
    dockerfile: PathBuf,
    fingerprint: &str,
    image_ref: &str,
) -> Result<Vec<OsString>> {
    let commit = runtime
        .upstream_commit
        .as_deref()
        .context("yosys runtime requires upstream_commit")?;
    let commit_prefix = &commit[..8];
    Ok(vec![
        OsString::from("build"),
        OsString::from("--file"),
        dockerfile.into_os_string(),
        OsString::from("--tag"),
        OsString::from(image_ref),
        OsString::from("--build-arg"),
        OsString::from(format!("YOSYS_COMMIT={commit}")),
        OsString::from("--build-arg"),
        OsString::from(format!("YOSYS_COMMIT_PREFIX={commit_prefix}")),
        OsString::from("--build-arg"),
        OsString::from(format!("BVC_RUNTIME_FINGERPRINT={fingerprint}")),
        OsString::from("."),
    ])
}

pub(crate) fn ensure_driver_image(
    repo_root: &Path,
    runtime: &DriverRuntimeSpec,
) -> Result<Option<CommandTrace>> {
    let fingerprint = driver_runtime_fingerprint(runtime)?;
    if !runtime.docker_image_id.is_empty() {
        let content_ref = docker_image_content_ref(&runtime.docker_image_id)?;
        require_image_runtime_fingerprint(&content_ref, &fingerprint)?;
        return Ok(None);
    }

    let dockerfile =
        checked_runtime_dockerfile(repo_root, &runtime.dockerfile, &runtime.dockerfile_sha256)?;
    let build_ref = fingerprint_qualified_image_ref(&runtime.docker_image, "driver", &fingerprint)?;
    if let Some(actual) = inspect_image_runtime_fingerprint(&build_ref)? {
        if actual != fingerprint {
            bail!(
                "fingerprint-qualified driver image `{}` has the wrong runtime identity",
                build_ref
            );
        }
        return Ok(None);
    }

    let build_args = driver_image_build_args(runtime, dockerfile, &fingerprint, &build_ref);

    let status = Command::new("docker")
        .args(&build_args)
        .current_dir(repo_root)
        .status()
        .context("running docker build for xlsynth-driver image")?;

    if !status.success() {
        bail!(
            "docker image build failed for image `{}` with exit code {:?}",
            build_ref,
            status.code()
        );
    }

    require_image_runtime_fingerprint(&build_ref, &fingerprint)?;
    Ok(Some(CommandTrace {
        argv: os_args_to_string("docker", &build_args),
        exit_code: status.code().unwrap_or(1),
    }))
}

pub(crate) fn ensure_yosys_image(
    repo_root: &Path,
    runtime: &YosysRuntimeSpec,
) -> Result<Option<CommandTrace>> {
    let fingerprint = yosys_runtime_fingerprint(runtime)?;
    if !runtime.docker_image_id.is_empty() {
        let content_ref = docker_image_content_ref(&runtime.docker_image_id)?;
        require_image_runtime_fingerprint(&content_ref, &fingerprint)?;
        if !image_has_python3(&content_ref)? {
            bail!("pinned Yosys image `{content_ref}` lacks python3");
        }
        return Ok(None);
    }

    let dockerfile =
        checked_runtime_dockerfile(repo_root, &runtime.dockerfile, &runtime.dockerfile_sha256)?;
    let build_ref = fingerprint_qualified_image_ref(&runtime.docker_image, "yosys", &fingerprint)?;
    if let Some(actual) = inspect_image_runtime_fingerprint(&build_ref)? {
        if actual != fingerprint {
            bail!(
                "fingerprint-qualified Yosys image `{}` has the wrong runtime identity",
                build_ref
            );
        }
        if !image_has_python3(&build_ref)? {
            bail!("fingerprint-qualified Yosys image `{build_ref}` lacks python3");
        }
        return Ok(None);
    }

    let build_args = yosys_image_build_args(runtime, dockerfile, &fingerprint, &build_ref)?;

    let status = Command::new("docker")
        .args(&build_args)
        .current_dir(repo_root)
        .status()
        .context("running docker build for yosys image")?;

    if !status.success() {
        bail!(
            "docker image build failed for image `{}` with exit code {:?}",
            build_ref,
            status.code()
        );
    }

    require_image_runtime_fingerprint(&build_ref, &fingerprint)?;
    if !image_has_python3(&build_ref)? {
        bail!("built Yosys image `{build_ref}` lacks python3");
    }
    Ok(Some(CommandTrace {
        argv: os_args_to_string("docker", &build_args),
        exit_code: status.code().unwrap_or(1),
    }))
}

#[cfg(test)]
fn test_runtime_image_id(kind: &str, image: &str, fingerprint: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"xlsynth-bvc/test-runtime-image/v1\0");
    hasher.update(kind.as_bytes());
    hasher.update([0]);
    hasher.update(image.as_bytes());
    hasher.update([0]);
    hasher.update(fingerprint.as_bytes());
    hex::encode(hasher.finalize())
}

pub(crate) fn bind_driver_runtime_image(
    repo_root: &Path,
    mut runtime: DriverRuntimeSpec,
) -> Result<DriverRuntimeSpec> {
    runtime.docker_image_id.clear();
    #[cfg(test)]
    {
        runtime.docker_image_id = test_runtime_image_id(
            "driver",
            &runtime.docker_image,
            &driver_runtime_fingerprint(&runtime)?,
        );
        let _ = repo_root;
        return Ok(runtime);
    }
    #[cfg(not(test))]
    {
        ensure_driver_image(repo_root, &runtime)?;
        let fingerprint = driver_runtime_fingerprint(&runtime)?;
        let build_ref =
            fingerprint_qualified_image_ref(&runtime.docker_image, "driver", &fingerprint)?;
        let image_id = inspect_image_id(&build_ref)?
            .with_context(|| format!("driver image `{build_ref}` disappeared after preparation"))?;
        let content_ref = docker_image_content_ref(&image_id)?;
        require_image_runtime_fingerprint(&content_ref, &fingerprint)?;
        runtime.docker_image_id = image_id;
        Ok(runtime)
    }
}

pub(crate) fn bind_yosys_runtime_image(
    repo_root: &Path,
    mut runtime: YosysRuntimeSpec,
) -> Result<YosysRuntimeSpec> {
    runtime.docker_image_id.clear();
    #[cfg(test)]
    {
        runtime.docker_image_id = test_runtime_image_id(
            "yosys",
            &runtime.docker_image,
            &yosys_runtime_fingerprint(&runtime)?,
        );
        let _ = repo_root;
        return Ok(runtime);
    }
    #[cfg(not(test))]
    {
        ensure_yosys_image(repo_root, &runtime)?;
        let fingerprint = yosys_runtime_fingerprint(&runtime)?;
        let build_ref =
            fingerprint_qualified_image_ref(&runtime.docker_image, "yosys", &fingerprint)?;
        let image_id = inspect_image_id(&build_ref)?
            .with_context(|| format!("Yosys image `{build_ref}` disappeared after preparation"))?;
        let content_ref = docker_image_content_ref(&image_id)?;
        require_image_runtime_fingerprint(&content_ref, &fingerprint)?;
        if !image_has_python3(&content_ref)? {
            bail!("prepared Yosys image `{content_ref}` lacks python3");
        }
        runtime.docker_image_id = image_id;
        Ok(runtime)
    }
}
fn image_has_python3(image: &str) -> Result<bool> {
    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--pull",
            "never",
            image,
            "python3",
            "--version",
        ])
        .output()
        .with_context(|| format!("probing python3 availability in image `{image}`"))?;
    Ok(output.status.success())
}

const PERSISTENT_RUNNER_SCHEMA_VERSION: u32 = 2;
const PERSISTENT_RUNNER_ROOT_DIR: &str = "persistent-runners";
const PERSISTENT_RUNNER_RUNNERS_DIR: &str = "runners";
const PERSISTENT_RUNNER_REQUEST_TIMEOUT_GRACE_SECS: u64 = 15;
const PERSISTENT_RUNNER_HEARTBEAT_STALE_SECS: u64 = 15;
const PERSISTENT_RUNNER_POOL_SIZE_ENV: &str = "XLSYNTH_BVC_PERSISTENT_RUNNER_POOL_SIZE";
const PERSISTENT_RUNNER_DRIVER_POOL_SIZE_ENV: &str =
    "XLSYNTH_BVC_DRIVER_PERSISTENT_RUNNER_POOL_SIZE";
const PERSISTENT_RUNNER_YOSYS_POOL_SIZE_ENV: &str = "XLSYNTH_BVC_YOSYS_PERSISTENT_RUNNER_POOL_SIZE";
const PERSISTENT_RUNNER_IDLE_TTL_SECS_ENV: &str = "XLSYNTH_BVC_PERSISTENT_RUNNER_IDLE_TTL_SECS";
const PERSISTENT_RUNNER_DEFAULT_IDLE_TTL_SECS: u64 = 15 * 60;
const PERSISTENT_RUNNER_WORKER_SCRIPT: &str = "scripts/persistent_runner_worker.py";
static PERSISTENT_RUNNER_START_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> =
    std::sync::OnceLock::new();

#[derive(Clone, Copy)]
enum PersistentRunnerFamily {
    Driver,
    Yosys,
}

#[derive(Debug, Clone)]
struct PersistentRunnerPaths {
    root: PathBuf,
    jobs_dir: PathBuf,
    inbox_dir: PathBuf,
    processing_dir: PathBuf,
    results_dir: PathBuf,
    archive_dir: PathBuf,
    heartbeat_path: PathBuf,
    live_path: PathBuf,
    capabilities_path: PathBuf,
}

impl PersistentRunnerPaths {
    fn new(store_root: &Path, runner_key: &str) -> Self {
        let root = store_root
            .join(PERSISTENT_RUNNER_ROOT_DIR)
            .join(PERSISTENT_RUNNER_RUNNERS_DIR)
            .join(runner_key);
        Self {
            jobs_dir: root.join("jobs"),
            inbox_dir: root.join("inbox"),
            processing_dir: root.join("processing"),
            results_dir: root.join("results"),
            archive_dir: root.join("archive"),
            heartbeat_path: root.join("heartbeat.json"),
            live_path: root.join("live.json"),
            capabilities_path: root.join("capabilities.json"),
            root,
        }
    }

    fn ensure_layout(&self) -> Result<()> {
        for path in [
            &self.root,
            &self.jobs_dir,
            &self.inbox_dir,
            &self.processing_dir,
            &self.results_dir,
            &self.archive_dir,
        ] {
            fs::create_dir_all(path)
                .with_context(|| format!("creating persistent runner dir: {}", path.display()))?;
        }
        Ok(())
    }

    fn reset_request_state(&self) -> Result<()> {
        for path in [
            &self.jobs_dir,
            &self.inbox_dir,
            &self.processing_dir,
            &self.results_dir,
            &self.archive_dir,
        ] {
            remove_path_if_exists(path)?;
            fs::create_dir_all(path)
                .with_context(|| format!("recreating persistent runner dir: {}", path.display()))?;
        }
        remove_file_if_exists(&self.heartbeat_path)?;
        remove_file_if_exists(&self.live_path)?;
        remove_file_if_exists(&self.capabilities_path)?;
        Ok(())
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistentRunnerMountRequest {
    source_path: String,
    target_path: String,
    read_only: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistentRunnerRequest {
    schema_version: u32,
    request_id: String,
    request_token: String,
    runner_key: String,
    runner_instance_id: String,
    container_name: String,
    image: String,
    job_root: String,
    heartbeat_path: String,
    timeout_secs: u64,
    env: BTreeMap<String, String>,
    script: String,
    mounts: Vec<PersistentRunnerMountRequest>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct PersistentRunnerResult {
    request_id: String,
    request_token: String,
    status: String,
    exit_code: Option<i32>,
    timed_out: bool,
    error: Option<String>,
    stdout_tail: String,
    stderr_tail: String,
    command_argv: Vec<String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
struct PersistentRunnerCapabilities {
    #[serde(default)]
    runner_family: String,
    #[serde(default)]
    driver_subcommands: BTreeMap<String, bool>,
    #[serde(default)]
    driver_help_tokens: BTreeMap<String, BTreeMap<String, bool>>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
struct PersistentRunnerHeartbeat {
    #[serde(default)]
    state: String,
    #[serde(default)]
    current_request_id: Option<String>,
}

#[derive(Debug, Clone)]
struct SelectedPersistentRunner {
    runner_key: String,
    runner_instance_id: String,
    container_name: String,
    paths: PersistentRunnerPaths,
}

#[derive(Debug, Clone)]
struct PersistentRunnerSlotStatus {
    slot: usize,
    runner_key: String,
    container_name: String,
    paths: PersistentRunnerPaths,
    running: bool,
    depth: usize,
    idle: bool,
}

#[derive(Debug)]
struct ExternalWriteback {
    host_path: PathBuf,
    job_path: PathBuf,
}

fn persistent_runner_start_lock() -> &'static std::sync::Mutex<()> {
    PERSISTENT_RUNNER_START_LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

fn sanitize_container_name_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else {
            out.push('-');
        }
    }
    while out.contains("--") {
        out = out.replace("--", "-");
    }
    out.trim_matches('-').to_string()
}

fn persistent_runner_key(image: &str, store_root: &Path) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"xlsynth-bvc/persistent-runner/v2\0");
    hasher.update(PERSISTENT_RUNNER_SCHEMA_VERSION.to_le_bytes());
    hasher.update(include_bytes!("../../scripts/persistent_runner_worker.py"));
    hasher.update([0]);
    hasher.update(image.as_bytes());
    hasher.update([0]);
    hasher.update(store_root.as_os_str().as_encoded_bytes());
    hex::encode(hasher.finalize())[..24].to_string()
}

fn persistent_runner_request_identity(incarnation: &str, request_seq: u64) -> (String, String) {
    let request_id = format!("req-{}-{}", &incarnation[..16], request_seq);
    let mut token_hasher = Sha256::new();
    token_hasher.update(b"xlsynth-bvc/persistent-runner-request/v1\0");
    token_hasher.update(incarnation.as_bytes());
    token_hasher.update(request_id.as_bytes());
    (request_id, hex::encode(token_hasher.finalize()))
}

fn persistent_runner_container_name(image: &str, runner_key: &str) -> String {
    let image_hint = sanitize_container_name_component(image);
    let image_hint = if image_hint.is_empty() {
        "runner".to_string()
    } else {
        image_hint.chars().take(20).collect()
    };
    format!("xlsynth-bvc-pr-{}-{}", image_hint, runner_key)
}

fn persistent_runner_slot_key(base_runner_key: &str, pool_size: usize, slot: usize) -> String {
    if pool_size <= 1 {
        base_runner_key.to_string()
    } else {
        format!("{base_runner_key}-s{slot:02}")
    }
}

fn persistent_runner_instance_id(runner_key: &str) -> String {
    format!("instance-{}", runner_key)
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
}

fn persistent_runner_pool_size(family: PersistentRunnerFamily) -> usize {
    let image_default = if matches!(family, PersistentRunnerFamily::Yosys) {
        std::thread::available_parallelism()
            .map(|n| n.get().clamp(1, 8))
            .unwrap_or(8)
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get().clamp(1, 4))
            .unwrap_or(4)
    };
    let family_override = if matches!(family, PersistentRunnerFamily::Yosys) {
        env_usize(PERSISTENT_RUNNER_YOSYS_POOL_SIZE_ENV)
    } else {
        env_usize(PERSISTENT_RUNNER_DRIVER_POOL_SIZE_ENV)
    };
    family_override
        .or_else(|| env_usize(PERSISTENT_RUNNER_POOL_SIZE_ENV))
        .unwrap_or(image_default)
        .clamp(1, 64)
}

fn persistent_runner_idle_ttl_secs() -> u64 {
    env_u64(PERSISTENT_RUNNER_IDLE_TTL_SECS_ENV).unwrap_or(PERSISTENT_RUNNER_DEFAULT_IDLE_TTL_SECS)
}

fn store_root_container_path() -> &'static str {
    "/store-root"
}

fn worker_script_container_path() -> &'static str {
    "/opt/xlsynth/persistent_runner_worker.py"
}

fn read_persistent_runner_capabilities(
    runner_paths: &PersistentRunnerPaths,
) -> Result<PersistentRunnerCapabilities> {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if runner_paths.capabilities_path.exists() {
            let text = fs::read_to_string(&runner_paths.capabilities_path).with_context(|| {
                format!(
                    "reading runner capabilities file: {}",
                    runner_paths.capabilities_path.display()
                )
            })?;
            return serde_json::from_str::<PersistentRunnerCapabilities>(&text).with_context(
                || {
                    format!(
                        "parsing runner capabilities file: {}",
                        runner_paths.capabilities_path.display()
                    )
                },
            );
        }
        thread::sleep(Duration::from_millis(100));
    }
    bail!(
        "persistent runner capabilities did not appear: {}",
        runner_paths.capabilities_path.display()
    );
}

pub(crate) fn driver_runner_supports_subcommand(
    store: &ArtifactStore,
    repo_root: &Path,
    runtime: &DriverRuntimeSpec,
    subcommand: &str,
) -> Result<bool> {
    let capabilities = load_driver_runner_capabilities(store, repo_root, runtime)?;
    Ok(capabilities
        .driver_subcommands
        .get(subcommand)
        .copied()
        .unwrap_or(false))
}

pub(crate) fn driver_runner_subcommand_help_has_token(
    store: &ArtifactStore,
    repo_root: &Path,
    runtime: &DriverRuntimeSpec,
    subcommand: &str,
    token: &str,
) -> Result<bool> {
    let capabilities = load_driver_runner_capabilities(store, repo_root, runtime)?;
    Ok(capabilities
        .driver_help_tokens
        .get(subcommand)
        .and_then(|tokens| tokens.get(token))
        .copied()
        .unwrap_or(false))
}

fn load_driver_runner_capabilities(
    store: &ArtifactStore,
    repo_root: &Path,
    runtime: &DriverRuntimeSpec,
) -> Result<PersistentRunnerCapabilities> {
    ensure_driver_image(repo_root, runtime)?;
    let image = docker_image_content_ref(&runtime.docker_image_id)?;
    let base_runner_key = persistent_runner_key(&image, &store.root);
    let pool_size = persistent_runner_pool_size(PersistentRunnerFamily::Driver);
    let runner_key = persistent_runner_slot_key(&base_runner_key, pool_size, 0);
    let container_name = persistent_runner_container_name(&image, &runner_key);
    let runner_paths = PersistentRunnerPaths::new(&store.root, &runner_key);
    ensure_persistent_runner_started(
        repo_root,
        &store.root,
        &image,
        &runner_key,
        &container_name,
        &runner_paths,
    )?;
    let mut capabilities = read_persistent_runner_capabilities(&runner_paths)?;
    if capabilities.runner_family == "driver" && capabilities.driver_subcommands.is_empty() {
        cleanup_persistent_runner_container(&container_name).ok();
        ensure_persistent_runner_started(
            repo_root,
            &store.root,
            &image,
            &runner_key,
            &container_name,
            &runner_paths,
        )?;
        capabilities = read_persistent_runner_capabilities(&runner_paths)?;
    }
    if capabilities.runner_family != "driver" {
        bail!(
            "persistent runner for image {} did not report driver capabilities (family={})",
            runtime.docker_image,
            capabilities.runner_family
        );
    }
    Ok(capabilities)
}

fn infer_store_root_from_path(path: &Path) -> Option<PathBuf> {
    let mut current = if path.is_dir() {
        path.to_path_buf()
    } else {
        path.parent()?.to_path_buf()
    };
    loop {
        if let Some(name) = current.file_name().and_then(|s| s.to_str()) {
            if name == ".staging"
                || name == ".materialized-actions"
                || name == crate::DRIVER_RELEASE_CACHE_DIR
            {
                return current.parent().map(|p| p.to_path_buf());
            }
        }
        if current.join(".staging").exists() && current.join("queue").exists() {
            return Some(current);
        }
        let Some(parent) = current.parent() else {
            return None;
        };
        if parent == current {
            return None;
        }
        current = parent.to_path_buf();
    }
}

fn infer_store_root_from_mounts(mounts: &[DockerMount]) -> Result<PathBuf> {
    for mount in mounts {
        if let Some(root) = infer_store_root_from_path(&mount.host_path) {
            return Ok(root);
        }
    }
    bail!("unable to infer artifact store root from docker mounts")
}

fn path_to_forward_slashes(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn store_container_path(store_root: &Path, host_path: &Path) -> Option<String> {
    let rel = host_path.strip_prefix(store_root).ok()?;
    let rel = path_to_forward_slashes(rel);
    Some(if rel.is_empty() {
        store_root_container_path().to_string()
    } else {
        format!("{}/{}", store_root_container_path(), rel)
    })
}

fn remove_path_if_exists(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("stat path: {}", path.display()))?;
    if metadata.file_type().is_symlink() || metadata.is_file() {
        fs::remove_file(path).with_context(|| format!("removing file: {}", path.display()))?;
    } else if metadata.is_dir() {
        fs::remove_dir_all(path).with_context(|| format!("removing dir: {}", path.display()))?;
    }
    Ok(())
}

fn copy_path_recursive(src: &Path, dst: &Path) -> Result<()> {
    if src.is_dir() {
        fs::create_dir_all(dst)
            .with_context(|| format!("creating directory copy target: {}", dst.display()))?;
        for entry in WalkDir::new(src).sort_by_file_name() {
            let entry = entry.with_context(|| format!("walking copy source: {}", src.display()))?;
            let path = entry.path();
            let rel = path
                .strip_prefix(src)
                .with_context(|| format!("computing copy relpath: {}", path.display()))?;
            if rel.as_os_str().is_empty() {
                continue;
            }
            let target = dst.join(rel);
            if entry.file_type().is_dir() {
                fs::create_dir_all(&target)
                    .with_context(|| format!("creating copied dir: {}", target.display()))?;
            } else if entry.file_type().is_file() {
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent).with_context(|| {
                        format!("creating copied file parent: {}", parent.display())
                    })?;
                }
                fs::copy(path, &target).with_context(|| {
                    format!("copying file {} -> {}", path.display(), target.display())
                })?;
            }
        }
    } else {
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating copied file parent: {}", parent.display()))?;
        }
        fs::copy(src, dst)
            .with_context(|| format!("copying file {} -> {}", src.display(), dst.display()))?;
    }
    Ok(())
}

fn prepare_mount_request(
    store_root: &Path,
    runner_paths: &PersistentRunnerPaths,
    request_id: &str,
    mount_index: usize,
    mount: &DockerMount,
) -> Result<(PersistentRunnerMountRequest, Option<ExternalWriteback>)> {
    if mount.read_only
        && let Some(source_path) = store_container_path(store_root, &mount.host_path)
    {
        return Ok((
            PersistentRunnerMountRequest {
                source_path,
                target_path: mount.container_path.clone(),
                read_only: mount.read_only,
            },
            None,
        ));
    }

    let aux_root = runner_paths
        .jobs_dir
        .join(request_id)
        .join(if mount.read_only { "aux" } else { "writeback" })
        .join(format!("{mount_index:03}"));
    let host_copy_path = if mount.host_path.is_dir() {
        aux_root.join(
            mount
                .host_path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("dir"),
        )
    } else {
        aux_root.join(
            mount
                .host_path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("file"),
        )
    };
    remove_path_if_exists(&aux_root)?;
    fs::create_dir_all(&aux_root)
        .with_context(|| format!("creating request aux root: {}", aux_root.display()))?;
    if mount.read_only {
        copy_path_recursive(&mount.host_path, &host_copy_path)?;
    } else if mount.host_path.exists() {
        copy_path_recursive(&mount.host_path, &host_copy_path)?;
    } else if mount.host_path.is_dir() {
        fs::create_dir_all(&host_copy_path).with_context(|| {
            format!(
                "creating writeback mount request target dir: {}",
                host_copy_path.display()
            )
        })?;
    } else if let Some(parent) = host_copy_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating writeback parent: {}", parent.display()))?;
    }
    let container_source_path =
        store_container_path(store_root, &host_copy_path).ok_or_else(|| {
            anyhow!(
                "unable to translate request mount path {}",
                host_copy_path.display()
            )
        })?;
    let writeback = if mount.read_only {
        None
    } else {
        Some(ExternalWriteback {
            host_path: mount.host_path.clone(),
            job_path: host_copy_path.clone(),
        })
    };
    Ok((
        PersistentRunnerMountRequest {
            source_path: container_source_path,
            target_path: mount.container_path.clone(),
            read_only: mount.read_only,
        },
        writeback,
    ))
}

fn sync_external_writeback(writeback: &ExternalWriteback) -> Result<()> {
    remove_path_if_exists(&writeback.host_path)?;
    copy_path_recursive(&writeback.job_path, &writeback.host_path)
}

fn ensure_worker_script_path(repo_root: &Path) -> Result<PathBuf> {
    let path = repo_root.join(PERSISTENT_RUNNER_WORKER_SCRIPT);
    if !path.exists() {
        bail!(
            "persistent runner worker script not found: {}",
            path.display()
        );
    }
    path.canonicalize()
        .with_context(|| format!("canonicalizing worker script path: {}", path.display()))
}

fn docker_container_is_running(container_name: &str) -> Result<bool> {
    let output = Command::new("docker")
        .args(["inspect", "--format", "{{.State.Running}}", container_name])
        .output()
        .with_context(|| format!("inspecting docker container: {container_name}"))?;
    if !output.status.success() {
        return Ok(false);
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim() == "true")
}

fn docker_container_image_id(container_name: &str) -> Result<Option<String>> {
    let output = Command::new("docker")
        .args(["inspect", "--format", "{{.Image}}", container_name])
        .output()
        .with_context(|| format!("inspecting docker container image: {container_name}"))?;
    if !output.status.success() {
        return Ok(None);
    }
    Ok(Some(normalize_docker_image_id(
        String::from_utf8(output.stdout)
            .context("decoding docker container image ID")?
            .trim(),
    )?))
}

fn count_json_files(path: &Path) -> Result<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let mut count = 0;
    for entry in
        fs::read_dir(path).with_context(|| format!("reading runner dir: {}", path.display()))?
    {
        let entry = entry.with_context(|| format!("reading runner entry: {}", path.display()))?;
        let entry_path = entry.path();
        if entry_path.is_file() && entry_path.extension().and_then(|s| s.to_str()) == Some("json") {
            count += 1;
        }
    }
    Ok(count)
}

fn persistent_runner_depth(paths: &PersistentRunnerPaths) -> Result<usize> {
    Ok(count_json_files(&paths.inbox_dir)? + count_json_files(&paths.processing_dir)?)
}

fn read_persistent_runner_heartbeat(
    paths: &PersistentRunnerPaths,
) -> Result<Option<PersistentRunnerHeartbeat>> {
    if !paths.heartbeat_path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(&paths.heartbeat_path).with_context(|| {
        format!(
            "reading runner heartbeat: {}",
            paths.heartbeat_path.display()
        )
    })?;
    serde_json::from_str::<PersistentRunnerHeartbeat>(&text)
        .map(Some)
        .with_context(|| {
            format!(
                "parsing runner heartbeat: {}",
                paths.heartbeat_path.display()
            )
        })
}

fn persistent_runner_is_idle(paths: &PersistentRunnerPaths) -> Result<bool> {
    if persistent_runner_depth(paths)? != 0 {
        return Ok(false);
    }
    let Some(heartbeat) = read_persistent_runner_heartbeat(paths)? else {
        return Ok(false);
    };
    Ok(heartbeat.state == "idle" && heartbeat.current_request_id.is_none())
}

fn persistent_runner_heartbeat_age_secs(paths: &PersistentRunnerPaths) -> Option<u64> {
    let metadata = fs::metadata(&paths.heartbeat_path).ok()?;
    let modified = metadata.modified().ok()?;
    std::time::SystemTime::now()
        .duration_since(modified)
        .ok()
        .map(|age| age.as_secs())
}

fn cleanup_persistent_runner_container(container_name: &str) -> Result<()> {
    let output = Command::new("docker")
        .args(["rm", "-f", container_name])
        .output()
        .with_context(|| format!("removing persistent runner container: {container_name}"))?;
    if output.status.success()
        || String::from_utf8_lossy(&output.stderr).contains("No such container")
    {
        return Ok(());
    }
    bail!(
        "failed removing persistent runner container {}: {}",
        container_name,
        first_line(&String::from_utf8_lossy(&output.stderr))
    );
}

fn ensure_persistent_runner_started(
    repo_root: &Path,
    store_root: &Path,
    image: &str,
    runner_key: &str,
    container_name: &str,
    runner_paths: &PersistentRunnerPaths,
) -> Result<()> {
    let _guard = persistent_runner_start_lock()
        .lock()
        .map_err(|_| anyhow!("persistent runner start lock poisoned"))?;
    ensure_persistent_runner_started_locked(
        repo_root,
        store_root,
        image,
        runner_key,
        container_name,
        runner_paths,
    )
}

fn ensure_persistent_runner_started_locked(
    repo_root: &Path,
    store_root: &Path,
    image: &str,
    runner_key: &str,
    container_name: &str,
    runner_paths: &PersistentRunnerPaths,
) -> Result<()> {
    if docker_container_is_running(container_name)? {
        let expected_image_id = inspect_image_id(image)?
            .with_context(|| format!("persistent runner image `{image}` is missing"))?;
        if docker_container_image_id(container_name)?.as_deref() == Some(expected_image_id.as_str())
        {
            return Ok(());
        }
        cleanup_persistent_runner_container(container_name)?;
    }

    cleanup_persistent_runner_container(container_name).ok();
    runner_paths.ensure_layout()?;
    runner_paths.reset_request_state()?;

    let store_root = store_root
        .canonicalize()
        .with_context(|| format!("canonicalizing store root: {}", store_root.display()))?;
    let staging_root = store_root.join(".staging");
    fs::create_dir_all(&staging_root)
        .with_context(|| format!("creating staging root: {}", staging_root.display()))?;
    let persistent_root = store_root.join(PERSISTENT_RUNNER_ROOT_DIR);
    fs::create_dir_all(&persistent_root).with_context(|| {
        format!(
            "creating persistent runner root directory: {}",
            persistent_root.display()
        )
    })?;
    let worker_script = ensure_worker_script_path(repo_root)?;
    let instance_id = persistent_runner_instance_id(runner_key);
    let runner_root_container = format!(
        "{}/{}/{}/{}",
        store_root_container_path(),
        PERSISTENT_RUNNER_ROOT_DIR,
        PERSISTENT_RUNNER_RUNNERS_DIR,
        runner_key
    );

    let args: Vec<OsString> = vec![
        OsString::from("run"),
        OsString::from("-d"),
        OsString::from("--name"),
        OsString::from(container_name.to_string()),
        OsString::from("--pull"),
        OsString::from("never"),
        OsString::from("--network"),
        OsString::from("none"),
        OsString::from("-e"),
        OsString::from("PYTHONUNBUFFERED=1"),
        OsString::from("-v"),
        OsString::from(format!(
            "{}:{}:ro",
            store_root.display(),
            store_root_container_path()
        )),
        OsString::from("-v"),
        OsString::from(format!(
            "{}:{}/.staging",
            staging_root.display(),
            store_root_container_path()
        )),
        OsString::from("-v"),
        OsString::from(format!(
            "{}:{}/{}",
            persistent_root.display(),
            store_root_container_path(),
            PERSISTENT_RUNNER_ROOT_DIR
        )),
        OsString::from("-v"),
        OsString::from(format!(
            "{}:{}:ro",
            worker_script.display(),
            worker_script_container_path()
        )),
        OsString::from(image.to_string()),
        OsString::from("python3"),
        OsString::from(worker_script_container_path()),
        OsString::from("--store-root"),
        OsString::from(store_root_container_path()),
        OsString::from("--runner-root"),
        OsString::from(runner_root_container),
        OsString::from("--runner-key"),
        OsString::from(runner_key.to_string()),
        OsString::from("--runner-instance-id"),
        OsString::from(instance_id),
        OsString::from("--container-name"),
        OsString::from(container_name.to_string()),
        OsString::from("--image"),
        OsString::from(image.to_string()),
    ];
    let output = Command::new("docker")
        .args(&args)
        .current_dir(repo_root)
        .output()
        .context("starting persistent runner container")?;
    if !output.status.success() {
        bail!(
            "failed starting persistent runner container {} for image {}: stdout={} stderr={}",
            container_name,
            image,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if runner_paths.heartbeat_path.exists() && docker_container_is_running(container_name)? {
            return Ok(());
        }
        if !docker_container_is_running(container_name)? {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    bail!(
        "persistent runner container {} did not become healthy for image {}: {}",
        container_name,
        image,
        first_line(&stderr)
    );
}

fn persistent_runner_slot_status(
    store_root: &Path,
    image: &str,
    base_runner_key: &str,
    pool_size: usize,
    slot: usize,
) -> Result<PersistentRunnerSlotStatus> {
    let runner_key = persistent_runner_slot_key(base_runner_key, pool_size, slot);
    let container_name = persistent_runner_container_name(image, &runner_key);
    let paths = PersistentRunnerPaths::new(store_root, &runner_key);
    let running = docker_container_is_running(&container_name)?;
    let depth = if running {
        persistent_runner_depth(&paths)?
    } else {
        0
    };
    let idle = running && persistent_runner_is_idle(&paths)?;
    Ok(PersistentRunnerSlotStatus {
        slot,
        runner_key,
        container_name,
        paths,
        running,
        depth,
        idle,
    })
}

fn cleanup_idle_persistent_runner_pool_slots(
    store_root: &Path,
    image: &str,
    base_runner_key: &str,
    pool_size: usize,
) -> Result<()> {
    if pool_size <= 1 {
        return Ok(());
    }
    let ttl_secs = persistent_runner_idle_ttl_secs();
    if ttl_secs == 0 {
        return Ok(());
    }
    for slot in 1..pool_size {
        let status =
            persistent_runner_slot_status(store_root, image, base_runner_key, pool_size, slot)?;
        if !status.running || !status.idle {
            continue;
        }
        if persistent_runner_heartbeat_age_secs(&status.paths).unwrap_or(0) < ttl_secs {
            continue;
        }
        cleanup_persistent_runner_container(&status.container_name).ok();
    }
    Ok(())
}

fn select_persistent_runner_locked(
    repo_root: &Path,
    store_root: &Path,
    image: &str,
    family: PersistentRunnerFamily,
) -> Result<SelectedPersistentRunner> {
    let base_runner_key = persistent_runner_key(image, store_root);
    let pool_size = persistent_runner_pool_size(family);
    cleanup_idle_persistent_runner_pool_slots(store_root, image, &base_runner_key, pool_size)?;

    let mut statuses = Vec::with_capacity(pool_size);
    for slot in 0..pool_size {
        statuses.push(persistent_runner_slot_status(
            store_root,
            image,
            &base_runner_key,
            pool_size,
            slot,
        )?);
    }

    if let Some(status) = statuses.iter().find(|status| status.idle) {
        return Ok(SelectedPersistentRunner {
            runner_key: status.runner_key.clone(),
            runner_instance_id: persistent_runner_instance_id(&status.runner_key),
            container_name: status.container_name.clone(),
            paths: status.paths.clone(),
        });
    }

    if let Some(status) = statuses.iter().find(|status| !status.running) {
        ensure_persistent_runner_started_locked(
            repo_root,
            store_root,
            image,
            &status.runner_key,
            &status.container_name,
            &status.paths,
        )?;
        return Ok(SelectedPersistentRunner {
            runner_key: status.runner_key.clone(),
            runner_instance_id: persistent_runner_instance_id(&status.runner_key),
            container_name: status.container_name.clone(),
            paths: status.paths.clone(),
        });
    }

    let status = statuses
        .iter()
        .min_by_key(|status| (status.depth, status.slot))
        .ok_or_else(|| anyhow!("persistent runner pool has no slots"))?;
    Ok(SelectedPersistentRunner {
        runner_key: status.runner_key.clone(),
        runner_instance_id: persistent_runner_instance_id(&status.runner_key),
        container_name: status.container_name.clone(),
        paths: status.paths.clone(),
    })
}

pub(crate) fn execute_persistent_runner_script(
    image: &str,
    mounts: &[DockerMount],
    env: &BTreeMap<String, String>,
    script: &str,
    run_hint: &str,
) -> Result<CommandTrace> {
    execute_persistent_runner_script_with_timeout(
        image,
        mounts,
        env,
        script,
        run_hint,
        DEFAULT_ACTION_TIMEOUT_SECONDS,
    )
}

pub(crate) fn execute_yosys_persistent_runner_script(
    image: &str,
    mounts: &[DockerMount],
    env: &BTreeMap<String, String>,
    script: &str,
    run_hint: &str,
) -> Result<CommandTrace> {
    execute_persistent_runner_script_with_timeout_and_family(
        image,
        mounts,
        env,
        script,
        run_hint,
        DEFAULT_ACTION_TIMEOUT_SECONDS,
        PersistentRunnerFamily::Yosys,
    )
}

pub(crate) fn execute_persistent_runner_script_with_timeout(
    image: &str,
    mounts: &[DockerMount],
    env: &BTreeMap<String, String>,
    script: &str,
    run_hint: &str,
    timeout_secs: u64,
) -> Result<CommandTrace> {
    execute_persistent_runner_script_with_timeout_and_family(
        image,
        mounts,
        env,
        script,
        run_hint,
        timeout_secs,
        PersistentRunnerFamily::Driver,
    )
}

fn execute_persistent_runner_script_with_timeout_and_family(
    image: &str,
    mounts: &[DockerMount],
    env: &BTreeMap<String, String>,
    script: &str,
    run_hint: &str,
    timeout_secs: u64,
    family: PersistentRunnerFamily,
) -> Result<CommandTrace> {
    let repo_root = std::env::current_dir().context("getting current directory")?;
    let store_root = infer_store_root_from_mounts(mounts)?;
    let request_seq = DOCKER_RUN_NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    let (request_id, request_token) =
        persistent_runner_request_identity(process_instance_id(), request_seq);

    let (runner, result_path, command_argv, writebacks) = {
        let _guard = persistent_runner_start_lock()
            .lock()
            .map_err(|_| anyhow!("persistent runner start lock poisoned"))?;
        let runner = select_persistent_runner_locked(&repo_root, &store_root, image, family)?;
        let job_root = runner.paths.jobs_dir.join(&request_id);
        fs::create_dir_all(&job_root).with_context(|| {
            format!("creating persistent runner job dir: {}", job_root.display())
        })?;
        let mut request_mounts = Vec::with_capacity(mounts.len());
        let mut writebacks = Vec::new();
        for (index, mount) in mounts.iter().enumerate() {
            let (request_mount, writeback) =
                prepare_mount_request(&store_root, &runner.paths, &request_id, index, mount)?;
            request_mounts.push(request_mount);
            if let Some(writeback) = writeback {
                writebacks.push(writeback);
            }
        }

        let request = PersistentRunnerRequest {
            schema_version: PERSISTENT_RUNNER_SCHEMA_VERSION,
            request_id: request_id.clone(),
            request_token: request_token.clone(),
            runner_key: runner.runner_key.clone(),
            runner_instance_id: runner.runner_instance_id.clone(),
            container_name: runner.container_name.clone(),
            image: image.to_string(),
            job_root: store_container_path(&store_root, &job_root).ok_or_else(|| {
                anyhow!("unable to translate job root path {}", job_root.display())
            })?,
            heartbeat_path: store_container_path(&store_root, &runner.paths.heartbeat_path)
                .ok_or_else(|| {
                    anyhow!(
                        "unable to translate runner heartbeat path {}",
                        runner.paths.heartbeat_path.display()
                    )
                })?,
            timeout_secs,
            env: env.clone(),
            script: script.to_string(),
            mounts: request_mounts,
        };
        let request_json = serde_json::to_string_pretty(&request)
            .context("serializing persistent runner request")?;
        let request_path = job_root.join("request.json");
        fs::write(&request_path, &request_json)
            .with_context(|| format!("writing request file: {}", request_path.display()))?;
        let inbox_path = runner.paths.inbox_dir.join(format!("{request_id}.json"));
        let inbox_tmp = runner
            .paths
            .inbox_dir
            .join(format!(".{request_id}.tmp-{}", std::process::id()));
        fs::write(&inbox_tmp, &request_json)
            .with_context(|| format!("writing request inbox temp file: {}", inbox_tmp.display()))?;
        fs::rename(&inbox_tmp, &inbox_path).with_context(|| {
            format!(
                "publishing request into runner inbox: {} -> {}",
                inbox_tmp.display(),
                inbox_path.display()
            )
        })?;

        let result_path = runner.paths.results_dir.join(format!("{request_id}.json"));
        let command_argv = vec![
            "persistent-runner".to_string(),
            "--image".to_string(),
            image.to_string(),
            "--container".to_string(),
            runner.container_name.clone(),
            "--request-id".to_string(),
            request_id.clone(),
            "--run-hint".to_string(),
            run_hint.to_string(),
        ];
        (runner, result_path, command_argv, writebacks)
    };

    let deadline = Instant::now()
        + Duration::from_secs(timeout_secs + PERSISTENT_RUNNER_REQUEST_TIMEOUT_GRACE_SECS);
    let result = loop {
        if result_path.exists() {
            let text = fs::read_to_string(&result_path).with_context(|| {
                format!("reading runner result file: {}", result_path.display())
            })?;
            break serde_json::from_str::<PersistentRunnerResult>(&text).with_context(|| {
                format!("parsing runner result file: {}", result_path.display())
            })?;
        }
        if !docker_container_is_running(&runner.container_name)? {
            bail!(
                "persistent runner container exited before producing result (container={} request_id={})",
                runner.container_name,
                request_id
            );
        }
        if let Ok(metadata) = fs::metadata(&runner.paths.heartbeat_path)
            && let Ok(modified) = metadata.modified()
            && let Ok(age) = std::time::SystemTime::now().duration_since(modified)
            && age.as_secs() > PERSISTENT_RUNNER_HEARTBEAT_STALE_SECS
        {
            let cleanup_summary = cleanup_timed_out_container(&runner.container_name);
            bail!(
                "persistent runner heartbeat stale for request {} (container={}) cleanup: {}",
                request_id,
                runner.container_name,
                cleanup_summary
            );
        }
        if Instant::now() >= deadline {
            let cleanup_summary = cleanup_timed_out_container(&runner.container_name);
            bail!(
                "TIMEOUT({}) waiting for persistent runner result (container={} request_id={}) cleanup: {}",
                timeout_secs,
                runner.container_name,
                request_id,
                cleanup_summary
            );
        }
        thread::sleep(Duration::from_millis(100));
    };

    let cleanup_request_files = || {
        fs::remove_file(&result_path).ok();
        fs::remove_file(runner.paths.archive_dir.join(format!("{request_id}.json"))).ok();
        remove_path_if_exists(&runner.paths.jobs_dir.join(&request_id)).ok();
    };
    if result.request_id != request_id || result.request_token != request_token {
        cleanup_request_files();
        bail!(
            "persistent runner result identity mismatch for request {}",
            request_id
        );
    }

    let writeback_result = writebacks.iter().try_for_each(sync_external_writeback);
    cleanup_request_files();
    writeback_result?;
    if result.status != "completed" || result.exit_code.unwrap_or(1) != 0 || result.timed_out {
        let stderr_first = first_line(&result.stderr_tail).trim();
        let stdout_first = first_line(&result.stdout_tail).trim();
        let root_cause = result
            .error
            .as_deref()
            .filter(|s| !s.is_empty())
            .or_else(|| {
                if !stderr_first.is_empty() {
                    Some(stderr_first)
                } else {
                    None
                }
            })
            .or_else(|| {
                if !stdout_first.is_empty() {
                    Some(stdout_first)
                } else {
                    None
                }
            })
            .unwrap_or("no stderr/stdout details");
        bail!(
            "persistent runner request failed (exit={:?}): {}\ncommand:\n{}\nstdout:\n{}\nstderr:\n{}",
            result.exit_code,
            root_cause,
            result.command_argv.join(" "),
            result.stdout_tail,
            result.stderr_tail
        );
    }

    Ok(CommandTrace {
        argv: command_argv,
        exit_code: 0,
    })
}

#[cfg(test)]
#[derive(Debug)]
struct BoundedOutputCapture {
    bytes: Vec<u8>,
    truncated: bool,
}

#[cfg(test)]
fn read_bounded_output_tail(
    reader: &mut dyn Read,
    max_bytes: usize,
) -> std::io::Result<BoundedOutputCapture> {
    let mut out = Vec::new();
    let mut truncated = false;
    let mut chunk = [0_u8; 8192];
    loop {
        let count = reader.read(&mut chunk)?;
        if count == 0 {
            break;
        }
        out.extend_from_slice(&chunk[..count]);
        if out.len() > max_bytes {
            let overflow = out.len() - max_bytes;
            out.drain(0..overflow);
            truncated = true;
        }
    }
    Ok(BoundedOutputCapture {
        bytes: out,
        truncated,
    })
}

#[cfg(test)]
fn bounded_capture_to_string(capture: BoundedOutputCapture, max_bytes: usize) -> String {
    let decoded = String::from_utf8_lossy(&capture.bytes);
    if capture.truncated {
        format!("[truncated to last {} bytes]\n{}", max_bytes, decoded)
    } else {
        decoded.into_owned()
    }
}

pub(crate) fn cleanup_timed_out_container(container_name: &str) -> String {
    let rm_args = vec![
        "rm".to_string(),
        "-f".to_string(),
        container_name.to_string(),
    ];
    let rm_output = Command::new("docker").args(&rm_args).output();
    match rm_output {
        Ok(output) => {
            if output.status.success() {
                "removed timed-out container".to_string()
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                if stderr.contains("No such container") {
                    "container already absent".to_string()
                } else {
                    format!(
                        "cleanup_failed(exit={:?}, stderr={})",
                        output.status.code(),
                        first_line(&stderr)
                    )
                }
            }
        }
        Err(err) => format!("cleanup_failed(error={})", err),
    }
}

pub(crate) fn first_line(text: &str) -> &str {
    text.lines().next().unwrap_or("")
}

pub(crate) fn os_args_to_string(program: &str, args: &[OsString]) -> Vec<String> {
    let mut out = Vec::with_capacity(args.len() + 1);
    out.push(program.to_string());
    for arg in args {
        out.push(arg.to_string_lossy().to_string());
    }
    out
}

pub(crate) fn collect_output_files(payload_dir: &Path) -> Result<Vec<OutputFile>> {
    let mut files = Vec::new();
    for entry in WalkDir::new(payload_dir).sort_by_file_name() {
        let entry = entry.context("walking payload tree")?;
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        let rel = path
            .strip_prefix(payload_dir)
            .with_context(|| format!("stripping prefix for path: {}", path.display()))?;
        let rel = rel.to_string_lossy().replace('\\', "/");
        let metadata = entry
            .metadata()
            .with_context(|| format!("reading metadata: {}", path.display()))?;
        files.push(OutputFile {
            path: rel,
            bytes: metadata.len(),
            sha256: sha256_file(path)?,
        });
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(files)
}

pub(crate) fn sha256_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)
        .with_context(|| format!("opening file for sha256: {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let count = file
            .read(&mut buffer)
            .with_context(|| format!("reading file for sha256: {}", path.display()))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::drain_queue;
    use crate::executor::compute_action_id;
    use crate::model::{
        ActionSpec, ArtifactRef, ArtifactType, CommandTrace, DriverRuntimeSpec, G8rLoweringMode,
        Provenance, ScriptRef, YosysRuntimeSpec,
    };
    use crate::queue::{enqueue_action, list_queue_files};
    use crate::store::ArtifactStore;
    use chrono::Utc;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use std::collections::BTreeSet;
    use std::io::Cursor;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_temp_store_root(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-runtime-docker-test-{}-{}-{}",
            label,
            std::process::id(),
            nanos
        ));
        root
    }

    #[test]
    fn bounded_output_capture_keeps_tail_and_marks_truncated() {
        let mut input = Cursor::new(b"abcdefghij".to_vec());
        let capture = read_bounded_output_tail(&mut input, 4).expect("capture should succeed");
        assert!(capture.truncated);
        assert_eq!(capture.bytes, b"ghij");
        let rendered = bounded_capture_to_string(capture, 4);
        assert!(rendered.starts_with("[truncated to last 4 bytes]"));
        assert!(rendered.ends_with("ghij"));
    }

    #[test]
    fn bounded_output_capture_without_truncation_is_plain_text() {
        let mut input = Cursor::new(b"hello".to_vec());
        let capture = read_bounded_output_tail(&mut input, 32).expect("capture should succeed");
        assert!(!capture.truncated);
        assert_eq!(capture.bytes, b"hello");
        assert_eq!(bounded_capture_to_string(capture, 32), "hello");
    }

    #[test]
    fn infer_store_root_from_path_returns_parent_of_staging_tree() {
        let root = make_temp_store_root("infer-store-root");
        let payload = root.join(".staging/job-1/payload");
        fs::create_dir_all(&payload).expect("create payload dir");
        let inferred = infer_store_root_from_path(&payload).expect("infer store root");
        assert_eq!(inferred, root);
        fs::remove_dir_all(inferred).ok();
    }

    #[test]
    fn store_container_path_translates_host_store_paths() {
        let root = make_temp_store_root("store-container-path");
        let artifact = root.join(".staging/job-2/payload/result.aig");
        if let Some(parent) = artifact.parent() {
            fs::create_dir_all(parent).expect("create artifact parent");
        }
        fs::write(&artifact, "aig").expect("write artifact");
        let translated = store_container_path(&root, &artifact).expect("translate path");
        assert_eq!(translated, "/store-root/.staging/job-2/payload/result.aig");
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn persistent_runner_request_identity_is_process_incarnation_unique() {
        let (first_id, first_token) = persistent_runner_request_identity(&"a".repeat(64), 0);
        let (second_id, second_token) = persistent_runner_request_identity(&"b".repeat(64), 0);
        assert_ne!(first_id, second_id);
        assert_ne!(first_token, second_token);
    }

    #[test]
    fn driver_release_cache_manifest_rejects_truncated_content() {
        let root = make_temp_store_root("cache-manifest-truncation");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure store");
        let version = "v0.39.0";
        let platform = crate::DEFAULT_RELEASE_PLATFORM;
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let expected_input_sha256 = driver_release_cache_input_sha256(repo_root, version, platform)
            .expect("cache input digest");
        seed_fake_driver_release_cache(&store, version, platform).expect("seed cache");
        let cache_dir = store
            .driver_release_cache_dir(&expected_input_sha256)
            .expect("digest-addressed cache dir");
        validate_driver_release_cache(&cache_dir, version, platform, &expected_input_sha256)
            .expect("seeded cache validates");

        fs::write(cache_dir.join("delay_info_main"), b"x").expect("truncate cached binary");
        assert!(
            !driver_release_cache_is_ready(&cache_dir, version, platform, &expected_input_sha256,),
            "a cache mutation after prior validation must be detected"
        );
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn invalid_digest_addressed_cache_is_never_replaced_in_place() {
        let root = make_temp_store_root("cache-immutable-generation");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure store");
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let version = "v0.39.0";
        let platform = crate::DEFAULT_RELEASE_PLATFORM;
        let expected_input_sha256 = driver_release_cache_input_sha256(repo_root, version, platform)
            .expect("cache input digest");
        let cache_dir = store
            .driver_release_cache_dir(&expected_input_sha256)
            .expect("digest-addressed cache dir");
        fs::create_dir_all(&cache_dir).expect("create invalid immutable generation");
        let marker = cache_dir.join("operator-marker");
        fs::write(&marker, "preserve").expect("write marker");

        let error = ensure_driver_release_cache(
            &store,
            repo_root,
            version,
            platform,
            &expected_input_sha256,
        )
        .expect_err("invalid immutable generation must fail closed");
        assert!(format!("{error:#}").contains("present but invalid"));
        assert_eq!(fs::read_to_string(marker).expect("read marker"), "preserve");

        let other = store
            .driver_release_cache_dir(&"e".repeat(64))
            .expect("other digest-addressed cache dir");
        assert_ne!(cache_dir, other);
        assert!(store.driver_release_cache_dir("../escape").is_err());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    #[ignore = "requires network"]
    fn release_cache_input_manifest_resolves_live_checksums() {
        let version = "v0.45.0";
        let platform = crate::DEFAULT_RELEASE_PLATFORM;
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let inputs = fetch_release_cache_input_manifest(repo_root, version, platform)
            .expect("resolve live inputs");
        validate_driver_release_cache_inputs(&inputs, version, platform)
            .expect("validate live inputs");
        assert_eq!(inputs.files.len(), 11);
        assert_eq!(driver_release_cache_input_sha256_hex(&inputs).len(), 64);
    }

    #[test]
    fn live_cache_setup_lock_is_not_reaped_by_age() {
        let root = make_temp_store_root("cache-lock-owner");
        fs::create_dir_all(&root).expect("create lock test root");
        let lock_path = root.join("setup.lock");
        let start_ticks = process_start_ticks(std::process::id()).expect("process start ticks");
        let mut lock = crate::proto::v1::DriverReleaseCacheSetupLock {
            record_version: 1,
            hostname: std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".to_string()),
            pid: std::process::id(),
            process_start_ticks: start_ticks,
        };
        fs::write(&lock_path, lock.encode_to_vec()).expect("write live lock");
        assert!(!is_stale_cache_setup_lock(&lock_path, 0));

        lock.process_start_ticks = start_ticks.saturating_add(1);
        fs::write(&lock_path, lock.encode_to_vec()).expect("write reused-pid lock");
        assert!(is_stale_cache_setup_lock(&lock_path, u64::MAX));
        fs::remove_dir_all(root).ok();
    }

    static DOCKER_INTEGRATION_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn docker_integration_test_lock() -> &'static Mutex<()> {
        DOCKER_INTEGRATION_TEST_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn unique_test_token(label: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        format!("{label}-{}-{nanos}", std::process::id())
    }

    fn fake_driver_runtime(driver_version: &str, image: String) -> DriverRuntimeSpec {
        DriverRuntimeSpec {
            driver_version: driver_version.to_string(),
            release_platform: crate::DEFAULT_RELEASE_PLATFORM.to_string(),
            docker_image: image,
            dockerfile: "testdata/persistent_runners/fake-driver.Dockerfile".to_string(),
            dockerfile_sha256: hex::encode(Sha256::digest(include_bytes!(
                "../../testdata/persistent_runners/fake-driver.Dockerfile"
            ))),
            docker_image_id: String::new(),
            release_cache_input_sha256: driver_release_cache_input_sha256(
                Path::new(env!("CARGO_MANIFEST_DIR")),
                "v0.39.0",
                crate::DEFAULT_RELEASE_PLATFORM,
            )
            .expect("test cache input digest"),
        }
    }

    fn fake_yosys_runtime(image: String) -> YosysRuntimeSpec {
        YosysRuntimeSpec {
            docker_image: image,
            dockerfile: "testdata/persistent_runners/fake-yosys.Dockerfile".to_string(),
            dockerfile_sha256: hex::encode(Sha256::digest(include_bytes!(
                "../../testdata/persistent_runners/fake-yosys.Dockerfile"
            ))),
            docker_image_id: String::new(),
            upstream_commit: Some(crate::DEFAULT_YOSYS_UPSTREAM_COMMIT.to_string()),
        }
    }

    #[test]
    fn runtime_fingerprints_bind_dockerfile_digest_and_yosys_commit() {
        let driver = fake_driver_runtime("0.47.0", "driver:test".to_string());
        let driver_fingerprint = driver_runtime_fingerprint(&driver).expect("driver fingerprint");
        let mut changed_driver = driver.clone();
        changed_driver.dockerfile_sha256 = "e".repeat(64);
        assert_ne!(
            driver_fingerprint,
            driver_runtime_fingerprint(&changed_driver).expect("changed driver fingerprint")
        );
        let mut changed_cache = driver.clone();
        changed_cache.release_cache_input_sha256 = "d".repeat(64);
        assert_ne!(
            driver_fingerprint,
            driver_runtime_fingerprint(&changed_cache).expect("changed cache fingerprint")
        );

        let yosys = fake_yosys_runtime("yosys:test".to_string());
        let yosys_fingerprint = yosys_runtime_fingerprint(&yosys).expect("yosys fingerprint");
        let mut changed_yosys = yosys.clone();
        changed_yosys.upstream_commit =
            Some("abcdef0123456789abcdef0123456789abcdef01".to_string());
        assert_ne!(
            yosys_fingerprint,
            yosys_runtime_fingerprint(&changed_yosys).expect("changed yosys fingerprint")
        );
    }

    #[test]
    fn yosys_build_args_pass_exact_declared_commit_and_fingerprint() {
        let runtime = fake_yosys_runtime("yosys:test".to_string());
        let fingerprint = yosys_runtime_fingerprint(&runtime).expect("yosys fingerprint");
        let build_ref =
            fingerprint_qualified_image_ref(&runtime.docker_image, "yosys", &fingerprint)
                .expect("qualified image ref");
        let args = yosys_image_build_args(
            &runtime,
            PathBuf::from(&runtime.dockerfile),
            &fingerprint,
            &build_ref,
        )
        .expect("yosys build args")
        .into_iter()
        .map(|value| value.to_string_lossy().to_string())
        .collect::<Vec<_>>();
        let commit = runtime.upstream_commit.as_deref().expect("commit");
        assert!(args.contains(&format!("YOSYS_COMMIT={commit}")));
        assert!(args.contains(&format!("YOSYS_COMMIT_PREFIX={}", &commit[..8])));
        assert!(args.contains(&format!("BVC_RUNTIME_FINGERPRINT={fingerprint}")));
        assert!(args.contains(&build_ref));

        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        checked_runtime_dockerfile(root, &runtime.dockerfile, &runtime.dockerfile_sha256)
            .expect("matching Dockerfile digest");
        let error = checked_runtime_dockerfile(root, &runtime.dockerfile, &"0".repeat(64))
            .expect_err("mismatched Dockerfile digest must fail");
        assert!(error.to_string().contains("Dockerfile digest mismatch"));
    }

    fn docker_available() -> bool {
        Command::new("docker")
            .args(["version", "--format", "{{.Server.Version}}"])
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    fn docker_container_id(container_name: &str) -> Result<String> {
        let output = Command::new("docker")
            .args(["inspect", "--format", "{{.Id}}", container_name])
            .output()
            .with_context(|| format!("inspecting docker container id: {container_name}"))?;
        if !output.status.success() {
            bail!(
                "failed to inspect docker container {}: {}",
                container_name,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    #[cfg(unix)]
    fn set_executable(path: &Path) -> Result<()> {
        let mut permissions = fs::metadata(path)
            .with_context(|| format!("reading metadata for {}", path.display()))?
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions)
            .with_context(|| format!("setting executable bit on {}", path.display()))
    }

    #[cfg(not(unix))]
    fn set_executable(_path: &Path) -> Result<()> {
        Ok(())
    }

    fn fake_driver_release_cache_hash(ir_text: &str, top: &str) -> String {
        hex::encode(Sha256::digest(format!("{ir_text}\n{top}").as_bytes()))
    }

    fn make_script_ref(repo_root: &Path, relpath: &str) -> Result<ScriptRef> {
        let path = repo_root.join(relpath);
        Ok(ScriptRef {
            path: relpath.to_string(),
            sha256: sha256_file(&path)?,
        })
    }

    fn write_completed_action<F>(
        store: &ArtifactStore,
        action: ActionSpec,
        artifact_type: ArtifactType,
        relpath: &str,
        details: serde_json::Value,
        dependencies: Vec<ArtifactRef>,
        payload_writer: F,
    ) -> Result<String>
    where
        F: FnOnce(&Path) -> Result<()>,
    {
        let action_id = compute_action_id(&action)?;
        let staging_dir = store.staging_dir().join(format!("{action_id}-seed"));
        let payload_dir = staging_dir.join("payload");
        fs::create_dir_all(&payload_dir)
            .with_context(|| format!("creating payload dir: {}", payload_dir.display()))?;
        payload_writer(&payload_dir)?;
        let output_files = collect_output_files(&payload_dir)?;
        let provenance = Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: action_id.clone(),
            created_utc: Utc::now(),
            action,
            dependencies,
            output_artifact: ArtifactRef {
                action_id: action_id.clone(),
                artifact_type,
                relpath: relpath.to_string(),
            },
            output_files,
            commands: vec![CommandTrace {
                argv: vec!["seed".to_string()],
                exit_code: 0,
            }],
            details,
            suggested_next_actions: Vec::new(),
        };
        fs::write(
            staging_dir.join("provenance.pb"),
            crate::proto::encode_provenance(&provenance)
                .context("encoding seeded protobuf provenance")?,
        )
        .with_context(|| format!("writing seeded provenance for {}", action_id))?;
        store.promote_staging_action_dir(&action_id, &staging_dir)?;
        Ok(action_id)
    }

    fn seed_fake_driver_release_cache(
        store: &ArtifactStore,
        version: &str,
        platform: &str,
    ) -> Result<()> {
        let repo_root = std::env::current_dir().context("getting repo root for fake cache")?;
        let inputs = resolve_driver_release_cache_input_manifest(&repo_root, version, platform)?;
        let expected_input_sha256 = driver_release_cache_input_sha256_hex(&inputs);
        let cache_dir = store.driver_release_cache_dir(&expected_input_sha256)?;
        fs::create_dir_all(&cache_dir)
            .with_context(|| format!("creating fake driver cache dir: {}", cache_dir.display()))?;
        let delay_info_main = cache_dir.join("delay_info_main");
        fs::copy(
            repo_root.join("testdata/persistent_runners/fake_delay_info_main.py"),
            &delay_info_main,
        )
        .with_context(|| {
            format!(
                "copying fake delay_info_main into {}",
                delay_info_main.display()
            )
        })?;
        set_executable(&delay_info_main)?;

        let delay_info_proto = cache_dir.join("protos/xls/estimators/delay_model/delay_info.proto");
        if let Some(parent) = delay_info_proto.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating proto parent dir: {}", parent.display()))?;
        }
        fs::write(
            &delay_info_proto,
            "syntax = \"proto3\";\npackage xls;\nmessage DelayInfoProto {}\n",
        )
        .with_context(|| {
            format!(
                "writing fake delay_info.proto: {}",
                delay_info_proto.display()
            )
        })?;
        let op_proto = cache_dir.join("protos/xls/ir/op.proto");
        if let Some(parent) = op_proto.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating proto parent dir: {}", parent.display()))?;
        }
        fs::write(
            &op_proto,
            "syntax = \"proto3\";\npackage xls;\nmessage OpProto {}\n",
        )
        .with_context(|| format!("writing fake op.proto: {}", op_proto.display()))?;
        for binary in DRIVER_RELEASE_CACHE_BINARIES.split(',') {
            let path = cache_dir.join(binary);
            if path.exists() {
                continue;
            }
            fs::write(&path, "#!/bin/sh\nexit 0\n")
                .with_context(|| format!("writing fake cached binary: {}", path.display()))?;
            set_executable(&path)?;
        }
        fs::write(cache_dir.join(format!("libxls-{platform}.so")), b"fake-dso")?;
        fs::write(cache_dir.join("dslx_stdlib.tar.gz"), b"fake-stdlib-archive")?;
        let manifest = build_driver_release_cache_manifest(&cache_dir, version, platform, inputs)?;
        write_cache_manifest_atomic(
            &cache_dir.join(crate::DRIVER_RELEASE_CACHE_READY_FILE),
            &manifest,
        )?;
        validate_driver_release_cache(&cache_dir, version, platform, &expected_input_sha256)?;
        Ok(())
    }

    fn seed_subtree_action(
        store: &ArtifactStore,
        version: &str,
        dslx_relpath: &str,
        dslx_text: &str,
    ) -> Result<String> {
        let action = ActionSpec::DownloadAndExtractXlsynthSourceSubtree {
            version: version.to_string(),
            subtree: "xls/modules/add_dual_path".to_string(),
            discovery_runtime: None,
            source_commit: "2".repeat(40),
        };
        write_completed_action(
            store,
            action,
            ArtifactType::DslxFileSubtree,
            "payload",
            json!({ "seeded": "dslx_subtree" }),
            Vec::new(),
            |payload_dir| {
                let target = payload_dir.join(dslx_relpath);
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent).with_context(|| {
                        format!("creating seeded DSLX parent dir: {}", parent.display())
                    })?;
                }
                fs::write(&target, dslx_text)
                    .with_context(|| format!("writing seeded DSLX file: {}", target.display()))?;
                Ok(())
            },
        )
    }

    fn seed_import_ir_action(store: &ArtifactStore, ir_text: &str, top: &str) -> Result<String> {
        let action = ActionSpec::ImportIrPackageFile {
            source_sha256: hex::encode(Sha256::digest(ir_text.as_bytes())),
            top_fn_name: Some(top.to_string()),
        };
        write_completed_action(
            store,
            action,
            ArtifactType::IrPackageFile,
            "payload/package.ir",
            json!({
                "ir_top": top,
                "output_ir_fn_structural_hash": fake_driver_release_cache_hash(ir_text, top),
                "seeded": "import_ir",
            }),
            Vec::new(),
            |payload_dir| {
                fs::write(payload_dir.join("package.ir"), ir_text).with_context(|| {
                    format!(
                        "writing seeded IR package: {}",
                        payload_dir.join("package.ir").display()
                    )
                })?;
                Ok(())
            },
        )
    }

    fn sample_ir(top: &str) -> String {
        format!(
            "package sample\n\n\
top fn {top}(x: bits[1] id=1) -> bits[1] {{\n\
  literal.2: bits[1] = literal(value=0, id=2)\n\
  ret xor.3: bits[1] = xor(x, literal.2, id=3)\n\
}}\n"
        )
    }

    fn action_kind_label(action: &ActionSpec) -> &'static str {
        match action {
            ActionSpec::ImportIrPackageFile { .. } => "import_ir_package_file",
            ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { .. } => {
                "download_and_extract_xlsynth_release_stdlib_tarball"
            }
            ActionSpec::DownloadAndExtractXlsynthSourceSubtree { .. } => {
                "download_and_extract_xlsynth_source_subtree"
            }
            ActionSpec::DriverDslxFnToIr { .. } => "driver_dslx_fn_to_ir",
            ActionSpec::DriverIrToOpt { .. } => "driver_ir_to_opt",
            ActionSpec::DriverIrToDelayInfo { .. } => "driver_ir_to_delay_info",
            ActionSpec::DriverIrEquiv { .. } => "driver_ir_equiv",
            ActionSpec::DriverIrAigEquiv { .. } => "driver_ir_aig_equiv",
            ActionSpec::DriverIrToG8rAig { .. } => "driver_ir_to_g8r_aig",
            ActionSpec::IrFnToCombinationalVerilog { .. } => "ir_fn_to_combinational_verilog",
            ActionSpec::IrFnToKBoolConeCorpus { .. } => "ir_fn_to_k_bool_cone_corpus",
            ActionSpec::IrFnToMffcCorpus { .. } => "ir_fn_to_mffc_corpus",
            ActionSpec::ComboVerilogToYosysAbcAig { .. } => "combo_verilog_to_yosys_abc_aig",
            ActionSpec::AigToYosysAbcAig { .. } => "aig_to_yosys_abc_aig",
            ActionSpec::DriverAigToStats { .. } => "driver_aig_to_stats",
            ActionSpec::AigStatDiff { .. } => "aig_stat_diff",
        }
    }

    struct ScopedEnvVar {
        key: &'static str,
        old_value: Option<String>,
    }

    impl ScopedEnvVar {
        fn set(key: &'static str, value: &str) -> Self {
            let old_value = std::env::var(key).ok();
            // SAFETY: the ignored Docker integration test serializes itself with a
            // process-wide mutex before mutating environment variables.
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, old_value }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            match self.old_value.as_deref() {
                Some(value) => {
                    // SAFETY: see ScopedEnvVar::set; restoration happens under the same lock.
                    unsafe {
                        std::env::set_var(self.key, value);
                    }
                }
                None => {
                    // SAFETY: see ScopedEnvVar::set; restoration happens under the same lock.
                    unsafe {
                        std::env::remove_var(self.key);
                    }
                }
            }
        }
    }

    struct DockerCleanup {
        containers: Vec<String>,
        images: Vec<String>,
    }

    impl DockerCleanup {
        fn new(images: Vec<String>) -> Self {
            Self {
                containers: Vec::new(),
                images,
            }
        }

        fn add_container(&mut self, container: String) {
            self.containers.push(container);
        }
    }

    impl Drop for DockerCleanup {
        fn drop(&mut self) {
            for container in self.containers.iter().rev() {
                let _ = Command::new("docker")
                    .args(["rm", "-f", container])
                    .output();
            }
            for image in self.images.iter().rev() {
                let _ = Command::new("docker")
                    .args(["image", "rm", "-f", image])
                    .output();
            }
        }
    }

    #[test]
    #[ignore = "requires docker"]
    fn queue_drain_persistent_runners_covers_docker_backed_actions() -> Result<()> {
        let _guard = docker_integration_test_lock()
            .lock()
            .map_err(|_| anyhow!("docker integration test lock poisoned"))?;
        if !docker_available() {
            eprintln!("skipping docker integration test because docker is unavailable");
            return Ok(());
        }

        let _disable_suggested_enqueue =
            ScopedEnvVar::set(crate::BVC_DISABLE_AUTO_SUGGESTED_ENQUEUE_ENV, "1");
        let _disable_incremental_upsert =
            ScopedEnvVar::set(crate::BVC_ENABLE_INCREMENTAL_IR_CORPUS_UPSERT_ENV, "0");

        let repo_root = std::env::current_dir().context("getting repo root")?;
        let store_root = make_temp_store_root("queue-drain-persistent-runners");
        let store = ArtifactStore::new(store_root.clone());
        store.ensure_layout()?;

        let version = "v0.39.0";
        let unique = unique_test_token("persistent-runners");
        let mut runtime_a =
            fake_driver_runtime("0.39.0", format!("xlsynth-bvc-fake-driver-a:{unique}"));
        let mut runtime_b =
            fake_driver_runtime("0.35.0", format!("xlsynth-bvc-fake-driver-b:{unique}"));
        let mut yosys_runtime = fake_yosys_runtime(format!("xlsynth-bvc-fake-yosys:{unique}"));
        let runtime_a_ref = fingerprint_qualified_image_ref(
            &runtime_a.docker_image,
            "driver",
            &driver_runtime_fingerprint(&runtime_a)?,
        )?;
        let runtime_b_ref = fingerprint_qualified_image_ref(
            &runtime_b.docker_image,
            "driver",
            &driver_runtime_fingerprint(&runtime_b)?,
        )?;
        let yosys_runtime_ref = fingerprint_qualified_image_ref(
            &yosys_runtime.docker_image,
            "yosys",
            &yosys_runtime_fingerprint(&yosys_runtime)?,
        )?;
        let mut cleanup = DockerCleanup::new(vec![
            runtime_a_ref.clone(),
            runtime_b_ref.clone(),
            yosys_runtime_ref.clone(),
        ]);

        ensure_driver_image(&repo_root, &runtime_a)?;
        runtime_a.docker_image_id = inspect_image_id(&runtime_a_ref)?
            .with_context(|| format!("missing built image {runtime_a_ref}"))?;
        ensure_driver_image(&repo_root, &runtime_b)?;
        runtime_b.docker_image_id = inspect_image_id(&runtime_b_ref)?
            .with_context(|| format!("missing built image {runtime_b_ref}"))?;
        ensure_yosys_image(&repo_root, &yosys_runtime)?;
        yosys_runtime.docker_image_id = inspect_image_id(&yosys_runtime_ref)?
            .with_context(|| format!("missing built image {yosys_runtime_ref}"))?;
        runtime_a.docker_image = format!("missing-driver-alias:{unique}");
        yosys_runtime.docker_image = format!("missing-yosys-alias:{unique}");
        let missing_recipe_root = store_root.join("missing-read-only-deployment");
        ensure_driver_image(&missing_recipe_root, &runtime_a)?;
        ensure_yosys_image(&missing_recipe_root, &yosys_runtime)?;
        seed_fake_driver_release_cache(&store, version, &runtime_a.release_platform)?;

        let subtree_action_id = seed_subtree_action(
            &store,
            version,
            "xls/modules/add_dual_path/sample.x",
            "fn sample_main(x: u1) -> u1 { !x }\n",
        )?;
        let import_a_id = seed_import_ir_action(&store, &sample_ir("top_a"), "top_a")?;
        let import_b_id = seed_import_ir_action(&store, &sample_ir("top_b"), "top_b")?;
        let import_c_id = seed_import_ir_action(&store, &sample_ir("top_c"), "top_c")?;
        let yosys_script_ref = make_script_ref(&repo_root, crate::DEFAULT_YOSYS_FLOW_SCRIPT)?;

        let dslx_to_ir_b = ActionSpec::DriverDslxFnToIr {
            dslx_subtree_action_id: subtree_action_id,
            dslx_file: "xls/modules/add_dual_path/sample.x".to_string(),
            dslx_fn_name: "sample_main".to_string(),
            version: version.to_string(),
            runtime: runtime_b.clone(),
        };
        let opt_a = ActionSpec::DriverIrToOpt {
            ir_action_id: import_a_id.clone(),
            top_fn_name: Some("top_a".to_string()),
            version: version.to_string(),
            runtime: runtime_a.clone(),
        };
        let opt_b = ActionSpec::DriverIrToOpt {
            ir_action_id: import_b_id.clone(),
            top_fn_name: Some("top_b".to_string()),
            version: version.to_string(),
            runtime: runtime_a.clone(),
        };
        let opt_c = ActionSpec::DriverIrToOpt {
            ir_action_id: import_c_id.clone(),
            top_fn_name: Some("top_c".to_string()),
            version: version.to_string(),
            runtime: runtime_b.clone(),
        };
        let opt_a_id = compute_action_id(&opt_a)?;
        let opt_b_id = compute_action_id(&opt_b)?;
        let opt_c_id = compute_action_id(&opt_c)?;

        let delay_a = ActionSpec::DriverIrToDelayInfo {
            ir_action_id: opt_a_id.clone(),
            top_fn_name: Some("top_a".to_string()),
            delay_model: "asap7".to_string(),
            output_format: crate::DELAY_INFO_OUTPUT_FORMAT_TEXTPROTO_V1.to_string(),
            version: version.to_string(),
            runtime: runtime_a.clone(),
        };
        let ir_equiv_a = ActionSpec::DriverIrEquiv {
            lhs_ir_action_id: import_a_id.clone(),
            rhs_ir_action_id: opt_a_id.clone(),
            top_fn_name: Some("top_a".to_string()),
            version: version.to_string(),
            runtime: runtime_a.clone(),
        };
        let g8r_a = ActionSpec::DriverIrToG8rAig {
            ir_action_id: opt_a_id.clone(),
            top_fn_name: Some("top_a".to_string()),
            fraig: false,
            lowering_mode: G8rLoweringMode::Default,
            version: version.to_string(),
            runtime: runtime_a.clone(),
        };
        let g8r_b = ActionSpec::DriverIrToG8rAig {
            ir_action_id: opt_b_id.clone(),
            top_fn_name: Some("top_b".to_string()),
            fraig: false,
            lowering_mode: G8rLoweringMode::Default,
            version: version.to_string(),
            runtime: runtime_a.clone(),
        };
        let g8r_c = ActionSpec::DriverIrToG8rAig {
            ir_action_id: opt_c_id.clone(),
            top_fn_name: Some("top_c".to_string()),
            fraig: false,
            lowering_mode: G8rLoweringMode::Default,
            version: version.to_string(),
            runtime: runtime_b.clone(),
        };
        let g8r_frontend_a = ActionSpec::DriverIrToG8rAig {
            ir_action_id: opt_a_id.clone(),
            top_fn_name: Some("top_a".to_string()),
            fraig: false,
            lowering_mode: G8rLoweringMode::FrontendNoPrepRewrite,
            version: version.to_string(),
            runtime: runtime_a.clone(),
        };
        let combo_a = ActionSpec::IrFnToCombinationalVerilog {
            ir_action_id: opt_a_id.clone(),
            top_fn_name: Some("top_a".to_string()),
            use_system_verilog: false,
            version: version.to_string(),
            runtime: runtime_a.clone(),
        };
        let kbool_a = ActionSpec::IrFnToKBoolConeCorpus {
            ir_action_id: opt_a_id.clone(),
            top_fn_name: Some("top_a".to_string()),
            k: 3,
            max_ir_ops: crate::default_k_bool_cone_max_ir_ops_for_k(3),
            version: version.to_string(),
            runtime: runtime_a.clone(),
        };
        let g8r_a_id = compute_action_id(&g8r_a)?;
        let g8r_c_id = compute_action_id(&g8r_c)?;
        let g8r_frontend_a_id = compute_action_id(&g8r_frontend_a)?;
        let combo_a_id = compute_action_id(&combo_a)?;
        let yosys_from_combo_a = ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id: combo_a_id.clone(),
            verilog_top_module_name: Some("top_a".to_string()),
            yosys_script_ref: yosys_script_ref.clone(),
            runtime: yosys_runtime.clone(),
        };
        let yosys_from_g8r_a = ActionSpec::AigToYosysAbcAig {
            aig_action_id: g8r_frontend_a_id.clone(),
            yosys_script_ref: yosys_script_ref.clone(),
            runtime: yosys_runtime.clone(),
        };
        let yosys_from_combo_a_id = compute_action_id(&yosys_from_combo_a)?;
        let ir_aig_equiv_a = ActionSpec::DriverIrAigEquiv {
            ir_action_id: opt_a_id.clone(),
            aig_action_id: g8r_a_id.clone(),
            top_fn_name: Some("top_a".to_string()),
            version: version.to_string(),
            runtime: runtime_a.clone(),
        };
        let stats_g8r_a = ActionSpec::DriverAigToStats {
            aig_action_id: g8r_a_id.clone(),
            version: version.to_string(),
            runtime: runtime_a.clone(),
        };
        let stats_yosys_combo_a = ActionSpec::DriverAigToStats {
            aig_action_id: yosys_from_combo_a_id.clone(),
            version: version.to_string(),
            runtime: runtime_a.clone(),
        };
        let delay_a_id = compute_action_id(&delay_a)?;
        let stats_g8r_a_id = compute_action_id(&stats_g8r_a)?;
        let stats_yosys_combo_a_id = compute_action_id(&stats_yosys_combo_a)?;
        let aig_stat_diff_a = ActionSpec::AigStatDiff {
            opt_ir_action_id: opt_a_id.clone(),
            g8r_aig_stats_action_id: stats_g8r_a_id.clone(),
            yosys_abc_aig_stats_action_id: stats_yosys_combo_a_id.clone(),
        };

        for action in [
            dslx_to_ir_b.clone(),
            opt_a.clone(),
            opt_b.clone(),
            opt_c.clone(),
            delay_a.clone(),
            ir_equiv_a.clone(),
            g8r_a.clone(),
            g8r_b.clone(),
            g8r_c.clone(),
            g8r_frontend_a.clone(),
            combo_a.clone(),
            kbool_a.clone(),
            yosys_from_combo_a.clone(),
            yosys_from_g8r_a.clone(),
            ir_aig_equiv_a.clone(),
            stats_g8r_a.clone(),
            stats_yosys_combo_a.clone(),
            aig_stat_diff_a.clone(),
        ] {
            enqueue_action(&store, action)?;
        }

        let worker_id = "runtime-docker-integration";
        let drained_first = drain_queue(&store, &repo_root, None, worker_id, 300, false, None)?;
        assert_eq!(drained_first, 18);
        assert!(list_queue_files(&store.queue_pending_dir())?.is_empty());
        assert!(list_queue_files(&store.queue_running_dir())?.is_empty());
        assert!(store.load_failed_action_records()?.is_empty());

        let image_a = docker_image_content_ref(&runtime_a.docker_image_id)?;
        let image_b = docker_image_content_ref(&runtime_b.docker_image_id)?;
        let image_y = docker_image_content_ref(&yosys_runtime.docker_image_id)?;
        let runner_key_a = persistent_runner_slot_key(
            &persistent_runner_key(&image_a, &store.root),
            persistent_runner_pool_size(PersistentRunnerFamily::Driver),
            0,
        );
        let runner_key_b = persistent_runner_slot_key(
            &persistent_runner_key(&image_b, &store.root),
            persistent_runner_pool_size(PersistentRunnerFamily::Driver),
            0,
        );
        let runner_key_y = persistent_runner_slot_key(
            &persistent_runner_key(&image_y, &store.root),
            persistent_runner_pool_size(PersistentRunnerFamily::Yosys),
            0,
        );
        let container_a = persistent_runner_container_name(&image_a, &runner_key_a);
        let container_b = persistent_runner_container_name(&image_b, &runner_key_b);
        let container_y = persistent_runner_container_name(&image_y, &runner_key_y);
        cleanup.add_container(container_a.clone());
        cleanup.add_container(container_b.clone());
        cleanup.add_container(container_y.clone());
        let runner_a_paths = PersistentRunnerPaths::new(&store.root, &runner_key_a);
        let runner_b_paths = PersistentRunnerPaths::new(&store.root, &runner_key_b);
        let runner_y_paths = PersistentRunnerPaths::new(&store.root, &runner_key_y);
        assert!(runner_a_paths.live_path.exists());
        assert!(runner_b_paths.live_path.exists());
        assert!(runner_y_paths.live_path.exists());
        assert!(docker_container_is_running(&container_a)?);
        assert!(docker_container_is_running(&container_b)?);
        assert!(docker_container_is_running(&container_y)?);
        let container_a_id_before = docker_container_id(&container_a)?;
        let container_b_id_before = docker_container_id(&container_b)?;

        let delay_b = ActionSpec::DriverIrToDelayInfo {
            ir_action_id: opt_b_id.clone(),
            top_fn_name: Some("top_b".to_string()),
            delay_model: "asap7".to_string(),
            output_format: crate::DELAY_INFO_OUTPUT_FORMAT_TEXTPROTO_V1.to_string(),
            version: version.to_string(),
            runtime: runtime_a.clone(),
        };
        let stats_g8r_c = ActionSpec::DriverAigToStats {
            aig_action_id: g8r_c_id.clone(),
            version: version.to_string(),
            runtime: runtime_b.clone(),
        };
        let delay_b_id = enqueue_action(&store, delay_b)?;
        let stats_g8r_c_id = enqueue_action(&store, stats_g8r_c)?;

        let drained_second = drain_queue(&store, &repo_root, None, worker_id, 300, false, None)?;
        assert_eq!(drained_second, 2);
        assert_eq!(docker_container_id(&container_a)?, container_a_id_before);
        assert_eq!(docker_container_id(&container_b)?, container_b_id_before);
        assert!(store.action_exists(&delay_b_id));
        assert!(store.action_exists(&stats_g8r_c_id));

        let delayed_text = fs::read_to_string(
            store.resolve_artifact_ref_path(&store.load_provenance(&delay_a_id)?.output_artifact),
        )?;
        assert!(delayed_text.contains("model: \"fake\""));
        let combo_text = fs::read_to_string(
            store.resolve_artifact_ref_path(&store.load_provenance(&combo_a_id)?.output_artifact),
        )?;
        assert!(combo_text.contains("module top_a"));
        let g8r_bytes = fs::read_to_string(
            store.resolve_artifact_ref_path(&store.load_provenance(&g8r_a_id)?.output_artifact),
        )?;
        assert!(g8r_bytes.starts_with("aag "));

        let kinds: BTreeSet<_> = store
            .list_provenances()?
            .into_iter()
            .map(|provenance| action_kind_label(&provenance.action).to_string())
            .collect();
        for expected in [
            "driver_dslx_fn_to_ir",
            "driver_ir_to_opt",
            "driver_ir_to_delay_info",
            "driver_ir_equiv",
            "driver_ir_aig_equiv",
            "driver_ir_to_g8r_aig",
            "ir_fn_to_combinational_verilog",
            "ir_fn_to_k_bool_cone_corpus",
            "combo_verilog_to_yosys_abc_aig",
            "aig_to_yosys_abc_aig",
            "driver_aig_to_stats",
            "aig_stat_diff",
        ] {
            assert!(
                kinds.contains(expected),
                "expected completed provenance for action kind `{expected}`, got {kinds:?}"
            );
        }

        drop(cleanup);
        fs::remove_dir_all(&store_root).ok();
        Ok(())
    }
}
