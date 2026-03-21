use super::*;

pub(crate) fn driver_cache_mount(store: &ArtifactStore) -> Result<DockerMount> {
    DockerMount::read_only(&store.driver_release_cache_root(), "/cache")
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
    if let Some(trace) =
        ensure_driver_release_cache(store, repo_root, version, &runtime.release_platform)?
    {
        commands.push(trace);
    }
    Ok(())
}

pub(crate) fn ensure_driver_release_cache(
    store: &ArtifactStore,
    repo_root: &Path,
    version: &str,
    platform: &str,
) -> Result<Option<CommandTrace>> {
    let cache_dir = store.driver_release_cache_dir(version, platform);
    let ready_path = cache_dir.join(DRIVER_RELEASE_CACHE_READY_FILE);
    if ready_path.exists() {
        return Ok(None);
    }
    fs::create_dir_all(&cache_dir)
        .with_context(|| format!("creating driver release cache dir: {}", cache_dir.display()))?;

    let lock_path = cache_dir.join(DRIVER_RELEASE_CACHE_LOCK_FILE);
    let start = std::time::Instant::now();
    loop {
        if ready_path.exists() {
            return Ok(None);
        }
        match fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock_path)
        {
            Ok(_) => break,
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

    let setup_result = (|| -> Result<Option<CommandTrace>> {
        if ready_path.exists() {
            return Ok(None);
        }

        let script_path = repo_root.join(VENDORED_DOWNLOAD_RELEASE_SCRIPT);
        if !script_path.exists() {
            bail!(
                "vendored download_release.py not found: {}",
                script_path.display()
            );
        }
        let args: Vec<OsString> = vec![
            script_path.into_os_string(),
            OsString::from("-v"),
            OsString::from(version),
            OsString::from("-o"),
            cache_dir.clone().into_os_string(),
            OsString::from("-p"),
            OsString::from(platform),
            OsString::from("-d"),
            OsString::from("-b"),
            OsString::from(DRIVER_RELEASE_CACHE_BINARIES),
        ];
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
        let delay_info_proto = cache_dir.join("protos/xls/estimators/delay_model/delay_info.proto");
        let op_proto = cache_dir.join("protos/xls/ir/op.proto");
        let delay_info_url = format!(
            "https://raw.githubusercontent.com/xlsynth/xlsynth/{}/xls/estimators/delay_model/delay_info.proto",
            version
        );
        let op_url = format!(
            "https://raw.githubusercontent.com/xlsynth/xlsynth/{}/xls/ir/op.proto",
            version
        );
        if let Some(parent) = delay_info_proto.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating proto cache directory: {}", parent.display()))?;
        }
        if let Some(parent) = op_proto.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating proto cache directory: {}", parent.display()))?;
        }
        download_to_file(&client, &delay_info_url, &delay_info_proto)?;
        download_to_file(&client, &op_url, &op_proto)?;

        let ready = json!({
            "schema_version": 1,
            "version": version,
            "platform": platform,
            "prepared_utc": Utc::now().to_rfc3339(),
            "download_release_script": VENDORED_DOWNLOAD_RELEASE_SCRIPT,
            "binaries": DRIVER_RELEASE_CACHE_BINARIES,
        });
        fs::write(
            &ready_path,
            serde_json::to_string_pretty(&ready).context("serializing cache ready marker")?,
        )
        .with_context(|| format!("writing cache ready marker: {}", ready_path.display()))?;

        Ok(Some(CommandTrace {
            argv: os_args_to_string("python3", &args),
            exit_code: output.status.code().unwrap_or(1),
        }))
    })();

    fs::remove_file(&lock_path).ok();
    setup_result
}

pub(crate) fn is_stale_cache_setup_lock(lock_path: &Path, stale_after_secs: u64) -> bool {
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
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("reading pending queue record: {}", path.display()));
            }
        };
        match parse_queue_work_item(&text, &path) {
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
    let mut driver_releases: HashSet<(String, String)> = HashSet::new();
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
            } => {
                driver_releases.insert((version, runtime.release_platform.clone()));
                driver_runtimes.insert(runtime);
            }
            ActionSpec::DriverAigToStats { runtime, .. } => {
                let release_platform = runtime.release_platform.clone();
                let driver_version = runtime.driver_version.clone();
                driver_runtimes.insert(runtime);
                let runtime_xlsynth_version =
                    resolve_xlsynth_version_for_driver(repo_root, &driver_version)?;
                driver_releases.insert((runtime_xlsynth_version, release_platform));
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
    for (version, platform) in &driver_releases {
        ensure_driver_release_cache(store, repo_root, version, platform)?;
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

    pub(crate) fn to_arg(&self) -> String {
        let mode = if self.read_only { ":ro" } else { "" };
        format!(
            "{}:{}{}",
            self.host_path.display(),
            self.container_path,
            mode
        )
    }
}

pub(crate) fn ensure_driver_image(
    repo_root: &Path,
    runtime: &DriverRuntimeSpec,
) -> Result<Option<CommandTrace>> {
    let inspect_args = vec![
        "image".to_string(),
        "inspect".to_string(),
        runtime.docker_image.clone(),
    ];
    let inspect_out = Command::new("docker")
        .args(&inspect_args)
        .output()
        .context("running `docker image inspect`")?;
    if inspect_out.status.success() {
        return Ok(None);
    }

    let dockerfile_path = PathBuf::from(&runtime.dockerfile);
    let dockerfile = if dockerfile_path.is_absolute() {
        dockerfile_path
    } else {
        repo_root.join(dockerfile_path)
    };
    if !dockerfile.exists() {
        bail!("dockerfile not found: {}", dockerfile.display());
    }

    let build_args = vec![
        OsString::from("build"),
        OsString::from("--file"),
        dockerfile.into_os_string(),
        OsString::from("--tag"),
        OsString::from(runtime.docker_image.clone()),
        OsString::from("--build-arg"),
        OsString::from(format!("DRIVER_CRATE_VERSION={}", runtime.driver_version)),
        OsString::from("."),
    ];

    let status = Command::new("docker")
        .args(&build_args)
        .current_dir(repo_root)
        .status()
        .context("running docker build for xlsynth-driver image")?;

    if !status.success() {
        bail!(
            "docker image build failed for image `{}` with exit code {:?}",
            runtime.docker_image,
            status.code()
        );
    }

    Ok(Some(CommandTrace {
        argv: os_args_to_string("docker", &build_args),
        exit_code: status.code().unwrap_or(1),
    }))
}

pub(crate) fn ensure_yosys_image(
    repo_root: &Path,
    runtime: &YosysRuntimeSpec,
) -> Result<Option<CommandTrace>> {
    let inspect_args = vec![
        "image".to_string(),
        "inspect".to_string(),
        runtime.docker_image.clone(),
    ];
    let inspect_out = Command::new("docker")
        .args(&inspect_args)
        .output()
        .context("running `docker image inspect`")?;
    if inspect_out.status.success() {
        return Ok(None);
    }

    let dockerfile_path = PathBuf::from(&runtime.dockerfile);
    let dockerfile = if dockerfile_path.is_absolute() {
        dockerfile_path
    } else {
        repo_root.join(dockerfile_path)
    };
    if !dockerfile.exists() {
        bail!("dockerfile not found: {}", dockerfile.display());
    }

    let build_args = vec![
        OsString::from("build"),
        OsString::from("--file"),
        dockerfile.into_os_string(),
        OsString::from("--tag"),
        OsString::from(runtime.docker_image.clone()),
        OsString::from("."),
    ];

    let status = Command::new("docker")
        .args(&build_args)
        .current_dir(repo_root)
        .status()
        .context("running docker build for yosys image")?;

    if !status.success() {
        bail!(
            "docker image build failed for image `{}` with exit code {:?}",
            runtime.docker_image,
            status.code()
        );
    }

    Ok(Some(CommandTrace {
        argv: os_args_to_string("docker", &build_args),
        exit_code: status.code().unwrap_or(1),
    }))
}

pub(crate) fn run_docker_script(
    image: &str,
    mounts: &[DockerMount],
    env: &BTreeMap<String, String>,
    script: &str,
    run_hint: &str,
) -> Result<CommandTrace> {
    let container_name = docker_run_container_name(run_hint);
    let mut args: Vec<OsString> = vec![
        OsString::from("run"),
        OsString::from("--name"),
        OsString::from(container_name.clone()),
        OsString::from("--rm"),
        OsString::from("--pull"),
        OsString::from("never"),
        OsString::from("--network"),
        OsString::from("none"),
    ];

    for mount in mounts {
        args.push(OsString::from("-v"));
        args.push(OsString::from(mount.to_arg()));
    }

    for (k, v) in env {
        args.push(OsString::from("-e"));
        args.push(OsString::from(format!("{}={}", k, v)));
    }

    args.push(OsString::from(image));
    args.push(OsString::from("bash"));
    args.push(OsString::from("-lc"));
    args.push(OsString::from(script));
    let command_argv = os_args_to_string("docker", &args);
    let command_line = command_argv.join(" ");

    let mut child = Command::new("docker")
        .args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("running docker action command")?;

    let mut stdout_pipe = child
        .stdout
        .take()
        .context("capturing docker action stdout pipe")?;
    let mut stderr_pipe = child
        .stderr
        .take()
        .context("capturing docker action stderr pipe")?;

    let capture_limit_bytes = docker_output_capture_limit_bytes();
    let stdout_reader = thread::spawn(move || -> std::io::Result<BoundedOutputCapture> {
        read_bounded_output_tail(&mut stdout_pipe, capture_limit_bytes)
    });
    let stderr_reader = thread::spawn(move || -> std::io::Result<BoundedOutputCapture> {
        read_bounded_output_tail(&mut stderr_pipe, capture_limit_bytes)
    });

    let timeout = Duration::from_secs(DEFAULT_ACTION_TIMEOUT_SECONDS);
    let start = Instant::now();
    let (status, timed_out) = loop {
        if let Some(status) = child
            .try_wait()
            .context("waiting on docker action command")?
        {
            break (status, false);
        }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let status = child
                .wait()
                .context("waiting on timed-out docker action command")?;
            break (status, true);
        }
        thread::sleep(Duration::from_millis(100));
    };

    let stdout_capture = stdout_reader
        .join()
        .map_err(|_| anyhow!("joining docker action stdout reader thread"))?
        .context("reading docker action stdout")?;
    let stderr_capture = stderr_reader
        .join()
        .map_err(|_| anyhow!("joining docker action stderr reader thread"))?
        .context("reading docker action stderr")?;
    let stdout = bounded_capture_to_string(stdout_capture, capture_limit_bytes);
    let stderr = bounded_capture_to_string(stderr_capture, capture_limit_bytes);

    if timed_out {
        let cleanup_summary = cleanup_timed_out_container(&container_name);
        bail!(
            "TIMEOUT({}) docker run exceeded {} seconds (container={})\ncommand:\n{}\ncleanup: {}\nstdout:\n{}\nstderr:\n{}",
            DEFAULT_ACTION_TIMEOUT_SECONDS,
            DEFAULT_ACTION_TIMEOUT_SECONDS,
            container_name,
            command_line,
            cleanup_summary,
            stdout,
            stderr
        );
    }

    if !status.success() {
        let stderr_first = first_line(&stderr).trim();
        let stdout_first = first_line(&stdout).trim();
        let root_cause = if !stderr_first.is_empty() {
            stderr_first
        } else if !stdout_first.is_empty() {
            stdout_first
        } else {
            "no stderr/stdout details"
        };
        bail!(
            "docker run failed (exit={:?}): {}\ncommand:\n{}\nstdout:\n{}\nstderr:\n{}",
            status.code(),
            root_cause,
            command_line,
            stdout,
            stderr
        );
    }

    Ok(CommandTrace {
        argv: command_argv,
        exit_code: status.code().unwrap_or(0),
    })
}

const DEFAULT_DOCKER_OUTPUT_CAPTURE_MAX_BYTES: usize = 256 * 1024;
const MIN_DOCKER_OUTPUT_CAPTURE_MAX_BYTES: usize = 4 * 1024;
const MAX_DOCKER_OUTPUT_CAPTURE_MAX_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug)]
struct BoundedOutputCapture {
    bytes: Vec<u8>,
    truncated: bool,
}

fn docker_output_capture_limit_bytes() -> usize {
    std::env::var("BVC_DOCKER_OUTPUT_CAPTURE_MAX_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .map(|value| {
            value.clamp(
                MIN_DOCKER_OUTPUT_CAPTURE_MAX_BYTES,
                MAX_DOCKER_OUTPUT_CAPTURE_MAX_BYTES,
            )
        })
        .unwrap_or(DEFAULT_DOCKER_OUTPUT_CAPTURE_MAX_BYTES)
}

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

fn bounded_capture_to_string(capture: BoundedOutputCapture, max_bytes: usize) -> String {
    let decoded = String::from_utf8_lossy(&capture.bytes);
    if capture.truncated {
        format!("[truncated to last {} bytes]\n{}", max_bytes, decoded)
    } else {
        decoded.into_owned()
    }
}

pub(crate) fn docker_run_container_name(run_hint: &str) -> String {
    let seq = DOCKER_RUN_NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    let hint_prefix: String = run_hint
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(16)
        .collect();
    let hint_prefix = if hint_prefix.is_empty() {
        "run".to_string()
    } else {
        hint_prefix
    };
    format!(
        "xlsynth-bvc-run-{}-{}-{}",
        hint_prefix,
        std::process::id(),
        seq
    )
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
    use std::io::Cursor;

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
}
