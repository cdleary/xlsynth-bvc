// SPDX-License-Identifier: Apache-2.0

//! Resumable, content-addressed ASAP7 corpus execution. Operational JSON is
//! admitted only at the existing corpus-export boundary; protobuf checkpoints
//! are the authority for execution and publication.

use anyhow::{Context, Result, anyhow, bail};
use fs2::FileExt;
use prost::Message;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use crate::techmap_proto::wire::{SampleStatus, TechmapPoint, TechmapRun, TechmapSample};

#[derive(Clone)]
pub(crate) struct RunTechmapCorpusOptions {
    pub(crate) source_run_dir: PathBuf,
    pub(crate) output_dir: PathBuf,
    pub(crate) cohort: String,
    pub(crate) version: String,
    pub(crate) dso_version: String,
    pub(crate) driver_image: String,
    pub(crate) abc_image: String,
    pub(crate) evaluator_image: String,
    pub(crate) evaluator_version: String,
    pub(crate) evaluator_release_cache_dir: PathBuf,
    pub(crate) evaluator_dso_version: String,
    pub(crate) release_cache_dir: PathBuf,
    pub(crate) shared_cache_dir: PathBuf,
    pub(crate) workers: usize,
    pub(crate) limit: Option<usize>,
    pub(crate) timeout_seconds: u64,
}

#[derive(Deserialize)]
struct SourceManifest {
    schema_version: u32,
    dso_version: String,
    driver_runtime: crate::model::DriverRuntimeSpec,
    samples: Vec<SourceSample>,
}

#[derive(Deserialize)]
struct SourceSample {
    sample_id: String,
    logical_name: String,
    source_relpath: String,
    source_sha256: String,
    top_fn_name: String,
    status: String,
    yosys_abc_aig_action_id: String,
    combo_verilog_action_id: String,
    import_ir_action_id: String,
}

const RECIPE: &[u8] = b"xlsynth-bvc/asap7-corpus/v1/default-lowering/pi10-load1/zero-wire\0";

fn sha(bytes: &[u8]) -> Vec<u8> {
    Sha256::digest(bytes).to_vec()
}

fn file_sha(path: &Path) -> Result<Vec<u8>> {
    Ok(sha(
        &fs::read(path).with_context(|| format!("reading {}", path.display()))?
    ))
}

pub(crate) fn sample_digest(sample: &TechmapSample) -> Vec<u8> {
    let mut sample = sample.clone();
    sample.record_sha256.clear();
    sha(&sample.encode_to_vec())
}

pub(crate) fn run_spec_digest(run: &TechmapRun) -> Vec<u8> {
    let mut run = run.clone();
    run.samples.clear();
    run.run_spec_sha256.clear();
    run.complete = false;
    sha(&run.encode_to_vec())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("output has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name().unwrap().to_string_lossy(),
        std::process::id()
    ));
    fs::write(&temporary, bytes)?;
    fs::rename(&temporary, path)?;
    Ok(())
}

pub(crate) fn lock(path: &Path) -> Result<File> {
    fs::create_dir_all(path.parent().context("lock has no parent")?)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    file.lock_exclusive()?;
    Ok(file)
}

/// External commands never receive canonical output paths. A killed host can
/// leave a command running, so attempts are unique and retained, never reused or
/// removed while a late writer could still hold an open file or start an exec.
pub(crate) fn attempt_directory(parent: &Path) -> Result<PathBuf> {
    static NEXT_ATTEMPT: AtomicU64 = AtomicU64::new(0);
    fs::create_dir_all(parent)?;
    loop {
        let attempt = parent.join(format!(
            ".attempt-{}-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
            NEXT_ATTEMPT.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::create_dir(&attempt) {
            Ok(()) => return Ok(attempt),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
}

pub(crate) fn command_output(command: &mut Command) -> Result<String> {
    let output = command.output().context("starting mapping command")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "mapping command failed ({}): {}",
            output.status,
            stderr.chars().take(3000).collect::<String>()
        );
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

fn image_id(image: &str) -> Result<String> {
    command_output(Command::new("docker").args(["image", "inspect", "--format", "{{.Id}}", image]))
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

struct Containers {
    driver: String,
    abc: String,
    evaluator: String,
}

const CONTAINER_OWNER_LABEL: &str = "org.xlsynth.bvc.asap7-output";

fn container_owner(output: &Path) -> Result<String> {
    Ok(hex::encode(sha(fs::canonicalize(output)?
        .as_os_str()
        .as_encoded_bytes())))
}

fn recover_container(name: &str, owner: &str) -> Result<()> {
    let ids = command_output(Command::new("docker").args([
        "ps",
        "--all",
        "--quiet",
        "--no-trunc",
        "--filter",
        &format!("name=^/{name}$"),
    ]))?;
    for id in ids.lines() {
        let observed = command_output(Command::new("docker").args([
            "inspect",
            "--format",
            &format!("{{{{ index .Config.Labels {CONTAINER_OWNER_LABEL:?} }}}}"),
            id,
        ]))?;
        if observed != owner {
            bail!("mapping container name belongs to a different owner");
        }
        command_output(Command::new("docker").args(["rm", "--force", id]))?;
    }
    Ok(())
}

impl Drop for Containers {
    fn drop(&mut self) {
        for id in [&self.driver, &self.abc, &self.evaluator] {
            if !id.is_empty() {
                let _ = Command::new("docker").args(["rm", "-f", id]).output();
            }
        }
    }
}

impl Containers {
    fn start(
        options: &RunTechmapCorpusOptions,
        driver_id: &str,
        abc_id: &str,
        evaluator_id: &str,
        platform: &str,
    ) -> Result<Self> {
        // The run lock is already held. Recover only containers labeled for
        // this exact output, including idle containers left by a killed host.
        // Execs address immutable IDs, so a delayed old Docker client cannot
        // attach its command to a newly created replacement container.
        let owner = container_owner(&options.output_dir)?;
        let names =
            ["driver", "abc", "evaluator"].map(|role| format!("bvc-asap7-{role}-{}", &owner[..32]));
        for name in &names {
            recover_container(name, &owner)?;
        }
        let mut result = Self {
            driver: String::new(),
            abc: String::new(),
            evaluator: String::new(),
        };
        for (index, (name, image, release_cache)) in [
            (&names[0], driver_id, &options.release_cache_dir),
            (&names[1], abc_id, &options.release_cache_dir),
            (
                &names[2],
                evaluator_id,
                &options.evaluator_release_cache_dir,
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let mut command = Command::new("docker");
            command.args([
                "run",
                "--rm",
                "-d",
                "--network=none",
                "--name",
                name,
                "--label",
                &format!("{CONTAINER_OWNER_LABEL}={owner}"),
                "--entrypoint",
                "/bin/bash",
            ]);
            for (host, container, readonly) in [
                (&options.source_run_dir, "/source", true),
                (&options.output_dir, "/work", false),
                (&options.shared_cache_dir, "/cache", false),
                (release_cache, "/release-input", true),
            ] {
                let host = fs::canonicalize(host)?;
                let specification = format!(
                    "type=bind,source={},target={container}{}",
                    host.display(),
                    if readonly { ",readonly" } else { "" }
                );
                command.args(["--mount", &specification]);
            }
            command.args([image, "-c", "while :; do sleep 3600; done"]);
            let id = command_output(&mut command)?;
            if id.len() != 64 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                bail!("Docker returned an invalid mapping container identity");
            }
            match index {
                0 => result.driver = id,
                1 => result.abc = id,
                _ => result.evaluator = id,
            }
        }
        for (container, version, declared_version) in [
            (&result.driver, &options.dso_version, &options.version),
            (
                &result.evaluator,
                &options.evaluator_dso_version,
                &options.evaluator_version,
            ),
        ] {
            let version = version.trim_start_matches('v');
            let setup = format!(
                "set -euo pipefail\nmkdir -p /tmp/xlsynth-release\ncp -a /release-input/. /tmp/xlsynth-release/\nfor ext in so dylib; do if [ -f /tmp/xlsynth-release/libxls-{platform}.$ext ]; then ln -sf /tmp/xlsynth-release/libxls-{platform}.$ext /tmp/xlsynth-release/libxls-v{version}-{platform}.$ext; fi; done\n"
            );
            Self::execute_in(container, &setup, options.timeout_seconds)?;
            let observed = Self::output_in(
                container,
                "xlsynth-driver --version",
                options.timeout_seconds,
            )?;
            let expected = format!(
                "xlsynth-driver {}",
                declared_version.trim_start_matches('v')
            );
            if observed != expected {
                bail!("driver image version mismatch: expected {expected}, observed {observed}");
            }
        }
        Ok(result)
    }

    fn execute(&self, native: bool, script: &str, timeout: u64) -> Result<()> {
        Self::execute_in(
            if native { &self.driver } else { &self.abc },
            script,
            timeout,
        )
    }

    fn evaluate(&self, script: &str, timeout: u64) -> Result<()> {
        Self::execute_in(&self.evaluator, script, timeout)
    }

    fn execute_in(container: &str, script: &str, timeout: u64) -> Result<()> {
        Self::output_in(container, script, timeout).map(|_| ())
    }

    fn output_in(container: &str, script: &str, timeout: u64) -> Result<String> {
        let prefix = "set -euo pipefail\nexport XLSYNTH_TOOLS=/tmp/xlsynth-release\nexport LD_LIBRARY_PATH=/tmp/xlsynth-release:${LD_LIBRARY_PATH:-}\n";
        let script = format!("{prefix}{script}");
        command_output(Command::new("timeout").args([
            "--signal=TERM",
            "--kill-after=15s",
            &format!("{}s", timeout.max(1) + 30),
            "docker",
            "exec",
            container,
            "timeout",
            "--signal=TERM",
            "--kill-after=10s",
            &format!("{}s", timeout.max(1)),
            "bash",
            "-c",
            &script,
        ]))
    }
}

fn cache_key(fields: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(RECIPE);
    for field in fields {
        hasher.update((field.len() as u64).to_le_bytes());
        hasher.update(field);
    }
    hex::encode(hasher.finalize())
}

fn container_path(options: &RunTechmapCorpusOptions, path: &Path) -> Result<String> {
    for (host, mount) in [
        (&options.shared_cache_dir, "/cache"),
        (&options.source_run_dir, "/source"),
        (&options.output_dir, "/work"),
    ] {
        if let Ok(relative) = path.strip_prefix(host) {
            return Ok(format!("{mount}/{}", relative.display()));
        }
    }
    bail!("mapping input is outside the mounted result and cache directories")
}

fn writable_directory(resource_root: &Path, path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut ancestor = absolute.as_path();
    while !ancestor.exists() {
        ancestor = ancestor
            .parent()
            .context("output has no existing ancestor")?;
    }
    let mut resolved = fs::canonicalize(ancestor).context("resolving output ancestor")?;
    for part in absolute.strip_prefix(ancestor)?.components() {
        match part {
            Component::Normal(name) => resolved.push(name),
            Component::CurDir => {}
            // A missing/../existing-link suffix can re-enter an unchecked symlink.
            _ => bail!("output has an invalid suffix after a missing directory"),
        }
    }
    let root = fs::canonicalize(resource_root)?;
    if resolved.starts_with(&root) || root.starts_with(&resolved) {
        bail!("mapping output and cache must not overlap the resource root");
    }
    Ok(resolved)
}

struct MappingResources {
    resource_root: PathBuf,
    summary: crate::techmap_proto::wire::Asap7LibrarySummary,
    preparation_script: PathBuf,
    archive_sha256: Vec<u8>,
    preparation_script_sha256: Vec<u8>,
    yosys_flow_sha256: Vec<u8>,
}

impl MappingResources {
    fn load(resource_root: &Path) -> Result<Self> {
        let resource_root =
            fs::canonicalize(resource_root).context("resolving the deployed resource root")?;
        let summary = crate::asap7_library::summary()?;
        let preparation_script = resource_root.join("scripts/prepare_asap7.py");
        let preparation_script_sha256 = file_sha(&preparation_script)?;
        if preparation_script_sha256 != summary.preparation_script_sha256 {
            bail!("deployed ASAP7 preparation script differs from the pinned summary");
        }
        Ok(Self {
            archive_sha256: summary.archive_sha256.clone(),
            preparation_script_sha256,
            yosys_flow_sha256: file_sha(&resource_root.join(crate::DEFAULT_YOSYS_FLOW_SCRIPT))?,
            resource_root,
            summary,
            preparation_script,
        })
    }
}

fn prepare_library(
    resources: &MappingResources,
    archive: &Path,
    options: &RunTechmapCorpusOptions,
    containers: &Containers,
    driver_id: &str,
    evaluator: bool,
) -> Result<(PathBuf, PathBuf, Vec<u8>, Vec<u8>)> {
    let archive_sha = resources.archive_sha256.clone();
    let script_sha = resources.preparation_script_sha256.clone();
    let key = cache_key(&[b"prepared-liberty", &archive_sha, &script_sha]);
    let directory = options.shared_cache_dir.join("libraries").join(key);
    let _guard = lock(&directory.with_extension("lock"))?;
    fs::create_dir_all(&directory)?;
    if !directory.join("merged.lib").is_file() {
        let attempt = attempt_directory(&directory)?;
        command_output(
            Command::new("python3")
                .arg(&resources.preparation_script)
                .arg(archive)
                .arg(&attempt),
        )?;
        validate_liberty(
            &attempt.join("merged.lib"),
            &resources.summary.merged_liberty_sha256,
        )?;
        fs::rename(attempt.join("merged.lib"), directory.join("merged.lib"))?;
    }
    let liberty = directory.join("merged.lib");
    let lib_sha = validate_liberty(&liberty, &resources.summary.merged_liberty_sha256)?;
    let proto_key = cache_key(&[b"timing-liberty-protobuf", driver_id.as_bytes(), &lib_sha]);
    let proto = directory.join(format!("{proto_key}.proto"));
    let proto_record = directory.join(format!("{proto_key}.record.pb"));
    if !proto.is_file() || !proto_record.is_file() {
        let attempt = attempt_directory(&directory)?;
        let staged_proto = attempt.join("library.proto");
        let target = container_path(options, &staged_proto)?;
        let library = container_path(options, &liberty)?;
        let log = attempt.join("lib2proto.log");
        let log = container_path(options, &log)?;
        let script = format!(
            "xlsynth-driver lib2proto {} --output {}.tmp.proto --include-timing --default-output-load 1 > {} 2>&1\nmv {}.tmp.proto {}\n",
            shell_quote(&library),
            shell_quote(&target),
            shell_quote(&log),
            shell_quote(&target),
            shell_quote(&target)
        );
        if evaluator {
            containers.evaluate(&script, options.timeout_seconds)?;
        } else {
            containers.execute(true, &script, options.timeout_seconds)?;
        }
        file_sha(&staged_proto)?;
        fs::rename(staged_proto, &proto)?;
    }
    let mut expected = TechmapSample {
        sample_id: proto_key,
        source_ir_sha256: lib_sha,
        g8r_aig_sha256: file_sha(&proto)?,
        status: SampleStatus::Done as i32,
        ..Default::default()
    };
    expected.record_sha256 = sample_digest(&expected);
    if proto_record.is_file() {
        let saved = TechmapSample::decode(fs::read(&proto_record)?.as_slice())?;
        if saved != expected {
            bail!("prepared driver library protobuf checksum mismatch");
        }
    } else {
        atomic_write(&proto_record, &expected.encode_to_vec())?;
    }
    Ok((liberty, proto, archive_sha, script_sha))
}

fn validate_liberty(liberty: &Path, expected: &[u8]) -> Result<Vec<u8>> {
    let digest = file_sha(liberty)?;
    if digest != expected {
        bail!("prepared ASAP7 library differs from the pinned deterministic merge");
    }
    Ok(digest)
}

#[derive(Deserialize)]
struct StatsReport {
    cell_area: f64,
    cell_count: u64,
    max_delay: f64,
    time_unit: String,
}

fn metrics(directory: &Path, tool: &str, goal: &str, relpath: String) -> Result<TechmapPoint> {
    let stats: StatsReport = serde_json::from_slice(&fs::read(directory.join("stats.json"))?)?;
    let unit = stats.time_unit.trim().replace(' ', "").to_ascii_lowercase();
    let split = unit
        .find(|c: char| c.is_ascii_alphabetic())
        .context("missing timing units")?;
    let scale = if split == 0 {
        1.0
    } else {
        unit[..split].parse::<f64>()?
    };
    let ps = match &unit[split..] {
        "s" => 1e12,
        "ms" => 1e9,
        "us" => 1e6,
        "ns" => 1e3,
        "ps" => 1.0,
        "fs" => 1e-3,
        _ => bail!("unsupported timing unit"),
    };
    let delay = stats.max_delay * scale * ps;
    if !scale.is_finite()
        || scale <= 0.0
        || !stats.cell_area.is_finite()
        || stats.cell_area < 0.0
        || !delay.is_finite()
        || delay < 0.0
    {
        bail!("non-finite or negative mapped metrics");
    }
    Ok(TechmapPoint {
        tool: tool.to_string(),
        goal: goal.to_string(),
        area: stats.cell_area,
        delay_ps: delay,
        cell_count: stats.cell_count,
        mapped_netlist_sha256: file_sha(&directory.join("mapped.v"))?,
        mapped_netlist_relpath: relpath,
        policy: if tool == "g8r" {
            format!("aig-tech-map --strategy {goal}")
        } else if goal == "area" {
            "map -S 10 -a".to_string()
        } else {
            "map -S 10".to_string()
        },
    })
}

fn validate_cached_point(
    sample: &TechmapSample,
    directory: &Path,
    expected_key: &str,
    expected_netlist_sha: &[u8],
    tool: &str,
    goal: &str,
) -> Result<TechmapPoint> {
    if sample.sample_id != expected_key
        || sample.record_sha256 != sample_digest(sample)
        || sample.status != SampleStatus::Done as i32
        || sample.points.len() != 1
    {
        bail!("invalid cached mapping checkpoint");
    }
    let point = &sample.points[0];
    if point.mapped_netlist_sha256 != expected_netlist_sha
        || file_sha(&directory.join("mapped.v"))? != expected_netlist_sha
        || !point.area.is_finite()
        || point.area < 0.0
        || !point.delay_ps.is_finite()
        || point.delay_ps < 0.0
    {
        bail!("cached mapping netlist or policy mismatch");
    }
    let mut result = point.clone();
    result.tool = tool.to_string();
    result.goal = goal.to_string();
    result.policy = if tool == "g8r" {
        format!("aig-tech-map --strategy {goal}")
    } else if goal == "area" {
        "map -S 10 -a".to_string()
    } else {
        "map -S 10".to_string()
    };
    Ok(result)
}

struct Execution<'a> {
    options: &'a RunTechmapCorpusOptions,
    containers: &'a Containers,
    driver_id: &'a str,
    abc_id: &'a str,
    evaluator_id: &'a str,
    evaluator_proto: &'a Path,
    liberty: &'a Path,
    proto: &'a Path,
    library_sha: Vec<u8>,
}

impl Execution<'_> {
    fn point(&self, aig: &Path, tool: &str, goal: &str) -> Result<TechmapPoint> {
        let input_sha = file_sha(aig)?;
        // Native mapping and the common evaluator are versioned. ABC's mapped
        // netlist cache is independent of the evaluator, allowing cross-release reuse.
        let mapper_id = if tool == "g8r" {
            self.driver_id
        } else {
            self.abc_id
        };
        let map_key = cache_key(&[
            b"mapped-netlist",
            tool.as_bytes(),
            goal.as_bytes(),
            &input_sha,
            mapper_id.as_bytes(),
            &self.library_sha,
        ]);
        let map_dir = self.options.shared_cache_dir.join("mapped").join(&map_key);
        let _map_guard = lock(&map_dir.with_extension("lock"))?;
        fs::create_dir_all(&map_dir)?;
        let input = container_path(self.options, aig)?;
        let liberty = container_path(self.options, self.liberty)?;
        let proto = container_path(self.options, self.proto)?;
        if !map_dir.join("netlist.sha256").is_file() {
            let attempt = attempt_directory(&map_dir)?;
            let map_container = container_path(self.options, &attempt)?;
            let script = if tool == "g8r" {
                let timing_arguments = if crate::versioning::cmp_dotted_numeric_version(
                    self.options.version.trim_start_matches('v'),
                    "0.76.0",
                )
                .is_lt()
                {
                    ""
                } else {
                    " --primary-input-transition 10 --module-output-load 1"
                };
                format!(
                    "xlsynth-driver aig-tech-map {} --liberty_proto {} --strategy {goal}{timing_arguments} --module_name mapped --netlist_out {}/mapped.v --json_out {}/map.json > {}/map.log 2>&1\n",
                    shell_quote(&input),
                    shell_quote(&proto),
                    shell_quote(&map_container),
                    shell_quote(&map_container),
                    shell_quote(&map_container)
                )
            } else if empty_aig(aig)? {
                fs::write(attempt.join("mapped.v"), b"module mapped(); endmodule\n")?;
                fs::write(
                    attempt.join("map.log"),
                    b"Empty AIGER: exact empty cell netlist.\n",
                )?;
                String::new()
            } else {
                fs::write(attempt.join("abc.constr"), b"set_load 1\n")?;
                let policy = if goal == "area" {
                    "map -S 10 -a"
                } else {
                    "map -S 10"
                };
                let command = format!(
                    "read_lib -S 10 {liberty}; read_aiger {input}; strash; read_constr {map_container}/abc.constr; {policy}; write_verilog {map_container}/mapped.v"
                );
                format!(
                    "yosys-abc -c {} > {}/map.log 2>&1\ntest -s {}/mapped.v\n",
                    shell_quote(&command),
                    shell_quote(&map_container),
                    shell_quote(&map_container)
                )
            };
            if !script.is_empty() {
                self.containers
                    .execute(tool == "g8r", &script, self.options.timeout_seconds)
                    .with_context(|| {
                        format!(
                            "{tool} {goal} mapping; retained log {}",
                            attempt.join("map.log").display()
                        )
                    })?;
            }
            let netlist_sha = file_sha(&attempt.join("mapped.v"))?;
            fs::rename(attempt.join("mapped.v"), map_dir.join("mapped.v"))?;
            atomic_write(
                &map_dir.join("netlist.sha256"),
                hex::encode(netlist_sha).as_bytes(),
            )?;
        }
        let netlist_sha = file_sha(&map_dir.join("mapped.v"))?;
        if fs::read_to_string(map_dir.join("netlist.sha256"))? != hex::encode(&netlist_sha) {
            bail!("mapped netlist content digest mismatch");
        }
        let eval_key = cache_key(&[
            b"common-evaluation-v2",
            self.evaluator_id.as_bytes(),
            &netlist_sha,
            &self.library_sha,
        ]);
        let eval_dir = self
            .options
            .shared_cache_dir
            .join("evaluated")
            .join(&eval_key);
        let _eval_guard = lock(&eval_dir.with_extension("lock"))?;
        fs::create_dir_all(&eval_dir)?;
        let checkpoint = eval_dir.join("point.pb");
        if checkpoint.is_file() {
            let saved = TechmapSample::decode(fs::read(&checkpoint)?.as_slice())?;
            let mut point =
                validate_cached_point(&saved, &eval_dir, &eval_key, &netlist_sha, tool, goal)?;
            point.mapped_netlist_relpath =
                relative_path(&self.options.output_dir, &eval_dir.join("mapped.v"))?;
            return Ok(point);
        }
        let attempt = attempt_directory(&eval_dir)?;
        // Copy rather than link: an attempt must not share writable inodes with
        // canonical cache entries, even if its command outlives this host.
        fs::copy(map_dir.join("mapped.v"), attempt.join("mapped.v"))?;
        if file_sha(&attempt.join("mapped.v"))? != netlist_sha {
            bail!("evaluation input differs from the requested mapped netlist");
        }
        let eval_container = container_path(self.options, &attempt)?;
        let script = format!(
            "xlsynth-driver gv-stats --netlist {d}/mapped.v --liberty_proto {p} --primary_input_transition 10 --module_output_load 1 --json_out {d}/stats.json > {d}/stats.log 2>&1\n",
            d = shell_quote(&eval_container),
            p = shell_quote(&container_path(self.options, self.evaluator_proto)?)
        );
        self.containers
            .evaluate(&script, self.options.timeout_seconds)
            .with_context(|| {
                format!(
                    "common {tool} {goal} evaluation; retained logs {}",
                    attempt.display()
                )
            })?;
        // Keep links portable relative to the run directory. Publication copies
        // verified assets as needed; host paths are never browser data.
        let mut point = metrics(&attempt, tool, goal, String::new())?;
        if point.mapped_netlist_sha256 != netlist_sha {
            bail!("evaluation changed the requested mapped netlist");
        }
        fs::rename(attempt.join("mapped.v"), eval_dir.join("mapped.v"))?;
        fs::rename(attempt.join("stats.json"), eval_dir.join("stats.json"))?;
        point.mapped_netlist_relpath =
            relative_path(&self.options.output_dir, &eval_dir.join("mapped.v"))?;
        let mut saved = TechmapSample {
            sample_id: eval_key,
            g8r_aig_sha256: if tool == "g8r" {
                input_sha.clone()
            } else {
                Vec::new()
            },
            abc_aig_sha256: if tool == "abc" { input_sha } else { Vec::new() },
            points: vec![point.clone()],
            status: SampleStatus::Done as i32,
            ..Default::default()
        };
        saved.record_sha256 = sample_digest(&saved);
        atomic_write(&checkpoint, &saved.encode_to_vec())?;
        Ok(point)
    }
}

fn relative_path(from: &Path, to: &Path) -> Result<String> {
    let from = fs::canonicalize(from)?;
    let to = fs::canonicalize(to)?;
    let a: Vec<_> = from.components().collect();
    let b: Vec<_> = to.components().collect();
    let common = a.iter().zip(&b).take_while(|(a, b)| a == b).count();
    let mut result = PathBuf::new();
    for _ in common..a.len() {
        result.push("..");
    }
    for component in &b[common..] {
        result.push(component.as_os_str());
    }
    Ok(result.to_string_lossy().into_owned())
}

fn empty_aig(path: &Path) -> Result<bool> {
    use std::io::BufRead;
    let mut line = String::new();
    std::io::BufReader::new(File::open(path)?).read_line(&mut line)?;
    let mut fields = line.split_whitespace();
    if fields.next() != Some("aig") {
        bail!("ABC mapping input must be binary AIGER");
    }
    let counts = fields
        .map(str::parse::<u64>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if !(5..=9).contains(&counts.len()) || counts[2] != 0 || counts.iter().skip(5).any(|v| *v != 0)
    {
        bail!("expected combinational AIGER without properties");
    }
    Ok(counts.iter().all(|v| *v == 0))
}

fn source_provenance(source: &Path, id: &str) -> Result<crate::model::Provenance> {
    if id.len() != 64 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("invalid canonical source action id");
    }
    let path = source
        .join(".bvc/bvc-artifacts/.materialized-actions")
        .join(&id[..2])
        .join(&id[2..4])
        .join(id)
        .join("provenance.pb");
    let provenance = crate::proto::decode_provenance(
        &fs::read(&path).context("missing retained canonical source provenance")?,
    )?;
    if provenance.action_id != id
        || provenance.output_artifact.action_id != id
        || crate::executor::compute_action_id(&provenance.action)? != id
    {
        bail!("source action identity mismatch");
    }
    Ok(provenance)
}

fn verify_source_aig(
    resources: &MappingResources,
    source: &Path,
    manifest: &SourceManifest,
    row: &SourceSample,
    aig: &Path,
    requested_abc_image_id: &str,
) -> Result<Vec<u8>> {
    use crate::model::ActionSpec;
    let imported = source_provenance(source, &row.import_ir_action_id)?;
    match imported.action {
        ActionSpec::ImportIrPackageFile {
            source_sha256,
            top_fn_name,
        } if source_sha256 == row.source_sha256
            && top_fn_name.as_deref() == Some(&row.top_fn_name) => {}
        _ => bail!("source import is not bound to the selected IR and top"),
    }
    let combo = source_provenance(source, &row.combo_verilog_action_id)?;
    let combo_digest = canonical_output_digest(&combo)?;
    match combo.action {
        ActionSpec::IrFnToCombinationalVerilog {
            ir_action_id,
            top_fn_name,
            use_system_verilog: false,
            version,
            runtime,
        } if ir_action_id == row.import_ir_action_id
            && top_fn_name.as_deref() == Some(&row.top_fn_name)
            && version.trim_start_matches('v') == manifest.dso_version.trim_start_matches('v')
            && runtime == manifest.driver_runtime => {}
        _ => bail!("source Verilog is not derived from the selected IR, top, and runtime"),
    }
    let provenance = source_provenance(source, &row.yosys_abc_aig_action_id)?;
    match &provenance.action {
        ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id,
            verilog_top_module_name,
            frontend: crate::model::YosysVerilogFrontend::Builtin,
            yosys_script_ref,
            runtime,
        } if runtime.docker_image_id.trim_start_matches("sha256:")
            == requested_abc_image_id.trim_start_matches("sha256:")
            && verilog_action_id == &row.combo_verilog_action_id
            && verilog_top_module_name.as_deref() == Some(&row.top_fn_name)
            && yosys_script_ref.path == crate::DEFAULT_YOSYS_FLOW_SCRIPT
            && hex::decode(&yosys_script_ref.sha256)? == resources.yosys_flow_sha256 => {}
        _ => bail!("source ABC AIG has an incompatible codegen or Yosys lineage"),
    }
    if canonical_output_digest(&provenance)? != file_sha(aig)? {
        bail!("exported source AIG differs from its canonical provenance");
    }
    Ok(combo_digest)
}

fn canonical_output_digest(provenance: &crate::model::Provenance) -> Result<Vec<u8>> {
    let relative = provenance
        .output_artifact
        .relpath
        .strip_prefix("payload/")
        .unwrap_or(&provenance.output_artifact.relpath);
    let output = provenance
        .output_files
        .iter()
        .find(|file| file.path == relative)
        .context("source output has no canonical digest")?;
    let digest = hex::decode(&output.sha256)?;
    if digest.len() != 32 {
        bail!("invalid canonical source output digest");
    }
    Ok(digest)
}

fn safe_component(value: &str) -> bool {
    let mut components = Path::new(value).components();
    matches!(components.next(), Some(Component::Normal(_)))
        && components.next().is_none()
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
}

fn validated_release_identity(cache: &Path, dso: &str, platform: &str) -> Result<Vec<u8>> {
    let bytes = fs::read(cache.join(".ready.pb"))?;
    let manifest = crate::proto::v1::DriverReleaseCacheManifest::decode(bytes.as_slice())?;
    let inputs = manifest
        .inputs
        .context("release cache has no typed input identity")?;
    let digest = sha(&inputs.encode_to_vec());
    crate::service::validate_driver_release_cache(cache, dso, platform, &hex::encode(&digest))?;
    Ok(digest)
}

fn record_version_for_new_run(version: &str) -> u32 {
    if crate::versioning::cmp_dotted_numeric_version(version.trim_start_matches('v'), "0.76.0")
        .is_lt()
    {
        2
    } else {
        1
    }
}

fn required_points(
    version: &str,
    record_version: u32,
) -> Result<&'static [(&'static str, &'static str)]> {
    let area_only_native =
        crate::versioning::cmp_dotted_numeric_version(version.trim_start_matches('v'), "0.76.0")
            .is_lt();
    match (area_only_native, record_version) {
        (true, 1) => Ok(&[("g8r", "area"), ("abc", "area")]),
        (true, 2) => Ok(&[("g8r", "area"), ("abc", "area"), ("abc", "delay")]),
        (false, 1) => Ok(&[
            ("g8r", "area"),
            ("abc", "area"),
            ("g8r", "delay"),
            ("abc", "delay"),
        ]),
        _ => bail!("unsupported technology mapping goal contract"),
    }
}

fn validate_completed_checkpoint(
    output: &Path,
    source_ir: &Path,
    sample: &TechmapSample,
    required_points: &[(&str, &str)],
) -> Result<()> {
    if sample.status != SampleStatus::Done as i32
        || sample.record_sha256 != sample_digest(sample)
        || sample.g8r_aig_sha256.len() != 32
        || sample.abc_aig_sha256.len() != 32
        || !sample.error.is_empty()
        || file_sha(source_ir)? != sample.source_ir_sha256
    {
        bail!("completed checkpoint has inconsistent source or evidence");
    }
    let expected: BTreeSet<_> = required_points.iter().copied().collect();
    let mut observed = BTreeSet::new();
    for point in &sample.points {
        let key = (point.tool.as_str(), point.goal.as_str());
        if !expected.contains(&key)
            || !observed.insert(key)
            || point.policy.is_empty()
            || !point.area.is_finite()
            || point.area < 0.0
            || !point.delay_ps.is_finite()
            || point.delay_ps < 0.0
            || Path::new(&point.mapped_netlist_relpath).is_absolute()
            || point.mapped_netlist_sha256.len() != 32
            || file_sha(&output.join(&point.mapped_netlist_relpath))? != point.mapped_netlist_sha256
        {
            bail!("completed checkpoint has missing or inconsistent mapping evidence");
        }
    }
    if observed != expected {
        bail!("completed checkpoint lacks required tool/goal points");
    }
    Ok(())
}

fn checkpoint_path(output: &Path, id: &str) -> PathBuf {
    output
        .join("samples")
        .join(format!("{}.pb", hex::encode(sha(id.as_bytes()))))
}

fn export_manifest(output: &Path, template: &TechmapRun, samples: &[TechmapSample]) -> Result<()> {
    let mut run = template.clone();
    run.samples = samples.to_vec();
    let done = samples
        .iter()
        .filter(|s| s.status == SampleStatus::Done as i32)
        .count();
    let failed = samples
        .iter()
        .filter(|s| s.status == SampleStatus::Failed as i32)
        .count();
    run.complete = done == run.expected_sample_count as usize && samples.len() == done;
    atomic_write(&output.join("manifest.pb"), &run.encode_to_vec())?;
    let summary = serde_json::json!({"schema_version":1,"crate_version":run.crate_version,"cohort":run.cohort_id,"expected":run.expected_sample_count,"selected":samples.len(),"done":done,"failed":failed,"pending":samples.len()-done-failed,"complete":run.complete});
    atomic_write(
        &output.join("summary.json"),
        &serde_json::to_vec_pretty(&summary)?,
    )?;
    eprintln!(
        "ASAP7 {} {}: {done}/{} done, {failed} failed, {} pending",
        run.crate_version,
        run.cohort_id,
        run.expected_sample_count,
        samples.len() - done - failed
    );
    Ok(())
}

#[derive(Serialize)]
pub(crate) struct TechmapCorpusSummary {
    crate_version: String,
    cohort: String,
    expected: usize,
    selected: usize,
    done: usize,
    failed: usize,
    complete: bool,
}

pub(crate) fn run(
    resource_root: &Path,
    options: &RunTechmapCorpusOptions,
) -> Result<TechmapCorpusSummary> {
    if options.workers == 0 || options.timeout_seconds == 0 {
        bail!("workers and timeout must be positive");
    }
    let version: Vec<_> = options
        .version
        .trim_start_matches('v')
        .split('.')
        .map(str::parse::<u64>)
        .collect::<std::result::Result<_, _>>()?;
    if version.len() != 3 || version.as_slice() < [0, 75, 0].as_slice() {
        bail!("ASAP7 mapping requires driver 0.75.0 or newer");
    }
    let record_version = record_version_for_new_run(&options.version);
    let point_plan = required_points(&options.version, record_version)?;
    let resources = MappingResources::load(resource_root)?;
    let mut resolved_options = options.clone();
    resolved_options.output_dir = writable_directory(resource_root, &options.output_dir)?;
    resolved_options.shared_cache_dir =
        writable_directory(resource_root, &options.shared_cache_dir)?;
    let options = &resolved_options;
    fs::create_dir_all(&options.output_dir)?;
    fs::create_dir_all(&options.shared_cache_dir)?;
    let _run_guard = lock(&options.output_dir.join(".run.lock"))?;
    let source: SourceManifest =
        serde_json::from_slice(&fs::read(options.source_run_dir.join("manifest.json"))?)?;
    if source.schema_version != 5 || source.samples.is_empty() {
        bail!("expected an existing version-5 operational corpus export");
    }
    let expected = match options.cohort.as_str() {
        "full-corpus" => 32806,
        "whole-functions-v1" => 187,
        "mffc-v1" => 904,
        _ => bail!("unknown corpus cohort"),
    };
    if source.samples.len() != expected {
        bail!(
            "source cohort has {} samples, expected {expected}",
            source.samples.len()
        );
    }
    let mut ids = BTreeSet::new();
    let mut corpus = Sha256::new();
    corpus.update(b"xlsynth-bvc/asap7-source-corpus/v1\0");
    for row in &source.samples {
        if !safe_component(&row.sample_id) || !ids.insert(&row.sample_id) || row.status != "done" {
            bail!("source manifest contains unsafe, repeated, or unfinished samples");
        }
        let source_sha = hex::decode(&row.source_sha256)?;
        if source_sha.len() != 32 || row.top_fn_name.is_empty() {
            bail!("source identity is incomplete");
        }
        for bytes in [
            row.sample_id.as_bytes(),
            row.top_fn_name.as_bytes(),
            &source_sha,
        ] {
            corpus.update((bytes.len() as u64).to_le_bytes());
            corpus.update(bytes);
        }
    }
    let release_input_sha = validated_release_identity(
        &options.release_cache_dir,
        &options.dso_version,
        &source.driver_runtime.release_platform,
    )?;
    let evaluator_release_sha = validated_release_identity(
        &options.evaluator_release_cache_dir,
        &options.evaluator_dso_version,
        &source.driver_runtime.release_platform,
    )?;
    let evaluator_id = image_id(&options.evaluator_image)?;
    let driver_id = image_id(&options.driver_image)?;
    let abc_id = image_id(&options.abc_image)?;
    let archive = crate::asap7_library::materialize(
        &resources.resource_root,
        &options.shared_cache_dir,
        &resources.summary,
    )?;
    let containers = Containers::start(
        &options,
        &driver_id,
        &abc_id,
        &evaluator_id,
        &source.driver_runtime.release_platform,
    )?;
    let (liberty, proto, archive_sha, preparation_sha) = prepare_library(
        &resources,
        &archive,
        &options,
        &containers,
        &driver_id,
        false,
    )?;
    let (_, evaluator_proto, _, _) = prepare_library(
        &resources,
        &archive,
        &options,
        &containers,
        &evaluator_id,
        true,
    )?;
    let mut template = TechmapRun {
        record_version,
        crate_version: options.version.trim_start_matches('v').to_string(),
        dso_version: options.dso_version.trim_start_matches('v').to_string(),
        cohort_id: options.cohort.clone(),
        expected_sample_count: expected as u64,
        corpus_sha256: corpus.finalize().to_vec(),
        liberty_bundle_sha256: archive_sha,
        preparation_script_sha256: preparation_sha,
        driver_image_id: driver_id.clone(),
        abc_image_id: abc_id.clone(),
        primary_input_transition_ps: 10.0,
        module_output_load_ff: 1.0,
        source_lowering_mode: "default".to_string(),
        release_cache_input_sha256: release_input_sha,
        source_recipe_revision: 1,
        evaluator_image_id: evaluator_id.clone(),
        evaluator_crate_version: options
            .evaluator_version
            .trim_start_matches('v')
            .to_string(),
        evaluator_release_cache_input_sha256: evaluator_release_sha,
        ..Default::default()
    };
    template.run_spec_sha256 = run_spec_digest(&template);
    if options.output_dir.join("manifest.pb").is_file() {
        let old = TechmapRun::decode(fs::read(options.output_dir.join("manifest.pb"))?.as_slice())?;
        if old.run_spec_sha256 != template.run_spec_sha256 {
            bail!("output directory belongs to a different immutable mapping run");
        }
    }
    let selected: Vec<_> = source
        .samples
        .iter()
        .take(options.limit.unwrap_or(usize::MAX))
        .collect();
    let mut initial = Vec::with_capacity(selected.len());
    for row in &selected {
        let mut base = TechmapSample {
            sample_id: row.sample_id.clone(),
            logical_name: row.logical_name.clone(),
            source_relpath: row.source_relpath.clone(),
            source_ir_sha256: hex::decode(&row.source_sha256)?,
            top_fn_name: row.top_fn_name.clone(),
            status: SampleStatus::Pending as i32,
            ..Default::default()
        };
        base.record_sha256 = sample_digest(&base);
        let path = checkpoint_path(&options.output_dir, &row.sample_id);
        let previous = if path.is_file() {
            Some(TechmapSample::decode(fs::read(&path)?.as_slice())?)
        } else {
            None
        };
        if let Some(ref saved) = previous {
            if saved.record_sha256 != sample_digest(saved)
                || saved.sample_id != base.sample_id
                || saved.source_ir_sha256 != base.source_ir_sha256
                || saved.top_fn_name != base.top_fn_name
            {
                bail!("sample checkpoint identity or digest mismatch");
            }
        }
        initial.push(previous.unwrap_or(base));
    }
    export_manifest(&options.output_dir, &template, &initial)?;
    let shared = Arc::new(Mutex::new(initial));
    let finished = Arc::new(AtomicBool::new(false));
    let monitor_samples = Arc::clone(&shared);
    let monitor_done = Arc::clone(&finished);
    let monitor_output = options.output_dir.clone();
    let monitor_template = template.clone();
    let monitor = std::thread::spawn(move || -> Result<()> {
        let mut last = Instant::now();
        while !monitor_done.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_secs(1));
            if last.elapsed() >= Duration::from_secs(60) {
                let snapshot = monitor_samples
                    .lock()
                    .map_err(|_| anyhow!("mapping progress lock poisoned"))?
                    .clone();
                export_manifest(&monitor_output, &monitor_template, &snapshot)?;
                last = Instant::now();
            }
        }
        Ok(())
    });
    let execution = Execution {
        options: &options,
        containers: &containers,
        driver_id: &driver_id,
        abc_id: &abc_id,
        evaluator_id: &evaluator_id,
        evaluator_proto: &evaluator_proto,
        library_sha: file_sha(&liberty)?,
        liberty: &liberty,
        proto: &proto,
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(options.workers)
        .build()?;
    pool.install(|| {
        selected.par_iter().enumerate().for_each(|(index, row)| {
            let mut sample = shared.lock().unwrap()[index].clone();
            let result = (|| -> Result<()> {
                let source_dir = options
                    .source_run_dir
                    .join("artifacts")
                    .join(&row.sample_id);
                let input_ir = source_dir.join("input.ir");
                if file_sha(&input_ir)? != sample.source_ir_sha256 {
                    bail!("source IR digest differs from corpus identity");
                }
                let source_abc = source_dir.join("yosys_abc.aig");
                let source_combo_digest = verify_source_aig(
                    &resources,
                    &options.source_run_dir,
                    &source,
                    row,
                    &source_abc,
                    &abc_id,
                )?;
                if sample.status == SampleStatus::Done as i32 {
                    validate_completed_checkpoint(
                        &options.output_dir,
                        &input_ir,
                        &sample,
                        point_plan,
                    )?;
                    return Ok(());
                }
                let prepared = crate::techmap_sources::prepare_sources(
                    crate::techmap_sources::SourcePreparation {
                        input_ir: &input_ir,
                        input_ir_container: &container_path(&options, &input_ir)?,
                        top_fn_name: &row.top_fn_name,
                        source_abc_aig: &source_abc,
                        source_combo_verilog_sha256: &source_combo_digest,
                        source_dso_version: &source.dso_version,
                        target_dso_version: &options.dso_version,
                        driver_image_id: &driver_id,
                        abc_image_id: &abc_id,
                        cache_dir: &options.shared_cache_dir,
                        cache_container_root: "/cache",
                        driver_container: &containers.driver,
                        abc_container: &containers.abc,
                        release_dir_container: "/tmp/xlsynth-release",
                        timeout_secs: options.timeout_seconds,
                    },
                )?;
                sample.g8r_aig_sha256 = file_sha(&prepared.g8r_path)?;
                sample.abc_aig_sha256 = file_sha(&prepared.abc_path)?;
                sample.points.clear();
                for &(tool, goal) in point_plan {
                    let aig = if tool == "g8r" {
                        &prepared.g8r_path
                    } else {
                        &prepared.abc_path
                    };
                    sample.points.push(execution.point(aig, tool, goal)?);
                }
                Ok(())
            })();
            match result {
                Ok(()) => {
                    sample.status = SampleStatus::Done as i32;
                    sample.error.clear();
                }
                Err(error) => {
                    sample.status = SampleStatus::Failed as i32;
                    sample.error = public_error(&error, options);
                    eprintln!("ASAP7 sample {} failed: {}", row.sample_id, sample.error);
                }
            }
            sample.record_sha256 = sample_digest(&sample);
            if let Err(error) = atomic_write(
                &checkpoint_path(&options.output_dir, &row.sample_id),
                &sample.encode_to_vec(),
            ) {
                sample.status = SampleStatus::Failed as i32;
                sample.error = format!("writing checkpoint: {}", public_error(&error, options));
                sample.record_sha256 = sample_digest(&sample);
            }
            shared.lock().unwrap()[index] = sample;
        })
    });
    finished.store(true, Ordering::Release);
    monitor
        .join()
        .map_err(|_| anyhow!("mapping progress monitor panicked"))??;
    let samples = shared
        .lock()
        .map_err(|_| anyhow!("mapping result lock poisoned"))?;
    export_manifest(&options.output_dir, &template, &samples)?;
    let failed = samples
        .iter()
        .filter(|sample| sample.status == SampleStatus::Failed as i32)
        .count();
    if failed != 0 {
        bail!("{failed} mapping samples failed; checkpoints and logs retained for resumption");
    }
    let done = samples
        .iter()
        .filter(|sample| sample.status == SampleStatus::Done as i32)
        .count();
    Ok(TechmapCorpusSummary {
        crate_version: template.crate_version,
        cohort: template.cohort_id,
        expected,
        selected: samples.len(),
        done,
        failed,
        complete: done == expected,
    })
}

fn public_error(error: &anyhow::Error, options: &RunTechmapCorpusOptions) -> String {
    let mut message = format!("{error:#}");
    for (path, replacement) in [
        (&options.source_run_dir, "source-run"),
        (&options.shared_cache_dir, "mapping-cache"),
        (&options.output_dir, "mapping-run"),
        (&options.release_cache_dir, "release-cache"),
    ] {
        message = message.replace(path.to_string_lossy().as_ref(), replacement);
    }
    // Tool diagnostics and host paths remain in local logs. Public protobufs
    // contain only the structured operation context, never captured stderr.
    if let Some(index) = message.find("mapping command failed") {
        message.truncate(index);
        message.push_str("mapping command failed; inspect retained operation logs");
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "bvc-techmap-evidence-{}-{}",
                std::process::id(),
                chrono::Utc::now().timestamp_nanos_opt().unwrap()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    #[cfg(unix)]
    fn killed_host_retry_isolated_from_open_and_delayed_orphan_writers() {
        const CHILD_ROOT: &str = "BVC_TECHMAP_TEST_KILLED_HOST_ROOT";
        if let Some(root) = std::env::var_os(CHILD_ROOT) {
            let root = PathBuf::from(root);
            let _guard = lock(&root.join("mapped.lock")).unwrap();
            let attempt = attempt_directory(&root.join("mapped")).unwrap();
            let child = Command::new("python3")
                .args([
                    "-c",
                    r#"from pathlib import Path
import sys, time
attempt, root = map(Path, sys.argv[1:])
with (attempt / 'mapped.v').open('wb') as output:
    output.write(b'partial')
    output.flush()
    (root / 'writer-ready').write_text(str(attempt))
    deadline = time.monotonic() + 15
    while not (root / 'release-writer').exists():
        if time.monotonic() >= deadline:
            sys.exit('test parent did not release orphan writer')
        time.sleep(0.01)
    output.seek(0)
    output.write(b'orphan through original open handle')
    output.flush()
(attempt / 'mapped.v').write_bytes(b'orphan through delayed pathname')
(root / 'writer-done').write_text('done')
"#,
                ])
                .arg(&attempt)
                .arg(&root)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap();
            // Deliberately leave an external writer alive when this test's
            // parent kills the host process, as Docker exec does in production.
            std::mem::forget(child);
            std::thread::sleep(Duration::from_secs(15));
            panic!("test parent did not kill the host");
        }

        fn wait_for(path: &Path) {
            let deadline = Instant::now() + Duration::from_secs(10);
            while !path.exists() {
                assert!(Instant::now() < deadline, "external writer did not respond");
                std::thread::sleep(Duration::from_millis(10));
            }
        }

        let fixture = Fixture::new();
        let mut host = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "techmap_corpus::tests::killed_host_retry_isolated_from_open_and_delayed_orphan_writers",
            ])
            .env(CHILD_ROOT, &fixture.0)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        wait_for(&fixture.0.join("writer-ready"));
        host.kill().unwrap();
        assert!(!host.wait().unwrap().success());

        // A retry (possibly another run sharing this cache) can reacquire the
        // lock after SIGKILL, but never reuses the orphan's writable attempt.
        let _guard = lock(&fixture.0.join("mapped.lock")).unwrap();
        let directory = fixture.0.join("mapped");
        let retry = attempt_directory(&directory).unwrap();
        let abandoned = PathBuf::from(fs::read_to_string(fixture.0.join("writer-ready")).unwrap());
        assert_ne!(retry, abandoned);
        let expected = b"successful retry netlist";
        fs::write(retry.join("mapped.v"), expected).unwrap();
        fs::rename(retry.join("mapped.v"), directory.join("mapped.v")).unwrap();
        atomic_write(
            &directory.join("netlist.sha256"),
            hex::encode(sha(expected)).as_bytes(),
        )
        .unwrap();

        fs::write(fixture.0.join("release-writer"), b"continue").unwrap();
        wait_for(&fixture.0.join("writer-done"));
        assert_eq!(fs::read(directory.join("mapped.v")).unwrap(), expected);
        assert_eq!(
            fs::read_to_string(directory.join("netlist.sha256")).unwrap(),
            hex::encode(file_sha(&directory.join("mapped.v")).unwrap())
        );
        assert_eq!(
            fs::read(abandoned.join("mapped.v")).unwrap(),
            b"orphan through delayed pathname"
        );
    }

    #[test]
    fn resources_follow_the_deployed_bundle_and_do_not_fall_back_to_the_build_checkout() {
        let fixture = Fixture::new();
        let resource_root = fixture.0.join("deployed-bundle");
        let script = resource_root.join("scripts/prepare_asap7.py");
        let flow = resource_root.join(crate::DEFAULT_YOSYS_FLOW_SCRIPT);
        for (path, contents) in [
            (
                &script,
                include_bytes!("../scripts/prepare_asap7.py").as_slice(),
            ),
            (&flow, b"deployed Yosys flow".as_slice()),
        ] {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, contents).unwrap();
        }
        let resources = MappingResources::load(&resource_root).unwrap();
        assert_eq!(
            resources.resource_root,
            fs::canonicalize(&resource_root).unwrap()
        );
        assert!(
            !resource_root
                .join("third_party/asap7/asap7_rvt_ss.tar.gz")
                .exists()
        );
        assert_eq!(
            resources.preparation_script,
            fs::canonicalize(&script).unwrap()
        );
        assert_eq!(
            resources.archive_sha256,
            crate::asap7_library::summary().unwrap().archive_sha256
        );
        assert_eq!(
            resources.preparation_script_sha256,
            sha(include_bytes!("../scripts/prepare_asap7.py"))
        );
        assert_eq!(resources.yosys_flow_sha256, sha(b"deployed Yosys flow"));

        fs::remove_file(&flow).unwrap();
        assert!(MappingResources::load(&resource_root).is_err());
    }

    #[test]
    fn runner_rejects_resource_outputs_before_creating_any_state() {
        let fixture = Fixture::new();
        let root = fixture.0.join("bundle");
        for relative in ["scripts/prepare_asap7.py", crate::DEFAULT_YOSYS_FLOW_SCRIPT] {
            let path = root.join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            let contents = if relative == "scripts/prepare_asap7.py" {
                include_bytes!("../scripts/prepare_asap7.py").as_slice()
            } else {
                b"fixture Yosys flow".as_slice()
            };
            fs::write(path, contents).unwrap();
        }
        for cache_inside in [false, true] {
            let outside = fixture.0.join("state");
            let inside = root.join("state");
            let options = RunTechmapCorpusOptions {
                source_run_dir: fixture.0.join("missing-source"),
                output_dir: if cache_inside {
                    outside.clone()
                } else {
                    inside.clone()
                },
                shared_cache_dir: if cache_inside {
                    inside.clone()
                } else {
                    outside.clone()
                },
                cohort: "whole-functions-v1".into(),
                version: "0.79.0".into(),
                dso_version: "0.79.0".into(),
                driver_image: String::new(),
                abc_image: String::new(),
                evaluator_image: String::new(),
                evaluator_version: "0.79.0".into(),
                evaluator_release_cache_dir: fixture.0.join("evaluator"),
                evaluator_dso_version: "0.79.0".into(),
                release_cache_dir: fixture.0.join("release"),
                workers: 1,
                limit: None,
                timeout_seconds: 1,
            };
            assert!(run(&root, &options).is_err());
            assert!(!inside.exists());
            assert!(!outside.exists());
        }
        #[cfg(unix)]
        {
            let alias = fixture.0.join("resource-alias");
            std::os::unix::fs::symlink(&root, &alias).unwrap();
            assert!(writable_directory(&root, &alias.join("new-output")).is_err());
            assert!(writable_directory(&root, &fixture.0).is_err());
        }
    }

    fn point(root: &Path, tool: &str) -> TechmapPoint {
        let filename = format!("{tool}.v");
        fs::write(
            root.join(&filename),
            format!("module {tool}(); endmodule\n"),
        )
        .unwrap();
        TechmapPoint {
            tool: tool.to_string(),
            goal: "area".to_string(),
            area: 0.125,
            delay_ps: 7.0,
            cell_count: 1,
            mapped_netlist_sha256: file_sha(&root.join(&filename)).unwrap(),
            mapped_netlist_relpath: filename,
            policy: "area".to_string(),
        }
    }

    fn sign(sample: &mut TechmapSample) {
        sample.record_sha256 = sample_digest(sample);
    }

    #[test]
    fn completed_resume_requires_unchanged_source_and_every_measured_netlist() {
        let fixture = Fixture::new();
        let input = fixture.0.join("input.ir");
        let original = b"package fixture\ntop fn foo() -> bits[1] {}\n";
        fs::write(&input, original).unwrap();
        let mut sample = TechmapSample {
            sample_id: "fixture".to_string(),
            source_ir_sha256: sha(original),
            g8r_aig_sha256: vec![1; 32],
            abc_aig_sha256: vec![2; 32],
            points: vec![point(&fixture.0, "g8r"), point(&fixture.0, "abc")],
            status: SampleStatus::Done as i32,
            ..Default::default()
        };
        sign(&mut sample);
        assert!(
            validate_completed_checkpoint(
                &fixture.0,
                &input,
                &sample,
                required_points("0.75.0", 1).unwrap()
            )
            .is_ok()
        );
        fs::write(&input, b"changed IR").unwrap();
        assert!(
            validate_completed_checkpoint(
                &fixture.0,
                &input,
                &sample,
                required_points("0.75.0", 1).unwrap()
            )
            .is_err()
        );
        fs::write(&input, original).unwrap();
        fs::write(fixture.0.join("abc.v"), b"changed netlist").unwrap();
        assert!(
            validate_completed_checkpoint(
                &fixture.0,
                &input,
                &sample,
                required_points("0.75.0", 1).unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn completed_resume_rejects_missing_duplicate_or_invalid_measurements_even_when_resigned() {
        let fixture = Fixture::new();
        let input = fixture.0.join("input.ir");
        fs::write(&input, b"IR").unwrap();
        let sample = TechmapSample {
            sample_id: "fixture".to_string(),
            source_ir_sha256: sha(b"IR"),
            g8r_aig_sha256: vec![1; 32],
            abc_aig_sha256: vec![2; 32],
            points: vec![point(&fixture.0, "g8r"), point(&fixture.0, "abc")],
            status: SampleStatus::Done as i32,
            ..Default::default()
        };
        let mut missing = sample.clone();
        missing.points.pop();
        sign(&mut missing);
        assert!(
            validate_completed_checkpoint(
                &fixture.0,
                &input,
                &missing,
                required_points("0.75.0", 1).unwrap()
            )
            .is_err()
        );
        let mut duplicated = sample.clone();
        duplicated.points[1] = duplicated.points[0].clone();
        sign(&mut duplicated);
        assert!(
            validate_completed_checkpoint(
                &fixture.0,
                &input,
                &duplicated,
                required_points("0.75.0", 1).unwrap()
            )
            .is_err()
        );
        let mut invalid = sample.clone();
        invalid.points[0].delay_ps = f64::INFINITY;
        sign(&mut invalid);
        assert!(
            validate_completed_checkpoint(
                &fixture.0,
                &input,
                &invalid,
                required_points("0.75.0", 1).unwrap()
            )
            .is_err()
        );
        let mut valid = sample;
        sign(&mut valid);
        assert!(
            validate_completed_checkpoint(
                &fixture.0,
                &input,
                &valid,
                required_points("0.75.0", 1).unwrap()
            )
            .is_ok()
        );
        assert!(
            validate_completed_checkpoint(
                &fixture.0,
                &input,
                &valid,
                required_points("0.76.0", 1).unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn area_only_native_contract_requires_both_abc_goals_in_version_two() {
        let fixture = Fixture::new();
        let input = fixture.0.join("input.ir");
        fs::write(&input, b"IR").unwrap();
        let mut sample = TechmapSample {
            sample_id: "fixture".to_string(),
            source_ir_sha256: sha(b"IR"),
            g8r_aig_sha256: vec![1; 32],
            abc_aig_sha256: vec![2; 32],
            points: vec![point(&fixture.0, "g8r"), point(&fixture.0, "abc")],
            status: SampleStatus::Done as i32,
            ..Default::default()
        };
        sign(&mut sample);
        let legacy = required_points("0.75.0", 1).unwrap();
        let current = required_points("0.75.0", 2).unwrap();
        assert!(validate_completed_checkpoint(&fixture.0, &input, &sample, legacy).is_ok());
        assert!(validate_completed_checkpoint(&fixture.0, &input, &sample, current).is_err());
        let mut abc_delay = sample.points[1].clone();
        abc_delay.goal = "delay".to_string();
        sample.points.push(abc_delay);
        sign(&mut sample);
        assert!(validate_completed_checkpoint(&fixture.0, &input, &sample, current).is_ok());
        assert!(validate_completed_checkpoint(&fixture.0, &input, &sample, legacy).is_err());
        let mut native_delay = sample.points[0].clone();
        native_delay.goal = "delay".to_string();
        sample.points.push(native_delay);
        sign(&mut sample);
        assert!(validate_completed_checkpoint(&fixture.0, &input, &sample, current).is_err());
        assert!(
            validate_completed_checkpoint(
                &fixture.0,
                &input,
                &sample,
                required_points("0.76.0", 1).unwrap()
            )
            .is_ok()
        );
        for version in ["0.76.0", "0.77.0", "0.78.0", "0.79.0"] {
            assert_eq!(record_version_for_new_run(version), 1);
            assert_eq!(
                required_points(version, 1).unwrap(),
                &[
                    ("g8r", "area"),
                    ("abc", "area"),
                    ("g8r", "delay"),
                    ("abc", "delay")
                ]
            );
        }
        assert_eq!(record_version_for_new_run("0.75.0"), 2);
        assert!(required_points("0.76.0", 2).is_err());
    }

    #[test]
    fn evaluation_cache_binds_expected_address_and_expected_netlist() {
        let fixture = Fixture::new();
        fs::write(fixture.0.join("mapped.v"), b"netlist A").unwrap();
        let expected = sha(b"netlist A");
        let mut saved = TechmapSample {
            sample_id: "cache-A".to_string(),
            status: SampleStatus::Done as i32,
            points: vec![TechmapPoint {
                tool: "g8r".to_string(),
                goal: "area".to_string(),
                area: 1.0,
                delay_ps: 2.0,
                mapped_netlist_sha256: expected.clone(),
                policy: "area".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        sign(&mut saved);
        let point = validate_cached_point(&saved, &fixture.0, "cache-A", &expected, "abc", "delay")
            .unwrap();
        assert_eq!((point.tool.as_str(), point.goal.as_str()), ("abc", "delay"));
        assert!(
            validate_cached_point(&saved, &fixture.0, "cache-B", &expected, "abc", "delay")
                .is_err()
        );
        assert!(
            validate_cached_point(
                &saved,
                &fixture.0,
                "cache-A",
                &sha(b"netlist B"),
                "abc",
                "delay"
            )
            .is_err()
        );
        fs::write(fixture.0.join("mapped.v"), b"netlist B").unwrap();
        saved.points[0].mapped_netlist_sha256 = sha(b"netlist B");
        sign(&mut saved);
        assert!(
            validate_cached_point(&saved, &fixture.0, "cache-A", &expected, "abc", "delay")
                .is_err()
        );
    }
}
