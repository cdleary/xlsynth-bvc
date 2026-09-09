// SPDX-License-Identifier: Apache-2.0

//! Local DSLX ingestion. Only input-relative names and content hashes enter the public manifest.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use walkdir::WalkDir;

use crate::cli::DslxCorpusIngestCli;
use crate::model::{RawBoolConeManifestLine, RawMffcManifestLine};
use crate::service::{infer_ir_top_function, parse_ir_fn_op_count_from_file};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct IngestManifest {
    pub(crate) schema_version: u32,
    pub(crate) driver_version: String,
    pub(crate) source_tree_sha256: String,
    pub(crate) settings: IngestSettings,
    pub(crate) files: Vec<SourceFile>,
    #[serde(default)]
    pub(crate) counts: IngestCounts,
    pub(crate) functions: Vec<SourceFunction>,
    pub(crate) cones: Vec<ConeOccurrence>,
    pub(crate) failures: Vec<IngestFailure>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct IngestCounts {
    pub(crate) discovered_functions: usize,
    pub(crate) parametric_skipped: usize,
    pub(crate) attempted_concrete_functions: usize,
    pub(crate) k_oversize_skipped: usize,
    pub(crate) function_limit_reached: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct IngestSettings {
    pub(crate) max_files: usize,
    pub(crate) max_functions: usize,
    pub(crate) max_mffcs: u64,
    pub(crate) min_internal_non_literal: u64,
    pub(crate) max_frontier_non_literal: u64,
    pub(crate) k: Option<u32>,
    pub(crate) max_k_cones: u64,
    pub(crate) max_k_ir_ops: u64,
    pub(crate) mffcs: bool,
    pub(crate) optimized_dslx_to_ir: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SourceFile {
    pub(crate) relpath: String,
    pub(crate) sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SourceFunction {
    pub(crate) source_relpath: String,
    pub(crate) name: String,
    pub(crate) optimized_ir_relpath: String,
    pub(crate) optimized_ir_sha256: String,
    pub(crate) ir_top: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ConeOccurrence {
    pub(crate) kind: String,
    pub(crate) cone_relpath: String,
    pub(crate) content_sha256: String,
    pub(crate) driver_cone_sha256: String,
    pub(crate) source_relpath: String,
    pub(crate) source_fn: String,
    pub(crate) source_ir_sha256: String,
    pub(crate) ir_top: String,
    pub(crate) rank: Option<u64>,
    pub(crate) ir_op_count: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct IngestFailure {
    pub(crate) source_relpath: String,
    pub(crate) source_fn: Option<String>,
    pub(crate) stage: String,
    pub(crate) reason: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct IngestSummary {
    output_dir: String,
    manifest: String,
    cone_dir: String,
    source_files: usize,
    attempted_functions: usize,
    unique_cones: usize,
    cone_occurrences: usize,
    failures: usize,
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn relative_name(base: &Path, file: &Path) -> Result<String> {
    let rel = file
        .strip_prefix(base)
        .context("source file is outside input root")?;
    let name = rel
        .to_str()
        .context("DSLX source path is not UTF-8")?
        .replace('\\', "/");
    if name.starts_with('/') || name.split('/').any(|part| part == ".." || part.is_empty()) {
        bail!("invalid DSLX source relative path");
    }
    Ok(name)
}

fn resolved_path(path: &Path) -> Result<PathBuf> {
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
    let mut result = fs::canonicalize(ancestor).context("resolving path ancestor")?;
    for part in absolute.strip_prefix(ancestor)?.components() {
        match part {
            std::path::Component::Normal(name) => result.push(name),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                result.pop();
            }
            _ => bail!("invalid output directory suffix"),
        }
    }
    Ok(result)
}

fn checked_empty_output(output_dir: &Path, protected: &[&Path]) -> Result<PathBuf> {
    // Resolve aliases and missing suffixes before checking containment, and use the
    // resolved result for writes so the checks and the actual destination agree.
    let output = resolved_path(output_dir)?;
    for root in protected {
        let root = resolved_path(root)?;
        if output.starts_with(&root) || root.starts_with(&output) {
            bail!("corpus output must not overlap an input or the resource root");
        }
    }
    if let Ok(metadata) = fs::symlink_metadata(output_dir) {
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            bail!("corpus output must be a regular directory");
        }
    }
    if output.exists() && fs::read_dir(&output)?.next().is_some() {
        bail!("corpus output directory must be empty");
    }
    Ok(output)
}

fn checked_output_dir(
    input_dir: &Path,
    output_dir: &Path,
    repo_root: &Path,
) -> Result<(PathBuf, PathBuf)> {
    let input = fs::canonicalize(input_dir).context("resolving DSLX input directory")?;
    if !input.is_dir() {
        bail!("DSLX input must be a directory");
    }
    let output = checked_empty_output(output_dir, &[&input, repo_root])?;
    Ok((input, output))
}

fn driver_command(opts: &DslxCorpusIngestCli, subcommand: &str) -> Command {
    let mut cmd = Command::new(&opts.driver);
    if let Some(toolchain) = &opts.toolchain {
        cmd.arg("--toolchain").arg(toolchain);
    }
    cmd.arg(subcommand);
    cmd
}

fn run_driver(mut cmd: Command, log: &Path) -> Result<Vec<u8>> {
    let output = cmd.output().context("starting xlsynth-driver")?;
    if !output.status.success() {
        fs::write(log, &output.stderr).context("recording driver failure log")?;
        bail!(
            "xlsynth-driver failed (exit status {}; see logs)",
            output
                .status
                .code()
                .map_or_else(|| "signal".to_string(), |v| v.to_string())
        );
    }
    Ok(output.stdout)
}

fn snapshot_sources(input: &Path, output: &Path) -> Result<Vec<SourceFile>> {
    let mut files = Vec::new();
    for entry in WalkDir::new(input).sort_by_file_name() {
        let entry = entry.context("walking DSLX tree")?;
        if !entry.file_type().is_file() || entry.path().extension().is_none_or(|ext| ext != "x") {
            continue;
        }
        let relpath = relative_name(input, entry.path())?;
        let bytes = fs::read(entry.path()).context("reading DSLX source")?;
        let destination = output.join(&relpath);
        fs::create_dir_all(destination.parent().context("source path missing parent")?)?;
        fs::write(destination, &bytes)?;
        files.push(SourceFile {
            relpath,
            sha256: sha256(&bytes),
        });
    }
    Ok(files)
}

fn import_args(
    cmd: &mut Command,
    source_file: &Path,
    source_dir: &Path,
    opts: &DslxCorpusIngestCli,
) {
    // Match dslx-list-fns, which resolves imports beside the source file first.
    let parent = source_file.parent().unwrap_or(source_dir);
    let mut paths = vec![parent.to_string_lossy().to_string()];
    if parent != source_dir {
        paths.push(source_dir.to_string_lossy().to_string());
    }
    paths.extend(
        opts.dslx_path
            .iter()
            .map(|p| p.to_string_lossy().to_string()),
    );
    cmd.arg("--dslx_path").arg(paths.join(";"));
    if let Some(stdlib) = &opts.dslx_stdlib_path {
        cmd.arg("--dslx_stdlib_path").arg(stdlib);
    }
}

fn report_failure(
    manifest: &mut IngestManifest,
    source: &str,
    name: Option<&str>,
    stage: &str,
    err: &anyhow::Error,
) {
    // Keep tool errors in local logs; the portable manifest contains no host paths.
    let reason = if err.to_string().starts_with("xlsynth-driver failed") {
        err.to_string()
    } else {
        format!("{stage} failed; inspect the local driver output")
    };
    manifest.failures.push(IngestFailure {
        source_relpath: source.to_string(),
        source_fn: name.map(str::to_string),
        stage: stage.to_string(),
        reason,
    });
}

fn cone_rows(manifest_path: &Path, kind: &str) -> Result<Vec<(String, Option<u64>)>> {
    let text = fs::read_to_string(manifest_path).context("reading cone manifest")?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            if kind == "mffc" {
                let row: RawMffcManifestLine = serde_json::from_str(line)?;
                Ok((row.sha256, Some(row.rank)))
            } else {
                let row: RawBoolConeManifestLine = serde_json::from_str(line)?;
                Ok((row.sha256, None))
            }
        })
        .collect()
}

fn collect_cones(
    opts: &DslxCorpusIngestCli,
    root: &Path,
    function: &SourceFunction,
    kind: &str,
    manifest: &mut IngestManifest,
) -> Result<()> {
    let raw_dir = root
        .join("raw")
        .join(&function.optimized_ir_sha256)
        .join(kind);
    fs::create_dir_all(&raw_dir)?;
    let manifest_path = raw_dir.join("manifest.jsonl");
    if !manifest_path.exists() {
        let mut command = driver_command(
            opts,
            if kind == "mffc" {
                "ir-fn-mffcs"
            } else {
                "ir-bool-cones"
            },
        );
        command
            .arg("--top")
            .arg(&function.ir_top)
            .arg("--output_dir")
            .arg(&raw_dir)
            .arg("--manifest_jsonl")
            .arg(&manifest_path)
            .arg("--emit_pos_data")
            .arg("false");
        if kind == "mffc" {
            command
                .arg("--max_mffcs")
                .arg(opts.max_mffcs.to_string())
                .arg("--min_internal_non_literal")
                .arg(opts.min_internal_non_literal.to_string())
                .arg("--max_frontier_non_literal")
                .arg(opts.max_frontier_non_literal.to_string());
        } else {
            command
                .arg("--k")
                .arg(
                    opts.k
                        .context("k-cone extraction requires --k")?
                        .to_string(),
                )
                .arg("--max_cones")
                .arg(opts.max_k_cones.to_string());
        }
        command.arg(root.join(&function.optimized_ir_relpath));
        let log = root
            .join("logs")
            .join(format!("{}-{kind}.stderr", function.optimized_ir_sha256));
        run_driver(command, &log)?;
    }
    for (driver_hash, rank) in cone_rows(&manifest_path, kind)? {
        if driver_hash.len() != 64 || !driver_hash.bytes().all(|b| b.is_ascii_hexdigit()) {
            bail!("cone extractor returned an invalid content hash");
        }
        let raw_file = raw_dir.join(format!("{driver_hash}.ir"));
        let bytes = fs::read(&raw_file).context("cone manifest referenced a missing IR file")?;
        let content_sha256 = sha256(&bytes);
        let ir_top =
            infer_ir_top_function(&raw_file).context("cone IR has no unique top function")?;
        let ir_op_count = parse_ir_fn_op_count_from_file(&raw_file, &ir_top)?;
        if kind != "mffc" && ir_op_count.is_none_or(|count| count > opts.max_k_ir_ops) {
            manifest.counts.k_oversize_skipped += 1;
            continue;
        }
        let cone_relpath = format!("cones/{kind}-{content_sha256}.ir");
        let destination = root.join(&cone_relpath);
        if destination.exists() {
            if fs::read(&destination)? != bytes {
                bail!("cone content identity collision");
            }
        } else {
            fs::write(destination, bytes)?;
        }
        manifest.cones.push(ConeOccurrence {
            kind: kind.to_string(),
            cone_relpath,
            content_sha256,
            driver_cone_sha256: driver_hash,
            source_relpath: function.source_relpath.clone(),
            source_fn: function.name.clone(),
            source_ir_sha256: function.optimized_ir_sha256.clone(),
            ir_top,
            rank,
            ir_op_count,
        });
    }
    Ok(())
}

pub(crate) fn ingest(repo_root: &Path, opts: &DslxCorpusIngestCli) -> Result<IngestSummary> {
    if opts.max_files == 0
        || opts.max_functions == 0
        || (!opts.no_mffcs && opts.max_mffcs == 0)
        || (opts.k.is_some()
            && (opts.k == Some(0) || opts.max_k_cones == 0 || opts.max_k_ir_ops == 0))
        || (opts.no_mffcs && opts.k.is_none())
    {
        bail!("choose at least one extraction kind and positive work limits");
    }
    let (input, output) = checked_output_dir(&opts.input_dir, &opts.output_dir, repo_root)?;
    let mut version = Command::new(&opts.driver);
    version.arg("--version");
    let version = version
        .output()
        .context("starting xlsynth-driver --version")?;
    if !version.status.success() {
        bail!("xlsynth-driver --version failed");
    }
    let version = String::from_utf8(version.stdout)?.trim().to_string();
    if version.is_empty() {
        bail!("xlsynth-driver --version returned no version");
    }

    fs::create_dir_all(output.join("sources"))?;
    fs::create_dir_all(output.join("optimized"))?;
    fs::create_dir_all(output.join("cones"))?;
    fs::create_dir_all(output.join("logs"))?;
    let files = snapshot_sources(&input, &output.join("sources"))?;
    if files.is_empty() {
        bail!("DSLX input directory has no .x files");
    }
    let tree_digest = sha256(
        files
            .iter()
            .map(|f| format!("{}\0{}\n", f.relpath, f.sha256))
            .collect::<String>()
            .as_bytes(),
    );
    let mut manifest = IngestManifest {
        schema_version: 1,
        driver_version: version,
        source_tree_sha256: tree_digest,
        settings: IngestSettings {
            max_files: opts.max_files,
            max_functions: opts.max_functions,
            max_mffcs: opts.max_mffcs,
            min_internal_non_literal: opts.min_internal_non_literal,
            max_frontier_non_literal: opts.max_frontier_non_literal,
            k: opts.k,
            max_k_cones: opts.max_k_cones,
            max_k_ir_ops: opts.max_k_ir_ops,
            mffcs: !opts.no_mffcs,
            optimized_dslx_to_ir: true,
        },
        files,
        counts: IngestCounts::default(),
        functions: Vec::new(),
        cones: Vec::new(),
        failures: Vec::new(),
    };
    let mut attempted = 0;
    for file in manifest
        .files
        .iter()
        .take(opts.max_files)
        .map(|f| f.relpath.clone())
        .collect::<Vec<_>>()
    {
        let input_file = output.join("sources").join(&file);
        let mut list = driver_command(opts, "dslx-list-fns");
        list.arg("--dslx_input_file")
            .arg(&input_file)
            .arg("--format")
            .arg("json");
        import_args(&mut list, &input_file, &output.join("sources"), opts);
        let log = output
            .join("logs")
            .join(format!("{}-list.stderr", sha256(file.as_bytes())));
        let discovered = (|| -> Result<Vec<serde_json::Value>> {
            let bytes = run_driver(list, &log)?;
            Ok(serde_json::from_slice(&bytes).context("parsing function discovery JSON")?)
        })();
        let discovered = match discovered {
            Ok(v) => v,
            Err(err) => {
                report_failure(&mut manifest, &file, None, "list_functions", &err);
                continue;
            }
        };
        manifest.counts.discovered_functions += discovered.len();
        for entry in discovered {
            let Some(name) = entry.get("name").and_then(|v| v.as_str()) else {
                manifest.failures.push(IngestFailure {
                    source_relpath: file.clone(),
                    source_fn: None,
                    stage: "list_functions".into(),
                    reason: "function discovery returned an entry without a name".into(),
                });
                continue;
            };
            if entry
                .get("is_parametric")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                manifest.counts.parametric_skipped += 1;
                continue;
            }
            if attempted >= opts.max_functions {
                manifest.counts.function_limit_reached = true;
                continue;
            }
            attempted += 1;
            manifest.counts.attempted_concrete_functions += 1;
            let mut convert = driver_command(opts, "dslx2ir");
            convert
                .arg("--dslx_input_file")
                .arg(&input_file)
                .arg("--dslx_top")
                .arg(name)
                .arg("--opt")
                .arg("true");
            import_args(&mut convert, &input_file, &output.join("sources"), opts);
            let log = output.join("logs").join(format!(
                "{}-convert.stderr",
                sha256(format!("{file}\0{name}").as_bytes())
            ));
            let result = (|| -> Result<SourceFunction> {
                let unstripped_ir = run_driver(convert, &log)?;
                if unstripped_ir.is_empty() {
                    bail!("optimized DSLX conversion returned empty IR");
                }
                // DSLX conversion includes absolute source paths in the file_number table.
                // Strip position data with the driver before assigning portable content IDs.
                let temp = output.join("optimized").join(format!(
                    ".unstripped-{}.ir",
                    sha256(format!("{file}\0{name}").as_bytes())
                ));
                fs::write(&temp, unstripped_ir)?;
                let mut strip = driver_command(opts, "ir-strip-pos-data");
                strip.arg(&temp);
                let stripped = run_driver(
                    strip,
                    &output.join("logs").join(format!(
                        "{}-strip.stderr",
                        sha256(format!("{file}\0{name}").as_bytes())
                    )),
                );
                fs::remove_file(&temp)?;
                let ir = stripped?;
                let hash = sha256(&ir);
                let ir_relpath = format!("optimized/{hash}.ir");
                let path = output.join(&ir_relpath);
                if path.exists() {
                    if fs::read(&path)? != ir {
                        bail!("optimized IR content identity collision");
                    }
                } else {
                    fs::write(&path, ir)?;
                }
                let ir_top = infer_ir_top_function(&path)?;
                Ok(SourceFunction {
                    source_relpath: file.clone(),
                    name: name.to_string(),
                    optimized_ir_relpath: ir_relpath,
                    optimized_ir_sha256: hash,
                    ir_top,
                })
            })();
            let function = match result {
                Ok(v) => v,
                Err(err) => {
                    report_failure(&mut manifest, &file, Some(name), "dslx2ir", &err);
                    continue;
                }
            };
            if !opts.no_mffcs {
                if let Err(err) = collect_cones(opts, &output, &function, "mffc", &mut manifest) {
                    report_failure(&mut manifest, &file, Some(name), "mffc", &err);
                }
            }
            if let Some(k) = opts.k {
                let kind = format!("k{k}");
                if let Err(err) = collect_cones(opts, &output, &function, &kind, &mut manifest) {
                    report_failure(&mut manifest, &file, Some(name), "k_cones", &err);
                }
            }
            manifest.functions.push(function);
        }
    }
    manifest.cones.sort_by(|a, b| {
        (
            &a.kind,
            &a.cone_relpath,
            &a.source_relpath,
            &a.source_fn,
            a.rank,
        )
            .cmp(&(
                &b.kind,
                &b.cone_relpath,
                &b.source_relpath,
                &b.source_fn,
                b.rank,
            ))
    });
    manifest
        .functions
        .sort_by(|a, b| (&a.source_relpath, &a.name).cmp(&(&b.source_relpath, &b.name)));
    let unique_cones = manifest
        .cones
        .iter()
        .map(|c| &c.cone_relpath)
        .collect::<BTreeSet<_>>()
        .len();
    let summary = IngestSummary {
        output_dir: output.display().to_string(),
        manifest: output.join("manifest.json").display().to_string(),
        cone_dir: output.join("cones").display().to_string(),
        source_files: manifest.files.len(),
        attempted_functions: attempted,
        unique_cones,
        cone_occurrences: manifest.cones.len(),
        failures: manifest.failures.len(),
    };
    fs::write(
        output.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(summary)
}

#[derive(Debug, Serialize)]
pub(crate) struct ReportSummary {
    output_dir: String,
    html: String,
    input_files: usize,
    optimized_functions: usize,
    unique_cones: usize,
    completed_pairs: usize,
    ingest_failures: usize,
    comparison_failures: usize,
}

#[derive(Serialize)]
struct ReportOrigin<'a> {
    file: &'a str,
    function: &'a str,
    source_ir_sha256: &'a str,
    rank: Option<u64>,
}

#[derive(Serialize)]
struct ReportSample<'a> {
    sample_id: &'a str,
    cone_ir: String,
    kind: &'a str,
    ir_top: &'a str,
    ir_op_count: Option<u64>,
    origins: Vec<ReportOrigin<'a>>,
    g8r_nodes: f64,
    g8r_depth: f64,
    yosys_nodes: f64,
    yosys_depth: f64,
    g8r_stats_action_id: &'a str,
    yosys_stats_action_id: &'a str,
    g8r_abc_aig_action_id: &'a str,
    yosys_aig_action_id: &'a str,
    driver_crate_version: &'a str,
    dso_version: &'a str,
    yosys_script_sha256: &'a str,
}

fn valid_cone_path(path: &str) -> bool {
    // The suffix becomes both a filesystem filename and a URL. Accept only the
    // canonical filename spelling emitted by ingestion, without normalization.
    let Some(stem) = path
        .strip_prefix("cones/")
        .and_then(|p| p.strip_suffix(".ir"))
    else {
        return false;
    };
    let Some((kind, hash)) = stem.split_once('-') else {
        return false;
    };
    let valid_kind = kind == "mffc"
        || kind
            .strip_prefix('k')
            .is_some_and(|k| k.parse::<u32>().is_ok_and(|n| n > 0 && n.to_string() == k));
    valid_kind
        && hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn read_jsonl(path: &Path) -> Result<Vec<serde_json::Value>> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).context("parsing corpus JSONL row"))
        .collect()
}

fn field<'a>(row: &'a serde_json::Value, name: &str) -> Result<&'a str> {
    row.get(name)
        .and_then(|v| v.as_str())
        .with_context(|| format!("comparison row missing {name}"))
}

fn number(row: &serde_json::Value, name: &str) -> Result<f64> {
    let n = row
        .get(name)
        .and_then(|v| v.as_f64())
        .with_context(|| format!("comparison row missing numeric {name}"))?;
    if !n.is_finite() || n < 0.0 {
        bail!("comparison row has invalid {name}");
    }
    Ok(n)
}

fn matching_field(lhs: &serde_json::Value, rhs: &serde_json::Value, name: &str) -> Result<()> {
    let value = lhs
        .get(name)
        .with_context(|| format!("comparison export missing {name}"))?;
    if rhs.get(name) != Some(value) {
        bail!("comparison exports disagree on {name}; refresh the comparison exports");
    }
    Ok(())
}

fn validate_comparison_exports(
    manifest: &serde_json::Value,
    statuses: &[serde_json::Value],
    joined: &[serde_json::Value],
) -> Result<()> {
    let manifest_samples = manifest
        .get("samples")
        .and_then(|v| v.as_array())
        .context("comparison manifest is missing samples")?;
    if manifest_samples != statuses {
        bail!("comparison manifest and samples disagree; refresh the comparison exports");
    }
    let mut completed = BTreeMap::new();
    for status in statuses {
        if field(status, "preset")? != field(manifest, "recipe_preset")? {
            bail!("comparison sample recipe disagrees with manifest");
        }
        for name in [
            "dso_version",
            "yosys_script",
            "yosys_script_sha256",
            "fraig",
        ] {
            matching_field(manifest, status, name)?;
        }
        for (runtime, version_field) in [
            ("driver_runtime", "driver_crate_version"),
            ("stats_runtime", "stats_driver_crate_version"),
        ] {
            let version = manifest.get(runtime).and_then(|v| v.get("driver_version"));
            if version.is_none() || version != status.get(version_field) {
                bail!("comparison sample runtime disagrees with manifest");
            }
        }
        if field(status, "status")? == "done"
            && completed
                .insert(field(status, "sample_id")?, status)
                .is_some()
        {
            bail!("comparison contains duplicate completed samples");
        }
    }
    for row in joined {
        let status = completed
            .remove(field(row, "sample_id")?)
            .context("joined row is duplicate or has no completed sample")?;
        for name in [
            "source_relpath",
            "source_sha256",
            "top_fn_name",
            "fraig",
            "dso_version",
            "driver_crate_version",
            "stats_driver_crate_version",
            "yosys_script",
            "yosys_script_sha256",
            "import_ir_action_id",
            "g8r_aig_action_id",
            "g8r_abc_aig_action_id",
            "g8r_stats_action_id",
            "combo_verilog_action_id",
            "yosys_abc_aig_action_id",
            "yosys_abc_stats_action_id",
            "aig_stat_diff_action_id",
        ] {
            matching_field(status, row, name)?;
        }
    }
    if !completed.is_empty() {
        bail!("joined export omits completed samples; refresh the comparison exports");
    }
    Ok(())
}

const REPORT_STYLE: &str = r#"
:root { color-scheme: dark; font-family: system-ui, sans-serif; background:#0b1421; color:#e7f3fa; }
body { max-width:1300px; margin:0 auto; padding:1.5rem; }
h1,h2 { margin:.6rem 0; } p,.muted { color:#a9bfd1; }
.grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:.7rem; margin:1rem 0; }
.card,section { background:#142337; border:1px solid #31475a; padding:1rem; border-radius:.5rem; }
.card strong { display:block; font-size:1.45rem; margin-top:.3rem; }
label { display:inline-flex; gap:.4rem; align-items:center; margin:0 .7rem .7rem 0; }
select,input { background:#0b1421; color:#e7f3fa; border:1px solid #5d829a; padding:.4rem; }
svg { width:100%; height:auto; background:#0c1a2a; border:1px solid #31475a; }
table { border-collapse:collapse; width:100%; } th,td { padding:.45rem; border-bottom:1px solid #31475a; text-align:left; }
.scroll { overflow:auto; max-height:520px; } tr[data-id] { cursor:pointer; } tr[data-id]:hover { background:#284358; }
pre { overflow:auto; max-height:440px; white-space:pre; background:#091423; padding:1rem; }
a { color:#7bd4ff; } code { overflow-wrap:anywhere; } .cols { display:grid; grid-template-columns:repeat(auto-fit,minmax(380px,1fr)); gap:1rem; }
.win { color:#9ceec0; } .loss { color:#ff9bb0; } .mixed { color:#f5d28d; }
"#;

pub(crate) fn render_report(
    repo_root: &Path,
    ingest_dir: &Path,
    comparison_dir: &Path,
    output_dir: &Path,
) -> Result<ReportSummary> {
    let output_dir = checked_empty_output(output_dir, &[repo_root, ingest_dir, comparison_dir])?;
    let ingest: IngestManifest =
        serde_json::from_slice(&fs::read(ingest_dir.join("manifest.json"))?)
            .context("reading DSLX ingest manifest")?;
    if ingest.schema_version != 1 || !ingest.settings.optimized_dslx_to_ir {
        bail!("unsupported DSLX ingest manifest");
    }
    let comparison: serde_json::Value =
        serde_json::from_slice(&fs::read(comparison_dir.join("manifest.json"))?)
            .context("reading IR comparison manifest")?;
    if field(&comparison, "recipe_preset")? != "g8r-abc-vs-yabc-aig-diff" {
        bail!("report requires the g8r-abc-vs-yabc-aig-diff recipe");
    }
    let joined = read_jsonl(&comparison_dir.join("joined/g8r-abc-vs-yabc-aig-diff.jsonl"))?;
    let statuses = read_jsonl(&comparison_dir.join("samples.jsonl"))?;
    validate_comparison_exports(&comparison, &statuses, &joined)?;
    let is_failed = |row: &&serde_json::Value| {
        matches!(
            row.get("status").and_then(|v| v.as_str()),
            Some("failed" | "canceled")
        )
    };
    let comparison_failures = statuses.iter().filter(is_failed).count();
    let mut by_cone = BTreeMap::<&str, Vec<&ConeOccurrence>>::new();
    for occurrence in &ingest.cones {
        if !valid_cone_path(&occurrence.cone_relpath) {
            bail!("ingest manifest has an invalid cone path");
        }
        by_cone
            .entry(occurrence.cone_relpath.strip_prefix("cones/").unwrap())
            .or_default()
            .push(occurrence);
    }
    let mut by_sample_id = BTreeMap::new();
    for status in &statuses {
        let cone = field(status, "source_relpath")?;
        if !by_cone.contains_key(cone) {
            bail!("comparison contains a cone absent from this ingest manifest");
        }
        if by_sample_id
            .insert(field(status, "sample_id")?, status)
            .is_some()
        {
            bail!("comparison has duplicate sample IDs");
        }
    }
    let mut linked_ir = BTreeMap::<String, Vec<u8>>::new();
    let mut samples = Vec::new();
    for row in &joined {
        let source_relpath = field(row, "source_relpath")?;
        let status = by_sample_id
            .get(field(row, "sample_id")?)
            .context("joined comparison sample is missing from samples.jsonl")?;
        if field(status, "source_relpath")? != source_relpath || field(status, "status")? != "done"
        {
            bail!("joined comparison sample does not match its completed status");
        }
        let occurrences = by_cone
            .get(source_relpath)
            .with_context(|| format!("comparison sample has no source cone: {source_relpath}"))?;
        let first = occurrences[0];
        let ir_path = ingest_dir.join(&first.cone_relpath);
        let ir_bytes = fs::read(&ir_path).context("reading source cone IR")?;
        if sha256(&ir_bytes) != first.content_sha256
            || field(row, "source_sha256")? != first.content_sha256
        {
            bail!("cone IR digest disagrees with ingest manifest");
        }
        linked_ir.insert(source_relpath.to_string(), ir_bytes);
        let abc_id = field(row, "g8r_abc_aig_action_id")?;
        if abc_id.is_empty() {
            bail!("comparison row is missing G8r ABC evidence");
        }
        samples.push(ReportSample {
            sample_id: field(row, "sample_id")?,
            cone_ir: format!("ir/{source_relpath}"),
            kind: &first.kind,
            ir_top: field(row, "top_fn_name")?,
            ir_op_count: first.ir_op_count,
            origins: occurrences
                .iter()
                .map(|c| ReportOrigin {
                    file: &c.source_relpath,
                    function: &c.source_fn,
                    source_ir_sha256: &c.source_ir_sha256,
                    rank: c.rank,
                })
                .collect(),
            g8r_nodes: number(row, "g8r_and_nodes")?,
            g8r_depth: number(row, "g8r_depth")?,
            yosys_nodes: number(row, "yosys_abc_and_nodes")?,
            yosys_depth: number(row, "yosys_abc_depth")?,
            g8r_stats_action_id: field(row, "g8r_stats_action_id")?,
            yosys_stats_action_id: field(row, "yosys_abc_stats_action_id")?,
            g8r_abc_aig_action_id: abc_id,
            yosys_aig_action_id: field(row, "yosys_abc_aig_action_id")?,
            driver_crate_version: field(row, "driver_crate_version")?,
            dso_version: field(row, "dso_version")?,
            yosys_script_sha256: field(row, "yosys_script_sha256")?,
        });
    }
    let unique_cones = by_cone.len();
    let failed_rows = statuses
        .iter()
        .filter(is_failed)
        .map(|row| {
            let stages = [
                "g8r_aig",
                "g8r_abc_aig",
                "g8r_stats",
                "combo_verilog",
                "yosys_abc_aig",
                "yosys_abc_stats",
                "aig_stat_diff",
            ]
            .iter()
            .filter(|name| {
                row.get(format!("{name}_status")).and_then(|v| v.as_str()) == Some("failed")
            })
            .collect::<Vec<_>>();
            serde_json::json!({
                "cone": row.get("source_relpath"),
                "reason": "comparison failed; inspect the local runner workspace",
                "stages": stages,
            })
        })
        .collect::<Vec<_>>();
    let coverage = serde_json::json!({
        "input_files": ingest.files.len(),
        "scanned_files": ingest.files.len().min(ingest.settings.max_files),
        "discovered_functions": ingest.counts.discovered_functions,
        "attempted_functions": ingest.counts.attempted_concrete_functions,
        "parametric_skipped": ingest.counts.parametric_skipped,
        "k_oversize_skipped": ingest.counts.k_oversize_skipped,
        "function_limit_reached": ingest.counts.function_limit_reached,
        "optimized_functions": ingest.functions.len(),
        "unique_cones": unique_cones,
        "cone_occurrences": ingest.cones.len(),
        "completed_pairs": samples.len(),
        "comparison_samples": statuses.len(),
        "ingest_failures": ingest.failures,
        "comparison_failures": failed_rows,
        "driver_version": ingest.driver_version,
    });
    let dataset = serde_json::json!({"coverage": coverage, "samples": samples});
    let dataset_text = serde_json::to_string(&dataset)?;
    fs::create_dir_all(output_dir.join("ir"))?;
    for (name, bytes) in linked_ir {
        fs::write(output_dir.join("ir").join(name), bytes)?;
    }
    fs::write(
        output_dir.join("data.json"),
        serde_json::to_vec_pretty(&dataset)?,
    )?;
    let embedded = dataset_text
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026");
    let html = format!(
        r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'">
  <title>DSLX cone comparison</title>
  <style>{REPORT_STYLE}</style>
</head>
<body>
  <header>
    <h1>DSLX cone comparison</h1>
    <p>Optimized DSLX IR → MFFCs and optional k-cones → G8r+ABC versus codegen+Yosys/ABC. Cone occurrences can overlap.</p>
  </header>
  <main>
    <div id="coverage" class="grid"></div>
    <section>
      <h2>Paired samples</h2>
      <label>Kind <select id="kind"><option value="">all</option></select></label>
      <label>Outcome <select id="outcome"><option value="">all</option><option value="loss">G8r loses both</option><option value="win">G8r wins both</option><option value="mixed">mixed or tie</option></select></label>
      <label>Maximum IR ops <input id="max-ops" type="number" min="0" placeholder="all"></label>
      <p id="shown" class="muted"></p>
      <div class="cols">
        <div><h2>AND nodes</h2><svg id="nodes" viewBox="0 0 520 400" aria-label="G8r+ABC versus Yosys/ABC nodes"></svg></div>
        <div><h2>Depth</h2><svg id="depth" viewBox="0 0 520 400" aria-label="G8r+ABC versus Yosys/ABC depth"></svg></div>
      </div>
      <div class="scroll"><table>
        <thead><tr><th>Kind</th><th>Source function</th><th>IR ops</th><th>G8r nodes/depth</th><th>Yosys nodes/depth</th><th>Outcome</th></tr></thead>
        <tbody id="rows"></tbody>
      </table></div>
    </section>
    <section><h2>Selected cone</h2><div id="detail" class="muted">Select a point or row to inspect the cone and evidence.</div></section>
    <section><h2>Coverage and failures</h2><div id="failures"></div></section>
  </main>
  <script id="dataset" type="application/json">{embedded}</script>
  <script>{}</script>
</body>
</html>"##,
        include_str!("dslx_corpus_report.js")
    );
    fs::write(output_dir.join("index.html"), html)?;
    Ok(ReportSummary {
        output_dir: output_dir.display().to_string(),
        html: output_dir.join("index.html").display().to_string(),
        input_files: ingest.files.len(),
        optimized_functions: ingest.functions.len(),
        unique_cones,
        completed_pairs: joined.len(),
        ingest_failures: ingest.failures.len(),
        comparison_failures,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("bvc-dslx-test-{}-{stamp}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn comparison_fixture(
        cone_name: &str,
        digest: &str,
    ) -> (serde_json::Value, serde_json::Value, serde_json::Value) {
        let row = serde_json::json!({
            "sample_id": "one", "source_relpath": cone_name, "source_sha256": digest,
            "top_fn_name": "cone", "fraig": false, "g8r_and_nodes": 9, "g8r_depth": 3,
            "yosys_abc_and_nodes": 12, "yosys_abc_depth": 4,
            "import_ir_action_id": "import", "g8r_aig_action_id": "g8r",
            "g8r_stats_action_id": "g8r-stats", "yosys_abc_stats_action_id": "yosys-stats",
            "g8r_abc_aig_action_id": "g8r-abc", "yosys_abc_aig_action_id": "yosys-abc",
            "combo_verilog_action_id": "combo", "aig_stat_diff_action_id": "diff",
            "driver_crate_version": "0.1", "stats_driver_crate_version": "0.1", "dso_version": "v0.1",
            "yosys_script": "flows/yosys_to_aig.ys", "yosys_script_sha256": "script-digest"
        });
        let mut status = row.clone();
        status["status"] = serde_json::json!("done");
        status["preset"] = serde_json::json!("g8r-abc-vs-yabc-aig-diff");
        let manifest = serde_json::json!({
            "recipe_preset": "g8r-abc-vs-yabc-aig-diff", "samples": [status],
            "fraig": false, "dso_version": "v0.1", "yosys_script": "flows/yosys_to_aig.ys",
            "yosys_script_sha256": "script-digest", "driver_runtime": {"driver_version": "0.1"},
            "stats_runtime": {"driver_version": "0.1"}
        });
        (manifest, status, row)
    }

    #[test]
    fn report_rejects_stale_duplicate_and_incomplete_exports() {
        let (manifest, status, row) = comparison_fixture("mffc-example.ir", "digest");
        validate_comparison_exports(&manifest, &[status.clone()], &[row.clone()]).unwrap();
        for name in [
            "g8r_abc_aig_action_id",
            "g8r_stats_action_id",
            "source_sha256",
            "driver_crate_version",
        ] {
            let mut stale = row.clone();
            stale[name] = serde_json::json!("old-run");
            assert!(
                validate_comparison_exports(&manifest, &[status.clone()], &[stale]).is_err(),
                "{name}"
            );
        }
        assert!(
            validate_comparison_exports(&manifest, &[status.clone()], &[row.clone(), row.clone()])
                .is_err()
        );
        assert!(validate_comparison_exports(&manifest, &[status.clone()], &[]).is_err());
        let mut stale_status = status.clone();
        stale_status["g8r_abc_aig_action_id"] = serde_json::json!("old-run");
        assert!(validate_comparison_exports(&manifest, &[stale_status], &[row.clone()]).is_err());
        let mut stale_manifest = manifest.clone();
        stale_manifest["driver_runtime"]["driver_version"] = serde_json::json!("old-version");
        assert!(validate_comparison_exports(&stale_manifest, &[status], &[row]).is_err());
    }

    #[test]
    fn cone_paths_must_have_the_canonical_relative_spelling() {
        let filename = format!("mffc-{}.ir", "a".repeat(64));
        assert!(valid_cone_path(&format!("cones/{filename}")));
        for path in [
            format!("cones//{filename}"),
            format!("cones/./{filename}"),
            format!("cones/../{filename}"),
            format!("cones/dir/{filename}"),
            format!("cones/\\{filename}"),
            format!("/cones/{filename}"),
            format!("cones/{filename}?query"),
            format!("cones/k0-{}.ir", "a".repeat(64)),
        ] {
            assert!(!valid_cone_path(&path), "{path}");
        }
    }

    #[test]
    fn corpus_output_rejects_resource_and_input_overlap_before_writes() {
        let root = temp_dir();
        let resource = root.join("resource");
        let input = root.join("input");
        fs::create_dir(&resource).unwrap();
        fs::create_dir(&input).unwrap();
        for output in [
            resource.join("corpus"),
            resource.join("new/nested"),
            resource.join("new/../report"),
            input.join("report"),
            root.clone(),
        ] {
            assert!(checked_output_dir(&input, &output, &resource).is_err());
        }
        assert_eq!(fs::read_dir(&resource).unwrap().count(), 0);
        assert_eq!(fs::read_dir(&input).unwrap().count(), 0);
        assert!(checked_output_dir(&input, &root.join("outside/new"), &resource).is_ok());
        // Rendering must reject before trying to read even a missing manifest.
        let error = render_report(
            &resource,
            &input,
            &root.join("comparison"),
            &resource.join("report"),
        )
        .unwrap_err();
        assert!(error.to_string().contains("overlap"));
        #[cfg(unix)]
        {
            let alias = root.join("alias");
            std::os::unix::fs::symlink(&resource, &alias).unwrap();
            assert!(checked_output_dir(&input, &alias.join("report"), &resource).is_err());
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn nested_sources_search_sibling_imports_before_corpus_root() {
        use clap::Parser;
        let crate::cli::TopCommand::DslxCorpusIngest(opts) = crate::cli::Cli::try_parse_from([
            "xlsynth-bvc",
            "dslx-corpus-ingest",
            "--input-dir",
            "input",
            "--output-dir",
            "output",
            "--dslx-path",
            "extra",
        ])
        .unwrap()
        .command
        else {
            panic!("ingest options");
        };
        for subcommand in ["dslx-list-fns", "dslx2ir"] {
            let mut command = driver_command(&opts, subcommand);
            import_args(
                &mut command,
                Path::new("sources/sub/main.x"),
                Path::new("sources"),
                &opts,
            );
            let args: Vec<_> = command.get_args().collect();
            let index = args.iter().position(|arg| *arg == "--dslx_path").unwrap();
            assert_eq!(args[index + 1], "sources/sub;sources;extra");
        }
    }

    #[test]
    fn report_joins_cone_and_comparison_without_inlining_source_markup() {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("bvc-report-test-{}-{stamp}", std::process::id()));
        let ingest_dir = root.join("ingest");
        let compare_dir = root.join("compare");
        let output_dir = root.join("report");
        fs::create_dir_all(ingest_dir.join("cones")).expect("cone directory");
        fs::create_dir_all(compare_dir.join("joined")).expect("joined directory");
        let ir = b"package example\n\ntop fn cone(x: bits[1]) -> bits[1] {\n  ret x: bits[1] = identity(x)\n}\n";
        let digest = sha256(ir);
        let cone_name = format!("mffc-{digest}.ir");
        fs::write(ingest_dir.join("cones").join(&cone_name), ir).expect("write cone");
        let ingest = serde_json::json!({
            "schema_version": 1, "driver_version": "0.1", "source_tree_sha256": "a",
            "settings": {"max_files": 2, "max_functions": 2, "max_mffcs": 2,
                "min_internal_non_literal": 1, "max_frontier_non_literal": 0, "k": null,
                "max_k_cones": 1, "max_k_ir_ops": 10, "mffcs": true,
                "optimized_dslx_to_ir": true},
            "files": [{"relpath": "input.x", "sha256": "a"}],
            "functions": [{"source_relpath": "input.x", "name": "<script>unsafe</script>",
                "optimized_ir_relpath": "optimized/one.ir", "optimized_ir_sha256": "b", "ir_top": "top"}],
            "cones": [{"kind": "mffc", "cone_relpath": format!("cones/{cone_name}"),
                "content_sha256": digest, "driver_cone_sha256": digest,
                "source_relpath": "input.x", "source_fn": "<script>unsafe</script>",
                "source_ir_sha256": "b", "ir_top": "cone", "rank": 1, "ir_op_count": 1}],
            "failures": []
        });
        fs::write(
            ingest_dir.join("manifest.json"),
            serde_json::to_vec(&ingest).unwrap(),
        )
        .unwrap();
        let (comparison, status, row) = comparison_fixture(&cone_name, &digest);
        fs::write(
            compare_dir.join("manifest.json"),
            serde_json::to_vec(&comparison).unwrap(),
        )
        .unwrap();
        fs::write(compare_dir.join("samples.jsonl"), format!("{status}\n")).unwrap();
        fs::write(
            compare_dir.join("joined/g8r-abc-vs-yabc-aig-diff.jsonl"),
            format!("{row}\n"),
        )
        .unwrap();
        let resource = root.join("resource");
        fs::create_dir(&resource).unwrap();
        let result = render_report(&resource, &ingest_dir, &compare_dir, &output_dir)
            .expect("render report");
        assert_eq!(result.completed_pairs, 1);
        assert_eq!(
            fs::read(output_dir.join("ir").join(&cone_name)).unwrap(),
            ir
        );
        let html = fs::read_to_string(output_dir.join("index.html")).unwrap();
        assert!(html.contains("\\u003cscript\\u003eunsafe\\u003c/script\\u003e"));
        assert!(!html.contains("<script>unsafe</script>"));
        assert!(!html.contains(&root.display().to_string()));
        assert!(html.contains("G8r+ABC"));
        let data: serde_json::Value =
            serde_json::from_slice(&fs::read(output_dir.join("data.json")).unwrap()).unwrap();
        assert_eq!(
            data["samples"][0]["origins"][0]["function"],
            "<script>unsafe</script>"
        );
        fs::remove_dir_all(root).expect("cleanup");
    }
}
