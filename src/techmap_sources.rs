// SPDX-License-Identifier: Apache-2.0

//! Generate version-specific default g8r and codegen/ABC inputs without rebuilding
//! unrelated corpus leaves. Cached source artifacts have a typed, hashed boundary.

use anyhow::{Context, Result, bail};
use fs2::FileExt;
use prost::Message;
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::techmap_proto::wire::SourceArtifact;

pub(crate) struct SourcePreparation<'a> {
    pub(crate) input_ir: &'a Path,
    pub(crate) input_ir_container: &'a str,
    pub(crate) top_fn_name: &'a str,
    pub(crate) source_abc_aig: &'a Path,
    pub(crate) source_combo_verilog_sha256: &'a [u8],
    pub(crate) source_dso_version: &'a str,
    pub(crate) target_dso_version: &'a str,
    pub(crate) driver_image_id: &'a str,
    pub(crate) abc_image_id: &'a str,
    pub(crate) cache_dir: &'a Path,
    pub(crate) cache_container_root: &'a str,
    pub(crate) driver_container: &'a str,
    pub(crate) abc_container: &'a str,
    pub(crate) release_dir_container: &'a str,
    pub(crate) timeout_secs: u64,
}

pub(crate) struct PreparedSources {
    pub(crate) g8r_path: PathBuf,
    pub(crate) abc_path: PathBuf,
}

fn quoted(text: &str) -> String {
    format!("'{}'", text.replace('\'', "'\\''"))
}

fn read_digest(path: &Path) -> Result<Vec<u8>> {
    Ok(Sha256::digest(fs::read(path)?).to_vec())
}

fn shell(container: &str, timeout_secs: u64, body: &str) -> Result<()> {
    let status = Command::new("timeout")
        .args([
            "--signal=TERM",
            "--kill-after=10s",
            &format!("{}s", timeout_secs.saturating_add(30)),
        ])
        .args([
            "docker",
            "exec",
            container,
            "timeout",
            "--signal=TERM",
            "--kill-after=10s",
            &format!("{timeout_secs}s"),
            "bash",
            "-c",
            body,
        ])
        .status()
        .context("executing source-generation container")?;
    if !status.success() {
        bail!(
            "source generation failed with exit {}",
            status.code().unwrap_or(-1)
        );
    }
    Ok(())
}

fn expected_record(
    options: &SourcePreparation<'_>,
    input_sha: &[u8],
    kind: &str,
) -> SourceArtifact {
    SourceArtifact {
        record_version: 1,
        kind: kind.to_string(),
        source_ir_sha256: input_sha.to_vec(),
        top_fn_name: options.top_fn_name.to_string(),
        driver_image_id: options.driver_image_id.to_string(),
        abc_image_id: if kind == "codegen-yosys-abc" {
            options.abc_image_id.to_string()
        } else {
            String::new()
        },
        dso_version: options
            .target_dso_version
            .trim_start_matches('v')
            .to_string(),
        artifact_sha256: Vec::new(),
        recipe_revision: 1,
    }
}

fn cached(path: &Path, filename: &str, expected: &SourceArtifact) -> Result<bool> {
    let record_path = path.join("record.pb");
    let artifact = path.join(filename);
    if !record_path.exists() || !artifact.exists() {
        return Ok(false);
    }
    let bytes = fs::read(record_path)?;
    let mut record = SourceArtifact::decode(bytes.as_slice())?;
    if record.encode_to_vec() != bytes {
        bail!("noncanonical cached source record");
    }
    let digest = std::mem::take(&mut record.artifact_sha256);
    if record != *expected || digest != read_digest(&artifact)? {
        bail!("cached source artifact identity or checksum mismatch");
    }
    Ok(true)
}

fn finish(path: &Path, filename: &str, mut record: SourceArtifact) -> Result<PathBuf> {
    let artifact = path.join(filename);
    let bytes = fs::read(&artifact)?;
    let end = bytes
        .iter()
        .position(|byte| *byte == b'\n')
        .unwrap_or(bytes.len());
    let fields: Vec<_> = std::str::from_utf8(&bytes[..end])?
        .split_whitespace()
        .collect();
    if fields.len() < 6 || fields[0] != "aig" {
        bail!("source generation did not produce binary AIGER");
    }
    record.artifact_sha256 = Sha256::digest(&bytes).to_vec();
    fs::write(path.join("record.pb.tmp"), record.encode_to_vec())?;
    fs::rename(path.join("record.pb.tmp"), path.join("record.pb"))?;
    Ok(artifact)
}

fn publish(staging: &Path, directory: &Path, filename: &str) -> Result<PathBuf> {
    // Container commands write only within their unique attempt directory. Publish
    // from the host while holding the cache lock, with the record committed last.
    let artifact = directory.join(filename);
    fs::rename(staging.join(filename), &artifact)?;
    fs::rename(staging.join("record.pb"), directory.join("record.pb"))?;
    Ok(artifact)
}

fn staging_container_path(options: &SourcePreparation<'_>, staging: &Path) -> Result<String> {
    let relative = staging.strip_prefix(options.cache_dir)?;
    Ok(format!(
        "{}/{}",
        options.cache_container_root,
        relative.display()
    ))
}

fn driver_prelude(options: &SourcePreparation<'_>) -> String {
    let release = quoted(options.release_dir_container);
    format!(
        "set -euo pipefail\nexport XLSYNTH_TOOLS={release}\nexport LD_LIBRARY_PATH={release}:\"${{LD_LIBRARY_PATH:-}}\"\n"
    )
}

pub(crate) fn prepare_sources(options: SourcePreparation<'_>) -> Result<PreparedSources> {
    let input_sha = read_digest(options.input_ir)?;
    let native_record = expected_record(&options, &input_sha, "g8r-default");
    let native_key = hex::encode(Sha256::digest(native_record.encode_to_vec()));
    let native_rel = format!("sources/native/{native_key}");
    let native_dir = options.cache_dir.join(&native_rel);
    fs::create_dir_all(&native_dir)?;
    let native_lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(native_dir.join(".lock"))?;
    native_lock.lock_exclusive()?;
    let g8r_path = if cached(&native_dir, "g8r.aig", &native_record)? {
        native_dir.join("g8r.aig")
    } else {
        let staging = crate::techmap_corpus::attempt_directory(&native_dir)?;
        let container_dir = staging_container_path(&options, &staging)?;
        let script = format!(
            "{}xlsynth-driver ir2g8r {} --top {} --fraig=false --aiger-out {} > {} 2>&1\n",
            driver_prelude(&options),
            quoted(options.input_ir_container),
            quoted(options.top_fn_name),
            quoted(&format!("{container_dir}/g8r.aig")),
            quoted(&format!("{container_dir}/lower.log")),
        );
        shell(options.driver_container, options.timeout_secs, &script)?;
        finish(&staging, "g8r.aig", native_record)?;
        publish(&staging, &native_dir, "g8r.aig")?
    };
    drop(native_lock);

    let abc_path = if options.source_dso_version.trim_start_matches('v')
        == options.target_dso_version.trim_start_matches('v')
    {
        if !options.source_abc_aig.is_file() {
            bail!("source ABC AIG is missing");
        }
        options.source_abc_aig.to_path_buf()
    } else {
        let baseline_record = expected_record(&options, &input_sha, "codegen-yosys-abc");
        let baseline_key = hex::encode(Sha256::digest(baseline_record.encode_to_vec()));
        let baseline_rel = format!("sources/baseline/{baseline_key}");
        let baseline_dir = options.cache_dir.join(&baseline_rel);
        fs::create_dir_all(&baseline_dir)?;
        let baseline_lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(baseline_dir.join(".lock"))?;
        baseline_lock.lock_exclusive()?;
        if !cached(&baseline_dir, "yosys_abc.aig", &baseline_record)? {
            let staging = crate::techmap_corpus::attempt_directory(&baseline_dir)?;
            let container_dir = staging_container_path(&options, &staging)?;
            let toolchain = format!(
                "[toolchain]\ntool_path = {:?}\n",
                options.release_dir_container
            );
            fs::write(staging.join("toolchain.toml"), toolchain)?;
            let normalized_ir = crate::executor::rewrite_ir_package_top_function(
                &fs::read_to_string(options.input_ir)?,
                options.top_fn_name,
            )?;
            fs::write(staging.join("input.with_top.ir"), normalized_ir)?;
            let script = format!(
                "{}xlsynth-driver --toolchain {} ir2combo {} --delay_model unit --top {} --use_system_verilog false > {} 2> {}\n",
                driver_prelude(&options),
                quoted(&format!("{container_dir}/toolchain.toml")),
                quoted(&format!("{container_dir}/input.with_top.ir")),
                quoted(options.top_fn_name),
                quoted(&format!("{container_dir}/combo.v")),
                quoted(&format!("{container_dir}/codegen.log")),
            );
            shell(options.driver_container, options.timeout_secs, &script)?;
            // Reuse ABC only after fresh codegen proves the Verilog is identical
            // to the retained canonical source. The caller has verified the
            // source's top, ABC runtime, flow, and output AIG digest.
            if retained_baseline_matches(
                &staging.join("combo.v"),
                options.source_combo_verilog_sha256,
            )? {
                copy_retained_baseline(options.source_abc_aig, &staging)?;
                finish(&staging, "yosys_abc.aig", baseline_record)?;
                publish(&staging, &baseline_dir, "yosys_abc.aig")?;
                return Ok(PreparedSources {
                    g8r_path,
                    abc_path: baseline_dir.join("yosys_abc.aig"),
                });
            }
            // Names originate in XLS IR; reject syntax that could alter the Yosys command.
            if options.top_fn_name.is_empty()
                || !options
                    .top_fn_name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_')
            {
                bail!("unsupported Verilog module name");
            }
            let flow = format!(
                "read_verilog {container_dir}/combo.v\nhierarchy -top {}\nproc\nopt\ntechmap\nabc -g AND\nclean\nwrite_aiger {container_dir}/yosys_abc.aig\n",
                options.top_fn_name,
            );
            fs::write(staging.join("map.ys"), flow)?;
            let script = format!(
                "set -euo pipefail\nyosys -s {} > {} 2>&1\n",
                quoted(&format!("{container_dir}/map.ys")),
                quoted(&format!("{container_dir}/yosys.log"))
            );
            shell(options.abc_container, options.timeout_secs, &script)?;
            finish(&staging, "yosys_abc.aig", baseline_record)?;
            publish(&staging, &baseline_dir, "yosys_abc.aig")?;
        }
        baseline_dir.join("yosys_abc.aig")
    };
    Ok(PreparedSources { g8r_path, abc_path })
}

fn retained_baseline_matches(combo: &Path, expected_sha256: &[u8]) -> Result<bool> {
    if expected_sha256.len() != 32 {
        bail!("retained codegen output has no canonical SHA-256");
    }
    Ok(read_digest(combo)? == expected_sha256)
}

fn copy_retained_baseline(source: &Path, directory: &Path) -> Result<()> {
    let temporary = directory.join("retained.aig.tmp");
    fs::copy(source, &temporary)?;
    fs::rename(temporary, directory.join("yosys_abc.aig"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_baseline_requires_identical_fresh_codegen() {
        let temporary = std::env::temp_dir().join(format!(
            "bvc-source-reuse-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        fs::create_dir_all(&temporary).unwrap();
        let combo = temporary.join("combo.v");
        fs::write(&combo, b"module selected(); endmodule\n").unwrap();
        let expected = read_digest(&combo).unwrap();
        assert!(retained_baseline_matches(&combo, &expected).unwrap());
        fs::write(&combo, b"module changed(); endmodule\n").unwrap();
        assert!(!retained_baseline_matches(&combo, &expected).unwrap());
        assert!(retained_baseline_matches(&combo, &[]).is_err());
        fs::remove_dir_all(temporary).unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn retained_baseline_replaces_read_only_interrupted_output() {
        use std::os::unix::fs::PermissionsExt;
        let directory = std::env::temp_dir().join(format!(
            "bvc-source-interrupted-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        fs::create_dir_all(&directory).unwrap();
        let source = directory.join("canonical.aig");
        let output = directory.join("yosys_abc.aig");
        fs::write(&source, b"aig 0 0 0 0 0\n").unwrap();
        fs::write(&output, b"interrupted output").unwrap();
        fs::set_permissions(&output, fs::Permissions::from_mode(0o444)).unwrap();
        copy_retained_baseline(&source, &directory).unwrap();
        assert_eq!(fs::read(output).unwrap(), fs::read(source).unwrap());
        assert!(!directory.join("retained.aig.tmp").exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn source_record_round_trip_binds_artifact_bytes() {
        let root = std::env::temp_dir().join(format!(
            "bvc-source-record-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        fs::create_dir_all(&root).unwrap();
        let record = SourceArtifact {
            record_version: 1,
            kind: "g8r-default".to_string(),
            recipe_revision: 1,
            ..Default::default()
        };
        fs::write(root.join("g8r.aig"), b"aig 0 0 0 0 0\n").unwrap();
        finish(&root, "g8r.aig", record.clone()).unwrap();
        assert!(cached(&root, "g8r.aig", &record).unwrap());
        fs::write(root.join("g8r.aig"), b"aig 1 1 0 0 0\n").unwrap();
        assert!(cached(&root, "g8r.aig", &record).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn delayed_source_writer_cannot_mutate_retry_publication() {
        use std::io::Write;

        let root = std::env::temp_dir().join(format!(
            "bvc-source-delayed-writer-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        fs::create_dir_all(&root).unwrap();
        let stale = crate::techmap_corpus::attempt_directory(&root).unwrap();
        let retry = crate::techmap_corpus::attempt_directory(&root).unwrap();
        let mut stale_output = fs::File::create(stale.join("g8r.aig")).unwrap();
        let bytes = b"aig 0 0 0 0 0\n";
        let record = SourceArtifact {
            record_version: 1,
            kind: "g8r-default".to_string(),
            recipe_revision: 1,
            ..Default::default()
        };
        fs::write(retry.join("g8r.aig"), bytes).unwrap();
        finish(&retry, "g8r.aig", record.clone()).unwrap();
        let canonical = publish(&retry, &root, "g8r.aig").unwrap();

        // Both an already-running writer and an exec that starts after retry
        // publication still target their obsolete attempt directory.
        stale_output.write_all(b"interrupted writer").unwrap();
        fs::write(stale.join("g8r.aig"), b"delayed writer").unwrap();
        fs::write(stale.join("lower.log"), b"late diagnostics").unwrap();

        assert_eq!(fs::read(canonical).unwrap(), bytes);
        assert!(cached(&root, "g8r.aig", &record).unwrap());
        assert!(stale.join("lower.log").is_file());
        fs::remove_dir_all(root).unwrap();
    }
}
