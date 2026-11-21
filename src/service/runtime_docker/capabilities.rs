// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::proto::cache::DriverRunnerCapabilities;

pub(super) const PROBE_SCRIPT: &str = r#"
python3 - <<'PY'
import os
import runpy
from pathlib import Path
probe = runpy.run_path("/probe/worker.py")["write_capabilities"]
probe(Path("/outputs/capabilities.json"), os.environ["CAPABILITY_RUNNER_KEY"],
      os.environ["CAPABILITY_INSTANCE_ID"], os.environ["CAPABILITY_IMAGE"])
PY
"#;

pub(super) fn probe(
    repo_root: &Path,
    image: &str,
    mounts: &[DockerMount],
    env: &BTreeMap<String, String>,
    script: &str,
) -> Result<()> {
    let mut command = Command::new("docker");
    command.args([
        "run",
        "--rm",
        "--pull",
        "never",
        "--network",
        "none",
        "--entrypoint",
        "timeout",
    ]);
    for mount in mounts {
        command.arg("--volume").arg(format!(
            "{}:{}{}",
            mount.host_path.display(),
            mount.container_path,
            if mount.read_only { ":ro" } else { "" },
        ));
    }
    for (name, value) in env {
        command.arg("--env").arg(format!("{name}={value}"));
    }
    let output = command
        .args([image, "--kill-after=5s", "30s", "bash", "-c", script])
        .current_dir(repo_root)
        .output()
        .context("running bound driver capability probe")?;
    anyhow::ensure!(
        output.status.success(),
        "bound driver capability probe failed (exit={:?}): {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr),
    );
    Ok(())
}

pub(super) fn bound_cache_key(
    image: &str,
    runtime_fingerprint: &str,
    dso_version: &str,
    worker_script: &[u8],
) -> String {
    format!(
        "capabilities-{}",
        super::runtime_fingerprint(
            "driver-capability-probe-v1",
            &[
                image,
                runtime_fingerprint,
                dso_version,
                &hex::encode(Sha256::digest(worker_script)),
                &hex::encode(Sha256::digest(PROBE_SCRIPT)),
            ],
        )
    )
}

#[derive(serde::Deserialize)]
struct WorkerReport {
    schema_version: u32,
    runner_key: String,
    runner_instance_id: String,
    image: String,
    runner_family: String,
    driver_subcommands: BTreeMap<String, bool>,
    driver_help_tokens: BTreeMap<String, BTreeMap<String, bool>>,
}

fn validate(record: &DriverRunnerCapabilities, runner_key: &str) -> Result<()> {
    anyhow::ensure!(
        record.runner_key == runner_key,
        "capability cache key mismatch"
    );
    anyhow::ensure!(
        record.aig2ir.is_some()
            && record.ir_equiv.is_some()
            && record.ir2g8r.is_some()
            && record.ir2gates.is_some()
            && record.ir2gates_prepared_ir_out.is_some(),
        "incomplete driver capability probes"
    );
    Ok(())
}

// Import only the completed bound probe's report, under its cache lock. The
// request environment and immutable image bind the help calls to that runtime.
pub(super) fn publish(
    paths: &PersistentRunnerPaths,
    runner_key: &str,
    image: &str,
    instance_id: &str,
) -> Result<DriverRunnerCapabilities> {
    let bytes = fs::read(&paths.capabilities_path).context("reading worker capability report")?;
    let report: WorkerReport =
        serde_json::from_slice(&bytes).context("parsing worker capability report")?;
    anyhow::ensure!(
        report.schema_version == PERSISTENT_RUNNER_SCHEMA_VERSION
            && report.image == image
            && report.runner_instance_id == instance_id
            && report.runner_family == "driver",
        "driver capability report has the wrong protocol, image, instance, or family"
    );
    let record = DriverRunnerCapabilities {
        runner_key: report.runner_key,
        aig2ir: report.driver_subcommands.get("aig2ir").copied(),
        ir_equiv: report.driver_subcommands.get("ir-equiv").copied(),
        ir2g8r: report.driver_subcommands.get("ir2g8r").copied(),
        ir2gates: report.driver_subcommands.get("ir2gates").copied(),
        ir2gates_prepared_ir_out: report
            .driver_help_tokens
            .get("ir2gates")
            .and_then(|tokens| tokens.get("--prepared-ir-out"))
            .copied(),
    };
    validate(&record, runner_key)?;
    write_cache_manifest_atomic(&paths.root.join("capabilities.pb"), &record)?;
    Ok(record)
}

fn read_cached(
    paths: &PersistentRunnerPaths,
    runner_key: &str,
) -> Result<Option<DriverRunnerCapabilities>> {
    let bytes = match fs::read(paths.root.join("capabilities.pb")) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("reading cached driver capabilities"),
    };
    let record = DriverRunnerCapabilities::decode(bytes.as_slice())
        .ok()
        .filter(|record| validate(record, runner_key).is_ok());
    Ok(record)
}

pub(super) fn load_or_probe(
    paths: &PersistentRunnerPaths,
    runner_key: &str,
    probe: impl FnOnce() -> Result<DriverRunnerCapabilities>,
) -> Result<DriverRunnerCapabilities> {
    let _reservation = loop {
        if let Some(record) = read_cached(paths, runner_key)? {
            return Ok(record);
        }
        if let Some(guard) = PersistentRunnerSlotGuard::try_acquire(paths)? {
            break guard;
        }
        // Another bound probe may publish the cache while holding its reservation.
        thread::sleep(Duration::from_millis(100));
    };
    // A concurrent startup may have published between the read and the lock.
    match read_cached(paths, runner_key)? {
        Some(record) => Ok(record),
        None => probe(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::runtime_docker::tests::make_temp_store_root;
    use serde_json::json;

    fn report() -> serde_json::Value {
        json!({
            "schema_version": PERSISTENT_RUNNER_SCHEMA_VERSION,
            "runner_key": "slot",
            "runner_instance_id": "instance",
            "image": "image",
            "runner_family": "driver",
            "driver_subcommands": {"aig2ir": true, "ir-equiv": false, "ir2g8r": true, "ir2gates": true},
            "driver_help_tokens": {"ir2gates": {"--prepared-ir-out": false}},
        })
    }

    #[test]
    fn bound_keys_change_with_the_image_runtime_dso_and_parser() {
        let key = bound_cache_key("image", "runtime", "dso", b"parser");
        assert_eq!(key, bound_cache_key("image", "runtime", "dso", b"parser"));
        for changed in [
            bound_cache_key("other-image", "runtime", "dso", b"parser"),
            bound_cache_key("image", "other-runtime", "dso", b"parser"),
            bound_cache_key("image", "runtime", "other-dso", b"parser"),
            bound_cache_key("image", "runtime", "dso", b"other-parser"),
        ] {
            assert_ne!(key, changed);
        }
    }

    #[test]
    fn concurrent_cold_queries_publish_once_and_failed_probes_can_retry() -> Result<()> {
        let root = make_temp_store_root("bound-capability-concurrent");
        let paths = PersistentRunnerPaths::new(&root, "slot");
        paths.ensure_layout()?;
        assert!(load_or_probe(&paths, "slot", || bail!("failed help")).is_err());
        assert!(!paths.root.join("capabilities.pb").exists());
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let ready = std::sync::Barrier::new(8);
        thread::scope(|scope| {
            let handles = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        ready.wait();
                        load_or_probe(&paths, "slot", || {
                            calls.fetch_add(1, Ordering::SeqCst);
                            fs::write(&paths.capabilities_path, serde_json::to_vec(&report())?)?;
                            publish(&paths, "slot", "image", "instance")
                        })
                    })
                })
                .collect::<Vec<_>>();
            for handle in handles {
                let record = handle.join().expect("capability query thread")?;
                assert_eq!(record.aig2ir, Some(true));
                assert_eq!(record.ir_equiv, Some(false));
                assert_eq!(record.ir2gates_prepared_ir_out, Some(false));
            }
            Ok::<_, anyhow::Error>(())
        })?;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn only_complete_reports_from_the_started_driver_are_published() -> Result<()> {
        let root = make_temp_store_root("capability-report");
        let paths = PersistentRunnerPaths::new(&root, "slot");
        paths.ensure_layout()?;
        for (field, value) in [
            ("schema_version", json!(0)),
            ("runner_key", json!("other-slot")),
            ("runner_instance_id", json!("old-instance")),
            ("image", json!("other-image")),
            ("runner_family", json!("yosys")),
            ("driver_subcommands", json!({"aig2ir": true})),
            ("driver_help_tokens", json!({"ir2gates": {}})),
        ] {
            let mut invalid = report();
            invalid[field] = value;
            fs::write(&paths.capabilities_path, serde_json::to_vec(&invalid)?)?;
            assert!(
                publish(&paths, "slot", "image", "instance").is_err(),
                "{field}"
            );
            assert!(!paths.root.join("capabilities.pb").exists());
        }
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn legacy_and_invalid_caches_reprobe_and_valid_protobuf_survives_retirement() -> Result<()> {
        let root = make_temp_store_root("capability-cache");
        let paths = PersistentRunnerPaths::new(&root, "slot");
        paths.ensure_layout()?;
        let cache_path = paths.root.join("capabilities.pb");
        let fresh = serde_json::to_vec(&report())?;
        let mut legacy = report();
        legacy["driver_subcommands"]["ir-equiv"] = json!(true);
        fs::write(&paths.capabilities_path, serde_json::to_vec(&legacy)?)?;
        let probe = || {
            assert!(PersistentRunnerSlotGuard::try_acquire(&paths)?.is_none());
            fs::write(&paths.capabilities_path, &fresh)?;
            publish(&paths, "slot", "image", "instance")
        };
        let record = load_or_probe(&paths, "slot", probe)?;
        assert_eq!(record.ir_equiv, Some(false));
        assert_eq!(record.aig2ir, Some(true));
        assert_eq!(record.ir2gates_prepared_ir_out, Some(false));

        let mut wrong_key = record.clone();
        wrong_key.runner_key = "other-slot".into();
        let mut partial = record.clone();
        partial.ir2gates_prepared_ir_out = None;
        for bytes in [
            vec![0xff],
            wrong_key.encode_to_vec(),
            partial.encode_to_vec(),
        ] {
            fs::write(&cache_path, bytes)?;
            assert_eq!(load_or_probe(&paths, "slot", probe)?, record);
            assert_eq!(
                DriverRunnerCapabilities::decode(fs::read(&cache_path)?.as_slice())?,
                record
            );
        }
        paths.reset_request_state()?;
        let reservation = PersistentRunnerSlotGuard::try_acquire(&paths)?.expect("free slot");
        let (tx, rx) = std::sync::mpsc::channel();
        thread::scope(|scope| {
            scope.spawn(|| {
                tx.send(load_or_probe(&paths, "slot", || {
                    bail!("unexpected restart")
                }))
                .unwrap();
            });
            let observed = rx.recv_timeout(Duration::from_secs(5));
            drop(reservation);
            assert_eq!(
                observed.expect("cache hit while slot is reserved").unwrap(),
                record
            );
        });
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
