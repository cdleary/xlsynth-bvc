// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, bail};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};
use walkdir::WalkDir;

mod maintenance_gate;

use maintenance_gate::MaintenanceGate;

use crate::store::ArtifactStore;
use crate::{app, queue};

const FINITE_WORKER_MAX_CONSECUTIVE_ERRORS: usize = 3;

/// Install only for the CLI worker command, not for embedded/web runners or unit tests.
/// The handlers only set an atomic flag; leases and durable flushing remain on worker threads.
pub(crate) struct WorkerShutdownSignals(Vec<signal_hook::SigId>);

impl WorkerShutdownSignals {
    pub(crate) fn install(shutdown: &Arc<AtomicBool>) -> Result<Self> {
        let mut guard = Self(Vec::new());
        for signal in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM] {
            guard
                .0
                .push(signal_hook::flag::register(signal, shutdown.clone())?);
        }
        Ok(guard)
    }
}

impl Drop for WorkerShutdownSignals {
    fn drop(&mut self) {
        for id in self.0.drain(..) {
            signal_hook::low_level::unregister(id);
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RunWorkersSummary {
    pub(crate) workers: usize,
    pub(crate) worker_id_prefix: String,
    pub(crate) lease_seconds: i64,
    pub(crate) poll_millis: u64,
    pub(crate) batch_size: usize,
    pub(crate) reclaim_expired: bool,
    pub(crate) exit_when_idle: bool,
    pub(crate) drained_actions: usize,
    pub(crate) elapsed_secs: f64,
    pub(crate) exit_reason: String,
}

pub(crate) fn run_workers(
    store: Arc<ArtifactStore>,
    repo_root: PathBuf,
    workers: usize,
    worker_id_prefix: &str,
    lease_seconds: i64,
    poll_interval: Duration,
    batch_size: usize,
    reclaim_expired: bool,
    exit_when_idle: bool,
    shutdown: Arc<AtomicBool>,
) -> Result<RunWorkersSummary> {
    if workers == 0 {
        bail!("--workers must be > 0");
    }
    if lease_seconds <= 0 {
        bail!("--lease-seconds must be > 0, got {}", lease_seconds);
    }
    if poll_interval.is_zero() {
        bail!("--poll-millis must be > 0");
    }
    if batch_size == 0 {
        bail!("--batch-size must be > 0");
    }

    let started = Instant::now();
    let drained_actions = AtomicUsize::new(0);
    let max_consecutive_errors = exit_when_idle.then_some(FINITE_WORKER_MAX_CONSECUTIVE_ERRORS);

    let result = run_worker_pool(
        workers,
        |worker_index, pool| {
            let worker_id = format!("{}:runner-{}", worker_id_prefix, worker_index);
            let mut retry_reclaim = false;
            let mut reclaim = || {
                if reclaim_expired {
                    pool.reclaim_gate
                        .run_with_retry(&mut retry_reclaim, poll_interval, || {
                            queue::reclaim_expired_running_leases(&store)
                        })?;
                }
                Ok(())
            };
            worker_loop(
                pool,
                &shutdown,
                &drained_actions,
                &worker_id,
                poll_interval,
                exit_when_idle,
                max_consecutive_errors,
                || {
                    app::drain_queue_with_reclaim(
                        &store,
                        &repo_root,
                        Some(batch_size),
                        &worker_id,
                        lease_seconds,
                        &mut reclaim,
                        Some(&shutdown),
                    )
                },
                || {
                    pool.idle_gate
                        .run_if_due(Instant::now(), poll_interval, || queue_is_idle(&store))
                        .unwrap_or(false)
                },
            )
        },
        || {
            !exit_when_idle
                || shutdown.load(Ordering::Relaxed)
                || workers == 1
                || queue_is_idle(&store)
        },
    );
    store.flush_durable()?;
    result?;
    Ok(RunWorkersSummary {
        workers,
        worker_id_prefix: worker_id_prefix.to_string(),
        lease_seconds,
        poll_millis: poll_interval.as_millis() as u64,
        batch_size,
        reclaim_expired,
        exit_when_idle,
        drained_actions: drained_actions.load(Ordering::Relaxed),
        elapsed_secs: started.elapsed().as_secs_f64(),
        exit_reason: if shutdown.load(Ordering::Relaxed) {
            "shutdown_requested".to_string()
        } else if exit_when_idle {
            "idle".to_string()
        } else {
            "completed".to_string()
        },
    })
}

#[derive(Default)]
struct WorkerPoolState {
    reclaim_gate: MaintenanceGate,
    idle_gate: MaintenanceGate,
    stop_requested: Mutex<bool>,
    wake: Condvar,
}

impl WorkerPoolState {
    fn stop_requested(&self) -> bool {
        *self.stop_requested.lock().unwrap()
    }

    fn request_stop(&self) {
        *self.stop_requested.lock().unwrap() = true;
        self.wake.notify_all();
    }

    fn wait_for_poll(&self, interval: Duration) {
        let requested = self.stop_requested.lock().unwrap();
        drop(
            self.wake
                .wait_timeout_while(requested, interval, |stop| !*stop)
                .unwrap(),
        );
    }
}

// Wake polling peers on every exit, including unwinding. The parent may
// already be joining a different worker.
struct WakePeersOnExit<'a>(&'a WorkerPoolState);

impl Drop for WakePeersOnExit<'_> {
    fn drop(&mut self) {
        self.0.request_stop();
    }
}

fn run_worker_pool(
    workers: usize,
    run: impl Fn(usize, &WorkerPoolState) -> Result<()> + Sync,
    finished: impl Fn() -> bool,
) -> Result<()> {
    loop {
        let pool = WorkerPoolState::default();
        let run_one = &|index| {
            let _wake_peers = WakePeersOnExit(&pool);
            run(index, &pool)
        };
        thread::scope(|scope| {
            let mut handles = Vec::new();
            let mut first_error = None;
            for index in 0..workers {
                match thread::Builder::new().spawn_scoped(scope, move || run_one(index)) {
                    Ok(handle) => handles.push(handle),
                    Err(error) => {
                        pool.request_stop();
                        first_error =
                            Some(anyhow::Error::new(error).context("starting queue worker"));
                        break;
                    }
                }
            }
            for handle in handles {
                let result = handle
                    .join()
                    .unwrap_or_else(|_| Err(anyhow::anyhow!("worker thread panicked")));
                if let Err(error) = result {
                    first_error.get_or_insert(error);
                }
            }
            first_error.map_or(Ok(()), Err)
        })?;
        // A peer can publish pending work between an idle scan's pending and
        // running walks, or enqueue a follow-up as it finishes its last batch.
        // Verify once the pool has joined, when all such writes are visible.
        if finished() {
            return Ok(());
        }
    }
}

fn worker_loop(
    stop: &WorkerPoolState,
    shutdown: &AtomicBool,
    drained_actions: &AtomicUsize,
    worker_id: &str,
    poll_interval: Duration,
    exit_when_idle: bool,
    max_consecutive_errors: Option<usize>,
    mut drain: impl FnMut() -> Result<usize>,
    queue_is_idle: impl Fn() -> bool,
) -> Result<()> {
    let mut consecutive_errors = 0_usize;
    loop {
        if stop.stop_requested() || shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }
        match drain() {
            Ok(0) => {
                consecutive_errors = 0;
                if exit_when_idle && queue_is_idle() {
                    return Ok(());
                }
                stop.wait_for_poll(poll_interval);
            }
            Ok(drained) => {
                consecutive_errors = 0;
                drained_actions.fetch_add(drained, Ordering::Relaxed);
                if exit_when_idle && queue_is_idle() {
                    return Ok(());
                }
            }
            Err(err) => {
                consecutive_errors += 1;
                eprintln!("run-workers ({worker_id}) error: {err:#}");
                if max_consecutive_errors.is_some_and(|maximum| consecutive_errors >= maximum) {
                    return Err(err).with_context(|| {
                        format!(
                            "run-workers ({worker_id}) stopped after {consecutive_errors} consecutive errors"
                        )
                    });
                }
                stop.wait_for_poll(poll_interval);
            }
        }
    }
}

fn queue_is_idle(store: &ArtifactStore) -> bool {
    // Existence, not cardinality: do not count the whole pending tree after every batch.
    !queue_has_pb_files(&store.queue_pending_dir())
        && !queue_has_pb_files(&store.queue_running_dir())
}

fn queue_has_pb_files(dir: &Path) -> bool {
    match std::fs::metadata(dir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return false,
        Err(_) => return true,
        Ok(_) => {}
    }
    WalkDir::new(dir).into_iter().any(|entry| match entry {
        // A failed read must not make a finite worker report the queue as empty.
        Err(_) => true,
        Ok(entry) => {
            entry.file_type().is_file()
                && entry.path().extension().and_then(|s| s.to_str()) == Some("pb")
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_test_store(prefix: &str) -> (Arc<ArtifactStore>, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-ops-test-{}-{}-{}",
            prefix,
            std::process::id(),
            nanos
        ));
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure store layout");
        (Arc::new(store), root)
    }

    #[test]
    fn run_workers_exit_when_idle_returns_immediately_for_empty_queue() {
        let (store, root) = make_test_store("idle-empty");
        let summary = run_workers(
            store,
            root.clone(),
            2,
            "worker-test",
            60,
            Duration::from_millis(1),
            4,
            true,
            true,
            Arc::new(AtomicBool::new(false)),
        )
        .expect("run workers");
        assert_eq!(summary.exit_reason, "idle");
        assert_eq!(summary.drained_actions, 0);

        fs::remove_dir_all(root).expect("cleanup temp root");
    }

    #[test]
    fn run_workers_recovers_preexisting_leases_and_drains() {
        use crate::model::{
            ActionSpec, ArtifactRef, ArtifactType, DriverRuntimeSpec, OutputFile, Provenance,
        };
        use sha2::{Digest, Sha256};

        for dead_local_owner in [false, true] {
            if dead_local_owner && !cfg!(target_os = "linux") {
                continue;
            }
            let (store, root) = make_test_store("recover-lease");
            let fixture = |action: ActionSpec, artifact_type: ArtifactType, bytes: &[u8]| {
                let id = crate::executor::compute_action_id(&action).unwrap();
                let relpath = "payload/input";
                let provenance = Provenance {
                    schema_version: crate::ACTION_SCHEMA_VERSION,
                    action_id: id.clone(),
                    created_utc: chrono::Utc::now(),
                    action,
                    dependencies: Vec::new(),
                    output_artifact: ArtifactRef {
                        action_id: id.clone(),
                        artifact_type,
                        relpath: relpath.into(),
                    },
                    output_files: vec![OutputFile {
                        path: relpath.into(),
                        bytes: bytes.len() as u64,
                        sha256: format!("{:x}", Sha256::digest(bytes)),
                    }],
                    commands: Vec::new(),
                    details: serde_json::json!({}),
                    suggested_next_actions: Vec::new(),
                };
                let staged = store.staging_dir().join(&id);
                fs::create_dir_all(staged.join("payload")).unwrap();
                fs::write(staged.join(relpath), bytes).unwrap();
                fs::write(
                    staged.join("provenance.pb"),
                    crate::proto::encode_provenance(&provenance).unwrap(),
                )
                .unwrap();
                store.promote_staging_action_dir(&id, &staged).unwrap();
                id
            };
            let ir = b"package test\n\nfn main(x: bits[1]) -> bits[1] { ret identity.2: bits[1] = identity(x, id=2) }\n";
            let ir_id = fixture(
                ActionSpec::ImportIrPackageFile {
                    source_sha256: format!("{:x}", Sha256::digest(ir)),
                    top_fn_name: Some("main".into()),
                },
                ArtifactType::IrPackageFile,
                ir,
            );
            let runtime = DriverRuntimeSpec {
                driver_version: "0.31.0".into(),
                source_revision: None,
                release_platform: "ubuntu2004".into(),
                docker_image: "xlsynth-bvc-driver:0.31.0".into(),
                dockerfile: "docker/xlsynth-driver.Dockerfile".into(),
                dockerfile_sha256: "d".repeat(64),
                docker_image_id: "e".repeat(64),
                release_cache_input_sha256: "f".repeat(64),
            };
            let lhs = fixture(
                ActionSpec::DriverAigToStats {
                    aig_action_id: "a".repeat(64),
                    version: "v0.37.0".into(),
                    runtime: runtime.clone(),
                },
                ArtifactType::AigStatsFile,
                br#"{"and_nodes":12,"depth":3}"#,
            );
            let rhs = fixture(
                ActionSpec::DriverAigToStats {
                    aig_action_id: "b".repeat(64),
                    version: "v0.37.0".into(),
                    runtime,
                },
                ArtifactType::AigStatsFile,
                br#"{"and_nodes":8,"depth":4}"#,
            );
            let id = queue::enqueue_action(
                &store,
                ActionSpec::AigStatDiff {
                    opt_ir_action_id: ir_id,
                    g8r_aig_stats_action_id: lhs,
                    yosys_abc_aig_stats_action_id: rhs,
                },
            )
            .unwrap();
            let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".into());
            let owner = if dead_local_owner {
                format!("{host}:{}:0:retired-worker", u32::MAX)
            } else {
                "retired-remote-worker".into()
            };
            let lease = queue::claim_next_pending_item(
                &store,
                &owner,
                if dead_local_owner { 900 } else { -1 },
            )
            .unwrap()
            .unwrap();
            assert!(store.running_queue_path(&id).is_file());
            assert!(!store.pending_queue_path(&id).exists());
            drop(lease);
            let summary = run_workers(
                store.clone(),
                root.clone(),
                4,
                "recovery-test",
                60,
                Duration::from_millis(1),
                4,
                true,
                true,
                Arc::new(AtomicBool::new(false)),
            )
            .unwrap();
            assert_eq!(summary.exit_reason, "idle");
            assert_eq!(summary.drained_actions, 1);
            assert!(queue_is_idle(&store));
            let result = store.load_provenance(&id).unwrap();
            let output: serde_json::Value = serde_json::from_slice(
                &fs::read(store.resolve_artifact_ref_path(&result.output_artifact)).unwrap(),
            )
            .unwrap();
            assert_eq!(output["g8r_stats"]["and_nodes"], 12);
            assert_eq!(output["yosys_abc_stats"]["and_nodes"], 8);
            assert!(!store.running_queue_path(&id).exists());
            drop(store);
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn maintenance_failure_preserves_finite_worker_errors() {
        let (store, root) = make_test_store("maintenance-error");
        let id = queue::enqueue_action(
            &store,
            crate::model::ActionSpec::ImportIrPackageFile {
                source_sha256: "a".repeat(64),
                top_fn_name: None,
            },
        )
        .unwrap();
        queue::claim_next_pending_item(&store, "expired", -1)
            .unwrap()
            .unwrap();
        // Force reclamation to fail opening its lock, even for privileged test users.
        let lock = store.queue_transition_lock_path(&id);
        fs::remove_file(&lock).unwrap();
        fs::create_dir(lock).unwrap();
        let result = run_workers(
            store,
            root.clone(),
            2,
            "maintenance-error",
            60,
            Duration::from_millis(1),
            4,
            true,
            true,
            Arc::new(AtomicBool::new(false)),
        );
        fs::remove_dir_all(root).unwrap();
        let error = format!("{:#}", result.unwrap_err());
        assert!(error.contains("3 consecutive errors"), "{error}");
        assert!(error.contains("opening queue transition lock"), "{error}");
    }

    #[test]
    fn idle_pool_resumes_work_published_by_a_finishing_peer() {
        let draining = std::sync::Barrier::new(2);
        let pending = AtomicBool::new(false);
        let drained = AtomicUsize::new(0);
        let checks = AtomicUsize::new(0);
        run_worker_pool(
            2,
            |index, stop| {
                worker_loop(
                    stop,
                    &AtomicBool::new(false),
                    &drained,
                    "idle-test",
                    Duration::from_millis(1),
                    true,
                    Some(3),
                    || {
                        if checks.load(Ordering::Relaxed) == 0 {
                            // Both workers are in a drain. One sees idle before
                            // its peer finishes publishing a follow-up.
                            draining.wait();
                            if index == 1 {
                                stop.wait_for_poll(Duration::from_secs(5));
                                assert!(stop.stop_requested());
                                pending.store(true, Ordering::Relaxed);
                            }
                            Ok(0)
                        } else {
                            Ok(usize::from(pending.swap(false, Ordering::Relaxed)))
                        }
                    },
                    || !pending.load(Ordering::Relaxed),
                )
            },
            || {
                checks.fetch_add(1, Ordering::Relaxed);
                !pending.load(Ordering::Relaxed)
            },
        )
        .unwrap();
        assert_eq!(drained.load(Ordering::Relaxed), 1);
        assert_eq!(checks.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn worker_panic_wakes_polling_peer_before_join() {
        let polling = std::sync::Barrier::new(2);
        let woke = AtomicBool::new(false);
        let error = run_worker_pool(
            2,
            |index, stop| {
                polling.wait();
                if index == 1 {
                    panic!("injected worker panic");
                }
                stop.wait_for_poll(Duration::from_secs(5));
                woke.store(stop.stop_requested(), Ordering::Relaxed);
                Ok(())
            },
            || panic!("retried after worker panic"),
        )
        .unwrap_err();
        assert!(woke.load(Ordering::Relaxed));
        assert_eq!(error.to_string(), "worker thread panicked");
    }

    #[test]
    fn queue_has_pb_files_ignores_non_pb_files() {
        let (store, root) = make_test_store("queue-count");
        let queue_dir = store.queue_pending_dir();
        fs::create_dir_all(&queue_dir).expect("create queue dir");
        fs::write(queue_dir.join("a.pb"), "{}").expect("write protobuf placeholder");
        fs::write(queue_dir.join("b.bad"), "{}").expect("write bad");
        fs::write(queue_dir.join("c.tmp"), "{}").expect("write tmp");
        assert!(queue_has_pb_files(&queue_dir));
        fs::remove_file(queue_dir.join("a.pb")).expect("remove only queue record");
        assert!(!queue_has_pb_files(&queue_dir));

        fs::remove_dir_all(root).expect("cleanup temp root");
    }

    #[test]
    fn shutdown_finishes_current_drain_without_starting_another() {
        let stop = WorkerPoolState::default();
        let shutdown = AtomicBool::new(false);
        let drained = AtomicUsize::new(0);
        let invocations = AtomicUsize::new(0);
        worker_loop(
            &stop,
            &shutdown,
            &drained,
            "shutdown-test",
            Duration::from_millis(1),
            false,
            None,
            || {
                assert_eq!(invocations.fetch_add(1, Ordering::Relaxed), 0);
                shutdown.store(true, Ordering::Relaxed);
                // The active drain still returns its completed, finalized actions.
                Ok(3)
            },
            || false,
        )
        .unwrap();
        assert_eq!(drained.load(Ordering::Relaxed), 3);
        assert_eq!(invocations.load(Ordering::Relaxed), 1);
    }

    #[test]
    #[cfg(unix)]
    fn graceful_signals_finish_in_flight_drain_in_subprocess() {
        // Exercise actual signal delivery in a child so the test runner's signal state is untouched.
        const CHILD_DIR: &str = "BVC_TEST_SHUTDOWN_CHILD_DIR";
        if let Some(root) = std::env::var_os(CHILD_DIR) {
            let root = PathBuf::from(root);
            let shutdown = Arc::new(AtomicBool::new(false));
            let _signals = WorkerShutdownSignals::install(&shutdown).unwrap();
            let stop = WorkerPoolState::default();
            let drained = AtomicUsize::new(0);
            let invocations = AtomicUsize::new(0);
            worker_loop(
                &stop,
                &shutdown,
                &drained,
                "signal-child",
                Duration::from_millis(1),
                false,
                None,
                || {
                    assert_eq!(invocations.fetch_add(1, Ordering::Relaxed), 0);
                    fs::write(root.join("ready"), []).unwrap();
                    let deadline = Instant::now() + Duration::from_secs(15);
                    while !shutdown.load(Ordering::Relaxed) {
                        assert!(Instant::now() < deadline, "signal did not arrive");
                        thread::sleep(Duration::from_millis(5));
                    }
                    fs::write(root.join("finalized"), b"3").unwrap();
                    Ok(3)
                },
                || false,
            )
            .unwrap();
            assert_eq!(drained.load(Ordering::Relaxed), 3);
            assert_eq!(invocations.load(Ordering::Relaxed), 1);
            return;
        }
        for signal in ["-TERM", "-INT"] {
            let (store, root) = make_test_store("signal-child");
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "ops::tests::graceful_signals_finish_in_flight_drain_in_subprocess",
                    "--nocapture",
                ])
                .env(CHILD_DIR, &root)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            while !root.join("ready").is_file() && Instant::now() < deadline {
                if child.try_wait().unwrap().is_some() {
                    break;
                }
                thread::sleep(Duration::from_millis(5));
            }
            if !root.join("ready").is_file() {
                let _ = child.kill();
                let output = child.wait_with_output().unwrap();
                panic!("child never reached in-flight drain: {output:?}");
            }
            let sent = std::process::Command::new("kill")
                .args([signal, &child.id().to_string()])
                .status()
                .unwrap();
            assert!(sent.success());
            let output = child.wait_with_output().unwrap();
            assert!(output.status.success(), "signal child failed: {output:?}");
            assert_eq!(
                fs::read_to_string(root.join("finalized"))
                    .unwrap()
                    .parse::<usize>()
                    .unwrap(),
                3
            );
            drop(store);
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn pre_requested_shutdown_preserves_pending_work_for_resume() {
        let (store, root) = make_test_store("shutdown-before-claim");
        let action_id = crate::queue::enqueue_action(
            &store,
            crate::model::ActionSpec::ImportIrPackageFile {
                source_sha256: "d".repeat(64),
                top_fn_name: None,
            },
        )
        .unwrap();
        let summary = run_workers(
            store.clone(),
            root.clone(),
            2,
            "shutdown-test",
            60,
            Duration::from_millis(1),
            4,
            true,
            true,
            Arc::new(AtomicBool::new(true)),
        )
        .unwrap();
        assert_eq!(summary.exit_reason, "shutdown_requested");
        assert_eq!(summary.drained_actions, 0);
        assert!(store.pending_queue_path(&action_id).is_file());
        assert!(!queue_has_pb_files(&store.queue_running_dir()));
        drop(store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn finite_worker_stops_after_persistent_errors() {
        let stop = WorkerPoolState::default();
        let drained = AtomicUsize::new(0);
        let attempts = AtomicUsize::new(0);
        let error = worker_loop(
            &stop,
            &AtomicBool::new(false),
            &drained,
            "persistent-error-test",
            Duration::from_millis(1),
            true,
            Some(3),
            || {
                attempts.fetch_add(1, Ordering::Relaxed);
                bail!("persistent drain failure")
            },
            || false,
        )
        .expect_err("finite worker should stop");
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
        assert!(
            error
                .to_string()
                .contains("stopped after 3 consecutive errors")
        );
    }

    #[test]
    fn finite_worker_resets_error_count_after_success() {
        let stop = WorkerPoolState::default();
        let drained = AtomicUsize::new(0);
        let attempts = AtomicUsize::new(0);
        worker_loop(
            &stop,
            &AtomicBool::new(false),
            &drained,
            "transient-error-test",
            Duration::from_millis(1),
            true,
            Some(3),
            || match attempts.fetch_add(1, Ordering::Relaxed) {
                0 | 1 | 3 | 4 => bail!("transient drain failure"),
                2 => Ok(1),
                _ => Ok(0),
            },
            || attempts.load(Ordering::Relaxed) >= 6,
        )
        .expect("success should reset consecutive error count");
        assert_eq!(attempts.load(Ordering::Relaxed), 6);
        assert_eq!(drained.load(Ordering::Relaxed), 1);
    }
}
