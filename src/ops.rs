// SPDX-License-Identifier: Apache-2.0

use anyhow::{Result, bail};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};
use walkdir::WalkDir;

use crate::app;
use crate::store::ArtifactStore;

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
    let drained_actions = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(workers);

    for worker_index in 0..workers {
        let store = store.clone();
        let repo_root = repo_root.clone();
        let drained_actions = drained_actions.clone();
        let stop = stop.clone();
        let worker_id = format!("{}:runner-{}", worker_id_prefix, worker_index);
        handles.push(thread::spawn(move || -> Result<()> {
            loop {
                if stop.load(Ordering::Relaxed) {
                    return Ok(());
                }
                match app::drain_queue(
                    &store,
                    &repo_root,
                    Some(batch_size),
                    &worker_id,
                    lease_seconds,
                    reclaim_expired,
                    None,
                ) {
                    Ok(0) => {
                        if exit_when_idle && queue_is_idle(&store) {
                            stop.store(true, Ordering::Relaxed);
                            return Ok(());
                        }
                        thread::sleep(poll_interval);
                    }
                    Ok(drained) => {
                        drained_actions.fetch_add(drained, Ordering::Relaxed);
                        if exit_when_idle && queue_is_idle(&store) {
                            stop.store(true, Ordering::Relaxed);
                        }
                    }
                    Err(err) => {
                        eprintln!("run-workers ({}) error: {:#}", worker_id, err);
                        thread::sleep(poll_interval);
                    }
                }
            }
        }));
    }

    for handle in handles {
        let result = handle
            .join()
            .map_err(|_| anyhow::anyhow!("worker thread panicked"))?;
        result?;
    }

    store.flush_durable()?;
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
        exit_reason: if exit_when_idle {
            "idle".to_string()
        } else {
            "completed".to_string()
        },
    })
}

fn queue_is_idle(store: &ArtifactStore) -> bool {
    count_queue_json_files(&store.queue_pending_dir()) == 0
        && count_queue_json_files(&store.queue_running_dir()) == 0
}

fn count_queue_json_files(dir: &Path) -> usize {
    if !dir.exists() {
        return 0;
    }
    WalkDir::new(dir)
        .sort_by_file_name()
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|s| s.to_str())
                .map(|ext| ext == "json")
                .unwrap_or(false)
        })
        .count()
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
        )
        .expect("run workers");
        assert_eq!(summary.exit_reason, "idle");
        assert_eq!(summary.drained_actions, 0);

        fs::remove_dir_all(root).expect("cleanup temp root");
    }

    #[test]
    fn count_queue_json_files_ignores_non_json_files() {
        let (store, root) = make_test_store("queue-count");
        let queue_dir = store.queue_pending_dir();
        fs::create_dir_all(&queue_dir).expect("create queue dir");
        fs::write(queue_dir.join("a.json"), "{}").expect("write json");
        fs::write(queue_dir.join("b.bad"), "{}").expect("write bad");
        fs::write(queue_dir.join("c.tmp"), "{}").expect("write tmp");
        assert_eq!(count_queue_json_files(&queue_dir), 1);

        fs::remove_dir_all(root).expect("cleanup temp root");
    }
}
