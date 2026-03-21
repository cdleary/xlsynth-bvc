use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

use crate::snapshot::StaticSnapshotManifest;
use crate::store::ArtifactStore;
use crate::web::cache::WebUiCache;

#[derive(Debug, Clone)]
pub(crate) struct WebUiState {
    pub(crate) store: Arc<ArtifactStore>,
    pub(crate) cache: Arc<WebUiCache>,
    pub(crate) heavy_route_limit: Arc<Semaphore>,
    pub(crate) repo_root: PathBuf,
    pub(crate) snapshot_manifest: Option<StaticSnapshotManifest>,
    pub(crate) runner_enabled: bool,
    pub(crate) runner_owner_prefix: Option<String>,
    pub(crate) runner_control: Option<Arc<WebRunnerControl>>,
}

#[derive(Debug, Clone)]
pub(crate) struct WebRunnerConfig {
    pub(crate) enabled: bool,
    pub(crate) lease_seconds: i64,
    pub(crate) poll_interval: Duration,
    pub(crate) drain_batch_size: usize,
    pub(crate) worker_count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StdlibFnsTrendQuery {
    pub(crate) metric: Option<String>,
    pub(crate) fraig: Option<bool>,
    pub(crate) file: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StdlibFnsG8rVsYosysAbcQuery {
    pub(crate) fraig: Option<bool>,
    pub(crate) max_ir_nodes: Option<u64>,
    pub(crate) crate_version: Option<String>,
    pub(crate) losses_only: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct IrFnCorpusG8rVsYosysAbcQuery {
    pub(crate) max_ir_nodes: Option<u64>,
    pub(crate) crate_version: Option<String>,
    pub(crate) losses_only: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct IrFnCorpusK3LossesVsCrateVersionQuery {
    pub(crate) max_ir_nodes: Option<u64>,
    pub(crate) crate_version: Option<String>,
    pub(crate) show_same: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct IrFnCorpusStructuralQuery {
    pub(crate) fn_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct DbSizeQuery {
    pub(crate) top: Option<usize>,
    pub(crate) sample: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StdlibSampleDetailsQuery {
    pub(crate) ir_action_id: String,
    pub(crate) ir_top: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StdlibFileActionGraphQuery {
    pub(crate) crate_version: Option<String>,
    pub(crate) file: Option<String>,
    pub(crate) fn_name: Option<String>,
    pub(crate) action_id: Option<String>,
    pub(crate) include_k3_descendants: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StdlibFnVersionHistoryQuery {
    pub(crate) fraig: Option<bool>,
    pub(crate) delay_model: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ActionOutputFileQuery {
    pub(crate) path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct EnqueueCrateVersionForm {
    pub(crate) crate_version: String,
    #[serde(default = "default_queue_priority")]
    pub(crate) priority: i32,
}

fn default_queue_priority() -> i32 {
    crate::DEFAULT_QUEUE_PRIORITY
}

#[derive(Debug, Clone)]
pub(crate) struct RunnerControlSnapshot {
    pub(crate) paused: bool,
    pub(crate) sync_pending: bool,
    pub(crate) last_sync_utc: Option<DateTime<Utc>>,
    pub(crate) last_sync_error: Option<String>,
}

#[derive(Debug, Default)]
struct RunnerSyncState {
    last_sync_utc: Option<DateTime<Utc>>,
    last_sync_error: Option<String>,
}

#[derive(Debug)]
pub(crate) struct WebRunnerControl {
    paused: AtomicBool,
    sync_pending: AtomicBool,
    sync_state: Mutex<RunnerSyncState>,
}

impl WebRunnerControl {
    pub(crate) fn new() -> Self {
        Self {
            paused: AtomicBool::new(false),
            sync_pending: AtomicBool::new(false),
            sync_state: Mutex::new(RunnerSyncState::default()),
        }
    }

    pub(crate) fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    pub(crate) fn pause_flag(&self) -> &AtomicBool {
        &self.paused
    }

    pub(crate) fn is_sync_pending(&self) -> bool {
        self.sync_pending.load(Ordering::Relaxed)
    }

    pub(crate) fn request_pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
        self.sync_pending.store(true, Ordering::Relaxed);
    }

    pub(crate) fn request_resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
        self.sync_pending.store(false, Ordering::Relaxed);
    }

    pub(crate) fn mark_sync_success(&self) {
        if let Ok(mut guard) = self.sync_state.lock() {
            guard.last_sync_utc = Some(Utc::now());
            guard.last_sync_error = None;
        }
        self.sync_pending.store(false, Ordering::Relaxed);
    }

    pub(crate) fn mark_sync_error(&self, error: String) {
        if let Ok(mut guard) = self.sync_state.lock() {
            guard.last_sync_error = Some(error);
        }
        self.sync_pending.store(true, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> RunnerControlSnapshot {
        let (last_sync_utc, last_sync_error) = if let Ok(guard) = self.sync_state.lock() {
            (guard.last_sync_utc, guard.last_sync_error.clone())
        } else {
            (None, Some("runner sync state lock poisoned".to_string()))
        };
        RunnerControlSnapshot {
            paused: self.paused.load(Ordering::Relaxed),
            sync_pending: self.sync_pending.load(Ordering::Relaxed),
            last_sync_utc,
            last_sync_error,
        }
    }
}
