// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use walkdir::WalkDir;

use crate::model::{
    ActionBatchKey, ActionSpec, ArtifactRef, QueueCanceled, QueueCancellationKind, QueueDone,
    QueueFailed, QueueItem, QueueRunning, QueueRunningWithPath, action_batch_key,
};
use crate::proto::{
    decode_queue_canceled, decode_queue_done, decode_queue_item, decode_queue_running,
    encode_queue_canceled, encode_queue_done, encode_queue_item, encode_queue_running,
};
use crate::store::ArtifactStore;

static CLAIM_SCAN_CURSOR: AtomicUsize = AtomicUsize::new(0);
static QUEUE_WRITE_TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn write_bytes_atomic(path: &Path, contents: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("path missing parent: {}", path.display()))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("creating parent directory: {}", parent.display()))?;
    let filename = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("queue_record");
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let nonce = QUEUE_WRITE_TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp_path = parent.join(format!(
        ".{filename}.tmp-{}-{ts}-{nonce}",
        std::process::id()
    ));
    fs::write(&tmp_path, contents)
        .with_context(|| format!("writing temp queue record: {}", tmp_path.display()))?;
    match fs::rename(&tmp_path, path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Handle rare races where parent directories are pruned externally.
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "recreating parent directory for queue record promotion: {}",
                    parent.display()
                )
            })?;
            fs::rename(&tmp_path, path).with_context(|| {
                format!(
                    "atomically promoting queue record after parent recreation: {} -> {}",
                    tmp_path.display(),
                    path.display()
                )
            })
        }
        Err(e) => {
            let _ = fs::remove_file(&tmp_path);
            Err(e).with_context(|| {
                format!(
                    "atomically promoting queue record: {} -> {}",
                    tmp_path.display(),
                    path.display()
                )
            })
        }
    }
}

pub(crate) fn quarantine_corrupt_queue_file(path: &Path, reason: &str) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("queue path missing parent: {}", path.display()))?;
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let quarantine_path = parent.join(format!("{stem}.corrupt-{ts}.bad"));
    match fs::rename(path, &quarantine_path) {
        Ok(()) => {
            eprintln!(
                "warning: quarantined corrupt queue record {} => {} ({})",
                path.display(),
                quarantine_path.display(),
                reason
            );
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => {
            // As a last resort, remove the corrupt file so it cannot block the runner.
            remove_file_if_exists(path)?;
            eprintln!(
                "warning: removed corrupt queue record {} after failed quarantine: {:#} ({})",
                path.display(),
                e,
                reason
            );
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum QueueState {
    Pending,
    Running {
        owner: Option<String>,
        expires_utc: Option<DateTime<Utc>>,
    },
    Done,
    Failed,
    Canceled,
    None,
}

impl QueueState {
    pub(crate) fn as_label(&self) -> String {
        match self {
            Self::Pending => "pending".to_string(),
            Self::Running {
                owner: Some(owner),
                expires_utc: Some(expires_utc),
            } => format!("running(owner={},expires={})", owner, expires_utc),
            Self::Running { .. } => "running".to_string(),
            Self::Done => "done".to_string(),
            Self::Failed => "failed".to_string(),
            Self::Canceled => "canceled".to_string(),
            Self::None => "none".to_string(),
        }
    }

    pub(crate) fn key(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running { .. } => "running",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::None => "not_queued",
        }
    }

    pub(crate) fn display_label(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running { .. } => "running",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::None => "not queued",
        }
    }

    pub(crate) fn from_key(key: &str) -> Option<Self> {
        match key {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running {
                owner: None,
                expires_utc: None,
            }),
            "done" => Some(Self::Done),
            "failed" => Some(Self::Failed),
            "canceled" => Some(Self::Canceled),
            "not_queued" => Some(Self::None),
            _ => None,
        }
    }

    pub(crate) fn is_active(&self) -> bool {
        matches!(self, Self::Pending | Self::Running { .. })
    }

    pub(crate) fn is_failure(&self) -> bool {
        matches!(self, Self::Failed | Self::Canceled)
    }
}

pub(crate) fn queue_state_key(state: &QueueState) -> &'static str {
    state.key()
}

pub(crate) fn queue_state_display_label(state: &QueueState) -> &'static str {
    state.display_label()
}

fn terminal_queue_state_for_action(store: &ArtifactStore, action_id: &str) -> Option<QueueState> {
    if store.action_exists(action_id) || store.done_queue_path(action_id).exists() {
        return Some(QueueState::Done);
    }
    if store.failed_action_record_exists(action_id) {
        return Some(QueueState::Failed);
    }
    if store.canceled_queue_path(action_id).exists() {
        return Some(QueueState::Canceled);
    }
    None
}

pub(crate) fn queue_state_for_action(store: &ArtifactStore, action_id: &str) -> QueueState {
    if let Some(terminal) = terminal_queue_state_for_action(store, action_id) {
        return terminal;
    }
    let running_path = store.running_queue_path(action_id);
    if running_path.exists() {
        if let Ok(bytes) = fs::read(&running_path)
            && let Ok(running) = decode_queue_running(&bytes)
        {
            return QueueState::Running {
                owner: Some(running.lease_owner),
                expires_utc: Some(running.lease_expires_utc),
            };
        }
        return QueueState::Running {
            owner: None,
            expires_utc: None,
        };
    }
    if store.pending_queue_path(action_id).exists() {
        return QueueState::Pending;
    }
    QueueState::None
}

pub(crate) fn enqueue_action_with_priority(
    store: &ArtifactStore,
    action: ActionSpec,
    priority: i32,
) -> Result<String> {
    let action_id = crate::executor::compute_action_id(&action)?;
    if store.action_exists(&action_id) {
        return Ok(action_id);
    }

    let pending_path = store.pending_queue_path(&action_id);
    if store.running_queue_path(&action_id).exists()
        || store.done_queue_path(&action_id).exists()
        || store.failed_action_record_exists(&action_id)
        || store.canceled_queue_path(&action_id).exists()
    {
        return Ok(action_id);
    }

    if pending_path.exists() {
        let bytes = match fs::read(&pending_path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("reading queue item: {}", pending_path.display()));
            }
        };
        if !bytes.is_empty()
            && let Ok(mut item) = decode_queue_item(&bytes)
            && item.action_id == action_id
            && item.priority < priority
        {
            item.priority = priority;
            write_bytes_atomic(&pending_path, &encode_queue_item(&item)?)?;
        }
        return Ok(action_id);
    }

    let parent = pending_path
        .parent()
        .ok_or_else(|| anyhow!("pending queue path missing parent"))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("creating queue shard path: {}", parent.display()))?;

    let item = QueueItem {
        schema_version: crate::ACTION_SCHEMA_VERSION,
        action_id: action_id.clone(),
        enqueued_utc: Utc::now(),
        priority,
        action,
    };
    write_bytes_atomic(&pending_path, &encode_queue_item(&item)?)?;
    Ok(action_id)
}

pub(crate) fn enqueue_action(store: &ArtifactStore, action: ActionSpec) -> Result<String> {
    enqueue_action_with_priority(store, action, crate::DEFAULT_QUEUE_PRIORITY)
}

pub(crate) fn claim_next_pending_item(
    store: &ArtifactStore,
    worker_id: &str,
    lease_seconds: i64,
) -> Result<Option<QueueRunningWithPath>> {
    #[derive(Debug)]
    struct ReadyCandidate {
        pending_path: PathBuf,
        action_id: String,
        enqueued_utc: DateTime<Utc>,
        queue_priority: i32,
        action_priority: u8,
    }

    fn cmp_ready_candidate(a: &ReadyCandidate, b: &ReadyCandidate) -> std::cmp::Ordering {
        b.queue_priority
            .cmp(&a.queue_priority)
            .then_with(|| a.action_priority.cmp(&b.action_priority))
            .then_with(|| a.enqueued_utc.cmp(&b.enqueued_utc))
            .then_with(|| a.action_id.cmp(&b.action_id))
    }

    fn insert_ready_candidate(
        candidates: &mut Vec<ReadyCandidate>,
        candidate: ReadyCandidate,
        max_candidates: usize,
    ) {
        candidates.push(candidate);
        candidates.sort_by(cmp_ready_candidate);
        if candidates.len() > max_candidates {
            candidates.pop();
        }
    }

    let mut pending = list_queue_files(&store.queue_pending_dir())?;
    pending.sort();
    if pending.is_empty() {
        return Ok(None);
    }
    const MAX_READY_CANDIDATES: usize = 128;
    const MAX_PENDING_SCAN_PER_CLAIM: usize = 1024;
    let total_pending = pending.len();
    let start_offset =
        CLAIM_SCAN_CURSOR.fetch_add(MAX_PENDING_SCAN_PER_CLAIM, Ordering::Relaxed) % total_pending;
    let mut scanned = 0_usize;
    let mut ready_candidates: Vec<ReadyCandidate> = Vec::new();
    for i in 0..total_pending {
        let pending_path = pending[(start_offset + i) % total_pending].clone();
        scanned += 1;
        let bytes = match fs::read(&pending_path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("reading pending queue record: {}", pending_path.display())
                });
            }
        };
        let (action_id, enqueued_utc, priority, action) =
            match parse_queue_work_item(&bytes, &pending_path) {
                Ok(parsed) => parsed,
                Err(err) => {
                    quarantine_corrupt_queue_file(
                        &pending_path,
                        &format!("parse error while scanning pending queue: {:#}", err),
                    )?;
                    continue;
                }
            };
        match classify_action_readiness(store, &action)? {
            ActionReadiness::Ready => {
                insert_ready_candidate(
                    &mut ready_candidates,
                    ReadyCandidate {
                        pending_path,
                        action_id,
                        enqueued_utc,
                        queue_priority: priority,
                        action_priority: action_scheduler_priority(&action),
                    },
                    MAX_READY_CANDIDATES,
                );
            }
            ActionReadiness::NotReady => continue,
            ActionReadiness::Blocked {
                dependency_action_id,
                root_failed_action_id,
                reason,
            } => {
                try_write_dependency_canceled_for_pending(
                    store,
                    &pending_path,
                    &action_id,
                    enqueued_utc,
                    action,
                    worker_id,
                    &dependency_action_id,
                    &root_failed_action_id,
                    &reason,
                )?;
                continue;
            }
        }

        if scanned >= MAX_PENDING_SCAN_PER_CLAIM && !ready_candidates.is_empty() {
            break;
        }
    }
    ready_candidates.sort_by(cmp_ready_candidate);
    for candidate in ready_candidates {
        if let Some(claimed) =
            try_claim_pending_item(store, &candidate.pending_path, worker_id, lease_seconds)?
        {
            return Ok(Some(claimed));
        }
    }
    Ok(None)
}

pub(crate) fn claim_compatible_pending_items(
    store: &ArtifactStore,
    worker_id: &str,
    lease_seconds: i64,
    batch_key: &ActionBatchKey,
    required_queue_priority: i32,
    limit: usize,
) -> Result<Vec<QueueRunningWithPath>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    #[derive(Debug)]
    struct ReadyCandidate {
        pending_path: PathBuf,
        action_id: String,
        enqueued_utc: DateTime<Utc>,
        queue_priority: i32,
        action_priority: u8,
    }

    fn cmp_ready_candidate(a: &ReadyCandidate, b: &ReadyCandidate) -> std::cmp::Ordering {
        b.queue_priority
            .cmp(&a.queue_priority)
            .then_with(|| a.action_priority.cmp(&b.action_priority))
            .then_with(|| a.enqueued_utc.cmp(&b.enqueued_utc))
            .then_with(|| a.action_id.cmp(&b.action_id))
    }

    fn insert_ready_candidate(
        candidates: &mut Vec<ReadyCandidate>,
        candidate: ReadyCandidate,
        max_candidates: usize,
    ) {
        candidates.push(candidate);
        candidates.sort_by(cmp_ready_candidate);
        if candidates.len() > max_candidates {
            candidates.pop();
        }
    }

    let mut pending = list_queue_files(&store.queue_pending_dir())?;
    pending.sort();
    if pending.is_empty() {
        return Ok(Vec::new());
    }

    let mut ready_candidates = Vec::new();
    for pending_path in pending {
        let bytes = match fs::read(&pending_path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("reading pending queue record: {}", pending_path.display())
                });
            }
        };
        let (action_id, enqueued_utc, priority, action) =
            match parse_queue_work_item(&bytes, &pending_path) {
                Ok(parsed) => parsed,
                Err(err) => {
                    quarantine_corrupt_queue_file(
                        &pending_path,
                        &format!("parse error while scanning pending queue: {:#}", err),
                    )?;
                    continue;
                }
            };
        if action_batch_key(&action).as_ref() != Some(batch_key) {
            continue;
        }
        if priority != required_queue_priority {
            continue;
        }
        match classify_action_readiness(store, &action)? {
            ActionReadiness::Ready => {
                insert_ready_candidate(
                    &mut ready_candidates,
                    ReadyCandidate {
                        pending_path,
                        action_id,
                        enqueued_utc,
                        queue_priority: priority,
                        action_priority: action_scheduler_priority(&action),
                    },
                    limit,
                );
            }
            ActionReadiness::NotReady => continue,
            ActionReadiness::Blocked {
                dependency_action_id,
                root_failed_action_id,
                reason,
            } => {
                try_write_dependency_canceled_for_pending(
                    store,
                    &pending_path,
                    &action_id,
                    enqueued_utc,
                    action,
                    worker_id,
                    &dependency_action_id,
                    &root_failed_action_id,
                    &reason,
                )?;
            }
        }
    }

    ready_candidates.sort_by(cmp_ready_candidate);
    let mut claimed = Vec::new();
    for candidate in ready_candidates {
        if claimed.len() >= limit {
            break;
        }
        if let Some(running) =
            try_claim_pending_item(store, &candidate.pending_path, worker_id, lease_seconds)?
        {
            claimed.push(running);
        }
    }
    Ok(claimed)
}

const LOWEST_ACTION_SCHEDULER_PRIORITY: u8 = 13;

pub(crate) fn action_scheduler_priority(action: &ActionSpec) -> u8 {
    match action {
        ActionSpec::AigStatDiff { .. } => 0,
        ActionSpec::DriverAigToStats { .. } => 1,
        ActionSpec::AigToYosysAbcAig { .. } => 2,
        ActionSpec::ComboVerilogToYosysAbcAig { .. } => 3,
        ActionSpec::DriverIrToG8rAig { .. } => 4,
        ActionSpec::IrFnToCombinationalVerilog {
            top_fn_name: Some(top_fn_name),
            ..
        } if top_fn_name.starts_with("__mffc_") => 3,
        ActionSpec::IrFnToCombinationalVerilog { .. } => 5,
        ActionSpec::DriverIrAigEquiv { .. } => 6,
        ActionSpec::DriverIrToOpt { .. } => 7,
        ActionSpec::DriverDslxFnToIr { .. } => 8,
        ActionSpec::DriverIrToDelayInfo { .. } => 9,
        ActionSpec::DriverIrEquiv { .. } => 10,
        ActionSpec::IrFnToMffcCorpus { .. } => 11,
        ActionSpec::IrFnToKBoolConeCorpus { .. } => 12,
        ActionSpec::ImportIrPackageFile { .. }
        | ActionSpec::DownloadAndExtractXlsynthSourceSubtree { .. } => 12,
        ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { .. } => {
            LOWEST_ACTION_SCHEDULER_PRIORITY
        }
    }
}

pub(crate) fn suggested_action_queue_priority(base_priority: i32, action: &ActionSpec) -> i32 {
    base_priority
        + i32::from(
            LOWEST_ACTION_SCHEDULER_PRIORITY.saturating_sub(action_scheduler_priority(action)),
        )
}

#[derive(Debug, Clone)]
pub(crate) enum ActionReadiness {
    Ready,
    NotReady,
    Blocked {
        dependency_action_id: String,
        root_failed_action_id: String,
        reason: String,
    },
}

pub(crate) fn classify_action_readiness(
    store: &ArtifactStore,
    action: &ActionSpec,
) -> Result<ActionReadiness> {
    let mut saw_not_ready = false;
    for dep_action_id in action_dependency_action_ids(action) {
        if store.action_exists(dep_action_id) {
            continue;
        }
        match queue_state_for_action(store, dep_action_id) {
            QueueState::Pending | QueueState::Running { .. } => {
                saw_not_ready = true;
            }
            QueueState::Failed => {
                return Ok(ActionReadiness::Blocked {
                    dependency_action_id: dep_action_id.to_string(),
                    root_failed_action_id: dep_action_id.to_string(),
                    reason: "dependency failed or was canceled".to_string(),
                });
            }
            QueueState::Canceled => {
                let root_failed_action_id =
                    load_canceled_root_failed_action_id(store, dep_action_id)?
                        .unwrap_or_else(|| dep_action_id.to_string());
                return Ok(ActionReadiness::Blocked {
                    dependency_action_id: dep_action_id.to_string(),
                    root_failed_action_id,
                    reason: "dependency failed or was canceled".to_string(),
                });
            }
            QueueState::Done => {
                return Ok(ActionReadiness::Blocked {
                    dependency_action_id: dep_action_id.to_string(),
                    root_failed_action_id: dep_action_id.to_string(),
                    reason: "dependency has stale done-queue state but missing artifact/provenance"
                        .to_string(),
                });
            }
            QueueState::None => {
                return Ok(ActionReadiness::Blocked {
                    dependency_action_id: dep_action_id.to_string(),
                    root_failed_action_id: dep_action_id.to_string(),
                    reason: "dependency is missing from artifacts and queue".to_string(),
                });
            }
        }
    }
    if saw_not_ready {
        Ok(ActionReadiness::NotReady)
    } else {
        Ok(ActionReadiness::Ready)
    }
}

pub(crate) fn load_canceled_root_failed_action_id(
    store: &ArtifactStore,
    action_id: &str,
) -> Result<Option<String>> {
    let path = store.canceled_queue_path(action_id);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(&path)
        .with_context(|| format!("reading canceled queue record: {}", path.display()))?;
    let canceled = decode_queue_canceled(&bytes)
        .with_context(|| format!("parsing canceled queue record: {}", path.display()))?;
    Ok(Some(canceled.root_failed_action_id))
}

pub(crate) fn try_claim_pending_item(
    store: &ArtifactStore,
    pending_path: &Path,
    worker_id: &str,
    lease_seconds: i64,
) -> Result<Option<QueueRunningWithPath>> {
    let action_id = match queue_action_id_from_path(pending_path) {
        Some(v) => v,
        None => return Ok(None),
    };
    if terminal_queue_state_for_action(store, &action_id).is_some() {
        remove_file_if_exists(pending_path)?;
        return Ok(None);
    }
    let running_path = store.running_queue_path(&action_id);
    if let Some(parent) = running_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating running queue dir: {}", parent.display()))?;
    }
    match fs::hard_link(pending_path, &running_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(None),
        Err(e) => {
            return Err(e).with_context(|| {
                format!(
                    "claiming queue item by hard-link: {} -> {}",
                    pending_path.display(),
                    running_path.display()
                )
            });
        }
    }
    if terminal_queue_state_for_action(store, &action_id).is_some() {
        remove_file_if_exists(pending_path)?;
        remove_file_if_exists(&running_path)?;
        return Ok(None);
    }
    match fs::remove_file(pending_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            remove_file_if_exists(&running_path).ok();
            return Err(e).with_context(|| {
                format!("removing claimed pending file: {}", pending_path.display())
            });
        }
    }

    let bytes = fs::read(&running_path)
        .with_context(|| format!("reading claimed queue item: {}", running_path.display()))?;
    let (action_id_from_file, enqueued_utc, priority, action) =
        parse_queue_work_item(&bytes, &running_path)?;
    if action_id_from_file != action_id {
        bail!(
            "claimed queue action id mismatch in {}: path={} file={}",
            running_path.display(),
            action_id,
            action_id_from_file
        );
    }
    let computed = crate::executor::compute_action_id(&action)?;
    if computed != action_id {
        bail!(
            "claimed queue action hash mismatch in {}: path={} computed={}",
            running_path.display(),
            action_id,
            computed
        );
    }

    let lease_acquired_utc = Utc::now();
    let lease_expires_utc = lease_acquired_utc + chrono::Duration::seconds(lease_seconds);
    let running = QueueRunning {
        schema_version: crate::ACTION_SCHEMA_VERSION,
        action_id,
        enqueued_utc,
        priority,
        action,
        lease_owner: worker_id.to_string(),
        lease_acquired_utc,
        lease_expires_utc,
    };
    write_bytes_atomic(&running_path, &encode_queue_running(&running)?)?;
    Ok(Some(QueueRunningWithPath {
        running,
        path: running_path,
    }))
}

pub(crate) fn write_done_record(
    store: &ArtifactStore,
    running: &QueueRunningWithPath,
    output_artifact: ArtifactRef,
    worker_id: &str,
) -> Result<()> {
    let done = QueueDone {
        schema_version: crate::ACTION_SCHEMA_VERSION,
        action_id: running.action_id().to_string(),
        completed_utc: Utc::now(),
        completed_by: worker_id.to_string(),
        output_artifact,
    };
    let done_path = store.done_queue_path(running.action_id());
    if let Some(parent) = done_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating done queue dir: {}", parent.display()))?;
    }
    write_bytes_atomic(&done_path, &encode_queue_done(&done)?)?;
    Ok(())
}

pub(crate) fn write_failed_record(
    store: &ArtifactStore,
    running: &QueueRunningWithPath,
    worker_id: &str,
    error: &str,
) -> Result<bool> {
    if store.action_exists(running.action_id()) {
        return Ok(false);
    }
    let failed = QueueFailed {
        schema_version: crate::ACTION_SCHEMA_VERSION,
        action_id: running.action_id().to_string(),
        enqueued_utc: *running.enqueued_utc(),
        failed_utc: Utc::now(),
        failed_by: worker_id.to_string(),
        action: running.action().clone(),
        error: error.to_string(),
    };
    // Queue records are ephemeral; persist terminal failures only in the durable store.
    write_failed_action_record(store, &failed)?;
    if store.action_exists(running.action_id()) {
        store.delete_failed_action_record(running.action_id())?;
        return Ok(false);
    }
    Ok(true)
}

pub(crate) fn write_failed_action_record(
    store: &ArtifactStore,
    failed: &QueueFailed,
) -> Result<()> {
    store.write_failed_action_record(failed)
}

pub(crate) fn action_dependency_role_action_ids(action: &ActionSpec) -> Vec<(&'static str, &str)> {
    match action {
        ActionSpec::ImportIrPackageFile { .. }
        | ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball { .. }
        | ActionSpec::DownloadAndExtractXlsynthSourceSubtree { .. } => Vec::new(),
        ActionSpec::DriverDslxFnToIr {
            dslx_subtree_action_id,
            ..
        } => vec![("dslx_subtree_action_id", dslx_subtree_action_id.as_str())],
        ActionSpec::DriverIrToOpt { ir_action_id, .. } => {
            vec![("ir_action_id", ir_action_id.as_str())]
        }
        ActionSpec::DriverIrToDelayInfo { ir_action_id, .. } => {
            vec![("ir_action_id", ir_action_id.as_str())]
        }
        ActionSpec::DriverIrEquiv {
            lhs_ir_action_id,
            rhs_ir_action_id,
            ..
        } => vec![
            ("lhs_ir_action_id", lhs_ir_action_id.as_str()),
            ("rhs_ir_action_id", rhs_ir_action_id.as_str()),
        ],
        ActionSpec::DriverIrAigEquiv {
            ir_action_id,
            aig_action_id,
            ..
        } => vec![
            ("ir_action_id", ir_action_id.as_str()),
            ("aig_action_id", aig_action_id.as_str()),
        ],
        ActionSpec::DriverIrToG8rAig { ir_action_id, .. } => {
            vec![("ir_action_id", ir_action_id.as_str())]
        }
        ActionSpec::IrFnToCombinationalVerilog { ir_action_id, .. } => {
            vec![("ir_action_id", ir_action_id.as_str())]
        }
        ActionSpec::IrFnToKBoolConeCorpus { ir_action_id, .. }
        | ActionSpec::IrFnToMffcCorpus { ir_action_id, .. } => {
            vec![("ir_action_id", ir_action_id.as_str())]
        }
        ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id, ..
        } => vec![("verilog_action_id", verilog_action_id.as_str())],
        ActionSpec::AigToYosysAbcAig { aig_action_id, .. } => {
            vec![("aig_action_id", aig_action_id.as_str())]
        }
        ActionSpec::DriverAigToStats { aig_action_id, .. } => {
            vec![("aig_action_id", aig_action_id.as_str())]
        }
        ActionSpec::AigStatDiff {
            opt_ir_action_id,
            g8r_aig_stats_action_id,
            yosys_abc_aig_stats_action_id,
        } => vec![
            ("opt_ir_action_id", opt_ir_action_id.as_str()),
            ("g8r_aig_stats_action_id", g8r_aig_stats_action_id.as_str()),
            (
                "yosys_abc_aig_stats_action_id",
                yosys_abc_aig_stats_action_id.as_str(),
            ),
        ],
    }
}

pub(crate) fn action_dependency_action_ids(action: &ActionSpec) -> Vec<&str> {
    action_dependency_role_action_ids(action)
        .into_iter()
        .map(|(_, action_id)| action_id)
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn write_canceled_record(
    store: &ArtifactStore,
    action_id: &str,
    enqueued_utc: DateTime<Utc>,
    action: ActionSpec,
    worker_id: &str,
    canceled_due_to_action_id: &str,
    root_failed_action_id: &str,
    reason: &str,
) -> Result<()> {
    let canceled = QueueCanceled {
        schema_version: crate::ACTION_SCHEMA_VERSION,
        action_id: action_id.to_string(),
        enqueued_utc,
        canceled_utc: Utc::now(),
        canceled_by: worker_id.to_string(),
        canceled_due_to_action_id: canceled_due_to_action_id.to_string(),
        root_failed_action_id: root_failed_action_id.to_string(),
        action,
        reason: reason.to_string(),
        cancellation_kind: QueueCancellationKind::Dependency,
        work_policy_rule_id: None,
        work_policy_rule_fingerprint: None,
    };
    let canceled_path = store.canceled_queue_path(action_id);
    if let Some(parent) = canceled_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating canceled queue dir: {}", parent.display()))?;
    }
    write_bytes_atomic(&canceled_path, &encode_queue_canceled(&canceled)?)?;
    Ok(())
}

fn reserve_pending_for_terminal_transition(
    store: &ArtifactStore,
    pending_path: &Path,
    action_id: &str,
) -> Result<Option<PathBuf>> {
    let running_path = store.running_queue_path(action_id);
    if let Some(parent) = running_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "creating terminal-transition reservation dir: {}",
                parent.display()
            )
        })?;
    }
    match fs::hard_link(pending_path, &running_path) {
        Ok(()) => Ok(Some(running_path)),
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                || error.kind() == std::io::ErrorKind::AlreadyExists =>
        {
            Ok(None)
        }
        Err(error) => Err(error).with_context(|| {
            format!(
                "reserving pending action for terminal transition: {} -> {}",
                pending_path.display(),
                running_path.display()
            )
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn try_write_dependency_canceled_for_pending(
    store: &ArtifactStore,
    pending_path: &Path,
    action_id: &str,
    enqueued_utc: DateTime<Utc>,
    action: ActionSpec,
    worker_id: &str,
    canceled_due_to_action_id: &str,
    root_failed_action_id: &str,
    reason: &str,
) -> Result<bool> {
    let Some(reservation_path) =
        reserve_pending_for_terminal_transition(store, pending_path, action_id)?
    else {
        return Ok(false);
    };
    write_canceled_record(
        store,
        action_id,
        enqueued_utc,
        action,
        worker_id,
        canceled_due_to_action_id,
        root_failed_action_id,
        reason,
    )?;
    remove_file_if_exists(pending_path)?;
    remove_file_if_exists(&reservation_path)?;
    Ok(true)
}

pub(crate) fn write_work_policy_excluded_record(
    store: &ArtifactStore,
    action_id: &str,
    source_action_id: &str,
    action: ActionSpec,
    rule_id: &str,
    reason: &str,
    rule_fingerprint: &str,
) -> Result<bool> {
    if store.action_exists(action_id)
        || store.running_queue_path(action_id).exists()
        || store.done_queue_path(action_id).exists()
    {
        return Ok(false);
    }

    if let Some(existing) = load_queue_canceled_record(store, action_id)? {
        if existing.cancellation_kind == QueueCancellationKind::Dependency {
            return Ok(false);
        }
        let existing_action_matches = crate::executor::compute_action_id(&existing.action)
            .is_ok_and(|existing_action_id| existing_action_id == action_id);
        if existing.work_policy_rule_id.as_deref() == Some(rule_id)
            && existing.work_policy_rule_fingerprint.as_deref() == Some(rule_fingerprint)
            && existing.canceled_due_to_action_id == source_action_id
            && existing.root_failed_action_id == action_id
            && existing.canceled_by == "campaign-work-policy"
            && existing.reason == reason
            && existing_action_matches
        {
            remove_file_if_exists(&store.pending_queue_path(action_id))?;
            store.delete_failed_action_record(action_id)?;
            return Ok(true);
        }
    }

    let pending_path = store.pending_queue_path(action_id);
    let reservation_path = if pending_path.exists() {
        let Some(path) = reserve_pending_for_terminal_transition(store, &pending_path, action_id)?
        else {
            return Ok(false);
        };
        Some(path)
    } else {
        None
    };

    let now = Utc::now();
    let canceled = QueueCanceled {
        schema_version: crate::ACTION_SCHEMA_VERSION,
        action_id: action_id.to_string(),
        enqueued_utc: now,
        canceled_utc: now,
        canceled_by: "campaign-work-policy".to_string(),
        canceled_due_to_action_id: source_action_id.to_string(),
        root_failed_action_id: action_id.to_string(),
        action,
        reason: reason.to_string(),
        cancellation_kind: QueueCancellationKind::WorkPolicyExcluded,
        work_policy_rule_id: Some(rule_id.to_string()),
        work_policy_rule_fingerprint: Some(rule_fingerprint.to_string()),
    };
    let canceled_path = store.canceled_queue_path(action_id);
    if let Some(parent) = canceled_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating canceled queue dir: {}", parent.display()))?;
    }

    write_bytes_atomic(&canceled_path, &encode_queue_canceled(&canceled)?)?;
    store.delete_failed_action_record(action_id)?;
    if let Some(reservation_path) = reservation_path {
        remove_file_if_exists(&pending_path)?;
        remove_file_if_exists(&reservation_path)?;
    }
    Ok(true)
}

pub(crate) fn remove_work_policy_excluded_record(
    store: &ArtifactStore,
    action_id: &str,
) -> Result<bool> {
    let Some(existing) = load_queue_canceled_record(store, action_id)? else {
        return Ok(false);
    };
    if existing.cancellation_kind != QueueCancellationKind::WorkPolicyExcluded {
        return Ok(false);
    }
    remove_file_if_exists(&store.canceled_queue_path(action_id))?;
    Ok(true)
}

pub(crate) fn cancel_downstream_pending_actions(
    store: &ArtifactStore,
    root_failed_action_id: &str,
    worker_id: &str,
) -> Result<usize> {
    let mut blocked_ids = HashSet::new();
    blocked_ids.insert(root_failed_action_id.to_string());
    let mut canceled = 0_usize;

    loop {
        let mut changed = false;
        let mut pending_paths = list_queue_files(&store.queue_pending_dir())?;
        pending_paths.sort();
        for pending_path in pending_paths {
            let bytes = match fs::read(&pending_path) {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(e).with_context(|| {
                        format!("reading pending queue record: {}", pending_path.display())
                    });
                }
            };
            let (action_id, enqueued_utc, _, action) =
                parse_queue_work_item(&bytes, &pending_path)?;
            if blocked_ids.contains(&action_id) {
                continue;
            }
            let Some(dep) = action_dependency_action_ids(&action)
                .into_iter()
                .find(|dep_id| blocked_ids.contains(*dep_id))
                .map(ToOwned::to_owned)
            else {
                continue;
            };

            if try_write_dependency_canceled_for_pending(
                store,
                &pending_path,
                &action_id,
                enqueued_utc,
                action,
                worker_id,
                &dep,
                root_failed_action_id,
                "dependency failed or was canceled",
            )? {
                blocked_ids.insert(action_id);
                canceled += 1;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    Ok(canceled)
}

pub(crate) fn reclaim_expired_running_leases(store: &ArtifactStore) -> Result<usize> {
    let now = Utc::now();
    let local_host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".to_string());
    let mut reclaimed = 0_usize;
    let mut running_paths = list_queue_files(&store.queue_running_dir())?;
    running_paths.sort();
    for running_path in running_paths {
        if let Some(action_id) = queue_action_id_from_path(&running_path)
            && terminal_queue_state_for_action(store, &action_id).is_some()
        {
            remove_file_if_exists(&store.pending_queue_path(&action_id))?;
            remove_file_if_exists(&running_path)?;
            if store.action_exists(&action_id) {
                store.delete_failed_action_record(&action_id)?;
                remove_file_if_exists(&store.canceled_queue_path(&action_id))?;
            }
            reclaimed += 1;
            continue;
        }
        let bytes = match fs::read(&running_path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("reading running queue record: {}", running_path.display())
                });
            }
        };

        let reclaim = if let Ok(running) = decode_queue_running(&bytes) {
            match running_owner_local_process_exists(&running.lease_owner, &local_host) {
                Some(true) => false,
                Some(false) => true,
                None => running.lease_expires_utc <= now,
            }
        } else {
            true
        };
        if !reclaim {
            continue;
        }

        let (action_id, enqueued_utc, priority, action) =
            match parse_queue_work_item(&bytes, &running_path) {
                Ok(parsed) => parsed,
                Err(err) => {
                    quarantine_corrupt_queue_file(
                        &running_path,
                        &format!("parse error while reclaiming running lease: {:#}", err),
                    )?;
                    reclaimed += 1;
                    continue;
                }
            };
        let pending = QueueItem {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: action_id.clone(),
            enqueued_utc,
            priority,
            action,
        };
        if terminal_queue_state_for_action(store, &action_id).is_some() {
            remove_file_if_exists(&store.pending_queue_path(&action_id))?;
            remove_file_if_exists(&running_path)?;
            if store.action_exists(&action_id) {
                store.delete_failed_action_record(&action_id)?;
                remove_file_if_exists(&store.canceled_queue_path(&action_id))?;
            }
            reclaimed += 1;
            continue;
        }
        let pending_path = store.pending_queue_path(&action_id);
        if let Some(parent) = pending_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating pending queue dir: {}", parent.display()))?;
        }
        if !pending_path.exists() {
            write_bytes_atomic(&pending_path, &encode_queue_item(&pending)?)?;
        }
        remove_file_if_exists(&running_path)?;
        reclaimed += 1;
    }
    Ok(reclaimed)
}

fn queue_process_start_ticks(pid: u32) -> Option<u64> {
    let stat = fs::read_to_string(Path::new("/proc").join(pid.to_string()).join("stat")).ok()?;
    let after_comm = stat.rsplit_once(") ")?.1;
    after_comm.split_whitespace().nth(19)?.parse().ok()
}

fn running_owner_local_process_exists(lease_owner: &str, local_host: &str) -> Option<bool> {
    let mut parts = lease_owner.split(':');
    let owner_host = parts.next()?;
    if owner_host != local_host {
        return None;
    }
    let owner_pid = parts.next()?.parse::<u32>().ok()?;
    let proc_path = Path::new("/proc").join(owner_pid.to_string());
    if !proc_path.exists() {
        return Some(false);
    }
    let Some(recorded_start_ticks) = parts.next().and_then(|part| part.parse::<u64>().ok()) else {
        return Some(true);
    };
    Some(queue_process_start_ticks(owner_pid) == Some(recorded_start_ticks))
}

#[cfg(test)]
fn running_owner_process_missing_on_local_host(lease_owner: &str, local_host: &str) -> bool {
    running_owner_local_process_exists(lease_owner, local_host) == Some(false)
}

pub(crate) fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("removing file: {}", path.display())),
    }
}

pub(crate) fn queue_action_id_from_path(path: &Path) -> Option<String> {
    path.file_stem()
        .and_then(|s| s.to_str())
        .map(ToOwned::to_owned)
}

pub(crate) fn parse_queue_work_item(
    bytes: &[u8],
    path: &Path,
) -> Result<(String, DateTime<Utc>, i32, ActionSpec)> {
    if let Ok(running) = decode_queue_running(bytes) {
        return Ok((
            running.action_id,
            running.enqueued_utc,
            running.priority,
            running.action,
        ));
    }

    let item = decode_queue_item(bytes)
        .with_context(|| format!("parsing queue item: {}", path.display()))?;
    Ok((
        item.action_id,
        item.enqueued_utc,
        item.priority,
        item.action,
    ))
}

pub(crate) fn list_queue_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    for entry in WalkDir::new(root).sort_by_file_name() {
        let entry = entry.context("walking queue tree")?;
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("pb") {
            files.push(path.to_path_buf());
        }
    }
    Ok(files)
}

pub(crate) fn load_queue_pending_record(
    store: &ArtifactStore,
    action_id: &str,
) -> Result<Option<QueueItem>> {
    load_queue_record(
        &store.pending_queue_path(action_id),
        "pending queue record",
        decode_queue_item,
    )
}

pub(crate) fn load_queue_running_record(
    store: &ArtifactStore,
    action_id: &str,
) -> Result<Option<QueueRunning>> {
    load_queue_record(
        &store.running_queue_path(action_id),
        "running queue record",
        decode_queue_running,
    )
}

pub(crate) fn load_queue_done_record(
    store: &ArtifactStore,
    action_id: &str,
) -> Result<Option<QueueDone>> {
    load_queue_record(
        &store.done_queue_path(action_id),
        "done queue record",
        decode_queue_done,
    )
}

pub(crate) fn load_queue_failed_record(
    store: &ArtifactStore,
    action_id: &str,
) -> Result<Option<QueueFailed>> {
    store.load_failed_action_record(action_id)
}

pub(crate) fn load_queue_canceled_record(
    store: &ArtifactStore,
    action_id: &str,
) -> Result<Option<QueueCanceled>> {
    load_queue_record(
        &store.canceled_queue_path(action_id),
        "canceled queue record",
        decode_queue_canceled,
    )
}

pub(crate) fn load_queue_record<T>(
    path: &Path,
    label: &str,
    decode: fn(&[u8]) -> Result<T>,
) -> Result<Option<T>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path).with_context(|| format!("reading {}: {}", label, path.display()))?;
    let record =
        decode(&bytes).with_context(|| format!("parsing {}: {}", label, path.display()))?;
    Ok(Some(record))
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CorruptQueueRecord {
    pub(crate) queue_state: String,
    pub(crate) path: String,
    pub(crate) action_id_hint: Option<String>,
    pub(crate) reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct QueueRepairSummary {
    pub(crate) scanned_files: usize,
    pub(crate) corrupt_count: usize,
    pub(crate) removed_count: usize,
    pub(crate) corrupt_records: Vec<CorruptQueueRecord>,
}

pub(crate) fn repair_corrupt_queue_records(
    store: &ArtifactStore,
    apply: bool,
) -> Result<QueueRepairSummary> {
    fn scan_and_validate_queue_dir(
        root: &Path,
        queue_state: &str,
        parser: fn(&[u8], &Path) -> Result<String>,
        scanned_files: &mut usize,
        corrupt_records: &mut Vec<CorruptQueueRecord>,
    ) -> Result<()> {
        for path in list_queue_files(root)? {
            *scanned_files += 1;
            let action_id_hint = queue_action_id_from_path(&path);
            let bytes = match fs::read(&path) {
                Ok(bytes) => bytes,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    corrupt_records.push(CorruptQueueRecord {
                        queue_state: queue_state.to_string(),
                        path: path.display().to_string(),
                        action_id_hint,
                        reason: format!("reading {} queue record failed: {}", queue_state, err),
                    });
                    continue;
                }
            };
            let parsed_action_id = match parser(&bytes, &path) {
                Ok(action_id) => action_id,
                Err(err) => {
                    corrupt_records.push(CorruptQueueRecord {
                        queue_state: queue_state.to_string(),
                        path: path.display().to_string(),
                        action_id_hint,
                        reason: format!("{:#}", err),
                    });
                    continue;
                }
            };
            if let Some(file_action_id) = queue_action_id_from_path(&path)
                && file_action_id != parsed_action_id
            {
                corrupt_records.push(CorruptQueueRecord {
                    queue_state: queue_state.to_string(),
                    path: path.display().to_string(),
                    action_id_hint: Some(file_action_id),
                    reason: format!(
                        "action id mismatch: file stem {} != parsed {}",
                        queue_action_id_from_path(&path).unwrap_or_default(),
                        parsed_action_id
                    ),
                });
            }
        }
        Ok(())
    }

    fn parse_pending_record(bytes: &[u8], path: &Path) -> Result<String> {
        let item = decode_queue_item(bytes)
            .with_context(|| format!("parsing pending queue record: {}", path.display()))?;
        Ok(item.action_id)
    }

    fn parse_running_record(bytes: &[u8], path: &Path) -> Result<String> {
        let running = decode_queue_running(bytes)
            .with_context(|| format!("parsing running queue record: {}", path.display()))?;
        Ok(running.action_id)
    }

    fn parse_done_record(bytes: &[u8], path: &Path) -> Result<String> {
        let done = decode_queue_done(bytes)
            .with_context(|| format!("parsing done queue record: {}", path.display()))?;
        Ok(done.action_id)
    }

    fn parse_canceled_record(bytes: &[u8], path: &Path) -> Result<String> {
        let canceled = decode_queue_canceled(bytes)
            .with_context(|| format!("parsing canceled queue record: {}", path.display()))?;
        Ok(canceled.action_id)
    }

    let mut scanned_files = 0_usize;
    let mut corrupt_records = Vec::new();
    scan_and_validate_queue_dir(
        &store.queue_pending_dir(),
        "pending",
        parse_pending_record,
        &mut scanned_files,
        &mut corrupt_records,
    )?;
    scan_and_validate_queue_dir(
        &store.queue_running_dir(),
        "running",
        parse_running_record,
        &mut scanned_files,
        &mut corrupt_records,
    )?;
    scan_and_validate_queue_dir(
        &store.queue_done_dir(),
        "done",
        parse_done_record,
        &mut scanned_files,
        &mut corrupt_records,
    )?;
    scan_and_validate_queue_dir(
        &store.queue_canceled_dir(),
        "canceled",
        parse_canceled_record,
        &mut scanned_files,
        &mut corrupt_records,
    )?;

    let mut removed_count = 0_usize;
    if apply {
        let mut removed = HashSet::new();
        for record in &corrupt_records {
            let path = Path::new(&record.path);
            if !removed.insert(path.to_path_buf()) {
                continue;
            }
            if path.exists() {
                remove_file_if_exists(path)?;
                removed_count += 1;
            }
        }
    }

    Ok(QueueRepairSummary {
        scanned_files,
        corrupt_count: corrupt_records.len(),
        removed_count,
        corrupt_records,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ArtifactType, G8rLoweringMode, OutputFile, Provenance};
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_test_store() -> (ArtifactStore, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "xlsynth-bvc-queue-test-{}-{}",
            std::process::id(),
            nanos
        ));
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure store layout");
        (store, root)
    }

    fn sample_runtime() -> crate::model::DriverRuntimeSpec {
        crate::model::DriverRuntimeSpec {
            driver_version: "0.31.0".to_string(),
            release_platform: "ubuntu2004".to_string(),
            docker_image: "xlsynth-bvc-driver:0.31.0".to_string(),
            dockerfile: "docker/xlsynth-driver.Dockerfile".to_string(),
            dockerfile_sha256: "d".repeat(64),
            docker_image_id: "e".repeat(64),
        }
    }

    fn write_completed_provenance(store: &ArtifactStore, seed_digest: &str) -> String {
        let bytes = b"package test\n";
        let action = ActionSpec::ImportIrPackageFile {
            source_sha256: seed_digest.to_string(),
            top_fn_name: Some("main".to_string()),
        };
        let action_id = crate::executor::compute_action_id(&action).expect("compute V2 action id");
        let provenance = Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: action_id.to_string(),
            created_utc: Utc::now(),
            action,
            dependencies: Vec::new(),
            output_artifact: ArtifactRef {
                action_id: action_id.to_string(),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: "payload/input.ir".to_string(),
            },
            output_files: vec![OutputFile {
                path: "payload/input.ir".to_string(),
                bytes: bytes.len() as u64,
                sha256: format!("{:x}", Sha256::digest(bytes)),
            }],
            commands: Vec::new(),
            details: json!({}),
            suggested_next_actions: Vec::new(),
        };
        let staging_dir = store.staging_dir().join(format!("{action_id}-ready"));
        fs::create_dir_all(staging_dir.join("payload")).expect("create ready payload");
        fs::write(staging_dir.join("payload/input.ir"), bytes).expect("write ready input");
        fs::write(
            staging_dir.join("provenance.pb"),
            crate::proto::encode_provenance(&provenance).expect("encode provenance"),
        )
        .expect("write provenance");
        store
            .promote_staging_action_dir(&action_id, &staging_dir)
            .expect("promote ready action");
        action_id
    }

    #[test]
    fn queue_state_key_roundtrip_covers_all_known_states() {
        let keys = [
            "pending",
            "running",
            "done",
            "failed",
            "canceled",
            "not_queued",
        ];
        for key in keys {
            let state = QueueState::from_key(key).expect("valid state key");
            assert_eq!(queue_state_key(&state), key);
            assert!(!queue_state_display_label(&state).is_empty());
        }
        assert!(QueueState::from_key("bogus").is_none());
    }

    #[test]
    fn claim_next_pending_item_moves_record_to_running() {
        let (store, root) = make_test_store();
        let action = ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
            version: "v0.37.0".to_string(),
            discovery_runtime: None,
            stdlib_tarball_sha256: "11".repeat(32),
        };
        let action_id = enqueue_action(&store, action.clone()).expect("enqueue action");
        assert!(store.pending_queue_path(&action_id).exists());

        let claimed = claim_next_pending_item(&store, "worker-a", 60)
            .expect("claim next pending")
            .expect("expected claimed item");
        assert_eq!(claimed.action_id(), action_id);
        assert_eq!(
            serde_json::to_value(claimed.action()).expect("serialize claimed action"),
            serde_json::to_value(&action).expect("serialize expected action")
        );
        assert_eq!(claimed.running.lease_owner, "worker-a");
        assert!(claimed.running.lease_expires_utc > claimed.running.lease_acquired_utc);
        assert!(!store.pending_queue_path(&action_id).exists());
        assert!(store.running_queue_path(&action_id).exists());

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn enqueue_action_with_priority_promotes_existing_pending_item() {
        let (store, root) = make_test_store();
        let action = ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
            version: "v0.37.0".to_string(),
            discovery_runtime: None,
            stdlib_tarball_sha256: "11".repeat(32),
        };
        let action_id =
            enqueue_action_with_priority(&store, action.clone(), 0).expect("enqueue initial");
        let initial = load_queue_pending_record(&store, &action_id)
            .expect("load pending record")
            .expect("pending record exists");
        assert_eq!(initial.priority, 0);

        enqueue_action_with_priority(&store, action.clone(), 42).expect("promote pending item");
        let promoted = load_queue_pending_record(&store, &action_id)
            .expect("load pending record")
            .expect("pending record exists");
        assert_eq!(promoted.priority, 42);

        enqueue_action_with_priority(&store, action, -5).expect("keep promoted priority");
        let not_demoted = load_queue_pending_record(&store, &action_id)
            .expect("load pending record")
            .expect("pending record exists");
        assert_eq!(not_demoted.priority, 42);

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn claim_next_pending_item_prefers_explicit_queue_priority_over_action_kind_priority() {
        let (store, root) = make_test_store();
        let runtime = sample_runtime();
        let lower_action_priority = ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
            version: "v0.37.0".to_string(),
            discovery_runtime: Some(runtime.clone()),
            stdlib_tarball_sha256: "11".repeat(32),
        };
        let higher_action_priority = ActionSpec::DownloadAndExtractXlsynthSourceSubtree {
            version: "v0.37.0".to_string(),
            subtree: "xls/modules/add_dual_path".to_string(),
            discovery_runtime: Some(runtime),
            source_commit: "2".repeat(40),
        };

        let lower_id = crate::executor::compute_action_id(&lower_action_priority)
            .expect("compute lower action id");
        let higher_id = crate::executor::compute_action_id(&higher_action_priority)
            .expect("compute higher action id");
        assert!(
            action_scheduler_priority(&higher_action_priority)
                < action_scheduler_priority(&lower_action_priority)
        );

        enqueue_action_with_priority(&store, higher_action_priority, 0)
            .expect("enqueue default-priority action");
        enqueue_action_with_priority(&store, lower_action_priority, 100)
            .expect("enqueue urgent action");

        let claimed = claim_next_pending_item(&store, "worker-priority-override", 60)
            .expect("claim next pending")
            .expect("expected claimed item");
        assert_eq!(claimed.action_id(), lower_id);
        assert_ne!(claimed.action_id(), higher_id);
        assert_eq!(claimed.priority(), 100);

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn claim_compatible_pending_items_only_claims_matching_g8r_batch_key() {
        let (store, root) = make_test_store();
        let dep_id = write_completed_provenance(&store, &"d".repeat(64));

        let matching_a = ActionSpec::DriverIrToG8rAig {
            ir_action_id: dep_id.clone(),
            top_fn_name: Some("__cone_a".to_string()),
            fraig: false,
            lowering_mode: G8rLoweringMode::Default,
            version: "v0.39.0".to_string(),
            runtime: sample_runtime(),
        };
        let matching_b = ActionSpec::DriverIrToG8rAig {
            ir_action_id: dep_id.clone(),
            top_fn_name: Some("__cone_b".to_string()),
            fraig: false,
            lowering_mode: G8rLoweringMode::Default,
            version: "v0.39.0".to_string(),
            runtime: sample_runtime(),
        };
        let different_mode = ActionSpec::DriverIrToG8rAig {
            ir_action_id: dep_id,
            top_fn_name: Some("__cone_c".to_string()),
            fraig: false,
            lowering_mode: G8rLoweringMode::FrontendNoPrepRewrite,
            version: "v0.39.0".to_string(),
            runtime: sample_runtime(),
        };
        let lower_priority = ActionSpec::DriverIrToG8rAig {
            ir_action_id: "d".repeat(64),
            top_fn_name: Some("__cone_d".to_string()),
            fraig: false,
            lowering_mode: G8rLoweringMode::Default,
            version: "v0.39.0".to_string(),
            runtime: sample_runtime(),
        };

        let matching_a_id = enqueue_action_with_priority(&store, matching_a.clone(), 7)
            .expect("enqueue matching a");
        let matching_b_id = enqueue_action_with_priority(&store, matching_b.clone(), 7)
            .expect("enqueue matching b");
        let different_id = enqueue_action(&store, different_mode).expect("enqueue different");
        let lower_priority_id = enqueue_action_with_priority(&store, lower_priority, 6)
            .expect("enqueue lower priority");

        let first = claim_next_pending_item(&store, "worker-batch", 60)
            .expect("claim first")
            .expect("claimed first item");
        let batch_key = action_batch_key(first.action()).expect("g8r batch key");
        let claimed = claim_compatible_pending_items(
            &store,
            "worker-batch",
            60,
            &batch_key,
            first.priority(),
            8,
        )
        .expect("claim compatible items");
        let claimed_ids: HashSet<_> = claimed
            .iter()
            .map(|running| running.action_id().to_string())
            .collect();

        let first_id = first.action_id().to_string();
        let expected_other = if first_id == matching_a_id {
            matching_b_id
        } else {
            matching_a_id
        };
        assert_eq!(claimed_ids.len(), 1);
        assert!(claimed_ids.contains(&expected_other));
        assert!(!claimed_ids.contains(&different_id));
        assert!(!claimed_ids.contains(&lower_priority_id));

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn classify_action_readiness_blocks_on_failed_or_canceled_dependencies() {
        let (store, root) = make_test_store();
        let dep_failed = "1".repeat(64);
        let dep_canceled = "2".repeat(64);
        let root_failed = "3".repeat(64);

        let failed_record = QueueFailed {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: dep_failed.clone(),
            enqueued_utc: Utc::now(),
            failed_utc: Utc::now(),
            failed_by: "worker-a".to_string(),
            action: ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                version: "v0.37.0".to_string(),
                discovery_runtime: None,
                stdlib_tarball_sha256: "11".repeat(32),
            },
            error: "boom".to_string(),
        };
        write_failed_action_record(&store, &failed_record).expect("write failed action record");

        let blocked_on_failed = ActionSpec::DriverIrToOpt {
            ir_action_id: dep_failed.clone(),
            top_fn_name: None,
            version: "v0.35.0".to_string(),
            runtime: sample_runtime(),
        };
        let readiness =
            classify_action_readiness(&store, &blocked_on_failed).expect("classify readiness");
        match readiness {
            ActionReadiness::Blocked {
                dependency_action_id,
                root_failed_action_id,
                ..
            } => {
                assert_eq!(dependency_action_id, dep_failed);
                assert_eq!(root_failed_action_id, dep_failed);
            }
            other => panic!("expected blocked readiness, got {:?}", other),
        }

        write_canceled_record(
            &store,
            &dep_canceled,
            Utc::now(),
            ActionSpec::DriverIrToOpt {
                ir_action_id: dep_failed.clone(),
                top_fn_name: None,
                version: "v0.35.0".to_string(),
                runtime: sample_runtime(),
            },
            "worker-b",
            &dep_failed,
            &root_failed,
            "dependency failed",
        )
        .expect("write canceled record");

        let blocked_on_canceled = ActionSpec::DriverIrToOpt {
            ir_action_id: dep_canceled.clone(),
            top_fn_name: None,
            version: "v0.35.0".to_string(),
            runtime: sample_runtime(),
        };
        let readiness =
            classify_action_readiness(&store, &blocked_on_canceled).expect("classify readiness");
        match readiness {
            ActionReadiness::Blocked {
                dependency_action_id,
                root_failed_action_id,
                ..
            } => {
                assert_eq!(dependency_action_id, dep_canceled);
                assert_eq!(root_failed_action_id, root_failed);
            }
            other => panic!("expected blocked readiness, got {:?}", other),
        }

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    fn touch_fake_provenance(store: &ArtifactStore, seed_digest: &str) -> String {
        let action = ActionSpec::ImportIrPackageFile {
            source_sha256: seed_digest.to_string(),
            top_fn_name: Some("main".to_string()),
        };
        let action_id = crate::executor::compute_action_id(&action).expect("compute V2 action id");
        let staging_dir = store.staging_dir().join(format!("{action_id}-fake"));
        let payload_dir = staging_dir.join("payload");
        fs::create_dir_all(&payload_dir).expect("create fake payload dir");
        fs::write(payload_dir.join("marker.txt"), &action_id).expect("write fake payload marker");
        let provenance = crate::model::Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: action_id.to_string(),
            created_utc: Utc::now(),
            action,
            dependencies: Vec::new(),
            output_artifact: ArtifactRef {
                action_id: action_id.to_string(),
                artifact_type: crate::model::ArtifactType::DslxFileSubtree,
                relpath: "payload".to_string(),
            },
            output_files: vec![crate::model::OutputFile {
                path: "payload/marker.txt".to_string(),
                bytes: action_id.len() as u64,
                sha256: "0".repeat(64),
            }],
            commands: Vec::new(),
            details: serde_json::json!({"synthetic": true}),
            suggested_next_actions: Vec::new(),
        };
        fs::write(
            staging_dir.join("provenance.pb"),
            crate::proto::encode_provenance(&provenance).expect("encode fake provenance"),
        )
        .expect("write fake provenance");
        store
            .promote_staging_action_dir(&action_id, &staging_dir)
            .expect("promote fake provenance action");
        action_id
    }

    #[test]
    fn claim_next_pending_item_prefers_leaf_priority_over_lexicographic_order() {
        let (store, root) = make_test_store();
        let runtime = sample_runtime();
        let low_dep_id = touch_fake_provenance(&store, &"1".repeat(64));

        let dep_a = touch_fake_provenance(&store, &"a".repeat(64));
        let dep_b = touch_fake_provenance(&store, &"b".repeat(64));
        let dep_c = touch_fake_provenance(&store, &"c".repeat(64));

        let high_action = ActionSpec::AigStatDiff {
            opt_ir_action_id: dep_a.clone(),
            g8r_aig_stats_action_id: dep_b.clone(),
            yosys_abc_aig_stats_action_id: dep_c.clone(),
        };
        let high_action_id = crate::executor::compute_action_id(&high_action).expect("high id");

        let mut chosen_low_action = None;
        let mut chosen_low_id = None;
        for i in 0..1024_u32 {
            let candidate = ActionSpec::DriverDslxFnToIr {
                dslx_subtree_action_id: low_dep_id.clone(),
                dslx_file: format!("xls/dslx/stdlib/f{i}.x"),
                dslx_fn_name: format!("fn_{i}"),
                version: "v0.35.0".to_string(),
                runtime: runtime.clone(),
            };
            let candidate_id =
                crate::executor::compute_action_id(&candidate).expect("candidate low id");
            if candidate_id < high_action_id {
                chosen_low_action = Some(candidate);
                chosen_low_id = Some(candidate_id);
                break;
            }
        }
        let low_action = chosen_low_action.expect("find low-priority action ordered before high");
        let low_action_id = chosen_low_id.expect("low action id");

        // Ensure the low-priority action sorts earlier lexicographically than the high-priority one.
        // This guarantees the test checks scheduler priority, not path order.
        assert!(low_action_id < high_action_id);

        enqueue_action(&store, low_action).expect("enqueue low action");
        enqueue_action(&store, high_action).expect("enqueue high action");

        let claimed = claim_next_pending_item(&store, "worker-priority", 60)
            .expect("claim next pending")
            .expect("expected claimed item");
        assert_eq!(claimed.action_id(), high_action_id);

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn action_scheduler_priority_prefers_leaf_actions() {
        let runtime = sample_runtime();
        let yosys_runtime = crate::model::YosysRuntimeSpec {
            docker_image: "xlsynth-bvc-yosys:latest".to_string(),
            dockerfile: "docker/yosys-abc.Dockerfile".to_string(),
            docker_image_id: "e".repeat(64),
            dockerfile_sha256: "d".repeat(64),
            upstream_commit: Some(crate::DEFAULT_YOSYS_UPSTREAM_COMMIT.to_string()),
        };
        let diff = ActionSpec::AigStatDiff {
            opt_ir_action_id: "a".repeat(64),
            g8r_aig_stats_action_id: "b".repeat(64),
            yosys_abc_aig_stats_action_id: "c".repeat(64),
        };
        let stats = ActionSpec::DriverAigToStats {
            aig_action_id: "d".repeat(64),
            version: "v0.35.0".to_string(),
            runtime: runtime.clone(),
        };
        let yabc = ActionSpec::ComboVerilogToYosysAbcAig {
            verilog_action_id: "e".repeat(64),
            verilog_top_module_name: Some("__top".to_string()),
            yosys_script_ref: crate::model::ScriptRef {
                path: "flows/yosys_to_aig.ys".to_string(),
                sha256: "f".repeat(64),
            },
            runtime: yosys_runtime,
        };
        let combo = ActionSpec::IrFnToCombinationalVerilog {
            ir_action_id: "1".repeat(64),
            top_fn_name: Some("__top".to_string()),
            use_system_verilog: false,
            version: "v0.35.0".to_string(),
            runtime: runtime.clone(),
        };
        let ir_aig_equiv = ActionSpec::DriverIrAigEquiv {
            ir_action_id: "9".repeat(64),
            aig_action_id: "a".repeat(64),
            top_fn_name: Some("__top".to_string()),
            version: "v0.35.0".to_string(),
            runtime: runtime.clone(),
        };
        let ir_equiv = ActionSpec::DriverIrEquiv {
            lhs_ir_action_id: "b".repeat(64),
            rhs_ir_action_id: "c".repeat(64),
            top_fn_name: Some("__top".to_string()),
            version: "v0.35.0".to_string(),
            runtime: runtime.clone(),
        };
        let opt = ActionSpec::DriverIrToOpt {
            ir_action_id: "2".repeat(64),
            top_fn_name: Some("__top".to_string()),
            version: "v0.35.0".to_string(),
            runtime,
        };

        assert!(action_scheduler_priority(&diff) < action_scheduler_priority(&stats));
        assert!(action_scheduler_priority(&stats) < action_scheduler_priority(&yabc));
        assert!(action_scheduler_priority(&yabc) < action_scheduler_priority(&combo));
        assert!(action_scheduler_priority(&combo) < action_scheduler_priority(&ir_aig_equiv));
        assert!(action_scheduler_priority(&ir_aig_equiv) < action_scheduler_priority(&opt));
        assert!(action_scheduler_priority(&opt) < action_scheduler_priority(&ir_equiv));
        assert!(action_scheduler_priority(&combo) < action_scheduler_priority(&opt));
    }

    #[test]
    fn suggested_action_queue_priority_grows_toward_leaf_actions() {
        let runtime = sample_runtime();
        let base_priority = 5;

        let opt = ActionSpec::DriverIrToOpt {
            ir_action_id: "2".repeat(64),
            top_fn_name: Some("__top".to_string()),
            version: "v0.35.0".to_string(),
            runtime: runtime.clone(),
        };
        let g8r = ActionSpec::DriverIrToG8rAig {
            ir_action_id: "3".repeat(64),
            top_fn_name: Some("__top".to_string()),
            fraig: false,
            lowering_mode: G8rLoweringMode::Default,
            version: "v0.35.0".to_string(),
            runtime: runtime.clone(),
        };
        let stats = ActionSpec::DriverAigToStats {
            aig_action_id: "4".repeat(64),
            version: "v0.35.0".to_string(),
            runtime,
        };
        let diff = ActionSpec::AigStatDiff {
            opt_ir_action_id: "5".repeat(64),
            g8r_aig_stats_action_id: "6".repeat(64),
            yosys_abc_aig_stats_action_id: "7".repeat(64),
        };

        assert!(suggested_action_queue_priority(base_priority, &opt) > base_priority);
        assert!(
            suggested_action_queue_priority(base_priority, &g8r)
                > suggested_action_queue_priority(base_priority, &opt)
        );
        assert!(
            suggested_action_queue_priority(base_priority, &stats)
                > suggested_action_queue_priority(base_priority, &g8r)
        );
        assert!(
            suggested_action_queue_priority(base_priority, &diff)
                > suggested_action_queue_priority(base_priority, &stats)
        );
    }

    #[test]
    fn repair_corrupt_queue_records_detects_corrupt_running_protobuf() {
        let (store, root) = make_test_store();
        let action_id = "a".repeat(64);
        let running_path = store.running_queue_path(&action_id);
        if let Some(parent) = running_path.parent() {
            fs::create_dir_all(parent).expect("create running queue shard");
        }
        fs::write(&running_path, "").expect("write corrupt running queue file");

        let summary =
            repair_corrupt_queue_records(&store, false).expect("scan corrupt queue records");
        assert_eq!(summary.scanned_files, 1);
        assert_eq!(summary.corrupt_count, 1);
        assert_eq!(summary.removed_count, 0);
        assert_eq!(summary.corrupt_records.len(), 1);
        assert_eq!(summary.corrupt_records[0].queue_state, "running");
        assert!(
            summary.corrupt_records[0]
                .reason
                .contains("parsing running queue record")
        );
        assert!(running_path.exists());

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn repair_corrupt_queue_records_apply_removes_corrupt_files() {
        let (store, root) = make_test_store();
        let action_id = "b".repeat(64);
        let pending_path = store.pending_queue_path(&action_id);
        if let Some(parent) = pending_path.parent() {
            fs::create_dir_all(parent).expect("create pending queue shard");
        }
        fs::write(&pending_path, "{").expect("write invalid pending json");

        let summary =
            repair_corrupt_queue_records(&store, true).expect("repair corrupt queue records");
        assert_eq!(summary.scanned_files, 1);
        assert_eq!(summary.corrupt_count, 1);
        assert_eq!(summary.removed_count, 1);
        assert!(!pending_path.exists());

        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    fn terminal_test_action() -> ActionSpec {
        ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
            version: "v0.37.0".to_string(),
            discovery_runtime: None,
            stdlib_tarball_sha256: "11".repeat(32),
        }
    }

    #[test]
    fn reclaim_keeps_failed_action_terminal_instead_of_requeueing() {
        let (store, root) = make_test_store();
        let action_id = enqueue_action(&store, terminal_test_action()).expect("enqueue");
        let running = claim_next_pending_item(&store, "worker-failed", 900)
            .expect("claim")
            .expect("running");
        write_failed_record(&store, &running, "worker-failed", "boom").expect("write failed");

        assert!(running.path.exists());
        assert_eq!(
            queue_state_for_action(&store, &action_id),
            QueueState::Failed
        );
        assert_eq!(reclaim_expired_running_leases(&store).expect("reclaim"), 1);
        assert!(!running.path.exists());
        assert!(!store.pending_queue_path(&action_id).exists());
        assert!(store.failed_action_record_exists(&action_id));

        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn failed_transition_reports_loss_when_success_already_exists() {
        let (store, root) = make_test_store();
        let action_id = write_completed_provenance(&store, &"ab".repeat(32));
        let provenance = store.load_provenance(&action_id).expect("load success");
        let now = Utc::now();
        let running = QueueRunningWithPath {
            running: QueueRunning {
                schema_version: crate::ACTION_SCHEMA_VERSION,
                action_id: action_id.clone(),
                enqueued_utc: now,
                priority: 0,
                action: provenance.action,
                lease_owner: "duplicate-worker".to_string(),
                lease_acquired_utc: now,
                lease_expires_utc: now,
            },
            path: store.running_queue_path(&action_id),
        };

        assert!(
            !write_failed_record(&store, &running, "duplicate-worker", "late failure")
                .expect("write losing failure")
        );
        assert!(!store.failed_action_record_exists(&action_id));
        assert!(store.action_exists(&action_id));

        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn reclaim_finishes_interrupted_terminal_first_cancellation() {
        let (store, root) = make_test_store();
        let action = terminal_test_action();
        let action_id = enqueue_action(&store, action.clone()).expect("enqueue");
        let pending_path = store.pending_queue_path(&action_id);
        let running_path = store.running_queue_path(&action_id);
        fs::create_dir_all(running_path.parent().expect("running parent"))
            .expect("create running parent");
        fs::hard_link(&pending_path, &running_path).expect("reserve cancellation");
        write_canceled_record(
            &store,
            &action_id,
            Utc::now(),
            action,
            "worker-cancel",
            &action_id,
            &action_id,
            "test cancellation",
        )
        .expect("write cancellation");

        assert_eq!(
            queue_state_for_action(&store, &action_id),
            QueueState::Canceled
        );
        assert!(pending_path.exists());
        assert!(running_path.exists());
        assert_eq!(reclaim_expired_running_leases(&store).expect("reclaim"), 1);
        assert!(!pending_path.exists());
        assert!(!running_path.exists());
        assert!(store.canceled_queue_path(&action_id).exists());

        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn claim_discards_pending_record_when_terminal_evidence_exists() {
        let (store, root) = make_test_store();
        let action = terminal_test_action();
        let action_id = enqueue_action(&store, action.clone()).expect("enqueue");
        write_canceled_record(
            &store,
            &action_id,
            Utc::now(),
            action,
            "worker-cancel",
            &action_id,
            &action_id,
            "test cancellation",
        )
        .expect("write cancellation");

        assert!(
            claim_next_pending_item(&store, "worker-claim", 60)
                .expect("claim scan")
                .is_none()
        );
        assert!(!store.pending_queue_path(&action_id).exists());
        assert_eq!(
            queue_state_for_action(&store, &action_id),
            QueueState::Canceled
        );

        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn running_owner_process_missing_on_local_host_true_for_missing_local_pid() {
        let local_host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".to_string());
        let owner = format!("{local_host}:999999:web-runner-0");
        assert!(running_owner_process_missing_on_local_host(
            &owner,
            &local_host
        ));
    }

    #[test]
    fn expired_lease_is_not_reclaimed_from_live_local_process() {
        let (store, root) = make_test_store();
        let action = ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
            version: "v0.37.0".to_string(),
            discovery_runtime: None,
            stdlib_tarball_sha256: "11".repeat(32),
        };
        let action_id = enqueue_action(&store, action).expect("enqueue action");
        let local_host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".to_string());
        let pid = std::process::id();
        let start_ticks = queue_process_start_ticks(pid).expect("process start ticks");
        let owner = format!("{local_host}:{pid}:{start_ticks}:test-worker");
        let claimed = claim_next_pending_item(&store, &owner, -1)
            .expect("claim pending action")
            .expect("action should be claimed");
        assert!(claimed.running.lease_expires_utc < Utc::now());

        assert_eq!(reclaim_expired_running_leases(&store).expect("reclaim"), 0);
        assert!(store.running_queue_path(&action_id).exists());
        assert!(!store.pending_queue_path(&action_id).exists());

        drop(store);
        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn reclaims_unexpired_lease_from_dead_local_coordinator_process() {
        let (store, root) = make_test_store();
        let action = ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
            version: "v0.37.0".to_string(),
            discovery_runtime: None,
            stdlib_tarball_sha256: "11".repeat(32),
        };
        let action_id = enqueue_action(&store, action).expect("enqueue action");
        let local_host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".to_string());
        let dead_pid = u32::MAX;
        assert!(!Path::new("/proc").join(dead_pid.to_string()).exists());
        let owner = format!("{local_host}:{dead_pid}:campaign:test-run:runner-0");
        let claimed = claim_next_pending_item(&store, &owner, 900)
            .expect("claim pending action")
            .expect("action should be claimed");
        assert!(claimed.running.lease_expires_utc > Utc::now());
        assert!(store.running_queue_path(&action_id).exists());

        assert_eq!(
            reclaim_expired_running_leases(&store).expect("reclaim dead coordinator lease"),
            1
        );
        assert!(!store.running_queue_path(&action_id).exists());
        assert!(store.pending_queue_path(&action_id).exists());

        drop(store);
        fs::remove_dir_all(root).expect("cleanup temp store");
    }

    #[test]
    fn running_owner_process_missing_on_local_host_false_for_other_host() {
        let local_host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".to_string());
        let owner = "other-host:999999:web-runner-0";
        assert!(!running_owner_process_missing_on_local_host(
            owner,
            &local_host
        ));
    }

    #[test]
    fn claim_next_pending_item_quarantines_corrupt_pending_record() {
        let (store, root) = make_test_store();
        let action_id = "c".repeat(64);
        let pending_path = store.pending_queue_path(&action_id);
        if let Some(parent) = pending_path.parent() {
            fs::create_dir_all(parent).expect("create pending queue shard");
        }
        fs::write(&pending_path, "{").expect("write invalid pending json");

        let claimed =
            claim_next_pending_item(&store, "worker-corrupt", 60).expect("claim should not fail");
        assert!(
            claimed.is_none(),
            "no valid pending item should be claimable"
        );
        assert!(!pending_path.exists(), "corrupt .json should be removed");

        let parent = pending_path.parent().expect("pending path parent");
        let mut quarantined = Vec::new();
        for entry in fs::read_dir(parent).expect("read pending shard dir") {
            let entry = entry.expect("read entry");
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if file_name.starts_with(&action_id) && file_name.ends_with(".bad") {
                quarantined.push(entry.path());
            }
        }
        assert_eq!(
            quarantined.len(),
            1,
            "expected exactly one quarantined bad queue record"
        );

        fs::remove_dir_all(root).expect("cleanup temp store");
    }
}
