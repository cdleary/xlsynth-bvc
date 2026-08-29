// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use fs2::FileExt;
use prost::Message;
use serde::Serialize;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::analysis::analyze_campaign_run;
use crate::campaign::{finalize_campaign_run, plan_campaign_run, reconcile_campaign_run};
use crate::ops::run_workers;
use crate::proto::v1 as pb;
use crate::proto::{timestamp_from_proto, timestamp_to_proto};
use crate::publish::{publish_static_site, verify_published_site};
use crate::query::rebuild_web_indices;
use crate::service::{
    check_ir_fn_corpus_structural_freshness, populate_ir_fn_corpus_structural_index,
};
use crate::site::{BuildStaticSiteOptions, build_static_site, verify_static_site};
use crate::snapshot::{BuildStaticSnapshotOptions, build_static_snapshot, verify_static_snapshot};
use crate::store::ArtifactStore;
use crate::{
    DEFAULT_QUEUE_LEASE_SECONDS, DEFAULT_WEB_RUNNER_DRAIN_BATCH_SIZE,
    DEFAULT_WEB_RUNNER_POLL_MILLIS,
};

const COORDINATOR_RECORD_VERSION: u32 = 1;
pub(crate) const COORDINATOR_LOCK_FILENAME: &str = "coordinator.lock";
static WRITE_NONCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub(crate) struct CoordinateReleaseOptions {
    pub(crate) crate_version: String,
    pub(crate) baseline_crate_version: Option<String>,
    pub(crate) work_dir: PathBuf,
    pub(crate) base_url: String,
    pub(crate) publish_root: Option<PathBuf>,
    pub(crate) workers: usize,
    pub(crate) priority: i32,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CoordinateReleaseSummary {
    pub(crate) crate_version: String,
    pub(crate) run_id: String,
    pub(crate) run_status: String,
    pub(crate) finding_count: usize,
    pub(crate) snapshot_id: String,
    pub(crate) snapshot_dir: String,
    pub(crate) site_dir: String,
    pub(crate) published_site_id: Option<String>,
    pub(crate) coordinator_state_path: String,
}

struct CoordinatorLock {
    file: File,
}

impl CoordinatorLock {
    fn acquire(store: &ArtifactStore) -> Result<Self> {
        let dir = store.coordinator_dir();
        fs::create_dir_all(&dir)
            .with_context(|| format!("creating coordinator directory: {}", dir.display()))?;
        let path = dir.join(COORDINATOR_LOCK_FILENAME);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("opening coordinator lock: {}", path.display()))?;
        file.try_lock_exclusive().with_context(|| {
            format!(
                "another xlsynth-bvc coordinator holds the machine/store lock {}",
                path.display()
            )
        })?;
        Ok(Self { file })
    }
}

impl Drop for CoordinatorLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn required<'a, T>(value: &'a Option<T>, field: &str) -> Result<&'a T> {
    value
        .as_ref()
        .with_context(|| format!("missing required protobuf field {field}"))
}

fn digest_from_hex(value: &str, field: &str) -> Result<pb::Sha256Digest> {
    let value = hex::decode(value).with_context(|| format!("decoding {field} as hex"))?;
    if value.len() != 32 {
        bail!("{field} must contain exactly 32 bytes");
    }
    Ok(pb::Sha256Digest { value })
}

fn digest_hex(value: &pb::Sha256Digest, field: &str) -> Result<String> {
    if value.value.len() != 32 {
        bail!("{field} must contain exactly 32 bytes");
    }
    Ok(hex::encode(&value.value))
}

fn state_path(store: &ArtifactStore, run_id: &str) -> PathBuf {
    store
        .coordinator_dir()
        .join(&run_id[0..2])
        .join(&run_id[2..4])
        .join(format!("{run_id}.pb"))
}

fn validate_state(state: &pb::CoordinatorState) -> Result<()> {
    if state.record_version != COORDINATOR_RECORD_VERSION {
        bail!(
            "unsupported coordinator record version {}",
            state.record_version
        );
    }
    digest_hex(
        required(&state.run_id, "coordinator.run_id")?,
        "coordinator.run_id",
    )?;
    if required(&state.crate_version, "coordinator.crate_version")?
        .value
        .is_empty()
    {
        bail!("coordinator crate version must not be empty");
    }
    let current = pb::CoordinatorStage::try_from(state.current_stage)
        .context("coordinator current stage is unknown")?;
    if current == pb::CoordinatorStage::Unspecified {
        bail!("coordinator current stage must be specified");
    }
    timestamp_from_proto(&state.updated_at, "coordinator.updated_at")?;
    let mut prior_stage = 0;
    for result in &state.stage_results {
        let stage = pb::CoordinatorStage::try_from(result.stage)
            .context("coordinator result stage is unknown")?;
        let status = pb::CoordinatorStageStatus::try_from(result.status)
            .context("coordinator result status is unknown")?;
        if stage == pb::CoordinatorStage::Unspecified
            || status == pb::CoordinatorStageStatus::Unspecified
            || result.stage <= prior_stage
        {
            bail!("coordinator stage results must be concrete and strictly sorted");
        }
        let started = timestamp_from_proto(&result.started_at, "stage.started_at")?;
        let finished = timestamp_from_proto(&result.finished_at, "stage.finished_at")?;
        if finished < started || result.summary.trim().is_empty() {
            bail!("coordinator stage result has invalid time or summary");
        }
        prior_stage = result.stage;
    }
    if let Some(site_id) = &state.published_site_id {
        digest_hex(site_id, "coordinator.published_site_id")?;
    }
    Ok(())
}

pub(crate) fn decode_coordinator_state(bytes: &[u8]) -> Result<pb::CoordinatorState> {
    let state = pb::CoordinatorState::decode(bytes).context("decoding CoordinatorState")?;
    validate_state(&state)?;
    Ok(state)
}

fn atomic_write_state(path: &Path, state: &pb::CoordinatorState) -> Result<()> {
    validate_state(state)?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("coordinator state path has no parent"))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("creating coordinator state parent: {}", parent.display()))?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let nonce = WRITE_NONCE.fetch_add(1, Ordering::Relaxed);
    let temp = parent.join(format!(
        ".coordinator.pb.tmp-{}-{timestamp}-{nonce}",
        std::process::id()
    ));
    fs::write(&temp, state.encode_to_vec())
        .with_context(|| format!("writing coordinator temp state: {}", temp.display()))?;
    fs::rename(&temp, path).with_context(|| {
        format!(
            "atomically promoting coordinator state: {} -> {}",
            temp.display(),
            path.display()
        )
    })
}

fn load_or_new_state(
    path: &Path,
    run_id: &str,
    crate_version: &str,
) -> Result<pb::CoordinatorState> {
    if path.exists() {
        let bytes = fs::read(path)
            .with_context(|| format!("reading coordinator state: {}", path.display()))?;
        let state = decode_coordinator_state(&bytes)
            .with_context(|| format!("validating coordinator state: {}", path.display()))?;
        if digest_hex(
            required(&state.run_id, "coordinator.run_id")?,
            "coordinator.run_id",
        )? != run_id
            || required(&state.crate_version, "coordinator.crate_version")?.value != crate_version
        {
            bail!("coordinator state identity does not match requested run");
        }
        return Ok(state);
    }
    Ok(pb::CoordinatorState {
        record_version: COORDINATOR_RECORD_VERSION,
        run_id: Some(digest_from_hex(run_id, "run_id")?),
        crate_version: Some(pb::CrateVersion {
            value: crate_version.to_string(),
        }),
        current_stage: pb::CoordinatorStage::Planned as i32,
        stage_results: Vec::new(),
        updated_at: Some(timestamp_to_proto(&Utc::now())),
        snapshot_dir: String::new(),
        site_dir: String::new(),
        published_site_id: None,
    })
}

fn record_stage(
    state: &mut pb::CoordinatorState,
    path: &Path,
    stage: pb::CoordinatorStage,
    status: pb::CoordinatorStageStatus,
    started: chrono::DateTime<Utc>,
    summary: String,
) -> Result<()> {
    state
        .stage_results
        .retain(|result| result.stage != stage as i32);
    state.stage_results.push(pb::CoordinatorStageResult {
        stage: stage as i32,
        status: status as i32,
        started_at: Some(timestamp_to_proto(&started)),
        finished_at: Some(timestamp_to_proto(&Utc::now())),
        summary,
    });
    state.stage_results.sort_by_key(|result| result.stage);
    state.current_stage = stage as i32;
    state.updated_at = Some(timestamp_to_proto(&Utc::now()));
    atomic_write_state(path, state)
}

fn stage<T>(
    state: &mut pb::CoordinatorState,
    path: &Path,
    stage_name: pb::CoordinatorStage,
    failure_status: pb::CoordinatorStageStatus,
    operation: impl FnOnce() -> Result<(T, String)>,
) -> Result<T> {
    let started = Utc::now();
    match operation() {
        Ok((value, summary)) => {
            record_stage(
                state,
                path,
                stage_name,
                pb::CoordinatorStageStatus::Succeeded,
                started,
                summary,
            )?;
            Ok(value)
        }
        Err(error) => {
            record_stage(
                state,
                path,
                stage_name,
                failure_status,
                started,
                format!("{error:#}"),
            )?;
            Err(error)
        }
    }
}

fn stage_succeeded(state: &pb::CoordinatorState, stage: pb::CoordinatorStage) -> bool {
    state.stage_results.iter().any(|result| {
        result.stage == stage as i32
            && result.status == pb::CoordinatorStageStatus::Succeeded as i32
    })
}

fn ensure_structural_index_current(
    store: &ArtifactStore,
    repo_root: &Path,
    work_dir: &Path,
    workers: usize,
) -> Result<String> {
    let freshness = check_ir_fn_corpus_structural_freshness(store)?;
    if freshness.up_to_date {
        return Ok("reused current structural corpus index".to_string());
    }

    let summary =
        populate_ir_fn_corpus_structural_index(store, repo_root, work_dir, false, workers)?;
    Ok(format!(
        "rebuilt structural corpus index actions={} hashes={}",
        summary.indexed_actions, summary.distinct_structural_hashes
    ))
}

pub(crate) fn coordinate_release(
    store: ArtifactStore,
    repo_root: &Path,
    options: &CoordinateReleaseOptions,
) -> Result<CoordinateReleaseSummary> {
    if options.workers == 0 {
        bail!("coordinator workers must be greater than zero");
    }
    let _lock = CoordinatorLock::acquire(&store)?;
    let plan = plan_campaign_run(&store, repo_root, &options.crate_version)?;
    let path = state_path(&store, &plan.run_id);
    let mut state = load_or_new_state(&path, &plan.run_id, &plan.crate_version)?;
    record_stage(
        &mut state,
        &path,
        pb::CoordinatorStage::Planned,
        pb::CoordinatorStageStatus::Succeeded,
        Utc::now(),
        format!(
            "run_id={} roots={} cached_roots={}",
            plan.run_id, plan.root_action_count, plan.completed_root_count
        ),
    )?;

    let reconciled = stage(
        &mut state,
        &path,
        pb::CoordinatorStage::Reconciled,
        pb::CoordinatorStageStatus::FailedTransient,
        || {
            let summary = reconcile_campaign_run(
                &store,
                repo_root,
                &options.crate_version,
                options.priority,
            )?;
            let text = format!(
                "status={} pending={} running={}",
                summary.status, summary.pending_count, summary.running_count
            );
            Ok((summary, text))
        },
    )?;

    let store = Arc::new(store);
    let worker_id = format!("campaign:{}", &plan.run_id[..12]);
    let workers = stage(
        &mut state,
        &path,
        pb::CoordinatorStage::Drained,
        pb::CoordinatorStageStatus::FailedTransient,
        || {
            let summary = run_workers(
                store.clone(),
                repo_root.to_path_buf(),
                options.workers,
                &worker_id,
                DEFAULT_QUEUE_LEASE_SECONDS,
                Duration::from_millis(DEFAULT_WEB_RUNNER_POLL_MILLIS),
                DEFAULT_WEB_RUNNER_DRAIN_BATCH_SIZE,
                true,
                true,
            )?;
            let text = format!(
                "workers={} drained_actions={} exit={}",
                summary.workers, summary.drained_actions, summary.exit_reason
            );
            Ok((summary, text))
        },
    )?;

    let indexed_already_succeeded = stage_succeeded(&state, pb::CoordinatorStage::Indexed);
    stage(
        &mut state,
        &path,
        pb::CoordinatorStage::Indexed,
        pb::CoordinatorStageStatus::FailedTransient,
        || {
            if indexed_already_succeeded {
                Ok((
                    (),
                    "reused previously verified web/publication datasets".to_string(),
                ))
            } else {
                let structural_index = ensure_structural_index_current(
                    &store,
                    repo_root,
                    &options.work_dir,
                    options.workers,
                )?;
                rebuild_web_indices(&store, repo_root)?;
                Ok((
                    (),
                    format!(
                        "{}; rebuilt all declared web/publication datasets",
                        structural_index
                    ),
                ))
            }
        },
    )?;

    let finalized = stage(
        &mut state,
        &path,
        pb::CoordinatorStage::Finalized,
        pb::CoordinatorStageStatus::FailedDeterministic,
        || {
            let summary = finalize_campaign_run(&store, repo_root, &options.crate_version)?;
            if !matches!(summary.status.as_str(), "complete" | "degraded") {
                bail!(
                    "campaign finalization refused publication: status={} missing_outputs={:?}",
                    summary.status,
                    summary.missing_outputs
                );
            }
            let text = format!(
                "status={} roots={}/{} failed_samples={}",
                summary.status,
                summary.completed_root_count,
                summary.root_action_count,
                summary.failed_samples.len()
            );
            Ok((summary, text))
        },
    )?;

    let analysis = stage(
        &mut state,
        &path,
        pb::CoordinatorStage::Analyzed,
        pb::CoordinatorStageStatus::FailedDeterministic,
        || {
            let summary = analyze_campaign_run(
                &store,
                repo_root,
                &options.crate_version,
                options.baseline_crate_version.as_deref(),
            )?;
            let text = format!("findings={}", summary.finding_count);
            Ok((summary, text))
        },
    )?;

    let snapshot_dir = options.work_dir.join("snapshots").join(&plan.run_id);
    state.snapshot_dir = snapshot_dir.display().to_string();
    let snapshot = stage(
        &mut state,
        &path,
        pb::CoordinatorStage::SnapshotVerified,
        pb::CoordinatorStageStatus::FailedTransient,
        || {
            build_static_snapshot(
                &store,
                repo_root,
                &BuildStaticSnapshotOptions {
                    out_dir: snapshot_dir.clone(),
                    overwrite: true,
                    skip_rebuild_web_indices: true,
                },
            )?;
            let summary = verify_static_snapshot(&snapshot_dir)?;
            let text = format!(
                "snapshot_id={} files={} bytes={}",
                summary.snapshot_id, summary.dataset_file_count, summary.total_dataset_bytes
            );
            Ok((summary, text))
        },
    )?;

    let site_dir = options.work_dir.join("sites").join(&plan.run_id);
    state.site_dir = site_dir.display().to_string();
    stage(
        &mut state,
        &path,
        pb::CoordinatorStage::SiteVerified,
        pb::CoordinatorStageStatus::FailedTransient,
        || {
            build_static_site(&BuildStaticSiteOptions {
                snapshot_dir: snapshot_dir.clone(),
                out_dir: site_dir.clone(),
                base_url: options.base_url.clone(),
                overwrite: true,
            })?;
            let summary = verify_static_site(&site_dir)?;
            let text = format!(
                "snapshot_id={} files={} bytes={}",
                summary.snapshot_id, summary.file_count, summary.total_bytes
            );
            Ok((summary, text))
        },
    )?;

    let published_site_id = if let Some(publish_root) = &options.publish_root {
        let published = stage(
            &mut state,
            &path,
            pb::CoordinatorStage::Published,
            pb::CoordinatorStageStatus::FailedTransient,
            || {
                let summary = publish_static_site(&site_dir, publish_root)?;
                verify_published_site(publish_root)?;
                let text = format!(
                    "site_id={} relpath={} reused={}",
                    summary.site_id, summary.site_relpath, summary.reused_immutable_site
                );
                Ok((summary, text))
            },
        )?;
        state.published_site_id = Some(digest_from_hex(&published.site_id, "published_site_id")?);
        atomic_write_state(&path, &state)?;
        Some(published.site_id)
    } else {
        None
    };

    let _ = reconciled;
    let _ = workers;
    Ok(CoordinateReleaseSummary {
        crate_version: plan.crate_version,
        run_id: plan.run_id,
        run_status: finalized.status,
        finding_count: analysis.finding_count,
        snapshot_id: snapshot.snapshot_id,
        snapshot_dir: snapshot_dir.display().to_string(),
        site_dir: site_dir.display().to_string(),
        published_site_id,
        coordinator_state_path: path.display().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "xlsynth-bvc-coordinator-{label}-{}-{nanos}",
            std::process::id()
        ))
    }

    #[test]
    fn coordinator_lock_excludes_overlap_and_recovers_on_drop() {
        let root = temp_path("lock");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let first = CoordinatorLock::acquire(&store).expect("first lock");
        let error = CoordinatorLock::acquire(&store)
            .err()
            .expect("second lock fails");
        assert!(
            error
                .to_string()
                .contains("another xlsynth-bvc coordinator")
        );
        drop(first);
        CoordinatorLock::acquire(&store).expect("lock after drop");
        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn succeeded_stage_checkpoint_is_detected() {
        let state = pb::CoordinatorState {
            stage_results: vec![pb::CoordinatorStageResult {
                stage: pb::CoordinatorStage::Indexed as i32,
                status: pb::CoordinatorStageStatus::Succeeded as i32,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(stage_succeeded(&state, pb::CoordinatorStage::Indexed));
        assert!(!stage_succeeded(
            &state,
            pb::CoordinatorStage::SnapshotVerified
        ));
    }

    #[test]
    fn structural_index_is_initialized_once_for_a_fresh_store() {
        let root = temp_path("structural-index");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");

        let first = ensure_structural_index_current(&store, &root, &root, 1)
            .expect("initialize structural index");
        assert!(first.starts_with("rebuilt structural corpus index"));
        let freshness = check_ir_fn_corpus_structural_freshness(&store)
            .expect("check initialized structural index");
        assert!(freshness.up_to_date, "{:?}", freshness.stale_reasons);

        let second = ensure_structural_index_current(&store, &root, &root, 1)
            .expect("reuse structural index");
        assert_eq!(second, "reused current structural corpus index");

        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }
}
