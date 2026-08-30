// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, bail};
use chrono::Utc;
use fs2::FileExt;
use prost::Message;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};

use crate::analysis::{analyze_campaign_run, select_analysis_baseline};
use crate::campaign::{
    compute_campaign_id, finalize_stored_campaign_run, list_campaign_runs, load_campaign_run_by_id,
    load_default_campaign, persist_campaign_run_plan, reconcile_stored_campaign_run,
    summarize_campaign_run,
};
use crate::ops::run_workers;
use crate::proto::v1 as pb;
use crate::proto::{encode_provenance, timestamp_from_proto, timestamp_to_proto};
use crate::publish::{publish_static_site_with_protected_roots, verify_published_site};
use crate::query::rebuild_web_indices;
use crate::service::{
    check_ir_fn_corpus_structural_freshness, default_worker_id,
    populate_ir_fn_corpus_structural_index,
};
use crate::site::{
    BuildStaticSiteOptions, build_static_site_with_protected_roots, verify_static_site,
};
use crate::snapshot::{BuildStaticSnapshotOptions, build_static_snapshot, verify_static_snapshot};
use crate::store::ArtifactStore;
use crate::versioning::normalize_tag_version;
use crate::{
    DEFAULT_QUEUE_LEASE_SECONDS, DEFAULT_WEB_RUNNER_DRAIN_BATCH_SIZE,
    DEFAULT_WEB_RUNNER_POLL_MILLIS,
};

const COORDINATOR_RECORD_VERSION: u32 = 2;
const INDEXED_SOURCE_FINGERPRINT_DOMAIN: &[u8] = b"xlsynth-bvc/indexed-source/v1\0";
const INDEXED_OUTPUT_FINGERPRINT_DOMAIN: &[u8] = b"xlsynth-bvc/indexed-output/v1\0";
pub(crate) const COORDINATOR_LOCK_FILENAME: &str = "coordinator.lock";

#[derive(Debug, Clone)]
pub(crate) struct CoordinateReleaseOptions {
    pub(crate) crate_version: String,
    pub(crate) run_id: Option<String>,
    pub(crate) baseline_run_id: Option<String>,
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

pub(crate) fn coordinator_state_path(store: &ArtifactStore, run_id: &str) -> PathBuf {
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
    if let Some(fingerprint) = &state.indexed_source_fingerprint {
        digest_hex(fingerprint, "coordinator.indexed_source_fingerprint")?;
    }
    if let Some(fingerprint) = &state.indexed_output_fingerprint {
        digest_hex(fingerprint, "coordinator.indexed_output_fingerprint")?;
    }
    match (&state.baseline_run_id, &state.baseline_crate_version) {
        (Some(run_id), Some(version)) => {
            digest_hex(run_id, "coordinator.baseline_run_id")?;
            if version.value.is_empty() {
                bail!("coordinator baseline crate version must not be empty");
            }
        }
        (None, None) => {}
        _ => bail!("coordinator baseline run id and crate version must be present together"),
    }
    Ok(())
}

pub(crate) fn decode_coordinator_state(bytes: &[u8]) -> Result<pb::CoordinatorState> {
    let state = pb::CoordinatorState::decode(bytes).context("decoding CoordinatorState")?;
    validate_state(&state)?;
    Ok(state)
}

fn atomic_write_state(
    store: &ArtifactStore,
    path: &Path,
    state: &pb::CoordinatorState,
) -> Result<()> {
    validate_state(state)?;
    store.write_record_atomic("coordinator", path, &state.encode_to_vec())
}

fn load_or_new_state(
    path: &Path,
    run_id: &str,
    crate_version: &str,
    baseline_run_id: Option<&str>,
    baseline_crate_version: Option<&str>,
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
        let stored_baseline_run_id = state
            .baseline_run_id
            .as_ref()
            .map(|id| digest_hex(id, "coordinator.baseline_run_id"))
            .transpose()?;
        let stored_baseline_version = state
            .baseline_crate_version
            .as_ref()
            .map(|version| version.value.as_str());
        if stored_baseline_run_id.as_deref() != baseline_run_id
            || stored_baseline_version != baseline_crate_version
        {
            bail!(
                "coordinator state baseline binding does not match the selected baseline; resume with --baseline-run-id {}",
                stored_baseline_run_id.as_deref().unwrap_or("<none>")
            );
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
        indexed_source_fingerprint: None,
        indexed_output_fingerprint: None,
        baseline_run_id: baseline_run_id
            .map(|run_id| digest_from_hex(run_id, "baseline_run_id"))
            .transpose()?,
        baseline_crate_version: baseline_crate_version.map(|version| pb::CrateVersion {
            value: version.to_string(),
        }),
    })
}

fn checkpointed_baseline_binding(
    path: &Path,
    requested_baseline_run_id: Option<&str>,
    requested_baseline_crate_version: Option<&str>,
) -> Result<Option<Option<(String, String)>>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes =
        fs::read(path).with_context(|| format!("reading coordinator state: {}", path.display()))?;
    let state = decode_coordinator_state(&bytes)
        .with_context(|| format!("validating coordinator state: {}", path.display()))?;
    let stored_run_id = state
        .baseline_run_id
        .as_ref()
        .map(|id| digest_hex(id, "coordinator.baseline_run_id"))
        .transpose()?;
    let stored_version = state
        .baseline_crate_version
        .as_ref()
        .map(|version| version.value.clone());
    if requested_baseline_run_id
        .is_some_and(|requested| stored_run_id.as_deref() != Some(requested))
        || requested_baseline_crate_version.is_some_and(|requested| {
            stored_version.as_deref() != Some(normalize_tag_version(requested))
        })
    {
        bail!(
            "requested baseline does not match the coordinator checkpoint binding ({})",
            stored_run_id.as_deref().unwrap_or("no baseline")
        );
    }
    Ok(Some(stored_run_id.zip(stored_version)))
}

fn record_stage(
    store: &ArtifactStore,
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
    atomic_write_state(store, path, state)
}

fn stage<T>(
    store: &ArtifactStore,
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
                store,
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
                store,
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

fn indexed_source_fingerprint(store: &ArtifactStore) -> Result<pb::Sha256Digest> {
    let mut records = store
        .list_provenances()?
        .into_iter()
        .map(|provenance| {
            let action_id = provenance.action_id.clone();
            Ok((action_id, encode_provenance(&provenance)?))
        })
        .collect::<Result<Vec<_>>>()?;
    records.sort_by(|a, b| a.0.cmp(&b.0));

    let mut hasher = Sha256::new();
    hasher.update(INDEXED_SOURCE_FINGERPRINT_DOMAIN);
    for (action_id, bytes) in records {
        hasher.update((action_id.len() as u64).to_be_bytes());
        hasher.update(action_id.as_bytes());
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    Ok(pb::Sha256Digest {
        value: hasher.finalize().to_vec(),
    })
}

fn indexed_output_fingerprint(store: &ArtifactStore) -> Result<pb::Sha256Digest> {
    let mut entries = store.list_web_index_entries_with_prefix("")?;
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let mut hasher = Sha256::new();
    hasher.update(INDEXED_OUTPUT_FINGERPRINT_DOMAIN);
    for (index_key, bytes) in entries {
        hasher.update((index_key.len() as u64).to_be_bytes());
        hasher.update(index_key.as_bytes());
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    Ok(pb::Sha256Digest {
        value: hasher.finalize().to_vec(),
    })
}

fn indexed_checkpoint_matches(
    state: &pb::CoordinatorState,
    source_fingerprint: &pb::Sha256Digest,
    output_fingerprint: &pb::Sha256Digest,
) -> bool {
    stage_succeeded(state, pb::CoordinatorStage::Indexed)
        && state.indexed_source_fingerprint.as_ref() == Some(source_fingerprint)
        && state.indexed_output_fingerprint.as_ref() == Some(output_fingerprint)
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

fn coordinator_worker_id_prefix(run_id: &str) -> String {
    format!("{}:campaign:{}", default_worker_id(), &run_id[..12])
}

fn select_or_plan_campaign_run(
    store: &ArtifactStore,
    repo_root: &Path,
    crate_version: &str,
    exact_run_id: Option<&str>,
) -> Result<pb::CampaignRunManifest> {
    let requested_version = normalize_tag_version(crate_version);
    let campaign_id = compute_campaign_id(&load_default_campaign()?)?;
    if let Some(run_id) = exact_run_id {
        let manifest = load_campaign_run_by_id(store, run_id)?;
        if manifest.campaign_id.as_ref() != Some(&campaign_id)
            || manifest
                .crate_version
                .as_ref()
                .is_none_or(|version| version.value != requested_version)
        {
            bail!(
                "stored campaign run {run_id} does not match the current campaign and requested crate version {requested_version}"
            );
        }
        return Ok(manifest);
    }

    let candidates = list_campaign_runs(store)?
        .into_iter()
        .filter(|manifest| {
            manifest.campaign_id.as_ref() == Some(&campaign_id)
                && manifest
                    .crate_version
                    .as_ref()
                    .is_some_and(|version| version.value == requested_version)
        })
        .collect::<Vec<_>>();
    let mut resumable = candidates
        .into_iter()
        .filter(|manifest| {
            pb::CampaignRunStatus::try_from(manifest.status)
                .is_ok_and(|status| status == pb::CampaignRunStatus::Building)
        })
        .collect::<Vec<_>>();
    if resumable.len() > 1 {
        let ids = resumable
            .iter()
            .map(|manifest| {
                digest_hex(
                    required(&manifest.run_id, "campaign_run.run_id")?,
                    "campaign_run.run_id",
                )
            })
            .collect::<Result<Vec<_>>>()?;
        bail!(
            "multiple in-progress campaign runs match crate version {requested_version}; resume one explicitly with --run-id: {}",
            ids.join(", ")
        );
    }
    if let Some(manifest) = resumable.pop() {
        return Ok(manifest);
    }

    // Finalized generations are history. Bind today's declared inputs and select
    // their exact run ID (reusing it if already present) instead of choosing an
    // arbitrary old generation by crate-version label alone.
    persist_campaign_run_plan(store, repo_root, requested_version)
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
    let manifest = select_or_plan_campaign_run(
        &store,
        repo_root,
        &options.crate_version,
        options.run_id.as_deref(),
    )?;
    let plan = summarize_campaign_run(&store, &manifest, true)?;
    let path = coordinator_state_path(&store, &plan.run_id);
    let baseline = match checkpointed_baseline_binding(
        &path,
        options.baseline_run_id.as_deref(),
        options.baseline_crate_version.as_deref(),
    )? {
        Some(Some((run_id, version))) => {
            select_analysis_baseline(&store, &manifest, Some(&run_id), Some(&version))?
        }
        Some(None) => None,
        None => select_analysis_baseline(
            &store,
            &manifest,
            options.baseline_run_id.as_deref(),
            options.baseline_crate_version.as_deref(),
        )?,
    };
    let baseline_run_id = baseline
        .as_ref()
        .and_then(|manifest| manifest.run_id.as_ref())
        .map(|id| digest_hex(id, "baseline_run_id"))
        .transpose()?;
    let baseline_crate_version = baseline
        .as_ref()
        .and_then(|manifest| manifest.crate_version.as_ref())
        .map(|version| version.value.clone());
    let mut state = load_or_new_state(
        &path,
        &plan.run_id,
        &plan.crate_version,
        baseline_run_id.as_deref(),
        baseline_crate_version.as_deref(),
    )?;
    record_stage(
        &store,
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
        &store,
        &mut state,
        &path,
        pb::CoordinatorStage::Reconciled,
        pb::CoordinatorStageStatus::FailedTransient,
        || {
            let summary =
                reconcile_stored_campaign_run(&store, repo_root, &manifest, options.priority)?;
            let text = format!(
                "status={} pending={} running={}",
                summary.status, summary.pending_count, summary.running_count
            );
            Ok((summary, text))
        },
    )?;

    let store = Arc::new(store);
    let worker_id = coordinator_worker_id_prefix(&plan.run_id);
    let workers = stage(
        &store,
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

    let current_indexed_source_fingerprint = indexed_source_fingerprint(&store)?;
    let current_indexed_source_fingerprint_hex = digest_hex(
        &current_indexed_source_fingerprint,
        "coordinator.indexed_source_fingerprint",
    )?;
    let current_indexed_output_fingerprint = indexed_output_fingerprint(&store)?;
    let current_indexed_output_fingerprint_hex = digest_hex(
        &current_indexed_output_fingerprint,
        "coordinator.indexed_output_fingerprint",
    )?;
    let indexed_already_succeeded = indexed_checkpoint_matches(
        &state,
        &current_indexed_source_fingerprint,
        &current_indexed_output_fingerprint,
    );
    let verified_indexed_output_fingerprint = stage(
        &store,
        &mut state,
        &path,
        pb::CoordinatorStage::Indexed,
        pb::CoordinatorStageStatus::FailedTransient,
        || {
            if indexed_already_succeeded {
                Ok((
                    current_indexed_output_fingerprint.clone(),
                    format!(
                        "reused previously verified web/publication datasets source_fingerprint={current_indexed_source_fingerprint_hex} output_fingerprint={current_indexed_output_fingerprint_hex}"
                    ),
                ))
            } else {
                let structural_index = ensure_structural_index_current(
                    &store,
                    repo_root,
                    &options.work_dir,
                    options.workers,
                )?;
                rebuild_web_indices(&store, repo_root)?;
                store
                    .flush_durable()
                    .context("durably flushing rebuilt web/publication datasets")?;
                let output_fingerprint = indexed_output_fingerprint(&store)?;
                let output_fingerprint_hex = digest_hex(
                    &output_fingerprint,
                    "coordinator.indexed_output_fingerprint",
                )?;
                Ok((
                    output_fingerprint,
                    format!(
                        "{}; rebuilt all declared web/publication datasets source_fingerprint={} output_fingerprint={}",
                        structural_index,
                        current_indexed_source_fingerprint_hex,
                        output_fingerprint_hex,
                    ),
                ))
            }
        },
    )?;
    state.indexed_source_fingerprint = Some(current_indexed_source_fingerprint);
    state.indexed_output_fingerprint = Some(verified_indexed_output_fingerprint);
    atomic_write_state(&store, &path, &state)?;

    let finalized = stage(
        &store,
        &mut state,
        &path,
        pb::CoordinatorStage::Finalized,
        pb::CoordinatorStageStatus::FailedDeterministic,
        || {
            let current = load_campaign_run_by_id(&store, &plan.run_id)?;
            let summary = finalize_stored_campaign_run(&store, &current)?;
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
        &store,
        &mut state,
        &path,
        pb::CoordinatorStage::Analyzed,
        pb::CoordinatorStageStatus::FailedDeterministic,
        || {
            let summary = analyze_campaign_run(
                &store,
                repo_root,
                &options.crate_version,
                Some(&plan.run_id),
                baseline_run_id.as_deref(),
                baseline_crate_version.as_deref(),
            )?;
            let text = format!("findings={}", summary.finding_count);
            Ok((summary, text))
        },
    )?;

    let snapshot_dir = options.work_dir.join("snapshots").join(&plan.run_id);
    state.snapshot_dir = snapshot_dir.display().to_string();
    let snapshot = stage(
        &store,
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
        &store,
        &mut state,
        &path,
        pb::CoordinatorStage::SiteVerified,
        pb::CoordinatorStageStatus::FailedTransient,
        || {
            build_static_site_with_protected_roots(
                &BuildStaticSiteOptions {
                    snapshot_dir: snapshot_dir.clone(),
                    out_dir: site_dir.clone(),
                    base_url: options.base_url.clone(),
                    overwrite: true,
                },
                &[
                    ("resource checkout", repo_root),
                    ("private store", store.root.as_path()),
                    (
                        "artifact backend storage",
                        store.artifact_backend_storage_path(),
                    ),
                ],
            )?;
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
            &store,
            &mut state,
            &path,
            pb::CoordinatorStage::Published,
            pb::CoordinatorStageStatus::FailedTransient,
            || {
                let summary = publish_static_site_with_protected_roots(
                    &site_dir,
                    publish_root,
                    &[
                        ("resource checkout", repo_root),
                        ("private store", store.root.as_path()),
                        (
                            "artifact backend storage",
                            store.artifact_backend_storage_path(),
                        ),
                        ("coordinator work directory", options.work_dir.as_path()),
                    ],
                )?;
                verify_published_site(publish_root)?;
                let text = format!(
                    "site_id={} relpath={} reused={}",
                    summary.site_id, summary.site_relpath, summary.reused_immutable_site
                );
                Ok((summary, text))
            },
        )?;
        state.published_site_id = Some(digest_from_hex(&published.site_id, "published_site_id")?);
        atomic_write_state(&store, &path, &state)?;
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

    fn make_mutable_resource_root(label: &str) -> PathBuf {
        let source_root = std::env::current_dir().expect("current dir");
        let root = temp_path(label);
        for relpath in [
            crate::VERSION_COMPAT_PATH,
            crate::DEFAULT_DOCKERFILE,
            crate::VENDORED_DOWNLOAD_RELEASE_SCRIPT,
        ] {
            let target = root.join(relpath);
            fs::create_dir_all(target.parent().expect("resource parent"))
                .expect("create resource parent");
            fs::copy(source_root.join(relpath), &target).expect("copy resource input");
        }
        root
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
    fn coordinator_worker_prefix_retains_host_and_process_identity() {
        let run_id = "a".repeat(64);
        let prefix = coordinator_worker_id_prefix(&run_id);
        assert_eq!(
            prefix,
            format!("{}:campaign:{}", default_worker_id(), &run_id[..12])
        );
    }

    #[test]
    fn stored_campaign_run_is_selected_before_live_planning_inputs() {
        let root = temp_path("stored-resume");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let repo_root = std::env::current_dir().expect("current repo root");
        let version = crate::versioning::load_version_compat_map(&repo_root)
            .expect("version map")
            .into_keys()
            .next()
            .expect("known version");
        let persisted = persist_campaign_run_plan(&store, &repo_root, &version)
            .expect("persist fully bound plan");
        let expected_run_id = digest_hex(
            required(&persisted.run_id, "campaign_run.run_id").expect("run id"),
            "campaign_run.run_id",
        )
        .expect("run id hex");

        let unavailable_resource_root = root.join("resource-root-is-offline");
        let selected =
            select_or_plan_campaign_run(&store, &unavailable_resource_root, &version, None)
                .expect("resume without live resource inputs");
        assert_eq!(selected.run_id, persisted.run_id);
        let explicit = select_or_plan_campaign_run(
            &store,
            &unavailable_resource_root,
            &version,
            Some(&expected_run_id),
        )
        .expect("explicit offline resume");
        assert_eq!(explicit.run_id, persisted.run_id);

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn finalized_history_does_not_hide_current_runtime_generation() {
        let root = temp_path("current-generation-store");
        let repo_root = make_mutable_resource_root("current-generation-resources");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let version = crate::versioning::load_version_compat_map(&repo_root)
            .expect("version map")
            .into_keys()
            .next()
            .expect("known version");
        let mut old = persist_campaign_run_plan(&store, &repo_root, &version)
            .expect("persist old generation");
        old.status = pb::CampaignRunStatus::Complete as i32;
        old.completion = Some(pb::CompletionReport {
            status: pb::CampaignRunStatus::Complete as i32,
            root_action_count: old.root_actions.len() as u64,
            completed_root_count: old.root_actions.len() as u64,
            ..Default::default()
        });
        let old_summary = summarize_campaign_run(&store, &old, true).expect("old summary");
        fs::write(&old_summary.manifest_path, old.encode_to_vec())
            .expect("finalize old generation fixture");

        let setup_script = repo_root.join(crate::VENDORED_DOWNLOAD_RELEASE_SCRIPT);
        let mut bytes = fs::read(&setup_script).expect("read setup script");
        bytes.extend_from_slice(b"\n# next runtime generation\n");
        fs::write(&setup_script, bytes).expect("change runtime input");

        let selected = select_or_plan_campaign_run(&store, &repo_root, &version, None)
            .expect("select current generation");
        assert_ne!(selected.run_id, old.run_id);
        assert_eq!(
            pb::CampaignRunStatus::try_from(selected.status).expect("selected status"),
            pb::CampaignRunStatus::Building
        );
        assert_eq!(
            list_campaign_runs(&store).expect("campaign history").len(),
            2
        );

        drop(store);
        fs::remove_dir_all(root).expect("cleanup store");
        fs::remove_dir_all(repo_root).expect("cleanup resources");
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
    fn coordinator_checkpoint_rejects_baseline_generation_drift() {
        let root = temp_path("baseline-binding");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let run_id = "1".repeat(64);
        let baseline_run_id = "2".repeat(64);
        let path = coordinator_state_path(&store, &run_id);
        let state = load_or_new_state(
            &path,
            &run_id,
            "0.40.0",
            Some(&baseline_run_id),
            Some("0.39.0"),
        )
        .expect("new bound state");
        atomic_write_state(&store, &path, &state).expect("persist bound state");

        load_or_new_state(
            &path,
            &run_id,
            "0.40.0",
            Some(&baseline_run_id),
            Some("0.39.0"),
        )
        .expect("same baseline resumes");
        assert_eq!(
            checkpointed_baseline_binding(&path, None, None).expect("read checkpointed baseline"),
            Some(Some((baseline_run_id.clone(), "0.39.0".to_string())))
        );
        let error = load_or_new_state(
            &path,
            &run_id,
            "0.40.0",
            Some(&"3".repeat(64)),
            Some("0.39.0"),
        )
        .expect_err("baseline generation drift must fail");
        assert!(format!("{error:#}").contains("baseline binding"));

        let no_baseline_run_id = "4".repeat(64);
        let no_baseline_path = coordinator_state_path(&store, &no_baseline_run_id);
        let no_baseline =
            load_or_new_state(&no_baseline_path, &no_baseline_run_id, "0.40.0", None, None)
                .expect("new no-baseline state");
        atomic_write_state(&store, &no_baseline_path, &no_baseline)
            .expect("persist no-baseline state");
        assert_eq!(
            checkpointed_baseline_binding(&no_baseline_path, None, None)
                .expect("read no-baseline checkpoint"),
            Some(None),
            "newly appearing history must not change a checkpointed no-baseline choice"
        );
        let error = checkpointed_baseline_binding(&no_baseline_path, None, Some("0.39.0"))
            .expect_err("an explicit baseline cannot replace a checkpointed no-baseline choice");
        assert!(format!("{error:#}").contains("does not match"));

        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn indexed_checkpoint_requires_exact_current_input_and_output_fingerprints() {
        let root = temp_path("indexed-source-fingerprint");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let source_before = indexed_source_fingerprint(&store).expect("empty source fingerprint");
        let output_before = indexed_output_fingerprint(&store).expect("empty output fingerprint");
        let mut state = pb::CoordinatorState {
            stage_results: vec![pb::CoordinatorStageResult {
                stage: pb::CoordinatorStage::Indexed as i32,
                status: pb::CoordinatorStageStatus::Succeeded as i32,
                ..Default::default()
            }],
            indexed_source_fingerprint: Some(source_before.clone()),
            indexed_output_fingerprint: Some(output_before.clone()),
            ..Default::default()
        };
        assert!(indexed_checkpoint_matches(
            &state,
            &source_before,
            &output_before
        ));

        let action = crate::model::ActionSpec::ImportIrPackageFile {
            source_sha256: "a".repeat(64),
            top_fn_name: Some("main".to_string()),
        };
        let action_id = crate::executor::compute_action_id(&action).expect("action id");
        let mut provenance = crate::model::Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: action_id.clone(),
            created_utc: Utc::now(),
            action,
            dependencies: Vec::new(),
            output_artifact: crate::model::ArtifactRef {
                action_id,
                artifact_type: crate::model::ArtifactType::IrPackageFile,
                relpath: "payload/input.ir".to_string(),
            },
            output_files: Vec::new(),
            commands: Vec::new(),
            details: serde_json::json!({
                "source_path": "first.ir",
                "import_kind": "local_ir_file"
            }),
            suggested_next_actions: Vec::new(),
        };
        store
            .write_provenance(&provenance)
            .expect("write newly completed action");
        let source_after_action =
            indexed_source_fingerprint(&store).expect("updated source fingerprint");
        assert_ne!(source_before, source_after_action);
        assert!(!indexed_checkpoint_matches(
            &state,
            &source_after_action,
            &output_before
        ));

        state.indexed_source_fingerprint = Some(source_after_action.clone());
        assert!(indexed_checkpoint_matches(
            &state,
            &source_after_action,
            &output_before
        ));
        provenance.details["source_path"] = serde_json::json!("refreshed.ir");
        store
            .write_provenance(&provenance)
            .expect("refresh provenance contents");
        let source_after_refresh =
            indexed_source_fingerprint(&store).expect("refreshed source fingerprint");
        assert_ne!(source_after_action, source_after_refresh);
        assert!(!indexed_checkpoint_matches(
            &state,
            &source_after_refresh,
            &output_before
        ));

        state.indexed_source_fingerprint = Some(source_after_refresh.clone());
        store
            .write_web_index_bytes("checkpoint-test.v1.json", br#"{"value":1}"#)
            .expect("write index output");
        let output_after_write =
            indexed_output_fingerprint(&store).expect("written output fingerprint");
        assert_ne!(output_before, output_after_write);
        assert!(!indexed_checkpoint_matches(
            &state,
            &source_after_refresh,
            &output_after_write
        ));

        state.indexed_output_fingerprint = Some(output_after_write.clone());
        assert!(indexed_checkpoint_matches(
            &state,
            &source_after_refresh,
            &output_after_write
        ));
        store
            .write_web_index_bytes("checkpoint-test.v1.json", br#"{"value":2}"#)
            .expect("tamper index output");
        let output_after_tamper =
            indexed_output_fingerprint(&store).expect("tampered output fingerprint");
        assert_ne!(output_after_write, output_after_tamper);
        assert!(!indexed_checkpoint_matches(
            &state,
            &source_after_refresh,
            &output_after_tamper
        ));

        store
            .delete_web_index_keys_with_prefix("checkpoint-test.v1.json")
            .expect("delete index output");
        let output_after_delete =
            indexed_output_fingerprint(&store).expect("deleted output fingerprint");
        assert_eq!(output_before, output_after_delete);
        assert!(!indexed_checkpoint_matches(
            &state,
            &source_after_refresh,
            &output_after_delete
        ));

        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
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
