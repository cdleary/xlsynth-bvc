// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, bail};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use crate::analysis::{decode_analysis_report, validate_analysis_report_against_store};
use crate::campaign::{
    CAMPAIGN_ANALYSIS_FILENAME, CAMPAIGN_RUN_MANIFEST_FILENAME, campaign_analysis_path,
    campaign_run_path, load_campaign_run_file,
};
use crate::coordinator::{
    COORDINATOR_LOCK_FILENAME, coordinator_state_path, decode_coordinator_state,
};
use crate::executor::compute_action_id;
use crate::model::{QueueCanceled, QueueDone, QueueFailed, QueueItem, QueueRunning};
use crate::proto::{
    decode_queue_canceled, decode_queue_done, decode_queue_item, decode_queue_running,
};
use crate::store::ArtifactStore;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ValidateStoreSummary {
    pub(crate) provenance_records: usize,
    pub(crate) failed_records: usize,
    pub(crate) pending_records: usize,
    pub(crate) running_records: usize,
    pub(crate) done_records: usize,
    pub(crate) canceled_records: usize,
    pub(crate) campaign_run_records: usize,
    pub(crate) analysis_records: usize,
    pub(crate) coordinator_records: usize,
    pub(crate) verified_payload_files: usize,
    pub(crate) verified_payload_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "state", content = "record", rename_all = "snake_case")]
pub(crate) enum QueueRecordProjection {
    Pending(QueueItem),
    Running(QueueRunning),
    Done(QueueDone),
    Failed(QueueFailed),
    Canceled(QueueCanceled),
}

fn decode_path<T>(path: &Path, decode: impl Fn(&[u8]) -> Result<T>) -> Result<T> {
    let bytes =
        fs::read(path).with_context(|| format!("reading queue record: {}", path.display()))?;
    decode(&bytes).with_context(|| format!("validating queue record: {}", path.display()))
}

pub(crate) fn show_queue_record(
    store: &ArtifactStore,
    action_id: &str,
) -> Result<QueueRecordProjection> {
    let candidates = [
        ("pending", store.pending_queue_path(action_id)),
        ("running", store.running_queue_path(action_id)),
        ("done", store.done_queue_path(action_id)),
        ("canceled", store.canceled_queue_path(action_id)),
    ];
    let present = candidates
        .iter()
        .filter(|(_, path)| path.is_file())
        .collect::<Vec<_>>();
    let failed = store.load_failed_action_record(action_id)?;
    let state_count = present.len() + usize::from(failed.is_some());
    if state_count == 0 {
        bail!("action has no queue record: {action_id}");
    }
    if state_count != 1 {
        let states = present
            .iter()
            .map(|(state, _)| *state)
            .chain(failed.as_ref().map(|_| "failed"))
            .collect::<Vec<_>>()
            .join(", ");
        bail!("action has conflicting queue records: action_id={action_id} states={states}");
    }
    if let Some(record) = failed {
        return Ok(QueueRecordProjection::Failed(record));
    }
    let (state, path) = present[0];
    match *state {
        "pending" => Ok(QueueRecordProjection::Pending(decode_path(
            path,
            decode_queue_item,
        )?)),
        "running" => Ok(QueueRecordProjection::Running(decode_path(
            path,
            decode_queue_running,
        )?)),
        "done" => Ok(QueueRecordProjection::Done(decode_path(
            path,
            decode_queue_done,
        )?)),
        "canceled" => Ok(QueueRecordProjection::Canceled(decode_path(
            path,
            decode_queue_canceled,
        )?)),
        unexpected => unreachable!("unexpected queue state: {unexpected}"),
    }
}

fn list_regular_files(root: &Path) -> Result<Vec<std::path::PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    for entry in WalkDir::new(root).sort_by_file_name() {
        let entry = entry.with_context(|| format!("walking record tree: {}", root.display()))?;
        if entry.file_type().is_file() {
            paths.push(entry.into_path());
        } else if !entry.file_type().is_dir() {
            bail!(
                "record tree contains a symlink or special filesystem node: {}",
                entry.path().display()
            );
        }
    }
    Ok(paths)
}

fn validate_action_record_identity(
    state: &str,
    action_id: &str,
    action: &crate::model::ActionSpec,
) -> Result<()> {
    let computed = compute_action_id(action)
        .with_context(|| format!("computing {state} queue action identity"))?;
    if computed != action_id {
        bail!("{state} queue action_id does not match its typed action identity");
    }
    Ok(())
}

fn validate_done_record_identity(done: &QueueDone) -> Result<()> {
    if done.output_artifact.action_id != done.action_id {
        bail!("done queue output artifact action_id does not match queue action_id");
    }
    Ok(())
}

fn expected_queue_record_relpath(action_id: &str) -> PathBuf {
    PathBuf::from(&action_id[0..2])
        .join(&action_id[2..4])
        .join(format!("{action_id}.pb"))
}

fn validate_queue_dir<T>(
    dir: &Path,
    state: &str,
    decode: impl Fn(&[u8]) -> Result<T>,
    validate: impl Fn(&T) -> Result<String>,
) -> Result<BTreeSet<String>> {
    let paths = list_regular_files(dir)?;
    let mut action_ids = BTreeSet::new();
    for path in &paths {
        if path.extension().and_then(|value| value.to_str()) != Some("pb") {
            bail!("queue contains a non-protobuf record: {}", path.display());
        }
        let bytes =
            fs::read(path).with_context(|| format!("reading queue record: {}", path.display()))?;
        let record = decode(&bytes)
            .with_context(|| format!("validating queue record: {}", path.display()))?;
        let action_id = validate(&record)
            .with_context(|| format!("validating {state} queue identity: {}", path.display()))?;
        let relative = path
            .strip_prefix(dir)
            .with_context(|| format!("resolving {state} queue record path"))?;
        if relative != expected_queue_record_relpath(&action_id) {
            bail!("{state} queue record path does not match its embedded action_id");
        }
        if !action_ids.insert(action_id.clone()) {
            bail!("{state} queue contains duplicate action_id {action_id}");
        }
    }
    Ok(action_ids)
}

fn validate_campaign_records(store: &ArtifactStore) -> Result<(usize, usize)> {
    let root = store.campaign_runs_dir();
    let mut campaign_runs = 0_usize;
    let mut analyses = 0_usize;
    let mut campaign_run_ids = BTreeSet::new();
    let mut analysis_run_ids = BTreeSet::new();
    for path in list_regular_files(&root)? {
        match path.file_name().and_then(|value| value.to_str()) {
            Some(CAMPAIGN_RUN_MANIFEST_FILENAME) => {
                let manifest = load_campaign_run_file(&path)?;
                let run_id = manifest
                    .run_id
                    .as_ref()
                    .context("campaign run manifest missing run_id")?;
                let expected_path = campaign_run_path(store, run_id)?;
                if path != expected_path {
                    bail!(
                        "campaign run path does not match its embedded run_id: {}",
                        path.display()
                    );
                }
                let run_id = hex::encode(&run_id.value);
                if !campaign_run_ids.insert(run_id.clone()) {
                    bail!("campaign record tree contains duplicate run_id {run_id}");
                }
                campaign_runs += 1;
            }
            Some(CAMPAIGN_ANALYSIS_FILENAME) => {
                let bytes = fs::read(&path)
                    .with_context(|| format!("reading campaign analysis: {}", path.display()))?;
                let report = decode_analysis_report(&bytes)
                    .with_context(|| format!("validating campaign analysis: {}", path.display()))?;
                validate_analysis_report_against_store(store, &report).with_context(|| {
                    format!("validating campaign analysis lineage: {}", path.display())
                })?;
                let expected_path = campaign_analysis_path(
                    store,
                    report
                        .run_id
                        .as_ref()
                        .context("campaign analysis missing run_id")?,
                )?;
                if path != expected_path {
                    bail!(
                        "campaign analysis path does not match its embedded run_id: {}",
                        path.display()
                    );
                }
                let run_id = hex::encode(
                    &report
                        .run_id
                        .as_ref()
                        .expect("validated campaign analysis run_id")
                        .value,
                );
                if !analysis_run_ids.insert(run_id.clone()) {
                    bail!("campaign analysis tree contains duplicate run_id {run_id}");
                }
                analyses += 1;
            }
            _ => bail!(
                "campaign record tree contains an unexpected file: {}",
                path.display()
            ),
        }
    }
    Ok((campaign_runs, analyses))
}

fn validate_coordinator_records(store: &ArtifactStore) -> Result<usize> {
    let root = store.coordinator_dir();
    let lock_path = root.join(COORDINATOR_LOCK_FILENAME);
    let mut records = 0_usize;
    let mut run_ids = BTreeSet::new();
    for path in list_regular_files(&root)? {
        if path == lock_path {
            continue;
        }
        if path.extension().and_then(|value| value.to_str()) != Some("pb") {
            bail!(
                "coordinator record tree contains an unexpected file: {}",
                path.display()
            );
        }
        let bytes = fs::read(&path)
            .with_context(|| format!("reading coordinator record: {}", path.display()))?;
        let state = decode_coordinator_state(&bytes)
            .with_context(|| format!("validating coordinator record: {}", path.display()))?;
        let run_id = hex::encode(
            &state
                .run_id
                .as_ref()
                .expect("validated coordinator run_id")
                .value,
        );
        let expected_path = coordinator_state_path(store, &run_id);
        if path != expected_path {
            bail!(
                "coordinator record path does not match its embedded run_id: {}",
                path.display()
            );
        }
        if !run_ids.insert(run_id.clone()) {
            bail!("coordinator record tree contains duplicate run_id {run_id}");
        }
        records += 1;
    }
    Ok(records)
}

pub(crate) fn validate_store(
    store: &ArtifactStore,
    verify_payloads: bool,
) -> Result<ValidateStoreSummary> {
    let provenances = store.list_provenances()?;
    let failed_records = store.load_failed_action_records_uncached()?;
    for failed in &failed_records {
        validate_action_record_identity("failed", &failed.action_id, &failed.action)?;
    }
    let failed_ids = failed_records
        .iter()
        .map(|record| record.action_id.clone())
        .collect::<BTreeSet<_>>();
    let provenance_ids = provenances
        .iter()
        .map(|record| record.action_id.clone())
        .collect::<BTreeSet<_>>();
    let pending_ids = validate_queue_dir(
        &store.queue_pending_dir(),
        "pending",
        decode_queue_item,
        |record| {
            validate_action_record_identity("pending", &record.action_id, &record.action)?;
            Ok(record.action_id.clone())
        },
    )?;
    let running_ids = validate_queue_dir(
        &store.queue_running_dir(),
        "running",
        decode_queue_running,
        |record| {
            validate_action_record_identity("running", &record.action_id, &record.action)?;
            Ok(record.action_id.clone())
        },
    )?;
    let done_ids = validate_queue_dir(
        &store.queue_done_dir(),
        "done",
        decode_queue_done,
        |record| {
            validate_done_record_identity(record)?;
            Ok(record.action_id.clone())
        },
    )?;
    let canceled_ids = validate_queue_dir(
        &store.queue_canceled_dir(),
        "canceled",
        decode_queue_canceled,
        |record| {
            validate_action_record_identity("canceled", &record.action_id, &record.action)?;
            Ok(record.action_id.clone())
        },
    )?;
    let mut states_by_action: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (state, ids) in [
        ("pending", &pending_ids),
        ("running", &running_ids),
        ("done", &done_ids),
        ("failed", &failed_ids),
        ("canceled", &canceled_ids),
    ] {
        for action_id in ids {
            states_by_action.entry(action_id).or_default().push(state);
        }
    }
    for (action_id, states) in states_by_action {
        if states.len() > 1 {
            bail!(
                "action has conflicting queue states: action_id={} states={}",
                action_id,
                states.join(",")
            );
        }
    }
    for action_id in &done_ids {
        if !provenance_ids.contains(action_id) {
            bail!("done queue action is missing committed provenance: action_id={action_id}");
        }
    }
    for (state, ids) in [
        ("pending", &pending_ids),
        ("running", &running_ids),
        ("failed", &failed_ids),
        ("canceled", &canceled_ids),
    ] {
        if let Some(action_id) = ids.iter().find(|id| provenance_ids.contains(*id)) {
            bail!(
                "committed action has conflicting {} queue state: action_id={}",
                state,
                action_id
            );
        }
    }

    let (campaign_run_records, analysis_records) = validate_campaign_records(store)?;
    let coordinator_records = validate_coordinator_records(store)?;
    let mut verified_payload_files = 0_usize;
    let mut verified_payload_bytes = 0_u64;
    if verify_payloads {
        for provenance in &provenances {
            let payload_dir = store
                .materialize_action_dir(&provenance.action_id)?
                .join("payload");
            for output in &provenance.output_files {
                let path = payload_dir.join(&output.path);
                let bytes = fs::read(&path).with_context(|| {
                    format!(
                        "reading declared output action_id={} path={}",
                        provenance.action_id,
                        path.display()
                    )
                })?;
                if bytes.len() as u64 != output.bytes {
                    bail!(
                        "output size mismatch action_id={} path={} declared={} actual={}",
                        provenance.action_id,
                        output.path,
                        output.bytes,
                        bytes.len()
                    );
                }
                let digest = hex::encode(Sha256::digest(&bytes));
                if digest != output.sha256 {
                    bail!(
                        "output digest mismatch action_id={} path={} declared={} actual={}",
                        provenance.action_id,
                        output.path,
                        output.sha256,
                        digest
                    );
                }
                verified_payload_files += 1;
                verified_payload_bytes += bytes.len() as u64;
            }
        }
    }
    store.flush_durable()?;
    Ok(ValidateStoreSummary {
        provenance_records: provenances.len(),
        failed_records: failed_ids.len(),
        pending_records: pending_ids.len(),
        running_records: running_ids.len(),
        done_records: done_ids.len(),
        canceled_records: canceled_ids.len(),
        campaign_run_records,
        analysis_records,
        coordinator_records,
        verified_payload_files,
        verified_payload_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use prost::Message;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::campaign::reconcile_campaign_run;
    use crate::model::{ActionSpec, ArtifactRef, ArtifactType, QueueDone};
    use crate::proto::{encode_queue_failed, encode_queue_item, timestamp_to_proto, v1 as pb};
    use crate::versioning::load_version_compat_map;

    fn temp_path() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "xlsynth-bvc-validate-store-{}-{nanos}",
            std::process::id()
        ))
    }

    fn identity_test_action() -> ActionSpec {
        ActionSpec::ImportIrPackageFile {
            source_sha256: "1".repeat(64),
            top_fn_name: Some("main".to_string()),
        }
    }

    #[test]
    fn empty_protobuf_store_validates() {
        let root = temp_path();
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let summary = validate_store(&store, true).expect("validate");
        assert_eq!(summary.provenance_records, 0);
        assert_eq!(summary.pending_records, 0);
        assert_eq!(summary.campaign_run_records, 0);
        assert_eq!(summary.analysis_records, 0);
        assert_eq!(summary.coordinator_records, 0);
        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn non_protobuf_queue_file_is_rejected() {
        let root = temp_path();
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        fs::write(store.queue_pending_dir().join("legacy.json"), "{}")
            .expect("write unexpected queue record");
        let error = validate_store(&store, false).expect_err("unexpected queue file must fail");
        assert!(error.to_string().contains("non-protobuf record"));
        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn queue_identity_validators_cover_every_action_bearing_state_and_done_output() {
        let action = identity_test_action();
        let wrong_action_id = "f".repeat(64);
        for state in ["pending", "running", "failed", "canceled"] {
            let error = validate_action_record_identity(state, &wrong_action_id, &action)
                .expect_err("computed action mismatch must fail");
            assert!(
                format!("{error:#}").contains("does not match its typed action identity"),
                "unexpected error: {error:#}"
            );
        }

        let action_id = compute_action_id(&action).expect("action id");
        let done = QueueDone {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id,
            completed_utc: Utc::now(),
            completed_by: "test-worker".to_string(),
            output_artifact: ArtifactRef {
                action_id: "e".repeat(64),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: "payload/input.ir".to_string(),
            },
        };
        let error = validate_done_record_identity(&done)
            .expect_err("done output ownership mismatch must fail");
        assert!(format!("{error:#}").contains("output artifact action_id"));
    }

    #[test]
    fn validate_store_rejects_queue_path_identity_mismatch() {
        let root = temp_path();
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let action = identity_test_action();
        let action_id = compute_action_id(&action).expect("action id");
        let pending = QueueItem {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id,
            enqueued_utc: Utc::now(),
            priority: crate::DEFAULT_QUEUE_PRIORITY,
            action,
        };
        let wrong_path = store.pending_queue_path(&"e".repeat(64));
        fs::create_dir_all(wrong_path.parent().expect("queue parent")).expect("create queue path");
        fs::write(
            &wrong_path,
            encode_queue_item(&pending).expect("encode pending"),
        )
        .expect("write misplaced pending record");

        let error =
            validate_store(&store, false).expect_err("misplaced queue record must fail validation");
        assert!(
            format!("{error:#}").contains("path does not match its embedded action_id"),
            "unexpected error: {error:#}"
        );
        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn validate_store_rejects_sled_failed_key_identity_mismatch() {
        let root = temp_path();
        let db_path = root.join("artifacts.test.sled");
        let initializer = ArtifactStore::new_with_sled(root.clone(), db_path.clone());
        initializer.ensure_layout().expect("layout");
        drop(initializer);
        let action = identity_test_action();
        let action_id = compute_action_id(&action).expect("action id");
        let now = Utc::now();
        let failed = QueueFailed {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id,
            enqueued_utc: now,
            failed_utc: now,
            failed_by: "test-worker".to_string(),
            action,
            error: "test failure".to_string(),
        };
        let db = sled::open(&db_path).expect("open sled");
        let tree = db.open_tree("failed_by_action").expect("open failed tree");
        tree.insert(
            "e".repeat(64).as_bytes(),
            encode_queue_failed(&failed).expect("encode failed"),
        )
        .expect("insert mismatched failed row");
        db.flush().expect("flush mismatched failed row");
        drop(tree);
        drop(db);

        let store = ArtifactStore::new_with_sled(root.clone(), db_path);
        let error =
            validate_store(&store, false).expect_err("mismatched Sled key must fail validation");
        assert!(
            format!("{error:#}").contains("Sled key does not match"),
            "unexpected error: {error:#}"
        );
        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn validate_store_rejects_conflicting_active_and_terminal_states() {
        let root = temp_path();
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let action = identity_test_action();
        let action_id = crate::queue::enqueue_action(&store, action.clone()).expect("enqueue");
        let now = Utc::now();
        store
            .write_failed_action_record(&QueueFailed {
                schema_version: crate::ACTION_SCHEMA_VERSION,
                action_id,
                enqueued_utc: now,
                failed_utc: now,
                failed_by: "test-worker".to_string(),
                action,
                error: "test failure".to_string(),
            })
            .expect("write failed");

        let error = validate_store(&store, false).expect_err("conflict must fail");
        assert!(
            format!("{error:#}").contains("conflicting queue states"),
            "unexpected error: {error:#}"
        );
        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn campaign_and_coordinator_records_are_counted_and_validated() {
        let root = temp_path();
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let repo_root = std::env::current_dir().expect("current dir");
        let version = load_version_compat_map(&repo_root)
            .expect("compatibility map")
            .into_keys()
            .next()
            .expect("known version");
        let campaign =
            reconcile_campaign_run(&store, &repo_root, &version, 0).expect("campaign record");

        let run_id = hex::decode(&campaign.run_id).expect("decode run id");
        let state = pb::CoordinatorState {
            record_version: 2,
            run_id: Some(pb::Sha256Digest {
                value: run_id.clone(),
            }),
            crate_version: Some(pb::CrateVersion { value: version }),
            current_stage: pb::CoordinatorStage::Planned as i32,
            stage_results: Vec::new(),
            updated_at: Some(timestamp_to_proto(&Utc::now())),
            snapshot_dir: String::new(),
            site_dir: String::new(),
            published_site_id: None,
            indexed_source_fingerprint: None,
            indexed_output_fingerprint: None,
            baseline_run_id: None,
            baseline_crate_version: None,
        };
        let state_path = coordinator_state_path(&store, &campaign.run_id);
        fs::create_dir_all(state_path.parent().expect("state parent"))
            .expect("create state parent");
        fs::write(&state_path, state.encode_to_vec()).expect("write coordinator state");
        fs::write(store.coordinator_dir().join(COORDINATOR_LOCK_FILENAME), "")
            .expect("write coordinator lock");

        for domain in ["queue", "campaign", "analysis", "coordinator"] {
            let debris = store
                .staging_dir()
                .join("atomic-records")
                .join(domain)
                .join("interrupted-record.tmp");
            fs::create_dir_all(debris.parent().expect("debris parent"))
                .expect("create atomic debris directory");
            fs::write(&debris, b"interrupted protobuf write")
                .expect("seed interrupted atomic write");
        }

        let summary = validate_store(&store, false).expect("validate records");
        assert_eq!(summary.campaign_run_records, 1);
        assert_eq!(summary.analysis_records, 0);
        assert_eq!(summary.coordinator_records, 1);

        let manifest_path =
            campaign_run_path(&store, &pb::Sha256Digest { value: run_id }).expect("campaign path");
        let wrong_manifest_path = store
            .campaign_runs_dir()
            .join("ff/ff/copied")
            .join(CAMPAIGN_RUN_MANIFEST_FILENAME);
        fs::create_dir_all(wrong_manifest_path.parent().expect("wrong manifest parent"))
            .expect("create wrong manifest parent");
        fs::copy(&manifest_path, &wrong_manifest_path).expect("copy miskeyed campaign manifest");
        let error = crate::campaign::list_campaign_runs(&store)
            .expect_err("ordinary campaign enumeration must reject a miskeyed manifest");
        assert!(
            format!("{error:#}").contains("campaign run path does not match"),
            "unexpected error: {error:#}"
        );
        let error = validate_store(&store, false).expect_err("miskeyed campaign must fail");
        assert!(
            format!("{error:#}").contains("campaign run path does not match"),
            "unexpected error: {error:#}"
        );
        fs::remove_file(&wrong_manifest_path).expect("remove miskeyed campaign");

        let wrong_state_path = store.coordinator_dir().join("ff/ff/copied.pb");
        fs::create_dir_all(wrong_state_path.parent().expect("wrong state parent"))
            .expect("create wrong state parent");
        fs::copy(&state_path, &wrong_state_path).expect("copy miskeyed coordinator state");
        let error = validate_store(&store, false).expect_err("miskeyed coordinator must fail");
        assert!(
            format!("{error:#}").contains("coordinator record path does not match"),
            "unexpected error: {error:#}"
        );

        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[cfg(unix)]
    #[test]
    fn record_tree_validation_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let root = temp_path();
        let outside = root.with_extension("outside");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        fs::create_dir_all(&outside).expect("outside directory");
        symlink(&outside, store.queue_pending_dir().join("linked-records"))
            .expect("create record-tree symlink");

        let error = validate_store(&store, false).expect_err("symlink must fail validation");
        assert!(
            format!("{error:#}").contains("symlink or special filesystem node"),
            "unexpected error: {error:#}"
        );

        drop(store);
        fs::remove_dir_all(root).expect("cleanup store");
        fs::remove_dir_all(outside).expect("cleanup outside");
    }

    #[test]
    fn corrupt_analysis_record_is_rejected() {
        let root = temp_path();
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let analysis_path = store.campaign_runs_dir().join("aa/bb/analysis.pb");
        fs::create_dir_all(analysis_path.parent().expect("analysis parent"))
            .expect("create analysis parent");
        fs::write(&analysis_path, b"not protobuf").expect("write corrupt analysis");
        let error = validate_store(&store, false).expect_err("corrupt analysis must fail");
        assert!(error.to_string().contains("validating campaign analysis"));
        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn payload_verification_resolves_output_files_below_payload_root() {
        use crate::executor::compute_action_id;
        use crate::model::{ActionSpec, ArtifactRef, ArtifactType, OutputFile, Provenance};

        let root = temp_path();
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let action = ActionSpec::ImportIrPackageFile {
            source_sha256: "1".repeat(64),
            top_fn_name: None,
        };
        let action_id = compute_action_id(&action).expect("action id");
        let staging_dir = store.staging_dir().join(format!("{action_id}-stage"));
        let payload_dir = staging_dir.join("payload");
        fs::create_dir_all(&payload_dir).expect("payload directory");
        let payload =
            b"package test\n\nfn test() -> bits[1] { ret one: bits[1] = literal(value=1) }\n";
        fs::write(payload_dir.join("input.ir"), payload).expect("payload file");
        let provenance = Provenance {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: action_id.clone(),
            created_utc: Utc::now(),
            action,
            dependencies: Vec::new(),
            output_artifact: ArtifactRef {
                action_id: action_id.clone(),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: "payload/input.ir".to_string(),
            },
            output_files: vec![OutputFile {
                path: "input.ir".to_string(),
                bytes: payload.len() as u64,
                sha256: hex::encode(Sha256::digest(payload)),
            }],
            commands: Vec::new(),
            details: serde_json::json!({}),
            suggested_next_actions: Vec::new(),
        };
        fs::write(
            staging_dir.join("provenance.pb"),
            crate::proto::encode_provenance(&provenance).expect("encode provenance"),
        )
        .expect("provenance file");
        store
            .promote_staging_action_dir(&action_id, &staging_dir)
            .expect("promote action");

        let summary = validate_store(&store, true).expect("verify payload");
        assert_eq!(summary.verified_payload_files, 1);
        assert_eq!(summary.verified_payload_bytes, payload.len() as u64);

        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }
}
