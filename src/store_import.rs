// SPDX-License-Identifier: Apache-2.0

//! Combine idle canonical stores without using derived web projections as evidence.

use anyhow::{Context, Result, bail};
use prost::Message;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use crate::model::Provenance;
use crate::proto::{decode_provenance, encode_queue_failed, v1 as pb};
use crate::store::ArtifactStore;
use crate::store_validation::validate_store;

#[derive(Debug, Default, Serialize)]
pub(crate) struct ImportStoreSummary {
    pub(crate) imported_actions: usize,
    pub(crate) reused_actions: usize,
    pub(crate) imported_payload_files: usize,
    pub(crate) imported_payload_bytes: u64,
    pub(crate) imported_failed_records: usize,
    pub(crate) imported_terminal_records: usize,
    pub(crate) imported_campaign_records: usize,
    pub(crate) imported_identity_alias_records: usize,
}

fn resolved_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut existing = absolute.as_path();
    let mut missing = Vec::new();
    while !existing.exists() {
        if fs::symlink_metadata(existing).is_ok() {
            bail!("canonical import path contains a dangling link");
        }
        missing.push(
            existing
                .file_name()
                .context("import path has no existing ancestor")?,
        );
        existing = existing.parent().context("import path has no parent")?;
    }
    let mut resolved = fs::canonicalize(existing)?;
    for part in missing.into_iter().rev() {
        resolved.push(part);
    }
    Ok(resolved)
}

/// Check all store-root/database pairs before either store is initialized. Existing ancestors
/// are resolved so a symlink cannot hide source/destination overlap.
pub(crate) fn preflight_paths(
    destination_root: &Path,
    destination_db: &Path,
    source_root: &Path,
    source_db: &Path,
) -> Result<()> {
    if !source_root.is_dir() || !source_db.is_dir() {
        bail!("canonical import source store and database must already exist");
    }
    let source = [resolved_path(source_root)?, resolved_path(source_db)?];
    let destination = [
        resolved_path(destination_root)?,
        resolved_path(destination_db)?,
    ];
    for source_path in &source {
        for destination_path in &destination {
            if source_path.starts_with(destination_path)
                || destination_path.starts_with(source_path)
            {
                bail!("canonical import source and destination must not overlap");
            }
        }
    }
    Ok(())
}

fn regular_files(root: &Path) -> Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in WalkDir::new(root).sort_by_file_name() {
        let entry = entry.context("walking canonical import records")?;
        if entry.file_type().is_file() {
            files.push(entry.into_path());
        } else if !entry.file_type().is_dir() {
            bail!("canonical import rejects links and special filesystem nodes");
        }
    }
    Ok(files)
}

fn idle(store: &ArtifactStore) -> Result<()> {
    if !regular_files(&store.queue_pending_dir())?.is_empty()
        || !regular_files(&store.queue_running_dir())?.is_empty()
    {
        bail!("canonical store import requires idle source and destination queues");
    }
    Ok(())
}

fn validate_done_contracts(store: &ArtifactStore) -> Result<()> {
    for path in regular_files(&store.queue_done_dir())? {
        let done = crate::proto::decode_queue_done(&fs::read(path)?)?;
        let committed = store.load_provenance(&done.action_id)?.output_artifact;
        if done.output_artifact.action_id != committed.action_id
            || done.output_artifact.artifact_type != committed.artifact_type
            || done.output_artifact.relpath != committed.relpath
        {
            bail!("canonical import done record disagrees with committed output artifact");
        }
    }
    Ok(())
}

fn validate_combined_aliases(source: &ArtifactStore, destination: &ArtifactStore) -> Result<()> {
    let mut aliases = BTreeMap::new();
    for store in [source, destination] {
        for path in regular_files(&store.queue_identity_aliases_dir())? {
            let source_id = path
                .file_stem()
                .and_then(|s| s.to_str())
                .context("canonical import alias has no action id")?
                .to_owned();
            let target_id = fs::read_to_string(path)?.trim().to_owned();
            if let Some(existing) = aliases.insert(source_id, target_id.clone())
                && existing != target_id
            {
                bail!("canonical import alias targets conflict");
            }
        }
    }
    for start in aliases.keys() {
        let mut seen = BTreeSet::new();
        let mut current = start;
        while let Some(next) = aliases.get(current) {
            if !seen.insert(current) {
                bail!("combined canonical import aliases contain a cycle");
            }
            current = next;
        }
    }
    Ok(())
}

fn canonical_files(
    store: &ArtifactStore,
    expected: &Provenance,
) -> Result<BTreeMap<String, Vec<u8>>> {
    let files = store
        .read_action_files(&expected.action_id)?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let provenance_bytes = files
        .get("provenance.pb")
        .context("canonical action files are missing provenance")?;
    let provenance = decode_provenance(provenance_bytes)?;
    if provenance.action_id != expected.action_id {
        bail!("canonical import action identity changed during enumeration");
    }
    for output in &provenance.output_files {
        let bytes = files
            .get(&format!("payload/{}", output.path))
            .with_context(|| {
                format!(
                    "canonical action is missing declared output {}",
                    output.path
                )
            })?;
        if bytes.len() as u64 != output.bytes || hex::encode(Sha256::digest(bytes)) != output.sha256
        {
            bail!(
                "canonical import output digest mismatch for action {}",
                expected.action_id
            );
        }
    }
    Ok(files)
}

fn equal_action_files(
    source: &BTreeMap<String, Vec<u8>>,
    destination: &BTreeMap<String, Vec<u8>>,
) -> Result<bool> {
    let source_provenance = pb::Provenance::decode(source["provenance.pb"].as_slice())?;
    let destination_provenance = pb::Provenance::decode(destination["provenance.pb"].as_slice())?;
    // Execution time, command traces, and suggestions can differ across executions of the same
    // immutable action. The retained destination owns those diagnostics. Its result must agree.
    let same_contract = source_provenance.record_version == destination_provenance.record_version
        && source_provenance.action_id == destination_provenance.action_id
        && source_provenance.action == destination_provenance.action
        && source_provenance.dependencies == destination_provenance.dependencies
        && source_provenance.output_artifact == destination_provenance.output_artifact
        && source_provenance.output_files == destination_provenance.output_files
        && source_provenance.details == destination_provenance.details;
    Ok(same_contract
        && source
            .iter()
            .filter(|(path, _)| *path != "provenance.pb")
            .eq(destination
                .iter()
                .filter(|(path, _)| *path != "provenance.pb")))
}

fn require_same_record(path: &Path, bytes: &[u8], done: bool) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let existing = fs::read(path)?;
    let same = if done {
        let a = pb::QueueDoneRecord::decode(existing.as_slice())?;
        let b = pb::QueueDoneRecord::decode(bytes)?;
        a.record_version == b.record_version
            && a.action_id == b.action_id
            && a.output_artifact == b.output_artifact
    } else {
        existing == bytes
    };
    if !same {
        bail!(
            "canonical import record conflicts with destination: {}",
            path.display()
        );
    }
    Ok(())
}

fn record_files(
    source: &ArtifactStore,
    destination: &ArtifactStore,
) -> Result<Vec<(PathBuf, Vec<u8>, bool, &'static str)>> {
    let mut records = Vec::new();
    for (source_root, destination_root, done, kind) in [
        (
            source.queue_done_dir(),
            destination.queue_done_dir(),
            true,
            "terminal",
        ),
        (
            source.queue_canceled_dir(),
            destination.queue_canceled_dir(),
            false,
            "terminal",
        ),
        (
            source.campaign_runs_dir(),
            destination.campaign_runs_dir(),
            false,
            "campaign",
        ),
        (
            source.queue_identity_aliases_dir(),
            destination.queue_identity_aliases_dir(),
            false,
            "alias",
        ),
    ] {
        for path in regular_files(&source_root)? {
            let destination_path = destination_root.join(path.strip_prefix(&source_root)?);
            let bytes = fs::read(&path)?;
            require_same_record(&destination_path, &bytes, done)?;
            records.push((destination_path, bytes, done, kind));
        }
    }
    Ok(records)
}

/// Import into an idle destination using the normal canonical promotion and record writers.
/// Identity and payload conflicts are checked before the first import write. An I/O failure may
/// leave a valid prefix imported; retrying is idempotent. Derived web indices and runtime caches
/// are deliberately omitted and must be regenerated from the combined canonical actions.
pub(crate) fn import_store(
    destination: &ArtifactStore,
    source: &ArtifactStore,
) -> Result<ImportStoreSummary> {
    if source.is_snapshot_backend() || destination.is_snapshot_backend() {
        bail!("canonical store import requires artifact stores, not publication snapshots");
    }
    preflight_paths(
        &destination.root,
        destination.artifact_backend_storage_path(),
        &source.root,
        source.artifact_backend_storage_path(),
    )?;
    idle(source)?;
    idle(destination)?;
    validate_store(source, false).context("validating canonical import source")?;
    validate_store(destination, false).context("validating canonical import destination")?;
    validate_done_contracts(source)?;
    validate_done_contracts(destination)?;
    validate_combined_aliases(source, destination)?;
    let provenances = source.list_provenances_uncached()?;
    let failed = source.load_failed_action_records_uncached()?;
    let records = record_files(source, destination)?;

    // Preflight all identities and canonical payloads, avoiding partial writes on conflicts.
    for provenance in &provenances {
        let action_id = &provenance.action_id;
        let files = canonical_files(source, provenance)?;
        if destination.load_failed_action_record(action_id)?.is_some()
            || destination.canceled_queue_path(action_id).exists()
        {
            bail!("canonical import conflicts with destination terminal state for {action_id}");
        }
        if destination.action_exists(action_id) {
            let existing = destination.load_provenance(action_id)?;
            let existing_files = canonical_files(destination, &existing)?;
            if !equal_action_files(&files, &existing_files)? {
                bail!("canonical import action result conflicts for {action_id}");
            }
        }
    }
    for record in &failed {
        let action_id = &record.action_id;
        if destination.action_exists(action_id)
            || destination.done_queue_path(action_id).exists()
            || destination.canceled_queue_path(action_id).exists()
        {
            bail!(
                "canonical import failure conflicts with destination terminal state for {action_id}"
            );
        }
        if let Some(existing) = destination.load_failed_action_record(action_id)?
            && encode_queue_failed(&existing)? != encode_queue_failed(record)?
        {
            bail!("canonical import failed record conflicts for {action_id}");
        }
    }
    for path in regular_files(&source.queue_canceled_dir())? {
        let canceled = crate::proto::decode_queue_canceled(&fs::read(&path)?)?;
        let action_id = &canceled.action_id;
        if destination.action_exists(action_id)
            || destination.done_queue_path(action_id).exists()
            || destination.load_failed_action_record(action_id)?.is_some()
        {
            bail!(
                "canonical import cancellation conflicts with destination terminal state for {action_id}"
            );
        }
    }
    idle(source)?;
    idle(destination)?;
    let mut summary = ImportStoreSummary::default();
    for provenance in &provenances {
        if destination.action_exists(&provenance.action_id) {
            summary.reused_actions += 1;
            continue;
        }
        let files = canonical_files(source, provenance)?;
        let stage = destination.staging_dir().join(format!(
            "import-{}-{}-{}",
            provenance.action_id,
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        fs::create_dir(&stage)?;
        let result = (|| -> Result<()> {
            for (relpath, bytes) in &files {
                let path = stage.join(relpath);
                fs::create_dir_all(path.parent().context("import file lacks parent")?)?;
                fs::write(path, bytes)?;
            }
            destination.promote_staging_action_dir(&provenance.action_id, &stage)
        })();
        if stage.exists() {
            fs::remove_dir_all(&stage)?;
        }
        result?;
        summary.imported_actions += 1;
        summary.imported_payload_files += provenance.output_files.len();
        summary.imported_payload_bytes += provenance
            .output_files
            .iter()
            .map(|file| file.bytes)
            .sum::<u64>();
    }
    for record in failed {
        if destination
            .load_failed_action_record(&record.action_id)?
            .is_none()
        {
            destination.write_failed_action_record(&record)?;
            summary.imported_failed_records += 1;
        }
    }
    for (path, bytes, _, kind) in records {
        if !path.exists() {
            destination.write_record_atomic("canonical-import", &path, &bytes)?;
            match kind {
                "terminal" => summary.imported_terminal_records += 1,
                "campaign" => summary.imported_campaign_records += 1,
                "alias" => summary.imported_identity_alias_records += 1,
                _ => unreachable!(),
            }
        }
    }
    destination.flush_durable()?;
    validate_store(destination, false).context("validating combined canonical store")?;
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ActionSpec, ArtifactRef, ArtifactType, OutputFile, QueueDone, QueueItem};
    use crate::proto::{encode_provenance, encode_queue_done, encode_queue_item};
    use chrono::Utc;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn with_stores(test: impl FnOnce(&ArtifactStore, &ArtifactStore)) {
        let root = std::env::temp_dir().join(format!(
            "bvc-canonical-import-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let source = ArtifactStore::new(root.join("source"));
        let destination = ArtifactStore::new(root.join("destination"));
        source.ensure_layout().unwrap();
        destination.ensure_layout().unwrap();
        test(&source, &destination);
        drop(source);
        drop(destination);
        fs::remove_dir_all(root).unwrap();
    }

    fn add_action(store: &ArtifactStore, bytes: &[u8], identity: &str) -> Provenance {
        let action = ActionSpec::ImportIrPackageFile {
            source_sha256: hex::encode(Sha256::digest(identity)),
            top_fn_name: Some("main".into()),
        };
        let action_id = crate::proto::compute_model_action_id_v2(&action)
            .unwrap()
            .to_hex();
        let provenance = Provenance {
            schema_version: 1,
            action_id: action_id.clone(),
            created_utc: Utc::now(),
            action,
            dependencies: Vec::new(),
            output_artifact: ArtifactRef {
                action_id: action_id.clone(),
                artifact_type: ArtifactType::IrPackageFile,
                relpath: "payload/package.ir".into(),
            },
            output_files: vec![OutputFile {
                path: "package.ir".into(),
                bytes: bytes.len() as u64,
                sha256: hex::encode(Sha256::digest(bytes)),
            }],
            commands: Vec::new(),
            details: json!({"source_path": "input/package.ir", "import_kind": "local_ir_file"}),
            suggested_next_actions: Vec::new(),
        };
        let stage = store.staging_dir().join(&action_id);
        fs::create_dir_all(stage.join("payload")).unwrap();
        fs::write(stage.join("payload/package.ir"), bytes).unwrap();
        fs::write(
            stage.join("provenance.pb"),
            encode_provenance(&provenance).unwrap(),
        )
        .unwrap();
        store
            .promote_staging_action_dir(&action_id, &stage)
            .unwrap();
        let done = QueueDone {
            schema_version: 1,
            action_id: action_id.clone(),
            completed_utc: Utc::now(),
            completed_by: "test-worker".into(),
            output_artifact: provenance.output_artifact.clone(),
        };
        store
            .write_record_atomic(
                "test",
                &store.done_queue_path(&action_id),
                &encode_queue_done(&done).unwrap(),
            )
            .unwrap();
        provenance
    }

    #[test]
    fn imports_canonical_sled_bytes_and_terminal_records_idempotently() {
        with_stores(|source, destination| {
            let provenance = add_action(source, b"abc", "first");
            // Materialized files are a cache, never import evidence.
            let cache = source
                .materialize_action_dir(&provenance.action_id)
                .unwrap();
            fs::write(cache.join("payload/package.ir"), b"tampered cache").unwrap();
            let summary = import_store(destination, source).unwrap();
            assert_eq!(summary.imported_actions, 1);
            assert_eq!(summary.imported_terminal_records, 1);
            assert_eq!(summary.imported_payload_bytes, 3);
            assert_eq!(
                destination
                    .read_action_files(&provenance.action_id)
                    .unwrap(),
                source.read_action_files(&provenance.action_id).unwrap()
            );
            let done = crate::proto::decode_queue_done(
                &fs::read(destination.done_queue_path(&provenance.action_id)).unwrap(),
            )
            .unwrap();
            assert_eq!(done.action_id, provenance.action_id);
            let repeated = import_store(destination, source).unwrap();
            assert_eq!(repeated.imported_actions, 0);
            assert_eq!(repeated.reused_actions, 1);
            assert_eq!(repeated.imported_terminal_records, 0);
        });
    }

    #[test]
    fn accepts_equal_results_from_independent_execution_times() {
        with_stores(|source, destination| {
            let provenance = add_action(source, b"abc", "first");
            add_action(destination, b"abc", "first");
            let summary = import_store(destination, source).unwrap();
            assert_eq!(summary.imported_actions, 0);
            assert_eq!(summary.reused_actions, 1);
            assert_eq!(destination.list_provenances_uncached().unwrap().len(), 1);
            assert!(destination.done_queue_path(&provenance.action_id).is_file());
        });
    }

    #[test]
    fn rejects_payload_identity_collision_before_any_import_writes() {
        with_stores(|source, destination| {
            add_action(source, b"unrelated", "new");
            let collision = add_action(source, b"source result", "same");
            add_action(destination, b"destination result", "same");
            assert!(import_store(destination, source).is_err());
            let retained = destination.list_provenances_uncached().unwrap();
            assert_eq!(retained.len(), 1);
            assert_eq!(retained[0].action_id, collision.action_id);
            assert_eq!(
                destination
                    .read_action_files(&collision.action_id)
                    .unwrap()
                    .into_iter()
                    .collect::<BTreeMap<_, _>>()["payload/package.ir"],
                b"destination result"
            );
        });
    }

    #[test]
    fn rejects_active_source_queue_without_importing_completed_work() {
        with_stores(|source, destination| {
            add_action(source, b"abc", "completed");
            let action = ActionSpec::ImportIrPackageFile {
                source_sha256: "1".repeat(64),
                top_fn_name: Some("main".into()),
            };
            let action_id = crate::proto::compute_model_action_id_v2(&action)
                .unwrap()
                .to_hex();
            let item = QueueItem {
                schema_version: 1,
                action_id: action_id.clone(),
                enqueued_utc: Utc::now(),
                priority: 0,
                action,
            };
            source
                .write_record_atomic(
                    "test",
                    &source.pending_queue_path(&action_id),
                    &encode_queue_item(&item).unwrap(),
                )
                .unwrap();
            assert!(import_store(destination, source).is_err());
            assert!(destination.list_provenances_uncached().unwrap().is_empty());
        });
    }

    #[test]
    fn rejects_success_failure_terminal_conflict() {
        with_stores(|source, destination| {
            let provenance = add_action(source, b"abc", "first");
            let failed = crate::model::QueueFailed {
                schema_version: 1,
                action_id: provenance.action_id.clone(),
                enqueued_utc: Utc::now(),
                failed_utc: Utc::now(),
                failed_by: "test-worker".into(),
                action: provenance.action,
                error: "bounded proof timeout".into(),
            };
            destination.write_failed_action_record(&failed).unwrap();
            assert!(import_store(destination, source).is_err());
            assert!(destination.list_provenances_uncached().unwrap().is_empty());
            assert!(
                destination
                    .load_failed_action_record(&failed.action_id)
                    .unwrap()
                    .is_some()
            );
        });
    }

    #[test]
    fn preserves_failure_records_and_does_not_copy_derived_indices() {
        with_stores(|source, destination| {
            let action = ActionSpec::ImportIrPackageFile {
                source_sha256: "1".repeat(64),
                top_fn_name: Some("main".into()),
            };
            let action_id = crate::proto::compute_model_action_id_v2(&action)
                .unwrap()
                .to_hex();
            let failed = crate::model::QueueFailed {
                schema_version: 1,
                action_id: action_id.clone(),
                enqueued_utc: Utc::now(),
                failed_utc: Utc::now(),
                failed_by: "test-worker".into(),
                action,
                error: "bounded proof timeout".into(),
            };
            source.write_failed_action_record(&failed).unwrap();
            source
                .write_web_index_bytes("derived-index", b"stale derived data")
                .unwrap();
            let summary = import_store(destination, source).unwrap();
            assert_eq!(summary.imported_failed_records, 1);
            assert_eq!(
                encode_queue_failed(
                    &destination
                        .load_failed_action_record(&action_id)
                        .unwrap()
                        .unwrap()
                )
                .unwrap(),
                encode_queue_failed(&failed).unwrap()
            );
            assert!(
                destination
                    .load_web_index_bytes("derived-index")
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[test]
    fn rejects_overlapping_stores() {
        with_stores(|source, _| {
            assert!(import_store(source, source).is_err());
        });
    }

    #[test]
    fn path_preflight_rejects_every_cross_root_database_overlap_without_creation() {
        with_stores(|source, destination| {
            let parent = source.root.parent().unwrap();
            let source_root = parent.join("path-source");
            let source_db = parent.join("path-source-db");
            fs::create_dir(&source_root).unwrap();
            fs::create_dir(&source_db).unwrap();
            let destination_root = destination.root.join("not-created");
            let destination_db = parent.join("new-destination-db");
            preflight_paths(&destination_root, &destination_db, &source_root, &source_db).unwrap();
            assert!(!destination_root.exists());
            assert!(!destination_db.exists());
            for source_path in [&source_root, &source_db] {
                for nested in [source_path.to_path_buf(), source_path.join("child")] {
                    assert!(
                        preflight_paths(&nested, &destination_db, &source_root, &source_db)
                            .is_err()
                    );
                    assert!(
                        preflight_paths(&destination_root, &nested, &source_root, &source_db)
                            .is_err()
                    );
                }
                assert!(
                    preflight_paths(
                        source_path.parent().unwrap(),
                        &destination_db,
                        &source_root,
                        &source_db
                    )
                    .is_err()
                );
                assert!(
                    preflight_paths(
                        &destination_root,
                        source_path.parent().unwrap(),
                        &source_root,
                        &source_db
                    )
                    .is_err()
                );
            }
            assert!(!destination_root.exists());
            assert!(!destination_db.exists());
        });
    }

    #[test]
    fn rejects_alias_cycle_created_only_by_the_union_before_writing() {
        with_stores(|source, destination| {
            add_action(source, b"abc", "completed");
            let a = "a".repeat(64);
            let b = "b".repeat(64);
            source
                .write_record_atomic("test", &source.queue_identity_alias_path(&a), b.as_bytes())
                .unwrap();
            destination
                .write_record_atomic(
                    "test",
                    &destination.queue_identity_alias_path(&b),
                    a.as_bytes(),
                )
                .unwrap();
            assert!(import_store(destination, source).is_err());
            assert!(destination.list_provenances_uncached().unwrap().is_empty());
            assert!(!destination.queue_identity_alias_path(&a).exists());
            validate_store(destination, false).unwrap();
        });
    }

    #[test]
    fn rejects_done_record_with_a_different_output_contract_before_writing() {
        with_stores(|source, destination| {
            let provenance = add_action(source, b"abc", "completed");
            let path = source.done_queue_path(&provenance.action_id);
            let mut done = crate::proto::decode_queue_done(&fs::read(&path).unwrap()).unwrap();
            done.output_artifact.relpath = "payload/other.ir".into();
            source
                .write_record_atomic("test", &path, &encode_queue_done(&done).unwrap())
                .unwrap();
            assert!(import_store(destination, source).is_err());
            assert!(destination.list_provenances_uncached().unwrap().is_empty());
        });
    }
}
