// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, bail};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

use crate::model::{QueueCanceled, QueueDone, QueueFailed, QueueItem, QueueRunning};
use crate::proto::{
    decode_queue_canceled, decode_queue_done, decode_queue_item, decode_queue_running,
};
use crate::queue::list_queue_files;
use crate::store::ArtifactStore;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ValidateStoreSummary {
    pub(crate) provenance_records: usize,
    pub(crate) failed_records: usize,
    pub(crate) pending_records: usize,
    pub(crate) running_records: usize,
    pub(crate) done_records: usize,
    pub(crate) canceled_records: usize,
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

fn validate_queue_dir<T>(dir: &Path, decode: impl Fn(&[u8]) -> Result<T>) -> Result<usize> {
    let paths = list_queue_files(dir)?;
    for path in &paths {
        if path.extension().and_then(|value| value.to_str()) != Some("pb") {
            bail!("queue contains a non-protobuf record: {}", path.display());
        }
        let bytes =
            fs::read(path).with_context(|| format!("reading queue record: {}", path.display()))?;
        decode(&bytes).with_context(|| format!("validating queue record: {}", path.display()))?;
    }
    Ok(paths.len())
}

pub(crate) fn validate_store(
    store: &ArtifactStore,
    verify_payloads: bool,
) -> Result<ValidateStoreSummary> {
    let provenances = store.list_provenances()?;
    let failed_records = store.load_failed_action_records()?;
    let pending_records = validate_queue_dir(&store.queue_pending_dir(), decode_queue_item)?;
    let running_records = validate_queue_dir(&store.queue_running_dir(), decode_queue_running)?;
    let done_records = validate_queue_dir(&store.queue_done_dir(), decode_queue_done)?;
    let canceled_records = validate_queue_dir(&store.queue_canceled_dir(), decode_queue_canceled)?;
    let mut verified_payload_files = 0_usize;
    let mut verified_payload_bytes = 0_u64;
    if verify_payloads {
        for provenance in &provenances {
            let action_dir = store.materialize_action_dir(&provenance.action_id)?;
            for output in &provenance.output_files {
                let path = action_dir.join(&output.path);
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
        failed_records: failed_records.len(),
        pending_records,
        running_records,
        done_records,
        canceled_records,
        verified_payload_files,
        verified_payload_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

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

    #[test]
    fn empty_protobuf_store_validates() {
        let root = temp_path();
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("layout");
        let summary = validate_store(&store, true).expect("validate");
        assert_eq!(summary.provenance_records, 0);
        assert_eq!(summary.pending_records, 0);
        drop(store);
        fs::remove_dir_all(root).expect("cleanup");
    }
}
