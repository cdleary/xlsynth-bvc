// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use prost::Message;
use prost_types::Timestamp;

use crate::model;
use crate::proto::action::{
    action_id_to_hex, action_id_to_proto, action_spec_from_proto, action_spec_to_proto,
    digest_from_hex, digest_to_hex,
};
use crate::proto::v1 as pb;

pub(crate) const QUEUE_RECORD_VERSION: u32 = 1;

pub(crate) fn timestamp_to_proto(value: &DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

pub(crate) fn timestamp_from_proto(
    value: &Option<Timestamp>,
    field: &str,
) -> Result<DateTime<Utc>> {
    let value = value
        .as_ref()
        .with_context(|| format!("missing required protobuf timestamp {field}"))?;
    if !(0..1_000_000_000).contains(&value.nanos) {
        bail!("{field}.nanos is out of range: {}", value.nanos);
    }
    DateTime::from_timestamp(value.seconds, value.nanos as u32)
        .with_context(|| format!("{field} is outside chrono's supported range"))
}

fn required<'a, T>(value: &'a Option<T>, field: &str) -> Result<&'a T> {
    value
        .as_ref()
        .with_context(|| format!("missing required protobuf field {field}"))
}

fn validate_record_version(version: u32, field: &str) -> Result<()> {
    if version != QUEUE_RECORD_VERSION {
        bail!("{field} has unsupported record version {version}; expected {QUEUE_RECORD_VERSION}");
    }
    Ok(())
}

fn cancellation_kind_to_proto(value: model::QueueCancellationKind) -> pb::QueueCancellationKind {
    match value {
        model::QueueCancellationKind::Dependency => pb::QueueCancellationKind::Dependency,
        model::QueueCancellationKind::WorkPolicyExcluded => {
            pb::QueueCancellationKind::WorkPolicyExcluded
        }
    }
}

fn cancellation_kind_from_proto(raw: i32) -> Result<model::QueueCancellationKind> {
    match pb::QueueCancellationKind::try_from(raw)
        .context("queue_canceled.cancellation_kind is unknown")?
    {
        pb::QueueCancellationKind::Dependency => Ok(model::QueueCancellationKind::Dependency),
        pb::QueueCancellationKind::WorkPolicyExcluded => {
            Ok(model::QueueCancellationKind::WorkPolicyExcluded)
        }
        pb::QueueCancellationKind::Unspecified => {
            bail!("queue_canceled.cancellation_kind must be specified")
        }
    }
}

fn validate_cancellation_kind(
    kind: model::QueueCancellationKind,
    work_policy_rule_id: &Option<String>,
    work_policy_rule_fingerprint: &Option<String>,
) -> Result<()> {
    match (kind, work_policy_rule_id, work_policy_rule_fingerprint) {
        (model::QueueCancellationKind::Dependency, None, None) => Ok(()),
        (model::QueueCancellationKind::Dependency, _, _) => {
            bail!("dependency cancellation must not have work policy evidence")
        }
        (
            model::QueueCancellationKind::WorkPolicyExcluded,
            Some(rule_id),
            Some(rule_fingerprint),
        ) if !rule_id.trim().is_empty() => {
            digest_from_hex(
                rule_fingerprint,
                "queue_canceled.work_policy_rule_fingerprint",
            )?;
            Ok(())
        }
        (model::QueueCancellationKind::WorkPolicyExcluded, _, _) => {
            bail!("work policy cancellation requires a rule id and fingerprint")
        }
    }
}

pub(crate) fn artifact_ref_to_proto(value: &model::ArtifactRef) -> Result<pb::ArtifactRef> {
    let artifact_type = match value.artifact_type {
        model::ArtifactType::DslxFileSubtree => pb::ArtifactType::DslxFileSubtree,
        model::ArtifactType::IrPackageFile => pb::ArtifactType::IrPackageFile,
        model::ArtifactType::IrDelayInfoFile => pb::ArtifactType::IrDelayInfoFile,
        model::ArtifactType::AigFile => pb::ArtifactType::AigFile,
        model::ArtifactType::VerilogFile => pb::ArtifactType::VerilogFile,
        model::ArtifactType::AigStatsFile => pb::ArtifactType::AigStatsFile,
        model::ArtifactType::AigStatDiffFile => pb::ArtifactType::AigStatDiffFile,
        model::ArtifactType::EquivReportFile => pb::ArtifactType::EquivReportFile,
    };
    Ok(pb::ArtifactRef {
        action_id: Some(action_id_to_proto(
            &value.action_id,
            "artifact_ref.action_id",
        )?),
        artifact_type: artifact_type as i32,
        relpath: Some(pb::NormalizedRelpath {
            value: value.relpath.clone(),
        }),
    })
}

pub(crate) fn artifact_ref_from_proto(value: &pb::ArtifactRef) -> Result<model::ArtifactRef> {
    let artifact_type = match pb::ArtifactType::try_from(value.artifact_type)
        .context("artifact_ref.artifact_type is unknown")?
    {
        pb::ArtifactType::Unspecified => bail!("artifact_ref.artifact_type must be specified"),
        pb::ArtifactType::DslxFileSubtree => model::ArtifactType::DslxFileSubtree,
        pb::ArtifactType::IrPackageFile => model::ArtifactType::IrPackageFile,
        pb::ArtifactType::IrDelayInfoFile => model::ArtifactType::IrDelayInfoFile,
        pb::ArtifactType::AigFile => model::ArtifactType::AigFile,
        pb::ArtifactType::VerilogFile => model::ArtifactType::VerilogFile,
        pb::ArtifactType::AigStatsFile => model::ArtifactType::AigStatsFile,
        pb::ArtifactType::AigStatDiffFile => model::ArtifactType::AigStatDiffFile,
        pb::ArtifactType::EquivReportFile => model::ArtifactType::EquivReportFile,
    };
    let relpath = required(&value.relpath, "artifact_ref.relpath")?
        .value
        .clone();
    if relpath.is_empty()
        || relpath.starts_with('/')
        || relpath
            .split('/')
            .any(|p| p.is_empty() || p == "." || p == "..")
    {
        bail!("artifact_ref.relpath is not a normalized relative path: {relpath:?}");
    }
    Ok(model::ArtifactRef {
        action_id: action_id_to_hex(
            required(&value.action_id, "artifact_ref.action_id")?,
            "artifact_ref.action_id",
        )?,
        artifact_type,
        relpath,
    })
}

fn encode<M: Message>(value: &M) -> Vec<u8> {
    value.encode_to_vec()
}

pub(crate) fn encode_queue_item(value: &model::QueueItem) -> Result<Vec<u8>> {
    validate_record_version(value.schema_version, "queue_pending.record_version")?;
    Ok(encode(&pb::QueuePendingRecord {
        record_version: QUEUE_RECORD_VERSION,
        action_id: Some(action_id_to_proto(
            &value.action_id,
            "queue_pending.action_id",
        )?),
        enqueued_at: Some(timestamp_to_proto(&value.enqueued_utc)),
        priority: value.priority,
        action: Some(action_spec_to_proto(&value.action)?),
    }))
}

pub(crate) fn decode_queue_item(bytes: &[u8]) -> Result<model::QueueItem> {
    let value = pb::QueuePendingRecord::decode(bytes).context("decoding QueuePendingRecord")?;
    validate_record_version(value.record_version, "queue_pending.record_version")?;
    Ok(model::QueueItem {
        schema_version: value.record_version,
        action_id: action_id_to_hex(
            required(&value.action_id, "queue_pending.action_id")?,
            "queue_pending.action_id",
        )?,
        enqueued_utc: timestamp_from_proto(&value.enqueued_at, "queue_pending.enqueued_at")?,
        priority: value.priority,
        action: action_spec_from_proto(required(&value.action, "queue_pending.action")?)?,
    })
}

pub(crate) fn encode_queue_running(value: &model::QueueRunning) -> Result<Vec<u8>> {
    validate_record_version(value.schema_version, "queue_running.record_version")?;
    Ok(encode(&pb::QueueRunningRecord {
        record_version: QUEUE_RECORD_VERSION,
        action_id: Some(action_id_to_proto(
            &value.action_id,
            "queue_running.action_id",
        )?),
        enqueued_at: Some(timestamp_to_proto(&value.enqueued_utc)),
        priority: value.priority,
        action: Some(action_spec_to_proto(&value.action)?),
        lease_owner: value.lease_owner.clone(),
        lease_acquired_at: Some(timestamp_to_proto(&value.lease_acquired_utc)),
        lease_expires_at: Some(timestamp_to_proto(&value.lease_expires_utc)),
    }))
}

pub(crate) fn decode_queue_running(bytes: &[u8]) -> Result<model::QueueRunning> {
    let value = pb::QueueRunningRecord::decode(bytes).context("decoding QueueRunningRecord")?;
    validate_record_version(value.record_version, "queue_running.record_version")?;
    if value.lease_owner.trim().is_empty() {
        bail!("queue_running.lease_owner must not be empty");
    }
    Ok(model::QueueRunning {
        schema_version: value.record_version,
        action_id: action_id_to_hex(
            required(&value.action_id, "queue_running.action_id")?,
            "queue_running.action_id",
        )?,
        enqueued_utc: timestamp_from_proto(&value.enqueued_at, "queue_running.enqueued_at")?,
        priority: value.priority,
        action: action_spec_from_proto(required(&value.action, "queue_running.action")?)?,
        lease_owner: value.lease_owner,
        lease_acquired_utc: timestamp_from_proto(
            &value.lease_acquired_at,
            "queue_running.lease_acquired_at",
        )?,
        lease_expires_utc: timestamp_from_proto(
            &value.lease_expires_at,
            "queue_running.lease_expires_at",
        )?,
    })
}

pub(crate) fn encode_queue_done(value: &model::QueueDone) -> Result<Vec<u8>> {
    validate_record_version(value.schema_version, "queue_done.record_version")?;
    Ok(encode(&pb::QueueDoneRecord {
        record_version: QUEUE_RECORD_VERSION,
        action_id: Some(action_id_to_proto(
            &value.action_id,
            "queue_done.action_id",
        )?),
        completed_at: Some(timestamp_to_proto(&value.completed_utc)),
        completed_by: value.completed_by.clone(),
        output_artifact: Some(artifact_ref_to_proto(&value.output_artifact)?),
    }))
}

pub(crate) fn decode_queue_done(bytes: &[u8]) -> Result<model::QueueDone> {
    let value = pb::QueueDoneRecord::decode(bytes).context("decoding QueueDoneRecord")?;
    validate_record_version(value.record_version, "queue_done.record_version")?;
    if value.completed_by.trim().is_empty() {
        bail!("queue_done.completed_by must not be empty");
    }
    Ok(model::QueueDone {
        schema_version: value.record_version,
        action_id: action_id_to_hex(
            required(&value.action_id, "queue_done.action_id")?,
            "queue_done.action_id",
        )?,
        completed_utc: timestamp_from_proto(&value.completed_at, "queue_done.completed_at")?,
        completed_by: value.completed_by,
        output_artifact: artifact_ref_from_proto(required(
            &value.output_artifact,
            "queue_done.output_artifact",
        )?)?,
    })
}

pub(crate) fn encode_queue_failed(value: &model::QueueFailed) -> Result<Vec<u8>> {
    validate_record_version(value.schema_version, "queue_failed.record_version")?;
    Ok(encode(&pb::QueueFailedRecord {
        record_version: QUEUE_RECORD_VERSION,
        action_id: Some(action_id_to_proto(
            &value.action_id,
            "queue_failed.action_id",
        )?),
        enqueued_at: Some(timestamp_to_proto(&value.enqueued_utc)),
        failed_at: Some(timestamp_to_proto(&value.failed_utc)),
        failed_by: value.failed_by.clone(),
        action: Some(action_spec_to_proto(&value.action)?),
        error: value.error.clone(),
    }))
}

pub(crate) fn decode_queue_failed(bytes: &[u8]) -> Result<model::QueueFailed> {
    let value = pb::QueueFailedRecord::decode(bytes).context("decoding QueueFailedRecord")?;
    validate_record_version(value.record_version, "queue_failed.record_version")?;
    if value.failed_by.trim().is_empty() || value.error.trim().is_empty() {
        bail!("queue_failed.failed_by and error must not be empty");
    }
    Ok(model::QueueFailed {
        schema_version: value.record_version,
        action_id: action_id_to_hex(
            required(&value.action_id, "queue_failed.action_id")?,
            "queue_failed.action_id",
        )?,
        enqueued_utc: timestamp_from_proto(&value.enqueued_at, "queue_failed.enqueued_at")?,
        failed_utc: timestamp_from_proto(&value.failed_at, "queue_failed.failed_at")?,
        failed_by: value.failed_by,
        action: action_spec_from_proto(required(&value.action, "queue_failed.action")?)?,
        error: value.error,
    })
}

pub(crate) fn encode_queue_canceled(value: &model::QueueCanceled) -> Result<Vec<u8>> {
    validate_record_version(value.schema_version, "queue_canceled.record_version")?;
    validate_cancellation_kind(
        value.cancellation_kind,
        &value.work_policy_rule_id,
        &value.work_policy_rule_fingerprint,
    )?;
    Ok(encode(&pb::QueueCanceledRecord {
        record_version: QUEUE_RECORD_VERSION,
        action_id: Some(action_id_to_proto(
            &value.action_id,
            "queue_canceled.action_id",
        )?),
        enqueued_at: Some(timestamp_to_proto(&value.enqueued_utc)),
        canceled_at: Some(timestamp_to_proto(&value.canceled_utc)),
        canceled_by: value.canceled_by.clone(),
        canceled_due_to_action_id: Some(action_id_to_proto(
            &value.canceled_due_to_action_id,
            "queue_canceled.canceled_due_to_action_id",
        )?),
        root_failed_action_id: Some(action_id_to_proto(
            &value.root_failed_action_id,
            "queue_canceled.root_failed_action_id",
        )?),
        action: Some(action_spec_to_proto(&value.action)?),
        reason: value.reason.clone(),
        cancellation_kind: cancellation_kind_to_proto(value.cancellation_kind) as i32,
        work_policy_rule_id: value.work_policy_rule_id.clone(),
        work_policy_rule_fingerprint: value
            .work_policy_rule_fingerprint
            .as_deref()
            .map(|fingerprint| {
                digest_from_hex(fingerprint, "queue_canceled.work_policy_rule_fingerprint")
            })
            .transpose()?,
    }))
}

pub(crate) fn decode_queue_canceled(bytes: &[u8]) -> Result<model::QueueCanceled> {
    let value = pb::QueueCanceledRecord::decode(bytes).context("decoding QueueCanceledRecord")?;
    validate_record_version(value.record_version, "queue_canceled.record_version")?;
    if value.canceled_by.trim().is_empty() || value.reason.trim().is_empty() {
        bail!("queue_canceled.canceled_by and reason must not be empty");
    }
    let cancellation_kind = cancellation_kind_from_proto(value.cancellation_kind)?;
    let work_policy_rule_fingerprint = value
        .work_policy_rule_fingerprint
        .as_ref()
        .map(|fingerprint| {
            digest_to_hex(fingerprint, "queue_canceled.work_policy_rule_fingerprint")
        })
        .transpose()?;
    validate_cancellation_kind(
        cancellation_kind,
        &value.work_policy_rule_id,
        &work_policy_rule_fingerprint,
    )?;
    Ok(model::QueueCanceled {
        schema_version: value.record_version,
        action_id: action_id_to_hex(
            required(&value.action_id, "queue_canceled.action_id")?,
            "queue_canceled.action_id",
        )?,
        enqueued_utc: timestamp_from_proto(&value.enqueued_at, "queue_canceled.enqueued_at")?,
        canceled_utc: timestamp_from_proto(&value.canceled_at, "queue_canceled.canceled_at")?,
        canceled_by: value.canceled_by,
        canceled_due_to_action_id: action_id_to_hex(
            required(
                &value.canceled_due_to_action_id,
                "queue_canceled.canceled_due_to_action_id",
            )?,
            "queue_canceled.canceled_due_to_action_id",
        )?,
        root_failed_action_id: action_id_to_hex(
            required(
                &value.root_failed_action_id,
                "queue_canceled.root_failed_action_id",
            )?,
            "queue_canceled.root_failed_action_id",
        )?,
        action: action_spec_from_proto(required(&value.action, "queue_canceled.action")?)?,
        reason: value.reason,
        cancellation_kind,
        work_policy_rule_id: value.work_policy_rule_id,
        work_policy_rule_fingerprint,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime() -> model::DriverRuntimeSpec {
        model::DriverRuntimeSpec {
            driver_version: "0.47.0".to_string(),
            release_platform: "ubuntu2004".to_string(),
            docker_image: "xlsynth-bvc-driver:0.47.0".to_string(),
            dockerfile: "docker/xlsynth-driver.Dockerfile".to_string(),
        }
    }

    fn action() -> model::ActionSpec {
        model::ActionSpec::DriverIrToOpt {
            ir_action_id: hex::encode([0x11; 32]),
            top_fn_name: Some("main".to_string()),
            version: "0.47.0".to_string(),
            runtime: runtime(),
        }
    }

    #[test]
    fn all_queue_states_round_trip_binary_protobuf() {
        let action_id = hex::encode([0x22; 32]);
        let now = DateTime::from_timestamp(1_700_000_000, 123_456_789).unwrap();
        let pending = model::QueueItem {
            schema_version: QUEUE_RECORD_VERSION,
            action_id: action_id.clone(),
            enqueued_utc: now,
            priority: 17,
            action: action(),
        };
        assert_eq!(
            decode_queue_item(&encode_queue_item(&pending).unwrap())
                .unwrap()
                .action_id,
            action_id
        );

        let running = model::QueueRunning {
            schema_version: QUEUE_RECORD_VERSION,
            action_id: action_id.clone(),
            enqueued_utc: now,
            priority: 17,
            action: action(),
            lease_owner: "builder-1".to_string(),
            lease_acquired_utc: now,
            lease_expires_utc: now + chrono::Duration::seconds(30),
        };
        assert_eq!(
            decode_queue_running(&encode_queue_running(&running).unwrap())
                .unwrap()
                .lease_owner,
            "builder-1"
        );

        let done = model::QueueDone {
            schema_version: QUEUE_RECORD_VERSION,
            action_id: action_id.clone(),
            completed_utc: now,
            completed_by: "builder-1".to_string(),
            output_artifact: model::ArtifactRef {
                action_id: action_id.clone(),
                artifact_type: model::ArtifactType::IrPackageFile,
                relpath: "payload/result.ir".to_string(),
            },
        };
        assert_eq!(
            decode_queue_done(&encode_queue_done(&done).unwrap())
                .unwrap()
                .output_artifact
                .relpath,
            "payload/result.ir"
        );

        let failed = model::QueueFailed {
            schema_version: QUEUE_RECORD_VERSION,
            action_id: action_id.clone(),
            enqueued_utc: now,
            failed_utc: now,
            failed_by: "builder-1".to_string(),
            action: action(),
            error: "boom".to_string(),
        };
        assert_eq!(
            decode_queue_failed(&encode_queue_failed(&failed).unwrap())
                .unwrap()
                .error,
            "boom"
        );

        let canceled = model::QueueCanceled {
            schema_version: QUEUE_RECORD_VERSION,
            action_id,
            enqueued_utc: now,
            canceled_utc: now,
            canceled_by: "builder-1".to_string(),
            canceled_due_to_action_id: hex::encode([0x33; 32]),
            root_failed_action_id: hex::encode([0x44; 32]),
            action: action(),
            reason: "dependency failed".to_string(),
            cancellation_kind: model::QueueCancellationKind::Dependency,
            work_policy_rule_id: None,
            work_policy_rule_fingerprint: None,
        };
        assert_eq!(
            decode_queue_canceled(&encode_queue_canceled(&canceled).unwrap())
                .unwrap()
                .reason,
            "dependency failed"
        );
    }

    #[test]
    fn rejects_truncated_missing_and_wrong_version_records() {
        assert!(decode_queue_item(&[0x0a, 0x80]).is_err());
        let missing = pb::QueuePendingRecord {
            record_version: QUEUE_RECORD_VERSION,
            ..Default::default()
        };
        assert!(decode_queue_item(&missing.encode_to_vec()).is_err());
        let wrong_version = pb::QueuePendingRecord {
            record_version: 99,
            ..Default::default()
        };
        assert!(decode_queue_item(&wrong_version.encode_to_vec()).is_err());
    }

    #[test]
    fn work_policy_cancellation_requires_a_valid_rule_fingerprint() {
        let now = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let mut canceled = model::QueueCanceled {
            schema_version: QUEUE_RECORD_VERSION,
            action_id: hex::encode([0x22; 32]),
            enqueued_utc: now,
            canceled_utc: now,
            canceled_by: "campaign-work-policy".to_string(),
            canceled_due_to_action_id: hex::encode([0x33; 32]),
            root_failed_action_id: hex::encode([0x22; 32]),
            action: action(),
            reason: "reviewed exclusion".to_string(),
            cancellation_kind: model::QueueCancellationKind::WorkPolicyExcluded,
            work_policy_rule_id: Some("rule-1".to_string()),
            work_policy_rule_fingerprint: None,
        };
        assert!(encode_queue_canceled(&canceled).is_err());
        canceled.work_policy_rule_fingerprint = Some("not-a-digest".to_string());
        assert!(encode_queue_canceled(&canceled).is_err());
        canceled.work_policy_rule_fingerprint = Some(hex::encode([0x44; 32]));
        let decoded = decode_queue_canceled(
            &encode_queue_canceled(&canceled).expect("encode complete policy evidence"),
        )
        .expect("decode complete policy evidence");
        assert_eq!(
            decoded.work_policy_rule_fingerprint,
            canceled.work_policy_rule_fingerprint
        );
    }
}
