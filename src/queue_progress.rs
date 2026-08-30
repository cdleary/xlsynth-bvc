// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use crate::proto::{decode_queue_done, decode_queue_running};
use crate::query::{action_is_expander, action_kind_label, action_subject, artifact_type_label};
use crate::queue::{list_queue_files, parse_queue_work_item};

#[derive(Debug, Clone)]
pub(crate) struct WatchQueueOptions {
    pub(crate) interval: Duration,
    pub(crate) running_limit: usize,
    pub(crate) once: bool,
    pub(crate) exit_when_idle: bool,
    pub(crate) json: bool,
}

pub(crate) fn watch_interval(interval_seconds: f64) -> Result<Duration> {
    if !interval_seconds.is_finite() || interval_seconds <= 0.0 {
        bail!("--interval-seconds must be finite and greater than zero");
    }
    Ok(Duration::from_secs_f64(interval_seconds))
}

#[derive(Debug, Clone, Serialize)]
struct QueueProgressSnapshot {
    updated_utc: DateTime<Utc>,
    pending: usize,
    pending_expanders: usize,
    pending_actions: Vec<ActionKindProgress>,
    running: usize,
    running_expanders: usize,
    done: usize,
    canceled: usize,
    pending_is_lower_bound: bool,
    malformed_records: usize,
    completions_last_minute: usize,
    completion_bins_last_ten_minutes: Vec<usize>,
    completed_artifacts: Vec<CompletedArtifactProgress>,
    running_actions: Vec<RunningActionProgress>,
}

#[derive(Debug, Clone, Serialize)]
struct CompletedArtifactProgress {
    artifact_type: String,
    count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct ActionKindProgress {
    action_kind: String,
    count: usize,
}

#[derive(Debug, Default)]
struct DoneProgressCache {
    records: HashMap<PathBuf, DoneProgressRecord>,
}

#[derive(Debug)]
struct DoneProgressRecord {
    completed_utc: DateTime<Utc>,
    artifact_type: String,
}

#[derive(Debug, Clone, Serialize)]
struct RunningActionProgress {
    action_id: String,
    action_kind: String,
    subject: String,
    driver_version: Option<String>,
    lease_owner: String,
    running_seconds: i64,
    lease_expires_utc: DateTime<Utc>,
}

pub(crate) fn watch_queue(store_dir: &Path, options: &WatchQueueOptions) -> Result<()> {
    if options.interval.is_zero() {
        bail!("--interval-seconds must be greater than zero");
    }
    if options.running_limit == 0 {
        bail!("--running-limit must be greater than zero");
    }

    let terminal = io::stdout().is_terminal() && !options.json;
    let mut done_cache = DoneProgressCache::default();
    loop {
        let snapshot = read_queue_progress(store_dir, &mut done_cache)?;
        if terminal {
            print!("\x1b[2J\x1b[H");
        }
        if options.json {
            println!(
                "{}",
                serde_json::to_string(&snapshot).expect("serializing queue progress snapshot")
            );
        } else {
            print!(
                "{}",
                render_queue_progress(&snapshot, options.running_limit)
            );
        }
        io::stdout().flush().context("flushing queue progress")?;

        let idle = snapshot.pending == 0 && snapshot.running == 0;
        if options.once || (options.exit_when_idle && idle) {
            return Ok(());
        }
        thread::sleep(options.interval);
    }
}

fn read_queue_progress(
    store_dir: &Path,
    done_cache: &mut DoneProgressCache,
) -> Result<QueueProgressSnapshot> {
    let queue_root = store_dir.join("queue");
    let pending_paths = list_queue_files(&queue_root.join("pending"))?;
    let running_paths = list_queue_files(&queue_root.join("running"))?;
    let done_paths = list_queue_files(&queue_root.join("done"))?;
    let done = done_paths.len();
    let canceled = list_queue_files(&queue_root.join("canceled"))?.len();

    let mut malformed_records = done_cache.refresh(&done_paths)?;
    let mut pending_expanders = 0_usize;
    let mut pending_action_counts = BTreeMap::<String, usize>::new();
    for path in &pending_paths {
        match read_pending_action(path) {
            Ok(action) => {
                if action_is_expander(&action) {
                    pending_expanders += 1;
                }
                *pending_action_counts
                    .entry(action_kind_label(&action).to_string())
                    .or_default() += 1;
            }
            Err(_) => malformed_records += 1,
        }
    }
    let mut pending_actions = pending_action_counts
        .into_iter()
        .map(|(action_kind, count)| ActionKindProgress { action_kind, count })
        .collect::<Vec<_>>();
    pending_actions.sort_by(|a, b| {
        b.count
            .cmp(&a.count)
            .then(a.action_kind.cmp(&b.action_kind))
    });

    let now = Utc::now();
    let mut running_expanders = 0_usize;
    let mut running_actions = Vec::new();
    for path in &running_paths {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("reading running queue record: {}", path.display()));
            }
        };
        let running = match decode_queue_running(&bytes) {
            Ok(running) => running,
            Err(_) => {
                malformed_records += 1;
                continue;
            }
        };
        if action_is_expander(&running.action) {
            running_expanders += 1;
        }
        let driver_version =
            crate::query::action_driver_version(&running.action).map(ToOwned::to_owned);
        running_actions.push(RunningActionProgress {
            action_id: running.action_id,
            action_kind: action_kind_label(&running.action).to_string(),
            subject: action_subject(&running.action),
            driver_version,
            lease_owner: running.lease_owner,
            running_seconds: now
                .signed_duration_since(running.lease_acquired_utc)
                .num_seconds()
                .max(0),
            lease_expires_utc: running.lease_expires_utc,
        });
    }
    running_actions.sort_by(|a, b| {
        b.running_seconds
            .cmp(&a.running_seconds)
            .then(a.action_id.cmp(&b.action_id))
    });

    let (completions_last_minute, completion_bins_last_ten_minutes) =
        done_cache.completion_activity(now);
    let completed_artifacts = done_cache.completed_artifacts();

    Ok(QueueProgressSnapshot {
        updated_utc: now,
        pending: pending_paths.len(),
        pending_expanders,
        pending_actions,
        running: running_paths.len(),
        running_expanders,
        done,
        canceled,
        pending_is_lower_bound: !pending_paths.is_empty() || !running_paths.is_empty(),
        malformed_records,
        completions_last_minute,
        completion_bins_last_ten_minutes,
        completed_artifacts,
        running_actions,
    })
}

impl DoneProgressCache {
    fn refresh(&mut self, done_paths: &[PathBuf]) -> Result<usize> {
        let present = done_paths
            .iter()
            .map(PathBuf::as_path)
            .collect::<HashSet<_>>();
        self.records
            .retain(|path, _| present.contains(path.as_path()));

        let mut malformed_records = 0_usize;
        for path in done_paths {
            if self.records.contains_key(path) {
                continue;
            }
            let bytes = match fs::read(path) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("reading done queue record: {}", path.display()));
                }
            };
            let done = match decode_queue_done(&bytes) {
                Ok(done) => done,
                Err(_) => {
                    malformed_records += 1;
                    continue;
                }
            };
            self.records.insert(
                path.clone(),
                DoneProgressRecord {
                    completed_utc: done.completed_utc,
                    artifact_type: artifact_type_label(&done.output_artifact.artifact_type)
                        .to_string(),
                },
            );
        }
        Ok(malformed_records)
    }

    fn completion_activity(&self, now: DateTime<Utc>) -> (usize, Vec<usize>) {
        let mut bins = vec![0_usize; 10];
        for record in self.records.values() {
            let age_seconds = now
                .signed_duration_since(record.completed_utc)
                .num_seconds();
            if !(0..600).contains(&age_seconds) {
                continue;
            }
            let newest_first = (age_seconds / 60) as usize;
            bins[9 - newest_first] += 1;
        }
        (bins[9], bins)
    }

    fn completed_artifacts(&self) -> Vec<CompletedArtifactProgress> {
        let mut counts = BTreeMap::<String, usize>::new();
        for record in self.records.values() {
            *counts.entry(record.artifact_type.clone()).or_default() += 1;
        }
        let mut artifacts = counts
            .into_iter()
            .map(|(artifact_type, count)| CompletedArtifactProgress {
                artifact_type,
                count,
            })
            .collect::<Vec<_>>();
        artifacts.sort_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then(a.artifact_type.cmp(&b.artifact_type))
        });
        artifacts
    }
}

fn read_pending_action(path: &Path) -> Result<crate::model::ActionSpec> {
    let bytes = fs::read(path)
        .with_context(|| format!("reading pending queue record: {}", path.display()))?;
    let (_, _, _, action) = parse_queue_work_item(&bytes, path)?;
    Ok(action)
}

fn render_queue_progress(snapshot: &QueueProgressSnapshot, running_limit: usize) -> String {
    let visible_total = snapshot.pending + snapshot.running + snapshot.done + snapshot.canceled;
    let terminal = snapshot.done + snapshot.canceled;
    let completed_percent = if visible_total == 0 {
        100.0
    } else {
        terminal as f64 * 100.0 / visible_total as f64
    };
    let mut out = String::new();
    out.push_str(&format!(
        "xlsynth-bvc queue  {}\n\n",
        snapshot
            .updated_utc
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    ));
    out.push_str(&format!(
        "pending {:>6}  running {:>3}  done {:>6}  canceled {:>4}  visible terminal {:>5.1}%\n",
        snapshot.pending, snapshot.running, snapshot.done, snapshot.canceled, completed_percent,
    ));
    out.push_str(&format!(
        "expanders: pending {} / running {}",
        snapshot.pending_expanders, snapshot.running_expanders
    ));
    if snapshot.malformed_records > 0 {
        out.push_str(&format!(
            "  malformed records: {}",
            snapshot.malformed_records
        ));
    }
    out.push_str("\nVisible totals can grow as completed actions enqueue follow-on work.\n\n");
    if !snapshot.pending_actions.is_empty() {
        out.push_str("Queued action kinds\n");
        for action in snapshot.pending_actions.iter().take(6) {
            out.push_str(&format!(
                "  {:<38} {:>7}\n",
                action.action_kind, action.count
            ));
        }
        out.push('\n');
    }
    out.push_str(&format!(
        "Completion activity  {:>4}/min  {}  (oldest → newest, 10m)\n",
        snapshot.completions_last_minute,
        sparkline(&snapshot.completion_bins_last_ten_minutes)
    ));
    if !snapshot.completed_artifacts.is_empty() {
        out.push_str("Completed output artifacts\n");
        for artifact in snapshot.completed_artifacts.iter().take(6) {
            out.push_str(&format!(
                "  {:<24} {:>7}\n",
                artifact.artifact_type, artifact.count
            ));
        }
    }
    out.push('\n');

    if snapshot.running_actions.is_empty() {
        out.push_str("No actions are currently running.\n");
    } else {
        out.push_str("Running actions\n");
        for action in snapshot.running_actions.iter().take(running_limit) {
            let version = action
                .driver_version
                .as_deref()
                .map(|value| format!(" driver={value}"))
                .unwrap_or_default();
            out.push_str(&format!(
                "  {}  {:<38} {:>6}s{}  {}\n",
                short_id(&action.action_id),
                action.action_kind,
                action.running_seconds,
                version,
                truncate(&action.subject, 72)
            ));
        }
        let hidden = snapshot.running_actions.len().saturating_sub(running_limit);
        if hidden > 0 {
            out.push_str(&format!("  ... and {hidden} more\n"));
        }
    }
    out.push_str(
        "\nNote: failed-action payloads may live in the locked artifact database and are not counted by this attach-safe view.\n",
    );
    out
}

fn short_id(value: &str) -> &str {
    value.get(..12).unwrap_or(value)
}

fn truncate(value: &str, maximum_chars: usize) -> String {
    if value.chars().count() <= maximum_chars {
        return value.to_string();
    }
    let mut result = value
        .chars()
        .take(maximum_chars.saturating_sub(1))
        .collect::<String>();
    result.push('…');
    result
}

fn sparkline(values: &[usize]) -> String {
    const BLOCKS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    let maximum = values.iter().copied().max().unwrap_or(0);
    if maximum == 0 {
        return "▁".repeat(values.len());
    }
    values
        .iter()
        .map(|value| {
            let index = value.saturating_mul(BLOCKS.len() - 1) / maximum;
            BLOCKS[index]
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watch_interval_rejects_values_that_duration_cannot_construct() {
        for value in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(watch_interval(value).is_err(), "accepted {value}");
        }
        assert_eq!(
            watch_interval(2.5).expect("interval"),
            Duration::from_millis(2500)
        );
    }

    #[test]
    fn done_progress_cache_evicts_records_absent_from_latest_scan() {
        let mut cache = DoneProgressCache::default();
        cache.records.insert(
            PathBuf::from("removed.pb"),
            DoneProgressRecord {
                completed_utc: Utc::now(),
                artifact_type: "aig_file".to_string(),
            },
        );

        cache.refresh(&[]).expect("refresh empty done set");

        assert!(cache.records.is_empty());
        assert!(cache.completed_artifacts().is_empty());
    }

    #[test]
    fn render_queue_progress_labels_lower_bound_and_running_actions() {
        let now = Utc::now();
        let snapshot = QueueProgressSnapshot {
            updated_utc: now,
            pending: 7,
            pending_expanders: 1,
            pending_actions: vec![ActionKindProgress {
                action_kind: "driver_ir_to_g8r_aig".to_string(),
                count: 7,
            }],
            running: 1,
            running_expanders: 0,
            done: 12,
            canceled: 2,
            pending_is_lower_bound: true,
            malformed_records: 0,
            completions_last_minute: 42,
            completion_bins_last_ten_minutes: vec![1, 2, 4, 8, 4, 2, 1, 3, 6, 9],
            completed_artifacts: vec![CompletedArtifactProgress {
                artifact_type: "aig_file".to_string(),
                count: 10,
            }],
            running_actions: vec![RunningActionProgress {
                action_id: "1234567890abcdef".to_string(),
                action_kind: "driver_ir_to_g8r_aig".to_string(),
                subject: "float32.x::add".to_string(),
                driver_version: Some("0.68.0".to_string()),
                lease_owner: "worker".to_string(),
                running_seconds: 17,
                lease_expires_utc: now,
            }],
        };
        let rendered = render_queue_progress(&snapshot, 8);
        assert!(rendered.contains("pending      7"));
        assert!(rendered.contains("can grow as completed actions enqueue follow-on work"));
        assert!(rendered.contains("Queued action kinds"));
        assert!(rendered.contains("1234567890ab"));
        assert!(rendered.contains("driver=0.68.0"));
        assert!(rendered.contains("float32.x::add"));
        assert!(rendered.contains("42/min"));
        assert!(rendered.contains("aig_file"));
    }

    #[test]
    fn truncate_adds_ellipsis_only_when_needed() {
        assert_eq!(truncate("short", 6), "short");
        assert_eq!(truncate("seven!!", 6), "seven…");
    }

    #[test]
    fn sparkline_scales_values_to_blocks() {
        assert_eq!(sparkline(&[0, 4, 8]), "▁▄█");
        assert_eq!(sparkline(&[0, 0]), "▁▁");
    }
}
