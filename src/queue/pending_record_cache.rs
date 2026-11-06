// SPDX-License-Identifier: Apache-2.0

//! Memoized decoding of freshly read queue bytes; never an authority for leases.

use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::{PendingPaths, parse_queue_work_item};
use crate::model::ActionSpec;

/// Immutable validated metadata shared by scans of an identical record.
#[derive(Debug)]
pub(super) struct PendingRecord {
    bytes: Box<[u8]>,
    pub action_id: String,
    pub enqueued_utc: DateTime<Utc>,
    pub priority: i32,
    pub action: ActionSpec,
    pub canonical_id: String,
}

impl PendingRecord {
    fn cache_charge(&self, path: &Path) -> usize {
        // Charge the wire bytes plus an allowance for decoded strings (digests expand to hex).
        (size_of::<(PathBuf, Arc<Self>, Self)>() + path.as_os_str().len() + self.canonical_id.len())
            .saturating_add(self.bytes.len().saturating_mul(3))
    }
}

#[derive(Debug, Default)]
struct RecordShard {
    records: HashMap<PathBuf, Arc<PendingRecord>>,
    charge: usize,
}

/// Shards share a fixed admission budget. Rotating scans keep getting hits on
/// admitted paths even when the whole pending queue cannot fit. Reconciliation
/// frees space as those paths leave the queue.
#[derive(Debug)]
pub(super) struct PendingRecordCache {
    shards: Vec<Mutex<RecordShard>>,
    shard_budget: usize,
}

impl Default for PendingRecordCache {
    fn default() -> Self {
        let count = std::thread::available_parallelism().map_or(1, |count| count.get());
        Self {
            shards: (0..count).map(|_| Mutex::default()).collect(),
            shard_budget: Self::BUDGET_BYTES / count,
        }
    }
}

impl PendingRecordCache {
    const BUDGET_BYTES: usize = 64 * 1024 * 1024;

    pub fn decode(&self, path: &Path, bytes: Vec<u8>) -> Result<Arc<PendingRecord>> {
        let mut hash = DefaultHasher::new();
        path.hash(&mut hash);
        let shard = &self.shards[hash.finish() as usize % self.shards.len()];
        let cached = shard.lock().unwrap().records.get(path).cloned();
        if let Some(cached) = cached {
            if cached.bytes.as_ref() == bytes.as_slice() {
                return Ok(cached);
            }
        }
        let (action_id, enqueued_utc, priority, action) = parse_queue_work_item(&bytes, path)?;
        let action = crate::executor::canonicalize_action_identity(action);
        let canonical_id = crate::executor::compute_action_id(&action)?;
        let record = Arc::new(PendingRecord {
            bytes: bytes.into_boxed_slice(),
            action_id,
            enqueued_utc,
            priority,
            action,
            canonical_id,
        });
        let charge = record.cache_charge(path);
        let mut shard = shard.lock().unwrap();
        let retired = shard.records.remove_entry(path);
        if let Some((old_path, old)) = &retired {
            shard.charge -= old.cache_charge(old_path);
        }
        if charge <= self.shard_budget - shard.charge {
            shard.records.insert(path.to_owned(), record.clone());
            shard.charge += charge;
        }
        drop(shard);
        drop(retired);
        Ok(record)
    }

    /// Bound retained entries to the reconciled pending snapshot. Concurrent
    /// inserts missed by the snapshot may be evicted, which only causes a reparse.
    pub fn retain(&self, pending: &PendingPaths) {
        for shard in &self.shards {
            let mut retired = Vec::new();
            let mut shard = shard.lock().unwrap();
            let RecordShard { records, charge } = &mut *shard;
            records.retain(|path, record| {
                let keep = pending.snapshot.binary_search(path).is_ok()
                    || pending.additions.binary_search(path).is_ok();
                if !keep {
                    *charge -= record.cache_charge(path);
                    retired.push(record.clone());
                }
                keep
            });
            drop(shard);
            drop(retired);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::QueueItem;

    fn item() -> QueueItem {
        let action = ActionSpec::ImportIrPackageFile {
            source_sha256: "11".repeat(32),
            top_fn_name: Some("main".into()),
        };
        QueueItem {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: crate::executor::compute_action_id(&action).unwrap(),
            enqueued_utc: Utc::now(),
            priority: 0,
            action,
        }
    }

    #[test]
    fn unchanged_bytes_share_metadata_but_replacement_and_corruption_are_observed() {
        let cache = PendingRecordCache::default();
        let path = Path::new("queue/pending/test.pb");
        let mut item = item();
        let bytes = crate::proto::encode_queue_item(&item).unwrap();
        let first = cache.decode(path, bytes.clone()).unwrap();
        assert!(Arc::ptr_eq(&first, &cache.decode(path, bytes).unwrap()));
        item.priority = 19;
        let replaced = cache
            .decode(path, crate::proto::encode_queue_item(&item).unwrap())
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &replaced));
        assert_eq!(replaced.priority, 19);
        assert_eq!(replaced.canonical_id, first.canonical_id);
        assert!(cache.decode(path, b"corrupt".to_vec()).is_err());
    }

    #[test]
    fn full_shard_decodes_misses_and_reclaims_space_on_removal_or_replacement() {
        let mut cache = PendingRecordCache {
            shards: vec![Mutex::default()],
            shard_budget: usize::MAX,
        };
        let one = Path::new("one.pb");
        let two = Path::new("two.pb");
        let mut item = item();
        let bytes = crate::proto::encode_queue_item(&item).unwrap();
        let first = cache.decode(one, bytes.clone()).unwrap();
        cache.shard_budget = first.cache_charge(one);
        let miss = cache.decode(two, bytes.clone()).unwrap();
        assert_eq!(miss.canonical_id, first.canonical_id);
        assert!(!Arc::ptr_eq(
            &miss,
            &cache.decode(two, bytes.clone()).unwrap()
        ));
        assert!(Arc::ptr_eq(
            &first,
            &cache.decode(one, bytes.clone()).unwrap()
        ));

        cache.retain(&PendingPaths {
            snapshot: Arc::new(vec![two.to_owned()]),
            additions: Vec::new(),
        });
        let second = cache.decode(two, bytes.clone()).unwrap();
        assert!(Arc::ptr_eq(
            &second,
            &cache.decode(two, bytes.clone()).unwrap()
        ));

        // A larger valid replacement still reaches the caller and retires the old entry.
        item.priority = 1;
        let larger = crate::proto::encode_queue_item(&item).unwrap();
        assert!(larger.len() > bytes.len());
        assert_eq!(cache.decode(two, larger).unwrap().priority, 1);
        let readmitted = cache.decode(one, bytes.clone()).unwrap();
        assert!(Arc::ptr_eq(&readmitted, &cache.decode(one, bytes).unwrap()));
    }

    #[test]
    fn concurrent_replacements_return_the_metadata_for_each_callers_bytes() {
        let cache = Arc::new(PendingRecordCache::default());
        let item = item();
        let threads: Vec<_> = (0..8)
            .map(|priority| {
                let cache = cache.clone();
                let mut item = item.clone();
                std::thread::spawn(move || {
                    item.priority = priority;
                    let bytes = crate::proto::encode_queue_item(&item).unwrap();
                    for _ in 0..32 {
                        let record = cache.decode(Path::new("shared.pb"), bytes.clone()).unwrap();
                        assert_eq!(record.priority, priority);
                        assert_eq!(record.action_id, item.action_id);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
    }

    #[test]
    fn reconciliation_retires_only_absent_records_and_preserves_live_readers() {
        let cache = PendingRecordCache::default();
        let bytes = crate::proto::encode_queue_item(&item()).unwrap();
        let old = PathBuf::from("old.pb");
        let added = PathBuf::from("added.pb");
        let first = cache.decode(&old, bytes.clone()).unwrap();
        let second = cache.decode(&added, bytes.clone()).unwrap();
        cache.retain(&PendingPaths {
            snapshot: Arc::new(Vec::new()),
            additions: vec![added.clone()],
        });
        assert!(Arc::ptr_eq(
            &second,
            &cache.decode(&added, bytes.clone()).unwrap()
        ));
        assert!(!Arc::ptr_eq(&first, &cache.decode(&old, bytes).unwrap()));
        assert_eq!(first.canonical_id, second.canonical_id);
    }
}
