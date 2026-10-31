// SPDX-License-Identifier: Apache-2.0

use anyhow::{Result, anyhow};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Mutex, TryLockError};
use std::time::{Duration, Instant};

use super::{Provenance, semantic_input_hash};

/// Candidate identities only: callers must load and validate current provenance.
#[derive(Debug, Default)]
pub(super) struct SemanticCandidateIndex {
    state: Mutex<State>,
    refresh: Mutex<()>,
}

impl SemanticCandidateIndex {
    pub(super) fn candidates(
        &self,
        hash: &str,
        max_age: Duration,
        current_hash: impl Fn(&str) -> Result<Option<String>>,
        load: impl FnOnce(&mut Index) -> Result<usize>,
    ) -> Result<Vec<String>> {
        self.candidates_at(hash, max_age, Instant::now(), current_hash, load)
    }

    pub(super) fn update(
        &self,
        action_id: &str,
        current_hash: impl FnOnce() -> Result<Option<String>>,
    ) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.loaded_at.is_none() && state.updates.is_none() {
            return Ok(());
        }
        state.touch(action_id);
        // Order point reads with other changes to the live index.
        let hash = current_hash().inspect_err(|_| state.loaded_at = None)?;
        state.index.record(action_id, hash.as_deref());
        Ok(())
    }

    pub(super) fn read_current(
        &self,
        action_id: &str,
        load: impl Fn() -> Result<Option<Provenance>>,
    ) -> Result<Option<Provenance>> {
        let observed = load()?;
        let mut state = self.state.lock().unwrap();
        state.touch(action_id);
        if state.index.by_action.get(action_id).map(String::as_str)
            == observed.as_ref().and_then(semantic_input_hash)
        {
            return Ok(observed);
        }
        // A write or refresh may have overtaken the observation before we took the lock.
        let current = load().inspect_err(|_| state.loaded_at = None)?;
        state
            .index
            .record(action_id, current.as_ref().and_then(semantic_input_hash));
        Ok(current)
    }

    fn candidates_at(
        &self,
        hash: &str,
        max_age: Duration,
        now: Instant,
        current_hash: impl Fn(&str) -> Result<Option<String>>,
        load: impl FnOnce(&mut Index) -> Result<usize>,
    ) -> Result<Vec<String>> {
        {
            let state = self.state.lock().unwrap();
            if state
                .loaded_at
                .is_some_and(|at| now.saturating_duration_since(at) < max_age)
            {
                return Ok(state.index.candidates(hash));
            }
        }
        // A warm reader can use current identities while one caller refreshes.
        // Fresh provenance validation still happens outside this cache.
        let _refresh = match self.refresh.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => {
                let state = self.state.lock().unwrap();
                if state.loaded_at.is_some() {
                    return Ok(state.index.candidates(hash));
                }
                drop(state);
                self.refresh
                    .lock()
                    .map_err(|_| anyhow!("semantic index refresh poisoned"))?
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(anyhow!("semantic index refresh poisoned"));
            }
        };
        {
            let mut state = self.state.lock().unwrap();
            if state
                .loaded_at
                .is_some_and(|at| now.saturating_duration_since(at) < max_age)
            {
                return Ok(state.index.candidates(hash));
            }
            state.updates = Some(HashSet::new());
        }
        let started = Instant::now();
        let mut index = Index::default();
        let loaded = load(&mut index);
        let mut state = self.state.lock().unwrap();
        // Either the scan or a concurrent update may have read the later bytes.
        // Reread conflicting mappings before publishing the new index.
        let updates = state.updates.take().expect("active refresh");
        let rows = loaded?;
        for action_id in updates {
            if state.loaded_at.is_some()
                && index.by_action.get(&action_id) == state.index.by_action.get(&action_id)
            {
                continue;
            }
            let hash = current_hash(&action_id)?;
            index.record(&action_id, hash.as_deref());
        }
        log::debug!(target: "bvc_timing", "semantic_index_refresh rows={} candidates={} elapsed_us={}",
            rows, index.by_action.len(), started.elapsed().as_micros());
        // Only a complete refresh renews the deadline for discovering external additions.
        state.loaded_at = Some(now);
        let retired = std::mem::replace(&mut state.index, index);
        let ids = state.index.candidates(hash);
        drop(state);
        drop(retired);
        Ok(ids)
    }
}

#[derive(Debug, Default)]
struct State {
    index: Index,
    loaded_at: Option<Instant>,
    updates: Option<HashSet<String>>,
}

impl State {
    fn touch(&mut self, action_id: &str) {
        if let Some(updates) = &mut self.updates {
            updates.insert(action_id.to_owned());
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct Index {
    by_hash: BTreeMap<String, BTreeSet<String>>,
    by_action: HashMap<String, String>,
}

impl Index {
    pub(super) fn record(&mut self, action_id: &str, hash: Option<&str>) {
        if self.by_action.get(action_id).map(String::as_str) == hash {
            return;
        }
        if let Some(previous) = self.by_action.remove(action_id) {
            let ids = self.by_hash.get_mut(&previous).expect("indexed hash");
            ids.remove(action_id);
            if ids.is_empty() {
                self.by_hash.remove(&previous);
            }
        }
        if let Some(hash) = hash {
            self.by_hash
                .entry(hash.to_owned())
                .or_default()
                .insert(action_id.to_owned());
            self.by_action.insert(action_id.to_owned(), hash.to_owned());
        }
    }

    fn candidates(&self, hash: &str) -> Vec<String> {
        self.by_hash
            .get(hash)
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, mpsc};

    fn no_updates(id: &str) -> Result<Option<String>> {
        panic!("unexpected point read for {id}");
    }

    fn record(cache: &SemanticCandidateIndex, id: &str, hash: Option<&str>) {
        cache.update(id, || Ok(hash.map(str::to_owned))).unwrap();
    }

    #[test]
    fn appends_and_replacements_do_not_rebuild_or_renew_external_refresh() {
        let cache = SemanticCandidateIndex::default();
        let now = Instant::now();
        let ttl = Duration::from_secs(60);
        assert!(
            cache
                .candidates_at("hash", ttl, now, no_updates, |_| Ok(0))
                .unwrap()
                .is_empty()
        );
        for number in (0..100).rev() {
            record(&cache, &format!("{number:04}"), Some("hash"));
        }
        record(&cache, "0001", Some("other"));
        record(&cache, "0002", None);
        let ids = cache
            .candidates_at(
                "hash",
                ttl,
                now + Duration::from_secs(59),
                no_updates,
                |_| panic!("local writes must not rebuild"),
            )
            .unwrap();
        assert_eq!(ids.len(), 98);
        assert_eq!(ids[0], "0000");
        assert_eq!(ids[1], "0003");
        let ids = cache
            .candidates_at("external", ttl, now + ttl, no_updates, |index| {
                index.record("external-id", Some("external"));
                Ok(1)
            })
            .unwrap();
        assert_eq!(ids, ["external-id"]);
    }

    #[test]
    fn refresh_keeps_a_replacement_scanned_after_a_warm_observation() {
        let cache = SemanticCandidateIndex::default();
        let now = Instant::now();
        let ttl = Duration::from_secs(60);
        cache
            .candidates_at("original", ttl, now, no_updates, |index| {
                index.record("id", Some("original"));
                Ok(1)
            })
            .unwrap();
        let ids = cache
            .candidates_at(
                "latest",
                ttl,
                now + ttl,
                |id| {
                    assert_eq!(id, "id");
                    Ok(Some("latest".into()))
                },
                |index| {
                    // A warm reader observes a deletion before the scan sees a replacement.
                    assert!(cache.read_current("id", || Ok(None))?.is_none());
                    index.record("id", Some("latest"));
                    Ok(1)
                },
            )
            .unwrap();
        assert_eq!(ids, ["id"]);
        assert!(
            cache
                .state
                .lock()
                .unwrap()
                .index
                .candidates("original")
                .is_empty()
        );
    }

    #[test]
    fn cold_writes_skip_point_reads_and_a_failed_update_expires_the_index() {
        let cache = SemanticCandidateIndex::default();
        let now = Instant::now();
        let ttl = Duration::from_secs(60);
        cache.update("id", || panic!("cold index")).unwrap();
        cache
            .candidates_at("hash", ttl, now, no_updates, |_| Ok(0))
            .unwrap();
        assert!(cache.update("id", || Err(anyhow!("read failure"))).is_err());
        let ids = cache
            .candidates_at("hash", ttl, now, no_updates, |index| {
                index.record("id", Some("hash"));
                Ok(1)
            })
            .unwrap();
        assert_eq!(ids, ["id"]);
    }

    #[test]
    fn refresh_merges_concurrent_writes_and_warm_readers_do_not_wait() {
        let cache = Arc::new(SemanticCandidateIndex::default());
        let now = Instant::now();
        let ttl = Duration::from_secs(60);
        cache
            .candidates_at("a", ttl, now, no_updates, |index| {
                index.record("old", Some("a"));
                Ok(1)
            })
            .unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let thread_cache = cache.clone();
        let thread = std::thread::spawn(move || {
            thread_cache
                .candidates_at(
                    "b",
                    ttl,
                    now + ttl,
                    |id| {
                        assert!(matches!(id, "old" | "new"));
                        Ok(Some("b".into()))
                    },
                    |index| {
                        index.record("old", Some("a"));
                        entered_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        Ok(1)
                    },
                )
                .unwrap()
        });
        entered_rx.recv().unwrap();
        record(&cache, "old", Some("b"));
        record(&cache, "new", Some("b"));
        assert_eq!(
            cache
                .candidates_at("b", ttl, now + ttl, no_updates, |_| panic!(
                    "single refresher"
                ))
                .unwrap(),
            ["new", "old"]
        );
        release_tx.send(()).unwrap();
        assert_eq!(thread.join().unwrap(), ["new", "old"]);
    }

    #[test]
    fn failed_refresh_preserves_local_writes_and_retries() {
        let cache = SemanticCandidateIndex::default();
        let now = Instant::now();
        let ttl = Duration::from_secs(60);
        cache
            .candidates_at("a", ttl, now, no_updates, |_| Ok(0))
            .unwrap();
        assert!(
            cache
                .candidates_at("a", ttl, now + ttl, no_updates, |_| {
                    record(&cache, "id", Some("a"));
                    Err(anyhow!("read failure"))
                })
                .is_err()
        );
        assert_eq!(cache.state.lock().unwrap().index.candidates("a"), ["id"]);
        assert_eq!(
            cache
                .candidates_at("a", ttl, now + ttl, no_updates, |index| {
                    index.record("id", Some("a"));
                    Ok(1)
                })
                .unwrap(),
            ["id"]
        );
    }
}
