// SPDX-License-Identifier: Apache-2.0

use anyhow::{Result, anyhow};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Mutex, TryLockError};
use std::time::{Duration, Instant};

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
        load: impl FnOnce(&mut Index) -> Result<usize>,
    ) -> Result<Vec<String>> {
        self.candidates_at(hash, max_age, Instant::now(), load)
    }

    pub(super) fn read_current(
        &self,
        action_id: &str,
        load: impl FnOnce() -> Result<Option<crate::model::Provenance>>,
    ) -> Result<Option<crate::model::Provenance>> {
        // Serialize the fresh read with index updates so a delayed observer
        // cannot overwrite a newer local replacement's hash mapping.
        let mut state = self
            .state
            .lock()
            .expect("semantic candidate index poisoned");
        let provenance = load()?;
        let hash = provenance.as_ref().and_then(|value| {
            value
                .details
                .get(crate::INPUT_IR_FN_STRUCTURAL_HASH_DETAILS_KEY)
                .and_then(|v| v.as_str())
        });
        state.record(action_id, hash);
        Ok(provenance)
    }

    #[cfg(test)]
    fn record(&self, action_id: &str, hash: Option<&str>) {
        self.state.lock().unwrap().record(action_id, hash);
    }

    fn candidates_at(
        &self,
        hash: &str,
        max_age: Duration,
        now: Instant,
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
            state.refreshing = true;
        }
        let started = Instant::now();
        let mut index = Index::default();
        let loaded = load(&mut index);
        let mut state = self.state.lock().unwrap();
        state.refreshing = false;
        let retired = if let Ok(rows) = loaded.as_ref() {
            for (action_id, hash) in state.updates.drain() {
                index.record(&action_id, hash.as_deref());
            }
            log::debug!(target: "bvc_timing", "semantic_index_refresh rows={} candidates={} elapsed_us={}",
                rows, index.by_action.len(), started.elapsed().as_micros());
            let retired = std::mem::replace(&mut state.index, index);
            // Local writes do not renew this deadline: out-of-band additions
            // are discovered even while normal actions keep completing.
            state.loaded_at = Some(now);
            Some(retired)
        } else {
            // Writes were already applied to the previous index. Keep it on
            // failure and leave its deadline expired so the next reader retries.
            state.updates.clear();
            None
        };
        let result = loaded.map(|_| state.index.candidates(hash));
        drop(state);
        drop(retired);
        result
    }
}

#[derive(Debug, Default)]
struct State {
    index: Index,
    loaded_at: Option<Instant>,
    refreshing: bool,
    updates: HashMap<String, Option<String>>,
}

impl State {
    fn record(&mut self, action_id: &str, hash: Option<&str>) {
        self.index.record(action_id, hash);
        if self.refreshing {
            self.updates
                .insert(action_id.to_owned(), hash.map(str::to_owned));
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

    #[test]
    fn appends_and_replacements_do_not_rebuild_or_renew_external_refresh() {
        let cache = SemanticCandidateIndex::default();
        let now = Instant::now();
        let ttl = Duration::from_secs(60);
        assert!(
            cache
                .candidates_at("hash", ttl, now, |_| Ok(0))
                .unwrap()
                .is_empty()
        );
        for number in (0..100).rev() {
            cache.record(&format!("{number:04}"), Some("hash"));
        }
        cache.record("0001", Some("other"));
        cache.record("0002", None);
        let ids = cache
            .candidates_at("hash", ttl, now + Duration::from_secs(59), |_| {
                panic!("local writes must not rebuild")
            })
            .unwrap();
        assert_eq!(ids.len(), 98);
        assert_eq!(ids[0], "0000");
        assert_eq!(ids[1], "0003");
        let ids = cache
            .candidates_at("external", ttl, now + ttl, |index| {
                index.record("external-id", Some("external"));
                Ok(1)
            })
            .unwrap();
        assert_eq!(ids, ["external-id"]);
    }

    #[test]
    fn refresh_merges_concurrent_writes_and_warm_readers_do_not_wait() {
        let cache = Arc::new(SemanticCandidateIndex::default());
        let now = Instant::now();
        let ttl = Duration::from_secs(60);
        cache
            .candidates_at("a", ttl, now, |index| {
                index.record("old", Some("a"));
                Ok(1)
            })
            .unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let thread_cache = cache.clone();
        let thread = std::thread::spawn(move || {
            thread_cache
                .candidates_at("b", ttl, now + ttl, |index| {
                    index.record("old", Some("a"));
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(1)
                })
                .unwrap()
        });
        entered_rx.recv().unwrap();
        cache.record("old", Some("b"));
        cache.record("new", Some("b"));
        assert_eq!(
            cache
                .candidates_at("b", ttl, now + ttl, |_| panic!("single refresher"))
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
        cache.candidates_at("a", ttl, now, |_| Ok(0)).unwrap();
        assert!(
            cache
                .candidates_at("a", ttl, now + ttl, |_| {
                    cache.record("id", Some("a"));
                    Err(anyhow!("read failure"))
                })
                .is_err()
        );
        assert_eq!(cache.state.lock().unwrap().index.candidates("a"), ["id"]);
        assert_eq!(
            cache
                .candidates_at("a", ttl, now + ttl, |index| {
                    index.record("id", Some("a"));
                    Ok(1)
                })
                .unwrap(),
            ["id"]
        );
    }
}
