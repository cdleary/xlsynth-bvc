// SPDX-License-Identifier: Apache-2.0

use super::*;

// This is a bound on open descriptors and memory, not a storage-format change.
pub(crate) const MAX_ATOMIC_RECORD_BATCH: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Staged,
    DataSynced,
    Promoted,
    DirectoriesSynced,
}

#[derive(Default)]
struct StagedRecords(Vec<(PathBuf, fs::File)>);

impl Drop for StagedRecords {
    fn drop(&mut self) {
        for (path, _) in &self.0 {
            // Promoted paths no longer exist. Never remove a destination on failure.
            let _ = fs::remove_file(path);
        }
    }
}

fn prepare_directory(root: &Path, path: &Path, sync_dirs: &mut BTreeSet<PathBuf>) -> Result<()> {
    validate_store_path_without_links(root, path, StorePathLeafKind::Directory, "batch directory")?;
    let existed = path.is_dir();
    fs::create_dir_all(path)?;
    validate_store_path_without_links(root, path, StorePathLeafKind::Directory, "batch directory")?;
    let cached = DURABLY_SYNCED_ATOMIC_RECORD_DIRS.get_or_init(|| Mutex::new(HashSet::new()));
    let was_synced = cached
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .contains(path);
    sync_dirs.insert(path.to_path_buf());
    if !existed || !was_synced {
        // The normal single-record writer syncs this entire chain immediately. A batch can
        // defer it until acknowledgement and share ancestors across newly created shards.
        let mut ancestor = path.parent();
        while let Some(directory) = ancestor {
            if !directory.starts_with(root) {
                break;
            }
            sync_dirs.insert(directory.to_path_buf());
            if directory == root {
                break;
            }
            ancestor = directory.parent();
        }
    }
    Ok(())
}

fn sync_directories(directories: BTreeSet<PathBuf>) -> Result<()> {
    let mut directories: Vec<_> = directories.into_iter().collect();
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in &directories {
        fs::File::open(directory)?
            .sync_all()
            .context("syncing batch directory")?;
    }
    DURABLY_SYNCED_ATOMIC_RECORD_DIRS
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .extend(directories);
    Ok(())
}

impl ArtifactStore {
    /// Finish durability for records that may have been published by an interrupted batch.
    /// Do not rewrite unchanged queue bytes during resume. The caller holds transition locks.
    pub(crate) fn sync_existing_records(&self, records: &[PathBuf]) -> Result<()> {
        if records.len() > MAX_ATOMIC_RECORD_BATCH {
            bail!("existing record batch exceeds bound");
        }
        let mut directories = BTreeSet::new();
        for path in records {
            validate_store_path_without_links(
                &self.root,
                path,
                StorePathLeafKind::RegularFileOrMissing,
                "existing batch record",
            )?;
            fs::File::open(path)?
                .sync_all()
                .context("syncing existing record data")?;
            prepare_directory(
                &self.root,
                path.parent().context("record has no parent")?,
                &mut directories,
            )?;
        }
        sync_directories(directories)
    }

    /// Publish individually atomic records, acknowledging only after all data and directory
    /// entries are durable. This is NOT an all-or-nothing transaction: interruption may publish
    /// a prefix, which callers must reconcile idempotently. Callers retain their transition
    /// locks until this returns. The existing protobuf records and paths are unchanged.
    pub(crate) fn write_records_atomic(
        &self,
        domain: &str,
        records: &[(PathBuf, Vec<u8>)],
    ) -> Result<()> {
        self.write_records_atomic_observed(domain, records, |_, _| Ok(()))
    }

    fn write_records_atomic_observed(
        &self,
        domain: &str,
        records: &[(PathBuf, Vec<u8>)],
        mut observe: impl FnMut(Phase, usize) -> Result<()>,
    ) -> Result<()> {
        if records.len() > MAX_ATOMIC_RECORD_BATCH {
            bail!("atomic record batch exceeds bounded size");
        }
        if domain.is_empty()
            || !domain
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            bail!("atomic record domain must be a nonempty ASCII identifier");
        }
        if records.is_empty() {
            return Ok(());
        }
        let mut destinations = BTreeSet::new();
        let mut parents = BTreeSet::new();
        // Validate the entire request before publishing anything.
        for (destination, _) in records {
            validate_store_path_without_links(
                &self.root,
                destination,
                StorePathLeafKind::RegularFileOrMissing,
                "atomic batch destination",
            )?;
            if !destinations.insert(destination) {
                bail!("duplicate atomic batch destination");
            }
            parents.insert(
                destination
                    .parent()
                    .context("batch destination has no parent")?,
            );
        }
        let mut sync_dirs = BTreeSet::new();
        for parent in &parents {
            prepare_directory(&self.root, parent, &mut sync_dirs)?;
        }
        let staging = self.staging_dir().join("atomic-records").join(domain);
        prepare_directory(&self.root, &staging, &mut sync_dirs)?;
        let mut staged = StagedRecords::default();
        for (index, (_, contents)) in records.iter().enumerate() {
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let nonce = ATOMIC_RECORD_WRITE_NONCE.fetch_add(1, Ordering::Relaxed);
            let temp = staging.join(format!(
                "batch-{}-{timestamp}-{nonce}.tmp",
                std::process::id()
            ));
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)
                .with_context(|| format!("creating batch staging file: {}", temp.display()))?;
            staged.0.push((temp, file));
            staged.0.last_mut().unwrap().1.write_all(contents)?;
            observe(Phase::Staged, index)?;
        }
        // Submit every write before waiting for durability. Flushes may coalesce, but no record
        // can be renamed into the queue until its own file data has been synchronized.
        for (index, (_, file)) in staged.0.iter().enumerate() {
            file.sync_all().context("syncing batch record data")?;
            observe(Phase::DataSynced, index)?;
        }
        for (index, ((temp, _), (destination, _))) in staged.0.iter().zip(records).enumerate() {
            validate_store_path_without_links(
                &self.root,
                &staging,
                StorePathLeafKind::Directory,
                "atomic batch staging",
            )?;
            validate_store_path_without_links(
                &self.root,
                destination,
                StorePathLeafKind::RegularFileOrMissing,
                "atomic batch destination",
            )?;
            fs::rename(temp, destination).context("promoting atomic batch record")?;
            observe(Phase::Promoted, index)?;
        }
        // Sync each touched directory once, children before parents, including all newly
        // created ancestor entries. Only cache durability after every required sync succeeds.
        sync_directories(sync_dirs)?;
        observe(Phase::DirectoriesSynced, records.len())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_root(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{prefix}-{}-{}",
            std::process::id(),
            ATOMIC_RECORD_WRITE_NONCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn fixture() -> (ArtifactStore, Vec<(PathBuf, Vec<u8>)>) {
        let root = make_root("atomic-batch");
        let store = ArtifactStore::new(root);
        store.ensure_layout().unwrap();
        let records = (0..4)
            .map(|i| {
                (
                    store
                        .queue_pending_dir()
                        .join(format!("shard-{}/record-{i}.pb", i % 2)),
                    vec![i as u8; i + 1],
                )
            })
            .collect();
        (store, records)
    }

    #[test]
    fn batch_syncs_all_data_before_publishing_and_acknowledges_last() {
        let (store, records) = fixture();
        let mut phases = Vec::new();
        store
            .write_records_atomic_observed("queue", &records, |phase, i| {
                phases.push((phase, i));
                Ok(())
            })
            .unwrap();
        let expected: Vec<_> = [Phase::Staged, Phase::DataSynced, Phase::Promoted]
            .into_iter()
            .flat_map(|p| (0..4).map(move |i| (p, i)))
            .chain([(Phase::DirectoriesSynced, 4)])
            .collect();
        assert_eq!(phases, expected);
        for (path, bytes) in &records {
            assert_eq!(fs::read(path).unwrap(), *bytes);
        }
        assert_eq!(
            fs::read_dir(store.staging_dir().join("atomic-records/queue"))
                .unwrap()
                .count(),
            0
        );
        fs::remove_dir_all(&store.root).unwrap();
    }

    #[test]
    fn interrupted_batch_can_be_replayed_without_partial_records() {
        for stop in [
            (Phase::Staged, 1),
            (Phase::DataSynced, 2),
            (Phase::Promoted, 1),
        ] {
            let (store, records) = fixture();
            let result = store.write_records_atomic_observed("queue", &records, |phase, i| {
                if (phase, i) == stop {
                    bail!("injected interruption");
                }
                Ok(())
            });
            assert!(result.is_err());
            let visible: Vec<_> = records.iter().filter(|(p, _)| p.exists()).collect();
            assert_eq!(visible.len(), if stop.0 == Phase::Promoted { 2 } else { 0 });
            for (path, bytes) in visible {
                assert_eq!(fs::read(path).unwrap(), *bytes);
            }
            store.write_records_atomic("queue", &records).unwrap();
            for (path, bytes) in &records {
                assert_eq!(fs::read(path).unwrap(), *bytes);
            }
            fs::remove_dir_all(&store.root).unwrap();
        }
    }

    #[test]
    fn resume_sync_does_not_rewrite_existing_files() {
        let (store, records) = fixture();
        // Stop after renames, before the final directory sync, as with a failed preparation.
        assert!(
            store
                .write_records_atomic_observed("queue", &records, |phase, index| {
                    if phase == Phase::Promoted && index == 3 {
                        bail!("interrupted before acknowledgement");
                    }
                    Ok(())
                })
                .is_err()
        );
        let before: Vec<_> = records
            .iter()
            .map(|(p, _)| fs::metadata(p).unwrap().modified().unwrap())
            .collect();
        store
            .sync_existing_records(&records.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>())
            .unwrap();
        let after: Vec<_> = records
            .iter()
            .map(|(p, _)| fs::metadata(p).unwrap().modified().unwrap())
            .collect();
        assert_eq!(before, after);
        for (path, bytes) in records {
            assert_eq!(fs::read(path).unwrap(), bytes);
        }
        fs::remove_dir_all(&store.root).unwrap();
    }

    #[test]
    fn invalid_batches_publish_nothing() {
        let (store, records) = fixture();
        let mut invalid = records.clone();
        invalid.push(records[0].clone());
        assert!(store.write_records_atomic("queue", &invalid).is_err());
        invalid = records.clone();
        invalid[3].0 = store.root.join("../escape.pb");
        assert!(store.write_records_atomic("queue", &invalid).is_err());
        assert!(store.write_records_atomic("../escape", &records).is_err());
        assert!(
            store
                .write_records_atomic("queue", &vec![records[0].clone(); 257])
                .is_err()
        );
        assert!(records.iter().all(|(p, _)| !p.exists()));
        fs::remove_dir_all(&store.root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn batch_rejects_symlink_destinations_before_publication() {
        let (store, records) = fixture();
        let outside = make_root("atomic-batch-outside");
        fs::create_dir_all(&outside).unwrap();
        fs::create_dir_all(store.queue_pending_dir()).unwrap();
        std::os::unix::fs::symlink(&outside, records[3].0.parent().unwrap()).unwrap();
        assert!(store.write_records_atomic("queue", &records).is_err());
        assert!(records.iter().all(|(p, _)| !p.exists()));
        assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
        fs::remove_dir_all(&store.root).unwrap();
        fs::remove_dir_all(outside).unwrap();
    }
}
