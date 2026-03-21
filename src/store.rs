use anyhow::{Context, Result, anyhow, bail};
use log::{info, warn};
use serde::Serialize;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fs;
use std::io::Cursor;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use walkdir::WalkDir;

use crate::model::{ArtifactRef, Provenance, QueueCanceled, QueueFailed, QueueItem, QueueRunning};
use crate::queue::action_dependency_action_ids;
use crate::snapshot::snapshot_web_index_path;

#[derive(Debug, Clone)]
pub(crate) enum ArtifactBackendSelection {
    Sled { db_path: PathBuf },
    Snapshot { snapshot_dir: PathBuf },
}

trait ArtifactBackend: std::fmt::Debug + Send + Sync {
    fn ensure_layout(&self, store_root: &Path) -> Result<()>;
    fn artifacts_dir(&self, store_root: &Path) -> PathBuf;
    fn action_dir(&self, store_root: &Path, action_id: &str) -> PathBuf;
    fn materialize_action_dir(&self, store_root: &Path, action_id: &str) -> Result<PathBuf>;
    fn promote_staging_action_dir(
        &self,
        store_root: &Path,
        action_id: &str,
        staging_dir: &Path,
    ) -> Result<()>;
    fn provenance_path(&self, store_root: &Path, action_id: &str) -> PathBuf;
    fn failed_action_record_exists(&self, store_root: &Path, action_id: &str) -> bool;
    fn load_failed_action_record(
        &self,
        store_root: &Path,
        action_id: &str,
    ) -> Result<Option<QueueFailed>>;
    fn write_failed_action_record(&self, store_root: &Path, failed: &QueueFailed) -> Result<()>;
    fn delete_failed_action_record(&self, store_root: &Path, action_id: &str) -> Result<bool>;
    fn action_exists(&self, store_root: &Path, action_id: &str) -> bool;
    fn load_provenance(&self, store_root: &Path, action_id: &str) -> Result<Provenance>;
    fn write_provenance(&self, store_root: &Path, provenance: &Provenance) -> Result<()>;
    fn for_each_provenance(
        &self,
        store_root: &Path,
        visitor: &mut dyn FnMut(Provenance) -> Result<ControlFlow<()>>,
    ) -> Result<()>;
    fn resolve_artifact_ref_path(&self, store_root: &Path, artifact_ref: &ArtifactRef) -> PathBuf;
    fn list_provenances(&self, store_root: &Path) -> Result<Vec<Provenance>>;
    fn load_failed_action_records(&self, store_root: &Path) -> Result<Vec<QueueFailed>>;
    fn load_web_index_bytes(&self, store_root: &Path, index_key: &str) -> Result<Option<Vec<u8>>>;
    fn write_web_index_bytes(&self, store_root: &Path, index_key: &str, bytes: &[u8])
    -> Result<()>;
    fn list_web_index_entries_with_prefix(
        &self,
        store_root: &Path,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>>;
    fn delete_web_index_keys_with_prefix(&self, store_root: &Path, prefix: &str) -> Result<usize>;
    fn web_index_location(&self, store_root: &Path, index_key: &str) -> String;
    fn size_on_disk_bytes(&self, store_root: &Path) -> Result<u64>;
    fn flush_durable(&self) -> Result<()>;
}

fn shard_dir(base: &Path, key: &str) -> PathBuf {
    let first = key.get(0..2).unwrap_or("xx");
    let second = key.get(2..4).unwrap_or("xx");
    base.join(first).join(second)
}

fn promote_staging_action_dir_fs(staging_dir: &Path, final_dir: &Path) -> Result<()> {
    let final_parent = final_dir
        .parent()
        .ok_or_else(|| anyhow::anyhow!("final action dir had no parent"))?;
    fs::create_dir_all(final_parent)
        .with_context(|| format!("creating final shard path: {}", final_parent.display()))?;

    match fs::rename(staging_dir, final_dir) {
        Ok(()) => Ok(()),
        Err(first_err) => {
            if !final_dir.exists() {
                return Err(first_err).with_context(|| {
                    format!(
                        "moving staged artifact into place: {} -> {}",
                        staging_dir.display(),
                        final_dir.display()
                    )
                });
            }

            let final_provenance = final_dir.join("provenance.json");
            if final_provenance.exists() {
                fs::remove_dir_all(staging_dir).ok();
                return Ok(());
            }

            fs::remove_dir_all(final_dir).with_context(|| {
                format!(
                    "removing failed-only final action dir before promotion: {}",
                    final_dir.display()
                )
            })?;
            fs::rename(staging_dir, final_dir).with_context(|| {
                format!(
                    "moving staged artifact into place after removing failed-only dir: {} -> {}",
                    staging_dir.display(),
                    final_dir.display()
                )
            })?;
            Ok(())
        }
    }
}

#[derive(Debug)]
struct SledArtifactBackend {
    db_path: PathBuf,
    db: Mutex<Option<sled::Db>>,
}

impl SledArtifactBackend {
    const TREE_PROVENANCE_BY_ACTION: &'static str = "provenance_by_action";
    const TREE_ACTION_FILE_BYTES: &'static str = "action_file_bytes";
    const TREE_FAILED_BY_ACTION: &'static str = "failed_by_action";
    const TREE_WEB_INDEX_BYTES: &'static str = "web_index_bytes";
    const DEFAULT_CACHE_CAPACITY_BYTES: u64 = 256 * 1024 * 1024;
    const MIN_CACHE_CAPACITY_BYTES: u64 = 16 * 1024 * 1024;
    const MAX_CACHE_CAPACITY_BYTES: u64 = 8 * 1024 * 1024 * 1024;
    const ACTION_FILE_ZSTD_MAGIC: &'static [u8] = b"BVCZSTD1";
    const ACTION_FILE_COMPRESS_MIN_BYTES_DEFAULT: usize = 4 * 1024;
    const ACTION_FILE_COMPRESS_LARGE_MIN_BYTES_DEFAULT: usize = 1024 * 1024;
    const ACTION_FILE_COMPRESS_LEVEL_DEFAULT: i32 = 3;
    const ACTION_FILE_COMPRESS_LEVEL_LARGE_DEFAULT: i32 = 12;

    fn action_file_compress_min_bytes() -> usize {
        static VALUE: OnceLock<usize> = OnceLock::new();
        *VALUE.get_or_init(|| {
            std::env::var("BVC_SLED_ACTION_FILE_COMPRESS_MIN_BYTES")
                .ok()
                .and_then(|raw| raw.trim().parse::<usize>().ok())
                .unwrap_or(Self::ACTION_FILE_COMPRESS_MIN_BYTES_DEFAULT)
        })
    }

    fn action_file_compress_large_min_bytes() -> usize {
        static VALUE: OnceLock<usize> = OnceLock::new();
        *VALUE.get_or_init(|| {
            std::env::var("BVC_SLED_ACTION_FILE_COMPRESS_LARGE_MIN_BYTES")
                .ok()
                .and_then(|raw| raw.trim().parse::<usize>().ok())
                .unwrap_or(Self::ACTION_FILE_COMPRESS_LARGE_MIN_BYTES_DEFAULT)
        })
    }

    fn action_file_compress_level() -> i32 {
        static VALUE: OnceLock<i32> = OnceLock::new();
        *VALUE.get_or_init(|| {
            std::env::var("BVC_SLED_ACTION_FILE_COMPRESS_LEVEL")
                .ok()
                .and_then(|raw| raw.trim().parse::<i32>().ok())
                .unwrap_or(Self::ACTION_FILE_COMPRESS_LEVEL_DEFAULT)
                .clamp(1, 19)
        })
    }

    fn action_file_compress_level_large() -> i32 {
        static VALUE: OnceLock<i32> = OnceLock::new();
        *VALUE.get_or_init(|| {
            std::env::var("BVC_SLED_ACTION_FILE_COMPRESS_LEVEL_LARGE")
                .ok()
                .and_then(|raw| raw.trim().parse::<i32>().ok())
                .unwrap_or(Self::ACTION_FILE_COMPRESS_LEVEL_LARGE_DEFAULT)
                .clamp(1, 19)
        })
    }

    fn action_file_compress_level_for_size(len: usize) -> i32 {
        if len >= Self::action_file_compress_large_min_bytes() {
            Self::action_file_compress_level_large()
        } else {
            Self::action_file_compress_level()
        }
    }

    fn maybe_encode_action_file_value(raw_bytes: &[u8]) -> Result<Vec<u8>> {
        if raw_bytes.len() < Self::action_file_compress_min_bytes() {
            return Ok(raw_bytes.to_vec());
        }
        let level = Self::action_file_compress_level_for_size(raw_bytes.len());
        let compressed = zstd::bulk::compress(raw_bytes, level).with_context(|| {
            format!(
                "zstd compressing action-file row bytes={} level={}",
                raw_bytes.len(),
                level
            )
        })?;
        let framed_len = Self::ACTION_FILE_ZSTD_MAGIC.len() + compressed.len();
        if framed_len >= raw_bytes.len() {
            return Ok(raw_bytes.to_vec());
        }
        let mut encoded = Vec::with_capacity(framed_len);
        encoded.extend_from_slice(Self::ACTION_FILE_ZSTD_MAGIC);
        encoded.extend_from_slice(&compressed);
        Ok(encoded)
    }

    fn decode_action_file_value(stored_bytes: &[u8]) -> Result<Vec<u8>> {
        let Some(payload) = stored_bytes.strip_prefix(Self::ACTION_FILE_ZSTD_MAGIC) else {
            return Ok(stored_bytes.to_vec());
        };
        zstd::stream::decode_all(Cursor::new(payload)).context("zstd decoding action-file row")
    }

    fn cache_capacity_bytes() -> u64 {
        std::env::var("BVC_SLED_CACHE_CAPACITY_BYTES")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .map(|value| {
                value.clamp(
                    Self::MIN_CACHE_CAPACITY_BYTES,
                    Self::MAX_CACHE_CAPACITY_BYTES,
                )
            })
            .unwrap_or(Self::DEFAULT_CACHE_CAPACITY_BYTES)
    }

    fn open_db(&self) -> Result<sled::Db> {
        let mut guard = self
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("acquiring sled db mutex"))?;
        if let Some(db) = guard.as_ref() {
            return Ok(db.clone());
        }
        if let Some(parent) = self.db_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "creating parent directory for sled db path: {}",
                    parent.display()
                )
            })?;
        }
        let cache_capacity = Self::cache_capacity_bytes();
        let db = sled::Config::new()
            .path(&self.db_path)
            .cache_capacity(cache_capacity)
            .open()
            .with_context(|| {
                format!(
                    "opening sled db: {} (cache_capacity_bytes={})",
                    self.db_path.display(),
                    cache_capacity
                )
            })?;
        *guard = Some(db.clone());
        Ok(db)
    }

    fn materialized_actions_root(store_root: &Path) -> PathBuf {
        store_root.join(".materialized-actions")
    }

    fn action_files_prefix(action_id: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(action_id.len() + 1);
        key.extend_from_slice(action_id.as_bytes());
        key.push(0);
        key
    }

    fn action_file_key(action_id: &str, relpath: &str) -> Vec<u8> {
        let mut key = Self::action_files_prefix(action_id);
        key.extend_from_slice(relpath.as_bytes());
        key
    }

    fn relpath_from_action_file_key(action_id: &str, key: &[u8]) -> Result<String> {
        let prefix = Self::action_files_prefix(action_id);
        let rel = key
            .strip_prefix(prefix.as_slice())
            .ok_or_else(|| anyhow::anyhow!("action-file key prefix mismatch"))?;
        String::from_utf8(rel.to_vec()).context("decoding action-file key relpath as UTF-8")
    }

    fn normalize_relpath(path: &Path) -> Result<String> {
        let mut parts = Vec::new();
        for component in path.components() {
            match component {
                std::path::Component::Normal(part) => {
                    parts.push(part.to_string_lossy().to_string());
                }
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    bail!(
                        "refusing to normalize relpath containing parent traversal: {}",
                        path.display()
                    );
                }
                std::path::Component::RootDir | std::path::Component::Prefix(_) => {
                    bail!(
                        "refusing to normalize absolute relpath component: {}",
                        path.display()
                    );
                }
            }
        }
        Ok(parts.join("/"))
    }

    fn materialized_action_dir_for(&self, store_root: &Path, action_id: &str) -> PathBuf {
        shard_dir(&Self::materialized_actions_root(store_root), action_id).join(action_id)
    }

    fn materialize_action_from_db(&self, store_root: &Path, action_id: &str) -> Result<PathBuf> {
        let final_dir = self.materialized_action_dir_for(store_root, action_id);
        if final_dir.join("provenance.json").exists() {
            return Ok(final_dir);
        }

        let db = self.open_db()?;
        let provenance_tree = db
            .open_tree(Self::TREE_PROVENANCE_BY_ACTION)
            .context("opening sled tree provenance_by_action")?;
        let file_tree = db
            .open_tree(Self::TREE_ACTION_FILE_BYTES)
            .context("opening sled tree action_file_bytes")?;

        let provenance_bytes = match provenance_tree
            .get(action_id.as_bytes())
            .context("loading provenance from sled")?
        {
            Some(bytes) => bytes.to_vec(),
            None => return Ok(final_dir),
        };

        let staging_root = store_root.join(".staging");
        fs::create_dir_all(&staging_root).with_context(|| {
            format!(
                "creating materialization staging root: {}",
                staging_root.display()
            )
        })?;
        let pid = std::process::id();
        let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let stage_dir = staging_root.join(format!("materialize-{action_id}-{pid}-{ts}"));
        fs::create_dir(&stage_dir).with_context(|| {
            format!(
                "creating materialization staging dir: {}",
                stage_dir.display()
            )
        })?;

        let result = (|| -> Result<()> {
            let prefix = Self::action_files_prefix(action_id);
            let mut wrote_any = false;
            for row in file_tree.scan_prefix(prefix) {
                let (key, value) = row.context("iterating action-file rows from sled")?;
                let relpath = Self::relpath_from_action_file_key(action_id, key.as_ref())?;
                let destination =
                    stage_dir.join(relpath.replace('/', std::path::MAIN_SEPARATOR_STR));
                if let Some(parent) = destination.parent() {
                    fs::create_dir_all(parent).with_context(|| {
                        format!(
                            "creating materialized parent directory: {}",
                            parent.display()
                        )
                    })?;
                }
                let decoded = Self::decode_action_file_value(value.as_ref())
                    .with_context(|| format!("decoding action-file row payload for {}", relpath))?;
                fs::write(&destination, decoded).with_context(|| {
                    format!(
                        "writing materialized artifact file: {}",
                        destination.display()
                    )
                })?;
                wrote_any = true;
            }

            let provenance_path = stage_dir.join("provenance.json");
            if !wrote_any || !provenance_path.exists() {
                fs::write(&provenance_path, &provenance_bytes).with_context(|| {
                    format!(
                        "writing materialized provenance fallback: {}",
                        provenance_path.display()
                    )
                })?;
            }

            promote_staging_action_dir_fs(&stage_dir, &final_dir)?;
            Ok(())
        })();

        if result.is_err() {
            let _ = fs::remove_dir_all(&stage_dir);
        }
        result?;
        Ok(final_dir)
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SledActionFileCompressionBackfillSummary {
    pub(crate) db_path: String,
    pub(crate) dry_run: bool,
    pub(crate) limit: Option<usize>,
    pub(crate) target_relpath: Option<String>,
    pub(crate) scanned_provenance_rows: Option<u64>,
    pub(crate) scanned_entries: u64,
    pub(crate) rewritten_entries: u64,
    pub(crate) unchanged_entries: u64,
    pub(crate) already_compressed_entries: u64,
    pub(crate) written_compressed_entries: u64,
    pub(crate) written_uncompressed_entries: u64,
    pub(crate) stored_bytes_before: u64,
    pub(crate) stored_bytes_after: u64,
    pub(crate) stored_bytes_delta: i64,
    pub(crate) elapsed_secs: f64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SledCompactionTreeSummary {
    pub(crate) tree: String,
    pub(crate) entries: u64,
    pub(crate) key_bytes: u64,
    pub(crate) value_bytes: u64,
    pub(crate) logical_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SledCompactionSummary {
    pub(crate) source_db_path: String,
    pub(crate) destination_db_path: String,
    pub(crate) replaced_source: bool,
    pub(crate) backup_db_path: Option<String>,
    pub(crate) tree_count: usize,
    pub(crate) total_entries: u64,
    pub(crate) total_key_bytes: u64,
    pub(crate) total_value_bytes: u64,
    pub(crate) total_logical_bytes: u64,
    pub(crate) elapsed_secs: f64,
    pub(crate) trees: Vec<SledCompactionTreeSummary>,
}

fn default_compacted_db_path(source_db_path: &Path) -> PathBuf {
    let parent = source_db_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let file_name = source_db_path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "artifacts.sled".to_string());
    parent.join(format!("{file_name}.compacted"))
}

fn default_compacted_backup_db_path(source_db_path: &Path) -> PathBuf {
    let parent = source_db_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let file_name = source_db_path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "artifacts.sled".to_string());
    let epoch_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    parent.join(format!("{file_name}.precompact-{epoch_secs}.bak"))
}

pub(crate) fn compact_sled_db(
    source_db_path: &Path,
    destination_db_path: Option<&Path>,
    replace_source: bool,
) -> Result<SledCompactionSummary> {
    const SOURCE_CACHE_CAPACITY_BYTES: u64 = 16 * 1024 * 1024;
    const DEST_CACHE_CAPACITY_BYTES: u64 = 64 * 1024 * 1024;

    let started = Instant::now();
    let destination_path = destination_db_path
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_compacted_db_path(source_db_path));
    let source_abs = source_db_path
        .canonicalize()
        .unwrap_or_else(|_| source_db_path.to_path_buf());
    let destination_abs = destination_path
        .canonicalize()
        .unwrap_or_else(|_| destination_path.to_path_buf());

    if source_abs == destination_abs {
        bail!(
            "compact-sled-db destination must differ from source: {}",
            source_db_path.display()
        );
    }
    if destination_path.starts_with(source_db_path) {
        bail!(
            "compact-sled-db destination must not be inside source db dir: {}",
            destination_path.display()
        );
    }
    if source_db_path.starts_with(&destination_path) {
        bail!(
            "compact-sled-db source must not be inside destination db dir: {}",
            source_db_path.display()
        );
    }
    if destination_path.exists() {
        bail!(
            "compact-sled-db destination already exists: {}",
            destination_path.display()
        );
    }
    if let Some(parent) = destination_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "creating destination parent directory for compacted db: {}",
                parent.display()
            )
        })?;
    }

    eprintln!(
        "compact-sled-db: opening source {}",
        source_db_path.display()
    );
    let source_db = match sled::Config::new()
        .path(source_db_path)
        .cache_capacity(SOURCE_CACHE_CAPACITY_BYTES)
        .open()
    {
        Ok(db) => db,
        Err(err) => {
            let err_text = err.to_string();
            if err_text.contains("could not acquire lock") {
                bail!(
                    "opening source sled db: {} (database lock is held by another process; stop the service first)",
                    source_db_path.display()
                );
            }
            return Err(err)
                .with_context(|| format!("opening source sled db: {}", source_db_path.display()));
        }
    };

    eprintln!(
        "compact-sled-db: creating destination {}",
        destination_path.display()
    );
    let destination_db = sled::Config::new()
        .path(&destination_path)
        .cache_capacity(DEST_CACHE_CAPACITY_BYTES)
        .open()
        .with_context(|| {
            format!(
                "opening destination sled db: {}",
                destination_path.display()
            )
        })?;

    let tree_names = source_db.tree_names();
    eprintln!("compact-sled-db: copying {} trees", tree_names.len());
    let mut trees = Vec::with_capacity(tree_names.len());
    let mut total_entries = 0_u64;
    let mut total_key_bytes = 0_u64;
    let mut total_value_bytes = 0_u64;
    let mut last_progress = Instant::now();

    for tree_name_bytes in tree_names {
        let tree_label = String::from_utf8_lossy(tree_name_bytes.as_ref()).to_string();
        let source_tree = source_db
            .open_tree(tree_name_bytes.as_ref())
            .with_context(|| format!("opening source tree '{}'", tree_label))?;
        let destination_tree = destination_db
            .open_tree(tree_name_bytes.as_ref())
            .with_context(|| format!("opening destination tree '{}'", tree_label))?;
        eprintln!("compact-sled-db: tree={} begin", tree_label);

        let mut tree_entries = 0_u64;
        let mut tree_key_bytes = 0_u64;
        let mut tree_value_bytes = 0_u64;
        for row in source_tree.iter() {
            let (key, value) =
                row.with_context(|| format!("reading source tree '{}'", tree_label))?;
            tree_entries += 1;
            tree_key_bytes += key.len() as u64;
            tree_value_bytes += value.len() as u64;
            destination_tree
                .insert(key, value)
                .with_context(|| format!("writing destination tree '{}'", tree_label))?;
            if last_progress.elapsed().as_secs() >= 5 {
                eprintln!(
                    "compact-sled-db: tree={} entries={} logical_gib={:.3} total_entries={} elapsed_s={:.1}",
                    tree_label,
                    tree_entries,
                    (tree_key_bytes + tree_value_bytes) as f64 / (1024.0 * 1024.0 * 1024.0),
                    total_entries + tree_entries,
                    started.elapsed().as_secs_f64(),
                );
                last_progress = Instant::now();
            }
        }
        destination_tree
            .flush()
            .with_context(|| format!("flushing destination tree '{}'", tree_label))?;

        let tree_logical_bytes = tree_key_bytes + tree_value_bytes;
        eprintln!(
            "compact-sled-db: tree={} done entries={} logical_gib={:.3}",
            tree_label,
            tree_entries,
            tree_logical_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
        );
        trees.push(SledCompactionTreeSummary {
            tree: tree_label,
            entries: tree_entries,
            key_bytes: tree_key_bytes,
            value_bytes: tree_value_bytes,
            logical_bytes: tree_logical_bytes,
        });
        total_entries += tree_entries;
        total_key_bytes += tree_key_bytes;
        total_value_bytes += tree_value_bytes;
    }
    destination_db
        .flush()
        .context("flushing destination sled db after compaction copy")?;
    drop(destination_db);
    drop(source_db);

    let mut backup_db_path = None;
    let mut final_destination = destination_path.clone();
    if replace_source {
        let backup_path = default_compacted_backup_db_path(source_db_path);
        if backup_path.exists() {
            bail!(
                "compact-sled-db backup path already exists: {}",
                backup_path.display()
            );
        }
        eprintln!(
            "compact-sled-db: replacing source {} with compacted db {} (backup={})",
            source_db_path.display(),
            destination_path.display(),
            backup_path.display()
        );
        fs::rename(source_db_path, &backup_path).with_context(|| {
            format!(
                "renaming source db to backup: {} -> {}",
                source_db_path.display(),
                backup_path.display()
            )
        })?;
        if let Err(swap_err) = fs::rename(&destination_path, source_db_path) {
            let restore_result = fs::rename(&backup_path, source_db_path);
            if let Err(restore_err) = restore_result {
                return Err(swap_err).with_context(|| {
                    format!(
                        "swapping compacted db into source failed and source restore also failed: {}",
                        restore_err
                    )
                });
            }
            return Err(swap_err).with_context(|| {
                format!(
                    "swapping compacted db into source: {} -> {}",
                    destination_path.display(),
                    source_db_path.display()
                )
            });
        }
        backup_db_path = Some(backup_path.display().to_string());
        final_destination = source_db_path.to_path_buf();
    }

    Ok(SledCompactionSummary {
        source_db_path: source_db_path.display().to_string(),
        destination_db_path: final_destination.display().to_string(),
        replaced_source: replace_source,
        backup_db_path,
        tree_count: trees.len(),
        total_entries,
        total_key_bytes,
        total_value_bytes,
        total_logical_bytes: total_key_bytes + total_value_bytes,
        elapsed_secs: started.elapsed().as_secs_f64(),
        trees,
    })
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SledPruneActionsByRelpathSizeSummary {
    pub(crate) db_path: String,
    pub(crate) store_root: String,
    pub(crate) relpath: String,
    pub(crate) min_bytes: usize,
    pub(crate) include_downstream: bool,
    pub(crate) dry_run: bool,
    pub(crate) seed_action_count: u64,
    pub(crate) downstream_action_count: u64,
    pub(crate) total_action_count: u64,
    pub(crate) seed_action_ids_sample: Vec<String>,
    pub(crate) deleted_provenance_rows: u64,
    pub(crate) deleted_action_file_rows: u64,
    pub(crate) deleted_action_file_key_bytes: u64,
    pub(crate) deleted_action_file_stored_value_bytes: u64,
    pub(crate) deleted_action_file_decoded_value_bytes: u64,
    pub(crate) deleted_queue_pending_files: u64,
    pub(crate) deleted_queue_running_files: u64,
    pub(crate) deleted_queue_done_files: u64,
    pub(crate) deleted_queue_failed_files: u64,
    pub(crate) deleted_queue_canceled_files: u64,
    pub(crate) deleted_failed_action_record_files: u64,
    pub(crate) deleted_materialized_action_dirs: u64,
    pub(crate) elapsed_secs: f64,
}

fn queue_json_path(store_root: &Path, state: &str, action_id: &str) -> PathBuf {
    shard_dir(&store_root.join("queue").join(state), action_id).join(format!("{action_id}.json"))
}

fn materialized_action_dir_path(store_root: &Path, action_id: &str) -> PathBuf {
    shard_dir(&store_root.join(".materialized-actions"), action_id).join(action_id)
}

fn remove_file_if_exists(path: &Path, dry_run: bool) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    if !dry_run {
        fs::remove_file(path).with_context(|| format!("removing file {}", path.display()))?;
    }
    Ok(true)
}

fn remove_dir_if_exists(path: &Path, dry_run: bool) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    if !dry_run {
        fs::remove_dir_all(path).with_context(|| format!("removing dir {}", path.display()))?;
    }
    Ok(true)
}

fn queue_dependency_edges_path(
    path: &Path,
    parse_action: impl FnOnce(String) -> Result<(String, Vec<String>)>,
    reverse_edges: &mut HashMap<String, Vec<String>>,
) {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) => {
            warn!(
                "skipping queue record; read failed path={} error={:#}",
                path.display(),
                err
            );
            return;
        }
    };
    let (action_id, deps) = match parse_action(text) {
        Ok(parsed) => parsed,
        Err(err) => {
            warn!(
                "skipping queue record; parse failed path={} error={:#}",
                path.display(),
                err
            );
            return;
        }
    };
    for dep in deps {
        reverse_edges
            .entry(dep)
            .or_default()
            .push(action_id.clone());
    }
}

fn build_reverse_dependency_edges(
    store_root: &Path,
    provenance_tree: &sled::Tree,
    failed_tree: &sled::Tree,
) -> Result<HashMap<String, Vec<String>>> {
    let mut reverse_edges: HashMap<String, Vec<String>> = HashMap::new();
    let started = Instant::now();
    let mut scanned_provenance_rows = 0_u64;
    let mut scanned_queue_rows = 0_u64;
    let mut scanned_failed_record_rows = 0_u64;
    let mut last_progress = Instant::now();

    for row in provenance_tree.iter() {
        let (action_id, provenance_bytes) =
            row.context("iterating provenance_by_action rows for prune graph")?;
        let action_id = String::from_utf8(action_id.to_vec())
            .context("decoding action_id from provenance key during prune graph build")?;
        let provenance: Provenance = serde_json::from_slice(provenance_bytes.as_ref())
            .with_context(|| format!("parsing provenance JSON for action {}", action_id))?;
        scanned_provenance_rows += 1;
        for dep in provenance.dependencies {
            reverse_edges
                .entry(dep.action_id)
                .or_default()
                .push(action_id.clone());
        }
        if last_progress.elapsed().as_secs() >= 5 {
            eprintln!(
                "prune-sled-actions-by-relpath-size: reverse-graph provenance_rows={} queue_rows={} failed_rows={} edge_keys={} elapsed_s={:.1}",
                scanned_provenance_rows,
                scanned_queue_rows,
                scanned_failed_record_rows,
                reverse_edges.len(),
                started.elapsed().as_secs_f64()
            );
            last_progress = Instant::now();
        }
    }

    let mut scan_queue_dir = |state: &str,
                              parse: fn(String) -> Result<(String, Vec<String>)>,
                              reverse_edges: &mut HashMap<String, Vec<String>>|
     -> Result<()> {
        let root = store_root.join("queue").join(state);
        if !root.exists() {
            return Ok(());
        }
        for entry in WalkDir::new(&root) {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    warn!(
                        "skipping queue walk entry state={} root={} error={:#}",
                        state,
                        root.display(),
                        err
                    );
                    continue;
                }
            };
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            scanned_queue_rows += 1;
            queue_dependency_edges_path(path, parse, reverse_edges);
            if last_progress.elapsed().as_secs() >= 5 {
                eprintln!(
                    "prune-sled-actions-by-relpath-size: reverse-graph provenance_rows={} queue_rows={} failed_rows={} edge_keys={} elapsed_s={:.1}",
                    scanned_provenance_rows,
                    scanned_queue_rows,
                    scanned_failed_record_rows,
                    reverse_edges.len(),
                    started.elapsed().as_secs_f64()
                );
                last_progress = Instant::now();
            }
        }
        Ok(())
    };

    fn parse_pending(text: String) -> Result<(String, Vec<String>)> {
        let record: QueueItem =
            serde_json::from_str(&text).context("parsing pending queue record JSON")?;
        let deps = action_dependency_action_ids(&record.action)
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        Ok((record.action_id, deps))
    }

    fn parse_running(text: String) -> Result<(String, Vec<String>)> {
        let record: QueueRunning =
            serde_json::from_str(&text).context("parsing running queue record JSON")?;
        let deps = action_dependency_action_ids(&record.action)
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        Ok((record.action_id, deps))
    }

    fn parse_canceled(text: String) -> Result<(String, Vec<String>)> {
        let record: QueueCanceled =
            serde_json::from_str(&text).context("parsing canceled queue record JSON")?;
        let deps = action_dependency_action_ids(&record.action)
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        Ok((record.action_id, deps))
    }

    scan_queue_dir("pending", parse_pending, &mut reverse_edges)?;
    scan_queue_dir("running", parse_running, &mut reverse_edges)?;
    scan_queue_dir("canceled", parse_canceled, &mut reverse_edges)?;
    for row in failed_tree.iter() {
        let (_action_id, value) = row.context("iterating failed_by_action rows for prune graph")?;
        let failed: QueueFailed = serde_json::from_slice(value.as_ref())
            .context("parsing failed queue record from sled failed_by_action row")?;
        scanned_failed_record_rows += 1;
        for dep in action_dependency_action_ids(&failed.action) {
            reverse_edges
                .entry(dep.to_string())
                .or_default()
                .push(failed.action_id.clone());
        }
        if last_progress.elapsed().as_secs() >= 5 {
            eprintln!(
                "prune-sled-actions-by-relpath-size: reverse-graph provenance_rows={} queue_rows={} failed_rows={} edge_keys={} elapsed_s={:.1}",
                scanned_provenance_rows,
                scanned_queue_rows,
                scanned_failed_record_rows,
                reverse_edges.len(),
                started.elapsed().as_secs_f64()
            );
            last_progress = Instant::now();
        }
    }

    eprintln!(
        "prune-sled-actions-by-relpath-size: reverse-graph complete provenance_rows={} queue_rows={} failed_rows={} edge_keys={} elapsed_s={:.1}",
        scanned_provenance_rows,
        scanned_queue_rows,
        scanned_failed_record_rows,
        reverse_edges.len(),
        started.elapsed().as_secs_f64()
    );

    Ok(reverse_edges)
}

pub(crate) fn prune_sled_actions_by_relpath_size(
    store_root: &Path,
    db_path: &Path,
    relpath: &str,
    min_bytes: usize,
    include_downstream: bool,
    dry_run: bool,
) -> Result<SledPruneActionsByRelpathSizeSummary> {
    const PRUNE_CACHE_CAPACITY_BYTES: u64 = 16 * 1024 * 1024;
    let started = Instant::now();
    eprintln!(
        "prune-sled-actions-by-relpath-size: opening {}",
        db_path.display()
    );
    let db = match sled::Config::new()
        .path(db_path)
        .cache_capacity(PRUNE_CACHE_CAPACITY_BYTES)
        .open()
    {
        Ok(db) => db,
        Err(err) => {
            let err_text = err.to_string();
            if err_text.contains("could not acquire lock") {
                bail!(
                    "opening sled db for prune: {} (database lock is held by another process; stop the service first)",
                    db_path.display()
                );
            }
            return Err(err)
                .with_context(|| format!("opening sled db for prune: {}", db_path.display()));
        }
    };
    eprintln!(
        "prune-sled-actions-by-relpath-size: opened {} elapsed_s={:.1}",
        db_path.display(),
        started.elapsed().as_secs_f64()
    );
    let provenance_tree = db
        .open_tree(SledArtifactBackend::TREE_PROVENANCE_BY_ACTION)
        .context("opening provenance_by_action tree for prune")?;
    let file_tree = db
        .open_tree(SledArtifactBackend::TREE_ACTION_FILE_BYTES)
        .context("opening action_file_bytes tree for prune")?;
    let failed_tree = db
        .open_tree(SledArtifactBackend::TREE_FAILED_BY_ACTION)
        .context("opening failed_by_action tree for prune")?;

    let mut seed_action_ids: BTreeSet<String> = BTreeSet::new();
    let mut scanned_provenance_rows = 0_u64;
    let mut matched_relpath_outputs = 0_u64;
    let mut last_progress = Instant::now();
    for row in provenance_tree.iter() {
        let (_, provenance_bytes) =
            row.context("iterating provenance_by_action rows during prune seed scan")?;
        scanned_provenance_rows += 1;
        let provenance: Provenance = serde_json::from_slice(provenance_bytes.as_ref())
            .context("parsing provenance JSON during prune seed scan")?;
        if let Some(output_file) = provenance.output_files.iter().find(|f| f.path == relpath) {
            matched_relpath_outputs += 1;
            if output_file.bytes >= min_bytes as u64 {
                seed_action_ids.insert(provenance.action_id.clone());
            }
        }
        if last_progress.elapsed().as_secs() >= 5 {
            eprintln!(
                "prune-sled-actions-by-relpath-size: scanned_provenance_rows={} matched_relpath_outputs={} seed_actions={} elapsed_s={:.1}",
                scanned_provenance_rows,
                matched_relpath_outputs,
                seed_action_ids.len(),
                started.elapsed().as_secs_f64()
            );
            last_progress = Instant::now();
        }
    }

    let reverse_edges = if include_downstream {
        eprintln!("prune-sled-actions-by-relpath-size: building reverse dependency graph");
        build_reverse_dependency_edges(store_root, &provenance_tree, &failed_tree)?
    } else {
        HashMap::new()
    };

    let mut action_ids_to_prune: BTreeSet<String> = seed_action_ids.clone();
    if include_downstream {
        let mut queue: VecDeque<String> = seed_action_ids.iter().cloned().collect();
        while let Some(action_id) = queue.pop_front() {
            let Some(children) = reverse_edges.get(&action_id) else {
                continue;
            };
            for child in children {
                if action_ids_to_prune.insert(child.clone()) {
                    queue.push_back(child.clone());
                }
            }
        }
    }
    let downstream_action_count = action_ids_to_prune
        .len()
        .saturating_sub(seed_action_ids.len()) as u64;

    eprintln!(
        "prune-sled-actions-by-relpath-size: seed_actions={} downstream_actions={} total_actions={} dry_run={}",
        seed_action_ids.len(),
        downstream_action_count,
        action_ids_to_prune.len(),
        dry_run
    );

    let mut deleted_provenance_rows = 0_u64;
    let mut deleted_action_file_rows = 0_u64;
    let mut deleted_action_file_key_bytes = 0_u64;
    let mut deleted_action_file_stored_value_bytes = 0_u64;
    let mut deleted_action_file_decoded_value_bytes = 0_u64;
    let mut deleted_queue_pending_files = 0_u64;
    let mut deleted_queue_running_files = 0_u64;
    let mut deleted_queue_done_files = 0_u64;
    let mut deleted_queue_failed_files = 0_u64;
    let mut deleted_queue_canceled_files = 0_u64;
    let mut deleted_failed_action_record_files = 0_u64;
    let mut deleted_materialized_action_dirs = 0_u64;
    let total_actions_to_prune = action_ids_to_prune.len() as u64;
    let mut processed_actions = 0_u64;
    let mut last_delete_progress = Instant::now();

    for action_id in &action_ids_to_prune {
        processed_actions += 1;
        if provenance_tree
            .get(action_id.as_bytes())
            .with_context(|| format!("reading provenance row before delete for {}", action_id))?
            .is_some()
        {
            deleted_provenance_rows += 1;
            if !dry_run {
                provenance_tree
                    .remove(action_id.as_bytes())
                    .with_context(|| format!("deleting provenance row for {}", action_id))?;
            }
        }

        let prefix = SledArtifactBackend::action_files_prefix(action_id);
        let mut keys = Vec::new();
        for row in file_tree.scan_prefix(prefix.as_slice()) {
            let (key, value) = row.with_context(|| {
                format!(
                    "iterating action_file rows for delete action_id={}",
                    action_id
                )
            })?;
            deleted_action_file_rows += 1;
            deleted_action_file_key_bytes += key.len() as u64;
            deleted_action_file_stored_value_bytes += value.len() as u64;
            // Avoid decoding large payloads during prune accounting; this field acts as a
            // lower bound when rows are compressed.
            deleted_action_file_decoded_value_bytes += value.len() as u64;
            keys.push(key);
        }
        if !dry_run {
            for key in keys {
                file_tree.remove(key).with_context(|| {
                    format!("deleting action_file row for action_id={}", action_id)
                })?;
            }
        }

        if remove_file_if_exists(&queue_json_path(store_root, "pending", action_id), dry_run)? {
            deleted_queue_pending_files += 1;
        }
        if remove_file_if_exists(&queue_json_path(store_root, "running", action_id), dry_run)? {
            deleted_queue_running_files += 1;
        }
        if remove_file_if_exists(&queue_json_path(store_root, "done", action_id), dry_run)? {
            deleted_queue_done_files += 1;
        }
        if remove_file_if_exists(&queue_json_path(store_root, "failed", action_id), dry_run)? {
            deleted_queue_failed_files += 1;
        }
        if remove_file_if_exists(&queue_json_path(store_root, "canceled", action_id), dry_run)? {
            deleted_queue_canceled_files += 1;
        }
        if failed_tree
            .get(action_id.as_bytes())
            .with_context(|| {
                format!(
                    "reading failed_by_action row before delete for {}",
                    action_id
                )
            })?
            .is_some()
        {
            deleted_failed_action_record_files += 1;
            if !dry_run {
                failed_tree
                    .remove(action_id.as_bytes())
                    .with_context(|| format!("deleting failed_by_action row for {}", action_id))?;
            }
        }
        if remove_dir_if_exists(
            &materialized_action_dir_path(store_root, action_id),
            dry_run,
        )? {
            deleted_materialized_action_dirs += 1;
        }

        if last_delete_progress.elapsed().as_secs() >= 5 {
            eprintln!(
                "prune-sled-actions-by-relpath-size: pruning progress actions={}/{} provenance_rows={} action_file_rows={} queue_files={} elapsed_s={:.1}",
                processed_actions,
                total_actions_to_prune,
                deleted_provenance_rows,
                deleted_action_file_rows,
                deleted_queue_pending_files
                    + deleted_queue_running_files
                    + deleted_queue_done_files
                    + deleted_queue_failed_files
                    + deleted_queue_canceled_files,
                started.elapsed().as_secs_f64()
            );
            last_delete_progress = Instant::now();
        }
    }

    if !dry_run {
        db.flush().context("flushing sled db after prune")?;
    }

    Ok(SledPruneActionsByRelpathSizeSummary {
        db_path: db_path.display().to_string(),
        store_root: store_root.display().to_string(),
        relpath: relpath.to_string(),
        min_bytes,
        include_downstream,
        dry_run,
        seed_action_count: seed_action_ids.len() as u64,
        downstream_action_count,
        total_action_count: action_ids_to_prune.len() as u64,
        seed_action_ids_sample: seed_action_ids.into_iter().take(25).collect(),
        deleted_provenance_rows,
        deleted_action_file_rows,
        deleted_action_file_key_bytes,
        deleted_action_file_stored_value_bytes,
        deleted_action_file_decoded_value_bytes,
        deleted_queue_pending_files,
        deleted_queue_running_files,
        deleted_queue_done_files,
        deleted_queue_failed_files,
        deleted_queue_canceled_files,
        deleted_failed_action_record_files,
        deleted_materialized_action_dirs,
        elapsed_secs: started.elapsed().as_secs_f64(),
    })
}

pub(crate) fn backfill_sled_action_file_compression(
    db_path: &Path,
    limit: Option<usize>,
    dry_run: bool,
    target_relpath: Option<&str>,
) -> Result<SledActionFileCompressionBackfillSummary> {
    const BACKFILL_CACHE_CAPACITY_BYTES: u64 = 16 * 1024 * 1024;
    let started = Instant::now();
    eprintln!(
        "backfill-sled-action-file-compression: opening {}",
        db_path.display()
    );
    let db = match sled::Config::new()
        .path(db_path)
        .cache_capacity(BACKFILL_CACHE_CAPACITY_BYTES)
        .open()
    {
        Ok(db) => db,
        Err(err) => {
            let err_text = err.to_string();
            if err_text.contains("could not acquire lock") {
                bail!(
                    "opening sled db for backfill: {} (database lock is held by another process; stop the service first)",
                    db_path.display()
                );
            }
            return Err(err)
                .with_context(|| format!("opening sled db for backfill: {}", db_path.display()));
        }
    };
    let file_tree = db
        .open_tree(SledArtifactBackend::TREE_ACTION_FILE_BYTES)
        .context("opening sled tree action_file_bytes for backfill")?;
    let limit_rows = limit.unwrap_or(usize::MAX);
    let compress_min = SledArtifactBackend::action_file_compress_min_bytes();

    let mut scanned_entries = 0_u64;
    let mut rewritten_entries = 0_u64;
    let mut unchanged_entries = 0_u64;
    let mut already_compressed_entries = 0_u64;
    let mut written_compressed_entries = 0_u64;
    let mut written_uncompressed_entries = 0_u64;
    let mut stored_bytes_before = 0_u64;
    let mut stored_bytes_after = 0_u64;
    let mut last_progress = Instant::now();
    let mut scanned_provenance_rows = None;
    let mut process_row = |scanned_now: u64, key: sled::IVec, value: sled::IVec| -> Result<()> {
        let before_len = value.len() as u64;
        stored_bytes_before += before_len;
        let was_compressed = value.starts_with(SledArtifactBackend::ACTION_FILE_ZSTD_MAGIC);
        if was_compressed {
            already_compressed_entries += 1;
            unchanged_entries += 1;
            written_compressed_entries += 1;
            stored_bytes_after += before_len;
            if last_progress.elapsed().as_secs() >= 5 {
                eprintln!(
                    "backfill-sled-action-file-compression: scanned={} rewritten={} before_gib={:.3} after_gib={:.3} elapsed_s={:.1}",
                    scanned_now,
                    rewritten_entries,
                    stored_bytes_before as f64 / (1024.0 * 1024.0 * 1024.0),
                    stored_bytes_after as f64 / (1024.0 * 1024.0 * 1024.0),
                    started.elapsed().as_secs_f64(),
                );
                last_progress = Instant::now();
            }
            return Ok(());
        }
        if !was_compressed && value.len() < compress_min {
            unchanged_entries += 1;
            written_uncompressed_entries += 1;
            stored_bytes_after += before_len;
            if last_progress.elapsed().as_secs() >= 5 {
                eprintln!(
                    "backfill-sled-action-file-compression: scanned={} rewritten={} before_gib={:.3} after_gib={:.3} elapsed_s={:.1}",
                    scanned_now,
                    rewritten_entries,
                    stored_bytes_before as f64 / (1024.0 * 1024.0 * 1024.0),
                    stored_bytes_after as f64 / (1024.0 * 1024.0 * 1024.0),
                    started.elapsed().as_secs_f64(),
                );
                last_progress = Instant::now();
            }
            return Ok(());
        }
        let decoded =
            SledArtifactBackend::decode_action_file_value(value.as_ref()).with_context(|| {
                format!(
                    "decoding action_file_bytes row during backfill key_len={}",
                    key.len()
                )
            })?;
        let encoded =
            SledArtifactBackend::maybe_encode_action_file_value(&decoded).with_context(|| {
                format!(
                    "encoding action_file_bytes row during backfill key_len={}",
                    key.len()
                )
            })?;
        stored_bytes_after += encoded.len() as u64;
        if encoded.starts_with(SledArtifactBackend::ACTION_FILE_ZSTD_MAGIC) {
            written_compressed_entries += 1;
        } else {
            written_uncompressed_entries += 1;
        }
        if encoded.as_slice() != value.as_ref() {
            rewritten_entries += 1;
            if !dry_run {
                file_tree
                    .insert(key, encoded)
                    .context("writing rewritten action_file_bytes row")?;
            }
        } else {
            unchanged_entries += 1;
        }
        if last_progress.elapsed().as_secs() >= 5 {
            eprintln!(
                "backfill-sled-action-file-compression: scanned={} rewritten={} before_gib={:.3} after_gib={:.3} elapsed_s={:.1}",
                scanned_now,
                rewritten_entries,
                stored_bytes_before as f64 / (1024.0 * 1024.0 * 1024.0),
                stored_bytes_after as f64 / (1024.0 * 1024.0 * 1024.0),
                started.elapsed().as_secs_f64(),
            );
            last_progress = Instant::now();
        }
        Ok(())
    };
    if let Some(relpath) = target_relpath {
        let relpath_needle = relpath.as_bytes();
        for row in file_tree.iter() {
            let (key, value) =
                row.context("iterating action_file_bytes rows for targeted backfill")?;
            if scanned_entries >= limit_rows as u64 {
                break;
            }
            let key_bytes = key.as_ref();
            let Some(separator_index) = key_bytes.iter().position(|byte| *byte == 0) else {
                continue;
            };
            let key_relpath = &key_bytes[(separator_index + 1)..];
            if key_relpath != relpath_needle {
                continue;
            }
            scanned_entries += 1;
            process_row(scanned_entries, key, value)?;
        }
        scanned_provenance_rows = Some(0);
    } else {
        for row in file_tree.iter().take(limit_rows) {
            let (key, value) = row.context("iterating action_file_bytes rows for backfill")?;
            scanned_entries += 1;
            process_row(scanned_entries, key, value)?;
        }
    }
    if !dry_run {
        db.flush()
            .context("flushing sled db after action_file compression backfill")?;
    }

    let delta_i128 = i128::from(stored_bytes_after) - i128::from(stored_bytes_before);
    let stored_bytes_delta = i64::try_from(delta_i128).unwrap_or_else(|_| {
        if delta_i128.is_negative() {
            i64::MIN
        } else {
            i64::MAX
        }
    });

    Ok(SledActionFileCompressionBackfillSummary {
        db_path: db_path.display().to_string(),
        dry_run,
        limit,
        target_relpath: target_relpath.map(|v| v.to_string()),
        scanned_provenance_rows,
        scanned_entries,
        rewritten_entries,
        unchanged_entries,
        already_compressed_entries,
        written_compressed_entries,
        written_uncompressed_entries,
        stored_bytes_before,
        stored_bytes_after,
        stored_bytes_delta,
        elapsed_secs: started.elapsed().as_secs_f64(),
    })
}

impl ArtifactBackend for SledArtifactBackend {
    fn ensure_layout(&self, store_root: &Path) -> Result<()> {
        fs::create_dir_all(self.artifacts_dir(store_root))
            .context("creating materialized artifacts directory")?;
        let db = self.open_db()?;
        db.open_tree(Self::TREE_PROVENANCE_BY_ACTION)
            .context("opening sled tree provenance_by_action")?;
        db.open_tree(Self::TREE_ACTION_FILE_BYTES)
            .context("opening sled tree action_file_bytes")?;
        db.open_tree(Self::TREE_FAILED_BY_ACTION)
            .context("opening sled tree failed_by_action")?;
        db.open_tree(Self::TREE_WEB_INDEX_BYTES)
            .context("opening sled tree web_index_bytes")?;
        Ok(())
    }

    fn artifacts_dir(&self, store_root: &Path) -> PathBuf {
        Self::materialized_actions_root(store_root)
    }

    fn action_dir(&self, store_root: &Path, action_id: &str) -> PathBuf {
        self.materialized_action_dir_for(store_root, action_id)
    }

    fn materialize_action_dir(&self, store_root: &Path, action_id: &str) -> Result<PathBuf> {
        self.materialize_action_from_db(store_root, action_id)
    }

    fn promote_staging_action_dir(
        &self,
        store_root: &Path,
        action_id: &str,
        staging_dir: &Path,
    ) -> Result<()> {
        let db = self.open_db()?;
        let provenance_tree = db
            .open_tree(Self::TREE_PROVENANCE_BY_ACTION)
            .context("opening sled tree provenance_by_action")?;
        let file_tree = db
            .open_tree(Self::TREE_ACTION_FILE_BYTES)
            .context("opening sled tree action_file_bytes")?;

        let mut staged_files: Vec<(String, Vec<u8>)> = Vec::new();
        let mut provenance_bytes: Option<Vec<u8>> = None;
        for entry in WalkDir::new(staging_dir).sort_by_file_name() {
            let entry = entry.context("walking staged action directory for sled ingest")?;
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.path();
            let rel = path
                .strip_prefix(staging_dir)
                .with_context(|| format!("stripping staging prefix from {}", path.display()))?;
            let relpath = Self::normalize_relpath(rel)?;
            let bytes = fs::read(path)
                .with_context(|| format!("reading staged action file: {}", path.display()))?;
            if relpath == "provenance.json" {
                let parsed: Provenance = serde_json::from_slice(&bytes).with_context(|| {
                    format!("parsing staged provenance file: {}", path.display())
                })?;
                if parsed.action_id != action_id {
                    bail!(
                        "staged provenance action id mismatch: expected {} got {}",
                        action_id,
                        parsed.action_id
                    );
                }
                provenance_bytes = Some(bytes.clone());
            }
            staged_files.push((relpath, bytes));
        }
        let provenance_bytes = provenance_bytes.ok_or_else(|| {
            anyhow::anyhow!(
                "staged action directory missing provenance.json for action {}",
                action_id
            )
        })?;

        let prefix = Self::action_files_prefix(action_id);
        let mut existing_keys = Vec::new();
        for row in file_tree.scan_prefix(prefix) {
            let (key, _) = row.context("iterating existing sled action-file rows")?;
            existing_keys.push(key);
        }
        for key in existing_keys {
            file_tree
                .remove(key)
                .context("removing existing sled action-file row")?;
        }

        for (relpath, bytes) in &staged_files {
            let key = Self::action_file_key(action_id, relpath);
            let encoded = Self::maybe_encode_action_file_value(bytes)
                .with_context(|| format!("encoding action-file row relpath={relpath}"))?;
            file_tree
                .insert(key, encoded)
                .context("writing sled action-file row")?;
        }
        provenance_tree
            .insert(action_id.as_bytes(), provenance_bytes)
            .context("writing sled provenance row")?;
        db.flush().context("flushing sled artifact database")?;

        let materialized_dir = self.materialized_action_dir_for(store_root, action_id);
        if materialized_dir.exists() {
            fs::remove_dir_all(&materialized_dir).ok();
        }
        fs::remove_dir_all(staging_dir).ok();
        Ok(())
    }

    fn provenance_path(&self, store_root: &Path, action_id: &str) -> PathBuf {
        self.materialized_action_dir_for(store_root, action_id)
            .join("provenance.json")
    }

    fn failed_action_record_exists(&self, _store_root: &Path, action_id: &str) -> bool {
        let db = match self.open_db() {
            Ok(db) => db,
            Err(_) => return false,
        };
        let tree = match db.open_tree(Self::TREE_FAILED_BY_ACTION) {
            Ok(tree) => tree,
            Err(_) => return false,
        };
        matches!(tree.get(action_id.as_bytes()), Ok(Some(_)))
    }

    fn load_failed_action_record(
        &self,
        _store_root: &Path,
        action_id: &str,
    ) -> Result<Option<QueueFailed>> {
        let db = self.open_db()?;
        let tree = db
            .open_tree(Self::TREE_FAILED_BY_ACTION)
            .context("opening sled tree failed_by_action")?;
        if let Some(bytes) = tree
            .get(action_id.as_bytes())
            .context("loading failed action record row from sled")?
        {
            let parsed: QueueFailed = serde_json::from_slice(bytes.as_ref())
                .context("parsing failed action record row from sled")?;
            return Ok(Some(parsed));
        }
        Ok(None)
    }

    fn write_failed_action_record(&self, _store_root: &Path, failed: &QueueFailed) -> Result<()> {
        let db = self.open_db()?;
        let tree = db
            .open_tree(Self::TREE_FAILED_BY_ACTION)
            .context("opening sled tree failed_by_action")?;
        let bytes =
            serde_json::to_vec_pretty(failed).context("serializing failed action record row")?;
        tree.insert(failed.action_id.as_bytes(), bytes)
            .context("writing failed action record row to sled")?;
        db.flush()
            .context("flushing sled artifact database after failed record write")?;
        Ok(())
    }

    fn delete_failed_action_record(&self, _store_root: &Path, action_id: &str) -> Result<bool> {
        let db = self.open_db()?;
        let tree = db
            .open_tree(Self::TREE_FAILED_BY_ACTION)
            .context("opening sled tree failed_by_action")?;
        let mut deleted = false;
        if tree
            .remove(action_id.as_bytes())
            .context("removing failed action record row from sled")?
            .is_some()
        {
            deleted = true;
        }
        if deleted {
            db.flush()
                .context("flushing sled artifact database after failed record delete")?;
        }
        Ok(deleted)
    }

    fn action_exists(&self, _store_root: &Path, action_id: &str) -> bool {
        let Ok(db) = self.open_db() else {
            return false;
        };
        let Ok(tree) = db.open_tree(Self::TREE_PROVENANCE_BY_ACTION) else {
            return false;
        };
        matches!(tree.get(action_id.as_bytes()), Ok(Some(_)))
    }

    fn load_provenance(&self, _store_root: &Path, action_id: &str) -> Result<Provenance> {
        let db = self.open_db()?;
        let tree = db
            .open_tree(Self::TREE_PROVENANCE_BY_ACTION)
            .context("opening sled tree provenance_by_action")?;
        let bytes = tree
            .get(action_id.as_bytes())
            .context("loading provenance row from sled")?
            .ok_or_else(|| anyhow::anyhow!("provenance not found for action {}", action_id))?;
        let parsed: Provenance =
            serde_json::from_slice(bytes.as_ref()).context("parsing provenance row from sled")?;
        Ok(parsed)
    }

    fn write_provenance(&self, store_root: &Path, provenance: &Provenance) -> Result<()> {
        let db = self.open_db()?;
        let tree = db
            .open_tree(Self::TREE_PROVENANCE_BY_ACTION)
            .context("opening sled tree provenance_by_action")?;
        let bytes = serde_json::to_vec_pretty(provenance).context("serializing provenance row")?;
        tree.insert(provenance.action_id.as_bytes(), bytes)
            .context("writing provenance row to sled")?;
        db.flush().context("flushing sled artifact database")?;
        let materialized_dir = self.materialized_action_dir_for(store_root, &provenance.action_id);
        if materialized_dir.exists() {
            fs::remove_dir_all(&materialized_dir).ok();
        }
        Ok(())
    }

    fn for_each_provenance(
        &self,
        _store_root: &Path,
        visitor: &mut dyn FnMut(Provenance) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        let db = self.open_db()?;
        let tree = db
            .open_tree(Self::TREE_PROVENANCE_BY_ACTION)
            .context("opening sled tree provenance_by_action")?;
        for row in tree.iter() {
            let (_, value) = row.context("iterating sled provenance rows")?;
            let provenance: Provenance =
                serde_json::from_slice(value.as_ref()).context("parsing sled provenance row")?;
            match visitor(provenance)? {
                ControlFlow::Continue(()) => {}
                ControlFlow::Break(()) => break,
            }
        }
        Ok(())
    }

    fn resolve_artifact_ref_path(&self, store_root: &Path, artifact_ref: &ArtifactRef) -> PathBuf {
        if let Err(err) = self.materialize_action_from_db(store_root, &artifact_ref.action_id) {
            eprintln!(
                "warning: failed to materialize action {} from sled backend: {:#}",
                artifact_ref.action_id, err
            );
        }
        self.materialized_action_dir_for(store_root, &artifact_ref.action_id)
            .join(
                artifact_ref
                    .relpath
                    .replace('/', std::path::MAIN_SEPARATOR_STR),
            )
    }

    fn list_provenances(&self, _store_root: &Path) -> Result<Vec<Provenance>> {
        let mut provenances = Vec::new();
        self.for_each_provenance(_store_root, &mut |provenance| {
            provenances.push(provenance);
            Ok(ControlFlow::Continue(()))
        })?;
        provenances.sort_by(|a, b| a.action_id.cmp(&b.action_id));
        Ok(provenances)
    }

    fn load_failed_action_records(&self, store_root: &Path) -> Result<Vec<QueueFailed>> {
        let db = self.open_db()?;
        let tree = db
            .open_tree(Self::TREE_FAILED_BY_ACTION)
            .context("opening sled tree failed_by_action")?;
        let _ = store_root;
        let mut records = Vec::new();

        for row in tree.iter() {
            let (_key, value) = row.context("iterating failed action record rows from sled")?;
            let record: QueueFailed = serde_json::from_slice(value.as_ref())
                .context("parsing failed action record row from sled")?;
            records.push(record);
        }
        records.sort_by(|a, b| a.action_id.cmp(&b.action_id));
        Ok(records)
    }

    fn load_web_index_bytes(&self, _store_root: &Path, index_key: &str) -> Result<Option<Vec<u8>>> {
        let db = self.open_db()?;
        let tree = db
            .open_tree(Self::TREE_WEB_INDEX_BYTES)
            .context("opening sled tree web_index_bytes")?;
        let value = tree
            .get(index_key.as_bytes())
            .context("loading web index row from sled")?;
        Ok(value.map(|v| v.as_ref().to_vec()))
    }

    fn write_web_index_bytes(
        &self,
        _store_root: &Path,
        index_key: &str,
        bytes: &[u8],
    ) -> Result<()> {
        let db = self.open_db()?;
        let tree = db
            .open_tree(Self::TREE_WEB_INDEX_BYTES)
            .context("opening sled tree web_index_bytes")?;
        tree.insert(index_key.as_bytes(), bytes)
            .context("writing web index row to sled")?;
        Ok(())
    }

    fn list_web_index_entries_with_prefix(
        &self,
        _store_root: &Path,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let db = self.open_db()?;
        let tree = db
            .open_tree(Self::TREE_WEB_INDEX_BYTES)
            .context("opening sled tree web_index_bytes")?;
        let mut rows = Vec::new();
        for row in tree.scan_prefix(prefix.as_bytes()) {
            let (key, value) = row.context("iterating web index rows for prefix list")?;
            let key = String::from_utf8(key.as_ref().to_vec())
                .context("decoding web index key as UTF-8")?;
            rows.push((key, value.as_ref().to_vec()));
        }
        Ok(rows)
    }

    fn delete_web_index_keys_with_prefix(&self, _store_root: &Path, prefix: &str) -> Result<usize> {
        let db = self.open_db()?;
        let tree = db
            .open_tree(Self::TREE_WEB_INDEX_BYTES)
            .context("opening sled tree web_index_bytes")?;
        let mut keys = Vec::new();
        for row in tree.scan_prefix(prefix.as_bytes()) {
            let (key, _) = row.context("iterating web index rows for prefix delete")?;
            keys.push(key);
        }
        let deleted = keys.len();
        for key in keys {
            tree.remove(key)
                .context("removing web index row during prefix delete")?;
        }
        db.flush().context("flushing sled artifact database")?;
        Ok(deleted)
    }

    fn web_index_location(&self, _store_root: &Path, index_key: &str) -> String {
        format!("sled://{}/{}", Self::TREE_WEB_INDEX_BYTES, index_key)
    }

    fn size_on_disk_bytes(&self, _store_root: &Path) -> Result<u64> {
        let db = self.open_db()?;
        db.size_on_disk().context("querying sled size_on_disk")
    }

    fn flush_durable(&self) -> Result<()> {
        let db = self.open_db()?;
        db.flush().context("flushing sled artifact database")?;
        Ok(())
    }
}

#[derive(Debug)]
struct SnapshotArtifactBackend {
    snapshot_dir: PathBuf,
}

impl SnapshotArtifactBackend {
    fn web_index_root(&self) -> PathBuf {
        self.snapshot_dir
            .join(crate::snapshot::STATIC_SNAPSHOT_WEB_INDEX_DIR)
    }

    fn dir_size_bytes(path: &Path) -> Result<u64> {
        let mut total = 0_u64;
        if !path.exists() {
            return Ok(0);
        }
        for entry in WalkDir::new(path) {
            let entry =
                entry.with_context(|| format!("walking snapshot path {}", path.display()))?;
            if !entry.file_type().is_file() {
                continue;
            }
            let metadata = entry
                .metadata()
                .with_context(|| format!("reading metadata for {}", entry.path().display()))?;
            total = total.saturating_add(metadata.len());
        }
        Ok(total)
    }
}

impl ArtifactBackend for SnapshotArtifactBackend {
    fn ensure_layout(&self, _store_root: &Path) -> Result<()> {
        if !self.snapshot_dir.is_dir() {
            bail!(
                "snapshot directory does not exist or is not a directory: {}",
                self.snapshot_dir.display()
            );
        }
        let manifest_path = self
            .snapshot_dir
            .join(crate::snapshot::STATIC_SNAPSHOT_MANIFEST_FILENAME);
        if !manifest_path.is_file() {
            bail!("snapshot manifest missing: {}", manifest_path.display());
        }
        Ok(())
    }

    fn artifacts_dir(&self, _store_root: &Path) -> PathBuf {
        self.snapshot_dir.join("artifacts")
    }

    fn action_dir(&self, _store_root: &Path, action_id: &str) -> PathBuf {
        shard_dir(&self.snapshot_dir.join("actions"), action_id).join(action_id)
    }

    fn materialize_action_dir(&self, _store_root: &Path, action_id: &str) -> Result<PathBuf> {
        bail!(
            "materialize_action_dir is unavailable in snapshot read-only backend (action_id={})",
            action_id
        )
    }

    fn promote_staging_action_dir(
        &self,
        _store_root: &Path,
        action_id: &str,
        _staging_dir: &Path,
    ) -> Result<()> {
        bail!(
            "promote_staging_action_dir is unavailable in snapshot read-only backend (action_id={})",
            action_id
        )
    }

    fn provenance_path(&self, _store_root: &Path, action_id: &str) -> PathBuf {
        shard_dir(&self.snapshot_dir.join("provenance"), action_id)
            .join(format!("{action_id}.json"))
    }

    fn failed_action_record_exists(&self, _store_root: &Path, _action_id: &str) -> bool {
        false
    }

    fn load_failed_action_record(
        &self,
        _store_root: &Path,
        _action_id: &str,
    ) -> Result<Option<QueueFailed>> {
        Ok(None)
    }

    fn write_failed_action_record(&self, _store_root: &Path, failed: &QueueFailed) -> Result<()> {
        bail!(
            "write_failed_action_record is unavailable in snapshot read-only backend (action_id={})",
            failed.action_id
        )
    }

    fn delete_failed_action_record(&self, _store_root: &Path, _action_id: &str) -> Result<bool> {
        Ok(false)
    }

    fn action_exists(&self, _store_root: &Path, _action_id: &str) -> bool {
        false
    }

    fn load_provenance(&self, _store_root: &Path, action_id: &str) -> Result<Provenance> {
        bail!(
            "load_provenance is unavailable in snapshot read-only backend (action_id={})",
            action_id
        )
    }

    fn write_provenance(&self, _store_root: &Path, provenance: &Provenance) -> Result<()> {
        bail!(
            "write_provenance is unavailable in snapshot read-only backend (action_id={})",
            provenance.action_id
        )
    }

    fn for_each_provenance(
        &self,
        _store_root: &Path,
        _visitor: &mut dyn FnMut(Provenance) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        Ok(())
    }

    fn resolve_artifact_ref_path(&self, _store_root: &Path, artifact_ref: &ArtifactRef) -> PathBuf {
        self.snapshot_dir.join("artifacts").join(
            artifact_ref
                .relpath
                .replace('/', std::path::MAIN_SEPARATOR_STR),
        )
    }

    fn list_provenances(&self, _store_root: &Path) -> Result<Vec<Provenance>> {
        Ok(Vec::new())
    }

    fn load_failed_action_records(&self, _store_root: &Path) -> Result<Vec<QueueFailed>> {
        Ok(Vec::new())
    }

    fn load_web_index_bytes(&self, _store_root: &Path, index_key: &str) -> Result<Option<Vec<u8>>> {
        let path = snapshot_web_index_path(&self.snapshot_dir, index_key)?;
        match fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err).with_context(|| {
                format!(
                    "reading snapshot web index key {} from {}",
                    index_key,
                    path.display()
                )
            }),
        }
    }

    fn write_web_index_bytes(
        &self,
        _store_root: &Path,
        index_key: &str,
        _bytes: &[u8],
    ) -> Result<()> {
        bail!(
            "write_web_index_bytes is unavailable in snapshot read-only backend (index_key={})",
            index_key
        )
    }

    fn list_web_index_entries_with_prefix(
        &self,
        _store_root: &Path,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let web_index_root = self.web_index_root();
        if !web_index_root.exists() {
            return Ok(Vec::new());
        }
        let mut entries = Vec::new();
        for entry in WalkDir::new(&web_index_root) {
            let entry = entry.with_context(|| {
                format!(
                    "walking snapshot web index root {}",
                    web_index_root.display()
                )
            })?;
            if !entry.file_type().is_file() {
                continue;
            }
            let rel = entry
                .path()
                .strip_prefix(&web_index_root)
                .with_context(|| {
                    format!(
                        "stripping snapshot web index root prefix: {} from {}",
                        web_index_root.display(),
                        entry.path().display()
                    )
                })?
                .to_string_lossy()
                .replace('\\', "/");
            if !rel.starts_with(prefix) {
                continue;
            }
            let bytes = fs::read(entry.path()).with_context(|| {
                format!("reading snapshot web index file {}", entry.path().display())
            })?;
            entries.push((rel, bytes));
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(entries)
    }

    fn delete_web_index_keys_with_prefix(&self, _store_root: &Path, prefix: &str) -> Result<usize> {
        bail!(
            "delete_web_index_keys_with_prefix is unavailable in snapshot read-only backend (prefix={})",
            prefix
        )
    }

    fn web_index_location(&self, _store_root: &Path, index_key: &str) -> String {
        format!(
            "snapshot://{}/{}",
            crate::snapshot::STATIC_SNAPSHOT_WEB_INDEX_DIR,
            index_key
        )
    }

    fn size_on_disk_bytes(&self, _store_root: &Path) -> Result<u64> {
        Self::dir_size_bytes(&self.snapshot_dir)
    }

    fn flush_durable(&self) -> Result<()> {
        Ok(())
    }
}

const DEFAULT_STORE_LIST_CACHE_TTL_SECS: u64 = 60;

#[derive(Debug)]
struct TimedCache<T> {
    loaded_at: Instant,
    value: Arc<T>,
}

#[derive(Debug)]
pub(crate) struct ArtifactStore {
    pub(crate) root: PathBuf,
    artifact_backend_selection: ArtifactBackendSelection,
    artifact_backend: Box<dyn ArtifactBackend>,
    list_cache_ttl: Duration,
    provenance_cache: Mutex<Option<TimedCache<Vec<Provenance>>>>,
    failed_records_cache: Mutex<Option<TimedCache<Vec<QueueFailed>>>>,
    db_size_cache: Mutex<Option<TimedCache<u64>>>,
}

impl ArtifactStore {
    fn list_cache_ttl_from_env() -> Duration {
        let secs = std::env::var("BVC_STORE_LIST_CACHE_TTL_SECS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(DEFAULT_STORE_LIST_CACHE_TTL_SECS);
        Duration::from_secs(secs)
    }

    #[cfg(test)]
    pub(crate) fn new(root: PathBuf) -> Self {
        Self::new_with_sled(root.clone(), root.join("artifacts.test.sled"))
    }

    pub(crate) fn new_with_sled(root: PathBuf, db_path: PathBuf) -> Self {
        let artifact_backend_selection = ArtifactBackendSelection::Sled {
            db_path: db_path.clone(),
        };
        let artifact_backend: Box<dyn ArtifactBackend> = Box::new(SledArtifactBackend {
            db_path,
            db: Mutex::new(None),
        });
        Self {
            root,
            artifact_backend_selection,
            artifact_backend,
            list_cache_ttl: Self::list_cache_ttl_from_env(),
            provenance_cache: Mutex::new(None),
            failed_records_cache: Mutex::new(None),
            db_size_cache: Mutex::new(None),
        }
    }

    pub(crate) fn new_with_snapshot(root: PathBuf, snapshot_dir: PathBuf) -> Self {
        let artifact_backend_selection = ArtifactBackendSelection::Snapshot {
            snapshot_dir: snapshot_dir.clone(),
        };
        let artifact_backend: Box<dyn ArtifactBackend> =
            Box::new(SnapshotArtifactBackend { snapshot_dir });
        Self {
            root,
            artifact_backend_selection,
            artifact_backend,
            list_cache_ttl: Self::list_cache_ttl_from_env(),
            provenance_cache: Mutex::new(None),
            failed_records_cache: Mutex::new(None),
            db_size_cache: Mutex::new(None),
        }
    }

    pub(crate) fn ensure_layout(&self) -> Result<()> {
        self.artifact_backend.ensure_layout(&self.root)?;
        fs::create_dir_all(self.staging_dir()).context("creating staging directory")?;
        fs::create_dir_all(self.driver_release_cache_root())
            .context("creating driver release cache directory")?;
        fs::create_dir_all(self.queue_pending_dir()).context("creating queue pending directory")?;
        fs::create_dir_all(self.queue_running_dir()).context("creating queue running directory")?;
        fs::create_dir_all(self.queue_done_dir()).context("creating queue done directory")?;
        fs::create_dir_all(self.queue_canceled_dir())
            .context("creating queue canceled directory")?;
        Ok(())
    }

    pub(crate) fn artifacts_sled_db_path(&self) -> PathBuf {
        match &self.artifact_backend_selection {
            ArtifactBackendSelection::Sled { db_path } => db_path.clone(),
            ArtifactBackendSelection::Snapshot { snapshot_dir } => snapshot_dir.clone(),
        }
    }

    fn backend_label(&self) -> &'static str {
        match &self.artifact_backend_selection {
            ArtifactBackendSelection::Sled { .. } => "sled",
            ArtifactBackendSelection::Snapshot { .. } => "snapshot",
        }
    }

    pub(crate) fn is_snapshot_backend(&self) -> bool {
        matches!(
            self.artifact_backend_selection,
            ArtifactBackendSelection::Snapshot { .. }
        )
    }

    pub(crate) fn snapshot_dir(&self) -> Option<PathBuf> {
        match &self.artifact_backend_selection {
            ArtifactBackendSelection::Snapshot { snapshot_dir } => Some(snapshot_dir.clone()),
            ArtifactBackendSelection::Sled { .. } => None,
        }
    }

    fn invalidate_list_caches(&self) {
        if let Ok(mut cached) = self.provenance_cache.lock() {
            *cached = None;
        }
        if let Ok(mut cached) = self.failed_records_cache.lock() {
            *cached = None;
        }
        if let Ok(mut cached) = self.db_size_cache.lock() {
            *cached = None;
        }
    }

    #[cfg(test)]
    pub(crate) fn artifacts_dir(&self) -> PathBuf {
        self.artifact_backend.artifacts_dir(&self.root)
    }

    pub(crate) fn staging_dir(&self) -> PathBuf {
        self.root.join(".staging")
    }

    pub(crate) fn driver_release_cache_root(&self) -> PathBuf {
        self.root.join(crate::DRIVER_RELEASE_CACHE_DIR)
    }

    pub(crate) fn driver_release_cache_dir(&self, version: &str, platform: &str) -> PathBuf {
        self.driver_release_cache_root()
            .join(version)
            .join(platform)
    }

    pub(crate) fn queue_root(&self) -> PathBuf {
        self.root.join("queue")
    }

    pub(crate) fn queue_pending_dir(&self) -> PathBuf {
        self.queue_root().join("pending")
    }

    pub(crate) fn queue_running_dir(&self) -> PathBuf {
        self.queue_root().join("running")
    }

    pub(crate) fn queue_done_dir(&self) -> PathBuf {
        self.queue_root().join("done")
    }

    pub(crate) fn queue_canceled_dir(&self) -> PathBuf {
        self.queue_root().join("canceled")
    }

    pub(crate) fn action_dir(&self, action_id: &str) -> PathBuf {
        self.artifact_backend.action_dir(&self.root, action_id)
    }

    pub(crate) fn materialize_action_dir(&self, action_id: &str) -> Result<PathBuf> {
        self.artifact_backend
            .materialize_action_dir(&self.root, action_id)
    }

    pub(crate) fn promote_staging_action_dir(
        &self,
        action_id: &str,
        staging_dir: &Path,
    ) -> Result<()> {
        self.artifact_backend
            .promote_staging_action_dir(&self.root, action_id, staging_dir)?;
        self.invalidate_list_caches();
        Ok(())
    }

    pub(crate) fn provenance_path(&self, action_id: &str) -> PathBuf {
        self.artifact_backend.provenance_path(&self.root, action_id)
    }

    pub(crate) fn action_exists(&self, action_id: &str) -> bool {
        self.artifact_backend.action_exists(&self.root, action_id)
    }

    pub(crate) fn pending_queue_path(&self, action_id: &str) -> PathBuf {
        self.shard_dir(self.queue_pending_dir(), action_id)
            .join(format!("{action_id}.json"))
    }

    pub(crate) fn running_queue_path(&self, action_id: &str) -> PathBuf {
        self.shard_dir(self.queue_running_dir(), action_id)
            .join(format!("{action_id}.json"))
    }

    pub(crate) fn done_queue_path(&self, action_id: &str) -> PathBuf {
        self.shard_dir(self.queue_done_dir(), action_id)
            .join(format!("{action_id}.json"))
    }

    pub(crate) fn failed_action_record_exists(&self, action_id: &str) -> bool {
        self.artifact_backend
            .failed_action_record_exists(&self.root, action_id)
    }

    pub(crate) fn canceled_queue_path(&self, action_id: &str) -> PathBuf {
        self.shard_dir(self.queue_canceled_dir(), action_id)
            .join(format!("{action_id}.json"))
    }

    pub(crate) fn load_provenance(&self, action_id: &str) -> Result<Provenance> {
        self.artifact_backend.load_provenance(&self.root, action_id)
    }

    pub(crate) fn write_provenance(&self, provenance: &Provenance) -> Result<()> {
        self.artifact_backend
            .write_provenance(&self.root, provenance)?;
        self.invalidate_list_caches();
        Ok(())
    }

    pub(crate) fn for_each_provenance(
        &self,
        mut visitor: impl FnMut(Provenance) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        self.artifact_backend
            .for_each_provenance(&self.root, &mut visitor)
    }

    pub(crate) fn resolve_artifact_ref_path(&self, artifact_ref: &ArtifactRef) -> PathBuf {
        self.artifact_backend
            .resolve_artifact_ref_path(&self.root, artifact_ref)
    }

    pub(crate) fn list_provenances_shared(&self) -> Result<Arc<Vec<Provenance>>> {
        if self.list_cache_ttl.is_zero() {
            let started = Instant::now();
            let provenances = self.artifact_backend.list_provenances(&self.root)?;
            info!(
                "store.list_provenances cache_disabled backend={} count={} elapsed_ms={}",
                self.backend_label(),
                provenances.len(),
                started.elapsed().as_millis()
            );
            return Ok(Arc::new(provenances));
        }

        let cache_started = Instant::now();
        let mut cache = self
            .provenance_cache
            .lock()
            .map_err(|_| anyhow!("locking provenance cache"))?;
        if let Some(cached) = cache.as_ref()
            && cached.loaded_at.elapsed() <= self.list_cache_ttl
        {
            let shared = cached.value.clone();
            let age_ms = cached.loaded_at.elapsed().as_millis();
            info!(
                "store.list_provenances cache_hit backend={} count={} age_ms={} elapsed_ms={}",
                self.backend_label(),
                shared.len(),
                age_ms,
                cache_started.elapsed().as_millis(),
            );
            return Ok(shared);
        }

        let started = Instant::now();
        let result = self.artifact_backend.list_provenances(&self.root);
        match result {
            Ok(provenances) => {
                let count = provenances.len();
                let shared = Arc::new(provenances);
                *cache = Some(TimedCache {
                    loaded_at: Instant::now(),
                    value: shared.clone(),
                });
                drop(cache);
                info!(
                    "store.list_provenances cache_miss backend={} count={} elapsed_ms={}",
                    self.backend_label(),
                    count,
                    started.elapsed().as_millis()
                );
                Ok(shared)
            }
            Err(err) => {
                warn!(
                    "store.list_provenances backend={} elapsed_ms={} error={:#}",
                    self.backend_label(),
                    started.elapsed().as_millis(),
                    err
                );
                Err(err)
            }
        }
    }

    pub(crate) fn list_provenances(&self) -> Result<Vec<Provenance>> {
        Ok(self.list_provenances_shared()?.as_ref().clone())
    }

    pub(crate) fn load_failed_action_records_shared(&self) -> Result<Arc<Vec<QueueFailed>>> {
        if self.list_cache_ttl.is_zero() {
            let started = Instant::now();
            let records = self
                .artifact_backend
                .load_failed_action_records(&self.root)?;
            info!(
                "store.load_failed_action_records cache_disabled backend={} count={} elapsed_ms={}",
                self.backend_label(),
                records.len(),
                started.elapsed().as_millis()
            );
            return Ok(Arc::new(records));
        }

        let cache_started = Instant::now();
        let mut cache = self
            .failed_records_cache
            .lock()
            .map_err(|_| anyhow!("locking failed records cache"))?;
        if let Some(cached) = cache.as_ref()
            && cached.loaded_at.elapsed() <= self.list_cache_ttl
        {
            let shared = cached.value.clone();
            let age_ms = cached.loaded_at.elapsed().as_millis();
            info!(
                "store.load_failed_action_records cache_hit backend={} count={} age_ms={} elapsed_ms={}",
                self.backend_label(),
                shared.len(),
                age_ms,
                cache_started.elapsed().as_millis(),
            );
            return Ok(shared);
        }

        let started = Instant::now();
        let result = self.artifact_backend.load_failed_action_records(&self.root);
        match result {
            Ok(records) => {
                let count = records.len();
                let shared = Arc::new(records);
                *cache = Some(TimedCache {
                    loaded_at: Instant::now(),
                    value: shared.clone(),
                });
                drop(cache);
                info!(
                    "store.load_failed_action_records cache_miss backend={} count={} elapsed_ms={}",
                    self.backend_label(),
                    count,
                    started.elapsed().as_millis()
                );
                Ok(shared)
            }
            Err(err) => {
                warn!(
                    "store.load_failed_action_records backend={} elapsed_ms={} error={:#}",
                    self.backend_label(),
                    started.elapsed().as_millis(),
                    err
                );
                Err(err)
            }
        }
    }

    pub(crate) fn load_failed_action_records(&self) -> Result<Vec<QueueFailed>> {
        Ok(self.load_failed_action_records_shared()?.as_ref().clone())
    }

    pub(crate) fn artifacts_db_size_bytes(&self) -> Result<u64> {
        if self.list_cache_ttl.is_zero() {
            let started = Instant::now();
            let size = self.artifact_backend.size_on_disk_bytes(&self.root)?;
            info!(
                "store.artifacts_db_size cache_disabled backend={} bytes={} elapsed_ms={}",
                self.backend_label(),
                size,
                started.elapsed().as_millis()
            );
            return Ok(size);
        }

        let cache_started = Instant::now();
        let mut cache = self
            .db_size_cache
            .lock()
            .map_err(|_| anyhow!("locking db size cache"))?;
        if let Some(cached) = cache.as_ref()
            && cached.loaded_at.elapsed() <= self.list_cache_ttl
        {
            let size = *cached.value;
            let age_ms = cached.loaded_at.elapsed().as_millis();
            info!(
                "store.artifacts_db_size cache_hit backend={} bytes={} age_ms={} elapsed_ms={}",
                self.backend_label(),
                size,
                age_ms,
                cache_started.elapsed().as_millis(),
            );
            return Ok(size);
        }

        let started = Instant::now();
        let result = self.artifact_backend.size_on_disk_bytes(&self.root);
        match result {
            Ok(size) => {
                *cache = Some(TimedCache {
                    loaded_at: Instant::now(),
                    value: Arc::new(size),
                });
                drop(cache);
                info!(
                    "store.artifacts_db_size cache_miss backend={} bytes={} elapsed_ms={}",
                    self.backend_label(),
                    size,
                    started.elapsed().as_millis()
                );
                Ok(size)
            }
            Err(err) => {
                warn!(
                    "store.artifacts_db_size backend={} elapsed_ms={} error={:#}",
                    self.backend_label(),
                    started.elapsed().as_millis(),
                    err
                );
                Err(err)
            }
        }
    }

    pub(crate) fn load_failed_action_record(&self, action_id: &str) -> Result<Option<QueueFailed>> {
        self.artifact_backend
            .load_failed_action_record(&self.root, action_id)
    }

    pub(crate) fn write_failed_action_record(&self, failed: &QueueFailed) -> Result<()> {
        self.artifact_backend
            .write_failed_action_record(&self.root, failed)?;
        self.invalidate_list_caches();
        Ok(())
    }

    pub(crate) fn delete_failed_action_record(&self, action_id: &str) -> Result<bool> {
        let deleted = self
            .artifact_backend
            .delete_failed_action_record(&self.root, action_id)?;
        if deleted {
            self.invalidate_list_caches();
        }
        Ok(deleted)
    }

    pub(crate) fn shard_dir(&self, base: PathBuf, key: &str) -> PathBuf {
        shard_dir(&base, key)
    }

    pub(crate) fn load_web_index_bytes(&self, index_key: &str) -> Result<Option<Vec<u8>>> {
        self.artifact_backend
            .load_web_index_bytes(&self.root, index_key)
    }

    pub(crate) fn write_web_index_bytes(&self, index_key: &str, bytes: &[u8]) -> Result<()> {
        self.artifact_backend
            .write_web_index_bytes(&self.root, index_key, bytes)
    }

    pub(crate) fn list_web_index_entries_with_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        self.artifact_backend
            .list_web_index_entries_with_prefix(&self.root, prefix)
    }

    pub(crate) fn delete_web_index_keys_with_prefix(&self, prefix: &str) -> Result<usize> {
        self.artifact_backend
            .delete_web_index_keys_with_prefix(&self.root, prefix)
    }

    pub(crate) fn web_index_location(&self, index_key: &str) -> String {
        self.artifact_backend
            .web_index_location(&self.root, index_key)
    }

    pub(crate) fn flush_durable(&self) -> Result<()> {
        self.artifact_backend.flush_durable()
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct IngestLegacyFailedRecordsSummary {
    pub(crate) store_root: String,
    pub(crate) dry_run: bool,
    pub(crate) keep_legacy_files: bool,
    pub(crate) scanned_legacy_failed_action_record_files: u64,
    pub(crate) scanned_legacy_queue_failed_files: u64,
    pub(crate) parsed_records: u64,
    pub(crate) parse_errors: u64,
    pub(crate) ingested_into_sled: u64,
    pub(crate) skipped_existing_in_sled: u64,
    pub(crate) removed_legacy_failed_action_records_dir: bool,
    pub(crate) removed_legacy_queue_failed_dir: bool,
    pub(crate) elapsed_secs: f64,
}

pub(crate) fn ingest_legacy_failed_records(
    store: &ArtifactStore,
    dry_run: bool,
    keep_legacy_files: bool,
) -> Result<IngestLegacyFailedRecordsSummary> {
    let started = Instant::now();
    let legacy_failed_records_root = store.root.join("failed-action-records");
    let legacy_queue_failed_root = store.queue_root().join("failed");
    let mut scanned_legacy_failed_action_record_files = 0_u64;
    let mut scanned_legacy_queue_failed_files = 0_u64;
    let mut parsed_records = 0_u64;
    let mut parse_errors = 0_u64;
    let mut ingested_into_sled = 0_u64;
    let mut skipped_existing_in_sled = 0_u64;

    let mut load_records_from_dir = |root: &Path,
                                     count: &mut u64,
                                     file_matches: &dyn Fn(&Path) -> bool|
     -> Vec<QueueFailed> {
        let mut records = Vec::new();
        if !root.exists() {
            return records;
        }
        for entry in WalkDir::new(root).sort_by_file_name() {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    warn!(
                        "skipping unreadable legacy failed-record walk entry root={} error={:#}",
                        root.display(),
                        err
                    );
                    continue;
                }
            };
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.path();
            if !file_matches(path) {
                continue;
            }
            *count += 1;
            let text = match fs::read_to_string(path) {
                Ok(text) => text,
                Err(err) => {
                    parse_errors += 1;
                    warn!(
                        "skipping legacy failed record; read failed path={} error={:#}",
                        path.display(),
                        err
                    );
                    continue;
                }
            };
            let record: QueueFailed = match serde_json::from_str(&text) {
                Ok(record) => record,
                Err(err) => {
                    parse_errors += 1;
                    warn!(
                        "skipping legacy failed record; parse failed path={} error={:#}",
                        path.display(),
                        err
                    );
                    continue;
                }
            };
            parsed_records += 1;
            records.push(record);
        }
        records
    };

    let mut records = load_records_from_dir(
        &legacy_failed_records_root,
        &mut scanned_legacy_failed_action_record_files,
        &|path| path.file_name().and_then(|name| name.to_str()) == Some("failed.json"),
    );
    records.extend(load_records_from_dir(
        &legacy_queue_failed_root,
        &mut scanned_legacy_queue_failed_files,
        &|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"),
    ));
    records.sort_by(|a, b| a.action_id.cmp(&b.action_id));

    for record in records {
        if store.failed_action_record_exists(&record.action_id) {
            skipped_existing_in_sled += 1;
            continue;
        }
        ingested_into_sled += 1;
        if !dry_run {
            store.write_failed_action_record(&record)?;
        }
    }

    let mut removed_legacy_failed_action_records_dir = false;
    let mut removed_legacy_queue_failed_dir = false;
    if !dry_run && !keep_legacy_files && parse_errors == 0 {
        if legacy_failed_records_root.exists() {
            fs::remove_dir_all(&legacy_failed_records_root).with_context(|| {
                format!(
                    "removing legacy failed-action-records directory: {}",
                    legacy_failed_records_root.display()
                )
            })?;
            removed_legacy_failed_action_records_dir = true;
        }
        if legacy_queue_failed_root.exists() {
            fs::remove_dir_all(&legacy_queue_failed_root).with_context(|| {
                format!(
                    "removing legacy queue failed directory: {}",
                    legacy_queue_failed_root.display()
                )
            })?;
            removed_legacy_queue_failed_dir = true;
        }
    } else if !dry_run && !keep_legacy_files && parse_errors > 0 {
        warn!(
            "legacy failed-record ingest parsed with errors (parse_errors={}); keeping legacy directories for manual inspection",
            parse_errors
        );
    }

    Ok(IngestLegacyFailedRecordsSummary {
        store_root: store.root.display().to_string(),
        dry_run,
        keep_legacy_files,
        scanned_legacy_failed_action_record_files,
        scanned_legacy_queue_failed_files,
        parsed_records,
        parse_errors,
        ingested_into_sled,
        skipped_existing_in_sled,
        removed_legacy_failed_action_records_dir,
        removed_legacy_queue_failed_dir,
        elapsed_secs: started.elapsed().as_secs_f64(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        ActionSpec, ArtifactRef, ArtifactType, CommandTrace, DriverRuntimeSpec, OutputFile,
        Provenance, QueueFailed, QueueItem,
    };
    use chrono::Utc;
    use serde_json::json;
    use std::ops::ControlFlow;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_test_root(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
    }

    fn make_test_provenance(action_id: &str, output_path: &str, output_bytes: u64) -> Provenance {
        Provenance {
            schema_version: 1,
            action_id: action_id.to_string(),
            created_utc: Utc::now(),
            action: ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                version: "v0.37.0".to_string(),
                discovery_runtime: None,
            },
            dependencies: Vec::new(),
            output_artifact: ArtifactRef {
                action_id: action_id.to_string(),
                artifact_type: ArtifactType::DslxFileSubtree,
                relpath: "payload".to_string(),
            },
            output_files: vec![OutputFile {
                path: output_path.to_string(),
                bytes: output_bytes,
                sha256: "a".repeat(64),
            }],
            commands: vec![CommandTrace {
                argv: vec!["echo".to_string(), "ok".to_string()],
                exit_code: 0,
            }],
            details: json!({"test": "sled-roundtrip"}),
            suggested_next_actions: Vec::new(),
        }
    }

    #[test]
    fn test_default_store_uses_sled_backend() {
        let root = make_test_root("xlsynth-bvc-store-sled-default");
        let store = ArtifactStore::new(root.clone());
        store.ensure_layout().expect("ensure sled layout");
        assert!(store.artifacts_dir().is_dir());
        assert!(store.staging_dir().is_dir());
        assert!(store.driver_release_cache_root().is_dir());
        assert!(store.queue_pending_dir().is_dir());
        assert!(store.queue_running_dir().is_dir());
        assert!(store.queue_done_dir().is_dir());
        assert!(store.queue_canceled_dir().is_dir());
        assert_eq!(
            store.artifacts_sled_db_path(),
            root.join("artifacts.test.sled")
        );
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn queue_paths_use_sharded_layout() {
        let root = make_test_root("xlsynth-bvc-store-queue-paths");
        let store = ArtifactStore::new_with_sled(root.clone(), root.join("artifacts.sled"));
        std::fs::create_dir_all(&root).expect("create test root");
        let action_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert_eq!(
            store.pending_queue_path(action_id),
            root.join("queue")
                .join("pending")
                .join("01")
                .join("23")
                .join(format!("{action_id}.json"))
        );
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn test_sled_backend_ensure_layout_succeeds() {
        let root = make_test_root("xlsynth-bvc-store-sled");
        let db_path = root.join("store.sled");
        let store = ArtifactStore::new_with_sled(root.clone(), db_path.clone());
        store.ensure_layout().expect("ensure sled layout");
        assert!(db_path.exists());
        assert!(store.artifacts_dir().is_dir());
        assert!(store.staging_dir().is_dir());
        assert!(store.queue_pending_dir().is_dir());
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn test_sled_backend_promote_and_materialize_roundtrip() {
        let root = make_test_root("xlsynth-bvc-store-sled-roundtrip");
        let db_path = root.join("store.sled");
        let store = ArtifactStore::new_with_sled(root.clone(), db_path.clone());
        store.ensure_layout().expect("ensure sled layout");

        let action_id =
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string();
        let staging_dir = store.staging_dir().join(format!("{action_id}-staged"));
        let payload_dir = staging_dir.join("payload");
        std::fs::create_dir_all(&payload_dir).expect("create staged payload");
        std::fs::write(payload_dir.join("result.txt"), "hello sled").expect("write staged file");

        let provenance = make_test_provenance(&action_id, "payload/result.txt", 10);
        std::fs::write(
            staging_dir.join("provenance.json"),
            serde_json::to_string_pretty(&provenance).expect("serialize provenance"),
        )
        .expect("write staged provenance");

        store
            .promote_staging_action_dir(&action_id, &staging_dir)
            .expect("promote staged sled action");
        assert!(store.action_exists(&action_id));

        let loaded = store
            .load_provenance(&action_id)
            .expect("load sled provenance");
        assert_eq!(loaded.action_id, action_id);
        assert_eq!(loaded.details, provenance.details);

        let all = store.list_provenances().expect("list sled provenances");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].action_id, provenance.action_id);

        let action_dir = store
            .materialize_action_dir(&provenance.action_id)
            .expect("materialize sled action dir");
        let payload_text =
            std::fs::read_to_string(action_dir.join("payload/result.txt")).expect("read payload");
        assert_eq!(payload_text, "hello sled");

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn test_sled_backend_compresses_large_action_file_rows() {
        let root = make_test_root("xlsynth-bvc-store-sled-zstd-large");
        let db_path = root.join("store.sled");
        let action_id =
            "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_string();
        let large_relpath = "payload/prep_for_gatify.ir";
        let large_bytes = "fn foo(x: bits[32]) -> bits[32] { add(x, x) }\n".repeat(8 * 1024);

        {
            let store = ArtifactStore::new_with_sled(root.clone(), db_path.clone());
            store.ensure_layout().expect("ensure sled layout");
            let staging_dir = store.staging_dir().join(format!("{action_id}-staged"));
            let payload_dir = staging_dir.join("payload");
            std::fs::create_dir_all(&payload_dir).expect("create staged payload");
            std::fs::write(payload_dir.join("prep_for_gatify.ir"), &large_bytes)
                .expect("write large staged file");
            let provenance =
                make_test_provenance(&action_id, large_relpath, large_bytes.len() as u64);
            std::fs::write(
                staging_dir.join("provenance.json"),
                serde_json::to_string_pretty(&provenance).expect("serialize provenance"),
            )
            .expect("write staged provenance");
            store
                .promote_staging_action_dir(&action_id, &staging_dir)
                .expect("promote staged action");
        }

        let db = sled::Config::new()
            .path(&db_path)
            .cache_capacity(16 * 1024 * 1024)
            .open()
            .expect("open sled db for inspection");
        let file_tree = db
            .open_tree(SledArtifactBackend::TREE_ACTION_FILE_BYTES)
            .expect("open action_file_bytes tree");
        let key = SledArtifactBackend::action_file_key(&action_id, large_relpath);
        let stored = file_tree
            .get(key)
            .expect("read action-file row")
            .expect("action-file row should exist");
        assert!(stored.starts_with(SledArtifactBackend::ACTION_FILE_ZSTD_MAGIC));
        assert!(stored.len() < large_bytes.len());
        drop(file_tree);
        drop(db);

        let store = ArtifactStore::new_with_sled(root.clone(), db_path.clone());
        let action_dir = store
            .materialize_action_dir(&action_id)
            .expect("materialize sled action");
        let decoded =
            std::fs::read_to_string(action_dir.join("payload/prep_for_gatify.ir")).expect("read");
        assert_eq!(decoded, large_bytes);

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn test_backfill_sled_action_file_compression_rewrites_raw_rows() {
        let root = make_test_root("xlsynth-bvc-store-sled-zstd-backfill");
        let db_path = root.join("store.sled");
        std::fs::create_dir_all(&root).expect("create test root");
        let db = sled::Config::new()
            .path(&db_path)
            .cache_capacity(16 * 1024 * 1024)
            .open()
            .expect("open sled db");
        let file_tree = db
            .open_tree(SledArtifactBackend::TREE_ACTION_FILE_BYTES)
            .expect("open action_file_bytes tree");
        let action_id = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
        let relpath = "payload/prep_for_gatify.ir";
        let key = SledArtifactBackend::action_file_key(action_id, relpath);
        let raw_bytes = "fn bar(x: bits[32]) -> bits[32] { add(x, x) }\n".repeat(8 * 1024);
        file_tree
            .insert(key.clone(), raw_bytes.as_bytes())
            .expect("insert raw row");
        db.flush().expect("flush raw row");
        drop(file_tree);
        drop(db);

        let summary = backfill_sled_action_file_compression(&db_path, None, false, None)
            .expect("run backfill");
        assert_eq!(summary.scanned_entries, 1);
        assert_eq!(summary.rewritten_entries, 1);
        assert_eq!(summary.unchanged_entries, 0);
        assert_eq!(summary.written_compressed_entries, 1);

        let db = sled::Config::new()
            .path(&db_path)
            .cache_capacity(16 * 1024 * 1024)
            .open()
            .expect("reopen sled db");
        let file_tree = db
            .open_tree(SledArtifactBackend::TREE_ACTION_FILE_BYTES)
            .expect("open action_file_bytes tree");
        let stored = file_tree
            .get(key)
            .expect("read stored row")
            .expect("stored row missing");
        assert!(stored.starts_with(SledArtifactBackend::ACTION_FILE_ZSTD_MAGIC));
        assert!(stored.len() < raw_bytes.len());
        let decoded = SledArtifactBackend::decode_action_file_value(stored.as_ref())
            .expect("decode compressed stored row");
        assert_eq!(decoded, raw_bytes.as_bytes());

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn test_backfill_sled_action_file_compression_target_relpath_filters_rows() {
        let root = make_test_root("xlsynth-bvc-store-sled-zstd-backfill-targeted");
        let db_path = root.join("store.sled");
        std::fs::create_dir_all(&root).expect("create test root");
        let db = sled::Config::new()
            .path(&db_path)
            .cache_capacity(16 * 1024 * 1024)
            .open()
            .expect("open sled db");
        let file_tree = db
            .open_tree(SledArtifactBackend::TREE_ACTION_FILE_BYTES)
            .expect("open action_file_bytes tree");
        let target_action_id = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        let target_relpath = "payload/prep_for_gatify.ir";
        let target_key = SledArtifactBackend::action_file_key(target_action_id, target_relpath);
        let target_bytes = "fn target(x: bits[32]) -> bits[32] { add(x, x) }\n".repeat(8 * 1024);
        file_tree
            .insert(target_key.clone(), target_bytes.as_bytes())
            .expect("insert target raw row");

        let other_action_id = "abababababababababababababababababababababababababababababababab";
        let other_relpath = "payload/other.ir";
        let other_key = SledArtifactBackend::action_file_key(other_action_id, other_relpath);
        let other_bytes = "fn other(x: bits[32]) -> bits[32] { sub(x, x) }\n".repeat(8 * 1024);
        file_tree
            .insert(other_key.clone(), other_bytes.as_bytes())
            .expect("insert non-target raw row");
        db.flush().expect("flush raw rows");
        drop(file_tree);
        drop(db);

        let summary =
            backfill_sled_action_file_compression(&db_path, None, false, Some(target_relpath))
                .expect("run targeted backfill");
        assert_eq!(summary.scanned_entries, 1);
        assert_eq!(summary.rewritten_entries, 1);
        assert_eq!(summary.written_compressed_entries, 1);

        let db = sled::Config::new()
            .path(&db_path)
            .cache_capacity(16 * 1024 * 1024)
            .open()
            .expect("reopen sled db");
        let file_tree = db
            .open_tree(SledArtifactBackend::TREE_ACTION_FILE_BYTES)
            .expect("open action_file_bytes tree");
        let target_stored = file_tree
            .get(target_key)
            .expect("read target row")
            .expect("target row missing");
        assert!(target_stored.starts_with(SledArtifactBackend::ACTION_FILE_ZSTD_MAGIC));
        let other_stored = file_tree
            .get(other_key)
            .expect("read other row")
            .expect("other row missing");
        assert!(!other_stored.starts_with(SledArtifactBackend::ACTION_FILE_ZSTD_MAGIC));

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn test_compact_sled_db_copies_rows_without_replacing_source() {
        let root = make_test_root("xlsynth-bvc-store-sled-compact-copy");
        let source_db_path = root.join("source.sled");
        let destination_db_path = root.join("compacted.sled");
        std::fs::create_dir_all(&root).expect("create test root");

        let source_db = sled::Config::new()
            .path(&source_db_path)
            .cache_capacity(16 * 1024 * 1024)
            .open()
            .expect("open source sled db");
        let alpha_tree = source_db.open_tree("alpha").expect("open alpha tree");
        alpha_tree.insert(b"k1", b"v1").expect("insert alpha k1");
        alpha_tree.insert(b"k2", b"v2").expect("insert alpha k2");
        let beta_tree = source_db.open_tree("beta").expect("open beta tree");
        beta_tree.insert(b"k3", b"v3").expect("insert beta k3");
        source_db.flush().expect("flush source db");
        drop(beta_tree);
        drop(alpha_tree);
        drop(source_db);

        let summary = compact_sled_db(&source_db_path, Some(&destination_db_path), false)
            .expect("compact sled db");
        assert!(!summary.replaced_source);
        assert_eq!(summary.backup_db_path, None);
        assert_eq!(summary.total_entries, 3);
        assert!(source_db_path.exists());
        assert!(destination_db_path.exists());

        let destination_db = sled::Config::new()
            .path(&destination_db_path)
            .cache_capacity(16 * 1024 * 1024)
            .open()
            .expect("open destination sled db");
        let alpha_tree = destination_db.open_tree("alpha").expect("open alpha tree");
        let beta_tree = destination_db.open_tree("beta").expect("open beta tree");
        assert_eq!(
            alpha_tree
                .get(b"k1")
                .expect("load alpha k1")
                .expect("alpha k1 missing")
                .as_ref(),
            b"v1"
        );
        assert_eq!(
            alpha_tree
                .get(b"k2")
                .expect("load alpha k2")
                .expect("alpha k2 missing")
                .as_ref(),
            b"v2"
        );
        assert_eq!(
            beta_tree
                .get(b"k3")
                .expect("load beta k3")
                .expect("beta k3 missing")
                .as_ref(),
            b"v3"
        );

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn test_compact_sled_db_replace_source_swaps_compacted_copy() {
        let root = make_test_root("xlsynth-bvc-store-sled-compact-swap");
        let source_db_path = root.join("source.sled");
        let destination_db_path = root.join("compacted.sled");
        std::fs::create_dir_all(&root).expect("create test root");

        let source_db = sled::Config::new()
            .path(&source_db_path)
            .cache_capacity(16 * 1024 * 1024)
            .open()
            .expect("open source sled db");
        let alpha_tree = source_db.open_tree("alpha").expect("open alpha tree");
        alpha_tree
            .insert(b"key", b"value")
            .expect("insert source row");
        source_db.flush().expect("flush source db");
        drop(alpha_tree);
        drop(source_db);

        let summary = compact_sled_db(&source_db_path, Some(&destination_db_path), true)
            .expect("compact and replace source db");
        assert!(summary.replaced_source);
        let backup_path = summary
            .backup_db_path
            .as_ref()
            .map(PathBuf::from)
            .expect("backup path should be present");
        assert!(backup_path.exists());
        assert!(source_db_path.exists());
        assert!(!destination_db_path.exists());

        let compacted_source = sled::Config::new()
            .path(&source_db_path)
            .cache_capacity(16 * 1024 * 1024)
            .open()
            .expect("open swapped source db");
        let alpha_tree = compacted_source
            .open_tree("alpha")
            .expect("open alpha tree");
        assert_eq!(
            alpha_tree
                .get(b"key")
                .expect("load swapped row")
                .expect("swapped row missing")
                .as_ref(),
            b"value"
        );

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn test_prune_sled_actions_by_relpath_size_removes_downstream_and_queue_descendants() {
        let root = make_test_root("xlsynth-bvc-store-sled-prune-relpath-size");
        let db_path = root.join("store.sled");
        std::fs::create_dir_all(&root).expect("create test root");

        let db = sled::Config::new()
            .path(&db_path)
            .cache_capacity(16 * 1024 * 1024)
            .open()
            .expect("open sled db");
        let provenance_tree = db
            .open_tree(SledArtifactBackend::TREE_PROVENANCE_BY_ACTION)
            .expect("open provenance tree");
        let file_tree = db
            .open_tree(SledArtifactBackend::TREE_ACTION_FILE_BYTES)
            .expect("open action_file_bytes tree");

        let action_a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let action_b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let action_c = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let queue_child = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

        let mut prov_a = make_test_provenance(
            action_a,
            "payload/prep_for_gatify.ir",
            (2 * 1024 * 1024) as u64,
        );
        prov_a.dependencies = Vec::new();
        let mut prov_b = make_test_provenance(action_b, "payload/result.aig", 7);
        prov_b.dependencies = vec![ArtifactRef {
            action_id: action_a.to_string(),
            artifact_type: ArtifactType::IrPackageFile,
            relpath: "payload".to_string(),
        }];
        let mut prov_c = make_test_provenance(action_c, "payload/result.v", 7);
        prov_c.dependencies = vec![ArtifactRef {
            action_id: action_b.to_string(),
            artifact_type: ArtifactType::AigFile,
            relpath: "payload".to_string(),
        }];

        provenance_tree
            .insert(
                action_a.as_bytes(),
                serde_json::to_vec(&prov_a).expect("serialize prov_a"),
            )
            .expect("insert prov_a");
        provenance_tree
            .insert(
                action_b.as_bytes(),
                serde_json::to_vec(&prov_b).expect("serialize prov_b"),
            )
            .expect("insert prov_b");
        provenance_tree
            .insert(
                action_c.as_bytes(),
                serde_json::to_vec(&prov_c).expect("serialize prov_c"),
            )
            .expect("insert prov_c");

        let large_prep_bytes = vec![b'x'; 2 * 1024 * 1024];
        file_tree
            .insert(
                SledArtifactBackend::action_file_key(action_a, "payload/prep_for_gatify.ir"),
                large_prep_bytes.as_slice(),
            )
            .expect("insert prep_for_gatify bytes for action_a");
        file_tree
            .insert(
                SledArtifactBackend::action_file_key(action_b, "payload/result.aig"),
                b"small-aig".as_slice(),
            )
            .expect("insert result.aig bytes for action_b");
        file_tree
            .insert(
                SledArtifactBackend::action_file_key(action_c, "payload/result.v"),
                b"small-v".as_slice(),
            )
            .expect("insert result.v bytes for action_c");

        db.flush().expect("flush seed db");
        drop(file_tree);
        drop(provenance_tree);
        drop(db);

        let queue_pending_path = queue_json_path(&root, "pending", queue_child);
        if let Some(parent) = queue_pending_path.parent() {
            std::fs::create_dir_all(parent).expect("create queue pending parent");
        }
        let runtime = DriverRuntimeSpec {
            driver_version: "v0.37.0".to_string(),
            release_platform: "linux-x64".to_string(),
            docker_image: "xlsynth-driver:test".to_string(),
            dockerfile: "docker/xlsynth-driver.Dockerfile".to_string(),
        };
        let queue_item = QueueItem {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: queue_child.to_string(),
            enqueued_utc: Utc::now(),
            priority: crate::DEFAULT_QUEUE_PRIORITY,
            action: ActionSpec::DriverIrToOpt {
                ir_action_id: action_c.to_string(),
                top_fn_name: None,
                version: "v0.37.0".to_string(),
                runtime,
            },
        };
        std::fs::write(
            &queue_pending_path,
            serde_json::to_string_pretty(&queue_item).expect("serialize queue pending record"),
        )
        .expect("write queue pending record");

        let summary = prune_sled_actions_by_relpath_size(
            &root,
            &db_path,
            "payload/prep_for_gatify.ir",
            1024 * 1024,
            true,
            false,
        )
        .expect("prune by relpath size");

        assert_eq!(summary.seed_action_count, 1);
        assert_eq!(summary.total_action_count, 4);
        assert_eq!(summary.downstream_action_count, 3);
        assert_eq!(summary.deleted_provenance_rows, 3);
        assert_eq!(summary.deleted_queue_pending_files, 1);
        assert!(summary.deleted_action_file_rows >= 3);

        let db = sled::Config::new()
            .path(&db_path)
            .cache_capacity(16 * 1024 * 1024)
            .open()
            .expect("reopen sled db");
        let provenance_tree = db
            .open_tree(SledArtifactBackend::TREE_PROVENANCE_BY_ACTION)
            .expect("open provenance tree");
        let file_tree = db
            .open_tree(SledArtifactBackend::TREE_ACTION_FILE_BYTES)
            .expect("open action_file tree");

        assert!(
            provenance_tree
                .get(action_a.as_bytes())
                .expect("load prov_a after prune")
                .is_none()
        );
        assert!(
            provenance_tree
                .get(action_b.as_bytes())
                .expect("load prov_b after prune")
                .is_none()
        );
        assert!(
            provenance_tree
                .get(action_c.as_bytes())
                .expect("load prov_c after prune")
                .is_none()
        );
        assert_eq!(
            file_tree
                .scan_prefix(SledArtifactBackend::action_files_prefix(action_a))
                .count(),
            0
        );
        assert_eq!(
            file_tree
                .scan_prefix(SledArtifactBackend::action_files_prefix(action_b))
                .count(),
            0
        );
        assert_eq!(
            file_tree
                .scan_prefix(SledArtifactBackend::action_files_prefix(action_c))
                .count(),
            0
        );
        assert!(!queue_pending_path.exists());

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn test_sled_backend_load_failed_action_records_reads_persisted_records() {
        let root = make_test_root("xlsynth-bvc-store-sled-failed");
        let db_path = root.join("store.sled");
        let store = ArtifactStore::new_with_sled(root.clone(), db_path.clone());
        store.ensure_layout().expect("ensure sled layout");

        let action_id =
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_string();
        let failed = QueueFailed {
            schema_version: 1,
            action_id: action_id.clone(),
            enqueued_utc: Utc::now(),
            failed_utc: Utc::now(),
            failed_by: "test-worker".to_string(),
            action: ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                version: "v0.37.0".to_string(),
                discovery_runtime: None,
            },
            error: "synthetic failure".to_string(),
        };
        store
            .write_failed_action_record(&failed)
            .expect("write failed record");

        let loaded = store
            .load_failed_action_records()
            .expect("load failed records");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].action_id, action_id);
        assert_eq!(loaded[0].error, "synthetic failure");

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn test_ingest_legacy_failed_records_moves_data_to_sled_and_prunes_legacy_dirs() {
        let root = make_test_root("xlsynth-bvc-store-ingest-legacy-failed");
        let db_path = root.join("store.sled");
        let store = ArtifactStore::new_with_sled(root.clone(), db_path);
        store.ensure_layout().expect("ensure store layout");

        let legacy_record = QueueFailed {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: "a".repeat(64),
            enqueued_utc: Utc::now(),
            failed_utc: Utc::now(),
            failed_by: "legacy-failed-action-records".to_string(),
            action: ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                version: "v0.37.0".to_string(),
                discovery_runtime: None,
            },
            error: "legacy failed-action-records".to_string(),
        };
        let legacy_path = root
            .join("failed-action-records")
            .join("aa")
            .join("bb")
            .join(&legacy_record.action_id)
            .join("failed.json");
        if let Some(parent) = legacy_path.parent() {
            std::fs::create_dir_all(parent).expect("create legacy failed-action-records parent");
        }
        std::fs::write(
            &legacy_path,
            serde_json::to_string_pretty(&legacy_record).expect("serialize legacy failed record"),
        )
        .expect("write legacy failed-action-records record");

        let queue_legacy_record = QueueFailed {
            schema_version: crate::ACTION_SCHEMA_VERSION,
            action_id: "b".repeat(64),
            enqueued_utc: Utc::now(),
            failed_utc: Utc::now(),
            failed_by: "legacy-queue-failed".to_string(),
            action: ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                version: "v0.37.0".to_string(),
                discovery_runtime: None,
            },
            error: "legacy queue failed".to_string(),
        };
        let queue_legacy_path = root
            .join("queue")
            .join("failed")
            .join("bb")
            .join("cc")
            .join(format!("{}.json", queue_legacy_record.action_id));
        if let Some(parent) = queue_legacy_path.parent() {
            std::fs::create_dir_all(parent).expect("create legacy queue failed parent");
        }
        std::fs::write(
            &queue_legacy_path,
            serde_json::to_string_pretty(&queue_legacy_record)
                .expect("serialize legacy queue failed record"),
        )
        .expect("write legacy queue failed record");

        let summary = ingest_legacy_failed_records(&store, false, false)
            .expect("ingest legacy failed records");
        assert_eq!(summary.parse_errors, 0);
        assert_eq!(summary.ingested_into_sled, 2);
        assert!(summary.removed_legacy_failed_action_records_dir);
        assert!(summary.removed_legacy_queue_failed_dir);
        assert!(!root.join("failed-action-records").exists());
        assert!(!root.join("queue").join("failed").exists());

        let mut loaded = store
            .load_failed_action_records()
            .expect("load failed action records");
        loaded.sort_by(|a, b| a.action_id.cmp(&b.action_id));
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].action_id, legacy_record.action_id);
        assert_eq!(loaded[1].action_id, queue_legacy_record.action_id);

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn test_for_each_provenance_supports_early_break() {
        let root = make_test_root("xlsynth-bvc-store-for-each");
        let db_path = root.join("store.sled");
        let store = ArtifactStore::new_with_sled(root.clone(), db_path.clone());
        store.ensure_layout().expect("ensure sled layout");

        for suffix in ["1", "2"] {
            let action_id = format!("{:0>64}", suffix);
            let staging_dir = store.staging_dir().join(format!("{action_id}-staged"));
            std::fs::create_dir_all(staging_dir.join("payload")).expect("create staged payload");
            let provenance = Provenance {
                schema_version: 1,
                action_id: action_id.clone(),
                created_utc: Utc::now(),
                action: ActionSpec::DownloadAndExtractXlsynthReleaseStdlibTarball {
                    version: "v0.37.0".to_string(),
                    discovery_runtime: None,
                },
                dependencies: Vec::new(),
                output_artifact: ArtifactRef {
                    action_id: action_id.clone(),
                    artifact_type: ArtifactType::DslxFileSubtree,
                    relpath: "payload".to_string(),
                },
                output_files: Vec::new(),
                commands: Vec::new(),
                details: json!({}),
                suggested_next_actions: Vec::new(),
            };
            std::fs::write(
                staging_dir.join("provenance.json"),
                serde_json::to_string_pretty(&provenance).expect("serialize provenance"),
            )
            .expect("write staged provenance");
            store
                .promote_staging_action_dir(&action_id, &staging_dir)
                .expect("promote staged action");
        }

        let mut visited = 0_usize;
        store
            .for_each_provenance(|_| {
                visited += 1;
                Ok(ControlFlow::Break(()))
            })
            .expect("iterate with early break");
        assert_eq!(visited, 1);

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn test_sled_backend_web_index_roundtrip() {
        let root = make_test_root("xlsynth-bvc-store-sled-web-index");
        let db_path = root.join("store.sled");
        let store = ArtifactStore::new_with_sled(root.clone(), db_path.clone());
        store.ensure_layout().expect("ensure sled layout");

        let key = "ir-fn-corpus-g8r-vs-yosys-abc.v2.json";
        let bytes = br#"{"schema_version":1}"#;
        store
            .write_web_index_bytes(key, bytes)
            .expect("write sled web index");
        let loaded = store
            .load_web_index_bytes(key)
            .expect("load sled web index")
            .expect("sled web index should exist");
        assert_eq!(loaded, bytes);
        assert_eq!(
            store.web_index_location(key),
            format!("sled://web_index_bytes/{}", key)
        );

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn test_snapshot_backend_web_index_roundtrip() {
        let root = make_test_root("xlsynth-bvc-store-snapshot-web-index");
        let snapshot_dir = root.join("snapshot");
        let web_index_dir = snapshot_dir.join(crate::snapshot::STATIC_SNAPSHOT_WEB_INDEX_DIR);
        std::fs::create_dir_all(&web_index_dir).expect("create snapshot web_index dir");
        std::fs::create_dir_all(web_index_dir.join("nested")).expect("create nested dir");

        let manifest = crate::snapshot::StaticSnapshotManifest {
            schema_version: crate::snapshot::STATIC_SNAPSHOT_SCHEMA_VERSION,
            snapshot_id: "test-snapshot".to_string(),
            generated_utc: Utc::now(),
            git_commit: None,
            source_action_set_sha256: None,
            dataset_files: Vec::new(),
            total_dataset_bytes: 0,
        };
        std::fs::write(
            snapshot_dir.join(crate::snapshot::STATIC_SNAPSHOT_MANIFEST_FILENAME),
            serde_json::to_vec_pretty(&manifest).expect("serialize snapshot manifest"),
        )
        .expect("write snapshot manifest");
        std::fs::write(
            web_index_dir.join("nested/index.json"),
            br#"{\"schema_version\":1}"#,
        )
        .expect("write snapshot index file");

        let store = ArtifactStore::new_with_snapshot(root.clone(), snapshot_dir.clone());
        store.ensure_layout().expect("ensure snapshot layout");

        let loaded = store
            .load_web_index_bytes("nested/index.json")
            .expect("load snapshot web index")
            .expect("snapshot web index should exist");
        assert_eq!(loaded, br#"{\"schema_version\":1}"#);
        assert_eq!(
            store.web_index_location("nested/index.json"),
            "snapshot://web_index/nested/index.json"
        );
        assert!(store.is_snapshot_backend());
        assert_eq!(
            store.snapshot_dir().as_deref(),
            Some(snapshot_dir.as_path())
        );
        assert!(
            store
                .list_provenances()
                .expect("list provenances")
                .is_empty()
        );

        std::fs::remove_dir_all(root).expect("cleanup");
    }
}
