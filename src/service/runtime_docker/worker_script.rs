// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) const EMBEDDED: &[u8] = include_bytes!("../../../scripts/persistent_runner_worker.py");

/// Bind each binary to its own worker bytes, independently of mutable repo files.
pub(super) fn materialize(store_root: &Path, script: &[u8]) -> Result<PathBuf> {
    // The store root is read-only in every worker. Its runtime and staging subtrees are writable.
    let directory = store_root.join(".worker-scripts");
    fs::create_dir_all(&directory)
        .with_context(|| format!("creating worker script cache: {}", directory.display()))?;
    if !fs::symlink_metadata(&directory)?.is_dir() {
        bail!(
            "worker script cache is not a directory: {}",
            directory.display()
        );
    }
    let filename = format!("{}.py", hex::encode(Sha256::digest(script)));
    let path = directory.join(&filename);
    match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            if !metadata.is_file() {
                bail!(
                    "worker script cache entry is not a regular file: {}",
                    path.display()
                );
            }
            if fs::read(&path)
                .with_context(|| format!("reading cached worker script: {}", path.display()))?
                == script
            {
                return path
                    .canonicalize()
                    .context("resolving cached worker script");
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("inspecting cached worker script"),
    }

    static NONCE: AtomicU64 = AtomicU64::new(0);
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let temporary = directory.join(format!(
        ".{filename}.tmp-{}-{timestamp}-{}",
        std::process::id(),
        NONCE.fetch_add(1, Ordering::Relaxed)
    ));
    let publish = || -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .context("creating staged worker script")?;
        file.write_all(script)
            .context("writing staged worker script")?;
        drop(file);
        // This is a disposable cache: the executable remains its source of truth.
        fs::rename(&temporary, &path).context("publishing embedded worker script")?;
        Ok(())
    };
    if let Err(error) = publish() {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    if fs::read(&path).context("verifying published worker script")? != script {
        bail!("embedded worker script cache changed during publication");
    }
    path.canonicalize()
        .context("resolving published worker script")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn root(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "bvc-worker-script-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn distinct_embedded_versions_coexist_and_ignore_repo_edits() {
        let root = root("versions");
        let repo = root.join("repo/scripts");
        fs::create_dir_all(&repo).unwrap();
        fs::write(
            repo.join("persistent_runner_worker.py"),
            b"unrelated repo version",
        )
        .unwrap();
        let old = materialize(&root, b"print('old')\n").unwrap();
        let new = materialize(&root, b"print('new')\n").unwrap();
        assert_ne!(old, new);
        fs::write(
            repo.join("persistent_runner_worker.py"),
            b"changed while running",
        )
        .unwrap();
        assert_eq!(materialize(&root, b"print('old')\n").unwrap(), old);
        assert_eq!(fs::read(&old).unwrap(), b"print('old')\n");
        assert_eq!(fs::read(&new).unwrap(), b"print('new')\n");
        assert!(old.is_absolute());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn repairs_truncation_without_exposing_temporary_bytes() {
        let root = root("repair");
        let path = materialize(&root, EMBEDDED).unwrap();
        fs::write(&path, &EMBEDDED[..17]).unwrap();
        let orphan = root.join(".worker-scripts/.interrupted.tmp");
        fs::write(&orphan, b"partial write").unwrap();
        assert_eq!(materialize(&root, EMBEDDED).unwrap(), path);
        assert_eq!(fs::read(path).unwrap(), EMBEDDED);
        assert_eq!(fs::read(orphan).unwrap(), b"partial write");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn concurrent_materialization_publishes_complete_versions() {
        let root = root("concurrent");
        let handles: Vec<_> = (0..16)
            .map(|index| {
                let root = root.clone();
                thread::spawn(move || {
                    let bytes: &[u8] = if index % 2 == 0 {
                        EMBEDDED
                    } else {
                        b"print('other')\n"
                    };
                    let path = materialize(&root, bytes).unwrap();
                    assert_eq!(fs::read(&path).unwrap(), bytes);
                    path
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(
            fs::read_dir(root.join(".worker-scripts")).unwrap().count(),
            2
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn rejects_symlink_cache_entries_without_modifying_targets() {
        use std::os::unix::fs::symlink;
        let root = root("symlink");
        let path = materialize(&root, EMBEDDED).unwrap();
        fs::remove_file(&path).unwrap();
        let target = root.join("outside.py");
        fs::write(&target, b"preserve").unwrap();
        symlink(&target, &path).unwrap();
        assert!(
            materialize(&root, EMBEDDED)
                .unwrap_err()
                .to_string()
                .contains("regular file")
        );
        assert_eq!(fs::read(target).unwrap(), b"preserve");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn unreadable_script_is_an_error_not_a_different_worker() {
        use std::os::unix::fs::PermissionsExt;
        let root = root("permission");
        let path = materialize(&root, EMBEDDED).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0)).unwrap();
        if fs::read(&path).is_err() {
            assert!(
                materialize(&root, EMBEDDED)
                    .unwrap_err()
                    .to_string()
                    .contains("reading cached worker script")
            );
        }
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::remove_dir_all(root).unwrap();
    }
}
