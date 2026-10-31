// SPDX-License-Identifier: Apache-2.0

use super::{DOCKER_RUN_NAME_COUNTER, ExternalWriteback, process_instance_id};
use anyhow::{Context, Result, bail, ensure};
use rustix::fs::{
    AtFlags, CWD, Dir, FileType, Mode, OFlags, RenameFlags, fchmod, fstat, mkdirat, openat,
    renameat, renameat_with, statat, unlinkat,
};
use rustix::io::Errno;
use std::ffi::OsStr;
use std::fs::File;
use std::os::fd::AsFd;
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path};
use std::sync::atomic::Ordering;

// The runner shares these paths with the host. Resolve every component relative
// to an already open directory; validating a pathname before fs::copy would
// leave a race with a process that replaced it with a symlink.
const OPEN_DIR: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);
const MAX_DEPTH: usize = 128;

// DockerMount canonicalizes the user path before dispatch. Paths under the
// inferred store (including the job path) inherit that canonical prefix.
fn open_parent(path: &Path) -> Result<(File, &OsStr)> {
    let name = path.file_name().context("missing output filename")?;
    let mut components = path.parent().context("missing output parent")?.components();
    ensure!(
        components.next() == Some(Component::RootDir),
        "expected absolute path"
    );
    // Traversing a known name requires search permission, not directory listing.
    #[cfg(target_vendor = "apple")]
    let search_dir = OPEN_DIR | OFlags::from_bits_retain(libc::O_SEARCH as _);
    #[cfg(not(target_vendor = "apple"))]
    let search_dir = OPEN_DIR | OFlags::PATH;
    let mut dir = openat(CWD, "/", search_dir, Mode::empty())?;
    for component in components {
        let Component::Normal(name) = component else {
            bail!("invalid writeback path component: {component:?}");
        };
        dir = openat(&dir, name, search_dir, Mode::empty())?;
    }
    Ok((dir.into(), name))
}

fn for_each_child(dir: &File, mut visit: impl FnMut(&OsStr) -> Result<()>) -> Result<()> {
    for entry in Dir::read_from(dir)? {
        let entry = entry?;
        let bytes = entry.file_name().to_bytes();
        if bytes != b"." && bytes != b".." {
            let name = OsStr::from_bytes(bytes);
            visit(name).with_context(|| format!("processing writeback entry {name:?}"))?;
        }
    }
    Ok(())
}

fn copy_entry(src_dir: &File, dst_dir: &File, name: &OsStr, depth: usize) -> Result<()> {
    ensure!(
        depth <= MAX_DEPTH,
        "runner output exceeds maximum copy depth"
    );
    let stat = statat(src_dir, name, AtFlags::SYMLINK_NOFOLLOW)?;
    match FileType::from_raw_mode(stat.st_mode) {
        FileType::Directory => {
            let source = File::from(openat(src_dir, name, OPEN_DIR, Mode::empty())?);
            mkdirat(dst_dir, name, Mode::from_raw_mode(0o777))?;
            let target = File::from(openat(dst_dir, name, OPEN_DIR, Mode::empty())?);
            for_each_child(&source, |name| {
                copy_entry(&source, &target, name, depth + 1)
            })
        }
        FileType::RegularFile => {
            // NONBLOCK prevents a swapped-in FIFO from hanging the host before
            // we can check the opened file's actual type.
            let flags = OFlags::RDONLY
                | OFlags::NOFOLLOW
                | OFlags::NONBLOCK
                | OFlags::NOCTTY
                | OFlags::CLOEXEC;
            let mut source = File::from(openat(src_dir, name, flags, Mode::empty())?);
            let actual = fstat(&source)?;
            ensure!(
                FileType::from_raw_mode(actual.st_mode) == FileType::RegularFile,
                "runner output changed file type: {name:?}"
            );
            let mut target = File::from(openat(
                dst_dir,
                name,
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )?);
            std::io::copy(&mut source, &mut target)?;
            // Preserve ordinary file permissions, including executable tools.
            fchmod(&target, Mode::from_raw_mode(actual.st_mode & 0o777))?;
            Ok(())
        }
        _ => bail!("runner output must be a regular file or directory: {name:?}"),
    }
}

// Unlink leaves directly, including symlinks. Keep the traversal on the
// heap: an existing output can predate our limit on new runner output.
fn unlink_leaf_or_open_dir(parent: impl AsFd, name: &OsStr) -> Result<Option<Dir>> {
    match unlinkat(&parent, name, AtFlags::empty()) {
        Ok(()) | Err(Errno::NOENT) => Ok(None),
        Err(Errno::ISDIR | Errno::PERM) => Ok(Some(Dir::new(openat(
            parent,
            name,
            OPEN_DIR,
            Mode::empty(),
        )?)?)),
        Err(error) => Err(error.into()),
    }
}

fn remove_entry(parent: &File, name: &OsStr) -> Result<()> {
    let Some(root) = unlink_leaf_or_open_dir(parent, name)? else {
        return Ok(());
    };
    let mut stack = vec![(name.to_owned(), root)];
    while let Some((_, dir)) = stack.last_mut() {
        if let Some(entry) = dir.next() {
            let entry = entry?;
            let bytes = entry.file_name().to_bytes();
            let name = OsStr::from_bytes(bytes);
            if bytes != b"."
                && bytes != b".."
                && let Some(child) = unlink_leaf_or_open_dir(dir.fd()?, name)?
            {
                stack.push((name.to_owned(), child));
            }
        } else {
            let (name, _) = stack.pop().expect("current directory");
            let containing_dir = match stack.last() {
                Some((_, dir)) => dir.fd()?,
                None => parent.as_fd(),
            };
            unlinkat(containing_dir, name, AtFlags::REMOVEDIR)?;
        }
    }
    Ok(())
}

pub(super) fn sync_external_writeback(writeback: &ExternalWriteback) -> Result<()> {
    let (source, source_name) = open_parent(&writeback.job_path).with_context(|| {
        format!(
            "opening runner output parent: {}",
            writeback.job_path.display()
        )
    })?;
    let (destination, dest_name) = open_parent(&writeback.host_path).with_context(|| {
        format!(
            "opening host output parent: {}",
            writeback.host_path.display()
        )
    })?;
    let sequence = DOCKER_RUN_NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    let staging_name = format!(".bvc-writeback-{}-{sequence}", process_instance_id());
    let staging_name = OsStr::new(&staging_name);
    mkdirat(&destination, staging_name, Mode::RWXU)
        .context("creating fresh writeback directory")?;
    let staging = match openat(&destination, staging_name, OPEN_DIR, Mode::empty()) {
        Ok(dir) => File::from(dir),
        Err(error) => {
            let _ = unlinkat(&destination, staging_name, AtFlags::REMOVEDIR);
            return Err(error).context("opening fresh writeback directory");
        }
    };
    let result = copy_entry(&source, &staging, source_name, 0)
        .with_context(|| {
            format!(
                "copying staged runner output: {}",
                writeback.job_path.display()
            )
        })
        .and_then(|()| publish_entry(&staging, source_name, &destination, dest_name));

    // The held staging directory contains either a partial copy or the old
    // generation. Never recursively clean a sibling looked up by pathname.
    let cleanup = remove_entry(&staging, source_name)
        .and_then(|()| Ok(unlinkat(&destination, staging_name, AtFlags::REMOVEDIR)?));
    if let Err(cleanup_error) = cleanup {
        // A sibling can be inside the action payload. Never report success
        // and let downstream code collect a partial or retired generation.
        let context =
            format!("could not clean up writeback directory {staging_name:?}: {cleanup_error:#}");
        return match result {
            Ok(()) => Err(anyhow::anyhow!(
                "runner output was published, but {context}"
            )),
            Err(error) => Err(error.context(context)),
        };
    }
    result.with_context(|| {
        format!(
            "publishing runner output to {}",
            writeback.host_path.display()
        )
    })
}

fn publish_entry(staging: &File, src: &OsStr, destination: &File, dst: &OsStr) -> Result<()> {
    // Ordinary rename also works on filesystems without atomic exchange.
    // A nonempty destination directory or a type change requires a swap.
    match renameat(staging, src, destination, dst) {
        Err(Errno::NOTEMPTY | Errno::EXIST | Errno::ISDIR | Errno::NOTDIR) => {
            renameat_with(staging, src, destination, dst, RenameFlags::EXCHANGE).context(
                "atomically exchanging the complete runner output with the existing destination",
            )?;
        }
        result => result?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{
        DockerMount, PersistentRunnerPaths, prepare_mount_request, tests::make_temp_store_root,
    };
    use super::*;
    use std::{fs, os::unix::fs::symlink};

    #[test]
    fn writeback_rejects_symlinks_and_missing_output_before_replacing_destination() -> Result<()> {
        let root = make_temp_store_root("writeback-failure");
        let staged = root.join("job");
        let host = root.join("host");
        let fixture = root.join("fixture");
        fs::create_dir_all(&staged)?;
        fs::create_dir(&host)?;
        fs::create_dir(&fixture)?;
        fs::write(fixture.join("output"), b"host-only fixture")?;
        let writeback = ExternalWriteback {
            job_path: staged.join("output"),
            host_path: host.join("output"),
        };
        fs::create_dir(&writeback.host_path)?;
        fs::write(writeback.host_path.join("existing"), b"prior output")?;

        let assert_rejected = || -> Result<()> {
            assert!(sync_external_writeback(&writeback).is_err());
            assert_eq!(
                fs::read(writeback.host_path.join("existing"))?,
                b"prior output"
            );
            assert_eq!(fs::read_dir(&writeback.host_path)?.count(), 1);
            assert_eq!(fs::read_dir(&host)?.count(), 1, "left partial staging data");
            assert_eq!(fs::read(fixture.join("output"))?, b"host-only fixture");
            Ok(())
        };
        for target in [fixture.join("output"), fixture.clone()] {
            symlink(target, &writeback.job_path)?;
            assert_rejected()?;
            fs::remove_file(&writeback.job_path)?;
        }
        assert_rejected()?;
        fs::create_dir(&writeback.job_path)?;
        fs::write(writeback.job_path.join("partial"), b"new output")?;
        symlink(fixture.join("output"), writeback.job_path.join("link"))?;
        assert_rejected()?;
        fs::remove_file(writeback.job_path.join("link"))?;
        let mut deep = writeback.job_path.clone();
        for _ in 0..=MAX_DEPTH {
            deep.push("d");
        }
        fs::create_dir_all(deep)?;
        assert_rejected()?;
        fs::remove_dir_all(&staged)?;
        symlink(&fixture, &staged)?;
        assert_rejected()?;
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn writeback_propagates_cleanup_failure_after_publication() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let root = make_temp_store_root("writeback-cleanup-failure");
        let writeback = ExternalWriteback {
            job_path: root.join("staged"),
            host_path: root.join("output"),
        };
        fs::create_dir_all(&writeback.job_path)?;
        let readonly = writeback.host_path.join("readonly");
        fs::create_dir_all(&readonly)?;
        fs::write(writeback.job_path.join("current"), b"new")?;
        fs::write(readonly.join("old"), b"old")?;
        fs::set_permissions(&readonly, fs::Permissions::from_mode(0o555))?;
        // Privileged test users can remove children of read-only directories.
        if fs::write(readonly.join("probe"), b"").is_err() {
            let err = sync_external_writeback(&writeback).unwrap_err();
            assert!(err.to_string().contains("was published"), "{err:#}");
            let staging_dir = fs::read_dir(&root)?
                .filter_map(|e| e.ok())
                .find(|e| e.file_name().as_bytes().starts_with(b".bvc-writeback-"))
                .expect("retired output");
            fs::set_permissions(
                staging_dir.path().join("staged/readonly"),
                fs::Permissions::from_mode(0o700),
            )?;
            assert_eq!(fs::read(writeback.host_path.join("current"))?, b"new");
        } else {
            fs::set_permissions(&readonly, fs::Permissions::from_mode(0o700))?;
        }
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn writeback_preserves_existing_output_if_promotion_fails() -> Result<()> {
        let root = make_temp_store_root("writeback-promotion-failure");
        fs::create_dir_all(root.join("staging"))?;
        fs::write(root.join("output"), b"existing output")?;
        let staging = File::open(root.join("staging"))?;
        let destination = File::open(&root)?;
        // Simulate a staged result disappearing just before publication.
        assert!(
            publish_entry(
                &staging,
                OsStr::new("missing"),
                &destination,
                OsStr::new("output")
            )
            .is_err()
        );
        assert_eq!(fs::read(root.join("output"))?, b"existing output");
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn writeback_accepts_a_mount_prepared_through_a_host_alias() -> Result<()> {
        let root = make_temp_store_root("writeback-mount-alias");
        let store = root.join("store");
        let host = store.join(".staging/output");
        fs::create_dir_all(&host)?;
        // A store ancestor can be searchable without being listable.
        use std::os::unix::fs::PermissionsExt;
        let permissions = fs::metadata(&root)?.permissions();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o300))?;
        symlink(&store, root.join("alias"))?;
        let mount = DockerMount::read_write(&root.join("alias/.staging/output"), "/outputs")?;
        let store_root = super::super::infer_store_root_from_mounts(std::slice::from_ref(&mount))?;
        let paths = PersistentRunnerPaths::new(&store_root, "runner");
        let (_, writeback) = prepare_mount_request(&store_root, &paths, "request", 0, &mount)?;
        let writeback = writeback.expect("writable mount");
        fs::write(writeback.job_path.join("result"), b"copied out")?;
        sync_external_writeback(&writeback)?;
        assert_eq!(fs::read(host.join("result"))?, b"copied out");
        fs::set_permissions(&root, permissions)?;
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn writeback_publishes_complete_directories_and_regular_files() -> Result<()> {
        let root = make_temp_store_root("writeback-success");
        let host = root.join("host");
        fs::create_dir_all(&host)?;
        let writeback = ExternalWriteback {
            job_path: root.join("staged"),
            host_path: host.join("output"),
        };
        fs::create_dir_all(writeback.job_path.join("nested/empty"))?;
        fs::write(writeback.job_path.join("nested/result"), b"new data")?;
        fs::create_dir(&writeback.host_path)?;
        fs::write(writeback.host_path.join("obsolete"), b"old data")?;
        sync_external_writeback(&writeback)?;
        assert_eq!(
            fs::read(writeback.host_path.join("nested/result"))?,
            b"new data"
        );
        assert!(writeback.host_path.join("nested/empty").is_dir());
        assert!(!writeback.host_path.join("obsolete").exists());

        fs::remove_dir_all(&writeback.job_path)?;
        fs::write(&writeback.job_path, b"first")?;
        sync_external_writeback(&writeback)?;
        assert_eq!(fs::read(&writeback.host_path)?, b"first");
        fs::write(&writeback.job_path, b"second")?;
        sync_external_writeback(&writeback)?;
        assert_eq!(fs::read(&writeback.host_path)?, b"second");
        fs::remove_file(&writeback.host_path)?;
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&writeback.job_path, fs::Permissions::from_mode(0o751))?;
        sync_external_writeback(&writeback)?;
        assert_eq!(
            fs::metadata(&writeback.host_path)?.permissions().mode() & 0o777,
            0o751
        );
        fs::remove_file(&writeback.job_path)?;
        fs::create_dir(&writeback.job_path)?;
        sync_external_writeback(&writeback)?;
        assert!(writeback.host_path.is_dir());
        assert_eq!(fs::read_dir(&host)?.count(), 1);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
