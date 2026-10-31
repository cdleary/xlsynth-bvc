// SPDX-License-Identifier: Apache-2.0

use std::{path::Path, thread, time::Duration};

#[cfg(target_os = "linux")]
use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    fd::OwnedFd,
    fs::inotify,
    io::{Errno, read},
};

/// Optional rename notifications. The file and the runner identity remain authoritative.
pub(super) struct DirectoryEvents {
    #[cfg(target_os = "linux")]
    fd: Option<OwnedFd>,
}

impl DirectoryEvents {
    pub(super) fn watch(path: &Path) -> Self {
        #[cfg(target_os = "linux")]
        {
            let fd = inotify::init(inotify::CreateFlags::CLOEXEC | inotify::CreateFlags::NONBLOCK)
                .and_then(|fd| {
                    inotify::add_watch(&fd, path, inotify::WatchFlags::MOVED_TO)?;
                    Ok(fd)
                });
            if let Err(error) = &fd {
                log::debug!(target: "bvc_timing", "runner result watch unavailable: {error}");
            }
            Self { fd: fd.ok() }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            Self {}
        }
    }

    pub(super) fn wait(&mut self, timeout: Duration) {
        #[cfg(target_os = "linux")]
        if let Some(fd) = &self.fd {
            match wait_for_rename(fd, timeout) {
                Ok(_) | Err(Errno::INTR) => return,
                Err(_) => self.fd = None,
            }
        }
        thread::sleep(timeout);
    }
}

#[cfg(target_os = "linux")]
fn wait_for_rename(fd: &OwnedFd, timeout: Duration) -> rustix::io::Result<bool> {
    let timeout = Timespec::try_from(timeout).map_err(|_| Errno::OVERFLOW)?;
    let mut fds = [PollFd::new(fd, PollFlags::IN)];
    if poll(&mut fds, Some(&timeout))? == 0 {
        return Ok(false);
    }
    // Consume any pending hints, including an overflow, then recheck the file.
    read(fd, &mut [0u8; 4096]).map(|count| count > 0)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn queued_rename_wakes_a_late_waiter() {
        let root = std::env::temp_dir().join(format!(
            "bvc-rename-watch-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        let watch = DirectoryEvents::watch(&root);
        let fd = watch.fd.as_ref().unwrap();
        fs::write(root.join("staging"), b"complete").unwrap();
        assert!(!wait_for_rename(fd, Duration::ZERO).unwrap());
        fs::rename(root.join("staging"), root.join("result")).unwrap();
        assert!(wait_for_rename(fd, Duration::ZERO).unwrap());
        assert!(!wait_for_rename(fd, Duration::ZERO).unwrap());
        fs::remove_dir_all(&root).unwrap();
        assert!(DirectoryEvents::watch(&root).fd.is_none());
    }
}
