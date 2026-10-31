// SPDX-License-Identifier: Apache-2.0

use std::sync::{Mutex, TryLockError};
use std::time::{Duration, Instant};

/// Coalesce maintenance without making dispatch wait for another worker's scan.
#[derive(Default)]
pub(super) struct MaintenanceGate {
    last_scan: Mutex<Option<Instant>>,
}

impl MaintenanceGate {
    pub(super) fn run_if_due<T>(
        &self,
        now: Instant,
        interval: Duration,
        work: impl FnOnce() -> T,
    ) -> Option<T> {
        let mut last_scan = match self.last_scan.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => return None,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
        };
        if last_scan.is_some_and(|last| now.duration_since(last) < interval) {
            return None;
        }
        // The guard covers only maintenance. Successful idle results are never cached.
        *last_scan = Some(now);
        Some(work())
    }

    pub(super) fn run_with_retry<T, E>(
        &self,
        retry: &mut bool,
        interval: Duration,
        work: impl FnOnce() -> Result<T, E>,
    ) -> Result<(), E> {
        // A worker that saw an error must retry it. A skipped scan would reset
        // its consecutive-error count. Resume sharing after its first success.
        let attempt = if *retry {
            Some(work())
        } else {
            self.run_if_due(Instant::now(), interval, work)
        };
        if let Some(result) = attempt {
            *retry = result.is_err();
            result?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, mpsc};
    use std::thread;

    #[test]
    fn skips_until_due_without_caching_idle() {
        let gate = MaintenanceGate::default();
        let now = Instant::now();
        let interval = Duration::from_secs(1);
        assert_eq!(gate.run_if_due(now, interval, || true), Some(true));
        assert!(
            !gate
                .run_if_due(now, interval, || panic!("not due"))
                .unwrap_or(false)
        );
        assert_eq!(
            gate.run_if_due(now + interval, interval, || false),
            Some(false)
        );
        assert_eq!(
            gate.run_if_due(now + interval * 2, interval, || true),
            Some(true)
        );
    }

    #[test]
    fn another_worker_can_dispatch_while_scan_is_active() {
        let gate = Arc::new(MaintenanceGate::default());
        let (started_tx, started_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel();
        let child_gate = gate.clone();
        let child = thread::spawn(move || {
            child_gate.run_if_due(Instant::now(), Duration::from_secs(1), || {
                started_tx.send(()).unwrap();
                finish_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            })
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let skipped = gate.run_if_due(Instant::now(), Duration::from_secs(1), || {
            panic!("concurrent scan")
        });
        // The caller has returned and can dispatch before the first scan finishes.
        assert_eq!(skipped, None::<()>);
        finish_tx.send(()).unwrap();
        assert_eq!(child.join().unwrap(), Some(()));
    }

    #[test]
    fn only_the_failing_worker_retries_until_success() {
        let gate = MaintenanceGate::default();
        let mut retry = false;
        let mut peer_retry = false;
        let interval = Duration::MAX;
        let unexpected_scan = || -> Result<(), &str> { panic!("unexpected scan") };
        for _ in 0..2 {
            assert_eq!(
                gate.run_with_retry(&mut retry, interval, || Err::<(), _>("failed")),
                Err("failed")
            );
            gate.run_with_retry(&mut peer_retry, interval, unexpected_scan)
                .unwrap();
        }
        gate.run_with_retry(&mut retry, interval, || Ok::<_, &str>(()))
            .unwrap();
        gate.run_with_retry(&mut retry, interval, unexpected_scan)
            .unwrap();
    }
}
