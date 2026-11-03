// SPDX-License-Identifier: Apache-2.0

use std::sync::{
    Mutex, TryLockError,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

/// Coalesce maintenance without making dispatch wait for another worker's scan.
#[derive(Default)]
pub(super) struct MaintenanceGate {
    next_scan: Mutex<Option<Instant>>,
    disabled: AtomicBool,
}

impl MaintenanceGate {
    pub(super) fn run_if_due<T>(
        &self,
        now: Instant,
        interval: Duration,
        work: impl FnOnce() -> T,
    ) -> Option<T> {
        if self.disabled.load(Ordering::Acquire) {
            return Some(work());
        }
        let mut next_scan = match self.next_scan.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => return None,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
        };
        if next_scan.is_some_and(|next| now < next) {
            return None;
        }
        // The guard covers only maintenance. Successful idle results are never cached.
        *next_scan = Some(now + interval);
        Some(work())
    }

    pub(super) fn disable(&self) {
        self.disabled.store(true, Ordering::Release);
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
    fn disabling_restores_unthrottled_work() {
        let gate = MaintenanceGate::default();
        let now = Instant::now();
        let interval = Duration::from_secs(60);
        assert_eq!(
            gate.run_if_due(now, interval, || Err::<(), _>("scan failed")),
            Some(Err("scan failed"))
        );
        gate.disable();
        for _ in 0..3 {
            assert_eq!(
                gate.run_if_due(now, interval, || Err::<(), _>("scan failed")),
                Some(Err("scan failed"))
            );
        }
    }
}
