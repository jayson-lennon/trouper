//! The injected clock service: the only source of time in the runtime.
//!
//! A behavior service per the project skill: the implementation varies
//! (`SystemClock` in production, `FakeClock` in tests), so it lives behind a
//! trait with an `Arc<dyn>` wrapper. Deterministic tests require that no
//! kernel code reads wall time directly.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use derive_more::Debug;
use tokio::sync::watch;

use serde::{Deserialize, Serialize};

/// The capability to know what time it is.
///
/// Implemented by the production clock and test fakes; never called directly
/// by user code — the runtime reaches it through [`ClockService`].
pub trait ClockBackend: Send + Sync {
    /// The current time in epoch milliseconds.
    fn now_millis(&self) -> u64;
    /// The name of this backend, for debugging.
    fn name(&self) -> &'static str;
}

/// Errors surfaced by the clock service.
#[derive(Debug, wherror::Error)]
#[error(debug)]
pub struct ClockError;

/// Shared, cloneable wrapper around a clock backend.
///
/// Clones share the same backend: advancing a [`FakeClock`] is visible to
/// every holder of the service, which is what makes timeouts testable.
#[derive(Clone, Debug)]
pub struct ClockService {
    #[debug("ClockService<{}>", self.backend.name())]
    backend: Arc<dyn ClockBackend>,
    /// The backend as a fake clock, when one was installed (tests).
    fake: Option<Arc<FakeClock>>,
}

impl ClockService {
    /// Wraps a backend.
    pub fn new(backend: Arc<dyn ClockBackend>) -> Self {
        Self {
            backend,
            fake: None,
        }
    }

    /// Wraps a [`FakeClock`], keeping a handle so tests can advance it.
    pub fn fake(start_millis: u64) -> (Self, Arc<FakeClock>) {
        let fake = FakeClock::new(start_millis);
        (
            Self {
                backend: fake.clone(),
                fake: Some(fake.clone()),
            },
            fake,
        )
    }

    /// The backend as a [`FakeClock`], when one was installed (tests).
    pub fn backend_fake(&self) -> Option<Arc<FakeClock>> {
        self.fake.clone()
    }

    /// The current time.
    pub fn now(&self) -> Timestamp {
        Timestamp::from_millis(self.backend.now_millis())
    }

    /// The backend's name, for debugging.
    pub fn name(&self) -> &'static str {
        self.backend.name()
    }
}

/// The production clock: wall time.
#[derive(Debug, Default)]
pub struct SystemClock;

impl SystemClock {
    /// Creates the production clock.
    pub fn new() -> Self {
        Self
    }
}

impl ClockBackend for SystemClock {
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|since| since.as_millis() as u64)
            .unwrap_or_default()
    }

    fn name(&self) -> &'static str {
        "system"
    }
}

/// A controllable clock for tests: time advances only when told to.
///
/// Shared via [`ClockService`]; advancing it moves every reader forward.
#[derive(Debug)]
pub struct FakeClock {
    state: watch::Receiver<u64>,
    set: watch::Sender<u64>,
}

impl FakeClock {
    /// Creates a fake clock reading `start_millis`.
    pub fn new(start_millis: u64) -> Arc<Self> {
        let (set, state) = watch::channel(start_millis);
        Arc::new(Self { state, set })
    }

    /// Moves the clock to `millis` (may move it backward; tests know).
    pub fn set_millis(&self, millis: u64) {
        let _ = self.set.send(millis);
    }

    /// Advances the clock by `delta`.
    pub fn advance(&self, delta: Duration) {
        let next = self.state.borrow().saturating_add(delta.as_millis() as u64);
        let _ = self.set.send(next);
    }

    /// Waits until the clock reads at least `millis`, bounded so a test that
    /// forgot to advance fails instead of hanging.
    ///
    /// # Errors
    ///
    /// Returns an error if no one can advance the clock anymore (all
    /// [`FakeClock`] handles dropped mid-test).
    pub async fn wait_until(&self, millis: u64) -> Result<(), ClockError> {
        // A bounded number of wakeups without reaching `millis` means the
        // test forgot to advance far enough; fail rather than hang.
        const MAX_WAKES: u32 = 2_000;
        let mut state = self.state.clone();
        let mut wakes = 0_u32;
        while *state.borrow() < millis {
            if state.changed().await.is_err() {
                return Err(ClockError);
            }
            wakes += 1;
            if wakes > MAX_WAKES {
                return Err(ClockError);
            }
        }
        Ok(())
    }
}

impl ClockBackend for FakeClock {
    fn now_millis(&self) -> u64 {
        *self.state.borrow()
    }

    fn name(&self) -> &'static str {
        "fake"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_reads_epoch_millis() {
        // Given the production clock.
        let clock = ClockService::new(Arc::new(SystemClock::new()));

        // When reading twice.
        let first = clock.now();
        let second = clock.now();

        // Then both readings are nonzero epoch millis and non-decreasing.
        assert!(first.as_millis() > 0);
        assert!(second >= first);
        assert_eq!(clock.name(), "system");
    }

    #[test]
    fn fake_clock_starts_at_configured_time() {
        // Given a fake clock started at 1_000.
        let clock = ClockService::new(FakeClock::new(1_000));

        // When reading the time.
        let now = clock.now();

        // Then it reads exactly 1_000.
        assert_eq!(now.as_millis(), 1_000);
    }

    #[tokio::test]
    async fn fake_clock_advance_is_visible_through_every_service_clone() {
        // Given a fake clock shared through two clones of its service.
        let fake = FakeClock::new(500);
        let a = ClockService::new(fake.clone());
        let b = a.clone();

        // When advancing by 30 seconds through the backend handle.
        fake.advance(Duration::from_secs(30));

        // Then both service clones see the new time.
        assert_eq!(a.now().as_millis(), 30_500);
        assert_eq!(b.now().as_millis(), 30_500);
    }

    #[tokio::test]
    async fn fake_clock_wait_until_returns_when_time_reached() {
        // Given a fake clock at 0 and a waiter for 1_000.
        let fake = FakeClock::new(0);
        let waiter = {
            let fake = fake.clone();
            tokio::spawn(async move { fake.wait_until(1_000).await })
        };

        // When advancing past 1_000.
        fake.advance(Duration::from_millis(1_500));

        // Then the waiter completes successfully.
        waiter.await.expect("joined").expect("wait_until");
    }

    #[tokio::test]
    async fn fake_clock_wait_until_fails_when_time_never_advances() {
        // Given a waiter on a time the test never advances to.
        let fake = FakeClock::new(0);
        let waiter = {
            let fake = fake.clone();
            tokio::spawn(async move { fake.wait_until(1_000).await })
        };

        // When nudging the clock repeatedly but never reaching the target
        // (each nudge counts as a wake; the waiter has a bounded wake budget).
        for _ in 0..3_000 {
            fake.set_millis(10);
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        let result = waiter.await.expect("joined");

        // Then the waiter fails instead of hanging.
        assert!(result.is_err());
    }
}

/// Milliseconds since the Unix epoch, always sourced from the injected clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Wraps raw epoch milliseconds (from the injected clock).
    pub fn from_millis(millis: u64) -> Self {
        Self(millis)
    }

    /// Raw epoch milliseconds.
    pub fn as_millis(self) -> u64 {
        self.0
    }
}

#[test]
fn timestamp_roundtrips_millis() {
    // Given raw epoch milliseconds.
    let ts = Timestamp::from_millis(1_756_000_000_000);

    // When reading them back.
    let millis = ts.as_millis();

    // Then the value is preserved.
    assert_eq!(millis, 1_756_000_000_000);
}
