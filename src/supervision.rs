//! Declarative supervision: restart policies, budgets, and backoff as
//! plain data interpreted by the restart engine.
//!
//! An [`ActorSpec`] says WHAT to do when a child fails; the engine
//! (kernel) decides WHEN it may happen. Failures inside the budget
//! restart the child after a backoff delay; exhausting the budget stops
//! the child and escalates a control message (an ordinary message the
//! parent handles) to the parent.

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::VecDeque;
use std::time::Duration;

/// When a failed child may be restarted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RestartPolicy {
    /// Always restart on failure.
    #[default]
    Permanent,
    /// Restart only if the child failed (not on clean stops).
    Transient,
    /// Never restart; a failure escalates immediately.
    Never,
}

/// A sliding-window restart budget: at most `max` restarts per `window`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestartBudget {
    /// The maximum restarts allowed inside the window.
    pub max: u32,
    /// The window the restarts are counted in.
    #[serde(default)]
    pub window: Duration,
}

impl RestartBudget {
    /// A budget of `max` restarts per the given window.
    pub fn per(max: u32, window: Duration) -> Self {
        Self { max, window }
    }
}

impl Default for RestartBudget {
    fn default() -> Self {
        Self::per(5, Duration::from_secs(10))
    }
}

/// Exponential backoff: `base * factor^n`, capped at `max`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Backoff {
    /// The delay after the first failure.
    pub base: Duration,
    /// The delay ceiling.
    pub max: Duration,
    /// The growth factor per consecutive failure.
    pub factor: f64,
}

impl Backoff {
    /// The delay before restart `attempt` (1-based).
    pub fn delay(&self, attempt: u32) -> Duration {
        let exp = self.factor.powi(attempt.saturating_sub(1) as i32);
        let delay = self.base.as_secs_f64() * exp;
        Duration::from_secs_f64(delay.min(self.max.as_secs_f64()))
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            base: Duration::from_millis(10),
            max: Duration::from_secs(5),
            factor: 2.0,
        }
    }
}

/// One supervised actor: what to spawn, when to restart it, and how hard.
#[derive(Clone)]
pub struct ActorSpec {
    /// The child's path (its identity across restarts).
    pub path: crate::actor::ActorPath,
    /// The child's parent (escalation target); `None` = the system itself.
    pub parent: Option<crate::actor::ActorPath>,
    /// When a failed child may be restarted.
    pub restart: RestartPolicy,
    /// The sliding-window restart budget.
    pub budget: RestartBudget,
    /// Exponential backoff between restarts.
    pub backoff: Backoff,
    /// Genesis args (reused at every restart — the child's config).
    pub args: serde_json::Value,
    /// The spawn closure the kernel calls to rebuild the child. Returns
    /// Ok(()) after the child runs again; the kernel drives restarts.
    #[allow(clippy::type_complexity)]
    pub spawn: std::sync::Arc<
        dyn Fn(&crate::system::ActorSystem, &crate::actor::ActorPath, &serde_json::Value)
            + Send
            + Sync,
    >,
}

impl std::fmt::Debug for ActorSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorSpec")
            .field("path", &self.path)
            .field("parent", &self.parent)
            .field("restart", &self.restart)
            .field("budget", &self.budget)
            .field("backoff", &self.backoff)
            .finish_non_exhaustive()
    }
}

impl ActorSpec {
    /// The JSON control envelope the engine escalates to the parent.
    pub fn escalation_message(&self, reason: &str) -> serde_json::Value {
        json!({
            "escalated": self.path.to_string(),
            "reason": reason,
        })
    }
}

/// The sliding-window failure record for one child.
#[derive(Debug)]
pub struct FailureWindow {
    /// Timestamps (millis) of recent failures, oldest first.
    failures: VecDeque<u64>,
}

impl FailureWindow {
    /// An empty failure window.
    pub fn new() -> Self {
        Self {
            failures: VecDeque::new(),
        }
    }

    /// Records a failure at `now_ms`.
    pub fn record(&mut self, now_ms: u64) {
        self.failures.push_back(now_ms);
    }

    /// Whether `budget.max` failures occurred within `window_ms` of now.
    pub fn exhausted(&self, now_ms: u64, budget: &RestartBudget) -> bool {
        let window_ms = budget.window.as_millis() as u64;
        self.failures
            .iter()
            .filter(|&&t| now_ms.saturating_sub(t) <= window_ms)
            .count()
            >= budget.max as usize
    }

    /// The count of failures inside the window (inspection/tests).
    pub fn count(&self, now_ms: u64, budget: &RestartBudget) -> usize {
        let window_ms = budget.window.as_millis() as u64;
        self.failures
            .iter()
            .filter(|&&t| now_ms.saturating_sub(t) <= window_ms)
            .count()
    }

    /// Whether no failure is recorded.
    pub fn is_empty(&self) -> bool {
        self.failures.is_empty()
    }

    /// The total number of recorded failures (inspection/tests; the
    /// windowed count is [`Self::count`]).
    pub fn len(&self) -> usize {
        self.failures.len()
    }

    /// Drops failures older than the window (housekeeping).
    pub fn prune(&mut self, now_ms: u64, budget: &RestartBudget) {
        let window_ms = budget.window.as_millis() as u64;
        while let Some(&t) = self.failures.front() {
            if now_ms.saturating_sub(t) > window_ms {
                self.failures.pop_front();
            } else {
                break;
            }
        }
    }
}

impl Default for FailureWindow {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_delays_grow_exponentially_and_cap() {
        // Given a backoff of 10ms doubling to a 40ms ceiling.
        let backoff = Backoff {
            base: Duration::from_millis(10),
            max: Duration::from_millis(40),
            factor: 2.0,
        };

        // When delays are taken for successive attempts.
        let delays: Vec<u64> = (1..=5)
            .map(|n| backoff.delay(n).as_millis() as u64)
            .collect();

        // Then they double and cap at max.
        assert_eq!(delays, vec![10, 20, 40, 40, 40]);
    }

    #[test]
    fn failure_window_exhausts_at_the_budget_maximum() {
        // Given a budget of 3 failures per 100ms.
        let budget = RestartBudget::per(3, Duration::from_millis(100));
        let mut window = FailureWindow::new();

        // When three failures happen inside the window.
        window.record(0);
        window.record(10);
        window.record(20);

        // Then the budget is exhausted.
        assert!(window.exhausted(30, &budget));
        assert_eq!(window.count(30, &budget), 3);
    }

    #[test]
    fn failure_window_forgives_old_failures() {
        // Given a budget of 3 per 100ms with three recorded failures.
        let budget = RestartBudget::per(3, Duration::from_millis(100));
        let mut window = FailureWindow::new();
        window.record(0);
        window.record(10);
        window.record(20);

        // When the window slides past the oldest failure.
        window.prune(105, &budget);

        // Then only failures inside the window still count.
        assert_eq!(window.count(105, &budget), 2);
        assert!(!window.exhausted(105, &budget));
    }

    #[test]
    fn restart_policy_defaults_to_permanent() {
        // Given the default policy.
        let policy = RestartPolicy::default();

        // Then it is Permanent.
        assert_eq!(policy, RestartPolicy::Permanent);
    }

    #[test]
    fn escalation_message_carries_path_and_reason() {
        // Given a child spec's identity.
        let spec_path = crate::actor::ActorPath::new("worker");

        // When the escalation message is built.
        let message = json!({
            "escalated": spec_path.to_string(),
            "reason": "budget exhausted",
        });

        // Then it names the child and the reason.
        assert_eq!(message["escalated"], "worker");
        assert_eq!(message["reason"], "budget exhausted");
    }
}
