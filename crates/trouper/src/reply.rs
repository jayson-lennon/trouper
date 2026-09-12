//! Reply-slot leases: the mechanism half of the name/mechanism split.
//!
//! A reply address is either a durable [`Address::Path`] (a name — survives
//! restarts, an ordinary envelope) or [`Address::Slot`] (a lease — a oneshot
//! channel that exists only as long as its asker awaits, with an expiry so
//! a dead asker's slot can never leak). The log records only names; slots
//! are runtime-internal and die with the ask.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::time::Duration;

use serde_json::Value as JsonValue;
use tokio::sync::oneshot;

use crate::types::{LeaseId, Timestamp};

/// One reply slot: the asker's oneshot plus its expiry.
pub struct ReplySlot {
    /// Where the reply is delivered (taken on reply).
    pub sender: oneshot::Sender<JsonValue>,
    /// When the lease expires (swept by [`ReplyTable::prune`]).
    pub expires_at: Timestamp,
}

/// All live reply leases.
#[derive(Default)]
pub struct ReplyTable {
    slots: Mutex<HashMap<LeaseId, ReplySlot>>,
}

impl ReplyTable {
    /// Opens a lease: registers a oneshot with an expiry and returns both
    /// ends — the [`LeaseId`] (for the envelope's reply address) and the
    /// receiver the asker awaits.
    pub fn open(&self, ttl: Duration, now: Timestamp) -> (LeaseId, oneshot::Receiver<JsonValue>) {
        let (sender, receiver) = oneshot::channel();
        let lease = LeaseId::new();
        self.slots.lock().insert(
            lease,
            ReplySlot {
                sender,
                expires_at: Timestamp::from_millis(now.as_millis() + ttl.as_millis() as u64),
            },
        );
        (lease, receiver)
    }

    /// Completes a lease: delivers `payload` to the asker if the slot is
    /// still live. Returns false when the slot is gone (expired or pruned).
    pub fn complete(&self, lease: &LeaseId, payload: JsonValue) -> bool {
        match self.slots.lock().remove(lease) {
            Some(slot) => slot.sender.send(payload).is_ok(),
            None => false,
        }
    }

    /// Cancels a lease outright (timeout/settle): the slot is removed so
    /// a late reply finds nothing. Idempotent.
    pub fn cancel(&self, lease: &LeaseId) {
        self.slots.lock().remove(lease);
    }

    /// Drops expired leases (a dead asker's slot must not accumulate).
    pub fn prune(&self, now: Timestamp) {
        self.slots.lock().retain(|_, slot| slot.expires_at > now);
    }

    /// The number of live leases (inspection).
    pub fn len(&self) -> usize {
        self.slots.lock().len()
    }

    /// Whether no leases are live.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_and_complete_roundtrips_the_reply() {
        // Given an open lease.
        let table = ReplyTable::default();
        let (lease, receiver) = table.open(Duration::from_secs(10), Timestamp::from_millis(0));

        // When completing it.
        let delivered = table.complete(&lease, serde_json::json!({ "ok": true }));

        // Then the asker receives the payload and the slot is consumed.
        assert!(delivered);
        assert_eq!(receiver.await_sync(), serde_json::json!({ "ok": true }));
        assert!(table.is_empty());
    }

    #[test]
    fn complete_after_prune_reports_a_dead_slot() {
        // Given a lease with a short ttl, expired on the clock.
        let table = ReplyTable::default();
        let (lease, _receiver) = table.open(Duration::from_millis(5), Timestamp::from_millis(0));

        // When pruning after expiry.
        table.prune(Timestamp::from_millis(10));
        let delivered = table.complete(&lease, serde_json::json!({}));

        // Then the slot is gone and completion reports failure.
        assert!(!delivered);
        assert!(table.is_empty());
    }

    /// Await helper for the sync test (single value, sender already fired).
    trait AwaitSync {
        fn await_sync(self) -> JsonValue;
    }

    impl AwaitSync for oneshot::Receiver<JsonValue> {
        fn await_sync(self) -> JsonValue {
            use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
            fn noop(_: *const ()) {}
            fn clone(_: *const ()) -> RawWaker {
                RawWaker::new(std::ptr::null(), &VTABLE)
            }
            static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
            let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
            let mut cx = Context::from_waker(&waker);
            let mut receiver = self;
            loop {
                if let Poll::Ready(Ok(value)) = std::pin::Pin::new(&mut receiver).poll(&mut cx) {
                    return value;
                }
                std::thread::yield_now();
            }
        }
    }
}
