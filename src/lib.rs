//! # trouper
//!
//! A single-machine actor runtime whose product is the communication fabric:
//! a runtime-level schema registry, envelope/trace metadata, a tap stream of
//! facts, journal-backed event-sourced actors, topics with per-subscriber
//! cursors, declarative supervision, and dynamic add/remove of actors.
//!
//! See `.plans/actor-runtime-core/plan.md` for the authoritative spec.

pub mod actor;
pub mod builder;
pub mod clock;
pub mod context;
/// Note: public signatures use `error-stack` **0.8** — depend on that
/// version to name the `Report<E>` types this crate returns.
pub mod envelope;
pub mod inbox;
pub mod journal;
pub mod kernel;
pub mod pool;
pub mod registry;
pub mod reply;
pub mod schema;
pub mod state_report;
pub mod supervision;
pub mod system;
pub mod tap;
pub mod topics;

/// Everything a typical actor author needs.
pub mod prelude {
    pub use crate::actor::{ActorKind, ActorPath, SnapshotCadence, StopReason};
    pub use crate::builder::{spawn_es_builder, spawn_foreign, spawn_service_builder};
    pub use crate::clock::Timestamp;
    pub use crate::context::*;
    pub use crate::envelope::*;
    pub use crate::inbox::InboxOffset;
    pub use crate::journal::SeqNo;
    pub use crate::kernel::DeadLetterReason;
    pub use crate::reply::LeaseId;
    pub use crate::schema::*;
    pub use crate::state_report::{ReportState, StateReported, StateReporter};
    pub use crate::system::*;
    pub use crate::topics::Topic;
}
