//! # trouper
//!
//! A single-machine actor runtime whose product is the communication fabric:
//! a runtime-level schema registry, envelope/trace metadata, opt-in runtime
//! observation, journal-backed event-sourced actors, declarative supervision, and
//! dynamic add/remove of actors.
//!
//! The `examples/` directory in the repository is the tour: each file is a runnable scenario over the public API.

// The schema derive macros emit `::trouper::...` paths (so they resolve for
// downstream consumers); aliasing `self` makes the same paths work inside
// this crate, where `trouper` is not a dependency of itself.
extern crate self as trouper;

pub mod actor;
pub mod builder;
pub mod clock;
pub mod context;
/// Note: public signatures use `error-stack` **0.8** — depend on that
/// version to name the `Report<E>` types this crate returns.
pub mod envelope;
pub mod inbox;
pub mod journal;
pub mod json;
pub mod kernel;
pub mod observe;
pub mod pool;
pub mod registry;
pub mod reply;
pub mod schema;
pub mod state_report;
pub mod supervision;
pub mod system;

pub use crate::json::Json;

// The schema derive macros live in `schema` (the prelude globs them from
// there); the root re-export lets consumers write `trouper::Event` /
// `trouper::Command` or `use trouper::{Event, Command}` without reaching
// through the module or the prelude.
pub use crate::schema::{Command, Event};

/// Everything a typical actor author needs.
pub mod prelude {
    pub use crate::actor::{ActorKind, ActorPath, Projector, SnapshotCadence, StopReason};
    pub use crate::builder::{
        spawn_es_builder, spawn_foreign, spawn_projector_builder, spawn_service_builder,
    };
    pub use crate::clock::Timestamp;
    pub use crate::context::*;
    pub use crate::envelope::*;
    pub use crate::inbox::InboxOffset;
    pub use crate::journal::SeqNo;
    pub use crate::json::Json;
    pub use crate::kernel::DeadLetterReason;
    pub use crate::pool::{PartitionSpec, ProjectorSetSpec};
    pub use crate::reply::LeaseId;
    pub use crate::schema::*;
    pub use crate::state_report::{ReportState, StateReported, StateReporter};
    pub use crate::system::*;
}
