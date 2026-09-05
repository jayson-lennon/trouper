//! # actor-runtime
//!
//! A single-machine actor runtime whose product is the communication fabric:
//! a runtime-level schema registry, envelope/trace metadata, a tap stream of
//! facts, journal-backed event-sourced actors, topics with per-subscriber
//! cursors, declarative supervision, and dynamic add/remove of actors.
//!
//! See `.plans/actor-runtime-core/plan.md` for the authoritative spec.

pub mod actor;
pub mod clock;
pub mod context;
pub mod envelope;
pub mod inbox;
pub mod journal;
pub mod kernel;
pub mod registry;
pub mod schema;
pub mod supervision;
pub mod system;
pub mod tap;
pub mod topics;
pub mod types;

/// Everything a typical actor author needs.
pub mod prelude {
    pub use crate::actor::*;
    pub use crate::context::*;
    pub use crate::envelope::*;
    pub use crate::schema::*;
    pub use crate::system::*;
    pub use crate::types::*;
}
