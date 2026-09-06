//! canvas-server: exposes a running [`ActorSystem`] over loopback TCP as
//! NDJSON.
//!
//! One request type, one response family, versioned envelopes (see
//! [`protocol`]): a `snapshot_request` is answered with a `snapshot`
//! carrying the full JSON `SystemExport` document; malformed or
//! unknown-version input receives a versioned `error` reply and the
//! connection stays open.
//!
//! Run it inside a host process (an example, a demo, eventually any
//! binary that owns an `ActorSystem`):
//!
//! ```ignore
//! # async fn demo(system: std::sync::Arc<actor_runtime::system::ActorSystem>) -> std::io::Result<()> {
//! canvas_server::serve(system, "127.0.0.1:7667".parse().expect("valid addr")).await
//! # }
//! ```

pub mod protocol;
pub mod server;
