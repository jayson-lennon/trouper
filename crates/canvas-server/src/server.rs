//! The TCP surface: an accept loop plus per-connection handlers.
//!
//! The accept loop never propagates per-connection failures: each
//! connection runs in its own task, so a panicking or stuck handler
//! cannot kill the listener. Per connection, v1 is strictly sequential —
//! read a line, write a reply, read the next (no pipelining).
//!
//! Error replies keep the connection open ([`Reject`]-shaped failures);
//! an internal failure (a reply that cannot be produced or written) is
//! answered best-effort and then closes, since framing may already be
//! broken. EOF or a read error simply drops the connection.

use crate::protocol::{ErrorReply, ErrorCode, Reject, Request, SnapshotReply};
use actor_runtime::system::ActorSystem;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

/// Serves canvas clients on `bind` forever.
///
/// Loopback by convention — pass a loopback [`SocketAddr`] (the demo host
/// uses `127.0.0.1:7667`). Bind to `127.0.0.1:0` for an OS-assigned
/// (ephemeral) port — tests do exactly that. Only listener failures
/// propagate; connection handling never returns an error to the caller.
///
/// # Errors
///
/// Returns the [`io::Error`] from [`TcpListener::bind`] or the accept
/// loop.
pub async fn serve(system: Arc<ActorSystem>, bind: SocketAddr) -> io::Result<()> {
    let listener = bind_server(bind).await?;
    accept_loop(system, listener).await
}

/// Like [`serve`] but returns the bound listener — the seam tests and
/// hosts need to learn the actual (possibly ephemeral) port. Does not
/// accept connections; pair it with [`accept_loop`].
///
/// # Errors
///
/// Returns the bind error.
pub async fn bind_server(bind: SocketAddr) -> io::Result<TcpListener> {
    let listener = TcpListener::bind(bind).await?;
    let local = listener.local_addr()?;
    tracing::info!("canvas server listening on {local}");
    Ok(listener)
}

/// Accept loop: spawn one task per connection; keep accepting no matter
/// what any single connection does. Exposed for hosts and tests that
/// bind themselves (via [`bind_server`]) to learn the port first.
pub async fn accept_loop(system: Arc<ActorSystem>, listener: TcpListener) -> io::Result<()> {
    loop {
        let (stream, _peer) = listener.accept().await?;
        let system = system.clone();
        tokio::spawn(handle_conn(system, stream));
    }
}

/// One connection: sequential request/response over NDJSON lines.
///
/// Exits on EOF or read error (drop, silently — a reply is not possible);
/// [`Reject`]s reply an error line and keep reading.
async fn handle_conn(system: Arc<ActorSystem>, stream: TcpStream) {
    let addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_owned());
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            // EOF (including a discarded trailing partial line) or read
            // error: nothing more to answer.
            Ok(None) | Err(_) => break,
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let reply = reply_for(&system, trimmed).await;
        let internal = matches!(reply, Reply::Error(ref e) if e.code == ErrorCode::Internal);
        let written = write_line(&mut writer, &reply).await;
        if written.is_err() {
            // Framing is broken; no further replies are possible.
            break;
        }
        if internal {
            // The spec: an internal failure closes the connection.
            break;
        }
    }
    tracing::debug!("canvas connection closed ({addr})");
}

/// Maps one request line to its reply envelope.
///
/// Snapshot export is `async` (live ES state capture takes internal
/// locks); the reply document is built here, so serialization failures
/// surface as [`ErrorCode::Internal`] rather than a half-written line.
async fn reply_for(system: &ActorSystem, line: &str) -> Reply {
    match Request::parse(line) {
        Ok(Request::Snapshot) => {
            let export = system.export().await;
            match SnapshotReply::new(export) {
                Ok(reply) => Reply::Snapshot(reply),
                Err(e) => Reply::Error(ErrorReply::internal(format!(
                    "export serialization failed: {e}"
                ))),
            }
        }
        Err(reject) => Reply::Error(reject_to_reply(&reject)),
    }
}

/// Converts a parse rejection into a versioned error reply.
fn reject_to_reply(reject: &Reject) -> ErrorReply {
    ErrorReply::new(reject.code(), reject.detail())
}

/// Writes one NDJSON line (JSON + `\n`), flushing.
async fn write_line(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    reply: &Reply,
) -> Result<(), io::Error> {
    let json = serde_json::to_string(reply).map_err(io::Error::other)?;
    writer.write_all(json.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await
}

/// Any outbound envelope.
#[derive(Debug, serde::Serialize)]
#[serde(untagged)]
enum Reply {
    /// A successful snapshot reply.
    Snapshot(SnapshotReply),
    /// A versioned error reply.
    Error(ErrorReply),
}

#[cfg(test)]
mod tests {
    use super::*;
    use actor_runtime::actor::{CommandHandler, EventSourcedActor};
    use actor_runtime::prelude::*;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    const REQ: &str = r#"{"v":1,"kind":"snapshot_request"}"#;
    const REQ_V2: &str = r#"{"v":2,"kind":"snapshot_request"}"#;

    /// A minimal command schema so the test system has real content
    /// (schemas, an ES actor, an event, observed edges).
    #[derive(Deserialize)]
    struct Work {
        #[allow(dead_code)]
        n: i64,
    }

    impl Schema for Work {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "Work".into(),
                version: 1,
                kind: SchemaKind::Command,
                fields: vec![FieldDef::required("n", FieldTy::Int)],
                description: None,
            }
        }
    }

    #[derive(Deserialize)]
    struct WorkDone {
        #[allow(dead_code)]
        n: i64,
    }

    impl Schema for WorkDone {
        fn schema_def() -> SchemaDef {
            SchemaDef {
                name: "WorkDone".into(),
                version: 1,
                kind: SchemaKind::Event,
                fields: vec![FieldDef::required("n", FieldTy::Int)],
                description: None,
            }
        }
    }

    #[derive(Serialize, Deserialize, Default)]
    struct Worker {
        total: i64,
    }

    impl EventSourcedActor for Worker {
        fn manifest() -> ActorManifest {
            ActorManifest::new()
                .handles::<Work>()
                .emits::<WorkDone>()
                .kind(ActorKind::EventSourced)
        }

        fn restore(_args: &serde_json::Value) -> Self {
            Self::default()
        }

        fn apply(&mut self, event: &Event) {
            self.total += event.payload["n"].as_i64().unwrap_or(0);
        }
    }

    impl CommandHandler<Work> for Worker {
        fn handle(&self, cmd: Work, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
            vec![Event::new(WorkDone::schema_id(), json!({ "n": cmd.n }))]
        }
    }

    /// A system with two schemas and one ES actor that has handled one
    /// command (so the export has actors, state, and observed edges).
    async fn populated_system() -> Arc<ActorSystem> {
        let system = Arc::new(ActorSystem::new(SystemConfig::production()));
        system.register_schema::<Work>();
        system.register_schema::<WorkDone>();
        actor_runtime::builder::spawn_es_builder::<Worker>(&system)
            .at(ActorPath::new("api"))
            .args(json!({}))
            .handles::<Work>()
            .emits::<WorkDone>()
            .start();
        system
            .send(system.envelope(Work::schema_id(), ActorPath::new("api"), json!({ "n": 7 })))
            .await
            .expect("delivered");
        for _ in 0..1_000 {
            if system
                .inbox_cursor(&ActorPath::new("api"))
                .map(actor_runtime::types::InboxOffset::as_u64)
                == Some(1)
            {
                return system;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("test system never processed its command");
    }

    /// Binds a server on an ephemeral loopback port and spawns its accept
    /// loop. Returns the bound address.
    async fn spawn_server(system: Arc<ActorSystem>) -> SocketAddr {
        let listener = bind_server("127.0.0.1:0".parse().expect("loopback"))
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(accept_loop(system, listener));
        addr
    }

    /// A connected test client: write raw lines, read reply lines.
    struct TestClient {
        writer: tokio::net::tcp::OwnedWriteHalf,
        lines: tokio::io::Lines<tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>>,
    }

    async fn connect(addr: SocketAddr) -> TestClient {
        let stream = TcpStream::connect(addr).await.expect("connect");
        let (reader, writer) = stream.into_split();
        TestClient {
            writer,
            lines: BufReader::new(reader).lines(),
        }
    }

    /// Sends one raw line and reads back exactly one reply line.
    async fn ask(client: &mut TestClient, line: &str) -> serde_json::Value {
        use tokio::io::AsyncWriteExt;
        client.writer.write_all(line.as_bytes()).await.expect("write");
        client.writer.write_all(b"\n").await.expect("write");
        client.writer.flush().await.expect("flush");
        let reply = client
            .lines
            .next_line()
            .await
            .expect("reply line")
            .expect("read");
        serde_json::from_str(&reply).expect("reply JSON")
    }

    #[tokio::test]
    async fn snapshot_request_answers_with_export_equal_to_direct_export() {
        // Given a live system behind a canvas server.
        let system = populated_system().await;
        let addr = spawn_server(system.clone()).await;
        let mut client = connect(addr).await;

        // When a client asks for a snapshot.
        let reply = ask(&mut client, REQ).await;

        // Then the reply is a versioned snapshot.
        assert_eq!(reply["v"], 1);
        assert_eq!(reply["kind"], "snapshot");
        // And its export document equals the direct export, JSON for JSON.
        let direct = serde_json::to_value(system.export().await).expect("serialize");
        assert_eq!(reply["export"], direct);
        // And the round-trip survives deserialization back to SystemExport.
        let round: SystemExport =
            serde_json::from_value(reply["export"].clone()).expect("deserialize");
        assert_eq!(round, system.export().await);
    }

    #[tokio::test]
    async fn unknown_version_request_receives_error_and_connection_stays_open() {
        // Given a running canvas server.
        let system = populated_system().await;
        let addr = spawn_server(system).await;
        let mut client = connect(addr).await;

        // When the client sends a v2 request.
        let reply = ask(&mut client, REQ_V2).await;

        // Then it receives a versioned unknown_version error.
        assert_eq!(reply["kind"], "error");
        assert_eq!(reply["code"], "unknown_version");
        // And the same connection completes a valid request afterwards.
        let after = ask(&mut client, REQ).await;
        assert_eq!(after["kind"], "snapshot");
    }

    #[tokio::test]
    async fn malformed_request_receives_error_and_listener_survives() {
        // Given a running canvas server.
        let system = populated_system().await;
        let addr = spawn_server(system).await;

        // When a client sends a line that is not a request envelope.
        let mut client = connect(addr).await;
        let reply = ask(&mut client, "hello").await;

        // Then it receives a malformed error on that connection.
        assert_eq!(reply["kind"], "error");
        assert_eq!(reply["code"], "malformed");

        // And a fresh connection still gets a snapshot (the listener
        // survived).
        let mut fresh = connect(addr).await;
        let after = ask(&mut fresh, REQ).await;
        assert_eq!(after["kind"], "snapshot");
    }

    #[tokio::test]
    async fn concurrent_connections_each_receive_snapshots() {
        // Given a running canvas server with two clients connected.
        let system = populated_system().await;
        let addr = spawn_server(system).await;
        let mut client_a = connect(addr).await;
        let mut client_b = connect(addr).await;

        // When both connections complete a snapshot request.
        let from_a = ask(&mut client_a, REQ).await;
        let from_b = ask(&mut client_b, REQ).await;

        // Then both received snapshots.
        assert_eq!(from_a["kind"], "snapshot");
        assert_eq!(from_b["kind"], "snapshot");
    }

    #[tokio::test]
    async fn empty_lines_are_skipped_without_a_reply() {
        // Given a running canvas server.
        let system = populated_system().await;
        let addr = spawn_server(system).await;
        let mut client = connect(addr).await;

        // When the client sends blank lines followed by a valid request.
        use tokio::io::AsyncWriteExt;
        client.writer.write_all(b"\n   \n").await.expect("write");
        client.writer.write_all(REQ.as_bytes()).await.expect("write");
        client.writer.write_all(b"\n").await.expect("write");
        client.writer.flush().await.expect("flush");

        // Then the first reply line is the snapshot (no error replies for
        // the blanks).
        let first = client.lines.next_line().await.expect("reply").expect("read");
        let reply: serde_json::Value = serde_json::from_str(&first).expect("JSON");
        assert_eq!(reply["kind"], "snapshot");
    }
}
