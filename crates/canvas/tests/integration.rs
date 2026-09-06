//! Client-side integration tests: every [`CanvasError`] variant against
//! a scripted fake server, plus the happy path against the real
//! `canvas_server` (the cross-crate round-trip).

use canvas::{connect_snapshot, CanvasError};
use std::time::Duration;

/// JSON literal helper (the runtime prelude's `json!` is not exported as
/// a macro path here).
macro_rules! json {
    ($($tt:tt)*) => {
        serde_json::json!($($tt)*)
    };
}

/// A short budget for failure-mode tests (keeps the suite fast).
const SHORT: Duration = Duration::from_millis(300);

/// A scripted one-shot server: accepts a single connection, reads the
/// request line (so the client's write cannot race ahead), then performs
/// `behavior` and drops the connection.
async fn fake_server<F, Fut>(behavior: F) -> std::net::SocketAddr
where
    F: FnOnce(tokio::net::tcp::OwnedWriteHalf) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    use tokio::io::AsyncBufReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake server bind");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("fake server accept");
        let (reader, writer) = stream.into_split();
        let mut lines = tokio::io::BufReader::new(reader).lines();
        // Consume the request line (best-effort; behavior owns the rest).
        let _ = lines.next_line().await;
        behavior(writer).await;
        drop(lines);
    });
    addr
}

#[tokio::test]
async fn connect_refused_maps_to_connect_error_naming_the_addr() {
    // Given no listener at all (bind a socket, then drop the listener so
    // the port is closed).
    let addr = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        listener.local_addr().expect("local addr")
    };

    // When connecting.
    let error = connect_snapshot(addr, SHORT).await.expect_err("refused");

    // Then the error is Connect and its message names the address.
    assert!(matches!(error, CanvasError::Connect { addr: got, .. } if got == addr));
    let message = error.to_string();
    assert!(
        message.starts_with(&format!("could not connect to {addr}: ")),
        "message did not name the addr: {message}"
    );
}

#[tokio::test]
async fn silent_server_maps_to_timeout_within_the_budget() {
    // Given a fake server that accepts and then parks forever, holding
    // the connection open without ever replying.
    let addr = fake_server(|writer| async move {
        // The async block must own the write half — dropping it would
        // close the connection and turn this into a hangup test.
        let _held = writer;
        std::future::pending::<()>().await;
    })
    .await;
    let started = std::time::Instant::now();

    // When asking for a snapshot.
    let error = connect_snapshot(addr, SHORT).await.expect_err("timeout");

    // Then the error is Timeout (not Connect/Closed/Protocol — print it
    // to name the actual variant on failure), and it arrived no earlier
    // than the budget.
    assert!(
        matches!(error, CanvasError::Timeout { addr: got, .. } if got == addr),
        "expected Timeout, got: {error:?}"
    );
    assert!(started.elapsed() >= SHORT);
}

#[tokio::test]
async fn error_reply_maps_to_protocol_error_carrying_code_and_detail() {
    // Given a fake server that answers with a versioned error envelope.
    let addr = fake_server(|mut writer| async move {
        use tokio::io::AsyncWriteExt;
        let line = r#"{"v":1,"kind":"error","code":"internal","detail":"boom"}"#;
        writer.write_all(line.as_bytes()).await.expect("write");
        writer.write_all(b"\n").await.expect("write");
        writer.flush().await.expect("flush");
    })
    .await;

    // When asking for a snapshot.
    let error = connect_snapshot(addr, SHORT).await.expect_err("protocol error");

    // Then the error is Protocol carrying the code and detail verbatim.
    match error {
        CanvasError::Protocol { code, detail, .. } => {
            assert_eq!(code, "internal");
            assert_eq!(detail, "boom");
        }
        other => panic!("expected Protocol, got {other:?}"),
    }
}

#[tokio::test]
async fn future_version_reply_maps_to_protocol_error() {
    // Given a fake server that answers with a v2 snapshot envelope.
    let addr = fake_server(|mut writer| async move {
        use tokio::io::AsyncWriteExt;
        let line = r#"{"v":2,"kind":"snapshot","export":{}}"#;
        writer.write_all(line.as_bytes()).await.expect("write");
        writer.write_all(b"\n").await.expect("write");
        writer.flush().await.expect("flush");
    })
    .await;

    // When asking for a snapshot.
    let error = connect_snapshot(addr, SHORT).await.expect_err("version error");

    // Then the error is Protocol with the unknown_version code and a
    // detail naming both versions.
    match error {
        CanvasError::Protocol { code, detail, .. } => {
            assert_eq!(code, "unknown_version");
            assert!(detail.contains("v1"));
            assert!(detail.contains("v2"));
        }
        other => panic!("expected Protocol, got {other:?}"),
    }
}

#[tokio::test]
async fn garbage_reply_maps_to_protocol_error_without_panicking() {
    // Given a fake server that answers with a non-JSON line.
    let addr = fake_server(|mut writer| async move {
        use tokio::io::AsyncWriteExt;
        writer.write_all(b"not json at all\n").await.expect("write");
        writer.flush().await.expect("flush");
    })
    .await;

    // When asking for a snapshot.
    let error = connect_snapshot(addr, SHORT).await.expect_err("protocol error");

    // Then the error is Protocol with the unparseable code.
    match error {
        CanvasError::Protocol { code, .. } => assert_eq!(code, "unparseable"),
        other => panic!("expected Protocol, got {other:?}"),
    }
}

#[tokio::test]
async fn undecodable_snapshot_maps_to_payload_error() {
    // Given a fake server that answers a well-formed snapshot envelope
    // whose export is not a SystemExport.
    let addr = fake_server(|mut writer| async move {
        use tokio::io::AsyncWriteExt;
        let line = r#"{"v":1,"kind":"snapshot","export":{"bogus":true}}"#;
        writer.write_all(line.as_bytes()).await.expect("write");
        writer.write_all(b"\n").await.expect("write");
        writer.flush().await.expect("flush");
    })
    .await;

    // When asking for a snapshot.
    let error = connect_snapshot(addr, SHORT).await.expect_err("payload error");

    // Then the error is Payload.
    assert!(matches!(error, CanvasError::Payload { .. }));
}

#[tokio::test]
async fn server_hangup_maps_to_closed() {
    // Given a fake server that reads the request and immediately drops
    // the connection without replying.
    let addr = fake_server(|_| async {}).await;

    // When asking for a snapshot with NO timeout budget (0-sized timeout
    // would fire first, so use a generous one and rely on the hangup).
    let error = connect_snapshot(addr, Duration::from_secs(5))
        .await
        .expect_err("closed");

    // Then the error is Closed (EOF surfaced before the timeout).
    assert!(matches!(error, CanvasError::Closed { .. }));
}

#[tokio::test]
async fn happy_path_against_the_real_server_round_trips_the_export() {
    use actor_runtime::actor::{CommandHandler, EventSourcedActor};
    use actor_runtime::prelude::*;
    use serde::{Deserialize, Serialize};

    // Given a live mini-system behind a real canvas server.
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

    let system = std::sync::Arc::new(ActorSystem::new(SystemConfig::production()));
    system.register_schema::<Work>();
    system.register_schema::<WorkDone>();
    actor_runtime::builder::spawn_es_builder::<Worker>(&system)
        .at(ActorPath::new("api"))
        .args(json!({}))
        .handles::<Work>()
        .emits::<WorkDone>()
        .start();

    let listener = canvas_server::server::bind_server("127.0.0.1:0".parse().expect("loopback"))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let system_for_loop = system.clone();
    tokio::spawn(async move {
        let _ = canvas_server::server::accept_loop(system_for_loop, listener).await;
    });

    // When connecting with the client API.
    let export = connect_snapshot(addr, Duration::from_secs(5))
        .await
        .expect("snapshot");

    // Then the received export equals the server's direct export.
    assert_eq!(export, system.export().await);
    // And the summary matches the direct export's real content (the
    // system may carry builtin schemas, so counts come from the source).
    let direct = system.export().await;
    let summary = canvas::SnapshotSummary::of(&export);
    assert_eq!(summary.schemas, direct.schemas.len());
    assert!(summary.schemas >= 2, "registered schemas must be exported");
    assert_eq!(summary.es, 1);
}
