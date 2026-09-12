//! End-to-end: the demo system behind a real zenoh bridge, consumed by
//! the real `fetch`. Asserts the export shows the full demo topology —
//! the counts a human compares against the example's own printout in the
//! manual two-shell run — and matches the direct export exactly.

// The example is a thin wrapper over its testable builder — include it
// as a module so the E2E drives exactly the system humans run.
#[path = "../../../examples/state_report.rs"]
#[allow(dead_code)]
mod demo;

use trouper::prelude::*;
use state_report::{StateKey, fetch_on, install_on};

#[tokio::test(flavor = "multi_thread")]
async fn demo_system_answers_queries_with_its_full_topology_over_zenoh() {
    // Given the demo system with its bridge installed (the same flow
    // `run_demo_system` performs, minus the ctrl-c wait).
    let system = demo::build_demo_system().await;
    let key = StateKey::scoped("e2e-demo-topology");
    let session = install_on(
        key.clone(),
        system.clone(),
        ActorPath::new("state/reporter"),
    )
    .await
    .expect("bridge installed");

    // When a client fetches the state over real zenoh, on that island.
    let export = fetch_on(key).await.expect("fetch");
    let actors = &export.actors;

    // Then every demo topology section is present and populated.
    assert!(
        export.pools.iter().any(|p| p.path.as_str() == "api"),
        "pool 'api' declared"
    );
    let entities = export
        .partitions
        .iter()
        .find(|p| p.path.as_str() == "accounts")
        .map(|p| p.entities.len())
        .unwrap_or(0);
    assert!(entities >= 2, "partition 'accounts' activated 2 entities");
    assert!(
        actors.iter().any(|a| a.path.as_str() == "watchdog"),
        "watchdog (service actor) is present"
    );
    assert!(
        actors.iter().any(|a| a.path.as_str() == "state/reporter"),
        "the state reporter is present"
    );
    assert!(!export.rules.is_empty(), "tee rule declared");
    assert!(
        export.schemas.len() >= 5,
        "all demo message types are registered"
    );

    // And the fetched document matches the direct export on everything
    // except the reporter itself (the reporter's ES state necessarily
    // differs: the report that produced this reply is journaled after the
    // capture it carries — the doc test-proves the seq snapshot design).
    let direct = system.export().await;
    let fetched = serde_json::to_value(&export).expect("fetched serializes");
    let direct = serde_json::to_value(&direct).expect("direct serializes");

    fn section<'a>(doc: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
        &doc[name]
    }
    for section_name in ["schemas", "declared_edges", "partitions", "pools", "rules"] {
        assert_eq!(
            section(&fetched, section_name),
            section(&direct, section_name),
            "{section_name} must match the direct export"
        );
    }
    // Actors match except for the reporter, compared separately.
    let actor = |doc: &serde_json::Value, path: &str| {
        doc["actors"]
            .as_array()
            .expect("actors array")
            .iter()
            .find(|a| a["path"].as_str() == Some(path))
            .unwrap_or_else(|| panic!("no actor at {path}"))
            .clone()
    };
    let reporter = "state/reporter";
    // Settled ES actors must match wholesale.
    for path in [
        "api/worker-0",
        "api/worker-1",
        "accounts/acme",
        "accounts/globex",
    ] {
        assert_eq!(
            actor(&fetched, path),
            actor(&direct, path),
            "actor {path} must match the direct export"
        );
    }
    // The watchdog's fact cursor is a moving gauge (reporting itself
    // produces facts), so only its identity can be compared.
    for doc_name in [("fetched", &fetched), ("direct", &direct)] {
        let watchdog = actor(doc_name.1, "watchdog");
        assert_eq!(
            watchdog["kind"], "Service",
            "watchdog kind ({})",
            doc_name.0
        );
        assert_eq!(
            watchdog["manifest"]["handles"],
            serde_json::json!(["Fact@1"]),
            "watchdog manifest ({})",
            doc_name.0
        );
    }
    // The reporter advanced between capture and now: seq went 0→1.
    assert_eq!(actor(&fetched, reporter)["state"]["seq"], 0);
    assert_eq!(actor(&direct, reporter)["state"]["seq"], 1);
    assert!(
        actor(&direct, reporter)["state"]["export"].is_object(),
        "the journaled report carries the captured export"
    );

    // Teardown: leave the mesh gracefully.
    session.close().await.expect("bridge session closed");
}
