//! End-to-end: the demo host's system behind a real canvas server on an
//! ephemeral port, consumed by the real client. Asserts the snapshot
//! summary shows the full demo topology — the counts a human compares
//! against the host's own printout in the manual two-shell run.

// The example is a thin wrapper over its testable builder — include it
// as a module so the E2E drives exactly the system humans run.
#[path = "../../../examples/demo_host.rs"]
#[allow(dead_code)]
mod demo_host;

use canvas::{connect_snapshot, SnapshotSummary};
use std::time::Duration;

#[tokio::test]
async fn demo_system_snapshot_shows_the_full_topology_over_real_tcp() {
    // Given the demo host's system behind a real canvas server on an
    // ephemeral loopback port.
    let system = demo_host::build_demo_system().await;
    let listener = canvas_server::server::bind_server("127.0.0.1:0".parse().expect("loopback"))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let system_for_loop = system.clone();
    tokio::spawn(async move {
        let _ = canvas_server::server::accept_loop(system_for_loop, listener).await;
    });

    // When a client fetches a snapshot over real TCP.
    let export = connect_snapshot(addr, Duration::from_secs(5))
        .await
        .expect("snapshot");
    let summary = SnapshotSummary::of(&export);

    // Then every demo topology section is present and populated.
    assert!(summary.pools >= 1, "demo host declares a pool");
    assert!(
        summary.partitions >= 1 && summary.entities >= 1,
        "demo host activates partition entities"
    );
    assert!(summary.es >= 1, "pool workers + entities are ES actors");
    assert!(summary.service >= 1, "watchdog is a service actor");
    assert!(summary.rules >= 1, "demo host installs a tee rule");
    assert!(summary.observed_edges >= 1, "warm-up traffic produced observed edges");
    assert!(summary.schemas >= 5, "all demo message types are registered");

    // And the counts match the direct export (no client-side drift).
    let direct = system.export().await;
    assert_eq!(summary, SnapshotSummary::of(&direct));
}
