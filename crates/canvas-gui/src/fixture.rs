//! The captured demo export, embedded verbatim.
//!
//! This is the real stdout of a two-shell run (`cargo run --example
//! state_report` in shell 1, `cargo run -p canvas` in shell 2, summary
//! lines stripped) — not a hand-written abstraction — so model-test
//! counts agree with what the CLI prints (7 schemas, 6 actors, 12
//! declared edges, 1 pool, 1 partition, 1 rule, 0 observed edges).
//!
//! Test-only: the GUI binary never parses the fixture.

/// The captured export document.
#[cfg(test)]
const FIXTURE_JSON: &str = r#"
{
  "schemas": [
    {
      "name": "Added",
      "version": 1,
      "kind": "event",
      "fields": [
        {
          "name": "n",
          "ty": "int"
        }
      ]
    },
    {
      "name": "Fact",
      "version": 1,
      "kind": "event",
      "fields": [
        {
          "name": "kind",
          "ty": "str"
        },
        {
          "name": "offset",
          "ty": "int"
        },
        {
          "name": "ts",
          "ty": "int"
        }
      ]
    },
    {
      "name": "KeyedAdd",
      "version": 1,
      "kind": "command",
      "fields": [
        {
          "name": "account",
          "ty": "str",
          "role": "ShardKey"
        },
        {
          "name": "n",
          "ty": "int"
        }
      ]
    },
    {
      "name": "ReportState",
      "version": 1,
      "kind": "command",
      "fields": [
        {
          "name": "export",
          "ty": "json",
          "description": "the captured SystemExport document"
        }
      ],
      "description": "record the attached system export as a fact"
    },
    {
      "name": "StateReported",
      "version": 1,
      "kind": "event",
      "fields": [],
      "description": "the payload is the SystemExport document itself, not a field of it"
    },
    {
      "name": "Work",
      "version": 1,
      "kind": "command",
      "fields": [
        {
          "name": "n",
          "ty": "int"
        }
      ]
    },
    {
      "name": "WorkDone",
      "version": 1,
      "kind": "event",
      "fields": [
        {
          "name": "n",
          "ty": "int"
        }
      ]
    }
  ],
  "actors": [
    {
      "path": "state/reporter",
      "kind": "EventSourced",
      "manifest": {
        "handles": [
          "ReportState@1"
        ],
        "emits": [
          "StateReported@1"
        ],
        "emits_on_topics": [],
        "subscribes": [],
        "kind": "EventSourced"
      },
      "state": {
        "export": null,
        "seq": 0
      },
      "cursor": 0
    },
    {
      "path": "accounts/acme",
      "kind": "EventSourced",
      "manifest": {
        "handles": [
          "KeyedAdd@1"
        ],
        "emits": [
          "Added@1"
        ],
        "emits_on_topics": [],
        "subscribes": [],
        "kind": "EventSourced"
      },
      "state": {
        "balance": 15
      },
      "cursor": 2
    },
    {
      "path": "accounts/globex",
      "kind": "EventSourced",
      "manifest": {
        "handles": [
          "KeyedAdd@1"
        ],
        "emits": [
          "Added@1"
        ],
        "emits_on_topics": [],
        "subscribes": [],
        "kind": "EventSourced"
      },
      "state": {
        "balance": 20
      },
      "cursor": 1
    },
    {
      "path": "api/worker-0",
      "kind": "EventSourced",
      "manifest": {
        "handles": [
          "Work@1"
        ],
        "emits": [
          "WorkDone@1"
        ],
        "emits_on_topics": [],
        "subscribes": [],
        "kind": "EventSourced"
      },
      "state": {
        "total": 6
      },
      "cursor": 3
    },
    {
      "path": "watchdog",
      "kind": "Service",
      "manifest": {
        "handles": [
          "Fact@1"
        ],
        "emits": [],
        "emits_on_topics": [],
        "subscribes": [],
        "kind": "Service"
      },
      "state": null,
      "cursor": 186909
    },
    {
      "path": "api/worker-1",
      "kind": "EventSourced",
      "manifest": {
        "handles": [
          "Work@1"
        ],
        "emits": [
          "WorkDone@1"
        ],
        "emits_on_topics": [],
        "subscribes": [],
        "kind": "EventSourced"
      },
      "state": {
        "total": 9
      },
      "cursor": 3
    }
  ],
  "declared_edges": [
    {
      "actor": "state/reporter",
      "schema": "ReportState@1",
      "direction": "Handles",
      "topic": null
    },
    {
      "actor": "state/reporter",
      "schema": "StateReported@1",
      "direction": "Emits",
      "topic": null
    },
    {
      "actor": "accounts/acme",
      "schema": "KeyedAdd@1",
      "direction": "Handles",
      "topic": null
    },
    {
      "actor": "accounts/acme",
      "schema": "Added@1",
      "direction": "Emits",
      "topic": null
    },
    {
      "actor": "accounts/globex",
      "schema": "KeyedAdd@1",
      "direction": "Handles",
      "topic": null
    },
    {
      "actor": "accounts/globex",
      "schema": "Added@1",
      "direction": "Emits",
      "topic": null
    },
    {
      "actor": "api/worker-0",
      "schema": "Work@1",
      "direction": "Handles",
      "topic": null
    },
    {
      "actor": "api/worker-0",
      "schema": "WorkDone@1",
      "direction": "Emits",
      "topic": null
    },
    {
      "actor": "watchdog",
      "schema": "Fact@1",
      "direction": "Handles",
      "topic": null
    },
    {
      "actor": "api/worker-1",
      "schema": "Work@1",
      "direction": "Handles",
      "topic": null
    },
    {
      "actor": "api/worker-1",
      "schema": "WorkDone@1",
      "direction": "Emits",
      "topic": null
    },
    {
      "actor": "watchdog",
      "schema": "Any@1",
      "direction": "Subscribes",
      "topic": "system.facts"
    }
  ],
  "observed_edges": [],
  "pools": [
    {
      "path": "api",
      "algo": "round-robin",
      "workers": [
        "api/worker-0",
        "api/worker-1"
      ],
      "spec_parent": "api-parent"
    }
  ],
  "partitions": [
    {
      "path": "accounts",
      "key_field": "account",
      "entities": [
        "accounts/acme",
        "accounts/globex"
      ]
    }
  ],
  "rules": [
    {
      "source": null,
      "schema": "Work@1",
      "dest": "api",
      "action": "tee",
      "observer": "watchdog"
    }
  ]
}
"#;

/// Parses the embedded capture into a `SystemExport`.
///
/// # Panics
///
/// Panics if the embedded document stops deserializing — which would
/// mean the fixture no longer matches `SystemExport`'s serde shape and
/// every model test should fail loudly.
#[cfg(test)]
#[must_use]
pub fn fixture() -> actor_runtime::system::SystemExport {
    serde_json::from_str(FIXTURE_JSON).expect("embedded fixture deserializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_parses_with_demo_counts() {
        // Given the embedded capture from the real two-shell run.
        let export = fixture();

        // When reading the section sizes.
        // Then they match what the CLI printed for that run.
        assert_eq!(export.schemas.len(), 7);
        assert_eq!(export.actors.len(), 6);
        assert_eq!(export.declared_edges.len(), 12);
        assert_eq!(export.observed_edges.len(), 0);
        assert_eq!(export.pools.len(), 1);
        assert_eq!(export.partitions.len(), 1);
        assert_eq!(export.rules.len(), 1);
    }
}
