//! System state reporting: a journaled record of what a running system
//! looked like.
//!
//! [`ReportState`] carries an already-captured [`SystemExport`] document
//! (built by an async caller via [`ActorSystem::export`]); the [`StateReporter`]
//! actor emits it verbatim as a [`StateReported`] event. Because event-sourced
//! handlers are pure and synchronous, the capture itself happens outside the
//! actor — the command payload is the seam.
//!
//! The reporter's state (`seq`, last export) is inspectable via
//! [`ActorSystem::es_state`], which is how a bridge detects that a fresh
//! report has landed.

use crate::actor::{CommandHandler, EventSourcedActor};
use crate::context::CmdCtx;
use crate::envelope::Event;
use crate::schema::{ActorManifest, FieldDef, FieldTy, Schema, SchemaDef, SchemaKind};
use crate::types::ActorKind;
use serde::Deserialize;
use serde_json::Value;

/// Ask the reporter to record the attached export document.
///
/// The payload embeds the whole [`crate::system::SystemExport`] as JSON:
/// export is async and locks the system, so it cannot happen inside an
/// event-sourced handler — the async caller captures, then hands the
/// document over.
#[derive(Deserialize)]
pub struct ReportState {
    /// The captured `SystemExport` document.
    pub export: Value,
}

impl Schema for ReportState {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "ReportState".into(),
            version: 1,
            kind: SchemaKind::Command,
            fields: vec![{
                let mut export = FieldDef::required("export", FieldTy::Json);
                export.description = Some("the captured SystemExport document".into());
                export
            }],
            description: Some("record the attached system export as a fact".into()),
        }
    }
}

/// A system export was recorded. The event payload IS the export document —
/// a verbatim, replayable record of what the system looked like.
#[derive(Deserialize)]
pub struct StateReported;

impl Schema for StateReported {
    fn schema_def() -> SchemaDef {
        SchemaDef {
            name: "StateReported".into(),
            version: 1,
            kind: SchemaKind::Event,
            fields: vec![],
            description: Some(
                "the payload is the SystemExport document itself, not a field of it".into(),
            ),
        }
    }
}

/// Event-sourced recorder of system exports.
///
/// State is deliberately tiny: how many reports landed (`seq`) and the last
/// recorded export. `seq` is the freshness signal — a caller reads it before
/// injecting a [`ReportState`], then waits for it to advance, so a query
/// never returns a stale export.
#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct StateReporter {
    seq: u64,
    export: Option<Value>,
}

impl EventSourcedActor for StateReporter {
    fn manifest() -> ActorManifest {
        ActorManifest::new()
            .handles::<ReportState>()
            .emits::<StateReported>()
            .kind(ActorKind::EventSourced)
    }

    fn restore(_args: &Value) -> Self {
        Self::default()
    }

    fn apply(&mut self, event: &Event) {
        if event.schema == StateReported::schema_id() {
            self.seq += 1;
            self.export = Some(event.payload.clone());
        }
    }
}

impl CommandHandler<ReportState> for StateReporter {
    fn handle(&self, cmd: ReportState, _ctx: &mut CmdCtx<'_>) -> Vec<Event> {
        vec![Event::new(StateReported::schema_id(), cmd.export)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{CtxCore, Outbox, RuntimeView};
    use crate::envelope::TraceCtx;
    use crate::types::{ActorPath, SchemaId};
    use serde_json::json;

    fn test_export(actors: usize) -> Value {
        json!({ "actors": [{ "path": format!("actor-{actors}") }] })
    }

    fn noop_ctx() -> CmdCtx<'static> {
        struct NullView;
        impl RuntimeView for NullView {
            fn lookup(&self, _path: &ActorPath) -> Option<crate::registry::EndpointInfo> {
                None
            }
            fn who_handles(&self, _schema: &SchemaId) -> Vec<ActorPath> {
                Vec::new()
            }
            fn now(&self) -> crate::types::Timestamp {
                crate::types::Timestamp::from_millis(0)
            }
        }
        // Leak is bounded: a per-test fixture, never read after the call.
        let trace: &'static TraceCtx = Box::leak(Box::new(TraceCtx::root()));
        let path: &'static ActorPath = Box::leak(Box::new(ActorPath::new("reporter")));
        let outbox: &'static mut Outbox = Box::leak(Box::new(Outbox::new()));
        let view: &'static NullView = Box::leak(Box::new(NullView));
        CmdCtx(CtxCore {
            self_path: path,
            trace,
            reply_to: None,
            view,
            outbox,
        })
    }

    #[test]
    fn handle_echoes_export_as_one_state_reported_event() {
        // Given a fresh reporter and a captured export document.
        let reporter = StateReporter::default();
        let export = test_export(3);

        // When handling ReportState.
        let events = reporter.handle(ReportState { export }, &mut noop_ctx());

        // Then exactly one StateReported event is declared and its payload
        // IS the export document (verbatim, not wrapped).
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].schema, StateReported::schema_id());
        assert_eq!(events[0].payload, test_export(3));
    }

    #[test]
    fn replay_of_state_reported_events_restores_seq_and_last_export() {
        // Given an empty reporter.
        let mut reporter = StateReporter::default();

        // When replaying the journaled events (same apply path as live).
        let first = test_export(1);
        let second = test_export(2);
        for payload in [&first, &second] {
            reporter.apply(&Event::new(StateReported::schema_id(), payload.clone()));
        }

        // Then seq counts every applied report and export is the LAST one.
        assert_eq!(reporter.seq, 2);
        assert_eq!(reporter.export, Some(second));
    }

    #[test]
    fn apply_ignores_foreign_event_schemas() {
        // Given a reporter with one recorded report.
        let mut reporter = StateReporter::default();
        reporter.apply(&Event::new(StateReported::schema_id(), test_export(1)));

        // When a foreign-schema event is applied.
        let other = SchemaId::new("Foreign", 1);
        reporter.apply(&Event::new(other, json!({ "x": 1 })));

        // Then seq and export are untouched.
        assert_eq!(reporter.seq, 1);
        assert_eq!(reporter.export, Some(test_export(1)));
    }

    #[tokio::test]
    async fn reported_export_lands_in_es_state_and_journal() {
        // Given a mini system with a spawned reporter.
        let (system, _clock) = crate::system::ActorSystem::test();
        let path = ActorPath::new("state/reporter");
        system.register_schema::<ReportState>();
        system.register_schema::<StateReported>();
        crate::builder::spawn_es_builder::<StateReporter>(&system)
            .at(path.clone())
            .args(json!({}))
            .handles::<ReportState>()
            .emits::<StateReported>()
            .start();

        // When the bridge-style flow runs: capture export, inject
        // ReportState, wait for the inbox to drain.
        let export = serde_json::to_value(system.export().await).expect("export");
        system
            .send(system.envelope(
                ReportState::schema_id(),
                path.clone(),
                json!({ "export": export }),
            ))
            .await
            .expect("delivered");
        for _ in 0..2_000 {
            if system.inbox_cursor(&path).map(|c| c.as_u64()) == Some(1) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        // Then the reporter's folded state holds the export, and the
        // journal records the StateReported decision.
        let state = system.es_state(&path).await.expect("live state");
        assert_eq!(state["seq"], 1);
        assert_eq!(state["export"], export);
        let schemas = system.journal_schemas(&path);
        assert_eq!(schemas, vec![StateReported::schema_id()]);
    }
}
