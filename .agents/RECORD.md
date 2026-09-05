# Record

- ADD: "All actor communication is mediated by the runtime: actors never hold channels directly; every send is routed by path or topic through the registry and emits a tap fact."
- ADD: "Event-sourced actors are pure decision functions (sync `handle(&self)` returning events) with a single `apply` used for both live state application and replay; all other actors may perform side effects and use `ask`."
- ADD: "The registry is kernel code, not an actor: path→endpoint slots, schema, type→handler, and topic→subscriber tables persist across actor restarts; actor identity is its registered path."
- ADD: "Message schemas are runtime data: Rust types and external JSON descriptors register into the same schema table; payloads cross the runtime boundary as JSON."
- ADD: "Event-sourced journals are in-memory, seq-anchored lists of `Event` and `Snapshot` entries; restart restores from the latest snapshot plus the tail, and command redelivery is independent of snapshots; journal persistence is deliberately out of scope."
- ADD: "External-process ports are planned as ordinary actors reusing the same schema/manifest tables (not yet implemented)."
