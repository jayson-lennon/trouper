use trouper::prelude::*;

// Derives on non-structs are rejected: schemas are named-field structs.
#[derive(Event, serde::Serialize, serde::Deserialize)]
enum Kind {
    A,
    B,
}

fn main() {}
