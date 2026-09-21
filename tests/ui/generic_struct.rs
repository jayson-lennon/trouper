use trouper::prelude::*;

// Schemas are concrete wire types; generics are rejected.
#[derive(Event, serde::Serialize, serde::Deserialize)]
struct Boxed<T> {
    value: T,
}

fn main() {}
