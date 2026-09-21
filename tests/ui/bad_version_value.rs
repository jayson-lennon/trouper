use trouper::prelude::*;

// `version` must be an integer literal.
#[derive(Event, serde::Serialize, serde::Deserialize)]
#[schema(version = "two")]
struct Fact {
    n: i64,
}

fn main() {}
