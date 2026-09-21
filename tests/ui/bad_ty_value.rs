use trouper::prelude::*;

// The only #[schema(ty)] override is "json".
#[derive(Event, serde::Serialize, serde::Deserialize)]
struct Odd {
    #[schema(ty = "int42")]
    n: i64,
}

fn main() {}
