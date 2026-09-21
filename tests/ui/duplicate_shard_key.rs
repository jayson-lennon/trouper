use trouper::prelude::*;

// At most one field may carry #[schema(shard_key)].
#[derive(Command, serde::Serialize, serde::Deserialize)]
struct Move {
    #[schema(shard_key)]
    from: String,
    #[schema(shard_key)]
    to: String,
}

fn main() {}
