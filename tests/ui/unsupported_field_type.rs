use trouper::prelude::*;

// An exotic field type the mapping table does not know; the error must
// name the field and point at the #[schema(ty = "json")] escape hatch.
#[derive(Event, serde::Serialize, serde::Deserialize)]
struct Receipt {
    total: std::collections::HashMap<String, i64>,
}

fn main() {}
