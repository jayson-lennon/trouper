//! The `canvas` binary: query a running system's state over zenoh,
//! print a summary and the export document, exit 0 — or abort with a
//! legible stderr message and a non-zero exit before any GUI startup
//! path.
//!
//! Bare invocation fetches from whatever bridge answers
//! `actor-runtime/state` on the local network (zenoh peer discovery;
//! there are no addresses to configure). `-h`/`--help` prints usage.
//! Unknown flags are a usage error (exit 2); a failed fetch is an abort
//! (exit 1). A future GUI would only start after the fetch succeeds —
//! there is deliberately no GUI stub here.

use canvas::{SnapshotSummary, StateError, fetch_export};
use std::process::ExitCode;

fn main() -> ExitCode {
    match parse_args(std::env::args().skip(1)) {
        Ok(Args::Fetch) => match run() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("canvas: {error}");
                ExitCode::from(1)
            }
        },
        Ok(Args::Help) => {
            print_usage(&mut std::io::stdout());
            ExitCode::SUCCESS
        }
        Err(usage) => {
            eprintln!("canvas: {usage}");
            print_usage(&mut std::io::stderr().lock());
            ExitCode::from(2)
        }
    }
}

/// The parsed command line.
enum Args {
    /// Bare invocation: fetch and print.
    Fetch,
    /// `-h`/`--help`: print usage, exit 0.
    Help,
}

/// Parses the argument list. Only a bare invocation and `-h`/`--help`
/// exist; anything else is a usage error.
fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut result = Args::Fetch;
    for flag in args {
        match flag.as_str() {
            "--help" | "-h" => result = Args::Help,
            other => return Err(format!("unknown flag {other:?}")),
        }
    }
    Ok(result)
}

fn print_usage(out: &mut dyn std::io::Write) {
    let _ = writeln!(out, "usage: canvas [-h]");
    let _ = writeln!(out);
    let _ = writeln!(out, "Queries the state key over zenoh, prints the running");
    let _ = writeln!(out, "system's export summary and the full export document,");
    let _ = writeln!(out, "and exits 0. Discovery is automatic: any system on");
    let _ = writeln!(out, "this network with a bridge installed answers.");
    let _ = writeln!(out);
    let _ = writeln!(out, "Arguments:");
    let _ = writeln!(out, "  -h, --help  Print this help");
}

/// The success path: fetch the fresh export, print the summary and the
/// full JSON document. Runs inside a single-shot tokio runtime.
fn run() -> Result<(), StateError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .map_err(|e| StateError::Zenoh(format!("tokio runtime failed to start: {e}")))?;
    runtime.block_on(async move {
        let export = fetch_export().await?;
        println!("{}", SnapshotSummary::of(&export).render());
        println!(
            "{}",
            serde_json::to_string_pretty(&export).expect("serializable")
        );
        Ok(())
    })
}
