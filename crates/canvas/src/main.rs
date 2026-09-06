//! The `canvas` binary: connect to a live system, print a snapshot
//! summary, exit 0 — or abort with a legible stderr message and a
//! non-zero exit before any GUI startup path.
//!
//! Bare invocation connects to [`DEFAULT_ADDR`] (documented in the
//! usage text). `--connect <ip>:<port>` names a different system.
//! Unknown flags are a usage error (exit 2); a failed connection or
//! snapshot is an abort (exit 1). A future GUI would only start after
//! the connection succeeds — there is deliberately no GUI stub here.

use canvas::{connect_snapshot, CanvasError, SnapshotSummary};
use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

/// Where a bare `canvas` invocation looks for a running system.
pub const DEFAULT_ADDR: &str = "127.0.0.1:7667";

/// The whole connect+request+reply budget.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

fn main() -> ExitCode {
    match parse_args(std::env::args().skip(1)) {
        Ok(Args::Connect(addr)) => {
            let addr = addr.unwrap_or_else(|| DEFAULT_ADDR.parse().expect("valid default addr"));
            match run(addr) {
            Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("canvas: {error}");
                    ExitCode::from(1)
                }
            }
        }
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
    /// Observe this address (None = the documented default).
    Connect(Option<SocketAddr>),
    /// `--help`: print usage, exit 0.
    Help,
}

/// Parses the argument list. Only `--connect <addr>` and `--help` exist;
/// anything else (including `--connect` without a value) is a usage
/// error.
fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut connect = None;
    let mut items: std::collections::VecDeque<String> = args.collect();
    while let Some(flag) = items.pop_front() {
        match flag.as_str() {
            "--help" | "-h" => return Ok(Args::Help),
            "--connect" => {
                let value = items.pop_front().ok_or("--connect expects <ip>:<port>")?;
                let parsed = value
                    .parse::<SocketAddr>()
                    .map_err(|_| format!("--connect expects <ip>:<port>, got {value:?}"))?;
                connect = Some(parsed);
            }
            other => return Err(format!("unknown flag {other:?}")),
        }
    }
    Ok(Args::Connect(connect))
}

fn print_usage(out: &mut dyn std::io::Write) {
    let _ = writeln!(out, "usage: canvas [--connect <ip>:<port>]");
    let _ = writeln!(out);
    let _ = writeln!(out, "Connects to a running system's canvas server, prints a snapshot");
    let _ = writeln!(out, "summary and the full export document, and exits 0.");
    let _ = writeln!(out);
    let _ = writeln!(out, "Arguments:");
    let _ = writeln!(out, "  --connect <ip>:<port>  Address of the system to observe");
    let _ = writeln!(out, "                         (default: {DEFAULT_ADDR})");
    let _ = writeln!(out, "  -h, --help             Print this help");
}

/// The success path: fetch the snapshot, print the summary and the full
/// JSON document. Runs inside a single-shot tokio runtime.
fn run(addr: SocketAddr) -> Result<(), CanvasError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| CanvasError::Protocol {
            addr,
            code: "runtime".to_owned(),
            detail: format!("tokio runtime failed to start: {e}"),
        })?;
    runtime.block_on(async move {
        let export = connect_snapshot(addr, CONNECT_TIMEOUT).await?;
        print_summary(&export, addr);
        print!("{}", serde_json::to_string_pretty(&export).expect("serializable"));
        Ok(())
    })
}

/// Prints the one-glance summary: counts per export section. The full
/// JSON dump follows in the caller.
fn print_summary(
    export: &actor_runtime::system::SystemExport,
    addr: SocketAddr,
) {
    println!("connected to {addr}");
    println!("{}", SnapshotSummary::of(export).render());
}
