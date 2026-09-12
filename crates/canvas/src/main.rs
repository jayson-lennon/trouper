//! The `canvas` binary: query a running system's state over zenoh, print
//! a summary and the export document, exit 0 — or pass a control command
//! through to a running system's control bridge and print its result —
//! or abort with a legible stderr message and a non-zero exit.
//!
//! Bare invocation fetches from whatever bridge answers
//! `trouper/state` on the local network (zenoh peer discovery;
//! there are no addresses to configure). `-h`/`--help` prints usage.
//! `ctl` sends commands on `trouper/control`: bare `ctl` asks the
//! bridge what it can do, `ctl <name> [json]` runs a command and prints
//! the reply. Unknown flags or arguments are a usage error (exit 2); a
//! failed fetch or a failed command is an abort (exit 1). A future GUI
//! would only start after the fetch succeeds — there is deliberately no
//! GUI stub here.

use canvas::{SnapshotSummary, StateError, ctl_command, fetch_export};
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
        Ok(Args::Ctl { name, args }) => match run_ctl(name, args) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("canvas: control error: {error}");
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
#[derive(Debug)]
enum Args {
    /// Bare invocation: fetch and print.
    Fetch,
    /// `ctl [name] [json]`: send a control command (or ask for the
    /// command list when `name` is `None`).
    Ctl {
        /// The command name; `None` = the `List` pseudo-command.
        name: Option<String>,
        /// The command's argument document (defaults to `Null`).
        args: serde_json::Value,
    },
    /// `-h`/`--help`: print usage, exit 0.
    Help,
}

/// Parses the argument list: bare fetch, `-h`/`--help`, or
/// `ctl [name] [json]`; anything else is a usage error.
fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut iter = args.peekable();
    let Some(first) = iter.next() else {
        return Ok(Args::Fetch);
    };
    match first.as_str() {
        "--help" | "-h" => Ok(Args::Help),
        "ctl" => {
            let name = iter.next();
            parse_ctl(iter, name)
        }
        other => Err(format!("unknown flag {other:?}")),
    }
}

/// Finishes parsing a `ctl` invocation: an optional command name, then an
/// optional JSON argument document.
fn parse_ctl(
    mut iter: std::iter::Peekable<impl Iterator<Item = String>>,
    name: Option<String>,
) -> Result<Args, String> {
    // The name is already taken when given; only the args document can
    // remain. `Null` (the no-arguments document) when absent.
    let args = match iter.next() {
        None => serde_json::Value::Null,
        Some(raw) => serde_json::from_str(&raw)
            .map_err(|error| format!("ctl args are not valid JSON: {error}"))?,
    };
    if let Some(extra) = iter.next() {
        return Err(format!("unexpected extra argument {extra:?}"));
    }
    Ok(Args::Ctl { name, args })
}

fn print_usage(out: &mut dyn std::io::Write) {
    let _ = writeln!(
        out,
        "usage: canvas [-h] | canvas ctl [<command> [<args-json>]]"
    );
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "With no arguments, queries the state key over zenoh, prints the running"
    );
    let _ = writeln!(
        out,
        "system's export summary and the full export document, and exits 0."
    );
    let _ = writeln!(
        out,
        "Discovery is automatic: any system on this network with a bridge"
    );
    let _ = writeln!(out, "installed answers.");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "`canvas ctl` sends control commands on the control key to a running"
    );
    let _ = writeln!(out, "system's control bridge:");
    let _ = writeln!(
        out,
        "  canvas ctl                 list the commands the bridge serves"
    );
    let _ = writeln!(
        out,
        "  canvas ctl <command>       run a command with no arguments"
    );
    let _ = writeln!(
        out,
        "  canvas ctl <command> '<json>'   run it with an argument document"
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "Examples:");
    let _ = writeln!(
        out,
        "  canvas ctl ScalePool '{{\"kind\":\"saver\",\"workers\":3}}'"
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "Arguments:");
    let _ = writeln!(out, "  -h, --help  Print this help");
}

/// The fetch path: fetch the fresh export, print the summary and the full
/// JSON document. Runs inside a single-shot tokio runtime.
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

/// The control path: send the command, print the result document. Runs
/// inside a single-shot tokio runtime.
fn run_ctl(name: Option<String>, args: serde_json::Value) -> Result<(), canvas::ControlError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .map_err(|e| canvas::ControlError::Zenoh(format!("tokio runtime failed to start: {e}")))?;
    runtime.block_on(async move {
        let name = name.unwrap_or_else(|| "List".to_string());
        let result = ctl_command(name, args).await?;
        println!(
            "{}",
            serde_json::to_string_pretty(&result).expect("serializable")
        );
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Args, String> {
        parse_args(args.iter().map(|s| s.to_string()))
    }

    #[test]
    fn bare_invocation_parses_to_fetch() {
        // Given no arguments.
        // When parsing.
        let parsed = parse(&[]).expect("bare invocation is valid");
        // Then it is the fetch path.
        assert!(matches!(parsed, Args::Fetch));
    }

    #[test]
    fn help_flag_parses_to_help() {
        // Given the -h flag.
        // When parsing.
        let parsed = parse(&["-h"]).expect("-h is valid");
        // Then it is the help path.
        assert!(matches!(parsed, Args::Help));
    }

    #[test]
    fn bare_ctl_parses_to_the_list_pseudo_command() {
        // Given `ctl` with nothing after it.
        // When parsing.
        let parsed = parse(&["ctl"]).expect("bare ctl is valid");
        // Then it is a ctl with no name (List) and no args.
        let Args::Ctl { name, args } = parsed else {
            panic!("expected Args::Ctl, got {parsed:?}");
        };
        assert!(name.is_none(), "bare ctl names no command");
        assert_eq!(args, serde_json::Value::Null);
    }

    #[test]
    fn ctl_with_name_parses_with_null_args() {
        // Given `ctl` plus a command name.
        // When parsing.
        let parsed = parse(&["ctl", "ScalePool"]).expect("named ctl is valid");
        // Then the name is taken and args default to Null.
        let Args::Ctl { name, args } = parsed else {
            panic!("expected Args::Ctl, got {parsed:?}");
        };
        assert_eq!(name.as_deref(), Some("ScalePool"));
        assert_eq!(args, serde_json::Value::Null);
    }

    #[test]
    fn ctl_with_name_and_json_parses_both() {
        // Given `ctl` plus a name and a JSON argument document.
        // When parsing.
        let parsed = parse(&["ctl", "ScalePool", r#"{"kind":"saver","workers":3}"#])
            .expect("ctl with json args is valid");
        // Then both halves arrive intact.
        let Args::Ctl { name, args } = parsed else {
            panic!("expected Args::Ctl, got {parsed:?}");
        };
        assert_eq!(name.as_deref(), Some("ScalePool"));
        assert_eq!(args, serde_json::json!({ "kind": "saver", "workers": 3 }));
    }

    #[test]
    fn unknown_flag_is_a_usage_error() {
        // Given an unrecognized flag.
        // When parsing.
        let parsed = parse(&["--verbose"]);
        // Then it is a usage error naming the flag.
        let error = parsed.expect_err("unknown flag must be a usage error");
        assert!(error.contains("--verbose"));
    }

    #[test]
    fn ctl_with_invalid_json_args_is_a_usage_error() {
        // Given ctl args that are not JSON.
        // When parsing.
        let parsed = parse(&["ctl", "ScalePool", "{not json"]);
        // Then it is a usage error about the args.
        let error = parsed.expect_err("invalid JSON must be a usage error");
        assert!(error.contains("not valid JSON"), "got: {error}");
    }

    #[test]
    fn ctl_with_extra_arguments_is_a_usage_error() {
        // Given more tokens than name + args.
        // When parsing.
        let parsed = parse(&["ctl", "ScalePool", "{}", "extra"]);
        // Then it is a usage error naming the extra token.
        let error = parsed.expect_err("extra arguments must be a usage error");
        assert!(error.contains("extra"), "got: {error}");
    }
}
