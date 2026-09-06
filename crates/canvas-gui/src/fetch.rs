//! The fetch thread: on-demand `SystemExport` retrieval off the render
//! thread.
//!
//! Zenoh needs tokio, and zenoh PANICS under tokio's current-thread
//! scheduler ("Zenoh runtime doesn't support Tokio's current thread
//! scheduler") — proven by a real CLI run. The thread therefore builds
//! a multi-thread runtime with one worker. The GUI never blocks: it
//! sends [`FetchCommand::Refresh`] and drains [`ExportMsg`]s.

use actor_runtime::system::SystemExport;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::time::Instant;

/// A request from the GUI to the fetch thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchCommand {
    /// Fetch the current export once.
    Refresh,
}

/// One delivered fetch result.
#[derive(Debug, Clone)]
pub struct ExportMsg {
    /// The export, or why the fetch failed (string-mapped at the
    /// thread boundary so the GUI has no zenoh error types).
    pub export: Result<SystemExport, String>,
    /// When the fetch completed (drives the legend's age display).
    pub at: Instant,
}

/// Spawns the background fetch thread (multi-thread tokio runtime,
/// one worker — see the module docs for why).
pub fn spawn_fetch_thread(
    commands: Receiver<FetchCommand>,
    results: Sender<ExportMsg>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let runtime = {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("tokio runtime builds")
        };
        run_fetch_loop(commands, results, || {
            runtime
                .block_on(state_report::fetch())
                .map_err(|error| error.to_string())
        });
    })
}

/// The fetch loop: one fetch per command, results delivered in order;
/// exits when the command channel closes (GUI shutdown).
pub fn run_fetch_loop<F>(commands: Receiver<FetchCommand>, results: Sender<ExportMsg>, mut fetch: F)
where
    F: FnMut() -> Result<SystemExport, String>,
{
    while let Ok(command) = commands.recv() {
        match command {
            FetchCommand::Refresh => {
                let export = fetch();
                let _ = results.send(ExportMsg {
                    export,
                    at: Instant::now(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;

    fn empty_export() -> SystemExport {
        SystemExport {
            schemas: Vec::new(),
            actors: Vec::new(),
            declared_edges: Vec::new(),
            observed_edges: Vec::new(),
            pools: Vec::new(),
            partitions: Vec::new(),
            rules: Vec::new(),
        }
    }

    #[test]
    fn fetch_loop_delivers_requested_exports() {
        // Given a fetch closure that always succeeds.
        let (cmd_tx, cmd_rx) = channel();
        let (res_tx, res_rx) = channel();

        // When one refresh is requested and the command channel
        // closes (ending the loop).
        cmd_tx.send(FetchCommand::Refresh).expect("send");
        drop(cmd_tx);
        run_fetch_loop(cmd_rx, res_tx, || Ok(empty_export()));

        // Then exactly one export arrives.
        let msg = res_rx.recv().expect("result");
        assert!(msg.export.is_ok());
    }

    #[test]
    fn fetch_loop_reports_fetch_errors() {
        // Given a fetch closure that always fails.
        let (cmd_tx, cmd_rx) = channel();
        let (res_tx, res_rx) = channel();

        // When a refresh is requested.
        cmd_tx.send(FetchCommand::Refresh).expect("send");
        drop(cmd_tx);
        run_fetch_loop(cmd_rx, res_tx, || Err("zenoh down".into()));

        // Then the failure is delivered as a legible string.
        let msg = res_rx.recv().expect("result");
        assert_eq!(msg.export.unwrap_err(), "zenoh down");
    }
}
