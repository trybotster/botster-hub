//! The loopback listener, its port file, and the accept loop.
//!
//! Sessions outlive the daemon and keep `BOTSTER_MCP_URL` in their
//! environment, so the port is stable per data directory: the first start
//! takes an OS-chosen port and records it in `<data_dir>/mcp-http.endpoint`;
//! every later start binds the recorded port. When that port is taken, the
//! Hub does not fail to start (a foreign process must not take it down): it
//! binds a new port, rewrites the file, and reports `port_changed`.

use std::fs;
use std::io::{self, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};

use crate::admission::budgets::DAEMON_MAX_CONNECTIONS;
use crate::daemon::control::message::ControlSender;
use crate::transport::http_mcp::connection::serve_connection;

/// File in the data directory that records the listener's port.
pub(crate) const ENDPOINT_FILE: &str = "mcp-http.endpoint";

/// Where the listener is, and whether it moved from the recorded port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct McpHttpEndpoint {
    pub(crate) port: u16,
    /// True when a recorded port was taken and a new one was chosen.
    pub(crate) port_changed: bool,
}

impl McpHttpEndpoint {
    /// The URL a session's `BOTSTER_MCP_URL` carries.
    pub(crate) fn url(self) -> String {
        format!("http://127.0.0.1:{}/mcp", self.port)
    }
}

/// A bound listener, not yet accepting.
pub(crate) struct BoundMcpHttp {
    listener: StdTcpListener,
    pub(crate) endpoint: McpHttpEndpoint,
}

fn endpoint_path(data_directory: &Path) -> PathBuf {
    data_directory.join(ENDPOINT_FILE)
}

fn loopback(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

/// Bind the listener. `configured` pins a port: if that port cannot be
/// bound, this fails, because the operator asked for exactly that port.
pub(crate) fn bind(data_directory: &Path, configured: Option<u16>) -> io::Result<BoundMcpHttp> {
    fs::create_dir_all(data_directory)?;
    let path = endpoint_path(data_directory);
    let recorded = read_recorded_port(&path);
    let (listener, port_changed) = match (configured, recorded) {
        (Some(port), _) => (StdTcpListener::bind(loopback(port))?, false),
        (None, Some(port)) => match StdTcpListener::bind(loopback(port)) {
            Ok(listener) => (listener, false),
            Err(_) => (StdTcpListener::bind(loopback(0))?, true),
        },
        (None, None) => (StdTcpListener::bind(loopback(0))?, false),
    };
    listener.set_nonblocking(true)?;
    let port = listener.local_addr()?.port();
    if recorded != Some(port) {
        write_recorded_port(&path, port)?;
    }
    Ok(BoundMcpHttp {
        listener,
        endpoint: McpHttpEndpoint { port, port_changed },
    })
}

fn read_recorded_port(path: &Path) -> Option<u16> {
    fs::read_to_string(path).ok()?.trim().parse::<u16>().ok()
}

/// Record the port atomically: a temporary file, then a rename over the
/// endpoint file, so a crash never leaves half a port.
fn write_recorded_port(path: &Path, port: u16) -> io::Result<()> {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&temporary)?;
    writeln!(file, "{port}")?;
    file.sync_all()?;
    fs::rename(&temporary, path)
}

impl BoundMcpHttp {
    /// Accept until shutdown. Must run inside the transport runtime.
    pub(crate) async fn serve(
        self,
        control_tx: ControlSender,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let port = self.endpoint.port;
        let Ok(listener) = TcpListener::from_std(self.listener) else {
            eprintln!("botster-hub mcp http listener could not register with the runtime");
            return;
        };
        // A pool of its own: the Unix listener's connections are not charged
        // to HTTP peers, nor the other way round.
        let admission = Arc::new(Semaphore::new(DAEMON_MAX_CONNECTIONS));
        loop {
            tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => {
                        // Over the cap the connection is dropped at once.
                        let Ok(permit) = admission.clone().try_acquire_owned() else {
                            continue;
                        };
                        let control_tx = control_tx.clone();
                        let shutdown_rx = shutdown_rx.clone();
                        tokio::spawn(async move {
                            serve_connection(stream, port, control_tx, shutdown_rx).await;
                            drop(permit);
                        });
                    }
                    Err(error) => {
                        eprintln!("botster-hub mcp http accept error: {error}");
                        // timer: backoff — accept failed (for example EMFILE); retry after a pause.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                },
                _ = shutdown_rx.changed() => return,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data_directory(name: &str) -> PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "mcp-http-{name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).unwrap();
        directory
    }

    #[test]
    fn the_first_start_records_its_port_and_a_restart_reuses_it() {
        let directory = data_directory("reuse");
        let first = bind(&directory, None).unwrap();
        let port = first.endpoint.port;
        assert!(!first.endpoint.port_changed);
        assert_eq!(read_recorded_port(&endpoint_path(&directory)), Some(port));
        drop(first);
        let second = bind(&directory, None).unwrap();
        assert_eq!(second.endpoint.port, port);
        assert!(!second.endpoint.port_changed);
        assert_eq!(second.endpoint.url(), format!("http://127.0.0.1:{port}/mcp"));
    }

    #[test]
    fn a_taken_port_falls_back_and_says_so() {
        let directory = data_directory("taken");
        let held = bind(&directory, None).unwrap();
        let taken = held.endpoint.port;
        // The recorded port is still held by another listener.
        let moved = bind(&directory, None).unwrap();
        assert_ne!(moved.endpoint.port, taken);
        assert!(moved.endpoint.port_changed);
        assert_eq!(
            read_recorded_port(&endpoint_path(&directory)),
            Some(moved.endpoint.port)
        );
    }

    #[test]
    fn a_configured_port_that_is_taken_is_an_error() {
        let directory = data_directory("configured");
        let held = bind(&directory, None).unwrap();
        assert!(bind(&directory, Some(held.endpoint.port)).is_err());
    }

    #[test]
    fn the_endpoint_file_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let directory = data_directory("private");
        let _bound = bind(&directory, None).unwrap();
        let mode = fs::metadata(endpoint_path(&directory))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn an_unreadable_record_is_replaced() {
        let directory = data_directory("garbage");
        fs::write(endpoint_path(&directory), "not a port").unwrap();
        let bound = bind(&directory, None).unwrap();
        assert!(!bound.endpoint.port_changed);
        assert_eq!(
            read_recorded_port(&endpoint_path(&directory)),
            Some(bound.endpoint.port)
        );
    }
}
