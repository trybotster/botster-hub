//! Unix listener, socket path, and accept-loop admission.
use std::fs;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use botster_hub_client::DaemonTransportError as ClientDaemonTransportError;
use botster_hub_client::{
    DaemonDiagnostic, DaemonEndpoint, DaemonHello, DaemonHelloAck, PROTOCOL, read_frame,
    write_frame,
};
use tokio::io::BufReader as AsyncBufReader;
use tokio::net::{UnixListener as TokioUnixListener, UnixStream as TokioUnixStream};
use tokio::sync::{Semaphore, mpsc as tokio_mpsc, watch};

use crate::HubConfig;
use crate::admission::budgets::{DAEMON_HANDSHAKE_TIMEOUT, DAEMON_MAX_REJECTION_TASKS};
use crate::admission::unix_hello::daemon_hello_ack;
use crate::daemon::error::{DaemonTransportError, DaemonTransportResult};
use crate::daemon_transport::ControlMessage;
use crate::transport::unix::mux_write::{read_async_frame, write_async_frame};

pub(crate) static NEXT_SOCKET_CLIENT_ID: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

pub(crate) async fn accept_connections(
    listener: TokioUnixListener,
    control_tx: tokio_mpsc::Sender<ControlMessage>,
    mut shutdown_rx: watch::Receiver<bool>,
    admission: Arc<Semaphore>,
) {
    let rejection_admission = Arc::new(Semaphore::new(DAEMON_MAX_REJECTION_TASKS));
    let mut rejection_tasks = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        match admission.clone().try_acquire_owned() {
                            Ok(admission_permit) => {
                                if control_tx
                                    .send(ControlMessage::AcceptedConnection {
                                        stream,
                                        admission_permit,
                                    })
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            Err(_) => {
                                let permit = tokio::select! {
                                    permit = rejection_admission.clone().acquire_owned() => {
                                        permit.expect("rejection semaphore remains owned by accept loop")
                                    }
                                    changed = shutdown_rx.changed() => {
                                        let _ = changed;
                                        return;
                                    }
                                };
                                let rejection_tx = control_tx.clone();
                                rejection_tasks.spawn(async move {
                                    let _permit = permit;
                                    reject_connection_async(stream).await;
                                    let _ = rejection_tx
                                        .send(ControlMessage::RejectedConnection)
                                        .await;
                                });
                            }
                        }
                    }
                    Err(error) => {
                        eprintln!("botster-hub daemon accept error: {error}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            changed = shutdown_rx.changed() => {
                let _ = changed;
                return;
            }
            result = rejection_tasks.join_next(), if !rejection_tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    eprintln!("botster-hub daemon rejection task error: {error}");
                }
            }
        }
    }
}

pub(crate) async fn reject_connection_async(stream: TokioUnixStream) {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = AsyncBufReader::new(read_half);
    if read_async_frame::<DaemonHello, _>(&mut reader, Some(DAEMON_HANDSHAKE_TIMEOUT))
        .await
        .is_err()
    {
        return;
    }
    let _ = write_async_frame(
        &mut write_half,
        &daemon_hello_ack(vec![DaemonDiagnostic::backpressure(
            "daemon_connection_admission",
            "daemon connection capacity reached",
        )]),
    )
    .await;
}

pub(crate) fn socket_path(config: &HubConfig) -> DaemonTransportResult<PathBuf> {
    config
        .transports
        .local_socket
        .as_ref()
        .map(|binding| binding.path.clone())
        .ok_or(DaemonTransportError::MissingSocketBinding)
}

pub(crate) fn daemon_endpoint(config: &HubConfig) -> DaemonTransportResult<DaemonEndpoint> {
    socket_path(config).map(DaemonEndpoint::new)
}

pub(crate) fn prepare_socket_path(path: &PathBuf) -> DaemonTransportResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(DaemonTransportError::Io)?;
    }
    match UnixStream::connect(path) {
        Ok(mut stream) => {
            let hello = write_frame(
                &mut stream,
                &DaemonHello {
                    protocol: PROTOCOL.to_string(),
                    compatibility: botster_hub_client::DaemonCompatibilityRequirement::current(),
                    terminal_compatibility: None,
                },
            );
            match hello {
                Ok(()) => {
                    let ack = read_frame::<DaemonHelloAck>(&mut stream);
                    if ack.is_ok() {
                        return Err(DaemonTransportError::AlreadyRunning);
                    }
                }
                Err(ClientDaemonTransportError::ClientDisconnected) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {}
    }
    if path.exists() {
        fs::remove_file(path).map_err(DaemonTransportError::Io)?;
    }
    Ok(())
}

pub(crate) fn rebind_missing_socket_path(_path: &PathBuf) {
    // The current std-only listener cannot recreate the public pathname without
    // replacing the accept loop. Keep the daemon alive; clients report
    // not-running until a future listener-rebind pass repairs the path.
}

pub(crate) fn cleanup_socket_path(path: &PathBuf) {
    let _ = fs::remove_file(path);
}
