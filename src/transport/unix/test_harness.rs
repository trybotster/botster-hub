//! Unix connection test harness shared by owner-loop and transport tests.
//! Moved verbatim from owner_loop.rs (helpers and control receivers).

use std::os::unix::net::UnixStream;
use std::time::Duration;

use botster_hub_client::{
    ClientFrame, DaemonCompatibilityRequirement, DaemonHello, DaemonHelloAck, DaemonRequest,
    DaemonResponse, DaemonUnixFrameReader, DaemonUnixMuxFrame, DaemonUnixTerminalFrame, PROTOCOL,
    ServerFrame, write_client_frame,
};
use tokio::sync::mpsc as tokio_mpsc;

use crate::daemon::control::ControlMessage;

pub(crate) fn receive_test_control_message(
    receiver: &mut tokio_mpsc::Receiver<ControlMessage>,
) -> ControlMessage {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build bounded test receive runtime");
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("timed out waiting for daemon control message")
            .expect("daemon control sender remains live")
    })
}

pub(crate) fn receive_test_control_request(
    receiver: &mut tokio_mpsc::Receiver<ControlMessage>,
) -> ControlMessage {
    loop {
        match receive_test_control_message(receiver) {
            ControlMessage::RegisterUnixAdmission { reply_tx, .. } => {
                let _ = reply_tx.send(());
            }
            ControlMessage::RegisterWebrtcAdmission { .. } => {}
            message => return message,
        }
    }
}

pub(crate) fn write_hello(client: &mut UnixStream) {
    write_client_frame(
        client,
        &ClientFrame::Hello {
            hello: DaemonHello {
                protocol: PROTOCOL.to_string(),
                compatibility: DaemonCompatibilityRequirement::current(),
                terminal_compatibility: None,
            },
        },
    )
    .expect("write daemon hello");
}

pub(crate) fn write_request(client: &mut UnixStream, request_id: u64, request: DaemonRequest) {
    write_client_frame(
        client,
        &ClientFrame::Request {
            request_id: request_id.to_string(),
            request,
        },
    )
    .expect("write client request");
}

pub(crate) fn read_hello_ack(
    client: &mut UnixStream,
    reader: &mut DaemonUnixFrameReader,
) -> DaemonHelloAck {
    match reader.read_frame(client).expect("read daemon hello ack") {
        DaemonUnixMuxFrame::Server(ServerFrame::HelloAck { ack }) => ack,
        other => panic!("expected hello ack, got {other:?}"),
    }
}

pub(crate) fn read_response(
    client: &mut UnixStream,
    reader: &mut DaemonUnixFrameReader,
    expected_request_id: u64,
) -> DaemonResponse {
    match reader.read_frame(client).expect("read daemon response") {
        DaemonUnixMuxFrame::Server(ServerFrame::Response {
            request_id,
            response,
        }) => {
            assert_eq!(request_id, expected_request_id.to_string());
            response
        }
        other => panic!("expected correlated response, got {other:?}"),
    }
}

pub(crate) fn read_terminal(
    client: &mut UnixStream,
    reader: &mut DaemonUnixFrameReader,
) -> DaemonUnixTerminalFrame {
    match reader.read_frame(client).expect("read terminal frame") {
        DaemonUnixMuxFrame::Terminal(frame) => frame,
        other => panic!("expected terminal container, got {other:?}"),
    }
}
