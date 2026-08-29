use std::sync::Arc;
use std::time::Duration;

use botster_core::AesGcmKey;
use botster_hub_client::{DaemonDiagnostic, DaemonLocalWebrtcAnswer, DaemonLocalWebrtcBootstrap};
use serde_json::Value;
use webrtc::peer_connection::{PeerConnection, PeerConnectionBuilder, RTCSessionDescription};
use webrtc::runtime::{channel, default_runtime, timeout};

use crate::admission::grants::LocalWebrtcSignalRequest;
use crate::daemon::control::message::ControlSender;
use crate::transport::webrtc::peer::{
    LocalWebrtcHandler, LocalWebrtcPeerState, LocalWebrtcTransport,
};
use crate::transport::webrtc::{LocalWebrtcError, LocalWebrtcResult};
pub(crate) const WEBRTC_SIGNAL_OPERATION: &str = "local_webrtc_signal";
impl LocalWebrtcTransport {
    /// Mint a local, single-use bootstrap grant bound to an already-running app origin.
    pub fn issue_bootstrap(
        &mut self,
        package_name: &str,
        entrypoint_id: &str,
        expected_origin: &str,
    ) -> Result<DaemonLocalWebrtcBootstrap, LocalWebrtcError> {
        self.grants
            .issue_bootstrap(package_name, entrypoint_id, expected_origin)
            .map_err(LocalWebrtcError::from)
    }

    /// Redeem one grant and create a WebRTC answer for the supplied offer.
    pub(crate) fn signal(
        &mut self,
        request: LocalWebrtcSignalRequest,
        runtime_tx: ControlSender,
    ) -> LocalWebrtcResult<DaemonLocalWebrtcAnswer> {
        let accepted = self
            .grants
            .redeem(&request)
            .map_err(LocalWebrtcError::from)?;
        let grant_id = accepted.grant_id.clone();

        let event_plane = self.event_plane.0.clone();
        let answer = self.runtime()?.block_on(answer_offer(
            grant_id.clone(),
            accepted.stream_key,
            request.offer,
            runtime_tx,
            event_plane,
        ))?;
        self.peers.insert(grant_id.clone(), answer.peer);
        self.peer_states.insert(grant_id.clone(), answer.peer_state);
        #[cfg(test)]
        self.peer_handlers.insert(grant_id.clone(), answer.handler);
        Ok(DaemonLocalWebrtcAnswer {
            grant_id,
            answer: answer.answer,
            diagnostics: vec![DaemonDiagnostic::connected(WEBRTC_SIGNAL_OPERATION)],
        })
    }
}
pub(crate) struct LocalWebrtcAnswer {
    pub(crate) answer: Value,
    pub(crate) peer: Arc<dyn PeerConnection>,
    pub(crate) peer_state: Arc<LocalWebrtcPeerState>,
    #[cfg(test)]
    pub(crate) handler: Arc<LocalWebrtcHandler>,
}
pub(crate) async fn answer_offer(
    grant_id: String,
    stream_key: AesGcmKey,
    offer: Value,
    runtime_tx: ControlSender,
    event_plane: Arc<crate::subscription::package_events::ClientEventPlane>,
) -> LocalWebrtcResult<LocalWebrtcAnswer> {
    let runtime = default_runtime()
        .ok_or_else(|| LocalWebrtcError::Webrtc("no async runtime".to_string()))?;
    let (gather_complete_tx, mut gather_complete_rx) = channel::<()>(1);
    let peer_state = Arc::new(LocalWebrtcPeerState::new_with_event_plane(
        grant_id.clone(),
        runtime_tx,
        event_plane,
    ));
    let handler = Arc::new(LocalWebrtcHandler {
        stream_key,
        runtime: runtime.clone(),
        peer_state: peer_state.clone(),
        gather_complete_tx,
    });

    let peer_connection = PeerConnectionBuilder::new()
        .with_handler(handler.clone())
        .with_runtime(runtime.clone())
        .with_udp_addrs(vec!["127.0.0.1:0"])
        .build()
        .await
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    let peer: Arc<dyn PeerConnection> = Arc::new(peer_connection);
    let offer = serde_json::from_value::<RTCSessionDescription>(offer)
        .map_err(|error| LocalWebrtcError::InvalidOffer(error.to_string()))?;
    peer.set_remote_description(offer)
        .await
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    let answer = peer
        .create_answer(None)
        .await
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    peer.set_local_description(answer)
        .await
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    let _ = timeout(
        runtime.as_ref(),
        Duration::from_secs(5),
        gather_complete_rx.recv(),
    )
    .await;
    let answer = peer
        .local_description()
        .await
        .ok_or_else(|| LocalWebrtcError::Webrtc("missing local description".to_string()))?;
    let answer = serde_json::to_value(answer)
        .map_err(|error| LocalWebrtcError::Webrtc(error.to_string()))?;
    Ok(LocalWebrtcAnswer {
        answer,
        peer,
        peer_state,
        #[cfg(test)]
        handler,
    })
}
pub(crate) fn random_token(prefix: &str) -> LocalWebrtcResult<String> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| LocalWebrtcError::Random(error.to_string()))?;
    Ok(format!("{prefix}-{}", hex(&bytes)))
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    pub(crate) const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}
