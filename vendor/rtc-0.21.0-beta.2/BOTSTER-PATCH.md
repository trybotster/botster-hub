# Ordered data channel close

This directory contains the published `rtc 0.21.0-beta.2` crate with a local close-order repair.

- Upstream repository: `https://github.com/webrtc-rs/rtc`.
- Upstream revision from `.cargo_vcs_info.json`: `ae66413d5f6816fa9ec83cb4690234665b44b647`.
- Published crate checksum: `18b8cc79ca0599ef8b851f8b2b1ac30334e2f51a8a792e67829c6b29f533d15a`.
- Original published contents: 345 files, 10,718,918 file bytes in the local registry copy.
- The directory preserves `LICENSE-APACHE` and `LICENSE-MIT` without changes.
- The package version and public function signatures remain unchanged.

The published implementation can process a close marker before an accepted application payload.
Application payload enters the endpoint queue. The old close method writes directly to the lower channel queue.

The repair queues the existing internal close event through the endpoint queue.
The channel handler applies close after earlier payload from that queue.
The public close method marks the channel `Closing` immediately and rejects new sends.
Repeated close remains idempotent. Internal abort paths still close directly.
The repair uses the channel handler's logical time. It adds no sleep or Hub delivery wait.

Changed upstream files:

- `src/data_channel/mod.rs`: queue close and reject sends after close starts.
- `src/peer_connection/handler/datachannel.rs`: apply queued close and expose logical time within the crate.
- `src/peer_connection/handler/mod.rs`: test both queue schedules, repeated close, rejected late sends, and public close before open with a late ACK.
- `tests/data_channel_backpressure_rtc2rtc.rs`: test final payload delivery before remote close over a real peer connection, and accepted payload under the Hub high-water pressure signal before remote close.

Hub selects this directory through its workspace-root `[patch.crates-io]` entry.
Cargo does not inherit that patch into another workspace root.
The supported Hub runtime builds run from the Hub workspace root.
The release script changes to that root. The production package proof builds in its configured Hub checkout.
The shared-session proof consumes explicit Hub and worker binaries.
Hub client and test-support do not depend on the Hub runtime crate.
The reviewed TUI manifest and lockfile contain no `rtc` or `webrtc` dependency.
A consumer that embeds the Hub runtime from another workspace must select this patch explicitly.
Such a consumer is not validated by this patch's Hub-root checks.

The original failed queue-order check is preserved in pipeline artifact `artifact_1788588870_745432`.
Validation results belong in the Hub implementation report. Source preparation alone is not delivery proof.
