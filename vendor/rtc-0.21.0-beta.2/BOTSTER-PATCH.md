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

## Receive-side close barrier

The send-side repair above is unchanged. This second repair sits on the receiving peer connection.

`RTCPeerConnection::handle_read` drains every handler's `poll_read` into the public `data_read_outs` queue in one pass.
The datachannel handler's `OnClose` for that stream waits in its own event queue at the same time.
The `webrtc` wrapper driver polls events before reads on every iteration.
When the final payload and the close marker are processed in one intake, the driver sees `OnClose` while the accepted payload still waits in `data_read_outs`.
A consumer that stops at `OnClose` never reads that payload.

The barrier holds a channel's `OnClose` at the public queue boundary until that channel's accepted data has been read.

- `PipelineContext` keeps a per-channel pending count. The single enqueue site into `data_read_outs` increments it. Both public dequeue paths, `poll_data_read` and the data branches of `poll_read`, decrement it and remove the entry at zero.
- The public `poll_event` holds `OnDataChannel(OnClose(id))` while channel `id` has a pending count. Every other event, including a close for a drained channel, passes unchanged. One channel's backlog never blocks another channel's close.
- A held close becomes eligible when its channel's count reaches zero. `poll_event` releases eligible held closes first, in held order, and keeps the readiness flag set while another eligible close remains.
- The public `poll_timeout` keeps every handler deadline. While an eligible held close waits, it also reports the datachannel handler's last observed logical instant, so the driver wakes at once. While data stays undrained under back-pressure, no timer is added.
- Held state is bounded by live channels: at most one held close per channel. A second `OnClose` for a channel that already holds one is dropped. The datachannel handler already emits `OnClose` at most once per stream.
- `OnClosing` is not held. The vendored crate has no producer for it. Local close semantics are unchanged.
- Whole-connection `close()` is a separate teardown case. Queued events and accepted data stay pollable after `close()`, as before. `close()` moves every held close into the event queue in held order, ahead of the `Closed` state event, and clears the counts and the flag. Retained data stays readable through both read APIs. A close that reaches the public queue after teardown is never held again and schedules no wake. Explicit teardown can therefore expose closure before retained data. That is teardown behavior, not proof of graceful delivery.

Changed upstream files for this repair:

- `src/peer_connection/handler/mod.rs`: pending counts, held closes, the `poll_event` barrier, the `poll_timeout` wake, the `close()` flush, the single routing site `route_read_out`, and unit tests through the public API with events polled first: one channel, a back-pressured sibling, both `poll_read` data branches, no added deadline while data stays undrained, several eligible closes, duplicate close, and whole-connection teardown.
- `src/peer_connection/internal.rs`: initialize the barrier state.
- `tests/data_channel_backpressure_rtc2rtc.rs`: real-peer case in which the receiver polls events before reads and drains every datagram already at its socket in one intake.

This crate is excluded from the Hub workspace and is also patched by path, so Cargo cannot test it in place.
The Hub report records the disposable copy used to run its unit and real-peer tests.
The Hub strict gate lints Hub workspace members, not this crate; upstream files outside this repair fail `clippy -D warnings` under Rust 1.97.0 as published.
