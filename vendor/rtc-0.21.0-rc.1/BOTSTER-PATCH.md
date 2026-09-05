# Botster local repairs on `rtc 0.21.0-rc.1`

This directory contains the published `rtc 0.21.0-rc.1` crate with two local repairs.

- Upstream repository: `https://github.com/webrtc-rs/rtc`.
- Upstream revision from `.cargo_vcs_info.json`: `51558ffb550bb17a540343b338e2cd4a764f3690`.
- Published crate checksum (`rtc-0.21.0-rc.1.crate`, sha256): `f1c97fa165233b7cab98318df83f7dc2fa867fca3feca07854f7defd6b83e5ef`.
- Original published contents: 365 files, 10,983,125 file bytes in the local registry copy.
- The directory preserves `LICENSE-APACHE` and `LICENSE-MIT` without changes.
- The package version and public function signatures remain unchanged.

The rest of the `rtc` family is consumed unmodified from crates.io at the same version.
`Cargo.lock` records these checksums for the family members this crate depends on:

| crate | sha256 |
| --- | --- |
| rtc-crypto | `bb09bbf5cc31a6f8aa7fc9ec1d29fc9ef451943032be2d7ff555eb53b715f0a7` |
| rtc-datachannel | `04e47846f9b966400f1dcfba90632207c6525c37d9b0689886c7ff3e26a21831` |
| rtc-dtls | `c5d24b04ea94f43e9954945acc1ec3d1ff3aa60c72c0b350321e131776318ac5` |
| rtc-ice | `83144d5a1e024abc5ecf1cb1f4722928bc88faa182ebaad8b401580c5ed6d5fc` |
| rtc-interceptor | `5542d3adc85fc54b3d4323fb6f6b7c45bfcf6205cb940e6377dcc9f3893a2187` |
| rtc-mdns | `80b653fc94391ae039494538592063ef06fa9dd62341d46dcd4c04ecf065e8b2` |
| rtc-media | `3ea4c517928271a5ec44abda29c96a5eceb76e6833c2faed5435c12a8d62d673` |
| rtc-rtcp | `c362f897ddf93eca1aeef832116c3bd47935a81810dd4e870b4bd364ee3d757d` |
| rtc-rtp | `9a318d08b1a61bd195acde83f248d059c6bbeedde3d69fcf46815f296c954ac9` |
| rtc-sctp | `8823005e23738c18e7ddf86ed15e4e539565ec07de1609017fab3422988a7587` |
| rtc-sdp | `e265cdb020ced9b2711c2e93b764a82486ea7240a7d6e160f7c3b60cbb305216` |
| rtc-shared | `e9cb3827cf45aaeacf955b1577ff3d13f55f24319cffe94efe7d6345035b564e` |
| rtc-srtp | `47333e36d1d2551d4f41fad31040ab9ac7ec6825bc76d3da489ca37225235322` |
| rtc-stun | `e48f8f1c5a025cddc50d7f042862ae264ca47ecb54b4124ab13f8b02b77a838d` |
| rtc-turn | `776aff367180ee29227f0db44a3c1ab2dfe69be90a207b93f5f107a78b2d46db` |
| webrtc | `58f5fb2584f39cea75e2918b513c4ebc7dadc9f543391e4a5a1919c58270e756` |

`rtc-sctp` and `rtc` were published from the same upstream revision `51558ffb550bb17a540343b338e2cd4a764f3690`.
`webrtc 0.21.0-rc.1` was published from `4b636c1221b9894c83ed4a8aeab5fe27c0e21352` in its own repository and requires `rtc 0.21.0-rc.1`.

## Repair 1: ordered data channel close (send side)

The published implementation can process a close marker before an accepted application payload.
Application payload enters the endpoint queue. The published close method writes directly to the lower channel queue.

The repair queues the existing internal close event through the endpoint queue.
The channel handler applies close after earlier payload from that queue.
The public close method marks the channel `Closing` immediately and rejects new sends.
Repeated close remains idempotent. Internal abort paths still close directly.
The repair uses the channel handler's logical time. It adds no sleep or Hub delivery wait.

This repair was first written against `rtc 0.21.0-beta.2`. Upstream `rc.1` does not contain it.
The same hunks apply to `rc.1` without change.

Changed upstream files:

- `src/data_channel/mod.rs`: queue close and reject sends after close starts.
- `src/peer_connection/handler/datachannel.rs`: apply queued close and expose logical time within the crate.
- `src/peer_connection/handler/mod.rs`: test both queue schedules, repeated close, rejected late sends, and public close before open with a late ACK.
- `tests/data_channel_backpressure_rtc2rtc.rs`: test final payload delivery before remote close over a real peer connection, and accepted payload under the Hub high-water pressure signal before remote close.

## Repair 2: terminal stream results inside the SCTP drain (receive side)

`rtc-sctp 0.21.0-rc.1` defers a peer's outgoing stream reset while the stream still holds a complete unread message.
It performs the deferred reset inside the `read_sctp()` call that drains the last message.
That read unregisters the stream. The next `read_sctp()` in the same drain loop reports `ErrStreamClosed`.

`SctpHandler::drain_stream` in the published crate propagates that error with `?`.
The error leaves `handle_read` or `resume_pending_reads` before either moves its collected batch into `read_outs`.
The accepted payload is lost. On the parked path the entry also stays parked, and every later `poll_read` retries and fails again.

The repair treats two results as normal terminal states of one stream:

- `Association::stream()` reports `ErrStreamNotExisted`: the parked entry names a stream the association no longer has. The drain forgets the entry and reads nothing.
- `read_sctp()` reports `ErrStreamClosed` after an earlier read in the same loop: the drain forgets the entry and returns the messages it already collected.

Every other error is returned unchanged.

The reset performed inside the drain queues `AssociationLost` on the association. `handle_read` polls that queue after its own drain, so on the immediate path `SCTPStreamClosed` enters `event_outs` in the same `handle_read`.
The published `resume_pending_reads` never polled that queue, so a close produced by a resumed drain waited for the next inbound datagram, which a peer that has already sent its reset need not send.
The repair moves the event translation of `handle_read` into one shared function, `forward_association_event`, and calls it from `resume_pending_reads` after each drain. The resumed drain therefore places `SCTPStreamClosed` into `event_outs` inside the same `poll_read`.

An event placed into `event_outs` during `poll_read` is consumed by the peer connection's next `poll_event`. The `webrtc` driver runs writes, then events, then reads in each pass, so no stage of the pass that produced the event consumes it. The next pass follows the driver's next wake. The peer does not guarantee that wake: it has sent its reset, and `rtc-sctp` stops its reconfig timer when the in-progress reply arrives, without reading the reply's result.

The repair therefore adds one local readiness wake. `SctpHandler::poll_timeout` reports the last observed logical instant while `event_outs` is not empty, in addition to the association deadlines it already reports. A caller reads that instant as zero delay and runs another pass. That pass's `poll_event` empties the queue, which clears the wake. The drain also marks the transport for flush, so the reset reply and the raised window leave in that pass's `poll_write`.
The wake is set by queued events only. Parked unread data never sets it, which the published test `poll_timeout_reports_nothing_for_a_parked_stream` continues to check. Events produced by `handle_read` are consumed by the pass that follows the read, before `poll_timeout` runs, so the wake does not fire for them.

The repair changes no signature and no queue.

Changed upstream files:

- `src/peer_connection/handler/sctp.rs`: `forward_association_event` shared by `handle_read` and `resume_pending_reads`; the two terminal arms in `drain_stream`; the event forward after a resumed drain; the readiness wake in `poll_timeout`; and tests for the immediate drain with a reset that overtook its data, the parked drain with a reset deferred against parked data (payload sequence, cleared entry, the close forwarded without another datagram, the wake while that close waits, and the wake cleared once it is consumed), a stale parked entry for a missing stream, and an unrelated `ErrShortBuffer` that must still fail the read. The two drain tests run with a read budget above one.
- The immediate-path test asserts a data count and a close count on two separate queues. It does not prove public data-before-close ordering; that proof belongs to the peer-connection boundary and the real-peer tests.

## Repair 3: receive-side close barrier at the public queue boundary

`RTCPeerConnection::handle_read` drains every handler's `poll_read` into the public `data_read_outs` queue in one pass.
The datachannel handler's `OnClose` for that channel waits in its own event queue at the same time.
The `webrtc` driver polls events before reads on every pass.
When the final payload and the close marker are processed in one intake, the driver sees `OnClose` while the accepted payload still waits in `data_read_outs`.
A consumer that stops at `OnClose` never reads that payload.

The barrier holds a channel's `OnClose` at the public queue boundary until that channel's accepted data has been read.
Both barrier maps are keyed by the public channel handle (`RTCDataChannelId`), which the rc.1 registry allocates monotonically and never reuses.
A successor channel on the same SCTP stream id therefore has its own count and its own held close. No stream-id parking or generation tracking exists.

- `PipelineContext` keeps a per-handle pending count. The single enqueue site into `data_read_outs`, `route_read_out`, increments it. Both public dequeue paths, `poll_data_read` and the data branches of `poll_read`, go through `pop_data_read_out`, which decrements it and removes the entry at zero.
- The public `poll_event` holds `OnDataChannel(OnClose(handle))` while that handle has a pending count. Every other event, including a close for a drained channel, passes unchanged. One channel's backlog never blocks another channel's close.
- The read that brings a handle's count to zero moves that handle's held close into the ordinary `event_outs` queue, behind whatever is already queued there. Held closes therefore surface in read order, one per `poll_event`.
- A held close exists only while its handle has a pending count, at most one per handle, so the number of held closes never exceeds the number of unread data-channel messages. A second `OnClose` for a handle that already holds one is dropped; the datachannel handler emits `OnClose` at most once per channel.
- `poll_timeout` keeps every handler deadline. While `event_outs` is not empty it also reports the datachannel handler's last observed logical instant, so the driver runs another pass now; that pass's `poll_event` empties the queue, which clears the wake. `event_outs` is otherwise empty when `poll_timeout` runs, because the driver drains events before reads. A held close whose data stays undrained schedules nothing.
- Whole-connection `close()` is a separate teardown case. Queued events and accepted data stay pollable after `close()`, as before. `close()` moves every held close into the event queue in held order, ahead of the `Closed` state event, and clears the counts. Retained data stays readable through both read APIs. A close that reaches the public queue after teardown is never held again. Explicit teardown can therefore expose closure before retained data. That is teardown behavior, not proof of graceful delivery.
- Local close semantics are unchanged. `OnClosing` has no producer in this crate and is not held.

Changed upstream files for this repair:

- `src/peer_connection/handler/mod.rs`: pending counts, held closes, `route_read_out`, `pop_data_read_out`, `hold_close_behind_pending_data`, the `poll_timeout` wake, the `close()` flush, and unit tests through the public API with events polled first: one channel, a reused stream id with distinct handles, a back-pressured sibling, both `poll_read` data branches, no added deadline while data stays undrained, two released closes in read order, duplicate close, and whole-connection teardown.
- `src/peer_connection/internal.rs`: initialize the barrier state.
- `tests/data_channel_backpressure_rtc2rtc.rs`: real-peer case in which the receiver polls events before reads and drains every datagram already at its socket in one intake.

## Workspace notes

Hub selects this directory through its workspace-root `[patch.crates-io]` entry.
Cargo does not inherit that patch into another workspace root.
The supported Hub runtime builds run from the Hub workspace root.
Hub client and test-support do not depend on the Hub runtime crate.
A consumer that embeds the Hub runtime from another workspace must select this patch explicitly.

This crate is excluded from the Hub workspace and is also patched by path, so Cargo cannot test it in place.
Tests run from a disposable copy of this directory with an empty `[workspace]` table appended to its `Cargo.toml`.
The Hub strict gate lints Hub workspace members, not this crate.

Validation results belong in the Hub implementation report. Source preparation alone is not delivery proof.
