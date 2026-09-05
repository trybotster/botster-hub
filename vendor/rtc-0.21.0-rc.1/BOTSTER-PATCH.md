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

Every other error is returned unchanged. The `AssociationLost` event queued by the reset still reaches the caller's event loop, so `SCTPStreamClosed` follows the drained data in the same pass.
The repair changes no signature, no queue, and no timer.

Changed upstream files:

- `src/peer_connection/handler/sctp.rs`: the two terminal arms in `drain_stream`, and tests for the immediate drain with a reset that overtook its data, the parked drain with a reset deferred against parked data, a stale parked entry for a missing stream, and an unrelated `ErrShortBuffer` that must still fail the read. The two drain tests run with a read budget above one.

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
