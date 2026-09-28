# Botster local repair on `rtc-sctp 0.21.0-rc.1`

This directory contains the published `rtc-sctp 0.21.0-rc.1` crate with one local repair.

- Upstream repository: `https://github.com/webrtc-rs/rtc` (path `rtc-sctp`).
- Upstream revision from `.cargo_vcs_info.json`: `51558ffb550bb17a540343b338e2cd4a764f3690`, the same as the vendored `rtc`.
- Published crate checksum (`rtc-sctp-0.21.0-rc.1.crate`, sha256): `8823005e23738c18e7ddf86ed15e4e539565ec07de1609017fab3422988a7587`.
- The import commit contains the crate unmodified (extracted from the checksummed `.crate`); the repair is a separate commit.
- The package version and public signatures remain unchanged.
- Upstream candidate: yes. Both parts are RFC conformance fixes with no Botster-specific policy.

## Repair 4: a stream id is reusable only after both directions of its reset complete

### The failure

The Hub closes a terminal DataChannel when a client re-attaches the same subscription. The published crate then lets the client's next channel die before it opens:

1. The Hub resets its outgoing stream S (request H1).
2. The client performs the incoming reset, unregisters S at once (so `rtc` frees the id), and answers with its own outgoing reset of S (O1).
3. The Hub receives O1. S still exists there, and `reset_streams_if_any` always answers an incoming reset with another outgoing reset (H2), although the Hub's outgoing S was already reset by H1.
4. The client creates a channel. `rtc` hands out the lowest free id of its parity, which is S again.
5. H2 arrives and resets the new stream S: the channel closes before its DCEP ACK.

`vendor/rtc-0.21.0-rc.1/tests/data_channel_stream_id_reuse_rtc2rtc.rs` reproduces this deterministically. Before the repair it fails with `C (stream Some(3); X had stream 3) closed before it opened`. It was the cause of the intermittent Hub failure of `webrtc_terminal_adapter_host_close_emits_negotiated_terminal_subscription_closed`.

### The rules

RFC 8831 §6.7: "if one side decides to close the data channel, it resets the corresponding outgoing stream. When the peer sees that an incoming stream was reset, it also resets its corresponding outgoing stream. Once this is completed, the data channel is closed. [...] Streams are available for reuse after a reset has been performed."

RFC 6525 §5.2.7: "If the Result field indicates 'In progress', the timer for the Re-configuration Request Sequence Number is started again."

### The repair

Each stream records where its close handshake stands: `outgoing_reset` (`NotRequested`, `Requested`, `Performed`) and `incoming_reset`.

- Part B, no echo of an answer (protects the peer that reuses the id; in the failure above, the client). On an incoming reset, the association resets its own outgoing direction only if that is `NotRequested`. A request that answers our own reset (ours is `Requested` or `Performed`) starts no second reset. This is the RFC 8831 rule: the answering reset exists to reset *our* outgoing direction, and it is already reset.
- Part A, no reuse before both directions complete (protects the side that reuses the id, against a peer without part B, and against a late reset of the old stream). A stream is unregistered, which is what emits `SCTPStreamClosed` and lets `rtc` free the id, only when its incoming reset was performed and our outgoing reset was answered with a final result. Until then it stays registered with I/O closed. An answer of "In progress" is not completion: the request stays and its timer starts again (RFC 6525 §5.2.7); the published crate dropped the request on any answer.
- New data on a stream whose incoming direction was already reset belongs to a new stream on the same id: the peer reuses an id only after performing our reset. The old stream is retired and the data opens the new one, so a lost or late answer cannot swallow a new channel's first message.
- `Stream::stop` marks the outgoing reset `Requested` when it queues it. A reset processed without an answer (`respond == false`, the TSN-deferred path) keeps the published behaviour: unregister at once.

Consequence for `rtc`: `OnClose` for a remotely closed channel now follows the peer's answer to our reset, one round trip later than before. `SCTPStreamClosed` means the id is reusable.

Changed upstream files:

- `src/association/stream.rs`: `OutgoingReset`, the two `StreamState` fields, `stop` marks the request.
- `src/association/mod.rs`: `reset_streams_if_any` (part B and the deferred unregister), `handle_reconfig_param` (final answers complete the reset; "In progress" restarts the timer), `complete_outgoing_reset`, the data path (retire a reset stream on new data), and `now` passed to `handle_reconfig`.

### Validation

- `rtc` repro above: red before, green after (see the Hub implementation report).
- The crate's own tests: the same 129 pass and the same four published `kps_816*` tests fail before and after the repair, at the same assertions with the same output. Those four fail on the unmodified published crate; `rtc` Repair 2 handles that drain case.
- Not covered here: a peer retransmitting a request whose answer was lost after we reused the id. RFC 6525 §5.2.1 requires answering a duplicate request without re-performing it; the published crate does not track performed request sequence numbers. This is a follow-up candidate.

Tests run from a disposable copy of this directory with an empty `[workspace]` table appended to its `Cargo.toml`.
