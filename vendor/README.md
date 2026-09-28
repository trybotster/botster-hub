# Vendored crates with local patches

The Hub workspace selects these through `[patch.crates-io]` in the root `Cargo.toml`. Each directory is the published crate plus local repairs; its `BOTSTER-PATCH.md` describes every change, why, and how it was tested. On an upgrade, re-apply each repair or confirm upstream has it, then drop the patch.

| crate | repairs | upstream candidate |
| --- | --- | --- |
| `rtc-0.21.0-rc.1` | 1: ordered DataChannel close (send side). 2: terminal stream results inside the SCTP drain. 3: receive-side close barrier at the public queue boundary. | yes |
| `rtc-sctp-0.21.0-rc.1` | 4: a stream id is reusable only after both directions of its reset complete; no reset echo in answer to our own reset; "In progress" answers keep the request (RFC 8831 §6.7, RFC 6525 §5.2.7). | yes |

Repair 4's regression test lives with `rtc`: `rtc-0.21.0-rc.1/tests/data_channel_stream_id_reuse_rtc2rtc.rs`. Running it requires the `rtc` test copy to patch `rtc-sctp` to this directory as well.
