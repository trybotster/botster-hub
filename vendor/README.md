# Vendored crates with local patches

The Hub workspace selects these through `[patch.crates-io]` in the root `Cargo.toml`. Each directory is the published crate plus local repairs; its `BOTSTER-PATCH.md` describes every change, why, and how it was tested. On an upgrade, re-apply each repair or confirm upstream has it, then drop the patch.

| crate | repairs | upstream candidate |
| --- | --- | --- |
| `rtc-0.21.0-rc.1` | 1: ordered DataChannel close (send side). 2: terminal stream results inside the SCTP drain. 3: receive-side close barrier at the public queue boundary. | yes |



`rtc-sctp` is not vendored. The root `Cargo.toml` takes it (and `rtc-shared`) from `https://github.com/trybotster/rtc`, branch `botster/rtc-sctp-stream-reuse`: upstream `51558ffb` plus one commit. Repair 4's regression test lives with `rtc`: `rtc-0.21.0-rc.1/tests/data_channel_stream_id_reuse_rtc2rtc.rs`.
