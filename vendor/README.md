# Vendored crates with local patches

The Hub workspace selects these through `[patch.crates-io]` in the root `Cargo.toml`. Each directory is the published crate plus local repairs; its `BOTSTER-PATCH.md` describes every change, why, and how it was tested. On an upgrade, re-apply each repair or confirm upstream has it, then drop the patch.

| crate | repairs | upstream candidate |
| --- | --- | --- |
| `rtc-0.21.0-rc.1` | 1: ordered DataChannel close (send side). 2: terminal stream results inside the SCTP drain. 3: receive-side close barrier at the public queue boundary. | yes |


