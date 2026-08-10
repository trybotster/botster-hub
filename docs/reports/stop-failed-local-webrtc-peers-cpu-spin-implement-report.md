# Implement report: Stop failed local WebRTC peers from spinning botster-hub CPU

## Target
- **repository**: botster-hub
- **target_id**: tgt_7e208a0c76a44980a83b63af976b1f22
- **ticket**: ticket_1786324642_480494
- **run**: run_1786324716_877362
- **approved plan**: Plan v6 (artifact_1786328406_552580), Plan Review approved

## Playbooks and notes applied
- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[terminal webrtc failure records do not prove peer runtime teardown]]
- [[webrtc peer cleanup removes every per peer owner together]]
- [[webrtc bootstrap origin must be requested after the package server binds]]
- [[test script required for rust tests not cargo test]]
- [[graceful-termination-requires-explicit-cleanup-hooks]]
- [[late webrtc messages after disconnect must not recreate clients]]
- [[file descriptor exhaustion from stale webrtc connections]]

## Files changed
- `src/local_webrtc.rs` — per-peer Tokio runtime, `close_peer` single teardown path, handler retention, grant tags on peer-originated control messages, focused production-path tests
- `src/daemon_transport.rs` — grant fields on `ControlMessage::{Request,SubscribeEntities}`, entity subscription owner field, attach owner map, stale-peer admission gates, grant-selective PeerClosed sweep via `close_peer`, ownership tests
- `crates/botster-hub-test-support/src/lib.rs` — `write_botster_web_production_fixture`
- `tests/hub_daemon_lifecycle_test.rs` — fixture writer delegates to shared helper
- `examples/local_webrtc_offerer.rs` — `write-fixture` + long-lived `connect` offerer for the live oracle

## Ownership boundaries preserved
- All lifecycle policy remains in hub (`src/local_webrtc.rs`, `src/daemon_transport.rs`)
- No botster-core changes
- No botster-hub-client DTO field additions
- No botster-web / browser changes
- ControlMessage and EntitySubscriptionState remain `pub(crate)` internals

## Cross-repo dependencies / separately routed work
- None. Live oracle preparation uses production hub CLI (`packages enable`, `apps open`) already owned by this repository.

## Deviations from plan
- **First Implement pass (commit 825a863):** the two-grant Attach proof was under-specified relative to Plan v4/v6. The suite only had an owner-map unit insert test (`attach_owner_map_is_grant_selective_on_peer_closed`) plus peer-map independence. That was a material acceptance gap (finding_1786330602_859086).
- **Returned Implement pass 1:** replaced the owner-map insert test, but still injected `local_webrtc_grant_id` into `ControlMessage::Request` (finding_1786331425_582493). Ablation that cleared the production tag at `run_data_channel` still passed.
- **Returned Implement pass 2:** rewrote `two_live_grant_attach_isolation_preserves_sibling_delivery` to use the production WebRTC DataChannel path:
  1. real offer/answer peers via `signal()` with open DataChannels
  2. encrypted Attach over each peer DataChannel so `run_data_channel` stamps the grant
  3. asserts observed Attach control messages carry `Some(grant_a)` / `Some(grant_b)` (fails if production tag is cleared)
  4. fails peer A via the production handler + pumped `LocalWebrtcPeerClosed`
  5. SendInput/Drain over peer B's DataChannel after A fails, still delivery-proven
  6. fails B to baseline (`webrtc_peer_failed == 2`, empty owner map)
- Plan v6 command order remains the acceptance order for Verify.
- Focused in-process peer_failed test still drives the production handler Arc retained on `LocalWebrtcPeerHandle` after `signal()`.

## Production entry point
`IssueLocalWebrtcBootstrap` → `LocalWebrtcSignal` → `LocalWebrtcTransport::signal` (per-peer runtime + `answer_offer`) → `LocalWebrtcHandler::on_connection_state_change(Failed)` → `cleanup_once` → `ControlMessage::LocalWebrtcPeerClosed` → `close_peer` (peer.close + runtime.shutdown_timeout) + grant-owned attach/entity sweeps.

## Tests and downstream proof run
```
BOTSTER_ENV=test cargo test --lib -- two_live_grant_attach_isolation production_handler_peer_failed close_peer_is_idempotent two_peer_independence late_subscribe early_entity
# 6 passed (includes production two-live-grant Attach isolation)

BOTSTER_ENV=test cargo test --lib -- local_webrtc
# 25 passed

cargo build --example local_webrtc_offerer
cargo run --example local_webrtc_offerer -- write-fixture /tmp/botster-web-fixture-test
# fixture_written; package files present

cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```

## Unverified behavior / residual risk
- Full live no-spin oracle (debug hub + offerer + kill -9 + ps/sample CPU bounds within 40s) is Verify-stage evidence, not re-run here as a complete multi-minute campaign.
- rtc-ice failed timeout remains lockfile-pinned (~25s); oracle bound is 40s as planned.
- Two-grant Attach isolation is now proven in-process through production signal/handler/control paths. Browser-side DataChannel framing of Attach is the same control request after decrypt; end-to-end browser framing remains oracle/smoke coverage.

## Missing vault guidance discovered
- Candidate (from plan): webrtc-rs PeerConnectionDriver exits only on explicit Close; dropping the handle alone does not stop the driver.
- Candidate (from plan): apps open web_app prints app_url after supervised bind — sanctioned origin capture for local WebRTC bootstrap.
- Candidate: rtc-ice 0.20 keepalive/disconnected/failed timing constants underpin peer_failed detection after abrupt offerer death.
