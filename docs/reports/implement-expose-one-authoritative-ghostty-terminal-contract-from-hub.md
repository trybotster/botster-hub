# Implementation report: Expose one authoritative Ghostty terminal contract from Hub

Ticket: `ticket_1786471489_718500`  
Run: `run_1786476458_719916`  
Step: `botster_stack_implement`  
Approved plan: rev 9 (`docs/plans/expose-one-authoritative-ghostty-terminal-contract-from-hub.md`)

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Worktree | `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1786471489_718500` |
| Branch | `project-pipelines/ticket_1786471489_718500` |
| Implement Core pin | `2c5171a6cb3b073c53620a9838d8b08480dd215c` (in `Cargo.lock`) |
| Protocol | 6 |
| Conformance fixture revision | 34 |
| Feature token | `mode_gated_input` |
| npm package candidate | `@trybotster/hub-test-support@0.1.29` (published assets ready; npm OTP blocked publish) |
| `teardown_class_applies` | false |

## Repository playbook and other playbooks/notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster-hub-client-playbook]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster hub is a first party host profile over core]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[adding a hub client feature constant is a three site change]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[conformance fixture revisions must be unique per published content]]
- [[initial terminal snapshots must precede live output activation]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[hub test support npm releases need external consumer smoke]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[test script required for rust tests not cargo test]]
- Human answers: `question_1786477685_799257`, `question_1786478263_847168`

## Files changed

- `Cargo.lock` — Core pin `2c5171a6…`
- `crates/botster-hub-client/src/lib.rs` — conf 34, `FEATURE_MODE_GATED_INPUT`, full `DaemonModeFlags` + `#[non_exhaustive]`, `ModeGatedInput` request/response, handshake proofs
- `crates/botster-hub-client/src/typescript.rs` — generated TS DTOs
- `crates/botster-hub-client/generated/daemon-protocol.ts` — checked TS artifact
- `src/runtime.rs` — startup color baseline, `mode_gated_input` runtime path (Core 5s default timeout)
- `src/client_api.rs` — full ModeFlags projection + ModeGatedInput API
- `src/daemon_transport.rs` — public daemon socket mapping
- `src/main.rs`, `src/lib.rs`, `src/local_webrtc.rs` — response projection / operator print / DTO fields
- `crates/botster-hub-test-support/src/lib.rs` — mode fixtures + support matrix
- `packages/hub-test-support/*` — npm 0.1.29 assets, conf 34 fixtures, README/test
- `tests/hub_daemon_lifecycle_test.rs` — named `external_hub_*` live proofs
- `tests/hub_client_api_test.rs`, `tests/support/mod.rs`
- `README.md`, `docs/client-protocol.md`, plan build command for `botster-core-daemon` worker package
- `script/build-release-artifacts`, `script/test-production-package-runtime`

## Ownership boundaries preserved

- Hub remains control/policy/orchestration; **no** public control response carries GHOSTSNP/`payload_base64`.
- GHOSTSNP install path is **only** `DaemonEvent::Snapshot` on the SessionIo/ClientWorker data plane.
- `CaptureSnapshot` remains metadata-only.
- Startup color profile is baseline/reset only (FG `#FFFFFF`, BG `#282C34`, cursor `#FFFFFF`); current colors after start come from GHOSTSNP install.
- External DTOs live in `botster-hub-client` (hub-client charter); runtime mapping in hub.
- Web/TUI product code not modified (downstream tickets).

## Cross-repo dependencies or separately routed work

- Core dependencies closed and pinned (`2c5171a6…`): color + mode revision APIs.
- Web and TUI consume this Hub contract in separately routed tickets.
- Session worker binary package is now `botster-core-daemon` (not `botster-core`); hub scripts/tests updated accordingly.

## Deviations from plan

1. **Worker package path:** Core pin places `botster-session-worker` under `-p botster-core-daemon` (plan text still said `botster-core` in places). Implementation + plan artifact updated.
2. **npm publish:** Package `0.1.29` is fully prepared and dry-run clean; live `npm publish` requires human OTP (`EOTP`). Not a product-code deviation.
3. **No public CaptureColorAndSnapshot:** follows plan rev 8/9 (control-path GHOSTSNP forbidden).

## Tests and downstream proof run

Commands (via `./test.sh` unless noted):

| Proof | Result |
| --- | --- |
| Full workspace `./test.sh` | **pass** |
| `external_hub_ghostty_snapshot_install_before_live_rejects_scrollback_as_ghostsnp` | **pass** |
| `external_hub_mode_gated_kitty_stale_token_rejects_and_reprobe_admits` | **pass** |
| `external_hub_mode_gated_mouse_stale_token_rejects_and_reprobe_admits` | **pass** |
| `external_hub_ghostty_snapshot_reflects_osc_palette_and_specials` | **pass** |
| `external_hub_osc_101112_session_side_replies_with_startup_baseline` | **pass** |
| `external_daemon_same_session_reattach_replays_opaque_history_before_live_output` | **pass** |
| `read_mode_flags_returns_exact_authoritative_values_and_session_attribution` | **pass** |
| hub-client lib (incl. ModeGatedInput + handshake + TS artifact) | **pass** |
| hub_test_support_conformance_test | **pass** |
| `packages/hub-test-support` `npm test` | **pass** |
| TUI scratch cargo patch → `cargo check -p botster-tui` | **pass** (non_exhaustive ModeFlags additive; TUI worktree restored clean) |

### Live binary provenance (for external_hub_* runs)

| Identity | Value |
| --- | --- |
| Hub source (pre-commit tip) | branch `project-pipelines/ticket_1786471489_718500` |
| Core lock pin | `2c5171a6cb3b073c53620a9838d8b08480dd215c` |
| Hub binary | `…/target/debug/botster-hub` |
| Session worker binary | `…/target/debug/botster-session-worker` |

## Unverified behavior or residual risk

- **npm OTP publish:** `@trybotster/hub-test-support@0.1.29` not on registry until human publishes with OTP. Generated package contents verified locally via `npm test` and dry-run.
- **GHOSTSNP color decode on Hub:** intentionally not done (charter: Hub does not decode GHOSTSNP). OSC mutation proof asserts non-empty GHOSTSNP Snapshot after OSC; Core already proved color/GHOSTSNP agreement on the pin.
- **External clean-install npm smoke against registry 0.1.29:** blocked until publish.

## Missing vault guidance discovered

- None that blocked implementation. Worker package rename (`botster-core` → `botster-core-daemon` for the session-worker bin) is a Core packaging change that Hub build scripts must track; not previously captured as a Hub-side atomic note in the plan’s load set.

## Production entry points using the new behavior

1. Daemon hello/status advertises `mode_gated_input` + conf 34 via `DaemonCompatibility::current()`.
2. `daemon_transport` maps `DaemonRequest::ModeGatedInput` → `HubClientApi` → `HubRuntime::mode_gated_input` → `CoreDaemon::mode_gated_input` (default Core 5s timeout).
3. `ReadModeFlags` projects full ModeFlags + freshness from Core `ModeFlagsReady`.
4. Startup composition applies `with_terminal_color_profile` product baseline before PTY output.
5. Attach/drain continues to deliver GHOSTSNP only on `DaemonEvent::Snapshot`.
