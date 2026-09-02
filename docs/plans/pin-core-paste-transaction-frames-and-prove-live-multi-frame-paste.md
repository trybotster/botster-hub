# Plan: Pin Core paste transaction frames and prove live multi-frame paste over Unix and WebRTC

Ticket: `ticket_1788313897_932611`
Run: `run_1788326546_496759`
Step: `botster_stack_plan`
Pipeline: `botster_stack_delivery` (direct merge into `main`, no PR)
Parent (closed): `ticket_1788287678_207209` — Core: bounded atomic multi-frame terminal input transactions
Plan **revision 2** after Plan Review `review_1788328352_202496`

## Plan Review corrections (rev 2)

| Finding | Status |
| --- | --- |
| `finding_1788328352_438396` content-blind source guard omits production file tails | **Locked.** The guard no longer splits at the first `mod tests`. It scans every byte of every `.rs` file under the four roots, so scanner state cannot skip a tail. The guard takes a scan root, returns the scanned file set, requires anchor files and the `push_complete` anchor symbol, and ships with a permanent seeded-EOF red control that scans a mutated scratch copy. Loaded [[a source scanner can stay in cfg test skip mode through end of file]], [[a known positive control proves a scan is live not that its pattern set is complete]], and [[hub moves must extend source scanning guard file lists]]. See Scope item 5. |
| `finding_1788328352_902303` plan base stale (`080ca9a` vs `origin/main` `db2c43c`) | **Locked.** Branch rebased onto `db2c43c` (plan commit only; clean). `db2c43c` removes `drain_runtime_once` from `src/runtime.rs` and moves the adapter-loss unit test in `src/subscription/attach_routes.rs` onto the production wake driver. The planned live tests already drive the production driver through the isolated Hub, so no plan step depended on the removed helper. Pin inventory re-run on the new base: 24 active matches (18 source sites + 6 lock sources), unchanged. Cited paths re-checked. |
| `finding_1788328352_269589` Plan completion evidence empty | **Locked.** Gate evidence and completion evidence on this visit carry `plan_uri`, `artifact_id` (new rev-2 artifact), `checklist_id`, `target_id`, and `target_repository`. |

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Plan worktree | Pipeline-provided ticket worktree, branch `project-pipelines/ticket_1788313897_932611`, base `db2c43c` (rebased from `080ca9a` in rev 2) |
| Worktree hygiene | tracked `.gitignore` has 5 lines; path has no `:`; no `CARGO_TARGET_DIR` override is allowed for the official gate |
| Merge policy | direct into `main`; do not create a PR |

Resolution: the ticket and run both carry `target_id` `tgt_7e208a0c76a44980a83b63af976b1f22`. The same target id maps to `botster-hub` in `docs/plans/bind-content-blind-webrtc-terminal-adapters-at-admission.md`. The ambient worktree is a `botster-hub` checkout, but routing came from the ticket target, not from the directory.

## Repository playbook loaded

[[botster-hub-playbook]]

## Other role/surface playbooks and atomic notes loaded

Role:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-architecture]] and [[cli-patterns]] (index only; ownership comes from the Hub charter)

Ownership and content-blind contract:

- [[botster hub is a first party host profile over core]]
- [[botster Hub Rust stays a trusted host kernel]]
- [[core owns duplex terminal transport while Hub stays content blind]]
- [[core owns bounded atomic terminal input transactions across clients]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[core terminal progress is wake driven and targeted]]
- [[concrete terminal transports stay in hub until a second host needs them]]
- [[botster subscriptions use dedicated ordered DataChannels]]
- [[Core terminal subscription ownership is session, subscription, and generation]]

Pin roll and gate discipline:

- [[Hub Core pin rolls update eleven literal sites and six lock sources]]
- [[Git-consumed Hub members pin Core protocol by exact revision]]
- [[Hub test support copies Core protocol fixtures from the pinned crate source]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[source guard ablations must not overlap a running full suite]]
- [[exact Rust test ablations require a one test baseline]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[unused test environment seams signal skipped acceptance checks]]
- [[fixed source guard lists need one ablation per added file]]
- [[region bounded source guards need a required symbol anchor]]
- [[a source scanner can stay in cfg test skip mode through end of file]]
- [[a known positive control proves a scan is live not that its pattern set is complete]]
- [[hub moves must extend source scanning guard file lists]]

Runtime-teardown class decision: **does not apply**. This ticket adds no peer, session, ClientWorker, or SessionIo teardown behavior, no new ownership-creating message, and no close path. It rolls a dependency pin and adds live data-path proof over existing bound routes. [[botster runtime teardown lenses]] was read to make this decision. If Plan Review forces the class, the answers are: isolation and ownership identity are unchanged from `bind-content-blind-webrtc-terminal-adapters-at-admission`; the only hard stop this ticket touches is the existing Core `TerminalIngress::Lost` hard stop, which retires exactly one `(session, subscription, generation)` route and is exercised as a control arm below.

Not loaded, with reason:

- [[project-pipelines-playbook]] — no Project Pipelines package or plugin path changes.
- [[botster-hub-client-playbook]] — no DTO, serde name, or protocol version change in `botster-hub-client`; only its manifest pin moves.
- Other repository charters — this run stays on `botster-hub`.

## Context loaded

- Ticket, run, gate, and dependency state from `project_pipelines_current_context`. The parent dependency is `closed`; `blocking_dependencies` is empty.
- Parent ticket `ticket_1788287678_207209`: closed run `run_1788312391_642018` on `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` (botster-core). Project Pipelines has no PR link for it because Core also merges directly.
- Core evidence from the local `~/Projects/botster-core` checkout after `git fetch`: `origin/main` = `48a437032791e678010254708259568ce4ad02bf` ("Enforce declared paste assembly bounds", 2026-09-01 22:02:42 -0700). The four commits above the current Hub pin `e5a927c31d5b7d0b0f4b198e5e556ed75d53ddf1` are `8d9cb1c` (Add bounded atomic terminal paste transactions), `58d328d` (Preserve one result per paste operation), `e065f75` (Remove unreachable paste rejection), and `48a4370`. No commit exists above `48a4370` on `origin/main`.
- Core protocol delta (`crates/botster-terminal-protocol`): version `0.1.0` → `0.2.0`. `TerminalInputFrame::from_bytes` now accepts kind bytes `4..7` (`paste_begin`, `paste_chunk`, `paste_commit`, `paste_abort`). New public constants: `PASTE_BEGIN_BODY_BYTES = 24`, `PASTE_CHUNK_PREFIX_BYTES = 8`, `MAX_PASTE_CHUNK_DATA_BYTES = 65_527`, `PASTE_COMMIT_BODY_BYTES = 4`, `PASTE_ABORT_BODY_BYTES = 4`, `MAX_PASTE_BYTES = 1_048_576`, `MAX_PASTE_CHUNKS = 17`. The Hub-safe crate validates only scheme, kind, and body length.
- Core assembly contract: `PASTE_ASSEMBLY_TIMEOUT = 5 s` starts when Core intakes Begin. `INTAKE_FRAMES_PER_SUBSCRIPTION_PER_TICK = 64` equals `MIN_ADAPTER_INGRESS_BUFFER_FRAMES = 64`. Core wraps content in `ESC[200~` / `ESC[201~` only when the session's `bracketed_paste` flag is on. `input_result` frames are JSON with `"type":"input_result"`, `"kind":"paste"`, `"operation_id"`, `"admitted"`, `"bytes_written"` (content bytes, not wrapper bytes), `"mode_generation"`, `"mode_revision"`, and optional `"rejection"`.
- Core live proof today (`crates/botster-core-daemon/tests/terminal_wake_test.rs::paste_above_one_frame_delivers_one_atomic_worker_write_and_result`) pastes 70,012 bytes (two chunks) into a real worker with `stty raw -echo; ... dd bs=1 count=70012 | wc -c` and asserts exactly one admitted result. Core has not proved a 1 MiB paste into a real PTY.
- Core daemon `wait_pump` already clamps to `clamp_paste_wait` and returns `expired_paste_wake_batch` on timeout (`crates/botster-core-daemon/src/daemon.rs`). Hub's data-plane driver (`src/data_plane/driver.rs`) calls `wait_pump` and `pump_woken` unchanged. No Hub driver change is needed for paste deadlines.
- Hub ingress path: `src/transport/shared/ingress.rs` (`IngressBuffer`, `sync_channel(MIN_ADAPTER_INGRESS_BUFFER_FRAMES)`, header validation only, `Lost` latch), `src/transport/shared/adapter_slot.rs::push_ingress`, Unix caller `src/transport/unix/connection.rs` (`UnixInbound::Terminal` → `handle.push_ingress`), WebRTC caller `src/transport/webrtc/subscription_channel.rs::run_bound_terminal_channel` (versioned WebRTC chunks → one opaque AES-GCM envelope → one complete Core frame → `handle.push_ingress`).
- Frame size correction from Implement question `question_1788330311_325612`: Unix mux framing fits a 65,539-byte Core frame. WebRTC limits each serialized application frame to `LOCAL_WEBRTC_MAX_FRAME_BYTES = 65,536`; the encrypted JSON envelope for a maximum Core frame exceeds that bound. The human approved bounded inbound transport reassembly in this ticket. Hub may buffer one opaque ciphertext envelope per subscription route, up to `LOCAL_WEBRTC_MAX_DELIVERY_BYTES`, but it must not inspect or buffer paste transaction state.
- Active Core SHA literal sites in this tree: 18 source sites plus 6 `Cargo.lock` `source` lines (24 active matches; 4 more live under `docs/plans` and `docs/reports` and stay historical). The vault note counts eleven; see Vault gaps.
- Hub tests encode terminal input frames by hand (`tests/hub_daemon_lifecycle/common.rs::terminal_input_frame_bytes`, `terminal_mode_gated_frame_bytes`). No Hub workspace member depends on `botster-terminal-protocol-client`, and `src/session_projection.rs` forbids that crate name in Hub source.
- Existing seams reused, not added: `BOTSTER_HUB_TEST_PAUSE_DATA_PLANE=<file>` pauses the data-plane driver without touching inventory reads (`paused_data_plane_keeps_control_requests_from_driving_terminal_progress`); `DaemonEvent::TerminalSubscriptionClosed` with reason `core_adapter_closed` is the Hub-visible oracle for a Core hard stop (`core_write_budget_hard_stop_emits_core_adapter_closed`).
- Sibling tickets checked (open, same project): `ticket_1788206393_323469` (Hub: reproduce targeted pump_woken resize with merged Core) has no run; its Core prerequisite is already inside the current Hub pin `e5a927c`, so this roll does not conflict with it. `ticket_1788313903_124535` (Core: publish `@trybotster/terminal-protocol` 0.3.0) is an npm publication that Hub does not consume. `ticket_1787603674_865638` (TUI) and `ticket_1787600676_914408` (Web) consume this Hub roll downstream. `ticket_1787600679_990088` (Integration cold-cut) and `ticket_1787600691_401181` (Hub Rust/Lua boundary) do not touch the Core pin.
- Toolchain: pipeline shell reports `rustc 1.92.0`; `RUSTUP_TOOLCHAIN=1.97.0 rustc --version` reports `1.97.0`; `zig 0.16.0`. CI pins Rust `1.97.0`.

## Botster layers touched

- Rust hub (dependency manifests, lockfile, provenance literals).
- Rust hub integration tests (`tests/hub_daemon_lifecycle/**`).
- Hub docs (`docs/client-protocol.md` one sentence).

No Lua, SPA, TUI, MCP, or package changes.

## Scope

1. **Core pin roll to `48a437032791e678010254708259568ce4ad02bf`.** Update every active Core revision literal to the same SHA in one commit. Keep the URL `https://github.com/trybotster/botster-core.git` and the `rev =` selector on every dependency. Sites:
   - root `Cargo.toml` (5): `botster-core`, `botster-core-daemon`, `botster-terminal-protocol`, `botster-core-test-support`, `botster-terminal-ghostty`.
   - `crates/botster-hub-client/Cargo.toml` (1).
   - `crates/botster-hub-test-support/Cargo.toml` (3).
   - `crates/botster-hub-test-support/build.rs` `PROTOCOL_REV`.
   - `crates/botster-hub-test-support/src/conformance_data.rs` `LATE_ATTACH_GHOSTSNP_CORE_PIN`.
   - `crates/botster-hub-test-support/src/lib.rs` provenance unit-test literal (line ~6473).
   - `tests/session_projection_owner_loop.rs` `REQUIRED_CORE_REV`.
   - `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` `LOCKED_CORE_REV`.
   - `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`, `webrtc_terminal_adapter.rs`, `package_event_plane.rs`, `event_plane_saturation.rs` live-proof literals.
   - `Cargo.lock`: six `source = "git+...?rev=<sha>#<sha>"` lines for `botster-core`, `botster-core-daemon`, `botster-core-test-support`, `botster-terminal-ghostty`, `botster-terminal-protocol`, `botster-terminal-protocol-client`, plus the `version` bumps Cargo records (`botster-terminal-protocol` `0.1.0` → `0.2.0`, and any other Core-family version Cargo rewrites). Use `cargo update -p <pkg> --precise` or a targeted `cargo update` for the Core family only; do not float other registry crates.
   - Post-roll invariant: `grep -rn e5a927c31d5b7d0b0f4b198e5e556ed75d53ddf1 --exclude-dir=target --exclude-dir=node_modules --exclude-dir=.git .` returns matches only under `docs/plans/**` and `docs/reports/**`.
2. **Live multi-frame paste proof over the Unix terminal adapter.** New test module `tests/hub_daemon_lifecycle/paste_transaction.rs`, `include!`d from `tests/hub_daemon_lifecycle_test.rs` after `webrtc_terminal_adapter.rs`, under `daemon_test_guard()`.
   - Test-side frame encoder in `tests/hub_daemon_lifecycle/common.rs`: `terminal_paste_frame_bytes(operation_id, mode_generation, mode_revision, data) -> Vec<Vec<u8>>` producing the compact Begin (`[1,4,0,24]` + `u32` op id + `u64` generation + `u64` revision + `u32` total), ordered Chunk frames (`[1,5]` + `u16` body len + `u32` op id + `u32` index + ≤65,527 data bytes), and Commit (`[1,6,0,4]` + `u32` op id). Mirror Core's `compact_paste_frames` test helper. This is test-only byte assembly; production Hub code gains no paste knowledge.
   - Payload: exactly `1_048_576` bytes (`MAX_PASTE_BYTES`), deterministic pseudo-random bytes covering all 256 values, so `encode` yields `1 + 17 + 1 = 19` frames. Assert the frame count is 19 before sending.
   - Session fixture: a shell child that switches to raw mode before any paste byte can arrive, sinks exactly the declared byte count to a file under the test directory, and prints a done marker: `stty raw -echo; printf 'paste-sink-ready'; head -c 1048576 > <abs sink path>; printf 'paste-sink-done'`. Wait for `paste-sink-ready` on the bound route and for `ReadModeFlags` to report `mode_generation != 0`; take `mode_generation` and `mode_revision` from that response for Begin. `bracketed_paste` stays off so the PTY receives content bytes only.
   - Send all 19 frames back to back on the bound Unix mux connection (`DaemonUnixTerminalEnvelope::from_frame_bytes` per frame) with no pacing.
   - Oracles: (a) exactly one opaque terminal envelope whose payload bytes contain `"type":"input_result"`, `"operation_id":<id>`, `"admitted":true`, and `"bytes_written":1048576`, observed within a bounded drain window and counted across the whole window, with zero `"rejection"` substrings for that operation id; (b) `paste-sink-done` arrives and the sink file equals the sent payload byte for byte (`std::fs::read == payload`); (c) no `TerminalSubscriptionClosed` event for the route during the window; (d) the route remains listed by `Status` occupancy after the paste.
3. **Bounded WebRTC inbound framing and live paste proof.** Reuse `DaemonLocalWebrtcDeliveryChunk` version 2 with `DaemonTerminalFrame`. The client test fixture encrypts each complete opaque Core frame, divides the serialized encrypted envelope into 12 KiB payloads, and sends the chunks on the ordered terminal DataChannel. `run_bound_terminal_channel` holds one bounded `InboundTerminalEnvelopeAssembly` per route. It validates version, kind, message identity, exact index/count/total length, per-frame size, and the existing maximum delivery bytes. It decrypts only after complete reassembly and sends one complete Core frame through `handle.push_ingress`. Malformed, duplicate, interleaved, oversized, incomplete-on-close, or post-close input fails closed. Hub does not decode the Core body or own paste state. The same 19 Core frames, payload, fixture, and result oracles run through this path. The control channel must carry zero terminal frames, and no `TerminalSubscriptionClosed` host event may arrive.
4. **Ingress buffer hold proof (64 ≥ 19, no `Lost`).** Unix arm with the existing pause seam: start the isolated Hub with `BOTSTER_HUB_TEST_PAUSE_DATA_PLANE=<file>`, bind and reach `paste-sink-ready`, arm the pause file and wait for the `.entered` acknowledgement, send all 19 frames while Core cannot intake, hold ≥ 500 ms, assert no `TerminalSubscriptionClosed` and no `input_result` arrived, remove the pause file, then require the single admitted `input_result` with `bytes_written = 1048576` and the byte-exact sink. Because Core sees Begin only after resume, the 5 s assembly timeout cannot start during the hold. This proves the `IngressBuffer` held the full burst without latching `Lost`.
   - **Control arm (proves the oracle can go red):** same paused setup, send `MIN_ADAPTER_INGRESS_BUFFER_FRAMES + 1 = 65` valid single-byte kind-1 input frames, resume, and require `TerminalSubscriptionClosed` with reason `core_adapter_closed` for exactly that route while a sibling route on the same session stays live. This is the live counterpart of the unit test `overflow_latches_lost_once`.
5. **Content-blind source guard (rev 2).** In `paste_transaction.rs`, a helper `assert_hub_source_paste_blind(root: &Path) -> Result<BTreeSet<PathBuf>, String>` walks `<root>/transport/**`, `<root>/subscription/**`, `<root>/data_plane/**`, and `<root>/admission/**` recursively with no fixed file list and scans **every byte of every `.rs` file**, including inline `#[cfg(test)]` modules. There is no `mod tests` split and no brace-state scanner, so no tail can be skipped; Hub source may not name paste-transaction internals even in its own unit tests. Forbidden tokens: `KIND_PASTE`, `PASTE_BEGIN`, `PASTE_CHUNK`, `PASTE_COMMIT`, `PASTE_ABORT`, `MAX_PASTE`, `operation_id`, `encode_paste`, `botster_terminal_protocol_client`. `bracketed_paste` (a mode flag DTO field) is not forbidden. Positive invariants: the returned set must contain `transport/shared/ingress.rs`, `transport/shared/adapter_slot.rs`, `transport/unix/connection.rs`, and `transport/webrtc/subscription_channel.rs`, and `ingress.rs` must contain the anchor symbol `push_complete`, so an empty or mis-rooted walk fails. Two tests: `hub_transport_source_stays_paste_blind` runs the helper on the real `src/` (via `CARGO_MANIFEST_DIR`); `paste_blind_guard_fails_on_seeded_eof_token` copies the four roots into a scratch directory, appends `operation_id` as the **last line of `transport/shared/ingress.rs`, after its final `#[cfg(test)] mod tests` block**, runs the helper on the scratch root, and requires an `Err` that names that file. That red control is permanent in the suite, not a manual ablation, and it proves tail coverage because the seeded token sits where a `cfg(test)` skipper would have stopped scanning. The existing `FORBIDDEN_PRODUCTION_CONSTRUCTS` list in `src/lib.rs` is not extended; its fixed file list would need one ablation per file and it lives in production source.
6. **Docs.** `docs/client-protocol.md` line ~1094: extend "Clients send input, mode-gated input, and resize as compact binary frames" to name paste transaction frames (`paste_begin`, `paste_chunk`, `paste_commit`, `paste_abort`) as Core-owned opaque frames that Hub header-validates only. `README.md` line ~100 gains the same one clause if it enumerates frame kinds. No new docs file.
7. **Plan document.** This file, committed on the run branch.

## Non-scope

- Any Hub-side paste state, Core frame chunk policy, reordering, acknowledgement, retry, timeout, or body decode. Hub stays content blind. The approved WebRTC exception holds one bounded opaque encrypted envelope for transport reassembly.
- Changes to `IngressBuffer` capacity, `MIN_ADAPTER_INGRESS_BUFFER_FRAMES`, DataChannel `max_message_size`, or `DAEMON_MAX_FRAME_BYTES`.
- Data-plane driver changes for paste deadlines; Core clamps inside `wait_pump`.
- `botster-hub-client` DTO, serde, protocol-version, or `hub-test-support` npm changes. Exported fixture bytes do not change with this Core delta (`git diff e5a927c..48a4370 -- crates/botster-terminal-protocol/fixtures` is empty), so no npm bump.
- Wiring the dead `store_partial` / `complete_partial` / `inject_ingress_*` test seams in `adapter_slot.rs`. They are pre-existing and unrelated to this ticket; see Vault gaps.
- Bracketed-paste wrapper proof (`ESC[200~ … ESC[201~`). Core proves wrapping in `terminal_wake_test.rs`; a Hub arm with `bracketed_paste` on is optional and must not replace the plain arm.
- Web or TUI client work (`ticket_1787600676_914408`, `ticket_1787603674_865638`).
- Publishing `@trybotster/terminal-protocol` (`ticket_1788313903_124535`).
- Sibling `ticket_1788206393_323469` scope (resize reproduction).

## Repository ownership boundaries and cross-repo dependencies

- **botster-core** owns frame kinds, paste bounds, assembly, timeout, bracket wrapping, `TerminalInputResult`, and the wake clamp. Hub consumes them by exact Git revision. Dependency `ticket_1788287678_207209` is closed; the roll target is `origin/main` head `48a4370`, which is the last commit of that ticket's direct merge. Implement must re-verify `git -C <core> branch -r --contains 48a437032791e678010254708259568ce4ad02bf` includes `origin/main` and that no newer Core commit exists at roll time. If a newer commit exists, stop and ask a human which SHA closes the parent; do not silently roll to a newer head.
- **botster-hub** owns concrete Unix and WebRTC framing, adapters, header validation, the bounded opaque ingress buffer, admission, and route lifecycle. This ticket adds bounded WebRTC ciphertext reassembly because maximum Core frames exceed one serialized WebRTC application frame.
- **botster-hub-client** (in-repo crate) only moves its manifest pin. Downstream Git consumers (TUI) resolve that manifest and must roll with it; that is their tickets' work.
- **botster-web / botster-tui** consume the Core client helpers and this Hub roll. No dependency registration is needed from this ticket toward them; they already depend on the parent. Implement records the merged Hub SHA so those tickets can pin it.
- No new dependency ticket is required. If the 1 MiB live paste exposes a Core PTY write defect (see Risks), register a Core ticket against `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`, add it as a dependency here, and do not add a Hub workaround.

## Assumptions and unknowns

- Assumption: `48a437032791e678010254708259568ce4ad02bf` is the exact SHA that closed the parent. Evidence: it is `origin/main` head, authored 2026-09-01 22:02:42 -0700, and the parent run closed at epoch 1788326508 (about 19 minutes later). Project Pipelines stores no PR link because Core merges directly. Implement must confirm no later Core commit before rolling.
- Assumption: `head -c 1048576` after `stty raw -echo` sinks exactly the paste bytes on macOS and Linux PTYs. Raw mode disables canonical line limits, `ICRNL`, `ISIG`, and echo. If a platform quirk appears, switch the sink to a Python script using `tty.setraw` and `sys.stdin.buffer.read(N)`; do not shrink the payload.
- Assumption: Core's worker writes 1 MiB into the PTY without a partial write under `head -c` consumption. Core proved 70,012 bytes live. A partial write surfaces as `input_result` with a rejection plus `core_adapter_closed` (Core's `partial_paste_write_delivers_result_then_hard_stops_only_that_owner`). That outcome is a Core finding, not a Hub fix.
- Assumption: with the driver running, 19 back-to-back frames never exceed 64 buffered frames. Core intakes up to 64 frames per tick per subscription, so the live arm cannot latch `Lost`; the paused arm proves the hold independently of timing.
- Unknown: exact wall time for the 1 MiB PTY write through `head -c`. Bound the drain window at 20 s (same as `wait_for_subscription_closed`) and treat wall-clock as observation only.
- Unknown: whether `cargo update` also bumps `botster-core` / `botster-core-daemon` `version` fields in `Cargo.lock`. Accept whatever Cargo records for the Core family; reject any registry crate drift in the same commit.

## Affected surfaces / files

- `Cargo.toml`, `Cargo.lock`
- `crates/botster-hub-client/Cargo.toml`
- `crates/botster-hub-test-support/Cargo.toml`, `build.rs`, `src/conformance_data.rs`, `src/lib.rs` (literal only)
- `tests/session_projection_owner_loop.rs` (literal only)
- `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs`, `unix_terminal_adapter.rs`, `webrtc_terminal_adapter.rs`, `package_event_plane.rs`, `event_plane_saturation.rs` (literal only)
- `tests/hub_daemon_lifecycle/common.rs` (new `terminal_paste_frame_bytes` helper)
- `src/transport/webrtc/delivery.rs`, `src/transport/webrtc/subscription_channel.rs` (bounded opaque inbound envelope reassembly)
- `src/local_webrtc_smoke.rs` (Hub-owned smoke client sends version 2 terminal delivery chunks)
- `tests/hub_daemon_lifecycle/webrtc_fixtures.rs` (versioned inbound chunk sender)
- `tests/hub_daemon_lifecycle/paste_transaction.rs` (new: Unix live paste, WebRTC live paste, paused-hold arm, 65-frame `Lost` control, content-blind source guard)
- `tests/hub_daemon_lifecycle_test.rs` (one `include!` line)
- `docs/client-protocol.md` (one sentence), `README.md` (one clause if applicable)
- `docs/plans/pin-core-paste-transaction-frames-and-prove-live-multi-frame-paste.md` (this plan)

The only production source change is bounded opaque WebRTC inbound reassembly. Human answer `question_1788330311_325612` approved this deviation.

## Risks

- **Mixed pins.** Missing one of the 18 literal sites or 6 lock sources leaves two Core identities. Mitigation: the post-roll zero-match grep and `tests/session_projection_owner_loop.rs`.
- **Core 1 MiB PTY write path unproven.** Core's live proof stopped at 70 KB. Mitigation: the byte-exact sink plus `bytes_written` assertion; on failure, file a Core ticket and register the dependency rather than pace or split in Hub.
- **WebRTC envelope fragmentation.** A maximum Core frame exceeds the WebRTC serialized application-frame bound after encryption. Mitigation: one bounded versioned reassembly per ordered subscription route, strict index/count/length checks, fail-closed malformed controls, and a live maximum-frame red-on-revert proof.
- **Fixture race before raw mode.** Bytes sent before `stty raw` would be cooked and echoed. Mitigation: wait for `paste-sink-ready`, which the child prints only after `stty` returns.
- **Timing-only hold proof.** A live burst alone cannot show the buffer held 19 frames. Mitigation: the paused arm holds Core intake deterministically; the 65-frame control proves the `Lost` oracle fires.
- **Test-side paste encoder drift.** A hand-encoded frame that disagrees with Core is rejected by Core and shows as a non-admitted result. Mitigation: the byte-exact sink and admitted result assertions; keep the encoder next to the existing `terminal_mode_gated_frame_bytes`.
- **Guard false positives.** `bracketed_paste` appears in production DTO code. Mitigation: forbid the specific paste-transaction tokens listed above, not the word `paste`.
- **Sibling pin roll.** `ticket_1788206393_323469` could roll the pin separately. Its prerequisite is already inside `e5a927c`; if it later lands, it must rebase onto this roll. Review must renew after any semantic rebase.
- **Suite overlap.** Source-guard ablations and the pre-roll red arm must finish before the official `./test.sh --locked` run starts.

## Acceptance checks / tests

Run every Rust gate with `RUSTUP_TOOLCHAIN=1.97.0` and record `rustc --version`. Run from the colon-free worktree with `CARGO_TARGET_DIR` unset.

Dependency evidence:

```sh
git -C ~/Projects/botster-core fetch origin
git -C ~/Projects/botster-core rev-parse origin/main          # expect 48a437032791e678010254708259568ce4ad02bf
git -C ~/Projects/botster-core branch -r --contains 48a437032791e678010254708259568ce4ad02bf   # includes origin/main
grep -rn e5a927c31d5b7d0b0f4b198e5e556ed75d53ddf1 --exclude-dir=target --exclude-dir=node_modules --exclude-dir=.git . | grep -v '^./docs/plans/\|^./docs/reports/'   # expect no output
grep -c 48a437032791e678010254708259568ce4ad02bf Cargo.lock     # expect 6
```

Focused live proofs (each must pass at default concurrency inside the lifecycle suite and alone):

```sh
./test.sh --test hub_daemon_lifecycle_test unix_paste_transaction_delivers_one_result_and_byte_exact_pty_content -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test webrtc_paste_transaction_delivers_one_result_and_byte_exact_pty_content -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test paused_ingress_holds_nineteen_paste_frames_without_lost -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test paused_ingress_sixty_fifth_frame_latches_lost_and_closes_only_that_route -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test hub_transport_source_stays_paste_blind -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test paste_blind_guard_fails_on_seeded_eof_token -- --exact --nocapture
./test.sh -p botster-hub --lib transport::webrtc::delivery::tests::terminal_input_assembly_reassembles_one_large_opaque_envelope -- --exact --nocapture
./test.sh -p botster-hub --lib transport::webrtc::delivery::tests::terminal_input_assembly_fails_closed_on_malformed_or_unbounded_chunks -- --exact --nocapture
```

Required assertions inside those tests: 19 frames encoded; one and only one `input_result` for the operation id with `admitted:true` and `bytes_written:1048576`; sink file equals payload; `paste-sink-done` observed; no `TerminalSubscriptionClosed` for the paste route; WebRTC control channel terminal frame count is 0; control arm emits `core_adapter_closed` for exactly one route while the sibling stays live.

Red arms (record command and failure text):

- Pre-roll red: on a temporary commit that restores the `e5a927c` manifests and lock, run the Unix live test once. Expected failure: Hub rejects kind `4` at `push_complete`, closes the route, and the test sees `TerminalSubscriptionClosed` or no `input_result`. Restore the roll afterwards; never use bare `git stash`.
- Source-guard red: covered permanently by `paste_blind_guard_fails_on_seeded_eof_token` (seeded token after the final `cfg(test)` block of a scratch `ingress.rs`; the helper scans the scratch root). No manual source mutation is needed, so it cannot overlap the official suite.
- WebRTC framing red: bypass inbound reassembly and send one encrypted maximum-valid Core chunk frame as the prior single DataChannel message. The live WebRTC paste test must return `operation_incomplete`; restore the bounded reassembly before official gates.

Official gates (after all ablations are restored and `git status` shows only intended changes):

```sh
RUSTUP_TOOLCHAIN=1.97.0 rustc --version
RUSTUP_TOOLCHAIN=1.97.0 cargo build --locked -p botster-core-daemon --bin botster-session-worker
RUSTUP_TOOLCHAIN=1.97.0 cargo build --locked --bin botster-hub
RUSTUP_TOOLCHAIN=1.97.0 cargo fmt --all -- --check
RUSTUP_TOOLCHAIN=1.97.0 cargo clippy --workspace --all-targets --locked -- -D warnings
RUSTUP_TOOLCHAIN=1.97.0 ./test.sh --locked
```

Provenance to record in the implementation report: Hub commit SHA, locked Core SHA `48a4370…`, `realpath target/debug/botster-hub` and `target/debug/botster-session-worker`, and the clean tracked worktree state before the suite.

Downstream proof: none required by the charter for a Hub pin roll. Record the merged Hub SHA so `ticket_1787600676_914408` (Web) and `ticket_1787603674_865638` (TUI) can pin it.

## Pipeline gates and artifacts

- Plan gate evidence includes `plan_uri`, `artifact_id`, `checklist_id`, `target_id`, and `target_repository`.
- Implement attaches the pre-roll red output, the eight focused test outputs, the WebRTC framing red output, the zero-match grep, the official gate outputs, and binary provenance.
- Review should reject: production changes outside bounded opaque WebRTC inbound reassembly, a payload below 65,536 bytes or fewer than 19 Core frames, an `input_result` oracle that accepts any count other than one, a hold proof that relies on wall clock without the paused arm, a fixed source-guard file list, a guard that skips any file region, a guard red control that does not scan the mutated root, or a partial pin roll.
- Implement must rebase onto current `origin/main` before the official gate and renew review if that rebase is semantic.

## Vault gaps worth capturing

- [[Hub Core pin rolls update eleven literal sites and six lock sources]] is stale: this tree has 18 active literal sites, including `LOCKED_CORE_REV` in `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` and the `botster_core` provenance literal in `tests/hub_daemon_lifecycle/event_plane_saturation.rs`. Capture the corrected inventory or replace the count with the zero-match grep invariant.
- No vault note records the inbound WebRTC size mismatch. A maximum Core frame fits Core but its encrypted JSON envelope exceeds `LOCAL_WEBRTC_MAX_FRAME_BYTES`. Capture the opaque transport reassembly rule.
- New durable rule candidate: a Hub multi-frame ingress proof needs a paused-intake hold arm plus a `MIN + 1` `Lost` control; a live burst alone proves only timing.
- `adapter_slot.rs` carries `#[allow(dead_code)]` ingress seams (`inject_ingress_frame`, `inject_ingress_partial`, `complete_ingress_partial`, `drop_buffered_ingress_frame`) with no production or test consumer visible from this ticket. Capture a disposition note per [[unused test environment seams signal skipped acceptance checks]] or open a cleanup ticket.
- The Core-side fact that `bytes_written` counts paste content bytes and excludes bracketed-paste wrapper bytes is not in the vault.
