# Implement report: isolate control and terminal subscriptions on dedicated DataChannels

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | `botster-hub` |
| Pipeline worktree | the pipeline-provided ticket worktree |
| Ticket | `ticket_1787600674_500120` |
| Run | `run_1787678814_340532` |
| Step | `botster_stack_implement` |
| Approved plan | `docs/plans/isolate-control-and-terminal-subscriptions-on-dedicated-datachannels.md` revision 4 |
| Plan commit | `307ff70` |
| Base commit | `a0c7141` on `main` |
| Locked Core SHA | `9cabdfd0588b6c7ed2e121e7b50086ce2a250ec6` |
| Merge policy | pull request |
| Test-support coordinate | unpublished `@trybotster/hub-test-support@0.1.43` |
| Conformance fixture revision | 47 |
| Protocol version | 7 (unchanged) |

Routing used the run `target_id`, not the process working directory. Implementation stayed in this run worktree. The ambient Hub checkout was not edited.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]

Required charter/context notes:

- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]] — loaded because the implementer overlay requires it; this ticket has no React or SPA edit surface

### Targeted atomic notes

The approved plan section 2 lists 34 targeted notes plus the five gate-hygiene notes. This visit applied that inventory. The load-bearing names for the remaining work were:

- [[botster subscriptions use dedicated ordered DataChannels]]
- [[the browser creates each subscription DataChannel after Hub reserves its label]]
- [[rejected channel isolation needs a surviving channel positive control]]
- [[core owns duplex terminal transport while Hub stays content blind]]
- [[Hub test support version bumps must update the Node mirror test literals]]
- [[hub generated protocol changes are a four site release chain]]
- [[tui shaped Hub consumer proofs must include hub test support]]
- [[clean consumer smokes resolve exported root entrypoints not package json]]
- [[a Cargo source identity proof needs a wrong tag ablation]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[test script required for rust tests not cargo test]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

### Explicitly not loaded

- [[project-pipelines-playbook]] — this ticket changes no Project Pipelines package or plugin path

### Constraints applied before edits

- Edit only the run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Follow the approved plan. Do not absorb entity/event channels, Unix duplex bind, or JSON handler deletion.
- Keep `DaemonRequest::SendInput`, `ModeGatedInput`, and `Resize` until `ticket_1787600679_990088`.
- Charge a subscription slot at `Reserved`. Permit 32 charged routes. Use `> 32`, never `>= 32`.
- Do not set `CARGO_TARGET_DIR`.
- Do not claim npm publish of `0.1.43`.
- After every Clippy repair, rerun the full clippy gate.

## What landed

One RTCPeerConnection still owns one reliable ordered control DataChannel (`botster-client`). Attach authorizes, assigns a generation, charges section 9, inserts a `Reserved` route, and returns the exact label. Admission does not call Core `attach` or `bind_terminal_adapter`. The browser creates that labeled terminal DataChannel after admission. Open validates identity, generation, peer, and route, then Core-attaches and binds the content-blind adapter. Late, stale, mismatched, duplicate, unreserved, and over-limit opens close without bind. A `Reserved` route that never opens retires with `subscription_channel_open_timeout` and does not create a Core owner. Terminal output no longer shares the control write path.

## Review-return repairs (`review_1787700610_987769`)

This visit repaired the eight open Review findings without changing ticket intent.

- `finding_1787700610_777730`: every `run_subscription_channel` exit runs bounded `local_close`.
- `finding_1787700610_544227`: the control loop sleeps until the nearest reserved-open deadline, then expires and sweeps.
- `finding_1787700610_189357`: subscription flush routes `OnMessage` to terminal ingress. It does not parse Hello or Request.
- `finding_1787700610_327965`: reservations are keyed by grant. Bind requires the exact grant. Peer close, timeout, and replacement sweep the side table.
- `finding_1787700610_919731`: live `try_write` stays one slot. A refused live write does not become Hub history. The later Review return removed the Hub bootstrap remainder.
- `finding_1787700610_543030`: subscription send copies `DataChannel::outstanding_bytes()` into the mux aggregate.
- `finding_1787700610_979135`: subscription output uses binary `send()`. Control stays text JSON.
- `finding_1787700610_773390`: Linux Clippy `u32::from(st_mode)` is now `file_mode_bits`.

## Review-return repairs (`review_1787706019_175557`)

This visit repaired the seven open Review findings without changing ticket intent. Prior eight findings from `review_1787700610_987769` stay resolved.

- `finding_1787706019_121460`: Hub no longer stores attach dump or live frames in a `Vec` or `VecDeque`. `WebrtcReservedAttach` keeps only `grant_id`. `write_opaque_frame` uses one-slot `try_write`. Extra attach frames return `Full` and drop.
- `finding_1787706019_694288`: `refresh_subscription_outstanding` copies live `DataChannel::outstanding_bytes()` after send, after channel events, and on close. Close sets buffered bytes to 0. `live_outstanding_bytes_drain_to_zero_on_low_water` drives HIGH to LOW-1 to 0.
- `finding_1787706019_269972`: `subscription_flush_keeps_terminal_input_off_the_control_parser` encrypts the payload and asserts `TerminalIngress::Frame(b"core-terminal-input")` with an empty control parser. `subscription_ingress_reports_lost_after_sixty_four_frames` covers the 64-frame Lost bound.
- `finding_1787706019_408429`: `reserved_open_deadline_wakes_the_control_loop_and_sweeps` runs production `run_data_channel` against `std::time::Instant`. It waits for `SweepWebrtcReservation`, applies that message, and asserts no `local_close` before cleanup.
- `finding_1787706019_627428`: `webrtc_reservation_control_messages_keep_foreign_grants` uses two live signaled peers. Bind-first, timeout-first, PeerClosed-first, and Sweep-after-PeerClosed remove only grant A.
- `finding_1787706019_286286`: installer `install.rs` compares `facts.mode` to exported `file_mode_bits(ARTIFACT_MODE)` instead of `u32::from(ARTIFACT_MODE)`. Full workspace Clippy `-D warnings` passed after that repair.
- `finding_1787706019_555946`: this report and PR 210 record the new head, the 517/317 counts, and the Clippy repair.

Same-client re-attach and cross-client steal both detach the live row and reserve `generation + 1`. Reuse of generation 1 returned `OperatorError` on host-close Attach.

## Review-return repairs (`review_1787719554_794236`)

This visit repaired the three open Review findings without changing ticket intent.

- `finding_1787719554_916665`: Attach only reserves. `BindReservedWebrtcChannel` calls `begin_core_attach` and `bind_terminal_adapter` after identity, generation, peer, and route checks. `spawn_and_attach_on_peer` asserts Core inventory has no owner before open.
- `finding_1787719554_607050`: Core `attach()` extracts dump only after open validation. Hub delivers that finite dump through `attach_handoff`, one frame after each slot clear. Live `try_write` stays one slot. `attach_handoff_delivers_the_second_frame_after_the_slot_clears` goes red if the second frame drops.
- `finding_1787719554_253516`: GitHub `verify` now runs `cargo build --locked -p botster-core-daemon --bin botster-session-worker` and `cargo build --locked --bin botster-hub` before `./test.sh --locked`.

## Core consume (`ticket_1787768219_768283`)

Core PR https://github.com/trybotster/botster-core/pull/122 merged as `9cabdfd0588b6c7ed2e121e7b50086ce2a250ec6`. This Hub visit now consumes that contract.

- Hub pins Core `9cabdfd`.
- Reserved-channel open calls `expect_terminal_adapter` before `attach`, then binds and marks a Core pump.
- Hub no longer writes extracted attach residue (`write_first_webrtc_attach_frame` removed).
- Isolated `webrtc_reserved_bind_delivers_ready_history_finish_attached_in_order` proved READY, HISTORY, FINISH, and Attached on the bound adapter, in order, without a Hub queue.
- Isolated `webrtc_terminal_adapter_attach_emits_a_nonempty_frame_without_host_drain` and `external_hub_webrtc_live_output_preserves_exact_bytes` passed.

`finding_1787763686_257426` is resolved by the Core hold plus Hub `expect_terminal_adapter`. Hub no longer drops extracted attach frames.

This consume also repaired IsolatedHub bind and pump races that were not a Hub attach queue:

- `snapshot_write_for` and `pressure()` drop one lock before taking the other. The writer no longer deadlocks the owner loop.
- Reserved open marks the route in flight so the 5 s never-open sweep cannot retire a channel that already opened.
- Bind after open waits `LOCAL_WEBRTC_RESERVED_BIND_REPLY_BOUND` (30 s). That wait is not the never-open sweep.
- After a reserved flush, Hub `send`s `ReservedWebrtcSlotReady` so the owner loop observes that session until the one slot is occupied.
- Journal remakes do not consume a journal wake while every live bound WebRTC slot is full. A leftover full slot on one mux does not block a ready slot on another mux.
- While a recently bound session still has an empty slot, owner-loop reconcile ticks every 50 ms. This is not a tight Observe remake. A full slot returns the owner loop to the 500 ms interval so the WebRTC flush thread can run.
- Live-output proofs wait for `attach_state:attached` before they release the producer. Bytes written before Attached are HISTORY.
- Those proofs do not poke Observe over Unix or encrypted WebRTC while dump is in flight. Unix `ReadScreen` during dump closed the reserved channel.

`finding_1787763686_945566` is resolved. Isolated `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup` passed five sequential IsolatedHub rounds. Official `./test.sh --locked` then passed that proof and `external_hub_webrtc_live_output_preserves_exact_bytes` under default parallel load.

## Review-return repairs (`review_1787763686_531984`)

Question `question_1787768250_921634` required this Hub visit to wait for Core. Core ticket `ticket_1787768219_768283` is closed. Merge commit `9cabdfd0588b6c7ed2e121e7b50086ce2a250ec6`. Dependency `dependency_1787768226_190452` is closed.

Ownership rule kept on this Hub branch:

- Core owns READY, HISTORY, FINISH, Attached, ordering, pressure, and lossless attach delivery.
- Hub must not add an attach-frame queue.
- Hub must not retain a hidden dump or tail.
- Hub must not drop extracted attach frames.
- Hub must bind only through the corrected Core contract.

Unix attach still does not call `expect_terminal_adapter`. Unix duplex bind is out of scope.

`review_1787763686_531984` stays closed. This visit does not waive `review_1787786109_244719` or `review_1787799293_532860`. It requests a new Review after official green.

## Review-return repairs (`review_1787799293_532860`)

This visit repaired the open Review finding without changing ticket intent.

- `finding_1787799293_644143`: GitHub Verify stalled `external_hub_webrtc_live_output_preserves_exact_bytes` and `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup` after READY because queued host/web control won every owner turn and skipped the 50 ms bind-observe tick. The owner loop now exact-observes an empty just-bound WebRTC slot when that queued control wins. That is not a tight Observe remake on every turn. `owner_loop_observes_in_flight_webrtc_binds_when_queued_control_wins` pins the control-arm placement. HISTORY IsolatedHub and its negative control stay.

## Review-return repairs (`review_1787786109_244719`)

This visit repaired the two open Review findings without changing ticket intent.

- `finding_1787786109_679721`: `reserved_bind_history_pages_hold` requires at least one HISTORY page between READY and FINISH. `reserved_bind_attach_order_holds` also requires Attached after FINISH. The IsolatedHub producer uses Core's 2000-line default 24x80 recipe and waits for that dump to finish before Attach. It does not shrink to 2x80. A 2-row viewport multiplied Ghostty HISTORY pages through the one-slot adapter and stalled sibling IsolatedHub proofs under official load. `webrtc_reserved_bind_delivers_ready_history_finish_attached_in_order` waits until READY, HISTORY, FINISH, and Attached all hold. `webrtc_reserved_bind_history_assertion_fails_when_only_history_is_dropped` keeps READY, FINISH, and Attached, drops only HISTORY, and fails at the HISTORY assertion.
- `finding_1787786109_913378`: PR 210 records the new official local suite counts for this head and keeps GitHub Verify separate from that local result.

A 2x80 / 1000-line IsolatedHub producer was attempted first. Isolated HISTORY often finished in about 3 s, but official `./test.sh --locked` then lost live bytes or Attached on sibling IsolatedHub proofs. Two leaked worktree `botster-hub` processes were also spinning at ~100% CPU from earlier overlapping suites. Those were killed before the green official run. Live-output now issues a production Status observe after the go-file release so post-Attached bytes do not wait on a stale bind-observe window.

## Review-return repairs (`review_1787760007_932950`)

This visit repaired the two open Review findings without changing ticket intent.

- `finding_1787760007_162985`: Hub no longer stores attach dump in `attach_handoff`. After open validation, Core attach and bind run, then Hub writes at most one already-extracted attach residue into the one-slot adapter. Later incremental and live frames stay in Core and enter the adapter only when `try_write` returns Ready. Bind marks a Core pump so the owner loop observes after the reserved channel is live.
- `finding_1787760007_595176`: The Hub queue no longer reports `Full` while leftover dump sits off-slot, so live Core writes are not blocked. Isolated `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup` and `external_hub_webrtc_live_output_preserves_exact_bytes` passed after the removal.

Clients that still send input after Attach must open the reserved label first. The local WebRTC smoke offerer and the oversized encrypted-response proof now do that.

`LOCAL_WEBRTC_CHANNEL_OPEN_BOUND` is 5 s in tests and production. A 200 ms test timer retired reserved routes before the browser opened the channel.

Product repairs after the first official lifecycle failures:

- Hub answerer PeerConnections set a per-channel send-buffer limit of 256 KiB so a paused inbound peer can keep one adapter slot `Full`.
- Unnegotiated control peers skip both `TerminalSubscriptionClosed` and `SubscriptionChannelOpenTimeout` host events.
- `expire_reserved_opens` queues `SubscriptionChannelOpenTimeout` only when close events are admitted.

Test repairs:

- Hello-bind and lifecycle fixtures open the reserved label after Attach.
- Terminal-frame collection polls only the matching subscription channel.
- Write-budget sibling collection no longer drains the stalled channel.
- Node mirror literals now assert conformance revision 47.

## Review-return repairs (`review_1787801253_862883`)

Question `question_1787803324_624838` chose option 2: keep iterating Hub owner-loop scheduling. Do not restore exact observation on Status, ListSessions, or generic control. Do not park for Core. Do not only lengthen waits.

`finding_1787801254_356028` stays accepted: generic control does not exact-observe in-flight WebRTC binds. `scheduled_webrtc_bind_observe_is_not_driven_by_generic_control` still forbids `observe_session_lifecycle` in Status/ListSessions and in the generic control arm.

The remaining official defect was SlotReady starvation under load, not Core attach retain. After the first attach frame fills the one-slot adapter, the 50 ms empty-slot tick stops and journal remakes stop. The next Core write needs a SlotReady wake. That wake used `runtime_tx.send().await` on the same FIFO as Status/ListSessions. Continuous generic control delayed the wake; a full control queue could park the DataChannel loop.

This visit:

- Coalesces SlotReady onto a shared `WebrtcSlotReadyWake`. Duplicate notes for one session stay one key. A failed `try_send` does not drop the mark.
- The DataChannel loop `try_send`s `ReservedWebrtcSlotReady` and never awaits enqueue.
- `mark_due_reconciliation` marks Pump when a coalesced SlotReady is pending.
- Every owner turn drains coalesced SlotReady after control, outside generic request arms. Pump still drains it.
- A SlotReady taken while the one-slot is Full is restored, not dropped.
- SlotReady remains the route-specific observe opportunity.

Deterministic red controls:

- `coalesced_webrtc_slot_ready_marks_pump_and_coalesces_without_lost_wakeup`
- `ready_webrtc_route_progresses_within_two_background_slices_under_queued_control`
- `slot_ready_wake_coalesces_duplicate_notes_without_dropping_the_mark`
- `slot_ready_does_not_block_the_datachannel_loop_on_control_send`
- IsolatedHub `GenericControlFlood` of Status and ListSessions around reserved bind for HISTORY, Attached-without-host-Status, exact-bytes, shutdown, and stale-generation replacement. Those proofs do not use Status, ListSessions, or ReadScreen as the attach observe path.

Official `./test.sh --locked` on this head: exit 0. Hub lib 528 passed. Lifecycle 320 passed, 2 ignored. Lifecycle 301.80 s. `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup` and `webrtc_terminal_adapter_stale_generation_close_does_not_sweep_replacement_owner` passed in that full-suite order.

## Review-return repairs (`review_1787807102_632470`)

This visit repaired the two open Review findings without changing ticket intent.

- `finding_1787807102_177084`: `ReservedWebrtcSlotReady` is a doorbell only. Coalesced drain observes empty slots. A full slot keeps the session key but `mark_due_reconciliation` does not mark Pump until a slot is empty. The owner loop Blocks and may schedule a 50 ms recheck. `full_webrtc_slot_does_not_keep_the_owner_loop_runnable` holds a live bound slot Full and proves Pump is not forced.
- `finding_1787807102_232658`: `webrtc_reserved_bind_reaches_attached_without_host_status` no longer starts `GenericControlFlood`. It binds and waits for Attached with no Status, ListSessions, or ReadScreen after bind. Continuous generic control remains a separate IsolatedHub load on the stale-generation replacement proof.

`finding_1787801254_356028` stays accepted: generic control still does not exact-observe. The no-control IsolatedHub proof now matches that claim.

## Official gates

First Implement visit, same shell, `RUSTUP_TOOLCHAIN=1.97.0`, `CARGO_TARGET_DIR` unset:

| Command | Result |
| --- | --- |
| `rustc --version` | `rustc 1.97.0 (2d8144b78 2026-07-07)` |
| `zig version` | `0.16.0` |
| `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | pass |
| `cargo build --locked --bin botster-hub` | pass |
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass after the installer `file_mode_bits` repair |
| `node packages/hub-test-support/scripts/sync-assets.mjs --check` | `hub test-support package assets are current` |
| `./test.sh --locked` | first visit: exit 0. Hub lib 504 passed. Lifecycle 317 passed, 2 ignored. Elapsed 480616 ms. First Review return: exit 0. Hub lib 512 passed. Lifecycle 317 passed, 2 ignored. Elapsed 450573 ms. Second Review return: exit 0. Hub lib 517 passed. Lifecycle 317 passed, 2 ignored. Elapsed 437289 ms. Third Review return: exit 0. Hub lib 519 passed. Lifecycle 317 passed, 2 ignored. Elapsed 397602 ms. Fourth Review return: exit 0. Hub lib 518 passed. Lifecycle 317 passed, 2 ignored. Elapsed 403168 ms. Core consume: exit 0. Hub lib 523 passed. Lifecycle 318 passed, 2 ignored. Elapsed 392450 ms. HISTORY IsolatedHub oracle: exit 0. Hub lib 523 passed. Lifecycle 319 passed, 2 ignored. Lifecycle 340160 ms. Wrapper elapsed 434720 ms. In-flight bind observe: exit 0. Hub lib 524 passed. Lifecycle 319 passed, 2 ignored. Lifecycle 304380 ms. Wrapper elapsed 384770 ms. Owner-loop SlotReady coalescing: exit 0. Hub lib 528 passed. Lifecycle 320 passed, 2 ignored. Lifecycle 301800 ms. Full-slot Pump gating: exit 0. Hub lib 529 passed. Lifecycle 320 passed, 2 ignored. Lifecycle 308860 ms |
| `cd packages/hub-test-support && npm install --no-save && npm test` | `hub test-support package import and fixture materialization passed` |
| `git diff --check a0c7141...HEAD` | pass |

Named former official failures, all green on the locked suite:

- `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup`
- `webrtc_peer_rejects_a_second_data_channel`
- `webrtc_terminal_adapter_bound_peer_loss_closes_adapter_without_hub_detach`
- `webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable`

## Section 11.3 downstream proofs

Sites 1 and 2 are in this ticket. Site 3 is unpublished. Site 4 belongs to downstream consumer tickets.

**TUI-shaped Cargo proof.** A scratch crate outside the Hub workspace declared `botster-hub-client` and `botster-ui-contract` as normal dependencies and `botster-hub-test-support` as a dev-dependency. `cargo build --tests` compiled the dev edge.

Matching tag `botster-ui-contract-v0.3.3`:

- `cargo build --tests` passed.
- `cargo test` passed `reserved_channel_tokens_are_constructible` and `ui_node_crosses_the_hub_client_boundary`.
- `cargo tree -i botster-ui-contract -e normal,dev` showed one package: `botster-ui-contract v0.3.3` at tag `botster-ui-contract-v0.3.3#12e0cc69`, reached from the consumer, `botster-hub-client`, and `botster-hub-test-support`.

Wrong-tag ablation changed only the consumer's direct pin to `botster-ui-contract-v0.3.2`:

- Cargo compiled two `botster-ui-contract` packages: `v0.3.2#0775e661` and `v0.3.3#12e0cc69`.
- `cargo build --tests` failed with `E0308` at the `DaemonPluginSurface.body` boundary.

**Web-shaped packed-tarball proof.** `npm pack` produced unpublished `@trybotster/hub-test-support@0.1.43`. A clean scratch consumer installed that tarball. Resolution used exported roots, not `package.json`. `require.resolve("@trybotster/hub-test-support/package.json")` failed with `ERR_PACKAGE_PATH_NOT_EXPORTED`. Installed metadata reported package `0.1.43`, protocol 7, revision 47, UI contract `0.3.3`. The installed `daemon-protocol.ts` SHA-256 matched `metadata.daemon_protocol.sha256`. The generated import line was:

```text
import type { PackageNoticeReactionDescriptor, PackageSurfaceDescriptor, UiActionRequest, UiActionResult, UiNode } from "@trybotster/ui-contract";
```

The installed protocol also contains `subscription_channel_label`, `subscription_channel_generation`, and `subscription_channel_open_timeout`.

This ticket does not publish `0.1.43`.

## files_changed

Branch paths relative to `a0c7141`, including this report:

- `Cargo.lock`, `Cargo.toml`, `README.md` — live Core pin `9cabdfd`; historical plan text still names `358ef1a`
- `crates/botster-hub-client/Cargo.toml`
- `crates/botster-hub-client/generated/daemon-protocol.ts` — reservation label, generation, open-timeout event
- `crates/botster-hub-client/src/lib.rs`
- `crates/botster-hub-client/src/typescript.rs`
- `crates/botster-hub-test-support/Cargo.toml`
- `crates/botster-hub-test-support/build.rs`
- `crates/botster-hub-test-support/src/conformance_data.rs`
- `crates/botster-hub-test-support/src/lib.rs`
- `docs/client-protocol.md`
- `docs/plans/freeze-subscription-ownership-and-capture-the-regression-baseline.md` — architecture section 8.2
- `docs/plans/isolate-control-and-terminal-subscriptions-on-dedicated-datachannels.md` — approved plan
- `docs/reports/isolate-control-and-terminal-subscriptions-on-dedicated-datachannels-implement.md` — this report
- `packages/hub-test-support/*` — unpublished `0.1.43`, revision 47, Node mirror literals
- `src/daemon_attach_stream.rs` — predicted reservation generation, Core-bind only after open. `write_first_webrtc_attach_frame` removed
- `src/daemon_transport.rs` — Attach reserves only. Open validates, calls `expect_terminal_adapter`, then Core-attaches, binds, and observes. SlotReady stays route-specific. Coalesced SlotReady wakes mark Pump and drain after every owner turn. Generic control does not exact-observe. Empty just-bound slots keep the 50 ms tick
- `src/local_webrtc.rs` — production Attach asserts no Core owner before open
- `.github/workflows/ci.yml` — locked session-worker and Hub prebuild before `./test.sh --locked`
- `src/local_webrtc_smoke.rs` — smoke offerer opens the reserved label after Attach
- `crates/botster-hub-installation/src/lib.rs` — export `file_mode_bits`
- `crates/botster-hub-installation/src/safety.rs` — Linux Clippy mode-bit conversion
- `crates/botster-hub-installer/src/install.rs` — compare artifact mode with `file_mode_bits`
- `src/runtime.rs`
- `src/unix_terminal_adapter.rs`
- `src/webrtc_subscription_channel.rs` — new module: labels, predicates, framing, flush
- `src/webrtc_terminal_adapter.rs` — reserve, bind at open, one-slot live write, aggregate
- `tests/hub_daemon_lifecycle/event_plane_saturation.rs`
- `tests/hub_daemon_lifecycle/package_event_plane.rs`
- `tests/hub_daemon_lifecycle/sessions.rs`
- `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs`
- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`
- `tests/hub_daemon_lifecycle/webrtc_fixtures.rs`
- `tests/hub_daemon_lifecycle/webrtc_proofs.rs`
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`
- `tests/session_projection_owner_loop.rs`

## deviations_from_plan

None that change ticket intent or acceptance ownership.

JSON input handlers remain, as answered earlier. Entity/event channels and Unix duplex bind stay out of scope.

The plan named a 200 ms test open bound. This return uses 5 s in tests and production. A 200 ms timer retired reserved routes before the browser opened the channel.

Core `attach()` no longer returns the attach dump when Hub calls `expect_terminal_adapter` first. Frames stay in Core until bind, then the next drain writes them into the one-slot adapter. Hub does not keep those frames in a queue. Unix attach still does not call expect. JSON `SendInput` and `Resize` stay, and clients must open the reserved channel before those handlers can use a Core owner.

A25, A26, and A27 live 31-channel fill to exactly 2,097,152 B is proved at the mux unit layer (`a25_a26_a27_exact_aggregate_ceiling_refuses_before_write_and_drains_to_zero` and `live_outstanding_bytes_drive_thirty_one_channel_aggregate`). This visit did not add a live IsolatedHub 31-channel byte fill.

A27b Core `WRITE_ATTEMPT_BUDGET` hard-stop is proved by the live write-budget test `webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable`, not by holding a full 2,097,152 B aggregate for 512 attempts.

## unverified_behavior_or_residual_risk

- Site 3 is unpublished. Downstream Web/TUI tickets cannot consume `0.1.43` until a human publishes it and inspects the coordinate.
- Live A25/A26/A27 31-channel IsolatedHub fill is not in this suite. Mux-level predicates and the live write-budget sibling test are.
- Core attach and adapter bind start only after the reserved channel opens. `SendInput` before that open has no Core owner.
- Unix attach still does not call `expect_terminal_adapter`. Unix duplex bind is out of scope.
- Isolated `unix_eof_skip_core_detach_ablation_keeps_named_pair_on_status` failed once under official parallel load on an earlier visit, then passed isolated. This visit did not change Unix detach. The latest official run passed that proof.
- `owner_loop_queues_and_completes_two_fanout_plugin_handlers` can flake under default lib concurrency (1 vs 2 handlers). Isolated reruns pass. The latest official suite passed.
- This visit will record the new GitHub Verify result after the push.
- Binary send still wraps JSON `DaemonLocalWebrtcDeliveryChunk`. This ticket did not add a new encrypted-binary framing DTO.
- Browser and TUI channel-creation clients remain owned by their downstream tickets.

## missing_vault_guidance

None that blocked the remaining repairs. The Node mirror note and the three section 11.3 notes were present and applied.

## Runtime behavior not verified

- No live browser attach through Botster Web.
- No live TUI attach through the TUI package.
- No npm registry publish of `0.1.43`.
