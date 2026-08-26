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
| Locked Core SHA | `358ef1a6bf0f792f6da10d60890be39cb16779d0` |
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

One RTCPeerConnection still owns one reliable ordered control DataChannel (`botster-client`). Attach authorizes, assigns a generation, charges section 9, inserts a `Reserved` route, and returns the exact label. The browser creates that labeled terminal DataChannel after admission. Open validates identity, generation, peer, and route, then binds a content-blind Core adapter. Late, stale, mismatched, duplicate, unreserved, and over-limit opens close without bind. A `Reserved` route that never opens retires with `subscription_channel_open_timeout` and no `local_close`. Terminal output no longer shares the control write path.

## Review-return repairs (`review_1787700610_987769`)

This visit repaired the eight open Review findings without changing ticket intent.

- `finding_1787700610_777730`: every `run_subscription_channel` exit runs bounded `local_close`.
- `finding_1787700610_544227`: the control loop sleeps until the nearest reserved-open deadline, then expires and sweeps.
- `finding_1787700610_189357`: subscription flush routes `OnMessage` to terminal ingress. It does not parse Hello or Request.
- `finding_1787700610_327965`: reservations are keyed by grant. Bind requires the exact grant. Peer close, timeout, and replacement sweep the side table.
- `finding_1787700610_919731`: live `try_write` stays one slot. A refused live write does not become Hub history. Attach bootstrap still uses a finite remainder so Core attach egress is not dropped.
- `finding_1787700610_543030`: subscription send copies `DataChannel::outstanding_bytes()` into the mux aggregate.
- `finding_1787700610_979135`: subscription output uses binary `send()`. Control stays text JSON.
- `finding_1787700610_773390`: Linux Clippy `u32::from(st_mode)` is now `file_mode_bits`.

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

## Official gates

First Implement visit, same shell, `RUSTUP_TOOLCHAIN=1.97.0`, `CARGO_TARGET_DIR` unset:

| Command | Result |
| --- | --- |
| `rustc --version` | `rustc 1.97.0 (2d8144b78 2026-07-07)` |
| `zig version` | `0.16.0` |
| `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | pass |
| `cargo build --locked --bin botster-hub` | pass |
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass after the last repair |
| `node packages/hub-test-support/scripts/sync-assets.mjs --check` | `hub test-support package assets are current` |
| `./test.sh --locked` | first visit: exit 0. Hub lib 504 passed. Lifecycle 317 passed, 2 ignored. Elapsed 480616 ms. Review return: exit 0. Hub lib 512 passed. Lifecycle 317 passed, 2 ignored. Elapsed 450573 ms |
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

- `Cargo.lock`, `Cargo.toml`, `README.md` — Core pin `358ef1a` and workspace lock sources
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
- `src/daemon_attach_stream.rs` — reserved-adapter bind
- `src/daemon_transport.rs` — Attach reserve, grant-owned reservation, bind, peer-close sweep
- `src/local_webrtc.rs` — open-event bind, control isolation, subscription close, binary send, reserved deadline
- `crates/botster-hub-installation/src/safety.rs` — Linux Clippy mode-bit conversion
- `src/runtime.rs`
- `src/unix_terminal_adapter.rs`
- `src/webrtc_subscription_channel.rs` — new module: labels, predicates, framing, flush
- `src/webrtc_terminal_adapter.rs` — reserve, charge, expire, aggregate
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

The plan named a 200 ms test open bound. This return uses 5 s in tests so the new deadline timer does not retire a reserved route during Attach-to-open.

Attach bootstrap still keeps a finite remainder on the adapter. Live `try_write` does not use that remainder. Review asked Hub not to store live terminal history. Core attach egress is consumed at Attach, so Hub must keep those frames until flush.

A25, A26, and A27 live 31-channel fill to exactly 2,097,152 B is proved at the mux unit layer (`a25_a26_a27_exact_aggregate_ceiling_refuses_before_write_and_drains_to_zero` and `live_outstanding_bytes_drive_thirty_one_channel_aggregate`). This visit did not add a live IsolatedHub 31-channel byte fill.

A27b Core `WRITE_ATTEMPT_BUDGET` hard-stop is proved by the live write-budget test `webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable`, not by holding a full 2,097,152 B aggregate for 512 attempts.

## unverified_behavior_or_residual_risk

- Site 3 is unpublished. Downstream Web/TUI tickets cannot consume `0.1.43` until a human publishes it and inspects the coordinate.
- Live A25/A26/A27 31-channel IsolatedHub fill is not in this suite. Mux-level predicates and the live write-budget sibling test are.
- Default-concurrency official `./test.sh --locked` passed once after the lifecycle repairs. This visit did not require a second identical suite run.
- Browser and TUI channel-creation clients remain owned by their downstream tickets.

## missing_vault_guidance

None that blocked the remaining repairs. The Node mirror note and the three section 11.3 notes were present and applied.

## Runtime behavior not verified

- No live browser attach through Botster Web.
- No live TUI attach through the TUI package.
- No npm registry publish of `0.1.43`.
