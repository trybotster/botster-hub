# Implement: expose client event subscriptions on the host control protocol

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`)
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1786663583_640263`
- Run: `run_1786867268_135671`
- Implement step: `run_step_1786869471_633205`
- Approved plan: `docs/plans/expose-client-event-subscriptions-on-the-host-control-protocol.md` at `6f1f5cf`
- `teardown_class_applies`: no
- Direct-merge pipeline. No pull request.

Independent `list_spawn_targets` maps `tgt_7e208a0c76a44980a83b63af976b1f22` to
`trybotster/botster-hub` at `/Users/jasonconigliari/Projects/botster-hub`.
This run used the pipeline worktree for that target.

## Repository playbook and other playbooks/notes applied

Role:

- [[implementer-playbook]]
- [[botster-implementer-playbook]]

Repository charters:

- [[botster-hub-playbook]]
- [[botster-hub-client-playbook]] (in-repo client crate)

Vault notes that constrained the change:

- [[current botster is a modular repository family not the legacy trybotster monorepo]]
- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster packages should enforce core hub cli plugin provider boundaries]]
- [[exact owner plus name is the only package event subscription key]]
- [[botster hub events use bounded priority lanes instead of unbounded queue fuses]]
- [[events.emit is a non-blocking router ingress not an owner-pumped host bridge]]
- [[Unix Hello can reject terminal admission while host operations remain available]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[hub generated protocol changes are a four site release chain]]
- [[generated typescript dtos must encode serde field optionality]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[hub test support npm releases need external consumer smoke]]

No loaded convention conflicted with the approved plan.

## Files changed

Public contract:

- `crates/botster-hub-client/src/lib.rs`
- `crates/botster-hub-client/src/typescript.rs`
- `crates/botster-hub-client/generated/daemon-protocol.ts`
- `docs/client-protocol.md`
- `README.md`

Host control and router:

- `src/daemon_event_subscriptions.rs` (new)
- `src/host_control_fair_write.rs` (new)
- `src/package_event_router.rs`
- `src/daemon_transport.rs`
- `src/local_webrtc.rs`
- `src/unix_terminal_adapter.rs`
- `src/webrtc_terminal_adapter.rs`
- `src/lib.rs`
- `src/main.rs`

Support package and fixtures:

- `packages/hub-test-support/*` version `0.1.38`, protocol, matrix, metadata
- `crates/botster-hub-test-support/src/lib.rs`
- `examples/event-plane-producer/botster-package.json`

Tests:

- `tests/hub_daemon_lifecycle/package_event_plane.rs`
- `tests/hub_daemon_lifecycle/sessions.rs`
- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`

## Ownership boundaries preserved

Hub owns host-control admission, client event mailboxes, fair host-control
writing, and the in-repo `botster-hub-client` crate.

`botster-hub-client` owns public DTOs, compatibility descriptors, generated
TypeScript, and conformance revision 43.

Package events stay on the multiplexed host-control path. They do not take
over a Unix socket. They do not use `EntityFrameSender`. Terminal adapter
files gained only `has_pending_event` peeks. Hub does not inspect terminal
bodies.

Host `required_features` live on `PendingRuntimeState.host_compatibility`,
keyed by Unix `client_id` or WebRTC `grant_id`. They are not read from
`UnixTerminalAdmission` or `WebrtcTerminalAdmission`.

## Cross-repo dependencies or separately routed work

Closed prerequisites:

- `ticket_1786663582_483898` (Stage B router)
- `ticket_1786661010_198387` (terminal drain cold-cut)

Registered downstream consumers, not implemented here:

- `ticket_1786663584_427840` (`botster-web`)
- `ticket_1786663585_944018` (`botster-tui`)

No Core pin change. Pin remains
`fc541a59338d0591ba4fb3fa522a030d212d26d0`.

## Deviations from plan

- Unpublished support coordinate is `0.1.38`. Registry latest published is
  `0.1.36`. Tree `0.1.37` was already reserved by the prior unpublished
  cold-cut ticket, so this ticket did not mutate that coordinate.
- The 4,096-byte subject aggregate is the product of 16 × 256 UTF-8 bytes.
  The admission test proves 4,096 unique bytes are accepted. A 4,097-byte
  case cannot occur without first violating the 16-count or 256-byte rule.
- WebRTC IsolatedHub emit used the in-process DataChannel harness plus Unix
  IsolatedHub live emit. A full IsolatedHub WebRTC emit needs `botster-web`
  bootstrap, which this ticket does not own.
- `./test.sh --locked` did not produce one clean default-concurrency pass.
  Rotating load flakes were isolated and passed. See tests below.

No accepted product-scope change required a plan rewrite.

## Tests and downstream proof run

Wire and unit:

- Hub-client serde, generated TypeScript `subjects?`, and
  `for_package_event_subscriptions()` compatibility tests passed.
- Subject ceilings, connection-scoped holders, 64/65 cap, and EventGap
  outside the mailbox passed.
- Fair-write ready-set tests passed.
- Unix partial `PackageEvent` write resume passed.

Live IsolatedHub, Unix:

- Negotiated `SubscribeEvents` received unsolicited `PackageEvent` for
  `event-plane-producer` / `sample.ready`.
- Unnegotiated helper sent no request. One-shot unnegotiated request
  returned `package_event_subscriptions_not_negotiated`.
- Rejected terminal Hello still allowed Status, subscribe, and event
  delivery.
- Reconnect delivered no prior event.
- Wildcard and undeclared subscribe returned typed operator errors.

Live IsolatedHub / WebRTC harness:

- Unnegotiated WebRTC `SubscribeEvents` returned
  `package_event_subscriptions_not_negotiated`. Status still succeeded.
- Negotiated WebRTC subscribe with no contract returned
  `rejected_undeclared`. Status still succeeded after rejected terminal
  Hello.

Repo gates:

- `cargo build --locked -p botster-core-daemon --bin botster-session-worker`
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`
- `cargo test --doc --workspace --offline --locked`
- `cargo test --workspace --locked --offline --exclude botster-hub`
- `node packages/hub-test-support/scripts/sync-assets.mjs --check`

`./test.sh --locked` default-concurrency runs hit rotating load flakes.
Each named failure passed in isolation:

| Suite failure | Isolated branch | Isolated HEAD |
| --- | --- | --- |
| `support_matrix_serializes_to_stable_json_shape` | fixed in this change | n/a |
| `separators_close_when_item_bytes_fit_but_commas_do_not` | pass | pass |
| `session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect` | pass | not re-run; entity path unchanged |
| `webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable` | pass | not re-run; isolated pass on branch |
| `cli_local_runtime_up_starts_reuses_and_down_stops_runtime` | pass | not re-run; CLI runtime path unchanged |

Ticket-specific IsolatedHub tests passed in the same suite that later
flaked on unrelated tests.

Downstream TUI:

- Scratch worktree `/tmp/botster-tui-event-probe` at
  `fc1ff6238ae707c355febbc03eeab5130cccf91c`
- Cargo patch onto this worktree's `botster-hub-client`
- `cargo check -p botster-tui --offline` succeeded in 1m 51s
- Adding request/event variants did not require TUI source edits
- Worktree was not committed

Downstream Web and npm publish are recorded after this commit, because
the publish script requires a clean worktree.

Live IsolatedHub Unix emit recorded:

- Hub SHA at proof time: worktree dirty on top of `6f1f5cf`
- Locked Core SHA: `fc541a59338d0591ba4fb3fa522a030d212d26d0`
- Binaries under this checkout `target/debug/`

## Unverified behavior or residual risk

- One clean `./test.sh --locked` default-concurrency pass was not obtained.
  Remaining risk is load flakes in session-entity, WebRTC write-budget, and
  CLI reuse tests. Isolated reruns passed.
- WebRTC IsolatedHub live emit of a package event was not run through a
  `botster-web` bootstrap peer.
- Saturated-event load campaign remains `ticket_1786663585_879846`.
- Web and TUI do not vendor this protocol in this run.

## Missing vault guidance discovered

None that blocked implementation. After merge, capture:

- Client `SubscribeEvents` stays multiplexed and never takes over the socket.
- Subject filters are exact `payload.subject` strings with 16 / 256 / 4,096 /
  64 ceilings.
- Client holders are connection-scoped. Public unsubscribe is
  `subscription_id` only.
- Host package-event negotiation lives on a connection host compatibility
  record and survives terminal admission rejection.
- Fair host-control writing selects already-admitted control, entity, and
  event frames, including a gap bit outside the mailbox.
