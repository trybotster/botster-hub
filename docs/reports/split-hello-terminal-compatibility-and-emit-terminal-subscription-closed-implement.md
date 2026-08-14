# Implement report: Split Hello terminal compatibility and emit TerminalSubscriptionClosed

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | `botster-hub` |
| Pipeline worktree | the pipeline-provided ticket worktree |
| Ticket | `ticket_1786705502_228757` |
| Run | `run_1786705508_262530` |
| Step | `botster_stack_implement` (`run_step_1786710108_556857`) |
| Approved plan | `docs/plans/split-hello-terminal-compatibility-and-emit-terminal-subscription-closed.md` revision 2 |
| Human answer | `question_1786705427_821834` chose **1B** |
| Merge policy | direct into `main`; do not create a PR |
| Base | `origin/main` `9d1f858fbfaf87ff2e95cf292690b03e91558695` |
| Locked Core | `Cargo.lock` pins `botster-core` `f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| `teardown_class_applies` | yes |

Routing verified independently: `project_pipelines_current_context` ticket/run `target_id` and `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` → `botster-hub`. Implementation stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]] — ownership charter
- [[botster-hub-client-playbook]] — public DTO overlay inside this repository
- [[botster runtime teardown lenses]] — required; class applies

### Targeted atomic notes

- [[botster hub is a first party host profile over core]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[proposed Hub admission binds adapters with negotiated subscription capabilities]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[pre READY attach failure creates no attach ownership]]
- [[Core bind stores an immutable negotiated terminal capability set]]
- [[test script required for rust tests not cargo test]]
- [[adding a hub client feature constant is a three site change]]
- [[generated typescript dtos must encode serde field optionality]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[botster web generated protocol drift checks need explicit hub artifact paths]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[rust repo strict lints must be verified before dismissing warnings]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[implementation artifacts must match actual git state]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

### Explicitly not loaded

- [[project-pipelines-playbook]] — Project Pipelines package/plugin paths are out of scope
- Other repository charters (Core, Web, TUI, Workspaces, Ghostty)

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`
- Follow approved revision 2; keep Hub charter ownership
- Do not edit Core, TUI, Web, or WebRTC adapters
- Advertise `terminal_subscription_closed` without raising `DaemonCompatibilityRequirement::current()`
- `PROTOCOL_VERSION` stays 7; bump `CONFORMANCE_FIXTURE_REVISION` 39 → 40
- Default host floor stays 36
- Runtime-teardown lenses are implemented, not deferred
- Unix adapter stays content-blind

## Files changed

| Path | Change |
| --- | --- |
| `crates/botster-hub-client/Cargo.toml` | Types-only `botster-terminal-protocol` |
| `Cargo.lock` | Hub-client now depends on locked Core `botster-terminal-protocol` at `f4f6bf5` |
| `crates/botster-hub-client/src/lib.rs` | Hello/HelloAck terminal fields, `TerminalSubscriptionClosed`, mux Event class, optional feature, conformance 40, connect sibling |
| `crates/botster-hub-client/src/typescript.rs` | Optional Hello fields, Core descriptor interfaces, new event variant |
| `crates/botster-hub-client/generated/daemon-protocol.ts` | Regenerated |
| `src/unix_terminal_adapter.rs` | Host-close flag, generation-scoped mux routes, pending host events, dying/suppress |
| `src/daemon_attach_stream.rs` | Bind the stored Hello-time capability set; host-close on Hub adapter close |
| `src/daemon_transport.rs` | `UnixTerminalAdmission::{Admitted,Rejected}`, Hello diagnostic, Attach gate before `start_attach`, emit path |
| `src/main.rs` | Print the new event in the operator console |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | Production-path IsolatedHub proofs |
| `tests/hub_daemon_lifecycle/sessions.rs` | Exhaustive `DaemonEvent` matches |
| `docs/client-protocol.md` | Hello split, Rejected admission, close event |
| `README.md` | Conformance 40 |
| `packages/hub-test-support/*` | Unpublished `0.1.35`, regenerated protocol and matrix |
| `crates/botster-hub-test-support/src/lib.rs` | Matrix assertion includes the new optional feature |
| `docs/plans/split-hello-terminal-compatibility-and-emit-terminal-subscription-closed.md` | Approved plan |
| `docs/reports/split-hello-terminal-compatibility-and-emit-terminal-subscription-closed-implement.md` | This report |

## Ownership boundaries preserved

Hub owns Hello admission, host `DaemonCompatibility`, capability intersection, adapter/mux/route records, and `TerminalSubscriptionClosed`.

Core types and bind/hard-stop APIs were consumed, not edited. Locked Core remains `f4f6bf5`.

`botster-hub-client` lives in this repository. Public DTO work stayed here.

Unix `try_write` still serializes opaque `TerminalFrame` bytes only. Close observation uses `is_closed()`, the host-close flag, route identity, and Hub session lifecycle.

## Cross-repo dependencies or separately routed work

| Surface | Action |
| --- | --- |
| Core | None. Types and bind API already ship at `f4f6bf5`. |
| TUI `tgt_c3d470bab78549df920a41e8fb0e58d8` / `ticket_1786661009_551067` | Already depends on this ticket. Not edited. Scratch Cargo patch recorded. |
| Web `tgt_40abcf71ccf049f4ac0c99953a799869` / `ticket_1786661008_897067` | Not edited. Scratch `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` drift check recorded. |
| WebRTC adapter | Follow-up after that adapter binds Core adapters. |

## Runtime-teardown lenses implemented

| Lens | Implementation |
| --- | --- |
| Isolation | One closed route is `(client_id, session_id, subscription_id, generation)` plus its adapter handle. Sibling mux routes stay up. |
| Bounds | Adapter `close()` is non-blocking. Emit is one mux write. Envelope/event flush uses a 50ms non-blocking write so a full socket does not `block_on` the connection task. |
| Late-message matrix | Hello is complete. Rejected Attach returns `OperatorError` before `start_attach`. Detach suppresses the event. Connection `close_all` sets dying and does not emit. ShutdownSession / RemoveSession / non-running lifecycle suppress. |
| Production-path proof | IsolatedHub Unix path: mismatch Hello then Attach; host close; Core write-budget stall; Detach; process exit / ShutdownSession; two-connection replacement owner. |
| Ownership identity | Mux routes are keyed by generation. Event for N does not close N+1. Two-connection test: A observes N closed, B stays bound and echoes. |
| Sibling fail-closed | Host close of one session leaves the sibling listed and Status works. Write-budget close of the stalled route leaves the quiet sibling running. Connection write failure does not emit. |

## Deviations from plan

1. Mux envelope and close-event writes use a 50ms non-blocking `write_all`. A blocking write with the existing 2s deadline would treat a full socket as connection death and could not emit `core_adapter_closed`. This is required for the write-budget production path.
2. Same-connection re-attach of one subscription fail-closes (`attach_failed`) because Core reuses the live generation for the same client. Host-close emit is proved by re-attaching one of two sessions on the same connection. Replacement-owner proof uses two connections: B attaches the same key, Core hard-stops A, A receives `TerminalSubscriptionClosed` for generation 1, B stays bound.
3. IsolatedHub cannot call `list_terminal_subscriptions`. Mismatched Hello then Attach is proved by `OperatorError` before `start_attach`, no `AttachFailed`, no adapter envelopes, and Status still succeeding.
4. `packages/hub-test-support/test.mjs` was not executed as a Node process because `@trybotster/ui-contract` is not installed in that package directory. Rust `botster-hub-test-support` tests, including node-package copy equality, passed.
5. Review `review_1786710092_413915` required three high fixes: resumable mux writes, suppress-after-success, and path-neutral report wording. Those are in this revision.

No accepted product-scope change. The committed plan's acceptance checks remain the contract.

## Tests and downstream proof run

Repo wrapper is `./test.sh` (`BOTSTER_ENV=test cargo test --workspace`).

| Command | Result |
| --- | --- |
| `./test.sh --offline --test hub_daemon_lifecycle_test unix_adapter` | 8 passed |
| `hello_ack_advertises_independent_terminal_compatibility` | pass |
| `mismatched_terminal_hello_rejects_attach_before_core_ownership` | pass |
| `host_adapter_close_emits_terminal_subscription_closed_for_one_route` | pass |
| `core_write_budget_hard_stop_emits_core_adapter_closed` | pass |
| `connection_death_and_detach_do_not_emit_terminal_subscription_closed` | pass |
| `process_exit_and_shutdown_session_do_not_emit_terminal_subscription_closed` | pass |
| `stale_generation_close_does_not_sweep_replacement_owner` | pass |
| `terminal_subscription_closed_feature_does_not_raise_default_requirement` | pass |
| `botster-hub-client` lib (70) | pass |
| `botster-hub` lib including Unix adapter conformance harness (242) | pass |
| `botster-hub-test-support` lib (44) | pass |
| `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings` | `CLIPPY_EXIT=0` |

Full workspace `./test.sh --offline` passed (exit 0), including `hub_daemon_lifecycle_test` (185 tests), installer, capability, client API, and crate doctests.

Review-loop tests added:

- `daemon_transport::mux_write_resume_tests::resumable_mux_write_keeps_offset_and_emits_one_valid_frame`
- `daemon_transport::mux_write_resume_tests::resumable_mux_write_does_not_start_a_second_frame_while_first_is_pending`
- `failed_remove_session_does_not_suppress_later_core_close`

### Production entry points

- Hello: `handle_connection_async` writes `DaemonHelloAck` with `Some(TerminalCompatibility::current())` and registers `UnixTerminalAdmission`.
- Attach: `handle_runtime_control_request` returns `OperatorError` for `Rejected` **before** `start_attach` / `begin_core_attach`.
- Bind: `bind_unix_adapter_after_attaching` uses the stored Admitted set.
- Close: control-thread `queue_unix_subscription_closed_events` plus the Unix connection loop `flush_unix_mux_writes` write one mux-classified `DaemonEvent::TerminalSubscriptionClosed`.

IsolatedHub launches `CARGO_BIN_EXE_botster-hub`. Locked Core worker remains `f4f6bf5`.

### Downstream proof

TUI scratch worktree at `5d2af28`, isolated Cargo target dir, `[patch]` to this ticket worktree's `botster-hub-client` and `botster-ui-contract`:

- `cargo check --workspace` exit 0
- `cargo check --workspace --all-targets` exit 0

This TUI revision has no Hello struct literals or exhaustive `DaemonEvent` matches that break. First-party attach consumption remains TUI ticket `ticket_1786661009_551067`.

Web scratch worktree at `e2c3192`:

```sh
BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL=/path/to/botster-hub/crates/botster-hub-client/generated/daemon-protocol.ts \
  npm test
```

The drift check ran and failed as expected. It was not skipped. Vendored `src/botster/generated/daemon-protocol.ts` lacks optional `terminal_compatibility` fields, the Core descriptor interfaces, and `terminal_subscription_closed`. Web was not edited.

## Unverified behavior or residual risk

- IsolatedHub cannot print Core inventory rows. The mismatch path is proved by the Attach gate and socket-visible oracles.
- Write-budget proof depends on `yes` filling the adapter slot and Core pumping 512 unsuccessful writes. It passed in IsolatedHub in about 8s.
- Same-connection re-attach of one subscription still fail-closes. That is existing Core same-client generation reuse, not a second phase machine.
- Unpublished `@trybotster/hub-test-support@0.1.35` is not published. Follow-up npm publish remains allowed.
- WebRTC emission remains a later adapter ticket.

## Missing vault guidance discovered

- Mux-classified unsolicited host events (`DaemonUnixMuxFrame::Event`) are now a durable convention. Capture after this merge if Review agrees.
- Hello that keeps host operations open after a terminal mismatch, including the durable `UnixTerminalAdmission::Rejected` row, is now shipped. Capture after this merge if Review agrees.
- No Core close-reason API was added.

No capture this Implement visit unless Review asks for it.
