# Implement report: Emit TerminalSubscriptionClosed on WebRTC after adapter close

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | `botster-hub` at `/Users/jasonconigliari/Projects/botster-hub` |
| Pipeline worktree | this run worktree |
| Ticket | `ticket_1786724303_284888` |
| Run | `run_1786724337_992334` |
| Step | `botster_stack_implement` (`run_step_1786730972_238937`) |
| Approved plan | `docs/plans/emit-terminal-subscription-closed-on-webrtc-after-adapter-close.md` revision 2 |
| Merge policy | direct into `main`; do not create a PR |
| Integrated base | `origin/main` `4f30d6952f9a29541ab3a670a54bf5e136b8eb8e` (includes published hub-test-support `0.1.35`) |
| Locked Core | `Cargo.lock` pins `botster-core` and `botster-terminal-protocol` at `f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| `teardown_class_applies` | yes |
| Session-type eligibility consumer | false |

Independent routing: `project_pipelines_current_context` ticket/run `target_id` and `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. The approved plan used the same routing. Implementation stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]] — ownership charter
- [[botster-hub-client-playbook]] — public DTO overlay inside this repository
- [[botster runtime teardown lenses]] — required; class applies

### Targeted atomic notes

- [[Unix mux host events are unsolicited control frames]]
- [[Unix mux host frames flush before new terminal slots]]
- [[host reconciliation must not rewrite a completed Core adapter close reason]]
- [[WebRTC DataChannel local close uses the peer close bound before cleanup]]
- [[a ready WebRTC send must win over a queued DataChannel close]]
- [[WebRTC terminal admission requires an encrypted DataChannel Hello]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[Core subscription hard-stop is synchronous close and drop on the host tick]]
- [[Core terminal subscription ownership is session, subscription, and generation]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[conformance fixture revisions must be unique per published content]]
- [[webrtc peer cleanup removes every per peer owner together]]
- [[terminal webrtc failure records do not prove peer runtime teardown]]
- [[mux envelope delivery does not prove Hub route ownership]]
- [[PeerClosed attach occupancy must use the live attach route set]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster hub is a first party host profile over core]]
- [[botster hub client crate is the external client boundary]]
- [[botster core public enums are breaking until non exhaustive is decided]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[rust repo strict lints must be verified before dismissing warnings]]
- [[test script required for rust tests not cargo test]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

Human answer `question_1786730510_751282` is applied: protocol stays 7; `daemon_event` requires Hello `terminal_subscription_closed`.

### Explicitly not loaded

- [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope
- Other repository charters (Core, Web, TUI, Workspaces, Ghostty)

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`
- Follow approved plan revision 2; keep Hub host-plane ownership
- Do not edit `botster-web` or Unix mux policy except shared classification already used by both transports
- Runtime-teardown lenses are implemented, not deferred
- WebRTC adapter stays content-blind
- Keep protocol 7; bump conformance to 41; cut unpublished hub-test-support `0.1.36`

## Files changed

| Path | Change |
| --- | --- |
| `src/webrtc_terminal_adapter.rs` | `host_closed`, generation-bearing routes, pending events, suppress lists, dying flag, Hello `close_events_admitted`, `queue_closed_subscription_events`, `close_from_host` that does not rewrite a completed Core close |
| `src/daemon_attach_stream.rs` | Register WebRTC routes with generation; `close_from_host` on WebRTC handles; `hello_requires_terminal_subscription_closed` |
| `src/daemon_transport.rs` | Queue WebRTC close events beside Unix; suppress Detach generations and shutdown/exit/remove sessions on WebRTC muxes; queue before `reconcile_inventory` |
| `src/local_webrtc.rs` | Admit close events from encrypted Hello; flush `daemon_event` before terminal slots; drop queued events when the feature is not negotiated |
| `crates/botster-hub-client/src/lib.rs` | `DaemonLocalWebrtcDeliveryKind::DaemonEvent`; `for_webrtc_terminal_subscription_closed()`; protocol 7 / conformance 41 |
| `crates/botster-hub-client/src/typescript.rs` | Generated union includes `daemon_event` |
| `crates/botster-hub-client/generated/daemon-protocol.ts` | Regenerated |
| `tests/hub_daemon_lifecycle/webrtc_fixtures.rs` | Park negotiated host events; unnegotiated receive path fails closed on `daemon_event` |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | IsolatedHub negotiated, unnegotiated, sibling, negative, and keep-reading write-budget proofs |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | Conformance revision assert 41 |
| `docs/client-protocol.md` | WebRTC host-event delivery, Hello gate, and non-emit cases |
| `README.md` | Runtime example conformance revision 41 |
| `packages/hub-test-support/**` | Unpublished `0.1.36`; synced protocol, fixtures, metadata |
| `docs/plans/emit-terminal-subscription-closed-on-webrtc-after-adapter-close.md` | Approved plan revision 2 |

## Ownership boundaries preserved

Hub owns host events, WebRTC admission, adapter instances, framing, encryption, and transport writes.

Core owns terminal subscriptions, attach generations, write-budget hard-stop, and adapter `close()` on the host tick. No Core edit.

`botster-hub-client` in this repo owns the public host DTO, delivery-kind enum, generated TypeScript, and IsolatedHub consumer helpers.

Unix mux policy is unchanged except the shared conformance-revision assert.

No `botster-web` edit.

## Cross-repo routing

| Seam | Action |
| --- | --- |
| Closed Unix sibling `ticket_1786705502_228757` | Copied the emit contract. Did not reopen Unix. |
| Open Unix sibling `ticket_1786716545_417854` | Not treated as the WebRTC oracle. Its keep-reading merge is on the integrated base. |
| Published `0.1.35` (`ticket_1786723348_522242`) | Left immutable. This run cuts unpublished `0.1.36`. |
| Hub publish `ticket_1786730686_674642` | Consumes this merge. Not published here. |
| Web `ticket_1786661008_897067` | Downstream. Must Hello-require `terminal_subscription_closed` before decoding `daemon_event`. Not edited here. |

## Runtime-teardown lenses implemented

| Lens | Implementation |
| --- | --- |
| Isolation | One closed generation dies. Peer, DataChannel, sibling subscriptions, and host Status/ListSessions stay up. Peer death marks the mux dying and emits nothing. |
| Bounds | Adapter `close` stays non-blocking. DataChannel `local_close` still uses `LOCAL_WEBRTC_PEER_CLOSE_BOUND`. A closed adapter does not call `close_all`. |
| Late-message matrix | Detach suppresses that generation. Shutdown/exit/remove suppress the session. Hello after dying does not admit a new live close-event path. Stale generation N does not close N+1. |
| Production-path proof | IsolatedHub peer: Core or host close → handle `is_closed` → control-thread queue → peer-loop host-event flush → `daemon_event` plaintext is `TerminalSubscriptionClosed` → sibling `daemon_terminal_frame` continues. |
| Ownership identity | session + subscription + generation + grant/peer. Replacement-owner proof uses Hub `live_attach_subscriptions` plus B's owned Drain, not mux delivery alone. |
| Sibling / fail-closed | Successful one-generation close keeps siblings working. Ultimate peer close retires that peer once. Other peers are untouched. |

No lens was dropped to informal follow-up.

## Deviations from plan

None in product behavior.

Process: `project_pipelines_create_vault_checklist` timed out twice during Implement. Evidence is recorded here and will be attached to a checklist item if the tool recovers. The Plan-visit checklist `checklist_1786724757_253940` was not reused as an Implement substitute.

Test-oracle only: IsolatedHub sibling checks decode `TerminalOutput` payloads the same way Unix `unix_envelope_contains_live_bytes` does. That is not a production change.

## Tests and downstream proof

Production IsolatedHub WebRTC subscriber (hub-client peer, not Web):

1. Negotiated host close of one of two bound subscriptions emits exactly one `daemon_event` whose plaintext is `terminal_subscription_closed` with `host_adapter_closed`. Sibling `daemon_terminal_frame` continues. Status/ListSessions still work.
2. Authentic Core write-budget hard-stop on a negotiated keep-reading peer emits exactly one event with reason `core_adapter_closed`. Sibling `echo:wwb-sibling-live` continues. Status-on-timeout is not the oracle.
3. Unnegotiated protocol-7 adapter Hello never receives `DaemonEvent`. IsolatedHub receive path fails closed if it sees the new kind. Adapter bind and sibling frames still work.
4. Explicit Detach, peer death, process exit, and `ShutdownSession` do not emit.
5. Failed `RemoveSession` does not suppress a later Core close.
6. Replacement owner A receives close for generation 1. B stays bound, Drain stays owned, and live terminal frames continue.
7. `assert_terminal_adapter_conformance` still passes.
8. Default `DaemonCompatibilityRequirement::current()` and `for_webrtc_terminal_adapter()` omit the close feature. Protocol stays 7. Conformance is 41.

### Commands

```
git fetch origin --prune
git merge origin/main --no-edit   # fast-forward 24517f4 -> 4f30d6952f9a29541ab3a670a54bf5e136b8eb8e
cargo run --quiet -p botster-hub-client --example generate_typescript
node packages/hub-test-support/scripts/sync-assets.mjs
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
./test.sh --test hub_daemon_lifecycle_test webrtc_terminal_adapter
./test.sh -p botster-hub webrtc_terminal_adapter
cargo test --locked -p botster-hub-client --lib
```

Results:

- session-worker build: exit 0
- rustfmt check: exit 0
- workspace clippy `-D warnings`: exit 0
- `./test.sh --test hub_daemon_lifecycle_test webrtc_terminal_adapter`: 15 passed, 187 filtered
- `./test.sh -p botster-hub webrtc_terminal_adapter`: adapter unit tests 6 passed; lifecycle `webrtc_terminal_adapter_*` 8 passed (filter before rename of new proofs). After rename, the 15-test IsolatedHub command is the complete wrapper.
- `cargo test --locked -p botster-hub-client --lib`: 73 passed

Provenance:

- Hub binary realpath: `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1786724303_284888/target/debug/botster-hub`
- session-worker realpath: `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1786724303_284888/target/debug/botster-session-worker`
- Integrated Hub SHA: `4f30d6952f9a29541ab3a670a54bf5e136b8eb8e`
- Locked Core SHA: `f4f6bf5babe92dfb9241a760c414187f711c2c42`

`./test.sh -p botster-hub-client` ran the workspace wrapper once before IsolatedHub oracle fixes. That run executed Unix and WebRTC lifecycle tests; the three then-failing IsolatedHub oracles are now green under the named commands above. A second full workspace wrapper was not rerun after the README revision-41 fix.

`node packages/hub-test-support/test.mjs` failed because `@trybotster/ui-contract` is not installed in this worktree. `sync-assets.mjs --check` passed through `./test.sh`.

## Unverified behavior or residual risk

- Browser Web decode of `daemon_event` is intentionally unverified here. Web ticket `ticket_1786661008_897067` owns that after publish `ticket_1786730686_674642`.
- npm publish of `@trybotster/hub-test-support@0.1.36` is not done here.
- IsolatedHub loopback did reach keep-reading `core_adapter_closed` without a new Hub terminal policy queue. Fast DataChannels may still complete most sends; the production flush path is what the test drives.
- `node packages/hub-test-support/test.mjs` was not a green local Node install in this worktree.

## Missing vault guidance discovered

Capture after Review agrees, as the plan recorded:

1. WebRTC host events use a `daemon_event` delivery kind and are unsolicited.
2. Protocol 7 may add that kind only after Hello negotiates `terminal_subscription_closed`.
3. WebRTC `close_from_host` must not set `host_closed` on an already-closed handle. Queue before reconcile.

No additional missing vault guidance blocked implementation.

## Assumptions

- Human answer `question_1786730510_751282` is the protocol decision.
- IsolatedHub keep-reading proof is the required oracle.
- `CONFORMANCE_FIXTURE_REVISION` 41 was free after published `0.1.35` / revision 40.
- Direct merge, no PR.
