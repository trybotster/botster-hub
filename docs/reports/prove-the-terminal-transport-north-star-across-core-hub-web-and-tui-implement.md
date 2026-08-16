# Implementation report: Prove the terminal transport north star across Core, Hub, Web, and TUI

Ticket: `ticket_1786661010_115885`
Run: `run_1786867245_870799`
Step: `botster_stack_implement` (`run_step_1786907374_480353`)
Approved plan: rev 6 (`docs/plans/prove-the-terminal-transport-north-star-across-core-hub-web-and-tui.md`)

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Worktree | this pipeline worktree, no `:` in path, no `CARGO_TARGET_DIR` override |
| Branch | `project-pipelines/ticket_1786661010_115885` |
| Base | `c72712e2606b8abe77e1b91c2a736791036fadd8` |
| Lockfile Core | `fc541a59338d0591ba4fb3fa522a030d212d26d0` |
| Merge policy | direct into `main`; **not merged** — authentic same-session proof is blocked |
| Session-type eligibility consumer | true |
| `teardown_class_applies` | yes |

Independent routing: `project_pipelines_current_context` and `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub`. Implementation stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

Playbooks:

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]

Not loaded: [[project-pipelines-playbook]] — package/plugin workflow implementation is out of scope.

Targeted notes:

- [[live hub proof records distinct hub and locked core binary provenance]]
- [[Hub bee15e7 builds the session worker from botster-core-daemon]]
- [[TUI live Ghostty has IsolatedHub ghostty plus attach-only ghostty-shared and ghostty-shared-exit]]
- [[the packaged-protocol terminal lane has a caller-owned keep-alive mode]]
- [[a public occupancy oracle must union Hub routes with Core inventory]]
- [[live attach counters and omitted occupancy fields are not identity oracles]]
- [[webrtc bootstrap origin must be requested after the package server binds]]
- [[supervised web entrypoint tests use health for readiness and local url for later contract proof]]
- [[host ShutdownSession classification must call the exact-session Core query]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]

Convention conflicts: none.

## Constraints applied before edits

- Hub charter only. No Core, Web, TUI, or TUI Kit source edits in this worktree.
- Follow plan rev 6. Named WebRTC bootstrap repair only on the named test. Other suite failures need exact diagnosis.
- Runtime-teardown lenses stay implemented, not deferred.
- Use `./test.sh --locked`. Isolated reruns are diagnostic only.
- Do not publish hub-test-support. Protocol stays 7.

## Files changed

| Path | Change |
| --- | --- |
| `script/prove-north-star-shared-session` | Unconditional Hub coordinator. Starts provenance-pinned Hub, enables `botster-web`, Option A `list_session_types_for_target` + `spawn_session_type`, shared producer (`NORTH_STAR_HISTORY` then Web production loop), then shipped Web and TUI attach profiles. |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | IsolatedHub one-session Unix+WebRTC occupancy oracle. `start_webrtc_adapter_hub` waits for listener health and published `local_url` before `IssueLocalWebrtcBootstrap`. |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | `unix_shutdown_session_from_another_connection_classifies_attached_exit` keeps the session attached (`sleep 30`) so ShutdownSession is not a race with natural exit. |
| `docs/plans/prove-the-terminal-transport-north-star-across-core-hub-web-and-tui.md` | Approved plan rev 6. |
| `docs/reports/prove-the-terminal-transport-north-star-across-core-hub-web-and-tui-implement.md` | This report. |

## Ownership boundaries preserved

- Hub owns host admission, adapters, occupancy, owner loop, and the coordinator.
- Core still owns terminal frames, attach generations, and the adapter conformance harness.
- Web and TUI source were executed, not edited.
- Host Drain remains control-plane only. Production `drain_subscription` / `drain_runtime_once` stay `cfg(test)`.
- Hub and hub-client have no direct `botster-terminal-protocol-client` dependency (`cargo tree` confirmed).
- TUI Kit still pins only `botster-ui-contract-v0.3.2`.

## Cross-repo dependencies or separately routed work

Closed dependencies used as given.

New blocking tickets registered on their owner targets (not this Hub `target_id`):

| Ticket | Target | target_id | Why |
| --- | --- | --- | --- |
| `ticket_1786912123_916503` | botster-web | `tgt_40abcf71ccf049f4ac0c99953a799869` | Caller-owned cancel oracle reports 0 or 2 detaches instead of exactly one |
| `ticket_1786912267_788084` | botster-tui | `tgt_c3d470bab78549df920a41e8fb0e58d8` | IsolatedHub `session-types` live profile misses created agent type |

Dependencies added: `dependency_1786912130_730748`, plus the TUI ticket dependency.

Re-resolved mains at Implement start (all still the plan floor or newer):

| Repo | SHA |
| --- | --- |
| Hub | `c72712e2606b8abe77e1b91c2a736791036fadd8` |
| Core | `fc541a59338d0591ba4fb3fa522a030d212d26d0` |
| Web | `ebb6677902ff5920ebb75685a74bba30b9b81b87` |
| TUI | `8b4df69e27b65071aa94b7e5d6b31d0990c041fc` |
| TUI Kit | `c83ba6c518e2324e34ce24c7abe5a8a05e56293c` |

TUI `botster-terminal-protocol-client` is now pinned to Core `fc541a59`, not the older plan snapshot `f4f6bf5`. TUI host-DTO Hub Git pin remains `c72712e` (allowed by `question_1786867995_904640`).

## Deviations from plan

1. **No merge to Hub `main`.** Authentic same-session proof did not print the required coordinator pass lines. Hub-owned code is committed on the ticket branch only.
2. **No production WebRTC bootstrap code change.** The named test `webrtc_terminal_adapter_stale_generation_close_does_not_sweep_replacement_owner` passed all three baseline suite runs. A sibling test later failed in the same helper because bootstrap ran before bind. The helper now waits for health + `local_url` ([[webrtc bootstrap origin must be requested after the package server binds]]). Production `IssueLocalWebrtcBootstrap` already fail-closes without `local_url`.
3. **ShutdownSession suite race repaired in the test, not production.** `printf; exit 0` could be reaped before sibling ShutdownSession under load. Isolated pass on branch and base. The test now holds the attached session until ShutdownSession.
4. **Web keep-alive and TUI IsolatedHub session-types did not pass.** Foreign tickets registered. Not repaired here.

## Runtime-teardown lenses

| Lens | Implementation evidence |
| --- | --- |
| Isolation | New IsolatedHub oracle: Unix pair stays occupied after WebRTC peer close; host session stays listed; Unix SendInput still accepted. |
| Bounds | No new unbounded `block_on(close)`. Bootstrap wait uses the existing readiness backstop. |
| Late-message matrix | Unchanged production matrix from plan rev 6. IsolatedHub proves stale WebRTC pair leaves occupancy without sweeping the Unix pair. |
| Production-path proof | Unix socket/peer loss and WebRTC peer close remain on production adapters. Authentic Web DataChannel close did not complete because the Web cancel oracle failed first. |
| Ownership identity | Occupancy rows are session + subscription + generation. Dual-attach test uses distinct subscription ids. |
| Sibling fail-closed | Dual-attach test fails if the Unix sibling dies after WebRTC peer loss. |

## Tests and downstream proof run

Disk: 76 GiB free. `.gitignore` 53/53 bytes. Path has no `:`.

Lifecycle suite (`./test.sh --locked --test hub_daemon_lifecycle_test`):

| Run | Result | Notes |
| --- | --- | --- |
| 1 | 219 passed, 1 ignored | Named WebRTC test ok |
| 2 | 219 passed, 1 ignored | Named WebRTC test ok |
| 3 | 219 passed, 1 ignored | Named WebRTC test ok |
| 4 | 219 passed, 1 failed | `unix_shutdown_session_from_another_connection_classifies_attached_exit`; isolated pass on branch and base |
| 5 | 220 passed, 1 ignored | After IsolatedHub oracle + ShutdownSession hold |
| 6 | 219 passed, 1 failed | `webrtc_terminal_adapter_failed_remove_session_does_not_suppress_later_core_close` missing bootstrap; isolated pass |
| 7–9 | 220 passed, 1 ignored each | After bootstrap-wait helper; three consecutive post-change passes |

Other Hub gates:

```
./test.sh --locked --test hub_client_api_test          # 34 passed
./test.sh --locked --test hub_test_support_conformance_test  # 2 passed
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo tree -e normal -p botster-hub --depth 1
cargo tree -e normal -p botster-hub-client --depth 1
cargo tree -e normal -p botster-hub-test-support --depth 1
```

Provenance-pinned binaries (fresh target `/tmp/north-star-implement/hub-target`, realpaths under `/private/tmp/...`):

| Binary | Path | Source SHA |
| --- | --- | --- |
| `botster-hub` | `/private/tmp/north-star-implement/hub-target/debug/botster-hub` | Hub `c72712e` |
| `botster-session-worker` | `/private/tmp/north-star-implement/hub-target/debug/botster-session-worker` | Core `fc541a59` |

Coordinator:

```
BOTSTER_HUB_BIN=... BOTSTER_SESSION_WORKER_BIN=... \
BOTSTER_WEB_CHECKOUT=<web ebb6677> BOTSTER_TUI_CHECKOUT=<tui 8b4df69> \
BOTSTER_SHARED_SESSION_ID=north-star-shared \
  script/prove-north-star-shared-session
```

Both coordinator attempts printed `north-star-shared-spawned session_id=north-star-shared session_type_id=device/north-star-shared`. Web keep-alive 1 then failed:

`expected exactly one detach for held subscription botster-web-terminal-…, got 2`

Web standalone `npm run smoke:live-packaged-protocol:shared-session` against the same binaries failed on the second keep-alive with `got 0`. Session-type live proof, Option A picker, and alternate-screen cycle 0 passed before the cancel oracle.

TUI `script/test-live-hub session-types` against the same binaries failed: `created agent type missing from entity store`.

Required coordinator pass lines **not** printed: `live-shared-session-keep-alive-passed` (twice), `ghostty-shared-complete`, `ghostty-shared-exit-attached`, `live-shared-session-exit-passed`, `ghostty-shared-exit-complete`, `north-star-shared-session-complete`.

## Unverified behavior or residual risk

- Authentic Web+TUI same-session attach, connection-loss, and exit oracles are unverified.
- Web cancel detach count is unstable (0 or 2) on current Web main. Hub Detach remains idempotent.
- TUI IsolatedHub session-types entity store miss is unverified on this Hub.
- Named WebRTC bootstrap flake did not recur in the three baseline runs; helper now waits for bind. Residual load sensitivity on other WebRTC tests is possible.
- Vault checklist MCP create timed out; ticket checklist `checklist_1786867630_648562` already exists from Plan.

## Missing vault guidance discovered

None that blocked Hub-owned work. The Web cancel detach-count contract lives in Web harness code, not a vault note. Ratification of [[transport ownership north star for modular Botster is proposed]] still waits for Verify after authentic proof.

## Assumptions

- Web `got 2` / `got 0` cancel counts are Web-owned request emission, not Hub forging extra Detach rows.
- IsolatedHub Unix+WebRTC occupancy plus three consecutive suite passes after the last Hub change satisfy the Hub-owned oracles in this ticket.
- Merge to `main` waits until the registered Web ticket (and TUI session-types ticket, if Review requires it) close, or a human waives authentic proof.
