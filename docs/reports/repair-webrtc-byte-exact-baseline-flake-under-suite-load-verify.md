# Verify report: repair the WebRTC byte-exact baseline flake under suite load

| Field | Value |
| --- | --- |
| Ticket | `ticket_1787999248_674913` |
| Run | `run_1787999630_574649` |
| Step | `botster_stack_verify` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Verified commit | `4390758787bd150d86d073904746c52769d760f0` |
| Base | `38d140c006b8b278b3e04f98ddc37d6ec99b3b8b` |
| Toolchain | `RUSTUP_TOOLCHAIN=1.97.0`; `rustc 1.97.0 (2d8144b78 2026-07-07)`; `CARGO_TARGET_DIR` unset |
| Host during the official suite | 12 cores; load average 5.6 to 6.2 |
| Class | runtime-teardown does not apply |

## Independent target resolution

Verify resolved the target without using the ambient directory. `project_pipelines_current_context`
reports `target_id=tgt_7e208a0c76a44980a83b63af976b1f22` on both the ticket and the run.
`list_spawn_targets` maps that identifier to name `botster-hub`, repository `trybotster/botster-hub`.
The worktree `origin` is `https://github.com/trybotster/botster-hub.git`. The routing matches.

## Playbooks and notes loaded

- [[verifier-playbook]]
- [[botster-verifier-playbook]]
- [[botster-hub-playbook]] (repository charter)
- [[botster-runtime-verifier-playbook]] (PTY and WebRTC transport overlay)
- [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]]
- [[live byte delivery proofs need producer readiness and a completion oracle]]
- [[webrtc starvation markers must drop pre release producer ready bytes]]
- [[host exhaustion markers identify each failed test]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[exact Rust test ablations require a one test baseline]]
- [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]]
- [[observed-exit waits must issue a production exact-session observe turn]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]

Not loaded: other repository charters, [[project-pipelines-playbook]], the web overlay, and the
package overlay. The branch changes one Hub integration test body and two Hub documents.

## Diff inspected

`git diff main...HEAD --stat` reports three files: the lifecycle test, the plan, and the Implement
report. No production source, Core pin, client DTO, package artifact, or JavaScript surface changed.
The test name `webrtc_terminal_output_is_byte_exact` is unchanged. The assertion text
`WebRTC adapter frames must preserve exact bytes, got {concatenated:?}` is byte-identical to base
`38d140c`. The expected window is still `&[0x00, 0x1b, 0xff, 0xc0]`.

## Commands and results

All commands ran in one shell with `CARGO_TARGET_DIR` unset and `RUSTUP_TOOLCHAIN=1.97.0`.

1. Prebuild: `cargo build --locked -p botster-core-daemon --bin botster-session-worker` and
   `cargo build --locked --bin botster-hub`. Exit 0.
2. Focused exact baseline:
   `./test.sh --locked --test hub_daemon_lifecycle_test webrtc_terminal_output_is_byte_exact -- --exact --nocapture`
   Result: `ok. 1 passed; 0 failed; 0 ignored; 0 measured; 320 filtered out; finished in 3.11s`.
   The 320 filtered count proves the exact filter selected one real test.
3. Product red-on-revert ablation: the producer wrote `&[0x00, 0x1b, 0xff, 0xc1]` while the
   assertion still expected `&[0x00, 0x1b, 0xff, 0xc0]`.
   Result: exit 101 at `subscription_ownership_baseline.rs:1007`:
   `WebRTC adapter frames must preserve exact bytes, got [0, 27, 255, 193]`. No
   `harness_budget_expired`. The run reached the assertion in 4.67 s through the observed-exit plus
   quiet-drain oracle. Source restored.
4. Starvation marker ablation: the release-file write was removed.
   Result: exit 101 at `subscription_ownership_baseline.rs:900`:
   `harness_budget_expired test=webrtc_terminal_output_is_byte_exact kind=webrtc_byte_exact budget_ms=30000 resource=ETIMEDOUT probe=unconfirmed timed out waiting for WebRTC adapter frames after producer-ready release; concatenated is empty`.
   Duration 33.13 s. Source restored.
5. Non-empty post-release ablation: the producer wrote one byte `0x41` and then held without exit,
   through `write_python_held_live_script` with an exit-release file that never appeared.
   Result: exit 101 after 32.83 s: `WebRTC adapter frames must preserve exact bytes, got [65]`.
   This lane carries no named marker. Source restored. See Remaining risk.
6. `cargo fmt --all -- --check`. Exit 0.
7. `cargo clippy --workspace --all-targets --locked -- -D warnings`. Exit 0.
8. Official locked suite: `./test.sh --locked`. Exit 0.
   `hub_daemon_lifecycle_test`: `ok. 319 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 264.82s`,
   including `test webrtc_terminal_output_is_byte_exact ... ok`. No `harness_budget_expired` string
   appears in the suite log. Host load average stayed between 5.57 and 6.20 on 12 cores.
9. `git status --porcelain` is empty after every ablation restore. HEAD is
   `4390758787bd150d86d073904746c52769d760f0`.

One transient failure occurred during ablation 3 and did not recur: `start isolated hub: ReadyTimeout`
at `session_fixtures.rs:375`, before the changed code ran. The immediate rerun on the same source
reached the intended byte assertion. This is the known quiet-host requirement for the Hub lifecycle
suite, not a product signal, and it is unrelated to the changed oracle.

## Behavior and production path proved

- The test is Hub-owned integration test code. The production WebRTC delivery path is unchanged, so
  no production entry point gained new behavior. The ticket is intentionally test-only.
- Command 3 proves the byte-exactness claim still fails when the adapter delivers different bytes.
  The load-tolerant oracle did not weaken the proof.
- Command 4 proves the starvation lane emits a named per-test marker with the test name, kind,
  budget, resource class, and probe.
- Command 8 proves the repaired test passes inside the full locked suite on a loaded host, which is
  the exact condition that produced the reported baseline flake at base `38d140c`.
- The oracle now matches the charter overlay rules: readiness through the typed 10-second
  `wait_for_producer_ready`, then a WebRTC drain through `PRODUCER_READY_MARKER` with a buffer clear
  before the release write, then completion on the exact byte window or on observed session exit
  plus eight counted quiet drain turns. `webrtc_session_has_exited` issues an exact-session
  `ReadScreen` before `ListSessions`, as the observed-exit convention requires.

## Review findings

- `finding_1788002895_328395` (high, resolved): Verify rechecked the live source. The readiness loop
  leaves only when `concatenated` contains `PRODUCER_READY_MARKER`
  (`subscription_ownership_baseline.rs:944`). Both readiness backstop lanes call
  `panic_webrtc_byte_exact_starvation`, so no readiness expiry can reach the release write. The
  finding is closed on source and on the green focused and suite runs.
- No open findings and no open questions remain.

## Cross-repository consumer proof

None required. The branch changes no public seam, DTO, protocol, package artifact, or generated
file. `Cargo.lock` still pins `botster-core` at `7eafa470a18025895995bbedc20d34b58106a03b`. Hub
decomposition 4a (`ticket_1787894421_128594`) is the interested consumer of a clean baseline. This
run does not change that ticket, and Verify did not build it.

## Unverified behavior

- The quiet-drain completion lane after observed exit was not reproduced in isolation under injected
  transport delay. Commands 3 and 8 exercised it only at normal speed.
- The `hub-test-support` JavaScript gate was not run. No JavaScript or TypeScript file changed, and
  the wrapper reported `hub test-support package assets are current`.
- Verify measured one full locked suite run. One green run does not prove a flake rate.

## Remaining risk

- Command 5 shows a lane that fails without a named marker: a non-empty but incomplete post-release
  window at the 30-second backstop reaches the byte assertion. The vault rule
  [[webrtc starvation markers must drop pre release producer ready bytes]] requires exactly this
  mutual exclusion, so the lane is intended. It is also not reachable by ambient load with this
  fixture, because `write_python_wait_then_write_script` exits immediately after one four-byte
  `os.write`. Verify had to substitute a hanging producer to enter the lane.
- The eight quiet drain turns are still a wall-clock heuristic layered on the decision signal. Each
  turn costs one drain round trip plus up to 200 ms. Extreme starvation between observed exit and
  the last frame could close the window early and fail through the byte assertion without a marker.
- Sibling `Duration::from_secs(8)` waits elsewhere in the repository keep the original flake shape.
- `AUTHORITATIVE_SESSION_EXIT_WAIT` remains 10 seconds and shared. A later suite-load expiry of that
  wait needs its own ticket.

## Vault gaps

Two notes captured by Implement now exist in the vault and already state this design:
`live byte delivery proofs need producer readiness and a completion oracle` and
`webrtc starvation markers must drop pre release producer ready bytes`. The runtime verifier
overlay carries the matching Verify rules. One gap is worth capturing later: no note states that a
counted quiet-drain oracle is itself load sensitive and needs a stated turn budget rationale.
