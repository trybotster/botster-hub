# Hub terminal adapter alignment verification report

Ticket: `ticket_1788137128_417142`

Run: `run_1788138132_150545`

Verified commit: `c988cf92f47d084a5f2b8112e0adb41df82c9c62` over base `main` at
`c674a62ac505b990e06f4aca34db1daf586996dc`.

## 1. Independently resolved target

- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Target repository: `botster-hub` (`trybotster/botster-hub`).

Resolution method. `project_pipelines_current_context` reported the run target
id. `list_spawn_targets` mapped that id to repository `trybotster/botster-hub`.
`git remote -v` in the run worktree returned the same repository. The ambient
directory did not select the repository.

## 2. Guidance loaded

Role and repository charter:

- [[verifier-playbook]]
- [[botster-verifier-playbook]]
- [[botster-hub-playbook]]

Changed-surface overlays:

- [[botster-runtime-verifier-playbook]] for the terminal transport, adapter, and
  lifecycle diff.
- [[botster-package-verifier-playbook]] scope check for the manifest and
  `hub-test-support` pin roll.
- [[botster-pipeline-verifier-playbook]] for the committed plan and report
  artifacts.
- [[botster runtime teardown lenses]] for the runtime-teardown class.

Targeted notes:

- [[Hub official gates must not set CARGO TARGET DIR]]
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]
- [[source guard ablations must not overlap a running full suite]]
- [[cargo exact with a name prefix runs zero tests and exits zero]]
- [[verify must recheck resolved findings against the live worktree]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[core owns duplex terminal transport while Hub stays content blind]]
- [[Hub Core pin rolls update eleven literal sites and six lock sources]]

## 3. Environment

- Worktree path contains no `:`, so `CARGO_TARGET_DIR` stayed unset for every
  gate.
- `RUSTUP_TOOLCHAIN=1.97.0`; `rustc 1.97.0 (2d8144b78 2026-07-07)` from the same
  shell.
- Host load was high through the whole session, because sibling pipeline
  sessions ran Rails suites. The official locked gate still passed, so the load
  raised confidence rather than lowering it.

## 4. Commands and results

| # | Command | Result | Behavior proved |
| --- | --- | --- | --- |
| 1 | `git ls-remote origin refs/heads/project-pipelines/ticket_1788128130_441301 refs/heads/main` in `botster-core` | candidate `a781556258789dea4a50ffcb17351e7294c8ff26`, main `3672c667d516b93bfbc4b60c7f2dc02bba1dd31d` | The frozen Core candidate did not move again, so the human stop-and-repeat rule did not trigger. |
| 2 | `git diff a781556 3672c667 -- crates/botster-core/src/contract/ crates/botster-core-test-support/src/terminal_adapter/ crates/botster-core-daemon/src/daemon.rs` | empty | The Hub alignment is valid for Core main and for the candidate. |
| 3 | `grep -rn 7eafa470 --exclude-dir=target --exclude-dir=.git .` | hits only under `docs/plans/` | The pin roll left no active old-revision literal. |
| 4 | `cargo tree -e normal -i botster-terminal-protocol --locked` | one source, `rev=a781556258789dea4a50ffcb17351e7294c8ff26` | The dependency graph has one terminal protocol source. |
| 5 | `git --no-pager diff --check main...HEAD` | exit 0 | The committed tree has no whitespace defect. |
| 6 | `cargo fmt --all -- --check` | exit 0 | Format gate. |
| 7 | `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 | Strict workspace lint gate. |
| 8 | `cargo build --locked -p botster-core-daemon --bin botster-session-worker` and `cargo build --locked --bin botster-hub` | exit 0 | The worker and Hub binaries exist in the default worktree target before the locked gate. |
| 9 | `./test.sh --locked --test session_projection_owner_loop git_visible_hub_members_share_one_exact_core_revision -- --exact` | 1 passed | Every Git-visible Hub member shares one exact Core revision. |
| 10 | `./test.sh --locked -p botster-hub --lib -- --exact` on the three `adapter_slot` tests, the runtime error-mapping test, and the Unix conformance test | 5 passed | Ingress precedence, capacity floor, idle losslessness, close race, and error mapping. |
| 11 | `./test.sh --locked -p botster-hub --lib -- --exact` on the WebRTC conformance test and `subscription::attach_routes::tests::ingress_loss_hard_stops_exact_bound_route_and_preserves_sibling` | 2 passed | Core conformance for the WebRTC adapter, and exact-route hard stop with sibling survival. |
| 12 | `./test.sh --locked -p botster-hub --lib -- --exact tests::production_sources_reject_terminal_drain_and_snapshot_phase_decode tests::production_source_known_positives_catch_every_forbidden_construct` | 2 passed | The production source guard still runs. |
| 13 | `./test.sh --locked -p botster-hub --test hub_daemon_lifecycle_test unix_adapter_unbound_scoped_drain_delivers_terminal_output -- --exact --nocapture` | 1 passed | The named ticket reproduction passes at the candidate pin. |
| 14 | `env -u CARGO_TARGET_DIR RUSTUP_TOOLCHAIN=1.97.0 ./test.sh --locked` | exit 0; 1313 passed, 0 failed, 3 ignored | The official repository gate across every workspace member, integration target, and doc-test. |

Command 14 ran with host load average between 24 and 50 on a 12-core machine.

## 5. Independent red ablations

Each ablation reverted one guard, ran the exact test, and then restored the
file. No ablation overlapped a running suite. `git status --porcelain` was empty
after each restore, and each file matched `git show HEAD:<path>` at the end.

| # | Ablation | Result |
| --- | --- | --- |
| 1 | Delete the post-insert closed check in `push_ingress_frame_after_admission` | `close_discards_ingress_for_both_close_and_push_queue_orders` FAILED at `assertion failed: push_first.ingress.lock()...is_empty()`. |
| 2 | Return `Empty` instead of `Closed` from the closed branch of `AdapterSlot::try_read` | Both `production_unix_adapter_passes_core_conformance_harness` and `production_webrtc_adapter_passes_core_conformance_harness` FAILED inside Core's own harness at `botster-core-test-support/src/terminal_adapter/mod.rs:356`. |
| 3 | Give both `ControlPlaneFailed` arms one class string | `bind_terminal_adapter_mapping_is_total_over_published_variants` FAILED with `left: "control_plane_failed"`, `right: "bind_terminal_adapter.control_plane_failed"`. |
| 4 | Insert a production-path `drain_runtime_once(` literal into `src/subscription/attach_routes.rs` | `production_sources_reject_terminal_drain_and_snapshot_phase_decode` FAILED with `src/subscription/attach_routes.rs production source must not contain drain_runtime_once(`. |

Ablation 2 is the load-bearing one. It fails inside the Core test-support crate
at the exact pinned Core revision, so the new Hub ingress laws are enforced by
the real upstream conformance contract, not by a Hub-local assertion.

Ablation 4 proves acceptance check 13b: the source guard still names
`attach_routes.rs` and still rejects a production `drain_runtime_once(` call,
so the guard was not weakened to let the new test compile.

## 6. Cross-repository consumer proof

- Upstream contract consumer: Core's `botster-core-test-support` conformance
  harness at `a781556258789dea4a50ffcb17351e7294c8ff26` drives both production
  adapters. Ablation 2 shows the harness is live, not a passthrough.
- Real Core teardown: `ingress_loss_hard_stops_exact_bound_route_and_preserves_sibling`
  binds two sibling routes through the production Hub attach and bind path,
  marks ingress loss on one, drives real Core intake, and then asserts exact
  route retirement, lost-adapter closure, sibling route survival, and a
  successful sibling write.
- Pin roll consumers: `crates/botster-hub-client` and
  `crates/botster-hub-test-support` moved to the same exact Core revision. The
  guard test in command 9 proves one exact revision across every Git-visible
  member. No package version, conformance revision, or fixture byte changed, so
  this roll starts no npm or published-package release chain.
- Registered dependency: `project_pipelines_list_ticket_dependencies` on
  `ticket_1788128130_441301` returns `dependency_1788137138_804385`, Core
  depends on this Hub ticket. The Hub ticket itself has no dependency, so Hub
  can merge first as the plan requires.

## 7. Runtime-teardown class evidence

- Live hard stop: proved through real Core code in
  `ingress_loss_hard_stops_exact_bound_route_and_preserves_sibling`. The lost
  adapter reports closed and the route leaves `list_terminal_subscriptions`.
- Late-message matrix: rows 1 to 7 are covered by the three `adapter_slot`
  tests and the Core conformance harness. Row 11, the close and push race, is
  covered by a deterministic two-thread test with a barrier, and ablation 1
  makes it red.
- Sibling survival: the same test asserts the sibling stays bound, stays open,
  and still accepts an opaque terminal frame.
- Bounds: the queue holds at most `MIN_ADAPTER_INGRESS_BUFFER_FRAMES` frames,
  every ingress operation uses `try_lock`, and no path blocks.
- Production path: Hub cannot reach `TerminalAdapter::try_read` in production
  today. See section 9.

## 8. Review findings rechecked against the live worktree

| Finding | Status | Live evidence |
| --- | --- | --- |
| `finding_1788150881_471943` close race retains ingress | Resolved | The post-insert closed check exists at `src/transport/shared/adapter_slot.rs`, the deterministic race test passes, and ablation 1 makes it red on retained bytes. |
| `finding_1788150881_687191` whitespace gate not green | Resolved | Raw `git diff --check main...HEAD` exits 0 on the committed tree. |
| `finding_1788150881_203541` typed cleanup skips RemoveSession | Resolved | `production_shutdown_and_remove_session` accepts `SessionCleanup` only for the named session and the already-exited outcome, waits for authoritative exit, then issues `RemoveSession`. `final_cleanup_accepts_already_exited_without_altering_sibling` proves target absence and exact sibling equality, and both it and `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable` pass inside the official locked gate. |
| `finding_1788150881_173921` absolute path in the plan | Resolved | A raw scan of every changed file for the three home-path prefix patterns (Unix user root, Linux home root, and the Windows user root) returns only two pre-existing negative assertions in `tests/hub_daemon_lifecycle/sessions.rs`. A seeded three-pattern positive control confirms the scan detects all three forms. |
| Plan Review findings `finding_1788140354_731497`, `finding_1788139424_468510`, `finding_1788139424_883412`, `finding_1788139424_837637` | Resolved | The committed plan pins `a781556`, records the reverse dependency and its direction, carries the expanded late-message matrix, and states the scaffold-only limit in sections 3, 9b, and 10. |

## 9. Production path, stated exactly

Live rejection path, separate from the class mapping. Hub reaches both new Core
rejections in production.
`CoreDaemon::attach` calls `ensure_control_plane_live` and can return
`CoreDaemonError::ControlPlaneFailed`. `CoreDaemon::bind_terminal_adapter` and
`CoreDaemon::bind_waking_terminal_adapter` can return
`BindTerminalAdapterError::ControlPlaneFailed`. Hub's Unix and WebRTC bind path
in `src/subscription/attach_routes.rs` treats any bind error through
`fail_closed_pre_bind_attach`, which closes the adapter and fails the attach.

Correction to the committed artifacts. The two new match arms live in
`managed_session_core_error_class`. That function has exactly one production
call site, `src/runtime.rs:2167`, on a `CoreDaemon::spawn` failure in the
plugin-managed session path. `CoreDaemon::spawn` cannot return either new
variant, because `ensure_control_plane_live` has one caller, `CoreDaemon::attach`,
and `spawn` binds no adapter. So the two new arms complete an exhaustive match
and keep the crate compiling, but no production path emits either new class
string today. Earlier plan and implementation report statements overstate this.
Those artifacts now state the verified call-site limit. The shipped behavior is
correct and unchanged.

Scaffold half. The ingress buffer has no production producer.
`push_ingress_frame`, `mark_ingress_lost`, and `drop_newest_ingress_frame` carry
`#[allow(dead_code)]`, which is itself evidence that the compiler found no
production caller. `HubRuntime::drain_runtime_once` stays `#[cfg(test)]`, and
ablation 4 proves the source guard still blocks a production call.
`checklist_1788139722_173987` on `ticket_1787894427_525056` holds the real
production proof obligation.

## 10. Unverified behavior

- Acceptance check 16, the two `3672c667` passes of
  `unix_adapter_unbound_scoped_drain_delivers_terminal_output`, was not
  reproduced. That arm needs a scratch roll of every literal and lock source to
  a revision the branch does not commit. Human answer
  `question_1788142641_571879` authorized the green/green interpretation, the
  contract diff in command 2 is empty, and the committed pin arm passes, so the
  claim carries no product weight.
- GitHub CI was not run. Only local repository gates were executed.
- No live browser, WebRTC peer, or packaged-browser path was exercised beyond
  the tests inside the official locked gate.

## 11. Remaining risk

- The Hub branch pins an unmerged Core candidate. Hub must merge first, and Core
  must then merge `a781556258789dea4a50ffcb17351e7294c8ff26` unchanged. If Core
  Review changes the candidate, the Hub pin and this evidence go stale.
- The adapter ingress half stays unreachable until `ticket_1787894427_525056`
  wires the producers and the wake pump. A wrong `Lost` becomes a live route
  teardown on the day that lands.
## 12. Vault gaps worth capturing

1. Hub adapter `try_read` precedence is `Closed`, `Lost`, `Frame`, `Empty`, and
   Core's own conformance harness enforces it.
2. Hub cannot reach `TerminalAdapter::try_read` in production before the wake
   pump lands, and `#[allow(dead_code)]` on the ingress producers is the cheap
   compiler oracle for that fact.
3. An exhaustive-match arm added only to keep a crate compiling is not a
   production path. Name the single call site before calling an error mapping
   live.
4. A frozen Core candidate can move during one Plan step, so a consumer must
   re-resolve the tip immediately before it edits literals.
5. Hub Core pin rolls now have fourteen literal sites and six lock sources.
