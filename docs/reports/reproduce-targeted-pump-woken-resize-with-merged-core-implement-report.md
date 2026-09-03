# Targeted wake progress implementation report

## Routing

- Ticket: `ticket_1788206393_323469`
- Run: `run_1788405008_320393`
- Target repository: `botster-hub` (`trybotster/botster-hub`)
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Implementation commits: `6e82219c381843c2814a1ef8f0c6e1251b992ead` and `ceb0823eba6d902f7b0dad12df68e774e8fb0e01`
- Committed Core pin: `48a437032791e678010254708259568ce4ad02bf`
- Wake-only Core candidate: `05464a186c974e2d1b21b190679a0486f066f8d6`
- Merge policy: direct. This run does not require a pull request.

Project Pipelines and an independent spawn-target lookup mapped the target id to `botster-hub`. The approved plan uses the same routing.

## Guidance applied

The implementation used these playbooks:

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[project-pipelines-playbook]]
- [[botster runtime teardown lenses]]

The implementation used these atomic notes:

- [[botster hub is a first party host profile over core]]
- [[botster Hub Rust stays a trusted host kernel]]
- [[core terminal progress is wake driven and targeted]]
- [[terminal adapters emit coalesced writable and closed wakes]]
- [[core owns duplex terminal transport while Hub stays content blind]]
- [[session registry size follows the worker applied resize]]
- [[worker resize acknowledgment precedes the next control frame]]
- [[resize completion wake durability has one ablation point and needs three core armed pumps]]
- [[core one slot adapters preserve resize input and echo wake obligations]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[exact Rust test ablations require a one test baseline]]
- [[Core subscription hard-stop is synchronous close and drop on the host tick]]
- [[Core control-plane lifecycle journal advances without a terminal client or Hub terminal Drain]]
- [[release file gated producers flush readiness before release]]
- [[live byte delivery proofs need producer readiness and a completion oracle]]
- [[webrtc starvation markers must drop pre release producer ready bytes]]
- [[observed-exit waits must issue a production exact-session observe turn]]
- [[counted quiet drain oracles still carry a wall clock floor]]

The implementation also used the project identity and goals notes. No convention conflict occurred.

## Files changed

- `src/transport/webrtc/adapter.rs`
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`
- `tests/hub_daemon_lifecycle/package_fixtures.rs`
- `tests/hub_daemon_lifecycle/sessions.rs`
- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`
- `tests/hub_daemon_lifecycle/webrtc_proofs.rs`
- `docs/plans/reproduce-targeted-pump-woken-resize-with-merged-core.md`
- `docs/reports/reproduce-targeted-pump-woken-resize-with-merged-core-implement-report.md`

## Implementation result

The adapter unit test now binds a waking adapter and pumps only its named route. The test keeps the 511 retained pumps and the 512th hard-stop assertion.

The two pressured WebRTC tests now use continuous PTY output. Ingress wakes now drive the Core attempt budget. The tests keep all close, identity, sibling, and cleanup assertions.

The resize proof now verifies the persisted `entity-session.json` record. It requires `rows == 31` and `cols == 101` after both subscriber patch assertions.

The Core candidate full suite exposed residual live-output fixtures. Those fixtures wrote final bytes and exited before delivery completed. Hub `Drain` calls supplied the old polling progress.

The migrated fixtures keep each producer live until the asserted output arrives. The tests then release and verify authoritative process exit. The implementation removed the unused immediate-exit fixture.

The exact-owner Unix process-exit proof now performs production exact-session observation after the exit release. It then requires the same `process_exit` frame before `ShutdownSession`.

No production code changed.

## Ownership boundaries

Hub changes are limited to Hub-owned tests, fixtures, the plan, and this report. Hub remains terminal-content blind.

Core still owns resize application, session-size persistence, lifecycle patches, wake batches, adapter pumping, and route retirement. This run did not change Core code or the committed Core pin.

The Hub client data transfer object already contains `rows` and `cols`. This run did not change Hub client protocol types.

Cross-repository dependencies remain unchanged:

- Closed Core resize ticket: `ticket_1788198279_441580`
- Core polling-path deletion ticket: `ticket_1787894967_973951`
- Existing Core-to-Hub dependency: `dependency_1788405916_426816`

## Plan deviations

The approved revision 2 plan named three polling-seam migrations. The Core candidate full suite exposed five more affected proofs.

The final migration covers seven residual proofs. Two additional proofs used the same obsolete immediate-exit fixture.

Human answer `question_1788410756_336404` approved the added Hub migration when diagnosis confirmed the removed polling path. The answer prohibited polling compatibility, timeout increases, and a new ticket.

Plan revision 3 records the added files, tests, red evidence, and acceptance checks. The implementation added no compatibility path and changed no timeout.

## Verification environment and provenance

- Official Rust: `rustc 1.97.0 (2d8144b78 2026-07-07)`
- Zig: `0.16.0`
- `CARGO_TARGET_DIR`: unset for every official command
- Current Hub binary: `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1788206393_323469/target/debug/botster-hub`
- Current worker binary: `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1788206393_323469/target/debug/botster-session-worker`
- Candidate Hub binary: `/private/tmp/botster-hub-candidate.oaUdJT/repo/target/debug/botster-hub`
- Candidate worker binary: `/private/tmp/botster-hub-candidate.oaUdJT/repo/target/debug/botster-session-worker`
- Old-pin Hub binary: `/private/tmp/botster-hub-old.MnpPVC/repo/target/debug/botster-hub`
- Old-pin worker binary: `/private/tmp/botster-hub-old.MnpPVC/repo/target/debug/botster-session-worker`

The current lockfile has six `48a4370` Core sources. The candidate lockfile has six `05464a1` Core sources and zero old-SHA matches outside `docs/`.

The candidate scratch tree used base commit `6e82219`. Its uncommitted test diff matched the final test code at `ceb0823`. The candidate pin override was not committed.

## Tests and downstream proof

Current Core pin:

- Resize and persisted record proof: 3 of 3 exact runs passed.
- Waking adapter unit proof: 3 of 3 exact runs passed.
- WebRTC write-budget proof: 3 of 3 exact runs passed.
- WebRTC failed-remove proof: 3 of 3 exact runs passed.
- WebRTC shutdown live-output migration: 3 of 3 exact runs passed.
- Split UTF-8 migration: 3 of 3 exact runs passed.
- Exact-owner Unix process-exit migration: 3 of 3 exact runs passed.
- Four removed-fixture users: each exact test passed.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `node packages/hub-test-support/scripts/sync-assets.mjs --check`: passed.
- `./test.sh --locked`: passed at default concurrency. The lifecycle binary reported 342 passed, 0 failed, and 2 ignored.

Wake-only Core candidate:

- All named exact tests passed. The shutdown, split UTF-8, and Unix process-exit proofs each passed 3 of 3 runs.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- Final `./test.sh --locked`: passed at default concurrency. The lifecycle binary reported 342 passed, 0 failed, and 2 ignored.

Red evidence:

- Hub `4f858173d98f5e3b32baad5b5e19decb4d584ba6` with Core `a781556` failed the resize test with exit 101. The failure stage was `lifecycle patch`, and the message was `timed out waiting for entity frame`.
- On the Core candidate, the two old silent pressure fixtures failed at their close assertions. Their statuses were 101.
- Before the residual fixture migrations, candidate full-suite runs failed the WebRTC shutdown proof twice under load.
- A later candidate run failed the split UTF-8 proof at `drain_until_subscription_deadline` with `events=[]`.
- A later candidate run failed the exact-byte and finite-producer proofs at the same live-output wait.
- A later candidate run failed the Unix process-exit proof because the exact-owner `process_exit` frame was absent.
- The final candidate full suite passed after each corresponding migration.

## Runtime teardown lenses

- Isolation: the hard-stop tests still close only the exact owner. The sibling route remains live.
- Bounds: the 512th targeted pump still closes synchronously. No new timer or retry supplies progress.
- Late messages: the failed `RemoveSession` still cannot suppress the later Core close.
- Production path: the live tests use the real Hub child, worker, PTY, Unix adapter, WebRTC adapter, and Core wake driver.
- Identity: the unit test still asserts client, session, subscription, and generation fields. The Unix proof still requires the exact-owner process-exit frame.
- Sibling policy: the pressured route fails closed. The sibling session and scoped drain remain usable.

## Unverified behavior and residual risk

`script/run-lifecycle-suite` returned `verdict=environment_dirty`. It found live Hub and worker processes from other ticket worktrees. This run did not stop processes that belong to other runs.

The direct full locked suite passed on both Core revisions despite that host state. Final integration still owns the complete cross-repository matrix and a quiet-host lifecycle-wrapper run.

The residual fixture red proofs are load-shaped. Their focused old-fixture forms can pass on an idle host. The repeated candidate full-suite failures supply the downstream-shaped red evidence.

## Vault result

No missing vault guidance blocked the work.

The implementation captured one durable gap at `ops/inbox/2026-09-02-wake-only-core-live-output-tests-hold-producer-until-delivery.md`. The note records the held-live output rule and the exact-session process-exit rule.
