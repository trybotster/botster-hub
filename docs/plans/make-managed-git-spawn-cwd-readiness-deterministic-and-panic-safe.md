# Make managed Git spawn cwd readiness deterministic and panic-safe

## Target repository and context loaded

- Ticket: `ticket_1785548694_519212`; run: `run_1785549874_728086`; Plan step: `botster_stack_plan`.
- Authoritative target: `tgt_7e208a0c76a44980a83b63af976b1f22`, resolving to `trybotster/botster-hub`. The run worktree has that repository as `origin` and starts from `281db04523503c5cf692813ea313344aa6067644` on branch `project-pipelines/ticket_1785548694_519212`.
- Repository charter: [[botster-hub-playbook]]. Role/surface playbooks: [[planner-playbook]], [[botster-planner-playbook]], [[botster-runtime-reviewer-playbook]], and [[project-pipelines-playbook]] for this run's artifact/checklist/gate policy only.
- Architecture maps loaded: [[botster-architecture]], [[cli-patterns]], and [[spa-patterns]]. This ticket touches the Rust Hub lifecycle-test surface; SPA guidance is context only.
- Hub charter notes loaded: [[botster hub is a first party host profile over core]], [[botster hub gravity must be watched before it becomes the new monolith]], [[botster data plane bypasses the hub through session and client actors]], [[botster local client api lives over hubruntime not raw core routers]], [[botster hub events use bounded priority lanes instead of unbounded queue fuses]], [[may supervise permits the hub to supervise the package entrypoint]], [[hub supervision admission changes require exact live hub launch proof]], [[live hub proof records distinct hub and locked core binary provenance]], [[webrtc bootstrap origin must be requested after the package server binds]], [[plugin worker queue capacity and executor concurrency are independent host profile knobs]], and [[durable state version preflight must precede shape deserialization after cold turkey changes]].
- Ticket-specific notes loaded: [[PTY integration tests poll for readiness not fixed sleeps]], [[subprocess harnesses must kill child on failed readiness]], [[subprocess reader threads drain to eof after matching a readiness marker]], [[subprocess reader threads that drop early cause broken-pipe panic on child write]], [[pty integration tests that spawn botster start must be serialized to avoid socket-path races]], [[poisoned rust mutex test locks cascade one failure across parallel suite]], [[daemon shutdown disconnects count as success only after clean owned process exit]], [[daemon probe order changes require lifecycle integration tests]], [[test script required for rust tests not cargo test]], [[a regression test must be shown to go red with the fix reverted]], [[suite wide acceptance criteria make every observed test failure in scope]], and [[loaded lifecycle ci precompiles the exact test target before synthetic cpu stress]].
- Workflow notes loaded: [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan agents must author vault context as wikilinks not home paths]], [[pipeline vault checklists must cite exact resolvable note titles]], [[vault example paths are not repository placement conventions]], [[plan steps need reviewable plan artifacts]], and [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Repository context inspected: `README.md`, `Cargo.toml`, `Cargo.lock`, `test.sh`, `.github/workflows/loaded-daemon-lifecycle.yml`, `script/run-loaded-daemon-lifecycle`, `script/run-loaded-daemon-lifecycle-selftest`, `tests/support/mod.rs`, the managed Git fixture and `live_hub_managed_git_spawn_reconciles_and_reuses_after_restart` in `tests/hub_daemon_lifecycle_test.rs`, the existing `PanicSafeCliDaemon`, production `ReadScreen` examples, recent lifecycle history, and related plans under `docs/plans/`.

## Observed failure and production proof boundary

`PluginMcpCallTool` returns a managed worktree path and session UUID before the PTY command is guaranteed to have executed. The test currently polls `live-managed.txt` only 100 times at 10 ms, then reads it. Under default-parallel load, scheduler delay can consume that one-second window even though the real Hub/Core/session-worker path is healthy.

The replacement readiness oracle will be a terminal marker emitted by the managed session command after it writes `live-managed.txt`. The test will poll the production daemon `ReadScreen` request for the returned session UUID until the exact marker includes the canonical managed worktree path. That observation traverses the real path—plugin capability request to Hub-managed worktree/session spawn, CoreDaemon/session worker PTY execution, retained terminal state, and Hub daemon readback—and proves the shell executed in the managed cwd before the test reads the filesystem marker. The existing `LOCAL_RUNTIME_DAEMON_READINESS_BUDGET` is a hang backstop, not a readiness threshold; no timeout is increased and no blind file retry remains.

## Scope

- Change the generated managed Git session fixture so its script writes `live-managed.txt` and then prints a stable cwd-readiness record containing its actual `$PWD`.
- Add the smallest test-local condition-driven helper needed to request `ReadScreen` for the returned session UUID, fail immediately on terminal daemon/readback errors, and wait within the existing runtime readiness budget for the exact expected cwd record.
- In `live_hub_managed_git_spawn_reconciles_and_reuses_after_restart`, wait for that production readiness record before reading the marker, while retaining the exact marker contents assertion.
- Replace this test's three raw daemon `Child` owners (first, competing, restarted) with `PanicSafeCliDaemon`; use its consuming `shutdown()` on normal transitions so `Drop` owns every assertion/readiness unwind.
- Preserve the existing start, competing-Hub branch rejection, restart reconciliation, worktree policy errors, reuse, distinct-session, explicit session shutdown, and clean final shutdown assertions.
- Produce deterministic red/green and survivor evidence for the old readiness path, normal path, and an intentionally injected panic path.

Every implementation line must implement the semantic readiness oracle, transfer this test's existing raw daemon ownership into the established panic-safe guard, preserve an existing assertion, or provide the required regression evidence.

## Non-scope

- No production managed Git, worktree, session-template, spawn-target, PTY, daemon, or restart semantics change without new positive attribution.
- No timeout inflation, blind retry, fixed sleep as readiness, weakened cwd/file assertion, serial-only acceptance, or broad process-name cleanup.
- No new public option, test-only production knob, generic readiness framework, service/manager abstraction, or adjacent refactor.
- No bulk retrofit of the roughly 70 unrelated raw-`Child` lifecycle tests. Apply `PanicSafeCliDaemon` when those tests are otherwise touched.
- No Project Pipelines plugin/package, SPA, TUI, Rails, protocol DTO, package manifest, documentation contract, or cross-repository implementation change.
- No workflow/runner edit unless implementation proves the existing closed-enum `lifecycle-suite` and `full-suite-contention` paths cannot record the required exact test and residual-tail evidence. Existing runner capability is sufficient at plan time.

## Repository ownership boundaries and cross-repository dependencies

| Boundary | Owner | This ticket |
| --- | --- | --- |
| Hub integration fixture, daemon ownership guard, daemon transport readback, lifecycle runner evidence | `botster-hub` | Change and prove here. |
| Reusable PTY/session-worker mechanics and `ReadScreen` implementation | `botster-core` | Consume through the pinned production path; do not patch or duplicate. |
| External Hub client DTOs | `botster-hub-client` | No contract change expected. |
| Project Pipelines workflow state | Project Pipelines plugin | Record artifacts/gates only; no plugin code change. |

No cross-repository dependency is currently required. If the exact production `ReadScreen` path cannot expose the emitted cwd marker, or panic cleanup proves a live worker/shell survives correct Hub-owned shutdown because Core does not terminate/reap it, stop and register a blocking dependency against the `botster-core` target rather than adding a Hub-side polling substitute or broad kill workaround.

## Assumptions and unknowns

### Assumptions

- A script statement that writes the file and only then emits the exact cwd readiness line gives a happens-before boundary: observing the terminal line means the file write was attempted from the same shell cwd.
- `ReadScreen` retains the short-lived command's output through the daemon product path, as documented and exercised elsewhere in this integration file.
- The existing readiness budget is large enough because it already governs local runtime daemon readiness; the deadline protects against hangs while the terminal condition controls success.
- `PanicSafeCliDaemon::shutdown()` preserves the current validated shutdown classifier, including the rule that a disconnect is never accepted without clean owned-child exit.
- The real-daemon mutex remains poison-recovering, and acceptance runs remain at Cargo's default test concurrency.

### Unknowns to resolve during implementation

- The exact marker encoding must be chosen so PTY formatting cannot create a false match. Prefer one line with a fixed prefix plus the canonical worktree path and compare the complete expected record.
- `ReadScreen` may briefly return a typed not-ready/unknown-session condition while the returned session is being published. Classify only an explicitly transitional response as retryable; all terminal/transport errors must fail with the last typed response and elapsed budget. Do not turn arbitrary errors into retries.
- The current short-lived fixture may exit before panic cleanup census samples a worker or shell. Panic-path evidence must inject the panic at a point where the wrapper owns the Hub and must still prove exact Hub PID, any recorded session-worker/shell descendants, daemon metadata, and local socket are absent. If no descendant existed, report that fact rather than overclaiming it was reaped.

## Affected surfaces/files

- `tests/hub_daemon_lifecycle_test.rs` — expected implementation location: fixture marker, semantic `ReadScreen` wait, and the three `PanicSafeCliDaemon` owners.
- `docs/plans/make-managed-git-spawn-cwd-readiness-deterministic-and-panic-safe.md` — this reviewable plan artifact.
- Verification-only, not expected edits: `test.sh`, `script/run-loaded-daemon-lifecycle`, `script/process-census`, `.github/workflows/loaded-daemon-lifecycle.yml`, `tests/support/mod.rs`, and `Cargo.toml`.

No `src/`, crate API, generated artifact, manifest, or README change is expected.

## Implementation plan

1. Update `write_managed_git_session_package` so `bin/init.sh` writes the existing file contents first and then emits an unambiguous cwd-readiness line using the shell's actual `$PWD`. Do not make the expected path an injected value; the point is to observe what the PTY-spawned shell used.
2. Add a narrow helper beside the managed Git fixture/test that takes data dir, session UUID, and canonical expected worktree path. Repeatedly issue the production `DaemonRequest::ReadScreen`, compare for the exact expected line, check only documented transitional states as retryable, and stop at `LOCAL_RUNTIME_DAEMON_READINESS_BUDGET` with the last typed response/screen tail in diagnostics. This replaces the 100 x 10 ms marker-file loop; it does not wrap the later filesystem assertion.
3. In the live managed Git test, construct the expected readiness line from the returned worktree path, cross the `ReadScreen` barrier, then read `live-managed.txt` exactly once and retain `"live-managed\n"` equality. This proves the actual runtime path, not merely that helper code exists.
4. Replace `first_child`, `competing_child`, and `second_child` with `PanicSafeCliDaemon` instances as soon as each daemon starts. Replace normal raw-child shutdown calls and redundant output checks with the guard's validated consuming `shutdown()`. Keep explicit session shutdown before final daemon shutdown. Do not touch unrelated raw children.
5. Inspect the final diff for any change beyond this test surface. If production behavior appears necessary, pause for attribution and dependency routing rather than broadening silently.

## Risks and mitigations

- **False readiness from partial or stale text:** match the complete prefixed canonical cwd line for the newly returned session UUID, not a generic substring or file existence.
- **Readback polling hides terminal errors:** retry only the running/not-yet-published condition supported by the daemon contract; fail on transport error, wrong response kind, or terminal session failure with diagnostics.
- **Marker ordering still races the file:** write the file before printing the readiness record; then perform a single file read after observing terminal output.
- **Panic cleanup hides the original assertion:** keep `Drop` cleanup best-effort and diagnostic as the established guard does; the inducing panic remains primary.
- **Intentional restart semantics are weakened:** use the same validated daemon shutdown path during first-to-second Hub restart and preserve the test's reconciliation/reuse assertions.
- **Parallel-test contamination:** track exact daemon/session identities and rely on the existing recovering real-daemon guard plus runner run-token census; never use broad `pgrep` or `pkill` as proof.
- **Short-lived descendants make a zero count ambiguous:** record which exact PIDs were observed before cleanup and distinguish “observed then absent” from “not present at sampling.”
- **Ablation fails for the wrong reason:** delay only the generated fixture command during the temporary negative-control run, restore it afterward, and require the old one-second file poll to fail specifically at `live managed cwd marker` while the semantic readback version passes under the same delay.

## Acceptance checks and downstream proof

1. **Formatting and static gates**
   - `cargo fmt --all -- --check`
   - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
   - `git diff --check`

2. **Focused production-path regression**
   - Precompile through the repository wrapper: `./test.sh --test hub_daemon_lifecycle_test --no-run`.
   - Run the exact test through the wrapper at default concurrency: `./test.sh --test hub_daemon_lifecycle_test live_hub_managed_git_spawn_reconciles_and_reuses_after_restart -- --exact --nocapture`.
   - Run it repeatedly at default concurrency in a bounded loop or existing lifecycle campaign, recording exact command, count, elapsed time, and zero failures. A serial rerun may diagnose a failure but cannot satisfy acceptance.
   - The passing log must positively show the semantic cwd readiness observation for both expected path and returned session, marker equality, competing-Hub rejection, restart reconciliation, reused worktree, distinct second session, explicit session shutdowns, and clean daemon shutdowns.

3. **Red-on-old-path / ablation proof**
   - In a temporary uncommitted negative-control worktree state, keep the generated fixture delayed beyond the old one-second ceiling and restore the old 100 x 10 ms file poll while retaining the final assertion. Run the exact test and require a nonzero exit at the original `live managed cwd marker` boundary.
   - Restore the semantic `ReadScreen` barrier while retaining the same fixture delay; require the exact test to pass without raising the readiness budget. Restore the fixture delay before committing. Preserve patch/command/status excerpts as a run artifact. An unrelated failure or printed panic with exit zero is not ablation proof.

4. **Panic-safe ownership proof**
   - Temporarily inject an intentional panic immediately after the semantic managed-cwd barrier while `PanicSafeCliDaemon` owns the daemon, run the exact test/census path, and require the test command to fail for the injected panic while cleanup evidence reaches zero exact owned Hub, observed session-worker/shell descendants, zombie rows, daemon metadata, and local socket survivors.
   - Remove the panic injection and rerun green with the same census. Record exact pre-panic owned PIDs and classify each as observed-then-absent or absent-before-sampling. The runner's cleanup cannot substitute for proving the guard's `Drop` already converged before the residual check.

5. **Repository and loaded acceptance**
   - Run `./test.sh` at default Cargo concurrency. Because this is a suite-wide gate, every observed failure blocks until exactly attributed or human-rescoped; poison cascades do not waive the first root failure.
   - On the exact committed SHA, use the existing loaded workflow/runner with exact-target precompile and `stress_profile=residual-tail` for repeated `lifecycle-suite` runs, plus repeated `full-suite-contention` runs if required by the ticket's final acceptance campaign. Preserve Hub SHA, locked Core SHA, runner/platform, repetition count, default-parallel command, per-run result, cleanup status, and process-census artifacts.
   - Green and injected-panic evidence must end with zero new Hub/session-worker/shell live survivors, zero zombie survivors, zero stale owned daemon/session sockets, and `cleanup_status=0`. Do not inflate the 900-second per-run budget or serialize the suite to obtain green.

6. **Runtime wiring proof**
   - Review the final test trace to show that success depends on `PluginMcpCallTool` returning a real session UUID and the daemon's production `ReadScreen` observing output emitted by that PTY session. A unit-only marker helper or direct filesystem wait is insufficient.

## Pipeline artifacts and gates

- Attach this committed plan as a `plan` artifact before submitting `botster_stack_plan_gate`.
- The Plan gate evidence must include every required field, exact resolvable vault note titles, the authoritative target, assumptions above, and the existing-checklist reconciliation after the create timeout.
- Plan Review must verify the artifact in current context/events and refresh target refs before approving. Implement must persist red/green commands, panic survivor census, focused/default-parallel/full-suite results, and any deviation from this plan.

## Vault gaps worth capturing

No new durable vault note is required at Plan time. The loaded notes already cover semantic PTY readiness, failed-readiness child cleanup, panic/drop ownership, shutdown classification, red-on-revert proof, exact-target loaded precompile, and suite-wide default-concurrency acceptance. If implementation establishes a reusable, previously undocumented rule that a `ReadScreen` marker printed after a filesystem write is the correct managed-template cwd barrier, capture it inbox-first after verification; otherwise record no capture to avoid duplicating existing readiness notes.

## Convention conflicts

None. The plan is a test-only surgical change, uses the established production daemon/readback and panic-safe guard, preserves Hub/Core ownership boundaries and managed Git semantics, avoids new abstractions and configurability, and keeps unrelated raw-child migration port-on-touch.
