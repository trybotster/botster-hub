# Make managed Git spawn cwd readiness deterministic and panic-safe

## Target repository and context loaded

- Ticket: `ticket_1785548694_519212`; run: `run_1785549874_728086`; Plan step: `botster_stack_plan`.
- Authoritative target: `tgt_7e208a0c76a44980a83b63af976b1f22`, resolving to `trybotster/botster-hub`. The run worktree has that repository as `origin` and starts from `281db04523503c5cf692813ea313344aa6067644` on branch `project-pipelines/ticket_1785548694_519212`.
- Repository charter: [[botster-hub-playbook]]. Role/surface playbooks: [[planner-playbook]], [[botster-planner-playbook]], [[botster-runtime-reviewer-playbook]], and [[project-pipelines-playbook]] for this run's artifact/checklist/gate policy only.
- Architecture maps loaded: [[botster-architecture]], [[cli-patterns]], and [[spa-patterns]]. This ticket touches the Rust Hub lifecycle-test surface; SPA guidance is context only.
- Hub charter notes loaded: [[botster hub is a first party host profile over core]], [[botster hub gravity must be watched before it becomes the new monolith]], [[botster data plane bypasses the hub through session and client actors]], [[botster local client api lives over hubruntime not raw core routers]], [[botster hub events use bounded priority lanes instead of unbounded queue fuses]], [[may supervise permits the hub to supervise the package entrypoint]], [[hub supervision admission changes require exact live hub launch proof]], [[live hub proof records distinct hub and locked core binary provenance]], [[webrtc bootstrap origin must be requested after the package server binds]], [[plugin worker queue capacity and executor concurrency are independent host profile knobs]], and [[durable state version preflight must precede shape deserialization after cold turkey changes]].
- Ticket-specific notes loaded: [[PTY integration tests poll for readiness not fixed sleeps]], [[subprocess harnesses must kill child on failed readiness]], [[subprocess reader threads drain to eof after matching a readiness marker]], [[subprocess reader threads that drop early cause broken-pipe panic on child write]], [[pty integration tests that spawn botster start must be serialized to avoid socket-path races]], [[poisoned rust mutex test locks cascade one failure across parallel suite]], [[daemon shutdown disconnects count as success only after clean owned process exit]], [[daemon probe order changes require lifecycle integration tests]], [[test script required for rust tests not cargo test]], [[a regression test must be shown to go red with the fix reverted]], [[suite wide acceptance criteria make every observed test failure in scope]], and [[loaded lifecycle ci precompiles the exact test target before synthetic cpu stress]].
- Workflow notes loaded: [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan agents must author vault context as wikilinks not home paths]], [[pipeline vault checklists must cite exact resolvable note titles]], [[vault example paths are not repository placement conventions]], [[plan steps need reviewable plan artifacts]], and [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Repository context inspected: `README.md`, `Cargo.toml`, `Cargo.lock`, `test.sh`, `.github/workflows/loaded-daemon-lifecycle.yml`, `script/run-loaded-daemon-lifecycle`, `script/run-loaded-daemon-lifecycle-selftest`, `tests/support/mod.rs`, the managed Git fixture and `live_hub_managed_git_spawn_reconciles_and_reuses_after_restart` in `tests/hub_daemon_lifecycle_test.rs`, the existing `PanicSafeCliDaemon`, production fast-exit `ProcessExit`/`ListSessions` lifecycle examples, recent lifecycle history, and related plans under `docs/plans/`.

## Observed failure and production proof boundary

`PluginMcpCallTool` returns a managed worktree path and session UUID before the PTY command is guaranteed to have executed. The test currently polls `live-managed.txt` only 100 times at 10 ms, then reads it. Under default-parallel load, scheduler delay can consume that one-second window even though the real Hub/Core/session-worker path is healthy.

The replacement readiness oracle will be the returned session UUID reaching terminal lifecycle `exited` through the production daemon `ListSessions` request. The repository's fast-exit diagnostic already treats `ProcessExit` as the primary completion boundary and reads the same canonical lifecycle through `ListSessions`; `ReadScreen` is only a fallback there and is explicitly allowed to fail after fast exit, so this plan does not use it. Observing `exited` proves the PTY-spawned shell has completed. The unchanged fixture writes the relative path `live-managed.txt`, so reading the exact expected content from `<returned-worktree>/live-managed.txt` after that lifecycle boundary proves the shell ran in the managed cwd. The existing `LOCAL_RUNTIME_DAEMON_READINESS_BUDGET` is a hang backstop, not a readiness threshold; no timeout is increased and no blind file retry remains.

## Scope

- Add the smallest test-local condition-driven helper needed to request `ListSessions` for the returned session UUID and wait within the existing runtime readiness budget for lifecycle `exited`, failing immediately on lifecycle `failed` or daemon/transport error.
- In `live_hub_managed_git_spawn_reconciles_and_reuses_after_restart`, cross that production lifecycle boundary before reading the relative marker from the returned managed worktree, while retaining the exact marker contents assertion.
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
- No workflow/runner edit. Existing `lifecycle-suite` executes this whole integration target repeatedly and `full-suite-contention` supplies the broader default-parallel proof. If an evidence gap is discovered, pause and route it to open same-target sibling `ticket_1785549893_470247`, which owns the Hub CI/evidence surface, rather than editing the workflow or runner in this run.

## Repository ownership boundaries and cross-repository dependencies

| Boundary | Owner | This ticket |
| --- | --- | --- |
| Hub integration fixture, daemon ownership guard, daemon lifecycle projection, lifecycle runner evidence | `botster-hub` | Change and prove here. |
| Reusable PTY/session-worker mechanics and lifecycle authority | `botster-core` | Consume through the pinned production path; do not patch or duplicate. |
| External Hub client DTOs | `botster-hub-client` | No contract change expected. |
| Project Pipelines workflow state | Project Pipelines plugin | Record artifacts/gates only; no plugin code change. |

No cross-repository dependency is currently required. If production `ListSessions` cannot expose terminal lifecycle for the returned session, or panic cleanup proves a live worker/shell survives correct Hub-owned shutdown because Core does not terminate/reap it, stop and register a blocking dependency against the `botster-core` target rather than adding a Hub-side file poll or broad kill workaround. The same-repository CI sibling above is coordination, not a dependency on the implementation path.

## Assumptions and unknowns

### Assumptions

- The canonical `exited` lifecycle is published only after the PTY child has completed; therefore the fixture's preceding relative file write has completed before the assertion reads it.
- The relative marker path plus exact content at the returned worktree path proves cwd without modifying the fixture to echo `$PWD`.
- The existing readiness budget is large enough because it already governs local runtime daemon readiness; the deadline protects against hangs while the terminal condition controls success.
- `PanicSafeCliDaemon::shutdown()` uses the validated shutdown classifier on normal transitions. Its panic-time `Drop` is deliberately best-effort and diagnostic; panic cleanup is proven by exact PID, zombie, metadata, and socket census, not by treating its shutdown-disconnect handling as validated success.
- The real-daemon mutex remains poison-recovering, and acceptance runs remain at Cargo's default test concurrency.

### Unknowns to resolve during implementation

- The session may be `starting` or `running` on early `ListSessions` observations; only those states (and a not-yet-projected row after a successful spawn result) remain in the condition-driven wait. `failed`, an unexpected terminal value, or daemon/transport error fails with the last session snapshot and elapsed budget.
- The ordinary fixture exits too quickly to prove panic reaping positively. Panic-path evidence must use a temporary uncommitted fixture variant that writes the same relative marker and then remains live, record the exact live session-worker and shell PIDs for the returned session, and only then inject the panic. A run with no live worker or shell at injection is invalid evidence and must be repeated.

## Affected surfaces/files

- `tests/hub_daemon_lifecycle_test.rs` — expected implementation location: semantic `ListSessions` lifecycle wait and the three `PanicSafeCliDaemon` owners. The fixture remains unchanged in the committed diff.
- `docs/plans/make-managed-git-spawn-cwd-readiness-deterministic-and-panic-safe.md` — this reviewable plan artifact.
- Verification-only, not expected edits: `test.sh`, `script/run-loaded-daemon-lifecycle`, `script/process-census`, `.github/workflows/loaded-daemon-lifecycle.yml`, `tests/support/mod.rs`, and `Cargo.toml`.

No `src/`, crate API, generated artifact, manifest, or README change is expected.

## Implementation plan

1. Add a narrow helper beside the managed Git test that takes data dir and the returned session UUID. Repeatedly issue the production `DaemonRequest::ListSessions`, locate only that session, and return on lifecycle `exited`; retain `starting`/`running` or a not-yet-projected successful spawn as transitional, fail on `failed`/unexpected state or request error, and stop at `LOCAL_RUNTIME_DAEMON_READINESS_BUDGET` with the last session snapshot in diagnostics.
2. Replace the old 100 x 10 ms file-existence loop with that lifecycle barrier, then read `live-managed.txt` exactly once from the returned worktree and retain `"live-managed\n"` equality. The unchanged relative-path fixture plus the terminal lifecycle supplies the cwd and write-completion proof.
3. Replace `first_child`, `competing_child`, and `second_child` with `PanicSafeCliDaemon` instances as soon as each daemon starts. Replace normal raw-child shutdown calls and redundant output checks with the guard's validated consuming `shutdown()`. Keep explicit session shutdown before final daemon shutdown. Do not touch unrelated raw children.
4. Inspect the final diff for any change beyond this test surface. If production behavior appears necessary, pause for attribution and dependency routing rather than broadening silently. If CI evidence plumbing is insufficient, route to `ticket_1785549893_470247` rather than editing its owned files.

## Risks and mitigations

- **Wrong session or nonterminal state creates false readiness:** match the exact returned session UUID and require canonical lifecycle `exited`; `starting`/`running` are not success.
- **Lifecycle polling hides terminal errors:** fail on `failed`, unexpected lifecycle, wrong response kind, or transport error with the last full session snapshot.
- **Cwd proof is weakened:** retain the unchanged relative-path write and exact path/content assertion after process completion; lifecycle alone is not the cwd assertion.
- **Panic cleanup hides the original assertion:** keep `Drop` cleanup best-effort and diagnostic as the established guard does; the inducing panic remains primary.
- **Intentional restart semantics are weakened:** use the same validated daemon shutdown path during first-to-second Hub restart and preserve the test's reconciliation/reuse assertions.
- **Parallel-test contamination:** track exact daemon/session identities and rely on the existing recovering real-daemon guard plus runner run-token census; never use broad `pgrep` or `pkill` as proof.
- **Panic proof passes vacuously:** use a temporary blocking fixture, require exact live worker and shell PID observations before injecting panic, then require each observed PID to be absent and non-zombie afterward.
- **Ablation fails for the wrong reason:** delay only the generated fixture command during the temporary negative-control run, restore it afterward, and require the old one-second file poll to fail specifically at `live managed cwd marker` while the lifecycle version passes under the same delay.

## Acceptance checks and downstream proof

1. **Formatting and static gates**
   - `cargo fmt --all -- --check`
   - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
   - `git diff --check`

2. **Focused production-path regression**
   - Precompile through the repository wrapper: `./test.sh --test hub_daemon_lifecycle_test --no-run`.
   - Run the exact test through the wrapper at default concurrency: `./test.sh --test hub_daemon_lifecycle_test live_hub_managed_git_spawn_reconciles_and_reuses_after_restart -- --exact --nocapture`.
   - Run it repeatedly at default concurrency in a bounded loop or existing lifecycle campaign, recording exact command, count, elapsed time, and zero failures. A serial rerun may diagnose a failure but cannot satisfy acceptance.
   - The passing log must positively show the returned session reaching `exited`, marker equality at the returned worktree path, competing-Hub rejection, restart reconciliation, reused worktree, distinct second session, explicit session shutdowns, and clean daemon shutdowns.

3. **Red-on-old-path / ablation proof**
   - In a temporary uncommitted negative-control worktree state, keep the generated fixture delayed beyond the old one-second ceiling and restore the old 100 x 10 ms file poll while retaining the final assertion. Run the exact test and require a nonzero exit at the original `live managed cwd marker` boundary.
   - Restore the semantic `ListSessions` lifecycle barrier while retaining the same fixture delay; require the exact test to pass without raising the readiness budget. Restore the fixture delay before committing. Preserve patch/command/status excerpts as a run artifact. An unrelated failure or printed panic with exit zero is not ablation proof.

4. **Panic-safe ownership proof**
   - In a temporary uncommitted proof state, make the fixture write the same relative marker and then remain live. After the returned session is observably `running`, record the exact live Hub, session-worker, and shell PIDs attributable to that session; a missing live worker or shell invalidates the run.
   - Inject an intentional panic while `PanicSafeCliDaemon` owns the daemon. Require the test command to fail for that panic and require every recorded PID to become absent and non-zombie, with daemon metadata and local/session sockets absent, before any runner cleanup acts. Remove both temporary changes and rerun the committed path green with the same residual census.

5. **Repository and loaded acceptance**
   - Run `./test.sh` at default Cargo concurrency. Because this is a suite-wide gate, every observed failure blocks until exactly attributed or human-rescoped; poison cascades do not waive the first root failure.
   - On the exact committed SHA, use the existing loaded workflow/runner with exact-target precompile and `stress_profile=residual-tail` for repeated `lifecycle-suite` runs, plus repeated `full-suite-contention` runs if required by the ticket's final acceptance campaign. Preserve Hub SHA, locked Core SHA, runner/platform, repetition count, default-parallel command, per-run result, cleanup status, and process-census artifacts.
   - Green and injected-panic evidence must end with zero new Hub/session-worker/shell live survivors, zero zombie survivors, zero stale owned daemon/session sockets, and `cleanup_status=0`. Do not inflate the 900-second per-run budget or serialize the suite to obtain green.

6. **Runtime wiring proof**
   - Review the final test trace to show that success depends on `PluginMcpCallTool` returning a real session UUID, production `ListSessions` observing that exact session reach `exited`, and only then the unchanged relative-path marker assertion succeeding. A unit-only helper or direct filesystem wait is insufficient.

## Pipeline artifacts and gates

- Attach this committed plan as a `plan` artifact before submitting `botster_stack_plan_gate`.
- The Plan gate evidence must include every required field, exact resolvable vault note titles, the authoritative target, assumptions above, and the existing-checklist reconciliation after the create timeout.
- Plan Review must verify the artifact in current context/events and refresh target refs before approving. Implement must persist red/green commands, panic survivor census, focused/default-parallel/full-suite results, and any deviation from this plan.

## Vault gaps worth capturing

No new durable vault note is required at Plan time. The loaded notes already cover semantic PTY readiness, lifecycle integration proof, failed-readiness child cleanup, panic/drop ownership, shutdown classification, red-on-revert proof, exact-target loaded precompile, and suite-wide default-concurrency acceptance. If implementation establishes a reusable, previously undocumented rule about terminal lifecycle as the completion barrier for short-lived managed-template commands, capture it inbox-first after verification; otherwise record no capture to avoid duplicating existing readiness notes.

## Convention conflicts

None. The plan is a test-only surgical change, uses the established production daemon lifecycle projection and panic-safe guard, preserves Hub/Core ownership boundaries and managed Git semantics, avoids new abstractions and configurability, and keeps unrelated raw-child migration port-on-touch.
