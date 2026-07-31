# Make foreground app output progress deterministic under load

## Target and context

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`, resolved from the Hub spawn-target registry rather than inferred from the pipeline worktree.
- Ticket/run: `ticket_1785457482_500249`, run `run_1785458485_906751`, Plan step `botster_stack_plan`, gate `botster_stack_plan_gate`.
- Worktree/base: pipeline-owned ticket worktree at `868c617`, equal to the current `origin/main` merge base when planning began. The worktree had no pre-existing changes.
- Required base synchronization: fetched `origin/main` after the returned Plan pass, resolved it to merged PR #182 commit `868c61700c8c145e5dadca5005ae20ccf3220805`, verified that commit is an ancestor of the work branch, and ran `git merge --no-edit origin/main`. Git reported `Already up to date`; there were no conflicts, and the merge-result branch SHA before recording this evidence was `6b2f60585d0ed54f60858359e058171fc67a1dfd`.
- Repository charter: [[botster-hub-playbook]].
- Role and surface playbooks loaded: [[planner-playbook]], [[botster-planner-playbook]], [[botster-runtime-reviewer-playbook]], and [[botster-runtime-verifier-playbook]]. [[project-pipelines-playbook]] was intentionally not loaded because this ticket changes Hub tests and the Hub-owned loaded-lifecycle workflow, not Project Pipelines package/plugin code or workflow policy.
- Planner maps and orchestration context loaded: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[botster pipeline needs continuous product owner between agent steps]], [[plan agents must author vault context as wikilinks not home paths]], and [[vault example paths are not repository placement conventions]].
- Hub ownership context loaded: [[botster hub is a first party host profile over core]], [[botster hub gravity must be watched before it becomes the new monolith]], [[botster data plane bypasses the hub through session and client actors]], [[botster local client api lives over hubruntime not raw core routers]], [[botster hub events use bounded priority lanes instead of unbounded queue fuses]], [[may supervise permits the hub to supervise the package entrypoint]], [[hub supervision admission changes require exact live hub launch proof]], [[live hub proof records distinct hub and locked core binary provenance]], [[webrtc bootstrap origin must be requested after the package server binds]], [[plugin worker queue capacity and executor concurrency are independent host profile knobs]], and [[durable state version preflight must precede shape deserialization after cold turkey changes]].
- Ticket-specific and verification notes loaded: [[apps cli uses exact selectors and daemon resolved terminal launch contracts]], [[foreground terminal app open conformance belongs in hub test support]], [[botster core hosts need an explicit drain loop contract]], [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[test script required for rust tests not cargo test]], and [[a regression test must be shown to go red with the fix reverted]].
- Initial pipeline context contained no prior artifacts, reviews, findings, dependencies, questions, or answers. Plan Review returned the artifact with one blocker, two high, two medium, and one low finding: correct the contradicted early-readiness diagnosis, investigate the external `stty`/ISIG boundary, distinguish signal failure from terminal-restore stalls, use a mechanism-matched negative control, make full-suite contention the primary loaded proof, and reconcile checklist note evidence. No dependency or human question was added.

## Current repository and failure evidence

- The production path is the operator console in `src/operator_console.rs` dispatching `apps open` to `open_terminal_app` in `src/main.rs`. It resolves the launch through the daemon, gives the foreground app its own process group and controlling terminal, inherits the PTY streams, waits for exit, restores the console process group, reports a nonzero/signal outcome, and prints the next prompt.
- The failing coverage is `cli_operator_console_starts_reuses_detaches_handles_ctrl_c_and_stops` in `tests/hub_daemon_lifecycle_test.rs`. Its fixture runs `stty raw -echo; stty isig; printf 'foreground-ready\r\n'; sleep 300`, then the test sends Ctrl-C as soon as the reader observes the marker.
- Early readiness by itself cannot explain the observed silence. The child `pre_exec` establishes the new process group and terminal foreground before `sh` executes and resets the shell's SIGINT disposition to default. If Ctrl-C is generated between `printf` and `sleep`, it kills the foreground shell and the console reports `foreground app terminated by signal 2`; that would satisfy the existing second-occurrence barrier.
- A distinct fixture defect does match the symptom: `stty raw` disables ISIG, and a separate external `stty isig` must re-enable it. The shell does not use `set -e`, so a failed second `stty` can still be followed by `foreground-ready`; with ISIG off, byte `0x03` becomes ordinary input and `sleep` remains alive. Residual-tail launches 12 busy-loop workers per CPU (capped at 64), making fork/exec and scheduling pressure relevant. The failed workflow transcript contains no visible `stty`, fork, or resource-unavailable error, so this remains a candidate to prove rather than an established root cause.
- The same test uses cumulative substring occurrence counts over the complete PTY transcript. That does not identify output produced after a particular input action and is the stale-state barrier rejected by [[a regression test must be shown to go red with the fix reverted]].
- Workflow `30590359513` confirms the shape under residual-tail pressure: repetitions 1-3 passed; repetition 4 printed `foreground-ready`, then produced neither a second `foreground app ` line nor a returned prompt before the 30-second liveness backstop. The harness terminated the stuck console, and cleanup reported no run-token or session survivors.
- The missing completion line has another production-shaped explanation that transcript-only evidence cannot distinguish: after the foreground app exits, `open_terminal_app` must restore the console foreground process group with `tcsetpgrp`, then the console restores termios with `tcsetattr(TCSADRAIN)`, before it prints the foreground outcome. A stall in either restore step also yields no completion line or prompt.
- The neighboring focused test `operator_console_ctrl_c_reaches_foreground_app_process_group_and_returns_prompt` already uses the stronger fixture contract: the long-lived Node child installs its SIGINT handler before printing its readiness marker, while the shell waits with SIGINT ignored. That test exercises the same production process-group handoff and expects exit code 130.
- The loaded-lifecycle harness currently has no exact operator-console target. `focused-cli-smoke` runs a different smoke test, so repeated residual-tail proof for this ticket presently requires the whole suite.

## Scope

1. Add attribution diagnostics to the test-owned `OperatorConsolePty` before selecting a fix. On a foreground-progress timeout, capture the console child's wait status, the PTY's effective termios/ISIG state, its foreground process-group id, and a process census for that group before cleanup terminates anything.
2. Use those observations plus a mechanism-matched deterministic reproduction to decide among: line discipline never generated SIGINT; the byte was generated for the wrong foreground group; or the foreground group exited but console terminal restoration/output progress stalled.
3. If attribution lands on the fixture's external `stty`/ISIG boundary, replace it with the neighboring proven child-owned acknowledgement: no external `stty` establishes signal semantics, the long-lived child installs its handler before it prints readiness, and the shell waits with SIGINT ignored. If attribution instead establishes a production handoff/restore defect, stop and return the evidence to Plan before editing product code.
4. Give `OperatorConsolePty` a post-action output observation primitive: capture a byte offset/checkpoint before input and wait for an exact marker only in bytes appended after that checkpoint. Keep the existing bounded liveness backstop, add the attribution snapshot above, and do not present the checkpoint itself as root-cause evidence.
5. After sending Ctrl-C, require post-checkpoint evidence for the exact foreground completion (`exited with code 130`) and subsequent prompt. Keep the broad lifecycle and focused process-group scenarios on the same proven fixture contract without rewriting unrelated waits.
6. Add a `focused-operator-console` loaded-lifecycle target that invokes the existing exact broad test through `./test.sh`; this is supporting branch/base attribution evidence, not a substitute for full-suite contention.
7. Preserve and verify owned-daemon, PTY/foreground children, session, fixture-directory, socket, and metadata cleanup on success and every diagnostic/timeout path.

## Non-scope

- No timeout increase, blind retry, sleep-as-success-oracle, or relaxation/removal of the foreground completion and prompt assertions.
- No production `src/main.rs` or `src/operator_console.rs` behavior change in this plan. If attribution shows the foreground group exited but `tcsetpgrp`, `tcsetattr(TCSADRAIN)`, outcome printing, or prompt progress stalled—or shows the correct foreground group had ISIG enabled but received no signal—stop and return to Plan with the evidence.
- No change to daemon launch resolution, app selectors, package manifests, public client DTOs, Core session/worker behavior, terminal data-plane routing, or package supervision policy.
- No `botster-core`, client-repository, TUI, Web, Ghostty, Project Pipelines plugin, or npm package change.
- No broad cleanup of cumulative waits elsewhere in the lifecycle test. Port the checkpoint pattern only where this ticket touches an action whose output can repeat.
- The shared child-owned SIGINT fixture does not mutate termios, so this ticket no longer combines raw-mode mutation with signal termination in one fixture. Clean- and failure-exit raw-mode restoration remain covered by the broad lifecycle test; restore-after-signal in raw mode is a recorded coverage delta rather than a reason to reintroduce an external `stty` dependency into the deterministic interrupt contract.

## Ownership boundaries and cross-repository dependencies

- `botster-hub` owns the operator-console process-group/terminal handoff, the Hub integration fixture, and the repository's loaded lifecycle workflow. The observed race and the planned surgical test/harness correction are wholly Hub-owned.
- `botster-core` and `botster-session-worker` own session PTYs, not the foreground package app launched directly by the Hub CLI. No Core API or dependency change is indicated.
- The installed terminal app's runnable contract remains daemon-resolved as required by [[apps cli uses exact selectors and daemon resolved terminal launch contracts]]. The fixture changes only the test package's command behavior.
- No external client contract changes. `botster-hub-test-support` conformance is adjacent but does not own this operator-console PTY interaction and is not expected to change.
- No cross-repository prerequisite is registered. If deterministic evidence identifies a Core/session-worker defect instead, create a dependency against Core target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` rather than broadening this Hub run.

## Implementation plan

1. Retain the portable-pty master handle in `OperatorConsolePty`. Before the timeout helper terminates the console, use the existing `MasterPty::process_group_leader`/raw fd plus `libc::tcgetattr` to record foreground pgid and whether `c_lflag & ISIG` is set. Use `libc::killpg(pgid, 0)` and exact `libc::kill(pid, 0)` probes as the liveness authority. Capture human-readable process detail portably with `ps -axo pid=,ppid=,pgid=,stat=,command=` and filter the requested pgid in Rust; the portable common column set omits `sid`, while the loaded harness retains its independent SID-scoped cleanup census. Treat a zero-row census that disagrees with a live syscall probe as inspection failure. Include failures to inspect each field rather than converting missing diagnostics into a pass.
2. Run a transient mechanism-matched ablation against the current fixture: intentionally leave ISIG disabled after `stty raw -echo`, emit the same readiness marker, receive byte `0x03`, and fail at the same missing foreground-completion barrier with an alive foreground group and `ISIG=false`. Temporarily use the test-local liveness budget, verify the enclosing test command exits nonzero, then restore the committed deterministic fixture. This proves the candidate mechanism can produce workflow `30590359513`'s exact symptom; it does not alone prove that the historical `stty isig` failed, and no permanently failing reproduction belongs in the suite.
3. Run the unmodified current fixture with the new diagnostics under residual-tail/default-parallel pressure before changing it, bounded to three full-suite repetitions or the configured campaign deadline, whichever arrives first. Decision ledger:
   - `ISIG=false` plus a live foreground shell/leaf means the fixture's line-discipline setup is the defect.
   - `ISIG=true` but the PTY foreground pgid/census identifies the console rather than the fixture shell/leaf means terminal ownership is wrong; stop and return to Plan.
   - `ISIG=true` plus a live correct fixture foreground group means signal/input delivery is a product-path defect; stop and return to Plan.
   - No live foreground process plus a console still waiting, with the PTY foreground pgid either old or restored, means restoration/output progress is a product-path defect; stop and return to Plan.
   - A dead console or inspection failure needs its exact status/error resolved before choosing a fix.
   - If the bounded campaign does not reproduce the foreground failure, record non-reproduction rather than claiming attribution, then proceed with the child-owned fixture only on the independent ground that its acknowledgement contract is strictly stronger. Production changes still require positive product-path evidence.
4. Only on the fixture-defect branch, reuse the neighboring deterministic interrupt script in both tests: the shell ignores SIGINT, the long-lived child registers its own handler before printing readiness, and no external `stty` command establishes the SIGINT-relevant terminal state. Have the shell wait for code 130.
5. Add a small post-action byte checkpoint helper. It must snapshot the synchronized capture length, search only the suffix after that position, preserve byte correctness across partial UTF-8 chunks, and report full transcript, suffix, console status, ISIG, foreground pgid, and group census on failure. Add helper coverage proving stale identical output cannot satisfy it.
6. In the broad lifecycle test, checkpoint after readiness and before Ctrl-C, then require the exact post-checkpoint code-130 outcome and a new prompt. Preserve sentinel-session and later inline/idle Ctrl-C assertions.
7. Add `focused-operator-console` to `script/run-loaded-daemon-lifecycle` validation/dispatch and `.github/workflows/loaded-daemon-lifecycle.yml`. Keep the exact broad test name stable for authoritative-base comparison.
8. Run the mechanism negative control, focused checks, strict gates, 20-repetition focused branch/base support runs, and the 20-repetition full-suite branch campaign below. Record immutable Hub SHA, locked Core SHA, fresh target realpaths, attribution snapshots, per-repetition results, first-red attribution, and cleanup censuses.

## Assumptions and unknowns

- Assumption: none of the candidate mechanisms is established yet. Early readiness alone is ruled out because the foreground shell has default SIGINT and would report signal 2; external `stty`/ISIG failure matches the silence but the failed transcript contains no visible `stty` or fork error; a foreground-exit/terminal-restore stall remains possible.
- Assumption: a suffix/checkpoint wait is the smallest deterministic observation contract. It distinguishes progress caused by the current action from identical earlier transcript text without changing production output.
- Assumption: retaining the exact broad test name is necessary for same-harness authoritative-base comparison.
- Unknown: the effective ISIG bit, foreground pgid, and foreground-group liveness at the historical timeout. The new snapshot must capture these before cleanup in any reproduced failure.
- Unknown: whether the foreground child remained alive or exited before one of the two terminal restores. The group census plus PTY foreground pgid distinguishes these states.
- Unknown: whether authoritative base reproduces the intermittent red during a bounded focused campaign. Base results are attribution evidence, not permission to weaken the branch negative control.
- Assumption: 20 full-suite residual-tail repetitions fit the configured 19,800-second campaign budget: the observed repetitions were roughly 430 seconds, so the primary proof is expected to consume about 8,600 seconds. If actual runtime threatens the bounded campaign limit, stop with completed-count/timing evidence and ask the pipeline owner rather than silently reducing the proof.
- No convention conflict or waiver is known. The plan keeps behavior in Hub, uses existing Rust/PTY primitives and repository harnesses, avoids a speculative abstraction, and preserves current production behavior.

## Affected surfaces and likely files

- `docs/plans/make-foreground-app-output-progress-deterministic-under-load.md` — durable Plan artifact.
- `tests/hub_daemon_lifecycle_test.rs` — output checkpoint helper/coverage and deterministic foreground fixture assertions.
- `script/run-loaded-daemon-lifecycle` — exact focused operator-console campaign target.
- `.github/workflows/loaded-daemon-lifecycle.yml` — workflow-dispatch choice for that target.
- `src/main.rs` and `src/operator_console.rs` — inspected production entry points; expected unchanged. Any edit requires deterministic product-bug evidence and a synchronized plan update.

## Risks and mitigations

- **The plan fixes an unproven cause:** capture ISIG, foreground pgid, group processes, and console status before mutation; choose the fixture branch only when evidence lands there.
- **External `stty` silently leaves ISIG disabled:** the replacement fixture must not use a separate external process to establish SIGINT semantics. If terminal shaping truly must remain, verify effective ISIG and fail before readiness.
- **Foreground app exits but restoration stalls:** treat an empty foreground group plus live console/no outcome as product evidence and return to Plan; do not hide it with a fixture rewrite.
- **A new helper passes on stale output:** make the checkpoint an absolute byte position and require the needle wholly after it; prove stale identical output does not satisfy the wait.
- **Fixture stops resembling production:** continue using the real daemon-resolved `foreground_stdio` launch, inherited PTY, separate process group, terminal handoff, Ctrl-C byte, child wait status, restoration, and prompt path.
- **Raw-mode restore-after-signal coverage narrows:** the deterministic interrupt fixture intentionally avoids external termios setup, while the broad lifecycle test still proves raw-mode restoration after clean and failure exits. Keep this coverage delta explicit; do not restore it by recreating the two-command `stty raw`/`stty isig` race.
- **Workflow surface grows without value:** add one exact target only because the ticket explicitly requires repeated loaded branch/base proof; do not add knobs or duplicate harness logic.
- **A panic leaks the now-stuck child or daemon:** preserve RAII cleanup, process-group termination/reaping, typed stopped-status proof, and socket/metadata absence checks.
- **Loaded green hides unrelated leftovers:** require the harness's independent run-token and session censuses plus explicit Hub, worker, fixture-shell, zombie, socket, test/load/sampler group evidence.
- **Base red is misused as a waiver:** compare exact immutable inputs and first failure; require branch-focused green and regression ablation regardless of base behavior.

## Acceptance checks and downstream proof

1. Regression negative control:
   - With the current fixture shape, intentionally omit/force failure of the ISIG re-enable after `stty raw -echo`. Emit the same `foreground-ready`, send Ctrl-C, and show the exact test-local path exits nonzero at the missing `foreground app ` barrier while diagnostics report `ISIG=false` and a live foreground group.
   - Restore the attributed fix and show the identical command passes with post-checkpoint code-130 outcome and prompt. A printed panic with process exit zero is not evidence.
   - Separately ablate the byte checkpoint to prove stale identical output cannot satisfy post-action progress; label this helper strictness proof separately from root-cause reproduction.
2. Focused local checks:
   - `./test.sh --test hub_daemon_lifecycle_test <new-output-checkpoint-test> -- --exact --nocapture`
   - `./test.sh --test hub_daemon_lifecycle_test operator_console_ctrl_c_reaches_foreground_app_process_group_and_returns_prompt -- --exact --nocapture`
   - `./test.sh --test hub_daemon_lifecycle_test cli_operator_console_starts_reuses_detaches_handles_ctrl_c_and_stops -- --exact --nocapture`
   - Repeat the exact broad test under default parallel suite pressure, not only with `--test-threads=1`.
3. Repository gates:
   - `cargo fmt --check`
   - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
   - `./test.sh`
4. Loaded exact-path proof:
   - Dispatch `focused-operator-console` with `stress_profile=residual-tail` and 20 repetitions against the immutable branch SHA.
   - Dispatch the same workflow harness, target, profile, and repetition count against the authoritative base SHA. Record whether base reproduces; do not require a base red as the branch's only negative proof.
5. Broader lifecycle proof:
   - Treat focused runs as supporting evidence only. Run `full-suite-contention` under `residual-tail` for 20 green repetitions on the immutable branch SHA; this is the primary suite-pressure proof.
   - Attribute any first red with the same-input authoritative base rather than declaring it pre-existing. A base match does not complete the branch campaign; resume/restart until the required branch proof completes or ask the pipeline owner with exact timing/failure evidence.
   - Implementation outcome: workflow `30601821409` passed every suite in repetition 1, then stopped in repetition 2 only at the untouched `cli_shutdown_waits_for_metadata_owned_runtime_daemon_cleanup` assertion after both foreground-console tests passed. Human answer `question_1785470478_876698` explicitly rescoped this ticket to the owned same-scope comparison—implementation workflow `30601806607` passed 20/20 while authoritative-base workflow `30603039894` reproduced the exact foreground timeout on repetition 4—and created `ticket_1785470554_126900` for the independently attributed shutdown defect. No additional full-suite 20/20 rerun is required unless a foreground-console test regresses.
6. Runtime provenance and cleanup:
   - Record the exact Hub subject SHA, lockfile-pinned Core SHA, and Hub/session-worker realpaths under the fresh subject target.
   - Require each run and final cleanup to report zero live Hub daemons, session workers, foreground fixture shells/children, zombies, stale runtime sockets/metadata, run-token descendants, and session survivors.
   - Require test, load, and sampler process groups to be gone and `cleanup_status=0`.

This ticket is intentionally production-behavior-preserving unless attribution proves otherwise. The changed path is the test's acknowledgement/observation contract, but it must exercise the real runtime path: a real Hub binary resolves the package app through the daemon, launches it on the console PTY, transfers foreground terminal ownership, observes Ctrl-C, waits/reaps the app, restores terminal ownership, emits the foreground outcome, and accepts the next command. Code-presence or helper-only tests are not sufficient.

## Vault gaps worth capturing

- If attribution confirms the ISIG branch, capture one durable gotcha: foreground fixtures must not let a non-fatal external `stty` command silently establish the line-discipline state on which Ctrl-C progress depends.
- If attribution instead finds a restore stall, capture the proven ordering/blocking boundary for `tcsetpgrp`, `tcsetattr(TCSADRAIN)`, outcome emission, and prompt progress.
- Capture the post-action PTY checkpoint pattern if it proves reusable: cumulative transcript occurrence counts cannot establish progress after an action when identical output already exists.
- Do not capture either as established knowledge during Plan; current evidence supports the plan, but implementation/ablation must prove the durable claim first.
