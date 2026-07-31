# Make foreground app output progress deterministic under load

## Target and context

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`, resolved from the Hub spawn-target registry rather than inferred from the pipeline worktree.
- Ticket/run: `ticket_1785457482_500249`, run `run_1785458485_906751`, Plan step `botster_stack_plan`, gate `botster_stack_plan_gate`.
- Worktree/base: pipeline-owned ticket worktree at `868c617`, equal to the current `origin/main` merge base when planning began. The worktree had no pre-existing changes.
- Repository charter: [[botster-hub-playbook]].
- Role and surface playbooks loaded: [[planner-playbook]], [[botster-planner-playbook]], [[botster-runtime-reviewer-playbook]], and [[botster-runtime-verifier-playbook]]. [[project-pipelines-playbook]] was intentionally not loaded because this ticket changes Hub tests and the Hub-owned loaded-lifecycle workflow, not Project Pipelines package/plugin code or workflow policy.
- Planner maps and orchestration context loaded: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[botster pipeline needs continuous product owner between agent steps]], [[plan agents must author vault context as wikilinks not home paths]], and [[vault example paths are not repository placement conventions]].
- Hub ownership context loaded: [[botster hub is a first party host profile over core]], [[botster hub gravity must be watched before it becomes the new monolith]], [[botster data plane bypasses the hub through session and client actors]], [[botster local client api lives over hubruntime not raw core routers]], [[botster hub events use bounded priority lanes instead of unbounded queue fuses]], [[may supervise permits the hub to supervise the package entrypoint]], [[hub supervision admission changes require exact live hub launch proof]], [[live hub proof records distinct hub and locked core binary provenance]], [[webrtc bootstrap origin must be requested after the package server binds]], [[plugin worker queue capacity and executor concurrency are independent host profile knobs]], and [[durable state version preflight must precede shape deserialization after cold turkey changes]].
- Ticket-specific and verification notes loaded: [[apps cli uses exact selectors and daemon resolved terminal launch contracts]], [[foreground terminal app open conformance belongs in hub test support]], [[botster core hosts need an explicit drain loop contract]], [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[test script required for rust tests not cargo test]], and [[a regression test must be shown to go red with the fix reverted]].
- Pipeline context contained no prior artifacts, reviews, findings, dependencies, questions, or answers. A run-scoped vault checklist was created and reconciled after its create call timed out post-persistence.

## Current repository and failure evidence

- The production path is the operator console in `src/operator_console.rs` dispatching `apps open` to `open_terminal_app` in `src/main.rs`. It resolves the launch through the daemon, gives the foreground app its own process group and controlling terminal, inherits the PTY streams, waits for exit, restores the console process group, reports a nonzero/signal outcome, and prints the next prompt.
- The failing coverage is `cli_operator_console_starts_reuses_detaches_handles_ctrl_c_and_stops` in `tests/hub_daemon_lifecycle_test.rs`. Its fixture prints `foreground-ready` from a shell and only then starts `sleep 300`. The test sends Ctrl-C as soon as the reader observes that marker. The marker therefore acknowledges output, but it does not acknowledge that the long-lived leaf process is installed and able to observe SIGINT.
- The same test uses cumulative substring occurrence counts over the complete PTY transcript. That does not identify output produced after a particular input action and is the stale-state barrier rejected by [[a regression test must be shown to go red with the fix reverted]].
- Workflow `30590359513` confirms the shape under residual-tail pressure: repetitions 1-3 passed; repetition 4 printed `foreground-ready`, then produced neither a second `foreground app ` line nor a returned prompt before the 30-second liveness backstop. The harness terminated the stuck console, and cleanup reported no run-token or session survivors.
- The neighboring focused test `operator_console_ctrl_c_reaches_foreground_app_process_group_and_returns_prompt` already uses the stronger fixture contract: the long-lived Node child installs its SIGINT handler before printing its readiness marker, while the shell waits with SIGINT ignored. That test exercises the same production process-group handoff and expects exit code 130.
- The loaded-lifecycle harness currently has no exact operator-console target. `focused-cli-smoke` runs a different smoke test, so repeated residual-tail proof for this ticket presently requires the whole suite.

## Scope

1. Give `OperatorConsolePty` a post-action output observation primitive: capture a byte offset/checkpoint before input and wait for an exact marker only in bytes appended after that checkpoint. Keep the existing bounded liveness backstop and child-exit diagnostics.
2. Change the long operator-console lifecycle fixture to acknowledge readiness only after the signal-observing foreground child is live, using the already-proven child/handler shape from the neighboring focused process-group test.
3. After sending Ctrl-C, require post-checkpoint evidence for the exact foreground completion (`exited with code 130`) and the subsequent prompt. Do not use a global occurrence count as the completion barrier.
4. Keep the broad lifecycle scenario and focused process-group scenario aligned on the same deterministic fixture contract without broadly rewriting unrelated `wait_for` call sites.
5. Add a `focused-operator-console` loaded-lifecycle target that invokes the existing exact broad test through `./test.sh`, and expose it in the workflow input choices so branch/base residual-tail campaigns can prove the requested path without paying for unrelated suites.
6. Preserve and verify the existing owned-daemon, PTY child, session, fixture-directory, socket, and metadata cleanup behavior on success and assertion failure.

## Non-scope

- No timeout increase, blind retry, sleep-as-success-oracle, or relaxation/removal of the foreground completion and prompt assertions.
- No production `src/main.rs` or `src/operator_console.rs` behavior change unless a deterministic fixture plus post-action observation proves that the real console fails to deliver a signal or regain the terminal. If that happens, stop implementation and return the evidence to Plan Review before changing product semantics.
- No change to daemon launch resolution, app selectors, package manifests, public client DTOs, Core session/worker behavior, terminal data-plane routing, or package supervision policy.
- No `botster-core`, client-repository, TUI, Web, Ghostty, Project Pipelines plugin, or npm package change.
- No broad cleanup of cumulative waits elsewhere in the lifecycle test. Port the checkpoint pattern only where this ticket touches an action whose output can repeat.

## Ownership boundaries and cross-repository dependencies

- `botster-hub` owns the operator-console process-group/terminal handoff, the Hub integration fixture, and the repository's loaded lifecycle workflow. The observed race and the planned surgical test/harness correction are wholly Hub-owned.
- `botster-core` and `botster-session-worker` own session PTYs, not the foreground package app launched directly by the Hub CLI. No Core API or dependency change is indicated.
- The installed terminal app's runnable contract remains daemon-resolved as required by [[apps cli uses exact selectors and daemon resolved terminal launch contracts]]. The fixture changes only the test package's command behavior.
- No external client contract changes. `botster-hub-test-support` conformance is adjacent but does not own this operator-console PTY interaction and is not expected to change.
- No cross-repository prerequisite is registered. If deterministic evidence identifies a Core/session-worker defect instead, create a dependency against Core target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` rather than broadening this Hub run.

## Implementation plan

1. Add a small `OperatorConsolePty` output checkpoint type or byte-offset helper in `tests/hub_daemon_lifecycle_test.rs`. It must snapshot the synchronized capture length, search only the suffix after that position, preserve byte correctness across partial UTF-8 chunks, report the full transcript plus checkpoint/suffix diagnostics on failure, and retain early-child-exit detection.
2. Add focused helper coverage showing a pre-checkpoint duplicate marker cannot satisfy a post-checkpoint wait and a newly emitted marker can. Avoid real-time sleeps as the pass oracle; drive the fixture through PTY input/output.
3. Reuse one deterministic foreground interrupt script shape in both operator-console tests: ignore SIGINT in the shell, start a long-lived child that registers its SIGINT handler, print readiness only after registration, and have the shell wait for the child's code 130 exit.
4. In `cli_operator_console_starts_reuses_detaches_handles_ctrl_c_and_stops`, capture a checkpoint after the readiness marker and before Ctrl-C, then wait after that checkpoint for the exact code-130 foreground completion followed by a new prompt. Preserve the sentinel-session and subsequent inline/idle Ctrl-C assertions so terminal restoration remains proven.
5. Keep production files unchanged if the corrected fixture passes. If the child-side acknowledgement fires but post-checkpoint completion still fails deterministically, capture child/process-group/foreground-terminal diagnostics and return to planning; do not paper over a product bug with fixture edits.
6. Add `focused-operator-console` to `script/run-loaded-daemon-lifecycle` validation and dispatch, and to `.github/workflows/loaded-daemon-lifecycle.yml`. Keep the exact test name stable so the same workflow harness can exercise an authoritative base SHA.
7. Run the negative control, focused checks, strict repository gates, and branch/base loaded campaigns below. Record immutable Hub SHA, locked Core SHA, fresh target realpaths, per-repetition results, first-red attribution, and cleanup census artifacts.

## Assumptions and unknowns

- Assumption: the loaded failure is the fixture's false readiness boundary, not a proven production terminal-handoff bug. The log ends immediately after `foreground-ready`, and the neighboring handler-installed child fixture already proves the real process-group path.
- Assumption: a suffix/checkpoint wait is the smallest deterministic observation contract. It distinguishes progress caused by the current action from identical earlier transcript text without changing production output.
- Assumption: retaining the exact broad test name is necessary for same-harness authoritative-base comparison.
- Unknown: whether only the fixture acknowledgement is needed or whether the output checkpoint also exposes a reader/capture defect. The checkpoint helper gets focused coverage and better diagnostics so implementation can distinguish them.
- Unknown: whether authoritative base reproduces the intermittent red during a bounded focused campaign. Base results are attribution evidence, not permission to weaken the branch negative control.
- Unknown: the final repetition count affordable within the workflow budget. Default to 20 focused residual-tail repetitions for branch and base, then run the required broader campaign at the ticket's five-repetition shape unless the pipeline owner approves a different bounded count.
- No convention conflict or waiver is known. The plan keeps behavior in Hub, uses existing Rust/PTY primitives and repository harnesses, avoids a speculative abstraction, and preserves current production behavior.

## Affected surfaces and likely files

- `docs/plans/make-foreground-app-output-progress-deterministic-under-load.md` — durable Plan artifact.
- `tests/hub_daemon_lifecycle_test.rs` — output checkpoint helper/coverage and deterministic foreground fixture assertions.
- `script/run-loaded-daemon-lifecycle` — exact focused operator-console campaign target.
- `.github/workflows/loaded-daemon-lifecycle.yml` — workflow-dispatch choice for that target.
- `src/main.rs` and `src/operator_console.rs` — inspected production entry points; expected unchanged. Any edit requires deterministic product-bug evidence and a synchronized plan update.

## Risks and mitigations

- **A new helper passes on stale output:** make the checkpoint an absolute byte position and require the needle wholly after it; prove stale identical output does not satisfy the wait.
- **Readiness still precedes the signal observer:** the child, not the launching shell, emits readiness only after installing its handler.
- **Fixture stops resembling production:** continue using the real daemon-resolved `foreground_stdio` launch, inherited PTY, separate process group, terminal handoff, Ctrl-C byte, child wait status, restoration, and prompt path.
- **A fixture-only fix hides a real product defect:** retain the neighboring focused production-path test, add post-action diagnostics, and treat any deterministic failure after child acknowledgement as a stop-and-replan condition.
- **Workflow surface grows without value:** add one exact target only because the ticket explicitly requires repeated loaded branch/base proof; do not add knobs or duplicate harness logic.
- **A panic leaks the now-stuck child or daemon:** preserve RAII cleanup, process-group termination/reaping, typed stopped-status proof, and socket/metadata absence checks.
- **Loaded green hides unrelated leftovers:** require the harness's independent run-token and session censuses plus explicit Hub, worker, fixture-shell, zombie, socket, test/load/sampler group evidence.
- **Base red is misused as a waiver:** compare exact immutable inputs and first failure; require branch-focused green and regression ablation regardless of base behavior.

## Acceptance checks and downstream proof

1. Regression negative control:
   - Temporarily move the fixture readiness marker before signal-handler installation, with a deterministic test-only pause before the observer becomes live, or narrowly bypass the post-action checkpoint.
   - Run the exact affected test through `./test.sh`; it must exit nonzero at the intended completion barrier.
   - Restore the implementation and show the identical command passes. A printed panic with process exit zero is not evidence.
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
   - Run `full-suite-contention` under `residual-tail` for the ticket-required five-repetition campaign on the branch.
   - Attribute any first red with the same-input authoritative base rather than declaring all pre-existing failures irrelevant.
6. Runtime provenance and cleanup:
   - Record the exact Hub subject SHA, lockfile-pinned Core SHA, and Hub/session-worker realpaths under the fresh subject target.
   - Require each run and final cleanup to report zero live Hub daemons, session workers, foreground fixture shells/children, zombies, stale runtime sockets/metadata, run-token descendants, and session survivors.
   - Require test, load, and sampler process groups to be gone and `cleanup_status=0`.

The changed runtime path is proven by the exact operator-console integration test: a real Hub binary resolves the package app through the daemon, launches it on the console PTY, transfers foreground terminal ownership, observes Ctrl-C in the acknowledged child, waits/reaps it, restores terminal ownership, emits the foreground outcome, and accepts the next command. Code-presence or helper-only tests are not sufficient.

## Vault gaps worth capturing

- If the implementation and negative control confirm the diagnosis, capture one durable gotcha: a foreground-process readiness marker must be emitted by the signal-observing child after handler installation; launcher-shell output is not a signal-readiness acknowledgement.
- Capture the post-action PTY checkpoint pattern if it proves reusable: cumulative transcript occurrence counts cannot establish progress after an action when identical output already exists.
- Do not capture either as established knowledge during Plan; current evidence supports the plan, but implementation/ablation must prove the durable claim first.
