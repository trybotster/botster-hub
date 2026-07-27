# Add a single-terminal interactive operator console

## Target and context loaded

- Target repository: `botster-hub`.
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Authoritative target: spawn target `botster-hub` (`trybotster/botster-hub`); the assigned pipeline worktree was verified against that remote before repository inspection.
- Pipeline context: ticket `ticket_1785175248_823922`, run `run_1785175251_298034`, Plan step `botster_stack_plan`, gate `botster_stack_plan_gate`; no dependencies, prior artifacts, findings, or reviews were present when planning began. Plan Review `review_1785176188_374833` returned six findings covering exhaustive command classification, foreground child exit propagation, the session-worker prerequisite, in-flight Ctrl-C, data-dir pin enforcement, and unconditional restart/recovery proof; this revision addresses them.
- Role and repository playbooks: [[planner-playbook]], [[botster-planner-playbook]], and the exact repository charter [[botster-hub-playbook]].
- Botster maps and workflow guidance: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project-pipelines-playbook]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan agents must author vault context as wikilinks not home paths]], and [[vault example paths are not repository placement conventions]].
- Hub/runtime guidance: [[botster-runtime-reviewer-playbook]], [[botster hub is a first party host profile over core]], [[botster hub gravity must be watched before it becomes the new monolith]], [[botster data plane bypasses the hub through session and client actors]], [[botster local client api lives over hubruntime not raw core routers]], [[botster hub events use bounded priority lanes instead of unbounded queue fuses]], [[botster is a local rust runtime that can optionally federate with cloud]], [[botster packages should enforce core hub cli plugin provider boundaries]], [[botster terminal clients share one sessionio data plane subscription path]], and [[rust repo strict lints must be verified before dismissing warnings]].
- Targeted atomic notes: [[botster hub no arg summary must not touch durable home state]], [[botster hub daemon startup requires explicit data dir]], [[botster hub socket liveness requires a protocol handshake]], [[daemon probe order changes require lifecycle integration tests]], [[botster session worker requires explicit build in dogfood launchers]], [[package mutations require the running daemon owner]], [[serve daemon package reads must refresh registry after mutations]], [[apps cli uses exact selectors and daemon resolved terminal launch contracts]], [[foreground terminal app open conformance belongs in hub test support]], [[operator diagnostic remediation must survive the diagnosed failure]], and [[cold turkey migrations eliminate dual code paths and version suffixes]].
- Repository context: `src/main.rs` is the production binary entrypoint and currently contains the hand-written command dispatcher, command parsers, output renderers, no-argument summary, and `up`/`down` daemon start/reuse helpers. `src/daemon_transport.rs` owns the daemon protocol and signal-driven daemon shutdown. `tests/hub_daemon_lifecycle_test.rs` already owns real CLI/daemon lifecycle coverage under isolated short data directories and a serialized daemon-test guard. `test.sh` is the repository test wrapper. `README.md` is the canonical operator guide and `docs/plans/` is established plan prior art.

## Product decision ledger

- Human answer `question_1785175397_313498` confirms a cold-turkey replacement of [[botster hub no arg summary must not touch durable home state]]. Bare `botster-hub` in an interactive TTY becomes the primary durable product entrypoint and may create/use the resolved normal data directory. Bare no-argument non-TTY must reject before creating directories, starting processes, or mutating state. Replace the old vault convention after implementation; do not retain both rules.
- Human answer `question_1785175429_540075` defines prompt lifecycle: idle Ctrl-C cancels the current line and redraws without detaching or stopping the daemon; `exit` and EOF/Ctrl-D detach while leaving Hub running; successful `down` stops the daemon, confirms shutdown, and exits the console.
- Human answer `question_1785175612_459289` defines the command boundary by terminal ownership: inline request/response commands run in the prompt; foreground-TTY commands use one reusable suspend/handoff/restore mechanism (`apps open` now, reusable by `sessions attach` if later brought into scope); external stdin owners/daemon hosts such as `mcp-serve` and `start` are rejected with their exact explicit invocation.
- Human answer `question_1785176278_686353` defines in-flight Ctrl-C: acknowledge the interrupt immediately, let synchronous inline work and RAII cleanup finish, then restore the prompt with daemon/session state intact. Foreground handoff temporarily restores normal signal behavior so the child receives Ctrl-C. Cooperative cancellation is explicitly non-scope.
- Default unless the human directs otherwise: one console instance is pinned to the data directory resolved at startup; prompt commands do not switch runtime roots with a nested `--data-dir`.

## Scope

- Replace the current no-argument summary branch with an early TTY boundary:
  - if stdin and stdout are interactive terminals, resolve the normal Hub data directory, start or reuse its daemon through the existing production lifecycle helper, and open the console;
  - if bare invocation is noninteractive, return a clear nonzero diagnostic before config resolution can create durable state or spawn a daemon;
  - leave every explicit subcommand on its current noninteractive path.
- Add a small console loop that prints the resolved data directory, whether the daemon was started or reused, first-party package prerequisite state derived from daemon package rows, a concise command summary, and a stable prompt.
- Parse prompt lines with normal shell-word quoting and escaping, then route them through the same command dispatcher, command parsers, daemon requests, and output renderers used by explicit CLI invocations. Command errors print and return to the prompt rather than terminating the console.
- Extract only the dispatch/startup seams required to share existing behavior. The console must not mutate `hub-state.json` directly, reconstruct daemon requests independently, infer sibling repositories, or invent first-party paths.
- Start a bare daemon at console entry without running `up`: first-run package install/enable commands need a live owner before both first-party packages exist. `up` remains the explicit action that refreshes installed local packages, requires enabled `botster-web` and `botster-tui`, and starts the daily app stack.
- Reuse a live compatible daemon before resolving a session-worker binary. When a daemon must be started, require the normal colocated `botster-session-worker` artifact; do not discover sibling repositories or build dependencies at runtime. A missing worker blocks console entry with remediation that tells packaged users to install the complete distribution and source users to build the core worker beside the hub, then rerun bare `botster-hub`. Do not suggest an unavailable bare-invocation flag.
- Isolate a daemon spawned by the console from the console terminal's foreground signal group so Ctrl-C at the prompt cannot signal the daemon or its owned session workers. Preserve explicit `down` as the daemon-stop path.
- Keep `exit` and terminal EOF as detach-only console exits that leave the daemon running. Idle Ctrl-C cancels/redraws without detaching. During startup or inline work, acknowledge Ctrl-C, finish safely, and restore the prompt; during foreground handoff, the child receives normal Ctrl-C. Successful `down` stops the daemon and exits the console.
- Update operator documentation to distinguish:
  - bare interactive `botster-hub`: start/reuse daemon and attach the operator console;
  - `botster-hub start`: low-level foreground daemon;
  - `botster-hub up`: noninteractive daily package refresh/start orchestrator;
  - explicit subcommands: scriptable noninteractive commands.
- Add deterministic parser/transcript tests and real PTY/daemon integration tests without touching the real `~/.botster/hub`.

## Non-scope

- No changes to `botster-core`, `botster-hub-client`, `botster-web`, `botster-tui`, package manifests, daemon protocol DTOs, Project Pipelines plugin code, browser/TUI UI, Rails, MCP tools, or persistence schema.
- No sibling-repository discovery, first-party checkout scanning, inferred package paths, package auto-install, or hard-coded local source paths.
- No second command grammar, second package/app/session policy implementation, direct state-file writes, alternate daemon lifecycle, or subprocess invocation of `botster-hub` for ordinary prompt commands.
- No general readline framework, history persistence, completion engine, colors/themes, configurable prompts, remote console, or broad `src/main.rs` parser rewrite.
- No cooperative cancellation framework for synchronous CLI handlers; deferred safe completion is the bounded behavior for this ticket.
- No change to the behavior of explicit commands beyond the narrow dispatcher extraction and daemon process-group isolation needed by the console.

## Exhaustive console command classification

Every current `main` dispatch family is classified below. Classification is based on runtime and terminal ownership, not a second command implementation. A future or unknown command is rejected in the console by default with its exact `botster-hub ...` explicit invocation until its ownership mode is deliberately classified.

| Existing command | Console mode | Required behavior |
| --- | --- | --- |
| `start` | External-only daemon host | Reject with exact explicit invocation; the console already starts/reuses the daemon. |
| `up` | Inline request/response | Run through the existing orchestrator and restore the prompt. |
| `down` | Stop-and-exit | Run existing daemon shutdown, confirm it, then exit the console. |
| `doctor`, `smoke`, `status` | Inline request/response | Run existing handler/output and restore the prompt. |
| `sessions list`, `spawn`, `send-input`, `resize`, `detach`, `shutdown` | Inline request/response | Run the existing subcommand parser/daemon request and restore the prompt. |
| `sessions attach` | External-only stdin owner | Reject explicitly with `botster-hub sessions attach --data-dir <pinned-path> ...`; it may use the reusable handoff in a later scoped ticket. |
| `session-templates *` | Inline request/response | Run existing subcommand parser/daemon requests. |
| `spawn-targets *` | Inline request/response | Run existing subcommand parser/daemon requests. |
| `context` | Inline request/response | Run existing handler/output. |
| `shutdown` | Stop-and-exit | Give it the same console transition as `down`: confirm daemon shutdown and exit. |
| `mcp-serve` | External-only stdin owner | Reject with exact explicit invocation. |
| `open web` | Resolve then inline | Resolve the Web app through the existing app path; background Web launch prints `app_url=` and returns. |
| `open tui` | Resolve then foreground handoff | Resolve the terminal app and suspend prompt reads while its inherited stdio owns the terminal; restore the prompt for every child exit result. |
| `reload` | Inline request/response | Run the existing package reload alias. |
| `apps list`, `apps show` | Inline request/response | Run existing daemon-backed app projection. |
| `apps open` | Resolve by app kind | `web_app` is inline; `terminal_app` uses the same foreground suspend/handoff/restore path as `open tui`; unsupported kinds remain existing errors. |
| `packages *` | Inline request/response | Run all existing package parsers and daemon-owned mutations/reads. |
| `providers *` | Inline request/response | Run existing provider package projection. |
| `inspect` | External-only bounded runtime host | Reject with exact explicit invocation rather than starting a second `HubDaemon` inside the console process. |
| `run-one` | External-only bounded runtime host | Reject with exact explicit invocation rather than creating a parallel runtime inside the console process. |
| `help`, `--help`, `-h` | Console help | Print concise console help plus exact external-only guidance. |
| `exit` | Console-only detach | Exit the console without stopping the daemon. |

## Repository ownership boundaries and cross-repository dependencies

- `botster-hub` owns this work: its CLI is the thin operator adapter, its host profile owns resolved data-directory policy, daemon lifecycle, package state, app launch resolution, and local client APIs.
- `botster-core` continues to own session/PTY mechanics and worker-backed session durability. The console sends no terminal bytes through a new hub path and does not change SessionIo/ClientWorker contracts.
- `botster-hub-client` continues to own external daemon DTOs. Existing `DaemonRequest`/`DaemonResponse` shapes are sufficient; adding console-specific protocol fields would be a scope violation unless implementation proves a concrete missing contract and registers a separate dependency first.
- `botster-web` and `botster-tui` remain explicit operator-installed packages. Their paths and build/runtime behavior are not part of this repository run.
- Project Pipelines is in scope only as workflow policy/checklist evidence for this run, not as a changed package/plugin surface.
- No blocking cross-repository dependency is currently identified. If implementation discovers that daemon process-group isolation or command reuse requires a core/client contract change, stop and register that prerequisite against the appropriate target rather than broadening this ticket.

## Affected surfaces and likely files

- `src/main.rs`
  - Introduce a reusable top-level command dispatch result instead of duplicating the current `match` inside the console.
  - Detect bare interactive versus noninteractive invocation before durable startup.
  - Share resolved data-directory injection and existing parser/handler/output paths with prompt commands.
  - Extract daemon-only start/reuse preparation from `prepare_local_runtime`, which currently also refreshes/requires packages and launches Web.
  - Change `open_terminal_app` to return the child's exit result rather than calling `process::exit`; explicit CLI `main` converts that result to the same process exit code, while the console reports it and restores the prompt.
  - Keep explicit command exit codes and error prefixes stable at the outer `main` boundary.
- `src/operator_console.rs` (new, narrow binary module)
  - Own only TTY prompt I/O, shell-word tokenization, console-safe command classification, startup/status/prerequisite presentation, and exit/EOF/Ctrl-C loop state.
  - Receive callbacks or dispatch results from `main`; do not own daemon/package/app/session policy.
- `Cargo.toml` and `Cargo.lock`
  - Add direct `shell-words` `1.1.1` only if used for prompt tokenization; this is the current published version and is already present transitively in the lockfile.
  - Add direct dev-only `portable-pty` `0.9.0` only if the existing test surface cannot access a suitable PTY harness; this is the current published version and is already present transitively through core.
- `tests/hub_daemon_lifecycle_test.rs`
  - Add isolated real-binary console PTY coverage beside the existing serialized daemon lifecycle tests and reuse their short unique data directories, worker-binary setup, daemon guard, and cleanup discipline.
- `src/main.rs` unit-test module or a focused console test module
  - Cover tokenization, empty lines, quoting, parse errors, command classification, data-dir pinning/injection, and non-TTY early rejection without relying on a real home directory.
- `README.md`
  - Replace the no-arg summary documentation and update Start here/daily command guidance, command-layer distinctions, one-terminal first-run transcript, scripting behavior, and detach/down semantics.
- `docs/plans/add-single-terminal-interactive-operator-console.md`
  - This repository-routed plan and its resolved human decisions.

## Implementation outline

1. Refactor the production entrypoint around one reusable dispatcher.
   - Represent a command invocation as command plus argument vector and return a structured outcome suitable for either process exit or prompt continuation.
   - Preserve all existing explicit command parsers, daemon requests, renderers, diagnostics, and externally observed exit behavior.
   - Remove the nested `process::exit` calls from `open_terminal_app`: return the child's code/signal outcome through the dispatcher, let explicit CLI `main` exit with the historical nonzero status, and let the console render the failure and redraw.
   - Give console-only `help` and `exit` handling a narrow boundary; do not fork the parser for existing commands.
2. Establish the no-argument TTY boundary before side effects.
   - Use the standard terminal detection API on stdin/stdout.
   - Non-TTY bare invocation writes an actionable error explaining that scripts must use an explicit subcommand and exits nonzero before data-directory creation or daemon spawn.
   - Interactive invocation resolves one data directory using the existing precedence (`--data-dir` is unavailable because there is no subcommand, then `BOTSTER_HUB_DATA_DIR`, then `$HOME/.botster/hub`) and pins the console to it.
3. Start or reuse only the daemon.
   - Extract the daemon ensure/spawn/readiness portion already used by `up`; do not call `prepare_local_runtime`, because first run intentionally begins before packages are installed.
   - Continue to prove identity/readiness through the daemon protocol rather than socket existence.
   - Probe and reuse a live compatible daemon before worker lookup. Only the spawn branch resolves `botster-session-worker` beside the running hub binary (including the existing Cargo `deps/` parent fallback).
   - If the sibling worker is missing, do not spawn a degraded in-process daemon. Fail before runtime metadata is written with remediation to install both binaries or run `cargo build --locked -p botster-core --bin botster-session-worker` in a source checkout and rerun bare `botster-hub`.
   - Put a spawned daemon in a separate process group before the prompt becomes active so terminal-generated SIGINT targets the console, not the daemon/session tree.
4. Open the console and project existing state.
   - Request typed daemon status and package rows, print `data_dir`, `daemon=started|reused`, and installed/enabled/missing prerequisite states for `botster-web` and `botster-tui`, followed by concise help.
   - Do not print or discover source paths. Missing packages show explicit `packages install --path /path/to/...` guidance as operator-supplied placeholders.
   - Parse each line with `shell-words`. Before canonicalization, reject a `--data-dir` option appearing before the operand separator with a message that the console is pinned and an exact external invocation for another root; preserve a literal `--data-dir` after `--` as a child operand. Then inject the pinned data directory and invoke the shared dispatcher.
   - On parse/usage/daemon errors, print the existing diagnostic and continue. Empty lines redraw the prompt.
   - Classify commands by interaction mode: run inline request/response commands directly; use one suspend/handoff/restore mechanism for `apps open`; reject `start`, `mcp-serve`, and other external-only stdin owners with the exact explicit invocation.
   - Install console signal handling before daemon startup. At an idle prompt, Ctrl-C cancels the current line and redraws. During startup or an inline command, print a bounded acknowledgement such as `interrupt requested; finishing safely`, allow the synchronous operation and RAII cleanup to finish, clear the pending interrupt, and redraw. The foreground handoff temporarily restores ordinary terminal SIGINT behavior for the child, then reinstalls console behavior on return.
   - `exit` or EOF detaches. Successful `down` confirms daemon shutdown and exits.
5. Document and prove the actual product entrypoint.
   - Update README examples around the real `main` no-argument branch, not a scaffold helper.
   - Add a one-terminal transcript for install/enable of explicitly supplied Web and TUI paths, `up`, status/list flows, detach/reconnect, and down.
   - Keep explicit subcommand examples for automation and describe `start`/console/`up` ownership precisely.

## Assumptions and unknowns

- Confirmed: interactive no-argument startup intentionally supersedes the old no-arg summary convention; non-TTY rejection is side-effect free.
- Assumption: both stdin and stdout must be terminals before entering the console. A piped input or redirected output is noninteractive and must fail rather than partially entering prompt mode.
- Assumption: the console is bound to one resolved data directory for its lifetime; nested prompt commands cannot override it.
- Assumption: `shell-words` is preferable to a new hand-written lexer because existing command operands can contain spaces/quotes and the dependency is already in the graph.
- Assumption: prerequisite display names the required first-party package identities but never searches for their repositories or supplies actual paths.
- Confirmed: Ctrl-C cancels/redraws and preserves attachment/daemon; exit/EOF detach; successful `down` stops the daemon and exits.
- Confirmed: in-flight Ctrl-C is acknowledged but does not cancel synchronous inline work; cleanup completes before prompt restoration. Foreground children receive normal Ctrl-C.
- Confirmed: command support follows terminal ownership. `apps open` uses reusable foreground handoff; `start` and `mcp-serve` remain explicit external-only commands.
- Implementation must verify whether a persistent buffered stdin lock would prefetch bytes before `apps open` hands the terminal to a foreground app. Structure prompt reads so a child receiving inherited stdio does not lose input to the parent buffer.
- Implementation must verify daemon process-group isolation on supported Unix targets and preserve the current owned-process metadata/down cleanup behavior.
- Implementation must preserve the existing hub-test-support foreground terminal launch conformance while adding console-specific handoff/error restoration proof.

## Risks

- Policy duplication: a console-specific command implementation could drift from explicit CLI behavior. Require one dispatcher/parser/request/rendering path.
- First-run deadlock: reusing `up` wholesale would fail before packages can be installed. Start/reuse only the daemon at console entry.
- Signal fanout: the currently spawned daemon shares the caller's process group; Ctrl-C from the console terminal could shut it down through its SIGINT forwarder. Isolate the daemon and test actual PTY-generated Ctrl-C.
- Input ownership: a buffered parent stdin reader can steal bytes intended for `apps open` foreground terminal apps; avoid read-ahead across handoff and restore the prompt after child exit.
- Nested process exit: `open_terminal_app` currently calls `process::exit` for nonzero/signal child outcomes. Move exit translation to the outer explicit-CLI boundary so a failed foreground child cannot kill the console.
- Worker packaging: a plain hub build does not produce `botster-session-worker`. Reuse does not require local worker discovery, but fresh console startup must fail with reachable install/build remediation rather than an unusable flag suggestion.
- Lifecycle race: extracting/reordering readiness probes can falsely reuse an exiting daemon. Preserve `try_wait`/protocol probe ordering and run restart-after-down coverage.
- Hidden side effects: a non-TTY check placed after config/startup could touch the real default directory. Test with an isolated fake HOME/data-dir and assert no directory/socket/state creation.
- Parser drift: whitespace splitting would break quoted paths and arguments. Use a shell-word parser and test quotes, escapes, malformed input, and paths containing spaces.
- Exit semantics: treating EOF, Ctrl-C, `exit`, and `down` alike could accidentally stop the daemon or sessions. Give each event an explicit state transition and PTY assertion.
- Interrupt cleanup: default SIGINT during startup or inline work would skip destructors and could leave daemon metadata/processes behind. Install handling before spawn, defer cancellation through cleanup, and temporarily restore normal child signal behavior only inside foreground handoff.
- Test leakage/flakiness: real daemon and PTY tests can leave processes or sockets, poison the shared daemon lock, or exceed Unix socket path limits. Use short unique temp roots, RAII/guard cleanup, exact process ownership, and existing serialization.
- Scope creep: prompt history/completion/configuration or daemon protocol changes are not required for first-run acceptance.

## Acceptance checks and tests

- Focused unit/parser tests through `./test.sh`:
  - interactive/no-subcommand routing is selected only when both relevant streams are terminals;
  - bare non-TTY exits nonzero with explicit-subcommand guidance and creates no data directory, state file, socket, metadata, or process;
  - shell-word parsing preserves quoted/escaped package paths, rejects malformed quotes clearly, ignores empty input, and does not accept a repeated `botster-hub` prefix;
  - console commands receive the pinned resolved data directory and reuse the existing command parsers;
  - a prompt-level `--data-dir` option is rejected before canonicalization, no second root is created, and a literal token after `--` remains an operand;
  - every row in the exhaustive interaction-mode table is covered by a unit classification assertion, including aliases and the reject-by-default behavior;
  - `open web`/Web `apps open` returns inline; `open tui`/terminal `apps open` hands off the foreground terminal and restores the prompt;
  - `sessions attach`, `start`, `mcp-serve`, `inspect`, and `run-one` are rejected with exact explicit invocations;
  - `shutdown` and `down` both stop the daemon and exit the console.
  - worker resolution reuses a live daemon without requiring a local worker; a fresh start with a missing sibling worker emits reachable install/source-build remediation, starts no daemon, and writes no runtime metadata.
- Deterministic transcript/PTY integration test in `tests/hub_daemon_lifecycle_test.rs`:
  - call the existing `ensure_session_worker_binary()` harness setup, then spawn the actual `CARGO_BIN_EXE_botster-hub` under a PTY so production sibling-worker resolution is exercised without a nonexistent bare-invocation flag;
  - observe resolved data dir, `daemon=started`, missing/disabled prerequisite rows, concise help, and stable prompt;
  - issue quoted `packages install --path` and `packages enable` commands for isolated Web/TUI fixtures, then `packages list/show`;
  - run `up`, `status`, `apps list`, and `sessions list` and assert their existing output models appear through the prompt path;
  - issue an invalid/unknown/malformed command, assert a clear error, then prove the prompt and daemon remain usable;
  - launch a fixture terminal app that exits nonzero and another terminated by a signal; assert both return control to the prompt, report failure, preserve the daemon, and preserve the same explicit-CLI exit codes outside the console;
  - type a partial line, issue Ctrl-C, observe a clean fresh prompt, and prove the daemon plus a daemon-owned sentinel session remain alive;
  - issue Ctrl-C while a deterministic delayed startup/`up` fixture is in flight, observe immediate `interrupt requested; finishing safely`, allow the operation/cleanup to finish, then assert the prompt returns with no orphaned daemon, no stale runtime metadata, and existing sessions intact;
  - issue Ctrl-C during foreground terminal-app handoff, prove the child receives it under normal signal behavior, and prove the console restores its own Ctrl-C behavior afterward;
  - `exit` leaves the daemon answering `Status`; reconnecting with a second PTY reports `daemon=reused`;
  - EOF leaves the daemon running under the same proof;
  - successful `down` prints confirmation, stops the daemon, and exits the console to the parent shell;
  - every cleanup path reaps only test-owned processes and removes only the isolated test root.
- Existing lifecycle regression:
  - `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_up_starts_reuses_and_down_stops_runtime -- --exact`
  - `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_down_recovers_owned_incompatible_daemon -- --exact`
  - `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_recovery_removes_only_selected_data_dir_socket -- --exact`
  - `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_up_refuses_unowned_incompatible_daemon -- --exact`
  - restart-after-`down` is unconditionally proven inside `cli_local_runtime_up_starts_reuses_and_down_stops_runtime`.
- Existing explicit command/runtime regression:
  - `./test.sh --test hub_daemon_lifecycle_test external_hub_test_support_drives_isolated_daemon_socket_protocol -- --exact`
  - `./test.sh --test hub_daemon_lifecycle_test foreground_terminal_app_open_absolutizes_relative_runtime_paths -- --exact`
  - `./test.sh --test hub_daemon_lifecycle_test`
  - `./test.sh --test hub_local_runtime_test`
  - `./test.sh` for the full repository-owned suite.
- Strict repository Rust gates before Review/Verify:
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  - `git diff --check`
- Downstream production-path proof required by [[botster-hub-playbook]]:
  - build the exact hub and session-worker binaries;
  - drive the real no-argument PTY entrypoint through start, package bootstrap, `up`, detach, reuse, Ctrl-C preservation, and `down`;
  - preserve the existing `script/test-production-package-runtime` acceptance when its required sibling repository revisions are available. This is downstream verification only; it does not authorize changing sibling repositories.

## Pipeline gates and artifacts

- Plan artifact: this file.
- Plan checklist: `checklist_1785175305_811425`, recording notes loaded, the superseded convention/human answer, verification commands, and required vault replacement.
- Implement evidence must include changed files, commit/PR identity, exact command-dispatch reuse, proof that non-TTY rejects before side effects, real PTY transcript evidence, daemon/session survival after Ctrl-C, exit/EOF reuse, down shutdown, and README changes.
- Review must load [[botster-runtime-reviewer-playbook]], reject a second parser/policy path or unwired console module, inspect signal/process-group and stdin handoff behavior, and require exact lifecycle/strict-gate evidence rather than code existence.
- Verify must rerun focused PTY/lifecycle proof and the repository strict/full gates against the live worktree, then exercise downstream production acceptance where available.

## Vault gaps worth capturing

- Required replacement after implementation: retire [[botster hub no arg summary must not touch durable home state]] and capture one TTY-sensitive rule stating that interactive bare invocation is the primary durable console entrypoint while non-TTY bare invocation rejects before side effects.
- Capture the process-group/signal rule if implementation proves a durable Botster convention for consoles that spawn persistent daemons.
- Capture a console stdin-handoff rule if `apps open` exposes a reusable constraint around buffered prompt readers and foreground terminal children.
- Capture nothing else unless implementation reveals a repeated architectural rule; command-specific implementation details belong in repository docs/tests, not the vault.
