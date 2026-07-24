# Add daemon-backed CLI attach and streaming workflow

## Context loaded

- Project Pipelines context: ticket `ticket_1780532740_614451`, run `run_1780607156_958519`, current step `botster_plan`, gate `botster_plan_gate`; no prior artifacts, findings, questions, answers, or reviews; dependency `Connect hub daemon to core daemon session runtime` is closed.
- Required playbooks: [[planner-playbook]] and [[botster-planner-playbook]].
- Required Botster context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], and [[botster orchestration prompts must bind agents to explicit worktrees]].
- Targeted daemon/client context: [[botster hub daemon startup requires explicit data dir]], [[botster hub daemon status should be typed and path neutral]], [[botster local client api lives over hubruntime not raw core routers]], and [[botster terminal clients share one sessionio data plane subscription path]].
- Targeted socket/test context loaded after Plan Review: [[botster hub socket liveness requires a protocol handshake]], [[botster hub socket cleanup must preserve connectable sockets and repair missing socket paths]], [[botster hub smoke cli entrypoints stay thin explicit and facade backed]], and [[pty integration tests that spawn botster start must be serialized to avoid socket-path races]].
- Repo context: `src/main.rs` currently starts and stops a fresh in-process `HubDaemon` for `start`, `status`, and `sessions`; `README.md` explicitly documents that live sessions do not survive separate CLI invocations; `HubClientApi` and `HubRuntime` already expose typed status/list/spawn/attach/detach/input/resize/drain/shutdown verbs; existing tests prove in-process behavior but not cross-process CLI continuity.
- Plan Review context: review `review_1780607683_877616` approved architecture and scope but required targeted additions for single-runtime ownership, daemon-authoritative logical clock, socket liveness/cleanup/test notes, and dropped-attach cleanup acceptance.

## Scope

- Turn the current bounded `start --data-dir` smoke command into a real local daemon runtime that stays alive until explicit shutdown or process termination.
- Add a small same-device control transport, preferably Unix domain socket on Unix, using the existing resolved `HubConfig.transports.local_socket` path under the explicit data directory.
- Route separate CLI invocations through that daemon instead of creating a fresh `HubDaemon` for every live-session command.
- Preserve the existing `HubClientApi` request/response/event contract as the command boundary; add serialization/framing only as an adapter around it.
- Own runtime mutation in the daemon, not in client threads: concurrent socket connections must submit work to one daemon-owned runtime loop so a long-lived attach stream never holds `&mut HubRuntime` while writing to a slow stdout consumer.
- Make the daemon authoritative for logical time. Stateless CLI clients must not supply `now_seconds` or `last_output_at` values that the daemon trusts; the daemon stamps/advances monotonic logical time and stores per-session/per-subscription drain cursors.
- Support operator commands for:
  - `start --data-dir <path>`: start long-running daemon and publish enough socket/status metadata for later commands.
  - `status --data-dir <path>`: connect to the running daemon and report typed scrubbed status.
  - `sessions spawn --data-dir <path> [--session-id <id>] -- <command>`: create a live session in the daemon.
  - `sessions list --data-dir <path>`: list live daemon sessions across a separate invocation.
  - `sessions attach --data-dir <path> <session-id> [--subscription-id <id>]`: attach to a session and stream output.
  - `sessions send-input --data-dir <path> <session-id> -- <bytes>`: send terminal input to the daemon-owned session.
  - `sessions resize --data-dir <path> <session-id> <rows> <cols>`: resize through `HubClientApi::Resize`.
  - `sessions detach --data-dir <path> <session-id> [--subscription-id <id>]`: detach without shutting down the session.
  - `shutdown --data-dir <path>`: ask the daemon to stop cleanly.
- Make attach streaming observable for a command that prints `production runtime-ok`, through the attach/stream path rather than `run-one`.
- Update docs to remove the old limitation that live sessions are unavailable across separate CLI invocations, and replace it with current daemon-backed behavior and limitations.

## Non-scope

- No TUI, browser, Rails, ActionCable, WebRTC, cloud provider, marketplace UX, OAuth/device-code flow, or provider process supervision.
- No new runtime engine or duplicate core router. The daemon remains a host-profile lifecycle over `HubRuntime` and `HubClientApi`.
- No durable PTY/session recovery after daemon process exit unless `botster-core` already provides it through the configured runtime path. This ticket is about separate CLI invocations while the daemon remains alive.
- No database, new dependency-heavy RPC framework, background supervisor, launchd/systemd integration, PID-file manager beyond minimal liveness metadata if needed, or broad CLI parser replacement.
- No local paths, usernames, keys, fingerprints, environment dumps, tokens, or PII in status output, fixtures, logs, or tests.
- No client-owned logical clocks for daemon-backed requests. CLI arguments remain user intent only; the daemon assigns runtime timestamps.

## Botster layers touched

- Rust hub daemon lifecycle: long-running daemon loop, stop/shutdown ownership, status.
- Rust CLI/operator surface: command parsing and command dispatch in `src/main.rs`.
- Local client transport adapter: request/response/event framing around `HubClientApi`.
- Session/client runtime path: existing `HubRuntime` and `HubClientApi` methods for session verbs, plus any tiny missing request variant needed for daemon status/shutdown.
- Docs and integration tests.

No Project Pipelines plugin, SPA, TUI, Rails relay, or MCP surface should change for the feature itself.

## Affected surfaces/files

- `src/daemon.rs`
  - Keep startup/status ownership here.
  - Add long-running serve/shutdown behavior or delegate to a new narrow transport module while preserving `HubDaemon` as lifecycle owner.
  - Own the single `HubRuntime` on one daemon control thread or equivalent serialized owner. Per-connection workers may parse sockets and write client output, but runtime calls must be discrete messages into that owner.
  - Own logical clock and drain cursors so separate CLI processes cannot regress `now_seconds` or `last_output_at`.
  - Status must remain typed and path-neutral.
- `src/main.rs`
  - Change `start` from bounded smoke start/stop to the daemon entrypoint.
  - Route `status`, `sessions *`, and `shutdown` through the local daemon transport.
  - Add `sessions detach`, `sessions resize`, and streaming attach output behavior.
- `src/client_api.rs`
  - Prefer existing request/response/event types.
  - Add only narrow serializable request fields or variants if the transport needs daemon lifecycle status/shutdown distinct from session shutdown.
  - Do not expose client-supplied runtime clock fields on the daemon transport even though `HubClientRequest` currently carries them internally.
- `src/config.rs`
  - Use existing `TransportBindings.local_socket` resolution if possible.
  - Avoid hardcoded socket paths outside the explicit data directory.
- `src/lib.rs`
  - Export any new narrow daemon transport/status types needed by tests.
- `tests/hub_daemon_lifecycle_test.rs`
  - Replace current assertions that later `sessions list` sees `session_count=0`.
  - Add cross-process CLI tests for start/status/spawn/list/attach/send-input/resize/detach/shutdown.
- `tests/hub_client_api_test.rs`
  - Keep as in-process contract coverage; add only if transport changes require a new client API variant.
- `tests/hub_local_runtime_test.rs`
  - Preserve existing production-shaped in-process production runtime proof unless the new daemon transport becomes the better production runtime proof.
- `README.md`
  - Document daemon-backed CLI behavior, attach/detach workflow, slow-consumer limitation/backpressure behavior, and current non-goals.

## Implementation outline

1. Define the daemon transport contract.
   - Use length-delimited JSON over Unix socket or one-line JSON frames if that stays simple and testable with standard library primitives.
   - Keep frames as hub-local operator protocol types wrapping `HubClientRequest`, `HubClientResponseBody`, and `HubClientEvent`, but with daemon-owned clock/cursor fields rather than trusting client-supplied `now_seconds` or `last_output_at`.
   - Include a control request for daemon status and daemon shutdown if `HubClientRequest::Status` is insufficient for lifecycle state.
   - Start every connection with a `hello` / `hello_ack` handshake. `status`, attach resolution, stale-socket cleanup, and "daemon not running" decisions must use protocol identity, not pathname existence or `connect()` alone.
2. Make `start --data-dir` long-running.
   - Build explicit config.
   - Start `HubDaemon`.
   - Bind the resolved local socket path.
   - Accept commands until shutdown.
   - Clean up the socket path on normal stop, while avoiding destructive cleanup of unrelated live sockets.
   - Preserve connectable sockets that answer the Botster handshake and self-heal the advertised socket path if the live daemon observes that the filesystem entry has disappeared.
3. Add client-side command dispatch.
   - `status`, `sessions list`, `sessions spawn`, `sessions send-input`, `sessions resize`, `sessions detach`, and `shutdown` should connect to the running daemon and send one request.
   - Report a typed "daemon not running" error if no `hello_ack` handshake succeeds.
   - Keep parsing dependency-free and facade-backed; CLI commands are thin adapters over the daemon transport and `HubClientApi`, not a second implementation core.
4. Implement attach streaming.
   - `sessions attach` should attach a subscription, then continue draining daemon events for that session/subscription and write terminal bytes to stdout.
   - Detach should be triggered by explicit `sessions detach` and by daemon-side socket EOF/client disconnect cleanup. Ctrl-C can be implemented through process exit/EOF behavior unless signal handling is added narrowly.
   - Slow stdout consumers must not block unrelated sessions: use a single runtime owner plus per-connection bounded queues. Attach streaming is a sequence of discrete `drain_runtime_once` calls multiplexed with other daemon requests; no connection may hold the runtime while performing a blocking socket/stdout write. Report/drop/lag through existing `HubRuntime::report_delivery_lag`, `report_delivery_failure`, or equivalent typed observations.
   - Before choosing per-connection threads, add a compile-time or integration proof that the selected runtime owner model is valid for `HubRuntime` and `botster_core::DefaultBotsterEngine` thread-safety. If they are not `Send`, keep the runtime and socket accept/control loop on one thread and use non-runtime worker threads only for blocking output.
5. Preserve existing production entry points.
   - The user path is `botster-hub start --data-dir ...` in one process and subsequent `botster-hub status/sessions/... --data-dir ...` invocations connected to that daemon.
   - Tests must prove those separate processes use the daemon transport, not an in-process `HubDaemon` fallback.

## Assumptions and unknowns

- Assumption: Unix-only tests are acceptable because the existing PTY tests are already `#![cfg(unix)]`; the production transport can start with Unix domain sockets.
- Assumption: no additional crate is needed for the daemon protocol; `serde`/`serde_json` and standard library sockets are enough for this ticket.
- Assumption: `botster-core`'s current `DefaultBotsterEngine` keeps live sessions available while the daemon process stays alive; durable recovery after daemon exit is non-scope.
- Assumption to verify during implementation: the selected daemon worker model is compatible with `HubRuntime` and `DefaultBotsterEngine` thread-safety. If `Send` is unavailable, use a single-threaded runtime owner and keep blocking writes outside the runtime owner.
- Unknown resolved by plan constraint: logical time is daemon-owned. CLI processes are stateless and must not provide authoritative `now_seconds` or `last_output_at`.
- Unknown resolved by plan constraint: runtime mutation is serialized through one owner. Concurrent socket connections communicate with that owner through bounded request/egress channels or an equivalent serialized model.
- Unknown: whether `botster-core` terminal egress output events already provide enough subscription isolation and pressure observations for slow attach consumers. Implementer must inspect actual event and queue behavior before adding any hub-side queue.
- Unknown: whether the daemon transport should use the existing `HubClientRequest` types directly or a thin `DaemonRequest` wrapper with lifecycle commands. Prefer the wrapper only if needed to avoid polluting the stable client API with transport-only control.
- Unknown: how best to make attach detach on Ctrl-C without signal-handling scope creep. At minimum, explicit `sessions detach` and process-exit cleanup should be tested; signal behavior should be documented if not implemented.

## Risks

- Underwiring risk: adding protocol types without routing `src/main.rs` operator commands through a running daemon would fail the ticket. Tests must spawn one `start` process and drive separate command processes against it.
- Blocking risk: a streaming attach implementation that writes directly from the daemon accept loop can block unrelated sessions. Keep connection output bounded and off the main accept/control loop.
- Serialization risk: the daemon owns one synchronous `HubRuntime`; poorly scoped locks or direct `&mut HubRuntime` sharing can make a slow attach block `sessions list`, input, resize, or shutdown.
- Logical-clock risk: stateless CLI invocations can send stale `now_seconds` or `last_output_at` values if the transport mirrors current in-process request fields too literally. The daemon must stamp runtime calls and own drain cursors.
- Thread-safety risk: a threaded std-socket daemon must not assume `HubRuntime`/`DefaultBotsterEngine` are `Send` without compile proof.
- Session leak risk: integration tests that spawn long-lived shell loops must always request shutdown/cleanup, even on assertion failure.
- Socket cleanup risk: stale socket files and live sockets must be distinguished by `hello` / `hello_ack`, not path existence or connectability alone. A live daemon also needs socket path self-healing if the public path disappears.
- Status/PII risk: status, docs, and failure output can leak explicit data dirs or host paths. Keep output typed and scrubbed.
- Test flake risk: real daemon/socket tests can race on deterministic socket paths if run in parallel. Serialize this test subset or guard it with a process-wide lock in addition to unique data dirs.
- Scope creep risk: this ticket can tempt a full supervisor, parser framework, TUI attach, or provider transport. Keep it to daemon-backed local CLI workflow.

## Acceptance checks/tests

- `./test.sh --test hub_daemon_lifecycle_test`
  - Serializes real daemon/socket tests with a process-wide mutex/lock or an equivalent harness guard, per [[pty integration tests that spawn botster start must be serialized to avoid socket-path races]].
  - Starts `botster-hub start --data-dir <tmp>` as a long-running process.
  - `botster-hub status --data-dir <same>` proves socket liveness through `hello` / `hello_ack` and reports running typed scrubbed status.
  - `botster-hub sessions spawn --data-dir <same> --session-id runtime-session -- <command>` creates a daemon-owned session.
  - A separate `sessions list` invocation reports that session as live.
  - `sessions attach` observes `production runtime-ok` from the daemon-backed stream path, not `run-one`.
  - `sessions send-input` across multiple separate invocations produces ordered later markers observed by attach/streaming, proving daemon-authoritative clock/cursor behavior.
  - `sessions resize` returns a typed success/event path.
  - `sessions detach` stops delivery for that subscription while another attached/reattached stream remains valid if tested.
  - Killing an attached CLI process or closing its socket releases that subscription daemon-side; a later reattach/status/list proof must not show leaked delivery for the dropped subscription.
  - A slow or blocked attach consumer does not block `sessions list`, `sessions send-input`, `sessions resize`, or `shutdown` for another CLI invocation.
  - Socket cleanup preserves a connectable/handshaking live socket and reports/repairs missing public socket paths without unlinking another live listener.
  - `shutdown --data-dir <same>` stops the daemon and a later `status` reports not running or fails with a typed not-running error.
- `./test.sh --test hub_client_api_test`
  - Existing in-process API contract still covers status, spawn, attach, input, resize, detach, shutdown, package/lifecycle queries, and admission.
- `./test.sh --test hub_local_runtime_test local_runtime_runs_daemon_package_lifecycle_session_and_clean_shutdown`
  - Existing production runtime proof stays green unless superseded by a stronger daemon-backed production runtime test.
- `cargo run -- start --data-dir target/botster-hub-daemon-runtime-data`
  - Manual run in one terminal.
- In separate terminals:
  - `cargo run -- status --data-dir target/botster-hub-daemon-runtime-data`
  - `cargo run -- sessions spawn --data-dir target/botster-hub-daemon-runtime-data --session-id runtime-session -- "printf 'production runtime-ok\n'; while IFS= read -r line; do printf 'runtime:%s\n' \"$line\"; done"`
  - `cargo run -- sessions list --data-dir target/botster-hub-daemon-runtime-data`
  - `cargo run -- sessions attach --data-dir target/botster-hub-daemon-runtime-data runtime-session`
  - `cargo run -- sessions send-input --data-dir target/botster-hub-daemon-runtime-data runtime-session -- "from-cli\n"`
  - `cargo run -- sessions detach --data-dir target/botster-hub-daemon-runtime-data runtime-session`
  - `cargo run -- shutdown --data-dir target/botster-hub-daemon-runtime-data`

## Pipeline gates and artifacts

- Plan artifact: this document.
- Plan Review return addressed:
  - runtime ownership/serialization model named,
  - daemon-authoritative logical clock required,
  - socket liveness/cleanup/thin-entrypoint/test-serialization notes loaded and cited,
  - dropped attach EOF cleanup and serialized daemon tests added to acceptance.
- Implement gate should include:
  - git diff summary and commit hash,
  - exact production entry point evidence showing CLI commands connect to the long-running daemon,
  - evidence of the runtime ownership model and clock stamping path used by daemon requests,
  - test command outputs for the daemon-backed CLI flow,
  - manual production runtime commands or an explanation if automated tests fully cover the path,
  - docs diff for README limitations.
- Review should reject code-only evidence that does not prove separate CLI invocation continuity.

## Convention conflicts

None found. The plan follows the loaded Botster conventions: CLI stays thin, hub owns lifecycle and local policy, core owns PTY/session mechanics, local clients use `HubRuntime`/`HubClientApi`, terminal egress remains subscription-backed, socket liveness uses protocol handshake evidence, daemon socket cleanup preserves live listeners and self-heals missing paths, real daemon/socket tests are serialized, and status evidence stays typed and path-neutral.

## Vault gaps worth capturing

- Capture after implementation if this settles a durable rule for `botster-hub`: the first daemon-backed local operator transport should be a thin socket adapter over `HubClientApi`, not a second runtime API.
- Capture after implementation if slow-consumer handling exposes a concrete convention for local CLI attach queues and pressure reporting.
- Capture after implementation if daemon socket liveness/cleanup behavior adds a `botster-hub`-specific refinement beyond the existing socket handshake notes.
- Capture after implementation if daemon-owned logical clocks become a standing convention for cross-process `HubClientApi` adapters.
