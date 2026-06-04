# Prove End-to-End Persistent Local Botster Runtime

Ticket: `ticket_1780532740_747815`

## Context Loaded

- Project Pipelines context loaded with `project_pipelines_current_context` for run `run_1780613692_190630`, current step `botster_plan`, run step `run_step_1780613692_136688`, gate `botster_plan_gate`, and ticket `Prove end-to-end persistent local Botster runtime`.
- Pipeline dependencies are closed:
  - `Prove hub restart reconnects without losing sessions`
  - `Add daemon-backed CLI attach and streaming workflow`
  - `Harden daemon and worker hot paths for many PTYs`
- No prior artifacts, findings, reviews, open questions, or question answers were present in this run context.
- Required playbooks loaded: [[planner-playbook]] and [[botster-planner-playbook]].
- Required Botster overlays loaded: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], and [[plan agents must author vault context as wikilinks not home paths]].
- Targeted artifact and verification notes loaded: [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[pipeline artifacts should cite vault notes by wikilink not home path]], [[botster review and verify must scan all committed artifacts for pii]], and [[test script required for rust tests not cargo test]].
- Identity/goals context loaded as [[identity]] and [[goals]].
- Repo context inspected: `README.md`, `Cargo.toml`, `test.sh`, `src/main.rs`, `src/runtime.rs`, `src/daemon.rs`, `src/client_api.rs`, `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_client_api_test.rs`, `tests/hub_local_dogfood_test.rs`, and prior `docs/plans/*`.
- Current repo state observed:
  - `HubRuntime` already wraps `botster_core_daemon::CoreDaemon` and configures a sibling `botster-session-worker` path.
  - `HubDaemon::start` already loads durable hub state, restores package policy, initializes `HubRuntime`, and runs session reconciliation.
  - `tests/hub_daemon_lifecycle_test.rs` already proves hub restart reconnect for a worker-backed session and cross-process daemon CLI session commands.
  - `tests/hub_local_dogfood_test.rs` already proves package lifecycle, hub-state reload, plugin lifecycle observation/invocation, session spawn/attach/input/drain/shutdown in one in-process dogfood flow.
  - `README.md` already documents daemon-backed CLI commands, restart behavior, dogfood-ready scope, and feature-parity gaps.
- Project Pipelines checklist discipline attempted: both `project_pipelines_create_vault_checklist` and `project_pipelines_create_checklist` timed out in the plugin worker. Per [[project pipelines checklist worker timeouts require artifact evidence fallback]], this plan, the artifact, and gate evidence carry checklist provenance instead of blocking Plan.

## Scope

In scope:

- Produce the final local-runtime proof by composing the already-merged daemon-backed pieces into one explicit automated and documented end-to-end path.
- Add or update a focused proof test that demonstrates the ticket acceptance in one flow:
  - fresh explicit data directory,
  - local daemon start/status,
  - package/provider lifecycle still works,
  - session spawn/list/attach/input/resize/detach/shutdown through the daemon-backed user path,
  - hub restart preserves a live worker-backed session,
  - core daemon restart/adoption behavior is exercised where the current core contract supports it,
  - output evidence comes from real daemon/session-worker protocol paths, not fabricated registry defaults.
- Add a concise readiness note in repo docs, either as a new `docs/adr`/`docs/plans` handoff or a README section, stating whether this stack is ready for local dogfood and the exact remaining gaps before replacing the monolith locally.
- Tighten README commands only where needed so a fresh checkout can follow documented commands and observe a durable worker-backed session across hub restart.
- Keep all proof output, docs, fixtures, and test assertions path-neutral and free of PII.

Non-scope:

- No WebRTC, cloud, Rails, TUI, browser SPA, ActionCable, marketplace UI, OAuth/device-code flow, provider process supervision, or public hosted-preview behavior.
- No new runtime architecture. The plan should reuse `HubDaemon`, `HubRuntime`, `CoreDaemon`, `HubClientApi`, daemon transport, package registry, and existing test support.
- No speculative configurability, broad CLI parser rewrite, supervisor installation, service manager integration, or new dependency-heavy RPC layer.
- No permanent parallel in-process session runtime path for proof purposes. Existing in-process tests may remain as lower-level contract coverage, but final proof must identify the daemon-backed production entry point.
- No fake adoption evidence, optimistic default classifiers, or registry-only proof for live recovery.

Botster layers touched:

- Rust hub daemon lifecycle and daemon transport.
- Rust hub runtime facade over `botster-core-daemon`.
- Thin operator CLI commands in `src/main.rs`.
- Package/provider lifecycle through hub policy and local package fixture.
- Rust integration tests and docs/readiness note.

No Project Pipelines plugin, SPA, TUI, Rails relay, or cloud provider surface should change for this ticket.

## Assumptions And Unknowns

- Assumption: The assigned pipeline worktree is the intended implementation checkout for target `tgt_7e208a0c76a44980a83b63af976b1f22`; no extra agent spawn is needed for this Plan step.
- Assumption: The closed dependency tickets are part of `main` for this run, so implementation should build on the existing daemon-backed CLI and hub-restart tests instead of replacing them.
- Assumption: "Persistent local Botster runtime" means durable local dogfood readiness for the production-shaped local stack: hub as host profile/control plane, core daemon as multiplexer/supervisor/router, and session workers as durable PTY owners.
- Assumption: "Core daemon restart adopts live workers where the core contract supports it" should be proven only to the level exposed by current `botster-core-daemon` APIs. If core cannot preserve PTYs across a true core daemon process exit, the readiness note must say so explicitly rather than imply broader durability.
- Unknown: Whether the existing tests can be composed without adding a new high-level integration test. Prefer one focused final proof test if separate existing tests leave acceptance scattered across too many files.
- Unknown: Whether a README-only readiness note is enough. If the implementation produces nuanced dogfood readiness/gap analysis, prefer a small `docs/adr` or `docs/plans` readiness document linked from README.
- Unknown: Whether `packages enable` should be driven against the long-running daemon in this ticket. Current package commands start a short-lived `HubDaemon` over durable state; that may satisfy package persistence, but final proof should document the distinction if session runtime commands use the long-running daemon transport.

No human question blocks planning. If implementation discovers that satisfying any acceptance bullet requires waiving part of the ticket, it should ask a human instead of silently narrowing scope.

## Affected Surfaces / Files

Expected changes:

- `tests/hub_daemon_lifecycle_test.rs`
  - Add or extend one end-to-end proof over the real daemon-backed CLI path.
  - Reuse the existing serialized real-daemon lock, explicit data dirs, and `ensure_session_worker_binary` support.
  - Assert hub restart preserves a session and post-restart attach/input/drain still works through `HubClientApi` or daemon transport.
  - Include any current-contract core restart/adoption evidence, or assert/document the exact unsupported gap.
- `tests/hub_local_dogfood_test.rs`
  - Keep package/plugin dogfood proof green.
  - Extend only if the final proof should include package lifecycle in the same test rather than relying on a daemon lifecycle test plus this dogfood test.
- `README.md`
  - Ensure the documented fresh-checkout commands exactly match what was verified.
  - Add or link the readiness note and remaining local dogfood gaps.
  - Make hub restart vs core daemon restart semantics concrete.
- Optional new readiness document under `docs/adr/` or `docs/plans/`
  - Summarize dogfood readiness and remaining gaps before replacing the monolith.
  - Keep it concise and path-neutral.

Likely unchanged unless a test exposes a real gap:

- `src/runtime.rs`: already routes through `CoreDaemon` with explicit worker path and reconciliation.
- `src/daemon.rs`: already owns startup/state restoration and status.
- `src/client_api.rs`: already routes status/list/spawn/attach/input/resize/detach/drain/shutdown through `HubRuntime`.
- `src/main.rs`: already exposes daemon-backed `start`, `status`, `sessions *`, `shutdown`, package commands, and inspect.
- `Cargo.toml` / `Cargo.lock`: avoid dependency churn unless current `botster-core` APIs are insufficient.

## Risks

- Fragmented proof risk: existing tests individually prove many pieces, but acceptance asks for an end-to-end persistent runtime proof. The implementation should either compose one final proof test or document the exact test matrix as the proof contract.
- Overclaiming risk: hub restart, core daemon restart, and daemon process exit are different failure modes. Docs and readiness notes must use precise wording for what survives today.
- Underwired proof risk: tests that call `HubRuntime` directly without touching the documented user path are useful but not enough. At least one proof must name and exercise the production entry point a local dogfood user follows.
- Package/runtime split risk: package commands currently persist state through a short-lived hub object, while session commands use a long-running daemon. That may be acceptable, but the readiness note must not blur the distinction.
- False adoption evidence risk: recovery must use real `CoreDaemon` adoption scan/protocol evidence and worker-backed sessions, not registry-shaped fixtures alone.
- Test flake/leak risk: long-running PTY/daemon tests must serialize real daemon processes, use unique data dirs, and always request shutdown or kill child processes on failure.
- PII risk: plan/docs/output must not include local absolute paths, usernames, tokens, environment dumps, or host-specific identifiers.
- Scope creep risk: replacing the monolith locally can tempt browser/TUI/cloud work. This ticket is explicitly local-runtime proof only.

## Acceptance Checks / Tests

Required verification:

- `./test.sh --test hub_daemon_lifecycle_test daemon_restart_reconnects_worker_backed_session_through_client_api`
- A final proof command, preferably one of:
  - `./test.sh --test hub_daemon_lifecycle_test <new_final_proof_test_name>`
  - or `./test.sh --test hub_local_dogfood_test local_dogfood_runs_daemon_package_lifecycle_session_and_clean_shutdown` plus a documented daemon-restart test matrix if one test would duplicate too much harness code.
- `./test.sh --test hub_client_api_test`
- Full `./test.sh` before Review unless runtime cost is prohibitive; if scoped, implementation must state why and list skipped surfaces.
- `cargo clippy --all-targets --all-features -- -D warnings` for changed Rust code. If clippy fails, capture raw diagnostics and attribute them to touched files versus pre-existing baseline.
- README command smoke exactly as documented, or an explicit explanation that the automated integration test is the authoritative fresh-checkout proof.
- PII scan over committed diff and generated proof snippets: no local absolute home paths, usernames, emails, tokens, or secret-bearing metadata.

Acceptance assertions:

- Fresh explicit data directory initializes durable hub state.
- `start --data-dir` launches the local daemon and `status --data-dir` proves a handshaking daemon path with typed scrubbed status.
- Package/provider lifecycle remains available: install/enable/list from the synthetic fixture persists across hub-state reload and reports sanitized package/provider state.
- Session lifecycle works through the daemon-backed local path: spawn, list, attach, send input, resize, detach, drain observed output, and shutdown.
- Hub restart preserves a live worker-backed session: stop only the hub lifecycle, restart over the same data root, list the same session, reattach, send input, observe post-restart output, and shut down.
- Core restart/adoption is tested to the current contract boundary. If true core-daemon-process restart cannot preserve PTYs today, the test/readiness note must say exactly what is supported and what remains.
- Readiness note states whether the new stack is ready for local dogfood and lists remaining gaps before broader feature parity.

## Pipeline Gates And Artifacts

- This plan document is the repo-visible Plan artifact required by [[plan steps need reviewable plan artifacts]].
- Gate evidence should reference this document and include the checklist timeout fallback, because both checklist creation tools timed out for this run.
- Plan Review should verify that the plan:
  - does not reimplement closed dependency work,
  - requires runtime/user-path proof rather than code-existence proof,
  - distinguishes hub restart from core daemon restart and daemon process exit,
  - keeps local runtime proof separate from WebRTC/cloud/Rails/TUI/browser scope.
- Implement gate should report:
  - changed files and commit hash,
  - exact production entry point used by the proof,
  - exact tests/commands run,
  - readiness note location,
  - any current-contract gap that remains before monolith replacement.
- Verify should rerun the final proof and inspect the readiness note rather than trusting implementation claims.

## Convention Conflicts

None found. The plan follows the loaded Botster conventions: hub remains a first-party host profile over core, local clients use `HubRuntime`/`HubClientApi`, PTY/session mechanics stay in `botster-core-daemon` and session workers, CLI stays thin and explicit-data-dir based, terminal egress remains session-backed, Project Pipelines evidence is preserved through durable artifacts when checklist persistence times out, and repo artifacts cite vault context by wikilink/note title instead of local paths.

## Vault Gaps Worth Capturing

- Capture the final local-runtime proof contract once implementation settles the exact automated test and README/readiness command matrix.
- Capture the precise distinction between hub restart, core daemon restart, and daemon process exit if implementation discovers that current docs blur those boundaries.
- Capture whether package commands intentionally remain short-lived hub-state operations while live session commands use the long-running daemon transport.
- Capture recurring Project Pipelines checklist worker timeout evidence if this run's failed checklist creation is part of a continuing operational pattern.
