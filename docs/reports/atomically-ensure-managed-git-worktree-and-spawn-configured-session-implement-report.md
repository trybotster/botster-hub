# Atomic managed Git worktree and configured-session implementation report

## Routing and constraints

- Target repository: `trybotster/botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1785192690_547868`
- Run: `run_1785199797_806574`
- Repository charter: [[botster-hub-playbook]]
- In-repository contract charter: [[botster-hub-client-playbook]]
- Role playbooks: [[implementer-playbook]] and
  [[botster-implementer-playbook]]
- Applied architecture and verification notes include
  [[botster hub is a first party host profile over core]],
  [[botster hub gravity must be watched before it becomes the new monolith]],
  [[botster hub events use bounded priority lanes instead of unbounded queue fuses]],
  [[hub supervision admission changes require exact live hub launch proof]],
  [[queued hub orchestration uses a shared queue policy]],
  [[device hub owns admitted spawn targets not ambient repo cwd]],
  [[workspace session templates are hub owned capabilities callable from lua workers]],
  [[session template override sources use package device repo explicit precedence]],
  [[plugin capability tests must validate against real lua runtime table not injected stubs]],
  [[an mpsc round trip is not a durability barrier]],
  [[test script required for rust tests not cargo test]],
  [[rust repo strict lints must be verified before dismissing warnings]],
  [[workspace struct field changes require workspace cargo gates]],
  [[botster hub client crate is the external client boundary]],
  [[daemon event shape changes bump conformance fixture revision not protocol version]],
  and [[generated typescript dtos must encode serde field optionality]].
- `[[project-pipelines-playbook]]` was not applied because this change does not
  alter Project Pipelines package paths or workflow policy.

The implementation assumes that each successful atomic call creates a distinct
session, while the Git ensure portion reuses an exact matching managed
worktree. It also assumes local branches only: no fetch, remote discovery,
reset, clean, pull, merge, or rebase is part of the operation.

## Implementation and production path

The Hub now persists and validates Git-capable spawn targets with an
authoritative `base_ref`, owns deterministic managed-worktree creation and
reconciliation, and exposes one scoped Lua operation that combines ensure,
trusted session-template materialization, and Core-backed session spawn.

The production entry path is:

1. A packaged Lua plugin calls
   `session_templates.ensure_worktree_and_spawn`.
2. `src/lua_runtime.rs` accepts semantic inputs only and routes the request
   through the existing plugin-worker bridge.
3. `src/runtime.rs` checks the exact capability, target, and template, then
   submits Git preparation to the bounded off-owner lane.
4. `src/managed_git_worktrees.rs` verifies repository identity, stored base
   policy, branch ownership, and deterministic path before creating or reusing
   the worktree.
5. The Hub owner persists the managed row, uses the private trusted
   materializer in `src/session_templates.rs`, and spawns the canonical
   full-UUID session through the existing `CoreDaemon`.
6. The same lane commits the operation or rolls back only resources created by
   that call. Rollback preserves changed or identity-mismatched content and
   returns reconciliation evidence instead of deleting it.

Git work has a 20-second ceiling inside a 25-second operation budget. Timed-out
children are killed and reaped. The lane permits one active and one waiting
request, returns typed backpressure beyond that, and releases independently of
owner-loop polling.

Repo-source template commands resolve from the selected branch's managed
worktree. Package/device-source commands retain their trusted source root.
Package-root and relative working-directory policies map beneath the managed
worktree with canonical containment checks. Ordinary template spawning retains
its existing caller-cwd admission.

## Files changed

- Hub implementation: `src/managed_git_worktrees.rs`,
  `src/spawn_targets.rs`, `src/worktrees.rs`, `src/session_templates.rs`,
  `src/runtime.rs`, `src/lua_runtime.rs`, `src/profile.rs`,
  `src/persistence.rs`, `src/daemon_transport.rs`, `src/main.rs`,
  `src/client_api.rs`, `src/config.rs`, and `src/lib.rs`.
- Hub client contract: `crates/botster-hub-client/src/lib.rs`,
  `crates/botster-hub-client/src/typescript.rs`, and
  `crates/botster-hub-client/generated/daemon-protocol.ts`.
- Downstream support mirror: `packages/hub-test-support/daemon-protocol.ts`,
  support metadata and conformance fixtures, `test.mjs`, and its README.
- Verification: `tests/hub_capability_runtime_test.rs`,
  `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs`,
  `tests/hub_lua_runtime_test.rs`, `tests/hub_mcp_test.rs`,
  `tests/hub_plugin_lifecycle_test.rs`, and `tests/hub_runtime_test.rs`.
- Dependency and documentation: `Cargo.lock`, `README.md`,
  `docs/client-protocol.md`, and the approved plan.

## Ownership and cross-repository work

The Hub remains the sole owner of Git execution, target admission, stored base
policy, managed-root naming, locking, reconciliation, rollback, template
eligibility, trusted context, and session orchestration. The in-repository
`botster-hub-client` crate owns only the external DTO/TypeScript contract.
Plugins receive semantic operations and opaque facts; they do not receive Git
or filesystem mutation authority. Core still receives a generic materialized
session request, and no terminal data-plane or renderer behavior changed.

Canonical UUID proof exposed a private Core Unix-socket path-length defect on
macOS. Human answer `question_1785202296_840826` rejected a compact Hub identity
workaround. The fix was separately routed as Core ticket
`ticket_1785211693_238645`, is closed, and is consumed from Core `main` at
`e36435f2cb583c344d6f6ba2d62c39da324c7a64`. `git ls-remote` confirmed that
exact revision at `refs/heads/main`. No cross-repository source was edited in
this run.

## Deviations from the approved plan

The approved plan was amended for the separately routed Core dependency above.
Hub consumes the Core API rename from `per_plugin_capacity` to
`per_plugin_queue_capacity`, and test data directories became process-unique
because the corrected Core runtime rejects stale insecure socket directories.
There is no behavioral waiver or Hub-side UUID compaction. Otherwise the
implementation follows the approved scope.

## Verification and downstream-shaped proof

All commands exited successfully:

- `./test.sh managed_git_worktrees::tests`: 10 passed, including all three
  resolution cases, dirty reuse, collisions, restart adoption, controlled
  missing/hung Git, exact rollback, and preservation after content changes.
- `./test.sh spawn_targets::tests`: 2 passed, including registration-time
  defaulting and atomic set/clear/invalid/empty `base_ref` updates.
- `./test.sh -p botster-hub-client`: 41 tests and 4 doc tests passed.
- `npm run sync` and `npm run check` in `packages/hub-test-support`: generated
  TypeScript and support fixtures match revision 20.
- `npm test` in `packages/hub-test-support`: passed.
- `cargo check --workspace --offline`: passed against the locked Core
  coordinate.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`:
  passed.
- `git diff --check`: passed.
- `./test.sh`: passed in full. The suite reported 124 library tests, 12 binary
  tests, 14 capability tests, 23 client API tests, 101 daemon lifecycle tests
  with one documented adversarial test ignored, 1 local runtime test, 20 Lua
  runtime tests, 6 MCP tests, 6 plugin lifecycle tests, 7 runtime tests, 2
  conformance tests, and 1 doc test.

The downstream-shaped proof uses the exact Cargo-built `botster-hub` and
`botster-session-worker` binaries. A real installed/enabled fixture package
declares the new scope and invokes the atomic method through the daemon MCP
path. The test observes a canonical 36-character session UUID, branch-sourced
command execution, managed relative cwd, trusted context, target-filtered
templates, capability denial, symlink escape denial, and spawn-failure
rollback. It then starts an independent competing Hub against the same
repository to prove typed branch ownership conflict, restarts the first Hub
with the same data directory, observes the managed row, blocks target
downgrade, and reuses the exact path while creating a distinct UUID session.

## Residual risk and vault disposition

The subprocess seam proves kill/reap and typed timeout without waiting 20
seconds in every Lua integration run; the exact full-budget timeout is not
repeated end-to-end through Lua. Response-loss cleanup and deferred owner
reconciliation use the same tested state machine, but no new test deliberately
tears down a plugin invocation during Git completion. Restart reconciliation
and worker-owned lane release limit those risks, and Review should inspect
those paths directly.

No conflicting or missing vault guidance was discovered. Four durable rules
were confirmed and are captured here pending post-merge vault promotion:
registration-time `base_ref` defaulting versus spawn-time stored authority;
deterministic identity plus exact restart adoption; removal of only
call-created clean resources with reconciliation on doubt; and target-filtered
Lua reads plus one scoped atomic mutation operation.
