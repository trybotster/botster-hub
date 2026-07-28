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
The exact Lua integration still needs a short temporary data-directory root
because total Unix socket path length remains bounded; the test now uses the
shared short-unique shape and removes the directory after success. There is no
behavioral waiver or Hub-side UUID compaction.

After Review, the branch was rebased onto current `origin/main`. Published
`@trybotster/hub-test-support@0.1.12` had already assigned revision 20 to the
application-primitives fixture, so this change allocates globally unique
revision 21 and unused package candidate version 0.1.13 rather than reusing
those bytes.

Review also tightened the implementation without changing its ownership
boundary: managed list/show reads now project the last startup/lane
reconciliation result without spawning Git on the owner thread; decision
timeout preserves prepared resources; piped child output is drained while the
child runs; Git target admission is deadline-bounded; target kinds are
validated at mutation boundaries; and generic deletion cannot orphan
Hub-managed rows.

## Verification and downstream-shaped proof

All commands exited successfully:

- `./test.sh managed_git_worktrees::tests`: 10 passed, including all three
  resolution cases, dirty reuse, collisions, restart adoption, controlled
  missing/hung Git, output above the pipe buffer, exact rollback, and
  preservation after content changes.
- `./test.sh spawn_targets::tests`: 4 passed, including registration-time
  defaulting, atomic set/clear/invalid/empty `base_ref` updates, kind typo
  rejection, and bounded hung-Git admission.
- `./test.sh worktrees::tests`: 3 passed, including Git-free managed DTO
  projection and record-only delete refusal.
- `./test.sh runtime::tests::managed_git`: 2 passed, including real lane
  contention and decision-timeout resource preservation.
- `./test.sh -p botster-hub-client`: 43 tests and 4 doc tests passed.
- `npm run sync` and `npm run check` in `packages/hub-test-support`: generated
  TypeScript and support fixtures match revision 21.
- `npm test` in `packages/hub-test-support`: passed.
- A clean external consumer installed the packed 0.1.13 candidate
  (`sha1 205590707fd009d1849ceae8abf0ecd29d5d6669`), verified all package
  checksums, materialized the plugin fixture, asserted version 0.1.13 and
  revision 21, and found the generated `base_ref` and `management` DTO tokens.
- `npm view @trybotster/hub-test-support@0.1.13 version` returned the expected
  registry `E404`, proving the candidate coordinate is still unused rather than
  colliding with another published meaning.
- `cargo check --workspace --offline`: passed against the locked Core
  coordinate.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`:
  passed.
- `git diff --check`: passed.
- `./test.sh`: passed in full on the rebased tree. The suite reported 130
  library tests, 12 binary tests, 14 capability tests, 23 client API tests,
  102 passing daemon lifecycle tests with one documented adversarial test
  ignored, 1 local runtime test, 20 Lua
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
and worker-owned lane release limit those risks.

Managed list/show returns the last status established by startup or the
managed-Git lane so DTO reads remain Git-free; an out-of-band filesystem change
becomes visible at the next startup or managed operation rather than by
shelling out during every read. Version 0.1.13 is a verified package candidate,
not a claimed npm publication; registry publication and installed-registry
proof remain release-workflow work. Short temporary data roots remain necessary
where the full Hub/Core Unix socket path would exceed the platform limit.

No conflicting or missing vault guidance was discovered. Four durable rules
were confirmed and are captured here pending post-merge vault promotion:
registration-time `base_ref` defaulting versus spawn-time stored authority;
deterministic identity plus exact restart adoption; removal of only
call-created clean resources with reconciliation on doubt; and target-filtered
Lua reads plus one scoped atomic mutation operation.
