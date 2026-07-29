# Allow Authorized Cross-Package Managed Template Spawning

## Target and context

- Target repository: `trybotster/botster-hub` (`botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Pipeline ticket/run: `ticket_1785298630_852133` /
  `run_1785298641_743448`.
- Repository charter: [[botster-hub-playbook]].
- Role and surface playbooks: [[planner-playbook]],
  [[botster-planner-playbook]], [[botster-runtime-reviewer-playbook]], and
  [[botster-package-reviewer-playbook]].
- Downstream surface charters loaded for boundary and acceptance planning:
  [[botster-workspaces-playbook]] and [[project-pipelines-playbook]]. They do
  not transfer either package's product policy into Hub.
- Architecture maps and atomic notes loaded: [[botster-architecture]],
  [[cli-patterns]], [[spa-patterns]],
  [[botster hub is a first party host profile over core]],
  [[botster hub gravity must be watched before it becomes the new monolith]],
  [[botster data plane bypasses the hub through session and client actors]],
  [[botster local client api lives over hubruntime not raw core routers]],
  [[botster hub events use bounded priority lanes instead of unbounded queue fuses]],
  [[workspace session templates are hub owned capabilities callable from lua workers]],
  [[session template override sources use package device repo explicit precedence]],
  [[botster workspace records are plugin owned references not hub authority]],
  [[plugin capability tests must validate against real lua runtime table not injected stubs]],
  [[review must diff stale capability disclaimers when behavior changes]],
  [[may supervise permits the hub to supervise the package entrypoint]],
  [[hub supervision admission changes require exact live hub launch proof]],
  [[live hub proof records distinct hub and locked core binary provenance]],
  [[project pipeline orchestration belongs in a device-level botster plugin]],
  [[project pipelines needs an operator workbench not more primitives]],
  [[project pipelines ui contract belongs in the plugin readme]],
  [[botster orchestration should spawn agents with explicit target ids]],
  [[botster orchestration prompts must bind agents to explicit worktrees]],
  [[botster pipeline needs continuous product owner between agent steps]],
  [[plan agents must author vault context as wikilinks not home paths]], and
  [[vault example paths are not repository placement conventions]].
- Repository context inspected: `README.md`, `Cargo.toml`, `Cargo.lock`,
  `test.sh`, `src/runtime.rs`, `src/lua_runtime.rs`,
  `src/session_templates.rs`, `src/packages.rs`,
  `src/managed_git_worktrees.rs`, `src/daemon.rs`,
  `src/daemon_transport.rs`, `tests/hub_lua_runtime_test.rs`,
  `tests/hub_daemon_lifecycle_test.rs`,
  `examples/project-pipelines/**`, and the predecessor plan/report for the
  atomic managed-Git operation.
- Downstream context inspected read-only:
  `botster-project-pipelines/plugin.lua`, its real-Hub acceptance scripts and
  package manifest, plus the current `botster-workspaces` package/caller
  surface. The Workspaces checkout contains unrelated local changes and is
  evidence only; this run must not edit or depend on that dirty checkout.
- Locked substrate: `Cargo.lock` resolves `botster-core` and
  `botster-core-daemon` to
  `e36435f2cb583c344d6f6ba2d62c39da324c7a64`. Core receives only a generic
  materialized `SpawnSessionRequest`; no new Core contract is currently
  indicated.

## Existing behavior and production entry point

The production call is:

`package A Lua worker -> botster.capabilities.session_templates.ensure_worktree_and_spawn -> HubSessionTemplateSpawner -> HubRuntime managed-Git owner state machine -> materialize_managed_session_template -> CoreDaemon::spawn`.

The existing Hub integration proves only a same-package arrangement:
`session-template-spawner.plugin` both owns the exact managed-spawn
capability and contributes the selected template. Its focused test passes.
Production downstream proof separates those identities: Project Pipelines or
Workspaces is the authorized caller while another enabled package contributes
the target-effective template. Current requests can prepare and then roll back
the worktree but collapse the later failure to `spawn_failed` /
`configured session could not be spawned`.

The code ordering materially narrows the diagnosis. `HubRuntime` calls
`validate_managed_git_request` before `ManagedGitCoordinator::submit`.
Caller capability admission and target-filtered template eligibility therefore
fail before worktree preparation, with their own typed errors. The reported
sequence—prepare, roll back, then return `configured session could not be
spawned`—reaches the `CoreDaemon::spawn` failure arm in `src/runtime.rs`,
where the inner Core error is currently discarded. Also,
`source_templates` already searches all supplied package records rather than
filtering to the caller package. An implicit same-package restriction or stale
package snapshot is therefore not an established root cause of the reported
failure.

The implementation must first reproduce both reported package-A/package-B
failures and retain the discarded inner error in Hub-only test diagnostics.
The fix must then follow that evidence. The verified load-bearing boundary is
command/source materialization plus Core dispatch; package registry projection
changes are allowed only if a separate failing-first test proves they are
required for the production selection or atomic path.

Throughout diagnosis and the final fix, treat these as independent principals:

- caller package identity authorizes the operation and remains the spawned
  session's owner metadata;
- template source identity controls enabled/effective lookup, declared target,
  trusted source root, command resolution, and template context;
- selected spawn target controls repository admission and the managed
  worktree;
- the caller cannot supply or spoof any of those trusted identities or paths.

`LuaPluginRuntime` currently captures all `PackageRecord` rows when the caller
package is loaded and carries that snapshot with template list/show and spawn
operations. That is a potential stale-projection defect, but it cannot produce
the ticket's reported post-prepare error. It is not part of the primary fix
without a concrete reproduction.

## Implement disposition

Implementation reproduced the downstream Workspaces failure on the exact
reported Hub commit `35e92f46a98c445765b6ba7755e029f5dde702f8`,
locked Core commit `e36435f2cb583c344d6f6ba2d62c39da324c7a64`,
package records, target, template, and public plugin-worker call. Retaining the
discarded raw Core error identified the concrete cause: the worker control
socket parent was not owned by the effective user with private permissions.
The durable Hub diagnostic deliberately records only the typed, path-neutral
Core kind `runtime.spawn_failed`; Hub does not couple itself to Core's
free-text diagnostic.

The Core session worker rejected a pre-existing hashed control-socket parent
whose permissions were not private. This happened after managed-worktree
preparation and therefore produced the reported rollback plus generic
`spawn_failed` shape. It was unrelated to caller/template package identity,
template eligibility, command root, or package-record projection.

The same Hub state, target, package A caller, package B
`project-pipelines/agent-step` template, and Workspaces invocation succeeded
after starting the matched rebuilt Hub/worker with a fresh private temporary
socket root. It returned canonical UUID
`4e7cb99c-345c-410a-9ff4-095f4046dc4a`.

Per the answered blocking Project Pipelines question
`question_1785300505_529418`, this supersedes the speculative production
portions of Scope 2-7 for this Hub implementation:

- do not add a Hub workaround for Core-owned worker-socket state;
- preserve real-worker cross-package and shipped Project Pipelines success as
  regression coverage, including canonical UUID, caller capability denial, and
  mismatched-target rejection;
- retain a path-neutral Hub-side Core error classification while keeping the
  Lua error sanitized;
- leave registry projection, command materialization, authorization, and
  public error kinds unchanged because the matched evidence found no defect in
  those boundaries;
- route any repair or cold migration for stale Core worker-socket directories
  to a separately targeted `botster-core` ticket.

The matched Workspaces registry enabled the Project Pipelines contributor
before loading the Workspaces caller worker. The caller's production
`list`/`show`/atomic calls all saw that contributor, so this run takes the
plan's passing selection-path branch and leaves registry projection unchanged.
Late enablement after a caller worker is already loaded remains a distinct
snapshot question; it is not the ordering used by the reported production
path and requires a separate Hub ticket if product behavior should support it.

## Scope

1. Reproduce and localize the exact production-shaped failure before changing
   behavior.
   - Add a failing real-worker case where authorized package A invokes an
     explicit target-bound template from enabled package B and observes the
     reported post-prepare `spawn_failed`.
   - Add a second failing case using Hub's shipped
     `project-pipelines/agent-step` template with a Git spawn target whose
     literal id is `package:project-pipelines`.
   - At the `CoreDaemon::spawn` boundary, capture the inner `CoreDaemonError`
     in test-visible Hub diagnostics/logging while preserving the existing
     sanitized Lua-facing error. Record the concrete failing command, resolved
     root/cwd category, and Core error class in the implement report.
   - Do not treat current-registry staleness or same-package filtering as the
     cause unless one of these failing tests demonstrates it.

2. Make command/source materialization and Core dispatch explicitly
   cross-package safe.
   - Keep `PackageRoot`/`Relative` working-directory policy mapped to the
     managed worktree as the existing atomic contract requires.
   - Resolve package/device template commands from the selected template
     source root and repo template commands from the selected managed
     worktree; never substitute the caller package root.
   - Canonicalize and contain source/command paths at the Hub boundary. Treat a
     missing, unsafe, non-executable, or otherwise unresolvable configured
     command as a typed template/source resolution failure before
     `CoreDaemon::spawn`.
   - Continue assembling `target_id`, repository/worktree paths, branch/base
     facts, and trusted context inside Hub. Preserve the Lua rejection of
     caller-supplied trusted fields.
   - Keep session ownership metadata bound to authenticated package A while
     retaining template package B only as internal resolution provenance.
   - Preserve the inner error class and safe structured context in Hub-side
     diagnostics/audit, but return only the sanitized typed error to Lua.
   - Fix the reproduced cause at this boundary with the smallest change. Do
     not add a caller/template compatibility branch.

3. Preserve the already-correct caller/template authorization split and test
   it explicitly.
   - Resolve the caller by the worker-authenticated `PluginKey`; never accept a
     caller/package field from Lua.
   - Require package A to hold the exact
     `SessionActions/session_template_managed_git_spawn` capability.
   - Resolve package B's template through existing enabled-source precedence
     and exact target eligibility. Prefer fully qualified ids and preserve
     ambiguity rejection.
   - Keep all capability and template-eligibility failures before Git
     preparation. Do not move or duplicate admission after preparation merely
     to satisfy the cross-package case.

4. Resolve the load-time package projection only if a failing-first production
   path proves it necessary.
   - Before changing registry plumbing, load package A's worker, then enable
     package B and test `session_templates.list`, `session_templates.show`, and
     the atomic operation through the real worker.
   - If list/show cannot enumerate B's target-effective template, include the
     read projection in the same narrow current-registry change so the
     Workspaces selection path is actually fixed; acceptance must cover
     enumeration, show, and spawn.
   - If the reported configurations load both packages before the caller
     worker, document that evidence and leave registry plumbing unchanged.
     If staleness is reproducible but independent of this ticket's production
     configuration, create a separately targeted Hub ticket rather than
     widening this fix.
   - If a current projection is required, use the daemon-owned
     `HubState.package_registry` consistently for list/show and atomic
     validation; do not create a second authority or a general template
     service.

5. Preserve sanitized typed error boundaries.
   - Authorization denial: exact caller capability/package admission failure.
   - Incompatibility: target disabled/not Git, template disabled, or template
     not effective/eligible for the selected target.
   - Resolution: unknown/ambiguous template, unavailable source root, unsafe
     or missing command/cwd.
   - Runtime spawn failure: a fully resolved request reaches Core but the
     session runtime rejects or cannot start it.
   - Continue returning the existing tagged `{ok=false,error={kind,message}}`
     Lua shape without Git stderr, raw package paths, Core internals, or
     caller-controlled values in diagnostics.
   - Preserve generated canonical UUID success and the existing rollback
     cleanup when any post-prepare stage fails.

6. Add production-shaped tests where package A and package B are distinct.
   - Build package A as a real loaded Lua worker with only the exact managed
     spawn capability and no relevant template.
   - Build package B as an independently enabled template contributor without
     package A's authority.
   - Exercise fully qualified target-effective template ids through the public
     plugin MCP/worker call, not a direct Rust helper or injected Lua
     capability stub.
   - Include the shipped `project-pipelines/agent-step` package template in the
     matrix with a target admitted as `package:project-pipelines`.
   - Retain the passing same-package test as compatibility coverage, not as the
     cross-package oracle.

7. Update only Hub-owned reference docs whose capability/error wording changes.
   Document that caller authorization and template contribution are separate,
   and remove any same-package implication. Check Project Pipelines/Workspaces
   README wording for downstream drift, but route required package-repository
   edits to their own targets instead of modifying them here.

## Non-scope

- No Workspaces-specific or Project-Pipelines-specific branch in Rust, package
  name allowlist, caller spoofing field, local override, or same-package
  compatibility path.
- No changes to `botster-workspaces` records, Project Pipelines workflow
  policy, package UI, pipeline schema, or either downstream repository.
- No new template source precedence, multi-target template schema, arbitrary
  template/path access, or caller-selected command/cwd.
- No new capability scope, compatibility alias, versioned operation, service
  object, or broad package-runtime abstraction.
- No change to Git ensure semantics, managed-root naming, locking, branch
  creation policy, generic worktree CRUD, Core session contracts, terminal
  data paths, or renderer/client DTOs unless implementation proves a
  public-contract change is unavoidable.
- No adjacent refactor of ordinary `session_templates.spawn`. The list/show
  projection enters scope only if its failing-first real-worker test proves
  the production selection path is stale.

## Ownership boundaries and cross-repository dependencies

- `botster-hub` owns package admission, current registry truth, caller
  capability enforcement, template source/target resolution, trusted context,
  managed Git preparation/rollback/reconciliation, command materialization,
  CoreDaemon dispatch, and sanitized errors. This ticket is correctly routed
  to Hub.
- `botster-core` owns the policy-free generic spawn/runtime substrate. The
  locked revision already exposes the error categories and generic spawn
  request Hub needs. Do not change Core unless implementation proves that a
  required generic error distinction is absent; if so, create/register a
  dependency ticket against target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`
  (`botster-core`) instead of adding a Hub workaround.
- `botster-workspaces` owns workspace references and the user workflow that
  requests the capability; it never owns the template source, Git path, or
  returned UUID. Any caller-package manifest or product-test change belongs to
  target `tgt_71266a8d976d4535902ffed09c18a7ba`.
- `botster-project-pipelines` owns pipeline policy and its external live-Hub
  acceptance harness. Any package-side follow-up belongs to target
  `tgt_a72ca1a83d504385b8648f71409119ab`.
- No blocking cross-repository code dependency is known at Plan time. Required
  gate evidence is Hub-local: install the real shipped Project Pipelines
  package plus a minimal Workspaces-shaped caller package into the exact built
  Hub and exercise the public worker path. This substitutes for edits or
  provenance-pin advances in either downstream repository.
- The Project Pipelines `script/test-hub-flow` and a clean Workspaces
  product-flow run are non-blocking downstream corroboration, not prerequisites
  for this Hub ticket. If either requires a package code or pin change before
  it can run, create a follow-up ticket against its repository target and
  register that dependency on the follow-up run; do not leave this Hub gate
  conditional on unregistered work.

## Assumptions and unknowns

- “Cross-package” means the authenticated caller package and the selected
  template source package may differ; it does not permit package A to borrow
  package B's capabilities or package B to bypass package A's authorization.
- The reported failure is at or beneath Core dispatch because validation
  precedes worktree preparation and the reported message is emitted only by
  the `CoreDaemon::spawn` error arm. The exact inner cause remains unknown
  until the new failing-first cases retain it.
- Load-time package snapshots are a potential independent staleness issue, not
  an assumed cause. They are changed only if a real-worker failing-first test
  proves the ticket's selection or atomic path requires current projection.
- A template with an explicit target must match the requested target. A
  package template without an explicit target remains bound to its existing
  default `package:<package-name>` target. Consequently, the ephemeral package
  B fixture must declare the real Git target explicitly. Hub's shipped
  Project Pipelines template is exercised by registering a Git-kind spawn
  target whose literal id is `package:project-pipelines`, pointing at the test
  repository. This ticket does not make package templates globally eligible
  for arbitrary Git targets.
- If implementation cannot construct both required positive cases while
  preserving that exact target rule, stop and ask the product owner whether
  template target semantics are intended to expand. Do not silently make
  templates global or weaken equality.
- A fully qualified template id identifies its source, but source identity
  alone does not confer eligibility; enabled state, precedence, target
  admission, and path safety still apply.
- A canonical 36-character UUID is returned only after Core accepts the
  session. The caller cannot provide a session id to the atomic operation.
- Existing branch, missing branch, and existing managed-worktree cases share
  authorization/materialization behavior but retain their current
  created/reused rollback rules.
- The exact stable error-kind names may reuse or narrow current
  `ManagedGitError`/`SessionTemplateError` kinds. The acceptance requirement is
  that denial, incompatibility, resolution, and runtime spawn failure are
  machine-distinguishable and sanitized; do not expose raw inner errors merely
  to add detail.
- Unknown to resolve during implementation: the discarded inner Core error
  and whether it identifies command existence/executability, command root,
  cwd, environment admission, or a later runtime failure. The failing-first
  evidence determines the surgical fix.

## Affected surfaces and likely files

- `src/runtime.rs`: retain safe diagnostics for the discarded Core error,
  preserve owner state-machine rollback, and classify sanitized resolution
  versus runtime failures; change package projection only with failing-first
  evidence.
- `src/lua_runtime.rs`: primarily test the authenticated real-worker boundary.
  Change captured package-record handling only if the list/show/atomic
  staleness reproduction requires a current projection.
- `src/session_templates.rs`: expose/reuse a narrow resolved-source value or
  validation helper so package B's root/rank/target/command remain coherent;
  add managed command/source resolution checks without weakening ordinary
  spawn admission.
- `src/packages.rs` only if a proven staleness defect requires a narrow
  existing-registry helper without duplicating reconstruction policy.
- `tests/hub_lua_runtime_test.rs`: focused real-worker cross-package matrix,
  denial/target mismatch/path-resolution/runtime-failure negatives, canonical
  UUID, and atomic rollback assertions.
- `tests/hub_daemon_lifecycle_test.rs`: exact built-Hub cross-package,
  restart/reconciliation, and shipped Project Pipelines template proof.
- `README.md` and `docs/client-protocol.md`: caller-versus-template ownership,
  eligibility, error, and authorization contract if current text implies
  same-package behavior.
- `docs/reports/allow-authorized-cross-package-managed-template-spawning-implement-report.md`
  at Implement handoff, following current repository prior art.

No `botster-hub-client` or generated TypeScript change is planned because the
atomic operation is a Lua worker capability and its tagged result already
crosses the plugin tool boundary. If implementation changes a daemon/client
DTO after all, it must update the Rust client crate, generated TypeScript,
support-package parity, fixture revision, and downstream consumer proof as one
contract change.

## Risks and controls

- **Privilege union:** looking up capability and template on “any matching
  package” could combine package A's template with package B's authority.
  Resolve the authenticated caller record first; resolve template ownership
  separately; assert B cannot lend capability to A.
- **Stale authority:** a caller worker can outlive package B enable/disable or
  update. Reproduce this separately through list/show and atomic calls before
  changing authority plumbing; either cover all affected production reads or
  route the independent defect to another Hub ticket.
- **TOCTOU across prepare/spawn:** target, template, or source can change while
  the worker prepares a worktree. Revalidate current identity/effectiveness
  before materialization and let the existing finalization lane serialize
  rollback.
- **Wrong-root execution:** a package B command could accidentally resolve
  relative to package A or ambient cwd. Assert the marker/script originates
  from B's canonical root and that sibling/arbitrary paths are rejected.
- **Error collapse:** preflight resolution and Core runtime failures currently
  can both look like `spawn_failed`, and the inner Core error is discarded.
  Retain safe Hub-side error class/context, add explicit negative fixtures at
  each boundary, and assert stable distinct kinds rather than message
  fragments.
- **Rollback regression:** a later admission failure occurs after worktree
  preparation. Reuse existing rollback/reconciliation machinery and prove
  created resources are removed while reused worktrees and pre-existing
  branches survive.
- **False worker proof:** injected `botster.capabilities` tables can hide
  production wiring defects. All positive and authorization-negative calls
  must load a package through `LuaPluginRuntime` and dispatch via the real
  plugin worker/MCP path.
- **Hub gravity:** keep this as privileged admission/materialization policy
  around existing mechanisms. Do not absorb workspace/pipeline workflow or add
  a general cross-package broker.
- **Dirty downstream checkout:** the local Workspaces checkout has unrelated
  changes. Do not edit, commit, or treat it as source authority; use the Hub
  fixture for deterministic CI and run package-owned proof only in a clean
  dedicated checkout/worktree.

## Acceptance checks and tests

The Implement disposition above is authoritative over the earlier speculative
Scope 2-7 matrix. This branch must deliver:

- a real package-A Lua worker holding
  `session_template_managed_git_spawn`, with no contributed template, invoking
  enabled package B, which contributes the template but holds no spawn
  capability;
- `session_templates.list` and `show` assertions proving A sees B's fully
  qualified target-effective template;
- successful explicit-target B and shipped
  `project-pipelines/agent-step` atomic spawns, canonical 36-character UUIDs,
  and proof that B's command executes from the managed worktree;
- a caller capability denial and a mismatched-target
  `template_not_eligible` denial;
- a path-neutral, kind-based Hub diagnostic at the production Core dispatch
  boundary while the Lua result remains the existing sanitized
  `spawn_failed`;
- exact matched Workspaces reproduction and a fresh-private-socket-root control
  using Hub `35e92f46a98c445765b6ba7755e029f5dde702f8` and locked Core
  `e36435f2cb583c344d6f6ba2d62c39da324c7a64`;
- strict repository gates and downstream confirmation that the returned UUID
  persists, renders, survives reload, and works through the live packaged
  WebRTC harness.

The following pre-disposition checks are explicitly superseded rather than
silently waived:

- the existing-worktree and existing-local-branch cross-package variants,
  caller-owner/created-reused assertions, old-scope denial, smuggling, and the
  full resolution/rollback matrix remain owned by the existing same-package
  tests; the cross-package regression owns only the package
  identity/eligibility/authorization boundary;
- late contributor enablement after caller-worker load, contributor disable,
  and expanded resolution/error-taxonomy cases require a new Hub ticket if
  product behavior requires them; the matched Workspaces path enabled the
  contributor before caller load and passed list/show/atomic selection;
- a deterministic Core failure fixture and Core worker-socket cleanup or
  migration belong to Core target
  `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`; Hub retains a safe kind and operation
  correlation without matching Core diagnostic text;
- restart/reconciliation is covered by the matched downstream reload and
  persistence proof; no downstream repository changes are part of this Hub
  branch.

### Repository gates

Focused commands use the implemented test names and must each run a nonzero
count:

```sh
./test.sh real_lua_plugin_cross_package_managed_template_spawning -- --nocapture
./test.sh managed_session_core_error_diagnostic_is_kind_based_and_path_neutral
./test.sh real_lua_plugin_atomically_ensures_managed_worktree_and_spawns_session
./test.sh managed_session_template
cargo check --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
git diff --check
./test.sh
```

The full test suite remains required because package reload, persistence, Lua
worker, managed Git, and daemon lifecycle share this path. Implement and Review
both observed one passing test for each focused filter. After merging current
`origin/main` `7c6d9488481da3fc43c6fb813eeb583c507f802c`, Implement ran the
complete wrapper green: 133 library and 103 daemon-integration tests passed
(one larger local adversarial test ignored).

Plan-stage baseline evidence:

- `./test.sh real_lua_plugin_atomically_ensures_managed_worktree_and_spawns_session`
  passed one test through the repository wrapper, confirming the existing
  same-package path and its rollback fixtures.
- `git remote -v` identifies this worktree as
  `https://github.com/trybotster/botster-hub.git`.
- `Cargo.lock` records Core revision
  `e36435f2cb583c344d6f6ba2d62c39da324c7a64`.

## Pipeline gates and artifacts

- Plan artifact: this file.
- Plan Review must check the caller/template identity split, current-registry
  decision tree, exact reproduced Core error, Hub-local downstream-shaped
  proof, and that no package-specific branch or cross-repository edits entered
  scope.
- Implement evidence must include the focused matrix, full strict gates,
  exact-Hub/Core provenance, rollback/restart observations, and a committed
  implement report.
- Review must inspect stale Hub and downstream capability documentation,
  hidden same-package assumptions, unwired helpers, error-message-only tests,
  and whether each negative actually crossed the real plugin worker.
- Verify must rerun the cross-package real-Hub path and downstream smoke; code
  existence or a same-package-only test is insufficient.

## Vault gaps worth capturing

- Capture a durable note after implementation if confirmed: managed template
  spawning authorizes the caller package independently from the enabled
  package that contributes the target-effective template.
- Capture the runtime rule for current Hub package projection only if the
  list/show/atomic failing-first test proves it and the implementation changes
  that boundary.
- Capture a durable diagnostic-boundary note if confirmed: Hub preserves safe
  structured Core failure classification internally while returning sanitized
  typed plugin errors.
- Capture the final sanitized error taxonomy for denial, incompatibility,
  template/source resolution, and runtime spawn failure if it becomes a stable
  plugin ABI convention.
- No convention conflict was found. The plan keeps privileged policy in Hub,
  workflow state in packages, generic spawn mechanics in Core, and uses the
  existing resolver/state machine instead of introducing a service object or
  speculative abstraction.
