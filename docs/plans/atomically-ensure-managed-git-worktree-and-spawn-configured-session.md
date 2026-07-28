# Atomically Ensure a Managed Git Worktree and Spawn a Configured Session

## Target and context

- Target repository: `trybotster/botster-hub` (`botster-hub`)
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Assigned branch/worktree: the Project Pipelines ticket worktree for `ticket_1785192690_547868`; its `origin` was verified as `trybotster/botster-hub`.
- Base: `d79403c`, the `origin/main` merge of the closed prerequisite ticket `ticket_1785192683_691772`.
- Repository charter: [[botster-hub-playbook]]
- Affected in-repository client-contract charter: [[botster-hub-client-playbook]]
- Role and surface guidance: [[planner-playbook]], [[botster-planner-playbook]], and [[botster-runtime-reviewer-playbook]].
- Architecture maps and atomic constraints: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[botster hub is a first party host profile over core]], [[botster hub gravity must be watched before it becomes the new monolith]], [[botster hub events use bounded priority lanes instead of unbounded queue fuses]], [[hub supervision admission changes require exact live hub launch proof]], [[botster data plane bypasses the hub through session and client actors]], [[botster local client api lives over hubruntime not raw core routers]], [[queued hub orchestration uses a shared queue policy]], [[device hub owns admitted spawn targets not ambient repo cwd]], [[workspace session templates are hub owned capabilities callable from lua workers]], [[session template override sources use package device repo explicit precedence]], [[botster workspace records are plugin owned references not hub authority]], [[plugin capability tests must validate against real lua runtime table not injected stubs]], [[an mpsc round trip is not a durability barrier]], [[test script required for rust tests not cargo test]], [[rust repo strict lints must be verified before dismissing warnings]], [[workspace struct field changes require workspace cargo gates]], [[botster hub client crate is the external client boundary]], [[botster hub client compatibility descriptors belong in client crate]], [[daemon event shape changes bump conformance fixture revision not protocol version]], and [[generated typescript dtos must encode serde field optionality]].
- Workflow artifact guidance: [[project pipeline orchestration belongs in a device-level botster plugin]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]], [[plan agents must author vault context as wikilinks not home paths]], and [[vault example paths are not repository placement conventions]].
- `[[project-pipelines-playbook]]` was not loaded: this ticket changes Hub runtime/client code, not Project Pipelines package paths or workflow policy.
- Implementation dependency amendment: canonical UUID production proof exposed a
  macOS Unix-socket path limit in Core. Human answer
  `question_1785202296_840826` required preserving full UUID identity and routing
  the private transport fix to Core. Dependency
  `ticket_1785211693_238645` is now closed; this run consumes tested Core
  coordinate `e36435f2` and contains no Hub session-id compaction workaround.

## Existing production seams

The repository already has the pieces this ticket should compose:

- `SpawnTarget` and `Worktree` are additive v1 `HubState` records, persisted through `FileHubStateStore` and projected to Lua and the daemon/client protocol.
- `session_templates.rs` owns package/device/repo precedence, target eligibility, context assembly, and materialization into `SessionSpawnRequest`.
- `HubSessionTemplateSpawner` is the real Lua-worker-to-Hub-owner request bridge. The owner thread checks the exact package capability, records context, calls `CoreDaemon::spawn`, and rolls context/session state back on failure or lost response delivery.
- Lua currently has read-only target/worktree projections and only `session_templates.spawn`; it does not expose template list/show and does not run Git.
- Generic worktree CRUD admits an already-existing directory and never deletes filesystem contents. Its behavior remains valid for non-Git and externally registered directories.

The new runtime path must extend these seams. It must not introduce plugin-run Git, a plugin-visible ensure-then-spawn race, a second spawn-target registry, or a parallel session-spawn implementation.

## Scope

1. Make Git capability a persisted declaration: the exact existing `SpawnTarget.kind` value `git` opts a target into managed-Git behavior; all other existing kind values remain non-Git and otherwise unchanged. Extend `SpawnTarget` with a serde-defaulted optional `base_ref`, including Git-specific validation, daemon request/response DTOs, generated TypeScript, CLI create/update/printing, persistence compatibility, and docs.
   - Creating or updating a target to `kind = "git"` verifies that its root is a usable Git worktree/repository. Registration may derive `base_ref` once from the checkout's current symbolic branch/ref if the operator omitted it.
   - The stored value is thereafter authoritative. Atomic spawning must reject a missing or invalid stored `base_ref`; it must never guess `main`/`master` or reread live `HEAD` as policy.
   - Existing and new `kind = "directory"` rows remain supported for existing target/template/worktree behavior, even when their roots happen to be Git repositories. The atomic operation rejects them with `target_not_git` until the operator explicitly updates the declaration.
   - A target declared `kind = "git"` whose root later ceases to be a repository fails with typed `repository_unavailable` before mutation; Hub does not silently downgrade it or infer a replacement capability from the filesystem.

2. Add a small Hub-owned managed-Git module using argument-separated subprocesses and the system `git` resolved from `PATH`; do not add a configurable executable path.
   - Preflight the executable before repository or state mutation and map missing, unexecutable, or unusable Git to sanitized `git_unavailable`.
   - Validate branch names with Git, resolve repository/common-dir identity, verify `base_ref` resolves to a commit, and parse `git worktree list --porcelain` without scraping human output.
   - Place managed worktrees at `<data-dir>/managed-worktrees/<target-id>/<encoded-full-branch>`, using validated target ids and a reversible or hash-suffixed collision-free branch encoding.
   - Reuse an exact matching managed worktree, add a worktree for an existing unowned branch, or create the missing branch from the stored base commit and add its worktree.
   - Reject path/repository/branch collisions, a branch checked out at another path, invalid base refs, disabled/missing targets, and mismatched persisted records before mutation.
   - Dirty matching worktrees are reusable; the ensure path performs no reset, checkout, clean, fetch, pull, prune, or deletion of caller-owned content.
   - Give the operation a 25-second whole-operation deadline beginning when Hub accepts the request, below the existing 30-second plugin bridge timeout. Queue wait consumes that budget; an expired waiting job is rejected before mutation. Allow at most 20 seconds for Git work, preserve 5 seconds for owner persistence/spawn/rollback/reconciliation, and pass the smaller remaining deadline to every subprocess. A timed-out child is killed and reaped and returns typed `ensure_timed_out`; the caller must not fall through to the bridge's generic timeout.

3. Reuse the durable `Worktree` registry for successful managed worktrees, with an additive serde-defaulted management/provenance discriminator (`registered` versus `hub_managed_git`).
   - Existing public `CreateWorktree` remains an admission of an existing path under the target root and remains non-destructive.
   - Deliberately change the registry invariant only for `hub_managed_git` rows: they live under the explicit Hub data-directory managed root and are exempt from generic target-root containment. Their reconciliation verifies the exact deterministic path, Git common-dir identity, and branch ownership and reports `present`, `missing`, or `stale` from those facts. Generic `registered` rows retain today's target-root containment and status behavior.
   - The management discriminator and resulting status semantics are DTO-visible. Update public client/TypeScript projections and conformance fixtures as required; list/show of a valid managed row must report it as present rather than stale.
   - Startup and pre-operation reconciliation adopt an exact deterministic worktree left between Git creation and state persistence, refresh matching rows, and report mismatches without deleting anything.

4. Extend the existing worker-to-owner session-template bridge with one atomic request, exposed as one scoped Lua operation such as `botster.capabilities.session_templates.ensure_worktree_and_spawn`.
   - Require a new exact `SessionActions` capability scope for the Git-mutating operation; the existing `session_template_spawn` scope must not imply it.
   - Accept only semantic inputs: `target_id`, `branch`, eligible `template_id`, and permitted prompt/ticket/workspace/safe metadata values. Generate a unique session UUID unless an explicitly admitted internal test input is used.
   - Resolve the effective template for the selected target. Add Lua `session_templates.list({target_id=...})` and `show({target_id=..., template_id=...})` projections that expose only enabled/effective templates eligible for that target.
   - Construct `cwd`, `repo_path`, `worktree_path`, `branch_name`, target/base facts, and session context inside Hub from the ensured worktree. Callers cannot override these trusted fields.
   - Keep ordinary `materialize_session_template` and `session_templates.spawn` unchanged: caller-supplied `cwd` remains absolute and contained by the template source root.
   - Add a separate internal trusted materializer that accepts an opaque ensured-managed-worktree result, never a public request or caller-controlled path. For this atomic operation, `PackageRoot` maps to the managed worktree root and `Relative { path }` maps to that relative path under the managed root after canonical containment validation. Repo-source template commands resolve from the managed worktree so the selected branch supplies repository code; package/device-source commands retain their trusted package/device source root. This defines `working_directory_policy` without weakening ordinary cwd admission.
   - Spawn through the existing `CoreDaemon` path. Return a tagged success containing `session_id`, target, branch, worktree id/path, stored base ref/resolved base commit, and created/reused facts.
   - Return tagged, sanitized typed failures. Do not require plugins to parse Git stderr or absolute-path-bearing free text.

5. Make the operation transactional across the effects Hub owns.
   - Add one internal managed-Git worker with capacity for one active and one waiting job, off the owner/plugin drain thread. The owner validates capability/target/template and uses nonblocking enqueue; saturation returns typed `ensure_backpressured`. The single worker serializes Git mutations and operation state from ensure through the owner's spawn decision and any rollback. This lane is the explicit in-process locking boundary; do not add a second unused lock.
   - Convert fulfillment of this request into a nonblocking owner-loop state machine: enqueue, poll for the worker's opaque prepared result on later owner ticks, persist/materialize/spawn, then send commit-or-rollback to the same worker and poll for its final result. The owner never waits on Git or cleanup and continues servicing coordination and ordinary template traffic. The Lua worker still sees one synchronous atomic call and never receives a plugin-visible ensure result or separate mutation operation.
   - Persist enough phase/provenance state that restart reconciliation can distinguish an exact managed worktree from a collision.
   - On spawn or response-delivery failure, shut down any newly spawned session, remove context, and roll back only the worktree/branch created by that call. Never remove a reused worktree or pre-existing branch.
   - Before deleting a newly created branch during rollback, reverify repository identity, deterministic path ownership, and the exact commit created by this call. If safe rollback cannot be proven or a cleanup command fails, preserve the resource, reconcile/persist its actual state, and return a typed rollback/reconciliation error.
   - A deadline or response-delivery failure does not release the lane until cleanup/reconciliation completes, so a retry cannot interleave with prior rollback. If the 5-second reserve cannot prove safe cleanup before the bridge deadline, preserve the resource and durable reconciliation evidence and return the operation's prepared typed reconciliation failure.

6. Document the public/operator contract in `README.md` and `docs/client-protocol.md`, including `base_ref`, managed-root ownership, non-destructive reuse/conflict behavior, Lua template filtering, the single atomic capability, and the distinction from generic worktree CRUD.

## Non-scope

- No Git fetch/pull/push, remote branch discovery, merge/rebase, reset/clean, arbitrary branch deletion, or general repository-management API.
- No automatic migration that reclassifies legacy directory targets or fills missing `base_ref` from mutable `HEAD` while spawning. Operators must explicitly update a legacy target to `kind = "git"` before using the new operation.
- No plugin-side filesystem path selection, Git subprocesses, or separate worktree-create/session-spawn mutation calls.
- No changes to `botster-core`, terminal byte/data-plane routing, renderer behavior, Project Pipelines policy, or the `botster-workspaces` data model.
- No broad rewrite of generic spawn-target/worktree CRUD or unrelated session-template APIs.
- No compatibility alias or second versioned operation.

## Ownership boundaries and dependencies

- Hub owns target admission, stored `base_ref`, managed-root naming, Git execution, locking, reconciliation, rollback, template eligibility/materialization, trusted context, capability enforcement, and CoreDaemon spawn.
- `botster-hub-client` owns the external optional `base_ref` DTO/request shape, serde behavior, generated TypeScript, compatibility metadata, and downstream fixture parity; it does not own Git policy.
- `botster-core` continues to receive only the fully materialized generic
  session spawn request. The separately routed and now-closed Core dependency
  `ticket_1785211693_238645` made worker socket paths independent of caller
  session-id length; Hub consumes that fix through its normal `main` Git
  dependency and retains canonical UUIDs.
- Plugins own only workflow records and references to the returned target/worktree/session ids. They cannot mutate Git or claim filesystem authority.
- The closed Hub UI-contract prerequisite is present at the run base. It is not an implementation blocker for this runtime ticket.
- The open `botster-workspaces` and final integration tickets are downstream consumers/proof owners, not code dependencies to absorb into this run. If their eventual request shape needs additional product fields beyond the ticket's target/branch/template/context inputs, register that change against their repository target rather than expanding Hub speculatively.

## Assumptions and unknowns

- “Matching existing managed worktree” means the deterministic Hub path, selected target repository identity, and requested local branch all agree. A worktree for the branch at any other path is a conflict, even if clean.
- The requested branch is a local branch name. Remote-only refs are out of scope; missing local branches start from the stored `base_ref` commit.
- `base_ref` may be a branch, tag, or commit-ish that resolves to a commit. The response records both the stored ref and resolved commit so mutable ref movement is observable for that operation.
- Each successful call spawns a new session while idempotently reusing the same worktree. The ensure sub-operation is idempotent; session creation is intentionally not deduplicated.
- The 25-second deadline is a deliberate product limit for this synchronous Lua operation: a repository whose checkout cannot complete within it gets a typed failure rather than monopolizing Hub or producing ambiguous late success.
- Hub's daemon singleton is per data directory, not per repository. Independent Hubs/data directories may admit the same repository; cross-process safety relies on Git's own worktree/branch locking and conflict behavior, translated to typed Hub conflicts. Hub does not claim a machine-global repository lock.
- Existing persisted rows default to `kind = "directory"` and `management = "registered"` under their existing serde defaults. A legacy target rooted at a Git repository is not opted into managed Git until explicitly updated to `kind = "git"` and given or registration-defaulted a `base_ref`.
- Exact public names for the new error enum and internal provenance fields may follow adjacent source naming, but the Lua method remains a single unversioned atomic capability and its tagged result fields are contract-tested.
- No blocking product ambiguity remains. If implementation discovers that a template must be eligible for multiple targets under the current single-`target_id` declaration, stop and ask rather than silently broadening the template schema.

## Affected surfaces and likely files

- `src/spawn_targets.rs`: `base_ref` persistence, Git-capability/default validation, create/update behavior, and typed errors.
- `src/managed_git_worktrees.rs` (new) and `src/worktrees.rs`: bounded Git command boundary, deterministic managed root, exact matching/collision checks, management discriminator, separate registered/managed reconciliation, rollback, and restart adoption.
- `src/session_templates.rs`: target-filtered list/show helpers and the opaque internal trusted managed-worktree materializer while preserving ordinary source-root cwd admission.
- `src/runtime.rs`: bounded off-owner Git lane, operation deadline/state handoff, atomic pending-request bridge, capability check, session UUID generation, persistence/spawn decision, rollback/response-loss handling, and startup reconciliation.
- `src/lua_runtime.rs`: filtered list/show plus the one atomic Lua method and tagged result conversion.
- `src/persistence.rs`: additive v1 defaults and legacy-state/restart tests.
- `src/profile.rs`: admit the new exact first-party capability scope.
- `src/daemon_transport.rs`, `src/main.rs`, and `src/lib.rs`: `base_ref` operator request/projection/CLI/export wiring; no separate public ensure and spawn endpoints.
- `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/src/typescript.rs`, `crates/botster-hub-client/generated/daemon-protocol.ts`, and `packages/hub-test-support/daemon-protocol.ts`: serde-accurate optional DTO/request fields and generated artifact parity. Bump the conformance fixture revision if the checked public fixture bytes change; do not add a new feature constant unless a genuinely new daemon client capability is exposed.
- `tests/hub_lua_runtime_test.rs`, `tests/hub_client_api_test.rs`, and `tests/hub_daemon_lifecycle_test.rs`: focused policy tests, real Git repositories, real Lua worker calls, public DTO coverage, and exact live-Hub restart proof.
- `README.md` and `docs/client-protocol.md`: operator, plugin-author, ownership, and compatibility documentation.

## Risks and controls

- **Git command ambiguity or injection:** pass every argument separately to `Command`; validate refs with Git; use porcelain/NUL-capable output; never invoke a shell.
- **Missing or hung Git:** resolve from `PATH`, preflight before mutation, impose the operation/subprocess deadlines, kill and reap timed-out children, and return path-neutral typed errors.
- **Owner-loop starvation or queue growth:** run Git only on the one bounded worker lane; owner polling remains free for coordination requests, queue saturation is explicit backpressure, and every job has the same whole-operation deadline.
- **Branch/path race:** serialize in-process mutation and rollback on the single managed-Git lane; re-observe before rollback. Across independent data directories, rely only on Git's repository locks and translate their conflicts without cleanup.
- **Wrong-repository collision:** compare canonical Git common-dir identity and branch, not only directory existence or `.git` presence.
- **Destructive rollback:** track call-created resources and verify exact identity/commit immediately before removal; preserve and reconcile on doubt.
- **Hidden mutable policy:** registration-time defaulting is the only permitted `HEAD` read for `base_ref`; every spawn reads the persisted field and resolves it explicitly.
- **State split between `HubRuntime`, `HubDaemon`, and disk:** make runtime state updates use the same atomic `FileHubStateStore` boundary and refresh all shared target/worktree projections before replying. Do not treat an in-memory queue response as a durability barrier.
- **Untrusted context smuggling or cwd broadening:** the Lua parser rejects caller values for cwd/repo/worktree/branch/base fields; only the private opaque ensured result enters the trusted materializer; ordinary template spawning retains source-root containment.
- **Protocol drift:** keep `base_ref` optional on old frames/state, regenerate both checked TypeScript artifacts, and run workspace—not crate-only—tests.
- **Unwired implementation:** require proof through a packaged Lua plugin on the live `botster-hub` binary, not only module tests or injected Lua tables.

## Acceptance checks

### Focused model and Git behavior

- Real temporary repositories with commits prove:
  1. an exact existing managed worktree is reused without changing dirty files;
  2. an existing local branch without a worktree gets the deterministic worktree;
  3. a missing branch is created from the stored `base_ref` commit and gets the deterministic worktree.
- Assert branch names with slashes and path-hostile-but-Git-valid characters map collision-free and round-trip to the requested branch fact.
- Assert typed, path-neutral failures and zero unintended mutation for missing/disabled/directory targets, a declared Git target whose root ceased to be a repository, missing/invalid base refs, invalid branch names, a branch checked out elsewhere, deterministic-path collisions, wrong-repository worktrees, and mismatched persisted records.
- With a controlled command runner/PATH, prove absent and unexecutable Git return `git_unavailable` before filesystem or registry mutation.
- Drive two concurrent submissions directly against the bounded coordinator/managed-Git lane, where contention is real. Assert at most one Git mutation is active, exactly one ensure creates the branch/worktree, the second reuses it after the first operation outcome, and two successful atomic operations produce distinct session UUIDs. Do not treat two calls drained serially by the existing Lua bridge alone as lock proof.
- Run two independent Hub processes with different data directories against one repository and branch. Assert Git permits only one branch/worktree owner, the loser receives a typed conflict, and neither Hub deletes or corrupts the other's resource.
- Force spawn failure after each of the “new branch/new worktree” and “existing branch/new worktree” paths. Verify only call-created resources roll back, pre-existing branches/worktrees and dirty content remain, and rollback failure leaves reconciled durable evidence.
- Use a fake/controlled Git child that exceeds the 20-second Git budget. Prove the child is killed/reaped, the Lua caller receives `ensure_timed_out` before the outer 30-second timeout, no session is orphaned, actual worktree/branch state is durably reconciled rather than silently deleted, and an immediate retry cannot enter until prior cleanup/reconciliation releases the lane. While the child is pending, coordination publish/drain/ack traffic remains serviceable.

### Real Lua and runtime path

- Through the production `capabilities_table` and real plugin worker:
  - list/show return only enabled effective templates eligible for the selected target;
  - a package with only the old or unscoped session capability is denied before Git mutation;
  - the exact new scope can call only the single atomic method;
  - caller attempts to supply cwd/repo/worktree/branch/base facts are rejected or ignored in favor of Hub-derived values;
  - ordinary `session_templates.spawn` still rejects cwd outside its source root;
  - the private atomic path maps package-root/relative working-directory policies under the ensured worktree, rejects relative escape/symlink escape, and repo-source execution observes files from the requested branch while package/device command sources remain trusted;
  - success returns a UUID plus target/branch/worktree/base facts and the session exists in CoreDaemon;
  - the spawned fixture reads Hub context and proves the derived cwd, repo/worktree path, branch, target, stored base ref, and resolved base commit.

### Live Hub, persistence, and restart

- Start the exact built `botster-hub` binary with an isolated data directory and matching `botster-session-worker`, install/enable a fixture package declaring the new capability, invoke its MCP/action handler over the daemon path, and observe the real session and Git worktree.
- Restart Hub with the same data directory and repository, prove startup reconciliation recognizes the exact managed worktree, then call the atomic operation again and prove reuse without a second path/branch.
- Seed crash-window states (Git worktree exists without a row, stale row with exact path, and mismatched row/path) and prove restart adopts only the exact match and reports collisions without cleanup.
- List/show a valid `hub_managed_git` row and prove status is `present` despite living outside `target.root`; missing and mismatched managed rows report `missing`/`stale`. Generic `registered` rows still require target-root containment and preserve existing statuses.
- Create/update/list/restart tests prove `kind = "git"` targets persist `base_ref`; plain old v1 targets/state files deserialize as directory/registered with `base_ref = None` and receive `target_not_git`. Updating one to Git validates the repository and captures or accepts `base_ref`; removing its repository afterward produces `repository_unavailable` without consulting live `HEAD` for policy.

### Client contract, docs, and repository gates

- Serde round trips cover `base_ref` omission/presence on create, update, and response DTOs; generated TypeScript marks optional fields optional and both checked artifacts equal generator output.
- If public fixture bytes change, allocate a unique `CONFORMANCE_FIXTURE_REVISION` and update source-derived support-matrix expectations; keep `PROTOCOL_VERSION` unchanged unless request/response semantics actually require a wire-version change.
- Focused commands:
  - `./test.sh --test hub_lua_runtime_test <new_atomic_operation_filter>`
  - `./test.sh --test hub_client_api_test <spawn_target_or_template_filter>`
  - `./test.sh --test hub_daemon_lifecycle_test <managed_worktree_spawn_filter> -- --test-threads=1`
- Required repository gates:
  - `cargo fmt --check`
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  - `./test.sh`
  - `git diff --check`
- Inspect test counts/output so a filename-like filter cannot pass while running zero tests. Record the exact live-Hub binary/worker paths, commands, exit codes, restart observations, and cleanup evidence in the implementation/verification artifacts.

## Pipeline gates and artifacts

- Plan: this committed document plus the Project Pipelines plan gate evidence and vault checklist.
- Plan Review: reject any design that exposes separate Git/worktree mutation calls to plugins, reads live `HEAD` during spawn, lacks exact collision/rollback rules, or proves only in-memory/module behavior.
- Implement: committed code and synchronized plan, exact test/strict-lint evidence, generated artifact parity, and a linked PR before review.
- Review/Verify: apply the Hub runtime and hub-client overlays; rerun the exact live-Hub restart path and all resolved findings against the live worktree.
- Final downstream proof remains the separate integration ticket. This Hub run must nevertheless leave a packaged, production-path fixture/API that the `botster-workspaces` and Web/TUI integration tickets can consume without sibling-worktree overrides.

## Vault gaps worth capturing

- Capture after implementation if confirmed: the stable rule for registration-time `base_ref` defaulting versus spawn-time stored-ref authority.
- Capture the final deterministic managed-worktree identity and restart-adoption rule once code and tests establish it.
- Capture the rollback rule that only call-created Git resources may be removed and every doubtful cleanup becomes reconciliation, not deletion.
- Capture the scoped Lua contract: target-filtered template reads plus one atomic managed-worktree/session spawn operation.
- Do not capture speculative names or path layouts before the implementation and live restart evidence settle them.
