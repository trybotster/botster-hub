---
ticket: ticket_1785711607_995393
run: run_1785713187_553330
step: botster_stack_plan
---

# Expose atomic multi-record `plugin_db` mutations

## Target and context loaded

- Target repository: `trybotster/botster-hub` (`botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Assigned branch: `project-pipelines/ticket_1785711607_995393`, clean at
  `e8febabf73259cfd922592346b244ec473c17323`, matching the worktree's
  `origin/main` before planning.
- Repository charter: [[botster-hub-playbook]].
- Role and surface guidance: [[planner-playbook]],
  [[botster-planner-playbook]], [[project-pipelines-playbook]],
  [[botster-architecture]], [[cli-patterns]], and [[spa-patterns]]. The Project
  Pipelines charter is downstream test-shape guidance only; this run does not
  own Project Pipelines workflow policy.
- Hub boundary notes: [[botster hub is a first party host profile over core]],
  [[botster hub gravity must be watched before it becomes the new monolith]],
  [[botster packages should enforce core hub cli plugin provider boundaries]],
  [[plugin db grants must update admission and runtime sources together]], and
  [[botster plugin runtime data must not live in the plugin source tree]].
- Runtime and verification notes: [[botster plugin runtime uses supervisor plus
  per plugin workers]], [[plugin mcp handlers run in plugin worker vms]],
  [[worker isolated and non blocking are different dispatch guarantees]],
  [[hub event loop blocking must use spawn_blocking for IO-bound tasks]],
  [[an mpsc round trip is not a durability barrier]], [[botster plugins need
  headless real-runtime test harnesses]], [[test script required for rust tests
  not cargo test]], [[a regression test must be shown to go red with the fix
  reverted]], [[suite wide acceptance criteria make every observed test failure
  in scope]], and [[live hub proof records distinct hub and locked core binary
  provenance]].
- Workflow/artifact notes: [[plan steps need reviewable plan artifacts]],
  [[project pipelines checklist worker timeouts require artifact evidence
  fallback]], [[plan review must check open sibling tickets that own part of the
  plan scope]], and [[fixture driven acceptance smoke tests can prove first
  party package plumbing]].
- The schema-upgrade note [[plugin db schema upgrades fail on required columns
  and unique constraints]] was loaded, but the current Hub store is a
  JSON-record filesystem store rather than the older SQLite-backed `plugin.db`
  model described there. No plugin schema migration is planned.
- Repository evidence inspected: `README.md`, `docs/lua-plugin-abi.md`,
  `examples/project-pipelines/README.md`, `src/lua_runtime.rs`,
  `src/capabilities.rs`, `src/profile.rs`, `src/packages.rs`,
  `tests/hub_lua_runtime_test.rs`, `tests/hub_capability_runtime_test.rs`,
  `tests/hub_daemon_lifecycle_test.rs`, `test.sh`, and
  `.github/workflows/loaded-daemon-lifecycle.yml`.
- The consumed Core contract is locked at
  `5846fc776d31e2b6c98a8d932f50a31078743901`. Core currently supplies typed
  single-record `PluginStoreOperation`, `PluginStoreResult`, limits, and backend
  traits; Hub supplies the concrete filesystem backend and public Lua ABI.
- Baseline proof during Plan:
  `./test.sh --test hub_lua_runtime_test plugin_db_missing_get_returns_absent_record_shape_and_preserves_success_shape -- --exact --nocapture`
  passed (1 test). The wrapper also confirmed synced test-support assets.
- Pipeline context had no prior artifacts, findings, reviews, questions, or
  blocking dependencies. A same-project/same-target open-ticket search found no
  overlapping Hub sibling. Downstream ticket `ticket_1785635393_993057` already
  carries the dependency edge on this ticket.
- Revision 2 resolves Plan Review `review_1785714442_638950`: it fixes the
  whole-namespace lock span and artifact layout, adds `patch_failed`, makes the
  synchronous-Lua-only reachability explicit, requires concurrent-write proof,
  and moves crash recovery proof onto the public Lua runtime path.

## Scope

1. Add exactly one generic public Lua helper,
   `botster.capabilities.plugin_db.batch({ mutations = {...} })`, for an enabled
   package to atomically mutate multiple records in its own admitted namespace.
   The v1 mutation vocabulary is `set`, `patch`, and `delete`; reads and lists
   remain the existing helpers.
2. Require a valid `expected_revision` for every batch mutation (`0` for a new
   record). Preserve set `schema_version`, patch merge semantics, key validation,
   missing-record handling, and monotonic revisions. Reject empty batches,
   duplicate keys, read/list operations, malformed fields, and batches larger
   than the namespace key ceiling before persistence.
3. Return a typed Lua result rather than a success-shaped partial response:
   success includes `ok = true` plus ordered per-mutation written/deleted record
   metadata; operational failure includes `ok = false` plus stable error kind,
   message, mutation index, and key where applicable. At minimum preserve the
   existing Core error classifications for `invalid_request`,
   `revision_conflict`, `store_not_found`, `quota_exceeded`, `patch_failed`,
   and `backend_failed`. Existing single-record helpers and their result/error
   shapes do not change.
4. Prepare the batch through the same loaded-plugin namespace/capability check
   used by single-record Lua operations, release the shared capability-runtime
   lock, and execute filesystem work inside the already-isolated plugin worker.
   Do not reintroduce a second scheduling hop or convert the general async
   capability submit/event API into a synchronous API.
5. Acquire the `LocalPluginStoreBackend` mutex once and hold that same guard
   continuously from recovery and namespace snapshot load through candidate
   validation, staging, promotion, parent-directory synchronization, and
   transaction-artifact cleanup. No `get`, `list`, `set`, `patch`, `delete`, or
   second batch may observe or write the namespace between snapshot and the
   promotion visibility point. Enforce record count, per-record bytes, and final
   aggregate bytes against the complete candidate before staging; any failed
   mutation leaves the live snapshot untouched.
6. Commit the complete candidate with one fixed whole-namespace layout. The
   live directory remains `plugin-data/<plugin>/`, containing only ordinary
   `encode_key(key) + ".json"` record files. Staging and backup are sibling,
   non-`.json` directories beneath `plugin-data/` (for example
   `.<plugin>.batch-staging` and `.<plugin>.batch-backup`), so
   `read_records()` can never enumerate a staged record or transaction artifact.
   Synchronize staged files/directories and the `plugin-data/` parent before
   reporting success. Recover deterministic pre-commit and post-commit sibling
   directory shapes on the next store access, under the same mutex, so restart
   never exposes a mixed generation; recovery may repair on-disk state during
   `get` or `list` while preserving their existing Lua result shapes. Reuse
   ordinary filesystem primitives; add no storage dependency or plugin-visible
   filesystem handle.
7. Document the batch request/result contract, atomicity/restart boundary, CAS
   requirement, limits, and worker-local synchronous behavior in the Hub-owned
   Lua ABI documentation.
8. Prove both the backend guarantee and the actual public runtime path: inject a
   failure after at least one staged record without changing the live snapshot,
   and drive a Project Pipelines-shaped ticket-open to ticket-active plus new
   run/current-step/event mutation through an enabled package's real MCP
   handler, plugin worker, and public Lua batch helper.

## Non-scope

- No Project Pipelines transition policy, status rules, schema, namespace
  special case, MCP contract, entity publication, or package-repository change.
- No edit to `examples/project-pipelines/plugin.lua` merely to create a second
  Project Pipelines authority or a proof-only production tool. The downstream
  shape belongs in a test package fixture that crosses the real public ABI.
- No `botster-core` public enum/trait change or Core lockfile repin. Hub composes
  the existing policy-free single-record Core types inside a private concrete
  backend transaction. If implementation proves a Core public contract change
  is unavoidable, stop and ask for a separately routed Core ticket instead of
  expanding this run.
- No raw filesystem path, SQLite connection, SQL statement, transaction handle,
  callback, or arbitrary namespace exposed to Lua.
- No new package capability or broadened static grant. Batch uses the existing
  scoped `PluginDb` grant, so `src/profile.rs` and package admission tables
  should remain unchanged unless a test reveals an existing mismatch.
- No bulk retrofit of existing `set`, `patch`, or `delete` callers. The
  downstream Project Pipelines ticket consumes the new primitive after this
  prerequisite merges.
- No general capability executor, worker queue, daemon transport, Web/TUI,
  session, or Core storage refactor; no optional transaction knobs or new gem/
  crate dependency.

## Ownership boundaries and cross-repository dependencies

- Hub owns the public Lua ABI, capability admission, plugin data root, concrete
  local store policy, synchronous prepared-operation seam, and restart recovery
  implemented here.
- Core remains authoritative for policy-free single-record operation/result,
  key, limit, record, error, and plugin-worker contracts. This plan deliberately
  avoids adding a public batch variant to Core. The new batch is intentionally
  reachable only through Hub's synchronous Lua helper; it is not added to
  `CapabilityOperation::PluginStore`, the async submit/drain path, or a future
  second `PluginStoreBackend` implementation.
- The package owns when workflow records belong in one batch and when committed
  entities may be published. Hub sees only generic keys, JSON payloads,
  revisions, and mutation actions.
- Web and TUI are downstream readers of package entity frames and require no
  change for this primitive.
- There is no blocking dependency for this run. The already-registered
  downstream edge is `ticket_1785635393_993057` (target
  `tgt_a72ca1a83d504385b8648f71409119ab`) depending on this Hub ticket. This run
  must deliver a merged Hub revision and exact consumable coordinate for that
  package run.

## Assumptions and unknowns

- Assumption: `batch` with `mutations` is the smallest clear name/shape allowed
  by the ticket's “batch/transaction” wording; plugins receive one atomic call,
  not a long-lived transaction object or callback.
- Assumption: requiring CAS for every mutation is intentional. It prevents a
  workflow batch from silently overwriting a record changed after load and
  gives delete the revision protection missing from the legacy single delete.
- Assumption: duplicate keys are rejected rather than ordered within one batch.
  Project Pipelines writes each changed model/counter/event key once, and a
  one-key/one-expectation rule keeps conflict attribution deterministic.
- Assumption: limits apply to the final candidate snapshot, not transient
  request order. A delete plus create may therefore commit at the key ceiling
  while an oversized final snapshot fails before staging.
- Assumption: a successful return requires the promoted namespace and parent
  directory durability barrier to complete. A caller timeout after commit is
  still an ambiguous response and must be reconciled by an authoritative read;
  the store never reports success before commit.
- Assumption: the existing global backend mutex may serialize store filesystem
  work across plugin workers. Worker isolation says where Lua executes, not that
  persistence is nonblocking; broad per-namespace concurrency is not required
  by this ticket.
- Assumption: the backend mutex covers the complete batch from recovery/snapshot
  through durable promotion and cleanup. A concurrent single-record write
  therefore either lands before the batch snapshot and is copied into the new
  generation, or waits and lands after promotion; it is never silently reverted.
- Assumption: the fixed whole-namespace staging and backup directories are
  non-`.json` siblings of the live namespace under `plugin-data/`. No transaction
  artifact is ever placed where `read_records()` could decode it as a live row.
- Assumption: recovery is part of the next backend access, including `get` and
  `list`. Those logical reads may repair transaction artifacts on disk under the
  mutex, but their public record/list result shapes remain unchanged.
- Assumption: `plugin_db.batch` is a synchronous Lua-only Hub ABI. The existing
  asynchronous `CapabilityOperation::PluginStore` submit/event surface remains
  single-record and unchanged.
- Ask-human threshold: any need to weaken CAS, return success before the
  durability barrier, change Core public contracts, add a storage dependency,
  expose host storage handles, or waive real worker/restart/failure evidence.

## Affected surfaces and files

- `src/capabilities.rs`: private batch mutation/result types as needed; shared
  namespace admission/preparation; candidate validation; final-snapshot limits;
  staged commit/recovery; ordered typed results; focused private backend tests.
- `src/lua_runtime.rs`: parse the new `plugin_db.batch` request, invoke the
  prepared Hub operation outside the shared runtime lock, and serialize typed
  success/failure without changing legacy helper shapes.
- `tests/hub_lua_runtime_test.rs`: an enabled scoped package fixture whose MCP
  handler batch-updates ticket/run/current-step/event records through the real
  public Lua ABI; CAS/quota failure and runtime reconstruction/reload assertions.
- `tests/hub_capability_runtime_test.rs`: preserve namespace denial, limits,
  async submit/event semantics, and ordinary single-record CRUD around the
  changed backend. Add focused coverage here only where it exercises the public
  runtime rather than private commit internals.
- `docs/lua-plugin-abi.md`: authoritative public helper contract and corrected
  synchronous prepared-operation wording.
- `README.md` or `examples/project-pipelines/README.md`: update only if their
  current capability summary becomes factually incomplete; do not add workflow
  policy or duplicate the ABI reference.
- `docs/reports/...` during Implement: durable implementation and verification
  handoff. No generated DTO, client crate, manifest, or lockfile change is
  expected.

## Implementation sequence

1. Define the narrow Hub-private request/result model and factor the existing
   namespace admission/preparation so legacy single operations and the batch do
   not diverge on grants, backend, or limits.
2. Build and unit-test candidate application: validate all inputs/revisions,
   reject duplicate keys, compute ordered results, and enforce final-snapshot
   limits before touching the live namespace.
3. Add the fixed sibling-directory namespace commit and restart recovery state
   machine while holding one backend mutex guard for the entire operation. Use
   a private deterministic failure hook in unit tests only; inject failure after
   staged progress and at the promotion boundary, then assert byte-for-byte
   unchanged old state or complete new state—never a mixture or a live `.json`
   transaction artifact.
4. Wire `plugin_db.batch` into the Lua table and typed public response. Confirm
   the shared runtime mutex is dropped before disk I/O and that the plugin
   worker remains the execution boundary.
5. Add the Project Pipelines-shaped package fixture and drive its MCP handler
   through `HubRuntime::call_plugin_mcp_tool`. Assert ticket/run/step/event
   revisions and payloads change together, conflict/quota failure changes none,
   success survives reconstructing the runtime from the same data directory,
   and another namespace cannot be selected.
6. Update ABI docs, run focused and full repository gates, capture negative
   controls, and hand the exact merged Hub/Core provenance to the dependent
   package ticket.

## Risks

- A loop of ordinary record writes is serialized but not atomic; the commit
  must have one visibility point rather than rely on rollback after partial live
  writes.
- A crash between directory renames can leave staging/backup artifacts. Recovery
  rules need exhaustive pre/post-commit tests, including initially empty
  namespaces, or restart can select the wrong generation.
- Releasing the backend mutex between snapshot and whole-namespace promotion
  can silently erase an unrelated single-record write. Hold one guard through
  the durability barrier and test both before-snapshot and blocked-during-stage
  interleavings.
- A staged `.json` file or `.json` directory inside the live namespace can
  collide nondeterministically by internal key or poison every store read.
  Transaction artifacts must remain non-`.json` siblings outside the directory
  enumerated by `read_records()`.
- Returning a runtime string error would discard the conflict/quota/patch/
  backend distinction required by the ticket. The new batch result must keep
  stable machine-readable kinds and failing-mutation context.
- Enforcing limits mutation-by-mutation can reject a valid delete-plus-create or
  admit an oversized final combination. Validate one final candidate snapshot.
- Holding `SharedHubCapabilityRuntime` during staging/fsync would couple file I/O
  to unrelated capability progress. Clone the prepared backend/limits under the
  lock and execute only after release.
- Treating worker isolation as nonblocking could hide MCP response latency. Keep
  the operation bounded by existing 4 MiB/1,024-key limits and record measured
  runtime behavior; do not claim fire-and-forget semantics.
- A public Core batch addition would broaden this ticket across repository
  ownership and downstream enum compatibility. Stop and route it separately if
  the Hub-private composition proves insufficient.
- Tests that only provoke CAS or quota validation before staging do not prove
  rollback-free atomic commit. The deterministic mid-stage negative control is
  mandatory and must fail when the transaction implementation is replaced by
  sequential live writes.
- A fixture that calls private Rust helpers proves code presence, not the
  product path. The ticket-shaped proof must originate in Lua inside an enabled
  package worker and read the committed state back through public helpers after
  restart.
- Recovery on next access means existing `get` and `list` calls can repair disk
  state. Public-path recovery tests must prove this side effect cannot alter
  their result shape or expose staged/mixed records.

## Acceptance checks and tests

### Focused behavior

- `./test.sh --lib <new private-backend batch test filter>` proves set/patch/
  delete CAS, duplicate/invalid/missing handling, final-snapshot limits, ordered
  typed results including `patch_failed`, deterministic mid-stage failure,
  commit-boundary failure, and restart recovery shapes.
- Private concurrency tests prove both legal interleavings with an unrelated
  single-record `set`: a write completed before snapshot remains in the promoted
  generation, while a write attempted during staged progress blocks until
  promotion and then commits with its own revision. A mid-stage `get`/`list`
  likewise cannot complete through partial state; after release it returns the
  complete promoted generation. No transaction artifact uses a live `.json`
  filename, and recovery leaves none behind in the namespace directory.
- `./test.sh --test hub_lua_runtime_test <new atomic batch public ABI test> -- --exact --nocapture`
  proves the enabled package/MCP/plugin-worker/Lua/Hub-backend path.
- In that public test, seed an open ticket and counters, atomically create a run,
  current run-step, and event while changing the ticket to active, and assert
  every returned revision and persisted payload. Recreate the runtime over the
  same explicit data directory and assert all records remain complete and
  readable.
- Through the same public helper, inject a stale expected revision, malformed
  merge patch, and over-limit final candidate. Assert stable typed
  `revision_conflict`, `patch_failed`, and `quota_exceeded` results, the failing
  index/key, and no changed ticket, run, step, event, counter, or unrelated
  record.
- Public-path crash recovery seeds each recoverable on-disk shape before boot:
  live plus staging (discard pre-commit staging), backup plus staging with no
  live namespace (restore the old generation), live plus backup (retain the
  promoted generation), and staging-only for an initially empty namespace
  (remain empty). For each case, start the real Hub runtime, invoke Lua
  `plugin_db.get` and `list`, assert exactly the old or new generation—never a
  mixture—verify transaction artifacts are repaired, then commit a subsequent
  public Lua batch successfully.
- Preserve existing single-operation behavior:
  `./test.sh --test hub_lua_runtime_test plugin_db_missing_get_returns_absent_record_shape_and_preserves_success_shape -- --exact --nocapture`
  and
  `./test.sh --test hub_lua_runtime_test plugin_db_missing_patch_and_delete_still_raise_runtime_errors -- --exact --nocapture`.
- Preserve capability admission/general async behavior:
  `./test.sh --test hub_capability_runtime_test hub_runtime_stores_plugin_json_under_plugin_data_and_enforces_namespace -- --exact --nocapture`
  and
  `./test.sh --test hub_capability_runtime_test hub_runtime_admits_botster_workspaces_plugin_store_namespace_only -- --exact --nocapture`.

### Regression and runtime proof

- Run the decisive backend failure test against a temporary sequential-live-
  write reversion and preserve its partial-state failure; restore the fix and
  preserve the pass. CAS/quota preflight alone is not an acceptable negative
  control.
- Run the complete `hub_lua_runtime_test` and `hub_capability_runtime_test`
  targets at default concurrency through `./test.sh`.
- Prove the shared capability mutex is released before batch filesystem work,
  the batch then holds its separate backend mutex continuously from recovery/
  snapshot through durable promotion, the call executes in the loaded plugin
  worker, legacy async plugin-store submission still publishes completion
  events, the batch is documented as synchronous-Lua-only, and no namespace
  string is matched specially in batch code.
- If timing or worker resource behavior changes under default concurrency, use
  the existing fixed-SHA `focused-lua-worker-suite` loaded workflow without
  reduced repetitions, altered stress, or retries. Any observed failure in the
  promised run is blocking until exactly attributed or human-rescoped.

### Repository gates and downstream handoff

- `cargo fmt --all -- --check`.
- `git diff --check`.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- `./test.sh` for the full repository wrapper and asset drift check.
- Build the exact runtime pair from a fresh final Hub revision:
  `cargo build --locked --bin botster-hub -p botster-hub` and
  `cargo build --locked -p botster-core --bin botster-session-worker`.
  Record Hub SHA, locked Core SHA, and both binary realpaths under the fresh
  target directory.
- Downstream proof required by the Hub charter and ticket: attach the public Lua
  ABI ticket/run lifecycle result and restart readback, not only source or a
  private Rust test. The dependent package run must later pin the merged Hub
  revision and replace sequential lifecycle writes with this generic batch;
  that consumption is owned by `ticket_1785635393_993057`, not this branch.

## Pipeline gates and artifacts

- Plan artifact: this document, attached to
  `run_1785713187_553330` / `botster_stack_plan`.
- Workflow checklist: `Atomic plugin_db planning discipline`.
- Vault checklist: `Plan vault discipline`. Its create call returned the known
  post-commit worker-timeout shape; listing reconciled the committed checklist
  instead of retrying.
- Implement handoff must state the exact batch ABI, atomic visibility point,
  recovery states, failure hook/negative control, files changed, test commands,
  Hub/Core provenance, and any deviation from this plan.
- Review must reject sequential live writes, rollback-only atomicity, untyped
  Lua errors, held capability locks during I/O, Project Pipelines special cases,
  Core/public-client expansion, missing restart proof, or a green-only failure
  test.
- Verify must rerun the public worker path and failure/restart evidence against
  the review commit, then provide the exact consumable merged coordinate to the
  downstream ticket.

## Vault gaps worth capturing

- After implementation proof, capture “file-backed multi-record stores need one
  generation visibility point plus deterministic restart recovery” if the
  staging/swap state machine proves reusable beyond this ticket.
- After the Lua result contract is consumed, capture “atomic plugin mutations
  return typed failure context instead of collapsing conflicts into runtime
  strings” if this becomes the durable ABI convention.
- No vault note is written during Plan. The older SQLite `plugin.db` schema note
  does not describe this Hub JSON-record backend; that naming/backend drift may
  merit a clarification note only after implementation confirms the new
  transaction boundary.
