---
ticket: "Tests: diagnose loaded Lua plugin_db operation timeout"
run: run_1784603955_575504
step: botster_implement
pull_request: 152
---

# Loaded Lua `plugin_db` timeout implementation report

## Outcome

The synchronous Lua `plugin_db` path no longer submits an asynchronous
capability operation, polls the plugin event queue, sleeps, or depends on a
second per-operation thread being scheduled. It prepares the operation while
holding the shared capability-runtime lock, releases that lock, and executes
the prepared backend operation in the owning plugin worker. The general
capability API remains asynchronous and continues to return its resource handle
and publish a completion event from `botster-hub-plugin-store-capability`.

## Assumptions

- The preserved red run `29790592442` and source path establish the extra
  thread/completion hop as the load-sensitive seam, but do not distinguish
  worker-not-started, worker-not-completed, or completion-event loss. This
  report does not claim a narrower mechanism.
- The synchronous path must not apply async queue capacity/backpressure or
  register an in-flight resource: it enqueues no operation and returns only
  after inline completion. The async path retains both behaviors unchanged.
- Existing production-entry tests already cover present records, absent `get`,
  missing patch/delete, and read-after-write semantics, while capability tests
  cover namespace denial and the submit/event contract. No new test-only hook
  or product configuration is needed.
- `docs/lua-plugin-abi.md` requires no change because its synchronous result and
  read-after-write contract is unchanged.

## Files changed

- `src/capabilities.rs`: added the crate-private prepared plugin-store
  operation, shared admission/validation seam, and reused it from the existing
  asynchronous submit worker.
- `src/lua_runtime.rs`: executes prepared plugin-store operations directly and
  preserves the prior Lua success/missing/error shapes; removed the Lua-only
  operation sequence, timeout, event-drain polling, and sleep loop.
- `docs/plans/diagnose-loaded-lua-plugin-db-operation-timeout.md`: committed the
  approved plan artifact so Review sees the exact scope and acceptance
  contract.
- `docs/reports/diagnose-loaded-lua-plugin-db-operation-timeout-implement-report.md`:
  this durable implementation handoff.

No test or Lua ABI documentation file changed because the existing tests
already exercise the required production and lower-level contracts and the
public behavior is unchanged.

## Playbook constraints applied

- Used `[[implementer-playbook]]` and `[[botster-implementer-playbook]]` as the
  implementation boundary, plus the Botster architecture/worker and repository
  test-harness notes named in checklist `checklist_1784604823_613542`.
- Kept plugin-owned synchronous work in the existing per-plugin worker and kept
  the hub capability runtime responsible for admission and backend mechanics.
- Used the repository `./test.sh` wrapper for every Rust test command.
- Preserved the existing async API, capacity/backpressure, resource handle,
  namespace grant, validation, limits, typed results, and error shapes.
- Made no timeout increase, retry, reduced-load acceptance, global
  serialization, public configurability, dependency change, or adjacent
  capability refactor.
- Committed the reviewed plan and implementation, pushed the ticket branch,
  opened draft PR #152, and will attach this report as a pipeline artifact
  before requesting Review.

## Deterministic runtime evidence

- `src/lua_runtime.rs::execute_plugin_store_for_lua` obtains a prepared value in
  a lexical lock scope and calls `prepared.execute()` only after that guard is
  dropped. Backend filesystem I/O therefore does not hold the shared runtime
  mutex.
- The Lua path contains no `drain_events`, one-millisecond sleep loop, timeout
  string, or `botster-hub-plugin-store-capability` spawn. It also no longer
  removes unrelated same-plugin capability events while awaiting a result.
- `src/capabilities.rs::submit_plugin_store` still calls
  `ensure_runtime_capacity`, creates its `resource_ref`, starts the named
  per-operation worker, returns `CapabilityRuntimeHandle { resource: Some(..) }`,
  and sends the typed completion. Only preparation/execution policy is shared.
- Fixed-SHA run `29790592442` is the negative control: subject
  `c3b104df5769d63c24c5ec2f1ef9ed6d6cd4dd8e` retained the old submit/drain/sleep
  path and stopped first-red at repetition 8 with the exact Lua `plugin_db`
  timeout. It is not being replaced by rerun-until-green evidence.

## Tests run

- `cargo fmt --check` — passed.
- `git diff --check` — passed.
- `./test.sh --test hub_lua_runtime_test plugin_db_missing_get_returns_absent_record_shape_and_preserves_success_shape -- --exact --nocapture`
  — passed, 1/1.
- `./test.sh --test hub_lua_runtime_test plugin_db_missing_patch_and_delete_still_raise_runtime_errors -- --exact --nocapture`
  — passed, 1/1. An initial implementation run correctly caught an error-string
  drift; the mapping was repaired to preserve `CapabilityRuntimeError.message`
  and the exact test then passed.
- `./test.sh --test hub_capability_runtime_test hub_runtime_stores_plugin_json_under_plugin_data_and_enforces_namespace -- --exact --nocapture`
  — passed, 1/1.
- `./test.sh --test hub_capability_runtime_test hub_runtime_admits_botster_workspaces_plugin_store_namespace_only -- --exact --nocapture`
  — passed, 1/1.
- `./test.sh --test hub_lua_runtime_test` — passed, 18/18 at default
  concurrency.
- `./test.sh --test hub_capability_runtime_test` — passed, 14/14 at default
  concurrency.
- `./test.sh --test hub_lua_runtime_test --no-run` — passed before starting the
  loaded campaign.
- Binding loaded campaign [run `29799095973`](https://github.com/trybotster/botster-hub/actions/runs/29799095973)
  — passed all 20/20 repetitions against immutable subject
  `7543907c12e97b6d0111310d86c4c57ab8872351`, using harness SHA
  `b6835b981e7887e9bbd68e75dfdbe0f2bca18a81`, `focused-lua-worker-suite`,
  `residual-tail`, the unchanged `./test.sh --test hub_lua_runtime_test --
  --nocapture` command, default test concurrency, 48 workers on four CPUs, and
  900-second per-run/19,800-second campaign bounds. The artifact contains 181
  resource samples with 1-minute mean/max load 77.16/132.94 and 5-minute
  mean/max load 55.30/91.73. Every run log reports 18 passed/0 failed;
  `exit_status=0`, `cleanup_status=0`, all owned test/load/sampler groups are
  recorded gone, and `active-pgids.tsv` is empty.

## Deviations from plan

None. No new test code was necessary because all four named regressions already
existed and proved the affected production and async paths. The implementation
used the plan's preferred private prepared-operation struct shape.

## Unverified behavior or residual risk

- Although the campaign passes, the preserved failure cannot identify whether
  the old child worker was unscheduled, unfinished, or completed into an event
  that Lua never observed. The repair removes all three dependencies from the
  synchronous path without claiming which narrower mechanism occurred.
- Other one-thread-per-operation capability families were not changed or load
  tested; no failure evidence brought them into scope.

## Missing vault guidance discovered

None required to implement this ticket. If the 20-run campaign verifies the
repair, the candidate durable rule is that synchronous helpers already running
inside a plugin worker should execute prepared host operations without a second
scheduling hop. Capture should wait for Verify rather than generalizing from
structural evidence alone.
