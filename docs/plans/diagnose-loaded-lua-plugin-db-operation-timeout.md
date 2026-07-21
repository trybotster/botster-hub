---
ticket: "Tests: diagnose loaded Lua plugin_db operation timeout"
run: run_1784603955_575504
step: botster_plan
---

# Diagnose loaded Lua `plugin_db` operation timeout

## Context loaded

- Project Pipelines context: ticket `ticket_1784595368_465499`, run `run_1784603955_575504`, active step `botster_plan`, gate `botster_plan_gate`; there were no prior artifacts, reviews, findings, questions, answers, dependencies, or blocking dependencies at plan time.
- Required planning context: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[botster pipeline needs continuous product owner between agent steps]], [[plan agents must author vault context as wikilinks not home paths]], and [[vault example paths are not repository placement conventions]].
- Ticket-specific vault constraints: [[botster plugin runtime uses supervisor plus per plugin workers]], [[plugin workers use typed mailbox handler refs not lua closures]], [[plugin mcp handlers run in plugin worker vms]], [[hub event loop blocking must use spawn_blocking for IO-bound tasks]], [[test script required for rust tests not cargo test]], [[loaded lifecycle ci precompiles the exact test target before synthetic cpu stress]], [[suite wide acceptance criteria make every observed test failure in scope]], [[an mpsc round trip is not a durability barrier]], and [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Repo placement evidence: this repository keeps current plan artifacts under `docs/plans/`; it has no `docs/plans/README.md` retiring that directory, and the existing sibling plan `docs/plans/plugin-db-get-missing-records-return-absence.md` is the direct prior artifact for the failing behavior.
- Repo/runtime evidence inspected:
  - `src/lua_runtime.rs` exposes synchronous Lua `plugin_db.get/set/patch/delete/list` helpers. `submit_plugin_store_and_wait` submits to `HubCapabilityRuntime`, then performs 1,000 drain-and-sleep iterations before returning `plugin_db operation did not complete before timeout`.
  - `src/capabilities.rs` validates each plugin-store request, starts a new `botster-hub-plugin-store-capability` OS thread for that operation, and publishes its result through the shared completion channel. General capability callers consume that deliberately non-blocking submit/event contract.
  - `src/runtime.rs` invokes the loaded MCP handler through the real plugin worker path, so `tests/hub_lua_runtime_test.rs::plugin_db_missing_get_returns_absent_record_shape_and_preserves_success_shape` is production-entry-path coverage, not a helper-only test.
  - `docs/lua-plugin-abi.md` promises synchronous read-after-write semantics to Lua while binding namespace selection to the loaded plugin key.
  - Fixed-SHA GitHub Actions run `29790592442` used Hub commit `c3b104df5769d63c24c5ec2f1ef9ed6d6cd4dd8e`, exact target `hub_lua_runtime_test`, 20 planned repetitions, `residual-tail`, 48 CPU workers on 4 CPUs, and stopped first-red on repetition 8. The failure occurred on the first missing `get`; missing patch/delete in a separate runtime passed, both session-template callers passed, and cleanup completed with no active process groups. The preserved artifact is `loaded-daemon-lifecycle-29790592442-1`.
- Project Pipelines workflow evidence is tracked in checklists `checklist_1784604060_903711` and `checklist_1784604076_807275`. Both create calls returned the known post-commit worker timeout shape; listing confirmed both checklists persisted, so no blind retry was used.

## Scope

- Remove the load-sensitive secondary scheduling hop from the synchronous Lua `plugin_db` path while retaining plugin namespace/capability admission, request validation, limits, typed results, and existing missing-record semantics.
- Extract one internal prepared plugin-store operation from `HubCapabilityRuntime`: validate and clone the backend/limits while holding the shared runtime lock, release that lock, then execute the prepared operation in the already-isolated plugin worker that called the synchronous Lua helper.
- Reuse that same preparation/execution seam from the existing asynchronous `HubCapabilityRuntime::submit_plugin_store` path so admission and execution policy do not fork. General capability callers must keep the current non-blocking submit-plus-event contract.
- Replace the Lua helper's submit/drain/sleep loop with direct prepared-operation execution and Lua result/error conversion. This removes the new-thread startup dependency without increasing any timeout or adding retry behavior.
- Preserve the production entry path: `HubRuntime::call_plugin_mcp_tool` -> core plugin worker -> loaded Lua MCP handler -> `botster.capabilities.plugin_db.*` -> hub-owned plugin-store backend.
- Add regression coverage and verification evidence that distinguish the repaired scheduling boundary from a merely green rerun.

### Implementation sequence

1. In `src/capabilities.rs`, factor the existing namespace grant check, operation validation, backend clone, limits capture, and `execute_plugin_store` call into a crate-private prepared-operation seam. Keep `submit_plugin_store` asynchronous by moving the prepared operation into its current completion-producing thread.
2. In `src/lua_runtime.rs`, prepare under the short shared-runtime lock, release the lock before file I/O, execute in the plugin worker, and convert `PluginStoreResult` or `CapabilityRuntimeError` directly to Lua. Preserve missing `get` as `{ kind = "record" }`/`record == nil`; preserve missing patch/delete as runtime errors. Remove only the Lua-only operation sequence, polling loop, sleep, and timeout constant made dead by this change.
3. Strengthen focused tests so the production Lua path proves missing and successful reads plus missing patch/delete behavior, while lower-level capability tests prove the ordinary asynchronous submit/event API remains intact. Record a red-when-reverted result for the causal seam or, if a narrowly scoped deterministic test hook is unavoidable, keep it crate-private/test-only and document why it is required; do not add product configurability.
4. Run the exact local targets, then run the existing fixed-SHA loaded Lua-worker campaign against the implementation commit. Preserve the first red if one occurs; do not retry it away. Only all 20 planned repetitions under the unchanged profile is loaded acceptance for this sibling.

## Non-scope

- No timeout inflation, retry, rerun-until-green policy, reduced stress, altered test concurrency, `--test-threads=1` acceptance, or serialization of the loaded test suite.
- No global serialization of plugin-store operations. The shared runtime lock must be released before backend I/O, and different plugin workers must remain able to execute their prepared operations independently.
- No change to the general non-blocking capability runtime contract for filesystem, HTTP, WebSocket, timers, or ordinary plugin-store submit/event callers.
- No `botster-core` DTO or dependency change unless implementation proves the hub-local prepared-operation seam cannot preserve the contract; that would require a new human decision before expanding scope.
- No change to plugin DB schema, on-disk format, namespace grants, missing-record shape, Project Pipelines product workflow, session-template control sockets, or unrelated lifecycle failures.
- No duplicate loaded-workflow selector or broad harness refactor. Reuse the existing fixed-SHA Lua-worker diagnostic harness from the parent investigation; this sibling supplies the repaired subject SHA.
- No adjacent cleanup of other sleep/poll loops or one-thread-per-request capability families.

## Assumptions and unknowns

- Assumption: the ticket asks for a production runtime repair plus deterministic causal proof, not only added diagnostics. The statement that this sibling must land before the parent's pinned campaign makes a code-only diagnostic artifact insufficient.
- Assumption: synchronous `plugin_db` execution inside the owning plugin worker is architecture-compatible because plugin handlers already run in isolated per-plugin workers and the Lua ABI is synchronous. The hub/client/session event loop must not perform this I/O or wait while holding the shared capability-runtime mutex.
- Assumption: the captured timeout is caused by the extra per-operation worker/completion hop failing to make progress within the Lua helper's fixed polling budget. This is a source-and-artifact inference: submission succeeded, the timeout is emitted only after no matching completion was observed, the failing call was the first operation in a fresh runtime, and sibling DB/session-template paths passed. Implementation must preserve evidence that can distinguish worker-not-started/worker-not-completed from an event-routing loss before declaring the diagnosis final.
- Assumption: the existing `PluginStoreBackend` and `execute_plugin_store` implementation remain authoritative for storage semantics; the new seam should compose them rather than invent another database implementation.
- Worktree/target assumption: downstream agents use target `tgt_7e208a0c76a44980a83b63af976b1f22` and only this assigned worktree/branch. Committed artifacts use path-neutral repo paths and vault note titles, not local home-directory paths.
- Unknown: whether the most surgical prepared-operation type belongs as a small private struct or a private method/closure pair in `src/capabilities.rs`. Choose the shape that releases the mutex before I/O and lets both synchronous Lua and asynchronous general callers share validation and execution.
- Unknown: whether a deterministic negative-control test can be expressed through the factored seam without any hook. Prefer a direct unit test around prepare/execute plus production-path integration evidence. If scheduling suppression is necessary to prove red-when-reverted, it must be test-only and narrowly scoped; do not expose timeout or executor knobs to users.
- No human question is required at plan time because the ticket exclusions and failing runtime path are specific. Ask before implementation only if the fix would require changing `botster-core`, weakening the non-blocking capability contract, changing the loaded workload, or accepting fewer than 20 clean repetitions.

## Product decision ledger

- Binding default: preserve current Lua result/error shapes and the 1,000 ms plugin invocation budget; eliminate the unnecessary nested scheduling dependency instead of relaxing the budget.
- Binding non-goals: no timeout increase, retries, stress reduction, suite serialization, global plugin DB serialization, or unrelated capability refactor.
- Follow-up acceptable: broader capability worker-pool/resource-observability work may become a separate ticket if evidence shows filesystem or other operation families share the one-thread-per-request weakness; it must not be bundled without a reproduced failure.
- Ask-human threshold: any need for a `botster-core` contract change, public configuration, workload waiver, or acceptance campaign change.

## Affected surfaces/files

- Botster layers touched: Rust hub capability runtime, loaded Lua plugin runtime inside core-managed plugin workers, Rust integration/unit tests, and the Lua ABI documentation only if implementation wording changes.
- `src/capabilities.rs`: shared prepare/execute seam; existing async plugin-store submit continues to produce typed completion events.
- `src/lua_runtime.rs`: synchronous Lua `plugin_db` execution and result conversion; removal of the nested submit/poll/sleep timeout path.
- `tests/hub_lua_runtime_test.rs`: decisive production-entry-path regression for missing get, successful set/get, and missing patch/delete.
- `tests/hub_capability_runtime_test.rs`: regression that general plugin-store submission remains non-blocking/event-driven and preserves namespace/grant behavior.
- `docs/lua-plugin-abi.md`: update only if necessary to describe direct worker-local completion accurately; user-visible shapes and synchronous read-after-write semantics stay unchanged.
- `docs/plans/diagnose-loaded-lua-plugin-db-operation-timeout.md`: this reviewed plan artifact.
- Existing external verification surface, not expected code changes: the fixed-SHA loaded Lua-worker workflow/harness and artifact bundle defined by the parent diagnostic branch.

## Risks

- Lock-scope risk: directly executing while holding `SharedHubCapabilityRuntime` would make plugin file I/O block unrelated capability progress and could violate the hub event-loop convention. Preparation must clone what execution needs and release the mutex first.
- Contract drift risk: bypassing `submit_plugin_store` can accidentally bypass namespace grants, validation, limits, or typed error mapping. One shared preparation seam must serve both call paths.
- Async regression risk: changing `submit_plugin_store` itself to synchronous execution would violate the general non-blocking capability API even if Lua tests pass. Lower-level event-driven coverage must remain.
- Concurrency risk: a single global DB worker would cure thread startup by serializing all plugins and contradict the ticket. Prepared operations should execute in their existing owning plugin workers without a global execution queue.
- False diagnosis risk: the artifact proves an unobserved completion, not by itself whether the child thread never started, did not finish, or its completion was lost. The implementation report must provide deterministic seam/negative-control evidence before naming one narrower mechanism.
- False-green risk: a local focused test or one loaded pass does not satisfy the ticket. The first-red 20-run campaign must be repeated unchanged and must complete all 20 repetitions.
- Event-loss risk: `drain_events` removes the whole plugin event queue. Removing Lua's event-drain loop also avoids this helper consuming unrelated same-plugin capability events; tests should ensure ordinary async callers still own their events.
- Cleanup risk: a red campaign must still upload diagnostics and show `cleanup_status=0` with no active owned process groups.
- Scope-creep risk: filesystem capability submission uses a similar child-thread pattern, but no failure for it is established here. Capture/re-ticket only if evidence reproduces it.

## Acceptance checks/tests

- Static/format checks:
  - `cargo fmt --check`
  - `git diff --check`
- Focused behavior and production entry path, through the required wrapper:
  - `./test.sh --test hub_lua_runtime_test plugin_db_missing_get_returns_absent_record_shape_and_preserves_success_shape -- --exact --nocapture`
  - `./test.sh --test hub_lua_runtime_test plugin_db_missing_patch_and_delete_still_raise_runtime_errors -- --exact --nocapture`
  - `./test.sh --test hub_lua_runtime_test` at default libtest concurrency.
- General capability contract regression:
  - `./test.sh --test hub_capability_runtime_test hub_runtime_stores_plugin_json_under_plugin_data_and_enforces_namespace -- --exact --nocapture`
  - `./test.sh --test hub_capability_runtime_test hub_runtime_admits_botster_workspaces_plugin_store_namespace_only -- --exact --nocapture`
  - Run the full `hub_capability_runtime_test` target if shared preparation changes more than plugin-store-local code.
- Deterministic proof required in the implementation report:
  - Evidence that the production Lua call no longer spawns and polls a secondary per-operation worker, while ordinary capability submit still does.
  - Evidence that the shared runtime mutex is released before plugin-store backend I/O.
  - A regression/negative-control result that goes red with the old Lua submit/drain/sleep path restored; if only the loaded campaign can provide the negative control, cite preserved run `29790592442` as red and the repaired fixed-SHA campaign as green without substituting reruns.
  - Evidence that present records, absent `get`, missing patch/delete, namespace denial, and read-after-write semantics remain unchanged.
- Binding loaded acceptance:
  - Precompile the exact `hub_lua_runtime_test` target with `./test.sh --test hub_lua_runtime_test --no-run` before starting stress.
  - Use the existing fixed-SHA `focused-lua-worker-suite`, 20 repetitions, `residual-tail`, default test concurrency, 48 workers on a 4-CPU runner when that is the resolved profile, unchanged inner timeouts, and first-red stop behavior.
  - Accept only if all 20 repetitions pass. Preserve the workflow URL/run id, subject SHA, harness SHA/ref, command, observed load, per-run status, and cleanup evidence. Any new red is blocking and must not be rerun away.
- Parent handoff:
  - After this sibling lands, refresh the parent branch and rerun its pinned loaded campaign. The sibling's focused campaign does not by itself resolve the parent control-socket ticket.

## Pipeline gates and artifacts

- Plan artifact: this file, attached to run `run_1784603955_575504`.
- Checklist artifacts: vault discipline `checklist_1784604060_903711`; plan workflow discipline `checklist_1784604076_807275`.
- Implement gate: attach a report naming the exact runtime seam, every changed file, deterministic diagnosis/negative control, focused commands, and any docs disposition.
- Review gate: inspect for held mutexes during I/O, duplicated grant/validation logic, synchronous regression in the general capability API, global serialization, timeout inflation, retries, dead polling code, and tests that bypass `HubRuntime`/the plugin worker.
- Verify gate: run the focused/default-concurrency targets and the unchanged 20-run loaded campaign; attach the complete fixed-SHA artifact and exact cleanup evidence.
- Advancement rule: no waiver, retry, workload reduction, or claim of resolution from code existence alone. A human must explicitly re-scope any unmet loaded acceptance criterion.

## Vault gaps worth capturing

- Capture candidate after implementation proof: "synchronous plugin worker helpers should execute prepared host operations without a second scheduling hop" — a general boundary pattern only if the fix demonstrates it beyond this one helper.
- Capture candidate if reproduced elsewhere: "one OS thread per capability operation is starvation-sensitive under loaded plugin workloads" — do not generalize from this single `plugin_db` observation without filesystem/other-family evidence.
- Capture candidate if event ownership matters to the fix: "synchronous capability helpers must not drain unrelated same-plugin events" — current whole-plugin queue removal is a latent ownership hazard, but it is not yet proven as this timeout's cause.
- No durable note should be written during Plan. Route any verified new knowledge through the vault inbox/document/connect/verify workflow after implementation or verification establishes the mechanism.
