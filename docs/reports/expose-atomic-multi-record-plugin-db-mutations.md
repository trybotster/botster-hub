---
ticket: ticket_1785711607_995393
run: run_1785713187_553330
step: botster_stack_implement
target_id: tgt_7e208a0c76a44980a83b63af976b1f22
---

# Atomic multi-record `plugin_db` implementation report

## Target and guidance

- Target repository: `trybotster/botster-hub`.
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Repository charter: [[botster-hub-playbook]].
- Role playbooks: [[implementer-playbook]] and
  [[botster-implementer-playbook]].
- Architecture and runtime guidance: [[botster-architecture]],
  [[cli-patterns]], [[spa-patterns]],
  [[botster core lua owns plugin framework primitives not product policy]],
  [[botster plugin runtime uses supervisor plus per plugin workers]],
  [[plugin mcp handlers run in plugin worker vms]],
  [[worker isolated and non blocking are different dispatch guarantees]],
  [[hub event loop blocking must use spawn_blocking for IO-bound tasks]],
  [[an mpsc round trip is not a durability barrier]],
  [[plugin db grants must update admission and runtime sources together]], and
  [[botster plugin runtime data must not live in the plugin source tree]].
- Verification and handoff guidance: [[botster plugins need headless
  real-runtime test harnesses]], [[test script required for rust tests not cargo
  test]], [[a regression test must be shown to go red with the fix reverted]],
  [[suite wide acceptance criteria make every observed test failure in scope]],
  [[live hub proof records distinct hub and locked core binary provenance]],
  [[fixture driven acceptance smoke tests can prove first party package
  plumbing]], [[implementation artifacts must match actual git state]],
  [[implement gate must verify committed work and pr link before review]],
  [[implementation steps must persist report artifacts for review]], and
  [[pipeline vault checklists must cite exact resolvable note titles]].
- [[project-pipelines-playbook]] was not an implementation overlay: no Project
  Pipelines package path or workflow policy changed. The test fixture uses only
  downstream-shaped generic records through the Hub-owned ABI.

## Files changed

- `src/capabilities.rs` — adds the Hub-private batch model, final-candidate CAS/
  patch/quota validation, one-mutex whole-namespace staging/promotion/recovery,
  typed results, hard-link reuse for unchanged records, deterministic failure
  hooks, concurrency proof, and recovery tests. Per-record quota failures carry
  their exact mutation index/key; namespace-aggregate quota failures do not
  falsely attribute a mutation.
- `src/lua_runtime.rs` — exposes `plugin_db.batch`, validates its strict Lua
  request shape, preserves failing mutation index/key, prepares under the shared
  capability runtime lock, and executes after releasing that lock.
- `tests/hub_lua_runtime_test.rs` — adds an enabled Project Pipelines-shaped
  package fixture and proves the MCP/plugin-worker/public-Lua path, typed
  no-change failures (including a late conflict and oversized-first batch),
  malformed missing-key attribution, live capability denial, runtime
  reconstruction, and all recoverable disk shapes.
- `docs/lua-plugin-abi.md` — documents the public request/result contract,
  synchronous worker-local reachability, limits, atomicity, durability, and
  read-path recovery.
- `docs/reports/expose-atomic-multi-record-plugin-db-mutations.md` — this durable
  implementation handoff.

## Ownership and routing

Hub ownership is preserved: all production changes are in the Hub-authored Lua
ABI and Hub-private concrete filesystem backend. The locked Core public
`PluginStoreOperation`, `PluginStoreBackend`, async submit/event surface, and
dependency coordinate are unchanged. No package policy, Project Pipelines
transition rule, client DTO, Web/TUI behavior, raw filesystem handle, SQL API,
new capability grant, or storage dependency was added.

The separately routed downstream ticket remains
`ticket_1785635393_993057` on target
`tgt_a72ca1a83d504385b8648f71409119ab`; it depends on this ticket and will
consume the merged Hub binary's generic Lua ABI. No cross-repository file was
edited in this run.

## Implementation behavior

- Every mutation requires CAS. Set/patch revisions remain monotonic and delete
  verifies its expected revision.
- Empty batches, duplicate keys, unsupported operations, malformed/unknown
  fields, invalid keys, missing records, malformed patches, and final-snapshot
  quota violations return stable typed failures. A mutation-specific failure
  includes a 1-based index and includes a key when the request supplied one;
  whole-request and namespace-aggregate failures omit both.
  Final-snapshot key limits allow a delete+create replacement at the ceiling.
- The final namespace candidate is complete before persistence begins. The
  backend mutex remains held from recovery/snapshot through validation,
  staging, promotion, parent sync, and artifact cleanup.
- Live records remain ordinary encoded `.json` files under the namespace.
  Staging and backup are non-JSON sibling directories under `plugin-data/`.
  Unchanged records are hard-linked into staging, so only mutated records are
  JSON-encoded and file-synced; staging still performs a bounded namespace walk
  and link operation per surviving record. If a filesystem cannot create a
  hard link, that record falls back to the ordinary encode/write/sync path.
- Recovery deterministically discards pre-commit staging, restores a backed-up
  old generation when live is absent, retains a promoted live generation, and
  leaves an initially empty namespace empty. Existing `get` and `list` may
  perform that repair without changing their Lua result shapes.
- `batch` is intentionally synchronous-Lua-only and executes inside the
  isolated plugin worker. The general async Core operation family remains
  single-record.

## Deviations from plan

None. The implementation follows approved plan revision 2. Review corrections
clarified typed quota attribution and capability-denial documentation, removed
the test-only sequential production branch, and reduced staging write cost
without changing the approved contract. `README.md`,
`examples/project-pipelines/README.md`, `src/profile.rs`, and
`tests/hub_capability_runtime_test.rs` did not need edits because the existing
summary, capability grant, and async-runtime contract remain accurate; their
relevant existing tests were rerun instead.

## Verification and downstream proof

- `./test.sh --lib plugin_store_batch -- --nocapture` — 6 passed, including
  exact per-record and aggregate quota attribution, legal replacement at the
  key ceiling, failure injection, concurrency, hard-link reuse and fallback,
  and recovery.
- `./test.sh --test hub_lua_runtime_test plugin_db_batch_atomically_commits_project_pipeline_lifecycle_and_returns_typed_failures -- --exact --nocapture` — 1 passed; the complete enabled-package MCP → plugin worker → Lua → Hub store path completed in 0.23 seconds in the latest focused test process.
- `./test.sh --test hub_lua_runtime_test plugin_db_batch_capability_denial_raises_a_lua_error -- --exact --nocapture` — 1 passed through an enabled MCP package without a `plugin_db` grant; `pcall` captured the raised namespace/capability denial.
- `./test.sh --test hub_lua_runtime_test plugin_db_reads_recover_every_batch_directory_shape_before_a_subsequent_public_commit -- --exact --nocapture` — 1 passed across live+staging, backup+staging/no-live, live+backup, and initially-empty staging-only cases.
- Legacy Lua regression tests for missing get and missing patch/delete — both
  passed with their exact repository-wrapper commands.
- Existing async namespace/capability test
  `hub_runtime_stores_plugin_json_under_plugin_data_and_enforces_namespace` —
  passed with its exact repository-wrapper command.
- `cargo fmt --all -- --check` — passed after formatting.
- `cargo clippy --all-targets --all-features -- -D warnings` — passed.
- `./test.sh` — passed: all default repository tests and doctests; 1 documented
  large local adversarial test remained ignored by the repository default.
- Negative control: the production `LocalPluginStoreBackend::batch` was
  temporarily reverted in this run worktree to write each mutation directly
  and sequentially, with no environment switch or shipped alternate path.
  `./test.sh --test hub_lua_runtime_test plugin_db_batch_atomically_commits_project_pipeline_lifecycle_and_returns_typed_failures -- --exact --nocapture`
  failed with exit 101 at the late-conflict assertion (reported index 1 instead
  of 2 after writing the first record). The temporary implementation was then
  removed; `rg` confirmed no sequential-ablation symbol remained, and the same
  exact public runtime test passed.
- `git diff --check` — passed.
- Provenance before the implementation commit: Hub plan HEAD
  `bf96da77ed6aea54cb6f1c50bd36889a91ccc81c`; base
  `e8febabf73259cfd922592346b244ec473c17323`; locked Core
  `5846fc776d31e2b6c98a8d932f50a31078743901`. The final gate artifact records
  the containing implementation commit and PR.

## Residual risk and unverified behavior

- The existing backend mutex is global, so a batch's bounded filesystem sync
  window serializes plugin-store access across namespaces. The 1,024-key/4 MiB
  limits bound the work. Promotion performs a namespace walk and hard-link per
  unchanged record but encodes and file-syncs only mutated records; this ticket
  does not introduce per-namespace locks.
- A host filesystem failure after rename visibility but before the final
  durability/cleanup barrier returns `backend_failed`; callers must reconcile
  ambiguous transport or storage failures with an authoritative read. Success
  is never returned before the barrier.
- Power-loss durability is proven through deterministic recoverable directory
  shapes and runtime reconstruction, not destructive machine-level power-cut
  testing.

## Missing vault guidance

None discovered. The approved plan and existing Hub/runtime/testing notes
covered ownership, worker isolation, filesystem durability, public-path proof,
ablation, artifact truth, and PR-backed handoff.
