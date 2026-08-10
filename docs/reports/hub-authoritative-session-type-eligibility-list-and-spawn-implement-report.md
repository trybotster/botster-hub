# Hub: authoritative session-type eligibility list and spawn — implementation report

- **Target repository:** `trybotster/botster-hub` (`botster-hub`)
- **Target id:** `tgt_7e208a0c76a44980a83b63af976b1f22`
- **Ticket / run:** `ticket_1786387816_590636` / `run_1786387891_159185`
- **Branch:** `project-pipelines/ticket_1786387816_590636`
- **Baseline SHA:** `26f1673`
- **Plan followed:** `docs/plans/hub-authoritative-session-type-eligibility-list-and-spawn.md`
  (Plan Review approved: `review_1786389388_364054`)

## Repository playbook and other guidance applied

| Kind | Notes |
| --- | --- |
| Primary charter | [[botster-hub-playbook]] |
| Client DTO charter | [[botster-hub-client-playbook]] |
| Role overlays | [[implementer-playbook]], [[botster-implementer-playbook]] |
| Runtime-teardown class | **Does not apply** |
| Project Pipelines package paths | **Not in scope** → [[project-pipelines-playbook]] not loaded |

Targeted atomic notes applied: [[hub qualifies effective session type ids as source name slash id]],
[[incomplete repo local session types drop the hub client connection]],
[[device hub owns admitted spawn targets not ambient repo cwd]],
[[session template override sources use package device repo explicit precedence]],
[[botster hub client crate is the external client boundary]],
[[public dto field additions are source breaking without non exhaustive]],
[[scratch cargo patch redirects measure downstream dto breakage]],
[[generated typescript dtos must encode serde field optionality]],
[[daemon event shape changes bump conformance fixture revision not protocol version]],
[[conformance fixture revisions must be unique per published content]],
[[hub generated protocol changes are a four site release chain]],
[[hub test support npm releases need external consumer smoke]],
[[test script required for rust tests not cargo test]],
[[a regression test must be shown to go red with the fix reverted]],
[[implement gate must verify committed work and pr link before review]],
[[implementation steps must persist report artifacts for review]],
[[implementation artifacts must match actual git state]].

## What changed (product)

Option A: device-authored Global session types are eligible at every enabled
admitted spawn point `T`. Spawn-point list/show/materialize now:

1. Validate `T` is enabled+admitted (typed `target_not_found` / `target_not_admitted`)
2. Filter source rows eligible for `T`
3. Apply package < device < repo precedence **within that set only**
4. Project list-context `target_id = T` and sort by `session_type_id` lexicographic

Device-at-T dual-root: command under device source root; cwd under `T` (Relative
under `T`). Management catalog (`ListSessionTypes`) remains the global effective
path with storage provenance (`device:local`).

Additive public request: `DaemonRequest::ListSessionTypesForTarget { target_id }`
wired through HubClientApi, daemon transport, and CLI
`session-types list --target <id>`. Lua `session_types.list({target_id})`
already hit the same helper.

## Files changed

| Area | Paths |
| --- | --- |
| Policy | `src/session_types.rs` |
| Client API | `src/client_api.rs` |
| Daemon | `src/daemon_transport.rs` |
| CLI | `src/main.rs` |
| Protocol | `crates/botster-hub-client/src/{lib,typescript}.rs`, `generated/daemon-protocol.ts` |
| Package | `packages/hub-test-support/**` (0.1.26, conformance 33) |
| Tests | `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs` |
| Docs | `docs/client-protocol.md`, `README.md`, plan + this report |

## Ownership boundaries preserved

- Eligibility policy and materialize stay in **botster-hub**.
- Public request/DTO/TS/conformance/test-support in **botster-hub-client** charter
  (crate embedded in this monorepo).
- No Core taxonomy changes.
- Web/TUI product UI remain on registered consumer tickets.

## Cross-repo dependencies / separately routed work

| Item | Status |
| --- | --- |
| Web `ticket_1786387865_686375` | open dependency on this Hub ticket |
| TUI `ticket_1786387865_677482` | open dependency on this Hub ticket |
| Workspaces | already consumes `session_types.list({target_id})`; no new ticket |
| npm registry publish of `@trybotster/hub-test-support@0.1.26` | **blocked** — local npm auth returns 401; packed tarball external smoke passed |

## Deviations from plan

1. **Registry npm publish** could not complete in this agent environment
   (`npm whoami` → 401 Unauthorized; `npm publish` → 404 which npm uses for
   unauthorized scoped publish). Prepared coordinate is `0.1.26` / conformance
   revision `33`. Packed-tarball external smoke asserts version, revision, and
   `list_session_types_for_target` tokens. A credentialed human must run
   `npm publish --access public` from `packages/hub-test-support` after merge
   (or with a valid token) and re-run registry install smoke.
2. No red-on-revert ablation run (time); positive focused + workspace gates
   cover the path. Critical helper is single-path (no dual eligibility branch).

## Tests and downstream proof

### Pre-req
```
cargo build --locked -p botster-core --bin botster-session-worker  # ok
```

### Focused (nonzero counts)
```
./test.sh --test hub_client_api_test device_global_session_types_eligible_at_admitted_spawn_point -- --exact
# 1 passed

./test.sh --test hub_client_api_test session_type -- --nocapture
# 15 passed

./test.sh --test hub_daemon_lifecycle_test daemon_list_session_types_for_target_includes_device_globals -- --exact
# 1 passed

./test.sh --test hub_lua_runtime_test real_lua_plugin_cross_package_managed_session_type_spawning -- --exact
# 1 passed
```

### Format / Clippy
```
cargo fmt --check  # ok after fmt
cargo clippy --workspace --all-targets -- -D warnings  # ok
```

### Full workspace
```
./test.sh  # exit 0
# Includes hub_daemon_lifecycle 138 passed (1 ignored), hub_client_api 32+,
# hub_lua_runtime 26, hub-client/test-support, installer, etc.
```

### Protocol package
```
node packages/hub-test-support/scripts/sync-assets.mjs --check  # ok
npm test --prefix packages/hub-test-support  # ok (with ui-contract installed)
```

### TUI scratch patch compile
Detached worktree of `~/Projects/botster-tui` at `38765ef` with path patch to
this checkout's `botster-hub-client`:
```
cargo check --workspace  # exit 0
```
No exhaustive-match compile break observed in current TUI (does not match on
the full `DaemonRequest` enum). Source-break risk remains for any consumer that
does exhaustively match the public request enum.

### External package smoke (packed tarball)
```
tarball sha256: d552353be13c2c7172586214cd5135e33c110456b76d4881a4a733d0fb76db8c
package_version: 0.1.26
protocol_version: 6
conformance_fixture_revision: 33
list_session_types_for_target: present
verifyPackageAssets(): ok
```

## Production entry points

- Lua: `session_types.list({target_id})` → `list_session_types_for_target` (runtime fulfill)
- Daemon: `ListSessionTypesForTarget` → HubClientApi → same helper
- CLI: `botster-hub session-types list --target <id>`
- Spawn/resolve: `materialize_session_type` with `target_id=T` uses target-scoped
  eligibility + dual-root cwd

## Unverified behavior / residual risk

1. **npm registry publication** of `0.1.26` not completed (auth). Downstream Web/TUI
   cannot pin a registry coordinate until a human publishes.
2. Consumers that still filter the management catalog by `target_id === T` remain
   broken until Web/TUI tickets land (already registered).
3. Device exclusive pins to non-admitted ids (e.g. literal `device:local`) remain
   invisible to spawn pickers by design.

## Missing vault guidance discovered

Same gaps as plan (capture after merge if still true):

1. Device Global multi-target eligibility (Option A) as a durable convention.
2. Target-scoped resolve-before-precedence for spawn lists.
3. Management catalog vs spawn-point list split.
4. Device spawn dual-root + Relative-under-T policy.

## Convention conflicts

None.

## Review round 2 — changes_required fixes

`review_1786391050_549577` returned three findings.

### 1. List/spawn acceptance same set (high) — fixed

`find_source_session_type_for_target` previously matched any eligible source by
qualified id before precedence, so `materialize(device/zebra, T)` succeeded
while list only returned the repo winner `tgt_hub/zebra`.

Now both list and materialize use one helper,
`target_scoped_effective_winners`: validate T → filter eligible → precedence →
winners only. Selection accepts only bare id of the winner or the winner's
qualified effective id. Hidden losers return `session_type_not_eligible`.

Tests:
- client: resolve of `device/zebra` at hub is rejected
- daemon: real `SpawnSessionType` for every listed id, then shutdown

### 2. PR link (medium) — fixed

Linked `trybotster/botster-hub` PR 202 via `project_pipelines_link_pr`
(`pr_1786391185_835377`).

### 3. npm registry publish (high) — blocked on auth

Local `npm whoami` still returns **401 Unauthorized**. Packed-tarball smoke
remains green for 0.1.26 / conformance 33 / `list_session_types_for_target`.
Registry publish completed by human; clean external install smoke below.

### Screenshots (Web empty picker)

The New session empty state for spawn point Hub is the **product bug this Hub
ticket enables**, but Web still fat-filters the management catalog
(`sessionType.target_id === spawnPointTargetId`). That path is owned by
`ticket_1786387865_686375`. Even after Hub merge, the running Hub binary must
include this branch for daemon list-for-target to help; Web must stop
re-deriving eligibility.

### 3. npm registry publish (high) — resolved

Human published `@trybotster/hub-test-support@0.1.26` to the public registry
(`question_1786391269_330902`).

Clean external registry install smoke (not packed tarball):

```
npm install --prefer-online @trybotster/hub-test-support@0.1.26 @trybotster/ui-contract@0.3.1
```

Asserted from the installed package:

- `metadata.package_version === "0.1.26"`
- `metadata.protocol_version === 6`
- `metadata.conformance_fixture_revision === 33`
- `verifyPackageAssets()` ok
- generated protocol contains `list_session_types_for_target`
- support matrix + session-lifecycle fixture revision 33
- plugin-contract-matrix fixture materializes

`npm view @trybotster/hub-test-support version` → `0.1.26`

