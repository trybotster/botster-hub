# Hub contract: add reactive entity-backed select options

## Plan revision

| Field | Value |
| --- | --- |
| Revision | **3** — addresses Plan Review `review_1786476382_415416` |
| Closed in rev 2 | `finding_1786475791_819472`, `finding_1786475791_257390`, `finding_1786475791_334483`, `finding_1786475791_657371` |
| Closed in rev 3 | `finding_1786476382_937809` (ordered entity-frame fixture + collector oracle), `finding_1786476382_443333` (exact string value domain + comparator) |
| Process note | Reuse canonical checklist `checklist_1786475251_205420`; do not create another vault checklist |

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Canonical path (registry) | spawn target `botster-hub` |
| Pipeline ticket | `ticket_1786474779_865884` |
| Pipeline run | `run_1786474895_731953` |
| Base ref / HEAD at Plan | `main` @ `0ee42e9b84a0b0e9b0ab89834675535c8b831993` |
| Locked Core (Cargo.lock) | `ff115694caf61e435bfb3d7ffcc5a6459689c8d9` |
| Assigned worktree | Project Pipelines session worktree for this ticket |
| Runtime-teardown class | **Does not apply** |
| Session-type eligibility consumer | **Does not apply** |

Repository routing: `project_pipelines_current_context` → ticket/run `target_id` → `list_spawn_targets` (`name: botster-hub`). Not inferred from ambient cwd.

## Repository playbook loaded

- [[botster-hub-playbook]]

## Other role / surface playbooks and atomic notes loaded

### Role entrypoints

- [[planner-playbook]]
- [[botster-planner-playbook]]

### Intentionally not loaded

- [[project-pipelines-playbook]] — no PP package/plugin path or workflow-policy change.
- [[botster runtime teardown lenses]] — not runtime-teardown class.
- Web/TUI ownership charters as implementation owners — consumers remain generic; this run does not patch those repos.

### Architecture and surface maps

- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]]

### Contract, binding, package, and proof notes

- [[botster package surface semantics live in ui contract while hub owns admission]]
- [[botster shared form primitive v1 is intentionally narrow and catalyst first]]
- [[cross-client ui should share semantic primitives and actions with renderer-specific adapters]]
- [[plugin dynamic ui lists bind to plugin-owned entities]]
- [[plugin surfaces request model state through ui bindings not hub subscribe]]
- [[plugin surface handlers must validate against hub locked uinode contract]]
- [[plugin entity families publish filterable record supersets]]
- [[ui bind list where filters plugin entity rows before template expansion]]
- [[ui bind list empty template renders entity backed empty rows]]
- [[package entity hydration uses explicit providers not mcp naming]]
- [[botster entity snapshots are authoritative reconnect baselines]]
- [[session UUID is the sole routing key across all layers]]
- [[botster rust consumers that share ui contract must pin one hub revision]]
- [[generated typescript dtos must encode serde field optionality]]
- [[conformance fixture revisions must be unique per published content]]
- [[shared conformance fixtures that contradict the core contract teach clients the wrong state machine]]
- [[published fixture readmes are part of the shipped contract]]
- [[plugin conformance packages prove shared contracts while examples prove product behavior]]
- [[hub supervision admission changes require exact live hub launch proof]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[hub test support npm releases need external consumer smoke]]
- [[cold turkey migrations eliminate dual code paths and version suffixes]]
- [[test script required for rust tests not cargo test]]
- [[vault example paths are not repository placement conventions]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

### Hub charter-required notes (bounded)

- [[botster hub is a first party host profile over core]]
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster hub events use bounded priority lanes instead of unbounded queue fuses]]

## Context loaded

### Ticket intent

Add one **generic** entity-backed options contract for `ui.select` in `botster-ui-contract`, plus **Hub surface admission** for that descriptor.

Descriptor fields: source entity family, exact value field, display fields, deterministic order, optional exact top-level filters, dynamic exclusion value source from another admitted family. No arbitrary query language. Preserve `session_uuid` (and any value field) **exactly**. Define option add/update/remove/exclude/reappear, selection invalidation, duplicates, missing fields, stable order. Prove Rust **and** executable TypeScript/JavaScript parity. Prove live Hub path and consumable npm artifacts.

### Repository facts that close prior plan gaps

1. **There is no Hub bind-list demand collector.** Repo-wide search finds only:
   - Hub admission walkers: `validate_plugin_surface_binding_*` in `src/runtime.rs` (walks `$bind`, `bind_list.source`, `bind_if.path`).
   - Explicit client/daemon subscriptions: `DaemonRequest::SubscribeEntities` / `HubClientRequest::SubscribeEntities` through `src/client_api.rs`, `src/daemon_transport.rs`, `src/local_webrtc.rs`.
2. **`DaemonPluginSurface` is body-only** (`package_name`, `surface_id`, `body: UiNode`, optional `ui_tree_snapshot`). No dependency DTO is returned today. Conformance already **parses** `bind_list.source` from the rendered body and then issues explicit `SubscribeEntities` (`crates/botster-hub-test-support` contract-matrix path).
3. **Admission examples must use exact declared families.** Undeclared `/project-pipelines.ticket` is rejected when only `project-pipelines.run` is admitted (`plugin_surface_binding_admission_accepts_only_exact_declared_plugin_family`). Single-segment package IDs map identity-preserving (`project-pipelines` → families `project-pipelines.*`). Dotted packages use `bns1_<hex>.…` owners (fixture family `bns1_626f74737465722e706c7567696e2d636f6e74726163742d6d6174726978.run`).
4. **npm already exports executable JS helpers** (`realizeBindListDescendantId` in `packages/ui-contract/index.js`). Fixture-only parity is insufficient for a projector; match that export pattern.
5. **Live daemon tests need `botster-session-worker`.** README and operator paths require `cargo build --locked -p botster-core --bin botster-session-worker` so the worker sits beside the Hub binary. Plan Review observed `./test.sh` failures from a missing worker — this is a **prerequisite**, not an unrelated flaky suite.
6. Current `@trybotster/ui-contract@0.3.1`; hub-test-support pins that coordinate.

## Resolved production topology (finding 819472)

### Boundary (locked)

| Layer | Owns | Does not own |
| --- | --- | --- |
| **botster-ui-contract** | Descriptor schema/validation; pure family collector; pure projector; shared fixture vectors; Rust + generated JS implementations | Transport, package policy, auto-subscribe |
| **Hub** | Surface **admission** of every family referenced by `options_source` (source + exclude); namespace isolation; when a family is subscribed, **authoritative snapshot then ordered entity changes**; reconnect baseline; typed `invalid_surface` without dropping the client connection | Auto-subscribe on render; inventing undeclared families; package-specific option branches; list_sessions / list-refresh fallbacks |
| **Client (Web/TUI/support harness)** | Walk rendered `UiNode` body with the **shared collector**, issue **explicit** `SubscribeEntities` for each required family, run **shared projector** over store + descriptor | Client-owned lifecycle policy for options; second projector implementation |

This matches the existing bind_list pattern: Hub admits sources at render/action-result; clients demand hydration via explicit subscribe ([[plugin surfaces request model state through ui bindings not hub subscribe]]). Ticket language “Hub owns subscription dependencies” means **Hub admits and serves** those dependency families correctly when subscribed — **not** that Hub invents an automatic render-time subscribe fanout or a hidden walker that does not exist.

### Named production entry points

1. **Author** — package surface emits `ui.select` with `props.options_source` (`$kind: entity_options`), not static `select_option` children.
2. **Hub render admission** — `HubRuntime::render_plugin_surface` → `validate_plugin_surface_node` → extended `validate_plugin_surface_binding_value` recognizes `$kind: entity_options`, admits `source` and `exclude.source` with the same rules as `bind_list.source` (`/session…` always; other absolute families only when in the admitted set for enabled package providers).
3. **Hub action-result admission** — `validate_plugin_surface_action_result` applies the same walk to replacement trees.
4. **Client demand** — after receiving `DaemonPluginSurface.body`, call contract helper `collect_entity_option_families(body)` (and existing bind paths) → one `SubscribeEntities` per distinct family.
5. **Hub subscription serve** — existing `SubscribeEntities` path delivers provider/session snapshot then ordered frames; reconnect/gap recovery = new authoritative snapshot ([[botster entity snapshots are authoritative reconnect baselines]]). Proven today for package entities in `tests/hub_lua_runtime_test.rs` and contract-matrix / daemon lifecycle tests.
6. **Client project** — `project_entity_options(descriptor, source_records, exclude_records, selection)` → ordered options + selection-valid flag. Submitted value is the exact projected `value` (e.g. raw `session_uuid`).

### Explicit non-paths (forbidden)

- No Hub auto-subscribe from render.
- No new dependency DTO **required** for this ticket (body walk is the production path). If Implement discovers a compelling DTO need, stop and ask — do not silently broaden hub-client protocol.
- No `list_sessions` polling, list refresh fallback, package-named Hub branch, or static-options fallback for the entity-options producer.
- Example exclusion/source paths must be **exactly admitted** families, e.g.:
  - `/session` (Hub-owned)
  - `/project-pipelines.run` (single-segment package, when that provider is declared)
  - `/bns1_626f74737465722e706c7567696e2d636f6e74726163742d6d6174726978.run` (dotted package fixture)
  - **Not** `/project-pipelines.ticket` unless that exact family is declared in the test package.

## Scope

### 1. Authored descriptor

```json
{
  "type": "select",
  "props": {
    "name": "session",
    "label": "Session",
    "options_source": {
      "$kind": "entity_options",
      "source": "/session",
      "value_field": "session_uuid",
      "display_fields": ["label", "lifecycle_class", "session_type", "spawn_point"],
      "order": ["label", "session_uuid"],
      "where": { "lifecycle_class": "current" },
      "exclude": {
        "source": "/project-pipelines.run",
        "value_field": "session_uuid",
        "where": { "status": "active" }
      }
    }
  }
}
```

Rules:

- Exactly one options producer: static `slots.options` of `select_option` **xor** `props.options_source` with `$kind: entity_options`.
- Absolute family paths only; exact top-level `where` equality only (BindList grammar).
- `value_field`, non-empty `display_fields`, non-empty `order` required; `exclude` optional but when present requires `source` + `value_field`.
- Default product surface for this ticket is **`UiNodeKind::Select` only** (not `UiFieldSchema` form-schema options) unless Implement finds schema-driven Select is the live plugin path.

### 2. Deterministic types, dual projector, and shared frame fixture (findings 257390 / 443333 / 937809)

#### Option value domain (locked — finding 443333)

**Option values are JSON strings only.**

| Concern | Rule |
| --- | --- |
| Accepted `value_field` / `exclude.value_field` types | **JSON string only** (including empty string). Number, boolean, null, object, array, or missing → **skip that record** (do not invent or coerce). |
| Why not numbers | JavaScript cannot preserve integers outside `Number.MAX_SAFE_INTEGER` while Rust/`serde_json` can; f64 bit equality is not exact JSON identity. String-only is the smallest exact dual-runtime domain and matches `session_uuid` producers. |
| Value equality | Exact UTF-8 byte equality of the string contents (no trim, casefold, NFC, or locale). |
| Label | First `display_fields` entry whose record value is a **string** (including `""`) becomes `label`. Non-string display values are skipped for label selection. If no string display field exists, `label = ""`. |
| Metadata | For each `display_fields` entry present as a **string** on the record, copy into option metadata under that field name. Non-string or missing fields omitted from metadata. |
| Order keys | For each key in `order`: missing field sorts **after** present; present non-string order keys sort **after** present strings (and are ordered only by presence/type rank among non-strings as equal rank, then fall through); string keys compare by **UTF-8 byte order** (Rust: `a.as_bytes().cmp(b.as_bytes())`; JS: compare `TextEncoder().encode` byte sequences lexicographically — **not** `localeCompare`, **not** UTF-16 code units). |
| Final value tie-break | Same UTF-8 byte order on the option `value` string. |
| Duplicates | After full sort, first option wins for a given `value`; later duplicates dropped. |
| Exclusion | Exclude set = strings from exclude-family records' `exclude.value_field` (string gate). Omit source options whose value is in that set. |
| Selection invalidation | `selection` must be a string or absent. If `Some(s)` and no projected option equals `s` by UTF-8 bytes, `selection_valid = false`. Contract never auto-picks. |
| Reconnect / gap | Projector is pure over current family **record maps**. Callers rebuild maps by applying the shared frame timeline (below). |

Authored validation still requires non-empty `value_field` name strings; it does not coerce entity data types at admission time.

#### Executable dual implementation (required)

- Rust: `project_entity_options` (+ types) in `botster-ui-contract`.
- JavaScript: export `projectEntityOptions` from `packages/ui-contract/index.js` / `index.d.ts`.
- Rust tests and Node tests must execute every **projector stage** (record-set inputs) and every **timeline step's expected projection** with exact equality.
- Export `collectEntityOptionFamilies(node)` in Rust + JS.

#### Family path → SubscribeEntities oracle (locked)

Authored absolute paths use a leading `/`. Daemon `SubscribeEntities.entity_type` does **not**. Collector must return subscription identifiers with the leading slash stripped, preserving exact remaining UTF-8 bytes (including dots in package families).

| Authored path (`source` / `exclude.source`) | `SubscribeEntities.entity_type` |
| --- | --- |
| `/session` | `session` |
| `/session/…` (field binds only; not a family source) | not collected as a family |
| `/project-pipelines.run` | `project-pipelines.run` |
| `/bns1_626f74737465722e706c7567696e2d636f6e74726163742d6d6174726978.run` | `bns1_626f74737465722e706c7567696e2d636f6e74726163742d6d6174726978.run` |

Shared fixture must include **exact collector vectors** for these mappings (and reject/skip non-absolute paths). This matches contract-matrix code that does `source.strip_prefix('/')` before subscribe.

#### One ordered shared fixture timeline (required — finding 937809)

Ticket requires **one** fixture covering source snapshots, exclusion snapshots, ordered upsert/patch/remove, reconnect, gap recovery, duplicates, and Unicode labels. Record-set-only stages are **not** enough for Web/TUI entity-store consumers.

Ship **one** conformance object (name e.g. `entity_options_reactive_timeline`) generated from Rust into `conformance-fixtures.json`, with:

1. **`descriptor`** — the entity-options select descriptor under test (source + exclude families, string `value_field`s, display_fields, order, optional where).
2. **`collector_vectors`** — authored path → subscription id oracle cases above, plus the families collected from a sample select node tree.
3. **`timeline`** — ordered steps. Each step:
   - `name` (stable id)
   - `frames`: one or more canonical **entity frames** for the source family and/or exclude family, shaped like production `DaemonEntityFrame` discriminants consumers already know:
     - `snapshot` `{ entity_type, snapshot_seq, items[], resync_reason? }`
     - `upsert` `{ entity_type, id, fields, seq }`
     - `patch` `{ entity_type, id, fields, seq }`
     - `remove` `{ entity_type, id, seq }`
   - `expected_store`: whole-family record maps after applying **all frames in this step** to the running store (snapshot replaces family; upsert/patch merge by id; remove drops id; reconnect/gap steps use a new snapshot with higher/epoch seq and optional `resync_reason`).
   - `expected_projection`: exact `{ options: [...], selection_valid }` from `projectEntityOptions` over those maps + fixed `selection` string (include a step where selection becomes invalid).

Minimum timeline coverage (single fixture, both families exercised):

| Step | Frames | Proves |
| --- | --- | --- |
| `source_snapshot` | source family snapshot | baseline options |
| `exclude_snapshot` | exclude family snapshot | exclusion removes values still in source |
| `source_upsert` | ordered upsert | addition + re-sort |
| `source_patch` | patch | label/metadata update + re-sort |
| `source_remove` | remove | option gone |
| `exclude_remove` | exclude remove | reappearance when exclusion clears |
| `duplicate_values` | snapshot or upserts with two records sharing value string | first-after-sort wins |
| `unicode_labels` | snapshot with Unicode label/value strings | exact UTF-8 preservation |
| `reconnect_snapshot` | new authoritative snapshot (both families as needed) with resync/new seq | store replace; no cross-reconnect delta merge |
| `gap_recovery_snapshot` | snapshot baseline after simulated gap | same replacement semantics as reconnect |
| `selection_invalid` | remove or exclude the selected value | `selection_valid: false` |

Rules:

- Projector unit tests may still feed **record maps** (the `expected_store` of a step) for speed.
- Downstream Web/TUI consumer tickets must apply the **frame timeline** to a real entity store, then call the shared projector — not invent their own mutation order.
- Frame field names must match the Hub client entity-frame wire vocabulary already published (`Snapshot` / `Upsert` / `Patch` / `Remove`); do not invent a second private mutation language.
- Per [[plugin conformance packages prove shared contracts while examples prove product behavior]]: this fixture lives in hub-owned `botster-ui-contract` / `@trybotster/ui-contract` as the **shared contract** artifact; live Hub package surfaces remain the product/admission proof, not a second private fixture language.

### 3. Hub admission (no phantom walker)

Extend `validate_plugin_surface_binding_value` in `src/runtime.rs`:

- When object has `$kind: "entity_options"` (or is nested under recognized `options_source`), validate absolute paths for `source` and `exclude.source`.
- Reuse `validate_plugin_surface_binding_path`.
- Unit tests: accept `/session` with empty admitted set; accept exact declared package family; reject foreign / undeclared (including exclude); reject entity-options + static options together at contract validation layer.

No auto-subscribe. Subscription dependencies are **proven** by live tests that issue explicit `SubscribeEntities` for both families after render.

### 4. Live Hub + package + npm proof (finding 334483)

#### Worker / gate prerequisite

Before any `./test.sh` daemon/workspace gate that needs the product topology:

```sh
cargo build --locked -p botster-core --bin botster-session-worker
# Worker must resolve beside botster-hub (target/debug or deps parent fallback).
./test.sh   # or targeted filters listed below
```

If a failure remains, Implement must attribute it with exact test name + unrelated evidence — not “suite was already red.”

#### Isolated live Hub scenario (required)

Use an owner-authored package surface on the real path  
`HubDaemon/HubRuntime → plugin worker → PluginSurfaceRender/Action → SubscribeEntities`:

Preferred vehicle: extend **plugin-contract-matrix** fixture (or a surgical sibling surface in that package) so it stays hub-owned conformance material — **or** a minimal path-local test package in `tests/` following `package_owned_entity_provider_drives_surface_admission_and_fresh_snapshots`.

Must prove:

1. Render admits select with `options_source` using `/session` and one exact package family as exclude (or source).
2. Explicit `SubscribeEntities` for **both** families returns authoritative snapshot then ordered upsert/patch/remove frames when records change.
3. Reconnect / second subscribe returns a **new** authoritative snapshot (fresh baseline).
4. Gap recovery path uses snapshot replacement (same serve contract as existing entity subscriptions).
5. Foreign/undeclared family in `options_source` or `exclude` → typed `invalid_surface` **without** dropping the client connection.
6. Action-result replacement carrying entity-options is admitted/rejected with the same rules.
7. Record in implement report:
   - Hub source SHA (`git rev-parse HEAD`)
   - Locked Core SHA from `Cargo.lock`
   - Realpaths of `botster-hub` and `botster-session-worker` binaries used ([[live hub proof records distinct hub and locked core binary provenance]])

#### npm consumed-artifact proof (required)

```sh
cd packages/ui-contract && npm run generate && npm run check && npm test
npm pack --dry-run   # record included files
npm pack             # tarball
# clean temp dir: npm install ./trybotster-ui-contract-<ver>.tgz
# assert: packageVersion, projectEntityOptions, collectEntityOptionFamilies (if exported),
# run every conformance entity_options stage via installed package
```

If hub-test-support version/pin/export changes, same pack + clean-install smoke for that package ([[hub test support npm releases need external consumer smoke]]).

If registry `npm publish` needs operator 2FA:

```sh
cd packages/ui-contract && npm publish --access public
# and only if support package ships in the same chain:
cd packages/hub-test-support && npm publish --access public
```

Report commands; do not file a separate publishing ticket; do not claim published without evidence.

### 5. Version and assets

- Coordinate bump for `botster-ui-contract` / `@trybotster/ui-contract` (prefer **0.3.2** if additive; **0.4.0** only if wire-breaking — document).
- Regenerate schema, d.ts, fixtures, index.js helpers.
- Update hub-test-support pin/metadata/tests that hardcode `0.3.1`.

## Non-scope

- Web/TUI renderer product UI beyond consuming shared helpers after they pin.
- Project Pipelines product adoption of entity-options selects (separate consumer ticket).
- Arbitrary query language, remote search, multi-select policy redesign.
- Hub auto-subscribe on render or new dependency DTO without an explicit human decision.
- Package-specific Hub option branches, list_sessions polling, static fallback for entity-options.
- Core changes unless a missing policy-free mechanism is proven (then register Core dependency).
- Runtime-teardown work.
- Creating additional vault checklists this visit.

## Repository ownership boundaries and cross-repo dependencies

| Repository | This ticket | Dependency posture |
| --- | --- | --- |
| **botster-hub** | Contract, projector dual impl, admission, live package/daemon proof, npm pack/smoke in-workspace | Implementation target |
| botster-core | Existing entity frames/store/subscribe | Locked at `ff115694…`; no Core ticket expected |
| botster-hub-client | Only if a DTO change is **required** (default: no) | In-workspace if forced |
| botster-web / botster-tui / botster-tui-kit | Generic consumers; pin one Hub/`ui-contract` revision after merge | Follow-up tickets; do not edit here |
| botster-project-pipelines | May later author entity-options selects | Out of scope |

## Assumptions and unknowns

### Assumptions (locked where possible)

1. Client-driven explicit `SubscribeEntities` after body walk is the production demand path (matches bind_list).
2. No new `DaemonPluginSurface` dependency field is required.
3. `UiNodeKind::Select` only for authored entity options.
4. Option values are strings only; sort/equality use UTF-8 byte order in both runtimes.
5. Shared fixture is a frame timeline, not record-set-only stages.
6. Worker binary build is a required gate prerequisite for live tests.

### Remaining Implement unknowns (not product ambiguity)

1. Whether to extend plugin-contract-matrix vs a dedicated `tests/` package for the live scenario (prefer matrix if small; otherwise tests/ package is fine).
2. Exact hub-test-support version bump necessity if only pin string changes.

If product-visible choices arise outside the locked tables (e.g. multi-select entity options, auto-clear policy inside contract), **ask a human** — do not invent.

## Affected surfaces / files

| Area | Paths |
| --- | --- |
| Contract | `crates/botster-ui-contract/src/lib.rs`, `src/assets.rs`, tests |
| npm | `packages/ui-contract/*` (`index.js`, `index.d.ts`, schema, fixtures, README, `test.mjs`) |
| Hub admission | `src/runtime.rs` |
| Live proof | `tests/hub_lua_runtime_test.rs` and/or `tests/hub_daemon_lifecycle_test.rs` and/or `fixtures/plugins/plugin-contract-matrix/*` + hub-test-support mirrors |
| Support pin | `packages/hub-test-support/package.json`, `metadata.json`, `README.md`, `test.mjs` as needed |
| Plan | this file; implement report under `docs/reports/` |

## Risks

1. **Misreading subscription ownership** as auto-subscribe → Hub gravity and dual paths (mitigated by locked topology).
2. **Undeclared example families** fail admission silently in tests (use exact declared paths only).
3. **Rust/JS sort drift** (mitigated by string-only values + UTF-8 byte order + timeline/projection equality tests).
4. **Missing session-worker** fails live gates (explicit build prerequisite).
5. **npm pin skew** for downstream Rust consumers ([[botster rust consumers that share ui contract must pin one hub revision]]).
6. **Fixture authority drift** teaching wrong subscribe/snapshot order.

## Acceptance checks / tests

### Contract / dual projector

- [ ] Authored validation: entity-options accept/reject matrix (paths, required fields, xor with static options).
- [ ] Values are **string-only**; non-string value_field data is skipped; UTF-8 byte order for sort/equality.
- [ ] One shared **frame timeline** fixture covers source/exclude snapshots, ordered upsert/patch/remove, reconnect, gap recovery, duplicates, Unicode, selection invalidation — with expected_store + expected_projection per step.
- [ ] Collector vectors prove `/session`→`session` and package absolute paths → subscription ids without leading `/`.
- [ ] **Rust and Node execute every timeline step's projection (and projector record-set stages) with exact output equality.**
- [ ] Exported JS `projectEntityOptions` + `collectEntityOptionFamilies` present in packed tarball.

### Hub admission

- [ ] `/session` entity-options admitted with empty package set.
- [ ] Exact declared package family admitted; foreign/undeclared (source **and** exclude) rejected as `invalid_surface` without connection drop.
- [ ] Action-result replacement uses the same rules.
- [ ] No package-named branch, list_sessions, or static fallback for entity-options.

### Live Hub + npm

- [ ] Worker built; live scenario green on exact Hub + locked Core binaries with recorded realpaths/SHAs.
- [ ] Explicit dual-family `SubscribeEntities` proves snapshot → ordered changes → reconnect snapshot.
- [ ] `npm pack` + clean install runs projector stages successfully.
- [ ] `./test.sh` targeted/workspace gates green, or exact unrelated-failure attribution.
- [ ] Production path evidence in implement report (render admission → subscribe → project), not type-existence only.

### Downstream (document, do not implement)

- After merge: Web/TUI/TUI-kit must pin one Hub/`ui-contract` coordinate together before consuming the helper.

## Vault gaps worth capturing

1. Entity-options producer: xor with static options; value type gate; exclusion set; selection invalidation signal.
2. UI demand remains **client explicit SubscribeEntities** after body walk; Hub admits and serves — no Hub auto-subscribe walker.
3. Executable dual projector (Rust + npm) is required for projection contracts, matching `realizeBindListDescendantId`.

## Implementation sequence

1. Types + validation (xor static options).
2. Pure projector + type-rank rules + Rust stage tests.
3. Generate assets; implement JS projector; Node stage tests.
4. Hub admission walker extension + unit tests.
5. Live package/daemon scenario + worker build in gate notes.
6. hub-test-support pin alignment; npm pack + clean install.
7. `./test.sh` proof; implement report with SHAs/realpaths/production path.

## Product decision ledger

| Item | Decision |
| --- | --- |
| Subscribe topology | Client explicit `SubscribeEntities` after shared body collector |
| Hub auto-subscribe | **Non-goal** |
| Dependency DTO | **Non-goal** by default |
| TS projector | **Required** executable export |
| Option value domain | **JSON strings only** (UTF-8 byte equality/order) |
| Shared fixture | **One ordered entity-frame timeline** + collector oracle |
| Auto-select on invalidation | **Non-goal** |
| Query language | **Non-goal** |
| Web/TUI product work | **Follow-up** |

## Plan completion evidence map

| Field | Value |
| --- | --- |
| target_repository | `botster-hub` |
| target_id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| repository_playbook | [[botster-hub-playbook]] |
| plan_uri | `docs/plans/add-reactive-entity-backed-select-options.md` |
| checklist_id | `checklist_1786475251_205420` (canonical; no new checklist) |
| teardown_class_applies | `false` |
| plan_revision | `3` |
