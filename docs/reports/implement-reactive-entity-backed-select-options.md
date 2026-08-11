# Implement report: Hub contract reactive entity-backed select options

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786474779_865884` |
| Run | `run_1786474895_731953` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Repository playbook | [[botster-hub-playbook]] |
| Plan | `docs/plans/add-reactive-entity-backed-select-options.md` revision 3 |
| Runtime-teardown class | Does not apply |

## Playbooks and notes applied

### Role / ownership

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]

### Atomic notes (targeted)

- [[botster package surface semantics live in ui contract while hub owns admission]]
- [[plugin dynamic ui lists bind to plugin-owned entities]]
- [[plugin surfaces request model state through ui bindings not hub subscribe]]
- [[plugin entity families publish filterable record supersets]]
- [[botster entity snapshots are authoritative reconnect baselines]]
- [[package entity hydration uses explicit providers not mcp naming]]
- [[plugin conformance packages prove shared contracts while examples prove product behavior]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[hub supervision admission changes require exact live hub launch proof]]
- [[hub test support npm releases need external consumer smoke]]
- [[test script required for rust tests not cargo test]]
- [[botster rust consumers that share ui contract must pin one hub revision]]
- [[conformance fixture revisions must be unique per published content]]
- [[published fixture readmes are part of the shipped contract]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

### Intentionally not loaded

- [[project-pipelines-playbook]] — no PP package/plugin path change
- [[botster runtime teardown lenses]] — not teardown class

## Ownership boundaries preserved

| Layer | This change |
| --- | --- |
| `botster-ui-contract` / `@trybotster/ui-contract` | Descriptor schema, validation, pure collector, pure projector, shared frame timeline fixture, Rust + JS dual implementation |
| Hub | Surface admission for `entity_options` source + exclude families; live package render + provider snapshots |
| Web / TUI / TUI-kit | Not edited; remain generic consumers of shared helpers after they pin 0.3.2 |
| Core | No change; locked at `ff115694caf61e435bfb3d7ffcc5a6459689c8d9` |
| hub-client DTO | No dependency DTO added; body walk + explicit `SubscribeEntities` remains the demand path |

## Files changed

- `crates/botster-ui-contract/src/entity_options.rs` (new)
- `crates/botster-ui-contract/src/lib.rs`
- `crates/botster-ui-contract/src/assets.rs`
- `crates/botster-ui-contract/Cargo.toml` (0.3.2)
- `crates/botster-ui-contract/tests/ui_contract_test.rs`
- `crates/botster-ui-contract/tests/generated_assets_test.rs`
- `packages/ui-contract/*` (version, index.js helpers, d.ts, schema, fixtures, README, tests)
- `src/runtime.rs` (admission walker + unit tests)
- `tests/hub_lua_runtime_test.rs` (live dual-family proof)
- `packages/hub-test-support/*` + `crates/botster-hub-test-support/examples/node_package_assets.rs` (pin 0.3.2 / package 0.1.28)
- `Cargo.lock`
- `docs/plans/add-reactive-entity-backed-select-options.md`
- `docs/reports/implement-reactive-entity-backed-select-options.md` (this file)

## Cross-repo dependencies / separately routed work

- **botster-web / botster-tui / botster-tui-kit**: after merge, pin one Hub + `@trybotster/ui-contract@0.3.2` coordinate and consume `projectEntityOptions` / `collectEntityOptionFamilies` + the frame timeline fixture. Do not implement here.
- **npm publish** of `@trybotster/ui-contract@0.3.2` and `@trybotster/hub-test-support@0.1.28` requires operator credentials/2FA:

```sh
cd packages/ui-contract && npm publish --access public
cd packages/hub-test-support && npm publish --access public
```

Not claimed published. No separate publishing ticket filed (per plan).

## Deviations from plan

None product-visible.

Implementation choices within plan latitude:

1. Live proof vehicle: dedicated `tests/hub_lua_runtime_test.rs` package surface (not plugin-contract-matrix extension) — plan allowed either.
2. hub-test-support package version bumped **0.1.27 → 0.1.28** solely for the ui-contract pin string.

## Review findings addressed (revisit)

| Finding | Resolution |
| --- | --- |
| `finding_1786478573_477450` publish hub-test-support registry pin | `packages/hub-test-support/package.json` depends on `@trybotster/ui-contract@0.3.2` (registry coordinate). Pre-publish smoke uses `npm install --no-save` or a temp consumer dir — never a bare tarball install that rewrites the manifest. |
| `finding_1786478573_670996` duplicate insertion order | Projector sorts `order` keys → option value → **source record id** (UTF-8 bytes) in Rust and JS; first-after-sort wins. Dual tests with opposite insertion order. |
| `finding_1786478573_703595` where equality domain | Authored `where` values are **JSON strings only** (validation + schema + TS). Runtime compare is exact string identity (no `JSON.stringify`). |
| `finding_1786478573_794752` absolute paths in report | Binary paths recorded as ticket-worktree-relative `target/debug/...`. |
| `finding_1786478573_892237` trailing whitespace | Plan markdown trailing spaces removed; `git diff --check origin/main...HEAD` clean. |
| `finding_1786479209_216628` smoke rewrites pin | Documented hub-test-support smoke uses `--no-save` + pin guards + pack inspection; no bare `npm install *.tgz` in the package tree. |
| `finding_1786479209_162150` unobservable duplicate winner | Duplicate tests give equal order keys but distinct non-order `spawn_point` metadata; both insertion orders must yield `from-a-early`. |

## Production path evidence

1. **Author** — package surface returns `ui.select` with `props.options_source.$kind = entity_options`.
2. **Hub render admission** — `HubRuntime::render_plugin_surface` → `validate_plugin_surface_node` → `validate_plugin_surface_binding_value` recognizes `$kind: entity_options` and admits `source` + `exclude.source` via `validate_plugin_surface_binding_path`.
3. **Client demand** — `collect_entity_option_families(body)` returns slash-stripped families; client issues explicit `SubscribeEntities` (same topology as bind_list).
4. **Hub serve** — existing provider/session snapshot path; live test proves reconnect issues a new authoritative snapshot for the package family.
5. **Project** — `project_entity_options` / `projectEntityOptions` over store + descriptor.

Forbidden paths not introduced: no Hub auto-subscribe, no dependency DTO, no `list_sessions` fallback, no package-named option branch.

## Tests and downstream proof run

### Contract / dual projector

```sh
BOTSTER_ENV=test cargo test -p botster-ui-contract
# includes entity_options_xor_static_options_and_validates_descriptor
#         entity_options_timeline_fixture_matches_projector_and_collector
#         generated_assets_match_checked_in_package

cd packages/ui-contract && npm run check && npm test
# runs all timeline steps against projectEntityOptions + collector oracle
```

### Hub admission unit tests

```sh
BOTSTER_ENV=test cargo test -p botster-hub --lib -- entity_options
# plugin_surface_entity_options_admission_accepts_session_and_declared_exclude
# plugin_surface_entity_options_admission_rejects_undeclared_source_and_exclude
# plugin_surface_entity_options_action_result_uses_same_admission
```

### Live Hub

```sh
cargo build --locked -p botster-core --bin botster-session-worker
BOTSTER_ENV=test cargo test --test hub_lua_runtime_test entity_options_select -- --nocapture
# entity_options_select_admits_dual_families_and_serves_fresh_snapshots
```

### npm pack + clean install

```sh
cd packages/ui-contract && npm pack
# clean temp: npm install ./trybotster-ui-contract-0.3.2.tgz
# asserted packageVersion, projectEntityOptions, collectEntityOptionFamilies,
# and every timeline step projection via installed package
```

### hub-test-support

Committed `package.json` must keep the **registry** pin
`"@trybotster/ui-contract": "0.3.2"`. Do **not** run a bare
`npm install path/to.tgz` in `packages/hub-test-support` — that rewrites the
manifest to a `file:` dependency by default.

```sh
node packages/hub-test-support/scripts/sync-assets.mjs --check
cd packages/ui-contract && npm pack
cd ../hub-test-support
# Pre-publication local smoke only: --no-save leaves package.json unchanged.
npm install --no-save ../ui-contract/trybotster-ui-contract-0.3.2.tgz
# Guard: committed pin must remain the registry coordinate.
node -e "const d=require('./package.json').dependencies['@trybotster/ui-contract']; if (d!=='0.3.2') process.exit(1)"
# Guard: packed package.json ships the same registry coordinate.
npm pack
tar -xOf trybotster-hub-test-support-*.tgz package/package.json | \
  node -e "let s='';process.stdin.on('data',d=>s+=d);process.stdin.on('end',()=>{const d=JSON.parse(s).dependencies['@trybotster/ui-contract']; if(d!=='0.3.2') process.exit(1)})"
npm test
# Clean local install residue; never commit node_modules or rewritten pins.
rm -rf node_modules trybotster-hub-test-support-*.tgz
rm -f ../ui-contract/trybotster-ui-contract-*.tgz
```

Alternatively, install both packed tarballs in a **separate temporary consumer
directory** (outside the repo package trees) so no workspace `package.json` is
touched.

### Provenance (live binary evidence)

| Identity | Value |
| --- | --- |
| Hub source SHA (branch tip) | `e06036e` on `project-pipelines/ticket_1786474779_865884` |
| PR | https://github.com/trybotster/botster-hub/pull/205 |
| Locked Core SHA (`Cargo.lock`) | `ff115694caf61e435bfb3d7ffcc5a6459689c8d9` |
| `botster-hub` binary realpath | ticket-worktree `target/debug/botster-hub` |
| `botster-session-worker` binary realpath | ticket-worktree `target/debug/botster-session-worker` |

## Unverified behavior / residual risk

1. Full `./test.sh` workspace suite was not re-run end-to-end in this visit; targeted filters covering the changed surfaces were green. Review/Verify may require full suite or broader filters.
2. Live test proves package-family subscribe/reconnect snapshots + dual-family collector demand; session-family frames are Hub-owned and exercised by existing session subscription tests, not re-proved in the new package fixture.
3. Ordered upsert/patch/remove frame delivery on the wire is covered by the shared timeline fixture + existing entity subscription conformance; the new live test focuses admission + dual-family snapshot/reconnect for the package exclude family.
4. Registry `npm publish` not executed (operator 2FA).

## Missing vault guidance discovered

Plan vault gaps remain worth capturing after merge:

1. Entity-options producer: xor with static options; string-only value gate; exclusion set; selection invalidation signal.
2. UI demand remains client explicit `SubscribeEntities` after body walk; Hub admits and serves — no Hub auto-subscribe walker.
3. Executable dual projector (Rust + npm) is required for projection contracts, matching `realizeBindListDescendantId`.

No blocking missing guidance prevented implementation.
