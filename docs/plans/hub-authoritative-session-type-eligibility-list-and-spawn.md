# Hub: authoritative session-type eligibility list and spawn for admitted spawn points

**Plan revision:** addresses Plan Review `review_1786388756_440614` (`changes_required`). Operator screenshots confirm the product bug: New session for spawn point **Hub** shows *No session types for this spawn point* while Global/device types exist.

## Target and context

| Field | Value |
| --- | --- |
| **Target repository** | `trybotster/botster-hub` (`botster-hub`) |
| **Target id** | `tgt_7e208a0c76a44980a83b63af976b1f22` (resolved via `list_spawn_targets`; not ambient cwd) |
| **Ticket / run** | `ticket_1786387816_590636` / `run_1786387891_159185` |
| **Baseline SHA** | `26f1673` on worktree branch `project-pipelines/ticket_1786387816_590636` |
| **Primary charter** | [[botster-hub-playbook]] |
| **Client DTO charter (required this revision)** | [[botster-hub-client-playbook]] — public daemon request/DTO surface lives in the client crate embedded here |
| **Role overlays** | [[planner-playbook]], [[botster-planner-playbook]] |
| **Review overlays for Implement** | [[botster-runtime-reviewer-playbook]]; [[botster-package-reviewer-playbook]] for hub-test-support npm publication |
| **Runtime-teardown class** | **Does not apply.** [[botster runtime teardown lenses]] **not** loaded. |
| **Project Pipelines package workflow** | **Not** in scope → [[project-pipelines-playbook]] not loaded. |

## Playbooks and atomic notes loaded

### Role / repository charters

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-hub-playbook]]
- [[botster-hub-client-playbook]] ← **added this revision** (public request enum / TS / npm proof)
- [[botster-architecture]], [[cli-patterns]], [[spa-patterns]] (planner overlay)

### Targeted atomic notes

- [[hub qualifies effective session type ids as source name slash id]]
- [[incomplete repo local session types drop the hub client connection]]
- [[device hub owns admitted spawn targets not ambient repo cwd]]
- [[web-session-creation-must-be-target-first]]
- [[workspace session templates are hub owned capabilities callable from lua workers]]
- [[session template override sources use package device repo explicit precedence]]
- [[botster hub is a first party host profile over core]]
- [[botster hub gravity must be watched before it becomes the new monolith]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[botster hub client crate is the external client boundary]]
- [[botster hub client compatibility descriptors belong in client crate]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[generated typescript dtos must encode serde field optionality]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[conformance fixture revisions must be unique per published content]]
- [[adding a hub client feature constant is a three site change]]
- [[published capability matrices must derive enumerations from source]]
- [[botster first party client support matrices belong in hub test support]]
- [[external client hub tests use subprocess spawned hub test support]]
- [[hub test support npm releases need external consumer smoke]]
- [[hub generated protocol changes are a four site release chain]] (if present; else follow four-site chain from hub-test-support note)
- [[cold turkey migrations eliminate dual code paths and version suffixes]]
- [[prefer framework and library components over custom solutions]]
- [[test script required for rust tests not cargo test]]
- [[rust repo strict lints must be verified before dismissing warnings]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[vault example paths are not repository placement conventions]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

### Code / product context loaded

- **Observed UX (operator screenshots):** New session → spawn point Hub → empty state *No session types for this spawn point* / manage types CTA — matches ticket and Web fat-client filter bug.
- **Root cause in Hub:** `list_session_types_for_target` filters global effective rows with `target_id == T` after `source_default_target_id` pins device types to `device:local`; `materialize_session_type` rejects `request.target_id != default`.
- **Precedence hazard (Plan Review finding):** today `list_session_types` → `effective_session_type_rows` groups by **bare id across all sources**, then for-target filters. A repo type on T2 with the same bare id as a device Global can win globally and **hide** the device type when listing T; same-rank repo collisions across targets can also surface as `ambiguous_session_type`. **Target-scoped resolution is required**, not post-filter of global winners.
- Lua `session_types.list({target_id})` already calls `list_session_types_for_target`; daemon has only management `ListSessionTypes`.
- Protocol: `PROTOCOL_VERSION = 6`, `CONFORMANCE_FIXTURE_REVISION = 32`, hub-test-support npm `0.1.25`.
- Prior art additive op: `docs/plans/publish-lossless-session-type-authoring-view.md`.
- Plan path: `docs/plans/**` (repo prior art).

---

## Product decision ledger (pinned)

| Decision | Choice |
| --- | --- |
| Product rule | **Option A.** Device-authored Global types are storage-global and **eligible at every enabled admitted spawn point T**. List-for-T and spawn/materialize with `target_id=T` include/accept them. |
| Option B | Rejected (`device:local`-only eligibility) unless human waives A. |
| **Resolver order (critical)** | For spawn-point surfaces: **(1)** validate T is enabled+admitted → typed reject otherwise; **(2)** collect **source rows eligible for T** only; **(3)** apply package &lt; device &lt; repo precedence **within that set**; **(4)** project rows with effective id `source_name/id`, list-context `target_id = T`, available, optional reason. Management catalog keeps the existing **global** effective path unchanged. |
| Single policy path | One eligibility predicate + one target-scoped effective-row builder used by list-for-T, show-for-T, materialize, managed-git precheck, Lua, daemon, CLI. No dual eligibility branches. |
| Device cwd/root | At admitted T: **command root** = device source root; **default cwd** = T's admitted root; **Relative** working_directory path is resolved **under T's root** (not under device root); explicit cwd must stay under T's root; context `target_id` = T. |
| Package eligibility | Unchanged: default `package:{name}` and/or explicit authored `target_id` pin. Not multi-target Global. |
| Repo eligibility | Only for that target's repo source. |
| Disabled / missing T | **Typed reject** (`target_not_admitted` / `target_not_found`), never empty list that looks like "no types." Empty list only for enabled admitted T with zero available eligible types. |
| Daemon API | Additive **`ListSessionTypesForTarget { target_id }`** (tag stable). Keep unit `ListSessionTypes` as management catalog. |
| Protocol versioning | `PROTOCOL_VERSION` stays **6** (additive request). Bump **`CONFORMANCE_FIXTURE_REVISION`** above 32 and every concurrent published meaning at regenerate time. Prefer **not** adding a new required feature constant unless matrix/proof forces it (three-site gotcha). |
| Management vs picker | Entity + `ListSessionTypes` = management catalog. Spawn picker = list-for-T only. |
| Core | Hub-only. No Core taxonomy. |
| Downstream | Existing tickets only (named below). No new Workspaces ticket unless consumer proof finds a real change. |

---

## Scope

### In scope

1. Target-scoped source filter **before** precedence for all spawn-point surfaces.
2. Eligibility predicate encoding Option A + package/repo rules.
3. Materialize dual-root policy (device command root + T cwd, including Relative under T).
4. Additive daemon `ListSessionTypesForTarget`, HubClientApi, CLI, wiring identical to Lua helper.
5. **Mandatory** public contract publication chain (client crate → generated TS → hub-test-support assets → version → npm → external smoke).
6. **Mandatory** scratch-worktree botster-tui compile against patched `botster-hub-client` for the new enum variant.
7. Tests: list/spawn parity, cross-target bare-id collisions, device-vs-repo same bare id, ordering, Relative cwd at T, disabled target reject.
8. Docs: client-protocol, README session-types, lua-plugin-abi if needed.

### Non-scope

- Web/TUI implementation (existing consumer tickets).
- Core changes.
- Making all package types multi-target.
- Dual spawn list APIs with different eligibility.
- Project Pipelines workflow policy.
- Runtime-teardown class.
- Fixing unrelated baseline WebRTC product bugs unless this change regresses them (see baseline disposition).

---

## Repository ownership and cross-repo dependencies

### Ownership

| Surface | Owner |
| --- | --- |
| Eligibility policy, target-scoped resolve, materialize, entity catalog | **botster-hub** |
| Public daemon request/DTO/TS/conformance/test-support publish | **botster-hub-client** charter (crate in this monorepo) |
| Policy-free PTY spawn | botster-core (unchanged) |
| New session UI | botster-web / botster-tui consumer tickets |

### Registered downstream dependencies (already exist — do not invent later)

| Consumer ticket | Target | Target id | Dependency on this Hub ticket |
| --- | --- | --- | --- |
| **Web:** `ticket_1786387865_686375` — *Web: skinny New session picker — Hub list only, no client eligibility filter* | botster-web | `tgt_40abcf71ccf049f4ac0c99953a799869` | `dependency_1786387869_522414` → `ticket_1786387816_590636` (open) |
| **TUI:** `ticket_1786387865_677482` — *TUI: drop client session-type eligibility synthesis; use Hub spawn-point list* | botster-tui | `tgt_c3d470bab78549df920a41e8fb0e58d8` | `dependency_1786387871_193328` → `ticket_1786387816_590636` (open) |

**botster-workspaces:** already consumes `session_types.list({target_id})` and Hub-qualified effective ids ([[hub qualifies effective session type ids as source name slash id]]). No new Workspaces ticket in this plan unless Implement/Verify finds a real call-site break after target-scoped fix.

**Core:** no dependency ticket expected.

---

## Implementation sketch (surgical)

### A. Target-scoped resolver (addresses finding: eligibility before precedence)

```text
list/show/materialize for T:
  ensure_enabled_admitted_target(T)?          // typed reject if not
  sources = source_session_types(...)
  eligible = sources.filter(|s| is_eligible_for_target(s, T, state))
  rows = effective_session_type_rows(eligible) // package < device < repo within eligible only
  project target_id field = T for multi-target winners
  order = stable (see Acceptance)
```

**`is_eligible_for_target` (summary):**

- **Device**, available, no exclusive pin (or pin == T): eligible for every enabled admitted T.
- **Repo**: eligible iff `source_name == T` (and target enabled — already required for loading).
- **Package**: eligible iff default/package pin or authored `target_id` equals T and available.

Management catalog continues: `list_session_types` → all sources → global effective rows (device may still show storage `target_id` provenance `device:local`). Document that catalog is **not** the New session authority.

### B. Materialize

- Use eligibility helper instead of `resolved_target_id == source_default_target_id`.
- Device@T: command under device root; cwd default = T root; Relative path under T root; fail closed if Relative escapes T.

### C. Public contract (addresses client charter + mandatory publish)

1. Add `DaemonRequest::ListSessionTypesForTarget { target_id }` (+ HubClientOperation, tags, examples).
2. **Source-break assessment:** public enum is exhaustive today (no `non_exhaustive`) → new variant breaks consumer `match` arms. Measure with **scratch** botster-tui worktree + `[patch]` to this checkout ([[scratch cargo patch redirects measure downstream dto breakage]]); record exact compile failures. Prefer minimal fix in **this** crate only if a constructor helper is needed; consumer code fixes stay on TUI ticket except proof compile may temporarily patch for measurement.
3. Regenerate TypeScript; optionality rules per [[generated typescript dtos must encode serde field optionality]].
4. Bump `CONFORMANCE_FIXTURE_REVISION`; sync `packages/hub-test-support` assets/metadata; allocate next npm version above `0.1.25`.
5. **Mandatory** publish + **external installed-package smoke** asserting new request token(s), revision, `metadata.package_version` ([[hub test support npm releases need external consumer smoke]]).
6. Wire daemon_transport, client_api admission (same group as `ListSessionTypes` / `allow_packages` unless editor secrets appear — list-for-T is sanitized spawn options), CLI (`session-types list --target <id>` or dedicated subcommand matching existing `DataArgs` style).

### D. Do not dual-path

Remove or never add a second for-target filter that re-applies global winners. Cold-cut the broken equality filter.

---

## Affected surfaces / files

| Area | Paths |
| --- | --- |
| Policy | `src/session_types.rs` |
| Runtime / Lua | `src/runtime.rs`, `src/lua_runtime.rs` |
| Daemon / API / CLI | `src/client_api.rs`, `src/daemon_transport.rs`, `src/main.rs` |
| Client contract | `crates/botster-hub-client/src/lib.rs`, `typescript.rs`, `generated/daemon-protocol.ts` |
| Test support | `crates/botster-hub-test-support/**`, `packages/hub-test-support/**` |
| Tests | `tests/hub_client_api_test.rs`, `tests/hub_lua_runtime_test.rs`, `tests/hub_daemon_lifecycle_test.rs` |
| Docs | `docs/client-protocol.md`, `README.md`, this plan; reports under `docs/reports/**` |

---

## Assumptions and unknowns

### Assumptions

- Device Global = device-rank definitions without an exclusive non-T pin; explicit authored device `target_id` pins to that target only.
- Workspaces list API already correct once Hub helper is fixed; no automatic Workspaces ticket.
- Additive request keeps PROTOCOL 6; conformance revision + publish are load-bearing.

### Unknowns settled in ledger where possible

- List ordering: **pin** to deterministic order — sort effective rows by `(source rank ascending for display? or: source kind order package→device→repo, then session_type_id lexicographic)`. **Decision:** sort by `session_type_id` lexicographic after effective resolution (stable, easy to test). Document in client-protocol.
- Relative cwd under T: **decided above** (under T root).
- Unavailable rows with reasons: **available-only** in v1 spawn list unless a first-party consumer blocks.

### Convention conflicts

**None.** Option A, Hub policy ownership, thin clients, qualified ids, additive conformance bumps, and client-crate DTO ownership align.

---

## Baseline test disposition (addresses finding: adjacent failures)

**Evidence (Plan Review on refreshed origin/main `26f1673`):** `./test.sh` reached tests → **185 passed, 2 failed**.

| Failure class (reviewer) | Characterization | Disposition for this ticket |
| --- | --- | --- |
| Stale-transition / worker spawn | `botster-session-worker` cannot spawn (binary/env) | **Environment pre-req**, not session-type eligibility policy. Implement/Verify **must** run `cargo build --locked -p botster-core --bin botster-session-worker` before claiming lifecycle suite green. Re-run and capture exact test name + first non-cascade cause. |
| Local WebRTC spawn failure | Transport/bootstrap path | **Orthogonal** to list-for-target eligibility and device@T materialize policy. Re-run after worker prebuild; record exact test name. If still red and stack does not enter `session_types` eligibility, document isolation evidence (stack / assertion) that the failure is unrelated. If isolation cannot be shown, **open an owner ticket** and register dependency before claiming full workspace green. |

**Plan gate statement:** This Plan does **not** claim bare `./test.sh` is currently green. Acceptance for Implement:

1. **Always required:** focused eligibility/parity tests (nonzero executed count) + format + strict Clippy.
2. **Workspace `./test.sh`:** required when conformance/publish chain moves; may only be attested green after baseline disposition above (worker prebuild + named residual failures proved unrelated or ticketed).

---

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Global precedence hides device Global | Target-scoped filter **before** precedence; collision tests |
| List/spawn drift | Single helper; parity tests every listed id spawns at T |
| Dual-root cwd escape | Relative under T; negative cwd tests |
| Exhaustive enum source break | Scratch TUI patch compile; publish + consumer tickets |
| Publish lag | Mandatory four-site chain + external npm smoke |
| Baseline noise masks regressions | Focused suite always; full workspace only with disposition |
| Clients keep entity filtering | Downstream Web/TUI tickets already registered |

---

## Acceptance checks / tests

### Functional

1. **Device Global @ T:** device G + enabled T → list-for-T includes effective `device/{id}`; spawn/materialize(G,T) succeeds; context/cwd bind to T.
2. **Relative device @ T:** device type with `working_directory: Relative { path }` → cwd is `T_root/path`, not `device_root/path`; escape rejected.
3. **Cross-target bare id:** repo type bare id `init` on T2 only → list(T) still includes device Global `init` if device defines it; list(T2) includes repo winner, not T1-only types.
4. **Same bare id device vs repo on T:** repo wins on T (precedence); list(T) shows repo effective id/provenance; device still available on T' without that repo.
5. **Repo isolation:** T repo types not on T2.
6. **Package pin policy** unchanged.
7. **List/spawn parity** for every available list-for-T row.
8. **Disabled/missing T:** typed reject.
9. **Stable order:** list-for-T sorted by `session_type_id` lexicographic (assert exact order in test).
10. **Management catalog** still lists device types for authoring; definition path lossless.
11. **Lua** list matches daemon list-for-T for same state.
12. **Production path:** daemon + CLI + Lua all hit the same helper.

### Commands (non-vacuous; assert executed count > 0)

```sh
# Pre-req for any real spawn lifecycle suite
cargo build --locked -p botster-core --bin botster-session-worker

# Focused (example names — implementer uses real test fn names; always check "N passed" N>0)
./test.sh --test hub_client_api_test device_global_session_types_eligible_at_admitted_spawn_point -- --exact --nocapture
./test.sh --test hub_lua_runtime_test <real_lua_list_parity_test_name> -- --exact --nocapture
./test.sh --test hub_daemon_lifecycle_test <real_daemon_list_for_target_test_name> -- --exact --nocapture

# Format + strict lints (required)
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings

# When CONFORMANCE_FIXTURE_REVISION / hub-test-support / feature matrix moves:
./test.sh
# then: package asset sync check, npm version allocate, publish, external install smoke with new request tokens
```

Ablation: critical parity test red with fix reverted ([[a regression test must be shown to go red with the fix reverted]]).

### Downstream / publication proof (mandatory, not optional)

1. Scratch botster-tui worktree: Cargo patch to this hub-client → `cargo check --workspace` (+ `--all-targets` as needed); record breaks.
2. Generated TS committed and drift-checked.
3. hub-test-support version + metadata + fixtures include new request tag.
4. External npm install smoke: package version, revision, `ListSessionTypesForTarget` / `list_session_types_for_target` tokens present.
5. Web/TUI product UI proof stays on their tickets after this publish.

---

## Botster layers touched

Rust hub policy + daemon API + CLI; botster-hub-client protocol; hub-test-support; Lua capability behavior; docs.

**Worktree:** only this Hub worktree for implementation. TUI scratch tree is disposable measurement only.

---

## Pipeline gates and artifacts

- Plan artifact: this file.
- Plan Review must re-check: target-scoped resolver, client charter + mandatory publish, baseline disposition, named downstream tickets, acceptance commands, vault checklist.
- Implement report: `docs/reports/` with test counts, SHAs, publish version, TUI patch results, baseline residual disposition.

---

## Vault gaps (capture after implement if still true)

1. Device Global multi-target eligibility (Option A).
2. Target-scoped resolve-before-precedence for spawn lists.
3. Management catalog vs spawn-point list split.
4. Device spawn dual-root + Relative-under-T policy.

No Plan-stage vault capture required beyond this plan.

---

## Plan Review finding map (this revision)

| Finding | Resolution in this plan |
| --- | --- |
| Apply target eligibility before source precedence | § Product ledger + § Implementation sketch A + acceptance 3–4 |
| Load client charter; mandatory publication proof | Loaded [[botster-hub-client-playbook]]; § C + mandatory publish acceptance |
| Adjacent baseline failures | § Baseline test disposition; no false green claim |
| Stale downstream statements | Named tickets + dependency ids + targets |
| Missing success checks / commands | Ordering, Relative, exact paths, fmt/clippy, nonzero count |
| Vault checklist | Complete one Plan checklist; skip duplicate |

---

## Summary for implementer

Do **not** filter global effective winners by `target_id`. For spawn-point T: validate T → filter sources eligible for T → then package &lt; device &lt; repo. Device Global is multi-target; dual-root cwd at T (Relative under T). Expose additive daemon list-for-T; publish client + npm with mandatory smoke and TUI patch compile. Keep management catalog separate. Hub-only; Web/TUI tickets already depend on this one.
