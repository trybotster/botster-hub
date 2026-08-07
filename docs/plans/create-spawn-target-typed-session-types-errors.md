# Plan: CreateSpawnTarget typed operator frame for invalid repo-local session-types

## Revision history

| Rev | When | Why |
| --- | --- | --- |
| 1 | Plan first pass | Initial root-cause plan |
| 2 | After Plan Review `review_1786071654_410637` | Address four open findings: stale base/core pin, poisoned-target recovery + acceptance check, cut Validate scope speculation, name strict Rust gates |

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Base target path | hub base target checkout (`tgt_7e208a0c76a44980a83b63af976b1f22`) |
| Pipeline worktree | session workspace on branch `project-pipelines/ticket_1786066158_557371` |
| Ticket / run | `ticket_1786066158_557371` / `run_1786070914_398826` |
| Pipeline | `botster_stack_delivery` step `botster_stack_plan` |
| Discovery pin (repro) | Hub `8a60bd58841179f8b1fd4040d9362d18ea244230` (from discovering ticket) |
| **Required base for Implement / acceptance** | `origin/main` @ `a35a0cca7a5f108044977fe89253e27e354d2952` (merge of PR #197 Ghostty/core cutover) |
| **Required locked core pin for acceptance evidence** | `botster-core` `363602bd756ff0f358a50b37e2abeb0a07ff66f7` (from that main tip’s `Cargo.lock`) |
| Plan-reviewer noted stale worktree HEAD | `302190ec2acc5ecee744432a6c9ffd1f040ebe01` — **not** an acceptable evidence base; rebase before Implement |

Do **not** infer ownership from ambient cwd; the ticket and run bind this work to the hub target above.

### Base / rebase obligation (finding_1786071654_872881)

1. Before any implementation commit or acceptance run, **rebase** the run branch onto current `origin/main` (`a35a0cca…` at Plan Review time; re-fetch if main moved).
2. Confirm `Cargo.lock` pins botster-core **`363602b…`** (post–Ghostty cold cutover). Green tests on superseded pin `33ebcd98…` do **not** count.
3. Scope survives the rebase: PR #197 did not touch `src/daemon_transport.rs` or `src/session_types.rs`. It did touch `tests/hub_client_api_test.rs` — if rebase conflicts there, **prefer `origin/main`’s version** and re-apply only this ticket’s test additions.
4. All focused + workspace + clippy/fmt evidence must be produced **after** that rebase on the post-cutover core pin.

## Repository playbook loaded

- [[botster-hub-playbook]] — ownership charter for this target.

## Other role/surface playbooks and atomic notes loaded

**Role / stack**

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-architecture]]
- [[cli-patterns]] (via hub charter / planner overlay; CLI/daemon surface)

**Surface overlays for later stages (named now, not expanded into Project Pipelines policy)**

- [[botster-runtime-reviewer-playbook]] — daemon/transport/operator-frame review for Implement/Review
- Matching runtime verifier overlay at Verify

**Atomic constraints**

- [[daemon request errors should return operator frames without dropping transport]] — charter rule this defect violates
- [[hub qualifies effective session type ids as source name slash id]] — protocol-6 session-type shape context
- [[device hub owns admitted spawn targets not ambient repo cwd]] — spawn-target admission is hub-owned
- [[incomplete repo-local session-types drop the hub client connection]] — inbox gotcha from discovering review (`ticket_1786036326_597046` / `finding_1786066043_137823`)
- [[live hub proof records distinct hub and locked core binary provenance]] — only if a live-binary proof is added; unit/integration daemon path is primary
- [[pipeline checklist creation duplicates on plugin worker invoke timeout]] — checklist create timed out after row commit; adopted existing checklist rather than retry-create
- [[plan agents must author vault context as wikilinks not home paths]]
- [[vault example paths are not repository placement conventions]] — plan lands under existing `docs/plans/` prior art in this repo
- [[test script required for rust tests not cargo test]] — use `./test.sh` / workspace form
- [[a regression test must be shown to go red with the fix reverted]]
- [[rust repo strict lints must be verified before dismissing warnings]] — clippy `-D warnings` is a named acceptance gate

**Not loaded (out of scope)**

- [[project-pipelines-playbook]] — no Project Pipelines package/plugin paths or workflow policy in this ticket
- Other repository charters (core, hub-client, web, tui, workspaces, tui-kit, ghostty)

## Context loaded

### Pipeline

- Ticket: Hub CreateSpawnTarget must return a typed operator frame for invalid repo-local session-types.
- Defect: incomplete `.botster/session-types.json` under a git/directory root causes CreateSpawnTarget to tear down the client (`ClientDisconnected`) instead of a typed operator error.
- Control: same incomplete file on ListSessionTypes returns `invalid_repo_session_types` / missing `label`…
- Expected: typed DaemonResponse operator error (same diagnosis family), transport remains open; no drop on invalid session-type handling during target create/update.
- Ownership: hub only; TUI fixture completion is a consumer workaround, not the fix.
- Plan Review returned `changes_required` with four findings (this rev addresses all).
- Vault checklist: `checklist_1786071067_706023`.

### Code (root cause)

Production entry path:

```
DaemonRequest::CreateSpawnTarget
  → session_type_definition_map(daemon)          // before
  → mutate_spawn_targets_response(create_…)      // persists target
  → advance_session_type_generation_if_changed   // after
       → session_type_definition_map
            → session_type_entity_snapshot
                 → list_session_types / source_session_types
                      → repo_session_types(root)  // serde of .botster/session-types.json
```

Critical bug site in `src/daemon_transport.rs` (`session_type_entity_snapshot`):

```rust
let session_types = crate::session_types::list_session_types(&records, &state)
    .map_err(|_| DaemonTransportError::UnexpectedResponse)?;
```

`UnexpectedResponse` is **not** mapped to an operator frame in the control-handler `or_else` (only `Client` / `Package` / `SpawnTarget` / `Worktree` / `State` / `Entrypoint` / `LocalWebrtc` are). Unmapped transport errors collapse the request path; clients observe `ClientDisconnected` and stderr `unexpected daemon response`.

ListSessionTypes goes through `HubClientApi` and maps `SessionTypeError` to `HubClientError::SessionType { kind, message }` → typed `operator_error` with `code = kind`.

Secondary defect: mutate **persists** before generation advance fails → half-admit if only mapping is fixed. Pre-admission validation must reject enabled create/update **before** persist.

### Already-admitted poison path (finding_1786071655_384699)

Once an enabled target is admitted and its `.botster/session-types.json` later becomes invalid (stale pre-protocol-6 files; targets admitted before this fix ships):

- `source_session_types` (`src/session_types.rs` ~944) hard-fails for the **whole** hub state with `invalid_repo_session_types`.
- `UpdateSpawnTarget` and `DeleteSpawnTarget` both call `session_type_definition_map(daemon)?` **before** mutation (~1264 / ~1297).
- After mapping fix alone: those paths return a typed frame instead of disconnecting — ticket literal satisfied — but the operator **cannot disable or delete** the offending target via daemon; only filesystem repair works.

**Pinned product default (recovery path succeeds):**

| Operation | When current state has invalid repo session-types on some enabled target | Required outcome |
| --- | --- | --- |
| Create enabled + invalid root | N/A (pre-check) | Reject framed; do not admit |
| Update that would leave/make an **enabled** invalid contribution | — | Reject framed; do not persist that change |
| Update `enabled=false` on any target (recovery) | before-snapshot fails with `invalid_repo_session_types` | **Still succeed**; persist disable; force-advance session_type_generation if needed |
| Delete any target (recovery) | before-snapshot fails with `invalid_repo_session_types` | **Still succeed**; remove target; force-advance generation if needed |
| ListSessionTypes with poison still present | — | Typed `invalid_repo_session_types` (existing; unchanged hard-fail list) |

Implementation shape (smallest surgical): do **not** soft-fail global ListSessionTypes. For Delete and disable-Update only, when the pre-mutation session-type definition map fails with `invalid_repo_session_types`, proceed with the spawn-target mutation anyway, then force-bump `session_type_generation` (skip or replace the compare-and-maybe-advance path). Non-recovery updates that keep an enabled poisoned contribution continue to return the typed operator error without disconnect.

### ValidateSpawnTarget (finding_1786071655_588898)

`DaemonRequest::ValidateSpawnTarget` calls `validate_spawn_target` against `state.spawn_targets` only and **never** reaches `list_session_types`. It cannot drop transport today. Ticket “create/update/validate” for validate is already satisfied by the mapping fix + existing path. **No Validate status extension in this ticket.**

### Repo gates

- Tests: `./test.sh` (workspace; `BOTSTER_ENV=test cargo test --workspace`)
- Focused: `./test.sh --test hub_daemon_lifecycle_test <filter>`
- Strict lints (charter + prior hub runs):
  `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  `cargo fmt --all -- --check`
- CI workflow `loaded-daemon-lifecycle.yml` does **not** run clippy/fmt — these remain **manual named gates** for Implement/Verify.

## Scope

1. **Preserve operator frames on session-type snapshot failures**
   Map `SessionTypeError` from `list_session_types` in `session_type_entity_snapshot` (and any peer hard path still using `UnexpectedResponse` for this class) to `DaemonTransportError::Client(HubClientError::SessionType { kind, message, … })` so control `or_else` returns a typed frame and keeps transport open.

2. **Pre-admission validation for create / non-recovery update**
   Before persisting CreateSpawnTarget / UpdateSpawnTarget when the resulting target would be **enabled** and contribute repo session types, validate the root’s `.botster/session-types.json` with the same loader ListSessionTypes uses (`repo_session_types` + `validate_session_types`, or a thin shared wrapper). On failure: framed `invalid_repo_session_types`, **do not** admit/persist, daemon remains usable.

3. **Pinned recovery path for already-admitted poison**
   Implement the table above: Delete and Update(enabled=false) must succeed even when pre-mutation `session_type_definition_map` fails with `invalid_repo_session_types`. Force-advance generation after those recovery mutations. Do **not** leave recovery as “edit the file only.” Document one operator line if README/spawn-targets docs currently imply all mutations require healthy session-types materialization (only if false/misleading).

4. **Regression proof** (daemon lifecycle, real daemon child / `daemon_transport_request`):
   - **Create reject:** incomplete session-types fixture → CreateSpawnTarget framed `operator_error` / `invalid_repo_session_types`; target not listed; no transport `Err`.
   - **Positive control:** complete PackageSessionType create succeeds; ListSessionTypes returns `<target_id>/<id>`.
   - **Update reject:** enable or repoint to invalid session-types rejects framed.
   - **Poison recovery:** admit target with **valid** session-types → rewrite file incomplete on disk → ListSessionTypes returns typed `invalid_repo_session_types` → Update `enabled=false` **succeeds** → Delete **succeeds** (or delete alone after poison succeeds). Assert no disconnect on any of these requests.
   - **Post-reject usability:** after rejected create, ListSpawnTargets/Status still works.
   - **Ablation:** fix reverted → create path regresses (disconnect / UnexpectedResponse / half-admit); recovery path may again be blocked — record red/green.

5. **Docs**
   Minimal only: if needed, one sentence that invalid repo session-types reject **enabled** admission with `invalid_repo_session_types`, and that disable/delete remain the recovery path when an admitted root’s file later goes bad. No broad rewrite.

## Non-scope

- **ValidateSpawnTarget status extension** — request path never loads session-types; already cannot disconnect; separate ticket if operators need validate-time session-type status (finding_1786071655_588898).
- Softening ListSessionTypes so one bad target does not fail the whole list (existing hard-fail; separate product decision).
- botster-tui / Workspaces fixture migration (consumer workaround on discovering ticket).
- Protocol version / conformance revision / hub-client DTO regeneration (no wire DTO change expected).
- botster-core changes (consume main’s locked pin only).
- Live multi-repo TUI re-proof as blocking dependency.
- Speculative abstractions, optional configurability, adjacent spawn-target refactors.
- Project Pipelines package/plugin work.

## Repository ownership boundaries and cross-repo dependencies

| Boundary | Owner | This run |
| --- | --- | --- |
| Daemon Create/Update/Delete spawn targets, operator frames, session-type snapshot, recovery sequencing | botster-hub | **yes** |
| Session-type schema / `PackageSessionType` validation | botster-hub | yes (reuse) |
| External client DTO crate | botster-hub-client (in-repo) | no change expected |
| TUI live harness / fixtures | botster-tui | non-scope |
| Workspaces plugin | botster-workspaces | non-scope |
| Core runtime | botster-core | consume locked pin only; no code change |

**Cross-repo dependencies:** none to register.

## Botster layers touched

- Rust hub daemon transport / control path
- Hub session-type loading/validation (shared helper if extracted)
- Spawn-target admission + recovery sequencing (pre-persist check; delete/disable under poison)
- Integration tests on the real daemon socket path

## Worktree / target assumptions

- Implement in the pipeline-owned hub worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`.
- **First Implement action:** rebase onto `origin/main` and confirm core pin `363602b…` before editing code or running acceptance.
- Tests use isolated data dirs as in existing hub daemon lifecycle tests.

## Assumptions and unknowns

**Assumptions**

1. Rejecting **enabled** admission when repo-local session-types are invalid is correct product behavior.
2. Missing `.botster/session-types.json` remains valid (empty contribution).
3. Create with `enabled=false` and an invalid file may succeed (disabled targets do not contribute); enabling later re-runs validation and rejects.
4. Recovery is **disable and/or delete via daemon**, not filesystem-only — pinned in scope and acceptance.
5. No protocol/DTO bump if existing `DaemonOperatorError` + `code` string is reused.
6. Primary proof is daemon integration path; live multi-binary pin proof is optional downstream.
7. Exact request_id/operation labels for projected SessionType errors follow existing ListSessionTypes / `HubClientError::SessionType` patterns.

**Unknowns**

- None blocking. Prior Validate consumer-set unknown is **removed** (Validate extension cut to non-scope).

**No human question required.**

## Affected surfaces / files

| Path | Change |
| --- | --- |
| `src/daemon_transport.rs` | SessionType mapping in `session_type_entity_snapshot`; pre-validate enabled create/update; recovery for Delete + disable-Update when map fails with `invalid_repo_session_types`; force generation advance |
| `src/session_types.rs` | Thin shared `validate_repo_session_types_at(root)` if that is the cleanest seam |
| `src/lib.rs` | Export helper only if needed |
| `tests/hub_daemon_lifecycle_test.rs` | Create reject + poison recovery + update reject regressions |
| `tests/hub_client_api_test.rs` | Only if shared helper unit tests are useful; resolve rebase conflicts toward main |
| `docs/plans/create-spawn-target-typed-session-types-errors.md` | This plan |
| `README.md` / spawn-target docs | One line only if recovery/admission contract is currently wrong |

`src/spawn_targets.rs` — **untouched** unless a pure helper is cleaner there (Validate status not extended).

## Risks

| Risk | Mitigation |
| --- | --- |
| Mapping-only fix half-admits | Pre-validate before persist |
| Mapping-only leaves Delete/disable stuck on poison | Explicit recovery path + acceptance check |
| Soft-fail ListSessionTypes by accident | Do not change global list hard-fail; recovery is spawn-target mutation only |
| Validation drift from ListSessionTypes | Share one loader |
| Acceptance on stale core pin | Rebase + pin `363602b` before evidence |
| Clippy/fmt skipped (not in CI workflow) | Named must-pass acceptance items |
| Ablation not recorded | Implementer records red/green for create + recovery |

## Acceptance checks / tests

**Must pass (after rebase onto required base / core pin)**

1. **Create reject:** incomplete session-types CreateSpawnTarget → framed `operator_error` / `invalid_repo_session_types`; target not admitted; request is `Ok(response)` not transport `Err`.
2. **Positive control:** complete PackageSessionType Create succeeds; ListSessionTypes returns qualified id.
3. **List control with poison:** after admitting valid then invalidating file on disk, ListSessionTypes returns typed `invalid_repo_session_types` (no disconnect).
4. **Update reject:** enable or repoint to invalid session-types rejects framed without disconnect.
5. **Poison recovery (finding_1786071655_384699 + finding_1786072057_715920):** two independent cases from a freshly poisoned state — (a) admit valid → poison → Delete succeeds with no prior disable; (b) admit valid → poison → Update `enabled=false` succeeds. Sequencing disable-then-delete is vacuous because disable unpoisons before Delete runs.
6. **Post-reject usability:** after rejected create, further daemon requests still work.
7. **Ablation:** fix reverted → create path fails old way; recovery path blocked or disconnects — recorded.
8. **Focused tests:** `./test.sh --test hub_daemon_lifecycle_test <new filters>` (and any new unit tests).
9. **Workspace tests:** `./test.sh` (or workspace form required by repo) green on post-cutover pin.
10. **Strict Rust gates (finding_1786071655_429309):**
    - `cargo clippy --workspace --all-targets --all-features -- -D warnings` (zero diagnostics)
    - `cargo fmt --all -- --check`

**Downstream proof**

- Not required inside this hub ticket to re-run botster-tui live Workspaces acceptance.
- After hub fix merges and consumers pin the new hub revision, stale-fixture create should surface typed operator error (or succeed with completed fixtures). Follow-up observation, not a cross-repo dependency.

**Runtime path proof**

- Production entry: local client → daemon socket → `handle_control_request` → Create/Update/Delete spawn targets.
- Tests must use real daemon child / `daemon_transport_request` as existing spawn-target CRUD tests do — not scaffold-only.

## Implementation sequence (for Implement)

1. Rebase onto `origin/main` (`a35a0cca…` or newer); confirm core pin `363602b…`.
2. Extract or call shared repo session-types validation for a root.
3. Pre-validate enabled create/update before mutate.
4. Fix `session_type_entity_snapshot` error mapping to SessionType/Client operator errors.
5. Wire recovery: Delete + disable-Update proceed when pre-map fails with `invalid_repo_session_types`; force generation advance.
6. Add create-reject + poison-recovery + update-reject daemon tests; record ablation.
7. One-line docs only if needed.
8. Run acceptance items 8–10; attach command evidence.

## Pipeline gates and artifacts

- Plan artifact: this file (rev 2) + updated `project_pipelines_add_artifact` payload.
- Plan gate: `botster_stack_plan_gate` with required fields.
- Addresses open findings: `finding_1786071654_872881`, `finding_1786071655_384699`, `finding_1786071655_588898`, `finding_1786071655_429309`.

## Vault gaps worth capturing

| Item | Action |
| --- | --- |
| Inbox gotcha documents disconnect symptom | After fix, promote/refresh with UnexpectedResponse site + pre-admission + **recovery path** (disable/delete under poison) |
| Erasing SessionTypeError into UnexpectedResponse | Optional durable note if more call sites found |
| Poisoned admitted target recovery | Capture if not already covered when promoting the inbox note |

## Convention conflicts

**None.** Aligns with [[daemon request errors should return operator frames without dropping transport]], [[botster-hub-playbook]] (including strict Rust gates), and no Validate speculation.

## Product decision ledger (compact)

- **Default:** invalid repo session-types reject **enabled** create/update with `invalid_repo_session_types`; transport stays up.
- **Default:** share ListSessionTypes validation; no second schema.
- **Default:** never map SessionTypeError to UnexpectedResponse on the control path.
- **Default:** already-admitted poison remains ListSessionTypes hard-fail, but **Delete and disable succeed** as the daemon recovery path; force-advance generation.
- **Default:** Implement/acceptance on main tip with core pin `363602b…`.
- **Default:** clippy `-D warnings` + `fmt --check` are must-pass.
- **Non-goal:** ValidateSpawnTarget status extension; soft-fail multi-target list; client fixtures; protocol bumps.
- **Ask-human threshold:** only if recovery semantics for delete/disable under poison prove to conflict with an explicit operator product decision not visible here — none known.
