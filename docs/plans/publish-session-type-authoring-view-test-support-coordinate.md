# Plan: Hub release — publish the session-type authoring-view test-support coordinate

**Revision 2** — addresses Plan Review `review_1786060990_570228` (`changes_required`).

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `trybotster/botster-hub` |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Base path | `/Users/jasonconigliari/Projects/botster-hub` |
| Worktree | `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1786042460_231768` |
| Branch | `project-pipelines/ticket_1786042460_231768` |
| Base ref | `main` @ `6ad6dfadef61cccd559fecc6536f2d391888cac2` (includes PR #195 / `ticket_1786039258_173310`) |
| Ticket | `ticket_1786042460_231768` |
| Run | `run_1786060049_244982` |
| Pipeline | `botster_stack_delivery` / step `botster_stack_plan` |

Resolved from `project_pipelines_current_context` + `botster context` (`target_repo`, `target_id`). Not inferred from the ambient process CWD alone.

## Repository playbook loaded

- [[botster-hub-playbook]] — exact ownership charter for `botster-hub`

## Other role/surface playbooks and atomic notes loaded

### Role / stack entrypoints

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster playbooks compose role with changed surface overlays]] — package-surface selection for this npm release

### Targeted atomic notes (ticket-implicated)

- [[hub generated protocol changes are a four site release chain]] — this ticket is site 3 (publish); sites 1–2 already merged; integrity + external content proof (not metadata self-report alone)
- [[closed dependency tickets signal merged source not a consumable release]] — closed source ticket ≠ consumable npm artifact
- [[hub test support npm releases need external consumer smoke]] — clean external install is required proof
- [[conformance fixture revisions must be unique per published content]] — re-check published revision uniqueness before releasing
- [[daemon event shape changes bump conformance fixture revision not protocol version]] — protocol stays 6; conformance already 32 in source
- [[botster hub client crate is the external client boundary]]
- [[botster first party client support matrices belong in hub test support]]
- [[published capability matrices must derive enumerations from source]]
- [[botster local client api lives over hubruntime not raw core routers]] — no re-open of admission design in this release
- [[sanitized projection plus wholesale replacement update contracts silent data loss]] — authoring-view contract already implemented upstream
- [[editor scoped reads sit in the mutation admission group not the sanitized read group]] — inherited; not reworked here
- [[hub qualifies effective session type ids as source name slash id]] — selection semantics inherited
- [[implementation steps must persist report artifacts for review]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[vault example paths are not repository placement conventions]]

### Intentionally not loaded

- [[project-pipelines-playbook]] — no Project Pipelines package/plugin path or workflow-policy change in this ticket
- Other repository charters (`botster-core`, `botster-web`, `botster-tui`, …) — not the target; downstream pin work stays on those targets

### Surface overlays for later stages (not re-planned here)

- Package surface: [[botster-package-reviewer-playbook]] / [[botster-package-verifier-playbook]]
- Runtime surface only if implementer re-touches daemon code (should not be needed)

## Context loaded

### Pipeline

- Dependency `ticket_1786039258_173310` (“Hub: publish a lossless authoring view…”) status **closed**
- Plan Review `review_1786060990_570228` verdict **changes_required** with six findings (one high, two medium, one low, two info)
- Gate `botster_stack_plan_gate` requires a repository-routed plan with the required fields
- This revision incorporates the review’s required fixes before re-advancing

### Plan Review findings disposition

| ID | Severity | Disposition |
| --- | --- | --- |
| `finding_1786060990_338839` | high | **Fixed in plan** — package README is required in scope (ships in tarball) |
| `finding_1786060990_305088` | medium | **Fixed in plan** — step 0 restore `.gitignore`; runnable package test command; porcelain empty before dry-run/publish |
| `finding_1786060990_590842` | medium | **Fixed in plan** — exact matrix field assertion; protocol.ts absence of refusal token is not a failure |
| `finding_1786060990_728058` | low | **Fixed in plan** — direct SHA-256 of installed `daemon-protocol.ts` |
| `finding_1786060990_506429` | info | **Accepted** — `docs/client-protocol.md` and root `README.md` stay out of scope |
| `finding_1786060990_437494` | info | **Accepted** — npm 401 fail-closed is implementer’s first action; dry-run ≠ publish |

### Source readiness (merged main)

On `main` / this worktree HEAD `6ad6dfa`:

| Check | Local (merged) | Published `@trybotster/hub-test-support@0.1.24` |
| --- | --- | --- |
| `package.json` version | `0.1.24` | `0.1.24` |
| `protocol_version` | `6` | `6` |
| `conformance_fixture_revision` | `32` | `31` |
| `daemon_protocol.sha256` | `fb441d038011b940db43618864bfab061bdd5baf586bfe274eea3270d3e46d69` | `c5cc9413a546ddde344ed50021df8024df266d7bdd24008115258e633388599f` |
| `show_session_type_definition` request | present in `daemon-protocol.ts` | **absent** as a request type |
| `session_type_authoring` matrix section | present (`request_type`, `read_only_error_kind`, etc.) | not the rev-32 content |
| Package README install pin | still names `0.1.24` | tarball ships README naming `0.1.24` |

Published registry versions: `0.1.0`–`0.1.14`, `0.1.16`–`0.1.18`, `0.1.20`, `0.1.21`, `0.1.24` (latest). Newest published coordinate is **0.1.24**. Gaps (`0.1.15`, `0.1.19`, `0.1.22`, `0.1.23`) are **not** “above newest” and must not be reused. Plan Review independently confirmed `0.1.25` is free and revision 32 is unique across published + unmerged remotes.

`@trybotster/ui-contract@0.3.1` is already published and is the current hub-test-support dependency. This release does not need a ui-contract version bump.

### Refusal token surface (exact)

On merged main, `read_only_session_type_source` appears:

- **Exactly once** in package assets: `first-party-client-support-matrix.json` → `session_type_authoring.read_only_error_kind`
- **Zero times** in `daemon-protocol.ts` (generated TS models error kind as a plain string; absence there is correct, not drift)

External smoke must assert the **matrix field**, not hunt the protocol file.

### Publish tooling

- Operator path: `script/publish-npm-packages` (and `--dry-run`)
- Script requires a **clean** git worktree (`git status --porcelain` empty, including untracked); packs both packages; validates registry `dist.integrity` before skip; publishes dependency-order
- `require_clean_worktree` treats untracked files as dirty — so `target/` and `node_modules/` must be gitignored before any build/install
- Current agent environment: `npm whoami` → **401 Unauthorized** (`~/.npmrc` has a token that is rejected; expired or wrong scope — not “missing file”)

### Prior art destinations (repo-owned)

- Plans: `docs/plans/`
- Release reports / evidence: `docs/reports/` (e.g. `hub-test-support-0.1.18-release-evidence.json`, `hub-test-support-0.1.9-session-lifecycle-release.md`)

## Scope

Smallest surgical release of the already-merged authoring-view contract as a **new unused npm coordinate** above `0.1.24`.

### Step 0 — worktree hygiene (before any cargo/npm install)

1. **`git checkout -- .gitignore`** (or otherwise restore the committed ignore rules). This is what re-ignores `target/`, `node_modules/`, `.env`, and `mise.local.toml` so `require_clean_worktree` can pass. Do **not** treat “leave session files untracked without ignore rules” as sufficient — porcelain reports untracked files.
2. Confirm session-only files (`.env`, `mise.local.toml`) remain untracked and ignored; do not commit them.
3. Assert `git status --porcelain` shows only intentional product changes after each commit checkpoint (and empty product noise before dry-run/publish).

### Release body

1. **Allocate** an unused package version **strictly above** newest published (`0.1.24`). Default candidate: **`0.1.25`**. Re-check with `npm view` immediately before bump; if taken, pick the next free patch above it. Call this version `<ver>` below.
2. **Bump / update** (all required — not conditional):
   - `packages/hub-test-support/package.json` `version` → `<ver>`
   - `packages/hub-test-support/README.md`:
     - Usage install line (currently line 22): pin both packages, with hub-test-support at `<ver>`  
       `npm install --save-dev @trybotster/ui-contract@0.3.1 @trybotster/hub-test-support@<ver>`
     - Exact package-spec JSON example (currently lines 79–85): `"@trybotster/hub-test-support": "<ver>"`
     - Prepared-coordinate sentence (currently lines 87–89): rename to state `@trybotster/hub-test-support@<ver>` is the prepared/published coordinate for this authoring-view release (wording may say “prepared” pre-publish and be finalized in the report after registry proof; the **tarball README must name `<ver>`**, never `0.1.24`)
   - Regenerated `packages/hub-test-support/metadata.json` via `npm run sync` / `node scripts/sync-assets.mjs` so `package_version` matches `<ver>`
   - Any sync-driven checksum/metadata fields that change solely because of the version field (no protocol rewrites)
3. **Preserve** `PROTOCOL_VERSION` / metadata `protocol_version` **6** and `CONFORMANCE_FIXTURE_REVISION` / `conformance_fixture_revision` **32** (already on main).
4. **Preflight dry-run**: after a commit that leaves `git status --porcelain` empty, run `script/publish-npm-packages --dry-run`.
5. **Publish**: `script/publish-npm-packages` (preferred). Expect `@trybotster/ui-contract@0.3.1` skip-after-integrity-match; publish only the new hub-test-support coordinate. **Dry-run success is not publication.**
6. **Prove from a CLEAN EXTERNAL INSTALL** (not hub checkout, not local pack alone as final proof) — see Acceptance checks.
7. **Record** published coordinate, `dist.integrity` / shasum, tarball URL, exact Hub commit SHA, operator identity if a human published, and both the metadata-stated and directly computed daemon-protocol SHA-256 in `docs/reports/`.
8. **Commit** version bump + package README + metadata + report via the normal PR path (`merge_policy: pr`).

### Runtime / user path this ticket changes

This ticket’s production entry point is the **public npm registry coordinate** consumed by first-party clients (notably botster-web’s hub-test-support pin). It does **not** change Hub daemon runtime behavior beyond what `ticket_1786039258_173310` already merged. Success is proven by clean external install of the published tarball — including the **shipped package README pin** and the **installed protocol file bytes** — not by re-running daemon lifecycle tests unless the version bump unexpectedly regenerates protocol bytes.

## Non-scope

- Re-implementing or redesigning `show_session_type_definition`, admission grouping, selection semantics, or sanitized list/show rows
- Bumping `PROTOCOL_VERSION` (must stay 6 so tui/web do not hard-break on exact equality)
- Changing conformance fixture content or revision beyond what main already has (32)
- Publishing or republishing `@trybotster/ui-contract` unless integrity validation fails (then stop and ask human — do not silently rewrite)
- botster-web / botster-tui pin updates (`ticket_1786039279_917823` and peers stay on their targets)
- Core / workspaces / TUI / Ghostty code
- **`docs/client-protocol.md` historical/drifting coordinate mentions** (six versions mixed with narrative — no per-release sync convention; leave for a separate docs ticket)
- **Root `README.md` stale `@trybotster/hub-test-support@0.1.23` claim** (never published; does not ship in the npm tarball)
- Speculative tooling, release automation, or multi-package refactors
- Adding `read_only_session_type_source` to `daemon-protocol.ts` (out-of-scope emitter change; token lives on the support matrix)

## Repository ownership boundaries and cross-repo dependencies

| Boundary | Owner | This ticket |
| --- | --- | --- |
| Host profile, client API, session-type authoring read (already merged) | `botster-hub` | consume only |
| DTO / generated TS / hub-test-support npm package (including shipped README) | `botster-hub` (+ in-repo `botster-hub-client`) | **publish site** |
| Policy-free core mechanisms | `botster-core` | none |
| Web edit control consuming published coordinate | `botster-web` | **downstream only** after this coordinate exists |
| TUI protocol pin | `botster-tui` | no repin required if protocol stays 6 |

### Dependencies

| Ticket | Role | Status | Implication |
| --- | --- | --- | --- |
| `ticket_1786039258_173310` | Source contract (sites 1–2 of four-site chain) | **closed / merged** @ `6ad6dfa` (PR #195) | Prerequisite satisfied for *source*; not for *registry* |
| `ticket_1786039279_917823` (Web edit control) | Downstream consumer | blocked on **this** release, not on source alone | Do not treat Web as in-scope here |

No new cross-repo dependency registration required for this run. Do not broaden this run into Web.

## Assumptions and unknowns

### Assumptions (explicit)

1. Candidate coordinate is `@trybotster/hub-test-support@0.1.25` unless `npm view` shows it taken at implement time.
2. Merged main content at rev 32 is the intended publish payload; no further protocol feature work belongs on this ticket.
3. `script/publish-npm-packages` is the only sanctioned publish path (integrity skip guard is load-bearing after the 0.1.17→0.1.18 incident).
4. ui-contract `0.3.1` remains correct; hub package dependency stays `"@trybotster/ui-contract": "0.3.1"`.
5. Session-local dirty files are not product changes; **restoring `.gitignore` first** is the mechanism that keeps them out of porcelain.
6. Pipeline merge policy remains PR-based; version bump + package README + evidence report land via PR even though npm publish is a registry side effect.
7. Package README install-pin sync on every version bump is established repo practice (history: 0.1.18 → 0.1.24); skipping it ships wrong consumer instructions inside an immutable tarball.

### Unknowns / human gates

1. **npm authentication**: `npm whoami` fails with 401 despite `~/.npmrc` token presence (expired/wrong scope). **Implementer action 1 (after .gitignore restore):** re-check `npm whoami`. If still failing, call `project_pipelines_ask_human` for operator publish rather than inventing an alternate distribution channel. Record operator identity in the evidence report if a human publishes.
2. Whether org publish requires 2FA/provenance flags beyond `npm publish --access public` (script uses that form today).
3. Exact final patch number if concurrent publishes claim 0.1.25 between plan and implement.

## Affected surfaces/files

### Expected edits (required)

- `packages/hub-test-support/package.json` — version bump to unused coordinate above 0.1.24
- `packages/hub-test-support/README.md` — install pin, package-spec example, prepared-coordinate sentence → `<ver>`
- `packages/hub-test-support/metadata.json` — regenerated `package_version` (+ any version-tied fields)
- `docs/plans/publish-session-type-authoring-view-test-support-coordinate.md` — this plan (rev 2)
- `docs/reports/hub-test-support-<ver>-release.md` and/or `docs/reports/hub-test-support-<ver>-release-evidence.json` — published coordinate, integrity, Hub commit, external smoke evidence

### Worktree hygiene (not a product commit unless already dirty)

- `.gitignore` — **restore to HEAD** if emptied/modified by the session; do not leave ignore rules deleted

### Touch only if sync requires (should be no-op for content)

- Other `packages/hub-test-support/*` assets — only if re-sync rewrites hashes; content should already match main

### Must not change (unless accidental drift found — then stop)

- `crates/botster-hub-client/**` protocol / `PROTOCOL_VERSION` / `CONFORMANCE_FIXTURE_REVISION`
- `src/session_types.rs`, `src/client_api.rs`, `src/daemon_transport.rs`
- `packages/ui-contract/**` version or contents
- `docs/client-protocol.md`, root `README.md` (stale coords stay for a separate docs ticket)
- botster-web / botster-tui trees

## Risks

| Risk | Mitigation |
| --- | --- |
| Immutable wrong README pin (0.1.25 tarball still names 0.1.24) | **Required** README update in scope; external smoke asserts installed README contains `@trybotster/hub-test-support@<ver>` |
| Republish collision on `0.1.24` with different bytes | Never reuse 0.1.24; allocate > newest; script integrity check aborts on mismatch |
| Revision uniqueness collision on 32 | Plan Review confirmed unique; re-check at implement time |
| False green from local pack / metadata self-report | Final proof: clean temp install + **direct SHA-256 of installed `daemon-protocol.ts`** + token/matrix asserts |
| `require_clean_worktree` fails after build/install | Step 0 restore `.gitignore` **before** cargo/npm; porcelain empty before dry-run and publish |
| Package tests fail with missing ui-contract | Document install prerequisite (or out-of-tree suite) before `npm test` |
| Mis-asserting refusal token in `daemon-protocol.ts` | Exact matrix field assertion only; document protocol.ts absence as expected |
| npm auth missing / stale token | Early `npm whoami`; ask human; dry-run never substitutes for publish |
| Accidental protocol bump | Explicit assert protocol 6 before and after; ticket forbids bump |
| Treating closed source ticket as done for Web | Keep Web ticket blocked on **this** coordinate’s external smoke |

## Acceptance checks/tests

### Pre-publish (source + package)

0. **Hygiene first:** `git checkout -- .gitignore` (if dirty). Confirm `git check-ignore -v target node_modules .env mise.local.toml` matches ignore rules. Session files stay ignored.
1. `npm whoami` succeeds **or** human operator engaged before any publish attempt. Record result. Do not treat later dry-run success as auth.
2. `npm view @trybotster/hub-test-support versions --json --prefer-online` — `<ver>` unused; newest still considered.
3. `npm view @trybotster/hub-test-support@0.1.24` still reports conformance 31 (or document if registry moved).
4. After version + README bump: `node packages/hub-test-support/scripts/sync-assets.mjs --check` (exit 0; assets current).
5. **Package suite (runnable form)** — either:
   - From `packages/hub-test-support` after installing the dependency:  
     `npm install --no-save --package-lock=false @trybotster/ui-contract@0.3.1` then `npm test`  
     (with restored `.gitignore` so `node_modules/` does not dirty porcelain), **or**
   - Copy the package tree to a disposable out-of-tree directory, install ui-contract there, run `npm test` there.  
   Suite must pass (asserts `show_session_type_definition` tokens, rev 32, protocol 6).  
   **Bare `npm test` without ui-contract installed is not an accepted check** — it fails with `ERR_MODULE_NOT_FOUND` on a clean checkout.
6. Optional focused safety: `./test.sh -p botster-hub-client` and `./test.sh -p botster-hub-test-support` if any non-metadata/Rust file drifts; full `./test.sh` only if implementer changes Rust (should not).
7. Commit product changes. Assert **`git status --porcelain` is empty**. Then `script/publish-npm-packages --dry-run` exit 0; capture local tarball integrity.
8. Immediately before real publish: assert **`git status --porcelain` is empty** again.

### Publish

9. `script/publish-npm-packages` publishes `@trybotster/hub-test-support@<ver>`; ui-contract skipped only after integrity match. Record publisher (`npm whoami` or human operator name).
10. `npm view @trybotster/hub-test-support@<ver> version dist.integrity dist.tarball --prefer-online` matches local packed integrity.

### Downstream / external consumer proof (required; not optional)

In a **fresh temp directory outside the hub repo**, with no `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` override and no path dependency:

```text
npm init -y
npm install @trybotster/hub-test-support@<ver> --prefer-online
```

Assert **all** of the following (no hedges):

| # | Assertion |
| --- | --- |
| E1 | ESM import of `@trybotster/hub-test-support` succeeds |
| E2 | `metadata.package_version === "<ver>"` |
| E3 | `metadata.protocol_version === 6` |
| E4 | `metadata.conformance_fixture_revision === 32` |
| E5 | `verifyPackageAssets()` succeeds (self-consistency) |
| E6 | `readDaemonProtocolTypescript()` contains `show_session_type_definition` and `session_type_definition` response vocabulary / editable definition types |
| E7 | **Direct content hash (independent of metadata self-report):**  
    `shasum -a 256 node_modules/@trybotster/hub-test-support/daemon-protocol.ts`  
    equals  
    `fb441d038011b940db43618864bfab061bdd5baf586bfe274eea3270d3e46d69`  
    (recompute expected from the release commit’s file if a legitimate re-sync changes it; record both expected and actual). Also record `metadata.daemon_protocol.sha256` and require it equals the same value. |
| E8 | `readFirstPartyClientSupportMatrix().session_type_authoring.request_type === "show_session_type_definition"` |
| E9 | `readFirstPartyClientSupportMatrix().session_type_authoring.read_only_error_kind === "read_only_session_type_source"`  
    **Do not** require this string in `daemon-protocol.ts` — its absence there is expected. |
| E10 | **Installed package README** (`node_modules/@trybotster/hub-test-support/README.md`) contains the published coordinate string `@trybotster/hub-test-support@<ver>` (and does not instruct consumers to install `@0.1.24` as the authoring-view pin). |

### Evidence artifact must include

- Published coordinate string
- `dist.integrity` and tarball URL
- Exact Hub git commit SHA used for the release tree
- Publisher identity (`npm whoami` or human operator)
- Commands + pass/fail for dry-run, publish, and clean external install (E1–E10)
- Direct SHA-256 of installed `daemon-protocol.ts` **and** metadata-stated hash
- Explicit statement that local pack alone was **not** accepted as final consumable proof
- Explicit statement that dry-run alone was **not** accepted as publication

## Implementation sequence (for Implement)

0. `git checkout -- .gitignore` if dirty; confirm ignore rules for `target/`, `node_modules/`, `.env`, `mise.local.toml`.
1. `npm whoami` — if 401, `project_pipelines_ask_human` before any publish; may still prepare version bump + dry-run once auth or human publisher is arranged.
2. Re-run registry preflight; lock `<ver>` (default 0.1.25).
3. Bump `package.json` version; update **package README** pins (install line, JSON example, prepared-coordinate sentence); `npm run sync` / check.
4. Install ui-contract for package tests (or out-of-tree copy); `npm test` in hub-test-support.
5. Commit version + README + metadata. Assert porcelain empty.
6. `script/publish-npm-packages --dry-run`.
7. Assert porcelain empty; `script/publish-npm-packages` (or human-run with identity recorded).
8. Clean external install smoke E1–E10; write `docs/reports/hub-test-support-<ver>-release*`.
9. Commit report; open/stack PR per pipeline policy.
10. Do **not** close downstream Web ticket from this run; do **not** sweep `docs/client-protocol.md` or root README.

## Vault gaps worth capturing

1. **Release ticket template for hub-test-support**: “restore ignore rules → bump above newest published → **sync package README pin (ships in tarball)** → sync metadata version → runnable package test with ui-contract → porcelain empty → dry-run script → publish → clean external install with protocol hash + matrix fields + README pin asserts.”
2. **Pipeline agent npm auth posture**: fail closed on `npm whoami` (including present-but-rejected tokens); dry-run ≠ release.
3. **Published package README is part of the shipped contract** for version pins (if not already covered by [[published fixture readmes are part of the shipped contract]]).

Convention conflicts: **none**. Durable knowledge captured this step: plan rev 2 only; no new vault note authored yet.

## Botster layers touched

- **Node package distribution** (`packages/hub-test-support`) — version, **shipped README**, publish
- **Docs/reports** — release evidence
- Not touched: Lua plugin, Rust hub daemon policy, SPA, TUI, MCP, Rails relay, repo-root narrative docs

## Worktree / target assumptions

- All work in target `tgt_7e208a0c76a44980a83b63af976b1f22` worktree on branch `project-pipelines/ticket_1786042460_231768`
- Base is current `main` containing PR #195
- Agents must not publish from a different ambient checkout

## Pipeline gates and artifacts

| Stage | Artifact / gate |
| --- | --- |
| Plan | This file (rev 2); gate `botster_stack_plan_gate` |
| Implement | Version + README + metadata commit; dry-run + publish logs; external smoke E1–E10; `docs/reports/hub-test-support-<ver>-release*` |
| Plan Review / Review / Verify | Package surface overlays; require external install proof including README pin and direct protocol hash |

## Required docs updates

- **`packages/hub-test-support/README.md` — required** (ships in tarball; pin must move with `<ver>`)
- Release evidence under `docs/reports/` — required
- `docs/client-protocol.md` / root `README.md` — **not** required this run

---

## Plan checklist summary

| Required plan field | Covered |
| --- | --- |
| Target repository / target_id | yes |
| Repository charter | [[botster-hub-playbook]] |
| Playbooks/notes | listed above with exact titles |
| Scope / non-scope | release + **required package README**; no protocol redesign; no narrative doc sweep |
| Ownership / cross-repo deps | hub publish; Web depends on this ticket |
| Assumptions / unknowns | version 0.1.25; npm auth 401 fail-closed first |
| Affected surfaces/files | package.json, **README.md**, metadata, reports |
| Risks | immutable wrong pin, clean-tree guard, false local proof, auth |
| Acceptance checks | dry-run, publish, clean external install E1–E10 |
| Vault gaps | release template includes README pin; agent npm auth |
