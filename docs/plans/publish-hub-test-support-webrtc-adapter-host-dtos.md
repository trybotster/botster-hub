# Plan: publish hub-test-support with WebRTC adapter host DTOs

**Revision 2** — addresses Plan Review `review_1786725170_922494` (`changes_required`).

Product finding `finding_1786725170_664633`: build the complete intended publish tree, including the README prior-coordinate correction, and pack that tree before any occupancy decision. Compare registry integrity only to that corrected tarball. If occupied `0.1.35` differs, allocate the next unused patch. The installed package must contain the corrected prior-coordinate statement.

Process finding `finding_1786725170_644718`: this revision resubmits `plan_uri`, `artifact_id`, `checklist_id`, `target_id`, and `target_repository` on both the gate and the step-advance evidence. No new vault checklist.

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `trybotster/botster-hub` |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn target name | `botster-hub` |
| Ticket | `ticket_1786723348_522242` |
| Run | `run_1786724333_797392` |
| Pipeline | `botster_stack_delivery` / step `botster_stack_plan` |
| Authoritative HEAD at plan time | `24517f4879a6effdd87eacddbb4b40aca13104c1` (`origin/main`) |

Resolved from `project_pipelines_current_context` plus `list_spawn_targets`. Not inferred from the ambient process directory.

## Repository playbook loaded

- [[botster-hub-playbook]] — ownership charter for `botster-hub`

## Other role/surface playbooks and atomic notes loaded

### Role and stack entrypoints

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster playbooks compose role with changed surface overlays]] — this ticket is a packages/release surface, not a runtime or web surface
- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]] — loaded as planner overlay; no SPA code changes in this ticket
- [[project pipeline orchestration belongs in a device-level botster plugin]]
- [[project pipelines needs an operator workbench not more primitives]]
- [[project pipelines ui contract belongs in the plugin readme]]
- [[botster orchestration should spawn agents with explicit target ids]]
- [[botster orchestration prompts must bind agents to explicit worktrees]]

`[[project-pipelines-playbook]]` is not loaded. This ticket does not change Project Pipelines package or plugin paths.

`[[botster runtime teardown lenses]]` is not loaded. This ticket publishes already-synced host-plane DTO tokens. It does not change WebRTC peer lifecycle, SessionIo/ClientWorker teardown, multi-peer ownership, resource spin, or terminal-state versus live-runtime behavior.

### Targeted atomic notes

- [[hub generated protocol changes are a four site release chain]] — this ticket is site 3
- [[closed dependency tickets signal merged source not a consumable release]]
- [[hub test support npm releases need external consumer smoke]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[ready then history cutover uses Hub test support version 0.1.33]] — records why public latest is still `0.1.33`
- [[conformance fixture revisions must be unique per published content]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[published fixture readmes are part of the shipped contract]]
- [[botster first party client support matrices belong in hub test support]]
- [[published capability matrices must derive enumerations from source]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[generated typescript dtos must encode serde field optionality]]
- [[botster hub client crate is the external client boundary]]
- [[botster hub is a first party host profile over core]]
- [[vault example paths are not repository placement conventions]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[plan steps need reviewable plan artifacts]]
- [[implementation steps must persist report artifacts for review]]

This ticket is not a consumer of Hub session-type eligibility work. Do not inject parent session-type pins.

## Context loaded

- Return visit after Plan Review `review_1786725170_922494` (`changes_required`).
- Product finding: occupied `0.1.35` must not be accepted against the current stale-README tree. Process finding: resubmit required identifiers on gate and advance evidence. Do not create another checklist.
- Project: Botster Terminal Transport North Star. Direct-merge. Do not create a pull request.
- Authoritative spawn target `tgt_7e208a0c76a44980a83b63af976b1f22` is `trybotster/botster-hub`.
- Assigned worktree started at `origin/main` `24517f4`. Tracked `.gitignore` is present and non-empty. The worktree path has no colon.
- Hub source `packages/hub-test-support` is already `@trybotster/hub-test-support@0.1.35`, protocol 7, conformance fixture revision 40, ui-contract `0.3.2`.
- Public npm latest at Plan time is `0.1.33` (protocol 7, revision 38, ui-contract `0.3.2`). Versions `0.1.34` and `0.1.35` were unpublished then. Re-check at Implement time.
- Published `0.1.33` `daemon-protocol.ts` has `DaemonLocalWebrtcDeliveryKind` but lacks `daemon_terminal_frame`, `terminal_compatibility`, `webrtc_terminal_adapter`, and `terminal_subscription_closed`.
- Source `0.1.35` `daemon-protocol.ts` has `DaemonLocalWebrtcDeliveryKind` including `daemon_terminal_frame`, optional `DaemonHello.terminal_compatibility`, optional `DaemonHelloAck.terminal_compatibility`, and `terminal_subscription_closed`.
- Source `first-party-client-support-matrix.json` lists optional `webrtc_terminal_adapter` and `terminal_subscription_closed` under `supported_features`, not `required_features`.
- Shipped README still says `0.1.32` is the prior published coordinate at the same protocol and revision. That sentence is part of the published contract.
- `npm whoami` returned HTTP 401 in the first Plan visit.
- Downstream Web ticket `ticket_1786661008_897067` already depends on this ticket (`dependency_1786724311_472762`).
- Sibling tickets stay out of this run: Core `ticket_1786723347_177328` and Hub `ticket_1786724303_284888`.
- Vault checklist `checklist_1786724715_318556` already exists for this ticket. Skip a new checklist.

## Scope

Publish a new unused `@trybotster/hub-test-support` coordinate from the already-synced Hub main assets plus the required README correction.

Preferred coordinate: `0.1.35`, only when that version is unused or already equals the corrected tarball.

1. Build the complete intended publish tree first. Correct the shipped README sentence that still calls `0.1.32` the prior published coordinate at the same protocol and revision. The prior published coordinate is `0.1.33` at protocol 7 / revision 38. This tree is revision 40.
2. Re-run package tests on that corrected tree. Do not make the occupancy decision against the stale-README bytes.
3. Pack the corrected tree. Record that local tarball integrity. Use `script/publish-npm-packages --dry-run` or an equivalent pack of the same tree.
4. Then re-check registry occupancy.
5. If `0.1.35` is unused, publish the corrected tarball as `0.1.35`.
6. If `0.1.35` is occupied and `dist.integrity` equals the corrected tarball, treat publication as done. Still run the external content smoke, including the README prior-coordinate check.
7. If `0.1.35` is occupied and `dist.integrity` differs from the corrected tarball, allocate the next unused patch (`0.1.36` or later). Bump `package.json`, regenerate `metadata.json`, update README pin sites and `test.mjs`, pack again, and publish that unused coordinate. A stale-README `0.1.35` is a mismatch. Do not accept it as done.
8. Keep `protocol_version` 7 and `@trybotster/ui-contract@0.3.2`.
9. Publish with `script/publish-npm-packages`. Do not skip an already-published coordinate on version-exists alone.
10. If `npm whoami` fails, ask a blocking human. Do not invent a file, git, or `/tmp` coordinate.
11. After publish, prove a clean external install of the registry coordinate.
12. Persist a report under `docs/reports/` with coordinate, integrity, Hub SHA, and smoke evidence.
13. Merge directly into `main`. Do not create a pull request.

## Non-scope

- Do not change terminal adapter policy, admission, grants, framing, encryption, or teardown.
- Do not inspect READY, PAGE, FINISH, Snapshot, or other terminal bodies.
- Do not edit `botster-web`, `botster-tui`, `botster-core`, or `@trybotster/terminal-protocol`.
- Do not republish or mutate `0.1.33` or any other published version.
- Do not bump `PROTOCOL_VERSION`.
- Do not change `@trybotster/ui-contract` away from `0.3.2`.
- Do not emit `TerminalSubscriptionClosed` on WebRTC. That is `ticket_1786724303_284888`.
- Do not delete Hub-owned goldens or consume Core protocol fixtures. That is `ticket_1786664495_777899`.
- Do not rewrite historical `docs/client-protocol.md` coordinate sentences.

## Repository ownership boundaries and cross-repo dependencies

| Boundary | Owner | This ticket |
| --- | --- | --- |
| Host control DTOs and npm `@trybotster/hub-test-support` | botster-hub | publish site 3 |
| Generated TypeScript emitter | botster-hub-client inside this repo | consume already-synced bytes |
| Core terminal protocol package | botster-core | sibling `ticket_1786723347_177328`; do not absorb |
| Web pin and vendored protocol | botster-web | downstream `ticket_1786661008_897067`; already registered |
| WebRTC adapter-close host event | botster-hub runtime | sibling `ticket_1786724303_284888`; do not absorb |

No new cross-repo dependency is required on this ticket. The Web consumer already depends on this Hub target.

## Assumptions and unknowns

- Assumption: `0.1.35` on `origin/main` is the intended publish tree. Sites 1 and 2 of the four-site chain already landed.
- Assumption: `webrtc_terminal_adapter` is the optional host feature token in the shipped support matrix and README. It is not a TypeScript type name in `daemon-protocol.ts`. Acceptance greps the installed tarball, including the matrix.
- Assumption: optional `terminal_compatibility` fields must stay optional in the published TypeScript.
- Assumption: public npm remains the approved distribution path.
- Unknown: npm authentication in the Implement environment. Plan observed HTTP 401. Implement must ask a human if publish is unauthorized.
- Unknown: whether `0.1.35` remains unpublished at Implement time. Re-read the registry only after packing the corrected tree.

## Affected surfaces/files

Botster layers touched: Hub-owned Node package distribution and release docs.

Likely files:

- `packages/hub-test-support/README.md` — prior-coordinate sentence only, unless a version reallocation is required
- `docs/plans/publish-hub-test-support-webrtc-adapter-host-dtos.md` — this plan
- `docs/reports/publish-hub-test-support-webrtc-adapter-host-dtos-implement-report.md` — Implement report
- optional evidence JSON beside that report

Touch these files only if occupied `0.1.35` differs from the corrected tarball:

- `packages/hub-test-support/package.json`
- `packages/hub-test-support/metadata.json`
- `packages/hub-test-support/test.mjs`

Do not touch Rust adapter, daemon, or emitter files unless `node packages/hub-test-support/scripts/sync-assets.mjs --check` fails. If that check fails, stop and ask a human. This ticket is not a source-sync ticket.

## Risks

- Publishing without an external content smoke can ship a self-consistent stale tarball. Mitigation: clean-dir install and token grep after registry visibility.
- Skipping because the version exists can hide a bad publish. Mitigation: integrity compare against the corrected tarball only, then extra unused version on mismatch.
- npm auth can block publish. Mitigation: blocking human question. No unapproved fallback coordinate.
- Another publisher can occupy `0.1.35` with stale-README bytes between Plan and Implement. Mitigation: always pack the README-corrected tree first. Treat any integrity mismatch, including a stale-README `0.1.35`, as a new unused patch.
- A later main commit can change package bytes before publish. Mitigation: publish from a recorded SHA and pack that tree.

## Acceptance checks/tests

`teardown_class_applies`: no.

Production path: botster-web and other external clients install the public npm coordinate. Local Hub source is not the consumable artifact.

1. `node packages/hub-test-support/scripts/sync-assets.mjs --check` passes on the publish tree.
2. `node packages/hub-test-support/test.mjs` passes.
3. Occupancy is decided only after the README-corrected tree is packed. Registry `0.1.35` is accepted only when its `dist.integrity` equals that corrected tarball.
4. `script/publish-npm-packages --dry-run` packs `@trybotster/hub-test-support` at the chosen unused version, or confirms occupied `0.1.35` matches the corrected tarball, and packs `@trybotster/ui-contract@0.3.2` with matching integrity if that ui-contract version is already published.
5. `npm view @trybotster/hub-test-support version` reports a version newer than `0.1.33`.
6. `npm view @trybotster/hub-test-support@<published>` reports `dist.integrity` equal to the locally packed corrected tarball.
7. A clean temporary directory can `npm install @trybotster/hub-test-support@<published> --prefer-online` and then:
   - `metadata.package_version` equals the published version
   - `metadata.protocol_version === 7`
   - `metadata.conformance_fixture_revision === 40`
   - `metadata.ui_contract.package_version === "0.3.2"`
   - `verifyPackageAssets()` succeeds
   - `readDaemonProtocolTypescript()` contains `DaemonLocalWebrtcDeliveryKind`, `daemon_terminal_frame`, `DaemonHello` `terminal_compatibility`, and `DaemonHelloAck` `terminal_compatibility`
   - installed package files contain `webrtc_terminal_adapter` and `terminal_subscription_closed`
   - the installed README names `0.1.33` as the prior published coordinate at protocol 7 / revision 38 and does not claim `0.1.32` is the prior coordinate at the same protocol and revision
8. Do not require a full Hub `./test.sh` unless Implement changes Rust or daemon source.
9. Merge the plan, report, and any README or version files directly into `main`. Do not open a pull request.

## Vault gaps worth capturing

- None required for this visit. The four-site chain, unpublished-version cutover, and external-smoke rules already cover this shape.
- Optional later capture: published `0.1.35` / revision 40 as the host-plane token baseline, after Implement records the integrity and SHA. Do not capture that coordinate before publish.

## Implement sequence

1. Confirm the assigned worktree is based on current `origin/main`. Restore tracked `.gitignore` from HEAD if it is empty. Set a colon-free `CARGO_TARGET_DIR` only if the path contains `:`.
2. Confirm source tokens and `sync-assets.mjs --check`.
3. Correct the README prior-coordinate sentence on the intended publish tree.
4. Run package tests on that corrected tree.
5. Pack the corrected tree and record its integrity. Then run `npm view @trybotster/hub-test-support versions --json --prefer-online`.
6. Decide occupancy only against the corrected tarball. Publish `0.1.35` if unused. Accept occupied `0.1.35` only on exact integrity match. Otherwise allocate the next unused patch and pack again.
7. Check `npm whoami`. Ask a human if unauthorized.
8. Publish with `script/publish-npm-packages`.
9. Run the clean-dir registry smoke, including the installed README prior-coordinate check.
10. Write the Implement report with coordinate, integrity, SHA, and smoke commands.
11. Merge to `main` without a pull request.
