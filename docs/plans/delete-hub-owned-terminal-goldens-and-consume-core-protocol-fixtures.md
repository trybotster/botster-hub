# Plan: Delete Hub-owned terminal goldens and consume Core protocol fixtures

Ticket: `ticket_1786664495_777899`
Run: `run_1786756876_578418`
Step: `botster_stack_plan`
Pipeline: `botster_stack_delivery` (direct merge, no PR)
Depends on closed Core ticket `ticket_1786661004_962658`
Plan **revision 2** after Plan Review `review_1786757835_527911`

## Plan Review corrections (rev 1 → rev 2)

| Finding | Class | Fix |
| --- | --- | --- |
| `finding_1786757836_416580` absence check rejects required Core fixture names | product / high | Split absence vs consume checks. Forbid Hub-local fixture paths and the deleted generator. Require the five Core file names in `build.rs`, `OUT_DIR` includes, provenance, and identity tests. |
| `finding_1786757836_545889` external Git-consumer proof is missing | product / high | After commit, run a clean Cargo crate that depends on `botster-hub-test-support` through Git at the ticket commit, with no Hub workspace `[patch]`. Call the five payload helpers. Prove one `botster-terminal-protocol` identity at `f4f6bf5` and no client-crate edge. |
| `finding_1786757836_892371` disk exhaustion blocked baseline tests | infra / info | Implement must check free disk before cargo/script gates. `errno 28` is not a passing baseline. Restore space and rerun the same commands. |
| `finding_1786757836_443194` duplicate Plan vault checklists | process / low | Keep `checklist_1786757345_721712`. Do not create another Plan vault checklist. |

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn-target name | `botster-hub` |
| Authoritative path | spawn target `botster-hub` |
| Plan worktree | this pipeline worktree; Plan does not mutate product code |
| Worktree hygiene | tracked `.gitignore` has 53 bytes matching HEAD; path has no `:`; no `CARGO_TARGET_DIR` override |
| Base | this checkout `279d828` (`Record hub-test-support 0.1.36 publish evidence.`) |
| Locked Core / protocol crate | `git+https://github.com/trybotster/botster-core.git` rev `f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| Merge policy | direct into `main`; do not create a PR |
| Session-type eligibility consumer | **false** |
| `teardown_class_applies` | **no** |

Independent resolution: `project_pipelines_current_context` ticket/run `target_id` plus `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub`. Routing did not use the process working directory.

## Repository playbook loaded

[[botster-hub-playbook]]

## Other role/surface playbooks and atomic notes loaded

Role / stack:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]] — planner Must Load only. This ticket has no React/SPA edit surface.
- [[project pipeline orchestration belongs in a device-level botster plugin]]
- [[project pipelines needs an operator workbench not more primitives]]
- [[project pipelines ui contract belongs in the plugin readme]]
- [[botster orchestration should spawn agents with explicit target ids]]
- [[botster orchestration prompts must bind agents to explicit worktrees]]
- [[prefer framework and library components over custom solutions]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[vault example paths are not repository placement conventions]]
- [[plan steps need reviewable plan artifacts]]
- [[cross repo dependency registration must use dependency repo target]]
- [[colon worktree paths break cargo dyld library paths]]
- [[hearth gate runs require restoring a pipeline wiped gitignore before attribution]]

Not loaded, with reason:

- [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope
- [[botster runtime teardown lenses]] — this ticket deletes Hub-owned fixture bytes and retargets compile-time includes. It does not change WebRTC, SessionIo/ClientWorker teardown, multi-peer ownership, resource spin, or terminal-state vs live-runtime behavior
- other repository charters — this run stays on `botster-hub`

Targeted notes:

- [[botster hub is a first party host profile over core]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[Core owns the incremental attach phase machine]]
- [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]]
- [[Git-consumed Hub members pin Core protocol by exact revision]]
- [[Cargo Git URL and selector form are part of crate identity]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[conformance fixture revisions must be unique per published content]]
- [[published fixture readmes are part of the shipped contract]]
- [[botster first party client support matrices belong in hub test support]]
- [[external client hub tests use subprocess spawned hub test support]]
- [[proposed Hub terminal tests enforce content blind adapters]] — proposed, not ratified. Do not use it to delete existing identity tests.
- [[authentic GHOSTSNP tests cross the pinned encoder and embedded decoder]]
- [[Hub incremental GHOSTSNP attach passed production proof at Core revision 033cd01]]
- [[blocking dependency premises must be revalidated per consuming crate]]

## Context loaded

Ticket intent: after Core owns the types-only terminal protocol, Hub must stop owning late-attach GHOSTSNP goldens and the Hub generator. Hub tests that still need those bytes must load them from the Core `botster-terminal-protocol` crate coordinate. SHA identity for history vs blank must stay distinct and must match the Core-owned files.

Closed parent: `ticket_1786661004_962658` on Core target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`. Status `closed`. Core `f4f6bf5babe92dfb9241a760c414187f711c2c42` already contains the five files under `crates/botster-terminal-protocol/fixtures/ghostsnp/`. Core SHAs match the current Hub-embedded constants:

| File | SHA-256 | Len |
| --- | --- | --- |
| `late-attach-history-ready-v2.ghostsnp` | `fbcdda31d682a61420251eed68f72e413485f057e3f374c57582955b0316bb6d` | 2838 |
| `late-attach-history-page-v2.ghostsnp` | `b1b65d9d205f10a2cce4384ea15f0b6b20ee07bb3fda8e3bbdb8bd81dffb071f` | 3365 |
| `late-attach-history-finish-v2.ghostsnp` | `6e0bfa87315d3225b0dedaa88387eb37c5cb31922b7891741445114bf19a3085` | 10 |
| `late-attach-blank-ready-v2.ghostsnp` | `06962b11d4a3acfb9b7c52b673a7b476904ddee2dd754b89b190ff82fdcfd0cc` | 1131 |
| `late-attach-blank-finish-v2.ghostsnp` | `a172e2380afec9ba9248735973f18965ee384ec2ae3440dbb4ddf4d5ced9d325` | 26 |

Core docs name this ticket as the Hub cleanup. Core's Hub-safe crate public API does **not** export fixture bytes. Core tests load the files from the crate source tree via `CARGO_MANIFEST_DIR`. Hub must consume those crate-owned files without editing Core and without adding Snapshot-body accessors.

Current Hub ownership to remove:

- `crates/botster-hub-test-support/fixtures/ghostsnp/` — five v2 goldens, two unused v1 leftovers, and a Hub-authored README
- `crates/botster-hub-test-support/tests/generate_incremental_late_attach_frames.rs` — Hub generator that recreates those bytes
- `include_bytes!("../fixtures/ghostsnp/...")` in `conformance_data.rs`
- `Cargo.toml` `include = ["fixtures/ghostsnp/**"]`

Current Hub surfaces that must keep working after retarget:

- Public helpers `late_attach_history_payload_bytes()`, `late_attach_no_history_payload_bytes()`, SHA helpers, and `late_attach_*_events()` wrap opaque `DaemonEvent::Snapshot` payloads through `DaemonOpaqueHistoryPayload::from_bytes`. That is host DTO wrapping, not Snapshot-body inspection.
- Crate tests in `crates/botster-hub-test-support/src/lib.rs` assert SHA identity and import-semantic screen state.
- `tests/hub_test_support_conformance_test.rs` compares Hub event order to Core regression shapes.
- `tests/hub_daemon_lifecycle/sessions.rs` compares a live idle Snapshot to Golden B bytes.
- `@trybotster/hub-test-support@0.1.36` ships `late-attach-history-conformance-fixture.json` generated from the Rust serde scenario. It does **not** ship `.ghostsnp` files.

## Scope

1. Delete Hub Git authority for the late-attach goldens:
   - Delete the five v2 files named in the ticket.
   - Delete leftover unused v1 files `late-attach-history-marker-v1.ghostsnp` and `late-attach-blank-v1.ghostsnp` plus `fixtures/ghostsnp/README.md`. Leaving those files would keep Hub Git as a fixture authority.
   - Delete `crates/botster-hub-test-support/tests/generate_incremental_late_attach_frames.rs`.
   - Remove `fixtures/ghostsnp/**` from the crate `include` list.
2. Make `botster-hub-test-support` consume the Core-owned files from the pinned `botster-terminal-protocol` crate:
   - Add `botster-terminal-protocol` to `crates/botster-hub-test-support/Cargo.toml` with the same `.git` URL and exact rev as the workspace and `botster-hub-client`: `https://github.com/trybotster/botster-core.git` rev `f4f6bf5babe92dfb9241a760c414187f711c2c42`.
   - Add a small crate `build.rs` that locates that package's source via `cargo metadata` and emits compile-time `include_bytes!` paths (or copies the five files into `OUT_DIR` and includes those). Name the five Core file names in that resolver. Do not vendor a second copy into Hub Git.
   - Add `build.rs` to the crate `include` list when removing `fixtures/ghostsnp/**`. Git consumers must still receive the resolver.
   - Point `LATE_ATTACH_*_PAYLOAD` constants and public helpers at those Core-owned bytes. The five public payload helpers that the Git smoke must call are `late_attach_history_payload_bytes`, `late_attach_no_history_payload_bytes`, `late_attach_history_payload_sha256`, `late_attach_no_history_payload_sha256`, and `late_attach_incremental_frame_identity`.
3. Keep Hub public helpers as opaque byte and host-DTO wrappers. Do not add Snapshot phase, history, or payload accessors. Do not depend on `botster-terminal-protocol-client`.
4. Retarget provenance comments and `LateAttachGhostsnpProvenance` so they name the Core protocol crate coordinate and file names, not a Hub generator. Keep SHA and length identity. Do not treat Hub Git as the encoder owner.
5. Keep existing identity tests. Change them so they hash the consumed Core bytes, still assert history SHA ≠ blank SHA, and still match the five Core file SHAs above.
6. Run `packages/hub-test-support/scripts/sync-assets.mjs --check`. Published JSON must stay byte-identical. Do not bump `@trybotster/hub-test-support` above `0.1.36` and do not bump host conformance revision when the published fixture bytes do not change.
7. Commit on this ticket branch and merge directly into `main`. Do not create a PR.

## Non-scope

- Do not edit `botster-core`.
- Do not bump the Hub Core / protocol pin unless Implement proves the five files are missing at `f4f6bf5`. Plan evidence says they are present.
- Do not publish npm. Do not mutate published `0.1.36` fixture bytes.
- Do not change Unix/WebRTC adapter policy, Hello, or `TerminalSubscriptionClosed`.
- Do not inspect READY, PAGE, FINISH, or Snapshot bodies on the Hub public API.
- Do not add a Core public fixture helper in this run. If later work wants `botster_terminal_protocol::late_attach_*()` constants, register a Core ticket against `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`.
- Do not edit Web, TUI, or TUI Kit.
- Do not dual-pipeline a runtime-teardown path.
- Do not treat this ticket as a session-type eligibility consumer.

## Repository ownership boundaries and cross-repo dependencies

Hub owns host control DTOs, IsolatedHub, and first-party support-matrix JSON. Hub does not own terminal fixture bytes.

Core owns `botster-terminal-protocol` files, SHAs, and regeneration. This run consumes that crate. It does not broaden into Core.

`botster-hub-test-support` may depend on the Hub-safe protocol crate. It must not depend on `botster-terminal-protocol-client`.

Registered dependency: `ticket_1786661004_962658` on Core target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`, status `closed`. Premise revalidated against the locked consume crate: the five files and matching SHAs exist at rev `f4f6bf5babe92dfb9241a760c414187f711c2c42`. No new Core ticket.

Downstream Web/TUI consume the npm JSON fixture, not Hub Git goldens. If `sync-assets --check` stays green, no downstream consumer ticket is required.

## Assumptions and unknowns

- Assumption: "Core-owned versioned assets" means the five committed files in the pinned `botster-terminal-protocol` crate source, not a public Rust export. Core's `PUBLIC_API_ALLOWLIST` has no fixture helpers.
- Assumption: compile-time include from the resolved crate source via `build.rs` + `cargo metadata` is the smallest Hub-only consume path. It keeps Hub Git from storing the bytes. The required Git-consumer smoke is the proof that this resolver works outside the Hub workspace.
- Assumption: deleting unused v1 leftovers is in scope because they are Hub-owned golden bytes the ticket wants removed from Hub Git.
- Assumption: published npm JSON is derived from the same opaque bytes. Identical SHAs mean no package version cutover.
- Assumption: keeping `late_attach_*_payload_bytes()` as opaque helpers is required. Downstream Hub tests already call them.
- Assumption: the existing Ghostty import-semantic test may remain as a crate-private identity check of Core-owned bytes. It is not a new public Snapshot API.
- Unknown: none blocking. If Implement cannot resolve the protocol crate source from `cargo metadata` in this worktree, stop and ask. Do not copy bytes back into Hub Git.

## Affected surfaces/files

| Path | Change |
| --- | --- |
| `crates/botster-hub-test-support/Cargo.toml` | Add `botster-terminal-protocol` at the locked `.git` rev. Drop `fixtures/ghostsnp/**` from `include`. Add `build.rs` to `include`. Keep `botster-terminal-ghostty` as a dev-dep only for the existing import-semantic test. |
| `crates/botster-hub-test-support/build.rs` | New. Resolve the five Core file names from the protocol crate source and emit include paths into `OUT_DIR`. |
| `crates/botster-hub-test-support/src/conformance_data.rs` | Replace Hub `include_bytes!` with Core-consumed bytes. Keep opaque helpers and event wrappers. |
| `crates/botster-hub-test-support/src/lib.rs` | Update identity tests to hash consumed Core bytes. Keep public helper exports. |
| `crates/botster-hub-test-support/fixtures/ghostsnp/**` | Delete the directory. |
| `crates/botster-hub-test-support/tests/generate_incremental_late_attach_frames.rs` | Delete. |
| `tests/hub_test_support_conformance_test.rs` | No event-order change expected. Re-run. |
| `tests/hub_daemon_lifecycle/sessions.rs` | No Golden B helper signature change expected. Re-run the idle GHOSTSNP comparison. |
| `packages/hub-test-support/**` | No source edit unless `sync-assets --check` proves drift. Expected result: no drift. |
| `Cargo.lock` | Refresh only if the new member dependency requires it. Do not change the Core rev. |
| `docs/plans/delete-hub-owned-terminal-goldens-and-consume-core-protocol-fixtures.md` | This plan. |

## Risks

- `build.rs` that shells `cargo metadata` can recurse or fail in unusual Cargo invocations. Keep it a single metadata query of the current manifest and fail with the protocol crate name and rev if the five files are absent.
- Git-consumed `botster-hub-test-support` does not inherit the Hub workspace `[patch]`. The new protocol dep must use the `.git` URL and exact rev so Cargo unifies with `botster-hub-client`. Local workspace `./test.sh` does not prove this. The clean Git smoke is required.
- A Hub-local `rg` that forbids the five Core file names will fail a correct resolver. Split forbidden Hub paths from required Core file-name references.
- A future Core fixture-byte change will change Hub tests automatically. That is intended. Do not re-freeze copies in Hub Git.
- Editing published npm README prose would force a new unpublished package version. Do not edit that README unless fixture bytes actually change.
- The proposed content-blind test note is unratified. Deleting the import-semantic test would weaken SHA-to-screen identity without a replacement Core-side consumer in this repo.

## Acceptance checks/tests

Production path: Hub tests and IsolatedHub helpers that still need late-attach bytes must compile against Core-owned files. After this change, `include_bytes!` in Hub Git must not reference `fixtures/ghostsnp/`. The helpers remain the production entry for Hub tests (`late_attach_history_payload_bytes`, `late_attach_history_events`, serde JSON emitter).

Implement must check free disk (`df`) before cargo or script gates. If `rustc` or `ld` returns `errno 28`, restore space and rerun. Do not treat that failure as a passing baseline.

Implement must record command output for:

1. Forbidden Hub-local authority (must be empty / absent):
   - `test ! -d crates/botster-hub-test-support/fixtures/ghostsnp`
   - `test ! -f crates/botster-hub-test-support/tests/generate_incremental_late_attach_frames.rs`
   - `rg -n 'include_bytes!\(.*fixtures/ghostsnp' crates/botster-hub-test-support`
   - `rg -n 'fixtures/ghostsnp' crates/botster-hub-test-support/Cargo.toml`
2. Required Core file-name references (must exist, and only in consume/identity surfaces):
   - `build.rs` names all five files: `late-attach-history-ready-v2.ghostsnp`, `late-attach-history-page-v2.ghostsnp`, `late-attach-history-finish-v2.ghostsnp`, `late-attach-blank-ready-v2.ghostsnp`, `late-attach-blank-finish-v2.ghostsnp`.
   - Generated `OUT_DIR` includes or copies use those same five names.
   - Provenance and identity tests may name the files and their SHAs.
   - Do not require zero hits for those five names across the crate.
3. Consume identity:
   - Hash the five files from the resolved `botster-terminal-protocol` crate source at rev `f4f6bf5babe92dfb9241a760c414187f711c2c42`.
   - Assert they match the table above.
   - Assert history-ready SHA ≠ blank-ready SHA.
   - Assert the five public payload helpers return those same bytes and SHA pins.
4. In-workspace crate identity:
   - `cargo tree -p botster-hub-test-support -e normal` shows one `botster-terminal-protocol` identity at the locked rev.
   - No `botster-terminal-protocol-client` edge from this crate.
5. External Git-consumer smoke (required; local workspace tests are not a substitute):
   - Commit the Hub change first. Record `HUB_COMMIT`.
   - Create a disposable crate **outside** the Hub workspace. Do not add a `[patch]` table.
   - Depend on `botster-hub-test-support` only as a Git dependency at `HUB_COMMIT`. Use `git = "https://github.com/trybotster/botster-hub.git"` after the commit is on the remote, or `git = "file://<hub-git-dir>"` with that same rev when the commit is still local. Do not use a path dependency.
   - Call `late_attach_history_payload_bytes`, `late_attach_no_history_payload_bytes`, `late_attach_history_payload_sha256`, `late_attach_no_history_payload_sha256`, and `late_attach_incremental_frame_identity`.
   - `cargo run` or `cargo test` in that crate must compile the Git-resolved `build.rs` and succeed.
   - `cargo tree -e normal` in that crate shows exactly one `botster-terminal-protocol` whose source is `git+https://github.com/trybotster/botster-core.git` rev `f4f6bf5babe92dfb9241a760c414187f711c2c42`.
   - The same tree has no `botster-terminal-protocol-client` edge.
6. Repo wrapper:
   - `./test.sh -p botster-hub-test-support`
   - `./test.sh --test hub_test_support_conformance_test`
   - `./test.sh --test hub_daemon_lifecycle_test idle` or the exact idle GHOSTSNP / late-attach filters that call `late_attach_no_history_payload_bytes` and opaque history replay.
7. Published host fixture:
   - `node packages/hub-test-support/scripts/sync-assets.mjs --check` exits 0 with no regeneration.
8. Hygiene:
   - `cargo fmt --all -- --check`
   - `git diff --check`
   - Confirm `.gitignore` still matches HEAD and the worktree path has no `:`.

Downstream proof required by this charter: Hub tests that still need the bytes load them from the Core protocol crate, and a Git consumer of `botster-hub-test-support` can compile those helpers without the Hub workspace patch. The npm package is not the golden authority and should not change. Do not require a Web or TUI live attach for this fixture-ownership ticket.

## Vault gaps worth capturing

- Capture after Implement if the `build.rs` consume path is the durable Hub rule: Hub test support includes Core protocol fixture files from the pinned crate source and must not commit those bytes.
- Do not capture a Core public fixture API unless a later Core ticket adds one.
- No capture needed for the SHA table. Core already tests those identities.

## Botster layers touched

- Rust hub test-support crate and its compile-time fixture consume path.
- Not daemon runtime, not adapters, not SPA, not plugin workflow.

## Worktree/target assumptions

- Implement edits this run's Hub worktree only.
- Do not edit spawn-target `botster-core`.
- Direct merge to `main`. No PR.

## Pipeline gates and artifacts

- Plan artifact: this file, revision 2.
- Implement must add a report under `docs/reports/` naming the Core rev, deleted Hub paths, in-workspace commands, and the external Git-consumer smoke (commit, file or https git source, tree identity, helper calls).
- Vault checklist for this ticket remains `checklist_1786757345_721712`. Do not create another Plan vault checklist.
- Gate evidence must include `plan_uri`, `artifact_id`, `checklist_id`, `target_id`, and `target_repository`.

## Required docs or plugin README updates

- Update crate comments that call the goldens Hub-owned or name the deleted generator.
- Do not edit `packages/hub-test-support/README.md` unless published fixture bytes change.

## Runtime-teardown class

`teardown_class_applies`: no. Isolation, bounds, late-message matrix, production-path teardown proof, ownership identity, and sibling/fail-closed policy are not in scope.
