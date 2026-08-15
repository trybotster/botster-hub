# Implement report: Delete Hub-owned terminal goldens and consume Core protocol fixtures

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | `botster-hub` |
| Pipeline worktree | the pipeline-provided ticket worktree |
| Ticket | `ticket_1786664495_777899` |
| Run | `run_1786756876_578418` |
| Step | `botster_stack_implement` (`run_step_1786759528_248189`) |
| Approved plan | `docs/plans/delete-hub-owned-terminal-goldens-and-consume-core-protocol-fixtures.md` revision 2 |
| Merge policy | direct into `main`; do not create a PR |
| Base | `origin/main` `279d828ca377d23e743ae3e724a1ac9ce81520e2` |
| Implementation commit | `92ed4d77bcfd663655400262a40ea99040893fb3` |
| Locked Core / protocol crate | `git+https://github.com/trybotster/botster-core.git` rev `f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| Session-type eligibility consumer | false |
| `teardown_class_applies` | no |

Routing verified independently: `project_pipelines_current_context` ticket/run `target_id` and `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub`. The approved plan used the same routing. Implementation stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]] — ownership charter
- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]] — loaded because the implementer overlay requires it; this ticket has no React or SPA edit surface

### Targeted atomic notes

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
- [[proposed Hub terminal tests enforce content blind adapters]] — proposed, not ratified. The import-semantic identity test remains.
- [[authentic GHOSTSNP tests cross the pinned encoder and embedded decoder]]
- [[Hub incremental GHOSTSNP attach passed production proof at Core revision 033cd01]]
- [[blocking dependency premises must be revalidated per consuming crate]]
- [[test script required for rust tests not cargo test]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[project pipelines checklist worker timeouts require artifact evidence fallback]]

### Explicitly not loaded

- [[project-pipelines-playbook]] — Project Pipelines package and plugin paths are out of scope
- [[botster runtime teardown lenses]] — this ticket deletes Hub-owned fixture bytes and retargets compile-time includes. It does not change WebRTC, SessionIo/ClientWorker teardown, multi-peer ownership, or resource spin
- Other repository charters

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`
- Follow approved plan revision 2
- Hub does not own terminal fixture bytes
- Consume `botster-terminal-protocol` at `https://github.com/trybotster/botster-core.git` rev `f4f6bf5babe92dfb9241a760c414187f711c2c42`
- Do not depend on `botster-terminal-protocol-client`
- Do not add Snapshot phase, history, or payload accessors
- Do not edit Core, Web, TUI, or TUI Kit
- Do not bump `@trybotster/hub-test-support` above `0.1.36`
- Do not mutate published fixture JSON
- Use `./test.sh` for Hub Rust tests
- Direct-merge pipeline: commit on the ticket branch; do not create a PR
- Check free disk before cargo or script gates

## Files changed

Implementation commit `92ed4d77bcfd663655400262a40ea99040893fb3`:

| Path | Change |
| --- | --- |
| `crates/botster-hub-test-support/Cargo.toml` | Add `botster-terminal-protocol` at the locked `.git` rev. Drop `fixtures/ghostsnp/**` from `include`. Add `build.rs` to `include`. Keep `botster-terminal-ghostty` as a dev-dep. |
| `crates/botster-hub-test-support/build.rs` | New. Resolve the five Core file names from the protocol crate source and copy them into `OUT_DIR`. |
| `crates/botster-hub-test-support/src/conformance_data.rs` | Replace Hub `include_bytes!` with `OUT_DIR` copies. Retarget provenance to the Core crate coordinate and file names. Keep opaque helpers. |
| `crates/botster-hub-test-support/src/lib.rs` | Hash consumed Core bytes. Assert history SHA ≠ blank SHA. Assert the five Core file SHAs and public helper pins. |
| `crates/botster-hub-test-support/fixtures/ghostsnp/**` | Deleted, including unused v1 leftovers and the Hub-authored README. |
| `crates/botster-hub-test-support/tests/generate_incremental_late_attach_frames.rs` | Deleted. |
| `Cargo.lock` | One added `botster-terminal-protocol` edge under `botster-hub-test-support`. Core rev unchanged. |
| `docs/plans/delete-hub-owned-terminal-goldens-and-consume-core-protocol-fixtures.md` | Approved plan revision 2. |

This report is committed separately after the Git-consumer smoke.

No edits to `packages/hub-test-support/**`, adapters, Hello, or `TerminalSubscriptionClosed`.

## Ownership boundaries preserved

Hub owns host control DTOs, IsolatedHub, and first-party support-matrix JSON. Hub does not own terminal fixture bytes.

`botster-hub-test-support` depends on the Hub-safe protocol crate. It does not declare `botster-terminal-protocol-client`. Public helpers wrap opaque `DaemonEvent::Snapshot` payloads through `DaemonOpaqueHistoryPayload::from_bytes`. They do not inspect READY, PAGE, FINISH, or Snapshot bodies.

The published npm package stays `@trybotster/hub-test-support@0.1.36`. `sync-assets --check` reported no regeneration.

## Cross-repo dependencies or separately routed work

Registered dependency `ticket_1786661004_962658` on Core target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` is closed. Premise revalidated: the five files and matching SHAs exist at rev `f4f6bf5babe92dfb9241a760c414187f711c2c42`.

This run does not edit Core. No new Core ticket.

Downstream Web/TUI consume the npm JSON fixture. That JSON did not change. No downstream consumer ticket.

## Deviations from plan

None in product scope.

Process notes:

- The pipeline is Implement → Review → Verify → Merge with `merge_policy: direct`. This visit commits on the ticket branch and does not merge before Review. No PR is created.
- `./test.sh -p botster-hub-test-support` expands to `cargo test --workspace -p botster-hub-test-support` and executed the root Hub suite before the member crate. That suite hit `botster_web_health_rejects_stale_daemon_socket_file`. The same exact test passed in isolation on this branch and on base `279d828`. The changed crate was then proven with `BOTSTER_ENV=test cargo test -p botster-hub-test-support`.
- `botster-terminal-protocol-client` remains a transitive edge of the pre-existing `botster-core` IsolatedHub dependency. `cargo tree -e normal -p botster-hub-test-support --depth 1` has no client-crate edge. This run did not add that crate.

## Tests and downstream proof run

Production entry point: Hub tests and IsolatedHub helpers that still need late-attach bytes call `late_attach_history_payload_bytes`, `late_attach_no_history_payload_bytes`, `late_attach_history_events`, and the serde JSON emitter. After this change those helpers include Core-owned files from `OUT_DIR`. Hub Git has no `include_bytes!(...fixtures/ghostsnp...)`.

Free disk before gates: 184 GiB available. No `errno 28`.

| Check | Result |
| --- | --- |
| `test ! -d crates/botster-hub-test-support/fixtures/ghostsnp` | absent |
| `test ! -f crates/botster-hub-test-support/tests/generate_incremental_late_attach_frames.rs` | absent |
| `rg -n 'include_bytes!\(.*fixtures/ghostsnp' crates/botster-hub-test-support` | no hits |
| `rg -n 'fixtures/ghostsnp' crates/botster-hub-test-support/Cargo.toml` | no hits |
| `build.rs`, `OUT_DIR` includes, provenance, and identity tests name the five Core files | present |
| `cargo tree -p botster-hub-test-support -e normal` | one `botster-terminal-protocol` at `git+https://github.com/trybotster/botster-core.git?rev=f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| `cargo tree -p botster-hub-test-support -e normal --depth 1` | no `botster-terminal-protocol-client` |
| `BOTSTER_ENV=test cargo test -p botster-hub-test-support` | 44 lib tests + 3 doctests passed, including identity and import-semantic proofs |
| `./test.sh --test hub_test_support_conformance_test` | 2 passed |
| `./test.sh --test hub_daemon_lifecycle_test external_hub_idle_attach_emits_ghostsnp_snapshot_before_attached -- --exact --nocapture` | 1 passed. Live idle Snapshot compared to Golden B helper. |
| `./test.sh --test hub_daemon_lifecycle_test external_daemon_same_session_reattach_replays_opaque_history_before_live_output -- --exact --nocapture` | 1 passed |
| `node packages/hub-test-support/scripts/sync-assets.mjs --check` | exit 0, "package assets are current" |
| `cargo fmt --all -- --check` | exit 0 |
| `git diff --check` | clean |
| `.gitignore` vs HEAD | unchanged |
| worktree path colon | none |

External Git-consumer smoke (required):

- Commit: `92ed4d77bcfd663655400262a40ea99040893fb3`
- Disposable crate outside the Hub workspace
- No `[patch]` table
- Dependency: `botster-hub-test-support` via `git = "file://<spawn-target botster-hub repo>"` rev `92ed4d77bcfd663655400262a40ea99040893fb3`
- `cargo run` compiled the Git-resolved `build.rs` and called `late_attach_history_payload_bytes`, `late_attach_no_history_payload_bytes`, `late_attach_history_payload_sha256`, `late_attach_no_history_payload_sha256`, and `late_attach_incremental_frame_identity`
- Helper output: history 2838 / `fbcdda31d682a61420251eed68f72e413485f057e3f374c57582955b0316bb6d`; blank 1131 / `06962b11d4a3acfb9b7c52b673a7b476904ddee2dd754b89b190ff82fdcfd0cc`; PAGE/FINISH pins match the plan table
- `cargo tree -e normal -i botster-terminal-protocol` shows one protocol crate whose source is `git+https://github.com/trybotster/botster-core.git?rev=f4f6bf5babe92dfb9241a760c414187f711c2c42`
- Depth-1 `botster-hub-test-support` tree has no `botster-terminal-protocol-client`

Unrelated suite flake evidence:

- Command: `./test.sh --test hub_daemon_lifecycle_test botster_web_health_rejects_stale_daemon_socket_file -- --exact --nocapture`
- Branch isolate: exit 0
- Base `279d828` isolate: exit 0
- That test does not call late-attach helpers or read GHOSTSNP files

## Unverified behavior or residual risk

- This ticket does not run live Web or TUI attach. The plan excludes those paths.
- A future Core fixture-byte change at a new revision will change Hub tests after Hub bumps the pin. That is intended.
- `botster-core` still pulls `botster-terminal-protocol-client` for IsolatedHub. Hub test-support source does not import that crate.
- `./test.sh -p botster-hub-test-support` remains a workspace-wide wrapper. Review should treat crate-scoped `cargo test -p botster-hub-test-support` plus the filtered `./test.sh --test` commands as the product proof.

## Missing vault guidance discovered

The consume path is now a durable Hub rule. Implement captured:

- `knowledge/inbox/hub-test-support-consumes-core-protocol-fixture-files-from-the-pinned-crate-source.md`

No Core public fixture API was captured.

Vault checklist: `checklist_1786759753_536915` (ticket-scoped Implement vault workflow). Plan checklist `checklist_1786757345_721712` was left as the Plan visit record. The two pending Plan duplicates remain workflow history.
