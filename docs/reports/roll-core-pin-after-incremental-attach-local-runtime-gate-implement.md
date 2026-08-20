# Implement report: Roll Core pin after IncrementalAttach local-runtime gate

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | `botster-hub` |
| Pipeline worktree | the pipeline-provided ticket worktree |
| Ticket | `ticket_1787251447_191212` |
| Run | `run_1787254466_486567` |
| Step | `botster_stack_implement` |
| Approved plan | `docs/plans/roll-core-pin-after-incremental-attach-local-runtime-gate.md` revision 2 |
| Merge policy | direct into `main`; do not create a PR |
| Base | `origin/main` `7a09292cd518186e0def758c823c0841ee1cacf1` |
| Pin-roll candidate | `a111248140b14086c1eb4a4dcb0cdd5eb350a88b` |
| Review-return candidate | `0139335f474f74c67705010887acb45dfcb91e35` |
| Exact-command return candidate | `4dd67f55a53c6230093776d1a5e142438e13c9e6` |
| Old Core pin | `8fce2041b9fe742cb2a6df9e74cb262606672742` |
| New Core pin | `7eafa470a18025895995bbedc20d34b58106a03b` |
| Session-type eligibility consumer | no |
| `teardown_class_applies` | no |

Routing: `project_pipelines_current_context` ticket/run `target_id` is `tgt_7e208a0c76a44980a83b63af976b1f22`. `list_spawn_targets` maps that id to spawn target `botster-hub` (`trybotster/botster-hub`). The approved plan uses the same routing. Work stayed in this run worktree.

Grok-native Botster MCP handshake failed at session start because the configured env passed a literal `${env:BOTSTER_SESSION_UUID}`. Implement later called `botster mcp-serve` with the real session UUID. `list_spawn_targets` in checklist `check_1787258316_786085` is that later call, not a Grok-native MCP success at the start of the first Implement visit.

`git ls-remote https://github.com/trybotster/botster-core.git refs/heads/main` returned `7eafa470a18025895995bbedc20d34b58106a03b` before the pin edit. Implement pinned that exact revision.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]] — ownership charter
- [[botster-hub-client-playbook]] — Git-consumed member overlay; only the member manifest revision changed
- [[botster-architecture]]
- [[cli-patterns]]

### Targeted atomic notes

- [[Git-consumed Hub members pin Core protocol by exact revision]]
- [[Cargo Git URL and selector form are part of crate identity]]
- [[Hub test support copies Core protocol fixtures from the pinned crate source]]
- [[git-visible Hub member manifests must use the UI contract tag]]
- [[botster-core local process runtime is feature-gated from contract-only embeds]]
- [[TUI bin only Core 8fce204 builds require local runtime feature unification]]
- [[botster-core CI runs a contract only test lane because workspace feature unification hides breaks]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[Hub bee15e7 builds the session worker from botster-core-daemon]]
- [[pin rolls update live lane provenance defaults and README pin prose]]
- [[botster core contract surface needs consumer proof]]
- [[hub test support npm releases need external consumer smoke]]
- [[external client hub tests use subprocess spawned hub test support]]
- [[botster hub client crate is the external client boundary]]
- [[botster review agents must run verify strict gates not lighter equivalents]]
- [[botster review and verify must scan all committed artifacts for pii]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[test script required for rust tests not cargo test]]
- [[cli test sh filters match rust test names not filenames]]
- [[a pipeline run target id must match a registered project target]]

### Explicitly not loaded

- [[project-pipelines-playbook]] — no Project Pipelines package or plugin paths are in scope
- [[botster runtime teardown lenses]] — no runtime, peer, or teardown code changed
- Other repository charters

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`
- Follow approved plan revision 2
- Keep `https://github.com/trybotster/botster-core.git` and the `rev =` selector on every Core-family dependency
- Do not change Hub runtime, protocol, DTO, fixture bytes, or npm package version
- Do not change `LATE_ATTACH_GHOSTSNP_GHOSTTY_PIN` or the `botster-ui-contract` tag
- Do not edit historical `docs/plans/**` or `docs/reports/**` records of `8fce204`
- Do not edit TUI or Core
- Use `./test.sh` for Hub tests
- Direct-merge pipeline: commit on the ticket branch; do not create a PR

## Files changed

| Path | Change |
| --- | --- |
| `Cargo.toml` | Five Core-family `rev` values to `7eafa47` |
| `crates/botster-hub-client/Cargo.toml` | Git-visible `botster-terminal-protocol` pin |
| `crates/botster-hub-test-support/Cargo.toml` | Three Core-family pins |
| `crates/botster-hub-test-support/build.rs` | `PROTOCOL_REV` |
| `crates/botster-hub-test-support/src/conformance_data.rs` | `LATE_ATTACH_GHOSTSNP_CORE_PIN` |
| `crates/botster-hub-test-support/src/lib.rs` | Provenance unit-test literal |
| `tests/session_projection_owner_loop.rs` | `REQUIRED_CORE_REV`, README assertions, per-declaration Core-family guard, mixed-rev and mixed-URL red tests |
| `tests/hub_daemon_lifecycle/package_event_plane.rs` | Live-proof locked Core assertion |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | Provenance log literal |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | Provenance log literal |
| `Cargo.lock` | Six Core-family `source` lines only (6 insertions, 6 deletions) |
| `README.md` | `## Dependency policy` first two paragraphs; production-session paragraph kept |
| `docs/plans/roll-core-pin-after-incremental-attach-local-runtime-gate.md` | Plan revision 2 (Plan step) |
| `docs/reports/roll-core-pin-after-incremental-attach-local-runtime-gate-implement.md` | This report |

No edits to `packages/hub-test-support/**`, `.github/workflows/*`, `crates/botster-hub-installation`, or the root `[patch]` table.

## Ownership boundaries preserved

Hub owns the dependency policy, member manifests, lockfile, test-support fixture consumption, and live-proof provenance literals. All product edits stay inside this repository.

`botster-hub-client` remains the Git-consumed external client boundary. Its manifest still uses the Core `.git` URL and one exact `rev`. Downstream TUI and Web resolve that member, not the Hub lockfile.

The UI-contract tag is unchanged: `tag = "botster-ui-contract-v0.3.2"`. Ghostty submodule pin `eb72ec61304ea256be1d86ed8fa961c84e43ecbd` is unchanged.

## Cross-repo dependencies or separately routed work

- Core owns the `IncrementalAttach` feature-gate fix. It is already merged at `7eafa47` under closed dependency `dependency_1787251453_570256` against `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`. This run adds no Core work.
- TUI ticket `ticket_1786663585_944018` already depends on this ticket (`dependency_1787251454_407744`). Proof B built that TUI branch at `b8872811ea088fe445aa262e1d92a1d1fb627417` against this Hub candidate. This run does not edit TUI.
- No new cross-repository dependency is required.

## Deviations from plan

None in product scope. Review finding `finding_1787259246_848243` showed the first Implement guard was weaker than the plan and README claim. This return implements that claim: every Core-family declaration is enumerated. Plan section 8 and acceptance check 6 now name the per-declaration tests.

Process notes:

- Grok-native Botster MCP handshake failed at session start. Later `botster mcp-serve` calls succeeded, including `list_spawn_targets` recorded on `check_1787258316_786085`.
- Duplicate vault checklist `checklist_1787258333_250297` came from a `create_vault_checklist` timeout retry. All four items are now `skipped` as duplicates of `checklist_1787258316_886631`.
- `./test.sh --locked --test hub_daemon_lifecycle_test package_event_plane` compiled and ran zero tests because the filter matches function names, not module files. Implement reran `isolated_hub_two_packages_emit_and_consume_exact_event_without_blocking_worktree` with `--exact --nocapture`.
- The pipeline is Implement → Review → Verify → Merge with `merge_policy: direct`. This visit commits on the ticket branch and does not merge before Review. No PR is created.

## Review return (`review_1787259246_580208`)

Open findings addressed:

- `finding_1787259246_848243` (product, high): `git_visible_hub_members_share_one_exact_core_revision` now enumerates every expected Core-family git table in each member manifest and requires the exact `.git` URL and `REQUIRED_CORE_REV` on each declaration. `git_visible_hub_members_reject_one_mixed_core_revision` and `git_visible_hub_members_reject_one_mixed_core_url` use fixtures that still contain the approved URL and rev, so a whole-file `contains` check would pass, and they require the per-declaration guard to fail.
- `finding_1787259246_896862` (process, low): routing prose now distinguishes the failed Grok-native handshake from the later `botster mcp-serve` `list_spawn_targets` call. Duplicate checklist items are skipped.

## Review return (`review_1787259748_300660`)

Open findings addressed:

- `finding_1787259748_439457` (test evidence, high): acceptance check 6 now uses three `--exact` commands, one per full test name. Each command ran `1 passed`. The prefix filter `git_visible_hub_members -- --exact` still runs 0 tests and is no longer the documented gate.
- `finding_1787259748_121512` (product hygiene, medium): the mixed-rev fixture uses synthetic rev `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`, not retired pin `8fce2041b9fe742cb2a6df9e74cb262606672742`. `rg` of the old pin outside `docs/` and `target/` now returns zero matches.

## Tests and downstream proof run

Production entry point: Git consumers of `botster-hub-client` resolve `botster-terminal-protocol` from the member manifest. After this change, that manifest names Core `.git` rev `7eafa470a18025895995bbedc20d34b58106a03b`. A contract-only TUI production build (`default-features = false`) can compile against that pin without `local-runtime`.

Local toolchain: `rustc 1.92.0`, `cargo 1.92.0`. GitHub Hub CI still pins Rust `1.97.0` and was already red on `main` at `7a09292` (`clippy::useless_conversion` in `crates/botster-hub-installation/src/safety.rs`, run `32329783340`). This change does not touch that crate.

| # | Command | Result |
| --- | --- | --- |
| 1 | `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | exit 0; worker compiled from Core `7eafa47` |
| 2 | `cargo build --locked --bin botster-hub` | exit 0 |
| 3 | `cargo fmt --all -- --check` | exit 0 |
| 4 | `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 on local `1.92.0` |
| 5 | `./test.sh --locked` | exit 0; one clean default-concurrency run; `packages/hub-test-support` assets current; 1166 passed, 0 failed |
| 6a | `./test.sh --locked --test session_projection_owner_loop git_visible_hub_members_share_one_exact_core_revision -- --exact` | 1 passed |
| 6b | `./test.sh --locked --test session_projection_owner_loop git_visible_hub_members_reject_one_mixed_core_revision -- --exact` | 1 passed |
| 6c | `./test.sh --locked --test session_projection_owner_loop git_visible_hub_members_reject_one_mixed_core_url -- --exact` | 1 passed |
| 6-prefix | `./test.sh --locked --test session_projection_owner_loop git_visible_hub_members -- --exact` | 0 tests (documents why the prefix plus `--exact` is not a gate) |
| 7 | `BOTSTER_ENV=test cargo test --locked -p botster-hub-test-support` | 44 lib tests + 3 doctests passed, including `late_attach_goldens_have_distinct_content_identity_and_pinned_provenance` |
| 8 | `./test.sh --locked --test hub_daemon_lifecycle_test isolated_hub_two_packages_emit_and_consume_exact_event_without_blocking_worktree -- --exact --nocapture` | 1 passed after candidate commit. Log: `hub_sha=a111248140b14086c1eb4a4dcb0cdd5eb350a88b core_sha=7eafa470a18025895995bbedc20d34b58106a03b`. Both binaries live under this checkout `target/` |
| 9 | `cargo tree -e normal -i botster-terminal-protocol --locked` | one source `git+https://github.com/trybotster/botster-core.git?rev=7eafa470a18025895995bbedc20d34b58106a03b#7eafa470` |
| 10 | `cargo tree -p botster-hub-test-support -e normal --depth 1 --locked` | no `botster-terminal-protocol-client` |
| 11 | `git diff --stat origin/main...HEAD -- Cargo.lock` | six Core-family `source` lines (6 insertions, 6 deletions) |
| 12 | `rg` old pin outside `docs/` and `target/` | zero matches |
| 13 | README `from the \`main\` branch` | zero matches |
| 14 | `git diff --check origin/main...HEAD` and PII grep of the branch diff | clean |

Proof A (contract-only Git consumer of this candidate; no `[patch]`; isolated target dir):

- `botster-core` and `botster-terminal-protocol-client` at Core `7eafa47` with `default-features = false` on `botster-core`
- `botster-hub-client` from the ticket worktree at rev `a111248140b14086c1eb4a4dcb0cdd5eb350a88b`
- `cargo generate-lockfile` then `cargo build --locked` exit 0 (`Finished dev profile` in 18.37s)
- Binary printed `contract-only consumer ok`
- `cargo tree -e features,no-dev -i botster-core --locked` has 0 `local-runtime` and 0 `portable-pty` edges
- `cargo tree -e normal -i botster-terminal-protocol --locked` shows one source at `rev=7eafa47`

Proof A red oracle (old pin):

- Core-family revs `8fce204`; `botster-hub-client` at base `7a09292`
- `cargo build --locked` failed with `error[E0412]: cannot find type IncrementalAttach in this scope` at `botster.rs:1767` and `could not compile botster-core (lib)`

Proof B (real first-party consumer; no `[patch]`; isolated target dir):

- TUI ticket branch `project-pipelines/ticket_1786663585_944018` at `b8872811ea088fe445aa262e1d92a1d1fb627417` (unchanged from plan time)
- Core-family revs set to `7eafa47` with `default-features = false` kept
- `botster-hub-client` and `botster-hub-test-support` set to the ticket worktree at rev `a111248`
- `botster-ui-contract` left on `botster-ui-contract-v0.3.2`
- `cargo generate-lockfile` then `cargo build -p botster-tui --locked` exit 0 (`Finished dev profile` in 54.72s)
- Binary size 18243760 bytes
- `cargo tree -p botster-tui -e features,no-dev -i botster-core --locked` has 0 `local-runtime` and 0 `portable-pty` edges
- `cargo tree -p botster-tui -e normal -i botster-terminal-protocol --locked` shows one source at `rev=7eafa47`

Scratch clones and isolated target dirs lived outside the Hub worktree and are not part of this branch.

## Unverified behavior or residual risk

- GitHub Hub CI at `7a09292` is already red under Rust `1.97.0` for an unrelated `clippy::useless_conversion` lint in `crates/botster-hub-installation`. Local strict clippy on `1.92.0` is green. This ticket does not claim GitHub CI green.
- Core GitHub CI at `7eafa47` skipped the contract-only lane because Lint failed first (`clippy::drain_collect` under floating `stable`). Proof A is the consumer-side contract-only evidence for this pin.
- TUI `main` still pins an older Hub revision. The consumer that failed, and the consumer this ticket unblocks, is TUI ticket branch `b887281`, not TUI `main`.
- No npm publish and no fixture-byte change. Downstream npm smoke is not implicated.

## Missing vault guidance discovered

Captured to the vault inbox:

- Hub Core pin rolls update eleven literal sites and six lock sources. The zero-match grep outside `docs/` is the completion check.
- Hub suite runs require an explicit session-worker prebuild before `./test.sh --locked`, even though `ensure_session_worker_binary` exists.
- When a consumer ticket branch already carries the pins that failed, downstream proof must target that branch, not the consumer `main`.

This Review return captured:

- cargo `--exact` with a name prefix runs zero tests and exits 0.
