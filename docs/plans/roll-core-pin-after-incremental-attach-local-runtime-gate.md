# Plan: Roll Core pin after IncrementalAttach local-runtime gate

Ticket: `ticket_1787251447_191212`
Run: `run_1787254466_486567`
Step: `botster_stack_plan`
Pipeline: `botster_stack_delivery` (direct merge, no PR)
Registered dependency: Core `ticket_1787251441_640678` (`dependency_1787251453_570256`), status closed
Triggering finding: botster-tui `ticket_1786663585_944018` Review `finding_1787251254_962248`
Vault checklist: `checklist_1787254776_257555` (run scope, one Plan visit)

## 1. Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn-target name | `botster-hub` (path from the spawn-target registry) |
| Run worktree | `project-pipelines/ticket_1787251447_191212` at `7a09292cd518186e0def758c823c0841ee1cacf1` (equals `origin/main`) |
| Worktree hygiene | tracked `.gitignore` is 53 bytes and matches HEAD; worktree path has no `:`; no `CARGO_TARGET_DIR` override needed |
| Dependency repository | `botster-core` (`trybotster/botster-core`), `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` |
| Old Core pin | `8fce2041b9fe742cb2a6df9e74cb262606672742` |
| New Core pin | `7eafa470a18025895995bbedc20d34b58106a03b` |
| Session-type eligibility consumer | no |
| `teardown_class_applies` | no (manifest and revision-literal change; no runtime, peer, or teardown code changes) |

Independent resolution: `project_pipelines_current_context` gives the ticket and run `target_id`. `list_spawn_targets` maps `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub`. The process working directory was not used for routing.

New pin provenance: Core merge artifacts `artifact_1787254440_418919` and `artifact_1787254446_793014` record a fast-forward push of `7eafa470a18025895995bbedc20d34b58106a03b` to `trybotster/botster-core` `main` from `8fce2041b9fe742cb2a6df9e74cb262606672742`. `git ls-remote https://github.com/trybotster/botster-core.git refs/heads/main` returns `7eafa470a18025895995bbedc20d34b58106a03b` at plan time.

## 2. Repository playbook loaded

[[botster-hub-playbook]]

## 3. Other role/surface playbooks and atomic notes loaded

Role and stack:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-architecture]]
- [[cli-patterns]]
- [[botster hub is a first party host profile over core]]
- [[botster orchestration should spawn agents with explicit target ids]]
- [[botster orchestration prompts must bind agents to explicit worktrees]]
- [[prefer framework and library components over custom solutions]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[vault example paths are not repository placement conventions]]
- [[cross repo dependency registration must use dependency repo target]]
- [[colon worktree paths break cargo dyld library paths]]
- [[hearth gate runs require restoring a pipeline wiped gitignore before attribution]]
- [[plan review must check open sibling tickets that own part of the plan scope]]
- [[plan review must diff the run branch against origin main before approving]]

Targeted atomic notes for this ticket:

- [[Git-consumed Hub members pin Core protocol by exact revision]] -- the rule this ticket must keep.
- [[Cargo Git URL and selector form are part of crate identity]] -- the URL form and `rev` selector must not change.
- [[Hub test support copies Core protocol fixtures from the pinned crate source]] -- `build.rs` names the revision and must roll with the manifests.
- [[git-visible Hub member manifests must use the UI contract tag]] -- the UI contract tag is separate and stays at `botster-ui-contract-v0.3.2`.
- [[botster-core local process runtime is feature-gated from contract-only embeds]] -- the boundary the consumer proof must respect.
- [[TUI bin only Core 8fce204 builds require local runtime feature unification]] -- the defect this roll removes from the Hub pin.
- [[botster-core CI runs a contract only test lane because workspace feature unification hides breaks]] -- the Core lane added at `7eafa47`.
- [[live hub proof records distinct hub and locked core binary provenance]] -- live tests assert the locked Core SHA.
- [[Hub bee15e7 builds the session worker from botster-core-daemon]] -- worker build command for live lanes.
- [[pin rolls update live lane provenance defaults and README pin prose]] -- a roll must leave zero stale revision literals outside `Cargo.lock` and `docs/`.
- [[botster core contract surface needs consumer proof]] -- downstream-shaped proof is required.
- [[hub test support npm releases need external consumer smoke]] -- confirms no npm release is implicated (no fixture or DTO change).
- [[external client hub tests use subprocess spawned hub test support]] -- shape of the downstream test graph.
- [[botster hub client crate is the external client boundary]] -- `botster-hub-client` is the Git-consumed member.
- [[botster review agents must run verify strict gates not lighter equivalents]] -- full charter gates, not crate-local subsets.
- [[botster review and verify must scan all committed artifacts for pii]] -- artifact scan before merge.
- [[implementation artifacts must match actual git state]] -- the report must name the real commit and lock SHAs.
- [[review must diff stale capability disclaimers when behavior changes]] -- README prose check.

Not loaded, with reason: [[botster runtime teardown lenses]] does not apply; this ticket has no runtime code change. [[project-pipelines-playbook]] does not apply; no Project Pipelines paths are in scope. [[spa-patterns]] does not apply; no SPA surface.

## 4. Context loaded

- Ticket, run, gates, dependency, and events from `project_pipelines_current_context`.
- Dependency run `run_1787251456_699480` context: Plan, Plan Review, Implement, Review, Verify all approved; merge artifacts name `7eafa47`.
- Core Verify artifact `artifact_1787254251_945968`: contract-only lanes green at `7eafa47` (`cargo check -p botster-core --no-default-features --lib` and `--all-targets` exit 0; `cargo test -p botster-core --no-default-features --lib` 13 passed on reruns after one pre-existing `plugin_worker` flake); downstream TUI proof used a scratch TUI clone with `[patch]` path entries and `cargo build -p botster-tui` exit 0, `cargo tree ... -i botster-core` with zero `local-runtime`; red oracle reproduced `E0412 IncrementalAttach` when the cfg was removed.
- Core diff `8fce204..7eafa47`: 4 files. `crates/botster-core/src/engine/botster.rs` gains one `#[cfg(feature = "local-runtime")]` line at 1764; `.github/workflows/ci.yml` adds the contract-only test step; two docs files. No fixture bytes, no protocol crate, no `botster-core-daemon`, no Ghostty submodule change (`crates/botster-terminal-ghostty/vendor/ghostty` is `eb72ec61304ea256be1d86ed8fa961c84e43ecbd` at both revisions).
- Hub repository: `Cargo.toml`, member manifests, `crates/botster-hub-test-support/build.rs`, `test.sh`, `.github/workflows/ci.yml`, README sections `Dependency policy` and release provenance, `docs/plans` and `docs/reports` prior art.
- Hub precedent pin-roll commits: `6988d90` (Core `302c7f7`) and `e864c3c` (Ghostty rollout). Each touched the same file set that section 8 lists.
- Sibling check: open `botster-hub` tickets are this ticket and `ticket_1786663585_879846` (integration load campaign). No open sibling owns a Core pin roll.
- GitHub CI status (pre-existing, unrelated to this ticket):
  - Hub `main` run `32329783340` at `7a09292` fails at `Lint workspace` with `clippy::useless_conversion` at `crates/botster-hub-installation/src/safety.rs:507:19` and `:580:8` under the CI-pinned Rust `1.97.0`. Local toolchain is `1.92.0`, which does not emit that lint.
  - Core `main` run `32409332848` at `7eafa47` fails at `Lint` with `clippy::drain_collect` at `crates/botster-core/src/runtime/local_process.rs:599:33` under floating `stable` (`1.98.0`). The CI `Test contract-only core library` step was skipped because Lint failed first. The local Core Verify artifact ran that lane green.
- Local toolchain: `rustc 1.92.0`, `cargo 1.92.0`, `zig 0.16.0`.

## 5. Scope and non-scope

### Scope

1. Replace every `8fce2041b9fe742cb2a6df9e74cb262606672742` Core revision literal in Hub source and manifests with `7eafa470a18025895995bbedc20d34b58106a03b` (inventory in section 8). Keep the `https://github.com/trybotster/botster-core.git` URL form and the `rev =` selector on every Core-family dependency.
2. Re-resolve `Cargo.lock` so the six Core-family packages (`botster-core`, `botster-core-daemon`, `botster-core-test-support`, `botster-terminal-ghostty`, `botster-terminal-protocol`, `botster-terminal-protocol-client`) point at the new revision. No other lock entry may change.
3. Prove that a contract-only Core consumer with `default-features = false` builds against the new pin through the Git-visible `botster-hub-client` manifest (section 10, proof A), and run the TUI-shaped lockstep build (section 10, proof B).
4. Run the Hub charter gates on the rolled worktree.
5. Commit the implementation report at `docs/reports/roll-core-pin-after-incremental-attach-local-runtime-gate-implement.md` with the Hub commit SHA, locked Core SHA, exact commands, and results.

### Non-scope

- No Hub runtime, protocol, DTO, fixture, or policy change. The Core diff contains none.
- No `botster-hub-test-support` npm package version change and no conformance fixture revision change. Fixture bytes and host DTOs are unchanged at `7eafa47`.
- No change to `LATE_ATTACH_GHOSTSNP_GHOSTTY_PIN` (`eb72ec61...`); the submodule is unchanged.
- No change to the `botster-ui-contract` tag or the root `[patch]` table.
- No edit to historical `docs/plans/**` and `docs/reports/**` files that mention `8fce204`. Those are dated records.
- No fix for the pre-existing Hub CI `useless_conversion` lint in `crates/botster-hub-installation/src/safety.rs`. That lint is outside the ticket and outside the changed file set. A separate Hub ticket should own it.
- No rewrite of the README `Dependency policy` paragraph (README lines near 1113-1121). That paragraph still says Hub tracks Core `main` with the revision only in `Cargo.lock`; it has been stale since `175dd36`. This ticket does not change that behavior, so the paragraph stays out of scope and is recorded as a vault gap and docs follow-up.
- No TUI or Web pin roll. TUI `ticket_1786663585_944018` consumes the merged Hub commit and Core `7eafa47` in lockstep after this ticket merges.

## 6. Repository ownership boundaries and cross-repo dependencies

- `botster-hub` owns its dependency policy, member manifests, lockfile, test-support fixture consumption, and live-proof provenance assertions. All changes stay inside this repository.
- `botster-core` owns the feature gate fix. It is merged at `7eafa47` under closed dependency `dependency_1787251453_570256` against target `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`. This run adds no Core work.
- `botster-hub-client` is the Git-consumed external client boundary. Its manifest revision is the contract that downstream TUI and Web resolve. `botster-hub-test-support` must carry the same URL and revision so `TerminalFrame` stays one crate identity.
- Downstream consumers: botster-tui `ticket_1786663585_944018` (open) triggered this roll and will roll its own pins to the merged Hub commit and Core `7eafa47`. That work stays in the TUI repository. This plan recommends that the TUI run register a dependency on this ticket; this run does not edit TUI tickets.
- No new cross-repository dependency is required for this run.

## 7. Assumptions and unknowns

Assumptions:

- `7eafa470a18025895995bbedc20d34b58106a03b` remains the head of `trybotster/botster-core` `main` when Implement runs. Implement must re-run `git ls-remote` and refuse to pin any other revision without a human answer.
- Cargo can fetch the Core repository over the network during Implement and Verify.
- The Hub production worker for live tests is built with `cargo build --locked -p botster-core-daemon --bin botster-session-worker` from the rolled lockfile; `tests/hub_daemon_lifecycle/process.rs::session_worker_binary_path` resolves it from the worktree target directory.

Unknowns:

- Whether the TUI-shaped lockstep build (proof B) compiles against the candidate `botster-hub-client`. TUI `main` (`dc7d600`) pins Hub `e864c3c`; Hub `main` has since merged client event subscriptions (`7a09292`). A compile error in TUI code against newer Hub client DTOs would be TUI-owned drift, not a Core feature-gate failure. Section 10 defines how Implement classifies and records that outcome.
- Whether `./test.sh --locked` completes in one clean default-concurrency run on this host. Known flake classes are documented in the Hub charter notes. Any failure requires isolation and a base comparison at `7a09292` with the old pin, with exact evidence.
- GitHub CI for Hub `main` is red before this change for an unrelated lint under Rust `1.97.0`. Local gates run on `1.92.0`. The Implement report must state both toolchains and must not claim GitHub CI green.

## 8. Affected surfaces and files

Revision literal sites (all change `8fce2041b9fe742cb2a6df9e74cb262606672742` to `7eafa470a18025895995bbedc20d34b58106a03b`):

| File | Line(s) | Purpose |
| --- | --- | --- |
| `Cargo.toml` | 24, 25, 26 | `botster-core`, `botster-core-daemon`, `botster-terminal-protocol` runtime pins |
| `Cargo.toml` | 43, 44 | `botster-core-test-support`, `botster-terminal-ghostty` dev pins |
| `crates/botster-hub-client/Cargo.toml` | 11 | Git-visible `botster-terminal-protocol` pin |
| `crates/botster-hub-test-support/Cargo.toml` | 17, 21, 34 | `botster-core`, `botster-terminal-protocol`, dev `botster-terminal-ghostty` |
| `crates/botster-hub-test-support/build.rs` | 10 | `PROTOCOL_REV` used to locate the fixture source through `cargo metadata` |
| `crates/botster-hub-test-support/src/conformance_data.rs` | 42 | `LATE_ATTACH_GHOSTSNP_CORE_PIN` |
| `crates/botster-hub-test-support/src/lib.rs` | 5946 | provenance unit test literal |
| `tests/session_projection_owner_loop.rs` | 9 | `REQUIRED_CORE_REV` manifest and lockfile identity test |
| `tests/hub_daemon_lifecycle/package_event_plane.rs` | 48 | live-proof locked Core assertion |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | 1589 | provenance log literal |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | 999 | provenance log literal |
| `Cargo.lock` | six `source =` lines | Core-family package sources |

New files:

- `docs/plans/roll-core-pin-after-incremental-attach-local-runtime-gate.md` (this plan, committed by the Plan step).
- `docs/reports/roll-core-pin-after-incremental-attach-local-runtime-gate-implement.md` (Implement report).

Unchanged by design: `LATE_ATTACH_GHOSTSNP_GHOSTTY_PIN`, `packages/hub-test-support/**`, `botster-ui-contract` tag, root `[patch]` table, `.github/workflows/*`, README.

Implementation procedure:

1. Edit the eleven source and manifest sites above with one exact replacement. Do not change URL strings or selector keywords.
2. Run `cargo fetch` (or `cargo metadata --format-version 1 >/dev/null`) without `--locked` so Cargo re-resolves only the changed Git sources. Then run `git diff --stat Cargo.lock` and confirm the diff is the six Core-family `source` lines only (twelve changed lines). If any other package moved, restore `Cargo.lock` from HEAD and redo the resolution with `cargo update -p <package> --precise` forms until the diff is exact.
3. Run `grep -rn 8fce2041b9fe742cb2a6df9e74cb262606672742 --exclude-dir=target --exclude-dir=docs .` and require zero matches (this includes `Cargo.lock`).
4. Run the gates in section 10.
5. Commit as one implementation commit (for example `Pin Hub to Core 7eafa47 after the IncrementalAttach local-runtime gate.`), then commit the report.

## 9. Risks

- Partial roll. One missed literal makes `git_visible_hub_members_share_one_exact_core_revision`, the `lib.rs` provenance test, or the event-plane live proof fail, or leaves a stale provenance log. Mitigation: the zero-match grep in step 3 and the precedent file set from `6988d90`.
- Lock drift. A broad `cargo update` would move unrelated crates. Mitigation: exact lock diff check in step 2.
- Identity split. Changing the URL form or selector on one member would create two `botster-terminal-protocol` identities. Mitigation: `cargo tree -e normal -i botster-terminal-protocol` must show one source.
- Fixture resolution. `build.rs` panics if `cargo metadata` does not contain `botster-terminal-protocol` at `PROTOCOL_REV`. Mitigation: roll `PROTOCOL_REV` with the manifests; the hub-test-support unit tests prove the copied bytes are unchanged (same lengths and SHA-256 constants).
- Suite flake. `./test.sh --locked` may hit a documented flake under default concurrency. Mitigation: isolate, compare against base `7a09292`, record exact evidence; do not retry silently.
- TUI-shaped proof drift (section 7). Mitigation: classification rule in section 10.
- Toolchain gap. Local clippy `1.92.0` cannot reproduce the CI `1.97.0` lint. Mitigation: report states the local toolchain and the pre-existing CI failure with run id `32329783340`; the changed files do not touch `crates/botster-hub-installation`.
- Revision moves. If Core `main` advances before Implement, the ticket still names the gated revision. Mitigation: pin exactly `7eafa470a18025895995bbedc20d34b58106a03b` unless a human answer selects a later revision.

## 10. Acceptance checks and tests

Repository gates (Hub charter and CI), run from the rolled worktree:

| Command | Expected |
| --- | --- |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 on local `1.92.0` |
| `./test.sh --locked` | one clean default-concurrency run; includes `node packages/hub-test-support/scripts/sync-assets.mjs --check` |
| `./test.sh --locked --test session_projection_owner_loop git_visible_hub_members_share_one_exact_core_revision -- --exact` | 1 passed against the new revision |
| `BOTSTER_ENV=test cargo test --locked -p botster-hub-test-support` | lib tests pass, including the `late_attach_ghostsnp_provenance` assertions with the new pin and unchanged fixture lengths and SHA-256 values |
| `./test.sh --locked --test hub_daemon_lifecycle_test package_event_plane -- --nocapture` (or the exact live event-plane test names in that module) | passes; log line shows `core_sha=7eafa470a18025895995bbedc20d34b58106a03b` |
| `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | exit 0; worker built from locked Core `7eafa47` |
| `cargo tree -e normal -i botster-terminal-protocol` | exactly one source `git+https://github.com/trybotster/botster-core.git?rev=7eafa470a18025895995bbedc20d34b58106a03b#7eafa470a18025895995bbedc20d34b58106a03b` |
| `cargo tree -p botster-hub-test-support -e normal --depth 1` | no `botster-terminal-protocol-client` |
| `git diff --stat origin/main..HEAD -- Cargo.lock` | only the six Core-family `source` lines changed |
| `grep -rn 8fce2041b9fe742cb2a6df9e74cb262606672742 --exclude-dir=target --exclude-dir=docs .` | zero matches |
| `git diff --check origin/main...HEAD` and PII grep over the branch diff | clean |

Proof A (required, ticket acceptance "contract-only Core consumer with default-features=false builds against that pin"):

1. Create a disposable crate outside the Hub workspace in the session scratchpad with an isolated `CARGO_TARGET_DIR`. No `[patch]` table.
2. Dependencies:
   - `botster-core = { git = "https://github.com/trybotster/botster-core.git", rev = "7eafa470a18025895995bbedc20d34b58106a03b", default-features = false }`
   - `botster-terminal-protocol-client = { git = "https://github.com/trybotster/botster-core.git", rev = "7eafa470a18025895995bbedc20d34b58106a03b" }`
   - `botster-hub-client = { git = "file:///<run worktree>", rev = "<candidate Hub commit>" }` (same shape as the prior Git-consumer smoke in `docs/reports/delete-hub-owned-terminal-goldens-and-consume-core-protocol-fixtures-implement.md`)
   - `main.rs` references one item from each crate (for example `botster_core::BotsterEngine` as a type path, a `botster_hub_client` request helper, and a protocol-client type) so all three compile into the binary.
3. Commands and expected results:
   - `cargo generate-lockfile` then `cargo build --locked` exit 0.
   - `cargo tree -e features,no-dev -i botster-core` shows zero `local-runtime` and zero `portable-pty`.
   - `cargo tree -e normal -i botster-terminal-protocol` shows one source at `rev=7eafa470a18025895995bbedc20d34b58106a03b`.
4. Red oracle: the same crate with every Core `rev` set to `8fce2041b9fe742cb2a6df9e74cb262606672742` (and `botster-hub-client` at base `7a09292`) must fail `cargo build` with `error[E0412]: cannot find type IncrementalAttach`. Record both results.

Proof B (required attempt, downstream-shaped TUI lockstep build):

1. Scratch clone of `trybotster/botster-tui` `origin/main` (record the SHA; `dc7d600` at plan time) in the scratchpad with an isolated `CARGO_TARGET_DIR`. No `[patch]` table.
2. In `crates/botster-tui/Cargo.toml` set every Core-family `rev` (`botster-core`, `botster-terminal-ghostty`, `botster-terminal-protocol-client`, `botster-core-test-support`) to `7eafa470a18025895995bbedc20d34b58106a03b`, keep `default-features = false` where present, and set `botster-hub-client` and `botster-hub-test-support` to `git = "file:///<run worktree>"`, `rev = "<candidate Hub commit>"`.
3. `cargo generate-lockfile`, then `cargo build -p botster-tui --locked`, then `cargo tree -p botster-tui -e features,no-dev -i botster-core` with zero `local-runtime`.
4. Classification rule:
   - exit 0: record as downstream proof passed.
   - failure whose first error is in `botster-core` or mentions `IncrementalAttach` or `local-runtime`: Hub or Core defect; stop and route a finding.
   - failure whose first error is in `botster-tui` source against `botster-hub-client` or `botster-hub-test-support` API changes: TUI-owned drift. Record the exact first error, file, and line in the report as residual risk; proof A remains the ticket acceptance; Review decides whether to accept. Do not waive silently and do not edit TUI code in this run.

Live-proof provenance: every live test log must show Hub SHA equal to the candidate commit and locked Core SHA `7eafa470a18025895995bbedc20d34b58106a03b`, following [[live hub proof records distinct hub and locked core binary provenance]].

## 11. Vault gaps worth capturing

- Hub Core pin-roll site inventory. Three rolls (`175dd36`, `6988d90`, `e864c3c`) and this plan touched the same eleven literal sites plus `Cargo.lock`, and no vault note lists them. Capture candidate: "Hub Core pin rolls update eleven literal sites and six lock sources" with the zero-match grep as the completion check.
- GitHub CI toolchain drift. Hub CI pins Rust `1.97.0` and Core CI floats `stable`; both `main` branches are red on clippy lints that the local `1.92.0` toolchain does not emit (`useless_conversion`, `drain_collect`). Capture candidate: local strict gates do not reproduce CI lints when the CI toolchain is newer; Review and Verify must record the CI run id and toolchain instead of claiming CI green.
- README `Dependency policy` prose is stale since `175dd36` (it still describes a `main` branch track). Capture candidate or docs follow-up ticket.
- The Core contract-only CI step at `7eafa47` was skipped on GitHub because Lint failed first; the only executed contract-only evidence is the local Core Verify artifact plus this ticket's proof A. Capture as a gotcha for Core CI ordering if the Core team wants the lane independent of Lint.
