# Implement report: Pin hub-client terminal-protocol to a Core revision

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | `botster-hub` |
| Pipeline worktree | the pipeline-provided ticket worktree |
| Ticket | `ticket_1786716545_950076` |
| Run | `run_1786717045_127787` |
| Step | `botster_stack_implement` (`run_step_1786718165_117179`) |
| Approved plan | `docs/plans/pin-hub-client-terminal-protocol-to-core-revision.md` revision 2 |
| Merge policy | direct into `main`; do not create a PR |
| Base | `origin/main` `aafd6c2cde430804f1bb54094c568fc88c15944b` |
| Core pin | `f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| Session-type eligibility consumer | false |
| `teardown_class_applies` | no |

Routing verified independently: `project_pipelines_current_context` ticket/run `target_id` and `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub`. The approved plan used the same routing. Implementation stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]] — ownership charter
- [[botster-hub-client-playbook]] — Git-consumed client crate overlay inside this repository
- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]] — loaded because the implementer overlay requires it; this ticket has no React or SPA edit surface

### Targeted atomic notes

- [[botster hub is a first party host profile over core]]
- [[botster hub client crate is the external client boundary]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[git-visible Hub member manifests must use the UI contract tag]]
- [[first-party Rust consumers pin the UI contract Git tag not a Hub rev]]
- [[botster rust consumers that share ui contract must pin one hub revision]]
- [[kit UI contract pin proof uses an already split TUI consumer]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[botster core contract surface needs consumer proof]]
- [[external client hub tests use subprocess spawned hub test support]]
- [[rust repo strict lints must be verified before dismissing warnings]]
- [[botster review agents must run verify strict gates not lighter equivalents]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[implementation artifacts must match actual git state]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[test script required for rust tests not cargo test]]

### Explicitly not loaded

- [[project-pipelines-playbook]] — Project Pipelines package and plugin paths are out of scope
- [[botster runtime teardown lenses]] — this ticket is a Cargo identity pin
- Other repository charters

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`
- Follow approved plan revision 2
- Pin `botster-terminal-protocol` and companion Core-family crates to `https://github.com/trybotster/botster-core.git` `rev = "f4f6bf5babe92dfb9241a760c414187f711c2c42"`
- Do not edit Core or TUI
- Do not add a TUI workspace `[patch]`
- Do not depend on `botster-terminal-protocol-client`
- Do not change Unix adapter policy, Hello admission, mux framing, or `core_adapter_closed`
- Do not change the UI-contract Git tag
- Do not bump protocol version, conformance revision, or hub-test-support npm version
- Use `./test.sh` for Rust tests
- Direct-merge pipeline: commit on the ticket branch; do not create a PR; Review runs before the Merge step

## Files changed

| Path | Change |
| --- | --- |
| `crates/botster-hub-client/Cargo.toml` | Pin `botster-terminal-protocol` to Core `.git` + rev `f4f6bf5...` |
| `Cargo.toml` | Same protocol pin; companion `botster-core`, `botster-core-daemon`, `botster-core-test-support`, and `botster-terminal-ghostty` pins; replace the tracks-main comment |
| `crates/botster-hub-test-support/Cargo.toml` | Companion `botster-core` and `botster-terminal-ghostty` pins to the same `.git` + rev |
| `Cargo.lock` | Rewrite six Core-family sources from `?branch=main#f4f6bf5` to `?rev=f4f6bf5#f4f6bf5` with `.git`. No other lock churn |
| `docs/plans/pin-hub-client-terminal-protocol-to-core-revision.md` | Approved plan revision 2 |
| `docs/reports/pin-hub-client-terminal-protocol-to-core-revision-implement-report.md` | This report |

No edits to `src/unix_terminal_adapter.rs`, `src/daemon_transport.rs`, `src/daemon_attach_stream.rs`, or `crates/botster-hub-client/src/lib.rs`.

## Ownership boundaries preserved

Hub owns this pin. `botster-hub-client` stays a Git-consumed member of this repository. The crate depends on types-only `botster-terminal-protocol`. It does not depend on Core runtime or `botster-terminal-protocol-client`.

The UI-contract tag on Git-visible member manifests is unchanged: `tag = "botster-ui-contract-v0.3.2"`.

Companion Core-family pins stay inside this Hub repository. They keep one `TerminalFrame` identity for `TerminalAdapter::try_write` and hub-client compatibility re-exports. They do not change adapter policy.

## Cross-repo dependencies or separately routed work

- Core owns the protocol crate source. This run does not edit Core.
- TUI ticket `ticket_1786661009_551067` already depends on this ticket. After this merge, that TUI run must bump its `botster-hub-client` Git revision and re-run its consumer tree. This run does not edit TUI.
- No new dependency ticket is required.

## Deviations from plan

None in product scope.

Process note: the plan lists "Merge directly to `main`" as an Implement acceptance check. The pipeline is Implement → Review → Verify → Merge with `merge_policy: direct`. This visit commits on the ticket branch and does not merge before Review. No PR is created.

## Tests and downstream proof run

Production entry point: Git consumers of `botster-hub-client` resolve `botster-terminal-protocol` from the member manifest. After this change, that manifest names Core `.git` rev `f4f6bf5babe92dfb9241a760c414187f711c2c42`. Hello admission types re-exported from hub-client share that identity with a TUI that pins `botster-terminal-protocol-client` to the same rev.

| Check | Result |
| --- | --- |
| `rg -n 'botster-terminal-protocol' Cargo.toml crates/botster-hub-client/Cargo.toml crates/botster-hub-test-support/Cargo.toml` | No `branch = "main"`. No Core URL without `.git` |
| `cargo update -p botster-core -p botster-core-daemon -p botster-core-test-support -p botster-terminal-protocol -p botster-terminal-ghostty` | Six Core-family sources moved from `?branch=main#f4f6bf5` to `.git?rev=f4f6bf5#f4f6bf5`. 140 other deps unchanged |
| `cargo tree -p botster-hub-client -e normal` | One `botster-terminal-protocol` package: `git+https://github.com/trybotster/botster-core.git?rev=f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| `cargo tree -p botster-hub -e normal` | One `botster-terminal-protocol` source, same identity |
| Disposable TUI worktree from parent `97b6202b33b77645a3527bd77e9f3bc3b2c0fdbe` | Changed only `botster-hub-client` to a path dep on the Hub candidate checkout. No `[patch]`. No other pin changes |
| `cargo tree -p botster-tui -e normal` in that worktree | One `botster-terminal-protocol` identity: `git+https://github.com/trybotster/botster-core.git?rev=f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| `cargo check -p botster-tui` in that worktree | Exit 0 |
| Disposable TUI worktree | Deleted after recording the tree and check. Live TUI parent checkout stayed at `97b6202` with a clean tree |
| `cargo check --workspace --locked` | Exit 0 |
| `./test.sh --test hub_client_api_test` | 32 passed, 0 failed |
| `./test.sh --test hub_daemon_lifecycle_test unix_adapter` | 8 passed, 0 failed, 178 filtered |
| `cargo fmt --all -- --check` | Exit 0 |
| `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | Exit 0 |

TUI parent base commit: `97b6202b33b77645a3527bd77e9f3bc3b2c0fdbe`. Hub candidate commit is recorded after this report is committed.

## Unverified behavior or residual risk

- This ticket does not run live Hub admission or teardown proofs. The plan excludes those paths.
- The parent TUI ticket still owns the later `botster-hub-client` Git rev bump after this merge. The disposable path-pin proof shows one protocol identity. It does not replace that TUI pin update.
- Future Core advances need an explicit Hub revision bump. Hub no longer floats Core `main`.
- `botster-terminal-protocol-client` appears in Hub `Cargo.lock` as a Core transitive crate. Hub and hub-client do not depend on it.

## Missing vault guidance discovered

The UI-contract notes cover tag versus rev. They did not name the `.git` suffix or the `branch` versus `rev` split for `botster-terminal-protocol`. Implement captured two inbox notes:

- `knowledge/inbox/cargo-git-url-form-and-branch-vs-rev-are-crate-identity.md`
- `knowledge/inbox/git-consumed-hub-members-must-not-float-core-main.md`

Vault checklist: `checklist_1786718261_713894` (run-scoped Implement vault workflow). Plan checklist `checklist_1786717437_932944` was left as the Plan visit record.
