# Plan: Pin hub-client terminal-protocol to a Core revision

Ticket: `ticket_1786716545_950076`
Run: `run_1786717045_127787`
Step: `botster_stack_plan`
Pipeline: `botster_stack_delivery` (direct merge, no PR)
Required by TUI ticket `ticket_1786661009_551067` (already registered)
Parent review finding: `finding_1786715974_149013`
Plan **revision 2** after Plan Review `review_1786717915_698409`

## Plan Review corrections (rev 1 → rev 2)

| Finding | Class | Fix |
| --- | --- | --- |
| `finding_1786717915_247686` downstream proof is not the TUI consumer | product / high | Require a disposable worktree of TUI parent ticket `ticket_1786661009_551067`. Change only `botster-hub-client` to the candidate Hub checkout or candidate commit. Do not add a workspace `[patch]`. Run `cargo tree -p botster-tui -e normal` and `cargo check -p botster-tui`. Record Hub and TUI commits. |
| `finding_1786717915_410162` Unix adapter filter runs zero tests | product / medium | Replace `unix_terminal_adapter` with `unix_adapter`. That filter matches eight tests in `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`. |

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn-target name | `botster-hub` |
| Authoritative path | spawn target `botster-hub` |
| Plan worktree | this pipeline worktree; Plan does not mutate `Cargo.lock` |
| Worktree hygiene | tracked `.gitignore` has 53 bytes matching HEAD; path has no `:`; no `CARGO_TARGET_DIR` override |
| Base | `origin/main` `aafd6c2cde430804f1bb54094c568fc88c15944b` |
| Locked Core (current) | `Cargo.lock` records `botster-terminal-protocol` as `git+https://github.com/trybotster/botster-core?branch=main#f4f6bf5babe92dfb9241a760c414187f711c2c42` |
| Merge policy | direct into `main`; do not create a PR |
| Session-type eligibility consumer | **false** |
| `teardown_class_applies` | **no** |

Independent resolution: `project_pipelines_current_context` ticket/run `target_id` plus `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` → `botster-hub`. Routing did not use the process working directory.

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
- [[cross repo dependency registration must use dependency repo target]]
- [[colon worktree paths break cargo dyld library paths]]
- [[hearth gate runs require restoring a pipeline wiped gitignore before attribution]]

Repository overlay for the Git-consumed client crate inside this repo:

- [[botster-hub-client-playbook]]

Not loaded, with reason:

- [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope
- [[botster runtime teardown lenses]] — this ticket is a Cargo identity pin. It does not change WebRTC, SessionIo/ClientWorker teardown, multi-peer ownership, resource spin, or terminal-state vs live-runtime behavior
- other repository charters — this run stays on `botster-hub`

Targeted notes:

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

## Context loaded

Ticket intent: `botster-hub-client` and the Hub workspace pin `botster-terminal-protocol` to `git+https://github.com/trybotster/botster-core` `branch=main`. TUI consumers pin the same crate through `https://github.com/trybotster/botster-core.git` `rev=f4f6bf5babe92dfb9241a760c414187f711c2c42`. Cargo treats those URLs as two crate identities.

TUI Review `finding_1786715974_149013` observed the live consumer graph: Hello admission imported `botster_hub_client` terminal types from `branch=main` (resolved independently to `a047574` in that TUI lock) while `TerminalEvent` decoding used the `f4f6bf5` client crate. The files can match today. `cargo update` can move the branch source without moving the decoder pin.

Verified in this Hub worktree:

- `crates/botster-hub-client/Cargo.toml` line 11: `botster-terminal-protocol = { git = "https://github.com/trybotster/botster-core", branch = "main" }`
- root `Cargo.toml` line 26: the same `branch = "main"` form, plus `botster-core` / `botster-core-daemon` / test-support / ghostty also on `branch = "main"` without `.git`
- `crates/botster-hub-test-support/Cargo.toml`: `botster-core` and `botster-terminal-ghostty` also use `branch = "main"` without `.git`
- Hub `Cargo.lock` currently records those crates at SHA `f4f6bf5babe92dfb9241a760c414187f711c2c42` under source `git+https://github.com/trybotster/botster-core?branch=main#...`
- Git consumers of `botster-hub-client` do not inherit this lock. They resolve `branch=main` themselves.

Verified TUI parent branch `project-pipelines/ticket_1786661009_551067`:

```toml
botster-terminal-protocol-client = { git = "https://github.com/trybotster/botster-core.git", rev = "f4f6bf5babe92dfb9241a760c414187f711c2c42" }
botster-hub-client = { git = "https://github.com/trybotster/botster-hub.git", rev = "aafd6c2cde430804f1bb54094c568fc88c15944b" }
```

TUI `origin/main` currently pins Core `033cd01` and does not pin `botster-terminal-protocol-client`. The ticket names the parent TUI coordinate `f4f6bf5`, not `origin/main`.

Type-crossing that makes a protocol-only workspace pin unsafe:

- `UnixTerminalAdapter` implements `botster_core::contract::terminal_adapter::TerminalAdapter`
- that trait takes `&botster_terminal_protocol::TerminalFrame` from Core's path dependency
- Hub's impl takes `botster_terminal_protocol::TerminalFrame` from the workspace direct dep
- `daemon_transport` / `daemon_attach_stream` also pass `TerminalCompatibility*` values that `botster-hub-client` re-exports from its protocol dep

If Hub-client uses `https://github.com/trybotster/botster-core.git?rev=f4f6bf5` and Hub runtime still uses `https://github.com/trybotster/botster-core?branch=main`, those `TerminalFrame` / compatibility types are different Rust crates. The workspace will not type-check.

Current Hub comment at `Cargo.toml:23` says Hub tracks Core `main` and the lock records the tested revision. That policy is what lets Git consumers of hub-client float. This ticket replaces that float for the protocol crate.

No crates.io `botster-terminal-protocol` identity is in the TUI parent graph. Use the named Git rev.

## Scope

1. Change `crates/botster-hub-client/Cargo.toml` so `botster-terminal-protocol` is:

   ```toml
   botster-terminal-protocol = { git = "https://github.com/trybotster/botster-core.git", rev = "f4f6bf5babe92dfb9241a760c414187f711c2c42" }
   ```

2. Change the same crate in root `Cargo.toml` to that exact `.git` + `rev` form. Remove `branch = "main"` from every `botster-terminal-protocol` dependency in this repository.

3. Companion Core-family pin in this repository only, required for one Rust type identity inside Hub:

   - root `Cargo.toml`: `botster-core`, `botster-core-daemon`, `botster-core-test-support`, and `botster-terminal-ghostty` use the same `https://github.com/trybotster/botster-core.git` + `rev = "f4f6bf5babe92dfb9241a760c414187f711c2c42"`
   - `crates/botster-hub-test-support/Cargo.toml`: the same form for `botster-core` and `botster-terminal-ghostty`
   - replace the "tracks botster-core main" comment with a one-line note that the protocol identity and Hub runtime Core share this rev

   This is not adapter-policy work. It is the minimum lock alignment so `TerminalAdapter::try_write(&TerminalFrame)` and hub-client compatibility re-exports stay one crate.

4. Update `Cargo.lock` so every `botster-terminal-protocol` source is `git+https://github.com/trybotster/botster-core.git?rev=f4f6bf5babe92dfb9241a760c414187f711c2c42#f4f6bf5babe92dfb9241a760c414187f711c2c42`. No remaining `branch=main` protocol source.

5. Commit the plan, the pin, the lock, and a short implement report. Merge directly to `main`. Do not open a PR.

## Non-scope

- Do not commit edits in `botster-tui` or `botster-core`. A disposable TUI worktree is required proof only. Delete it after recording the tree and check. Do not merge TUI changes from this Hub run.
- Do not change Unix adapter policy, Hello admission, mux framing, or `core_adapter_closed` behavior. Those belong to sibling `ticket_1786716545_417854` or already-shipped adapter work.
- Do not depend on `botster-terminal-protocol-client` from Hub or hub-client.
- Do not change `botster-ui-contract` tag identity.
- Do not add a workspace `[patch]` so TUI can unify crates. The TUI consumer must unify without a patch.
- Do not publish a crates.io protocol crate in this ticket.
- Do not bump host protocol version, conformance revision, or hub-test-support npm version. Manifest pins only.
- Do not treat this ticket as a session-type eligibility consumer.
- Do not run live Hub admission or teardown proofs. This ticket does not change those paths.

## Repository ownership boundaries and cross-repo dependencies

Hub owns this pin. `botster-hub-client` is a Git-consumed member of this repository. The crate may depend on types-only `botster-terminal-protocol`. It must not depend on Core runtime or `botster-terminal-protocol-client`.

Core owns the protocol crate source and the `TerminalAdapter` trait that names `TerminalFrame`. This ticket only changes how Hub names that crate. It does not edit Core.

TUI owns the consumer pin of `botster-terminal-protocol-client` and the later `cargo tree -p botster-tui` proof. TUI ticket `ticket_1786661009_551067` already depends on this ticket (`dependency_1786716578_236639`). After this merge, TUI Implement updates its hub-client pin and re-runs the consumer tree. This run must not edit TUI.

No new dependency ticket is required. The Core rev already exists. The TUI parent already points at this Hub target.

## Assumptions and unknowns

- Assumption: the ticket's named coordinate `f4f6bf5babe92dfb9241a760c414187f711c2c42` is the protocol identity. Do not silently jump to TUI `origin/main` Core `033cd01`.
- Assumption: the `.git` URL form is load-bearing. `https://github.com/trybotster/botster-core` and `https://github.com/trybotster/botster-core.git` are different Cargo sources even at the same SHA.
- Assumption: pinning the Hub workspace Core git family to that same `.git` + rev is required. A protocol-only workspace change leaves `TerminalFrame` split between Core's path dep and hub-client.
- Assumption: aligning `botster-hub-test-support` Core/ghostty pins is required once the workspace Core URL form changes. Otherwise the lock keeps two Core sources.
- Assumption: this is not a session-type eligibility consumer and not runtime-teardown class.
- Assumption: TUI parent branch `project-pipelines/ticket_1786661009_551067` at commit `97b6202` is the required consumer graph. Do not use TUI `origin/main`, which does not pin `botster-terminal-protocol-client`.
- Unknown until Implement: exact `cargo update` invocation needed after the URL form change. Prefer package-scoped update of the Core git crates, then inspect the lock. Do not run an unbounded workspace `cargo update` of unrelated crates.
- Unknown until Implement: whether Hub `./test.sh` needs a fresh target dir after the source-URL change of same-version crates. If compile uses stale path artifacts, refresh `CARGO_TARGET_DIR` / local `target`.
- Unknown until Implement: whether the disposable TUI worktree needs a colon-free `CARGO_TARGET_DIR`. Set one if the scratch path contains `:`.

## Affected surfaces/files

| File | Change |
| --- | --- |
| `crates/botster-hub-client/Cargo.toml` | pin `botster-terminal-protocol` to `.git` + rev `f4f6bf5...` |
| `Cargo.toml` | same protocol pin; companion Core-family pins; replace tracks-main comment |
| `crates/botster-hub-test-support/Cargo.toml` | companion `botster-core` / `botster-terminal-ghostty` pins to the same `.git` + rev |
| `Cargo.lock` | rewrite Core git sources from `?branch=main#f4f6bf5` to `?rev=f4f6bf5#f4f6bf5` with `.git` |
| `docs/plans/pin-hub-client-terminal-protocol-to-core-revision.md` | this plan |
| `docs/reports/pin-hub-client-terminal-protocol-to-core-revision-implement-report.md` | Implement report |

Do not change `src/unix_terminal_adapter.rs`, `src/daemon_transport.rs`, `src/daemon_attach_stream.rs`, or `crates/botster-hub-client/src/lib.rs` unless the pin exposes a compile error that is not a crate-identity mismatch.

## Risks

- Protocol-only pin without companion Core pins: Hub fails to compile with `E0308` on `TerminalAdapter` / compatibility types.
- Keeping the no-`.git` URL: TUI still sees two `botster-terminal-protocol` identities.
- Unbounded `cargo update`: unrelated lock churn hides the identity change.
- Committing TUI edits or adding a TUI workspace `[patch]`: wrong repository. The disposable worktree may change only the `botster-hub-client` line, then must be deleted.
- Using TUI `origin/main` as the consumer: that tree does not pin `botster-terminal-protocol-client`, so it cannot expose the identity split.
- Treating lock SHA equality as identity: `branch=main#f4f6bf5` is still not `rev=f4f6bf5` on the `.git` URL.
- Future Core advances: Hub no longer floats Core `main`. A later Core feature needs an explicit Hub rev bump. That is the cost of one crate identity.

## Acceptance checks/tests

Production entry point: Git consumers of `botster-hub-client` resolve `botster-terminal-protocol` from the member manifest, not from Hub's lock. After this change, that manifest names one versioned Core identity. Hello admission types re-exported from hub-client then share that identity with a TUI that pins `botster-terminal-protocol-client` to the same rev.

Implement must prove:

1. `rg -n 'botster-terminal-protocol' Cargo.toml crates/botster-hub-client/Cargo.toml crates/botster-hub-test-support/Cargo.toml` shows no `branch = "main"` and no `https://github.com/trybotster/botster-core"` without `.git`.
2. `cargo tree -p botster-hub-client -e normal` prints exactly one `botster-terminal-protocol` package. Its source is `git+https://github.com/trybotster/botster-core.git?rev=f4f6bf5babe92dfb9241a760c414187f711c2c42`.
3. `cargo tree -p botster-hub -e normal` also prints one `botster-terminal-protocol` source. This is the Hub type-unification check for the companion Core pin.
4. Downstream TUI consumer proof in this Hub run, not deferred to the parent ticket:
   - create a disposable worktree from TUI ticket `ticket_1786661009_551067` branch `project-pipelines/ticket_1786661009_551067` (current consumer commit `97b6202`, or the tip of that branch if it still pins `botster-terminal-protocol-client` to Core `.git` rev `f4f6bf5babe92dfb9241a760c414187f711c2c42`)
   - do not use the live TUI implement checkout
   - in that disposable worktree only, change `botster-hub-client` to the candidate Hub checkout (`path = "<hub-worktree>/crates/botster-hub-client"`) or to the candidate Hub commit after it exists
   - do not add a workspace `[patch]`
   - do not change `botster-terminal-protocol-client`, `botster-core`, or other TUI pins
   - run `cargo tree -p botster-tui -e normal`
   - that tree must print exactly one `botster-terminal-protocol` identity: `git+https://github.com/trybotster/botster-core.git?rev=f4f6bf5babe92dfb9241a760c414187f711c2c42`
   - run `cargo check -p botster-tui` against that same graph
   - record the Hub candidate commit and the TUI worktree base commit in the implement report
   - delete the disposable TUI worktree; do not commit TUI changes
5. Workspace compile: `cargo check --workspace --locked` (or the repo equivalent after lock refresh).
6. Targeted tests that already import protocol types:
   - `./test.sh --test hub_client_api_test`
   - `./test.sh --test hub_daemon_lifecycle_test unix_adapter`
   - Plan Review independently executed that filter and got eight matching tests. The old `unix_terminal_adapter` filter executed zero tests.
7. Format and strict lint, raw cargo output, not an RTK summary:
   - `cargo fmt --all -- --check`
   - `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
8. If clippy, Hub tests, or the TUI `cargo check` fail on untouched files, compare the same command on the recorded base commits. Attribute pre-existing failures. Do not use them as a blanket excuse.
9. Merge directly to `main`. Do not create a PR.

This Hub run must produce the TUI `cargo tree -p botster-tui -e normal` one-identity result. The parent TUI ticket still owns its later hub-client Git rev bump after this merge.

## Vault gaps worth capturing

- Cargo treats `https://github.com/trybotster/botster-core` `branch=main` and `https://github.com/trybotster/botster-core.git` `rev=<sha>` as different crate identities even when both resolve to the same commit. The UI-contract notes cover rev vs tag. They do not yet name the `.git` suffix or `branch` vs `rev` split for `botster-terminal-protocol`.
- Hub's former "track Core main, lock the SHA" policy is unsafe for Git-consumed member crates. Consumers do not inherit `Cargo.lock`. Capture after Implement proves the new pin, or record why the existing UI-contract identity notes are enough.

No capture in this Plan visit. Implement or a later capture pass should write the inbox note if the tree proof confirms the identity split.

## Product decision ledger

| Item | Decision |
| --- | --- |
| Default | Pin protocol and Hub Core git family to TUI parent rev `f4f6bf5` on the `.git` URL |
| Non-goal | Adapter policy, TUI edits, crates.io publish, protocol version bump |
| Follow-up OK | TUI parent re-pins hub-client Git rev after this merge; later Hub Core rev bumps are explicit |
| Ask-human threshold | A published protocol identity that TUI already consumes as one crate, if Implement finds one that is not this Git rev |
