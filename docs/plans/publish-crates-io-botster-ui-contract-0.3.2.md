# Plan: Hub Git tag botster-ui-contract-v0.3.2 as the consumer identity

Implement-stage human answer `question_1786664733_777672` revised this ticket:
do not publish crates.io, do not request a crates.io token, and use GitHub tag
`botster-ui-contract-v0.3.2` as the Rust consumer identity. The Plan-time
crates.io decision is superseded. This plan is resynchronized to that answer.

## Target and routing

| Field | Value |
| --- | --- |
| Target repository | `trybotster/botster-hub` (`botster-hub`) |
| Target ID | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786661468_861481` |
| Run | `run_1786662525_387029` |
| Step | `botster_stack_plan` |
| Project | `project_1786660949_205223` — Botster Terminal Transport North Star |
| Planned base | Hub `main` at `f9f0d8df997a1f59a7ac8d40cab1c06f363c5d7d` |
| Repository ownership charter | [[botster-hub-playbook]] |
| Downstream consumer | TUI Kit ticket `ticket_1786661009_576857` (`tgt_3dfae49c02454037bf13554f552baf7f`) |
| Registered consumer edge | `dependency_1786661471_370439` (TUI Kit depends on this Hub ticket) |
| Human identity answer | `question_1786661321_439525` — choose A |

Routing used spawn-target state. `tgt_7e208a0c76a44980a83b63af976b1f22` maps to admitted name `botster-hub`, repo `trybotster/botster-hub`. The ambient process directory was not used as routing authority.

This is **not** runtime-teardown class. Do **not** load [[botster runtime teardown lenses]].

This is **not** a consumer of Hub session-type eligibility parent work. Do not inject `list_session_types_for_target` pins. Do not apply hub-test-support 0.1.26 / conf 33 session-type floors.

`[[project-pipelines-playbook]]` is **not** loaded as a product overlay. Package/plugin paths and workflow-policy source are out of scope. Pipeline discipline uses Project Pipelines checklist, artifact, and gate tools only.

## Repository playbook loaded

- [[botster-hub-playbook]] — authoritative charter for this ticket target

## Other role/surface playbooks and atomic notes loaded

Role entrypoints (required order):

1. [[planner-playbook]]
2. [[botster-planner-playbook]]
3. [[botster-hub-playbook]]
4. Targeted atomic notes and task-surface guidance below
5. [[project-pipelines-playbook]] — not loaded

Botster maps required by the planner overlay:

- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]] — loaded because the planner overlay requires it. It does not constrain this Rust/npm publish.
- [[prefer framework and library components over custom solutions]]
- [[project pipeline orchestration belongs in a device-level botster plugin]]
- [[project pipelines needs an operator workbench not more primitives]]
- [[project pipelines ui contract belongs in the plugin readme]]
- [[botster orchestration should spawn agents with explicit target ids]]
- [[botster orchestration prompts must bind agents to explicit worktrees]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[vault example paths are not repository placement conventions]]
- [[plan steps need reviewable plan artifacts]]
- [[plan review must verify a plan artifact exists before trusting gate summaries]]
- [[pipeline artifacts should cite vault notes by wikilink not home path]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[always look up latest dependency versions never use training cutoff]]
- [[cross repo dependency registration must use dependency repo target]]
- [[closed dependency tickets signal merged source not a consumable release]]
- [[test script required for rust tests not cargo test]]
- [[a root package workspace silently scopes cargo test to one package]]
- [[rust repo strict lints must be verified before dismissing warnings]]
- [[botster review agents must run verify strict gates not lighter equivalents]]
- [[colon worktree paths break cargo dyld library paths]]
- [[hearth gate runs require restoring a pipeline wiped gitignore before attribution]]

Primary ticket / identity notes:

- [[botster rust consumers that share ui contract must pin one hub revision]] — **current vault rule**. Implement-stage human answer `question_1786664733_777672` supersedes crates.io and Hub `rev` identity. Cargo identity must become Hub Git tag `botster-ui-contract-v0.3.2`. Manifests must use `tag` and must not use `rev`.
- [[public protocol versions host control and Core terminal planes independently]] — clients pin protocol and contract versions, not Hub revisions. Current Hub Git identity is the cost this ticket removes for the UI contract.
- [[botster package surface semantics live in ui contract while hub owns admission]] — contract ownership stays in Hub. This run publishes that crate; it does not move the owner.
- [[botster hub is a first party host profile over core]] — Hub remains the host profile. The published crate is a sibling contract package, not a Core or TUI Kit extraction.
- [[botster hub client crate is the external client boundary]] — git `botster-hub-client` stays a Hub-git consumer pin. Its package metadata must stop forcing a second `botster-ui-contract` Git identity.
- [[TUI Kit pairing metadata does not authorize Hub test support dependencies]] — TUI Kit still pins only the UI contract. This run does not add hub-client or test-support to the kit.
- [[kit UI contract pin proof uses an already split TUI consumer]] — the disposable TUI graph is identity proof, not unfinished TUI product proof.
- [[scratch cargo patch redirects measure downstream dto breakage]] — disposable consumer overrides are proof, not durable product pins.
- [[Hub test support capability cutovers use a new unpublished package version]] — published npm `@trybotster/ui-contract@0.3.2` is immutable. Do not republish it.
- [[hub test support npm releases need external consumer smoke]] — registry publication needs an installed consumer outside this workspace, not only local pack/check.
- [[closed dependency tickets signal merged source not a consumable release]] — TUI Kit cannot consume a Hub merge. It needs the crates.io coordinate.
- [[blocking dependency premises must be revalidated per consuming crate]] — after publish, the downstream kit run must revalidate crates.io `0.3.2` itself.

Seam guidance, not a second ownership charter:

- [[botster-hub-client-playbook]] — loaded only because git-consumed `botster-hub-client` metadata is the Cargo identity seam. This run does not change daemon DTOs, protocol version, or TypeScript generation.

## Context loaded

- Ticket: publish a versioned Rust UI contract identity that TUI Kit and TUI can consume without a Hub Git SHA.
- Human answer `question_1786661321_439525`: choose A. Publish crates.io `botster-ui-contract = "0.3.2"`. TUI Kit must not publish or extract the crate. Do not use a Hub git tag or Hub commit SHA as the consumer identity.
- Current crate version is already `0.3.2` in `crates/botster-ui-contract/Cargo.toml`. npm `@trybotster/ui-contract@0.3.2` is already on the public npm registry.
- Plan-time `cargo search botster-ui-contract` returned no crates.io crate. The crates.io HTTP API rejected anonymous access; treat empty `cargo search` as unpublished until Implement revalidates.
- `cargo package -p botster-ui-contract --allow-dirty --no-verify` already packs 11 files including `LICENSE`. The crate has no path dependencies.
- Hub workspace members still path-depend the crate:

  ```toml
  # crates/botster-hub-client/Cargo.toml
  botster-ui-contract = { path = "../botster-ui-contract" }

  # crates/botster-hub-test-support/Cargo.toml
  botster-ui-contract = { path = "../botster-ui-contract" }
  ```

  A consumer that pins `botster-hub-client` from Hub git therefore inherits `git+https://github.com/trybotster/botster-hub.git?<sha>` as the `botster-ui-contract` Cargo identity. `{ version = "0.3.2", path = "..." }` does **not** fix that: Cargo still uses the path when the parent crate is a git dependency.
- TUI Kit `main` still pins:

  ```toml
  botster-ui-contract = { git = "https://github.com/trybotster/botster-hub.git", rev = "f9f0d8df997a1f59a7ac8d40cab1c06f363c5d7d" }
  ```

  That kit ticket is already blocked on this Hub ticket (`dependency_1786661471_370439`). This run does not edit TUI Kit.
- `script/publish-npm-packages` already packs and publishes `@trybotster/ui-contract` and `@trybotster/hub-test-support` together. It does not publish the Rust crate or enforce crates.io lockstep.
- `packages/ui-contract/README.md` still documents a manual `npm publish` after merge. That is not one release process.
- Worktree hygiene: tracked `.gitignore` is present and unchanged (5 lines). The ticket worktree path has no `:`; no `CARGO_TARGET_DIR` override is required.
- `CARGO_REGISTRY_TOKEN` is unset and no Cargo credentials file is present in this Plan environment. Implement must fail closed and ask a human for crates.io publish rights. Dry-run is not publication.
- Repo-owned gates: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `./test.sh --locked`.
- Direct merge to `main`. Do not create a PR.

## Scope

1. Do not publish crates.io `botster-ui-contract`. Revert unmerged crates.io publication changes from `fdfc80a`.
2. Make git-consumed Hub package metadata resolve tag `botster-ui-contract-v0.3.2`:
   - `crates/botster-hub-client/Cargo.toml` and `crates/botster-hub-test-support/Cargo.toml` must declare `botster-ui-contract = { git = "https://github.com/trybotster/botster-hub.git", tag = "botster-ui-contract-v0.3.2" }` with **no** `path`, `rev`, or crates.io source.
   - Keep Hub workspace path resolution for **local development only** via a workspace `[patch."https://github.com/trybotster/botster-hub.git"]` pointing at `crates/botster-ui-contract`.
   - The root `botster-hub` package may keep a path dependency.
3. Keep npm `@trybotster/ui-contract@0.3.2` unchanged. Do not republish it.
4. Add the smallest tag create/verify support (`script/tag-ui-contract`) and document the tag identity in the crate README, root README, and client-protocol.
5. Prove an external Cargo consumer outside the Hub workspace depends on the tag and compiles a public contract type.
6. Prove a disposable TUI graph that pins:
   - `botster-hub-client` from Hub **git** at this ticket's merged/pushed Hub revision;
   - `botster-ui-contract` from Hub git **tag** `botster-ui-contract-v0.3.2` (no `rev`);
   - a path-pinned TUI Kit;
   and resolves **exactly one** `botster-ui-contract` source from that tag, with no Hub `rev` identity and no crates.io identity.
7. Merge directly into `main`. Create and push annotated tag `botster-ui-contract-v0.3.2` on the merged main commit. Do not create a PR.

## Non-scope

- Do not change UiNode schema, validation, fixtures, generated npm assets, protocol version, or conformance revision.
- Do not republish npm `@trybotster/ui-contract@0.3.2`.
- Do not publish `botster-hub`, `botster-hub-client`, or `botster-hub-test-support` to crates.io.
- Do not use a Hub git tag or Hub commit SHA as the consumer identity.
- Do not keep `{ version = "0.3.2", path = "..." }` on git-visible `hub-client` / `hub-test-support` manifests. That still yields a Hub Git SHA identity for git consumers.
- Do not edit TUI Kit, TUI, Web, or Core as durable product changes.
- Do not perform TUI live Hub / Ghostty attach proof. That belongs to TUI ticket `ticket_1786661009_551067`.
- Do not perform kit pin work. That belongs to `ticket_1786661009_576857` after this ticket closes.
- Do not load or apply runtime-teardown lenses.
- Do not dual-pipeline for planner variety.

## Repository ownership boundaries and cross-repo dependencies

| Repository | Owns | This run |
| --- | --- | --- |
| `botster-hub` (`tgt_7e208a0c76a44980a83b63af976b1f22`) | `botster-ui-contract` crate, npm `@trybotster/ui-contract`, publication, release process, and git-visible hub-client / hub-test-support metadata that must resolve crates.io | **This run** |
| `botster-hub-client` (crate inside Hub) | Daemon protocol DTOs | Metadata-only dependency source change. No DTO or protocol change. |
| `botster-tui-kit` (`tgt_3dfae49c02454037bf13554f552baf7f`) | Reusable Ratatui/Crossterm UiNode mechanics and the kit's contract pin | Downstream only. Already depends on this ticket. Not edited here. |
| `botster-tui` (`tgt_c3d470bab78549df920a41e8fb0e58d8`) | App policy and durable multipath pins | Disposable identity/compile proof only. Durable TUI pin is `ticket_1786661009_551067`. |
| `botster-web` | npm `@trybotster/ui-contract` consumer | No change. npm `0.3.2` already exists. |
| `botster-core` | Terminal protocol and adapter contract | Out of scope. |

Cross-repo rule: [[cross repo dependency registration must use dependency repo target]]. This ticket is the Hub-target prerequisite. Do not spawn kit or TUI work onto this Hub target. Do not add a reverse dependency from this ticket onto TUI Kit.

## Assumptions and unknowns

Assumptions:

1. Implement-stage human answer `question_1786664733_777672` supersedes Plan-time choose A. Hub Git tag `botster-ui-contract-v0.3.2` is the consumer identity. crates.io is not published.
2. Empty `cargo search botster-ui-contract` means the crate is unpublished. Implement must revalidate immediately before `cargo publish`.
3. Local crate `0.3.2` is the same contract version as already-published npm `@trybotster/ui-contract@0.3.2`. Implement must prove that before calling them one release. If generated schema/fixtures diverge from the published npm tarball, stop and ask a human. Do not republish npm `0.3.2` and do not silently publish a drifted Rust crate under the same version.
4. `{ version, path }` on a git-consumed member is not enough. Version-only member deps plus a workspace-only `[patch.crates-io]` is the Cargo mechanism that keeps local Hub development on the path crate while git hub-client consumers resolve crates.io.
5. The root Hub package path dependency does not leak into TUI/TUI Kit graphs.
6. `publish = false` on unpublished workspace members is allowed hygiene if needed to prevent accidental `cargo publish --workspace`.
7. Direct merge to `main` remains the project merge policy.
8. A path-only disposable hub-client pin is rehearsal, not acceptance. Final graph proof must use Hub **git**.

Unknowns that stop Implement rather than invent a coordinate:

1. crates.io name is taken by an unrelated crate, yanked, or already published with different bytes.
2. crates.io publish rights are unavailable in the implement environment. Ask a human. Do not treat `cargo publish --dry-run` or a local `.crate` as the consumer identity.
3. Published npm `0.3.2` bytes do not match this tree's generated contract assets.
4. After metadata change, a disposable TUI graph with git hub-client still resolves a git `botster-ui-contract` source. That is a packaging defect in this ticket, not a kit workaround.

## Affected surfaces / files

| Path | Change |
| --- | --- |
| `crates/botster-ui-contract/Cargo.toml` | Add `readme` only. Do not bump `0.3.2`. |
| `crates/botster-ui-contract/README.md` | Document the Git tag identity |
| `crates/botster-hub-client/Cargo.toml` | git tag `botster-ui-contract-v0.3.2`, no path/rev/crates.io |
| `crates/botster-hub-test-support/Cargo.toml` | same tag pin |
| `Cargo.toml` | Workspace-only `[patch."https://github.com/trybotster/botster-hub.git"]` to the path crate |
| `Cargo.lock` | Refresh only if the tag+patch identity requires it |
| `script/tag-ui-contract` | Create/verify the annotated tag. No crates.io publish |
| `packages/ui-contract/README.md` | Unchanged. npm `0.3.2` stays unpublished |
| `docs/plans/publish-crates-io-botster-ui-contract-0.3.2.md` | This plan |
| `docs/reports/publish-crates-io-botster-ui-contract-0.3.2.md` | Implement report after publication and graph proof |

Not changed unless packaging requires it: `crates/botster-ui-contract/src/**`, generated npm assets, protocol DTOs, fixtures.

## Implementation sequence

1. Revert unmerged crates.io publication changes from `fdfc80a`.
2. Change git-visible member manifests to tag `botster-ui-contract-v0.3.2` and add the workspace git `[patch]`.
3. Land `script/tag-ui-contract` and document the tag identity.
4. Run Hub workspace gates. The patch must keep local development on the path crate.
5. Merge directly to `main`. Create and push annotated tag `botster-ui-contract-v0.3.2` on the merged main commit.
6. Prove an external consumer against the tag.
7. Prove the disposable TUI graph against Hub git hub-client + tag UI contract + path kit.
8. Write the implement report.

## Risks

- **Publish credentials.** This environment has no Cargo token. Mitigation: ask a human; do not waive publication.
- **Irreversible crates.io version.** A bad `0.3.2` cannot be overwritten. Mitigation: package, compare with npm `0.3.2`, smoke the `.crate`, then publish last.
- **`{ version, path }` false fix.** Git consumers would still get a Hub SHA. Mitigation: version-only on git-visible members; patch only at workspace root.
- **Patch leaking to consumers.** Cargo does not transport `[patch]` through git dependencies. Still verify the disposable TUI lockfile contains no Hub-git `botster-ui-contract` source.
- **Local workspace breakage before publish.** Version-only members fail `cargo fetch` until patch exists or the crate is published. Land patch in the same change.
- **npm/crate drift under the same version.** Mitigation: byte/content compare before publish; ask a human if they diverge.
- **Downstream kit still on Hub git until its ticket runs.** Expected. This ticket's disposable graph must override that by using a TUI workspace pin of crates.io `0.3.2` plus a path kit, not by editing kit `main`.

## Acceptance checks / tests

Hub workspace (production entry is the published crate plus git-visible member metadata):

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `./test.sh --locked` and `./test.sh --locked -p botster-ui-contract`
- `cargo package -p botster-ui-contract`
- `cargo tree -p botster-ui-contract` inside the Hub workspace may show the path patch. That is local-dev only, not consumer proof.

Registry and lockstep:

- `npm view @trybotster/ui-contract@0.3.2 version` remains `0.3.2` and was not republished.
- `script/tag-ui-contract --verify` confirms tag `botster-ui-contract-v0.3.2` points at crate version `0.3.2`.
- crates.io `botster-ui-contract` remains unpublished.

External consumer (required; code existence is not enough):

- A temp crate outside the Hub workspace depends on `botster-ui-contract = { git = "https://github.com/trybotster/botster-hub.git", tag = "botster-ui-contract-v0.3.2" }` and compiles a public type such as `UiNode` or `validate_ui_node`.
- That consumer's `Cargo.lock` source is the Hub git tag, not crates.io and not a `rev`.

Disposable TUI graph (charter downstream proof):

- Pins `botster-hub-client` from Hub git at this ticket's published Hub revision.
- Pins `botster-ui-contract` from Hub git tag `botster-ui-contract-v0.3.2` with no `rev`.
- Path-pins TUI Kit.
- `cargo tree -i botster-ui-contract` resolves exactly one package from that tag.
- `Cargo.lock` has one `botster-ui-contract` entry and no Hub `rev` or crates.io identity for that crate.
- The `botster-tui` binary compiles. Handshake or fixture failures from unfinished TUI product work do not fail this ticket ([[kit UI contract pin proof uses an already split TUI consumer]]). Dual-source `E0308` does fail this ticket.

Merge:

- Changes land on `main` with no PR.

## Vault gaps worth capturing

1. [[botster rust consumers that share ui contract must pin one hub revision]] will be stale for consumer identity after this ticket. Capture that first-party Rust consumers pin Hub Git tag `botster-ui-contract-v0.3.2` and must not use `rev` or crates.io.
2. New convention candidate: Hub workspace git `[patch]` is local-only; git-visible member manifests for the UI contract must use the versioned tag.
3. Capture the Implement-stage human revision that superseded crates.io choose A.

Do not capture those notes during Plan. Capture after Implement proves the coordinate, or record why no capture was needed if the existing notes are updated in a later vault pass.

## Runtime-teardown class

`teardown_class_applies`: no.

This ticket is packaging, registry identity, and dependency metadata. It does not change WebRTC/peer lifecycle, SessionIo/ClientWorker teardown, multi-peer ownership, CPU/battery/FD spin, or terminal-state vs live-runtime divergence.
