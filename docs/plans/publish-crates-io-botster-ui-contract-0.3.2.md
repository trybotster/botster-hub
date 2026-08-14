# Plan: Hub Git tag botster-ui-contract-v0.3.2 as the consumer identity

This is the live plan. Verify must follow this document, not the Plan-time
crates.io artifact summary.

Implement-stage human answer `question_1786664733_777672` replaced crates.io
publication with Hub Git tag `botster-ui-contract-v0.3.2`. Review-stage human
answer `question_1786667676_293745` confirms this Hub ticket provides and proves
that source identity; durable TUI Kit and TUI manifest adoption belong to their
routed tickets.

## Superseded Plan-time decision (historical only)

These facts are not live instructions:

- Plan-time human answer `question_1786661321_439525` choose A asked for crates.io
  `botster-ui-contract = "0.3.2"` and forbade a Hub git tag. That answer is
  superseded.
- Plan-time empty `cargo search botster-ui-contract` and a missing Cargo token
  were crates.io publication premises. They are not current acceptance checks.
- Unmerged crates.io publication commit `fdfc80a` was reverted by `2779b58`.
- Do not publish this crate to crates.io. Do not request a crates.io token.

## Target and routing

| Field | Value |
| --- | --- |
| Target repository | `trybotster/botster-hub` (`botster-hub`) |
| Target ID | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1786661468_861481` |
| Run | `run_1786662525_387029` |
| Planned base | Hub `main` at `f9f0d8df997a1f59a7ac8d40cab1c06f363c5d7d` |
| Shipped tag commit | `0775e661e23790b4d68183851493c9f08df33803` |
| Repository ownership charter | [[botster-hub-playbook]] |
| Downstream consumer | TUI Kit ticket `ticket_1786661009_576857` (`tgt_3dfae49c02454037bf13554f552baf7f`) |
| Registered consumer edge | `dependency_1786661471_370439` (TUI Kit depends on this Hub ticket) |
| Live identity answer | `question_1786664733_777672` — Git tag, no crates.io |
| Downstream adoption answer | `question_1786667676_293745` — Hub proves the tag; kit/TUI adopt later |

Routing used spawn-target state. `tgt_7e208a0c76a44980a83b63af976b1f22` maps to
admitted name `botster-hub`, repo `trybotster/botster-hub`.

This is **not** runtime-teardown class. Do **not** load [[botster runtime teardown lenses]].

`[[project-pipelines-playbook]]` is **not** a product overlay.

## Repository playbook loaded

- [[botster-hub-playbook]] — authoritative charter for this ticket target

## Other role/surface playbooks and atomic notes loaded

Role entrypoints (required order):

1. [[planner-playbook]] / [[implementer-playbook]]
2. [[botster-planner-playbook]] / [[botster-implementer-playbook]]
3. [[botster-hub-playbook]]
4. Targeted atomic notes below
5. [[project-pipelines-playbook]] — not loaded as a product overlay

Required maps:

- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]] — overlay-required; does not constrain this tag/docs work
- [[prefer framework and library components over custom solutions]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[always look up latest dependency versions never use training cutoff]]
- [[cross repo dependency registration must use dependency repo target]]
- [[closed dependency tickets signal merged source not a consumable release]]
- [[test script required for rust tests not cargo test]]
- [[a root package workspace silently scopes cargo test to one package]]
- [[rust repo strict lints must be verified before dismissing warnings]]
- [[colon worktree paths break cargo dyld library paths]]
- [[hearth gate runs require restoring a pipeline wiped gitignore before attribution]]
- [[implementation deviations must resync committed plan acceptance checks]]

Primary identity notes:

- [[botster rust consumers that share ui contract must pin one hub revision]] —
  superseded for UI-contract identity. Live rule: pin Hub Git tag
  `botster-ui-contract-v0.3.2`. Manifests must use `tag` and must not use `rev`.
- [[git-visible Hub member manifests must use the UI contract tag]] — git-visible
  hub-client / hub-test-support declare the tag; workspace git `[patch]` is local only.
- [[first-party Rust consumers pin the UI contract Git tag not a Hub rev]] —
  TUI and TUI Kit adopt the same tag on their own tickets.
- [[public protocol versions host control and Core terminal planes independently]]
- [[botster package surface semantics live in ui contract while hub owns admission]]
- [[botster hub is a first party host profile over core]]
- [[botster hub client crate is the external client boundary]]
- [[TUI Kit pairing metadata does not authorize Hub test support dependencies]]
- [[kit UI contract pin proof uses an already split TUI consumer]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[Hub test support capability cutovers use a new unpublished package version]] —
  published npm `@trybotster/ui-contract@0.3.2` is immutable. Do not republish it.
- [[closed dependency tickets signal merged source not a consumable release]] —
  TUI Kit cannot consume a Hub merge. It needs the pushed tag, adopted on the
  kit ticket.
- [[blocking dependency premises must be revalidated per consuming crate]] —
  the kit ticket must revalidate the tag itself.

Seam guidance, not a second ownership charter:

- [[botster-hub-client-playbook]] — git-consumed `botster-hub-client` metadata
  is the Cargo identity seam. No DTO or protocol change.

## Context loaded

- Ticket (current): create GitHub tag `botster-ui-contract-v0.3.2` so TUI Kit and
  TUI can consume the Rust contract without a Hub `rev`.
- Crate version is `0.3.2`. npm `@trybotster/ui-contract@0.3.2` is already
  published and must stay unpublished by this ticket.
- Before this ticket, git-visible members path-depended the crate, so a git
  `hub-client` pin inherited a Hub SHA as the contract identity.
- TUI Kit `main` still uses `rev = "f9f0d8df..."`. That is expected until the
  kit ticket runs. This Hub ticket does not edit kit or TUI manifests.
- `script/publish-npm-packages` remains the npm release path. It is not a
  crates.io publish path for this crate.
- Direct merge to `main`. Do not create a PR.

## Scope

1. Do not publish crates.io `botster-ui-contract`.
2. Make git-consumed Hub package metadata resolve tag `botster-ui-contract-v0.3.2`:
   - `crates/botster-hub-client/Cargo.toml` and `crates/botster-hub-test-support/Cargo.toml`
     declare `botster-ui-contract = { git = "https://github.com/trybotster/botster-hub.git", tag = "botster-ui-contract-v0.3.2" }`
     with **no** `path`, `rev`, or crates.io source.
   - Keep Hub workspace path resolution for **local development only** via
     `[patch."https://github.com/trybotster/botster-hub.git"]`.
   - The root `botster-hub` package may keep a path dependency.
3. Keep npm `@trybotster/ui-contract@0.3.2` unchanged.
4. Provide `script/tag-ui-contract` and document the tag identity.
5. Merge to `main`, create and push annotated tag `botster-ui-contract-v0.3.2`
   on the merged contract commit.
6. Prove an external Cargo consumer compiles from the exact tag.
7. Prove a disposable TUI graph with git hub-client, tag UI contract, and a
   path-pinned kit that resolves exactly one tag source.
8. Keep the committed plan's live instructions aligned with the tag identity.
   `docs/client-protocol.md` examples that this ticket touches must install
   `@trybotster/ui-contract@0.3.2`, not `0.3.1`.

This ticket provides and proves the source identity. It does not wait for
durable TUI Kit or TUI manifest adoption.

## Non-scope

- Do not change UiNode schema, validation, fixtures, generated npm assets,
  protocol version, or conformance revision.
- Do not republish npm `@trybotster/ui-contract@0.3.2`.
- Do not publish `botster-hub`, `botster-hub-client`, `botster-hub-test-support`,
  or `botster-ui-contract` to crates.io.
- Do not use a Hub `rev` or crates.io version as the Rust consumer identity.
- Do not keep `{ version = "0.3.2", path = "..." }` on git-visible member
  manifests.
- Do not edit TUI Kit, TUI, Web, or Core as durable product changes.
- Do not perform TUI live Hub / Ghostty attach proof (`ticket_1786661009_551067`).
- Do not perform durable kit pin work (`ticket_1786661009_576857`).
- Do not load runtime-teardown lenses.

## Repository ownership boundaries and cross-repo dependencies

| Repository | Owns | This run |
| --- | --- | --- |
| `botster-hub` (`tgt_7e208a0c76a44980a83b63af976b1f22`) | UI contract crate, npm package, tag, tag script, and git-visible hub-client / hub-test-support metadata that must resolve the tag | **This run** |
| `botster-hub-client` (crate inside Hub) | Daemon protocol DTOs | Metadata-only dependency source change |
| `botster-tui-kit` (`tgt_3dfae49c02454037bf13554f552baf7f`) | Kit mechanics and durable contract pin | Downstream. Adopts the tag on its own ticket |
| `botster-tui` (`tgt_c3d470bab78549df920a41e8fb0e58d8`) | App policy and durable pins | Disposable identity proof only. Durable pin is `ticket_1786661009_551067` |
| `botster-web` | npm `@trybotster/ui-contract` consumer | No change. npm `0.3.2` already exists |
| `botster-core` | Terminal protocol | Out of scope |

A Core terminal capability change must not require a TUI Kit UI-contract tag change.

## Assumptions and unknowns

Assumptions:

1. Tag `botster-ui-contract-v0.3.2` is the consumer identity.
2. `{ version, path }` on a git-consumed member still yields a Hub SHA. The
   required mechanism is tag-only member deps plus a workspace-only git `[patch]`.
3. The root Hub path dependency does not leak into TUI/kit graphs.
4. Direct merge to `main` remains the merge policy.
5. Registered downstream tickets plus disposable compile proof satisfy this Hub
   ticket (`question_1786667676_293745`).

Unknowns that stop Implement:

1. After metadata change, a disposable TUI graph with git hub-client still
   resolves a Hub `rev` identity for `botster-ui-contract`. That is a packaging
   defect in this ticket.
2. Generated npm schema/fixtures diverge from published npm `0.3.2`. Stop and
   ask a human. Do not republish npm.

## Affected surfaces / files

| Path | Change |
| --- | --- |
| `crates/botster-ui-contract/Cargo.toml` | `readme` only. Do not bump `0.3.2` |
| `crates/botster-ui-contract/README.md` | Tag identity |
| `crates/botster-hub-client/Cargo.toml` | git tag pin, no path/rev/crates.io |
| `crates/botster-hub-test-support/Cargo.toml` | same tag pin |
| `Cargo.toml` | Workspace-only git `[patch]` to the path crate |
| `script/tag-ui-contract` | Create/verify the annotated tag |
| `docs/client-protocol.md` | Tag identity; npm examples `0.3.2` |
| `README.md` | Tag identity |
| `docs/plans/publish-crates-io-botster-ui-contract-0.3.2.md` | This plan |
| `docs/reports/publish-crates-io-botster-ui-contract-0.3.2.md` | Implement report |

Unchanged: crate source, generated npm assets, `script/publish-npm-packages`,
`packages/ui-contract/README.md` (frozen published npm `0.3.2` bytes).

## Implementation sequence

1. Land tag-only member manifests and the workspace git `[patch]`.
2. Land `script/tag-ui-contract` and tag documentation.
3. Align this plan and touched client-protocol examples with the tag identity.
4. Run Hub gates. Use `BOTSTER_ENV=test cargo test --locked -p botster-ui-contract`
   to isolate the crate: `./test.sh` always prepends `--workspace`, so
   `./test.sh --locked -p botster-ui-contract` still executes other members.
5. Merge to `main`. Create and push the annotated tag on the contract commit.
6. Prove the external tag consumer and the disposable TUI graph.
7. Write the implement report.

## Risks

- **Tag versus `rev` identity split.** A tag pin and a `rev` pin are different
  crates even at the same commit. Mitigation: git-visible members use the tag;
  disposable TUI proof must show one tag source.
- **Patch leaking.** Cargo does not transport `[patch]` through git
  dependencies. Still inspect the disposable TUI lockfile.
- **Local workspace fetch before the tag exists.** Land the workspace git
  `[patch]` in the same change.
- **Downstream kit/TUI still on Hub `rev` until their tickets.** Expected.
  Disposable graph proof overrides that locally.

## Acceptance checks / tests

Hub workspace (local-dev path patch is allowed here only):

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `./test.sh --locked` for the workspace wrapper
- Isolated crate tests: `BOTSTER_ENV=test cargo test --locked -p botster-ui-contract`
- `cargo tree -p botster-ui-contract` may show the path patch locally

Identity:

- GitHub exposes annotated tag `botster-ui-contract-v0.3.2`.
- `script/tag-ui-contract --verify` confirms that tag points at crate version `0.3.2`.
- `npm view @trybotster/ui-contract@0.3.2 version` remains `0.3.2` and was not republished.
- crates.io `botster-ui-contract` remains unpublished.
- Touched `docs/client-protocol.md` install examples use `@trybotster/ui-contract@0.3.2`.

External consumer (required):

- A temp crate outside the Hub workspace depends on
  `botster-ui-contract = { git = "https://github.com/trybotster/botster-hub.git", tag = "botster-ui-contract-v0.3.2" }`
  and compiles a public type such as `UiNode` or `validate_ui_node`.
- That consumer's `Cargo.lock` source is the Hub git tag, not crates.io and not a `rev`.

Disposable TUI graph:

- Pins `botster-hub-client` from Hub git at this ticket's published Hub revision.
- Pins `botster-ui-contract` from tag `botster-ui-contract-v0.3.2` with no `rev`.
- Path-pins TUI Kit.
- `cargo tree -i botster-ui-contract` resolves exactly one package from that tag.
- Handshake or fixture failures from unfinished TUI product work do not fail
  this ticket. Dual-source `E0308` does fail this ticket.

Downstream adoption is not a Hub-ticket merge gate:

- TUI Kit and TUI declare the tag on their routed tickets after this tag exists.
- A later integration ticket verifies both durable manifests.

Merge:

- Changes land on `main` with no PR.

## Vault gaps

Captured after the first Implement pass:

- first-party Rust consumers pin the UI contract Git tag not a Hub rev
- git-visible Hub member manifests must use the UI contract tag

## Runtime-teardown class

`teardown_class_applies`: no.

This ticket is tag identity and dependency metadata. It does not change
WebRTC/peer lifecycle, SessionIo/ClientWorker teardown, or resource spin.
