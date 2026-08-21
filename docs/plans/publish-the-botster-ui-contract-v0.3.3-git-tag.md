# Plan: publish the botster-ui-contract-v0.3.3 Git tag for Rust consumers

## Target and routing

| Field | Value |
| --- | --- |
| Target repository | `trybotster/botster-hub` (`botster-hub`) |
| Target ID | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1787349524_364728` |
| Run | `run_1787349530_928420` |
| Pipeline | `botster_stack_delivery`, merge policy `direct` |
| Base ref | `origin/main` at `e950f4f0d5d1d7953eb5d9f378330ea044b0be1c` |
| Tag commit (decided) | `12e0cc6994be18024e4bdfffb22947526a652204` |
| Consumer proof Hub rev (decided) | `12e0cc6994be18024e4bdfffb22947526a652204` |
| Repository ownership charter | [[botster-hub-playbook]] |
| Human answer | `question_1787349702_525447` — option A, local tag only, no push without new authorization |
| Plan Review return | `review_1787350835_469845` — findings `finding_1787350835_436188` and `finding_1787350835_992535`, both fixed in this revision |

Routing came from spawn-target state. `tgt_7e208a0c76a44980a83b63af976b1f22` maps to
admitted name `botster-hub`, repository `trybotster/botster-hub`. The process working
directory did not decide the routing.

This is **not** runtime-teardown class. `teardown_class_applies`: no. This ticket
publishes a Git tag and proves Cargo source identity. It does not change WebRTC or
peer lifecycle, SessionIo or ClientWorker teardown, multi-peer ownership, resource
spin, or terminal-state versus live-runtime divergence. Do **not** load
[[botster runtime teardown lenses]].

[[project-pipelines-playbook]] is **not** loaded. No Project Pipelines package or
plugin path is in scope.

## Repository playbook loaded

- [[botster-hub-playbook]] — authoritative ownership charter for this target.

## Other role/surface playbooks and atomic notes loaded

Role entrypoints, in required order:

1. [[planner-playbook]]
2. [[botster-planner-playbook]]
3. [[botster-hub-playbook]]
4. Targeted atomic notes below
5. [[project-pipelines-playbook]] — not loaded

Targeted atomic notes:

- [[first-party Rust consumers pin the UI contract Git tag not a Hub rev]] — the live
  consumer identity rule. Its text still names `botster-ui-contract-v0.3.2`. See vault gaps.
- [[git-visible Hub member manifests must use the UI contract tag]] — Git-consumed member
  manifests declare the tag, and the root `[patch]` stays local-development only. Its text
  also still names `botster-ui-contract-v0.3.2`.
- [[botster rust consumers that share ui contract must pin one hub revision]] — superseded
  for identity, still the failure evidence for split contract sources.
- [[closed dependency tickets signal merged source not a consumable release]] — merged Hub
  source is not a consumable artifact for Rust consumers until the tag exists.
- [[botster hub client crate is the external client boundary]] — external Rust consumers
  reach Hub through the split client crate.
- [[botster package surface semantics live in ui contract while hub owns admission]]
- [[botster hub is a first party host profile over core]]
- [[kit UI contract pin proof uses an already split TUI consumer]] — downstream proof shape
  that detects split Cargo identities.
- [[Hub suite runs prebuild the session worker before the locked test wrapper]] — the locked
  worker build is a required precondition of `./test.sh --locked` on a fresh target.
- [[Hub bee15e7 builds the session worker from botster-core-daemon]] — the current package
  target that emits `botster-session-worker`.
- [[colon worktree paths break cargo dyld library paths]] — worktree hygiene.
- [[hearth gate runs require restoring a pipeline wiped gitignore before attribution]] — worktree hygiene.
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[plan review must verify a plan artifact exists before trusting gate summaries]]
- [[vault example paths are not repository placement conventions]] — `docs/plans/` and
  `docs/reports/` come from this repository's own prior art, not from a vault example.

Seam guidance, not a second ownership charter:

- [[botster-hub-client-playbook]] — `botster-hub-client` is the Cargo identity seam that
  external consumers resolve. This run changes no DTO and no protocol.

## Context loaded

Verified repository facts, all read in this Plan step:

1. `git ls-remote --tags origin` lists only `botster-ui-contract-v0.3.2`
   (tag object `615f5710`, commit `0775e661`). No `botster-ui-contract-v0.3.3` ref exists,
   so this run creates a new tag and moves nothing.
2. `crates/botster-ui-contract/Cargo.toml` declares `version = "0.3.3"`.
3. `crates/botster-hub-client/Cargo.toml:12` and `crates/botster-hub-test-support/Cargo.toml:22`
   already declare
   `botster-ui-contract = { git = "https://github.com/trybotster/botster-hub.git", tag = "botster-ui-contract-v0.3.3" }`.
4. Root `Cargo.toml` keeps `[patch."https://github.com/trybotster/botster-hub.git"]` to the
   path crate for local development only.
5. Version `0.3.3` reached `main` through implement commit `0188016`, merged by merge commit
   `12e0cc6`. `git diff 12e0cc6..origin/main -- crates/botster-ui-contract` is empty, so the
   contract tree is byte-identical at `12e0cc6` and at tip `e950f4f`.
6. `crates/botster-hub-client` changed between `12e0cc6` and `e950f4f`
   (`src/lib.rs`, `src/typescript.rs`, generated `daemon-protocol.ts`).
7. `script/tag-ui-contract` creates an annotated tag on **HEAD only**. Its `--verify` mode
   works for any tag, because it compares the crate version inside the tagged tree with the
   working crate version.
8. `README.md`, `crates/botster-ui-contract/README.md`, and `docs/client-protocol.md` already
   document tag `botster-ui-contract-v0.3.3`. No documentation rewrite is needed for the tag name.
9. `docs/reports/publish-package-owned-client-notice-reactions-implement.md` records the parent
   run's open follow-up: the maintainer must tag merged `main` for `botster-ui-contract-v0.3.3`.
10. `botster-tui` `crates/botster-tui/Cargo.toml` pins `botster-hub-client` at Hub rev
    `b3b54f1f` (the first parent of `12e0cc6`) and still pins UI contract tag
    `botster-ui-contract-v0.3.2`. `botster-tui-kit` also still pins `botster-ui-contract-v0.3.2`.
11. Worktree hygiene: tracked `.gitignore` is present and non-empty (53 bytes); the worktree
    path contains no `:`, so no `CARGO_TARGET_DIR` override is required.
12. `git ls-remote --tags origin botster-ui-contract-v0.3.2` returns the tag-object line only.
    `git ls-remote --tags origin 'botster-ui-contract-v0.3.2*'` returns the tag-object line and
    the `^{}` dereference line. Verified in this Plan step against the published tag.
13. Plan Review return `review_1787350835_469845` raised two product findings:
    `finding_1787350835_436188` (the suite gate omitted the locked binary prebuilds) and
    `finding_1787350835_992535` (the exact `ls-remote` pattern cannot produce the dereference
    line). This revision fixes both.

## Scope

1. Create annotated tag `botster-ui-contract-v0.3.3` on commit `12e0cc6994be18024e4bdfffb22947526a652204`
   with message `botster-ui-contract 0.3.3`, using
   `git tag -a botster-ui-contract-v0.3.3 12e0cc6 -m "botster-ui-contract 0.3.3"`.
   The create mode of `script/tag-ui-contract` cannot be used, because it tags HEAD.
2. Verify the tag locally with `script/tag-ui-contract --verify`, which must report crate
   version `0.3.3` for that tag.
3. Record the pre-push baseline: an external Cargo consumer that requests
   `tag = "botster-ui-contract-v0.3.3"` from the GitHub URL fails to resolve while the remote
   ref is absent. This is the negative half of the consumer proof.
4. Ask the human for explicit push authorization for the permanent remote tag, per answer
   `question_1787349702_525447`.
5. After that authorization only: push the tag with
   `git push origin botster-ui-contract-v0.3.3`.
6. After the push only: verify the remote ref with
   `git ls-remote --tags origin 'botster-ui-contract-v0.3.3*'` and confirm the dereferenced
   commit is `12e0cc6`. The wildcard pattern is required. An exact pattern returns the tag
   object line only, and never the `^{}` dereference line.
7. After the push only: run the external `botster-tui`-shaped consumer proof described under
   acceptance checks, outside the Hub workspace and without any `[patch]` section.
8. Write `docs/plans/publish-the-botster-ui-contract-v0.3.3-git-tag.md` (this plan) and
   `docs/reports/publish-the-botster-ui-contract-v0.3.3-git-tag-implement.md` with the exact
   tag commit, the `ls-remote` output, the consumer lockfile source line, and the
   `cargo tree -i botster-ui-contract` output.
9. Merge the documentation commits directly to `main`. Do not open a pull request.

## Non-scope

- Do not move, delete, or replace tag `botster-ui-contract-v0.3.2`.
- Do not move or replace `botster-ui-contract-v0.3.3` if the remote ref appears with a
  different commit. Stop and ask a human instead.
- Do not bump the `botster-ui-contract` crate version.
- Do not change crate source, schema, fixtures, generated npm assets, protocol version, or
  conformance revision.
- Do not publish `botster-ui-contract` or any Hub crate to crates.io.
- Do not publish npm `@trybotster/ui-contract@0.3.3`. That coordinate is still unpublished and
  belongs to the npm release path `script/publish-npm-packages`, not to this ticket.
- Do not change `crates/botster-hub-client/Cargo.toml`, `crates/botster-hub-test-support/Cargo.toml`,
  or the root `[patch]` entry. They already carry the correct tag.
- Do not edit `botster-tui`, `botster-tui-kit`, `botster-web`, or `botster-core` manifests.
  Their adoption of `v0.3.3` belongs to their own routed tickets.
- Do not push the tag before the human grants explicit push authorization.

## Repository ownership boundaries and cross-repo dependencies

| Repository | Owns | This run |
| --- | --- | --- |
| `botster-hub` (`tgt_7e208a0c76a44980a83b63af976b1f22`) | UI contract crate, its Git tag, the tag script, and Git-visible member manifests | **This run** |
| `botster-hub-client` (crate inside Hub) | External client DTO boundary | Consumed unchanged at rev `12e0cc6` for proof only |
| `botster-tui-kit` (`tgt_3dfae49c02454037bf13554f552baf7f`) | Kit mechanics and its durable contract pin | Downstream follow-up. Still pins `v0.3.2` |
| `botster-tui` (`tgt_c3d470bab78549df920a41e8fb0e58d8`) | App policy and durable pins | Disposable proof shape only. Durable adoption is its own ticket |
| `botster-core` (`tgt_1f7bce66eb304881980f9b4a2a5ae3fe`) | Terminal protocol crates | Unchanged. `botster-hub-client` keeps Core rev `7eafa470` |
| `botster-web` (`tgt_40abcf71ccf049f4ac0c99953a799869`) | npm contract consumer | Unchanged. This ticket does not publish npm |

Cross-repository prerequisites: none. Nothing outside `botster-hub` must land before this run.
No blocking dependency ticket is required, and none is registered.

Downstream follow-ups, which are **not** merge gates for this ticket:

- `botster-tui-kit` moves its `botster-ui-contract` tag pin from `v0.3.2` to `v0.3.3`.
- `botster-tui` moves `botster-hub-client`, `botster-hub-test-support`, the direct contract
  tag, and the kit rev as one set, per
  [[botster rust consumers that share ui contract must pin one hub revision]].

## Assumptions and unknowns

Assumptions, all traceable to evidence or to answer `question_1787349702_525447`:

1. The annotated tag names merge commit `12e0cc6`, the point where `main` first contained the
   reviewed 0.3.3 contract. Human answer A decided this.
2. The consumer proof pins `botster-hub-client` at Hub rev `12e0cc6`, so the proof measures the
   exact release seam and excludes later unrelated client changes. Human answer A decided this.
3. The Implement session prepares and verifies the tag locally and does not push it. Jason
   authorized direct source merges, but this session holds no authorization for a permanent
   release tag push.
4. An annotated tag with message `botster-ui-contract 0.3.3` matches the `v0.3.2` precedent and
   the message that `script/tag-ui-contract` would produce.
5. Direct merge to `main` remains the merge policy for the documentation commits.

Unknowns that stop Implement:

1. **The remote tag appears with a different commit.** If
   `git ls-remote --tags origin 'botster-ui-contract-v0.3.3*'` returns any commit other than
   `12e0cc6`, stop, do not force, and ask a human.
2. **Push authorization is refused or absent.** Then Implement stops after the verified local
   tag and the recorded pre-push baseline, and reports the remaining acceptance checks as
   blocked, not as passed.
3. **The consumer graph resolves more than one `botster-ui-contract` source.** That is a
   packaging defect of this ticket, not a downstream problem.

## Affected surfaces / files

| Path | Change |
| --- | --- |
| Git ref `refs/tags/botster-ui-contract-v0.3.3` | New annotated tag on `12e0cc6`, local first, remote after authorization |
| `docs/plans/publish-the-botster-ui-contract-v0.3.3-git-tag.md` | This plan |
| `docs/reports/publish-the-botster-ui-contract-v0.3.3-git-tag-implement.md` | Implement report with tag commit and consumer proof |

No source file, manifest, lockfile, script, or generated asset changes. The manifests and the
documentation already declare tag `botster-ui-contract-v0.3.3`.

## Risks

- **Permanent identity.** A Git tag that external Cargo consumers resolve must never move.
  Mitigation: verify the absent remote ref immediately before the push, and stop on any
  mismatch.
- **Unauthorized publication.** A push makes the tag public and cacheable by consumers.
  Mitigation: Implement stops before the push and asks for explicit authorization.
- **Proof cannot run before the push.** Cargo resolves a `tag` dependency from the GitHub
  remote, so the positive consumer proof is impossible while the ref is absent. Mitigation:
  record the pre-push failure as the baseline, then run the full proof after the push. Do not
  substitute a `file://` mirror or a consumer `[patch]` section, because either one changes the
  Cargo source identity that this ticket must prove.
- **Split contract identity in the proof graph.** `botster-tui-kit` still pins `v0.3.2`, so a
  proof graph that includes the published kit rev would resolve two contract sources.
  Mitigation: the disposable consumer pins `botster-hub-client` plus the direct contract tag
  and excludes the kit, so `cargo tree -i botster-ui-contract` measures this ticket's identity
  only.
- **Hub workspace gates prove nothing about the tag.** The root `[patch]` path-resolves the
  crate locally. Mitigation: identity acceptance uses the external consumer, never the Hub
  workspace lockfile.
- **Stale vault text.** Two live convention notes still name `v0.3.2` and could send a later
  agent to the wrong tag. Mitigation: the vault gaps below.

## Acceptance checks / tests

Worktree hygiene, before any gate:

- `git status --porcelain` is empty before tagging.
- Tracked `.gitignore` is non-empty. Restore with `git checkout HEAD -- .gitignore` if it is
  empty or missing.
- The worktree path contains no `:`, so no `CARGO_TARGET_DIR` override is needed. Set a
  colon-free `CARGO_TARGET_DIR` if the Implement worktree path does contain `:`.

Tag identity, before the push:

- `git rev-parse botster-ui-contract-v0.3.3^{commit}` prints `12e0cc6994be18024e4bdfffb22947526a652204`.
- `git cat-file -t botster-ui-contract-v0.3.3` prints `tag`, which proves an annotated tag.
- `script/tag-ui-contract --verify` reports crate version `0.3.3` for that tag.
- `git ls-remote --tags origin 'botster-ui-contract-v0.3.3*'` prints nothing, which proves that
  the push creates a new ref and moves no existing ref.

Pre-push baseline, recorded as evidence:

- A temporary crate outside the Hub workspace, with
  `botster-ui-contract = { git = "https://github.com/trybotster/botster-hub.git", tag = "botster-ui-contract-v0.3.3" }`,
  fails `cargo generate-lockfile` while the remote ref is absent. Record the exact Cargo error.

Push authorization:

- A durable `project_pipelines_ask_human` question requests push authorization and receives an
  explicit answer before the push.

Remote publication, after authorization:

- `git push origin botster-ui-contract-v0.3.3` succeeds.
- `git ls-remote --tags origin 'botster-ui-contract-v0.3.3*'` returns exactly two lines: the
  tag-object line `refs/tags/botster-ui-contract-v0.3.3`, and the dereference line
  `refs/tags/botster-ui-contract-v0.3.3^{}` whose commit is
  `12e0cc6994be18024e4bdfffb22947526a652204`. The wildcard pattern is required, because an
  exact pattern returns the tag-object line only. Verified against the published
  `botster-ui-contract-v0.3.2` tag in this Plan step.
- `script/tag-ui-contract --verify` passes from a clean fetch.

External consumer proof, after publication, outside the Hub workspace and with no `[patch]`:

- The temporary crate declares exactly these two dependencies:
  - `botster-hub-client = { git = "https://github.com/trybotster/botster-hub.git", rev = "12e0cc6994be18024e4bdfffb22947526a652204" }`
  - `botster-ui-contract = { git = "https://github.com/trybotster/botster-hub.git", tag = "botster-ui-contract-v0.3.3" }`
- Its source compiles a function that passes a `botster-ui-contract` type across the
  `botster-hub-client` boundary, so a split identity would produce `E0308` instead of an
  unused dependency.
- `cargo build` succeeds.
- `cargo tree -i botster-ui-contract` resolves exactly one package.
- `Cargo.lock` contains exactly one `botster-ui-contract` entry, and its source is
  `git+https://github.com/trybotster/botster-hub.git?tag=botster-ui-contract-v0.3.3#12e0cc69...`.
- No `botster-ui-contract` entry resolves from crates.io, from a `rev`, or from a path.

Hub workspace regression check:

- `cargo fmt --all -- --check`
- `cargo build --locked -p botster-core-daemon --bin botster-session-worker`
- `cargo build --locked -p botster-hub --bin botster-hub`
- `./test.sh --locked` for the workspace wrapper, which must stay green after the tag exists.

Run the two locked builds **before** the suite wrapper, and record that order in the
acceptance evidence. The prebuild is a required suite precondition, not a build
optimization: lazy worker discovery in `tests/support/mod.rs::ensure_session_worker_binary`
leaves a fresh target without the worker binary, and Plan Review reproduced eight
missing-worker failures on this exact plan. See
[[Hub suite runs prebuild the session worker before the locked test wrapper]] and
[[Hub bee15e7 builds the session worker from botster-core-daemon]] for the package target.

The Hub workspace resolves the contract through the root `[patch]`, so this suite guards
against regression only. It is not identity evidence.

Documentation:

- The Implement report records the tag commit, the `ls-remote` output, the consumer lockfile
  source line, and the `cargo tree -i botster-ui-contract` output as durable evidence.

Downstream adoption is not a merge gate for this ticket. `botster-tui-kit` and `botster-tui`
adopt `v0.3.3` on their own routed tickets.

## Vault gaps

Capture candidates after Implement:

- Update [[first-party Rust consumers pin the UI contract Git tag not a Hub rev]]. Its text
  names `botster-ui-contract-v0.3.2` as the required pin, while Hub manifests already require
  `v0.3.3`. The note should name the current tag and state how the tag advances.
- Update [[git-visible Hub member manifests must use the UI contract tag]] for the same reason.
  It should also record that `script/tag-ui-contract` creates a tag on HEAD only, so a tag on an
  earlier merged commit needs `git tag -a <name> <sha>`.
- New candidate: a UI contract tag is unusable by external Cargo consumers until it is pushed,
  so the positive identity proof cannot precede publication. A merged manifest that names a tag
  is not a consumable artifact. This is the Rust counterpart of
  [[closed dependency tickets signal merged source not a consumable release]].
- New candidate: a permanent release tag push needs its own human authorization, even when
  direct source merges are already authorized.
