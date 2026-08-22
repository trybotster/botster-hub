# Implement report: publish the botster-ui-contract-v0.3.3 Git tag

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | `botster-hub` |
| Pipeline worktree | the pipeline-provided ticket worktree |
| Ticket | `ticket_1787349524_364728` |
| Run | `run_1787349530_928420` |
| Step | `botster_stack_implement` (`run_step_1787351189_903908`) |
| Approved plan | `docs/plans/publish-the-botster-ui-contract-v0.3.3-git-tag.md` revision 2 (`8c6a94d`) |
| Plan Review | `review_1787351170_814630` approved |
| Merge policy | `direct`; do not create a PR |
| Base | `origin/main` `e950f4f0d5d1d7953eb5d9f378330ea044b0be1c` |
| Tag commit | `12e0cc6994be18024e4bdfffb22947526a652204` |
| Tag object | `df2e16e917d8a3fc9ac0516e4d8c74243a905fbc` |
| `teardown_class_applies` | no |

Independent routing: `project_pipelines_current_context` and `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to admitted name `botster-hub`, repository `trybotster/botster-hub`. The approved plan uses the same routing. Work stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

Playbooks:

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster-hub-client-playbook]] — Cargo identity seam only; no DTO or protocol edit
- [[botster-architecture]]
- [[cli-patterns]]

[[project-pipelines-playbook]] was not loaded. No Project Pipelines package or plugin path was in scope.

[[botster runtime teardown lenses]] was not loaded. This ticket publishes a Git tag and proves Cargo source identity.

Targeted notes:

- [[first-party Rust consumers pin the UI contract Git tag not a Hub rev]]
- [[git-visible Hub member manifests must use the UI contract tag]]
- [[botster rust consumers that share ui contract must pin one hub revision]]
- [[closed dependency tickets signal merged source not a consumable release]]
- [[botster hub client crate is the external client boundary]]
- [[botster package surface semantics live in ui contract while hub owns admission]]
- [[botster hub is a first party host profile over core]]
- [[kit UI contract pin proof uses an already split TUI consumer]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[Hub bee15e7 builds the session worker from botster-core-daemon]]
- [[colon worktree paths break cargo dyld library paths]]
- [[hearth gate runs require restoring a pipeline wiped gitignore before attribution]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[implementation artifacts must match actual git state]]
- [[test script required for rust tests not cargo test]]
- [[project pipelines checklist worker timeouts require artifact evidence fallback]]
- [[prefer framework and library components over custom solutions]]

Vault checklist: `checklist_1787351486_828898` (create timed out in the plugin worker; listing recovered the committed id).

## Files changed

Relative to planned base `origin/main` `e950f4f`:

| Path | Change |
| --- | --- |
| Git ref `refs/tags/botster-ui-contract-v0.3.3` | New annotated tag on `12e0cc6`. Created locally, then pushed after answer A on `question_1787357787_608652`. |
| `docs/plans/publish-the-botster-ui-contract-v0.3.3-git-tag.md` | Plan (Plan-step commits `0ed4be3`, `8c6a94d`) |
| `docs/reports/publish-the-botster-ui-contract-v0.3.3-git-tag-implement.md` | This report |

No crate source, manifest, lockfile, script, schema, fixture, or generated asset changes. Member manifests already declared `tag = "botster-ui-contract-v0.3.3"`.

## Ownership boundaries preserved

Hub still owns the UI contract crate, its Git tag, `script/tag-ui-contract`, and Git-visible member manifests. This run did not edit `botster-hub-client` DTOs, Hub daemon protocol, Core, TUI, TUI Kit, or Web. The disposable consumer lived outside the Hub workspace and was discarded after proof.

## Cross-repo dependencies or separately routed work

None blocking. Downstream follow-ups remain their own tickets:

- `botster-tui-kit` (`tgt_3dfae49c02454037bf13554f552baf7f`) still pins `botster-ui-contract-v0.3.2`.
- `botster-tui` (`tgt_c3d470bab78549df920a41e8fb0e58d8`) still pins the older Hub client rev and `v0.3.2`. Durable adoption is not a merge gate here.

Human npm publication of `@trybotster/ui-contract@0.3.3` is a separate release path. This ticket did not run `script/publish-npm-packages` and does not treat npm as Rust consumer identity.

## Deviations from plan

None for tag identity, tag commit, consumer pin, or Hub gate order.

Process note: `question_1787351567_371149` was answered "Ok it's published now". Independent proof showed npm `@trybotster/ui-contract@0.3.3` published and the Git tag still absent. Follow-up `question_1787357787_608652` chose A. This session then pushed the existing local annotated tag. The plan already required explicit push authorization before `git push origin botster-ui-contract-v0.3.3`.

`script/tag-ui-contract` create mode was not used. That script tags HEAD. HEAD was the Plan revision, not `12e0cc6`.

## Tests and downstream proof run

Worktree hygiene before tagging: `git status --porcelain` empty; tracked `.gitignore` 53 bytes; worktree path contains no `:`.

Tag identity before the push:

- `git rev-parse botster-ui-contract-v0.3.3^{commit}` → `12e0cc6994be18024e4bdfffb22947526a652204`
- `git cat-file -t botster-ui-contract-v0.3.3` → `tag`
- `script/tag-ui-contract --verify` → crate version `0.3.3` on that commit
- `git ls-remote --tags origin 'botster-ui-contract-v0.3.3*'` → empty

Pre-push baseline, temporary crate outside the Hub workspace, no `[patch]`:

```text
botster-ui-contract = { git = "https://github.com/trybotster/botster-hub.git", tag = "botster-ui-contract-v0.3.3" }
```

`cargo generate-lockfile` exited 101:

```text
Unable to update https://github.com/trybotster/botster-hub.git?tag=botster-ui-contract-v0.3.3
failed to find tag `botster-ui-contract-v0.3.3`
```

Push authorization: `question_1787357787_608652` answer A.

Remote publication:

```text
git push origin botster-ui-contract-v0.3.3
 * [new tag]         botster-ui-contract-v0.3.3 -> botster-ui-contract-v0.3.3
```

`git ls-remote --tags origin 'botster-ui-contract-v0.3.3*'` after the push:

```text
df2e16e917d8a3fc9ac0516e4d8c74243a905fbc        refs/tags/botster-ui-contract-v0.3.3
12e0cc6994be18024e4bdfffb22947526a652204        refs/tags/botster-ui-contract-v0.3.3^{}
```

`script/tag-ui-contract --verify` after fetch still reports crate version `0.3.3` on `12e0cc6`.

External consumer proof, temporary crate outside the Hub workspace, no `[patch]`:

```toml
botster-hub-client = { git = "https://github.com/trybotster/botster-hub.git", rev = "12e0cc6994be18024e4bdfffb22947526a652204" }
botster-ui-contract = { git = "https://github.com/trybotster/botster-hub.git", tag = "botster-ui-contract-v0.3.3" }
```

The crate compiled a function that takes `botster_ui_contract::UiNode` and returns `botster_hub_client::DaemonPluginSurface`. Split Cargo identities would fail with `E0308`.

- `cargo build` succeeded.
- `cargo tree -i botster-ui-contract`:

```text
botster-ui-contract v0.3.3 (https://github.com/trybotster/botster-hub.git?tag=botster-ui-contract-v0.3.3#12e0cc69)
├── botster-hub-client v0.1.0 (https://github.com/trybotster/botster-hub.git?rev=12e0cc6994be18024e4bdfffb22947526a652204#12e0cc69)
│   └── ui-contract-tag-consumer v0.0.0
└── ui-contract-tag-consumer v0.0.0
```

- `Cargo.lock` contains exactly one `botster-ui-contract` package. Source:

```text
git+https://github.com/trybotster/botster-hub.git?tag=botster-ui-contract-v0.3.3#12e0cc6994be18024e4bdfffb22947526a652204
```

No `botster-ui-contract` entry resolves from crates.io, from a `rev`, or from a path.

Hub workspace regression, required order:

1. `cargo fmt --all -- --check` passed.
2. `cargo build --locked -p botster-core-daemon --bin botster-session-worker` passed.
3. `cargo build --locked -p botster-hub --bin botster-hub` passed.
4. `./test.sh --locked` passed (exit 0, 0 failed across workspace tests and doctests).

The Hub workspace still path-resolves the contract through the root `[patch]`. The suite is regression evidence only. Identity evidence is the external consumer.

## Unverified behavior or residual risk

- Durable TUI and TUI Kit pins still name `v0.3.2`. Those repos must adopt `v0.3.3` on their own tickets.
- npm `@trybotster/ui-contract@0.3.3` exists because the human published it on a separate path. This run did not verify npm tarball contents or Web consumption.
- crates.io still has no `botster-ui-contract` crate. That remains out of scope.
- A Git tag that Cargo has resolved is cacheable. This tag must not move.

## Missing vault guidance discovered

Inbox captures from this step:

- `a-ui-contract-git-tag-is-unusable-by-external-cargo-until-pushed.md`
- `a-permanent-ui-contract-tag-push-needs-its-own-human-authorization.md`
- `hub-ui-contract-tag-identity-notes-still-name-v0.3.2-after-v0.3.3.md`

Live notes [[first-party Rust consumers pin the UI contract Git tag not a Hub rev]] and [[git-visible Hub member manifests must use the UI contract tag]] still name `botster-ui-contract-v0.3.2`. They should name the current tag after vault processing. This Hub ticket did not edit vault `notes/`.
