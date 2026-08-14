# Implement report: Hub Git tag botster-ui-contract-v0.3.2

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`)
- Target ID: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1786661468_861481`
- Run: `run_1786662525_387029`
- Merge: direct to `origin/main` at `0775e661e23790b4d68183851493c9f08df33803`
- Tag: annotated `botster-ui-contract-v0.3.2` on that commit
- No PR, per pipeline `merge_policy: direct` and ticket/human instruction

## Repository playbook and other playbooks/notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster-hub-client-playbook]] (metadata seam only)
- [[botster-architecture]]
- [[cli-patterns]]
- Targeted notes: [[botster rust consumers that share ui contract must pin one hub revision]], [[public protocol versions host control and Core terminal planes independently]], [[botster package surface semantics live in ui contract while hub owns admission]], [[botster hub is a first party host profile over core]], [[botster hub client crate is the external client boundary]], [[TUI Kit pairing metadata does not authorize Hub test support dependencies]], [[kit UI contract pin proof uses an already split TUI consumer]], [[scratch cargo patch redirects measure downstream dto breakage]], [[Hub test support capability cutovers use a new unpublished package version]], [[hub test support npm releases need external consumer smoke]], [[closed dependency tickets signal merged source not a consumable release]], [[blocking dependency premises must be revalidated per consuming crate]], [[always look up latest dependency versions never use training cutoff]], [[test script required for rust tests not cargo test]], [[a root package workspace silently scopes cargo test to one package]], [[rust repo strict lints must be verified before dismissing warnings]], [[colon worktree paths break cargo dyld library paths]], [[hearth gate runs require restoring a pipeline wiped gitignore before attribution]], [[implement gate must verify committed work and pr link before review]], [[implementation artifacts must match actual git state]], [[implementation steps must persist report artifacts for review]], [[implementation deviations must resync committed plan acceptance checks]], [[pipeline vault checklists must cite exact resolvable note titles]], [[pipeline artifacts should use path neutral worktree references]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[prefer framework and library components over custom solutions]]
- [[project-pipelines-playbook]] not loaded as a product overlay
- [[botster runtime teardown lenses]] not loaded; teardown class does not apply

## Files changed

Relative to planned base `f9f0d8df`:

| Path | Change |
| --- | --- |
| `Cargo.toml` | Workspace-only git `[patch]` to the path crate |
| `README.md` | Tag identity and `script/tag-ui-contract` |
| `crates/botster-hub-client/Cargo.toml` | Tag pin, no path/rev/crates.io |
| `crates/botster-hub-test-support/Cargo.toml` | Tag pin |
| `crates/botster-ui-contract/Cargo.toml` | `readme = "README.md"` |
| `crates/botster-ui-contract/README.md` | Tag consumer identity |
| `docs/client-protocol.md` | Tag pin; no Hub SHA / crates.io identity |
| `docs/plans/publish-crates-io-botster-ui-contract-0.3.2.md` | Resynchronized to the Implement-stage human revision |
| `docs/reports/publish-crates-io-botster-ui-contract-0.3.2.md` | This report |
| `script/tag-ui-contract` | Create/verify annotated tag; no crates.io publish |

`packages/ui-contract/README.md` and `script/publish-npm-packages` are unchanged. `Cargo.lock` is unchanged.

## Ownership boundaries preserved

Hub still owns the UI contract crate, npm package, and git-visible hub-client / hub-test-support metadata. No durable edits to TUI Kit, TUI, Web, or Core. Daemon DTOs and protocol version were not changed.

## Cross-repo dependencies or separately routed work

- TUI Kit ticket `ticket_1786661009_576857` (`tgt_3dfae49c02454037bf13554f552baf7f`) remains downstream via `dependency_1786661471_370439`. It must pin the tag; this run did not edit kit `main`.
- Durable TUI pin remains `ticket_1786661009_551067`.
- Human answer required TUI/kit manifests to use the tag. That work stays on those repository targets.

## Deviations from plan

Implement-stage human answer `question_1786664733_777672` superseded Plan-time choose A:

- Do not publish crates.io. Do not request or use a crates.io token.
- Consumer identity is Hub Git tag `botster-ui-contract-v0.3.2`.
- Reverted unmerged crates.io publication commit `fdfc80a`.
- npm `@trybotster/ui-contract@0.3.2` left unchanged.

The committed plan was resynchronized to that answer.

## Tests and downstream proof run

- `cargo fmt --all -- --check` passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` failed on untouched `src/daemon_attach_stream.rs` (`clippy::derivable_impls`). The same command failed the same way on base `f9f0d8df`.
- `BOTSTER_ENV=test cargo test --locked -p botster-ui-contract` passed (4 generated + 85 contract).
- `BOTSTER_ENV=test cargo test --locked -p botster-hub-client -p botster-hub-test-support -p botster-hub-installation -p botster-hub-installer` passed.
- `BOTSTER_ENV=test cargo test --locked --test hub_mcp_test --test hub_lua_runtime_test -- --test-threads=1` passed (7 + 32).
- Isolated pre-existing failures, same command on branch and base `f9f0d8df`:
  - `local_webrtc::tests::local_webrtc_stale_peer_attach_snapshot_does_not_detach_replacement_owner` — both exit 101, `live_attach_before >= 1`.
  - `hub_capability_runtime_test` three hot-path tests — both exit 101, timed out waiting for `"ready"`.
- `./test.sh --locked --no-fail-fast` hung 20+ minutes in `hub_daemon_lifecycle_test` on `botster-hub smoke` at 0% CPU. Killed. Unrelated to metadata.
- External consumer outside the Hub workspace compiled `UiNode` / `validate_ui_node` from `tag = "botster-ui-contract-v0.3.2"`. Lock source: `git+https://github.com/trybotster/botster-hub.git?tag=botster-ui-contract-v0.3.2#0775e661...`.
- Disposable TUI graph: git hub-client `0775e66`, tag UI contract, path-pinned scratch kit with the same tag. `cargo tree -i botster-ui-contract` showed one package from the tag. `cargo check -p botster-tui` passed. No `rev` or crates.io identity for the contract crate.
- `npm view @trybotster/ui-contract@0.3.2 version` remains `0.3.2`. crates.io crate still 404.
- `script/tag-ui-contract --verify` confirmed the annotated tag on `0775e66` with crate version `0.3.2`.

## Unverified behavior or residual risk

- Full `./test.sh --locked` did not complete because of the hung daemon-lifecycle smoke and the pre-existing lib/capability failures above.
- Durable TUI and TUI Kit `main` still pin Hub `rev` `f9f0d8df`. Downstream tickets must move those manifests to the tag.
- Hub workspace `cargo tree` still shows the path patch. That is local-dev only.

## Missing vault guidance discovered

- No existing note said git-visible member manifests must be tag-only with a workspace-only git patch.
- [[botster rust consumers that share ui contract must pin one hub revision]] is now stale for UI-contract identity.

Captured to vault inbox:

- `first-party-rust-consumers-pin-the-ui-contract-git-tag-not-a-hub-rev.md`
- `git-visible-hub-member-manifests-must-use-the-ui-contract-tag.md`
