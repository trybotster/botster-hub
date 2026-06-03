# Resolve PR 14 Merge Conflicts Plan

Ticket: `ticket_1780508731_136973`
Run: `run_1780510185_724219`
PR: `https://github.com/trybotster/botster-hub/pull/14`

## Context Loaded

- Project Pipelines context reloaded after GitHub review feedback returned the run to `botster_plan`.
- Doorbell message received from Project Pipelines:
  - PR #14 review state: `changes_requested`.
  - Review body: "Please fix merge conflicts".
  - Instruction: update the existing PR branch in this ticket worktree; do not create a new run or PR.
- Current branch context:
  - Branch: `project-pipelines/ticket_1780508731_136973`.
  - Local worktree was clean before this returned-Plan artifact was added.
  - PR head: `d1df626` (`Define durable hub state boundary`).
  - Current `origin/main`: `79ff14d` after package admission and plugin lifecycle work merged.
  - `gh pr view 14 --json ...` reports `mergeable: CONFLICTING`, `reviewDecision: CHANGES_REQUESTED`.
- Dry merge evidence:
  - `git merge-tree --name-only HEAD origin/main` reports content conflicts in:
    - `src/lib.rs`
    - `src/packages.rs`
    - `src/runtime.rs`
  - The same dry merge auto-merges:
    - `README.md`
    - `src/main.rs`
- Main-side changes that conflict or interact with this ticket:
  - `src/lib.rs` now exports `lifecycle`, `PackageAdmissionPolicy`, `PackageAdmissionReason`, `default_package_policy`, and plugin lifecycle facade decisions.
  - `src/packages.rs` now has hub package admission policy, richer audit reasons on registry decisions/errors, and default grants from `host_profile().default_capability_grants()`.
  - `src/runtime.rs` now has `HubPluginLifecycle` and plugin load/invoke/reload/unload methods.
  - `origin/main` also adds `src/lifecycle.rs` and `tests/hub_plugin_lifecycle_test.rs`.
- Required playbooks reloaded:
  - [[planner-playbook]]
  - [[botster-planner-playbook]]
- Botster/package notes loaded or applied:
  - [[botster-architecture]]
  - [[cli-patterns]]
  - [[spa-patterns]]
  - [[project pipeline orchestration belongs in a device-level botster plugin]]
  - [[botster package manifests and lockfiles should declare capabilities and provenance]]
  - [[botster cloud should be an installable privileged provider not a hub dependency]]
  - [[implement gate must verify committed work and pr link before review]]
  - [[pipeline artifacts should cite vault notes by wikilink not home path]]
- Project Pipelines checklist evidence updated on the existing run checklist.

## Scope And Non-Scope

In scope:

- Resolve PR #14 merge conflicts against current `origin/main` on the existing branch.
- Integrate by merging `origin/main` into the PR branch. Do not rebase or force-push
  the shared PR branch unless a human explicitly redirects.
- Preserve this ticket's durable hub state boundary:
  - `HubState` v1 aggregate.
  - `FileHubStateStore`.
  - storage trait/API and typed errors.
  - schema/version handling.
  - atomic write behavior.
  - runtime load/initialize path.
  - package registry snapshot import/export and re-admission behavior.
- Integrate main's package admission policy work instead of reverting it:
  - keep `PackageAdmissionPolicy`.
  - keep `PackageAdmissionReason`.
  - keep richer decision/error audit metadata.
  - keep `default_package_policy()` and host-profile default grants.
- Integrate main's plugin lifecycle work instead of reverting it:
  - keep `src/lifecycle.rs`.
  - keep `HubPluginLifecycle`.
  - keep `HubPluginRuntimeBundle`.
  - keep `HubRuntime` plugin load/invoke/reload/unload methods.
  - keep lifecycle tests.
- Update imports, re-exports, tests, README/ADR wording, and runtime constructors so durable state and lifecycle/admission code compile together.
- Commit the conflict-resolution result to the existing PR branch and push to update PR #14.

Non-scope:

- No new durable-state features beyond the already approved implementation.
- No new package-manager/operator mutation commands.
- No cloud sync, marketplace fetch, Rails, WebRTC, browser/TUI UI, ActionCable, or provider process implementation.
- No new PR and no replacement branch.
- No broad refactor of package/lifecycle/runtime APIs beyond what conflict resolution requires.
- No local path references in committed artifacts.

Botster layers touched:

- Rust hub crate and docs only.
- No core, plugin worker internals, SPA, TUI, Rails, cloud, or MCP surface change beyond preserving existing main-branch APIs.

## Assumptions And Unknowns

Assumptions:

- Conflict resolution should merge the two valid streams of work: durable hub state from PR #14 and package admission/plugin lifecycle from `origin/main`.
- `origin/main` package admission naming should win where it supersedes PR #14's older `PackagePolicyReason` vocabulary.
- Durable storage snapshots should adapt to the current `PackageAdmissionReason`/audit metadata shape instead of preserving duplicate old error vocabulary.
- `HubRuntime::new(config)` may remain an in-memory constructor, but production boot and `run-one` should continue to use the storage-backed load path introduced by PR #14.
- Lifecycle methods should exist on the same `HubRuntime` that also carries loaded durable `HubState`.

Unknowns for Implement to resolve:

- Whether `PackageRegistrySnapshotError` should keep its current PR #14 shape or gain audit/context fields to match main's richer package errors.
- Whether snapshot tests need minor fixture updates after `PackageDecision` and `PackageRegistryError` retain `audit_reason`.
- Whether `HubRuntime::load_from_store` initialization order should construct `HubPluginLifecycle` before or after loading durable state. The choice should be behavior-neutral; prefer the clearer struct literal.
- Whether README/ADR references to package policy need wording updates after integrating `PackageAdmissionPolicy`.

No human question is currently blocking. The GitHub request is unambiguous: fix merge conflicts on the existing PR branch.

## Affected Surfaces / Files

Conflict files:

- `src/lib.rs`
  - Merge durable persistence re-exports with main's `lifecycle` module and package admission re-exports.
  - Keep both durable state doctest expectations and main's default package policy doctest expectations if practical.
  - Keep facade audit entries for both plugin lifecycle and durable runtime/storage where present.
- `src/packages.rs`
  - Merge PR #14 serde/snapshot/from_snapshot/re-admission support with main's package admission policy, default package policy, audit reason propagation, and `PackageAdmissionReason` vocabulary.
  - Avoid resurrecting old `PackagePolicyReason` if main renamed it.
- `src/runtime.rs`
  - Merge durable `HubState`/`FileHubStateStore` load path with `HubPluginLifecycle` field and plugin lifecycle methods.
  - Keep `state()` accessor and storage-backed constructors.

Likely auto-merged but must inspect:

- `README.md`
- `src/main.rs`

Main-side additions to keep:

- `src/lifecycle.rs`
- `src/profile.rs` default capability grants (`host_profile().default_capability_grants()`)
  that feed `default_package_policy()`.
- `tests/hub_plugin_lifecycle_test.rs`
- main-side plan docs and ADR updates.

PR-side additions to keep:

- `docs/adr/durable-hub-state-v1.md`
- `docs/plans/durable-hub-state-model-storage-boundary.md`
- durable-state tests in `src/persistence.rs`, `src/packages.rs`, and `tests/hub_runtime_test.rs`.

## Risks

- Regression risk: resolving conflicts by choosing either side wholesale could drop durable-state production wiring or main's lifecycle/admission APIs.
- API drift risk: `PackagePolicyReason` vs `PackageAdmissionReason` vocabulary can leave stale imports, docs, or tests.
- Invariant risk: `admitted_host_profile` must still be re-derived on snapshot reload for enabled provider records.
- Runtime wiring risk: `HubRuntime` must initialize both `state` and `plugin_lifecycle` and keep the production boot path storage-backed.
- PR durability risk: fixing conflicts locally without committing and pushing leaves GitHub in the same `CONFLICTING` state.
- PII risk: this returned-Plan artifact and any conflict-resolution docs must use wikilinks/note names, not home paths.

## Acceptance Checks / Tests

Required after conflict resolution:

- `git status --short --branch` shows a clean branch after commit.
- `git diff origin/main...HEAD --stat` shows a real committed diff.
- PR #14 remains the linked PR and no new PR is created.
- `git merge-tree --write-tree HEAD origin/main` reports no conflict text before relying
  on GitHub's async mergeability recompute.
- `gh pr view 14 --json mergeable,reviewDecision,state,url` reports the PR is no longer
  `CONFLICTING` after push, or GitHub is still computing mergeability with deterministic
  local mergeability already proven.
- A raw conflict-marker scan returns no merge markers in committed artifacts.
- A raw PII scan over committed docs, README, source, and tests returns no local
  home paths or operator usernames except intentional negative test assertions
  and synthetic test paths.
- `cargo fmt`
- `./test.sh`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo run -- run-one --data-dir target/botster-hub-smoke-data -- /bin/sh -c "printf 'botster-hub-smoke-ok\n'"`
- `git diff --check`

Behavior to prove:

- Durable state file-backed load/initialize still crosses the production `src/main.rs`/`run-one` path.
- Package registry snapshot round-trip still rehydrates enabled provider admission.
- Package admission policy default grants from `host_profile()` still work.
- Plugin lifecycle load/invoke/reload/unload tests from main still pass.
- Existing hub runtime spawn/attach/write/drain/shutdown integration test still passes.

## Vault Gaps Worth Capturing

- No new durable vault note is required for merge conflict resolution itself.
- Carry forward the existing capture candidate from review: runtime invariants stored in `serde(skip)` fields must be re-derived on load rather than silently defaulted.
- This conflict is a useful example of why durable-state boundary work must compose with package admission and lifecycle work, but the existing package/provenance and cloud-provider notes already cover the architectural decision.
