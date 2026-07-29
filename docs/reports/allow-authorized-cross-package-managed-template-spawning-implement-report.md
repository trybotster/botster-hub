# Allow Authorized Cross-Package Managed Template Spawning — Implement Report

## Target

- Repository: `trybotster/botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1785298630_852133`
- Run: `run_1785298641_743448`

## Guidance applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[project-pipelines-playbook]]
- [[workspace session templates are hub owned capabilities callable from lua workers]]
- [[session template override sources use package device repo explicit precedence]]
- [[botster workspace records are plugin owned references not hub authority]]
- [[plugin capability tests must validate against real lua runtime table not injected stubs]]
- [[review must diff stale capability disclaimers when behavior changes]]
- [[test script required for rust tests not cargo test]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[implementation deviations must resync committed plan acceptance checks]]

No convention conflict was found. Hub remains the owner of package admission,
template resolution, managed-Git orchestration, Core dispatch, and sanitized
plugin errors. Core remains the owner of generic session-worker process and
control-socket mechanics.

## Finding and implementation

The required real-worker package-A to package-B cases already succeeded on Hub
`35e92f46a98c445765b6ba7755e029f5dde702f8` with locked Core
`e36435f2cb583c344d6f6ba2d62c39da324c7a64`:

- an enabled explicit-target package B template;
- shipped `project-pipelines/agent-step` for the Git target whose literal id is
  `package:project-pipelines`;
- canonical 36-character UUID return;
- package A capability denial;
- mismatched-target rejection.

The exact downstream Workspaces fixture nevertheless reproduced
`hub_spawn_rejected` / `configured session could not be spawned`. Retaining the
discarded raw Core diagnostic localized it to the worker-control-parent
permission check. The durable, path-neutral Hub class is
`runtime.spawn_failed`; it does not substring-match Core diagnostic text.

The Core worker rejected a pre-existing hashed control-socket parent with
non-private permissions. The failure was not caused by cross-package template
ownership, eligibility, source/command materialization, caller authorization,
or package snapshot timing.

The control run reused the same Hub state, target, package records, template,
and Workspaces invocation while starting the matched rebuilt Hub and worker
with a fresh private temporary socket root. It succeeded with canonical UUID
`4e7cb99c-345c-410a-9ff4-095f4046dc4a`.

The Hub change is therefore deliberately limited to:

- real Lua worker regression coverage for cross-package list/show/spawn,
  shipped Project Pipelines spawning, canonical UUID, capability denial, and
  target mismatch;
- path-neutral Hub-side Core failure classification with generated session-id
  correlation, while the public Lua result remains the existing sanitized
  `spawn_failed` error.

No Hub workaround repairs or bypasses Core worker-socket permission policy.

## Files changed

- `src/runtime.rs` — retain a sanitized Core spawn-failure class in Hub stderr
  diagnostics without exposing Core messages or paths.
- `tests/hub_lua_runtime_test.rs` — add real-worker package-A/package-B success
  and authorization/eligibility regression coverage.
- `docs/plans/allow-authorized-cross-package-managed-template-spawning.md` —
  record the approved evidence-driven scope correction.
- `docs/reports/allow-authorized-cross-package-managed-template-spawning-implement-report.md`
  — persist this handoff.

## Ownership boundaries

- Preserved Hub ownership of admission, resolution, managed Git, trusted
  context, rollback, and sanitized plugin errors.
- Preserved Core ownership of worker process and control-socket mechanics.
- Did not edit Workspaces, Project Pipelines, Core, client DTOs, package policy,
  UI, or pipeline schema.
- Did not add a package-name branch, caller spoofing field, compatibility path,
  or general template abstraction.

## Cross-repository dependencies

No cross-repository code was changed. A durable repair or cold migration for
pre-existing insecure Core worker-socket parents belongs in a separately
routed `botster-core` ticket against
`tgt_1f7bce66eb304881980f9b4a2a5ae3fe`.

The downstream Workspaces checkout was used read-only for matched-provenance
acceptance evidence. Its package code and tests remain owned by the Workspaces
ticket/run.

## Deviations from the approved plan

The plan expected a cross-package Hub defect in command/source materialization
or Core dispatch. The exact evidence disproved that premise. Blocking question
`question_1785300505_529418` authorized preserving regression coverage and
forbade an invented Hub behavior change. A later matched-provenance message
confirmed the failure, and diagnostic reproduction proved it was stale
Core-owned socket-directory state.

The committed plan now records this disposition. Registry projection,
materialization, authorization, and public error taxonomy changes were not
made.

The matched Workspaces registry enabled Project Pipelines before the
Workspaces caller worker loaded. Package A then successfully listed, showed,
and atomically spawned package B's template. This is the production ordering
for the reported path and takes the plan's passing selection-path branch;
late contributor enablement after caller-worker load remains separately
routable rather than widening this ticket.

## Verification

PR #175 was initially verified on `80db7b4`, then current `origin/main`
`7c6d9488481da3fc43c6fb813eeb583c507f802c` advanced the production
plugin-worker configuration path and made the PR conflict. Implement merged
main, retained both `MultiplexerEngineError` and
`PluginWorkerDebugSnapshot`, and reran every gate on the merged tree.

- `./test.sh real_lua_plugin_cross_package_managed_template_spawning -- --nocapture`
  — passed 1 test; proves list/show enumeration for both contributors, both
  positive package-B cases, canonical UUIDs, package-B command execution,
  capability denial, and target mismatch.
- `./test.sh real_lua_plugin_atomically_ensures_managed_worktree_and_spawns_session`
  — passed 1 test; preserves the prior same-package, reuse, relative-cwd, and
  rollback baseline.
- `./test.sh managed_session_template`
  — passed 1 test; preserves old-scope denial and trusted-field-smuggling
  rejection.
- `cargo check --workspace --locked` — passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` —
  passed.
- `cargo fmt --all -- --check` — passed.
- `git diff --check` — passed.
- `./test.sh` — observed both a complete pass on the current-main merged tree
  (133 library tests, 103 daemon-integration tests, one larger local
  adversarial test ignored, and all remaining targets/doctests) and
  intermittent failures solely in
  `cli_operator_console_starts_reuses_detaches_handles_ctrl_c_and_stops`.
  Review's interleaved branch/base control produced matched pairs
  fail/fail, pass/pass, fail/fail; across six runs per ref, the branch failed
  4/6 and unmodified current main `7c6d948` failed 2/6. Focused and strict
  gates are deterministic. This isolated startup race is attributable to
  current-main/machine state; any other failure remains blocking and requires
  its own branch/base isolation.
- `./test.sh cli_operator_console_starts_reuses_detaches_handles_ctrl_c_and_stops -- --nocapture`
  — passed on this branch in isolation; an earlier full-suite attempt timed out
  waiting for `daemon=started`, establishing intermittent timing rather than a
  standing waiver.
- The identical wrapper command on untouched `origin/main` `35e92f4` — failed
  once in isolation at the same `daemon=started` timeout. This is retained only
  as timing evidence. Verify must treat any future failure as in scope.
- Exact matched Workspaces command on the original temp-root state — reproduced
  `hub_spawn_rejected`; the retained raw Core diagnostic identified the
  worker-control-parent permission check and the committed Hub diagnostic
  records `runtime.spawn_failed`.
- Exact matched Workspaces command with a fresh private worker-socket temp root
  — passed and returned UUID
  `4e7cb99c-345c-410a-9ff4-095f4046dc4a`.
- Workspaces downstream confirmation — UUID persisted exactly once, survived
  plugin reload, missing-target failure preserved membership, the surface
  rendered the returned identity, workspace deletion preserved the Hub
  session/worktree, and the live packaged WebRTC protocol harness passed with
  the matched Hub/worker and fresh private temporary socket root.
- Current-main downstream rerun — because main rejects the former schema-v1
  held data directory, recreated the same Git target and shipped Project
  Pipelines template topology in a fresh schema-v2 Hub directory. The
  Workspaces acceptance smoke passed with canonical UUID
  `3c77d44f-da15-43f9-a097-f18c291695ec`, and the live packaged WebRTC harness
  passed using the exact merged Hub and locked worker binaries.

## Unverified behavior and residual risk

- Pre-existing insecure Core worker-socket directories remain unrepaired. A
  future session using the same hashed directory can still fail until the
  directory is removed/recreated privately or Core gains an owned migration.
- The full Hub suite has completed green and has also intermittently failed
  only at the operator-console startup timing test described above. This is a
  narrow attribution, not a blanket waiver: every other failure remains
  blocking. A separate Hub ticket is recommended to make daemon readiness
  deterministic in that test and ensure timed-out console runs reap their
  spawned daemon.
- A package enabled only after an already-loaded caller worker remains a
  potential independent snapshot issue. It did not cause this post-prepare
  failure and was not changed.

## Missing vault guidance

The vault had no atomic note for stale hashed Core worker control-socket parent
permissions surviving across runs and producing an apparently unrelated Hub
spawn failure. No vault file was written from this restricted run worktree.
The durable capture should be routed to the vault inbox after the Core owner
decides whether the intended remedy is cleanup, permission repair, or a cold
migration.
