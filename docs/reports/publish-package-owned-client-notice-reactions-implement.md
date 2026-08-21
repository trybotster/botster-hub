# Implement report: publish a package-owned client notice reaction descriptor

## Review-return findings

Review `review_1787294428_253011` returned two open findings. This visit addresses both of them.

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787294428_366815` Package schema admission allocates an unbounded minLength string | high | `accepts_some_string` no longer allocates after type, const, and enum checks. It compares `minLength` and `maxLength` only. Tests include `minLength: u64::MAX` and a contradictory `u64::MAX` / `u64::MAX - 1` pair. |
| `finding_1787294428_826162` Required default-concurrency workspace gate is not green | high | The start-boundary hook ignores threads with no start token, so untokened waiters no longer send `foreign` while they wait for the daemon mutex. Injected taint uses `ScopedHarnessTaint`, which clears the latch on drop while the caller still holds `daemon_test_guard`. `./test.sh --locked --offline --test hub_daemon_lifecycle_test` passed 265 tests at default concurrency. |

Review `review_1787291755_566319` returned two open findings. The previous Implement visit addressed both of them.

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787291755_539295` Declaration schema rejects optional owner | high | Removed `not: { required: ["owner"] }`. Root `oneOf` now has one nested `anyOf` of declaration and descriptor. Direct `$defs/PackageNoticeReactionDeclaration` tests cover omitted owner, exact owner, and malformed owner. |
| `finding_1787291755_872245` String admission is not satisfiability | medium | `CompiledEventSchema::accepts_some_string` is the shared query. It honors `const`, `enum`, `minLength`, and `maxLength`. Hub admission uses it for subject and notice. |

Review `review_1787290014_342254` returned three open findings. The previous Implement visit addressed all of them.

| Finding | Severity | Fix |
| --- | --- | --- |
| `finding_1787290014_414325` Canonical JSON Schema rejects projected descriptors | high | Pointer pattern is `^/([^/~]|~0|~1)+$`. Schema tests accept one owner-bearing descriptor and reject `/`, `/notice~`, `/notice~2`, `/a/b`, and `notice`. The later visit replaced exclusive `oneOf` plus `not required owner` with nested `anyOf` so an authored owner remains valid on the declaration definition. |
| `finding_1787290014_835267` Live notice test fails under default-concurrency | high | The batch emit queued two events against a 1 s mailbox age. This visit emits empty, waits both subscribers, then emits oversized. IsolatedHub root uses a pid suffix. `./test.sh --locked --offline` at default concurrency passed, including this test. |
| `finding_1787290015_322853` Notice admission ignores compiled string semantics | medium | `schema_accepts_string` now compiles through `CompiledEventSchema`. Type arrays fail compilation. Session `subject` must accept a string. Tests cover an empty property schema, a type array, and a numeric subject. |

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative spawn target | `botster-hub` |
| Pipeline worktree | the pipeline-provided ticket worktree |
| Ticket | `ticket_1787278643_145174` |
| Run | `run_1787282470_625000` |
| Step | `botster_stack_implement` (`run_step_1787294442_455048`) |
| Approved plan | `docs/plans/publish-package-owned-client-notice-reactions.md` revision 3 |
| Plan Review | `review_1787284569_918928` approved |
| Merge policy | `direct`; do not create a PR |
| Base | `origin/main` `b3b54f1` |
| Locked Core SHA | `7eafa470a18025895995bbedc20d34b58106a03b` |
| Session-type eligibility consumer | false |
| `teardown_class_applies` | no |

Routing verified independently: ticket/run `target_id` is `tgt_7e208a0c76a44980a83b63af976b1f22` → `trybotster/botster-hub`. The approved plan uses the same routing. Implementation stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]] — ownership charter
- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]] — loaded because the implementer overlay requires it; this ticket has no React or SPA edit surface

### Targeted atomic notes

- [[client notice reactions belong to package declarations not client constants]]
- [[hub package event declarations have no client projection]]
- [[package event contracts live on HubPackageManifest not Core PackageManifest]]
- [[botster package surface semantics live in ui contract while hub owns admission]]
- [[botster package daemon dto exposes sanitized package rows]]
- [[git-visible Hub member manifests must use the UI contract tag]]
- [[first-party Rust consumers pin the UI contract Git tag not a Hub rev]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[hub test support npm releases need external consumer smoke]]
- [[conformance fixture revisions must be unique per published content]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[generated typescript dtos must encode serde field optionality]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[test script required for rust tests not cargo test]]
- [[botster test sh forwards arguments to cargo not custom unit flags]]
- [[rust repo strict lints must be verified before dismissing warnings]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[pre existing failure waivers must isolate the first non cascade failure on base]]

### Explicitly not loaded

- [[project-pipelines-playbook]] — Project Pipelines package and plugin paths are out of scope
- [[botster runtime teardown lenses]] — this ticket is a manifest contract, admission rule, and sanitized projection. It does not change peer lifecycle, `SessionIo`/`ClientWorker` teardown, or a resource-spin path
- Other repository charters (`botster-core`, `botster-web`, `botster-tui`)

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`
- Follow approved plan revision 3
- `botster-ui-contract` owns renderer-neutral notice vocabulary, pointer decoding, validation, and `resolve_notice_text`
- Hub owns `events.notices` admission and sanitized projection
- Core `PackageManifest` stays untouched
- `PROTOCOL_VERSION` stays 7; `CONFORMANCE_FIXTURE_REVISION` 44 → 45
- Git-consumed member manifests declare `tag = "botster-ui-contract-v0.3.3"`; the root `[patch]` path-resolves the crate until the maintainer tags merged `main`
- Do not publish npm packages or create the Git tag
- Direct-merge pipeline: commit on the ticket branch; do not create a PR

### Vault checklist

| Item | Result |
| --- | --- |
| Notes that constrained the work | The playbooks and targeted notes listed above. Filenames match vault `notes/` entries. |
| Convention conflicts | None. Session-start Rails conventions do not apply to this Rust Hub ticket. |
| Verification evidence | Commands in Tests and downstream proof. |
| Durable knowledge captured | Product ledger already lives in plan revision 3. Selected cutover coordinates are recorded here. Tag creation and npm publish remain maintainer follow-up. |

## Files changed

| Path | Change |
| --- | --- |
| `crates/botster-ui-contract/src/notices.rs` | Declaration/descriptor types, pointer decode, validation, `resolve_notice_text`, `NOTICE_TEXT_MAX_BYTES = 512` |
| `crates/botster-ui-contract/src/lib.rs` | Re-export notice API |
| `crates/botster-ui-contract/src/assets.rs` | Schema, TypeScript, and conformance vectors including byte-boundary and `~1`/`~0` cases |
| `crates/botster-ui-contract/Cargo.toml` | Version `0.3.3` |
| `crates/botster-ui-contract/README.md` | Document the notice contract |
| `crates/botster-ui-contract/tests/ui_contract_test.rs` | Validation and resolution unit tests |
| `crates/botster-ui-contract/tests/generated_assets_test.rs` | Generated asset and schema coverage |
| `packages/ui-contract/*` | Regenerated npm assets, `resolveNoticeText`, version `0.3.3` |
| `src/packages.rs` | Admit `events.notices` after emitted-event rules |
| `src/client_api.rs` | Project `HubClientPackage.notice_reactions` |
| `src/daemon_transport.rs` | Copy projected descriptors onto `DaemonPackage` |
| `src/daemon_projection.rs` | Empty `notice_reactions` on synthetic package rows |
| `crates/botster-hub-client/src/lib.rs` | `DaemonPackage.notice_reactions`, revision 45, serde tests |
| `crates/botster-hub-client/src/typescript.rs` | Optional `notice_reactions?` imported from `@trybotster/ui-contract` |
| `crates/botster-hub-client/generated/daemon-protocol.ts` | Regenerated |
| `crates/botster-hub-client/Cargo.toml` | UI contract tag `v0.3.3` |
| `crates/botster-hub-test-support/Cargo.toml` | UI contract tag `v0.3.3` |
| `crates/botster-hub-test-support/src/lib.rs` | Fixture assertion that the published ABI package declares a session notice |
| `crates/botster-hub-test-support/examples/node_package_assets.rs` | Metadata UI contract version `0.3.3` |
| `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/*` | ABI fixture emitted event + session notice |
| `fixtures/plugins/plugin-contract-matrix/*` | Matching repo-root fixture copy |
| `packages/hub-test-support/*` | Version `0.1.40`, revision 45, fixture and protocol copies |
| `examples/event-plane-producer/*` | Live producer schema, notice declaration, emit path |
| `tests/hub_daemon_lifecycle/package_event_plane.rs` | Live projection, session filter, resolver, and suppression proof |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | Revision 45 literal |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | Revision 45 literal |
| `docs/client-protocol.md` | Record the additive field and revision bump |
| `README.md` | UI contract / test-support version prose |
| `Cargo.toml` | Patch comment for tag `v0.3.3` |
| `Cargo.lock` | Path-resolved `botster-ui-contract` `0.3.3` |
| `script/fixtures/ui-contract-typescript-consumer.ts` | Packed TypeScript consumer of the new exports |
| `docs/reports/publish-package-owned-client-notice-reactions-implement.md` | This report |

## Ownership boundaries preserved

- Core is untouched. Event and notice policy stay on `HubPackageManifest`.
- `botster-ui-contract` owns renderer-neutral vocabulary and the shared resolver.
- Hub owns admission, exact-owner projection, and sanitized package rows.
- `botster-hub-client` owns the public `DaemonPackage` DTO and generated TypeScript.
- Web and TUI rendering stay with those repositories. This run does not remove client product constants.

## Cross-repo dependencies or separately routed work

No blocking cross-repo dependency tickets. Downstream consumers:

- `botster-web` `ticket_1787278327_274484` must use `resolveNoticeText` and bounded local diagnostics.
- `botster-tui` `ticket_1787278327_199618` must use the crate resolver and bounded local diagnostics.
- Project Pipelines cannot declare a session notice until `question.opened` gains a `subject` field.

Maintainer follow-up after merge: `script/tag-ui-contract` for `botster-ui-contract-v0.3.3`, then `script/publish-npm-packages`. Those coordinates are unpublished (`npm view` 404). Git tag `botster-ui-contract-v0.3.3` does not exist yet.

## Deviations from plan

- Live suppression proof emits empty, waits both subscribers, then emits oversized. After each emit, the wait issues `Status` so the connection loop takes a production host-control write turn and flushes admitted events. Timeout stays 10 s. This is the completion oracle for suite load.
- The start-boundary test hook now ignores untokened `daemon_test_guard` waiters. That is harness isolation for the existing race tests, not a product-behavior change.
- `emit_sample_ready_payload` now asserts `plugin_tool_result.status == "accepted"`, so a rejected emit fails at the producer rather than as a later wait timeout.
- Locked commands used `--offline` because tag `botster-ui-contract-v0.3.3` is not on the remote yet. The workspace `[patch]` path-resolves the crate. This is the plan's U2/R8 handling, not a product-behavior change.
- `DaemonPackage` stays exhaustive. The downstream source cost is one TUI test helper literal, recorded below. The plan allowed that measurement before a `#[non_exhaustive]` decision; this run does not add `#[non_exhaustive]`.

No accepted product-scope change. Plan acceptance checks are unchanged.

## Tests and downstream proof run

Provenance for live Hub proof:

- Hub binary from this candidate checkout after `cargo build --locked --offline --bin botster-hub`
- Session worker from locked Core `7eafa470a18025895995bbedc20d34b58106a03b` via `cargo build --locked --offline -p botster-core-daemon --bin botster-session-worker`
- Both binaries resolve under this checkout's `target/` directory

Commands (all through `./test.sh` or explicit `BOTSTER_ENV=test` except fmt/clippy/node):

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --workspace --all-targets --locked --offline -- -D warnings` | pass |
| `./test.sh --locked --offline --test hub_daemon_lifecycle_test isolated_hub_projects_notice_reactions_and_resolves_session_scoped_text -- --exact` | pass |
| `./test.sh --locked --offline --test hub_daemon_lifecycle_test isolated_hub` | 10 passed, including subscribe, `package_event`, `event_gap`, reconnect, and queue-limit tests |
| `BOTSTER_ENV=test cargo test --locked --offline -p botster-ui-contract -p botster-hub-client -p botster-hub-test-support` | pass |
| `BOTSTER_ENV=test cargo test --locked --offline --lib packages::` | pass, including notice admission tests |
| `node packages/ui-contract/test.mjs` | pass |
| Packed `npm pack` of `@trybotster/ui-contract@0.3.3` and `@trybotster/hub-test-support@0.1.40`, install both tarballs in a scratch consumer, assert metadata `0.1.40` / UI contract `0.3.3` / revision 45 / protocol 7 / matching `daemon_protocol.sha256`, `notice_reactions?`, imported descriptor with required `owner`, materialized fixture notice, `resolveNoticeText`, then strict `tsc` of `script/fixtures/ui-contract-typescript-consumer.ts` | pass |
| `node packages/hub-test-support/test.mjs` after installing the packed UI-contract tarball | pass |
| `./test.sh --locked --offline -p botster-hub-test-support published_plugin_contract_matrix_fixture_declares_a_session_notice` | pass |
| `./test.sh --locked --offline --test hub_daemon_lifecycle_test -- isolated_hub_projects_notice_reactions_and_resolves_session_scoped_text --exact` after Status-pump wait | pass |
| `BOTSTER_ENV=test cargo test --locked --offline -p botster-hub --lib package_event_schema::tests::accepts_some_string_uses_const_enum_and_length_bounds -- --exact` | pass, including `minLength: u64::MAX` |
| `./test.sh --locked --offline --test hub_daemon_lifecycle_test injected_taint_cannot_race_an_unguarded_real_daemon_start -- --exact` | pass |
| `./test.sh --locked --offline --test hub_daemon_lifecycle_test injected_taint_race_fails_when_start_guard_is_bypassed -- --exact` | pass |
| `./test.sh --locked --offline --test hub_daemon_lifecycle_test sibling_real_daemon_start_cannot_satisfy_intended_boundary_hook -- --exact` | pass |
| `./test.sh --locked --offline --test hub_daemon_lifecycle_test untokened_start_boundary_notify_is_ignored -- --exact` | pass |
| `./test.sh --locked --offline --test hub_daemon_lifecycle_test` default concurrency after taint-hook fix | 265 passed, 1 ignored. This is the binary that Review saw fail 157/108 with injected taint. The injected taint tests passed in this run. |
| `./test.sh --locked --offline` this visit, run 1 | fail-fast in `hub_daemon_lifecycle_test`: `shutdown_session_exact_keys_preserve_replacement_owner_and_siblings`. Isolated exact command passed on this branch in 2.36 s and on `origin/main` `b3b54f1` in 2.52 s after session-worker prebuild. |
| `./test.sh --locked --offline` this visit, run 2 | fail-fast in `--lib`: `owner_loop_queues_and_completes_two_fanout_plugin_handlers` (`left: 1`, `right: 2`). Previously isolated-passed on branch and `origin/main`. |
| `./test.sh --locked --offline` this visit, run 3 | lib passed; `hub_daemon_lifecycle_test` 264 passed, 1 failed: `unix_eof_skip_core_detach_ablation_keeps_named_pair_on_status`. Injected taint tests passed. Isolated exact command passed on this branch in 3.25 s. |

First full-suite root failure: `daemon_maintenance::tests::owner_loop_queues_and_completes_two_fanout_plugin_handlers` (`left: 1`, `right: 2` in-flight handlers). Isolated command `./test.sh --locked --offline -p botster-hub --lib owner_loop_queues_and_completes_two_fanout_plugin_handlers` passed on this branch. The same isolated command passed on `origin/main` `b3b54f1`. A second `./test.sh --locked --offline -p botster-hub --lib` (456 tests) passed. A second full `./test.sh --locked --offline` passed. This is a default-concurrency flake present on base, not a notice-contract regression.

Production path: IsolatedHub starts the real `botster-hub` binary, enables `examples/event-plane-producer`, and `ListPackages` returns the projected descriptor. Matching session-subject events resolve to `"ready"`. Empty and 513-byte notices still arrive as `package_event`s for two subscribers; `resolve_notice_text` returns typed `Empty` / `Oversized` errors and does not truncate.

Cutover uniqueness: `npm view @trybotster/ui-contract@0.3.3` and `npm view @trybotster/hub-test-support@0.1.40` return 404. Remote tag `botster-ui-contract-v0.3.3` is absent.

Downstream DTO cost: `DaemonPackage` is not `#[non_exhaustive]`. Adding `notice_reactions` is source-breaking for external struct literals. A scratch TUI worktree with a path `[patch]` earlier in this Implement run compiled production TUI code. One `cfg(test)` helper, `fn package()` in TUI `app.rs`, needed `notice_reactions: Vec::new()`. Direct git-tag consumption cannot resolve `botster-ui-contract-v0.3.3` until the maintainer tags merged `main`.

## Unverified behavior or residual risk

- This continuation did not rebuild the scratch TUI worktree. The literal cost above is from the earlier probe in the same Implement run.
- npm publication and Git tag creation are maintainer steps. Local `--offline` builds depend on the workspace path patch until the tag exists.
- `node packages/hub-test-support/test.mjs` still requires an installed `@trybotster/ui-contract`. That is an existing clean-checkout gotcha, now documented against `0.3.3`.
- Web and TUI must consume the shared resolver. This run does not prove those clients.
- Default-concurrency flake `owner_loop_queues_and_completes_two_fanout_plugin_handlers` can still fail a future suite run. Isolated evidence on branch and `origin/main` shows it is unrelated to this change.
- Workspace-load flake `webrtc_terminal_adapter_bound_peer_loss_closes_adapter_without_hub_detach` failed once under the full suite. Isolated reruns passed on this branch and on `origin/main` `b3b54f1` after the session-worker prebuild.
- Workspace-load flake `shutdown_session_exact_keys_preserve_replacement_owner_and_siblings` failed once under `./test.sh --locked --offline`. Isolated exact command passed on this branch and on `origin/main` `b3b54f1`.
- Workspace-load flake `unix_eof_skip_core_detach_ablation_keeps_named_pair_on_status` failed once under `./test.sh --locked --offline` after the taint fix. The injected-taint tests passed in that run. Isolated exact command is recorded in Tests.
- The Review taint cascade (108 `environment_tainted` failures) did not recur. Default-concurrency `hub_daemon_lifecycle_test` passed 265 tests.
- Mailbox `queue_age` remains 1 s production policy. Live waits now issue `Status` so a host-control write turn flushes admitted events.

## Missing vault guidance discovered

Plan revision 3 already named these. This run did not find additional missing notes:

- Version-one notice contract: session-only scope, `info|warning|error`, `ttl_ms` `1000..=60000`, one top-level RFC 6901 pointer with raw `/` count before `~1`/`~0` decode, `NOTICE_TEXT_MAX_BYTES = 512` after JSON decode with no trim or truncate.
- Invalid notice returns a typed resolver error and suppresses only the transient notice. The package event continues.
- Byte bound belongs in the shared resolver, not in JSON Schema, admission, or the producer hot path.
- Selected coordinates: `@trybotster/ui-contract` `0.3.3`, `@trybotster/hub-test-support` `0.1.40`, conformance revision 45, protocol 7.
- `node packages/hub-test-support/test.mjs` needs a local UI-contract install.
- Downstream source cost of the `DaemonPackage` field: one TUI test helper literal. That cost does not justify `#[non_exhaustive]` in this run.
