# Plan: Integration: cold-cut the old terminal route and prove isolation

Ticket: `ticket_1787600679_990088`
Run: `run_1788459722_264752`
Step: `botster_stack_plan` (`run_step_1788459723_541600`)
Pipeline: `botster_stack_delivery` (direct merge into `main`, no PR)
Plan revision 2, renewed 2026-09-03 on Hub base `ae6a0b1fe99d97215fa82d796da8f01a904171f0` (`origin/main`) after Plan Review `review_1788460991_459578`.
Human decisions: `question_1788460117_825061` and `question_1788461094_542980` (section 5).

## 1. Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn target name | `botster-hub` (canonical spawn-target checkout; no machine path) |
| Plan worktree | this pipeline worktree; path has no `:`; tracked `.gitignore` has content |
| Base commit | `ae6a0b1fe99d97215fa82d796da8f01a904171f0` (revision 1 used `bb1a330`; main advanced through `ticket_1788206393_323469`) |
| Locked Core pin at base | `48a437032791e678010254708259568ce4ad02bf` |
| Published Core revision to consume | `72d1c7571bc229dbb2cbd67aa979b6504ac150a5` (merge commit of `ticket_1787894967_973951`, `artifact_1788459695_462764`) |
| Merge policy | direct into `main` |
| Session-type eligibility consumer | no |
| `teardown_class_applies` | yes (section 12) |

Routing: `project_pipelines_current_context` gives `target_id=tgt_7e208a0c76a44980a83b63af976b1f22`. `list_spawn_targets` maps that id to `botster-hub`. The project target list confirms the same id on every Hub ticket in the project. The repository was not inferred from the working directory.

## 2. Repository playbook loaded

[[botster-hub-playbook]]

## 3. Other role and surface playbooks and atomic notes loaded

Role, in order:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]] (class applies)

Botster planner must-load context:

- [[botster-architecture]] (current modular map; the legacy monorepo is a different generation)
- [[cli-patterns]] (mixed-generation index; ownership taken from the Hub charter, not this map)
- [[spa-patterns]] (Web is a client consumer here; no SPA implementation)
- [[project pipeline orchestration belongs in a device-level botster plugin]]
- [[project pipelines needs an operator workbench not more primitives]]
- [[project pipelines ui contract belongs in the plugin readme]]
- [[botster orchestration should spawn agents with explicit target ids]] (this run is bound to `tgt_7e208a0c76a44980a83b63af976b1f22`)
- [[botster orchestration prompts must bind agents to explicit worktrees]] (Implement works in the run worktree on branch `project-pipelines/ticket_1787600679_990088`)
- [[botster pipeline needs continuous product owner between agent steps]] (product decision ledger in section 5)
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[vault example paths are not repository placement conventions]] (plan and report destinations come from Hub `docs/plans` and `docs/reports` prior art)

Charters consulted only to route consumer proof, not to implement in those repositories:

- [[botster-core-playbook]]
- [[botster-hub-client-playbook]] (in-repo member crate; its gates apply if DTO files change)
- [[botster-web-playbook]]
- [[botster-tui-playbook]]

Not loaded, with reason:

- [[project-pipelines-playbook]]: no Project Pipelines package or plugin path changes.
- [[botster-workspaces-playbook]], [[botster-tui-kit-playbook]], [[botster-terminal-ghostty-playbook]]: no changes in those repositories.

Targeted atomic notes:

- [[botster hub is a first party host profile over core]]
- [[botster Hub Rust stays a trusted host kernel]]
- [[lua plugins are the hub composition layer]]
- [[core owns duplex terminal transport while Hub stays content blind]]
- [[core terminal progress is wake driven and targeted]]
- [[concrete terminal transports stay in hub until a second host needs them]]
- [[botster subscriptions use dedicated ordered DataChannels]]
- [[the browser creates each subscription DataChannel after Hub reserves its label]]
- [[WebRTC input delivery chunks reassemble encrypted Core frames before decryption]]
- [[core owns bounded atomic terminal input transactions across clients]]
- [[Hub Core pin rolls update eleven literal sites and six lock sources]]
- [[a downstream reproduction ticket can be overtaken by a pin roll]]
- [[Hub test support copies Core protocol fixtures from the pinned crate source]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[cold turkey migrations eliminate dual code paths and version suffixes]]
- [[cold cut grep gates exclude rejection tests that name retired inputs]]
- [[code moves need paired absence and presence source guards]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[source guard ablations must not overlap a running full suite]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]
- [[terminal transport north star publishes behavioral oracles not numeric budgets]]
- [[loaded daemon lifecycle workflow is structurally single repository]]
- [[webrtc peer cleanup removes every per peer owner together]]
- [[terminal webrtc failure records do not prove peer runtime teardown]]
- [[Core terminal subscription ownership is session, subscription, and generation]]
- [[Core subscription hard-stop is synchronous close and drop on the host tick]]
- [[ShutdownSession suppresses exact route generations before Core teardown]]
- [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]
- [[a post-bind start gate is not an obsolete progress trigger]]
- [[pin rolls update live lane provenance defaults and README pin prose]] (TUI consumer roll)
- [[botster web pinned hub test support claims span readme and architecture docs]] (Web consumer check)

## 4. Context loaded

Botster layers touched: Rust hub (dependency pin, source guards, lifecycle tests, README), hub-client member crate (test guard only), docs. Not touched: Lua core, plugins, session/client worker (Core), TUI, React SPA, Rails relay, MCP.

Worktree and target assumptions: Implement runs in this run worktree on branch `project-pipelines/ticket_1787600679_990088`, rebased on `ae6a0b1`. The path contains no `:`; `CARGO_TARGET_DIR` stays unset; the default `target/` holds the 1.97.0 artifacts. Consumer proofs use read-only checkouts of Web at `origin/main` and a scratch TUI worktree.

Pipeline gates and artifacts: Plan gate (this artifact), Plan Review, Implement (commits plus `docs/reports/...-implement.md` artifact and the observation JSON), Review, Verify (independent rerun of the strict gates and named proofs), direct merge to `main`.

Required docs updates: Hub `README.md` responsibility text (section 6.C.4). No plugin README changes.

Base renewal (revision 2): `bb1a330..ae6a0b1` changed `src/transport/webrtc/adapter.rs` (a `#[cfg(test)]` block now uses `bind_waking_terminal_adapter` and `pump_woken`), `tests/hub_daemon_lifecycle/{package_fixtures,sessions,unix_terminal_adapter,webrtc_proofs,webrtc_terminal_adapter}.rs` (polling seams migrated to targeted wakes; WebRTC producer readiness gate restored), plus two docs. No production transport code changed. The guard inventory, named tests, and pin-site list in this plan were re-checked against `ae6a0b1`; the adapter test change removes the last Hub call to a Core name deleted at `72d1c75`, which strengthens assumption 1 in section 9.

Facts verified in this Plan visit:

1. All four dependency tickets are closed: Web terminal DataChannel (`ticket_1787600676_914408`), TUI duplex input (`ticket_1787603674_865638`), Web entity and package-event DataChannels (`ticket_1787600684_892051`), and Core polling adapter deletion (`ticket_1787894967_973951`). No other ticket in project `project_1787600579_585482` is open.
2. The absorbed audit ticket `ticket_1787600691_401181` is closed with no run. Its requirements are folded into section 6, item C.
3. Hub `main` already deleted `DaemonRequest::SendInput`, `ModeGatedInput`, and `Resize` (commits `b1aab5d`, `bf95942`, `ticket_1787894427_525056`). The only remaining references are absence guards: `tests/daemon_control_ownership.rs` (`duplicating_a_variant_into_the_wrong_owner_fails_the_matrix`), `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` (`terminal_input_is_not_a_json_control_request`), `crates/botster-hub-client/src/lib.rs` (TypeScript generation guard for `mode_gated_input`), and `packages/hub-test-support/test.mjs` line 179 (README guard).
4. `HubRuntime::drain_runtime_once` has no callers. `src/lib.rs` `FORBIDDEN_PRODUCTION_CONSTRUCTS` forbids `drain_subscription(`, `drain_runtime_once(`, `.drain(session_id`, `lifecycle_baseline()`, and the terminal `DaemonEvent` variants in production source. `DaemonRequest::Drain` and `HubClientRequest::DrainRuntime` return empty events and drive no Core terminal progress.
5. `no_lua_dispatch_in_terminal_input_or_output` walks every file under `src/` and allows `lua_runtime` only in `src/lib.rs` and `src/runtime.rs`. No transport, data-plane, subscription, or admission module names Lua.
6. `HubRuntime::bind_terminal_adapter` (`src/runtime.rs`) calls Core `bind_waking_terminal_adapter`. Core `72d1c75` removed `bind_terminal_adapter`, `drain_runtime_once_without_pump`, `apply_terminal_input`, `pump_bound_adapters`, `intake_terminal_input`, `prepare_terminal_input`, and `handle_session_request_with`. Hub source and tests call none of those Core names.
7. Core `48a4370..72d1c75` changes no file under `crates/botster-terminal-protocol`, `crates/botster-terminal-protocol-client`, or `packages/`. Hub test-support fixtures copied from the protocol crate therefore do not change content. Only provenance literals change.
8. The old Core SHA appears at 18 active source sites plus 6 `Cargo.lock` sources plus historical `docs/plans` and `docs/reports` entries (see section 8).
9. TUI `origin/main` (`b051c67`) pins Hub `bb1a330` and Core `48a4370` in `crates/botster-tui/Cargo.toml`, in `crates/botster-tui/src/app.rs` live-lane defaults, and in README prose. The TUI live lane asserts `fixture core_pin == worker rev`.
10. Web `origin/main` (`e5573a2`) pins `@trybotster/hub-test-support@0.1.43`, `@trybotster/terminal-protocol@0.3.0`, and `@trybotster/ui-contract@0.3.3`. Web pins no Hub Git revision. Web live lanes take `BOTSTER_HUB_BIN` and `BOTSTER_SESSION_WORKER_BIN`.
11. `gh api repos/trybotster/botster-hub/actions/runners` returns zero runners. No `botster-ubuntu-24.04-16core` runner exists. No post-Restty controlled baseline exists (botster-web `docs/terminal-baseline-observation-format.md`, version 3).
12. The local trybotster repository contains `f598075e6c143ef14b34d3a3dffdf2ec6a8d9eb6` and has a dirty `main` checkout.
13. Hub strict gates come from `.github/workflows/ci.yml`: Rust 1.97.0, Zig 0.16.0, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `./test.sh --locked`, plus `node packages/hub-test-support/scripts/sync-assets.mjs --check`.

## 5. Human decisions (product decision ledger)

| Decision | Source | Binding outcome |
| --- | --- | --- |
| Reference-runner comparison | `question_1788460117_825061` | Waived for this ticket. `botster-ubuntu-24.04-16core` has zero registrations and no post-Restty controlled baseline exists. The report records the rerun steps from botster-web `docs/terminal-baseline-observation-format.md` "Controlled runner rerun" verbatim. |
| Two-arm `format_version=3` local timing record | `question_1788461094_542980` (supersedes the option B choice in `question_1788460117_825061` after Plan Review found the authentication prerequisite) | Waived for this ticket. The report records the exact rerun steps, the GitHub sign-in prerequisite (`completeLegacyNewSession` fails closed at "Sign in with GitHub"), the missing Playwright storage-state input in the harness, and the separate legacy database and GitHub OAuth app requirements. No Web harness, legacy application, authentication path, or credential handling change. No ticket for this observation. |
| Deterministic correctness gates | both answers | Required and unchanged: isolation, byte order, pressure, reconnect, old-route deletion, current-revision compatibility (Core `72d1c75`), and north-star ownership. The waiver applies only to timing observations. |
| Defaults | this plan | Cold cut only; no compatibility path; no npm publication; TUI durable roll is a registered consumer ticket. |
| Ask-human threshold | this plan | Implement asks before any change outside `botster-hub`, before any npm or crate publication, and before touching the local trybotster repository. |
| Review-return consumer lanes | `question_1788465866_563736` | Do not waive durable, shared-session cancel ablation, or `ghostty-shared` late-history. Keep the durable-session failure in this Hub run. Add the smallest Hub persistence repair plus a daemon-restart ended-row regression test. Do not add compatibility behavior or a second persistence path. Create exactly one botster-web ticket for cancel ablation and exactly one botster-tui ticket for `ghostty-shared` late-history. This integration ticket depends on those two tickets. Each consumer ticket runs repository gates and one focused integration proof. This run reruns the complete matrix once after both merges. |

## 6. Scope

Invariant: this run changes Hub only. Every changed Hub line traces to the Core pin roll, the residual-route proof, the ownership audit, the proof reports, or the Review-return persistence repair in F.

### A. Core pin roll to `72d1c7571bc229dbb2cbd67aa979b6504ac150a5`

1. Update every active Core revision literal (section 8 list) and `Cargo.lock` (`cargo update -p botster-core -p botster-core-daemon -p botster-terminal-protocol -p botster-core-test-support -p botster-terminal-ghostty --precise 72d1c75...` or equivalent locked refresh). Keep the Git URL and `rev =` selector form.
2. Completion rule: `grep -rn 48a437032791e678010254708259568ce4ad02bf --exclude-dir=target --exclude-dir=node_modules --exclude-dir=.git .` returns matches only under `docs/plans/` and `docs/reports/`. `grep -c 72d1c7571bc229dbb2cbd67aa979b6504ac150a5 Cargo.lock` equals 6.
3. Prebuild `botster-session-worker` and `botster-hub` in the default `target/` before the locked suite.
4. `tests/session_projection_owner_loop.rs` and `subscription_ownership_baseline.rs` `LOCKED_CORE_REV` must pass at the new pin.

### B. Prove no runtime reads, writes, parses, serializes, or routes the old terminal path

Produce a guard inventory table in the Implement report. Each row names one ticket category, the guard test, the file it scans, and a one-line ablation result (seeded token turned the guard red, then restored). Existing rows:

| Category | Guard |
| --- | --- |
| JSON terminal handlers (`SendInput`, `ModeGatedInput`, `Resize`) | `duplicating_a_variant_into_the_wrong_owner_fails_the_matrix`; `terminal_input_is_not_a_json_control_request` |
| Terminal JSON RPC DTO serialization | hub-client TypeScript guard (`mode_gated_input` absence); `each_daemon_request_has_exactly_one_family_owner` |
| Shared-channel terminal routing | `webrtc_dedicated_channels_carry_control_entity_event_and_terminal_frames`; `webrtc_terminal_adapter_second_data_channel_does_not_receive_terminal_frames`; `webrtc_ready_entity_frame_defers_terminal_output` |
| Translation of terminal bodies | `webrtc_terminal_adapter_source_does_not_name_snapshot_phases`; `hub_transport_source_stays_paste_blind`; `FORBIDDEN_PRODUCTION_CONSTRUCTS` (`READY`, `PAGE`, `FINISH`, `GHOSTSNP`) |
| Drain-driven terminal progress | `FORBIDDEN_PRODUCTION_CONSTRUCTS` (`drain_subscription(`, `drain_runtime_once(`, `.drain(session_id`); `paused_data_plane_keeps_control_requests_from_driving_terminal_progress`; `pump_woken_lives_only_in_the_data_plane_driver` |
| Lua in hot paths | `no_lua_dispatch_in_terminal_input_or_output` |

Holes to close in this run (smallest change that makes the row exist):

1. Extend the hub-client TypeScript generation guard so the generated union contains no `type: "send_input"` and no `type: "resize"` request members, beside the existing `mode_gated_input` assertion. The `resize` feature token in `first-party-client-support-matrix.json` is a capability name and stays.
2. Add one region-bounded negative scan over `src/transport/**` and `src/data_plane/**` for terminal retry or scheduling tokens (`retry_terminal`, `reschedule_terminal`, `terminal_backoff`, `requeue_frame`) with a required-symbol anchor, per [[region bounded source guards need a required symbol anchor]]. Existing `retry` hits in `src/transport/webrtc/peer.rs` (peer close retry) and `src/transport/unix/mux_write.rs` (host mux frame rotation) are host-control mechanics, not terminal bytes; name them as exemptions in the guard.
3. If the inventory finds any remaining production symbol from the old route, delete it in this run. Do not add a compatibility path.

### C. Responsibility audit (absorbs `ticket_1787600691_401181`)

1. Write an ownership table in the Implement report: each `src/` top-level module and directory mapped to one class from {admission, security, persistence, process and package supervision, WebRTC setup, adapter creation and transport mechanics, plugin isolation and safe Lua primitives, control-plane dispatch, host policy composition}. Any module that fits no class is a finding; resolve it in this run or record it as recorded drift with exact scope in the report (no new ticket; project rule 2026-09-02).
2. Confirm `daemon_transport.rs` and `local_webrtc.rs` do not exist, and `daemon_modules_reject_unix_transport_mechanism_symbols` still holds.
3. Confirm Lua: the recursive `no_lua_dispatch_in_terminal_input_or_output` guard is the architecture check. Add `src/data_plane.rs`, `src/data_plane/driver.rs`, `src/data_plane/close_work.rs`, and `src/transport/shared/ingress.rs` (if present) to its fixed list so each named hot-path file has an explicit row, and run one ablation per added entry per [[fixed source guard lists need one ablation per added file]].
4. Update `README.md` "Responsibility split" `botster-hub` row to state the final split in one sentence each: Hub Rust owns admission, security, persistence, process and package supervision, WebRTC setup, adapter creation, plugin isolation, and safe Lua primitives; Core owns terminal lifecycle and duplex transport mechanics; Lua composes commands, hooks, workflows, lifecycle policy, defaults, and customization and never runs in terminal input or output hot paths. Update the "Product today" sentence that lists `input/resize` under the daemon protocol to say input and resize travel on the bound adapter plane.
5. Playbook updates are vault work. Implement writes one inbox capture with the exact proposed additions for [[botster-hub-playbook]] (Required Gates rows: "Lua absent from terminal hot paths is a recursive source guard", "old terminal JSON route absence guards"), [[botster-core-playbook]] (Core owns terminal lifecycle and duplex transport; polling adapter path deleted at `72d1c75`), and [[botster-architecture]] (final ownership statement). Gate evidence cites the capture by wiki-link or inbox filename, not a home path. No knowledge-repo ticket is created.

### D. Full-stack proof matrix, run once here

All Hub commands run with `RUSTUP_TOOLCHAIN=1.97.0`, `CARGO_TARGET_DIR` unset, `BOTSTER_ENV=test`, and a quiet host (`script/process-census dev-artifact-rows` empty before the lifecycle suite).

Hub repository gates:

```sh
export RUSTUP_TOOLCHAIN=1.97.0; unset CARGO_TARGET_DIR
rustc --version; zig version   # 1.97.0, 0.16.0
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo build --locked --bin botster-hub
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
node packages/hub-test-support/scripts/sync-assets.mjs --check
./test.sh --locked
(cd packages/hub-test-support && npm install --no-save && npm test)
```

Hub deterministic correctness and bounds gates (named tests, all inside `./test.sh --locked`, also run isolated with `--exact` and the full module path):

| Claim | Test |
| --- | --- |
| Exact byte order and delivery, WebRTC | `hub_daemon_lifecycle::subscription_ownership_baseline::webrtc_terminal_output_is_byte_exact`; `webrtc_proofs::external_hub_webrtc_live_output_preserves_exact_bytes` |
| Exact byte order and delivery, Unix | `unix_terminal_adapter::unix_adapter_bound_printf_stream_attach_delivers_process_exit`; `paste_transaction::unix_paste_transaction_delivers_one_result_and_byte_exact_pty_content` |
| Multi-frame input | `paste_transaction::webrtc_paste_transaction_delivers_one_result_and_byte_exact_pty_content` |
| Terminal progress during sustained entity and package-event traffic | `subscription_ownership_baseline::webrtc_dedicated_channels_carry_control_entity_event_and_terminal_frames`; `webrtc_proofs::daemon_package_entity_held_open_fanout_over_local_webrtc`; `package_event_plane::isolated_hub_two_packages_emit_and_consume_exact_event_without_blocking_worktree` |
| Control requests do not drive terminal progress | `unix_terminal_adapter::paused_data_plane_keeps_control_requests_from_driving_terminal_progress` |
| One slow subscription does not delay another (ingress) | `paste_transaction::paused_ingress_sixty_fifth_frame_latches_lost_and_closes_only_that_route` |
| One slow subscription does not delay another (egress) | `unix_terminal_adapter::core_write_budget_hard_stop_emits_core_adapter_closed`; `webrtc_terminal_adapter::webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable`; plus the new test in item D.1 |
| Sibling isolation on teardown | `subscription_ownership_baseline::peer_close_leaves_sibling_peers_working`; `unix_terminal_adapter::shutdown_session_exact_keys_preserve_replacement_owner_and_siblings`; `webrtc_terminal_adapter::one_session_unix_and_webrtc_dual_attach_exposes_hub_occupancy` |
| Reconnect and one-document reconnect | Web `smoke:live-packaged-protocol:shared-session` (cancel, keep-alive, reconnect on the surviving document) |
| Lifecycle and Ghostty coverage unchanged | full `./test.sh --locked`; TUI `script/test-live-hub ghostty` |

D.1 New deterministic test, required: the inventory above has no test where a slow **open** terminal route and a healthy sibling route on the same connection both stay open while the sibling keeps receiving bytes. Add one Hub lifecycle test (Unix, and WebRTC if the seam exists there) that holds one route below its close budget using the existing `BOTSTER_HUB_TEST_FORCE_ADAPTER_WOULD_BLOCK_SESSION` seam, then asserts the sibling route delivers its expected bytes and the held route is still `Bound` (not closed) at the end. Oracle is bytes plus route state, not wall clock. Red-on-revert: run once with the seam applied to both routes and confirm the sibling assertion fails. If Implement finds an existing test that already carries this exact oracle, cite it in the inventory instead of adding one.

Downstream consumer proofs (current-revision compatibility) against the candidate Hub build (`target/debug/botster-hub` and `target/debug/botster-session-worker` from the pin-rolled commit):

- Web (`botster-web` at `origin/main`, no edits): `npm ci`, `npm test` (drift check against `@trybotster/hub-test-support@0.1.43`; the Core roll changes no Hub DTO, so no republish is expected), then `npm run smoke:live-packaged-protocol`, `npm run smoke:live-packaged-protocol:durable`, `npm run smoke:live-packaged-protocol:shared-session`, and `npm run smoke:plugin-contract-matrix` with `BOTSTER_HUB_BIN` and `BOTSTER_SESSION_WORKER_BIN`. Real browser through the production engine types (the harness uses the shipped WebRTC data plane).
- TUI (scratch worktree of `botster-tui` at `origin/main`): roll `botster-hub-client` and `botster-hub-test-support` to the Hub candidate commit and every Core pin to `72d1c75` in `crates/botster-tui/Cargo.toml`, `Cargo.lock`, and the `app.rs` live-lane defaults, uncommitted, for proof only. Run `script/test-live-hub ghostty` with `BOTSTER_HUB_BIN_REV=<candidate>` and `BOTSTER_SESSION_WORKER_BIN_REV=72d1c75...`. Record the exact TUI diff in the report. The durable TUI roll is the consumer ticket in section 7.
- Unix and local Hub: Hub `unix_terminal_adapter` module, `botster-hub smoke` against a fresh data dir, and `webrtc_proofs::cli_smoke_proves_local_runtime_daemon_package_app_session_and_webrtc`.
- North-star same-session ownership: `script/prove-north-star-shared-session` with `BOTSTER_WEB_CHECKOUT`, `BOTSTER_TUI_CHECKOUT` (the scratch TUI worktree), and `BOTSTER_SHARED_SESSION_ID=north-star-shared`; then Web `drive:live-packaged-protocol:shared-session` and TUI `ghostty-shared` and `ghostty-shared-exit` as the script documents.

Frozen-format observation (waived, `question_1788461094_542980`): no timing capture runs in this ticket. The Implement report carries a "Timing observations: waived" section with:

1. The exact rerun command from botster-web `README.md` (`BOTSTER_LEGACY_CHECKOUT=<clean f598075e> BOTSTER_HUB_SOURCE=<hub> npm run observe:terminal-baseline`, then `observe:terminal-baseline:validate`), keeping `format_version=3`.
2. The prerequisites that block it today: a signed-in GitHub session on the legacy arm (the harness has no storage-state or cookie input), a provisioned legacy development database, GitHub OAuth application credentials, and a clean `f598075e` checkout (a detached worktree of the local trybotster repository is one way to obtain it).
3. The controlled-runner rerun steps and the fact that zero runners are registered.
4. The statement that no row of any future record is transport causality (`product_baseline_only`).

### E. Reports and evidence

- `docs/reports/cold-cut-the-old-terminal-route-and-prove-isolation-implement.md`: pin roll diff summary, guard inventory with ablations, ownership audit table, gate outputs with `rustc --version`, consumer proof outputs, TUI scratch diff, the two waivers with rerun steps and prerequisites, provenance (`hub_sha`, `locked_core_sha`), and the Review-return persistence repair.

### F. Review-return persistence repair (accepted `question_1788465866_563736`)

Keep one persistence path: Core's session registry under the Hub data directory. After `ShutdownSession` the registry row is `Exited`. After Hub process restart, Core `list()` still returns that row, but the engine session is gone, so `lifecycle_record` omits `lifecycle`. Hub projection must treat `registry_state=Exited` with omitted engine lifecycle as ended evidence and project `lifecycle=exited` / `lifecycle_class=ended`. Running rows with omitted lifecycle stay `indeterminate`. Do not add a Hub-owned session store.

Required test: `process_ownership_daemon_restart_lists_ended_session_row`. Spawn, `ShutdownSession`, production Hub shutdown, restart on the same data directory. Assert `Status.session_count >= 1`, `ListSessions` keeps `lifecycle=exited`, and a later entity subscriber receives the ended row.

Consumer tickets (created by Implement; this ticket depends on them):

- `ticket_1788467459_333288` (`tgt_40abcf71ccf049f4ac0c99953a799869`): Web cancel ablation. `BOTSTER_LIVE_ABLATE_CANCEL_DETACH=1` must fail for the intended reason. Preserve the dedicated channel path.
- `ticket_1788467460_864070` (`tgt_c3d470bab78549df920a41e8fb0e58d8`): TUI `ghostty-shared` late-history. Reproduce the integrated failure first. Preserve isolated `script/test-live-hub ghostty` and the ready-then-history contract.

Do not create a second Web ticket for durable restore. Do not create another Hub ticket. The complete consumer matrix in D reruns once after both consumer merges.

### Non-scope

- Any Core, Web, TUI, Workspaces, or Ghostty source change. The TUI scratch pin roll is uncommitted proof scaffolding only.
- Transport crate extraction, replay buffers, raw WebRTC plugin access, subscription limit tables (frozen artifact section 9, owned by closed `ticket_1787600682_233928`).
- Any `@trybotster/hub-test-support` npm publication. The Core roll changes no Hub DTO and no protocol fixture (section 4, item 7). Web `npm test` at `0.1.43` is a required gate; if it fails, that is a product finding to resolve in this run's Review, not a publication trigger.
- Registering the reference runner, producing the controlled comparison, or capturing the two-arm timing record (both waived; section 5).
- Any change to the botster-web baseline harness, the legacy application, or authentication and credential handling.
- Weakening or deleting lifecycle, Ghostty, reconnect, or one-document reconnect tests.

## 7. Repository ownership boundaries and cross-repository dependencies

- `botster-core` owns terminal subscription identity, attach phases, duplex bytes, mode-gated input, resize, ordering, bounded queues, pressure, generation fencing, teardown, wakes, and targeted pumping. Published at `72d1c75`; consumed here by pin only.
- `botster-hub` owns admission (grants, key derivation, labels, peer generations, budgets, route policy), subscription route state (Reserved, Bound, Retired), concrete Unix and WebRTC mechanics (framing, sealing, chunking, bounded close), the hosting process, and the data-plane driver. Hub does not decode terminal bodies.
- `botster-hub-client` (in-repo member) owns the external DTO boundary. Only a test guard changes; if the generated TypeScript changes, the [[botster-hub-client-playbook]] gates apply.
- Lua plugins compose commands, hooks, workflows, lifecycle policy, defaults, and customization outside transport hot paths.
- `botster-web` and `botster-tui` are equal clients. Web owns cancel ablation (`ticket_1788467459_333288`). TUI owns `ghostty-shared` late-history (`ticket_1788467460_864070`) and the durable pin roll after this merge. Hub owns persisted exited-row projection after daemon restart.

Upstream dependencies: all closed (section 4, item 1).

Downstream consumer registration (different repository, blocks its own live gate): create `TUI: roll Hub and Core pins to the integration cold cut` against `tgt_c3d470bab78549df920a41e8fb0e58d8` with a dependency on this ticket. Scope: roll `botster-hub-client` and `botster-hub-test-support` to the merged Hub commit, roll every Core pin to `72d1c75`, update `app.rs` live-lane defaults and README pin prose per [[pin rolls update live lane provenance defaults and README pin prose]], and run `script/test-live-hub ghostty`. This is a durable-pin follow-up, not a finding ticket.

This integration ticket now depends on two later consumer tickets from `question_1788465866_563736`:

- `ticket_1788467459_333288` botster-web cancel ablation (`tgt_40abcf71ccf049f4ac0c99953a799869`)
- `ticket_1788467460_864070` botster-tui `ghostty-shared` late-history (`tgt_c3d470bab78549df920a41e8fb0e58d8`)

Those tickets must close before a later pipeline run of this ticket starts. The current Implement visit may finish the Hub persistence repair. The complete matrix in section 6.D reruns once after both merges.

## 8. Affected surfaces and files

Core pin literals (18 active sites, verified by grep at base):

- `Cargo.toml` (5 dependencies)
- `crates/botster-hub-client/Cargo.toml` (1)
- `crates/botster-hub-test-support/Cargo.toml` (3)
- `crates/botster-hub-test-support/build.rs` `PROTOCOL_REV`
- `crates/botster-hub-test-support/src/conformance_data.rs` `LATE_ATTACH_GHOSTSNP_CORE_PIN`
- `crates/botster-hub-test-support/src/lib.rs` provenance unit-test literal
- `tests/session_projection_owner_loop.rs` `REQUIRED_CORE_REV`
- `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` `LOCKED_CORE_REV`
- `tests/hub_daemon_lifecycle/event_plane_saturation.rs`, `package_event_plane.rs`, `unix_terminal_adapter.rs`, `webrtc_terminal_adapter.rs` live-proof literals
- `Cargo.lock` (6 sources)

Guards and docs:

- `crates/botster-hub-client/src/lib.rs` (TypeScript guard extension)
- `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` (Lua guard list, new terminal retry/scheduling scan)
- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` and/or `webrtc_terminal_adapter.rs` (D.1 sibling-progress test)
- `README.md` ("Responsibility split", "Product today" sentences)
- `docs/plans/cold-cut-the-old-terminal-route-and-prove-isolation.md` (this plan)
- `docs/reports/cold-cut-the-old-terminal-route-and-prove-isolation-implement.md`
- vault inbox capture for the final ownership statement (outside this repository; cite by note title, not a home path)

Persistence repair (section 6.F):

- `src/session_projection.rs` (ended class and `lifecycle=exited` when the registry is Exited and engine lifecycle is omitted)
- `src/subscription/entity.rs` (matching test-only projection copy)
- `tests/hub_daemon_lifecycle/shutdown.rs` (`process_ownership_daemon_restart_lists_ended_session_row`)

Unchanged by design: `src/transport/**`, `src/data_plane/**`, `src/admission/**`, `src/daemon/**` unless the inventory (6.B.3) finds a residual old-route symbol. Production `src/subscription/` stays unchanged except the cfg(test) projection copy in `entity.rs`.

## 9. Assumptions and unknowns

| Item | Handling |
| --- | --- |
| Hub compiles against Core `72d1c75` without source edits | Expected: at `ae6a0b1` Hub uses `bind_waking_terminal_adapter` and `pump_woken` and none of the removed names. Implement step 1 is the locked build; if it fails, the fix is Hub-side adoption of the published surface only, and the report names each changed line. |
| `@trybotster/hub-test-support` fixtures unchanged | Verified: Core diff touches no protocol crate or `packages/` path. `npm test` and Web drift check re-verify. |
| Hub full suite needs a quiet host | Poll `script/process-census dev-artifact-rows` until empty. Attribute a flake with an isolated `--exact` run (full module path) before any retry; never kill foreign daemons. |
| Timing observations | Both the controlled-runner comparison and the two-arm local record are waived (section 5). The report records rerun steps and prerequisites. |
| TUI scratch roll compiles at the candidate Hub | Expected; TUI already consumes Hub `bb1a330` DTOs, and this run changes no DTO. |

## 10. Risks

- **Mixed pin.** A roll that misses one of the 18 sites leaves provenance tests green on manifests but wrong in live proof. Mitigation: zero-old-SHA grep and `Cargo.lock` count of 6 are gate evidence.
- **Guard ablation overlapping the suite.** Seeding forbidden tokens while `./test.sh --locked` runs invalidates the run. Mitigation: complete every ablation and restore before starting the official gate.
- **Wall-clock oracles under load.** New D.1 test must use bytes plus route state, not elapsed time.
- **Cascade taint.** One `owned worker pid N still live` taints later lifecycle tests. Read the first non-cascade failure.
- **Waiver drift.** A later reader may treat the waived timing record as measured evidence. The report states that no timing was captured in this ticket.
- **Scope creep in the audit.** Findings become report rows or same-run fixes, never new tickets, per the 2026-09-02 consolidation rule.

## 11. Acceptance checks and tests

1. Hub strict gates green at the candidate commit with `rustc 1.97.0` and `zig 0.16.0` quoted from the same shell.
2. Zero old-SHA matches outside `docs/plans` and `docs/reports`; `Cargo.lock` has 6 `72d1c75` sources; `hub_sha` and `locked_core_sha` recorded per the CI step.
3. Guard inventory complete: every ticket category has a named guard, each guard has one recorded red ablation and restore, including one per added Lua-guard file entry.
4. New D.1 sibling-progress test green, with its red arm recorded.
5. Ownership audit table complete with no unclassified module, or each unclassified module recorded with exact scope.
6. Web `npm test` plus the four live lanes green against the candidate binaries, with `live-shared-session-*` markers printed.
7. TUI `script/test-live-hub ghostty` green against the candidate with the scratch pin diff recorded; `ghostty-shared-complete` and `ghostty-shared-exit-complete` printed in the north-star run.
8. `script/prove-north-star-shared-session` completes with Web and TUI on one caller-owned session.
9. `botster-hub smoke` green on a fresh data dir.
10. Implement report contains the "Timing observations: waived" section with both waivers, rerun commands, and the authentication, database, OAuth, and clean-checkout prerequisites; no timing number appears as a gate.
11. README responsibility text updated; inbox capture path recorded.
12. Consumer TUI pin-roll ticket created with a dependency edge on this ticket.
13. Daemon-restart ended-row test green: `process_ownership_daemon_restart_lists_ended_session_row`. Complete-baseline `registry_state=Exited` with omitted engine lifecycle is ended evidence. Running + omitted lifecycle stays indeterminate.
14. Web cancel-ablation ticket `ticket_1788467459_333288` and TUI late-history ticket `ticket_1788467460_864070` exist. This ticket depends on both. The complete matrix in section 6.D reruns once after both merge.

## 12. Runtime-teardown lenses

`teardown_class_applies`: yes. The pin roll consumes Core `4f40bcc` (explicit adapter close on every bind rejection, pre-attach declaration removal on every attach return) and the run proves route deletion, sibling isolation, and peer or connection loss paths.

`teardown_isolation`: the ownership set that dies with one failed route is `(session_id, subscription_id, generation)` plus its Hub route row, reservation, and adapter slot. Unix: one connection's routes die on EOF; other connections continue. WebRTC: one peer's routes die on `PeerClosed`; healthy sibling peers continue after a successful close (`peer_close_leaves_sibling_peers_working`).

`teardown_bounds`: WebRTC close uses `LOCAL_WEBRTC_PEER_CLOSE_BOUND`; timeout is ultimate failure and takes the documented fail-closed path. Core hard-stop is synchronous close and drop on the host tick. Unix close work retires to the live route baseline. No unbounded `block_on(close)` is introduced; this run adds no close path.

`late_message_matrix` (unchanged by this run, verified by existing guards):

| Message | Tag | Reject after terminal failure | Sweep on race |
| --- | --- | --- | --- |
| Attach | grant + session + subscription + generation | stale generation or unknown subscription rejected at bind (`mismatched_terminal_hello_rejects_attach_before_core_ownership`, Core `4f40bcc` closes the adapter on rejection) | `webrtc_terminal_adapter_late_attach_after_peer_close_does_not_recreate_route` |
| Detach | exact route key | idempotent; separate from connection death | `unix_adapter_detach_retires_close_work_to_the_live_route_baseline` |
| SubscribeEntities / UnsubscribeEntities | connection or grant owner | typed OperatorError on bound mux | connection-scoped cleanup |
| SubscribeEvents / UnsubscribeEvents | connection-scoped holder | typed error when unnegotiated | `isolated_hub_reconnect_does_not_replay_package_events` |
| Reserved-label channel open | reservation owner peer | expired, never-reserved, wrong-peer rejected | `webrtc_late_channel_after_reservation_expiry_emits_reservation_expired` |
| ShutdownSession | exact route generations | suppression before Core teardown | `shutdown_suppresses_exact_route_generations_before_core_teardown` |
| Terminal input frames (binary) | subscription route | closed route drops; 65th paused frame latches lost and closes only that route | `paused_ingress_sixty_fifth_frame_latches_lost_and_closes_only_that_route` |

`production_path_proof`: peer loss → `PeerClosed` handler → route sweep → Core `detach_terminal_subscription` → adapter close → driver idle; Unix EOF → connection cleanup → live route set release. Live oracles: `webrtc_terminal_adapter_bound_peer_loss_closes_adapter_without_hub_detach`, `unix_eof_releases_exact_attach_occupancy_on_sibling_status`, `local_webrtc_peer_close_detaches_terminal_subscriptions`, and `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup`. Red-on-revert is carried by the existing ablation tests (`unix_eof_*_ablation_*`).

`ownership_identity`: `(session_id, subscription_id, generation)`; stale-generation closes do not sweep a replacement owner (`stale_generation_close_does_not_sweep_replacement_owner`, both transports).

`sibling_fail_closed_policy`: successful close keeps siblings working. Ultimate local WebRTC close failure sacrifices every peer on the dedicated runtime and sweeps all owned state ([[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]); Unix connections are unaffected. This run does not change that policy.

## 13. Vault gaps worth capturing

- The final Core, Hub Rust, Lua, client, and transport ownership statement as one note replacing the proposal-era slices (capture in 6.C.5).
- "Hub lifecycle suite lacks a slow-open-route sibling progress oracle" until D.1 lands; then capture the oracle shape.
- The reference runner is unregistered; every plan that names `botster-ubuntu-24.04-16core` must check `gh api .../actions/runners` first.
- The legacy two-arm observation is blocked by GitHub sign-in and harness storage-state input, not only by checkout cleanliness; record the full prerequisite list before proposing option B again.
- Hub Core roll literal count is now 18 active sites plus 6 lock sources; the vault note title still says eleven.
