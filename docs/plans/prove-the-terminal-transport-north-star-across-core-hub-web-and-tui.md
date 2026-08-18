# Plan: Prove the terminal transport north star across Core, Hub, Web, and TUI

Ticket: `ticket_1786661010_115885`
Run: `run_1786867245_870799`
Step: `botster_stack_plan`
Pipeline: `botster_stack_delivery` (direct merge, no PR)
Plan **revision 7** after Implement `run_step_1786907374_480353` parked at `7c055f5` and a human request to return to Plan.

Implement status: All registered parent dependencies are closed. Hub main `a700916` is merged at worktree `a9e5639`. Core pin is `302c7f7`. Controlled `script/run-lifecycle-suite` is `verdict=clean` (253/0/1). Final-worktree `./test.sh --locked` is `TEST_SH_EXIT:0`. Ready to merge directly to Hub `main`.

Satisfied after rev 7 (do not re-wait):

| Ticket | Status | Evidence |
| --- | --- | --- |
| `ticket_1786912123_916503` Web cancel oracle | closed | registered dependency closed |
| `ticket_1786912267_788084` TUI IsolatedHub session-types | closed | registered dependency closed |
| `ticket_1786913892_208903` WebRTC write-budget sibling | closed | registered dependency closed |
| `ticket_1786937228_425608` Unix unbound printf attach lifecycle flake | closed | Hub `origin/main` contains `c1ce7e525aef080e10eee79a306482d5bfc66860` (`Merge ticket: Hub tests: fix unix adapter unbound printf lifecycle flake`). Dependency `dependency_1787003379_762765` is closed. |
| `ticket_1787015956_494734` Core ProcessExited without worker-reap gate | closed | Core `origin/main` contains `d981bb03f91e2d13428000ac989c50d794f659b2`. Not a direct parent dependency; Hub `ticket_1786977409_499180` owns consume + verify of this revision (`dependency_1787015963_708930` already closed). |
| `ticket_1786938984_190098` session projection before ready Spawn | closed | Hub `origin/main` contains `bf249af40b7c9462ec5dba1279086590b20af18c` (`Merge ticket: Hub: complete session projection before ready Spawn snapshot delivery`). Dependency `dependency_1787003375_571466` is closed. |
| `ticket_1787011770_110683` lifecycle-suite harness isolation | closed | Hub `origin/main` contains `952032ddc6b824211f62c077267ff33287565cad` (`Serialize latch fixtures under the daemon test guard.`). Dependency `dependency_1787013165_736482` is closed. Do **not** start the clean-host lifecycle suite from this parent until ShutdownSession and its Core wait merge. |

This is a Hub-owned **final integration proof**. It does not invent a second transport path. It proves the architecture already merged by the closed north-star tickets through production binaries and authentic client harnesses.

## Parked Implement (rev 7)

Human requested return to Plan. Do **not** re-author the Hub coordinator or IsolatedHub occupancy work.

| Field | Value |
| --- | --- |
| Parked commit | `7c055f57f8f875d4d1cc9437ebb65cec20b7b6a3` |
| Hub-owned delivered | `script/prove-north-star-shared-session`; IsolatedHub Unix+WebRTC occupancy oracle; bootstrap-wait helper; ShutdownSession hold test |
| Suite after last Hub change | three consecutive `./test.sh --locked --test hub_daemon_lifecycle_test` at 220 passed / 1 ignored |
| Merge | **not** on `main` |
| Authentic same-session | **not done.** Coordinator spawned `north-star-shared` with `session_type_id=device/north-star-shared`. Web keep-alive failed: expected exactly one cancel detach, got 2 (standalone Web smoke later got 0). TUI IsolatedHub `session-types` failed: created agent type missing from entity store. Required coordinator pass lines were not printed. |

Next Implement, after ShutdownSession and its remaining Core dependency merge. Do **not** start the clean-host lifecycle suite yet.

1. Rebase this branch onto current Hub `origin/main` (must contain `7c055f5`, write-budget, `c1ce7e5`, `bf249af`, and `952032d`).
2. Re-resolve Web and TUI `origin/main` to the closed ticket merges.
3. Run `script/prove-north-star-shared-session` twice with the shipped Web/TUI entry points. Require every coordinator pass line.
4. Run TUI `session-types` against the same binaries.
5. Merge to Hub `main`. No PR.

## Plan Review corrections (rev 7)

| Finding | Class | Fix |
| --- | --- | --- |
| Human return to Plan after Implement | process | Record parked commit `7c055f5`. Authentic proof waits on three registered owner tickets. Do not broaden this Hub run into Web/TUI source. |
| `finding_1786907173_388175` every suite fail routed to WebRTC bootstrap repair | product / medium | Still resolved. Split disposition kept. Named test did not recur in the three baseline runs. A later sibling bootstrap miss was repaired by waiting for health + `local_url`. |
| `finding_1786906844_938668` lifecycle flake has no unconditional disposition | product / high | Still locked: three consecutive default-concurrency suite runs. Refined by the split above. |
| `finding_1786905978_326070` coordinator optional, unnamed, sequential | product / high | Still resolved: `script/prove-north-star-shared-session`. |
| `finding_1786905978_942337` current-main lifecycle suite fails | product / high | Superseded by `finding_1786906844_938668`. Worktree is on `c72712e`. Review later saw 219/1 ignored on the same SHA. That later pass does not close the flake. |
| `finding_1786905978_910956` missing occupancy/shared-client notes | process / low | Still resolved. |
| `finding_1786868395_783448` same-session was conditional | product / high | Still resolved. |
| `question_1786867995_904640` protocol-pin meaning | product / locked | Unchanged. |

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn-target name | `botster-hub` |
| Authoritative path | spawn target `botster-hub` |
| Plan worktree | this pipeline worktree, rebased onto `origin/main` `c72712e` before this revision |
| Worktree hygiene | tracked `.gitignore` has content matching HEAD (53 bytes); path has no `:`; no `CARGO_TARGET_DIR` override |
| Merge policy | direct into `main`; do not create a PR |
| Session-type eligibility consumer | **true** |
| `teardown_class_applies` | **yes** |

Independent resolution: `project_pipelines_current_context` ticket/run `target_id` plus `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub`. Routing did not use the process working directory.

Project `project_1786660949_205223` now has this Hub ticket as the last open north-star ticket. TUI and Web caller-owned harness tickets are **closed**. Previously closed siblings remain closed.

## Repository playbook loaded

[[botster-hub-playbook]]

## Other role/surface playbooks and atomic notes loaded

Role / stack:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]] — planner Must Load only. This run does not edit React/SPA source.
- [[project pipeline orchestration belongs in a device-level botster plugin]]
- [[project pipelines needs an operator workbench not more primitives]]
- [[project pipelines ui contract belongs in the plugin readme]]
- [[botster orchestration should spawn agents with explicit target ids]]
- [[botster orchestration prompts must bind agents to explicit worktrees]]
- [[botster pipeline needs continuous product owner between agent steps]]
- [[prefer framework and library components over custom solutions]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should cite vault notes by wikilink not home path]]
- [[vault example paths are not repository placement conventions]]
- [[plan steps need reviewable plan artifacts]]
- [[cross repo dependency registration must use dependency repo target]]
- [[colon worktree paths break cargo dyld library paths]]
- [[hearth gate runs require restoring a pipeline wiped gitignore before attribution]]

In-repo client overlay (Hub member crate; not a separate run):

- [[botster hub client crate is the external client boundary]]

Runtime-teardown class applies (WebRTC/peer lifecycle, SessionIo/ClientWorker teardown, connection-loss vs host-session survival, terminal-state vs live-runtime):

- [[botster runtime teardown lenses]]

Not loaded, with reason:

- [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope
- [[botster-core-playbook]], [[botster-web-playbook]], [[botster-tui-playbook]], [[botster-tui-kit-playbook]] — consulted only as ownership seams; this run's implementation charter is Hub
- other repository charters as this run's implementation charter

Targeted notes:

- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[Hub embeds CoreDaemon behind one client admission point]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[Core owns the incremental attach phase machine]]
- [[Core reports terminal mechanism capabilities and Hub admits their use]]
- [[Core ClientWorker bind requires a live attach generation]]
- [[Core subscription hard-stop is synchronous close and drop on the host tick]]
- [[Core terminal subscription ownership is session, subscription, and generation]]
- [[Hub owner loop calls bounded Core lifecycle page APIs]]
- [[Core control-plane lifecycle journal advances without a terminal client or Hub terminal Drain]]
- [[Hub session projection continues without subscribers or terminal Drain]]
- [[host ShutdownSession classification must call the exact-session Core query]]
- [[observe-first attached Drain can return SessionLifecycle without ProcessExit]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[Hub bee15e7 builds the session worker from botster-core-daemon]]
- [[first-party clients put terminal mechanism tokens only in terminal compatibility]]
- [[Git-consumed Hub members pin Core protocol by exact revision]]
- [[Hub test support copies Core protocol fixtures from the pinned crate source]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[cold turkey migrations eliminate dual code paths and version suffixes]]
- [[cold cut grep gates exclude rejection tests that name retired inputs]]
- [[a known positive control proves a scan is live not that its pattern set is complete]]
- [[hub qualifies effective session type ids as source name slash id]]
- [[host-plane session_type deltas use per-subscriber contiguous snapshot_seq]]
- [[incomplete repo local session types drop the hub client connection]]
- [[Unix Hello can reject terminal admission while host operations remain available]]
- [[Unix mux host events are unsolicited control frames]]
- [[host reconciliation must not rewrite a completed Core adapter close reason]]
- [[WebRTC DataChannel local close uses the peer close bound before cleanup]]
- [[a ready WebRTC send must win over a queued DataChannel close]]
- [[webrtc peer cleanup removes every per peer owner together]]
- [[terminal webrtc failure records do not prove peer runtime teardown]]
- [[mux envelope delivery does not prove Hub route ownership]]
- [[PeerClosed attach occupancy must use the live attach route set]]
- [[sessionio hard socket death must fan process exited to clientworkers]]
- [[hub shutdown preserves durable session workers]]
- [[pre READY attach failure creates no attach ownership]]
- [[session shutdown during attach does not produce attach failed]]
- [[a page reload is not a reconnect]]
- [[transport ownership north star for modular Botster is proposed]]
- [[proposed Hub terminal tests enforce content blind adapters]]
- [[TUI live Ghostty has IsolatedHub ghostty plus attach-only ghostty-shared and ghostty-shared-exit]]
- [[the packaged-protocol terminal lane has a caller-owned keep-alive mode]]
- [[a public occupancy oracle must union Hub routes with Core inventory]]
- [[live attach counters and omitted occupancy fields are not identity oracles]]
- [[test script required for rust tests not cargo test]]
- [[rust repo strict lints must be verified before dismissing warnings]]

## Context loaded

Human returned this run to Plan after Implement. Parked Hub commit `7c055f5`. Implement report: `docs/reports/prove-the-terminal-transport-north-star-across-core-hub-web-and-tui-implement.md`. Authentic coordinator failed on Web cancel detach count (2, later 0) and TUI IsolatedHub session-types. Three owner tickets are open and registered. `question_1786867995_904640` stays locked.

This worktree is on Hub `origin/main` `c72712e`. Two Review suite observations on that SHA:

- Earlier: 218 passed, 1 failed, 1 ignored. Root: `webrtc_terminal_adapter_stale_generation_close_does_not_sweep_replacement_owner` panicked (bootstrap lacked the local WebRTC response). Isolated rerun passed 1/1.
- Later (`review_1786906844_485724`): 219 passed, 1 ignored.

One later pass does not resolve a load-sensitive failure. Disposition is the three-run default-concurrency gate below, not a promised repair.

Project `Botster Terminal Transport North Star` completion rule: every ticket merges directly into main; Unix and WebRTC use the same Core adapter contract; Web and TUI pass authentic shared terminal proofs; Hub cannot inspect terminal bodies; terminal capability changes do not require Hub or TUI Kit Git repins; no PR.

Closed dependencies used as given:

| Ticket | Title | Status |
| --- | --- | --- |
| `ticket_1786661010_198387` | Hub: cold-cut terminal drains and translation from the production path | closed |
| `ticket_1786664495_777899` | Hub: delete Hub-owned terminal goldens and consume Core protocol fixtures | closed |
| `ticket_1786841413_921609` | Hub: preserve held session-type subscription through CRUD | closed |

Reviewed-main snapshot at Plan rev 4 (Implement re-resolves `origin/main` before proof):

| Repo | Spawn target | SHA | Role in this proof |
| --- | --- | --- | --- |
| Hub | `tgt_7e208a0c76a44980a83b63af976b1f22` | `c72712e2606b8abe77e1b91c2a736791036fadd8` | host + `attach_occupancy`; this worktree is on this SHA |
| Core | `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` | `fc541a59338d0591ba4fb3fa522a030d212d26d0` | locked worker source |
| Web | `tgt_40abcf71ccf049f4ac0c99953a799869` | `ebb6677902ff5920ebb75685a74bba30b9b81b87` | `drive:live-packaged-protocol:shared-session` |
| TUI | `tgt_c3d470bab78549df920a41e8fb0e58d8` | `8b4df69e27b65071aa94b7e5d6b31d0990c041fc` | `script/test-live-hub ghostty-shared` / `ghostty-shared-exit` |
| TUI Kit | `tgt_3dfae49c02454037bf13554f552baf7f` | `c83ba6c518e2324e34ce24c7abe5a8a05e56293c` | UI-contract-only pin |

Hub lock and member manifests already pin Core protocol crates at `fc541a59`. Protocol stays 7. In-tree `@trybotster/hub-test-support` is `0.1.37` / conformance 42. Published Web pin is `@trybotster/hub-test-support@0.1.36` plus `@trybotster/terminal-protocol@0.1.0`. TUI pins `botster-terminal-protocol-client` to Core `f4f6bf5` and host DTOs to Hub Git `4f30d695`. TUI Kit pins UI contract tag `botster-ui-contract-v0.3.2` only.

Production Hub already:

- binds Unix and WebRTC content-blind adapters and runs `assert_terminal_adapter_conformance` in both adapter modules
- scans production sources for Drain helpers, two-argument Core drain, and `READY`/`PAGE`/`FINISH`/`GHOSTSNP` plus terminal `DaemonEvent` construction
- classifies `ShutdownSession` through `observe_session_lifecycle`
- keeps `HubRuntime::drain_subscription` / `drain_runtime_once` behind `cfg(test)`
- consumes Core-owned late-attach fixture bytes through hub-test-support `build.rs`

`stream_attach` in `botster-hub-client` is Attach + host `ReadScreen`. It is not a terminal Drain.

## Botster layers touched

- Hub proof/audit: production scan, adapter conformance, IsolatedHub dual-attach, owner-loop teardown
- In-repo hub-client / hub-test-support only if the audit finds a missing Hub-owned oracle
- Downstream **execution** of the merged caller-owned Web and TUI shared-session harnesses against one Hub-spawned session
- Docs/report evidence
- Not: Core implementation, Web/TUI/TUI Kit source in this worktree, Project Pipelines plugin, npm publish unless the pin audit proves a published host DTO is required

## Worktree / target assumptions

- Implement stays in this Hub worktree and rebases onto current Hub `origin/main` before proof.
- Do not infer other repositories from this worktree. Downstream harnesses run from their spawn-target checkouts.
- Merge directly into `main`. Do not create a PR.

## Product decision ledger

| Decision | Choice |
| --- | --- |
| Owner of this run | Hub only. Register any Core/Web/TUI/TUI Kit defect against that repo's `target_id`. |
| Reviewed mains | Re-resolve each `origin/main` at Implement start. The Plan snapshot is the floor, not a frozen substitute if main moved. |
| Session-type consumer | Yes. Live proof must go through `list_session_types_for_target` + spawn Option A. Do not filter by client `target_id` equality. Parent floor is Hub `804dde7` / hub-test-support `0.1.26` / conf 33. Current Hub is already above that floor. |
| Client Hub Git pins | Locked by `question_1786867995_904640`. Terminal compatibility is `TerminalCompatibilityRequirement` + protocol version + feature tokens. TUI Cargo Git pins for `botster-hub-client` / `botster-hub-test-support` are host-control build provenance, not protocol identity. Fail if terminal acceptance compares or derives behavior from a Hub commit SHA. Do not publish or tag only to remove those pins. |
| Same live session | **Mandatory.** One Hub-spawned session identity `north-star-shared` (or the coordinator-printed id). Authentic Web and authentic TUI Ghostty both attach to that session. IsolatedHub Unix+WebRTC dual-attach remains a Hub transport oracle and is **not** the ticket acceptance. Existing isolated Web smoke and `script/test-live-hub ghostty` are **not** sufficient. |
| Connection loss | One effective detach. Host session stays alive. Prove Unix peer/socket loss and WebRTC peer loss separately from explicit Detach and from `ShutdownSession`. |
| Hub-test-support publish | Do not publish `0.1.37` in this ticket unless live Web Hello against current Hub fails closed on the published `0.1.36` pin. Additive host capabilities must not raise the default client requirement. |
| North-star vault state | [[transport ownership north star for modular Botster is proposed]] and [[proposed Hub terminal tests enforce content blind adapters]] stay proposed until this proof lands. Capture ratification after Verify, not during Plan. |
| Harness updates | Add the smallest Hub-owned test or script that the existing suite cannot prove. Do not add a parallel production path, runtime flag, or dual drain. |
| Docs | Path from Hub prior art: `docs/plans/**` and `docs/reports/**`. |

## Scope

1. **Re-resolve pins.** Record Hub HEAD, lockfile Core SHA, Web `origin/main`, TUI `origin/main`, TUI Kit `origin/main`. Fail if Hub lock Core != Core `origin/main` unless a registered Core ticket explains the lag.
2. **Audit Hub production path.** Re-run the production-source scan and a cargo-tree / source audit for:
   - production terminal Drain (`drain_subscription`, `drain_runtime_once`, `.drain(session_id`)
   - `ATTACH_DRAIN_INTERVAL` or fixed attach-poll sleeps
   - Hub-owned READY/PAGE/FINISH/GHOSTSNP body inspection
   - `botster-terminal-protocol-client` as a Hub or hub-client direct dependency
   - Hub-owned late-attach goldens or `generate_incremental_late_attach_frames.rs`
   - leftover runtime fallback / dual production path
3. **Shared adapter conformance.** Run the Core harness through both production adapters:
   - `production_unix_adapter_passes_core_conformance_harness`
   - the matching WebRTC adapter conformance test
   Same `assert_terminal_adapter_conformance` suite, two drivers.
4. **Same-session IsolatedHub transport oracle.** One session, Unix + WebRTC adapters, Hub-visible route-ownership. This proves Hub adapters; it does **not** satisfy authentic Web+TUI acceptance.
5. **Shipped caller-owned entry points (mandatory).** Do not invent a second client path.
   - Web `ebb6677`: `npm run drive:live-packaged-protocol:shared-session` with `BOTSTER_LIVE_DATA_DIR` + `BOTSTER_SHARED_SESSION_ID`. Missing either fails closed. `BOTSTER_LIVE_SHARED_HUB_DRIVER=1` is Workspaces-only and must not be combined. IsolatedHub `npm run smoke:live-packaged-protocol` and Web's standalone `smoke:live-packaged-protocol:shared-session` coordinator are **not** the dual-client proof (the latter starts its own Hub and never attaches TUI).
   - TUI `8b4df69`: `script/test-live-hub ghostty-shared` and `ghostty-shared-exit`. Require only `BOTSTER_HUB_CONNECTION` + `BOTSTER_SHARED_SESSION_ID`. Do **not** set `BOTSTER_HUB_BIN` / `BOTSTER_SESSION_WORKER_BIN`. IsolatedHub `script/test-live-hub ghostty` is not a substitute. `ghostty-shared` Hello requires advertised `attach_occupancy`; empty `Status.live_attach_occupancy` without that token is not release proof.
6. **Mandatory authentic same-session proof.** Hub-owned coordinator on Hub `c72712e` starts one provenance-pinned Hub, enables `botster-web`, lists session types for the admitted spawn point (`list_session_types_for_target` + Option A), spawns **one** session id `north-star-shared` with a producer that prints `NORTH_STAR_HISTORY` before the live read-loop (TUI late-attach) and then the Web production echo/bytes/resize/exit contract. Then attach both shipped clients to that id. Connection loss of one client is one detach and must not `ShutdownSession`.
7. **Pin-graph proof.** Web pins `@trybotster/terminal-protocol` by package version. TUI terminal tokens come from `botster-terminal-protocol-client`. Host `DaemonCompatibility` and Core `TerminalCompatibility` stay separate. TUI Kit has no Hub Git pin. Terminal acceptance must not compare a Hub commit SHA.
8. **Teardown proof.** Drive production close handlers (Unix mux loss, WebRTC peer loss, explicit Detach, `ShutdownSession`, Hub stop). Prove one detach, sibling survival, and worker idle / adapter drop. Terminal JSON alone is not enough.
9. **Evidence + merge.** Write `docs/reports/prove-the-terminal-transport-north-star-across-core-hub-web-and-tui-implement.md`. Merge directly to Hub `main`. No PR.

## Non-scope

- Re-implementing the cold-cut, fixture deletion, or session-type CRUD tickets
- Editing `botster-core`, `botster-web`, `botster-tui`, or `botster-tui-kit` in this worktree
- Restoring Hub-owned GHOSTSNP goldens or a Hub attach-phase machine
- Publishing npm unless live Web Hello fails on the published pin
- Raising `PROTOCOL_VERSION` or mutating published hub-test-support `0.1.36`
- Project Pipelines package/plugin work
- Ratifying vault notes during Plan
- Dual production paths, feature flags, or optional Drain fallbacks

## Repository ownership boundaries and cross-repo dependencies

| Surface | Owner | This run |
| --- | --- | --- |
| Host admission, adapters, route registry, owner loop, host Drain, `ShutdownSession` | Hub | audit + proof + any missing Hub oracle |
| Terminal frames, attach generations, adapter trait, conformance harness, exact-session lifecycle query | Core | consume locked `fc541a59` or newer recorded Core main |
| Browser Restty decode, packaged-protocol smoke, session-entity detach | Web | Caller-owned lane shipped at `ebb6677`. **Open** `ticket_1786912123_916503` must make cancel emit exactly one detach. |
| Ghostty decode, TUI live attach, session-types live profile | TUI | `ghostty-shared` shipped at `8b4df69`. **Open** `ticket_1786912267_788084` must fix IsolatedHub `session-types` entity-store miss. |
| Ratatui/Crossterm kit | TUI Kit | pin check only |
| Host DTOs / generated TS | in-repo `botster-hub-client` | audit only unless a Hub-owned oracle is missing |

Already-closed dependencies stay closed.

Rev 2 caller-owned harness tickets remain **closed**.

Open blocking dependencies (already registered; do not re-register). Final integration stays parked until this harness taint is repaired. Do **not** waive `script/run-lifecycle-suite`.

| Ticket | Target | Target id | Why |
| --- | --- | --- | --- |
| `ticket_1787076374_645547` | botster-hub | `tgt_7e208a0c76a44980a83b63af976b1f22` | `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable` identity capture taints the default-concurrency lifecycle suite (`verdict=environment_tainted` twice on a 0/0 census) |

Closed after rev 7, including harness `ticket_1787011770_110683` at Hub `952032d`, projection `ticket_1786938984_190098` at Hub `bf249af`, and Unix-attach `ticket_1786937228_425608` at Hub `c1ce7e5`. This parent Implement fail-closes while ShutdownSession is open. After it closes, rebase onto current Hub `origin/main` (include `7c055f5`, write-budget, `c1ce7e5`, `bf249af`, and `952032d`) and run the coordinator twice.

If Implement finds a further defect owned elsewhere, register a new ticket against that repository's `target_id`:

- Core `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`
- Web `tgt_40abcf71ccf049f4ac0c99953a799869`
- TUI `tgt_c3d470bab78549df920a41e8fb0e58d8`
- TUI Kit `tgt_3dfae49c02454037bf13554f552baf7f`

Never register a cross-repo fix against this Hub `target_id`.

## Assumptions and unknowns

Assumptions:

- Closed north-star tickets remain merged on each repo's `origin/main` at Implement start.
- Web `30d961cd` session-entity detach and TUI `fc1ff623` terminal-token split remain the authentic client floors.
- Host `ReadScreen` is control-plane, not terminal Drain.
- `hub-test-support@0.1.37` may remain unpublished; Web `0.1.36` Hello against protocol 7 / advertised revision 42 is the expected live path.
- TUI Core protocol pin `f4f6bf5` can still attach to Hub-locked Core `fc541a59`. If live TUI fails on that lag, that is a TUI pin ticket, not a Hub Git terminal-compatibility pin.
- Shared session id is `north-star-shared` unless the coordinator prints a different exact id. Both clients must use that printed id.
- Shared producer is Hub-owned: print `NORTH_STAR_HISTORY` before the first TUI attach, then the Web production read-loop (echo / bytes / resize / `botster-web-production-exit`). Web and TUI do not each spawn a private producer.
- Shipped flags are now known: Web `BOTSTER_LIVE_DATA_DIR` + `BOTSTER_SHARED_SESSION_ID` + `drive:live-packaged-protocol:shared-session`; TUI `BOTSTER_HUB_CONNECTION` + `BOTSTER_SHARED_SESSION_ID` + `ghostty-shared` / `ghostty-shared-exit`.

Unknowns Implement must resolve with evidence, not guesswork:

- Whether a Hub IsolatedHub test already covers Unix+WebRTC on one session with the transport matrix, or a new focused test is required
- Whether current disk headroom allows `./test.sh --locked` plus the same-session live proof
- Whether TUI Ghostty submodule / Zig 0.16 is present on the machine that runs Verify
- Whether any of the three required default-concurrency suite runs reproduces `webrtc_terminal_adapter_stale_generation_close_does_not_sweep_replacement_owner`. If none do, no bootstrap repair ships. If that named test fails, bootstrap repair is required. Other failures are diagnosed separately.

## Affected surfaces/files

Expected Hub-owned writes:

- `docs/plans/prove-the-terminal-transport-north-star-across-core-hub-web-and-tui.md` (this plan)
- `docs/reports/prove-the-terminal-transport-north-star-across-core-hub-web-and-tui-implement.md`
- `script/prove-north-star-shared-session` — **unconditional**. Named production-path coordinator. Not optional.

Hub-owned WebRTC bootstrap repair **only if** the named test `webrtc_terminal_adapter_stale_generation_close_does_not_sweep_replacement_owner` fails in any of the three required default-concurrency suite runs:

- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` and the production WebRTC bootstrap path that `start_webrtc_adapter_hub` / `issue_second_webrtc_bootstrap` exercise under suite load

Any other lifecycle-suite failure still blocks merge. Diagnose the exact test. Repair it in this Hub ticket if it is in scope, or register a blocking ticket against the owning repository target. Then run three more consecutive default-concurrency suite passes.

Only if the audit finds a missing Hub-owned oracle:

- other files under `tests/hub_daemon_lifecycle/`
- `src/lib.rs` production-scan known-positives only if a new forbidden construct is required
- `README.md` / `docs/client-protocol.md` only to record the proved one-path state, not to describe a new path

Do not touch production adapter write/close policy unless the audit finds a leftover Drain or body inspect, or the suite-wide WebRTC bootstrap repair requires it.

## Coordinator contract (`script/prove-north-star-shared-session`)

Unconditional Implement deliverable. Hub origin/main does not have this file yet.

**Command**

```sh
BOTSTER_HUB_BIN=<hub-bin> \
BOTSTER_SESSION_WORKER_BIN=<worker-bin> \
BOTSTER_WEB_CHECKOUT=<web-spawn-target-at-ebb6677-or-newer> \
BOTSTER_TUI_CHECKOUT=<tui-spawn-target-at-8b4df69-or-newer> \
BOTSTER_SHARED_SESSION_ID=north-star-shared \
  script/prove-north-star-shared-session
```

Missing any required env fails closed. Do not call Web `smoke:live-packaged-protocol:shared-session` (that script starts its own Hub and never attaches TUI).

**Barriers** (each step waits for the listed marker or Status lifecycle; timeout 900s per client pass, 2700s overall):

| Step | Action | Ready / pass line | Fail-closed if |
| --- | --- | --- | --- |
| 1 | start Hub; enable `botster-web`; `list_session_types_for_target` + Option A spawn | `north-star-shared-spawned session_id=north-star-shared` | spawn error or session not `running` |
| 2 | Web keep-alive 1 | `live-shared-session-keep-alive-passed` | session not `running` after |
| 3 | Web keep-alive 2 | `live-shared-session-keep-alive-passed` | session not `running` after |
| 4 | TUI `ghostty-shared` (no Hub/worker bin env) | `ghostty-shared-complete` | session not `running`; or TUI pair still in `live_attach_occupancy` after socket cut |
| 5 | TUI `ghostty-shared-exit` (stdout line-buffered) | `ghostty-shared-exit-attached` | marker not seen before timeout (do not wait for process exit) |
| 6 | Web `BOTSTER_SHARED_SESSION_PROVE_EXIT=1` | `live-shared-session-exit-passed` | `ShutdownSession` observed |
| 7 | TUI observes caller-ended session | `ghostty-shared-exit-complete` | TUI sent `ShutdownSession` |
| 8 | coordinator cleanup | `north-star-shared-session-complete` | Hub leftover PID after stop |

**Exit ownership:** only the Web exit pass sends the documented producer exit command. TUI never `ShutdownSession`. Coordinator `down`/`stop` is after both exit markers.

**Connection loss (ticket-level, not simultaneous dual-hold):**

- Web keep-alive already proves in-page DataChannel close ([[a page reload is not a reconnect]]). After that pass, Status must still show session `running`.
- TUI `ghostty-shared` already cuts the TUI socket and proves exact-pair release through `DaemonStatus.live_attach_occupancy` after requiring `attach_occupancy` ([[TUI live Ghostty has IsolatedHub ghostty plus attach-only ghostty-shared and ghostty-shared-exit]], [[a public occupancy oracle must union Hub routes with Core inventory]], [[live attach counters and omitted occupancy fields are not identity oracles]]). After that pass, Status must still show session `running`.
- Do **not** claim Web remains attached during the TUI cut, or TUI remains attached during the Web DataChannel close. The ticket requires one effective detach and no host session shutdown. The shipped keep-alive / socket-cut profiles already prove that per client on the same session.

**Cleanup:** stop Hub; remove the coordinator data dir unless `BOTSTER_LIVE_DATA_DIR` was supplied. Print all pass lines to stdout unbuffered.

## Risks

- Live Web packaged-protocol remaining flakes (alternate-screen cycle 0, `waitForTerminalDetached`) hide a real Hub defect or a Web-owned detach gap
- TUI Ghostty live proof needs Zig 0.16 and the vendored Ghostty submodule inside the resolved `botster-terminal-ghostty` checkout
- Disk exhaustion (`errno 28`) is not a passing baseline
- IsolatedHub adapter conformance is not by itself authentic client proof
- Mux envelope delivery is not a Hub route-ownership oracle
- Observe-first lifecycle can consume `ProcessExited` so Web/TUI must accept session-entity exit as the host detach oracle
- Treating TUI's host-DTO Hub Git pin as a terminal-protocol pin would create a false fail (`question_1786867995_904640`)
- Publishing `0.1.37` without a Hello failure would mutate the release chain for no product reason
- Using Web `smoke:live-packaged-protocol:shared-session` (Web-owned coordinator) instead of Hub-owned dual-client attach recreates `finding_1786868395_783448`
- Using IsolatedHub `ghostty` or IsolatedHub `web-prod` as the same-session proof recreates `finding_1786868395_783448`
- Suite-wide `webrtc_terminal_adapter_stale_generation_close_does_not_sweep_replacement_owner` on `c72712e` is a load-sensitive base failure; isolated pass is not a waiver
- Treating Web stay-attached-during-TUI-cut as required would force a new hold-open harness the ticket does not ask for

## Runtime-teardown answers

| Field | Answer |
| --- | --- |
| `teardown_class_applies` | yes — connection-loss detach, WebRTC peer close, Unix mux loss, multi-client attach to one session, and host-session survival vs terminal-subscription death |
| `teardown_isolation` | One failed peer/subscription dies with its adapter, route row, and attach generation. Sibling attaches on the same session keep their generation. Host session and session worker survive peer loss. Hub process stop preserves durable workers. |
| `teardown_bounds` | Adapter `close()` is non-blocking on the host tick ([[Core subscription hard-stop is synchronous close and drop on the host tick]]). WebRTC local DataChannel close uses the peer close bound, then cleanup still runs. Unbounded `block_on(close)` on the Hub control plane is a reject. |
| `late_message_matrix` | See table below. |
| `production_path_proof` | Unix: socket/peer EOF → mux reader → forget route → adapter close/drop → idle. WebRTC: channel/peer close → peer cleanup set (channel, send task, runner, ping, ICE) → route forget → adapter close/drop → idle. Oracles: live IsolatedHub tests plus authentic Web DataChannel close (not page reload) and TUI subscription-closed recovery. Thread/runtime idle or join, not only a terminal JSON file. |
| `ownership_identity` | Core identity is session + subscription + generation. Hub route rows use that triple. Stale PeerClosed / late Attach must not detach a replacement owner with a reused subscription id. |
| `sibling_fail_closed_policy` | Successful close: siblings keep working. Ultimate close failure: bound the blast radius to the affected subscription/peer; do not shut the host session; fail the test if a sibling attach dies or the session worker is torn down. |

### Late-message matrix

| Message | Tag / owner | Reject after terminal failure | Sweep if it races PeerClosed |
| --- | --- | --- | --- |
| Attach | grant + live attach generation | fail-closed; no bind without live generation; no occupancy increment | stale Attach must not recreate a closed route or steal a replacement generation |
| Detach | session + subscription | idempotent; second Detach is a no-op | Detach after PeerClosed does not decrement a replacement route |
| Drain (host) | control-plane only | host Drain returns no terminal bodies | never used to infer detach or shutdown |
| SubscribeEntities | grant + subscription id | rejected after peer/session revoke | residual rows swept with the peer owner set |
| UnsubscribeEntities | same subscription id | idempotent | cannot unsubscribe a replacement owner |
| Hello / admission | connection / peer | terminal Hello reject leaves host ops available on Unix | failed Hello does not leave an adapter |
| Input / resize | bound generation | rejected after close; no Hub retry | late input after close is dropped |
| Peer / socket loss | peer id | one effective detach; no `ShutdownSession` | ownership sweep uses live attach route set |
| ShutdownSession | host session policy + exact Core query | `Found` / `Absent` / `Err` preserved; no Drain classify | does not run on connection loss |
| Inventory reconcile | host route registry vs Core subscription inventory | control-plane only; no terminal-silence inference | cannot rewrite a completed Core adapter close reason |

## Acceptance checks/tests

Hub (this worktree, after hygiene and disk check):

```sh
git checkout HEAD -- .gitignore   # only if tracked file is empty/missing
# three consecutive default-concurrency runs; record all three
./test.sh --locked --test hub_daemon_lifecycle_test
./test.sh --locked --test hub_daemon_lifecycle_test
./test.sh --locked --test hub_daemon_lifecycle_test
./test.sh --locked --test hub_client_api_test
./test.sh --locked --test hub_test_support_conformance_test
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo tree -e normal -p botster-hub --depth 1
cargo tree -e normal -p botster-hub-client --depth 1
cargo tree -e normal -p botster-hub-test-support --depth 1
```

Named bootstrap-test failure requires the production WebRTC bootstrap repair, then three more consecutive suite passes. Any other suite failure still blocks merge and needs exact diagnosis plus in-scope repair or a correctly targeted blocking ticket, then three more consecutive passes. Isolated `--exact webrtc_terminal_adapter_stale_generation_close_does_not_sweep_replacement_owner` does not satisfy the three-run gate.

Required Hub oracles (existing tests or the smallest new ones):

- `production_unix_adapter_passes_core_conformance_harness`
- matching WebRTC `assert_terminal_adapter_conformance` driver
- `production_sources_reject_terminal_drain_and_snapshot_phase_decode` plus known-positives
- Unix and WebRTC connection-loss close without host session shutdown
- explicit Detach ≠ peer loss ≠ `ShutdownSession`
- one-session Unix+WebRTC dual-attach with Hub-visible occupancy
- `observe_session_lifecycle` is the only `ShutdownSession` classify path

Provenance-pinned live binaries:

```sh
cargo build --locked -p botster-hub
cargo build --locked -p botster-core-daemon --bin botster-session-worker
```

Record Hub SHA, lockfile Core SHA, and realpaths under the fresh target dir ([[live hub proof records distinct hub and locked core binary provenance]], [[Hub bee15e7 builds the session worker from botster-core-daemon]]).

Mandatory same-session proof. Run the named coordinator twice:

```sh
BOTSTER_HUB_BIN=<hub-bin> \
BOTSTER_SESSION_WORKER_BIN=<worker-bin> \
BOTSTER_WEB_CHECKOUT=<web-ebb6677+> \
BOTSTER_TUI_CHECKOUT=<tui-8b4df69+> \
BOTSTER_SHARED_SESSION_ID=north-star-shared \
  script/prove-north-star-shared-session
```

Both runs must print `north-star-shared-spawned`, two `live-shared-session-keep-alive-passed`, `ghostty-shared-complete`, `ghostty-shared-exit-attached`, `live-shared-session-exit-passed`, `ghostty-shared-exit-complete`, and `north-star-shared-session-complete`. IsolatedHub `web-prod`, IsolatedHub `ghostty`, and Web `smoke:live-packaged-protocol:shared-session` are not this proof.

Same-session oracles. Both clients must report the **same** `session_id`. Isolated Web smoke and isolated `ghostty` are not substitutes.

| Oracle | Shared session evidence | Web | TUI |
| --- | --- | --- | --- |
| Identity | printed `north-star-shared` (or coordinator id) | attach that id, not `web-prod` unless it is that id | attach that id, no IsolatedHub spawn |
| Ordering | READY before FINISH / PAGE order on this session | attach chronology | Ghostty chronology |
| Bytes | same exact payloads (including non-UTF8) | Restty / readScreen | Ghostty `decoded_bytes` / viewport |
| History / late-attach | late attach sees retained history from this PTY | history + cycle-0 final-row if the producer emits it | GHOSTSNP history / `HISTORY_HEAD` or equivalent |
| Resize | one resize reflected to both | `botster-web-production-size` or shared marker | Ghostty size / ReadScreen |
| Input | input from each client echoed by the shared producer | echo oracle | echo oracle |
| Cancellation | cancel stops the in-flight attach/input campaign | existing cancel oracle | existing cancel oracle |
| Reconnect | same document / same TUI process; new subscription | DataChannel close, not page reload | subscription-closed recovery, not IsolatedHub restart |
| Exit | ProcessExited or session-entity `exited`/`failed` | `waitForTerminalDetached` | Ghostty / entity exit |
| Connection loss | one effective detach; host session stays `running` | Web keep-alive DataChannel close; no `ShutdownSession` | `ghostty-shared` socket cut; pair absent from `live_attach_occupancy`; no `ShutdownSession` |
| Session types | live, not residual | `exerciseSessionTypes` on this Hub; Option A; no client `target_id` equality filter | `script/test-live-hub session-types` against the same binaries |

Run the same-session coordinator twice. Both runs must print the shared session id and both client pass lines.

Pin-graph assertions:

- Hub and hub-client have no direct `botster-terminal-protocol-client` dependency
- hub-test-support still copies the five Core fixture names and has no Hub-owned golden generator
- Web `package.json` pins `@trybotster/terminal-protocol` and does not pin a Hub Git revision for terminal compatibility
- TUI Kit `Cargo.toml` has no Hub Git revision
- TUI terminal tokens come from `botster-terminal-protocol-client`, not host `required_features`

Downstream proof required by the Hub charter: live Web and live TUI against the exact Hub binary and the lockfile-pinned worker. Suite-only Hub tests are not enough.

## Vault gaps worth capturing

- [[transport ownership north star for modular Botster is proposed]] and [[proposed Hub terminal tests enforce content blind adapters]] still say `ratification_needed`. After this proof merges, capture that the north star is ratified by production-path evidence, not by a second architecture ticket.
- `question_1786867995_904640` already answers the TUI host-DTO Git pin. Capture after Verify only if the shipped proof adds a durable pin-graph rule beyond that answer.
- No new Plan-time capture. The ratification note waits for Implement/Verify evidence.

## Implement sequence

1. If `ticket_1786912123_916503`, `ticket_1786912267_788084`, or `ticket_1786913892_208903` is open, **stop**. Do not edit Web/TUI here. Do not rewrite the parked coordinator.
2. Restore `.gitignore` if wiped. Confirm no `:` in the worktree path. Check free disk.
3. Rebase onto Hub `origin/main` that contains `7c055f5` and the closed write-budget ticket.
4. Re-resolve Web and TUI `origin/main` to the closed cancel-oracle and session-types merges.
5. Build provenance-pinned binaries from the rebased Hub.
6. Run `script/prove-north-star-shared-session` twice. Require every coordinator pass line.
7. Run TUI `session-types` against the same binaries.
8. If the three-run lifecycle suite is no longer current on the rebased SHA, rerun the split disposition from rev 6.
9. Write the implement report and merge to Hub `main`. No PR.

## Review / Verify overlays

- Review: [[botster-runtime-reviewer-playbook]]
- Verify: [[botster-runtime-verifier-playbook]]
- Package overlay only if Implement publishes or mutates hub-test-support
