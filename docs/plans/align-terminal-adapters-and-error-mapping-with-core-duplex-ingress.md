# Align Hub terminal adapters and error mapping with the Core duplex ingress contract

Ticket: `ticket_1788137128_417142` — "Hub: align terminal adapters and error mapping with Core 3672c667"
Run: `run_1788138132_150545`
Pipeline: `botster_stack_delivery`
Base ref: `main` at `c674a62`

## 1. Target and context

| Item | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Repository path | `/Users/jasonconigliari/Projects/botster-hub` |
| Repository playbook | [[botster-hub-playbook]] |
| Current Core pin | `7eafa470a18025895995bbedc20d34b58106a03b` |
| New Core pin | `a781556258789dea4a50ffcb17351e7294c8ff26` |

Repository routing came from the ticket `target_id`, resolved through
`list_spawn_targets`. The process working directory was not used to select the
repository.

### Playbooks and atomic notes loaded

- [[planner-playbook]] — generic Plan-stage role contract.
- [[botster-planner-playbook]] — Botster Plan overlay.
- [[botster-hub-playbook]] — repository ownership charter for `botster-hub`.
- [[botster hub is a first party host profile over core]] — Hub owns trusted
  product policy over policy-free Core.
- [[botster Hub Rust stays a trusted host kernel]] — Hub Rust owns privileged
  boundaries and must not duplicate Core mechanisms.
- [[botster terminal egress is session backed only]] — terminal bytes stay in
  the Core data path, not in the Hub owner loop.
- [[Core reports terminal mechanism capabilities and Hub admits their use]] —
  Core publishes the mechanism, Hub admits it.
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
  — Hub adapters stay content blind.
- [[Hub official gates must not set CARGO TARGET DIR]] — the locked gate uses a
  colon-free worktree and the default `target/` layout.
- [[colon worktree paths break cargo dyld library paths]] — worktree hygiene.
- [[pipeline vault checklists must cite exact resolvable note titles]] — note
  identity discipline for gate evidence.
- [[botster-architecture]] — Botster domain map and source of architectural truth.
- [[cli-patterns]] — Rust CLI, TUI, PTY, and terminal-layer constraints.
- [[project-pipelines-playbook]] — loaded because a registered cross-repository
  dependency governs this seam. See section 5.
- [[each acceptance condition names its authoritative production oracle]] — every
  acceptance proof names its production owner and API. This is the note that
  forced the section 9 correction.
- [[dependency closure must requeue the blocked parent step]] — closing this Hub
  ticket resumes the blocked Core ticket.
- [[core owns duplex terminal transport while Hub stays content blind]] — the
  ratified decision that extends the adapter contract to ingress and retires
  `mode_gated_input` as a Hub JSON-RPC request.
- [[core terminal progress is wake driven and targeted]] — Core advances only
  subscriptions named by adapter or ingress wakes, which is the pump Hub does
  not yet drive.

The runtime-teardown lens note was considered and is answered in section 9.

### Repository context read

- `Cargo.toml`, `crates/botster-hub-client/Cargo.toml`,
  `crates/botster-hub-test-support/Cargo.toml`,
  `crates/botster-hub-test-support/build.rs`, `Cargo.lock`.
- `src/transport/unix/adapter.rs`, `src/transport/webrtc/adapter.rs`,
  `src/transport/shared/adapter_slot.rs`.
- `src/runtime.rs` around `managed_session_core_error_class`.
- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`,
  `tests/session_projection_owner_loop.rs`.
- `test.sh`.
- Prior art: `docs/plans/roll-core-pin-after-incremental-attach-local-runtime-gate.md`,
  which holds the authoritative Core pin-roll literal inventory and its guard test.
- Core source at revisions `7eafa470`, `3672c667`, and `a781556`.

## 2. Problem, established by compile and test evidence

Plan-time evidence, produced in this worktree and then reverted to a clean tree.

1. Rolling every Core literal from `7eafa470` to `3672c667` and running
   `RUSTUP_TOOLCHAIN=1.97.0 cargo check --all-targets` produces exactly six
   distinct compile errors and no others:
   - `E0046` missing `TerminalAdapter::try_read` at `src/transport/unix/adapter.rs:160`.
   - `E0046` missing `TerminalAdapter::try_read` at `src/transport/webrtc/adapter.rs:145`.
   - `E0046` missing `inject_ingress_frame`, `inject_ingress_partial`,
     `complete_ingress_partial`, and `drop_buffered_ingress_frame` on
     `TerminalAdapterHarnessDriver` at `src/transport/unix/adapter.rs:477`.
   - The same four missing driver methods at `src/transport/webrtc/adapter.rs:443`.
   - `E0004` non-exhaustive match on `CoreDaemonError::ControlPlaneFailed` at
     `src/runtime.rs:4255`.
   - `E0004` non-exhaustive match on
     `BindTerminalAdapterError::ControlPlaneFailed` at `src/runtime.rs:4294`.
2. A partial roll is not viable. Leaving
   `crates/botster-hub-client/Cargo.toml` and
   `crates/botster-hub-test-support/Cargo.toml` at the old pin produces five
   extra `E0308` errors caused by two `botster_terminal_protocol` versions in
   one dependency graph, at `src/admission/unix_hello.rs:63`, `:72`, `:86`,
   `src/transport/webrtc/control_channel.rs:349`, `:366`, `:381`, and
   `src/transport/webrtc/peer.rs:1168`, `:1207`, `:1251`.
3. `crates/botster-hub-test-support/build.rs` panics with
   "cargo metadata did not include botster-terminal-protocol at rev …" unless
   its `PROTOCOL_REV` constant rolls with the manifests.
4. The Core adapter contract, the conformance harness, and the daemon error
   enums are byte-identical between `3672c667` and `a781556`.
   `git diff 3672c667 a781556` over `crates/botster-core/src/contract/`,
   `crates/botster-core-test-support/src/terminal_adapter/`, and
   `crates/botster-core-daemon/src/daemon.rs` is empty. The alignment work in
   this ticket is therefore identical for both revisions.
5. Baseline: at the current pin `7eafa470`,
   `RUSTUP_TOOLCHAIN=1.97.0 ./test.sh --locked -p botster-hub --test hub_daemon_lifecycle_test unix_adapter_unbound_scoped_drain_delivers_terminal_output -- --exact --nocapture`
   passes. The reproduction is a new Core regression at `3672c667`, not a
   pre-existing Hub failure.

## 3. Product decisions taken by the human

Two answered questions govern this plan: `question_1788138575_832281` and
`question_1788139680_730932`. The second was asked after Plan Review
`review_1788139424_854213` returned changes required.

**From `question_1788139680_730932` (second visit, supersedes where they differ):**

| Decision | Value |
| --- | --- |
| Core pin value | The pin moves from `d47ede0` to the current frozen candidate `a781556258789dea4a50ffcb17351e7294c8ff26`. |
| Ingress proof | Option B. Add the strongest sanctioned test-seam proof in this ticket. State plainly that it does not prove Hub production reachability. Do not describe the ingress buffer as active production input. |
| Production proof owner | `ticket_1787894427_525056` must prove a real Hub wake pump reaches `try_read` through the wired Unix and WebRTC producers, retires only the lost route, and preserves the sibling. Recorded as checklist `checklist_1788139722_173987`, items `check_1788139727_933838` and `check_1788139731_744972`. |
| Scope guard | Do not expand this alignment ticket into the wake pump or the transport cutover. |

**From `question_1788138575_832281` (first visit, still in force):**

| Decision | Value |
| --- | --- |
| Core pin strategy | Option C. Pin every Hub Core site and lock source to the exact candidate `a781556`. |
| Red main | Not allowed. Do not merge a red Hub `main`. |
| Reproduction test | Do not mark it `#[ignore]`. |
| Downstream proof | Run the reproduction against both `3672c667` and `a781556` and record the fail/pass pair. |
| Candidate stability | The Core run must merge `a781556` unchanged. If Core Review changes the candidate, stop both runs, update the Hub pin, and repeat the proof. |
| After Core merge | The exact pin stays valid once `main` contains `a781556`. Do not create a no-op re-pin. |
| Adapter ingress | Option 1. Add the conformance-complete bounded ingress buffer and the required harness methods. Do not wire Unix or WebRTC producers here. |
| Production duplex cutover | Owned by the active Hub cold-cut ticket `ticket_1787894427_525056`. Do not build a second competing implementation, and do not retain the control-plane terminal route in the final cold cut. |

## 4. Scope

1. Roll every Core revision literal and lock source from `7eafa470` to
   `a781556`. The inventory is in section 7.
2. Add a bounded ingress buffer to the shared adapter slot, and implement
   `TerminalAdapter::try_read` on `UnixTerminalAdapter` and
   `WebRtcTerminalAdapter` against it.
3. Add the transport-side ingress push and loss-marking entry points on
   `UnixTerminalAdapterHandle` and `WebRtcTerminalAdapterHandle`.
4. Implement the four new `TerminalAdapterHarnessDriver` methods in both
   adapter test modules so the existing conformance harness tests exercise the
   new ingress laws.
5. Add `CoreDaemonError::ControlPlaneFailed` and
   `BindTerminalAdapterError::ControlPlaneFailed` arms to
   `managed_session_core_error_class` in `src/runtime.rs`, with distinct class
   strings and unit coverage.
6. Add the sanctioned test-seam teardown proof described in section 6.5: two
   sibling routes through the real Hub attach and bind path, ingress loss on one
   route, the real Core hard-stop path, exact route retirement, and sibling
   survival.
7. Produce the downstream reproduction proof pair against `3672c667` and
   `a781556`.

### Non-scope

- No Unix or WebRTC production ingress producer. No read-side wiring of client
  terminal frames into the new buffer. That belongs to `ticket_1787894427_525056`.
- No removal or change of the existing control-plane `SendInput` route.
- No change to Core, `botster-web`, `botster-tui`, or `botster-workspaces`.
- No new transport crate, no replay buffer, no second terminal route.
- No refactor of `AdapterSlot` write-path behavior, pressure semantics, or close
  semantics beyond the ingress addition.
- No `#[ignore]` on any test.
- No change to the `README.md` dependency-policy prose. That text is already
  correct for an exact `rev` pin and needs no edit for a pin value change.

## 5. Ownership boundaries and cross-repository seams

- Core owns the `TerminalAdapter` contract, `TerminalIngress`, the
  `MIN_ADAPTER_INGRESS_BUFFER_FRAMES` constant, the conformance harness, and the
  `ControlPlaneFailed` error variants. Hub consumes them and must not restate or
  redefine them. Hub must use Core's constant, not a local number.
- Hub owns the concrete Unix and WebRTC adapter mechanics, the bounded ingress
  buffer implementation, and the Hub-facing error class strings.
- Hub adapters stay content blind. The ingress buffer stores opaque byte
  vectors and must not parse `TerminalInputFrame`. Core decodes.
- All source changes stay in `botster-hub`, as the ticket requires.
  `botster-hub-client` and `botster-hub-test-support` are in-repo path crates
  under `crates/`, so their pin rolls are Hub changes and create no
  cross-repository edit.

### Cross-repository merge-order constraint

`botster-hub` will pin `a781556`, which is the unmerged tip of Core branch
`project-pipelines/ticket_1788128130_441301` and a descendant of Core `main` at
`3672c667`. The constraint is:

1. Hub merges first, pinned at `a781556`.
2. The Core run for `ticket_1788128130_441301` then merges `a781556` unchanged
   to Core `main`.
3. If Core Review changes the candidate, both runs stop, the Hub pin moves to
   the new candidate, and the proof pair is repeated.

The correct dependency edge is already registered, in the correct direction, and
I confirmed it with `project_pipelines_list_ticket_dependencies`:

| Field | Value |
| --- | --- |
| Dependency id | `dependency_1788137138_804385` |
| Dependent ticket | `ticket_1788128130_441301` (Core) |
| Depends on | `ticket_1788137128_417142` (this Hub ticket) |
| Dependency repository target | `botster-core`, `tgt_1f7bce66eb304881980f9b4a2a5ae3fe` |
| Direction | Core waits on Hub, which matches the required merge order |

I added no second dependency in the reverse direction. An edge from this Hub
ticket onto the Core ticket would state that Hub cannot start until Core closes,
which inverts the order above and would deadlock the pair against the existing
edge. My first plan revision said no dependency was registered at all. That was
wrong: `dependency_1788137138_804385` already existed, and Plan Review
`finding_1788139424_837637` correctly caught the omission.

Because a cross-repository Project Pipelines dependency governs this seam,
[[project-pipelines-playbook]] is in scope and is loaded. Two of its notes apply
directly: [[each acceptance condition names its authoritative production oracle]]
drives the honest production-oracle statement in section 9, and
[[dependency closure must requeue the blocked parent step]] means the Core
ticket resumes once this Hub ticket closes.

Implement and Verify must restate the merge-order constraint in their reports.

## 6. Design

### 6.1 Ingress buffer

Add ingress state to `AdapterSlot` in `src/transport/shared/adapter_slot.rs`, so
both transports share one implementation and one set of laws.

State:

- `ingress: Mutex<VecDeque<Vec<u8>>>` holding complete opaque frames in arrival
  order.
- `ingress_lost: AtomicBool`, a pending-loss flag.

Capacity is
`botster_core::contract::terminal_adapter::MIN_ADAPTER_INGRESS_BUFFER_FRAMES`.
Do not introduce a Hub constant.

`try_read` order is exact and must not be reordered:

1. If the slot is closed, return `TerminalIngress::Closed`. Closed outranks a
   pending loss and outranks buffered frames, and buffered ingress is dropped on
   close.
2. If `ingress_lost` is set, clear it and return `TerminalIngress::Lost`.
3. Pop the front frame and return `TerminalIngress::Frame(bytes)`.
4. Otherwise return `TerminalIngress::Empty`.

Producer entry points, used by the harness drivers now and by the cold-cut
ticket later:

- `push_ingress_frame(bytes)`: if the adapter is closed, drop the bytes. If the
  queue already holds `MIN_ADAPTER_INGRESS_BUFFER_FRAMES` frames, drop the new
  frame and set `ingress_lost`. Otherwise push to the back.
- `mark_ingress_lost()`: set `ingress_lost`.
- `drop_newest_ingress_frame()`: pop the back frame, if any, and set
  `ingress_lost`. This is the transport-loss primitive the harness drives.

`close` clears the ingress queue in addition to its current work. `try_read`
must never block: it uses the same non-blocking lock discipline the existing
`close` path uses, and never waits on the transport writer.

Ordering rationale, taken from the Core harness in
`crates/botster-core-test-support/src/terminal_adapter/mod.rs`:
`assert_ingress_lost` injects `keep`, injects `drop`, calls
`drop_buffered_ingress_frame`, then requires `Lost` first and
`Frame("keep")` second. `assert_ingress_closed_local` and
`assert_ingress_closed_transport` require `Closed` after an injected frame.
`assert_ingress_lost` also injects `MIN_ADAPTER_INGRESS_BUFFER_FRAMES` frames and
requires that the first read is not `Lost`, which fixes the capacity floor.

### 6.2 Adapter implementations

`UnixTerminalAdapter::try_read` and `WebRtcTerminalAdapter::try_read` each
delegate to their inner slot. Both keep their existing `try_write`, `close`, and
`pressure` behavior unchanged.

Producer methods land on the connection-owned handles,
`UnixTerminalAdapterHandle` and `WebRtcTerminalAdapterHandle`, because the
connection reader owns the handle in production. Keep them `pub(crate)`, matching
the existing handle methods.

### 6.3 Harness drivers

In both adapter test modules:

- `inject_ingress_frame(bytes)` calls the handle's `push_ingress_frame`.
- `inject_ingress_partial(bytes)` stores the bytes in a driver-local
  `partial: Option<Vec<u8>>` field and pushes nothing. This is faithful to
  production: Unix framing is length prefixed at the connection reader and a
  WebRTC DataChannel message is atomic, so an incomplete frame never reaches an
  adapter.
- `complete_ingress_partial()` takes the staged bytes and calls
  `push_ingress_frame`.
- `drop_buffered_ingress_frame()` calls the handle's
  `drop_newest_ingress_frame`.

### 6.4 Error mapping

Add two arms to `managed_session_core_error_class` in `src/runtime.rs`:

- `CoreDaemonError::ControlPlaneFailed(_)` maps to `"control_plane_failed"`.
- `BindTerminalAdapterError::ControlPlaneFailed { .. }` maps to
  `"bind_terminal_adapter.control_plane_failed"`.

The two strings must stay distinct, because the two variants report different
Core rejections: a control-plane call refused on a failed session, and a bind
refused on a failed session. Keep the existing no-slash invariant that the
neighbouring assertion at `src/runtime.rs:5447` already checks.

### 6.5 Sanctioned test-seam teardown proof

Plan Review `finding_1788139424_468510` requires a live hard-stop and
sibling-survival proof. Section 9b records why a Hub production-path version is
not achievable in this ticket. This is the strongest proof that is achievable,
and the human approved it as option B in `question_1788139680_730932`.

Add one in-crate test beside the existing attach-route tests in
`src/subscription/attach_routes.rs`, so it can reach the `pub(crate)` handles and
the `#[cfg(test)]` drain seam. The test:

1. Starts an isolated Hub and spawns one session.
2. Drives the **real** Hub attach and bind path twice, for two sibling
   subscriptions on that one session, so both routes are bound through
   production code and both handles are registered in the mux.
3. Calls `mark_ingress_lost` on exactly one route's registered handle.
4. Calls the sanctioned `#[cfg(test)]` `HubRuntime::drain_runtime_once` seam.
   That runs **real Core code**: `CoreDaemon::drain`, `apply_terminal_input`,
   `ClientWorker::intake_terminal_input`, `adapter.try_read`, the
   `TerminalIngress::Lost` arm, and `hard_stop_key`.
5. Asserts that Core retired exactly the lost `(session_id, subscription_id,
   generation)` route.
6. Asserts that the sibling route on the same session is still live and still
   accepts terminal output.

The test must carry a comment, and the Implement report must carry a sentence,
stating this exact limit: the driver is the Hub test seam, not a Hub production
loop, so the test proves Core's teardown behavior and Hub's adapter behavior, but
it does not prove Hub production reachability. The production-path obligation
lives on `ticket_1787894427_525056`.

Two mechanics to settle during Implement:

- The mux stores handles in a private `routes` map. Add a small `#[cfg(test)]`
  lookup by `(session_id, subscription_id, generation)` rather than keeping a
  side copy of the handle, so the test reads the handle the production bind path
  actually registered.
- `src/lib.rs:917` guards production paths against calling `drain_runtime_once(`.
  Confirm the guard's scanned file set still excludes `#[cfg(test)]` modules, and
  do not weaken the guard to make the test compile.

### Core revision literal sites

All change `7eafa470a18025895995bbedc20d34b58106a03b` to `a781556258789dea4a50ffcb17351e7294c8ff26`.

| File | Line(s) | Purpose |
| --- | --- | --- |
| `Cargo.toml` | 27, 28, 29 | `botster-core`, `botster-core-daemon`, `botster-terminal-protocol` runtime pins |
| `Cargo.toml` | 46, 47 | `botster-core-test-support`, `botster-terminal-ghostty` dev pins |
| `crates/botster-hub-client/Cargo.toml` | 11 | Git-visible `botster-terminal-protocol` pin |
| `crates/botster-hub-test-support/Cargo.toml` | 17, 21, 34 | `botster-core`, `botster-terminal-protocol`, dev `botster-terminal-ghostty` |
| `crates/botster-hub-test-support/build.rs` | 10 | `PROTOCOL_REV` fixture locator |
| `crates/botster-hub-test-support/src/conformance_data.rs` | 42 | `LATE_ATTACH_GHOSTSNP_CORE_PIN` |
| `crates/botster-hub-test-support/src/lib.rs` | 6433 | provenance unit test literal |
| `tests/session_projection_owner_loop.rs` | 9 | `REQUIRED_CORE_REV` |
| `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` | 5 | `LOCKED_CORE_REV` |
| `tests/hub_daemon_lifecycle/event_plane_saturation.rs` | 2863 | live `botster_core` provenance assertion |
| `tests/hub_daemon_lifecycle/package_event_plane.rs` | 48 | live-proof locked Core assertion |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | 1589 | provenance log literal |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | 1010 | provenance log literal |
| `Cargo.lock` | 387, 406, 418, 511, 519, 529 | six Core-family `source =` lines |

That is fourteen literal sites plus the six lock sources the Hub charter names.
Historical documents under `docs/plans/` and `docs/reports/` keep the old SHA and
must not be edited.

### Source changes

| File | Change |
| --- | --- |
| `src/transport/shared/adapter_slot.rs` | Ingress queue, loss flag, `try_read`, `push_ingress_frame`, `mark_ingress_lost`, `drop_newest_ingress_frame`; clear ingress on `close` |
| `src/transport/unix/adapter.rs` | `try_read` on the adapter, ingress methods on `UnixTerminalAdapterHandle`, four driver methods, driver `partial` field |
| `src/transport/webrtc/adapter.rs` | Same shape for `WebRtcTerminalAdapter` and `WebRtcTerminalAdapterHandle` |
| `src/runtime.rs` | Two new error class arms plus unit coverage near the existing bind-error test at line 5467 |

## 8. Assumptions and unknowns

Assumptions:

- `a781556258789dea4a50ffcb17351e7294c8ff26` is the current frozen Core
  candidate, confirmed by `git ls-remote` at plan time. It replaced `d47ede0`
  during this Plan step, so treat candidate movement as likely, not theoretical.
  Implement re-runs `git ls-remote` immediately before editing any literal and
  refuses to pin a different revision without a new human answer.
- The Core adapter contract stays identical between `3672c667` and `a781556`.
  Verified at plan time by an empty diff over the three contract paths. Implement
  re-checks after any candidate change.
- The existing guard test `git_visible_hub_members_share_one_exact_core_revision`
  is the enforcement for a complete roll, so a missed literal fails a test rather
  than shipping.
- `README.md` dependency-policy prose already states the exact `rev` policy and
  needs no edit. Implement confirms this and states the confirmation.

Unknowns for Implement and Verify:

- Whether `./test.sh --locked` completes green in one default-concurrency run on
  this host. The Hub suite has documented flake classes and needs a quiet host.
  Any failure needs isolation plus a base comparison at `c674a62` with the old
  pin, with exact evidence. A pre-existing failure is not an excuse without that
  comparison.
- Whether the `a781556` Core fix changes any other Hub-visible behavior beyond
  the reproduction. The `3672c667`-to-`a781556` diff touches
  `client_worker.rs`, `managed_session_runtime.rs`, `control_queue.rs`,
  `runtime/mod.rs`, and `worker_process.rs`. The full Hub suite at the new pin is
  the check.
- Whether GitHub CI for Hub `main` is green before this change. The Implement
  report must state the local toolchain and must not claim GitHub CI green.

## 9. Runtime-teardown class

The class applies. The change is on the terminal adapter seam and the ingress
path feeds Core teardown decisions.

| Field | Answer |
| --- | --- |
| `teardown_class_applies` | Yes. The ticket changes bound-adapter ingress, and `TerminalIngress::Lost` drives a Core hard stop in `ClientWorker::intake_terminal_input_keys`. |
| `teardown_isolation` | Ingress state lives in one `AdapterSlot`, which is per subscription route and per generation. One route's loss or close cannot reach a sibling route, because the mux keys routes by `(session_id, subscription_id, generation)`. |
| `teardown_bounds` | The queue holds at most `MIN_ADAPTER_INGRESS_BUFFER_FRAMES` frames. Overflow drops one frame and sets one flag. `try_read` does bounded constant work, never blocks, and never waits on the transport writer. Core reads at most `INTAKE_FRAMES_PER_SUBSCRIPTION_PER_TICK` frames per tick. No unbounded growth and no spin. |
| `late_message_matrix` | See the expanded table below. It now covers the ownership-creating Attach and bind surface, which the first plan revision omitted. |
| `production_path_proof` | **Corrected. Hub cannot reach `try_read` in production today.** See the dedicated subsection below. The error-mapping half is live; the ingress half is intentionally scaffold-only. |
| `ownership_identity` | The adapter is owned by the Core-side bound route. The handle is owned by the Hub connection. Both point at the same `Arc<…Inner>`, so identity is the `Arc`, not a name lookup. Route identity in the mux stays `(session_id, subscription_id, generation)`. |
| `sibling_fail_closed_policy` | Fail closed on doubt. A dropped or overflowed frame sets `ingress_lost`, which makes Core hard stop that one route rather than deliver a gap. A closed adapter reports `Closed` permanently and never returns to `Empty`. No sibling route is torn down by another route's loss. |

### 9a. Expanded late-message matrix

Rows 1 to 7 are the adapter ingress states. Rows 8 to 12 are the
ownership-creating Attach and bind surface that the first plan revision omitted,
which Plan Review `finding_1788139424_468510` correctly required.

| # | Late event | Required behavior |
| --- | --- | --- |
| 1 | Push after local `close` | Bytes dropped. `try_read` stays `Closed`. |
| 2 | Push after transport-side close | Bytes dropped. `try_read` stays `Closed`. |
| 3 | `try_read` after close with frames buffered | `Closed`. Buffered ingress discarded. |
| 4 | `try_read` after close with a pending loss | `Closed`. Closed outranks loss. |
| 5 | Loss marked, then close | `Closed`. |
| 6 | Read on a fresh adapter | `Empty`. |
| 7 | Read on a drained adapter | `Empty`, never a spurious `Lost`. |
| 8 | Push arrives for a route whose generation was already retired | The retired generation's `AdapterSlot` is closed, so the bytes are dropped and `try_read` stays `Closed`. A retired generation can never feed a live route, because the mux keys handles by `(session_id, subscription_id, generation)`. |
| 9 | Attach creates a new generation while an old handle still holds buffered ingress | The old generation's adapter is closed by the existing detach and rebind path. Its ingress is discarded. The new generation starts with an empty queue and no pending loss. Ingress never crosses a generation boundary. |
| 10 | Bind is rejected after the adapter was created | `attach_routes` already calls `fail_closed_pre_bind_attach` with the `BoundAdapterHandle`. That path closes the adapter, so any ingress pushed between creation and rejection is dropped. `BindTerminalAdapterError::ControlPlaneFailed` now takes this same path, and Core also calls `adapter.close()` before returning it. |
| 11 | Push races an in-flight `close` from the connection reader | `close` and `push_ingress_frame` take the same non-blocking lock discipline. `close` sets the closed cause first, so a push that observes the closed cause drops the bytes. A push that already holds the lock cannot resurrect a closed adapter, because `try_read` checks closed before it pops. |
| 12 | Loss marked on a retired route, then a sibling is read | The sibling has its own `AdapterSlot` and its own flag. Its `try_read` returns `Empty` or `Frame`. Sibling survival is asserted by the section 6.5 test. |

### 9b. Production-path proof, corrected

My first plan revision claimed that Core calls `TerminalAdapter::try_read` on
every bound Hub adapter in production, citing Core's wake intake. **That claim
was wrong.** The corrected, verified position:

`try_read` is reachable only through two Core entry points:

1. `CoreDaemon::pump_woken`, the wake intake path.
2. `CoreDaemon::drain`, which calls `apply_terminal_input`, then
   `ClientWorker::intake_terminal_input`, then `adapter.try_read`.

Hub drives neither today:

- Hub source contains no call to `pump_woken` or `wait_wakes`.
- Hub's only wrapper for `CoreDaemon::drain` is
  `HubRuntime::drain_runtime_once` at `src/runtime.rs:3786`. It is `#[cfg(test)]`
  and its own doc comment reads "Test helper for Core terminal Drain. Production
  daemon paths must not call this."
- `src/lib.rs:917` carries a source guard that fails if a production path calls
  `drain_runtime_once(` or `drain_subscription(`.
- Core's `observe_session` calls `engine.drain_runtime_once`, which skips
  `apply_terminal_input`. So Hub's observe pump and `ReadScreen` do not reach
  `try_read` either.

So the two halves of this ticket have different standing, and the plan must not
blur them:

| Half | Production oracle | Status |
| --- | --- | --- |
| `ControlPlaneFailed` error mapping | `CoreDaemon::attach` calls `ensure_control_plane_live`; `bind_terminal_adapter` and `bind_waking_terminal_adapter` return `BindTerminalAdapterError::ControlPlaneFailed`. Hub reaches all three on the live Unix and WebRTC attach paths. | **Live.** Both new arms run on real rejections. |
| Adapter ingress buffer and `try_read` | None in Hub today. `ticket_1787894427_525056` will supply it by wiring the producers and the wake pump. | **Intentionally scaffold-only**, by human decision `question_1788138575_832281` option 1 and `question_1788139680_730932` option B. |

The ingress buffer must not be described as active production input in any
report. The section 6.5 test proves Core's teardown behavior and Hub's adapter
behavior through real Core code, but its driver is the Hub test seam, so it does
not prove Hub production reachability. The production-path obligation is
registered on `ticket_1787894427_525056` as checklist
`checklist_1788139722_173987`.

`try_read` still has to be correct now, even while unreachable, because the
moment `ticket_1787894427_525056` wires the pump, a wrong `Lost` becomes a live
route teardown.

## 10. Risks

| Risk | Mitigation |
| --- | --- |
| Wrong `try_read` precedence silently tears down live terminals, because `Lost` triggers a Core hard stop. | The Core conformance harness pins the exact order. Both adapters already run `assert_terminal_adapter_conformance`, so the new laws are exercised the moment the driver methods exist. Add a red ablation for the precedence. |
| A spurious `Lost` on an idle adapter kills healthy routes. | `assert_ingress_non_blocking` and `assert_ingress_empty` require `Empty` on fresh and drained adapters. Add a Hub test that reads an idle bound adapter many times and requires `Empty` every time. |
| Partial pin roll leaves two `botster_terminal_protocol` versions and five `E0308` errors. | The section 7 inventory, a zero-match grep for the old SHA outside `docs/`, and the existing `git_visible_hub_members_share_one_exact_core_revision` guard. |
| `build.rs` panics because `PROTOCOL_REV` did not roll. | It is line 10 in the section 7 inventory, and the failure is loud at build time. |
| Pinning an unmerged Core candidate leaves Hub `main` depending on a branch tip. | Section 5 merge-order constraint, restated in gate evidence and in the Implement and Verify reports. |
| The Core candidate changes during review, so the Hub pin and the proof pair go stale. | The human instruction: stop both runs, update the pin, repeat the proof. Implement re-runs `git rev-parse` and the contract diff before pinning. |
| A dead ingress buffer with no producer reads as unwired scaffolding. | It **is** scaffolding, and the plan says so rather than dressing it up. Sections 3 and 9b record the approved scaffold-only scope. Acceptance check 13a proves the adapter and Core teardown behavior through the sanctioned test seam. `checklist_1788139722_173987` on `ticket_1787894427_525056` owns the true production proof through the wired Unix and WebRTC producers and the wake pump. |
| Hub suite flake on a busy host produces a false red. | Run the locked gate on a quiet host, and compare any failure against `c674a62` at the old pin. |
| The section 6.5 test is mistaken for a production-path proof, so the reachability gap is forgotten. | Section 9b states the limit, the test carries the same comment, the Implement report must repeat it, and `checklist_1788139722_173987` holds the obligation on `ticket_1787894427_525056`. |
| Adding a `#[cfg(test)]` mux handle lookup tempts a widening of production visibility. | Keep the lookup `#[cfg(test)]` and keyed by the exact route triple. Do not make the `routes` map or the handles public. |
| The Core candidate already moved once, from `d47ede0` to `a781556`, during this Plan step. | Implement re-runs `git ls-remote` for the branch immediately before editing literals, and repeats the section 11 check 2 contract diff. If the tip moved again, stop and repeat the proof per the human rule. |

## 11. Acceptance checks

Run every Rust gate with `RUSTUP_TOOLCHAIN=1.97.0` and record `rustc --version`
from that shell. The worktree path holds no `:`, so do not set
`CARGO_TARGET_DIR`, and unset it before the official locked gate.

| # | Command or check | Expected result |
| --- | --- | --- |
| 1 | `git rev-parse` the Core candidate and record the 40-character SHA | Matches `a781556258789dea4a50ffcb17351e7294c8ff26` and is the tip of `project-pipelines/ticket_1788128130_441301` |
| 2 | `git diff <sha> 3672c667 -- crates/botster-core/src/contract/ crates/botster-core-test-support/src/terminal_adapter/ crates/botster-core-daemon/src/daemon.rs` | Empty, so the alignment work is valid for both revisions |
| 3 | `grep -rn 7eafa470 --exclude-dir=target --exclude-dir=.git .` | Hits only under `docs/plans/` and `docs/reports/` |
| 4 | `cargo tree -e normal -i botster-terminal-protocol --locked` | Exactly one source, at the new rev |
| 5 | `cargo fmt --all -- --check` | Clean |
| 6 | `cargo clippy --workspace --all-targets --locked -- -D warnings` | Clean, rerun after each repair |
| 7 | `./test.sh --locked --test session_projection_owner_loop git_visible_hub_members_share_one_exact_core_revision -- --exact` | Passes at the new rev |
| 8 | `./test.sh --locked -p botster-hub --lib transport::unix::adapter::tests::production_unix_adapter_passes_core_conformance_harness -- --exact` | Passes, now including every ingress law |
| 9 | The WebRTC conformance harness test, by its exact full module path | Passes |
| 10 | New Hub unit test: `try_read` precedence, `Closed` before `Lost` before `Frame` before `Empty` | Passes |
| 11 | New Hub unit test: `MIN_ADAPTER_INGRESS_BUFFER_FRAMES` frames buffer without `Lost`, and frame `N + 1` sets `Lost` | Passes |
| 12 | New Hub unit test: an idle bound adapter returns `Empty` on repeated reads and never `Lost` | Passes |
| 13 | New Hub unit test: both new error class strings, distinct, no `/`, next to the existing bind-error test | Passes |
| 13a | New in-crate test from section 6.5: two sibling routes bound through the real Hub attach path, ingress loss on one, real Core hard stop through the `#[cfg(test)]` drain seam | Passes. Exactly the lost `(session_id, subscription_id, generation)` route retires, and the sibling stays live and still accepts output. |
| 13b | Confirm `src/lib.rs:917` source guard still rejects a production call to `drain_runtime_once(` | Passes unchanged. The guard was not weakened to make 13a compile. |
| 14 | `cargo build --locked -p botster-core-daemon --bin botster-session-worker` and `cargo build --locked --bin botster-hub` | Succeed, before the locked suite |
| 15 | `./test.sh --locked` on a quiet host with `CARGO_TARGET_DIR` unset | Green in one default-concurrency run |

### Downstream proof pair, required by the human answer

| # | Command | Expected result |
| --- | --- | --- |
| 16 | With all literals at `3672c667`: `RUSTUP_TOOLCHAIN=1.97.0 ./test.sh --locked -p botster-hub --test hub_daemon_lifecycle_test unix_adapter_unbound_scoped_drain_delivers_terminal_output -- --exact --nocapture` | Fails. No `echo:from-unbound` on `ReadScreen`. This is the reproduction for `ticket_1788128130_441301`. |
| 17 | With all literals at the candidate: the same command | Passes |
| 18 | Baseline already recorded at plan time, at `7eafa470` on base `c674a62` | Passed, so the reproduction is a new Core regression and not a pre-existing Hub failure |

Steps 16 and 17 use a scratch roll of the same literal inventory. Only the
candidate pin is committed. Both results go in the Implement report with exact
command text, the `rustc --version` line, and the pass or fail counts.

### Red ablations

Each ablation reverts one guard, proves the matching test fails, then restores it.
Use full module paths with `--exact`, because a bare leaf name filters every test
out and reports `ok`. Do not run an ablation while another Hub suite is running.

1. Return `Empty` instead of `Closed` from `try_read` after close. Expect the
   conformance closed assertions to fail.
2. Return the queued frame before the pending loss. Expect `assert_ingress_lost`
   to fail.
3. Set the queue capacity to `MIN_ADAPTER_INGRESS_BUFFER_FRAMES - 1`. Expect the
   capacity floor assertion to fail.
4. Give both `ControlPlaneFailed` arms the same class string. Expect the new
   error-class test to fail.

## 12. Vault gaps worth capturing

1. "Hub adapter `try_read` precedence is Closed, Lost, Frame, Empty" — the exact
   order the Core conformance harness pins, and the fact that a misordered or
   spurious `Lost` makes Core hard stop a live route.
2. "Hub may pin an unmerged Core candidate when the alternative is a red main" —
   the merge-order constraint, and why a Project Pipelines blocking dependency is
   the wrong instrument for it.
3. "Hub Core pin rolls have fourteen literal sites and six lock sources" — the
   current inventory has grown past the older plan's twelve rows, and
   `git_visible_hub_members_share_one_exact_core_revision` is the guard.
4. "Hub cannot reach TerminalAdapter::try_read in production" — Hub calls neither
   `CoreDaemon::pump_woken` nor `CoreDaemon::drain`, and `drain_runtime_once` is
   a guarded `#[cfg(test)]` seam. This is the fact that made the first plan
   revision's production-path claim wrong, and it stays true until
   `ticket_1787894427_525056` wires the pump.
5. "A frozen Core candidate can move during a single Plan step" — the candidate
   moved from `d47ede0` to `a781556` between the first and second Plan visits.
   Consumers that pin an unmerged candidate must re-resolve the tip immediately
   before editing literals, not once at plan time.
