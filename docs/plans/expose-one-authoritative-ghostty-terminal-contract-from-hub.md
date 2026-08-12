# Plan: Expose one authoritative Ghostty terminal contract from Hub

Ticket: `ticket_1786471489_718500`  
Run: `run_1786476458_719916`  
Plan **revision 9** after Plan Review `review_1786499667_906431` (`changes_required`)

## Target

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| **Implement Core pin** | `2c5171a6cb3b073c53620a9838d8b08480dd215c` |
| Plan worktree | Plan artifact only; **do not** mutate `Cargo.lock` at Plan |

## Playbooks and ownership notes (required)

- [[botster-hub-playbook]] — host profile; **does not own terminal bytes / scrollback / per-client egress**
- [[botster-hub-client-playbook]] — external DTOs
- [[botster data plane bypasses the hub through session and client actors]] — **hard constraint this revision**
- [[planner-playbook]], [[botster-planner-playbook]], [[project-pipelines-playbook]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[hub test support npm releases need external consumer smoke]]
- [[adding a hub client feature constant is a three site change]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[conformance fixture revisions must be unique per published content]]
- [[initial terminal snapshots must precede live output activation]]
- [[botster clients restore visible terminal state from readscreen before buffered live output]]
- [[synced state types are allowed while pushed event variants are forbidden]]
- Human answers: `question_1786477685_799257`, `question_1786478263_847168`

`teardown_class_applies` = **false**

## Plan Review corrections (rev 8–9)

| Finding | Decision |
| --- | --- |
| Control path carries GHOSTSNP (`finding_1786499237_465402`) | **GHOSTSNP only on `DaemonEvent::Snapshot`.** No control payload bytes. Colors on attach from GHOSTSNP install. |
| Weak acceptance (`finding_1786499237_182032`) | Named `external_hub_*` production proofs (below). |
| ModeGatedInput has no compatibility identity (`finding_1786499667_886491`) | **Locked additive identity (below):** required feature token + conformance rev **34** + protocol **6**. |

## Context

- Core deps closed: Ghostty-only, color `9d41ad4…`, mode `2c5171a6…`.
- Core still exposes `capture_color_and_snapshot` for **Hub-internal / test** agreement proofs; **not** a client attach byte path.
- Charter: SessionIo/ClientWorker own snapshot/scrollback bytes; Hub is control/policy/orchestration ([[botster data plane bypasses the hub through session and client actors]], [[botster-hub-playbook]]).
- Checklist `checklist_1786476742_921432` reused.

## Architecture (locked)

### Data plane (authoritative terminal bytes)

| Carrier | Role |
| --- | --- |
| `DaemonEvent::Snapshot` | **Only** GHOSTSNP import path (`ghostty-terminal-snapshot-v1` / magic). Install before live. |
| `DaemonEvent::Scrollback` | Non-GHOSTSNP raw/history carrier if emitted; **never import as GHOSTSNP**. |
| `DaemonEvent::TerminalOutput` | Live text after install |

Path: public client → daemon socket → `daemon_transport` → attach/subscription → **SessionIo → ClientWorker → Snapshot/TerminalOutput events**. Hub control handlers must **not** re-emit GHOSTSNP on request/response bodies.

### Control plane (no terminal payload bytes)

| Operation | Role |
| --- | --- |
| Startup `with_terminal_color_profile` | Initial/reset baseline only (human RGB defaults) for pre-attach OSC 10/11/12 via Core/session Ghostty |
| `ReadModeFlags` | Full `ModeFlags` + `ModeFreshnessToken { mode_generation, mode_revision }` |
| `ModeGatedInput` | Race-free mode-dependent input via `CoreDaemon::mode_gated_input` |
| `ReadScreen` / `CaptureSnapshot` (existing metadata-only capture) | Unchanged opacity policy for capture metadata |
| **No** public `CaptureColorAndSnapshot` that returns `payload_base64` | Would reintroduce control-path terminal bytes |

**Current colors after session start:** carried inside **GHOSTSNP** on the data-plane Snapshot (Ghostty-owned, including OSC mutations). Clients must not apply Hub startup profile after GHOSTSNP install (`question_1786478263_847168`).

**Optional colors-only control DTO:** **Out of this ticket.** Attach/reconnect use GHOSTSNP only. If a future ticket needs colors-only re-probe, it must not carry snapshot bytes and must not override data-plane Snapshot install.

**Hub-internal tests** may call `CoreDaemon::capture_color_and_snapshot` in-process to assert Core agreement; that is not a public daemon client contract and must not ship GHOSTSNP on `DaemonResponse`.

## Product contract

### Startup baseline (host profile)

1. Explicit host config if present  
2. Else FG `#FFFFFF`, BG `#282C34`, cursor `#FFFFFF` @ indexes `0x1000/0x1001/0x1002`  
3. Applied once at `CoreDaemon` construction via `with_terminal_color_profile`  
4. Does not rewrite durable sessions; not “current” after Snapshot install  

Mode-gated timeout: Core **`DEFAULT_MODE_GATED_INPUT_TIMEOUT` = 5s** (Hub does not override for production).

### Mode

```text
DaemonModeFlags { // #[non_exhaustive]
  session_id, kitty_enabled, cursor_visible, bracketed_paste,
  mouse_mode, alt_screen, focus_reporting, application_cursor,
  mode_generation: u64, mode_revision: u64
}

DaemonRequest::ModeGatedInput {
  session_id, data, mode_generation, mode_revision
  // no deadline field
}

DaemonModeGatedInputResult {
  session_id, admitted, bytes_written,
  /* current flags */, mode_generation, mode_revision, error_kind?
}
```

Kitty/mouse encodings **must** use `ModeGatedInput`. Plain `Input` is non-mode-dependent only.

### Compatibility identity for ModeGatedInput + mode freshness (locked — rev 9)

Published baseline (npm `@trybotster/hub-test-support@0.1.28`): `PROTOCOL_VERSION = 6`, `CONFORMANCE_FIXTURE_REVISION = 33`, no mode-gated request / freshness fields.

| Item | Locked value |
| --- | --- |
| Protocol | **`PROTOCOL_VERSION` remains `6`** (additive request + response fields; exact protocol match still required) |
| Conformance | **`CONFORMANCE_FIXTURE_REVISION = 34`** (strictly above published 33; unique content for ModeGatedInput + ModeFlags freshness + Snapshot-only GHOSTSNP rules) |
| Feature token | **`FEATURE_MODE_GATED_INPUT: &str = "mode_gated_input"`** |
| Advertisement | Hub `DaemonCompatibility.features` includes `mode_gated_input` (via hub-client `current_feature_list()` used by daemon hello/status) |
| Client requirement | `DaemonCompatibilityRequirement::current()` includes `mode_gated_input` in **`required_features`** (same list as today: advertised == required for first-party) |
| Support matrix | `botster_hub_test_support` first-party matrix lists the token in both supported and required features; document ModeGatedInput + freshness fixtures |
| Three-site change | Constant + `current_feature_list()` + support-matrix / npm assets ([[adding a hub client feature constant is a three site change]]) |

**Handshake proofs (required):**

1. **New client vs old Hub:** client built with rev34 / required `mode_gated_input` connecting to Hub advertising protocol 6 **without** `mode_gated_input` → `ensure_compatible` fails **before** any `ModeGatedInput` dispatch (missing required feature).  
2. **Old client vs new Hub:** client requiring only pre-34 features / conf floor ≤33 connects to Hub advertising protocol 6, features including `mode_gated_input`, conf ≥34 → **accepts** (conformance is a floor; extra features OK).  
3. Generated TypeScript + clean installed npm package (`>0.1.28`, candidate **0.1.29**) contain: feature string `mode_gated_input`, `ModeGatedInput` request, gated result fields, `mode_generation` / `mode_revision` on mode flags.

Expanding `ReadModeFlags` with freshness fields is covered by the same conformance rev 34 identity (not a separate protocol bump).

### Hydration (attach / reconnect)

| Step | Action |
| --- | --- |
| H0 | Attach, new `subscription_id` |
| H1 | Buffer `TerminalOutput` until H5 |
| H2 | Receive data-plane events; **import only verified `Snapshot` GHOSTSNP** |
| H3 | `attached` |
| H4 | Install GHOSTSNP into Restty/TUI (scrollback, palette, specials, modes in snapshot). Scrollback events ignored for import. |
| H4b | `ReadModeFlags` for UI + freshness token (not a second color source) |
| H4c | `ReadScreen` optional visible-text supplement |
| H5 | Flush buffered live |
| Reconnect | New subscription; full H0–H5; prior sub gets no later live |

**No** control-path capture step that re-delivers GHOSTSNP or overrides H2/H4 Snapshot.

## Scope

**Implement:**
1. Pin Core to `2c5171a6…` (Implement only)
2. Startup color baseline + default mode-gated timeout composition
3. Expand ModeFlags + freshness; add ModeGatedInput end-to-end
4. **`FEATURE_MODE_GATED_INPUT` + conf rev 34 + matrix/TS/npm** (three-site feature)
5. Snapshot-only GHOSTSNP install contract in docs/fixtures; Scrollback non-import
6. Worker-backed live proofs listed below
7. hub-client `#[non_exhaustive]`; tui cargo patch
8. npm publish `>0.1.28` (candidate **0.1.29**); README + `docs/client-protocol.md`

**Non-scope:** Public control GHOSTSNP; Web/TUI product code; Core reimplementation; Plan-time lockfile edit.

## Exact acceptance proofs (production path)

### Shared setup (every live proof)

```sh
# Implement worktree, clean then:
cargo update -p botster-core -p botster-core-daemon -p botster-core-test-support
# require lock records 2c5171a6… (or newer main with same APIs)
cargo build --locked --bin botster-hub
cargo build --locked -p botster-core-daemon --bin botster-session-worker
# record: Hub SHA = git rev-parse HEAD; Core SHA from Cargo.lock; realpaths under target/
```

Entry point for live proofs: **public `botster-hub-client` over Unix daemon socket** → `daemon_transport` → `HubClientApi` → `HubRuntime` → `CoreDaemon` → worker SessionIo/ClientWorker.

Prefer extending existing external tests in `tests/hub_daemon_lifecycle_test.rs` and unit projection tests in `tests/hub_client_api_test.rs` / `tests/hub_test_support_conformance_test.rs`.

### Required live proofs (name + command)

| Behavior | Production entry | Test target (add/extend) | Command |
| --- | --- | --- | --- |
| Snapshot before live + no Scrollback-as-GHOSTSNP | Attach + Drain events | Extend `external_daemon_same_session_reattach_replays_opaque_history_before_live_output` + new fixture assert | `./test.sh --test hub_daemon_lifecycle_test external_daemon_same_session_reattach_replays_opaque_history_before_live_output` and new exact test name `external_hub_ghostty_snapshot_install_before_live_rejects_scrollback_as_ghostsnp` |
| Reconnect retained scrollback | Socket loss + reattach new sub | Extend reattach test above | same file; assert non-empty GHOSTSNP Snapshot, install ordering, retained marker via ReadScreen after install |
| Kitty flags + gated input | ReadModeFlags + ModeGatedInput | New: `external_hub_mode_gated_kitty_stale_token_rejects_and_reprobe_admits` | `./test.sh --test hub_daemon_lifecycle_test external_hub_mode_gated_kitty_stale_token_rejects_and_reprobe_admits` |
| Mouse flags + gated input | ReadModeFlags + ModeGatedInput | New: `external_hub_mode_gated_mouse_stale_token_rejects_and_reprobe_admits` | `./test.sh --test hub_daemon_lifecycle_test external_hub_mode_gated_mouse_stale_token_rejects_and_reprobe_admits` |
| OSC palette/special mutation in GHOSTSNP | Attach after OSC in session | New: `external_hub_ghostty_snapshot_reflects_osc_palette_and_specials` | prove Snapshot payload non-empty GHOSTSNP after OSC; client must not use Hub startup RGB as current after install |
| OSC 10/11/12 pre-attach, no client, no Hub synthesis | Spawn with baseline profile; child queries; session write_pty | New: `external_hub_osc_101112_session_side_replies_with_startup_baseline` | assert bound RGB for product `#FFFFFF` / `#282C34` / `#FFFFFF`; prove no Hub reply synthesizer path |
| Full ModeFlags projection | ReadModeFlags | Extend `external_hub_client_read_mode_flags_drives_real_daemon_socket_protocol` + `read_mode_flags_returns_exact_authoritative_values…` | include freshness fields |
| Control path has **zero** GHOSTSNP on responses | Grep + contract test | New unit: `daemon_responses_never_include_ghostsnp_payload_base64_on_control_bodies` except documenting that CaptureColorAndSnapshot is **not** shipped | `./test.sh --test hub_client_api_test` / transport tests |
| Conformance fixture | hub-test-support | Update late-attach + mode fixtures; Snapshot-only GHOSTSNP rule | `./test.sh --test hub_test_support_conformance_test` |
| Generated TS + npm | packages/hub-test-support | `npm test`; publish **0.1.29** (or next free `>0.1.28`); external install asserts `mode_gated_input` feature, conf **34**, ModeGatedInput + freshness fields | publish + clean install smoke |
| Compatibility handshake | hub-client ensure_compatible | Unit/integration: new client rejects Hub missing `mode_gated_input`; old client accepts Hub with extra feature + conf 34 at protocol 6 | `./test.sh --test` hub-client / lifecycle compatibility tests |
| Tui Rust source | patch hub-client | scratch cargo patch → `cargo check -p botster-tui` | document in implement report |
| Workspace gate | | | `./test.sh` (full workspace after pin) |
| Live provenance | exact binaries | implement report | Hub SHA, Core `2c5171a6…`, hub + session-worker realpaths, then run the external_hub_* tests with those bins |

### Projection unit tests (non-live, still required)

| File | Focus |
| --- | --- |
| `tests/hub_client_api_test.rs` | ModeFlags expansion + freshness; ModeGatedInput projection; drain Snapshot before live |
| `src/client_api.rs` / `src/daemon_transport.rs` unit modules | operator_error fail-closed; no control GHOSTSNP body |
| `crates/botster-hub-client` | serde + generated TS for new requests |

## Risks

| Risk | Mitigation |
| --- | --- |
| Control-path byte creep | Charter + explicit non-goal + contract test |
| Clients import Scrollback | Fixture reject + docs |
| Weak live coverage | Named external_hub_* matrix above |

## Product decision ledger

| Item | Decision |
| --- | --- |
| GHOSTSNP bytes | **Data-plane Snapshot only** |
| CaptureColorAndSnapshot public | **Not shipped** this ticket |
| Current colors on attach | **GHOSTSNP install only** |
| Startup profile | Baseline / OSC pre-attach only |
| Mode admit | ModeGatedInput + Core 5s timeout |
| Compatibility | Feature `mode_gated_input`; conf **34**; protocol **6** |
| Live proofs | Named external_hub_* tests |

## Finding disposition

| Finding | Disposition |
| --- | --- |
| `finding_1786499237_465402` | Data-plane only; no control GHOSTSNP; named data-plane note |
| `finding_1786499237_182032` | Exact tests/commands matrix |
| `finding_1786499667_886491` | Feature token + conf 34 + protocol 6 + handshake proofs |

## Completion evidence

| Field | Value |
| --- | --- |
| `target_repository` | `botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| `repository_playbook` | [[botster-hub-playbook]] + [[botster-hub-client-playbook]] |
| `data_plane_note` | [[botster data plane bypasses the hub through session and client actors]] |
| `plan_uri` | `docs/plans/expose-one-authoritative-ghostty-terminal-contract-from-hub.md` |
| `checklist_id` | `checklist_1786476742_921432` |
| `implement_core_pin` | `2c5171a6cb3b073c53620a9838d8b08480dd215c` |
| `feature_token` | `mode_gated_input` |
| `conformance_fixture_revision` | `34` |
| `protocol_version` | `6` |
| `teardown_class_applies` | `false` |
