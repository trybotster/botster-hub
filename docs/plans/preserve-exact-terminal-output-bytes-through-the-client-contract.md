# Plan: Preserve exact terminal output bytes through the client contract

Ticket: `ticket_1786562565_286591`
Run: `run_1786562586_334049`
Step: `botster_stack_plan`

Plan **revision 2** after Plan Review `review_1786563473_981134` (`changes_required`). Plan worktree is documentation only. Do not mutate `Cargo.lock` at Plan. Reuse plan artifact `artifact_1786563042_418499`. Do not create a second plan artifact.

## Target

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Target path (resolved from spawn target, not CWD) | botster-hub spawn target `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Locked Core revision in this worktree `Cargo.lock` | `5a9938377b492ee1fa3acfb31365ebbebccc2a96` |
| Current protocol | `PROTOCOL_VERSION = 6` |
| Current conformance | `CONFORMANCE_FIXTURE_REVISION = 35` |
| Current support package | `@trybotster/hub-test-support@0.1.30` |
| `teardown_class_applies` | **false** |
| Session-type eligibility consumer | **false** |

This run owns only `botster-hub`. Do not implement Web or TUI consumption here.

## Plan Review corrections (rev 2)

| Finding | Decision |
| --- | --- |
| `finding_1786563474_520298` worker-backed live proof | Locked README worker build plus distinct Hub/Core provenance before `./test.sh`. Load [[live hub proof records distinct hub and locked core binary provenance]]. |
| `finding_1786563474_221644` split UTF-8 sleep | Replace sleep with a producer barrier. Drain the first exact `[0xE2]` frame, then release `[0x82, 0xAC]`. Assert both payload boundaries. |
| `finding_1786563474_675328` npm gate | No root `npm test`. Install `@trybotster/ui-contract@0.3.2` with the repo no-lock command, then prefix check/test. |
| `finding_1786563474_855811` empty step.completed evidence | Process-only. Keep this artifact. Submit structured `plan_uri`, `artifact_id`, `checklist_id`, `target_id`, and `target_repository` on gate and step advance. |

## Playbooks and notes loaded

### Role and repository charters

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-hub-playbook]] — exact repository ownership charter
- [[botster-hub-client-playbook]] — ticket changes the external daemon DTO; crate lives in this repo
- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]]
- [[botster-runtime-reviewer-playbook]] — Review/Verify overlay for daemon/transport work

Not loaded:

- [[project-pipelines-playbook]] — this ticket does not change Project Pipelines package or plugin paths
- [[botster runtime teardown lenses]] — this ticket is a contract encoding change, not WebRTC/peer lifecycle, SessionIo/ClientWorker teardown, multi-peer ownership, CPU/battery/FD spin, or terminal-state vs live-runtime teardown divergence

### Targeted atomic notes

- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster hub client crate is the external client boundary]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[botster terminal clients share one sessionio data plane subscription path]]
- [[botster durable terminal egress is owned by sessionio and clientworker actors]]
- [[botster clients restore visible terminal state from readscreen before buffered live output]]
- [[coredaemon attached follows initial snapshots before live terminal output]]
- [[initial terminal snapshots must precede live output activation]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[conformance fixture revisions must be unique per published content]]
- [[hub generated protocol changes are a four site release chain]]
- [[hub test support npm releases need external consumer smoke]]
- [[closed dependency tickets signal merged source not a consumable release]]
- [[adding a hub client feature constant is a three site change]]
- [[generated typescript dtos must encode serde field optionality]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[cross repo dependency registration must use dependency repo target]]
- [[cold turkey migrations eliminate dual code paths and version suffixes]]
- [[webrtc peer registry owns production data plane receivers]]
- [[live hub target dirs can cache stale same version client schema]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[botster session worker requires explicit build in dogfood launchers]]
- [[external client hub tests use subprocess spawned hub test support]]
- [[rust repo strict lints must be verified before dismissing warnings]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[identity]]
- [[goals]]

## Context loaded

Project: `project_1786468118_227513` — Botster Ghostty-only terminal cutover.

Closed parent Hub ticket `ticket_1786471489_718500` already made Snapshot/Scrollback byte-faithful and locked attach order. Live `DaemonEvent::TerminalOutput` still uses a UTF-8 `data` string.

Sibling consumer tickets already exist and now depend on this ticket:

- Web `ticket_1786562565_267926` on `tgt_40abcf71ccf049f4ac0c99953a799869` — decode Hub live output into Restty
- TUI `ticket_1786562566_712634` on `tgt_c3d470bab78549df920a41e8fb0e58d8` — consume Hub live output in the thin Ghostty client

Current production facts in this worktree:

1. Core `TransportEgress::TerminalOutput.data` is `Vec<u8>`.
2. `HubClientEvent::TerminalOutput.data` is already `Vec<u8>`.
3. `src/daemon_transport.rs` `daemon_event_from_client` converts those bytes with `String::from_utf8_lossy`.
4. `botster_hub_client::DaemonEvent::TerminalOutput` serializes `{ type, session_id, subscription_id, data: string }`.
5. Snapshot/Scrollback already use `payload_base64`, `payload_encoding: "base64"`, and `bytes` via `DaemonOpaqueHistoryPayload`.
6. Generated TypeScript and `@trybotster/hub-test-support` still type `terminal_output.data` as `string`.
7. Late-attach fixtures still carry `"data": "live-after-attach\\r\\n"`.
8. `write_terminal_events` / `stream_attach` write `data.as_bytes()` of that UTF-8 string.
9. Local WebRTC encrypted Drain carries the same `DaemonEvent` JSON. It inherits the lossy conversion.

The production mutation site is the daemon-event projection, not Core and not `HubClientEvent`.

## Scope

Replace the live-output client contract so Hub never decodes PTY bytes as text.

Locked product shape:

```text
DaemonEvent::TerminalOutput {
  session_id,
  subscription_id,
  #[serde(flatten)]
  payload: DaemonLiveOutputPayload {
    payload_base64,                 // standard padded base64
    payload_encoding: "base64",     // only accepted value
    bytes,                          // exact decoded length
  }
}
```

Required work in this repository:

1. Add `DaemonLiveOutputPayload` in `crates/botster-hub-client`.
   - Same validation as Snapshot/Scrollback: reject invalid base64, unknown encodings, and length mismatch.
   - Share the private decode helper. Do not flatten `DaemonOpaqueHistoryPayload` onto live output. That type is documented as opaque engine state that must not be rendered.
2. Replace `DaemonEvent::TerminalOutput.data: String` with the flattened payload. Delete the string field. No dual shape. No `data` fallback. No `_v2` name.
3. Change `daemon_event_from_client` to `DaemonLiveOutputPayload::from_bytes(&data)`. Remove `String::from_utf8_lossy` from this path.
4. Change `write_terminal_events` to write `payload.decoded_bytes()`, not UTF-8 of a display string.
5. Update the TypeScript emitter so `terminal_output` matches Snapshot field names and types.
6. Bump compatibility identity (locked below).
7. Update docs, fixtures, support-matrix derivation, and package metadata.
8. Prove the production path listed in Acceptance.
9. Synchronize `packages/hub-test-support` and publish only through the documented operator step.

## Non-scope

- Web or TUI decode/render work. Those stay on the registered consumer tickets.
- Core PTY reader, SessionIo, or ClientWorker frame ownership. Core already emits `Vec<u8>`.
- `SendInput` / `ModeGatedInput` string payloads.
- Snapshot/Scrollback field names or GHOSTSNP import rules.
- `ReadScreen.text` visible restoration.
- New feature token. `FEATURE_TERMINAL_STREAMING` still names streaming. Protocol 7 is the fail-closed handshake signal.
- WebRTC peer lifecycle, teardown, or queue policy.
- Session-type eligibility, spawn Option A, or hub-test-support 0.1.26 / conf 33 parent pins.
- Optional configurability, dual decoders, or “text if valid UTF-8” shortcuts.
- Unrelated `from_utf8_lossy` uses (operator stdin, command output, update logs).

## Compatibility identity (locked)

This is a cold-turkey field replacement, not an additive event field.

[[daemon event shape changes bump conformance fixture revision not protocol version]] allows a conformance-only bump for additive event-shape work. [[botster-hub-client-playbook]] also requires an unchanged client to accept a newer conformance revision at the **same** protocol. An unchanged protocol-6 client cannot deserialize the new `terminal_output` shape. Keeping protocol 6 would make handshake succeed and the first live frame fail.

Therefore:

| Item | Locked value |
| --- | --- |
| Protocol | **`PROTOCOL_VERSION = 7`** |
| Conformance | **`CONFORMANCE_FIXTURE_REVISION = 36`**, then re-read the newest published artifact at Implement time and allocate strictly above every published meaning |
| Support package | **`@trybotster/hub-test-support@0.1.31`**, or the next unpublished coordinate after a registry check |
| Feature token | **Do not add** |
| Old `data` JSON | **Reject**. No dual decoder |

Unchanged protocol-6 clients must fail at `ensure_compatible()`, not on the first live frame.

Implement must re-query the published npm coordinate before choosing the package version and before claiming uniqueness for revision 36. Do not reuse a published revision for different bytes.

## Repository ownership and cross-repo dependencies

| Layer | Owner | This run |
| --- | --- | --- |
| PTY bytes / SessionIo / ClientWorker | botster-core | Prerequisite already true. No Core ticket. |
| Host projection of those bytes onto the public daemon contract | botster-hub | This ticket |
| Public DTO, generated TS, fixtures, npm support package | botster-hub-client crate inside botster-hub | This ticket |
| Browser decode into Restty | botster-web `tgt_40abcf71ccf049f4ac0c99953a799869` | Consumer `ticket_1786562565_267926` |
| TUI Ghostty client consume | botster-tui `tgt_c3d470bab78549df920a41e8fb0e58d8` | Consumer `ticket_1786562566_712634` |

Registered dependencies:

- `ticket_1786562565_267926` depends on `ticket_1786562565_286591`
- `ticket_1786562566_712634` depends on `ticket_1786562565_286591`

Do not silently broaden this run into those consumer worktrees. Downstream consumption waits for Hub merge **and** a published support coordinate. [[closed dependency tickets signal merged source not a consumable release]].

Measure Rust DTO breakage with scratch cargo patch redirects against TUI and Web. Record the exact compile failures. Do not commit consumer patches here.

## Architecture

Hub remains the control-plane host profile. It must not become a byte decoder.

```text
PTY -> SessionIo -> ClientWorker -> TransportEgress { data: Vec<u8> }
  -> HubClientEvent::TerminalOutput { data: Vec<u8> }          # already exact
  -> daemon_event_from_client -> DaemonEvent::TerminalOutput   # TODAY: from_utf8_lossy
  -> daemon socket JSON / encrypted WebRTC Drain JSON
  -> generated TypeScript / hub-test-support / first-party clients
```

After this ticket, the projection step encodes exact bytes as validated base64. WebRTC does not get a second encoding. It carries the same `DaemonEvent` JSON.

`stream_attach` stays a convenience writer of live bytes only. After the change it writes decoded payload bytes. Clients that need event kind or ordering continue to use `Attach` + `Drain`.

## Implementation steps

1. Add `DaemonLiveOutputPayload` next to `DaemonOpaqueHistoryPayload` in `crates/botster-hub-client/src/lib.rs`.
2. Replace the `TerminalOutput` DTO field and every in-repo constructor/match.
3. Point `daemon_event_from_client` at `from_bytes`.
4. Point `write_terminal_events` at `decoded_bytes()`.
5. Update `crates/botster-hub-client/src/typescript.rs` and regenerate `generated/daemon-protocol.ts`.
6. Set `PROTOCOL_VERSION = 7` and `CONFORMANCE_FIXTURE_REVISION = 36` (or the revalidated unique revision).
7. Update `docs/client-protocol.md` so live output is payload bytes, not `TerminalOutput.data` text.
8. Update late-attach fixtures and any other `terminal_output` JSON that still has `data`.
9. Run `npm run sync` in `packages/hub-test-support`. Bump `package.json` / README / metadata.
10. Update production tests named below. Convert string `data.contains(marker)` helpers to decoded-byte predicates. The split-UTF-8 test must use the producer barrier, not a sleep.
11. Run scratch cargo patch redirects against TUI and Web. Record the break list for those consumer tickets.
12. Publish only through `script/publish-npm-packages` when operator credentials exist. If credentials are absent, leave the package prepared and record the operator publish as remaining Hub work. Do not treat a local sync as a published coordinate.

## Affected surfaces / files

- `crates/botster-hub-client/src/lib.rs`
- `crates/botster-hub-client/src/typescript.rs`
- `crates/botster-hub-client/generated/daemon-protocol.ts`
- `src/daemon_transport.rs` (`daemon_event_from_client` and tests)
- `src/main.rs` (WebRTC smoke that concatenates `TerminalOutput.data`)
- `src/client_api.rs` only if comments or re-exports claim a string live-output contract. Keep `HubClientEvent` as `Vec<u8>`.
- `docs/client-protocol.md`
- `packages/hub-test-support/**` including `daemon-protocol.ts`, `metadata.json`, `package.json`, README, late-attach fixture
- `crates/botster-hub-test-support` fixture materializers
- `tests/hub_daemon_lifecycle_test.rs`
- `tests/hub_client_api_test.rs`
- `packages/hub-test-support/first-party-client-support-matrix.json` if source-derived revision/protocol change it

## Assumptions

- Core lock `5a9938377b492ee1fa3acfb31365ebbebccc2a96` already emits exact `Vec<u8>` live frames. Implement must reconfirm that symbol on the locked crate before coding. If Core has started decoding, stop and register a Core ticket on `tgt_1f7bce66eb304881980f9b4a2a5ae3fe`.
- Protocol 7 is required. Conformance 36 is the planned unique revision above published 35.
- No new required feature token.
- `DaemonLiveOutputPayload` is the public type name. Field names stay `payload_base64` / `payload_encoding` / `bytes`.
- WebRTC proof uses the existing encrypted local-WebRTC Drain path. No new peer lifecycle.
- npm publish may require a human operator. Prepared local 0.1.31 is not a consumable release.
- This ticket is not a consumer of Hub session-type eligibility work.

## Unknowns

- Exact unpublished npm coordinate at Implement time. Re-read the registry.
- Exact TUI/Web compile-break list. Measure with scratch patch redirects; do not guess.
- Whether any first-party matrix comment still says “live output is UTF-8 text”. Sweep during Implement.

## Risks

- Protocol 7 is a stack-wide handshake flag day. Downstream tickets must pin protocol 7. That is intended.
- Leaving protocol at 6 would violate the unchanged-client-at-same-protocol floor and fail later on the first live frame.
- Reusing `DaemonOpaqueHistoryPayload` on live output would teach clients that live bytes are opaque history and must not be rendered.
- Keeping any `data` decoder would violate the ticket and [[cold turkey migrations eliminate dual code paths and version suffixes]].
- Fixture revision collision if another branch publishes 36 first. Reallocate above published history.
- Publishing from a stale tree can ship old protocol bytes under a new version. Use the documented publish script and an external install smoke.
- `collect_attach_events` and WebRTC smokes that call `data.contains(...)` will not compile until they decode payload bytes.
- Same-version hub-client target-dir caches can hide DTO changes during live smokes. Refresh `BOTSTER_LIVE_HUB_TARGET_DIR` after the shape change.
- A clean checkout can fail worker-backed `./test.sh` when `botster-session-worker` is absent. `ensure_session_worker_binary` is not provenance. Record Hub SHA and lockfile Core SHA separately.
- A brief sleep before the second PTY write can pass a split-UTF-8 test when both bytes share one frame or when unrelated output supplies the second frame.

## Acceptance checks / tests

Prove the changed production path. Code existence is not enough.

### Unit / crate

- Serialize and deserialize exact bytes: empty, ASCII, NUL `0x00`, ESC CSI, invalid UTF-8 (`0xFF`, `0xC0`), and a 3-byte UTF-8 scalar.
- Two separate live events `[0xE2]` then `[0x82, 0xAC]` concatenate to the euro-sign bytes. Hub must not insert `U+FFFD`.
- Reject invalid base64, unknown `payload_encoding`, and `bytes` mismatch. Mirror current history-payload tests.
- Reject legacy `{ "type": "terminal_output", "data": "..." }` JSON.
- `write_terminal_events` writes decoded payload bytes and still ignores Snapshot/Scrollback.
- `PROTOCOL_VERSION == 7`. A protocol-6 requirement fails `ensure_compatible()` against the new hello/status.
- A protocol-7 client with minimum conformance 35 still accepts hub conformance 36.
- Generated TypeScript `terminal_output` has `payload_base64`, `payload_encoding: "base64"`, `bytes: number`, and no `data`.
- `node packages/hub-test-support/scripts/sync-assets.mjs --check` is clean.

### Live Hub / worker setup and provenance (required before worker-backed tests)

Independent Plan Review ran `./test.sh` on a fetched base and failed four worker-backed tests because `botster-session-worker` was absent. A plain Hub build does not produce that Core binary ([[botster session worker requires explicit build in dogfood launchers]]).

Before any worker-backed production proof or `./test.sh`:

```sh
# Hub binary from this checkout.
cargo build --locked --bin botster-hub

# README-owned locked worker. Do not use -p botster-core here.
# This repo's worker target is botster-core-daemon.
cargo build --locked -p botster-core-daemon --bin botster-session-worker
```

Then record, in Implement/Verify evidence:

| Identity | Source | Must prove |
| --- | --- | --- |
| Hub SHA | `git rev-parse HEAD` of the tested Hub checkout | The `botster-hub` binary came from this SHA |
| Hub binary realpath | resolved `target/.../botster-hub` | Realpath lives under this checkout target dir |
| Locked Core SHA | `Cargo.lock` `botster-core` git `#` revision | Distinct from the Hub SHA |
| Worker binary realpath | resolved `target/.../botster-session-worker` | Realpath lives under this checkout target dir; source identity is the lockfile Core SHA, not the Hub SHA |

Do not assign the Hub SHA to the worker because both files sit in the same `target/` directory. Worker-backed tests must pass that explicit worker path (`--session-worker-bin` / `IsolatedHubBuilder::session_worker_bin`). `ensure_session_worker_binary()` may build the binary; it does not satisfy provenance recording.

### Production path (required)

Entry point: the provenance-pinned `botster-hub` + `botster-session-worker` + `botster_hub_client::DaemonConnection` Attach/Drain. Same topology as `external_hub_ghostty_snapshot_install_before_live_rejects_scrollback_as_ghostsnp`.

Named proofs to add or extend in `tests/hub_daemon_lifecycle_test.rs`:

1. `external_hub_live_output_preserves_exact_bytes`
   - Spawn a session that writes known arbitrary bytes, including NUL, ESC, and invalid UTF-8, with a process `write(2)` (not a UTF-8 `String` helper).
   - Collect live `TerminalOutput` payloads by `decoded_bytes()`.
   - Assert exact equality with the written sequence.
2. `external_hub_live_output_preserves_split_utf8_frames`
   - Use a deterministic producer barrier. Do not sleep.
   - Spawn a session that writes `[0xE2]`, flushes, then blocks until the test releases it (stdin line or equivalent explicit token).
   - Drain until one decoded live payload is exactly `[0xE2]`. Fail if that first fragment never arrives as its own payload.
   - Then release the producer so it writes `[0x82, 0xAC]`.
   - Drain until a later decoded live payload is exactly `[0x82, 0xAC]`.
   - Assert those two payload boundaries in order. Concatenation equals `[0xE2, 0x82, 0xAC]`.
   - Fail if both fragments arrive in one frame. Fail if a second live frame appears before the release. Fail if any relevant frame is UTF-8 replacement `EF BF BD`.
3. `external_hub_live_output_keeps_ghostsnp_then_attached_then_bytes`
   - Event order remains attaching, GHOSTSNP Snapshot, attached, then live byte frames.
   - Snapshot still starts with `GHOSTSNP`.
   - Live frames use the new payload fields only.
4. `external_hub_webrtc_live_output_preserves_exact_bytes`
   - Same arbitrary-byte payload over the production local-WebRTC encrypted Drain path used by the existing WebRTC smoke in `src/main.rs`.
   - Use the same provenance-pinned Hub and worker binaries.
   - Assert decoded live frames match. Do not concatenate UTF-8 strings.

Existing Ghostty ordering test `external_hub_ghostty_snapshot_install_before_live_rejects_scrollback_as_ghostsnp` must stay green after the DTO change.

Ablation: revert only `daemon_event_from_client` to `from_utf8_lossy` and show proofs 1 and 2 go red.

### Downstream-shaped proof this Hub run must produce

- Scratch cargo patch redirect of TUI (`tgt_c3d470bab78549df920a41e8fb0e58d8`) and Web (`tgt_40abcf71ccf049f4ac0c99953a799869`) onto the local hub-client change. Record compile failures. Do not fix those repos here.
- After publish, clean install of `@trybotster/hub-test-support@<new>` and assert:
  - `metadata.protocol_version === 7`
  - `metadata.conformance_fixture_revision` equals the allocated unique revision
  - generated TS contains `payload_base64` on `terminal_output` and does not type `data: string`
  - late-attach fixture live events use payload fields
  - history Snapshot still has GHOSTSNP magic

If npm credentials are missing, Implement records that publish is operator-blocked and does not claim a consumable release.

### Repo gates

Run in this order:

1. Locked worker + Hub build from the provenance section.
2. `./test.sh` after those binaries exist. Do not treat a missing-worker failure as unrelated.
3. `cargo fmt --all -- --check`
4. `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
5. Package Node gates on a clean checkout:

```sh
npm --prefix packages/hub-test-support install --no-save --package-lock=false @trybotster/ui-contract@0.3.2
npm --prefix packages/hub-test-support run check
npm --prefix packages/hub-test-support test
```

Do not run `npm test` at the repository root. There is no root `package.json`.

Worktree path has no `:`. Keep tracked `.gitignore`. Do not truncate it.

## Vault gaps

Capture after Implement if the new contract holds:

- Update [[botster clients restore visible terminal state from readscreen before buffered live output]]. It still says `TerminalOutput.data` is renderable text.
- New note candidate: live terminal output uses the same validated base64 envelope as opaque history, but the bytes are renderable PTY output and must be concatenated without UTF-8 repair.

Do not capture those notes from Plan. The code has not changed yet.

## Product decision ledger

| Item | Decision |
| --- | --- |
| Envelope | JSON + standard padded base64. No raw binary daemon frame. |
| Field names | `payload_base64`, `payload_encoding`, `bytes` |
| Dual `data` field | Forbidden |
| Protocol | 7 |
| Conformance | 36, revalidated against published history |
| Feature token | None |
| Teardown lenses | Not applicable |
| Downstream implement | Separate tickets, now formally dependent |
| Ask-human threshold | Stop only if locked Core no longer emits `Vec<u8>`, or if published npm already used revision 36 / 0.1.31 for different bytes |
