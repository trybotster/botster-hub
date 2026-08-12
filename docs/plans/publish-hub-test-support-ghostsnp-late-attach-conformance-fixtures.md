# Plan: Publish hub-test-support with GHOSTSNP late-attach conformance fixtures

Ticket: `ticket_1786509361_611999`  
Run: `run_1786509517_796604`  
Pipeline: `botster_stack_delivery` / step `botster_stack_plan`  
Plan revision: **3** (addresses Plan Review `review_1786510742_952675`)

## Plan Review disposition

| Finding | Severity | Disposition |
| --- | --- | --- |
| `finding_1786510742_166914` complete-v1 cannot serve as no-history | high / product | **Rev 3:** two distinct goldens — history-aligned + blank-screen; separate SHAs; import semantic asserts |
| `finding_1786510322_829031` importable payload | high / product | **Still in force:** import proof required per golden |
| `finding_1786510321_502447` no-history live proof | high / product | **Still in force:** live idle attach test required |
| Process findings from earlier reviews | — | Resolved; reuse checklist; full evidence fields |

### Explicit rev 2 error corrected

Rev 2 incorrectly reused Ghostty **`complete-v1`** for both sequences. That golden is built from a history-bearing 2×3 terminal (history pages A/B, active cells, alternate screen `alternate`, title, cwd). Pairing it with empty `no_history_read_screen_text` teaches a **contradictory** terminal state to GHOSTSNP importers. Rev 3 forbids that.

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn path | `/Users/jasonconigliari/Projects/botster-hub` |
| Base | `89dae7e15a844bcb7411b83b32581121720e23eb` |
| Core lock pin | `2c5171a6cb3b073c53620a9838d8b08480dd215c` |
| Ghostty submodule pin (Core README / vendor) | `5e9ba17a22ba8e40bf8de7d3e7555b8378cb1880` |
| Published floor | `@trybotster/hub-test-support@0.1.29` / conf **34** |
| Candidate package | `@trybotster/hub-test-support@0.1.30` (or next free `>0.1.29`) |
| Conformance revision | **`35`** |
| Protocol | **6** (unchanged) |
| `teardown_class_applies` | **false** |

## Repository playbook loaded

- [[botster-hub-playbook]]

## Other role/surface playbooks and atomic notes loaded

- [[planner-playbook]], [[botster-planner-playbook]]
- [[botster-hub-client-playbook]], [[botster-package-reviewer-playbook]]
- [[conformance fixture revisions must be unique per published content]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[shared conformance fixtures that contradict the core contract teach clients the wrong state machine]]
- [[hub test support npm releases need external consumer smoke]]
- [[published fixture readmes are part of the shipped contract]]
- [[botster first party client support matrices belong in hub test support]]
- [[coredaemon attached follows initial snapshots before live terminal output]]
- [[opaque terminal snapshot bytes do not prove renderable history]]
- [[initial terminal snapshots must precede live output activation]]
- [[botster clients restore visible terminal state from readscreen before buffered live output]]
- [[plugin conformance packages prove shared contracts while examples prove product behavior]]

Not loaded: [[project-pipelines-playbook]], [[botster runtime teardown lenses]]

## Context loaded

### Ticket defect (base still broken)

History Snapshot `AP9HVFkB` → `00ff47545901` is not `GHOSTSNP`. Source `LATE_ATTACH_HISTORY_PAYLOAD`. Package 0.1.29 / conf 34.

### Producer path (unchanged, still true)

1. Subscribe → `request_initial_snapshot` always captures Ghostty snapshot.
2. Ghostty export is non-empty GHOSTSNP.
3. `client_stream` emits `TransportEgress::Snapshot` when bytes non-empty, then `Attached`.
4. Visible text oracle is `ReadScreen`, not opaque payload length ([[botster clients restore visible terminal state from readscreen before buffered live output]]).

### complete-v1 role (rev 3)

| Use | Allowed? |
| --- | --- |
| Sole history fixture payload | **Only if** import-visible state is made consistent with `read_screen_text` (it currently is not for `history-before-live\r\n`) |
| no_history Snapshot | **Forbidden** |
| Optional format/import control in a separate unit test | Allowed, not the shared late-attach scenario bytes |

## Scope

1. **Two deterministic GHOSTSNP goldens** with distinct content identity (generation recipe locked below).
2. Wire late-attach **history** Snapshot → history golden; **no_history** Snapshot → blank golden.
3. Keep both sequences: `attaching → snapshot → attached → live → process_exit`.
4. Keep empty `no_history_read_screen_text`; history keeps `history-before-live\r\n` as the ReadScreen oracle **and** must match imported visible content after Ghostty import of the history golden.
5. Executable **import + semantic** proofs for each golden.
6. Live Hub idle attach production proof.
7. Conf **35**, package **>0.1.29**, asset regen, README + client-protocol, external install smoke with **two SHAs**.

## Non-scope

Web/TUI/Restty product code; Core producer changes; protocol bump; ModeGatedInput redesign; control-path GHOSTSNP; teardown; new vault checklists.

## Locked product decisions (rev 3)

### Golden A — history (`history_then_live` Snapshot)

| Field | Locked value |
| --- | --- |
| Purpose | Authentic GHOSTSNP that, when imported, yields visible state consistent with fixture history oracle |
| Generation (Implement, freeze once) | On Core pin + Ghostty pin above, create Ghostty terminal at fixed size (**24×80**, Hub product default unless Implement documents a required fixture size already used by late-attach helpers). Write exactly `history-before-live\r\n` via the production write path used by Ghostty adapter tests (`write_output` / equivalent). Export GHOSTSNP bytes. |
| Alternate allowed generation | Live Hub: spawn session, write marker, attach, freeze the data-plane Snapshot payload — only if the same Core/Ghostty pins and marker text apply; still freeze committed bytes (no non-deterministic regen at package build). |
| Forbidden | Reusing complete-v1 unless re-import proves visible text matches `history-before-live` (it does not today). |
| Pins after generation | length, SHA-256, Core SHA `2c5171a6…`, Ghostty SHA `5e9ba17a…`, generation recipe comment in source |
| Import proof | Ghostty import **Ok**; imported `screen_state().plain_text` (or Hub `ReadScreen` on a restored runtime) **contains** `history-before-live` and does **not** contain complete-v1 markers such as alternate-screen `alternate` as the sole history story |
| Fixture fields | Snapshot = Golden A; `read_screen_text` = `history-before-live\r\n` |

### Golden B — blank / no-history (`no_history_then_live` Snapshot)

| Field | Locked value |
| --- | --- |
| Purpose | Authentic GHOSTSNP for a **fresh idle** Ghostty terminal with **no prior renderable output** |
| Generation (Implement, freeze once) | Same Core + Ghostty pins. Create Ghostty terminal at same fixed size as Golden A. **Zero** `write_output` / PTY writes. Export GHOSTSNP immediately. |
| Alternate | Live Hub fresh idle attach Snapshot freeze under same pins (still commit frozen bytes). |
| Forbidden | complete-v1 or any golden whose import shows retained history, alternate-screen text, or non-blank content |
| Pins after generation | length, SHA-256 (**must differ from Golden A**), Core SHA, Ghostty SHA, recipe comment |
| Import proof | Ghostty import **Ok**; imported plain text is empty / blank (no `history-before-live`, no `alternate`, no A/B/C/D/E history cells from complete-v1); retained history pages empty if API exposes that |
| Fixture fields | Snapshot = Golden B; `no_history_read_screen_text` = `""` |

### Shared sequence rules

| Sequence | Events |
| --- | --- |
| `history_then_live` | attaching → Snapshot(A) → attached → terminal_output(`live-after-attach\r\n`) → process_exit |
| `no_history_then_live` | attaching → Snapshot(B) → attached → terminal_output(`live-without-history\r\n`) → process_exit |
| Scrollback | absent in both |
| Client hydration | Import GHOSTSNP Snapshot; visible restore via ReadScreen; buffer live until install |

### Live production proof (required)

`external_hub_idle_attach_emits_ghostsnp_snapshot_before_attached`:

1. Real hub + session-worker.
2. Fresh session, **no prior output**.
3. Attach; drain to attached.
4. Order: attaching < Snapshot < attached.
5. Snapshot starts with `GHOSTSNP`, non-trivial length.
6. ReadScreen empty.
7. No Scrollback-as-GHOSTSNP.
8. Session cleanup via production shutdown/remove.

Optional but preferred: assert live idle Snapshot SHA equals Golden B **or** document why live dimensions/env differ while still proving presence of blank GHOSTSNP (if SHAs differ, both must still import as blank).

### Compatibility

| Item | Value |
| --- | --- |
| PROTOCOL_VERSION | 6 |
| CONFORMANCE_FIXTURE_REVISION | 35 |
| npm | 0.1.30 candidate |
| New feature token | none |

## Ownership boundaries

| Layer | Owner | This ticket |
| --- | --- | --- |
| Fixtures + publish | botster-hub | yes |
| Conf revision | botster-hub-client | 35 |
| Ghostty export/import | Core / botster-terminal-ghostty | generate goldens via locked pins; no Core PR required if generation works on pin |
| Consumers | Web/TUI | pin after close |

## Assumptions and unknowns

### Assumptions

1. Fresh Ghostty export with zero writes is blank-importable under the locked pins.
2. Writing only `history-before-live\r\n` yields a stable enough export to freeze (deterministic for fixed size).
3. hub-test-support can take a Ghostty import path via `botster-terminal-ghostty` (dev-dep / workspace test) under `libghostty-vt` if required for import proofs.

### Unknowns for Implement (not product forks)

1. Exact frozen byte length/SHA for A and B (must be recorded before publish).
2. Whether import proof runs under hub-test-support unit tests vs hub lifecycle tests (must be CI-executable).
3. Zig/libghostty-vt availability in CI for import tests — if unavailable in hub CI, freeze goldens generated offline and still prove import in a CI job that has the feature, **or** prove via live Hub attach + ReadScreen agreement without full import API (weaker — prefer real Ghostty import).

If import cannot run in CI, **ask human** before weakening; do not silently drop import proof.

## Affected surfaces / files

- `crates/botster-hub-client/src/lib.rs` — conf 35
- `crates/botster-hub-test-support/src/lib.rs` — two golden constants, scenarios, tests
- `crates/botster-hub-test-support/fixtures/` (optional `.bin` goldens)
- `crates/botster-hub-test-support/Cargo.toml` — Ghostty/core test deps if needed
- `tests/hub_daemon_lifecycle_test.rs` — live idle test
- `packages/hub-test-support/*` — regen, version, README, test.mjs dual-SHA asserts
- `docs/client-protocol.md`, implement report

## Implementation steps

1. Hygiene checks (gitignore, colon path).
2. Generate Golden B (blank) and Golden A (history marker) under Core `2c5171a6…` + Ghostty `5e9ba17a…`; commit frozen bytes + SHAs.
3. Import semantic tests for A and B.
4. Wire late-attach scenarios; conf 35; package regen/version.
5. Live idle attach test green.
6. Docs/README rev 35 narrative (distinct goldens; no complete-v1 dual-use).
7. Publish; external smoke with dual SHAs + ordering.

## Risks

| Risk | Mitigation |
| --- | --- |
| Dual-use golden regression | Two SHAs + semantic import asserts |
| History oracle vs payload mismatch | Import must contain history marker |
| Non-deterministic export | Freeze bytes; pin size/env; unit equality on frozen const |
| libghostty-vt CI gap | Ask human if import CI blocked; do not drop proof |

## Acceptance checks / tests

```sh
# Fixture + SHA + import semantics
./test.sh --test hub_test_support_conformance_test
# (or package-local tests covering golden A/B import)

# Live idle producer
./test.sh --test hub_daemon_lifecycle_test external_hub_idle_attach_emits_ghostsnp_snapshot_before_attached

# History production regression
./test.sh --test hub_daemon_lifecycle_test external_hub_ghostty_snapshot_install_before_live_rejects_scrollback_as_ghostsnp

cd packages/hub-test-support && npm test
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

### External install smoke (required)

Assert:

1. package version published; conf **35**
2. `verifyPackageAssets().ok`
3. history Snapshot SHA == Golden A; magic GHOSTSNP
4. no_history Snapshot SHA == Golden B; **≠** Golden A; magic GHOSTSNP
5. both sequences attaching < snapshot < attached < terminal_output
6. no_history has empty `no_history_read_screen_text` and one snapshot
7. install path is clean node_modules

## Vault gaps

1. Shared fixtures must not dual-use a history-bearing GHOSTSNP golden as no-history.
2. External smoke should assert **content identity (SHA)** per scenario, not only magic.

## Product decision ledger

| Decision | Rev 2 | Rev 3 |
| --- | --- | --- |
| History payload | complete-v1 | **Generated/frozen history marker golden A** (complete-v1 forbidden unless oracle-matched) |
| No-history payload | same complete-v1 | **Distinct blank golden B** |
| Import proof | required | required **per golden + semantic asserts** |
| Live idle | required | required |
| Conf / package | 35 / 0.1.30 | unchanged |

## Checklist policy this visit

- **Reuse** `checklist_1786509815_303220` only — do not create another vault checklist.
- Prior duplicates remain skipped.

## Pipeline evidence fields

- plan_uri, artifact_id, checklist_id, target_id, target_repository (all required on gate + advance)

---

## Plan self-check

- target_repository: `botster-hub`
- target_id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- repository_playbook: [[botster-hub-playbook]]
- notes include ReadScreen hydration + dual-golden constraint
- scope/non-scope, ownership, risks, acceptance (live + dual import + dual SHA smoke), vault gaps: present
- `teardown_class_applies`: false
