# Implement report: Preserve exact terminal output bytes through the client contract

Ticket: `ticket_1786562565_286591`
Run: `run_1786562586_334049`
Step: `botster_stack_implement`

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`)
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Worktree: the ticket run worktree on `project-pipelines/ticket_1786562565_286591`
- `teardown_class_applies`: false

## Repository playbook and other playbooks/notes applied

Playbooks:

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster-hub-client-playbook]]

Not loaded:

- [[project-pipelines-playbook]] — no Project Pipelines package/plugin path changes
- [[botster runtime teardown lenses]] — approved plan is not runtime-teardown class

Targeted notes included the approved plan list: Hub host-profile and data-plane boundaries, hub-client DTO/compatibility notes, cold-turkey replacement, protocol vs conformance, four-site release chain, distinct Hub/Core provenance, subprocess live-hub tests, generated TS optionality, and scratch cargo-patch downstream measurement.

## Files changed

- `crates/botster-hub-client/src/lib.rs` — `DaemonLiveOutputPayload`, cold-turkey `TerminalOutput` replacement, protocol 7 / conformance 36, unit proofs, retired `data` key rejection on an otherwise valid envelope
- `README.md` — runtime example reports protocol 7 / conformance 36
- `crates/botster-hub-client/src/typescript.rs` and `generated/daemon-protocol.ts`
- `src/daemon_transport.rs` — `daemon_event_from_client` uses `from_bytes`; projection test
- `src/main.rs` — WebRTC smoke and operator event printer use decoded payload bytes
- `crates/botster-hub-test-support/src/lib.rs` — late-attach live events and fixture JSON
- `tests/hub_daemon_lifecycle_test.rs` — decoded-byte predicates and four production proofs
- `docs/client-protocol.md`
- `docs/plans/preserve-exact-terminal-output-bytes-through-the-client-contract.md`
- `packages/hub-test-support/**` prepared as `@trybotster/hub-test-support@0.1.31`

`HubClientEvent::TerminalOutput.data` remains `Vec<u8>`. Unrelated `from_utf8_lossy` sites were left alone.

## Ownership boundaries preserved

Hub only projects Core `Vec<u8>` live frames onto the public daemon DTO. SessionIo/ClientWorker byte ownership stays in locked Core (`5a9938377b492ee1fa3acfb31365ebbebccc2a96`). External DTO ownership stays in the in-repo `botster-hub-client` crate. Web and TUI consumption were not implemented here.

## Cross-repo dependencies or separately routed work

Already registered:

- Web `ticket_1786562565_267926` on `tgt_40abcf71ccf049f4ac0c99953a799869`
- TUI `ticket_1786562566_712634` on `tgt_c3d470bab78549df920a41e8fb0e58d8`

Those tickets still need Hub merge **and** a published `@trybotster/hub-test-support@0.1.31` coordinate. Local package sync is not a consumable release.

## Deviations from plan

- Split-UTF-8 producer uses a file token rather than a stdin line. Plan allowed “stdin line or equivalent explicit token”; stdin raced with PTY open/EOF.
- Short-lived write(2) sessions accept `ShutdownSession` `Events` or `SessionCleanup`.
- npm publish is operator-blocked (`npm whoami` returned 401). Package 0.1.31 is prepared only.
- Web has no Rust `botster-hub-client` crate. Downstream proof used the generated-protocol drift check instead of a cargo patch.

No accepted product-scope change. Plan acceptance checks were not rewritten.

## Tests and downstream proof run

Provenance before worker-backed tests:

- Hub checkout SHA at implement start: `6c48a9342bf78bbffa474a0dc94b25a3522febb2` (pre-commit)
- Locked Core SHA from `Cargo.lock`: `5a9938377b492ee1fa3acfb31365ebbebccc2a96`
- Hub binary realpath under this checkout `target/debug/botster-hub`
- Worker binary realpath under this checkout `target/debug/botster-session-worker`
- Builds: `cargo build --locked --bin botster-hub` and `cargo build --locked -p botster-core-daemon --bin botster-session-worker`

Repo gates:

- `./test.sh` — passed after fixture JSON update
- `cargo fmt --all -- --check` — passed
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` — passed
- `npm --prefix packages/hub-test-support install --no-save --package-lock=false @trybotster/ui-contract@0.3.2`
- `npm --prefix packages/hub-test-support run check` — passed
- `npm --prefix packages/hub-test-support test` — passed

Named production proofs:

- `external_hub_live_output_preserves_exact_bytes`
- `external_hub_live_output_preserves_split_utf8_frames`
- `external_hub_live_output_keeps_ghostsnp_then_attached_then_bytes`
- `external_hub_webrtc_live_output_preserves_exact_bytes`
- Existing `external_hub_ghostty_snapshot_install_before_live_rejects_scrollback_as_ghostsnp` stayed green

Ablation: temporarily projected live bytes through `String::from_utf8_lossy` in `daemon_event_from_client`. Exact-bytes and split-UTF-8 proofs failed (`0xFF 0xC0` became U+FFFD; `[0xE2]` became `EF BF BD`). Projection restored.

Downstream-shaped proof:

- TUI scratch worktree at `fbe6cbc37b43f619fc3ff521cb7d5bd1d783abf1`, cargo patch of local `botster-hub-client` + `botster-ui-contract`. Production compile failed at `DaemonEvent::TerminalOutput { data }` in `crates/botster-tui/src/app.rs` (~3748) and test constructors (`data` field gone; `payload` required). `DaemonDiagnosticKind::WorkerCompatibility` is an additional exhaustive-match break from the newer client crate.
- Web has no Rust hub-client crate. `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL=<generated daemon-protocol.ts> node scripts/check-daemon-protocol-drift.mjs` failed: vendored `terminal_output` still types `data: string`.

## Review return (`review_1786566696_871984`)

- `finding_1786566696_941039`: reject retired `data` on a valid current envelope. `UncheckedDaemonLiveOutputPayload` now fails when the leftover `data` key is present and still ignores unrelated unknown fields. Test `live_output_rejects_retired_data_key_on_an_otherwise_valid_envelope` starts from a serialized current event, adds only `data`, and requires deserialization to fail.
- `finding_1786566696_629344`: README runtime example now reports `protocol_version=7` and `conformance_fixture_revision=36`. `readme_runtime_example_reports_current_protocol_and_conformance` asserts those literals stay current.

## Unverified behavior or residual risk

- `@trybotster/hub-test-support@0.1.31` is unpublished. External install smoke was not run.
- Ablation was restored; the committed tree is the exact-byte projection.
- Protocol 7 is a stack-wide handshake flag day for first-party clients.

## Missing vault guidance discovered

Captured to vault inbox:

- live terminal output uses a validated base64 envelope whose decoded bytes are renderable PTY output
- amendment that [[botster clients restore visible terminal state from readscreen before buffered live output]] still names `TerminalOutput.data`
