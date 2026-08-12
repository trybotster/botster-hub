# Implement report: Publish hub-test-support with GHOSTSNP late-attach conformance fixtures

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Worktree | `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1786509361_611999` |
| Branch | `project-pipelines/ticket_1786509361_611999` |
| Base | `89dae7e15a844bcb7411b83b32581121720e23eb` |
| Plan | `docs/plans/publish-hub-test-support-ghostsnp-late-attach-conformance-fixtures.md` rev **3** |
| `teardown_class_applies` | **false** |

## Repository playbook and other playbooks/notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]] (repository ownership charter)
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

Not loaded: [[project-pipelines-playbook]] (product paths / workflow policy not in scope), [[botster runtime teardown lenses]] (not runtime-teardown class).

## Constraints applied before edits

- Work only in the routed botster-hub run worktree; do not edit Web/TUI/Core product code.
- Keep fixture publication + conf revision 35 + package 0.1.30 inside hub ownership; Ghostty export/import only via locked Core pin as a generation/import tool.
- Prefer the smallest change that satisfies the dual-golden plan; no dual-use of complete-v1.
- Prove production path (live idle attach) and import semantics, not only fixture presence.

## Files changed

### Frozen goldens

- `crates/botster-hub-test-support/fixtures/ghostsnp/late-attach-history-marker-v1.ghostsnp` — Golden A
- `crates/botster-hub-test-support/fixtures/ghostsnp/late-attach-blank-v1.ghostsnp` — Golden B
- `crates/botster-hub-test-support/fixtures/ghostsnp/README.md` — generation recipe + pins

| Golden | Role | Len | SHA-256 |
| --- | --- | --- | --- |
| A | history_then_live Snapshot | 1176 | `fc8664159efdc7bd6959dd294485c6bc2f87ad8ea2fb3a0a16ab78b2eb87fd77` |
| B | no_history_then_live Snapshot | 1157 | `b0e28fe69ba590f067236a2a0b1eb8b05d2aa0be74fda4324e55a6066eac328c` |

Generation (locked): Core `2c5171a6cb3b073c53620a9838d8b08480dd215c`, Ghostty `5e9ba17a22ba8e40bf8de7d3e7555b8378cb1880`, size 24×80. A: write `history-before-live\r\n` then export. B: zero writes then export.

### Source / package

- `crates/botster-hub-client/src/lib.rs` — `CONFORMANCE_FIXTURE_REVISION` **35**
- `crates/botster-hub-test-support/Cargo.toml` — include `fixtures/ghostsnp/**`; dev-dep `botster-terminal-ghostty` + `sha2` for import proof
- `crates/botster-hub-test-support/src/lib.rs` — dual goldens, no_history Snapshot(B), provenance API, unit + import tests
- `tests/hub_daemon_lifecycle_test.rs` — `external_hub_idle_attach_emits_ghostsnp_snapshot_before_attached`
- `packages/hub-test-support/*` — version **0.1.30**, conf **35**, regenerated fixtures/matrix/metadata, dual-SHA npm asserts, README
- `docs/client-protocol.md` — rev 35 narrative
- `docs/plans/publish-hub-test-support-ghostsnp-late-attach-conformance-fixtures.md` — approved plan (from Plan step)
- `Cargo.lock` — hub-test-support dev-deps

## Ownership boundaries preserved

| Layer | Owner | This ticket |
| --- | --- | --- |
| Fixtures + npm publish | botster-hub | yes |
| Conf revision (in-workspace crate) | botster-hub-client | 35 |
| Ghostty export/import mechanism | Core / botster-terminal-ghostty | used as locked pin tool only; no Core PR |
| Web/TUI consumers | downstream | pin after close; out of scope |

## Cross-repo dependencies or separately routed work

- Consumers (botster-web ticket_1786471490_562794, TUI) must pin `@trybotster/hub-test-support@0.1.30` / conf 35 after this closes.
- No Core or Ghostty product change required; goldens frozen under existing lock pins.

## Deviations from plan

None material.

- Plan optional: live idle Snapshot SHA may equal Golden B. Live test proves GHOSTSNP presence, ordering, blank ReadScreen, and cleanup; if live dimensions/env differ from the frozen 24×80 generator, SHA equality is not required (documented in test stderr). Both goldens still import with correct semantics under unit tests.
- Authentic GHOSTSNP may embed screen glyphs in binary form, so the old npm assert that UTF-8-decoding Snapshot bytes must not contain `history-before-live` was replaced with magic + SHA identity checks. Visible restore remains the ReadScreen oracle.

## Tests and downstream proof run

```sh
./test.sh --test hub_test_support_conformance_test
# 2 passed

./test.sh -p botster-hub-test-support --lib late_attach
# 9 passed (includes late_attach_ghostsnp_goldens_import_with_semantic_screen_state)

./test.sh --test hub_daemon_lifecycle_test external_hub_idle_attach_emits_ghostsnp_snapshot_before_attached
# 1 passed

./test.sh --test hub_daemon_lifecycle_test external_hub_ghostty_snapshot_install_before_live_rejects_scrollback_as_ghostsnp
# 1 passed

cd packages/hub-test-support && npm test
# hub test-support package import and fixture materialization passed

cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

External install smoke (after publish / clean pack install): package 0.1.30, conf 35, `verifyPackageAssets().ok`, history Snapshot SHA == Golden A, no_history Snapshot SHA == Golden B ≠ A, both sequences attaching < snapshot < attached < terminal_output, empty no_history ReadScreen oracle, single no_history Snapshot.

## PR and package coordinates

- PR: https://github.com/trybotster/botster-hub/pull/208
- Commit: `efff96a`
- Package: `@trybotster/hub-test-support@0.1.30` / conf **35**
- Packed tarball external smoke: **passed** (clean install, dual SHAs, ordering, `verifyPackageAssets`)
- Registry publish: **blocked on OTP** (`npm publish` returned `EOTP`). Human must complete `npm publish packages/hub-test-support` (or packed tarball) with `--otp`. Until then, consumers can use the packed tarball / merged source pin.

## Unverified behavior or residual risk

- Registry coordinate `@trybotster/hub-test-support@0.1.30` is not yet visible on npmjs until OTP publish completes.
- Live idle Snapshot may not byte-equal Golden B under different PTY env; production still emits non-trivial GHOSTSNP before attached with blank ReadScreen.
- libghostty-vt import tests require Zig 0.16.0 (same as session-worker builds).

## Missing vault guidance discovered

1. Shared fixtures must not dual-use a history-bearing GHOSTSNP golden as no-history (plan vault gap; still worth a durable note).
2. External smoke should assert content identity (SHA) per scenario, not only magic (plan vault gap).
3. Authentic GHOSTSNP payloads may contain screen text as binary cell data; “UTF-8 must not include oracle text” is not a valid opaque-payload check.
