# Verify report: Hub decomposition 4a

## Target and context

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1787894421_128594` |
| Run / step | `run_1787997552_597206` / `botster_stack_verify` (`run_step_1788028137_355687`) |
| Verified HEAD | `468a330eb41d5409ad650d0cbbe1abd6903e25e2` |
| Base | `origin/main` `ddb2de9cdc11a2e3a050e477cf396685686887f2`, also the merge-base |
| Toolchain | `rustc 1.97.0 (2d8144b78 2026-07-07)`, `CARGO_TARGET_DIR` unset |

Routing was resolved independently from the ticket `target_id`, not from the ambient
directory.

## Playbooks and notes loaded

- Role: [[verifier-playbook]], [[botster-verifier-playbook]], [[botster-runtime-verifier-playbook]]
- Repository charter: [[botster-hub-playbook]]
- Class overlay: [[botster runtime teardown lenses]]
- Not loaded, with reason: web, package, and Project Pipelines verifier overlays plus
  [[project-pipelines-playbook]]. The branch changes only Hub Rust transport sources, Hub
  tests, and Hub docs.
- Targeted notes: [[hub moves must extend source scanning guard file lists]],
  [[fixed source guard lists need one ablation per added file]],
  [[a source scanner can stay in cfg test skip mode through end of file]],
  [[region bounded source guards need a required symbol anchor]],
  [[code moves need paired absence and presence source guards]],
  [[exact Rust test ablations require a one test baseline]],
  [[Hub official gates must not set CARGO TARGET DIR]],
  [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]],
  [[Hub suite runs prebuild the session worker before the locked test wrapper]],
  [[verify must recheck resolved findings against the live worktree]],
  [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]],
  [[rejected channel isolation needs a surviving channel positive control]].

## Commands and results

All commands ran from the run worktree with `RUSTUP_TOOLCHAIN=1.97.0` and
`CARGO_TARGET_DIR` unset. `git status --short` was empty before and after every batch.

| # | Command | Exit | Result |
| --- | --- | --- | --- |
| 1 | `cargo fmt --all -- --check` | 0 | clean |
| 2 | `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | 0 | worker prebuilt |
| 3 | `cargo build --locked --bin botster-hub` | 0 | hub prebuilt |
| 4 | `cargo clippy --workspace --all-targets --locked -- -D warnings` | 0 | zero warning or error lines, rerun at final HEAD |
| 5 | `./test.sh --locked` | 0 | `TESTSH_EXIT=0`; 32 result lines, every one `ok`, 0 failed |
| 5a | `webrtc_terminal_output_is_byte_exact` inside run 5 | ok | passed under the full suite, not in isolation |
| 6 | `cargo test --workspace --locked -- --list` at HEAD and at `ddb2de9` | 0 | base 1300, HEAD 1302 |
| 7 | `git --no-pager diff --check origin/main...HEAD` | 0 | clean |
| 8 | PII pattern scan over raw `git diff origin/main...HEAD` | 1 (no match) | 0 hits, known-positive control matched 2 |

Run 5 detail: `hub_daemon_lifecycle_test` 319 passed, 0 failed, 2 ignored, 269.15s. Lib
500 passed. No `FAILED`, `panicked at`, or `failures:` line appears anywhere in the log.

The first `./test.sh --locked` attempt was discarded, not reported. I had started it and
then seeded ablations into `src/` in the same worktree while it ran. The suite spawns
nested `cargo test` children that recompile the crate from the live tree, so that run was
not trustworthy. Run 5 above is a clean serial rerun with an empty working tree
throughout.

## Behavior and production path proved

- Production entry point: `HubDaemon` owns `LocalWebrtcTransport` (`src/daemon.rs:84`,
  constructed at `:118`) and now resolves it through `crate::transport::webrtc`. The
  daemon binary is the runtime user of the moved code.
- Public surface is byte-identical in meaning: `botster_hub::LocalWebrtcError` and
  `botster_hub::LocalWebrtcTransport` keep their crate-root paths. The only crate-root
  `pub` change is the source path of that one re-export.
- Move-only content check: I normalized every non-blank line of the two base sources and
  of the eight new files, stripping visibility modifiers, and compared multisets. 41 lines
  differ on the base side and every one is an import regroup, a rustfmt rewrap, or an
  `include_str!` retarget. No production statement, literal, or constant was lost. The
  three `BOTSTER_HUB_TEST_*` environment names and the three `#[serde(rename_all)]`
  attributes survive unchanged; only their line wrapping moved.
- Proof-name preservation: `--list` leaf-name multisets at base and HEAD differ by exactly
  the two intentional new guards. Zero leaf names were removed, renamed, or reduced.
- No protocol change: `src/client_api_dto/`, `src/daemon/error.rs`, `crates/`, and
  `packages/` are untouched. `Cargo.toml` and `Cargo.lock` are untouched, so the Core pin
  and `webrtc 0.21.0-beta.2` are unchanged.
- No forwarding facade: `git ls-files src` contains neither `local_webrtc.rs` nor
  `webrtc_terminal_adapter.rs`, and no file re-exports a WebRTC symbol from outside
  `src/transport/webrtc/**`.

### Guard liveness, proven by ablation

Every arm restored its file immediately and the tree was clean afterwards. Every `--exact`
filter used the full module path and showed a one-test baseline first.

- Fixed guard list, `tests::production_sources_reject_terminal_drain_and_snapshot_phase_decode`.
  Baseline: 3 tests run, all pass. Seeding `GHOSTSNP` at the top of each of the eight
  `src/transport/webrtc/**` files failed 101 and named that exact file. A still-listed
  control file, `src/transport/unix/adapter.rs`, failed the same way.
- Scanner tail state. Appending `GHOSTSNP` after the final `#[cfg(test)]` block of
  `peer.rs` and of `control_channel.rs` also failed and named each file, so the scanner
  closes skip mode at end of file.
- Ownership guards. Duplicating `struct LocalWebrtcFlowControl` into `peer.rs` failed with
  `found ["control_channel.rs", "peer.rs"]`. Breaking the needle inside the owner file
  failed with `found []`. Seeding `struct GrantRegistry` into `signaling.rs` failed the
  admission guard. Presence and absence are separate live arms.
- Extra-channel reject guards, which now span two files. Three arms failed at three
  distinct assertion lines of `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs`:
  renaming `reject_extra_data_channel` failed at `:137`, changing the reject `eprintln`
  text failed at `:141`, and changing the close-marker comment failed at `:124`. No
  earlier assertion shielded a later one.
- Region anchor. I moved the claim statement out of the `on_data_channel` region while
  leaving the identical literal elsewhere in `peer.rs`. The run failed at `:107`, the
  region `expect`, not at the file-level assertion. The bounded scan is not blind after
  the move.

### Runtime-teardown class

The class applies, and this ticket changes no teardown behavior. I verified preservation
rather than new behavior. These lanes ran and passed inside run 5:
`peer_close_leaves_sibling_peers_working`,
`local_webrtc_peer_close_detaches_terminal_subscriptions`,
`webrtc_peer_rejects_a_second_data_channel`,
`webrtc_peer_rejects_a_second_data_channel_requires_one_shot_claim`,
`local_webrtc_sender_terminal_record_rejects_stale_malformed_and_oversized_evidence`,
`local_webrtc_chunks_oversized_encrypted_daemon_response`, and
`webrtc_terminal_output_is_byte_exact`. The lib target additionally ran
`local_webrtc_close_failure_fail_closed_parks_runtime_and_stops_driver_threads` and
`ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners`, whose sibling-
sacrifice and Core-inventory-sweep oracles I confirmed still live in `peer.rs` beside
their self-scan.

## Review findings

All ten findings on this run are `resolved`. I rechecked each against the live worktree
rather than trusting status text.

| Finding | Recheck |
| --- | --- |
| `finding_1788026828_663575` missing per-file ablations | I reran the whole matrix myself. All eight files plus a control go red and name themselves. |
| `finding_1788026828_613962` plan does not match harness layout | The committed plan at HEAD names `test_support.rs` in assumption 2 and in checks 12-16. |
| `finding_1788026828_144063` plan leaks a local user path | PII scan over the raw branch diff returns 0 hits with a working positive control. |
| `finding_1787999655_294747` must wait for the WebRTC baseline repair | `ticket_1787999248_674913` is closed, `ddb2de9` contains the repair, the branch is a linear descendant, and `webrtc_terminal_output_is_byte_exact` passed under the absolute suite gate. |
| `finding_1787998986_147916` plan evidence omits artifact_id | Plan artifacts and the resync are recorded on the run. |
| `finding_1787998986_615689` guard census inconsistent | I enumerated the guards independently. Nine base expressions map to the post-move targets, and the `src/lib.rs` entry expands to eight files. |
| `finding_1787998985_266740` stale WebRTC 0.20 baseline | `Cargo.toml:38` reads `webrtc = "0.21.0-beta.2"` and the manifest is unchanged on this branch. |
| `finding_1787998985_376877` teardown matrix omits request classes | The plan carries the 11-row matrix and the named oracles all pass. |
| `finding_1787998985_197852` baseline suite not green | Base `ddb2de9` is green by construction of the repair ticket, and HEAD passes absolutely. |
| `finding_1787998985_198296` commit shape weakened | `15b35e3` is one compiling move-only commit. Genuinely new guards land separately in `a808092` and `e02ae38`. |

Human answer `question_1788028021_109295` allowed `test_support.rs` under test-only limits.
The implementation meets them: the module is declared `#[cfg(test)] pub(crate) mod
test_support`, its only importers are `mod tests` blocks in four sibling role files, and it
holds fixtures, builders, and harnesses, not production state, policy, scheduling, or
protocol logic.

## Cross-repository consumer proof

Zero cost, and the cost is zero for a checkable reason rather than by assertion. The two
deleted modules were `pub(crate)` and `private`, so no external crate could name them. The
only crate-root `pub use` change is the source path of an unchanged pair of types.
`crates/` and `packages/` are untouched, so `botster-hub-client` DTOs, generated
TypeScript, and `@trybotster/hub-test-support` fixtures are unchanged. No UI contract, DTO,
serde name, or protocol version moved, so `botster-tui` and `botster-web` were not rebuilt.

## Unverified behavior

- Live browser-to-Hub WebRTC over a real browser. The suite drives real peers and real
  DataChannels in process, but no packaged-browser smoke ran in this step.
- Load behavior on a different host. The absolute suite gate passed here. Another pipeline
  worktree was running its own Hub process during part of this session, so the host was
  busy rather than idle, which strengthens rather than weakens the green result.
- `signaling.rs` has no unit tests of its own. Its coverage is indirect, through the live
  `PeerHarness` lanes in `peer.rs`.

## Remaining risk

- Low. The change is a pure relocation with a proven-live guard family and an identical
  test inventory.
- The residual risk is that a future move breaks one of the two-file guards silently. The
  extra-channel guard now spans `peer.rs` and `subscription_channel.rs`, which is exactly
  the shape that goes blind when a symbol leaves a scanned file. Today it is proven live in
  all three arms.

## Vault gaps

One new capture: `~/knowledge/inbox/2026-08-29-source-ablations-must-not-overlap-a-running-suite.md`.
Seeding ablations into `src/` while `./test.sh --locked` runs can corrupt the suite,
because the Hub suite spawns nested `cargo test` children that recompile the crate from
the live tree. I hit this and discarded the affected run.

The plan's eight gaps stand. Two of them are now confirmed by live evidence rather than by
reasoning: region-bounded scans going blind on a move, and `cargo test --exact` needing the
full module path.
