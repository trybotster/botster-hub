# Implement report: Hub decomposition 4a

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1787894421_128594` |
| Run | `run_1787997552_597206` |
| Step | `botster_stack_implement` (`run_step_1788026853_755448`, review-return) |
| Approved plan | `docs/plans/hub-decomposition-4a-split-webrtc-by-channel-role.md` implementation resync after `review_1788026828_608920` |
| Merge policy | direct into `main`; do not create a PR |
| Base | `origin/main` `ddb2de9cdc11a2e3a050e477cf396685686887f2` |
| Move commit | `15b35e3` |
| Ownership-guard commit | `a808092` |
| Extra-channel source retarget | `e02ae38` |
| Review-return commit | recorded with this report |
| Runtime-teardown class | applies; every lens is preserved as a survive-the-move invariant |

Independent routing: `project_pipelines_current_context` ticket/run `target_id` and `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. The approved plan used the same routing. Implementation stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]

[[project-pipelines-playbook]] was not loaded. This ticket changes no Project Pipelines package, plugin, or workflow-policy path.

### Targeted atomic notes

- [[daemon transport extraction moves ownership before deleting the facade]]
- [[Hub extraction must reduce ownership rather than only split files]]
- [[hub moves must extend source scanning guard file lists]]
- [[code moves need paired absence and presence source guards]]
- [[fixed source guard lists need one ablation per added file]]
- [[a source scanner can stay in cfg test skip mode through end of file]]
- [[region bounded source guards need a required symbol anchor]]
- [[exact Rust test ablations require a one test baseline]]
- [[WebRTC DataChannel local close uses the peer close bound before cleanup]]
- [[a ready WebRTC send must win over a queued DataChannel close]]
- [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]
- [[webrtc peer cleanup removes every per peer owner together]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[implementation deviations must resync committed plan acceptance checks]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[botster review and verify must scan all committed artifacts for pii]]
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[test script required for rust tests not cargo test]]

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Follow the approved plan. Keep Hub host-policy ownership.
- Do not move the daemon owner loop, control dispatch, or admission.
- Keep the current single `botster-daemon` DataChannel.
- Delete `local_webrtc.rs` and `webrtc_terminal_adapter.rs` with no forwarding facade.
- Do not change the Core pin, DTO shapes, serde names, protocol version, or existing proof leaf names.
- Run every Rust gate with `RUSTUP_TOOLCHAIN=1.97.0` and `CARGO_TARGET_DIR` unset.

## Files changed

| Path | Why |
| --- | --- |
| `src/transport/webrtc.rs` | Module root: error taxonomy, re-exports, ownership guards |
| `src/transport/webrtc/peer.rs` | Peer lifecycle, close bounds, handler, live PeerHarness tests |
| `src/transport/webrtc/signaling.rs` | Offer/answer, `issue_bootstrap`, `signal` |
| `src/transport/webrtc/control_channel.rs` | Single `botster-daemon` channel loop and scheduling |
| `src/transport/webrtc/subscription_channel.rs` | Extra-channel reject path and attach-subscription records |
| `src/transport/webrtc/delivery.rs` | Sealing, framing, chunking |
| `src/transport/webrtc/adapter.rs` | Former `webrtc_terminal_adapter.rs` (git rename, 99%) |
| `src/transport/webrtc/test_support.rs` | Shared `cfg(test)` harness |
| `src/local_webrtc.rs` | Deleted |
| `src/webrtc_terminal_adapter.rs` | Deleted (renamed into adapter.rs) |
| `src/transport.rs` | `pub(crate) mod webrtc` |
| `src/lib.rs` | Module list, `pub use`, architecture_summary, forbidden-construct file list |
| `src/daemon.rs`, `src/daemon_transport.rs`, `src/admission/unix_hello.rs`, `src/subscription/attach_routes.rs` | Import path retarget |
| `src/host_control_fair_write.rs`, `src/transport/shared.rs` | Guard `include_str!` retarget |
| `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` | `hub_source` retarget; extra-channel reject assertions span peer + subscription_channel |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | Adapter production `include_str!` retarget; sibling test-file scan unchanged |
| `tests/hub_daemon_lifecycle/event_plane_saturation.rs` | `read_to_string` retarget to adapter.rs and peer.rs |
| `docs/plans/hub-decomposition-4a-split-webrtc-by-channel-role.md` | Plan from earlier pipeline visits |
| `docs/reports/hub-decomposition-4a-split-webrtc-by-channel-role-implement.md` | This report |

## Test allocation

Moved leaf names are unchanged. Module paths changed.

| Owner file | Tests |
| --- | --- |
| `peer.rs` | Peer lifecycle, close bounds, hang/fail-closed, late-message, live `PeerHarness` lanes, `peer_admits_only_the_first_data_channel` |
| `control_channel.rs` | Channel loop, flow control, FIFO, shutdown delivery, entity multiplex |
| `delivery.rs` | Framing, chunking, assembly-budget |
| `subscription_channel.rs` | Extra-channel reject and close-marker unit tests |
| `adapter.rs` | Former adapter unit tests (7) |
| `signaling.rs` | None. `signal` / `answer_offer` are exercised through `PeerHarness` in `peer.rs` |
| `webrtc.rs` | Two new ownership guards (commit 2) |

Shared harness (`PeerHarness`, `FakeDataChannel`, process-tree helpers, test locks) lives in `src/transport/webrtc/test_support.rs`, declared from `webrtc.rs` as `#[cfg(test)] pub(crate) mod test_support`.

## Ownership boundaries preserved

- Hub still owns concrete WebRTC transport.
- Admission stays in `src/admission/`. Transport holds a `GrantRegistry` field; it does not declare grant policy types.
- `persist_local_webrtc_terminal_record` and `detach_local_webrtc_subscriptions` stay in `src/daemon_transport.rs`.
- Public exports `botster_hub::LocalWebrtcError` and `botster_hub::LocalWebrtcTransport` keep crate-root paths.
- `src/local_webrtc_smoke.rs` is unchanged except that it never imported the deleted module.
- No Core pin change. No `webrtc` crate version change. No DTO/serde/protocol change.

## Cross-repo dependencies or separately routed work

None. Closed dependencies `ticket_1787894419_699597` and `ticket_1787999248_674913` were already merged. No botster-web, botster-tui, or botster-hub-client change.

## Runtime-teardown lenses

Every lens from the approved plan is implemented by preserving the existing production path, not by changing teardown behavior.

| Lens | Preserved owner after the move | Oracle that still passes |
| --- | --- | --- |
| Isolation | `peer.rs` owns `peers`, `peer_states`, `stale_close_peers` | `local_webrtc_single_peer_failed_cleanup_preserves_sibling_peer_and_runtime` |
| Bounds | `LOCAL_WEBRTC_PEER_CLOSE_BOUND` and handler join deadline in `peer.rs` | `hanging_data_channel_local_close_still_runs_cleanup_once_within_bound`, hang-close child oracle |
| Late-message matrix | Attach records in `subscription_channel.rs`; entity/event ingress in `control_channel.rs`; sweep still in `daemon_transport.rs` | late-subscribe / late-attach / late-spawn / replacement-owner tests |
| Production-path hard-stop | Handler → `cleanup_once` → `remove_peer` in `peer.rs` | `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners` |
| Ownership identity | grant-id keyed maps in `peer.rs` | stale snapshot tests |
| Sibling fail-closed | `fail_closed_drop_dedicated_runtime` in `peer.rs` | ultimate close-failure test |

No lens was dropped to follow-up.

## Deviations from plan

1. **Three product commits, not two, plus this review-return.** Extra-channel source assertions needed `e02ae38`. Review then required ablation evidence, plan resync, and PII cleanup.
2. **Two new leaf test names.** `webrtc_state_machines_have_one_owner_file` and `webrtc_transport_does_not_declare_admission_or_unix_policy_types` were added for acceptance checks 3 and 5. Every moved leaf name is preserved.
3. **Harness file, now a plan contract.** `src/transport/webrtc/test_support.rs` is the body of `#[cfg(test)] pub(crate) mod test_support`. The committed plan now names that file. It is in the forbidden-construct list and the check-13 ablation matrix. It is not a seventh role file.
4. **Live `PeerHarness` tests stay in `peer.rs`.**
5. **`signaling.rs` has no dedicated `#[test]` functions.**
6. **`pub(crate)` widening** on types, fields, and helpers that crossed module boundaries. No item became `pub`.
7. **Architecture summary `transport` row description** now names WebRTC.
8. **Extra-channel production source guard** reads `peer.rs` for the claim needle and `subscription_channel.rs` for the reject eprintln. The committed plan check 4 now states that split.

## Tests and downstream proof run

Toolchain: `rustc 1.97.0 (2d8144b78 2026-07-07)`. `CARGO_TARGET_DIR` unset.

| Check | Command | Result |
| --- | --- | --- |
| 19 | `rustc --version` | `1.97.0` |
| 20 | `cargo fmt --all -- --check` | pass |
| 21 | `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass, rerun after the last compile repair |
| 22 | `cargo build --locked -p botster-core-daemon --bin botster-session-worker` and `cargo build --locked --bin botster-hub` | pass |
| 23 / 7 | `./test.sh --locked` | pass, `SUITE_EXIT:0` |
| 7a | `webrtc_terminal_output_is_byte_exact` inside that suite | `ok` in `hub_daemon_lifecycle_test` (`319 passed; 0 failed`) |
| 8 | `cargo test --workspace --locked -- --list` | base 1300 tests, HEAD 1302 tests; leaf multiset identical except the two new ownership guards |
| 1 | `git ls-files src` has no `local_webrtc.rs` or `webrtc_terminal_adapter.rs` | confirmed |
| 10 | `git diff` of `Cargo.toml` / `Cargo.lock` vs `origin/main` for Core and webrtc | no pin change |
| Lib smoke | `cargo test --locked --lib transport::webrtc` | 61 passed |

### Guard census (check 11)

Base nine expressions, accounted by row:

| # | Base | HEAD |
| --- | --- | --- |
| 1 | `src/lib.rs` `include_str!("local_webrtc.rs")` | replaced by eight `src/transport/webrtc/**` entries, including `test_support.rs` |
| 2 | `src/host_control_fair_write.rs` `include_str!("local_webrtc.rs")` | `include_str!("transport/webrtc/control_channel.rs")` |
| 3 | `src/local_webrtc.rs` self-scan | `src/transport/webrtc/peer.rs` `include_str!("peer.rs")` |
| 4 | `src/transport/shared.rs` adapter include | `include_str!("webrtc/adapter.rs")` |
| 5 | `src/webrtc_terminal_adapter.rs` self-scan | `src/transport/webrtc/adapter.rs` `include_str!("adapter.rs")` |
| 6 | `hub_source("src/local_webrtc.rs")` one-shot claim | `hub_source("src/transport/webrtc/peer.rs")` |
| 7 | `hub_source("src/local_webrtc.rs")` deferred-egress needle | `hub_source("src/transport/webrtc/control_channel.rs")`; needle text unchanged |
| 8 | adapter production include | `include_str!("../../src/transport/webrtc/adapter.rs")` |
| 9 | sibling test-file include | unchanged `include_str!("webrtc_terminal_adapter.rs")` |

Additional necessary retargets: extra-channel close-marker strings now also scan `subscription_channel.rs`; `event_plane_saturation.rs` reads adapter.rs and peer.rs; `no_lua_dispatch` lists every new WebRTC file, including `test_support.rs`.

### Source-guard ablations (checks 13, 16, 17)

Exact filter, one-test baseline first:

```text
BOTSTER_ENV=test cargo test --locked --package botster-hub --lib \
  tests::production_sources_reject_terminal_drain_and_snapshot_phase_decode -- --exact
```

Baseline: `running 1 test`, `1 passed`, exit 0. `RUSTUP_TOOLCHAIN=1.97.0`. `CARGO_TARGET_DIR` unset. Each arm restored the file before the next arm.

Check 13, production GHOSTSNP in each listed file. Every arm exit 101 and named that file:

| File | Result |
| --- | --- |
| `src/transport/webrtc.rs` | `src/transport/webrtc.rs production source must not contain GHOSTSNP` |
| `src/transport/webrtc/peer.rs` | same, names `peer.rs` |
| `src/transport/webrtc/signaling.rs` | same, names `signaling.rs` |
| `src/transport/webrtc/control_channel.rs` | same, names `control_channel.rs` |
| `src/transport/webrtc/subscription_channel.rs` | same, names `subscription_channel.rs` |
| `src/transport/webrtc/delivery.rs` | same, names `delivery.rs` |
| `src/transport/webrtc/adapter.rs` | same, names `adapter.rs` |
| `src/transport/webrtc/test_support.rs` | same, names `test_support.rs` |

Check 16, GHOSTSNP after the final `#[cfg(test)]` block. Every arm exit 101 and named that file: `webrtc.rs`, `peer.rs`, `control_channel.rs`, `subscription_channel.rs`, `delivery.rs`, `adapter.rs`. `signaling.rs` has no test block. `test_support.rs` is the harness file; check 13 covers it.

Restore after the last arm: `running 1 test`, `1 passed`, exit 0.

### PII scan

`rg` over the 4a plan and this report for `/Users/`, `/home/jason`, `botster-sessions`, and a personal email returned no matches. The vault inbox capture is named by title, not by a local filesystem path.

### Downstream proof (check 24)

Zero cost. `botster_hub::LocalWebrtcError` and `botster_hub::LocalWebrtcTransport` remain crate-root exports. No UI contract or DTO change. Generic clients were not rebuilt.

### Production path

`LocalWebrtcHandler::on_data_channel` in `peer.rs` still claims the first channel and calls `run_data_channel` in `control_channel.rs`. Extra channels call `reject_extra_data_channel` in `subscription_channel.rs`. Peer close still uses `LOCAL_WEBRTC_PEER_CLOSE_BOUND` then `cleanup_once` / `remove_peer`. The full-suite byte-exact test and the extra-channel reject tests drive those production functions.

## Review-return findings

| Finding | Disposition |
| --- | --- |
| `finding_1788026828_663575` missing per-file ablations | Ran and recorded checks 13, 16, and 17. See the ablation table above. |
| `finding_1788026828_613962` plan/harness mismatch | Kept `test_support.rs`. Resynced the committed plan: assumption 2, affected files, and checks 12–16. Added the file to the `src/lib.rs` inventory and the `no_lua_dispatch` list. |
| `finding_1788026828_144063` local user path | Replaced the vault inbox absolute path with the inbox title. PII scan of the 4a plan and this report is clean. |

## Unverified behavior or residual risk

- `signaling.rs` has no unit tests of its own. Coverage is through live `PeerHarness` signaling.
- Load-only regressions outside the suite host used here remain possible. The required absolute suite gate passed on this host, including `webrtc_terminal_output_is_byte_exact`.
- This review-return did not rerun the full `./test.sh --locked` after adding `test_support.rs` to `include_str!`. It did rerun the exact production-source guard (green), both ownership guards, and `cargo clippy --workspace --all-targets --locked -- -D warnings`.

## Missing vault guidance discovered

The plan already recorded eight vault gaps. This visit did not add a ninth note. The extra-channel source-guard split is the concrete instance of plan gap 3 (move-fragile needles) plus gap 5 (region/file scan going green after a subject leaves the scanned file). Capture those gaps in a later vault pipeline pass rather than duplicating them here.
