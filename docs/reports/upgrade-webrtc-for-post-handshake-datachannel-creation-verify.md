# Verification report: upgrade WebRTC for post-handshake DataChannel creation

| Field | Value |
| --- | --- |
| Ticket | `ticket_1787654915_646236` |
| Run | `run_1787654940_337274` |
| Step | `botster_stack_verify` (`run_step_1787663113_724053`) |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Commit verified | `47d964abaf0f8f13748a1482aa8c450501dda9ed` |
| Base | `f6db5c436f72b151fd6dacde61d3f4836a4dc925` |
| Worktree state | `git status --porcelain` empty before and after every command |
| Verdict | pass |

## Target resolution

I resolved the target from the run record, not from the ambient directory. The run and the ticket
both carry `target_id` `tgt_7e208a0c76a44980a83b63af976b1f22`. The ticket text names the
`botster-hub` ownership charter. The diff touches only Hub-owned paths.

## Guidance loaded

| Layer | Notes |
| --- | --- |
| Role | [[verifier-playbook]], [[botster-verifier-playbook]] |
| Repository charter | [[botster-hub-playbook]] |
| Surface overlay | [[botster-runtime-verifier-playbook]] |
| Class overlay | [[botster runtime teardown lenses]] |
| Targeted | [[the pinned Rust WebRTC peer cannot open a DataChannel created after the SCTP handshake]], [[the browser creates each subscription DataChannel after Hub reserves its label]], [[botster subscriptions use dedicated ordered DataChannels]], [[rejected channel isolation needs a surviving channel positive control]], [[WebRTC DataChannel local close uses the peer close bound before cleanup]], [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]], [[Hub suite runs prebuild the session worker before the locked test wrapper]], [[a regression test must be shown to go red with the fix reverted]], [[verify must recheck resolved findings against the live worktree]] |

Not loaded: web, package, and Project Pipelines verifier overlays. No changed file belongs to those
surfaces.

## Commands and results

| # | Command | Result |
| --- | --- | --- |
| 1 | `cargo build --locked -p botster-core-daemon --bin botster-session-worker` | Finished |
| 2 | `cargo build --locked --bin botster-hub` | Finished |
| 3 | `cargo check --workspace --all-targets --locked` | Finished, no error |
| 4 | `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 |
| 5 | `BOTSTER_ENV=test cargo test --locked --lib -- --exact local_webrtc::tests::post_handshake_data_channel_opens_and_delivers_bytes local_webrtc::tests::peer_closed_removes_webrtc_admission_and_host_compatibility local_webrtc::tests::runtime_spawn_detach_on_drop_runs_to_completion` | running 3 tests, 3 passed |
| 6 | `BOTSTER_ENV=test cargo test --locked --test hub_daemon_lifecycle_test -- --exact webrtc_peer_post_handshake_data_channel_reaches_production_reject webrtc_peer_rejects_a_second_data_channel_requires_one_shot_claim` | running 2 tests, 2 passed |
| 7 | Independent `0.20` ablation, see below | FAILED at `remote late channel by label` |
| 8 | `./test.sh --locked` | exit 0, 0 failures on every target |

Command 1 has a charter correction. `cargo build --locked --bin botster-session-worker` from the Hub
root fails with `no bin target named botster-session-worker in default-run packages`. The worker
belongs to the pinned Core package. The `-p botster-core-daemon` form is required.

Command 8 target totals, each read from its own `running N tests` header:

| Target | Result |
| --- | --- |
| `botster-hub` lib | 493 run, 493 passed |
| `botster-hub` main | 29 run, 29 passed |
| `hub_daemon_lifecycle_test` | 320 run, 318 passed, 0 failed, 2 ignored, 298.74 s |
| every other member, test, and doctest target | 0 failed |

One `test result: ok. 1 passed; 492 filtered out` line appears inside the lib target output. That is
the re-executed child harness of
`local_webrtc_close_hang_fail_closed_returns_handler_within_deadline`. It is not the parent total.

## Selector integrity

A first attempt at command 6 used the module prefix
`subscription_ownership_baseline::webrtc_peer_post_handshake_data_channel_reaches_production_reject`
and selected **zero** tests while still exiting 0. `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs`
is `include!`d at the test-crate root, so its tests carry no module prefix. I captured
`cargo test --test hub_daemon_lifecycle_test -- --list` to a file, counted 320 test lines, and
confirmed the target name is present before rerunning. Every focused command above reports a
nonzero selected count.

## Independent red-on-revert ablation

The Implement report cites a detached worktree at `d9ff12c` with uncommitted changes. I did not rely
on it. I created my own detached worktree at base `f6db5c4`, confirmed
`webrtc = "0.20.0-beta.2"` in `Cargo.toml` and `webrtc 0.20.0-rc.1` in `Cargo.lock`, confirmed
`git status --porcelain` was empty, then appended the HEAD `post_handshake_data_channel_opens_and_delivers_bytes`
body with only one mechanical change: the `0.21` three-argument `timeout(runtime.as_ref(), D, F)`
reduced to the `0.20` two-argument `timeout(D, F)`.

Result on `webrtc 0.20.0-rc.1`:

```
test local_webrtc::tests::post_handshake_data_channel_opens_and_delivers_bytes ... FAILED
thread '...' panicked at src/local_webrtc.rs:7638:14:
remote late channel by label: ()
test result: FAILED. 0 passed; 1 failed; ... finished in 10.62s
```

The failure occurs **after** the pre-handshake `botster-client` channel was received and its label
asserted. It measures the missing post-handshake channel, not a stale setup channel. I removed the
worktree after the run. The ticket worktree stayed clean throughout.

## Behavior and production path proved

| Claim | Oracle | Production path |
| --- | --- | --- |
| Either peer can create a DataChannel after the SCTP association exists | `post_handshake_data_channel_opens_and_delivers_bytes` | Two real `PeerConnection`s over `127.0.0.1` UDP. Both reach `Connected`. The late channel fires local `OnOpen`, fires remote `OnOpen`, and delivers `post-handshake-bytes`. Ordering and reliability are asserted from the channel itself. |
| The dependency is the cause | Ablation above | Same test body, `0.20` only, red |
| A post-handshake channel reaches the live Hub handler | `webrtc_peer_post_handshake_data_channel_reaches_production_reject` | A real isolated `botster-hub` child. Encrypted Hello takes the one-shot claim, then the offerer creates `botster-extra` after the handshake. Hub's `on_data_channel` writes the observation file with `lost_claim=true`, `close_ok=true`, `label=botster-extra`, and the bounded `local_close` marker exists. |
| Rejected-channel isolation still has a positive control | `webrtc_terminal_adapter_second_data_channel_does_not_receive_terminal_frames` | `create_extra_data_channel` now requires `OnOpen` instead of discarding it, so the zero-frame assertion can no longer pass on a channel that never opened |
| Hello admission ownership is swept on `PeerClosed` | `peer_closed_removes_webrtc_admission_and_host_compatibility` | `RegisterWebrtcAdmission` inserts both `pending_runtime` rows through `handle_control_message`; `LocalWebrtcPeerClosed` removes both. The test asserts positive controls first and records why `webrtc_is_admitted` is not a valid oracle for a `Rejected` row. |
| Close bounds, sibling survival, and ultimate sacrifice are unchanged | `local_webrtc_close_hang_fail_closed_returns_handler_within_deadline`, `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners`, `local_webrtc_single_peer_failed_cleanup_preserves_sibling_peer_and_runtime`, `peer_close_leaves_sibling_peers_working` | All green inside command 8 |
| Detached `Runtime::spawn` tasks still complete | `runtime_spawn_detach_on_drop_runs_to_completion` | Closes the risk that a dropped `JoinHandle` silently cancels a poller |

## Migration correctness, checked at the crate source

- `RTCPeerConnectionState` and `DataChannelEvent` gain `#[non_exhaustive]` in `0.21`. I diffed both
  enums between `rtc-0.20.0-rc.1` and `rtc-0.21.0-beta.2` and between `webrtc-0.20.0-rc.1` and
  `webrtc-0.21.0-beta.2`. **Neither gains a variant.** The added `_ => "unknown"` arm is
  compiler-required, every named variant stays explicit, and no behavior changes. The pre-existing
  `_ => Ok(())` in `apply_data_channel_event` is unchanged from base.
- The local `block_on` replacement is byte-identical in semantics to the removed
  `webrtc 0.20 runtime::tokio::block_on`: both build a multi-thread Tokio runtime with
  `enable_all()` and block on it. No new nested-runtime hazard, confirming report item U4.
- `default_runtime()` returns `Arc::new(TokioRuntime)` over a zero-sized type. Calling it per
  `timeout` site costs one `Arc` allocation and starts no runtime.

## Dependency graph

`Cargo.lock`: 398 packages at base, 374 at HEAD, 53 changed. `webrtc`, `rtc`, and all fifteen
`rtc-*` members move `0.20.0-rc.1` to `0.21.0-beta.2`. `rtc-crypto`, `quinn-udp`, and `crc32c` are
added. Twenty-seven crates are dropped, including `p256`, `p384`, `curve25519-dalek`,
`x25519-dalek`, `chacha20poly1305`, and `signature`. `nix`, `rand`, `rand_core`, `chacha20`, and
`rkyv` also move.

`cargo tree --locked -i rand@0.10.2` and `-i nix@0.31.3` show only `rtc*` parents. `Cargo.toml`
declares no direct `rand`, `nix`, `chacha20`, or `rkyv` dependency. Every transitive change is
confined to the `rtc`/`webrtc` subtree. The only Hub-declared version change is `webrtc`.

## Review findings

All thirteen findings across six reviews are `resolved` and `open_findings` is empty. I rechecked
the three findings from the last return against the live worktree instead of trusting the status:

| Finding | Recheck |
| --- | --- |
| `finding_1787662081_291539` (high) — ablation failed before it measured the late channel | Confirmed fixed by my own independent ablation. The `0.20` failure is now at `remote late channel by label`, after `botster-client` was consumed and label-asserted. |
| `finding_1787662081_738710` (medium) — test ignored the remote `OnOpen` | Confirmed fixed at `src/local_webrtc.rs`. Both the timeout and the receive result use `expect`. The payload assertion is a separate proof. |
| `finding_1787662081_406911` (medium) — committed report contained local absolute paths | Confirmed fixed. A case-insensitive scan of every changed file for the local username, `/Users/`, `botster-sessions`, session tokens, and personal email addresses returned zero hits. |

No finding remains open. I opened no new finding.

## Runtime-teardown lenses

The ticket is runtime-teardown class. The roll changes no lens:

| Lens | Evidence |
| --- | --- |
| Isolation | Unchanged. `local_webrtc_single_peer_failed_cleanup_preserves_sibling_peer_and_runtime` and live `peer_close_leaves_sibling_peers_working` both pass. |
| Bounds | Production close still uses `tokio::time::timeout(LOCAL_WEBRTC_PEER_CLOSE_BOUND, ...)`. Those sites were not converted to the webrtc timeout. `local_webrtc_close_hang_fail_closed_returns_handler_within_deadline` passes, with the fail-closed log lines visible in the suite output. |
| Late-message matrix | No owner tag, rejection rule, or sweep changed. The Hello row is proved live by the admission sweep test; the post-handshake `on_data_channel` row is proved live by the A4-live test. |
| Production hard stop | A4-live drives the real Hub binary end to end and requires the bounded `local_close` marker file, not a terminal JSON record alone. |
| Ownership identity | Unchanged. `grant_id` remains the owner. The sweep test uses `contains_key`, not the false-negative `webrtc_is_admitted`. |
| Sibling fail-closed | Unchanged. `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners` passes. |

## Cross-repository consumer proof

None is required, and none is claimed.

- No public Hub contract changed. `botster-hub-client`, `botster-ui-contract`, and
  `botster-hub-test-support` have no version, DTO, protocol, or fixture change in the diff. Their
  own test targets are green inside command 8: 81, 90, and 45 tests respectively.
- `webrtc` is an internal Hub implementation dependency, not a published Hub surface. No sibling
  repository resolves it through Hub.
- `src/daemon_transport.rs` gains only two `#[cfg(test)]` accessors. No production behavior changes.
- The downstream consumers named in the plan, `ticket_1787600674_500120` and
  `ticket_1787600682_233928`, own the admitted-subscription work. This ticket deliberately excludes
  reservation, routing, renegotiation, and channel pooling.

## Unverified behavior

1. `smoke_local_webrtc_round_trip`, reached from the `src/main.rs` smoke CLI, was not executed. It
   requires an installed and running `botster-web` package with a local URL. Its diff is limited to
   the same mechanical `block_on`, `sleep`, and `timeout` adaptation that the executed paths use,
   and it compiles under `--all-targets`.
2. Answerer-originated post-handshake channel creation is not separately tested. The single
   regression uses the offerer as creator because that matches the browser production role, and the
   ticket requires one real two-peer regression.
3. A real browser peer was not driven. The A4-live test simulates the browser with a Rust peer.
4. A4-live was not run on `webrtc 0.20`. Plan check A5 designates that result diagnostic, not a
   gate, because live Hub traffic can wake the driver on its own. A4 carries the causality gate and
   I reproduced it red.

## Remaining risk

1. `webrtc 0.21.0-beta.2` is a beta release. `Cargo.lock` pins the exact graph, so the risk is a
   future deliberate roll, not drift.
2. The `0.21` graph replaces the RustCrypto elliptic-curve and AEAD stack with `rtc-crypto`. DTLS,
   SRTP, and ICE all pass through the live Hub tests, but this is a new and less-exercised crypto
   dependency.
3. `hub_daemon_lifecycle_test` remains sensitive to host load. My run was green on a host carrying
   load average 5.5 to 7.0 and several foreign Botster daemons, which is stronger than a quiet-host
   run, but it is one sample.
4. Real browser-created admitted subscription channels remain unproved until
   `ticket_1787600674_500120`.

## Vault gaps

Two inbox notes written under `~/knowledge/inbox/`:

1. `webrtc-021-restores-post-handshake-datachannel-creation-in-hub.md`. Records the measured `0.21`
   behavior and migration cost, and marks the version-scoped precondition in
   [[the pinned Rust WebRTC peer cannot open a DataChannel created after the SCTP handshake]] and in
   the [[botster-hub-playbook]] Required Gates line as superseded on merge. The rest of that charter
   line, requiring every isolation test channel to prove `Open`, stays valid and is now enforced in
   the fixture.
2. `hub-session-worker-prebuild-needs-the-core-daemon-package-flag.md`. The charter prebuild gate
   omits `-p botster-core-daemon`, so its literal command fails.
