# Nonblocking resize: downstream validation

Status: the classification repair and three focused downstream checks passed in the isolated export.
The implementer confirmed cleanup and unchanged source inputs. Fable approved the final provenance and focused results.
Core resize code and the separate close-contract clarification are merged and pushed to Core main `c47eadbf476501ec611e18572d8e4afc87d4304d`.
The isolated Hub classification patch is not merged. The active Hub ticket still consumes Core `93acae3`.

## Follow-up results

The replacement Codex implementer added the narrow `explicit_resize_busy` classification and its regression.
Fable reviewed the source before execution.
The coordinator inspected these logs in `foundation-validation` under the isolated Hub directory:

- `classification-green.log`: one classification test passed.
- `classification-negative.log`: the same test failed when the busy error mapped to `control_plane_failed`.
- `worker-build.log`: the matching worker build passed.
- `hub-build.log`: the isolated Hub build passed.
- `entity-resize.log`: `session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect` passed.
- `stale-transition.log`: `live_session_entity_subscription_emits_exact_stale_transition_patch` passed.
- `attached-exit.log`: `session_entity_subscription_observes_attached_natural_exit_with_pending_egress` passed.

Each focused test executed exactly once in its selected suite.
Other workspace suites with zero matching tests do not count as additional coverage.
The implementer reported immediate restoration of the approved source after the negative control.
The restored `runtime.rs` SHA-256 is `b37cbcb1ab63eec8fc28a4d7fb2f73cceef9338a02177960777354efa50b28e3`.
The sections below preserve the earlier preparation failures and exact source inputs.
These focused results do not prove the complete integration matrix or the later Hub candidate.

## Fixed inputs

- Approved Core: `5923bf1847979e2897796fadb9863183ffa5e3f1`.
- Hub validation base: `12b4a5482457fa9204f6c08916810abaaef682be`.
- Ghostty: `eb72ec61304ea256be1d86ed8fa961c84e43ecbd`.
- Isolated Hub source: `/private/tmp/botster-resize-downstream.g2c6fu`.
- Isolated Core source: `/private/tmp/botster-resize-core.0yQvP0`.

The coordinator exported each exact revision with `git archive`.
The temporary Hub manifest overrides all six Core-family packages with the isolated Core source.
Cargo resolved all six packages as local sources. The temporary lockfile reflects those overrides.
The active Hub ticket and Grok's frozen Core worktree remain unchanged.
Hub later advanced to `205cadf` for an unrelated error-classification repair; this validation retains the recorded base.

## Preparation

The Hub implementer confirmed owned-process cleanup and released the validation window before compilation.
The coordinator limited Cargo to two build jobs.

The first build pointed at the frozen Core worktree.
Ghostty attempted to install generated files inside that source tree, and the sandbox denied the writes.
The coordinator then exported Core and Ghostty to writable temporary directories.
The next build needed cached Zig dependencies that were absent from the new cache.
The coordinator copied the existing Zig package cache into the isolated Hub directory.
No production source change was needed for either preparation issue.

The matching session worker built successfully with:

```sh
ZIG_GLOBAL_CACHE_DIR=/private/tmp/botster-resize-downstream.g2c6fu/zig-cache CARGO_BUILD_JOBS=2 cargo build --offline --locked -p botster-core-daemon --bin botster-session-worker
```

Exit status: zero. Final build duration: 44.17 seconds.

## Focused checks

The first check uses the repository wrapper:

```sh
ZIG_GLOBAL_CACHE_DIR=/private/tmp/botster-resize-downstream.g2c6fu/zig-cache CARGO_BUILD_JOBS=2 ./test.sh --offline --locked --test hub_daemon_lifecycle_test session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect -- --exact --nocapture
```

Log: `/private/tmp/botster-resize-downstream.g2c6fu/resize-entity.log`.
Initial result: compilation failed before test execution.
The Hub test-support build script requires the old Git source identifier and rejects Cargo path overrides.
In the temporary copy only, the coordinator added an exact-path match for the approved protocol source.
This preserves Core-owned fixture loading and rejects unrelated local paths.
The adaptation changes test build setup, not Hub runtime behavior.
The adapted run log is `/private/tmp/botster-resize-downstream.g2c6fu/resize-entity-adapted.log`.
Adapted result: exit 101 before test execution.
Hub `src/runtime.rs:4169` exhaustively matches `CoreDaemonError` in `managed_session_core_error_class`.
It does not handle the candidate's new `ExplicitResizeBusy(SessionId)` variant.
Rust reports `E0004` at log line 195.
This is a real downstream source change required for the Core pin update, not a test setup issue.
The earlier plan's claim that Hub needs no source change is therefore incorrect.

No daemon test ran, and no Hub or worker process started during validation.
The build completed, and the coordinator released the host to the existing ticket's final matrix.
That ticket still consumes Core `93acae3`; this new-variant issue does not affect its current candidate.
Next: add a narrow Hub error-classification arm with a focused test in isolated integration work.
Resume runtime validation after the active matrix releases the host.

This test checks acknowledged resize geometry in session entities and persisted records, then later terminal input and lifecycle transitions.
It does not replace the Core delayed-sibling regression or a full consumer matrix.
Additional targeted checks will cover route retirement and pending exit delivery.
