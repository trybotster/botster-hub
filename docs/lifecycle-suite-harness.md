# Lifecycle-suite harness contract

`script/run-lifecycle-suite` is the exclusive wrapper for
`hub_daemon_lifecycle_test`.

It does this, in order:

1. Scan the host for Botster dev-artifact processes (`script/process-census
   dev-artifact-rows`) and capture a zombie baseline.
2. Refuse with `verdict=environment_dirty` when that scan is nonempty, unless
   `BOTSTER_LIFECYCLE_SUITE_FORCE_DIRTY=1` is set.
3. Prebuild `botster-session-worker` and run exactly one
   `./test.sh --locked --test hub_daemon_lifecycle_test`.
4. Require exactly one `test result:` tally.
5. Prove this worktree has no live owned executables, no new botster-role
   zombies, and no new host-wide dev-artifact rows.
6. Emit one verdict.

Verdict order:

1. `environment_dirty` — pre-run leftovers (or `environment_dirty_forced`).
2. `product_failure` — any failed test without a confirmed host-resource marker.
3. `host_exhaustion` — every failure carries `harness_budget_expired` with
   `EMFILE`/`ENFILE`/`PTY` or `probe=confirmed`.
4. `clean` — zero failures, one tally, empty post-run census.

`environment_tainted` failures from the process-wide latch are grouped under
the originating taint. Survivors annotate the verdict with
`survivors_present` and fail the command.
