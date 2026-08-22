# Event-plane load proof

This document publishes **event-plane coexistence regression budgets**. They
state how much a Hub operation may degrade when the package-event plane runs
beside it.

They are not Terminal Transport North Star budgets. They are not general
terminal transport service levels. The North Star behavioural oracles remain
identity, ordering, exact bytes, late-attach history, resize, input,
cancellation, reconnect, and `ProcessExited`. This campaign proves those oracles
under saturation and does not change terminal byte ownership.

## Machine profile

| Parameter | Fixed value |
| --- | --- |
| Runner | fresh GitHub-hosted `ubuntu-24.04` from `.github/workflows/loaded-daemon-lifecycle.yml` |
| Recorded fields | runner image, architecture, CPU count, total memory, kernel release, `ulimit -n`, PTY ceiling, Rust 1.97.0, Zig 0.16.0 |
| Stress profile | `none`, identical in calibration and acceptance |

## Fleet and schedule

| Parameter | Fixed value |
| --- | --- |
| Background sessions `N` | 300 quiet sessions |
| Spawn ramp | 10 waves of 30, 200 ms between waves |
| Steady state | all 300 report `running` |
| Attached noisy PTY | exactly 1 |
| Driver concurrency | 4 workers |
| Cycle | `Spawn`, `Attach`, `Drain`, `Input`, `Resize`, MCP, UI, entity read, `Shutdown` |
| Think time | none |
| Measurement window | 600 seconds after warm-up |
| Warm-up | first 30 seconds of steady state and the first 20 samples of each operation, whichever ends later |
| Minimum samples | 200 post-warm-up samples per operation per arm |
| Event rate, enabled arm | 150 events per second in 25-event bursts, 4 KiB payload |
| Terminal output | 4 KiB every 100 ms |
| Terminal input | 64 bytes every 500 ms |

A shortfall against `N = 300` fails Gate 1 as `product_failure` unless
scheduler-lag or confirmed FD/PTY evidence selects `host_exhaustion`. The
campaign does not silently reduce `N`.

## Literals

| Symbol | Value | Meaning |
| --- | --- | --- |
| `R` | 1.25 | enabled/decoupled latency ratio ceiling |
| `S` | 8 ms | one extra bounded background slice (`EVENT_DELIVERY_MAX_ELAPSED`) |
| `T` | 0.80 | throughput retention floor, exactly `1 / R` |

Percentiles use nearest-rank on the ascending sample vector. Derived millisecond
thresholds round up. Derived throughput floors round down. Ratios compare in
`f64` at three decimal places. No post-warm-up sample is discarded. Any failed
operation in a measurement arm is an immediate `product_failure`.

## Derivation

Let `Pxcal_e(op)` / `Pxcal_d(op)` be calibration percentiles for the enabled and
decoupled arms, and `THRcal_e` / `THRcal_d` the calibration throughput.

| Metric | Absolute budget | Relative acceptance gate |
| --- | --- | --- |
| p50 | `ABS50(op) = ceil_ms(P50cal_e(op) * 1.20 + S)` | `P50acc_e <= P50acc_d * R + S` |
| p95 | `ABS95(op) = ceil_ms(P95cal_e(op) * 1.20 + S)` | `P95acc_e <= P95acc_d * R + S` |
| p99 | `ABS99(op) = ceil_ms(P99cal_e(op) * 1.20 + S)` | `P99acc_e <= P99acc_d * R + S` |
| maximum | `ABSMAX(op) = ceil_ms(P99cal_e(op) * 3.00 + S)` | `MAXacc_e <= MAXacc_d * 3.00 + S` |
| throughput | `THRMIN(op) = floor_int(THRcal_e(op) * T)` | `THRacc_e >= THRacc_d * T` |

## Two phases

1. Calibration dispatch on the reference runner. It writes
   `docs/reports/prove-the-event-plane-cannot-lag-or-block-hub-operations-calibration.json`
   and the derived thresholds below. It does not judge the product.
2. Commit those literals.
3. Acceptance dispatch on the same profile. Acceptance samples never enter
   threshold derivation.

## Derived thresholds

Pending the reference-runner calibration commit. Until that file contains
immutable `ABS*` and `THRMIN` literals, acceptance must not run.

## Deterministic host limits

These are shipped production bounds. A breach names the limit, not a new
tuning opportunity.

| Bound | Value |
| --- | --- |
| `MAX_OWNER_TURN_MS` | 25 |
| `MAX_READY_OPERATION_WAIT_MS` | 50 |
| Observe slice | 8 sessions / 64 KiB / 8 ms |
| Baseline page | 16 rows / 64 KiB / 8 ms |
| Event delivery | 8 items / 32 KiB / 8 ms |
| Session delivery | 16 items / 64 KiB / 8 ms |
| Producer queue | 256 events / 512 KiB |
| Consumer queue | 128 events / 2 MiB |
| Global in-flight | 16 MiB |
| Package rate | 100 / sec, burst 200 |
| Queue age | 1000 ms |
| `DAEMON_MAX_CONNECTIONS` | 64 (clients, not sessions) |

## Verdicts

Reuse `clean`, `product_failure`, `host_exhaustion`, `environment_tainted`, and
`survivors_present` from `docs/lifecycle-suite-harness.md`.

The disabled/decoupled arm is host-valid only when all eleven immutable
pre-calibration gates pass: N=300 at steady state, the 600-second window
(`window_completed` only; worker errors do not fail Gate 2), ≥200
post-warm-up samples per operation, zero operation failures (attempts
recorded at start, completions recorded even after the window, so incomplete
cycles, timeouts, disconnects, incomplete responses, and attempts≠successes
fail Gate 4), terminal oracles stored as structured evidence (exact bytes
and ordering, continuous sequence, zero I/O failure, zero unexpected
terminal gap, no peer loss), `max_owner_turn_us` ≤ 25,000 µs,
`max_ready_operation_wait_us` ≤ 50,000 µs, monotonic scheduler-lag maximum
at most 50,000 µs (50,000 passes; only a strictly greater lag fails) across
the full disabled-arm interval, applicable queue age ≤ 1,000,000 µs, no
confirmed FD or PTY exhaustion, and no environment taint or survivors.
PackageEvent or EventGap on Unix/WebRTC event subscriptions are valid
event-plane observations and are not Gate 5 unexpected-gap or peer-loss
failures. An arm panic keeps partial evidence, stops workers and the
watchdog, classifies available gates, and persists the host-validity
artifact before exit. Gates 1–5 are `fail` rather than `not_evaluated` on
that path.

`host_exhaustion` is selected only from scheduler-lag maximum or confirmed
FD/PTY evidence. Owner-turn, ready-wait, queue-age, sample, and terminal
failures without that evidence are `product_failure`. Gate 11 selects
`environment_tainted` or `survivors_present`. Load average, runnable count,
CPU steal (`linux_proc_stat_steal_ticks`), and raw lag are diagnostics and
never select the verdict. Calibration records operation latency and
throughput without gating those numeric values until a valid calibration
exists; it does not invent pre-calibration `ABS*` or `THRMIN`. If the
disabled arm is valid and the enabled arm fails, the verdict is
`product_failure`. Every classification path persists
`event-plane-host-validity.json` with all eleven gate results before exit.

## How to run

```sh
gh workflow run loaded-daemon-lifecycle.yml \
  --ref <branch-or-main> \
  -f subject_sha=<exact Hub SHA> \
  -f test_target=event-plane-saturation \
  -F repetitions=1 \
  -f stress_profile=none
```

Local Darwin hosts are not the reference runner. The campaign's published
reference is GitHub-hosted `ubuntu-24.04` with `stress_profile=none`. Other
loaded-lifecycle tickets keep `residual-tail` as their default.
