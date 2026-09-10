# Darwin sampler checks

Run the focused checks from the Hub repository:

```sh
python3 script/test-measure-processes/run.py
```

The command compiles the sampler and its unit checks with Clang warnings treated as errors.
It prints the directory containing compiler logs and raw JSONL test evidence.
It checks Mach tick conversion, identity changes, PID reuse, reparenting, observed exits, and unresolved child accounting.
It checks a native resource-usage sample against two `CLOCK_PROCESS_CPUTIME_ID` reads.
That check permits only the clock resolution reported by `clock_getres`.
It does not run a CPU workload.
It then samples an owned root and child twice.
The test keeps both processes live through the final sample.
The test closes their input pipes after sampling.
Neither the sampler nor the test sends signals to target processes.

The stable-mode checks also use a native fixture whose child reports readiness before sampling starts.
They check a stable rooted set, a fork and exit between sample reads, and the setup-attempt limit.
Synthetic checks verify registration receipts, complete event drains, and permanent invalidation after the baseline starts.

Compile the sampler separately with:

```sh
clang -std=c11 -Wall -Wextra -Werror -O1 -lproc script/measure-processes.c -o /tmp/measure-processes
```

Pass an owned process identity and a role to the sampler:

```sh
/tmp/measure-processes --owned-root "$owned_pid:hub" --interval-ms 100 --samples 10
```

Repeat `--owned-root` for independent roots.
Keep each root live through the final sample.
Roles use ASCII letters, digits, underscores, periods, and hyphens.
The sampler never starts, stops, or signals a target process.
The optional `--physical-footprint` flag emits physical footprint separately from resident set size (RSS).

Every output line contains a JSON object with version `1`.
The configuration records the Mach timebase and requested delay between completed samples.
Sample boundaries and process reads record actual Mach timestamps.
Process identity consists of the PID and `ri_proc_start_abstime`.
The sampler reads resource usage before and after BSD metadata.
It rejects identity changes across those reads.
The sampler does not use the executable UUID as process identity.

Own CPU counters and reaped-child CPU counters remain separate.
Their raw unit is Mach ticks.
Nanosecond conversion uses the reported timebase and rounds down.
An overflow emits `null`, marks that conversion invalid, and invalidates the observations.
The sampler never adds process CPU counters to ancestor child counters.

The sampler records first observations, observed exits, disappearances, reparenting, and sampling errors.
A first observation does not prove that a birth occurred during sampling.
The start timestamp identifies the process birth reported by the kernel.
Disappearance does not prove an exit time.
The sampler retains previously observed descendants after reparenting.
It cannot discover every process that starts and exits between polls.
Snapshots are not atomic across processes.

The sampler discovers children recursively with `proc_listchildpids` from verified owned processes.
It does not perform a global process census.
The sampler checks each parent identity before enumeration and after reading the provisional children.
It accepts those children only if the parent identity remains unchanged.
Each child must retain the expected BSD parent PID during its bracketed resource-usage read.
The sampler reads resource usage only for owned candidates and previously tracked PIDs.
A tracked PID must retain its previous start timestamp before the sampler retains ownership after reparenting.
An unrelated inaccessible process cannot invalidate owned observations because the sampler never queries that process.
An enumeration error marks ancestry discovery incomplete and invalidates observations.
Errors while reading owned candidates or tracked PIDs invalidate observations.
Post-initial births, observed exits, reparenting, disappearance without an observed exit, and changes to reaped-child CPU counters mark unresolved turnover.
The sampler does not reconcile those lifecycle changes and therefore marks their observations invalid.
The summary always marks lifecycle accounting invalid because polling cannot establish complete lifecycle coverage.
Exit code `2` indicates invalid observations or invalid arguments.
Exit code `0` does not establish complete lifecycle accounting.

## Optional stable baseline

```sh
/tmp/measure-processes --owned-root "$owned_pid:hub" --interval-ms 100 --samples 10 --stable-baseline --setup-attempts 8
```

Raw mode retains its existing behavior when `--stable-baseline` is absent.
Stable mode requires at least two samples.
The default setup limit is eight attempts.
`--setup-attempts` sets a different explicit limit and requires stable mode.

The sampler registers `EVFILT_PROC` watches for `NOTE_FORK`, `NOTE_EXEC`, and `NOTE_EXIT`.
It checks every `EV_RECEIPT` result and checks process identity across registration.
The sampler registers roots before discovering children.
It registers each child before enumerating that child's descendants.
Setup succeeds only after a quiet, complete discovery pass whose participants were already watched before that pass began.
Otherwise the sampler reports `unable_to_stabilize` when it reaches the attempt limit.

The accepted baseline contains the currently rooted participants in that quiet pass.
It does not contain every historical descendant.
Supply known escaped or orphan processes as additional explicit roots.
Rejected setup passes do not establish historical completeness.
The output records these coverage limits.

From the beginning of baseline reads, any lifecycle event or error permanently invalidates the interval.
Each drain reads every pending batch until a zero-time read returns no events.
The sampler never disables or removes baseline watches.
It checks endpoint identities, parent PIDs, participant sets, and reaped-child counters.
It performs a final zero-time event drain after all final counter, identity, parent, and enumeration reads.
The sampler closes the event queue only after it has finished the interval.

The sampler emits raw process rows and unverified sample markers during collection.
It emits own CPU deltas and sampled RSS sums only after the final drain verifies the interval.
Invalid intervals emit no CPU deltas or RSS sums.
Their summary contains `null` CPU totals.
CPU deltas use each fixed participant's own counters; they never add reaped-child counters.
Per-process and collection timestamps bracket the reads.
The interval is not atomic across processes.
RSS sums represent sampled values, not peak memory.

Apple's [kqueue manual](https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/man/man2/kqueue.2) describes registration receipts and zero-time event reads.
The stable-mode source and tests passed their focused execution on 2026-09-09.

Apple's [libproc implementation](https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/libsyscall/wrappers/libproc/libproc.c) returns a PID count from `proc_listchildpids`.
The wrapper can return zero after an error.
The sampler clears and checks `errno` to distinguish that case from an empty child list.
The sampler grows a full buffer and repeats the child-list read.

These checks verify the harness. They do not run a performance benchmark or define an aggregate accounting contract.

## Rejected first check, 2026-09-09

The first test command passed with Apple Clang 17.0.0 (`clang-1700.6.4.2`) on `arm64-apple-darwin25.5.0`.
Root rejected that test result because the test accepted invalid observations for a healthy owned tree.
The measured Mach timebase was `125/3`.
Both owned processes appeared in both samples and remained live through the final sample.
The census reported 466 `rusage_before` errors with errno `1` and 256 `bsd_metadata` errors with errno `3`.
The sampler returned exit code `2` and marked observations invalid.
The revised test requires valid owned observations and rejects any error while reading the owned processes.

The wrapper log is `/private/tmp/measure-processes-check-1.log`.
The compiler version is recorded in `/private/tmp/measure-processes-compiler-1.log`.
Raw test evidence is in `/var/folders/2k/r_31n4yj0xv5wjvzq_yjrjkm0000gn/T/measure-processes-evidence-8tcg4rx4`.

## Revised focused check, 2026-09-09

The revised command passed all three builds with warnings treated as errors.
It ran one synthetic unit executable, one native CPU executable, three invalid-argument cases, and one owned process tree check.
The owned tree check recorded two samples with zero sampling errors.
Both samples reported valid observations and complete ancestry discovery for the observed tree.
Both owned processes remained live through the final sample.
The sampler returned exit code `0` and kept lifecycle accounting invalid.

The native CPU check reported own CPU time of `2,109,416 ns` after Mach conversion with timebase `125/3`.
The process-clock bracket was `2,106,000 ns` through `2,111,000 ns`, with reported resolution `1,000 ns`.
The native measurement fell within that bracket without additional tolerance.

The wrapper log is `/private/tmp/measure-processes-check-2.log`.
The compiler version is recorded in `/private/tmp/measure-processes-compiler-2.log`.
Raw evidence is in `/var/folders/2k/r_31n4yj0xv5wjvzq_yjrjkm0000gn/T/measure-processes-evidence-bh2fu2ca`.

The stricter process-set assertion was replayed against that saved JSONL after review.
Each sample contained exactly two process rows with PID set `{98643, 98645}`.
The replay passed without another compiler or live test run.
Its log is `/private/tmp/measure-processes-replay-1.log`.

## Stable-baseline focused check, 2026-09-09

The command `PYTHONDONTWRITEBYTECODE=1 python3 script/test-measure-processes/run.py` passed with exit code `0`.
All five builds passed with warnings treated as errors.
The command ran three test executables, three invalid-argument cases, and four controlled process cases.
The raw owned case retained two valid samples with zero sampling errors.
The native CPU check reported `2,511,916 ns` within a process-clock bracket of `2,510,000 ns` through `2,514,000 ns`.
The reported clock resolution was `1,000 ns`.

The stable owned case verified two participants and two samples.
Its idle participants recorded an own CPU delta of zero.
The test checked CPU arithmetic and sampled RSS sums against the raw process rows.
The final drain followed all endpoint reads.

The hidden-fork case retained the same two participants at both sampled endpoints.
The transient child completed between those endpoint reads and appeared in neither sample.
The parent's `NOTE_FORK` event permanently invalidated the interval.
The sampler returned exit code `2` and emitted no CPU delta or RSS aggregate records.
The one-attempt setup case also returned exit code `2` and emitted no aggregates.

The wrapper log is `/private/tmp/measure-processes-stable-check-1.log`.
The compiler version is recorded in `/private/tmp/measure-processes-stable-compiler-1.log`.
The executed source diff is `/private/tmp/measure-processes-stable-source-1.patch`.
Raw evidence is in `/var/folders/2k/r_31n4yj0xv5wjvzq_yjrjkm0000gn/T/measure-processes-evidence-9m8malvp`.
These results verify the fixed-baseline harness contract. They do not provide a performance benchmark or historical descendant accounting.
