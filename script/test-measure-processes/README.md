# Darwin sampler checks

Run the focused checks from the Hub repository:

```sh
python3 script/test-measure-processes/run.py
```

The command compiles the sampler and its unit checks with Clang warnings treated as errors.
It prints the directory containing compiler logs and raw JSONL test evidence.
It checks Mach tick conversion, identity changes, PID reuse, reparenting, observed exits, and unresolved child accounting.
It then samples an owned root and child twice.
The test keeps both processes live through the final sample.
The test closes their input pipes after sampling.
Neither the sampler nor the test sends signals to target processes.

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

Sampling errors conservatively invalidate observations, including errors for processes whose ancestry the sampler cannot establish.
Reparenting, disappearance without an observed exit, and changes to reaped-child CPU counters also mark unresolved turnover.
The summary always marks lifecycle accounting invalid because polling cannot establish complete lifecycle coverage.
Exit code `2` indicates invalid observations or invalid arguments.
Exit code `0` does not establish complete lifecycle accounting.

These checks verify the harness. They do not run a performance benchmark or define an aggregate accounting contract.

## Executed check, 2026-09-09

The focused command passed with Apple Clang 17.0.0 (`clang-1700.6.4.2`) on `arm64-apple-darwin25.5.0`.
The measured Mach timebase was `125/3`.
Both owned processes appeared in both samples and remained live through the final sample.
The census reported 466 `rusage_before` errors with errno `1` and 256 `bsd_metadata` errors with errno `3`.
The sampler returned exit code `2` and marked observations invalid.
The harness accepted this explicit failure evidence; it did not classify the run as valid lifecycle accounting.

The wrapper log is `/private/tmp/measure-processes-check-1.log`.
The compiler version is recorded in `/private/tmp/measure-processes-compiler-1.log`.
Raw test evidence is in `/var/folders/2k/r_31n4yj0xv5wjvzq_yjrjkm0000gn/T/measure-processes-evidence-8tcg4rx4`.
