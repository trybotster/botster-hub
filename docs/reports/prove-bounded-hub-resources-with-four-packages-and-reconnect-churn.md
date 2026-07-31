# Implementation report: bounded Hub resources

## Target and guidance

- Target repository: `trybotster/botster-hub`
- Target ID: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Applied in order: [[implementer-playbook]],
  [[botster-implementer-playbook]], [[botster-hub-playbook]], the exact atomic
  notes named in the committed plan, and the Hub customization skill.
- Project Pipelines repository guidance was not applied because no Project
  Pipelines package/plugin source or workflow policy was edited.
- Assumption: the four-package campaign may consume exact clean package
  revisions, but any generated-protocol consumer change remains owned by that
  consumer repository.

## Files changed

- `.github/workflows/loaded-daemon-lifecycle.yml` — exposes the unstressed
  focused resource selector.
- `crates/botster-hub-client/src/lib.rs`,
  `crates/botster-hub-client/src/typescript.rs`, and
  `crates/botster-hub-client/generated/daemon-protocol.ts` — additive optional
  sanitized plugin-resource counters and generated contract coverage.
- `src/capabilities.rs`, `src/runtime.rs`, `src/client_api.rs`,
  `src/daemon_transport.rs`, `src/local_webrtc.rs`, and `src/lib.rs` — live
  active-timer observation projected through the existing lifecycle request.
- `tests/hub_capability_runtime_test.rs`,
  `tests/hub_plugin_lifecycle_test.rs`, and
  `tests/hub_daemon_lifecycle_test.rs` — zero/nonzero timer proof, reload
  cleanup, four-owner production-default bounds, reconnects, public reload,
  stepwise disable, OS threads, and the public-protocol probe.
- `script/probe-hub-resources` — bounded standard-library daemon-protocol and
  macOS/Linux process census.
- `script/test-production-package-runtime` — exact zero-timer source baseline,
  caller-owned-Hub resource phases, reconnect/reload/idle/disable generations,
  and cross-session post-down census.
- `script/run-loaded-daemon-lifecycle` and
  `script/run-loaded-daemon-lifecycle-selftest` — selector mapping, explicit
  no-stress/CPU assertion signal, and platform-correct self-test routing.
- `README.md`, `docs/client-protocol.md`, `docs/hub-resource-proof.md`, and the
  committed plan — operator contract, diagnostics, and implementation-aligned
  acceptance criteria.
- This report.

## Ownership boundaries and cross-repository work

Hub owns the aggregate timer observation, lifecycle projection, host-level
resource policy, exact-package orchestration, cleanup census, and CI selector.
Core remains the sole producer of queue capacity, executor concurrency, live
executor/worker, queued-job, and in-flight-job counters. No Core mechanism or
package repository was edited, and terminal bytes remain on the existing
SessionIo/ClientWorker path.

The exact production campaign stopped at its pre-launch artifact gate because
Web's pinned/vendored protocol does not yet contain
`plugin_resource_counters`. Follow-up `ticket_1785515827_864108` is routed to
the `botster-web` target and registered as dependency
`dependency_1785515833_230748`. This Hub run does not bypass artifact parity or
patch Web.

## Deviations from plan

- The Plan Review CPU constraint is now explicit: Linux always records the
  five-second CPU delta, but only the `focused-plugin-resource-bounds` selector
  sets `BOTSTER_ASSERT_IDLE_CPU_BOUND=1`; that selector rejects stressed runs.
- The cleanup runner self-test already documented numeric SID and `setsid`
  fixtures as Linux-only but attempted them on Darwin. It now uses the runner's
  platform launcher for common coverage and skips only those Linux-specific
  fixtures on macOS; Linux CI coverage is unchanged.
- The plan expected Web's protocol artifact to remain directly consumable.
  The additive DTO made that assumption false, so the committed plan now names
  the separately routed Web dependency.

## Verification

Passed:

- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- full `./test.sh`: all default tests passed, including 120/120 executed daemon
  lifecycle tests; one documented local adversarial test remains ignored
- exact focused resource test through `./test.sh`
- Hub client generated-protocol unit suite: 44 passed
- focused timer and reload cleanup tests
- `script/run-loaded-daemon-lifecycle-selftest` on macOS
- loaded selector validation with `stress_profile=none`, plus a negative check
  proving `moderate` exits 2
- shell/Ruby syntax checks and `git diff --check`

Downstream-shaped proof passed inside the focused real-daemon test: the
production entrypoint loaded four owners at `256`/`2`, observed exactly eight
executor workers and no more than 64 Hub threads, exercised normal and abrupt
entity reconnects through `script/probe-hub-resources`, reloaded every owner,
retired them stepwise through public disable, and ended with zero timer,
queued-job, and in-flight-job resources.

The exact-coordinate fresh campaign was invoked at the committed Hub revision
with clean exact Core, Web, TUI, TUI Kit, Workspaces, and Project Pipelines
inputs. It correctly stopped before launch with `installed Hub test-support
artifact does not match exact Hub source and Web vendored bytes`; therefore it
did not produce a false four-package pass.

## Unverified behavior and residual risk

- The exact named-package fresh campaign cannot execute its new phases until
  `ticket_1785515827_864108` updates Web's generated/vendored protocol and
  pinned Hub test-support artifact. After that dependency closes, rerun the
  unchanged campaign and retain its redacted evidence bundle.
- The 250 ms Linux CPU assertion is implemented and selector-gated but was not
  executed on this macOS implementation host. The loaded Ubuntu selector is
  the authoritative environment for that threshold.
- No full exact-package slow-consumer phase was added beyond the existing
  focused connection lifecycle regression; the new production probe covers
  normal and abrupt entity reconnects while the existing test remains the
  deterministic slow-consumer oracle.

## Missing vault guidance

No missing durable guidance was found. Existing notes covered Hub/Core
ownership, distinct worker knobs, deterministic conformance, timer worker
semantics, exact binary provenance, subprocess teardown, and cross-session
survivor census. No new vault note was captured because the implementation did
not establish a reusable method beyond those existing conventions.
