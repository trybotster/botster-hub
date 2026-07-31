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
  macOS/Linux process census, authoritative convergence, reconnect/cleanup
  delta accounting, and idle delivery/reconciliation bounds.
- `script/assert-no-plugin-timers` — source and manifest timer-declaration gate
  with Lua-API and capability-manifest positive controls.
- `script/process-census` — shared macOS/Linux process/zombie scanner consumed
  by both lifecycle and production harnesses, including a real spawned-child
  executable-provenance positive control.
- `script/test-production-package-runtime` — exact zero-timer source baseline,
  caller-owned-Hub resource phases, reconnect/reload/idle/disable generations,
  cross-generation stability comparison, and split live/zombie post-down
  census.
- `script/run-loaded-daemon-lifecycle` and
  `script/run-loaded-daemon-lifecycle-selftest` — selector mapping, explicit
  no-stress/CPU assertion signal, and platform-correct self-test routing.
- `README.md`, `docs/client-protocol.md`, `docs/hub-resource-proof.md`, and the
  committed plan — operator contract, diagnostics, and implementation-aligned
  acceptance criteria.
- `docs/reports/hub-test-support-0.1.17-release-evidence.json` — immutable
  release-content commit, tarball integrity, dependency order, clean-install
  proof, and operator handoff.
- `packages/hub-test-support/daemon-protocol.ts` and `metadata.json` — synced
  Hub-owned npm release assets for the additive protocol field.
- `test.sh` — fails the repository wrapper when the Hub-owned npm assets drift.
- This report.

## Ownership boundaries and cross-repository work

Hub owns the aggregate timer observation, lifecycle projection, host-level
resource policy, exact-package orchestration, cleanup census, and CI selector.
Core remains the sole producer of queue capacity, executor concurrency, live
executor/worker, queued-job, and in-flight-job counters. No Core mechanism or
package repository was edited, and terminal bytes remain on the existing
SessionIo/ClientWorker path.

Hub now owns and completes the source/generated/package-copy portion of the
protocol release chain: `@trybotster/hub-test-support@0.1.17` contains the
synced `plugin_resource_counters` bytes and matching metadata. Human answer
`question_1785521549_236526` prohibits pipeline agents from publishing npm
releases. Follow-up answer `question_1785522029_180355` confirmed the dependency
`@trybotster/ui-contract@0.2.0` is part of the same Hub-owned release batch. The
operator command after reviewing
`docs/reports/hub-test-support-0.1.17-release-evidence.json` is
`script/publish-npm-packages` from the committed Hub root; the script publishes
ui-contract before hub-test-support and stops on failure.

The remaining vendored-byte and exact-pin update is Follow-up
`ticket_1785515827_864108`, routed to the `botster-web` target and registered as
dependency `dependency_1785515833_230748`. It remains blocked until the human
confirms npm serves 0.1.17. This Hub run does not bypass registry artifact
parity or patch Web.

## Deviations from plan

- The Plan Review CPU constraint is now explicit: Linux always records the
  five-second CPU delta, but only the `focused-plugin-resource-bounds` selector
  sets `BOTSTER_ASSERT_IDLE_CPU_BOUND=1`; that selector rejects stressed runs.
- The cleanup runner self-test already documented numeric SID and `setsid`
  fixtures as Linux-only but attempted them on Darwin. It now uses the runner's
  platform launcher for common coverage and skips only those Linux-specific
  fixtures on macOS; Linux CI coverage is unchanged.
- The plan expected the existing evidence helper to gain a new generic
  resource-artifact subcommand. The implementation instead routes bounded JSON
  through the campaign's existing `capture`/`redact_file` path and its existing
  `audit`/`pii-scan` gates; the committed plan now records that narrower design.
- The cleanup census required a semantics-preserving extraction into
  `script/process-census` so both the loaded lifecycle runner and production
  campaign use the same process/zombie scanner. Live survivors use exact
  executable provenance; zombies use a settled pre-campaign baseline because
  `<defunct>` rows retain no argv.

## Verification

Passed:

- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- full `./test.sh`: all default tests passed, including 120/120 executed daemon
  lifecycle tests; one documented local adversarial test remains ignored
- exact focused resource test through `./test.sh`
- the focused test's deliberate wrong-owner-count control, which exits nonzero
  and emits the last authoritative snapshot at its convergence deadline
- Hub client generated-protocol unit suite: 44 passed
- focused timer and reload cleanup tests
- `script/run-loaded-daemon-lifecycle-selftest` on macOS
- `script/assert-no-plugin-timers --self-test`, proving both
  `botster.capabilities.timer_once` and manifest `{ "surface": "timer" }`
  declarations make the gate fail
- `script/process-census --self-test`, proving a real spawned executable is
  visible to the live-survivor oracle
- `node packages/hub-test-support/scripts/sync-assets.mjs --check`
- `script/publish-npm-packages --dry-run`, covering generation/check/test/pack
  for both `@trybotster/ui-contract@0.2.0` and
  `@trybotster/hub-test-support@0.1.17`
- a clean temporary-project install of those exact two tarballs, followed by
  package import, asset verification, protocol-field/hash validation, and
  plugin fixture materialization
- registry checks confirming both target versions still return npm 404
- loaded selector validation with `stress_profile=none`, plus a negative check
  proving `moderate` exits 2
- shell/Ruby syntax checks and `git diff --check`

Downstream-shaped proof passed inside the focused real-daemon test: the
production entrypoint loaded four owners at `256`/`2`, observed exactly eight
executor workers and no more than 64 Hub threads, exercised normal and abrupt
entity reconnects through `script/probe-hub-resources`, reloaded every owner,
retired them stepwise through public disable, and ended with zero timer,
queued-job, and in-flight-job resources.

The exact-coordinate fresh campaign remains correctly blocked before launch:
the Hub-owned 0.1.17 package is publish-ready but is not yet available from npm,
and Web cannot consume or vendor it until the human publication step completes.
No sibling-worktree or local-tarball override is accepted as four-package
production proof.

## Unverified behavior and residual risk

- The exact named-package fresh campaign cannot execute its new phases until a
  human publishes the prepared 0.1.17 artifact and
  `ticket_1785515827_864108` updates Web's generated/vendored protocol and exact
  pin. After that dependency closes, rerun the unchanged campaign and retain
  its redacted evidence bundle.
- The 250 ms Linux CPU assertion is implemented and selector-gated but was not
  executed on this macOS implementation host. The loaded Ubuntu selector is
  the authoritative environment for that threshold.
- No full exact-package slow-consumer phase was added beyond the existing
  focused connection lifecycle regression; the new production probe covers
  normal and abrupt entity reconnects while the existing test remains the
  deterministic slow-consumer oracle.

## Missing vault guidance

Review exposed two missing durable rules and captured them for vault
processing: [[hub generated protocol changes are a four site release chain]]
for producer sync/publish obligations, and [[argv marker censuses cannot see
zombie survivors]] for the required split between live executable provenance
and baseline-diff zombie state. Existing notes already covered Hub/Core
ownership, distinct worker knobs, deterministic conformance, timer worker
semantics, exact binary provenance, subprocess teardown, and cross-session
survivor census. Implementation found no additional durable gap beyond the two
Review captures.
