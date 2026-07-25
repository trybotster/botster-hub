# Implementation Report: First-Party Production Runtime Acceptance

## Routing And Guidance

- Target repository: `botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Applied: [[implementer-playbook]], [[botster-implementer-playbook]],
  [[botster-hub-playbook]], its required atomic notes, the runtime/package
  launch and readiness notes named by the approved plan, and
  [[project-pipelines-playbook]].
- Assumption: exact repository `origin/main` revisions and repository-supported
  locked build/test paths are authoritative. No dependency override or sibling
  checkout discovery is permitted.

## Files Changed

- `Cargo.lock`: consume merged Core `49159e7`, including cache isolation,
  idempotent natural-exit shutdown, and retained terminal egress.
- `README.md`: production acceptance prerequisite and current runnable-entrypoint
  operator contract.
- `docs/client-protocol.md`: cross-repository production proof contract.
- `docs/plans/prove-first-party-production-runtime-has-no-dogfood-path.md`:
  approved plan and resolved README prose finding.
- `script/production-package-runtime-evidence`: path-neutral revision, artifact,
  audit, snapshot, redaction, and PII evidence helper.
- `script/test-production-package-runtime`: seven-repository fresh/upgrade
  runtime campaign, explicit Ruby preflight, dynamic/explicit/invalid Web port
  proof, Web-owned browser reload/reconnect against both runtime legs, and
  bounded cleanup.
- `src/main.rs`: shut down a newly started owned daemon when `up` fails during
  package refresh/admission/launch.
- `tests/hub_daemon_lifecycle_test.rs`: regression proof that failed startup
  leaves neither a responding daemon nor an owned socket, plus downstream proof
  of clean-exit cross-connection delivery: shutdown from a second connection
  preserves final output and exactly one `ProcessExit` for the attached
  terminal subscription.
- This report.

## Ownership And Cross-Repository Routing

The changes remain inside Hub-owned startup, package admission/supervision,
integration proof, and operator documentation. Core, Web, TUI, TUI Kit,
Workspaces, and Project Pipelines are consumed through their public artifacts
and supported harnesses; none was patched in this run. The Ghostty cache defect
was routed to closed Core ticket `ticket_1784931226_385888`; the durable Web
harness defect was routed to closed Web ticket `ticket_1784938737_759157`.
Their merged artifacts are consumed here without integration-only patches.
The production campaign subsequently exposed two Core-owned natural-exit
shutdown defects. They were routed through closed Core tickets
`ticket_1784955886_116612` and `ticket_1784997182_148130`; merged Core
`49159e7` is consumed here. Hub and Web production behavior was not weakened or
given a compatibility path.

## Deviations From Plan

- The macOS `$TMPDIR` path exceeded Unix socket `SUN_LEN`; the harness now uses
  `/tmp` for the intentionally short isolated runtime root, as the plan
  required.
- The Plan Review finding expanded the README correction to its introductory
  prose. The committed plan was synchronized before handoff.
- Review identified missing cold-build redaction coordinates, product-surface
  extensions, failed-launch cleanup assertions, operator-root coverage, and a
  shell matcher control. Those proof defects were corrected without changing
  product runtime ownership.
- The new failed-launch assertion exposed a Hub-owned lifecycle defect rather
  than a harness-only gap. The committed plan now records the runtime and test
  changes permitted by its “actual Hub-owned defect” clause.
- The merged Core fix isolates Zig caches for current builds but cannot alter
  the immutable pre-cutover Core commit. The deterministic historical producer
  now supplies target-contained Zig local/global cache directories through the
  same public environment contract, together with isolated Cargo home and
  target directories, so it neither consumes nor mutates the operator's shared
  legacy checkout cache.
- The real upgraded state exposed that Web's live harness assumed an empty
  package/session registry. Closed Web ticket `ticket_1784938737_759157`
  corrected the caller-owned durable-data path. The final campaign consumes
  merged Web `e044484` and keeps the durable state intact rather than pruning,
  removing/reinstalling, or editing it to force the proof.
- The unchanged campaign then exposed a natural-exit shutdown race and, after
  that was fixed, loss of the attached subscription's terminal egress during
  successful Core recovery. Both were routed to Core rather than hidden in Hub
  classification or Web retry behavior. Hub's added coverage exercises the
  clean-exit contract: attach on connection A, allow a controlled natural exit,
  shut down on connection B, then prove A receives final output and one
  `ProcessExit`, with no duplicate on its next drain. It does not induce Core's
  shutdown-recovery capture-failure branch. The discriminating regression
  oracle is the unchanged Web production campaign: it failed at Core `2e494b8`
  with a terminal-detach timeout and passes at Core `49159e7`.

## Verification

Passed:

- `git diff --check`
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `sh -n script/test-production-package-runtime`
- `ruby -c script/production-package-runtime-evidence`
- Ruby missing and Ruby 2.6 negative preflight checks (both exit 69 with the
  documented remediation)
- Three focused lifecycle/smoke/restart tests through `./test.sh`
- Focused locked clean-exit cross-connection terminal-delivery coverage:
  passed.
- Full `./test.sh --locked`: all default tests passed, including 97 daemon
  lifecycle tests (one documented ignored adversarial test).
- Fresh cross-repository acceptance through current builds, artifact parity,
  package install/enable/refresh, dynamic/explicit/invalid Web ports, Web
  health/HTML/browser/WebRTC, TUI live runtime, plugin tools, status, doctor,
  smoke, and cleanup. The refreshed auditable evidence bundle is required
  before this claim is handed back to Review. The corrected invocation exited
  0 with `production_package_runtime=pass` at Hub commit `53a11032`; durable
  Project Pipelines evidence is `artifact_1784933087_975596`, which embeds the
  exact seven-repository revisions, command log, artifact/port/audit/PII
  results, operator-root manifests, and runtime summary.
- Final upgrade acceptance exited 0 with
  `production_package_runtime=pass` at Hub `307adae`, Core `011e299`, and Web
  `e044484`. It proves the exact historical producer, untouched durable state,
  automatic refresh, readiness/health/UI, TUI, plugin tools, session
  spawn/list/attach/input/resize/readback/lifecycle, Hub restart/adoption,
  browser reload/reconnect, smoke, down/cleanup, operator-state isolation,
  artifact parity, seven-repository product-surface audit, and PII scan.
  Durable Project Pipelines evidence, including the exact path-neutral
  invocation and command log, is `artifact_1784954240_655051`.
- Final post-dependency all-mode acceptance exited 0 with
  `production_package_runtime=pass` at clean exact revisions Hub
  `4f494cb47179e50ac547f0ae73cf8d8dff3bac59`, Core
  `49159e7373ffc2cdbb26c856bb3c738841a42742`, Web
  `e044484a0ff719dcec2ed753e25eba545faacb95`, TUI
  `fd68331e09dbba709b276dc650cbaecd90d73631`, TUI Kit
  `4961e141d76020e53e6db8c80b85539aa26f2a3a`, Workspaces
  `d4dcf3b9be4d1613db89477217d98634212c6aca`, and Project Pipelines
  `3e4a3c08c8e0c34fcfa29ebb58f814b035db9384`. Both fresh and untouched
  durable-upgrade paths passed the complete production workflow; the upgraded
  Web live-browser proof that previously timed out now passed unchanged.
- Independent Verify reran the complete all-mode campaign at exact clean PR
  head `b8831a3073f8d59dd776fcdc9837d976c5fce9cd` with the same downstream
  revisions and exited 0. Durable artifact `artifact_1785008382_573531`
  includes the redacted command log, revision and upgrade manifests, Web port
  proof, operator-state manifests, artifact parity, product-surface audit, and
  PII scan.

Residual risk: Hub's ordinary automated suite covers clean-exit
cross-connection delivery but has no negative control that forces Core's
shutdown-recovery capture-failure branch. Core owns the discriminating
daemon-level tests. The real first-party user path is verified by the
single-variable failed-before/passed-after Web production campaign pair.

## Durable Knowledge

Existing vault guidance covered ownership, exact-Hub launch proof, path-neutral
artifacts, and dependency routing. Missing guidance discovered: vendored
Ghostty builds from Cargo git checkouts need an owner-approved cache-isolation
contract across target directories. The owning Core run established and
captured that durable contract in its crate README, tests, and CI; no separate
vault note was added by this Hub integration run.
