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

- `Cargo.lock`: consume merged Core `011e299` cache isolation.
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
  leaves neither a responding daemon nor an owned socket.
- This report.

## Ownership And Cross-Repository Routing

The changes remain inside Hub-owned startup, package admission/supervision,
integration proof, and operator documentation. Core, Web, TUI, TUI Kit,
Workspaces, and Project Pipelines are consumed through their public artifacts
and supported harnesses; none was patched in this run. The Ghostty cache defect
was routed to closed Core ticket `ticket_1784931226_385888`; the durable Web
harness defect was routed to closed Web ticket `ticket_1784938737_759157`.
Their merged artifacts are consumed here without integration-only patches.

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

## Verification

Passed:

- `git diff --check`
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `sh -n script/test-production-package-runtime`
- `ruby -c script/production-package-runtime-evidence`
- Ruby missing and Ruby 2.6 negative preflight checks (both exit 69 with the
  documented remediation)
- Three focused lifecycle/smoke/restart tests through `./test.sh`
- Full `./test.sh`: all default tests passed (one documented ignored
  adversarial test)
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

Unverified behavior or residual risk: none within the approved acceptance
matrix.

## Durable Knowledge

Existing vault guidance covered ownership, exact-Hub launch proof, path-neutral
artifacts, and dependency routing. Missing guidance discovered: vendored
Ghostty builds from Cargo git checkouts need an owner-approved cache-isolation
contract across target directories. The owning Core run established and
captured that durable contract in its crate README, tests, and CI; no separate
vault note was added by this Hub integration run.
