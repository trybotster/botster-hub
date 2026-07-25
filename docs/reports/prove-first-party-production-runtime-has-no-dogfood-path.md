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
and supported harnesses; none was patched. The reproducible pre-cutover Ghostty
build failure was routed to Core ticket `ticket_1784931226_385888` and attached
as a blocking dependency instead of adding an integration workaround.

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
- Web's supported live browser harness owns install/enable for `botster-web`
  and rejects an already-installed record. After the upgraded runtime has
  already proven automatic refresh, restart/adoption, sessions, and smoke, the
  campaign removes only `botster-web` through Hub's public package command and
  hands the same durable data directory to Web's harness. No package reload or
  direct state edit is used.

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

Unverified/blocking:

- The deterministic upgrade producer cannot currently be built. Two campaign
  attempts fail when the vendored Ghostty Zig build cannot spawn its generated
  `uucode_build_tables` executable. Review's control reproduced the same exit
  101 at current locked Core `16bf08f` with a fresh `CARGO_TARGET_DIR`; the
  defect is target-directory/cache-specific, not pre-cutover-specific. The
  corrected owner ticket is `ticket_1784931226_385888`.
- Upgrade browser reload/reconnect is now wired through Web's supported live
  packaged-protocol harness with `BOTSTER_LIVE_DATA_DIR` pointing at the same
  upgraded durable runtime. It remains unexecuted behind the Core blocker.
  Sessions list and resize are also explicit in the upgrade leg; the complete
  upgrade matrix still must run before ticket acceptance.

## Durable Knowledge

Existing vault guidance covered ownership, exact-Hub launch proof, path-neutral
artifacts, and dependency routing. Missing guidance discovered: vendored
Ghostty builds from Cargo git checkouts need an owner-approved cache-isolation
contract across target directories. No vault note was captured here because
the owning Core investigation has not yet established the durable fix.
