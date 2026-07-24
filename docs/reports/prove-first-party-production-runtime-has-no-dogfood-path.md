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
  proof, and bounded cleanup.
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
  smoke, and cleanup.

Unverified/blocking:

- The deterministic upgrade producer cannot currently be built. Two clean
  campaign attempts fail at exact pre-cutover Hub `823ded16`, locked Core
  `879f55e6`, when the vendored Ghostty Zig build cannot spawn its generated
  `uucode_build_tables` executable from the Cargo git checkout cache. This is
  the sole incomplete downstream proof and is tracked by
  `ticket_1784931226_385888`.

## Durable Knowledge

Existing vault guidance covered ownership, exact-Hub launch proof, path-neutral
artifacts, and dependency routing. Missing guidance discovered: reproducible
pre-cutover builds that compile vendored Ghostty from Cargo git checkouts need
an owner-approved cache-isolation contract. No vault note was captured here
because the owning Core investigation has not yet established the durable fix.
