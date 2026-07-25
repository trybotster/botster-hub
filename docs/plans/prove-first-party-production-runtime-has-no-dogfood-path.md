# Prove The First-Party Production Runtime Has No Dogfood Path

## Target Repository And Context

- Target repository: `botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Pipeline ticket: `ticket_1784854143_789468`
- Pipeline run: `run_1784928695_310519`
- Repository charter: [[botster-hub-playbook]]
- The target was resolved through the admitted Botster spawn-target registry. The
  ambient worktree was not used as routing authority.
- Current pipeline context was inspected for the ticket, run, active step, gate,
  dependencies, reviews, findings, artifacts, questions, and durable answers.
  The seven declared dependencies are closed:
  - Hub production package runtime
  - Web production Hub/package runtime
  - TUI production client/runtime identity
  - TUI Kit product-neutral vocabulary
  - Core runnable-entrypoint Hub connection descriptor
  - published Hub test-support release checkpoint
  - Web consumption of that published release
- The current Plan context began with no artifacts, reviews, findings, or open
  questions. It retained the earlier human decision about deterministic upgrade
  fixtures and vocabulary-audit exclusions.
- The current branch already contains the prior-run implementation in commits
  `bc64f29` and `10e0e76`, with open PR #164. A new blocking human answer
  requires Implement to reuse that work, integrate it onto exact current main,
  inspect the resulting diff, and generate fresh acceptance evidence. Prior-run
  evidence is context only.

## Playbooks And Notes Loaded

Loaded in the required order:

1. [[planner-playbook]]
2. [[botster-planner-playbook]]
3. [[botster-hub-playbook]]
4. Task surface and atomic guidance:
   - [[botster-architecture]]
   - [[cli-patterns]]
   - [[spa-patterns]]
   - [[botster hub is a first party host profile over core]]
   - [[botster hub gravity must be watched before it becomes the new monolith]]
   - [[botster data plane bypasses the hub through session and client actors]]
   - [[botster local client api lives over hubruntime not raw core routers]]
   - [[botster hub events use bounded priority lanes instead of unbounded queue fuses]]
   - [[may supervise permits the hub to supervise the package entrypoint]]
   - [[hub supervision admission changes require exact live hub launch proof]]
   - [[webrtc bootstrap origin must be requested after the package server binds]]
   - [[cold turkey migrations eliminate dual code paths and version suffixes]]
   - [[botster hub daemon startup requires explicit data dir]]
   - [[botster hub no arg summary must not touch durable home state]]
   - [[botster hub socket liveness requires a protocol handshake]]
   - [[botster hub smoke cli entrypoints stay thin explicit and facade backed]]
   - [[botster host injected runtime paths are absolute before package cwd boundaries]]
   - [[botster runtime artifact resolution should be read only]]
   - [[durable package snapshots must reconstruct admission through live helpers]]
   - [[botster runnable entrypoints are hub owned launch contracts]]
   - [[installed apps are daemon app rows projected from package runnable entrypoints]]
   - [[manifest required injections must be consumed by the launched runtime]]
   - [[closed dependency tickets signal merged source not a consumable release]]
   - [[cross repo dependency registration must use dependency repo target]]
   - [[hub test support npm releases need external consumer smoke]]
   - [[botster web generated protocol drift checks need explicit hub artifact paths]]
   - [[conformance fixture revisions must be unique per published content]]
   - [[empty gate output is not success without a valid exit status]]
   - [[botster review and verify must scan all committed artifacts for pii]]
   - [[pipeline artifacts should use path neutral worktree references]]
   - [[botster orchestration should spawn agents with explicit target ids]]
   - [[botster orchestration prompts must bind agents to explicit worktrees]]
   - [[plan agents must author vault context as wikilinks not home paths]]
   - [[vault example paths are not repository placement conventions]]
   - [[botster-runtime-reviewer-playbook]]
   - [[botster-package-reviewer-playbook]]
   - [[botster-runtime-verifier-playbook]]
   - [[botster-package-verifier-playbook]]
5. [[project-pipelines-playbook]], because this run uses Project Pipelines gates,
   artifacts, questions, and checklists and the acceptance stack includes the
   first-party Project Pipelines package.

Also loaded: [[identity]], [[goals]], the vault runtime/methodology guidance, the
ticket's durable human answer, repository `README.md`, current ADRs and protocol
docs, `Cargo.toml`/`Cargo.lock`, `test.sh`, the production package runtime
script, current CI, the Hub CLI production entrypoints, lifecycle tests, and the
supported build/test/package manifests for Core, Web, TUI, TUI Kit, Workspaces,
and Project Pipelines.

## Repository Evidence

The integration seam already has a Hub-owned starting point:

- `script/test-production-package-runtime` accepts explicit Web and TUI package
  paths, records revisions, creates a short temporary data directory, installs
  packages through the daemon, runs `up`, checks Web health/HTML, opens the TUI,
  runs `status`, `doctor`, `smoke`, and shuts down.
- `src/main.rs` owns the production `up`, `down`, `doctor`, `smoke`, package/app,
  and session commands. `up` refreshes directly installed local packages before
  launching enabled entrypoints and reports the supervised Web app's structured
  `local_url`.
- `tests/hub_daemon_lifecycle_test.rs` already proves focused startup/reuse,
  atomic package refresh, unchanged Web runtime preservation, smoke, daemon
  restart/session adoption, and cleanup behavior.
- Web's supported live packaged-protocol harness uses the exact Hub and worker
  binaries, dynamic package-server readiness, a real browser, WebRTC, session
  lifecycle, attach/input/readback, reload/reconnect, resize, terminal
  rendering, and clean shutdown.
- TUI's supported live-Hub test and `--headless-live-runtime` path exercise the
  production package launch descriptor without a timing-based interactive quit.
- Workspaces and Project Pipelines each expose `script/test` plus real Hub
  package acceptance guidance.

The current run worktree is based on merged main
`0484ca8653d3b77679d5c8d4600742e99f1c7c91`.
The exact pre-cutover Hub producer revision is its first parent,
`823ded16f148ef8655c71344cde0d2e4b3dd951c`. Execution must fetch and resolve
all repository main refs again; these observed SHAs are planning evidence, not
permission to use stale revisions.

The worktree is stale relative to authoritative Hub main. Remote main currently
resolves to `42a2f1df02f69719d5a7a47216fb22515c1c0762`, which merged the
Hub test-support release preparation after this branch diverged. Implement must
integrate current main before changing or executing the prior-run implementation.

- The earlier artifact-coupled mismatch is now resolved in merged source and
  published-consumer state:
- Current Hub main's
  `crates/botster-hub-client/generated/daemon-protocol.ts` hashes to
  `39e9202bd333584be077e1d1ef5c3fa31a9409996607cb4c01471c103e263980`
  and includes `refresh_local_packages`.
- Web main `5b1bbdb17fc835580c9c7a6a88e09ffebdacf5a9` pins the published
  `@trybotster/hub-test-support@0.1.11`; its vendored protocol has the same
  `39e920...` hash as Hub source.
- Closure and local hashes are prerequisite evidence, not final acceptance.
  The current run must still perform a clean external install and package asset
  verification before any Web runtime leg.

## Scope

Make the existing Hub-owned production acceptance harness conclusive and use it
to produce durable evidence:

1. Reuse commits `bc64f29` and `10e0e76`; do not rewrite the completed
   implementation. Integrate them onto exact current Hub main, including
   `42a2f1d` or its fetched successor, resolve drift without compatibility
   scaffolding, and inspect the complete resulting diff before execution.
2. Preserve the existing extension of
   `script/test-production-package-runtime` rather than creating a parallel
   launcher. Require explicit repository/package coordinates and expected
   revisions for Hub, Core, Web, TUI, TUI Kit, Workspaces, and Project
   Pipelines. Reject dirty, non-Git, or revision-mismatched inputs before
   starting processes. Preflight Ruby 2.7 or newer with an exact installation
   remediation before using the stdlib-only evidence helper.
3. Before any expensive build or Web runtime leg, gate every artifact-coupled
   first-party seam against the exact upstream source of truth. For
   `@trybotster/hub-test-support`, install the declared registry coordinate in a
   clean external consumer and require:
   - installed `metadata.json` package version equals the declared Web
     dependency;
   - installed `daemon_protocol.sha256` and actual installed
     `daemon-protocol.ts` bytes equal the exact Hub main generated protocol;
   - installed `conformance_fixture_revision` equals
     `botster_hub_client::CONFORMANCE_FIXTURE_REVISION` at the exact Hub SHA;
   - contract-defining fixture bytes verify through the package's public asset
     checker, not revision metadata alone.
   Record upstream, metadata, installed-artifact, and Web-vendored hashes. Treat
   `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` as diagnostic comparison only, never the
   acceptance path.
4. Build every required artifact through its repository-supported locked path:
   Hub and session worker through Hub/Core Cargo, Web through `npm run build`,
   TUI through locked Cargo, and the plugin repositories through their own
   `script/test` checks. Do not inject local dependency overrides; lockfiles and
   declared Git revisions remain authoritative.
5. Run the complete current runtime from a new short temporary data directory:
   install and enable the four first-party packages, run `up`, prove automatic
   local-package refresh, and exercise public package, app, session, status,
   doctor, smoke, and down contracts.
6. Prove the Web user path with Web's supported live packaged-protocol harness
   against the exact current Hub/worker binaries and current Workspaces package.
   Require the dynamically bound structured `local_url`, `/health`, HTML shell,
   browser render, WebRTC connection, session lifecycle, terminal
   attach/input/readback/resize, browser reload/reconnect, and cleanup.
7. Prove the TUI package launch through `apps open` with its supported headless
   live-runtime mode, so launch-context, Hub connection decoding, session
   lifecycle, terminal input/readback, and exit are event-driven rather than a
   fixed-delay Ctrl-Q injection.
8. Exercise Workspaces and Project Pipelines as real installed packages. Verify
   their enabled package rows, plugin workers, navigation/surface descriptors,
   and registered public tools. Use the package-owned test/harness for workflow
   semantics rather than moving workflow policy into Hub.
9. Generate the durable upgrade fixture with the exact pre-cutover Hub revision
   `823ded16f148ef8655c71344cde0d2e4b3dd951c` and the corresponding pre-cutover
   package revisions in isolated temporary Git worktrees. Record every producer
   SHA and setup command. Stop the old daemon cleanly, change only the package
   worktrees to the exact current merged revisions, and open the untouched data
   directory with the exact current Hub binary. Prove package refresh,
   admission reconstruction, startup, Web/TUI launch, session recovery, public
   status/doctor/smoke, and clean down without a compatibility alias or manual
   state edit.
10. Record exact repository revisions, resolved first-party dependency revisions,
   build commands, runtime commands, dynamically allocated endpoints, data-dir
   mode (`fresh` or `upgrade`), observable readiness/lifecycle evidence, and
   cleanup results in one machine-readable evidence directory plus the
   implementation report. Attach the evidence to Project Pipelines.
11. Run two separate cold-turkey audit gates:
    - Vocabulary: scan current product surfaces in all seven repositories for
      `dogfood` case-insensitively across source, tests, executable scripts,
      manifests, README, current architecture/reference/operator docs, UI copy,
      identifiers, and supported examples. Exclude retained `docs/plans/**`,
      `docs/reports/**`, and Git history. Any ADR or document presented as
      current guidance remains in scope regardless of directory.
    - Rules evidence matrix:
      - compatibility aliases and old modes: targeted absence checks for the
        deleted Hub `dogfood`/`dev-stack` CLI branches and deleted Web runtime
        mode identifiers;
      - old environment/query contracts: targeted tokens such as
        `BOTSTER_WEB_DOGFOOD_*`, `VITE_BOTSTER_REAL_HUB_DOGFOOD`, and
        `?dogfood=`; do not broadly reject `BOTSTER_HUB_SOCKET`, whose
        session-template and generic environment-map uses remain valid;
      - dynamic ports: without a port override, start two isolated current Web
        package runtimes concurrently and require distinct nonzero ports in
        their structured `local_url` results plus passing `/health` and HTML
        shell checks. This is the load-bearing no-hard-coded-port assertion;
      - retired fixed-port control: occupy port `41739`, the fixed port named by
        the Web productionization ticket, while running the dynamic proof. This
        is a secondary negative control, not the source of the dynamic-binding
        claim. Do not use `5173` as product provenance; it comes from the stale
        Hub README example that this ticket removes;
      - explicit port override: choose an available nonzero port, launch the
        current supervised Web package with
        `BOTSTER_WEB_PACKAGE_SERVER_PORT=<port>`, require structured
        `local_url` to report exactly that port, and require `/health` plus the
        HTML shell to pass. Then launch with a non-integer or out-of-range value
        and require the package-owned `invalid_package_server_port` diagnostic
        instead of fallback. Record the environment name plus both dynamic and
        explicit ports in the evidence bundle;
      - sibling paths: reject runtime/script/manifest discovery through
        `../botster-*`; every checkout/package path comes from explicit harness
        input;
      - local dependency overrides: inspect manifests and lockfiles for
        out-of-repository path/file/patch overrides while allowing internal
        workspace paths, then prove clean registry/Git resolution;
      - sleeps as readiness: every wait must name an observable protocol,
        health, lifecycle, browser, terminal, or process-exit predicate with a
        bounded deadline. Intentional PTY fixture sleeps are classified
        separately and cannot satisfy readiness;
      - hidden manual reload: after advancing the package worktrees, current
        `up` alone must refresh daemon-visible package/app state before launch;
        no `packages reload` or manual file edit may intervene.
    Every silent matcher must preserve diagnostics, distinguish “no matches”
    from command failure, and run a known-positive control for its scope.
12. Correct the stale runnable-entrypoint operator contract in `README.md`.
    Correct the introductory field summary so it names only `web_app` and
    `terminal_app`, uses `launch_mode` with `background` and
    `foreground_stdio`, and includes the structured `injections` and
    `readiness` fields.
    Replace the old `kind: web` / `mode: dev` /
    `BOTSTER_WEB_PORT=5173` example with the current `web_app`,
    `launch_mode: background`, required structured `hub_connection` injection,
    empty manifest environment, `readiness.result_fields: ["local_url"]`, and
    `may_supervise: true` shape. Delete the obsolete claim that entrypoints are
    always `not_started` and are not spawned, supervised, restarted, or
    health-checked. This is cleanup required by the ticket's current
    operator-documentation acceptance surface, not adjacent documentation work.

## Non-Scope

- No compatibility alias for removed commands, environment variables, query
  parameters, or runtime modes.
- No restoration of the deleted Hub launch path and no integration-only
  workaround for a repository-owned defect.
- No Hub ownership of terminal bytes, client DTOs, Web/TUI rendering policy,
  Workspaces state, or Project Pipelines workflow policy.
- No local path override of Cargo/npm dependencies to force repositories to
  consume nearby checkouts. Record locked dependency SHAs and route a mismatch
  to the owning repository.
- No broad refactor of `src/main.rs`, package policy, daemon transport, or the
  external client repositories merely to make the harness convenient.
- No mutation of a user's real Botster state. The upgrade directory is a
  deterministic isolated fixture, and the evidence must be path-neutral. The
  harness must snapshot the resolved default/home Botster state root before and
  after both legs and prove its metadata and content are unchanged.
- No blocking scan of retained implementation-history plans/reports. They are
  not supported product paths per the durable human decision.

## Ownership Boundaries And Cross-Repository Dependencies

- `botster-hub` owns this integration harness because it owns startup
  composition, package admission/refresh/supervision, explicit data-directory
  lifecycle, the local client API, and exact-Hub launch proof.
- `botster-core` owns the worker/runtime mechanisms and typed Hub connection
  descriptor. This run consumes Core through the Hub lockfile and exact worker
  build; it does not patch Core.
- `botster-web` owns the production package server, browser client, WebRTC
  bootstrap consumption, browser render, and browser live harness.
- `botster-tui` and `botster-tui-kit` own terminal client policy and reusable
  rendering/input behavior. The integration run launches the supported TUI
  package path and does not replace its harness.
- `botster-workspaces` and `botster-project-pipelines` own their plugin state,
  tools, surfaces, and workflow semantics. Hub only installs, enables, and
  hosts them.
- All seven existing dependency tickets are closed. If execution exposes a
  defect in any external repository, create a dependency ticket against that
  repository's exact target and stop the affected acceptance leg. Do not patch
  it in this Hub run.
- The artifact mismatch was pre-routed and its dependencies are now closed:
  - release checkpoint `ticket_1784916883_931144` on Hub target
    `tgt_7e208a0c76a44980a83b63af976b1f22` published
    `@trybotster/hub-test-support@0.1.11`;
  - `ticket_1784912421_508855` on Web target
    `tgt_40abcf71ccf049f4ac0c99953a799869` consumed that release and
    re-vendored the authoritative protocol.
  - The current run still proves installed artifact, exact Hub source, and Web
    vendored bytes match before Web build/browser execution.

## Assumptions And Unknowns

- Durable human decision: the upgrade leg uses a deterministic fixture produced
  by the exact pre-cutover Hub revision, not a private local data directory.
- Durable human decision: retained `docs/plans/**` and `docs/reports/**` are
  historical implementation artifacts excluded from the blocking vocabulary
  scan; current ADR/reference/operator docs are included.
- Durable human decision: reuse the prior-run implementation commits, integrate
  exact current main, and proceed through normal Implement, Review, and Verify.
  Do not restart or rewrite already-completed work; prior evidence cannot
  substitute for fresh current-run acceptance.
- Assumption and explicit execution prerequisite: preserve the existing
  stdlib-only Ruby evidence helper rather than rewrite prior completed work.
  The harness requires Ruby 2.7 or newer for `filter_map`; it adds no gem. It
  must fail before builds with an exact Ruby installation/version remediation,
  and `README.md` must document that prerequisite beside the acceptance command.
- Assumption: a repository's supported locked build is authoritative. The run
  records both the top-level current-main SHA and resolved first-party
  dependency SHAs, except that artifact-coupled seams additionally require
  installed-content parity with upstream source. It does not override a
  TUI/Core/Hub/TUI-Kit pin merely to make all SHAs equal.
- Assumption: the pre-cutover package revisions are the first parents of their
  productionization merges. The implementer must resolve and record them from
  Git topology rather than copy abbreviated SHAs from this plan.
- Unknown: whether the pre-cutover Web/TUI package worktrees can be advanced in
  place and refreshed by current `up` without another repository defect. That
  is the behavior the upgrade leg must prove, not assume.
- Unknown: whether one external package's supported harness exposes a missing
  browser/TUI binary prerequisite on the execution host. Missing prerequisites
  are failures with exact remediation, not waived proof.
- Known resolved prerequisite: Hub published
  `@trybotster/hub-test-support@0.1.11` and current Web main consumes it. The
  clean external artifact-availability gate remains blocking runtime evidence.

## Affected Surfaces And Files

Existing Hub-owned implementation to preserve and integrate:

- `script/test-production-package-runtime`
  - expand from fresh Web/TUI smoke to revision-pinned seven-repository
    orchestration, fresh/upgrade legs, four-package install/refresh, supported
    Web/TUI live proofs, dynamic and explicit Web-port proofs, Ruby preflight,
    evidence capture, and complete cleanup.
- `README.md`
  - document the final explicit-path acceptance command, evidence contract,
    Ruby 2.7+ prerequisite, fresh/upgrade coverage, and supported
    product-surface audit boundary; replace the stale runnable-entrypoint
    example and obsolete no-supervision claim with the current production
    contract.
- `docs/client-protocol.md`
  - update the cross-repository production-runtime proof only if its current
    acceptance description becomes inaccurate.
- `docs/plans/prove-first-party-production-runtime-has-no-dogfood-path.md`
  - this reviewable plan artifact.
- `script/production-package-runtime-evidence`
  - narrow filesystem/Git evidence helper for snapshot comparison, artifact
    parity, seven-repository audit, revision manifests, redaction, and PII
    checks.

Implementation-stage artifact still required:

- `docs/reports/prove-first-party-production-runtime-has-no-dogfood-path.md`
  - implementation-stage exact revisions, commands, results, deviations, and
  residual risks.

Changed after the acceptance run found an actual Hub-owned defect:

- `src/main.rs`
- `tests/hub_daemon_lifecycle_test.rs`
  - a failed `up` that started a new daemon must shut down that owned daemon
    and remove its socket before returning the launch error.

Expected unchanged:

- `src/daemon.rs`
- `src/daemon_transport.rs`
- `src/entrypoint_supervisor.rs`
- `src/packages.rs`

## Risks

- A current package may pass manifest/source checks while exact Hub supervision
  rejects it. Mitigation: launch all runnable entrypoints through the current
  binary and public daemon path.
- Web can pass stale-versus-stale protocol drift checks. Mitigation: install the
  published artifact externally and compare metadata plus actual bytes to exact
  Hub main before any Web leg.
- A fixture generated with current serializers would fake the upgrade.
  Mitigation: build and run the exact pre-cutover Hub producer and leave its
  data directory untouched before current main opens it.
- Temporary worktree paths can leak into committed evidence. Mitigation:
  machine-readable evidence records repository name and SHA; reports use
  path-neutral placeholders.
- Multiple long-lived processes can leave sockets, workers, browsers, or app
  entrypoints behind after a failure. Mitigation: bounded condition-based waits,
  owned-process tracking, graceful `down`, and an explicit final teardown audit.
- A wrong or omitted data directory can mutate the operator's active Hub.
  Mitigation: reject every Hub invocation without explicit fixture
  `--data-dir`, require each resolved socket to be contained by its fixture,
  snapshot the product default state root before/after, and permit teardown only
  for recorded owned PIDs/sockets.
- A fixed sleep can make a false readiness claim. Mitigation: wait on protocol
  handshake, structured app state/URL/health, session lifecycle, terminal
  markers, browser events, or process exit with bounded deadlines.
- An explicit Web port can silently fall back to dynamic binding or accept an
  invalid value while the default-path smoke stays green. Mitigation: prove the
  exact `BOTSTER_WEB_PACKAGE_SERVER_PORT` value in structured `local_url` plus
  health/UI, and prove invalid input emits `invalid_package_server_port`.
- The retained Ruby helper is a new repository execution prerequisite.
  Mitigation: keep it stdlib-only, require Ruby 2.7+, preflight before builds
  with exact remediation, document it beside the command, and run `ruby -c`.
- A literal vocabulary scan can miss stale current operator guidance that uses
  different old contract names. Mitigation: correct the known
  `BOTSTER_WEB_PORT=5173`/no-supervision README section and keep the separate
  Rules evidence matrix for old environments, ports, and runtime claims.
- Broad text matching can either miss renamed identifiers or block historical
  plan artifacts intentionally retained. Mitigation: codify the human-approved
  include/exclude set and print every scanned repository/path class.
- TUI and Web harnesses are expensive. Mitigation: keep focused repository tests
  separate, but require the full live paths once for final acceptance.

## Acceptance Checks And Tests

Plan/implementation static gates:

- `git diff --check`
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `./test.sh`
- `sh -n script/test-production-package-runtime`
- `ruby -c script/production-package-runtime-evidence`
- Ruby 2.7+ preflight negative and positive checks, including the exact missing
  or too-old interpreter remediation
- the seven-repository product-surface vocabulary audit, with the approved
  history exclusions printed in evidence
- the separate Rules evidence matrix for aliases/modes, targeted old
  environment/query tokens, two-runtime dynamic ports, the `41739` retired-port
  control, `BOTSTER_WEB_PACKAGE_SERVER_PORT` success/failure, sibling paths,
  dependency overrides, readiness predicates, and automatic refresh
- a PII/path-neutrality scan over the full committed diff and evidence bundle

Focused Hub regression gates if the production script or docs are the only
changed surfaces:

- `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_up_starts_reuses_and_down_stops_runtime`
- `./test.sh --test hub_daemon_lifecycle_test cli_smoke_proves_local_runtime_daemon_package_app_session_and_webrtc`
- `./test.sh --test hub_daemon_lifecycle_test cli_daemon_restart_recovers_worker_backed_session_through_transport`
- affected package-refresh and unchanged-entrypoint tests selected from
  `hub_daemon_lifecycle_test`

Cross-repository final proof:

1. Fetch each repository and integrate the two existing implementation commits
   onto exact Hub `origin/main`. Confirm PR #164 points at the integrated head,
   then inspect the complete diff against current main.
2. Resolve exact merged `origin/main` for all seven repositories, verify clean
   detached worktrees, and record SHA plus supported build command.
3. Verify all seven registered dependencies are closed, then run the clean
   external artifact-availability gate. Record package coordinate, upstream
   source hash/revision, installed metadata hash/revision, actual installed
   bytes, and Web vendored hash. Stop before builds if they differ.
4. Snapshot the resolved operator default Botster state root without mutating
   it. Record a content manifest and metadata baseline.
5. Run each repository's supported locked build/test prerequisite.
6. Run the Hub production acceptance harness in `fresh` mode.
7. Run Web's live packaged-protocol/browser harness with the exact current Hub
   and worker binaries and current Workspaces package.
8. Run the TUI package entrypoint in supported headless live-runtime mode.
9. Run the Hub production acceptance harness in `upgrade` mode using the
   pre-cutover-produced untouched data directory, then run Web's supported
   live packaged-protocol/browser harness against that same upgraded data
   directory to prove reload/reconnect through the durable runtime. Build the
   historical producer with both an isolated Cargo home and target directory
   so its immutable dependency checkout cannot read or mutate operator caches.
10. For both runtime legs, require:
   - every Hub command has an explicit fixture `--data-dir`;
   - the resolved socket is contained by that fixture and differs from any
     pre-existing live/default socket;
   - `up` success and automatic package refresh;
   - protocol-level status and structured `doctor`;
   - four expected enabled first-party packages;
   - dynamic Web `local_url`, exact `/health`, HTML/UI, and fresh bootstrap;
   - TUI launch through daemon-resolved app contract;
   - session spawn/list/attach/input/resize/readback/lifecycle;
   - Web reload/reconnect and Hub restart/session adoption;
   - `smoke` success;
   - `down` success, no responding owned Hub socket, and no owned child left
     alive; teardown must refuse unrecorded sockets/PIDs.
11. In the fresh leg, additionally require two concurrent default-configured
    Web runtimes to publish distinct nonzero structured ports while `41739` is
    occupied; require one explicitly configured
    `BOTSTER_WEB_PACKAGE_SERVER_PORT` to publish and serve that exact port; and
    require invalid override input to fail with
    `invalid_package_server_port`, never dynamic fallback.
12. Re-snapshot the operator default Botster state root and require identical
    metadata/content. Record both fixture data directories and socket paths so
    Verify can audit isolation without rerunning.
13. Run the vocabulary audit, Rules evidence matrix, and committed-artifact PII
    scan with valid exit-status handling and known-positive controls.
14. Attach the exact command log, revision manifest, and summarized report to the
   Project Pipelines run. Code existence, source regexes, or an unrun command do
   not satisfy this gate.

## Pipeline Gates And Artifacts

- Plan: attach this file and the complete `botster_stack_plan_gate` evidence.
- Plan Review must enforce the human routing decision: preserve the prior-run
  implementation, integrate current main, and require fresh evidence in
  Implement. The seven registered dependencies are closed.
- Implement: commit the bounded Hub changes, link the PR, attach the
  implementation report, revision manifest, and command/evidence bundle.
- Review: load [[botster-runtime-reviewer-playbook]] for daemon
  startup/reuse/restart/adoption/shutdown and terminal lifecycle proof, plus
  [[botster-package-reviewer-playbook]] for package
  install/enable/refresh/supervision/readiness and plugin-worker proof. Reject
  compatibility scaffolding, external-repo fixes, hard-coded coordinates,
  timing-only readiness, incomplete cleanup, or code-only proof.
- Verify: load [[botster-runtime-verifier-playbook]] and
  [[botster-package-verifier-playbook]], rerun or independently inspect both
  fresh and upgrade live evidence, recheck every finding against the live
  worktree, and verify the product-surface scan across all seven exact
  revisions.

## Convention Conflicts

No architecture convention conflicts. The plan keeps host-profile integration
proof in Hub, keeps product workflow policy in packages, uses
repository-supported locked builds, uses universal Git/filesystem/script
primitives, performs a cold-turkey audit, avoids local dependency overrides,
and requires exact production entrypoint proof. The retained Ruby helper is a
deliberate new repo execution prerequisite, not a new framework or gem: keeping
the stdlib-only existing implementation follows the binding reuse decision,
while preflight, documentation, version pinning, and `ruby -c` make the
operational cost explicit.

## Vault Gaps Worth Capturing

- Capture a note if the deterministic pre-cutover worktree-to-current-package
  refresh pattern becomes the reusable standard for cross-repository Botster
  cutover acceptance.
- Capture a note if execution establishes a durable distinction between
  supported product documentation and retained implementation-history
  plans/reports beyond this ticket's human decision.
- No note is needed at Plan time for exact-Hub launch, dynamic readiness,
  package injection consumption, or cold-turkey removal; existing notes already
  cover those constraints.
