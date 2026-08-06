# Botster Hub Client Protocol

The authoritative reusable client-to-hub daemon protocol lives in:

- `crates/botster-hub-client/src/lib.rs`
- `src/daemon_transport.rs`

The renderer-neutral UI contract used by those protocol DTOs lives in:

- `crates/botster-ui-contract/src/lib.rs`
- `packages/ui-contract`

Rust clients consume `botster-ui-contract`; TypeScript clients consume the
prepared `@trybotster/ui-contract@0.3.1`. The generated declarations, schema, and
conformance fixtures are one contract surface and must not be copied into a
client repository.

Implementation baseline before this split: `9b39f1607144319138151cdf776e8909f35a63d4`. The pipeline implementation commit should be treated as the final protocol revision once merged.

External same-device clients should depend on the `botster-hub-client` crate and use `DaemonEndpoint`, `DaemonConnection`, `request`, or `stream_attach` to talk to a running `botster-hub` daemon socket. The crate owns the client-facing handshake, request, response, event, and JSON frame helpers.

Browser clients should import the checked generated TypeScript protocol artifact
instead of maintaining handwritten DTO mirrors:

- `crates/botster-hub-client/generated/daemon-protocol.ts`

That artifact is generated from the Rust serde DTO surface in
`crates/botster-hub-client` and is checked by the crate test suite. Rust serde
remains canonical for the daemon wire shape; TypeScript consumers should treat
the generated file as a downstream contract artifact, not as an independent
source of truth. The artifact intentionally includes the hello/compatibility
handshake DTOs as well as request, response, event, package, plugin lifecycle,
plugin surface/action, diagnostics, coordination, and session DTO families.
Downstream `botster-web` drift checks should point
`BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` at this exact file; a skipped check because
the hub checkout was not found is not protocol evidence.

Node-based first-party clients can consume the same checked artifact without a
sibling hub checkout through the public package:

```sh
npm install --save-dev @trybotster/ui-contract@0.3.1 @trybotster/hub-test-support@0.1.20
```

```js
import {
  materializeApplicationPrimitivesFixture,
  materializePluginContractMatrixFixture,
  materializeSessionPluginBindingScenario,
  materializeSessionPluginRowScenario,
  metadata,
  readDaemonProtocolTypescript,
  readFirstPartyClientSupportMatrix,
  readLateAttachHistoryConformanceFixture,
  readSessionLifecycleSubscriptionConformanceFixture,
  readSessionPluginBindingConformanceFixture,
} from "@trybotster/hub-test-support";

const protocolSource = readDaemonProtocolTypescript();
const fixturePath = materializePluginContractMatrixFixture(tempDirectory);
const applicationPrimitivesPath = materializeApplicationPrimitivesFixture(tempDirectory);
const applicationSurfaceId = metadata.application_primitives.surface_id;
const supportMatrix = readFirstPartyClientSupportMatrix();
const lateAttachFixture = readLateAttachHistoryConformanceFixture();
const sessionLifecycleFixture = readSessionLifecycleSubscriptionConformanceFixture();
const sessionBindingStages = materializeSessionPluginBindingScenario(
  readSessionPluginBindingConformanceFixture(),
);
const sessionRowStages = materializeSessionPluginRowScenario(
  readSessionPluginBindingConformanceFixture(),
);

console.log(
  metadata.protocol,
  metadata.conformance_fixture_revision,
  fixturePath,
  applicationPrimitivesPath,
  applicationSurfaceId,
  supportMatrix.required_features,
  lateAttachFixture.history_then_live,
  sessionLifecycleFixture.normalized_frames,
  sessionBindingStages,
  sessionRowStages,
);
```

The package includes checksum metadata so browser-client tests can fail clearly
when checked assets are stale. The metadata's protocol version and conformance
fixture revision are emitted by the Rust `botster-hub-test-support` asset
generator instead of being maintained independently in JavaScript.

Version 0.1.18 is published to the public npm registry. npm-based client repos
such as botster-web use the exact dependency spec
`"@trybotster/hub-test-support": "0.1.18"` in `devDependencies` and let npm write
the corresponding package-lock entry from the public npm registry. The package
is public, so registry install does not require a scoped `.npmrc` entry or CI
auth token. After updating the lockfile, run the client smoke that imports the
package, reads the daemon protocol artifact, calls `verifyPackageAssets()`, and
materializes the application-primitives fixture.

The application-primitives fixture is an explicit consumer alias over the
hub/core-validated plugin contract matrix package. Botster web and TUI should
use `materializeApplicationPrimitivesFixture(destination)`,
`metadata.application_primitives.surface_id` (`contract.app`), and
`metadata.application_primitives.renderer_entrypoint` (`ui_tree_snapshot.body`).
The current primitive inventory is `button`, `empty_state`, `form`, `metric`,
`metric_grid`, `panel`, `section`, `status_badge`, `table`, `text_input`, and
`toolbar`. The current core contract fixture does not include `list` or an
`action_bar` alias; downstream renderers should not hand-author those shapes.

## Compatibility Handshake

Clients should check hub compatibility before depending on request-specific
behavior. `DaemonConnection::connect`, `request`, and `stream_attach` perform
the current first-party compatibility check during the socket hello handshake.
The running hub also returns the same descriptor on `DaemonRequest::Status` so
operator UIs can show protocol diagnostics without opening a special endpoint.

The current descriptor includes:

- protocol name and version;
- supported features: sessions, session and plugin entity subscriptions, terminal streaming, resize, terminal readback,
  plugin surface render, plugin surface action dispatch, package navigation
  discovery, and hub-owned spawn targets;
- conformance fixture revision.

The hub-owned first-party support matrix lives in
`botster_hub_test_support::first_party_client_support_matrix`. It is a
serde-serializable test/docs contract that expands the compatibility descriptor
into the exact first-party client surface covered today: diagnostic kinds,
session actions, held-open terminal streaming, resize, Project Pipelines
surface/action dispatch, and known limitations. It is not a daemon runtime
endpoint.

Downstream clients with the same requirements as the current crate can rely on
the default connection helper. The checked example for this path lives on
`botster_hub_client::DaemonConnection`.

```rust
let endpoint = botster_hub_client::DaemonEndpoint::new(socket_path);
let mut connection = botster_hub_client::DaemonConnection::connect(&endpoint)
    .map_err(|error| error.to_string())?;
```

Clients that need to declare stricter requirements should use the explicit
handshake helper and display the returned diagnostic as a connection/status
error. The checked examples for this path live on
`botster_hub_client::DaemonCompatibilityRequirement::current` and
`botster_hub_client::connect_and_hello_with_requirement`.

```rust
let mut requirement = botster_hub_client::DaemonCompatibilityRequirement::current();
requirement.client_name = "botster-tui".to_string();
requirement
    .required_features
    .push("future_feature".to_string());

let stream = botster_hub_client::connect_and_hello_with_requirement(
    &endpoint,
    &requirement,
)
.map_err(|error| error.to_string())?;
```

`botster-tui` should run this check as part of its daemon connect/reconnect
path and render `DaemonTransportError::Compatibility` as the status panel
connection error instead of continuing into session or terminal operations.
`botster-web` should perform the same check in its local hub bridge/status path
before relying on sessions, terminal streaming, resize, or plugin surface/action
dispatch, and show the diagnostic in the hub connection state.

## Session entity subscriptions

`subscribe_session_entities` opens a dedicated held-open connection for the
built-in `session` family. The first pushed frame is an authoritative,
stable-id-ordered `entity_snapshot`; later `entity_upsert`, sparse
`entity_patch`, and `entity_remove` frames carry strictly increasing sequence
values from CoreDaemon's lifecycle cursor. Every frame includes the caller's
connection-scoped `subscription_id` and `entity_type: "session"`.

Package-owned families use the same held-open request and `DaemonEntityFrame`
wire envelope with generic JSON records. They require the advertised
`plugin_entity_subscriptions` feature. Hub admits only an exact family declared
by its currently loaded owning package. The family owner is Hub's canonical
single-segment package token: ordinary single-segment manifest names remain
unchanged, while dotted or `bns1_`-prefixed names use `bns1_` plus lowercase
hex of their exact UTF-8 bytes. Hub invokes that package's provider through
the plugin worker, validates a whole-family snapshot and non-empty string `id`
for every row, and sends that authoritative snapshot first. A reconnect is a
new provider query; cached rows from the prior connection are not a baseline.
The typed `DaemonSessionEntity` remains the convenience projection for the
built-in `session` family, including omission of absent lifecycle/exit fields.

The sanitized row contains `session_uuid`, registry/lifecycle state, terminal
dimensions, update time, and optional exit/failure detail. It never contains
worker control sockets, environment variables, host filesystem paths, or raw
Core implementation types. Subscribing does not hydrate status, packages,
worktrees, spawn targets, plugin entities, or UI trees.

Delivery is bounded independently per subscriber. Queue overflow or a stale,
foreign, or future Core cursor requires a fresh `entity_snapshot` with
`resync_reason` before later deltas; if that snapshot cannot be delivered, the
subscription closes instead of silently presenting stale state. Socket EOF,
write failure, explicit `unsubscribe`, and daemon shutdown release the
connection-owned subscription. Reconnect with a new subscription id and treat
its first snapshot as the sole baseline; frames from a prior connection are not
replayed. `reconnect_registrations` counts a new entity or attach registration
that follows a released registration generation; it does not retain historical
subscription ids.

`lifecycle_resync_reads` is an exceptional-path scaffold counter. Its producer
is wired for Core `resync_required` and future unknown lifecycle variants, but
the pinned Core runtime has no deterministic public fixture that forces either
condition. Normal-path conformance therefore expects it to remain zero.

The daemon admits 64 live local socket connections. Excess clients receive a
typed `daemon_connection_admission` backpressure hello and are closed without
entering the runtime request path. Admitted connections are async tasks on the
daemon's fixed transport runtime, not detached OS threads. One connection
owner releases every attach and entity registration exactly once on EOF,
malformed or incomplete frames, write failure, cancellation, normal close, or
shutdown.

Healthy frame-complete streams have no lease and may remain idle indefinitely.
The handshake, a frame that has started but not completed, and each socket
write have bounded deadlines. Entity delivery waits on socket input, its
bounded frame queue, and shutdown; it has no per-client timer or 20 ms poll.

`DaemonStatus.lifecycle_counters` is optional for old-daemon compatibility and
always populated by the current daemon. It exposes counts only: connection and
subscription current/high-water values, reconnect registrations, cleanup
outcomes/reasons, reconciliation wake/change/baseline/resync/drain work,
entity delivery attempts/outcomes, and stalled writes. It never contains
connection, session, subscription, path, command, or payload identifiers.
During steady state the daemon reads one shared Core lifecycle journal cursor;
the filesystem-backed baseline counter advances only for initial seeding or an
explicit journal resync.

`DaemonStatus.software` is the authoritative identity of the running binary:
product id/name, `CARGO_PKG_VERSION`, and an optional sanitized build revision
embedded at compile time. `DaemonStatus.installation` is a separate fact from
host identity, protocol compatibility, durable schema, and package rows. It is
resolved only from `$HOME/.botster/installations/botster-hub.json`; unsafe or
mismatched receipts diagnose but never override the embedded binary identity.
Receipt schema 1 is strict and contains exactly `schema_version`, `product_id`,
`binary_version`, `installation_mode`, `release_channel`, `provider`, and
`source_url`. Managed sources must use HTTPS; plaintext HTTP is accepted only
for explicit loopback test fixtures. Additive receipt fields require a schema
version bump. A Hub that does not recognize that schema reports
`unsupported_receipt_schema` and falls back to development or unmanaged mode
according to its build profile, so an installer must replace the Hub binary
before atomically writing a receipt that requires the newer schema.

`RemoveSession` forgets only an already-terminal session and produces the
ordered remove delta. `ListSessions` remains available for operator queries,
but normal client reconciliation must not poll it or maintain a list-refresh
fallback beside the entity stream.

The prepared current contract ships from
`@trybotster/hub-test-support@0.1.24` as
`session-lifecycle-subscription-conformance-fixture.json` and through
`readSessionLifecycleSubscriptionConformanceFixture()`. The fixture serializes
the public `DaemonEntityFrame` DTOs and normalizes only timestamps and sequence
values. Rust clients can run
`botster_hub_test_support::run_session_lifecycle_subscription_conformance`
against an `IsolatedHub`; it proves snapshot, ordered upsert/patch/remove,
independent concurrent delivery, socket-loss cleanup, and a fresh reconnect
snapshot through the real HubDaemon/CoreDaemon/session-worker topology. Web and
TUI consumers must use these same semantics and must not add polling or
list-refresh fallbacks.

The built-in renderer-neutral binding family is `/session`; Hub rejects every
other absolute family after generic `UiNode` validation. Its row id and
`session_uuid` are the canonical session UUID. Every present
`DaemonSessionEntity` has required `lifecycle_class` with this total mapping:

- `registry_state == "stale"` => `indeterminate`, regardless of lifecycle;
- otherwise `starting | running | stopping` => `current`;
- otherwise `exited | failed` => `ended`;
- otherwise an omitted lifecycle => `indeterminate`.

Absence from an authoritative snapshot is the only unknown/unavailable state.
Shutdown does not remove the row. Only explicit retention removal emits
`entity_remove`. `session-plugin-binding-conformance-fixture.json` combines the
real fixture surface shape with public `DaemonEntityFrame` values and exercises
current, ended, indeterminate, missing, patch, remove, and reconnect semantics.
Rust uses `materialize_session_plugin_bindings`; Node uses
`materializeSessionPluginBindingScenario`. The additive
`materialize_session_plugin_rows` and `materializeSessionPluginRowScenario`
helpers resolve the current-row Button's authored id and action payload after
filtering. The Spawn Button's authored required label binds
`@/lifecycle_class`; Rust and Node materializers resolve it to a literal before
strict realized validation. Only the direct BindList item-template root id accepts `@/field`;
roots outside BindList, item-template descendants, static children, empty
templates, and action request/result `node_id` remain literal. Descendant
identity for multi-control rows is separately tracked in
`ticket_1785443253_376782`.
Blank, non-string, or duplicate realized ids fail. These are Hub-owned
reference materializers, not proof that the shipped Web or TUI renderer already
resolves bindings.

## Spawn Targets

Spawn targets are hub-owned runtime policy state. They live in `hub-state.json`
under the hub profile, not in `botster-core`, package manifests, or plugin
state. Clients and plugins reference the stable `target_id`; the hub admits and
resolves the local directory root.

The daemon protocol exposes `ListSpawnTargets`, `ShowSpawnTarget`,
`CreateSpawnTarget`, `UpdateSpawnTarget`, `DeleteSpawnTarget`, and
`ValidateSpawnTarget`.

`DaemonSpawnTarget.root` is a runtime local path returned to trusted same-device
clients. Committed docs and fixtures must use placeholders or temporary paths,
not user-specific absolute paths. `kind = "directory"` is the legacy/default
generic admission and does not infer Git capability. `kind = "git"` is an
explicit managed-Git declaration and carries optional-on-the-wire `base_ref`.
Admission validates the repository and resolves the ref to a commit. Create, or
an explicit directory-to-Git update, may default the ref once from symbolic
`HEAD`; managed spawning thereafter uses the stored ref and does not reread
`HEAD` or guess a conventional branch.

## Worktrees

Worktrees are hub-owned working-directory records scoped to an admitted spawn
target. They live in `hub-state.json` and reference `target_id`; they are not a
second target model and do not carry workflow-specific ticket, run, gate, or PR
fields. Plugins should persist those associations in plugin state and reference
the returned `worktree_id`.

The daemon protocol exposes `ListWorktrees`, `ShowWorktree`, `CreateWorktree`,
and `DeleteWorktree`. Create admits an existing directory under the selected
spawn target root. The hub canonicalizes the target root and requested path and
rejects traversal or symlink escapes before persisting the row. Delete removes
registered hub records only; it does not remove filesystem contents.
Hub-managed Git rows reject this record-only deletion path.

`DaemonWorktree.management` distinguishes ordinary `registered` rows from
`hub_managed_git` rows. Legacy rows default to `registered`. Registered rows
remain contained beneath `DaemonSpawnTarget.root`. Managed Git rows use a
deterministic Hub-owned path beneath the daemon data directory and reconcile
their actual Git common-directory identity and branch, so a valid managed row
reports `present` even though it is outside the target root.

`DaemonWorktree.status` values are `present`, `missing`, and `stale`.
Registered rows are reconciled when returned. Managed rows project the last
status persisted by startup adoption or the bounded managed-Git lane; list/show
do not execute Git or refresh externally removed paths, so that status can
remain stale until restart or another managed operation. Missing paths remain
listable after daemon reload so clients can explain stale local state instead of
treating startup as a fatal error. `DaemonWorktree.git` is optional
opportunistic metadata; plain directories without `.git` are valid worktrees.

## Connection Diagnostics

The daemon protocol exposes policy-free diagnostics through stable
`DaemonDiagnostic` values. Clients should branch on `kind`, `operation`, and
`feature`, and treat `message` as optional operator detail rather than a parsing
contract.

Diagnostics are additive fields on `DaemonHelloAck`, `DaemonStatus`,
`DaemonResponse`, and `DaemonOperatorError`. Older responses that do not include
diagnostics still deserialize with empty diagnostic lists.

Current diagnostic kinds are:

- `connected` for successful hello/status/shutdown lifecycle checks;
- `compatibility_mismatch` for protocol, protocol-version, or conformance
  descriptor mismatch;
- `unsupported_feature` for missing handshake features or unsupported daemon
  operations;
- `terminal_stream_unavailable` when a terminal stream request has a distinct
  runtime signal such as missing session on attach/drain;
- `action_failure` when a plugin surface action returns a rejected or error
  result;
- `daemon_startup_failure` for startup failures reported by client/test-support
  helpers before a daemon socket protocol response can exist;
- `backpressure` for bounded daemon-client egress pressure summaries such as
  terminal or control write failures;
- `disconnected` for client-side transport disconnect classification.

Backpressure diagnostics report only lane and counter summaries. They must not
include terminal payloads, session ids, plugin payloads, secrets, or local paths.

Downstream clients should prefer the structured fields over private string
parsing:

```rust
let response = connection.request(&botster_hub_client::DaemonRequest::Status)?;
if response.diagnostics.iter().any(|diagnostic| {
    diagnostic.kind == botster_hub_client::DaemonDiagnosticKind::Connected
}) {
    // Render connected state.
}
```

Compatibility errors also carry diagnostics:

```rust
match botster_hub_client::connect_and_hello_with_requirement(&endpoint, &requirement) {
    Err(botster_hub_client::DaemonTransportError::Compatibility(error)) => {
        for diagnostic in error.diagnostics {
            // Render compatibility_mismatch or unsupported_feature.
        }
    }
    other => other.map(drop)?,
}
```

Diagnostic messages intentionally avoid local data directories, socket paths,
raw worktree paths, and mutable Botster identity. First-party clients may add UI
severity or remediation copy, but that policy belongs in the client renderer,
not in the daemon protocol.

Stateful CLI commands resolve one Hub-owned runtime root before opening the
daemon transport: explicit `--data-dir`, then `BOTSTER_HUB_DATA_DIR`, then
`$HOME/.botster/hub`. The result is independent of the current working
directory. `XDG_DATA_HOME` and the sibling device/configuration directories
under `$HOME/.botster` are not runtime-root inputs.

## Package Registry Sources And Install Preview

The hub exposes a local/static marketplace registry preview path through the
daemon protocol. This is a hub-owned catalog contract for first-party fixtures
and local package catalogs, not a hosted marketplace or remote installer.

Current daemon requests:

- `ListAvailablePackages { registry_path }` reads a local registry directory or
  `botster-registry.json` file and returns sanitized `DaemonAvailablePackage`
  rows.
- `InspectAvailablePackage { registry_path, entry_id }` returns one available
  row for inspect-before-install UI.
- `PreviewPackageInstall { registry_path, entry_id }` returns a
  `DaemonPackageInstallPlan` without mutating `HubState.package_registry`.
- `InstallPackageRegistryEntry { registry_path, entry_id }` explicitly installs
  one registry entry, persists source metadata and pins, and returns refreshed
  installed package rows.
- `CheckPackageUpdate { package_name }` returns `DaemonPackageUpdateStatus`
  for the installed package without mutating state.
- `CheckHubUpdate` is a distinct Hub-binary maintenance request. It returns
  `DaemonHubUpdate` with typed `current`, `available`, or `unavailable` state,
  optional release/build information, and reason/action metadata. It never
  delegates to or reuses package-update state. `available_version` reports what
  the configured source advertises and can be older than `current_version` when
  state is `current` with reason `source_behind`; clients must branch on
  `state`, never on the field's presence.
- `PreviewPackageUpdate { package_name, pin }` returns
  `DaemonPackageUpdateStatus` plus a `DaemonPackageInstallPlan`-shaped preview
  that reuses `DaemonPackagePin` metadata and reports that no entrypoints start.
- `ApplyPackageUpdate { package_name, pin }` records pinned source metadata and
  update policy on the installed package, preserves configuration values, and
  returns refreshed package rows.

Registry responses intentionally do not expose the local registry path or local
package root. Local entries use path-neutral source labels such as
`local:<entry_id>`. Git-shaped entries carry repo plus branch/tag/rev pin
metadata for preview and persistence only; the daemon does not clone, fetch, or
checkout remote content in this path.

Install preview reports compatibility, requested capabilities, current
installed-vs-available state, and effects such as adding a package record,
recording source metadata, requiring a separate explicit enable, and not
starting entrypoints. Explicit registry install leaves packages in the installed
state. Clients must call existing enable/start requests separately if the
operator wants activation or process supervision.

Package lifecycle UI must render hub-owned action descriptors instead of
inferring policy from package state. Installed `DaemonPackage` rows, available
`DaemonAvailablePackage` rows, `DaemonPackageUpdateStatus`, and runnable
entrypoints can include an additive `actions` list. Each action carries a stable
`action_id`, a status of `available`, `blocked`, or `unavailable`, optional
diagnostics and required references, and an optional request mapping for actions
the daemon can invoke. Unsupported reload and hub-restart style actions are
reported as unavailable diagnostics; they are not hidden client policy and do
not imply an implementation exists.

`PluginLifecycleStatus` projects two sanitized aggregate observations. The
optional `plugin_worker_counters` object carries Core-authored queue capacity,
executor concurrency, live executor/worker, queued-job, and in-flight-job
counters. The optional `plugin_resource_counters` object carries Hub-owned
resource observations; currently it contains only `active_timer_resources`.
Neither object exposes plugin identities, payloads, handler names, or resource
IDs. Clients must treat absence as an older-daemon compatibility case rather
than synthesizing a zero value.

CLI operators can inspect the same daemon path:

```sh
botster-hub packages available --data-dir <path> --registry <registry-dir-or-file>
botster-hub packages inspect --data-dir <path> --registry <registry-dir-or-file> <entry-id>
botster-hub packages preview-install --data-dir <path> --registry <registry-dir-or-file> <entry-id>
botster-hub packages install --data-dir <path> --registry <registry-dir-or-file> <entry-id>
botster-hub packages check-update --data-dir <path> <package>
botster-hub packages preview-update --data-dir <path> <package> --revision <revision> [--branch <branch>] [--tag <tag>] [--rev <rev>] [--checksum <checksum>] [--policy manual|track_source]
botster-hub packages apply-update --data-dir <path> <package> --revision <revision> [--branch <branch>] [--tag <tag>] [--rev <rev>] [--checksum <checksum>] [--policy manual|track_source]
```

Update lifecycle requests are production-shaped but deliberately do not fetch,
clone, reload, or restart the hub. Unsupported source/update cases are reported
as `DaemonPackageUpdateStatus.diagnostics` rows such as `update_unavailable`,
`pin_required`, and `reload_unavailable`. If an enabled package or running
entrypoint would need operator action after pin metadata changes, the daemon
sets `reload_required` or `restart_required`; it does not invent a reload or hub
restart path.

## Package Runnable Entrypoints

`DaemonPackage` rows include `runnable_entrypoints` for hub-owned local/dev
process contracts declared by installed packages. Entrypoints marked
`may_supervise` can be started, stopped, restarted, and inspected with
`StartPackageEntrypoint`, `StopPackageEntrypoint`,
`RestartPackageEntrypoint`, and `PackageEntrypointStatus` daemon requests.
Runtime process state is owned by the running hub daemon and is not persisted
into package registry state.

Each entrypoint exposes sanitized manifest declarations: `id`, `kind`,
`command`, `args`, `working_directory`, declarative `environment`
requirements, `launch_mode`, capability needs, `may_supervise`, and process
diagnostics. Runtime process fields are additive: `pid`, `started_at`,
`exited_at`, and `exit_status` may be omitted when no supervised process state
exists. The daemon response must not expose the local package root, provenance
path, socket path, or host-resolved environment values. Environment defaults
are manifest-provided literals, not snapshots from the operator's machine.
Entrypoint `actions` are derived after the daemon applies current supervisor
snapshots, so start/stop/restart availability reflects live process state.

`ListApps` exposes the installed app registry as first-class daemon DTOs. App
rows are projected by the hub from installed package `runnable_entrypoints` plus
the live `EntrypointSupervisor` snapshots, then returned as `DaemonResponse.apps`
with response kind `apps`. Clients should consume those rows instead of
inferring app state from package rows or parsing diagnostics.

Each app row includes `package_name`, `app_id`, `entrypoint_id`, core
`kind` (`web_app` or `terminal_app`), core `launch_mode` (`background` or
`foreground_stdio`), `lifecycle_state`, `diagnostics`, `actions`,
`blocked_reasons`, and a structured `launch_target`. Web apps may include
`launch_target.local_url` only when the entrypoint readiness declares the
`local_url` result field and the supervisor has a core
`RunnableEntrypointLaunchResult.local_url` for that entrypoint.
`launch_target.kind` mirrors the core app kind (`web_app` or `terminal_app`) so
clients do not need a second kind vocabulary. Supervised runtimes can emit the
structured launch result through the hub-provided
`BOTSTER_ENTRYPOINT_LAUNCH_RESULT` file path. The hub must not derive
`local_url` from stdout, stderr, diagnostics, command arguments, environment
defaults, local package names, or known ports. Terminal apps use a terminal app
launch target and do not expose fake background URLs.

`ResolveAppLaunch { package_name, entrypoint_id }` is the request-scoped path
for opening installed foreground terminal apps. The daemon validates that the
package is enabled, the entrypoint is a `terminal_app`, and the launch mode is
`foreground_stdio`, then returns `DaemonResponse.resolved_app_launch` with the
hub-resolved command, arguments, working directory, and allowlisted environment
needed by a same-device client that owns the foreground TTY. Clients should
spawn that contract with inherited stdio and should not reconstruct package
roots, socket paths, data-dir values, or manifest environment policy from
package rows. Normal `ListApps` output intentionally omits this launch contract
to avoid path and environment leakage.

Runnable launch contracts use each manifest's declared injection targets. For
`hub_connection`, the daemon serializes Core's typed descriptor with a
`unix_socket` transport and an absolute socket path. For `data_dir`, it injects
the absolute package runtime data directory. Environment names and argument
placeholders are package policy rather than Hub constants, and clients must not
reconstruct either value from package rows or reinterpret the paths relative to
the package working directory.

Supervised entrypoints are local development processes, not a production
installer or sandbox. The daemon stops them on explicit stop/restart, package
disable/remove, `DaemonShutdown`, and daemon SIGINT/SIGTERM cleanup.

Web app `local_url` values are child-authored supervised package app outputs.
Hub uses the exact returned URL for health/UI verification; it does not derive
the URL from a configured port or a second bridge owner.

For the installed `botster-web` `web-client` entrypoint, `StartPackageEntrypoint`
returns package state only after structured readiness. A page-load
`IssueLocalWebrtcBootstrap` request mints the short-lived local browser grant
after the app URL is known and validates its origin against `local_url`. The
bootstrap contains the grant id/secret, expected same-device origin, expiry,
signaling transport (`daemon_request`), data plane
(`webrtc_data_channel`), and the required DataChannel reliability contract:
ordered `true`, no `max_retransmits`, no `max_packet_lifetime_ms`, and no hub
application reorder buffer.

After the entrypoint is already running, a page-serving runtime can request a
fresh one-shot browser grant with
`IssueLocalWebrtcBootstrap { package_name, entrypoint_id, origin }`. Initial
support is deliberately limited to `botster-web` / `web-client`. The daemon only
mints a grant when the package is enabled, the entrypoint is running, and the
supplied origin matches the origin of the supervisor's structured
`launch_target.local_url`. This request is the reload-safe page-load contract;
it does not make existing grants reusable.

The local signaling surface is the daemon request
`LocalWebrtcSignal { grant_id, grant_secret, origin, offer }`. The hub validates
that the grant exists, has not expired, has not already been redeemed, has the
expected secret, and matches the expected origin before creating a WebRTC answer.
The origin check is defense in depth for same-device launches; the short-lived
grant secret is the admission boundary.
Accepted client-to-hub DataChannel messages remain JSON serialized
`botster_core::AesGcmEnvelope` values whose plaintext is the existing daemon
`DaemonRequest`. Hub-to-client messages use only
`DaemonLocalWebrtcDeliveryChunk`; the former response-only chunk and direct
`AesGcmEnvelope` response
frame is intentionally deleted. This is a coordinated breaking upgrade, not a
negotiated feature or compatibility path.

The hub serializes and AES-GCM encrypts each `DaemonResponse` or
`DaemonEntityFrame` once, then slices the serialized encrypted envelope into
ordered chunks. Every chunk carries protocol `version`, `delivery_kind`, a
hub-minted `message_id`, zero-based `chunk_index`,
`chunk_count`, declared `total_bytes`, and a `payload` slice. Small responses
use the identical contract with one chunk. Serialized frames are always below
64 KiB and the declared encrypted delivery is capped at 16 MiB. An over-budget
response is replaced before any of its bytes are sent by one bounded encrypted
operator-error response.

The ordered channel and one-response-at-a-time handler preserve the existing
request FIFO; `message_id` correlates all chunks of the current logical delivery.
Response and entity chunks never interleave within a logical message. Entity
subscribe is registered through the daemon owner's bounded session registry;
the correlated `entity_subscribed` response completes before its queued
authoritative snapshot is eligible for delivery. The
sender applies fixed DataChannel watermarks (128 KiB high, 64 KiB low), queues
at most 16 inbound request payloads consumed while a response drains, and
represents each excess request with one ordered encrypted operator-error
response using constant-memory counted queue state. Only contiguous overflow
runs coalesce, preserving their position relative to later accepted requests
rather than dropping the peer or losing positional correlation. Send errors,
disconnects, and a missing low-water event fail closed. A paused sender has one
non-resetting five-second deadline, after which it closes the channel, cleans
the peer and its subscription/request state, and emits no completion frame for
the partial response.

Invalid or unauthenticated request frames are not answered with a plaintext
fallback. Clients must validate version, identity, contiguous indices, counts,
declared bytes, frame bounds, and the 16 MiB assembly bound before concatenating
payloads and decrypting the complete envelope. The checked
`local-webrtc-delivery-chunk-conformance-fixture.json` artifact covers response
and entity kinds, single-chunk, multi-chunk, over-budget-error, and deterministic
greater-than-256 KiB reassembly shapes. Peer teardown unsubscribes every
peer-owned entity id; a replacement peer uses a fresh grant, subscription id,
and authoritative snapshot. There is no SSE or polling fallback.
The generated TypeScript artifact mirrors the browser-visible envelope as
`AesGcmEnvelope` with `nonce`, `ciphertext`, and `version` fields while keeping
the authoritative core Rust struct out of the `botster-hub-client` dependency
boundary.

The hub-side harness uses a Rust WebRTC peer to prove localhost signaling,
ordered/reliable DataChannel establishment, encrypted representative
status/list/attach/input/resize/drain/session traffic, plus byte-exact
reassembly of an encrypted response larger than 256 KiB with every frame below
64 KiB. Browser reassembly lands in the coordinated botster-web ticket; there
is deliberately no period where the old browser decoder can consume this new
hub response wire.

The runnable contract is intentionally adjacent to core package `entrypoints`.
Core `entrypoints` remain the plugin/provider code-load ABI, while
`runnable_entrypoints` is the package discovery shape for clients and future
launchers.

## Stable Package Routes

`DaemonPackage.routes` is the hub-owned browser route contract for installed
package surfaces and package settings. Clients should use these descriptors for
navigation, refresh, direct-load, and browser history instead of reconstructing
paths from local UI conventions.

Route ids are deterministic:

- Plugin surface routes use `surface:<surface_id>` and
  `/packages/<package_name>/surfaces/<surface_id>`.
- Package settings/config routes use `settings` and
  `/packages/<package_name>/settings`.
- Runnable app entrypoint routes use `app:<entrypoint_id>` and
  `/packages/<package_name>/apps/<entrypoint_id>`.

Each descriptor includes `package_name`, `route_id`, `route_path`, `target`,
`title`, `label`, optional `app_id` / `surface_id`, optional icon/category,
`layout_mode`, `required_capabilities`, `enabled`, `blocked`, route
`diagnostics`, and `supports_settings`.

Producer rules are intentionally narrow. Plugin surface route metadata comes
from manifest `surfaces` and package capabilities. Settings routes are exposed
only when the package has a configuration schema; their `required_capabilities`
list is empty because configuration has no package capability producer today.
`layout_mode` is hub-derived route disposition: `plugin_surface`,
`settings_form`, or `app_entrypoint`. Runnable app entrypoint routes use the
entrypoint's declared capabilities. Descriptors must not expose commands, args,
working directories, environment values, package roots, socket paths, or
provenance paths.

`DaemonApp.route` carries the same route descriptor for app rows projected from
runnable entrypoints. `ResolvePackageRoute { package_name, route_id }` resolves a
single route descriptor through the daemon socket without requiring a prior
`ListApps` or `ListPackages` click state. Existing but disabled/blocked routes
return descriptors with structured diagnostics such as `package_not_enabled` or
`missing_required_configuration`. Missing packages or undeclared route ids return
`operator_error` responses with specific codes such as `package_not_installed`
or `route_not_found`.

`ListPackageNavigation` exposes the admitted package navigation registry as
`DaemonPackageNavigationEntry` rows. Explicit manifest `navigation` entries win;
packages without explicit navigation derive default rows from app-like
`surfaces`. Navigation rows reuse the same route descriptors and diagnostics as
`DaemonPackage.routes`, so disabled or blocked packages stay visible but carry
`enabled=false`, `blocked=true`, and the route diagnostic that explains why.
Navigation rows intentionally do not contain ordering authority such as
`order`, `priority`, sidebar placement, layout, or route padding policy.
Clients that need presentation ordering should apply their own local grouping
rules over the stable route and navigation ids.

Plugin surfaces that need embedded package content should return typed UI nodes
such as `iframe` with a package-scoped URL reference in `props.src`, for example
`/packages/<package_name>/assets/<file>`, for the client package bridge to
resolve. The daemon response preserves the validated UI tree in both
`plugin_surface.body` and `plugin_surface.ui_tree_snapshot.body`. The parent UI
node payload must not carry raw HTML fields such as `html`, `raw_html`,
`inner_html`, or `srcdoc`; HTML content remains behind the URL/reference rather
than being injected into the client app DOM. The hub daemon does not currently
serve that URL as a static HTTP asset endpoint.

## Authoritative Session Types And Context

`session_types` is a hub-owned package manifest extension for PTY session
definitions. Packages may contribute declarations, but the running Hub
validates and materializes them into the generic core spawn contract before
calling Core. Definitions combine launch fields with bounded presentation
metadata and orthogonal `role`, `interaction`, `traits`, and `lifecycle`
descriptors. Core treats the classification metadata as opaque strings.

Session types are not `runnable_entrypoints`. Runnable entrypoints describe
installed app/process launch contracts; session types describe PTY sessions
with trusted hub context. The protocol exposes `ListSessionTypes`,
`ShowSessionType`, `ResolveSessionType`, `SpawnSessionType`, and
`ReadSessionContext`, plus source-aware `CreateSessionType`,
`UpdateSessionType`, and `DeleteSessionType` requests.

Resolution precedence is package < device < repo < explicit request values.
Device definitions, admitted repo target roots, and a monotonic definition
generation are durable Hub state. Schema 3 is a cold cut: older schema versions
are rejected before the new shape is deserialized.

Repo-local session types are read from `.botster/session-types.json` under an
admitted target root. The file uses the same `session_types` array shape as
package manifests. Device definitions support full CRUD. Repo definitions
support CRUD only with an enabled admitted `target_id` and are atomically
written beneath that target. Package definitions are read-only and mutation
returns `read_only_session_type_source`. Disabled or unadmitted targets do not
contribute repo definitions.

Explicit environment overrides must appear in
`allowed_environment_overrides`. Explicit cwd overrides must stay inside the
selected source root. Unauthorized target, cwd, path, session type, or environment
requests return operator errors before core spawn.

Spawned scripts receive `BOTSTER_SESSION_ID`, `BOTSTER_CONTEXT_ID`,
`BOTSTER_HUB_DATA_DIR`, `BOTSTER_HUB_SOCKET`, and `BOTSTER_HUB_BIN`. Scripts can
call `botster-hub context` through `BOTSTER_HUB_BIN` to read selected values such
as `prompt`, `repo_path`, `worktree_path`, `branch_name`, `ticket_id`,
`workspace_id`, and safe metadata. Context reads reuse the existing admitted
local daemon API boundary; an unadmitted local caller cannot read context by
guessing a session or context id.

List/show/resolve responses are sanitized and do not include prompt values or
raw context payloads. `ReadSessionContext` is explicit user-path output for the
spawned session or an admitted local operator.

Effective rows expose `source`, `source_name`, `editable`,
`overridden_sources`, and diagnostics. The built-in `session_type` entity family
publishes an initial snapshot and ordered upsert/remove deltas at the durable
generation. It is a Hub-owned lane, distinct from Core-backed `session` and
plugin-provider families. Spawned `session` rows project session type id/source,
role, traits, interaction, and lifecycle from Core lifecycle metadata across
reconnect and restart; missing metadata is explicit absence.

Lua plugins receive target-filtered
`session_types.list({target_id=...})` and
`session_types.show({target_id=..., session_type_id=...})` as ordinary read
projections. The exact `session_type_managed_git_spawn` session-action
scope gates only the single
`session_types.ensure_worktree_and_spawn(...)` mutation. The mutation
accepts semantic target, branch, session type, environment, prompt, ticket,
workspace, and safe metadata values. Hub rejects caller-supplied session id,
cwd, repo/worktree path, branch/base facts, derives those values from the
ensured worktree, and returns a tagged result with a canonical UUID plus
target/branch/worktree/base facts.

Managed Git creation uses the stored target `base_ref`, performs no fetch,
pull, reset, clean, or prune, and reuses dirty exact matches without mutation.
Branch/path/repository ownership conflicts are typed and path-neutral. Session
spawn failure removes only resources created by that call; uncertain cleanup is
reconciled and preserved. The existing `session_type_spawn` scope does not
grant managed Git mutation, and the daemon `CreateWorktree`/`DeleteWorktree`
contract remains generic registered-record admission/removal rather than a Git
operation. Hub-managed Git rows reject record-only deletion, and a target with
managed rows cannot be deleted or reclassified.

## Package Availability

`DaemonPackage` rows include resolved availability so clients do not infer
dependency, feature, config, auth, or capability state from raw package fields.
The hub assembles current registry/config/auth/capability state, calls the core
`resolve_package_dependencies` contract, and projects the resulting matrix into
sanitized daemon DTOs.

Availability fields are additive and default to available for legacy rows:

- `availability`: package-level state and reason/action rows.
- `dependency_availability`: manifest dependency rows in core resolution order.
- `feature_availability`: manifest feature rows in core resolution order.

Reason/action strings are stable client vocabulary and do not expose core debug
strings, local paths, provenance, token values, or auth identities. Current
reason/action pairs are:

- `missing_package` / `install_package`
- `disabled_package` / `enable_package`
- `missing_provider` / `install_provider`
- `missing_capability` / `grant_capability`
- `missing_config` / `configure_package`
- `missing_auth` / `authenticate`
- package-level `package_disabled` / `enable_package`
- package-level invalid configuration diagnostics / `fix_configuration`

Optional integrations should block only the features that declare them. For
example, Project Pipelines local features remain available when no GitHub
provider is installed, while GitHub PR lifecycle features are blocked until the
provider package is installed, enabled, configured, authenticated, and
capability-admitted.

## Package Configuration

Package manifests may declare a core-owned `configuration` schema. The hub owns
policy for submitted values: it validates keys and value types against the
manifest schema, applies manifest defaults to the effective view, blocks enable
when required values are missing, and persists configuration under the package
record in `HubState.package_registry`.

`DaemonPackage` rows expose a sanitized `configuration` object:

- `schema`: the manifest schema as JSON, or omitted for packages without config.
- `effective_values`: defaults plus stored values, keyed by field.
- `missing_required`: required field keys with no effective value.
- `diagnostics`: schema/value diagnostics suitable for clients.

Secrets are write-only. Clients may send a secret value marker through
`SetPackageConfiguration` with `{ "type": "secret", "state": "write_only" }`.
The hub persists and returns only `{ "type": "secret", "state": "redacted" }`
or an unset marker; raw secret material is not part of the daemon protocol.

CLI operators can inspect package configuration with:

```sh
botster-hub packages config --data-dir <path> <package>
```

The checked-in local runtime acceptance target is `project-pipelines` at
`examples/project-pipelines`. Its manifest exposes deterministic
`operator_endpoint`, `pipeline_mode`, and `api_token` configuration fields
through the same `DaemonPackage.configuration` DTO used by first-party clients.

They can update configuration with a JSON object whose values use the core
configuration value shape:

```sh
botster-hub packages config set --data-dir <path> <package> '{"endpoint":{"type":"url","value":"https://example.invalid/hook"},"api_token":{"type":"secret","state":"write_only"}}'
```

The control-plane production route is:

`botster_hub_client::DaemonConnection::request`
to the daemon socket, then `src/daemon_transport.rs` `serve_daemon`/`handle_connection`, then `handle_runtime_control_request`, then `HubClientApi::handle_request`, then `HubRuntime` and the core daemon `SessionIo`/`ClientWorker` terminal data plane.

Terminal attach and drain conformance uses `botster_hub_client::stream_attach`.
That helper still connects through the daemon socket, but terminal bytes are
delivered by the hub-owned client/session actor data plane rather than by a
private session-worker frame contract.

`DaemonEvent::TerminalOutput.data` is renderable terminal text. In contrast,
`DaemonEvent::Snapshot.payload_base64` and
`DaemonEvent::Scrollback.payload_base64` are opaque binary engine state encoded
as standard padded base64. Their `payload_encoding` is the literal `base64`,
and `bytes` is the exact decoded payload length. The client DTO rejects invalid
base64, unknown encodings, and mismatched lengths during deserialization. The
hub preserves the decoded bytes without UTF-8 conversion.
Clients must never append opaque payloads to a terminal, attempt backend-specific
decoding, or infer visible history from byte length or non-emptiness.

The attach/drain ordering contract is that explicit `Attach` enters the
core-owned SessionIo/ClientWorker subscription path and requests initial
terminal history for that subscription. The guaranteed per-subscription order
is `attaching`, optional `snapshot` or `scrollback` history, `attached`, then
later live `terminal_output`. `attaching` means the subscription was requested
but authoritative initial history has not been delivered. `attached` means
initial snapshot delivery is complete and live output may flow. Initial history
is therefore delivered before readiness and later live output.
Clients that restore visible content request `ReadScreen`, present its text,
buffering any live terminal output until restoration is installed, then append
subsequent live output. An idle terminal may produce `attaching`, an optional
authoritative blank `snapshot`, then `attached`: opaque snapshot bytes can
encode dimensions, parser state, and backend metadata without representing
prior renderable terminal output. Clients must not infer visible history from
the snapshot payload byte length. Empty history does not fabricate scrollback,
and the daemon does not maintain a separate scrollback cache. The wire-defined
`detached` state remains part of the client contract and clients must tolerate
it, although no production core component emits it as of the core revision
recorded in `Cargo.lock`.
`stream_attach` writes only
`TerminalOutput` data into its output writer; clients that need event kind,
opaque history payloads, byte-count metadata, or ordering
metadata should use `DaemonConnection` with `Attach` and `Drain`.

Each socket attach cycle owns a fresh transport-local `subscription_id`.
When a persistent daemon socket closes without an explicit `Detach`, the hub
detaches every subscription owned by that connection. Reconnecting clients
attach the same running session with a new `subscription_id`; reusing a prior
subscription ID across socket loss is not supported. The new subscription
receives the session's opaque initial engine state before `attached`, then only
later live output. The dropped subscription must not receive that later output.
`ReadScreen.text` on the same running session is the backend-neutral semantic
source for retained visible terminal markers.

Attach `Snapshot`/`Scrollback` and `DaemonRequest::CaptureSnapshot` both describe
backend-opaque state. Their payloads, formats, and byte counts must never be used
as evidence that visible terminal history exists. Only `ReadScreen.text` and
later `TerminalOutput.data` are renderable terminal text.

`DaemonRequest::ReadScreen`, `DaemonRequest::ReadModeFlags`, and
`DaemonRequest::CaptureSnapshot` are control-plane request/response readback
operations for a running session. They route through the same production path
as other local clients:
`daemon_transport -> HubClientApi -> HubRuntime -> CoreDaemon`. `ReadScreen`
returns `DaemonReadScreen { session_id, text }`. `ReadModeFlags` returns
`DaemonModeFlags { session_id, mouse_mode }`, where `mouse_mode` is the exact
authoritative `u8` bitmask (`0` is off and combined tracking plus SGR reporting
is `9`). The other core mode booleans are not authoritative and are not exposed.
Unknown sessions and backend failures return `operator_error` with no
`mode_flags` body; clients must not substitute a successful zero value.
`CaptureSnapshot` returns `DaemonCaptureSnapshot { session_id, rows, cols,
payload_format, payload_bytes }`. The hub does not expose the opaque snapshot
bytes in this response. Mode flags are probed on demand and never arrive as a
server-pushed mode-change event.

The reusable first-party fixture for this rendering contract lives in
`botster_hub_test_support::late_attach_history_conformance_scenario`. It returns
public `botster_hub_client::DaemonEvent` values only:

- `read_screen_text` contains the visible restored-history marker;
- `history_then_live` includes `attaching`, binary-safe `snapshot` or
  `scrollback` whose decoded base64 length equals `bytes`, `attached`, later
  `terminal_output`, and process-exit metadata;
- `no_history_then_live` includes `attaching`, then `attached`, then later live
  terminal output and process-exit metadata without prior renderable output or
  fabricated scrollback, while `no_history_read_screen_text` is empty.
  Production backends may insert an opaque authoritative blank `snapshot`
  before `attached`.

Rust downstream tests can consume the typed scenario directly. Browser/TUI
tests that cannot depend on the Rust crate should mirror the stable JSON from
`botster_hub_test_support::late_attach_history_conformance_fixture_json` and
assert the same event ordering and classification. `AttachState` and
`ProcessExit` are metadata/control events, not terminal bytes to render.

Node clients can consume that exact JSON through
`readLateAttachHistoryConformanceFixture()` from
`@trybotster/hub-test-support@0.1.16`. Version 0.1.6 / conformance revision 13
uses JSON number arrays for opaque history and is superseded because that shape
unnecessarily expands large Ghostty snapshots on the bounded WebRTC response
path. Version 0.1.5 / revision 12 still exposes lossy string history. Neither is
the current binary contract. The same package exposes
`readFirstPartyClientSupportMatrix()`. Because the current hub-client helper
uses one feature list for advertised and required compatibility, the published
matrix includes `terminal_readback` in both `supported_features` and
`required_features`. Downstream compatibility checks must therefore implement
terminal readback; splitting required from supported is a separate protocol
contract change.

The targeted mode readback fixture lives in
`botster_hub_test_support::mode_flags_conformance_scenario`, with stable JSON
from `mode_flags_conformance_fixture_json`. The checked Node package exports it
through `readModeFlagsConformanceFixture()`. It covers exact off (`0`) and
combined-on (`9`) values, response session attribution, unknown-session error,
and backend failure without a default success body.

`DaemonEvent::WorktreeLifecycle` exposes hub-owned worktree CRUD lifecycle
events to clients through the normal `DaemonResponse.events` field. The inner
`DaemonWorktreeLifecycleEvent` carries `event`, optional `worktree_id`, optional
`target_id`, optional `status`, optional `label`, optional relative
`display_path`, and failure `failure_kind`/`message` fields. Current emitted
event names are `worktree_created`, `worktree_create_failed`,
`worktree_deleted`, and `worktree_delete_failed`. `worktree_deleted` is the
canonical successful delete event because the hub deletes the record, not the
filesystem directory.

Worktree lifecycle events are sanitized. They do not include raw absolute
worktree paths by default; clients that need trusted local paths should read the
ordinary `DaemonWorktree.path` field from the worktree response DTO. There is no
separate worktree entity family in this protocol revision.

The fixture is a client conformance contract, not a replacement daemon harness.
The live runtime path remains covered by
`external_daemon_same_session_reattach_replays_opaque_history_before_live_output`,
which proves socket-loss cleanup, fresh-subscription reattach, byte-exact opaque
payload projection and ordering before later live output, `ReadScreen` marker
fidelity, and the no-history case.

## Many-PTY client attach proof

`botster_hub_test_support::run_many_pty_client_attach_conformance` composes the
production path into one adversarial correctness proof. It starts many sessions
through public `DaemonRequest::Spawn` calls on an isolated hub configured with
the real session-worker binary. One noisy PTY remains interactive while the
other PTYs exit. Public `Drain` requests advance lifecycle reconciliation for
each quiet session without attaching it, then bounded public `ListSessions`
polling must observe `exited`. The proof does not attach to quiet sessions or
use a sleep as its success condition.

After the quiet sessions exit, the same public client connection attaches late
to the noisy session, drains opaque initial state, checks `ReadScreen` and
`CaptureSnapshot`, sends labeled input, and observes the later live marker after
the restored history. Every requested session receives an explicit
`ShutdownSession` cleanup attempt. Failures use one of the stable labels
`spawn`, `attach`, `drain`, `input`, `history`, or `cleanup`, with synthetic
session IDs and path-neutral details. A label names the phase of the proof, not
necessarily the daemon request that failed. In particular, quiet-session
reconciliation maps `Drain` failures to `spawn`, the pre-attach screen wait maps
`Drain` failures to `history`, and the post-input live-output loop maps `Drain`
failures to `drain`.

Run the CI-safe eight-session case with:

```sh
./test.sh --test hub_daemon_lifecycle_test external_hub_client_many_pty_adversarial_conformance_ci
```

Run the ignored 32-session local case with:

```sh
./test.sh --test hub_daemon_lifecycle_test external_hub_client_many_pty_adversarial_conformance_local -- --ignored --exact
```

Both commands exercise `botster-hub-client` over the daemon socket, then
`daemon_transport -> HubClientApi -> HubRuntime -> CoreDaemon`, and finally the
worker-backed `SessionIo`/`ClientWorker` terminal path. The session counts are
bounded correctness cases, not performance targets or benchmark claims.

Adding the `refresh_local_packages` daemon request changes the request
vocabulary, so `PROTOCOL_VERSION` advances to 3 alongside
`CONFORMANCE_FIXTURE_REVISION` 18. This was a cold cut with no protocol-v2 parser
or parallel fixture. Because `DaemonCompatibilityRequirement::current()`
derived `protocol_version` from `PROTOCOL_VERSION`, clients built at that
historical identity required a Hub advertising protocol version 3. Current
protocol identities require an exact version match.

Replacing revision-13 JSON byte arrays with validated `payload_base64`, literal
`payload_encoding: "base64"`, and decoded `bytes` fields increments
`CONFORMANCE_FIXTURE_REVISION` to 14. `PROTOCOL_VERSION` remains unchanged
because daemon framing and request issuance are unchanged. Revision 14 retains
the separate `read_screen_text` semantic restoration oracle. Revision 13 is
superseded and revision 12 does not identify any binary-safe history DTO.

Adding targeted `read_mode_flags`, its authoritative `DaemonModeFlags` body,
and the mode-flags conformance fixture increments
`CONFORMANCE_FIXTURE_REVISION` to 15. `PROTOCOL_VERSION` remains 1 because this
is an additive request/response operation under the existing
`terminal_readback` compatibility feature. Revision 14 does not identify the
mode readback DTO or exact-value/error fixture.

Aligning attach readiness with the core contract alongside the local WebRTC
chunk fixture increments `CONFORMANCE_FIXTURE_REVISION` to 12.
`PROTOCOL_VERSION` remains unchanged because the daemon framing and event shapes
are unchanged; revision 12 guarantees
`attaching -> optional initial state -> attached -> live output`. Revision 11
does not identify the corrected readiness ordering.

Adding `worktree_lifecycle` increments `CONFORMANCE_FIXTURE_REVISION`.
`PROTOCOL_VERSION` remains unchanged because daemon framing and request issuance
are unchanged.

Adding `read_screen` and `capture_snapshot` daemon requests, readback response
fields, and the `terminal_readback` feature increments
`CONFORMANCE_FIXTURE_REVISION`. `PROTOCOL_VERSION` remains unchanged because
daemon framing and request issuance are unchanged.

Do not reuse `botster_core::contract` session-worker protocol, session frame magic, `DefaultEngineCommand`, `TransportIngress`, or `BoundaryJson` for external clients. Those are not the client-to-hub protocol. The client crate also intentionally excludes hub runtime, Lua/plugin runtime, `ratatui`, `crossterm`, and `mlua`; its UI DTOs come only from `botster-ui-contract`.

Plugin surface render responses cross the daemon boundary as a
`DaemonPluginSurface` envelope containing `package_name`, `surface_id`, a JSON
typed `UiNode` body and `ui_tree_snapshot` for browser/TUI rendering. The
snapshot repeats `package_name` and `surface_id` and carries the same validated
`UiNode`. Hub-owned code renders through `HubRuntime::render_plugin_surface`,
deserializes against `botster-ui-contract`, and validates before serializing
the response. This is authored validation: Button/IconButton/MenuItem label,
Form submit_label, Iframe src/title, and Text text accept valid sentinels before
materialization. Clients own materialization and strict realized validation;
Rust clients use the contract crate validator, while non-Rust clients enforce
the equivalent sentinel-free boundary because the npm package publishes DTOs,
schema, and fixtures rather than a JavaScript runtime validator. Hub
intentionally has no realized-tree caller. Plugin actions use one canonical `UiActionRequest` envelope and
return a typed `UiActionResult`; the daemon and worker do not reconstruct split
request fields. A result must echo the request's `request_id`, `surface_id`,
`action_id`, and `node_id` exactly, including preserving an absent `node_id`.
The Hub rejects a mismatched identity as `invalid_action_result` before it can
cross the client boundary.

The packaged `contract.app` producer supplies live presentation proof in
addition to the static UI-contract fixtures. Its rendered open action returns
accepted `set` operations for `contract-dialog` and
`selected-workspace = "workspace-alpha"`; the test-support runner applies those
operations to package/surface-scoped client state and evaluates the delivered
presence/equality bindings. An invalid rendered form submission rejects while
retaining the tree and open state, a valid submission returns normalized values
and a replacement before clearing the dialog, and a distinct rendered action
proves deterministic toggle transitions.

Relocating the one canonical Form into `contract-dialog.slots.body` advances
`CONFORMANCE_FIXTURE_REVISION` to 22. The published browser-shaped conformance
consumer now materializes the delivered tree after accepted scoped effects,
restricts submit discovery to the active Dialog subtree, rejects actionable
sibling Forms, retains the visible Dialog/Form/input association after
rejection, applies an accepted replacement as the whole rendered surface tree,
and proves the scoped Dialog presence key is cleared after acceptance. The
replacement is not inserted beneath the submitting `node_id`; this keeps the
accepted replacement observable even though the same result closes the Dialog.
`PROTOCOL_VERSION` remains 4 because daemon framing, request vocabulary, DTO
shapes, and action semantics are unchanged. Because
`DaemonCompatibilityRequirement::current()` derives
`minimum_conformance_fixture_revision` from this constant, clients built at
revision 22 require a Hub reporting conformance revision 22 or later.

Adding required `DaemonSessionEntity.lifecycle_class`, the Hub-only `/session`
binding-family admission pass, and the real `contract.sessions` worker surface
advances `CONFORMANCE_FIXTURE_REVISION` to 23. `PROTOCOL_VERSION` remains 4:
daemon framing and request issuance are unchanged. No feature is added because
`session_entity_subscriptions` already names the delivered capability and the
current helper would otherwise make a new feature globally required.

Moving package surface and navigation semantics into
`@trybotster/ui-contract@0.1.1`, making `DaemonPackage.surfaces` reference the
canonical `PackageSurfaceDescriptor`, and adding explicit contract-matrix
navigation advances `CONFORMANCE_FIXTURE_REVISION` to 24.
`PROTOCOL_VERSION` remains 4 because request framing and discriminants are
unchanged; the generated TypeScript artifact now composes the canonical
contract instead of declaring a daemon-owned mirror.

This cold switch advances `PROTOCOL_VERSION` to 4 and
`CONFORMANCE_FIXTURE_REVISION` to 19. It removes `UiTreeUpdateRef` and
`tree_update`, requires explicit form `submit_label`, supports scoped
presentation set/clear/toggle operations, and permits one validated inline
replacement tree only on accepted results. There is no protocol-v3 parser or
parallel legacy action path.

Expanding the plugin contract matrix `contract.app` fixture to cover
application primitives `metric_grid`, `table`, `toolbar`, `empty_state`,
`status_badge`, `section`, and `panel` increments
`CONFORMANCE_FIXTURE_REVISION` to 20. `PROTOCOL_VERSION` remains unchanged because
the daemon framing and `plugin_surface_render` request/response shape are
unchanged; the hub still delegates validation to the locked
`botster-ui-contract::UiNode` contract.

Adding optional spawn-target `base_ref` fields and the worktree `management`
projection advances `CONFORMANCE_FIXTURE_REVISION` to 21.
`PROTOCOL_VERSION` remains 4: the existing request/response framing and feature
families are unchanged, while legacy JSON omitting the new fields continues to
deserialize as a directory target and registered worktree.

Publishing `@trybotster/hub-test-support@0.1.2` adds an explicit
application-primitives package API and metadata alias over that already-revised
fixture. This does not increment `CONFORMANCE_FIXTURE_REVISION` again because
the daemon conformance surface, fixture bytes, protocol DTOs, and validated
UiNode payload are unchanged from revision 8.

Adding authoritative software/install identity, the `CheckHubUpdate`
request/response family, and cold-removing inferred `hub_version` from public
package compatibility advances `PROTOCOL_VERSION` to 5 and
`CONFORMANCE_FIXTURE_REVISION` to 29. Current first-party requirements reject
protocol 4 or conformance 28 before issuing the new request. A stale 4/28
client accepts the newer descriptor and ignores additive status identity fields,
but does not know or issue `CheckHubUpdate`. No feature constant is added; the
protocol version is the single compatibility boundary.

Cold-replacing the legacy operations and DTOs with authoritative session types
advances `PROTOCOL_VERSION` to 6 and
`CONFORMANCE_FIXTURE_REVISION` to 31. Revision 31 corrects the generated
TypeScript discriminators for the Rust `source`- and `policy`-tagged unions and
adds the bounded `entity_error` frame used when a resync snapshot exceeds the
daemon frame limit. Protocol 6 requires exact protocol-version
agreement and the `session_type_entity_subscriptions` feature before any request
dispatch. It includes source-aware definition CRUD, provenance/editability,
orthogonal role/interaction/traits/lifecycle fields, durable definition entity
generations, and canonical session metadata projection. Protocol 5 clients fail
the typed compatibility handshake; no aliases or dual readers remain.

## Isolated Integration Tests For External Clients

External clients that need a true live-hub integration test should depend on the
client protocol crate plus the test-support crate, not on the full `botster-hub`
library. Until these crates are published to crates.io, the supported
out-of-repo dependency shape is a git dependency pinned to the same repository
revision for both crates. Use one exact commit SHA for every Botster crate in
the downstream test so the client protocol crate, test harness crate, hub
binary, and session-worker binary all come from the same protocol revision:

```toml
[dev-dependencies]
botster-hub-client = { git = "https://github.com/trybotster/botster-hub.git", package = "botster-hub-client", rev = "<hub-rev>" }
botster-hub-test-support = { git = "https://github.com/trybotster/botster-hub.git", package = "botster-hub-test-support", rev = "<hub-rev>" }
```

The harness starts the `botster-hub` binary as a subprocess and talks to it
through `botster-hub-client`. It does not compile or link hub runtime, TUI, Lua,
or plugin internals into the downstream client.

Build or otherwise provide both binaries before running the downstream test. The
fixture does not provision binaries itself; third-party CI should either build
them from the same checkout/revision or download a release artifact that matches
the crate revision under test. Use `--locked` when building from source so the
hub's committed lockfile preserves the `botster-core` revision paired with that
hub checkout.

```bash
BOTSTER_ENV=test cargo build --locked --bin botster-hub
BOTSTER_ENV=test cargo build --locked -p botster-core --bin botster-session-worker
```

Then pass explicit paths into the harness. Environment variables are accepted as
a convenience, but the library never relies on `CARGO_BIN_EXE_botster-hub`
internally because Cargo only injects that variable for the package that owns the
binary. The compile-checked usage examples live on
`botster_hub_test_support::IsolatedHubBuilder`,
`botster_hub_test_support::run_client_conformance`, and
`botster_hub_test_support::run_plugin_contract_matrix_conformance`.
Client protocol drift checks can obtain the checked generated TypeScript
protocol through
`botster_hub_test_support::daemon_protocol_typescript_artifact()`. The returned
artifact path is stable for reports, while the contents still come from the
authoritative `botster-hub-client` generator.

Node client tests should use the declared npm dependency instead of a relative
hub checkout:

```sh
npm install --save-dev @trybotster/ui-contract@0.3.1 @trybotster/hub-test-support@0.1.20
```

```js
import {
  materializePluginContractMatrixFixture,
  readDaemonProtocolTypescript,
} from "@trybotster/hub-test-support";

const protocolSource = readDaemonProtocolTypescript();
const fixturePath = materializePluginContractMatrixFixture(tempDirectory);
```

Local environment variables may still point legacy drift checks at a checked-out
hub artifact, but after publication the normal web-client dependency coordinate
is `@trybotster/hub-test-support@0.1.18` from the public npm registry. The
published 0.1.17 artifact contains stale daemon protocol bytes and must not be
used as contract authority.

Each harness instance creates a disposable data directory and socket path under
the configured test root, uses synthetic default hub identity, and attempts a
daemon shutdown on drop with a kill fallback for failed tests. Tests should still
call `shutdown()` explicitly when they need teardown failures to be visible.

`run_client_conformance` returns a stable report instead of raw event streams.
It covers status, empty session list, spawn, terminal attach/drain through
`stream_attach`, input echo, resize observation through `stty size`, a missing
session validation error, connected diagnostics, terminal-unavailable
diagnostics, and teardown. Downstream CI can run it twice against two fresh
isolated hubs and compare the reports to prove deterministic fixture output.

Downstream `botster-tui` tests should import
`botster_hub_test_support::first_party_client_support_matrix` directly and
compare it to `run_client_conformance` for the local client paths they exercise.
Downstream `botster-web` tests should consume the matrix and late-attach history
scenario as serialized JSON, for example with
`serde_json::to_value(first_party_client_support_matrix())` and
`late_attach_history_conformance_fixture_json()` from a Rust fixture or repo sync
step, rather than mirroring the matrix or daemon event fields by hand in
TypeScript.

If a downstream client also wants to prove plugin surface/action dispatch,
configuration, route descriptors, failure diagnostics, and the shared
application primitive/form/action-feedback contract, copy the published fixture
from `botster-hub-test-support` into a caller-owned temp directory and call
`run_plugin_contract_matrix_conformance` with the copied package root:

```rust
let fixture_root = tempfile::tempdir().expect("fixture tempdir");
let fixture_path = botster_hub_test_support::copy_plugin_contract_matrix_fixture(
    fixture_root.path(),
)
.expect("copy plugin contract matrix fixture");

let hub = botster_hub_test_support::IsolatedHubBuilder::new()
    .hub_bin(std::env::var("BOTSTER_HUB_BIN").expect("BOTSTER_HUB_BIN"))
    .session_worker_bin(
        std::env::var("BOTSTER_SESSION_WORKER_BIN").expect("BOTSTER_SESSION_WORKER_BIN"),
    )
    .start()
    .expect("isolated hub starts");

let report = botster_hub_test_support::run_plugin_contract_matrix_conformance(
    &hub,
    fixture_path,
)
.expect("plugin UI conformance");
assert_eq!(report.app_surface_node_id, "contract-app-panel");
assert!(report.dialog_visible_after_open);
assert!(report.selected_workspace_visible_after_open);
assert!(report.rejected_state_retained);
assert!(!report.dialog_visible_after_valid_submit);
assert_eq!(report.action_error_diagnostic_kind, "action_failure");
assert_eq!(
    report.client_render_check.class,
    botster_hub_test_support::ConformanceFailureClass::ClientRendering,
);
hub.shutdown().expect("shutdown isolated hub");
```

Explicit local package paths and environment-specific binary paths remain useful
for hub developers and local overrides, but client repositories should not need
a sibling hub checkout for the normal fixture or protocol-artifact path.

Hub developers can run the full fixture proof from this repository with:

```bash
./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts
```

Hub CI also runs the persisted-package runtime smoke:

```bash
./test.sh --test hub_daemon_lifecycle_test cli_smoke_proves_local_runtime_daemon_package_app_session_and_webrtc
```

The repository-level production acceptance command is
`script/test-production-package-runtime`. It requires explicit clean repository
paths and exact revisions for Hub, Core, Web, TUI, TUI Kit, Workspaces, and
Project Pipelines. Before any Web build or browser leg, it installs Web's
declared `@trybotster/hub-test-support` coordinate in a clean external consumer
and requires the installed metadata, actual generated protocol bytes, fixture
revision, and packaged asset checksums to match the exact Hub source and Web's
vendored copy.

Its fresh and pre-cutover-upgrade legs both use isolated explicit data
directories and daemon-owned sockets. The upgrade producer runs from temporary
detached worktrees; current `up` must refresh the same persisted local package
paths without a package reload command or state edit. Evidence includes
path-neutral revision and command manifests, structured dynamic Web URLs,
browser and TUI live-runtime output, plugin tool registration, worker-backed
session adoption after daemon restart, cleanup, and identical before/after
manifests for the operator's default runtime state.

Separate contract-matrix coverage runs `run_project_pipelines_conformance`
against the packaged Project Pipelines example to prove first-party external
package enablement, `PluginSurfaceRender`, `ui_tree_snapshot` identity, form
node structure, and
`PluginSurfaceAction` field-error/action-failure feedback. Downstream plugin
repos should consume these published helpers and fixture assets instead of
inventing DTO fixtures or reading a stale sibling hub checkout.

Web and TUI developers should run their renderer-specific tests against the
same report fields. Producer contract failures are `ConformanceError` values
classified as `ProducerContract`; local setup failures such as missing
`BOTSTER_HUB_BIN` or `BOTSTER_SESSION_WORKER_BIN` are `IsolatedHubError` values
classified as `EnvironmentSetup`; renderer mismatches are client-owned
comparisons against `report.client_render_check` and classified as
`ClientRendering`.

First-party plugin developers should keep shared primitive/form/action contract
tests pointed at this hub-owned fixture unless they need product-specific
behavior. Product helpers such as `run_project_pipelines_conformance` cover
package-specific render/action plumbing, but the support matrix's shared
plugin-surface claim is backed by the generic contract matrix fixture.

To prove foreground package app-open support without recreating hub launcher
policy, call `run_foreground_terminal_app_open_conformance`. The helper installs
a local `terminal_app` / `foreground_stdio` package, discovers it through
`ListApps`, resolves it through `ResolveAppLaunch`, executes the returned
command with the daemon-provided working directory and environment, and has the
child process decode the manifest-targeted Core Hub connection descriptor and
perform a real `Status` request through its absolute Unix socket path. Its
report asserts the typed connection and data-directory injections were present
and absolute, and that the child exited with code 0 after completing the real
daemon request from its package working directory.

The matrix marks JSON plugin surface render/action dispatch and the Hub-owned
`/session` producer/reference binding contract as supported. It explicitly
does not claim shipped Web/TUI rendering: browser ticket
`ticket_1785298229_125024` and terminal ticket
`ticket_1785438029_926883` owns those production entity-store/binding paths.
