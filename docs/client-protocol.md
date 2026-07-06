# Botster Hub Client Protocol

The authoritative reusable client-to-hub daemon protocol lives in:

- `crates/botster-hub-client/src/lib.rs`
- `src/daemon_transport.rs`

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
sibling hub checkout through the package:

```sh
npm install --save-dev @trybotster/hub-test-support@0.1.0
```

```js
import {
  materializePluginContractMatrixFixture,
  metadata,
  readDaemonProtocolTypescript,
} from "@trybotster/hub-test-support";

const protocolSource = readDaemonProtocolTypescript();
const fixturePath = materializePluginContractMatrixFixture(tempDirectory);

console.log(metadata.protocol, metadata.conformance_fixture_revision, fixturePath);
```

The package includes checksum metadata so browser-client tests can fail clearly
when checked assets are stale. The metadata's protocol version and conformance
fixture revision are emitted by the Rust `botster-hub-test-support` asset
generator instead of being maintained independently in JavaScript.

For npm-based client repos such as botster-web, use the exact dependency spec
`"@trybotster/hub-test-support": "0.1.0"` in `devDependencies` and let npm write
the corresponding package-lock entry from the public npm registry. The package
is public, so install does not require a scoped `.npmrc` entry or CI auth token.
After updating the lockfile, run the client smoke that imports the package,
reads the daemon protocol artifact, calls `verifyPackageAssets()`, and
materializes the plugin contract matrix fixture.

## Compatibility Handshake

Clients should check hub compatibility before depending on request-specific
behavior. `DaemonConnection::connect`, `request`, and `stream_attach` perform
the current first-party compatibility check during the socket hello handshake.
The running hub also returns the same descriptor on `DaemonRequest::Status` so
operator UIs can show protocol diagnostics without opening a special endpoint.

The current descriptor includes:

- protocol name and version;
- supported features: sessions, terminal streaming, resize, plugin surface
  render, and plugin surface action dispatch;
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

Foreground terminal launch contracts currently inject `BOTSTER_HUB_SOCKET` and
`BOTSTER_HUB_DATA_DIR`. These are the canonical same-device connection and
runtime data directory values for terminal clients; clients should not expect the
older example names `BOTSTER_HUB_CONNECTION` or `BOTSTER_PACKAGE_DATA_DIR`.

Supervised entrypoints are local development processes, not a production
installer or sandbox. The daemon stops them on explicit stop/restart, package
disable/remove, `DaemonShutdown`, and daemon SIGINT/SIGTERM cleanup.

Web app `local_url` values, including `botster-web` dogfood `bridge=` / `web=`
URLs, are supervised local package app outputs. They remain health and dev
bridge surfaces, not the terminal/session data plane.

For the installed `botster-web` `web-client` entrypoint, `StartPackageEntrypoint`
also returns `local_webrtc_bootstrap` when it mints a short-lived local browser
grant. The bootstrap contains the grant id/secret, expected same-device origin,
expiry, signaling transport (`daemon_request`), data plane
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
Accepted DataChannel messages are JSON serialized `botster_core::AesGcmEnvelope`
values. The envelope plaintext is the existing daemon `DaemonRequest`, and the
encrypted response plaintext is `DaemonResponse`; invalid or unauthenticated
DataChannel frames are not answered with a plaintext fallback.
The generated TypeScript artifact mirrors the browser-visible envelope as
`AesGcmEnvelope` with `nonce`, `ciphertext`, and `version` fields while keeping
the authoritative core Rust struct out of the `botster-hub-client` dependency
boundary.

The first hub-side harness uses a Rust WebRTC peer to prove localhost signaling,
ordered/reliable DataChannel establishment, encrypted representative
status/list/attach/input/resize/drain/session traffic, bounded grants, and no
persistence of grant secrets. That proves the hub adapter and local signaling
contract. Real browser `RTCPeerConnection` interop remains a botster-web parity
follow-up while the HTTP/SSE dogfood bridge stays available.

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

## Session Templates And Context

`session_templates` is a hub-owned package manifest extension for PTY session
launch templates. Packages may contribute declarations, but the running hub
validates and materializes them into the generic core spawn contract before
calling core. Core receives only command, args, cwd, environment, metadata, and
PTY size.

Session templates are not `runnable_entrypoints`. Runnable entrypoints describe
installed app/process launch contracts; session templates describe PTY sessions
with trusted hub context. The protocol exposes `ListSessionTemplates`,
`ShowSessionTemplate`, `ResolveSessionTemplate`, `SpawnSessionTemplate`, and
`ReadSessionContext`.

Resolution precedence is package < device < repo < explicit request values.
Device template sources and admitted repo target roots are durable hub-state
fields. They are additive v1 fields, so a `hub-state.json` written before these
sources existed loads with empty device/repo sources rather than requiring a
schema-version migration.

Repo-local templates are read from `.botster/session-templates.json` under an
admitted target root. The file uses the same `session_templates` array shape as
package manifests. Repo-local files are rediscovered fresh for each
list/show/resolve/spawn request; there is no separate reload command. Disabled
or unadmitted targets do not contribute repo templates.

Explicit environment overrides must appear in
`allowed_environment_overrides`. Explicit cwd overrides must stay inside the
selected source root. Unauthorized target, cwd, path, template, or environment
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

The checked-in dev-stack acceptance target is `project-pipelines` at
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

`DaemonEvent::TerminalOutput`, `DaemonEvent::Snapshot`, and
`DaemonEvent::Scrollback` expose renderable terminal data as a `data` string.
The daemon converts raw terminal bytes with the same lossy UTF-8 decoding for
all three event kinds. `Snapshot` and `Scrollback` also include `bytes`, which is
the raw byte length before decoding, so clients can preserve existing size/count
logic without deriving a possibly different decoded string length.

If a client observes older-hub or non-current history evidence with a positive
byte count but no renderable `data`, it must treat that history as opaque or
unsupported and continue live-only. It must not synthesize terminal scrollback
from the byte count. Current `botster-hub-client` `Snapshot` and `Scrollback`
DTOs require `data`, so byte-only JSON is a compatibility/fallback signal for
defensive client implementations rather than the current serde shape.

The attach/drain ordering contract is that explicit `Attach` enters the
core-owned SessionIo/ClientWorker subscription path and requests initial
terminal history for that subscription. Initial `snapshot` or `scrollback`
history is delivered before later live `terminal_output` for that subscription.
Clients should render the restored history payload first, then append subsequent
live output. Empty core snapshots do not fabricate history, and the daemon does
not maintain a separate scrollback cache. `stream_attach` writes only
`TerminalOutput` data into its output writer; clients that need event kind,
history payloads, history fallback handling, byte-count metadata, or ordering
metadata should use `DaemonConnection` with `Attach` and `Drain`.

The reusable first-party fixture for this rendering contract lives in
`botster_hub_test_support::late_attach_history_conformance_scenario`. It returns
public `botster_hub_client::DaemonEvent` values only:

- `history_then_live` includes attach metadata, non-empty `snapshot` or
  `scrollback` history with `bytes == data.len()`, later `terminal_output`, and
  process-exit metadata;
- `no_history_then_live` includes attach metadata, later live terminal output,
  and process-exit metadata without fabricating non-empty `snapshot` or
  `scrollback` history.

Rust downstream tests can consume the typed scenario directly. Browser/TUI
tests that cannot depend on the Rust crate should mirror the stable JSON from
`botster_hub_test_support::late_attach_history_conformance_fixture_json` and
assert the same event ordering and classification. `AttachState` and
`ProcessExit` are metadata/control events, not terminal bytes to render.

The fixture is a client conformance contract, not a replacement daemon harness.
The live runtime path remains covered by
`external_daemon_attach_replays_prior_history_with_renderable_byte_count`, which
proves the daemon socket path emits matching public event semantics for restored
history before later live output and for the no-history case.

The addition of required renderable `data` fields on `snapshot` and `scrollback`
increments `CONFORMANCE_FIXTURE_REVISION`. `PROTOCOL_VERSION` remains unchanged
because the daemon framing and request/response protocol are the same; clients
that depend on renderable history should require the current conformance fixture
revision during the hello handshake.

Do not reuse `botster_core::contract` session-worker protocol, session frame magic, `DefaultEngineCommand`, `TransportIngress`, or `BoundaryJson` for external clients. Those are not the client-to-hub protocol. The client crate also intentionally excludes hub runtime, Lua/plugin runtime, `ratatui`, `crossterm`, `mlua`, and core UI action/node types.

Plugin surface render responses cross the daemon boundary as a
`DaemonPluginSurface` envelope containing `package_name`, `surface_id`, a JSON
`body` payload for compatibility, and `ui_tree_snapshot` for browser/TUI
rendering. The snapshot repeats `package_name` and `surface_id` and carries the
same validated UiNode JSON in `body`. Hub-owned code renders through
`HubRuntime::render_plugin_surface`, deserializes the plugin payload into the
locked core UiNode contract, and validates it before serializing this response.
Clients should prefer `ui_tree_snapshot` as the blessed surface rendering path
and keep `body` only as a compatibility fallback for older hubs.

Adding `ui_tree_snapshot` increments `CONFORMANCE_FIXTURE_REVISION`.
`PROTOCOL_VERSION` remains unchanged because daemon framing and request issuance
are unchanged. Clients that require hub-validated plugin surface snapshots should
require the current conformance fixture revision during the hello handshake.
Plugin action responses still cross as JSON values. External clients are not
required to compile internal UI/runtime dependencies.

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
npm install --save-dev @trybotster/hub-test-support@0.1.0
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
hub artifact, but the normal web-client dependency coordinate is
`@trybotster/hub-test-support@0.1.0` from the public npm registry.

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
configuration, route descriptors, and failure diagnostics, copy the published
fixture from `botster-hub-test-support` into a caller-owned temp directory and
call `run_plugin_contract_matrix_conformance` with the copied package root:

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

Web and TUI developers should run their renderer-specific tests against the
same report fields. Producer contract failures are `ConformanceError` values
classified as `ProducerContract`; local setup failures such as missing
`BOTSTER_HUB_BIN` or `BOTSTER_SESSION_WORKER_BIN` are `IsolatedHubError` values
classified as `EnvironmentSetup`; renderer mismatches are client-owned
comparisons against `report.client_render_check` and classified as
`ClientRendering`.

First-party plugin developers should keep plugin contract tests pointed at this
hub-owned fixture unless they need product-specific behavior. Product helpers
such as `run_project_pipelines_conformance` can still cover product workflows,
but the support matrix's plugin-surface claim is backed by the generic contract
matrix fixture.

To prove foreground package app-open support without recreating hub launcher
policy, call `run_foreground_terminal_app_open_conformance`. The helper installs
a local `terminal_app` / `foreground_stdio` package, discovers it through
`ListApps`, resolves it through `ResolveAppLaunch`, executes the returned
command with the daemon-provided working directory and environment, and has the
child process perform a real `Status` request through `BOTSTER_HUB_SOCKET`. Its
report asserts the canonical `BOTSTER_HUB_SOCKET` and `BOTSTER_HUB_DATA_DIR`
environment values were present and that the child exited with code 0.

The matrix currently marks JSON plugin surface render/action dispatch as
supported through the contract-matrix fixture and full plugin entity-frame
hydration as intentionally unsupported by this conformance fixture. Clients
that render plugin entity stores should prove that path with their own
entity-frame tests until the hub publishes a dedicated entity conformance
fixture.
