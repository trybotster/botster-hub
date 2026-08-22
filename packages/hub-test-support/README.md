# @trybotster/hub-test-support

Node-consumable Botster hub test-support assets for first-party web clients.

The package is a generated wrapper over `botster-hub-test-support` and
`botster-hub-client`. Do not edit `daemon-protocol.ts`,
`first-party-client-support-matrix.json`,
`session-lifecycle-subscription-conformance-fixture.json`,
`session-plugin-binding-conformance-fixture.json`,
`late-attach-history-conformance-fixture.json`,
`local-webrtc-delivery-chunk-conformance-fixture.json`,
`mode-flags-conformance-fixture.json`,
`fixtures/plugin-contract-matrix`, or `metadata.json` by hand; run:

```sh
node packages/hub-test-support/scripts/sync-assets.mjs
```

## Usage

Use this command for version 0.1.42:

```sh
npm install --save-dev @trybotster/ui-contract@0.3.3 @trybotster/hub-test-support@0.1.42
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
  readLocalWebrtcDeliveryChunkConformanceFixture,
  readModeFlagsConformanceFixture,
  readSessionLifecycleSubscriptionConformanceFixture,
  readSessionPluginBindingConformanceFixture,
  readUiContractConformanceFixtures,
} from "@trybotster/hub-test-support";

const protocolSource = readDaemonProtocolTypescript();
const fixturePath = materializePluginContractMatrixFixture(tempDirectory);
const applicationPrimitivesPath = materializeApplicationPrimitivesFixture(tempDirectory);
const supportMatrix = readFirstPartyClientSupportMatrix();
const lateAttachFixture = readLateAttachHistoryConformanceFixture();
const localWebrtcChunkFixture = readLocalWebrtcDeliveryChunkConformanceFixture();
const modeFlagsFixture = readModeFlagsConformanceFixture();
const sessionLifecycleFixture = readSessionLifecycleSubscriptionConformanceFixture();
const sessionBindingFixture = readSessionPluginBindingConformanceFixture();
const sessionBindingStages =
  materializeSessionPluginBindingScenario(sessionBindingFixture);
const sessionRowStages =
  materializeSessionPluginRowScenario(sessionBindingFixture);
const uiContractFixtures = await readUiContractConformanceFixtures();
const applicationSurfaceId = metadata.application_primitives.surface_id;
const rendererEntryPoint = metadata.application_primitives.renderer_entrypoint;

console.log(
  metadata.protocol,
  metadata.conformance_fixture_revision,
  fixturePath,
  applicationPrimitivesPath,
  applicationSurfaceId,
  rendererEntryPoint,
  supportMatrix.required_features,
  lateAttachFixture.history_then_live,
  localWebrtcChunkFixture.scenarios.large_generated,
  modeFlagsFixture.mouse_on.mode_flags.mouse_mode,
  sessionLifecycleFixture.normalized_frames,
  sessionBindingStages,
  sessionRowStages,
  uiContractFixtures.contract_version,
);
```

Use this exact package spec in npm-based client repos:

```json
{
  "devDependencies": {
    "@trybotster/hub-test-support": "0.1.42"
  }
}
```

`@trybotster/hub-test-support@0.1.42` carries byte-faithful live
`terminal_output` payloads (`payload_base64`, `payload_encoding`, `bytes`)
and authentic dual GHOSTSNP late-attach fixtures (conformance revision 46).
History attach uses incremental READY, PAGE, and FINISH Snapshot frames.
No-history attach uses READY then FINISH. Import-visible state matches the
ReadScreen oracles; do not dual-use a history-bearing golden as no-history.
Protocol version is 7, including the live-output envelope, the
`mode_gated_input` feature, full `ModeFlags` freshness fields, explicit
session-type execution modes, spawn-point session-type listing
(`list_session_types_for_target`), and the session-type authoring view
(`show_session_type_definition`). First-party clients should pin this
coordinate when they use these contracts.

The support matrix is generated from the Rust compatibility descriptors.
`terminal_readback` appears in both `supported_features` and
`required_features`; downstream compatibility checks must implement it rather
than treating it as optional. The late-attach fixture is generated from the
Rust serde scenario and preserves attaching, READY, optional PAGE frames,
FINISH or `snapshot_history_incomplete`, attached, then live output. An opaque authoritative
snapshot may represent a blank terminal; clients must not infer visible
history from payload byte length. Only `read_screen_text` is renderable
restored content; `snapshot` and `scrollback` base64 payloads must never be
appended as terminal text. Version 0.1.6 / conformance revision 13 uses
superseded JSON number arrays, while version 0.1.5 / revision 12 exposes
lossy string history. Neither is current binary-history contract authority.

Version 0.1.42 carries protocol version 7 / conformance revision 46 with
advertised optional `unix_terminal_adapter`,
`terminal_subscription_closed`, `webrtc_terminal_adapter`,
`attach_occupancy`,
`package_event_subscriptions`,
negotiated WebRTC `daemon_event` close delivery, and
`snapshot_delivery=ready_then_history` support,
byte-faithful live `terminal_output` payloads, incremental GHOSTSNP
READY/PAGE/FINISH goldens, `mode_gated_input` ModeGatedInput +
ModeFlags freshness, Snapshot-only GHOSTSNP rules, and the
`DaemonSessionTypeExecution` contract. Version 0.1.35 is the prior published
coordinate at protocol 7 / revision 40. Protocol 7 Hello must require
`terminal_subscription_closed`
(`FEATURE_TERMINAL_SUBSCRIPTION_CLOSED` /
`DaemonCompatibilityRequirement::for_webrtc_terminal_subscription_closed()`)
before Hub sends `DaemonLocalWebrtcDeliveryKind` `daemon_event`. The feature
stays optional in default `required_features`. The contract
defines the explicit `relative_executable` and `shell_command` modes.
It also carries spawn-point session-type listing
(`list_session_types_for_target`) and the session-type authoring view
(`show_session_type_definition`, editable `session_type_definition` response
vocabulary, and the support-matrix `session_type_authoring` section including
the `read_only_session_type_source` refusal). It retains the generic
package-owned entity records and the `plugin_entity_subscriptions`
compatibility feature, authoritative session-type request/response and entity
records, and requires the `session_type_entity_subscriptions` feature. It
depends on `@trybotster/ui-contract@0.3.3` for the canonical UiNode,
UiActionRequest, UiActionResult, package surface, package navigation,
and package-owned client notice reaction declarations and conformance
fixtures. It also carries the optional
aggregate plugin-worker counters prepared in the unpublished 0.1.15 artifact.

Version 0.1.29 was the prior published coordinate (revision 34) and still
carried the non-GHOSTSNP placeholder history Snapshot bytes plus Snapshot-less
no-history sequence; it is not current late-attach contract authority.
Version 0.1.26 was an earlier published coordinate. It did not include the
explicit session-type execution contract.
Version 0.1.24 was an earlier published coordinate at protocol version 6 /
conformance revision 31 (authoritative software/install identity,
`CheckHubUpdate`, and the cold removal of package compatibility `hub_version`,
without the authoring-view request). Version 0.1.22 was the repository
baseline coordinate. Version 0.1.21 was an earlier published npm coordinate.
Version 0.1.17 was published with stale protocol bytes and is not a valid
consumer coordinate.
Revision 20 remains the already-published version 0.1.12 application-primitives
contract; revision 21 adds spawn-target `base_ref` and worktree `management`
without reusing those bytes; revision 22 makes the live presentation Dialog
form-operable without reusing either earlier revision.
Revision 23 adds required `DaemonSessionEntity.lifecycle_class`, the canonical
`/session` binding scenario, and the real `contract.sessions` plugin-worker
surface. Revision 24 removes the daemon-owned package-surface TypeScript mirror,
references `PackageSurfaceDescriptor` from `@trybotster/ui-contract`, and adds
explicit package navigation to the contract-matrix fixture.
Revision 25 makes only authored `UiNode.id` bindable inside a BindList item
template and adds the producer-backed `contract.sessions` multi-row Button
oracle. The row materializer resolves ids in producer order, binds each
matching action payload, rejects blank/non-string/duplicate identities, and
keeps action request/result `node_id` literal.
Revision 26 adds bound-root descendant keys, the canonical UTF-8 byte-length
identity helper, and `spawn`/`rename`/`remove` controls for every current row.
The Rust and Node materializers publish exact root/control IDs and payloads and
reject collisions with roots, static siblings, or other realized rows.
Revision 27 binds the Spawn Button's required label to
`@/lifecycle_class`. Both reference materializers resolve it to the selected
row's literal class and reject unresolved required values; the Rust path also
passes the materialized Button through strict realized UiNode validation.
The coordinated UI contract also closes earlier accidental authored acceptance
of sentinels on required fields outside the explicit seven-field allowlist;
0.3.2 is the intended prepared coordinate for that narrowing because the
distinct unpublished 0.3.0 bytes may already have been packed or consumed.
The Node reference materializer proves matching `current`, `ended`,
and `indeterminate` rows before an absent UUID selects the unavailable path,
then proves patch, remove, and authoritative reconnect convergence. This
package does not claim shipped botster-web or botster-tui renderer support;
those production paths are separately routed.
The protocol cold-switch replaces split action fields and untyped JSON bodies
with one canonical request envelope and typed surface/action response bodies.
It also retains the version 0.1.11
`refresh_local_packages` daemon request,
authenticated local-WebRTC delivery-kind fixture, and the
source-derived session lifecycle subscription fixture. Its normalized public
`DaemonEntityFrame` sequence is authoritative snapshot, spawn upsert,
lifecycle patches, and remove. A fresh connection discards prior-generation
frames and requires a new authoritative snapshot before accepting deltas.
Overflow recovery is an authoritative snapshot with
`resync_reason: "subscriber_overflow"`; a failed resync snapshot closes the
subscription instead of concealing a delta gap. Rust consumers can run
`run_session_lifecycle_subscription_conformance` against an `IsolatedHub` to
prove the same contract through the real Hub/Core/session-worker topology.
`run_plugin_contract_matrix_conformance` similarly proves rendered
open/form/toggle metadata through the real Hub/plugin worker, applies accepted
`set`/`clear`/`toggle` results to scoped client-shaped state, and materializes
the delivered tree against its Dialog presence and selected-workspace equality
bindings. It also renders and subscribes to the dotted fixture package's exact
encoded entity family twice, proving a fresh authoritative reconnect snapshot.
Revision 21 places the one canonical Form inside the active Dialog
and makes the browser-shaped consumer reject any actionable sibling Form.
Rejected results retain the visible Dialog, Form, input, and field-error
association; accepted results apply normalization and a whole-surface
replacement, then clear the scoped Dialog key. The published first-party
support matrix exposes the corresponding Dialog/Form/input composition,
whole-surface replacement scope, expected keys, values, and operation kinds to
TypeScript consumers; static
`@trybotster/ui-contract` fixtures remain complementary.

The mode-flags fixture covers the targeted `read_mode_flags` request/response
contract. It preserves exact authoritative mouse values (`0` for off and `9`
for combined tracking plus SGR reporting), attributes both successes to the
requested session, and records unknown-session and backend failures as
`operator_error` responses with no successful mode body. Mode flags are
readback-only; clients must not expect a pushed mode-change event.

Botster web and TUI renderers should consume
`metadata.application_primitives.surface_id` (`contract.app`) and render
`metadata.application_primitives.renderer_entrypoint` (`ui_tree_snapshot.body`).
The current core-validated primitive inventory is exposed as
`metadata.application_primitives.primitive_kinds`: `button`, `dialog`,
`empty_state`, `form`, `metric`, `metric_grid`, `panel`, `section`,
`status_badge`, `table`, `text`, `text_input`, and `toolbar`. The current core
contract fixture does not include
`list` or an `action_bar` alias; downstream clients should not invent those
shapes.

Client repos should update their lockfile from the registry coordinate or
packed tarball, then run a smoke that imports the package, reads the daemon
protocol artifact, calls `verifyPackageAssets()`, and materializes the
application-primitives fixture.

The normal consumer path is the declared npm dependency. Environment variables
such as `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` remain local override inputs for
older drift checks, not the package consumption path.
