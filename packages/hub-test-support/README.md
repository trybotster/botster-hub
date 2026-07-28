# @trybotster/hub-test-support

Node-consumable Botster hub test-support assets for first-party web clients.

The package is a generated wrapper over `botster-hub-test-support` and
`botster-hub-client`. Do not edit `daemon-protocol.ts`,
`first-party-client-support-matrix.json`,
`session-lifecycle-subscription-conformance-fixture.json`,
`late-attach-history-conformance-fixture.json`,
`local-webrtc-delivery-chunk-conformance-fixture.json`,
`mode-flags-conformance-fixture.json`,
`fixtures/plugin-contract-matrix`, or `metadata.json` by hand; run:

```sh
node packages/hub-test-support/scripts/sync-assets.mjs
```

## Usage

```sh
npm install --save-dev @trybotster/ui-contract@0.1.0 @trybotster/hub-test-support@0.1.13
```

```js
import {
  materializeApplicationPrimitivesFixture,
  materializePluginContractMatrixFixture,
  metadata,
  readDaemonProtocolTypescript,
  readFirstPartyClientSupportMatrix,
  readLateAttachHistoryConformanceFixture,
  readLocalWebrtcDeliveryChunkConformanceFixture,
  readModeFlagsConformanceFixture,
  readSessionLifecycleSubscriptionConformanceFixture,
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
  uiContractFixtures.contract_version,
);
```

Use this exact package spec in npm-based client repos:

```json
{
  "devDependencies": {
    "@trybotster/hub-test-support": "0.1.13"
  }
}
```

After `@trybotster/ui-contract@0.1.0` and
`@trybotster/hub-test-support@0.1.13` are published to the public npm
registry, no scoped `.npmrc` entry or CI auth token is required for install.

The support matrix is generated from the Rust compatibility descriptors.
`terminal_readback` appears in both `supported_features` and
`required_features`; downstream compatibility checks must implement it rather
than treating it as optional. The late-attach fixture is generated from the
Rust serde scenario and preserves `attaching -> optional initial state ->
attached -> live` ordering. An opaque authoritative snapshot may represent a
blank terminal; clients must not infer visible history from payload byte length.
Only `read_screen_text` is renderable restored content; `snapshot` and
`scrollback` base64 payloads must never be appended as terminal text. Version
0.1.6 / conformance revision 13 uses superseded JSON number arrays, while
version 0.1.5 / revision 12 exposes lossy string history. Neither is current
binary-history contract authority.

Version 0.1.13 publishes protocol version 4 / conformance revision 21 and
depends on `@trybotster/ui-contract@0.1.0` for the canonical UiNode,
UiActionRequest, and UiActionResult declarations and conformance fixtures.
Revision 20 remains the already-published version 0.1.12 application-primitives
contract; revision 21 adds spawn-target `base_ref` and worktree `management`
without reusing those bytes.
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
`set`/`clear`/`toggle` results to scoped client-shaped state, and evaluates the
delivered Dialog presence and selected-workspace equality bindings. The
published first-party support matrix exposes the corresponding expected keys,
values, and operation kinds to TypeScript consumers; static
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
`metadata.application_primitives.primitive_kinds`: `button`, `empty_state`,
`form`, `metric`, `metric_grid`, `panel`, `section`, `status_badge`, `table`,
`text_input`, and `toolbar`. The current core contract fixture does not include
`list` or an `action_bar` alias; downstream clients should not invent those
shapes.

Client repos should update their lockfile from the registry coordinate or
packed tarball, then run a smoke that imports the package, reads the daemon
protocol artifact, calls `verifyPackageAssets()`, and materializes the
application-primitives fixture.

The normal consumer path is the declared npm dependency. Environment variables
such as `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` remain local override inputs for
older drift checks, not the package consumption path.
