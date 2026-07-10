# @trybotster/hub-test-support

Node-consumable Botster hub test-support assets for first-party web clients.

The package is a generated wrapper over `botster-hub-test-support` and
`botster-hub-client`. Do not edit `daemon-protocol.ts`,
`first-party-client-support-matrix.json`,
`late-attach-history-conformance-fixture.json`,
`fixtures/plugin-contract-matrix`, or `metadata.json` by hand; run:

```sh
node packages/hub-test-support/scripts/sync-assets.mjs
```

## Usage

```sh
npm install --save-dev @trybotster/hub-test-support@0.1.3
```

```js
import {
  materializeApplicationPrimitivesFixture,
  materializePluginContractMatrixFixture,
  metadata,
  readDaemonProtocolTypescript,
  readFirstPartyClientSupportMatrix,
  readLateAttachHistoryConformanceFixture,
} from "@trybotster/hub-test-support";

const protocolSource = readDaemonProtocolTypescript();
const fixturePath = materializePluginContractMatrixFixture(tempDirectory);
const applicationPrimitivesPath = materializeApplicationPrimitivesFixture(tempDirectory);
const supportMatrix = readFirstPartyClientSupportMatrix();
const lateAttachFixture = readLateAttachHistoryConformanceFixture();
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
);
```

Use this exact package spec in npm-based client repos:

```json
{
  "devDependencies": {
    "@trybotster/hub-test-support": "0.1.3"
  }
}
```

`@trybotster/hub-test-support@0.1.3` is published to the public npm registry,
so no scoped `.npmrc` entry or CI auth token is required for install.

The support matrix is generated from the Rust compatibility descriptors. In
0.1.3, `terminal_readback` appears in both `supported_features` and
`required_features`; downstream compatibility checks must implement it rather
than treating it as optional. The late-attach fixture is generated from the
Rust serde scenario and preserves restored-history-before-live ordering and the
no-history case.

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
