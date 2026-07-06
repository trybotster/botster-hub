# @trybotster/hub-test-support

Node-consumable Botster hub test-support assets for first-party web clients.

The package is a generated wrapper over `botster-hub-test-support` and
`botster-hub-client`. Do not edit `daemon-protocol.ts`,
`fixtures/plugin-contract-matrix`, or `metadata.json` by hand; run:

```sh
node packages/hub-test-support/scripts/sync-assets.mjs
```

## Usage

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

Use this exact package spec in npm-based client repos:

```json
{
  "devDependencies": {
    "@trybotster/hub-test-support": "0.1.0"
  }
}
```

`@trybotster/hub-test-support@0.1.0` is published to the public npm registry.
No scoped `.npmrc` entry or CI auth token is required for install. Client repos
should update their lockfile from that registry coordinate, then run a smoke
that imports the package, reads the daemon protocol artifact, verifies package
assets, and materializes the plugin contract matrix fixture.

The normal consumer path is the declared npm dependency. Environment variables
such as `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` remain local override inputs for
older drift checks, not the package consumption path.
