# Publish Hub Test-Support Application-Primitives Fixture Implement Report

## Summary

Published an explicit application-primitives consumer surface from
`@trybotster/hub-test-support@0.1.2` without changing the underlying
core-validated fixture. The new Node API and metadata are aliases over the
existing `plugin-contract-matrix` package and its `contract.app` surface.

## Assumptions

- `contract.app` in `botster.plugin-contract-matrix` is the authoritative
  application-primitives composite for this ticket.
- `toolbar` is the current core contract spelling. No `action_bar` alias was
  added.
- `list` and `form` are absent from the current core-validated composite and
  are out of scope for this hub package publication.
- Adding the npm package API and metadata alias is an observable package export
  change, so the npm package version was bumped to `0.1.2`.
- `CONFORMANCE_FIXTURE_REVISION` stays at 8 because the daemon conformance
  surface, fixture bytes, protocol DTOs, and validated UiNode payload did not
  change.

## Files Changed

- `crates/botster-hub-test-support/src/lib.rs` publishes an
  `application_primitives_fixture_descriptor()` and reuses its exact node-kind
  list in runtime conformance.
- `crates/botster-hub-test-support/examples/node_package_assets.rs` emits the
  application-primitives descriptor into generated package metadata.
- `packages/hub-test-support/index.js` and `index.d.ts` expose
  `applicationPrimitivesFixturePath()` and
  `materializeApplicationPrimitivesFixture(destination)`.
- `packages/hub-test-support/scripts/sync-assets.mjs` carries the Rust-emitted
  descriptor into `metadata.json`.
- `packages/hub-test-support/metadata.json` records package version `0.1.2`,
  `contract.app`, `ui_tree_snapshot.body`, and the current primitive inventory
  derived from the render-checked node-kind descriptor.
- `packages/hub-test-support/package.json` bumps the package to `0.1.2`.
- `packages/hub-test-support/test.mjs` verifies the explicit application
  primitives API and metadata through the public package import.
- `packages/hub-test-support/README.md` and `docs/client-protocol.md` document
  the exact consumer dependency, API, surface id, renderer entrypoint, and
  tarball fallback.
- `docs/plans/publish-hub-test-support-application-primitives-fixture-for-web-consumers.md`
  is the approved plan artifact carried from the planning step.

## Consumer Instructions

Use this exact dev dependency when the package is published:

```json
{
  "devDependencies": {
    "@trybotster/hub-test-support": "0.1.2"
  }
}
```

Use this import path and API:

```js
import {
  materializeApplicationPrimitivesFixture,
  metadata,
} from "@trybotster/hub-test-support";

const fixturePath = materializeApplicationPrimitivesFixture(tempDirectory);
const surfaceId = metadata.application_primitives.surface_id; // contract.app
const rendererEntryPoint = metadata.application_primitives.renderer_entrypoint; // ui_tree_snapshot.body
```

If `@trybotster/hub-test-support@0.1.2` is not yet published, use the tarball
created by `npm pack` from `packages/hub-test-support` as a `file:` dependency
and keep the same import/API names.

The durable fallback route for downstream Project Pipelines work is the
reproducible `npm pack` command from committed `packages/hub-test-support`
source. Any tarball path under `/tmp` is only local verification output from
this implementation run, not the handoff coordinate for botster-web.

## Verification

Verification commands and external consumer proof are recorded in the implement
gate evidence.

## Residual Risk

Actual npm publication depends on registry credentials. The implementation is
designed to support a packed tarball fallback when publication is unavailable.
