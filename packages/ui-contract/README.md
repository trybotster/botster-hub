# `@trybotster/ui-contract`

Renderer-neutral plugin UI contract generated from the authoritative
`botster-ui-contract` Rust crate in this repository.

The package ships serde-accurate TypeScript declarations, a JSON Schema, and
shared conformance fixtures. Alongside `UiNode` and action contracts it owns
the renderer-neutral package surface, supported-operation, and navigation
discoverability vocabulary. Hub manifests own admission and registry policy;
clients must not copy these descriptors into daemon-specific mirrors. It is a
normal build/protocol dependency for Botster clients, not an installable
marketplace package.

The host scopes presentation keys to the active Hub, package, and surface.
Clients own the scoped presentation store and renderer policy. Plugin workers
receive one canonical `UiActionRequest`: form drafts are in `values`, while
non-form action metadata remains in `payload`. Accepted results may apply
presentation operations and one validated inline replacement tree. Rejected,
deferred, and error results retain the current tree and presentation state.
Every result must echo the request's `request_id`, `surface_id`, `action_id`,
and `node_id` exactly. This includes preserving an absent `node_id`; the Hub
rejects mismatched result identity as `invalid_action_result`.

Regenerate or check committed assets:

```sh
npm run generate
npm run check
npm test
```

After the merged artifact is ready, publish manually:

```sh
cd packages/ui-contract
npm publish --access public
```
