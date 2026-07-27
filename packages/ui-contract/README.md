# `@trybotster/ui-contract`

Renderer-neutral plugin UI contract generated from the authoritative
`botster-ui-contract` Rust crate in this repository.

The package ships serde-accurate TypeScript declarations, a JSON Schema, and
shared conformance fixtures. It is a normal build/protocol dependency for
Botster clients, not an installable marketplace package.

The host scopes presentation keys to the active Hub, package, and surface.
Clients own the scoped presentation store and renderer policy. Plugin workers
receive one canonical `UiActionRequest`: form drafts are in `values`, while
non-form action metadata remains in `payload`. Accepted results may apply
presentation operations and one validated inline replacement tree. Rejected,
deferred, and error results retain the current tree and presentation state.

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
