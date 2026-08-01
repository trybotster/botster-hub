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

`UiNode.id` is authored identity. The direct `UiBindList.item_template` root
may retain the 0.2.0 item-relative `{ "$bind": "@/field" }` form. Identity-
bearing descendants below that bound root may use
`{ "$kind": "bind_list_descendant_id", "key": "remove" }`. Keys are
nonblank, preserve their exact UTF-8 bytes, and are unique across the complete
authored item template. The canonical helper realizes them as
`botster-ui-descendant-v1:<row-bytes>:<row><key-bytes>:<key>`; consumers must
call the exported helper rather than synthesize IDs locally.

Clients filter rows, resolve the direct root, then realize descendants before
any identity enters renderer, focus, hit, or action state. The new keyed form
is invalid on the item root, outside a bound item template, under
`empty_template`, or below a literal/absent root. Descendant full-ID `$bind`
remains invalid. Action request/result `node_id` remains a literal `UiNodeId`.

Required field binding is explicit rather than inferred from arbitrary JSON.
Authored validation accepts a structurally valid `UiBind` for
`Button.label`, `IconButton.label`, `MenuItem.label`, `Form.submit_label`,
`Iframe.src`, `Iframe.title`, and `Text.text`. The first six fields otherwise
require a nonblank string. `Text.text` retains its existing presence-only
literal contract, including empty strings, numbers, and null; only its authored
binding sentinel receives structural validation. Other required fields remain
non-bindable. Version 0.3.1 intentionally closes an earlier permissive gap:
required fields outside this seven-field allowlist now reject a binding
sentinel that an earlier authored validator could admit accidentally.

`UiNode.validate()` and `validate_ui_node()` are compatible authored-tree
entry points. Rust consumers must call `UiNode.validate_realized()` or
`validate_ui_node_realized()` after materialization. Realized validation
rejects unresolved property, payload, list, conditional, and identity bindings
while applying each field's existing literal rules. Hub performs authored
admission and transport only; renderer clients own materialization and the
realized validation boundary.

Rust renderers that also validate capability fallbacks can call
`validate_ui_node_realized_with_capabilities()` or
`UiCapabilitySet.validate_realized_node()` for the combined realized-tree and
capability traversal. The npm package publishes DTO declarations, schema, and
fixtures but does not export a JavaScript runtime validator; non-Rust renderers
must enforce the equivalent post-materialization boundary in their own runtime.

The generated JSON Schema can describe the literal-or-binding wire union, but
cannot express the complete BindList row context or template-global key
uniqueness. Schema validity is therefore necessary but not sufficient; the
Rust/Hub validator is authoritative.

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
