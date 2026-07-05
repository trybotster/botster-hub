# Plugin Contract Matrix Fixture

This is the canonical hub-owned fixture package for exercising public Botster package and plugin contracts. It is intentionally PII-free and uses only `example.invalid` fixture values.

Client repos can install this directory as a local package and use the real daemon protocol to verify conformance:

```text
botster-hub packages install fixtures/plugins/plugin-contract-matrix
botster-hub packages enable botster.plugin-contract-matrix
```

## Matrix

- `contract.app`: app surface returning a concrete UiNode payload through `plugin_surface_render`.
- `contract.empty`: placeholder app surface returning a valid empty-state UiNode payload.
- `contract.blocked`: render handler that fails deliberately so clients can assert the daemon `operator_error` response and continued daemon responsiveness.
- `contract.settings`: settings surface returning sanitized effective configuration from `botster.capabilities.config.get()`.
- Configuration schema: `endpoint` URL default, `mode` select default and validation options, and redacted `api_token` secret.
- `contract.action`: `plugin_surface_action` handler with accepted and error states selected by the request payload.
- Package route descriptors: manifest `surfaces` should project to `surface:<id>` routes under `/packages/botster.plugin-contract-matrix/surfaces/<id>`.
- Package lifecycle compatibility: hub conformance should prove install, enable, list, show, route descriptors, and action-state projection through the daemon package DTOs. The installed `DaemonPackage` row currently does not expose a separate protocol compatibility descriptor; that remains covered by package admission and lifecycle state.
