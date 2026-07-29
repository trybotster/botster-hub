# Expose Canonical Session Lifecycle Projection to Plugin Surfaces — Implement Report

## Summary

The Hub now publishes a required, total `lifecycle_class` on canonical
`session` entity rows and admits plugin-authored absolute bindings only under
the `/session` family. The real `botster.plugin-contract-matrix` worker exposes
`contract.sessions`, which renders one exact-UUID `bind_list` per requested
session and binds its item to `@/lifecycle_class`.

The Hub-owned Rust and Node reference materializers prove current, ended,
indeterminate, missing, transition, removal, and authoritative reconnect
behavior. They are conformance references, not evidence that the shipped Web
or TUI renderer already supports the grammar.

## Routing and Guidance

- Target repository: `trybotster/botster-hub`
- Target ID: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Run worktree: the assigned pipeline worktree for
  `ticket_1785295607_887142` at base `35e92f4`
- Repository charter: `[[botster-hub-playbook]]`
- Role playbooks: `[[implementer-playbook]]` and
  `[[botster-implementer-playbook]]`
- Context: `self/identity.md`, `self/goals.md`, the approved plan, Hub
  architecture/CLI/SPA guidance, and targeted notes for Hub ownership,
  `HubRuntime`, plugin workers and surfaces, canonical session identity,
  binding grammar and hydration, client DTO ownership, protocol conformance
  revisions, generated TypeScript optionality, subscription snapshots,
  first-party support matrices, external consumer smoke tests, real worker
  boundary tests, and the repository test wrapper.
- `[[project-pipelines-playbook]]` was not loaded because no Project Pipelines
  product source, package path, or workflow policy is changed.

The approved plan and this run both resolve to the same target repository and
target ID.

## Assumptions and Contract Decisions

- `registry_state == "stale"` takes precedence over a concrete lifecycle and
  maps to `indeterminate`.
- Otherwise `starting`, `running`, and `stopping` map to `current`;
  `exited` and `failed` map to `ended`; and absent lifecycle maps to
  `indeterminate`.
- Only absence from an authoritative `/session` snapshot is unavailable.
  A present row missing the required class is a contract error.
- `/session` is the only absolute binding family admitted in this revision.
  Relative bindings remain valid. Owner-namespaced entity delivery is out of
  scope because the Hub cannot hydrate such a family today.
- Adding the required row field changes the public conformance fixture, so the
  conformance revision advances from 22 to 23. Protocol version 4 and the
  feature list remain unchanged because framing, request issuance, and the
  existing `session_entity_subscriptions` capability are unchanged.

## Files Changed

- `src/daemon_transport.rs` derives and publishes the total lifecycle class,
  including explicit patches and complete stale-first tests.
- `src/runtime.rs` enforces Hub-owned plugin-surface binding-family admission
  after generic `UiNode` validation for both initial renders and accepted
  action replacement trees.
- `src/local_webrtc.rs` boxes the now-larger entity-frame enum variant to keep
  the strict clippy size boundary green.
- `crates/botster-hub-client/src/lib.rs`,
  `crates/botster-hub-client/src/typescript.rs`, and
  `crates/botster-hub-client/generated/daemon-protocol.ts` add the required
  public DTO field and advance conformance revision 23.
- `fixtures/plugins/plugin-contract-matrix/**` adds the canonical
  `contract.sessions` worker surface; its test-support and npm fixture mirrors
  are generated from the same source.
- `crates/botster-hub-test-support/src/lib.rs` adds the support-matrix
  declaration, public-frame scenario, real-worker equality proof, and strict
  Rust reference materializer.
- `crates/botster-hub-test-support/examples/node_package_assets.rs` and
  `packages/hub-test-support/scripts/sync-assets.mjs` generate and synchronize
  the new fixture and all affected package metadata.
- `packages/hub-test-support/**` prepares immutable version `0.1.15`, exports
  the scenario and strict Node materializer, updates generated artifacts, and
  verifies packed-package behavior.
- `tests/hub_daemon_lifecycle_test.rs` proves real worker registration plus
  live current-to-ended and stale-to-indeterminate delivered entity paths.
- `README.md` and `docs/client-protocol.md` document the contract, compatibility
  posture, consumer limitations, and downstream routing.

## Ownership Boundaries

Hub remains the authority for lifecycle projection, plugin-worker execution,
privileged absolute-family admission, connection-scoped entity delivery, and
Hub-owned conformance artifacts. No workspace membership or product policy was
added. No Hub-local duplicate of the public session DTO was introduced.

No Web, TUI, TUI-kit, Core, or Workspaces source was changed. Shipped-client
hydration remains separately routed:

- Web: `ticket_1785298229_125024`,
  target `tgt_40abcf71ccf049f4ac0c99953a799869`
- TUI: `ticket_1785298229_854008`,
  target `tgt_c3d470bab78549df920a41e8fb0e58d8`
- Workspaces consumer: `ticket_1785296184_677408`, dependent on both client
  tickets

## Deviations from the Approved Plan

None. Boxing `PendingLocalWebrtcRequest::EntityFrame` is cleanup made necessary
by the required DTO field crossing the repository's strict large-enum clippy
threshold; it does not alter the transport contract.

## Verification and Downstream-Shaped Proof

- `./test.sh --locked`: passed; 103 live daemon tests passed and one documented
  large adversarial test remained intentionally ignored.
- `./test.sh --locked -p botster-hub-client`: passed, including generated
  TypeScript and DTO contract tests.
- Focused projection and Hub admission tests: passed.
- Real `contract.sessions` plugin-worker runtime proof: passed.
- Live entity subscription natural-exit, stale-row, and reconnect proofs:
  passed.
- A real worker-backed held-open entity subscription emitted the exact
  running-to-stale patch:
  `registry_state`, `lifecycle_class`, and `updated_at`, with the omitted
  optional `lifecycle` field retaining its prior concrete client value.
- Rust lifecycle binding reference materializer, including the matching-row
  negative control and malformed-present-row rejection: passed.
- `npm test --prefix packages/hub-test-support`: passed.
- `node packages/hub-test-support/scripts/sync-assets.mjs --check`: passed.
- Packed-tarball clean consumer proof: passed for version `0.1.15`, revision
  23, and all five materialization stages (`initial`,
  `after_ended_patch`, `after_indeterminate_patch`, `after_remove`,
  `after_reconnect`).
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`:
  passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed.

The full 35-test support-crate run exposed the pre-existing timing-sensitive
test `start_timeout_cleans_up_unready_child` failing to observe its fake PID
file when run concurrently; that function was not changed, and its focused
rerun passed. Earlier full support-crate attempts similarly exposed two
unmodified process-timing tests which also passed focused reruns. The
repository wrapper and all ticket-shaped paths are green.

Runtime provenance used the worktree-built
`target/debug/botster-hub` and `target/debug/botster-session-worker`; the Core
dependency is locked to
`e36435f2cb583c344d6f6ba2d62c39da324c7a64`.

## Package Artifact and Residual Risk

The last published coordinate is `@trybotster/hub-test-support@0.1.14`.
Version `0.1.15` was confirmed unused before packing and remained absent after
the publication attempt. The prepared tarball has:

- shasum: `f252eee3e50dd6b32e24c68fb6ceca3649852c54`
- integrity:
  `sha512-lIlVJ5E3hEVtk5VyqtroI2TBnLMfInkMWQbfyfCEA52V9xUa/zG29php0ONLiaU4JR5GkkBsEAVf0G3xgGXSbQ==`

`npm publish --access public` reached the registry but was rejected because an
authenticator OTP is required. An authorized operator can publish the committed
artifact with:

```sh
cd packages/hub-test-support
npm publish --access public
```

Until publication succeeds, downstream tickets must use a tarball produced
from the committed package source. Actual Web/TUI runtime rendering is
intentionally unverified here and remains owned by the separately routed
tickets above.

## Vault Guidance

Review exposed three existing targeted notes that were missing from the initial
Implement context:

- `[[pipeline artifacts should use path neutral worktree references]]`
- `[[botster review and verify must scan all committed artifacts for pii]]`
- `[[shared conformance fixtures that contradict the core contract teach
  clients the wrong state machine]]`

The report now uses path-neutral ticket/base-SHA wording, the raw full branch
diff and PR body pass an absolute-home-path scan with a known-positive control,
and the shared stale-transition frame is byte-shape faithful to the production
producer. No new vault note was captured because the applicable durable
guidance already exists.
