---
description: Plan for auditing hub transport admission and key storage before E2E WebRTC implementation.
---

# Audit Hub Transport Admission and Key Storage for E2E WebRTC

## Context Loaded

- Pipeline context: ticket `ticket_1782857256_894278`, run `run_1782857310_440658`, current step `botster_plan`, target `tgt_7e208a0c76a44980a83b63af976b1f22`; no prior artifacts, findings, reviews, questions, or answers were present.
- Required plan playbooks: [[planner-playbook]] and [[botster-planner-playbook]].
- Required Botster overlays: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], and [[botster orchestration prompts must bind agents to explicit worktrees]].
- Additional review-required vault context: [[botster core owns reusable crypto and identity mechanisms]], [[botster-core local process runtime is feature-gated from contract-only embeds]], [[botster browser pull requests must retry after webrtc reconnect]], [[botster webrtc request consumers should use operation gates not connection checks]], [[botster web pairing ui lives only in the share modal]], and [[botster web dogfood bridge ownership modes are explicit]].
- Self context loaded as [[identity]] and [[goals]].
- Repo inspection during planning covered `src/daemon_transport.rs`, `src/client_api.rs`, `src/auth.rs`, `src/config.rs`, `src/persistence.rs`, `src/capabilities.rs`, `src/entrypoint_supervisor.rs`, `src/main.rs`, `crates/botster-hub-client/src/lib.rs`, `docs/client-protocol.md`, existing `docs/plans/*`, existing `docs/reports/*`, and dogfood/bridge tests in `tests/hub_daemon_lifecycle_test.rs`.
- Plan Review context loaded after return to Plan: four findings from `review_1782857757_312675` plus answered agent question `question_1782857659_902105` requiring one ordered/reliable DataChannel and no application-layer reordering.

## Scope

- Produce a hub-side audit report, preferably `docs/reports/audit-hub-transport-admission-and-key-storage-for-e2e-webrtc.md`, that inventories every currently visible hub client transport and classifies each as control plane, data plane, dev harness, or production candidate.
- Confirm the concrete same-device production path today:
  - `botster-hub-client` daemon protocol frames over the local Unix socket.
  - CLI/TUI/package-app operations entering through `src/daemon_transport.rs`.
  - Runtime terminal attach/input/resize/drain flowing from `HubClientApi` into `HubRuntime` and core `SessionIo`/`ClientWorker` paths, not a hub-owned byte relay.
- Confirm current package/browser dev behavior:
  - `botster-web` is launched as a supervised local package `runnable_entrypoint` through `StartPackageEntrypoint`.
  - `dogfood` and `dev-stack bootstrap` pass local bridge settings into the supervised entrypoint and verify a `local_url` through `ListApps`.
  - The HTTP/SSE-style local bridge is a dev harness/package app surface, not production browser WebRTC.
- Confirm current network surfaces separately from browser transport:
  - `src/config.rs` has TCP transport binding config shape but no observed TCP listener production path in this repo.
  - `src/capabilities.rs` exposes plugin HTTP and in-memory WebSocket capability runtimes for admitted plugins; these are plugin capability surfaces, not client browser transport.
- Audit key storage support for:
  - long-lived hub identity;
  - per-browser trusted device identities;
  - short-lived local bootstrap grants;
  - file encryption keys.
- Define ticket-ready hub policy for browser admission:
  - local installed `botster-web` gets a short-lived localhost bootstrap grant or equivalent no-QR ceremony;
  - remote web requires QR/device approval/account-mediated approval;
  - both converge onto the same encrypted Botster stream after admission.
- Define the implementation-facing hub transport API for later work:
  - local signaling endpoints;
  - offer/answer/ICE lifecycle;
  - one ordered/reliable DataChannel attach into the existing core client stream;
  - reconnect behavior;
  - diagnostics;
  - package/web app settings exposure.
- Define transport ordering as a fixed constraint: WebRTC DataChannel ordering is authoritative; Botster sequence/transcript fields are for integrity, replay/duplicate/drop detection, reconnect diagnostics, and crypto transcript binding only.
- Add a small non-invasive guard only if there is a stable in-repo assertion point. Good candidates are a doc/protocol assertion or a focused Rust test that keeps dogfood/dev-stack bridge output or docs labeled as dev harness rather than production browser transport.

## Non-Scope

- Do not implement WebRTC, DTLS/SRTP, DataChannel framing, TURN/STUN, remote signaling, QR pairing, device approval UI, account mediation, cloud signaling, or file encryption.
- Do not edit the separate `botster-web` repository or any old monolith.
- Do not introduce new crypto/keychain dependencies in this audit unless a tiny compile-only hook is required to document an existing API; the expected output is the gap/owner report.
- Do not add optional configurability, broad transport abstractions, alternate daemon protocols, or package admission redesign.
- Do not treat plugin WebSocket capability runtime, TCP config structs, or the HTTP bridge fixture as production browser transport.
- Do not design app-level reordering, resequencing queues, holdback timers, or parallel ordering machinery over WebRTC. The first slice must use exactly one ordered/reliable DataChannel per Botster client stream with `ordered: true` and without `maxRetransmits` or `maxPacketLifeTime`.

## Assumptions And Unknowns

- Assumption: the implementation branch should create a reviewable repo artifact in `docs/reports/` and may update `docs/client-protocol.md` only where the current public docs could mislead clients about production browser transport.
- Assumption: the target/worktree from the pipeline context is authoritative for this run; implementers should not use ambient checkouts or sibling web repos.
- Assumption: "E2E WebRTC" means preserving the same encrypted Botster stream semantics for local and remote browser clients after admission, with only the admission ceremony differing.
- Assumption from repo inspection: hub identity is currently config metadata (`HostIdentity { id, display_name, fingerprint }`) persisted in `hub-state.json`, not a private-key-backed durable identity.
- Assumption from repo inspection: package configuration secrets are redacted markers in the daemon protocol and persisted state, not OS keychain-backed raw secret storage.
- Assumption from vault and Plan Review: `botster-core` already owns reusable credential/crypto/identity mechanisms. `Cargo.lock` pins `botster-core` to `42538009bc6f6291872c5657bedbe7370f504f8d`; the audit should confirm `identity/keyring.rs` `CredentialRecord` + `CredentialStore`, `identity/crypto.rs` `AesGcmKey`, and `identity/device.rs` `device_fingerprint()`/`verify_device_fingerprint()` from the cargo git checkout or `cargo doc -p botster-core`.
- Assumption: key storage sufficiency is a two-part verdict, not a single yes/no:
  - core mechanism present: yes, expected from the locked core revision and [[botster core owns reusable crypto and identity mechanisms]];
  - hub persistence/policy wired: no, expected gap in this crate unless implementation finds current `CredentialStore` wiring.
- Assumption: follow-up ownership should be hub/CLI/provider wiring of existing core primitives: OS keychain or file fallback provider, signing-key persistence, trusted-browser identity persistence, short-lived bootstrap-grant store with expiry/revocation, and file-key policy. Do not file "create core credential primitives" unless the locked dependency inspection disproves Plan Review's finding.
- Unknown: the eventual remote approval authority: QR-only local trust, cloud/account-mediated trust, or both. The audit should define the hub-side policy seam without implementing the cloud provider.
- Unknown: exact local signaling URL shape. Prefer narrow hub-owned localhost endpoints scoped to one short-lived grant and one peer/session route; do not make a generic web server surface without a concrete implementation ticket.

## Botster Layers Touched

- Rust hub: daemon socket transport, hub config/persistence, package app supervision, admission/auth hooks, capability runtime inventory.
- Client protocol crate: only for documenting or testing daemon/client DTO classifications if needed.
- Tests: Rust unit/integration tests only if the implementer adds a non-invasive guard.
- Docs: primary deliverable is a report; optional protocol doc update if current docs need a warning.
- Not touched: browser SPA/web repo, Rails/cloud, TUI UI behavior, Lua workflow policy, MCP server behavior except as an inventoried existing control plane.

## Affected Surfaces And Files

- `docs/reports/audit-hub-transport-admission-and-key-storage-for-e2e-webrtc.md`: new audit report.
- `docs/client-protocol.md`: optional clarification that `botster-web` local bridge/dogfood paths are dev harnesses and that production browser WebRTC is intentionally absent.
- `src/daemon_transport.rs`: inventory same-device Unix socket handling, daemon request routing, app launch, attach subscription cleanup, and any optional guard if warranted.
- `crates/botster-hub-client/src/lib.rs`: inventory daemon protocol request/response types and handshake; optional tests if classification constants or docs are added.
- `src/client_api.rs`: inventory transport-neutral local client API, `HubClientAdmission`, and current local-operator/unadmitted policy.
- `src/config.rs`: inventory `TransportBindings`, local socket default, and unwired TCP config risk.
- `src/persistence.rs`: inventory `hub-state.json`, host identity metadata, package registry, secret redaction persistence, and absence of keychain-backed secret material.
- `src/auth.rs`: inventory current auth hook seam and absence of concrete admission/auth flows.
- `src/capabilities.rs`: inventory plugin HTTP/WebSocket capability runtimes separately from browser transport.
- `src/main.rs`: inventory `dogfood` and `dev-stack bootstrap` package-entrypoint launch behavior and bridge URL printing.
- `src/entrypoint_supervisor.rs`: inventory supervised package launch, environment injection, and structured `local_url` result path.
- `tests/hub_daemon_lifecycle_test.rs`: optional guard around dev bridge semantics if the implementation finds a stable assertion point.

## Proposed Implementation Steps

1. Re-run the targeted inventory on the implementation branch:
   - `rg -n "WebRTC|webrtc|DataChannel|offer|answer|ICE|SSE|EventSource|WebSocket|websocket|bridge|local_url|StartPackageEntrypoint|ListApps|ResolveAppLaunch|keychain|keyring|credential|secret|identity|encrypt|bootstrap|admission|TransportBindings" src crates docs tests examples Cargo.toml`
   - `rg -n "enum DaemonRequest|PROTOCOL|stream_attach|DaemonConnection|ListApps|StartPackageEntrypoint" crates/botster-hub-client/src/lib.rs src/daemon_transport.rs docs/client-protocol.md`
   - Inspect the pinned `botster-core` dependency at `42538009bc6f6291872c5657bedbe7370f504f8d`, either by reading the cargo git checkout or with `cargo doc -p botster-core`, and confirm `CredentialStore`, `CredentialRecord`, `AesGcmKey`, `device_fingerprint()`, and `verify_device_fingerprint()`.
2. Write the report with a transport matrix:
   - Unix socket daemon protocol: production same-device control plane, terminal attach adapter/data-plane ingress into core client stream.
   - CLI commands: operator control plane over daemon socket.
   - TUI attach/open flows: same-device client over hub protocol, not session-worker protocol.
   - Package app launch: supervised local/dev app process contract, not browser transport by itself.
   - `botster-web` local bridge: dev harness/package app HTTP endpoint over localhost, not production browser transport.
   - Plugin HTTP/WebSocket capability runtimes: admitted plugin outbound/network capability surface, not browser client transport.
   - TCP binding config: config shape only unless implementation finds a live listener.
   - MCP: coordination/control plane, not terminal/browser data plane.
3. Write the key storage section:
   - State what exists: config/persistence host identity metadata, package config secret redaction markers, hub audit records, package registry state.
   - State the owner split from [[botster core owns reusable crypto and identity mechanisms]]: core owns reusable credential/crypto/identity mechanisms and shape; hub/CLI/provider layers own concrete persistence, prompts, registration, and runtime policy.
   - State the feature-boundary check from [[botster-core local process runtime is feature-gated from contract-only embeds]]: do not confuse feature visibility or contract-only embedding with absence of the core identity/credential contracts.
   - State what is missing or not wired: OS keychain/keyring-backed long-lived hub private identity, per-browser trusted device identities, short-lived local bootstrap grant store with expiry/revocation, and file encryption key storage.
   - Document the exact core APIs found and that hub has not wired them; follow-up tickets should wire existing core `CredentialStore`/identity primitives into hub keychain/file persistence and grant storage rather than creating new core primitives.
4. Define the local/remote admission policy:
   - Local installed package app may request a short-lived localhost bootstrap grant scoped to one hub identity, app instance, origin, and expiry.
   - Remote web must require QR/device approval/account-mediated approval before receiving equivalent trusted browser identity. Keep pairing UI ownership aligned with [[botster web pairing ui lives only in the share modal]].
   - Admission produces or unlocks browser identity and session keys; post-admission stream framing must be the same for local and remote.
   - Deny treating loopback origin or installed package status alone as long-lived trust.
5. Define the later hub transport API ticket:
   - Add hub-owned local signaling endpoint under a narrow localhost listener or existing package bridge only as a bootstrap/signaling adapter.
   - Offer/answer/ICE state is keyed by grant/peer id and bounded by timeout.
   - DataChannel adapter connects one ordered/reliable DataChannel to the existing `HubClientApi`/core client stream with `ClientWorker`/`SessionIo` ownership preserved. Use `ordered: true`; do not set `maxRetransmits` or `maxPacketLifeTime` in the first slice.
   - Botster-level sequence/transcript fields may support replay/duplicate/drop detection, reconnect high-water marks, and E2E crypto transcript validation. They must detect and abort on anomalies; they must not reorder or buffer ordinary DataChannel delivery.
   - Reconnect must probe peer/data-channel liveness and replay browser pulls/subscriptions in line with [[botster browser pull requests must retry after webrtc reconnect]].
   - Request consumers should enter through operation gates in line with [[botster webrtc request consumers should use operation gates not connection checks]] rather than page-specific connection checks.
   - Diagnostics must distinguish no grant, expired grant, signaling failure, ICE failure, DataChannel closed, core attach failure, encryption/auth failure, and sequence/transcript anomaly.
   - Package/web settings expose only safe local bootstrap status and pairing actions, not raw socket paths, key material, local data dirs, or private identity.
6. Add a small guard only if it stays surgical:
   - Prefer a test asserting dogfood/dev-stack bridge output remains labeled `bridge=`/`web=` as local dev harness and no docs/API claim it is production WebRTC. Keep bridge ownership language aligned with [[botster web dogfood bridge ownership modes are explicit]].
   - Add a parallel docs/test assertion where feasible that production browser transport requires a single ordered/reliable DataChannel and that no app-level reorder buffer, resequencing queue, or holdback timer is introduced.
   - If a reliable code-level guard would require new protocol vocabulary or web repo changes, skip it and document why in the report.

## Risks

- Overclaiming current browser support: the local bridge and test fixture can prove hub/package wiring, not production E2E browser WebRTC.
- Misassigning key-storage ownership: core credential/crypto primitives are already present per vault and locked dependency review; the audit must not file duplicate core-primitive work when the real gap is hub/CLI/provider persistence and policy wiring.
- Feature-boundary confusion: a missing import from this hub crate or a contract-only feature configuration is not proof that core lacks identity/credential mechanisms.
- Wrong data-plane ownership: future WebRTC must attach through the shared client/session worker path, not introduce a hub polling/relay path for PTY bytes.
- Wrong ordering ownership: future WebRTC must rely on ordered/reliable DataChannel delivery, not app-level reorder buffers or holdback queues.
- Trust-policy shortcut: local installed package status or localhost alone is not a durable browser identity. The local path needs a short-lived grant and the same encrypted stream after admission.
- Secret leakage: reports and tests must not include raw socket paths, local home paths, private keys, grants, tokens, or PII.
- Scope creep: implementing a web server, QR UI, crypto store, or DataChannel stack in this ticket would violate the audit intent.
- Stale docs risk: if `docs/client-protocol.md` says "browser-local bridge" without a dev-only caveat, clients may treat the bridge as production candidate.

## Acceptance Checks And Tests

- Report exists and names every existing hub transport path found in this repo, with classification as control plane, data plane, dev harness, or production candidate.
- Report states whether keychain/key storage APIs are sufficient using a two-part rubric:
  - Core mechanism present: confirm `CredentialStore`, `CredentialRecord`, `AesGcmKey`, `device_fingerprint()`, and `verify_device_fingerprint()` in pinned `botster-core` revision `42538009bc6f6291872c5657bedbe7370f504f8d`.
  - Hub persistence/policy wired: confirm whether this hub crate wires those primitives into OS keychain/file fallback, signing-key persistence, per-browser trusted identities, local bootstrap grants with expiry/revocation, and file-key storage.
- If hub persistence/policy is insufficient, follow-up owners/tickets must be scoped to wiring existing core primitives into hub/CLI/provider storage and admission policy. Do not propose new core credential primitives unless dependency inspection contradicts the reviewed core API evidence.
- Report includes a concrete implementation plan for hub-side local WebRTC signaling and E2E admission, with local and remote admission differing only before the shared encrypted stream.
- Report includes a `Transport ordering` subsection: one ordered/reliable DataChannel is authoritative; sequence/transcript fields are for integrity, replay/drop detection, reconnect, diagnostics, and crypto transcript binding only.
- Review rejects any plan/report or implementation-ticket recommendation that introduces a reorder buffer, holdback queue, or treats Botster sequence fields as a delivery-ordering mechanism.
- Report proves the production entry points it describes:
  - daemon socket path: `botster-hub-client` -> `src/daemon_transport.rs` -> `HubClientApi` -> `HubRuntime`;
  - package app path: `StartPackageEntrypoint`/`ListApps` -> `EntrypointSupervisor` -> structured `local_url`;
  - terminal data path: client attach/input/resize/drain enters existing core runtime/session-worker abstractions.
- If code/docs guards are added, run the narrow tests:
  - `./test.sh --test hub_daemon_lifecycle_test <new_or_existing_bridge_guard_test> -- --test-threads=1`
  - relevant unit test for any added classification helper.
- Always run or justify skipping:
  - `./test.sh --test hub_client_api_test`
  - `./test.sh --test hub_daemon_lifecycle_test cli_dogfood_launcher_bridge_request_endpoint_uses_same_daemon_state -- --test-threads=1` if bridge semantics are touched.
- No web repo edits, no old monolith edits, no PII in report or test fixtures.

## Pipeline Gates And Artifacts

- Plan artifact: `docs/plans/audit-hub-transport-admission-and-key-storage-for-e2e-webrtc.md`.
- Implementation artifact: `docs/reports/audit-hub-transport-admission-and-key-storage-for-e2e-webrtc.md`.
- Implement gate must include:
  - report path;
  - transport matrix summary;
  - two-part key storage sufficiency verdict, separating core mechanism presence from hub persistence/policy wiring;
  - follow-up ticket/owner list for missing hub keychain/key storage wiring over existing core primitives;
  - transport ordering decision and confirmation that no app-level reordering is proposed;
  - exact commands run or reason skipped;
  - statement that no WebRTC implementation and no web repo edits were made.
- Review must reject any implementation that only asserts code exists without tracing the runtime/user entry path.

## Vault Gaps Worth Capturing

- Capture a durable note if the audit settles the hub/browser admission policy vocabulary: "local bootstrap grant" vs "remote device approval" while preserving one encrypted stream.
- Capture a durable note if the audit identifies the final hub wiring pattern for credential storage over existing core primitives: OS keychain/file fallback provider, signing-key persistence, browser identity persistence, grant expiry/revocation, and file-key storage.
- Capture a durable note if `TransportBindings.tcp` is intentionally scaffold-only, because future planners will otherwise rediscover the same config-vs-listener ambiguity.
- Capture a durable note if a stable rule emerges that the dogfood HTTP bridge is a dev harness and must not be described as production browser transport.
- Capture a durable note if the single ordered/reliable DataChannel plus detection-only sequence/transcript rule becomes a reusable Botster WebRTC planning constraint.

## Checklist Evidence

- Vault/context evidence: notes listed in `Context Loaded` constrained the plan to Botster Rust hub/client/session-worker/package boundaries, repo-visible artifacts, explicit target/worktree assumptions, existing core crypto/identity mechanisms, browser reconnect/request-gate conventions, pairing UI ownership, dogfood bridge ownership, and no broad abstractions.
- Convention conflicts: none found. The plan follows the Botster playbook by keeping product workflow policy out of core, preserving hub/data-plane boundaries, and using docs/tests instead of implementing speculative WebRTC.
- Verification evidence gathered during planning: repository inspection found the local daemon socket in `src/daemon_transport.rs`, transport-neutral `HubClientApi` admission in `src/client_api.rs`, metadata-only host identity in `src/config.rs`/`src/persistence.rs`, package secret redaction tests in `src/persistence.rs`/`src/packages.rs`, plugin HTTP/WebSocket capability runtimes in `src/capabilities.rs`, supervised package bridge launch in `src/main.rs`/`src/entrypoint_supervisor.rs`, bridge fixture coverage in `tests/hub_daemon_lifecycle_test.rs`, and `Cargo.lock` pinning `botster-core` to `42538009bc6f6291872c5657bedbe7370f504f8d`.
- Capture evidence: no vault capture during planning; capture after implementation if the audit produces durable policy/owner decisions listed above.
