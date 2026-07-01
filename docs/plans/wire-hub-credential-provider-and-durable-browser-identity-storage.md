---
description: Plan for wiring hub credential provider policy and durable trusted browser identity storage.
---

# Wire Hub Credential Provider And Durable Browser Identity Storage

## Context Loaded

- Pipeline context: ticket `ticket_1782861299_366594`, run `run_1782862680_886606`, current step `botster_plan`, gate `botster_plan_gate`, target `tgt_7e208a0c76a44980a83b63af976b1f22`; no prior artifacts, findings, reviews, open questions, or answers were present.
- Required playbooks: [[planner-playbook]] and [[botster-planner-playbook]].
- Required Botster overlays: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], and [[botster orchestration prompts must bind agents to explicit worktrees]].
- Additional vault constraints: [[botster core owns reusable crypto and identity mechanisms]], [[plan steps need reviewable plan artifacts]], and [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Self context loaded as [[identity]] and [[goals]].
- Botster hub skill loaded: `botster-customize-hub`, used only for hub boundary guidance; this ticket is Rust hub policy/storage work, not Lua plugin orchestration.
- Repo context inspected: `Cargo.toml`, `Cargo.lock`, `README.md`, `docs/adr/durable-hub-state-v1.md`, `docs/reports/audit-hub-transport-admission-and-key-storage-for-e2e-webrtc.md`, `src/lib.rs`, `src/config.rs`, `src/persistence.rs`, `src/daemon.rs`, `src/runtime.rs`, `src/auth.rs`, `src/daemon_transport.rs`, `crates/botster-hub-client/src/lib.rs`, and persistence/daemon/client tests.
- Current dependency evidence: `Cargo.lock` pins `botster-core` and `botster-core-daemon` to `42538009bc6f6291872c5657bedbe7370f504f8d`; the local Cargo checkout for that revision contains `CredentialStore`, `CredentialRecord`, `AesGcmKey`, `device_fingerprint`, and `verify_device_fingerprint`.
- Checklist discipline: `project_pipelines_checklist_instructions` was loaded. `project_pipelines_create_vault_checklist` was attempted and timed out at the plugin worker boundary, so this plan and gate evidence preserve notes read, convention checks, verification plan, and capture decision per [[project pipelines checklist worker timeouts require artifact evidence fallback]].

## Scope

In scope:

- Add hub-side concrete credential-store provider wiring over existing `botster-core` contracts. Core owns mechanism; hub owns provider selection, key ids, persistence policy, and diagnostics.
- Prefer OS keychain/keyring-backed storage for production-capable private or secret material. Any file fallback must be explicitly scoped as test/dev-only or designed as encrypted production-safe storage with a documented key hierarchy.
- Extend the durable hub state model with non-secret browser trust metadata:
  - storage key references/ids for hub-owned credentials;
  - trusted browser public identity records and public fingerprints;
  - trust state, revocation metadata, expiry metadata, and audit-safe reasons;
  - bootstrap grant records only if they are metadata/reference records and do not store raw grant secrets.
- Wire startup/runtime paths so trusted browser identities and key references survive a fresh `HubDaemon`/`HubRuntime` load from the configured data directory.
- Add fail-closed behavior when the configured credential provider is unavailable, missing required records, corrupt, or mismatched with persisted hub-state references.
- Add structured diagnostics using existing daemon diagnostic/operator-error shapes where a client/operator can see why credential storage or browser trust is unavailable.
- Document the credential provider policy, storage key id scheme, file fallback policy, durable browser identity records, bootstrap grant expiry semantics, and the invariant that `hub-state.json` never stores raw private keys, browser secrets, or PII.
- Add focused Rust tests for durable restart, redaction/no-plaintext invariants, revocation/expiry enforcement, and unavailable provider diagnostics.

Botster layers touched:

- Rust hub crate: primary implementation surface.
- Rust core dependency: consumed only through public identity/credential contracts; no core changes expected.
- Hub client protocol: only if existing diagnostic kinds are insufficient for actionable fail-closed errors.
- Docs: README/client or ADR docs for storage policy.
- Not touched: browser SPA/WebRTC implementation, Rails/cloud provider, Lua plugin workflow policy, TUI UI, MCP behavior except incidental diagnostics exposure.

## Non-Scope

- No implementation of WebRTC signaling, DataChannel transport, QR pairing UI, remote cloud/account approval, relay provider, TURN/STUN, or browser repository changes.
- No new core credential primitives unless current locked core APIs prove inadequate during implementation; the expected path is hub wiring over existing core contracts.
- No raw private key, raw browser secret, grant token, email, hostname, local path, or other PII in `hub-state.json`, test fixtures, docs, diagnostics, or audit reasons.
- No broad rework of package registry persistence, session runtime, daemon socket framing, or capability admission.
- No speculative multi-user/cloud sync credential database.
- No silent plaintext file fallback. A file-backed store is acceptable only if explicitly test/dev-only or encrypted with a documented production-safe key source.

## Assumptions And Unknowns

- Assumption: the previous audit's owner split still applies: `botster-core` supplies reusable `CredentialStore`, `CredentialRecord`, `AesGcmKey`, public identity, fingerprint, and crypto contracts; `botster-hub` owns concrete local storage policy.
- Assumption: `hub-state.json` remains the canonical durable hub metadata aggregate, but it stores only references and public trust metadata for credentials, not secret material.
- Assumption: trusted browser identity persistence is needed before production WebRTC, but this ticket does not need to expose the final browser pairing transport.
- Assumption: local bootstrap grants are ephemeral admission artifacts with explicit expiry/revocation semantics. If any grant material must survive restart, persist only encrypted/sealed material through the credential provider and keep hub-state to reference metadata.
- Assumption: public browser identity/fingerprint records are not secret, but they are still privacy-sensitive enough to avoid real PII in docs/tests.
- Unknown: whether the hub should add a new `src/credentials.rs` or place provider wiring under `src/auth.rs`/`src/persistence.rs`. Prefer a small dedicated module if it keeps storage provider policy out of the JSON store mechanics.
- Unknown: whether current `DaemonDiagnosticKind` is sufficient. Prefer existing `ActionFailure`/operator-error diagnostics unless adding a narrow kind clearly improves client behavior without protocol churn.
- Unknown: whether production OS keychain can be added without a new dependency. Follow repo convention of minimal dependencies; if a crate is required, verify the latest version before choosing it.
- No human question is blocking at plan time. If implementation would need to waive OS keychain usage, persist plaintext fallback, or make the ticket scaffold-only, stop and ask.

## Affected Surfaces / Files

Expected changes:

- `Cargo.toml` / `Cargo.lock`: add the smallest OS keychain/keyring dependency only if needed after verifying current versions; avoid dependency sprawl.
- `src/credentials.rs` or equivalent new hub-owned module: concrete provider selection, key id construction, core `CredentialStore` adapter, test/dev fallback policy, and provider diagnostics.
- `src/persistence.rs`: additive v1 state fields for browser trust metadata and credential key references, with serde defaults for existing v1 files. Do not store secret bytes.
- `src/config.rs`: narrow provider/fallback policy config only if compile-time/default policy is not enough; avoid optional configurability unless needed for tests and production provider selection.
- `src/runtime.rs` and/or `src/daemon.rs`: load and validate credential provider plus durable browser identity state during startup so the runtime path actually changes.
- `src/auth.rs`: optional trust/admission hook names or small helpers for browser identity/grant validation.
- `src/daemon_transport.rs` and `crates/botster-hub-client/src/lib.rs`: optional diagnostic DTO additions if existing operator-error diagnostics cannot express fail-closed credential storage.
- `src/lib.rs`: re-export only stable public types needed by tests/docs.
- `README.md`, `docs/client-protocol.md`, or a new docs/ADR page: credential provider and no-plaintext invariant documentation.
- `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_runtime_test.rs`, `tests/hub_client_api_test.rs`, and/or persistence unit tests: restart, redaction, revocation/expiry, provider-unavailable diagnostics.

Reference surfaces:

- `docs/reports/audit-hub-transport-admission-and-key-storage-for-e2e-webrtc.md`: current audit and ticket-ready follow-up plan.
- `docs/adr/durable-hub-state-v1.md`: current hub-state JSON consistency and privacy posture.
- Existing package-secret tests in `src/persistence.rs`, `tests/hub_daemon_lifecycle_test.rs`, and `tests/hub_client_api_test.rs` for redaction style.

## Proposed Implementation Steps

1. Reconfirm current core API shape from the locked `botster-core` revision and import only public root exports where available.
2. Define hub credential provider policy:
   - production default: OS keychain/keyring where available;
   - test/dev fallback: deterministic in-memory or encrypted file-backed provider with explicit config/test construction;
   - unavailable provider: typed fail-closed error and daemon diagnostic, not implicit plaintext fallback.
3. Add durable metadata records to `HubState`:
   - `credential_keys` or equivalent references with stable key ids, purpose labels, provider kind, created/rotated timestamps if available, and no raw bytes;
   - `trusted_browser_identities` with synthetic/browser public id, public key/fingerprint, trust state, trusted/revoked/expiry timestamps, and audit-safe reason;
   - `bootstrap_grants` only as scoped metadata with expiry/revocation/redeemed state, and no plaintext grant token.
4. Add helper methods/models that enforce trust decisions:
   - valid trusted browser identity must match public fingerprint verification;
   - expired, revoked, missing, or provider-unavailable records deny admission;
   - grant redemption is one-time or explicitly state-transitioned, with expiry checked before use.
5. Wire runtime startup:
   - `HubRuntime::load_from_store` or `HubDaemon::start` constructs/validates the credential provider and loaded trust metadata;
   - failures before trust-sensitive operations return structured diagnostics;
   - status/diagnostic paths expose availability without leaking local paths or key ids that are secret-bearing.
6. Add focused tests:
   - direct persistence/model tests for serde defaults, restart, and no-plaintext JSON;
   - runtime/daemon test proving a fresh store instance reloads trust metadata and enforces it;
   - negative tests for revoked/expired identities and unavailable provider.
7. Update docs with the exact provider policy, key id naming, fallback policy, and invariant that hub-state contains public metadata/references only.

## Risks

- Secret leakage risk: adding trust records to `hub-state.json` can accidentally persist raw grants, private keys, browser secrets, or PII. Tests must scan serialized JSON for known raw secret fixtures.
- Fail-open risk: missing keychain or unreadable credential records must deny trust-sensitive operations with diagnostics, not silently regenerate or accept trust.
- Unwired implementation risk: adding provider structs/tests is not enough. Runtime startup or the browser-trust admission path must consume the provider and loaded records.
- Dependency risk: OS keychain support may require a new crate. Verify latest versions and keep the dependency narrow.
- Schema compatibility risk: current v1 state files must continue loading via serde defaults; unknown future versions should still fail as they do today.
- Overreach risk: browser trust storage can drift into full WebRTC pairing/transport. Keep this ticket to storage, policy, and diagnostics.
- Privacy risk: public fingerprints are durable identifiers. Use synthetic fixtures and avoid docs that imply fingerprints are harmless public UI strings.
- Diagnostic churn risk: adding new daemon diagnostic kinds may require generated TypeScript drift work. Prefer existing diagnostic shapes unless a new kind is necessary.

## Acceptance Checks / Tests

Required behavioral checks:

- Tests prove trusted browser public identity/fingerprint records and trust metadata survive restart through `HubStateStore`/`HubDaemon` or `HubRuntime` load.
- Tests prove `hub-state.json` contains no raw private key, browser secret, grant token, plaintext fallback key, `write_only`, local user path, email, or PII marker; only public metadata and provider key references appear.
- Tests prove revoked and expired browser identities/grants are denied, and valid unexpired trusted identities are accepted by the hub trust helper/runtime path.
- Tests prove missing/unavailable credential provider or missing key id fails closed with structured daemon/operator diagnostics.
- Tests prove any file fallback is either test/dev scoped or encrypted and documented; no production path silently uses plaintext files.
- Docs describe credential provider, storage key ids, fallback policy, trusted browser identity records, bootstrap grant expiry/revocation, and no-plaintext-secret invariant.
- Implementation report must identify the actual production entry point changed, such as `HubDaemon::start`/`HubRuntime::load_from_store` validation or a concrete browser trust/admission helper invoked by runtime/client API.

Expected commands:

- `cargo fmt --check` or `cargo fmt` before final diff.
- `./test.sh` for the full Rust test wrapper when feasible.
- Targeted tests while iterating, likely:
  - `./test.sh persistence`
  - `./test.sh --test hub_daemon_lifecycle_test <new_restart_or_diagnostic_test> -- --test-threads=1`
  - `./test.sh --test hub_client_api_test <new_trust_or_diagnostic_test>`
- `cargo clippy --all-targets --all-features -- -D warnings` if implementation changes public Rust surfaces, with exact attribution for any pre-existing baseline failures.
- `rg -n "super-secret|private key|grant-token|write_only|/Users/|/home/|@example\\.com" src tests docs README.md` or equivalent diff-focused scan after adding fixtures/docs.

## Pipeline Gates And Artifacts

- Plan artifact: `docs/plans/wire-hub-credential-provider-and-durable-browser-identity-storage.md`.
- Implement artifact should include changed entry points, credential provider policy, state schema additions, exact no-plaintext evidence, diagnostics examples, and commands run.
- Plan gate evidence should preserve checklist fallback because checklist creation timed out.
- Review should reject implementations that:
  - add storage/provider code without a runtime/admission consumer;
  - store raw secrets or PII in hub-state/docs/tests;
  - silently use plaintext file fallback;
  - propose new core credential primitives without proving current core contracts are insufficient.

## Vault Gaps Worth Capturing

- Capture a durable note if implementation settles the exact hub credential provider pattern over core `CredentialStore`: OS keychain default, encrypted/test fallback, key id naming, and provider diagnostics.
- Capture a durable note if the durable browser identity record shape becomes a reusable convention: public identity/fingerprint plus revocation/expiry metadata in hub-state, secrets only in credential provider.
- Capture a durable note if bootstrap grant semantics are settled: local, scoped, expiring, one-time/revocable metadata rather than durable plaintext secrets.
- Capture a durable note if a dependency choice for OS keychain/keyring becomes a project convention.

## Checklist Evidence

- Vault/context evidence: notes listed in `Context Loaded` constrained the plan to hub-owned policy over core-owned mechanisms, repo-visible plan artifacts, explicit target/worktree assumptions, and artifact fallback when checklist persistence times out.
- Convention conflicts: none found. The plan follows Botster's core/hub boundary and avoids speculative WebRTC, browser, cloud, or core-primitive work.
- Verification evidence gathered during planning: repo inspection found metadata-only `HostIdentity` in `src/config.rs`, JSON hub-state aggregate in `src/persistence.rs`, daemon/runtime startup loading through `src/daemon.rs` and `src/runtime.rs`, existing redacted package-secret tests, existing daemon diagnostic DTOs, and no current hub wiring for concrete credential providers or durable browser trust records.
- Capture evidence: no vault capture during planning; capture after implementation if the credential provider pattern or browser trust/grant semantics are settled by code.
