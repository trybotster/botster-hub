# Tighten Hub DTO Drift Helper

## Context Loaded
- Pipeline context: ticket `ticket_1783301954_442581`, run `run_1783301972_685287`, step `botster_plan`, gate `botster_plan_gate`.
- Playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Botster context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[botster web dto field names must match authoritative rust serde structs]], [[generated typescript dtos must encode serde field optionality]], [[botster web generated protocol drift checks need explicit hub artifact paths]], [[test script required for rust tests not cargo test]].
- Repo evidence: `crates/botster-hub-client/src/lib.rs` owns the serde DTO examples and current one-way `assert_generated_interface_fields`; `crates/botster-hub-client/src/typescript.rs` emits deterministic TypeScript; `crates/botster-hub-client/generated/daemon-protocol.ts` is the checked generated artifact; `test.sh` wraps `cargo test` with `BOTSTER_ENV=test`.

## Scope
- Strengthen the test-only generated DTO drift helper in `crates/botster-hub-client/src/lib.rs`.
- Make interface checks optionality-aware and symmetric for required fields:
  - every serialized serde example field must appear in the generated TypeScript interface;
  - every required TypeScript interface field (`name:`) must appear in the serde example;
  - optional TypeScript interface fields (`name?:`) may be absent from a serde example, because existing tests intentionally prove omitted optional fields are valid wire JSON.
- Add obvious type parity checks by parsing generated interface field lines and comparing them to expected TypeScript types supplied by focused tests. This is opt-in per field named by the test; it is not a full Rust-to-TypeScript type inference system.
- Preserve the existing full generated artifact equality check.
- Add focused regressions that would fail for an extra TypeScript field and for an obvious changed type.

## Non-Scope
- No redesign of protocol generation in `typescript.rs`.
- No broad DTO audit beyond the helper and the fields needed to prove the helper behavior.
- No browser, SPA, plugin, transport, or runtime behavior changes.
- No dependency additions.

## Assumptions And Unknowns
- Assumption: "obvious type drift" can be covered by exact TypeScript type strings already emitted by the deterministic generator for fields explicitly named by a test, not a full Rust-to-TypeScript type inference system.
- Assumption: regression tests can exercise the helper with small inline TypeScript interface snippets or helper-level parsing, without intentionally corrupting the checked generated artifact.
- Assumption: reverse field-set checking must be optionality-aware. Optional `?:` TypeScript fields that are omitted from the serde example are legitimate; missing required TypeScript fields are drift.
- Unknown: whether the implementer will choose to replace `assert_generated_interface_fields` directly or add a companion helper. Either is acceptable if existing call sites remain covered and the helper catches both reverse fields and type drift.

## Affected Surfaces And Files
- `crates/botster-hub-client/src/lib.rs`: test module helper and focused regression tests.
- Potentially `crates/botster-hub-client/src/typescript.rs` only if tiny testability exposure is required, but the preferred plan avoids generator changes.
- `docs/plans/tighten-hub-dto-drift-helper.md`: this plan artifact.

## Botster Layers Touched
- Rust hub-client public protocol test surface only.
- No Lua plugin, Rust hub runtime, TUI, React SPA, Rails relay, MCP, or transport-layer changes.

## Risks
- A brittle parser could overfit current formatting. Keep parsing constrained to the deterministic generator's own interface line format.
- Generated union variant checks currently have similar one-way behavior, but the ticket names the shared interface helper. Do not broaden to unions unless the helper refactor makes it essentially free and still surgical.
- Serde examples omit fields skipped by empty/default values, so reverse checking must distinguish optional generated TypeScript fields from required fields. Optional omitted fields such as `ui_tree_snapshot?`, `diagnostics?`, `max_retransmits?`, and `max_packet_lifetime_ms?` must stay valid.

## Acceptance Checks
- `./test.sh -p botster-hub-client generated_typescript_protocol_matches_checked_artifact`
- `./test.sh -p botster-hub-client generated_typescript_local_webrtc_fields_match_serde_json`
- `./test.sh -p botster-hub-client daemon_response_kinds_are_serde_stable_and_generated`
- Add and run focused regression test(s) proving:
  - an extra required TypeScript field fails the helper;
  - an extra optional TypeScript field absent from the serde example does not fail the helper;
  - a changed obvious TypeScript type fails the helper.
- The type-parity assertion should take `(type_name, field, expected_ts_type)` or equivalent and assert the generated interface line for `field` ends in the expected type, independent of whether the serde example populates that field. Coverage is opt-in for fields the test names.
- Existing optional-and-omitted-field call sites must still pass after tightening:
  - `plugin_surface_snapshot_is_serde_stable_and_generated` for `ui_tree_snapshot?`;
  - `generated_typescript_local_webrtc_fields_match_serde_json` for `max_retransmits?`, `max_packet_lifetime_ms?`, and the nested answer's optional diagnostics path.
- If the focused filters are awkward, run `./test.sh -p botster-hub-client` and record the exact result.

## Pipeline Gates And Artifacts
- Gate evidence should cite this plan and include the exact helper behavior, notes loaded, verification commands, and any skipped verification reason.
- Worktree/target assumption: this run is bound to target `tgt_7e208a0c76a44980a83b63af976b1f22` and workspace `Pipeline - Tighten hub DTO drift helper to assert symmetric fields and t...`; implementers should operate in this assigned worktree, not an ambient checkout.

## Vault Gaps Worth Capturing
- The existing vault already contains the durable rule under [[generated dto drift tests need symmetric field and type checks]] and related DTO mirror notes. No new durable vault note is needed unless implementation uncovers a reusable parsing/testing pattern or a broader union-helper gap.
