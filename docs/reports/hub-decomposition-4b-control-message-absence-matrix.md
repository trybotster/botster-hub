# Implement follow-up: ControlMessage paired-absence matrix

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1787894965_150479` |
| Run | `run_1788030103_935368` |
| Step | `botster_stack_implement` (`run_step_1788041472_312486`) |
| Open finding closed | `finding_1788041456_734415` (medium) |
| Runtime-teardown class | applies; lenses unchanged |

Routing is unchanged.

## Playbooks and notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]
- [[code moves need paired absence and presence source guards]]
- [[a regression test must be shown to go red with the fix reverted]]

[[project-pipelines-playbook]] was not loaded.

## Files changed

| Path | Why |
| --- | --- |
| `src/daemon/control/connection.rs` | `handle` matches connection ControlMessage variants so they have one module owner |
| `src/daemon/control/entities.rs` | `handle` matches Subscribe/UnsubscribeEntities control messages |
| `src/daemon/control.rs` | One-call delegation to those handlers |
| `tests/daemon_control_ownership.rs` | Complete ControlMessage owner map with paired absence across every control handler module |

## Ownership boundaries preserved

Each ControlMessage variant has one handler owner. `control.rs` still names variants only in delegating arms. `EgressWriteFailed` remains dispatcher-owned. Public DTOs, protocol, Core pin, live-peer gates, and teardown lenses are unchanged.

## Cross-repo routing

None.

## Deviations from plan

None. This completes acceptance check 4's paired-absence half.

## Tests and downstream proof

`RUSTUP_TOOLCHAIN=1.97.0`, `CARGO_TARGET_DIR` unset:

- `cargo test --locked --test daemon_control_ownership` — 8 passed
- Duplicating `ControlMessage::Request` into `plugins.rs` reddens `control_message_variants_have_one_family_or_dispatcher_owner`
- `cargo clippy --workspace --all-targets --locked -- -D warnings` — passed
- `./test.sh --locked` — passed

## Unverified behavior or residual risk

Downstream TUI/Web not rebuilt; DTOs unchanged.

## Missing vault guidance

None.
