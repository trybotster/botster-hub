# Implement follow-up: rustfmt and install-lease comment

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1787894965_150479` |
| Run | `run_1788030103_935368` |
| Step | `botster_stack_implement` (`run_step_1788045673_464087`) |
| Merge policy | direct into `main`; do not create a PR |
| Open findings closed | `finding_1788045657_335227` (medium), `finding_1788045657_536558` (low) |
| Runtime-teardown class | applies; no teardown bodies edited |

Routing is unchanged.

## Repository playbook and other playbooks/notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[rust repo strict lints must be verified before dismissing warnings]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

[[project-pipelines-playbook]] was not loaded.

## Files changed

| Path | Why |
| --- | --- |
| `src/daemon/control/spawn_targets.rs` | Rustfmt single-line signatures for private helpers |
| `src/main.rs` | Install-lease comment names the owner loop and control families, not the deleted file |
| `docs/reports/hub-decomposition-4b-fmt-and-lease-comment.md` | This report |

Live `src/` and `tests/` no longer contain the needle `daemon_transport.rs`. Historical `docs/plans/` and `docs/reports/` records were left alone. The public crate-root alias `daemon_transport_request` is unchanged.

## Ownership boundaries preserved

The installation lease still lives in the Hub binary entrypoint, not in daemon control. Public DTOs, protocol, Core pin, live-peer gates, and teardown lenses are unchanged.

## Cross-repo routing

None.

## Deviations from plan

None.

## Tests and downstream proof

Always `RUSTUP_TOOLCHAIN=1.97.0` and `CARGO_TARGET_DIR` unset:

- `cargo fmt --all -- --check` — exit 0
- `git diff --check` — exit 0
- `cargo clippy --workspace --all-targets --locked -- -D warnings` — exit 0

The official locked suite was not rerun. The delta is rustfmt wrapping plus a comment. The previous official suite at `2956c26` was green. The fmt failure appeared after a helper-visibility restore that skipped rustfmt.

## Unverified behavior or residual risk

Official `./test.sh --locked` was not rerun on `bd42b4a`. Downstream TUI/Web were not rebuilt.

## Missing vault guidance

None.

## Git state

- Repair commit: `bd42b4a`
- Branch: `project-pipelines/ticket_1787894965_150479`
- No PR, because the ticket requires a direct merge to main
