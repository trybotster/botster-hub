# Foundation worktree ownership, 2026-09-05

This note records which checkout owns each repository's foundation work, and the rules every writer follows. It is a coordination record, not a design document.

## Canonical worktrees

| Repository | Canonical worktree | Branch |
| --- | --- | --- |
| botster-hub | `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1787600679_990088-rc1` | `project-pipelines/ticket_1787600679_990088-rc1` |
| botster-core | `botster-core-foundation-stale-mode` | `foundation/stale-mode-contract` (published at `bf6e7d996bca2786ad4142c870a13c57a490e241`) |
| botster-web | `/private/tmp/botster-web-foundation.sm0cZt/web` | as assigned by the root coordinator |

The Hub worktree descends from `f78457a` and carries every earlier Hub commit. Do not cherry-pick them again.

## Rules

- One active implementation worktree per repository. A phase change does not create a worktree. If a new worktree is necessary, the root coordinator assigns a fresh agent started there.
- Every phase ends with a scoped checkpoint commit and a clean `git status`. A checkpoint does not imply review acceptance.
- Reviewers are read-only. Codex reviews; Fable implements.
- Evidence, pending changes, and unrelated user files are preserved. Scratch and evidence stay inside the worktree under `target/.botster-foundation-evidence/`.
- No push, merge, pipeline advance, full matrix, or Web live run until the combined candidate is built, reviewed, and identified by the root coordinator.

## Consolidated from other trees

- The root-owned consumer preparation from `/private/tmp/botster-registry-consumer.tuGcwH/hub` (base `11facecf`, an ancestor of this branch): `src/runtime.rs` and `src/update.rs` deltas, ported into this tree; the exact original patch is kept at `docs/plans/pending/2026-09-04-foundation-consumer-runtime-update.patch` (sha256 `c9212f37…`), and its report at `docs/reports/2026-09-04-foundation-consumer-preparation.md`. The original worktree is not deleted.
- Root orchestration plans copied where absent from `/Users/jasonconigliari/Projects/botster-hub/docs/plans/`: `2026-09-04-foundation-codex-handoff.md`, `2026-09-04-foundation-delivery-ledger.md`, `2026-09-04-nonblocking-resize.md`, `2026-09-04-tui-scheduling-foundation.md`. The four named audit, resize, and TUI plans already present here are byte-identical to the root copies and were not replaced.
