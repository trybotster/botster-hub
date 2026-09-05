# Foundation implementer handoff: Grok to Codex

The user requested this replacement because Grok reached its usage limit.
The root coordinator retains final technical decision authority.
Fable remains the independent reviewer.
This foundation work uses direct coordination, not Project Pipelines.

## Completed Core work

Grok's preserved worktree is `/Users/jasonconigliari/botster-sessions/trybotster-botster-core-foundation-nonblocking-resize`.
Its branch is `foundation/nonblocking-resize`.
The coordinator verified a clean worktree at `5923bf1847979e2897796fadb9863183ffa5e3f1` before this handoff.
Do not edit that frozen worktree or restart the implementation from Core main.

Commit sequence:

- Base: `93acae3f98adbc21dc981d113c4eb2f31ead4ad0`.
- Main repair: `53ed10f2387d532c8269669522e8c8f7f357af3c`.
- Lifecycle guard and bounded regression: `a2d6c97c8c38125c05b9507c3bccefa5bfb5db65`.
- Guard-test correction: `5923bf1847979e2897796fadb9863183ffa5e3f1`.

The repair removes blocking resize completion from the shared pump.
It reuses acknowledgement reconciliation, adds absolute deadlines, and caps pending resizes at 30.
Expired deadlines join ordinary wake batches, so sibling traffic cannot starve them.
Timeout failure affects only the named live session.
Stopping or exited sessions retain pending terminal exit delivery.
An explicit resize returns a distinct busy error while that session has pending ingress resizes.

Fable approved the exact final commit.
Fable independently verified the production-guard negative control, 89 Core library tests, 55 wake tests, formatting, and Clippy.
Earlier full review verified 967 workspace tests, one ignored test, 41 no-default library tests, and nine documentation tests.
The later follow-ups received focused verification; do not claim all full gates ran again on the final SHA.
The report labels post-teardown acknowledgement coverage as partial because the reader gate blocks shutdown.

Read these files completely before acting:

- `/Users/jasonconigliari/Projects/botster-hub/docs/plans/2026-09-04-nonblocking-resize.md`.
- `docs/reports/2026-09-04-nonblocking-resize-implementation.md` in Grok's frozen worktree.
- `/Users/jasonconigliari/botster-sessions/trybotster-botster-core-foundation-resize-review/docs/reports/2026-09-04-nonblocking-resize-implementation-review.md`.
- `/Users/jasonconigliari/Projects/botster-hub/docs/reports/2026-09-04-resize-downstream-validation.md`.

Also read the applicable repository instructions and ownership charters.

## Remaining work

Core implementation is approved. Downstream Hub validation is not complete.
No foundation commit has been merged or published.

The isolated Hub source is `/private/tmp/botster-resize-downstream.g2c6fu`, exported from Hub `12b4a5482457fa9204f6c08916810abaaef682be`.
The isolated Core source is `/private/tmp/botster-resize-core.0yQvP0`, exported from approved Core `5923bf1`.
All six Core packages use local overrides in the temporary Hub manifest.
The matching worker built successfully.
Ghostty installs generated files inside its source tree, so builds must not point at the frozen source worktree.
The isolated Zig package cache is already available; consult the validation report for commands.

Hub compilation fails at `src/runtime.rs:4169` because `managed_session_core_error_class` lacks `CoreDaemonError::ExplicitResizeBusy`.
Add a narrow classification arm and focused regression in the isolated Hub integration copy.
Do not remove the Core error variant or weaken Hub's exhaustive match with a wildcard.
The temporary fixture loader already has an exact-path adaptation for the isolated protocol source.
Record that adaptation as validation setup, not a production change.
No downstream daemon test has run yet.

Codex may inspect source and prepare the small isolated integration patch now.
Do not run builds, daemon tests, or a full matrix until the coordinator releases the validation window.
The existing Hub/TUI integration owns resource-sensitive validation during this handoff.
Do not merge, publish, update active ticket pins, or modify another agent's worktree.
Report the proposed classification and test before starting resource-intensive checks.
The coordinator will decide the durable Hub integration branch and final dependency update after existing integration settles.

## Concurrent integration: do not take ownership

Hub ticket: `ticket_1787600679_990088`.
Its latest recorded candidate is `205cadf6f8dab9dc990537c2c00ef3d27edb31dd`, still consuming Core `93acae3`.
TUI ticket: `ticket_1788460430_647093`.
TUI run: `run_1788570301_694931`.
Last recorded TUI stage is Plan, with no reviewed candidate yet.

The final sequence is: review and freeze TUI, run the complete matrix, merge Hub, then finalize and merge TUI.
Do not substitute the foundation Core candidate into that matrix.
Do not restart Grok's work or duplicate the TUI implementation.

## Coordination

Coordinator: `sess-1788561261-002e-6e11191cb68e3da8e22b8f8cbf0c82d0`.
Fable reviewer: `sess-1788561640-0030-9ab4035a5a3867e8f4970b3a6e440510`.
Retired Grok implementer: `sess-1788562456-0031-89e8d43a7b42a85a60820f2d4b8f3eb7`.

Use Botster `post_message` with `session_uuid`, `msg_type`, and `payload`.
Do not put message text in a field named `message`.
First report your session, worktree, branch, HEAD, and understanding of the remaining work.
Keep reports in files and send short progress messages.
