# Independent review of the active integration ticket

Ticket: `ticket_1787600679_990088`.
Reviewer: built-in Astra agent `validate_active_integration`.
Candidate: `7164c38947a06c5cb439a989f7ae3bb2caff170d`.
Locked Core revision: `93acae3f98adbc21dc981d113c4eb2f31ead4ad0`.
Worktree: `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1787600679_990088`.

## Verdict

The ticket produced useful work, but this candidate is not ready to merge.
The reviewer inspected source and existing logs without changing the active worktree or its build target.
The reviewer ran one isolated framing experiment against the existing client library.
The candidate remained clean and unchanged during the review.
All source locations below refer to this candidate, not Hub main.

## High: recoverable polling loses partial frames

`crates/botster-hub-client/src/lib.rs:332` adds `poll_terminal`, which converts a socket timeout into `Ok(None)`.
The parser at line 616 reads a frame into a new local `String`.
If a timeout occurs after a partial read, the function discards the consumed prefix.
The next poll attempts to parse only the remaining suffix.

The reviewer reproduced this behavior with an isolated Unix socket pair and the already-built Hub client library.
The harness is `/tmp/botster-audit-framing.lC5JX6/probe`.
The exported production parser accepted a complete envelope with payload `YQ==`.
The same envelope, split after nine bytes across a 30 ms timeout, produced `WouldBlock`, then `Json("trailing characters")`.
The experiment called `read_unix_mux_frame_from_reader`, not `poll_terminal` directly.
Source inspection establishes that `poll_terminal` suppresses the first error.

The timeout also applies to each socket read, not the complete operation.
Continuous host events can keep `next_terminal` running beyond the documented timeout.
This deadline finding comes from source inspection; the reviewer did not reproduce its timing.

Preserve partial frame bytes across polling timeouts.
Enforce an absolute deadline for the complete operation.
Add focused tests for fragmented frames and continuous nonterminal events.

## Medium: obsolete DrainRuntime code and documentation remain

The candidate deletes external `DaemonRequest::Drain`, but retains a second obsolete interface:

- `src/client_api.rs:748` exposes `HubClientRequest::DrainRuntime`.
- `src/client_api.rs:183` returns empty events for that request.
- Its documentation still promises Core subscription output.
- `src/daemon_projection.rs:1140` retains the operation mapping.
- `tests/hub_client_api_test.rs:2917` and `tests/hub_local_runtime_test.rs:376` still call the request.
- `README.md:1055` still describes terminal output through explicit `DrainRuntime` calls.

The documentation defect is definite. The remaining interface requires an explicit ownership decision.
This no-op is not a surviving external terminal route.
Repository searches found request constructors in tests, but no production callers.
Earlier plans explicitly retained this no-op, so its presence does not independently prove a ticket scope violation.
The coordinator recommends removal because it has no current function and the user permits clean cuts.
Retaining it requires a concrete consumer or contract reason.

## Verified progress

The candidate changes 32 files, with 521 insertions and 939 deletions.
It removes external Drain routing, constructors, generated exposure, ownership bookkeeping, and associated tests.
All six Core sources in `Cargo.lock` use `93acae3`.
The rejected global-pump and synthetic-control-wake helpers are absent.

The Unix and WebRTC isolation tests require sibling byte delivery and both routes to remain bound.
These tests prove isolation under adapter pressure.
They do not prove sibling progress while Core waits for a worker resize acknowledgement.
The separate Core resize repair remains necessary.

## Validation status at review time

The earlier `/tmp/botster-hub-matrix-99d9a9e-final.log` records 11 passing arms and no failing arms.
It includes Ghostty and north-star completion markers and matching clean Hub/TUI boundaries.
That complete matrix predates the substantial Drain removal.

The first matrix at `7164c38` failed these tests:

- `owned_worker_census_is_non_empty_before_absence_assertions`
- `reap_budget_expiry_is_not_successful_cleanup`

The log records load averages of `75.70 113.13 84.45`.
Load alone does not establish the cause of these failures.

The second matrix log is `/tmp/botster-hub-matrix-7164c38-2.log`.
At review time, fmt, clippy, locked, hub_ts, and web_unit had passed.
Lifecycle tests reported 347 passed, zero failed, and two ignored.
The run was progressing into Web lanes, not stalled in the many-PTY tests.
No completed matrix for the current revision was available at review time.

An earlier durable `waitForDaemonTerminalOutputBytes` failure still requires separate attribution.
The recorded human waiver covered two different harness signatures.

## Shortest completion path

1. Let the current bounded run finish without interference.
2. Repair frame buffering, the polling deadline, and the remaining obsolete interface together.
3. Run focused regression tests before the full matrix.
4. Attribute the two cleanup failures and the distinct durable-output failure.
5. Run one complete, unskipped matrix at the resulting clean commit.
6. Review that exact commit before acceptance.

Both ticket owners confirmed that no agent in this ticket is changing Core resize handling.
Keep the separate Core repair away from the active Hub worktree and its tested dependency revision.
The coordinator sent both owners the framing reproduction and the remaining obsolete-interface finding.

## Owner response after the review

The implementer confirmed the partial-frame defect and stopped the matrix to repair the candidate.
The implementer also confirmed the false README description and began correcting the documentation.
The implementer initially proposed retaining the distinct in-process `DrainRuntime` no-op.
The coordinator requested a concrete reason for retaining it and clarified its classification with the acceptance owner.
The acceptance owner recorded the findings as blocker artifact `artifact_1788562003_396758`.
The acceptance owner will not accept a passing matrix until framing is fixed and the interface decision is resolved.

The ticket produced substantive fixes and real consumer coverage.
The history also contains avoidable work from weakened tests, combined retries, and missed obsolete interfaces.
A new matrix after a source change is justified.
Checking the complete removal scope and new APIs before each full matrix can reduce repeated work.
