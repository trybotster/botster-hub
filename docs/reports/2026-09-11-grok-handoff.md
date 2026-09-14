# Grok implementation handoff

Jason requested Grok implementation, Claude detailed review, and limited Astra integration review.
Root paused the previous Codex process after its model-service wait prevented the requested handoff.
This commit preserves incomplete work. It does not establish a successful build or acceptance.

Read the current acceptance plan in the main Hub checkout. Its current execution section supersedes old mechanical approval gates.
Read `/private/tmp/claude-callback-review.OFb8pb/handoff-remaining-blockers.md` for detailed findings and evidence.
Root coordinates through Botster session `sess-1788561261-002e-6e11191cb68e3da8e22b8f8cbf0c82d0`.
Claude reviews through session `sess-1789062435-00a8-e8f5a8314c8baff1f0517bc0d4d91e05`.

## Work remaining

- Complete M1/M2/C1 admission, allocation sizing, transport ownership, conversion, and disposal through the real daemon.
- Inspect the latest transport edits before building. The previous writer did not finish verification.
- Continue S1/S2/R1 spawn lifecycle after Core supplies the shared reservation interfaces.
- Preserve no owner waits, no polling, existing identities, and repository ownership boundaries.
- Preserve the approved limits: 16 MiB per state, 128 MiB across states, 8 MiB per callback, and 64 MiB across callbacks.
- Do not activate the proposed 128-container depth limit. Jason has not selected it.
- Do not activate the paused hook allowance or the shared error diagnostic change. Jason has not selected the diagnostic change.
- Do not run or commit the preserved 277-line Lua reference-pressure diagnostic. It remains outside this checkpoint.
- Do not publish, install, restart the user runtime, or change another repository.

## Evidence

Root verified fifteen focused tests in `/private/tmp/c1-lua-callback-error-tests-20260910-1`.
Build 4 and eleven tests were reported passing in `/private/tmp/c1-lua-callback-error-build-20260910-4` and `/private/tmp/c1-lua-callback-error-tests-20260910-3`.
Root has not verified the latter raw artifacts. Subsequent transport changes are unbuilt.
Use Rust 1.97.0, at most two Cargo jobs, and no incremental compilation.
Coordinate the shared compiler with Root and the Core writer. Do not run the stopped diagnostics.
