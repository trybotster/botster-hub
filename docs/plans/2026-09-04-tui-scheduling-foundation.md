# TUI scheduling repair

Status: implementation preparation. Do not modify the active TUI integration candidate for this work.

## Scope and evidence

The inspected TUI candidate is `812b2004ab1bcc8ff24b824c42b0b7c5e5059550`.
Its worktree is `/Users/jasonconigliari/botster-sessions/git-github.com-trybotster-botster-tui-project-pipelines-ticket_1788460430_647093`.
The integration ticket retains ownership of its dependency update and final verification.
Start this repair from the resulting merged TUI revision in a separate worktree.

Current source has these scheduling defects:

- `app.rs::run_loop` waits up to 100 ms for Crossterm input after every draw. Terminal output cannot interrupt that wait.
- `HubConnection::request` removes the read timeout. `TuiApp::request_and_apply` calls that method from the application thread.
- `TuiApp::new_with_runtime_context` calls `try_connect` before the event loop starts.
- `apply_live_terminal_output` refreshes the viewport after each frame. The loop refreshes the viewport again before drawing.
- Snapshot application and scrolling also refresh the viewport directly.
- `projection_paint.rs::projected_symbol` allocates a String for each painted cell.
- Existing subscription pumps use message channels. The repair must include these producers in its wake and capacity design.

These are source findings. They do not establish measured latency, frame rate, or memory cost.

## Required ownership

The application thread owns application state, input routing, the client terminal model, and painting.
An I/O owner manages connection establishment, socket reads, socket writes, and request completion.
The I/O owner must continue reading unsolicited terminal and entity events while a request awaits its response.
A blocking request loop moved unchanged to another thread does not satisfy this requirement.

Use the existing response protocol. Do not assume that responses have correlation identifiers.
If the protocol requires one outstanding control request, serialize those requests within the I/O owner.
After an uncorrelated request times out, retire its connection before submitting the next request.
Otherwise, the late wire response could complete the wrong request despite local completion identifiers.
That restriction must not block terminal delivery, rendering, input routing, or cancellation.
Preserve terminal input ordering and the existing mode-dependent encoding contract.

Use bounded command and event storage. Define limits in both item count and retained bytes where payload sizes vary.
Define behavior when each limit is reached. Never discard arbitrary terminal bytes and continue the same decoder state.
Preserve stream order between terminal data and terminal lifecycle events.
Promote EventSubscribed before applying parked notice events. Preserve `event_subscribe_response_race_promotes_before_parked_apply`.
Bound each application turn so continuous output cannot prevent input handling or painting.

Connection attempts and requests need absolute deadlines.
Late results must carry enough local connection and subscription identity to reject obsolete completions.
Do not apply an old Attach result to a new attachment.
Keep connection cancellation and local quit available when Hub does not respond.
Shutdown must stop owned readers and writers within a documented bound.

## Event loop and projection

Terminal output, entity updates, request completion, input, timers, and shutdown must wake the event loop.
Select the smallest wake mechanism supported by the existing dependencies after checking their cancellation behavior.
Do not replace the current wait with a shorter periodic poll or an uncancellable input thread.

Apply each terminal frame to the client model in protocol order.
Mark the viewport dirty after output, snapshot changes, scrolling, resize, or other visible model changes.
Refresh the viewport once before a required paint, after applying the selected batch.
Do not suppress required cursor, input-mode, or attach-state updates when a paint is deferred.
Borrow the projected grapheme during painting. Preserve the blank-cell fallback.
Do not add row-damage APIs in this change without evidence that full projection remains a material cost.

## Acceptance

1. Start the actual event loop with no keyboard input. Deliver terminal output after the loop begins waiting. Verify that output wakes painting.
2. Withhold a Hub response. Deliver terminal output on the same connection. Verify output application, painting, local input handling, and quit.
3. Stall connection establishment. Verify that local quit remains available before connection completion.
4. Apply several output frames before one paint. Verify one viewport refresh and exact final terminal content.
5. Exercise snapshot Ready, History, and Finish events with live output. Preserve the existing attach ordering checks.
6. Complete an obsolete request after reconnect or attachment replacement. Verify that the application rejects the result.
7. Fill each bounded queue. Verify documented admission or recovery behavior and bounded retained storage.
8. Flood output while sending local input. Verify progress for both without a population-wide or time-based polling shortcut.
9. Cancel while a read, write, request, or connection attempt is pending. Verify bounded shutdown and no owned thread remains.
10. Run existing terminal decoding, input-mode, paste, resize, detach, and first-party integration tests against recorded revisions.

Use controlled wake and response barriers for ordering tests. Avoid fragile wall-clock assertions as the only proof.
Record a separate optimized-build latency and process-cost measurement after correctness checks pass.
Keep production instrumentation disabled unless the operator explicitly enables it.

## Delivery constraints

The implementer must first map all synchronous request callers and subscription producers to the new completion path.
Tests must exercise the production event loop and I/O owner, not a separate test-only scheduler.
The reviewer must check deadlines, queue bounds, cancellation, ordering, and obsolete completion handling.
Do not expand this repair into a UI redesign, plugin rewrite, or terminal protocol migration.
