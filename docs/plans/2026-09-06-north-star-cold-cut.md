# North-star cold cut

Status: superseded background draft. The implementation contract is committed at Hub `64fdfda2d735131fefe02bf009206c201aa31ba1`.
The live plan is `docs/plans/2026-09-06-north-star-cold-cut-implementation.md` in the canonical Hub worktree listed below.
Four fresh Fable writers now own Core, Hub, Web, and TUI implementation.
This draft does not authorize work or tests. Use the live plan and its root dispatch addendum.

## Authority and scope

The user authorized a coordinated replacement of the modular foundation.
The new system is not in use.
Replace superseded contracts and all their consumers together.
Do not retain old implementations, protocol fallbacks, migration scaffolding, or obsolete behavior tests for those contracts.
Keep correct mechanisms that satisfy the final contract.
Do not equate old code with obsolete code.

This plan supersedes intermediate pin, merge-order, and separate-worktree requirements in the September 4 foundation plans.
The old reports remain historical evidence.
Their pins and temporary constraints are not current implementation instructions.

The product remains a local-first Lua plugin platform.
Cloud federation is optional.
The repository split remains in place.
Workflow plugin redesign is outside this replacement.

## Working rules

1. Root owns the shared contract and cross-repository decisions.
2. Fable implements each layer after root and the architect agree on the contract.
3. Each repository has one canonical worktree and one active writer.
4. Each new agent starts in the worktree it will edit.
5. Agents do not edit another layer's repository.
6. Agents send required contract changes to the owning implementer and root.
7. Each completed implementation phase ends with a scoped commit and clean status.
8. Codex reviews a complete layer change and returns consolidated findings.
9. Current diagnostic and integration runs remain cancelled.
10. Compilation, formatting, and type checks may support implementation when assigned.
11. Tests of the replacement begin after the changed producers and consumers form a coherent stack.
12. No agent may remove unrelated user data, worktrees, or evidence.

## Starting worktrees

| Layer | Canonical worktree | Expected starting commit |
| --- | --- | --- |
| Core and worker | `/Users/jasonconigliari/botster-sessions/botster-core-foundation-stale-mode` | `bf6e7d996bca2786ad4142c870a13c57a490e241` |
| Hub and Hub client | `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1787600679_990088-rc1` | `02fa0a7a4c32f8666827e578ee4a28fc3b1b98ca` |
| Web | `/private/tmp/botster-web-foundation.sm0cZt/web` | `45a6eb696a7e472ade870d482d08c6f0bf1e37c0` |
| TUI | `/Users/jasonconigliari/botster-sessions/git-github.com-trybotster-botster-tui-project-pipelines-ticket_1788460430_647093` | `88cc23e2b4cef4178aeb48e95ec9a5549ba15ebe` |

Verify each starting identity before assignment.
Published branches do not necessarily match these local commits.
Do not reset a worktree to make this table true.

## Required final ownership

| Owner | Owns | Must not own |
| --- | --- | --- |
| Session worker | PTY, process survival, authoritative terminal parser, query replies, modes, consistent snapshots and readback | Hub product policy |
| Core and CoreDaemon | Generic lifecycle, fair routing, input ordering, bounded storage mechanisms, worker recovery, completion identities | A second server terminal parser or product retention policy |
| Hub | Trusted admission, credentials, retention policy, supervision, package composition, transport routing | Terminal parsing, blocking shared terminal progress, duplicate Core mechanisms |
| Hub client | Shared host-control schema and Rust client transport contract | TUI application state or a terminal model |
| Web | Browser connection owner, attachment generations, bounded input, terminal rendering and paint scheduling | Server terminal authority or protocol compatibility branches |
| TUI | Nonblocking application loop, connection owner, terminal projection and paint scheduling | Blocking request calls on the application thread |
| TUI Kit | Generic layout, focus, widgets, and input primitives | Botster lifecycle, session authority, or Hub transport policy |
| Lua plugins | Product composition through narrow host capabilities | Basic terminal liveness prerequisites |

## Contract requirements

### Progress and completion

The shared owner must not wait for worker startup, PTY output, snapshot encoding, filesystem access, or network responses.
Operations that can wait need explicit pending state, identity, cancellation, and an absolute deadline.
Completion must wake the owner.
Each owner turn has bounded work.
A flooded session or stalled client must not prevent sibling progress.
Routing work must follow the ready sessions and routes, not repeated scans of all sessions.

The final host-control protocol must support safe completion correlation.
Root and the architect will select the exact envelope before implementers change producers or consumers.
Do not preserve an uncorrelated legacy protocol through a fallback path.

### Terminal state and transport

The worker is the single server authority for terminal state.
Clients retain their own rendering models; these are not duplicate server authority.
Snapshots and readback must describe the same authoritative output boundary.
Adoption after Hub restart must not replay output through a second server parser.

Encode terminal frames once into immutable shared bytes where the process boundary permits sharing.
Hub treats terminal payloads as opaque.
Do not parse and reserialize a frame solely for routing or budget accounting.
Transport accounting includes retained bytes, not only item counts.

Natural exit must preserve accepted final output and ordered exit delivery.
Hard close must abandon unsent frames without replay.
A partially written stream envelope must finish or the affected stream must end.
Close must not wait on transport I/O or a writer-held lock.
Do not weaken sibling isolation to simplify cleanup.

### Bounds and retention

Every queue with variable payload sizes needs item and byte limits.
Every limit needs an explicit admission, backpressure, cancellation, or resynchronization outcome.
Do not drop arbitrary terminal bytes and continue the same decoder state.

Hub chooses finite retention defaults.
Core exposes accounting and eviction mechanisms.
The shared contract must specify restart behavior for live and ended sessions.
Do not silently promise persisted ended history if only live worker state survives.
Readback must not clone an entire retained snapshot to return a small field.

### Clients

Connection establishment, request completion, input, output, timers, and shutdown must not block painting.
Use bounded command and event storage with generation checks.
Apply terminal data in order and refresh projection at most once per required paint.
Remove per-cell string allocation when a borrowed symbol is sufficient.

Keep semantic paste distinct from key input.
Preserve byte-exact delivery, mode freshness, cancellation, and visible errors.
Remove per-keystroke acknowledgement serialization where the final input contract permits safe bounded batching.
Do not introduce speculative retries of input whose delivery is unknown.

### Hub background work

Catalog refresh must occur outside terminal owner turns and publish immutable generation-tagged results.
Keyed store reads must not decode every record in a namespace.
Use one transactional storage implementation for the final contract.
Do not introduce old-store migration or dual reads for an unused format.
Socket recovery must use authoritative ownership and bounded probes.
A failed handshake does not authorize deleting an unknown live listener.

## Completion criteria

The implementation must include production wiring, dependent consumers, deletion of replaced paths, and updated current documentation.
Dead APIs and old tests must not remain as an alternate implementation.
Historical evidence can remain outside runtime paths.

After the coherent replacement is reviewed, verify actual first-party user paths on one recorded stack.
Required behavior includes spawn, attach, input, large paste, resize, output, natural exit, hard close, reconnect, and live-worker adoption.
Include a flooded session, a stalled client, and a responsive sibling.
Measure optimized process-tree cost and interactive latency separately from correctness tests.
Do not claim that a passing unit count proves the final architecture.

## Known incomplete evidence

The previous stack had an unresolved intermittent reserved-channel open timeout in a lifecycle test.
The isolated test and one WebRTC subset passed.
Those results neither establish a production defect nor clear final acceptance.
Preserve the evidence for the final stack; do not continue the cancelled diagnostic campaign.

## Binding implementation assignments

Pending the consolidated Fable proposal and root decision.
No implementer may invent a second version of a shared contract while this section is pending.
