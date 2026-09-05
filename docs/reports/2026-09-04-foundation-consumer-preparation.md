# Foundation consumer preparation

Status: source preparation only. No test or build has run in this worktree.

The root coordinator owns this isolated worktree:
`/private/tmp/botster-registry-consumer.tuGcwH/hub`.
Branch: `foundation/registry-identity-consumer`.
Base: Hub `11facecf371271907f7d20d83e390601c4011966`.
The active Hub integration worktree is unchanged by this preparation.

## Changes

`src/update.rs` obtains records through `SessionRegistry::load` instead of constructing registry filenames.
It checks the requested identity before using recovery data.
The updater still blocks incompatible-worker termination when identity lookup fails.
Two tests cover Unicode and punctuation IDs, missing identity, and independently retained IDs that collided under the old filename encoding.
The collision test requires the new Core registry implementation.

`src/runtime.rs` includes the previously reviewed `ExplicitResizeBusy` classification and its regression.
This is the same narrow source change already tested in the isolated resize validation export.
That earlier test result does not prove this worktree or its eventual dependency combination.

## Required pairing

The worktree still pins Core `93acae3`.
The new classification cannot compile against that old Core error enum.
Do not treat this preparation as a buildable or merged candidate until the Core pin changes.
The intended registry candidate is Core `cfc51fb7a7528e6c0c848a81375c514ff7a468e7`, pending executable verification and final review.
Core main already contains the resize repair and close clarification at `68ca23d`, but not the registry repair.

After the active Hub integration lands, combine its final source with these consumer changes.
Use the exact approved Core registry revision for the dependency update.
Run the updater identity tests and resize classification test against that combination.
Preserve the expected failing collision test against old Core as a negative control when practical.
Do not replace the active integration ticket's fixed Core input with this branch.

Rust formatting and `git diff --check` passed. Compilation, runtime tests, and merge remain pending.
