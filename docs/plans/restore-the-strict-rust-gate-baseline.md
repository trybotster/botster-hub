# Plan: restore the strict Rust gate baseline

Ticket: `ticket_1787667162_566252` — "Hub: restore the strict Rust gate baseline"
Run: `run_1787667183_365249`
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)
Revision: 3. Revision 1 was returned by Plan Review `review_1787668688_184116`
(`changes_required`). Revision 2 was approved by Plan Review
`review_1787669695_148343`. Implement resynced this artifact after change-admission
rule 3 admitted one additional clippy repair. The "Implement resync" section records
that admission.

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`)
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Repository path resolved from `list_spawn_targets`, not from the process working
  directory: `/Users/jasonconigliari/Projects/botster-hub`.
- Run worktree: `.../botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1787667162_566252`
- Branch: `project-pipelines/ticket_1787667162_566252`
- Base commit: `55f620d`. Verified after `git fetch origin --prune`:
  `origin/main` and `55f620d` both resolve to `55f620dfd3f07cbdf889ba6abd3c3e75e1ef117e`.
  Plan Review independently confirmed the same base.

## Repository playbook loaded

- [[botster-hub-playbook]] — the botster-hub ownership charter. This is the ownership
  authority for this plan.

## Other role/surface playbooks and atomic notes loaded

Role playbooks:

- [[planner-playbook]]
- [[botster-planner-playbook]]

Required Botster planning context, from the [[botster-planner-playbook]] "Must Load"
list:

- [[botster-architecture]] — the Botster domain map and source of architectural truth.
- [[cli-patterns]] — Rust CLI, TUI, PTY, and terminal-layer constraints.
- [[spa-patterns]] — React/Catalyst and entity-store frontend constraints. **No affected
  surface.** This ticket changes no browser, DTO, or entity-frame wire shape, so this
  overlay adds no acceptance check. It is recorded as loaded and not applicable.
- [[botster orchestration should spawn agents with explicit target ids]] — the plan binds
  this run to `tgt_7e208a0c76a44980a83b63af976b1f22` explicitly.
- [[botster orchestration prompts must bind agents to explicit worktrees]] — every gate
  command in this plan runs from the named run worktree, never an ambient directory.
- [[project-pipelines-playbook]] — loaded for workflow evidence discipline, because Plan
  completion evidence and vault checklist policy are in scope for this visit. No Project
  Pipelines package or plugin **source path** is in scope.

Botster layer touched: **Hub Rust host kernel only.** No Lua core, plugin, session or
client worker, TUI, React SPA, Rails relay, MCP, or docs-surface layer changes.

Targeted atomic notes:

- [[express scope limits as invariants not closed enumerations]] — governs the Scope
  section. Revision 1 violated this note directly.
- [[rust repo strict lints must be verified before dismissing warnings]]
- [[botster review agents must run verify strict gates not lighter equivalents]]
- [[test script required for rust tests not cargo test]]
- [[botster pipeline reviewers must bypass rtk summaries for cargo gate evidence]]
- [[botster Hub Rust stays a trusted host kernel]]
- [[the pinned Rust WebRTC peer cannot open a DataChannel created after the SCTP handshake]]
- [[botster runtime teardown lenses]] — loaded to classify the ticket. See
  "Runtime-teardown class" below. The class does **not** apply.

Not loaded, with reason: other repository charters, because the ticket target is a single
repository and the charter for it is loaded above.

## Context loaded

- `Cargo.toml` — workspace members, `[lints.rust]`, the pinned `botster-core` revision
  `7eafa470a18025895995bbedc20d34b58106a03b`, and `webrtc = "0.21.0-beta.2"`.
- `test.sh` — the repository wrapper. It runs
  `node packages/hub-test-support/scripts/sync-assets.mjs --check` and then
  `BOTSTER_ENV=test cargo test --workspace "$@"`. `--workspace` is load-bearing.
- `.github/workflows/ci.yml` — the authoritative definition of the "strict Rust gates".
- `src/local_webrtc.rs` — the `fmt` defect site.
- `src/package_entity_fanout.rs` — the `clippy` defect site, the enclosing function
  `parse_publish_mutation`, and the existing `#[cfg(test)]` module.
- `src/runtime.rs` — the production call site of `parse_publish_mutation`.
- Measured baselines on `55f620d` with a clean tracked worktree (see below).

## Measured pre-fix baseline

Toolchain used for every measurement:

- `rustc 1.97.0 (2d8144b78 2026-07-07)`
- `cargo 1.97.0`
- `zig 0.16.0`

### Toolchain trap that this plan must carry forward

The pipeline agent process exports `RUSTUP_TOOLCHAIN=1.92.0`. `rustup show` reports
`active because: overridden by environment variable RUSTUP_TOOLCHAIN`, and
`rustup override list` prints nothing. CI pins `1.97.0`. The two toolchains **disagree on
this ticket's clippy defect**:

| Command | Toolchain | Exit |
| --- | --- | --- |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | 1.92.0 (ambient) | `0` — false green |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | 1.97.0 (CI pin) | `101` — reproduces |

Every Implement, Review, and Verify cargo command for this ticket must set
`RUSTUP_TOOLCHAIN=1.97.0` explicitly. A clean clippy run without that override is not
evidence.

### Gate 1 — `cargo fmt --all -- --check`

Pre-fix result: exit `1`. One diff hunk:

```
Diff in .../src/local_webrtc.rs:7710
```

The hunk is the `let remote = timeout(runtime.as_ref(), Duration::from_secs(10), async { ... })`
call inside `post_handshake_data_channel_opens_and_delivers_bytes`. `rustfmt` wants the
closing-argument form on one line instead of the expanded multi-line argument list. The
change is pure whitespace and line breaking.

### Gate 2 — `cargo clippy --workspace --all-targets --locked -- -D warnings`

Pre-fix result under 1.97.0: exit `101`.

```
error: this `if` can be collapsed into the outer `match`
   --> src/package_entity_fanout.rs:515:13
    |
515 | /             if id.0.is_empty() {
516 | |                 return Err("entity_publish remove requires non-empty id".to_string());
517 | |             }
    | |_____________^
    = note: `-D clippy::collapsible-match` implied by `-D warnings`

error: could not compile `botster-hub` (lib) due to 1 previous error
error: could not compile `botster-hub` (lib test) due to 1 previous error
```

Exactly one diagnostic. It names the `EntityFrame::Remove` arm of the `match &frame`
block inside `parse_publish_mutation` (`src/package_entity_fanout.rs:491`). The sibling
`EntityFrame::Patch` arm at line 508 has the same shape and is **not** reported by clippy
1.97.0.

### Gate 3 — `./test.sh --locked`

Plan Review measured this gate independently on the same base after building the locked
session worker and Hub binary. Result: **pass** across the full workspace, including 493
library tests and 318 daemon lifecycle tests with 2 ignored. This retires the revision-1
unknown about suite health on this host. Implement must still re-run the gate after the
fix, because the baseline result does not prove the post-fix result.

## Scope

Stated as invariants, per [[express scope limits as invariants not closed enumerations]].
Revision 1 used a fixed hunk count, which conflicted with the conditional `Patch`-arm
repair the same plan permitted.

### Hard invariants (absolute)

The ticket changes **no** runtime behavior, public or Lua-visible API, module layout,
dependency, lockfile entry, Core pin, protocol revision, DTO, capability, or feature
token. It adds no file outside `docs/plans/`, moves no responsibility between modules,
leaves no forwarding wrapper, and adds no `#[allow(...)]` suppression for either
diagnostic. `parse_publish_mutation` keeps its exact rejection conditions and its exact
error strings.

### Change-admission rule

Every changed line must trace to exactly one of:

1. The `cargo fmt` defect at `src/local_webrtc.rs:7710`.
2. The `clippy::collapsible_match` defect at `src/package_entity_fanout.rs:515`.
3. A **new** diagnostic that the exact strict command
   (`cargo clippy --workspace --all-targets --locked -- -D warnings` under `1.97.0`)
   reports **after** a change made under rule 1 or 2. This covers the `Patch` arm at line
   508 if clippy reports it once the `Remove` arm is fixed.
4. Behavior-invariance proof made necessary by the change under rule 2 — that is, the
   focused `Remove` test described in the acceptance checks.
5. This plan artifact.

No other change is admissible. Adjacent cleanup, style symmetry, and opportunistic
refactoring are all excluded, because none of them satisfy the rule. In particular, the
`Patch` arm at line 508 must **not** be touched for symmetry with the `Remove` arm; it may
be touched only under rule 3, with the triggering diagnostic recorded verbatim.

### Planned changes (leads, not a closed list)

- `src/local_webrtc.rs` — run `cargo fmt`; whitespace only.
- `src/package_entity_fanout.rs` — move the emptiness test into a match guard on the
  `EntityFrame::Remove` arm of `parse_publish_mutation`:

  ```rust
  EntityFrame::Remove {
      entity_type: _, id, ..
  } if id.0.is_empty() => {
      return Err("entity_publish remove requires non-empty id".to_string());
  }
  ```

  This is behavior-identical. Today a `Remove` frame with a non-empty id enters the arm
  and the body does nothing. With the guard it fails the guard and falls through to the
  existing `_ => {}` arm, which also does nothing. The error string, the error type, and
  the rejection condition are unchanged.
- `src/package_entity_fanout.rs` `#[cfg(test)]` module — one focused test proving the
  `Remove` behavior is invariant across the guard change. See acceptance criterion 6.
- `tests/hub_daemon_lifecycle/sessions.rs` — only if the exact 1.97.0 strict command
  reports a new diagnostic there after the `Remove` repair. Implement confirmed that
  report and applied the identical match-guard transformation.
- `docs/plans/restore-the-strict-rust-gate-baseline.md` — this artifact.

## Repository ownership boundaries and cross-repo dependencies

- Both source files are `botster-hub` Rust host-kernel source. `src/package_entity_fanout.rs`
  is Hub-owned package entity admission policy, which the charter assigns to Hub
  ([[botster Hub Rust stays a trusted host kernel]]). `src/local_webrtc.rs` is Hub
  transport. Neither belongs to `botster-core`.
- No Core, Web, TUI, hub-client, or ui-contract surface changes. No DTO, protocol
  revision, capability, or feature-token change. No published npm or Cargo artifact
  changes.
- Cross-repository dependencies: **none**. The pinned Core revision
  `7eafa470a18025895995bbedc20d34b58106a03b` and `webrtc 0.21.0-beta.2` stay exactly as
  they are. No dependency ticket is registered against another target, because this ticket
  needs no upstream work. Plan Review independently agreed.
- Sibling tickets on the same target (`ticket_1787600674_500120`,
  `ticket_1787600682_233928`, `ticket_1787603671_590198`, `ticket_1787600679_990088`,
  `ticket_1787600691_401181`) all touch `local_webrtc.rs` or `daemon_transport.rs`. This
  ticket exists so those tickets inherit green strict gates. It must merge as a small
  independent diff and must not absorb any of their work.

## Assumptions and unknowns

Assumptions, stated explicitly:

1. "Strict Rust gates" means the exact CI steps in `.github/workflows/ci.yml`:
   `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
   and `./test.sh --locked`, run on Rust `1.97.0` with Zig `0.16.0`.
2. The ticket's phrase "around line 515" refers to the `EntityFrame::Remove` arm. The
   measured diagnostic confirms line 515 exactly, so no interpretation is needed.
3. `cargo fmt` is authoritative for the `local_webrtc.rs` hunk. The Implementer runs the
   formatter rather than hand-editing, so the result matches CI byte for byte.
4. A focused `Remove` test is required work under change-admission rule 4, not adjacent
   cleanup. Rationale: the ticket demands "Keep the package entity fanout behavior
   unchanged", the guard changes arm selection, and no existing test covers the `Remove`
   arm. Without the test the invariance claim has no proof. Revision 1 asserted an
   existing test and simultaneously forbade adding one; both were wrong.

Unknowns the Implementer or Reviewer must resolve:

1. Whether fixing the `Remove` arm makes clippy then report the `Patch` arm at line 508.
   Clippy does not always report every instance of a lint in one pass. Resolution: rerun
   the strict gate after the fix. Change-admission rule 3 already covers the outcome, so
   neither answer blocks the ticket. **Resolved during Implement:** clippy 1.97.0 did
   **not** report the `Patch` arm. The next diagnostic was
   `tests/hub_daemon_lifecycle/sessions.rs:3126` (`collapsible_match` on a
   `DaemonEntityFrame::Upsert` prefix check). That site exists unchanged on `55f620d`
   and was hidden because the pre-fix command stopped after the lib compile failure.

Resolved since revision 1:

- Base freshness. `git fetch origin --prune` confirms `origin/main == 55f620d`.
- Suite health. Plan Review measured `./test.sh --locked` green on this base.

## Affected surfaces/files

| File | Change | Behavior change |
| --- | --- | --- |
| `src/local_webrtc.rs` | `cargo fmt` on one hunk at ~line 7710, inside the `post_handshake_data_channel_opens_and_delivers_bytes` test | None. Whitespace only. |
| `src/package_entity_fanout.rs` | `EntityFrame::Remove` arm at 512-518 of `parse_publish_mutation` gains a match guard; one focused test added to the existing `#[cfg(test)]` module | None. Same rejection condition, same error string. |
| `tests/hub_daemon_lifecycle/sessions.rs` | `DaemonEntityFrame::Upsert` arm at 3125-3129 gains a match guard. Admitted by change-admission rule 3 after the exact 1.97.0 strict command reported it. | None. Same prefix filter; non-matching ids still fall through to `_ => {}`. |
| `docs/plans/restore-the-strict-rust-gate-baseline.md` | Plan artifact, revision 3 | None. |

Production path: `parse_publish_mutation` is called from
`HubRuntime::admit_package_entity_publish_inner` at `src/runtime.rs:2265`, which serves
the Lua-visible `botster.entity_publish` admission path. That is the production entry
point whose behavior must stay identical, and it is what acceptance criterion 6 pins.

## Risks

1. **False green from the ambient toolchain.** `RUSTUP_TOOLCHAIN=1.92.0` is exported into
   every pipeline agent shell in this repository. Clippy 1.92.0 does not report this
   defect. An Implement or Verify agent that runs the strict gate without the override
   will report a clean gate on unfixed code, and the ticket will merge without doing its
   job. Mitigation: every cargo command in Implement, Review, and Verify prefixes
   `RUSTUP_TOOLCHAIN=1.97.0`, and every gate record quotes the `rustc --version` line
   captured in the same shell.
2. **Cascading clippy diagnostic after the `Remove` repair.** Covered by change-admission
   rule 3. The `Patch` arm at line 508 was **not** reported. The next diagnostic was
   `tests/hub_daemon_lifecycle/sessions.rs:3126`. Implement applied the identical guard
   transformation there, recorded the diagnostic verbatim, and changed nothing else.
3. **Silent behavior change from the guard.** A match guard changes arm selection, not
   just formatting. Mitigation: the fall-through target is the existing `_ => {}` arm,
   which is a no-op, so the observable result is identical. Acceptance criterion 6 proves
   this executably. Review must additionally confirm the `_` arm still exists, is still a
   no-op, and that no arm between `Remove` and `_` could capture a `Remove` frame.
4. **A vacuous whitespace proof.** Revision 1 used `git diff -w -- src/local_webrtc.rs`,
   which compares the worktree with the index and is therefore empty after any commit —
   it could never fail, even on a behavior change. Mitigation: acceptance criterion 7 now
   compares the stable base with `HEAD`. rustfmt call-collapse still produces a non-empty
   `git diff -w`; Review inspects that the hunk is only the `timeout(...)` call.
5. **Formatter drift.** Running `cargo fmt` under a different rustfmt than CI could
   reformat unrelated code. Mitigation: run `cargo fmt` under `1.97.0`, then confirm with
   `git diff --stat 55f620d...HEAD` that only the expected files changed.
6. **Flaky suite attribution.** `./test.sh --locked` runs the whole workspace including
   load-sensitive daemon lifecycle tests. A failure could be host noise rather than this
   diff. Mitigation: the base is now known green, so a post-fix failure is attributable;
   run on a quiet host, and on failure rerun the failing target with `--test-threads=1`
   before attributing. Never accept a pre-existing failure as a blanket excuse.
7. **Scope creep pressure.** The two touched files are large and are the subject of five
   open sibling tickets. Mitigation: acceptance criterion 8 requires every changed line to
   trace to the change-admission rule.

## Acceptance checks/tests

Run every command from the run worktree named above with `RUSTUP_TOOLCHAIN=1.97.0` and raw
output (`rtk proxy -- ...`), because cargo diagnostics and exit codes are gate-bearing
([[botster pipeline reviewers must bypass rtk summaries for cargo gate evidence]]).

Preconditions:

1. `git status --short` is empty for tracked files before gate runs.
2. Tracked `.gitignore` is non-empty and matches HEAD. Verified during Plan: 53 bytes,
   clean.
3. The worktree path contains no `:`. Verified during Plan, so no `CARGO_TARGET_DIR`
   override is required for correctness. A separate target directory is still permitted to
   keep the 1.92.0 and 1.97.0 artifact sets apart, but it **must stay under the worktree**.
   `executable_from_this_worktree` accepts only a `botster-session-worker` whose argv0
   starts with `CARGO_MANIFEST_DIR`. An out-of-worktree directory such as `/tmp/...`
   makes spawn census tests panic with
   `timed out waiting for live this-worktree session-worker after Spawn`. Implement
   measured that failure under `/tmp/botster-hub-ticket-1787667162-1970` and reran gates
   with `<worktree>/target/1.97.0`, which stays under the worktree and under ignored
   `/target`.
4. `git fetch origin --prune`, then confirm `origin/main` still equals `55f620d`. If it
   has moved, use the refreshed `origin/main` as the diff base and say so in the evidence.
5. `rustc --version` prints `1.97.0` and `zig version` prints `0.16.0`, captured in the
   same shell as the gates.

Gate commands, in order:

```sh
export RUSTUP_TOOLCHAIN=1.97.0
rustc --version                     # must print 1.97.0
zig version                         # must print 0.16.0
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo build --locked --bin botster-hub
rtk proxy -- cargo fmt --all -- --check
rtk proxy -- cargo clippy --workspace --all-targets --locked -- -D warnings
node packages/hub-test-support/scripts/sync-assets.mjs --check
./test.sh --locked
rtk proxy -- git diff --check 55f620d...HEAD
```

The two `cargo build --locked` steps precede `./test.sh --locked` because the charter
requires the session worker to exist before the locked wrapper runs on a fresh target.

Acceptance criteria:

1. **Pre-fix failure is recorded for each gate.** `fmt` exit `1` naming
   `src/local_webrtc.rs:7710`. `clippy` exit `101` naming
   `src/package_entity_fanout.rs:515` with `collapsible_match`. Both are captured in this
   plan under "Measured pre-fix baseline" and must be re-attached as Implement evidence
   with their exit codes.
2. **`cargo fmt --all -- --check` exits `0`.**
3. **`cargo clippy --workspace --all-targets --locked -- -D warnings` exits `0`**, with
   the `rustc 1.97.0` version line captured in the same evidence block.
4. **`./test.sh --locked` exits `0`**, with the executed workspace target list recorded. A
   bare `cargo test` is not a substitute
   ([[test script required for rust tests not cargo test]]).
5. **`git diff --check 55f620d...HEAD` exits `0`** — no committed whitespace artifacts.
6. **Behavior invariance for package entity fanout, proven executably.** No existing test
   covers the `Remove` arm: the only validation test in
   `src/package_entity_fanout.rs` is `upsert_validation_requires_extractable_record_id`
   (line 734), which covers `EntityFrame::Upsert` only. A repository-wide search finds no
   assertion on the string `"entity_publish remove requires non-empty id"` outside the
   production source. The Implementer therefore adds one focused test in the existing
   `#[cfg(test)]` module of `src/package_entity_fanout.rs`, in the style of the existing
   test, calling `parse_publish_mutation` directly and asserting both arms of the
   behavior:
   - an `entity_remove` frame with an empty `id` returns `Err`, and the message equals
     `"entity_publish remove requires non-empty id"`;
   - an `entity_remove` frame with a non-empty `id` returns `Ok`, which proves the
     non-empty case still falls through to `PackageEntityMutation::from_entity_frame`
     rather than being rejected by the new guard.

   The test must fail if the guard is written with an inverted condition. No existing test
   is edited or deleted. `upsert_validation_requires_extractable_record_id` still passes
   unchanged.
7. **Behavior invariance for the WebRTC post-handshake regression.**
   `post_handshake_data_channel_opens_and_delivers_bytes` passes. rustfmt collapsed the
   `timeout(runtime.as_ref(), Duration::from_secs(10), async { ... })` call from a
   multi-line argument list to one line. `git diff -w 55f620d...HEAD -- src/local_webrtc.rs`
   is **not** empty, because git treats moved tokens as content even when only newlines
   and indentation changed. Review must confirm the hunk is only that call, that no
   identifier, string, numeric literal, or operator was added or removed, and that the
   test passed. The comparison still uses `55f620d...HEAD`, not a worktree-vs-index
   diff.
8. **Change-admission compliance.** `git diff --stat 55f620d...HEAD` lists only files
   admitted by the five-rule change-admission rule. The current admitted set is
   `src/local_webrtc.rs` (rule 1), `src/package_entity_fanout.rs` (rules 2 and 4),
   `tests/hub_daemon_lifecycle/sessions.rs` (rule 3), and this plan artifact (rule 5).
   Review maps every changed line to one of the five rules and names which rule each hunk
   satisfies. The rule-3 hunk must quote this triggering diagnostic:

   ```
   error: this `if` can be collapsed into the outer `match`
       --> tests/hub_daemon_lifecycle/sessions.rs:3126:25
   ```

   Clippy 1.97.0 did not report the `Patch` arm after the `Remove` repair. That arm stays
   untouched.

### Downstream proof

None required. The charter requires live Hub, downstream consumer, or exact-binary proof
when Hub admission policy, DTOs, capabilities, protocol revisions, or supervision behavior
change. This ticket changes none of those: the diff is whitespace plus a match guard with
an identical rejection condition, proven by acceptance criterion 6 at the production entry
point `src/runtime.rs:2265`. The downstream benefit — sibling feature tickets inheriting
green strict gates — is proven by acceptance criteria 2, 3, and 4 on this branch, not by
rebuilding a consumer graph. Plan Review independently agreed that no downstream consumer
proof is required.

### Runtime-teardown class

`teardown_class_applies`: **no**. Plan Review independently agreed.

[[botster runtime teardown lenses]] was loaded and evaluated because the diff touches
`src/local_webrtc.rs`. The class does not apply, for these reasons:

- The `local_webrtc.rs` change is whitespace produced by `cargo fmt`. It changes no peer
  lifecycle, signaling, peer map, close path, ownership set, or bound.
- rustfmt changed only the `timeout(...)` call inside
  `post_handshake_data_channel_opens_and_delivers_bytes`. That is a mechanical proof
  over the committed diff that no teardown semantics moved (acceptance criterion 7).
  `git diff -w` is not empty because rustfmt collapsed argument-list newlines.
- The `package_entity_fanout.rs` change is an admission-time validation guard on a publish
  frame. It creates and destroys no durable ownership, holds no lock, and touches no
  session, peer, or worker teardown.
- No SessionIo, ClientWorker, multi-peer ownership, CPU/battery/FD spin, or terminal-state
  divergence surface is modified.

The lens fields are therefore recorded as not-applicable, with the invariant that carries
their intent: the existing WebRTC post-handshake regression test must keep passing with a
byte-identical token stream.

## Plan Review corrections (revision 1 → revision 2)

| Finding | Severity | Fix in this revision |
| --- | --- | --- |
| `finding_1787668688_137780` — acceptance cited an absent function and an absent test | high | Confirmed. `validate_publish_frame` does not exist; the enclosing function is `parse_publish_mutation` (`src/package_entity_fanout.rs:491`), now named throughout. Confirmed no `Remove` test exists — the only validation test is `upsert_validation_requires_extractable_record_id`. Acceptance criterion 6 now **requires** a focused `Remove` test instead of forbidding one, admitted by change-admission rule 4. |
| `finding_1787668688_938680` — whitespace proof did not inspect the committed diff | high | Confirmed by running it: `git diff -w -- src/local_webrtc.rs` exits `0` with empty output today, before any fix exists, so it could never fail. Criteria 5, 7, and 8 now all use the `55f620d...HEAD` base, with a precondition that re-verifies `origin/main` against that base. |
| `finding_1787668688_754140` — fixed hunk counts conflicted with required strict-gate work | high | Confirmed, and it violated [[express scope limits as invariants not closed enumerations]], which revision 1 had not loaded. Scope is now a hard invariant plus a five-rule change-admission rule. The planned changes are labelled leads, not a closed list. Acceptance criterion 8 enforces traceability instead of a count. |
| `finding_1787668688_156757` — required Botster planning context missing | medium | Confirmed against the [[botster-planner-playbook]] "Must Load" list. [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], and the two orchestration binding notes are now loaded and recorded, with `spa-patterns` explicitly marked as having no affected surface. [[botster-hub-playbook]] remains the ownership authority. |
| `finding_1787668688_973044` — Plan completion and checklist evidence incomplete | process | [[project-pipelines-playbook]] loaded for workflow discipline. The existing default items of `checklist_1787667603_524858` are updated with evidence and marked done rather than duplicated. No second checklist is created. All required gate evidence fields are resubmitted, including `artifact_1787667665_874982` and `checklist_1787667603_524858`. |

## Implement resync (revision 2 → revision 3)

After the `Remove`-arm repair, the exact command
`RUSTUP_TOOLCHAIN=1.97.0 cargo clippy --workspace --all-targets --locked -- -D warnings`
exited `101` with this new diagnostic:

```
error: this `if` can be collapsed into the outer `match`
    --> tests/hub_daemon_lifecycle/sessions.rs:3126:25
     |
3126 | /                         if id.starts_with("focused-idle-session-") {
3127 | |                             seen.insert(id);
3128 | |                         }
     | |_________________________^
     = note: `-D clippy::collapsible-match` implied by `-D warnings`
```

The `Patch` arm at `src/package_entity_fanout.rs:508` was not reported and stays
untouched. The `sessions.rs` site exists on base `55f620d`. Change-admission rule 3
admits the identical match-guard transformation. Hard invariants are unchanged: no
runtime behavior, API, module, dependency, Core pin, protocol, DTO, or `#[allow]`
change. Criterion 8 now names the admitted file set instead of a closed three-file
list.

## Vault gaps worth capturing

1. **`RUSTUP_TOOLCHAIN=1.92.0` in pipeline agent shells produces false-green Rust gates in
   `botster-hub`.** The highest-value gap. The env var is inherited from the Botster CLI
   process, `rustup override list` shows nothing, and `rustup show` is the only place the
   override is visible. Clippy 1.92.0 exits `0` on code that clippy 1.97.0 fails. Any
   Review or Verify agent that trusts a bare `cargo clippy` in this repository can approve
   unfixed lint defects. Candidate note: "botster-hub pipeline shells override
   RUSTUP_TOOLCHAIN below the CI pin". This sharpens
   [[rust repo strict lints must be verified before dismissing warnings]], which today
   covers warning strictness and command scope but not toolchain identity.
2. **`git diff -w -- <path>` is a vacuous whitespace-invariance proof.** It compares the
   worktree with the index, so it passes trivially once work is committed. A
   whitespace-only claim must compare the stable base with `HEAD`. Candidate note:
   "whitespace-invariance proofs need a base-to-HEAD range, not a worktree diff". This is
   a general planning gotcha, not a Botster-specific one.
3. **`clippy::collapsible_match` reports one compile unit at a time.** Confirmed during
   Implement: after the `Remove` arm was repaired, clippy did not report the sibling
   `Patch` arm. It next reported an unrelated pre-existing `collapsible_match` in
   `tests/hub_daemon_lifecycle/sessions.rs:3126` because the pre-fix command never reached
   that test crate. A plan that sizes a lint repair from the first diagnostic can hide
   later compile units. Candidate note: "strict clippy can hide later crate diagnostics
   behind the first compile failure".
4. **Strict-gate baseline repairs belong in their own ticket.** The owner's rule is
   recorded in project memory but has no vault note. This ticket is the worked example.
