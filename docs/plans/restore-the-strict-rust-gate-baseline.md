# Plan: restore the strict Rust gate baseline

Ticket: `ticket_1787667162_566252` — "Hub: restore the strict Rust gate baseline"
Run: `run_1787667183_365249`
Pipeline: Botster Stack Delivery (`botster_stack_delivery`)

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`)
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Repository path resolved from `list_spawn_targets`, not from the process working
  directory: `/Users/jasonconigliari/Projects/botster-hub`.
- Run worktree: `.../botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1787667162_566252`
- Branch: `project-pipelines/ticket_1787667162_566252`
- Base commit: `55f620d` ("Merge ticket: Hub: upgrade WebRTC for post-handshake DataChannel creation")

## Repository playbook loaded

- [[botster-hub-playbook]] — the botster-hub ownership charter.

## Other role/surface playbooks and atomic notes loaded

Role playbooks:

- [[planner-playbook]]
- [[botster-planner-playbook]]

Atomic notes:

- [[rust repo strict lints must be verified before dismissing warnings]]
- [[botster review agents must run verify strict gates not lighter equivalents]]
- [[test script required for rust tests not cargo test]]
- [[botster pipeline reviewers must bypass rtk summaries for cargo gate evidence]]
- [[botster Hub Rust stays a trusted host kernel]]
- [[the pinned Rust WebRTC peer cannot open a DataChannel created after the SCTP handshake]]
- [[botster runtime teardown lenses]] — loaded to classify the ticket. See
  "Runtime-teardown class" below. The class does **not** apply.

Not loaded, with reason:

- [[project-pipelines-playbook]] — no Project Pipelines package or plugin path is in
  scope. This ticket changes only `botster-hub` Rust source.
- Other repository charters — the ticket target is a single repository.

## Context loaded

- `Cargo.toml` — workspace members, `[lints.rust]`, the pinned `botster-core` revision
  `7eafa470a18025895995bbedc20d34b58106a03b`, and `webrtc = "0.21.0-beta.2"`.
- `test.sh` — the repository wrapper. It runs
  `node packages/hub-test-support/scripts/sync-assets.mjs --check` and then
  `BOTSTER_ENV=test cargo test --workspace "$@"`. `--workspace` is load-bearing.
- `.github/workflows/ci.yml` — the authoritative definition of the "strict Rust gates".
- `src/local_webrtc.rs` — the `fmt` defect site.
- `src/package_entity_fanout.rs` — the `clippy` defect site.
- Measured baselines on `55f620d` with a clean tracked worktree (see below).

## Measured pre-fix baseline (recorded during Plan)

Toolchain used for every measurement:

- `rustc 1.97.0 (2d8144b78 2026-07-07)`
- `cargo 1.97.0`
- `zig 0.16.0`

### Toolchain trap that this plan must carry forward

The pipeline agent process exports `RUSTUP_TOOLCHAIN=1.92.0`. `rustup show` reports
`active because: overridden by environment variable RUSTUP_TOOLCHAIN`. CI pins
`1.97.0`. The two toolchains **disagree on this ticket's clippy defect**:

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
closing-argument form on one line instead of the expanded multi-line argument list.
The change is pure whitespace and line breaking.

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
block in `validate_publish_frame`. The sibling `EntityFrame::Patch` arm at line 508 has
the same shape and is **not** reported by clippy 1.97.0.

## Scope

In scope, and nothing else:

1. `src/local_webrtc.rs` — apply `cargo fmt` to the single reported hunk at line 7710.
   Whitespace only. No token, identifier, assertion, or timeout value changes.
2. `src/package_entity_fanout.rs` — remove the `clippy::collapsible_match` diagnostic at
   line 515 by moving the emptiness test into a match guard on the `EntityFrame::Remove`
   arm:

   ```rust
   EntityFrame::Remove {
       entity_type: _, id, ..
   } if id.0.is_empty() => {
       return Err("entity_publish remove requires non-empty id".to_string());
   }
   ```

   This is behavior-identical. Today a `Remove` frame with a non-empty id enters the arm
   and the body does nothing. With the guard it falls through to the existing `_ => {}`
   arm and still does nothing. The error string, the error type, and the rejection
   condition are unchanged.
3. The plan artifact at `docs/plans/restore-the-strict-rust-gate-baseline.md`.

Explicitly out of scope:

- Any change to package entity fanout behavior, admission, validation order, or error
  strings.
- Any change to WebRTC post-handshake regression behavior, peer lifecycle, channel
  reservation, or test assertions.
- Reformatting the sibling `EntityFrame::Patch` arm at line 508 for style symmetry.
  Clippy 1.97.0 does not report it. Changing it is adjacent cleanup, which the ticket
  forbids. The one exception is stated under Risks.
- Any `#[allow(clippy::collapsible_match)]` suppression. The ticket asks to fix the
  warning, not to silence it.
- Extraction, module moves, forwarding wrappers, or any responsibility migration. The
  parent project forbids those outside their assigned tickets, and this ticket assigns
  none.
- Dependency, lockfile, Core pin, or `webrtc` version changes.
- Any repair of an unrelated gate failure discovered later. Report it as a separate
  ticket.

## Repository ownership boundaries and cross-repo dependencies

- Both files are `botster-hub` Rust host-kernel source. `src/package_entity_fanout.rs`
  is Hub-owned package entity admission policy, which the charter assigns to Hub
  ([[botster Hub Rust stays a trusted host kernel]]). `src/local_webrtc.rs` is Hub
  transport. Neither belongs to `botster-core`.
- No Core, Web, TUI, hub-client, or ui-contract surface changes. No DTO, protocol
  revision, capability, or feature-token change. No published npm or Cargo artifact
  changes.
- Cross-repository dependencies: **none**. The pinned Core revision
  `7eafa470a18025895995bbedc20d34b58106a03b` and `webrtc 0.21.0-beta.2` stay exactly as
  they are. No dependency ticket is registered against another target, because this
  ticket needs no upstream work.
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
3. `cargo fmt` is authoritative for hunk 1. The Implementer runs the formatter rather
   than hand-editing, so the result matches CI byte for byte.
4. `./test.sh --locked` is expected green on `55f620d` because the base commit is a merge
   of an already-verified ticket. This is an assumption, not a measurement — Plan did not
   run the full suite. Implement records the real result.

Unknowns the Implementer or Reviewer must resolve:

1. Whether fixing the `Remove` arm makes clippy then report the `Patch` arm at line 508.
   Clippy does not always report every instance of a lint in one pass. Resolution: rerun
   the strict gate after the fix. See Risks.
2. Whether `./test.sh --locked` is green on this host. The Hub lifecycle suite is
   load-sensitive. Resolution: run it on a quiet host and, on failure, isolate before
   attributing.

## Affected surfaces/files

| File | Change | Behavior change |
| --- | --- | --- |
| `src/local_webrtc.rs` | `cargo fmt` on one hunk at ~line 7710, inside the `post_handshake_data_channel_opens_and_delivers_bytes` test | None. Whitespace only. |
| `src/package_entity_fanout.rs` | `EntityFrame::Remove` arm at 512-518 gains a match guard | None. Same rejection condition, same error string. |
| `docs/plans/restore-the-strict-rust-gate-baseline.md` | New plan artifact | None. |

Expected diff size: two source hunks. Nothing else in `src/` changes.

## Risks

1. **False green from the ambient toolchain.** `RUSTUP_TOOLCHAIN=1.92.0` is exported into
   every pipeline agent shell in this repository. Clippy 1.92.0 does not report this
   defect. An Implement or Verify agent that runs the strict gate without the override
   will report a clean gate on unfixed code, and the ticket will merge without doing its
   job. Mitigation: every cargo command in Implement, Review, and Verify prefixes
   `RUSTUP_TOOLCHAIN=1.97.0`, and every gate record quotes the `rustc --version` line
   captured in the same shell.
2. **Cascading clippy diagnostic on the `Patch` arm.** If clippy reports line 508 after
   the `Remove` fix, the gate cannot pass without fixing it. That repair is then required
   by the gate, not adjacent cleanup, and it is in scope by the ticket's own success
   criterion. The Implementer applies the identical guard transformation, records the
   second diagnostic verbatim as evidence, and changes nothing else. If clippy does not
   report line 508, the `Patch` arm stays untouched.
3. **Silent behavior change from the guard.** A match guard changes arm selection, not
   just formatting. Mitigation: the fall-through target is the existing `_ => {}` arm,
   which is a no-op, so the observable result is identical. Review must confirm the `_`
   arm still exists and is still a no-op, and that no arm between `Remove` and `_` could
   capture a `Remove` frame.
4. **Formatter drift.** Running `cargo fmt` under a different rustfmt than CI could
   reformat unrelated code. Mitigation: run `cargo fmt` under `1.97.0`, then confirm with
   `git diff --stat` that only `src/local_webrtc.rs` changed and only at the expected
   hunk.
5. **Flaky suite attribution.** `./test.sh --locked` runs the whole workspace including
   load-sensitive daemon lifecycle tests. A failure here could be host noise rather than
   this diff. Mitigation: run on a quiet host; on failure, rerun the failing target with
   `--test-threads=1` before attributing, and never accept a pre-existing failure as a
   blanket excuse.
6. **Scope creep pressure.** The two touched files are large and are the subject of five
   open sibling tickets. Mitigation: the acceptance check below requires the final diff to
   contain exactly the two source hunks plus the plan artifact.

## Acceptance checks/tests

Run every command from the run worktree with `RUSTUP_TOOLCHAIN=1.97.0` and raw output
(`rtk proxy -- ...`), because cargo diagnostics and exit codes are gate-bearing
([[botster pipeline reviewers must bypass rtk summaries for cargo gate evidence]]).

Preconditions:

1. `git status --short` is empty for tracked files before gate runs.
2. Tracked `.gitignore` is non-empty and matches HEAD. Verified during Plan: 53 bytes,
   clean.
3. The worktree path contains no `:`. Verified during Plan, so no `CARGO_TARGET_DIR`
   override is required for correctness. A separate target directory is still permitted
   to keep the 1.92.0 and 1.97.0 artifact sets apart.
4. `rustc --version` prints `1.97.0` and `zig version` prints `0.16.0`, captured in the
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
rtk proxy -- git diff --check main...HEAD
```

The two `cargo build --locked` steps precede `./test.sh --locked` because the charter
requires the session worker to exist before the locked wrapper runs on a fresh target.

Acceptance criteria:

1. **Pre-fix failure is recorded for each gate.** `fmt` exit `1` naming
   `src/local_webrtc.rs:7710`. `clippy` exit `101` naming
   `src/package_entity_fanout.rs:515` with `collapsible_match`. Both are already captured
   in this plan under "Measured pre-fix baseline" and must be re-attached as Implement
   evidence with their exit codes.
2. **`cargo fmt --all -- --check` exits `0`.**
3. **`cargo clippy --workspace --all-targets --locked -- -D warnings` exits `0`**, with
   the `rustc 1.97.0` version line captured in the same evidence block.
4. **`./test.sh --locked` exits `0`**, with the executed workspace target list recorded.
   A bare `cargo test` is not a substitute
   ([[test script required for rust tests not cargo test]]).
5. **`git diff --check main...HEAD` exits `0`** — no committed whitespace artifacts.
6. **Behavior invariance for package entity fanout.** The existing
   `package_entity_fanout` unit tests pass unchanged, with no test added, deleted, or
   edited. Specifically, the test that asserts the
   `"entity_publish remove requires non-empty id"` rejection still passes on an empty id,
   and a `Remove` frame with a non-empty id still reaches
   `PackageEntityMutation::from_entity_frame`. If no test covers the non-empty `Remove`
   pass-through today, Review records that gap rather than adding a test in this ticket.
7. **Behavior invariance for the WebRTC post-handshake regression.**
   `post_handshake_data_channel_opens_and_delivers_bytes` passes, and
   `git diff -w -- src/local_webrtc.rs` is **empty** — proof that the only change is
   whitespace.
8. **Diff containment.** `git diff --stat main...HEAD` lists exactly
   `src/local_webrtc.rs`, `src/package_entity_fanout.rs`, and the plan artifact. No other
   source file appears.

### Downstream proof

None required. The charter requires live Hub, downstream consumer, or exact-binary proof
when Hub admission policy, DTOs, capabilities, protocol revisions, or supervision
behavior change. This ticket changes none of those: the diff is whitespace plus a match
guard with an identical rejection condition. The downstream benefit — sibling feature
tickets inheriting green strict gates — is proven by acceptance criteria 2, 3, and 4 on
this branch, not by rebuilding a consumer graph.

### Runtime-teardown class

`teardown_class_applies`: **no**.

[[botster runtime teardown lenses]] was loaded and evaluated because the diff touches
`src/local_webrtc.rs`. The class does not apply, for these reasons:

- The `local_webrtc.rs` change is whitespace produced by `cargo fmt`. It changes no peer
  lifecycle, signaling, peer map, close path, ownership set, or bound.
- `git diff -w -- src/local_webrtc.rs` must be empty, which is a mechanical proof that no
  teardown semantics moved (acceptance criterion 7).
- The `package_entity_fanout.rs` change is an admission-time validation guard on a
  publish frame. It creates and destroys no durable ownership, holds no lock, and touches
  no session, peer, or worker teardown.
- No SessionIo, ClientWorker, multi-peer ownership, CPU/battery/FD spin, or
  terminal-state divergence surface is modified.

The lens fields are therefore recorded as not-applicable, with the invariant that carries
their intent: the existing WebRTC post-handshake regression test must keep passing with a
byte-identical token stream. Plan Review may force the class; if it does, the ticket
should be sent back rather than widened, because a behavior-preserving formatting ticket
cannot produce teardown evidence it does not change.

## Vault gaps worth capturing

1. **`RUSTUP_TOOLCHAIN=1.92.0` in pipeline agent shells produces false-green Rust gates in
   `botster-hub`.** This is the highest-value gap. The env var is inherited from the
   Botster CLI process, `rustup override list` shows nothing, and `rustup show` is the only
   place the override is visible. Clippy 1.92.0 exits `0` on code that clippy 1.97.0 fails.
   Any Review or Verify agent that trusts a bare `cargo clippy` in this repository can
   approve unfixed lint defects. Candidate note: "botster-hub pipeline shells override
   RUSTUP_TOOLCHAIN below the CI pin". This sharpens
   [[rust repo strict lints must be verified before dismissing warnings]], which today
   covers warning strictness and command scope but not toolchain identity.
2. **`clippy::collapsible_match` reports one arm at a time.** Two structurally identical
   match arms in `validate_publish_frame` produced one diagnostic, not two. A plan that
   sizes a lint repair from a single diagnostic can under-scope it. Worth capturing if the
   Implementer confirms the cascade.
3. **Strict-gate baseline repairs belong in their own ticket.** The owner's rule is already
   recorded in project memory but has no vault note. This ticket is the worked example.
