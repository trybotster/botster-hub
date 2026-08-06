# Produce a managed installer and signed release metadata

Ticket: `ticket_1785970573_178886` — Hub distribution: produce managed installer
and signed release metadata. Follow-up to `ticket_1785970233_522967`.

Target repository: `botster-hub` (`trybotster/botster-hub`,
`tgt_7e208a0c76a44980a83b63af976b1f22`). Base commit `8a60bd5`.

## Why this ticket exists

`ticket_1785970233_522967` made Hub maintenance *readable*: `status` reports
embedded software identity plus installation provenance, and `check-update`
queries a managed receipt's configured source. It deliberately deferred the
write half. The README records the deferral verbatim: "receipt writes,
installer-managed apply or rollback, signed release publication". `check-update`
already answers `action=run_managed_installer` for a managed installation, and
no installer exists to run. This ticket closes that loop.

## Decisions taken before planning

Recorded from `question_1786036133_867074`, answered on this run. These were
genuine forks; the answers are load-bearing and a reviewer should read them as
settled rather than re-derivable from the ticket text.

1. **Signature verification is installer-only.** The installer is the trust
   boundary because it is the component that writes executables to disk. Hub
   `check-update` is read-only and non-destructive, so forged metadata yields at
   worst a bogus "update available" that the installer then refuses — a
   misleading label, not code execution. The Hub gains **no** crypto trust
   anchor and verifies **no** signature. It records signature *facts* only.
2. **One release document at schema 2, with forward-tolerant reading.** The
   document carries artifacts, checksums, and signature. Hub release-metadata
   validation is relaxed in the same change (details below).
3. **No real origin and no production key in this ticket.** This delivers the
   artifact build, metadata generation, signing procedure, and installer, proven
   against loopback HTTP and an unmistakably-named test keypair. Publication to
   a real HTTPS origin with real key custody is a follow-up release ticket,
   matching how `ticket_1785970233_522967` (contract) and `ticket_1785971560_802153`
   (publication) were split.

## Revision after Plan Review

`review_1786037088_177560` returned changes required with six findings, all
accepted as legitimate. What changed:

- **A real security hole (high).** The unsigned envelope duplicated four fields
  that also appear in the signed manifest, with no equality rule. That let a
  validly signed *old* manifest be advertised as a *new* release. Fixed by
  naming the verified manifest as sole authority, requiring exact equality on all
  four duplicated fields, adding `build_revision` to the manifest so it can be
  covered, and adding one negative test per field.
- **A self-contradiction (high).** Rollback promised the previous binaries were
  restored on any failure after the swap, while the ordering acceptance check
  asserted the new binaries stayed. Both cannot hold. Fixed by separating
  recoverable errors (full rollback) from abrupt termination (no rollback runs;
  crash window bounded to safe states), with a table of reachable states and
  distinct tests for each path.
- **Symlink safety was checks, not operations (high).** `symlink_metadata` then
  `fs::write` is a check/use race. Replaced with a descriptor-relative
  `openat`/`O_NOFOLLOW` sequence validated by `fstat` on the fds, and the shared
  crate now exposes only fd-taking APIs so the race cannot be reintroduced.
- **Unbounded child execution (medium).** Step 6 executes the staged binary with
  no deadline, process-group teardown, or output bound. Fixed by reusing the
  pattern already proven in `src/entrypoint_supervisor.rs`.
- **HTTPS policy did not cover artifact URLs (medium).** The manifest introduces
  independent network coordinates. Fixed with a per-URL policy and an explicit
  no-redirect rule.
- **Missing context (low).** [[cli-patterns]] was not loaded despite
  [[botster-planner-playbook]] requiring it for a Rust CLI surface; it is what
  surfaced the bounded-execution requirement. Loaded, along with
  [[bounded command execution requires process group termination and reaping]].

## Second revision after Plan Review

`review_1786037923_438404` returned changes required with three further findings.
All accepted. The first is a design change, not a wording fix:

- **Two renames still permitted a mixed revision pair (high).** Calling step 7
  "atomic" while replacing `botster-hub` and `botster-session-worker` as two
  separate files left Hub-at-N+1-beside-worker-at-N reachable by SIGKILL or power
  loss, and the crash table hid it by collapsing both renames into one row.
  `question_1786037807_656385` refused to waive this, on the grounds that the
  ticket's own "revision-coupled artifacts" and "exact Hub plus locked-Core worker
  provenance" language exists precisely to forbid that state. Replaced with one
  versioned generation directory per revision pair, a single atomic pointer
  switch, pointer-reversal rollback, retention of the previous generation, and an
  enforced-offline upgrade.
- **Fixed receipt temp name contradicted idempotent recovery (high).** A crash
  during the receipt write leaves `botster-hub.json.tmp`, and the plan's own
  `O_EXCL`-plus-abort rule then made every subsequent re-run fail — so the
  "re-run the idempotent installer" recovery contract was false. Fixed with a
  unique temp name per attempt plus a bounded, fail-safe stale-temp sweep. The
  durability sequence was also wrong: it `fsync`ed the directory *before* the
  rename, which does not commit the rename. Now file `fsync` → `renameat` →
  directory `fsync`.
- **Receipt recorded the wrong signature subject (medium).** It stored a digest of
  the whole release document while the signature covers only the decoded
  manifest bytes, implying authentication of an envelope that was never signed.
  Renamed to `signed_manifest_sha256` and defined as the digest of the exact
  bytes passed to Ed25519 verification.

## Forward tolerance is not a compatibility shim

This repository's standing posture is cold-cut replacement — see
[[cold turkey migrations eliminate dual code paths and version suffixes]] — so
relaxing a validator needs its exception argued rather than assumed.

Cold-cut applies where we control both ends and can replace them together.
Release metadata is read by binaries already in the field that we cannot reach.
A Hub that cannot parse a newer release document cannot tell its user that an
update exists — so the strictness disables the exact mechanism that would let us
ship a fix for the strictness. That is bricking the updater.

Today `ReleaseMetadata` carries `#[serde(deny_unknown_fields)]`
(`src/maintenance.rs:42`) and `RELEASE_SCHEMA_VERSION = 1` (`:23`) is checked
with `!=` (`:124`). No managed installation exists yet, so nothing in the field
depends on schema 1. This is the last moment the relaxation is free.

Required semantics, exactly:

- Drop `deny_unknown_fields` on `ReleaseMetadata`. Unknown fields are **ignored**,
  not rejected.
- Accept `schema_version >= MINIMUM_RELEASE_SCHEMA_VERSION` (2) instead of exact
  equality. A Hub that understands schema 2 must still parse a schema 3 document
  well enough to read `version` and `build_revision` and answer
  available/current.
- Keep `product_id` and `release_channel` as **exact** matches. Those are
  identity, not versioning; a mismatch means the document is not for this
  installation.
- A test asserting that a higher `schema_version` carrying an unknown future
  field still produces the correct available/current answer. Without that test
  the relaxation is only a deleted attribute.

**`RECEIPT_SCHEMA_VERSION` stays strict**, and the asymmetry gets a source
comment saying why: the receipt is local state written by our own installer,
both ends are controlled, and the ticket's upgrade ordering *depends* on a Hub
rejecting a receipt schema it does not know.

## Scope

### Release metadata (schema 2)

One signed document per channel. Hub reads only the top-level identity/version
fields; everything else exists for the installer and is ignored by the Hub.

```json
{
  "schema_version": 2,
  "product_id": "botster-hub",
  "release_channel": "stable",
  "version": "<semver>",
  "build_revision": "<botster-hub git sha>",
  "install_manifest": "<base64 of the exact manifest JSON bytes>",
  "signature": { "algorithm": "ed25519", "key_id": "...", "value": "<base64>" }
}
```

The signature covers the **decoded `install_manifest` bytes exactly as
transported**. This deliberately avoids signing a JSON object in place, which
would require a canonical-JSON implementation agreed between signer and verifier
— a well-known bug class. The signed bytes travel verbatim, so signer and
verifier cannot disagree about what was signed.

The decoded manifest carries:

```json
{
  "product_id": "botster-hub",
  "release_channel": "stable",
  "version": "<semver>",
  "build_revision": "<botster-hub git sha>",
  "source_revisions": { "botster_hub": "<sha>", "botster_core": "<locked sha>" },
  "artifacts": [
    { "name": "botster-hub",            "url": "...", "size": N, "sha256": "..." },
    { "name": "botster-session-worker", "url": "...", "size": N, "sha256": "..." }
  ]
}
```

`source_revisions` keeps the two source identities distinct per
[[live hub proof records distinct hub and locked core binary provenance]]: the
Hub SHA is the checkout, the Core SHA is the revision `Cargo.lock` pins for
`botster-session-worker`. Filesystem colocation of the two binaries does not
collapse their provenance.

#### The signed/unsigned authority boundary

Four fields — `product_id`, `release_channel`, `version`, `build_revision` —
appear both in the unsigned envelope and in the signed manifest. That duplication
is a genuine attack surface and needs an explicit rule, not an implied one.

**The verified manifest is the sole authority for the installer.** The envelope's
copies exist only so the Hub, which verifies nothing, can read version identity
for `check-update`.

The installer must exact-match all four duplicated fields between the envelope
and the verified manifest and **fail closed on any mismatch**. Without that rule
an attacker who cannot forge a signature can still wrap a legitimately signed
*old* manifest in an envelope advertising a *new* version: the Hub would report
that newer version as available, and the installer would silently install
something else. Every duplicated field therefore gets its own negative
acceptance test.

`build_revision` is added to the manifest specifically so that this equality
rule can cover it; without it the envelope's `build_revision` would be
unverifiable.

### Receipt schema 2 (cold turkey, no schema-1 acceptance)

```
schema_version: 2
product_id, binary_version, installation_mode, release_channel, provider, source_url   (schema 1, unchanged)
build_revision           -- must equal the running binary's embedded revision
artifacts[]              -- {name, sha256, size} installed-artifact checksum facts
source_revisions         -- {botster_hub, botster_core}
signature                -- {algorithm, key_id, signed_manifest_sha256}  (facts, not a signature to check)
installer                -- {id, version}
```

`signed_manifest_sha256` is the digest of the **exact bytes passed to Ed25519
verification** — the decoded `install_manifest` — not a hash of the whole release
document. Those are different things: the envelope is unsigned, so a
whole-document digest would record something the signature never covered and
would imply authentication that did not happen. The receipt has to be able to
say unambiguously *which payload was verified*. The whole-document hash is not
recorded, because nothing needs it.

Hub-side validation of the additive fields is **shape and agreement only**:
sanitized 64-char lowercase hex for checksums, an `ed25519` algorithm allowlist,
sanitized `key_id`/installer strings, a known-artifact-name allowlist, and
`build_revision` agreement against `option_env!("BOTSTER_EMBEDDED_BUILD_REVISION")`.
The Hub does **not** re-hash installed binaries at startup and does **not**
verify signatures. `binary_version` agreement against `CARGO_PKG_VERSION` is
retained as-is.

None of the new receipt fields may appear in serialized `DaemonStatus`. The
existing leak assertion (`!serialized.contains("source_url")`) is extended to
cover them.

### Installer

New workspace crates:

- **`crates/botster-hub-installation`** (library, no crypto): the receipt
  contract shared by both writer and reader — schema constant, path constant,
  size limit, the `InstallationReceipt` shape, the symlink/regular-file/owner/
  non-world-writable safety checks currently inline in `src/maintenance.rs`, and
  the atomic write. Sharing this is what prevents the installer and the Hub from
  disagreeing about the file they both touch.
- **`crates/botster-hub-installer`** (binary): fetch, signature verification,
  checksum verification, artifact placement, ordering, and rollback.

Two crates rather than one because the installer needs signature verification
and the Hub must not carry a crypto trust root even architecturally. The
one-crate alternative — optional dependencies behind a `required-features` bin —
was rejected: it would make the installer's tests opt-in under `./test.sh`,
which silently drops coverage. That is a hack, not a boundary.

`ring` supplies both SHA-256 and ed25519 verification. It is **already** in
`Cargo.lock` transitively via `rustls`/`webrtc`, so this adds no new compiled
dependency weight, consistent with
[[prefer framework and library components over custom solutions]].

#### Generations: the pair is one indivisible unit

The Hub and its locked-Core worker are **one revision-coupled generation**, never
two independently replaceable files. Replacing them as two renames makes a mixed
pair — Hub at N+1 beside worker at N — reachable by SIGKILL or power loss, and
that is exactly the state the ticket's "revision-coupled artifacts" and "exact
Hub plus locked-Core worker provenance" language exists to forbid. Required by
`question_1786037807_656385`, which explicitly declined to waive it.

Prefix layout:

```
<prefix>/
  generations/
    <hub-sha>-<core-sha>/
      botster-hub
      botster-session-worker
  current -> generations/<hub-sha>-<core-sha>     # the pointer
  bin/
    botster-hub -> ../current/botster-hub
```

The generation id is the revision pair itself, so the directory name states the
coupling rather than merely implying it.

**The switch is exactly one atomic operation**: `symlinkat` the new target to a
unique temp name in `generations`' parent, then `renameat` it over `current`.
`rename(2)` over an existing symlink is atomic — a concurrent resolver sees the
old target or the new one, never neither and never a blend. Before it the system
is entirely on the old generation; after it, entirely on the new. **Rollback is
that same single operation pointing back** at the retained previous generation,
which is what makes rollback cheap and genuinely testable rather than a narrative
claim. At least the previous generation is retained and is never deleted as part
of a switch.

`bin/botster-hub` is a stable symlink, so it never needs rewriting and `PATH`
never changes. Note that `env::current_exe()` (`src/main.rs:1336`) resolves to
the real path, so a Hub launched through `bin/botster-hub` sees
`generations/<id>/botster-hub`, and `session_worker_bin` resolving the sibling
(`src/main.rs:4169`) therefore finds the worker **from its own generation**. That
is a useful property but it is *not* claimed as a licence for online upgrade; see
below.

**Do not generalise the symlink here to the receipt.** The generation pointer and
the installation receipt are different objects with different rules. The receipt
remains a regular file in a user-owned, non-world-writable directory, created and
replaced through descriptor-relative operations and **never** through a symlink.
A pointer file plus a launcher shim was considered and rejected: it adds a
resolving indirection on every launch and changes `current_exe()` semantics, for
no gain over an atomic `renameat` on a symlink.

#### Upgrades are offline and the installer enforces it

The installer detects a running managed Hub daemon and **refuses with a clear
diagnostic** rather than racing it. Detection is a `connect()` attempt on
`<data-dir>/botster-hub.sock` (default `$HOME/.botster/hub`, overridable with
`--data-dir`); a successful connect means running, connection-refused means a
stale socket and the install proceeds.

Online upgrade is out of scope. Generation directories make it *conceivable* —
a daemon that pinned its generation at startup would keep resolving its own
generation after the pointer moved — but relying on that would require the Hub to
resolve workers through a start-time-pinned generation path as a guaranteed
contract rather than an incidental consequence of `current_exe()`. That is a Hub
behavioural change beyond this installer, and the ticket says to keep apply
mechanics out unless the existing contract can perform them safely. Recorded here
so the future enabler is discoverable.

#### Install order

1. Refuse if a managed Hub daemon is running.
2. Fetch release metadata. HTTPS required for any non-loopback host.
3. Verify the signature against the trust anchor. **Fail closed.**
4. Exact-match the envelope against the verified manifest per the authority
   boundary above; decode the manifest.
5. Stage both artifacts into a new `generations/<hub-sha>-<core-sha>/` directory
   inside the prefix, so the pointer switch is same-filesystem.
6. Verify each staged artifact's size and SHA-256.
7. Verify the staged Hub binary's *self-reported* identity via a new
   `botster-hub version` subcommand, requiring it to match the manifest's
   `version` and `build_revision`. The binary is executed only *after* its
   checksum and signature verify, so this is never execution of unvalidated
   bytes.
8. **Switch generations — the single atomic operation.**
9. Verify the Hub resolved through `current` reports the expected version and
   build revision.
10. **Only then** write the receipt atomically.

Steps 8–10 preserve the ordering the ticket requires: the binaries are replaced
and verified before a receipt requiring the newer schema is written. Writing the
receipt first would turn a managed installation into an unmanaged one — the old
binary would report `unsupported_receipt_schema` — until the binaries caught up.

#### Network coordinate policy

The manifest introduces artifact URLs, which are network coordinates in their own
right. The ticket's HTTPS rule applies to **every** coordinate the installer
fetches, not only the outer document:

- Each artifact URL is an independently validated absolute URL. HTTPS is required
  for any non-loopback host; loopback HTTP is accepted only for tests.
- **Redirects are not followed**, for the metadata document or for artifacts. The
  existing `fetch_release_metadata` already sets `max_redirects(0)`; the installer
  matches it. A followed redirect could downgrade to plaintext or cross origins
  after validation has already passed.
- Same-origin with the metadata document is **not** required. Forbidding a
  separate artifact host would rule out an ordinary CDN layout for no security
  gain, since each URL is validated on its own and every artifact is checksum-
  verified against the signed manifest regardless of where it came from.

#### Failure, rollback, and the crash window

The distinction between a *recoverable error* and *abrupt termination* is
load-bearing, and conflating them is how an ordering guarantee turns into a
contradiction.

**Recoverable errors** — any returned error at or after the switch: point
`current` back at the previous generation with the same single operation, leave
the previous receipt untouched, exit non-zero. No partial state is observable.
This is the case that "no partial state" describes.

**Abrupt termination** — SIGKILL, power loss, anything where no rollback code
runs. No durable journal or crash-recovery mechanism is in scope; recovery is
"re-run the installer", which is idempotent. What the design buys is that the
crash window is *bounded to safe states only*:

| Crash point | On-disk state | Hub behavior |
| --- | --- | --- |
| Before the switch | old generation; new generation staged but unreferenced | unchanged, still old pair |
| **During** the switch | `current` resolves to old **or** new — never neither | coherent pair either way |
| After switch, before receipt | new generation, **previous** receipt | stale coordinate / `receipt_binary_mismatch` → unmanaged |
| During receipt write | new generation, previous receipt intact, unique stale temp may remain | unmanaged; re-run converges |
| After receipt write | new generation, new receipt | managed |

Two properties matter here. First, **a mixed Hub/worker pair is unreachable by
construction** — not merely unlikely — because both binaries are only ever
referenced through one pointer that flips atomically. Second, every intermediate
row degrades *honestly*: the Hub reports unmanaged rather than falsely claiming a
managed install, and a re-run repairs it.

**No reachable state has a schema-2 receipt beside an old binary**, because the
receipt is written last. That is the invariant the ordering exists to protect,
and acceptance tests it distinctly from rollback, which is the recoverable-error
path.

#### Race-resistant filesystem operations

"Never through a symlink" is a property of the *operations*, not of a preceding
check. `symlink_metadata` followed by `fs::write` is a check/use race: the path
can be substituted between the two. The receipt write therefore uses
descriptor-relative operations throughout (`libc` is already a direct dependency):

1. Open `$HOME` normally. `$HOME` itself may legitimately be a symlink and is not
   constrained; the existing Hub reader likewise validates only the two
   components below it.
2. `openat(home_fd, ".botster", O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC)`, creating it
   with `mkdirat(…, 0o700)` on `ENOENT`.
3. `openat(botster_fd, "installations", O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC)`, same
   creation rule.
4. `fstat` **each directory fd** — owner equals the effective uid, not
   world-writable. Validating the fd rather than re-stating the path is what
   closes the race.
5. `openat(installations_fd, "botster-hub.json.<random>.tmp",
   O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC, 0o600)`, write, `fsync` the
   file.
6. `renameat` within that same directory fd, **then `fsync(installations_fd)`**
   so the rename itself is durably committed.

The temp name is **unique per attempt** (random suffix via `getrandom`, already a
direct dependency), not the fixed `botster-hub.json.tmp` an earlier draft used. A
crash during the receipt write leaves exactly one stale temp file; with a fixed
name plus `O_EXCL`, every subsequent re-run would abort on it forever, which
would have made the "re-run the idempotent installer" recovery contract false.
`O_EXCL` is retained so a pre-placed or attacker-controlled file at the chosen
name still fails closed rather than being overwritten.

Stale temps are swept before writing, bounded and fail-safe: within the validated
`installations_fd`, match only the installer's own name pattern, `openat` each
candidate `O_NOFOLLOW`, require a regular file owned by the effective uid and not
world-writable, then `unlinkat`. Anything failing those checks is left alone
rather than removed — the sweep never follows a symlink and never deletes a file
it cannot prove is its own.

Because every component below `$HOME` is opened `O_NOFOLLOW` relative to an
already-validated descriptor, a symlink substituted mid-sequence cannot redirect
the write — the open fails rather than following it.

The shared crate's public API accepts a **directory descriptor, not a path**, so
a path-based write is structurally unavailable to callers rather than merely
discouraged. The Hub-side reader moves to the same `openat` walk and `fstat`s the
opened fd; today it path-stats and then reads, which has the same race on the
read side. That change is cleanup made necessary by moving this code into a
shared crate, not opportunistic refactoring.

#### Bounded execution of the staged binary

Step 6 executes a child process, and a bare timeout does not bound a process
tree — per
[[bounded command execution requires process group termination and reaping]], a
descendant can outlive the child, retain the stdout write end, and keep drains
from reaching EOF.

This repository already has the correct pattern in `src/entrypoint_supervisor.rs`
(`setpgid(0,0)` via `pre_exec`, `signal_process_group_or_child` with a `killpg`
→ `kill` fallback on `ESRCH`, `supervised_process_group_exists`, TERM→KILL
escalation with a bounded grace period, and reaping). The installer reuses that
pattern rather than inventing a second one:

- Own process group via `setpgid(0, 0)` in `pre_exec`.
- Concurrent stdout/stderr drains into fixed bounded tails, started before the
  wait so a full pipe cannot deadlock the child.
- On deadline: TERM the **group**, bounded grace, then KILL; reap the leader;
  let both drains reach EOF.
- Reject non-zero exit, output exceeding the bound, and output that does not
  parse as the expected `key=value` identity lines.

The trust anchor is **not embedded** in this ticket. The installer requires an
explicit `--trust-anchor <path>` and refuses to run without one. No production
key exists, and shipping a default test key would create precisely the "mistaken
for production material" hazard the answer warned against. The publication
ticket ships a real embedded anchor.

Test key material lives at `fixtures/release-signing/` named
`UNTRUSTED-TEST-ONLY-botster-hub-release-signing.{pub,pkcs8}`, with `key_id`
`test-only-do-not-trust` inside generated fixtures.

### Release artifact build

`script/build-release-artifacts` builds the revision-coupled pair, computes
checksums, reads the locked Core revision from `Cargo.lock`, emits the schema-2
document, and signs it with a caller-supplied key:

```sh
BOTSTER_BUILD_REVISION=<hub sha> cargo build --locked --release
cargo build --locked --release -p botster-core --bin botster-session-worker
```

### `botster-hub version`

A new subcommand printing `product_id=`/`version=`/`build_revision=` in the
existing `key=value` operator style, requiring **no** data directory and **no**
running daemon. It exists because the installer must verify a staged,
not-yet-running binary's embedded identity; `status` needs a live daemon and is
unusable for that. It is also the honest expression of "the running Hub binary
remains authoritative for its embedded product version/build revision".

## Non-scope

- Publishing to a real origin; provisioning real signing keys; release CI.
- Any `botster-hub-client` DTO change. `PROTOCOL_VERSION` stays **6** and
  `CONFORMANCE_FIXTURE_REVISION` stays **31**.
- **Online upgrade.** Upgrades are offline and the installer refuses to run
  against a live daemon. Making them safe would require the Hub to resolve its
  worker through a start-time-pinned generation path as a guaranteed contract
  rather than as an incidental consequence of `current_exe()` resolution — a Hub
  behavioural change, and its own ticket.
- Pruning old generations beyond retaining the previous one. A retention policy
  is not this ticket's.
- Auto-update daemons, background update application, sandboxing, notarization,
  multi-platform artifact matrices, delta updates, downgrade support.
- Hub-side signature verification or startup re-hashing of installed binaries.
- Package install/update APIs. `botster-hub check-update` and
  `botster-hub packages check-update` stay separate commands; nothing here
  touches package update paths.
- Mutating development checkouts. The installer only ever writes inside an
  explicit `--prefix` and the receipt path under `$HOME`.

## Ownership boundaries and cross-repository dependencies

Per [[botster-hub-playbook]], Hub owns installation provenance policy, host
identity, and the local control API. That is exactly where this work lands.

- **botster-core**: supplies `botster-session-worker`. **No Core change is
  required.** The revision coupling already exists — `Cargo.toml` tracks
  `branch = "main"` and `Cargo.lock` pins the tested revision
  (`33ebcd98d19031d23e91b03d8da0ee3f8d1410d4` at this base). The installer reads
  that pin rather than introducing a second pinning mechanism. **No dependency
  ticket is registered**, because nothing in Core blocks this work.
- **botster-hub-client**: untouched by design.
- **botster-web** (`ticket_1785970234_234515`) and **botster-tui**
  (`ticket_1785970234_132113`): already render `action=run_managed_installer`
  from the existing DTO. This ticket makes that action real without changing the
  contract they consume, so neither is blocked by nor blocks this work.

### The constraint that shaped the DTO decision

`botster-tui`'s `ensure_compatible` moved from `>=` to **exact equality** on
protocol version, and `ticket_1785976581_841608` is mid-flight against
protocol 6 / conformance 31. Any protocol bump here would hard-break a run
currently in progress. Recording this so the "no DTO change" boundary reads as a
deliberate constraint rather than an arbitrary preference. The exact-equality
brittleness itself is a known project-level concern and is **not** this ticket's
to fix.

## Assumptions and unknowns

1. **No managed installation exists in the field.** This is what makes both
   schema bumps free. Stated explicitly because every compatibility argument
   here rests on it.
2. **Single platform.** Artifacts are host-native; the manifest carries a flat
   artifact list with no platform matrix. A `platform` key is *not* added
   speculatively.
3. **Upgrade proof uses synthetic second-version artifacts.** The Hub crate is
   at `0.1.0` and the receipt requires `binary_version == CARGO_PKG_VERSION`, so
   a genuine two-real-version upgrade cannot be produced without editing
   `Cargo.toml`. Installer mechanics (replace, verify, roll back) are proven with
   synthetic artifacts; receipt/binary agreement against a *real* Hub is proven
   separately with the actual built binary. Both halves are needed; neither
   alone is sufficient, and the report must say so rather than implying one
   test covered both.
4. **Ownership-mismatch cases may be unprovable unprivileged.** A test that
   requires a directory owned by a different uid cannot be constructed without
   privilege. If it cannot be built, it is recorded as an explicit gap at the
   skip site naming what is unproven — never silently omitted.
5. **Rollback is generation retention, not copied bytes.** The previous
   generation directory stays in place, so rollback is a pointer reversal rather
   than restoring saved copies. At least one previous generation is retained.
6. **`rename(2)` over an existing symlink is atomic** on the target platforms —
   a concurrent resolver observes the old target or the new one, never neither.
   The design rests on this, so acceptance proves it under crash injection rather
   than asserting it.
7. **`env::current_exe()` resolves through the `bin/botster-hub` symlink** to the
   real generation path, so a running Hub resolves its sibling worker within its
   own generation. Verified against `src/main.rs:1336` and the sibling resolution
   at `src/main.rs:4169`. This is relied on for coherence, *not* as a licence for
   online upgrade — upgrades remain offline and enforced.
8. **Crash-safety scope is explicit and narrow.** No durable journal, no
   crash-recovery state machine. Abrupt termination is bounded to the safe states
   tabulated above, and recovery is re-running the idempotent installer. Anything
   stronger — surviving power loss mid-`renameat` with automatic resume — is out
   of scope and would need its own ticket.
9. `$HOME` itself is not constrained to be a non-symlink; only the two components
   below it are, matching what the existing Hub reader already validates.
   Constraining `$HOME` would break legitimate setups where a home directory is a
   symlink to another volume.

## Affected surfaces and files

| Surface | Change |
| --- | --- |
| `crates/botster-hub-installation/` | New. Shared receipt contract; descriptor-relative (`openat`/`O_NOFOLLOW`) directory walk, fd-based ownership/permission validation, and atomic `renameat` write. Public API takes a directory fd, never a path. Uses `libc`, already a direct dependency. |
| `crates/botster-hub-installer/` | New. Installer binary: running-daemon refusal, fetch, signature verification, envelope/manifest equality, per-URL HTTPS policy, checksum verification, bounded process-group-owning child runner, generation staging, single atomic pointer switch, pointer-reversal rollback. |
| `src/maintenance.rs` | Receipt reading delegates to the shared crate; receipt schema 2; release schema 2 + forward tolerance; asymmetry comment. |
| `src/main.rs` | New `version` subcommand, dispatch, usage text. |
| `Cargo.toml` | Workspace members. |
| `script/build-release-artifacts` | New. Revision-coupled build, checksums, metadata generation, signing. |
| `fixtures/release-signing/` | New. Test-only keypair, unmistakably named. |
| `tests/hub_daemon_lifecycle_test.rs` | Real-daemon managed-install proof; forward-compat check-update; leak assertions. |
| `README.md`, `docs/client-protocol.md` | Receipt schema 2, release schema 2 + tolerance rule, installer usage; remove the now-satisfied deferrals. |

## Risks

1. **The relaxation gets over-applied.** Someone reads "forward tolerant" and
   loosens `product_id`, `release_channel`, or the receipt. Mitigated by exactness
   tests on all three and the source comment on the asymmetry.
2. **Ordering regressions are invisible without a failure injection point.** A
   test that only checks the happy path cannot distinguish "binary before
   receipt" from "receipt before binary". Mitigated by injection points at the
   two boundaries, mirroring the prior art already in `src/persistence.rs`
   (test-only injected failure between temp flush and rename). The injection must
   model *abrupt termination* for the ordering invariant and a *returned error*
   for rollback; using one mechanism for both is what made the first draft of
   this plan self-contradictory.
3. **A mixed Hub/worker pair.** The failure mode the generation design exists to
   eliminate. Any implementation that touches the two binaries separately —
   two renames, a rename plus a copy, an in-place overwrite of one — reintroduces
   it. The pair must move only through the single pointer switch. This is a
   defect class that would very likely never surface in testing and would be
   deeply unpleasant in the field, which is why it is proven by crash injection
   rather than by inspection.
4. **Signature verification that never fails.** A verifier wired so that any
   input passes is worse than none. Mitigated by negative tests: tampered
   payload, wrong key, absent signature — each must abort the install.
5. **Receipt-private data leaking into `DaemonStatus`.** New fields mean new leak
   surface. Mitigated by extending the existing serialization assertions.
6. **`build_revision` agreement breaking development builds.** `option_env!` is
   `None` without an embedded revision. The agreement check must only apply to a
   receipt-backed managed installation and must not perturb development or
   unmanaged fallback behavior.
7. **Scope creep into a real release.** No hostname, no production key, nothing
   implying a live origin exists.
8. **Trusting the unsigned envelope.** The easiest wrong implementation reads
   `version` from the envelope because it is already parsed, bypassing the
   equality rule entirely. Mitigated by four per-field negative tests rather than
   a single "signature verifies" test.
9. **Path-based convenience creeping back in.** A future caller reaching for
   `fs::write(receipt_path, …)` silently reintroduces the check/use race.
   Mitigated structurally: the shared crate's API takes a directory descriptor,
   so no path-based write is exposed to reintroduce.
10. **A bounded runner that is not actually bounded.** Killing only the direct
    child leaves descendants holding pipe writers. Mitigated by reusing the
    process-group pattern already proven in `src/entrypoint_supervisor.rs` and by
    a descendant-PID survival test.

## Acceptance checks

Gates: `./test.sh` (which also runs the hub-test-support asset sync check),
`cargo fmt --check`, and
`cargo clippy --workspace --all-targets --all-features -- -D warnings`. Any
pre-existing failure must be attributed to touched vs untouched files with exact
evidence, not waved through.

**Receipt contract**

1. Schema 2 accepted; schema 1 rejected as `unsupported_receipt_schema` and
   treated as unmanaged (cold-turkey proof).
2. `build_revision` disagreement with the embedded revision diagnoses and falls
   back to unmanaged.
3. Existing safety diagnostics still fire under schema 2: symlink receipt,
   non-regular file, wrong owner, world-writable file, world-writable parent.
4. Malformed checksum hex, unknown signature algorithm, and unknown artifact
   names each diagnose rather than being accepted.

**Release metadata**

5. Schema 3 with an unknown future field still yields the correct
   available/current answer. *(the guarantee test)*
6. `product_id` mismatch and `release_channel` mismatch each still yield
   `invalid_release_metadata`.
7. Schema below the minimum yields `invalid_release_metadata`.
8. Non-loopback `http://` metadata sources are rejected; `https://` and loopback
   `http://` accepted.

**Signed/unsigned authority boundary**

9. Four separate negative tests — envelope `product_id`, `release_channel`,
   `version`, and `build_revision` each disagreeing with the verified manifest —
   every one aborting the install. The `version` case is the attack this rule
   exists to stop: a validly signed old manifest advertised as a new release.
10. Tampered manifest bytes, wrong signing key, and absent signature each abort.

**Network coordinates**

11. Non-loopback `http://` artifact URL rejected; `https://` accepted; loopback
    `http://` accepted.
12. A redirect response for the metadata document and for an artifact are each
    rejected rather than followed.

**Installer filesystem safety**

13. Fresh install into an empty prefix: both binaries present, receipt written,
    receipt `0600`, `installations` directory `0700`, both owned by the effective
    uid.
14. Refusal when `.botster` **or** `installations` is a symlink, when either is a
    regular file, and when either is world-writable — six cases, both components
    covered.
15. A symlink pre-placed at the receipt path causes refusal and its target is
    provably byte-unchanged.
16. A pre-placed file at the chosen temp name causes `O_EXCL` failure and aborts
    rather than being overwritten. Separately, a **stale temp left by a previous
    crash does not block a re-run** — the regression that a fixed temp name would
    have caused.
17. The stale-temp sweep removes only installer-owned regular files matching its
    own pattern, and leaves a symlink, a differently-owned file, and a
    world-writable file in place rather than unlinking them.
18. Deterministic injection of an adversarial mid-write symlink substitution is
    impractical, so race resistance is proven structurally instead: the shared
    crate exposes only descriptor-relative APIs, and a test asserts every receipt
    write and read goes through a validated directory fd with `O_NOFOLLOW`. This
    substitution is recorded as a deliberate limit of the evidence, not passed
    off as a race test.

**Generation coupling and the atomic switch**

19. Fresh install creates `generations/<hub-sha>-<core-sha>/` containing both
    artifacts, and `current` resolving to it; `bin/botster-hub` resolves through
    `current` to that generation's Hub.
20. Upgrade creates a second generation and moves `current` to it while
    **retaining** the previous generation directory intact.
21. **A mixed pair is unreachable.** With crash injection immediately before and
    immediately after the switch, assert in every case that the Hub and worker
    reachable through `current` come from one and the same generation — never Hub
    at N+1 beside worker at N or the reverse.
22. Rollback is the same single operation reversed: `current` returns to the
    previous generation and the pair is coherent there.
23. The installer **refuses with a clear diagnostic** when a managed Hub daemon is
    running on the target data directory, and proceeds when only a stale socket
    remains.

**Installer failure semantics**

24. Byte-flipped artifact fails checksum verification and installs nothing; the
    pointer never moves.
25. **Recoverable error after the switch** reverses the pointer and leaves the
    previous receipt byte-identical.
26. Post-switch identity verification failure produces the same reversal.
27. **Crash injection at each of the three boundaries** required by
    `question_1786037807_656385` — immediately before the switch, after the switch
    but before the receipt write, and *during* the receipt write. After each,
    assert the Hub/worker pair is coherent, the receipt state is honest (the Hub
    reports unmanaged rather than falsely claiming managed), and **a re-run
    converges to a correct managed installation**.
28. No reachable state places a schema-2 receipt beside an old generation —
    asserted at every injection point above.

**Bounded staged-binary execution**

29. Hanging child is terminated at the deadline; a descendant PID captured before
    the timeout is provably gone afterwards, per
    [[bounded command execution requires process group termination and reaping]].
30. Non-zero exit, oversized output, and malformed non-`key=value` output are
    each rejected.
31. The process group leader is reaped and both drains reach EOF, and a second
    invocation through the same code path still succeeds — proving teardown left
    nothing wedged.

**Real runtime — the production path, not scaffolding**

32. Install the **actual** built `botster-hub` and `botster-session-worker` into
    an isolated prefix using the installer, then launch that **installed** Hub
    with `HOME` pointed at the prefix (reusing `start_cli_daemon_with_home`).
    Assert `status` reports `installation.mode=managed` with the expected channel
    and provider, and that `software.version`/`build_revision` come from the
    binary.
33. Serialized status contains no `source_url`, no artifact checksum, no
    signature, no installer identity, and no home path.
34. `check-update` against a loopback schema-2 fixture returns
    `state=available` / `action=run_managed_installer`; the schema-3
    forward-compat fixture returns the same.
35. Restart preserves identical `software` and `installation`.
36. `botster-hub version` prints identity with no data directory and no daemon.

**Provenance** — per the charter's live-binary requirement

37. Record the Hub SHA and the `Cargo.lock`-pinned Core SHA **separately**,
    resolve both binary realpaths under the fresh checkout's target directory,
    and assert the receipt's `source_revisions` matches both.
38. No orphaned processes or stale process groups remain after the installer's
    bounded-execution tests, checkable with the repo's existing
    `script/process-census`.

## Vault gaps worth capturing

1. **Forward tolerance belongs in readers we cannot reach.** The cold-cut
   convention has an exception that is not currently written down: a validator
   read by already-deployed binaries must tolerate unknown fields and newer
   schema versions, because strictness there disables the very channel that
   would deliver the fix. The receipt/release asymmetry in one file is a sharp
   worked example.
2. **Signing embedded transported bytes avoids canonical-JSON disagreement.**
   Worth capturing as a general pattern with its rejected alternative.
3. **Installer-versus-runtime trust boundaries.** "The component that writes
   executables verifies; the read-only reporter does not" is a reusable
   allocation rule, and a counterweight to reflexively adding verification
   everywhere.
4. **Fields duplicated across a signed/unsigned boundary need an explicit
   equality rule.** Found by Plan Review on this ticket: without it, a validly
   signed *old* payload can be wrapped in an unsigned envelope advertising
   something newer, and signature verification still passes. Probably the most
   transferable item here, because the hole recurs anywhere a signed payload sits
   inside an unsigned wrapper.
5. Possible extension of
   [[lorester production installs revision coupled cli and worker with a build receipt]]
   to cover Botster Hub, since this makes the Hub the second product installing a
   revision-coupled CLI/worker pair with a provenance receipt.
