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

### Receipt schema 2 (cold turkey, no schema-1 acceptance)

```
schema_version: 2
product_id, binary_version, installation_mode, release_channel, provider, source_url   (schema 1, unchanged)
build_revision           -- must equal the running binary's embedded revision
artifacts[]              -- {name, sha256, size} installed-artifact checksum facts
source_revisions         -- {botster_hub, botster_core}
signature                -- {algorithm, key_id, release_metadata_sha256}  (facts, not a signature to check)
installer                -- {id, version}
```

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

Install order — this is the ticket's central safety property:

1. Fetch release metadata (HTTPS required; loopback HTTP allowed for tests).
2. Verify the signature against the trust anchor. **Fail closed.**
3. Exact-match `product_id` and `release_channel`; decode the manifest.
4. Stage artifacts into a staging directory **inside the install prefix**, so the
   later rename is same-filesystem and therefore atomic.
5. Verify each staged artifact's size and SHA-256.
6. Verify the staged Hub binary's *self-reported* identity via a new
   `botster-hub version` subcommand, and require it to match the metadata's
   `version` and `build_revision`. This proves receipt/binary agreement from the
   binary itself before anything is committed.
7. Atomically replace `bin/botster-hub` and `bin/botster-session-worker`,
   retaining the previous bytes for rollback.
8. Re-verify the installed Hub's identity after the swap.
9. **Only then** create/validate `.botster/installations` and atomically write
   the receipt.
10. On any failure at or after step 7, restore the previous binaries and leave
    the previous receipt untouched.

Steps 7–9 are ordered exactly as the ticket requires: replace and verify the
binary before writing a receipt that requires the newer schema. Writing the
receipt first would turn a managed installation into an unmanaged one — the old
binary would report `unsupported_receipt_schema` — until the binary caught up.

Directory and file safety, all fail-closed: `.botster/installations` is created
`0700` if absent; if it exists it must be a real directory, not a symlink, owned
by the effective uid, and not world-writable. The receipt is written to a temp
file in that directory, `fchmod` `0600`, then renamed. The receipt path is never
followed through a symlink.

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
5. Retained rollback copies live beside the installed binaries in the prefix.
   Whether they are pruned after a successful install is an installer detail;
   the plan retains them for the duration of the install only.

## Affected surfaces and files

| Surface | Change |
| --- | --- |
| `crates/botster-hub-installation/` | New. Shared receipt contract, path/permission safety, atomic write. |
| `crates/botster-hub-installer/` | New. Installer binary: fetch, verify, install, order, roll back. |
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
   receipt" from "receipt before binary". Mitigated by an injected failure
   between the two, mirroring the prior art already in `src/persistence.rs`
   (test-only injected failure between temp flush and rename).
3. **Rollback leaving a mixed pair.** Hub replaced, worker not, or vice versa.
   Both binaries must be staged and verified before either is swapped, and
   rollback must restore both.
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
8. Non-loopback `http://` sources are rejected; `https://` and loopback `http://`
   accepted.

**Installer**

9. Fresh install into an empty prefix: both binaries present, receipt written,
   receipt `0600`, `installations` directory `0700`, both owned by the effective
   uid.
10. Refusal when `.botster/installations` is a symlink, a regular file, or
    world-writable.
11. A symlink pre-placed at the receipt path causes refusal, and the symlink's
    target is provably untouched.
12. Byte-flipped artifact fails checksum verification and installs nothing.
13. Tampered manifest, wrong signing key, and absent signature each abort.
14. Upgrade replaces both binaries and the receipt with no partial state.
15. **Ordering**: injected failure between the binary swap and the receipt rename
    leaves new binaries in place and the *previous* receipt intact — and the Hub
    then honestly reports `receipt_binary_mismatch`/unmanaged rather than
    claiming a managed install.
16. **Staged-receipt failure**: when the binary swap fails after the receipt is
    staged, the temp receipt is discarded and both the previous receipt and the
    previous binaries are byte-identical to their pre-install state.
17. Post-swap verification failure rolls both binaries back to their previous
    bytes.

**Real runtime — the production path, not scaffolding**

18. Install the **actual** built `botster-hub` and `botster-session-worker` into
    an isolated prefix using the installer, then launch that **installed** Hub
    with `HOME` pointed at the prefix (reusing `start_cli_daemon_with_home`).
    Assert `status` reports `installation.mode=managed` with the expected channel
    and provider, and that `software.version`/`build_revision` come from the
    binary.
19. Serialized status contains no `source_url`, no artifact checksum, no
    signature, no installer identity, and no home path.
20. `check-update` against a loopback schema-2 fixture returns
    `state=available` / `action=run_managed_installer`; the schema-3
    forward-compat fixture returns the same.
21. Restart preserves identical `software` and `installation`.
22. `botster-hub version` prints identity with no data directory and no daemon.

**Provenance** — per the charter's live-binary requirement

23. Record the Hub SHA and the `Cargo.lock`-pinned Core SHA **separately**,
    resolve both binary realpaths under the fresh checkout's target directory,
    and assert the receipt's `source_revisions` matches both.

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
4. Possible extension of
   [[lorester production installs revision coupled cli and worker with a build receipt]]
   to cover Botster Hub, since this makes the Hub the second product installing a
   revision-coupled CLI/worker pair with a provenance receipt.
