# Hub test support 0.1.25 authoring-view release

## Run and routing

- Ticket: `ticket_1786042460_231768`
- Run: `run_1786060049_244982`
- Target repository: `trybotster/botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Release tree commit: `c57d3889916f8ff35438565efd34b1ac4ada82aa`
- Source contract origin: merged main `6ad6dfadef61cccd559fecc6536f2d391888cac2` (PR #195 / `ticket_1786039258_173310`)
- Branch: `project-pipelines/ticket_1786042460_231768`
- Published coordinate: `@trybotster/hub-test-support@0.1.25`
- Publisher identity: `tonksthebear` (human operator Jason; path (1) outside agent env after npm token fix)
- Dry-run alone was **not** accepted as publication
- Local pack alone was **not** accepted as final consumable proof

## Guidance applied

- Role: `[[implementer-playbook]]`, `[[botster-implementer-playbook]]`
- Repository charter: `[[botster-hub-playbook]]`
- Targeted notes: `[[hub generated protocol changes are a four site release chain]]`,
  `[[closed dependency tickets signal merged source not a consumable release]]`,
  `[[hub test support npm releases need external consumer smoke]]`,
  `[[conformance fixture revisions must be unique per published content]]`,
  `[[daemon event shape changes bump conformance fixture revision not protocol version]]`,
  `[[botster hub client crate is the external client boundary]]`,
  `[[botster first party client support matrices belong in hub test support]]`,
  `[[published capability matrices must derive enumerations from source]]`,
  `[[published fixture readmes are part of the shipped contract]]`,
  `[[implementation steps must persist report artifacts for review]]`,
  `[[sanitized projection plus wholesale replacement update contracts silent data loss]]` (inherited, not reworked),
  `[[editor scoped reads sit in the mutation admission group not the sanitized read group]]` (inherited),
  `[[hub qualifies effective session type ids as source name slash id]]` (inherited),
  `[[botster local client api lives over hubruntime not raw core routers]]` (no re-open)
- `[[project-pipelines-playbook]]` intentionally not loaded (no package/plugin/workflow-policy path change)
- Convention conflicts: none

## What changed

Site 3 of the four-site protocol release chain: publish the already-merged
session-type authoring view as a new unused npm coordinate above `0.1.24`.

Files changed on the release branch:

- `packages/hub-test-support/package.json` — version `0.1.25`
- `packages/hub-test-support/README.md` — install pin, package-spec JSON, published-coordinate sentence, and (post-Review) current-contract narrative for 0.1.25 / rev 32 authoring view
- `packages/hub-test-support/metadata.json` — regenerated `package_version` `0.1.25`
- `packages/hub-test-support/test.mjs` — package_version assert `0.1.25` plus README pin-site guard
- `docs/plans/publish-session-type-authoring-view-test-support-coordinate.md` — approved plan rev 2
- `docs/reports/hub-test-support-0.1.25-release.md` — this report
- `docs/reports/hub-test-support-0.1.25-release-evidence.json` — machine-readable evidence

Not changed: protocol emitter, `PROTOCOL_VERSION` (stays 6), conformance content/revision (stays 32),
ui-contract, daemon Rust admission code, `docs/client-protocol.md`, root `README.md`, botster-web/tui pins.

## Ownership boundaries preserved

| Boundary | Owner | This ticket |
| --- | --- | --- |
| Host profile / session-type authoring read (already merged) | botster-hub | consume only |
| DTO / hub-test-support npm package (including shipped README) | botster-hub | **publish site** |
| Core mechanisms | botster-core | none |
| Web edit control pin | botster-web | downstream only (`ticket_1786039279_917823`) |
| TUI protocol pin | botster-tui | no repin (protocol stays 6) |

## Published registry facts

| Field | Value |
| --- | --- |
| Coordinate | `@trybotster/hub-test-support@0.1.25` |
| Tarball URL | `https://registry.npmjs.org/@trybotster/hub-test-support/-/hub-test-support-0.1.25.tgz` |
| dist.integrity | `sha512-hQl9j0a01tBC9y3KrtTb0bGDO3KYjlC+2kJ07WdGAgAzJhMKlsVcVTqboCVsYioXG7fHg6VwCMbyiIGtoS5LFQ==` |
| dist.shasum | `2feb30d45c95465e9ff2b04f2b8afbfbe18637f5` |
| Publisher | `tonksthebear` |
| ui-contract dependency | `@trybotster/ui-contract@0.3.1` (skip-after-integrity; not republished) |

Integrity matches the dry-run local packed hub tarball.

## README published-coordinate wording (E10 pin sites)

Pin sites present in both the published tarball and the in-repo HEAD:

> `@trybotster/hub-test-support@0.1.25` is the published coordinate that carries
> the session-type authoring view (`show_session_type_definition`, conformance
> fixture revision 32, protocol version 6). First-party clients should pin this
> coordinate for authoring-view support.

Install line pins `@trybotster/hub-test-support@0.1.25`. Package-spec JSON uses `"0.1.25"`.
The old “separately routed release ticket / must not pin until integrity” caveat was **not** carried forward.

Downstream consumers (`ticket_1786039279_917823`) should pin from the install
command, the package-spec JSON, and the published-coordinate sentence above —
not from the historical coordinate-narrative block described next.

## Known defect in the published 0.1.25 tarball

**Do not republish and do not allocate 0.1.26** for this prose-only defect.
Machine-checkable package bytes (protocol, metadata, matrix, fixtures) are
correct and publication provenance matches release tree `c57d388`.

The **published** `@trybotster/hub-test-support@0.1.25` tarball README’s
coordinate-narrative block (approximately lines 105–119 at publish time) still
says “Version 0.1.24 is the next cold consumer coordinate”, claims “0.1.21
remains the currently published npm coordinate”, and attributes “authoritative
session-type request/response” to 0.1.24 / revision 31. That block is **not**
repairable inside immutable 0.1.25.

In-repo (this branch, post-Review), that narrative is rewritten so the current
contract paragraph describes 0.1.25 / conformance revision 32 / the authoring
view, and demotes 0.1.24 / 0.1.21 to past-tense history without a self-referential
“next cold consumer coordinate” claim. A package-suite guard now ties README
install / package-spec pin sites to `package.json` version.

| Surface | Authoring pin sites | Narrative block :105–119 |
| --- | --- | --- |
| Published 0.1.25 tarball | correct (`0.1.25`) | **stale** (0.1.24 / rev 31 language) — known defect |
| In-repo branch after Review fix | correct (`0.1.25`) | corrected (0.1.25 / rev 32 authoring view) |

## Tests and proof

### Pre-publish

| Check | Result |
| --- | --- |
| Restored `.gitignore`; session files ignored | pass |
| `npm whoami` (agent env) | E401 — human publish engaged |
| Registry: `0.1.25` unused; newest published `0.1.24` | pass |
| `node packages/hub-test-support/scripts/sync-assets.mjs --check` | pass |
| `npm install --no-save --package-lock=false @trybotster/ui-contract@0.3.1` then `npm test` in package | pass |
| Commit `c57d388`; porcelain empty | pass |
| `script/publish-npm-packages --dry-run` | exit 0 |

### Publish

| Check | Result |
| --- | --- |
| Human operator ran sanctioned publish path after auth fix | pass (`tonksthebear`) |
| Registry `npm view @trybotster/hub-test-support@0.1.25` integrity matches dry-run pack | pass |
| Do not re-publish | observed |

### Clean external install (required final proof)

Fresh temp dir outside the hub repo; no `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` override; no path dependency:

```text
npm init -y
npm install @trybotster/hub-test-support@0.1.25 --prefer-online
```

| # | Assertion | Result |
| --- | --- | --- |
| E1 | ESM import succeeds | pass |
| E2 | `metadata.package_version === "0.1.25"` | pass |
| E3 | `metadata.protocol_version === 6` | pass |
| E4 | `metadata.conformance_fixture_revision === 32` | pass |
| E5 | `verifyPackageAssets()` succeeds | pass |
| E6 | protocol contains `show_session_type_definition` and editable `session_type_definition` vocabulary | pass |
| E7 | Direct SHA-256 of installed `daemon-protocol.ts` = `fb441d038011b940db43618864bfab061bdd5baf586bfe274eea3270d3e46d69` and equals metadata | pass |
| E8 | matrix `session_type_authoring.request_type === "show_session_type_definition"` | pass |
| E9 | matrix `read_only_error_kind === "read_only_session_type_source"`; token **absent** from protocol.ts (expected) | pass |
| E10 | Installed README install command, package-spec JSON, and published-coordinate sentence all name `0.1.25` | pass (pin sites only) |
| E10b | Installed README coordinate-narrative block still names 0.1.24 as next cold consumer / attributes session-type request-response to rev 31 | **known defect in published 0.1.25**; not repairable in-registry; fixed in-repo post-Review |

## Deviations from plan

1. **`packages/hub-test-support/test.mjs` version assert** updated to `0.1.25`. Required for the package suite (established per-release practice); not listed under “expected edits” but implied by acceptance check 5.
2. **Publish executed by human operator** (`tonksthebear`) outside the agent environment after fixing the rejected npm token, rather than the agent running `script/publish-npm-packages`. Sanctioned path; identity recorded. Agent did not invent an alternate channel.
3. **Post-Review (no republish):** rewrite in-repo README coordinate narrative for 0.1.25/rev 32; add README pin-site suite guard; correct evidence honesty for E10/E10b; register PR link with Project Pipelines. Does not change the immutable published tarball.

## Cross-repo / residual

- Do **not** close or implement `ticket_1786039279_917823` (web pin) from this run.
- Do **not** sweep `docs/client-protocol.md` or root `README.md` stale coordinates.
- Unverified: org 2FA/provenance flags beyond the operator’s successful publish (already complete).
- Residual risk: none for consumable bytes; downstream web still blocked until it pins `0.1.25`.

## Vault gaps discovered

1. Release ticket template should include: restore ignore rules → bump above newest → **sync package README pin** → sync metadata → package test with ui-contract → porcelain empty → dry-run → publish → clean external install (hash + matrix + README).
2. Pipeline agent npm auth posture: fail closed on `npm whoami` (including present-but-rejected tokens); dry-run ≠ release.
3. Published package README is part of the shipped contract for version pins (already partly covered by `[[published fixture readmes are part of the shipped contract]]`).

No new vault note authored this step.
