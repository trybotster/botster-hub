# Publish installable distribution coordinate for @trybotster/hub-test-support

## Summary

Published `@trybotster/hub-test-support@0.1.0` to the public npm registry and
documented that exact coordinate as the Node client dependency path for
botster-web and CI.

## Assumptions

- The approved distribution path is public npm, not a GitHub Release tarball.
- The package coordinate remains `@trybotster/hub-test-support@0.1.0`.
- The `trybotster` npm organization is owned by the publisher account
  `tonksthebear`.

## Changes

- Added package-local license metadata at `packages/hub-test-support/LICENSE`.
- Updated `packages/hub-test-support/package.json` so `license` resolves to the
  package-local `LICENSE` file and `files[]` includes that file in the tarball.
- Updated `packages/hub-test-support/README.md` and `docs/client-protocol.md`
  with the exact npm install/dependency spec and botster-web lockfile handoff.

## Published Artifact

- Package: `@trybotster/hub-test-support@0.1.0`
- Registry tarball:
  `https://registry.npmjs.org/@trybotster/hub-test-support/-/hub-test-support-0.1.0.tgz`
- Integrity:
  `sha512-mPzEtNDkeuHAAqqrobVnMY/w5p4uxhy4n3y3EzBfgwyhwIaIPr31kTYRck3MighQjzx+AZ00M1q6KAS9/ge/iA==`
- License metadata: `SEE LICENSE IN LICENSE`
- Published tarball includes 10 files: `LICENSE`, `README.md`,
  `daemon-protocol.ts`, fixture files, `index.d.ts`, `index.js`,
  `metadata.json`, and `package.json`.

## Verification

- `node packages/hub-test-support/scripts/sync-assets.mjs --check`
- `node packages/hub-test-support/test.mjs`
- `npm pack --dry-run --json --cache /private/tmp/botster-npm-cache`
- `npm pack --json --cache /private/tmp/botster-npm-cache`
- Installed the packed tarball from a clean non-hub temp npm project and proved
  import, `DaemonRequest`, `DaemonCompatibility`, `verifyPackageAssets()`,
  fixture materialization, and installed `LICENSE`.
- `./test.sh -p botster-hub-client`
- `./test.sh -p botster-hub-test-support`
- `npm view @trybotster/hub-test-support@0.1.0 dist.tarball dist.integrity license version --cache /private/tmp/botster-npm-cache`
- Installed `@trybotster/hub-test-support@0.1.0` from the public registry in a
  clean non-hub temp npm project and proved import, `DaemonRequest`,
  `DaemonCompatibility`, `verifyPackageAssets()`, fixture materialization,
  installed `LICENSE`, and `npm audit` with zero vulnerabilities.
- PII/local-path scan over changed package/docs surfaces found no
  `../botster-hub`, `/Users/`, `/home/`, npm token pattern, `NPM_TOKEN`, or
  `NODE_AUTH_TOKEN` matches.

## Deviations

- No deviation from the approved npm-publish plan. Temporary blockers were npm
  auth, npm 2FA, and missing npm org scope; all were resolved by human action.

## Residual Risk

- `0.1.0` is now immutable on npm. Pre-publish tarball install proof passed
  before the publish, and registry install proof passed after the publish.
- Project Pipelines checklist creation timed out in the plugin worker, so the
  same checklist evidence is preserved in this report and implement gate
  evidence.
