# Publish updated hub-test-support artifact with navigation protocol DTOs

## Summary

Published `@trybotster/hub-test-support@0.1.1` to the public npm registry so
external Node clients can install the current generated daemon protocol artifact
with package navigation DTOs.

Botster-web should pin:

```json
{
  "devDependencies": {
    "@trybotster/hub-test-support": "0.1.1"
  }
}
```

## Published Artifact

- Package: `@trybotster/hub-test-support@0.1.1`
- Tarball:
  `https://registry.npmjs.org/@trybotster/hub-test-support/-/hub-test-support-0.1.1.tgz`
- Integrity:
  `sha512-pAMT4Ev8wAn7y/Y2VLWSza8w4egXGGCbTC0HLqxV2cX5QEpA27M9G27mChUuD8iTPiaRMw0wueMZn5/jT20jVg==`
- License metadata: `SEE LICENSE IN LICENSE`

The package includes `LICENSE`, `README.md`, `package.json`,
`metadata.json`, `daemon-protocol.ts`, `index.js`, `index.d.ts`, and
`fixtures/plugin-contract-matrix/**`.

## Verification

- `node packages/hub-test-support/scripts/sync-assets.mjs --check`
  - Passed before and after the version bump.
- `node packages/hub-test-support/test.mjs`
  - Passed.
- `./test.sh -p botster-hub-client`
  - Passed: 32 unit tests and 4 doctests.
- `./test.sh -p botster-hub-test-support`
  - Initially exposed a stale support-matrix JSON expectation missing
    `package_navigation`; after updating that expectation, passed 21 unit tests
    and 3 doctests.
- `npm pack --dry-run --json --cache /private/tmp/botster-npm-cache`
  - Confirmed the package file list includes license, metadata, protocol, API
    files, declarations, and fixtures.
- Packed tarball clean-consumer smoke
  - Installed `/private/tmp/trybotster-hub-test-support-0.1.1.tgz` in a clean
    non-hub temp consumer.
  - Asserted `metadata.package_version === "0.1.1"`.
  - Asserted generated protocol tokens:
    `DaemonPackageNavigationEntry`, `DaemonPackageNavigationSource`,
    `list_package_navigation`, and `package_navigation`.
  - Called `verifyPackageAssets()` and materialized the plugin contract matrix
    fixture.
- `npm publish --access public --cache /private/tmp/botster-npm-cache`
  - Published `@trybotster/hub-test-support@0.1.1`.
- `npm view @trybotster/hub-test-support@0.1.1 dist.tarball dist.integrity license version --cache /private/tmp/botster-npm-cache --prefer-online`
  - Returned the published tarball URL, integrity, license metadata, and
    version `0.1.1`.
- Registry clean-consumer smoke
  - Installed `@trybotster/hub-test-support@0.1.1` from npm in a clean non-hub
    temp consumer.
  - Asserted `metadata.package_version === "0.1.1"`.
  - Asserted the same four navigation DTO tokens, called
    `verifyPackageAssets()`, materialized the fixture, and verified the package
    includes `LICENSE`.
- `git diff --check`
  - Passed.
- Targeted stale-doc scan
  - No remaining old `0.1.0` install or devDependency pin guidance remains in
    `packages/hub-test-support` or `docs/client-protocol.md`.

## Notes

The generated protocol and fixture assets were already current before the
version bump. The release changed package version metadata and docs, plus one
test expectation in `botster-hub-test-support` so the existing stable support
matrix test matches the landed `package_navigation` feature.
