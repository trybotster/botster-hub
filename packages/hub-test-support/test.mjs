import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  daemonProtocolTypescriptPath,
  materializePluginContractMatrixFixture,
  metadata,
  readDaemonProtocolTypescript,
  verifyPackageAssets,
} from "@trybotster/hub-test-support";

assert.equal(metadata.package_name, "@trybotster/hub-test-support");
assert.equal(metadata.protocol, "botster-hub-daemon-v1");
assert.equal(Number.isInteger(metadata.protocol_version), true);
assert.equal(Number.isInteger(metadata.conformance_fixture_revision), true);

const protocol = readDaemonProtocolTypescript();
assert.equal(protocol, readFileSync(daemonProtocolTypescriptPath(), "utf8"));
assert.match(protocol, /export type DaemonRequest/);
assert.match(protocol, /export interface DaemonCompatibility/);

const verification = verifyPackageAssets();
assert.deepEqual(verification, { ok: true, failures: [] });

const root = mkdtempSync(join(tmpdir(), "botster-node-fixture-"));
try {
  const fixturePath = materializePluginContractMatrixFixture(root);
  assert.equal(fixturePath, join(root, metadata.plugin_contract_matrix.artifact_path));
  assert.match(
    readFileSync(join(fixturePath, "botster-package.json"), "utf8"),
    /botster\.plugin-contract-matrix/,
  );
  assert.match(readFileSync(join(fixturePath, "plugin.lua"), "utf8"), /contract\.app/);
} finally {
  rmSync(root, { recursive: true, force: true });
}

console.log("hub test-support package import and fixture materialization passed");
