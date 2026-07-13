import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  applicationPrimitivesFixturePath,
  daemonProtocolTypescriptPath,
  firstPartyClientSupportMatrixPath,
  lateAttachHistoryConformanceFixturePath,
  materializeApplicationPrimitivesFixture,
  materializePluginContractMatrixFixture,
  metadata,
  pluginContractMatrixFixturePath,
  readDaemonProtocolTypescript,
  readFirstPartyClientSupportMatrix,
  readLateAttachHistoryConformanceFixture,
  verifyPackageAssets,
} from "@trybotster/hub-test-support";

assert.equal(metadata.package_name, "@trybotster/hub-test-support");
assert.equal(metadata.package_version, "0.1.4");
assert.equal(metadata.protocol, "botster-hub-daemon-v1");
assert.equal(Number.isInteger(metadata.protocol_version), true);
assert.equal(Number.isInteger(metadata.conformance_fixture_revision), true);
assert.deepEqual(metadata.application_primitives, {
  fixture_package_name: "botster.plugin-contract-matrix",
  artifact_path: "fixtures/plugin-contract-matrix",
  source_artifact_path: "botster_hub_test_support::application_primitives_fixture_descriptor()",
  surface_id: "contract.app",
  route_id: "surface:contract.app",
  renderer_entrypoint: "ui_tree_snapshot.body",
  primitive_kinds: [
    "button",
    "empty_state",
    "form",
    "metric",
    "metric_grid",
    "panel",
    "section",
    "status_badge",
    "table",
    "text_input",
    "toolbar",
  ],
});

const protocol = readDaemonProtocolTypescript();
assert.equal(protocol, readFileSync(daemonProtocolTypescriptPath(), "utf8"));
assert.match(protocol, /export type DaemonRequest/);
assert.match(protocol, /export interface DaemonCompatibility/);
assert.match(protocol, /read_screen/);
assert.match(protocol, /capture_snapshot/);
assert.match(protocol, /export interface DaemonReadScreen/);
assert.match(protocol, /export interface DaemonCaptureSnapshot/);

assert.equal(
  fileURLToPath(import.meta.resolve("@trybotster/hub-test-support/first-party-client-support-matrix")),
  firstPartyClientSupportMatrixPath(),
);
assert.equal(
  fileURLToPath(import.meta.resolve("@trybotster/hub-test-support/late-attach-history-conformance-fixture")),
  lateAttachHistoryConformanceFixturePath(),
);

const supportMatrix = readFirstPartyClientSupportMatrix();
assert.equal(supportMatrix.late_attach_history.supported, true);
assert.equal(supportMatrix.required_features.includes("terminal_readback"), true);
assert.equal(supportMatrix.supported_features.includes("terminal_readback"), true);

const lateAttachFixture = readLateAttachHistoryConformanceFixture();
const historyIndex = lateAttachFixture.history_then_live.findIndex(
  (event) => (event.type === "snapshot" || event.type === "scrollback") && event.data.length > 0,
);
const liveIndex = lateAttachFixture.history_then_live.findIndex(
  (event) => event.type === "terminal_output",
);
const attachingIndex = lateAttachFixture.history_then_live.findIndex(
  (event) => event.type === "attach_state" && event.state === "attaching",
);
const attachedIndex = lateAttachFixture.history_then_live.findIndex(
  (event) => event.type === "attach_state" && event.state === "attached",
);
assert.notEqual(attachingIndex, -1);
assert.notEqual(historyIndex, -1);
assert.notEqual(attachedIndex, -1);
assert.equal(attachingIndex < historyIndex, true);
assert.equal(historyIndex < attachedIndex, true);
assert.equal(attachedIndex < liveIndex, true);
for (const event of lateAttachFixture.history_then_live) {
  if (event.type === "snapshot" || event.type === "scrollback") {
    assert.equal(event.bytes, Buffer.byteLength(event.data));
  }
}
const noHistoryAttachingIndex = lateAttachFixture.no_history_then_live.findIndex(
  (event) => event.type === "attach_state" && event.state === "attaching",
);
const noHistoryAttachedIndex = lateAttachFixture.no_history_then_live.findIndex(
  (event) => event.type === "attach_state" && event.state === "attached",
);
const noHistoryLiveIndex = lateAttachFixture.no_history_then_live.findIndex(
  (event) => event.type === "terminal_output",
);
const noHistoryLastInitialStateIndex = lateAttachFixture.no_history_then_live.findLastIndex(
  (event) => event.type === "snapshot" || event.type === "scrollback",
);
const noHistoryFirstTerminalOutputIndex = lateAttachFixture.no_history_then_live.findIndex(
  (event) => event.type === "terminal_output",
);
assert.equal(
  lateAttachFixture.no_history_then_live.some((event) => event.type === "scrollback"),
  false,
);
assert.equal(noHistoryAttachingIndex < noHistoryAttachedIndex, true);
assert.equal(
  noHistoryLastInitialStateIndex === -1 || noHistoryLastInitialStateIndex < noHistoryAttachedIndex,
  true,
);
assert.equal(noHistoryAttachedIndex < noHistoryFirstTerminalOutputIndex, true);
assert.equal(noHistoryAttachedIndex < noHistoryLiveIndex, true);

const verification = verifyPackageAssets();
assert.deepEqual(verification, { ok: true, failures: [] });

const root = mkdtempSync(join(tmpdir(), "botster-node-fixture-"));
try {
  const fixturePath = materializePluginContractMatrixFixture(root);
  assert.equal(fixturePath, join(root, metadata.plugin_contract_matrix.artifact_path));
  assert.equal(applicationPrimitivesFixturePath(), pluginContractMatrixFixturePath());
  assert.match(
    readFileSync(join(fixturePath, "botster-package.json"), "utf8"),
    /botster\.plugin-contract-matrix/,
  );
  assert.match(readFileSync(join(fixturePath, "plugin.lua"), "utf8"), /contract\.app/);

  const applicationFixturePath = materializeApplicationPrimitivesFixture(join(root, "application"));
  assert.equal(
    applicationFixturePath,
    join(root, "application", metadata.application_primitives.artifact_path),
  );
  assert.match(readFileSync(join(applicationFixturePath, "plugin.lua"), "utf8"), /contract\.app/);
} finally {
  rmSync(root, { recursive: true, force: true });
}

console.log("hub test-support package import and fixture materialization passed");
