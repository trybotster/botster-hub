import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  applicationPrimitivesFixturePath,
  daemonProtocolTypescriptPath,
  firstPartyClientSupportMatrixPath,
  lateAttachHistoryConformanceFixturePath,
  localWebrtcDeliveryChunkConformanceFixturePath,
  modeFlagsConformanceFixturePath,
  materializeApplicationPrimitivesFixture,
  materializePluginContractMatrixFixture,
  metadata,
  pluginContractMatrixFixturePath,
  readDaemonProtocolTypescript,
  readFirstPartyClientSupportMatrix,
  readLateAttachHistoryConformanceFixture,
  readLocalWebrtcDeliveryChunkConformanceFixture,
  readModeFlagsConformanceFixture,
  readSessionLifecycleSubscriptionConformanceFixture,
  sessionLifecycleSubscriptionConformanceFixturePath,
  verifyPackageAssets,
} from "@trybotster/hub-test-support";

assert.equal(metadata.package_name, "@trybotster/hub-test-support");
assert.equal(metadata.package_version, "0.1.11");
assert.equal(metadata.protocol, "botster-hub-daemon-v1");
assert.equal(metadata.protocol_version, 3);
assert.equal(metadata.conformance_fixture_revision, 18);
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
assert.match(protocol, /read_mode_flags/);
assert.match(protocol, /capture_snapshot/);
assert.match(protocol, /export interface DaemonReadScreen/);
assert.match(protocol, /export interface DaemonModeFlags/);
assert.match(protocol, /export interface DaemonCaptureSnapshot/);
assert.match(protocol, /export interface DaemonLocalWebrtcDeliveryChunk/);
assert.match(protocol, /DaemonLocalWebrtcDeliveryKind/);
assert.match(protocol, /subscribe_entities/);
assert.match(protocol, /entity_snapshot/);
assert.match(protocol, /entity_upsert/);
assert.match(protocol, /entity_patch/);
assert.match(protocol, /entity_remove/);
assert.match(protocol, /resync_reason/);
assert.match(protocol, /refresh_local_packages/);

assert.equal(
  fileURLToPath(import.meta.resolve("@trybotster/hub-test-support/session-lifecycle-subscription-conformance-fixture")),
  sessionLifecycleSubscriptionConformanceFixturePath(),
);
assert.equal(
  fileURLToPath(import.meta.resolve("@trybotster/hub-test-support/first-party-client-support-matrix")),
  firstPartyClientSupportMatrixPath(),
);
assert.equal(
  fileURLToPath(import.meta.resolve("@trybotster/hub-test-support/late-attach-history-conformance-fixture")),
  lateAttachHistoryConformanceFixturePath(),
);
assert.equal(
  fileURLToPath(import.meta.resolve("@trybotster/hub-test-support/local-webrtc-delivery-chunk-conformance-fixture")),
  localWebrtcDeliveryChunkConformanceFixturePath(),
);
assert.equal(
  fileURLToPath(import.meta.resolve("@trybotster/hub-test-support/mode-flags-conformance-fixture")),
  modeFlagsConformanceFixturePath(),
);

const supportMatrix = readFirstPartyClientSupportMatrix();
const sessionLifecycleFixture = readSessionLifecycleSubscriptionConformanceFixture();
assert.equal(supportMatrix.late_attach_history.supported, true);
assert.equal(supportMatrix.required_features.includes("terminal_readback"), true);
assert.equal(supportMatrix.required_features.includes("session_entity_subscriptions"), true);
assert.equal(supportMatrix.session_entities.supported, true);
assert.equal(supportMatrix.session_entities.bounded_delivery, true);
assert.equal(supportMatrix.session_entities.explicit_snapshot_resync, true);
assert.equal(
  supportMatrix.session_entities.runtime_runner,
  "botster_hub_test_support::run_session_lifecycle_subscription_conformance",
);
assert.equal(
  supportMatrix.session_entities.json_helper,
  "botster_hub_test_support::session_lifecycle_subscription_conformance_fixture_json",
);
assert.equal(supportMatrix.supported_features.includes("terminal_readback"), true);

assert.equal(sessionLifecycleFixture.conformance_fixture_revision, 18);
assert.equal(sessionLifecycleFixture.entity_type, "session");
assert.deepEqual(
  sessionLifecycleFixture.normalized_frames.map((frame) => frame.type),
  ["entity_snapshot", "entity_upsert", "entity_patch", "entity_patch", "entity_remove"],
);
assert.deepEqual(
  sessionLifecycleFixture.normalized_frames.map((frame) => frame.snapshot_seq),
  [0, 1, 2, 3, 4],
);
assert.equal(
  sessionLifecycleFixture.fresh_subscription.requires_authoritative_snapshot_before_deltas,
  true,
);
assert.equal(sessionLifecycleFixture.fresh_subscription.prior_generation_frames_discarded, true);
assert.equal(sessionLifecycleFixture.overflow.resync_reason, "subscriber_overflow");
assert.equal(sessionLifecycleFixture.overflow.resync_snapshot.type, "entity_snapshot");
assert.equal(
  sessionLifecycleFixture.overflow.resync_snapshot.resync_reason,
  "subscriber_overflow",
);
assert.equal(sessionLifecycleFixture.overflow.snapshot_precedes_later_deltas, true);
assert.equal(sessionLifecycleFixture.overflow.failed_snapshot_delivery_closes_subscription, true);

const lateAttachFixture = readLateAttachHistoryConformanceFixture();
const chunkFixture = readLocalWebrtcDeliveryChunkConformanceFixture();
const modeFlagsFixture = readModeFlagsConformanceFixture();
assert.deepEqual(modeFlagsFixture.request, {
  type: "read_mode_flags",
  session_id: "mode-flags-fixture-session",
});
assert.equal(modeFlagsFixture.mouse_off.mode_flags.mouse_mode, 0);
assert.equal(modeFlagsFixture.mouse_on.mode_flags.mouse_mode, 9);
assert.equal(modeFlagsFixture.mouse_off.mode_flags.session_id, "mode-flags-fixture-session");
assert.equal(modeFlagsFixture.mouse_on.mode_flags.session_id, "mode-flags-fixture-session");
assert.equal(modeFlagsFixture.unknown_session.response_kind, "operator_error");
assert.equal(modeFlagsFixture.unknown_session.error_code, "unknown_session");
assert.equal(modeFlagsFixture.unknown_session.operation, "read_mode_flags");
assert.equal(modeFlagsFixture.unknown_session.mode_flags, null);
assert.equal(modeFlagsFixture.backend_failure.response_kind, "operator_error");
assert.equal(modeFlagsFixture.backend_failure.error_code, "runtime_error");
assert.equal(modeFlagsFixture.backend_failure.operation, "read_mode_flags");
assert.equal(modeFlagsFixture.backend_failure.mode_flags, null);
assert.equal(chunkFixture.version, 2);
assert.equal(chunkFixture.maximum_frame_bytes_exclusive, 65536);
assert.equal(chunkFixture.maximum_delivery_bytes, 16777216);
assert.equal(chunkFixture.scenarios.daemon_response.length, 1);
assert.equal(chunkFixture.scenarios.daemon_response[0].delivery_kind, "daemon_response");
assert.equal(chunkFixture.scenarios.daemon_entity_frame.length, 2);
assert.equal(chunkFixture.scenarios.daemon_entity_frame[0].delivery_kind, "daemon_entity_frame");
assert.equal(
  chunkFixture.scenarios.daemon_entity_frame.map((chunk) => chunk.payload).join(""),
  "encrypted-envelope",
);
const largeScenario = chunkFixture.scenarios.large_generated;
assert.equal(largeScenario.generator, "repeat_utf8_pattern");
assert.equal(largeScenario.total_bytes > 256 * 1024, true);
const generatedLargePayload = largeScenario.pattern
  .repeat(Math.ceil(largeScenario.total_bytes / largeScenario.pattern.length))
  .slice(0, largeScenario.total_bytes);
const generatedLargeChunks = [];
for (let offset = 0; offset < generatedLargePayload.length; offset += largeScenario.chunk_payload_bytes) {
  generatedLargeChunks.push(generatedLargePayload.slice(offset, offset + largeScenario.chunk_payload_bytes));
}
const reassembledLargePayload = generatedLargeChunks.join("");
assert.equal(generatedLargeChunks.length, largeScenario.expected_chunk_count);
assert.equal(reassembledLargePayload, generatedLargePayload);
assert.equal(
  createHash("sha256").update(reassembledLargePayload).digest("hex"),
  largeScenario.reassembled_sha256,
);
const historyIndex = lateAttachFixture.history_then_live.findIndex(
  (event) =>
    (event.type === "snapshot" || event.type === "scrollback") &&
    event.payload_base64.length > 0,
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
    const payload = Buffer.from(event.payload_base64, "base64");
    assert.equal(event.bytes, payload.length);
    assert.equal(event.payload_encoding, "base64");
    assert.equal(payload.toString("utf8").includes("history-before-live"), false);
  }
}
assert.equal(lateAttachFixture.read_screen_text.match(/history-before-live/g)?.length, 1);
assert.equal(lateAttachFixture.no_history_read_screen_text, "");
let restoredPresentation = lateAttachFixture.read_screen_text;
const bufferedLive = lateAttachFixture.history_then_live
  .filter((event) => event.type === "terminal_output")
  .map((event) => event.data)
  .join("");
restoredPresentation += bufferedLive;
assert.equal(
  restoredPresentation.indexOf("history-before-live") <
    restoredPresentation.indexOf("live-after-attach"),
  true,
);
const noHistoryAttachingIndex = lateAttachFixture.no_history_then_live.findIndex(
  (event) => event.type === "attach_state" && event.state === "attaching",
);
const noHistoryAttachedIndex = lateAttachFixture.no_history_then_live.findIndex(
  (event) => event.type === "attach_state" && event.state === "attached",
);
const noHistoryLiveIndex = lateAttachFixture.no_history_then_live.findIndex(
  (event) =>
    event.type === "terminal_output" && event.data.includes("live-without-history"),
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
