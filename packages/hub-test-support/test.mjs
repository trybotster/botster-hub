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
  materializeSessionPluginBindings,
  materializeSessionPluginBindingScenario,
  materializeSessionPluginRows,
  materializeSessionPluginRowScenario,
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
  readSessionPluginBindingConformanceFixture,
  readUiContractConformanceFixtures,
  sessionLifecycleSubscriptionConformanceFixturePath,
  sessionPluginBindingConformanceFixturePath,
  verifyPackageAssets,
} from "@trybotster/hub-test-support";

function luaTableRanges(source) {
  const codePositions = new Uint8Array(source.length);
  const stack = [];
  const ranges = [];
  let state = "code";

  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];
    const next = source[index + 1];

    if (state === "line_comment") {
      if (character === "\n") state = "code";
      continue;
    }
    if (state === "single_quote" || state === "double_quote") {
      if (character === "\\") {
        index += 1;
      } else if (
        (state === "single_quote" && character === "'") ||
        (state === "double_quote" && character === '"')
      ) {
        state = "code";
      }
      continue;
    }
    if (character === "-" && next === "-") {
      state = "line_comment";
      index += 1;
      continue;
    }
    if (character === "'" || character === '"') {
      state = character === "'" ? "single_quote" : "double_quote";
      continue;
    }

    codePositions[index] = 1;
    if (character === "{") {
      stack.push(index);
    } else if (character === "}") {
      const start = stack.pop();
      assert.notEqual(start, undefined, "plugin.lua contains an unmatched closing table brace");
      ranges.push({ start, end: index });
    }
  }

  assert.equal(state, "code", "plugin.lua contains an unterminated string or comment");
  assert.deepEqual(stack, [], "plugin.lua contains an unmatched opening table brace");
  return { codePositions, ranges };
}

function luaFieldTableRanges(source, field, expectedValue, parsed) {
  const escapedField = field.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const escapedValue = expectedValue.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const pattern = new RegExp(`\\b${escapedField}\\s*=\\s*"${escapedValue}"`, "g");
  const matches = [...source.matchAll(pattern)].filter(
    (match) => parsed.codePositions[match.index] === 1,
  );

  return matches.map((match) => {
    const containing = parsed.ranges
      .filter((range) => range.start < match.index && match.index < range.end)
      .sort((left, right) => left.end - left.start - (right.end - right.start))[0];
    assert.ok(containing, `${field} = "${expectedValue}" is not inside a Lua table`);
    return containing;
  });
}

function assertDialogFormComposition(source) {
  const parsed = luaTableRanges(source);
  const [dialog] = luaFieldTableRanges(source, "id", "contract-dialog", parsed);
  const [form] = luaFieldTableRanges(source, "id", "contract-app-form", parsed);
  const [input] = luaFieldTableRanges(source, "id", "contract-app-message", parsed);

  assert.ok(dialog, "plugin.lua must contain contract-dialog");
  assert.ok(form, "plugin.lua must contain contract-app-form");
  assert.ok(input, "plugin.lua must contain contract-app-message");
  assert.equal(
    luaFieldTableRanges(source, "id", "contract-dialog", parsed).length,
    1,
    "plugin.lua must contain one canonical contract-dialog",
  );
  assert.equal(
    luaFieldTableRanges(source, "id", "contract-app-form", parsed).length,
    1,
    "plugin.lua must contain one canonical contract-app-form",
  );
  assert.equal(
    form.start > dialog.start && form.end < dialog.end,
    true,
    "contract-app-form must be structurally nested inside contract-dialog",
  );
  assert.equal(
    input.start > form.start && input.end < form.end,
    true,
    "contract-app-message must be structurally nested inside contract-app-form",
  );
}

assert.equal(metadata.package_name, "@trybotster/hub-test-support");
assert.equal(metadata.package_version, "0.1.33");
assert.equal(metadata.protocol, "botster-hub-daemon-v1");
assert.equal(metadata.protocol_version, 7);
assert.equal(metadata.conformance_fixture_revision, 38);

// Package README ships in the npm tarball; keep install pin sites tied to package.json.
{
  const packageJson = JSON.parse(
    readFileSync(new URL("./package.json", import.meta.url), "utf8"),
  );
  const readme = readFileSync(new URL("./README.md", import.meta.url), "utf8");
  const version = packageJson.version;
  assert.equal(metadata.package_version, version);
  assert.match(
    readme,
    new RegExp(
      String.raw`npm install --save-dev @trybotster/ui-contract@0\.3\.2 @trybotster/hub-test-support@${version.replaceAll(".", String.raw`\.`)}`,
    ),
    "README install command must pin the package.json version",
  );
  assert.match(
    readme,
    new RegExp(
      String.raw`"@trybotster/hub-test-support": "${version.replaceAll(".", String.raw`\.`)}"`,
    ),
    "README package-spec JSON must pin the package.json version",
  );
  const pinCoords = [
    ...readme.matchAll(/@trybotster\/hub-test-support@(\d+\.\d+\.\d+)/g),
  ].map((match) => match[1]);
  assert.ok(pinCoords.length > 0, "README must name the published package coordinate");
  for (const pinned of pinCoords) {
    assert.equal(
      pinned,
      version,
      `README package coordinate pin must be ${version}, found @${pinned}`,
    );
  }
  const packageSpecPins = [
    ...readme.matchAll(/"@trybotster\/hub-test-support":\s*"([^"]+)"/g),
  ].map((match) => match[1]);
  assert.deepEqual(packageSpecPins, [version]);
}
assert.deepEqual(metadata.ui_contract, {
  conformance_fixture_export: "@trybotster/ui-contract/conformance-fixtures",
  package_name: "@trybotster/ui-contract",
  package_version: "0.3.2",
});
assert.deepEqual(metadata.application_primitives, {
  fixture_package_name: "botster.plugin-contract-matrix",
  artifact_path: "fixtures/plugin-contract-matrix",
  source_artifact_path: "botster_hub_test_support::application_primitives_fixture_descriptor()",
  surface_id: "contract.app",
  route_id: "surface:contract.app",
  renderer_entrypoint: "ui_tree_snapshot.body",
  primitive_kinds: [
    "button",
    "dialog",
    "empty_state",
    "form",
    "metric",
    "metric_grid",
    "panel",
    "section",
    "status_badge",
    "table",
    "text",
    "text_input",
    "toolbar",
  ],
});

const protocol = readDaemonProtocolTypescript();
assert.equal(protocol, readFileSync(daemonProtocolTypescriptPath(), "utf8"));
assert.match(protocol, /export type DaemonRequest/);
assert.match(protocol, /export type DaemonSessionTypeExecution/);
assert.match(protocol, /mode: "relative_executable"/);
assert.match(protocol, /mode: "shell_command"/);
assert.match(protocol, /create_session_type/);
assert.match(protocol, /update_session_type/);
assert.match(protocol, /delete_session_type/);
assert.match(protocol, /show_session_type_definition/);
assert.match(protocol, /list_session_types_for_target/);
assert.match(protocol, /export interface DaemonSessionTypeDefinition/);
assert.match(protocol, /export interface DaemonSessionTypeEditableDefinition/);
assert.match(
  protocol,
  /session_type_definition\?: DaemonSessionTypeEditableDefinition \| null/,
);
assert.match(protocol, /\| "session_type_definition"/);
assert.match(protocol, /\{ source: "device" \}/);
assert.match(protocol, /\{ source: "repo"; target_id: string \}/);
assert.match(protocol, /\{ policy: "package_root" \}/);
assert.match(protocol, /\{ policy: "relative"; path: string \}/);
assert.match(protocol, /export interface DaemonCompatibility/);
assert.match(protocol, /read_screen/);
assert.match(protocol, /read_mode_flags/);
assert.match(protocol, /capture_snapshot/);
assert.match(protocol, /export interface DaemonReadScreen/);
assert.match(protocol, /export interface DaemonModeFlags/);
assert.match(protocol, /export interface DaemonCaptureSnapshot/);
assert.match(protocol, /export interface DaemonLocalWebrtcDeliveryChunk/);
assert.match(protocol, /export interface DaemonPluginWorkerCounters/);
assert.match(protocol, /plugin_worker_counters\?: DaemonPluginWorkerCounters \| null/);
assert.match(protocol, /DaemonLocalWebrtcDeliveryKind/);
assert.match(protocol, /subscribe_entities/);
assert.match(protocol, /entity_snapshot/);
assert.match(protocol, /entity_upsert/);
assert.match(protocol, /entity_patch/);
assert.match(protocol, /entity_remove/);
assert.match(protocol, /lifecycle_class: string/);
assert.match(protocol, /resync_reason/);
assert.match(protocol, /refresh_local_packages/);
assert.match(protocol, /check_hub_update/);
assert.match(protocol, /export interface DaemonSoftwareIdentity/);
assert.match(protocol, /export interface DaemonInstallationIdentity/);
assert.match(protocol, /export interface DaemonHubUpdate/);
assert.match(protocol, /hub_update\?: DaemonHubUpdate \| null/);
assert.match(protocol, /reason\?: string \| null/);
assert.match(protocol, /action\?: string \| null/);
assert.doesNotMatch(protocol, /hub_version: string/);

assert.equal(
  fileURLToPath(import.meta.resolve("@trybotster/hub-test-support/session-lifecycle-subscription-conformance-fixture")),
  sessionLifecycleSubscriptionConformanceFixturePath(),
);
assert.equal(
  fileURLToPath(import.meta.resolve("@trybotster/hub-test-support/session-plugin-binding-conformance-fixture")),
  sessionPluginBindingConformanceFixturePath(),
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
const sessionPluginBindingFixture = readSessionPluginBindingConformanceFixture();
assert.equal(supportMatrix.late_attach_history.supported, true);
assert.equal(supportMatrix.required_features.includes("terminal_readback"), true);
assert.equal(supportMatrix.required_features.includes("session_entity_subscriptions"), true);
assert.equal(
  supportMatrix.required_features.includes("session_type_entity_subscriptions"),
  true,
);
assert.equal(supportMatrix.session_entities.supported, true);
assert.equal(supportMatrix.session_entities.bounded_delivery, true);
assert.equal(supportMatrix.session_entities.explicit_snapshot_resync, true);
assert.equal(supportMatrix.session_entities.binding_family, "/session");
assert.equal(supportMatrix.session_entities.lifecycle_class_field, "lifecycle_class");
assert.deepEqual(supportMatrix.session_entities.lifecycle_classes, [
  "current",
  "ended",
  "indeterminate",
]);
assert.equal(supportMatrix.session_entities.missing_row_state, "unavailable");
assert.equal(supportMatrix.session_entities.plugin_surface_id, "contract.sessions");
assert.equal(
  supportMatrix.session_entities.runtime_runner,
  "botster_hub_test_support::run_session_lifecycle_subscription_conformance",
);
assert.equal(
  supportMatrix.session_entities.json_helper,
  "botster_hub_test_support::session_lifecycle_subscription_conformance_fixture_json",
);
assert.equal(supportMatrix.supported_features.includes("terminal_readback"), true);
assert.equal(
  supportMatrix.supported_features.includes("session_type_entity_subscriptions"),
  true,
);
assert.equal(
  supportMatrix.plugin_surfaces.runtime_runner,
  "botster_hub_test_support::run_plugin_contract_matrix_conformance",
);
assert.deepEqual(supportMatrix.plugin_surfaces.presentation_operation_kinds, [
  "set",
  "clear",
  "toggle",
]);
assert.equal(supportMatrix.plugin_surfaces.dialog_presence_key, "contract-dialog");
assert.equal(supportMatrix.plugin_surfaces.dialog_form_node_id, "contract-app-form");
assert.equal(supportMatrix.plugin_surfaces.dialog_input_node_id, "contract-app-message");
assert.equal(supportMatrix.plugin_surfaces.actionable_sibling_form_forbidden, true);
assert.equal(supportMatrix.plugin_surfaces.accepted_replacement_scope, "whole_surface");
assert.equal(
  supportMatrix.plugin_surfaces.selected_workspace_equality_key,
  "selected-workspace",
);
assert.equal(
  supportMatrix.plugin_surfaces.selected_workspace_equality_value,
  "workspace-alpha",
);
assert.deepEqual(supportMatrix.plugin_surfaces.authored_set_values, {
  "contract-dialog": true,
  "selected-workspace": "workspace-alpha",
});

// The lossless authoring read: an editor must be able to reconstruct exactly what
// update_session_type consumes, which the sanitized session_type row cannot do.
assert.equal(supportMatrix.session_type_authoring.supported, true);
assert.equal(
  supportMatrix.session_type_authoring.request_type,
  "show_session_type_definition",
);
assert.equal(
  supportMatrix.session_type_authoring.response_kind,
  "session_type_definition",
);
assert.equal(
  supportMatrix.session_type_authoring.response_field,
  "session_type_definition",
);
assert.equal(
  supportMatrix.session_type_authoring.definition_type,
  "botster_hub_client::DaemonSessionTypeEditableDefinition",
);
assert.deepEqual(supportMatrix.session_type_authoring.editable_sources, ["device", "repo"]);
assert.equal(supportMatrix.session_type_authoring.read_only_source, "package");
assert.equal(
  supportMatrix.session_type_authoring.read_only_error_kind,
  "read_only_session_type_source",
);
assert.deepEqual(
  supportMatrix.session_type_authoring.authored_fields_absent_from_published_row,
  ["context", "environment", "working_directory"],
);
assert.equal(supportMatrix.session_type_authoring.admission_group, "allow_runtime");

assert.equal(sessionLifecycleFixture.conformance_fixture_revision, 38);
assert.equal(sessionLifecycleFixture.entity_type, "session");
assert.deepEqual(
  sessionLifecycleFixture.normalized_frames.map((frame) => frame.type),
  ["entity_snapshot", "entity_upsert", "entity_patch", "entity_patch", "entity_remove"],
);

const uiContractResolution = fileURLToPath(
  import.meta.resolve("@trybotster/ui-contract/conformance-fixtures"),
);
assert.match(
  uiContractResolution,
  /node_modules[\\/]@trybotster[\\/]ui-contract[\\/]/,
);
const uiContractFixtures = await readUiContractConformanceFixtures();
assert.equal(uiContractFixtures.contract_version, "0.3.2");
assert.equal(
  uiContractFixtures.fixtures.dialog_presence.predicate.key,
  "create-ticket-dialog",
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

assert.equal(sessionPluginBindingFixture.conformance_fixture_revision, 38);
assert.equal(sessionPluginBindingFixture.binding_family, "/session");
const sessionPluginMaterialization = materializeSessionPluginBindingScenario(
  sessionPluginBindingFixture,
);
assert.deepEqual(sessionPluginMaterialization, sessionPluginBindingFixture.expected);
const sessionPluginRows = materializeSessionPluginRowScenario(
  sessionPluginBindingFixture,
);
for (const [stage, rows] of Object.entries(sessionPluginRows)) {
  assert.deepEqual(rows, sessionPluginBindingFixture.row_expected[stage]);
  for (const row of rows) {
    assert.deepEqual(
      row.controls.map((control) => control.key),
      ["spawn", "rename", "remove"],
    );
    for (const control of row.controls) {
      assert.deepEqual(control.action_payload, {
        operation: control.key,
        session_uuid: row.node_id,
      });
      assert.match(control.node_id, /^botster-ui-descendant-v1:/);
    }
    assert.equal(row.controls[0].label, "current");
    assert.equal(row.controls[1].label, "Rename session");
  }
}
assert.equal(
  Object.values(sessionPluginMaterialization.initial).includes("current"),
  true,
  "matching rows must not false-pass through empty_template",
);
assert.equal(
  Object.values(sessionPluginMaterialization.initial).includes("ended"),
  true,
);
assert.equal(
  Object.values(sessionPluginMaterialization.initial).includes("indeterminate"),
  true,
);
assert.equal(
  Object.values(sessionPluginMaterialization.initial).includes("unavailable"),
  true,
);
assert.deepEqual(
  Object.keys(sessionPluginBindingFixture.transition_frames[1].patch),
  ["lifecycle_class", "registry_state", "updated_at"],
  "stale transition must match the producer's omitted optional lifecycle field",
);
const transitionRow = structuredClone(
  sessionPluginBindingFixture.initial_snapshot.items.find(
    (item) => item.session_uuid === "session-transition",
  ),
);
Object.assign(
  transitionRow,
  sessionPluginBindingFixture.transition_frames[0].patch,
  sessionPluginBindingFixture.transition_frames[1].patch,
);
assert.equal(transitionRow.lifecycle, "exited");
assert.equal(transitionRow.lifecycle_class, "indeterminate");
const malformedSessionFrames = [
  sessionPluginBindingFixture.initial_snapshot,
  {
    ...sessionPluginBindingFixture.transition_frames[0],
    patch: { lifecycle_class: null },
  },
];
assert.throws(
  () =>
    materializeSessionPluginBindings(
      sessionPluginBindingFixture.surface,
      malformedSessionFrames,
    ),
  /present session row session-transition is missing lifecycle_class/,
);

for (const mutate of [
  (surface) => surface.children.pop(),
  (surface) => surface.children.push(structuredClone(surface.children.at(-1))),
  (surface) => {
    surface.children[0].item_template.props.text.$bind = "@/registry_state";
  },
  (surface) => {
    surface.children.at(-1).item_template.id.$bind = "@/registry_state";
  },
  (surface) => {
    surface.children.at(-1).item_template.children[0].props.label.$bind =
      "@/missing_label";
  },
  (surface) => {
    surface.children.push({
      $kind: "bind_list",
      source: "/session",
      where: { registry_state: "running" },
      item_template: {
        type: "text",
        id: "extra",
        props: { text: "Extra" },
      },
    });
  },
]) {
  const surface = structuredClone(sessionPluginBindingFixture.surface);
  mutate(surface);
  assert.throws(() =>
    materializeSessionPluginBindings(
      surface,
      [sessionPluginBindingFixture.initial_snapshot],
    ),
  );
  assert.throws(() =>
    materializeSessionPluginRows(
      surface,
      [sessionPluginBindingFixture.initial_snapshot],
    ),
  );
}

for (const sessionUuid of [null, " \t"]) {
  const invalidIdPatch = {
    ...sessionPluginBindingFixture.transition_frames[0],
    patch: { session_uuid: sessionUuid },
  };
  assert.throws(() =>
    materializeSessionPluginRows(
      sessionPluginBindingFixture.surface,
      [sessionPluginBindingFixture.initial_snapshot, invalidIdPatch],
    ),
  );
}
const duplicateIdPatch = {
  ...sessionPluginBindingFixture.transition_frames[0],
  patch: { session_uuid: "session-stable-current" },
};
assert.throws(
  () =>
    materializeSessionPluginRows(
      sessionPluginBindingFixture.surface,
      [sessionPluginBindingFixture.initial_snapshot, duplicateIdPatch],
    ),
  /duplicate realized node id session-stable-current/,
);
const staticCollisionPatch = {
  ...sessionPluginBindingFixture.transition_frames[0],
  patch: { session_uuid: "contract-session-lifecycle-panel" },
};
assert.throws(
  () =>
    materializeSessionPluginRows(
      sessionPluginBindingFixture.surface,
      [sessionPluginBindingFixture.initial_snapshot, staticCollisionPatch],
    ),
  /duplicate realized node id contract-session-lifecycle-panel/,
);

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
const GHOSTSNP_MAGIC = Buffer.from("GHOSTSNP");
const GOLDEN_A_SHA256 =
  "fbcdda31d682a61420251eed68f72e413485f057e3f374c57582955b0316bb6d";
const GOLDEN_B_SHA256 =
  "06962b11d4a3acfb9b7c52b673a7b476904ddee2dd754b89b190ff82fdcfd0cc";
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
const historySnapshot = lateAttachFixture.history_then_live[historyIndex];
assert.equal(historySnapshot.type, "snapshot");
const historyPayload = Buffer.from(historySnapshot.payload_base64, "base64");
assert.equal(historySnapshot.bytes, historyPayload.length);
assert.equal(historySnapshot.payload_encoding, "base64");
assert.equal(historyPayload.subarray(0, 8).equals(GHOSTSNP_MAGIC), true);
assert.equal(createHash("sha256").update(historyPayload).digest("hex"), GOLDEN_A_SHA256);
// Authentic GHOSTSNP may embed screen glyphs in binary form; clients must restore
// via Ghostty import / ReadScreen, never by appending snapshot bytes as text.
// Only the first Snapshot is a READY frame with GHOSTSNP magic. Later PAGE and
// FINISH frames are continuation bytes.
const historySnapshots = lateAttachFixture.history_then_live.filter(
  (event) => event.type === "snapshot",
);
assert.ok(historySnapshots.length >= 2);
for (const [index, event] of historySnapshots.entries()) {
  const payload = Buffer.from(event.payload_base64, "base64");
  assert.equal(event.bytes, payload.length);
  assert.equal(event.payload_encoding, "base64");
  if (index === 0) {
    assert.equal(payload.subarray(0, 8).equals(GHOSTSNP_MAGIC), true);
  } else {
    assert.equal(payload.subarray(0, 8).equals(GHOSTSNP_MAGIC), false);
  }
}
assert.equal(lateAttachFixture.read_screen_text.match(/history-before-live/g)?.length, 1);
assert.equal(lateAttachFixture.no_history_read_screen_text, "");
function liveOutputText(event) {
  assert.equal(event.payload_encoding, "base64");
  assert.equal(event.data, undefined);
  const payload = Buffer.from(event.payload_base64, "base64");
  assert.equal(event.bytes, payload.length);
  return payload.toString("utf8");
}
let restoredPresentation = lateAttachFixture.read_screen_text;
const bufferedLive = lateAttachFixture.history_then_live
  .filter((event) => event.type === "terminal_output")
  .map(liveOutputText)
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
const noHistorySnapshotIndex = lateAttachFixture.no_history_then_live.findIndex(
  (event) => event.type === "snapshot",
);
const noHistoryAttachedIndex = lateAttachFixture.no_history_then_live.findIndex(
  (event) => event.type === "attach_state" && event.state === "attached",
);
const noHistoryLiveIndex = lateAttachFixture.no_history_then_live.findIndex(
  (event) =>
    event.type === "terminal_output" && liveOutputText(event).includes("live-without-history"),
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
assert.notEqual(noHistorySnapshotIndex, -1);
assert.equal(noHistoryAttachingIndex < noHistorySnapshotIndex, true);
assert.equal(noHistorySnapshotIndex < noHistoryAttachedIndex, true);
assert.equal(
  noHistoryLastInitialStateIndex === -1 || noHistoryLastInitialStateIndex < noHistoryAttachedIndex,
  true,
);
assert.equal(noHistoryAttachedIndex < noHistoryFirstTerminalOutputIndex, true);
assert.equal(noHistoryAttachedIndex < noHistoryLiveIndex, true);
const noHistorySnapshot = lateAttachFixture.no_history_then_live[noHistorySnapshotIndex];
const noHistoryPayload = Buffer.from(noHistorySnapshot.payload_base64, "base64");
assert.equal(noHistorySnapshot.bytes, noHistoryPayload.length);
assert.equal(noHistoryPayload.subarray(0, 8).equals(GHOSTSNP_MAGIC), true);
assert.equal(createHash("sha256").update(noHistoryPayload).digest("hex"), GOLDEN_B_SHA256);
assert.notEqual(GOLDEN_A_SHA256, GOLDEN_B_SHA256);
assert.equal(historyPayload.equals(noHistoryPayload), false);
// Exactly one Snapshot on no_history and empty ReadScreen oracle.
assert.equal(
  lateAttachFixture.no_history_then_live.filter((event) => event.type === "snapshot").length,
  2,
);
assert.equal(lateAttachFixture.conformance_fixture_revision, 38);

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
  const fixtureSource = readFileSync(join(fixturePath, "plugin.lua"), "utf8");
  assert.match(fixtureSource, /contract\.app/);
  assert.match(fixtureSource, /contract\.sessions/);
  assert.match(fixtureSource, /source = "\/session"/);
  assert.match(fixtureSource, /kind = "set"/);
  assert.match(fixtureSource, /key = "selected-workspace"/);
  assert.match(fixtureSource, /kind = "clear"/);
  assert.match(fixtureSource, /kind = "toggle"/);
  assertDialogFormComposition(fixtureSource);
  assert.throws(
    () =>
      assertDialogFormComposition(`
        return {
          id = "contract-app-panel",
          children = {
            { id = "contract-dialog", slots = { body = {} } },
            {
              id = "contract-app-form",
              children = { { id = "contract-app-message" } },
            },
          },
        }
      `),
    /contract-app-form must be structurally nested inside contract-dialog/,
  );
  assert.match(
    readFileSync(join(fixturePath, "README.md"), "utf8"),
    /restricts submit discovery to the active Dialog subtree/,
  );

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
