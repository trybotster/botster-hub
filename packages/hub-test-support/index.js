import { createHash } from "node:crypto";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
} from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { realizeBindListDescendantId } from "@trybotster/ui-contract";

const packageRoot = fileURLToPath(new URL(".", import.meta.url));

function packagePath(...segments) {
  return join(packageRoot, ...segments);
}

function readJson(relativePath) {
  return JSON.parse(readFileSync(packagePath(relativePath), "utf8"));
}

function sha256File(relativePath) {
  return createHash("sha256")
    .update(readFileSync(packagePath(relativePath)))
    .digest("hex");
}

function copyDirectory(source, destination) {
  mkdirSync(destination, { recursive: true });

  for (const entry of readdirSync(source, { withFileTypes: true })) {
    const from = join(source, entry.name);
    const to = join(destination, entry.name);

    if (entry.isDirectory()) {
      copyDirectory(from, to);
    } else if (entry.isFile()) {
      mkdirSync(dirname(to), { recursive: true });
      copyFileSync(from, to);
    }
  }
}

export const metadata = readJson("metadata.json");

export function daemonProtocolTypescriptPath() {
  return packagePath(metadata.daemon_protocol.artifact_path);
}

export function readDaemonProtocolTypescript() {
  return readFileSync(daemonProtocolTypescriptPath(), "utf8");
}

export function firstPartyClientSupportMatrixPath() {
  return packagePath(metadata.first_party_client_support_matrix.artifact_path);
}

export function readFirstPartyClientSupportMatrix() {
  return readJson(metadata.first_party_client_support_matrix.artifact_path);
}

export function sessionLifecycleSubscriptionConformanceFixturePath() {
  return packagePath(metadata.session_lifecycle_subscription_conformance_fixture.artifact_path);
}

export function readSessionLifecycleSubscriptionConformanceFixture() {
  return readJson(metadata.session_lifecycle_subscription_conformance_fixture.artifact_path);
}

export function sessionPluginBindingConformanceFixturePath() {
  return packagePath(metadata.session_plugin_binding_conformance_fixture.artifact_path);
}

export function readSessionPluginBindingConformanceFixture() {
  return readJson(metadata.session_plugin_binding_conformance_fixture.artifact_path);
}

function mergePatch(target, patch) {
  for (const [key, value] of Object.entries(patch)) {
    if (value === null) {
      delete target[key];
    } else if (
      value &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      target[key] &&
      typeof target[key] === "object" &&
      !Array.isArray(target[key])
    ) {
      mergePatch(target[key], value);
    } else {
      target[key] = structuredClone(value);
    }
  }
}

function inspectSessionPluginSurface(surface) {
  if (!Array.isArray(surface?.children)) {
    throw new TypeError("session binding surface children are missing");
  }
  const references = [];
  let oracle;
  for (const child of surface.children) {
    if (typeof child?.where?.session_uuid === "string") {
      const index = references.length + 1;
      const expected = {
        $kind: "bind_list",
        source: "/session",
        where: { session_uuid: child.where.session_uuid },
        item_template: {
          type: "text",
          id: `contract-session-${index}-lifecycle`,
          props: { text: { $bind: "@/lifecycle_class" } },
        },
        empty_template: {
          type: "text",
          id: `contract-session-${index}-unavailable`,
          props: { text: "Session unavailable" },
        },
      };
      if (!isDeepStrictEqual(child, expected)) {
        throw new TypeError("surface does not use the canonical /session binding grammar");
      }
      references.push(child.where.session_uuid);
      continue;
    }

    const expectedOracle = {
      $kind: "bind_list",
      source: "/session",
      where: { lifecycle_class: "current" },
      item_template: {
        type: "inline",
        id: { $bind: "@/session_uuid" },
        children: ["spawn", "rename", "remove"].map((key) => ({
          type: "button",
          id: { $kind: "bind_list_descendant_id", key },
          props: {
            label:
              key === "spawn"
                ? { $bind: "@/lifecycle_class" }
                : `${key[0].toUpperCase()}${key.slice(1)} session`,
            action: {
              id: "contract.action",
              payload: {
                operation: key,
                session_uuid: { $bind: "@/session_uuid" },
              },
            },
          },
        })),
      },
    };
    if (!isDeepStrictEqual(child, expectedOracle)) {
      throw new TypeError("surface contains an unrecognized session binding child");
    }
    if (oracle) {
      throw new TypeError("surface contains duplicate current-row identity oracles");
    }
    oracle = child;
  }
  if (!oracle) {
    throw new TypeError("surface is missing the current-row identity oracle");
  }
  return { references, oracle };
}

function materializeSessionEntities(frames) {
  const entities = [];
  for (const frame of frames) {
    if (frame.entity_type !== "session") {
      throw new TypeError("scenario contains a foreign entity family");
    }
    if (frame.type === "entity_snapshot") {
      entities.length = 0;
      for (const entity of frame.items) {
        entities.push([entity.session_uuid, structuredClone(entity)]);
      }
    } else if (frame.type === "entity_upsert") {
      const index = entities.findIndex(([id]) => id === frame.id);
      if (index === -1) entities.push([frame.id, structuredClone(frame.entity)]);
      else entities[index][1] = structuredClone(frame.entity);
    } else if (frame.type === "entity_patch") {
      const entity = entities.find(([id]) => id === frame.id)?.[1];
      if (!entity) throw new TypeError(`patch references unknown session row ${frame.id}`);
      mergePatch(entity, frame.patch);
    } else if (frame.type === "entity_remove") {
      const index = entities.findIndex(([id]) => id === frame.id);
      if (index !== -1) entities.splice(index, 1);
    } else {
      throw new TypeError(`unsupported entity frame ${frame.type}`);
    }
  }
  return entities;
}

export function materializeSessionPluginBindings(surface, frames) {
  const { references } = inspectSessionPluginSurface(surface);
  const entities = materializeSessionEntities(frames);
  return Object.fromEntries(
    references.map((sessionUuid) => {
      const entity = entities.find(([id]) => id === sessionUuid)?.[1];
      if (!entity) return [sessionUuid, "unavailable"];
      if (typeof entity.lifecycle_class !== "string") {
        throw new TypeError(
          `present session row ${sessionUuid} is missing lifecycle_class`,
        );
      }
      return [sessionUuid, entity.lifecycle_class];
    }),
  );
}

export function materializeSessionPluginRows(surface, frames) {
  const { oracle } = inspectSessionPluginSurface(surface);
  const expectedClass = oracle.where.lifecycle_class;
  const entities = materializeSessionEntities(frames);
  const seen = new Set();
  insertRealizedNodeId(seen, surface.id);
  for (const child of surface.children) {
    if (child === oracle) continue;
    const hasRow = entities.some(
      ([, entity]) => entity.session_uuid === child.where.session_uuid,
    );
    collectLiteralNodeIds(
      hasRow ? child.item_template : child.empty_template,
      seen,
    );
  }
  const controls = oracle.item_template.children;
  const rows = [];
  for (const [, entity] of entities) {
    if (entity.lifecycle_class !== expectedClass) continue;
    if (typeof entity.session_uuid !== "string") {
      throw new TypeError("selected session row is missing string session_uuid");
    }
    if (entity.session_uuid.trim() === "") {
      throw new TypeError("selected session row has blank session_uuid");
    }
    insertRealizedNodeId(seen, entity.session_uuid);
    rows.push({
      node_id: entity.session_uuid,
      controls: controls.map((control) => {
        const key = control.id.key;
        const nodeId = realizeBindListDescendantId(entity.session_uuid, key);
        insertRealizedNodeId(seen, nodeId);
        const label = materializeControlLabel(control, entity);
        const actionPayload = {
          operation: key,
          session_uuid: entity.session_uuid,
        };
        assertRealizedRequiredValues({
          type: "button",
          id: nodeId,
          props: {
            label,
            action: { id: "contract.action", payload: actionPayload },
          },
        });
        return {
          key,
          node_id: nodeId,
          label,
          action_payload: actionPayload,
        };
      }),
    });
  }
  return rows;
}

function materializeControlLabel(control, entity) {
  const label = control?.props?.label;
  if (typeof label === "string") return label;
  const path = label?.$bind;
  if (typeof path !== "string" || !path.startsWith("@/")) {
    throw new TypeError("identity-bearing control label is not a string or item-relative bind");
  }
  const field = path.slice(2);
  if (typeof entity[field] !== "string") {
    throw new TypeError(`selected session row is missing string ${field}`);
  }
  return entity[field];
}

function assertRealizedRequiredValues(value) {
  if (Array.isArray(value)) {
    for (const child of value) assertRealizedRequiredValues(child);
    return;
  }
  if (!value || typeof value !== "object") return;
  if (Object.hasOwn(value, "$bind")) {
    throw new TypeError("unresolved binding sentinel remains after materialization");
  }
  for (const child of Object.values(value)) assertRealizedRequiredValues(child);
}

function insertRealizedNodeId(seen, nodeId) {
  if (typeof nodeId !== "string" || nodeId.trim() === "") {
    throw new TypeError("realized node id must be a non-blank string");
  }
  if (seen.has(nodeId)) {
    throw new TypeError(`duplicate realized node id ${nodeId}`);
  }
  seen.add(nodeId);
}

function collectLiteralNodeIds(value, seen) {
  if (Array.isArray(value)) {
    for (const child of value) collectLiteralNodeIds(child, seen);
    return;
  }
  if (!value || typeof value !== "object") return;
  if (typeof value.type === "string" && typeof value.id === "string") {
    insertRealizedNodeId(seen, value.id);
  }
  for (const [key, child] of Object.entries(value)) {
    if (key !== "id") collectLiteralNodeIds(child, seen);
  }
}

export function materializeSessionPluginBindingScenario(scenario) {
  const frames = [scenario.initial_snapshot];
  const initial = materializeSessionPluginBindings(scenario.surface, frames);
  frames.push(scenario.transition_frames[0]);
  const after_ended_patch = materializeSessionPluginBindings(scenario.surface, frames);
  frames.push(scenario.transition_frames[1]);
  const after_indeterminate_patch = materializeSessionPluginBindings(scenario.surface, frames);
  frames.push(scenario.transition_frames[2]);
  const after_remove = materializeSessionPluginBindings(scenario.surface, frames);
  const after_reconnect = materializeSessionPluginBindings(
    scenario.surface,
    [scenario.reconnect_snapshot],
  );
  return {
    initial,
    after_ended_patch,
    after_indeterminate_patch,
    after_remove,
    after_reconnect,
  };
}

export function materializeSessionPluginRowScenario(scenario) {
  const frames = [scenario.initial_snapshot];
  const initial = materializeSessionPluginRows(scenario.surface, frames);
  frames.push(scenario.transition_frames[0]);
  const after_ended_patch = materializeSessionPluginRows(scenario.surface, frames);
  frames.push(scenario.transition_frames[1]);
  const after_indeterminate_patch = materializeSessionPluginRows(scenario.surface, frames);
  frames.push(scenario.transition_frames[2]);
  const after_remove = materializeSessionPluginRows(scenario.surface, frames);
  const after_reconnect = materializeSessionPluginRows(
    scenario.surface,
    [scenario.reconnect_snapshot],
  );
  return {
    initial,
    after_ended_patch,
    after_indeterminate_patch,
    after_remove,
    after_reconnect,
  };
}

export function lateAttachHistoryConformanceFixturePath() {
  return packagePath(metadata.late_attach_history_conformance_fixture.artifact_path);
}

export function readLateAttachHistoryConformanceFixture() {
  return readJson(metadata.late_attach_history_conformance_fixture.artifact_path);
}

export function localWebrtcDeliveryChunkConformanceFixturePath() {
  return packagePath(metadata.local_webrtc_delivery_chunk_conformance_fixture.artifact_path);
}

export function readLocalWebrtcDeliveryChunkConformanceFixture() {
  return readJson(metadata.local_webrtc_delivery_chunk_conformance_fixture.artifact_path);
}

export function modeFlagsConformanceFixturePath() {
  return packagePath(metadata.mode_flags_conformance_fixture.artifact_path);
}

export function readModeFlagsConformanceFixture() {
  return readJson(metadata.mode_flags_conformance_fixture.artifact_path);
}

export async function readUiContractConformanceFixtures() {
  const uiContract = await import("@trybotster/ui-contract");
  return uiContract.conformanceFixtures;
}

export function pluginContractMatrixFixturePath() {
  return packagePath(metadata.plugin_contract_matrix.artifact_path);
}

export function materializePluginContractMatrixFixture(destination) {
  if (!destination) {
    throw new TypeError("destination is required");
  }

  const target = join(destination, metadata.plugin_contract_matrix.artifact_path);
  copyDirectory(pluginContractMatrixFixturePath(), target);
  return target;
}

export function applicationPrimitivesFixturePath() {
  return pluginContractMatrixFixturePath();
}

export function materializeApplicationPrimitivesFixture(destination) {
  return materializePluginContractMatrixFixture(destination);
}

export function verifyPackageAssets() {
  const failures = [];

  if (!existsSync(daemonProtocolTypescriptPath())) {
    failures.push(`${metadata.daemon_protocol.artifact_path} is missing`);
  } else if (sha256File(metadata.daemon_protocol.artifact_path) !== metadata.daemon_protocol.sha256) {
    failures.push(`${metadata.daemon_protocol.artifact_path} checksum mismatch`);
  }

  for (const asset of [
    metadata.first_party_client_support_matrix,
    metadata.session_lifecycle_subscription_conformance_fixture,
    metadata.session_plugin_binding_conformance_fixture,
    metadata.late_attach_history_conformance_fixture,
    metadata.local_webrtc_delivery_chunk_conformance_fixture,
    metadata.mode_flags_conformance_fixture,
  ]) {
    if (!existsSync(packagePath(asset.artifact_path))) {
      failures.push(`${asset.artifact_path} is missing`);
    } else if (sha256File(asset.artifact_path) !== asset.sha256) {
      failures.push(`${asset.artifact_path} checksum mismatch`);
    }
  }

  for (const file of metadata.plugin_contract_matrix.files) {
    const relativePath = join(metadata.plugin_contract_matrix.artifact_path, file.path);
    if (!existsSync(packagePath(relativePath))) {
      failures.push(`${relativePath} is missing`);
    } else if (sha256File(relativePath) !== file.sha256) {
      failures.push(`${relativePath} checksum mismatch`);
    }
  }

  return {
    ok: failures.length === 0,
    failures,
  };
}
