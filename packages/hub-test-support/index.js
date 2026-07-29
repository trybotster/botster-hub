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

export function materializeSessionPluginBindings(surface, frames) {
  if (!Array.isArray(surface?.children)) {
    throw new TypeError("session binding surface children are missing");
  }
  const references = surface.children.map((child) => {
    if (
      child?.$kind !== "bind_list" ||
      child.source !== "/session" ||
      child.item_template?.props?.text?.$bind !== "@/lifecycle_class" ||
      !child.empty_template ||
      typeof child.where?.session_uuid !== "string"
    ) {
      throw new TypeError("surface does not use the canonical /session binding grammar");
    }
    return child.where.session_uuid;
  });
  const entities = new Map();
  for (const frame of frames) {
    if (frame.entity_type !== "session") {
      throw new TypeError("scenario contains a foreign entity family");
    }
    if (frame.type === "entity_snapshot") {
      entities.clear();
      for (const entity of frame.items) {
        entities.set(entity.session_uuid, structuredClone(entity));
      }
    } else if (frame.type === "entity_upsert") {
      entities.set(frame.id, structuredClone(frame.entity));
    } else if (frame.type === "entity_patch") {
      const entity = entities.get(frame.id);
      if (!entity) throw new TypeError(`patch references unknown session row ${frame.id}`);
      mergePatch(entity, frame.patch);
    } else if (frame.type === "entity_remove") {
      entities.delete(frame.id);
    } else {
      throw new TypeError(`unsupported entity frame ${frame.type}`);
    }
  }
  return Object.fromEntries(
    references.map((sessionUuid) => {
      const entity = entities.get(sessionUuid);
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
