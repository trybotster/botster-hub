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

export function lateAttachHistoryConformanceFixturePath() {
  return packagePath(metadata.late_attach_history_conformance_fixture.artifact_path);
}

export function readLateAttachHistoryConformanceFixture() {
  return readJson(metadata.late_attach_history_conformance_fixture.artifact_path);
}

export function localWebrtcResponseChunkConformanceFixturePath() {
  return packagePath(metadata.local_webrtc_response_chunk_conformance_fixture.artifact_path);
}

export function readLocalWebrtcResponseChunkConformanceFixture() {
  return readJson(metadata.local_webrtc_response_chunk_conformance_fixture.artifact_path);
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
    metadata.late_attach_history_conformance_fixture,
    metadata.local_webrtc_response_chunk_conformance_fixture,
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
