import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
const repoRoot = resolve(packageRoot, "../..");
const check = process.argv.includes("--check");

function fail(message) {
  console.error(message);
  process.exit(1);
}

function runRustOriginEmitter(outputDir) {
  const result = spawnSync(
    "cargo",
    [
      "run",
      "--quiet",
      "-p",
      "botster-hub-test-support",
      "--example",
      "node_package_assets",
      "--",
      outputDir,
    ],
    {
      cwd: repoRoot,
      encoding: "utf8",
    },
  );

  if (result.status !== 0) {
    fail([
      "failed to generate Node package assets from Rust test support",
      result.stdout,
      result.stderr,
    ].filter(Boolean).join("\n"));
  }
}

function listFiles(root) {
  const files = [];

  function walk(dir) {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(path);
      } else if (entry.isFile()) {
        files.push(relative(root, path).split(/[\\/]/).join("/"));
      }
    }
  }

  walk(root);
  return files.sort();
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

function sha256(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function metadataJson(originDir) {
  const origin = JSON.parse(readFileSync(join(originDir, "metadata-origin.json"), "utf8"));
  const fixtureRoot = join(originDir, origin.plugin_contract_matrix.artifact_path);
  const fixtureFiles = listFiles(fixtureRoot).map((path) => ({
    path,
    sha256: sha256(join(fixtureRoot, path)),
  }));

  return {
    package_name: "@trybotster/hub-test-support",
    package_version: JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8")).version,
    protocol: origin.protocol,
    protocol_version: origin.protocol_version,
    conformance_fixture_revision: origin.conformance_fixture_revision,
    generated_by: "cargo run -p botster-hub-test-support --example node_package_assets",
    daemon_protocol: {
      artifact_path: "daemon-protocol.ts",
      source_artifact_path: origin.daemon_protocol_source_artifact,
      sha256: sha256(join(originDir, "daemon-protocol.ts")),
    },
    plugin_contract_matrix: {
      package_name: origin.plugin_contract_matrix.package_name,
      artifact_path: origin.plugin_contract_matrix.artifact_path,
      source_artifact_path: "botster_hub_test_support::plugin_contract_matrix_fixture_asset()",
      files: fixtureFiles,
    },
  };
}

function stableJson(value) {
  return `${JSON.stringify(value, null, 2)}\n`;
}

function compareFile(packageRelativePath, expectedPath, failures) {
  const actualPath = join(packageRoot, packageRelativePath);
  if (!existsSync(actualPath)) {
    failures.push(`${packageRelativePath} is missing`);
    return;
  }
  if (readFileSync(actualPath).compare(readFileSync(expectedPath)) !== 0) {
    failures.push(`${packageRelativePath} is stale`);
  }
}

const originDir = mkdtempSync(join(tmpdir(), "botster-hub-test-support-"));

try {
  runRustOriginEmitter(originDir);
  const metadata = metadataJson(originDir);

  if (check) {
    const failures = [];
    compareFile("daemon-protocol.ts", join(originDir, "daemon-protocol.ts"), failures);
    const expectedMetadata = stableJson(metadata);
    const actualMetadataPath = join(packageRoot, "metadata.json");
    if (!existsSync(actualMetadataPath)) {
      failures.push("metadata.json is missing");
    } else if (readFileSync(actualMetadataPath, "utf8") !== expectedMetadata) {
      failures.push("metadata.json is stale");
    }

    for (const file of metadata.plugin_contract_matrix.files) {
      compareFile(
        join(metadata.plugin_contract_matrix.artifact_path, file.path),
        join(originDir, metadata.plugin_contract_matrix.artifact_path, file.path),
        failures,
      );
    }

    if (failures.length > 0) {
      fail(`package assets are stale:\n- ${failures.join("\n- ")}`);
    }

    console.log("hub test-support package assets are current");
  } else {
    copyFileSync(join(originDir, "daemon-protocol.ts"), join(packageRoot, "daemon-protocol.ts"));
    rmSync(join(packageRoot, "fixtures", "plugin-contract-matrix"), { recursive: true, force: true });
    copyDirectory(
      join(originDir, metadata.plugin_contract_matrix.artifact_path),
      join(packageRoot, metadata.plugin_contract_matrix.artifact_path),
    );
    writeFileSync(join(packageRoot, "metadata.json"), stableJson(metadata));
    console.log("synced hub test-support package assets from Rust test support");
  }
} finally {
  rmSync(originDir, { recursive: true, force: true });
}
