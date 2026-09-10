#!/usr/bin/env node
// Test the complete shell gate with isolated command fixtures.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const gate = resolve(dirname(fileURLToPath(import.meta.url)), "check-lua-memory-build-contract");
const fixture = mkdtempSync(join(tmpdir(), "botster-memory-gate-test-"));
const bin = join(fixture, "bin");
mkdirSync(bin);
const command = `#!/usr/bin/env node
const name = process.argv[1].split("/").pop();
if (name === "rustc") {
  console.log(process.env.BOTSTER_MEMORY_TEST_RUST);
} else if (process.argv[2] === "metadata") {
  if (process.env.BOTSTER_MEMORY_TEST_FAIL === "metadata") process.exit(42);
  console.log(process.env.BOTSTER_MEMORY_TEST_METADATA);
} else if (process.argv[2] === "tree") {
  if (process.env.BOTSTER_MEMORY_TEST_FAIL === "tree" && !process.argv.includes("mlua@0.11.6")) process.exit(43);
  if (process.env.BOTSTER_MEMORY_TEST_FAIL === "mlua_tree" && process.argv.includes("mlua@0.11.6")) process.exit(46);
  if (process.argv.includes("-i")) {
    console.log(process.argv.includes("mlua@0.11.6") ? process.env.BOTSTER_MEMORY_TEST_MLUA_GRAPH : process.env.BOTSTER_MEMORY_TEST_GRAPH);
  } else {
    if (process.env.BOTSTER_MEMORY_TEST_FAIL === "serde_edges") process.exit(45);
    const versions = JSON.parse(process.env.BOTSTER_MEMORY_TEST_SERDE_VERSIONS);
    for (const target of ["serde", "serde_core", "serde_derive"]) {
      if (process.env.BOTSTER_MEMORY_TEST_MISSING_EDGE === target) continue;
      console.log(target + " v" + (versions[target] ?? "1.0.228") + (target === "serde_derive" ? " (proc-macro)" : "") + (process.env.BOTSTER_MEMORY_TEST_DEDUP ? " (*)" : ""));
    }
    if (process.env.BOTSTER_MEMORY_TEST_EXTRA_EDGE) console.log(process.env.BOTSTER_MEMORY_TEST_EXTRA_EDGE);
  }
} else {
  process.exit(44);
}
`;
for (const name of ["cargo", "rustc"]) {
  const path = join(bin, name);
  writeFileSync(path, command);
  chmodSync(path, 0o755);
}

const dependency = {
  name: "serde_json",
  req: "=1.0.150",
  uses_default_features: true,
  features: ["raw_value"],
};
const serdeDependency = { name: "serde", req: "=1.0.228", uses_default_features: true, features: ["derive"] };
const mluaDependency = { name: "mlua", req: "=0.11.6", uses_default_features: true, features: ["lua54", "vendored", "serialize", "send"] };
const metadata = (dependencies = [dependency], serde = [serdeDependency], mlua = [mluaDependency]) => JSON.stringify({
  packages: [{
    name: "botster-hub",
    manifest_path: join(fixture, "Cargo.toml"),
    dependencies: [...dependencies, ...serde, ...mlua],
  }],
});
const graph = "serde_json v1.0.150|default,raw_value,std\nserde_json v1.0.150|default,std";
const cases = [
  { name: "target and host feature contexts", pass: true },
  { name: "unsupported compiler", rust: "rustc 1.98.0 (fixture)", error: "active rustc" },
  { name: "unsupported toolchain selection", channel: "1.98.0", error: "rust-toolchain.toml" },
  { name: "caret requirement", dependency: { ...dependency, req: "^1.0.150" }, error: "requirement must be" },
  { name: "unsupported direct feature", dependency: { ...dependency, features: ["raw_value", "float_roundtrip"] }, error: "features must be" },
  { name: "unsupported resolved feature", graph: `${graph}\nserde_json v1.0.150|default,float_roundtrip,std`, error: "unsupported resolved" },
  { name: "unsupported resolved version", graph: "serde_json v1.0.151|default,raw_value,std", error: "does not resolve" },
  { name: "missing raw_value", graph: "serde_json v1.0.150|default,std", error: "must enable raw_value" },
  { name: "metadata command failure", fail: "metadata", error: "cargo metadata failed" },
  { name: "feature command failure", fail: "tree", error: "cargo tree failed" },
  { name: "missing root package", metadata: JSON.stringify({ packages: [] }), error: "package missing" },
  { name: "missing direct dependency", metadata: metadata([]), error: "expected one serde_json dependency, found 0" },
  { name: "duplicate direct dependency", metadata: metadata([dependency, dependency]), error: "expected one serde_json dependency, found 2" },
  { name: "invalid metadata JSON", metadata: "{invalid", error: "Lua memory build contract:" },
  { name: "caret Serde requirement", metadata: metadata([dependency], [{ ...serdeDependency, req: "^1.0.228" }]), error: "serde requirement must be" },
  { name: "missing Serde dependency", metadata: metadata([dependency], []), error: "serde requirement must be" },
  ...["serde", "serde_core", "serde_derive"].map((name) => (
    { name: `unsupported ${name} version`, serdeVersions: { [name]: "1.0.229" }, error: `unsupported resolved ${name}` }
  )),
  { name: "Serde edge command failure", fail: "serde_edges", error: "cargo tree failed while checking Serde dependency edges" },
  { name: "additional unsupported derive edge", extraEdge: "serde_derive v1.0.229 (proc-macro)", error: "unsupported resolved serde_derive" },
  ...["serde", "serde_core", "serde_derive"].map((name) => (
    { name: `missing ${name} edge`, missingEdge: name, error: `does not resolve ${name}` }
  )),
  { name: "deduplicated Serde edges", dedup: true, pass: true },
  { name: "caret mlua requirement", metadata: metadata([dependency], [serdeDependency], [{ ...mluaDependency, req: "^0.11.6" }]), error: "mlua requirement must be" },
  { name: "missing mlua dependency", metadata: metadata([dependency], [serdeDependency], []), error: "mlua requirement must be" },
  { name: "unsupported direct mlua feature", metadata: metadata([dependency], [serdeDependency], [{ ...mluaDependency, features: [...mluaDependency.features, "async"] }]), error: "mlua features must be" },
  { name: "unsupported resolved mlua feature", mluaGraph: "mlua v0.11.6|async,error-send,lua54,send,serde,serialize,vendored", error: "unsupported resolved mlua feature" },
  { name: "unsupported resolved mlua version", mluaGraph: "mlua v0.11.7|error-send,lua54,send,serde,serialize,vendored", error: "does not resolve mlua" },
  { name: "mlua feature command failure", fail: "mlua_tree", error: "cargo tree failed while checking resolved mlua features" },
];

try {
  for (const test of cases) {
    writeFileSync(join(fixture, "rust-toolchain.toml"), `[toolchain]\nchannel = "${test.channel ?? "1.97.0"}"\n`);
    const result = spawnSync("sh", [gate], {
      cwd: fixture,
      encoding: "utf8",
      timeout: 30_000,
      env: {
        ...process.env,
        PATH: `${bin}:${process.env.PATH ?? ""}`,
        BOTSTER_MEMORY_TEST_RUST: test.rust ?? "rustc 1.97.0 (fixture)",
        BOTSTER_MEMORY_TEST_METADATA: test.metadata ?? metadata([test.dependency ?? dependency]),
        BOTSTER_MEMORY_TEST_GRAPH: test.graph ?? graph,
        BOTSTER_MEMORY_TEST_MLUA_GRAPH: test.mluaGraph ?? "mlua v0.11.6|error-send,lua54,send,serde,serialize,vendored",
        BOTSTER_MEMORY_TEST_FAIL: test.fail ?? "",
        BOTSTER_MEMORY_TEST_SERDE_VERSIONS: JSON.stringify(test.serdeVersions ?? {}),
        BOTSTER_MEMORY_TEST_EXTRA_EDGE: test.extraEdge ?? "",
        BOTSTER_MEMORY_TEST_MISSING_EDGE: test.missingEdge ?? "",
        BOTSTER_MEMORY_TEST_DEDUP: test.dedup ? "yes" : "",
      },
    });
    assert.ifError(result.error);
    if (test.pass) {
      assert.equal(result.status, 0, `${test.name}: ${result.stderr}`);
      assert.match(result.stdout, /lua_memory_build_contract=rust-1\.97\.0 serde_json-1\.0\.150/);
    } else {
      assert.notEqual(result.status, 0, `${test.name} must fail`);
      assert.ok(result.stderr.includes(test.error), `${test.name}: ${result.stderr}`);
      assert.doesNotMatch(result.stderr, /\n\s+at\s/, `${test.name} must not print a stack trace`);
    }
    console.log(`PASS ${test.name}`);
  }
  console.log(`${cases.length} build-contract cases passed`);
} finally {
  rmSync(fixture, { recursive: true, force: true });
}
