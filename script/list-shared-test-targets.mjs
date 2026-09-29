#!/usr/bin/env node
// Prints `--test <name>` for every integration test target that the gate runs
// without extra features, except the lifecycle target (the lifecycle phase runs
// that one). It fails when the lifecycle target is missing, so a rename cannot
// silently drop it from both phases.
import { execFileSync } from "node:child_process";

const LIFECYCLE = "hub_daemon_lifecycle_test";
const metadata = JSON.parse(
  execFileSync("cargo", ["metadata", "--no-deps", "--format-version", "1", "--locked"], {
    encoding: "utf8",
    maxBuffer: 256 * 1024 * 1024,
  }),
);
const workspace = new Set(metadata.workspace_members);
const names = new Set();
let lifecycleFound = false;
for (const pkg of metadata.packages) {
  if (!workspace.has(pkg.id)) continue;
  for (const target of pkg.targets) {
    if (!target.kind.includes("test")) continue;
    if ((target["required-features"] ?? []).length > 0) continue;
    if (target.name === LIFECYCLE) {
      lifecycleFound = true;
      continue;
    }
    names.add(target.name);
  }
}
if (!lifecycleFound) {
  console.error(`the lifecycle target ${LIFECYCLE} is not a workspace test target; update test.sh and this script`);
  process.exit(1);
}
process.stdout.write([...names].sort().map((name) => `--test ${name}`).join(" ") + "\n");
