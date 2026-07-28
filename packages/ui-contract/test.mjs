import assert from "node:assert/strict";
import fs from "node:fs";
import { conformanceFixtures, packageVersion, schema } from "./index.js";

assert.equal(packageVersion, "0.1.0");
assert.equal(schema.title, "Botster UI Contract");
assert.equal(
  conformanceFixtures.fixtures.request.values.title,
  "Ship contract",
);
assert.equal(
  conformanceFixtures.fixtures.request.payload.source,
  "toolbar",
);
assert.equal(
  conformanceFixtures.fixtures.accepted.presentation.at(-1).kind,
  "clear",
);
assert.equal(
  conformanceFixtures.fixtures.selected_workspace_equality.predicate.kind,
  "equals",
);
assert.equal(
  Object.hasOwn(conformanceFixtures.fixtures.dialog_presence.node.props, "open"),
  false,
);

const declarations = fs.readFileSync(
  new URL("./index.d.ts", import.meta.url),
  "utf8",
);
for (const token of [
  "UiNode",
  "UiActionRequest",
  "UiActionResult",
  "UiPresentationOperation",
  "UiPresentationPredicate",
  "submit_label",
]) {
  assert.match(declarations, new RegExp(token));
}
assert.doesNotMatch(declarations, /UiTreeUpdateRef|tree_update/);
