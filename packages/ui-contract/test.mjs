import assert from "node:assert/strict";
import fs from "node:fs";
import {
  conformanceFixtures,
  packageVersion,
  realizeBindListDescendantId,
  schema,
} from "./index.js";

const packageManifest = JSON.parse(
  fs.readFileSync(new URL("./package.json", import.meta.url), "utf8"),
);
assert.equal(packageVersion, packageManifest.version);
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
assert.deepEqual(
  conformanceFixtures.fixtures.bound_row_identity.children[0].item_template.id,
  { $bind: "@/session_uuid" },
);
for (const vector of conformanceFixtures.bind_list_descendant_identity_vectors) {
  assert.equal(
    realizeBindListDescendantId(vector.row, vector.key),
    vector.realized_id,
  );
}
assert.throws(() => realizeBindListDescendantId(" ", "remove"), /non-blank/);
assert.throws(() => realizeBindListDescendantId("session-1", "\t"), /non-blank/);
assert.equal(
  schema.$defs.UiNode.properties.id.$ref,
  "#/$defs/UiAuthoredNodeId",
);
assert.equal(
  schema.$defs.UiActionRequest.properties.node_id.$ref,
  "#/$defs/UiNodeId",
);
assert.equal(
  schema.$defs.UiActionResult.properties.node_id.$ref,
  "#/$defs/UiNodeId",
);
assert.match(
  schema.$defs.UiAuthoredNodeId.oneOf[1].description,
  /schema validation is necessary but not sufficient/i,
);

const declarations = fs.readFileSync(
  new URL("./index.d.ts", import.meta.url),
  "utf8",
);
for (const token of [
  "UiNode",
  "UiAuthoredNodeId",
  "UiBindListDescendantId",
  "UiActionRequest",
  "UiActionResult",
  "UiPresentationOperation",
  "UiPresentationPredicate",
  "submit_label",
]) {
  assert.match(declarations, new RegExp(token));
}
assert.doesNotMatch(declarations, /UiTreeUpdateRef|tree_update/);
