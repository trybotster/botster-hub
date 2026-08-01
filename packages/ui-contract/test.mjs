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
assert.equal(conformanceFixtures.contract_version, "0.3.1");
assert.equal(
  conformanceFixtures.fixtures.required_bindable_fields.authored.length,
  7,
);
assert.deepEqual(
  conformanceFixtures.fixtures.required_bindable_fields.authored[0].props.label,
  { $bind: "@/lifecycle_class" },
);
assert.equal(
  conformanceFixtures.fixtures.required_bindable_fields.realized[0].props.label,
  "current",
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
assert.equal(schema.$defs.UiBind.additionalProperties, false);
assert.deepEqual(schema.$defs.UiBind.required, ["$bind"]);
assert.equal(
  schema.$defs.UiBindableString.oneOf[1].$ref,
  "#/$defs/UiBind",
);
assert.deepEqual(schema.$defs.UiNonBindableValue.not.required, ["$bind"]);

const declarations = fs.readFileSync(
  new URL("./index.d.ts", import.meta.url),
  "utf8",
);
for (const token of [
  "UiNode",
  "UiBindableString",
  "UiAuthoredTextValue",
  "UiIconButtonProps",
  "UiMenuItemProps",
  "UiIframeProps",
  "UiFieldControlProps",
  "UiSelectOptionProps",
  "UiAuthoredNodeId",
  "UiBindListDescendantId",
  "realizeBindListDescendantId",
  "packageVersion",
  "conformanceFixtures",
  "schema",
  "UiActionRequest",
  "UiActionResult",
  "UiPresentationOperation",
  "UiPresentationPredicate",
  "submit_label",
]) {
  assert.match(declarations, new RegExp(token));
}
assert.doesNotMatch(declarations, /UiTreeUpdateRef|tree_update/);
