import assert from "node:assert/strict";
import fs from "node:fs";
import {
  applyEntityOptionsFrame,
  collectEntityOptionFamilies,
  conformanceFixtures,
  entityFamilySubscriptionId,
  packageVersion,
  projectEntityOptions,
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
  Object.hasOwn(conformanceFixtures.fixtures.dialog_presence.node.props, "open"),
  false,
);
assert.deepEqual(
  conformanceFixtures.fixtures.bound_row_identity.children[0].item_template.id,
  { $bind: "@/session_uuid" },
);
assert.equal(conformanceFixtures.contract_version, "0.3.2");
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
assert.equal(schema.$defs.UiEntityOptionsSource.properties.$kind.const, "entity_options");

const timeline = conformanceFixtures.entity_options_reactive_timeline;
assert.ok(timeline, "entity_options_reactive_timeline fixture present");
assert.equal(timeline.descriptor.$kind, "entity_options");

for (const vector of timeline.collector_vectors) {
  assert.equal(
    entityFamilySubscriptionId(vector.authored_path),
    vector.subscription_id,
    `collector oracle for ${vector.authored_path}`,
  );
}
assert.deepEqual(
  collectEntityOptionFamilies(timeline.sample_node),
  timeline.collector_from_sample_node,
);

const store = {};
for (const step of timeline.timeline) {
  for (const frame of step.frames) {
    applyEntityOptionsFrame(store, frame);
  }
  assert.deepEqual(
    store,
    step.expected_store,
    `store after step ${step.name}`,
  );
  const sourceKey = entityFamilySubscriptionId(timeline.descriptor.source);
  const excludeKey = entityFamilySubscriptionId(
    timeline.descriptor.exclude?.source,
  );
  const projection = projectEntityOptions(
    timeline.descriptor,
    store[sourceKey] ?? {},
    excludeKey ? (store[excludeKey] ?? {}) : {},
    timeline.selection,
  );
  assert.deepEqual(
    projection,
    step.expected_projection,
    `projection after step ${step.name}`,
  );
}

// Duplicate values with equal order keys: record-id UTF-8 order decides,
// independent of Object.entries insertion order.
const dupDescriptor = {
  $kind: "entity_options",
  source: "/session",
  value_field: "session_uuid",
  display_fields: ["label"],
  order: ["label"],
};
const dupZFirst = {
  "z-late": { session_uuid: "dup", label: "Same" },
  "a-early": { session_uuid: "dup", label: "Same" },
};
const dupAFirst = {
  "a-early": { session_uuid: "dup", label: "Same" },
  "z-late": { session_uuid: "dup", label: "Same" },
};
assert.deepEqual(
  projectEntityOptions(dupDescriptor, dupZFirst, {}, null),
  projectEntityOptions(dupDescriptor, dupAFirst, {}, null),
);
assert.equal(
  projectEntityOptions(dupDescriptor, dupZFirst, {}, null).options[0].value,
  "dup",
);

// String where equality is exact; non-string where expected fails closed.
assert.deepEqual(
  projectEntityOptions(
    {
      ...dupDescriptor,
      where: { lifecycle_class: "current" },
    },
    {
      s1: { session_uuid: "a", label: "A", lifecycle_class: "current" },
      s2: { session_uuid: "b", label: "B", lifecycle_class: "exited" },
    },
    {},
    null,
  ).options.map((o) => o.value),
  ["a"],
);
assert.deepEqual(
  projectEntityOptions(
    {
      ...dupDescriptor,
      where: { lifecycle_class: { nested: true } },
    },
    {
      s1: { session_uuid: "a", label: "A", lifecycle_class: "current" },
    },
    {},
    null,
  ).options,
  [],
);

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
  "UiSelectProps",
  "UiEntityOptionsSource",
  "projectEntityOptions",
  "collectEntityOptionFamilies",
  "entityFamilySubscriptionId",
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
