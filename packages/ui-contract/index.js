import schema from "./schema.json" with { type: "json" };
import conformanceFixtures from "./conformance-fixtures.json" with { type: "json" };

export { schema, conformanceFixtures };
export const packageVersion = "0.3.2";

const utf8Encoder = new TextEncoder();

function utf8Bytes(value) {
  return utf8Encoder.encode(value);
}

function utf8ByteCompare(left, right) {
  const a = utf8Bytes(left);
  const b = utf8Bytes(right);
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i += 1) {
    if (a[i] !== b[i]) {
      return a[i] - b[i];
    }
  }
  return a.length - b.length;
}

function compareOptionalUtf8Strings(left, right) {
  if (left != null && right != null) {
    return utf8ByteCompare(left, right);
  }
  if (left != null) {
    return -1;
  }
  if (right != null) {
    return 1;
  }
  return 0;
}

function stringField(record, field) {
  const value = record?.[field];
  return typeof value === "string" ? value : null;
}

function matchesWhere(record, where) {
  if (!where || typeof where !== "object") {
    return true;
  }
  // Authored where values are JSON strings only (Rust validation). Compare
  // exact string identity — never JSON.stringify, which is key-order fragile.
  for (const [key, expected] of Object.entries(where)) {
    if (typeof expected !== "string") {
      return false;
    }
    if (typeof record?.[key] !== "string" || record[key] !== expected) {
      return false;
    }
  }
  return true;
}

/**
 * Realize a descendant identity from the canonical row id and authored key.
 * Byte lengths match Rust `String::len` (UTF-8).
 */
export function realizeBindListDescendantId(rowId, key) {
  if (typeof rowId !== "string" || rowId.trim() === "") {
    throw new TypeError("bound list row identity must be a non-blank string");
  }
  if (typeof key !== "string" || key.trim() === "") {
    throw new TypeError("bound list descendant identity key must be a non-blank string");
  }

  const byteLength = (value) => utf8Bytes(value).byteLength;
  return `botster-ui-descendant-v1:${byteLength(rowId)}:${rowId}${byteLength(key)}:${key}`;
}

/**
 * Map authored absolute family path to SubscribeEntities entity_type, or null.
 */
export function entityFamilySubscriptionId(authoredPath) {
  if (typeof authoredPath !== "string" || !authoredPath.startsWith("/")) {
    return null;
  }
  const rest = authoredPath.slice(1);
  if (!rest || rest.includes("/")) {
    return null;
  }
  return rest;
}

/**
 * Pure projector: string-only values, UTF-8 byte order, first-after-sort wins.
 */
export function projectEntityOptions(
  descriptor,
  sourceRecords,
  excludeRecords,
  selection = null,
) {
  const excluded = new Set();
  if (descriptor?.exclude) {
    for (const record of Object.values(excludeRecords ?? {})) {
      if (!matchesWhere(record, descriptor.exclude.where)) {
        continue;
      }
      const value = stringField(record, descriptor.exclude.value_field);
      if (value != null) {
        excluded.add(value);
      }
    }
  }

  const ranked = [];
  for (const [recordId, record] of Object.entries(sourceRecords ?? {})) {
    if (!matchesWhere(record, descriptor.where)) {
      continue;
    }
    const value = stringField(record, descriptor.value_field);
    if (value == null || excluded.has(value)) {
      continue;
    }
    const metadata = {};
    let label = "";
    let labelSet = false;
    for (const field of descriptor.display_fields ?? []) {
      const text = stringField(record, field);
      if (text != null) {
        if (!labelSet) {
          label = text;
          labelSet = true;
        }
        metadata[field] = text;
      }
    }
    const orderKeys = (descriptor.order ?? []).map((key) => stringField(record, key));
    ranked.push({
      option: { value, label, metadata },
      orderKeys,
      recordId: String(recordId),
    });
  }

  // order keys → option value → record id (UTF-8 bytes). Record id makes
  // first-after-sort independent of Object.entries insertion order.
  ranked.sort((left, right) => {
    for (let i = 0; i < left.orderKeys.length; i += 1) {
      const cmp = compareOptionalUtf8Strings(left.orderKeys[i], right.orderKeys[i]);
      if (cmp !== 0) {
        return cmp;
      }
    }
    const valueCmp = utf8ByteCompare(left.option.value, right.option.value);
    if (valueCmp !== 0) {
      return valueCmp;
    }
    return utf8ByteCompare(left.recordId, right.recordId);
  });

  const seen = new Set();
  const options = [];
  for (const entry of ranked) {
    if (seen.has(entry.option.value)) {
      continue;
    }
    seen.add(entry.option.value);
    // Drop empty metadata objects for wire parity with serde skip_serializing_if
    const option = {
      value: entry.option.value,
      label: entry.option.label,
    };
    if (Object.keys(entry.option.metadata).length > 0) {
      option.metadata = entry.option.metadata;
    }
    options.push(option);
  }

  let selectionValid = true;
  if (selection != null) {
    if (typeof selection !== "string") {
      selectionValid = false;
    } else {
      selectionValid = options.some((option) => option.value === selection);
    }
  }

  return { options, selection_valid: selectionValid };
}

function walkNode(node, families) {
  if (!node || typeof node !== "object") {
    return;
  }
  if (node.type === "select" && node.props?.options_source?.$kind === "entity_options") {
    const source = entityFamilySubscriptionId(node.props.options_source.source);
    if (source) {
      families.add(source);
    }
    const excludeSource = entityFamilySubscriptionId(
      node.props.options_source.exclude?.source,
    );
    if (excludeSource) {
      families.add(excludeSource);
    }
  }
  for (const child of node.children ?? []) {
    walkChild(child, families);
  }
  for (const slot of Object.values(node.slots ?? {})) {
    for (const child of slot ?? []) {
      walkChild(child, families);
    }
  }
}

function walkChild(child, families) {
  if (!child || typeof child !== "object") {
    return;
  }
  if (child.$kind === "bind_list") {
    walkNode(child.item_template, families);
    if (child.empty_template) {
      walkNode(child.empty_template, families);
    }
    return;
  }
  if (
    child.$kind === "bind_if" ||
    child.$kind === "presentation_if" ||
    child.$kind === "when" ||
    child.$kind === "hidden"
  ) {
    walkNode(child.node, families);
    return;
  }
  walkNode(child, families);
}

/**
 * Collect slash-stripped SubscribeEntities family ids from a UiNode tree.
 */
export function collectEntityOptionFamilies(node) {
  const families = new Set();
  walkNode(node, families);
  return [...families].sort((a, b) => utf8ByteCompare(a, b));
}

/**
 * Apply one canonical entity frame to a multi-family store (test helper).
 */
export function applyEntityOptionsFrame(store, frame) {
  const type = frame.type;
  if (type === "snapshot") {
    const family = {};
    for (const item of frame.items ?? []) {
      const { id, ...rest } = item;
      const fields = { ...rest };
      if (fields.id == null) {
        fields.id = id;
      }
      family[id] = fields;
    }
    store[frame.entity_type] = family;
    return;
  }
  if (type === "upsert") {
    if (!store[frame.entity_type]) {
      store[frame.entity_type] = {};
    }
    const fields = { ...frame.fields };
    if (fields.id == null) {
      fields.id = frame.id;
    }
    store[frame.entity_type][frame.id] = fields;
    return;
  }
  if (type === "patch") {
    if (!store[frame.entity_type]) {
      store[frame.entity_type] = {};
    }
    const existing = store[frame.entity_type][frame.id] ?? {};
    const fields = { ...existing, ...frame.fields };
    if (fields.id == null) {
      fields.id = frame.id;
    }
    store[frame.entity_type][frame.id] = fields;
    return;
  }
  if (type === "remove") {
    if (store[frame.entity_type]) {
      delete store[frame.entity_type][frame.id];
    }
  }
}
