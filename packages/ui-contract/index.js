import schema from "./schema.json" with { type: "json" };
import conformanceFixtures from "./conformance-fixtures.json" with { type: "json" };

export { schema, conformanceFixtures };
export const packageVersion = "0.3.0";

export function realizeBindListDescendantId(rowId, key) {
  if (typeof rowId !== "string" || rowId.trim() === "") {
    throw new TypeError("bound list row identity must be a non-blank string");
  }
  if (typeof key !== "string" || key.trim() === "") {
    throw new TypeError("bound list descendant identity key must be a non-blank string");
  }

  const byteLength = (value) => new TextEncoder().encode(value).byteLength;
  return `botster-ui-descendant-v1:${byteLength(rowId)}:${rowId}${byteLength(key)}:${key}`;
}
