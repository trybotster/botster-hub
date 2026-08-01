import {
  packageVersion,
  realizeBindListDescendantId,
  type UiNodeId,
} from "@trybotster/ui-contract";

const realized: UiNodeId = realizeBindListDescendantId("session-café", "重命名");

if (packageVersion !== "0.3.0" || !realized.startsWith("botster-ui-descendant-v1:")) {
  throw new Error("unexpected UI contract runtime export");
}
