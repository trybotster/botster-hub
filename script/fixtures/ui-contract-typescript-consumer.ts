import {
  NOTICE_TEXT_MAX_BYTES,
  packageVersion,
  realizeBindListDescendantId,
  resolveNoticeText,
  type PackageNoticeReactionDescriptor,
  type UiNodeId,
} from "@trybotster/ui-contract";

const realized: UiNodeId = realizeBindListDescendantId("session-café", "重命名");
const descriptor: PackageNoticeReactionDescriptor = {
  owner: "event-plane-producer",
  name: "sample.ready",
  subject_scope: "session",
  text_pointer: "/notice",
  ttl_ms: 5000,
  severity: "info",
};
const notice = resolveNoticeText({ notice: "ready" }, descriptor.text_pointer);

if (
  packageVersion !== "0.3.3" ||
  NOTICE_TEXT_MAX_BYTES !== 512 ||
  !realized.startsWith("botster-ui-descendant-v1:") ||
  notice !== "ready" ||
  descriptor.owner.length === 0
) {
  throw new Error("unexpected UI contract runtime export");
}
