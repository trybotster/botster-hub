export interface PackageAssetChecksum {
  path: string;
  sha256: string;
}

export interface HubTestSupportMetadata {
  package_name: "@trybotster/hub-test-support";
  package_version: string;
  protocol: string;
  protocol_version: number;
  conformance_fixture_revision: number;
  generated_by: string;
  daemon_protocol: {
    artifact_path: "daemon-protocol.ts";
    source_artifact_path: string;
    sha256: string;
  };
  first_party_client_support_matrix: {
    artifact_path: "first-party-client-support-matrix.json";
    source_artifact_path: string;
    sha256: string;
  };
  session_lifecycle_subscription_conformance_fixture: {
    artifact_path: "session-lifecycle-subscription-conformance-fixture.json";
    source_artifact_path: string;
    sha256: string;
  };
  late_attach_history_conformance_fixture: {
    artifact_path: "late-attach-history-conformance-fixture.json";
    source_artifact_path: string;
    sha256: string;
  };
  local_webrtc_delivery_chunk_conformance_fixture: {
    artifact_path: "local-webrtc-delivery-chunk-conformance-fixture.json";
    source_artifact_path: string;
    sha256: string;
  };
  mode_flags_conformance_fixture: {
    artifact_path: "mode-flags-conformance-fixture.json";
    source_artifact_path: string;
    sha256: string;
  };
  plugin_contract_matrix: {
    package_name: string;
    artifact_path: "fixtures/plugin-contract-matrix";
    source_artifact_path: string;
    files: PackageAssetChecksum[];
  };
  application_primitives: {
    fixture_package_name: string;
    artifact_path: "fixtures/plugin-contract-matrix";
    source_artifact_path: string;
    surface_id: "contract.app";
    route_id: "surface:contract.app";
    renderer_entrypoint: "ui_tree_snapshot.body";
    primitive_kinds: string[];
  };
}

export const metadata: HubTestSupportMetadata;

export function daemonProtocolTypescriptPath(): string;
export function readDaemonProtocolTypescript(): string;
export function firstPartyClientSupportMatrixPath(): string;
export function readFirstPartyClientSupportMatrix(): Record<string, unknown>;
export function sessionLifecycleSubscriptionConformanceFixturePath(): string;
export function readSessionLifecycleSubscriptionConformanceFixture(): Record<string, unknown>;
export function lateAttachHistoryConformanceFixturePath(): string;
export function readLateAttachHistoryConformanceFixture(): Record<string, unknown>;
export function localWebrtcDeliveryChunkConformanceFixturePath(): string;
export function readLocalWebrtcDeliveryChunkConformanceFixture(): Record<string, unknown>;
export function modeFlagsConformanceFixturePath(): string;
export function readModeFlagsConformanceFixture(): Record<string, unknown>;
export function pluginContractMatrixFixturePath(): string;
export function materializePluginContractMatrixFixture(destination: string): string;
export function applicationPrimitivesFixturePath(): string;
export function materializeApplicationPrimitivesFixture(destination: string): string;
export function verifyPackageAssets(): { ok: boolean; failures: string[] };
