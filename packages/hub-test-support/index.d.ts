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
  plugin_contract_matrix: {
    package_name: string;
    artifact_path: "fixtures/plugin-contract-matrix";
    source_artifact_path: string;
    files: PackageAssetChecksum[];
  };
}

export const metadata: HubTestSupportMetadata;

export function daemonProtocolTypescriptPath(): string;
export function readDaemonProtocolTypescript(): string;
export function pluginContractMatrixFixturePath(): string;
export function materializePluginContractMatrixFixture(destination: string): string;
export function verifyPackageAssets(): { ok: boolean; failures: string[] };
