# Durable Hub State V1

`botster-hub` stores local dogfood hub state in a versioned JSON file at
`<HubConfig.data_directory>/hub-state.json`.

Version 1 is intentionally local-first and simple. It records host identity
metadata, hub config schema metadata, package/provider registry snapshots,
capability grant records, package admission decisions, enabled/disabled/pinned
state, provenance/checksum/update policy fields, local runtime settings, and
append-only audit entries.

The file is written through `FileHubStateStore`: serialize to a sibling
temporary file, flush the file, then rename it over the committed state file.
This proves the v1 consistency boundary tested in this repo: a failed write
before rename leaves the previous committed JSON state loadable and unchanged.
Production boot and `run-one` load or initialize this state file before core
engine operations start. Registry/grant/admission mutation save paths are
defined and tested at the storage boundary; user-facing operator and
package-manager commands that call `HubStateStore::update` are future work.

V1 is a single-writer store. It does not use a lockfile or cross-process write
coordination, so two hub processes pointed at the same data directory can still
produce last-writer-wins updates. That is acceptable for the local dogfood v1
boundary and should be revisited before multi-process or cloud-synced state.

Schema versioning is explicit. The root `schema_version` must be `1`; unknown
future versions return a typed unsupported-version error instead of silently
resetting to defaults. There is no old-version migration yet because this repo
had no prior durable hub state file, but `SchemaMetadata` keeps the migration
hook visible.

Audit history is append-only inside the JSON file, so each mutation rewrites
the whole file and the audit vector is unbounded in v1. Audit reasons are
operator-controlled free text and are not sanitized by the storage boundary;
callers must not store secrets, tokens, personal data, local paths, or other
sensitive content in audit reasons.

The package registry snapshot intentionally persists core package manifests and
hub-owned policy fields. `botster_core::AdmittedHostProfile` is a runtime
admission result and is not serde-stable in the current core revision, so it is
not stored inside `PackageRecord`. Reload paths that reconstruct a live
`PackageRegistry` from a snapshot must re-run core host-profile admission before
trusting an enabled provider. Durable admission decision records are part of the
v1 model, but production producers for those records arrive with the future
operator/package-manager commands that mutate registry state.
