use std::path::PathBuf;

use botster_hub_client::{
    DaemonAvailablePackage, DaemonCapability, DaemonPackage, DaemonPackageActionRequiredReference,
    DaemonPackageActionState, DaemonPackageAvailability, DaemonPackageAvailabilityReason,
    DaemonPackageAvailabilityState, DaemonPackageCompatibility, DaemonPackageConfiguration,
    DaemonPackageDecision, DaemonPackageDependencyAvailability, DaemonPackageDiagnostic,
    DaemonPackageEnvironmentRequirement, DaemonPackageFeatureAvailability, DaemonPackagePin,
    DaemonPackageProcess, DaemonPackageRunnableEntrypoint, DaemonPackageWorkingDirectory,
};

use crate::daemon::error::DaemonTransportResult;
use crate::daemon_projection::{
    available_package_action, available_package_actions, blocked_action, package_action_label,
    package_route_descriptors, package_state_label, request_for_entrypoint, request_for_package,
    request_for_package_with_pin, unavailable_action,
};
use crate::{
    AvailablePackage, AvailablePackageState, HubClientPackage, HubClientPackageAvailabilityReason,
    HubClientPackageAvailabilityState, HubClientPackageClassification, PackageAction,
    PackageAdmissionReason, PackageCompatibilityResult, PackageDecision, PackagePin,
    PackageRegistryEntrySourceKind, PackageRegistryError, PackageUpdatePolicy,
};

pub(crate) fn daemon_package_from_client(package: HubClientPackage) -> DaemonPackage {
    let package_name = package.package_name.clone();
    let package_state = package_state_label(package.state).to_string();
    let package_actions = installed_package_actions(&package);
    let routes = package_route_descriptors(&package);
    DaemonPackage {
        package_name: package.package_name,
        version: package.version,
        classification: package_classification_label(package.classification).to_string(),
        source_kind: package.source_kind,
        state: package_state_label(package.state).to_string(),
        requested_capabilities: package
            .requested_capabilities
            .into_iter()
            .map(|capability| DaemonCapability {
                surface: capability.surface,
                scope: capability.scope,
            })
            .collect(),
        surfaces: package.surfaces,
        notice_reactions: package.notice_reactions,
        routes,
        runnable_entrypoints: package
            .runnable_entrypoints
            .into_iter()
            .map(|entrypoint| {
                let actions = entrypoint_actions(&package_name, &package_state, &entrypoint);
                DaemonPackageRunnableEntrypoint {
                    id: entrypoint.id,
                    kind: entrypoint.kind,
                    launch_mode: entrypoint.launch_mode,
                    command: entrypoint.command,
                    args: entrypoint.args,
                    working_directory: DaemonPackageWorkingDirectory {
                        policy: entrypoint.working_directory.policy,
                        path: entrypoint.working_directory.path,
                    },
                    environment: entrypoint
                        .environment
                        .into_iter()
                        .map(|requirement| DaemonPackageEnvironmentRequirement {
                            name: requirement.name,
                            required: requirement.required,
                            default: requirement.default,
                            description: requirement.description,
                        })
                        .collect(),
                    capabilities: entrypoint
                        .capabilities
                        .into_iter()
                        .map(|capability| DaemonCapability {
                            surface: capability.surface,
                            scope: capability.scope,
                        })
                        .collect(),
                    may_supervise: entrypoint.may_supervise,
                    process: DaemonPackageProcess {
                        state: entrypoint.process.state,
                        pid: entrypoint.process.pid,
                        started_at: entrypoint.process.started_at,
                        exited_at: entrypoint.process.exited_at,
                        exit_status: entrypoint.process.exit_status,
                        diagnostics: entrypoint
                            .process
                            .diagnostics
                            .into_iter()
                            .map(|diagnostic| DaemonPackageDiagnostic {
                                kind: diagnostic.kind,
                                message: diagnostic.message,
                            })
                            .collect(),
                    },
                    actions,
                }
            })
            .collect(),
        configuration: DaemonPackageConfiguration {
            schema: package.configuration.schema,
            effective_values: package.configuration.effective_values,
            missing_required: package.configuration.missing_required,
            diagnostics: package
                .configuration
                .diagnostics
                .into_iter()
                .map(|diagnostic| DaemonPackageDiagnostic {
                    kind: diagnostic.kind,
                    message: diagnostic.message,
                })
                .collect(),
        },
        availability: DaemonPackageAvailability {
            state: daemon_availability_state(package.availability.state),
            reasons: package
                .availability
                .reasons
                .into_iter()
                .map(daemon_availability_reason)
                .collect(),
        },
        dependency_availability: package
            .dependency_availability
            .into_iter()
            .map(|dependency| DaemonPackageDependencyAvailability {
                id: dependency.id,
                package_name: dependency.package_name,
                state: daemon_availability_state(dependency.state),
                reasons: dependency
                    .reasons
                    .into_iter()
                    .map(daemon_availability_reason)
                    .collect(),
            })
            .collect(),
        feature_availability: package
            .feature_availability
            .into_iter()
            .map(|feature| DaemonPackageFeatureAvailability {
                id: feature.id,
                state: daemon_availability_state(feature.state),
                reasons: feature
                    .reasons
                    .into_iter()
                    .map(daemon_availability_reason)
                    .collect(),
            })
            .collect(),
        actions: package_actions,
        provider_profile_admitted: package.provider_profile_admitted,
    }
}

pub(crate) fn daemon_available_package_from_policy(
    package: AvailablePackage,
    registry_path: Option<&PathBuf>,
) -> DaemonAvailablePackage {
    let actions = available_package_actions(&package, registry_path);
    DaemonAvailablePackage {
        entry_id: package.entry_id,
        package_name: package.package_name,
        version: package.version,
        classification: package_classification_label(package.classification.into()).to_string(),
        source_kind: registry_source_kind_label(package.source_kind).to_string(),
        source_label: package.source_label,
        first_party: package.first_party,
        state: available_package_state_label(package.state).to_string(),
        requested_capabilities: package
            .requested_capabilities
            .into_iter()
            .map(|capability| DaemonCapability {
                surface: format!("{:?}", capability.surface),
                scope: capability.scope,
            })
            .collect(),
        compatibility: DaemonPackageCompatibility {
            botster_requirement: package.compatibility.botster_requirement,
            result: package_compatibility_label(package.compatibility.result).to_string(),
            diagnostics: package.compatibility.diagnostics,
        },
        pin: package.pin.map(daemon_package_pin_from_policy),
        actions,
    }
}

pub(crate) fn installed_package_actions(
    package: &HubClientPackage,
) -> Vec<DaemonPackageActionState> {
    let package_name = package.package_name.as_str();
    let availability_blocked = matches!(
        package.availability.state,
        HubClientPackageAvailabilityState::Blocked
    );
    let required_references = package_required_references(package);
    let blocked_diagnostics =
        package
            .availability
            .reasons
            .iter()
            .map(|reason| DaemonPackageDiagnostic {
                kind: reason.reason.clone(),
                message: format!("{} is blocked for {}", package.package_name, reason.action),
            })
            .chain(package.configuration.diagnostics.iter().map(|diagnostic| {
                DaemonPackageDiagnostic {
                    kind: diagnostic.kind.clone(),
                    message: diagnostic.message.clone(),
                }
            }))
            .collect::<Vec<_>>();
    let state = package_state_label(package.state);
    let mut actions = Vec::new();

    actions.push(unavailable_action(
        "install_package_registry_entry",
        "already_installed",
        "package is already installed; use update actions for source metadata changes",
    ));

    match state {
        "enabled" => actions.push(unavailable_action(
            "enable_package",
            "already_enabled",
            "package is already enabled",
        )),
        _ if availability_blocked => actions.push(blocked_action(
            "enable_package",
            "package_requirements_blocked",
            blocked_diagnostics.clone(),
            required_references.clone(),
        )),
        _ => actions.push(available_package_action(
            "enable_package",
            request_for_package("enable_package", package_name),
        )),
    }

    if state == "enabled" {
        actions.push(available_package_action(
            "disable_package",
            request_for_package("disable_package", package_name),
        ));
    } else {
        actions.push(unavailable_action(
            "disable_package",
            "not_enabled",
            "package is not enabled",
        ));
    }

    actions.push(available_package_action(
        "remove_package",
        request_for_package("remove_package", package_name),
    ));

    if package.configuration.schema.is_some()
        || !package.configuration.missing_required.is_empty()
        || !package.configuration.diagnostics.is_empty()
    {
        actions.push(available_package_action(
            "set_package_configuration",
            request_for_package("set_package_configuration", package_name),
        ));
    } else {
        actions.push(unavailable_action(
            "set_package_configuration",
            "no_configuration_schema",
            "package does not declare configurable fields",
        ));
    }

    actions.push(available_package_action(
        "check_package_update",
        request_for_package("check_package_update", package_name),
    ));
    actions.push(blocked_action(
        "preview_package_update",
        "pin_required",
        vec![DaemonPackageDiagnostic {
            kind: "pin_required".to_string(),
            message: "preview update requires explicit pinned source metadata".to_string(),
        }],
        vec![DaemonPackageActionRequiredReference {
            kind: "pin".to_string(),
            key: "package_update_pin".to_string(),
        }],
    ));
    actions.push(blocked_action(
        "apply_package_update",
        "pin_required",
        vec![DaemonPackageDiagnostic {
            kind: "pin_required".to_string(),
            message: "apply update requires explicit pinned source metadata".to_string(),
        }],
        vec![DaemonPackageActionRequiredReference {
            kind: "pin".to_string(),
            key: "package_update_pin".to_string(),
        }],
    ));
    if package.source_kind == "path" {
        actions.push(available_package_action(
            "reload_package",
            request_for_package("reload_package", package_name),
        ));
    } else {
        actions.push(unavailable_action(
            "reload_package",
            "local_path_required",
            "package reload is only available for local path packages",
        ));
    }
    actions.push(unavailable_action(
        "restart_hub",
        "unsupported",
        "hub restart is not exposed as a package lifecycle action",
    ));

    actions
}

pub(crate) fn entrypoint_actions(
    package_name: &str,
    package_state: &str,
    entrypoint: &crate::HubClientPackageRunnableEntrypoint,
) -> Vec<DaemonPackageActionState> {
    if !entrypoint.may_supervise {
        return vec![
            unavailable_action(
                "start_package_entrypoint",
                "entrypoint_not_supervisable",
                "entrypoint is not marked supervisable",
            ),
            unavailable_action(
                "stop_package_entrypoint",
                "entrypoint_not_supervisable",
                "entrypoint is not marked supervisable",
            ),
            unavailable_action(
                "restart_package_entrypoint",
                "entrypoint_not_supervisable",
                "entrypoint is not marked supervisable",
            ),
        ];
    }

    if package_state != "enabled" {
        return vec![
            blocked_action(
                "start_package_entrypoint",
                "package_not_enabled",
                vec![DaemonPackageDiagnostic {
                    kind: "package_not_enabled".to_string(),
                    message: "enable the package before starting entrypoints".to_string(),
                }],
                Vec::new(),
            ),
            blocked_action(
                "stop_package_entrypoint",
                "package_not_enabled",
                Vec::new(),
                Vec::new(),
            ),
            blocked_action(
                "restart_package_entrypoint",
                "package_not_enabled",
                Vec::new(),
                Vec::new(),
            ),
        ];
    }

    let running = entrypoint.process.state == "running";
    let mut actions = Vec::new();
    if running {
        actions.push(unavailable_action(
            "start_package_entrypoint",
            "already_running",
            "entrypoint is already running",
        ));
        actions.push(available_package_action(
            "stop_package_entrypoint",
            request_for_entrypoint("stop_package_entrypoint", package_name, &entrypoint.id),
        ));
    } else {
        actions.push(available_package_action(
            "start_package_entrypoint",
            request_for_entrypoint("start_package_entrypoint", package_name, &entrypoint.id),
        ));
        actions.push(unavailable_action(
            "stop_package_entrypoint",
            "not_running",
            "entrypoint is not running",
        ));
    }
    actions.push(available_package_action(
        "restart_package_entrypoint",
        request_for_entrypoint("restart_package_entrypoint", package_name, &entrypoint.id),
    ));
    actions
}

pub(crate) fn update_status_actions(
    package_name: &str,
    pin: Option<&DaemonPackagePin>,
    has_pin: bool,
    source_metadata_present: bool,
    local_path_source: bool,
) -> Vec<DaemonPackageActionState> {
    let mut actions = vec![available_package_action(
        "check_package_update",
        request_for_package("check_package_update", package_name),
    )];
    if has_pin && source_metadata_present {
        actions.push(available_package_action(
            "preview_package_update",
            request_for_package_with_pin("preview_package_update", package_name, pin.cloned()),
        ));
        actions.push(available_package_action(
            "apply_package_update",
            request_for_package_with_pin("apply_package_update", package_name, pin.cloned()),
        ));
    } else {
        let reason = if source_metadata_present {
            "pin_required"
        } else {
            "source_metadata_required"
        };
        let references = if has_pin {
            Vec::new()
        } else {
            vec![DaemonPackageActionRequiredReference {
                kind: "pin".to_string(),
                key: "package_update_pin".to_string(),
            }]
        };
        actions.push(blocked_action(
            "preview_package_update",
            reason,
            Vec::new(),
            references.clone(),
        ));
        actions.push(blocked_action(
            "apply_package_update",
            reason,
            Vec::new(),
            references,
        ));
    }
    if local_path_source {
        actions.push(available_package_action(
            "reload_package",
            request_for_package("reload_package", package_name),
        ));
    } else {
        actions.push(unavailable_action(
            "reload_package",
            "local_path_required",
            "package reload is only available for local path packages",
        ));
    }
    actions.push(unavailable_action(
        "restart_hub",
        "unsupported",
        "hub restart is not exposed as a package lifecycle action",
    ));
    actions
}

pub(crate) fn package_required_references(
    package: &HubClientPackage,
) -> Vec<DaemonPackageActionRequiredReference> {
    let mut references = package
        .configuration
        .missing_required
        .iter()
        .map(|key| DaemonPackageActionRequiredReference {
            kind: "config".to_string(),
            key: key.clone(),
        })
        .collect::<Vec<_>>();
    for dependency in &package.dependency_availability {
        if matches!(dependency.state, HubClientPackageAvailabilityState::Blocked) {
            references.push(DaemonPackageActionRequiredReference {
                kind: "dependency".to_string(),
                key: dependency.package_name.clone(),
            });
        }
    }
    references
}

pub(crate) fn daemon_package_pin_from_policy(pin: PackagePin) -> DaemonPackagePin {
    DaemonPackagePin {
        revision: pin.revision,
        branch: pin.branch,
        tag: pin.tag,
        rev: pin.rev,
        checksum: pin.checksum,
        update_policy: package_update_policy_label(pin.update_policy).to_string(),
    }
}

pub(crate) fn daemon_availability_state(
    state: HubClientPackageAvailabilityState,
) -> DaemonPackageAvailabilityState {
    match state {
        HubClientPackageAvailabilityState::Available => DaemonPackageAvailabilityState::Available,
        HubClientPackageAvailabilityState::Blocked => DaemonPackageAvailabilityState::Blocked,
    }
}

pub(crate) fn daemon_availability_reason(
    reason: HubClientPackageAvailabilityReason,
) -> DaemonPackageAvailabilityReason {
    DaemonPackageAvailabilityReason {
        reason: reason.reason,
        action: reason.action,
        package_name: reason.package_name,
        capability: reason.capability.map(|capability| DaemonCapability {
            surface: capability.surface,
            scope: capability.scope,
        }),
        requirement: reason.requirement,
    }
}

pub(crate) fn package_pin_from_daemon(pin: DaemonPackagePin) -> DaemonTransportResult<PackagePin> {
    let update_policy = match pin.update_policy.as_str() {
        "manual" => PackageUpdatePolicy::Manual,
        "track_source" => PackageUpdatePolicy::TrackSource,
        _ => {
            return Err(PackageRegistryError::without_record(
                "<package-update>",
                PackageAction::ApplyUpdate,
                PackageAdmissionReason::MissingPinRevision,
                "daemon socket apply package update".to_string(),
            )
            .into());
        }
    };
    Ok(PackagePin {
        revision: pin.revision,
        branch: pin.branch,
        tag: pin.tag,
        rev: pin.rev,
        checksum: pin.checksum,
        update_policy,
    })
}

pub(crate) fn daemon_package_decision_from_policy(
    decision: PackageDecision,
) -> DaemonPackageDecision {
    DaemonPackageDecision {
        package_name: decision.package_name,
        action: package_action_label(decision.action).to_string(),
        state: package_state_label(decision.state.into()).to_string(),
        classification: package_classification_label(decision.classification.into()).to_string(),
    }
}

pub(crate) fn package_classification_label(
    classification: HubClientPackageClassification,
) -> &'static str {
    match classification {
        HubClientPackageClassification::Plugin => "plugin",
        HubClientPackageClassification::Provider => "provider",
    }
}

pub(crate) fn available_package_state_label(state: AvailablePackageState) -> &'static str {
    match state {
        AvailablePackageState::Available => "available",
        AvailablePackageState::Installed => "installed",
        AvailablePackageState::Enabled => "enabled",
        AvailablePackageState::Disabled => "disabled",
    }
}

pub(crate) fn registry_source_kind_label(kind: PackageRegistryEntrySourceKind) -> &'static str {
    match kind {
        PackageRegistryEntrySourceKind::LocalPath => "local_path",
        PackageRegistryEntrySourceKind::Git => "git",
    }
}

pub(crate) fn package_compatibility_label(result: PackageCompatibilityResult) -> &'static str {
    match result {
        PackageCompatibilityResult::Compatible => "compatible",
        PackageCompatibilityResult::Incompatible => "incompatible",
        PackageCompatibilityResult::InvalidRequirement => "invalid_requirement",
    }
}

pub(crate) fn package_update_policy_label(policy: PackageUpdatePolicy) -> &'static str {
    match policy {
        PackageUpdatePolicy::Manual => "manual",
        PackageUpdatePolicy::TrackSource => "track_source",
    }
}
