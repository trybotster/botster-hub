//! Provider request preparation without owner model access.

use super::*;

/// Only validation metadata remains in Hub while Core executes the request.
#[derive(Debug)]
pub(crate) struct ProviderExpectation {
    pub(crate) entity_kind: EntityKind,
    pub(crate) handler: botster_core::PluginHandlerRef,
}

#[derive(Debug)]
pub(crate) struct ProviderRequestPlan {
    pub(crate) request: PluginInvocationRequest,
    pub(crate) expected: SharedView<ProviderExpectation>,
}

impl ProviderRequestPlan {
    pub(crate) fn set_scope(&mut self, scope_id: Option<u64>) {
        self.request.context.metadata = scope_id
            .map(|scope_id| BoundaryJson(serde_json::json!({ "causal_scope_id": scope_id })));
    }

    pub(crate) fn prepare(
        lifecycle: &HubPluginLifecycle,
        budget: &Arc<SharedViewBudget>,
        entity_type: &str,
        subscription_id: &str,
        request_id: RequestId,
        client_id: Option<ClientId>,
    ) -> Result<Self, crate::McpToolError> {
        // Validate before descriptor lookup, preserving the existing error order.
        EntityContract::validate_entity_type(&EntityKind(entity_type.to_string()), None).map_err(
            |error| crate::McpToolError::new("invalid_entity_provider", error.to_string()),
        )?;
        let expected = lifecycle.with_entity_provider_descriptor(entity_type, |descriptor| {
            let descriptor = descriptor.ok_or_else(|| {
                crate::McpToolError::new(
                    "entity_provider_unavailable",
                    format!("no enabled package provides entity family {entity_type}"),
                )
            })?;
            let entity_kind = EntityKind(entity_type.to_string());
            let owner_token = package_entity_owner_token(&descriptor.descriptor.plugin_key.0);
            EntityContract::validate_entity_type(&entity_kind, Some(&owner_token)).map_err(
                |error| crate::McpToolError::new("invalid_entity_provider", error.to_string()),
            )?;
            drop(entity_kind);
            let handler = descriptor.handler.as_ref().ok_or_else(|| {
                crate::McpToolError::new(
                    "entity_provider_unavailable",
                    format!("entity provider {entity_type} has no handler"),
                )
            })?;
            let logical_bytes = [
                entity_type.len(),
                handler.plugin_key.0.len(),
                handler.handler_id.len(),
            ]
            .into_iter()
            .try_fold(
                std::mem::size_of::<ProviderExpectation>(),
                usize::checked_add,
            )
            .ok_or_else(metadata_capacity_error)?;
            let charge = budget
                .reserve(logical_bytes)
                .map_err(|_| metadata_capacity_error())?;
            let expected = SharedView::from_reserved(
                ProviderExpectation {
                    entity_kind: EntityKind(entity_type.to_string()),
                    handler: handler.clone(),
                },
                charge,
            );
            Ok(expected)
        })?;
        // The request remains under Host preparation capacity until Core admission returns.
        let minimum_request_bytes = [
            request_id.0.len(),
            expected.handler.plugin_key.0.len(),
            expected.handler.handler_id.len(),
            entity_type.len(),
            subscription_id.len(),
            subscription_id.len(),
            client_id.as_ref().map_or(0, |client| client.0.len()),
        ]
        .into_iter()
        .try_fold(0usize, usize::checked_add)
        .ok_or_else(request_capacity_error)?;
        if minimum_request_bytes > crate::host_executor::HOST_PREPARED_BYTE_CAPACITY {
            return Err(request_capacity_error());
        }
        let request = PluginInvocationRequest {
            request_id,
            handler: expected.handler.clone(),
            timeout_ms: PLUGIN_EVENT_TIMEOUT_MS,
            context: botster_core::PluginInvocationContext {
                client_id,
                session_id: None,
                subscription_id: Some(SubscriptionId(subscription_id.to_string())),
                surface_id: None,
                origin: Some("local-client-api".to_string()),
                metadata: Some(BoundaryJson(
                    serde_json::json!({"causal_scope_id": u64::MAX}),
                )),
            },
            payload: BoundaryJson(serde_json::json!({
                "entity_type": entity_type,
                "subscription_id": subscription_id,
            })),
        };
        crate::bounded_json::encoded_len(
            &request,
            crate::host_executor::HOST_PREPARED_BYTE_CAPACITY,
        )
        .map_err(|_| request_capacity_error())?;
        let mut plan = Self { request, expected };
        plan.set_scope(None);
        Ok(plan)
    }
}

fn metadata_capacity_error() -> crate::McpToolError {
    crate::McpToolError::new(
        "entity_provider_metadata_capacity",
        "provider metadata exceeds the shared view capacity",
    )
}

fn request_capacity_error() -> crate::McpToolError {
    crate::McpToolError::new(
        "entity_provider_request_capacity",
        "provider request exceeds Host preparation capacity",
    )
}
