//! What a session-type spawn needs to be spawned again.
//!
//! `RestartSession` re-spawns an ended session-type session under the same
//! session id from the record kept here. The record holds the caller's
//! inputs, never a resolved definition: a restart re-resolves the CURRENT
//! session type, so an edit to the type applies to the next restart.
//!
//! HubState holds only references to secret material, never values. A client
//! environment may carry secrets, so only its keys are kept; a session spawned
//! with a non-empty client environment cannot be restarted from the record.
//! Variables the Hub injects (`BOTSTER_*`) are never part of the record: a
//! restart injects fresh ones, including a new session token.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::session_types::{SessionTypeContextInput, SessionTypeRequest};
use botster_core::SessionId;

/// The prefix of every variable the Hub injects into a session.
const HUB_INJECTED_PREFIX: &str = "BOTSTER_";

/// The inputs of one successful session-type spawn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestartRecord {
    pub session_type_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// The keys of the client environment. Its values are never kept.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub environment_keys: Vec<String>,
    #[serde(default)]
    pub context: RestartContext,
}

/// The trusted context inputs of the original spawn.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestartContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticket_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

/// Why a record cannot be turned back into a spawn request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestartRefusal {
    /// The original spawn carried client environment values, which were not
    /// kept. The keys name them.
    EnvironmentNotRetained { keys: Vec<String> },
}

impl RestartRecord {
    /// Record the inputs of a session-type spawn. Environment values and
    /// Hub-injected variables are dropped.
    #[must_use]
    pub fn from_request(session_type_id: &str, request: &SessionTypeRequest) -> Self {
        let context = &request.context;
        Self {
            session_type_id: session_type_id.to_string(),
            target_id: request.target_id.clone(),
            cwd: request.cwd.clone(),
            environment_keys: request
                .environment
                .keys()
                .filter(|key| !key.starts_with(HUB_INJECTED_PREFIX))
                .cloned()
                .collect(),
            context: RestartContext {
                worktree_path: context.worktree_path.clone(),
                repo_path: context.repo_path.clone(),
                branch_name: context.branch_name.clone(),
                prompt: context.prompt.clone(),
                ticket_id: context.ticket_id.clone(),
                workspace_id: context.workspace_id.clone(),
                metadata: context.metadata.clone(),
            },
        }
    }

    /// The spawn request that re-creates the session under `session_id`, or
    /// the reason it cannot be re-created from this record.
    pub fn to_request(&self, session_id: SessionId) -> Result<SessionTypeRequest, RestartRefusal> {
        if !self.environment_keys.is_empty() {
            return Err(RestartRefusal::EnvironmentNotRetained {
                keys: self.environment_keys.clone(),
            });
        }
        let context = &self.context;
        Ok(SessionTypeRequest {
            target_id: self.target_id.clone(),
            session_id: Some(session_id),
            cwd: self.cwd.clone(),
            environment: BTreeMap::new(),
            context: SessionTypeContextInput {
                worktree_path: context.worktree_path.clone(),
                repo_path: context.repo_path.clone(),
                branch_name: context.branch_name.clone(),
                prompt: context.prompt.clone(),
                ticket_id: context.ticket_id.clone(),
                workspace_id: context.workspace_id.clone(),
                metadata: context.metadata.clone(),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(environment: &[(&str, &str)]) -> SessionTypeRequest {
        SessionTypeRequest {
            target_id: Some("target-1".to_string()),
            session_id: Some(SessionId("old".into())),
            cwd: Some("sub/dir".to_string()),
            environment: environment
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
            context: SessionTypeContextInput {
                worktree_path: Some("/work/tree".to_string()),
                repo_path: Some("/repo".to_string()),
                branch_name: Some("feature".to_string()),
                prompt: Some("fix the bug".to_string()),
                ticket_id: Some("T-1".to_string()),
                workspace_id: Some("ws".to_string()),
                metadata: BTreeMap::from([("k".to_string(), "v".to_string())]),
            },
        }
    }

    #[test]
    fn a_record_rebuilds_the_spawn_request_under_the_restarted_id() {
        let original = request(&[]);
        let record = RestartRecord::from_request("claude", &original);
        let rebuilt = record.to_request(SessionId("same".into())).unwrap();
        assert_eq!(
            rebuilt,
            SessionTypeRequest {
                session_id: Some(SessionId("same".into())),
                ..original
            }
        );
    }

    #[test]
    fn a_record_keeps_environment_keys_but_never_values_or_hub_variables() {
        let record = RestartRecord::from_request(
            "claude",
            &request(&[
                ("API_TOKEN", "secret-value"),
                ("BOTSTER_SESSION_TOKEN", "hub-token"),
            ]),
        );
        assert_eq!(record.environment_keys, vec!["API_TOKEN".to_string()]);
        let encoded = serde_json::to_string(&record).unwrap();
        assert!(!encoded.contains("secret-value"), "{encoded}");
        assert!(!encoded.contains("BOTSTER_"), "{encoded}");
        assert!(!encoded.contains("hub-token"), "{encoded}");
    }

    #[test]
    fn a_session_spawned_with_client_environment_is_refused_naming_its_keys() {
        let record = RestartRecord::from_request("claude", &request(&[("API_TOKEN", "x")]));
        assert_eq!(
            record.to_request(SessionId("same".into())),
            Err(RestartRefusal::EnvironmentNotRetained {
                keys: vec!["API_TOKEN".to_string()]
            })
        );
    }

    #[test]
    fn hub_injected_variables_alone_do_not_block_a_restart() {
        let record =
            RestartRecord::from_request("claude", &request(&[("BOTSTER_CONTEXT_ID", "ctx")]));
        assert!(record.environment_keys.is_empty());
        assert!(record.to_request(SessionId("same".into())).is_ok());
    }
}
