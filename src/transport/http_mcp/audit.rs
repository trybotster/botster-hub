//! The tool-call audit log: one JSON line per MCP tool call.
//!
//! Each line names only these fields: the time, the verified caller
//! (`{hub_id, session_id}`, or `null` when the token was refused), the tool
//! name, the target session when the arguments name one, and the outcome
//! (`ok` or the typed error code). It never holds an argument body, a message
//! body, or a token. The file is `<data_dir>/audit/tool-calls.jsonl`, appended
//! from the HTTP task, never from the owner loop. It has no rotation: a size
//! bound needs an observed need first.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::session_credential::CallerToken;
use serde_json::{Value, json};

/// Directory and file of the log inside the data directory.
pub(crate) const AUDIT_DIRECTORY: &str = "audit";
pub(crate) const TOOL_CALLS_FILE: &str = "tool-calls.jsonl";

/// What one tool call leaves in the log.
pub(crate) struct ToolCallRecord {
    pub(crate) tool: String,
    /// The target session, when the arguments name one in `session_id`.
    pub(crate) target_session_id: Option<String>,
    /// `ok`, or the typed error code.
    pub(crate) outcome: String,
    /// False when the bearer token was refused: the claimed session is then
    /// not recorded as a caller.
    pub(crate) caller_verified: bool,
}

pub(crate) struct ToolAudit {
    path: PathBuf,
    /// The local hub: the only hub whose sessions this daemon serves.
    hub_id: String,
}

impl ToolAudit {
    pub(crate) fn new(data_directory: &Path, hub_id: String) -> Self {
        Self {
            path: log_path(data_directory),
            hub_id,
        }
    }

    /// The local hub: the only hub whose sessions this daemon serves.
    pub(crate) fn hub_id(&self) -> &str {
        &self.hub_id
    }

    /// Append one line, on the blocking pool so no worker waits on the disk. A
    /// failure is reported by name and never fails the call.
    pub(crate) async fn append(&self, token: &CallerToken, record: &ToolCallRecord) {
        let mut line = line(&self.hub_id, token, record, now_ms()).to_string();
        line.push('\n');
        let path = self.path.clone();
        let written = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            use std::io::Write;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?;
            file.write_all(line.as_bytes())
        })
        .await;
        match written {
            Ok(Ok(())) => {}
            Ok(Err(error)) => eprintln!("botster-hub tool audit write error: {error}"),
            Err(error) => eprintln!("botster-hub tool audit task error: {error}"),
        }
    }
}

/// Where a data directory's tool-call audit log lives.
pub fn log_path(data_directory: &Path) -> PathBuf {
    data_directory.join(AUDIT_DIRECTORY).join(TOOL_CALLS_FILE)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// The audit line for one call. Only the listed fields exist.
fn line(hub_id: &str, token: &CallerToken, record: &ToolCallRecord, ts_ms: u64) -> Value {
    json!({
        "ts_ms": ts_ms,
        "caller": record.caller_verified.then(|| json!({
            "hub_id": hub_id,
            "session_id": token.session_id(),
        })),
        "tool": record.tool,
        "target": record.target_session_id.as_ref().map(|session_id| json!({
            "hub_id": hub_id,
            "session_id": session_id,
        })),
        "outcome": record.outcome,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token() -> CallerToken {
        CallerToken::parse(&format!("sess-a.{}", "5a".repeat(32))).expect("a well-formed token")
    }

    #[test]
    fn a_line_names_the_caller_and_target_with_their_hub() {
        let record = ToolCallRecord {
            tool: "post_message".to_string(),
            target_session_id: Some("sess-b".to_string()),
            outcome: "ok".to_string(),
            caller_verified: true,
        };
        let line = line("hub-1", &token(), &record, 7);
        assert_eq!(line["ts_ms"], 7);
        assert_eq!(line["caller"]["hub_id"], "hub-1");
        assert_eq!(line["caller"]["session_id"], "sess-a");
        assert_eq!(line["target"]["hub_id"], "hub-1");
        assert_eq!(line["target"]["session_id"], "sess-b");
        assert_eq!(line["tool"], "post_message");
        assert_eq!(line["outcome"], "ok");
    }

    #[test]
    fn a_refused_token_records_no_caller_and_the_error_code() {
        let record = ToolCallRecord {
            tool: "whoami".to_string(),
            target_session_id: None,
            outcome: "caller_unauthenticated".to_string(),
            caller_verified: false,
        };
        let line = line("hub-1", &token(), &record, 1);
        assert!(
            line["caller"].is_null(),
            "an unproven claim is not a caller"
        );
        assert!(line["target"].is_null());
        assert_eq!(line["outcome"], "caller_unauthenticated");
    }

    #[test]
    fn a_line_holds_only_its_listed_fields_and_no_token() {
        let record = ToolCallRecord {
            tool: "post_message".to_string(),
            target_session_id: Some("sess-b".to_string()),
            outcome: "ok".to_string(),
            caller_verified: true,
        };
        let line = line("hub-1", &token(), &record, 1);
        let mut fields: Vec<_> = line.as_object().unwrap().keys().cloned().collect();
        fields.sort();
        assert_eq!(fields, ["caller", "outcome", "target", "tool", "ts_ms"]);
        assert!(!line.to_string().contains("5a5a"), "no secret in the line");
    }

    #[tokio::test]
    async fn append_writes_one_json_line_per_call() {
        let directory = std::env::temp_dir().join(format!("tool-audit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        let audit = ToolAudit::new(&directory, "hub-1".to_string());
        for outcome in ["ok", "unknown_session"] {
            audit
                .append(
                    &token(),
                    &ToolCallRecord {
                        tool: "post_message".to_string(),
                        target_session_id: Some("sess-b".to_string()),
                        outcome: outcome.to_string(),
                        caller_verified: true,
                    },
                )
                .await;
        }
        let text = std::fs::read_to_string(log_path(&directory)).unwrap();
        let outcomes: Vec<String> = text
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap()["outcome"].to_string())
            .collect();
        assert_eq!(outcomes, ["\"ok\"", "\"unknown_session\""]);
        let _ = std::fs::remove_dir_all(&directory);
    }
}
