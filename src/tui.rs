//! Minimal local TUI over the daemon socket client API.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use botster_core::{
    UiActionResult, UiActionResultState, UiChild, UiNode, UiNodeId, UiNodeKind,
};
use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyModifiers,
    MouseButton, MouseEvent, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};
use serde_json::{Map, Value};

use crate::{
    DaemonConnection, DaemonEvent, DaemonRequest, DaemonResponseKind, DaemonSession,
    DaemonTransportError, DaemonTransportResult, HubConfig,
};

const DRAIN_INTERVAL: Duration = Duration::from_millis(50);
const RECONNECT_INTERVAL: Duration = Duration::from_millis(250);
const DOGFOOD_PLUGIN: &str = "project-pipelines";
const DOGFOOD_SURFACE: &str = "project-pipelines.create-ticket";
const DOGFOOD_ACTION: &str = "project_pipelines.create_ticket";

/// Run the interactive terminal UI until the operator quits.
pub fn run(config: HubConfig) -> TuiResult<()> {
    let _guard = TerminalModeGuard::enter()?;
    let stdout = io::stdout();
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut app = TuiClient::new(config);
    app.reconnect()?;

    loop {
        terminal.draw(|frame| draw(frame, &mut app))?;
        if event::poll(DRAIN_INTERVAL)? && route_event(&mut app, event::read()?)? {
            break;
        }
        app.drain_or_reconnect()?;
    }

    app.detach();
    Ok(())
}

/// Non-rendering harness used by integration tests to prove the TUI client path.
pub fn run_scripted_probe(config: HubConfig, session_id: &str) -> TuiResult<ScriptedTuiProof> {
    let mut driver = ScriptedTuiDriver::connect(config)?;
    driver.select_session(session_id)?;
    let first_subscription_id = driver.attach_selected()?;
    driver.send_input("from-tui\n");
    driver.drain_until("echo:from-tui", Duration::from_secs(5))?;
    driver.resize(31, 101);
    let resize_sent = driver.resize_sent();
    driver.send_input("size-check\n");
    driver.drain_until("winsize:31 101", Duration::from_secs(5))?;
    let guarded = driver.guarded_notification("doorbell-from-tui\n")?;
    driver.detach();
    let second_subscription_id = driver.attach_selected()?;
    driver.send_input("after-reattach\n");
    driver.drain_until("echo:after-reattach", Duration::from_secs(5))?;

    Ok(ScriptedTuiProof {
        first_subscription_id,
        second_subscription_id,
        rendered_sessions: driver
            .client
            .sessions
            .iter()
            .map(|session| session.session_id.clone())
            .collect(),
        observed_output: driver.client.output.join(""),
        notification_rows: driver.client.notifications.clone(),
        guarded_decision: guarded.decision,
        guarded_states: guarded.states,
        resize_sent,
        ui_regions: driver.client.rendered_ui_regions(),
    })
}

/// Scripted driver for integration tests that must control daemon restarts.
pub struct ScriptedTuiDriver {
    client: TuiClient,
}

impl ScriptedTuiDriver {
    pub fn connect(config: HubConfig) -> TuiResult<Self> {
        let mut client = TuiClient::new(config);
        client.reconnect()?;
        Ok(Self { client })
    }

    pub fn reconnect(&mut self) -> TuiResult<()> {
        self.client.reconnect()
    }

    pub fn select_session(&mut self, session_id: &str) -> TuiResult<()> {
        self.client.select_session(session_id)
    }

    pub fn attach_selected(&mut self) -> TuiResult<String> {
        self.client.attach_selected()
    }

    pub fn detach(&mut self) {
        self.client.detach();
    }

    pub fn send_input(&mut self, data: &str) {
        self.client.send_input(data);
    }

    pub fn resize(&mut self, rows: u16, cols: u16) {
        self.client.resize(rows, cols);
    }

    pub fn resize_sent(&self) -> Option<(u16, u16)> {
        self.client.resize_sent
    }

    pub fn guarded_notification(&mut self, data: &str) -> TuiResult<crate::DaemonNotify> {
        self.client.guarded_notification(data)
    }

    pub fn drain_until(&mut self, needle: &str, timeout: Duration) -> TuiResult<()> {
        self.client.drain_until(needle, timeout)
    }

    pub fn drain_once(&mut self) -> TuiResult<()> {
        self.client.drain_or_reconnect()
    }

    pub fn output(&self) -> String {
        self.client.output.join("")
    }

    pub fn active_session_id(&self) -> Option<String> {
        self.client.active_session_id.clone()
    }

    pub fn subscription_id(&self) -> Option<String> {
        self.client.subscription_id.clone()
    }

    pub fn session_ids(&self) -> Vec<String> {
        self.client
            .sessions
            .iter()
            .map(|session| session.session_id.clone())
            .collect()
    }

    pub fn errors(&self) -> Vec<String> {
        self.client.errors.clone()
    }

    pub fn set_project_pipelines_form(&mut self, title: &str, pipeline_id: &str) {
        self.client.form_values.insert(
            "project-pipelines-create-title".to_string(),
            title.to_string(),
        );
        self.client.form_values.insert(
            "project-pipelines-create-pipeline".to_string(),
            pipeline_id.to_string(),
        );
    }

    pub fn submit_project_pipelines_form(&mut self) -> Vec<UiActionResult> {
        self.client.submit_plugin_action(DOGFOOD_ACTION);
        self.client.action_results.clone()
    }
}

/// Test proof emitted by the scripted TUI client path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptedTuiProof {
    pub first_subscription_id: String,
    pub second_subscription_id: String,
    pub rendered_sessions: Vec<String>,
    pub observed_output: String,
    pub notification_rows: Vec<String>,
    pub guarded_decision: String,
    pub guarded_states: Vec<String>,
    pub resize_sent: Option<(u16, u16)>,
    pub ui_regions: Vec<String>,
}

struct TuiClient {
    config: HubConfig,
    connection: Option<DaemonConnection>,
    sessions: Vec<DaemonSession>,
    selected: usize,
    active_session_id: Option<String>,
    subscription_id: Option<String>,
    subscription_generation: u64,
    output: Vec<String>,
    sent_inputs: Vec<String>,
    notifications: Vec<String>,
    errors: Vec<String>,
    status: String,
    reconnecting: bool,
    resize_sent: Option<(u16, u16)>,
    ui_regions: Vec<TuiHitRegion>,
    focused_node_id: Option<UiNodeId>,
    plugin_surface: Option<UiNode>,
    form_values: BTreeMap<String, String>,
    action_results: Vec<UiActionResult>,
}

impl TuiClient {
    fn new(config: HubConfig) -> Self {
        Self {
            config,
            connection: None,
            sessions: Vec::new(),
            selected: 0,
            active_session_id: None,
            subscription_id: None,
            subscription_generation: 0,
            output: Vec::new(),
            sent_inputs: Vec::new(),
            notifications: Vec::new(),
            errors: Vec::new(),
            status: "starting".to_string(),
            reconnecting: false,
            resize_sent: None,
            ui_regions: Vec::new(),
            focused_node_id: None,
            plugin_surface: None,
            form_values: BTreeMap::new(),
            action_results: Vec::new(),
        }
    }

    fn reconnect(&mut self) -> TuiResult<()> {
        self.connection = Some(DaemonConnection::connect(&self.config)?);
        self.reconnecting = false;
        self.refresh()?;
        if let Some(active_session_id) = self.active_session_id.clone() {
            if self.sessions.iter().any(|session| {
                session.session_id == active_session_id && session.lifecycle == "running"
            }) {
                self.attach_session(active_session_id)?;
            } else {
                self.subscription_id = None;
                self.errors
                    .push("attached session was not recovered after daemon reconnect".to_string());
            }
        }
        self.load_dogfood_surface();
        Ok(())
    }

    fn refresh(&mut self) -> TuiResult<()> {
        let status = self.request(DaemonRequest::Status)?;
        if let Some(status) = status.status {
            self.status = format!(
                "{} sessions={} recovered={} stale={}",
                status.lifecycle_state,
                status.session_count,
                status.recovered_sessions.len(),
                status.stale_sessions.len()
            );
        }

        let list = self.request(DaemonRequest::ListSessions)?;
        self.sessions = list.sessions;
        if self.selected >= self.sessions.len() {
            self.selected = self.sessions.len().saturating_sub(1);
        }
        self.load_dogfood_surface();
        Ok(())
    }

    fn load_dogfood_surface(&mut self) {
        let response =
            self.request_without_operator_error_row(DaemonRequest::PluginSurfaceRender {
                package_name: DOGFOOD_PLUGIN.to_string(),
                surface_id: DOGFOOD_SURFACE.to_string(),
                payload: serde_json::json!({}),
            });
        match response {
            Ok(response) if response.kind == DaemonResponseKind::PluginSurface => {
                self.plugin_surface = response
                    .plugin_surface
                    .and_then(|surface| serde_json::from_value(surface).ok());
                if let Some(surface) = self.plugin_surface.clone() {
                    self.seed_form_values(&surface);
                }
            }
            Ok(response) if response.kind == DaemonResponseKind::OperatorError => {
                if let Some(error) = response.error.as_ref()
                    && error.code != "unknown_surface"
                {
                    self.errors
                        .push(format!("{}: {}", error.code, error.message));
                }
            }
            Ok(_) => self
                .errors
                .push("plugin surface returned unexpected response".to_string()),
            Err(DaemonTransportError::NotRunning) => {}
            Err(error) => self.record_transport_error(error),
        }
    }

    fn seed_form_values(&mut self, node: &UiNode) {
        if let Some(node_id) = node.id.as_ref()
            && matches!(node.kind, UiNodeKind::TextInput | UiNodeKind::Textarea)
            && !self.form_values.contains_key(&node_id.0)
        {
            let value = string_prop(node, "value").unwrap_or_default().to_string();
            self.form_values.insert(node_id.0.clone(), value);
        }
        for child in node_children(node) {
            self.seed_form_values(child);
        }
    }

    fn select_session(&mut self, session_id: &str) -> TuiResult<()> {
        self.refresh()?;
        self.select_rendered_session(session_id)
    }

    fn select_rendered_session(&mut self, session_id: &str) -> TuiResult<()> {
        let Some(index) = self
            .sessions
            .iter()
            .position(|session| session.session_id == session_id)
        else {
            return Err(TuiError::SessionMissing(session_id.to_string()));
        };
        self.selected = index;
        Ok(())
    }

    fn selected_session_id(&self) -> Option<String> {
        self.sessions
            .get(self.selected)
            .map(|session| session.session_id.clone())
    }

    fn attach_selected(&mut self) -> TuiResult<String> {
        let Some(session_id) = self.selected_session_id() else {
            return Err(TuiError::NoSessions);
        };
        self.attach_session(session_id)
    }

    fn attach_session(&mut self, session_id: String) -> TuiResult<String> {
        self.detach();
        self.subscription_generation += 1;
        let subscription_id = format!(
            "tui:{}:{}",
            process_unique_seed(),
            self.subscription_generation
        );
        let response = self.request(DaemonRequest::Attach {
            session_id: session_id.clone(),
            subscription_id: subscription_id.clone(),
        })?;
        self.apply_events(response.events);
        self.active_session_id = Some(session_id);
        self.subscription_id = Some(subscription_id.clone());
        Ok(subscription_id)
    }

    fn detach(&mut self) {
        let (Some(session_id), Some(subscription_id)) =
            (self.active_session_id.clone(), self.subscription_id.clone())
        else {
            return;
        };
        if let Err(error) = self.request(DaemonRequest::Detach {
            session_id,
            subscription_id,
        }) {
            self.errors.push(format!("detach failed: {error}"));
        }
        self.subscription_id = None;
        self.active_session_id = None;
    }

    fn send_input(&mut self, data: &str) {
        let Some(session_id) = self.active_session_id.clone() else {
            return;
        };
        self.sent_inputs.push(data.to_string());
        match self.request(DaemonRequest::SendInput {
            session_id,
            data: data.to_string(),
        }) {
            Ok(response) => self.apply_events(response.events),
            Err(error) => self.record_transport_error(error),
        }
    }

    fn resize(&mut self, rows: u16, cols: u16) {
        let Some(session_id) = self.active_session_id.clone() else {
            return;
        };
        self.resize_sent = Some((rows, cols));
        match self.request(DaemonRequest::Resize {
            session_id,
            rows,
            cols,
        }) {
            Ok(response) => self.apply_events(response.events),
            Err(error) => self.record_transport_error(error),
        }
    }

    fn shutdown_session(&mut self) {
        let Some(session_id) = self.selected_session_id() else {
            return;
        };
        match self.request(DaemonRequest::ShutdownSession { session_id }) {
            Ok(response) => {
                self.apply_events(response.events);
                if let Some(cleanup) = response.cleanup {
                    self.notifications.push(format!(
                        "session {} cleanup {}",
                        cleanup.session_id, cleanup.outcome
                    ));
                }
                let _ = self.refresh();
            }
            Err(error) => self.record_transport_error(error),
        }
    }

    fn shutdown_daemon(&mut self) {
        match self.request(DaemonRequest::DaemonShutdown) {
            Ok(_) => self
                .notifications
                .push("daemon shutdown requested".to_string()),
            Err(error) => self.record_transport_error(error),
        }
    }

    fn guarded_notification(&mut self, data: &str) -> TuiResult<crate::DaemonNotify> {
        let Some(session_id) = self.active_session_id.clone() else {
            return Err(TuiError::NoAttachedSession);
        };
        let response = self.request(DaemonRequest::NotifySession {
            session_id,
            data: data.to_string(),
        })?;
        let Some(result) = response
            .coordination
            .and_then(|coordination| coordination.notify)
        else {
            return Err(TuiError::UnexpectedResponse);
        };
        self.notifications.push(format!(
            "doorbell decision={} states={}",
            result.decision,
            result.states.join(",")
        ));
        Ok(result)
    }

    fn drain_or_reconnect(&mut self) -> TuiResult<()> {
        if self.reconnecting {
            std::thread::sleep(RECONNECT_INTERVAL);
            return self.reconnect();
        }
        let Some(session_id) = self.active_session_id.clone() else {
            return Ok(());
        };
        match self.request_without_operator_error_row(DaemonRequest::Drain { session_id }) {
            Ok(response) => {
                if Self::is_drain_unknown_session(&response) {
                    self.clear_stale_attached_session();
                } else {
                    self.record_operator_error(&response);
                    self.apply_events(response.events);
                }
                Ok(())
            }
            Err(error) => {
                self.record_transport_error(error);
                Ok(())
            }
        }
    }

    fn drain_until(&mut self, needle: &str, timeout: Duration) -> TuiResult<()> {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            self.drain_or_reconnect()?;
            if self.output.iter().any(|line| line.contains(needle)) {
                return Ok(());
            }
            std::thread::sleep(DRAIN_INTERVAL);
        }
        Err(TuiError::TimedOut(needle.to_string()))
    }

    fn request(&mut self, request: DaemonRequest) -> DaemonTransportResult<crate::DaemonResponse> {
        self.request_with_operator_error_row(request, true)
    }

    fn request_without_operator_error_row(
        &mut self,
        request: DaemonRequest,
    ) -> DaemonTransportResult<crate::DaemonResponse> {
        self.request_with_operator_error_row(request, false)
    }

    fn request_with_operator_error_row(
        &mut self,
        request: DaemonRequest,
        record_operator_error: bool,
    ) -> DaemonTransportResult<crate::DaemonResponse> {
        let Some(connection) = self.connection.as_mut() else {
            return Err(DaemonTransportError::NotRunning);
        };
        let response = connection.request(&request)?;
        if record_operator_error {
            self.record_operator_error(&response);
        }
        Ok(response)
    }

    fn record_operator_error(&mut self, response: &crate::DaemonResponse) {
        if response.kind == DaemonResponseKind::OperatorError
            && let Some(error) = response.error.as_ref()
        {
            self.errors
                .push(format!("{}: {}", error.code, error.message));
        }
    }

    fn is_drain_unknown_session(response: &crate::DaemonResponse) -> bool {
        response.kind == DaemonResponseKind::OperatorError
            && response.error.as_ref().is_some_and(|error| {
                error.code == "unknown_session" && error.operation == "drain_runtime"
            })
    }

    fn clear_stale_attached_session(&mut self) {
        self.active_session_id = None;
        self.subscription_id = None;
        self.errors
            .push("attached session disappeared; detached and refreshed sessions".to_string());
        if let Err(error) = self.refresh() {
            match error {
                TuiError::Daemon(error) => self.record_transport_error(error),
                error => self
                    .errors
                    .push(format!("refresh failed after session loss: {error}")),
            }
        }
    }

    fn apply_events(&mut self, events: Vec<DaemonEvent>) {
        for event in events {
            match event {
                DaemonEvent::TerminalOutput { data, .. } => self.output.push(data),
                DaemonEvent::ProcessExit {
                    session_id, code, ..
                } => {
                    self.notifications
                        .push(format!("session {session_id} exited {:?}", code));
                }
                DaemonEvent::AttachState { state, .. } => {
                    self.notifications.push(format!("attach {state}"));
                }
                DaemonEvent::SessionLifecycle { session_id, state } => {
                    self.notifications
                        .push(format!("session {session_id} {state}"));
                }
                DaemonEvent::Snapshot { bytes, .. } => {
                    self.notifications.push(format!("snapshot {bytes} bytes"));
                }
                DaemonEvent::Scrollback { bytes, .. } => {
                    self.notifications.push(format!("scrollback {bytes} bytes"));
                }
                DaemonEvent::RuntimeObservation { kind } => {
                    self.notifications.push(format!("runtime {kind}"));
                }
            }
        }
    }

    fn record_transport_error(&mut self, error: DaemonTransportError) {
        self.errors.push(error.to_string());
        if matches!(
            error,
            DaemonTransportError::NotRunning
                | DaemonTransportError::ClientDisconnected
                | DaemonTransportError::Io(_)
        ) {
            self.connection = None;
            self.subscription_id = None;
            self.reconnecting = true;
            self.status = "reconnecting".to_string();
        }
    }

    fn previous_session(&mut self) {
        if !self.sessions.is_empty() {
            self.selected = self.selected.saturating_sub(1);
        }
    }

    fn next_session(&mut self) {
        if !self.sessions.is_empty() {
            self.selected = (self.selected + 1).min(self.sessions.len() - 1);
        }
    }

    fn ui_tree(&self) -> UiNode {
        let status = panel_node(
            "tui-status",
            Some("botster-hub tui"),
            vec![text_node("status-text", self.status.clone())],
        );

        let sessions = if self.sessions.is_empty() {
            empty_state_node(
                "sessions-empty",
                "No sessions",
                "Spawn a session before attaching.",
            )
        } else {
            list_node(
                "sessions-list",
                self.sessions
                    .iter()
                    .enumerate()
                    .map(|(index, session)| {
                        let selected = index == self.selected;
                        let attached = self.active_session_id.as_ref() == Some(&session.session_id);
                        session_row_node(session, selected, attached)
                    })
                    .collect(),
            )
        };
        let sessions = panel_node("sessions-panel", Some("Sessions"), vec![sessions]);

        let active_session_id = self
            .active_session_id
            .clone()
            .or_else(|| self.selected_session_id())
            .unwrap_or_else(|| "no-session".to_string());
        let terminal = panel_node(
            "terminal-panel",
            Some("Attached Output"),
            vec![terminal_view_node("attached-terminal", active_session_id)],
        );

        let body = if let Some(surface) = self.plugin_surface.clone() {
            stack_node(
                "body-stack",
                "horizontal",
                vec![sessions, terminal, surface],
            )
        } else {
            stack_node("body-stack", "horizontal", vec![sessions, terminal])
        };

        let help = panel_node(
            "keys-panel",
            Some("Keys"),
            vec![inline_node(
                "keys-row",
                vec![
                    badge_node("key-enter", "Enter"),
                    text_node("key-enter-text", "attach"),
                    badge_node("key-type", "type"),
                    text_node("key-type-text", "sends after attach"),
                    badge_node("key-detach", "Esc/Ctrl-D"),
                    text_node("key-detach-text", "detach"),
                    badge_node("key-quit", "Ctrl-Q"),
                    text_node("key-quit-text", "quit"),
                    badge_node("key-notify", "Ctrl-N"),
                    text_node("key-notify-text", "doorbell may defer"),
                    badge_node("key-shutdown", "Ctrl-S/Ctrl-X"),
                    text_node("key-shutdown-text", "shutdown"),
                ],
            )],
        );

        let notice_rows = self
            .notifications
            .iter()
            .rev()
            .take(3)
            .enumerate()
            .map(|(index, row)| activity_row_node("notice", index, row, "success"));
        let error_rows = self
            .errors
            .iter()
            .rev()
            .take(3)
            .enumerate()
            .map(|(index, row)| activity_row_node("error", index, row, "danger"));
        let activity_rows: Vec<UiNode> = notice_rows.chain(error_rows).collect();
        let activity = panel_node(
            "activity-panel",
            Some("Activity"),
            vec![if activity_rows.is_empty() {
                empty_state_node(
                    "activity-empty",
                    "No activity",
                    "Activity appears after TUI events.",
                )
            } else {
                list_node("activity-list", activity_rows)
            }],
        );

        stack_node("tui-root", "vertical", vec![status, body, help, activity])
    }

    fn rendered_ui_regions(&self) -> Vec<String> {
        let renderer = TuiUiRenderer::new(TuiUiRenderContext::from_client(self));
        renderer.region_ids(&self.ui_tree())
    }

    fn hit_region(&self, column: u16, row: u16) -> Option<&TuiHitRegion> {
        self.ui_regions
            .iter()
            .filter(|region| region.contains(column, row))
            .min_by_key(|region| u32::from(region.area.width) * u32::from(region.area.height))
    }

    fn activate_node(&mut self, node_id: &UiNodeId) -> TuiResult<()> {
        if let Some(session_id) = node_id.0.strip_prefix("session-row-") {
            self.select_rendered_session(session_id)?;
            return self.attach_selected().map(|_| ());
        }

        let tree = self.ui_tree();
        if let Some(action_id) = find_node(&tree, node_id)
            .and_then(|node| string_prop(node, "action"))
            .map(ToString::to_string)
        {
            self.submit_plugin_action(&action_id);
            return Ok(());
        }

        match node_id.0.as_str() {
            "attached-terminal" | "terminal-panel" => {
                if self.active_session_id.is_none()
                    && let Err(error) = self.attach_selected()
                {
                    self.errors.push(error.to_string());
                }
            }
            "sessions-list" => {
                if let Err(error) = self.attach_selected() {
                    self.errors.push(error.to_string());
                }
            }
            _ => self
                .notifications
                .push(format!("semantic action node {}", node_id.0)),
        }
        Ok(())
    }

    fn submit_plugin_action(&mut self, action_id: &str) {
        let mut payload = self.plugin_form_payload();
        payload.insert(
            "request_id".to_string(),
            Value::String(format!("tui-{action_id}")),
        );
        let response = self.request(DaemonRequest::PluginSurfaceAction {
            package_name: DOGFOOD_PLUGIN.to_string(),
            surface_id: DOGFOOD_SURFACE.to_string(),
            action_id: action_id.to_string(),
            payload: Value::Object(payload),
        });
        match response {
            Ok(response) if response.kind == DaemonResponseKind::PluginActionResult => {
                if let Some(result) = response
                    .plugin_action_result
                    .and_then(|result| serde_json::from_value::<UiActionResult>(result).ok())
                {
                    if result.state == UiActionResultState::Accepted {
                        self.notifications
                            .push("plugin action succeeded".to_string());
                    }
                    self.action_results.push(result);
                }
            }
            Ok(_) => self
                .errors
                .push("plugin action returned unexpected response".to_string()),
            Err(error) => self.record_transport_error(error),
        }
    }

    fn plugin_form_payload(&self) -> Map<String, Value> {
        let mut payload = Map::new();
        if let Some(surface) = self.plugin_surface.as_ref() {
            collect_form_values(surface, &self.form_values, &mut payload);
        }
        payload
    }

    fn edit_focused_form_field(&mut self, key: KeyEvent) -> bool {
        let Some(node_id) = self.focused_node_id.as_ref() else {
            return false;
        };
        if !self.form_values.contains_key(&node_id.0) {
            return false;
        }
        match key.code {
            KeyCode::Char(value) => {
                self.form_values
                    .entry(node_id.0.clone())
                    .or_default()
                    .push(value);
                true
            }
            KeyCode::Backspace => {
                self.form_values.entry(node_id.0.clone()).or_default().pop();
                true
            }
            _ => false,
        }
    }

    fn focus_node(&mut self, node_id: UiNodeId) {
        self.focused_node_id = Some(node_id);
    }
}

fn route_event(app: &mut TuiClient, event: Event) -> TuiResult<bool> {
    match event {
        Event::Key(key) => handle_key(app, key),
        Event::Mouse(mouse) => route_mouse(app, mouse),
        Event::Resize(cols, rows) => {
            app.resize(rows, cols);
            Ok(false)
        }
        _ => Ok(false),
    }
}

fn handle_key(app: &mut TuiClient, key: KeyEvent) -> TuiResult<bool> {
    if app.active_session_id.is_none() && app.edit_focused_form_field(key) {
        return Ok(false);
    }
    match (key.modifiers, key.code) {
        (KeyModifiers::CONTROL, KeyCode::Char('q')) => return Ok(true),
        (KeyModifiers::CONTROL, KeyCode::Char('d')) | (KeyModifiers::NONE, KeyCode::Esc) => {
            app.detach()
        }
        (KeyModifiers::CONTROL, KeyCode::Char('r')) => app.refresh()?,
        (KeyModifiers::CONTROL, KeyCode::Char('n')) => {
            if let Err(error) = app.guarded_notification("botster-doorbell\n") {
                app.errors.push(error.to_string());
            }
        }
        (KeyModifiers::CONTROL, KeyCode::Char('s')) => app.shutdown_session(),
        (KeyModifiers::CONTROL, KeyCode::Char('x')) => app.shutdown_daemon(),
        (KeyModifiers::NONE, KeyCode::Up) if app.active_session_id.is_none() => {
            app.previous_session();
        }
        (KeyModifiers::NONE, KeyCode::Down) if app.active_session_id.is_none() => {
            app.next_session();
        }
        (KeyModifiers::NONE, KeyCode::Enter) if app.active_session_id.is_none() => {
            if let Err(error) = app.attach_selected() {
                app.errors.push(error.to_string());
            }
        }
        (_, KeyCode::Char(value)) if app.active_session_id.is_some() => {
            let mut buffer = [0; 4];
            app.send_input(value.encode_utf8(&mut buffer));
        }
        (_, KeyCode::Enter) if app.active_session_id.is_some() => app.send_input("\r"),
        (_, KeyCode::Backspace) if app.active_session_id.is_some() => app.send_input("\u{7f}"),
        (KeyModifiers::NONE, KeyCode::Enter) | (KeyModifiers::NONE, KeyCode::Char(' ')) => {
            if let Some(node_id) = app.focused_node_id.clone() {
                app.activate_node(&node_id)?;
            }
        }
        _ => {}
    }
    Ok(false)
}

fn route_mouse(app: &mut TuiClient, mouse: MouseEvent) -> TuiResult<bool> {
    let Some(region) = app.hit_region(mouse.column, mouse.row).cloned() else {
        return Ok(false);
    };
    let node_id = region.node_id.clone();

    match mouse.kind {
        MouseEventKind::Down(MouseButton::Left) => {
            app.focus_node(node_id.clone());
            match region.kind {
                UiNodeKind::ListItem => {
                    if let Some(session_id) = node_id.0.strip_prefix("session-row-") {
                        let was_selected = app.selected_session_id().as_deref() == Some(session_id);
                        app.select_rendered_session(session_id)?;
                        if was_selected
                            && app.active_session_id.is_none()
                            && let Err(error) = app.attach_selected()
                        {
                            app.errors.push(error.to_string());
                        }
                    }
                }
                UiNodeKind::TerminalView => {
                    if app.active_session_id.is_none()
                        && let Err(error) = app.attach_selected()
                    {
                        app.errors.push(error.to_string());
                    }
                }
                _ => app.activate_node(&node_id)?,
            }
        }
        MouseEventKind::ScrollUp => {
            if region.kind == UiNodeKind::TerminalView && app.active_session_id.is_some() {
                app.send_input(&sgr_mouse_report(64, mouse.column, mouse.row));
            } else {
                app.previous_session();
            }
        }
        MouseEventKind::ScrollDown => {
            if region.kind == UiNodeKind::TerminalView && app.active_session_id.is_some() {
                app.send_input(&sgr_mouse_report(65, mouse.column, mouse.row));
            } else {
                app.next_session();
            }
        }
        _ => {}
    }

    Ok(false)
}

fn sgr_mouse_report(button: u16, column: u16, row: u16) -> String {
    format!("\x1b[<{button};{};{}M", column + 1, row + 1)
}

fn draw(frame: &mut ratatui::Frame<'_>, app: &mut TuiClient) {
    let tree = app.ui_tree();
    let renderer = TuiUiRenderer::new(TuiUiRenderContext::from_client(app));
    renderer.render(frame, frame.area(), &tree);
    app.ui_regions = renderer.into_regions();
}

#[derive(Debug, Clone, Default)]
struct TuiUiRenderContext {
    terminal_output: String,
    action_results: Vec<UiActionResult>,
    form_values: BTreeMap<String, String>,
}

impl TuiUiRenderContext {
    fn from_client(app: &TuiClient) -> Self {
        Self {
            terminal_output: app
                .output
                .iter()
                .rev()
                .take(200)
                .rev()
                .cloned()
                .collect::<String>(),
            action_results: app.action_results.clone(),
            form_values: app.form_values.clone(),
        }
    }

    fn failure_for(&self, node_id: Option<&UiNodeId>) -> Option<&str> {
        let node_id = node_id?;
        self.action_results.iter().rev().find_map(|result| {
            if result.state != UiActionResultState::Rejected {
                return None;
            }
            if result.node_id.as_ref() == Some(node_id) {
                return result.error.as_deref().or_else(|| {
                    result
                        .form_errors
                        .first()
                        .map(std::string::String::as_str)
                });
            }
            result
                .field_errors
                .get(&node_id.0)
                .and_then(|errors| errors.first())
                .map(std::string::String::as_str)
        })
    }

    fn success_for(&self, node_id: Option<&UiNodeId>) -> Option<&str> {
        let node_id = node_id?;
        self.action_results.iter().rev().find_map(|result| {
            if result.state == UiActionResultState::Accepted
                && result.node_id.as_ref() == Some(node_id)
            {
                return result
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.get("message"))
                    .and_then(Value::as_str)
                    .or(Some("success"));
            }
            None
        })
    }
}

struct TuiUiRenderer {
    context: TuiUiRenderContext,
    regions: RefCell<Vec<TuiHitRegion>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TuiHitRegion {
    node_id: UiNodeId,
    kind: UiNodeKind,
    area: ratatui::layout::Rect,
}

impl TuiHitRegion {
    fn contains(&self, column: u16, row: u16) -> bool {
        column >= self.area.x
            && row >= self.area.y
            && column < self.area.x.saturating_add(self.area.width)
            && row < self.area.y.saturating_add(self.area.height)
    }
}

impl TuiUiRenderer {
    fn new(context: TuiUiRenderContext) -> Self {
        Self {
            context,
            regions: RefCell::new(Vec::new()),
        }
    }

    fn render(&self, frame: &mut ratatui::Frame<'_>, area: ratatui::layout::Rect, node: &UiNode) {
        self.record_region(area, node);
        match node.kind {
            UiNodeKind::Stack => self.render_stack(frame, area, node),
            UiNodeKind::Inline => self.render_inline(frame, area, node),
            UiNodeKind::Panel => self.render_panel(frame, area, node),
            UiNodeKind::List => self.render_list(frame, area, node, None),
            UiNodeKind::TerminalView => self.render_terminal(frame, area, node, None),
            UiNodeKind::Form => self.render_form(frame, area, node, None),
            UiNodeKind::Dialog => self.render_dialog(frame, area, node),
            UiNodeKind::EmptyState => self.render_empty_state(frame, area, node, None),
            _ => self.render_leaf(frame, area, node, None),
        }
    }

    fn into_regions(self) -> Vec<TuiHitRegion> {
        self.regions.into_inner()
    }

    fn record_region(&self, area: ratatui::layout::Rect, node: &UiNode) {
        if area.width == 0 || area.height == 0 {
            return;
        }
        if let Some(node_id) = node.id.as_ref()
            && matches!(
                node.kind,
                UiNodeKind::Panel
                    | UiNodeKind::List
                    | UiNodeKind::ListItem
                    | UiNodeKind::Button
                    | UiNodeKind::IconButton
                    | UiNodeKind::Menu
                    | UiNodeKind::MenuItem
                    | UiNodeKind::Form
                    | UiNodeKind::TextInput
                    | UiNodeKind::Textarea
                    | UiNodeKind::Checkbox
                    | UiNodeKind::Select
                    | UiNodeKind::SelectOption
                    | UiNodeKind::Dialog
                    | UiNodeKind::ScrollArea
                    | UiNodeKind::TerminalView
            )
        {
            self.regions.borrow_mut().push(TuiHitRegion {
                node_id: node_id.clone(),
                kind: node.kind,
                area,
            });
        }
    }

    fn render_stack(
        &self,
        frame: &mut ratatui::Frame<'_>,
        area: ratatui::layout::Rect,
        node: &UiNode,
    ) {
        let children = node_children(node);
        if children.is_empty() {
            self.render_fallback(frame, area, node, None);
            return;
        }

        if node.id.as_ref().is_some_and(|id| id.0 == "tui-root") && children.len() == 4 {
            let areas = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Min(8),
                    Constraint::Length(3),
                    Constraint::Length(7),
                ])
                .split(area);
            for (child, area) in children.iter().zip(areas.iter()) {
                self.render(frame, *area, child);
            }
            return;
        }

        if node.id.as_ref().is_some_and(|id| id.0 == "body-stack") && children.len() == 2 {
            let areas = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Length(32), Constraint::Min(20)])
                .split(area);
            for (child, area) in children.iter().zip(areas.iter()) {
                self.render(frame, *area, child);
            }
            return;
        }

        let direction = match string_prop(node, "direction") {
            Some("horizontal") => Direction::Horizontal,
            _ => Direction::Vertical,
        };
        let constraints = vec![Constraint::Ratio(1, children.len() as u32); children.len()];
        let areas = Layout::default()
            .direction(direction)
            .constraints(constraints)
            .split(area);
        for (child, area) in children.iter().zip(areas.iter()) {
            self.render(frame, *area, child);
        }
    }

    fn render_inline(
        &self,
        frame: &mut ratatui::Frame<'_>,
        area: ratatui::layout::Rect,
        node: &UiNode,
    ) {
        let line = self.node_lines(node).join("  ");
        frame.render_widget(Paragraph::new(line).wrap(Wrap { trim: false }), area);
    }

    fn render_panel(
        &self,
        frame: &mut ratatui::Frame<'_>,
        area: ratatui::layout::Rect,
        node: &UiNode,
    ) {
        let block = Block::default()
            .title(string_prop(node, "title").unwrap_or("Panel"))
            .borders(Borders::ALL);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let children = node_children(node);
        if children.len() == 1 {
            self.render(frame, inner, children[0]);
        } else {
            frame.render_widget(
                Paragraph::new(self.node_lines(node).join("\n")).wrap(Wrap { trim: false }),
                inner,
            );
        }
    }

    fn render_list(
        &self,
        frame: &mut ratatui::Frame<'_>,
        area: ratatui::layout::Rect,
        node: &UiNode,
        title: Option<&str>,
    ) {
        let children = node_children(node);
        for (index, child) in children.iter().enumerate() {
            if area.height <= index as u16 {
                break;
            }
            self.record_region(
                ratatui::layout::Rect {
                    x: area.x,
                    y: area.y + index as u16,
                    width: area.width,
                    height: 1,
                },
                child,
            );
        }
        let items: Vec<ListItem<'_>> = children
            .into_iter()
            .map(|child| ListItem::new(self.node_lines(child).join(" ")))
            .collect();
        let list = List::new(items)
            .block(
                Block::default()
                    .title(title.unwrap_or("List"))
                    .borders(Borders::NONE),
            )
            .highlight_style(Style::default().add_modifier(Modifier::BOLD));
        frame.render_widget(list, area);
    }

    fn render_terminal(
        &self,
        frame: &mut ratatui::Frame<'_>,
        area: ratatui::layout::Rect,
        node: &UiNode,
        title: Option<&str>,
    ) {
        let title = title
            .or_else(|| string_prop(node, "title"))
            .unwrap_or("Terminal");
        let output = if self.context.terminal_output.is_empty() {
            format!(
                "terminal session {}",
                string_prop(node, "session_id").unwrap_or("unknown")
            )
        } else {
            self.context.terminal_output.clone()
        };
        frame.render_widget(
            Paragraph::new(output)
                .block(Block::default().title(title).borders(Borders::NONE))
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn render_form(
        &self,
        frame: &mut ratatui::Frame<'_>,
        area: ratatui::layout::Rect,
        node: &UiNode,
        title: Option<&str>,
    ) {
        for (index, child) in node_children(node).iter().enumerate() {
            if area.height <= index as u16 {
                break;
            }
            self.record_region(
                ratatui::layout::Rect {
                    x: area.x,
                    y: area.y + index as u16,
                    width: area.width,
                    height: 1,
                },
                child,
            );
        }
        let mut rows = self.node_lines(node);
        if let Some(error) = self.context.failure_for(node.id.as_ref()) {
            rows.push(format!("error {error}"));
        }
        if let Some(success) = self.context.success_for(node.id.as_ref()) {
            rows.push(format!("success {success}"));
        }
        frame.render_widget(
            Paragraph::new(rows.join("\n"))
                .block(
                    Block::default()
                        .title(title.unwrap_or("Form"))
                        .borders(Borders::NONE),
                )
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn render_dialog(
        &self,
        frame: &mut ratatui::Frame<'_>,
        area: ratatui::layout::Rect,
        node: &UiNode,
    ) {
        frame.render_widget(
            Paragraph::new(self.node_lines(node).join("\n"))
                .block(
                    Block::default()
                        .title(string_prop(node, "title").unwrap_or("Dialog"))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn render_empty_state(
        &self,
        frame: &mut ratatui::Frame<'_>,
        area: ratatui::layout::Rect,
        node: &UiNode,
        title: Option<&str>,
    ) {
        frame.render_widget(
            Paragraph::new(self.node_lines(node).join("\n"))
                .block(
                    Block::default()
                        .title(title.unwrap_or("Empty"))
                        .borders(Borders::NONE),
                )
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn render_leaf(
        &self,
        frame: &mut ratatui::Frame<'_>,
        area: ratatui::layout::Rect,
        node: &UiNode,
        title: Option<&str>,
    ) {
        frame.render_widget(
            Paragraph::new(self.node_lines(node).join("\n"))
                .block(
                    Block::default()
                        .title(title.unwrap_or(""))
                        .borders(Borders::NONE),
                )
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn render_fallback(
        &self,
        frame: &mut ratatui::Frame<'_>,
        area: ratatui::layout::Rect,
        node: &UiNode,
        title: Option<&str>,
    ) {
        frame.render_widget(
            Paragraph::new(format!("unsupported {:?}", node.kind)).block(
                Block::default()
                    .title(title.unwrap_or("Unsupported"))
                    .borders(Borders::NONE),
            ),
            area,
        );
    }

    fn node_lines(&self, node: &UiNode) -> Vec<String> {
        let mut lines = match node.kind {
            UiNodeKind::Text => vec![string_prop(node, "text").unwrap_or_default().to_string()],
            UiNodeKind::Icon => vec![
                string_prop(node, "label")
                    .or_else(|| string_prop(node, "icon"))
                    .unwrap_or_default()
                    .to_string(),
            ],
            UiNodeKind::Badge => vec![format!(
                "[{}]",
                string_prop(node, "label").unwrap_or_default()
            )],
            UiNodeKind::StatusDot => {
                vec![format!(
                    "* {}",
                    string_prop(node, "label").unwrap_or_default()
                )]
            }
            UiNodeKind::Button | UiNodeKind::IconButton | UiNodeKind::MenuItem => {
                vec![format!(
                    "<{}>",
                    string_prop(node, "label").unwrap_or("action")
                )]
            }
            UiNodeKind::TextInput | UiNodeKind::Textarea => vec![format!(
                "{}: {}",
                string_prop(node, "label").unwrap_or("field"),
                node.id
                    .as_ref()
                    .and_then(|id| self.context.form_values.get(&id.0))
                    .map(String::as_str)
                    .or_else(|| string_prop(node, "value"))
                    .or_else(|| string_prop(node, "placeholder"))
                    .unwrap_or("")
            )],
            UiNodeKind::Checkbox => vec![format!(
                "[{}] {}",
                if bool_prop(node, "checked") { "x" } else { " " },
                string_prop(node, "label").unwrap_or("field")
            )],
            UiNodeKind::Select => {
                let value = string_prop(node, "value").unwrap_or("");
                let mut select_lines = vec![format!(
                    "{}: {value}",
                    string_prop(node, "label").unwrap_or("select")
                )];
                if let Some(options) = node.slots.get("options") {
                    select_lines.extend(options.iter().flat_map(|child| match child {
                        UiChild::Node(node) => self.node_lines(node),
                        _ => vec!["unsupported binding".to_string()],
                    }));
                }
                select_lines
            }
            UiNodeKind::SelectOption => vec![format!(
                "{} ({})",
                string_prop(node, "label").unwrap_or("option"),
                string_prop(node, "value").unwrap_or("")
            )],
            UiNodeKind::EmptyState => vec![
                string_prop(node, "title").unwrap_or("Empty").to_string(),
                string_prop(node, "description")
                    .unwrap_or_default()
                    .to_string(),
            ],
            UiNodeKind::TerminalView => vec![format!(
                "terminal session {}",
                string_prop(node, "session_id").unwrap_or("unknown")
            )],
            UiNodeKind::ListItem | UiNodeKind::TreeItem | UiNodeKind::Dialog => {
                self.slot_lines(node)
            }
            UiNodeKind::Table | UiNodeKind::Menu | UiNodeKind::Tree | UiNodeKind::ScrollArea => {
                vec![format!("unsupported {:?}", node.kind)]
            }
            _ => node_children(node)
                .into_iter()
                .flat_map(|child| self.node_lines(child))
                .collect(),
        };

        if let Some(error) = self.context.failure_for(node.id.as_ref()) {
            lines.push(format!("error {error}"));
        }
        if let Some(success) = self.context.success_for(node.id.as_ref()) {
            lines.push(format!("success {success}"));
        }
        lines.retain(|line| !line.is_empty());
        lines
    }

    fn slot_lines(&self, node: &UiNode) -> Vec<String> {
        ["title", "subtitle", "meta", "actions", "body"]
            .into_iter()
            .filter_map(|slot| node.slots.get(slot))
            .flat_map(|children| {
                children.iter().flat_map(|child| match child {
                    UiChild::Node(node) => self.node_lines(node),
                    _ => vec!["unsupported binding".to_string()],
                })
            })
            .collect()
    }

    fn region_ids(&self, node: &UiNode) -> Vec<String> {
        let mut ids = Vec::new();
        self.collect_region_ids(node, &mut ids);
        ids
    }

    fn collect_region_ids(&self, node: &UiNode, ids: &mut Vec<String>) {
        if matches!(
            node.kind,
            UiNodeKind::Panel | UiNodeKind::List | UiNodeKind::TerminalView | UiNodeKind::Form
        ) && let Some(id) = node.id.as_ref()
        {
            ids.push(id.0.clone());
        }

        for child in node_children(node) {
            self.collect_region_ids(child, ids);
        }
        for children in node.slots.values() {
            for child in children {
                if let UiChild::Node(node) = child {
                    self.collect_region_ids(node, ids);
                }
            }
        }
    }
}

fn node_children(node: &UiNode) -> Vec<&UiNode> {
    node.children
        .iter()
        .filter_map(|child| match child {
            UiChild::Node(node) => Some(node.as_ref()),
            _ => None,
        })
        .collect()
}

fn find_node<'a>(node: &'a UiNode, node_id: &UiNodeId) -> Option<&'a UiNode> {
    if node.id.as_ref() == Some(node_id) {
        return Some(node);
    }
    node.children
        .iter()
        .chain(node.slots.values().flat_map(|children| children.iter()))
        .filter_map(|child| match child {
            UiChild::Node(node) => Some(node.as_ref()),
            _ => None,
        })
        .find_map(|child| find_node(child, node_id))
}

fn collect_form_values(
    node: &UiNode,
    values: &BTreeMap<String, String>,
    payload: &mut Map<String, Value>,
) {
    if matches!(
        node.kind,
        UiNodeKind::TextInput | UiNodeKind::Textarea | UiNodeKind::Select
    ) && let (Some(node_id), Some(name)) = (node.id.as_ref(), string_prop(node, "name"))
    {
        let value = values
            .get(&node_id.0)
            .cloned()
            .or_else(|| string_prop(node, "value").map(ToString::to_string))
            .unwrap_or_default();
        payload.insert(name.to_string(), Value::String(value));
    } else if node.kind == UiNodeKind::Checkbox
        && let Some(name) = string_prop(node, "name")
    {
        payload.insert(name.to_string(), Value::Bool(bool_prop(node, "checked")));
    }
    for child in node
        .children
        .iter()
        .chain(node.slots.values().flat_map(|children| children.iter()))
    {
        if let UiChild::Node(node) = child {
            collect_form_values(node, values, payload);
        }
    }
}

fn string_prop<'a>(node: &'a UiNode, prop: &str) -> Option<&'a str> {
    node.props.get(prop).and_then(Value::as_str)
}

fn bool_prop(node: &UiNode, prop: &str) -> bool {
    node.props
        .get(prop)
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn stack_node(id: &str, direction: &str, children: Vec<UiNode>) -> UiNode {
    node_with_props(
        id,
        UiNodeKind::Stack,
        vec![("direction", Value::String(direction.to_string()))],
        children,
    )
}

fn inline_node(id: &str, children: Vec<UiNode>) -> UiNode {
    node_with_props(id, UiNodeKind::Inline, Vec::new(), children)
}

fn panel_node(id: &str, title: Option<&str>, children: Vec<UiNode>) -> UiNode {
    node_with_props(
        id,
        UiNodeKind::Panel,
        title
            .map(|title| vec![("title", Value::String(title.to_string()))])
            .unwrap_or_default(),
        children,
    )
}

fn list_node(id: &str, children: Vec<UiNode>) -> UiNode {
    node_with_props(id, UiNodeKind::List, Vec::new(), children)
}

fn text_node(id: &str, text: impl Into<String>) -> UiNode {
    node_with_props(
        id,
        UiNodeKind::Text,
        vec![("text", Value::String(text.into()))],
        Vec::new(),
    )
}

fn badge_node(id: &str, label: &str) -> UiNode {
    node_with_props(
        id,
        UiNodeKind::Badge,
        vec![("label", Value::String(label.to_string()))],
        Vec::new(),
    )
}

fn empty_state_node(id: &str, title: &str, description: &str) -> UiNode {
    node_with_props(
        id,
        UiNodeKind::EmptyState,
        vec![
            ("title", Value::String(title.to_string())),
            ("description", Value::String(description.to_string())),
        ],
        Vec::new(),
    )
}

fn terminal_view_node(id: &str, session_id: String) -> UiNode {
    node_with_props(
        id,
        UiNodeKind::TerminalView,
        vec![
            ("session_id", Value::String(session_id)),
            ("title", Value::String("Attached Output".to_string())),
        ],
        Vec::new(),
    )
}

fn session_row_node(session: &DaemonSession, selected: bool, attached: bool) -> UiNode {
    let mut node = node_with_props(
        &format!("session-row-{}", session.session_id),
        UiNodeKind::ListItem,
        vec![
            ("value", Value::String(session.session_id.clone())),
            ("selected", Value::Bool(selected)),
        ],
        Vec::new(),
    );
    node.slots.insert(
        "title".to_string(),
        vec![ui_child(text_node(
            &format!("session-title-{}", session.session_id),
            format!(
                "{}{}",
                if selected { "> " } else { "  " },
                session.session_id
            ),
        ))],
    );
    node.slots.insert(
        "subtitle".to_string(),
        vec![ui_child(status_dot_node(
            &format!("session-state-{}", session.session_id),
            &session.lifecycle,
            "success",
        ))],
    );
    if attached {
        node.slots.insert(
            "meta".to_string(),
            vec![ui_child(badge_node(
                &format!("session-attached-{}", session.session_id),
                "attached",
            ))],
        );
    }
    node
}

fn activity_row_node(kind: &str, index: usize, message: &str, tone: &str) -> UiNode {
    let mut node = node_with_props(
        &format!("activity-{kind}-{index}"),
        UiNodeKind::ListItem,
        vec![("value", Value::String(message.to_string()))],
        Vec::new(),
    );
    node.slots.insert(
        "title".to_string(),
        vec![ui_child(status_dot_node(
            &format!("activity-{kind}-{index}-status"),
            kind,
            tone,
        ))],
    );
    node.slots.insert(
        "subtitle".to_string(),
        vec![ui_child(text_node(
            &format!("activity-{kind}-{index}-message"),
            message,
        ))],
    );
    node
}

fn status_dot_node(id: &str, label: &str, tone: &str) -> UiNode {
    node_with_props(
        id,
        UiNodeKind::StatusDot,
        vec![
            ("label", Value::String(label.to_string())),
            ("tone", Value::String(tone.to_string())),
        ],
        Vec::new(),
    )
}

fn node_with_props(
    id: &str,
    kind: UiNodeKind,
    props: Vec<(&str, Value)>,
    children: Vec<UiNode>,
) -> UiNode {
    UiNode {
        kind,
        id: Some(UiNodeId(id.to_string())),
        props: props
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect::<Map<String, Value>>(),
        children: children.into_iter().map(ui_child).collect(),
        slots: Default::default(),
    }
}

fn ui_child(node: UiNode) -> UiChild {
    UiChild::Node(Box::new(node))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use botster_core::{RequestId, UiActionId, UiFieldErrors, UiFormValues, UiSurfaceId};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    #[test]
    fn tui_ui_renderer_renders_representative_core_primitives() {
        let mut dialog = node_with_props(
            "confirm-dialog",
            UiNodeKind::Dialog,
            vec![("title", Value::String("Confirm".to_string()))],
            Vec::new(),
        );
        dialog.slots.insert(
            "body".to_string(),
            vec![ui_child(text_node("confirm-copy", "Dialog body"))],
        );

        let mut select = node_with_props(
            "priority",
            UiNodeKind::Select,
            vec![
                ("name", Value::String("priority".to_string())),
                ("label", Value::String("Priority".to_string())),
                ("value", Value::String("high".to_string())),
            ],
            Vec::new(),
        );
        select.slots.insert(
            "options".to_string(),
            vec![ui_child(node_with_props(
                "priority-high",
                UiNodeKind::SelectOption,
                vec![
                    ("value", Value::String("high".to_string())),
                    ("label", Value::String("High".to_string())),
                ],
                Vec::new(),
            ))],
        );

        let fixture = stack_node(
            "fixture",
            "vertical",
            vec![
                text_node("copy", "Hello UiNode"),
                badge_node("badge", "ready"),
                status_dot_node("state", "running", "success"),
                list_node("items", vec![list_item_with_title("item-1", "First row")]),
                node_with_props(
                    "name",
                    UiNodeKind::TextInput,
                    vec![
                        ("name", Value::String("name".to_string())),
                        ("label", Value::String("Name".to_string())),
                        ("value", Value::String("Botster".to_string())),
                    ],
                    Vec::new(),
                ),
                select,
                node_with_props(
                    "enabled",
                    UiNodeKind::Checkbox,
                    vec![
                        ("name", Value::String("enabled".to_string())),
                        ("label", Value::String("Enabled".to_string())),
                        ("checked", Value::Bool(true)),
                    ],
                    Vec::new(),
                ),
                dialog,
                empty_state_node("empty", "Nothing here", "Try another view."),
                terminal_view_node("terminal", "session-1".to_string()),
            ],
        );

        fixture
            .validate()
            .expect("fixture should satisfy core schema");
        let rendered = render_text(&fixture, TuiUiRenderContext::default());
        let frame = render_frame_text(&fixture, TuiUiRenderContext::default(), 80, 40);
        let select_frame = render_frame_text(
            node_children(&fixture)[5],
            TuiUiRenderContext::default(),
            40,
            6,
        );
        assert!(rendered.contains("Hello UiNode"));
        assert!(rendered.contains("[ready]"));
        assert!(rendered.contains("* running"));
        assert!(rendered.contains("First row"));
        assert!(rendered.contains("Name: Botster"));
        assert!(rendered.contains("Priority: high"));
        assert!(rendered.contains("High (high)"));
        assert!(rendered.contains("[x] Enabled"));
        assert!(rendered.contains("Dialog body"));
        assert!(rendered.contains("Nothing here"));
        assert!(rendered.contains("terminal session session-1"));
        assert!(frame.contains("Hello UiNode"));
        assert!(frame.contains("First row"));
        assert!(select_frame.contains("Priority: high"));
        assert!(select_frame.contains("High"));
        assert!(frame.contains("terminal session session-1"));
    }

    #[test]
    fn tui_ui_renderer_renders_form_fields_and_action_failure_errors() {
        let form = node_with_props(
            "settings-form",
            UiNodeKind::Form,
            Vec::new(),
            vec![
                node_with_props(
                    "project-name",
                    UiNodeKind::TextInput,
                    vec![
                        ("name", Value::String("name".to_string())),
                        ("label", Value::String("Name".to_string())),
                        ("value", Value::String("".to_string())),
                    ],
                    Vec::new(),
                ),
                node_with_props(
                    "notes",
                    UiNodeKind::Textarea,
                    vec![
                        ("name", Value::String("notes".to_string())),
                        ("label", Value::String("Notes".to_string())),
                    ],
                    Vec::new(),
                ),
                node_with_props(
                    "notify",
                    UiNodeKind::Checkbox,
                    vec![
                        ("name", Value::String("notify".to_string())),
                        ("label", Value::String("Notify".to_string())),
                        ("checked", Value::Bool(false)),
                    ],
                    Vec::new(),
                ),
            ],
        );
        let failure = UiActionResult {
            request_id: RequestId("request-1".to_string()),
            surface_id: UiSurfaceId("fixture.surface".to_string()),
            action_id: UiActionId("save-settings".to_string()),
            node_id: Some(UiNodeId("project-name".to_string())),
            state: UiActionResultState::Rejected,
            field_errors: UiFieldErrors::new(),
            form_errors: Vec::new(),
            warnings: Vec::new(),
            normalized_values: None,
            tree_update: None,
            payload: None,
            error: Some("Name is required".to_string()),
        };

        form.validate()
            .expect("form fixture should satisfy core schema");
        let rendered = render_text(
            &form,
            TuiUiRenderContext {
                terminal_output: String::new(),
                action_results: vec![failure],
                form_values: BTreeMap::new(),
            },
        );
        let frame = render_frame_text(
            &form,
            TuiUiRenderContext {
                terminal_output: String::new(),
                action_results: vec![UiActionResult {
                    request_id: RequestId("request-1".to_string()),
                    surface_id: UiSurfaceId("fixture.surface".to_string()),
                    action_id: UiActionId("save-settings".to_string()),
                    node_id: Some(UiNodeId("project-name".to_string())),
                    state: UiActionResultState::Rejected,
                    field_errors: UiFieldErrors::new(),
                    form_errors: Vec::new(),
                    warnings: Vec::new(),
                    normalized_values: None,
                    tree_update: None,
                    payload: None,
                    error: Some("Name is required".to_string()),
                }],
                form_values: BTreeMap::new(),
            },
            60,
            12,
        );
        assert!(rendered.contains("Name:"));
        assert!(rendered.contains("Notes:"));
        assert!(rendered.contains("[ ] Notify"));
        assert!(rendered.contains("error Name is required"));
        assert!(frame.contains("Name:"));
        assert!(frame.contains("error Name is required"));
    }

    #[test]
    fn tui_ui_renderer_maps_structured_plugin_field_errors_and_success_payloads() {
        let form = node_with_props(
            "project-pipelines-create-form",
            UiNodeKind::Form,
            vec![(
                "action",
                Value::String("project_pipelines.create_ticket".to_string()),
            )],
            vec![
                node_with_props(
                    "project-pipelines-create-title",
                    UiNodeKind::TextInput,
                    vec![
                        ("name", Value::String("title".to_string())),
                        ("label", Value::String("Title".to_string())),
                        ("placeholder", Value::String("Ticket title".to_string())),
                    ],
                    Vec::new(),
                ),
                node_with_props(
                    "project-pipelines-create-submit",
                    UiNodeKind::Button,
                    vec![
                        ("label", Value::String("Create ticket".to_string())),
                        (
                            "action",
                            Value::String("project_pipelines.create_ticket".to_string()),
                        ),
                    ],
                    Vec::new(),
                ),
            ],
        );
        let failure = UiActionResult {
            request_id: RequestId("request-1".to_string()),
            surface_id: UiSurfaceId("project-pipelines.create-ticket".to_string()),
            action_id: UiActionId("project_pipelines.create_ticket".to_string()),
            node_id: Some(UiNodeId("project-pipelines-create-form".to_string())),
            state: UiActionResultState::Rejected,
            field_errors: UiFieldErrors::from_iter([(
                "project-pipelines-create-title".to_string(),
                vec!["Title is required".to_string()],
            )]),
            form_errors: vec!["Title is required".to_string()],
            warnings: Vec::new(),
            normalized_values: None,
            tree_update: None,
            payload: None,
            error: Some("Title is required".to_string()),
        };
        let success = UiActionResult {
            request_id: RequestId("request-2".to_string()),
            surface_id: UiSurfaceId("project-pipelines.create-ticket".to_string()),
            action_id: UiActionId("project_pipelines.create_ticket".to_string()),
            node_id: Some(UiNodeId("project-pipelines-create-form".to_string())),
            state: UiActionResultState::Accepted,
            field_errors: UiFieldErrors::new(),
            form_errors: Vec::new(),
            warnings: Vec::new(),
            normalized_values: Some(UiFormValues(Map::from_iter([(
                "title".to_string(),
                Value::String("Dogfood ticket".to_string()),
            )]))),
            tree_update: None,
            payload: Some(serde_json::json!({
                "message": "Ticket created",
                "ticket": { "id": "ticket_local_1", "title": "Dogfood ticket" }
            })),
            error: None,
        };

        form.validate()
            .expect("project pipelines form should satisfy core schema");
        let failure_frame = render_frame_text(
            &form,
            TuiUiRenderContext {
                terminal_output: String::new(),
                action_results: vec![failure],
                form_values: BTreeMap::new(),
            },
            70,
            10,
        );
        let success_frame = render_frame_text(
            &form,
            TuiUiRenderContext {
                terminal_output: String::new(),
                action_results: vec![success],
                form_values: BTreeMap::from([(
                    "project-pipelines-create-title".to_string(),
                    "Dogfood ticket".to_string(),
                )]),
            },
            70,
            10,
        );

        assert!(failure_frame.contains("Title: Ticket title"));
        assert!(failure_frame.contains("error Title is required"));
        assert!(success_frame.contains("Title: Dogfood ticket"));
        assert!(success_frame.contains("success Ticket created"));
    }

    #[test]
    fn focused_form_field_typing_updates_submitted_value_without_placeholder() {
        let mut client = TuiClient::new(test_config());
        let surface = node_with_props(
            "generic-form",
            UiNodeKind::Form,
            Vec::new(),
            vec![node_with_props(
                "generic-title",
                UiNodeKind::TextInput,
                vec![
                    ("name", Value::String("title".to_string())),
                    ("label", Value::String("Title".to_string())),
                    ("placeholder", Value::String("Ticket title".to_string())),
                ],
                Vec::new(),
            )],
        );
        client.seed_form_values(&surface);
        client.plugin_surface = Some(surface);
        client.focus_node(UiNodeId("generic-title".to_string()));

        assert_eq!(
            client.form_values.get("generic-title").map(String::as_str),
            Some("")
        );
        assert!(
            !handle_key(
                &mut client,
                KeyEvent::new(KeyCode::Char('A'), KeyModifiers::NONE)
            )
            .expect("typing into a focused field should route through handle_key")
        );
        assert!(
            !handle_key(
                &mut client,
                KeyEvent::new(KeyCode::Char('b'), KeyModifiers::NONE)
            )
            .expect("typing into a focused field should route through handle_key")
        );
        assert_eq!(
            client.plugin_form_payload().get("title"),
            Some(&Value::String("Ab".to_string()))
        );
    }

    #[test]
    fn click_on_node_action_prop_dispatches_plugin_action_without_hardcoded_id() {
        let mut client = TuiClient::new(test_config());
        client.plugin_surface = Some(node_with_props(
            "generic-form",
            UiNodeKind::Form,
            Vec::new(),
            vec![node_with_props(
                "generic-submit",
                UiNodeKind::Button,
                vec![
                    ("label", Value::String("Save".to_string())),
                    (
                        "action",
                        Value::String("project_pipelines.create_ticket".to_string()),
                    ),
                ],
                Vec::new(),
            )],
        ));
        client.ui_regions = vec![TuiHitRegion {
            node_id: UiNodeId("generic-submit".to_string()),
            kind: UiNodeKind::Button,
            area: ratatui::layout::Rect::new(1, 1, 10, 1),
        }];

        route_event(
            &mut client,
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: 2,
                row: 1,
                modifiers: KeyModifiers::NONE,
            }),
        )
        .expect("mouse click should route through action-prop activation");

        assert!(
            client
                .errors
                .iter()
                .any(|error| error.contains("not running")),
            "click should attempt plugin action dispatch through the daemon"
        );
        assert!(
            client.notifications.is_empty(),
            "action-prop dispatch should not fall back to generic semantic notification"
        );
    }

    #[test]
    fn tui_ui_renderer_renders_action_rows() {
        let mut row = list_item_with_title("session-row", "session-1");
        row.slots.insert(
            "actions".to_string(),
            vec![ui_child(node_with_props(
                "attach-button",
                UiNodeKind::Button,
                vec![
                    ("label", Value::String("Attach".to_string())),
                    ("action", Value::String("session.attach".to_string())),
                ],
                Vec::new(),
            ))],
        );
        row.validate()
            .expect("action row should satisfy core schema");
        let rendered = render_text(&row, TuiUiRenderContext::default());
        let frame = render_frame_text(&row, TuiUiRenderContext::default(), 40, 6);
        assert!(rendered.contains("session-1"));
        assert!(rendered.contains("<Attach>"));
        assert!(frame.contains("session-1"));
        assert!(frame.contains("<Attach>"));
    }

    #[test]
    fn tui_ui_renderer_falls_back_for_unsupported_primitives() {
        let table = node_with_props(
            "table",
            UiNodeKind::Table,
            vec![("columns", Value::Array(Vec::new()))],
            Vec::new(),
        );
        let rendered = render_text(&table, TuiUiRenderContext::default());
        let frame = render_frame_text(&table, TuiUiRenderContext::default(), 40, 6);
        assert!(rendered.contains("unsupported Table"));
        assert!(frame.contains("unsupported Table"));
    }

    #[test]
    fn tui_ui_renderer_renders_hub_authored_tui_tree_through_real_frame() {
        let mut client = TuiClient::new(test_config());
        client.sessions = vec![
            DaemonSession {
                session_id: "session-a".to_string(),
                lifecycle: "running".to_string(),
            },
            DaemonSession {
                session_id: "session-b".to_string(),
                lifecycle: "exited".to_string(),
            },
        ];
        client.selected = 1;
        client.active_session_id = Some("session-a".to_string());
        client.output.push("hello from terminal\n".to_string());
        client.notifications.push("notice one".to_string());
        client.notifications.push("notice two".to_string());
        client.errors.push("error one".to_string());

        let frame = render_frame_text(
            &client.ui_tree(),
            TuiUiRenderContext::from_client(&client),
            100,
            30,
        );

        assert!(frame.contains("botster-hub tui"));
        assert!(frame.contains("session-a"));
        assert!(frame.contains("session-b"));
        assert!(frame.contains("hello from terminal"));
        assert!(frame.contains("notice"));
        assert!(frame.contains("error"));

        let ids = all_node_ids(&client.ui_tree());
        let mut sorted = ids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(ids.len(), sorted.len(), "UiNode ids should be unique");
    }

    #[test]
    fn route_event_handles_key_and_mouse_selection_through_runtime_router() {
        let mut client = TuiClient::new(test_config());
        client.sessions = vec![
            DaemonSession {
                session_id: "session-a".to_string(),
                lifecycle: "running".to_string(),
            },
            DaemonSession {
                session_id: "session-b".to_string(),
                lifecycle: "running".to_string(),
            },
        ];
        draw_client(&mut client, 100, 30);

        let should_quit = route_event(
            &mut client,
            Event::Key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE)),
        )
        .expect("key router should handle selection");
        assert!(!should_quit);
        assert_eq!(client.selected_session_id().as_deref(), Some("session-b"));

        route_event(
            &mut client,
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: 2,
                row: 4,
                modifiers: KeyModifiers::NONE,
            }),
        )
        .expect("mouse router should hit-test session row");
        assert_eq!(client.selected_session_id().as_deref(), Some("session-a"));
        assert_eq!(
            client.focused_node_id.as_ref().map(|id| id.0.as_str()),
            Some("session-row-session-a")
        );
    }

    #[test]
    fn route_event_forwards_terminal_owned_mouse_reports_as_raw_input() {
        let mut client = TuiClient::new(test_config());
        client.sessions = vec![DaemonSession {
            session_id: "session-a".to_string(),
            lifecycle: "running".to_string(),
        }];
        client.active_session_id = Some("session-a".to_string());
        draw_client(&mut client, 100, 30);

        route_event(
            &mut client,
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: 35,
                row: 5,
                modifiers: KeyModifiers::NONE,
            }),
        )
        .expect("mouse router should forward terminal-owned wheel report");

        assert_eq!(client.sent_inputs, vec!["\x1b[<65;36;6M".to_string()]);
    }

    fn render_text(node: &UiNode, context: TuiUiRenderContext) -> String {
        TuiUiRenderer::new(context).node_lines(node).join("\n")
    }

    fn test_config() -> HubConfig {
        crate::HubStartupOptions::default()
            .build_config_for_environment(&crate::RuntimeEnvironment::from_values(
                Some(PathBuf::from("target/tui-renderer-test")),
                None,
                None,
            ))
            .expect("test config should build")
    }

    fn render_frame_text(
        node: &UiNode,
        context: TuiUiRenderContext,
        width: u16,
        height: u16,
    ) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).expect("test backend terminal should initialize");
        let renderer = TuiUiRenderer::new(context);
        terminal
            .draw(|frame| renderer.render(frame, frame.area(), node))
            .expect("renderer should draw into test backend");
        buffer_text(terminal.backend().buffer())
    }

    fn draw_client(client: &mut TuiClient, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).expect("test backend terminal should initialize");
        terminal
            .draw(|frame| draw(frame, client))
            .expect("TUI client should draw into test backend");
        buffer_text(terminal.backend().buffer())
    }

    fn buffer_text(buffer: &ratatui::buffer::Buffer) -> String {
        let width = buffer.area.width as usize;
        buffer
            .content()
            .chunks(width)
            .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn all_node_ids(node: &UiNode) -> Vec<String> {
        let mut ids = Vec::new();
        collect_node_ids(node, &mut ids);
        ids
    }

    fn collect_node_ids(node: &UiNode, ids: &mut Vec<String>) {
        if let Some(id) = node.id.as_ref() {
            ids.push(id.0.clone());
        }
        for child in &node.children {
            if let UiChild::Node(node) = child {
                collect_node_ids(node, ids);
            }
        }
        for children in node.slots.values() {
            for child in children {
                if let UiChild::Node(node) = child {
                    collect_node_ids(node, ids);
                }
            }
        }
    }

    fn list_item_with_title(id: &str, title: &str) -> UiNode {
        let mut node = node_with_props(
            id,
            UiNodeKind::ListItem,
            vec![("value", Value::String(id.to_string()))],
            Vec::new(),
        );
        node.slots.insert(
            "title".to_string(),
            vec![ui_child(text_node(&format!("{id}-title"), title))],
        );
        node
    }
}

fn process_unique_seed() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{}-{nanos}", std::process::id())
}

struct TerminalModeGuard;

impl TerminalModeGuard {
    fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        execute!(io::stdout(), EnterAlternateScreen, EnableMouseCapture)?;
        Ok(Self)
    }
}

impl Drop for TerminalModeGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture);
    }
}

/// TUI runtime error.
#[derive(Debug)]
pub enum TuiError {
    Io(io::Error),
    Daemon(DaemonTransportError),
    NoSessions,
    NoAttachedSession,
    SessionMissing(String),
    UnexpectedResponse,
    TimedOut(String),
}

impl fmt::Display for TuiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Daemon(error) => write!(formatter, "{error}"),
            Self::NoSessions => write!(formatter, "no sessions available"),
            Self::NoAttachedSession => write!(formatter, "no attached session"),
            Self::SessionMissing(session_id) => {
                write!(formatter, "session not found: {session_id}")
            }
            Self::UnexpectedResponse => write!(formatter, "unexpected daemon response"),
            Self::TimedOut(needle) => write!(formatter, "timed out waiting for {needle:?}"),
        }
    }
}

impl std::error::Error for TuiError {}

impl From<io::Error> for TuiError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<DaemonTransportError> for TuiError {
    fn from(error: DaemonTransportError) -> Self {
        Self::Daemon(error)
    }
}

pub type TuiResult<T> = Result<T, TuiError>;
