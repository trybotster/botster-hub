//! Minimal local TUI over the daemon socket client API.

use std::fmt;
use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyModifiers,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};

use crate::{
    DaemonConnection, DaemonEvent, DaemonRequest, DaemonResponseKind, DaemonSession,
    DaemonTransportError, DaemonTransportResult, HubConfig,
};

const DRAIN_INTERVAL: Duration = Duration::from_millis(50);
const RECONNECT_INTERVAL: Duration = Duration::from_millis(250);

/// Run the interactive terminal UI until the operator quits.
pub fn run(config: HubConfig) -> TuiResult<()> {
    let _guard = TerminalModeGuard::enter()?;
    let stdout = io::stdout();
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut app = TuiClient::new(config);
    app.reconnect()?;

    loop {
        terminal.draw(|frame| draw(frame, &app))?;
        if event::poll(DRAIN_INTERVAL)? {
            match event::read()? {
                Event::Key(key) if handle_key(&mut app, key)? => break,
                Event::Resize(cols, rows) => app.resize(rows, cols),
                _ => {}
            }
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
    notifications: Vec<String>,
    errors: Vec<String>,
    status: String,
    reconnecting: bool,
    resize_sent: Option<(u16, u16)>,
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
            notifications: Vec::new(),
            errors: Vec::new(),
            status: "starting".to_string(),
            reconnecting: false,
            resize_sent: None,
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
        Ok(())
    }

    fn select_session(&mut self, session_id: &str) -> TuiResult<()> {
        self.refresh()?;
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
}

fn handle_key(app: &mut TuiClient, key: KeyEvent) -> TuiResult<bool> {
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
        _ => {}
    }
    Ok(false)
}

fn draw(frame: &mut ratatui::Frame<'_>, app: &TuiClient) {
    let areas = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(3),
            Constraint::Length(7),
        ])
        .split(frame.area());
    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(32), Constraint::Min(20)])
        .split(areas[1]);

    let title = Paragraph::new(Line::from(vec![
        Span::styled("botster-hub tui", Style::default().fg(Color::Cyan)),
        Span::raw("  "),
        Span::raw(app.status.clone()),
    ]))
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(title, areas[0]);

    let sessions: Vec<ListItem<'_>> = app
        .sessions
        .iter()
        .enumerate()
        .map(|(index, session)| {
            let prefix = if index == app.selected { "> " } else { "  " };
            let active = if app.active_session_id.as_ref() == Some(&session.session_id) {
                " attached"
            } else {
                ""
            };
            ListItem::new(format!(
                "{prefix}{} [{}]{}",
                session.session_id, session.lifecycle, active
            ))
        })
        .collect();
    let sessions = List::new(sessions)
        .block(Block::default().title("Sessions").borders(Borders::ALL))
        .highlight_style(Style::default().add_modifier(Modifier::BOLD));
    frame.render_widget(sessions, body[0]);

    let output = app
        .output
        .iter()
        .rev()
        .take(200)
        .rev()
        .cloned()
        .collect::<String>();
    let terminal = Paragraph::new(output)
        .block(
            Block::default()
                .title("Attached Output")
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(terminal, body[1]);

    let help = Paragraph::new(Line::from(vec![
        Span::styled("Enter", Style::default().fg(Color::Cyan)),
        Span::raw(" attach  "),
        Span::styled("type", Style::default().fg(Color::Cyan)),
        Span::raw(" sends after attach  "),
        Span::styled("Esc/Ctrl-D", Style::default().fg(Color::Cyan)),
        Span::raw(" detach  "),
        Span::styled("Ctrl-Q", Style::default().fg(Color::Cyan)),
        Span::raw(" quit  "),
        Span::styled("Ctrl-N", Style::default().fg(Color::Cyan)),
        Span::raw(" doorbell may defer  "),
        Span::styled("Ctrl-S/Ctrl-X", Style::default().fg(Color::Red)),
        Span::raw(" shutdown"),
    ]))
    .block(Block::default().title("Keys").borders(Borders::ALL))
    .wrap(Wrap { trim: false });
    frame.render_widget(help, areas[2]);

    let mut rows = Vec::new();
    rows.extend(app.notifications.iter().rev().take(3).map(|row| {
        Line::from(vec![
            Span::styled("notice ", Style::default().fg(Color::Green)),
            Span::raw(row.clone()),
        ])
    }));
    rows.extend(app.errors.iter().rev().take(3).map(|row| {
        Line::from(vec![
            Span::styled("error ", Style::default().fg(Color::Red)),
            Span::raw(row.clone()),
        ])
    }));
    let status = Paragraph::new(rows)
        .block(Block::default().title("Activity").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(status, areas[3]);
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
