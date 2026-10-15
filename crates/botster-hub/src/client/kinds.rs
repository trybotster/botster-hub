//! The `session` and `session_type` kinds of the control envelope (HC sections 5 and 6; design sections 7 and 7a): the
//! reads, and the writes of `session` (`create`, `update{label}`, `delete`).
//!
//! One [`ConnKinds`] lives with each control connection on the `io` thread. It never calls Core:
//! - [`ConnKinds::begin`] turns a `req` into a [`Step`]: an answer now, a request for the owner, or a local watch;
//! - [`ConnKinds::answer`] turns the owner's answer to a request into the frames to write;
//! - [`ConnKinds::frame`] turns a frame of a session watch into the frames to write.
//!
//! The rows are built in one place, [`session_row`]. A `list` is one retained cut: the first call takes the owner's rows
//! (one pointer per row), builds and filters them, and a cursor holds the rest (VB-1). A `filter` is an equality map over
//! the kind's core fields. A watch with a filter keeps the ids that it has shown, so that a row which stops matching gets
//! `remove` and a row which starts to match gets `upsert` (WT-2).
//!
//! The writes:
//! - `create` mints the session id (128 random bits, HC section 6), builds the `SpawnRequest` with the Hub's labels
//!   (`create_labels`), and asks the owner for the spawn operation. The answer is the receipt (A.9).
//! - `update` sets or removes `h/label` through the owner's one label writer (`SetLabels`); the answer is the row.
//! - `delete` asks the owner for `Remove` of an `ended` or `lost` row (VB-5, A.2c).
//!
//! `idempotency_key` and `operation_id` (EN-8, EN-7b) are not served in Stage 2: a mutation that carries one is
//! `unsupported`, never run twice by a retry.

use crate::bridge::SpawnAnswer;
use crate::bridge::{Answer, Effect, FailKind, Failure, Outbox, Request};
use crate::client::rows::{
    create_labels, session_row, RowFacts, SessionTypes, TypeRefused, COMMAND_TYPE, LABEL_KEY,
};
use crate::watch::{Row, WatchFilter, WatchFrame as ViewFrame, WatchKey};
use botster_core::contract::prelude::{
    CoreError, ErrorCategory, ErrorCode as CoreCode, OpResult, SessionId, Size, SpawnRequest,
    Timing,
};
use botster_hub_contract::prelude::{
    kind_spec, Category, Error, ErrorCode, ErrorEffect, ListArgs, ListResult, OpenObject, Outcome,
    Req, RequestId, Res, ServerFrame, SessionCreate, SessionCreateCommand, SessionCreateReceipt,
    SessionRef, SnapshotReason, StartingTag, UpdateArgs, Verb, WatchArgs, WatchClosedReason,
    WatchFrame, WatchOpened,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Items in a page when the client names no `limit`.
pub const DEFAULT_PAGE: usize = 100;
/// The most items in a page.
pub const MAX_PAGE: usize = 500;
/// Cursors that one connection keeps. A new listing past the bound drops the oldest cursor (its next use is
/// `expired_cursor`, which is retryable).
pub const MAX_CURSORS: usize = 8;

/// What the kinds read from the server (one source for each number).
#[derive(Debug, Clone, Copy)]
pub struct Ctx<'a> {
    pub hub_id: &'a str,
    pub types: &'a SessionTypes,
    pub max_frame_bytes: usize,
    pub cursor_idle: Duration,
    pub max_watches: usize,
    pub busy_retry_ms: u64,
    /// The working directory of a command that names none.
    pub default_cwd: &'a str,
}

/// The two kinds that this module serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Session,
    SessionType,
}

impl Kind {
    fn of(name: &str) -> Option<Kind> {
        match name {
            "session" => Some(Kind::Session),
            "session_type" => Some(Kind::SessionType),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Kind::Session => "session",
            Kind::SessionType => "session_type",
        }
    }
}

/// An equality map over the core fields of a kind (VB-1, WT-2).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Filter(BTreeMap<String, Value>);

impl Filter {
    /// A filter key must be a core field of the kind. A range filter belongs to a plugin kind (HP EN-2), so these two kinds
    /// have none.
    fn parse(kind: Kind, object: Option<OpenObject>) -> Result<Filter, String> {
        let Some(object) = object else {
            return Ok(Filter::default());
        };
        let fields = kind_spec(kind.name()).map_or(&[][..], |spec| spec.core_fields);
        let mut map = BTreeMap::new();
        for (key, value) in object {
            if !fields.contains(&key.as_str()) {
                return Err(format!("{key} is not a core field of {}", kind.name()));
            }
            map.insert(key, value);
        }
        Ok(Filter(map))
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn matches(&self, row: &Value) -> bool {
        self.0.iter().all(|(key, want)| row.get(key) == Some(want))
    }

    /// The session that the filter names, for the owner's cheaper watch filter.
    fn session(&self) -> Option<SessionId> {
        match self.0.get("id") {
            Some(Value::String(id)) => Some(SessionId(id.clone())),
            _ => None,
        }
    }

    /// A cursor is bound to its query (VB-1).
    fn query(&self, kind: Kind) -> String {
        format!(
            "{}|{}",
            kind.name(),
            Value::Object(self.0.clone().into_iter().collect())
        )
    }
}

/// What the owner's answer to a request means.
#[derive(Debug)]
pub enum Pending {
    Get,
    List {
        query: String,
        filter: Filter,
        limit: usize,
    },
    Watch {
        watch: u64,
        state: WatchState,
    },
    Unwatch {
        watch: u64,
    },
    Create {
        session: String,
    },
    Update,
    Delete,
}

/// A watch of this connection.
#[derive(Debug)]
pub struct WatchState {
    kind: Kind,
    filter: Filter,
    /// The ids that the client has, kept only when the filter is not empty (WT-2: entry and exit).
    known: HashSet<String>,
    /// After `watch_closed`: nothing more follows (WT-5).
    closed: bool,
}

/// What `io` does with a `req`.
#[derive(Debug)]
pub enum Step {
    /// Answer now.
    Reply(Res),
    /// Ask the owner. `io` reserves the reply slot, then calls [`ConnKinds::expect`] and submits.
    Owner(Request, Pending),
    /// A watch that `io` serves alone: the `res`, then the frames.
    Local(Res, Vec<ServerFrame>),
}

#[derive(Debug)]
struct Cursor {
    id: u64,
    query: String,
    items: Vec<Value>,
    next: usize,
    used: Instant,
}

/// The kind state of one control connection.
#[derive(Debug, Default)]
pub struct ConnKinds {
    pending: HashMap<u64, Pending>,
    watches: HashMap<u64, WatchState>,
    next_watch: u64,
    cursors: Vec<Cursor>,
    next_cursor: u64,
}

fn error(
    code: ErrorCode,
    category: Category,
    retryable: bool,
    effect: Option<ErrorEffect>,
    message: &str,
) -> Error {
    Error {
        code,
        category,
        retryable,
        effect,
        message: message.into(),
        reason: None,
        detail: None,
        field_errors: None,
        retry_after_ms: None,
    }
}

fn res_err(id: u64, err: Error) -> Res {
    Res {
        id: RequestId(id),
        outcome: Outcome::Err { err },
    }
}

fn res_ok(id: u64, ok: Value) -> Res {
    Res {
        id: RequestId(id),
        outcome: Outcome::Ok { ok },
    }
}

fn invalid(id: u64, message: &str) -> Res {
    res_err(
        id,
        error(
            ErrorCode::InvalidArguments,
            Category::Caller,
            false,
            Some(ErrorEffect::None),
            message,
        ),
    )
}

fn not_found(id: u64, message: &str) -> Res {
    res_err(
        id,
        error(
            ErrorCode::NotFound,
            Category::Caller,
            false,
            Some(ErrorEffect::None),
            message,
        ),
    )
}

/// An unknown kind or verb, or a verb that a kind does not declare (EN-6, VB-7).
pub fn unsupported(id: u64, message: &str) -> Res {
    res_err(
        id,
        error(
            ErrorCode::Unsupported,
            Category::Caller,
            false,
            Some(ErrorEffect::None),
            message,
        ),
    )
}

/// `too_many_requests`: the connection is at `max_outstanding` (EN-4). The connection stays open.
pub fn too_many_requests(id: u64) -> Res {
    res_err(
        id,
        error(
            ErrorCode::TooManyRequests,
            Category::Capacity,
            true,
            Some(ErrorEffect::None),
            "the connection has max_outstanding requests open",
        ),
    )
}

fn busy(ctx: &Ctx, message: &str) -> Error {
    let mut err = error(
        ErrorCode::Busy,
        Category::Capacity,
        true,
        Some(ErrorEffect::None),
        message,
    );
    err.retry_after_ms = Some(ctx.busy_retry_ms);
    err
}

fn effect(effect: Effect) -> ErrorEffect {
    match effect {
        Effect::None => ErrorEffect::None,
        Effect::Unknown => ErrorEffect::Unknown,
    }
}

/// The error of a failure that the Hub answers itself.
pub fn failure_error(ctx: &Ctx, failure: Failure) -> Error {
    let effect = Some(effect(failure.effect));
    match failure.kind {
        FailKind::HubShutdown => error(
            ErrorCode::HubShutdown,
            Category::State,
            true,
            effect,
            "the Hub is stopping",
        ),
        FailKind::Internal => error(
            ErrorCode::Internal,
            Category::Fault,
            false,
            effect,
            "the Hub failed to serve the request",
        ),
        FailKind::Busy => busy(ctx, "the Hub is busy"),
        FailKind::Unavailable => busy(ctx, "the Hub is not ready"),
        FailKind::TooManyWatches => busy(ctx, "the connection has max_watches watches open"),
    }
}

/// The error of a Core error, from its code and category (HC section 11). `effect` is what the caller knows about the
/// request: a refusal at `begin` or a read changed nothing; an error of a finished op may have.
pub fn core_error_with(error_: &CoreError, effect: ErrorEffect) -> Error {
    let effect = Some(effect);
    let (code, category, retryable, message) = match error_.code {
        CoreCode::UnknownSession
        | CoreCode::UnknownRoute
        | CoreCode::UnknownService
        | CoreCode::UnknownCapture => (
            ErrorCode::NotFound,
            Category::Caller,
            false,
            "no such resource",
        ),
        CoreCode::Cancelled => (ErrorCode::Cancelled, Category::State, false, "cancelled"),
        // The Hub mints the id, so a used id is a Hub fault.
        CoreCode::IdInUse => (
            ErrorCode::Internal,
            Category::Fault,
            false,
            "the Hub failed to serve the request",
        ),
        _ => match error_.code.category() {
            ErrorCategory::Caller => (
                ErrorCode::InvalidArguments,
                Category::Caller,
                false,
                "Core refused the arguments",
            ),
            ErrorCategory::State => (
                ErrorCode::WrongState,
                Category::State,
                false,
                "the session is in a state that forbids the request",
            ),
            ErrorCategory::Capacity => (
                ErrorCode::Busy,
                Category::Capacity,
                true,
                "Core is at a capacity limit",
            ),
            _ => (
                ErrorCode::Internal,
                Category::Fault,
                false,
                "the Hub failed to serve the request",
            ),
        },
    };
    error(code, category, retryable, effect, message)
}

/// The error of a Core refusal at `begin` or of a read: nothing changed.
pub fn core_error(error_: &CoreError) -> Error {
    core_error_with(error_, ErrorEffect::None)
}

/// The error of an op that finished with an error: a synchronous kind of error proves no change; any other may have changed
/// something (HC section 11.1).
fn op_error(error_: &CoreError) -> Error {
    let effect = if matches!(error_.code.timing(), Timing::Sync) {
        ErrorEffect::None
    } else {
        ErrorEffect::Unknown
    };
    core_error_with(error_, effect)
}

/// `idempotency_key` and `operation_id` are not served in Stage 2 (EN-8, EN-7b), so a retry cannot be made safe: the
/// mutation does not run.
fn not_served_with(req: &Req) -> Option<Res> {
    if req.idempotency_key.is_none() && req.operation_id.is_none() {
        return None;
    }
    let mut err = error(
        ErrorCode::Unsupported,
        Category::Caller,
        false,
        Some(ErrorEffect::None),
        "idempotency_key and operation_id are not served by this Hub",
    );
    err.reason = Some("idempotency_not_served".into());
    Some(res_err(req.id.0, err))
}

/// The spawn request of a command create (HC A.9). The label of the request travels in `labels` until the caller adds the
/// Hub's keys.
fn spawn_request(ctx: &Ctx, command: SessionCreateCommand) -> Result<SpawnRequest, String> {
    if command.target_id.is_some() {
        return Err("this Hub has no spawn targets".into());
    }
    if command.agent_scopes.is_some() {
        return Err("agent_scopes is not served yet".into());
    }
    let spec = command.command;
    if spec.argv.first().is_none_or(String::is_empty) {
        return Err("argv names the program".into());
    }
    let (rows, cols) = command.size.map_or((24, 80), |size| (size.rows, size.cols));
    if rows == 0 || cols == 0 || rows > u32::from(u16::MAX) || cols > u32::from(u16::MAX) {
        return Err("size is rows and cols from 1 to 65535".into());
    }
    let mut labels = BTreeMap::new();
    if let Some(label) = command.label {
        labels.insert(LABEL_KEY.to_owned(), label);
    }
    Ok(SpawnRequest {
        argv: spec.argv,
        env: spec.env.unwrap_or_default(),
        cwd: spec.cwd.unwrap_or_else(|| ctx.default_cwd.to_owned()),
        size: Size {
            rows,
            cols,
            cell_px: None,
        },
        labels,
        color_profile: None,
        notification_policy: None,
        size_policy: None,
    })
}

/// A session id: 128 random bits, hex (HC section 6). The Hub mints it and never reuses it.
fn mint_session_id() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("the system gives random bytes");
    data_encoding::HEXLOWER.encode(&bytes)
}

fn args<T: DeserializeOwned + Default>(req: &Req) -> Result<T, String> {
    match &req.args {
        None => Ok(T::default()),
        Some(value) => serde_json::from_value(value.clone()).map_err(|e| e.to_string()),
    }
}

fn session_value(ctx: &Ctx, row: &Row) -> Option<Value> {
    let built = session_row(RowFacts::of(row), ctx.hub_id, ctx.types)?;
    Some(serde_json::to_value(built).expect("a session row always encodes"))
}

fn type_values(ctx: &Ctx) -> Vec<Value> {
    ctx.types
        .list()
        .into_iter()
        .map(|row| serde_json::to_value(row).expect("a session_type row always encodes"))
        .collect()
}

/// The encoded size of a frame body, which `max_frame_bytes` bounds (EN-5).
fn encoded_len(frame: &ServerFrame) -> usize {
    serde_json::to_vec(frame).map_or(usize::MAX, |bytes| bytes.len())
}

/// How many items of `items[from..]` go in one frame, at most `limit`, so that the whole encoded frame stays within `bound`.
/// `overhead_last` is the encoded size of the frame with no items when it is the last page; `overhead_more` is the size
/// when a cursor follows. Items are comma separated, so `k` items add their sizes and `k - 1` commas.
fn take_fitting(
    items: &[Value],
    from: usize,
    limit: usize,
    bound: usize,
    overhead_last: usize,
    overhead_more: usize,
) -> usize {
    let greedy = |overhead: usize, limit: usize| {
        let mut used = overhead;
        let mut taken = 0;
        for item in items[from..].iter().take(limit) {
            let size = serde_json::to_vec(item).map_or(usize::MAX, |bytes| bytes.len());
            let add = size.saturating_add(usize::from(taken > 0));
            used = used.saturating_add(add);
            if used > bound {
                break;
            }
            taken += 1;
        }
        taken
    };
    let rest = items.len() - from;
    if rest <= limit {
        let all = greedy(overhead_last, rest);
        if all == rest {
            return all;
        }
    }
    greedy(overhead_more, limit)
}

impl ConnKinds {
    /// Whether a request with this id is open (EN-1: a duplicate outstanding id closes the connection).
    pub fn outstanding(&self, id: u64) -> bool {
        self.pending.contains_key(&id)
    }

    /// Records what the answer to request `id` means. `io` calls it after the reply slot is reserved and before `submit`.
    pub fn expect(&mut self, id: u64, pending: Pending) {
        if let Pending::Watch { watch, state } = &pending {
            let state = WatchState {
                kind: state.kind,
                filter: state.filter.clone(),
                known: HashSet::new(),
                closed: false,
            };
            self.watches.insert(*watch, state);
        }
        self.pending.insert(id, pending);
    }

    /// The step for a `req`.
    pub fn begin(
        &mut self,
        ctx: &Ctx,
        conn: u64,
        outbox: &Arc<Outbox>,
        req: &Req,
        now: Instant,
    ) -> Step {
        let id = req.id.0;
        if req.check().is_err() {
            return Step::Reply(invalid(id, "invalid action for the verb"));
        }
        let Some(kind) = Kind::of(&req.kind) else {
            return Step::Reply(unsupported(id, "this kind is not served"));
        };
        match req.verb {
            Verb::List => self.begin_list(ctx, kind, req, now),
            Verb::Get => Self::begin_get(kind, req, ctx),
            Verb::Watch => self.begin_watch(ctx, conn, outbox, kind, req),
            Verb::Unwatch => self.begin_unwatch(conn, req),
            Verb::Create | Verb::Update | Verb::Delete if kind == Kind::Session => {
                if let Some(refused) = not_served_with(req) {
                    return Step::Reply(refused);
                }
                match req.verb {
                    Verb::Create => Self::begin_create(ctx, req),
                    Verb::Update => Self::begin_update(req),
                    _ => Self::begin_delete(req),
                }
            }
            _ => Step::Reply(unsupported(id, "this verb is not served yet")),
        }
    }

    fn begin_get(kind: Kind, req: &Req, ctx: &Ctx) -> Step {
        let id = req.id.0;
        let Some(target) = req.target.as_deref() else {
            return Step::Reply(invalid(id, "get needs a target"));
        };
        match kind {
            Kind::Session => Step::Owner(Request::Get(SessionId(target.into())), Pending::Get),
            Kind::SessionType => Step::Reply(match ctx.types.get(target) {
                Some(found) => res_ok(
                    id,
                    serde_json::to_value(&found.row).expect("a session_type row always encodes"),
                ),
                None => not_found(id, "no such session type"),
            }),
        }
    }

    fn begin_list(&mut self, ctx: &Ctx, kind: Kind, req: &Req, now: Instant) -> Step {
        let id = req.id.0;
        let list: ListArgs = match args(req) {
            Ok(list) => list,
            Err(message) => return Step::Reply(invalid(id, &message)),
        };
        let limit = match list.limit {
            None => DEFAULT_PAGE,
            Some(0) => return Step::Reply(invalid(id, "limit is at least 1")),
            Some(n) => usize::try_from(n).map_or(MAX_PAGE, |n| n.min(MAX_PAGE)),
        };
        let filter = match Filter::parse(kind, list.filter) {
            Ok(filter) => filter,
            Err(message) => return Step::Reply(invalid(id, &message)),
        };
        let query = filter.query(kind);
        if let Some(cursor) = list.cursor {
            return Step::Reply(self.continue_list(ctx, id, &cursor, &query, limit, now));
        }
        match kind {
            Kind::Session => Step::Owner(
                Request::List,
                Pending::List {
                    query,
                    filter,
                    limit,
                },
            ),
            Kind::SessionType => {
                let items = type_values(ctx)
                    .into_iter()
                    .filter(|v| filter.matches(v))
                    .collect();
                Step::Reply(self.first_page(ctx, id, query, items, limit, now))
            }
        }
    }

    /// The first page of a new listing: the retained cut is `items`.
    fn first_page(
        &mut self,
        ctx: &Ctx,
        id: u64,
        query: String,
        items: Vec<Value>,
        limit: usize,
        now: Instant,
    ) -> Res {
        let taken = take_fitting(
            &items,
            0,
            limit,
            ctx.max_frame_bytes,
            list_overhead(id, None),
            list_overhead(id, Some(&cursor_name(self.next_cursor + 1))),
        );
        if taken == 0 && !items.is_empty() {
            return too_large(id);
        }
        let page: Vec<Value> = items[..taken].to_vec();
        let next_cursor = (taken < items.len()).then(|| {
            self.next_cursor += 1;
            let cursor = Cursor {
                id: self.next_cursor,
                query,
                items,
                next: taken,
                used: now,
            };
            if self.cursors.len() >= MAX_CURSORS {
                self.cursors.remove(0);
            }
            let name = cursor_name(cursor.id);
            self.cursors.push(cursor);
            name
        });
        list_res(id, page, next_cursor)
    }

    fn continue_list(
        &mut self,
        ctx: &Ctx,
        id: u64,
        cursor: &str,
        query: &str,
        limit: usize,
        now: Instant,
    ) -> Res {
        let expired = || {
            res_err(
                id,
                error(
                    ErrorCode::ExpiredCursor,
                    Category::State,
                    true,
                    Some(ErrorEffect::None),
                    "the cursor expired, or it belongs to another query: list again",
                ),
            )
        };
        let Some(n) = parse_cursor(cursor) else {
            return expired();
        };
        let Some(at) = self.cursors.iter().position(|c| c.id == n) else {
            return expired();
        };
        let held = &self.cursors[at];
        if held.query != query || now.saturating_duration_since(held.used) >= ctx.cursor_idle {
            self.cursors.remove(at);
            return expired();
        }
        let held = &mut self.cursors[at];
        let taken = take_fitting(
            &held.items,
            held.next,
            limit,
            ctx.max_frame_bytes,
            list_overhead(id, None),
            list_overhead(id, Some(&cursor_name(n))),
        );
        if taken == 0 {
            return too_large(id);
        }
        let page = held.items[held.next..held.next + taken].to_vec();
        held.next += taken;
        held.used = now;
        if held.next >= held.items.len() {
            self.cursors.remove(at);
            list_res(id, page, None)
        } else {
            list_res(id, page, Some(cursor_name(n)))
        }
    }

    fn begin_watch(
        &mut self,
        ctx: &Ctx,
        conn: u64,
        outbox: &Arc<Outbox>,
        kind: Kind,
        req: &Req,
    ) -> Step {
        let id = req.id.0;
        let watch: WatchArgs = match args(req) {
            Ok(watch) => watch,
            Err(message) => return Step::Reply(invalid(id, &message)),
        };
        let filter = match Filter::parse(kind, watch.filter) {
            Ok(filter) => filter,
            Err(message) => return Step::Reply(invalid(id, &message)),
        };
        // A session watch is in the table from `expect`, which comes before the next request, so each counts once.
        if self.watches.len() >= ctx.max_watches {
            return Step::Reply(res_err(
                id,
                busy(ctx, "the connection has max_watches open"),
            ));
        }
        self.next_watch += 1;
        let number = self.next_watch;
        let state = WatchState {
            kind,
            filter,
            known: HashSet::new(),
            closed: false,
        };
        match kind {
            Kind::Session => {
                let owner_filter = WatchFilter {
                    session: state.filter.session(),
                };
                Step::Owner(
                    Request::Watch {
                        key: WatchKey {
                            conn,
                            watch: number,
                        },
                        filter: owner_filter,
                        outbox: Arc::clone(outbox),
                    },
                    Pending::Watch {
                        watch: number,
                        state,
                    },
                )
            }
            Kind::SessionType => {
                // The registry does not change while the Hub runs, so the baseline is the whole watch (WT-1).
                let watch_id = number.to_string();
                let items: Vec<Value> = type_values(ctx)
                    .into_iter()
                    .filter(|v| state.filter.matches(v))
                    .collect();
                let mut frames = vec![ServerFrame::Watch(WatchFrame::SnapshotBegin {
                    watch_id: watch_id.clone(),
                    reason: None,
                })];
                match snapshot_items(&watch_id, items, ctx.max_frame_bytes) {
                    Ok(chunks) => frames.extend(chunks),
                    Err(()) => {
                        return Step::Reply(res_err(
                            id,
                            error(
                                ErrorCode::ResponseTooLarge,
                                Category::Capacity,
                                false,
                                Some(ErrorEffect::None),
                                "a session type is larger than max_frame_bytes",
                            ),
                        ))
                    }
                }
                frames.push(ServerFrame::Watch(WatchFrame::SnapshotEnd {
                    watch_id: watch_id.clone(),
                }));
                self.watches.insert(number, state);
                Step::Local(watch_opened(id, number), frames)
            }
        }
    }

    fn begin_create(ctx: &Ctx, req: &Req) -> Step {
        let id = req.id.0;
        let Some(value) = req.args.clone() else {
            return Step::Reply(invalid(id, "create needs args"));
        };
        let create: SessionCreate = match serde_json::from_value(value) {
            Ok(create) => create,
            Err(_) => {
                return Step::Reply(invalid(
                    id,
                    "args are neither { command, .. } nor { session_type_id, .. }",
                ))
            }
        };
        let command = match create {
            SessionCreate::Command(command) => command,
            SessionCreate::Type(by_type) => {
                // The registry check comes before `Spawn` (design section 7a).
                return Step::Reply(match ctx.types.for_create(&by_type.session_type_id) {
                    Err(TypeRefused::Unknown) => invalid(id, "no such session type"),
                    Err(TypeRefused::NotByIdentifier) => {
                        invalid(id, "this session type is created with a command")
                    }
                    // A type with a definition to run comes with the plugin kinds.
                    Ok(_) => unsupported(id, "creating by session type is not served yet"),
                });
            }
        };
        let mut request = match spawn_request(ctx, command) {
            Ok(request) => request,
            Err(message) => return Step::Reply(invalid(id, &message)),
        };
        let session = mint_session_id();
        request.labels = create_labels(
            &SessionId(session.clone()),
            request.labels.get(LABEL_KEY).map(String::as_str),
            COMMAND_TYPE,
        );
        Step::Owner(
            Request::Spawn {
                session: SessionId(session.clone()),
                request: Box::new(request),
            },
            Pending::Create { session },
        )
    }

    fn begin_update(req: &Req) -> Step {
        let id = req.id.0;
        let Some(target) = req.target.as_deref() else {
            return Step::Reply(invalid(id, "update needs a target"));
        };
        let update: UpdateArgs = match req
            .args
            .clone()
            .ok_or_else(|| "update needs args".to_owned())
            .and_then(|v| serde_json::from_value(v).map_err(|e| e.to_string()))
        {
            Ok(update) => update,
            Err(message) => return Step::Reply(invalid(id, &message)),
        };
        if update.if_rev.is_some() {
            return Step::Reply(unsupported(
                id,
                "if_rev is not served: the row has no rev yet",
            ));
        }
        let (mut set, mut remove) = (BTreeMap::new(), Vec::new());
        for (key, value) in update.patch {
            match (key.as_str(), value) {
                ("label", Value::String(label)) => {
                    set.insert(LABEL_KEY.to_owned(), label);
                }
                // RFC 7396: `null` deletes the member, and the label falls back to the session id.
                ("label", Value::Null) => remove.push(LABEL_KEY.to_owned()),
                ("label", _) => return Step::Reply(invalid(id, "label is a string or null")),
                ("task", _) => return Step::Reply(unsupported(id, "task is not served yet")),
                ("id" | "hub_id" | "kind", _) => {
                    return Step::Reply(invalid(id, "identity fields are immutable"))
                }
                _ => return Step::Reply(invalid(id, "the patch holds label and task only")),
            }
        }
        Step::Owner(
            Request::SetLabels {
                session: SessionId(target.into()),
                set,
                remove,
            },
            Pending::Update,
        )
    }

    fn begin_delete(req: &Req) -> Step {
        let id = req.id.0;
        let Some(target) = req.target.as_deref() else {
            return Step::Reply(invalid(id, "delete needs a target"));
        };
        Step::Owner(Request::Delete(SessionId(target.into())), Pending::Delete)
    }

    fn begin_unwatch(&mut self, conn: u64, req: &Req) -> Step {
        let id = req.id.0;
        let Some(number) = req.target.as_deref().and_then(|t| t.parse::<u64>().ok()) else {
            return Step::Reply(invalid(id, "unwatch needs the watch_id as its target"));
        };
        match self.watches.get(&number).map(|w| w.kind) {
            None => Step::Reply(not_found(id, "no such watch")),
            // A local watch has nothing at the owner.
            Some(Kind::SessionType) => {
                self.watches.remove(&number);
                Step::Reply(res_ok(id, json!({})))
            }
            Some(Kind::Session) => Step::Owner(
                Request::Unwatch(WatchKey {
                    conn,
                    watch: number,
                }),
                Pending::Unwatch { watch: number },
            ),
        }
    }

    /// The frames for the owner's answer to request `id`: the `res`, and the first frame of a new baseline.
    pub fn answer(&mut self, ctx: &Ctx, id: u64, answer: Answer) -> Vec<ServerFrame> {
        let frames = self.answer_unbounded(ctx, id, answer);
        // The Hub never sends a frame over `max_frame_bytes` (EN-5): the answer becomes `response_too_large`.
        frames
            .into_iter()
            .map(|frame| match frame {
                ServerFrame::Res(res)
                    if encoded_len(&ServerFrame::Res(res.clone())) > ctx.max_frame_bytes =>
                {
                    ServerFrame::Res(too_large(res.id.0))
                }
                other => other,
            })
            .collect()
    }

    fn answer_unbounded(&mut self, ctx: &Ctx, id: u64, answer: Answer) -> Vec<ServerFrame> {
        let Some(pending) = self.pending.remove(&id) else {
            return Vec::new();
        };
        if let Pending::Watch { watch, .. } = &pending {
            if !matches!(answer, Answer::Watching) {
                self.watches.remove(watch);
            }
        }
        let refusal = match &answer {
            Answer::Failed(failure) => Some(failure_error(ctx, *failure)),
            Answer::CoreRefused(refused) => Some(core_error(refused)),
            Answer::Op(OpResult::Err(refused)) => Some(op_error(refused)),
            Answer::Spawn(SpawnAnswer::Refused { error, effect: at }) => {
                Some(core_error_with(error, self::effect(*at)))
            }
            _ => None,
        };
        if let Some(err) = refusal {
            return vec![ServerFrame::Res(res_err(id, err))];
        }
        match (pending, answer) {
            (Pending::Get, Answer::Row(row)) => {
                vec![ServerFrame::Res(match session_value(ctx, &row) {
                    Some(value) => res_ok(id, value),
                    None => not_found(id, "no such session"),
                })]
            }
            (
                Pending::List {
                    query,
                    filter,
                    limit,
                },
                Answer::Rows(rows),
            ) => {
                let items: Vec<Value> = rows
                    .iter()
                    .filter_map(|row| session_value(ctx, row))
                    .filter(|value| filter.matches(value))
                    .collect();
                vec![ServerFrame::Res(self.first_page(
                    ctx,
                    id,
                    query,
                    items,
                    limit,
                    Instant::now(),
                ))]
            }
            (Pending::Watch { watch, .. }, Answer::Watching) => vec![
                ServerFrame::Res(watch_opened(id, watch)),
                ServerFrame::Watch(WatchFrame::SnapshotBegin {
                    watch_id: watch.to_string(),
                    reason: None,
                }),
            ],
            (
                Pending::Create { session },
                Answer::Spawn(SpawnAnswer::Accepted { operation_id }),
            ) => {
                let receipt = SessionCreateReceipt {
                    operation_id,
                    session: SessionRef {
                        hub_id: ctx.hub_id.to_owned(),
                        session_id: session,
                    },
                    state: StartingTag::Starting,
                };
                vec![ServerFrame::Res(res_ok(
                    id,
                    serde_json::to_value(receipt).expect("a receipt always encodes"),
                ))]
            }
            (Pending::Update, Answer::Row(row)) => {
                vec![ServerFrame::Res(match session_value(ctx, &row) {
                    Some(value) => res_ok(id, value),
                    None => not_found(id, "no such session"),
                })]
            }
            (Pending::Delete, Answer::Op(OpResult::Ok(_))) => {
                vec![ServerFrame::Res(res_ok(id, json!({})))]
            }
            (Pending::Unwatch { watch }, Answer::Unwatched) => {
                self.watches.remove(&watch);
                vec![ServerFrame::Res(res_ok(id, json!({})))]
            }
            // An answer of another shape is a Hub fault.
            _ => vec![ServerFrame::Res(res_err(
                id,
                failure_error(
                    ctx,
                    Failure {
                        kind: FailKind::Internal,
                        effect: Effect::None,
                    },
                ),
            ))],
        }
    }

    /// The frames for one frame of the watch `number`. A watch that is gone or closed gets none (WT-5, WT-6).
    pub fn frame(&mut self, ctx: &Ctx, number: u64, frame: ViewFrame) -> Vec<ServerFrame> {
        let frames = self.frame_unbounded(ctx, number, frame);
        // A frame over `max_frame_bytes` is never sent (EN-5, BD-2): the watch ends with `watch_closed { item_too_large }`.
        let mut out = Vec::with_capacity(frames.len());
        for frame in frames {
            if encoded_len(&frame) > ctx.max_frame_bytes {
                if let Some(state) = self.watches.get_mut(&number) {
                    state.closed = true;
                }
                out.push(ServerFrame::Watch(WatchFrame::WatchClosed {
                    watch_id: number.to_string(),
                    reason: WatchClosedReason::ItemTooLarge,
                }));
                break;
            }
            out.push(frame);
        }
        out
    }

    fn frame_unbounded(&mut self, ctx: &Ctx, number: u64, frame: ViewFrame) -> Vec<ServerFrame> {
        let Some(state) = self.watches.get_mut(&number) else {
            return Vec::new();
        };
        if state.closed {
            return Vec::new();
        }
        let watch_id = number.to_string();
        let mut out = Vec::new();
        match frame {
            ViewFrame::Resync => {
                state.known.clear();
                out.push(ServerFrame::Watch(WatchFrame::SnapshotBegin {
                    watch_id,
                    reason: Some(SnapshotReason::Overflow),
                }));
            }
            ViewFrame::SnapshotChunk(rows) => {
                let items: Vec<Value> = rows
                    .iter()
                    .filter_map(|row| session_value(ctx, row))
                    .filter(|value| state.filter.matches(value))
                    .collect();
                if !state.filter.is_empty() {
                    state.known.extend(items.iter().filter_map(id_of));
                }
                match snapshot_items(&watch_id, items, ctx.max_frame_bytes) {
                    Ok(chunks) => out.extend(chunks),
                    Err(()) => {
                        state.closed = true;
                        out.push(ServerFrame::Watch(WatchFrame::WatchClosed {
                            watch_id,
                            reason: WatchClosedReason::ItemTooLarge,
                        }));
                    }
                }
            }
            ViewFrame::SnapshotEnd => {
                out.push(ServerFrame::Watch(WatchFrame::SnapshotEnd { watch_id }));
            }
            ViewFrame::Delta(row) => {
                let Some(value) = session_value(ctx, &row) else {
                    return out;
                };
                let Some(id) = id_of(&value) else {
                    return out;
                };
                if state.filter.matches(&value) {
                    if !state.filter.is_empty() {
                        state.known.insert(id.clone());
                    }
                    out.push(ServerFrame::Watch(WatchFrame::Upsert {
                        watch_id,
                        id,
                        value,
                    }));
                } else if state.known.remove(&id) {
                    out.push(ServerFrame::Watch(WatchFrame::Remove { watch_id, id }));
                }
            }
            ViewFrame::Removed(session) => {
                let shown = state.filter.is_empty() || state.known.remove(&session.0);
                if shown {
                    out.push(ServerFrame::Watch(WatchFrame::Remove {
                        watch_id,
                        id: session.0,
                    }));
                }
            }
        }
        out
    }

    /// Watches of this connection (for tests and the bound).
    pub fn watches(&self) -> usize {
        self.watches.len()
    }
}

fn id_of(value: &Value) -> Option<String> {
    value.get("id")?.as_str().map(str::to_owned)
}

fn cursor_name(n: u64) -> String {
    format!("c{n}")
}

fn parse_cursor(name: &str) -> Option<u64> {
    name.strip_prefix('c')?.parse().ok()
}

fn watch_opened(id: u64, number: u64) -> Res {
    res_ok(
        id,
        serde_json::to_value(WatchOpened {
            watch_id: Some(number.to_string()),
            stream_id: None,
        })
        .expect("a watch_opened always encodes"),
    )
}

fn list_res(id: u64, items: Vec<Value>, next_cursor: Option<String>) -> Res {
    res_ok(
        id,
        serde_json::to_value(ListResult { items, next_cursor })
            .expect("a list result always encodes"),
    )
}

fn too_large(id: u64) -> Res {
    res_err(
        id,
        error(
            ErrorCode::ResponseTooLarge,
            Category::Capacity,
            false,
            Some(ErrorEffect::None),
            "one item is larger than max_frame_bytes",
        ),
    )
}

/// The `snapshot_items` frames for `items`, each within the bound (WT-1). `Err` when one item alone does not fit
/// (BD-2: `watch_closed { item_too_large }`).
fn snapshot_items(watch_id: &str, items: Vec<Value>, bound: usize) -> Result<Vec<ServerFrame>, ()> {
    let overhead = encoded_len(&ServerFrame::Watch(WatchFrame::SnapshotItems {
        watch_id: watch_id.to_owned(),
        items: Vec::new(),
    }));
    let mut frames = Vec::new();
    let mut from = 0;
    while from < items.len() {
        let taken = take_fitting(&items, from, usize::MAX, bound, overhead, overhead);
        if taken == 0 {
            return Err(());
        }
        frames.push(ServerFrame::Watch(WatchFrame::SnapshotItems {
            watch_id: watch_id.to_owned(),
            items: items[from..from + taken].to_vec(),
        }));
        from += taken;
    }
    Ok(frames)
}

/// The encoded size of a `list` answer with no items, for the cursor it will carry (or none).
fn list_overhead(id: u64, cursor: Option<&str>) -> usize {
    encoded_len(&ServerFrame::Res(list_res(
        id,
        Vec::new(),
        cursor.map(str::to_owned),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn types() -> SessionTypes {
        SessionTypes::device()
    }

    fn ctx(types: &SessionTypes) -> Ctx<'_> {
        Ctx {
            hub_id: "h",
            types,
            max_frame_bytes: 4096,
            cursor_idle: Duration::from_secs(60),
            max_watches: 2,
            busy_retry_ms: 50,
            default_cwd: "/home/u",
        }
    }

    fn req(value: Value) -> Req {
        serde_json::from_value(value).unwrap()
    }

    fn items(n: usize) -> Vec<Value> {
        (0..n)
            .map(|i| json!({"id": format!("s{i:03}"), "state": "running"}))
            .collect()
    }

    #[test]
    fn a_filter_names_core_fields_only() {
        let object = |v: Value| v.as_object().cloned();
        assert!(Filter::parse(Kind::Session, object(json!({"state": "running"}))).is_ok());
        let refused = Filter::parse(Kind::Session, object(json!({"nope": 1})));
        assert!(refused.unwrap_err().contains("nope"));
        let filter = Filter::parse(
            Kind::Session,
            object(json!({"id": "a", "state": "running"})),
        )
        .unwrap();
        assert!(filter.matches(&json!({"id": "a", "state": "running", "x": 1})));
        assert!(!filter.matches(&json!({"id": "a", "state": "ended"})));
        assert_eq!(filter.session(), Some(SessionId("a".into())));
    }

    /// KINDS-4: the bound is on the whole encoded `list` answer. A page that fits exactly is sent; one byte less takes
    /// fewer items, or none (`response_too_large`).
    #[test]
    fn a_page_is_bound_by_the_whole_encoded_answer() {
        let types = types();
        let mut ctx = ctx(&types);
        let all = items(3);
        // The last page: all three items, no cursor.
        let last = encoded_len(&ServerFrame::Res(list_res(7, all.clone(), None)));
        ctx.max_frame_bytes = last;
        let mut kinds = ConnKinds::default();
        let res = kinds.first_page(&ctx, 7, "q".into(), all.clone(), 10, Instant::now());
        let Outcome::Ok { ok } = res.outcome else {
            panic!("{res:?}")
        };
        assert_eq!(ok["items"].as_array().unwrap().len(), 3);
        assert!(ok.get("next_cursor").is_none());
        // One byte less: two items and a cursor, whose frame is no bigger than the bound.
        ctx.max_frame_bytes = last - 1;
        let mut kinds = ConnKinds::default();
        let res = kinds.first_page(&ctx, 7, "q".into(), all.clone(), 10, Instant::now());
        let Outcome::Ok { ok } = res.outcome.clone() else {
            panic!("{res:?}")
        };
        assert_eq!(ok["items"].as_array().unwrap().len(), 2);
        assert!(encoded_len(&ServerFrame::Res(res)) < last);
        // A page with a cursor: the cursor name is part of the answer.
        let with_cursor = encoded_len(&ServerFrame::Res(list_res(
            7,
            all[..1].to_vec(),
            Some("c1".into()),
        )));
        ctx.max_frame_bytes = with_cursor;
        let mut kinds = ConnKinds::default();
        let res = kinds.first_page(&ctx, 7, "q".into(), all.clone(), 1, Instant::now());
        let Outcome::Ok { ok } = res.outcome else {
            panic!("{res:?}")
        };
        assert_eq!(ok["items"].as_array().unwrap().len(), 1);
        ctx.max_frame_bytes = with_cursor - 1;
        let mut kinds = ConnKinds::default();
        let res = kinds.first_page(&ctx, 7, "q".into(), all.clone(), 1, Instant::now());
        assert!(
            matches!(&res.outcome, Outcome::Err { err } if err.code == ErrorCode::ResponseTooLarge)
        );
    }

    #[test]
    fn a_page_stops_at_the_limit() {
        let all = items(10);
        assert_eq!(take_fitting(&all, 0, 3, 4096, 20, 40), 3);
        assert_eq!(take_fitting(&all, 8, 5, 4096, 20, 40), 2);
    }

    #[test]
    fn a_listing_is_one_cut_in_pages_and_a_cursor_is_bound_to_its_query() {
        let types = types();
        let ctx = ctx(&types);
        let mut kinds = ConnKinds::default();
        let now = Instant::now();
        let first = kinds.first_page(&ctx, 1, "q".into(), items(5), 2, now);
        let Outcome::Ok { ok } = first.outcome else {
            panic!()
        };
        assert_eq!(ok["items"].as_array().unwrap().len(), 2);
        let cursor = ok["next_cursor"].as_str().unwrap().to_owned();
        // Another query: expired.
        let other = kinds.continue_list(&ctx, 2, &cursor, "other", 2, now);
        assert!(
            matches!(&other.outcome, Outcome::Err { err } if err.code == ErrorCode::ExpiredCursor)
        );
        // The cursor was dropped by the mismatch: the same query is expired now too.
        let again = kinds.continue_list(&ctx, 3, &cursor, "q", 2, now);
        assert!(
            matches!(&again.outcome, Outcome::Err { err } if err.code == ErrorCode::ExpiredCursor)
        );
        // A fresh listing pages to its end with no row twice or skipped.
        let Outcome::Ok { ok } = kinds
            .first_page(&ctx, 4, "q".into(), items(5), 2, now)
            .outcome
        else {
            panic!()
        };
        let mut seen: Vec<String> = ok["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| id_of(v).unwrap())
            .collect();
        let mut cursor = ok["next_cursor"].as_str().map(str::to_owned);
        while let Some(name) = cursor {
            let Outcome::Ok { ok } = kinds.continue_list(&ctx, 5, &name, "q", 2, now).outcome
            else {
                panic!()
            };
            seen.extend(
                ok["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| id_of(v).unwrap()),
            );
            cursor = ok["next_cursor"].as_str().map(str::to_owned);
        }
        assert_eq!(seen, ["s000", "s001", "s002", "s003", "s004"]);
        assert!(kinds.cursors.is_empty(), "the last page drops the cursor");
    }

    #[test]
    fn an_idle_cursor_expires_and_the_cursors_are_bounded() {
        let types = types();
        let mut ctx = ctx(&types);
        ctx.cursor_idle = Duration::from_secs(5);
        let mut kinds = ConnKinds::default();
        let now = Instant::now();
        let Outcome::Ok { ok } = kinds
            .first_page(&ctx, 1, "q".into(), items(4), 1, now)
            .outcome
        else {
            panic!()
        };
        let cursor = ok["next_cursor"].as_str().unwrap().to_owned();
        let late = now + Duration::from_secs(5);
        let expired = kinds.continue_list(&ctx, 2, &cursor, "q", 1, late);
        assert!(
            matches!(&expired.outcome, Outcome::Err { err } if err.code == ErrorCode::ExpiredCursor)
        );
        for n in 0..(MAX_CURSORS + 3) {
            kinds.first_page(&ctx, n as u64, "q".into(), items(4), 1, now);
        }
        assert_eq!(kinds.cursors.len(), MAX_CURSORS);
    }

    /// BD-2: one item over the bound is `response_too_large`; the snapshot frames are bound by their whole size, exactly.
    #[test]
    fn an_item_over_the_bound_is_response_too_large() {
        let types = types();
        let mut ctx = ctx(&types);
        ctx.max_frame_bytes = 40;
        let mut kinds = ConnKinds::default();
        let res = kinds.first_page(&ctx, 1, "q".into(), items(1), 10, Instant::now());
        assert!(
            matches!(&res.outcome, Outcome::Err { err } if err.code == ErrorCode::ResponseTooLarge)
        );
        assert!(snapshot_items("1", items(1), 40).is_err());
        let one = encoded_len(&ServerFrame::Watch(WatchFrame::SnapshotItems {
            watch_id: "1".into(),
            items: items(1),
        }));
        assert_eq!(snapshot_items("1", items(1), one).unwrap().len(), 1);
        assert!(snapshot_items("1", items(1), one - 1).is_err());
        let frames = snapshot_items("1", items(40), 600).unwrap();
        assert!(frames.len() > 1);
        assert!(frames.iter().all(|f| encoded_len(f) <= 600));
    }

    #[test]
    fn requests_that_the_kinds_do_not_serve_are_unsupported_or_invalid() {
        let types = types();
        let ctx = ctx(&types);
        let mut kinds = ConnKinds::default();
        let outbox = Outbox::new(
            crate::bridge::OutboxCaps {
                replies: 4,
                frames: 4,
                watch_queue: 4,
            },
            crate::phase::SharedPhase::new(),
            Arc::new(crate::phase::tests::Count::default()),
            Arc::new(crate::phase::tests::Count::default()),
        );
        let now = Instant::now();
        let mut step = |value: Value| kinds.begin(&ctx, 1, &outbox, &req(value), now);
        let code = |step: Step| match step {
            Step::Reply(Res {
                outcome: Outcome::Err { err },
                ..
            }) => err.code,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            code(step(json!({"id": "1", "verb": "list", "kind": "nope"}))),
            ErrorCode::Unsupported
        );
        assert_eq!(
            code(step(
                json!({"id": "2", "verb": "frobnicate", "kind": "session"})
            )),
            ErrorCode::Unsupported
        );
        assert_eq!(
            code(step(json!({"id": "3", "verb": "get", "kind": "session"}))),
            ErrorCode::InvalidArguments
        );
        assert_eq!(
            code(step(
                json!({"id": "4", "verb": "list", "kind": "session", "action": "x"})
            )),
            ErrorCode::InvalidArguments
        );
        assert_eq!(
            code(step(
                json!({"id": "5", "verb": "list", "kind": "session", "args": {"limit": 0}})
            )),
            ErrorCode::InvalidArguments
        );
        assert_eq!(
            code(step(
                json!({"id": "6", "verb": "list", "kind": "session", "args": {"filter": {"nope": 1}}})
            )),
            ErrorCode::InvalidArguments
        );
        assert_eq!(
            code(step(
                json!({"id": "7", "verb": "unwatch", "kind": "session", "target": "9"})
            )),
            ErrorCode::NotFound
        );
        assert_eq!(
            code(step(
                json!({"id": "8", "verb": "get", "kind": "session_type", "target": "nope"})
            )),
            ErrorCode::NotFound
        );
    }
}
