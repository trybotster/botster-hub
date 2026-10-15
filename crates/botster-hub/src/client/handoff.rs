//! The route handoff on the owner thread (design sections 2.2 to 2.5, revision 9): reserve, connect, and the Core route
//! events. Each function takes the Core and the tables, so the owner calls it in one step and the tests run it on the
//! contracts' fake Core.
//!
//! - [`reserve`]: `invoke(session, attach)` (TS-1). It checks the session and the format, mints `route_id` and
//!   `route_token`, and fixes the R-119 route limits.
//! - [`connect`]: the `route_connect` step (TB-L2). The consume point and Core `attach` run in one step.
//! - [`route_event`]: Core `RouteClosed` is the only removal from the route table; `RouteAdopted` rebuilds it after a
//!   Hub restart (DP-8).

use crate::client::formats::{common_formats, CommonFormats};
use crate::client::route::{Connect, Entropy, NotConnected, OsEntropy, ReserveError, RouteTables};
use botster_core::contract::prelude::{
    AttachOptions, ClientId, CoreApi, CoreError, CoreLimits, ErrorCode, Event, HistoryMode,
    RouteId, RouteLimitChoices, RouteTransport, SessionId, StreamEndpoint, UnsupportedReason,
};
use botster_hub_contract::prelude::{CloseReason, RouteLimits};
use botster_route_codec::prelude::RouteEndReason;
use std::collections::VecDeque;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The caller of a `local-stream` connection: `operator`, with the peer uid (HC HS-7). Its tag is the Core `owner` and
/// `client` of every route it attaches, so `RouteAdopted` gives the caller back after a Hub restart (A.3).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Caller {
    pub uid: u32,
}

const OPERATOR_TAG: &str = "operator/";

impl Caller {
    pub fn tag(&self) -> String {
        format!("{OPERATOR_TAG}{}", self.uid)
    }

    /// The caller of a tag that [`Caller::tag`] made, or `None` for any other tag.
    pub fn from_tag(tag: &str) -> Option<Caller> {
        let uid = tag.strip_prefix(OPERATOR_TAG)?;
        // `u32::from_str` takes a leading `+`; a tag this Hub made never has one, so only the canonical form matches.
        if uid.starts_with('+') {
            return None;
        }
        let caller = Caller {
            uid: uid.parse().ok()?,
        };
        (caller.tag() == tag).then_some(caller)
    }
}

/// What the owner fixed at reservation, for the `attach` of the connect step.
#[derive(Debug, Clone, PartialEq)]
pub struct AttachPlan {
    pub session: SessionId,
    pub options: AttachOptions,
}

/// One route-table entry (design 2.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteEntry {
    pub core_route: RouteId,
    pub session: SessionId,
    pub owner: Caller,
}

/// The owner's route tables.
pub type HubRouteTables = RouteTables<Caller, AttachPlan, RouteEntry>;

/// The Hub's route parameters (HP LM-5 rule: small test values are allowed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteParams {
    /// The reservation bound (Reserved and Consumed entries).
    pub max_reservations: usize,
    /// `route_connect_seconds` (TS-2).
    pub connect_window: Duration,
    /// The Core `query_deadline` of a route that answers queries; `welcome.limits.query_client_deadline_ms`.
    pub query_deadline: Duration,
    /// `<data root>/sessions` (HC RT-2). A session's file-input directory is `<sessions_root>/<session_id>/uploads`.
    pub sessions_root: PathBuf,
    /// The `route.ended` events that wait for the `routes` stream. More are counted as lost, for its `event_gap` (EV-4).
    pub pending_ends: usize,
}

/// The route parameters that come from the Hub's configuration (HC CF-2 `[limits]` and `[routes]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteConfig {
    pub max_reservations: usize,
    pub connect_window: Duration,
    pub query_deadline: Duration,
    pub pending_ends: usize,
}

impl RouteParams {
    /// The configuration, and the data root's `sessions` directory (HC RT-2).
    pub fn new(config: &RouteConfig, sessions_root: PathBuf) -> RouteParams {
        RouteParams {
            max_reservations: config.max_reservations,
            connect_window: config.connect_window,
            query_deadline: config.query_deadline,
            sessions_root,
            pending_ends: config.pending_ends,
        }
    }

    /// The TS-5f file directory of `session`, which the Hub passes to Core as `file_directory` (HC RT-2).
    pub fn file_directory(&self, session: &SessionId) -> PathBuf {
        self.sessions_root.join(&session.0).join("uploads")
    }
}

impl HubRouteTables {
    pub fn with_params(params: &RouteParams) -> HubRouteTables {
        RouteTables::new(params.max_reservations, params.connect_window)
    }
}

/// The `attach` arguments of a session `invoke` (TS-1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReserveRequest {
    pub caller: Caller,
    pub session: SessionId,
    /// `hello.terminal_formats`, in preference order.
    pub terminal_formats: Vec<String>,
    pub history: Option<HistoryMode>,
    pub answers_queries: bool,
    pub route_features: Vec<String>,
}

/// The `attach` answer, without the transport's `socket_path`, which `io` adds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reservation {
    pub route_id: String,
    pub route_token: String,
    pub route_limits: RouteLimits,
}

/// Why `attach` reserved nothing.
#[derive(Debug, Clone, PartialEq)]
pub enum ReserveRefused {
    /// `UnknownSession`, or `Unsupported { terminal_format }` when no format is common (OU-1).
    Core(CoreError),
    /// The reservation table is full (retryable).
    Busy,
    /// The CSPRNG failed, or a minted id collided: a Hub fault.
    Internal(String),
}

/// The client's first format that the session can emit (OU-1), named as Core names it.
fn choose_format<C: CoreApi>(
    core: &C,
    session: &SessionId,
    wanted: &[String],
) -> Result<String, CoreError> {
    let emits: Vec<String> = core
        .snapshot_formats(session)?
        .iter()
        .map(botster_core_link::route::terminal_format)
        .collect();
    wanted
        .iter()
        .find(|name| emits.contains(name))
        .cloned()
        .ok_or_else(|| {
            CoreError::new(
                ErrorCode::Unsupported {
                    what: Some(UnsupportedReason::TerminalFormat),
                },
                "no common terminal format",
            )
        })
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The three explicit choices of steward R-119, read from Core's limits.
fn limit_choices(limits: &CoreLimits) -> RouteLimitChoices {
    RouteLimitChoices {
        max_frame_bytes: Some(limits.effective_max_route_frame_bytes()),
        max_screen_frame_bytes: Some(limits.max_snapshot_bytes.saturating_add(1)),
        max_history_page_bytes: Some(limits.max_history_page_bytes),
    }
}

/// The `route_limits` of the answer (R-119): the three choices as passed to `attach`, and every other TS-1 field from
/// Core's `limits()`. `attached.limits` governs if a worker differs (A.4).
fn route_limits(
    choices: &RouteLimitChoices,
    limits: &CoreLimits,
    params: &RouteParams,
) -> RouteLimits {
    RouteLimits {
        max_frame_bytes: choices.max_frame_bytes.unwrap_or_default(),
        max_screen_frame_bytes: choices.max_screen_frame_bytes.unwrap_or_default(),
        max_history_page_bytes: choices.max_history_page_bytes.unwrap_or_default(),
        max_paste_bytes: limits.max_paste_bytes,
        max_file_bytes: limits.max_file_bytes,
        max_query_reply_bytes: limits.max_query_reply_bytes,
        query_client_deadline_ms: millis(params.query_deadline),
        stall_deadline_ms: millis(limits.reader_progress_deadline),
        stall_close_after_ms: millis(limits.stall_close_after),
    }
}

/// `invoke(session, attach)` (design 2.2).
pub fn reserve<C: CoreApi>(
    core: &C,
    tables: &mut HubRouteTables,
    params: &RouteParams,
    now: Instant,
    request: ReserveRequest,
    entropy: &mut impl Entropy,
) -> Result<Reservation, ReserveRefused> {
    let format = choose_format(core, &request.session, &request.terminal_formats)
        .map_err(ReserveRefused::Core)?;
    let limits = core.limits();
    let choices = limit_choices(&limits);
    let route_limits = route_limits(&choices, &limits, params);
    let options = AttachOptions {
        file_directory: params
            .file_directory(&request.session)
            .to_string_lossy()
            .into_owned(),
        // Core's default: readable and writable by the session's user only.
        file_permissions: None,
        route_features: request.route_features,
        terminal_formats: vec![format],
        owner: Some(request.caller.tag()),
        query_deadline: request.answers_queries.then_some(params.query_deadline),
        // Set at the connect step: the tag is the minted route id.
        route_tag: None,
        route_limits: Some(choices),
        history: request.history,
        stall_deadline: None,
        answers_queries: request.answers_queries,
        input: true,
    };
    let plan = AttachPlan {
        session: request.session,
        options,
    };
    let uid = request.caller.uid;
    match tables.reserve(now, request.caller, uid, plan, entropy) {
        Ok(minted) => Ok(Reservation {
            route_id: minted.route_id,
            route_token: minted.route_token,
            route_limits,
        }),
        Err(ReserveError::Busy) => Err(ReserveRefused::Busy),
        Err(ReserveError::Entropy(error)) => Err(ReserveRefused::Internal(error.to_string())),
        Err(ReserveError::Collision) => Err(ReserveRefused::Internal("route id collision".into())),
    }
}

/// The client's stream while it travels back to `io` in an answer. `Clone` is an `Arc` clone and equality is identity,
/// so the bridge's `Answer` keeps its derives. `io` writes the close through `&UnixStream`; the descriptor closes when the
/// last clone drops.
#[derive(Debug, Clone)]
pub struct ReturnedStream(pub Arc<UnixStream>);

impl PartialEq for ReturnedStream {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// A `route_connect` that `io` read (design 2.3, item 1).
#[derive(Debug)]
pub struct RouteConnectRequest {
    pub route_id: String,
    pub route_token: String,
    pub caller: Caller,
    /// The client's connection, converted from `mio` to std: Core takes a std `UnixStream` (Core `real.rs`).
    pub stream: UnixStream,
}

/// The owner's answer to a `route_connect`.
#[derive(Debug, Clone, PartialEq)]
pub enum RouteConnectAnswer {
    /// `io` writes `close { reason }` and closes the stream.
    Refused {
        reason: CloseReason,
        stream: ReturnedStream,
    },
    /// The stream is Core's, or it was closed after a sync `attach` refusal: `io` does nothing.
    Handed,
}

/// One `route.ended` event to the route's caller (TS-9), on the `routes` stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteEnded {
    pub route_id: String,
    pub caller: Caller,
    pub reason: RouteEndReason,
}

impl From<NotConnected<Caller>> for RouteEnded {
    fn from(ended: NotConnected<Caller>) -> RouteEnded {
        RouteEnded {
            route_id: ended.route_id,
            caller: ended.caller,
            reason: RouteEndReason::NotConnected,
        }
    }
}

/// The route-connect step (design 2.3, item 3). It returns the answer and the at most one `route.ended` that this step
/// caused.
pub fn connect<C: CoreApi>(
    core: &mut C,
    tables: &mut HubRouteTables,
    now: Instant,
    request: RouteConnectRequest,
) -> (RouteConnectAnswer, Option<RouteEnded>) {
    let RouteConnectRequest {
        route_id,
        route_token,
        caller,
        stream,
    } = request;
    let reserved = match tables.connect(now, &route_id, &route_token, &caller, caller.uid) {
        Connect::Consumed(reserved) => reserved,
        Connect::Refused { reason, expired } => {
            let answer = RouteConnectAnswer::Refused {
                reason,
                stream: ReturnedStream(Arc::new(stream)),
            };
            return (answer, expired.map(RouteEnded::from));
        }
    };
    let AttachPlan {
        session,
        mut options,
    } = reserved.payload;
    options.route_tag = Some(route_id.clone());
    let transport = RouteTransport::Stream(StreamEndpoint::new(stream));
    let client = ClientId(reserved.caller.tag());
    match core.attach(client, session.clone(), transport, options) {
        Ok(attached) => {
            tables.insert_route(
                route_id,
                RouteEntry {
                    core_route: attached.route,
                    session,
                    owner: reserved.caller,
                },
            );
            (RouteConnectAnswer::Handed, None)
        }
        // Core never had the route, so no `RouteClosed` follows: the returned transport drops here (closing the
        // client's stream), and this is the route's one end (design 2.5, "synchronous handoff_failed").
        Err(_refused) => (
            RouteConnectAnswer::Handed,
            Some(RouteEnded {
                route_id,
                caller: reserved.caller,
                reason: RouteEndReason::HandoffFailed,
            }),
        ),
    }
}

/// The owner's route state: the tables, the parameters, and the `route.ended` events that wait for the `routes` stream.
#[derive(Debug)]
pub struct Routes {
    pub tables: HubRouteTables,
    pub params: RouteParams,
    ends: VecDeque<RouteEnded>,
    lost: u64,
    /// `welcome.terminal_format`'s common set (R-124), shared with `io`.
    formats: Arc<CommonFormats>,
}

impl Routes {
    pub fn new(params: RouteParams) -> Routes {
        Routes {
            tables: HubRouteTables::with_params(&params),
            params,
            ends: VecDeque::new(),
            lost: 0,
            formats: Arc::new(CommonFormats::default()),
        }
    }

    /// The common format set that `io` reads at `hello`.
    pub fn formats(&self) -> Arc<CommonFormats> {
        self.formats.clone()
    }

    /// Recomputes the common set over `sessions` (R-124). The owner calls it after each session state change.
    pub fn refresh_formats<'s, C: CoreApi>(
        &self,
        core: &C,
        sessions: impl Iterator<Item = &'s SessionId>,
    ) {
        self.formats
            .set(common_formats(sessions.map(|id| core.snapshot_formats(id))));
    }

    fn push_end(&mut self, end: Option<RouteEnded>) {
        let Some(end) = end else { return };
        if self.ends.len() < self.params.pending_ends {
            self.ends.push_back(end);
        } else {
            self.lost += 1;
        }
    }

    /// The pending `route.ended` events, oldest first, and the number lost since the last take.
    pub fn take_ends(&mut self) -> (Vec<RouteEnded>, u64) {
        (
            self.ends.drain(..).collect(),
            std::mem::take(&mut self.lost),
        )
    }

    /// [`reserve`] with the OS CSPRNG.
    pub fn reserve<C: CoreApi>(
        &mut self,
        core: &C,
        now: Instant,
        request: ReserveRequest,
    ) -> Result<Reservation, ReserveRefused> {
        reserve(
            core,
            &mut self.tables,
            &self.params,
            now,
            request,
            &mut OsEntropy,
        )
    }

    /// [`connect`]; its `route.ended`, if any, waits for the `routes` stream.
    pub fn connect<C: CoreApi>(
        &mut self,
        core: &mut C,
        now: Instant,
        request: RouteConnectRequest,
    ) -> RouteConnectAnswer {
        let (answer, end) = connect(core, &mut self.tables, now, request);
        self.push_end(end);
        answer
    }

    /// [`route_event`] for one Core event.
    pub fn event(&mut self, event: &Event) {
        let end = route_event(&mut self.tables, event);
        self.push_end(end);
    }

    /// Ends every due reservation: one `not_connected` each (TS-9).
    pub fn expire(&mut self, now: Instant) {
        for ended in self.tables.expire(now) {
            self.push_end(Some(ended.into()));
        }
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.tables.next_deadline()
    }
}

/// The route cause of a Core close reason, through Core's own map (`route_ended_cause`). Core's codec and the Hub's
/// codec are two builds of one wire set (two contracts tags), so the value crosses by its wire name.
fn end_reason(reason: botster_core::contract::prelude::RouteCloseReason) -> Option<RouteEndReason> {
    let cause = botster_core_link::route::route_ended_cause(reason)?;
    let wire = serde_json::to_value(cause).ok()?;
    serde_json::from_value(wire).ok()
}

/// Applies a Core route event to the route table (design 2.5 and 4). It returns the `route.ended` to send, if any.
///
/// - `RouteClosed` with a tag in the table removes that entry: a failed reason gives one `route.ended { cause }`; a
///   healthy close gives none, because the worker already sent `route_closed`.
/// - `RouteAdopted` with a tag and an owner this Hub made inserts the entry.
pub fn route_event(tables: &mut HubRouteTables, event: &Event) -> Option<RouteEnded> {
    match event {
        Event::RouteClosed {
            reason,
            route_tag: Some(tag),
            ..
        } => {
            let entry = tables.route_closed(tag)?;
            Some(RouteEnded {
                route_id: tag.clone(),
                caller: entry.owner,
                reason: end_reason(*reason)?,
            })
        }
        Event::RouteAdopted {
            session,
            route,
            route_tag: Some(tag),
            owner: Some(owner),
            ..
        } => {
            if let Some(owner) = Caller::from_tag(owner) {
                tables.insert_route(
                    tag.clone(),
                    RouteEntry {
                        core_route: *route,
                        session: session.clone(),
                        owner,
                    },
                );
            }
            None
        }
        // Every other event, and a route with no tag or no Hub owner (not this Hub's), changes no route entry.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::route::{EntropyError, OsEntropy};
    use botster_core::contract::prelude::{
        InstanceId, Now, Op, OpenConfig, RouteCloseReason, SessionState, Size, SpawnRequest,
    };
    use botster_fake_core::{FakeCore, FakeStore};
    use std::io::Read;

    /// The fake Core's one format, as Core names it.
    const FORMAT: &str = "ghostsnp/1+raw";

    fn params() -> RouteParams {
        RouteParams {
            max_reservations: 4,
            connect_window: Duration::from_secs(10),
            query_deadline: Duration::from_millis(200),
            sessions_root: "/data/sessions".into(),
            pending_ends: 2,
        }
    }

    fn me() -> Caller {
        Caller { uid: 501 }
    }

    fn sid(name: &str) -> SessionId {
        SessionId(name.into())
    }

    fn open(store: &FakeStore, limits: CoreLimits) -> FakeCore {
        let config = OpenConfig {
            data_dir: "/fake".into(),
            worker_path: Some("/fake/botster-worker".into()),
            limits,
        };
        FakeCore::open(store, config, 7).unwrap()
    }

    /// Pumps and polls until `done` sees an event, at most 64 rounds.
    fn pump_until(core: &mut FakeCore, mut done: impl FnMut(&Event) -> bool) {
        let clock = Instant::now();
        for round in 0..64u64 {
            core.pump(Now {
                monotonic: clock + Duration::from_millis(round),
                unix: 1_000_000,
            });
            if core.poll_events(64).iter().any(&mut done) {
                return;
            }
        }
        panic!("no matching event");
    }

    /// A fake Core with one started session `name`.
    fn started(limits: CoreLimits, name: &str) -> FakeCore {
        let store = FakeStore::new();
        let mut core = open(&store, limits);
        let request = SpawnRequest {
            argv: vec!["/bin/sh".into()],
            env: Default::default(),
            cwd: "/".into(),
            size: Size {
                rows: 24,
                cols: 80,
                cell_px: None,
            },
            labels: Default::default(),
            color_profile: None,
            notification_policy: None,
            size_policy: None,
        };
        core.begin(Op::Create {
            session: sid(name),
            request,
        })
        .unwrap();
        pump_until(&mut core, |e| matches!(e, Event::Completed { .. }));
        core.begin(Op::Start { id: sid(name) }).unwrap();
        pump_until(
            &mut core,
            |e| matches!(e, Event::SessionState { state, .. } if *state == SessionState::Running),
        );
        core
    }

    fn request(session: &str, formats: &[&str]) -> ReserveRequest {
        ReserveRequest {
            caller: me(),
            session: sid(session),
            terminal_formats: formats.iter().map(|f| f.to_string()).collect(),
            history: None,
            answers_queries: true,
            route_features: vec![],
        }
    }

    fn stream_pair() -> (UnixStream, UnixStream) {
        UnixStream::pair().unwrap()
    }

    #[test]
    fn route_ends_wait_in_order_and_overflow_is_counted() {
        let mut routes = Routes::new(params());
        let now = Instant::now();
        let mut core = started(CoreLimits::default(), "a");
        for _ in 0..3 {
            routes.reserve(&core, now, request("a", &[FORMAT])).unwrap();
        }
        assert_eq!(routes.next_deadline(), Some(now + params().connect_window));
        routes.expire(now + params().connect_window);
        let (ends, lost) = routes.take_ends();
        assert_eq!(ends.len(), 2);
        assert!(ends
            .iter()
            .all(|e| e.reason == RouteEndReason::NotConnected));
        assert_eq!(lost, 1);
        assert_eq!(routes.take_ends(), (vec![], 0));
        let (stream, _peer) = stream_pair();
        let answer = routes.connect(
            &mut core,
            now,
            RouteConnectRequest {
                route_id: "gone".into(),
                route_token: "t".into(),
                caller: me(),
                stream,
            },
        );
        assert!(matches!(answer, RouteConnectAnswer::Refused { .. }));
        assert_eq!(routes.take_ends(), (vec![], 0));
    }

    #[test]
    fn the_file_directory_is_the_sessions_uploads_directory() {
        assert_eq!(
            params().file_directory(&sid("a")),
            PathBuf::from("/data/sessions/a/uploads")
        );
    }

    #[test]
    fn the_caller_tag_round_trips_and_other_tags_are_not_callers() {
        assert_eq!(me().tag(), "operator/501");
        assert_eq!(Caller::from_tag("operator/501"), Some(me()));
        for other in [
            "operator/",
            "operator/+501",
            "operator/0501",
            "plugin/501",
            "",
            "operator/x",
        ] {
            assert_eq!(Caller::from_tag(other), None, "{other:?}");
        }
    }

    #[test]
    fn reserve_checks_the_session_and_the_format_and_mints_nothing_on_refusal() {
        let core = started(CoreLimits::default(), "a");
        let mut tables = HubRouteTables::with_params(&params());
        let now = Instant::now();
        let unknown = reserve(
            &core,
            &mut tables,
            &params(),
            now,
            request("none", &[FORMAT]),
            &mut OsEntropy,
        );
        assert!(
            matches!(&unknown, Err(ReserveRefused::Core(e)) if e.code == ErrorCode::UnknownSession),
            "{unknown:?}"
        );
        let no_format = reserve(
            &core,
            &mut tables,
            &params(),
            now,
            request("a", &["vt/9+raw"]),
            &mut OsEntropy,
        );
        let unsupported = ErrorCode::Unsupported {
            what: Some(UnsupportedReason::TerminalFormat),
        };
        assert!(
            matches!(&no_format, Err(ReserveRefused::Core(e)) if e.code == unsupported),
            "{no_format:?}"
        );
        assert_eq!(tables.reservations(), 0);
    }

    #[test]
    fn reserve_picks_the_first_common_format_and_fixes_the_r119_limits() {
        let core = started(CoreLimits::default(), "a");
        let mut tables = HubRouteTables::with_params(&params());
        let now = Instant::now();
        let reservation = reserve(
            &core,
            &mut tables,
            &params(),
            now,
            request("a", &["vt/9+raw", FORMAT]),
            &mut OsEntropy,
        )
        .unwrap();
        let limits = core.limits();
        assert_eq!(
            reservation.route_limits,
            RouteLimits {
                max_frame_bytes: limits.effective_max_route_frame_bytes(),
                max_screen_frame_bytes: limits.max_snapshot_bytes + 1,
                max_history_page_bytes: limits.max_history_page_bytes,
                max_paste_bytes: limits.max_paste_bytes,
                max_file_bytes: limits.max_file_bytes,
                max_query_reply_bytes: limits.max_query_reply_bytes,
                query_client_deadline_ms: 200,
                stall_deadline_ms: millis(limits.reader_progress_deadline),
                stall_close_after_ms: millis(limits.stall_close_after),
            }
        );
        assert_eq!(tables.reservations(), 1);
    }

    #[test]
    fn a_full_table_or_a_failed_random_source_is_refused() {
        struct Failing;
        impl Entropy for Failing {
            fn fill(&mut self, _: &mut [u8]) -> Result<(), EntropyError> {
                Err(EntropyError("none".into()))
            }
        }
        let core = started(CoreLimits::default(), "a");
        let one = RouteParams {
            max_reservations: 1,
            ..params()
        };
        let mut tables = HubRouteTables::with_params(&one);
        let now = Instant::now();
        assert!(matches!(
            reserve(
                &core,
                &mut tables,
                &one,
                now,
                request("a", &[FORMAT]),
                &mut Failing
            ),
            Err(ReserveRefused::Internal(_))
        ));
        reserve(
            &core,
            &mut tables,
            &one,
            now,
            request("a", &[FORMAT]),
            &mut OsEntropy,
        )
        .unwrap();
        assert_eq!(
            reserve(
                &core,
                &mut tables,
                &one,
                now,
                request("a", &[FORMAT]),
                &mut OsEntropy
            ),
            Err(ReserveRefused::Busy)
        );
    }

    #[test]
    fn a_valid_connect_attaches_with_the_reserved_plan_and_a_second_is_token_used() {
        let mut core = started(CoreLimits::default(), "a");
        let mut tables = HubRouteTables::with_params(&params());
        let now = Instant::now();
        let r = reserve(
            &core,
            &mut tables,
            &params(),
            now,
            request("a", &[FORMAT]),
            &mut OsEntropy,
        )
        .unwrap();
        let (hub_end, _client_end) = stream_pair();
        let (answer, ended) = connect(
            &mut core,
            &mut tables,
            now,
            RouteConnectRequest {
                route_id: r.route_id.clone(),
                route_token: r.route_token.clone(),
                caller: me(),
                stream: hub_end,
            },
        );
        assert_eq!((answer, ended), (RouteConnectAnswer::Handed, None));
        let entry = tables.route(&r.route_id).expect("in the route table");
        assert_eq!(
            (entry.session.clone(), entry.owner.clone()),
            (sid("a"), me())
        );
        let (again, _peer) = stream_pair();
        let (answer, ended) = connect(
            &mut core,
            &mut tables,
            now,
            RouteConnectRequest {
                route_id: r.route_id,
                route_token: r.route_token,
                caller: me(),
                stream: again,
            },
        );
        assert!(matches!(
            answer,
            RouteConnectAnswer::Refused {
                reason: CloseReason::RouteTokenUsed,
                ..
            }
        ));
        assert_eq!(ended, None);
    }

    #[test]
    fn a_refused_connect_returns_the_untouched_stream() {
        let mut core = started(CoreLimits::default(), "a");
        let mut tables = HubRouteTables::with_params(&params());
        let (hub_end, mut client_end) = stream_pair();
        let (answer, ended) = connect(
            &mut core,
            &mut tables,
            Instant::now(),
            RouteConnectRequest {
                route_id: "unknown".into(),
                route_token: "t".into(),
                caller: me(),
                stream: hub_end,
            },
        );
        assert_eq!(ended, None);
        let RouteConnectAnswer::Refused { reason, stream } = answer else {
            panic!("{answer:?}");
        };
        assert_eq!(reason, CloseReason::RouteTokenInvalid);
        // `io` writes the close through the returned stream; the client reads it.
        std::io::Write::write_all(&mut &*stream.0, b"x").unwrap();
        drop(stream);
        let mut got = Vec::new();
        client_end.read_to_end(&mut got).unwrap();
        assert_eq!(got, b"x");
    }

    #[test]
    fn a_sync_attach_refusal_closes_the_stream_and_ends_the_route_handoff_failed_once() {
        let limits = CoreLimits {
            routes_per_session: 1,
            ..CoreLimits::default()
        };
        let mut core = started(limits, "a");
        let mut tables = HubRouteTables::with_params(&params());
        let now = Instant::now();
        let first = reserve(
            &core,
            &mut tables,
            &params(),
            now,
            request("a", &[FORMAT]),
            &mut OsEntropy,
        )
        .unwrap();
        let second = reserve(
            &core,
            &mut tables,
            &params(),
            now,
            request("a", &[FORMAT]),
            &mut OsEntropy,
        )
        .unwrap();
        let (one, _keep) = stream_pair();
        let request_for = |r: &Reservation, stream| RouteConnectRequest {
            route_id: r.route_id.clone(),
            route_token: r.route_token.clone(),
            caller: me(),
            stream,
        };
        assert_eq!(
            connect(&mut core, &mut tables, now, request_for(&first, one)).0,
            RouteConnectAnswer::Handed
        );
        let (two, mut client_end) = stream_pair();
        let (answer, ended) = connect(&mut core, &mut tables, now, request_for(&second, two));
        assert_eq!(answer, RouteConnectAnswer::Handed);
        assert_eq!(
            ended,
            Some(RouteEnded {
                route_id: second.route_id.clone(),
                caller: me(),
                reason: RouteEndReason::HandoffFailed,
            })
        );
        assert!(tables.route(&second.route_id).is_none());
        let mut got = Vec::new();
        client_end.read_to_end(&mut got).unwrap();
        assert!(got.is_empty(), "the stream was closed with no bytes");
    }

    fn closed(tag: &str, reason: RouteCloseReason) -> Event {
        Event::RouteClosed {
            route: RouteId(9),
            reason,
            route_tag: Some(tag.into()),
        }
    }

    fn adopted(tag: &str, owner: &str) -> Event {
        Event::RouteAdopted {
            session: sid("a"),
            instance: InstanceId("i".into()),
            route: RouteId(9),
            route_tag: Some(tag.into()),
            owner: Some(owner.into()),
            client: ClientId(owner.into()),
        }
    }

    #[test]
    fn route_adopted_rebuilds_only_this_hubs_routes() {
        let mut tables = HubRouteTables::with_params(&params());
        assert_eq!(
            route_event(&mut tables, &adopted("r1", "operator/501")),
            None
        );
        assert_eq!(
            tables.route("r1"),
            Some(&RouteEntry {
                core_route: RouteId(9),
                session: sid("a"),
                owner: me(),
            })
        );
        route_event(&mut tables, &adopted("r2", "someone-else"));
        assert!(tables.route("r2").is_none());
    }

    #[test]
    fn a_failed_close_ends_the_route_once_with_cores_cause() {
        let mut tables = HubRouteTables::with_params(&params());
        route_event(&mut tables, &adopted("r1", "operator/501"));
        assert_eq!(
            route_event(&mut tables, &closed("r1", RouteCloseReason::PeerClosed)),
            Some(RouteEnded {
                route_id: "r1".into(),
                caller: me(),
                reason: RouteEndReason::TransportLost,
            })
        );
        assert!(tables.route("r1").is_none());
        assert_eq!(
            route_event(&mut tables, &closed("r1", RouteCloseReason::PeerClosed)),
            None,
            "a second close finds nothing"
        );
    }

    #[test]
    fn a_healthy_close_removes_the_route_with_no_event() {
        let mut tables = HubRouteTables::with_params(&params());
        route_event(&mut tables, &adopted("r1", "operator/501"));
        assert_eq!(
            route_event(&mut tables, &closed("r1", RouteCloseReason::Detached)),
            None
        );
        assert!(tables.route("r1").is_none());
    }

    #[test]
    fn every_failed_core_reason_maps_to_its_route_cause() {
        for (reason, cause) in [
            (
                RouteCloseReason::HandoffFailed,
                RouteEndReason::HandoffFailed,
            ),
            (RouteCloseReason::WriteFailed, RouteEndReason::WriteFailed),
            (RouteCloseReason::StallTimeout, RouteEndReason::Stalled),
            (RouteCloseReason::SessionLost, RouteEndReason::SessionLost),
            (RouteCloseReason::PeerClosed, RouteEndReason::TransportLost),
        ] {
            assert_eq!(end_reason(reason), Some(cause));
        }
        assert_eq!(end_reason(RouteCloseReason::SessionRemoved), None);
    }
}
