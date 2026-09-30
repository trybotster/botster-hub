//! The owner side of the doorbell.
//!
//! [`crate::daemon::doorbell`] decides WHEN a ring may be typed. This module
//! feeds it and carries out what it asks. It has two parts:
//!
//! - [`Plan`] is pure bookkeeping. It holds the machine, the queue of Core
//!   jobs its effects produce, and the two timers per session. It has no
//!   Core, clock or owner state, so its behaviour is tested directly.
//! - [`DoorbellOwnerState`] adds the one Core ticket in flight and the owner's
//!   single deadline. [`drive`] polls that ticket, drains the edges the
//!   data-plane thread stored, fires due timers, and starts the next job.
//!
//! Core work is SINGLE FLIGHT across every session: one ticket at a time,
//! carried by the doorbell's background waiter like the pump observe pass. A
//! job queue holds the rest in order. A refused admission parks the class on
//! the request queue's room, never on an unrelated wake.
//!
//! There is one owner deadline for the whole doorbell, armed at the earliest
//! timer of any session. When it fires the owner compares each session's
//! timers with the clock.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Instant;

use botster_core::{RequestId, SessionId};
use botster_core_daemon::{CoreCompletion, CoreDaemonError, HostInputOutcome, SessionEdges};
use botster_terminal_protocol::InputOutcome;

use crate::HubDaemon;
use crate::daemon::doorbell::{Doorbell, Effect, Event, Facts, Purpose, Read, write_purpose};
use crate::daemon::owner_loop::DaemonControlState;
use crate::daemon::owner_schedule::DeadlineKey;
use crate::data_plane::driver::{CoreTicket, CoreTicketPoll};
use crate::runtime::CoreOperationTracker;

/// One Core request the machine's effects need, run one at a time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Job {
    /// Read the session's edges for a new ring: the machine needs Core's facts
    /// as of the ring and the ring carries its text through the read.
    Edges { session: String, text: String },
    /// One cursor read (`ReadCursor`).
    Cursor { session: String },
    /// One host write to the session.
    Write {
        session: String,
        bytes: Vec<u8>,
        purpose: Purpose,
    },
}

impl Job {
    fn session(&self) -> &str {
        match self {
            Job::Edges { session, .. } | Job::Cursor { session } | Job::Write { session, .. } => {
                session
            }
        }
    }
}

/// The two timers a session's attempt can have armed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Timers {
    quiet: Option<Instant>,
    echo: Option<Instant>,
}

impl Timers {
    fn is_empty(self) -> bool {
        self.quiet.is_none() && self.echo.is_none()
    }
}

/// Pure doorbell bookkeeping: the machine, the job queue and the timers.
#[derive(Debug, Default)]
pub(crate) struct Plan {
    machine: Doorbell,
    /// Sessions the owner tracks: a ring or an attempt exists.
    live: BTreeSet<String>,
    jobs: VecDeque<Job>,
    timers: BTreeMap<String, Timers>,
    /// When the one write in flight is cancelled. A write has no deadline of
    /// its own, and a PTY that never drains would hold the single flight.
    // timer: deadline: a write the PTY does not take within the echo deadline is cancelled; expiry is the exceptional path
    write_cancel: Option<Instant>,
}

impl Plan {
    /// A ring is requested for the session. The machine learns of it once its
    /// facts are read (a job); a newer ring for the same session replaces the
    /// text of one still waiting.
    pub(crate) fn request_ring(&mut self, session: &str, text: String) {
        self.live.insert(session.to_string());
        for job in &mut self.jobs {
            if let Job::Edges {
                session: queued,
                text: queued_text,
            } = job
                && queued == session
            {
                *queued_text = text;
                return;
            }
        }
        self.jobs.push_back(Job::Edges {
            session: session.to_string(),
            text,
        });
    }

    /// The facts read for a ring arrived. `None` means the session is gone.
    pub(crate) fn ring_facts(
        &mut self,
        session: &str,
        text: String,
        facts: Option<(Facts, Option<Instant>)>,
        now: Instant,
    ) {
        if !self.live.contains(session) {
            return;
        }
        let Some((facts, last_input_at)) = facts else {
            self.ended(session);
            return;
        };
        let effects = self.machine.step(
            &SessionId(session.to_string()),
            now,
            Event::Ring {
                text,
                facts,
                last_input_at,
            },
        );
        self.apply(session, effects);
    }

    /// Core's facts after an edge, for a session the machine watches.
    pub(crate) fn facts(&mut self, session: &str, facts: Facts, now: Instant) {
        if !self.live.contains(session) {
            return;
        }
        let effects = self
            .machine
            .step(&SessionId(session.to_string()), now, Event::Facts(facts));
        self.apply(session, effects);
    }

    /// The answer to a cursor read.
    pub(crate) fn cursor(&mut self, session: &str, read: Option<Read>, now: Instant) {
        if !self.live.contains(session) {
            return;
        }
        let event = read.map_or(Event::CursorFailed, Event::Cursor);
        let effects = self
            .machine
            .step(&SessionId(session.to_string()), now, event);
        self.apply(session, effects);
    }

    /// A write finished or was refused. A refused ring is pending again; a
    /// refused probe or erase stops the attempt; an accepted probe or erase
    /// needs no event (the machine waits for the output edge).
    pub(crate) fn write_done(&mut self, session: &str, purpose: Purpose, ok: bool, now: Instant) {
        self.write_cancel = None;
        if !self.live.contains(session) {
            return;
        }
        let event = match (purpose, ok) {
            (Purpose::Ring, true) => Event::Delivered,
            (Purpose::Ring, false) => Event::DeliveryFailed,
            (_, true) => return,
            (_, false) => Event::CursorFailed,
        };
        let effects = self
            .machine
            .step(&SessionId(session.to_string()), now, event);
        self.apply(session, effects);
    }

    /// Fire every timer that is due at `now`, in session order.
    pub(crate) fn fire_due(&mut self, now: Instant) {
        let due: Vec<(String, bool, bool)> = self
            .timers
            .iter()
            .filter_map(|(session, timers)| {
                let quiet = timers.quiet.is_some_and(|at| at <= now);
                let echo = timers.echo.is_some_and(|at| at <= now);
                (quiet || echo).then(|| (session.clone(), quiet, echo))
            })
            .collect();
        for (session, quiet, echo) in due {
            if let Some(timers) = self.timers.get_mut(&session) {
                if quiet {
                    timers.quiet = None;
                }
                if echo {
                    timers.echo = None;
                }
            }
            for event in [
                quiet.then_some(Event::QuietWake),
                echo.then_some(Event::EchoDeadline),
            ]
            .into_iter()
            .flatten()
            {
                let effects = self.machine.step(&SessionId(session.clone()), now, event);
                self.apply(&session, effects);
            }
            self.prune_timers(&session);
        }
    }

    /// The session ended: forget everything about it.
    pub(crate) fn ended(&mut self, session: &str) {
        let now = Instant::now();
        let effects = self
            .machine
            .step(&SessionId(session.to_string()), now, Event::Ended);
        self.apply(session, effects);
        self.live.remove(session);
        self.timers.remove(session);
        self.jobs.retain(|job| job.session() != session);
    }

    /// The sessions the doorbell tracks.
    pub(crate) fn live(&self) -> impl Iterator<Item = &String> {
        self.live.iter()
    }

    pub(crate) fn next_job(&mut self) -> Option<Job> {
        self.jobs.pop_front()
    }

    /// Put a refused job back at the front: it keeps its place in the queue.
    pub(crate) fn requeue_front(&mut self, job: Job) {
        if self.live.contains(job.session()) {
            self.jobs.push_front(job);
        }
    }

    pub(crate) fn has_jobs(&self) -> bool {
        !self.jobs.is_empty()
    }

    /// The earliest timer of any session, or the write cancel.
    pub(crate) fn earliest_timer(&self) -> Option<Instant> {
        self.timers
            .values()
            .flat_map(|timers| [timers.quiet, timers.echo])
            .flatten()
            .chain(self.write_cancel)
            .min()
    }

    /// A write went in flight: cancel it at `at` if it has not completed.
    pub(crate) fn arm_write_cancel(&mut self, at: Instant) {
        self.write_cancel = Some(at);
    }

    /// Whether the write in flight is due to be cancelled.
    pub(crate) fn write_cancel_due(&self, now: Instant) -> bool {
        self.write_cancel.is_some_and(|at| at <= now)
    }

    fn prune_timers(&mut self, session: &str) {
        if self
            .timers
            .get(session)
            .is_some_and(|timers| timers.is_empty())
        {
            self.timers.remove(session);
        }
    }

    /// Carry out the machine's effects for one session.
    fn apply(&mut self, session: &str, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::ReadCursor => self.jobs.push_back(Job::Cursor {
                    session: session.to_string(),
                }),
                Effect::Write(bytes) => {
                    let purpose = write_purpose(&bytes);
                    self.jobs.push_back(Job::Write {
                        session: session.to_string(),
                        bytes,
                        purpose,
                    });
                }
                Effect::ArmQuietWake(at) => {
                    self.timers.entry(session.to_string()).or_default().quiet = Some(at);
                }
                Effect::ArmEchoDeadline(at) => {
                    self.timers.entry(session.to_string()).or_default().echo = Some(at);
                }
                Effect::CancelEchoDeadline => {
                    if let Some(timers) = self.timers.get_mut(session) {
                        timers.echo = None;
                    }
                }
            }
        }
        self.prune_timers(session);
    }

    /// Sessions the doorbell tracks that have nothing left: the machine holds
    /// no state, and there is no job or timer. `in_flight` is the session of
    /// the Core ticket in flight, which is still work.
    pub(crate) fn idle_sessions(&self, in_flight: Option<&str>) -> Vec<String> {
        self.live
            .iter()
            .filter(|session| {
                Some(session.as_str()) != in_flight
                    && !self.machine.holds(&SessionId((*session).clone()))
                    && !self.timers.contains_key(*session)
                    && !self
                        .jobs
                        .iter()
                        .any(|job| job.session() == session.as_str())
            })
            .cloned()
            .collect()
    }

    /// Stop tracking an idle session.
    pub(crate) fn release(&mut self, session: &str) {
        self.live.remove(session);
    }
}

/// The Core ticket in flight, with the job it carries.
enum Flight {
    Edges {
        session: String,
        text: String,
        ticket: CoreTicket<Result<Option<SessionEdges>, CoreDaemonError>>,
    },
    Cursor {
        session: String,
        tracker: CoreOperationTracker,
    },
    Write {
        session: String,
        purpose: Purpose,
        tracker: CoreOperationTracker,
        /// The cancel request, once the cancel deadline passed.
        cancel: Option<CoreTicket<bool>>,
    },
}

impl Flight {
    fn session(&self) -> &str {
        match self {
            Flight::Edges { session, .. }
            | Flight::Cursor { session, .. }
            | Flight::Write { session, .. } => session,
        }
    }
}

/// What one drive left for the owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Progress {
    /// More can run now: mark the class ready again.
    Runnable,
    /// A ticket is in flight or a timer is armed: the owner wakes on its
    /// completion, its deadline, or the doorbell signal.
    Waiting,
}

/// The doorbell's owner state.
#[derive(Default)]
pub(crate) struct DoorbellOwnerState {
    plan: Plan,
    flight: Option<Flight>,
    /// The one owner deadline, armed at the earliest timer of any session.
    deadline: Option<DeadlineKey>,
}

impl std::fmt::Debug for DoorbellOwnerState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DoorbellOwnerState")
    }
}

/// The facts the machine needs, from Core's edges.
fn facts_of(edges: &SessionEdges) -> Facts {
    let flags = edges.mode_flags.as_ref();
    Facts {
        modes_epoch: edges.modes_epoch,
        output_seq: edges.output_seq,
        input_seq: edges.input_seq,
        composing: edges.composing,
        // No mode report yet: no cursor is known, so the gate holds.
        cursor_visible: flags.is_some_and(|flags| flags.cursor_visible),
        bracketed_paste: flags.is_some_and(|flags| flags.bracketed_paste),
        kitty_enabled: flags.is_some_and(|flags| flags.kitty_enabled),
        cols: edges.size.cols,
    }
}

/// A ring for the session, from the plugin surface. The ring text is not
/// trusted: the machine strips control bytes before it types anything.
pub(crate) fn ring(
    daemon: &HubDaemon,
    state: &mut DaemonControlState,
    session: &str,
    text: String,
) {
    let Some(runtime) = daemon.runtime() else {
        return;
    };
    runtime.doorbell_edges().watch(session);
    state.doorbell.plan.request_ring(session, text);
    crate::daemon::owner_loop::mark_doorbell_ready(state);
}

/// A session left the lifecycle projection or ended: the doorbell forgets it.
/// Called after the projection applies changes, for the sessions it tracks.
pub(crate) fn sync_lifecycle(daemon: &HubDaemon, state: &mut DaemonControlState) {
    let ended: Vec<String> = state
        .doorbell
        .plan
        .live()
        .filter(|session| {
            state
                .maintenance
                .projection
                .rows
                .get(session.as_str())
                .is_some_and(|row| row.live_ended || row.lifecycle_class == "ended")
                || (state.maintenance.projection.baseline_complete
                    && !state
                        .maintenance
                        .projection
                        .rows
                        .contains_key(session.as_str()))
        })
        .cloned()
        .collect();
    if ended.is_empty() {
        return;
    }
    for session in ended {
        state.doorbell.plan.ended(&session);
        if let Some(runtime) = daemon.runtime() {
            runtime.doorbell_edges().forget(&session);
        }
    }
    crate::daemon::owner_loop::mark_doorbell_ready(state);
}

/// The doorbell's deadline fired: the owner drives it to compare timers.
pub(crate) fn deadline_fired(
    state: &mut DaemonControlState,
    waiter_id: crate::owner_identity::WaiterId,
) -> bool {
    if crate::daemon::owner_loop::doorbell_waiter(state) != Some(waiter_id) {
        return false;
    }
    state.doorbell.deadline = None;
    crate::daemon::owner_loop::mark_doorbell_ready(state);
    true
}

/// One bounded doorbell slice. It never waits.
pub(crate) fn drive(daemon: &HubDaemon, state: &mut DaemonControlState) -> Progress {
    let Some(runtime) = daemon.runtime() else {
        state.doorbell = DoorbellOwnerState::default();
        return Progress::Waiting;
    };
    let now = Instant::now();
    let edges = runtime.doorbell_edges().clone();
    // 1. The ticket in flight: apply its result, or keep waiting for it.
    if let Some(progress) = poll_flight(runtime, state, &edges, now) {
        arm_deadline(state, now);
        return progress;
    }
    // 2. The edges the data-plane thread stored since the last drive.
    let seen = runtime
        .owner_signal()
        .seen(crate::daemon::owner_signal::SignalKey::Doorbell);
    for (session, session_edges) in edges.take_latest() {
        state
            .doorbell
            .plan
            .facts(&session, facts_of(&session_edges), now);
    }
    // 3. Timers that came due.
    state.doorbell.plan.fire_due(now);
    // 4. Nothing left for a session: stop watching it.
    for session in state
        .doorbell
        .plan
        .idle_sessions(state.doorbell.flight.as_ref().map(Flight::session))
    {
        state.doorbell.plan.release(&session);
        edges.unwatch(&session);
    }
    // 5. Start the next job, one ticket in flight.
    let started = start_next(runtime, state, now);
    arm_deadline(state, now);
    match started {
        Started::Refused(seen) => {
            crate::daemon::owner_loop::park_doorbell(state, seen);
            Progress::Waiting
        }
        Started::InFlight => Progress::Waiting,
        Started::Nothing => {
            if edges.has_latest() || state.doorbell.plan.has_jobs() {
                Progress::Runnable
            } else {
                // Arm-then-check: the signal epoch was read before the final
                // look at the table, so a raise after the read moves it.
                crate::daemon::owner_loop::park_doorbell(state, seen);
                Progress::Waiting
            }
        }
    }
}

enum Started {
    Nothing,
    InFlight,
    Refused(crate::daemon::owner_signal::Seen),
}

/// Apply the result of the ticket in flight. `Some` ends this drive.
fn poll_flight(
    runtime: &crate::runtime::HubRuntime,
    state: &mut DaemonControlState,
    edges: &crate::data_plane::doorbell_edges::DoorbellEdges,
    now: Instant,
) -> Option<Progress> {
    let flight = state.doorbell.flight.take()?;
    match flight {
        Flight::Edges {
            session,
            text,
            mut ticket,
        } => match ticket.poll() {
            CoreTicketPoll::Pending => {
                state.doorbell.flight = Some(Flight::Edges {
                    session,
                    text,
                    ticket,
                });
                Some(Progress::Waiting)
            }
            CoreTicketPoll::Refused => {
                // The request queue turned it away: it keeps its place.
                state
                    .doorbell
                    .plan
                    .requeue_front(Job::Edges { session, text });
                None
            }
            CoreTicketPoll::Lost => {
                state.doorbell.plan.ended(&session);
                edges.forget(&session);
                None
            }
            CoreTicketPoll::Ready(result) => {
                let facts = match result {
                    Ok(Some(session_edges)) => {
                        Some((facts_of(&session_edges), edges.input_at(&session)))
                    }
                    Ok(None) | Err(_) => None,
                };
                state.doorbell.plan.ring_facts(&session, text, facts, now);
                None
            }
        },
        Flight::Cursor {
            session,
            mut tracker,
        } => match tracker.poll(runtime) {
            CoreTicketPoll::Pending => {
                state.doorbell.flight = Some(Flight::Cursor { session, tracker });
                Some(Progress::Waiting)
            }
            CoreTicketPoll::Refused => {
                state.doorbell.plan.requeue_front(Job::Cursor { session });
                None
            }
            CoreTicketPoll::Lost => {
                state.doorbell.plan.cursor(&session, None, now);
                None
            }
            CoreTicketPoll::Ready(result) => {
                let read = match result {
                    Ok(CoreCompletion::ReadCursor {
                        result: Ok(readback),
                        ..
                    }) => Some(Read {
                        row: readback.cursor.row,
                        col: readback.cursor.col,
                        text_before_cursor: readback.cursor.text_before_cursor,
                    }),
                    _ => None,
                };
                state.doorbell.plan.cursor(&session, read, now);
                None
            }
        },
        Flight::Write {
            session,
            purpose,
            mut tracker,
            mut cancel,
        } => match tracker.poll(runtime) {
            CoreTicketPoll::Pending => {
                if cancel.is_none()
                    && state.doorbell.plan.write_cancel_due(now)
                    && let Some(id) = tracker.pending_id()
                    && let Some(waiter_id) = crate::daemon::owner_loop::doorbell_waiter(state)
                {
                    let ticket =
                        runtime.submit_core_for_owner(waiter_id, move |daemon| daemon.cancel(id));
                    if let Some(seen) = ticket.refused_wait() {
                        state.doorbell.flight = Some(Flight::Write {
                            session,
                            purpose,
                            tracker,
                            cancel: None,
                        });
                        crate::daemon::owner_loop::park_doorbell(state, seen);
                        return Some(Progress::Waiting);
                    }
                    cancel = Some(ticket);
                }
                state.doorbell.flight = Some(Flight::Write {
                    session,
                    purpose,
                    tracker,
                    cancel,
                });
                Some(Progress::Waiting)
            }
            CoreTicketPoll::Refused | CoreTicketPoll::Lost => {
                state
                    .doorbell
                    .plan
                    .write_done(&session, purpose, false, now);
                None
            }
            CoreTicketPoll::Ready(result) => {
                // Only a `Written` outcome is delivery. A full lane, a
                // cancel, an ended session or a lost link all mean the bytes
                // may not be there, and none of them is retried in a loop.
                let written = matches!(
                    result,
                    Ok(CoreCompletion::HostInput { result: ref host, .. }) if delivered(host)
                );
                state
                    .doorbell
                    .plan
                    .write_done(&session, purpose, written, now);
                None
            }
        },
    }
}

/// Whether Core's answer to a host write means the bytes reached the PTY.
/// Only `Written` does: a full lane, a cancel, an ended session, a lost link
/// and any error all mean they may not have.
fn delivered(result: &Result<HostInputOutcome, CoreDaemonError>) -> bool {
    result
        .as_ref()
        .is_ok_and(|outcome| outcome.outcome == InputOutcome::Written)
}

/// Submit the next queued job as the one Core ticket in flight.
fn start_next(
    runtime: &crate::runtime::HubRuntime,
    state: &mut DaemonControlState,
    _now: Instant,
) -> Started {
    if state.doorbell.flight.is_some() {
        return Started::InFlight;
    }
    let Some(job) = state.doorbell.plan.next_job() else {
        return Started::Nothing;
    };
    let Some(waiter_id) = crate::daemon::owner_loop::doorbell_waiter(state) else {
        state.doorbell.plan.requeue_front(job);
        return Started::Nothing;
    };
    let now_seconds = crate::daemon::owner_loop::tick(&mut state.logical_clock);
    match job {
        Job::Edges { session, text } => {
            let id = SessionId(session.clone());
            let ticket = runtime.submit_core_for_optional_owner(Some(waiter_id), move |daemon| {
                daemon.session_edges(&id)
            });
            if let Some(seen) = ticket.refused_wait() {
                state
                    .doorbell
                    .plan
                    .requeue_front(Job::Edges { session, text });
                return Started::Refused(seen);
            }
            state.doorbell.flight = Some(Flight::Edges {
                session,
                text,
                ticket,
            });
        }
        Job::Cursor { session } => {
            let tracker = runtime.begin_read_cursor_for_owner(
                waiter_id,
                RequestId(format!("doorbell-cursor-{now_seconds}")),
                SessionId(session.clone()),
                now_seconds,
            );
            state.doorbell.flight = Some(Flight::Cursor { session, tracker });
        }
        Job::Write {
            session,
            bytes,
            purpose,
        } => {
            let tracker = runtime.begin_host_input_for_owner(
                waiter_id,
                RequestId(format!("doorbell-write-{now_seconds}")),
                SessionId(session.clone()),
                bytes,
                now_seconds,
            );
            state
                .doorbell
                .plan
                .arm_write_cancel(Instant::now() + crate::daemon::doorbell::ECHO_DEADLINE);
            state.doorbell.flight = Some(Flight::Write {
                session,
                purpose,
                tracker,
                cancel: None,
            });
        }
    }
    Started::InFlight
}

/// Keep the one owner deadline at the earliest timer of any session.
fn arm_deadline(state: &mut DaemonControlState, now: Instant) {
    let Some(waiter_id) = crate::daemon::owner_loop::doorbell_waiter(state) else {
        return;
    };
    match state.doorbell.plan.earliest_timer() {
        Some(at) => {
            let already = state
                .doorbell
                .deadline
                .is_some_and(|key| key.instant() == at);
            if already {
                return;
            }
            match state.deadlines.arm(waiter_id, at, now) {
                Ok(arm) => {
                    state.doorbell.deadline = Some(arm.key());
                    if arm.is_due() {
                        crate::daemon::owner_loop::mark_doorbell_ready(state);
                    }
                }
                Err(_) => crate::daemon::owner_loop::mark_doorbell_ready(state),
            }
        }
        None => {
            if let Some(key) = state.doorbell.deadline.take() {
                state.deadlines.disarm(key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::doorbell::{ECHO_DEADLINE, QUIET_PERIOD};
    use std::time::Duration;

    const A: &str = "sess-a";
    const B: &str = "sess-b";

    fn facts() -> Facts {
        Facts {
            modes_epoch: 0,
            output_seq: 0,
            input_seq: 0,
            composing: false,
            cursor_visible: true,
            bracketed_paste: true,
            kitty_enabled: false,
            cols: 80,
        }
    }

    fn read() -> Read {
        Read {
            row: 10,
            col: 2,
            text_before_cursor: "> ".to_string(),
        }
    }

    /// A ring whose facts are read and found ready: the machine starts a
    /// baseline read, which the plan queues as a cursor job.
    fn ring_ready(plan: &mut Plan, session: &str, now: Instant) {
        plan.request_ring(session, "hello".to_string());
        let Some(Job::Edges { session: got, text }) = plan.next_job() else {
            panic!("a ring queues an edges job");
        };
        assert_eq!(got, session);
        plan.ring_facts(session, text, Some((facts(), None)), now);
    }

    fn pop_cursor(plan: &mut Plan, session: &str) {
        assert_eq!(
            plan.next_job(),
            Some(Job::Cursor {
                session: session.to_string()
            })
        );
    }

    #[test]
    fn a_newer_ring_replaces_the_text_of_one_still_queued() {
        let mut plan = Plan::default();
        plan.request_ring(A, "first".to_string());
        plan.request_ring(B, "other".to_string());
        plan.request_ring(A, "second".to_string());
        assert_eq!(
            plan.next_job(),
            Some(Job::Edges {
                session: A.to_string(),
                text: "second".to_string()
            }),
            "one job per session, with the latest text, in first-ring order"
        );
        assert!(matches!(plan.next_job(), Some(Job::Edges { session, .. }) if session == B));
        assert_eq!(plan.next_job(), None);
    }

    #[test]
    fn a_ready_ring_queues_one_cursor_read() {
        let mut plan = Plan::default();
        ring_ready(&mut plan, A, Instant::now());
        pop_cursor(&mut plan, A);
        assert_eq!(plan.next_job(), None);
    }

    #[test]
    fn a_cursor_answer_queues_a_probe_and_arms_the_echo_deadline() {
        let mut plan = Plan::default();
        let now = Instant::now();
        ring_ready(&mut plan, A, now);
        pop_cursor(&mut plan, A);
        plan.cursor(A, Some(read()), now);
        assert_eq!(
            plan.next_job(),
            Some(Job::Write {
                session: A.to_string(),
                bytes: b"zx".to_vec(),
                purpose: Purpose::Probe
            })
        );
        assert_eq!(plan.earliest_timer(), Some(now + ECHO_DEADLINE));
    }

    #[test]
    fn a_ring_under_the_quiet_period_arms_one_wake_that_new_input_replaces() {
        let mut plan = Plan::default();
        let now = Instant::now();
        plan.request_ring(A, "hello".to_string());
        let Some(Job::Edges { text, .. }) = plan.next_job() else {
            panic!("edges job");
        };
        plan.ring_facts(A, text, Some((facts(), Some(now))), now);
        assert_eq!(plan.earliest_timer(), Some(now + QUIET_PERIOD));
        assert_eq!(
            plan.next_job(),
            None,
            "no read while the client is quiet-waiting"
        );
        // Input advances one second later: the one wake moves, it does not multiply.
        let mut typed = facts();
        typed.input_seq = 1;
        let later = now + Duration::from_secs(1);
        plan.facts(A, typed, later);
        assert_eq!(plan.earliest_timer(), Some(later + QUIET_PERIOD));
        assert_eq!(
            plan.timers.get(A).map(|timers| timers.quiet.is_some()),
            Some(true)
        );
        assert_eq!(plan.timers.len(), 1);
    }

    #[test]
    fn due_timers_fire_in_session_order_and_a_fired_timer_is_cleared() {
        let mut plan = Plan::default();
        let now = Instant::now();
        for session in [B, A] {
            plan.request_ring(session, "hello".to_string());
            let Some(Job::Edges { text, .. }) = plan.next_job() else {
                panic!("edges job");
            };
            plan.ring_facts(session, text, Some((facts(), Some(now))), now);
        }
        assert_eq!(plan.timers.len(), 2);
        plan.fire_due(now + QUIET_PERIOD);
        // Both wakes fired; each session starts its baseline read, A before B.
        pop_cursor(&mut plan, A);
        pop_cursor(&mut plan, B);
        assert!(plan.timers.is_empty(), "fired timers are gone");
        assert_eq!(plan.earliest_timer(), None);
    }

    #[test]
    fn a_timer_that_is_not_due_does_not_fire() {
        let mut plan = Plan::default();
        let now = Instant::now();
        plan.request_ring(A, "hello".to_string());
        let Some(Job::Edges { text, .. }) = plan.next_job() else {
            panic!("edges job");
        };
        plan.ring_facts(A, text, Some((facts(), Some(now))), now);
        plan.fire_due(now + QUIET_PERIOD - Duration::from_millis(1));
        assert_eq!(plan.next_job(), None);
        assert_eq!(plan.earliest_timer(), Some(now + QUIET_PERIOD));
    }

    #[test]
    fn an_accepted_probe_needs_no_event_and_a_refused_one_stops_the_attempt() {
        let now = Instant::now();
        // Accepted: nothing new is queued and the echo deadline stays armed.
        let mut plan = Plan::default();
        ring_ready(&mut plan, A, now);
        pop_cursor(&mut plan, A);
        plan.cursor(A, Some(read()), now);
        plan.next_job();
        plan.write_done(A, Purpose::Probe, true, now);
        assert_eq!(plan.next_job(), None);
        assert_eq!(plan.earliest_timer(), Some(now + ECHO_DEADLINE));
        // Refused: the machine stops, the deadline is dropped, the ring waits.
        let mut plan = Plan::default();
        ring_ready(&mut plan, A, now);
        pop_cursor(&mut plan, A);
        plan.cursor(A, Some(read()), now);
        plan.next_job();
        plan.write_done(A, Purpose::Probe, false, now);
        assert_eq!(
            plan.earliest_timer(),
            None,
            "the echo deadline is cancelled"
        );
        assert!(
            plan.machine.holds(&SessionId(A.to_string())),
            "the ring still waits"
        );
        assert_eq!(plan.next_job(), None);
    }

    fn host_outcome(outcome: InputOutcome) -> Result<HostInputOutcome, CoreDaemonError> {
        Ok(HostInputOutcome {
            outcome,
            accepted_payload_bytes: None,
            written_pty_bytes: None,
            detail: String::new(),
        })
    }

    #[test]
    fn only_a_written_outcome_is_delivery() {
        assert!(delivered(&host_outcome(InputOutcome::Written)));
        for outcome in [
            InputOutcome::PartialWrite,
            InputOutcome::WriteFailed,
            InputOutcome::Cancelled,
            InputOutcome::RejectedNotWritable,
            InputOutcome::RejectedLaneFull,
            InputOutcome::SessionEnded,
            InputOutcome::OutcomeUnknown,
        ] {
            assert!(!delivered(&host_outcome(outcome)), "{outcome:?}");
        }
        assert!(!delivered(&Err(CoreDaemonError::Shutdown)));
    }

    #[test]
    fn a_write_in_flight_arms_one_cancel_deadline_and_its_end_clears_it() {
        let mut plan = Plan::default();
        let now = Instant::now();
        assert!(!plan.write_cancel_due(now));
        plan.arm_write_cancel(now + ECHO_DEADLINE);
        assert_eq!(plan.earliest_timer(), Some(now + ECHO_DEADLINE));
        assert!(!plan.write_cancel_due(now + ECHO_DEADLINE - Duration::from_millis(1)));
        assert!(plan.write_cancel_due(now + ECHO_DEADLINE));
        // A cancelled or refused write completes; the deadline goes with it,
        // whether or not the session is still tracked.
        plan.write_done(A, Purpose::Probe, false, now);
        assert_eq!(plan.earliest_timer(), None);
        assert!(!plan.write_cancel_due(now + ECHO_DEADLINE));
    }

    #[test]
    fn a_failed_cursor_read_stops_the_attempt_and_keeps_the_ring() {
        let mut plan = Plan::default();
        let now = Instant::now();
        ring_ready(&mut plan, A, now);
        pop_cursor(&mut plan, A);
        plan.cursor(A, None, now);
        assert_eq!(plan.next_job(), None);
        assert!(plan.machine.holds(&SessionId(A.to_string())));
    }

    #[test]
    fn a_delivered_ring_leaves_the_session_idle_and_a_refused_one_pending() {
        let now = Instant::now();
        let drive_to_ring_write = |plan: &mut Plan| {
            ring_ready(plan, A, now);
            pop_cursor(plan, A);
            plan.cursor(A, Some(read()), now);
            plan.next_job(); // the probe
            // The probe echoes: two cells on, `zx` before the cursor.
            let mut echoed = facts();
            echoed.output_seq = 1;
            plan.facts(A, echoed, now);
            pop_cursor(plan, A);
            plan.cursor(
                A,
                Some(Read {
                    row: 10,
                    col: 4,
                    text_before_cursor: "> zx".to_string(),
                }),
                now,
            );
            let Some(Job::Write { purpose, .. }) = plan.next_job() else {
                panic!("the erase");
            };
            assert_eq!(purpose, Purpose::Erase);
            // The erase lands: output advances and the cursor is back.
            let mut erased = facts();
            erased.output_seq = 2;
            plan.facts(A, erased, now);
            pop_cursor(plan, A);
            plan.cursor(A, Some(read()), now);
            let Some(Job::Write { purpose, bytes, .. }) = plan.next_job() else {
                panic!("the ring");
            };
            assert_eq!(purpose, Purpose::Ring);
            assert_eq!(bytes.last(), Some(&b'\r'));
        };
        let mut delivered = Plan::default();
        drive_to_ring_write(&mut delivered);
        delivered.write_done(A, Purpose::Ring, true, now);
        assert_eq!(delivered.idle_sessions(None), [A]);
        let mut refused = Plan::default();
        drive_to_ring_write(&mut refused);
        refused.write_done(A, Purpose::Ring, false, now);
        assert!(
            refused.machine.holds(&SessionId(A.to_string())),
            "pending again"
        );
        assert!(refused.idle_sessions(None).is_empty());
    }

    #[test]
    fn a_session_in_flight_is_never_idle_and_a_working_one_is_not_either() {
        let mut plan = Plan::default();
        plan.request_ring(A, "hello".to_string());
        // A queued job is work.
        assert!(plan.idle_sessions(None).is_empty());
        // The job goes in flight: the machine holds nothing yet.
        let job = plan.next_job();
        assert!(matches!(job, Some(Job::Edges { .. })));
        assert!(plan.idle_sessions(None).contains(&A.to_string()));
        assert!(
            plan.idle_sessions(Some(A)).is_empty(),
            "the ticket in flight still counts as work"
        );
        plan.release(A);
        assert_eq!(plan.live().count(), 0);
    }

    #[test]
    fn an_ended_session_forgets_its_jobs_timers_and_state() {
        let mut plan = Plan::default();
        let now = Instant::now();
        plan.request_ring(A, "hello".to_string());
        let Some(Job::Edges { text, .. }) = plan.next_job() else {
            panic!("edges job");
        };
        plan.ring_facts(A, text, Some((facts(), Some(now))), now);
        plan.request_ring(A, "again".to_string());
        plan.ended(A);
        assert_eq!(plan.next_job(), None);
        assert_eq!(plan.earliest_timer(), None);
        assert_eq!(plan.live().count(), 0);
        assert!(!plan.machine.holds(&SessionId(A.to_string())));
        // Late answers for the ended session are ignored, not resurrected.
        plan.facts(A, facts(), now);
        plan.cursor(A, Some(read()), now);
        plan.write_done(A, Purpose::Ring, true, now);
        assert_eq!(plan.live().count(), 0);
    }

    #[test]
    fn a_session_that_is_gone_when_its_facts_are_read_ends_the_ring() {
        let mut plan = Plan::default();
        plan.request_ring(A, "hello".to_string());
        let Some(Job::Edges { text, .. }) = plan.next_job() else {
            panic!("edges job");
        };
        plan.ring_facts(A, text, None, Instant::now());
        assert_eq!(plan.live().count(), 0);
        assert_eq!(plan.next_job(), None);
    }

    #[test]
    fn a_refused_job_keeps_its_place_unless_its_session_ended() {
        let mut plan = Plan::default();
        plan.request_ring(A, "a".to_string());
        plan.request_ring(B, "b".to_string());
        let first = plan.next_job().expect("first job");
        plan.requeue_front(first.clone());
        assert_eq!(plan.next_job(), Some(first), "back at the front");
        plan.ended(B);
        plan.requeue_front(Job::Cursor {
            session: B.to_string(),
        });
        assert_eq!(plan.next_job(), None, "an ended session's job is dropped");
    }

    #[test]
    fn facts_hold_the_gate_until_the_first_mode_report_and_carry_the_width() {
        let mut edges = SessionEdges {
            modes_epoch: 3,
            mode_flags: None,
            output_seq: 5,
            input_seq: 7,
            composing: true,
            size: botster_core::ResizePayload {
                rows: 24,
                cols: 120,
            },
        };
        let unknown = facts_of(&edges);
        assert!(
            !unknown.cursor_visible,
            "no mode report yet: the gate holds"
        );
        assert_eq!(
            (unknown.modes_epoch, unknown.output_seq, unknown.input_seq),
            (3, 5, 7)
        );
        assert!(unknown.composing);
        assert_eq!(unknown.cols, 120);
        edges.mode_flags = Some(botster_core::ModeFlags {
            cursor_visible: true,
            bracketed_paste: true,
            kitty_enabled: true,
            ..botster_core::ModeFlags::default()
        });
        let known = facts_of(&edges);
        assert!(known.cursor_visible && known.bracketed_paste && known.kitty_enabled);
    }
}
