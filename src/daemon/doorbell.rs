//! The doorbell: when it is safe to type a ring into a session.
//!
//! A ring is a short text (from the messaging plugin) that the Hub types into
//! a session's terminal so an agent notices new mail. Typing into a terminal
//! is only safe when the cursor sits in an empty prompt that no human is
//! using, and no agent CLI can be asked about that. So the Hub asks the
//! terminal model: it types the probe `zx`, waits for the echo, erases it,
//! and only then types the ring. This module is that state machine and
//! nothing else. It does no I/O and reads no clock: the owner feeds it
//! events with the time, and executes the effects it returns.
//!
//! The gate, checked in order before a probe (all from Core's facts):
//! 1. a ring is pending;
//! 2. the cursor is visible (every dialog of the measured agent CLIs hides it);
//! 3. the client is not composing (Core's rule: bytes since the last CR);
//! 4. the last client input is at least [`QUIET_PERIOD`] old (ONE wake at
//!    `last_input + QUIET_PERIOD`, re-armed by each new input).
//!
//! A probe attempt: read the cursor (baseline); write `zx`; on each later
//! output edge read the cursor again. Echo means the same row, the column two
//! cells further, and text before the cursor ending in `zx`: decided from the
//! terminal model, never from raw bytes. On echo: erase (two backspaces in the
//! session's keyboard encoding), confirm the cursor is back at the baseline,
//! then type the ring in ONE write. With no echo by [`ECHO_DEADLINE`] nothing is
//! erased (backspaces could act on a dialog); the machine keeps watching output
//! edges, without a timer, so a late echo is erased when it shows. A retry
//! starts only on an edge: a mode change, client input, a new ring, or the
//! quiet wake. There is no polling.
//!
//! Accepted residual risks (user decision 2026-09-28): a free-text field
//! inside a dialog echoes like a composer, and a vim-style normal mode treats
//! `z` and `x` as commands. One more, accepted by the orchestrator on
//! 2026-09-29 (`orchestrator-user-decisions.md`, "Doorbell residual risks"): a
//! human who types between the probe and its erase leaves `zx` inside the
//! draft. The stray-probe cleanup only sees a trailing `zx`; the window is at
//! most [`ECHO_DEADLINE`] plus the erase round trip. The human's input edge
//! abandons the attempt at once and the machine sends NO backspaces, because
//! erasing would edit a draft the human is typing.
//!
//! A probe that would wrap at the last column is not a risk: with the cursor
//! in the last two columns the machine does not probe at all, and waits for
//! the next screen edge like any other not-safe state.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use botster_core::SessionId;

/// How long the client must be quiet before a probe.
// timer: ui-lifetime — human-input quiet period, user decision 2026-09-28.
pub(crate) const QUIET_PERIOD: Duration = Duration::from_secs(5);

/// How long the probe's echo may take. It bounds only the "no echo" path; a
/// real echo ends the wait on its own output edge.
// timer: deadline — no echo means a dialog or a busy agent; expiry is the exceptional path.
pub(crate) const ECHO_DEADLINE: Duration = Duration::from_millis(500);

/// The probe: two letters that a composer echoes and a dialog does not.
const PROBE: &[u8] = b"zx";
const PROBE_TEXT: &str = "zx";

/// What Core knows about one session, as of one edge (`session_edges`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Facts {
    pub(crate) modes_epoch: u64,
    pub(crate) output_seq: u64,
    pub(crate) input_seq: u64,
    /// Core's rule: bytes since the last client CR.
    pub(crate) composing: bool,
    pub(crate) cursor_visible: bool,
    pub(crate) bracketed_paste: bool,
    pub(crate) kitty_enabled: bool,
    /// The terminal's width in cells (the session's size), so the machine can
    /// tell that a probe would not fit before the last column.
    pub(crate) cols: u16,
}

/// One cursor read, from one read of the terminal model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Read {
    pub(crate) row: u16,
    /// A cell column, so a wide character counts for its cells.
    pub(crate) col: u16,
    /// The cells left of the cursor, untrimmed; each wide character once.
    pub(crate) text_before_cursor: String,
}

/// What the owner tells the machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Event {
    /// A ring for the session, with Core's facts as of the ring and the time
    /// the owner last saw the client's input advance (`None`: never). The owner
    /// tracks that time for every session from the input edges. A later ring
    /// replaces an earlier one.
    Ring {
        text: String,
        facts: Facts,
        last_input_at: Option<Instant>,
    },
    /// Core's facts after an edge (modes, output or input advanced). Events
    /// for a session with no ring are ignored: the owner needs to send them
    /// only while a ring waits or an attempt is under way.
    Facts(Facts),
    /// The answer to an issued [`Effect::ReadCursor`].
    Cursor(Read),
    /// A cursor read failed. The machine stops the attempt and waits for an edge.
    CursorFailed,
    /// The quiet wake armed by [`Effect::ArmQuietWake`] fired.
    QuietWake,
    /// The echo deadline armed by [`Effect::ArmEchoDeadline`] fired.
    EchoDeadline,
    /// The ring's write completed.
    Delivered,
    /// The ring's write failed. The ring is pending again.
    DeliveryFailed,
    /// The session ended.
    Ended,
}

/// What the owner must do for the machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Effect {
    /// Issue a cursor read; answer with [`Event::Cursor`] or [`Event::CursorFailed`].
    ReadCursor,
    /// Write these bytes to the session (a guarded host write).
    Write(Vec<u8>),
    /// Wake the machine with [`Event::QuietWake`] at this time. It REPLACES
    /// any earlier quiet wake of the session.
    ArmQuietWake(Instant),
    /// Wake the machine with [`Event::EchoDeadline`] at this time.
    ArmEchoDeadline(Instant),
    /// The echo arrived: drop the deadline.
    CancelEchoDeadline,
}

/// What follows a confirmed erase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum After {
    /// The probe echoed: type the ring.
    Ring,
    /// A stray probe was cleared from the baseline: this attempt's probe follows.
    Probe,
    /// A late echo was cleared: the ring waits for the next edge.
    Wait,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    Idle,
    /// A cursor read is out for the baseline.
    Baseline,
    /// The probe is written; waiting for an output edge.
    Probing {
        baseline: Read,
        seen_output: u64,
    },
    /// A cursor read is out after an output edge while probing.
    Verifying {
        baseline: Read,
    },
    /// The deadline passed with no echo. No timer; a late echo is erased.
    Late {
        baseline: Read,
        seen_output: u64,
    },
    /// A cursor read is out after an output edge while watching late.
    LateVerifying {
        baseline: Read,
    },
    /// The probe is erased; waiting for an output edge, then a read that
    /// shows the cursor back at the baseline. `after` says what follows.
    Erasing {
        baseline: Read,
        seen_output: u64,
        after: After,
    },
    /// A cursor read is out to confirm the erase.
    ErasingVerifying {
        baseline: Read,
        after: After,
    },
    /// The ring's write is out.
    Delivering {
        text: String,
    },
}

#[derive(Debug, Clone)]
struct Ring {
    pending: Option<String>,
    facts: Option<Facts>,
    last_input_at: Option<Instant>,
    phase: Phase,
    /// The last baseline read found the cursor in the last two columns, so
    /// nothing was typed. Only output can move the cursor there, so output is
    /// an edge that retries this ring (and only this kind of refusal).
    margin: bool,
}

impl Ring {
    fn new() -> Self {
        Self {
            pending: None,
            facts: None,
            last_input_at: None,
            phase: Phase::Idle,
            margin: false,
        }
    }
}

/// The doorbell state of every session that has a ring.
#[derive(Debug, Default)]
pub(crate) struct Doorbell {
    sessions: HashMap<String, Ring>,
}

impl Doorbell {
    /// Sessions the machine holds state for, for tests and diagnostics.
    #[cfg(test)]
    pub(crate) fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// True when a ring waits for the session.
    #[cfg(test)]
    pub(crate) fn is_pending(&self, session: &SessionId) -> bool {
        self.sessions
            .get(&session.0)
            .is_some_and(|ring| ring.pending.is_some())
    }

    /// True when the machine holds state for the session: a ring waits or an
    /// attempt is under way.
    pub(crate) fn holds(&self, session: &SessionId) -> bool {
        self.sessions.contains_key(&session.0)
    }

    /// Feed one event for one session at `now`; execute the effects returned.
    pub(crate) fn step(&mut self, session: &SessionId, now: Instant, event: Event) -> Vec<Effect> {
        let mut effects = Vec::new();
        if event == Event::Ended {
            if self.sessions.remove(&session.0).is_some() {
                effects.push(Effect::CancelEchoDeadline);
            }
            return effects;
        }
        let ring = match event {
            Event::Ring { .. } => self
                .sessions
                .entry(session.0.clone())
                .or_insert_with(Ring::new),
            _ => match self.sessions.get_mut(&session.0) {
                Some(ring) => ring,
                None => return effects,
            },
        };
        match event {
            Event::Ended => unreachable!("handled above"),
            Event::Ring {
                text,
                facts,
                last_input_at,
            } => {
                ring.facts = Some(facts);
                ring.last_input_at = last_input_at.or(ring.last_input_at);
                ring.pending = Some(text);
                // A new ring is an edge: a late attempt may start over.
                if matches!(ring.phase, Phase::Late { .. }) {
                    ring.phase = Phase::Idle;
                }
                try_start(ring, now, &mut effects);
            }
            Event::Facts(facts) => on_facts(ring, facts, now, &mut effects),
            Event::Cursor(read) => on_cursor(ring, read, now, &mut effects),
            Event::CursorFailed => {
                // Nothing was decided: stop, keep the ring, wait for an edge.
                ring.phase = Phase::Idle;
                effects.push(Effect::CancelEchoDeadline);
            }
            Event::QuietWake => {
                if matches!(ring.phase, Phase::Late { .. }) {
                    ring.phase = Phase::Idle;
                }
                try_start(ring, now, &mut effects);
            }
            Event::EchoDeadline => on_deadline(ring),
            Event::Delivered => {
                if matches!(ring.phase, Phase::Delivering { .. }) {
                    ring.phase = Phase::Idle;
                    // A ring that arrived while this one was typed waits for an edge.
                }
            }
            Event::DeliveryFailed => {
                if let Phase::Delivering { text } = std::mem::replace(&mut ring.phase, Phase::Idle)
                {
                    ring.pending.get_or_insert(text);
                }
            }
        }
        // A session with nothing pending and nothing in flight holds no state.
        if ring.pending.is_none() && matches!(ring.phase, Phase::Idle) {
            self.sessions.remove(&session.0);
        }
        effects
    }
}

/// The erase: two backspaces in the session's current keyboard encoding.
fn erase_bytes(kitty_enabled: bool) -> Vec<u8> {
    if kitty_enabled {
        b"\x1b[127u\x1b[127u".to_vec()
    } else {
        vec![0x7f, 0x7f]
    }
}

/// What one of the machine's writes is for, so the owner can tell how a
/// failed write changes the attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Purpose {
    /// The `zx` probe. A refused probe stops the attempt: nothing was decided.
    Probe,
    /// The erase of a probe. A refused erase stops the attempt too; the next
    /// attempt's baseline read finds and erases the stray probe.
    Erase,
    /// The ring. A refused ring is pending again ([`Event::DeliveryFailed`]).
    Ring,
}

/// Tell what a write returned by the machine is for. The three encodings
/// cannot be confused: a ring always ends in CR (whatever its text), the probe
/// is exactly `zx`, and an erase is two backspaces in either keyboard encoding.
pub(crate) fn write_purpose(bytes: &[u8]) -> Purpose {
    if bytes.last() == Some(&b'\r') {
        Purpose::Ring
    } else if bytes == PROBE {
        Purpose::Probe
    } else {
        Purpose::Erase
    }
}

/// The ring text as a terminal may safely receive it. The text comes from a
/// plugin, so no byte of it may act as terminal input: a CR would submit, an
/// ESC could open a sequence or end the paste early (`ESC [ 201 ~`). Line
/// breaks and tabs become spaces; every other control character (C0, DEL, C1)
/// is dropped, so the paste-end sequence loses its ESC and stays plain text.
fn safe_text(text: &str) -> String {
    text.chars()
        .filter_map(|letter| match letter {
            '\n' | '\r' | '\t' => Some(' '),
            letter if letter.is_control() => None,
            letter => Some(letter),
        })
        .collect()
}

/// The ring: bracketed paste when the session has it on, then a CR, in one write.
fn delivery_bytes(text: &str, bracketed_paste: bool) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(text.len() + 14);
    if bracketed_paste {
        bytes.extend_from_slice(b"\x1b[200~");
    }
    bytes.extend_from_slice(safe_text(text).as_bytes());
    if bracketed_paste {
        bytes.extend_from_slice(b"\x1b[201~");
    }
    bytes.push(b'\r');
    bytes
}

/// The probe echoed: same row, two cells further, and `zx` before the cursor.
fn echoed(baseline: &Read, read: &Read) -> bool {
    read.row == baseline.row
        && read.col == baseline.col.saturating_add(2)
        && read.text_before_cursor.ends_with(PROBE_TEXT)
}

/// The cursor is back where the baseline had it.
fn at_baseline(baseline: &Read, read: &Read) -> bool {
    read.row == baseline.row && read.col == baseline.col
}

/// The cursor sits in the last two columns of its row, so the two-cell probe
/// would not fit before the last one.
fn last_columns(read: &Read, facts: &Facts) -> bool {
    read.col.saturating_add(PROBE.len() as u16) >= facts.cols
}

/// Check the gate; start a baseline read, arm the one quiet wake, or wait for
/// an edge.
fn try_start(ring: &mut Ring, now: Instant, effects: &mut Vec<Effect>) {
    if !matches!(ring.phase, Phase::Idle) || ring.pending.is_none() {
        return;
    }
    let Some(facts) = &ring.facts else {
        return;
    };
    if !facts.cursor_visible || facts.composing {
        return;
    }
    if let Some(last_input) = ring.last_input_at {
        let ready_at = last_input + QUIET_PERIOD;
        if now < ready_at {
            effects.push(Effect::ArmQuietWake(ready_at));
            return;
        }
    }
    // This attempt decides the margin question afresh: a read that fails
    // leaves the ring under the failure rule (an edge, not output, retries).
    ring.margin = false;
    ring.phase = Phase::Baseline;
    effects.push(Effect::ReadCursor);
}

fn on_facts(ring: &mut Ring, facts: Facts, now: Instant, effects: &mut Vec<Effect>) {
    let previous = ring.facts.replace(facts.clone());
    let input_changed = previous
        .as_ref()
        .is_some_and(|previous| previous.input_seq != facts.input_seq);
    let modes_changed = previous
        .as_ref()
        .is_some_and(|previous| previous.modes_epoch != facts.modes_epoch);
    let output_changed = previous
        .as_ref()
        .is_some_and(|previous| previous.output_seq != facts.output_seq);
    if input_changed {
        ring.last_input_at = Some(now);
        // A human is typing: abandon the attempt. A probe left in the composer
        // is erased by the next attempt's baseline check.
        let in_flight = !matches!(ring.phase, Phase::Idle | Phase::Delivering { .. });
        if in_flight {
            ring.phase = Phase::Idle;
            effects.push(Effect::CancelEchoDeadline);
        }
    }
    // A retry starts only on an edge that can change the gate: a mode change
    // (the cursor reappears) or client input (a CR ends composing). Output
    // alone never starts an attempt; it only advances one that is under way,
    // with one exception: a ring refused at the last columns typed nothing,
    // and only output moves the cursor out of them.
    let retry_edge = input_changed || modes_changed;
    match ring.phase.clone() {
        Phase::Idle => {
            if retry_edge || (ring.margin && output_changed) {
                try_start(ring, now, effects);
            }
        }
        Phase::Probing {
            baseline,
            seen_output,
        } if facts.output_seq > seen_output => {
            ring.phase = Phase::Verifying { baseline };
            effects.push(Effect::ReadCursor);
        }
        Phase::Late {
            baseline,
            seen_output,
        } => {
            if retry_edge {
                // A mode change or client input is the next idle point.
                ring.phase = Phase::Idle;
                try_start(ring, now, effects);
            } else if facts.output_seq > seen_output {
                ring.phase = Phase::LateVerifying { baseline };
                effects.push(Effect::ReadCursor);
            }
        }
        Phase::Erasing {
            baseline,
            seen_output,
            after,
        } if facts.output_seq > seen_output => {
            ring.phase = Phase::ErasingVerifying { baseline, after };
            effects.push(Effect::ReadCursor);
        }
        _ => {}
    }
}

fn on_cursor(ring: &mut Ring, read: Read, now: Instant, effects: &mut Vec<Effect>) {
    let Some(facts) = ring.facts.clone() else {
        ring.phase = Phase::Idle;
        return;
    };
    match ring.phase.clone() {
        Phase::Baseline => {
            // Re-check the gate: facts may have moved while the read was out.
            if !facts.cursor_visible || facts.composing || ring.pending.is_none() {
                ring.phase = Phase::Idle;
                return;
            }
            if last_columns(&read, &facts) {
                // The probe would reach the last cell, where a terminal holds
                // the cursor and then wraps: it could not be told from a
                // missing echo, and a stray `zx` would follow. Not safe: type
                // nothing and wait for the next screen edge, like any other
                // not-safe state, except that output also retries it: only
                // output moves the cursor out of the last columns.
                ring.margin = true;
                ring.phase = Phase::Idle;
                return;
            }
            if read.text_before_cursor.ends_with(PROBE_TEXT) {
                // A stray probe from an attempt that got no echo in time: erase
                // it first, so probes never pile up in a composer.
                ring.phase = Phase::Erasing {
                    baseline: Read {
                        col: read.col.saturating_sub(2),
                        text_before_cursor: read
                            .text_before_cursor
                            .trim_end_matches(PROBE_TEXT)
                            .to_string(),
                        ..read
                    },
                    seen_output: facts.output_seq,
                    after: After::Probe,
                };
                effects.push(Effect::Write(erase_bytes(facts.kitty_enabled)));
                return;
            }
            ring.phase = Phase::Probing {
                baseline: read,
                seen_output: facts.output_seq,
            };
            effects.push(Effect::Write(PROBE.to_vec()));
            effects.push(Effect::ArmEchoDeadline(now + ECHO_DEADLINE));
        }
        Phase::Verifying { baseline } => {
            if echoed(&baseline, &read) {
                effects.push(Effect::CancelEchoDeadline);
                ring.phase = Phase::Erasing {
                    baseline,
                    seen_output: facts.output_seq,
                    after: After::Ring,
                };
                effects.push(Effect::Write(erase_bytes(facts.kitty_enabled)));
            } else {
                // Not yet: wait for the next output edge (the deadline bounds this).
                ring.phase = Phase::Probing {
                    baseline,
                    seen_output: facts.output_seq,
                };
            }
        }
        Phase::LateVerifying { baseline } => {
            if echoed(&baseline, &read) {
                // A late echo: erase it now. The ring waits for the next attempt.
                ring.phase = Phase::Erasing {
                    baseline,
                    seen_output: facts.output_seq,
                    after: After::Wait,
                };
                effects.push(Effect::Write(erase_bytes(facts.kitty_enabled)));
            } else {
                ring.phase = Phase::Late {
                    baseline,
                    seen_output: facts.output_seq,
                };
            }
        }
        Phase::ErasingVerifying { baseline, after } => {
            if !at_baseline(&baseline, &read) {
                ring.phase = Phase::Erasing {
                    baseline,
                    seen_output: facts.output_seq,
                    after,
                };
                return;
            }
            match after {
                After::Ring => {
                    if let Some(text) = ring.pending.take() {
                        effects.push(Effect::Write(delivery_bytes(&text, facts.bracketed_paste)));
                        ring.phase = Phase::Delivering { text };
                    } else {
                        ring.phase = Phase::Idle;
                    }
                }
                After::Probe => {
                    // The composer is clean: this attempt goes on to its probe.
                    ring.phase = Phase::Probing {
                        baseline,
                        seen_output: facts.output_seq,
                    };
                    effects.push(Effect::Write(PROBE.to_vec()));
                    effects.push(Effect::ArmEchoDeadline(now + ECHO_DEADLINE));
                }
                // A late echo was erased: the ring waits for the next edge.
                After::Wait => ring.phase = Phase::Idle,
            }
        }
        // A read nobody asked for: ignore it.
        _ => {}
    }
}

fn on_deadline(ring: &mut Ring) {
    match ring.phase.clone() {
        Phase::Probing {
            baseline,
            seen_output,
        } => {
            // No echo: erase nothing. Keep watching output edges, with no timer.
            ring.phase = Phase::Late {
                baseline,
                seen_output,
            };
        }
        Phase::Verifying { baseline } => {
            // The read that decides is out; its answer still counts as late.
            ring.phase = Phase::LateVerifying { baseline };
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests;
