//! The doorbell state machine against a fake terminal and a fake clock.
//!
//! The rig plays the owner: it executes the machine's effects against a
//! terminal model (a composer, or a dialog that ignores input), reports the
//! edges Core would report, and fires the timers the machine armed.

use super::*;

const SESSION: &str = "sess-a";

/// How the fake terminal treats typed bytes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Behaviour {
    /// A composer: typed letters echo at the cursor; backspaces erase them.
    Composer,
    /// A dialog that ignores input: nothing echoes, nothing changes.
    Ignores,
    /// A slow composer: the echo shows only when `release_echo` is called.
    SlowEcho,
}

struct Terminal {
    behaviour: Behaviour,
    text: String,
    row: u16,
    col: u16,
    cursor_visible: bool,
    composing: bool,
    bracketed_paste: bool,
    kitty_enabled: bool,
    output_seq: u64,
    input_seq: u64,
    modes_epoch: u64,
    /// Every write the machine made, in order.
    writes: Vec<Vec<u8>>,
    /// Texts that reached the composer as a submitted line.
    submitted: Vec<String>,
    held_echo: bool,
}

impl Terminal {
    fn composer() -> Self {
        Self {
            behaviour: Behaviour::Composer,
            text: "> ".to_string(),
            row: 10,
            col: 2,
            cursor_visible: true,
            composing: false,
            bracketed_paste: true,
            kitty_enabled: false,
            output_seq: 0,
            input_seq: 0,
            modes_epoch: 0,
            writes: Vec::new(),
            submitted: Vec::new(),
            held_echo: false,
        }
    }

    fn facts(&self) -> Facts {
        Facts {
            modes_epoch: self.modes_epoch,
            output_seq: self.output_seq,
            input_seq: self.input_seq,
            composing: self.composing,
            cursor_visible: self.cursor_visible,
            bracketed_paste: self.bracketed_paste,
            kitty_enabled: self.kitty_enabled,
        }
    }

    fn read(&self) -> Read {
        Read {
            row: self.row,
            col: self.col,
            text_before_cursor: self.text.clone(),
        }
    }

    fn type_letters(&mut self, letters: &str) {
        for letter in letters.chars() {
            self.text.push(letter);
            self.col += width(letter);
        }
        self.output_seq += 1;
    }

    fn backspace(&mut self, count: usize) {
        for _ in 0..count {
            if let Some(letter) = self.text.pop() {
                self.col -= width(letter);
            }
        }
        self.output_seq += 1;
    }

    /// A host write from the machine: the terminal's reaction.
    fn write(&mut self, bytes: &[u8]) {
        self.writes.push(bytes.to_vec());
        if self.behaviour == Behaviour::Ignores {
            return;
        }
        if bytes == PROBE {
            if self.behaviour == Behaviour::SlowEcho {
                self.held_echo = true;
            } else {
                self.type_letters(PROBE_TEXT);
            }
        } else if bytes == [0x7f, 0x7f] || bytes == b"\x1b[127u\x1b[127u" {
            self.backspace(2);
        } else {
            // The ring: bracketed paste or plain text, ended by a CR.
            let text = String::from_utf8_lossy(bytes);
            let text = text
                .trim_start_matches("\x1b[200~")
                .replace("\x1b[201~", "")
                .trim_end_matches('\r')
                .to_string();
            self.submitted.push(text);
            self.output_seq += 1;
        }
    }

    /// The slow composer finally shows the probe.
    fn release_echo(&mut self) {
        if self.held_echo {
            self.held_echo = false;
            self.type_letters(PROBE_TEXT);
        }
    }

    /// A human types: input advances; a CR ends composing.
    fn human_types(&mut self, letters: &str) {
        self.input_seq += 1;
        self.composing = !letters.ends_with('\r');
        if letters != "\r" {
            self.type_letters(letters);
        } else {
            self.output_seq += 1;
        }
    }
}

/// A wide character takes two cells.
fn width(letter: char) -> u16 {
    if letter.len_utf8() > 1 { 2 } else { 1 }
}

struct Rig {
    doorbell: Doorbell,
    session: SessionId,
    terminal: Terminal,
    now: Instant,
    quiet_wake: Option<Instant>,
    echo_deadline: Option<Instant>,
    reported: Option<Facts>,
    /// When the rig last saw the client's input advance, like the owner does.
    input_at: Option<Instant>,
    read_out: bool,
    /// Every effect the machine returned, in order.
    log: Vec<Effect>,
}

impl Rig {
    fn new(terminal: Terminal) -> Self {
        Self {
            doorbell: Doorbell::new(),
            session: SessionId(SESSION.to_string()),
            terminal,
            now: Instant::now(),
            quiet_wake: None,
            echo_deadline: None,
            reported: None,
            input_at: None,
            read_out: false,
            log: Vec::new(),
        }
    }

    fn feed(&mut self, event: Event) {
        let effects = self.doorbell.step(&self.session, self.now, event);
        for effect in effects {
            self.log.push(effect.clone());
            match effect {
                Effect::ReadCursor => self.read_out = true,
                Effect::Write(bytes) => self.terminal.write(&bytes),
                Effect::ArmQuietWake(at) => self.quiet_wake = Some(at),
                Effect::ArmEchoDeadline(at) => self.echo_deadline = Some(at),
                Effect::CancelEchoDeadline => self.echo_deadline = None,
            }
        }
    }

    /// Core reports edges and answers reads until nothing changes.
    fn settle(&mut self) {
        for _ in 0..64 {
            let facts = self.terminal.facts();
            if self.reported.as_ref() != Some(&facts) {
                if self
                    .reported
                    .as_ref()
                    .is_none_or(|reported| reported.input_seq != facts.input_seq)
                    && facts.input_seq > 0
                {
                    self.input_at = Some(self.now);
                }
                self.reported = Some(facts.clone());
                self.feed(Event::Facts(facts));
                continue;
            }
            if self.read_out {
                self.read_out = false;
                let read = self.terminal.read();
                self.feed(Event::Cursor(read));
                continue;
            }
            return;
        }
        panic!("the rig did not settle");
    }

    /// A ring arrives with Core's facts as of now.
    fn ring(&mut self, text: &str) {
        let facts = self.terminal.facts();
        self.reported = Some(facts.clone());
        self.feed(Event::Ring {
            text: text.to_string(),
            facts,
            last_input_at: self.input_at,
        });
        self.settle();
    }

    fn advance(&mut self, by: Duration) {
        self.now += by;
        if let Some(at) = self.quiet_wake
            && at <= self.now
        {
            self.quiet_wake = None;
            self.feed(Event::QuietWake);
        }
        if let Some(at) = self.echo_deadline
            && at <= self.now
        {
            self.echo_deadline = None;
            self.feed(Event::EchoDeadline);
        }
        self.settle();
    }

    fn writes_of(&self, bytes: &[u8]) -> usize {
        self.terminal
            .writes
            .iter()
            .filter(|write| write.as_slice() == bytes)
            .count()
    }

    fn quiet_wakes_armed(&self) -> Vec<Instant> {
        self.log
            .iter()
            .filter_map(|effect| match effect {
                Effect::ArmQuietWake(at) => Some(*at),
                _ => None,
            })
            .collect()
    }

    fn pending(&self) -> bool {
        self.doorbell.is_pending(&self.session)
    }
}

#[test]
fn an_idle_composer_gets_the_probe_erased_and_then_the_ring() {
    let mut rig = Rig::new(Terminal::composer());
    rig.ring("botster: you have mail");
    // The probe was typed, echoed, erased; the ring came in one paste, and
    // the composer holds nothing of the probe.
    assert_eq!(rig.writes_of(PROBE), 1);
    assert_eq!(rig.writes_of(&[0x7f, 0x7f]), 1);
    assert_eq!(rig.terminal.submitted, ["botster: you have mail"]);
    assert_eq!(rig.terminal.text, "> ");
    let last = rig.terminal.writes.last().unwrap();
    assert_eq!(
        last.as_slice(),
        b"\x1b[200~botster: you have mail\x1b[201~\r"
    );
    // The echo was seen before the deadline, so the deadline was dropped.
    assert!(rig.echo_deadline.is_none());
    rig.feed(Event::Delivered);
    assert!(!rig.pending());
    assert_eq!(
        rig.doorbell.session_count(),
        0,
        "a finished ring holds no state"
    );
}

#[test]
fn without_bracketed_paste_the_ring_is_plain_text_and_a_cr() {
    let mut terminal = Terminal::composer();
    terminal.bracketed_paste = false;
    let mut rig = Rig::new(terminal);
    rig.ring("hello");
    assert_eq!(rig.terminal.writes.last().unwrap().as_slice(), b"hello\r");
}

#[test]
fn a_kitty_session_is_erased_with_the_kitty_backspace() {
    let mut terminal = Terminal::composer();
    terminal.kitty_enabled = true;
    let mut rig = Rig::new(terminal);
    rig.ring("hello");
    assert_eq!(rig.writes_of(b"\x1b[127u\x1b[127u"), 1);
    assert_eq!(rig.writes_of(&[0x7f, 0x7f]), 0);
    assert_eq!(rig.terminal.submitted, ["hello"]);
}

#[test]
fn nothing_is_typed_while_the_client_is_composing() {
    let mut terminal = Terminal::composer();
    terminal.composing = true;
    let mut rig = Rig::new(terminal);
    rig.ring("hello");
    assert!(rig.terminal.writes.is_empty(), "the probe ran over a draft");
    assert!(rig.pending());
    // The human submits: composing ends; after the quiet period the ring goes.
    rig.terminal.human_types("\r");
    rig.settle();
    assert!(
        rig.terminal.writes.is_empty(),
        "the quiet period has not passed"
    );
    rig.advance(QUIET_PERIOD);
    assert_eq!(rig.terminal.submitted, ["hello"]);
}

#[test]
fn one_quiet_wake_is_armed_and_each_new_input_replaces_it() {
    let mut terminal = Terminal::composer();
    // Composing false, so only the quiet period holds the ring back.
    terminal.input_seq = 0;
    let mut rig = Rig::new(terminal);
    rig.settle();
    rig.terminal.human_types("\r");
    rig.settle();
    rig.ring("hello");
    let first = rig.quiet_wake.expect("a quiet wake is armed");
    assert_eq!(first, rig.now + QUIET_PERIOD);
    // Input again, later: the wake moves; it is not added.
    rig.advance(Duration::from_secs(2));
    rig.terminal.human_types("\r");
    rig.settle();
    let second = rig.quiet_wake.expect("the wake was re-armed");
    assert_eq!(second, rig.now + QUIET_PERIOD);
    assert!(second > first);
    // The first instant passes with nothing typed: the wake was replaced.
    rig.advance(Duration::from_secs(3));
    assert!(rig.terminal.writes.is_empty(), "the old wake fired");
    rig.advance(QUIET_PERIOD);
    assert_eq!(rig.terminal.submitted, ["hello"]);
    assert!(rig.quiet_wakes_armed().len() >= 2);
}

#[test]
fn a_hidden_cursor_holds_the_ring_until_it_reappears() {
    let mut terminal = Terminal::composer();
    terminal.cursor_visible = false;
    let mut rig = Rig::new(terminal);
    rig.ring("hello");
    assert!(
        rig.terminal.writes.is_empty(),
        "the probe ran under a dialog"
    );
    // The dialog closes: the mode edge is the retry.
    rig.terminal.cursor_visible = true;
    rig.terminal.modes_epoch += 1;
    rig.settle();
    assert_eq!(rig.terminal.submitted, ["hello"]);
}

#[test]
fn a_free_text_field_that_echoes_takes_the_ring() {
    // The accepted residual risk: a text field inside a dialog echoes like a
    // composer and shows the cursor, so the doorbell cannot tell it apart.
    let mut terminal = Terminal::composer();
    terminal.text = "Other: ".to_string();
    terminal.col = 7;
    let mut rig = Rig::new(terminal);
    rig.ring("hello");
    assert_eq!(rig.terminal.submitted, ["hello"]);
}

#[test]
fn a_dialog_that_ignores_input_gets_the_probe_and_nothing_else() {
    let mut terminal = Terminal::composer();
    terminal.behaviour = Behaviour::Ignores;
    let mut rig = Rig::new(terminal);
    rig.ring("hello");
    assert_eq!(rig.terminal.writes, [PROBE.to_vec()]);
    // The deadline passes with no echo: nothing is erased, nothing is typed.
    rig.advance(ECHO_DEADLINE);
    assert_eq!(rig.terminal.writes, [PROBE.to_vec()]);
    assert!(rig.pending(), "the ring waits");
    // Time alone changes nothing: a retry starts only on an edge.
    rig.advance(Duration::from_secs(60));
    assert_eq!(rig.terminal.writes, [PROBE.to_vec()]);
    // A mode edge is the next idle point: the probe runs again.
    rig.terminal.modes_epoch += 1;
    rig.settle();
    assert_eq!(rig.writes_of(PROBE), 2);
}

#[test]
fn a_late_echo_is_erased_when_it_shows_and_the_ring_waits_for_an_edge() {
    let mut terminal = Terminal::composer();
    terminal.behaviour = Behaviour::SlowEcho;
    let mut rig = Rig::new(terminal);
    rig.ring("hello");
    rig.advance(ECHO_DEADLINE);
    assert_eq!(
        rig.writes_of(&[0x7f, 0x7f]),
        0,
        "nothing is erased at the deadline"
    );
    // The composer finally echoes the probe; the output edge finds it.
    rig.terminal.release_echo();
    rig.settle();
    assert_eq!(rig.writes_of(&[0x7f, 0x7f]), 1, "the late echo was erased");
    assert_eq!(rig.terminal.text, "> ");
    assert!(rig.terminal.submitted.is_empty(), "no ring on a late echo");
    assert!(rig.pending());
    // The next edge starts a fresh attempt on the clean composer.
    rig.terminal.behaviour = Behaviour::Composer;
    rig.terminal.modes_epoch += 1;
    rig.settle();
    assert_eq!(rig.terminal.submitted, ["hello"]);
}

#[test]
fn a_stray_probe_left_in_the_composer_is_erased_before_a_new_probe() {
    let mut terminal = Terminal::composer();
    terminal.text = "> zx".to_string();
    terminal.col = 4;
    let mut rig = Rig::new(terminal);
    rig.ring("hello");
    // The first write erases the leftover; only then is a probe typed.
    assert_eq!(rig.terminal.writes[0], vec![0x7f, 0x7f]);
    assert_eq!(
        rig.writes_of(PROBE),
        1,
        "writes {:?} log {:?}",
        rig.terminal.writes,
        rig.log
    );
    assert_eq!(rig.terminal.submitted, ["hello"]);
    assert_eq!(rig.terminal.text, "> ");
}

#[test]
fn a_human_who_starts_typing_ends_the_attempt() {
    let mut terminal = Terminal::composer();
    terminal.behaviour = Behaviour::SlowEcho;
    let mut rig = Rig::new(terminal);
    rig.ring("hello");
    assert_eq!(rig.writes_of(PROBE), 1);
    // A human types while the probe waits for its echo.
    rig.terminal.human_types("h");
    rig.settle();
    assert!(rig.echo_deadline.is_none(), "the attempt was dropped");
    assert!(rig.terminal.submitted.is_empty());
    assert!(rig.pending());
}

#[test]
fn a_wide_character_before_the_cursor_counts_cells_not_letters() {
    let mut terminal = Terminal::composer();
    terminal.text = "> 你".to_string();
    terminal.col = 4; // '>', ' ', and two cells for the wide character
    let mut rig = Rig::new(terminal);
    rig.ring("hello");
    assert_eq!(rig.terminal.submitted, ["hello"]);
    assert_eq!(rig.terminal.col, 4, "the cursor is back where it started");
}

#[test]
fn rings_for_one_session_coalesce_into_one_delivery_and_the_latest_text_wins() {
    let mut terminal = Terminal::composer();
    terminal.composing = true;
    let mut rig = Rig::new(terminal);
    rig.ring("first");
    rig.ring("second");
    rig.ring("second");
    // Composing ends; one attempt, one delivery, the latest text.
    rig.terminal.human_types("\r");
    rig.settle();
    rig.advance(QUIET_PERIOD);
    assert_eq!(rig.terminal.submitted, ["second"]);
    assert_eq!(rig.writes_of(PROBE), 1, "one probe for three rings");
}

#[test]
fn a_ring_that_could_not_be_written_is_pending_again() {
    let mut rig = Rig::new(Terminal::composer());
    rig.ring("hello");
    assert!(!rig.pending(), "the ring was taken for delivery");
    rig.feed(Event::DeliveryFailed);
    assert!(rig.pending(), "a failed delivery restores the ring");
}

#[test]
fn an_ended_session_drops_its_state_and_its_deadline() {
    let mut terminal = Terminal::composer();
    terminal.behaviour = Behaviour::Ignores;
    let mut rig = Rig::new(terminal);
    rig.ring("hello");
    assert!(rig.echo_deadline.is_some());
    rig.feed(Event::Ended);
    assert!(rig.echo_deadline.is_none());
    assert_eq!(rig.doorbell.session_count(), 0);
    // Later events for the id start from nothing.
    let writes = rig.terminal.writes.len();
    rig.advance(Duration::from_secs(60));
    assert_eq!(rig.terminal.writes.len(), writes);
}

#[test]
fn a_failed_cursor_read_stops_the_attempt_without_typing() {
    let mut rig = Rig::new(Terminal::composer());
    let facts = rig.terminal.facts();
    rig.reported = Some(facts.clone());
    rig.feed(Event::Ring {
        text: "hello".to_string(),
        facts,
        last_input_at: None,
    });
    assert!(rig.read_out);
    rig.read_out = false;
    rig.feed(Event::CursorFailed);
    assert!(rig.terminal.writes.is_empty());
    assert!(rig.pending());
}

#[test]
fn the_probe_echo_test_is_decided_from_the_model_never_from_bytes() {
    // The cursor moved two cells on the same row but the text before it does
    // not end in the probe: not an echo.
    let baseline = Read {
        row: 3,
        col: 2,
        text_before_cursor: "> ".to_string(),
    };
    let moved = Read {
        row: 3,
        col: 4,
        text_before_cursor: "> ab".to_string(),
    };
    assert!(!echoed(&baseline, &moved));
    // The right text on another row is not an echo either.
    let other_row = Read {
        row: 4,
        col: 4,
        text_before_cursor: "> zx".to_string(),
    };
    assert!(!echoed(&baseline, &other_row));
    let echo = Read {
        row: 3,
        col: 4,
        text_before_cursor: "> zx".to_string(),
    };
    assert!(echoed(&baseline, &echo));
}
