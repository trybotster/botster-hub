//! The owner thread's route tables (design sections 2.1 to 2.5; HC TS-1, TS-2, TS-9, TB-L2).
//!
//! - **Reservations**, keyed by `route_id`: `Reserved` until a valid connect consumes it or its deadline passes;
//!   `Consumed` until its deadline, so that a second presenter gets `route_token_used`. Both states count against the
//!   bound. The table is in memory only: a Hub restart drops it.
//! - **Routes**, keyed by `route_id` (the Core `route_tag`): inserted after a successful Core `attach` or from
//!   `RouteAdopted`, and removed only at Core `RouteClosed` for that tag.
//!
//! The owner calls every method in one turn with the `now` it read for that turn. Core is not called here: the owner
//! calls `attach` after [`RouteTables::connect`] returns [`Connect::Consumed`], in the same step.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use botster_hub_contract::prelude::{BusyData, Close, CloseReason, HubShutdownData, ServerFrame};
use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};
use subtle::ConstantTimeEq;

/// The random bytes of a route id (128 bits, design 2.2). A route id survives Hub restarts (HS-5, A.3), so it is never
/// a counter.
pub const ROUTE_ID_BYTES: usize = 16;

/// The random bytes of a route token (256 bits, design 2.2).
pub const ROUTE_TOKEN_BYTES: usize = 32;

/// The longest framed close that the route-connect path writes: the 4-byte prefix plus the JSON of
/// `close{busy{retry_after_ms: u64::MAX}}` (design 2.3). The `io` thread reserves it in the connection's outgoing bound
/// before `submit`. A test checks it against each of the four closes.
pub const MAX_ROUTE_CONNECT_CLOSE: usize = 78;

/// The framed `close { reason }` (TR-1 framing).
pub fn close_frame(reason: CloseReason) -> Vec<u8> {
    let json = serde_json::to_vec(&ServerFrame::Close(Close { reason }))
        .expect("a close frame always encodes");
    let len = u32::try_from(json.len()).expect("a close frame is small");
    let mut frame = Vec::with_capacity(4 + json.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&json);
    frame
}

/// The four closes of the route-connect path, at their widest values.
pub fn route_connect_closes() -> [CloseReason; 4] {
    [
        CloseReason::RouteTokenInvalid,
        CloseReason::RouteTokenUsed,
        CloseReason::Busy(BusyData {
            retry_after_ms: u64::MAX,
        }),
        CloseReason::HubShutdown(HubShutdownData {
            lock_released: false,
        }),
    ]
}

/// The OS CSPRNG, or a test's source.
pub trait Entropy {
    fn fill(&mut self, bytes: &mut [u8]) -> Result<(), EntropyError>;
}

/// The OS CSPRNG (`getrandom`).
#[derive(Debug, Default)]
pub struct OsEntropy;

impl Entropy for OsEntropy {
    fn fill(&mut self, bytes: &mut [u8]) -> Result<(), EntropyError> {
        getrandom::fill(bytes).map_err(|e| EntropyError(e.to_string()))
    }
}

/// The CSPRNG failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntropyError(pub String);

impl fmt::Display for EntropyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the OS random source failed: {}", self.0)
    }
}

impl std::error::Error for EntropyError {}

fn random_text(entropy: &mut impl Entropy, len: usize) -> Result<String, EntropyError> {
    let mut bytes = vec![0u8; len];
    entropy.fill(&mut bytes)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

/// A reservation that a valid connect consumed. The owner uses it for the Core `attach` of the same step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reserved<C, P> {
    pub caller: C,
    pub uid: u32,
    /// What the owner fixed at reservation: the session, the chosen format, the attach options and the R-119 limits.
    pub payload: P,
    pub deadline: Instant,
}

#[derive(Debug)]
enum Entry<C, P> {
    Reserved {
        token: String,
        reserved: Reserved<C, P>,
    },
    Consumed {
        deadline: Instant,
    },
}

impl<C, P> Entry<C, P> {
    fn deadline(&self) -> Instant {
        match self {
            Entry::Reserved { reserved, .. } => reserved.deadline,
            Entry::Consumed { deadline } => *deadline,
        }
    }
}

/// A route id and its token, as the `attach` answer carries them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Minted {
    pub route_id: String,
    pub route_token: String,
}

/// Why nothing was reserved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReserveError {
    /// The reservation table is full: `busy` (retryable), and nothing is minted.
    Busy,
    Entropy(EntropyError),
    /// The minted id is already a reservation or a route. With 128 random bits this does not happen; the owner answers
    /// `internal` and overwrites nothing.
    Collision,
}

/// A reservation that ended with no connect: one `route.ended { not_connected }` to its caller (TS-9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotConnected<C> {
    pub route_id: String,
    pub caller: C,
}

/// The owner's step for one `route_connect`.
#[derive(Debug, PartialEq, Eq)]
pub enum Connect<C, P> {
    /// The consume point: the entry is now `Consumed`. The owner calls Core `attach` with this reservation in the same
    /// step.
    Consumed(Reserved<C, P>),
    /// The `io` thread writes `close { reason }`. `expired` is the reservation that this step found due, whose one
    /// `not_connected` end the owner sends.
    Refused {
        reason: CloseReason,
        expired: Option<NotConnected<C>>,
    },
}

/// The reservation table and the route table of one Hub.
#[derive(Debug)]
pub struct RouteTables<C, P, R> {
    reservations: HashMap<String, Entry<C, P>>,
    routes: HashMap<String, R>,
    max_reservations: usize,
    window: Duration,
}

impl<C: Clone + PartialEq, P, R> RouteTables<C, P, R> {
    /// `max_reservations` bounds the reservation table (Reserved and Consumed entries); `window` is
    /// `route_connect_seconds` (TS-2).
    pub fn new(max_reservations: usize, window: Duration) -> Self {
        RouteTables {
            reservations: HashMap::new(),
            routes: HashMap::new(),
            max_reservations,
            window,
        }
    }

    /// Mints a route id and a token, and inserts a `Reserved` entry whose deadline is `now + window`.
    pub fn reserve(
        &mut self,
        now: Instant,
        caller: C,
        uid: u32,
        payload: P,
        entropy: &mut impl Entropy,
    ) -> Result<Minted, ReserveError> {
        if self.reservations.len() >= self.max_reservations {
            return Err(ReserveError::Busy);
        }
        let route_id = random_text(entropy, ROUTE_ID_BYTES).map_err(ReserveError::Entropy)?;
        if self.reservations.contains_key(&route_id) || self.routes.contains_key(&route_id) {
            return Err(ReserveError::Collision);
        }
        let route_token = random_text(entropy, ROUTE_TOKEN_BYTES).map_err(ReserveError::Entropy)?;
        let reserved = Reserved {
            caller,
            uid,
            payload,
            deadline: now + self.window,
        };
        self.reservations.insert(
            route_id.clone(),
            Entry::Reserved {
                token: route_token.clone(),
                reserved,
            },
        );
        Ok(Minted {
            route_id,
            route_token,
        })
    }

    /// The owner's one step for a `route_connect` from `caller` with the peer uid `uid` (design 2.3, item 3).
    ///
    /// - No entry, or the entry is due: `route_token_invalid`. A due `Reserved` entry ends `not_connected` first.
    /// - `Consumed`: `route_token_used`.
    /// - `Reserved` with a wrong token, caller or uid: `route_token_invalid`, and the entry stays `Reserved`.
    /// - `Reserved` with a valid proof: the entry becomes `Consumed`, and the reservation is returned.
    pub fn connect(
        &mut self,
        now: Instant,
        route_id: &str,
        route_token: &str,
        caller: &C,
        uid: u32,
    ) -> Connect<C, P> {
        let invalid = |expired| Connect::Refused {
            reason: CloseReason::RouteTokenInvalid,
            expired,
        };
        let Some(entry) = self.reservations.get(route_id) else {
            return invalid(None);
        };
        if now >= entry.deadline() {
            return invalid(self.remove_due(route_id));
        }
        let valid = match entry {
            Entry::Consumed { .. } => {
                return Connect::Refused {
                    reason: CloseReason::RouteTokenUsed,
                    expired: None,
                }
            }
            Entry::Reserved { token, reserved } => {
                let token_ok: bool = token.as_bytes().ct_eq(route_token.as_bytes()).into();
                token_ok && reserved.caller == *caller && reserved.uid == uid
            }
        };
        if !valid {
            return invalid(None);
        }
        let deadline = entry.deadline();
        match self
            .reservations
            .insert(route_id.to_owned(), Entry::Consumed { deadline })
        {
            Some(Entry::Reserved { reserved, .. }) => Connect::Consumed(reserved),
            _ => unreachable!("the entry was Reserved in this step"),
        }
    }

    fn remove_due(&mut self, route_id: &str) -> Option<NotConnected<C>> {
        match self.reservations.remove(route_id)? {
            Entry::Reserved { reserved, .. } => Some(NotConnected {
                route_id: route_id.to_owned(),
                caller: reserved.caller,
            }),
            Entry::Consumed { .. } => None,
        }
    }

    /// Removes every due entry. Each due `Reserved` entry gives one `not_connected` end; a due `Consumed` entry goes
    /// silently.
    pub fn expire(&mut self, now: Instant) -> Vec<NotConnected<C>> {
        let due: Vec<String> = self
            .reservations
            .iter()
            .filter(|(_, entry)| now >= entry.deadline())
            .map(|(route_id, _)| route_id.clone())
            .collect();
        due.iter().filter_map(|id| self.remove_due(id)).collect()
    }

    /// The earliest reservation deadline, for the owner's wait timeout.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.reservations.values().map(Entry::deadline).min()
    }

    /// The number of reservations, Reserved and Consumed.
    pub fn reservations(&self) -> usize {
        self.reservations.len()
    }

    /// Inserts a route after a successful `attach`, or from `RouteAdopted` (design 4). It returns the entry that the
    /// tag already had: a Hub that sees one has a bug, because route ids are unique.
    pub fn insert_route(&mut self, route_id: String, route: R) -> Option<R> {
        self.routes.insert(route_id, route)
    }

    /// Core `RouteClosed` for `route_tag`: the only removal from the route table (design 2.5).
    pub fn route_closed(&mut self, route_tag: &str) -> Option<R> {
        self.routes.remove(route_tag)
    }

    pub fn route(&self, route_id: &str) -> Option<&R> {
        self.routes.get(route_id)
    }

    pub fn routes(&self) -> impl Iterator<Item = (&String, &R)> {
        self.routes.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A counting source: each fill writes the next byte value into every byte.
    struct Counter(u8);

    impl Entropy for Counter {
        fn fill(&mut self, bytes: &mut [u8]) -> Result<(), EntropyError> {
            self.0 = self.0.wrapping_add(1);
            bytes.fill(self.0);
            Ok(())
        }
    }

    struct Failing;

    impl Entropy for Failing {
        fn fill(&mut self, _: &mut [u8]) -> Result<(), EntropyError> {
            Err(EntropyError("no entropy".into()))
        }
    }

    const WINDOW: Duration = Duration::from_secs(10);

    fn tables(max: usize) -> RouteTables<&'static str, &'static str, &'static str> {
        RouteTables::new(max, WINDOW)
    }

    #[test]
    fn the_closes_fit_the_reserved_bound_and_the_widest_is_exactly_it() {
        let lengths: Vec<usize> = route_connect_closes()
            .into_iter()
            .map(|reason| close_frame(reason).len())
            .collect();
        assert!(
            lengths.iter().all(|&len| len <= MAX_ROUTE_CONNECT_CLOSE),
            "{lengths:?}"
        );
        assert_eq!(lengths.iter().max(), Some(&MAX_ROUTE_CONNECT_CLOSE));
        assert!(
            lengths[0] > lengths[1],
            "route_token_invalid is longer than route_token_used"
        );
    }

    #[test]
    fn a_close_frame_is_the_framed_contract_close() {
        let frame = close_frame(CloseReason::RouteTokenUsed);
        let json = br#"{"type":"close","reason":"route_token_used"}"#;
        assert_eq!(&frame[..4], &(json.len() as u32).to_be_bytes());
        assert_eq!(&frame[4..], json);
    }

    #[test]
    fn the_os_source_mints_ids_of_128_bits_and_tokens_of_256_bits() {
        let mut t = tables(4);
        let a = t
            .reserve(Instant::now(), "op", 1, "s", &mut OsEntropy)
            .unwrap();
        let b = t
            .reserve(Instant::now(), "op", 1, "s", &mut OsEntropy)
            .unwrap();
        assert_eq!(URL_SAFE_NO_PAD.decode(&a.route_id).unwrap().len(), 16);
        assert_eq!(URL_SAFE_NO_PAD.decode(&a.route_token).unwrap().len(), 32);
        assert_eq!(a.route_token.len(), 43);
        assert_ne!(a, b);
    }

    #[test]
    fn a_valid_connect_consumes_once_and_a_second_presenter_gets_token_used() {
        let now = Instant::now();
        let mut t = tables(4);
        let m = t.reserve(now, "op", 7, "payload", &mut Counter(0)).unwrap();
        match t.connect(now, &m.route_id, &m.route_token, &"op", 7) {
            Connect::Consumed(r) => {
                assert_eq!((r.caller, r.uid, r.payload), ("op", 7, "payload"));
                assert_eq!(r.deadline, now + WINDOW);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            t.connect(now, &m.route_id, &m.route_token, &"op", 7),
            Connect::Refused {
                reason: CloseReason::RouteTokenUsed,
                expired: None
            }
        );
        assert_eq!(t.reservations(), 1, "the Consumed entry still counts");
    }

    #[test]
    fn a_wrong_token_caller_or_uid_is_invalid_and_keeps_the_token() {
        let now = Instant::now();
        let mut t = tables(4);
        let m = t.reserve(now, "op", 7, "p", &mut Counter(0)).unwrap();
        let invalid = Connect::Refused {
            reason: CloseReason::RouteTokenInvalid,
            expired: None,
        };
        let mut wrong = m.route_token.clone();
        wrong.replace_range(0..1, "Z");
        assert_eq!(t.connect(now, &m.route_id, &wrong, &"op", 7), invalid);
        assert_eq!(
            t.connect(now, &m.route_id, &m.route_token[1..], &"op", 7),
            invalid
        );
        assert_eq!(
            t.connect(now, &m.route_id, &m.route_token, &"other", 7),
            invalid
        );
        assert_eq!(
            t.connect(now, &m.route_id, &m.route_token, &"op", 8),
            invalid
        );
        assert_eq!(t.connect(now, "unknown", &m.route_token, &"op", 7), invalid);
        assert!(matches!(
            t.connect(now, &m.route_id, &m.route_token, &"op", 7),
            Connect::Consumed(_)
        ));
    }

    #[test]
    fn a_token_is_bound_to_its_route() {
        let now = Instant::now();
        let mut t = tables(4);
        let a = t.reserve(now, "op", 7, "a", &mut Counter(0)).unwrap();
        let b = t.reserve(now, "op", 7, "b", &mut Counter(10)).unwrap();
        assert!(matches!(
            t.connect(now, &a.route_id, &b.route_token, &"op", 7),
            Connect::Refused {
                reason: CloseReason::RouteTokenInvalid,
                ..
            }
        ));
    }

    #[test]
    fn a_connect_at_the_deadline_ends_the_reservation_not_connected_once() {
        let now = Instant::now();
        let mut t = tables(4);
        let m = t.reserve(now, "op", 7, "p", &mut Counter(0)).unwrap();
        let at = now + WINDOW;
        assert_eq!(
            t.connect(at, &m.route_id, &m.route_token, &"op", 7),
            Connect::Refused {
                reason: CloseReason::RouteTokenInvalid,
                expired: Some(NotConnected {
                    route_id: m.route_id.clone(),
                    caller: "op"
                })
            }
        );
        assert_eq!(t.reservations(), 0);
        assert!(t.expire(at).is_empty(), "no second end");
    }

    #[test]
    fn expiry_ends_reserved_entries_and_drops_consumed_ones_silently() {
        let now = Instant::now();
        let mut t = tables(4);
        let a = t.reserve(now, "op", 7, "a", &mut Counter(0)).unwrap();
        t.reserve(now + Duration::from_secs(1), "op", 7, "b", &mut Counter(10))
            .unwrap();
        let c = t.reserve(now, "other", 7, "c", &mut Counter(20)).unwrap();
        assert!(matches!(
            t.connect(now, &c.route_id, &c.route_token, &"other", 7),
            Connect::Consumed(_)
        ));
        assert_eq!(t.next_deadline(), Some(now + WINDOW));
        let ended = t.expire(now + WINDOW);
        assert_eq!(
            ended,
            vec![NotConnected {
                route_id: a.route_id,
                caller: "op"
            }]
        );
        assert_eq!(t.reservations(), 1);
        assert_eq!(
            t.next_deadline(),
            Some(now + Duration::from_secs(1) + WINDOW)
        );
        assert!(t.expire(now + WINDOW).is_empty());
    }

    #[test]
    fn a_full_table_is_busy_and_mints_nothing() {
        let now = Instant::now();
        let mut t = tables(1);
        let m = t.reserve(now, "op", 7, "p", &mut Counter(0)).unwrap();
        assert!(matches!(
            t.connect(now, &m.route_id, &m.route_token, &"op", 7),
            Connect::Consumed(_)
        ));
        let mut counter = Counter(50);
        assert_eq!(
            t.reserve(now, "op", 7, "p", &mut counter),
            Err(ReserveError::Busy)
        );
        assert_eq!(counter.0, 50, "no entropy was drawn");
        t.expire(now + WINDOW);
        assert!(t.reserve(now + WINDOW, "op", 7, "p", &mut counter).is_ok());
    }

    #[test]
    fn a_colliding_id_overwrites_nothing() {
        let now = Instant::now();
        let mut t = tables(4);
        let m = t.reserve(now, "op", 7, "p", &mut Counter(0)).unwrap();
        assert_eq!(
            t.reserve(now, "op", 7, "q", &mut Counter(0)),
            Err(ReserveError::Collision)
        );
        assert_eq!(t.reservations(), 1);
        assert!(matches!(
            t.connect(now, &m.route_id, &m.route_token, &"op", 7),
            Connect::Consumed(_)
        ));
    }

    #[test]
    fn an_id_already_in_the_route_table_is_a_collision() {
        let now = Instant::now();
        let mut t = tables(4);
        let id = random_text(&mut Counter(0), ROUTE_ID_BYTES).unwrap();
        t.insert_route(id, "adopted");
        assert_eq!(
            t.reserve(now, "op", 7, "p", &mut Counter(0)),
            Err(ReserveError::Collision)
        );
        assert_eq!(t.reservations(), 0);
    }

    #[test]
    fn an_entropy_failure_reserves_nothing() {
        let mut t = tables(4);
        assert!(matches!(
            t.reserve(Instant::now(), "op", 7, "p", &mut Failing),
            Err(ReserveError::Entropy(_))
        ));
        assert_eq!(t.reservations(), 0);
    }

    #[test]
    fn route_closed_is_the_one_removal_from_the_route_table() {
        let mut t = tables(4);
        assert_eq!(t.insert_route("r1".into(), "core-route"), None);
        assert_eq!(t.route("r1"), Some(&"core-route"));
        assert_eq!(t.routes().count(), 1);
        assert_eq!(t.route_closed("r1"), Some("core-route"));
        assert_eq!(t.route_closed("r1"), None, "a second close finds nothing");
    }
}
