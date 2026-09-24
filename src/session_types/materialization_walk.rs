//! Dormant repository heap model. No runtime caller is registered.
//!
//! The caller is the charged repository materialization path. This draft connects
//! the existing schema seed to one cursor through Serde's access interfaces.
//! It does not establish a construction permit. The counting pass is funded before
//! decoder construction. Shared events produce a conservative heap bound.
//! Worker-stack ownership remains a separate requirement before activation.

use std::cell::Cell;
use std::fmt;

use serde::Deserializer;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};

use super::definition_budget::{DefinitionStorage, Shape};
use super::materialization_error::{ErrorTrack, TypedErrorCandidates};
use super::materialization_timeline::{Timeline, Track};
use super::scratch_budget::ScratchStorage;
use crate::lua_memory::{LuaCallbackCharge, LuaMemoryCapacityError, LuaMemoryClass};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailureKind {
    Lexical,
    Structure,
    Arithmetic,
    Disagreement,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Token {
    None,
    String { decoded: usize, borrowed: bool },
    Array,
    Object,
    Scalar,
    Ignored,
}

#[derive(Clone, Copy)]
struct Cursor<'input> {
    input: &'input [u8],
    offset: usize,
    token: Token,
    // Predicted production storage, not the counting decoder's actual allocation.
    scratch: ScratchStorage,
    failure: Option<(FailureKind, usize)>,
    pending_ignore: Option<(usize, ScratchStorage)>,
    scratch_before_replay: Option<ScratchStorage>,
}

impl<'input> Cursor<'input> {
    fn new(input: &'input [u8]) -> Self {
        Self {
            input,
            offset: 0,
            token: Token::None,
            scratch: ScratchStorage::default(),
            failure: None,
            pending_ignore: None,
            scratch_before_replay: None,
        }
    }

    fn fail(&mut self, kind: FailureKind) {
        if self.failure.is_none() {
            self.failure = Some((kind, self.offset));
        }
    }

    fn whitespace(&mut self) {
        while matches!(
            self.input.get(self.offset),
            Some(b' ' | b'\n' | b'\r' | b'\t')
        ) {
            self.offset += 1;
        }
    }

    fn expect_item(&mut self, first: bool, closing: u8, key: bool) {
        self.pending_ignore = None;
        if self.failure.is_some() {
            return;
        }
        self.whitespace();
        if self.input.get(self.offset) == Some(&closing) {
            return;
        }
        if !first {
            if self.input.get(self.offset) != Some(&b',') {
                self.fail(FailureKind::Structure);
                return;
            }
            self.offset += 1;
            self.whitespace();
        }
        if self.input.get(self.offset).is_none()
            || self.input.get(self.offset) == Some(&closing)
            || (key && self.input.get(self.offset) != Some(&b'"'))
        {
            self.fail(FailureKind::Structure);
        }
    }

    fn colon(&mut self) {
        self.pending_ignore = None;
        if self.failure.is_some() {
            return;
        }
        self.whitespace();
        if self.input.get(self.offset) == Some(&b':') {
            self.offset += 1;
        } else {
            self.fail(FailureKind::Structure);
        }
    }

    fn append(&mut self, bytes: usize) {
        if self.scratch.append(bytes).is_none() {
            self.fail(FailureKind::Arithmetic);
        }
    }

    fn hex(&mut self) -> Option<u16> {
        let Some(end) = self.offset.checked_add(4) else {
            self.fail(FailureKind::Arithmetic);
            return None;
        };
        if end > self.input.len() {
            self.offset = self.input.len();
            self.fail(FailureKind::Lexical);
            return None;
        }
        let start = self.offset;
        self.offset = end;
        let mut value = 0u16;
        for &byte in &self.input[start..end] {
            let digit = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                b'A'..=b'F' => byte - b'A' + 10,
                _ => {
                    self.fail(FailureKind::Lexical);
                    return None;
                }
            };
            value = (value << 4) | u16::from(digit);
        }
        Some(value)
    }

    fn unicode_len(&mut self) -> Option<usize> {
        let first = self.hex()?;
        if (0xdc00..=0xdfff).contains(&first) {
            self.fail(FailureKind::Lexical);
            return None;
        }
        if (0xd800..=0xdbff).contains(&first) {
            for expected in [b'\\', b'u'] {
                let actual = self.input.get(self.offset).copied();
                if actual.is_some() {
                    self.offset += 1;
                }
                if actual != Some(expected) {
                    self.fail(FailureKind::Lexical);
                    return None;
                }
            }
            let second = self.hex()?;
            if !(0xdc00..=0xdfff).contains(&second) {
                self.fail(FailureKind::Lexical);
                return None;
            }
            return Some(4);
        }
        Some(match first {
            0..=0x7f => 1,
            0x80..=0x7ff => 2,
            _ => 3,
        })
    }

    /// Run before Serde consumes the string, including strings that later fail.
    fn string(&mut self, copied: bool) {
        if copied {
            self.scratch.clear();
        }
        self.offset += 1;
        let mut start = self.offset;
        let mut decoded = 0usize;
        let mut escaped = false;
        let mut valid_utf8 = true;
        loop {
            let Some(byte) = self.input.get(self.offset).copied() else {
                self.fail(FailureKind::Lexical);
                return;
            };
            if byte == b'"' || byte == b'\\' || byte < 0x20 {
                if byte < 0x20 {
                    self.offset += 1;
                    self.fail(FailureKind::Lexical);
                    return;
                }
                let span = &self.input[start..self.offset];
                valid_utf8 &= std::str::from_utf8(span).is_ok();
                let Some(next) = decoded.checked_add(span.len()) else {
                    self.fail(FailureKind::Arithmetic);
                    return;
                };
                decoded = next;
                if copied && (escaped || byte == b'\\') {
                    self.append(span.len());
                }
                self.offset += 1;
                if byte == b'"' {
                    if copied && !valid_utf8 {
                        self.fail(FailureKind::Lexical);
                    }
                    self.token = Token::String {
                        decoded,
                        borrowed: !escaped,
                    };
                    return;
                }
                escaped = true;
                let Some(escape) = self.input.get(self.offset).copied() else {
                    self.fail(FailureKind::Lexical);
                    return;
                };
                self.offset += 1;
                let bytes = match escape {
                    b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => 1,
                    b'u' if !copied => match self.hex() {
                        Some(_) => 0,
                        None => return,
                    },
                    b'u' => match self.unicode_len() {
                        Some(bytes) => bytes,
                        None => return,
                    },
                    _ => {
                        self.fail(FailureKind::Lexical);
                        return;
                    }
                };
                if copied {
                    if bytes == 1 {
                        self.append(1);
                    } else if self.scratch.append_unicode(bytes).is_none() {
                        self.fail(FailureKind::Arithmetic);
                    }
                }
                let Some(next) = decoded.checked_add(bytes) else {
                    self.fail(FailureKind::Arithmetic);
                    return;
                };
                decoded = next;
                start = self.offset;
            } else {
                self.offset += 1;
            }
        }
    }

    fn advance(&mut self) {
        if self.failure.is_some() {
            return;
        }
        self.whitespace();
        match self.input.get(self.offset).copied() {
            Some(b'"') => self.string(true),
            Some(b'[') => {
                self.offset += 1;
                self.token = Token::Array;
            }
            Some(b'{') => {
                self.offset += 1;
                self.token = Token::Object;
            }
            Some(b']' | b'}') | None => self.fail(FailureKind::Structure),
            Some(_) => {
                self.token = Token::Scalar;
                while let Some(byte) = self.input.get(self.offset) {
                    if matches!(byte, b',' | b']' | b'}' | b' ' | b'\n' | b'\r' | b'\t') {
                        break;
                    }
                    self.offset += 1;
                }
            }
        }
    }

    fn close(&mut self, expected: u8) {
        self.pending_ignore = None;
        if self.failure.is_some() {
            return;
        }
        self.whitespace();
        if self.input.get(self.offset) == Some(&expected) {
            self.offset += 1;
        } else {
            self.fail(FailureKind::Structure);
        }
    }

    /// This draft records depth, not JSON validity. Serde chooses the diagnostic.
    fn ignore(&mut self) {
        if self.failure.is_some() {
            return;
        }
        self.whitespace();
        self.pending_ignore = Some((self.offset, self.scratch));
        self.scratch.clear();
        let mut depth = 0usize;
        loop {
            match self.input.get(self.offset).copied() {
                Some(b'"') => {
                    self.string(false);
                    if self.failure.is_some() {
                        return;
                    }
                    if depth == 0 {
                        break;
                    }
                }
                Some(b'[' | b'{') => {
                    if depth != 0 {
                        self.append(1);
                    }
                    let Some(next) = depth.checked_add(1) else {
                        self.fail(FailureKind::Arithmetic);
                        return;
                    };
                    depth = next;
                    self.offset += 1;
                }
                Some(b']' | b'}') if depth != 0 => {
                    depth -= 1;
                    self.offset += 1;
                    if depth == 0 {
                        break;
                    }
                    self.scratch.pop();
                }
                Some(_) if depth != 0 => {
                    self.offset += 1;
                }
                Some(_) => {
                    self.advance();
                    break;
                }
                None => {
                    self.fail(FailureKind::Structure);
                    return;
                }
            }
        }
        self.token = Token::Ignored;
    }

    /// Serde selects the diagnostic and its byte position. This code does not
    /// interpret number syntax or choose a replacement structural error.
    /// The offset is line_start + column: an exclusive prefix end that includes
    /// the byte named by a peek error, even if Serde did not consume that byte.
    /// Replay can therefore include one inspected byte. It is a storage bound,
    /// not a claim about the decoder's internal read index.
    fn reconcile_error(&mut self, error: &serde_json::Error) {
        let mut line = 1usize;
        let mut start = 0usize;
        for (index, byte) in self.input.iter().enumerate() {
            if line == error.line() {
                break;
            }
            if *byte == b'\n' {
                line += 1;
                start = index + 1;
            }
        }
        let Some(offset) = start.checked_add(error.column()) else {
            self.fail(FailureKind::Arithmetic);
            return;
        };
        if line != error.line() || offset > self.input.len() {
            self.fail(FailureKind::Disagreement);
            return;
        }
        if let Some((begin, scratch)) = self.pending_ignore {
            if offset < begin {
                self.fail(FailureKind::Disagreement);
                return;
            }
            // The eager ignored-value scan can pass a structural failure.
            // Recompute its storage from only the prefix Serde reached.
            let mut prefix = Self::new(&self.input[..offset]);
            prefix.offset = begin;
            prefix.scratch = scratch;
            prefix.ignore();
            if matches!(prefix.failure, Some((FailureKind::Arithmetic, _))) {
                self.fail(FailureKind::Arithmetic);
                return;
            }
            // Retain the earlier prediction for evidence. Neither model owns
            // the counting decoder's workspace or can release its charge.
            self.scratch_before_replay = Some(self.scratch);
            self.scratch = prefix.scratch;
        }
        self.offset = offset;
        let kind = match self.failure {
            Some((FailureKind::Arithmetic | FailureKind::Disagreement, _)) => return,
            Some((kind, _)) => kind,
            None if self.pending_ignore.is_some() => FailureKind::Structure,
            None => FailureKind::Lexical,
        };
        self.failure = Some((kind, offset));
    }
}

#[derive(Clone, Copy)]
struct State<'a, 'input> {
    cursor: &'a Cell<Cursor<'input>>,
    scratch_held: &'a Cell<usize>,
    track: Track<'a>,
}

fn update<'input>(state: State<'_, 'input>, action: impl FnOnce(&mut Cursor<'input>)) {
    let mut cursor = state.cursor.get();
    action(&mut cursor);
    // Error replay can reduce a prediction, but it cannot release an owner.
    let old = state.scratch_held.get();
    let event = cursor.scratch.conservative_observation(old);
    if state
        .track
        .replace_with_peak(old, event.retained, event.live)
        .is_none()
    {
        cursor.fail(FailureKind::Arithmetic);
    }
    state.scratch_held.set(event.retained);
    state.cursor.set(cursor);
}

struct CountingDeserializer<'a, 'input, D> {
    inner: D,
    state: State<'a, 'input>,
    key: bool,
}
struct CountingVisitor<'a, 'input, V> {
    inner: V,
    state: State<'a, 'input>,
}
struct CountingSeed<'a, 'input, S> {
    inner: S,
    state: State<'a, 'input>,
    key: bool,
}
struct CountingAccess<'a, 'input, A> {
    inner: A,
    state: State<'a, 'input>,
    first: bool,
}

macro_rules! reject_counting_method {
    ($method:ident $(, $argument:ident: $kind:ty)*) => {
        fn $method<V: Visitor<'de>>(self, $($argument: $kind,)* _visitor: V) -> Result<V::Value, D::Error> {
            $(let _ = $argument;)*
            update(self.state, |cursor| cursor.fail(FailureKind::Disagreement));
            Err(<D::Error as de::Error>::custom("unsupported counting deserializer method"))
        }
    };
}

impl<'de, D: Deserializer<'de>> Deserializer<'de> for CountingDeserializer<'_, '_, D> {
    type Error = D::Error;
    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        update(self.state, |cursor| {
            cursor.pending_ignore = None;
            cursor.advance();
        });
        self.inner.deserialize_any(CountingVisitor {
            inner: visitor,
            state: self.state,
        })
    }
    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        update(self.state, |cursor| {
            cursor.pending_ignore = None;
            cursor.advance();
        });
        self.inner.deserialize_identifier(CountingVisitor {
            inner: visitor,
            state: self.state,
        })
    }
    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.key {
            update(self.state, Cursor::advance);
        } else {
            update(self.state, Cursor::ignore);
        }
        self.inner.deserialize_ignored_any(visitor)
    }
    reject_counting_method!(deserialize_bool);
    reject_counting_method!(deserialize_i8);
    reject_counting_method!(deserialize_i16);
    reject_counting_method!(deserialize_i32);
    reject_counting_method!(deserialize_i64);
    reject_counting_method!(deserialize_i128);
    reject_counting_method!(deserialize_u8);
    reject_counting_method!(deserialize_u16);
    reject_counting_method!(deserialize_u32);
    reject_counting_method!(deserialize_u64);
    reject_counting_method!(deserialize_u128);
    reject_counting_method!(deserialize_f32);
    reject_counting_method!(deserialize_f64);
    reject_counting_method!(deserialize_char);
    reject_counting_method!(deserialize_str);
    reject_counting_method!(deserialize_string);
    reject_counting_method!(deserialize_bytes);
    reject_counting_method!(deserialize_byte_buf);
    reject_counting_method!(deserialize_option);
    reject_counting_method!(deserialize_unit);
    reject_counting_method!(deserialize_unit_struct, name: &'static str);
    reject_counting_method!(deserialize_newtype_struct, name: &'static str);
    reject_counting_method!(deserialize_seq);
    reject_counting_method!(deserialize_tuple, len: usize);
    reject_counting_method!(deserialize_tuple_struct, name: &'static str, len: usize);
    reject_counting_method!(deserialize_map);
    reject_counting_method!(deserialize_struct, name: &'static str, fields: &'static [&'static str]);
    reject_counting_method!(deserialize_enum, name: &'static str, variants: &'static [&'static str]);
}

impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for CountingSeed<'_, '_, S> {
    type Value = S::Value;
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<S::Value, D::Error> {
        self.inner.deserialize(CountingDeserializer {
            inner: decoder,
            state: self.state,
            key: self.key,
        })
    }
}

impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for CountingAccess<'_, '_, A> {
    type Error = A::Error;
    fn next_element_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, A::Error> {
        update(self.state, |cursor| {
            cursor.expect_item(self.first, b']', false)
        });
        self.first = false;
        self.inner.next_element_seed(CountingSeed {
            inner: seed,
            state: self.state,
            key: false,
        })
    }
    fn size_hint(&self) -> Option<usize> {
        self.inner.size_hint()
    }
}

impl<'de, A: MapAccess<'de>> MapAccess<'de> for CountingAccess<'_, '_, A> {
    type Error = A::Error;
    fn next_key_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, A::Error> {
        update(self.state, |cursor| {
            cursor.expect_item(self.first, b'}', true)
        });
        self.first = false;
        self.inner.next_key_seed(CountingSeed {
            inner: seed,
            state: self.state,
            key: true,
        })
    }
    fn next_value_seed<S: DeserializeSeed<'de>>(&mut self, seed: S) -> Result<S::Value, A::Error> {
        update(self.state, Cursor::colon);
        self.inner.next_value_seed(CountingSeed {
            inner: seed,
            state: self.state,
            key: false,
        })
    }
    fn size_hint(&self) -> Option<usize> {
        self.inner.size_hint()
    }
}

impl<'de, V: Visitor<'de>> Visitor<'de> for CountingVisitor<'_, '_, V> {
    type Value = V::Value;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.expecting(formatter)
    }
    fn visit_unit<E: de::Error>(self) -> Result<V::Value, E> {
        update(self.state, |cursor| {
            if cursor.token != Token::Scalar {
                cursor.fail(FailureKind::Disagreement);
            }
        });
        self.inner.visit_unit()
    }
    fn visit_bool<E: de::Error>(self, value: bool) -> Result<V::Value, E> {
        update(self.state, |cursor| {
            if cursor.token != Token::Scalar {
                cursor.fail(FailureKind::Disagreement);
            }
        });
        self.inner.visit_bool(value)
    }
    fn visit_i64<E: de::Error>(self, value: i64) -> Result<V::Value, E> {
        update(self.state, |cursor| {
            if cursor.token != Token::Scalar {
                cursor.fail(FailureKind::Disagreement);
            }
        });
        self.inner.visit_i64(value)
    }
    fn visit_u64<E: de::Error>(self, value: u64) -> Result<V::Value, E> {
        update(self.state, |cursor| {
            if cursor.token != Token::Scalar {
                cursor.fail(FailureKind::Disagreement);
            }
        });
        self.inner.visit_u64(value)
    }
    fn visit_f64<E: de::Error>(self, value: f64) -> Result<V::Value, E> {
        update(self.state, |cursor| {
            if cursor.token != Token::Scalar {
                cursor.fail(FailureKind::Disagreement);
            }
        });
        self.inner.visit_f64(value)
    }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<V::Value, E> {
        update(self.state, |cursor| {
            if cursor.token
                != (Token::String {
                    decoded: value.len(),
                    borrowed: false,
                })
            {
                cursor.fail(FailureKind::Disagreement);
            }
        });
        self.inner.visit_str(value)
    }
    fn visit_borrowed_str<E: de::Error>(self, value: &'de str) -> Result<V::Value, E> {
        update(self.state, |cursor| {
            if cursor.token
                != (Token::String {
                    decoded: value.len(),
                    borrowed: true,
                })
            {
                cursor.fail(FailureKind::Disagreement);
            }
        });
        self.inner.visit_borrowed_str(value)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, sequence: A) -> Result<V::Value, A::Error> {
        update(self.state, |cursor| {
            if cursor.token != Token::Array {
                cursor.fail(FailureKind::Disagreement);
            }
        });
        let result = self.inner.visit_seq(CountingAccess {
            inner: sequence,
            state: self.state,
            first: true,
        });
        if result.is_ok() {
            update(self.state, |cursor| cursor.close(b']'));
        }
        result
    }
    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<V::Value, A::Error> {
        update(self.state, |cursor| {
            if cursor.token != Token::Object {
                cursor.fail(FailureKind::Disagreement);
            }
        });
        let result = self.inner.visit_map(CountingAccess {
            inner: map,
            state: self.state,
            first: true,
        });
        if result.is_ok() {
            update(self.state, |cursor| cursor.close(b'}'));
        }
        result
    }
}

/// Conservative counting-workspace reservation, not a production peak.
/// The scan allocates nothing and does not validate JSON or choose its errors.
fn counting_workspace_bytes(input: &[u8]) -> Option<usize> {
    let mut string_start = None;
    let mut escaped = false;
    let mut has_escape = false;
    let mut longest_string = 0usize;
    let mut depth = 0usize;
    let mut deepest = 0usize;
    for (index, &byte) in input.iter().enumerate() {
        if let Some(start) = string_start {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
                has_escape = true;
            } else if byte == b'"' {
                if has_escape {
                    longest_string = longest_string.max(index.checked_sub(start)?);
                }
                string_start = None;
            }
        } else {
            match byte {
                b'"' => {
                    string_start = Some(index.checked_add(1)?);
                    has_escape = false;
                }
                b'[' | b'{' => {
                    depth = depth.checked_add(1)?;
                    deepest = deepest.max(depth);
                }
                b']' | b'}' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
    }
    if let Some(start) = string_start.filter(|_| has_escape) {
        longest_string = longest_string.max(input.len().checked_sub(start)?);
    }
    // Raw string bytes bound every decoded append and reserve(4) request:
    // a completed unicode escape contributes six raw bytes before reserve(4).
    // An incomplete escape appends its prefix but does not reserve the codepoint.
    // Unescaped strings borrow input at every schema position and need no copy.
    // Counting every container bounds any ignored-value frame stack.
    let required = longest_string.max(deepest);
    // For growth, old < required and new = max(2*old, required, 8).
    // Thus max(3*required, 8) covers old+new, including moved reallocations.
    // Capacity history across tokens cannot exceed this bound.
    let scratch = if required == 0 {
        0
    } else {
        required.checked_mul(3)?.max(8)
    };
    scratch.checked_add(counting_error_bytes()?)
}

fn counting_error_bytes() -> Option<usize> {
    let longest = [
        "session type Content storage overflow",
        "session type Content storage underflow",
        "session type output storage overflow",
        "session type error size overflow",
        "unsupported counting deserializer method",
    ]
    .iter()
    .map(|message| message.len())
    .max()
    .unwrap_or(0);
    // E::custom(&str) copies exactly once. fix_position overlaps two boxes.
    // Syntax errors use one box and no owned message, so this also covers them.
    super::bounded_catalog::json_error_impl_bytes()
        .checked_mul(2)?
        .checked_add(longest)
}

/// This owns counting workspace only. It cannot authorize typed construction.
struct ChargedCountingResult<'input> {
    result: Result<DefinitionStorage, serde_json::Error>,
    cursor: Cursor<'input>,
    timeline: Timeline,
    typed_errors: TypedErrorCandidates,
    // Drop the result's error allocation before releasing its charge.
    _storage: Option<LuaCallbackCharge>,
}

#[derive(Debug)]
enum CountingRefusal {
    Arithmetic,
    Capacity(LuaMemoryCapacityError),
    Correspondence { offset: usize },
}

impl ChargedCountingResult<'_> {
    /// A conservative heap bound, never a permit to construct typed output.
    fn typed_construction_bytes(&self) -> Result<usize, CountingRefusal> {
        if self.timeline.refused {
            return Err(CountingRefusal::Arithmetic);
        }
        match self.cursor.failure {
            Some((FailureKind::Arithmetic, _)) => return Err(CountingRefusal::Arithmetic),
            Some((FailureKind::Disagreement, offset)) => {
                return Err(CountingRefusal::Correspondence { offset });
            }
            _ => {}
        }
        // The counting visitors do not validate schema values. Their Data
        // errors are checked size/layout failures. Decoder syntax stays separate.
        if self
            .result
            .as_ref()
            .err()
            .is_some_and(|error| error.is_data())
        {
            return Err(CountingRefusal::Arithmetic);
        }
        let errors = self
            .typed_errors
            .storage_bytes(self.cursor.input.len())
            .ok_or(CountingRefusal::Arithmetic)?;
        self.timeline
            .maximum
            .checked_add(errors)
            .ok_or(CountingRefusal::Arithmetic)
    }
}

/// The caller admits the aggregate charge through the plugin account first.
/// Splitting it cannot create a second independent callback allowance.
/// Refusal occurs before decoder construction and leaves the input untouched.
fn count_with_storage<'input>(
    input: &'input [u8],
    available: &mut LuaCallbackCharge,
) -> Result<ChargedCountingResult<'input>, CountingRefusal> {
    let capacity_error = |requested| LuaMemoryCapacityError {
        class: LuaMemoryClass::Callback,
        requested,
        available: available.bytes(),
    };
    let requested = counting_workspace_bytes(input).ok_or(CountingRefusal::Arithmetic)?;
    let failure = CountingRefusal::Capacity(capacity_error(requested));
    let storage = available.split_fixed(requested).ok_or(failure)?;
    count_prepaid(input, Some(storage))
}

/// The caller keeps a separate parent charge live for this whole decoder pass.
fn count_prepaid<'input>(
    input: &'input [u8],
    storage: Option<LuaCallbackCharge>,
) -> Result<ChargedCountingResult<'input>, CountingRefusal> {
    let cursor = Cell::new(Cursor::new(input));
    let timeline = Cell::new(Timeline::default());
    let typed_errors = Cell::new(TypedErrorCandidates::default());
    let scratch_held = Cell::new(0);
    let track = Track::new(&timeline).excluding_owner(&scratch_held);
    let state = State {
        cursor: &cursor,
        scratch_held: &scratch_held,
        track,
    };
    let result = {
        let mut decoder = serde_json::Deserializer::from_slice(input);
        let result = Shape::File
            .with_track(track, ErrorTrack::new(&typed_errors))
            .deserialize(CountingDeserializer {
                inner: &mut decoder,
                state,
                key: false,
            })
            .and_then(|value| decoder.end().map(|()| value));
        if let Err(error) = &result {
            update(state, |cursor| cursor.reconcile_error(error));
        }
        // The workspace charge remains live while the decoder drops its scratch.
        drop(decoder);
        result
    };
    let counted = ChargedCountingResult {
        result,
        cursor: cursor.get(),
        timeline: timeline.get(),
        typed_errors: typed_errors.get(),
        _storage: storage,
    };
    // Refuse model failures at this boundary, never as typed diagnostics.
    counted.typed_construction_bytes()?;
    Ok(counted)
}

/// Return the checked typed-parser peak under the caller's open parent.
/// This is a sizing result, not a construction permit.
pub(super) fn counted_parser_peak(
    input: &[u8],
    parent: &mut LuaCallbackCharge,
) -> Result<usize, &'static str> {
    let workspace = counting_workspace_bytes(input)
        .ok_or("repo session type counting size overflow")?;
    parent
        .grow(workspace)
        .map_err(|_| "repo session type counting capacity exhausted")?;
    let original = parent.bytes() - workspace;
    let peak = match count_prepaid(input, None) {
        Ok(counted) => counted
            .typed_construction_bytes()
            .map_err(|_| "repo session type parser peak is unavailable"),
        Err(_) => Err("repo session type counting capacity exhausted"),
    };
    assert!(parent.shrink_to(original));
    peak
}

#[cfg(test)]
fn count_fixture(input: &[u8]) -> ChargedCountingResult<'_> {
    use crate::lua_memory::{LuaMemoryAccount, LuaMemoryLimits};
    let bytes = counting_workspace_bytes(input).unwrap();
    let account = LuaMemoryAccount::new(LuaMemoryLimits {
        per_vm_bytes: 1,
        total_vm_bytes: 1,
        per_callback_bytes: bytes,
        total_callback_bytes: bytes,
    })
    .unwrap();
    let mut storage = account.reserve_callback_total(bytes).unwrap();
    count_with_storage(input, &mut storage).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeline_keeps_parent_output_during_a_malformed_child() {
        let parent = "p".repeat(4096);
        let child = "c".repeat(2048);
        let input =
            format!(r#"{{"session_types":[{{"label":"{parent}","description":"{child}\q"}}]}}"#);
        let counted = count_fixture(input.as_bytes());
        assert!(counted.result.is_err());
        assert!(counted.timeline.maximum >= parent.len() + child.len());
        assert!(counted.typed_construction_bytes().unwrap() > counted.timeline.maximum);
    }

    #[test]
    fn timeline_covers_output_and_scratch_growth_in_schema_order() {
        let input = br#"{"session_types":[{"description":"\naaaaaaaa","label":"aaaaaaaaaaaaa\u0080","id":"agent","role":"agent","interaction":"interactive","lifecycle":"persistent","command":"agent"}]}"#;
        let counted = count_fixture(input);
        assert!(counted.result.is_ok());
        // The first owned string survives the second string's 16+32 growth.
        assert!(counted.timeline.maximum >= 9 + 48);
        let output = &counted.result.as_ref().unwrap().output;
        assert!(counted.timeline.live >= output.retained + counted.cursor.scratch.capacity);
    }

    #[test]
    fn nested_content_growth_overlaps_parent_output() {
        let parent = "p".repeat(64);
        let input = format!(
            r#"{{"session_types":[{{"label":"{parent}","execution":{{"mode":"shell_command","extra":[null,null,null,null,null]}}}}]}}"#
        );
        let counted = count_fixture(input.as_bytes());
        assert!(counted.result.is_ok());
        let element = std::mem::size_of::<serde::__private228::de::Content<'static>>();
        assert!(counted.timeline.maximum >= parent.len() + (4 + 8) * element);
    }

    #[test]
    fn working_directory_unknown_content_uses_the_tagged_tree_charge() {
        let nested = format!("{}0{}", "[".repeat(24), "]".repeat(24));
        for fields in [
            format!(r#""extra":{nested},"policy":"relative""#),
            format!(r#""policy":"relative","extra":{nested}"#),
        ] {
            let input = format!(
                r#"{{"session_types":[{{"id":"agent","label":"Agent","role":"agent","interaction":"interactive","lifecycle":"persistent","command":"agent","working_directory":{{{fields},"path":"sub\u002fdir"}}}}]}}"#
            );
            let counted = count_fixture(input.as_bytes());
            assert!(counted.result.is_ok());
            let content = std::mem::size_of::<serde::__private228::de::Content<'static>>();
            // Pinned RawVec starts each one-element Content vector at four slots.
            assert!(counted.cursor.scratch.capacity > 0);
            assert!(
                counted.timeline.maximum >= 24 * 4 * content + counted.cursor.scratch.capacity
            );
            assert!(counted.typed_construction_bytes().unwrap() >= counted.timeline.maximum);
            let parsed =
                serde_json::from_slice::<super::super::RepoSessionTypesFile>(input.as_bytes());
            assert!(parsed.is_ok());
        }
    }

    #[test]
    fn failed_definition_does_not_release_its_recorded_content() {
        let input = br#"{"session_types":[{"execution":{"mode":"shell_command","extra":[null,null,null,null,null]},"label":"\q"}]}"#;
        let counted = count_fixture(input);
        assert!(counted.result.is_err());
        let element = std::mem::size_of::<serde::__private228::de::Content<'static>>();
        // Eight sequence slots and four map pairs remain in the failed scope.
        assert!(counted.timeline.live >= 16 * element);
    }

    #[test]
    fn owned_relative_path_transfers_without_a_second_string_charge() {
        let borrowed = count_fixture(
            br#"{"session_types":[{"working_directory":{"policy":"relative","path":"sub"}}]}"#,
        );
        let owned = count_fixture(
            br#"{"session_types":[{"working_directory":{"policy":"relative","path":"su\u0062"}}]}"#,
        );
        assert!(borrowed.result.is_ok());
        assert!(owned.result.is_ok());
        assert_eq!(borrowed.cursor.scratch.capacity, 0);
        assert_eq!(owned.cursor.scratch.capacity, 8);
        assert_eq!(owned.timeline.live, borrowed.timeline.live + 8);
    }

    #[test]
    fn divergence_preserves_the_earlier_typed_error_candidate() {
        let tag = "unknown".repeat(4096);
        let input =
            format!(r#"{{"session_types":[{{"execution":{{"mode":"{tag}"}},"label":"bad\q"}}]}}"#);
        let counted = count_fixture(input.as_bytes());
        assert!(counted.result.is_err());
        assert_eq!(counted.typed_errors.tag_bytes, tag.len());
        let error = serde_json::from_slice::<super::super::RepoSessionTypesFile>(input.as_bytes())
            .unwrap_err();
        assert!(error.to_string().starts_with("unknown variant"));
        let bound = counted.typed_construction_bytes().unwrap();
        assert!(bound >= error.to_string().len());
    }

    #[test]
    fn accepted_counting_shape_can_still_fail_typed_decoding() {
        let input = br#"{"session_types":"wrong shape"}"#;
        let counted = count_fixture(input);
        assert!(counted.result.is_ok());
        assert_eq!(
            counted.typed_errors.wrong_string_debug,
            "\"wrong shape\"".len()
        );
        let error =
            serde_json::from_slice::<super::super::RepoSessionTypesFile>(input).unwrap_err();
        assert!(error.to_string().starts_with("invalid type: string"));
        assert!(counted.typed_construction_bytes().unwrap() >= error.to_string().len());
    }

    #[test]
    fn counting_success_release_preserves_an_earlier_typed_failure_peak() {
        let values = std::iter::repeat_n("null", 64)
            .collect::<Vec<_>>()
            .join(",");
        let tag = "unknown".repeat(128);
        let input = format!(
            r#"{{"session_types":[{{"execution":{{"extra":[{values}],"mode":"{tag}"}}}}]}}"#
        );
        let counted = count_fixture(input.as_bytes());
        assert!(counted.result.is_ok());
        let error = serde_json::from_slice::<super::super::RepoSessionTypesFile>(input.as_bytes())
            .unwrap_err();
        assert!(error.to_string().starts_with("unknown variant"));
        assert_eq!(
            counted.timeline.live,
            counted.result.as_ref().unwrap().output.retained + counted.cursor.scratch.capacity
        );
        let content = 64 * std::mem::size_of::<serde::__private228::de::Content<'static>>();
        assert!(counted.timeline.maximum >= content);
        assert!(counted.typed_construction_bytes().unwrap() >= content + error.to_string().len());
    }

    #[test]
    fn typed_error_bound_is_input_dependent_and_counted_once() {
        let short = count_fixture(br#"{"session_types":[{"execution":{"mode":"x"}}]}"#);
        let tag = "x".repeat(8192);
        let input = format!(r#"{{"session_types":[{{"execution":{{"mode":"{tag}"}}}}]}}"#);
        let long = count_fixture(input.as_bytes());
        assert!(
            long.typed_construction_bytes().unwrap() > short.typed_construction_bytes().unwrap()
        );
        assert_eq!(
            long.typed_construction_bytes().unwrap(),
            long.timeline.maximum + long.typed_errors.storage_bytes(input.len()).unwrap()
        );
    }

    #[test]
    fn completed_definition_release_keeps_the_large_corpus_within_quota() {
        let values = std::iter::repeat_n("null", 2048)
            .collect::<Vec<_>>()
            .join(",");
        let definitions = (0..256).map(|index| format!(
            r#"{{"id":"agent-{index}","label":"Agent","role":"agent.worker","interaction":"interactive","lifecycle":"persistent","command":"agent","execution":{{"mode":"shell_command","extra":[{values}]}}}}"#
        )).collect::<Vec<_>>().join(",");
        let input = format!(r#"{{"session_types":[{definitions}]}}"#);
        assert!(input.len() <= super::super::REPO_SESSION_TYPES_FILE_BYTE_CAPACITY);
        let typed =
            serde_json::from_slice::<super::super::RepoSessionTypesFile>(input.as_bytes()).unwrap();
        super::super::validate_session_types(&typed.session_types).unwrap();
        let counted = count_fixture(input.as_bytes());
        assert!(counted.result.is_ok());
        let bound = counted.typed_construction_bytes().unwrap();
        assert_eq!(
            counted.timeline.live,
            counted.result.as_ref().unwrap().output.retained + counted.cursor.scratch.capacity
        );
        let quota = crate::config::lua_memory_limits().per_callback_bytes;
        assert!(input.len() + bound <= quota);
        let retained_content_without_release =
            256 * 2048 * std::mem::size_of::<serde::__private228::de::Content<'static>>();
        assert!(input.len() + retained_content_without_release > quota);
        eprintln!(
            "completed-definition bound: input={} typed_bound={} unreleased_content_lower_bound={} quota={}",
            input.len(),
            bound,
            retained_content_without_release,
            quota
        );
        // This checks the connected bound, not an allocator measurement or permit.
    }

    #[test]
    fn duplicate_environment_and_tagged_path_preserve_complete_values() {
        let input = br#"{"session_types":[{"id":"agent","label":"Agent","role":"agent","interaction":"interactive","lifecycle":"persistent","command":"agent","args":["a","b"],"environment":{"SAME":"old","SAME":"new","KEEP":"present"},"working_directory":{"policy":"relative","path":"sub","extra":["\n","\u0080"]},"context":["repository"]}]}"#;
        let counted = count_fixture(input);
        let typed = serde_json::from_slice::<super::super::RepoSessionTypesFile>(input).unwrap();
        let value = &typed.session_types[0];
        assert_eq!(value.command, "agent");
        assert_eq!(value.args, ["a", "b"]);
        assert_eq!(value.environment.get("SAME").unwrap(), "new");
        assert_eq!(value.environment.get("KEEP").unwrap(), "present");
        assert_eq!(value.context, ["repository"]);
        assert!(
            matches!(&value.working_directory, super::super::PackageSessionTypeWorkingDirectory::Relative { path } if path == "sub")
        );
        assert!(counted.timeline.live >= counted.result.as_ref().unwrap().output.retained);
        assert!(counted.typed_construction_bytes().is_ok());
    }

    #[test]
    fn short_counting_charge_refuses_without_consuming_the_parent_charge() {
        use crate::lua_memory::{LuaMemoryAccount, LuaMemoryLimits};
        let input = br#"{"session_types":[{"label":"\u0080"}]}"#;
        let bytes = counting_workspace_bytes(input).unwrap();
        let account = LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: bytes,
            total_callback_bytes: bytes,
        })
        .unwrap();
        let mut storage = account.reserve_callback_total(bytes - 1).unwrap();
        let error = match count_with_storage(input, &mut storage) {
            Err(CountingRefusal::Capacity(error)) => error,
            Err(other) => panic!("unexpected refusal: {other:?}"),
            Ok(_) => panic!("short storage admitted a counting decoder"),
        };
        assert_eq!(error.class, LuaMemoryClass::Callback);
        assert_eq!(error.requested, bytes);
        assert_eq!(error.available, bytes - 1);
        assert_eq!(storage.bytes(), bytes - 1);
        assert_eq!(account.usage().1, bytes - 1);
    }

    #[test]
    fn counting_result_keeps_its_charge_after_the_decoder_is_destroyed() {
        use crate::lua_memory::{LuaMemoryAccount, LuaMemoryLimits};
        for input in [&b"{}"[..], &b"["[..]] {
            let bytes = counting_workspace_bytes(input).unwrap();
            let account = LuaMemoryAccount::new(LuaMemoryLimits {
                per_vm_bytes: 1,
                total_vm_bytes: 1,
                per_callback_bytes: bytes,
                total_callback_bytes: bytes,
            })
            .unwrap();
            let mut storage = account.reserve_callback_total(bytes).unwrap();
            let counted = count_with_storage(input, &mut storage).unwrap();
            assert_eq!(counted.result.is_err(), input == b"[");
            assert_eq!(storage.bytes(), 0);
            drop(storage);
            assert_eq!(account.usage().1, bytes);
            assert!(account.reserve_callback_total(1).is_err());
            drop(counted);
            assert_eq!(account.usage().1, 0);
        }
    }

    #[test]
    fn counting_upper_scan_covers_ignored_surrogates_and_malformed_tails() {
        let inputs = [
            r#"{"session_types":[{"description":"\naaaaaaaa","label":"aaaaaaaaaaaaa\u0080"}]}"#
                .to_string(),
            r#"{"session_types":[{"unknown":"\uD800","label":"\u0080"}]}"#.to_string(),
            format!("{{\"unknown\":{}0{}}}", "[".repeat(300), "]".repeat(300)),
            format!("{{\"session_types\":[{{\"label\":\"{}\\q", "a".repeat(4096)),
            format!(
                "{{\"session_types\":[{{\"label\":\"{}\\u00",
                "a".repeat(4096)
            ),
        ];
        for input in inputs {
            let bytes = counting_workspace_bytes(input.as_bytes()).unwrap();
            let counted = count_fixture(input.as_bytes());
            let scratch = bytes - counting_error_bytes().unwrap();
            assert!(scratch >= counted.cursor.scratch.peak);
            if let Some(before) = counted.cursor.scratch_before_replay {
                assert!(scratch >= before.peak);
            }
        }
        let borrowed = format!("{{\"unknown\":\"{}\"}}", "a".repeat(4_194_290));
        let counted = count_fixture(borrowed.as_bytes());
        assert!(counted.result.is_ok());
        assert_eq!(borrowed.len(), 4 * 1024 * 1024);
        assert_eq!(counted.cursor.scratch.peak, 0);
        let workspace = counting_workspace_bytes(borrowed.as_bytes()).unwrap();
        assert_eq!(workspace, counting_error_bytes().unwrap() + 8);
        assert!(borrowed.len() + workspace < 8 * 1024 * 1024);
        for ending in ["\"}]}", ""] {
            let input = format!(
                "{{\"session_types\":[{{\"label\":\"{}{ending}",
                "a".repeat(4096)
            );
            let counted = count_fixture(input.as_bytes());
            assert_eq!(counted.cursor.scratch.peak, 0);
            assert_eq!(
                counting_workspace_bytes(input.as_bytes()).unwrap(),
                counting_error_bytes().unwrap() + 9
            );
        }
    }

    #[test]
    fn cursor_records_the_reserve_four_counterexample_in_schema_order() {
        let input = br#"{"session_types":[{"description":"\naaaaaaaa","label":"aaaaaaaaaaaaa\u0080","id":"agent","role":"agent","interaction":"interactive","lifecycle":"persistent","command":"agent"}]}"#;
        let counted = count_fixture(input);
        let result = &counted.result;
        let cursor = counted.cursor;
        assert!(result.is_ok());
        assert_eq!(cursor.failure, None);
        assert_eq!(cursor.offset, input.len());
        assert_eq!(cursor.scratch.capacity, 32);
        assert_eq!(cursor.scratch.peak, 48);
    }

    #[test]
    fn malformed_strings_record_the_prefix_before_serde_returns_an_error() {
        for ending in ["\\q\"}}]}", "\\u00"] {
            let input = format!(
                "{{\"session_types\":[{{\"label\":\"{}{ending}",
                "a".repeat(4096)
            );
            let counted = count_fixture(input.as_bytes());
            let result = &counted.result;
            let cursor = counted.cursor;
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("malformed input counted successfully"),
            };
            let typed =
                serde_json::from_slice::<super::super::RepoSessionTypesFile>(input.as_bytes())
                    .unwrap_err();
            assert_eq!(error.to_string(), typed.to_string());
            assert_eq!(
                cursor.failure.map(|(kind, _)| kind),
                Some(FailureKind::Lexical)
            );
            assert_eq!(cursor.scratch.len, 4096);
            assert_eq!(cursor.offset, error.column());
        }
    }

    #[test]
    fn escaped_keys_advance_once_and_unknown_strings_do_not_copy() {
        let input = br#"{"session_\u0074ypes":[{"la\u0062el":"Agent","unknown":"\uD800","environment":{"K":"\u0080"}}]}"#;
        let counted = count_fixture(input);
        let result = &counted.result;
        let cursor = counted.cursor;
        assert!(result.is_ok());
        assert_eq!(cursor.failure, None);
        assert_eq!(cursor.offset, input.len());
    }

    #[test]
    fn ignored_depth_is_not_limited_to_serde_typed_recursion() {
        let input = format!("{{\"unknown\":{}0{}}}", "[".repeat(300), "]".repeat(300));
        let counted = count_fixture(input.as_bytes());
        let result = &counted.result;
        let cursor = counted.cursor;
        assert!(result.is_ok());
        assert_eq!(cursor.failure, None);
        assert!(cursor.scratch.capacity >= 299);
        assert_eq!(cursor.offset, input.len());
    }

    #[test]
    fn positional_forms_and_duplicate_tags_use_the_existing_seeds() {
        for input in [
            r#"[[["agent","Agent",null,null,"agent","interactive",[],"persistent",["shell_command"],"agent",[],["relative","sub"],{},[],[],null]]]"#,
            r#"{"session_types":[{"execution":{"mode":"shell_command","mode":"shell_command","x":["\n"]}}]}"#,
            r#"[[["agent"]]]"#,
            r#"[[],[]]"#,
        ] {
            let counted = count_fixture(input.as_bytes());
            let result = &counted.result;
            let cursor = counted.cursor;
            assert!(result.is_ok());
            assert_eq!(cursor.failure, None);
            assert_eq!(cursor.offset, input.len());
        }
    }

    #[test]
    fn trailing_data_keeps_the_serde_diagnostic() {
        let input = br#"{"session_types":[]} false"#;
        let counted = count_fixture(input);
        let result = &counted.result;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("trailing input counted successfully"),
        };
        let typed =
            serde_json::from_slice::<super::super::RepoSessionTypesFile>(input).unwrap_err();
        assert_eq!(error.to_string(), typed.to_string());
    }

    #[test]
    fn ignored_failure_discards_scratch_growth_after_the_serde_error() {
        let input = format!(
            "{{\"unknown\":[0 1,{}0{}]}}",
            "[".repeat(300),
            "]".repeat(300),
        );
        let counted = count_fixture(input.as_bytes());
        let result = &counted.result;
        let cursor = counted.cursor;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("missing comma accepted"),
        };
        let typed = serde_json::from_slice::<super::super::RepoSessionTypesFile>(input.as_bytes())
            .unwrap_err();
        assert_eq!(error.to_string(), typed.to_string());
        assert_eq!(cursor.offset, error.column());
        assert_eq!(
            cursor.failure,
            Some((FailureKind::Structure, error.column()))
        );
        assert_eq!(cursor.scratch.capacity, 0);
        assert_eq!(cursor.scratch.peak, 0);
        assert!(cursor.scratch_before_replay.unwrap().peak > 0);
    }

    #[test]
    fn ignored_failure_keeps_capacity_from_earlier_strings_and_frames() {
        let input = br#"{"session_types":[{"label":"\naaaaaaaa","unknown":[[0}]}]}"#;
        let counted = count_fixture(input);
        let result = &counted.result;
        let cursor = counted.cursor;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("mismatched closer accepted"),
        };
        let typed =
            serde_json::from_slice::<super::super::RepoSessionTypesFile>(input).unwrap_err();
        assert_eq!(error.to_string(), typed.to_string());
        assert_eq!(cursor.offset, error.column());
        assert_eq!(cursor.scratch.capacity, 16);
        assert_eq!(cursor.scratch.peak, 24);
    }

    #[test]
    fn numeric_failures_use_serde_positions_without_a_second_number_parser() {
        for input in [
            "{\n\"session_types\":[{\"label\":1e+}]}",
            "{\n\"session_types\":[{\"label\":01}]}",
            "{\n\"session_types\":[{\"label\":1e9999}]}",
        ] {
            let counted = count_fixture(input.as_bytes());
            let result = &counted.result;
            let cursor = counted.cursor;
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("invalid number accepted"),
            };
            let typed =
                serde_json::from_slice::<super::super::RepoSessionTypesFile>(input.as_bytes())
                    .unwrap_err();
            assert_eq!(error.to_string(), typed.to_string());
            let line_start = input
                .as_bytes()
                .iter()
                .position(|byte| *byte == b'\n')
                .unwrap()
                + 1;
            assert_eq!(cursor.offset, line_start + error.column());
            assert_eq!(
                cursor.failure.map(|(_, offset)| offset),
                Some(cursor.offset)
            );
        }
    }
}
