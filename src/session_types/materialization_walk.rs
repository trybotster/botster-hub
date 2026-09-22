//! Dormant repository counting draft. No runtime caller is registered.
//!
//! The caller is the charged repository materialization path. This draft connects
//! the existing schema seed to one cursor through Serde's access interfaces.
//! It does not establish a construction permit. Funding the counting pass,
//! combining output and Content events, and failure dominance remain required.

use std::cell::Cell;
use std::fmt;

use serde::Deserializer;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};

#[cfg(test)]
use super::definition_budget::{DefinitionStorage, Shape};
use super::scratch_budget::ScratchStorage;

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

type State<'a, 'input> = &'a Cell<Cursor<'input>>;

fn update<'input>(state: State<'_, 'input>, action: impl FnOnce(&mut Cursor<'input>)) {
    let mut cursor = state.get();
    action(&mut cursor);
    state.set(cursor);
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

/// Test-only caller until the counting allocation permit and combined events exist.
/// This result is not a materialization permit and cannot construct production output.
#[cfg(test)]
fn count_fixture(input: &[u8]) -> (Result<DefinitionStorage, serde_json::Error>, Cursor<'_>) {
    let state = Cell::new(Cursor::new(input));
    let mut decoder = serde_json::Deserializer::from_slice(input);
    let result = Shape::File
        .deserialize(CountingDeserializer {
            inner: &mut decoder,
            state: &state,
            key: false,
        })
        .and_then(|value| decoder.end().map(|()| value));
    if let Err(error) = &result {
        update(&state, |cursor| cursor.reconcile_error(error));
    }
    (result, state.get())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_records_the_reserve_four_counterexample_in_schema_order() {
        let input = br#"{"session_types":[{"description":"\naaaaaaaa","label":"aaaaaaaaaaaaa\u0080","id":"agent","role":"agent","interaction":"interactive","lifecycle":"persistent","command":"agent"}]}"#;
        let (result, cursor) = count_fixture(input);
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
            let (result, cursor) = count_fixture(input.as_bytes());
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
        let (result, cursor) = count_fixture(input);
        assert!(result.is_ok());
        assert_eq!(cursor.failure, None);
        assert_eq!(cursor.offset, input.len());
    }

    #[test]
    fn ignored_depth_is_not_limited_to_serde_typed_recursion() {
        let input = format!("{{\"unknown\":{}0{}}}", "[".repeat(300), "]".repeat(300));
        let (result, cursor) = count_fixture(input.as_bytes());
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
            let (result, cursor) = count_fixture(input.as_bytes());
            assert!(result.is_ok());
            assert_eq!(cursor.failure, None);
            assert_eq!(cursor.offset, input.len());
        }
    }

    #[test]
    fn trailing_data_keeps_the_serde_diagnostic() {
        let input = br#"{"session_types":[]} false"#;
        let (result, _) = count_fixture(input);
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
        let (result, cursor) = count_fixture(input.as_bytes());
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
        let (result, cursor) = count_fixture(input);
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
            let (result, cursor) = count_fixture(input.as_bytes());
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
