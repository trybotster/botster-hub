//! The control framing of `local-stream` (HC TR-1): `[u32 big-endian length][UTF-8 JSON]`, where the length counts the
//! JSON bytes and is at most the bound in force. The route handshake uses the same framing (TB-L2).
//!
//! A reader checks the length against its bound **before** it allocates (HC EN-5, A.1 `frame_too_large`), and reads
//! exactly the frame: never a byte past it, so the bytes that follow stay in the stream.

use std::fmt;
use std::io::{self, Read, Write};

/// A frame that cannot be read or written.
#[derive(Debug)]
pub enum FrameError {
    /// The length is over the bound (A.1 `frame_too_large`). Nothing of the body was read or allocated.
    TooLarge {
        len: u64,
        max: u64,
    },
    Io(io::Error),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::TooLarge { len, max } => {
                write!(f, "a frame of {len} bytes is over the bound of {max} bytes")
            }
            FrameError::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for FrameError {}

impl From<io::Error> for FrameError {
    fn from(error: io::Error) -> Self {
        FrameError::Io(error)
    }
}

/// Writes one frame. A body over `max` (or over `u32::MAX`) is refused before any byte is written.
pub fn write_frame(w: &mut impl Write, body: &[u8], max: u64) -> Result<(), FrameError> {
    let len = body.len() as u64;
    let prefix = u32::try_from(len)
        .ok()
        .filter(|_| len <= max)
        .ok_or(FrameError::TooLarge { len, max })?;
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&prefix.to_be_bytes());
    frame.extend_from_slice(body);
    w.write_all(&frame)?;
    w.flush()?;
    Ok(())
}

/// Reads one frame's body. It reads the 4-byte length, checks it against `max`, then reads exactly that many bytes.
pub fn read_frame(r: &mut impl Read, max: u64) -> Result<Vec<u8>, FrameError> {
    let mut prefix = [0u8; 4];
    r.read_exact(&mut prefix)?;
    let len = u64::from(u32::from_be_bytes(prefix));
    if len > max {
        return Err(FrameError::TooLarge { len, max });
    }
    let mut body = vec![0u8; len as usize];
    r.read_exact(&mut body)?;
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn a_frame_is_a_big_endian_length_then_the_body() {
        let mut out = Vec::new();
        write_frame(&mut out, b"{\"a\":1}", 64).unwrap();
        assert_eq!(&out[..4], &[0, 0, 0, 7]);
        assert_eq!(&out[4..], b"{\"a\":1}");
        let body = read_frame(&mut Cursor::new(out), 64).unwrap();
        assert_eq!(body, b"{\"a\":1}");
    }

    #[test]
    fn a_body_at_the_bound_passes_and_one_byte_more_is_refused_before_writing() {
        let mut out = Vec::new();
        write_frame(&mut out, &[b'x'; 8], 8).unwrap();
        let mut refused = Vec::new();
        match write_frame(&mut refused, &[b'x'; 9], 8) {
            Err(FrameError::TooLarge { len: 9, max: 8 }) => {}
            other => panic!("{other:?}"),
        }
        assert!(refused.is_empty(), "nothing is written");
    }

    #[test]
    fn a_length_over_the_bound_is_refused_before_the_body_is_read() {
        // A length of 4 GiB - 1 with no body: the reader must refuse it from the prefix alone, with no allocation.
        let mut input = Cursor::new(vec![0xff, 0xff, 0xff, 0xff]);
        match read_frame(&mut input, 1024) {
            Err(FrameError::TooLarge { len, max: 1024 }) => assert_eq!(len, u64::from(u32::MAX)),
            other => panic!("{other:?}"),
        }
        assert_eq!(input.position(), 4, "only the prefix was read");
    }

    #[test]
    fn the_reader_takes_exactly_one_frame_and_leaves_the_next_bytes() {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, b"{}", 64).unwrap();
        bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x01, 0x83]);
        let mut input = Cursor::new(bytes);
        assert_eq!(read_frame(&mut input, 64).unwrap(), b"{}");
        assert_eq!(
            input.position(),
            6,
            "the following route bytes are untouched"
        );
    }

    #[test]
    fn a_short_stream_is_an_io_error() {
        let mut input = Cursor::new(vec![0, 0, 0, 5, b'{']);
        assert!(matches!(read_frame(&mut input, 64), Err(FrameError::Io(_))));
    }
}
