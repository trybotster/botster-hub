//! The control framing on a non-blocking socket (HC TR-1): `[u32 big-endian length][UTF-8 JSON]`.
//!
//! - [`FrameReader`] reads **exactly** one frame: it asks the socket only for the bytes that the frame still lacks, so it
//!   never takes a byte past the frame (design section 1: a route client may pipeline terminal bytes after
//!   `route_connect`, `tb_l2_no_terminal_frame_read_by_hub`). It checks the length against the bound before it allocates
//!   (EN-5, A.1 `frame_too_large`).
//! - [`OutBuf`] is a connection's bounded outgoing bytes, written on readiness and never blocking (design section 3).

use std::collections::VecDeque;
use std::io::{self, Read, Write};

/// Why no frame was read.
#[derive(Debug)]
pub enum ReadError {
    /// The length is over the bound. Nothing of the body was read or allocated.
    TooLarge {
        len: u64,
        max: u64,
    },
    /// The peer closed the connection.
    Eof,
    Io(io::Error),
}

#[derive(Debug)]
enum Part {
    Prefix { bytes: [u8; 4], have: usize },
    Body { body: Vec<u8>, have: usize },
}

/// One frame in progress.
#[derive(Debug)]
pub struct FrameReader {
    part: Part,
}

impl Default for FrameReader {
    fn default() -> Self {
        FrameReader {
            part: Part::Prefix {
                bytes: [0; 4],
                have: 0,
            },
        }
    }
}

fn read_some(r: &mut impl Read, into: &mut [u8]) -> Result<Option<usize>, ReadError> {
    loop {
        return match r.read(into) {
            Ok(0) => Err(ReadError::Eof),
            Ok(n) => Ok(Some(n)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => Err(ReadError::Io(e)),
        };
    }
}

impl FrameReader {
    /// Reads until one frame is complete (`Some(body)`), or the socket has no more bytes now (`None`).
    pub fn read(&mut self, r: &mut impl Read, max: u64) -> Result<Option<Vec<u8>>, ReadError> {
        loop {
            match &mut self.part {
                Part::Prefix { bytes, have } => {
                    let Some(n) = read_some(r, &mut bytes[*have..])? else {
                        return Ok(None);
                    };
                    *have += n;
                    if *have < 4 {
                        continue;
                    }
                    let len = u64::from(u32::from_be_bytes(*bytes));
                    if len > max {
                        return Err(ReadError::TooLarge { len, max });
                    }
                    self.part = Part::Body {
                        body: vec![0; len as usize],
                        have: 0,
                    };
                }
                Part::Body { body, have } => {
                    if *have == body.len() {
                        let body = std::mem::take(body);
                        *self = FrameReader::default();
                        return Ok(Some(body));
                    }
                    let Some(n) = read_some(r, &mut body[*have..])? else {
                        return Ok(None);
                    };
                    *have += n;
                }
            }
        }
    }

    /// Whether no byte of a frame was read yet.
    pub fn is_idle(&self) -> bool {
        matches!(self.part, Part::Prefix { have: 0, .. })
    }
}

/// The prefixed form of one frame body.
pub fn framed(body: &[u8]) -> Vec<u8> {
    let len = u32::try_from(body.len()).expect("a frame body is under 4 GiB");
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// A connection's outgoing bytes, bounded by `max` bytes.
#[derive(Debug)]
pub struct OutBuf {
    bytes: VecDeque<u8>,
    max: usize,
}

/// The frame does not fit the outgoing bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Full;

impl OutBuf {
    pub fn new(max: usize) -> OutBuf {
        OutBuf {
            bytes: VecDeque::new(),
            max,
        }
    }

    /// Appends one whole frame, or nothing.
    pub fn push(&mut self, frame: &[u8]) -> Result<(), Full> {
        if self.bytes.len() + frame.len() > self.max {
            return Err(Full);
        }
        self.bytes.extend(frame);
        Ok(())
    }

    /// The free bytes.
    pub fn room(&self) -> usize {
        self.max - self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Writes what the socket takes now. `Ok(true)` when nothing is left.
    pub fn flush(&mut self, w: &mut impl Write) -> io::Result<bool> {
        while !self.bytes.is_empty() {
            let (front, _) = self.bytes.as_slices();
            match w.write(front) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => {
                    self.bytes.drain(..n);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixStream;

    fn pair() -> (UnixStream, UnixStream) {
        let (a, b) = UnixStream::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        b.set_nonblocking(true).unwrap();
        (a, b)
    }

    #[test]
    fn a_frame_split_across_reads_completes_and_leaves_the_next_bytes() {
        let (mut hub, mut client) = pair();
        let mut reader = FrameReader::default();
        let frame = framed(b"{\"a\":1}");
        client.write_all(&frame[..2]).unwrap();
        assert!(reader.read(&mut hub, 64).unwrap().is_none());
        assert!(!reader.is_idle());
        client.write_all(&frame[2..6]).unwrap();
        assert!(reader.read(&mut hub, 64).unwrap().is_none());
        // The rest of the frame, then pipelined route bytes in the same write.
        let mut rest = frame[6..].to_vec();
        rest.extend_from_slice(&[0, 0, 0, 1, 0x83]);
        client.write_all(&rest).unwrap();
        assert_eq!(reader.read(&mut hub, 64).unwrap().unwrap(), b"{\"a\":1}");
        assert!(reader.is_idle());
        let mut left = [0u8; 8];
        let n = hub.read(&mut left).unwrap();
        assert_eq!(
            &left[..n],
            &[0, 0, 0, 1, 0x83],
            "the reader took no byte past the frame"
        );
    }

    #[test]
    fn a_length_over_the_bound_is_refused_from_the_prefix() {
        let (mut hub, mut client) = pair();
        client.write_all(&[0, 0, 1, 0]).unwrap();
        match FrameReader::default().read(&mut hub, 255) {
            Err(ReadError::TooLarge { len: 256, max: 255 }) => {}
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_closed_peer_is_eof() {
        let (mut hub, client) = pair();
        drop(client);
        assert!(matches!(
            FrameReader::default().read(&mut hub, 64),
            Err(ReadError::Eof)
        ));
    }

    #[test]
    fn an_empty_body_is_a_frame() {
        let (mut hub, mut client) = pair();
        client.write_all(&framed(b"")).unwrap();
        assert_eq!(
            FrameReader::default().read(&mut hub, 64).unwrap().unwrap(),
            b""
        );
    }

    #[test]
    fn the_out_buffer_takes_whole_frames_within_its_bound_and_flushes() {
        let (mut hub, mut client) = pair();
        let mut out = OutBuf::new(10);
        out.push(&[1; 6]).unwrap();
        assert_eq!(out.push(&[2; 5]), Err(Full));
        assert_eq!(out.room(), 4);
        out.push(&[3; 4]).unwrap();
        assert!(out.flush(&mut hub).unwrap());
        assert!(out.is_empty());
        let mut got = [0u8; 16];
        let n = client.read(&mut got).unwrap();
        assert_eq!(&got[..n], &[1, 1, 1, 1, 1, 1, 3, 3, 3, 3]);
    }
}
