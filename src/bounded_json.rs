//! JSON encoding with a fixed byte limit.

use serde::Serialize;
use std::io;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EncodeError {
    TooLarge,
    Serialize,
}

pub(crate) fn encode(value: &impl Serialize, limit: usize) -> Result<Vec<u8>, EncodeError> {
    let mut writer = CappedVecWriter::new(limit);
    if serde_json::to_writer(&mut writer, value).is_err() {
        return Err(if writer.exceeded {
            EncodeError::TooLarge
        } else {
            EncodeError::Serialize
        });
    }
    Ok(writer.bytes)
}

struct CappedVecWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl CappedVecWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(8 * 1024)),
            limit,
            exceeded: false,
        }
    }
}

impl io::Write for CappedVecWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("JSON byte limit exceeded"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
