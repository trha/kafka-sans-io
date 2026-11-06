//! Byte buffers for the socket. Every Kafka frame is a 4-byte big-endian
//! length, then that many bytes.

use core::ops::Range;

use crate::{error::Fatal, wire::Writer};

/// Received bytes. The caller writes into `spare`, then calls `filled`.
/// Frames are taken off the front.
pub struct RxBuf {
    bytes: Box<[u8]>,
    /// Bytes in `start..end` are received and not yet taken.
    start: usize,
    end: usize,
    /// Counts `spare` and `clear` calls: each one ends every span.
    epoch: u32,
}

impl RxBuf {
    /// `capacity` bounds one whole frame, length field included.
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity > LENGTH_BYTES,
            "a frame needs more than its length field"
        );
        RxBuf {
            bytes: vec![0; capacity].into_boxed_slice(),
            start: 0,
            end: 0,
            epoch: 0,
        }
    }

    /// Room for the next read. Moves the bytes not yet taken to the front
    /// when the room at the back is gone. Ends every span.
    pub fn spare(&mut self) -> &mut [u8] {
        self.epoch = self.epoch.wrapping_add(1);
        if self.start == self.end {
            self.start = 0;
            self.end = 0;
        } else if self.end == self.bytes.len() {
            self.bytes.copy_within(self.start..self.end, 0);
            self.end -= self.start;
            self.start = 0;
        }
        &mut self.bytes[self.end..]
    }

    /// `count` bytes of `spare` now hold received data.
    pub fn filled(&mut self, count: usize) {
        assert!(
            self.end + count <= self.bytes.len(),
            "filled past the spare room"
        );
        self.end += count;
    }

    /// The body of the next whole frame, without its length field, or `None`
    /// until all its bytes arrive. A frame that cannot fit the buffer, or a
    /// negative length, is `Fatal::Protocol`.
    pub fn next_frame(&mut self) -> Result<Option<Frame<'_>>, Fatal> {
        let Some((length, rest)) = self.bytes[self.start..self.end].split_first_chunk() else {
            return Ok(None);
        };
        let body = usize::try_from(i32::from_be_bytes(*length)).map_err(|_| Fatal::Protocol)?;
        if LENGTH_BYTES + body > self.bytes.len() {
            return Err(Fatal::Protocol);
        }
        let Some(frame) = rest.get(..body) else {
            return Ok(None);
        };
        let at = self.start + LENGTH_BYTES;
        self.start = at + body;
        Ok(Some(Frame {
            body: frame,
            at,
            epoch: self.epoch,
        }))
    }

    /// The bytes of a span from a frame of this buffer. Panics when a
    /// `spare` or `clear` came after the frame.
    pub fn slice(&self, span: RxSpan) -> &[u8] {
        assert_eq!(span.epoch, self.epoch, "span read after spare or clear");
        &self.bytes[span.start..span.end]
    }

    /// Drops every byte, for a new connection. Ends every span.
    pub fn clear(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
        self.start = 0;
        self.end = 0;
    }

    /// Received bytes not yet taken as frames.
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
}

/// Bytes to send. Frames are added at the back; the caller sends from
/// `pending` and calls `consume` with the count the socket took.
pub struct TxBuf {
    /// Fixed at `new`.
    bytes: Box<[u8]>,
    /// Bytes in `start..end` are not yet sent.
    start: usize,
    end: usize,
}

impl TxBuf {
    pub fn new(capacity: usize) -> Self {
        TxBuf {
            bytes: vec![0; capacity].into_boxed_slice(),
            start: 0,
            end: 0,
        }
    }

    /// Adds one frame: the length field, then the body `write` puts. `write`
    /// runs twice, once to count the body and once to write it, so it must
    /// put the same fields both times. A frame larger than `room` is
    /// `Fatal::Protocol`, and leaves the buffer unchanged.
    pub fn push(&mut self, write: impl Fn(&mut Writer<'_>)) -> Result<(), Fatal> {
        let body = Writer::size(&write);
        let length = i32::try_from(body).map_err(|_| Fatal::Protocol)?;
        if LENGTH_BYTES + body > self.room() {
            return Err(Fatal::Protocol);
        }
        if self.end + LENGTH_BYTES + body > self.bytes.len() {
            self.bytes.copy_within(self.start..self.end, 0);
            self.end -= self.start;
            self.start = 0;
        }
        let frame = &mut self.bytes[self.end..self.end + LENGTH_BYTES + body];
        let (length_field, body_bytes) = frame.split_at_mut(LENGTH_BYTES);
        length_field.copy_from_slice(&length.to_be_bytes());
        let mut writer = Writer::new(body_bytes);
        write(&mut writer);
        assert_eq!(writer.written(), body, "frame body has another size");
        self.end += LENGTH_BYTES + body;
        Ok(())
    }

    pub fn pending(&self) -> &[u8] {
        &self.bytes[self.start..self.end]
    }

    pub fn consume(&mut self, count: usize) {
        assert!(
            self.start + count <= self.end,
            "consumed past the pending bytes"
        );
        self.start += count;
        if self.start == self.end {
            self.clear();
        }
    }

    /// Drops every byte not yet sent, for a closed connection.
    pub fn clear(&mut self) {
        self.start = 0;
        self.end = 0;
    }

    /// Free bytes for frames, length fields included.
    pub fn room(&self) -> usize {
        self.bytes.len() - self.pending().len()
    }
}

/// One whole frame taken off the receive buffer.
pub struct Frame<'a> {
    /// The frame without its length field.
    pub body: &'a [u8],
    /// Where `body` starts in the buffer.
    at: usize,
    epoch: u32,
}

impl Frame<'_> {
    /// The span of `range` of the body.
    pub fn span(&self, range: Range<usize>) -> RxSpan {
        assert!(
            range.start <= range.end && range.end <= self.body.len(),
            "span outside the frame"
        );
        RxSpan {
            epoch: self.epoch,
            start: self.at + range.start,
            end: self.at + range.end,
        }
    }
}

/// A place in the receive buffer, from a frame taken off it. Valid until the
/// next `RxBuf::spare` or `RxBuf::clear`, which may move or drop the bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RxSpan {
    epoch: u32,
    start: usize,
    end: usize,
}

impl RxSpan {
    /// Bytes in the span.
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }

    /// The span of `range` of this span.
    pub fn subspan(self, range: Range<usize>) -> RxSpan {
        assert!(
            range.start <= range.end && range.end <= self.len(),
            "span outside the span"
        );
        RxSpan {
            epoch: self.epoch,
            start: self.start + range.start,
            end: self.start + range.end,
        }
    }
}

const LENGTH_BYTES: usize = 4;
