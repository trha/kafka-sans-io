//! Writes request fields into a fixed slice, or only counts them.

use heapless::Vec as ArrayVec;
use zerocopy::{Immutable, IntoBytes};

use crate::limits::MAX_PARTS;

/// Puts fields one after the other into a slice. A writer from `size` has
/// no slice and only counts: the count pass runs the same code as the write
/// pass, so the two cannot differ.
pub struct Writer<'b> {
    buf: Option<&'b mut [u8]>,
    /// Bytes put so far.
    written: usize,
}

impl<'b> Writer<'b> {
    /// Writes from the start of `buf`. A field past its end panics.
    pub fn new(buf: &'b mut [u8]) -> Self {
        Writer {
            buf: Some(buf),
            written: 0,
        }
    }

    /// Bytes `write` puts.
    pub fn size(write: impl FnOnce(&mut Writer<'_>)) -> usize {
        let mut counter = Writer {
            buf: None,
            written: 0,
        };
        write(&mut counter);
        counter.written
    }

    pub fn written(&self) -> usize {
        self.written
    }

    pub fn bytes(&mut self, bytes: &[u8]) {
        let end = self.written + bytes.len();
        if let Some(buf) = self.buf.as_deref_mut() {
            buf.get_mut(self.written..end)
                .expect("a field past the end of the slice")
                .copy_from_slice(bytes);
        }
        self.written = end;
    }

    /// A fixed layout as it lies in memory.
    pub fn put<T: IntoBytes + Immutable>(&mut self, value: &T) {
        self.bytes(value.as_bytes());
    }

    pub fn i16(&mut self, value: i16) {
        self.bytes(&value.to_be_bytes());
    }

    pub fn i32(&mut self, value: i32) {
        self.bytes(&value.to_be_bytes());
    }

    /// An i32 element count.
    pub fn array(&mut self, count: usize) {
        self.i32(i32::try_from(count).expect("an array below 2^31 items"));
    }

    /// An i16 length, then the bytes.
    pub fn string(&mut self, value: &str) {
        self.i16(i16::try_from(value.len()).expect("a string below 32 KiB"));
        self.bytes(value.as_bytes());
    }

    /// An i32 length, then the bytes.
    pub fn nullable_bytes(&mut self, value: &[u8]) {
        self.array(value.len());
        self.bytes(value);
    }

    /// 7 bits per byte, low group first.
    pub fn unsigned_varint(&mut self, value: u32) {
        let mut rest = value;
        while rest >= 0x80 {
            self.bytes(&[(rest as u8) | 0x80]);
            rest >>= 7;
        }
        self.bytes(&[rest as u8]);
    }

    /// The length plus one as an unsigned varint, then the bytes.
    pub fn compact_string(&mut self, value: &str) {
        let length = u32::try_from(value.len()).expect("a string below 4 GiB");
        self.unsigned_varint(length + 1);
        self.bytes(value.as_bytes());
    }

    /// A compact nullable string that is null: length 0.
    pub fn null_compact_string(&mut self) {
        self.unsigned_varint(0);
    }

    /// A tagged field section with no field.
    pub fn no_tagged_fields(&mut self) {
        self.unsigned_varint(0);
    }
}

/// Writes a topics array of non-flexible requests: each run of `items`
/// with one `topic` is one entry, the topic name and then an array of the
/// run's items. `item` writes one item. At most `MAX_PARTS` runs.
pub(super) fn topic_array<'t, T>(
    writer: &mut Writer<'_>,
    items: impl Clone + Iterator<Item = T>,
    topic: impl Fn(&T) -> &'t str,
    mut item: impl FnMut(&mut Writer<'_>, T),
) {
    let mut runs = ArrayVec::<usize, MAX_PARTS>::new();
    let mut last_topic = None;
    for value in items.clone() {
        let topic = topic(&value);
        if last_topic != Some(topic) {
            last_topic = Some(topic);
            runs.push(0).expect("at most MAX_PARTS topic runs");
        }
        if let Some(count) = runs.last_mut() {
            *count += 1;
        }
    }

    writer.array(runs.len());
    let mut last_topic = None;
    let mut run = 0;
    for value in items {
        let topic = topic(&value);
        if last_topic != Some(topic) {
            last_topic = Some(topic);
            writer.string(topic);
            writer.array(runs[run]);
            run += 1;
        }
        item(writer, value);
    }
}
