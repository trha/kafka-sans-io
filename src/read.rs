//! Reads fields in place, off the front of a body: reply fields for `wire`,
//! record fields for `batch`. Every short or malformed field is
//! `Fatal::Protocol`.

use core::ops::Range;

use zerocopy::{
    FromBytes, Immutable, KnownLayout, Unaligned,
    big_endian::{I16, I32},
};

use crate::error::Fatal;

#[derive(Clone)]
pub(crate) struct Cursor<'a> {
    body: &'a [u8],
    /// Bytes before `at` are read.
    at: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(body: &'a [u8]) -> Self {
        Cursor { body, at: 0 }
    }

    pub(crate) fn rest(&self) -> &'a [u8] {
        &self.body[self.at..]
    }

    /// A fixed layout in place.
    pub(crate) fn read<T: FromBytes + KnownLayout + Immutable + Unaligned>(
        &mut self,
    ) -> Result<&'a T, Fatal> {
        let (value, _) = T::ref_from_prefix(self.rest()).map_err(|_| Fatal::Protocol)?;
        self.at += size_of::<T>();
        Ok(value)
    }

    pub(crate) fn skip(&mut self, count: usize) -> Result<Range<usize>, Fatal> {
        if self.rest().len() < count {
            return Err(Fatal::Protocol);
        }
        let start = self.at;
        self.at += count;
        Ok(start..self.at)
    }

    pub(crate) fn take(&mut self, count: usize) -> Result<&'a [u8], Fatal> {
        let range = self.skip(count)?;
        Ok(&self.body[range])
    }

    /// Skips `count` items of `T`.
    pub(crate) fn skip_items<T>(&mut self, count: usize) -> Result<(), Fatal> {
        let bytes = count.checked_mul(size_of::<T>()).ok_or(Fatal::Protocol)?;
        self.skip(bytes).map(drop)
    }

    pub(crate) fn i16(&mut self) -> Result<i16, Fatal> {
        self.read::<I16>().map(|value| value.get())
    }

    pub(crate) fn i32(&mut self) -> Result<i32, Fatal> {
        self.read::<I32>().map(|value| value.get())
    }

    /// An i16 length, then the bytes; `None` for -1 (null).
    pub(crate) fn nullable_string(&mut self) -> Result<Option<&'a [u8]>, Fatal> {
        match self.i16()? {
            -1 => Ok(None),
            length => {
                let length = usize::try_from(length).map_err(|_| Fatal::Protocol)?;
                self.take(length).map(Some)
            }
        }
    }

    pub(crate) fn string(&mut self) -> Result<&'a [u8], Fatal> {
        self.nullable_string()?.ok_or(Fatal::Protocol)
    }

    /// An i32 element count, `None` for -1 (null).
    pub(crate) fn nullable_array(&mut self) -> Result<Option<usize>, Fatal> {
        match self.i32()? {
            -1 => Ok(None),
            count => usize::try_from(count)
                .map(Some)
                .map_err(|_| Fatal::Protocol),
        }
    }

    pub(crate) fn array(&mut self) -> Result<usize, Fatal> {
        self.nullable_array()?.ok_or(Fatal::Protocol)
    }

    /// An i32 length, then the bytes; `None` for -1 (null). Returns where
    /// the bytes are in the body.
    pub(crate) fn nullable_bytes(&mut self) -> Result<Option<Range<usize>>, Fatal> {
        match self.nullable_array()? {
            None => Ok(None),
            Some(length) => self.skip(length).map(Some),
        }
    }

    /// 7 bits per byte, low group first, at most 32 bits.
    pub(crate) fn unsigned_varint(&mut self) -> Result<u32, Fatal> {
        let mut value = 0u32;
        for shift in (0..32).step_by(7) {
            let byte = *self.read::<u8>()?;
            // The fifth byte holds the top 4 bits and ends the varint.
            if shift == 28 && byte > 0x0f {
                return Err(Fatal::Protocol);
            }
            value |= u32::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        unreachable!("the fifth byte ends the varint")
    }

    /// Zigzag, then 7 bits per byte, low group first, at most 10 bytes.
    pub(crate) fn varint(&mut self) -> Result<i64, Fatal> {
        let mut raw = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = *self.read::<u8>()?;
            raw |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok((raw >> 1) as i64 ^ -((raw & 1) as i64));
            }
        }
        Err(Fatal::Protocol)
    }

    /// A varint length, then the bytes; `None` for -1 (null).
    pub(crate) fn varint_bytes(&mut self) -> Result<Option<&'a [u8]>, Fatal> {
        match self.varint()? {
            -1 => Ok(None),
            length => {
                let length = usize::try_from(length).map_err(|_| Fatal::Protocol)?;
                self.take(length).map(Some)
            }
        }
    }

    /// A compact array: an unsigned varint of the element count plus one. A
    /// null (0) is malformed here.
    pub(crate) fn compact_array(&mut self) -> Result<usize, Fatal> {
        let count = self
            .unsigned_varint()?
            .checked_sub(1)
            .ok_or(Fatal::Protocol)?;
        usize::try_from(count).map_err(|_| Fatal::Protocol)
    }

    /// Skips a tagged field section: a count, then per field a tag, a size,
    /// and that many bytes. This crate reads no tagged field.
    pub(crate) fn tagged_fields(&mut self) -> Result<(), Fatal> {
        for _ in 0..self.unsigned_varint()? {
            let _tag = self.unsigned_varint()?;
            let size = self.unsigned_varint()?;
            self.skip(usize::try_from(size).map_err(|_| Fatal::Protocol)?)?;
        }
        Ok(())
    }

    pub(crate) fn end(&self) -> Result<(), Fatal> {
        if self.rest().is_empty() {
            Ok(())
        } else {
            Err(Fatal::Protocol)
        }
    }
}
