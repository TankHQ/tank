use anyhow::anyhow;
use std::{hint, str};
use tank_core::Result;

/// Cursor over a ClickHouse `Native` byte buffer.
pub(crate) struct Cursor<'a> {
    pub(crate) data: &'a [u8],
    pub(crate) pos: usize,
}

impl<'a> Cursor<'a> {
    #[inline(always)]
    pub(crate) const fn new(data: &'a [u8], pos: usize) -> Self {
        Self { data, pos }
    }

    #[inline(always)]
    pub(crate) fn read_varuint(&mut self) -> Result<usize> {
        #[cold]
        #[inline(never)]
        fn rest(cursor: &mut Cursor<'_>, first: u8) -> Result<usize> {
            let mut value = (first & 0x7f) as u64;
            let mut shift = 7u32;
            while shift < 64 {
                let Some(&byte) = cursor.data.get(cursor.pos) else {
                    return Err(anyhow!("Unexpected end of ClickHouse Native varuint"));
                };
                cursor.pos += 1;
                value |= ((byte & 0x7f) as u64) << shift;
                if byte < 0x80 {
                    return usize::try_from(value)
                        .map_err(|_| anyhow!("ClickHouse Native varuint overflow"));
                }
                shift += 7;
            }
            Err(anyhow!("Malformed ClickHouse Native varuint"))
        }

        let Some(&first) = self.data.get(self.pos) else {
            hint::cold_path();
            return Err(anyhow!("Unexpected end of ClickHouse Native varuint"));
        };
        self.pos += 1;
        if first < 0x80 {
            return Ok(first as usize);
        }
        rest(self, first)
    }

    #[inline(always)]
    pub(crate) fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| anyhow!("ClickHouse Native slice length overflow"))?;
        let Some(slice) = self.data.get(self.pos..end) else {
            hint::cold_path();
            return Err(anyhow!(
                "Unexpected end of ClickHouse Native buffer (need {len} bytes at {})",
                self.pos
            ));
        };
        self.pos = end;
        Ok(slice)
    }

    #[inline(always)]
    pub(crate) fn read_str(&mut self) -> Result<&'a str> {
        let len = self.read_varuint()?;
        let bytes = self.read_bytes(len)?;
        str::from_utf8(bytes).map_err(|e| anyhow!("Invalid UTF-8 in ClickHouse Native header: {e}"))
    }

    #[inline(always)]
    pub(crate) fn read_u64_le(&mut self) -> Result<u64> {
        let bytes = self.read_bytes(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
}
