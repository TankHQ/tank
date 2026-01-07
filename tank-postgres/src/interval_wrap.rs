use byteorder::{NetworkEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use std::{error::Error, io::Cursor};
use tank_core::Interval;

#[derive(Debug)]
pub(crate) struct IntervalWrap(pub(crate) Interval);

impl ToSql for IntervalWrap {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        if !matches!(*ty, Type::INTERVAL) {
            return Err(
                tank_core::Error::msg(format!("Cannot write Interval into type: {}", ty)).into(),
            );
        }
        let micros_all = self.0.nanos / 1000;
        let micros = micros_all.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
        let days = (self.0.days as i128 + (micros_all - micros as i128) / Interval::MICROS_IN_DAY)
            .clamp(i32::MIN as i128, i32::MAX as i128) as i32;
        let months = self.0.months.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
        out.put_i64(micros);
        out.put_i32(days);
        out.put_i32(months);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        matches!(*ty, Type::INTERVAL)
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for IntervalWrap {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if !matches!(*ty, Type::INTERVAL) {
            return Err(tank_core::Error::msg(format!(
                "Cannot read Interval sql from type: {}",
                ty
            ))
            .into());
        }
        let mut result = IntervalWrap(Default::default());
        let mut raw = Cursor::new(raw);
        result.0.nanos = raw.read_i64::<NetworkEndian>()? as i128 * 1000;
        result.0.days = raw.read_i32::<NetworkEndian>()? as i64;
        result.0.months = raw.read_i32::<NetworkEndian>()? as i64;
        Ok(result)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::INTERVAL)
    }
}

impl From<IntervalWrap> for Interval {
    fn from(value: IntervalWrap) -> Self {
        value.0
    }
}
