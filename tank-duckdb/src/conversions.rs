use libduckdb_sys::{
    duckdb_date, duckdb_decimal, duckdb_hugeint, duckdb_interval, duckdb_time, duckdb_timestamp,
    duckdb_uhugeint,
};
use anyhow::anyhow;
use rust_decimal::Decimal;
use std::cmp::{max, min};
use tank_core::{ErrorContext, Interval, Result};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time, macros::date};
use uuid::Uuid;

const EPOCH_JULIAN_DAY: i32 = date!(1970 - 01 - 01).to_julian_day();

pub(crate) fn i128_to_duckdb_hugeint(v: i128) -> duckdb_hugeint {
    duckdb_hugeint {
        lower: v as u64,
        upper: (v >> 64) as i64,
    }
}

pub(crate) fn duckdb_hugeint_to_i128(v: &duckdb_hugeint) -> i128 {
    (v.upper as i128) << 64 | v.lower as i128
}

pub(crate) fn u128_to_duckdb_uhugeint(v: u128) -> duckdb_uhugeint {
    duckdb_uhugeint {
        lower: v as u64,
        upper: (v >> 64) as u64,
    }
}

pub(crate) fn duckdb_uhugeint_to_u128(v: &duckdb_uhugeint) -> u128 {
    (v.upper as u128) << 64 | v.lower as u128
}

pub(crate) fn decimal_to_duckdb_decimal(v: &Decimal, width: u8, scale: u8) -> duckdb_decimal {
    let scale = min(scale, 38);
    let d_scale = v.scale() as u8;
    let mantissa = if d_scale <= scale {
        v.mantissa()
            .saturating_mul(10_i128.pow((scale - d_scale) as u32))
    } else {
        let div = 10_i128.pow((d_scale - scale) as u32);
        (v.mantissa() + (div / 2) * v.mantissa().signum()) / div
    };
    let width = if width == 0 || width < scale {
        let mut digits = 1_u32;
        let mut magnitude = mantissa.unsigned_abs();
        while magnitude >= 10 {
            magnitude /= 10;
            digits += 1;
        }
        max(scale as u32, digits).clamp(1, 38) as u8
    } else {
        min(width, 38)
    };
    duckdb_decimal {
        width,
        scale,
        value: i128_to_duckdb_hugeint(mantissa),
    }
}

// pub(crate) fn duckdb_decimal_to_decimal(v: &duckdb_decimal) -> Decimal {
//     Decimal::new(duckdb_hugeint_to_i128(&v.value) as i64, v.scale as u32)
// }

pub(crate) fn date_to_duckdb_date(v: &Date) -> duckdb_date {
    duckdb_date {
        days: (*v - date!(1970 - 01 - 01)).whole_days() as i32,
    }
}

pub(crate) fn time_to_duckdb_time(v: &Time) -> duckdb_time {
    duckdb_time {
        micros: (*v - Time::MIDNIGHT).whole_microseconds() as i64,
    }
}

pub(crate) fn primitive_date_time_to_duckdb_timestamp(v: &PrimitiveDateTime) -> duckdb_timestamp {
    duckdb_timestamp {
        micros: (v.assume_utc().unix_timestamp_nanos() / 1000) as i64,
    }
}

pub(crate) fn offsetdatetime_to_duckdb_timestamp(v: &OffsetDateTime) -> duckdb_timestamp {
    duckdb_timestamp {
        micros: (v.to_utc().unix_timestamp_nanos() / 1000) as i64,
    }
}

pub(crate) fn interval_to_duckdb_interval(v: &Interval) -> duckdb_interval {
    duckdb_interval {
        months: v.months as i32,
        days: v.days as i32,
        micros: (v.nanos / 1000) as i64,
    }
}

#[inline(always)]
pub(crate) fn duckdb_date_to_date(days: i32) -> Result<Date> {
    Date::from_julian_day(days + EPOCH_JULIAN_DAY).context("While extracting a date value")
}

#[inline(always)]
pub(crate) fn duckdb_time_to_time(micros: i64) -> Result<Time> {
    if !(0..86_400_000_000).contains(&micros) {
        return Err(anyhow!("While extracting a time value: out of range"));
    }
    let secs = (micros / 1_000_000) as u32;
    Time::from_hms_micro(
        (secs / 3600) as u8,
        ((secs / 60) % 60) as u8,
        (secs % 60) as u8,
        (micros % 1_000_000) as u32,
    )
    .context("While extracting a time value")
}

#[inline(always)]
pub(crate) fn duckdb_time_ns_to_time(nanos: i64) -> Result<Time> {
    if !(0..86_400_000_000_000).contains(&nanos) {
        return Err(anyhow!("While extracting a time value: out of range"));
    }
    let secs = (nanos / 1_000_000_000) as u32;
    Time::from_hms_nano(
        (secs / 3600) as u8,
        ((secs / 60) % 60) as u8,
        (secs % 60) as u8,
        (nanos % 1_000_000_000) as u32,
    )
    .context("While extracting a time value")
}

#[inline(always)]
pub(crate) fn nanos_to_primitive_date_time(nanos: i128) -> Result<PrimitiveDateTime> {
    let date_time =
        OffsetDateTime::from_unix_timestamp_nanos(nanos).context("While extracting a timestamp")?;
    Ok(PrimitiveDateTime::new(date_time.date(), date_time.time()))
}

#[inline(always)]
pub(crate) fn uuid_from_duckdb_hugeint(v: &duckdb_hugeint) -> Uuid {
    Uuid::from_u64_pair(v.upper as u64 ^ (1 << 63), v.lower)
}

#[inline(always)]
pub(crate) fn duckdb_interval_to_interval(v: duckdb_interval) -> Interval {
    Interval::new(v.months, v.days, v.micros * 1_000)
}

#[inline(always)]
pub(crate) fn uuid_to_duckdb_hugeint(v: &Uuid) -> duckdb_hugeint {
    let (upper, lower) = v.as_u64_pair();
    duckdb_hugeint {
        lower,
        upper: (upper ^ (1 << 63)) as i64,
    }
}
