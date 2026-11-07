use anyhow::anyhow;
use libduckdb_sys::v2::{duckdb_v2_hugeint_t, duckdb_v2_interval_t, duckdb_v2_uhugeint_t};
use rust_decimal::Decimal;
use std::cmp::{max, min};
use tank_core::{ErrorContext, Interval, Result};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time, macros::date};
use uuid::Uuid;

const EPOCH_DATE: Date = date!(1970 - 01 - 01);
const EPOCH_JULIAN_DAY: i32 = EPOCH_DATE.to_julian_day();

#[inline(always)]
pub(crate) fn i128_to_duckdb_hugeint(v: i128) -> duckdb_v2_hugeint_t {
    duckdb_v2_hugeint_t {
        lower: v as u64,
        upper: (v >> 64) as i64,
    }
}

#[inline(always)]
pub(crate) fn duckdb_hugeint_to_i128(v: &duckdb_v2_hugeint_t) -> i128 {
    ((v.upper as i128) << 64) | (v.lower as i128)
}

#[inline(always)]
pub(crate) fn u128_to_duckdb_uhugeint(v: u128) -> duckdb_v2_uhugeint_t {
    duckdb_v2_uhugeint_t {
        lower: v as u64,
        upper: (v >> 64) as u64,
    }
}

#[inline(always)]
pub(crate) fn duckdb_uhugeint_to_u128(v: &duckdb_v2_uhugeint_t) -> u128 {
    ((v.upper as u128) << 64) | (v.lower as u128)
}

#[inline(always)]
pub(crate) fn uuid_to_duckdb_hugeint(v: &Uuid) -> duckdb_v2_hugeint_t {
    let (upper, lower) = v.as_u64_pair();
    duckdb_v2_hugeint_t {
        lower,
        upper: (upper ^ (1 << 63)) as i64,
    }
}

#[inline(always)]
pub(crate) fn duckdb_hugeint_to_uuid(v: &duckdb_v2_hugeint_t) -> Uuid {
    Uuid::from_u64_pair((v.upper as u64) ^ (1 << 63), v.lower)
}

#[inline(always)]
pub(crate) fn decimal_to_duckdb_decimal(
    v: &Decimal,
    width: u8,
    scale: u8,
) -> (duckdb_v2_hugeint_t, u8, u8) {
    let scale = min(scale, 38);
    let mut digits = 1_u32;
    let mut magnitude = v.mantissa().unsigned_abs();
    while magnitude >= 10 {
        magnitude /= 10;
        digits += 1;
    }
    let width = max(width as u32, max(scale as u32, digits)).clamp(1, 38) as u8;
    (i128_to_duckdb_hugeint(v.mantissa()), width, scale)
}

#[inline(always)]
pub(crate) fn date_to_duckdb_date(v: &Date) -> i32 {
    (*v - EPOCH_DATE).whole_days() as i32
}

#[inline(always)]
pub(crate) fn duckdb_date_to_date(days: i32) -> Result<Date> {
    let julian = days
        .checked_add(EPOCH_JULIAN_DAY)
        .context("Error while creating extracting a date value: overflow")?;
    Date::from_julian_day(julian).context("Error while creating extracting a date value")
}

#[inline(always)]
pub(crate) fn time_to_duckdb_time(v: &Time) -> i64 {
    (*v - Time::MIDNIGHT).whole_microseconds() as i64
}

#[inline(always)]
pub(crate) fn duckdb_time_to_time(micros: i64) -> Result<Time> {
    if !(0..86_400_000_000).contains(&micros) {
        return Err(anyhow!(
            "Error while creating extracting a time value: out of range"
        ));
    }
    let secs = (micros / 1_000_000) as u32;
    let sub = (micros % 1_000_000) as u32;
    Time::from_hms_micro(
        (secs / 3600) as u8,
        ((secs / 60) % 60) as u8,
        (secs % 60) as u8,
        sub,
    )
    .context("Error while creating extracting a time value")
}

#[inline(always)]
pub(crate) fn duckdb_time_ns_to_time(nanos: i64) -> Result<Time> {
    if !(0..86_400_000_000_000).contains(&nanos) {
        return Err(anyhow!(
            "Error while creating extracting a time value: out of range"
        ));
    }
    let secs = (nanos / 1_000_000_000) as u32;
    let sub = (nanos % 1_000_000_000) as u32;
    Time::from_hms_nano(
        (secs / 3600) as u8,
        ((secs / 60) % 60) as u8,
        (secs % 60) as u8,
        sub,
    )
    .context("Error while creating extracting a time value")
}

#[inline(always)]
pub(crate) fn primitive_date_time_to_duckdb_timestamp(v: &PrimitiveDateTime) -> i64 {
    (v.assume_utc().unix_timestamp_nanos() / 1000) as i64
}

#[inline(always)]
pub(crate) fn offsetdatetime_to_duckdb_timestamp(v: &OffsetDateTime) -> i64 {
    (v.to_utc().unix_timestamp_nanos() / 1000) as i64
}

#[inline(always)]
pub(crate) fn nanos_to_primitive_date_time(nanos: i128) -> Result<PrimitiveDateTime> {
    let dt = OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .context("Error while creating a timestamp")?;
    Ok(PrimitiveDateTime::new(dt.date(), dt.time()))
}

#[inline(always)]
pub(crate) fn interval_to_duckdb_interval(v: &Interval) -> duckdb_v2_interval_t {
    duckdb_v2_interval_t {
        months: v.months,
        days: v.days,
        micros: (v.nanos / 1000),
    }
}

#[inline(always)]
pub(crate) fn duckdb_interval_to_interval(v: duckdb_v2_interval_t) -> Interval {
    Interval::new(v.months, v.days, v.micros * 1_000)
}
