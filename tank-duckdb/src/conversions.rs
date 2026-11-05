use libduckdb_sys::{
    duckdb_date, duckdb_decimal, duckdb_hugeint, duckdb_interval, duckdb_time, duckdb_timestamp,
    duckdb_uhugeint,
};
use rust_decimal::Decimal;
use std::cmp::{max, min};
use tank_core::Interval;
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time, macros::date};

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
    let width = if width == 0 || width < scale {
        let mut digits = 1_u32;
        let mut magnitude = v.mantissa().unsigned_abs();
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
        value: i128_to_duckdb_hugeint(v.mantissa()),
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
