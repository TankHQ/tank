use crate::{Error, Result};
use std::{
    hash::Hash,
    ops::{Add, AddAssign, Neg, Sub, SubAssign},
};

/// Time interval units.
///
/// Used for conversion and formatting.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum IntervalUnit {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Microsecond,
    Nanosecond,
}

impl IntervalUnit {
    pub fn from_bitmask(mask: u8) -> Result<IntervalUnit> {
        Ok(match mask {
            1 => IntervalUnit::Nanosecond,
            2 => IntervalUnit::Microsecond,
            4 => IntervalUnit::Second,
            8 => IntervalUnit::Minute,
            16 => IntervalUnit::Hour,
            32 => IntervalUnit::Day,
            64 => IntervalUnit::Month,
            128 => IntervalUnit::Year,
            _ => return Err(Error::msg("Invalid mask, it must be a single bit on")),
        })
    }
}

/// Calendar-aware time interval.
///
/// Stores months, days, and nanoseconds separately to preserve semantics.
#[derive(Default, Debug, Clone, Copy)]
pub struct Interval {
    pub months: i64,
    pub days: i64,
    pub nanos: i128,
}

impl Interval {
    pub const ZERO: Interval = Interval {
        months: 0,
        days: 0,
        nanos: 0,
    };

    pub const DAYS_IN_MONTH: f64 = 30.0;
    pub const DAYS_IN_MONTH_AVG: f64 = 30.436875;
    pub const SECS_IN_DAY: i64 = 60 * 60 * 24;
    pub const NANOS_IN_SEC: i128 = 1_000_000_000;
    pub const NANOS_IN_HOUR: i128 = Self::NANOS_IN_SEC * 3600;
    pub const NANOS_IN_DAY: i128 = Self::SECS_IN_DAY as i128 * Self::NANOS_IN_SEC;
    pub const MICROS_IN_DAY: i128 = Self::SECS_IN_DAY as i128 * 1_000_000;

    pub const fn new(months: i64, days: i64, nanos: i128) -> Self {
        Self {
            months,
            days,
            nanos,
        }
    }

    pub const fn from_duration(duration: &std::time::Duration) -> Self {
        Self {
            months: 0,
            days: 0,
            nanos: duration.as_nanos() as i128,
        }
    }

    pub const fn from_nanos(value: i128) -> Self {
        Self {
            months: 0,
            days: (value / Self::NANOS_IN_DAY) as _,
            nanos: (value % Self::NANOS_IN_DAY),
        }
    }

    pub const fn from_micros(value: i128) -> Self {
        const MICROS_IN_DAY: i128 = (Interval::SECS_IN_DAY * 1_000_000) as _;
        Self {
            months: 0,
            days: (value / MICROS_IN_DAY) as _,
            nanos: (value % MICROS_IN_DAY) * 1_000,
        }
    }

    pub const fn from_millis(value: i128) -> Self {
        const MILLIS_IN_DAY: i128 = (Interval::SECS_IN_DAY * 1_000) as _;
        Self {
            months: 0,
            days: (value / MILLIS_IN_DAY) as _,
            nanos: ((value % MILLIS_IN_DAY) * 1_000_000),
        }
    }

    pub const fn from_secs(value: i64) -> Self {
        Self {
            months: 0,
            days: (value / Self::SECS_IN_DAY) as _,
            nanos: ((value % Self::SECS_IN_DAY) * 1_000_000_000) as _,
        }
    }

    pub const fn from_mins(value: i64) -> Self {
        const MINS_IN_DAYS: i64 = 60 * 24;
        let days = value / MINS_IN_DAYS;
        let nanos = (value % MINS_IN_DAYS) * Interval::NANOS_IN_SEC as i64 * 60;
        Self {
            months: 0,
            days,
            nanos: nanos as _,
        }
    }

    pub const fn from_hours(value: i64) -> Self {
        Self {
            months: 0,
            days: (value / 24),
            nanos: ((value % 24) * Interval::NANOS_IN_HOUR as i64) as _,
        }
    }

    pub const fn from_days(value: i64) -> Self {
        Self {
            months: 0,
            days: value,
            nanos: 0,
        }
    }

    pub const fn from_weeks(value: i64) -> Self {
        Self {
            months: 0,
            days: value * 7,
            nanos: 0,
        }
    }

    pub const fn from_months(value: i64) -> Self {
        Self {
            months: value,
            days: 0,
            nanos: 0,
        }
    }

    pub const fn from_years(value: i64) -> Self {
        Self {
            months: value * 12,
            days: 0,
            nanos: 0,
        }
    }

    /// Deconstruct into (hours, minutes, seconds, nanoseconds).
    pub const fn as_hmsns(&self) -> (i128, u8, u8, u32) {
        let mut nanos = self.nanos;
        let mut hours = self.nanos / Self::NANOS_IN_HOUR;
        nanos %= Self::NANOS_IN_HOUR;
        hours += ((self.months * 30 + self.days) * 24) as i128;
        let m = nanos / (60 * Self::NANOS_IN_SEC);
        nanos %= 60 * Self::NANOS_IN_SEC;
        let s = nanos / Self::NANOS_IN_SEC;
        nanos %= Self::NANOS_IN_SEC;
        (hours, m as _, s as _, nanos as _)
    }

        pub const fn is_zero(&self) -> bool {
        self.months == 0 && self.days == 0 && self.nanos == 0
    }

    /// Convert to `std::time::Duration` (approximate).
    ///
    /// Uses `days_in_month` for conversion.
    pub const fn as_duration(&self, days_in_month: f64) -> std::time::Duration {
        let nanos = (self.months as f64) * days_in_month * (Interval::NANOS_IN_DAY as f64); // months
        let nanos = nanos as i128 + self.days as i128 * Interval::NANOS_IN_DAY; // days
        let nanos = nanos + self.nanos as i128;
        let secs = (nanos / Interval::NANOS_IN_SEC) as u64;
        let nanos = (nanos % Interval::NANOS_IN_SEC) as u32;
        std::time::Duration::new(secs, nanos)
    }

    pub const fn units_and_factors(&self) -> &[(IntervalUnit, i128)] {
        static UNITS: &[(IntervalUnit, i128)] = &[
            (IntervalUnit::Year, Interval::NANOS_IN_DAY * 30 * 12),
            (IntervalUnit::Month, Interval::NANOS_IN_DAY * 30),
            (IntervalUnit::Day, Interval::NANOS_IN_DAY),
            (IntervalUnit::Hour, Interval::NANOS_IN_SEC * 3600),
            (IntervalUnit::Minute, Interval::NANOS_IN_SEC * 60),
            (IntervalUnit::Second, Interval::NANOS_IN_SEC),
            (IntervalUnit::Microsecond, 1_000),
            (IntervalUnit::Nanosecond, 1),
        ];
        UNITS
    }

    /// Identify non-zero components (year, month, etc.).
    pub fn units_mask(&self) -> u8 {
        let mut mask = 0_u8;
        if self.months != 0 {
            if self.months % 12 == 0 {
                mask |= 1 << 7;
            } else if self.months != 0 {
                mask |= 1 << 6;
            }
        }
        let nanos = self.nanos + self.days as i128 * Interval::NANOS_IN_DAY;
        if nanos != 0 {
            for (i, &(_, factor)) in self.units_and_factors().iter().skip(2).enumerate() {
                if nanos % factor == 0 {
                    let shift = 5 - i; // i:0..5
                    mask |= 1 << shift;
                    break;
                }
            }
        }
        mask
    }

    /// Extract value for a specific unit (e.g. number of full years).
    pub fn unit_value(&self, unit: IntervalUnit) -> i128 {
        if unit == IntervalUnit::Year {
            self.months as i128 / 12
        } else if unit == IntervalUnit::Month {
            self.months as i128
        } else {
            let factor = *self
                .units_and_factors()
                .iter()
                .find_map(|(u, k)| if *u == unit { Some(k) } else { None })
                .expect("The unit must be present");
            (self.days as i128 * Interval::NANOS_IN_DAY + self.nanos) / factor
        }
    }
}

impl PartialEq for Interval {
    fn eq(&self, other: &Self) -> bool {
        self.months == other.months
            && self.days as i128 * Interval::NANOS_IN_DAY + self.nanos
                == other.days as i128 * Interval::NANOS_IN_DAY + other.nanos
    }
}

impl Eq for Interval {}

impl Hash for Interval {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.months.hash(state);
        (self.days as i128 * Interval::NANOS_IN_DAY + self.nanos).hash(state);
    }
}

macro_rules! sum_intervals {
    ($lhs:ident $op:tt $rhs:ident) => {{
        let days_total = $lhs.days as i128 $op $rhs.days as i128
            + $lhs.nanos / Interval::NANOS_IN_DAY $op $rhs.nanos / Interval::NANOS_IN_DAY;
        let days = days_total.clamp(i64::MIN as _, i64::MAX as _);
        let mut nanos = $lhs.nanos % Interval::NANOS_IN_DAY $op $rhs.nanos % Interval::NANOS_IN_DAY;
        if days != days_total {
            nanos += (days_total - days) * Interval::NANOS_IN_DAY;
        }
        Interval {
            months: $lhs.months $op $rhs.months,
            days: days as _,
            nanos,
        }
    }};
}

impl Add for Interval {
    type Output = Interval;
    fn add(self, rhs: Self) -> Self {
        sum_intervals!(self + rhs)
    }
}

impl AddAssign for Interval {
    fn add_assign(&mut self, rhs: Self) {
        *self = sum_intervals!(self + rhs);
    }
}

impl Sub for Interval {
    type Output = Interval;
    fn sub(self, rhs: Self) -> Self::Output {
        sum_intervals!(self - rhs)
    }
}

impl SubAssign for Interval {
    fn sub_assign(&mut self, rhs: Self) {
        *self = sum_intervals!(self - rhs);
    }
}

impl Neg for Interval {
    type Output = Interval;
    fn neg(self) -> Self::Output {
        Self::default() - self
    }
}

impl From<std::time::Duration> for Interval {
    fn from(value: std::time::Duration) -> Self {
        Self {
            months: 0,
            days: value.as_secs() as i64 / Interval::SECS_IN_DAY,
            nanos: (value.as_secs() as i64 % Interval::SECS_IN_DAY) as i128
                * Interval::NANOS_IN_SEC
                + value.subsec_nanos() as i128,
        }
    }
}

impl From<Interval> for std::time::Duration {
    fn from(value: Interval) -> Self {
        value.as_duration(Interval::DAYS_IN_MONTH)
    }
}

impl From<time::Duration> for Interval {
    fn from(value: time::Duration) -> Self {
        let seconds = value.whole_seconds();
        Self {
            months: 0,
            days: seconds / Interval::SECS_IN_DAY,
            nanos: ((seconds % Interval::SECS_IN_DAY) * Interval::NANOS_IN_SEC as i64
                + value.subsec_nanoseconds() as i64) as i128,
        }
    }
}

impl From<Interval> for time::Duration {
    fn from(value: Interval) -> Self {
        let seconds = ((value.days + value.months * Interval::DAYS_IN_MONTH as i64)
            * Interval::SECS_IN_DAY) as i128
            + value.nanos / Interval::NANOS_IN_SEC;
        let nanos = (value.nanos % Interval::NANOS_IN_SEC) as i32;
        time::Duration::new(seconds as i64, nanos)
    }
}
