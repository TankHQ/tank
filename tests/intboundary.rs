#[cfg(test)]
mod tests {
    use tank_core::{AsValue, Value};
    // For every (target, source-variant) pair, a value in range must convert
    // and a value out of range must error. Never silently wrap.
    macro_rules! conv {
        ($t:ty, $variant:ident, $v:expr) => { <$t>::try_from_value(Value::$variant(Some($v))) };
    }
    macro_rules! ok { ($t:ty,$var:ident,$v:expr,$e:expr) => { assert_eq!(conv!($t,$var,$v).unwrap(), $e, "{} from {} {:?}", stringify!($t), stringify!($var), $v); }; }
    macro_rules! err { ($t:ty,$var:ident,$v:expr) => { assert!(conv!($t,$var,$v).is_err(), "{} from {} {:?} should error", stringify!($t), stringify!($var), $v); }; }
    #[test]
    fn i8_bounds() {
        ok!(i8, Int16, 127i16, 127); err!(i8, Int16, 128i16); err!(i8, Int16, -129i16);
        ok!(i8, Int32, -128i32, -128); err!(i8, Int32, 128i32);
        ok!(i8, Int64, 0i64, 0); err!(i8, Int64, i64::MAX);
        err!(i8, UInt8, 255u8);
        ok!(i8, UInt16, 127u16, 127); err!(i8, UInt16, 128u16);
    }
    #[test]
    fn i16_bounds() {
        ok!(i16, Int8, -5i8, -5); ok!(i16, UInt16, 32767u16, 32767); err!(i16, UInt16, 32768u16); ok!(i16, UInt8, 255u8, 255);
        ok!(i16, Int32, -32768i32, -32768); err!(i16, Int32, 32768i32);
        ok!(i16, Int64, 1i64, 1); err!(i16, Int64, i64::MAX);
        ok!(i16, UInt32, 1u32, 1); err!(i16, UInt32, u32::MAX);
    }
    #[test]
    fn i32_bounds() {
        ok!(i32, Int16, -1i16, -1); ok!(i32, Int8, -1i8, -1);
        ok!(i32, UInt32, i32::MAX as u32, i32::MAX); err!(i32, UInt32, i32::MAX as u32 + 1);
        ok!(i32, UInt16, 65535u16, 65535); ok!(i32, UInt8, 255u8, 255);
        ok!(i32, Int64, -1i64, -1); err!(i32, Int64, i64::MAX);
    }
    #[test]
    fn i64_bounds() {
        ok!(i64, Int32, -1i32, -1); ok!(i64, Int16, -1i16, -1); ok!(i64, Int8, -1i8, -1);
        ok!(i64, UInt64, i64::MAX as u64, i64::MAX); err!(i64, UInt64, i64::MAX as u64 + 1);
        ok!(i64, UInt32, 1u32, 1); ok!(i64, UInt16, 1u16, 1); ok!(i64, UInt8, 1u8, 1);
        err!(i64, UInt128, Box::new(u128::MAX)); err!(i64, Int128, Box::new(i128::MAX));
    }
    #[test]
    fn i128_bounds() {
        ok!(i128, Int64, i64::MIN, i64::MIN as i128); ok!(i128, Int32, -1i32, -1); ok!(i128, Int16, -1i16, -1); ok!(i128, Int8, -1i8, -1);
        ok!(i128, UInt128, Box::new(i128::MAX as u128), i128::MAX); err!(i128, UInt128, Box::new(u128::MAX));
        ok!(i128, UInt64, u64::MAX, u64::MAX as i128); ok!(i128, UInt32, 1u32, 1); ok!(i128, UInt16, 1u16, 1); ok!(i128, UInt8, 1u8, 1);
    }
    #[test]
    fn isize_bounds() {
        ok!(isize, Int64, -1i64, -1); ok!(isize, Int32, -1i32, -1); ok!(isize, Int16, -1i16, -1); ok!(isize, Int8, -1i8, -1);
        ok!(isize, UInt64, 1u64, 1); err!(isize, UInt64, u64::MAX);
        ok!(isize, Int128, Box::new(1i128), 1); err!(isize, Int128, Box::new(i128::MAX));
    }
    #[test]
    fn u8_bounds() {
        ok!(u8, Int16, 255i16, 255); err!(u8, Int16, 256i16); err!(u8, Int16, -1i16);
        ok!(u8, UInt16, 255u16, 255); err!(u8, UInt16, 256u16);
        ok!(u8, UInt8, 1u8, 1);
    }
    #[test]
    fn u16_bounds() {
        ok!(u16, UInt8, 255u8, 255);
        ok!(u16, Int32, 65535i32, 65535); err!(u16, Int32, 65536i32); err!(u16, Int32, -1i32);
        ok!(u16, UInt32, 1u32, 1); err!(u16, UInt32, u32::MAX);
    }
    #[test]
    fn u32_bounds() {
        ok!(u32, UInt16, 1u16, 1); ok!(u32, UInt8, 1u8, 1);
        ok!(u32, UInt64, 1u64, 1); err!(u32, UInt64, u64::MAX);
        ok!(u32, Int64, 1i64, 1); err!(u32, Int64, -1i64);
    }
    #[test]
    fn u64_bounds() {
        ok!(u64, UInt32, 1u32, 1); ok!(u64, UInt16, 1u16, 1); ok!(u64, UInt8, 1u8, 1);
        ok!(u64, Int64, 1i64, 1); err!(u64, Int64, -1i64);
    }
    #[test]
    fn u128_bounds() {
        ok!(u128, UInt64, u64::MAX, u64::MAX as u128); ok!(u128, UInt32, 1u32, 1); ok!(u128, UInt16, 1u16, 1); ok!(u128, UInt8, 1u8, 1);
        ok!(u128, Int128, Box::new(1i128), 1); err!(u128, Int128, Box::new(-1i128));
    }
    #[test]
    fn usize_bounds() {
        ok!(usize, UInt32, 1u32, 1); ok!(usize, UInt16, 1u16, 1); ok!(usize, UInt8, 1u8, 1);
        ok!(usize, UInt64, 1u64, 1); ok!(usize, UInt64, u64::MAX, u64::MAX as usize);
    }
}
