use std::borrow::Cow;
use tank::{AsValue, Value};

#[cfg(test)]
mod tests {
    use rust_decimal::{
        Decimal,
        prelude::{FromPrimitive, Zero},
    };
    use serde_json::Number;
    use std::{
        borrow::Cow,
        collections::{LinkedList, VecDeque},
        str::FromStr,
    };
    use tank::TableRef;
    use tank_core::{AsValue, Interval, Value};
    use time::Month;
    use uuid::Uuid;

    #[test]
    fn value_none() {
        assert_ne!(Value::Float32(Some(1.0)), Value::Null);
    }

    #[test]
    fn value_bool() {
        let var = true;
        let val: Value = var.as_value();
        assert_eq!(val, Value::Boolean(Some(true)));
        assert_ne!(val, Value::Boolean(Some(false)));
        assert_ne!(val, Value::Boolean(None));
        assert_ne!(val, Value::Varchar(Some("true".into())));
        let var: bool = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: bool = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, true);
        assert_eq!(bool::try_from_value(1_i8.as_value()).unwrap(), true);
        assert_eq!(bool::try_from_value(8_i16.as_value()).unwrap(), true);
        assert_eq!(bool::try_from_value(0_i32.as_value()).unwrap(), false);
        assert_eq!(bool::try_from_value(0_i64.as_value()).unwrap(), false);
        assert_eq!(bool::try_from_value(9_i128.as_value()).unwrap(), true);
        assert_eq!(bool::try_from_value(0_u8.as_value()).unwrap(), false);
        assert_eq!(bool::try_from_value(1_u16.as_value()).unwrap(), true);
        assert_eq!(bool::try_from_value(1_u32.as_value()).unwrap(), true);
        assert_eq!(bool::try_from_value(0_u64.as_value()).unwrap(), false);
        assert_eq!(bool::try_from_value(2_u128.as_value()).unwrap(), true);
        assert!(bool::try_from_value(0.5_f32.as_value()).is_err());
        assert_eq!(bool::parse("true").unwrap(), true);
        assert_eq!(bool::parse("false").unwrap(), false);
        assert!(bool::parse("false more").is_err());
        assert!(bool::parse("hello").is_err());
        assert_eq!(bool::parse("1").expect("Could not parse 1"), true);
        assert_eq!(bool::parse("0").expect("Could not parse 0"), false);
        assert!(bool::parse("").is_err());
    }

    #[test]
    fn value_i8() {
        let var = 127_i8;
        let val: Value = var.as_value();
        assert_eq!(val, Value::Int8(Some(127)));
        assert_ne!(val, Value::Int8(Some(126)));
        let var: i8 = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: i8 = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, 127);
        assert_eq!(i8::try_from_value(99_u8.as_value()).unwrap(), 99);
        assert_eq!(i8::try_from_value((-128_i64).as_value()).unwrap(), -128);
        assert_eq!(i8::try_from_value(12_i64.as_value()).unwrap(), 12);
        assert_eq!(i8::try_from_value(127_i32.as_value()).unwrap(), 127);
        assert_eq!(i8::try_from_value(127_i64.as_value()).unwrap(), 127);
        assert_eq!(i8::try_from_value(127_i128.as_value()).unwrap(), 127);
        assert_eq!(i8::try_from_value(127.0.as_value()).unwrap(), 127);
        assert_eq!(i8::try_from_value("127".as_value()).unwrap(), 127);
        assert_eq!(
            i8::try_from_value(Value::Unknown(Some("127".into()))).unwrap(),
            127
        );
        assert_eq!(
            i8::try_from_value(serde_json::Value::Number(127_i32.into()).as_value()).unwrap(),
            127
        );
        assert!(i8::try_from_value(128_i32.as_value()).is_err());
        assert!(i8::try_from_value(128_i64.as_value()).is_err());
        assert!(i8::try_from_value(128_i128.as_value()).is_err());
        assert!(i8::try_from_value(127.1.as_value()).is_err());
        assert!(i8::try_from_value(127.1.as_value()).is_err());
        assert!(i8::try_from_value("128".as_value()).is_err());
        assert!(
            i8::try_from_value(
                serde_json::Value::Number(Number::from_f64(127.1).unwrap()).as_value()
            )
            .is_err()
        );
        assert!(
            i8::try_from_value(
                serde_json::Value::Number(Number::from_f64(128.0).unwrap()).as_value()
            )
            .is_err()
        );
        assert!(i8::try_from_value(256_i64.as_value()).is_err());
        assert_eq!(i8::try_from_value((-128_i32).as_value()).unwrap(), -128);
        assert_eq!(i8::try_from_value((-128_i64).as_value()).unwrap(), -128);
        assert_eq!(i8::try_from_value((-128_i128).as_value()).unwrap(), -128);
        assert_eq!(i8::try_from_value((-128.0).as_value()).unwrap(), -128);
        assert_eq!(i8::try_from_value("-128".as_value()).unwrap(), -128);
        assert_eq!(i8::parse("127").expect("Could not parse i8"), 127);
        assert_eq!(i8::parse("-128").expect("Could not parse i8"), -128);
        assert!(i8::parse("128").is_err());
        assert!(i8::parse("-129").is_err());
        i8::parse("54, next").expect_err("Should not parse");
        assert!(i8::parse("").is_err());
    }

    #[test]
    fn value_i16() {
        let var = -32768_i16;
        let val: Value = var.as_value();
        assert_eq!(val, Value::Int16(Some(-32768)));
        assert_ne!(val, Value::Int32(Some(-32768)));
        let var: i16 = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: i16 = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, -32768_i16);
        assert_eq!(i16::try_from_value(29_i8.as_value()).unwrap(), 29);
        assert_eq!(i16::try_from_value(100_u8.as_value()).unwrap(), 100);
        assert_eq!(i16::try_from_value(5000_u16.as_value()).unwrap(), 5000);
        assert_eq!(i16::try_from_value(32767_i32.as_value()).unwrap(), 32767);
        assert_eq!(i16::try_from_value(32767_i64.as_value()).unwrap(), 32767);
        assert_eq!(i16::try_from_value(32767_i128.as_value()).unwrap(), 32767);
        assert_eq!(i16::try_from_value("32767".as_value()).unwrap(), 32767);
        assert!(i16::try_from_value(32768_i32.as_value()).is_err());
        assert!(i16::try_from_value(32768_i64.as_value()).is_err());
        assert!(i16::try_from_value(32768_i128.as_value()).is_err());
        assert!(i16::try_from_value("32768".as_value()).is_err());
        assert_eq!(
            i16::try_from_value((-32768_i32).as_value()).unwrap(),
            -32768
        );
        assert_eq!(
            i16::try_from_value((-32768_i64).as_value()).unwrap(),
            -32768
        );
        assert_eq!(
            i16::try_from_value((-32768_i128).as_value()).unwrap(),
            -32768
        );
        assert!(i16::try_from_value((-32769_i32).as_value()).is_err());
        assert!(i16::try_from_value((-32769_i64).as_value()).is_err());
        assert!(i16::try_from_value((-32769_i128).as_value()).is_err());
        assert!(i16::try_from_value("-32769".as_value()).is_err());
        assert_eq!(i16::try_from_value("-32768".as_value()).unwrap(), -32768);
        assert!(i16::try_from_value(u16::MAX.as_value()).is_err());
        assert!(i16::try_from_value(32768_u16.as_value()).is_err());
        assert!(i16::parse("hello").is_err());
        assert_eq!(i16::parse("32767").expect("Could not parse i16"), 32767);
        assert_eq!(i16::parse("-32768").expect("Could not parse i16"), -32768);
        assert!(i16::parse("32768").is_err());
        assert!(i16::parse("-32769").is_err());
        i16::parse("12345, next").expect_err("Not a valid number");
        assert!(i16::parse("").is_err());
    }

    #[test]
    fn value_i32() {
        let var = -2147483648_i32;
        let val: Value = var.as_value();
        assert_eq!(val, Value::Int32(Some(-2147483648)));
        assert_ne!(val, Value::Null);
        let var: i32 = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: i32 = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, -2147483648_i32);
        assert_eq!(i32::try_from_value((-31_i8).as_value()).unwrap(), -31);
        assert_eq!(i32::try_from_value((-1_i16).as_value()).unwrap(), -1);
        assert_eq!(i32::try_from_value(77_u8.as_value()).unwrap(), 77);
        assert_eq!(i32::try_from_value(15_u16.as_value()).unwrap(), 15);
        assert_eq!(i32::try_from_value(1001_u32.as_value()).unwrap(), 1001);
        assert_eq!(
            i32::try_from_value(2147483647_i64.as_value()).unwrap(),
            i32::MAX,
        );
        assert_eq!(
            i32::try_from_value((-2147483648_i64).as_value()).unwrap(),
            i32::MIN,
        );
        assert_eq!(
            i32::parse("2147483647").expect("Could not parse i32"),
            i32::MAX,
        );
        assert_eq!(
            i32::parse("-2147483648").expect("Could not parse i32"),
            i32::MIN,
        );
        assert!(i32::parse("2147483648").is_err());
        assert!(i32::parse("-2147483649").is_err());
        assert!(i32::parse("2147483647, next").is_err());
        assert!(i32::try_from_value(u32::MAX.as_value()).is_err());
        assert!(i32::try_from_value(2147483648_u32.as_value()).is_err());
        assert!(i64::parse("").is_err());
    }

    #[test]
    fn value_i64() {
        let var = 9223372036854775807_i64;
        let val: Value = var.as_value();
        let var: i64 = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: i64 = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, 9223372036854775807_i64);
        assert_eq!(i64::try_from_value((-31_i8).as_value()).unwrap(), -31);
        assert_eq!(i64::try_from_value((-1234_i16).as_value()).unwrap(), -1234);
        assert_eq!(i64::try_from_value((-1_i32).as_value()).unwrap(), -1);
        assert_eq!(i64::try_from_value((77_u8).as_value()).unwrap(), 77);
        assert_eq!(i64::try_from_value((5555_u16).as_value()).unwrap(), 5555);
        assert_eq!(
            i64::try_from_value((123456_u32).as_value()).unwrap(),
            123456
        );
        assert_eq!(
            i64::try_from_value((12345678901234_u64).as_value()).unwrap(),
            12345678901234
        );
        assert_eq!(
            i64::parse("9223372036854775807").expect("Could not parse i64"),
            i64::MAX,
        );
        assert_eq!(
            i64::parse("-9223372036854775808").expect("Could not parse i64"),
            i64::MIN,
        );
        assert!(i64::parse("9223372036854775808").is_err());
        assert!(i64::parse("-9223372036854775809").is_err());
        assert!(i64::parse("").is_err());
        assert!(i64::parse("9223372036854775807, next").is_err());
        assert!(i64::parse("").is_err());
        assert!(i64::try_from_value(u64::MAX.as_value()).is_err());
        assert!(i64::try_from_value(9223372036854775808_u64.as_value()).is_err());
    }

    #[test]
    fn value_i128() {
        let var = -123456789101112131415_i128;
        let val: Value = var.as_value();
        let var: i128 = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: i128 = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, -123456789101112131415_i128);
        assert_eq!(i128::try_from_value((-31_i8).as_value()).unwrap(), -31);
        assert_eq!(i128::try_from_value((-1234_i16).as_value()).unwrap(), -1234);
        assert_eq!(i128::try_from_value((-1_i32).as_value()).unwrap(), -1);
        assert_eq!(
            i128::try_from_value((-12345678901234_i64).as_value()).unwrap(),
            -12345678901234
        );
        assert_eq!(i128::try_from_value((77_u8).as_value()).unwrap(), 77);
        assert_eq!(i128::try_from_value((5555_u16).as_value()).unwrap(), 5555);
        assert_eq!(
            i128::try_from_value((123456_u32).as_value()).unwrap(),
            123456
        );
        assert_eq!(
            i128::try_from_value((12345678901234_u64).as_value()).unwrap(),
            12345678901234
        );
        let i128_max = "170141183460469231731687303715884105727";
        let i128_over = "170141183460469231731687303715884105728";
        let i128_min = "-170141183460469231731687303715884105728";
        let i128_under = "170141183460469231731687303715884105729";
        assert_eq!(
            i128::parse(i128_max).expect("Could not parse i128 max"),
            i128::MAX
        );
        assert_eq!(
            i128::parse(i128_min).expect("Could not parse i128 min"),
            i128::MIN
        );
        assert!(i128::parse(i128_over).is_err());
        assert!(i128::parse(i128_under).is_err());
        assert!(i128::parse("").is_err());
        assert!(i128::try_from_value(u128::MAX.as_value()).is_err());
        assert!(i128::try_from_value((i128::MAX as u128 + 1).as_value()).is_err());
    }

    #[test]
    fn value_u8() {
        let var = 255_u8;
        let val: Value = var.as_value();
        let var: u8 = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: u8 = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, 255);
        assert_eq!(u8::parse("255").expect("Could not parse u8"), 255);
        assert!(u8::parse("256").is_err());
        assert!(u8::parse("-1").is_err());
        assert!(u8::parse("").is_err());
        let mut input = "255, next";
        assert!(u8::parse(&mut input).is_err());
        assert!(u8::try_from_value(0.1_f64.as_value()).is_err());
        assert!(u8::parse("").is_err());
    }

    #[test]
    fn value_u16() {
        let var = 65535_u16;
        let val: Value = var.as_value();
        let var: u16 = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: u16 = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, 65535);
        assert_eq!(u16::try_from_value((123_u8).as_value()).unwrap(), 123);
        assert_eq!(u16::parse("65535").expect("Could not parse u16"), 65535);
        assert!(u16::parse("65536").is_err());
        assert!(u16::parse("-1").is_err());
        assert!(u16::parse("884 trailing").is_err());
        assert!(u16::parse("").is_err());
    }

    #[test]
    fn value_u32() {
        let var = 4_000_000_000_u32;
        let val: Value = var.as_value();
        let var: u32 = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: u32 = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, 4_000_000_000);
        assert_eq!(u32::try_from_value((12_u8).as_value()).unwrap(), 12);
        assert_eq!(u32::try_from_value((65535_u16).as_value()).unwrap(), 65535);
        assert!(u32::parse("34a").is_err(),);
        assert_eq!(
            u32::parse("4294967295").expect("Could not parse u32"),
            u32::MAX,
        );
        assert!(u32::parse("4294967296").is_err());
        assert!(u32::parse("-1").is_err());
        assert!(u32::parse("").is_err());
    }

    #[test]
    fn value_u64() {
        let var = 18_000_000_000_000_000_000_u64;
        let val: Value = var.as_value();
        let var: u64 = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: u64 = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, 18_000_000_000_000_000_000);
        assert_eq!(u64::try_from_value((77_u8).as_value()).unwrap(), 77);
        assert_eq!(u64::try_from_value((1234_u16).as_value()).unwrap(), 1234);
        assert_eq!(
            u64::try_from_value((123456_u32).as_value()).unwrap(),
            123456
        );
        assert_eq!(
            u64::parse("18446744073709551615").expect("Could not parse u64"),
            u64::MAX,
        );
        assert!(u64::parse("76+").is_err());
        assert!(u64::parse("18446744073709551616").is_err());
        assert!(u64::parse("-1").is_err());
        assert!(u64::try_from_value(0.1_f64.as_value()).is_err());
        assert!(u64::parse("").is_err());
    }

    #[test]
    fn value_u128() {
        let var = 340_282_366_920_938_463_463_374_607_431_768_211_455_u128;
        let val: Value = var.as_value();
        let var: u128 = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: u128 = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, 340_282_366_920_938_463_463_374_607_431_768_211_455);
        assert_eq!(u128::try_from_value((11_u8).as_value()).unwrap(), 11);
        assert_eq!(u128::try_from_value((222_u16).as_value()).unwrap(), 222);
        assert_eq!(
            u128::try_from_value(333_333_u32.as_value()).unwrap(),
            333_333
        );
        assert_eq!(
            u128::try_from_value(444_444_444_444_u64.as_value()).unwrap(),
            444_444_444_444
        );
        assert_eq!(
            u128::try_from_value(1771684556600_i64.as_value()).unwrap(),
            1771684556600,
        );
        let u128_max = "340282366920938463463374607431768211455";
        assert_eq!(
            u128::parse(u128_max).expect("Could not parse u128"),
            u128::MAX
        );
        assert!(u128::parse("-905-").is_err());
        assert!(u128::parse("340282366920938463463374607431768211456").is_err());
        assert!(u128::parse("-1").is_err());
        assert!(u128::try_from_value(0.1_f64.as_value()).is_err());
        assert!(u128::parse("").is_err());
    }

    #[test]
    fn value_f32() {
        let var = 3.14f32;
        let val: Value = var.as_value();
        let var: f32 = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: f32 = AsValue::try_from_value(val).unwrap();
        assert!((var - 3.14).abs() < f32::EPSILON);
        assert_eq!(
            f32::try_from_value(Decimal::from_f64(2.125).as_value()).unwrap(),
            2.125
        );
        let v_pos_inf: Value = f32::INFINITY.as_value();
        let v_neg_inf: Value = f32::NEG_INFINITY.as_value();
        assert_ne!(v_pos_inf, v_neg_inf);
        assert_eq!(f32::try_from_value(v_pos_inf).unwrap(), f32::INFINITY);
        assert_eq!(f32::try_from_value(v_neg_inf).unwrap(), f32::NEG_INFINITY);
        assert_eq!(
            f32::try_from_value((12.5_f64).as_value()).unwrap(),
            12.5_f32
        );
        let d = Decimal::from_f64(99.125).unwrap();
        assert_eq!(f32::try_from_value(d.as_value()).unwrap(), 99.125_f32);
        assert_eq!(f32::parse("3.14").unwrap(), 3.14_f32);
        assert_eq!(f32::parse("3.14e2").unwrap(), 314.0_f32);
        let huge_pos = f32::parse("1e100").unwrap();
        assert!(huge_pos.is_infinite() && huge_pos.is_sign_positive());
        let huge_neg = f32::parse("-1e100").unwrap();
        assert!(huge_neg.is_infinite() && huge_neg.is_sign_negative());
        assert!(f32::parse("abc").is_err());
        assert!(f32::parse("1.0 trailing").is_err());
    }

    #[test]
    fn value_f64() {
        let var = 2.7182818284f64;
        let val: Value = var.as_value();
        let var: f64 = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: f64 = AsValue::try_from_value(val).unwrap();
        assert!((var - 2.7182818284).abs() < f64::EPSILON);
        assert_eq!(f64::try_from_value((3.5_f32).as_value()).unwrap(), 3.5);
        assert_eq!(
            f64::try_from_value(Decimal::from_f64(2.25).as_value()).unwrap(),
            2.25
        );
        let pos_inf = f64::INFINITY;
        let neg_inf = f64::NEG_INFINITY;
        assert_eq!(
            f64::try_from_value(pos_inf.as_value()).unwrap(),
            f64::INFINITY
        );
        assert_eq!(
            f64::try_from_value(neg_inf.as_value()).unwrap(),
            f64::NEG_INFINITY
        );
        assert_ne!(pos_inf.as_value(), neg_inf.as_value());
        let d = Decimal::from_f32(7.0625).unwrap();
        assert_eq!(f64::try_from_value(d.as_value()).unwrap(), 7.0625_f64);
        assert_eq!(f64::parse("6.022e23").unwrap(), 6.022e23_f64);
        let huge_pos = f64::parse("1e1000").unwrap();
        assert!(huge_pos.is_infinite() && huge_pos.is_sign_positive());
        let huge_neg = f64::parse("-1e1000").unwrap();
        assert!(huge_neg.is_infinite() && huge_neg.is_sign_negative());
        assert!(f64::parse("not_a_number").is_err());
        f64::parse("1.2345xyz").expect_err("Should not parse correctly");
    }

    #[test]
    fn value_char() {
        let var = 'a';
        let val: Value = var.as_value();
        assert_eq!(val, Value::Char(Some('a')));
        assert_ne!(val, Value::Char(Some('b')));
        let var: char = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, 'a');
        assert!(matches!(
            char::try_from_value(Value::Varchar(Some("t".into()))),
            Ok('t'),
        ));
        assert!(char::try_from_value(Value::Varchar(Some("long".into()))).is_err());
        assert!(char::try_from_value(Value::Varchar(Some("".into()))).is_err());
        assert_eq!(char::parse("v").expect("Could not parse char"), 'v');
        assert!(char::parse("").is_err());
    }

    #[test]
    fn value_string() {
        let var = "Hello World!";
        let val: Value = var.into();
        assert_eq!(val, Value::Varchar(Some("Hello World!".into())));
        assert_ne!(val, Value::Varchar(Some("Hello World.".into())));
        let var: String = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: String = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, "Hello World!");
        assert_eq!(String::try_from_value('x'.as_value()).unwrap(), "x");
        assert_eq!(String::try_from_value("hello".into()).unwrap(), "hello");
        assert_eq!(String::parse("").expect("Could not parse string"), "");
        assert_eq!(
            String::parse("\"\"").expect("Could not parse string"),
            "\"\""
        );
        assert_eq!(
            Value::Varchar(Some(Cow::Borrowed("hello"))),
            Value::Varchar(Some(Cow::Owned("hello".into()))),
        );
        assert_eq!(
            Value::Varchar(Some(Cow::Owned("world".into()))),
            Value::Varchar(Some(Cow::Borrowed("world"))),
        );
    }

    #[test]
    fn value_cow_str() {
        let var = Cow::Borrowed("Hello World!");
        let val: Value = var.as_value();
        assert_eq!(val, Value::Varchar(Some("Hello World!".into())));
        let var: Cow<'_, str> = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: Cow<'_, str> = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, "Hello World!");
        assert!(matches!(
            <Cow<'static, str> as AsValue>::as_empty_value(),
            Value::Varchar(..),
        ));
        assert!(matches!(
            <Cow<'static, str> as AsValue>::try_from_value(Value::Boolean(Some(false))),
            Err(..),
        ));
    }

    #[test]
    fn value_date() {
        let var = time::Date::from_calendar_date(2025, Month::July, 21).unwrap();
        let val: Value = var.as_value();
        assert_eq!(val, Value::Date(Some(var)));
        assert_ne!(val, Value::Null);
        let var: time::Date = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: time::Date = AsValue::try_from_value(val).unwrap();
        assert_eq!(
            var,
            time::Date::from_calendar_date(2025, Month::July, 21).unwrap()
        );
        let val: time::Date =
            AsValue::try_from_value(Value::Varchar(Some("2025-01-22".into()))).unwrap();
        assert_eq!(
            val,
            time::Date::from_calendar_date(2025, Month::January, 22).unwrap()
        );
        time::Date::try_from_value("1999-12-12error".into())
            .expect_err("Should not be able to convert wrong string");
    }

    #[test]
    fn value_time() {
        let var = time::Time::from_hms(0, 57, 21).unwrap();
        let val: Value = var.as_value();
        assert_eq!(val, Value::Time(Some(var)));
        assert_ne!(val, Value::Null);
        let var: time::Time = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: time::Time = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, time::Time::from_hms(0, 57, 21).unwrap());
        assert_eq!(
            time::Time::try_from_value(Value::Varchar(Some("13:22".into()))).unwrap(),
            time::Time::from_hms(13, 22, 0).unwrap()
        );
    }

    #[test]
    fn value_datetime() {
        let var = time::PrimitiveDateTime::new(
            time::Date::from_calendar_date(2025, Month::July, 29).unwrap(),
            time::Time::from_hms(13, 52, 13).unwrap(),
        );
        let val: Value = var.as_value();
        assert_eq!(val, Value::Timestamp(Some(var)));
        assert_ne!(val, Value::Varchar(None));
        let var: time::PrimitiveDateTime = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: time::PrimitiveDateTime = AsValue::try_from_value(val).unwrap();
        assert_eq!(
            var,
            time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2025, Month::July, 29).unwrap(),
                time::Time::from_hms(13, 52, 13).unwrap(),
            )
        );
        let val: time::PrimitiveDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-07-29T14:52:36.500".into())))
                .unwrap();
        assert_eq!(
            val,
            time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2025, Month::July, 29).unwrap(),
                time::Time::from_hms_milli(14, 52, 36, 500).unwrap()
            )
        );
        assert_ne!(
            val,
            time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2025, Month::July, 29).unwrap(),
                time::Time::from_hms(14, 52, 36).unwrap()
            )
        );
        let val: time::PrimitiveDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-07-29T14:52:36".into()))).unwrap();
        assert_eq!(
            val,
            time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2025, Month::July, 29).unwrap(),
                time::Time::from_hms(14, 52, 36).unwrap()
            )
        );
        let val: time::PrimitiveDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-07-29 14:52:36.500".into())))
                .unwrap();
        assert_eq!(
            val,
            time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2025, Month::July, 29).unwrap(),
                time::Time::from_hms_milli(14, 52, 36, 500).unwrap()
            )
        );
        let val: time::PrimitiveDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-07-29 14:52:36".into()))).unwrap();
        assert_eq!(
            val,
            time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2025, Month::July, 29).unwrap(),
                time::Time::from_hms(14, 52, 36).unwrap()
            )
        );
        let val: time::PrimitiveDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-07-29 14:52".into()))).unwrap();
        assert_eq!(
            val,
            time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2025, Month::July, 29).unwrap(),
                time::Time::from_hms(14, 52, 00).unwrap()
            )
        );
    }

    #[test]
    fn value_datetime_timezone() {
        let var = time::OffsetDateTime::new_in_offset(
            time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
            time::Time::from_hms(00, 35, 12).unwrap(),
            time::UtcOffset::from_hms(2, 0, 0).unwrap(),
        );
        let val: Value = var.as_value();
        assert_eq!(val, Value::TimestampWithTimezone(Some(var)));
        assert_ne!(val, Value::Date(Some(var.date())));

        assert_ne!(
            val,
            Value::TimestampWithTimezone(
                time::OffsetDateTime::new_in_offset(
                    time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
                    time::Time::from_hms(00, 35, 12).unwrap(),
                    time::UtcOffset::from_hms(1, 0, 0).unwrap(),
                )
                .into()
            )
        );
        let var: time::OffsetDateTime = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: time::OffsetDateTime = AsValue::try_from_value(val).unwrap();
        assert_eq!(
            var,
            time::OffsetDateTime::new_in_offset(
                time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
                time::Time::from_hms(00, 35, 12).unwrap(),
                time::UtcOffset::from_hms(2, 0, 0).unwrap(),
            )
        );
        let val: time::OffsetDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-08-16T00:35:12.123+01:00".into())))
                .unwrap();
        assert_eq!(
            val,
            time::OffsetDateTime::new_in_offset(
                time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
                time::Time::from_hms_milli(0, 35, 12, 123).unwrap(),
                time::UtcOffset::from_hms(1, 0, 0).unwrap(),
            )
        );
        let val: time::OffsetDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-08-16T00:35:12.123+01".into())))
                .unwrap();
        assert_eq!(
            val,
            time::OffsetDateTime::new_in_offset(
                time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
                time::Time::from_hms_milli(0, 35, 12, 123).unwrap(),
                time::UtcOffset::from_hms(1, 0, 0).unwrap(),
            )
        );
        let val: time::OffsetDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-08-16T00:35:12+01:00".into())))
                .unwrap();
        assert_eq!(
            val,
            time::OffsetDateTime::new_in_offset(
                time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
                time::Time::from_hms(0, 35, 12).unwrap(),
                time::UtcOffset::from_hms(1, 0, 0).unwrap(),
            )
        );
        let val: time::OffsetDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-08-16T00:35:12+01".into()))).unwrap();
        assert_eq!(
            val,
            time::OffsetDateTime::new_in_offset(
                time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
                time::Time::from_hms(0, 35, 12).unwrap(),
                time::UtcOffset::from_hms(1, 0, 0).unwrap(),
            )
        );
        let val: time::OffsetDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-08-16T00:35+01:00".into()))).unwrap();
        assert_eq!(
            val,
            time::OffsetDateTime::new_in_offset(
                time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
                time::Time::from_hms(0, 35, 0).unwrap(),
                time::UtcOffset::from_hms(1, 0, 0).unwrap(),
            )
        );
        let val: time::OffsetDateTime =
            AsValue::try_from_value(Value::Varchar(Some("2025-08-16T00:35+01".into()))).unwrap();
        assert_eq!(
            val,
            time::OffsetDateTime::new_in_offset(
                time::Date::from_calendar_date(2025, Month::August, 16).unwrap(),
                time::Time::from_hms(0, 35, 0).unwrap(),
                time::UtcOffset::from_hms(1, 0, 0).unwrap(),
            )
        );
    }

    #[test]
    fn value_interval() {
        let var = Interval::from_months(4);
        let val: Value = var.as_value();
        assert_eq!(val, Interval::from_months(4).as_value());
        assert_ne!(val, Interval::from_months(3).as_value());
        assert_ne!(val, Interval::from_days(28).as_value());
        let var: Interval = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: Interval = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, Interval::from_months(4));
        assert_eq!(
            Interval::parse("1 year 2 mons").expect("Could not parse the interval"),
            Interval::from_years(1) + Interval::from_months(2),
        );
        assert_eq!(
            Interval::parse("-100 year -12 mons +3 days -04:05:06")
                .expect("Could not parse the interval"),
            Interval::from_years(-101)
                + Interval::from_days(3)
                + Interval::from_hours(-4)
                + Interval::from_mins(-5)
                + Interval::from_secs(-6),
        );
        assert_eq!(
            Interval::parse("2years 60days").expect("Could not parse the interval"),
            Interval::from_years(2) + Interval::from_days(60),
        );
        assert_eq!(
            Interval::parse("'1 year 2 mons 3 days 04:05:06.789'").unwrap(),
            Interval::from_years(1)
                + Interval::from_months(2)
                + Interval::from_days(3)
                + Interval::from_hours(4)
                + Interval::from_mins(5)
                + Interval::from_secs(6)
                + Interval::from_micros(789_000)
        );
        assert_eq!(
            Interval::parse("'2 years 1 mon 5 days 12:00:00.000000123'").unwrap(),
            Interval::from_years(2)
                + Interval::from_months(1)
                + Interval::from_days(5)
                + Interval::from_hours(12)
                + Interval::from_nanos(123)
        );
        assert_eq!(
            Interval::parse("'-1 year 2 mons -3 days 04:05:06.001002003'").unwrap(),
            Interval::from_years(-1)
                + Interval::from_months(2)
                + Interval::from_days(-3)
                + Interval::from_hours(4)
                + Interval::from_mins(5)
                + Interval::from_secs(6)
                + Interval::from_micros(1_002)
                + Interval::from_nanos(3)
        );
        assert_eq!(
            Interval::parse("-04:05:06.000123").unwrap(),
            Interval::from_hours(-4)
                + Interval::from_mins(-5)
                + Interval::from_secs(-6)
                + Interval::from_micros(-123)
        );
        assert_eq!(
            Interval::parse(
                "3 years 4 months 5 days 6 hours 7 minutes 8 seconds 9 microseconds 10 nanoseconds"
            )
            .unwrap(),
            Interval::from_years(3)
                + Interval::from_months(4)
                + Interval::from_days(5)
                + Interval::from_hours(6)
                + Interval::from_mins(7)
                + Interval::from_secs(8)
                + Interval::from_micros(9)
                + Interval::from_nanos(10)
        );
        assert_eq!(
            Interval::parse("2 Y 3 MONS 4 d 5 H 6 MIN 7 S 8 MICRO 9 NS").unwrap(),
            Interval::from_years(2)
                + Interval::from_months(3)
                + Interval::from_days(4)
                + Interval::from_hours(5)
                + Interval::from_mins(6)
                + Interval::from_secs(7)
                + Interval::from_micros(8)
                + Interval::from_nanos(9)
        );
        assert_eq!(
            Interval::parse("10:11:12.123456789").unwrap(),
            Interval::from_hours(10)
                + Interval::from_mins(11)
                + Interval::from_secs(12)
                + Interval::from_micros(123_456)
                + Interval::from_nanos(789)
        );
        assert_eq!(
            Interval::parse("1 year 12 months").unwrap(),
            Interval::from_years(2)
        );
        assert!(Interval::parse("5 HORS").is_err());
        assert!(Interval::parse("04:").is_err());
        assert!(Interval::parse("04:05:").is_err());
        assert!(Interval::parse("04:05:06.").is_err());
        assert!(Interval::parse("'2 days 01:02:03.0040050068473'   more").is_err());
        assert!(Interval::parse("'2 days\"").is_err());
        assert!(Interval::parse("'2 days 01:02:03.0\"").is_err());
        assert_eq!(
            Interval::parse("'2 days 01:02:03.004005006'").unwrap(),
            Interval::from_days(2)
                + Interval::from_hours(1)
                + Interval::from_mins(2)
                + Interval::from_secs(3)
                + Interval::from_micros(4_005)
                + Interval::from_nanos(6)
        );
    }

    #[test]
    fn value_time_duration() {
        let var = time::Duration::days(14);
        let val: Value = var.as_value();
        assert_eq!(val, Interval::from_days(14).as_value());
        assert_ne!(val, Interval::from_days(15).as_value());
        assert_ne!(val, Interval::from_secs(1).as_value());
        let var: time::Duration = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: time::Duration = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, time::Duration::days(14));
    }

    #[test]
    fn value_std_duration() {
        let days_5 = std::time::Duration::new((5 * Interval::SECS_IN_DAY) as u64, 0);
        let days_1 = std::time::Duration::new((1 * Interval::SECS_IN_DAY) as u64, 0);
        let var = days_5.clone();
        let val: Value = var.as_value();
        assert_eq!(val, days_5.clone().as_value());
        assert_ne!(val, days_1.as_value());

        let var: std::time::Duration = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: std::time::Duration = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, days_5.clone());
    }

    #[test]
    fn value_uuid() {
        let var = Uuid::nil();
        let val: Value = var.as_value();
        assert_eq!(
            val,
            Uuid::parse_str("00000000-0000-0000-0000-000000000000")
                .unwrap()
                .as_value()
        );
        assert_ne!(
            val,
            Uuid::parse_str("10000000-0000-0000-0000-000000000000")
                .unwrap()
                .as_value()
        );
        assert_ne!(val, 5.as_value());

        let var: Uuid = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: Uuid = AsValue::try_from_value(val).unwrap();
        assert_eq!(
            var,
            Uuid::parse_str("00000000-0000-0000-0000-000000000000").unwrap()
        );

        let var = Uuid::parse_str("c959fd7d-d3a6-4453-a2ed-83116f2b1b84")
            .unwrap()
            .as_value();
        let val: Value = var.as_value();
        assert_eq!(
            val,
            Uuid::parse_str("c959fd7d-d3a6-4453-a2ed-83116f2b1b84")
                .unwrap()
                .as_value()
        );
        assert_ne!(
            val,
            Uuid::parse_str("80ae6ccb-2504-4d2e-b496-5d9759199625")
                .unwrap()
                .as_value()
        );
        assert_ne!(val, 5.as_value());

        let var: Uuid = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: Uuid = AsValue::try_from_value(val).unwrap();
        assert_eq!(
            var,
            Uuid::parse_str("c959fd7d-d3a6-4453-a2ed-83116f2b1b84").unwrap()
        );
        assert_eq!(
            Uuid::parse_str("6ed80631-c3ec-41a5-9f66-d9c1e5532798").unwrap(),
            Uuid::try_from_value("6ed80631-c3ec-41a5-9f66-d9c1e5532798".into()).unwrap()
        );
    }

    #[test]
    fn value_decimal() {
        let var = Decimal::from_i128_with_scale(12345, 2);
        let val: Value = var.as_value();
        assert_eq!(val, Decimal::from_f64(123.45).unwrap().as_value());
        assert_ne!(val, Decimal::from_f64(123.10).unwrap().as_value());
        let var: Decimal = AsValue::try_from_value(val).unwrap();
        let val = var.as_value();
        let var: Decimal = AsValue::try_from_value(val).unwrap();
        assert_eq!(var, Decimal::from_f64(123.45).unwrap());
        assert_eq!(
            Decimal::try_from_value(127_i8.as_value()).unwrap(),
            Decimal::from_f64(127.0).unwrap()
        );
        assert_ne!(
            Decimal::try_from_value(126_i8.as_value()).unwrap(),
            Decimal::from_f64(127.0).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value(0_i16.as_value()).unwrap(),
            Decimal::from_f64(0.0).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value((-2147483648_i32).as_value()).unwrap(),
            Decimal::from_f64(-2147483648.0).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value(82664_i64.as_value()).unwrap(),
            Decimal::from_f64(82664.0).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value((255_u8).as_value()).unwrap(),
            Decimal::from_f64(255.0).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value((10000_u16).as_value()).unwrap(),
            Decimal::from_f64(10000.0).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value((777_u32).as_value()).unwrap(),
            Decimal::from_f64(777.0).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value((2_u32).as_value()).unwrap(),
            Decimal::from_f64(2.0).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value((0_u64).as_value()).unwrap(),
            Decimal::ZERO
        );
        assert_eq!(
            Decimal::try_from_value((4.25_f32).as_value()).unwrap(),
            Decimal::from_f64(4.25).unwrap()
        );
        assert_eq!(
            Decimal::try_from_value((-11.29_f64).as_value()).unwrap(),
            Decimal::from_f64(-11.29).unwrap()
        );
        Decimal::try_from_value("hello".into()).expect_err("Cannot convert a string to decimal");
        assert_eq!(Decimal::as_empty_value(), Value::Decimal(None, 0, 0));
        assert_ne!(
            Decimal::as_empty_value(),
            Value::Decimal(Some(Decimal::zero()), 0, 0)
        );
        assert_ne!(Decimal::as_empty_value(), Value::Decimal(None, 1, 0));
        assert_ne!(Decimal::as_empty_value(), Value::Decimal(None, 0, 1));
    }

    #[test]
    fn value_array() {
        let var = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] as [i8; 10];
        let val: Value = var.as_value();
        let var = <[i8; 10]>::try_from_value(val).unwrap();
        assert_eq!(var, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_ne!(var, [0, 1, 2, 3, 4, 5, 6, 7, 7, 9]);
        assert_eq!(
            <[String; 2]>::try_from_value(["Hello".to_string(), "world".to_string()].as_value())
                .expect("Cannot convert the Value to array of 2 String"),
            ["Hello", "world"]
        );
        assert_eq!(
            <[Decimal; 5]>::try_from_value([12.50, 13.3, -6.1, 0.0, -3.34].as_value())
                .expect("Cannot convert the Value to array of 5 Decimal"),
            [
                Decimal::from_f32(12.50).unwrap(),
                Decimal::from_f32(13.3).unwrap(),
                Decimal::from_f32(-6.1).unwrap(),
                Decimal::from_f32(0.0).unwrap(),
                Decimal::from_f32(-3.34).unwrap(),
            ]
        );
        assert_ne!(
            <[Decimal; 5]>::try_from_value([12.50, 13.3, -6.1, 0.0, -3.34].as_value())
                .expect("Cannot convert the Value to array of 5 Decimal"),
            [
                Decimal::from_f32(12.50).unwrap(),
                Decimal::from_f32(13.3).unwrap(),
                Decimal::from_f32(-6.11).unwrap(),
                Decimal::from_f32(0.0).unwrap(),
                Decimal::from_f32(-3.34).unwrap(),
            ]
        );
        assert!(<[i32; 2]>::try_from_value(vec![1, 2, 3].as_value()).is_err()); // More elements than expected
        assert!(<[i32; 2]>::try_from_value(vec![1].as_value()).is_err()); // Less elements than expected
        assert!(<[char; 3]>::try_from_value(['x', 'y'].as_value()).is_err()); // Less elements than expected
        assert_ne!(
            <[char; 3]>::try_from_value(['x', 'y', 'z'].as_value())
                .expect("Cannot convert the Value to array of 3 chars"),
            ['x', 'y', 'a']
        );
    }

    #[test]
    fn value_list() {
        let var: VecDeque<_> = vec![
            Uuid::from_str("ae020ca8-c530-4f7c-8ce0-58d31914f2dc").unwrap(),
            Uuid::from_str("e3554ad6-e5c5-425b-9d0c-8c181d344932").unwrap(),
            Uuid::from_str("ebde0bdc-92c1-415d-b955-88e13bcd2726").unwrap(),
            Uuid::from_str("ed31d4ef-82ea-442e-b273-5f5006e55ab1").unwrap(),
        ]
        .into();
        let val: Value = var.as_value();
        let var = VecDeque::<Uuid>::try_from_value(val).unwrap();
        assert_eq!(
            var,
            vec![
                Uuid::from_str("ae020ca8-c530-4f7c-8ce0-58d31914f2dc").unwrap(),
                Uuid::from_str("e3554ad6-e5c5-425b-9d0c-8c181d344932").unwrap(),
                Uuid::from_str("ebde0bdc-92c1-415d-b955-88e13bcd2726").unwrap(),
                Uuid::from_str("ed31d4ef-82ea-442e-b273-5f5006e55ab1").unwrap(),
            ]
        );
        let val: Value = var.as_value();
        let var = LinkedList::<Uuid>::try_from_value(val).unwrap();
        assert_eq!(
            var,
            LinkedList::from_iter([
                Uuid::from_str("ae020ca8-c530-4f7c-8ce0-58d31914f2dc").unwrap(),
                Uuid::from_str("e3554ad6-e5c5-425b-9d0c-8c181d344932").unwrap(),
                Uuid::from_str("ebde0bdc-92c1-415d-b955-88e13bcd2726").unwrap(),
                Uuid::from_str("ed31d4ef-82ea-442e-b273-5f5006e55ab1").unwrap(),
            ])
        );
        assert!(Vec::<String>::try_from_value("hello".into()).is_err());
        assert_eq!(
            Vec::<char>::try_from_value(['a', 'b', 'c'].as_value())
                .expect("Cannot convert array to Vec"),
            vec!['a', 'b', 'c']
        );
        assert_eq!(
            LinkedList::try_from_value(Vec::<bool>::new().as_value())
                .expect("Cannot convert Value to LinkedList"),
            LinkedList::<bool>::new()
        );
        assert_eq!(
            VecDeque::<bool>::try_from_value(Value::List(None, Value::Boolean(None).into()))
                .expect("Cannot convert Value to LinkedList"),
            VecDeque::<bool>::new()
        );
        assert_eq!(
            Vec::<bool>::try_from_value(Value::List(None, Value::Boolean(None).into()))
                .expect("Cannot convert null list to Vector"),
            Vec::<bool>::new()
        );
    }

    #[test]
    fn value_option_and_box_wrappers() {
        let none_val: Value = (None::<i32>).as_value();
        assert_eq!(none_val, Value::Int32(None));
        let some_val: Value = Some(42_i32).as_value();
        assert_eq!(some_val, Value::Int32(Some(42)));
        let round: Option<i32> = Option::try_from_value(some_val).unwrap();
        assert_eq!(round, Some(42));
        let round_none: Option<i32> = Option::try_from_value(none_val).unwrap();
        assert_eq!(round_none, None);
        let boxed: Value = Box::new(11_i16).as_value();
        assert_eq!(boxed, Value::Int16(Some(11)));
        let unboxed: Box<i16> = Box::<i16>::try_from_value(boxed).unwrap();
        assert_eq!(*unboxed, 11);
    }

    #[test]
    fn value_shared_wrappers_arc_rc_cell_refcell() {
        use std::{
            cell::{Cell, RefCell},
            rc::Rc,
            sync::Arc,
        };
        let arc_v: Value = Arc::new(5_u8).as_value();
        assert_eq!(arc_v, Value::UInt8(Some(5)));
        let rc_v: Value = Rc::new(7_u16).as_value();
        assert_eq!(rc_v, Value::UInt16(Some(7)));
        let cell_v: Value = Cell::new(9_i32).as_value();
        assert_eq!(cell_v, Value::Int32(Some(9)));
        let refcell_v: Value = RefCell::new(13_i64).as_value();
        assert_eq!(refcell_v, Value::Int64(Some(13)));
        let arc_out: Arc<i8> = Arc::try_from_value(Arc::new(1_i8).as_value()).unwrap();
        assert_eq!(*arc_out, 1);
        let rc_out: Rc<i16> = Rc::try_from_value(Rc::new(2_i16).as_value()).unwrap();
        assert_eq!(*rc_out, 2);
        let cell_out: Cell<i32> = Cell::try_from_value(Cell::new(3_i32).as_value()).unwrap();
        assert_eq!(cell_out.get(), 3);
        let refcell_out: RefCell<i64> =
            RefCell::try_from_value(RefCell::new(4_i64).as_value()).unwrap();
        assert_eq!(*refcell_out.borrow(), 4);
    }

    #[test]
    fn value_map_semantics() {
        use std::collections::{BTreeMap, HashMap};
        let mut m1: HashMap<String, i32> = HashMap::new();
        m1.insert("a".into(), 1);
        let mut m2: HashMap<String, i32> = HashMap::new();
        m2.insert("b".into(), 2);
        let v1 = m1.clone().as_value();
        let v2 = m2.clone().as_value();
        assert_eq!(v1, v2, "Map Value equality only considers emptiness + type");
        let empty_map_v = HashMap::<String, i32>::new().as_value();
        assert_ne!(v1, empty_map_v);
        let round1: HashMap<String, i32> = HashMap::try_from_value(v1).unwrap();
        assert_eq!(round1.len(), 1);
        let round_empty: HashMap<String, i32> = HashMap::try_from_value(empty_map_v).unwrap();
        assert!(round_empty.is_empty());
        let mut bt: BTreeMap<String, bool> = BTreeMap::new();
        bt.insert("x".into(), true);
        let bt_v = bt.clone().as_value();
        let bt_rt: BTreeMap<String, bool> = BTreeMap::try_from_value(bt_v).unwrap();
        assert_eq!(bt_rt.get("x"), Some(&true));
    }

    #[test]
    fn value_struct_placeholder() {
        let s = Value::Struct(
            Some(vec![("id".into(), 1_i32.as_value())]),
            vec![("id".into(), i32::as_empty_value())],
            TableRef::new("special_type_t".into()),
        );
        let s_diff = Value::Struct(
            Some(vec![("id".into(), 2_i32.as_value())]),
            vec![("id".into(), i32::as_empty_value())],
            TableRef::new("special_type_t".into()),
        );
        assert_ne!(s, s_diff);
    }

    #[test]
    fn value_is_null() {
        assert!(Value::Null.is_null());
        assert!(Value::Boolean(None).is_null());
        assert!(Value::Int8(None).is_null());
        assert!(Value::Int16(None).is_null());
        assert!(Value::Int32(None).is_null());
        assert!(Value::Int64(None).is_null());
        assert!(Value::Int128(None).is_null());
        assert!(Value::UInt8(None).is_null());
        assert!(Value::UInt16(None).is_null());
        assert!(Value::UInt32(None).is_null());
        assert!(Value::UInt64(None).is_null());
        assert!(Value::UInt128(None).is_null());
        assert!(Value::Float32(None).is_null());
        assert!(Value::Float64(None).is_null());
        assert!(Value::Decimal(None, 0, 0).is_null());
        assert!(Value::Char(None).is_null());
        assert!(Value::Varchar(None).is_null());
        assert!(Value::Blob(None).is_null());
        assert!(Value::Date(None).is_null());
        assert!(Value::Time(None).is_null());
        assert!(Value::Timestamp(None).is_null());
        assert!(Value::TimestampWithTimezone(None).is_null());
        assert!(Value::Interval(None).is_null());
        assert!(Value::Uuid(None).is_null());
        assert!(Value::Array(None, Box::new(Value::Int32(None)), 3).is_null());
        assert!(Value::List(None, Box::new(Value::Int32(None))).is_null());
        assert!(
            Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            )
            .is_null()
        );
        assert!(Value::Json(None).is_null());
        assert!(Value::Json(Some(serde_json::Value::Null)).is_null());
        assert!(Value::Struct(None, vec![], TableRef::new("t".into()),).is_null());
        assert!(Value::Unknown(None).is_null());

        assert!(!Value::Boolean(Some(false)).is_null());
        assert!(!Value::Int32(Some(0)).is_null());
        assert!(!Value::Varchar(Some("".into())).is_null());
        assert!(!Value::Json(Some(serde_json::json!(42))).is_null());
    }

    #[test]
    fn value_as_null() {
        assert!(Value::Null.as_null().is_null());
        assert_eq!(Value::Boolean(Some(true)).as_null(), Value::Boolean(None));
        assert_eq!(Value::Int8(Some(5)).as_null(), Value::Int8(None));
        assert_eq!(Value::Int16(Some(5)).as_null(), Value::Int16(None));
        assert_eq!(Value::Int32(Some(5)).as_null(), Value::Int32(None));
        assert_eq!(Value::Int64(Some(5)).as_null(), Value::Int64(None));
        assert_eq!(Value::Int128(Some(5)).as_null(), Value::Int128(None));
        assert_eq!(Value::UInt8(Some(5)).as_null(), Value::UInt8(None));
        assert_eq!(Value::UInt16(Some(5)).as_null(), Value::UInt16(None));
        assert_eq!(Value::UInt32(Some(5)).as_null(), Value::UInt32(None));
        assert_eq!(Value::UInt64(Some(5)).as_null(), Value::UInt64(None));
        assert_eq!(Value::UInt128(Some(5)).as_null(), Value::UInt128(None));
        assert_eq!(Value::Float32(Some(1.0)).as_null(), Value::Float32(None));
        assert_eq!(Value::Float64(Some(1.0)).as_null(), Value::Float64(None));
        assert_eq!(
            Value::Decimal(Some(Decimal::from(10)), 10, 2).as_null(),
            Value::Decimal(None, 10, 2)
        );
        assert_eq!(Value::Char(Some('x')).as_null(), Value::Char(None));
        assert_eq!(
            Value::Varchar(Some("hi".into())).as_null(),
            Value::Varchar(None)
        );
        assert_eq!(
            Value::Blob(Some(vec![1, 2].into())).as_null(),
            Value::Blob(None)
        );
        assert_eq!(
            Value::Date(Some(
                time::Date::from_calendar_date(2025, Month::January, 1).unwrap()
            ))
            .as_null(),
            Value::Date(None)
        );
        assert_eq!(
            Value::Time(Some(time::Time::from_hms(0, 0, 0).unwrap())).as_null(),
            Value::Time(None)
        );
        assert_eq!(
            Value::Timestamp(Some(time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2025, Month::January, 1).unwrap(),
                time::Time::from_hms(0, 0, 0).unwrap(),
            )))
            .as_null(),
            Value::Timestamp(None)
        );
        assert_eq!(
            Value::TimestampWithTimezone(Some(time::OffsetDateTime::now_utc())).as_null(),
            Value::TimestampWithTimezone(None)
        );
        assert_eq!(
            Value::Interval(Some(Interval::from_days(1))).as_null(),
            Value::Interval(None)
        );
        assert_eq!(Value::Uuid(Some(Uuid::nil())).as_null(), Value::Uuid(None));
        assert_eq!(
            Value::Array(
                Some(vec![Value::Int32(Some(1))].into()),
                Box::new(Value::Int32(None)),
                1,
            )
            .as_null(),
            Value::Array(None, Box::new(Value::Int32(None)), 1)
        );
        assert_eq!(
            Value::List(Some(vec![]), Box::new(Value::Boolean(None))).as_null(),
            Value::List(None, Box::new(Value::Boolean(None)))
        );
        assert_eq!(
            Value::Map(
                Some(std::collections::HashMap::new()),
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None)),
            )
            .as_null(),
            Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            )
        );
        assert_eq!(
            Value::Json(Some(serde_json::json!({"a": 1}))).as_null(),
            Value::Json(None)
        );
        assert_eq!(
            Value::Struct(
                Some(vec![("id".into(), 1_i32.as_value())]),
                vec![("id".into(), i32::as_empty_value())],
                TableRef::new("t".into()),
            )
            .as_null(),
            Value::Struct(
                None,
                vec![("id".into(), i32::as_empty_value())],
                TableRef::new("t".into()),
            )
        );
        assert!(Value::Unknown(Some("x".into())).as_null().is_null());
    }

    #[test]
    fn value_is_scalar() {
        assert!(Value::Boolean(Some(true)).is_scalar());
        assert!(Value::Int8(Some(1)).is_scalar());
        assert!(Value::Int16(Some(1)).is_scalar());
        assert!(Value::Int32(Some(1)).is_scalar());
        assert!(Value::Int64(Some(1)).is_scalar());
        assert!(Value::Int128(Some(1)).is_scalar());
        assert!(Value::UInt8(Some(1)).is_scalar());
        assert!(Value::UInt16(Some(1)).is_scalar());
        assert!(Value::UInt32(Some(1)).is_scalar());
        assert!(Value::UInt64(Some(1)).is_scalar());
        assert!(Value::UInt128(Some(1)).is_scalar());
        assert!(Value::Float32(Some(1.0)).is_scalar());
        assert!(Value::Float64(Some(1.0)).is_scalar());
        assert!(Value::Decimal(Some(Decimal::from(1)), 0, 0).is_scalar());
        assert!(Value::Char(Some('a')).is_scalar());
        assert!(Value::Varchar(Some("x".into())).is_scalar());
        assert!(Value::Blob(Some(vec![].into())).is_scalar());
        assert!(Value::Date(None).is_scalar());
        assert!(Value::Time(None).is_scalar());
        assert!(Value::Timestamp(None).is_scalar());
        assert!(Value::TimestampWithTimezone(None).is_scalar());
        assert!(Value::Interval(None).is_scalar());
        assert!(Value::Uuid(None).is_scalar());
        assert!(Value::Unknown(None).is_scalar());

        assert!(!Value::Null.is_scalar());
        assert!(!Value::Array(None, Box::new(Value::Int32(None)), 1).is_scalar());
        assert!(!Value::List(None, Box::new(Value::Int32(None))).is_scalar());
        assert!(
            !Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            )
            .is_scalar()
        );
        assert!(!Value::Json(None).is_scalar());
        assert!(!Value::Struct(None, vec![], TableRef::new("t".into())).is_scalar());
    }

    #[test]
    fn value_same_type() {
        assert!(Value::Int32(Some(1)).same_type(&Value::Int32(Some(99))));
        assert!(Value::Int32(Some(1)).same_type(&Value::Int32(None)));
        assert!(!Value::Int32(Some(1)).same_type(&Value::Int64(Some(1))));

        assert!(Value::Decimal(None, 10, 2).same_type(&Value::Decimal(None, 10, 2)));
        assert!(Value::Decimal(None, 0, 2).same_type(&Value::Decimal(None, 10, 2)));
        assert!(Value::Decimal(None, 10, 0).same_type(&Value::Decimal(None, 10, 2)));
        assert!(!Value::Decimal(None, 10, 2).same_type(&Value::Decimal(None, 8, 2)));
        assert!(!Value::Decimal(None, 10, 2).same_type(&Value::Decimal(None, 10, 3)));

        assert!(
            Value::Array(None, Box::new(Value::Int32(None)), 5).same_type(&Value::Array(
                None,
                Box::new(Value::Int32(None)),
                5
            ))
        );
        assert!(
            !Value::Array(None, Box::new(Value::Int32(None)), 5).same_type(&Value::Array(
                None,
                Box::new(Value::Int32(None)),
                3
            ))
        );
        assert!(
            !Value::Array(None, Box::new(Value::Int32(None)), 5).same_type(&Value::Array(
                None,
                Box::new(Value::Int64(None)),
                5
            ))
        );

        assert!(
            Value::List(None, Box::new(Value::Varchar(None)))
                .same_type(&Value::List(None, Box::new(Value::Varchar(None))))
        );
        assert!(
            !Value::List(None, Box::new(Value::Varchar(None)))
                .same_type(&Value::List(None, Box::new(Value::Int32(None))))
        );

        assert!(
            Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            )
            .same_type(&Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None)),
            ))
        );
        assert!(
            !Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            )
            .same_type(&Value::Map(
                None,
                Box::new(Value::Int32(None)),
                Box::new(Value::Int32(None)),
            ))
        );
    }

    #[test]
    fn value_try_as() {
        let v = Value::Int32(Some(42));
        assert_eq!(v.clone().try_as(&Value::Int32(None)).unwrap(), v);
        assert_eq!(
            Value::Int32(Some(1)).try_as(&Value::Boolean(None)).unwrap(),
            Value::Boolean(Some(true))
        );
        assert_eq!(
            Value::Int32(Some(42)).try_as(&Value::Int8(None)).unwrap(),
            Value::Int8(Some(42))
        );
        assert_eq!(
            Value::Int8(Some(10)).try_as(&Value::Int16(None)).unwrap(),
            Value::Int16(Some(10))
        );
        assert_eq!(
            Value::Int16(Some(100)).try_as(&Value::Int32(None)).unwrap(),
            Value::Int32(Some(100))
        );
        assert_eq!(
            Value::Int32(Some(1000))
                .try_as(&Value::Int64(None))
                .unwrap(),
            Value::Int64(Some(1000))
        );
        assert_eq!(
            Value::Int64(Some(10000))
                .try_as(&Value::Int128(None))
                .unwrap(),
            Value::Int128(Some(10000))
        );
        assert_eq!(
            Value::Int32(Some(5)).try_as(&Value::UInt8(None)).unwrap(),
            Value::UInt8(Some(5))
        );
        assert_eq!(
            Value::Int32(Some(5)).try_as(&Value::UInt16(None)).unwrap(),
            Value::UInt16(Some(5))
        );
        assert_eq!(
            Value::Int32(Some(5)).try_as(&Value::UInt32(None)).unwrap(),
            Value::UInt32(Some(5))
        );
        assert_eq!(
            Value::Int32(Some(5)).try_as(&Value::UInt64(None)).unwrap(),
            Value::UInt64(Some(5))
        );
        assert_eq!(
            Value::Int32(Some(5)).try_as(&Value::UInt128(None)).unwrap(),
            Value::UInt128(Some(5))
        );
        assert_eq!(
            Value::Float64(Some(5.0))
                .try_as(&Value::Float32(None))
                .unwrap(),
            Value::Float32(Some(5.0))
        );
        assert_eq!(
            Value::Float32(Some(5.0))
                .try_as(&Value::Float64(None))
                .unwrap(),
            Value::Float64(Some(5.0))
        );
        assert_eq!(
            Value::Int32(Some(5))
                .try_as(&Value::Decimal(None, 0, 0))
                .unwrap(),
            Value::Decimal(Some(Decimal::from(5)), 0, 0)
        );
        assert_eq!(
            Value::Char(Some('x'))
                .try_as(&Value::Varchar(None))
                .unwrap(),
            Value::Varchar(Some("x".into()))
        );

        assert!(Value::Int32(Some(5)).try_as(&Value::Json(None)).is_err());
        assert!(
            Value::Int32(Some(5))
                .try_as(&Value::Array(None, Box::new(Value::Int32(None)), 1))
                .is_err()
        );

        assert_eq!(
            Value::UInt8(Some(10)).try_as(&Value::UInt8(None)).unwrap(),
            Value::UInt8(Some(10))
        );
        assert_eq!(
            Value::UInt16(Some(20))
                .try_as(&Value::UInt16(None))
                .unwrap(),
            Value::UInt16(Some(20))
        );
        assert_eq!(
            Value::UInt32(Some(30))
                .try_as(&Value::UInt32(None))
                .unwrap(),
            Value::UInt32(Some(30))
        );
        assert_eq!(
            Value::UInt64(Some(40))
                .try_as(&Value::UInt64(None))
                .unwrap(),
            Value::UInt64(Some(40))
        );
        assert_eq!(
            Value::UInt128(Some(50))
                .try_as(&Value::UInt128(None))
                .unwrap(),
            Value::UInt128(Some(50))
        );
        assert_eq!(
            Value::Char(Some('z')).try_as(&Value::Char(None)).unwrap(),
            Value::Char(Some('z'))
        );
        assert_eq!(
            Value::Varchar(Some("hi".into()))
                .try_as(&Value::Varchar(None))
                .unwrap(),
            Value::Varchar(Some("hi".into()))
        );
        assert_eq!(
            Value::Blob(Some(vec![1, 2].into()))
                .try_as(&Value::Blob(None))
                .unwrap(),
            Value::Blob(Some(vec![1, 2].into()))
        );
        let d = time::Date::from_calendar_date(2024, time::Month::January, 1).unwrap();
        assert_eq!(
            Value::Date(Some(d)).try_as(&Value::Date(None)).unwrap(),
            Value::Date(Some(d))
        );
        let t = time::Time::from_hms(12, 30, 0).unwrap();
        assert_eq!(
            Value::Time(Some(t)).try_as(&Value::Time(None)).unwrap(),
            Value::Time(Some(t))
        );
        let ts = time::PrimitiveDateTime::new(d, t);
        assert_eq!(
            Value::Timestamp(Some(ts))
                .try_as(&Value::Timestamp(None))
                .unwrap(),
            Value::Timestamp(Some(ts))
        );
        let tstz = ts.assume_utc();
        assert_eq!(
            Value::TimestampWithTimezone(Some(tstz))
                .try_as(&Value::TimestampWithTimezone(None))
                .unwrap(),
            Value::TimestampWithTimezone(Some(tstz))
        );
        assert_eq!(
            Value::Interval(Some(tank_core::Interval::from_days(5)))
                .try_as(&Value::Interval(None))
                .unwrap(),
            Value::Interval(Some(tank_core::Interval::from_days(5)))
        );
        assert_eq!(
            Value::Uuid(Some(Uuid::nil()))
                .try_as(&Value::Uuid(None))
                .unwrap(),
            Value::Uuid(Some(Uuid::nil()))
        );
    }

    #[test]
    fn value_partial_eq_complex() {
        assert_eq!(
            Value::Float32(Some(f32::NAN)),
            Value::Float32(Some(f32::NAN))
        );
        assert_eq!(
            Value::Float64(Some(f64::NAN)),
            Value::Float64(Some(f64::NAN))
        );
        assert_ne!(
            Value::Decimal(Some(Decimal::from(1)), 10, 2),
            Value::Decimal(Some(Decimal::from(1)), 10, 3)
        );
        assert_ne!(
            Value::Decimal(Some(Decimal::from(1)), 10, 2),
            Value::Decimal(Some(Decimal::from(1)), 8, 2)
        );
        assert_ne!(
            Value::Unknown(Some("a".into())),
            Value::Unknown(Some("a".into()))
        );
        assert_ne!(Value::Int32(Some(1)), Value::Int64(Some(1)));
        assert_eq!(
            Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            ),
            Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            ),
        );
        let mut m = std::collections::HashMap::new();
        m.insert(Value::Varchar(Some("k".into())), Value::Int32(Some(1)));
        assert_ne!(
            Value::Map(
                Some(std::collections::HashMap::new()),
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None)),
            ),
            Value::Map(
                Some(m),
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None)),
            ),
        );
        assert_eq!(
            Value::Map(
                Some(std::collections::HashMap::new()),
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None)),
            ),
            Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            ),
        );
        assert_eq!(
            Value::Struct(
                Some(vec![("a".into(), 1_i32.as_value())]),
                vec![("a".into(), i32::as_empty_value())],
                TableRef::new("t".into()),
            ),
            Value::Struct(
                Some(vec![("a".into(), 1_i32.as_value())]),
                vec![("a".into(), i32::as_empty_value())],
                TableRef::new("t".into()),
            ),
        );
        assert_ne!(
            Value::Struct(Some(vec![]), vec![], TableRef::new("t1".into()),),
            Value::Struct(Some(vec![]), vec![], TableRef::new("t2".into()),),
        );
        assert_eq!(Value::UInt8(Some(1)), Value::UInt8(Some(1)));
        assert_ne!(Value::UInt8(Some(1)), Value::UInt8(Some(2)));
        assert_eq!(Value::UInt16(Some(10)), Value::UInt16(Some(10)));
        assert_ne!(Value::UInt16(Some(10)), Value::UInt16(Some(20)));
        assert_eq!(Value::UInt32(Some(100)), Value::UInt32(Some(100)));
        assert_eq!(Value::UInt64(Some(1000)), Value::UInt64(Some(1000)));
        assert_eq!(Value::UInt128(Some(10000)), Value::UInt128(Some(10000)));

        let mut m1 = std::collections::HashMap::new();
        m1.insert(Value::Varchar(Some("a".into())), Value::Int32(Some(1)));
        let mut m2 = std::collections::HashMap::new();
        m2.insert(Value::Varchar(Some("a".into())), Value::Int32(Some(1)));
        assert_eq!(
            Value::Map(
                Some(m1),
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            ),
            Value::Map(
                Some(m2),
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            ),
        );
    }

    #[test]
    fn value_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Value::Null);
        set.insert(Value::Boolean(Some(true)));
        set.insert(Value::Boolean(None));
        set.insert(Value::Int8(Some(1)));
        set.insert(Value::Int16(Some(2)));
        set.insert(Value::Int32(Some(3)));
        set.insert(Value::Int64(Some(4)));
        set.insert(Value::Int128(Some(5)));
        set.insert(Value::UInt8(Some(6)));
        set.insert(Value::UInt16(Some(7)));
        set.insert(Value::UInt32(Some(8)));
        set.insert(Value::UInt64(Some(9)));
        set.insert(Value::UInt128(Some(10)));
        set.insert(Value::Float32(Some(1.5)));
        set.insert(Value::Float32(None));
        set.insert(Value::Float64(Some(2.5)));
        set.insert(Value::Float64(None));
        set.insert(Value::Decimal(Some(Decimal::from(1)), 10, 2));
        set.insert(Value::Char(Some('a')));
        set.insert(Value::Varchar(Some("hello".into())));
        set.insert(Value::Blob(Some(vec![1, 2].into())));
        set.insert(Value::Uuid(Some(Uuid::nil())));
        set.insert(Value::Json(Some(serde_json::json!(1))));
        set.insert(Value::Array(
            Some(vec![Value::Int32(Some(1))].into()),
            Box::new(Value::Int32(None)),
            1,
        ));
        set.insert(Value::List(
            Some(vec![Value::Int32(Some(1))]),
            Box::new(Value::Int32(None)),
        ));
        set.insert(Value::Map(
            Some(std::collections::HashMap::new()),
            Box::new(Value::Varchar(None)),
            Box::new(Value::Int32(None)),
        ));
        let mut m_hash = std::collections::HashMap::new();
        m_hash.insert(Value::Varchar(Some("k".into())), Value::Int32(Some(1)));
        set.insert(Value::Map(
            Some(m_hash),
            Box::new(Value::Varchar(None)),
            Box::new(Value::Int32(None)),
        ));
        set.insert(Value::Map(
            None,
            Box::new(Value::Varchar(None)),
            Box::new(Value::Int32(None)),
        ));
        set.insert(Value::Struct(
            Some(vec![("a".into(), 1_i32.as_value())]),
            vec![("a".into(), i32::as_empty_value())],
            TableRef::new("t".into()),
        ));
        set.insert(Value::Struct(
            None,
            vec![("a".into(), i32::as_empty_value())],
            TableRef::new("t2".into()),
        ));
        set.insert(Value::Unknown(Some("x".into())));
        let d = time::Date::from_calendar_date(2024, time::Month::January, 15).unwrap();
        set.insert(Value::Date(Some(d)));
        let t = time::Time::from_hms(10, 30, 0).unwrap();
        set.insert(Value::Time(Some(t)));
        set.insert(Value::Timestamp(Some(time::PrimitiveDateTime::new(d, t))));
        set.insert(Value::TimestampWithTimezone(Some(
            time::PrimitiveDateTime::new(d, t).assume_utc(),
        )));
        set.insert(Value::Interval(Some(tank_core::Interval::from_days(7))));
        assert!(set.len() >= 25);
    }

    #[test]
    fn value_display() {
        assert_eq!(format!("{}", Value::Null), "NULL");
        assert_eq!(format!("{}", Value::Boolean(Some(true))), "true");
        assert_eq!(format!("{}", Value::Int32(Some(42))), "42");
        assert_eq!(format!("{}", Value::Float64(Some(3.14))), "3.14");
        assert_eq!(format!("{}", Value::Varchar(Some("hello".into()))), "hello");
        assert_eq!(format!("{}", Value::Char(Some('x'))), "x");
        assert_eq!(
            format!("{}", Value::Uuid(Some(Uuid::nil()))),
            "00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(format!("{}", Value::UInt64(Some(999))), "999");
        assert_eq!(format!("{}", Value::Int128(Some(-100))), "-100");
        assert_eq!(format!("{}", Value::Boolean(Some(false))), "false");
    }
}

#[test]
fn value_value() {
    assert_eq!(
        Value::try_from_value("hello".as_value()).expect("Could not get a value from a value"),
        Cow::Borrowed("hello").as_value()
    );
    assert_ne!("2hello".as_value(), Cow::Borrowed("1hello").as_value());
    assert_eq!(
        "hello".to_string().as_value(),
        Cow::Borrowed("hello").as_value(),
    );
    assert_ne!(
        Cow::Borrowed("hello3").as_value(),
        "hello4".to_string().as_value(),
    );
    assert!(matches!(Value::as_empty_value(), Value::Null));
    assert_eq!(
        <[u128; 23]>::as_empty_value(),
        Value::Array(None, Box::new(Value::UInt128(None)), 23)
    );
    assert!(Value::parse("some input").is_err());
}

#[cfg(test)]
mod as_value_tests {
    use tank::{AsValue, Value};

    #[test]
    fn nonzero_conversions() {
        use std::num::*;
        let v = NonZeroI32::new(42).unwrap().as_value();
        assert_eq!(v, Value::Int32(Some(42)));
        let back = NonZeroI32::try_from_value(v).unwrap();
        assert_eq!(back.get(), 42);
        assert_eq!(NonZeroI32::as_empty_value(), Value::Int32(None));

        let v = NonZeroU64::new(100).unwrap().as_value();
        assert_eq!(v, Value::UInt64(Some(100)));
        let back = NonZeroU64::try_from_value(v).unwrap();
        assert_eq!(back.get(), 100);

        assert!(NonZeroI32::try_from_value(Value::Int32(Some(0))).is_err());
    }

    #[test]
    fn bool_from_various_types() {
        assert_eq!(bool::try_from_value(Value::Int8(Some(1))).unwrap(), true);
        assert_eq!(bool::try_from_value(Value::Int8(Some(0))).unwrap(), false);
        assert_eq!(bool::try_from_value(Value::Int16(Some(1))).unwrap(), true);
        assert_eq!(bool::try_from_value(Value::UInt8(Some(1))).unwrap(), true);
        assert_eq!(bool::try_from_value(Value::UInt16(Some(0))).unwrap(), false);
        assert_eq!(bool::try_from_value(Value::UInt32(Some(1))).unwrap(), true);
        assert_eq!(bool::try_from_value(Value::UInt64(Some(0))).unwrap(), false);
        assert_eq!(bool::try_from_value(Value::UInt128(Some(1))).unwrap(), true);
        assert_eq!(bool::try_from_value(Value::Int128(Some(0))).unwrap(), false);

        assert_eq!(bool::parse("true").unwrap(), true);
        assert_eq!(bool::parse("false").unwrap(), false);
        assert_eq!(bool::parse("T").unwrap(), true);
        assert_eq!(bool::parse("F").unwrap(), false);
        assert_eq!(bool::parse("1").unwrap(), true);
        assert_eq!(bool::parse("0").unwrap(), false);
        assert!(bool::parse("maybe").is_err());

        assert_eq!(
            bool::try_from_value(Value::Json(Some(serde_json::json!(true)))).unwrap(),
            true
        );
        assert_eq!(
            bool::try_from_value(Value::Json(Some(serde_json::json!(0)))).unwrap(),
            false
        );
        assert_eq!(
            bool::try_from_value(Value::Json(Some(serde_json::json!(1)))).unwrap(),
            true
        );
    }

    #[test]
    fn decimal_from_various_types() {
        use rust_decimal::Decimal;
        assert_eq!(
            Decimal::try_from_value(Value::Int8(Some(5))).unwrap(),
            Decimal::new(5, 0)
        );
        assert_eq!(
            Decimal::try_from_value(Value::UInt8(Some(10))).unwrap(),
            Decimal::new(10, 0)
        );
        assert_eq!(
            Decimal::try_from_value(Value::UInt16(Some(20))).unwrap(),
            Decimal::new(20, 0)
        );
        assert_eq!(
            Decimal::try_from_value(Value::UInt32(Some(30))).unwrap(),
            Decimal::new(30, 0)
        );
        assert_eq!(
            Decimal::try_from_value(Value::UInt64(Some(40))).unwrap(),
            Decimal::new(40, 0)
        );
        assert!(Decimal::try_from_value(Value::Float32(Some(1.5))).is_ok());
        assert!(Decimal::try_from_value(Value::Float64(Some(2.5))).is_ok());
        assert!(Decimal::try_from_value(Value::Varchar(Some("3.14".into()))).is_ok());
        assert!(Decimal::try_from_value(Value::Unknown(Some("1.23".into()))).is_ok());

        assert!(Decimal::try_from_value(Value::Json(Some(serde_json::json!(42.5)))).is_ok());
    }

    #[test]
    fn integer_from_json() {
        assert_eq!(
            i32::try_from_value(Value::Json(Some(serde_json::json!(42)))).unwrap(),
            42
        );
        assert_eq!(
            i32::try_from_value(Value::Json(Some(serde_json::json!("99")))).unwrap(),
            99
        );
        assert_eq!(
            i32::try_from_value(Value::Json(Some(serde_json::json!(5.0)))).unwrap(),
            5
        );
    }

    #[test]
    fn integer_from_varchar_and_unknown() {
        assert_eq!(
            i32::try_from_value(Value::Varchar(Some("42".into()))).unwrap(),
            42
        );
        assert_eq!(
            i32::try_from_value(Value::Unknown(Some("99".into()))).unwrap(),
            99
        );
    }

    #[test]
    fn integer_from_float64() {
        assert_eq!(i32::try_from_value(Value::Float64(Some(10.0))).unwrap(), 10);
        assert!(i32::try_from_value(Value::Float64(Some(10.5))).is_err());
    }

    #[test]
    fn integer_cross_type_conversions() {
        assert_eq!(i16::try_from_value(Value::Int8(Some(5))).unwrap(), 5);
        assert_eq!(i16::try_from_value(Value::UInt8(Some(200))).unwrap(), 200);
        assert_eq!(i16::try_from_value(Value::UInt16(Some(100))).unwrap(), 100);
        assert_eq!(i64::try_from_value(Value::UInt8(Some(1))).unwrap(), 1);
        assert_eq!(i64::try_from_value(Value::UInt16(Some(2))).unwrap(), 2);
        assert_eq!(i64::try_from_value(Value::UInt32(Some(3))).unwrap(), 3);
        assert_eq!(i64::try_from_value(Value::UInt64(Some(4))).unwrap(), 4);
        assert_eq!(i128::try_from_value(Value::UInt8(Some(1))).unwrap(), 1);
        assert_eq!(
            i128::try_from_value(Value::UInt128(Some(999))).unwrap(),
            999
        );
        assert_eq!(u64::try_from_value(Value::UInt8(Some(1))).unwrap(), 1);
        assert_eq!(u64::try_from_value(Value::UInt16(Some(2))).unwrap(), 2);
        assert_eq!(u64::try_from_value(Value::UInt32(Some(3))).unwrap(), 3);
        assert_eq!(u128::try_from_value(Value::UInt8(Some(1))).unwrap(), 1);
        assert_eq!(u128::try_from_value(Value::UInt64(Some(9))).unwrap(), 9);
    }

    #[test]
    fn integer_decimal_conversions() {
        use rust_decimal::Decimal;
        assert_eq!(
            i32::try_from_value(Value::Decimal(Some(Decimal::new(42, 0)), 0, 0)).unwrap(),
            42
        );
        assert!(i32::try_from_value(Value::Decimal(Some(Decimal::new(155, 1)), 0, 0)).is_err());
        assert_eq!(
            i64::try_from_value(Value::Decimal(Some(Decimal::new(100, 0)), 0, 0)).unwrap(),
            100
        );
        assert_eq!(
            u64::try_from_value(Value::Decimal(Some(Decimal::new(50, 0)), 0, 0)).unwrap(),
            50
        );
    }

    #[test]
    fn float_conversions() {
        assert!(
            f32::try_from_value(Value::Decimal(
                Some(rust_decimal::Decimal::new(15, 1)),
                0,
                0
            ))
            .is_ok()
        );
        assert_eq!(f32::try_from_value(Value::Float64(Some(2.5))).unwrap(), 2.5);
        assert_eq!(f64::try_from_value(Value::Float32(Some(1.5))).unwrap(), 1.5);
        assert!(
            f64::try_from_value(Value::Decimal(
                Some(rust_decimal::Decimal::new(25, 1)),
                0,
                0
            ))
            .is_ok()
        );
        assert!(f32::try_from_value(Value::Json(Some(serde_json::json!(1.5)))).is_ok());
        assert!(f64::try_from_value(Value::Json(Some(serde_json::json!(2.5)))).is_ok());
    }

    #[test]
    fn string_from_various_types() {
        assert_eq!(
            String::try_from_value(Value::Int32(Some(42))).unwrap(),
            "42"
        );
        assert_eq!(
            String::try_from_value(Value::Float64(Some(3.14))).unwrap(),
            "3.14"
        );
        assert_eq!(String::try_from_value(Value::Char(Some('x'))).unwrap(), "x");
        assert!(String::try_from_value(Value::Uuid(Some(uuid::Uuid::nil()))).is_ok());
        assert_eq!(
            String::try_from_value(Value::Json(Some(serde_json::json!("hi")))).unwrap(),
            "hi"
        );
    }

    #[test]
    fn char_conversions() {
        assert_eq!(
            char::try_from_value(Value::Varchar(Some("a".into()))).unwrap(),
            'a'
        );
        assert!(char::try_from_value(Value::Varchar(Some("ab".into()))).is_err());
        assert_eq!(
            char::try_from_value(Value::Json(Some(serde_json::json!("z")))).unwrap(),
            'z'
        );
    }

    #[test]
    fn blob_parse() {
        let v = Box::<[u8]>::try_from_value(Value::Varchar(Some("deadbeef".into()))).unwrap();
        assert_eq!(v.as_ref(), &[0xde, 0xad, 0xbe, 0xef]);
        let v2 = Box::<[u8]>::parse("\\xCAFE").unwrap();
        assert_eq!(v2.as_ref(), &[0xCA, 0xFE]);
    }

    #[test]
    fn interval_parse() {
        use tank_core::Interval;
        let i = Interval::parse("'1 year 2 months 3 days'").unwrap();
        assert_eq!(i.months, 14); // 12 + 2
        assert_eq!(i.days, 3);

        let i2 = Interval::parse("5 hours 30 minutes").unwrap();
        assert!(!i2.is_zero());

        let i3 = Interval::parse("01:30:00").unwrap();
        assert!(!i3.is_zero());
    }

    #[test]
    fn date_parse() {
        let d = <time::Date as AsValue>::parse("2024-06-15").unwrap();
        assert_eq!(
            d,
            time::Date::from_calendar_date(2024, time::Month::June, 15).unwrap()
        );
    }

    #[test]
    fn time_parse() {
        let t = <time::Time as AsValue>::parse("14:30:00").unwrap();
        assert_eq!(t, time::Time::from_hms(14, 30, 0).unwrap());
    }

    #[test]
    fn timestamp_parse() {
        let ts = <time::PrimitiveDateTime as AsValue>::parse("2024-06-15T14:30:00").unwrap();
        let d = time::Date::from_calendar_date(2024, time::Month::June, 15).unwrap();
        let t = time::Time::from_hms(14, 30, 0).unwrap();
        assert_eq!(ts, time::PrimitiveDateTime::new(d, t));
    }

    #[test]
    fn offset_datetime_parse() {
        let odt = <time::OffsetDateTime as AsValue>::parse("2024-06-15T14:30:00+05:00").unwrap();
        assert_eq!(odt.offset().whole_hours(), 5);
    }

    #[test]
    fn uuid_from_varchar() {
        let u = uuid::Uuid::try_from_value(Value::Varchar(Some(
            "550e8400-e29b-41d4-a716-446655440000".into(),
        )))
        .unwrap();
        assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn time_from_interval() {
        use tank_core::Interval;
        let t = time::Time::try_from_value(Value::Interval(Some(
            Interval::from_hours(2) + Interval::from_mins(30),
        )))
        .unwrap();
        assert_eq!(t, time::Time::from_hms(2, 30, 0).unwrap());
    }

    #[test]
    fn date_from_timestamp() {
        let d = time::Date::from_calendar_date(2024, time::Month::January, 1).unwrap();
        let t = time::Time::MIDNIGHT;
        let ts = time::PrimitiveDateTime::new(d, t);
        let result = time::Date::try_from_value(Value::Timestamp(Some(ts))).unwrap();
        assert_eq!(result, d);

        let t2 = time::Time::from_hms(12, 0, 0).unwrap();
        let ts2 = time::PrimitiveDateTime::new(d, t2);
        assert!(time::Date::try_from_value(Value::Timestamp(Some(ts2))).is_err());
    }

    #[test]
    fn offset_datetime_from_timestamp() {
        let d = time::Date::from_calendar_date(2024, time::Month::January, 1).unwrap();
        let t = time::Time::from_hms(12, 0, 0).unwrap();
        let ts = time::PrimitiveDateTime::new(d, t);
        let odt = time::OffsetDateTime::try_from_value(Value::Timestamp(Some(ts))).unwrap();
        assert_eq!(odt.date(), d);
    }

    #[test]
    fn vec_and_list_conversions() {
        let v = vec![1_i32, 2, 3].as_value();
        let back: Vec<i32> = Vec::try_from_value(v).unwrap();
        assert_eq!(back, vec![1, 2, 3]);

        let v = Value::Json(Some(serde_json::json!([1, 2, 3])));
        let back: Vec<i32> = Vec::try_from_value(v).unwrap();
        assert_eq!(back, vec![1, 2, 3]);
    }

    #[test]
    fn array_conversions() {
        let v = [10_i32, 20, 30].as_value();
        let back: [i32; 3] = <[i32; 3]>::try_from_value(v).unwrap();
        assert_eq!(back, [10, 20, 30]);
    }

    #[test]
    fn hashmap_conversions() {
        use std::collections::HashMap;
        let mut m = HashMap::new();
        m.insert("key".to_string(), 42_i32);
        let v = m.clone().as_value();
        let back: HashMap<String, i32> = HashMap::try_from_value(v).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn option_and_wrapper_conversions() {
        assert_eq!(Some(42_i32).as_value(), Value::Int32(Some(42)));
        assert_eq!(None::<i32>.as_value(), Value::Int32(None));
        let back: Option<i32> = Option::try_from_value(Value::Int32(Some(42))).unwrap();
        assert_eq!(back, Some(42));
        let none: Option<i32> = Option::try_from_value(Value::Int32(None)).unwrap();
        assert_eq!(none, None);

        assert_eq!(Box::new(42_i32).as_value(), Value::Int32(Some(42)));
        let back: Box<i32> = Box::try_from_value(Value::Int32(Some(42))).unwrap();
        assert_eq!(*back, 42);

        use std::sync::Arc;
        assert_eq!(Arc::new(42_i32).as_value(), Value::Int32(Some(42)));

        use std::rc::Rc;
        assert_eq!(Rc::new(42_i32).as_value(), Value::Int32(Some(42)));
    }

    #[test]
    fn fixed_decimal_round_trip() {
        use rust_decimal::Decimal;
        use tank_core::FixedDecimal;
        let fd: FixedDecimal<10, 2> = Decimal::new(1234, 2).into();
        let v = fd.as_value();
        let back: FixedDecimal<10, 2> = FixedDecimal::try_from_value(v).unwrap();
        assert_eq!(back.0, Decimal::new(1234, 2));
    }

    #[test]
    fn isize_usize_conversions() {
        assert_eq!(42_isize.as_value(), Value::Int64(Some(42)));
        let back: isize = isize::try_from_value(Value::Int64(Some(42))).unwrap();
        assert_eq!(back, 42);

        assert_eq!(42_usize.as_value(), Value::UInt64(Some(42)));
        let back: usize = usize::try_from_value(Value::UInt64(Some(42))).unwrap();
        assert_eq!(back, 42);
    }
}
