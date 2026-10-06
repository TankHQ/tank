#[cfg(test)]
mod tests {
    use rust_decimal::{Decimal, prelude::FromPrimitive};
    use serde_json::Number;
    use std::{
        borrow::Cow,
        cell::{Cell, RefCell},
        rc::Rc,
        sync::Arc,
    };
    use tank_core::{AsValue, Value};

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
        assert_eq!(i8::try_from_value(127_u8.as_value()).unwrap(), 127);
        assert!(i8::try_from_value(128_u8.as_value()).is_err());
        assert!(i8::try_from_value(200_u8.as_value()).is_err());
        assert!(i8::try_from_value(255_u8.as_value()).is_err());
        assert_eq!(i8::try_from_value((-128_i16).as_value()).unwrap(), -128);
        assert!(i8::try_from_value(128_i16.as_value()).is_err());
        assert!(i8::try_from_value((-129_i16).as_value()).is_err());
        assert!(i8::try_from_value(256_i16.as_value()).is_err());
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
        assert_eq!(i16::try_from_value((-128_i8).as_value()).unwrap(), -128);
        assert_eq!(i16::try_from_value(127_i8.as_value()).unwrap(), 127);
        assert_eq!(i16::try_from_value(255_u8.as_value()).unwrap(), 255);
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
    fn value_float_to_integer() {
        assert_eq!(i64::try_from_value(5.0_f64.as_value()).unwrap(), 5);
        assert_eq!(i32::try_from_value((5.0_f32).as_value()).unwrap(), 5);
        assert_eq!(u8::try_from_value((200.0_f32).as_value()).unwrap(), 200);
        assert_eq!(i128::try_from_value((-7.0_f64).as_value()).unwrap(), -7);

        assert!(i64::try_from_value(1.5_f64.as_value()).is_err());
        assert!(i32::try_from_value((1.5_f32).as_value()).is_err());
        assert!(i64::try_from_value(1e30_f64.as_value()).is_err());
        assert!(i64::try_from_value(f64::MAX.as_value()).is_err());
        assert!(u64::try_from_value(1e30_f64.as_value()).is_err());
        assert!(i32::try_from_value(1e30_f64.as_value()).is_err());
        assert!(i8::try_from_value(200.0_f64.as_value()).is_err());
        assert!(i8::try_from_value((-200.0_f64).as_value()).is_err());
        assert!(i8::try_from_value((200.0_f32).as_value()).is_err());
        assert!(u32::try_from_value((-1.0_f64).as_value()).is_err());

        assert!(i64::try_from_value(9_223_372_036_854_775_808.0_f64.as_value()).is_err());
        assert!(i64::try_from_value((9_223_372_036_854_775_808.0_f32).as_value()).is_err());
        assert!(i32::try_from_value(2_147_483_648.0_f64.as_value()).is_err());
        assert!(i8::try_from_value(128.0_f64.as_value()).is_err());
        assert!(u64::try_from_value(18_446_744_073_709_551_616.0_f64.as_value()).is_err());
        assert!(u8::try_from_value(256.0_f64.as_value()).is_err());
        assert!(
            i128::try_from_value(
                170_141_183_460_469_231_731_687_303_715_884_105_728.0_f64.as_value()
            )
            .is_err()
        );
        assert!(
            u128::try_from_value(
                340_282_366_920_938_463_463_374_607_431_768_211_456.0_f64.as_value()
            )
            .is_err()
        );
        assert_eq!(
            i64::try_from_value((9_223_372_036_854_775_808.0_f64 - 1024.0).as_value()).unwrap(),
            9_223_372_036_854_774_784,
        );
        assert_eq!(
            i64::try_from_value((-9_223_372_036_854_775_808.0_f64).as_value()).unwrap(),
            i64::MIN,
        );
        assert_eq!(
            i8::try_from_value((-128.0_f64).as_value()).unwrap(),
            i8::MIN
        );
        assert!(
            i64::try_from_value((-9_223_372_036_854_775_808.0_f64 - 2048.0).as_value()).is_err()
        );

        let json = |f: f64| serde_json::Value::Number(Number::from_f64(f).unwrap()).as_value();
        assert_eq!(i128::try_from_value(json(42.0)).unwrap(), 42);
        assert_eq!(i128::try_from_value(json(-7.0)).unwrap(), -7);
        assert!(i128::try_from_value(json(1e40)).is_err());
        assert!(i128::try_from_value(json(-1e40)).is_err());
        assert!(i64::try_from_value(json(1e30)).is_err());
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

        assert_eq!(char::try_from_value(Value::Int32(Some(65))).unwrap(), 'A');
        assert_eq!(char::try_from_value(Value::UInt8(Some(97))).unwrap(), 'a');
        assert!(char::try_from_value(Value::Int32(Some(-1))).is_err());
        assert!(char::try_from_value(Value::Int64(Some(u32::MAX as i64 + 1))).is_err());
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
}
