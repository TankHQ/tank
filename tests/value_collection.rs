#[cfg(test)]
mod tests {
    use rust_decimal::{Decimal, prelude::FromPrimitive};
    use std::{
        collections::{BTreeMap, HashMap, LinkedList, VecDeque},
        str::FromStr,
    };
    use tank::TableRef;
    use tank_core::{AsValue, Value};
    use uuid::Uuid;

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

        assert_eq!(<[char; 1]>::try_from_value("é".as_value()).unwrap(), ['é']);
        assert_eq!(
            <[char; 2]>::try_from_value("a€".as_value()).unwrap(),
            ['a', '€']
        );
        assert_eq!(
            <[char; 2]>::try_from_value("日本".as_value()).unwrap(),
            ['日', '本']
        );
        assert!(<[char; 2]>::try_from_value("a€b".as_value()).is_err());
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
    fn value_map_semantics() {
        let mut m1: HashMap<String, i32> = HashMap::new();
        m1.insert("a".into(), 1);
        let mut m2: HashMap<String, i32> = HashMap::new();
        m2.insert("b".into(), 2);
        let v1 = m1.clone().as_value();
        let v2 = m2.clone().as_value();
        assert_ne!(v1, v2, "Map Value equality must depend on the entries");
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
}
