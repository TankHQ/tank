#[cfg(test)]
mod tests {
    use tank::{
        Entity, Expression, FindOrder, GenericSqlWriter, IsAggregateFunction, IsAlias, IsAsterisk,
        IsConstant, IsFalse, IsQuestionMark, IsTrue, Order, expr,
    };

    #[derive(Entity)]
    struct Table {
        pub col_a: i64,
        #[tank(name = "second_column")]
        pub col_b: i128,
        pub str_column: String,
    }

    const WRITER: GenericSqlWriter = GenericSqlWriter {};

    #[test]
    fn visitor_is_true() {
        let mut out = Default::default();
        let mut ctx = Default::default();
        let e = expr!(true);
        assert!(e.accept_visitor(&mut IsTrue, &WRITER, &mut ctx, &mut out));
        let e = expr!(false);
        assert!(!e.accept_visitor(&mut IsTrue, &WRITER, &mut ctx, &mut out));
        let e = expr!(42);
        assert!(!e.accept_visitor(&mut IsTrue, &WRITER, &mut ctx, &mut out));
    }

    #[test]
    fn visitor_is_false() {
        let mut out = Default::default();
        let mut ctx = Default::default();
        let e = expr!(false);
        assert!(e.accept_visitor(&mut IsFalse, &WRITER, &mut ctx, &mut out));
        let e = expr!(true);
        assert!(!e.accept_visitor(&mut IsFalse, &WRITER, &mut ctx, &mut out));
    }

    #[test]
    fn visitor_is_constant() {
        let mut out = Default::default();
        let mut ctx = Default::default();
        let e = expr!(42);
        assert!(e.accept_visitor(&mut IsConstant, &WRITER, &mut ctx, &mut out));
        let e = expr!(NULL);
        assert!(e.accept_visitor(&mut IsConstant, &WRITER, &mut ctx, &mut out));
        let e = expr!("hello");
        assert!(e.accept_visitor(&mut IsConstant, &WRITER, &mut ctx, &mut out));
        let e = expr!(3.14);
        assert!(e.accept_visitor(&mut IsConstant, &WRITER, &mut ctx, &mut out));
        let e = expr!(true);
        assert!(e.accept_visitor(&mut IsConstant, &WRITER, &mut ctx, &mut out));
        // Column reference is not constant
        let e = expr!(Table::col_a);
        assert!(!e.accept_visitor(&mut IsConstant, &WRITER, &mut ctx, &mut out));
    }

    #[test]
    fn visitor_is_aggregate() {
        let mut out = Default::default();
        let mut ctx = Default::default();
        let e = expr!(COUNT(*));
        assert!(e.accept_visitor(&mut IsAggregateFunction, &WRITER, &mut ctx, &mut out));
        let e = expr!(SUM(Table::col_a));
        assert!(e.accept_visitor(&mut IsAggregateFunction, &WRITER, &mut ctx, &mut out));
        let e = expr!(AVG(Table::col_a));
        assert!(e.accept_visitor(&mut IsAggregateFunction, &WRITER, &mut ctx, &mut out));
        let e = expr!(MIN(Table::col_a));
        assert!(e.accept_visitor(&mut IsAggregateFunction, &WRITER, &mut ctx, &mut out));
        let e = expr!(MAX(Table::col_a));
        assert!(e.accept_visitor(&mut IsAggregateFunction, &WRITER, &mut ctx, &mut out));
        let e = expr!(ABS(Table::col_a));
        assert!(e.accept_visitor(&mut IsAggregateFunction, &WRITER, &mut ctx, &mut out));
        // Non-aggregate
        let e = expr!(Table::col_a);
        assert!(!e.accept_visitor(&mut IsAggregateFunction, &WRITER, &mut ctx, &mut out));
        let e = expr!(42);
        assert!(!e.accept_visitor(&mut IsAggregateFunction, &WRITER, &mut ctx, &mut out));
    }

    #[test]
    fn visitor_is_asterisk() {
        let mut out = Default::default();
        let mut ctx = Default::default();
        let e = expr!(*);
        assert!(e.accept_visitor(&mut IsAsterisk, &WRITER, &mut ctx, &mut out));
        let e = expr!(42);
        assert!(!e.accept_visitor(&mut IsAsterisk, &WRITER, &mut ctx, &mut out));
    }

    #[test]
    fn visitor_is_question_mark() {
        let mut out = Default::default();
        let mut ctx = Default::default();
        let e = expr!(?);
        assert!(e.accept_visitor(&mut IsQuestionMark, &WRITER, &mut ctx, &mut out));
        let e = expr!(42);
        assert!(!e.accept_visitor(&mut IsQuestionMark, &WRITER, &mut ctx, &mut out));
    }

    #[test]
    fn visitor_is_alias() {
        let mut out = Default::default();
        let mut ctx = Default::default();
        let mut alias_visitor = IsAlias::default();
        let e = expr!(Table::col_a as my_alias);
        assert!(e.accept_visitor(&mut alias_visitor, &WRITER, &mut ctx, &mut out));
        // Non-alias
        let mut alias_visitor2 = IsAlias::default();
        let e = expr!(Table::col_a);
        assert!(!e.accept_visitor(&mut alias_visitor2, &WRITER, &mut ctx, &mut out));
    }

    #[test]
    fn visitor_find_order() {
        use tank::cols;
        let mut out = Default::default();
        let mut ctx = Default::default();
        {
            let mut finder = FindOrder::default();
            let binding = cols!(Table::col_a ASC);
            let ordered = &binding[0];
            assert!(ordered.accept_visitor(&mut finder, &WRITER, &mut ctx, &mut out));
            assert_eq!(finder.order, Order::ASC);
        }
        {
            let mut finder = FindOrder::default();
            let binding = cols!(Table::col_b DESC);
            let ordered = &binding[0];
            assert!(ordered.accept_visitor(&mut finder, &WRITER, &mut ctx, &mut out));
            assert_eq!(finder.order, Order::DESC);
        }
    }
}
