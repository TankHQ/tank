#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tank::{
        Entity, Expression, ExpressionVisitor, FindOrder, GenericSqlWriter, IsAggregateFunction,
        IsAlias, IsAsterisk, IsConstant, IsFalse, IsQuestionMark, IsTrue, Order, cols, expr,
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
        let error = expr!(true);
        assert!(error.accept_visitor(&mut IsTrue, &WRITER, &mut ctx, &mut out));
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

        // Non-aggregate
        let e = expr!(ABS(Table::col_a));
        assert!(!e.accept_visitor(&mut IsAggregateFunction, &WRITER, &mut ctx, &mut out));
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

    #[test]
    fn visitor_default_impls_return_false() {
        struct Defaults;
        impl ExpressionVisitor for Defaults {}
        let mut visitor = Defaults;
        let mut out = Default::default();
        let mut ctx = Default::default();
        assert!(!expr!(Table::col_a).accept_visitor(&mut visitor, &WRITER, &mut ctx, &mut out));
        assert!(!expr!(1 + 2).accept_visitor(&mut visitor, &WRITER, &mut ctx, &mut out));
        assert!(!expr!(!true).accept_visitor(&mut visitor, &WRITER, &mut ctx, &mut out));
        let binding = cols!(Table::col_a ASC);
        assert!(!binding[0].accept_visitor(&mut visitor, &WRITER, &mut ctx, &mut out));
    }

    #[test]
    fn visitor_delegates_through_wrappers() {
        let mut out = Default::default();
        let mut ctx = Default::default();
        let arc_true = Arc::new(expr!(true));
        assert!(arc_true.accept_visitor(&mut IsTrue, &WRITER, &mut ctx, &mut out));
        let boxed_true = Box::new(expr!(true));
        assert!(boxed_true.accept_visitor(&mut IsTrue, &WRITER, &mut ctx, &mut out));
        let inner = expr!(true);
        let dyn_ref: &dyn Expression = &inner;
        assert!(dyn_ref.accept_visitor(&mut IsTrue, &WRITER, &mut ctx, &mut out));
        let boxed_arc = Arc::new(expr!(false));
        assert!(boxed_arc.accept_visitor(&mut IsFalse, &WRITER, &mut ctx, &mut out));
    }

    #[test]
    fn visitor_is_constant_lists_and_alias() {
        let mut out = Default::default();
        let mut ctx = Default::default();
        assert!(expr!([1, 2, 3]).accept_visitor(&mut IsConstant, &WRITER, &mut ctx, &mut out));
        assert!(expr!((1, 2)).accept_visitor(&mut IsConstant, &WRITER, &mut ctx, &mut out));
        assert!(!expr!([alpha, bravo]).accept_visitor(
            &mut IsConstant,
            &WRITER,
            &mut ctx,
            &mut out
        ));

        assert!(expr!(true as total).accept_visitor(&mut IsConstant, &WRITER, &mut ctx, &mut out));
        assert!(expr!(COUNT(*) as n).accept_visitor(
            &mut IsAggregateFunction,
            &WRITER,
            &mut ctx,
            &mut out
        ));
    }
}
