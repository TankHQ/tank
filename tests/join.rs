#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use tank::{
        DynQuery, BinaryOp, BinaryOpType, ColumnRef, DataSet, DeclareTableRef, Entity, Join,
        JoinType, Operand, SqlWriter, TableRef, join,
    };

    struct Writer;
    impl SqlWriter for Writer {
        fn as_dyn(&self) -> &dyn SqlWriter {
            self
        }
    }

    const WRITER: Writer = Writer {};

    #[derive(Entity)]
    #[tank(schema = "my_data")]
    struct Alpha {
        _a: u32,
        _b: String,
    }
    #[derive(Entity)]
    struct Bravo {
        _first: u32,
        _second: String,
    }

    #[test]
    fn join_simple() {
        let join = join!(Alpha AA JOIN crate::tests::Bravo BB ON AA.a == BB.first);
        assert!(matches!(
            join,
            Join {
                join: JoinType::Default,
                lhs: DeclareTableRef(TableRef {
                    name: Cow::Borrowed("alpha"),
                    schema: Cow::Borrowed("my_data"),
                    alias: Cow::Borrowed("AA"),
                    ..
                }),
                rhs: DeclareTableRef(TableRef {
                    name: Cow::Borrowed("bravo"),
                    schema: Cow::Borrowed(""),
                    alias: Cow::Borrowed("BB"),
                    ..
                }),
                on: Some(BinaryOp {
                    op: BinaryOpType::Equal,
                    lhs: Operand::LitField(["AA", "a"]),
                    rhs: Operand::LitField(["BB", "first"]),
                    ..
                }),
                ..
            }
        ));
        let mut query = DynQuery::default();
        join.write_query(&WRITER, &mut Default::default(), &mut query);
        assert_eq!(
            query.as_str(),
            r#""my_data"."alpha" AA JOIN "bravo" BB ON AA.a = BB.first"#
        );

        let join = join!(Alpha INNER JOIN Bravo ON Alpha::_a == Bravo::_first);
        assert!(matches!(
            join,
            Join {
                join: JoinType::Inner,
                ..
            }
        ));
        let mut query = DynQuery::default();
        join.write_query(&WRITER, &mut Default::default(), &mut query);
        assert_eq!(
            query.as_str(),
            r#""my_data"."alpha" INNER JOIN "bravo" ON "my_data"."alpha"."a" = "bravo"."first""#
        );

        let join = join!(Alpha FULL OUTER JOIN Bravo ON Alpha::_a == Bravo::_first);
        assert!(matches!(
            join,
            Join {
                join: JoinType::Outer,
                ..
            }
        ));
        let mut query = DynQuery::default();
        join.write_query(&WRITER, &mut Default::default(), &mut query);
        assert_eq!(
            query.as_str(),
            r#""my_data"."alpha" OUTER JOIN "bravo" ON "my_data"."alpha"."a" = "bravo"."first""#
        );

        let join = join!(Alpha OUTER JOIN Bravo ON Alpha::_a == Bravo::_first);
        assert!(matches!(
            join,
            Join {
                join: JoinType::Outer,
                ..
            }
        ));
        let mut query = DynQuery::default();
        join.write_query(&WRITER, &mut Default::default(), &mut query);
        assert_eq!(
            query.as_str(),
            r#""my_data"."alpha" OUTER JOIN "bravo" ON "my_data"."alpha"."a" = "bravo"."first""#
        );

        let join = join!(Alpha LEFT OUTER JOIN Bravo ON Alpha::_a == Bravo::_first);
        assert!(matches!(
            join,
            Join {
                join: JoinType::Left,
                ..
            }
        ));
        let mut query = DynQuery::default();
        join.write_query(&WRITER, &mut Default::default(), &mut query);
        assert_eq!(
            query.as_str(),
            r#""my_data"."alpha" LEFT JOIN "bravo" ON "my_data"."alpha"."a" = "bravo"."first""#
        );

        let join = join!(Alpha LEFT JOIN Bravo ON Alpha::_a == Bravo::_first);
        assert!(matches!(
            join,
            Join {
                join: JoinType::Left,
                ..
            }
        ));
        let mut query = DynQuery::default();
        join.write_query(&WRITER, &mut Default::default(), &mut query);
        assert_eq!(
            query.as_str(),
            r#""my_data"."alpha" LEFT JOIN "bravo" ON "my_data"."alpha"."a" = "bravo"."first""#
        );

        let join = join!(Alpha RIGHT OUTER JOIN Bravo ON Alpha::_a == Bravo::_first);
        assert!(matches!(
            join,
            Join {
                join: JoinType::Right,
                ..
            }
        ));
        let mut query = DynQuery::default();
        join.write_query(&WRITER, &mut Default::default(), &mut query);
        assert_eq!(
            query.as_str(),
            r#""my_data"."alpha" RIGHT JOIN "bravo" ON "my_data"."alpha"."a" = "bravo"."first""#
        );

        let join = join!(Alpha RIGHT JOIN Bravo ON Alpha::_a == Bravo::_first);
        assert!(matches!(
            join,
            Join {
                join: JoinType::Right,
                ..
            }
        ));
        let mut query = DynQuery::default();
        join.write_query(&WRITER, &mut Default::default(), &mut query);
        assert_eq!(
            query.as_str(),
            r#""my_data"."alpha" RIGHT JOIN "bravo" ON "my_data"."alpha"."a" = "bravo"."first""#
        );

        let join = join!(Alpha CROSS Bravo);
        assert!(matches!(
            join,
            Join {
                join: JoinType::Cross,
                on: None,
                ..
            },
        ));
        let mut query = DynQuery::default();
        join.write_query(&WRITER, &mut Default::default(), &mut query);
        assert_eq!(query.as_str(), r#""my_data"."alpha" CROSS "bravo""#);

        let join = join!(Alpha NATURAL JOIN Bravo);
        assert!(matches!(
            join,
            Join {
                join: JoinType::Natural,
                on: None,
                ..
            },
        ));
        let mut query = DynQuery::default();
        join.write_query(&WRITER, &mut Default::default(), &mut query);
        assert_eq!(query.as_str(), r#""my_data"."alpha" NATURAL JOIN "bravo""#);
    }

    #[test]
    fn join_left_nested() {
        #[derive(Entity)]
        #[tank(name = "another_table")]
        struct Charlie {
            _column: u128,
        }

        let join = join!((Charlie JOIN Alpha ON Charlie::_column < Alpha::_b) JOIN Bravo ON Alpha::_a == Bravo::_second);
        assert!(matches!(
            join,
            Join {
                join: JoinType::Default,
                lhs: Join {
                    join: JoinType::Default,
                    lhs: TableRef {
                        name: Cow::Borrowed("another_table"),
                        ..
                    },
                    rhs: TableRef {
                        name: Cow::Borrowed("alpha"),
                        ..
                    },
                    on: Some(BinaryOp {
                        op: BinaryOpType::Less,
                        lhs: ColumnRef {
                            name: "column",
                            table: "another_table",
                            ..
                        },
                        rhs: ColumnRef {
                            name: "b",
                            table: "alpha",
                            ..
                        },
                        ..
                    }),
                    ..
                },
                rhs: TableRef {
                    name: Cow::Borrowed("bravo"),
                    ..
                },
                on: Some(BinaryOp {
                    op: BinaryOpType::Equal,
                    lhs: ColumnRef { name: "a", .. },
                    rhs: ColumnRef { name: "second", .. },
                    ..
                }),
                ..
            }
        ));
        let mut query = DynQuery::default();
        join.write_query(&WRITER, &mut Default::default(), &mut query);
        assert_eq!(
            query.as_str(),
            r#""another_table" JOIN "my_data"."alpha" ON "another_table"."column" < "my_data"."alpha"."b" JOIN "bravo" ON "my_data"."alpha"."a" = "bravo"."second""#
        );
    }

    #[test]
    fn join_right_nested() {
        #[derive(Entity)]
        #[tank(schema = "delta_dataset", name = "delta_table")]
        struct Delta {
            _time_column: time::Date,
            #[tank(name = "the_string")]
            _string_column: Option<String>,
        }

        let join = join!(
            Bravo OUTER JOIN (
                Delta LEFT JOIN Alpha ON Delta::_string_column < Alpha::_b
            ) ON Bravo::_second == Delta::_string_column
        );
        assert!(matches!(
            join,
            Join {
                join: JoinType::Outer,
                lhs: TableRef {
                    name: Cow::Borrowed("bravo"),
                    schema: Cow::Borrowed(""),
                    ..
                },
                rhs: Join {
                    join: JoinType::Left,
                    lhs: TableRef {
                        name: Cow::Borrowed("delta_table"),
                        schema: Cow::Borrowed("delta_dataset"),
                        ..
                    },
                    rhs: TableRef {
                        name: Cow::Borrowed("alpha"),
                        schema: Cow::Borrowed("my_data"),
                        ..
                    },
                    on: Some(BinaryOp {
                        op: BinaryOpType::Less,
                        lhs: ColumnRef {
                            name: "the_string",
                            table: "delta_table",
                            schema: "delta_dataset",
                            ..
                        },
                        rhs: ColumnRef {
                            name: "b",
                            table: "alpha",
                            schema: "my_data",
                            ..
                        },
                        ..
                    }),
                    ..
                },
                on: Some(BinaryOp {
                    op: BinaryOpType::Equal,
                    lhs: ColumnRef {
                        name: "second",
                        table: "bravo",
                        ..
                    },
                    rhs: ColumnRef {
                        name: "the_string",
                        table: "delta_table",
                        ..
                    },
                    ..
                }),
                ..
            }
        ));
        let mut query = DynQuery::default();
        join.write_query(&WRITER, &mut Default::default(), &mut query);
        assert_eq!(
            query.as_str(),
            r#""bravo" OUTER JOIN "delta_dataset"."delta_table" LEFT JOIN "my_data"."alpha" ON "delta_dataset"."delta_table"."the_string" < "my_data"."alpha"."b" ON "bravo"."second" = "delta_dataset"."delta_table"."the_string""#
        );
    }

    #[test]
    fn join_chained() {
        #[derive(Entity)]
        struct Some {
            col: Box<i64>,
        }
        let join = join!(Alpha A FULL OUTER JOIN Bravo ON Alpha::_b >= Bravo::_second RIGHT JOIN Some ON Some::col == Bravo::_first);
        assert!(matches!(
            join,
            Join {
                join: JoinType::Right,
                lhs: Join {
                    join: JoinType::Outer,
                    lhs: DeclareTableRef(TableRef {
                        name: Cow::Borrowed("alpha"),
                        schema: Cow::Borrowed("my_data"),
                        alias: Cow::Borrowed("A"),
                        ..
                    }),
                    rhs: TableRef {
                        name: Cow::Borrowed("bravo"),
                        // alias: Cow::Borrowed(""),
                        ..
                    },
                    on: Some(BinaryOp {
                        op: BinaryOpType::GreaterEqual,
                        lhs: ColumnRef {
                            name: "b",
                            table: "alpha",
                            schema: "my_data",
                            ..
                        },
                        rhs: ColumnRef {
                            name: "second",
                            table: "bravo",
                            ..
                        },
                        ..
                    }),
                    ..
                },
                rhs: TableRef {
                    name: Cow::Borrowed("some"),
                    ..
                },
                on: Some(BinaryOp {
                    op: BinaryOpType::Equal,
                    lhs: ColumnRef {
                        name: "col",
                        table: "some",
                        ..
                    },
                    rhs: ColumnRef {
                        name: "first",
                        table: "bravo",
                        ..
                    },
                    ..
                }),
                ..
            }
        ));
        let mut query = DynQuery::default();
        join.write_query(&WRITER, &mut Default::default(), &mut query);
        assert_eq!(
            query.as_str(),
            r#""my_data"."alpha" A OUTER JOIN "bravo" ON "my_data"."alpha"."b" >= "bravo"."second" RIGHT JOIN "some" ON "some"."col" = "bravo"."first""#
        );
    }

    #[test]
    fn join_multi_chained() {
        #[derive(Entity)]
        #[tank(name = "ccc")]
        struct Charlie;

        #[derive(Entity)]
        struct Delta;

        let join = join!(
            Alpha NATURAL JOIN Charlie
                CROSS Bravo
                    LEFT JOIN Bravo ON Bravo::_second == Alpha::_b
                        CROSS Delta
        );
        assert!(matches!(
            join,
            Join {
                join: JoinType::Cross,
                lhs: Join {
                    join: JoinType::Left,
                    lhs: Join {
                        join: JoinType::Cross,
                        lhs: Join {
                            join: JoinType::Natural,
                            lhs: TableRef {
                                name: Cow::Borrowed("alpha"),
                                ..
                            },
                            rhs: TableRef {
                                name: Cow::Borrowed("ccc"),
                                ..
                            },
                            on: None,
                            ..
                        },
                        ..
                    },
                    rhs: TableRef {
                        name: Cow::Borrowed("bravo"),
                        ..
                    },
                    on: Some(BinaryOp {
                        op: BinaryOpType::Equal,
                        lhs: ColumnRef {
                            name: "second",
                            table: "bravo",
                            ..
                        },
                        rhs: ColumnRef {
                            name: "b",
                            table: "alpha",
                            schema: "my_data",
                            ..
                        },
                        ..
                    }),
                    ..
                },
                rhs: TableRef {
                    name: Cow::Borrowed("delta"),
                    ..
                },
                on: None,
                ..
            }
        ));
        let mut query = DynQuery::default();
        join.write_query(&WRITER, &mut Default::default(), &mut query);
        assert_eq!(
            query.as_str(),
            r#""my_data"."alpha" NATURAL JOIN "ccc" CROSS "bravo" LEFT JOIN "bravo" ON "bravo"."second" = "my_data"."alpha"."b" CROSS "delta""#
        );
    }

    #[test]
    fn join_with_many_parentheses() {
        let join = join!(
            ((((((Alpha RIGHT JOIN Bravo ON (((((((((((((Alpha::_a)) <= (((((((Bravo::_first))))))))))))))))))))))))
        );
        assert!(matches!(
            join,
            Join {
                join: JoinType::Right,
                lhs: TableRef {
                    name: Cow::Borrowed("alpha"),
                    schema: Cow::Borrowed("my_data"),
                    ..
                },
                rhs: TableRef {
                    name: Cow::Borrowed("bravo"),
                    schema: Cow::Borrowed(""),
                    ..
                },
                on: Some(BinaryOp {
                    op: BinaryOpType::LessEqual,
                    lhs: ColumnRef {
                        name: "a",
                        table: "alpha",
                        schema: "my_data",
                        ..
                    },
                    rhs: ColumnRef {
                        name: "first",
                        table: "bravo",
                        schema: "",
                        ..
                    },
                    ..
                }),
                ..
            }
        ));
        let mut query = DynQuery::default();
        join.write_query(&WRITER, &mut Default::default(), &mut query);
        assert_eq!(
            query.as_str(),
            r#""my_data"."alpha" RIGHT JOIN "bravo" ON "my_data"."alpha"."a" <= "bravo"."first""#
        );
    }
}
