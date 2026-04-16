#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use tank::{
        BinaryOp, BinaryOpType, ColumnRef, Context, DynQuery, Entity, Expression, Fragment,
        OpPrecedence, Operand, SqlWriter, UnaryOp, UnaryOpType, Value, expr,
    };

    struct Writer;
    impl SqlWriter for Writer {
        fn as_dyn(&self) -> &dyn SqlWriter {
            self
        }
    }

    const WRITER: Writer = Writer {};

    #[test]
    fn test_simple_expressions() {
        let expr = expr!();
        assert!(matches!(expr, Operand::LitBool(false)));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "false");

        let expr = expr!(1 + 2);
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::Addition,
                lhs: Operand::LitInt(1),
                rhs: Operand::LitInt(2),
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "1 + 2");

        let expr = expr!(5 * 1.2);
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::Multiplication,
                lhs: Operand::LitInt(5),
                rhs: Operand::LitFloat(1.2),
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "5 * 1.2");

        let expr = expr!(true && false);
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::And,
                lhs: Operand::LitBool(true),
                rhs: Operand::LitBool(false),
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "true AND false");

        let expr = expr!(45 | -90);
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::BitwiseOr,
                lhs: Operand::LitInt(45),
                rhs: UnaryOp {
                    op: UnaryOpType::Negative,
                    arg: Operand::LitInt(90),
                },
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "45 | -90");

        let expr = expr!(CAST(true as i32));
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::Cast,
                lhs: Operand::LitBool(true),
                rhs: Operand::Type(Value::Int32(..)),
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "CAST(true AS INTEGER)");

        let expr = expr!(CAST("1.5" as f64));
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::Cast,
                lhs: Operand::LitStr("1.5"),
                rhs: Operand::Type(Value::Float64(..)),
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "CAST('1.5' AS DOUBLE)");

        let expr = expr!(["a", "b", "c"]);
        assert!(matches!(
            expr,
            Operand::LitList([
                Operand::LitStr("a"),
                Operand::LitStr("b"),
                Operand::LitStr("c"),
            ])
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "['a','b','c']");

        let expr = expr!([11, 22, 33][1]);
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::Indexing,
                lhs: Operand::LitList([
                    Operand::LitInt(11),
                    Operand::LitInt(22),
                    Operand::LitInt(33),
                ]),
                rhs: Operand::LitInt(1),
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "[11,22,33][1]");

        let expr = expr!("hello" == "hell_" as LIKE);
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::Like,
                lhs: Operand::LitStr("hello"),
                rhs: Operand::LitStr("hell_"),
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "'hello' LIKE 'hell_'");

        let expr = expr!("abc" != "A%" as LIKE);
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::NotLike,
                lhs: Operand::LitStr("abc"),
                rhs: Operand::LitStr("A%"),
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "'abc' NOT LIKE 'A%'");

        let expr = expr!("log.txt" != "src/**/log.{txt,csv}" as GLOB);
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::NotGlob,
                lhs: Operand::LitStr("log.txt"),
                rhs: Operand::LitStr("src/**/log.{txt,csv}"),
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "'log.txt' NOT GLOB 'src/**/log.{txt,csv}'");

        let expr = expr!(CAST(true as i32));
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::Cast,
                lhs: Operand::LitBool(true),
                rhs: Operand::Type(Value::Int32(..))
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "CAST(true AS INTEGER)");

        let expr = expr!("value" != NULL);
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::IsNot,
                lhs: Operand::LitStr("value"),
                rhs: Operand::Null,
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "'value' IS NOT NULL");
    }

    #[test]
    fn test_asterisk_expressions() {
        let expr = expr!(COUNT(*));
        assert!(matches!(expr, Operand::Call("COUNT", _)));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "COUNT(*)");

        #[derive(Entity)]
        struct ATable {
            #[tank(name = "my_column")]
            a_column: u8,
        }
        let expr = expr!(SUM(ATable::a_column));
        assert!(matches!(expr, Operand::Call("SUM", _)));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), r#"SUM("my_column")"#);
    }

    #[test]
    fn test_question_mark_expressions() {
        let expr = expr!(alpha == ? && bravo > ?);
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::And,
                lhs: BinaryOp {
                    op: BinaryOpType::Equal,
                    lhs: Operand::LitIdent("alpha"),
                    rhs: Operand::QuestionMark,
                },
                rhs: BinaryOp {
                    op: BinaryOpType::Greater,
                    lhs: Operand::LitIdent("bravo"),
                    rhs: Operand::QuestionMark,
                },
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(&WRITER, &mut Context::qualify(true), &mut query);
        assert_eq!(query.as_str(), "alpha = ? AND bravo > ?");

        #[derive(Entity)]
        struct SomeTable {
            #[tank(name = "the_column")]
            some_column: Cow<'static, str>,
        }
        let expr = expr!(SomeTable::some_column != ? as LIKE);
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::NotLike,
                lhs: ColumnRef {
                    name: Cow::Borrowed("the_column"),
                    table: Cow::Borrowed("some_table"),
                    schema: Cow::Borrowed(""),
                },
                rhs: Operand::QuestionMark,
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(&WRITER, &mut Context::qualify(true), &mut query);
        assert_eq!(query.as_str(), r#""some_table"."the_column" NOT LIKE ?"#);
    }

    #[test]
    fn test_complex_expressions() {
        let expr = expr!(90.5 - -0.54 * 2 < 7 / 2);
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::Less,
                lhs: BinaryOp {
                    op: BinaryOpType::Subtraction,
                    lhs: Operand::LitFloat(90.5),
                    rhs: BinaryOp {
                        op: BinaryOpType::Multiplication,
                        lhs: UnaryOp {
                            op: UnaryOpType::Negative,
                            arg: Operand::LitFloat(0.54),
                        },
                        rhs: Operand::LitInt(2),
                    },
                },
                rhs: BinaryOp {
                    op: BinaryOpType::Division,
                    lhs: Operand::LitInt(7),
                    rhs: Operand::LitInt(2),
                },
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "90.5 - -0.54 * 2 < 7 / 2");

        let expr = expr!((2 + 3) * (4 - 1) >> 1 & (8 | 3));
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::BitwiseAnd,
                lhs: BinaryOp {
                    op: BinaryOpType::ShiftRight,
                    lhs: BinaryOp {
                        op: BinaryOpType::Multiplication,
                        lhs: BinaryOp {
                            op: BinaryOpType::Addition,
                            lhs: Operand::LitInt(2),
                            rhs: Operand::LitInt(3),
                        },
                        rhs: BinaryOp {
                            op: BinaryOpType::Subtraction,
                            lhs: Operand::LitInt(4),
                            rhs: Operand::LitInt(1),
                        },
                    },
                    rhs: Operand::LitInt(1),
                },
                rhs: BinaryOp {
                    op: BinaryOpType::BitwiseOr,
                    lhs: Operand::LitInt(8),
                    rhs: Operand::LitInt(3),
                },
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "(2 + 3) * (4 - 1) >> 1 & (8 | 3)");

        let expr = expr!(-(-PI) + 2 * (5 % (2 + 1)) == 7 && !(4 < 2));
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::And,
                lhs: BinaryOp {
                    op: BinaryOpType::Equal,
                    lhs: BinaryOp {
                        op: BinaryOpType::Addition,
                        lhs: UnaryOp {
                            op: UnaryOpType::Negative,
                            arg: UnaryOp {
                                op: UnaryOpType::Negative,
                                arg: Operand::LitIdent("PI"),
                            },
                        },
                        rhs: BinaryOp {
                            op: BinaryOpType::Multiplication,
                            lhs: Operand::LitInt(2),
                            rhs: BinaryOp {
                                op: BinaryOpType::Remainder,
                                lhs: Operand::LitInt(5),
                                rhs: BinaryOp {
                                    op: BinaryOpType::Addition,
                                    lhs: Operand::LitInt(2),
                                    rhs: Operand::LitInt(1),
                                },
                            },
                        },
                    },
                    rhs: Operand::LitInt(7),
                },
                rhs: UnaryOp {
                    op: UnaryOpType::Not,
                    arg: BinaryOp {
                        op: BinaryOpType::Less,
                        lhs: Operand::LitInt(4),
                        rhs: Operand::LitInt(2),
                    },
                },
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(
            query.as_str(),
            "-(-PI) + 2 * (5 % (2 + 1)) = 7 AND NOT 4 < 2"
        );
    }

    #[test]
    fn test_variables_expressions() {
        let one = 1;
        let three = 3;
        let expr = expr!(#one + 2 == #three);
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::Equal,
                lhs: BinaryOp {
                    op: BinaryOpType::Addition,
                    lhs: Operand::Variable(Value::Int32(Some(1))),
                    rhs: Operand::LitInt(2),
                },
                rhs: Operand::Variable(Value::Int32(Some(3))),
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "1 + 2 = 3");

        let vec = vec![-1, -2, -3, -4];
        let index = 2;
        let expr = expr!(#vec[#index + 1] + 60);
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::Addition,
                lhs: BinaryOp {
                    op: BinaryOpType::Indexing,
                    lhs: Operand::Variable(Value::List(Some(ref vec), ..)),
                    rhs: BinaryOp {
                        op: BinaryOpType::Addition,
                        lhs: Operand::Variable(Value::Int32(Some(2))),
                        rhs: Operand::LitInt(1),
                    },
                },
                rhs: Operand::LitInt(60),
            } if vec.as_slice() == &[
                Value::Int32(Some(-1)),
                Value::Int32(Some(-2)),
                Value::Int32(Some(-3)),
                Value::Int32(Some(-4)),
            ]
        ));
        let mut query = DynQuery::default();
        expr.write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut query,
        );
        assert_eq!(query.as_str(), "[-1,-2,-3,-4][2 + 1] + 60");
    }

    #[test]
    fn test_columns_expressions() {
        #[derive(Entity)]
        #[tank(name = "the_table")]
        struct MyEntity {
            _first: i128,
            _second: String,
            _third: Vec<f64>,
        }
        assert!(MyEntity::columns()[0].precedence(&WRITER) > 0); // For coverage purpose

        let expr = expr!(MyEntity::_first + 2);
        {
            let mut query = DynQuery::default();
            expr.write_query(
                &WRITER,
                &mut Context::new(Fragment::SqlSelect, false),
                &mut query,
            );
            assert_eq!(query.as_str(), r#""first" + 2"#);
        }
        {
            let mut query = DynQuery::default();
            expr.write_query(&WRITER, &mut Context::qualify(true), &mut query);
            assert_eq!(query.as_str(), r#""the_table"."first" + 2"#);
        }
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::Addition,
                lhs: ColumnRef {
                    name: Cow::Borrowed("first"),
                    table: Cow::Borrowed("the_table"),
                    schema: Cow::Borrowed(""),
                },
                rhs: Operand::LitInt(2),
            }
        ));

        let expr = expr!(MyEntity::_first != NULL);
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::IsNot,
                lhs: ColumnRef {
                    name: Cow::Borrowed("first"),
                    table: Cow::Borrowed("the_table"),
                    schema: Cow::Borrowed(""),
                },
                rhs: Operand::Null,
            }
        ));

        let expr =
            expr!(CAST(MyEntity::_first as String) == MyEntity::_second && MyEntity::_first > 0);
        assert!(matches!(
            expr,
            BinaryOp {
                op: BinaryOpType::And,
                lhs: BinaryOp {
                    op: BinaryOpType::Equal,
                    lhs: BinaryOp {
                        op: BinaryOpType::Cast,
                        lhs: ColumnRef {
                            name: Cow::Borrowed("first"),
                            table: Cow::Borrowed("the_table"),
                            schema: Cow::Borrowed(""),
                        },
                        rhs: Operand::Type(Value::Varchar(None)),
                    },
                    rhs: ColumnRef {
                        name: Cow::Borrowed("second"),
                        table: Cow::Borrowed("the_table"),
                        schema: Cow::Borrowed(""),
                    },
                },
                rhs: BinaryOp {
                    op: BinaryOpType::Greater,
                    lhs: ColumnRef {
                        name: Cow::Borrowed("first"),
                        table: Cow::Borrowed("the_table"),
                        schema: Cow::Borrowed(""),
                    },
                    rhs: Operand::LitInt(0),
                },
            }
        ));
        let mut query = DynQuery::default();
        expr.write_query(&WRITER, &mut Context::qualify(true), &mut query);
        assert_eq!(
            query.as_str(),
            r#"CAST("the_table"."first" AS VARCHAR) = "the_table"."second" AND "the_table"."first" > 0"#
        );
    }

    #[test]
    fn test_op_precedence() {
        assert!(().precedence(&WRITER) > 1000);
        assert_eq!(true.precedence(&WRITER), 0);
        assert_eq!("hello".precedence(&WRITER), 0);
        assert_eq!(Value::Int32(Some(1)).precedence(&WRITER), 0);
        assert!((&Operand::Asterisk).precedence(&WRITER) > 1000);
        assert!(
            BinaryOpType::Multiplication.precedence(&WRITER)
                > BinaryOpType::Addition.precedence(&WRITER)
        );
        assert!(
            BinaryOpType::Multiplication.precedence(&WRITER)
                > BinaryOpType::Subtraction.precedence(&WRITER)
        );
        assert!(
            BinaryOpType::Division.precedence(&WRITER) > BinaryOpType::Addition.precedence(&WRITER)
        );
        assert!(
            BinaryOpType::Division.precedence(&WRITER)
                > BinaryOpType::Subtraction.precedence(&WRITER)
        );
        assert!(BinaryOpType::And.precedence(&WRITER) > BinaryOpType::Or.precedence(&WRITER));
    }

    #[test]
    fn test_operand_eq() {
        assert_eq!(Operand::LitBool(true), Operand::LitBool(true));
        assert_ne!(Operand::LitBool(true), Operand::LitBool(false));
        assert_eq!(Operand::LitFloat(1.5), Operand::LitFloat(1.5));
        assert_eq!(Operand::LitIdent("a"), Operand::LitIdent("a"));
        assert_ne!(Operand::LitIdent("a"), Operand::LitIdent("b"));
        assert_eq!(
            Operand::LitField(&["a", "b"]),
            Operand::LitField(&["a", "b"])
        );
        assert_eq!(Operand::LitInt(42), Operand::LitInt(42));
        assert_eq!(Operand::LitStr("x"), Operand::LitStr("x"));
        assert_eq!(Operand::Asterisk, Operand::Asterisk);
        assert_eq!(Operand::QuestionMark, Operand::QuestionMark);
        assert_eq!(Operand::CurrentTimestampMs, Operand::CurrentTimestampMs);
        assert_eq!(
            Operand::Type(Value::Int32(None)),
            Operand::Type(Value::Int32(None))
        );
        assert_eq!(
            Operand::Variable(Value::Int32(Some(1))),
            Operand::Variable(Value::Int32(Some(1)))
        );
        assert_ne!(Operand::Asterisk, Operand::QuestionMark);
        assert_ne!(Operand::LitBool(true), Operand::LitBool(false));
        assert_ne!(Operand::LitBool(true), Operand::LitInt(1));
        assert_ne!(Operand::LitInt(0), Operand::LitInt(1));
    }

    #[test]
    fn test_expression_unit_impl() {
        let mut out = DynQuery::default();
        let mut ctx = Context::new(Fragment::SqlSelect, false);
        ().write_query(&WRITER, &mut ctx, &mut out);
        assert_eq!(out.as_str(), "");
    }

    #[test]
    fn test_expression_bool_impl() {
        let mut out = DynQuery::default();
        let mut ctx = Context::new(Fragment::SqlSelect, false);
        true.write_query(&WRITER, &mut ctx, &mut out);
        assert_eq!(out.as_str(), "true");
    }

    #[test]
    fn test_expression_str_impl() {
        let mut out = DynQuery::default();
        let mut ctx = Context::new(Fragment::SqlSelect, false);
        "hello".write_query(&WRITER, &mut ctx, &mut out);
        assert_eq!(out.as_str(), "'hello'");
    }

    #[test]
    fn test_expression_value_impl() {
        let mut out = DynQuery::default();
        let mut ctx = Context::new(Fragment::SqlSelect, false);
        Value::Int32(Some(42)).write_query(&WRITER, &mut ctx, &mut out);
        assert_eq!(out.as_str(), "42");
    }

    #[test]
    fn test_expression_as_identifier() {
        let mut ctx = Context::new(Fragment::SqlSelect, false);
        let ident = Operand::LitIdent("my_col").as_identifier(&mut ctx);
        assert_eq!(ident, "my_col");
    }

    #[test]
    fn test_expression_ref_delegation() {
        let op = Operand::LitInt(99);
        let mut out = DynQuery::default();
        let mut ctx = Context::new(Fragment::SqlSelect, false);
        // &T delegates to T
        (&op).write_query(&WRITER, &mut ctx, &mut out);
        assert_eq!(out.as_str(), "99");
        let ident = (&op).as_identifier(&mut ctx);
        assert_eq!(ident, "99");
    }

    #[test]
    fn test_default_value_type() {
        use tank::DefaultValueType;
        // None
        let dvt = DefaultValueType::None;
        assert!(!dvt.is_set());
        let mut out = DynQuery::default();
        let mut ctx = Context::new(Fragment::SqlSelect, false);
        dvt.write_query(&WRITER, &mut ctx, &mut out);
        assert_eq!(out.as_str(), "");
        assert_eq!(dvt.precedence(&WRITER), 0);

        // From Value
        let dvt: DefaultValueType = Value::Int64(Some(10)).into();
        assert!(dvt.is_set());
        let mut out = DynQuery::default();
        dvt.write_query(&WRITER, &mut ctx, &mut out);
        assert_eq!(out.as_str(), "10");
        assert_eq!(dvt.precedence(&WRITER), 0);

        // From bool
        let dvt: DefaultValueType = true.into();
        assert!(dvt.is_set());

        // From &str
        let dvt: DefaultValueType = "default_val".into();
        assert!(dvt.is_set());

        // From i64
        let dvt: DefaultValueType = 42i64.into();
        assert!(dvt.is_set());
    }

    #[test]
    fn test_unary_op_expression() {
        let expr = expr!(-42);
        let mut out = DynQuery::default();
        let mut ctx = Context::new(Fragment::SqlSelect, false);
        expr.write_query(&WRITER, &mut ctx, &mut out);
        assert_eq!(out.as_str(), "-42");
        // precedence
        let _p = expr.precedence(&WRITER);

        let expr = expr!(!true);
        let mut out = DynQuery::default();
        expr.write_query(&WRITER, &mut ctx, &mut out);
        assert_eq!(out.as_str(), "NOT true");
    }

    #[test]
    fn test_binary_op_as_identifier_alias() {
        // Alias uses rhs as identifier
        let expr = expr!(my_col as my_alias);
        let mut ctx = Context::new(Fragment::SqlSelect, false);
        let ident = expr.as_identifier(&mut ctx);
        assert_eq!(ident, "my_alias");
    }

    #[test]
    fn test_binary_op_as_identifier_non_alias() {
        // Non-alias renders the full expression
        let expr = expr!(1 + 2);
        let mut ctx = Context::new(Fragment::SqlSelect, false);
        let ident = expr.as_identifier(&mut ctx);
        assert_eq!(ident, "1 + 2");
    }

    #[test]
    fn test_ordered_expression() {
        use tank::{Order, Ordered};
        let ordered = Ordered {
            expression: Operand::LitInt(1),
            order: Order::ASC,
        };
        let mut out = DynQuery::default();
        let mut ctx = Context::new(Fragment::SqlSelect, false);
        ordered.write_query(&WRITER, &mut ctx, &mut out);
        // precedence delegates to inner expression
        let p = ordered.precedence(&WRITER);
        assert_eq!(p, Operand::LitInt(1).precedence(&WRITER));
    }

    #[test]
    fn test_dyn_expression_ref() {
        let op = Operand::LitStr("test");
        let dyn_ref: &dyn Expression = &op;
        let mut out = DynQuery::default();
        let mut ctx = Context::new(Fragment::SqlSelect, false);
        dyn_ref.write_query(&WRITER, &mut ctx, &mut out);
        assert_eq!(out.as_str(), "'test'");
        let ident = dyn_ref.as_identifier(&mut ctx);
        assert_eq!(ident, "'test'");
        assert_eq!(dyn_ref.precedence(&WRITER), 1_000_000);
    }
}
