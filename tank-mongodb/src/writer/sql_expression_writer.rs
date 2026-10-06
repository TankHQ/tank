use crate::{
    MongoDBDriver, MongoDBPrepared, MongoDBSqlWriter, NegateNumber, glob_to_regex, like_to_regex,
};
use mongodb::bson::{Bson, Regex, doc};
use std::{mem, ops::Deref};
use tank_core::{
    BinaryOp, BinaryOpType, Context, DynQuery, Expression, Operand, SqlExpressionWriter,
    SqlValueWriter, UnaryOp, UnaryOpType,
};

impl SqlExpressionWriter for MongoDBSqlWriter {
    fn write_question_mark(&self, context: &mut Context, out: &mut DynQuery) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!(
                "Failed to get the bson in MongoDBSqlWriter::write_expression_operand_question_mark"
            );
            return;
        };
        *target = Bson::String(format!("$$param_{}", context.counter));
        context.counter += 1;
    }

    fn write_current_timestamp_ms(&self, _context: &mut Context, out: &mut DynQuery) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!(
                "Failed to get the bson in MongoDBSqlWriter::write_expression_operand_current_timestamp_ms"
            );
            return;
        };
        *target = doc! { "$toLong": "$$NOW" }.into();
    }

    fn write_unary_op(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &UnaryOp<&dyn Expression>,
    ) {
        match value.op {
            UnaryOpType::Negative => {
                let mut matcher = NegateNumber::default();
                if value.arg.accept_visitor(&mut matcher, self, context, out) {
                    self.write_value(context, out, &matcher.value);
                } else {
                    // TODO: Change when MongoDB introduces a better way to handle negative
                    BinaryOp {
                        op: BinaryOpType::Multiplication,
                        lhs: Operand::LitInt(-1),
                        rhs: value.arg,
                    }
                    .write_query(self, context, out);
                }
            }
            UnaryOpType::Not => {
                value.arg.write_query(self, context, out);
                let Some(target) = out
                    .as_prepared::<MongoDBDriver>()
                    .and_then(MongoDBPrepared::current_bson)
                else {
                    log::error!(
                        "Failed to get the bson in MongoDBSqlWriter::write_expression_unary_op after writing the argument"
                    );
                    return;
                };
                *target = doc! { "$not": [mem::take(target)] }.into();
            }
        }
    }

    fn write_binary_op(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) {
        match value.op {
            BinaryOpType::ShiftLeft => {
                return BinaryOp {
                    op: BinaryOpType::Multiplication,
                    lhs: value.lhs,
                    rhs: Operand::Call("POW", &[&Operand::LitInt(2), value.rhs]),
                }
                .write_query(self, context, out);
            }
            BinaryOpType::ShiftRight => {
                return Operand::Call(
                    "FLOOR",
                    &[&BinaryOp {
                        op: BinaryOpType::Division,
                        lhs: value.lhs,
                        rhs: Operand::Call("POW", &[&Operand::LitInt(2), value.rhs]),
                    }],
                )
                .write_query(self, context, out);
            }
            BinaryOpType::NotLike | BinaryOpType::NotRegexp | BinaryOpType::NotGlob => {
                return UnaryOp {
                    op: UnaryOpType::Not,
                    arg: BinaryOp {
                        op: match value.op {
                            BinaryOpType::NotLike => BinaryOpType::Like,
                            BinaryOpType::NotRegexp => BinaryOpType::Regexp,
                            BinaryOpType::NotGlob => BinaryOpType::Glob,
                            _ => unreachable!(),
                        },
                        lhs: value.lhs,
                        rhs: value.rhs,
                    },
                }
                .write_query(self, context, out);
            }
            // MongoDB is schemaless, render the operand as-is and let the value flow through
            BinaryOpType::Cast | BinaryOpType::Alias => {
                return value.lhs.write_query(self, context, out);
            }
            _ => {}
        }
        let Some(document) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::switch_to_document)
        else {
            log::error!(
                "The query provided to write_expression_binary_op (out) does not have a document to write the content into"
            );
            return;
        };
        let lhs = {
            let mut lhs = MongoDBSqlWriter::make_prepared();
            value.lhs.write_query(self, context, &mut lhs);
            let Some(lhs) = lhs
                .as_prepared::<MongoDBDriver>()
                .and_then(MongoDBPrepared::current_bson)
                .map(mem::take)
            else {
                log::error!(
                    "Unexpected error while rendering the lhs of the binary expression, failed to get the bson object"
                );
                return;
            };
            lhs
        };
        let mut rhs = {
            let mut rhs = MongoDBSqlWriter::make_prepared();
            value.rhs.write_query(self, context, &mut rhs);
            let Some(rhs) = rhs
                .as_prepared::<MongoDBDriver>()
                .and_then(MongoDBPrepared::current_bson)
                .map(mem::take)
            else {
                log::error!(
                    "Unexpected error while rendering the rhs of the binary expression, failed to get the bson object"
                );
                return;
            };
            rhs
        };
        let mut op = value.op;
        if matches!(value.op, BinaryOpType::Like | BinaryOpType::Glob) {
            let Bson::String(pattern) = rhs else {
                log::error!(
                    "MongoDB can handle LIKE/GLOB operations but only if the pattern is a string literal (to transform it in $regexMatch)"
                );
                return;
            };
            let regex = if value.op == BinaryOpType::Glob {
                glob_to_regex(&pattern)
            } else {
                like_to_regex(&pattern)
            };
            op = BinaryOpType::Regexp;
            rhs = Bson::RegularExpression(Regex {
                pattern: regex.into(),
                options: Default::default(),
            });
        }
        let key = Self::expression_binary_op_key(op).to_string();
        document.insert(
            key,
            match op {
                BinaryOpType::Regexp => Bson::Document(doc! {
                    "input": lhs,
                    "regex": rhs,
                }),
                _ => Bson::Array(vec![lhs, rhs]),
            },
        );
    }

    fn write_function(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        function: &str,
        args: &[&dyn Expression],
    ) {
        let Some(document) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::switch_to_document)
        else {
            log::error!(
                "The query provided to write_expression_call (out) does not have a document to write the content into"
            );
            return;
        };
        let function = match function {
            s if s.eq_ignore_ascii_case("abs") => "$abs",
            s if s.eq_ignore_ascii_case("acos") => "$acos",
            s if s.eq_ignore_ascii_case("asin") => "$asin",
            s if s.eq_ignore_ascii_case("atan") => "$atan",
            s if s.eq_ignore_ascii_case("atan2") => "$atan2",
            s if s.eq_ignore_ascii_case("avg") => "$avg",
            s if s.eq_ignore_ascii_case("ceil") => "$ceil",
            s if s.eq_ignore_ascii_case("cos") => "$cos",
            s if s.eq_ignore_ascii_case("count") => {
                return self.write_function(context, out, "sum", &[&Operand::LitInt(1)]);
            }
            s if s.eq_ignore_ascii_case("exp") => "$exp",
            s if s.eq_ignore_ascii_case("floor") => "$floor",
            s if s.eq_ignore_ascii_case("log") => "$ln",
            s if s.eq_ignore_ascii_case("log10") => "$log10",
            s if s.eq_ignore_ascii_case("max") => "$max",
            s if s.eq_ignore_ascii_case("min") => "$min",
            s if s.eq_ignore_ascii_case("pow") => "$pow",
            s if s.eq_ignore_ascii_case("round") => "$round",
            s if s.eq_ignore_ascii_case("sin") => "$sin",
            s if s.eq_ignore_ascii_case("sqrt") => "$sqrt",
            s if s.eq_ignore_ascii_case("sum") => "$sum",
            s if s.eq_ignore_ascii_case("tan") => "$tan",
            _ => {
                log::error!("Unknown function: ${function}");
                return;
            }
        };
        let len = args.len();
        let mut query = Self::make_prepared();
        if len == 1 {
            args[0].write_query(self, context, &mut query);
        } else {
            self.write_list(
                context,
                &mut query,
                &mut args.iter().map(Deref::deref),
                None,
                false,
            );
        };
        let Some(arg) = query
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
            .map(mem::take)
        else {
            log::error!("The query returned from write_query (out) does not have a current bson");
            return;
        };
        document.insert(function, arg);
    }
}
