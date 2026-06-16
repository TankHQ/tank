use crate::{MongoDBDriver, MongoDBPrepared, MongoDBSqlWriter, glob_to_regex, like_to_regex};
use mongodb::bson::{Bson, Document, Regex, doc};
use std::{borrow::Cow, iter, mem, sync::Arc};
use tank_core::{
    AsValue, BinaryOp, BinaryOpType, ColumnRef, Context, DynQuery, Expression, ExpressionVisitor,
    IsConstant, IsFalse, IsTrue, Operand, Ordered, SqlWriter, UnaryOp, UnaryOpType, Value,
};

pub type AggregateAliases = Arc<Vec<(Bson, String)>>;

#[derive(Default, PartialEq, Eq, Debug)]
pub enum FieldType {
    #[default]
    None,
    Identifier(String),
    Column(ColumnRef),
}

#[derive(Default, Debug)]
pub struct IsField<'a> {
    pub field: FieldType,
    pub known_columns: Arc<Vec<&'a String>>,
    pub aggregate_aliases: AggregateAliases,
}
impl<'a> ExpressionVisitor for IsField<'a> {
    fn visit_column(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        value: &ColumnRef,
    ) -> bool {
        self.field = FieldType::Column(value.clone());
        true
    }
    fn visit_operand(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        _out: &mut DynQuery,
        value: &Operand,
    ) -> bool {
        match value {
            Operand::LitIdent(v) => {
                if self.known_columns.iter().any(|k| k.as_str() == *v) {
                    self.field = FieldType::Identifier(v.to_string());
                } else {
                    self.field = FieldType::Column(ColumnRef {
                        name: Cow::Owned(v.to_string()),
                        table: "".into(),
                        schema: "".into(),
                    });
                }
                true
            }
            Operand::LitField(v) => {
                let mut it = v.iter().rev().copied();
                let name = it.next().unwrap_or("");
                let table = it.next().unwrap_or("");
                let schema = it.next().unwrap_or("");
                if table.is_empty()
                    && schema.is_empty()
                    && self.known_columns.iter().any(|k| k.as_str() == name)
                {
                    self.field = FieldType::Identifier(name.to_string());
                } else {
                    self.field = FieldType::Column(ColumnRef {
                        name: name.to_string().into(),
                        table: table.to_string().into(),
                        schema: schema.to_string().into(),
                    });
                }
                true
            }
            _ => {
                if self.known_columns.is_empty() && self.aggregate_aliases.is_empty() {
                    return false;
                }
                let identifier = value.as_identifier(&mut context.switch_table("".into()).current);
                if self.known_columns.contains(&&identifier) {
                    self.field = FieldType::Identifier(identifier);
                    return true;
                }
                if !self.aggregate_aliases.is_empty() {
                    let mut query = MongoDBSqlWriter::make_prepared();
                    value.write_query(writer, context, &mut query);
                    if let Some(bson) = query
                        .as_prepared::<MongoDBDriver>()
                        .and_then(MongoDBPrepared::current_bson)
                        .map(mem::take)
                        && let Some((_, alias)) =
                            self.aggregate_aliases.iter().find(|(b, _)| b == &bson)
                    {
                        self.field = FieldType::Identifier(alias.clone());
                        return true;
                    }
                }
                false
            }
        }
    }
    fn visit_unary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        context: &mut Context,
        _out: &mut DynQuery,
        value: &UnaryOp<&dyn Expression>,
    ) -> bool {
        if self.known_columns.is_empty() {
            return false;
        }
        let identifier = value.as_identifier(&mut context.switch_table("".into()).current);
        if self.known_columns.contains(&&identifier) {
            self.field = FieldType::Identifier(identifier);
            true
        } else {
            false
        }
    }
    fn visit_binary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        context: &mut Context,
        _out: &mut DynQuery,
        value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) -> bool {
        if self.known_columns.is_empty() {
            return false;
        }
        let identifier = value.as_identifier(&mut context.switch_table("".into()).current);
        if self.known_columns.contains(&&identifier) {
            self.field = FieldType::Identifier(identifier);
            true
        } else {
            false
        }
    }
    fn visit_ordered(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
        value: &Ordered<&dyn Expression>,
    ) -> bool {
        value.expression.accept_visitor(self, writer, context, out)
    }
}

#[derive(Default, Debug)]
pub struct WriteMatchExpression<'a> {
    pub started: bool,
    pub known_columns: Arc<Vec<&'a String>>,
    pub aggregate_aliases: AggregateAliases,
}
impl<'a> WriteMatchExpression<'a> {
    pub fn new() -> Self {
        WriteMatchExpression::default()
    }
    pub fn make_unmatchable() -> Document {
        doc! {
            "_id": { "$exists": false }
        }
    }
}
impl<'a> ExpressionVisitor for WriteMatchExpression<'a> {
    fn visit_column(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
        value: &ColumnRef,
    ) -> bool {
        value.write_query(writer, context, out);
        false
    }

    fn visit_operand(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
        value: &Operand,
    ) -> bool {
        let top = !self.started;
        self.started = true;
        let mut is_expr = false;
        let value = 'wrote: {
            if top {
                let is_false = value.accept_visitor(&mut IsFalse, writer, context, out);
                let is_true = value.accept_visitor(&mut IsTrue, writer, context, out);
                if is_false || is_true {
                    break 'wrote Some(if is_true {
                        Bson::Document(Default::default())
                    } else {
                        Self::make_unmatchable().into()
                    });
                }
            }
            if matches!(
                value,
                Operand::LitIdent(..)
                    | Operand::LitField(..)
                    | Operand::Call(..)
                    | Operand::Asterisk
                    | Operand::QuestionMark
            ) {
                is_expr = true;
            }
            value.write_query(writer, context, out);
            None
        };
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in WriteMatchExpression::visit_operand");
            return false;
        };
        if let Some(value) = value {
            *target = value;
        }
        if top && is_expr {
            *target = doc! { "$expr": &*target }.into();
        }
        is_expr
    }

    fn visit_unary_op(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
        value: &UnaryOp<&dyn Expression>,
    ) -> bool {
        let top = !self.started;
        self.started = true;
        if top && value.op == UnaryOpType::Not {
            let mut handled = false;
            {
                let mut rewriter = NotRewriter {
                    parent: self,
                    handled: &mut handled,
                };
                value
                    .arg
                    .accept_visitor(&mut rewriter, writer, context, out);
            }
            if handled {
                return false;
            }
        }
        let mut negate_number = NegateNumber::default();
        let is_expr = !(value.op == UnaryOpType::Negative
            && value
                .arg
                .accept_visitor(&mut negate_number, writer, context, out));
        value.write_query(writer, context, out);
        if top && is_expr {
            let Some(target) = out
                .as_prepared::<MongoDBDriver>()
                .and_then(MongoDBPrepared::current_bson)
            else {
                log::error!("Failed to get the bson in WriteMatchExpression::visit_operand");
                return false;
            };
            *target = doc! { "$expr": &*target }.into();
        }
        is_expr
    }

    fn visit_binary_op(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
        value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) -> bool {
        let top = !self.started;
        self.started = true;
        let is_expr;
        'wrote: {
            if let Some(root) = match value.op {
                BinaryOpType::And => Some("$and"),
                BinaryOpType::Or => Some("$or"),
                _ => None,
            } {
                let mut args = Vec::new();
                let mut arg_is_expr = Vec::new();
                let mut all_expr = true;
                for side in [value.lhs, value.rhs] {
                    let mut query = MongoDBSqlWriter::make_prepared();
                    let expr_arg = side.accept_visitor(self, writer, context, &mut query);
                    all_expr = all_expr && expr_arg;
                    let Some(mut bson) = query
                        .as_prepared::<MongoDBDriver>()
                        .and_then(MongoDBPrepared::current_bson)
                        .map(mem::take)
                    else {
                        log::error!(
                            "Failed to get the bson in WriteMatchExpression::visit_binary_op"
                        );
                        return false;
                    };
                    if let Some(doc) = bson.as_document_mut()
                        && doc.keys().eq([root])
                        && let Ok(v) = doc.get_array_mut(root)
                    {
                        arg_is_expr.extend(iter::repeat(expr_arg).take(v.len()));
                        args.append(v);
                    } else {
                        arg_is_expr.push(expr_arg);
                        args.push(bson);
                    }
                }
                if all_expr {
                    is_expr = true;
                } else {
                    for (i, _) in arg_is_expr.iter().enumerate().filter(|(_, v)| **v) {
                        args[i] = doc! { "$expr": mem::take(&mut args[i]) }.into();
                    }
                    is_expr = false;
                }
                let Some(target) = out
                    .as_prepared::<MongoDBDriver>()
                    .and_then(MongoDBPrepared::current_bson)
                else {
                    log::error!("Failed to get the bson in WriteMatchExpression::visit_binary_op");
                    return false;
                };
                *target = doc! { root: Bson::Array(args) }.into();
                break 'wrote;
            }
            if matches!(
                value.op,
                BinaryOpType::In
                    | BinaryOpType::NotIn
                    | BinaryOpType::Is
                    | BinaryOpType::IsNot
                    | BinaryOpType::Equal
                    | BinaryOpType::NotEqual
                    | BinaryOpType::Like
                    | BinaryOpType::NotLike
                    | BinaryOpType::Regexp
                    | BinaryOpType::NotRegexp
                    | BinaryOpType::Glob
                    | BinaryOpType::NotGlob
                    | BinaryOpType::Less
                    | BinaryOpType::Greater
                    | BinaryOpType::LessEqual
                    | BinaryOpType::GreaterEqual
            ) {
                let mut l_column = IsField {
                    known_columns: self.known_columns.clone(),
                    aggregate_aliases: self.aggregate_aliases.clone(),
                    ..Default::default()
                };
                let mut r_column = IsField {
                    known_columns: self.known_columns.clone(),
                    aggregate_aliases: self.aggregate_aliases.clone(),
                    ..Default::default()
                };
                let l_constant = value
                    .lhs
                    .accept_visitor(&mut IsConstant, writer, context, out);
                let r_constant = value
                    .rhs
                    .accept_visitor(&mut IsConstant, writer, context, out);
                if (value
                    .lhs
                    .accept_visitor(&mut l_column, writer, context, out)
                    && r_constant)
                    != (value
                        .rhs
                        .accept_visitor(&mut r_column, writer, context, out)
                        && l_constant)
                {
                    let (field, value, op) = if l_column.field != FieldType::None {
                        (l_column, value.rhs, value.op)
                    } else if r_column.field != FieldType::None {
                        (
                            r_column,
                            value.lhs,
                            match value.op {
                                BinaryOpType::Less => BinaryOpType::Greater,
                                BinaryOpType::Greater => BinaryOpType::Less,
                                BinaryOpType::LessEqual => BinaryOpType::GreaterEqual,
                                BinaryOpType::GreaterEqual => BinaryOpType::LessEqual,
                                v => v,
                            },
                        )
                    } else {
                        log::error!(
                            "Unexpected error, the matcher conditions succeeded but the field was not found"
                        );
                        return false;
                    };
                    let Some(target) = out
                        .as_prepared::<MongoDBDriver>()
                        .and_then(MongoDBPrepared::current_bson)
                    else {
                        log::error!(
                            "Failed to get the bson in WriteMatchExpression::visit_binary_op"
                        );
                        return false;
                    };
                    let field = match field.field {
                        FieldType::None => unreachable!(),
                        FieldType::Identifier(v) => v,
                        FieldType::Column(v) => v.as_identifier(context),
                    };
                    let mut query: DynQuery = MongoDBSqlWriter::make_prepared();
                    value.write_query(writer, context, &mut query);
                    let Some(val_bson) = query
                        .as_prepared::<MongoDBDriver>()
                        .and_then(MongoDBPrepared::current_bson)
                        .map(mem::take)
                    else {
                        log::error!(
                            "Unexpected error, for some reason the rendered value does not have a current bson"
                        );
                        return false;
                    };
                    let val_bson = if op == BinaryOpType::Equal {
                        val_bson
                    } else if matches!(
                        op,
                        BinaryOpType::Like
                            | BinaryOpType::NotLike
                            | BinaryOpType::Regexp
                            | BinaryOpType::NotRegexp
                            | BinaryOpType::Glob
                            | BinaryOpType::NotGlob
                    ) {
                        let mut pattern = val_bson;
                        if matches!(
                            op,
                            BinaryOpType::Like
                                | BinaryOpType::NotLike
                                | BinaryOpType::Glob
                                | BinaryOpType::NotGlob
                        ) {
                            pattern = if let Bson::String(p) = pattern {
                                let regex =
                                    if matches!(op, BinaryOpType::Glob | BinaryOpType::NotGlob) {
                                        glob_to_regex(&p)
                                    } else {
                                        like_to_regex(&p)
                                    };
                                Bson::RegularExpression(Regex {
                                    pattern: regex,
                                    options: Default::default(),
                                })
                            } else {
                                log::error!(
                                    "MongoDB can handle LIKE/GLOB operations but only if the pattern is a string literal (to transform it in $regex)"
                                );
                                return false;
                            };
                        }
                        if matches!(
                            op,
                            BinaryOpType::NotLike | BinaryOpType::NotRegexp | BinaryOpType::NotGlob
                        ) {
                            doc! { "$not": { "$regex": pattern }, "$ne": Bson::Null }.into()
                        } else {
                            doc! { "$regex": pattern }.into()
                        }
                    } else {
                        let op_key = MongoDBSqlWriter::expression_binary_op_key(op).to_string();
                        doc! { op_key: val_bson }.into()
                    };
                    *target = doc! { field: val_bson }.into();
                    is_expr = false;
                    break 'wrote;
                }
            }
            writer.write_binary_op(context, out, value);
            is_expr = true;
        }
        if top && is_expr {
            let Some(target) = out
                .as_prepared::<MongoDBDriver>()
                .and_then(MongoDBPrepared::current_bson)
            else {
                log::error!("Failed to get the bson in WriteMatchExpression::visit_operand");
                return false;
            };
            *target = doc! { "$expr": &*target }.into();
        }
        is_expr
    }

    fn visit_ordered(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
        value: &Ordered<&dyn Expression>,
    ) -> bool {
        value.write_query(writer, context, out);
        true
    }
}

struct NotRewriter<'p, 'a, 'h> {
    parent: &'p mut WriteMatchExpression<'a>,
    handled: &'h mut bool,
}
impl<'p, 'a, 'h> NotRewriter<'p, 'a, 'h> {
    fn invert(op: BinaryOpType) -> Option<BinaryOpType> {
        match op {
            BinaryOpType::Equal => Some(BinaryOpType::NotEqual),
            BinaryOpType::NotEqual => Some(BinaryOpType::Equal),
            BinaryOpType::Less => Some(BinaryOpType::GreaterEqual),
            BinaryOpType::Greater => Some(BinaryOpType::LessEqual),
            BinaryOpType::LessEqual => Some(BinaryOpType::Greater),
            BinaryOpType::GreaterEqual => Some(BinaryOpType::Less),
            BinaryOpType::Like => Some(BinaryOpType::NotLike),
            BinaryOpType::NotLike => Some(BinaryOpType::Like),
            BinaryOpType::Regexp => Some(BinaryOpType::NotRegexp),
            BinaryOpType::NotRegexp => Some(BinaryOpType::Regexp),
            BinaryOpType::Glob => Some(BinaryOpType::NotGlob),
            BinaryOpType::NotGlob => Some(BinaryOpType::Glob),
            BinaryOpType::In => Some(BinaryOpType::NotIn),
            BinaryOpType::NotIn => Some(BinaryOpType::In),
            BinaryOpType::Is => Some(BinaryOpType::IsNot),
            BinaryOpType::IsNot => Some(BinaryOpType::Is),
            _ => None,
        }
    }
}
impl<'p, 'a, 'h> ExpressionVisitor for NotRewriter<'p, 'a, 'h> {
    fn visit_column(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
        value: &ColumnRef,
    ) -> bool {
        let rhs = Operand::LitBool(false);
        let rewritten = BinaryOp {
            op: BinaryOpType::Equal,
            lhs: value,
            rhs: &rhs,
        };
        rewritten.accept_visitor(self.parent, writer, context, out);
        *self.handled = true;
        true
    }
    fn visit_operand(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
        value: &Operand,
    ) -> bool {
        if matches!(value, Operand::LitIdent(..) | Operand::LitField(..)) {
            let rhs = Operand::LitBool(false);
            let rewritten = BinaryOp {
                op: BinaryOpType::Equal,
                lhs: value,
                rhs: &rhs,
            };
            rewritten.accept_visitor(self.parent, writer, context, out);
            *self.handled = true;
            true
        } else {
            false
        }
    }
    fn visit_binary_op(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
        value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) -> bool {
        if let Some(op) = Self::invert(value.op) {
            let rewritten = BinaryOp {
                op,
                lhs: value.lhs,
                rhs: value.rhs,
            };
            rewritten.accept_visitor(self.parent, writer, context, out);
            *self.handled = true;
            true
        } else {
            false
        }
    }
}

#[derive(Default, Debug)]
pub struct NegateNumber {
    pub value: Value,
}
impl ExpressionVisitor for NegateNumber {
    fn visit_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        value: &Operand,
    ) -> bool {
        match value {
            Operand::LitInt(v) => {
                self.value = (-v).as_value();
                return true;
            }
            Operand::LitFloat(v) => {
                self.value = (-v).as_value();
                return true;
            }
            Operand::Variable(v) => match v {
                Value::Int8(Some(v)) => {
                    self.value = (-v).as_value();
                    return true;
                }
                Value::Int16(Some(v)) => {
                    self.value = (-v).as_value();
                    return true;
                }
                Value::Int32(Some(v)) => {
                    self.value = (-v).as_value();
                    return true;
                }
                Value::Int64(Some(v)) => {
                    self.value = (-v).as_value();
                    return true;
                }
                Value::Int128(Some(v)) => {
                    self.value = (-v).as_value();
                    return true;
                }
                Value::Float32(Some(v)) => {
                    self.value = (-v).as_value();
                    return true;
                }
                Value::Float64(Some(v)) => {
                    self.value = (-v).as_value();
                    return true;
                }
                Value::Decimal(Some(v), ..) => {
                    self.value = (-v).as_value();
                    return true;
                }
                _ => {}
            },
            Operand::Value(v) => match v {
                Value::Int8(Some(v)) => {
                    self.value = (-v).as_value();
                    return true;
                }
                Value::Int16(Some(v)) => {
                    self.value = (-v).as_value();
                    return true;
                }
                Value::Int32(Some(v)) => {
                    self.value = (-v).as_value();
                    return true;
                }
                Value::Int64(Some(v)) => {
                    self.value = (-v).as_value();
                    return true;
                }
                Value::Int128(Some(v)) => {
                    self.value = (-v).as_value();
                    return true;
                }
                Value::Float32(Some(v)) => {
                    self.value = (-v).as_value();
                    return true;
                }
                Value::Float64(Some(v)) => {
                    self.value = (-v).as_value();
                    return true;
                }
                Value::Decimal(Some(v), ..) => {
                    self.value = (-v).as_value();
                    return true;
                }
                _ => {}
            },
            _ => {}
        }
        false
    }
}
