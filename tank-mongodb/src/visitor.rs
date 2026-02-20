use crate::{MongoDBDriver, MongoDBPrepared, MongoDBSqlWriter};
use mongodb::bson::{Bson, Document, doc};
use std::{borrow::Cow, iter, mem, sync::Arc};
use tank_core::{
    AsValue, BinaryOp, BinaryOpType, ColumnRef, Context, DynQuery, Expression, ExpressionVisitor,
    IsAsterisk, IsFalse, IsTrue, Operand, Ordered, SqlWriter, UnaryOp, UnaryOpType, Value,
};

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
        _writer: &dyn SqlWriter,
        context: &mut Context,
        _out: &mut DynQuery,
        value: &Operand,
    ) -> bool {
        match value {
            Operand::LitIdent(v) => {
                self.field = FieldType::Column(ColumnRef {
                    name: Cow::Owned(v.to_string()),
                    table: "".into(),
                    schema: "".into(),
                });
                true
            }
            Operand::LitField(v) => {
                let mut it = v.into_iter().rev();
                let name = it.next().map(ToString::to_string).unwrap_or_default();
                let table = it.next().map(ToString::to_string).unwrap_or_default();
                let schema = it.next().map(ToString::to_string).unwrap_or_default();
                self.field = FieldType::Column(ColumnRef {
                    name: name.into(),
                    table: table.into(),
                    schema: schema.into(),
                });
                true
            }
            _ => {
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
pub struct IsConstant;
impl ExpressionVisitor for IsConstant {
    fn visit_operand(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
        value: &Operand,
    ) -> bool {
        match value {
            Operand::Null
            | Operand::LitBool(..)
            | Operand::LitInt(..)
            | Operand::LitFloat(..)
            | Operand::LitStr(..)
            | Operand::Type(..)
            | Operand::Variable(..)
            | Operand::Value(..) => true,
            Operand::LitArray(operands) | Operand::LitTuple(operands) => operands
                .iter()
                .all(|v| v.accept_visitor(&mut IsConstant, writer, context, out)),
            _ => false,
        }
    }
}

#[derive(Default, Debug)]
pub struct WriteMatchExpression<'a> {
    pub started: bool,
    pub known_columns: Arc<Vec<&'a String>>,
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
                    | BinaryOpType::Less
                    | BinaryOpType::Greater
                    | BinaryOpType::LessEqual
                    | BinaryOpType::GreaterEqual
            ) {
                let mut l_column = IsField {
                    known_columns: self.known_columns.clone(),
                    ..Default::default()
                };
                let mut r_column = IsField {
                    known_columns: self.known_columns.clone(),
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
                    } else {
                        let op_key = MongoDBSqlWriter::expression_binary_op_key(op).to_string();
                        doc! { op_key: val_bson }.into()
                    };
                    *target = doc! { field: val_bson }.into();
                    is_expr = false;
                    break 'wrote;
                }
            }
            writer.write_expression_binary_op(context, out, value);
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

#[derive(Default, Debug)]
pub struct IsCount;
impl ExpressionVisitor for IsCount {
    fn visit_operand(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
        value: &Operand,
    ) -> bool {
        match value {
            Operand::Call(function, args) => {
                if function.eq_ignore_ascii_case("count")
                    && let [arg] = args
                    && let mut c = context.clone()
                    && arg.accept_visitor(&mut IsAsterisk, writer, &mut c, out)
                {
                    *context = c;
                    true
                } else {
                    false
                }
            }
            _ => false,
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
