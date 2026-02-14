use crate::{MongoDBDriver, MongoDBPrepared, MongoDBSqlWriter};
use mongodb::bson::{Bson, Document, doc};
use std::mem;
use tank_core::{
    AsValue, BinaryOp, BinaryOpType, ColumnRef, Context, DynQuery, Expression, ExpressionVisitor,
    IsAsterisk, IsColumn, IsFalse, IsTrue, Operand, Ordered, SqlWriter, UnaryOp, Value,
};

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
pub struct WriteMatchExpression {
    pub started: bool,
}
impl WriteMatchExpression {
    pub fn new() -> Self {
        WriteMatchExpression::default()
    }
    pub fn make_unmatchable() -> Document {
        doc! {
            "_id": { "$exists": false }
        }
    }
}
impl ExpressionVisitor for WriteMatchExpression {
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
        value.write_query(writer, context, out);
        true
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
        let mut is_expr = false;
        'wrote: {
            if let Some(root) = match value.op {
                BinaryOpType::And => Some("$and"),
                BinaryOpType::Or => Some("$or"),
                _ => None,
            } {
                let mut args = Vec::new();
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
                    if expr_arg {
                        bson = doc! { "$expr": bson }.into();
                    }
                    if let Some(doc) = bson.as_document_mut()
                        && doc.keys().eq([root])
                        && let Ok(v) = doc.get_array_mut(root)
                    {
                        args.append(v);
                    } else {
                        args.push(bson);
                    }
                }
                if all_expr {
                    for arg in &mut args {
                        *arg = mem::take(arg.as_document_mut().unwrap().get_mut("$expr").unwrap());
                    }
                    is_expr = true;
                } else {
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
                let mut l_column = IsColumn::default();
                let mut r_column = IsColumn::default();
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
                    let (field, value, op) = if let Some(field) = l_column.column {
                        (field, value.rhs, value.op)
                    } else if let Some(field) = mem::take(&mut r_column.column) {
                        (
                            field,
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
                    let field = field.as_identifier(context);
                    let mut query = MongoDBSqlWriter::make_prepared();
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
