use std::borrow::Cow;
use tank_core::{
    AnyQuery, BinaryOp, BinaryOpType, ColumnRef, Context, Expression, ExpressionVisitor,
    IsConstant, Operand, Ordered, SqlWriter, TableRef,
};

#[derive(Default, Debug)]
pub struct IsField {
    pub field: Cow<'static, str>,
}

impl ExpressionVisitor for IsField {
    fn visit_column(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut AnyQuery,
        value: &ColumnRef,
    ) -> bool {
        self.field = value.name.clone();
        true
    }
    fn visit_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut AnyQuery,
        value: &Operand,
    ) -> bool {
        match value {
            Operand::LitIdent(v) => {
                self.field = v.to_string().into();
                true
            }
            Operand::LitField(v) => {
                self.field = v
                    .into_iter()
                    .last()
                    .map(ToString::to_string)
                    .unwrap_or_default()
                    .into();
                true
            }
            _ => false,
        }
    }
    fn visit_ordered(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut AnyQuery,
        value: &Ordered<&dyn Expression>,
    ) -> bool {
        value.expression.accept_visitor(self, writer, context, out)
    }
}

pub struct IsPKCondition<'a> {
    keys_with_names: bool,
    pub key: String,
    started: bool,
    table: &'a TableRef,
    /// Primary key columns already matched, in key order.
    matched: usize,
    /// Rendered value of each matched column, used to accept retries.
    values: Vec<(Cow<'static, str>, String)>,
    retry: bool,
}
impl<'a> IsPKCondition<'a> {
    pub const fn new(keys_with_names: bool, prefix: String, table: &'a TableRef) -> Self {
        IsPKCondition {
            keys_with_names,
            key: prefix,
            started: false,
            table,
            matched: 0,
            values: Vec::new(),
            retry: false,
        }
    }
    fn is_complete(&self) -> bool {
        self.table.primary_key().nth(self.matched).is_none()
    }
}
impl ExpressionVisitor for IsPKCondition<'_> {
    fn visit_binary_op(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut AnyQuery,
        value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) -> bool {
        let is_tree_root = !self.started;
        self.started = true;
        match value.op {
            BinaryOpType::And => {
                let mut lhs_is_pk_condition = false;
                let mut rhs_is_pk_condition = false;
                loop {
                    let matched_before = self.matched;
                    if !lhs_is_pk_condition {
                        self.retry = false;
                        lhs_is_pk_condition = value.lhs.accept_visitor(self, writer, context, out);
                        if !lhs_is_pk_condition && !self.retry {
                            return false;
                        }
                    }
                    if !rhs_is_pk_condition {
                        self.retry = false;
                        rhs_is_pk_condition = value.rhs.accept_visitor(self, writer, context, out);
                        if !rhs_is_pk_condition && !self.retry {
                            return false;
                        }
                    }
                    if lhs_is_pk_condition && rhs_is_pk_condition {
                        return !is_tree_root || self.is_complete();
                    }
                    if self.matched == matched_before {
                        self.retry = true;
                        return false;
                    }
                }
            }
            BinaryOpType::Equal => {
                let mut is_column = IsField::default();
                let mut is_constant = IsConstant::default();
                let value = if value
                    .lhs
                    .accept_visitor(&mut is_column, writer, context, out)
                    && value
                        .rhs
                        .accept_visitor(&mut is_constant, writer, context, out)
                {
                    value.rhs
                } else if value
                    .lhs
                    .accept_visitor(&mut is_constant, writer, context, out)
                    && value
                        .rhs
                        .accept_visitor(&mut is_column, writer, context, out)
                {
                    value.lhs
                } else {
                    return false;
                };
                let mut primary_key = self.table.primary_key();
                let mut rendered = AnyQuery::default();
                value.write_query(writer, context, &mut rendered);
                if primary_key
                    .by_ref()
                    .take(self.matched)
                    .any(|c| c.name() == is_column.field)
                {
                    // Retries revisit matched columns, so an identical value is accepted
                    return self
                        .values
                        .iter()
                        .any(|(name, v)| name == &is_column.field && v == rendered.buffer());
                }
                let Some(next) = primary_key.next() else {
                    return false;
                };
                if next.name() != is_column.field {
                    self.retry = true;
                    return false;
                }
                if !self.key.is_empty() {
                    self.key.push_str(writer.separator());
                }
                if self.keys_with_names {
                    self.key.push_str(&is_column.field);
                    self.key.push_str(writer.separator());
                }
                self.key.push_str(rendered.buffer());
                self.values
                    .push((is_column.field.clone(), rendered.buffer().clone()));
                self.matched += 1;
                !is_tree_root || self.is_complete()
            }
            _ => false,
        }
    }
    fn visit_ordered(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut AnyQuery,
        value: &Ordered<&dyn Expression>,
    ) -> bool {
        value.expression.accept_visitor(self, writer, context, out)
    }
}
