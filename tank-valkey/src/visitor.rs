use std::{borrow::Cow, mem};
use tank_core::{
    BinaryOp, BinaryOpType, ColumnRef, Context, DynQuery, Expression, ExpressionVisitor,
    GenericSqlWriter, IsConstant, Operand, Ordered, SqlWriter,
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
        _out: &mut DynQuery,
        value: &ColumnRef,
    ) -> bool {
        self.field = value.name.clone();
        true
    }
    fn visit_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
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
        out: &mut DynQuery,
        value: &Ordered<&dyn Expression>,
    ) -> bool {
        value.expression.accept_visitor(self, writer, context, out)
    }
}

pub struct IsPKCondition {
    pub key: String,
}
impl IsPKCondition {
    pub fn new(prefix: String) -> Self {
        IsPKCondition { key: prefix }
    }
}
impl ExpressionVisitor for IsPKCondition {
    fn visit_binary_op(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
        value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) -> bool {
        match value.op {
            BinaryOpType::And => {
                value.lhs.accept_visitor(self, writer, context, out)
                    && value.rhs.accept_visitor(self, writer, context, out)
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
                if !self.key.is_empty() {
                    self.key.push(':');
                }
                self.key.push_str(&is_column.field);
                self.key.push(':');
                let mut out = DynQuery::new(mem::take(&mut self.key));
                value.write_query(writer, context, &mut out);
                self.key = mem::take(out.buffer());
                true
            }
            _ => false,
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

#[derive(Default)]
struct ExtractColumn {
    name: Option<String>,
}
