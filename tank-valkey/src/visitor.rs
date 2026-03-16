use std::{borrow::Cow, mem};
use tank_core::{
    BinaryOp, BinaryOpType, ColumnDef, ColumnRef, Context, DynQuery, Expression, ExpressionVisitor,
    IsConstant, Operand, Ordered, SqlWriter,
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
    started: bool,
    original_pk: &'static [&'static ColumnDef],
    pk: &'static [&'static ColumnDef],
    retry: bool,
}
impl IsPKCondition {
    pub fn new(prefix: String, pk: &'static [&'static ColumnDef]) -> Self {
        IsPKCondition {
            key: prefix,
            started: false,
            original_pk: pk,
            pk,
            retry: false,
        }
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
        let top = !self.started;
        self.started = true;
        match value.op {
            BinaryOpType::And => {
                let mut lhs_done = false;
                let mut rhs_done = false;
                loop {
                    let pk_len_before = self.pk.len();
                    if !lhs_done {
                        self.retry = false;
                        lhs_done = value.lhs.accept_visitor(self, writer, context, out);
                        if !lhs_done && !self.retry {
                            return false;
                        }
                    }
                    if !rhs_done {
                        self.retry = false;
                        rhs_done = value.rhs.accept_visitor(self, writer, context, out);
                        if !rhs_done && !self.retry {
                            return false;
                        }
                    }
                    if lhs_done && rhs_done {
                        if top && !self.pk.is_empty() {
                            return false;
                        }
                        return true;
                    }
                    if self.pk.len() == pk_len_before {
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
                let matched_count = self.original_pk.len() - self.pk.len();
                if self.original_pk[..matched_count]
                    .iter()
                    .any(|c| c.column_ref.name == is_column.field)
                {
                    return true;
                }
                let Some(first) = self.pk.first() else {
                    return false;
                };
                if is_column.field != first.column_ref.name {
                    self.retry = true;
                    return false;
                }
                if !self.key.is_empty() {
                    self.key.push_str(writer.separator());
                }
                self.key.push_str(&is_column.field);
                self.key.push_str(writer.separator());
                let mut out = DynQuery::new(mem::take(&mut self.key));
                value.write_query(writer, context, &mut out);
                self.key = mem::take(out.buffer());
                self.pk = &self.pk[1..];
                if top && !self.pk.is_empty() {
                    return false;
                }
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
