use tank_core::{Context, DynQuery, ExpressionVisitor, Operand, SqlWriter, Value};

#[derive(Default)]
pub(crate) struct IsChar {
    pub value: char,
}

impl ExpressionVisitor for IsChar {
    fn visit_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        value: &Operand,
    ) -> bool {
        match value {
            Operand::Variable(v) => match v {
                Value::Char(Some(v)) => {
                    self.value = *v;
                    return true;
                }
                _ => {}
            },
            Operand::Value(v) => match v {
                Value::Char(Some(v)) => {
                    self.value = *v;
                    return true;
                }
                _ => {}
            },
            _ => {}
        }
        false
    }
}
