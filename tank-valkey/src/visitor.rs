use std::collections::HashMap;
use tank_core::{
    BinaryOp, BinaryOpType, ColumnRef, Context, DynQuery, Expression, ExpressionVisitor, Operand,
    SqlWriter, UnaryOp, Value,
};

/// Visitor that extracts key-value pairs from a WHERE clause.
/// It expects the expression to be a conjunction (AND) of equality checks (column = literal).
#[derive(Default)]
pub struct KeyValueVisitor {
    pub values: HashMap<String, Value>,
}

impl<'a> ExpressionVisitor<'a> for KeyValueVisitor {
    type Output = ();

    fn visit_binary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) -> Self::Output {
        match value.op {
            BinaryOpType::And => {
                value.lhs.accept_visitor(self, _writer, _context, _out);
                value.rhs.accept_visitor(self, _writer, _context, _out);
            }
            BinaryOpType::Eq => {
                // Check if LHS is column and RHS is literal, or vice versa
                let mut col_name = None;
                let mut literal_value = None;

                // Simple check: Is LHS a column?
                // We need a helper visitor to check if expression is a column or literal without recursing
                // passed via `accept_visitor` 
                
                // Hack: We can just use string representation or try to inspect manually if possible, 
                // but Expression trait doesn't expose structure directly.
                // We rely on nested visitors.
                
                let mut extract_col = ExtractColumn::default();
                value.lhs.accept_visitor(&mut extract_col, _writer, _context, _out);
                if let Some(name) = extract_col.name {
                    col_name = Some(name);
                    let mut extract_val = ExtractValue::default();
                    value.rhs.accept_visitor(&mut extract_val, _writer, _context, _out);
                    literal_value = extract_val.value;
                } else {
                     // Try RHS as column
                    let mut extract_col = ExtractColumn::default();
                    value.rhs.accept_visitor(&mut extract_col, _writer, _context, _out);
                    if let Some(name) = extract_col.name {
                        col_name = Some(name);
                        let mut extract_val = ExtractValue::default();
                        value.lhs.accept_visitor(&mut extract_val, _writer, _context, _out);
                        literal_value = extract_val.value;
                    }
                }

                if let (Some(c), Some(v)) = (col_name, literal_value) {
                    self.values.insert(c, v);
                }
            }
            _ => {
                // Ignore other ops or log?
            }
        }
    }

    fn visit_unary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        _value: &UnaryOp<&dyn Expression>,
    ) -> Self::Output {
        // No-op for unary ops in simple key extraction
    }

    fn visit_operand(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        _value: &Operand,
    ) -> Self::Output {
        // Operands are leaves, handled in visit_binary_op logic via helpers
    }
}

#[derive(Default)]
struct ExtractColumn {
    name: Option<String>,
}

impl<'a> ExpressionVisitor<'a> for ExtractColumn {
    type Output = ();
    
    fn visit_column_ref(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        value: &ColumnRef,
    ) -> Self::Output {
        self.name = Some(value.name.to_string());
    }
    
    // Ignore others
    fn visit_binary_op(&mut self, _w: &dyn SqlWriter, _c: &mut Context, _o: &mut DynQuery, _v: &BinaryOp<&dyn Expression, &dyn Expression>) {}
    fn visit_unary_op(&mut self, _w: &dyn SqlWriter, _c: &mut Context, _o: &mut DynQuery, _v: &UnaryOp<&dyn Expression>) {}
    fn visit_operand(&mut self, _w: &dyn SqlWriter, _c: &mut Context, _o: &mut DynQuery, _v: &Operand) {}
    fn visit_value(&mut self, _w: &dyn SqlWriter, _c: &mut Context, _o: &mut DynQuery, _v: &Value) {}
}

#[derive(Default)]
struct ExtractValue {
    value: Option<Value>,
}

impl<'a> ExpressionVisitor<'a> for ExtractValue {
    type Output = ();
    
    fn visit_value(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        value: &Value,
    ) -> Self::Output {
        self.value = Some(value.clone());
    }
    
    fn visit_operand(
        &mut self,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
        value: &Operand,
    ) -> Self::Output {
         match value {
             Operand::Value(v) => self.visit_value(writer, context, out, v),
             Operand::LitInt(i) => self.value = Some(Value::Int64(Some(*i as i64))),
             Operand::LitFloat(f) => self.value = Some(Value::Float64(Some(*f))),
             Operand::LitStr(s) => self.value = Some(Value::Varchar(Some(s.clone()))),
             Operand::LitBool(b) => self.value = Some(Value::Boolean(Some(*b))),
             _ => {}
         }
    }

    // Ignore others
    fn visit_binary_op(&mut self, _w: &dyn SqlWriter, _c: &mut Context, _o: &mut DynQuery, _v: &BinaryOp<&dyn Expression, &dyn Expression>) {}
    fn visit_unary_op(&mut self, _w: &dyn SqlWriter, _c: &mut Context, _o: &mut DynQuery, _v: &UnaryOp<&dyn Expression>) {}
    fn visit_column_ref(&mut self, _w: &dyn SqlWriter, _c: &mut Context, _o: &mut DynQuery, _v: &ColumnRef) {}
}
