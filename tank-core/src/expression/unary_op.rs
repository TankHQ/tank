use crate::{
    DynQuery, Expression, OpPrecedence,
    writer::{Context, SqlWriter},
};

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum UnaryOpType {
    Negative,
    Not,
}
impl OpPrecedence for UnaryOpType {
    fn precedence(&self, writer: &dyn SqlWriter) -> i32 {
        writer.expression_unary_op_precedence(self)
    }
}

#[derive(Debug)]
pub struct UnaryOp<V: Expression> {
    pub op: UnaryOpType,
    pub arg: V,
}

impl<E: Expression> OpPrecedence for UnaryOp<E> {
    fn precedence(&self, writer: &dyn SqlWriter) -> i32 {
        writer.expression_unary_op_precedence(&self.op)
    }
}

impl<E: Expression> Expression for UnaryOp<E> {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_expression_unary_op(
            context,
            out,
            &UnaryOp {
                op: self.op,
                arg: &self.arg,
            },
        )
    }
}
