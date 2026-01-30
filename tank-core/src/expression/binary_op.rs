use crate::{
    DynQuery, Expression, ExpressionMatcher, OpPrecedence,
    writer::{Context, SqlWriter},
};
use proc_macro2::TokenStream;
use quote::{ToTokens, TokenStreamExt, quote};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BinaryOpType {
    Indexing,
    Cast,
    Multiplication,
    Division,
    Remainder,
    Addition,
    Subtraction,
    ShiftLeft,
    ShiftRight,
    BitwiseAnd,
    BitwiseOr,
    In,
    NotIn,
    Is,
    IsNot,
    Like,
    NotLike,
    Regexp,
    NotRegexp,
    Glob,
    NotGlob,
    Equal,
    NotEqual,
    Less,
    Greater,
    LessEqual,
    GreaterEqual,
    And,
    Or,
    Alias,
}

impl OpPrecedence for BinaryOpType {
    fn precedence(&self, writer: &dyn SqlWriter) -> i32 {
        writer.expression_binary_op_precedence(self)
    }
}

#[derive(Debug)]
pub struct BinaryOp<L: Expression, R: Expression> {
    pub op: BinaryOpType,
    pub lhs: L,
    pub rhs: R,
}

impl<L: Expression, R: Expression> OpPrecedence for BinaryOp<L, R> {
    fn precedence(&self, writer: &dyn SqlWriter) -> i32 {
        writer.expression_binary_op_precedence(&self.op)
    }
}

impl<L: Expression, R: Expression> Expression for BinaryOp<L, R> {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_expression_binary_op(
            context,
            out,
            &BinaryOp {
                op: self.op,
                lhs: &self.lhs,
                rhs: &self.rhs,
            },
        )
    }
    fn matches(&self, matcher: &mut dyn ExpressionMatcher, writer: &dyn SqlWriter) -> bool {
        matcher.match_binary_op(writer, &self.op, &self.lhs, &self.rhs)
    }
}

impl ToTokens for BinaryOpType {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let v = format!("{self:?}");
        tokens.append_all(quote!(::tank::BinaryOpType::#v));
    }
}
