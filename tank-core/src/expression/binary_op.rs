use crate::{
    DynQuery, Expression, ExpressionMatcher, OpPrecedence,
    writer::{Context, SqlWriter},
};
use proc_macro2::TokenStream;
use quote::{ToTokens, TokenStreamExt, quote};
use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    fn matches(&self, matcher: &mut dyn ExpressionMatcher) -> bool {
        matcher.match_binary_op(&self.op, &self.lhs, &self.rhs)
    }
}

impl Display for BinaryOpType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            BinaryOpType::Indexing => "Indexing",
            BinaryOpType::Cast => "Cast",
            BinaryOpType::Multiplication => "Multiplication",
            BinaryOpType::Division => "Division",
            BinaryOpType::Remainder => "Remainder",
            BinaryOpType::Addition => "Addition",
            BinaryOpType::Subtraction => "Subtraction",
            BinaryOpType::ShiftLeft => "ShiftLeft",
            BinaryOpType::ShiftRight => "ShiftRight",
            BinaryOpType::BitwiseAnd => "BitwiseAnd",
            BinaryOpType::BitwiseOr => "BitwiseOr",
            BinaryOpType::In => "In",
            BinaryOpType::NotIn => "NotIn",
            BinaryOpType::Is => "Is",
            BinaryOpType::IsNot => "IsNot",
            BinaryOpType::Like => "Like",
            BinaryOpType::NotLike => "NotLike",
            BinaryOpType::Regexp => "Regexp",
            BinaryOpType::NotRegexp => "NotRegexp",
            BinaryOpType::Glob => "Glob",
            BinaryOpType::NotGlob => "NotGlob",
            BinaryOpType::Equal => "Equal",
            BinaryOpType::NotEqual => "NotEqual",
            BinaryOpType::Less => "Less",
            BinaryOpType::Greater => "Greater",
            BinaryOpType::LessEqual => "LessEqual",
            BinaryOpType::GreaterEqual => "GreaterEqual",
            BinaryOpType::And => "And",
            BinaryOpType::Or => "Or",
            BinaryOpType::Alias => "Alias",
        })
    }
}

impl ToTokens for BinaryOpType {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let v = self.to_string();
        tokens.append_all(quote!(::tank::BinaryOpType::#v));
    }
}
