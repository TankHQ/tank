use crate::{
    DynQuery, Expression, OpPrecedence, Value,
    writer::{Context, SqlWriter},
};

#[derive(Debug)]
pub enum Operand<'a> {
    LitBool(bool),
    LitFloat(f64),
    LitIdent(&'a str),
    LitField(&'a [&'a str]),
    LitInt(i128),
    LitStr(&'a str),
    LitArray(&'a [Operand<'a>]),
    LitTuple(&'a [Operand<'a>]),
    Null,
    Type(Value),
    Variable(Value),
    Call(&'static str, &'a [&'a dyn Expression]),
    Asterisk,
    QuestionMark,
}

impl OpPrecedence for Operand<'_> {
    fn precedence(&self, _writer: &dyn SqlWriter) -> i32 {
        1_000_000
    }
}

impl Expression for Operand<'_> {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_expression_operand(context, out, self)
    }
}

impl PartialEq for Operand<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::LitBool(l), Self::LitBool(r)) => l == r,
            (Self::LitFloat(l), Self::LitFloat(r)) => l == r,
            (Self::LitIdent(l), Self::LitIdent(r)) => l == r,
            (Self::LitField(l), Self::LitField(r)) => l == r,
            (Self::LitInt(l), Self::LitInt(r)) => l == r,
            (Self::LitStr(l), Self::LitStr(r)) => l == r,
            (Self::LitArray(l), Self::LitArray(r)) => l == r,
            (Self::LitTuple(l), Self::LitTuple(r)) => l == r,
            (Self::Type(l), Self::Type(r)) => l.same_type(r),
            (Self::Variable(l), Self::Variable(r)) => l == r,
            (Self::Asterisk, Self::Asterisk) => true,
            (Self::QuestionMark, Self::QuestionMark) => true,
            _ => false,
        }
    }
}
