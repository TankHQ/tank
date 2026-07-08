use crate::{
    DynQuery, Expression, ExpressionVisitor, OpPrecedence, Value,
    writer::{Context, SqlWriter},
};
use std::fmt;

pub enum Operand<'a> {
    Null,
    LitBool(bool),
    LitInt(i128),
    LitFloat(f64),
    LitStr(&'a str),
    LitIdent(&'a str),
    LitField(&'a [&'a str]),
    LitList(&'a [Operand<'a>]),
    LitTuple(&'a [Operand<'a>]),
    Type(Value),
    Variable(Value),
    Value(&'a Value),
    Call(&'static str, &'a [&'a dyn Expression]),
    Asterisk,
    QuestionMark,
    CurrentTimestampMs,
}

impl fmt::Debug for Operand<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("Null"),
            Self::LitBool(v) => f.debug_tuple("LitBool").field(v).finish(),
            Self::LitInt(v) => f.debug_tuple("LitInt").field(v).finish(),
            Self::LitFloat(v) => f.debug_tuple("LitFloat").field(v).finish(),
            Self::LitStr(v) => f.debug_tuple("LitStr").field(v).finish(),
            Self::LitIdent(v) => f.debug_tuple("LitIdent").field(v).finish(),
            Self::LitField(v) => f.debug_tuple("LitField").field(v).finish(),
            Self::LitList(v) => f.debug_tuple("LitList").field(v).finish(),
            Self::LitTuple(v) => f.debug_tuple("LitTuple").field(v).finish(),
            Self::Type(v) => f.debug_tuple("Type").field(v).finish(),
            Self::Variable(v) => f.debug_tuple("Variable").field(v).finish(),
            Self::Value(v) => f.debug_tuple("Value").field(v).finish(),
            Self::Call(name, _) => f.debug_tuple("Call").field(name).field(&"..").finish(),
            Self::Asterisk => f.write_str("Asterisk"),
            Self::QuestionMark => f.write_str("QuestionMark"),
            Self::CurrentTimestampMs => f.write_str("CurrentTimestampMs"),
        }
    }
}

impl OpPrecedence for Operand<'_> {
    fn precedence(&self, _writer: &dyn SqlWriter) -> i32 {
        1_000_000
    }
}

impl Expression for Operand<'_> {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        writer.write_operand(context, out, self)
    }

    fn accept_visitor(
        &self,
        matcher: &mut dyn ExpressionVisitor,
        writer: &dyn SqlWriter,
        context: &mut Context,
        out: &mut DynQuery,
    ) -> bool {
        matcher.visit_operand(writer, context, out, self)
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
            (Self::LitList(l), Self::LitList(r)) => l == r,
            (Self::LitTuple(l), Self::LitTuple(r)) => l == r,
            (Self::Type(l), Self::Type(r)) => l.same_type(r),
            (Self::Variable(l), Self::Variable(r)) => l == r,
            (Self::Asterisk, Self::Asterisk) => true,
            (Self::QuestionMark, Self::QuestionMark) => true,
            (Self::CurrentTimestampMs, Self::CurrentTimestampMs) => true,
            _ => false,
        }
    }
}
