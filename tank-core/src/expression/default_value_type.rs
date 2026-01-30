use crate::{Context, DynQuery, Expression, ExpressionMatcher, OpPrecedence, SqlWriter, Value};

#[derive(Default, Debug)]
pub enum DefaultValueType {
    #[default]
    None,
    Value(Value),
    Expression(Box<dyn Expression>),
}

impl DefaultValueType {
    pub fn is_set(&self) -> bool {
        matches!(
            self,
            DefaultValueType::Value(..) | DefaultValueType::Expression(..)
        )
    }
}

impl OpPrecedence for DefaultValueType {
    fn precedence(&self, writer: &dyn SqlWriter) -> i32 {
        match self {
            DefaultValueType::None => 0,
            DefaultValueType::Value(..) => 0,
            DefaultValueType::Expression(v) => v.as_ref().precedence(writer),
        }
    }
}

impl Expression for DefaultValueType {
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        match self {
            DefaultValueType::None => ().write_query(writer, context, out),
            DefaultValueType::Value(v) => v.write_query(writer, context, out),
            DefaultValueType::Expression(v) => v.write_query(writer, context, out),
        }
    }
    fn matches(&self, matcher: &mut dyn ExpressionMatcher, writer: &dyn SqlWriter) -> bool {
        match self {
            DefaultValueType::None => ().matches(matcher, writer),
            DefaultValueType::Value(v) => v.matches(matcher, writer),
            DefaultValueType::Expression(v) => v.matches(matcher, writer),
        }
    }
}

impl From<Value> for DefaultValueType {
    fn from(value: Value) -> Self {
        Self::Value(value)
    }
}

impl From<bool> for DefaultValueType {
    fn from(value: bool) -> Self {
        Self::Value(Value::Boolean(Some(value)))
    }
}

impl From<&'static str> for DefaultValueType {
    fn from(value: &'static str) -> Self {
        Self::Value(Value::Varchar(Some(value.into())))
    }
}

impl From<i64> for DefaultValueType {
    fn from(value: i64) -> Self {
        Self::Value(Value::Int64(Some(value)))
    }
}
