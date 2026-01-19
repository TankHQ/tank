use crate::{bson_to_value, value_to_bson};
use mongodb::bson::Bson;
use std::borrow::Cow;
use tank_core::Error;

pub(crate) struct ValueWrap<'a>(pub(crate) Cow<'a, tank_core::Value>);

impl<'a> TryFrom<ValueWrap<'a>> for Bson {
    type Error = Error;
    fn try_from(value: ValueWrap<'a>) -> Result<Self, Self::Error> {
        value_to_bson(&value.0)
    }
}

impl<'a> TryFrom<&Bson> for ValueWrap<'a> {
    type Error = Error;
    fn try_from(value: &Bson) -> Result<Self, Self::Error> {
        bson_to_value(value).map(|v| ValueWrap(Cow::Owned(v)))
    }
}
