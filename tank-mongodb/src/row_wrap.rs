use crate::{bson_to_value, value_to_bson};
use mongodb::bson::Document;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::Error as _,
    ser::{Error as _, SerializeMap},
};
use std::borrow::Cow;
use tank_core::RowLabeled;

pub(crate) struct RowWrap<'a>(pub(crate) Cow<'a, RowLabeled>);

impl<'a> TryFrom<Document> for RowWrap<'a> {
    type Error = tank_core::Error;
    fn try_from(value: Document) -> tank_core::Result<Self> {
        let mut pairs = Vec::with_capacity(value.len());
        let mut id_pair = None;
        for (k, v) in value {
            let val = bson_to_value(&v)?;
            if k == "_id" {
                id_pair = Some((k, val));
            } else {
                pairs.push((k, val));
            }
        }
        if let Some(pair) = id_pair {
            pairs.push(pair);
        }
        let (labels, values): (Vec<_>, Vec<_>) = pairs.into_iter().unzip();
        Ok(RowWrap(Cow::Owned(RowLabeled {
            labels: labels.into(),
            values: values.into(),
        })))
    }
}

impl<'a> TryFrom<RowWrap<'a>> for Document {
    type Error = tank_core::Error;
    fn try_from(value: RowWrap<'a>) -> Result<Self, Self::Error> {
        let mut result = Document::new();
        for (k, v) in value.0.as_ref() {
            result.insert(k, value_to_bson(v)?);
        }
        Ok(result)
    }
}

impl<'a> Serialize for RowWrap<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in self.0.as_ref() {
            state.serialize_entry(
                k,
                &value_to_bson(v).map_err(|e| S::Error::custom(format!("{e}")))?,
            )?;
        }
        state.end()
    }
}

impl<'a, 'd> Deserialize<'d> for RowWrap<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'d>,
    {
        let doc: Document = Deserialize::deserialize(deserializer)?;
        Ok(doc
            .try_into()
            .map_err(|e| D::Error::custom(format!("{e}")))?)
    }
}
