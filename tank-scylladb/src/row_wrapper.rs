use crate::ValueWrap;
use scylla::{
    deserialize::{
        row::{ColumnIterator, DeserializeRow},
        value::DeserializeValue,
    },
    errors::{DeserializationError, TypeCheckError},
    frame::response::result::ColumnSpec,
};
use tank_core::RowLabeled;

pub(crate) struct RowWrap(pub(crate) RowLabeled);

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for RowWrap {
    fn type_check(_specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        Ok(())
    }
    fn deserialize(row: ColumnIterator<'frame, 'metadata>) -> Result<Self, DeserializationError> {
        let names = row
            .clone()
            .map(|v| v.map(|v| v.spec.name().to_string()))
            .collect::<Result<_, _>>()?;
        let values = row
            .map(|v| v.map(|v| ValueWrap::deserialize(v.spec.typ(), v.slice).map(|v| v.0)))
            .collect::<Result<_, _>>()
            .flatten()?;
        Ok(RowWrap(RowLabeled::new(names, values)))
    }
}
