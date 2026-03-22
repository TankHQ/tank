use crate::{ColumnDef, Entity, TableRef};
use rust_decimal::Decimal;
use std::{hash::Hash, marker::PhantomData};

/// Decimal wrapper enforcing static precision/scale.
///
/// `WIDTH` = total digits, `SCALE` = fractional digits.
#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct FixedDecimal<const WIDTH: u8, const SCALE: u8>(pub Decimal);

impl<const W: u8, const S: u8> From<Decimal> for FixedDecimal<W, S> {
    fn from(value: Decimal) -> Self {
        Self(value)
    }
}

impl<const W: u8, const S: u8> From<FixedDecimal<W, S>> for Decimal {
    fn from(value: FixedDecimal<W, S>) -> Self {
        value.0
    }
}

/// Represents a foreign key constraint to another entity.
pub struct References<T: Entity> {
    entity: PhantomData<T>,
    columns: Box<[ColumnDef]>,
}

impl<T: Entity> References<T> {
    pub fn new(columns: Box<[ColumnDef]>) -> Self {
        Self {
            columns,
            entity: Default::default(),
        }
    }
    pub fn table_ref(&self) -> TableRef {
        T::table().clone()
    }
    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }
}
