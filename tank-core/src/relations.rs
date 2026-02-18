use crate::{AsValue, ColumnDef, Entity, TableRef};
use rust_decimal::Decimal;
use std::{
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem,
};

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

/// Value wrapper for optional persistence.
///
/// - `Set(T)`: Value is actively written.
/// - `NotSet`: Value is omitted (DB default used).
#[derive(Debug, Default)]
pub enum Passive<T: AsValue> {
    /// Active value.
    Set(T),
    /// Omitted value.
    #[default]
    NotSet,
}

impl<T: AsValue> Passive<T> {
    pub fn new(value: T) -> Self {
        Passive::Set(value)
    }
    pub fn expect(self, msg: &str) -> T {
        match self {
            Passive::Set(v) => v,
            Passive::NotSet => panic!("{msg}"),
        }
    }
    pub fn unwrap(self) -> T {
        match self {
            Passive::Set(v) => v,
            Passive::NotSet => panic!("called `Passive::unwrap()` on a `NotSet` value"),
        }
    }
}

impl<T: PartialEq + AsValue> PartialEq for Passive<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Set(lhs), Self::Set(rhs)) => lhs == rhs,
            _ => mem::discriminant(self) == mem::discriminant(other),
        }
    }
}

impl<T: Eq + AsValue> Eq for Passive<T> {}

impl<T: Clone + AsValue> Clone for Passive<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        match self {
            Self::Set(v) => Self::Set(v.clone()),
            Self::NotSet => Self::NotSet,
        }
    }
}

impl<T: AsValue> From<T> for Passive<T> {
    fn from(value: T) -> Self {
        Self::Set(value)
    }
}

impl<T: Hash + AsValue> Hash for Passive<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        mem::discriminant(self).hash(state);
        if let Passive::Set(v) = self {
            v.hash(state);
        }
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
