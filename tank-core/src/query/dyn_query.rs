use crate::{Driver, Prepared, Query, RawQuery};
use std::{
    any::Any,
    borrow::Cow,
    fmt::{self, Write},
    mem,
};

/// Dyn compatible version of `Query`
pub enum DynQuery {
    Raw(RawQuery),
    Prepared(Box<dyn Prepared>),
}

impl DynQuery {
    pub fn new(value: String) -> Self {
        Self::Raw(RawQuery(value))
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(String::with_capacity(capacity))
    }
    pub fn buffer(&mut self) -> &mut String {
        if !matches!(self, Self::Raw(..)) {
            log::error!("DynQuery::buffer changed the query to raw, deleting the previous content");
            *self = Self::Raw(Default::default())
        }
        let Self::Raw(RawQuery(value)) = self else {
            unreachable!();
        };
        value
    }
    pub fn as_prepared<D: Driver>(&mut self) -> Option<&mut D::Prepared> {
        if let Self::Prepared(prepared) = self {
            return (&mut **prepared as &mut dyn Any).downcast_mut::<D::Prepared>();
        }
        None
    }
    pub fn as_str<'s>(&'s self) -> Cow<'s, str> {
        match self {
            Self::Raw(RawQuery(sql)) => Cow::Borrowed(sql),
            Self::Prepared(v) => Cow::Owned(format!("{:?}", *v)),
        }
    }
    pub fn push_str(&mut self, s: &str) {
        self.buffer().push_str(s);
    }
    pub fn push(&mut self, c: char) {
        self.buffer().push(c);
    }
    pub fn len(&self) -> usize {
        match self {
            Self::Raw(RawQuery(sql)) => sql.len(),
            Self::Prepared(..) => 0,
        }
    }
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Raw(RawQuery(sql)) => sql.is_empty(),
            Self::Prepared(..) => true,
        }
    }

    pub fn into_query<D: Driver>(self, _driver: D) -> Query<D> {
        self.into()
    }
}

impl Default for DynQuery {
    fn default() -> Self {
        Self::Raw(Default::default())
    }
}

impl Write for DynQuery {
    fn write_char(&mut self, c: char) -> fmt::Result {
        self.push(c);
        Ok(())
    }
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.push_str(s);
        Ok(())
    }
}

impl<D: Driver> From<Query<D>> for DynQuery {
    fn from(value: Query<D>) -> Self {
        match value {
            Query::Raw(v) => Self::Raw(v),
            Query::Prepared(p) => Self::Prepared(Box::new(p)),
        }
    }
}

impl<D: Driver> From<DynQuery> for Query<D> {
    fn from(value: DynQuery) -> Self {
        match value {
            DynQuery::Raw(r) => Self::Raw(r),
            DynQuery::Prepared(p) => match p.as_any().downcast::<D::Prepared>() {
                Ok(p) => Query::Prepared(*p),
                Err(..) => Query::raw(Default::default()),
            },
        }
    }
}

impl From<DynQuery> for String {
    fn from(mut value: DynQuery) -> Self {
        mem::take(value.buffer())
    }
}
