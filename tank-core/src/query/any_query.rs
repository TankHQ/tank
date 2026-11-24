use crate::{Driver, Prepared, Query, RawQuery};
use std::{
    any::Any,
    borrow::Cow,
    fmt::{self, Write},
};

/// [`Query`] with the driver erased: raw SQL text, or a boxed prepared statement.
///
/// The driver is known again when it converts into a [`Query<D>`](Query), which downcasts
/// the prepared variant, so a query built for one driver becomes empty for another.
#[derive(Debug)]
pub enum AnyQuery {
    /// Raw SQL text.
    Raw(RawQuery),
    /// Prepared statement of an unknown driver.
    Prepared(Box<dyn Prepared>),
}

impl AnyQuery {
    pub const fn new(value: String) -> Self {
        Self::Raw(RawQuery(value))
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(String::with_capacity(capacity))
    }

    pub fn clear(&mut self) {
        match self {
            Self::Raw(RawQuery(value)) => value.clear(),
            Self::Prepared(..) => *self = Self::Raw(Default::default()),
        }
    }

    #[inline]
    pub fn buffer(&mut self) -> &mut String {
        match self {
            Self::Raw(RawQuery(value)) => value,
            Self::Prepared(..) => {
                let res = reset_to_raw(self);
                #[cold]
                #[inline(never)]
                fn reset_to_raw(query: &mut AnyQuery) -> &mut String {
                    log::error!(
                        "AnyQuery::buffer reset a prepared query to raw, discarding its prepared content"
                    );
                    loop {
                        if let AnyQuery::Raw(RawQuery(value)) = query {
                            return value;
                        }
                        *query = AnyQuery::Raw(Default::default());
                    }
                }
                res
            }
        }
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

    #[inline(always)]
    pub fn push_str(&mut self, s: &str) {
        self.buffer().push_str(s);
    }

    #[inline(always)]
    pub fn push(&mut self, c: char) {
        self.buffer().push(c);
    }

    #[inline(always)]
    pub const fn len(&self) -> usize {
        match self {
            Self::Raw(RawQuery(sql)) => sql.len(),
            Self::Prepared(..) => 0,
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Raw(RawQuery(v)) => v.is_empty(),
            Self::Prepared(v) => v.is_empty(),
        }
    }

    #[inline(always)]
    pub fn into_query<D: Driver>(self, _driver: D) -> Query<D> {
        self.into()
    }
}

impl Default for AnyQuery {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl Write for AnyQuery {
    #[inline]
    fn write_char(&mut self, c: char) -> fmt::Result {
        self.push(c);
        Ok(())
    }
    #[inline]
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.push_str(s);
        Ok(())
    }
}

impl<D: Driver> From<Query<D>> for AnyQuery {
    fn from(value: Query<D>) -> Self {
        match value {
            Query::Raw(v) => Self::Raw(v),
            Query::Prepared(p) => Self::Prepared(Box::new(p)),
        }
    }
}

impl<D: Driver> From<AnyQuery> for Query<D> {
    fn from(value: AnyQuery) -> Self {
        match value {
            AnyQuery::Raw(r) => Self::Raw(r),
            AnyQuery::Prepared(p) => match p.as_any().downcast::<D::Prepared>() {
                Ok(p) => Query::Prepared(*p),
                Err(..) => Query::raw(Default::default()),
            },
        }
    }
}

impl From<AnyQuery> for String {
    fn from(value: AnyQuery) -> Self {
        match value {
            AnyQuery::Raw(RawQuery(value)) => value,
            AnyQuery::Prepared(value) => format!("{:?}", value),
        }
    }
}
