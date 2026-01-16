use crate::{DynQuery, Driver, Query, RawQuery};

pub trait AsQuery<D: Driver> {
    type Output: AsMut<Query<D>> + Send;
    fn as_query(self) -> Self::Output;
}

impl<D: Driver> AsQuery<D> for Query<D> {
    type Output = Query<D>;
    fn as_query(self) -> Self::Output {
        self
    }
}

impl<'q, D: Driver + 'q> AsQuery<D> for &'q mut Query<D> {
    type Output = &'q mut Query<D>;
    fn as_query(self) -> Self::Output {
        self
    }
}

impl<D: Driver> AsQuery<D> for DynQuery {
    type Output = Query<D>;
    fn as_query(self) -> Self::Output {
        match self {
            DynQuery::Raw(v) => Query::Raw(v),
            DynQuery::Prepared(p) => match p.as_any().downcast::<D::Prepared>() {
                Ok(p) => Query::Prepared(*p),
                Err(..) => Query::raw(Default::default()),
            },
        }
    }
}

impl<D: Driver> AsQuery<D> for RawQuery {
    type Output = Query<D>;
    fn as_query(self) -> Self::Output {
        Query::Raw(self)
    }
}

impl<D: Driver> AsQuery<D> for String {
    type Output = Query<D>;
    fn as_query(self) -> Self::Output {
        Query::raw(self)
    }
}

impl<D: Driver> AsQuery<D> for &str {
    type Output = Query<D>;
    fn as_query(self) -> Self::Output {
        Query::raw(self.into())
    }
}
