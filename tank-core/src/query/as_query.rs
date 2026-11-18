use crate::{AnyQuery, BoxQueryBuilder, Driver, Query, RawQuery};

pub trait AsQuery<D: Driver> {
    type Output: AsMut<Query<D>> + Send;
    fn as_query(self, driver: &D) -> Self::Output;
}

impl<D: Driver> AsQuery<D> for Query<D> {
    type Output = Query<D>;
    fn as_query(self, _driver: &D) -> Self::Output {
        self
    }
}

impl<'q, D: Driver + 'q> AsQuery<D> for &'q mut Query<D> {
    type Output = &'q mut Query<D>;
    fn as_query(self, _driver: &D) -> Self::Output {
        self
    }
}

impl<D: Driver> AsQuery<D> for AnyQuery {
    type Output = Query<D>;
    fn as_query(self, _driver: &D) -> Self::Output {
        match self {
            AnyQuery::Raw(v) => Query::Raw(v),
            AnyQuery::Prepared(p) => match p.as_any().downcast::<D::Prepared>() {
                Ok(p) => Query::Prepared(*p),
                Err(..) => Query::raw(Default::default()),
            },
        }
    }
}

impl<D: Driver> AsQuery<D> for RawQuery {
    type Output = Query<D>;
    fn as_query(self, _driver: &D) -> Self::Output {
        Query::Raw(self)
    }
}

impl<D: Driver> AsQuery<D> for String {
    type Output = Query<D>;
    fn as_query(self, _driver: &D) -> Self::Output {
        Query::raw(self)
    }
}

impl<D: Driver> AsQuery<D> for &str {
    type Output = Query<D>;
    fn as_query(self, _driver: &D) -> Self::Output {
        Query::raw(self.into())
    }
}

/// A driver agnostic builder, rendered by the executor's driver.
impl<D: Driver + 'static> AsQuery<D> for &BoxQueryBuilder {
    type Output = Query<D>;
    fn as_query(self, driver: &D) -> Self::Output {
        self.build(driver)
    }
}
