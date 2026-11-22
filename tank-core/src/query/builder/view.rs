use crate::{
    ColumnDef, Dataset, Expression, InsertIntoQuery, SelectQuery, TableRef, UpdateQuery,
    future::Either,
};

/// A select of any concrete type, borrowed as `&dyn` parts so any driver can render it.
pub(crate) struct SelectView<'a> {
    select: &'a [&'a dyn Expression],
    from: Option<&'a dyn Dataset>,
    where_expr: Option<&'a dyn Expression>,
    group_by: &'a [&'a dyn Expression],
    having: Option<&'a dyn Expression>,
    order_by: &'a [&'a dyn Expression],
    limit: Option<u32>,
    offset: Option<u32>,
    distinct: bool,
}

impl SelectView<'_> {
    /// Calls `f` with a view of `query`, the expressions it yields by value live in this frame.
    pub(crate) fn with<Data: Dataset, R>(
        query: &impl SelectQuery<Data>,
        f: impl FnOnce(SelectView<'_>) -> R,
    ) -> R {
        let select = query.get_select().collect::<Vec<_>>();
        let group_by = query.get_group_by().collect::<Vec<_>>();
        let order_by = query.get_order_by().collect::<Vec<_>>();
        f(SelectView {
            select: &as_dyn(&select),
            from: query.get_from().as_ref().map(|v| v as &dyn Dataset),
            where_expr: query.get_where().as_ref().map(|v| v as &dyn Expression),
            group_by: &as_dyn(&group_by),
            having: query.get_having().as_ref().map(|v| v as &dyn Expression),
            order_by: &as_dyn(&order_by),
            limit: query.get_limit(),
            offset: query.get_offset(),
            distinct: query.is_distinct(),
        })
    }
}

impl<'a> SelectQuery<&'a dyn Dataset> for SelectView<'a> {
    fn get_select(&self) -> impl Iterator<Item = impl Expression> + Clone {
        self.select.iter().copied()
    }
    fn get_from(&self) -> &Option<&'a dyn Dataset> {
        &self.from
    }
    fn get_where(&self) -> &Option<impl Expression> {
        &self.where_expr
    }
    fn get_group_by(&self) -> impl Iterator<Item = impl Expression> + Clone {
        self.group_by.iter().copied()
    }
    fn get_having(&self) -> &Option<impl Expression> {
        &self.having
    }
    fn get_order_by(&self) -> impl Iterator<Item = impl Expression> + Clone {
        self.order_by.iter().copied()
    }
    fn get_limit(&self) -> Option<u32> {
        self.limit
    }
    fn get_offset(&self) -> Option<u32> {
        self.offset
    }
    fn is_distinct(&self) -> bool {
        self.distinct
    }
}

/// An insert of any concrete type: its rows as one flat list of cells split at `ends`.
pub(crate) struct InsertView<'a> {
    table: &'a TableRef,
    columns: &'a [ColumnDef],
    values: Either<(&'a [&'a dyn Expression], &'a [usize]), SelectView<'a>>,
    update: bool,
}

impl InsertView<'_> {
    /// Calls `f` with a view of `query`, the cells it yields by value live in this frame.
    pub(crate) fn with<Q: InsertIntoQuery, R>(query: &Q, f: impl FnOnce(InsertView<'_>) -> R) -> R {
        let (table, columns, update) = (query.get_table(), query.get_columns(), query.get_update());
        match query.get_values() {
            Either::Left(rows) => {
                let mut cells = Vec::new();
                let mut ends = Vec::new();
                for row in rows {
                    cells.extend(row);
                    ends.push(cells.len());
                }
                f(InsertView {
                    table,
                    columns,
                    values: Either::Left((&as_dyn(&cells), &ends)),
                    update,
                })
            }
            Either::Right(select) => SelectView::with(select, |select| {
                f(InsertView {
                    table,
                    columns,
                    values: Either::Right(select),
                    update,
                })
            }),
        }
    }
}

impl<'a> InsertIntoQuery for InsertView<'a> {
    type From = &'a dyn Dataset;

    fn get_table(&self) -> &TableRef {
        self.table
    }
    fn get_columns(&self) -> &[ColumnDef] {
        self.columns
    }
    fn get_values(
        &self,
    ) -> Either<
        impl Iterator<Item = impl Iterator<Item = impl Expression>>,
        &impl SelectQuery<Self::From>,
    > {
        match &self.values {
            Either::Left((cells, ends)) => Either::Left(ends.iter().scan(0, |start, &end| {
                let row = cells.get(*start..end).unwrap_or_default();
                *start = end;
                Some(row.iter().copied())
            })),
            Either::Right(select) => Either::Right(select),
        }
    }
    fn get_update(&self) -> bool {
        self.update
    }
}

/// An update of any concrete type, borrowed as `&dyn` parts.
pub(crate) struct UpdateView<'a> {
    from: &'a dyn Dataset,
    set: &'a [&'a dyn Expression],
    where_expr: Option<&'a dyn Expression>,
}

impl UpdateView<'_> {
    /// Calls `f` with a view of `query`, the assignments it yields by value live in this frame.
    pub(crate) fn with<Data: Dataset, R>(
        query: &impl UpdateQuery<Data>,
        f: impl FnOnce(UpdateView<'_>) -> R,
    ) -> R {
        let set = query.get_set().collect::<Vec<_>>();
        f(UpdateView {
            from: query.get_from(),
            set: &as_dyn(&set),
            where_expr: query.get_where().as_ref().map(|v| v as &dyn Expression),
        })
    }
}

impl<'a> UpdateQuery<&'a dyn Dataset> for UpdateView<'a> {
    fn get_from(&self) -> &&'a dyn Dataset {
        &self.from
    }
    fn get_set(&self) -> impl Iterator<Item = impl Expression> + Clone {
        self.set.iter().copied()
    }
    fn get_where(&self) -> &Option<impl Expression> {
        &self.where_expr
    }
}

fn as_dyn<T: Expression>(items: &[T]) -> Vec<&dyn Expression> {
    items.iter().map(|v| v as &dyn Expression).collect()
}
