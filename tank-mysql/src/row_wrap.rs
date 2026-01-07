use crate::ValueWrap;
use mysql_async::FromRowError;

pub(crate) struct RowWrap(pub(crate) tank_core::RowLabeled);

impl mysql_async::prelude::FromRow for RowWrap {
    fn from_row_opt(mut row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        let names: tank_core::RowNames = row
            .columns()
            .iter()
            .map(|v| v.name_str().into_owned())
            .collect();
        let values: tank_core::Row = (0..row.len())
            .map(|i| {
                row.take_opt::<ValueWrap, _>(i)
                    .expect("Unexpected error: the column does not exist")
                    .map(|v| v.take_value())
            })
            .collect::<Result<_, _>>()
            .map_err(|_| FromRowError(row))?;
        Ok(RowWrap(tank_core::RowLabeled::new(names, values)))
    }
}
