use crate::extract_value;
use mysql_async::FromRowError;

pub(crate) struct RowWrap(pub(crate) tank_core::RowOwned);

impl mysql_async::prelude::FromRow for RowWrap {
    fn from_row_opt(mut row: mysql_async::Row) -> Result<Self, mysql_async::FromRowError>
    where
        Self: Sized,
    {
        let columns = row.columns();
        let names: tank_core::RowLabels =
            columns.iter().map(|v| v.name_str().into_owned()).collect();
        let values = (0..row.len())
            .map(|i| {
                let Some(value) = row.take::<mysql_async::Value, _>(i) else {
                    log::error!("Unexpected error, column {i} is not present");
                    return Err(());
                };
                let Some(column) = columns.get(i) else {
                    log::error!("Unexpected error, column {i} has no matching column metadata");
                    return Err(());
                };
                extract_value(column, value)
                    .map(|v| v.0.into_owned())
                    .map_err(|e| {
                        log::error!("Could not decode MySQL column `{}`: {e}", column.name_str());
                    })
            })
            .collect::<Result<Vec<_>, ()>>()
            .map_err(|_| FromRowError(row))?;
        Ok(RowWrap(tank_core::RowOwned::new(names, values)))
    }
}
