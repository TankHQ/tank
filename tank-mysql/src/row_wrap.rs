use crate::extract_value;
use mysql_async::FromRowError;

pub(crate) struct RowWrap(pub(crate) tank_core::Row);

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
                let value = row
                    .take::<mysql_async::Value, _>(i)
                    .expect("Unexpected error: the column does not exist");
                extract_value(&columns[i], value)
                    .map(|v| v.0.into_owned())
                    .map_err(|_| ())
            })
            .collect::<Result<tank_core::RowValues, ()>>()
            .map_err(|_| FromRowError(row))?;
        Ok(RowWrap(tank_core::Row::new(names, values)))
    }
}
