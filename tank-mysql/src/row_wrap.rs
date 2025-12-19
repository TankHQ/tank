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
        let mut values_vec: tank_core::Row = Vec::with_capacity(row.len());
        for i in 0..row.len() {
            match row.take_opt::<ValueWrap, _>(i) {
                Ok(opt) => values_vec.push(opt.map(|v| v.0).unwrap_or(tank_core::Value::Null)),
                Err(_) => return Err(FromRowError(row)),
            }
        }
        let values: tank_core::Row = values_vec.into_iter().collect();
        Ok(RowWrap(tank_core::RowLabeled::new(names, values)))
    }
}
