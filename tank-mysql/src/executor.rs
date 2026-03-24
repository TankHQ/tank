use crate::{MySQLDriver, MySQLPrepared, RowWrap, local_infile::Registry};
use async_stream::try_stream;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tank_core::{
    AsQuery, Context, Driver, DynQuery, Entity, Error, Executor, Query, RawQuery, Result,
    RowsAffected, SqlWriter,
    stream::{Stream, StreamExt, TryStreamExt},
};
use tokio::io::AsyncWriteExt;

static INFILE_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) struct MySQLQueryable<T: mysql_async::prelude::Queryable> {
    pub(crate) executor: T,
    pub(crate) registry: Registry,
}

impl<T: mysql_async::prelude::Queryable> Executor for MySQLQueryable<T> {
    type Driver = MySQLDriver;

    async fn do_prepare(&mut self, sql: String) -> Result<Query<MySQLDriver>> {
        Ok(MySQLPrepared::new(self.executor.prep(sql.as_str()).await?).into())
    }

    fn run<'s>(
        &'s mut self,
        query: impl AsQuery<MySQLDriver> + 's,
    ) -> impl Stream<Item = Result<tank_core::QueryResult>> + Send {
        let mut query = query.as_query();
        let context = Arc::new(format!("While running the query:\n{}", query.as_mut()));
        try_stream! {
            match query.as_mut() {
                Query::Raw(RawQuery(sql)) => {
                    let mut result = self.executor.query_iter(sql.as_str()).await?;
                    let mut rows = 0;
                    while let Some(mut stream) = result.stream::<RowWrap>().await? {
                        while let Some(row) = stream.next().await.transpose()? {
                            rows += 1;
                            yield tank_core::QueryResult::Row(row.0)
                        }
                    }
                    let affected = result.affected_rows();
                    if rows == 0 && affected > 0 {
                        yield tank_core::QueryResult::Affected(tank_core::RowsAffected {
                            rows_affected: Some(affected),
                            last_affected_id: result.last_insert_id().map(|v| v as _),
                        });
                    }
                }
                Query::Prepared(prepared) => {
                    let params = prepared.take_params()?;
                    let mut stream = self
                        .executor
                        .exec_stream::<RowWrap, _, _>(&prepared.statement, params)
                        .await?;
                    while let Some(row) = stream.next().await.transpose()? {
                        yield row.0.into()
                    }
                }
            }
        }
        .map_err(move |e: Error| {
            let error = e.context(context.clone());
            log::error!("{:#}", error);
            error
        })
    }

    async fn append<'a, E, It>(&mut self, entities: It) -> Result<RowsAffected>
    where
        E: Entity + 'a,
        It: IntoIterator<Item = &'a E> + Send,
        <It as IntoIterator>::IntoIter: Send,
    {
        let entities = entities.into_iter();
        let writer_tool = self.driver().sql_writer();
        let table = E::table();
        let table_name = table.name.clone();

        let mut column_names = Vec::new();
        for col in E::columns() {
            let mut q = DynQuery::default();
            writer_tool.write_identifier(&mut Context::default(), &mut q, col.name(), true);
            column_names.push(q.as_str().to_string());
        }
        let columns_sql = column_names.join(",");

        let id = format!(
            "{}-{}",
            table_name,
            INFILE_ID.fetch_add(1, Ordering::Relaxed)
        );
        let (reader, mut writer) = tokio::io::duplex(1024 * 1024);

        self.registry.register(id.clone(), Box::new(reader));

        let write_fut = async move {
            for entity in entities {
                let row = entity.row_values();
                for (i, val) in row.into_iter().enumerate() {
                    if i > 0 {
                        writer.write_u8(b'\t').await?;
                    }
                    let value = crate::sql_writer::MySQLSqlWriter::encode_load_data_value(&val);
                    writer.write_all(value.as_bytes()).await?;
                }
                writer.write_u8(b'\n').await?;
            }
            Ok::<_, std::io::Error>(())
        };

        // Quote table name?
        let mut query = DynQuery::default();
        query.push_str("LOAD DATA LOCAL INFILE '");
        query.push_str(&id);
        query.push_str("' INTO TABLE ");
        // qualified table name
        writer_tool.write_table_ref(&mut Context::default(), &mut query, table);
        query.push_str(" (");
        query.push_str(&columns_sql);
        query.push_str(")");

        let sql = query.as_str().to_string();

        let load_fut = async { self.executor.query_drop(sql).await };

        let (res_load, res_write) = tokio::join!(load_fut, write_fut);

        if let Err(e) = res_write {
            return Err(Error::new(e).context("While writing to LOAD DATA INFILE stream"));
        }
        res_load?;

        Ok(RowsAffected::default())
    }
}
