use crate::{
    native::{NativeConnection, NativeResult},
    parse_result,
};
use anyhow::{anyhow, ensure};
use std::sync::Arc;
use tank_core::{QueryResult, Result, RowOwned};
use tokio::sync::{Mutex, OwnedMutexGuard};

pub struct ChDBRows {
    stream: Option<NativeStream>,
    rows: std::vec::IntoIter<RowOwned>,
    context: String,
    schema_seen: bool,
}

struct NativeStream {
    handle: NativeResult,
    connection: OwnedMutexGuard<NativeConnection>,
}

impl Drop for NativeStream {
    fn drop(&mut self) {
        // Cancel before destroying the handle, including on parse errors and
        // early rowset/future drop. Keep exclusive access until both are gone.
        self.connection.cancel(&self.handle);
    }
}

pub(crate) fn execute(
    connection: Arc<Mutex<NativeConnection>>,
    directive: Option<&str>,
    sql: &str,
    context: String,
) -> Result<QueryResult<ChDBRows>> {
    let mut native = connection.blocking_lock_owned();
    if let Some(directive) = directive {
        native.query(directive, c"Null")?;
    }
    let handle = native.stream(sql)?;
    if let Some(error) = handle.error() {
        // chDB 26.5 ClientBase::processTextAsSingleQuery rejects non-SELECT
        // ASTs here, before processParsedSingleQuery. No other error may retry.
        let mut rejection =
            format!("Code: 36. DB::Exception: Streaming query is not supported for query: {sql}");
        if !rejection.ends_with('.') {
            rejection.push('.');
        }
        rejection.push_str(" (BAD_ARGUMENTS)");
        ensure!(
            error == rejection,
            "chDB streaming initialization failed: {error}"
        );
        drop(handle);
        let result = native.query(sql, c"JSONCompact")?;
        return Ok(match parse_result(result.data()?)? {
            QueryResult::Affected(affected) => QueryResult::Affected(affected),
            QueryResult::Rows(rows) => QueryResult::Rows(ChDBRows {
                stream: None,
                rows,
                context,
                schema_seen: true,
            }),
        });
    }
    let mut rows = ChDBRows {
        stream: Some(NativeStream {
            connection: native,
            handle,
        }),
        rows: Vec::new().into_iter(),
        context,
        schema_seen: false,
    };
    // In 26.5 cancel-before-first-fetch hangs. Prime exactly one native chunk
    // before transferring ownership, so even an unconsumed rowset can cancel.
    rows.fetch_chunk()?;
    Ok(QueryResult::Rows(rows))
}

impl ChDBRows {
    fn fetch_chunk(&mut self) -> Result<()> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(());
        };
        let chunk = (|| -> Result<_> {
            let chunk = stream.connection.fetch(&stream.handle)?;
            if chunk.data()?.is_empty() {
                ensure!(
                    self.schema_seen && chunk.is_finished(),
                    "chDB SELECT ended without a schema-bearing result"
                );
                return Ok(None);
            }
            let QueryResult::Rows(rows) = parse_result(chunk.data()?)? else {
                return Err(anyhow!("chDB SELECT chunk is missing its schema"));
            };
            Ok(Some((rows, chunk.is_finished())))
        })();
        match chunk {
            Ok(Some((rows, finished))) => {
                self.schema_seen = true;
                self.rows = rows;
                if finished {
                    self.stream.take();
                }
            }
            Ok(None) => {
                self.stream.take();
            }
            Err(error) => {
                self.stream.take();
                return Err(error.context(self.context.clone()));
            }
        }
        Ok(())
    }
}

impl Iterator for ChDBRows {
    type Item = Result<RowOwned>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.rows.next() {
                return Some(Ok(row));
            }
            self.stream.as_ref()?;
            if let Err(error) = self.fetch_chunk() {
                return Some(Err(error));
            }
        }
    }
}
