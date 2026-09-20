use anyhow::anyhow;
use chdb_rust::connection::Connection as ChConnection;
use std::{ffi::c_char, slice};
use tank_core::{Result, error_message_from_ptr};

/// Opaque handles from `chdb.h`.
enum ChdbConnection {}
enum ChdbResult {}

unsafe extern "C" {
    fn chdb_stream_query_n(
        connection: *mut ChdbConnection,
        query: *const c_char,
        query_len: usize,
        format: *const c_char,
        format_len: usize,
    ) -> *mut ChdbResult;
    fn chdb_stream_fetch_result(
        connection: *mut ChdbConnection,
        result: *mut ChdbResult,
    ) -> *mut ChdbResult;
    fn chdb_stream_cancel_query(connection: *mut ChdbConnection, result: *mut ChdbResult);
    fn chdb_result_buffer(result: *mut ChdbResult) -> *mut c_char;
    fn chdb_result_length(result: *mut ChdbResult) -> usize;
    fn chdb_result_error(result: *mut ChdbResult) -> *const c_char;
    fn chdb_destroy_query_result(result: *mut ChdbResult);
}

pub(crate) struct ChDBStream {
    connection: *mut ChdbConnection,
    result: *mut ChdbResult,
    finished: bool,
}

unsafe impl Send for ChDBStream {}

impl ChDBStream {
    pub(crate) fn start(connection: &ChConnection, sql: &str) -> Result<Self> {
        // `chdb_rust::Connection` wraps `*mut *mut chdb_connection_` in a private field.
        let connection: *mut ChdbConnection =
            unsafe { **(connection as *const ChConnection).cast::<*mut *mut ChdbConnection>() };
        let sql = sql.trim().trim_end_matches(';').trim_end();
        let format = b"JSONEachRow";
        eprintln!("[tank] chDB chdb_stream_query_n -> conn={connection:p} sql={sql:?}");
        let result = unsafe {
            chdb_stream_query_n(
                connection,
                sql.as_ptr().cast(),
                sql.len(),
                format.as_ptr().cast(),
                format.len(),
            )
        };
        if result.is_null() {
            return Err(anyhow!("chDB streaming query returned no result"));
        }
        eprintln!("[tank] chDB chdb_stream_query_n <- result={result:p}");
        let error = unsafe { chdb_result_error(result) };
        if !error.is_null() {
            let error = anyhow!("chDB streaming query failed: {}", error_message_from_ptr(&error));
            unsafe { chdb_destroy_query_result(result) };
            return Err(error);
        }
        Ok(Self {
            connection,
            result,
            finished: false,
        })
    }

    pub(crate) fn next(&mut self) -> Result<Option<ChDBChunk>> {
        if self.finished {
            return Ok(None);
        }
        eprintln!("[tank] chDB chdb_stream_fetch_result -> handle={:p}", self.result);
        let chunk = unsafe { chdb_stream_fetch_result(self.connection, self.result) };
        eprintln!("[tank] chDB chdb_stream_fetch_result <- chunk={chunk:p}");
        if chunk.is_null() {
            self.finished = true;
            return Ok(None);
        }
        let error = unsafe { chdb_result_error(chunk) };
        if !error.is_null() {
            let error = anyhow!("chDB streaming fetch failed: {}", error_message_from_ptr(&error));
            unsafe { chdb_destroy_query_result(chunk) };
            self.finished = true;
            return Err(error);
        }
        let length = unsafe { chdb_result_length(chunk) };
        eprintln!("[tank] chDB chunk len={length}");
        if length == 0 {
            unsafe { chdb_destroy_query_result(chunk) };
            self.finished = true;
            return Ok(None);
        }
        Ok(Some(ChDBChunk { result: chunk }))
    }
}

impl Drop for ChDBStream {
    fn drop(&mut self) {
        if self.result.is_null() {
            return;
        }
        if !self.finished {
            unsafe { chdb_stream_cancel_query(self.connection, self.result) };
        }
        unsafe { chdb_destroy_query_result(self.result) };
    }
}

pub(crate) struct ChDBChunk {
    result: *mut ChdbResult,
}

impl ChDBChunk {
    pub(crate) fn data(&self) -> &[u8] {
        let buffer = unsafe { chdb_result_buffer(self.result) };
        let length = unsafe { chdb_result_length(self.result) };
        if buffer.is_null() || length == 0 {
            return &[];
        }
        unsafe { slice::from_raw_parts(buffer.cast(), length) }
    }
}

impl Drop for ChDBChunk {
    fn drop(&mut self) {
        unsafe { chdb_destroy_query_result(self.result) };
    }
}
