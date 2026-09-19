use anyhow::anyhow;
use chdb_rust::connection::Connection as ChConnection;
use std::{
    ffi::{c_char, c_void},
    slice,
};
use tank_core::{Result, error_message_from_ptr};

type ChdbConnection = c_void;
type ChdbStreamingResult = c_void;

#[repr(C)]
struct ChdbResult {
    buffer: *mut c_char,
    length: usize,
    _vec: *mut c_void,
    _elapsed: f64,
    _rows_read: u64,
    _bytes_read: u64,
    error_message: *mut c_char,
}

unsafe extern "C" {
    fn chdb_stream_query_n(
        connection: *mut ChdbConnection,
        query: *const c_char,
        query_len: usize,
        format: *const c_char,
        format_len: usize,
    ) -> *mut ChdbStreamingResult;
    fn chdb_streaming_result_error(result: *mut ChdbStreamingResult) -> *const c_char;
    fn chdb_streaming_fetch_result(
        connection: *mut ChdbConnection,
        result: *mut ChdbStreamingResult,
    ) -> *mut ChdbResult;
    fn chdb_streaming_cancel_query(
        connection: *mut ChdbConnection,
        result: *mut ChdbStreamingResult,
    );
    fn chdb_destroy_result(result: *mut ChdbStreamingResult);
    fn free_result_v2(result: *mut ChdbResult);
}

pub(crate) struct ChDBStream {
    connection: *mut ChdbConnection,
    result: *mut ChdbStreamingResult,
    finished: bool,
}

unsafe impl Send for ChDBStream {}

impl ChDBStream {
    pub(crate) fn start(connection: &ChConnection, sql: &str) -> Result<Self> {
        // chdb-rust::Connection is a one-field wrapper around the C connection handle
        let connection =
            unsafe { **(connection as *const ChConnection).cast::<*mut *mut ChdbConnection>() };
        let sql = sql.trim().trim_end_matches(';').trim_end();
        let format = b"JSONEachRow";
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
        let error = unsafe { chdb_result_error(result) };
        if let Some(error) = error_string(error) {
            unsafe { chdb_destroy_result(result) };
            return Err(anyhow!("chDB streaming query failed: {error}"));
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
        let chunk = unsafe { chdb_streaming_fetch_result(self.connection, self.result) };
        if chunk.is_null() {
            self.finished = true;
            return Ok(None);
        }
        let error = unsafe { (*chunk).error_message.cast_const() };
        if let Some(error) = error_message_from_ptr(&error) {
            unsafe { free_result_v2(chunk) };
            self.finished = true;
            return Err(anyhow!("chDB streaming fetch failed: {error}"));
        }
        if unsafe { (*chunk).length } == 0 {
            unsafe { free_result_v2(chunk) };
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
            unsafe { chdb_streaming_cancel_query(self.connection, self.result) };
        }
        unsafe { chdb_destroy_result(self.result) };
    }
}

pub(crate) struct ChDBChunk {
    result: *mut ChdbResult,
}

impl ChDBChunk {
    pub(crate) fn data(&self) -> &[u8] {
        let buffer = unsafe { (*self.result).buffer };
        let length = unsafe { (*self.result).length };
        if buffer.is_null() || length == 0 {
            return &[];
        }
        unsafe { slice::from_raw_parts(buffer.cast(), length) }
    }
}

impl Drop for ChDBChunk {
    fn drop(&mut self) {
        unsafe { free_result_v2(self.result) };
    }
}
