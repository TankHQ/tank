use anyhow::anyhow;
use chdb_rust::connection::Connection as ChConnection;
use std::{
    ffi::{CStr, c_char, c_void},
    slice,
};
use tank_core::Result;

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
    fn query_conn_streaming_n(
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
        let connection = raw_connection(connection);
        let sql = sql.trim().trim_end_matches(';').trim_end();
        let format = b"JSONEachRow";
        let result = unsafe {
            query_conn_streaming_n(
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
        if let Some(error) = streaming_result_error(result) {
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
        if let Some(error) = chunk_error(chunk) {
            unsafe { free_result_v2(chunk) };
            self.finished = true;
            return Err(anyhow!("chDB streaming fetch failed: {error}"));
        }
        let length = unsafe { (*chunk).length };
        if length == 0 {
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

fn streaming_result_error(result: *mut ChdbStreamingResult) -> Option<String> {
    let error = unsafe { chdb_streaming_result_error(result) };
    error_string(error)
}

fn chunk_error(result: *mut ChdbResult) -> Option<String> {
    let error = unsafe { (*result).error_message };
    error_string(error.cast_const())
}

fn error_string(error: *const c_char) -> Option<String> {
    if error.is_null() {
        return None;
    }
    let error = unsafe { CStr::from_ptr(error) }.to_string_lossy();
    (!error.is_empty()).then(|| error.into_owned())
}

fn raw_connection(connection: &ChConnection) -> *mut ChdbConnection {
    // chdb-rust::Connection is a one-field wrapper around the C connection handle,
    // but does not expose that handle or the streaming C API yet.
    unsafe { **(connection as *const ChConnection).cast::<*mut *mut ChdbConnection>() }
}
