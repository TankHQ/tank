use anyhow::anyhow;
// Retain the dependency's native link directives, including the bundled feature.
use chdb_rust as _;
use std::ffi::{CStr, c_char, c_int, c_void};
use tank_core::Error;

pub(crate) const QUERY_CLASS_READ_ONLY: u32 = 0;

pub(crate) type QueryWithParamsFn = unsafe extern "C" fn(
    *mut c_void,
    *const c_char,
    usize,
    *const c_char,
    usize,
    *const *const c_char,
    *const usize,
    *const *const c_char,
    *const usize,
    usize,
) -> *mut c_void;

#[repr(C)]
pub(crate) struct QueryAnalysis {
    pub(crate) struct_size: u32,
    pub(crate) statement_count: u32,
    pub(crate) flags: u32,
    pub(crate) query_class: u32,
}

unsafe extern "C" {
    pub(crate) fn chdb_connect(argc: c_int, argv: *mut *mut c_char) -> *mut *mut c_void;
    pub(crate) fn chdb_close_conn(connection: *mut *mut c_void);
    pub(crate) fn chdb_classify_query_n(
        connection: *mut c_void,
        sql: *const c_char,
        sql_len: usize,
        target_database: *const c_char,
        target_database_len: usize,
        out_analysis: *mut QueryAnalysis,
    ) -> c_int;
    pub(crate) fn chdb_query_with_params_n(
        connection: *mut c_void,
        sql: *const c_char,
        sql_len: usize,
        format: *const c_char,
        format_len: usize,
        param_names: *const *const c_char,
        param_name_lens: *const usize,
        param_values: *const *const c_char,
        param_value_lens: *const usize,
        param_count: usize,
    ) -> *mut c_void;
    pub(crate) fn chdb_stream_query_with_params_n(
        connection: *mut c_void,
        sql: *const c_char,
        sql_len: usize,
        format: *const c_char,
        format_len: usize,
        param_names: *const *const c_char,
        param_name_lens: *const usize,
        param_values: *const *const c_char,
        param_value_lens: *const usize,
        param_count: usize,
    ) -> *mut c_void;
    pub(crate) fn chdb_stream_fetch_result(
        connection: *mut c_void,
        result: *mut c_void,
    ) -> *mut c_void;
    pub(crate) fn chdb_stream_cancel_query(connection: *mut c_void, result: *mut c_void);
    pub(crate) fn chdb_destroy_query_result(result: *mut c_void);
    pub(crate) fn chdb_result_buffer(result: *mut c_void) -> *mut c_char;
    pub(crate) fn chdb_result_length(result: *mut c_void) -> usize;
    pub(crate) fn chdb_result_rows_read(result: *mut c_void) -> u64;
    pub(crate) fn chdb_result_rows_written(result: *mut c_void) -> u64;
    pub(crate) fn chdb_result_column_count(result: *mut c_void) -> c_int;
    pub(crate) fn chdb_result_error(result: *mut c_void) -> *const c_char;
    pub(crate) fn chdb_stream_insert_n(
        connection: *mut c_void,
        sql: *const c_char,
        sql_len: usize,
        format: *const c_char,
        format_len: usize,
    ) -> *mut c_void;
    pub(crate) fn chdb_stream_append(
        stream: *mut c_void,
        data: *const c_void,
        len: usize,
    ) -> c_int;
    pub(crate) fn chdb_stream_done(stream: *mut c_void) -> *mut c_void;
    pub(crate) fn chdb_stream_cancel_insert(stream: *mut c_void);
    pub(crate) fn chdb_stream_insert_error(stream: *mut c_void) -> *const c_char;
    pub(crate) fn chdb_destroy_insert_stream(stream: *mut c_void);
}

#[inline(always)]
pub(crate) unsafe fn result_error<'a>(result: *mut c_void) -> Option<&'a CStr> {
    let error = unsafe { chdb_result_error(result) };
    if error.is_null() {
        return None;
    }
    let cstr = unsafe { CStr::from_ptr(error) };
    (!cstr.to_bytes().is_empty()).then_some(cstr)
}

#[cold]
#[inline(never)]
pub(crate) fn query_error(error: &CStr) -> Error {
    anyhow!("ChDB query failed: {}", error.to_string_lossy())
}
