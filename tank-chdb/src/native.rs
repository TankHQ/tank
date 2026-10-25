use anyhow::{anyhow, ensure};
// Retain the dependency's native link directives, including the bundled feature.
use chdb_rust as _;
use std::{
    ffi::{CStr, CString, c_char, c_int, c_void},
    ptr::NonNull,
};
use tank_core::Result;

// Public chdb.h handles only. Neither the Rust wrapper nor native private layouts
// are inspected. The pointer-to-handle returned by connect must be closed intact.
unsafe extern "C" {
    fn chdb_connect(argc: c_int, argv: *mut *mut c_char) -> *mut *mut c_void;
    fn chdb_close_conn(connection: *mut *mut c_void);
    fn chdb_query(
        connection: *mut c_void,
        sql: *const c_char,
        format: *const c_char,
    ) -> *mut c_void;
    fn chdb_stream_query(
        connection: *mut c_void,
        sql: *const c_char,
        format: *const c_char,
    ) -> *mut c_void;
    fn chdb_stream_fetch_result(connection: *mut c_void, result: *mut c_void) -> *mut c_void;
    fn chdb_stream_cancel_query(connection: *mut c_void, result: *mut c_void);
    fn chdb_destroy_query_result(result: *mut c_void);
    fn chdb_result_buffer(result: *mut c_void) -> *mut c_char;
    fn chdb_result_length(result: *mut c_void) -> usize;
    fn chdb_result_rows_read(result: *mut c_void) -> u64;
    fn chdb_result_error(result: *mut c_void) -> *const c_char;
}

#[derive(Debug)]
pub(crate) struct NativeConnection(NonNull<*mut c_void>);

// All access is serialized by ChDBConnection's mutex. Native calls support
// successive calls from different threads (as does chdb-rust's Connection).
unsafe impl Send for NativeConnection {}

impl NativeConnection {
    pub(crate) fn open(path: Option<&str>) -> Result<Self> {
        let mut args = vec![CString::new("clickhouse")?];
        if let Some(path) = path {
            args.push(CString::new(format!("--path={path}"))?);
        }
        let mut argv: Vec<_> = args.iter().map(|arg| arg.as_ptr().cast_mut()).collect();
        let handle = NonNull::new(unsafe { chdb_connect(argv.len() as c_int, argv.as_mut_ptr()) })
            .ok_or_else(|| anyhow!("Cannot open chDB connection"))?;
        let connection = Self(handle);
        ensure!(!connection.raw().is_null(), "Cannot open chDB connection");
        Ok(connection)
    }

    fn raw(&self) -> *mut c_void {
        unsafe { *self.0.as_ptr() }
    }

    pub(crate) fn query(&mut self, sql: &str, format: &CStr) -> Result<NativeResult> {
        let sql = CString::new(sql)?;
        let result =
            NativeResult::new(unsafe { chdb_query(self.raw(), sql.as_ptr(), format.as_ptr()) })?;
        result.check()?;
        Ok(result)
    }

    pub(crate) fn stream(&mut self, sql: &str) -> Result<NativeResult> {
        let sql = CString::new(sql)?;
        NativeResult::new(unsafe {
            chdb_stream_query(self.raw(), sql.as_ptr(), c"JSONCompact".as_ptr())
        })
    }

    pub(crate) fn fetch(&mut self, stream: &NativeResult) -> Result<NativeResult> {
        let result =
            NativeResult::new(unsafe { chdb_stream_fetch_result(self.raw(), stream.0.as_ptr()) })?;
        result.check()?;
        Ok(result)
    }

    pub(crate) fn cancel(&mut self, stream: &NativeResult) {
        unsafe { chdb_stream_cancel_query(self.raw(), stream.0.as_ptr()) };
    }
}

impl Drop for NativeConnection {
    fn drop(&mut self) {
        unsafe { chdb_close_conn(self.0.as_ptr()) };
    }
}

pub(crate) struct NativeResult(NonNull<c_void>);

// Unique owned handle; its connection is kept alive by the streaming owner.
unsafe impl Send for NativeResult {}

impl NativeResult {
    fn new(raw: *mut c_void) -> Result<Self> {
        NonNull::new(raw)
            .map(Self)
            .ok_or_else(|| anyhow!("chDB returned a null result"))
    }

    pub(crate) fn error(&self) -> Option<String> {
        let error = unsafe { chdb_result_error(self.0.as_ptr()) };
        if error.is_null() {
            None
        } else {
            let error = unsafe { CStr::from_ptr(error) }.to_string_lossy();
            (!error.is_empty()).then(|| error.into_owned())
        }
    }

    pub(crate) fn check(&self) -> Result<()> {
        match self.error() {
            Some(error) => Err(anyhow!("chDB query failed: {error}")),
            None => Ok(()),
        }
    }

    pub(crate) fn data(&self) -> Result<&[u8]> {
        let len = unsafe { chdb_result_length(self.0.as_ptr()) };
        if len == 0 {
            return Ok(&[]);
        }
        let data = unsafe { chdb_result_buffer(self.0.as_ptr()) };
        ensure!(
            !data.is_null(),
            "chDB returned a null buffer with nonzero length"
        );
        Ok(unsafe { std::slice::from_raw_parts(data.cast(), len) })
    }

    pub(crate) fn is_finished(&self) -> bool {
        // chDB 26.5 MaterializedQueryResult::isEmpty uses this counter, not
        // buffer length: an empty SELECT can still carry a JSON schema.
        unsafe { chdb_result_rows_read(self.0.as_ptr()) == 0 }
    }
}

impl Drop for NativeResult {
    fn drop(&mut self) {
        unsafe { chdb_destroy_query_result(self.0.as_ptr()) };
    }
}
