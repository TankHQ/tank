use anyhow::{anyhow, ensure};
// Retain the dependency's native link directives, including the bundled feature.
use chdb_rust as _;
use std::{
    ffi::{CStr, CString, c_char, c_int, c_void},
    hint, mem,
    ptr::{self, NonNull},
    slice,
};
use tank_core::{Error, Result};

pub(crate) const QUERY_CLASS_READ_ONLY: u32 = 0;

#[repr(C)]
struct QueryAnalysis {
    struct_size: u32,
    statement_count: u32,
    flags: u32,
    query_class: u32,
}

unsafe extern "C" {
    fn chdb_connect(argc: c_int, argv: *mut *mut c_char) -> *mut *mut c_void;
    fn chdb_close_conn(connection: *mut *mut c_void);
    fn chdb_classify_query_n(
        connection: *mut c_void,
        sql: *const c_char,
        sql_len: usize,
        target_database: *const c_char,
        target_database_len: usize,
        out_analysis: *mut QueryAnalysis,
    ) -> c_int;
    fn chdb_query_n(
        connection: *mut c_void,
        sql: *const c_char,
        sql_len: usize,
        format: *const c_char,
        format_len: usize,
    ) -> *mut c_void;
    fn chdb_query_with_params_n(
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
    fn chdb_stream_query_n(
        connection: *mut c_void,
        sql: *const c_char,
        sql_len: usize,
        format: *const c_char,
        format_len: usize,
    ) -> *mut c_void;
    fn chdb_stream_query_with_params_n(
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
    fn chdb_stream_fetch_result(connection: *mut c_void, result: *mut c_void) -> *mut c_void;
    fn chdb_stream_cancel_query(connection: *mut c_void, result: *mut c_void);
    fn chdb_destroy_query_result(result: *mut c_void);
    fn chdb_result_buffer(result: *mut c_void) -> *mut c_char;
    fn chdb_result_length(result: *mut c_void) -> usize;
    fn chdb_result_rows_read(result: *mut c_void) -> u64;
    fn chdb_result_rows_written(result: *mut c_void) -> u64;
    fn chdb_result_column_count(result: *mut c_void) -> c_int;
    fn chdb_result_error(result: *mut c_void) -> *const c_char;
    fn chdb_stream_insert_n(
        connection: *mut c_void,
        sql: *const c_char,
        sql_len: usize,
        format: *const c_char,
        format_len: usize,
    ) -> *mut c_void;
    fn chdb_stream_append(stream: *mut c_void, data: *const c_void, len: usize) -> c_int;
    fn chdb_stream_done(stream: *mut c_void) -> *mut c_void;
    fn chdb_stream_cancel_insert(stream: *mut c_void);
    fn chdb_stream_insert_error(stream: *mut c_void) -> *const c_char;
    fn chdb_destroy_insert_stream(stream: *mut c_void);
}

/// Reusable parameter name/value buffers for `chdb_*_with_params_n`.
#[derive(Default, Debug)]
pub(crate) struct ParamBuffer {
    names: Vec<u8>,
    values: Vec<u8>,
    name_lens: Vec<usize>,
    value_lens: Vec<usize>,
    name_ptrs: Vec<*const c_char>,
    value_ptrs: Vec<*const c_char>,
}

// Raw pointers inside `ParamBuffer` are rebuilt from its own Vecs before each FFI call.
unsafe impl Send for ParamBuffer {}

impl ParamBuffer {
    pub(crate) fn clear(&mut self) {
        self.names.clear();
        self.values.clear();
        self.name_lens.clear();
        self.value_lens.clear();
        self.name_ptrs.clear();
        self.value_ptrs.clear();
    }

    pub(crate) fn push(&mut self, name: &[u8], write_value: impl FnOnce(&mut Vec<u8>)) {
        self.names.extend_from_slice(name);
        self.name_lens.push(name.len());
        let start = self.values.len();
        write_value(&mut self.values);
        self.value_lens.push(self.values.len() - start);
    }

    pub(crate) fn finish(&mut self) {
        let mut ptr = self.names.as_ptr().cast::<c_char>();
        for &len in &self.name_lens {
            self.name_ptrs.push(ptr);
            ptr = unsafe { ptr.add(len) };
        }
        let mut ptr = self.values.as_ptr().cast::<c_char>();
        for &len in &self.value_lens {
            self.value_ptrs.push(ptr);
            ptr = unsafe { ptr.add(len) };
        }
    }

    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        self.name_lens.is_empty()
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.name_lens.len()
    }

    #[inline(always)]
    fn names_ptr(&self) -> *const *const c_char {
        if self.name_ptrs.is_empty() {
            ptr::null()
        } else {
            self.name_ptrs.as_ptr()
        }
    }

    #[inline(always)]
    fn name_lens_ptr(&self) -> *const usize {
        if self.name_lens.is_empty() {
            ptr::null()
        } else {
            self.name_lens.as_ptr()
        }
    }

    #[inline(always)]
    fn values_ptr(&self) -> *const *const c_char {
        if self.value_ptrs.is_empty() {
            ptr::null()
        } else {
            self.value_ptrs.as_ptr()
        }
    }

    #[inline(always)]
    fn value_lens_ptr(&self) -> *const usize {
        if self.value_lens.is_empty() {
            ptr::null()
        } else {
            self.value_lens.as_ptr()
        }
    }
}

#[derive(Debug)]
pub(crate) struct NativeConnection(NonNull<*mut c_void>);

// Access is serialized by `&mut ChDBConnection`.
unsafe impl Send for NativeConnection {}

impl NativeConnection {
    pub(crate) fn open(path: Option<&str>) -> Result<Self> {
        let mut args = vec![
            CString::new("clickhouse")?,
            CString::new("--allow_experimental_lightweight_delete=1")?,
            CString::new("--join_use_nulls=1")?,
            CString::new("--final=1")?,
        ];
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

    #[inline(always)]
    fn raw(&self) -> *mut c_void {
        unsafe { *self.0.as_ptr() }
    }

    #[inline(always)]
    pub(crate) fn classify(&self, sql: &str) -> u32 {
        let mut analysis = QueryAnalysis {
            struct_size: mem::size_of::<QueryAnalysis>() as u32,
            statement_count: 0,
            flags: 0,
            query_class: u32::MAX,
        };
        let state = unsafe {
            chdb_classify_query_n(
                self.raw(),
                sql.as_ptr().cast(),
                sql.len(),
                ptr::null(),
                0,
                &mut analysis,
            )
        };
        if state == 0 {
            analysis.query_class
        } else {
            u32::MAX
        }
    }

    pub(crate) fn query(&mut self, sql: &str, format: &str) -> Result<NativeResult> {
        let result = NativeResult::new(unsafe {
            chdb_query_n(
                self.raw(),
                sql.as_ptr().cast(),
                sql.len(),
                format.as_ptr().cast(),
                format.len(),
            )
        })?;
        result.check()?;
        Ok(result)
    }

    pub(crate) fn query_with_params(
        &mut self,
        sql: &str,
        format: &str,
        params: &ParamBuffer,
    ) -> Result<NativeResult> {
        let result = NativeResult::new(unsafe {
            chdb_query_with_params_n(
                self.raw(),
                sql.as_ptr().cast(),
                sql.len(),
                format.as_ptr().cast(),
                format.len(),
                params.names_ptr(),
                params.name_lens_ptr(),
                params.values_ptr(),
                params.value_lens_ptr(),
                params.len(),
            )
        })?;
        result.check()?;
        Ok(result)
    }

    pub(crate) fn stream(&mut self, sql: &str) -> Result<NativeResult> {
        const FORMAT: &str = "Native";
        NativeResult::new(unsafe {
            chdb_stream_query_n(
                self.raw(),
                sql.as_ptr().cast(),
                sql.len(),
                FORMAT.as_ptr().cast(),
                FORMAT.len(),
            )
        })
    }

    pub(crate) fn stream_with_params(
        &mut self,
        sql: &str,
        params: &ParamBuffer,
    ) -> Result<NativeResult> {
        const FORMAT: &str = "Native";
        NativeResult::new(unsafe {
            chdb_stream_query_with_params_n(
                self.raw(),
                sql.as_ptr().cast(),
                sql.len(),
                FORMAT.as_ptr().cast(),
                FORMAT.len(),
                params.names_ptr(),
                params.name_lens_ptr(),
                params.values_ptr(),
                params.value_lens_ptr(),
                params.len(),
            )
        })
    }

    #[inline(always)]
    pub(crate) fn fetch(&mut self, stream: &NativeResult) -> Result<NativeResult> {
        let result =
            NativeResult::new(unsafe { chdb_stream_fetch_result(self.raw(), stream.0.as_ptr()) })?;
        result.check()?;
        Ok(result)
    }

    pub(crate) fn cancel(&mut self, stream: &NativeResult) {
        unsafe { chdb_stream_cancel_query(self.raw(), stream.0.as_ptr()) };
    }

    pub(crate) fn insert_stream(&mut self, sql: &str, format: &str) -> Result<NativeInsertStream> {
        let raw = unsafe {
            chdb_stream_insert_n(
                self.raw(),
                sql.as_ptr().cast(),
                sql.len(),
                format.as_ptr().cast(),
                format.len(),
            )
        };
        let handle =
            NonNull::new(raw).ok_or_else(|| anyhow!("ChDB returned a null insert stream"))?;
        let stream = NativeInsertStream {
            handle,
            done: false,
        };
        stream.check()?;
        Ok(stream)
    }
}

impl Drop for NativeConnection {
    fn drop(&mut self) {
        unsafe { chdb_close_conn(self.0.as_ptr()) };
    }
}

#[derive(Debug)]
pub(crate) struct NativeResult(NonNull<c_void>);

// Owned handle, chunk results are read-only once returned by chDB.
unsafe impl Send for NativeResult {}
unsafe impl Sync for NativeResult {}

impl NativeResult {
    #[inline(always)]
    fn new(raw: *mut c_void) -> Result<Self> {
        match NonNull::new(raw) {
            Some(ptr) => Ok(Self(ptr)),
            None => {
                hint::cold_path();
                Err(anyhow!("ChDB returned a null result"))
            }
        }
    }

    #[inline(always)]
    pub(crate) fn error(&self) -> Option<&CStr> {
        let error = unsafe { chdb_result_error(self.0.as_ptr()) };
        if error.is_null() {
            return None;
        }
        let cstr = unsafe { CStr::from_ptr(error) };
        (!cstr.to_bytes().is_empty()).then_some(cstr)
    }

    #[cold]
    #[inline(never)]
    fn make_error(error: &CStr) -> Error {
        anyhow!("ChDB query failed: {}", error.to_string_lossy())
    }

    #[inline(always)]
    pub(crate) fn check(&self) -> Result<()> {
        if let Some(error) = self.error() {
            hint::cold_path();
            return Err(Self::make_error(error));
        }
        Ok(())
    }

    #[inline(always)]
    pub(crate) fn data(&self) -> Result<&[u8]> {
        let len = unsafe { chdb_result_length(self.0.as_ptr()) };
        if len == 0 {
            return Ok(&[]);
        }
        let data = unsafe { chdb_result_buffer(self.0.as_ptr()) };
        if data.is_null() {
            hint::cold_path();
            return Err(anyhow!("ChDB returned a null buffer with nonzero length"));
        }
        Ok(unsafe { slice::from_raw_parts(data.cast(), len) })
    }

    #[inline(always)]
    pub(crate) fn rows_read(&self) -> u64 {
        unsafe { chdb_result_rows_read(self.0.as_ptr()) }
    }

    #[inline(always)]
    pub(crate) fn rows_written(&self) -> u64 {
        unsafe { chdb_result_rows_written(self.0.as_ptr()) }
    }

    #[inline(always)]
    pub(crate) fn column_count(&self) -> i32 {
        unsafe { chdb_result_column_count(self.0.as_ptr()) }
    }
}

impl Drop for NativeResult {
    fn drop(&mut self) {
        unsafe { chdb_destroy_query_result(self.0.as_ptr()) };
    }
}

pub(crate) struct NativeInsertStream {
    handle: NonNull<c_void>,
    done: bool,
}

unsafe impl Send for NativeInsertStream {}

impl NativeInsertStream {
    #[cold]
    #[inline(never)]
    fn stream_error(&self, fallback: &str) -> Error {
        let err = unsafe { chdb_stream_insert_error(self.handle.as_ptr()) };
        if err.is_null() {
            anyhow!("{fallback}")
        } else {
            anyhow!(
                "ChDB insert stream failed: {}",
                unsafe { CStr::from_ptr(err) }.to_string_lossy()
            )
        }
    }

    #[inline(always)]
    fn check(&self) -> Result<()> {
        let err = unsafe { chdb_stream_insert_error(self.handle.as_ptr()) };
        if !err.is_null() {
            hint::cold_path();
            return Err(self.stream_error("ChDB insert stream initialization failed"));
        }
        Ok(())
    }

    #[inline(always)]
    pub(crate) fn append(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let state = unsafe {
            chdb_stream_append(self.handle.as_ptr(), data.as_ptr().cast(), data.len())
        };
        if state != 0 {
            hint::cold_path();
            return Err(self.stream_error("ChDB insert stream append failed"));
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<u64> {
        self.done = true;
        let raw = unsafe { chdb_stream_done(self.handle.as_ptr()) };
        if raw.is_null() {
            return Err(self.stream_error("ChDB insert stream returned null result"));
        }
        let result = NativeResult::new(raw)?;
        result.check()?;
        Ok(result.rows_written())
    }
}

impl Drop for NativeInsertStream {
    fn drop(&mut self) {
        if !self.done {
            unsafe { chdb_stream_cancel_insert(self.handle.as_ptr()) };
        }
        unsafe { chdb_destroy_insert_stream(self.handle.as_ptr()) };
    }
}
