use libduckdb_sys::v2::*;
use std::{ptr, slice};
use tank_core::{Error, Result};

#[inline(always)]
pub(crate) fn check_rc(
    f: impl FnOnce(*mut duckdb_v2_error_info_handle) -> DUCKDB_V2_ERROR,
) -> Result<()> {
    let mut err: duckdb_v2_error_info_handle = ptr::null_mut();
    let rc = f(&mut err);
    if rc == DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_NONE {
        return Ok(());
    }
    unsafe {
        if !err.is_null() {
            let mut text = duckdb_v2_str::default();
            let msg = if duckdb_v2_error_info_get_text(err, &mut text)
                == DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_NONE
                && !text.ptr.is_null()
            {
                String::from_utf8_lossy(slice::from_raw_parts(text.ptr as _, text.len as usize))
                    .into_owned()
            } else {
                format!("DuckDB error ({rc:?})")
            };
            duckdb_v2_error_info_destroy(&mut err);
            Err(Error::msg(msg))
        } else {
            Err(Error::msg(format!("DuckDB error ({rc:?})")))
        }
    }
}

#[inline(always)]
pub(crate) fn duckdb_bytes_to_slice(b: &duckdb_v2_bytes) -> &[u8] {
    unsafe {
        let len = b.value.inlined.length;
        let inlined = b.value.inlined.inlined.as_ptr() as *const u8;
        let pointed = b.value.pointer.ptr as *const u8;
        let ptr = if len <= DUCKDB_V2_BYTES_INLINE_LENGTH {
            inlined
        } else {
            pointed
        };
        slice::from_raw_parts(ptr, len as usize)
    }
}
