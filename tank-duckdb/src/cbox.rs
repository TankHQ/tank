use libduckdb_sys::*;
use std::{
    ffi::c_char,
    ops::{Deref, DerefMut},
    ptr,
};

pub(crate) trait CResource: Sized {
    fn is_null(&self) -> bool;
    unsafe fn finalize(&mut self);
}

macro_rules! impl_resource {
    ($($ty:ty => $destroy:ident),* $(,)?) => {$(
        impl CResource for $ty {
            #[inline(always)]
            fn is_null(&self) -> bool {
                (*self).is_null()
            }
            #[inline(always)]
            unsafe fn finalize(&mut self) {
                unsafe { $destroy(self) };
            }
        }
    )*};
}

impl_resource! {
    duckdb_appender => duckdb_appender_destroy,
    duckdb_config => duckdb_destroy_config,
    duckdb_connection => duckdb_disconnect,
    duckdb_database => duckdb_close,
    duckdb_data_chunk => duckdb_destroy_data_chunk,
    duckdb_extracted_statements => duckdb_destroy_extracted,
    duckdb_instance_cache => duckdb_destroy_instance_cache,
    duckdb_logical_type => duckdb_destroy_logical_type,
    duckdb_prepared_statement => duckdb_destroy_prepare,
    duckdb_value => duckdb_destroy_value,
}

impl CResource for *mut c_char {
    #[inline(always)]
    fn is_null(&self) -> bool {
        (*self).is_null()
    }
    #[inline(always)]
    unsafe fn finalize(&mut self) {
        unsafe { duckdb_free(*self as _) };
    }
}

impl CResource for duckdb_result {
    #[inline(always)]
    fn is_null(&self) -> bool {
        false
    }
    #[inline(always)]
    unsafe fn finalize(&mut self) {
        unsafe { duckdb_destroy_result(self) };
    }
}

#[repr(transparent)]
#[derive(Debug)]
pub(crate) struct CBox<T: CResource>(pub(crate) T);

impl<T> CBox<*mut T>
where
    *mut T: CResource,
{
    #[inline(always)]
    pub const fn null() -> Self {
        Self(ptr::null_mut())
    }
}

impl<T: CResource> Drop for CBox<T> {
    #[inline(always)]
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { self.0.finalize() };
        }
    }
}

impl<T: CResource> Deref for CBox<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: CResource> DerefMut for CBox<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

unsafe impl<T: CResource> Send for CBox<T> {}
unsafe impl<T: CResource> Sync for CBox<T> {}
