use std::{
    ops::{Deref, DerefMut},
    ptr,
};

pub(crate) trait NullCheck {
    fn is_null(&self) -> bool;
}

impl<T> NullCheck for *const T {
    #[inline(always)]
    fn is_null(&self) -> bool {
        (*self).is_null()
    }
}

impl<T> NullCheck for *mut T {
    #[inline(always)]
    fn is_null(&self) -> bool {
        (*self).is_null()
    }
}

#[derive(Debug)]
pub(crate) struct CBox<T: NullCheck> {
    ptr: T,
    dealloc: fn(T),
}

impl<T: NullCheck> CBox<T> {
    #[inline(always)]
    pub fn new(ptr: T, dealloc: fn(T)) -> Self {
        Self { ptr, dealloc }
    }
}

impl<T: NullCheck> Drop for CBox<T> {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                (self.dealloc)(ptr::read(&self.ptr));
            }
        }
    }
}

impl<T: NullCheck> Deref for CBox<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl<T: NullCheck> DerefMut for CBox<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ptr
    }
}

unsafe impl<T: NullCheck> Send for CBox<T> {}
unsafe impl<T: NullCheck> Sync for CBox<T> {}
