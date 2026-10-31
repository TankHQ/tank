mod as_value;
mod column;
mod connection;
mod dataset;
mod decode_type;
mod driver;
mod entity;
mod executor;
mod expression;
mod interval;
mod join;
mod pool;
mod pool_config;
mod query;
mod relations;
mod row;
mod table_ref;
mod transaction;
mod util;
mod value;
mod writer;

pub use ::anyhow::Context as ErrorContext;
pub use ::anyhow::anyhow;
pub use as_value::*;
pub use column::*;
pub use connection::*;
pub use dataset::*;
pub use decode_type::*;
pub use driver::*;
pub use entity::*;
pub use executor::*;
pub use expression::*;
pub use interval::*;
pub use join::*;
pub use pool::*;
pub use pool_config::*;
pub use query::*;
pub use relations::*;
pub use row::*;
pub use table_ref::*;
pub use transaction::*;
pub use util::*;
pub use value::*;
pub use writer::*;
pub mod stream {
    pub use ::futures::stream::*;
    use ::futures::{TryFuture, stream::TryStreamExt as FuturesTryStreamExt};
    use std::{
        future::Future,
        mem,
        pin::Pin,
        task::{Context, Poll},
    };

    /// Future for [`TryStreamExt::try_collect`] that forwards the stream's `size_hint`
    /// into [`Extend::extend`] once the stream yields its first ready item.
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct TryCollect<S, C> {
        stream: S,
        items: C,
    }

    struct ReadyItems<'a, 'b, S: TryStream> {
        stream: Pin<&'a mut S>,
        cx: &'a mut Context<'b>,
        err: &'a mut Option<S::Error>,
        done: &'a mut bool,
    }

    impl<S: TryStream> Iterator for ReadyItems<'_, '_, S> {
        type Item = S::Ok;

        #[inline(always)]
        fn next(&mut self) -> Option<Self::Item> {
            match self.stream.as_mut().try_poll_next(self.cx) {
                Poll::Ready(Some(Ok(item))) => Some(item),
                Poll::Ready(Some(Err(e))) => {
                    *self.err = Some(e);
                    None
                }
                Poll::Ready(None) => {
                    *self.done = true;
                    None
                }
                Poll::Pending => None,
            }
        }

        #[inline(always)]
        fn size_hint(&self) -> (usize, Option<usize>) {
            self.stream.size_hint()
        }
    }

    impl<S, C> Future for TryCollect<S, C>
    where
        S: TryStream,
        C: Default + Extend<S::Ok>,
    {
        type Output = Result<C, S::Error>;

        #[inline]
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = unsafe { self.get_unchecked_mut() };
            let mut err = None;
            let mut done = false;
            this.items.extend(ReadyItems {
                stream: unsafe { Pin::new_unchecked(&mut this.stream) },
                cx,
                err: &mut err,
                done: &mut done,
            });
            if let Some(e) = err {
                return Poll::Ready(Err(e));
            }
            if done {
                return Poll::Ready(Ok(mem::take(&mut this.items)));
            }
            Poll::Pending
        }
    }

    macro_rules! forward {
        ($(fn $name:ident $(<$($g:ident $(: $b:path)?),*>)? ($self:ident $(, $arg:ident: $arg_ty:ty)*) -> $ret:ty $(where { $($w:tt)* })?;)*) => {$(
            #[inline(always)]
            fn $name $(<$($g $(: $b)?),*>)? ($self $(, $arg: $arg_ty)*) -> $ret $(where $($w)*)? {
                FuturesTryStreamExt::$name($self $(, $arg)*)
            }
        )*};
    }

    pub trait TryStreamExt: TryStream {
        #[inline(always)]
        fn try_collect<C: Default + Extend<Self::Ok>>(self) -> TryCollect<Self, C>
        where
            Self: Sized,
        {
            TryCollect {
                stream: self,
                items: C::default(),
            }
        }

        #[inline(always)]
        fn try_next(&mut self) -> TryNext<'_, Self>
        where
            Self: Unpin,
        {
            FuturesTryStreamExt::try_next(self)
        }

        forward! {
            fn err_into<E>(self) -> ErrInto<Self, E> where { Self: Sized, Self::Error: Into<E> };
            fn try_flatten(self) -> TryFlatten<Self> where { Self::Ok: TryStream, <Self::Ok as TryStream>::Error: From<Self::Error>, Self: Sized };
            fn try_concat(self) -> TryConcat<Self> where { Self: Sized, Self::Ok: Extend<<<Self as TryStream>::Ok as IntoIterator>::Item> + IntoIterator + Default };
            fn into_stream(self) -> IntoStream<Self> where { Self: Sized };
            fn map_ok<T, F>(self, f: F) -> MapOk<Self, F> where { F: FnMut(Self::Ok) -> T, Self: Sized };
            fn map_err<E, F>(self, f: F) -> MapErr<Self, F> where { F: FnMut(Self::Error) -> E, Self: Sized };
            fn inspect_ok<F>(self, f: F) -> InspectOk<Self, F> where { F: FnMut(&Self::Ok), Self: Sized };
            fn inspect_err<F>(self, f: F) -> InspectErr<Self, F> where { F: FnMut(&Self::Error), Self: Sized };
            fn and_then<Fut, F>(self, f: F) -> AndThen<Self, Fut, F> where { F: FnMut(Self::Ok) -> Fut, Fut: TryFuture<Error = Self::Error>, Self: Sized };
            fn or_else<Fut, F>(self, f: F) -> OrElse<Self, Fut, F> where { F: FnMut(Self::Error) -> Fut, Fut: TryFuture<Ok = Self::Ok>, Self: Sized };
            fn try_for_each<Fut, F>(self, f: F) -> TryForEach<Self, Fut, F> where { F: FnMut(Self::Ok) -> Fut, Fut: TryFuture<Ok = (), Error = Self::Error>, Self: Sized };
            fn try_fold<T, Fut, F>(self, init: T, f: F) -> TryFold<Self, Fut, T, F> where { F: FnMut(T, Self::Ok) -> Fut, Fut: TryFuture<Ok = T, Error = Self::Error>, Self: Sized };
            fn try_filter<Fut, F>(self, f: F) -> TryFilter<Self, Fut, F> where { Fut: Future<Output = bool>, F: FnMut(&Self::Ok) -> Fut, Self: Sized };
            fn try_filter_map<Fut, T, F>(self, f: F) -> TryFilterMap<Self, Fut, F> where { Fut: TryFuture<Ok = Option<T>, Error = Self::Error>, F: FnMut(Self::Ok) -> Fut, Self: Sized };
            fn try_chunks(self, capacity: usize) -> TryChunks<Self> where { Self: Sized };
            fn try_ready_chunks(self, capacity: usize) -> TryReadyChunks<Self> where { Self: Sized };
            fn try_skip_while<Fut, F>(self, f: F) -> TrySkipWhile<Self, Fut, F> where { Fut: TryFuture<Ok = bool, Error = Self::Error>, F: FnMut(&Self::Ok) -> Fut, Self: Sized };
            fn try_take_while<Fut, F>(self, f: F) -> TryTakeWhile<Self, Fut, F> where { Fut: TryFuture<Ok = bool, Error = Self::Error>, F: FnMut(&Self::Ok) -> Fut, Self: Sized };
            fn try_buffer_unordered(self, n: usize) -> TryBufferUnordered<Self> where { Self::Ok: TryFuture<Error = Self::Error>, Self: Sized };
            fn try_buffered(self, n: usize) -> TryBuffered<Self> where { Self::Ok: TryFuture<Error = Self::Error>, Self: Sized };
            fn try_all<Fut, F>(self, f: F) -> TryAll<Self, Fut, F> where { Self: Sized, F: FnMut(Self::Ok) -> Fut, Fut: Future<Output = bool> };
            fn try_any<Fut, F>(self, f: F) -> TryAny<Self, Fut, F> where { Self: Sized, F: FnMut(Self::Ok) -> Fut, Fut: Future<Output = bool> };
        }

        #[inline(always)]
        fn try_for_each_concurrent<Fut, F>(
            self,
            limit: impl Into<Option<usize>>,
            f: F,
        ) -> TryForEachConcurrent<Self, Fut, F>
        where
            F: FnMut(Self::Ok) -> Fut,
            Fut: Future<Output = Result<(), Self::Error>>,
            Self: Sized,
        {
            FuturesTryStreamExt::try_for_each_concurrent(self, limit, f)
        }
    }

    impl<S: TryStream + ?Sized> TryStreamExt for S {}
}
pub use ::futures::future;
pub use ::futures::sink;

/// Result type.
pub type Result<T> = anyhow::Result<T>;
/// Error type.
pub type Error = anyhow::Error;
