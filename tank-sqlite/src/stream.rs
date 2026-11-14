use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tank_core::stream::Stream;

/// Bounds ready work per poll without a runtime-specific channel or worker queue.
pub(crate) struct SQLiteStream<I> {
    iter: I,
    remaining: u16,
}

impl<I> SQLiteStream<I> {
    pub(crate) fn new(iter: I) -> Self {
        Self {
            iter,
            remaining: Self::BUDGET,
        }
    }

    const BUDGET: u16 = 256;
}

impl<I: Iterator> Stream for SQLiteStream<I> {
    type Item = I::Item;

    #[inline(always)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // The iterator is never pinned or moved out, only advanced through &mut.
        let this = unsafe { self.get_unchecked_mut() };
        if this.remaining == 0 {
            this.remaining = Self::BUDGET;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        this.remaining -= 1;
        Poll::Ready(this.iter.next())
    }
}
