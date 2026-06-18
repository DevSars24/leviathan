//! Cooperative scheduling and future-related utilities for the Leviathan runtime.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// A manual future that yields execution back to the Tokio executor a specific
/// number of times before completing.
///
/// This is used to prevent CPU-intensive tasks from starving other tasks on the
/// executor by voluntarily yielding control.
#[derive(Debug)]
pub struct CooperativeYield {
    yields: usize,
    target_yields: usize,
}

impl CooperativeYield {
    /// Create a new `CooperativeYield` future that will yield execution `target_yields` times.
    pub fn new(target_yields: usize) -> Self {
        Self {
            yields: 0,
            target_yields,
        }
    }
}

impl Future for CooperativeYield {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.yields >= self.target_yields {
            Poll::Ready(())
        } else {
            self.yields += 1;
            // Wake the current task so it gets scheduled to be polled again
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// An exponential backoff helper for retrying network operations.
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    current: Duration,
    max: Duration,
    factor: f64,
}

impl ExponentialBackoff {
    /// Create a new backoff strategy.
    pub fn new(initial: Duration, max: Duration, factor: f64) -> Self {
        Self {
            current: initial,
            max,
            factor,
        }
    }

    /// Calculate the next backoff duration and advance the internal state.
    pub fn next_backoff(&mut self) -> Duration {
        let prev = self.current;
        let next_secs = self.current.as_secs_f64() * self.factor;
        self.current = Duration::from_secs_f64(next_secs).min(self.max);
        prev
    }

    /// Reset the backoff duration to the initial duration.
    pub fn reset(&mut self, initial: Duration) {
        self.current = initial;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cooperative_yield() {
        let yield_fut = CooperativeYield::new(3);
        // Execute the yield future to completion
        yield_fut.await;
    }

    #[test]
    fn test_exponential_backoff() {
        let mut backoff = ExponentialBackoff::new(
            Duration::from_millis(100),
            Duration::from_secs(1),
            2.0,
        );
        assert_eq!(backoff.next_backoff(), Duration::from_millis(100));
        assert_eq!(backoff.next_backoff(), Duration::from_millis(200));
        assert_eq!(backoff.next_backoff(), Duration::from_millis(400));
        assert_eq!(backoff.next_backoff(), Duration::from_millis(800));
        // Next should hit the max limit of 1 second
        assert_eq!(backoff.next_backoff(), Duration::from_secs(1));
        
        backoff.reset(Duration::from_millis(100));
        assert_eq!(backoff.next_backoff(), Duration::from_millis(100));
    }
}
