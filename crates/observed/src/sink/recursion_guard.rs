// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Shared thread-local guard for recursion protection and explicit emission
//! suppression.
//!
//! Ordinary emission acquires the guard only for processor dispatch. Building
//! an event value is ordinary user code - a field initializer may call a helper
//! that emits telemetry of its own - so emission takes the guard after the
//! event has been constructed. Explicit suppression holds the same guard for
//! the entire operation.
//!
//! # Scope: thread-wide, not per-sink
//!
//! The guard is a single un-keyed thread-local flag shared by **every**
//! [`Sink`](crate::Sink) on the thread, not one slot per sink identity. While
//! an event is being dispatched to processors or [`with_emission_suppressed`]
//! is running, *any* nested `emit!` on that thread skips dispatch - including
//! one targeting a completely unrelated sink.
//!
//! This is deliberate: nested telemetry is not a supported scenario. A
//! processor that emits while handling an event (e.g. reporting its own
//! failure to a separate diagnostics sink) would otherwise risk unbounded
//! recursion, and a per-sink guard would only push that risk one hop away
//! (sink A's processor emits to sink B, whose processor emits back to A).
//!
//! The consequence is that such nested events are dropped silently - there is
//! no warning, log, or error return, because reporting the drop would itself
//! require an emission. Processors must therefore not rely on emitting
//! telemetry from inside `process()`.

/// RAII guard that releases the current thread's reentrancy slot on drop.
struct ReentrancyGuard;
impl Drop for ReentrancyGuard {
    fn drop(&mut self) {
        AVAILABLE.set(true);
    }
}

thread_local! {
    static AVAILABLE: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

/// Runs `operation` with `observed` event dispatch suppressed on the current thread.
///
/// The operation always runs exactly once and its return value is passed through,
/// even inside another suppression scope or an [`EventProcessor::process`](crate::processing::EventProcessor::process)
/// call. Emissions through any [`Sink`](crate::Sink) on this thread are silently
/// dropped before reaching `process`, regardless of signal or processor.
/// Interest checks and event construction may still run.
///
/// Scopes nest with each other and with ordinary processor dispatch. The previous
/// suppression state is restored when the operation returns or unwinds after a
/// panic. Other threads are unaffected.
///
/// This scope covers synchronous execution only. Returning a future does not
/// suppress emissions when that future is later polled.
///
/// # Example
///
/// ```
/// use observed::processing::with_emission_suppressed;
///
/// let mut calls = 0;
/// let result = with_emission_suppressed(|| {
///     calls += 1;
///     // Any observed emissions made here are suppressed.
///     42
/// });
///
/// assert_eq!(calls, 1);
/// assert_eq!(result, 42);
/// ```
pub fn with_emission_suppressed<R>(operation: impl FnOnce() -> R) -> R {
    let _guard = try_acquire_reentrancy_guard();
    operation()
}

/// Attempts to acquire the current thread's reentrancy guard.
///
/// Returns `Some(guard)` when no guard is held on this thread; the
/// slot is released when the returned guard is dropped. Returns `None` when a
/// guard is already held by processor dispatch or explicit suppression.
/// Emission skips dispatch in that case; explicit suppression still runs its
/// operation because the outer scope owns the guard.
///
/// The slot is shared across all sinks on the thread, so a `None` here means
/// *some* scope holds the guard - not necessarily one on the same sink. See
/// the [module docs](self) for why the guard is thread-wide.
pub(super) fn try_acquire_reentrancy_guard() -> Option<impl Drop> {
    AVAILABLE.get().then(|| {
        AVAILABLE.set(false);
        ReentrancyGuard
    })
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_allows_single_acquisition() {
        assert!(try_acquire_reentrancy_guard().is_some());
    }

    #[test]
    fn guard_blocks_reentrancy() {
        let _guard = try_acquire_reentrancy_guard().expect("should acquire guard");
        assert!(try_acquire_reentrancy_guard().is_none(), "should block reentrancy");
    }

    #[test]
    fn guard_allows_after_drop() {
        {
            let _guard = try_acquire_reentrancy_guard().expect("should acquire guard");
        }
        assert!(try_acquire_reentrancy_guard().is_some(), "should allow after drop");
    }
}
