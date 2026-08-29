// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Caller-defined identifier carried from an early sampling decision to later stages.

/// A caller-supplied identifier carried from an early sampling decision to a later stage.
///
/// The framework passes this value to a later processing stage. The
/// [`ContinueWith`](super::EarlySamplingDecision::ContinueWith) decision
/// attaches the value to the event.
///
/// `observed` never generates, inspects, or interprets this value. It is an
/// opaque value. The caller assigns it with [`SamplingId::new`]. The caller
/// reads it unchanged with [`SamplingId::get`], typically from
/// [`EventView::sampling_id`](crate::processing::EventView::sampling_id).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SamplingId(u64);

impl SamplingId {
    /// Wraps a caller-supplied value.
    ///
    /// # Examples
    ///
    /// ```
    /// use observed::sampling::SamplingId;
    ///
    /// let id = SamplingId::new(42);
    /// assert_eq!(id.get(), 42);
    /// ```
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the wrapped value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_and_get_roundtrip_caller_supplied_value() {
        assert_eq!(SamplingId::new(42).get(), 42);
        assert_eq!(SamplingId::new(0).get(), 0);
        assert_eq!(SamplingId::new(u64::MAX).get(), u64::MAX);
    }
}
