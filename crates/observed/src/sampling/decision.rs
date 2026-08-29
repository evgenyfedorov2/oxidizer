// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! The outcome of an [`EarlySampler`](super::EarlySampler) decision.

/// The outcome of an [`EarlySampler::sample`](super::EarlySampler::sample) call.
///
/// Returned before an event's typed value is constructed, so `Drop` is the
/// only variant that can prevent that construction - see the
/// [module docs](super) for the full emission sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SamplingDecision {
    /// Discards the event before its typed value is constructed.
    ///
    /// No processor on this sink - log, metric, or otherwise - observes any
    /// signal from it.
    Drop,
    /// Continues emission normally.
    ///
    /// Interested processors receive the event.
    Continue,
}
