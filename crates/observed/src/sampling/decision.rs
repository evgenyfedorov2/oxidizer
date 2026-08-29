// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! The outcome of an [`EarlySampler`](super::EarlySampler) decision.

use super::SamplingId;

/// The outcome of an [`EarlySampler::sample`](super::EarlySampler::sample) call.
///
/// Returned before an event's typed value is constructed, so `Drop` is the
/// only variant that can prevent that construction - see the
/// [module docs](super) for the full emission sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EarlySamplingDecision {
    /// Discards the event before its typed value is constructed.
    ///
    /// No processor on this sink - log, metric, or otherwise - observes any
    /// signal from it.
    Drop,
    /// Continues emission normally.
    ///
    /// The resulting [`EventView`](crate::processing::EventView) carries no
    /// [`EventView::sampling_id`](crate::processing::EventView::sampling_id).
    Continue,
    /// Continues emission and attaches `SamplingId` to the resulting
    /// [`EventView`](crate::processing::EventView). A later stage can read the
    /// id with
    /// [`EventView::sampling_id`](crate::processing::EventView::sampling_id).
    ContinueWith(SamplingId),
}
