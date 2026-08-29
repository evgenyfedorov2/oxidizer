// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! The [`EarlySampler`] extension point.

use super::{EventMetadata, SamplingDecision};

/// A sampler called before event construction during direct emission through
/// a non-composite sink.
///
/// Attach one to a non-composite sink with
/// [`Sink::with_early_sampler`](crate::Sink::with_early_sampler). The sink
/// calls [`sample`](Self::sample) at most once for each direct emission.
/// It calls `sample` only after static processor interest accepts the event.
/// See the [module docs](super) for the full sequence.
///
/// # Must not emit through `observed`
///
/// `sample` runs *before* the reentrancy guard that protects
/// [`EventProcessor::process`](crate::processing::EventProcessor::process). A
/// sampler must not emit through `observed` from inside `sample`. Such an
/// emission calls the sampler repeatedly because the guard is not active.
///
/// # Examples
///
/// ```
/// use observed::sampling::{EarlySampler, EventMetadata, SamplingDecision};
///
/// /// Continues events from `important_crate` and drops all other events.
/// struct OnlyNoisyCrate;
///
/// impl EarlySampler for OnlyNoisyCrate {
///     fn sample(&self, event: &EventMetadata<'_>) -> SamplingDecision {
///         match event.source_crate().as_deref() {
///             Some("important_crate") => SamplingDecision::Continue,
///             _ => SamplingDecision::Drop,
///         }
///     }
/// }
/// ```
pub trait EarlySampler: Send + Sync {
    /// Returns the sampling decision for this event.
    ///
    /// The sink calls this method at most once per emission. The method must
    /// have a low cost and must not emit through `observed`. See
    /// [the trait-level warning](Self#must-not-emit-through-observed).
    fn sample(&self, event: &EventMetadata<'_>) -> SamplingDecision;
}
