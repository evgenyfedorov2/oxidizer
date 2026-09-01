// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! The [`EarlySampler`] extension point.

use super::EventMetadata;

/// A sampler that decides which events a non-composite sink processes.
///
/// Attach one to a non-composite sink with
/// [`Sink::with_early_sampler`](crate::Sink::with_early_sampler). The sink
/// calls [`sample`](Self::sample) once per direct emission, after static
/// processor interest accepts the event and after the event value is built.
/// See the [module docs](super) for the full sequence.
///
/// # Must not emit through `observed`
///
/// `sample` runs under the same thread-wide reentrancy guard that protects
/// [`EventProcessor::process`](crate::processing::EventProcessor::process), so
/// an emission from inside `sample` is silently dropped, even one to another
/// sink. Report sampler-internal failures through a non-`observed` channel.
///
/// The guard also means that the sink does not call `sample` for an emission
/// made from inside a processor. Such a nested emission never reaches any
/// processor, so a sampler that holds events keeps them instead of releasing
/// them into a dispatch that the sink must skip.
///
/// # Examples
///
/// ```
/// use observed::sampling::{EarlySampler, EventMetadata};
///
/// /// Processes events from `important_crate` and drops all other events.
/// struct OnlyOneCrate;
///
/// impl EarlySampler for OnlyOneCrate {
///     fn sample(&self, event: EventMetadata) -> Vec<EventMetadata> {
///         match event.view().source_crate().as_deref() {
///             Some("important_crate") => vec![event],
///             _ => Vec::new(),
///         }
///     }
/// }
/// ```
pub trait EarlySampler: Send + Sync {
    /// Returns the events that the sink processes now, in the order that the
    /// sink processes them.
    ///
    /// The sampler receives the current event by move and returns zero, one,
    /// or more events. An empty vector processes nothing. The sampler may keep
    /// any event that it does not return, for example in a buffer, and return
    /// it from a later call; each returned event keeps the timestamp, the
    /// enrichment, and the sink id of its own emission.
    ///
    /// Only a later call can release a kept event. The sink never asks for the
    /// kept events, so an event that no later call returns never reaches a
    /// processor, and it goes away with the sampler - see
    /// [the module docs](super#a-kept-event-waits-for-the-next-call).
    ///
    /// The sink calls this method once per emission. The method must have a
    /// low cost and must not emit through `observed`. See
    /// [the trait-level warning](Self#must-not-emit-through-observed).
    fn sample(&self, event: EventMetadata) -> Vec<EventMetadata>;
}
