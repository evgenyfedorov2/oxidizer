// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! The owned event value that an [`EarlySampler`](super::EarlySampler) receives.

use std::time::SystemTime;

use crate::SinkId;
use crate::enrichment::OptEnrichmentNode;
use crate::interop::DynEvent;
use crate::processing::EventView;

/// One complete event plus the emission context that the pipeline needs to
/// process it.
///
/// An [`EarlySampler`](super::EarlySampler) receives this value by move. The
/// event is already constructed, so the sampler can read every field, body,
/// and enrichment through [`view`](Self::view). The sampler decides what
/// happens next:
///
/// - Return the value to process it now.
/// - Keep the value in the sampler to process it from a later
///   [`sample`](super::EarlySampler::sample) call.
/// - Drop the value to discard the event.
///
/// The value carries the timestamp, the enrichment snapshot, and the sink id.
/// A value that a sampler keeps for a while therefore keeps the context of its
/// original emission.
/// Only a later [`sample`](super::EarlySampler::sample) call can release a
/// kept value - see
/// [the module docs](super#a-kept-event-waits-for-the-next-call).
///
/// # Examples
///
/// ```
/// use observed::sampling::{EarlySampler, EventMetadata};
///
/// struct DropNoisyEvents;
///
/// impl EarlySampler for DropNoisyEvents {
///     fn sample(&self, event: EventMetadata) -> Vec<EventMetadata> {
///         if event.view().name() == "noisy.event" {
///             Vec::new()
///         } else {
///             vec![event]
///         }
///     }
/// }
/// ```
pub struct EventMetadata {
    event: Box<dyn DynEvent>,
    id: SinkId,
    isolated_enrichment: bool,
    enrichment: OptEnrichmentNode,
    timestamp: SystemTime,
}

impl EventMetadata {
    /// Takes ownership of a constructed event and of the emission context that
    /// the sink captured before it called the sampler.
    pub(crate) const fn new(
        event: Box<dyn DynEvent>,
        id: SinkId,
        isolated_enrichment: bool,
        enrichment: OptEnrichmentNode,
        timestamp: SystemTime,
    ) -> Self {
        Self {
            event,
            id,
            isolated_enrichment,
            enrichment,
            timestamp,
        }
    }

    /// Borrows the event as the same [`EventView`] that a processor receives.
    ///
    /// The view reports the name, body, severity, source location,
    /// description, and timestamp of the event, and it visits the event fields
    /// and the enrichment entries that were active at emission time.
    #[must_use]
    pub fn view(&self) -> EventView<'_> {
        EventView::new(
            &*self.event,
            self.enrichment.clone(),
            self.isolated_enrichment,
            self.id,
            self.timestamp,
        )
    }
}

impl std::fmt::Debug for EventMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .field("name", &self.event.name())
            .field("sink", &self.id)
            .field("timestamp", &self.timestamp)
            .finish_non_exhaustive()
    }
}
