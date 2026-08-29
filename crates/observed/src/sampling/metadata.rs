// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Pre-construction event metadata passed to an [`EarlySampler`](super::EarlySampler).

use std::borrow::Cow;

use crate::interop::DynEvent;
use crate::metadata::{EventDescription, SourceLocation};

/// Borrowed event metadata that is available before event construction.
///
/// An [`EarlySampler`](super::EarlySampler) receives this *before* the event's
/// typed value is constructed, so it exposes identity and source-location
/// metadata but not field values, enrichments, or a dynamic body. A typed
/// event uses the static [`SourceLocation`] that `emit!` captured. A dynamic
/// event calls the applicable [`DynEvent`] source accessor only when a caller
/// requests the value.
///
/// # Examples
///
/// ```
/// use observed::sampling::{EarlySampler, EventMetadata, SamplingDecision};
///
/// struct DropNoisyEvents;
///
/// impl EarlySampler for DropNoisyEvents {
///     fn sample(&self, event: &EventMetadata<'_>) -> SamplingDecision {
///         if event.description().name() == "noisy.event" {
///             SamplingDecision::Drop
///         } else {
///             SamplingDecision::Continue
///         }
///     }
/// }
/// ```
pub struct EventMetadata<'a> {
    description: EventDescription,
    source: Source<'a>,
}

/// Source of the event-location metadata.
enum Source<'a> {
    /// Static location that `emit!` captured for a typed event.
    Typed(SourceLocation),
    /// Dynamic event that supplies source values on demand.
    Dyn(&'a dyn DynEvent),
}

impl<'a> EventMetadata<'a> {
    /// Projects metadata for a typed event from its static description and
    /// `emit!`-captured source location.
    pub(crate) fn typed(description: EventDescription, source_location: SourceLocation) -> Self {
        Self {
            description,
            source: Source::Typed(source_location),
        }
    }

    /// Projects metadata for a dynamic (bridged) event, deferring every
    /// source-location accessor to the event itself.
    pub(crate) fn dynamic(description: EventDescription, event: &'a dyn DynEvent) -> Self {
        Self {
            description,
            source: Source::Dyn(event),
        }
    }

    /// Returns the event's compile-time-shaped description (name, signals, metrics).
    #[must_use]
    pub fn description(&self) -> EventDescription {
        self.description
    }

    /// Returns the name of the crate where the event originated, if available.
    ///
    /// For a dynamic event this calls
    /// [`DynEvent::source_crate`](crate::interop::DynEvent::source_crate) - only
    /// when this accessor is invoked.
    #[must_use]
    pub fn source_crate(&self) -> Option<Cow<'static, str>> {
        match self.source {
            Source::Typed(location) => Some(Cow::Borrowed(location.crate_name())),
            Source::Dyn(event) => event.source_crate(),
        }
    }

    /// Returns the source file path, if available.
    ///
    /// For a dynamic event this calls
    /// [`DynEvent::source_file`](crate::interop::DynEvent::source_file) - only
    /// when this accessor is invoked.
    #[must_use]
    pub fn source_file(&self) -> Option<Cow<'static, str>> {
        match self.source {
            Source::Typed(location) => Some(Cow::Borrowed(location.file())),
            Source::Dyn(event) => event.source_file(),
        }
    }

    /// Returns the source line number, if available.
    ///
    /// For a dynamic event this calls
    /// [`DynEvent::source_line`](crate::interop::DynEvent::source_line) - only
    /// when this accessor is invoked.
    #[must_use]
    pub fn source_line(&self) -> Option<u32> {
        match self.source {
            Source::Typed(location) => Some(location.line()),
            Source::Dyn(event) => event.source_line(),
        }
    }
}

impl std::fmt::Debug for EventMetadata<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Do not call dynamic source accessors here. They must remain lazy
        // until a caller requests their values.
        f.debug_struct(std::any::type_name::<Self>())
            .field("description", &self.description)
            .finish_non_exhaustive()
    }
}
