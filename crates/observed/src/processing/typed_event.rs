// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! The owned carrier that turns a typed event into a [`DynEvent`].

use std::borrow::Cow;
use std::ops::ControlFlow;

use crate::Event;
use crate::interop::DynEvent;
use crate::metadata::{EventDescription, LogDescription, SourceLocation};
use crate::processing::FieldVisitorFn;

/// A constructed typed event together with the source location that
/// [`emit!`](crate::emit!) captured.
///
/// The pipeline owns the event value from the moment the build closure runs,
/// so this type owns `T` rather than borrowing it. That is what lets an
/// [`EarlySampler`](crate::sampling::EarlySampler) keep the event and return
/// it from a later call.
pub(crate) struct TypedEvent<T> {
    event: T,
    source_location: SourceLocation,
}

impl<T: Event> TypedEvent<T> {
    /// Takes ownership of a constructed event and its call site.
    pub(crate) const fn new(event: T, source_location: SourceLocation) -> Self {
        Self { event, source_location }
    }
}

impl<T: Event> DynEvent for TypedEvent<T> {
    fn name(&self) -> &'static str {
        T::DESCRIPTION.name()
    }

    fn body(&self) -> Option<Cow<'static, str>> {
        T::DESCRIPTION.log().and_then(LogDescription::body).map(Cow::Borrowed)
    }

    fn source_file(&self) -> Option<Cow<'static, str>> {
        Some(Cow::Borrowed(self.source_location.file()))
    }

    fn source_line(&self) -> Option<u32> {
        Some(self.source_location.line())
    }

    fn source_crate(&self) -> Option<Cow<'static, str>> {
        Some(Cow::Borrowed(self.source_location.crate_name()))
    }

    fn visit_fields(&self, visitor: &mut FieldVisitorFn<'_>) -> ControlFlow<()> {
        self.event.visit_fields(visitor)
    }

    fn description(&self) -> EventDescription {
        T::DESCRIPTION
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests {
    use super::*;

    struct TestEvent;

    impl Event for TestEvent {
        const DESCRIPTION: EventDescription = EventDescription::new("test.event", None, None, None, false, false);

        fn visit_fields(&self, _visitor: &mut FieldVisitorFn<'_>) -> ControlFlow<()> {
            ControlFlow::Continue(())
        }
    }

    #[test]
    fn a_typed_event_reports_its_name_and_captured_source_location() {
        // `emit!` captures the call site and the owned event is what carries
        // it to a processor, so each accessor must report its own part of it.
        let owned = TypedEvent::new(TestEvent, SourceLocation::new("observed", "crates/observed/src/lib.rs", 42));

        assert_eq!(owned.name(), "test.event");
        assert_eq!(owned.source_file(), Some(Cow::Borrowed("crates/observed/src/lib.rs")));
        assert_eq!(owned.source_line(), Some(42));
        assert_eq!(owned.source_crate(), Some(Cow::Borrowed("observed")));
    }
}
