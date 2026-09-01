// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Behavior tests for early sampling on a non-composite sink.
//!
//! These exercise the public `Sink` / `EventView` / `EarlySampler` surface -
//! never internal storage - one behavior per test.

use std::borrow::Cow;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use data_privacy::simple_redactor::{SimpleRedactor, SimpleRedactorMode};
use observed::enrichment::EnrichFnExt;
use observed::interop::{DynEvent, emit_dyn_event};
use observed::metadata::{EventDescription, FieldDescriptor, InstrumentKind, LogDescription, MetricDescription, SourceLocation};
use observed::processing::{EventProcessor, EventView, FieldVisitorFn};
use observed::sampling::{EarlySampler, EventMetadata};
use observed::{Enrichment, Event, FlushError, Severity, Sink, Value, emit, event};
use tick::{ClockControl, SimpleClock};

// ---------------------------------------------------------------------------
// Shared test fixtures
// ---------------------------------------------------------------------------

/// A typed event carrying both a log and a metric signal, so a single
/// emission can be checked against both kinds of processor at once.
struct MixedEvent;

impl Event for MixedEvent {
    const DESCRIPTION: EventDescription = EventDescription::new(
        "mixed.event",
        None,
        Some(LogDescription::new("mixed.event", Severity::Info, None)),
        Some(MetricDescription::new("mixed.count", InstrumentKind::Counter, "", "")),
        false,
        false,
    );

    fn visit_fields(&self, _visitor: &mut FieldVisitorFn<'_>) -> ControlFlow<()> {
        ControlFlow::Continue(())
    }
}

/// A typed log event carrying one runtime value, so a test can prove which
/// event a processor received and that its value survived a stay in the
/// sampler.
#[event("counted.event")]
#[info("A counted event")]
struct CountedEvent {
    #[unredacted]
    count: i64,
}

/// Enrichment attached around an emission, so a test can prove that a held
/// event keeps the enrichment of its own emission.
#[derive(Enrichment)]
struct RequestCtx {
    #[unredacted]
    request_id: i64,
}

/// Builds `MixedEvent` via `Sink::emit` directly (bypassing the `emit!`
/// macro) so the test controls the source location the event reports.
fn emit_mixed_event(sink: &Sink) {
    sink.emit(|| MixedEvent, SourceLocation::new("observed", "tests/sampling.rs", 1));
}

/// A redactor that returns every value unchanged, so a test reads the value
/// the event carries.
fn passthrough_redactor() -> SimpleRedactor {
    SimpleRedactor::with_mode(SimpleRedactorMode::Passthrough)
}

/// Collects the event fields as key-value pairs.
fn collect_fields(event: &EventView<'_>) -> Vec<(String, Value)> {
    let mut collected = Vec::new();
    _ = event.visit_fields(&mut |descriptor, getter| {
        collected.push((descriptor.field_name().to_owned(), getter(&passthrough_redactor())));
        ControlFlow::Continue(())
    });
    collected
}

/// Collects the enrichment entries as key-value pairs.
fn collect_enrichments(event: &EventView<'_>) -> Vec<(String, Value)> {
    let mut collected = Vec::new();
    _ = event.visit_enrichments(&mut |descriptor, getter| {
        collected.push((descriptor.field_name().to_owned(), getter(&passthrough_redactor())));
        ControlFlow::Continue(())
    });
    collected
}

/// Everything one processor observed about one event.
#[derive(Debug, Clone, PartialEq)]
struct Seen {
    name: &'static str,
    timestamp: SystemTime,
    source_crate: Option<Cow<'static, str>>,
    source_line: Option<u32>,
    fields: Vec<(String, Value)>,
    enrichments: Vec<(String, Value)>,
}

impl Seen {
    fn of(event: &EventView<'_>) -> Self {
        Self {
            name: event.name(),
            timestamp: event.timestamp(),
            source_crate: event.source_crate(),
            source_line: event.source_line(),
            fields: collect_fields(event),
            enrichments: collect_enrichments(event),
        }
    }
}

/// A processor that accepts every event and records what it received.
#[derive(Default)]
struct Recorder {
    seen: Mutex<Vec<Seen>>,
}

impl Recorder {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("lock is not poisoned").clone()
    }

    fn names(&self) -> Vec<&'static str> {
        self.seen().iter().map(|seen| seen.name).collect()
    }

    fn counts(&self) -> Vec<Value> {
        self.seen()
            .iter()
            .flat_map(|seen| seen.fields.iter().map(|(_, value)| value.clone()).collect::<Vec<_>>())
            .collect()
    }

    fn count(&self) -> usize {
        self.seen.lock().expect("lock is not poisoned").len()
    }
}

impl EventProcessor for Recorder {
    fn is_interested(&self, _description: &EventDescription) -> bool {
        true
    }

    fn process(&self, event: &EventView<'_>) {
        self.seen.lock().expect("lock is not poisoned").push(Seen::of(event));
    }

    fn flush(&self) -> Result<(), FlushError> {
        Ok(())
    }
}

/// Builds a sink that records everything, using the supplied clock.
fn recording_sink(id: &'static str, clock: impl AsRef<SimpleClock>) -> (Sink, Arc<Recorder>) {
    let recorder = Recorder::new();
    let sink = Sink::new(id, vec![Arc::clone(&recorder) as Arc<dyn EventProcessor>], clock);
    (sink, recorder)
}

/// A processor that declares no interest in anything - used to prove the
/// sampler is skipped when static interest is already absent.
#[derive(Default)]
struct NeverInterestedProcessor {
    process_count: AtomicU32,
}

impl EventProcessor for NeverInterestedProcessor {
    fn is_interested(&self, _description: &EventDescription) -> bool {
        false
    }

    fn process(&self, _event: &EventView<'_>) {
        self.process_count.fetch_add(1, Ordering::SeqCst);
    }

    fn flush(&self) -> Result<(), FlushError> {
        Ok(())
    }
}

/// A processor that only wants log-signal events, used with
/// [`MetricOnlyRecorder`] to prove an empty sampler result suppresses *every*
/// signal on a mixed log+metric event, not just the one a single processor
/// happens to watch.
#[derive(Default)]
struct LogOnlyRecorder {
    process_count: AtomicU32,
}

impl EventProcessor for LogOnlyRecorder {
    fn is_interested(&self, description: &EventDescription) -> bool {
        description.is_log()
    }

    fn process(&self, _event: &EventView<'_>) {
        self.process_count.fetch_add(1, Ordering::SeqCst);
    }

    fn flush(&self) -> Result<(), FlushError> {
        Ok(())
    }
}

#[derive(Default)]
struct MetricOnlyRecorder {
    process_count: AtomicU32,
}

impl EventProcessor for MetricOnlyRecorder {
    fn is_interested(&self, description: &EventDescription) -> bool {
        description.contains_metrics()
    }

    fn process(&self, _event: &EventView<'_>) {
        self.process_count.fetch_add(1, Ordering::SeqCst);
    }

    fn flush(&self) -> Result<(), FlushError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Samplers
// ---------------------------------------------------------------------------

/// A sampler that returns every event it receives and counts its calls.
#[derive(Clone, Default)]
struct PassThrough {
    calls: Arc<AtomicU32>,
}

impl PassThrough {
    fn new() -> Self {
        Self::default()
    }

    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }
}

impl EarlySampler for PassThrough {
    fn sample(&self, event: EventMetadata) -> Vec<EventMetadata> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        vec![event]
    }
}

/// A sampler that returns no event at all and counts its calls.
#[derive(Clone, Default)]
struct DropEverything {
    calls: Arc<AtomicU32>,
}

impl DropEverything {
    fn new() -> Self {
        Self::default()
    }

    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }
}

impl EarlySampler for DropEverything {
    fn sample(&self, _event: EventMetadata) -> Vec<EventMetadata> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Vec::new()
    }
}

/// A sampler that keeps the first event it receives and returns it, ahead of
/// the current event, on the next call.
#[derive(Default)]
struct HoldFirst {
    held: Mutex<Option<EventMetadata>>,
}

impl HoldFirst {
    fn new() -> Self {
        Self::default()
    }
}

impl EarlySampler for HoldFirst {
    fn sample(&self, event: EventMetadata) -> Vec<EventMetadata> {
        let mut held = self.held.lock().expect("lock is not poisoned");
        let Some(first) = held.take() else {
            *held = Some(event);
            return Vec::new();
        };
        vec![first, event]
    }
}

// ---------------------------------------------------------------------------
// Static interest gates the sampler
// ---------------------------------------------------------------------------

#[test]
fn statically_uninterested_sink_never_calls_sampler() {
    let processor = Arc::new(NeverInterestedProcessor::default());
    let sampler = PassThrough::new();
    let sink = Sink::new(
        "uninterested",
        vec![Arc::clone(&processor) as Arc<dyn EventProcessor>],
        SimpleClock::new_frozen(),
    )
    .with_early_sampler(sampler.clone());

    emit_mixed_event(&sink);

    assert_eq!(sampler.calls(), 0, "no processor is interested, so the sampler must not run");
    assert_eq!(processor.process_count.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------
// Returning the current event
// ---------------------------------------------------------------------------

#[test]
fn returning_current_event_processes_it() {
    let (sink, recorder) = recording_sink("pass-through", SimpleClock::new_frozen());
    let sampler = PassThrough::new();
    let sink = sink.with_early_sampler(sampler.clone());

    emit!(sink, CountedEvent { count: 7 });

    assert_eq!(sampler.calls(), 1);
    assert_eq!(recorder.names(), vec!["counted.event"]);
    assert_eq!(recorder.counts(), vec![Value::from(7_i64)]);
}

// ---------------------------------------------------------------------------
// Returning nothing
// ---------------------------------------------------------------------------

#[test]
fn returning_no_event_processes_nothing() {
    let (sink, recorder) = recording_sink("drop", SimpleClock::new_frozen());
    let sampler = DropEverything::new();
    let sink = sink.with_early_sampler(sampler.clone());

    emit!(sink, CountedEvent { count: 7 });

    assert_eq!(sampler.calls(), 1);
    assert_eq!(recorder.count(), 0, "an empty result processes nothing");
}

#[test]
fn returning_no_event_suppresses_every_signal_on_a_mixed_log_and_metric_event() {
    let log_processor = Arc::new(LogOnlyRecorder::default());
    let metric_processor = Arc::new(MetricOnlyRecorder::default());
    let sampler = DropEverything::new();
    let sink = Sink::new(
        "drop-mixed",
        vec![
            Arc::clone(&log_processor) as Arc<dyn EventProcessor>,
            Arc::clone(&metric_processor) as Arc<dyn EventProcessor>,
        ],
        SimpleClock::new_frozen(),
    )
    .with_early_sampler(sampler.clone());

    emit_mixed_event(&sink);

    assert_eq!(sampler.calls(), 1);
    assert_eq!(log_processor.process_count.load(Ordering::SeqCst), 0);
    assert_eq!(metric_processor.process_count.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------
// Holding an event and releasing it later
// ---------------------------------------------------------------------------

/// A sampler that keeps the first event and, on the next call, returns only
/// that kept event - the current event is dropped.
#[derive(Default)]
struct ReleaseFirstDropSecond {
    held: Mutex<Option<EventMetadata>>,
}

impl EarlySampler for ReleaseFirstDropSecond {
    fn sample(&self, event: EventMetadata) -> Vec<EventMetadata> {
        let mut held = self.held.lock().expect("lock is not poisoned");
        let Some(first) = held.take() else {
            *held = Some(event);
            return Vec::new();
        };
        vec![first]
    }
}

#[test]
fn held_event_is_processed_by_later_call_that_drops_current_event() {
    let (sink, recorder) = recording_sink("hold", SimpleClock::new_frozen());
    let sink = sink.with_early_sampler(ReleaseFirstDropSecond::default());

    emit!(sink, CountedEvent { count: 1 });
    assert_eq!(recorder.count(), 0, "the first event is held, so nothing is processed yet");

    emit!(sink, CountedEvent { count: 2 });

    assert_eq!(
        recorder.counts(),
        vec![Value::from(1_i64)],
        "the released event keeps its own value, and the dropped current event is not processed"
    );
}

#[test]
fn returned_events_are_processed_in_vector_order() {
    let (sink, recorder) = recording_sink("order", SimpleClock::new_frozen());
    let sink = sink.with_early_sampler(HoldFirst::new());

    emit!(sink, CountedEvent { count: 1 });
    emit!(sink, CountedEvent { count: 2 });

    assert_eq!(
        recorder.counts(),
        vec![Value::from(1_i64), Value::from(2_i64)],
        "the held event comes first because the sampler returned it first"
    );
}

#[test]
fn a_held_event_keeps_its_timestamp_source_and_enrichment() {
    let control = ClockControl::new();
    let clock = control.to_simple_clock();
    let first_time = clock.system_time();
    let (sink, recorder) = recording_sink("held-context", &clock);
    let sink = sink.with_early_sampler(HoldFirst::new());

    (|| sink.emit(|| CountedEvent { count: 1 }, SourceLocation::new("first_crate", "first.rs", 11)))
        .enrich(&sink, RequestCtx { request_id: 42 })();

    control.advance(Duration::from_mins(1));
    sink.emit(|| CountedEvent { count: 2 }, SourceLocation::new("second_crate", "second.rs", 22));

    let released = recorder.seen();
    assert_eq!(released.len(), 2);
    assert_eq!(released[0].timestamp, first_time, "the held event keeps its own timestamp");
    assert_eq!(released[0].source_crate, Some(Cow::Borrowed("first_crate")));
    assert_eq!(released[0].source_line, Some(11));
    assert_eq!(released[0].fields, vec![("count".to_owned(), Value::from(1_i64))]);
    assert_eq!(
        released[0].enrichments,
        vec![("request_id".to_owned(), Value::from(42_i64))],
        "the held event keeps the enrichment that was active at its own emission"
    );

    assert_eq!(released[1].source_crate, Some(Cow::Borrowed("second_crate")));
    assert!(released[1].enrichments.is_empty(), "the second emission carries no enrichment");
}

// ---------------------------------------------------------------------------
// Reading the event inside the sampler
// ---------------------------------------------------------------------------

/// A sampler that records the runtime fields and enrichments of the event it
/// receives, proving the whole event is readable before the decision.
#[derive(Clone, Default)]
struct RecordsEventData {
    seen: Arc<Mutex<Option<Seen>>>,
}

impl EarlySampler for RecordsEventData {
    fn sample(&self, event: EventMetadata) -> Vec<EventMetadata> {
        *self.seen.lock().expect("lock is not poisoned") = Some(Seen::of(&event.view()));
        vec![event]
    }
}

#[test]
fn sampler_reads_runtime_fields_and_event_enrichments() {
    let sampler = RecordsEventData::default();
    let (sink, recorder) = recording_sink("inspect", SimpleClock::new_frozen());
    let sink = sink.with_early_sampler(sampler.clone());

    (|| sink.emit(|| CountedEvent { count: 5 }, SourceLocation::new("app", "app.rs", 33))).enrich(&sink, RequestCtx { request_id: 9 })();

    let seen = sampler.seen.lock().expect("lock is not poisoned").clone().expect("the sampler ran");
    assert_eq!(seen.name, "counted.event");
    assert_eq!(seen.source_crate, Some(Cow::Borrowed("app")));
    assert_eq!(seen.source_line, Some(33));
    assert_eq!(seen.fields, vec![("count".to_owned(), Value::from(5_i64))]);
    assert_eq!(seen.enrichments, vec![("request_id".to_owned(), Value::from(9_i64))]);
    assert_eq!(recorder.seen(), vec![seen], "the processor sees exactly what the sampler saw");
}

// ---------------------------------------------------------------------------
// Reentrancy
// ---------------------------------------------------------------------------

/// A processor that emits one event of its own while it processes an event,
/// which the reentrancy guard must skip.
#[derive(Default)]
struct EmittingProcessor {
    sink: OnceLock<Sink>,
    process_count: AtomicU32,
}

impl EventProcessor for EmittingProcessor {
    fn is_interested(&self, _description: &EventDescription) -> bool {
        true
    }

    fn process(&self, _event: &EventView<'_>) {
        self.process_count.fetch_add(1, Ordering::SeqCst);
        if let Some(sink) = self.sink.get() {
            emit!(sink, CountedEvent { count: 99 });
        }
    }

    fn flush(&self) -> Result<(), FlushError> {
        Ok(())
    }
}

#[test]
fn emission_from_processor_never_reaches_sampler() {
    // A nested emission cannot reach any processor, so calling the sampler for
    // it would make a sampler that holds events release them into a dispatch
    // that the sink must skip, and those events would be lost.
    let processor = Arc::new(EmittingProcessor::default());
    let sampler = PassThrough::new();
    let sink = Sink::new(
        "reentrant",
        vec![Arc::clone(&processor) as Arc<dyn EventProcessor>],
        SimpleClock::new_frozen(),
    )
    .with_early_sampler(sampler.clone());
    processor.sink.set(sink.clone()).expect("the sink is set once");

    emit!(sink, CountedEvent { count: 1 });

    assert_eq!(sampler.calls(), 1, "the nested emission must not call the sampler");
    assert_eq!(processor.process_count.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// The path without a sampler
// ---------------------------------------------------------------------------

#[test]
fn a_sink_without_a_sampler_processes_every_event() {
    let (sink, recorder) = recording_sink("no-sampler", SimpleClock::new_frozen());

    emit!(sink, CountedEvent { count: 1 });
    emit!(sink, CountedEvent { count: 2 });

    assert_eq!(recorder.counts(), vec![Value::from(1_i64), Value::from(2_i64)]);
}

// ---------------------------------------------------------------------------
// Owned dynamic events
// ---------------------------------------------------------------------------

/// A bridged event that reports a runtime name and one runtime field, so a
/// test can prove that an owned dynamic event survives a stay in the sampler.
struct BridgedEvent {
    label: &'static str,
}

impl DynEvent for BridgedEvent {
    fn name(&self) -> &'static str {
        "bridged.event"
    }

    fn body(&self) -> Option<Cow<'static, str>> {
        Some(Cow::Borrowed("a bridged message"))
    }

    fn source_file(&self) -> Option<Cow<'static, str>> {
        Some(Cow::Borrowed("bridge.rs"))
    }

    fn source_line(&self) -> Option<u32> {
        Some(55)
    }

    fn source_crate(&self) -> Option<Cow<'static, str>> {
        Some(Cow::Borrowed("bridge_crate"))
    }

    fn visit_fields(&self, visitor: &mut FieldVisitorFn<'_>) -> ControlFlow<()> {
        let label = self.label;
        visitor(&FieldDescriptor::log_only("label"), &move |_redactor| Value::from(label))
    }

    fn description(&self) -> EventDescription {
        EventDescription::new(
            "bridged.event",
            None,
            Some(LogDescription::new("bridged.event", Severity::Info, None)),
            None,
            false,
            false,
        )
    }
}

#[test]
fn owned_dynamic_event_reaches_processors() {
    let (sink, recorder) = recording_sink("dyn", SimpleClock::new_frozen());

    emit_dyn_event(&sink, BridgedEvent { label: "one" });

    assert_eq!(recorder.names(), vec!["bridged.event"]);
    assert_eq!(recorder.counts(), vec![Value::from("one")]);
}

#[test]
fn a_held_dynamic_event_keeps_its_own_values() {
    let (sink, recorder) = recording_sink("dyn-hold", SimpleClock::new_frozen());
    let sink = sink.with_early_sampler(HoldFirst::new());

    emit_dyn_event(&sink, BridgedEvent { label: "first" });
    assert_eq!(recorder.count(), 0, "the first dynamic event is held");

    emit_dyn_event(&sink, BridgedEvent { label: "second" });

    assert_eq!(recorder.counts(), vec![Value::from("first"), Value::from("second")]);
    assert_eq!(recorder.seen()[0].source_line, Some(55));
}

// ---------------------------------------------------------------------------
// `with_early_sampler` builder semantics
// ---------------------------------------------------------------------------

#[test]
fn calling_with_early_sampler_twice_replaces_first_sampler() {
    let first = DropEverything::new();
    let second = PassThrough::new();
    let (sink, recorder) = recording_sink("replace", SimpleClock::new_frozen());

    let sink = sink.with_early_sampler(first.clone()).with_early_sampler(second.clone());

    emit_mixed_event(&sink);

    assert_eq!(first.calls(), 0, "the replaced sampler must never run");
    assert_eq!(second.calls(), 1);
    assert_eq!(recorder.count(), 1, "the second sampler returned the event");
}

#[test]
fn an_empty_sink_accepts_a_sampler_but_never_consults_it() {
    let sampler = PassThrough::new();
    let sink = Sink::new("empty", Vec::new(), SimpleClock::new_frozen()).with_early_sampler(sampler.clone());

    emit_mixed_event(&sink);

    assert_eq!(
        sampler.calls(),
        0,
        "a sink with no processors has nothing to gate, so the sampler is never consulted"
    );
}

#[test]
fn with_early_sampler_keeps_composite_sink_unchanged() {
    let (child, recorder) = recording_sink("child", SimpleClock::new_frozen());
    let sampler = DropEverything::new();
    let sink = Sink::composite([child]).with_early_sampler(sampler.clone());

    emit_mixed_event(&sink);

    assert_eq!(sampler.calls(), 0);
    assert_eq!(recorder.count(), 1, "the composite ignores the sampler");
}

#[test]
fn with_early_sampler_keeps_noop_sink_unchanged() {
    let sampler = PassThrough::new();
    let sink = Sink::noop().with_early_sampler(sampler.clone());

    emit_mixed_event(&sink);

    assert!(sink.is_noop());
    assert_eq!(sampler.calls(), 0);
}
