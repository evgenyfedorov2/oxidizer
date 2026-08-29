// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Behavior tests for early sampling on a non-composite sink.
//!
//! These exercise the public `Sink` / `EventView` surface directly - never
//! `emit!` macro expansion or internal cache shape - one behavior per test.

use std::borrow::Cow;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use observed::interop::{DynEvent, emit_dyn_event};
use observed::metadata::{EventDescription, InstrumentKind, LogDescription, MetricDescription, SourceLocation};
use observed::processing::{EventProcessor, EventView, FieldVisitorFn};
use observed::sampling::{EarlySampler, EventMetadata, SamplingDecision};
use observed::{Event, FlushError, Severity, Sink};
use tick::SimpleClock;

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

/// Builds `MixedEvent` via `Sink::emit` directly (bypassing the `emit!`
/// macro) so the test can observe whether the build closure ran at all,
/// which is exactly what a `SamplingDecision::Drop` must prevent.
fn emit_mixed_event(sink: &Sink, evaluated: &Arc<AtomicBool>) {
    let evaluated = Arc::clone(evaluated);
    sink.emit(
        move || {
            evaluated.store(true, Ordering::SeqCst);
            MixedEvent
        },
        SourceLocation::new("observed", "tests/sampling.rs", 1),
    );
}

/// A sampler whose decision is fixed at construction and that counts how many
/// times [`EarlySampler::sample`] ran.
struct ScriptedSampler {
    calls: AtomicU32,
    decision: SamplingDecision,
}

impl ScriptedSampler {
    fn new(decision: SamplingDecision) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicU32::new(0),
            decision,
        })
    }

    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }
}

impl EarlySampler for ScriptedSampler {
    fn sample(&self, _event: &EventMetadata<'_>) -> SamplingDecision {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.decision
    }
}

#[derive(Default)]
struct Recorder {
    process_count: AtomicU32,
}

impl Recorder {
    fn process_count(&self) -> usize {
        self.process_count.load(Ordering::SeqCst) as usize
    }
}

impl EventProcessor for Recorder {
    fn is_interested(&self, _description: &EventDescription) -> bool {
        true
    }

    fn process(&self, _event: &EventView<'_>) {
        self.process_count.fetch_add(1, Ordering::SeqCst);
    }

    fn flush(&self) -> Result<(), FlushError> {
        Ok(())
    }
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
/// [`MetricOnlyRecorder`] to prove `Drop` suppresses *every* signal on a
/// mixed log+metric event, not just the one a single processor happens to
/// watch.
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
// Static interest gates the sampler
// ---------------------------------------------------------------------------

#[test]
fn statically_uninterested_sink_never_calls_sampler() {
    let processor = Arc::new(NeverInterestedProcessor::default());
    let sampler = ScriptedSampler::new(SamplingDecision::Continue);
    let sink = Sink::new(
        "uninterested",
        vec![Arc::clone(&processor) as Arc<dyn EventProcessor>],
        SimpleClock::new_frozen(),
    )
    .with_early_sampler(Arc::clone(&sampler) as Arc<dyn EarlySampler>);

    let evaluated = Arc::new(AtomicBool::new(false));
    emit_mixed_event(&sink, &evaluated);

    assert_eq!(sampler.calls(), 0, "no processor is interested, so the sampler must not run");
    assert_eq!(processor.process_count.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------
// Drop
// ---------------------------------------------------------------------------

#[test]
fn drop_prevents_typed_event_closure_from_running() {
    let processor = Arc::new(Recorder::default());
    let sampler = ScriptedSampler::new(SamplingDecision::Drop);
    let sink = Sink::new(
        "drop-closure",
        vec![Arc::clone(&processor) as Arc<dyn EventProcessor>],
        SimpleClock::new_frozen(),
    )
    .with_early_sampler(Arc::clone(&sampler) as Arc<dyn EarlySampler>);

    let evaluated = Arc::new(AtomicBool::new(false));
    emit_mixed_event(&sink, &evaluated);

    assert_eq!(sampler.calls(), 1);
    assert!(!evaluated.load(Ordering::SeqCst), "Drop must prevent typed construction");
    assert_eq!(processor.process_count(), 0, "no processor may see a dropped event");
}

#[test]
fn drop_suppresses_every_signal_on_a_mixed_log_and_metric_event() {
    let log_processor = Arc::new(LogOnlyRecorder::default());
    let metric_processor = Arc::new(MetricOnlyRecorder::default());
    let sampler = ScriptedSampler::new(SamplingDecision::Drop);
    let sink = Sink::new(
        "drop-mixed",
        vec![
            Arc::clone(&log_processor) as Arc<dyn EventProcessor>,
            Arc::clone(&metric_processor) as Arc<dyn EventProcessor>,
        ],
        SimpleClock::new_frozen(),
    )
    .with_early_sampler(Arc::clone(&sampler) as Arc<dyn EarlySampler>);

    let evaluated = Arc::new(AtomicBool::new(false));
    emit_mixed_event(&sink, &evaluated);

    assert_eq!(sampler.calls(), 1);
    assert!(!evaluated.load(Ordering::SeqCst));
    assert_eq!(log_processor.process_count.load(Ordering::SeqCst), 0);
    assert_eq!(metric_processor.process_count.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------
// Continue
// ---------------------------------------------------------------------------

#[test]
fn continue_dispatches_event() {
    let processor = Arc::new(Recorder::default());
    let sampler = ScriptedSampler::new(SamplingDecision::Continue);
    let sink = Sink::new(
        "continue",
        vec![Arc::clone(&processor) as Arc<dyn EventProcessor>],
        SimpleClock::new_frozen(),
    )
    .with_early_sampler(Arc::clone(&sampler) as Arc<dyn EarlySampler>);

    let evaluated = Arc::new(AtomicBool::new(false));
    emit_mixed_event(&sink, &evaluated);

    assert!(evaluated.load(Ordering::SeqCst));
    assert_eq!(processor.process_count(), 1);
}

// ---------------------------------------------------------------------------
// Dynamic-event source laziness
// ---------------------------------------------------------------------------

/// A `DynEvent` that counts how many times each source accessor is called,
/// so a sampler that only asks for one of them is visible in the counters.
#[derive(Default)]
#[expect(
    clippy::struct_field_names,
    reason = "test fixture: one counter per accessor, symmetrically named for clarity"
)]
struct CountingDynEvent {
    crate_calls: AtomicU32,
    file_calls: AtomicU32,
    line_calls: AtomicU32,
}

impl DynEvent for CountingDynEvent {
    fn name(&self) -> &'static str {
        "dyn.counting"
    }

    fn body(&self) -> Option<Cow<'static, str>> {
        None
    }

    fn source_file(&self) -> Option<Cow<'static, str>> {
        self.file_calls.fetch_add(1, Ordering::SeqCst);
        Some(Cow::Borrowed("dyn/file.rs"))
    }

    fn source_line(&self) -> Option<u32> {
        self.line_calls.fetch_add(1, Ordering::SeqCst);
        Some(11)
    }

    fn source_crate(&self) -> Option<Cow<'static, str>> {
        self.crate_calls.fetch_add(1, Ordering::SeqCst);
        Some(Cow::Borrowed("dyn_crate"))
    }

    fn visit_fields(&self, _visitor: &mut FieldVisitorFn<'_>) -> ControlFlow<()> {
        ControlFlow::Continue(())
    }

    fn description(&self) -> EventDescription {
        EventDescription::new("dyn.counting", None, None, None, false, false)
    }
}

/// A sampler that reads only `source_line`, used to prove the other two
/// accessors stay untouched.
struct ReadsSourceLineOnly;

impl EarlySampler for ReadsSourceLineOnly {
    fn sample(&self, event: &EventMetadata<'_>) -> SamplingDecision {
        let _ = event.source_line();
        SamplingDecision::Continue
    }
}

#[test]
fn dynamic_source_access_reads_only_requested_accessor() {
    let event = CountingDynEvent::default();
    let processor = Arc::new(Recorder::default());
    let sink = Sink::new(
        "dyn-lazy",
        vec![Arc::clone(&processor) as Arc<dyn EventProcessor>],
        SimpleClock::new_frozen(),
    )
    .with_early_sampler(Arc::new(ReadsSourceLineOnly));

    emit_dyn_event(&sink, &event);

    assert_eq!(processor.process_count(), 1);
    assert_eq!(event.line_calls.load(Ordering::SeqCst), 1);
    assert_eq!(event.file_calls.load(Ordering::SeqCst), 0, "source_file must stay untouched");
    assert_eq!(event.crate_calls.load(Ordering::SeqCst), 0, "source_crate must stay untouched");
}

/// A `DynEvent` reporting no source location at all - the "bridge never knew
/// where this came from" case.
struct SourcelessDynEvent;

impl DynEvent for SourcelessDynEvent {
    fn name(&self) -> &'static str {
        "dyn.sourceless"
    }

    fn body(&self) -> Option<Cow<'static, str>> {
        None
    }

    fn source_file(&self) -> Option<Cow<'static, str>> {
        None
    }

    fn source_line(&self) -> Option<u32> {
        None
    }

    fn source_crate(&self) -> Option<Cow<'static, str>> {
        None
    }

    fn visit_fields(&self, _visitor: &mut FieldVisitorFn<'_>) -> ControlFlow<()> {
        ControlFlow::Continue(())
    }

    fn description(&self) -> EventDescription {
        EventDescription::new("dyn.sourceless", None, None, None, false, false)
    }
}

/// The `(crate, file, line)` triple captured by [`RecordsSource`] from a
/// single [`EarlySampler::sample`] call.
type SourceAccessorTriple = (Option<Cow<'static, str>>, Option<Cow<'static, str>>, Option<u32>);

/// A sampler that records every source accessor's answer, so the test can
/// assert on all three at once.
#[derive(Default)]
struct RecordsSource {
    seen: Mutex<Option<SourceAccessorTriple>>,
}

impl EarlySampler for RecordsSource {
    fn sample(&self, event: &EventMetadata<'_>) -> SamplingDecision {
        let captured = (event.source_crate(), event.source_file(), event.source_line());
        *self.seen.lock().expect("lock is not poisoned") = Some(captured);
        SamplingDecision::Continue
    }
}

#[test]
fn sourceless_dynamic_event_reports_none_for_every_source_accessor() {
    let event = SourcelessDynEvent;
    let sampler = Arc::new(RecordsSource::default());
    let processor = Arc::new(Recorder::default());
    let sink = Sink::new(
        "dyn-sourceless",
        vec![Arc::clone(&processor) as Arc<dyn EventProcessor>],
        SimpleClock::new_frozen(),
    )
    .with_early_sampler(Arc::clone(&sampler) as Arc<dyn EarlySampler>);

    emit_dyn_event(&sink, &event);

    let seen = sampler.seen.lock().expect("lock is not poisoned").clone();
    assert_eq!(seen, Some((None, None, None)));
}

#[test]
fn typed_event_metadata_reports_emit_source_location() {
    let sampler = Arc::new(RecordsSource::default());
    let processor = Arc::new(Recorder::default());
    let sink = Sink::new(
        "typed-source",
        vec![Arc::clone(&processor) as Arc<dyn EventProcessor>],
        SimpleClock::new_frozen(),
    )
    .with_early_sampler(Arc::clone(&sampler) as Arc<dyn EarlySampler>);

    sink.emit(|| MixedEvent, SourceLocation::new("typed_crate", "src/typed_event.rs", 77));

    let seen = sampler.seen.lock().expect("lock is not poisoned").clone();
    assert_eq!(
        seen,
        Some((
            Some(Cow::Borrowed("typed_crate")),
            Some(Cow::Borrowed("src/typed_event.rs")),
            Some(77),
        ))
    );
}

// ---------------------------------------------------------------------------
// `with_early_sampler` builder semantics
// ---------------------------------------------------------------------------

#[test]
fn calling_with_early_sampler_twice_replaces_first_sampler() {
    let first = ScriptedSampler::new(SamplingDecision::Drop);
    let second = ScriptedSampler::new(SamplingDecision::Continue);
    let processor = Arc::new(Recorder::default());

    let sink = Sink::new(
        "replace",
        vec![Arc::clone(&processor) as Arc<dyn EventProcessor>],
        SimpleClock::new_frozen(),
    )
    .with_early_sampler(Arc::clone(&first) as Arc<dyn EarlySampler>)
    .with_early_sampler(Arc::clone(&second) as Arc<dyn EarlySampler>);

    let evaluated = Arc::new(AtomicBool::new(false));
    emit_mixed_event(&sink, &evaluated);

    assert_eq!(first.calls(), 0, "the replaced sampler must never run");
    assert_eq!(second.calls(), 1);
    assert_eq!(
        processor.process_count(),
        1,
        "the second sampler's Continue must let the event through"
    );
}

#[test]
fn an_empty_sink_accepts_a_sampler_but_never_consults_it() {
    let sampler = ScriptedSampler::new(SamplingDecision::Continue);
    let sink = Sink::new("empty", Vec::new(), SimpleClock::new_frozen()).with_early_sampler(Arc::clone(&sampler) as Arc<dyn EarlySampler>);

    let evaluated = Arc::new(AtomicBool::new(false));
    emit_mixed_event(&sink, &evaluated);

    assert_eq!(
        sampler.calls(),
        0,
        "a sink with no processors has nothing to gate, so the sampler is never consulted"
    );
    assert!(!evaluated.load(Ordering::SeqCst));
}

#[test]
fn with_early_sampler_keeps_composite_sink_unchanged() {
    let processor = Arc::new(Recorder::default());
    let child = Sink::new(
        "child",
        vec![Arc::clone(&processor) as Arc<dyn EventProcessor>],
        SimpleClock::new_frozen(),
    );
    let sampler = ScriptedSampler::new(SamplingDecision::Drop);
    let sink = Sink::composite([child]).with_early_sampler(Arc::clone(&sampler) as Arc<dyn EarlySampler>);

    let evaluated = Arc::new(AtomicBool::new(false));
    emit_mixed_event(&sink, &evaluated);

    assert_eq!(sampler.calls(), 0);
    assert!(evaluated.load(Ordering::SeqCst));
    assert_eq!(processor.process_count(), 1);
}

#[test]
fn with_early_sampler_keeps_noop_sink_unchanged() {
    let sampler = ScriptedSampler::new(SamplingDecision::Continue);
    let sink = Sink::noop().with_early_sampler(Arc::clone(&sampler) as Arc<dyn EarlySampler>);

    let evaluated = Arc::new(AtomicBool::new(false));
    emit_mixed_event(&sink, &evaluated);

    assert!(sink.is_noop());
    assert_eq!(sampler.calls(), 0);
    assert!(!evaluated.load(Ordering::SeqCst));
}
