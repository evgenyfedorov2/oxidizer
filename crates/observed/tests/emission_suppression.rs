// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Behavioral tests for thread-local emission suppression through real sinks.

#![allow(
    clippy::panic,
    clippy::unwrap_used,
    reason = "Tests exercise unwinding and use unwrap for assertions."
)]

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::thread;

use observed::metadata::EventDescription;
use observed::processing::{EventProcessor, EventView, with_emission_suppressed};
use observed::{FlushError, Sink, emit, event};
use tick::SimpleClock;

#[event("log")]
#[info]
struct LogEvent;

#[event("metric")]
#[counter(name = "metric.count")]
struct MetricEvent;

#[event("custom", disabled)]
struct CustomEvent;

#[event("mixed")]
#[info]
#[counter(name = "mixed.count")]
struct MixedEvent;

type RecordedEvents = Arc<Mutex<Vec<&'static str>>>;

struct RecordingProcessor<F> {
    events: RecordedEvents,
    interested: fn(&EventDescription) -> bool,
    on_process: F,
}

impl<F: Fn() + Send + Sync> EventProcessor for RecordingProcessor<F> {
    #[mutants::skip]
    fn is_interested(&self, description: &EventDescription) -> bool {
        (self.interested)(description)
    }

    #[mutants::skip]
    fn process(&self, event: &EventView<'_>) {
        self.events.lock().unwrap().push(event.name());
        (self.on_process)();
    }

    #[mutants::skip]
    fn flush(&self) -> Result<(), FlushError> {
        Ok(())
    }
}

#[mutants::skip]
fn recording_sink(id: &'static str, on_process: impl Fn() + Send + Sync + 'static) -> (Sink, RecordedEvents) {
    let events = RecordedEvents::default();
    let sink = Sink::new(
        id,
        vec![Arc::new(RecordingProcessor {
            events: Arc::clone(&events),
            interested: |_| true,
            on_process,
        })],
        SimpleClock::new_frozen(),
    );
    (sink, events)
}

#[test]
fn suppression_runs_operation_and_returns_value() {
    let (sink, events) = recording_sink("test", || {});
    let mut calls = 0;
    let value = String::from("returned");

    emit!(sink, LogEvent);
    let result = with_emission_suppressed(|| {
        calls += 1;
        emit!(sink, CustomEvent);
        value
    });
    emit!(sink, MetricEvent);

    assert_eq!(calls, 1);
    assert_eq!(result, "returned");
    assert_eq!(*events.lock().unwrap(), ["log", "metric"]);
}

#[test]
fn suppression_nests_without_releasing_outer_scope() {
    let (sink, events) = recording_sink("test", || {});
    let mut calls = Vec::new();

    emit!(sink, LogEvent);
    let result = with_emission_suppressed(|| {
        calls.push("outer");
        emit!(sink, CustomEvent);
        let inner = with_emission_suppressed(|| {
            calls.push("inner");
            emit!(sink, MetricEvent);
            41
        });
        emit!(sink, MixedEvent);
        assert_eq!(*events.lock().unwrap(), ["log"]);
        inner + 1
    });
    emit!(sink, MetricEvent);

    assert_eq!(calls, ["outer", "inner"]);
    assert_eq!(result, 42);
    assert_eq!(*events.lock().unwrap(), ["log", "metric"]);
}

#[test]
fn suppression_nests_with_processor_dispatch() {
    let (nested_sink, nested_events) = recording_sink("nested", || {});
    let results = Arc::new(Mutex::new(Vec::new()));
    let (sink, events) = recording_sink("outer", {
        let nested_sink = nested_sink.clone();
        let results = Arc::clone(&results);
        move || {
            emit!(nested_sink, LogEvent);
            let result = with_emission_suppressed(|| {
                emit!(nested_sink, MetricEvent);
                let inner = with_emission_suppressed(|| {
                    emit!(nested_sink, CustomEvent);
                    41
                });
                emit!(nested_sink, MixedEvent);
                inner + 1
            });
            results.lock().unwrap().push(result);
            emit!(nested_sink, LogEvent);
        }
    });

    emit!(sink, MixedEvent);
    emit!(nested_sink, CustomEvent);

    assert_eq!(*results.lock().unwrap(), [42]);
    assert_eq!(*events.lock().unwrap(), ["mixed"]);
    assert_eq!(*nested_events.lock().unwrap(), ["custom"]);
}

#[test]
fn suppression_restores_emission_after_panic() {
    let (sink, events) = recording_sink("test", || {});

    emit!(sink, LogEvent);
    catch_unwind(AssertUnwindSafe(|| {
        with_emission_suppressed(|| {
            emit!(sink, CustomEvent);
            panic!("operation failed");
        });
    }))
    .unwrap_err();
    emit!(sink, MetricEvent);

    assert_eq!(*events.lock().unwrap(), ["log", "metric"]);
}

#[test]
fn nested_panic_preserves_outer_suppression() {
    let (sink, events) = recording_sink("test", || {});

    emit!(sink, LogEvent);
    with_emission_suppressed(|| {
        catch_unwind(AssertUnwindSafe(|| {
            with_emission_suppressed(|| {
                emit!(sink, CustomEvent);
                panic!("nested operation failed");
            });
        }))
        .unwrap_err();
        emit!(sink, MixedEvent);
        assert_eq!(*events.lock().unwrap(), ["log"]);
    });
    emit!(sink, MetricEvent);

    assert_eq!(*events.lock().unwrap(), ["log", "metric"]);
}

#[test]
fn nested_panic_preserves_processor_dispatch() {
    let (nested_sink, nested_events) = recording_sink("nested", || {});
    let (sink, events) = recording_sink("outer", {
        let nested_sink = nested_sink.clone();
        move || {
            catch_unwind(AssertUnwindSafe(|| {
                with_emission_suppressed(|| {
                    emit!(nested_sink, CustomEvent);
                    panic!("nested operation failed");
                });
            }))
            .unwrap_err();
            emit!(nested_sink, MixedEvent);
        }
    });

    emit!(sink, LogEvent);
    emit!(nested_sink, MetricEvent);

    assert_eq!(*events.lock().unwrap(), ["log"]);
    assert_eq!(*nested_events.lock().unwrap(), ["metric"]);
}

#[test]
fn suppression_covers_distinct_sinks_and_every_processor() {
    let log_events = RecordedEvents::default();
    let metric_events = RecordedEvents::default();
    let first = Sink::new(
        "first",
        vec![
            Arc::new(RecordingProcessor {
                events: Arc::clone(&log_events),
                interested: EventDescription::is_log,
                on_process: || {},
            }),
            Arc::new(RecordingProcessor {
                events: Arc::clone(&metric_events),
                interested: EventDescription::contains_metrics,
                on_process: || {},
            }),
        ],
        SimpleClock::new_frozen(),
    );
    let (second, custom_events) = recording_sink("second", || {});
    let composite = Sink::composite([first.clone(), second.clone()]);

    emit!(composite, MixedEvent);
    with_emission_suppressed(|| {
        emit!(first, LogEvent);
        emit!(first, MetricEvent);
        emit!(second, CustomEvent);
        emit!(composite, MixedEvent);

        assert_eq!(*log_events.lock().unwrap(), ["mixed"]);
        assert_eq!(*metric_events.lock().unwrap(), ["mixed"]);
        assert_eq!(*custom_events.lock().unwrap(), ["mixed"]);
    });
    emit!(composite, MixedEvent);

    assert_eq!(*log_events.lock().unwrap(), ["mixed", "mixed"]);
    assert_eq!(*metric_events.lock().unwrap(), ["mixed", "mixed"]);
    assert_eq!(*custom_events.lock().unwrap(), ["mixed", "mixed"]);
}

#[test]
fn suppression_is_thread_local() {
    let (sink, events) = recording_sink("shared", || {});

    with_emission_suppressed(|| {
        emit!(sink, CustomEvent);
        thread::scope(|scope| {
            scope
                .spawn(|| {
                    emit!(sink, MetricEvent);
                    with_emission_suppressed(|| emit!(sink, CustomEvent));
                    emit!(sink, LogEvent);
                })
                .join()
                .unwrap();
        });
        emit!(sink, CustomEvent);
    });
    emit!(sink, MixedEvent);

    assert_eq!(*events.lock().unwrap(), ["metric", "log", "mixed"]);
}
