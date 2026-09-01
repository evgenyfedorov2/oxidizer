// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Demonstrates an early sampler that owns every event it receives.
//!
//! The sampler gets one constructed event per emission and returns the events
//! the sink processes now. This example attaches one [`EarlySampler`] to a
//! sink with a log processor and a metric processor, and emits four events:
//!
//! | Event | What the sampler returns | What to notice |
//! |-------|--------------------------|-----------------|
//! | `login.attempt` (first) | nothing | the sampler keeps the event, so no processor sees it yet |
//! | `login.attempt` (second) | the kept event, then the new one | the kept event still reports its own field value and its own timestamp |
//! | `queue.depth` (metric-only) | nothing, and the event is dropped | the *whole event* is discarded - the metric processor never sees it |
//! | `active.workers` (metric-only) | the event | the metric processor prints it, proving the dropped metric had a live route |
//!
//! Run with:
//! ```sh
//! cargo run -p observed --example early_sampling
//! ```

use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use observed::metadata::EventDescription;
use observed::processing::{EventProcessor, EventView};
use observed::sampling::{EarlySampler, EventMetadata};
use observed::{Sink, Value, emit, event};

#[path = "support/redaction.rs"]
mod redaction;

/// A log-producing event. The sampler keeps the first one it sees and releases
/// it on the next call, so the output shows whether a kept event still reports
/// its own `attempt` value.
#[event("login.attempt")]
#[info("A login attempt")]
struct LoginAttempt {
    #[unredacted]
    attempt: i64,
}

/// A metric-only event that the sampler drops. No processor receives it,
/// because the sampler returns no event at all for it.
#[event("queue.depth")]
#[gauge(depth, name = "queue.depth")]
struct QueueDepth {
    #[unredacted]
    depth: i64,
}

/// A metric-only event that proves the metric route works.
#[event("active.workers")]
#[gauge(count, name = "active.workers")]
struct ActiveWorkers {
    #[unredacted]
    count: i64,
}

/// Keeps the first `login.attempt` and releases it together with the second
/// one. Drops `queue.depth`. Processes every other event at once.
///
/// Only a later `sample` call releases a kept event, so this example emits a
/// second `login.attempt` to release the first one.
#[derive(Default)]
struct DemoSampler {
    held: Mutex<Option<EventMetadata>>,
}

impl EarlySampler for DemoSampler {
    fn sample(&self, event: EventMetadata) -> Vec<EventMetadata> {
        match event.view().name() {
            // Drop the event: return nothing and let the value go out of scope.
            "queue.depth" => Vec::new(),
            "login.attempt" => {
                let mut held = self.held.lock().expect("lock is not poisoned");
                let Some(first) = held.take() else {
                    // Keep the first event: return nothing and store the value.
                    *held = Some(event);
                    return Vec::new();
                };
                // Release the kept event first, then the current one, so the
                // processors see them in emission order.
                vec![first, event]
            }
            _ => vec![event],
        }
    }
}

/// Records the name, the `attempt` field, and the timestamp of every
/// log-producing event it receives.
struct LogRecorder {
    lines: Arc<Mutex<Vec<String>>>,
    start: SystemTime,
    engine: data_privacy::RedactionEngine,
}

impl LogRecorder {
    /// Returns the `attempt` field of the event, if it has one.
    fn attempt(&self, event: &EventView<'_>) -> Option<Value> {
        let mut found = None;
        _ = event.visit_fields(&mut |descriptor, getter| {
            if descriptor.field_name() == "attempt" {
                found = Some(getter(&self.engine));
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        });
        found
    }

    /// Returns the age of the event relative to the start of the example, in
    /// whole seconds, so the printed value stays stable between runs.
    fn age_in_seconds(&self, event: &EventView<'_>) -> u64 {
        event.timestamp().duration_since(self.start).unwrap_or(Duration::ZERO).as_secs()
    }
}

impl EventProcessor for LogRecorder {
    fn is_interested(&self, description: &EventDescription) -> bool {
        description.is_log()
    }

    fn process(&self, event: &EventView<'_>) {
        let line = format!(
            "[LOG] {name} attempt={attempt:?} age={age}s",
            name = event.name(),
            attempt = self.attempt(event),
            age = self.age_in_seconds(event),
        );
        self.lines.lock().expect("lock is not poisoned").push(line);
    }

    fn flush(&self) -> Result<(), observed::FlushError> {
        Ok(())
    }
}

/// Records every metric-producing event it is handed.
struct MetricRecorder {
    lines: Arc<Mutex<Vec<String>>>,
}

impl EventProcessor for MetricRecorder {
    fn is_interested(&self, description: &EventDescription) -> bool {
        description.contains_metrics()
    }

    fn process(&self, event: &EventView<'_>) {
        self.lines
            .lock()
            .expect("lock is not poisoned")
            .push(format!("[METRIC] {name}", name = event.name()));
    }

    fn flush(&self) -> Result<(), observed::FlushError> {
        Ok(())
    }
}

fn main() {
    let log_lines: Arc<Mutex<Vec<String>>> = Arc::default();
    let metric_lines: Arc<Mutex<Vec<String>>> = Arc::default();
    // A controlled clock advances only when the example asks it to, so the
    // timestamp of the kept event is easy to tell apart from the later one.
    let control = tick::ClockControl::new();
    let clock = control.to_simple_clock();

    let sink = Sink::new(
        "early-sampling-demo",
        vec![
            Arc::new(LogRecorder {
                lines: Arc::clone(&log_lines),
                start: clock.system_time(),
                engine: redaction::passthrough_redaction_engine(),
            }) as Arc<dyn EventProcessor>,
            Arc::new(MetricRecorder {
                lines: Arc::clone(&metric_lines),
            }),
        ],
        &clock,
    )
    .with_early_sampler(DemoSampler::default());

    println!("Emitting login.attempt #1 (the sampler keeps it) ...");
    emit!(sink, LoginAttempt { attempt: 1 });

    // Move the clock forward, so the second event carries a later timestamp
    // than the kept one.
    control.advance(Duration::from_mins(1));

    println!("Emitting login.attempt #2 (the sampler releases both) ...");
    emit!(sink, LoginAttempt { attempt: 2 });

    println!("Emitting queue.depth (metric-only event, dropped as a whole event) ...");
    emit!(sink, QueueDepth { depth: 3 });

    println!("Emitting active.workers (processed metric-only event) ...");
    emit!(sink, ActiveWorkers { count: 5 });

    println!();
    println!("Log processor received:");
    for line in log_lines.lock().expect("lock is not poisoned").iter() {
        println!("  {line}");
    }

    println!();
    println!("Metric processor received:");
    for line in metric_lines.lock().expect("lock is not poisoned").iter() {
        println!("  {line}");
    }

    println!();
    println!("Notice: the kept event still reports attempt=1 and age=0s, while the");
    println!("event that released it reports attempt=2 and age=60s. `queue.depth`");
    println!("never appears, and `active.workers` proves the metric route is live.");
}
