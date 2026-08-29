// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Demonstrates an early sampler that can reject an entire event before its
//! typed value is constructed.
//!
//! This example attaches one [`EarlySampler`] to a sink with a log processor
//! and a metric processor, then emits four events that each exercise a
//! different decision:
//!
//! | Event | Decision | What to notice |
//! |-------|----------|-----------------|
//! | `noisy.request` | `Drop` | its field constructor never prints - the typed value is never built |
//! | `normal.request` | `Continue` | reaches both processors, with no sampling id |
//! | `special.request` | `ContinueWith(SamplingId::new(42))` | the log processor prints the exact id `42` |
//! | `queue.depth` (metric-only) | `Drop` | the *whole event* is dropped - the metric processor never sees it either |
//! | `active.workers` (metric-only) | `Continue` | the metric processor prints it, proving the dropped metric had a live route |
//!
//! Run with:
//! ```sh
//! cargo run -p observed --example early_sampling
//! ```

use std::sync::{Arc, Mutex};

use observed::metadata::EventDescription;
use observed::processing::{EventProcessor, EventView};
use observed::sampling::{EarlySampler, EarlySamplingDecision, EventMetadata, SamplingId};
use observed::{Sink, emit, event};

/// A log-producing event. Its `status` field is computed by
/// [`constructing_noisy_status`]. The function prints when it runs, so the
/// output shows if `Drop` prevented typed construction.
#[event("noisy.request")]
#[info("A noisy request the sampler always drops")]
struct NoisyRequest {
    #[unredacted]
    status: i64,
}

/// A log-producing event the sampler lets through unchanged.
#[event("normal.request")]
#[info("An ordinary request")]
struct NormalRequest {
    #[unredacted]
    status: i64,
}

/// A log-producing event. The sampler sets a fixed [`SamplingId`] on this
/// event.
#[event("special.request")]
#[info("A request with a sampling id for later processing")]
struct SpecialRequest {
    #[unredacted]
    status: i64,
}

/// A metric-only event that the sampler drops. No processor receives it
/// because `Drop` occurs before event construction.
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

/// Computes `NoisyRequest::status`.
///
/// If this function prints, `EarlySamplingDecision::Drop` did not stop typed
/// event construction.
fn constructing_noisy_status() -> i64 {
    println!("  (constructing NoisyRequest - this line must NOT print, since the sampler drops it before construction)");
    7
}

/// Drops `noisy.request` and `queue.depth`, tags `special.request` with id
/// `42`, and continues everything else unchanged.
struct DemoSampler;

impl EarlySampler for DemoSampler {
    fn sample(&self, event: &EventMetadata<'_>) -> EarlySamplingDecision {
        match event.description().name() {
            "noisy.request" | "queue.depth" => EarlySamplingDecision::Drop,
            "special.request" => EarlySamplingDecision::ContinueWith(SamplingId::new(42)),
            _ => EarlySamplingDecision::Continue,
        }
    }
}

/// Records every log-producing event it is handed, alongside the sampling id
/// (if any) attached to it.
struct LogRecorder {
    lines: Arc<Mutex<Vec<String>>>,
}

impl EventProcessor for LogRecorder {
    fn is_interested(&self, description: &EventDescription) -> bool {
        description.is_log()
    }

    fn process(&self, event: &EventView<'_>) {
        self.lines.lock().expect("lock is not poisoned").push(format!(
            "[LOG] {name} (sampling_id={id:?})",
            name = event.name(),
            id = event.sampling_id().map(SamplingId::get)
        ));
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

    let sink = Sink::new(
        "early-sampling-demo",
        vec![
            Arc::new(LogRecorder {
                lines: Arc::clone(&log_lines),
            }) as Arc<dyn EventProcessor>,
            Arc::new(MetricRecorder {
                lines: Arc::clone(&metric_lines),
            }),
        ],
        tick::SimpleClock::new_system(),
    )
    .with_early_sampler(Arc::new(DemoSampler));

    println!("Emitting noisy.request (dropped before construction) ...");
    emit!(
        sink,
        NoisyRequest {
            status: constructing_noisy_status(),
        }
    );

    println!("Emitting normal.request (continues, no sampling id) ...");
    emit!(sink, NormalRequest { status: 200 });

    println!("Emitting special.request (continues with SamplingId::new(42)) ...");
    emit!(sink, SpecialRequest { status: 200 });

    println!("Emitting queue.depth (metric-only event, dropped as a whole event) ...");
    emit!(sink, QueueDepth { depth: 3 });

    println!("Emitting active.workers (continued metric-only event) ...");
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
    println!("Notice: `noisy.request` and `queue.depth` never appear above, while");
    println!("`active.workers` proves the metric processor has a live route.");
}
