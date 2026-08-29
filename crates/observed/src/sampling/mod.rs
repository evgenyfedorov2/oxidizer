// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Early sampling: an optional decision made before event construction during
//! direct emission through a non-composite sink.
//!
//! An [`EarlySampler`] receives borrowed [`EventMetadata`] before event
//! construction. It returns an [`EarlySamplingDecision`]:
//!
//! - [`EarlySamplingDecision::Drop`] discards the event before its typed
//!   value is constructed - no processor on the sink, log or metric, sees any
//!   signal from it.
//! - [`EarlySamplingDecision::Continue`] emits the event normally, with no
//!   sampling id attached.
//! - [`EarlySamplingDecision::ContinueWith`] emits the event and attaches a
//!   caller-chosen [`SamplingId`], readable back from the resulting
//!   [`EventView`](crate::processing::EventView) via
//!   [`EventView::sampling_id`](crate::processing::EventView::sampling_id).
//!
//! Attach a sampler to a non-composite sink with
//! [`Sink::with_early_sampler`](crate::Sink::with_early_sampler):
//!
//! ```
//! use std::sync::Arc;
//!
//! use observed::Sink;
//! use observed::sampling::{EarlySampler, EarlySamplingDecision, EventMetadata};
//!
//! struct DropEverything;
//!
//! impl EarlySampler for DropEverything {
//!     fn sample(&self, _event: &EventMetadata<'_>) -> EarlySamplingDecision {
//!         EarlySamplingDecision::Drop
//!     }
//! }
//!
//! let sink = Sink::new("svc", Vec::new(), tick::SimpleClock::new_system())
//!     .with_early_sampler(Arc::new(DropEverything));
//! ```
//!
//! This minimal snippet demonstrates attachment only. Because the sink has no
//! processors, emission stops at the static-interest check and does not call
//! the sampler.
//!
//! See `examples/early_sampling.rs` for a complete runnable example
//! (`cargo run -p observed --example early_sampling`).
//!
//! A composite sink does not currently call the samplers of its child sinks.
//! Emit through a sampled child sink directly when the early decision is
//! required.

mod decision;
mod id;
mod metadata;
mod sampler;

pub use decision::EarlySamplingDecision;
pub use id::SamplingId;
pub use metadata::EventMetadata;
pub use sampler::EarlySampler;
