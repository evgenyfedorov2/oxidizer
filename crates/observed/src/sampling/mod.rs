// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Early sampling: an optional decision made before event construction during
//! direct emission through a non-composite sink.
//!
//! An [`EarlySampler`] receives borrowed [`EventMetadata`] before event
//! construction. It returns a [`SamplingDecision`]:
//!
//! - [`SamplingDecision::Drop`] discards the event before its typed
//!   value is constructed - no processor on the sink, log or metric, sees any
//!   signal from it.
//! - [`SamplingDecision::Continue`] emits the event normally.
//!
//! Attach a sampler to a non-composite sink with
//! [`Sink::with_early_sampler`](crate::Sink::with_early_sampler):
//!
//! ```
//! use std::sync::Arc;
//!
//! use observed::Sink;
//! use observed::sampling::{EarlySampler, EventMetadata, SamplingDecision};
//!
//! struct DropEverything;
//!
//! impl EarlySampler for DropEverything {
//!     fn sample(&self, _event: &EventMetadata<'_>) -> SamplingDecision {
//!         SamplingDecision::Drop
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
mod metadata;
mod sampler;

pub use decision::SamplingDecision;
pub use metadata::EventMetadata;
pub use sampler::EarlySampler;
