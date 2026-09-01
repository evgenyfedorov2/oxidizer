// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Early sampling: an optional decision that selects which events a
//! non-composite sink processes during direct emission.
//!
//! An [`EarlySampler`] receives one owned [`EventMetadata`] value per
//! emission and returns the events that the sink processes now:
//!
//! - An empty vector processes nothing.
//! - One value, usually the received one, processes one event.
//! - Several values process several events, in vector order.
//!
//! The sampler owns every value that it does not return. It can drop such a
//! value to discard the event, or keep the value and return it from a later
//! call. A kept event carries its own timestamp, its own enrichment snapshot,
//! and its own sink id. The later call processes the event through the same
//! sink.
//!
//! # A kept event waits for the next call
//!
//! Only a later [`EarlySampler::sample`] call can release a kept event,
//! because the sampler returns it from that call. There is no other release
//! path in this version: [`Sink::flush`](crate::Sink::flush) does not ask the
//! sampler for its kept events, and a sampler that drops discards them. A
//! sampler that keeps events therefore keeps them until the sink emits again,
//! and it loses them at shutdown. Keep only events that a subsequent emission
//! is sure to release, or drop them instead.
//!
//! # Emission sequence
//!
//! 1. The sink drops the event if it has no processors.
//! 2. The sink drops the event if no processor is interested in its
//!    [`EventDescription`](crate::metadata::EventDescription).
//! 3. The sink builds the event value.
//! 4. The sink takes the thread's reentrancy guard. It drops the event if a
//!    dispatch is already in progress on the thread, so a sampler never sees
//!    an emission that no processor can receive.
//! 5. The sink reads its clock and snapshots its enrichment chain.
//! 6. The sink calls [`EarlySampler::sample`] with the owned event.
//! 7. The sink processes each returned event, in vector order.
//!
//! Attach a sampler to a non-composite sink with
//! [`Sink::with_early_sampler`](crate::Sink::with_early_sampler):
//!
//! ```
//! use observed::Sink;
//! use observed::sampling::{EarlySampler, EventMetadata};
//!
//! struct DropEverything;
//!
//! impl EarlySampler for DropEverything {
//!     fn sample(&self, _event: EventMetadata) -> Vec<EventMetadata> {
//!         Vec::new()
//!     }
//! }
//!
//! let sink = Sink::new("svc", Vec::new(), tick::SimpleClock::new_system())
//!     .with_early_sampler(DropEverything);
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

mod metadata;
mod sampler;

pub use metadata::EventMetadata;
pub use sampler::EarlySampler;
