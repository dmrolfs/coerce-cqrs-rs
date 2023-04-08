#![warn(
    clippy::cargo,
    clippy::nursery,
    future_incompatible,
    rust_2018_idioms,
    rust_2021_compatibility,
    rust_2021_incompatible_closure_captures,
    rust_2021_prelude_collisions
)]
#![allow(clippy::multiple_crate_versions)]

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[macro_use]
extern crate tracing;

#[macro_use]
extern crate coerce_macros;

mod aggregate;
pub mod postgres;
pub mod projection;

pub use aggregate::{AggregateError, AggregateState, ApplyAggregateEvent, CommandResult};

//todo: remove once crate persistent & projection parts fully tested
pub mod memory;

#[doc(hidden)]
pub mod doc;
