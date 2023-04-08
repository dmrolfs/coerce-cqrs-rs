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
extern crate async_trait;

#[macro_use]
extern crate tracing;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate coerce_macros;

pub mod fixtures;
pub mod setup_tracing;
