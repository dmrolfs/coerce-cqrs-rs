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

pub use aggregate::{AggregateError, AggregateState, CommandResult, SnapshotTrigger};

//todo: remove once crate persistent & projection parts fully tested
pub mod memory;

#[doc(hidden)]
pub mod doc;

pub mod util {
    use iso8601_timestamp::Timestamp;

    pub(crate) fn now_timestamp() -> i64 {
        timestamp_seconds(Timestamp::now_utc())
    }

    pub(crate) fn timestamp_seconds(ts: Timestamp) -> i64 {
        ts.duration_since(Timestamp::UNIX_EPOCH).whole_seconds()
    }

    pub(crate) const fn timestamp_from_seconds(seconds: i64) -> Option<Timestamp> {
        Timestamp::UNIX_EPOCH.checked_add(iso8601_timestamp::Duration::seconds(seconds))
    }
}
