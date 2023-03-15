mod actor;
mod config;
mod provider;
mod view_repository;

#[doc(hidden)]
pub mod doc;

pub use actor::{protocol, PostgresJournal};
pub use config::PostgresStorageConfig;
pub use provider::{PostgresJournalStorage, PostgresStorageProvider};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CqrsError {
    #[error("{0}")]
    ActorRef(#[from] coerce::actor::ActorRefErr),
}
