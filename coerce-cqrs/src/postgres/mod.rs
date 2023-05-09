mod actor;
mod config;
mod provider;

#[doc(hidden)]
pub mod doc;
mod sql_query;
mod view_storage;
mod offset;

pub use actor::{protocol, PostgresJournal};
pub use config::PostgresStorageConfig;
pub use provider::{PostgresJournalStorage, PostgresStorageProvider};
pub use view_storage::PostgresViewStorage;
pub use offset::PostgresOffsetStorage;

use crate::projection::PersistenceId;
use smol_str::SmolStr;
use std::fmt;
use strum_macros::{Display, EnumVariantNames, IntoStaticStr};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct StorageKey(SmolStr);

impl fmt::Display for StorageKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl StorageKey {
    pub fn new(id: impl AsRef<str>) -> Self {
        Self(SmolStr::new(id.as_ref()))
    }
}

impl From<PersistenceId> for StorageKey {
    fn from(pid: PersistenceId) -> Self {
        Self(pid.to_string().into())
    }
}

impl From<String> for StorageKey {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl From<&str> for StorageKey {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

impl From<SmolStr> for StorageKey {
    fn from(id: SmolStr) -> Self {
        Self::new(id.as_str())
    }
}

impl AsRef<str> for StorageKey {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

// impl From<StorageKey> for PersistenceId {
//     fn from(key: StorageKey) -> Self {
//         let mut parts = key.as_ref().split(PERSISTENCE_ID_DELIMITER);
//         let aggregate_name = SmolStr::new(parts.next().expect("aggregate name and id"));
//         let id = SmolStr::new(parts.next().expect("aggregate id"));
//         Self { aggregate_name, id }
//     }
// }

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Display, IntoStaticStr, EnumVariantNames)]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
enum EntryType {
    Journal,
    Snapshot,
}

fn storage_key_provider(
    persistence_id: &str,
    entry_type: &str,
    config: &PostgresStorageConfig,
) -> StorageKey {
    format!(
        "{key_prefix}{persistence_id}:{entry_type}",
        key_prefix = config.key_prefix
    )
    .into()
}

#[derive(Debug, Error)]
pub enum PostgresStorageError {
    #[error("{0}")]
    ActorRef(#[from] coerce::actor::ActorRefErr),

    #[error("{0}")]
    ActorReply(#[from] tokio::sync::oneshot::error::RecvError),

    #[error("{0}")]
    Sql(#[from] sqlx::Error),

    #[error("{0}")]
    Storage(anyhow::Error),
}
