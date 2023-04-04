mod event_envelope;
mod materialized;
mod materialized_old;
mod offset;
mod processor;

pub use event_envelope::EventEnvelope;
pub use materialized::{InMemoryViewStorage, ViewApplicator, ViewContext, ViewId, ViewStorage};
pub use offset::{InMemoryOffsetStorage, Offset, OffsetStorage, OffsetStorageRef};
pub use processor::{
    CalculateInterval, CalculateIntervalFactory, ExponentialBackoff, Processor, ProcessorApi,
    ProcessorCommand, ProcessorErrorHandler, RegularInterval,
};

use coerce::persistent::PersistentActor;
use smol_str::SmolStr;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct ProjectionId(SmolStr);

impl fmt::Display for ProjectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ProjectionId {
    pub fn new(id: impl AsRef<str>) -> Self {
        Self(SmolStr::new(id.as_ref()))
    }
}

impl From<String> for ProjectionId {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl From<&str> for ProjectionId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

impl From<SmolStr> for ProjectionId {
    fn from(id: SmolStr) -> Self {
        Self::new(id.as_str())
    }
}

impl AsRef<str> for ProjectionId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PersistenceId {
    pub aggregate_name: SmolStr,
    pub id: SmolStr,
}

impl PersistenceId {
    pub fn from_parts(aggregate_name: &str, aggregate_id: &str) -> Self {
        Self {
            aggregate_name: SmolStr::new(aggregate_name),
            id: SmolStr::new(aggregate_id),
        }
    }

    pub fn from_aggregate_id<A: PersistentActor>(aggregate_id: &str) -> Self {
        let aggregate_name = crate::pretty_type_name::pretty_type_name::<A>();
        Self::from_parts(aggregate_name.as_str(), aggregate_id)
    }
}

impl fmt::Display for PersistenceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            self.id // "{}{PERSISTENCE_ID_DELIMITER}{}",
                    // self.aggregate_name, self.id
        )
    }
}

impl From<PersistenceId> for StorageKey {
    fn from(pid: PersistenceId) -> Self {
        Self(pid.to_string().into())
    }
}

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

#[async_trait]
pub trait EventProcessor<E> {
    type Error;
    async fn process(&self, event: EventEnvelope<E>) -> Result<(), Self::Error>;
}

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProjectionError {
    #[error("{0}")]
    Sql(#[from] sqlx::Error),

    #[error("{0}")]
    Json(#[from] serde_json::Error),

    #[error("{0}")]
    TaskJoin(#[from] tokio::task::JoinError),

    #[error("{0}")]
    MessageUnwrap(#[from] coerce::actor::message::MessageUnwrapErr),

    // #[error("{0}")]
    // Processor(anyhow::Error),

    // #[error("uninitialized field error: {0}")]
    // UninitializedField(String),
    #[error("{0}")]
    JournalEntryPull(anyhow::Error),
}
