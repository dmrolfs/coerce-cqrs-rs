mod commit_action;
mod event_envelope;
mod materialized;
mod offset;
mod processor;

pub use commit_action::PostCommitAction;
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
use tagid::{Entity, Id, IdGenerator};
use thiserror::Error;

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
        let aggregate_name = pretty_type_name::pretty_type_name::<A>();
        Self::from_parts(aggregate_name.as_str(), aggregate_id)
    }

    pub fn as_persistence_id(&self) -> String {
        format!("{self:#}") // delegate to alternate Display format
    }
}

pub const PERSISTENCE_ID_DELIMITER: &str = "::";

impl fmt::Display for PersistenceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(
                f,
                "{}{PERSISTENCE_ID_DELIMITER}{}",
                self.aggregate_name, self.id
            )
        } else {
            write!(f, "{}", self.id)
        }
    }
}

impl<T: ?Sized> From<PersistenceId> for Id<T, String> {
    fn from(pid: PersistenceId) -> Self {
        Self::direct(pid.aggregate_name.as_str(), pid.id.to_string())
    }
}

impl<T> From<Id<T, <<T as Entity>::IdGen as IdGenerator>::IdType>> for PersistenceId
where
    T: Entity + ?Sized,
    <<T as Entity>::IdGen as IdGenerator>::IdType: ToString,
{
    fn from(id: Id<T, <<T as Entity>::IdGen as IdGenerator>::IdType>) -> Self {
        Self {
            aggregate_name: id.label,
            id: SmolStr::new(id.id.to_string()),
        }
    }
}

#[async_trait]
pub trait EventProcessor<E> {
    type Error;
    async fn process(&self, event: EventEnvelope<E>) -> Result<(), Self::Error>;
}

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

    #[error("{0}")]
    JournalEntryPull(anyhow::Error),
}
