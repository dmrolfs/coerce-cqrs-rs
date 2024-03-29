mod commit_action;
mod event_envelope;
mod materialized;
mod offset;
pub mod processor;

pub use commit_action::PostCommitAction;
pub use event_envelope::EventEnvelope;
pub use materialized::{
    InMemoryProjectionStorage, ProjectionApplicator, ProjectionStorage, ProjectionStorageRef,
};
pub use offset::Offset;

use coerce::persistent::PersistentActor;
use smol_str::SmolStr;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::str::FromStr;
use tagid::{Entity, Id, IdGenerator};
use thiserror::Error;

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
        // format!("{self}") // delegate to aggregate::id display form
        // aggregate::id form
        self.to_string()
    }
}

impl FromStr for PersistenceId {
    type Err = ProjectionError;

    fn from_str(rep: &str) -> Result<Self, Self::Err> {
        let parts: Vec<_> = rep.split(PERSISTENCE_ID_DELIMITER).collect();
        if parts.len() != 2 {
            return Err(ProjectionError::Id(rep.to_string()));
        }

        let aggregate_name = parts[0];
        let aggregate_id = parts[1];
        Ok(Self::from_parts(aggregate_name, aggregate_id))
    }
}

pub const PERSISTENCE_ID_DELIMITER: &str = "::";

impl fmt::Display for PersistenceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(f, "{}", self.id)
        } else {
            write!(
                f,
                "{}{PERSISTENCE_ID_DELIMITER}{}",
                self.aggregate_name, self.id
            )
        }
    }
}

impl Debug for PersistenceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PersistenceId")
            .field("aggregate_name", &self.aggregate_name)
            .field("id", &self.id)
            .finish()
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

pub const META_VIEW_TABLE: &str = "view_table";
pub const META_OFFSET_TABLE: &str = "offset_table";
pub const META_PROJECTION_NAME: &str = "projection_name";
pub const META_PERSISTENCE_ID: &str = "persistence_id";

#[derive(Debug, Error)]
pub enum ProjectionError {
    #[error("{0}")]
    Sql(#[from] sqlx::Error),

    #[error("failed to decode projection from bytes: {0}")]
    Decode(#[source] anyhow::Error),

    #[error("failed to encode projection into bytes: {0}")]
    Encode(#[source] anyhow::Error),

    #[error("{0}")]
    TaskJoin(#[source] tokio::task::JoinError),

    #[error("{0}")]
    MessageUnwrap(#[from] coerce::actor::message::MessageUnwrapErr),

    #[error("Invalid persistence_id: {0}")]
    Id(String),

    #[error("meta:{meta:?}, {source}")]
    Storage {
        source: anyhow::Error,
        meta: HashMap<String, String>,
    },

    #[error("failure in projection processor: {0}")]
    Processor(#[from] processor::ProcessorError),

    #[error("Failed during while applying event to projection: {0}")]
    EventApplication(#[from] anyhow::Error),
}
