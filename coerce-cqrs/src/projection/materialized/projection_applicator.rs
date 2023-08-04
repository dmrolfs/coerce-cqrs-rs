use crate::projection::materialized::projection_storage::ProjectionStorage;
use crate::projection::processor::{AggregateOffsets, ProcessEntry, ProcessorContext};
use crate::projection::{Offset, PersistenceId, ProjectionError};
use coerce::actor::message::Message;
use coerce::persistent::journal::storage::JournalEntry;
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct ProjectionApplicator<S, E, P>
where
    S: ProjectionStorage,
    E: Message,
    P: Fn(S::Projection, E) -> (S::Projection, bool) + Send + Sync,
{
    storage: Arc<S>,
    processor: P,
    _marker: PhantomData<E>,
}

impl<S, E, P> Debug for ProjectionApplicator<S, E, P>
where
    S: ProjectionStorage + Debug,
    E: Message,
    P: Fn(S::Projection, E) -> (S::Projection, bool) + Send + Sync,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectionApplicator")
            .field("storage", &self.storage)
            .finish()
    }
}

impl<S, E, P> ProjectionApplicator<S, E, P>
where
    S: ProjectionStorage,
    E: Message,
    P: Fn(S::Projection, E) -> (S::Projection, bool) + Send + Sync,
{
    pub fn new(storage: Arc<S>, processor: P) -> Self {
        Self {
            storage,
            processor,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<S, E, P> ProcessEntry for ProjectionApplicator<S, E, P>
where
    S: ProjectionStorage<ViewId = PersistenceId> + Debug + Send + Sync,
    <S as ProjectionStorage>::Projection: Debug + Send,
    E: Message,
    P: Fn(S::Projection, E) -> (S::Projection, bool) + Send + Sync,
{
    type Projection = <S as ProjectionStorage>::Projection;

    #[instrument(level = "debug", skip(self, _ctx))]
    async fn load_projection(
        &self,
        persistence_id: &PersistenceId,
        _ctx: &ProcessorContext,
    ) -> Result<Self::Projection, ProjectionError> {
        self.storage
            .load_projection(persistence_id)
            .await
            .map(|p| p.unwrap_or_default())
    }

    #[instrument(level="debug", skip(self, ctx, entry), fields(entry_sequence=%entry.sequence))]
    fn apply_entry_to_projection(
        &self,
        projection: Self::Projection,
        entry: JournalEntry,
        ctx: &ProcessorContext,
    ) -> (Self::Projection, bool) {
        let event = match E::from_bytes(entry.bytes.to_vec()) {
            Ok(event) => event,
            Err(error) => {
                error!(?error, context=?ctx, "failed to decode event entry");
                return (projection, false);
            }
        };

        (self.processor)(projection, event)
    }

    #[instrument(level = "debug", skip(self, _ctx))]
    async fn save_projection_and_offset(
        &self,
        persistence_id: &PersistenceId,
        projection: Option<Self::Projection>,
        last_offset: Offset,
        _ctx: &ProcessorContext,
    ) -> Result<Offset, ProjectionError> {
        self.storage
            .save_projection(persistence_id, projection, last_offset)
            .await?;
        Ok(last_offset)
    }

    #[instrument(level = "debug", skip(self))]
    async fn read_all_offsets(
        &self,
        projection_name: &str,
    ) -> Result<AggregateOffsets, ProjectionError> {
        self.storage.read_all_offsets(projection_name).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn read_offset_for_persistence_id(
        &self,
        projection_name: &str,
        persistence_id: &PersistenceId,
    ) -> Result<Option<Offset>, ProjectionError> {
        self.storage
            .read_offset(projection_name, persistence_id)
            .await
    }
}
