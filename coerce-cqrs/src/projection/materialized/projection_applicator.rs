use crate::projection::materialized::projection_storage::ProjectionStorage;
use crate::projection::processor::{ProcessEntry, ProcessorContext};
use crate::projection::{AggregateOffsets, Offset, PersistenceId, ProjectionError};
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
    P: Fn(S::Projection, E) -> Option<S::Projection> + Send + Sync,
{
    storage: Arc<S>,
    processor: P,
    _marker: PhantomData<E>,
}

impl<S, E, P> Debug for ProjectionApplicator<S, E, P>
where
    S: ProjectionStorage + Debug,
    E: Message,
    P: Fn(S::Projection, E) -> Option<S::Projection> + Send + Sync,
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
    P: Fn(S::Projection, E) -> Option<S::Projection> + Send + Sync,
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
    P: Fn(S::Projection, E) -> Option<S::Projection> + Send + Sync,
{
    #[instrument(level = "debug", skip(self))]
    async fn process_entry(
        &self,
        persistence_id: &PersistenceId,
        entry: JournalEntry,
        ctx: &ProcessorContext,
    ) -> Result<Offset, ProjectionError> {
        let event = match E::from_bytes(entry.bytes.to_vec()).map_err(|err| err.into()) {
            Ok(event) => event,
            Err(error) => {
                self.handle_error(persistence_id, error, entry.payload_type.as_ref(), ctx)?;
                return Ok(Offset::new(entry.sequence));
            }
        };

        let view = self
            .storage
            .load_projection(persistence_id)
            .await?
            .unwrap_or_default();
        let updated_view = (self.processor)(view, event);
        let last_offset = Offset::new(entry.sequence);
        debug!("DMR: processed last_offset:{last_offset:?} updated_view:{updated_view:?}");
        self.storage
            .save_projection(persistence_id, updated_view, last_offset)
            .await?;
        Ok(last_offset)
    }

    #[instrument(level = "debug", skip(self))]
    async fn read_all_offsets(&self, projection_name: &str) -> Result<AggregateOffsets, ProjectionError> {
        self.storage.read_all_offsets(projection_name).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn offset_for_persistence_id(
        &self,
        projection_name: &str,
        persistence_id: &PersistenceId,
    ) -> Result<Option<Offset>, ProjectionError> {
        self.storage.read_offset(projection_name, persistence_id).await
    }
}
