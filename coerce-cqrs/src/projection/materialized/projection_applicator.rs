use crate::projection::materialized::projection_storage::ProjectionStorage;
use crate::projection::processor::{ProcessEntry, ProcessResult, ProcessorContext};
use crate::projection::{PersistenceId, ProjectionError};
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
    P: Fn(&S::Projection, E) -> Result<ProcessResult<S::Projection>, ProjectionError> + Send + Sync,
{
    storage: Arc<S>,
    processor: P,
    _marker: PhantomData<E>,
}

impl<S, E, P> Debug for ProjectionApplicator<S, E, P>
where
    S: ProjectionStorage + Debug,
    E: Message,
    P: Fn(&S::Projection, E) -> Result<ProcessResult<S::Projection>, ProjectionError> + Send + Sync,
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
    P: Fn(&S::Projection, E) -> Result<ProcessResult<S::Projection>, ProjectionError> + Send + Sync,
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
    P: Fn(&S::Projection, E) -> Result<ProcessResult<S::Projection>, ProjectionError> + Send + Sync,
{
    type Projection = <S as ProjectionStorage>::Projection;

    #[instrument(level="debug", skip(self, entry, _ctx), fields(entry_payload=%entry.sequence))]
    fn apply_entry_to_projection(
        &self,
        projection: &Self::Projection,
        entry: JournalEntry,
        _ctx: &ProcessorContext,
    ) -> Result<ProcessResult<Self::Projection>, ProjectionError> {
        let event = E::from_bytes(entry.bytes.to_vec())?;
        (self.processor)(projection, event)
    }
}
