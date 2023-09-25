use crate::projection::processor::{ProcessEntry, ProcessResult, ProcessorContext};
use crate::projection::{PersistenceId, ProjectionError};
use coerce::actor::message::Message;
use coerce::persistent::storage::JournalEntry;
use std::fmt::{self, Debug};
use std::marker::PhantomData;

pub struct ProjectionApplicator<P, E, A>
where
    E: Message,
    A: Fn(&PersistenceId, &P, E) -> ProcessResult<P, ProjectionError> + Send + Sync,
{
    applicator: A,
    _marker: PhantomData<fn() -> (P, E)>,
}

impl<P, E, A> Debug for ProjectionApplicator<P, E, A>
where
    E: Message,
    A: Fn(&PersistenceId, &P, E) -> ProcessResult<P, ProjectionError> + Send + Sync,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectionApplicator").finish()
    }
}

impl<P, E, A> ProjectionApplicator<P, E, A>
where
    E: Message,
    A: Fn(&PersistenceId, &P, E) -> ProcessResult<P, ProjectionError> + Send + Sync,
{
    pub fn new(applicator: A) -> Self {
        Self {
            applicator,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<P, E, A> ProcessEntry for ProjectionApplicator<P, E, A>
where
    P: Debug,
    E: Message + Debug,
    A: Fn(&PersistenceId, &P, E) -> ProcessResult<P, ProjectionError> + Send + Sync,
{
    type Projection = P;

    #[instrument(
        level = "debug",
        skip(self, entry, ctx),
        fields(projection_name=%ctx.projection_name, persistence_id=%ctx.persistence_id())
    )]
    fn apply_entry_to_projection(
        &self,
        projection: &Self::Projection,
        entry: JournalEntry,
        ctx: &ProcessorContext,
    ) -> ProcessResult<Self::Projection, ProjectionError> {
        match Self::from_bytes(entry) {
            Ok(event) => {
                info!(?event, ?projection, "processing event entry...");
                (self.applicator)(ctx.persistence_id(), projection, event)
            },

            Err(error) => {
                info!(?error, "failed to deserialize entry - skipping processing.");
                ProcessResult::Unchanged
            },
        }
    }
}
