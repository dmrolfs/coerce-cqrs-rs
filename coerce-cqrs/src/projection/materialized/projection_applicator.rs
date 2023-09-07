use crate::projection::processor::{ProcessEntry, ProcessResult, ProcessorContext};
use crate::projection::ProjectionError;
use coerce::actor::message::Message;
use coerce::persistent::storage::JournalEntry;
use std::fmt::{self, Debug};
use std::marker::PhantomData;

pub struct ProjectionApplicator<P, E, A>
where
    E: Message,
    A: Fn(&P, E) -> ProcessResult<P, ProjectionError> + Send + Sync,
{
    applicator: A,
    _marker: PhantomData<fn() -> (P, E)>,
}

impl<P, E, A> Debug for ProjectionApplicator<P, E, A>
where
    E: Message,
    A: Fn(&P, E) -> ProcessResult<P, ProjectionError> + Send + Sync,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectionApplicator").finish()
    }
}

impl<P, E, A> ProjectionApplicator<P, E, A>
where
    E: Message,
    A: Fn(&P, E) -> ProcessResult<P, ProjectionError> + Send + Sync,
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
    E: Message,
    A: Fn(&P, E) -> ProcessResult<P, ProjectionError> + Send + Sync,
{
    type Projection = P;

    #[instrument(level = "debug", skip(self, entry, _ctx))]
    fn apply_entry_to_projection(
        &self,
        projection: &Self::Projection,
        entry: JournalEntry,
        _ctx: &ProcessorContext,
    ) -> ProcessResult<Self::Projection, ProjectionError> {
        let event = match Self::from_bytes(entry) {
            Ok(evt) => evt,
            Err(error) => return ProcessResult::Err(error.into()),
        };

        (self.applicator)(projection, event)
    }
}
