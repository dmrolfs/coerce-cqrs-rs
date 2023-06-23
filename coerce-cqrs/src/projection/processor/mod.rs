use crate::projection::{Offset, PersistenceId, ProjectionError};
use coerce::persistent::journal::storage::JournalEntry;

mod interval;
#[allow(clippy::module_inception)]
mod processor;

pub use interval::{
    CalculateInterval, CalculateIntervalFactory, ExponentialBackoff, RegularInterval,
};
pub use processor::{
    protocol::{ProcessorApi, ProcessorCommand},
    AggregateEntries, AggregateOffsets, AggregateSequences, Processor, ProcessorContext,
    ProcessorEngine, ProcessorLifecycle, ProcessorSource, ProcessorSourceProvider,
    ProcessorSourceRef,
};

pub type ProcessorErrorHandler = dyn Fn(ProjectionError) + Send + Sync + 'static;

#[async_trait]
pub trait ProcessEntry {
    type Projection: std::fmt::Debug + Send;

    async fn load_projection(
        &self,
        persistence_id: &PersistenceId,
        ctx: &ProcessorContext,
    ) -> Result<Self::Projection, ProjectionError>;

    fn apply_entry_to_projection(
        &self,
        projection: Self::Projection,
        entry: JournalEntry,
        ctx: &ProcessorContext,
    ) -> (Self::Projection, bool);

    async fn save_projection_and_offset(
        &self,
        persistence_id: &PersistenceId,
        projection: Option<Self::Projection>,
        last_offset: Offset,
        ctx: &ProcessorContext,
    ) -> Result<Offset, ProjectionError>;

    async fn read_all_offsets(
        &self,
        projection_name: &str,
    ) -> Result<AggregateOffsets, ProjectionError>;

    async fn read_offset_for_persistence_id(
        &self,
        projection_name: &str,
        persistence_id: &PersistenceId,
    ) -> Result<Option<Offset>, ProjectionError>;
}
