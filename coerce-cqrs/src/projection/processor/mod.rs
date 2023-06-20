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
    AggregateEntries, AggregateOffsets, AggregateSequences, Processor, ProcessorContext, ProcessorEngine,
    ProcessorLifecycle, ProcessorSource, ProcessorSourceProvider, ProcessorSourceRef,
};

pub type ProcessorErrorHandler = dyn Fn(ProjectionError) + Send + Sync + 'static;

// #[derive(Debug, Clone, PartialEq, Eq)]
// pub struct ProcessorContext {
//     pub projection_id: ProjectionId,
//     pub persistence_id: PersistenceId,
//     pid_rep: SmolStr,
// }

// impl ProcessorContext {
//     pub fn new(projection_id: ProjectionId, persistence_id: PersistenceId) -> Self {
//         let pid_rep = format!("{}::{}", persistence_id.aggregate_name, persistence_id.id).into();
//         Self {
//             projection_id,
//             persistence_id,
//             pid_rep,
//         }
//     }
//
//     pub fn persistence_id_rep(&self) -> &str {
//         self.pid_rep.as_str()
//     }
// }

#[async_trait]
pub trait ProcessEntry {
    async fn process_entry(
        &self,
        persistence_id: &PersistenceId,
        entry: JournalEntry,
        ctx: &ProcessorContext,
    ) -> Result<Offset, ProjectionError>;

    async fn read_all_offsets(&self, projection_name: &str) -> Result<AggregateOffsets, ProjectionError>;

    async fn offset_for_persistence_id(
        &self,
        projection_name: &str,
        persistence_id: &PersistenceId,
    ) -> Result<Option<Offset>, ProjectionError>;

    #[instrument(level="error", skip(self))]
    fn handle_error(
        &self,
        persistence_id: &PersistenceId,
        error: ProjectionError,
        entry_payload_type: &str,
        ctx: &ProcessorContext,
    ) -> Result<Offset, ProjectionError> {
        error!(
            ?error, context=?ctx,
            "{} processor failed to process {entry_payload_type} entry for {persistence_id}.",
            ctx.projection_name
        );
        Err(error)
    }
}
