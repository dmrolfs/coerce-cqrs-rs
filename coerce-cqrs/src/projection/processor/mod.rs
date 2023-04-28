use crate::projection::{PersistenceId, ProjectionError, ProjectionId};
use coerce::persistent::journal::storage::JournalEntry;
use smol_str::SmolStr;
use std::fmt::Debug;

mod interval;
#[allow(clippy::module_inception)]
mod processor;

pub use interval::{
    CalculateInterval, CalculateIntervalFactory, ExponentialBackoff, RegularInterval,
};
pub use processor::ProcessorLifecycle;
pub use processor::{
    protocol::{ProcessorApi, ProcessorCommand},
    Processor, ProcessorEngine,
};

pub type ProcessorErrorHandler = dyn Fn(ProjectionError) + Send + Sync + 'static;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorContext {
    pub projection_id: ProjectionId,
    pub persistence_id: PersistenceId,
    pid_rep: SmolStr,
}

impl ProcessorContext {
    pub fn new(projection_id: ProjectionId, persistence_id: PersistenceId) -> Self {
        let pid_rep = format!("{}::{}", persistence_id.aggregate_name, persistence_id.id).into();
        Self {
            projection_id,
            persistence_id,
            pid_rep,
        }
    }

    pub fn persistence_id_rep(&self) -> &str {
        self.pid_rep.as_str()
    }
}

#[async_trait]
pub trait ProcessEntry {
    async fn process_entry(
        &mut self,
        entry: JournalEntry,
        ctx: &ProcessorContext,
    ) -> Result<(), ProjectionError>;

    fn handle_error(
        &self,
        error: ProjectionError,
        entry_payload_type: &str,
        ctx: &ProcessorContext,
    ) -> Result<(), ProjectionError> {
        error!(?error, context=?ctx, "failed to process {entry_payload_type} entry.");
        Err(error)
    }
}
