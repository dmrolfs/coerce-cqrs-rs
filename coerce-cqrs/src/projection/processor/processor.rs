use crate::projection::processor::interval::CalculateInterval;
use crate::projection::processor::{Building, ProcessEntry, ProcessorEngine};
use crate::projection::{Offset, PersistenceId};
use coerce::actor::system::ActorSystem;
use coerce::persistent::journal::storage::JournalEntry;
use coerce::persistent::storage::JournalStorage;
use coerce::persistent::PersistentActor;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;
use thiserror::Error;

pub mod protocol {
    use super::ProcessorError;
    use crate::projection::{Offset, PersistenceId};
    use tokio::sync::{mpsc, oneshot};

    pub type ProcessorApi = mpsc::UnboundedSender<ProcessorCommand>;

    #[allow(dead_code)]
    #[derive(Debug)]
    pub enum ProcessorCommand {
        GetOffset(PersistenceId, oneshot::Sender<Option<Offset>>),
        Stop(oneshot::Sender<()>),
    }

    #[allow(dead_code)]
    impl ProcessorCommand {
        pub async fn get_offset(
            persistence_id: PersistenceId,
            api: &ProcessorApi,
        ) -> Result<Option<Offset>, ProcessorError> {
            let (tx, rx) = oneshot::channel();
            api.send(Self::GetOffset(persistence_id, tx))?;
            Ok(rx.await?)
        }

        pub async fn stop(api: &ProcessorApi) -> Result<(), ProcessorError> {
            let (tx, rx) = oneshot::channel();
            api.send(Self::Stop(tx))?;
            Ok(rx.await?)
        }
    }
}

#[derive(Debug, Error)]
pub enum ProcessorError {
    #[error("{0}")]
    ApiSend(#[from] tokio::sync::mpsc::error::SendError<protocol::ProcessorCommand>),

    #[error("{0}")]
    ApiReceive(#[from] tokio::sync::oneshot::error::RecvError),

    #[error("uninitialized field error: {0}")]
    UninitializedField(String),
}

pub type ProcessorSourceRef = Arc<dyn ProcessorSource>;

pub trait ProcessorSourceProvider: 'static + Send + Sync {
    fn processor_source(&self) -> Option<ProcessorSourceRef>;
}

pub type AggregateSequences = HashMap<PersistenceId, Option<i64>>;
pub type AggregateEntries = HashMap<PersistenceId, Vec<JournalEntry>>;
pub type AggregateOffsets = HashMap<PersistenceId, Offset>;

#[async_trait]
pub trait ProcessorSource: JournalStorage {
    async fn read_persistence_ids(&self) -> anyhow::Result<Vec<PersistenceId>>;

    async fn read_bulk_latest_messages(
        &self,
        sequences: AggregateSequences,
    ) -> anyhow::Result<Option<AggregateEntries>>;
}

/// Entry point to build ProcessorEngine
pub struct Processor;

impl Processor {
    pub fn builder<VID, P, H, I>(
        projection_name: impl Into<String>,
    ) -> ProcessorEngine<Building<VID, P, H, I>>
    where
        H: ProcessEntry<Projection = P>,
        I: CalculateInterval,
    {
        ProcessorEngine::new(projection_name)
    }

    pub fn builder_for<A: PersistentActor, VID, P, H, I>(
        projection_name: impl Into<String>,
    ) -> ProcessorEngine<Building<VID, P, H, I>>
    where
        H: ProcessEntry<Projection = P>,
        I: CalculateInterval,
    {
        Self::builder(projection_name.into())
    }
}

pub struct ProcessorContext {
    pub projection_name: String,
    persistence_id: Option<PersistenceId>,
    pub(super) nr_repeat_empties: u32,
    pub(super) nr_repeat_failures: u32,
    system: ActorSystem,
}

impl ProcessorContext {
    pub fn new(projection_name: &str, system: ActorSystem) -> Self {
        Self {
            projection_name: projection_name.to_string(),
            persistence_id: None,
            nr_repeat_empties: 0,
            nr_repeat_failures: 0,
            system,
        }
    }

    pub(super) fn set_aggregate(&mut self, persistence_id: PersistenceId) {
        self.persistence_id = Some(persistence_id);
    }

    pub const fn persistence_id(&self) -> &PersistenceId {
        match self.persistence_id.as_ref() {
            Some(pid) => pid,
            None => panic!("called outside of aggregate context"),
        }
    }

    #[inline]
    pub fn view_id<VID: From<PersistenceId>>(&self) -> VID {
        self.persistence_id().clone().into()
    }

    pub(super) fn clear_aggregate(&mut self) {
        self.persistence_id = None;
        self.nr_repeat_empties = 0;
    }
}

impl Debug for ProcessorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessorContext")
            .field("projection_name", &self.projection_name)
            .field("persistence_id", &self.persistence_id)
            .field("nr_repeat_empties", &self.nr_repeat_empties)
            .field("nr_repeat_failures", &self.nr_repeat_failures)
            .finish()
    }
}

impl PartialEq for ProcessorContext {
    fn eq(&self, other: &Self) -> bool {
        self.projection_name == other.projection_name
            && self.persistence_id == other.persistence_id
            && self.nr_repeat_empties == other.nr_repeat_empties
            && self.nr_repeat_failures == other.nr_repeat_failures
            && self.system.system_id() == other.system.system_id()
    }
}
