use crate::projection::processor::interval::CalculateInterval;
use crate::projection::processor::processor::protocol::ProcessorApi;
use crate::projection::processor::ProcessEntry;
use crate::projection::{Offset, PersistenceId, ProjectionError};
use coerce::persistent::journal::storage::JournalEntry;
use coerce::persistent::storage::JournalStorage;
use coerce::persistent::PersistentActor;
use futures::FutureExt;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub mod protocol {
    use super::ProcessorError;
    use crate::projection::{Offset, PersistenceId};
    use tokio::sync::{mpsc, oneshot};

    pub type ProcessorApi = mpsc::UnboundedSender<ProcessorCommand>;

    #[allow(dead_code)]
    #[derive(Debug)]
    pub enum ProcessorCommand {
        Stop(oneshot::Sender<()>),
        GetOffset(PersistenceId, oneshot::Sender<Option<Offset>>),
    }

    #[allow(dead_code)]
    impl ProcessorCommand {
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
    pub fn builder<H, I>(
        projection_name: impl Into<String>,
    ) -> ProcessorEngine<Building<H, I>>
    where
        H: ProcessEntry,
        // O: OffsetStorage,
        I: CalculateInterval,
    {
        ProcessorEngine::new(projection_name)
    }

    pub fn builder_for<A: PersistentActor, H, I>(
        projection_name: impl Into<String>,
    ) -> ProcessorEngine<Building<H, I>>
    where
        H: ProcessEntry,
        // O: OffsetStorage,
        I: CalculateInterval,
    {
        Self::builder(projection_name.into())
    }
}

/// Manages the `Processor` lifecycle
pub trait ProcessorLifecycle {}

/// The `ProcessorEngine` subscribes to an aggregates `JournalEntry`s and processes the entries one
/// at a time. There can be more than one Processor for an aggregate type. Processors can be used to
/// update aggregate view projections for different requirements. The Processor can be thought
/// of as maintaining the "read-side" of a CQRS-based aggregate.
#[derive(Debug)]
pub struct ProcessorEngine<P: ProcessorLifecycle> {
    inner: P,
}

pub struct Building<H, I>
where
    H: ProcessEntry,
    // O: OffsetStorage,
    I: CalculateInterval,
{
    projection_name: String,
    entry_handler: Option<Arc<H>>,
    source: Option<ProcessorSourceRef>,
    // offset_storage: Option<Arc<O>>,
    interval_calculator: Option<I>,
}

impl<H, I> Debug for Building<H, I>
where
    H: ProcessEntry + Debug,
    // O: OffsetStorage + Debug,
    I: CalculateInterval + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Building")
            .field("projection_name", &self.projection_name)
            .field("entry_handler", &self.entry_handler)
            // .field("offset_storage", &self.offset_storage)
            .field("interval_calculator", &self.interval_calculator)
            .finish()
    }
}

impl<H, I> ProcessorLifecycle for Building<H, I>
where
    H: ProcessEntry,
    // O: OffsetStorage,
    I: CalculateInterval,
{
}

impl<H, I> ProcessorEngine<Building<H, I>>
where
    H: ProcessEntry,
    // O: OffsetStorage,
    I: CalculateInterval,
{
    pub fn new(projection_name: impl Into<String>) -> Self {
        Self {
            inner: Building {
                projection_name: projection_name.into(),
                entry_handler: None,
                source: None,
                // offset_storage: None,
                interval_calculator: None,
            },
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_entry_handler(self, entry_handler: H) -> Self {
        Self {
            inner: Building {
                entry_handler: Some(Arc::new(entry_handler)),
                ..self.inner
            },
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_source(self, source: ProcessorSourceRef) -> Self {
        Self {
            inner: Building {
                source: Some(source),
                ..self.inner
            },
        }
    }

    // #[allow(clippy::missing_const_for_fn)]
    // pub fn with_offset_storage(self, offset_storage: Arc<O>) -> Self {
    //     Self {
    //         inner: Building {
    //             offset_storage: Some(offset_storage),
    //             ..self.inner
    //         },
    //     }
    // }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_interval_calculator(self, interval_calculator: I) -> Self {
        Self {
            inner: Building {
                interval_calculator: Some(interval_calculator),
                ..self.inner
            },
        }
    }

    pub fn finish(self) -> Result<ProcessorEngine<Ready<H, I>>, ProcessorError> {
        let (tx_api, rx_api) = mpsc::unbounded_channel();

        let entry_handler = self
            .inner
            .entry_handler
            .ok_or_else(|| ProcessorError::UninitializedField("entry_handler".to_string()))?;
        let source = self
            .inner
            .source
            .ok_or_else(|| ProcessorError::UninitializedField("source".to_string()))?;
        // let offset_storage = self
        //     .inner
        //     .offset_storage
        //     .ok_or_else(|| ProcessorError::UninitializedField("offset_storage".to_string()))?;

        let interval_calculator = self
            .inner
            .interval_calculator
            .ok_or_else(|| ProcessorError::UninitializedField("interval_calculator".to_string()))?;

        Ok(ProcessorEngine {
            inner: Ready {
                projection_name: self.inner.projection_name,
                entry_handler,
                source,
                // offset_storage,
                interval_calculator,
                tx_api,
                rx_api,
            },
        })
    }
}

pub struct Ready<H, I>
where
    H: ProcessEntry,
    // O: OffsetStorage,
    I: CalculateInterval,
{
    projection_name: String,
    entry_handler: Arc<H>,
    source: ProcessorSourceRef,
    // offset_storage: Arc<O>,
    interval_calculator: I,
    tx_api: ProcessorApi,
    rx_api: mpsc::UnboundedReceiver<protocol::ProcessorCommand>,
}

impl<H, I> Debug for Ready<H, I>
where
    H: ProcessEntry + Debug,
    // O: OffsetStorage + Debug,
    I: CalculateInterval + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ready")
            .field("projection_name", &self.projection_name)
            .field("entry_handler", &self.entry_handler)
            // .field("offset_storage", &self.offset_storage)
            .field("interval_calculator", &self.interval_calculator)
            .finish()
    }
}

impl<H, I> ProcessorLifecycle for Ready<H, I>
where
    H: ProcessEntry,
    // O: OffsetStorage,
    I: CalculateInterval,
{
}

#[derive(Debug, PartialEq, Eq)]
pub struct ProcessorContext {
    pub projection_name: String,
    // pub projection_id: ProjectionId,
    nr_repeat_empties: u32,
    nr_repeat_failures: u32,
}

impl ProcessorContext {
    pub fn new(projection_name: &str) -> Self {
        Self {
            projection_name: projection_name.to_string(),
            nr_repeat_empties: 0,
            nr_repeat_failures: 0,
        }
    }
}

impl<H, I> ProcessorEngine<Ready<H, I>>
where
    H: ProcessEntry + Send + Sync + 'static,
    // O: OffsetStorage + Send + Sync + 'static,
    I: CalculateInterval + Send + Sync + 'static,
{
    #[allow(dead_code)]
    pub fn tx_api(&self) -> ProcessorApi {
        self.inner.tx_api.clone()
    }

    #[instrument(level = "trace", skip(self))]
    pub fn run(mut self) -> Result<ProcessorEngine<Running>, ProjectionError> {
        let tx_api = self.tx_api();

        let projection_name = self.inner.projection_name.clone();

        let handle = tokio::spawn(async move {
            let mut context = ProcessorContext::new(&projection_name);

            let handler = self.inner.entry_handler.clone();
            let source = self.inner.source.clone();
            // let offset_storage = self.inner.offset_storage.clone();

            loop {
                tokio::select! {
                    Some(command) = self.inner.rx_api.recv() => {
                        if !self.do_handle_api_command(command, &context).await? {
                            break;
                        }
                    },
                    // Some(protocol::ProcessorCommand::Stop(tx_reply)) = self.inner.rx_api.recv() => {
                    //     debug!("STOP command sent to processor: {:?}", context.projection_name);
                    //     let _ignore = tx_reply.send(());
                    //     break;
                    // },


                    latest = Self::read_all_latest_messages(&projection_name, source.clone(), handler.clone(), /*offset_storage.clone()*/) => {
                        context = self.do_handle_latest_entries(latest, context).await?;
                    },

                    else => {
                        error!("execution loop died unexpectedly for processor: {:?}", context.projection_name);
                        break;
                    },
                }
            }

            self.inner.rx_api.close();
            Ok(())
        });

        Ok(ProcessorEngine {
            inner: Running { tx_api, handle },
        })
    }

    #[instrument(level = "debug", skip(source, handler))]
    async fn read_all_latest_messages(
        projection_name: &str,
        source: Arc<dyn ProcessorSource>,
        handler: Arc<H>,
        // offset_storage: Arc<O>,
    ) -> anyhow::Result<Option<AggregateEntries>> {
        let offsets: Vec<_> =  handler  //offset_storage
            .read_all_offsets(projection_name)
            .await?
            .into_iter()
            .map(|(pid, offset)| (pid, Some(offset.as_i64() + 1)))
            .collect();
        debug!("DMR: offsets: {offsets:?}");

        let mut sequences: AggregateSequences = source
            .read_persistence_ids()
            .await?
            .into_iter()
            .map(|key| (key, None))
            .collect();
        debug!("DMR: sequences: {sequences:?}");

        sequences.extend(offsets);

        source.read_bulk_latest_messages(sequences).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn do_handle_api_command(
        &self,
        command: protocol::ProcessorCommand,
        ctx: &ProcessorContext,
    ) -> Result<bool, ProjectionError> {
        match command {
            protocol::ProcessorCommand::Stop(tx_reply) => {
                debug!("STOP command sent to processor: {:?}", ctx.projection_name);
                let _ignore = tx_reply.send(());
                Ok(false)
            }

            protocol::ProcessorCommand::GetOffset(pid, tx_reply) => {
                debug!(
                    "GetOffset command sent to processor: {:?}",
                    ctx.projection_name
                );
                let offset = self.inner.entry_handler.offset_for_persistence_id(&ctx.projection_name, &pid).await.ok().flatten();
                let _ignore = tx_reply.send(offset);
                Ok(true)
            }
        }
    }

    #[instrument( level = "debug", skip(self, latest), )]
    async fn do_handle_latest_entries(
        &mut self,
        latest: anyhow::Result<Option<AggregateEntries>>,
        mut context: ProcessorContext,
    ) -> Result<ProcessorContext, ProjectionError> {
        // debug!(
        //     latest=%format!("{:?}", latest.map(|rl| rl.map(|ol| {
        //         ol.iter().map(|(pid, entries)| (format!("{pid:#}"), entries)).collect::<HashMap<_, _>>()}
        //     ))),
        //     "Handling latest entries since last processing..."
        // );
        let mut error = None;

        match latest {
            Ok(Some(latest)) if latest.is_empty() => {
                debug!(
                    ?context,
                    "no journal entries (empty) retrieved for all aggregates."
                );
                context.nr_repeat_empties += 1;
            }

            Ok(None) => {
                debug!(?context, "no journal entries retrieved for all aggregates.");
                context.nr_repeat_empties += 1;
            }

            Ok(Some(latest)) => {
                debug!("DMR: FOUND {} AGGREGATES...", latest.len());
                for (persistence_id, entries) in latest {
                    debug!("DMR: # LATEST pulled: {persistence_id:#} => {}", entries.len());
                    for entry in entries {
                        if let Err(err) = self
                            .do_process_entry(&persistence_id, entry, &context)
                            .await
                        {
                            debug!("DMR: process error: {err:?}");
                            context.nr_repeat_failures += 1;
                            error = Some(err);
                            break;
                        }
                    }
                }

                context.nr_repeat_empties = 0;
            }

            Err(err) => {
                error!(error=?err, "failed to pull latest journal entries since last processor iteration.");
                context.nr_repeat_failures += 1;
                error = Some(ProjectionError::Storage {
                    cause: err,
                    meta: maplit::hashmap! {},
                });
            }
        };

        match error {
            None => {
                self.do_delay(&context).await;
                Ok(context)
            }

            Some(err) if context.nr_repeat_failures < 3 => {
                warn!(error=?err, ?context, "failed to process current set of entries.");
                self.do_delay(&context).await;
                Ok(context)
            }

            Some(err) => {
                error!(?context, error=?err, "too many processor errors - stopping.");
                Err(err)
            }
        }
    }

    async fn do_delay(&mut self, context: &ProcessorContext) {
        if 0 < context.nr_repeat_empties {
            let delay = self
                .inner
                .interval_calculator
                .next_interval(context.nr_repeat_empties);
            debug!(?context, "delaying next processor read for {delay:?}");
            tokio::time::sleep(delay).await;
        }
    }

    #[instrument(level = "debug", skip(self, entry))]
    async fn do_process_entry(
        &mut self,
        persistence_id: &PersistenceId,
        entry: JournalEntry,
        ctx: &ProcessorContext,
    ) -> Result<Offset, ProjectionError> {
        let payload_type = entry.payload_type.clone();

        debug!(
            "DMR: VIEW: Processing entry[{}] for {persistence_id}",
            entry.sequence
        );
        let offset = self
            .inner
            .entry_handler
            .process_entry(persistence_id, entry, ctx)
            .await;
        info!("DMR: VIEW: PROCESSED ENTRY FOR {persistence_id:#}: new last_offset = {offset:?}");

        match offset {
            Ok(o) => Ok(o),

            Err(error) => self.inner.entry_handler.handle_error(
                persistence_id,
                error,
                payload_type.as_ref(),
                ctx,
            ),
        }

        // self.save_offset(&ctx.projection_name, persistence_id, next_sequence)
        //     .await
    }

    // #[instrument(level = "trace", skip(self))]
    // async fn save_offset(
    //     &self,
    //     projection_name: &str,
    //     persistence_id: &PersistenceId,
    //     entry_sequence: i64,
    // ) -> Result<Offset, ProjectionError> {
    //     let offset = Offset::new(entry_sequence);
    //     self.inner
    //         .offset_storage
    //         .save_offset(projection_name, persistence_id, offset)
    //         .await?;
    //     Ok(offset)
    // }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct Running {
    // context: ProcessorContext,
    tx_api: ProcessorApi,
    handle: JoinHandle<Result<(), ProjectionError>>,
}

impl ProcessorLifecycle for Running {}

impl ProcessorEngine<Running> {
    #[allow(dead_code)]
    pub fn tx_api(&self) -> ProcessorApi {
        self.inner.tx_api.clone()
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn block_for_completion(self) -> Result<(), ProjectionError> {
        let handle = self.inner.handle.fuse();
        handle.await?
    }
}
