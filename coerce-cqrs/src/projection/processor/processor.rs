use crate::projection::processor::interval::CalculateInterval;
use crate::projection::processor::processor::protocol::ProcessorApi;
use crate::projection::processor::{ProcessEntry, ProcessResult};
use crate::projection::{Offset, PersistenceId, ProjectionError, ProjectionStorage};
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
    pub fn builder<S, H, I>(
        projection_name: impl Into<String>,
    ) -> ProcessorEngine<Building<S, H, I>>
    where
        S: ProjectionStorage,
        H: ProcessEntry,
        I: CalculateInterval,
    {
        ProcessorEngine::new(projection_name)
    }

    pub fn builder_for<A: PersistentActor, S, H, I>(
        projection_name: impl Into<String>,
    ) -> ProcessorEngine<Building<S, H, I>>
    where
        S: ProjectionStorage,
        H: ProcessEntry,
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

pub struct Building<S, H, I>
where
    S: ProjectionStorage,
    H: ProcessEntry,
    I: CalculateInterval,
{
    projection_name: String,
    entry_handler: Option<Arc<H>>,
    source: Option<ProcessorSourceRef>,
    projection_source: Option<Arc<S>>,
    interval_calculator: Option<I>,
}

impl<S, H, I> Debug for Building<S, H, I>
where
    S: ProjectionStorage,
    H: ProcessEntry + Debug,
    I: CalculateInterval + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Building")
            .field("projection_name", &self.projection_name)
            .field("entry_handler", &self.entry_handler)
            .field("interval_calculator", &self.interval_calculator)
            .finish()
    }
}

impl<S, H, I> ProcessorLifecycle for Building<S, H, I>
where
    S: ProjectionStorage,
    H: ProcessEntry,
    I: CalculateInterval,
{
}

impl<S, H, I> ProcessorEngine<Building<S, H, I>>
where
    S: ProjectionStorage,
    H: ProcessEntry,
    I: CalculateInterval,
{
    pub fn new(projection_name: impl Into<String>) -> Self {
        Self {
            inner: Building {
                projection_name: projection_name.into(),
                entry_handler: None,
                source: None,
                projection_source: None,
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

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_projection_source(self, projection_source: Arc<S>) -> Self {
        Self {
            inner: Building {
                projection_source: Some(projection_source),
                ..self.inner
            },
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_interval_calculator(self, interval_calculator: I) -> Self {
        Self {
            inner: Building {
                interval_calculator: Some(interval_calculator),
                ..self.inner
            },
        }
    }

    pub fn finish(self) -> Result<ProcessorEngine<Ready<S, H, I>>, ProcessorError> {
        let (tx_api, rx_api) = mpsc::unbounded_channel();

        let entry_handler = self
            .inner
            .entry_handler
            .ok_or_else(|| ProcessorError::UninitializedField("entry_handler".to_string()))?;

        let source = self
            .inner
            .source
            .ok_or_else(|| ProcessorError::UninitializedField("source".to_string()))?;

        let projection_storage = self
            .inner
            .projection_source
            .ok_or_else(|| ProcessorError::UninitializedField("projection_source".to_string()))?;

        let interval_calculator = self
            .inner
            .interval_calculator
            .ok_or_else(|| ProcessorError::UninitializedField("interval_calculator".to_string()))?;

        Ok(ProcessorEngine {
            inner: Ready {
                projection_name: self.inner.projection_name,
                entry_handler,
                source,
                projection_storage,
                interval_calculator,
                tx_api,
                rx_api,
            },
        })
    }
}

pub struct Ready<S, H, I>
where
    S: ProjectionStorage,
    H: ProcessEntry,
    I: CalculateInterval,
{
    projection_name: String,
    entry_handler: Arc<H>,
    source: ProcessorSourceRef,
    projection_storage: Arc<S>,
    interval_calculator: I,
    tx_api: ProcessorApi,
    rx_api: mpsc::UnboundedReceiver<protocol::ProcessorCommand>,
}

impl<S, H, I> Debug for Ready<S, H, I>
where
    S: ProjectionStorage,
    H: ProcessEntry + Debug,
    I: CalculateInterval + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ready")
            .field("projection_name", &self.projection_name)
            .field("entry_handler", &self.entry_handler)
            .field("interval_calculator", &self.interval_calculator)
            .finish()
    }
}

impl<S, H, I> ProcessorLifecycle for Ready<S, H, I>
where
    S: ProjectionStorage,
    H: ProcessEntry,
    I: CalculateInterval,
{
}

#[derive(Debug, PartialEq, Eq)]
pub struct ProcessorContext {
    pub projection_name: String,
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

impl<S, H, I, P> ProcessorEngine<Ready<S, H, I>>
where
    S: ProjectionStorage<ViewId = PersistenceId, Projection = P> + Send + Sync + 'static,
    H: ProcessEntry<Projection = P> + Send + Sync + 'static,
    P: Default + Debug + Clone + Send,
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

            let source = self.inner.source.clone();
            let projection_storage = self.inner.projection_storage.clone();

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

                    latest = Self::read_all_latest_messages(&projection_name, source.clone(), projection_storage.clone()) => {
                        match latest {
                            Ok(l) => { context = self.do_handle_latest_entries(l, context).await?; },

                            Err(error) if 3 < context.nr_repeat_failures => {
                                error!(?error, "too many {projection_name} processor failures - stopping");
                                break;
                            },

                            Err(error) => {
                                error!(?error, "failed to pull latest journal entries since last processor iteration.");
                                context.nr_repeat_failures += 1;
                            },
                        }
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

    #[instrument(level = "debug", skip(source, projection_storage))]
    async fn read_all_latest_messages(
        projection_name: &str,
        source: Arc<dyn ProcessorSource>,
        projection_storage: Arc<S>,
    ) -> anyhow::Result<Option<AggregateEntries>> {
        let offsets: Vec<_> = projection_storage //offset_storage
            .read_all_offsets(projection_name)
            .await?
            .into_iter()
            .map(|(pid, last_offset)| {
                debug!("DMR: LAST_OFFSET: {pid:?} = {}", last_offset.as_i64());
                (pid, Some(last_offset.as_i64() + 1))
            })
            .collect();
        debug!("DMR: offsets: {offsets:?}");

        let mut sequences: AggregateSequences = source
            .read_persistence_ids()
            .await?
            .into_iter()
            .map(|key| (key, None))
            .collect();
        debug!(
            "DMR: sequences: {:?}",
            sequences
                .iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect::<HashMap<_, _>>()
        );

        sequences.extend(offsets);

        let result = source.read_bulk_latest_messages(sequences).await;
        debug!(
            "DMR: PULLED MESSAGES PER AGGREGATE: {:?}",
            result.as_ref().map(|lo| lo.as_ref().map(|l| l
                .iter()
                .map(|(k, v)| (k.to_string(), v.len()))
                .collect::<HashMap<_, _>>()))
        );

        result
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
                let offset = self
                    .inner
                    .projection_storage
                    .read_offset(&ctx.projection_name, &pid)
                    .await
                    .ok()
                    .flatten();
                let _ignore = tx_reply.send(offset);
                Ok(true)
            }
        }
    }

    #[instrument(level = "debug", skip(self, latest))]
    async fn do_handle_latest_entries(
        &mut self,
        latest: Option<AggregateEntries>,
        mut context: ProcessorContext,
    ) -> Result<ProcessorContext, ProjectionError> {
        debug!(
            ?latest,
            "DMR: Handling latest entries since last processing..."
        );
        let mut error = None;

        let all_empty =
            |latest: &AggregateEntries| latest.values().all(|entries| entries.is_empty());

        match latest {
            None => {
                debug!(?context, "no journal entries retrieved for all aggregates.");
                context.nr_repeat_empties += 1;
            }

            Some(latest) if latest.is_empty() || all_empty(&latest) => {
                debug!(
                    ?context,
                    "no journal entries (empty) retrieved for all aggregates."
                );
                context.nr_repeat_empties += 1;
            }

            Some(latest) => {
                debug!("DMR: FOUND {} AGGREGATES...", latest.len());
                for (persistence_id, entries) in latest {
                    let outcome = self
                        .do_process_aggregate_entries(
                            &persistence_id,
                            entries,
                            // handler.as_ref(),
                            &context,
                        )
                        .await;
                    if let Err(err) = outcome {
                        debug!("DMR: failed to process entries: {err:?}");
                        context.nr_repeat_failures += 1;
                        error = Some(err);
                    }
                }

                context.nr_repeat_empties = 0;
            }
        };

        match error {
            None => {
                debug!("DMR - entry_process completed.");
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

    #[instrument(level = "debug", skip(self))]
    async fn do_process_aggregate_entries(
        &mut self,
        persistence_id: &PersistenceId,
        entries: Vec<JournalEntry>,
        ctx: &ProcessorContext,
    ) -> Result<(), ProjectionError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut projection = self
            .inner
            .projection_storage
            .load_projection(persistence_id)
            .await?
            .unwrap_or_default();
        debug!(
            ?projection,
            "DMR: # LATEST pulled: {persistence_id} => {}",
            entries.len()
        );

        let mut any_update = false;
        let mut last_offset = None;
        for entry in entries {
            let offset_sequence = entry.sequence;

            let projection_result =
                self.inner
                    .entry_handler
                    .apply_entry_to_projection(&projection, entry, ctx);

            match projection_result {
                ProcessResult::Changed(updated_projection) => {
                    info!(
                        ?updated_projection, old_projection=?projection,
                        "DMR: projection CHANGED by event entry"
                    );
                    projection = updated_projection;
                    any_update = true;
                }

                ProcessResult::Unchanged => {
                    debug!(?projection, "No change to projection for event entry.");
                }

                ProcessResult::Err(ProjectionError::EventApplication(error)) => {
                    warn!(
                        ?projection,
                        "failed to apply entry to projection - skipping entry: {error:?}"
                    );
                }

                ProcessResult::Err(error) => return Err(error),
            }

            last_offset = Some(Offset::new(offset_sequence));
        }
        let last_offset = last_offset.unwrap();

        let updated_projection = if any_update { Some(projection) } else { None };

        debug!(?last_offset, "DMR: applied to projection pulled entries for {persistence_id} => {updated_projection:?}");

        self.inner
            .projection_storage
            .save_projection(persistence_id, updated_projection, last_offset)
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct Running {
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
