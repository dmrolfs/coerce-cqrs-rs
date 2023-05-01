use crate::projection::processor::interval::CalculateInterval;
use crate::projection::processor::processor::protocol::ProcessorApi;
use crate::projection::processor::{ProcessEntry, ProcessorContext};
use crate::projection::{Offset, OffsetStorage, PersistenceId, ProjectionError, ProjectionId};
use coerce::persistent::journal::storage::{JournalEntry, JournalStorageRef};
use coerce::persistent::PersistentActor;
use futures::FutureExt;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub mod protocol {
    use super::ProcessorError;
    use tokio::sync::{mpsc, oneshot};

    pub type ProcessorApi = mpsc::UnboundedSender<ProcessorCommand>;

    #[allow(dead_code)]
    #[derive(Debug)]
    pub enum ProcessorCommand {
        Stop(oneshot::Sender<()>),
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

/// Entry point to build ProcessorEngine
pub struct Processor;

impl Processor {
    pub fn builder<H, O, I>(
        projection_id: impl Into<ProjectionId>,
        persistence_id: PersistenceId,
    ) -> ProcessorEngine<Building<H, O, I>>
    where
        H: ProcessEntry,
        O: OffsetStorage,
        I: CalculateInterval,
    {
        ProcessorEngine::new(projection_id, persistence_id)
    }

    pub fn builder_for<A: PersistentActor, H, O, I>(
        projection_id: impl Into<ProjectionId>,
        aggregate_id: &str,
    ) -> ProcessorEngine<Building<H, O, I>>
    where
        H: ProcessEntry,
        O: OffsetStorage,
        I: CalculateInterval,
    {
        let projection_id = projection_id.into();
        let persistence_id = PersistenceId::from_aggregate_id::<A>(aggregate_id);
        Self::builder(projection_id, persistence_id)
    }
}

/// Manages the `Processor` lifecycle
pub trait ProcessorLifecycle {}

/// The `ProcessorEngine` subscribes to an aggregates `JournalEntry`s and processes the entries one
/// at a time. There can be more than one Processor for an aggregate. Processors can be used to
/// update view projections of an aggregate for different requirements. The Processor can be thought
/// of as maintaining the "read-side" of a CQRS-based aggregate.
#[derive(Debug)]
pub struct ProcessorEngine<P: ProcessorLifecycle> {
    inner: P,
}

pub struct Building<H, O, I>
where
    H: ProcessEntry,
    O: OffsetStorage,
    I: CalculateInterval,
{
    projection_id: ProjectionId,
    persistence_id: PersistenceId,
    entry_handler: Option<H>,
    source: Option<JournalStorageRef>,
    offset_storage: Option<Arc<O>>,
    interval_calculator: Option<I>,
}

impl<H, O, I> Debug for Building<H, O, I>
where
    H: ProcessEntry + Debug,
    O: OffsetStorage + Debug,
    I: CalculateInterval + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Building")
            .field("projection_id", &self.projection_id)
            .field("persistence_id", &self.persistence_id)
            .field("entry_handler", &self.entry_handler)
            .field("offset_storage", &self.offset_storage)
            .field("interval_calculator", &self.interval_calculator)
            .finish()
    }
}

impl<H, O, I> ProcessorLifecycle for Building<H, O, I>
where
    H: ProcessEntry,
    O: OffsetStorage,
    I: CalculateInterval,
{
}

impl<H, O, I> ProcessorEngine<Building<H, O, I>>
where
    H: ProcessEntry,
    O: OffsetStorage,
    I: CalculateInterval,
{
    pub fn new(projection_id: impl Into<ProjectionId>, persistence_id: PersistenceId) -> Self {
        Self {
            inner: Building {
                projection_id: projection_id.into(),
                persistence_id,
                entry_handler: None,
                source: None,
                offset_storage: None,
                interval_calculator: None,
            },
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_entry_handler(self, entry_handler: H) -> Self {
        Self {
            inner: Building {
                entry_handler: Some(entry_handler),
                ..self.inner
            },
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_source(self, source: JournalStorageRef) -> Self {
        Self {
            inner: Building {
                source: Some(source),
                ..self.inner
            },
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_offset_storage(self, offset_storage: Arc<O>) -> Self {
        Self {
            inner: Building {
                offset_storage: Some(offset_storage),
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

    pub fn finish(self) -> Result<ProcessorEngine<Ready<H, O, I>>, ProcessorError> {
        let (tx_api, rx_api) = mpsc::unbounded_channel();

        let entry_handler = self
            .inner
            .entry_handler
            .ok_or_else(|| ProcessorError::UninitializedField("entry_handler".to_string()))?;
        let source = self
            .inner
            .source
            .ok_or_else(|| ProcessorError::UninitializedField("source".to_string()))?;
        let offset_storage = self
            .inner
            .offset_storage
            .ok_or_else(|| ProcessorError::UninitializedField("offset_storage".to_string()))?;
        let interval_calculator = self
            .inner
            .interval_calculator
            .ok_or_else(|| ProcessorError::UninitializedField("interval_calculator".to_string()))?;

        Ok(ProcessorEngine {
            inner: Ready {
                projection_id: self.inner.projection_id,
                persistence_id: self.inner.persistence_id,
                entry_handler,
                source,
                offset_storage,
                interval_calculator,
                tx_api,
                rx_api,
            },
        })
    }
}

pub struct Ready<H, O, I>
where
    H: ProcessEntry,
    O: OffsetStorage,
    I: CalculateInterval,
{
    projection_id: ProjectionId,
    persistence_id: PersistenceId,
    entry_handler: H,
    source: JournalStorageRef,
    offset_storage: Arc<O>,
    interval_calculator: I,
    tx_api: ProcessorApi,
    rx_api: mpsc::UnboundedReceiver<protocol::ProcessorCommand>,
}

impl<H, O, I> Debug for Ready<H, O, I>
where
    H: ProcessEntry + Debug,
    O: OffsetStorage + Debug,
    I: CalculateInterval + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ready")
            .field("entry_handler", &self.entry_handler)
            .field("offset_storage", &self.offset_storage)
            .field("interval_calculator", &self.interval_calculator)
            .finish()
    }
}

impl<H, O, I> ProcessorLifecycle for Ready<H, O, I>
where
    H: ProcessEntry,
    O: OffsetStorage,
    I: CalculateInterval,
{
}

#[derive(Debug, PartialEq)]
struct ProcessorExecutionContext {
    context: ProcessorContext,
    offset: Offset,
    nr_repeat_empties: u32,
    nr_repeat_errors: u32,
}

impl ProcessorExecutionContext {
    pub fn persistence_id_rep(&self) -> &str {
        self.context.persistence_id_rep()
    }
}

impl<H, O, I> ProcessorEngine<Ready<H, O, I>>
where
    H: ProcessEntry + Send + Sync + 'static,
    O: OffsetStorage + Send + Sync + 'static,
    I: CalculateInterval + Send + Sync + 'static,
{
    #[allow(dead_code)]
    pub fn tx_api(&self) -> ProcessorApi {
        self.inner.tx_api.clone()
    }

    #[instrument(level = "trace", skip(self))]
    pub fn run(mut self) -> Result<ProcessorEngine<Running>, ProjectionError> {
        let tx_api = self.tx_api();

        let handle = tokio::spawn(async move {
            let p_ctx = ProcessorContext::new(
                self.inner.projection_id.clone(),
                self.inner.persistence_id.clone(),
            );
            let offset = self.load_offset(&p_ctx).await?;
            let mut context = self.make_context(p_ctx, offset);
            let source = self.inner.source.clone();
            let pid = context.persistence_id_rep().to_string();

            loop {
                tokio::select! {
                    Some(protocol::ProcessorCommand::Stop(tx_reply)) = self.inner.rx_api.recv() => {
                        debug!("STOP command sent to processor: {:?}", context);
                        let _ignore = tx_reply.send(());
                        break;
                   },

                    latest = source.read_latest_messages(&pid, context.offset.as_i64()) => {
                        context = self.do_handle_latest_entries(latest, context).await?;
                    },

                    else => {
                        error!("execution loop died unexpectedly for processor: {:?}", context);
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

    const fn make_context(
        &self,
        p_ctx: ProcessorContext,
        offset: Offset,
    ) -> ProcessorExecutionContext {
        ProcessorExecutionContext {
            context: p_ctx,
            offset,
            nr_repeat_empties: 0,
            nr_repeat_errors: 0,
        }
    }

    #[instrument(level = "debug", skip(self))]
    async fn do_handle_latest_entries(
        &mut self,
        latest: Result<Option<Vec<JournalEntry>>, anyhow::Error>,
        mut context: ProcessorExecutionContext,
    ) -> Result<ProcessorExecutionContext, ProjectionError> {
        let mut error = None;

        match latest {
            Ok(Some(latest)) if latest.is_empty() => {
                debug!(
                    ?context,
                    "no journal entries (empty) retrieved for {}.",
                    context.persistence_id_rep()
                );
                context.nr_repeat_empties += 1;
            }

            Ok(None) => {
                debug!(
                    ?context,
                    "no journal entries retrieved for {}.",
                    context.persistence_id_rep()
                );
                context.nr_repeat_empties += 1;
            }

            Ok(Some(latest)) => {
                for entry in latest {
                    match self.do_process_entry(entry, &context.context).await {
                        Ok(offset) => {
                            context.offset = offset;
                        }

                        Err(err) => {
                            context.nr_repeat_errors += 1;
                            error = Some(err);
                            break;
                        }
                    }
                }

                context.nr_repeat_empties = 0;
            }

            Err(err) => {
                error!(error=?err, last_offset=?context.offset, "failed to pull latest journal entries for {:?}.", context.context);
                context.nr_repeat_errors += 1;
                error = Some(ProjectionError::JournalEntryPull(err));
            }
        };

        match error {
            None => {
                self.do_delay(&context).await;
                Ok(context)
            }

            Some(err) if context.nr_repeat_errors < 3 => {
                warn!(error=?err, ?context, "failed to process current set of entries.");
                self.do_delay(&context).await;
                Ok(context)
            }

            Some(err) => {
                error!(?context, error=?err, "too many errors processing for {} - stopping.", context.persistence_id_rep());
                Err(err)
            }
        }
    }

    async fn do_delay(&mut self, context: &ProcessorExecutionContext) {
        if 0 < context.nr_repeat_empties {
            let delay = self
                .inner
                .interval_calculator
                .next_interval(context.nr_repeat_empties);
            debug!(?context, "delaying next processor read for {delay:?}");
            tokio::time::sleep(delay).await;
        }
    }

    #[instrument(level = "trace", skip(self, entry))]
    async fn do_process_entry(
        &mut self,
        entry: JournalEntry,
        ctx: &ProcessorContext,
    ) -> Result<Offset, ProjectionError> {
        let next_sequence = entry.sequence + 1;

        let payload_type = entry.payload_type.clone();
        if let Err(error) = self.inner.entry_handler.process_entry(entry, ctx).await {
            self.inner
                .entry_handler
                .handle_error(error, payload_type.as_ref(), ctx)?;
        }

        self.save_offset(next_sequence, ctx).await
    }

    #[instrument(level = "trace", skip(self))]
    async fn load_offset(&self, ctx: &ProcessorContext) -> Result<Offset, ProjectionError> {
        let latest = self
            .inner
            .offset_storage
            .read_offset(&ctx.projection_id, &ctx.persistence_id)
            .await?;
        Ok(latest.unwrap_or_default())
    }

    #[instrument(level = "trace", skip(self))]
    async fn save_offset(
        &self,
        entry_sequence: i64,
        ctx: &ProcessorContext,
    ) -> Result<Offset, ProjectionError> {
        let offset = Offset::new(entry_sequence);
        self.inner
            .offset_storage
            .save_offset(&ctx.projection_id, &ctx.persistence_id, offset)
            .await?;
        Ok(offset)
    }
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
