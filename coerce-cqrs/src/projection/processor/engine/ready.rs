use crate::projection::materialized::ProjectionStorageRef;
use crate::projection::processor::engine::running::Running;
use crate::projection::processor::processor::protocol;
use crate::projection::processor::{
    AggregateEntries, AggregateSequences, CalculateInterval, ProcessEntry, ProcessResult,
    ProcessorApi, ProcessorContext, ProcessorEngine, ProcessorLifecycle, ProcessorSource,
    ProcessorSourceRef,
};
use crate::projection::{Offset, PersistenceId, ProjectionError};
use coerce::actor::system::ActorSystem;
use coerce::persistent::storage::JournalEntry;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct Ready<VID, P, H, I>
where
    H: ProcessEntry<Projection = P>,
    I: CalculateInterval,
{
    pub(super) projection_name: String,
    pub(super) entry_handler: Arc<H>,
    pub(super) system: ActorSystem,
    pub(super) source: ProcessorSourceRef,
    pub(super) projection_storage: ProjectionStorageRef<VID, P>,
    pub(super) interval_calculator: I,
    pub(super) tx_api: ProcessorApi,
    pub(super) rx_api: mpsc::UnboundedReceiver<protocol::ProcessorCommand>,
}

impl<VID, P, H, I> Debug for Ready<VID, P, H, I>
where
    H: ProcessEntry<Projection = P> + Debug,
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

impl<VID, P, H, I> ProcessorLifecycle for Ready<VID, P, H, I>
where
    H: ProcessEntry<Projection = P>,
    I: CalculateInterval,
{
}

impl<VID, P, H, I> ProcessorEngine<Ready<VID, P, H, I>>
where
    VID: From<PersistenceId> + Send + Sync + 'static,
    H: ProcessEntry<Projection = P> + Send + Sync + 'static,
    P: Default + Debug + Clone + Send + Sync + 'static,
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
        let system = self.inner.system.clone();
        let source = self.inner.source.clone();
        let projection_storage = self.inner.projection_storage.clone();

        let handle = tokio::spawn(async move {
            let mut context = ProcessorContext::new(&projection_name, system);

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
        projection_storage: ProjectionStorageRef<VID, P>,
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
                debug!(
                    "STOP command received by processor: {:?}",
                    ctx.projection_name
                );
                let _ignore = tx_reply.send(());
                Ok(false)
            }

            protocol::ProcessorCommand::GetOffset(pid, tx_reply) => {
                debug!(
                    "GetOffset command received by processor: {:?}",
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
                    context.set_aggregate(persistence_id);
                    let outcome = self.do_process_aggregate_entries(entries, &context).await;
                    if let Err(err) = outcome {
                        debug!(error=?err, "DMR: failed to process entries at: {:?}", context.persistence_id());
                        context.nr_repeat_failures += 1;
                        error = Some(err);
                    }
                }

                context.clear_aggregate();
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
        entries: Vec<JournalEntry>,
        ctx: &ProcessorContext,
    ) -> Result<(), ProjectionError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut projection = self
            .inner
            .projection_storage
            .load_projection(&ctx.view_id())
            .await?
            .unwrap_or_default();
        debug!(
            ?projection,
            "DMR: # LATEST pulled: {persistence_id} => {nr_entries}",
            persistence_id = ctx.persistence_id(),
            nr_entries = entries.len(),
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

        debug!(?last_offset, "DMR: applied to projection pulled entries for {persistence_id} => {updated_projection:?}", persistence_id=ctx.persistence_id());

        self.inner
            .projection_storage
            .save_projection(&ctx.view_id(), updated_projection, last_offset)
            .await?;
        Ok(())
    }
}
