use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::ActorId;
use coerce::persistent::batch::EventBatch;
use coerce::persistent::snapshot::Snapshot;
use coerce::persistent::storage::JournalEntry;
use coerce::persistent::types::JournalTypes;
use coerce::persistent::{
    ActorRecovery, PersistErr, PersistFailurePolicy, PersistentActor, ReadMessages, Recover,
    RecoverSnapshot, Recovery, RecoveryErr, RecoveryFailurePolicy, Retry,
};
use coerce_cqrs::Aggregate;
use std::sync::Arc;
use tagid::{CuidGenerator, Entity, Label};

#[derive(Debug, Default, Label)]
pub struct TestActor {
    pub received_numbers: Vec<i32>,
}

impl Entity for TestActor {
    type IdGen = CuidGenerator;
}

#[derive(JsonMessage, Serialize, Deserialize)]
#[result("()")]
pub struct Msg(pub i32);

#[derive(Debug, JsonSnapshot, Serialize, Deserialize)]
pub struct TestActorSnapshot {}

impl Aggregate for TestActor {}

#[async_trait]
impl PersistentActor for TestActor {
    #[instrument(level = "info", skip(journal))]
    fn configure(journal: &mut JournalTypes<Self>) {
        journal
            .snapshot::<TestActorSnapshot>(&Self::journal_snapshot_type_identifier::<
                TestActorSnapshot,
            >())
            .message::<Msg>(&Self::journal_message_type_identifier::<Msg>());
    }

    #[instrument(level = "info", skip(_ctx))]
    async fn pre_recovery(&mut self, _ctx: &mut ActorContext) {}

    #[instrument(level = "info", skip(_ctx))]
    async fn post_recovery(&mut self, _ctx: &mut ActorContext) {}

    #[instrument(level = "info", skip(_ctx))]
    async fn stopped(&mut self, _ctx: &mut ActorContext) {}

    #[instrument(level = "info", skip(message, ctx))]
    async fn persist<M: Message>(
        &self,
        message: &M,
        ctx: &mut ActorContext,
    ) -> Result<(), PersistErr>
    where
        Self: Recover<M>,
    {
        let message_bytes = message.as_bytes();
        match message_bytes {
            Ok(bytes) => {
                let mut attempts = 1;
                let bytes = Arc::new(bytes);
                loop {
                    let result = ctx
                        .persistence_mut()
                        .journal_mut::<Self>()
                        .persist_message::<M>(bytes.clone())
                        .await;

                    if let Some(res) = check(result, &mut attempts, self, ctx).await {
                        return res;
                    }
                }
            }

            Err(e) => return Err(PersistErr::Serialisation(e)),
        }
    }

    #[instrument(level = "info", skip(batch, ctx))]
    async fn persist_batch(
        &self,
        batch: EventBatch<Self>,
        ctx: &mut ActorContext,
    ) -> Result<(), PersistErr> {
        if batch.entries().is_empty() {
            return Ok(());
        }

        let mut attempts = 1;
        loop {
            let result = ctx
                .persistence_mut()
                .journal_mut::<Self>()
                .persist_batch(&batch)
                .await;

            if let Some(res) = check(result, &mut attempts, self, ctx).await {
                return res;
            }
        }
    }

    #[instrument(level = "info", skip(snapshot, ctx))]
    async fn snapshot<S: Snapshot>(
        &self,
        snapshot: S,
        ctx: &mut ActorContext,
    ) -> Result<(), PersistErr>
    where
        Self: RecoverSnapshot<S>,
    {
        let snapshot_bytes = snapshot.into_remote_envelope();
        match snapshot_bytes {
            Ok(bytes) => {
                let bytes = Arc::new(bytes.into_bytes());

                let mut attempts = 1;
                loop {
                    let result = ctx
                        .persistence_mut()
                        .journal_mut::<Self>()
                        .persist_snapshot::<S>(bytes.clone())
                        .await;

                    if let Some(res) = check(result, &mut attempts, self, ctx).await {
                        return res;
                    }
                }
            }

            Err(e) => return Err(PersistErr::Serialisation(e)),
        }
    }

    #[instrument(level = "info", skip(ctx))]
    async fn recover(&mut self, persistence_key: String, ctx: &mut ActorContext) -> Recovery<Self> {
        ActorRecovery::recover_journal(self, Some(persistence_key), ctx).await
    }

    #[instrument(level = "info", skip(ctx))]
    fn last_sequence_id(&self, ctx: &mut ActorContext) -> i64 {
        ctx.persistence_mut()
            .journal_mut::<Self>()
            .last_sequence_id()
    }

    #[instrument(level = "info", skip(ctx))]
    async fn read_messages<'a>(
        &self,
        args: ReadMessages<'a>,
        ctx: &mut ActorContext,
    ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
        ctx.persistence_mut()
            .journal_mut::<Self>()
            .read_messages(args)
            .await
    }

    #[instrument(level = "info", skip(ctx))]
    async fn clear_old_messages(&mut self, ctx: &mut ActorContext) -> bool {
        ctx.persistence_mut()
            .journal_mut::<Self>()
            .clear_old_messages()
            .await
    }

    #[instrument(level = "info")]
    fn recovery_failure_policy(&self) -> RecoveryFailurePolicy {
        RecoveryFailurePolicy::default()
    }

    #[instrument(level = "info")]
    fn persist_failure_policy(&self) -> PersistFailurePolicy {
        PersistFailurePolicy::default()
    }

    #[instrument(level = "info", skip(ctx))]
    fn event_batch(&self, ctx: &ActorContext) -> EventBatch<Self> {
        EventBatch::create(ctx)
    }

    #[instrument(level = "info", skip(_ctx))]
    async fn on_recovery_err(&mut self, _err: RecoveryErr, _ctx: &mut ActorContext) {}

    #[instrument(level = "info", skip(_ctx))]
    async fn on_recovery_failed(&mut self, _ctx: &mut ActorContext) {}

    #[instrument(level = "info", skip(_ctx))]
    async fn on_child_stopped(&mut self, _id: &ActorId, _ctx: &mut ActorContext) {}
}

async fn check<A: PersistentActor>(
    result: Result<(), PersistErr>,
    attempts: &mut usize,
    actor: &A,
    ctx: &mut ActorContext,
) -> Option<Result<(), PersistErr>> {
    match result {
        Ok(res) => Some(Ok(res)),

        Err(e) => {
            let failure_policy = actor.persist_failure_policy();

            error!(error=%e, actor_id=%ctx.id(), %failure_policy, %attempts, "persist failed");

            match failure_policy {
                PersistFailurePolicy::Retry(policy) => {
                    if !should_retry(ctx, attempts, policy).await {
                        return Some(Err(e));
                    }
                }

                PersistFailurePolicy::ReturnErr => return Some(Err(e)),

                PersistFailurePolicy::StopActor => {
                    ctx.stop(None);
                    return Some(Err(PersistErr::ActorStopping(Box::new(e))));
                }

                PersistFailurePolicy::Panic => panic!("persist failed"),
            };

            *attempts += 1;
            None
        }
    }
}

async fn should_retry(ctx: &mut ActorContext, attempts: &usize, policy: Retry) -> bool {
    match policy {
        Retry::UntilSuccess { delay } => {
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
        }

        Retry::MaxAttempts {
            max_attempts,
            delay,
        } => {
            if &max_attempts <= attempts {
                ctx.stop(None);
                return false;
            }

            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
        }
    }

    true
}

#[async_trait]
impl Handler<Msg> for TestActor {
    async fn handle(&mut self, message: Msg, ctx: &mut ActorContext) {
        if self.persist(&message, ctx).await.is_ok() {
            info!("persist ok, msg-number: {}", message.0);
            self.received_numbers.push(message.0);
        } else {
            // NACK
        }
    }
}

#[async_trait]
impl Recover<Msg> for TestActor {
    async fn recover(&mut self, message: Msg, _ctx: &mut ActorContext) {
        info!("recovered a msg-number: {}", message.0);
        self.received_numbers.push(message.0);
    }
}

#[async_trait]
impl RecoverSnapshot<TestActorSnapshot> for TestActor {
    async fn recover(&mut self, snapshot: TestActorSnapshot, _ctx: &mut ActorContext) {
        info!("recovered a snapshot: {snapshot:?}");
    }
}
