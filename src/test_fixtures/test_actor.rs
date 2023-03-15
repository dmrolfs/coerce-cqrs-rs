use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::ActorId;
use coerce::persistent::journal::snapshot::Snapshot;
use coerce::persistent::journal::types::JournalTypes;
use coerce::persistent::journal::{PersistErr, RecoveryErr};
use coerce::persistent::{
    ActorRecovery, PersistFailurePolicy, PersistentActor, Recover, RecoverSnapshot,
    RecoveryFailurePolicy, RecoveryResult,
};
use coerce_macros::{JsonMessage, JsonSnapshot};
use once_cell::sync::Lazy;
use tagid::{CuidGenerator, CuidId, Entity, Id, Label};

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

#[async_trait]
impl PersistentActor for TestActor {
    #[tracing::instrument(level = "info", skip(journal))]
    fn configure(journal: &mut JournalTypes<Self>) {
        journal
            .snapshot::<TestActorSnapshot>("test-snapshot")
            .message::<Msg>("test-message");
    }

    #[tracing::instrument(level = "info", skip(_ctx))]
    async fn pre_recovery(&mut self, _ctx: &mut ActorContext) {}

    #[tracing::instrument(level = "info", skip(_ctx))]
    async fn post_recovery(&mut self, _ctx: &mut ActorContext) {}

    #[tracing::instrument(level = "info", skip(_ctx))]
    async fn stopped(&mut self, _ctx: &mut ActorContext) {}

    #[tracing::instrument(level = "info", skip(ctx))]
    async fn recover_journal(
        &mut self,
        persistence_key: String,
        ctx: &mut ActorContext,
    ) -> RecoveryResult<Self> {
        ActorRecovery::recover_journal(self, Some(persistence_key), ctx).await
    }

    #[tracing::instrument(level = "info")]
    fn recovery_failure_policy(&self) -> RecoveryFailurePolicy {
        RecoveryFailurePolicy::default()
    }

    #[tracing::instrument(level = "info")]
    fn persist_failure_policy(&self) -> PersistFailurePolicy {
        PersistFailurePolicy::default()
    }

    #[tracing::instrument(level = "info", skip(_ctx))]
    async fn on_recovery_err(&mut self, _err: RecoveryErr, _ctx: &mut ActorContext) {}

    #[tracing::instrument(level = "info", skip(_ctx))]
    async fn on_recovery_failed(&mut self, _ctx: &mut ActorContext) {}

    #[tracing::instrument(level = "info", skip(_ctx))]
    async fn on_child_stopped(&mut self, _id: &ActorId, _ctx: &mut ActorContext) {}
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
