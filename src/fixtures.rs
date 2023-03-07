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
    fn persistence_key(&self, ctx: &ActorContext) -> String {
        ctx.id().to_string()
    }

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

// -- tracing --
use tracing::{subscriber, Subscriber};
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_log::LogTracer;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Registry};

#[allow(dead_code)]
pub static TEST_TRACING: Lazy<()> = Lazy::new(|| {
    let default_filter_level = "info";
    let subscriber_name = "test";
    if std::env::var("TEST_LOG").is_ok() {
        let subscriber = get_subscriber(subscriber_name, default_filter_level, std::io::stdout);
        init_subscriber(subscriber);
    } else {
        let subscriber = get_subscriber(subscriber_name, default_filter_level, std::io::sink);
        init_subscriber(subscriber);
    };
});

#[allow(unused)]
pub fn get_subscriber<S0, S1, W>(name: S0, env_filter: S1, sink: W) -> impl Subscriber + Sync + Send
where
    S0: Into<String>,
    S1: AsRef<str>,
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(env_filter));

    let formatting_layer = BunyanFormattingLayer::new(name.into(), sink);

    Registry::default()
        .with(env_filter)
        .with(JsonStorageLayer)
        .with(formatting_layer)
}

pub fn init_subscriber(subscriber: impl Subscriber + Sync + Send) {
    LogTracer::init().expect("Failed to set logger");
    subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
}
