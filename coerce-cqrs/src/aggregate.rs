mod command_result;
mod snapshot_trigger;

pub use command_result::CommandResult;
pub use snapshot_trigger::SnapshotTrigger;

use coerce::actor::context::ActorContext;
use coerce::actor::message::Message;
use std::fmt::Debug;
use thiserror::Error;

pub trait AggregateState<C, E>
where
    C: Message,
{
    type Error;
    type State;

    fn handle_command(&self, command: &C) -> CommandResult<Vec<E>, Self::Error>;

    fn apply_event(&mut self, event: E) -> Option<Self::State>;

    fn then_run(&self, _command: &C, _ctx: &ActorContext) {}
}

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum AggregateError {
    #[error("{0}")]
    Persist(#[from] coerce::persistent::journal::PersistErr),
}

#[cfg(test)]
mod tests {
    use crate::postgres::PostgresStorageConfig;
    use crate::projection::{processor::ProcessorSourceProvider, PersistenceId};
    use crate::SnapshotTrigger;
    use claim::{assert_ok, assert_some};
    use coerce::actor::system::ActorSystem;
    use coerce::actor::IntoActor;
    use coerce::persistent::Persistence;
    use coerce_cqrs_test::fixtures::actor::{Msg, TestActor};
    use once_cell::sync::Lazy;
    use pretty_assertions::assert_eq;
    use std::time::Duration;
    use tagid::Entity;

    #[test]
    pub fn test_snapshot_trigger_none() {
        let mut trigger = SnapshotTrigger::none();
        for _ in 0..10 {
            assert_eq!(trigger.incr(), false);
        }
    }

    #[test]
    pub fn test_snapshot_trigger_on_event_count() {
        Lazy::force(&coerce_cqrs_test::setup_tracing::TEST_TRACING);
        let main_span = tracing::info_span!("aggregate::test_snapshot_trigger_on_event_count");
        let _main_span_guard = main_span.enter();

        let mut trigger = SnapshotTrigger::on_event_count(3);
        for i in 1..=10 {
            assert_eq!(trigger.incr(), i % 3 == 0);
        }
    }

    #[test]
    pub fn test_aggregate_recovery() {
        Lazy::force(&coerce_cqrs_test::setup_tracing::TEST_TRACING);
        let main_span = tracing::info_span!("aggregate::test_aggregate_recovery");
        let _main_span_guard = main_span.enter();

        tracing::info!("entering tests...");
        tokio_test::block_on(async move {
            let provider_system = ActorSystem::new();
            let provider_config = PostgresStorageConfig {
                key_prefix: "tests".to_string(),
                username: "postgres".to_string(),
                password: secrecy::Secret::new("demo_pass".to_string()),
                host: "localhost".to_string(),
                port: 5432,
                database_name: "demo_cqrs_db".to_string(),
                event_journal_table_name: PostgresStorageConfig::default_event_journal_table(),
                projection_offsets_table_name:
                    PostgresStorageConfig::default_projection_offsets_table(),
                snapshot_table_name: PostgresStorageConfig::default_snapshot_table(),
                require_ssl: false,
                min_connections: None,
                max_connections: None,
                max_lifetime: None,
                acquire_timeout: Some(Duration::from_secs(10)),
                idle_timeout: None,
            };
            let storage_provider = assert_ok!(
                crate::postgres::PostgresStorageProvider::connect(
                    provider_config,
                    &provider_system
                )
                .await
            );
            let storage = assert_some!(storage_provider.processor_source());
            let system = ActorSystem::new().to_persistent(Persistence::from(storage_provider));
            let create_empty_actor = TestActor::default;

            info!("**** INITIAL AGGREGATE SETUP...");
            let id = TestActor::next_id();
            let actor = assert_ok!(
                create_empty_actor()
                    .into_actor(Some(id.clone()), &system)
                    .await
            );
            let pid: PersistenceId = id.clone().into();
            let journal = storage
                .read_latest_messages(&pid.as_persistence_id(), 0)
                .await;
            info!(?actor, ?journal, "**** before - actor and journal");
            assert_ok!(actor.notify(Msg(1)));
            assert_ok!(actor.notify(Msg(2)));
            assert_ok!(actor.notify(Msg(3)));
            assert_ok!(actor.notify(Msg(4)));

            let actual = assert_ok!(
                actor
                    .exec(|a| {
                        info!("received: {:?}", &a.received_numbers);
                        a.received_numbers.clone()
                    })
                    .await
            );
            info!(?actor, ?journal, "**** after - actor and journal");
            assert_eq!(actual, vec![1, 2, 3, 4]);
            assert_ok!(actor.stop().await);

            info!("**** RECOVER AGGREGATE...");
            let recovered_actor =
                assert_ok!(create_empty_actor().into_actor(Some(id), &system).await);
            info!("recovered_actor: {recovered_actor:?}");
            info!(
                ?recovered_actor,
                ?journal,
                "**** before - recovered_actor and journal"
            );
            let recovered = assert_ok!(
                recovered_actor
                    .exec(|a| {
                        info!("recovered received: {:?}", &a.received_numbers);
                        a.received_numbers.clone()
                    })
                    .await
            );
            info!(
                ?recovered_actor,
                ?journal,
                "**** after - recovered_actor and journal"
            );
            assert_eq!(recovered, vec![1, 2, 3, 4]);

            info!("**** SHUTDOWN...");
            system.shutdown().await;
        })
    }
}
