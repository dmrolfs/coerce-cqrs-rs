use coerce::actor::context::ActorContext;
use coerce::actor::message::Message;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{Debug, Display};
use thiserror::Error;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[allow(dead_code)]
pub enum CommandResult<T>
where
    T: Debug + PartialEq,
{
    Ok(T),
    Rejected(String),
    Err(String),
}

impl<T> Eq for CommandResult<T> where T: Debug + PartialEq + Eq {}

impl<T> CommandResult<T>
where
    T: Debug + PartialEq + Serialize + DeserializeOwned,
{
    pub const fn ok(payload: T) -> Self {
        Self::Ok(payload)
    }

    pub fn rejected(message: impl Into<String>) -> Self {
        Self::Rejected(message.into())
    }

    pub fn err(error: impl Display) -> Self {
        Self::Err(error.to_string())
    }
}

impl<T, E> From<E> for CommandResult<T>
where
    T: Debug + PartialEq,
    E: Error + Display,
{
    fn from(error: E) -> Self {
        Self::Err(error.to_string())
    }
}

pub trait ApplyAggregateEvent<E> {
    type BaseType;
    fn apply_event(&mut self, event: E, ctx: &mut ActorContext) -> Option<Self::BaseType>;
}

pub trait AggregateState<C, E>
where
    C: Message,
{
    type Error;
    type State;

    fn handle_command(&self, command: C, ctx: &mut ActorContext) -> Result<Vec<E>, Self::Error>;

    fn apply_event(&mut self, event: E, ctx: &mut ActorContext) -> Option<Self::State>;
}

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum AggregateError {
    #[error("rejected command: {0}")]
    RejectedCommand(String),

    #[error("{0}")]
    Persist(#[from] coerce::persistent::journal::PersistErr),
}

#[cfg(test)]
mod tests {
    use claim::{assert_ok, assert_some};
    use coerce::actor::system::ActorSystem;
    use coerce::actor::IntoActor;
    use coerce::persistent::journal::provider::StorageProvider;
    use coerce::persistent::Persistence;
    use coerce_cqrs_test::fixtures::actor::{Msg, TestActor};
    use once_cell::sync::Lazy;
    use tagid::Entity;

    #[test]
    pub fn test_aggregate_recovery() {
        Lazy::force(&coerce_cqrs_test::setup_tracing::TEST_TRACING);
        let main_span = tracing::info_span!("aggregate::test_aggregate_recovery");
        let _main_span_guard = main_span.enter();

        tracing::info!("entering tests...");
        tokio_test::block_on(async move {
            let provider_system = ActorSystem::new();
            let provider_config = crate::postgres::PostgresStorageConfig {
                key_prefix: "tests:".to_string(),
                username: "damon".to_string(),
                password: secrecy::Secret::new("otis".to_string()),
                host: "localhost".to_string(),
                port: 5432,
                database_name: "aggregate_test".to_string(),
                event_journal_table_name: crate::postgres::PostgresStorageConfig::default_event_journal_table(),
                snapshot_table_name: crate::postgres::PostgresStorageConfig::default_snapshot_table(),
                require_ssl: false,
                min_connections: None,
                max_connections: None,
                max_lifetime: None,
                idle_timeout: None,
            };
            let storage_provider = assert_ok!(
                crate::postgres::PostgresStorageProvider::connect(
                    provider_config,
                    &provider_system
                )
                .await
            );
            let storage = assert_some!(storage_provider.journal_storage());
            let system = ActorSystem::new().to_persistent(Persistence::from(storage_provider));
            let create_empty_actor = TestActor::default;

            info!("DMR: INITIAL AGGREGATE SETUP...");
            let id = TestActor::next_id();
            let actor = assert_ok!(
                create_empty_actor()
                    .into_actor(Some(id.clone()), &system)
                    .await
            );
            let pid = id.to_string();
            let journal = storage.read_latest_messages(&pid, 0).await;
            info!(?actor, ?journal, "DMR: before - actor and journal");
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
            info!(?actor, ?journal, "DMR: after - actor and journal");
            assert_eq!(actual, vec![1, 2, 3, 4]);
            assert_ok!(actor.stop().await);

            info!("DMR: RECOVER AGGREGATE...");
            let recovered_actor =
                assert_ok!(create_empty_actor().into_actor(Some(id), &system).await);
            info!("recovered_actor: {recovered_actor:?}");
            info!(
                ?recovered_actor,
                ?journal,
                "DMR: before - recovered_actor and journal"
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
                "DMR: after - recovered_actor and journal"
            );
            assert_eq!(recovered, vec![1, 2, 3, 4]);

            info!("DMR: SHUTDOWN...");
            system.shutdown().await;
        })
    }
}
