// #[async_trait]
// pub trait Aggregate: Entity + PersistentActor {
//     async fn
// }
//
// #[async_trait]
// impl<A> PersistentActor for A
// where
//     A: Aggregate,
// {
//     #[tracing::instrument(level = "debug", skip(journal))]
//     fn configure(journal: &mut JournalTypes<Self>) {
//         todo!()
//     }
// }

#[cfg(test)]
mod tests {
    use crate::fixtures::{Msg, TestActor};
    use claim::{assert_ok, assert_some};
    use coerce::actor::system::ActorSystem;
    use coerce::actor::IntoActor;
    use coerce::persistent::journal::provider::StorageProvider;
    use coerce::persistent::Persistence;
    use once_cell::sync::Lazy;
    use tagid::{CuidId, Entity};

    #[test]
    pub fn test_aggregate_recovery() {
        Lazy::force(&crate::fixtures::TEST_TRACING);
        let main_span = tracing::info_span!("aggregate::test_aggregate_recovery");
        let _main_span_guard = main_span.enter();

        tracing::info!("entering test...");
        tokio_test::block_on(async move {
            let provider_system = ActorSystem::new();
            let provider_config = crate::postgres::PostgresStorageConfig {
                key_prefix: "test:".to_string(),
                username: "damon".to_string(),
                password: secrecy::Secret::new("otis".to_string()),
                host: "localhost".to_string(),
                port: 5432,
                database_name: "aggregate_test".to_string(),
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
