use crate::fixtures::test_aggregate::{self, Summarize, TestAggregate, TestCommand, TestView};
use anyhow::anyhow;
use claim::*;
use coerce::actor::system::ActorSystem;
use coerce::actor::IntoActor;
use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::Persistence;
use coerce_cqrs::postgres::{PostgresStorageConfig, PostgresStorageProvider};
use coerce_cqrs::projection::RegularInterval;
use coerce_cqrs::projection::{
    InMemoryOffsetStorage, InMemoryViewStorage, OffsetStorage, PersistenceId, Processor,
    ProjectionId, ViewApplicator, ViewStorage,
};
use once_cell::sync::Lazy;
use secrecy::Secret;
use std::sync::Arc;
use std::time::Duration;
use tagid::Entity;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_memory_processor_config() -> anyhow::Result<()> {
    Lazy::force(&crate::TEST_TRACING);
    let main_span = tracing::info_span!("view_memory_storage::test_load_and_save");
    let _main_span_guard = main_span.enter();

    let system = ActorSystem::new();
    let view_storage = Arc::new(InMemoryViewStorage::<TestView>::new("test_load_and_save"));
    let view_apply = ViewApplicator::new(
        view_storage.clone(),
        test_aggregate::apply_test_event_to_view,
    );
    let storage_config = PostgresStorageConfig {
        key_prefix: "".to_string(),
        username: "test_user".to_string(),
        password: Secret::new("test_password".to_string()),
        host: "localhost".to_string(),
        port: 5432,
        database_name: "test_database".to_string(),
        require_ssl: false,
        min_connections: None,
        max_connections: None,
        max_lifetime: None,
        idle_timeout: None,
    };
    let storage_provider = PostgresStorageProvider::connect(storage_config, &system).await?;
    let storage = storage_provider
        .journal_storage()
        .ok_or_else(|| anyhow!("no journal storage!"))?;
    let system = system.to_persistent(Persistence::from(storage_provider));

    let offset_storage = Arc::new(InMemoryOffsetStorage::default());

    let aid = TestAggregate::next_id();
    let pid = PersistenceId::from_aggregate_id::<TestAggregate>(aid.id.as_str());
    let vid = view_storage.view_id_from_persistence(&pid);

    let processor =
        Processor::builder_for::<TestAggregate, _, _, _>("test_memory_projection", aid.id.as_str())
            .with_entry_handler(view_apply)
            .with_source(storage.clone())
            .with_offset_storage(offset_storage.clone())
            .with_interval_calculator(RegularInterval::of_duration(Duration::from_millis(50)));

    let processor = assert_ok!(processor.finish());
    let processor = assert_ok!(processor.run());
    let stop_api = processor.tx_api();
    let processor_handle = tokio::spawn(async move {
        let result = processor.block_for_completion().await;
        tracing::info!(
            ?result,
            "DMR: PROCESSOR BLOCK FINISHED...  EXITING SPAWN..."
        );
        result
    });

    let actor = assert_ok!(
        TestAggregate::default()
            .into_actor(Some(aid.clone()), &system)
            .await
    );
    tracing::info!("DMR: CMD - START");
    assert_ok!(actor.notify(TestCommand::Start(
        "test starting now!... now... now".to_string()
    )));

    tracing::info!("DMR: CMD - TEST-1");
    assert_ok!(actor.notify(TestCommand::Test(1)));
    let summary = assert_ok!(actor.send(Summarize).await);
    assert_eq!(summary, 1);

    tracing::info!("DMR: CMD - TEST-2");
    assert_ok!(actor.notify(TestCommand::Test(2)));
    let summary = assert_ok!(actor.send(Summarize).await);
    assert_eq!(summary, 3);

    tracing::info!("DMR: CMD - TEST-3");
    assert_ok!(actor.notify(TestCommand::Test(3)));
    let summary = assert_ok!(actor.send(Summarize).await);
    assert_eq!(summary, 6);

    tracing::info!("DMR: CMD - TEST-5");
    assert_ok!(actor.notify(TestCommand::Test(5)));
    let summary = assert_ok!(actor.send(Summarize).await);
    assert_eq!(summary, 11);

    tracing::info!("DMR: CMD - STOP");
    assert_ok!(actor.notify(TestCommand::Stop));
    let summary = assert_ok!(actor.send(Summarize).await);
    assert_eq!(summary, 11);

    tracing::info!("DMR: STOP ACTOR");
    assert_ok!(actor.stop().await);

    tracing::info!("DMR: SLEEP...");
    tokio::time::sleep(Duration::from_millis(100)).await;
    tracing::info!("DMR: WAKE");

    tracing::info!("DMR: LOAD VIEW...");
    let view = assert_some!(assert_ok!(view_storage.load_view(&vid).await));
    assert_eq!(
        view,
        TestView {
            label: "test starting now!... now... now".to_string(),
            sum: 11,
        }
    );

    let offset = assert_some!(assert_ok!(
        offset_storage
            .read_offset(&ProjectionId::new("test_memory_projection"), &pid)
            .await
    ));
    tracing::info!("DMR: after test offset: {offset:?}");
    assert_eq!(offset.as_i64(), 6);

    tracing::info!("DMR: STOP PROCESSOR...");
    assert_ok!(coerce_cqrs::projection::ProcessorCommand::stop(&stop_api).await);
    tracing::info!("DMR: SHUTTING DOWN ACTOR SYSTEM...");
    system.shutdown().await;
    tracing::info!("DMR: WAITING FOR PROCESSOR TO FINISH...");
    assert_ok!(assert_ok!(processor_handle.await));
    tracing::info!("DMR: DONE FINISHING TEST");
    Ok(())
}
