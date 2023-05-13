use anyhow::anyhow;
use claim::*;
use coerce::actor::message::Message;
use coerce::actor::system::ActorSystem;
use coerce::actor::IntoActor;
use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::Persistence;
use coerce_cqrs::postgres::{
    PostgresOffsetStorage, PostgresStorageConfig, PostgresStorageProvider, PostgresViewStorage,
};
use coerce_cqrs::projection::RegularInterval;
use coerce_cqrs::projection::{
    OffsetStorage, PersistenceId, Processor, ProjectionId, ViewApplicator, ViewStorage,
};
use coerce_cqrs::CommandResult;
use coerce_cqrs_test::fixtures::aggregate::{
    self, Summarize, TestAggregate, TestCommand, TestEvent, TestState, TestView,
};
use once_cell::sync::Lazy;
use std::sync::Arc;
use std::time::Duration;
use tagid::Entity;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_postgres_recover_snapshot() -> anyhow::Result<()> {
    Lazy::force(&coerce_cqrs_test::setup_tracing::TEST_TRACING);
    let main_span = info_span!("test_postgres_recover_snapshot");
    let _main_span_guard = main_span.enter();

    let system = ActorSystem::new();
    let storage_config = PostgresStorageConfig {
        key_prefix: "".to_string(),
        username: "postgres".to_string(),
        password: secrecy::Secret::new("demo_pass".to_string()),
        host: "localhost".to_string(),
        port: 5432,
        database_name: "demo_cqrs_db".to_string(),
        event_journal_table_name: PostgresStorageConfig::default_event_journal_table(),
        snapshot_table_name: PostgresStorageConfig::default_snapshot_table(),
        require_ssl: false,
        min_connections: None,
        max_connections: None,
        max_lifetime: None,
        acquire_timeout: Some(Duration::from_secs(5)),
        idle_timeout: None,
    };

    let view_storage = assert_ok!(
        PostgresViewStorage::<TestView>::new("test_view", "test_view", &storage_config, &system)
            .await
    );
    let view_storage = Arc::new(view_storage);
    let view_apply = ViewApplicator::new(view_storage.clone(), aggregate::apply_test_event_to_view);
    let storage_provider =
        PostgresStorageProvider::connect(storage_config.clone(), &system).await?;
    let storage = storage_provider
        .journal_storage()
        .ok_or_else(|| anyhow!("no journal storage!"))?;
    let system = system.to_persistent(Persistence::from(storage_provider));

    let offset_storage =
        Arc::new(PostgresOffsetStorage::new("projection_offset", &storage_config, &system).await?);

    let aid = TestAggregate::next_id();
    let pid = PersistenceId::from_aggregate_id::<TestAggregate>(aid.id.as_str());
    let vid = view_storage.view_id_from_persistence(&pid);

    let projection_id = ProjectionId::new("test_postgres_projection");
    let processor =
        Processor::builder_for::<TestAggregate, _, _, _>(projection_id.clone(), aid.id.as_str())
            .with_entry_handler(view_apply)
            .with_source(storage.clone())
            .with_offset_storage(offset_storage.clone())
            .with_interval_calculator(RegularInterval::of_duration(Duration::from_millis(500)));

    let processor = assert_ok!(processor.finish());
    let processor = assert_ok!(processor.run());
    let stop_api = processor.tx_api();
    let processor_handle = tokio::spawn(async move {
        let result = processor.block_for_completion().await;
        info!(
            ?result,
            "**** PROCESSOR BLOCK FINISHED...  EXITING SPAWN..."
        );
        result
    });

    let actor = assert_ok!(
        TestAggregate::default()
            .with_snapshots(3)
            .into_actor(Some(aid.clone()), &system)
            .await
    );
    info!("**** COMMANDS - START");

    const DESCRIPTION: &str = "tests starting now!... now... now";

    let reply = assert_ok!(
        actor
            .send(TestCommand::Start(DESCRIPTION.to_string()))
            .await
    );
    assert_eq!(reply, CommandResult::Ok("active:0".to_string()));

    info!("**** CMD - TEST-1");
    let reply = assert_ok!(actor.send(TestCommand::Test(1)).await);

    assert_eq!(reply, CommandResult::Ok("active:1".to_string()));
    let summary = assert_ok!(actor.send(Summarize::default()).await);
    assert_eq!(summary, TestState::active(DESCRIPTION, vec![1]));

    info!("**** CMD - TEST-2");
    let reply = assert_ok!(actor.send(TestCommand::Test(2)).await);
    assert_eq!(reply, CommandResult::Ok("active:3".to_string()));
    let summary = assert_ok!(actor.send(Summarize::default()).await);
    assert_eq!(summary, TestState::active(DESCRIPTION, vec![1, 2]));

    info!("**** STOP ACTOR");
    assert_ok!(actor.stop().await);

    info!("**** SLEEP...");
    tokio::time::sleep(Duration::from_millis(400)).await;
    info!("**** WAKE");

    info!("**** ACQUIRING AGGREGATE ACTOR...");
    let actor = assert_ok!(
        TestAggregate::default()
            .with_snapshots(3)
            .into_actor(Some(aid.clone()), &system)
            .await
    );
    // let summary = assert_ok!(actor.send(Summarize::default()).await);
    // assert_eq!(summary, TestState::active(DESCRIPTION, vec![1, 2, 3,]));

    info!("**** CMD - TEST-3");
    let reply = assert_ok!(actor.send(TestCommand::Test(3)).await);
    assert_eq!(reply, CommandResult::Ok("active:6".to_string()));
    let summary = assert_ok!(actor.send(Summarize::default()).await);
    assert_eq!(summary, TestState::active(DESCRIPTION, vec![1, 2, 3,]));

    info!("**** CMD - TEST-5");
    let reply = assert_ok!(actor.send(TestCommand::Test(5)).await);
    assert_eq!(reply, CommandResult::Ok("active:11".to_string()));
    let summary = assert_ok!(actor.send(Summarize::default()).await);
    assert_eq!(summary, TestState::active(DESCRIPTION, vec![1, 2, 3, 5,]));

    info!("**** CMD - STOP");
    let reply = assert_ok!(actor.send(TestCommand::Stop).await);
    assert_eq!(reply, CommandResult::Ok("completed:11".to_string()));
    let summary = assert_ok!(actor.send(Summarize::default()).await);
    assert_eq!(summary, TestState::completed(DESCRIPTION, vec![1, 2, 3, 5]));

    info!("**** STOP ACTOR");
    assert_ok!(actor.stop().await);

    info!("**** SLEEP...");
    tokio::time::sleep(Duration::from_millis(500)).await;
    info!("**** WAKE");

    info!("**** LOAD VIEW...");
    let view = assert_some!(assert_ok!(view_storage.load_view(&vid).await));
    assert_eq!(
        view,
        TestView {
            label: "tests starting now!... now... now".to_string(),
            sum: 11,
        }
    );

    info!("**** EXAMINE EVENTS");

    let events = assert_some!(assert_ok!(
        storage.read_latest_messages(&format!("{:#}", pid), 0).await
    ));
    let events: Vec<_> = events
        .into_iter()
        .map(|entry| assert_ok!(TestEvent::from_bytes(entry.bytes.to_vec())))
        .collect();

    assert_eq!(
        events,
        vec![
            TestEvent::Started("tests starting now!... now... now".to_string()),
            TestEvent::Tested(1),
            TestEvent::Tested(2),
            TestEvent::Tested(3),
            TestEvent::Tested(5),
            TestEvent::Stopped,
        ]
    );

    info!("**** EXAMINE OFFSET");
    let offset = assert_some!(assert_ok!(
        offset_storage.read_offset(&projection_id, &pid).await
    ));
    info!("**** after tests offset: {offset:?}");
    assert_eq!(offset.as_i64(), 7);

    info!("**** STOP PROCESSOR...");
    assert_ok!(coerce_cqrs::projection::ProcessorCommand::stop(&stop_api).await);
    info!("**** SHUTTING DOWN ACTOR SYSTEM...");
    system.shutdown().await;
    info!("**** WAITING FOR PROCESSOR TO FINISH...");
    assert_ok!(assert_ok!(processor_handle.await));
    info!("**** DONE FINISHING TEST");
    Ok(())
}