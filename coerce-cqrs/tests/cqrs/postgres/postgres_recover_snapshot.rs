use anyhow::anyhow;
use claim::*;
use coerce::actor::message::Message;
use coerce::actor::system::ActorSystem;
use coerce::actor::IntoActor;
use coerce::persistent::Persistence;
use coerce_cqrs::postgres::{
    PostgresProjectionStorage, PostgresStorageConfig, PostgresStorageProvider, TableName,
};
use coerce_cqrs::projection::processor::{Processor, ProcessorSourceProvider, RegularInterval};
use coerce_cqrs::projection::{PersistenceId, ProjectionApplicator, ProjectionStorage};
use coerce_cqrs::CommandResult;
use coerce_cqrs_test::fixtures::aggregate::{
    self, Summarize, TestAggregate, TestCommand, TestEvent, TestState, TestView,
};
use once_cell::sync::Lazy;
use pretty_assertions::assert_eq;
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
        projection_offsets_table_name: PostgresStorageConfig::default_projection_offsets_table(),
        snapshot_table_name: PostgresStorageConfig::default_snapshot_table(),
        require_ssl: false,
        min_connections: None,
        max_connections: None,
        max_lifetime: None,
        acquire_timeout: Some(Duration::from_secs(5)),
        idle_timeout: None,
    };

    let projection_name = "test_recover_postgres_projection";

    let view_storage = assert_ok!(
        PostgresProjectionStorage::<TestView>::new(
            projection_name,
            TableName::new("test_view").ok(),
            TableName::new("projection_offset").unwrap(),
            &storage_config,
            &system
        )
        .await
    );
    let view_storage = Arc::new(view_storage);
    let view_apply =
        ProjectionApplicator::<TestAggregate, _, _, _>::new(aggregate::apply_test_event_to_view);
    let journal_storage_provider =
        PostgresStorageProvider::connect(storage_config.clone(), &system).await?;
    let storage = journal_storage_provider
        .processor_source()
        .ok_or_else(|| anyhow!("no processor storage!"))?;
    let system = system.to_persistent(Persistence::from(journal_storage_provider));

    let aid = TestAggregate::next_id();
    let pid = PersistenceId::from_aggregate_id::<TestAggregate>(aid.id.as_str());
    let vid = pid.clone();

    let processor = Processor::builder_for::<TestAggregate, _, _, _, _>(projection_name.clone())
        .with_entry_handler(view_apply)
        .with_system(system.clone())
        .with_source(storage.clone())
        .with_projection_storage(view_storage.clone())
        .with_interval_calculator(RegularInterval::of_duration(Duration::from_millis(50)));

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
    let summary = assert_ok!(actor.send(Summarize::default()).await);
    assert_eq!(summary, TestState::active(DESCRIPTION, vec![1, 2,]));

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
    assert_ok!(actor.notify(TestCommand::Stop));
    // assert_eq!(reply, CommandResult::Ok("completed:11".to_string()));
    // let summary = assert_ok!(actor.send(Summarize::default()).await);
    // assert_eq!(summary, TestState::completed(DESCRIPTION, vec![1, 2, 3, 5]));

    info!("**** STOP ACTOR");
    assert_ok!(actor.stop().await);

    info!("**** SLEEP...");
    tokio::time::sleep(Duration::from_millis(500)).await;
    info!("**** WAKE");
    // let actor = assert_ok!(
    //     TestAggregate::default()
    //         .with_snapshots(3)
    //         .into_actor(Some(aid.clone()), &system)
    //         .await
    // );
    // let summary = assert_ok!(actor.send(Summarize::default()).await);
    // assert_eq!(summary, TestState::completed(DESCRIPTION, vec![1, 2, 3, 5,]));

    info!("**** LOAD VIEW...");
    let view = assert_some!(assert_ok!(view_storage.load_projection(&vid).await));
    assert_eq!(
        view,
        TestView {
            label: "tests starting now!... now... now".to_string(),
            count: 5,
            sum: 11,
            events: vec![
                TestEvent::Started("tests starting now!... now... now".to_string()),
                TestEvent::Tested(1),
                TestEvent::Tested(2),
                TestEvent::Tested(3),
                TestEvent::Tested(5),
            ],
        }
    );

    info!("**** EXAMINE EVENTS");

    let events = assert_some!(assert_ok!(
        storage
            .read_latest_messages(&format!("{}", pid.as_persistence_id()), 0)
            .await
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

    // info!("**** EXAMINE OFFSET");
    // let offset = assert_some!(assert_ok!(
    //     offset_storage.read_offset(&projection_name, &pid).await
    // ));
    // info!("**** after tests offset: {offset:?}");
    // assert_eq!(offset.as_i64(), 7);

    info!("**** STOP PROCESSOR...");
    assert_ok!(coerce_cqrs::projection::processor::ProcessorCommand::stop(&stop_api).await);
    info!("**** SHUTTING DOWN ACTOR SYSTEM...");
    system.shutdown().await;
    info!("**** WAITING FOR PROCESSOR TO FINISH...");
    assert_ok!(assert_ok!(processor_handle.await));
    info!("**** DONE FINISHING TEST");
    Ok(())
}
