// use coerce::persistent::PersistentActor;

// pub trait Aggregate: PersistentActor {}

// #[cfg(test)]
// mod tests {
//     use coerce::actor::system::ActorSystem;
//     use coerce::persistent::journal::provider::inmemory::InMemoryStorageProvider;
//     use coerce::sharding::coordinator::allocation::AllocateShardErr::Persistence;
//     use once_cell::sync::Lazy;
//     // use tag
//
//     #[test]
//     pub fn test_aggregate_recovery() {
//         Lazy::force(&crate::fixtures::TEST_TRACING);
//         let main_span = tracing::info_span!("aggregate::test_aggregate_recovery");
//         let _main_span_guard = main_span.enter();
//
//         let system = ActorSystem::new().to_persistent(Persistence::from(InMemoryStorageProvider::new()));
//         // let id =
//         tracing::info!("entering test...");
//         tokio_test::block_on(async move {
//
//         })
//     }
// }