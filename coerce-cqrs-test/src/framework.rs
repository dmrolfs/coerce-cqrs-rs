use crate::fixtures::aggregate::{Summarizable, Summarize};
use coerce::actor::message::{Handler, Message};
use coerce::actor::system::ActorSystem;
use coerce::actor::{IntoActor, LocalActorRef};
use coerce::persistent::provider::StorageProvider;
use coerce::persistent::storage::{JournalEntry, JournalStorageRef};
use coerce::persistent::{Persistence, PersistentActor};
use coerce_cqrs::memory::InMemoryStorageProvider;
use coerce_cqrs::projection::PersistenceId;
use pretty_assertions::assert_eq;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use tagid::{Entity, Id, IdGenerator};

/// A framework to test aggregates.
pub struct TestFramework<A, S>
where
    A: PersistentActor + Entity,
    S: StorageProvider,
{
    aggregate: A,
    system: Option<ActorSystem>,
    storage_provider: Option<S>,
}

impl<A, S> fmt::Debug for TestFramework<A, S>
where
    A: PersistentActor + Entity + Default + fmt::Debug,
    S: StorageProvider,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestFramework")
            .field("aggregate", &self.aggregate)
            .field("system", &self.system.as_ref().map(|s| s.system_id()))
            .field("has_storage_provider", &self.storage_provider.is_some())
            .finish()
    }
}

impl<A, S> Default for TestFramework<A, S>
where
    A: PersistentActor + Entity + Default,
    S: StorageProvider,
{
    fn default() -> Self {
        Self::for_aggregate(A::default())
    }
}

impl<A> TestFramework<A, InMemoryStorageProvider>
where
    A: PersistentActor + Entity,
{
    pub fn with_memory_storage(self) -> Self {
        self.with_storage(InMemoryStorageProvider::default())
    }
}

impl<A, S> TestFramework<A, S>
where
    A: PersistentActor + Entity,
    S: StorageProvider,
{
    pub const fn for_aggregate(aggregate: A) -> Self {
        Self {
            aggregate,
            system: None,
            storage_provider: None,
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_system(self, system: ActorSystem) -> Self {
        Self {
            system: Some(system),
            ..self
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_storage(self, storage_provider: S) -> Self {
        Self {
            storage_provider: Some(storage_provider),
            ..self
        }
    }

    fn into_parts(self) -> (A, ActorSystem, JournalStorageRef) {
        let storage_provider = self.storage_provider.expect("no storage provider!");
        let journal = storage_provider
            .journal_storage()
            .expect("no journal from storage provider");

        let system = self
            .system
            .unwrap_or_default()
            .to_persistent(Persistence::from(storage_provider));

        (self.aggregate, system, journal)
    }
}

impl<A, S> TestFramework<A, S>
where
    A: PersistentActor + Entity,
    <<A as Entity>::IdGen as IdGenerator>::IdType: Clone + Send + Sync + fmt::Display,
    S: StorageProvider,
{
    /// Initiates an aggregate test with no prior events.
    ///
    /// ```
    /// # use coerce_cqrs_test::fixtures::aggregate::TestAggregate;
    /// use coerce_cqrs_test::framework::TestFramework;
    ///
    /// let executor = TestFramework::<TestAggregate, _>::default()
    ///     .with_memory_storage()
    ///     .given_no_previous_events();
    /// ```
    #[must_use = "must use AggregateTestExecutor"]
    pub async fn given_no_previous_events(self) -> AggregateTestExecutor<A> {
        let (aggregate, system, journal) = self.into_parts();

        let aggregate_id = A::next_id();

        // after save events in journal
        let actor = aggregate
            .into_actor(Some(aggregate_id.clone()), &system)
            .await
            .expect("failed to instantiate local actor for aggregate");

        AggregateTestExecutor {
            actor,
            aggregate_id,
            system,
            journal,
        }
    }

    /// Initiates an aggregate test with a history of prior events.
    ///
    /// ```
    /// # use coerce_cqrs_test::fixtures::aggregate::{TestAggregate, TestEvent};
    /// use coerce_cqrs_test::framework::TestFramework;
    ///
    /// let executor = TestFramework::<TestAggregate, _>::default()
    ///     .with_memory_storage()
    ///     .given(
    ///         "my_journal_message_type",
    ///         vec![TestEvent::Started("started".to_string()), TestEvent::Tested(1)]
    ///     );
    /// ```
    #[must_use = "must use AggregateTestExecutor"]
    pub async fn given<E>(
        self,
        message_type_identifier: &str,
        events: Vec<E>,
    ) -> AggregateTestExecutor<A>
    where
        E: Message,
    {
        let (aggregate, system, journal) = self.into_parts();

        let aggregate_id = A::next_id();
        let persistence_id: PersistenceId = aggregate_id.clone().into();

        let entries = entries_from(message_type_identifier, events, 0);
        journal
            .write_message_batch(&persistence_id.as_persistence_id(), entries)
            .await
            .expect("failed to write message batch");

        let actor = aggregate
            .into_actor(Some(aggregate_id.clone()), &system)
            .await
            .expect("failed to instantiate local actor for aggregate");
        AggregateTestExecutor {
            actor,
            aggregate_id,
            system,
            journal,
        }
    }
}

fn entries_from<M: Message>(
    message_type: &str,
    messages: Vec<M>,
    offset_sequence_id: i64,
) -> Vec<JournalEntry> {
    messages
        .into_iter()
        .enumerate()
        .map(|(index, msg)| {
            let index =
                i64::try_from(index).expect("failed to convert usize index for sequence id");
            entry_from(message_type, msg, index + offset_sequence_id)
        })
        .collect()
}

fn entry_from<M: Message>(payload_type: &str, message: M, sequence_id: i64) -> JournalEntry {
    JournalEntry {
        sequence: sequence_id,
        payload_type: payload_type.into(),
        bytes: Arc::new(
            message
                .as_bytes()
                .expect("failed to convert message into bytes"),
        ),
    }
}

pub struct AggregateTestExecutor<A>
where
    A: PersistentActor + Entity,
{
    actor: LocalActorRef<A>,
    aggregate_id: Id<A, <<A as Entity>::IdGen as IdGenerator>::IdType>,
    system: ActorSystem,
    #[allow(dead_code)]
    journal: JournalStorageRef,
}

impl<A> fmt::Debug for AggregateTestExecutor<A>
where
    A: PersistentActor + Entity,
    <<A as Entity>::IdGen as IdGenerator>::IdType: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateTestExecutor")
            .field("actor", &self.actor)
            .field("aggregate_id", &self.aggregate_id)
            .field("system_id", &self.system.system_id())
            .finish()
    }
}

impl<A> Clone for AggregateTestExecutor<A>
where
    A: Clone + PersistentActor + Entity,
    <<A as Entity>::IdGen as IdGenerator>::IdType: Clone,
{
    fn clone(&self) -> Self {
        Self {
            actor: self.actor.clone(),
            aggregate_id: self.aggregate_id.clone(),
            system: self.system.clone(),
            journal: self.journal.clone(),
        }
    }
}

impl<A> AggregateTestExecutor<A>
where
    A: PersistentActor + Entity + Summarizable + Handler<Summarize<A>>,
{
    pub async fn when<C>(
        self,
        command: C,
    ) -> AggregateResultValidator<A, <C as Message>::Result, <Summarize<A> as Message>::Result>
    where
        A: Handler<C>,
        C: Message,
    {
        when(self.actor, command).await
    }
}

async fn when<A, C>(
    aggregate: LocalActorRef<A>,
    command: C,
) -> AggregateResultValidator<A, <C as Message>::Result, <Summarize<A> as Message>::Result>
where
    A: PersistentActor + Entity + Summarizable + Handler<C> + Handler<Summarize<A>>,
    C: Message,
{
    let reply = aggregate
        .send(command)
        .await
        .expect("failed to send command to actor");
    let summarize = Summarize::default();
    let state_summary = aggregate
        .send(summarize)
        .await
        .expect("failed to send summarize command to actor");
    AggregateResultValidator::new(reply, state_summary)
}

pub struct AggregateResultValidator<Aggregate, Reply, Summary>
where
    Aggregate: PersistentActor + Summarizable,
{
    reply: Reply,
    state_summary: Summary,
    _marker: PhantomData<Aggregate>,
}

impl<Aggregate, Reply, Summary> AggregateResultValidator<Aggregate, Reply, Summary>
where
    Aggregate: PersistentActor + Summarizable,
{
    const fn new(reply: Reply, state_summary: Summary) -> Self {
        Self {
            reply,
            state_summary,
            _marker: PhantomData,
        }
    }
}

impl<A, R, S> AggregateResultValidator<A, R, S>
where
    A: PersistentActor + Summarizable,
    R: fmt::Debug + PartialEq,
    S: fmt::Debug + PartialEq,
{
    /// Verifies that the expected events have been produced by the command.
    ///
    /// ```
    /// use coerce_cqrs::CommandResult;
    /// use coerce_cqrs_test::framework::TestFramework;
    /// use coerce_cqrs_test::fixtures::aggregate::{TestAggregate, TestCommand};
    /// async fn test() {
    ///
    /// TestFramework::<TestAggregate, _>::default()
    ///     .with_memory_storage()
    ///     .given_no_previous_events()
    ///     .await
    ///     .when(TestCommand::Start("my test".to_string()))
    ///     .await
    ///     .then_expect_reply(CommandResult::Ok("active:0".to_string()));
    ///
    /// }
    /// ```
    pub fn then_expect_reply(&self, expected_reply: R) -> &Self {
        // let actual_reply =  match &self.reply {
        //     Ok(reply) => reply,
        //     Err(error) => panic!("expected success, received actor error: '{error}'"),
        // };
        //
        // assert_eq!(actual_reply, &expected_reply);
        assert_eq!(self.reply, expected_reply);
        self
    }

    pub fn then_expect_state_summary(&self, expected_summary: S) -> &Self {
        // let actual_summary = match &self.state_summary {
        //     Ok(summary) => summary,
        //     Err(error) => panic!("expected success, received actor error: '{error}'"),
        // };
        //
        // assert_eq!(actual_summary, &expected_summary);
        assert_eq!(self.state_summary, expected_summary);
        self
    }

    /// Returns the internal error payload for validation by the user.
    ///
    /// ```
    /// use coerce_cqrs::CommandResult;
    /// use coerce_cqrs_test::fixtures::aggregate::{TestAggregate, TestCommand};
    /// use coerce_cqrs_test::framework::TestFramework;
    ///
    ///  async fn test() {
    ///
    /// let (reply, summary) = TestFramework::<TestAggregate, _>::default()
    ///     .with_memory_storage()
    ///     .given_no_previous_events()
    ///     .await
    ///     .when(TestCommand::Test(17))
    ///     .await
    ///     .inspect_parts();
    ///
    /// let expected = CommandResult::Rejected("no mas".to_string());
    /// assert_eq!(reply, expected);
    ///
    /// }
    /// ```
    #[allow(clippy::missing_const_for_fn)]
    pub fn inspect_parts(self) -> (R, S) {
        (self.reply, self.state_summary)
    }
}

// impl<A, R, S> AggregateResultValidator<A, R, S>
// where
//     A: PersistentActor + Summarizable,
//     R: fmt::Debug,
// {
//     // pub fn then_expect_error(self, expected_error: ActorRefErr) {
//         // match self.reply {
//         //     Ok(reply) => panic!("expected error, received reply: '{reply:?}'"),
//         //     Err(error) => assert_eq!(error, expected_error),
//         // }
//     // }
//
//     /// Verifies that the result is a `UserError` and returns the internal error payload for
//     /// further validation.
//     ///
//     /// ```
//     /// use coerce_cqrs::CommandResult;
//     /// use coerce_cqrs_test::framework::TestFramework;
//     /// use coerce_cqrs_test::fixtures::aggregate::{TestAggregate, TestCommand};
//     /// async fn test() {
//     ///
//     /// TestFramework::<TestAggregate, _>::default()
//     ///     .with_memory_storage()
//     ///     .given_no_previous_events()
//     ///     .await
//     ///     .when(TestCommand::Test(13))
//     ///     .await
//     ///     .then_expect_error_message("the expected error message");
//     ///
//     /// }
//     /// ```
//     // pub fn then_expect_error_message(self, error_message: impl Into<String>) {
//     //     match self.reply {
//     //         Ok(reply) => panic!("expected error, received reply: {reply:?}"),
//     //         Err(error) => assert_eq!(error.to_string(), error_message.into()),
//     //     }
//     // }
// }
