use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::ActorRefErr;
use coerce::persistent::types::JournalTypes;
use coerce::persistent::{PersistentActor, Recover, RecoverSnapshot};
use coerce_cqrs::{AggregateError, AggregateState, ApplyAggregateEvent, CommandResult};
use std::fmt;
use std::marker::PhantomData;
use tagid::{CuidGenerator, Entity, Label};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestView {
    pub label: String,
    pub count: usize,
    pub events: Vec<TestEvent>,
    pub sum: i32,
}

impl Default for TestView {
    fn default() -> Self {
        Self {
            label: "<empty>".to_string(),
            count: 0,
            events: vec![],
            sum: 0,
        }
    }
}

#[allow(dead_code, clippy::missing_const_for_fn)]
#[instrument(level = "debug")]
pub fn apply_test_event_to_view(mut view: TestView, event: TestEvent) -> (TestView, bool) {
    let is_updated = match &event {
        TestEvent::Started(label) => {
            debug!("DMR: VIEW: updating label: {label}");
            view.label = label.to_string();
            view.count += 1;
            view.events.push(event.clone());
            true
        }

        TestEvent::Tested(value) => {
            let old_sum = view.sum;
            view.count += 1;
            view.sum += value;
            view.events.push(event.clone());
            debug!(
                "DMR: VIEW: updating sum: {old_sum} + {value} = {new_sum}",
                new_sum = view.sum
            );
            true
        }

        TestEvent::Stopped => {
            debug!("DMR: VIEW: stopped event -- no view update");
            false
        }
    };

    debug!(
        new_view=?view,
        "DMR: VIEW: view {} updated.",
        if is_updated { "was" } else { "was not" }
    );

    (view, is_updated)
}

#[derive(Debug, Clone, PartialEq, Eq, JsonMessage, Serialize, Deserialize)]
#[result("CommandResult<String>")]
pub enum TestCommand {
    Start(String),
    Test(i32),
    Stop,
}

pub trait Summarizable {
    type Summary: fmt::Debug + PartialEq + Send + Sync;
    fn summarize(&self, ctx: &ActorContext) -> Self::Summary;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summarize<A: Summarizable> {
    _marker: PhantomData<A>,
}

impl<A: Summarizable> Default for Summarize<A> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<A: Summarizable + 'static> Message for Summarize<A> {
    type Result = <A as Summarizable>::Summary;
}

#[allow(unsafe_code)]
unsafe impl<A: Summarizable> Send for Summarize<A> {}

#[allow(unsafe_code)]
unsafe impl<A: Summarizable> Sync for Summarize<A> {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvokeError;

impl Message for ProvokeError {
    type Result = Result<(), ActorRefErr>;
}

#[derive(Debug, Clone, PartialEq, Eq, JsonMessage, Serialize, Deserialize)]
#[result("()")]
pub enum TestEvent {
    Started(String),
    Tested(i32),
    Stopped,
}

#[derive(Debug, PartialEq, Eq, JsonSnapshot, Serialize, Deserialize)]
pub struct TestAggregateSnapshot {
    pub state: TestState,
}

#[derive(Debug, Default, Clone, Label, PartialEq, Eq)]
pub struct TestAggregate {
    state: TestState,
    nr_events: i64,
    snapshot_after_nr_events: Option<i64>,
}

impl TestAggregate {
    #[allow(clippy::missing_const_for_fn)]
    pub fn with_snapshots(self, snapshot_after: i64) -> Self {
        Self {
            snapshot_after_nr_events: Some(snapshot_after),
            ..self
        }
    }
}

impl fmt::Display for TestAggregate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.state {
            TestState::Quiescent(_) => write!(f, "quiescent"),
            TestState::Active(state) => write!(f, "active:{}", state.tests.iter().sum::<i32>()),
            TestState::Completed(state) => {
                write!(f, "completed:{}", state.tests.iter().sum::<i32>())
            }
        }
    }
}

impl Entity for TestAggregate {
    type IdGen = CuidGenerator;
}

impl Summarizable for TestAggregate {
    type Summary = TestState;

    fn summarize(&self, _ctx: &ActorContext) -> Self::Summary {
        self.state.clone()
    }
}

#[async_trait]
impl PersistentActor for TestAggregate {
    #[instrument(level = "info", skip(journal))]
    fn configure(journal: &mut JournalTypes<Self>) {
        journal
            .snapshot::<TestAggregateSnapshot>("tests-aggregate-snapshot")
            .message::<TestEvent>("tests-aggregate-event");
    }
}

#[async_trait]
impl Handler<TestCommand> for TestAggregate {
    #[instrument(level = "debug", skip(ctx))]
    async fn handle(
        &mut self,
        command: TestCommand,
        ctx: &mut ActorContext,
    ) -> CommandResult<String> {
        let events = match self.state.handle_command(command, ctx) {
            Ok(events) => events,
            Err(AggregateError::RejectedCommand(msg)) => return CommandResult::Rejected(msg),
            Err(err) => return err.into(),
        };

        debug!("[{}] RESULTING EVENTS: {events:?}", ctx.id());
        for event in events.into_iter() {
            debug!("[{}] PERSISTING event: {event:?}", ctx.id());
            if let Err(error) = self.persist(&event, ctx).await {
                error!(?event, "[{}] failed to persist event: {error:?}", ctx.id());
                return error.into();
            }

            debug!("[{}] APPLYING event: {event:?}", ctx.id());
            if let Some(new_state) = self.state.apply_event(event, ctx) {
                self.state = new_state;
            }

            self.nr_events += 1;
            if let Some(snapshot_after_nr_events) = self.snapshot_after_nr_events {
                if self.nr_events % snapshot_after_nr_events == 0 {
                    let snapshot = TestAggregateSnapshot {
                        state: self.state.clone(),
                    };
                    debug!(
                        ?snapshot,
                        "[{}] TAKING A SNAPSHOT after {} events...",
                        ctx.id(),
                        self.nr_events
                    );
                    if let Err(error) = self.snapshot(snapshot, ctx).await {
                        error!(
                            %snapshot_after_nr_events,
                            "[{}] failed to snapshot event at sequence: {}",
                            ctx.id(), self.nr_events
                        );
                        return error.into();
                    }
                }
            }
        }

        // perform 0.. side effect tasks

        // determine result corresponding to command handling
        CommandResult::ok(self.to_string())
    }
}

#[async_trait]
impl Handler<Summarize<Self>> for TestAggregate {
    #[instrument(level = "debug", skip(_message, ctx))]
    async fn handle(
        &mut self,
        _message: Summarize<Self>,
        ctx: &mut ActorContext,
    ) -> <Summarize<Self> as Message>::Result {
        self.summarize(ctx)
    }
}

// #[async_trait]
// impl<A: PersistentActor + Summarizable> Handler<Summarize<Self>> for A {
//     #[instrument(level = "debug", skip(_msg, ctx))]
//     async fn handle(&mut self, _msg: Summarize<A>, ctx: &mut ActorContext) -> <Summarize<A> as Message>::Result {
//         self.summarize(ctx)
//     }
// }

#[async_trait]
impl Handler<ProvokeError> for TestAggregate {
    #[instrument(level = "debug", skip(_message, _ctx))]
    async fn handle(
        &mut self,
        _message: ProvokeError,
        _ctx: &mut ActorContext,
    ) -> <ProvokeError as Message>::Result {
        Err(ActorRefErr::Timeout {
            time_taken_millis: 1234,
        })
    }
}

impl ApplyAggregateEvent<TestEvent> for TestAggregate {
    type BaseType = Self;

    fn apply_event(&mut self, event: TestEvent, ctx: &mut ActorContext) -> Option<Self::BaseType> {
        if let Some(new_state) = self.state.apply_event(event, ctx) {
            self.state = new_state;
        }
        None
    }
}

#[async_trait]
impl Recover<TestEvent> for TestAggregate {
    #[instrument(level = "debug", skip(ctx))]
    async fn recover(&mut self, event: TestEvent, ctx: &mut ActorContext) {
        info!("[{}] RECOVERING from EVENT: {event:?}", ctx.id());
        if let Some(new_type) = self.apply_event(event, ctx) {
            *self = new_type;
        }
    }
}

#[async_trait]
impl RecoverSnapshot<TestAggregateSnapshot> for TestAggregate {
    #[instrument(level = "debug", skip(ctx))]
    async fn recover(&mut self, snapshot: TestAggregateSnapshot, ctx: &mut ActorContext) {
        info!("[{}] RECOVERING from SNAPSHOT: {snapshot:?}", ctx.id());
        self.state = snapshot.state;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TestState {
    Quiescent(QuiescentState),
    Active(ActiveState),
    Completed(CompletedState),
}

impl TestState {
    pub const fn quiescent() -> Self {
        Self::Quiescent(QuiescentState)
    }

    pub fn active(description: impl Into<String>, tests: Vec<i32>) -> Self {
        Self::Active(ActiveState {
            description: description.into(),
            tests,
        })
    }

    pub fn completed(description: impl Into<String>, tests: Vec<i32>) -> Self {
        Self::Completed(CompletedState {
            description: description.into(),
            tests,
        })
    }

    pub fn summarize(&self, _ctx: &ActorContext) -> i32 {
        match self {
            Self::Quiescent(_) => 0,
            Self::Active(state) => state.tests.iter().sum(),
            Self::Completed(state) => state.tests.iter().sum(),
        }
    }
}

impl AggregateState<TestCommand, TestEvent> for TestState {
    type Error = AggregateError;
    type State = Self;

    fn handle_command(
        &self,
        command: TestCommand,
        ctx: &mut ActorContext,
    ) -> Result<Vec<TestEvent>, Self::Error> {
        match self {
            Self::Quiescent(state) => state.handle_command(command, ctx),
            Self::Active(state) => state.handle_command(command, ctx),
            Self::Completed(state) => state.handle_command(command, ctx),
        }
    }

    fn apply_event(&mut self, event: TestEvent, ctx: &mut ActorContext) -> Option<Self::State> {
        match self {
            Self::Quiescent(state) => state.apply_event(event, ctx),
            Self::Active(state) => state.apply_event(event, ctx),
            Self::Completed(state) => state.apply_event(event, ctx),
        }
    }
}

impl Default for TestState {
    fn default() -> Self {
        Self::Quiescent(QuiescentState::default())
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuiescentState;

impl AggregateState<TestCommand, TestEvent> for QuiescentState {
    type Error = AggregateError;
    type State = TestState;

    #[instrument(level = "debug", skip(_ctx))]
    fn handle_command(
        &self,
        command: TestCommand,
        _ctx: &mut ActorContext,
    ) -> Result<Vec<TestEvent>, Self::Error> {
        match command {
            TestCommand::Start(description) => Ok(vec![TestEvent::Started(description)]),
            cmd => Err(AggregateError::RejectedCommand(format!(
                "TestAggregate must be started before handling command: {cmd:?}"
            ))),
        }
    }

    #[instrument(level = "debug", skip(_ctx))]
    fn apply_event(&mut self, event: TestEvent, _ctx: &mut ActorContext) -> Option<Self::State> {
        match event {
            TestEvent::Started(description) => {
                Some(TestState::Active(ActiveState::new(description)))
            }
            event => {
                warn!(?event, "unrecognized event while quiescent - ignored");
                None
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveState {
    pub description: String,
    pub tests: Vec<i32>,
}

impl ActiveState {
    pub const fn new(description: String) -> Self {
        Self {
            description,
            tests: vec![],
        }
    }
}

impl AggregateState<TestCommand, TestEvent> for ActiveState {
    type Error = AggregateError;
    type State = TestState;

    fn handle_command(
        &self,
        command: TestCommand,
        _ctx: &mut ActorContext,
    ) -> Result<Vec<TestEvent>, Self::Error> {
        match command {
            TestCommand::Test(value) => Ok(vec![TestEvent::Tested(value)]),
            TestCommand::Stop => Ok(vec![TestEvent::Stopped]),
            TestCommand::Start(_) => Err(AggregateError::RejectedCommand(
                "Active TestAggregate cannot be restarted.".to_string(),
            )),
        }
    }

    #[instrument(level = "debug", skip(_ctx))]
    fn apply_event(&mut self, event: TestEvent, _ctx: &mut ActorContext) -> Option<Self::State> {
        match event {
            TestEvent::Tested(value) => {
                self.tests.push(value);
                None
            }
            TestEvent::Stopped => Some(TestState::Completed(self.clone().into())),
            TestEvent::Started(_) => {
                warn!(?event, "unrecognized started event while active - ignored");
                None
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedState {
    pub description: String,
    pub tests: Vec<i32>,
}

impl From<ActiveState> for CompletedState {
    fn from(state: ActiveState) -> Self {
        Self {
            description: state.description,
            tests: state.tests,
        }
    }
}

impl AggregateState<TestCommand, TestEvent> for CompletedState {
    type Error = AggregateError;
    type State = TestState;

    #[instrument(level = "debug", skip(_ctx))]
    fn handle_command(
        &self,
        command: TestCommand,
        _ctx: &mut ActorContext,
    ) -> Result<Vec<TestEvent>, Self::Error> {
        Err(AggregateError::RejectedCommand(format!(
            "Completed TestAggregate does not accept further comments: {command:?}"
        )))
    }

    #[instrument(level = "debug", skip(_ctx))]
    fn apply_event(&mut self, event: TestEvent, _ctx: &mut ActorContext) -> Option<Self::State> {
        warn!(
            ?event,
            "completed TestAggregate does not recognize further events - ignored"
        );
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::TestFramework;
    use tokio_test::block_on;

    const DESCRIPTION: &str = "test starts now!";

    #[test]
    fn test_aggregate_start() {
        block_on(async {
            let validator = TestFramework::<TestAggregate, _>::default()
                .with_memory_storage()
                .given_no_previous_events()
                .await
                .when(TestCommand::Start(DESCRIPTION.to_string()))
                .await;

            validator.then_expect_reply(CommandResult::Ok("active:0".to_string()));
            validator.then_expect_state_summary(TestState::Active(ActiveState::new(
                DESCRIPTION.to_string(),
            )));
        })
    }

    #[test]
    fn test_aggregate_zero_stop() {
        block_on(async {
            TestFramework::<TestAggregate, _>::default()
                .with_memory_storage()
                .given_no_previous_events()
                .await
                .when(TestCommand::Stop)
                .await
                .then_expect_reply(CommandResult::Rejected(
                    "TestAggregate must be started before handling command: Stop".to_string(),
                ))
                .then_expect_state_summary(TestState::Quiescent(QuiescentState));
        });
    }

    #[test]
    fn test_aggregate_zero_test() {
        block_on(async {
            TestFramework::<TestAggregate, _>::default()
                .with_memory_storage()
                .given_no_previous_events()
                .await
                .when(TestCommand::Test(33))
                .await
                .then_expect_reply(CommandResult::Rejected(
                    "TestAggregate must be started before handling command: Test(33)".to_string(),
                ))
                .then_expect_state_summary(TestState::Quiescent(QuiescentState));
        });
    }

    #[test]
    fn test_aggregate_happy_life() {
        block_on(async {
            TestFramework::<TestAggregate, _>::default()
                .with_memory_storage()
                .given(
                    "tests-aggregate-event",
                    vec![
                        TestEvent::Started(DESCRIPTION.to_string()),
                        TestEvent::Tested(1),
                        TestEvent::Tested(2),
                        TestEvent::Tested(3),
                        TestEvent::Tested(5),
                    ],
                )
                .await
                .when(TestCommand::Stop)
                .await
                .then_expect_reply(CommandResult::Ok("completed:11".to_string()))
                .then_expect_state_summary(TestState::Completed(CompletedState {
                    description: DESCRIPTION.to_string(),
                    tests: vec![1, 2, 3, 5],
                }));
        });
    }

    #[test]
    fn test_aggregate_happy_summary() {
        block_on(async {
            TestFramework::<TestAggregate, _>::default()
                .with_memory_storage()
                .given(
                    "tests-aggregate-event",
                    vec![
                        TestEvent::Started(DESCRIPTION.to_string()),
                        TestEvent::Tested(1),
                        TestEvent::Tested(2),
                        TestEvent::Tested(3),
                        TestEvent::Tested(5),
                    ],
                )
                .await
                .when(Summarize::<TestAggregate>::default())
                .await
                .then_expect_reply(TestState::Active(ActiveState {
                    description: DESCRIPTION.to_string(),
                    tests: vec![1, 2, 3, 5],
                }));
        });
    }

    #[test]
    fn test_aggregate_happy_error() {
        block_on(async {
            TestFramework::<TestAggregate, _>::default()
                .with_memory_storage()
                .given(
                    "tests-aggregate-event",
                    vec![
                        TestEvent::Started(DESCRIPTION.to_string()),
                        TestEvent::Tested(1),
                        TestEvent::Tested(2),
                        TestEvent::Tested(3),
                        TestEvent::Tested(5),
                    ],
                )
                .await
                .when(ProvokeError)
                .await
                .then_expect_reply(Err(ActorRefErr::Timeout {
                    time_taken_millis: 1234,
                }));
        });
    }
}
