use std::fmt;
use crate::aggregate::{AggregateError, AggregateState, ApplyAggregateEvent};
use crate::projection::ApplyViewEvents;
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::persistent::journal::types::JournalTypes;
use coerce::persistent::{PersistentActor, Recover, RecoverSnapshot};
use tagid::{CuidGenerator, CuidId, Entity, Id, Label};
use crate::aggregate::CommandResult;

#[derive(Debug, Clone, PartialEq, JsonMessage, Serialize, Deserialize)]
#[result("CommandResult<String>")]
pub enum TestCommand {
    Start(String),
    Test(i32),
    Stop,
}

#[derive(Debug, Clone, PartialEq, JsonMessage, Serialize, Deserialize)]
#[result("i32")]
pub struct Summarize;

#[derive(Debug, Clone, PartialEq, JsonMessage, Serialize, Deserialize)]
#[result("()")]
pub enum TestEvent {
    Started(String),
    Tested(i32),
    Stopped,
}

#[derive(Debug, PartialEq, JsonSnapshot, Serialize, Deserialize)]
pub(crate) struct TestAggregateSnapshot {
    pub state: TestState,
}

#[derive(Debug, Default, Clone, Label, PartialEq)]
pub(crate) struct TestAggregate {
    state: TestState,
}

impl fmt::Display for TestAggregate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.state {
            TestState::Quiescent(_) => write!(f, "quiescent"),
            TestState::Active(state) => write!(f, "active:{}", state.tests.iter().sum::<i32>()),
            TestState::Completed(state) => write!(f, "completed:{}", state.tests.iter().sum::<i32>()),
        }
    }
}

impl Entity for TestAggregate {
    type IdGen = CuidGenerator;
}

#[async_trait]
impl PersistentActor for TestAggregate {
    #[instrument(level = "info", skip(journal))]
    fn configure(journal: &mut JournalTypes<Self>) {
        journal
            .snapshot::<TestAggregateSnapshot>("test-aggregate-snapshot")
            .message::<TestEvent>("test-aggregate-event");
    }
}

#[async_trait]
impl Handler<TestCommand> for TestAggregate {
    #[instrument(level = "debug", skip(ctx))]
    async fn handle(&mut self, command: TestCommand, ctx: &mut ActorContext) -> CommandResult<String> {
        let events = match self.state.handle_command(command, ctx) {
            Ok(events) => events,
            Err(AggregateError::RejectedCommand(msg)) => return CommandResult::Rejected(msg),
            Err(err) => return err.into(),
        };

        for event in events.iter() {
            if let Err(error) = self.persist(event, ctx).await {
                return error.into();
            }
        }

        // perform 0.. side effect tasks

        events.into_iter().for_each(|event| {
            if let Some(new_state) = self.state.apply_event(event, ctx) {
                self.state = new_state;
            }
        });

        // determine result corresponding to command handling
        CommandResult::ok(self.to_string())
    }
}

#[async_trait]
impl Handler<Summarize> for TestAggregate {
    #[instrument(level = "debug", skip(_message, ctx))]
    async fn handle(
        &mut self,
        _message: Summarize,
        ctx: &mut ActorContext,
    ) -> <Summarize as Message>::Result {
        self.state.summarize(ctx)
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
        if let Some(new_type) = self.apply_event(event, ctx) {
            *self = new_type;
        }
    }
}

#[async_trait]
impl RecoverSnapshot<TestAggregateSnapshot> for TestAggregate {
    #[instrument(level = "debug", skip(_ctx))]
    async fn recover(&mut self, snapshot: TestAggregateSnapshot, _ctx: &mut ActorContext) {
        self.state = snapshot.state;
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum TestState {
    Quiescent(QuiescentState),
    Active(ActiveState),
    Completed(CompletedState),
}

impl TestState {
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

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct QuiescentState;

impl AggregateState<TestCommand, TestEvent> for QuiescentState {
    type Error = AggregateError;
    type State = TestState;

    #[instrument(level="debug", skip(_ctx))]
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

    #[instrument(level = "debug", skip(ctx))]
    fn apply_event(&mut self, event: TestEvent, ctx: &mut ActorContext) -> Option<Self::State> {
        match event {
            TestEvent::Started(description) => Some(TestState::Active(ActiveState::new(description))),
            event => {
                warn!(?event, "unrecognized event while quiescent - ignored");
                None
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ActiveState {
    pub description: String,
    pub tests: Vec<i32>,
}

impl ActiveState {
    pub fn new(description: String) -> Self {
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
        ctx: &mut ActorContext,
    ) -> Result<Vec<TestEvent>, Self::Error> {
        match command {
            TestCommand::Test(value) => Ok(vec![TestEvent::Tested(value)]),
            TestCommand::Stop => Ok(vec![TestEvent::Stopped]),
            TestCommand::Start(_) => Err(AggregateError::RejectedCommand(
                "Active TestAggregate cannot be restarted.".to_string(),
            )),
        }
    }

    #[instrument(level = "debug", skip(ctx))]
    fn apply_event(&mut self, event: TestEvent, ctx: &mut ActorContext) -> Option<Self::State> {
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CompletedState {
    pub description: String,
    pub tests: Vec<i32>,
}

impl From<ActiveState> for CompletedState {
    fn from(state: ActiveState) -> Self {
        CompletedState {
            description: state.description,
            tests: state.tests,
        }
    }
}

impl AggregateState<TestCommand, TestEvent> for CompletedState {
    type Error = AggregateError;
    type State = TestState;

    #[instrument(level = "debug", skip(ctx))]
    fn handle_command(
        &self,
        command: TestCommand,
        ctx: &mut ActorContext,
    ) -> Result<Vec<TestEvent>, Self::Error> {
        Err(AggregateError::RejectedCommand(format!(
            "Completed TestAggregate does not accept further comments: {command:?}"
        )))
    }

    #[instrument(level = "debug", skip(ctx))]
    fn apply_event(&mut self, event: TestEvent, ctx: &mut ActorContext) -> Option<Self::State> {
        warn!(
            ?event,
            "completed TestAggregate does not recognize further events - ignored"
        );
        None
    }
}
