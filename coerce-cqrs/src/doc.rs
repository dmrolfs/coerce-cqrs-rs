use crate::{AggregateState, CommandResult};
use coerce::actor::context::ActorContext;
use coerce::actor::message::Handler;
use coerce::persistent::journal::types::JournalTypes;
use coerce::persistent::{PersistentActor, Recover};
use tagid::{CuidGenerator, Entity, Label};

#[derive(Debug, JsonMessage, Serialize, Deserialize)]
#[result("CommandResult<usize, String>")]
pub enum MyCommand {
    DoSomething,
    BadCommand,
}

#[derive(Debug, Clone, PartialEq, Eq, JsonMessage, Serialize, Deserialize)]
#[result("()")]
pub enum MyEvent {
    SomethingWasDone,
}

#[derive(Debug, Label, Serialize, Deserialize)]
pub struct MyAggregate {
    count: usize,
}

impl Entity for MyAggregate {
    type IdGen = CuidGenerator;
}

#[async_trait]
impl PersistentActor for MyAggregate {
    fn configure(journal: &mut JournalTypes<Self>) {
        journal.message::<MyEvent>("my-aggregate-event");
    }
}

#[async_trait]
impl Handler<MyCommand> for MyAggregate {
    async fn handle(
        &mut self,
        command: MyCommand,
        ctx: &mut ActorContext,
    ) -> CommandResult<usize, String> {
        let events = match self.handle_command(&command) {
            CommandResult::Ok(evts) => evts,
            CommandResult::Rejected(msg) => return CommandResult::Rejected(msg),
            CommandResult::Err(()) => {
                return CommandResult::Err("failed to handle command".to_string())
            }
        };

        for event in events.iter() {
            if let Err(error) = self.persist(event, ctx).await {
                return error.to_string().into();
            }
        }

        events.into_iter().for_each(|event| {
            if let Some(new_agg) = self.apply_event(event) {
                *self = new_agg;
            }
        });

        self.then_run(&command, ctx);

        CommandResult::Ok(self.count)
    }
}

impl AggregateState<MyCommand, MyEvent> for MyAggregate {
    type Error = ();
    type State = Self;

    fn handle_command(&self, command: &MyCommand) -> CommandResult<Vec<MyEvent>, Self::Error> {
        use MyCommand as C;

        match command {
            C::DoSomething => CommandResult::Ok(vec![MyEvent::SomethingWasDone]),
            C::BadCommand => CommandResult::Rejected("BadCommand".to_string()),
        }
    }

    fn apply_event(&mut self, event: MyEvent) -> Option<Self::State> {
        match event {
            MyEvent::SomethingWasDone => {
                self.count += 1;
            }
        }

        None
    }
}

#[async_trait]
impl Recover<MyEvent> for MyAggregate {
    async fn recover(&mut self, event: MyEvent, _ctx: &mut ActorContext) {
        if let Some(new_agg) = self.apply_event(event) {
            *self = new_agg;
        }
    }
}
