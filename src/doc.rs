use crate::{ApplyAggregateEvent, CommandResult};
use coerce::actor::context::ActorContext;
use coerce::actor::message::Handler;
use coerce::persistent::journal::types::JournalTypes;
use coerce::persistent::{PersistentActor, Recover};
use tagid::{CuidGenerator, Entity, Label};

#[derive(Debug, JsonMessage, Serialize, Deserialize)]
#[result("CommandResult<usize>")]
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
    async fn handle(&mut self, command: MyCommand, ctx: &mut ActorContext) -> CommandResult<usize> {
        let events = match command {
            MyCommand::DoSomething => vec![MyEvent::SomethingWasDone],
            MyCommand::BadCommand => return CommandResult::Rejected("BadCommand".to_string()),
        };

        for event in events.iter() {
            if let Err(error) = self.persist(event, ctx).await {
                return error.into();
            }
        }

        events.into_iter().for_each(|event| {
            if let Some(new_agg) = self.apply_event(event, ctx) {
                *self = new_agg;
            }
        });

        CommandResult::Ok(self.count)
    }
}

impl ApplyAggregateEvent<MyEvent> for MyAggregate {
    type BaseType = Self;

    fn apply_event(&mut self, event: MyEvent, _ctx: &mut ActorContext) -> Option<Self::BaseType> {
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
    async fn recover(&mut self, event: MyEvent, ctx: &mut ActorContext) {
        if let Some(new_agg) = self.apply_event(event, ctx) {
            *self = new_agg;
        }
    }
}
