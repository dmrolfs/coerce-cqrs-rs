use crate::doc::{MyAggregate, MyEvent};
use crate::{EventEnvelope, View};

pub type MyViewRepository = In
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MvView {
    pub count: usize,
    pub lots_done: bool,
}

impl View<MyEvent> for MyView {
    fn update(&mut self, event_envelope: &EventEnvelope<MyEvent>) {
        match &event_envelope.event {
            MyEvent::SomethingWasDone => {
                self.count += 1;
                if !self.lots_done && 2 < self.count {
                    self.lots_done = true;
                }
            },
        }
    }
}
