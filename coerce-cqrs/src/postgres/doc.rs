use crate::doc::MyEvent;

// pub type MyViewRepository = In
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MyView {
    pub count: usize,
    pub lots_done: bool,
}

pub const fn apply_event_on_my_view(mut view: MyView, event: MyEvent) -> MyView {
    match event {
        MyEvent::SomethingWasDone => {
            view.count += 1;
            if !view.lots_done && 2 < view.count {
                view.lots_done = true;
            }
        }
    }

    view
}
