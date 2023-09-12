#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotTrigger {
    None,
    OnEventCount {
        nr_events: u64,
        after_nr_events: u64,
    },
}

impl SnapshotTrigger {
    pub const fn none() -> Self {
        Self::None
    }

    pub const fn on_event_count(after_nr_events: u64) -> Self {
        Self::OnEventCount {
            nr_events: 0,
            after_nr_events,
        }
    }

    /// Increments the event count for the snapshot trigger and returns true
    /// if a snapshot should be taken.
    pub fn incr(&mut self) -> bool {
        match self {
            Self::None => false,
            Self::OnEventCount {
                ref mut nr_events,
                after_nr_events,
            } => {
                *nr_events += 1;
                *nr_events % *after_nr_events == 0
            }
        }
    }
}
