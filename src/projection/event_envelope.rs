use iso8601_timestamp::Timestamp;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct Offset(Timestamp, u64);

impl Offset {
    pub const fn new(timestamp: Timestamp, seen: u64) -> Self {
        Self(timestamp, seen)
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.1, self.0)
    }
}

impl Ord for Offset {
    fn cmp(&self, other: &Self) -> Ordering {
        let result = self.1.cmp(&other.1);
        if result != Ordering::Equal {
            result
        } else {
            self.0.cmp(&other.0)
        }
    }
}
impl PartialOrd for Offset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone)]
pub struct EventEnvelope<E> {
    pub offset: Offset,
    pub persistence_id: String,
    pub sequence_nr: u64,
    pub event: E,
    pub timestamp: Timestamp,
    pub metadata: HashMap<String, String>,
}

impl<E: fmt::Debug> fmt::Debug for EventEnvelope<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventEnvelope")
            .field("offset", &self.offset)
            .field("persistence_id", &self.persistence_id)
            .field("sequence_nr", &self.sequence_nr)
            .field("timestamp", &self.timestamp)
            .field("event", &self.event)
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl<E: PartialEq> PartialEq for EventEnvelope<E> {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
            && self.persistence_id == other.persistence_id
            && self.sequence_nr == other.sequence_nr
            && self.timestamp == other.timestamp
            && self.event == other.event
            && self.metadata == other.metadata
    }
}

impl<E: Eq> Eq for EventEnvelope<E> {}

impl<E: Eq + Hash> Hash for EventEnvelope<E> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.offset.hash(state);
        self.persistence_id.hash(state);
        self.sequence_nr.hash(state);
        self.timestamp.hash(state);
        self.event.hash(state);
    }
}
