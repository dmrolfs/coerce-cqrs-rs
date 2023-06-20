use iso8601_timestamp::Timestamp;
use std::cmp::Ordering;
use std::fmt;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct Offset(Timestamp, i64);

impl Offset {
    pub fn new(seen: i64) -> Self {
        Self::from_parts(Timestamp::now_utc(), seen)
    }

    pub const fn from_parts(timestamp: Timestamp, seen: i64) -> Self {
        Self(timestamp, seen)
    }

    pub const fn as_i64(&self) -> i64 {
        self.1
    }

    pub const fn timestamp(&self) -> Timestamp {
        self.0
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn into_parts(self) -> (Timestamp, i64) {
        (self.0, self.1)
    }
}

impl Default for Offset {
    fn default() -> Self {
        Self::new(0)
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

// pub type OffsetStorageRef = Arc<dyn OffsetStorage>;
//
// pub type AggregateOffsets = HashMap<PersistenceId, Offset>;
//
// #[async_trait]
// pub trait OffsetStorage {
//     /// Returns all of the offsets seen by the processor. When the processor pull the next batch of,
//     /// messages it must take care to include new aggregates not yet seen.
//     async fn read_all_offsets(
//         &self,
//         projection_name: &str,
//     ) -> Result<AggregateOffsets, ProjectionError>;
//
//     /// Returns the sequence number from which to start the next processor pull.
//     async fn read_offset(
//         &self,
//         projection_name: &str,
//         persistence_id: &PersistenceId,
//     ) -> Result<Option<Offset>, ProjectionError>;
//
//     /// Saves the sequence number from which to start the next processor pull.
//     async fn save_offset(
//         &self,
//         projection_name: &str,
//         persistence_id: &PersistenceId,
//         offset: Offset,
//     ) -> Result<(), ProjectionError>;
// }
//
// #[derive(Debug, Default)]
// pub struct InMemoryOffsetStorage {
//     inner: Arc<dashmap::DashMap<String, dashmap::DashMap<PersistenceId, Offset>>>,
// }
//
// #[async_trait]
// impl OffsetStorage for InMemoryOffsetStorage {
//     #[instrument(level = "debug")]
//     async fn read_all_offsets(
//         &self,
//         projection_name: &str,
//     ) -> Result<AggregateOffsets, ProjectionError> {
//         let result = self
//             .inner
//             .get(projection_name)
//             .map(|agg_offsets| agg_offsets.value().clone())
//             .map(|offsets| offsets.into_iter().collect())
//             .unwrap_or_default();
//
//         Ok(result)
//     }
//
//     #[instrument(level = "debug")]
//     async fn read_offset(
//         &self,
//         projection_name: &str,
//         persistence_id: &PersistenceId,
//     ) -> Result<Option<Offset>, ProjectionError> {
//         let result = self.inner.get(projection_name).and_then(|offsets| {
//             offsets
//                 .get(persistence_id)
//                 .map(|offset_ref| *offset_ref.value())
//         });
//
//         debug!("read_offset [{projection_name}::{persistence_id}] = {result:?}");
//         Ok(result)
//
//         // let key = (projection_id.clone(), persistence_id.clone());
//         // let key_rep = format!("({}-{:#})", key.0, key.1);
//         // let offset = Ok(self.inner.get(&key).map(|offset| *offset.value()));
//         // debug!("read_offset [{key_rep}] = {offset:?}");
//         // offset
//     }
//
//     #[instrument(level = "debug")]
//     async fn save_offset(
//         &self,
//         projection_name: &str,
//         persistence_id: &PersistenceId,
//         offset: Offset,
//     ) -> Result<(), ProjectionError> {
//         let prior_offset = self
//             .inner
//             .get(projection_name)
//             .and_then(|offsets| offsets.insert(persistence_id.clone(), offset));
//
//         debug!(
//             "save_offset [{projection_name}::{persistence_id}] = {offset:?} was {prior_offset:?}"
//         );
//         Ok(())
//
//         // let key = (projection_id.clone(), persistence_id.clone());
//         // let key_rep = format!("({}-{:#})", key.0, key.1);
//         // let prior_offset = self.inner.insert(key, offset);
//         // debug!("save_offset [{key_rep}] = {offset:?} was {prior_offset:?}");
//         // Ok(())
//     }
// }
