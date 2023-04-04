use crate::projection::{PersistenceId, ProjectionError, ProjectionId};
use iso8601_timestamp::Timestamp;
use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

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

pub type OffsetStorageRef = Arc<dyn OffsetStorage>;

#[async_trait]
pub trait OffsetStorage {
    async fn read_offset(
        &self,
        projection_id: &ProjectionId,
        persistence_id: &PersistenceId,
    ) -> Result<Option<Offset>, ProjectionError>;
    async fn save_offset(
        &self,
        projection_id: &ProjectionId,
        peristence_id: &PersistenceId,
        offset: Offset,
    ) -> Result<(), ProjectionError>;
}

#[derive(Debug, Default)]
pub struct InMemoryOffsetStorage {
    inner: Arc<dashmap::DashMap<(ProjectionId, PersistenceId), Offset>>,
}

#[async_trait]
impl OffsetStorage for InMemoryOffsetStorage {
    async fn read_offset(
        &self,
        projection_id: &ProjectionId,
        persistence_id: &PersistenceId,
    ) -> Result<Option<Offset>, ProjectionError> {
        let key = (projection_id.clone(), persistence_id.clone());
        Ok(self.inner.get(&key).map(|offset| *offset.value()))
    }

    async fn save_offset(
        &self,
        projection_id: &ProjectionId,
        persistence_id: &PersistenceId,
        offset: Offset,
    ) -> Result<(), ProjectionError> {
        let key = (projection_id.clone(), persistence_id.clone());
        let _prior_offset = self.inner.insert(key, offset);
        Ok(())
    }
}
