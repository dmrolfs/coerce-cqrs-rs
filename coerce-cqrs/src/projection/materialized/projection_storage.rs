use crate::projection::{Offset, PersistenceId, ProjectionError};
use std::collections::HashMap;

pub type AggregateOffsets = HashMap<PersistenceId, Offset>;

#[async_trait]
pub trait ProjectionStorage {
    type ViewId;
    type Projection: Default;

    fn name(&self) -> &str;

    /// returns the current projection instance
    async fn load_projection(
        &self,
        view_id: &Self::ViewId,
    ) -> Result<Option<Self::Projection>, ProjectionError>;

    /// saves the projection instance for the context, used by the `GenericProjectionProcessor` to
    /// record projections updated by committed events.
    async fn save_projection(
        &self,
        view_id: &Self::ViewId,
        projection: Option<Self::Projection>,
        offset: Offset,
    ) -> Result<(), ProjectionError>;

    /// Returns all of the offsets seen by the processor. When the processor pull the next batch of,
    /// messages it must take care to include new aggregates not yet seen.
    async fn read_all_offsets(
        &self,
        projection_name: &str,
    ) -> Result<AggregateOffsets, ProjectionError>;

    /// Returns the sequence number from which to start the next processor pull.
    async fn read_offset(
        &self,
        projection_name: &str,
        persistence_id: &PersistenceId,
    ) -> Result<Option<Offset>, ProjectionError>;
}
