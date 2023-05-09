use crate::projection::{PersistenceId, ProjectionError, ProjectionId};

#[async_trait]
pub trait ViewStorage {
    type View: Default;

    fn view_name(&self) -> &str;

    fn view_id_from_persistence(&self, pid: &PersistenceId) -> ProjectionId {
        ProjectionId::new(pid.id.as_str())
    }

    /// returns the current view instance
    async fn load_view(&self, view_id: &ProjectionId) -> Result<Option<Self::View>, ProjectionError>;

    /// saves the view instance for the context, used by the `GenericViewProcessor` to record
    /// views updated by committed events.
    async fn save_view(&self, view_id: &ProjectionId, view: Self::View) -> Result<(), ProjectionError>;
}
