use super::{ViewContext, ViewId};
use crate::projection::{PersistenceId, ProjectionError};

#[async_trait]
pub trait ViewStorage {
    type View: Default;

    fn view_name(&self) -> &str;

    fn version(&self) -> i64;

    fn view_id_from_persistence(&self, pid: &PersistenceId) -> ViewId {
        ViewId::new(pid.id.as_str())
    }

    fn context(&self) -> ViewContext {
        ViewContext::new(self.view_name(), self.version())
    }
    // fn view_context_from_processor(&self, p_ctx: &ProcessorContext) -> ViewContext {
    //     ViewContext::new(
    //         self.view_name(),
    //         self.view_id_from_persistence(&p_ctx.persistence_id),
    //         self.version()
    //     )
    // }

    /// returns the current view instance
    async fn load_view(&self, id: &ViewId) -> Result<Option<Self::View>, ProjectionError>;

    /// returns the current view instance and context, used by `GenericViewProcessor` to update
    /// views with committed events.
    async fn load_view_with_context(
        &self,
        id: &ViewId,
    ) -> Result<Option<(Self::View, ViewContext)>, ProjectionError>;

    /// saves the view instance for the context, used by the `GenericViewProcessor` to record
    /// views updated by committed events.
    async fn save_view(&self, view_id: ViewId, view: Self::View) -> Result<(), ProjectionError>;
}
