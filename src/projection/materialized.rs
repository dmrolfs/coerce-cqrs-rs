use super::ProjectionError;
use crate::projection::{EventEnvelope, EventProcessor};
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use coerce::persistent::journal::storage::JournalEntry;

#[async_trait]
pub trait ViewRepository {
    type View: View;

    /// returns the current view instance
    async fn load(&self, view_id: &str) -> Result<Option<Self::View>, ProjectionError>;

    /// returns the current view instance and context, used by the GenericProcessor to update views
    /// with committed events.
    async fn load_with_context(
        &self,
        view_id: &str,
    ) -> Result<Option<(Self::View, ViewContext)>, ProjectionError>;

    /// updates the view instance and context, used by the GenericProcessor to update views with
    /// committed events.
    async fn update_view(
        &self,
        view: Self::View,
        context: ViewContext,
    ) -> Result<(), ProjectionError>;
}

/// Context updating views
#[derive(Debug, Clone)]
pub struct ViewContext {
    /// unique identifier of the view instance that is being modified.
    pub view_instance_id: String,

    /// current version of the view instance, used for optimistic locking.
    pub version: i64,
}

impl ViewContext {
    pub fn new(view_id: impl Into<String>, version: i64) -> Self {
        Self {
            view_instance_id: view_id.into(),
            version,
        }
    }
}

/// A View represents a materialized view, generally serialized for persistence, that is updated by
/// an EventProcessor. This is a read element in a CQRS system.
pub trait View {
    /// Each implemented view is responsible for updating its state based on journal entries passed
    /// into this method.
    fn apply_entry(&mut self, entry: JournalEntry);
}

#[async_trait]
pub trait ApplyViewEvents<R, E>
where
    R: ViewRepository + Sync,
    <R as ViewRepository>::View: View<E> + Default,
{
    async fn apply_events(
        &self,
        view_id: &str,
        events: &[EventEnvelope<E>],
    ) -> Result<(), ProjectionError>;

    fn view_repository(&self) -> &R;

    /// convenience method to load view as mutable.
    async fn load_mut(
        &self,
        view_id: &str,
    ) -> Result<(<R as ViewRepository>::View, ViewContext), ProjectionError> {
        let result = self
            .view_repository()
            .load_with_context(view_id)
            .await?
            .unwrap_or_else(|| {
                let view_context = ViewContext::new(view_id, 0);
                (Default::default(), view_context)
            });

        Ok(result)
    }
}

pub type ProcessorErrorHandler = dyn Fn(ProjectionError) + Send + Sync + 'static;

/// A simple combination of an EventProcessor and ViewRepository, providing a quick event processing
/// into a materialized view.
pub struct GenericProcessor<R, V, E>
where
    R: ViewRepository<View = V> + Send + Sync,
    V: View<E> + Send,
    E: Send + Sync,
{
    view_id: String,
    view_repository: Arc<R>,
    error_handler: Option<Box<ProcessorErrorHandler>>,
    _marker: PhantomData<E>,
}

impl<R, V, E> fmt::Debug for GenericProcessor<R, V, E>
where
    R: ViewRepository<View = V> + fmt::Debug + Send + Sync,
    V: View<E> + Send,
    E: Send + Sync,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenericProcessor")
            .field("view-id", &self.view_id)
            .field("view_repository", &self.view_repository)
            .field("has_error_handler", &self.error_handler.is_some())
            .finish()
    }
}

impl<R, V, E> GenericProcessor<R, V, E>
where
    R: ViewRepository<View = V> + Send + Sync,
    V: View<E> + Send,
    E: Send + Sync,
{
    pub fn new(view_id: impl Into<String>, view_repository: R) -> Self {
        Self {
            view_id: view_id.into(),
            view_repository: Arc::new(view_repository),
            error_handler: None,
            _marker: PhantomData,
        }
    }

    /// Allows the user to apply a custom error handler to the GenericProcessor.
    /// EventProcessors are infallible and _should_ never cause errors,
    /// but programming errors or other technical problems
    /// might. Adding an error handler allows the user to choose whether to
    /// panic the application, log the error or otherwise register the issue.
    ///
    /// Use of an error handler is *strongly* recommended since without one any error encountered
    /// by the query repository will simply be ignored.
    #[allow(dead_code)]
    pub fn with_error_handler(&mut self, error_handler: Box<ProcessorErrorHandler>) {
        self.error_handler = Some(error_handler);
    }

    /// Loads and deserializes a view based on the provided view id.
    /// Use this method to load a materialized view when requested by a user.
    ///
    /// ```
    /// # use coerce_cqrs::GenericProcessor;
    /// # async fn config(mut processor: GenericProcessor<MyViewRepository,MyView,MyAggregate>) {
    /// let view = processor.load("customer-B24DA0").await;
    /// # }
    /// ```
    pub async fn load(&self, view_id: &str) -> Option<V> {
        let repo = self.view_repository.as_ref();
        match repo.load_with_context(view_id).await {
            Ok(vc) => vc.map(|(view, _)| view),
            Err(error) => {
                self.handle_error(error);
                None
            }
        }
    }

    fn handle_error(&self, error: ProjectionError) {
        self.error_handler
            .as_ref()
            .map_or((), |handler| (handler)(error))
    }
}

#[async_trait]
impl<R, V, E> EventProcessor<E> for GenericProcessor<R, V, E>
where
    R: ViewRepository<View = V> + Send + Sync,
    V: View<E> + Default + Send,
    E: Send + Sync,
{
    type Error = ProjectionError;

    async fn process(&self, event: EventEnvelope<E>) -> Result<(), Self::Error> {
        match self.apply_events(&self.view_id, &[event]).await {
            Ok(_) => Ok(()),
            Err(error) => {
                self.handle_error(error);
                Ok(())
            }
        }
    }
}

#[async_trait]
impl<R, V, E> ApplyViewEvents<R, E> for GenericProcessor<R, V, E>
where
    R: ViewRepository<View = V> + Send + Sync,
    V: View<E> + Default + Send,
    E: Send + Sync,
{
    fn view_repository(&self) -> &R {
        &self.view_repository
    }

    async fn apply_events(
        &self,
        view_id: &str,
        events: &[EventEnvelope<E>],
    ) -> Result<(), ProjectionError> {
        let (mut view, view_context) = self.load_mut(view_id).await?;
        for event in events {
            view.update(event);
        }
        self.view_repository()
            .update_view(view, view_context)
            .await?;
        Ok(())
    }
}
