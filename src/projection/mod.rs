mod event_envelope;
mod materialized;

pub use event_envelope::{EventEnvelope, Offset};
pub use materialized::{
    ApplyViewEvents, GenericProcessor, ProcessorErrorHandler, View, ViewContext,
    ViewRepository,
};

#[async_trait]
pub trait EventProcessor<E> {
    type Error;
    async fn process(&self, event: EventEnvelope<E>) -> Result<(), Self::Error>;
}

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProjectionError {
    #[error("{0}")]
    Sql(#[from] sqlx::Error),

    #[error("{0}")]
    Json(#[from] serde_json::Error),
}
