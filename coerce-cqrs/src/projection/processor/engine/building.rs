use crate::projection::materialized::ProjectionStorageRef;
use crate::projection::processor::engine::ready::Ready;
use crate::projection::processor::{
    CalculateInterval, ProcessEntry, ProcessorEngine, ProcessorError, ProcessorLifecycle,
    ProcessorSourceRef,
};
use coerce::actor::system::ActorSystem;
use std::fmt::{self, Debug};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct Building<VID, P, H, I>
where
    H: ProcessEntry<Projection = P>,
    I: CalculateInterval,
{
    projection_name: String,
    entry_handler: Option<Arc<H>>,
    system: Option<ActorSystem>,
    source: Option<ProcessorSourceRef>,
    projection_source: Option<ProjectionStorageRef<VID, P>>,
    interval_calculator: Option<I>,
}

impl<VID, P, H, I> Debug for Building<VID, P, H, I>
where
    H: ProcessEntry<Projection = P> + Debug,
    I: CalculateInterval + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Building")
            .field("projection_name", &self.projection_name)
            .field("entry_handler", &self.entry_handler)
            .field("system", &self.system.as_ref().map(|s| s.system_id()))
            .field("interval_calculator", &self.interval_calculator)
            .finish()
    }
}

impl<VID, P, H, I> ProcessorLifecycle for Building<VID, P, H, I>
where
    H: ProcessEntry<Projection = P>,
    I: CalculateInterval,
{
}

impl<VID, P, H, I> ProcessorEngine<Building<VID, P, H, I>>
where
    H: ProcessEntry<Projection = P>,
    I: CalculateInterval,
{
    pub fn new(projection_name: impl Into<String>) -> Self {
        Self {
            inner: Building {
                projection_name: projection_name.into(),
                entry_handler: None,
                system: None,
                source: None,
                projection_source: None,
                interval_calculator: None,
            },
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_entry_handler(self, entry_handler: H) -> Self {
        Self {
            inner: Building {
                entry_handler: Some(Arc::new(entry_handler)),
                ..self.inner
            },
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_system(self, system: ActorSystem) -> Self {
        Self {
            inner: Building {
                system: Some(system),
                ..self.inner
            },
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_source(self, source: ProcessorSourceRef) -> Self {
        Self {
            inner: Building {
                source: Some(source),
                ..self.inner
            },
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_projection_source(self, projection_source: ProjectionStorageRef<VID, P>) -> Self {
        Self {
            inner: Building {
                projection_source: Some(projection_source),
                ..self.inner
            },
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_interval_calculator(self, interval_calculator: I) -> Self {
        Self {
            inner: Building {
                interval_calculator: Some(interval_calculator),
                ..self.inner
            },
        }
    }

    pub fn finish(self) -> Result<ProcessorEngine<Ready<VID, P, H, I>>, ProcessorError> {
        let (tx_api, rx_api) = mpsc::unbounded_channel();

        let entry_handler = self
            .inner
            .entry_handler
            .ok_or_else(|| ProcessorError::UninitializedField("entry_handler".to_string()))?;

        let system = self
            .inner
            .system
            .ok_or_else(|| ProcessorError::UninitializedField("system".to_string()))?;

        let source = self
            .inner
            .source
            .ok_or_else(|| ProcessorError::UninitializedField("source".to_string()))?;

        let projection_storage = self
            .inner
            .projection_source
            .ok_or_else(|| ProcessorError::UninitializedField("projection_source".to_string()))?;

        let interval_calculator = self
            .inner
            .interval_calculator
            .ok_or_else(|| ProcessorError::UninitializedField("interval_calculator".to_string()))?;

        Ok(ProcessorEngine {
            inner: Ready {
                projection_name: self.inner.projection_name,
                entry_handler,
                system,
                source,
                projection_storage,
                interval_calculator,
                tx_api,
                rx_api,
            },
        })
    }
}
