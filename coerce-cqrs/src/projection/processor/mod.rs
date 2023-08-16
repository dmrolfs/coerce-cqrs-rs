mod interval;
#[allow(clippy::module_inception)]
mod processor;

pub use interval::{
    CalculateInterval, CalculateIntervalFactory, ExponentialBackoff, RegularInterval,
};
pub use processor::{
    protocol::{ProcessorApi, ProcessorCommand},
    AggregateEntries, AggregateOffsets, AggregateSequences, Building, Processor, ProcessorContext,
    ProcessorEngine, ProcessorError, ProcessorLifecycle, ProcessorSource, ProcessorSourceProvider,
    ProcessorSourceRef, Ready, Running,
};

use crate::projection::ProjectionError;
use coerce::persistent::journal::storage::JournalEntry;
use std::fmt::Debug;
use strum_macros::{Display, EnumString, EnumVariantNames, IntoStaticStr};

pub type ProcessorErrorHandler = dyn Fn(ProjectionError) + Send + Sync + 'static;

#[derive(Debug, Clone)]
pub enum ProcessResult<P: Debug + Clone> {
    Changed(P),
    Unchanged,
}

#[derive(
    PartialEq, Eq, Debug, Display, Clone, Copy, EnumString, EnumVariantNames, IntoStaticStr,
)]
pub enum ProcessResultKind {
    Changed,
    Unchanged,
}

impl<P> PartialEq for ProcessResult<P>
where
    P: Debug + Clone + PartialEq,
{
    fn eq(&self, rhs: &Self) -> bool {
        (self.kind() == rhs.kind()) && (self.as_ref() == rhs.as_ref())
    }
}

impl<P> ProcessResult<P>
where
    P: Debug + Clone,
{
    pub const fn changed(value: P) -> Self {
        Self::Changed(value)
    }

    pub const fn unchanged() -> Self {
        Self::Unchanged
    }

    pub const fn kind(&self) -> ProcessResultKind {
        match self {
            Self::Changed(_) => ProcessResultKind::Changed,
            Self::Unchanged => ProcessResultKind::Unchanged,
        }
    }

    pub const fn is_changed(&self) -> bool {
        matches!(self, Self::Changed(_))
    }

    pub const fn as_ref(&self) -> Option<&P> {
        match *self {
            Self::Changed(ref p) => Some(p),
            Self::Unchanged => None,
        }
    }

    #[inline]
    #[track_caller]
    pub fn unwrap(self) -> P {
        match self {
            Self::Changed(val) => val,
            Self::Unchanged => panic!("called `ProcessResult::unwrap()` on an `Unchanged` value"),
        }
    }
}

#[async_trait]
pub trait ProcessEntry {
    type Projection: Debug + Clone + Send;

    fn apply_entry_to_projection(
        &self,
        projection: &Self::Projection,
        entry: JournalEntry,
        ctx: &ProcessorContext,
    ) -> Result<ProcessResult<Self::Projection>, ProjectionError>;
}
