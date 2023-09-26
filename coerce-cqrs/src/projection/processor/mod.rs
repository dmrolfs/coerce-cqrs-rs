mod engine;
mod interval;
#[allow(clippy::module_inception)]
mod processor;

pub use engine::building::Building;
pub use engine::ready::Ready;
pub use engine::running::Running;
pub use engine::{ProcessorEngine, ProcessorEngineRef, ProcessorLifecycle};
pub use interval::{
    CalculateInterval, CalculateIntervalFactory, ExponentialBackoff, RegularInterval,
};
pub use processor::protocol::{ProcessorApi, ProcessorCommand};
pub use processor::{
    AggregateEntries, AggregateOffsets, AggregateSequences, Processor, ProcessorContext,
    ProcessorError, ProcessorSource, ProcessorSourceProvider, ProcessorSourceRef,
};
use std::collections::HashSet;

use crate::projection::ProjectionError;
use coerce::actor::message::{Message, MessageUnwrapErr};
use coerce::persistent::journal::storage::JournalEntry;
use std::fmt::Debug;
use strum_macros::{Display, EnumString, EnumVariantNames, IntoStaticStr};

pub type ProcessorErrorHandler = dyn Fn(ProjectionError) + Send + Sync + 'static;

#[derive(Debug, Clone)]
pub enum ProcessResult<P, E: Debug> {
    Unchanged,
    Changed(P),
    Err(E),
}

#[derive(
    PartialEq, Eq, Debug, Display, Clone, Copy, EnumString, EnumVariantNames, IntoStaticStr,
)]
pub enum ProcessResultKind {
    Changed,
    Unchanged,
    Error,
}

impl<P, E> PartialEq for ProcessResult<P, E>
where
    P: PartialEq,
    E: PartialEq + Debug,
{
    fn eq(&self, rhs: &Self) -> bool {
        if self.kind() != rhs.kind() {
            return false;
        }

        match (self, rhs) {
            (Self::Changed(l), Self::Changed(r)) => l == r,
            (Self::Unchanged, Self::Unchanged) => true,
            (Self::Err(l), Self::Err(r)) => l == r,
            (_, _) => false,
        }
    }
}

impl<P, E: Debug> ProcessResult<P, E> {
    pub const fn kind(&self) -> ProcessResultKind {
        match self {
            Self::Changed(_) => ProcessResultKind::Changed,
            Self::Unchanged => ProcessResultKind::Unchanged,
            Self::Err(_) => ProcessResultKind::Error,
        }
    }

    pub const fn is_changed(&self) -> bool {
        matches!(self, Self::Changed(_))
    }

    pub const fn is_unchanged(&self) -> bool {
        matches!(self, Self::Unchanged)
    }

    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Err(_))
    }

    #[inline]
    #[allow(clippy::missing_const_for_fn)]
    pub fn changed(self) -> Option<P> {
        match self {
            Self::Changed(p) => Some(p),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::missing_const_for_fn)]
    pub fn err(self) -> Option<E> {
        match self {
            Self::Err(e) => Some(e),
            _ => None,
        }
    }

    #[inline]
    pub const fn as_ref(&self) -> ProcessResult<&P, &E> {
        match *self {
            Self::Changed(ref p) => ProcessResult::Changed(p),
            Self::Unchanged => ProcessResult::Unchanged,
            Self::Err(ref e) => ProcessResult::Err(e),
        }
    }

    #[inline]
    pub fn as_mut(&mut self) -> ProcessResult<&mut P, &mut E> {
        match *self {
            Self::Changed(ref mut p) => ProcessResult::Changed(p),
            Self::Unchanged => ProcessResult::Unchanged,
            Self::Err(ref mut e) => ProcessResult::Err(e),
        }
    }

    #[inline]
    pub fn map_err<F: Debug, O: FnOnce(E) -> F>(self, op: O) -> ProcessResult<P, F> {
        match self {
            Self::Err(e) => ProcessResult::Err(op(e)),
            Self::Changed(p) => ProcessResult::Changed(p),
            Self::Unchanged => ProcessResult::Unchanged,
        }
    }

    #[inline]
    pub fn inspect<F: FnOnce(&P)>(self, f: F) -> Self {
        if let Self::Changed(ref p) = self {
            f(p);
        }

        self
    }

    #[inline]
    pub fn inspect_err<F: FnOnce(&E)>(self, f: F) -> Self {
        if let Self::Err(ref e) = self {
            f(e);
        }

        self
    }

    #[inline]
    #[track_caller]
    pub fn unwrap(self) -> P {
        match self {
            Self::Changed(val) => val,
            Self::Unchanged => panic!("called `ProcessResult::unwrap()` on an `Unchanged` value"),
            Self::Err(e) => panic!("called `ProcessResult::unwrap()` on an `Err` value: {e:?}"),
        }
    }
}

#[async_trait]
pub trait ProcessEntry {
    type Projection;

    fn known_entry_types(&self) -> &EntryPayloadTypes;

    fn knows_entry_type(&self, payload_type_identifier: &str) -> bool {
        self.known_entry_types().is_known(payload_type_identifier)
    }

    #[inline]
    fn from_bytes<E: Message>(entry: JournalEntry) -> Result<E, MessageUnwrapErr> {
        E::from_bytes(entry.bytes.to_vec())
    }

    fn apply_entry_to_projection(
        &self,
        projection: &Self::Projection,
        entry: JournalEntry,
        ctx: &ProcessorContext,
    ) -> ProcessResult<Self::Projection, ProjectionError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryPayloadTypes {
    All,
    Set(HashSet<String>),
}

impl EntryPayloadTypes {
    pub const fn all() -> Self {
        Self::All
    }

    pub fn set(payload_types: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let pts = payload_types.into_iter().map(|rep| rep.into()).collect();
        Self::Set(pts)
    }

    pub fn is_known(&self, payload_type: &str) -> bool {
        match self {
            Self::All => true,
            Self::Set(pts) => pts.contains(payload_type),
        }
    }
}
