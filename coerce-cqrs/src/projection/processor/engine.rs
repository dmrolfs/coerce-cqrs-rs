use std::sync::Arc;

pub mod building;
pub mod ready;
pub mod running;

/// Manages the `Processor` lifecycle
pub trait ProcessorLifecycle {}

pub type ProcessorEngineRef = Arc<ProcessorEngine<running::Running>>;

/// The `ProcessorEngine` subscribes to an aggregates `JournalEntry`s and processes the entries one
/// at a time. There can be more than one Processor for an aggregate type. Processors can be used to
/// update aggregate view projections for different requirements. The Processor can be thought
/// of as maintaining the "read-side" of a CQRS-based aggregate.
#[derive(Debug)]
pub struct ProcessorEngine<P: ProcessorLifecycle> {
    pub(crate) inner: P,
}
