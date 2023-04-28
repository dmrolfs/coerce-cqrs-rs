use coerce::actor::message::Message;
use coerce::persistent::storage::JournalEntry;
use serde::Serialize;
use std::fmt::{self, Debug};
use std::marker::PhantomData;

/// Each CQRS platform can have one or more post-commit actions where it will distribute committed
/// events. This is intended as a light-weight alternative to the `crate::projection::Processor`,
/// operates against the event journal. `PostCommitAction`s run within the local process and
/// are best-effort, whereas `Processor`s can operate in the same or a different process or node --
/// they only need to connect to the event journal.
///
/// Some example of tasks that queries commonly provide:
/// - update in-memory materialized views
/// - publish events to messaging service
/// - trigger a command on another aggregate
#[async_trait]
pub trait PostCommitAction: Debug + Send + Sync {
    /// Events will be dispatched here immediately after being committed.
    async fn dispatch(&self, persistence_id: &str, entry: &JournalEntry);
}

#[derive(Default)]
pub struct TracingAction<M: Message> {
    _marker: PhantomData<M>,
}

impl<M: Message> Debug for TracingAction<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let type_name = format!("TracingAction<{}>", std::any::type_name::<M>());
        f.debug_tuple(&type_name).finish()
    }
}

#[async_trait]
impl<M> PostCommitAction for TracingAction<M>
where
    M: Message + Serialize,
{
    async fn dispatch(&self, persistence_id: &str, entry: &JournalEntry) {
        let type_name = std::any::type_name::<M>();
        let Ok(message) = M::from_bytes(entry.bytes.to_vec()) else {
            debug!("EVENT_TRACE: failed to convert entry bytes into {type_name} message.");
            return;
        };

        match serde_json::to_string_pretty(&message) {
            Ok(payload) => {
                info!(
                    "EVENT_TRACE: {persistence_id}-{}: {payload}",
                    entry.sequence
                );
            }

            Err(error) => {
                debug!("EVENT_TRACE: failed to convert {type_name} event to json: {error:?}")
            }
        }
    }
}
