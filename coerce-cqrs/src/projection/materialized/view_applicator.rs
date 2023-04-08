use crate::projection::materialized::view_storage::ViewStorage;
use crate::projection::processor::{ProcessEntry, ProcessorContext};
use crate::projection::ProjectionError;
use coerce::actor::message::Message;
use coerce::persistent::journal::storage::JournalEntry;
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct ViewApplicator<S, E, P>
where
    S: ViewStorage,
    E: Message,
    P: FnMut(S::View, E) -> S::View,
{
    view_storage: Arc<S>,
    processor: P,
    _marker: PhantomData<E>,
}

impl<S, E, P> Debug for ViewApplicator<S, E, P>
where
    S: ViewStorage + Debug,
    E: Message,
    P: FnMut(S::View, E) -> S::View,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ViewApplicator")
            .field("view_storage", &self.view_storage)
            .finish()
    }
}

impl<S, E, P> ViewApplicator<S, E, P>
where
    S: ViewStorage,
    E: Message,
    P: FnMut(S::View, E) -> S::View,
{
    pub fn new(view_storage: Arc<S>, processor: P) -> Self {
        Self {
            view_storage,
            processor,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<S, E, P> ProcessEntry for ViewApplicator<S, E, P>
where
    S: ViewStorage + Debug + Send + Sync,
    <S as ViewStorage>::View: Send,
    E: Message,
    P: FnMut(S::View, E) -> S::View + Send,
{
    #[instrument(level = "debug", skip(self))]
    async fn process_entry(
        &mut self,
        entry: JournalEntry,
        ctx: &ProcessorContext,
    ) -> Result<(), ProjectionError> {
        let event = match E::from_bytes(entry.bytes.to_vec()).map_err(|err| err.into()) {
            Ok(event) => event,
            Err(error) => {
                self.handle_error(error, entry.payload_type.as_ref(), ctx)?;
                return Ok(());
            }
        };

        let view_id = self
            .view_storage
            .view_id_from_persistence(&ctx.persistence_id);
        let view = self
            .view_storage
            .load_view(&view_id)
            .await?
            .unwrap_or_default();
        // let (view, _view_ctx) = self
        //     .view_storage
        //     .load_view_with_context(&view_id)
        //     .await?
        //     .unwrap_or_else(|| (S::View::default(), self.view_storage.context()));
        let updated_view = (self.processor)(view, event);
        self.view_storage.save_view(view_id, updated_view).await?;
        Ok(())
    }
}
