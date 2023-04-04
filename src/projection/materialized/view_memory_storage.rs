use crate::projection::materialized::view_storage::ViewStorage;
use crate::projection::materialized::{ViewContext, ViewId};
use crate::projection::ProjectionError;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;

/// A memory-backed view storage for use in a `GenericViewProcessor`.
#[derive(Debug)]
pub struct InMemoryViewStorage<V> {
    context: ViewContext,
    store: RwLock<HashMap<ViewId, V>>,
    _marker: PhantomData<V>,
}

impl<V> InMemoryViewStorage<V> {
    pub fn new(view_name: impl AsRef<str>) -> Self {
        Self::new_with_version(view_name, 1)
    }

    pub fn new_with_version(view_name: impl AsRef<str>, version: i64) -> Self {
        Self {
            context: ViewContext::new(view_name, version),
            store: RwLock::new(HashMap::new()),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<V> ViewStorage for InMemoryViewStorage<V>
where
    V: Debug + Default + Clone + Send + Sync,
{
    type View = V;

    fn view_name(&self) -> &str {
        self.context.view_name.as_str()
    }

    fn version(&self) -> i64 {
        self.context.version
    }

    #[instrument(level="debug", skip(self), fields(view_context=?self.context))]
    async fn load_view(&self, id: &ViewId) -> Result<Option<Self::View>, ProjectionError> {
        let store = self.store.read();
        Ok(store.get(id).cloned())
    }

    #[instrument(level="debug", skip(self), fields(view_context=?self.context))]
    async fn load_view_with_context(
        &self,
        id: &ViewId,
    ) -> Result<Option<(Self::View, ViewContext)>, ProjectionError> {
        self.load_view(id)
            .await
            .map(|view| view.map(|v| (v, self.context())))
    }

    #[instrument(level="debug", skip(self), fields(view_context=?self.context))]
    async fn save_view(&self, view_id: ViewId, view: Self::View) -> Result<(), ProjectionError> {
        let mut store = self.store.write();
        store.insert(view_id, view);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::test_aggregate::TestView;
    use claim::*;
    use once_cell::sync::Lazy;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use tokio_test::block_on;

    #[test]
    fn test_load_and_save() {
        Lazy::force(&crate::test_fixtures::TEST_TRACING);
        let main_span = info_span!("view_memory_storage::test_load_and_save");
        let _main_span_guard = main_span.enter();

        let storage = Arc::new(InMemoryViewStorage::<TestView>::new("test_load_and_save"));
        // let app = ViewApplicator::new(storage.clone(), apply_test_event_to_view);
        let vid = "test_view_id".into();

        block_on(async move {
            assert_none!(assert_ok!(storage.load_view_with_context(&vid).await));

            let view = TestView {
                label: "test_view".to_string(),
                sum: 33,
            };
            assert_ok!(storage.save_view(vid.clone(), view.clone()).await);
            let saved = assert_some!(assert_ok!(storage.load_view_with_context(&vid).await));
            assert_eq!(saved.0, view);
            assert_eq!(
                saved.1,
                ViewContext {
                    view_name: "test_load_and_save".into(),
                    version: 1,
                }
            );
        });
    }
}
