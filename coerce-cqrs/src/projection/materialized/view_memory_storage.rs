use crate::projection::materialized::view_storage::ViewStorage;
use crate::projection::{ProjectionError, ProjectionId};
use parking_lot::RwLock;
use smol_str::SmolStr;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;

/// A memory-backed view storage for use in a `GenericViewProcessor`.
#[derive(Debug)]
pub struct InMemoryViewStorage<V> {
    view_name: SmolStr,
    store: RwLock<HashMap<ProjectionId, V>>,
    _marker: PhantomData<V>,
}

impl<V> InMemoryViewStorage<V> {
    pub fn new(view_name: impl AsRef<str>) -> Self {
        Self {
            view_name: SmolStr::new(view_name),
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
        self.view_name.as_str()
    }

    #[instrument(level="debug", skip(self), fields(view_name=%self.view_name))]
    async fn load_view(
        &self,
        view_id: &ProjectionId,
    ) -> Result<Option<Self::View>, ProjectionError> {
        let store = self.store.read();
        Ok(store.get(view_id).cloned())
    }

    #[instrument(level="debug", skip(self), fields(view_name=%self.view_name))]
    async fn save_view(
        &self,
        view_id: &ProjectionId,
        view: Self::View,
    ) -> Result<(), ProjectionError> {
        let mut store = self.store.write();
        store.insert(view_id.clone(), view);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use claim::*;
    use coerce_cqrs_test::fixtures::aggregate::TestView;
    use once_cell::sync::Lazy;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use tokio_test::block_on;

    #[test]
    fn test_load_and_save() {
        Lazy::force(&coerce_cqrs_test::setup_tracing::TEST_TRACING);
        let main_span = info_span!("view_memory_storage::test_load_and_save");
        let _main_span_guard = main_span.enter();

        let storage = Arc::new(InMemoryViewStorage::<TestView>::new("test_load_and_save"));
        // let app = ViewApplicator::new(storage.clone(), apply_test_event_to_view);
        let vid = "test_view_id".into();

        block_on(async move {
            assert_none!(assert_ok!(storage.load_view(&vid).await));

            let view = TestView {
                label: "test_view".to_string(),
                sum: 33,
            };
            assert_ok!(storage.save_view(&vid, view.clone()).await);
            let saved = assert_some!(assert_ok!(storage.load_view(&vid).await));
            assert_eq!(saved, view);
        });
    }
}
