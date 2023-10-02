use crate::projection::materialized::projection_storage::{AggregateOffsets, ProjectionStorage};
use crate::projection::{Offset, PersistenceId, ProjectionError};
use smol_str::SmolStr;
use std::fmt::Debug;
use std::marker::PhantomData;

/// A memory-backed projection storage for use in a `GenericProjectionProcessor`.
#[derive(Debug)]
pub struct InMemoryProjectionStorage<V> {
    name: SmolStr,
    view_store: dashmap::DashMap<PersistenceId, V>,
    offset_store: dashmap::DashMap<PersistenceId, Offset>,
    _marker: PhantomData<V>,
}

impl<V> InMemoryProjectionStorage<V> {
    pub fn new(name: impl AsRef<str>) -> Self {
        Self {
            name: SmolStr::new(name),
            view_store: Default::default(),
            offset_store: Default::default(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<V> ProjectionStorage for InMemoryProjectionStorage<V>
where
    V: Debug + Default + Clone + Send + Sync,
{
    type ViewId = PersistenceId;
    type Projection = V;

    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[instrument(level="debug", skip(self), fields(projection_name=%self.name))]
    async fn load_projection(
        &self,
        view_id: &Self::ViewId,
    ) -> Result<Option<Self::Projection>, ProjectionError> {
        Ok(self.view_store.get(view_id).map(|v| v.clone()))
    }

    #[instrument(level="debug", skip(self), fields(projection_name=%self.name))]
    async fn save_projection_and_last_offset(
        &self,
        view_id: &Self::ViewId,
        projection: Option<Self::Projection>,
        last_offset: Offset,
    ) -> Result<(), ProjectionError> {
        let is_new_projection = projection.is_some();
        let prior_view = projection.and_then(|p| self.view_store.insert(view_id.clone(), p));
        let prior_offset = self.offset_store.insert(view_id.clone(), last_offset);
        debug!(
            "save {} view's offset [{}::{view_id}] = {last_offset:?} was {prior_offset:?}",
            if !is_new_projection {
                "not-updated"
            } else if prior_view.is_none() {
                "new"
            } else {
                "replacement"
            },
            self.name
        );
        Ok(())
    }

    async fn read_all_offsets(
        &self,
        _projection_name: &str,
    ) -> Result<AggregateOffsets, ProjectionError> {
        debug!("DMR: All LAST_OFFSETS: {:?}", self.offset_store);
        Ok(self.offset_store.clone().into_iter().collect())
    }

    async fn read_offset(
        &self,
        _projection_name: &str,
        persistence_id: &PersistenceId,
    ) -> Result<Option<Offset>, ProjectionError> {
        Ok(self.offset_store.get(persistence_id).map(|v| *v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use claim::*;
    use coerce_cqrs_test::fixtures::aggregate::{TestEvent, TestView};
    use once_cell::sync::Lazy;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use tokio_test::block_on;

    #[test]
    fn test_load_and_save() {
        Lazy::force(&coerce_cqrs_test::setup_tracing::TEST_TRACING);
        let main_span = info_span!("projection_memory_storage::test_load_and_save");
        let _main_span_guard = main_span.enter();

        let storage = Arc::new(InMemoryProjectionStorage::<TestView>::new(
            "test_load_and_save",
        ));
        // let app = ViewApplicator::new(storage.clone(), apply_test_event_to_view);
        let vid = PersistenceId::from_parts("TestAggregate", "test_view_id");

        block_on(async move {
            assert_none!(assert_ok!(storage.load_projection(&vid).await));

            let view = TestView {
                label: "test_view".to_string(),
                count: 1,
                events: vec![TestEvent::Tested(1)],
                sum: 33,
            };
            assert_ok!(
                storage
                    .save_projection_and_last_offset(&vid, Some(view.clone()), Offset::new(1))
                    .await
            );
            let saved = assert_some!(assert_ok!(storage.load_projection(&vid).await));
            assert_eq!(saved, view);
        });
    }
}
