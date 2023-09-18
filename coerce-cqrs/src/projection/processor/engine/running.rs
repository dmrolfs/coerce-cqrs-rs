use crate::projection::processor::{ProcessorApi, ProcessorEngine, ProcessorLifecycle};
use crate::projection::ProjectionError;
use futures::FutureExt;
use tokio::task::JoinHandle;

#[derive(Debug)]
#[allow(dead_code)]
pub struct Running {
    pub(super) tx_api: ProcessorApi,
    pub(super) handle: JoinHandle<Result<(), ProjectionError>>,
}

impl ProcessorLifecycle for Running {}

impl ProcessorEngine<Running> {
    #[allow(dead_code)]
    pub fn tx_api(&self) -> ProcessorApi {
        self.inner.tx_api.clone()
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn block_for_completion(self) -> Result<(), ProjectionError> {
        let handle = self.inner.handle.fuse();
        handle.await?
    }
}
