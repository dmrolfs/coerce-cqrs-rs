use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::Actor;
use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::journal::storage::JournalEntry;
use sqlx::PgPool;
use tokio::sync::oneshot;

pub mod protocol {
    use super::*;
    use crate::projection::StorageKey;

    #[derive(Debug)]
    pub struct WriteJournal {
        pub storage_key: StorageKey,
        pub entry: JournalEntry,
        pub result_channel: oneshot::Sender<anyhow::Result<()>>,
    }

    impl Message for WriteJournal {
        type Result = ();
    }

    #[derive(Debug)]
    pub struct WriteSnapshot {
        pub storage_key: StorageKey,
        pub entry: JournalEntry,
        pub result_channel: oneshot::Sender<anyhow::Result<()>>,
    }

    impl Message for WriteSnapshot {
        type Result = ();
    }

    #[derive(Debug)]
    pub struct ReadSnapshot {
        pub storage_key: StorageKey,
        pub result_channel: oneshot::Sender<anyhow::Result<Option<JournalEntry>>>,
    }

    impl Message for ReadSnapshot {
        type Result = ();
    }

    #[derive(Debug)]
    pub struct ReadMessages {
        pub storage_key: StorageKey,
        pub from_sequence: i64,
        pub result_channel: oneshot::Sender<anyhow::Result<Option<Vec<JournalEntry>>>>,
    }

    impl Message for ReadMessages {
        type Result = ();
    }

    #[derive(Debug)]
    pub struct DeleteAll(pub Vec<StorageKey>);

    impl Message for DeleteAll {
        type Result = anyhow::Result<()>;
    }
}

#[derive(Debug, Default, Clone)]
pub struct PostgresJournal(crate::memory::InMemoryStorageProvider);

impl PostgresJournal {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    pub fn from_pool(_pool: PgPool) -> Self {
        Self::new()
    }
}

#[async_trait]
impl Actor for PostgresJournal {
    #[instrument(level = "trace", skip(_ctx))]
    async fn started(&mut self, _ctx: &mut ActorContext) {}

    #[instrument(level = "trace", skip(_ctx))]
    async fn stopped(&mut self, _ctx: &mut ActorContext) {}
}

use protocol::*;

#[async_trait]
impl Handler<WriteJournal> for PostgresJournal {
    #[instrument(level = "debug", skip(_ctx))]
    async fn handle(&mut self, message: WriteJournal, _ctx: &mut ActorContext) {
        let result = self
            .0
            .journal_storage()
            .unwrap()
            .write_message(message.storage_key.as_ref(), message.entry)
            .await;
        let _ = message.result_channel.send(result);
    }
}

#[async_trait]
impl Handler<WriteSnapshot> for PostgresJournal {
    #[instrument(level = "debug", skip(_ctx))]
    async fn handle(&mut self, message: WriteSnapshot, _ctx: &mut ActorContext) {
        let result = self
            .0
            .journal_storage()
            .unwrap()
            .write_snapshot(message.storage_key.as_ref(), message.entry)
            .await;
        let _ = message.result_channel.send(result);
    }
}

#[async_trait]
impl Handler<ReadSnapshot> for PostgresJournal {
    #[instrument(level = "debug", skip(_ctx))]
    async fn handle(&mut self, message: ReadSnapshot, _ctx: &mut ActorContext) {
        let result = self
            .0
            .journal_storage()
            .unwrap()
            .read_latest_snapshot(message.storage_key.as_ref())
            .await;
        let _ = message.result_channel.send(result);
    }
}

#[async_trait]
impl Handler<ReadMessages> for PostgresJournal {
    #[instrument(level = "debug", skip(_ctx))]
    async fn handle(&mut self, message: ReadMessages, _ctx: &mut ActorContext) {
        let result = self
            .0
            .journal_storage()
            .unwrap()
            .read_latest_messages(message.storage_key.as_ref(), message.from_sequence)
            .await;
        let _ = message.result_channel.send(result);
    }
}

#[async_trait]
impl Handler<DeleteAll> for PostgresJournal {
    #[instrument(level = "debug", skip(_ctx))]
    async fn handle(&mut self, message: DeleteAll, _ctx: &mut ActorContext) -> anyhow::Result<()> {
        let storage = self.0.journal_storage().unwrap();

        let mut results = Vec::with_capacity(message.0.len());
        for storage_key in message.0 {
            let s = storage.clone();
            let handle = tokio::spawn(async move { s.delete_all(storage_key.as_ref()).await });
            results.push(handle);
        }
        futures::future::join_all(results)
            .await
            .into_iter()
            .flatten()
            .find(|r| r.is_err())
            .unwrap_or(Ok(()))
    }
}
