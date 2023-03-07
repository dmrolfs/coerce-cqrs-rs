use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::Actor;
use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::journal::storage::JournalEntry;
use sqlx::PgPool;
use tokio::sync::oneshot;

pub mod protocol {
    use super::*;

    pub struct WriteJournal {
        pub key: String,
        pub entry: JournalEntry,
        pub result_channel: oneshot::Sender<anyhow::Result<()>>,
    }

    impl Message for WriteJournal {
        type Result = ();
    }

    pub struct WriteSnapshot {
        pub key: String,
        pub entry: JournalEntry,
        pub result_channel: oneshot::Sender<anyhow::Result<()>>,
    }

    impl Message for WriteSnapshot {
        type Result = ();
    }

    pub struct ReadSnapshot {
        pub key: String,
        pub result_channel: oneshot::Sender<anyhow::Result<Option<JournalEntry>>>,
    }

    impl Message for ReadSnapshot {
        type Result = ();
    }

    pub struct ReadMessages {
        pub key: String,
        pub from_sequence: i64,
        pub result_channel: oneshot::Sender<anyhow::Result<Option<Vec<JournalEntry>>>>,
    }

    impl Message for ReadMessages {
        type Result = ();
    }

    pub struct Delete(pub Vec<String>);

    impl Message for Delete {
        type Result = anyhow::Result<()>;
    }
}

#[derive(Debug, Clone)]
pub struct PostgresJournal(crate::memory::InMemoryStorageProvider);

impl PostgresJournal {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self(crate::memory::InMemoryStorageProvider::default())
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
    async fn handle(&mut self, message: WriteJournal, _ctx: &mut ActorContext) {
        let result = self
            .0
            .journal_storage()
            .unwrap()
            .write_message(message.key.as_str(), message.entry)
            .await;
        let _ = message.result_channel.send(result);
    }
}

#[async_trait]
impl Handler<WriteSnapshot> for PostgresJournal {
    async fn handle(&mut self, message: WriteSnapshot, _ctx: &mut ActorContext) {
        let result = self
            .0
            .journal_storage()
            .unwrap()
            .write_snapshot(message.key.as_str(), message.entry)
            .await;
        let _ = message.result_channel.send(result);
    }
}

#[async_trait]
impl Handler<ReadSnapshot> for PostgresJournal {
    async fn handle(&mut self, message: ReadSnapshot, _ctx: &mut ActorContext) {
        let result = self
            .0
            .journal_storage()
            .unwrap()
            .read_latest_snapshot(message.key.as_str())
            .await;
        let _ = message.result_channel.send(result);
    }
}

#[async_trait]
impl Handler<ReadMessages> for PostgresJournal {
    async fn handle(&mut self, message: ReadMessages, _ctx: &mut ActorContext) {
        let result = self
            .0
            .journal_storage()
            .unwrap()
            .read_latest_messages(message.key.as_str(), message.from_sequence)
            .await;
        let _ = message.result_channel.send(result);
    }
}

#[async_trait]
impl Handler<Delete> for PostgresJournal {
    async fn handle(&mut self, message: Delete, _ctx: &mut ActorContext) -> anyhow::Result<()> {
        let storage = self.0.journal_storage().unwrap();

        let mut results = Vec::with_capacity(message.0.len());
        for pid in message.0 {
            let s = storage.clone();
            let handle = tokio::spawn(async move { s.delete_all(pid.as_str()).await });
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
