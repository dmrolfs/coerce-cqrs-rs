use super::actor::{protocol, PostgresJournal};
use crate::postgres::config::PostgresStorageConfig;
use crate::postgres::protocol::SequenceRange;
use crate::postgres::{EntryType, PostgresStorageError, StorageKey};
use coerce::actor::system::ActorSystem;
use coerce::actor::{IntoActor, LocalActorRef};
use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::journal::storage::{JournalEntry, JournalStorage, JournalStorageRef};
use sqlx::PgPool;
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot;

pub struct PostgresStorageProvider {
    postgres: JournalStorageRef,
}

impl PostgresStorageProvider {
    #[instrument(level = "trace", skip(config, system))]
    pub async fn connect(
        config: PostgresStorageConfig,
        system: &ActorSystem,
    ) -> Result<Self, PostgresStorageError> {
        static POSTGRES_JOURNAL_COUNTER: AtomicU32 = AtomicU32::new(1);
        let connection_pool = connect_with(&config);
        let postgres_journal = PostgresJournal::from_pool(connection_pool, &config)
            .into_actor(
                Some(format!(
                    "postgres-journal-{}",
                    POSTGRES_JOURNAL_COUNTER.fetch_add(1, Ordering::Relaxed)
                )),
                system,
            )
            .await?;

        let storage = Arc::new(PostgresJournalStorage {
            postgres_journal,
            config,
            key_provider_fn: crate::postgres::storage_key_provider,
        });
        Ok(Self { postgres: storage })
    }
}

impl StorageProvider for PostgresStorageProvider {
    fn journal_storage(&self) -> Option<JournalStorageRef> {
        Some(self.postgres.clone())
    }
}

#[instrument(level = "trace")]
fn connect_with(config: &PostgresStorageConfig) -> PgPool {
    let connection_options = config.pg_connect_options_with_db();
    config
        .pg_pool_options()
        .connect_lazy_with(connection_options)
}

pub struct PostgresJournalStorage<K>
where
    K: Fn(&str, &str, &PostgresStorageConfig) -> StorageKey,
{
    postgres_journal: LocalActorRef<PostgresJournal>,
    config: PostgresStorageConfig,
    key_provider_fn: K,
}

impl<K> fmt::Debug for PostgresJournalStorage<K>
where
    K: Fn(&str, &str, &PostgresStorageConfig) -> StorageKey,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresJournalStorage")
            .field("config", &self.config)
            .finish()
    }
}

impl<K> PostgresJournalStorage<K>
where
    K: Fn(&str, &str, &PostgresStorageConfig) -> StorageKey,
{
    fn result_channel_and_storage_key_for<R>(
        &self,
        persistence_id: &str,
        entry: EntryType,
    ) -> ((oneshot::Sender<R>, oneshot::Receiver<R>), StorageKey) {
        let channel = oneshot::channel::<R>();
        let storage_key = (self.key_provider_fn)(persistence_id, entry.into(), &self.config);
        (channel, storage_key)
    }
}

#[async_trait]
impl<K> JournalStorage for PostgresJournalStorage<K>
where
    K: Fn(&str, &str, &PostgresStorageConfig) -> StorageKey + Send + Sync,
{
    #[instrument(level = "debug", skip())]
    async fn write_snapshot(
        &self,
        persistence_id: &str,
        entry: JournalEntry,
    ) -> anyhow::Result<()> {
        let ((result_channel, rx), storage_key) =
            self.result_channel_and_storage_key_for(persistence_id, EntryType::Snapshot);
        self.postgres_journal.notify(protocol::WriteSnapshot {
            storage_key,
            entry,
            result_channel,
        })?;
        rx.await?
    }

    #[instrument(level = "debug", skip())]
    async fn write_message(&self, persistence_id: &str, entry: JournalEntry) -> anyhow::Result<()> {
        debug!(
            "DMR: PERSISTING MESSAGE: {:?}",
            String::from_utf8(entry.bytes.to_vec())
        );

        let ((result_channel, rx), storage_key) =
            self.result_channel_and_storage_key_for(persistence_id, EntryType::Journal);
        self.postgres_journal.notify(protocol::WriteMessage {
            storage_key,
            entry,
            result_channel,
        })?;
        rx.await?
    }

    #[instrument(level = "debug", skip())]
    async fn write_message_batch(
        &self,
        persistence_id: &str,
        entries: Vec<JournalEntry>,
    ) -> anyhow::Result<()> {
        debug!(
            "DMR: PERSISTING MESSAGES: {:?}",
            entries
                .iter()
                .map(|e| String::from_utf8(e.bytes.to_vec()))
                .collect::<Vec<_>>()
        );

        let ((result_channel, rx), storage_key) =
            self.result_channel_and_storage_key_for(persistence_id, EntryType::Journal);
        self.postgres_journal.notify(protocol::WriteMessages {
            storage_key,
            entries,
            result_channel,
        })?;
        rx.await?
    }

    #[instrument(level = "debug", skip())]
    async fn read_latest_snapshot(
        &self,
        persistence_id: &str,
    ) -> anyhow::Result<Option<JournalEntry>> {
        let ((result_channel, rx), storage_key) =
            self.result_channel_and_storage_key_for(persistence_id, EntryType::Snapshot);
        self.postgres_journal.notify(protocol::ReadSnapshot {
            storage_key,
            result_channel,
        })?;
        rx.await?
    }

    #[instrument(level = "debug", skip())]
    async fn read_latest_messages(
        &self,
        persistence_id: &str,
        from_sequence: i64,
    ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
        let ((result_channel, rx), storage_key) =
            self.result_channel_and_storage_key_for(persistence_id, EntryType::Journal);
        info!(%persistence_id, %from_sequence, "DMR: READ_LATEST_MESSAGES...");

        self.postgres_journal.notify(protocol::ReadMessages {
            storage_key,
            sequence: SequenceRange::from_sequence(from_sequence),
            result_channel,
        })?;

        rx.await?
    }

    #[instrument(level = "debug", skip())]
    async fn read_message(
        &self,
        persistence_id: &str,
        sequence_id: i64,
    ) -> anyhow::Result<Option<JournalEntry>> {
        let ((result_channel, rx), storage_key) =
            self.result_channel_and_storage_key_for(persistence_id, EntryType::Journal);

        self.postgres_journal.notify(protocol::ReadMessages {
            storage_key,
            sequence: SequenceRange::single(sequence_id),
            result_channel,
        })?;

        let entries = rx.await?;
        entries.map(|entries_0| entries_0.and_then(|es| es.into_iter().next()))
    }

    #[instrument(level = "debug", skip())]
    async fn read_messages(
        &self,
        persistence_id: &str,
        from_sequence: i64,
        to_sequence: i64,
    ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
        let ((result_channel, rx), storage_key) =
            self.result_channel_and_storage_key_for(persistence_id, EntryType::Journal);

        self.postgres_journal.notify(protocol::ReadMessages {
            storage_key,
            sequence: SequenceRange::for_exclusive_range(from_sequence..to_sequence),
            result_channel,
        })?;

        rx.await?
    }

    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence: i64,
    ) -> anyhow::Result<()> {
        let ((result_channel, rx), storage_key) =
            self.result_channel_and_storage_key_for(persistence_id, EntryType::Journal);

        self.postgres_journal.notify(protocol::DeleteMessages {
            storage_key,
            sequence: SequenceRange::for_exclusive_range(0..to_sequence),
            result_channel,
        })?;

        rx.await?
    }

    #[instrument(level = "debug", skip())]
    async fn delete_all(&self, persistence_id: &str) -> anyhow::Result<()> {
        todo!()
    }
}
