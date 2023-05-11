use super::actor::{protocol, PostgresJournal};
use crate::postgres::config::{self, PostgresStorageConfig};
use crate::postgres::protocol::SequenceRange;
use crate::postgres::{EntryType, PostgresStorageError, StorageKey};
use coerce::actor::system::ActorSystem;
use coerce::actor::{IntoActor, LocalActorRef};
use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::journal::storage::{JournalEntry, JournalStorage, JournalStorageRef};
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

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
        let connection_pool = config::connect_with(&config);
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
    fn storage_key_for(&self, persistence_id: &str, entry: EntryType) -> StorageKey {
        (self.key_provider_fn)(persistence_id, entry.into(), &self.config)
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
        let storage_key = self.storage_key_for(persistence_id, EntryType::Snapshot);
        self.postgres_journal
            .send(protocol::WriteSnapshot { storage_key, entry })
            .await?
            .map_err(|err| err.into())
    }

    #[instrument(level = "debug", skip())]
    async fn write_message(&self, persistence_id: &str, entry: JournalEntry) -> anyhow::Result<()> {
        let storage_key = self.storage_key_for(persistence_id, EntryType::Journal);
        self.postgres_journal
            .send(protocol::WriteMessage { storage_key, entry })
            .await?
            .map_err(|err| err.into())
    }

    #[instrument(level = "debug", skip())]
    async fn write_message_batch(
        &self,
        persistence_id: &str,
        entries: Vec<JournalEntry>,
    ) -> anyhow::Result<()> {
        let storage_key = self.storage_key_for(persistence_id, EntryType::Journal);

        self.postgres_journal
            .send(protocol::WriteMessages {
                storage_key,
                entries,
            })
            .await?
            .map_err(|err| err.into())
    }

    #[instrument(level = "debug", skip())]
    async fn read_latest_snapshot(
        &self,
        persistence_id: &str,
    ) -> anyhow::Result<Option<JournalEntry>> {
        let storage_key = self.storage_key_for(persistence_id, EntryType::Snapshot);
        self.postgres_journal
            .send(protocol::ReadSnapshot { storage_key })
            .await?
            .map_err(|err| err.into())
    }

    #[instrument(level = "debug", skip())]
    async fn read_latest_messages(
        &self,
        persistence_id: &str,
        from_sequence: i64,
    ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
        let storage_key = self.storage_key_for(persistence_id, EntryType::Journal);

        self.postgres_journal
            .send(protocol::ReadMessages {
                storage_key,
                sequence: SequenceRange::from_sequence(from_sequence),
            })
            .await?
            .map_err(|err| err.into())
    }

    #[instrument(level = "debug", skip())]
    async fn read_message(
        &self,
        persistence_id: &str,
        sequence_id: i64,
    ) -> anyhow::Result<Option<JournalEntry>> {
        let storage_key = self.storage_key_for(persistence_id, EntryType::Journal);
        self.postgres_journal
            .send(protocol::ReadMessages {
                storage_key,
                sequence: SequenceRange::single(sequence_id),
            })
            .await?
            .map(|entries_0| entries_0.and_then(|es| es.into_iter().next()))
            .map_err(|err| err.into())
    }

    #[instrument(level = "debug", skip())]
    async fn read_messages(
        &self,
        persistence_id: &str,
        from_sequence: i64,
        to_sequence: i64,
    ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
        let storage_key = self.storage_key_for(persistence_id, EntryType::Journal);
        self.postgres_journal
            .send(protocol::ReadMessages {
                storage_key,
                sequence: SequenceRange::for_exclusive_range(from_sequence..to_sequence),
            })
            .await?
            .map_err(|err| err.into())
    }

    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence: i64,
    ) -> anyhow::Result<()> {
        let storage_key = self.storage_key_for(persistence_id, EntryType::Journal);
        self.postgres_journal
            .send(protocol::DeleteMessages {
                storage_key,
                sequence: SequenceRange::for_exclusive_range(0..to_sequence),
            })
            .await?
            .map_err(|err| err.into())
    }

    #[instrument(level = "debug", skip())]
    async fn delete_all(&self, persistence_id: &str) -> anyhow::Result<()> {
        todo!()
    }
}
