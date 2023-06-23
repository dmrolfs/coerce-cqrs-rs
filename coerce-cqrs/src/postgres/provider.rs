use super::actor::{protocol, PostgresJournal};
use crate::postgres::actor::protocol::SequenceRange;
use crate::postgres::config::{self, PostgresStorageConfig};
use crate::postgres::{
    EntryType, PostgresStorageError, SimpleStorageKeyCodec, StorageKey, StorageKeyCodec,
};
use crate::projection::{
    AggregateEntries, AggregateSequences, PersistenceId, ProcessorSource, ProcessorSourceProvider,
    ProcessorSourceRef,
};
use anyhow::Context;
use coerce::actor::system::ActorSystem;
use coerce::actor::{IntoActor, LocalActorRef};
use coerce::persistent::journal::storage::{JournalEntry, JournalStorage};
use coerce::persistent::provider::StorageProvider;
use coerce::persistent::storage::JournalStorageRef;
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

pub struct PostgresStorageProvider {
    postgres: Arc<PostgresJournalStorage>,
}

impl PostgresStorageProvider {
    #[instrument(level = "trace", skip(config, system))]
    pub async fn connect(
        config: PostgresStorageConfig,
        system: &ActorSystem,
    ) -> Result<Self, PostgresStorageError> {
        static POSTGRES_JOURNAL_COUNTER: AtomicU32 = AtomicU32::new(1);
        let connection_pool = config::connect_with(&config);
        let storage_key_codec = Arc::new(SimpleStorageKeyCodec::with_prefix(
            config.key_prefix.clone(),
        ));
        let postgres_journal =
            PostgresJournal::from_pool(connection_pool, &config, storage_key_codec.clone());
        let postgres_journal = postgres_journal
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
            storage_key_codec,
        });
        Ok(Self { postgres: storage })
    }
}

impl ProcessorSourceProvider for PostgresStorageProvider {
    fn processor_source(&self) -> Option<ProcessorSourceRef> {
        Some(self.postgres.clone())
    }
}

impl StorageProvider for PostgresStorageProvider {
    fn journal_storage(&self) -> Option<JournalStorageRef> {
        Some(self.postgres.clone())
    }
}

pub struct PostgresJournalStorage {
    postgres_journal: LocalActorRef<PostgresJournal>,
    config: PostgresStorageConfig,
    storage_key_codec: Arc<dyn StorageKeyCodec>,
}

impl fmt::Debug for PostgresJournalStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresJournalStorage")
            .field("actor", &self.postgres_journal)
            .field("config", &self.config)
            .finish()
    }
}

impl PostgresJournalStorage {
    fn storage_key_for(&self, persistence_id: &str, entry: EntryType) -> StorageKey {
        self.storage_key_codec
            .key_from_parts(persistence_id, entry.into())
    }
}

#[async_trait]
impl ProcessorSource for PostgresJournalStorage {
    #[instrument(level = "debug", skip())]
    async fn read_persistence_ids(&self) -> anyhow::Result<Vec<PersistenceId>> {
        self.postgres_journal
            .send(protocol::FindAllPersistenceIds)
            .await?
            .context("failed loading all persistence_ids")
    }

    #[instrument(level = "debug", skip(sequences))]
    async fn read_bulk_latest_messages(
        &self,
        sequences: AggregateSequences,
    ) -> anyhow::Result<Option<AggregateEntries>> {
        self.postgres_journal
            .send(protocol::ReadBulkLatestMessages { sequences })
            .await?
            .context("failed reading bulk latest messages")
    }
}

#[async_trait]
impl JournalStorage for PostgresJournalStorage {
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
            .with_context(|| format!("failed writing snapshot for {persistence_id}"))
    }

    #[instrument(level = "debug", skip())]
    async fn write_message(&self, persistence_id: &str, entry: JournalEntry) -> anyhow::Result<()> {
        let storage_key = self.storage_key_for(persistence_id, EntryType::Journal);
        self.postgres_journal
            .send(protocol::WriteMessage { storage_key, entry })
            .await?
            .with_context(|| format!("failed writing message for {persistence_id}"))
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
            .with_context(|| format!("failed writing message batch for {persistence_id}"))
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
            .with_context(|| format!("failed reading latest snapshot for {persistence_id}"))
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
            .with_context(|| format!("failed reading latest messages for {persistence_id} from sequence {from_sequence}"))
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
            .with_context(|| {
                format!("failed reading message for {persistence_id} at sequence_id {sequence_id}")
            })
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
            .with_context(|| format!("failed reading messages for {persistence_id} from {from_sequence} to {to_sequence} sequence_ids"))
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
            .with_context(|| format!("failed deleting messages to sequence for {persistence_id} to {to_sequence} sequence_id"))
    }

    #[instrument(level = "debug", skip())]
    async fn delete_all(&self, persistence_id: &str) -> anyhow::Result<()> {
        todo!()
    }
}
