use super::actor::{protocol, PostgresJournal};
use crate::postgres::config::PostgresStorageConfig;
use crate::postgres::CqrsError;
use coerce::actor::system::ActorSystem;
use coerce::actor::{IntoActor, LocalActorRef};
use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::journal::storage::{JournalEntry, JournalStorage, JournalStorageRef};
use sqlx::PgPool;
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use strum_macros::{Display, EnumVariantNames, IntoStaticStr};
use tokio::sync::oneshot;

pub struct PostgresStorageProvider {
    storage: JournalStorageRef,
}

impl PostgresStorageProvider {
    #[instrument(level = "trace", skip(config, system))]
    pub async fn connect(
        config: PostgresStorageConfig,
        system: &ActorSystem,
    ) -> Result<Self, CqrsError> {
        create_provider(config, system).await
    }
}

#[instrument(level = "trace", skip(config, system))]
async fn create_provider(
    config: PostgresStorageConfig,
    system: &ActorSystem,
) -> Result<PostgresStorageProvider, CqrsError> {
    let config = Arc::new(config);

    static POSTGRES_JOURNAL_COUNTER: AtomicU32 = AtomicU32::new(1);
    let connection_pool = connect_with(&config);
    let journal = PostgresJournal::from_pool(connection_pool)
        .into_actor(
            Some(format!(
                "postgres-journal-{}",
                POSTGRES_JOURNAL_COUNTER.fetch_add(1, Ordering::Relaxed)
            )),
            system,
        )
        .await?;

    let storage = Arc::new(PostgresJournalStorage {
        journal,
        config,
        key_provider_fn: |pid, value_type, config| {
            format!(
                "{key_prefix}{pid}:{value_type}",
                key_prefix = config.key_prefix
            )
        },
    });
    Ok(PostgresStorageProvider { storage })
}

impl StorageProvider for PostgresStorageProvider {
    fn journal_storage(&self) -> Option<JournalStorageRef> {
        Some(self.storage.clone())
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
    K: Fn(&str, &str, &PostgresStorageConfig) -> String,
{
    journal: LocalActorRef<PostgresJournal>,
    config: Arc<PostgresStorageConfig>,
    key_provider_fn: K,
}

impl<K> fmt::Debug for PostgresJournalStorage<K>
where
    K: Fn(&str, &str, &PostgresStorageConfig) -> String,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresJournalStorage")
            .field("config", &self.config)
            .finish()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Display, IntoStaticStr, EnumVariantNames)]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
enum EntryType {
    Journal,
    Snapshot,
}

impl<K> PostgresJournalStorage<K>
where
    K: Fn(&str, &str, &PostgresStorageConfig) -> String,
{
    fn channel_key_for<R>(
        &self,
        persistence_id: &str,
        entry: EntryType,
    ) -> (String, (oneshot::Sender<R>, oneshot::Receiver<R>)) {
        let channel = oneshot::channel::<R>();
        let key = (self.key_provider_fn)(persistence_id, entry.into(), self.config.as_ref());
        (key, channel)
    }
}

#[async_trait]
impl<K> JournalStorage for PostgresJournalStorage<K>
where
    K: Fn(&str, &str, &PostgresStorageConfig) -> String + Send + Sync,
{
    #[instrument(level = "trace", skip())]
    async fn write_snapshot(
        &self,
        persistence_id: &str,
        entry: JournalEntry,
    ) -> anyhow::Result<()> {
        let (key, (result_channel, rx)) = self.channel_key_for(persistence_id, EntryType::Snapshot);
        self.journal.notify(protocol::WriteSnapshot {
            key,
            entry,
            result_channel,
        })?;
        rx.await?
    }

    #[instrument(level = "trace", skip())]
    async fn write_message(&self, persistence_id: &str, entry: JournalEntry) -> anyhow::Result<()> {
        let (key, (result_channel, rx)) = self.channel_key_for(persistence_id, EntryType::Journal);
        self.journal.notify(protocol::WriteJournal {
            key,
            entry,
            result_channel,
        })?;
        rx.await?
    }

    #[instrument(level = "trace", skip())]
    async fn read_latest_snapshot(
        &self,
        persistence_id: &str,
    ) -> anyhow::Result<Option<JournalEntry>> {
        let (key, (result_channel, rx)) = self.channel_key_for(persistence_id, EntryType::Snapshot);
        self.journal.notify(protocol::ReadSnapshot {
            key,
            result_channel,
        })?;
        rx.await?
    }

    #[instrument(level = "trace", skip())]
    async fn read_latest_messages(
        &self,
        persistence_id: &str,
        from_sequence: i64,
    ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
        let (key, (result_channel, rx)) = self.channel_key_for(persistence_id, EntryType::Journal);
        self.journal.notify(protocol::ReadMessages {
            key,
            from_sequence,
            result_channel,
        })?;
        rx.await?
    }

    #[instrument(level = "trace", skip())]
    async fn delete_all(&self, persistence_id: &str) -> anyhow::Result<()> {
        todo!()
    }
}
