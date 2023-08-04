use crate::postgres::sql_query::SqlQueryFactory;
use crate::postgres::{
    EntryType, PostgresStorageConfig, PostgresStorageError, StorageKey, StorageKeyCodec,
    StorageKeyParts,
};
use crate::projection::{processor::AggregateEntries, PersistenceId, PostCommitAction};
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::Actor;
use coerce::persistent::journal::storage::JournalEntry;
use protocol::*;
use sqlx::postgres::{PgQueryResult, PgRow};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::sync::Arc;

pub(in crate::postgres) mod protocol {
    use super::*;
    use crate::postgres::{PostgresStorageError, StorageKey};
    use crate::projection::processor::{AggregateEntries, AggregateSequences};
    use std::fmt;

    #[derive(Debug)]
    pub struct FindAllPersistenceIds;

    impl Message for FindAllPersistenceIds {
        type Result = Result<Vec<PersistenceId>, PostgresStorageError>;
    }

    pub struct ReadBulkLatestMessages {
        pub sequences: AggregateSequences,
    }

    impl fmt::Debug for ReadBulkLatestMessages {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_map()
                .entries(
                    self.sequences
                        .iter()
                        .map(|(pid, seq)| (format!("{pid:?}"), seq)),
                )
                .finish()
        }
    }

    impl Message for ReadBulkLatestMessages {
        type Result = Result<Option<AggregateEntries>, PostgresStorageError>;
    }

    #[derive(Debug)]
    pub struct ReadMessages {
        pub(in crate::postgres) storage_key: StorageKey,
        pub sequence: SequenceRange,
    }

    impl Message for ReadMessages {
        type Result = Result<Option<Vec<JournalEntry>>, PostgresStorageError>;
    }

    #[derive(Debug)]
    pub struct WriteMessage {
        pub(in crate::postgres) storage_key: StorageKey,
        pub entry: JournalEntry,
    }

    impl Message for WriteMessage {
        type Result = Result<(), PostgresStorageError>;
    }

    #[derive(Debug)]
    pub struct WriteMessages {
        pub(in crate::postgres) storage_key: StorageKey,
        pub entries: Vec<JournalEntry>,
    }

    impl Message for WriteMessages {
        type Result = Result<(), PostgresStorageError>;
    }

    #[derive(Debug)]
    pub struct WriteSnapshot {
        pub(in crate::postgres) storage_key: StorageKey,
        pub entry: JournalEntry,
    }

    impl Message for WriteSnapshot {
        type Result = Result<(), PostgresStorageError>;
    }

    #[derive(Debug)]
    pub struct ReadSnapshot {
        pub(in crate::postgres) storage_key: StorageKey,
    }

    impl Message for ReadSnapshot {
        type Result = Result<Option<JournalEntry>, PostgresStorageError>;
    }

    #[derive(Debug, PartialEq, Eq)]
    pub enum SequenceRange {
        Single(i64),
        From(i64),
        Exclusive(std::ops::Range<i64>),
        All,
    }

    impl SequenceRange {
        #[allow(dead_code)]
        pub const fn single(sequence: i64) -> Self {
            Self::Single(sequence)
        }

        #[allow(dead_code)]
        pub const fn from_sequence(sequence: i64) -> Self {
            Self::From(sequence)
        }

        #[allow(dead_code)]
        pub const fn until(sequence: i64) -> Self {
            Self::for_exclusive_range(0..sequence)
        }

        #[allow(dead_code)]
        pub const fn for_exclusive_range(range: std::ops::Range<i64>) -> Self {
            Self::Exclusive(range)
        }
    }

    #[derive(Debug)]
    pub struct DeleteMessages {
        pub(in crate::postgres) storage_key: StorageKey,
        pub sequence: SequenceRange,
    }

    impl Message for DeleteMessages {
        type Result = Result<(), PostgresStorageError>;
    }

    #[derive(Debug)]
    pub struct DeleteAll(pub(in crate::postgres) Vec<StorageKey>);

    impl Message for DeleteAll {
        type Result = Result<(), PostgresStorageError>;
    }
}

#[derive(Debug)]
pub(in crate::postgres) struct PostgresJournal {
    pool: PgPool,
    post_commit_actions: Vec<Box<dyn PostCommitAction>>,
    sql_query: SqlQueryFactory,
    storage_key_codec: Arc<dyn StorageKeyCodec>,
}

impl PostgresJournal {
    #[allow(dead_code)]
    pub fn new(
        pool: PgPool,
        config: &PostgresStorageConfig,
        storage_key_codec: Arc<dyn StorageKeyCodec>,
        post_commit_actions: Vec<Box<dyn PostCommitAction>>,
    ) -> Self {
        let sql_query = SqlQueryFactory::new(
            &config.event_journal_table_name,
            &config.projection_offsets_table_name,
        )
        .with_snapshots_table(&config.snapshot_table_name);

        Self {
            pool,
            post_commit_actions,
            sql_query,
            storage_key_codec,
        }
    }

    #[allow(dead_code)]
    pub fn from_pool(
        pool: PgPool,
        config: &PostgresStorageConfig,
        storage_key_codec: Arc<dyn StorageKeyCodec>,
    ) -> Self {
        Self::new(pool, config, storage_key_codec, vec![])
    }

    #[allow(dead_code)]
    fn storage_key_into_parts(
        &self,
        key: StorageKey,
    ) -> Result<StorageKeyParts, PostgresStorageError> {
        self.storage_key_codec.key_into_parts(key)
    }
}

#[async_trait]
impl Actor for PostgresJournal {
    #[instrument(level = "trace", skip(ctx), fields(ctx=ctx.log().as_value()))]
    async fn started(&mut self, ctx: &mut ActorContext) {}

    #[instrument(level = "trace", skip(ctx), fields(ctx=ctx.log().as_value()))]
    async fn stopped(&mut self, ctx: &mut ActorContext) {}
}

const EMPTY_META: serde_json::Value = serde_json::Value::Null;

impl PostgresJournal {
    #[instrument(
        level = "debug",
        skip(tx, ctx),
        fields(ctx=ctx.log().as_value())
    )]
    async fn persist_message(
        &self,
        storage_key: &StorageKey,
        entry: JournalEntry,
        tx: &mut Transaction<'_, Postgres>,
        ctx: &mut ActorContext,
    ) -> Result<PgQueryResult, PostgresStorageError> {
        let query = sqlx::query(self.sql_query.append_event())
            .bind(storage_key) // persistence_id
            .bind(entry.sequence) // sequence_number
            .bind(false) // is_deleted
            .bind(entry.payload_type.as_ref()) // event_manifest
            .bind(entry.bytes.to_vec()) // event_payload
            .bind(EMPTY_META.clone()) // meta_payload
            .bind(crate::util::now_timestamp()); // created_at

        let query_result = query.execute(tx).await.map_err(|err| err.into());
        match &query_result {
            Ok(_) => debug!("postgres journal message saved."),
            Err(error) => error!("postgres journal failed to persist message: {error:?}"),
        }

        query_result
    }
}

#[async_trait]
impl Handler<WriteMessage> for PostgresJournal {
    #[instrument(level = "debug", skip(ctx), fields(ctx=ctx.log().as_value()))]
    async fn handle(
        &mut self,
        message: WriteMessage,
        ctx: &mut ActorContext,
    ) -> <WriteMessage as Message>::Result {
        let storage_key = message.storage_key.as_ref();

        let mut tx = sqlx::Acquire::begin(&self.pool).await?;
        let _ = self
            .persist_message(&message.storage_key, message.entry, &mut tx, ctx)
            .await?;

        if let Err(error) = tx.commit().await {
            error!(%storage_key, "Postgres journal failed to commit event persist message transaction: {error:?}");
            return Err(error.into());
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<WriteMessages> for PostgresJournal {
    #[instrument(level = "debug", skip(ctx), fields(ctx=ctx.log().as_value()))]
    async fn handle(
        &mut self,
        message: WriteMessages,
        ctx: &mut ActorContext,
    ) -> <WriteMessages as Message>::Result {
        let mut tx = sqlx::Acquire::begin(&self.pool).await?;
        for entry in message.entries {
            let _ = self
                .persist_message(&message.storage_key, entry, &mut tx, ctx)
                .await?;
        }

        if let Err(error) = tx.commit().await {
            error!(
                storage_key=%message.storage_key,
                "Postgres journal failed to commit event persist messages transaction: {error:?}"
            );
            return Err(error.into());
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<WriteSnapshot> for PostgresJournal {
    #[instrument(level = "debug", skip(ctx), fields(ctx=ctx.log().as_value()))]
    async fn handle(
        &mut self,
        message: WriteSnapshot,
        ctx: &mut ActorContext,
    ) -> <WriteSnapshot as Message>::Result {
        // let storage_key = message.storage_key.as_ref();

        let now = crate::util::now_timestamp();

        let mut tx = sqlx::Acquire::begin(&self.pool).await?;
        let _ = sqlx::query(self.sql_query.update_or_insert_snapshot())
            .bind(&message.storage_key) // persistence_id
            .bind(message.entry.sequence) // sequence_number
            .bind(message.entry.payload_type.as_ref()) // snapshot_manifest
            .bind(message.entry.bytes.to_vec()) // snapshot_payload
            .bind(EMPTY_META.clone()) // meta_payload
            .bind(now) // created_at
            .bind(now) // last_updated_at
            .execute(&mut tx)
            .await?;

        if let Err(error) = tx.commit().await {
            error!(
                storage_key=%message.storage_key,
                "postgres journal failed to commit event deletion transaction: {error:?}"
            );
            return Err(error.into());
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<FindAllPersistenceIds> for PostgresJournal {
    #[instrument(level = "debug", skip(ctx), fields(ctx=ctx.log().as_value()))]
    async fn handle(
        &mut self,
        _message: FindAllPersistenceIds,
        ctx: &mut ActorContext,
    ) -> <FindAllPersistenceIds as Message>::Result {
        let persistence_id_column = self.sql_query.persistence_id_column();
        let keys: Result<Vec<StorageKey>, PostgresStorageError> =
            sqlx::query(self.sql_query.select_persistence_ids())
                .map(|row: PgRow| row.get(persistence_id_column))
                .fetch_all(&self.pool)
                .await
                .map_err(|err| err.into());
        debug!("DMR: persistence keys: {keys:?}");

        keys?
            .into_iter()
            .map(|k| self.storage_key_into_parts(k).map(|(_, pid, _)| pid))
            .collect()
    }
}

impl PostgresJournal {
    fn entry_from_row(
        &self,
        row: PgRow,
        manifest_column: &str,
        payload_column: &str,
    ) -> JournalEntry {
        let sequence = row.get(self.sql_query.sequence_number_column());
        let payload_type: String = row.get(manifest_column);
        let payload_type = Arc::from(payload_type);
        let bytes = Arc::new(row.get(payload_column));
        JournalEntry {
            sequence,
            payload_type,
            bytes,
        }
    }
}

#[async_trait]
impl Handler<ReadSnapshot> for PostgresJournal {
    #[instrument(level = "debug", skip(ctx), fields(ctx=ctx.log().as_value()))]
    async fn handle(
        &mut self,
        message: ReadSnapshot,
        ctx: &mut ActorContext,
    ) -> <ReadSnapshot as Message>::Result {
        sqlx::query(self.sql_query.select_snapshot())
            .bind(&message.storage_key)
            .map(|row| {
                self.entry_from_row(
                    row,
                    self.sql_query.snapshot_manifest_column(),
                    self.sql_query.snapshot_payload_column(),
                )
            })
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| err.into())
    }
}

#[async_trait]
impl Handler<ReadBulkLatestMessages> for PostgresJournal {
    #[instrument(level="debug", skip(ctx,), fields(ctx=ctx.log().as_value()))]
    async fn handle(
        &mut self,
        message: ReadBulkLatestMessages,
        ctx: &mut ActorContext,
    ) -> <ReadBulkLatestMessages as Message>::Result {
        let manifest_column = self.sql_query.event_manifest_column();
        let payload_column = self.sql_query.event_payload_column();

        let mut result = AggregateEntries::default();

        for (persistence_id, last_offset) in message.sequences {
            debug!("DMR: last_offset for {persistence_id:?} = {last_offset:?}");
            let storage_key = self
                .storage_key_codec
                .key_from_persistence_parts(&persistence_id, EntryType::Journal);
            let query = sqlx::query(self.sql_query.select_latest_events())
                .bind(&storage_key)
                .bind(last_offset.unwrap_or(0))
                .map(|row: PgRow| self.entry_from_row(row, manifest_column, payload_column))
                .fetch_all(&self.pool);

            let entries = query.await?;
            result.insert(persistence_id, entries);
        }

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result))
        }

        // sqlx::query(self.sql_query.select_all_latest_events())
        //     .bind(message.projection_id)
        //     .map(|row: PgRow| {
        //         let storage_key: StorageKey = row.get(persistence_id_column);
        //         let entry = self.entry_from_row(row, manifest_column, payload_column);
        //         self.storage_key_into_parts(storage_key)
        //             .map(|parts| (parts.1, entry))
        //     })
        //     .fetch_all(&self.pool)
        //     .await
        //     .map_err(|err| err.into())
        //     .and_then(|pid_entries| {
        //         let pid_entries: Result<Vec<_>, PostgresStorageError> =
        //             pid_entries.into_iter().collect();
        //         pid_entries
        //     })
        //     .map(Some)
    }
}

#[async_trait]
impl Handler<ReadMessages> for PostgresJournal {
    #[instrument(level = "debug", skip(ctx), fields(ctx=ctx.log().as_value()))]
    async fn handle(
        &mut self,
        message: ReadMessages,
        ctx: &mut ActorContext,
    ) -> <ReadMessages as Message>::Result {
        // let persistence_id = message.storage_key.as_ref();

        let query = match message.sequence {
            SequenceRange::Single(sequence) => sqlx::query(self.sql_query.select_event())
                .bind(&message.storage_key)
                .bind(sequence),

            SequenceRange::From(from_sequence) => {
                sqlx::query(self.sql_query.select_latest_events())
                    .bind(&message.storage_key)
                    .bind(from_sequence)
            }

            SequenceRange::Exclusive(sequence_range) => {
                sqlx::query(self.sql_query.select_events_range())
                    .bind(&message.storage_key)
                    .bind(sequence_range.start)
                    .bind(sequence_range.end)
            }

            SequenceRange::All => sqlx::query(self.sql_query.select_latest_events())
                .bind(&message.storage_key)
                .bind(0),
        };

        query
            .map(|row| {
                self.entry_from_row(
                    row,
                    self.sql_query.event_manifest_column(),
                    self.sql_query.event_payload_column(),
                )
            })
            .fetch_all(&self.pool)
            .await
            .map_err(|err| err.into())
            .map(Some)
    }
}

#[async_trait]
impl Handler<DeleteMessages> for PostgresJournal {
    #[instrument(level = "debug", skip(ctx), fields(ctx=ctx.log().as_value()))]
    async fn handle(
        &mut self,
        message: DeleteMessages,
        ctx: &mut ActorContext,
    ) -> <DeleteMessages as Message>::Result {
        // let storage_key = message.storage_key.as_ref();

        let mut tx = sqlx::Acquire::begin(&self.pool).await?;
        let query = match message.sequence {
            SequenceRange::Single(sequence) => sqlx::query(self.sql_query.delete_event())
                .bind(&message.storage_key)
                .bind(sequence),

            SequenceRange::From(from_sequence) => {
                sqlx::query(self.sql_query.delete_latest_events())
                    .bind(&message.storage_key)
                    .bind(from_sequence)
            }

            SequenceRange::Exclusive(sequence_range) => {
                sqlx::query(self.sql_query.delete_events_range())
                    .bind(&message.storage_key)
                    .bind(sequence_range.start)
                    .bind(sequence_range.end)
            }

            SequenceRange::All => sqlx::query(self.sql_query.delete_latest_events())
                .bind(&message.storage_key)
                .bind(0),
        };

        let _ = query.execute(&mut tx).await?;

        if let Err(error) = tx.commit().await {
            error!(
                storage_key=%message.storage_key,
                "postgres journal failed to commit delete messages transaction: {error:?}"
            );
            return Err(error.into());
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<DeleteAll> for PostgresJournal {
    #[instrument(level = "debug", skip(ctx), fields(ctx=ctx.log().as_value()))]
    async fn handle(
        &mut self,
        message: DeleteAll,
        ctx: &mut ActorContext,
    ) -> <DeleteAll as Message>::Result {
        // let mut results = Vec::with_capacity(message.0.len());
        for storage_key in message.0 {
            let cmd = DeleteMessages {
                storage_key,
                sequence: SequenceRange::All,
            };
            ctx.actor_ref::<Self>().send(cmd).await??;
            // let handle = tokio::spawn(async move { s.delete_all(storage_key.as_ref()).await });
            // results.push(handle);
        }
        // futures::future::join_all(results)
        //     .await
        //     .into_iter()
        //     .flatten()
        //     .find(|r| r.is_err())
        //     .unwrap_or(Ok(()))
        Ok(())
    }
}
