use crate::postgres::sql_query::SqlQueryFactory;
use crate::postgres::{PostgresStorageConfig, PostgresStorageError};
use crate::projection::PostCommitAction;
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::Actor;
use coerce::persistent::journal::storage::JournalEntry;
use protocol::*;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::sync::Arc;
use tokio::sync::oneshot;
use valuable::Valuable;

pub mod protocol {
    use super::*;
    use crate::postgres::{PostgresStorageError, StorageKey};

    #[derive(Debug)]
    pub struct WriteMessage {
        pub storage_key: StorageKey,
        pub entry: JournalEntry,
        pub result_channel: oneshot::Sender<anyhow::Result<()>>,
    }

    impl Message for WriteMessage {
        type Result = Result<(), PostgresStorageError>;
    }

    #[derive(Debug)]
    pub struct WriteMessages {
        pub storage_key: StorageKey,
        pub entries: Vec<JournalEntry>,
        pub result_channel: oneshot::Sender<anyhow::Result<()>>,
    }

    impl Message for WriteMessages {
        type Result = Result<(), PostgresStorageError>;
    }

    #[derive(Debug)]
    pub struct WriteSnapshot {
        pub storage_key: StorageKey,
        pub entry: JournalEntry,
        pub result_channel: oneshot::Sender<anyhow::Result<()>>,
    }

    impl Message for WriteSnapshot {
        type Result = Result<(), PostgresStorageError>;
    }

    #[derive(Debug)]
    pub struct ReadSnapshot {
        pub storage_key: StorageKey,
        pub result_channel: oneshot::Sender<anyhow::Result<Option<JournalEntry>>>,
    }

    impl Message for ReadSnapshot {
        type Result = Result<(), PostgresStorageError>;
    }

    #[derive(Debug, PartialEq, Eq)]
    pub enum SequenceRange {
        Single(i64),
        From(i64),
        Exclusive(std::ops::Range<i64>),
        All,
    }

    impl SequenceRange {
        pub const fn single(sequence: i64) -> Self {
            Self::Single(sequence)
        }
        pub const fn from_sequence(sequence: i64) -> Self {
            Self::From(sequence)
        }
        pub const fn until(sequence: i64) -> Self {
            Self::for_exclusive_range(0..sequence)
        }
        pub const fn for_exclusive_range(range: std::ops::Range<i64>) -> Self {
            Self::Exclusive(range)
        }
    }

    // #[derive(Debug)]
    // pub struct ReadMessage {
    //     pub storage_key: StorageKey,
    //     pub sequence_id: i64,
    //     pub result_channel: oneshot::Sender<anyhow::Result<Option<JournalEntry>>>,
    // }
    //
    // impl Message for ReadMessage {
    //     type Result = Result<(), PostgresStorageError>;
    // }

    #[derive(Debug)]
    pub struct ReadMessages {
        pub storage_key: StorageKey,
        pub sequence: SequenceRange,
        // pub from_sequence: i64,
        // pub to_sequence: Option<i64>,
        pub result_channel: oneshot::Sender<anyhow::Result<Option<Vec<JournalEntry>>>>,
    }

    impl Message for ReadMessages {
        type Result = Result<(), PostgresStorageError>;
    }

    #[derive(Debug)]
    pub struct DeleteMessages {
        pub storage_key: StorageKey,
        pub sequence: SequenceRange,
        pub result_channel: oneshot::Sender<anyhow::Result<()>>,
    }

    impl Message for DeleteMessages {
        type Result = Result<(), PostgresStorageError>;
    }

    #[derive(Debug)]
    pub struct DeleteAll(pub Vec<StorageKey>);

    impl Message for DeleteAll {
        type Result = Result<(), PostgresStorageError>;
    }
}

#[derive(Debug)]
pub struct PostgresJournal {
    pool: PgPool,
    post_commit_actions: Vec<Box<dyn PostCommitAction>>,
    sql_query: SqlQueryFactory,
}

impl PostgresJournal {
    #[allow(dead_code)]
    pub fn new(
        pool: PgPool,
        config: &PostgresStorageConfig,
        post_commit_actions: Vec<Box<dyn PostCommitAction>>,
    ) -> Self {
        let sql_query = SqlQueryFactory::new(&config.event_journal_table_name)
            .with_snapshots_table(&config.snapshot_table_name);

        Self {
            pool,
            post_commit_actions,
            sql_query,
        }
    }

    #[allow(dead_code)]
    pub fn from_pool(pool: PgPool, config: &PostgresStorageConfig) -> Self {
        Self::new(pool, config, vec![])
    }
}

#[async_trait]
impl Actor for PostgresJournal {
    #[instrument(level = "trace", skip(ctx), fields(ctx=ctx.log().as_value()))]
    async fn started(&mut self, ctx: &mut ActorContext) {}

    #[instrument(level = "trace", skip(ctx), fields(ctx=ctx.log().as_value()))]
    async fn stopped(&mut self, ctx: &mut ActorContext) {}
}

fn created_at() -> i64 {
    iso8601_timestamp::Timestamp::now_utc()
        .duration_since(iso8601_timestamp::Timestamp::UNIX_EPOCH)
        .whole_seconds()
}

const EMPTY_META: serde_json::Value = serde_json::Value::Null;

impl PostgresJournal {
    #[instrument(level = "debug", skip(tx, ctx), fields(ctx=ctx.log().as_value()))]
    async fn persist_message(
        &self,
        persistence_id: &str,
        entry: JournalEntry,
        tx: &mut Transaction<'_, Postgres>,
        ctx: &mut ActorContext,
    ) -> Result<(), PostgresStorageError> {
        let _query_result = sqlx::query(self.sql_query.append_event())
            .bind(persistence_id) // persistence_id
            .bind(entry.sequence) // sequence_number
            .bind(false) // is_deleted
            .bind(entry.payload_type.as_ref()) // event_manifest
            .bind(entry.bytes.to_vec()) // event_payload
            .bind(EMPTY_META.clone()) // meta_payload
            .bind(created_at()) // created_at
            .execute(tx)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<WriteMessage> for PostgresJournal {
    #[instrument(level = "debug", skip(ctx), fields(ctx=ctx.log().as_value()))]
    async fn handle(
        &mut self,
        message: WriteMessage,
        ctx: &mut ActorContext,
    ) -> Result<(), PostgresStorageError> {
        let mut tx = sqlx::Acquire::begin(&self.pool).await?;
        self.persist_message(message.storage_key.as_ref(), message.entry, &mut tx, ctx)
            .await?;
        let result = tx.commit().await.map_err(|err| err.into());
        let _ = message.result_channel.send(result);
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
    ) -> Result<(), PostgresStorageError> {
        let persistence_id = message.storage_key.as_ref();
        let mut tx = sqlx::Acquire::begin(&self.pool).await?;
        for entry in message.entries {
            self.persist_message(persistence_id, entry, &mut tx, ctx)
                .await?;
        }
        let result = tx.commit().await.map_err(|err| err.into());
        let _ = message.result_channel.send(result);
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
    ) -> Result<(), PostgresStorageError> {
        let mut tx = sqlx::Acquire::begin(&self.pool).await?;
        let _query_result = sqlx::query(self.sql_query.insert_snapshot())
            .bind(message.storage_key.as_ref()) // persistence_id
            .bind(message.entry.sequence) // sequence_number
            .bind(message.entry.payload_type.as_ref()) // snapshot_manifest
            .bind(message.entry.bytes.to_vec()) // snapshot_payload
            .bind(EMPTY_META.clone()) // meta_payload
            .bind(created_at()) // created_at
            .execute(&mut tx)
            .await?;
        let result = tx.commit().await.map_err(|err| err.into());
        let _ = message.result_channel.send(result);
        Ok(())
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
    ) -> Result<(), PostgresStorageError> {
        let result = sqlx::query(self.sql_query.select_snapshot())
            .bind(message.storage_key.as_ref())
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| err.into())
            .map(|row| {
                row.map(|r| {
                    self.entry_from_row(
                        r,
                        self.sql_query.snapshot_manifest_column(),
                        self.sql_query.snapshot_payload_column(),
                    )
                })
            });

        let _ = message.result_channel.send(result);
        Ok(())
    }
}

// #[async_trait]
// impl Handler<ReadMessage> for PostgresJournal {
//     #[instrument(level = "debug", skip(_ctx))]
//     async fn handle(&mut self, message: ReadMessage, _ctx: &mut ActorContext) -> Result<(), PostgresStorageError> {
//         let result = sqlx::query(self.sql_query.select_event())
//             .bind(message.storage_key.as_ref())
//             .bind(message.sequence_id)
//             .fetch_optional(&self.pool)
//             .await
//             .map_err(|err| err.into())
//             .map(|row| row.map(|r| self.entry_from_row(r, self.sql_query.event_manifest_column(), self.sql_query.event_payload_column())))
//         ;
//
//         let _ = message.result_channel.send(result);
//         Ok(())
//     }
// }

#[async_trait]
impl Handler<ReadMessages> for PostgresJournal {
    #[instrument(level = "debug", skip(ctx), fields(ctx=ctx.log().as_value()))]
    async fn handle(
        &mut self,
        message: ReadMessages,
        ctx: &mut ActorContext,
    ) -> Result<(), PostgresStorageError> {
        let persistence_id = message.storage_key.as_ref();

        let query = match message.sequence {
            SequenceRange::Single(sequence) => sqlx::query(self.sql_query.select_event())
                .bind(persistence_id)
                .bind(sequence),

            SequenceRange::From(from_sequence) => {
                sqlx::query(self.sql_query.select_latest_events())
                    .bind(persistence_id)
                    .bind(from_sequence)
            }

            SequenceRange::Exclusive(sequence_range) => {
                sqlx::query(self.sql_query.select_events_range())
                    .bind(persistence_id)
                    .bind(sequence_range.start)
                    .bind(sequence_range.end)
            }

            SequenceRange::All => sqlx::query(self.sql_query.select_latest_events())
                .bind(persistence_id)
                .bind(0),
        };

        let result = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| err.into())
            .map(|rows| {
                let entries = rows
                    .into_iter()
                    .map(|r| {
                        self.entry_from_row(
                            r,
                            self.sql_query.event_manifest_column(),
                            self.sql_query.event_payload_column(),
                        )
                    })
                    .collect();

                Some(entries)
            });

        let _ = message.result_channel.send(result);
        Ok(())
    }
}

#[async_trait]
impl Handler<DeleteMessages> for PostgresJournal {
    #[instrument(level = "debug", skip(ctx), fields(ctx=ctx.log().as_value()))]
    async fn handle(
        &mut self,
        message: DeleteMessages,
        ctx: &mut ActorContext,
    ) -> Result<(), PostgresStorageError> {
        let persistence_id = message.storage_key.as_ref();

        let mut tx = sqlx::Acquire::begin(&self.pool).await?;
        let query = match message.sequence {
            SequenceRange::Single(sequence) => sqlx::query(self.sql_query.delete_event())
                .bind(persistence_id)
                .bind(sequence),

            SequenceRange::From(from_sequence) => {
                sqlx::query(self.sql_query.delete_latest_events())
                    .bind(persistence_id)
                    .bind(from_sequence)
            }

            SequenceRange::Exclusive(sequence_range) => {
                sqlx::query(self.sql_query.delete_events_range())
                    .bind(persistence_id)
                    .bind(sequence_range.start)
                    .bind(sequence_range.end)
            }

            SequenceRange::All => sqlx::query(self.sql_query.delete_latest_events())
                .bind(persistence_id)
                .bind(0),
        };

        let _query_result = query.execute(&mut tx).await?;
        let result = tx.commit().await.map_err(|err| err.into());

        let _ = message.result_channel.send(result);
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
    ) -> Result<(), PostgresStorageError> {
        // let mut results = Vec::with_capacity(message.0.len());
        for storage_key in message.0 {
            let (cmd_tx, _) = oneshot::channel();
            let cmd = DeleteMessages {
                storage_key,
                sequence: SequenceRange::All,
                result_channel: cmd_tx,
            };
            ctx.actor_ref::<Self>().notify(cmd)?;
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
