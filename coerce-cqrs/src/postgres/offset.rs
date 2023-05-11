use crate::postgres::offset::actor::PostgresOffsetStorageActor;
use crate::postgres::{config, PostgresStorageConfig, PostgresStorageError};
use crate::projection::{
    Offset, OffsetStorage, PersistenceId, ProjectionError, ProjectionId, META_PERSISTENCE_ID,
    META_PROJECTION_ID, META_TABLE,
};
use coerce::actor::system::ActorSystem;
use coerce::actor::{IntoActor, LocalActorRef};
use smol_str::SmolStr;
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Debug)]
pub struct PostgresOffsetStorage {
    storage: LocalActorRef<PostgresOffsetStorageActor>,
    offset_storage_table: SmolStr,
}

impl PostgresOffsetStorage {
    #[instrument(level = "trace", skip(config, system,))]
    pub async fn new(
        offset_storage_table: &str,
        config: &PostgresStorageConfig,
        system: &ActorSystem,
    ) -> Result<Self, PostgresStorageError> {
        static POSTGRES_OFFSET_STORAGE_COUNTER: AtomicU32 = AtomicU32::new(1);
        let connection_pool = config::connect_with(config);
        let storage = PostgresOffsetStorageActor::new(connection_pool, offset_storage_table)
            .into_actor(
                Some(format!(
                    "postgres-offset-storage-{}",
                    POSTGRES_OFFSET_STORAGE_COUNTER.fetch_add(1, Ordering::Relaxed)
                )),
                system,
            )
            .await?;

        Ok(Self {
            storage,
            offset_storage_table: SmolStr::new(offset_storage_table),
        })
    }
}

pub const META_OFFSET: &str = "offset";

#[async_trait]
impl OffsetStorage for PostgresOffsetStorage {
    #[instrument(level = "debug", skip(self))]
    async fn read_offset(
        &self,
        projection_id: &ProjectionId,
        persistence_id: &PersistenceId,
    ) -> Result<Option<Offset>, ProjectionError> {
        actor::protocol::load_offset(&self.storage, projection_id, persistence_id)
            .await
            .map_err(|err| ProjectionError::Storage {
                cause: err.into(),
                meta: maplit::hashmap! {
                    META_TABLE.to_string() => self.offset_storage_table.to_string(),
                    META_PROJECTION_ID.to_string() => projection_id.to_string(),
                    META_PERSISTENCE_ID.to_string() => persistence_id.as_persistence_id(),
                },
            })
    }

    async fn save_offset(
        &self,
        projection_id: &ProjectionId,
        persistence_id: &PersistenceId,
        offset: Offset,
    ) -> Result<(), ProjectionError> {
        let save_result =
            actor::protocol::save_offset(&self.storage, projection_id, persistence_id, offset)
                .await
                .map_err(|err| ProjectionError::Storage {
                    cause: err.into(),
                    meta: maplit::hashmap! {
                        META_TABLE.to_string() => self.offset_storage_table.to_string(),
                        META_PROJECTION_ID.to_string() => projection_id.to_string(),
                        META_PERSISTENCE_ID.to_string() => persistence_id.as_persistence_id(),
                        META_OFFSET.to_string() => offset.to_string(),
                    },
                });

        debug!(
            "postgres offset storage saved projection offset({}): {:?}",
            offset, save_result
        );
        save_result.map(|_| ())
    }
}

mod actor {
    use crate::postgres::offset::actor::protocol::{LoadOffset, SaveOffset};
    use crate::postgres::offset::sql_query::OffsetStorageSqlQueryFactory;
    use crate::postgres::PostgresStorageError;
    use crate::projection::Offset;
    use coerce::actor::context::ActorContext;
    use coerce::actor::message::Handler;
    use coerce::actor::Actor;
    use iso8601_timestamp::Timestamp;
    use smol_str::SmolStr;
    use sqlx::postgres::{PgQueryResult, PgRow};
    use sqlx::{PgPool, Row};

    pub mod protocol {
        use crate::postgres::offset::actor::PostgresOffsetStorageActor;
        use crate::postgres::PostgresStorageError;
        use crate::projection::{Offset, PersistenceId, ProjectionId};
        use coerce::actor::message::Message;
        use coerce::actor::LocalActorRef;
        use sqlx::postgres::PgQueryResult;

        #[instrument(level = "debug", name = "protocol::load_offset")]
        pub async fn load_offset(
            actor: &LocalActorRef<PostgresOffsetStorageActor>,
            projection_id: &ProjectionId,
            persistence_id: &PersistenceId,
        ) -> Result<Option<Offset>, PostgresStorageError> {
            let command = LoadOffset::new(projection_id.clone(), persistence_id.clone());
            actor.send(command).await?
        }

        #[instrument(level = "debug", name = "protocol::save_offset")]
        pub async fn save_offset(
            actor: &LocalActorRef<PostgresOffsetStorageActor>,
            projection_id: &ProjectionId,
            persistence_id: &PersistenceId,
            current_offset: Offset,
        ) -> Result<PgQueryResult, PostgresStorageError> {
            let command = SaveOffset::new(
                projection_id.clone(),
                persistence_id.clone(),
                current_offset,
            );
            actor.send(command).await?
        }

        #[derive(Debug)]
        pub struct LoadOffset {
            pub projection_id: ProjectionId,
            pub persistence_id: PersistenceId,
        }

        impl LoadOffset {
            pub const fn new(projection_id: ProjectionId, persistence_id: PersistenceId) -> Self {
                Self {
                    projection_id,
                    persistence_id,
                }
            }
        }

        impl Message for LoadOffset {
            type Result = Result<Option<Offset>, PostgresStorageError>;
        }

        #[derive(Debug)]
        pub struct SaveOffset {
            pub projection_id: ProjectionId,
            pub persistence_id: PersistenceId,
            pub current_offset: Offset,
        }

        impl SaveOffset {
            pub const fn new(
                projection_id: ProjectionId,
                persistence_id: PersistenceId,
                current_offset: Offset,
            ) -> Self {
                Self {
                    projection_id,
                    persistence_id,
                    current_offset,
                }
            }
        }

        impl Message for SaveOffset {
            type Result = Result<PgQueryResult, PostgresStorageError>;
        }
    }

    #[derive(Debug)]
    pub struct PostgresOffsetStorageActor {
        pool: PgPool,
        sql_query: OffsetStorageSqlQueryFactory,
    }

    impl PostgresOffsetStorageActor {
        pub fn new(pool: PgPool, offset_storage_table: &str) -> Self {
            let offset_storage_table = SmolStr::new(offset_storage_table);
            let sql_query = OffsetStorageSqlQueryFactory::new(&offset_storage_table);
            Self { pool, sql_query }
        }
    }

    #[async_trait]
    impl Actor for PostgresOffsetStorageActor {}

    impl PostgresOffsetStorageActor {
        fn entry_from_row(&self, row: PgRow) -> Offset {
            let current_offset = row.get(self.sql_query.current_offset_column());

            // UNIX_EPOCH is not great alt to unparseable ts. Value not used for processing,
            // but if it is, then other valid offsets would be preferred over UNIX_EPOCH
            let last_updated_at: i64 = row.get("last_updated_at");
            let last_updated_at =
                crate::timestamp_from_seconds(last_updated_at).unwrap_or(Timestamp::UNIX_EPOCH);

            Offset::from_parts(last_updated_at, current_offset)
        }
    }

    #[async_trait]
    impl Handler<LoadOffset> for PostgresOffsetStorageActor {
        #[instrument(level = "debug", skip(self, _ctx))]
        async fn handle(
            &mut self,
            message: LoadOffset,
            _ctx: &mut ActorContext,
        ) -> Result<Option<Offset>, PostgresStorageError> {
            sqlx::query(self.sql_query.select_offset())
                .bind(message.projection_id.as_ref())
                .bind(message.persistence_id.as_persistence_id())
                .map(|row| self.entry_from_row(row))
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| err.into())
        }
    }

    #[async_trait]
    impl Handler<SaveOffset> for PostgresOffsetStorageActor {
        #[instrument(level = "debug", skip(self, _ctx))]
        async fn handle(
            &mut self,
            message: SaveOffset,
            _ctx: &mut ActorContext,
        ) -> Result<PgQueryResult, PostgresStorageError> {
            let projection_id = message.projection_id.as_ref();
            let persistence_id = message.persistence_id.as_persistence_id();
            let offset = message.current_offset;

            let mut tx = sqlx::Acquire::begin(&self.pool).await?;

            let query_result = sqlx::query(self.sql_query.update_or_insert_offset())
                .bind(projection_id)
                .bind(&persistence_id)
                .bind(offset.as_i64())
                .bind(crate::timestamp_seconds(offset.timestamp()))
                .execute(&mut tx)
                .await
                .map_err(|err| PostgresStorageError::Storage(err.into()))?;

            if let Err(error) = tx.commit().await {
                error!(%projection_id, %persistence_id, %offset, "postgres offset storage failed to commit save offset transaction: {error:?}");
                return Err(error.into());
            }

            debug!("save_offset completed: {query_result:?}");
            Ok(query_result)
        }
    }
}

mod sql_query {
    use once_cell::sync::{Lazy, OnceCell};
    use smol_str::SmolStr;
    use sql_query_builder as sql;
    use std::fmt;

    const PROJECTION_ID_COL: &str = "projection_id";
    const PERSISTENCE_ID_COL: &str = "persistence_id";
    const CURRENT_OFFSET_COL: &str = "current_offset";
    const LAST_UPDATED_AT_COL: &str = "last_updated_at";

    const OFFSET_COLUMNS: [&str; 4] = [
        PROJECTION_ID_COL,
        PERSISTENCE_ID_COL,
        CURRENT_OFFSET_COL,
        LAST_UPDATED_AT_COL,
    ];
    static OFFSET_COLUMNS_REP: Lazy<String> = Lazy::new(|| OFFSET_COLUMNS.join(", "));
    static OFFSET_VALUES_REP: Lazy<String> = Lazy::new(|| {
        let values = (1..=OFFSET_COLUMNS.len())
            .map(|i| format!("${i}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("( {values} )")
    });

    pub struct OffsetStorageSqlQueryFactory {
        offset_storage_table: SmolStr,
        select: OnceCell<String>,
        update_or_insert: OnceCell<String>,
        where_projection: OnceCell<String>,
    }

    impl fmt::Debug for OffsetStorageSqlQueryFactory {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("OffsetStorageSqlQueryFactory").finish()
        }
    }

    impl OffsetStorageSqlQueryFactory {
        pub fn new(offset_storage_table: &str) -> Self {
            Self {
                offset_storage_table: SmolStr::new(offset_storage_table),
                select: OnceCell::new(),
                update_or_insert: OnceCell::new(),
                where_projection: OnceCell::new(),
            }
        }

        fn where_projection(&self) -> &str {
            self.where_projection.get_or_init(|| {
                format!(
                    "{projection_id} = $1 AND {persistence_id} = $2",
                    projection_id = self.projection_id_column(),
                    persistence_id = self.persistence_id_column(),
                )
            })
        }

        #[allow(dead_code, clippy::missing_const_for_fn)]
        #[inline]
        pub fn projection_id_column(&self) -> &str {
            PROJECTION_ID_COL
        }

        #[allow(dead_code, clippy::missing_const_for_fn)]
        #[inline]
        pub fn persistence_id_column(&self) -> &str {
            PERSISTENCE_ID_COL
        }

        #[allow(dead_code, clippy::missing_const_for_fn)]
        #[inline]
        pub fn current_offset_column(&self) -> &str {
            CURRENT_OFFSET_COL
        }

        #[inline]
        pub fn select_offset(&self) -> &str {
            self.select.get_or_init(|| {
                sql::Select::new()
                    .select(&OFFSET_COLUMNS_REP)
                    .from(self.offset_storage_table.as_str())
                    .where_clause(self.where_projection())
                    .order_by(format!("{CURRENT_OFFSET_COL} desc").as_str())
                    .limit("1")
                    .to_string()
            })
        }

        #[inline]
        pub fn update_or_insert_offset(&self) -> &str {
            self.update_or_insert.get_or_init(|| {
                let offset_col = self.current_offset_column();
                let update_clause = sql::Update::new()
                    .set(format!("{offset_col} = EXCLUDED.{offset_col}, last_updated_at = EXCLUDED.last_updated_at").as_str())
                    .to_string();

                let conflict_clause = format!(
                    "( {projection}, {persistence} ) DO UPDATE {update_clause}",
                    // persistence = self.persistence_id_column(),
                    projection = self.projection_id_column(), persistence = self.persistence_id_column(),
                );

                sql::Insert::new()
                    .insert_into(
                        format!(
                            "{table} ( {columns} )",
                            table = self.offset_storage_table, columns = OFFSET_COLUMNS_REP.as_str(),
                        )
                            .as_str()
                    )
                    .values(OFFSET_VALUES_REP.as_str())
                    .on_conflict(&conflict_clause)
                    .to_string()
            })
        }
    }
}
