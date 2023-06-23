use crate::postgres::config;
use crate::postgres::projection_storage::actor::PostgresProjectionStorageActor;
use crate::postgres::{PostgresStorageConfig, PostgresStorageError};
use crate::projection::{
    AggregateOffsets, Offset, PersistenceId, ProjectionError, ProjectionStorage, META_OFFSET_TABLE,
    META_PERSISTENCE_ID, META_PROJECTION_NAME, META_VIEW_TABLE,
};
use coerce::actor::system::ActorSystem;
use coerce::actor::{IntoActor, LocalActorRef};
use serde::de::DeserializeOwned;
use serde::Serialize;
use smol_str::SmolStr;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ProjectionEntry {
    pub bytes: Arc<Vec<u8>>,
}

/// A Postgres-backed view storage for use in a GenericViewProcessor.
#[derive(Debug)]
pub struct PostgresProjectionStorage<V> {
    name: SmolStr,
    view_storage_table: SmolStr,
    offset_storage_table: SmolStr,
    storage: LocalActorRef<PostgresProjectionStorageActor>,
    _marker: PhantomData<V>,
}

impl<V> PostgresProjectionStorage<V> {
    #[instrument(level = "trace", skip(config, system,))]
    pub async fn new(
        name: &str,
        view_storage_table: &str,
        offset_storage_table: &str,
        config: &PostgresStorageConfig,
        system: &ActorSystem,
    ) -> Result<Self, PostgresStorageError> {
        static POSTGRES_PROJECTION_STORAGE_COUNTER: AtomicU32 = AtomicU32::new(1);
        let name = SmolStr::new(name);
        let connection_pool = config::connect_with(config);
        let storage = PostgresProjectionStorageActor::new(
            connection_pool,
            view_storage_table,
            offset_storage_table,
        )
        .into_actor(
            Some(format!(
                "postgres-projection-storage-{}",
                POSTGRES_PROJECTION_STORAGE_COUNTER.fetch_add(1, Ordering::Relaxed)
            )),
            system,
        )
        .await?;

        Ok(Self {
            name,
            view_storage_table: SmolStr::new(view_storage_table),
            offset_storage_table: SmolStr::new(offset_storage_table),
            storage,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    fn view_key_for(&self, persistence_id: &PersistenceId) -> String {
        format!("{}::{persistence_id:#}", self.name)
    }
}

#[async_trait]
impl<V> ProjectionStorage for PostgresProjectionStorage<V>
where
    V: Serialize + DeserializeOwned + Debug + Default + Clone + Send + Sync,
{
    type ViewId = PersistenceId;
    type Projection = V;

    fn name(&self) -> &str {
        self.name.as_str()
    }

    #[instrument(
        level = "debug",
        skip(self),
        fields(
            projection_name=%self.name,
            view_storage_table=%self.view_storage_table,
            offset_storage_table=%self.offset_storage_table,
        )
    )]
    async fn load_projection(
        &self,
        view_id: &Self::ViewId,
    ) -> Result<Option<Self::Projection>, ProjectionError> {
        let projection_entry =
            actor::protocol::load_projection(&self.storage, &self.view_key_for(view_id))
                .await
                .map_err(|err| ProjectionError::Storage {
                    cause: err.into(),
                    meta: maplit::hashmap! {
                        META_VIEW_TABLE.to_string() => self.view_storage_table.to_string(),
                        META_PERSISTENCE_ID.to_string() => view_id.to_string(),
                        META_PROJECTION_NAME.to_string() => self.name.to_string(),
                    },
                })?;

        let projection = projection_entry
            .as_ref()
            .map(|e| {
                bincode::serde::decode_from_slice(e.bytes.as_slice(), bincode::config::standard())
                    .map(|(projection, _size)| projection)
            })
            .transpose()?;

        debug!(
            ?projection_entry,
            ?projection,
            "loaded projection for {view_id}"
        );
        Ok(projection)
    }

    #[instrument(
        level = "debug",
        skip(self),
        fields(
            projection_name=%self.name,
            view_storage_table=%self.view_storage_table,
            offset_storage_table=%self.offset_storage_table,
        )
    )]
    async fn save_projection(
        &self,
        view_id: &Self::ViewId,
        projection: Option<Self::Projection>,
        last_offset: Offset,
    ) -> Result<(), ProjectionError> {
        let bytes = projection
            .map(|p| bincode::serde::encode_to_vec(p, bincode::config::standard()))
            .transpose()?;
        let bytes = bytes.map(Arc::new);
        let entry = bytes.map(|b| ProjectionEntry { bytes: b });
        let query_result = actor::protocol::save_projection(
            &self.storage,
            &self.name,
            view_id,
            &self.view_key_for(view_id),
            entry,
            last_offset,
        )
        .await
        .map_err(|err| ProjectionError::Storage {
            cause: err.into(),
            meta: maplit::hashmap! {
                META_VIEW_TABLE.to_string() => self.view_storage_table.to_string(),
                META_OFFSET_TABLE.to_string() => self.offset_storage_table.to_string(),
                META_PERSISTENCE_ID.to_string() => view_id.to_string(),
                META_PROJECTION_NAME.to_string() => self.name.to_string(),
            },
        })?;
        debug!(?query_result, "save_projection completed");
        Ok(())
    }

    #[instrument(level = "debug", skip(self))]
    async fn read_all_offsets(
        &self,
        projection_name: &str,
    ) -> Result<AggregateOffsets, ProjectionError> {
        actor::protocol::load_all_offsets(&self.storage, projection_name)
            .await
            .map_err(|err| ProjectionError::Storage {
                cause: err.into(),
                meta: maplit::hashmap! {
                    META_OFFSET_TABLE.to_string() => self.offset_storage_table.to_string(),
                    META_PROJECTION_NAME.to_string() => projection_name.to_string(),
                },
            })
    }

    async fn read_offset(
        &self,
        projection_name: &str,
        persistence_id: &PersistenceId,
    ) -> Result<Option<Offset>, ProjectionError> {
        actor::protocol::load_offset(&self.storage, projection_name, persistence_id)
            .await
            .map_err(|err| ProjectionError::Storage {
                cause: err.into(),
                meta: maplit::hashmap! {
                    META_OFFSET_TABLE.to_string() => self.offset_storage_table.to_string(),
                    META_PROJECTION_NAME.to_string() => projection_name.to_string(),
                    META_PERSISTENCE_ID.to_string() => persistence_id.as_persistence_id(),
                },
            })
    }
}

mod actor {
    pub mod protocol {
        use crate::postgres::projection_storage::actor::PostgresProjectionStorageActor;
        use crate::postgres::projection_storage::ProjectionEntry;
        use crate::postgres::PostgresStorageError;
        use crate::projection::{Offset, PersistenceId};
        use coerce::actor::message::Message;
        use coerce::actor::LocalActorRef;
        use sqlx::postgres::PgQueryResult;
        use std::collections::HashMap;

        #[instrument(level = "debug", name = "protocol::load_projection")]
        pub async fn load_projection(
            actor: &LocalActorRef<PostgresProjectionStorageActor>,
            view_key: &str,
        ) -> Result<Option<ProjectionEntry>, PostgresStorageError> {
            let command = LoadProjection::new(view_key);
            actor.send(command).await?
        }

        #[instrument(level = "debug", name = "protocol::save_projection")]
        pub async fn save_projection(
            actor: &LocalActorRef<PostgresProjectionStorageActor>,
            projection_name: &str,
            persistence_id: &PersistenceId,
            view_key: &str,
            entry: Option<ProjectionEntry>,
            last_offset: Offset,
        ) -> Result<SavedProjectOutcome, PostgresStorageError> {
            let command = SaveProjection::new(
                projection_name,
                persistence_id,
                view_key,
                entry,
                last_offset,
            );
            actor.send(command).await?
        }

        #[instrument(level = "debug", name = "protocol::load_all_offsets")]
        pub async fn load_all_offsets(
            actor: &LocalActorRef<PostgresProjectionStorageActor>,
            projection_name: &str,
        ) -> Result<HashMap<PersistenceId, Offset>, PostgresStorageError> {
            let command = LoadAllOffsets::new(projection_name);
            debug!(
                ?actor,
                "DMR: BEFORE actor valid?[{}]: {:?}",
                actor.is_valid(),
                actor.status().await
            );
            let result = actor.send(command).await;
            debug!(
                ?actor,
                ?result,
                "DMR: AFTER actor valid?[{}]: {:?}",
                actor.is_valid(),
                actor.status().await
            );
            result?
        }

        #[instrument(level = "debug", name = "protocol::load_offset")]
        pub async fn load_offset(
            actor: &LocalActorRef<PostgresProjectionStorageActor>,
            projection_name: &str,
            persistence_id: &PersistenceId,
        ) -> Result<Option<Offset>, PostgresStorageError> {
            let command = LoadOffset::new(projection_name, persistence_id.clone());
            actor.send(command).await?
        }

        #[derive(Debug)]
        pub struct LoadProjection {
            pub view_key: String,
        }

        impl LoadProjection {
            pub fn new(view_key: &str) -> Self {
                Self {
                    view_key: view_key.to_string(),
                }
            }
        }

        impl Message for LoadProjection {
            type Result = Result<Option<ProjectionEntry>, PostgresStorageError>;
        }

        #[derive(Debug)]
        pub struct SaveProjection {
            pub projection_name: String,
            pub persistence_id: PersistenceId,
            pub view_key: String,
            pub entry: Option<ProjectionEntry>,
            pub last_offset: Offset,
        }

        impl SaveProjection {
            pub fn new(
                projection_name: &str,
                persistence_id: &PersistenceId,
                view_key: &str,
                entry: Option<ProjectionEntry>,
                last_offset: Offset,
            ) -> Self {
                Self {
                    projection_name: projection_name.to_string(),
                    persistence_id: persistence_id.clone(),
                    view_key: view_key.to_string(),
                    entry,
                    last_offset,
                }
            }
        }

        #[derive(Debug)]
        pub struct SavedProjectOutcome {
            pub projection: Option<PgQueryResult>,
            pub offset: PgQueryResult,
        }

        impl Message for SaveProjection {
            type Result = Result<SavedProjectOutcome, PostgresStorageError>;
        }

        #[derive(Debug)]
        pub struct LoadAllOffsets(String);

        impl LoadAllOffsets {
            pub fn new(projection_name: &str) -> Self {
                Self(projection_name.to_string())
            }

            pub fn projection_name(&self) -> &str {
                self.0.as_str()
            }
        }

        impl Message for LoadAllOffsets {
            type Result = Result<HashMap<PersistenceId, Offset>, PostgresStorageError>;
        }

        #[derive(Debug)]
        pub struct LoadOffset {
            pub projection_name: String,
            pub persistence_id: PersistenceId,
        }

        impl LoadOffset {
            pub fn new(projection_name: &str, persistence_id: PersistenceId) -> Self {
                Self {
                    projection_name: projection_name.to_string(),
                    persistence_id,
                }
            }
        }

        impl Message for LoadOffset {
            type Result = Result<Option<Offset>, PostgresStorageError>;
        }
    }

    use super::sql_query::ProjectionStorageSqlQueryFactory;
    use crate::postgres::projection_storage::actor::protocol::{
        LoadAllOffsets, LoadOffset, LoadProjection, SaveProjection, SavedProjectOutcome,
    };
    use crate::postgres::projection_storage::ProjectionEntry;
    use crate::postgres::PostgresStorageError;
    use crate::projection::{Offset, PersistenceId};
    use coerce::actor::context::ActorContext;
    use coerce::actor::message::{Handler, Message};
    use coerce::actor::Actor;
    use iso8601_timestamp::Timestamp;
    use sqlx::postgres::PgRow;
    use sqlx::{PgPool, Row};
    use std::sync::Arc;

    #[derive(Debug)]
    pub struct PostgresProjectionStorageActor {
        pool: PgPool,
        sql_query: ProjectionStorageSqlQueryFactory,
    }

    impl PostgresProjectionStorageActor {
        pub fn new(
            pool: PgPool,
            projection_storage_table: &str,
            offset_storage_table: &str,
        ) -> Self {
            let sql_query = ProjectionStorageSqlQueryFactory::new(
                projection_storage_table,
                offset_storage_table,
            );
            Self { pool, sql_query }
        }
    }

    #[async_trait]
    impl Actor for PostgresProjectionStorageActor {}

    impl PostgresProjectionStorageActor {
        fn entry_from_row(&self, row: PgRow) -> ProjectionEntry {
            let bytes = Arc::new(row.get(self.sql_query.payload_column()));
            ProjectionEntry { bytes }
        }

        fn offset_from_row(&self, row: PgRow) -> Offset {
            let current_offset = row.get(self.sql_query.current_offset_column());

            // UNIX_EPOCH is not great alt to unparseable ts. Value not used for processing,
            // but if it is, then other valid offsets would be preferred over UNIX_EPOCH
            let last_updated_at: i64 = row.get("last_updated_at");
            let last_updated_at = crate::util::timestamp_from_seconds(last_updated_at)
                .unwrap_or(Timestamp::UNIX_EPOCH);

            Offset::from_parts(last_updated_at, current_offset)
        }
    }

    #[async_trait]
    impl Handler<LoadProjection> for PostgresProjectionStorageActor {
        #[instrument(level = "debug", skip(self, _ctx))]
        async fn handle(
            &mut self,
            message: LoadProjection,
            _ctx: &mut ActorContext,
        ) -> Result<Option<ProjectionEntry>, PostgresStorageError> {
            sqlx::query(self.sql_query.select_latest_view())
                .bind(&message.view_key)
                .map(|row| self.entry_from_row(row))
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| err.into())
        }
    }

    #[async_trait]
    impl Handler<SaveProjection> for PostgresProjectionStorageActor {
        #[instrument(level = "debug", skip(self, _ctx))]
        async fn handle(
            &mut self,
            message: SaveProjection,
            _ctx: &mut ActorContext,
        ) -> <SaveProjection as Message>::Result {
            let projection_name = message.projection_name;
            let view_key = message.view_key;
            let persistence_id = message.persistence_id.as_persistence_id();
            // let projection_id = &persistence_id;
            let last_offset = message.last_offset;

            let mut tx = sqlx::Acquire::begin(&self.pool).await?;

            let now = crate::util::now_timestamp();

            let save_projection_result = match message.entry {
                Some(entry) => Some(
                    sqlx::query(self.sql_query.update_or_insert_view())
                        .bind(&view_key)
                        .bind(entry.bytes.to_vec())
                        .bind(now)
                        .bind(now)
                        .execute(&mut tx)
                        .await
                        .map_err(|err| err.into()),
                ),

                None => None,
            }
            .transpose();

            let save_offset_result = sqlx::query(self.sql_query.update_or_insert_offset())
                .bind(&projection_name)
                .bind(&persistence_id)
                .bind(last_offset.as_i64())
                .bind(crate::util::timestamp_seconds(last_offset.timestamp()))
                .execute(&mut tx)
                .await
                .map_err(|err| PostgresStorageError::Storage(err.into()));

            debug!(
                ?save_projection_result,
                ?save_offset_result,
                "DMR: save_projection query submitted."
            );

            if let Err(error) = tx.commit().await {
                error!(
                    %projection_name, %persistence_id, %view_key, ?last_offset,
                    "postgres projection storage failed to commit save projection transaction: {error:?}"
                );
                return Err(error.into());
            }

            let result = save_projection_result.and_then(|projection| {
                save_offset_result.map(|offset| SavedProjectOutcome { projection, offset })
            });
            debug!("DMR: save_projection committed: {result:?}");
            result
        }
    }

    #[async_trait]
    impl Handler<LoadAllOffsets> for PostgresProjectionStorageActor {
        #[instrument(level = "debug", skip(self, _ctx))]
        async fn handle(
            &mut self,
            message: LoadAllOffsets,
            _ctx: &mut ActorContext,
        ) -> <LoadAllOffsets as Message>::Result {
            let projection_name = message.projection_name();
            let persistence_id_col = self.sql_query.persistence_id_column();
            let sql = self.sql_query.select_all_offsets();
            debug!(%projection_name, %persistence_id_col, %sql, "DMR-AAA");

            let sql_result = sqlx::query(sql)
                .bind(projection_name)
                .map(|row: PgRow| {
                    let id: PersistenceId = row.get(persistence_id_col);
                    debug!(persistence_id=%id, "DMR-BBB");
                    let offset = self.offset_from_row(row);
                    debug!(?offset, "DMR-CCC");
                    (id, offset)
                })
                .fetch_all(&self.pool)
                .await;
            debug!(?sql_result, "DMR-DDD");
            let result = sql_result
                .map(|results| {
                    debug!(
                        ?results,
                        "DMR: read all offsets for {}",
                        message.projection_name()
                    );
                    results.into_iter().collect()
                })
                .map_err(|err| {
                    error!(
                        ?err,
                        "DMR: failed reading all offsets for {}",
                        message.projection_name()
                    );
                    err.into()
                });

            debug!(?result, "DMR-EEE");
            result
        }
    }

    #[async_trait]
    impl Handler<LoadOffset> for PostgresProjectionStorageActor {
        #[instrument(level = "debug", skip(self, _ctx))]
        async fn handle(
            &mut self,
            message: LoadOffset,
            _ctx: &mut ActorContext,
        ) -> <LoadOffset as Message>::Result {
            sqlx::query(self.sql_query.select_offset())
                .bind(message.projection_name)
                .bind(message.persistence_id.as_persistence_id())
                .map(|row| self.offset_from_row(row))
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| err.into())
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
    const PAYLOAD_COL: &str = "payload";
    const CREATED_AT_COL: &str = "created_at";
    const LAST_UPDATED_AT_COL: &str = "last_updated_at";

    const PROJECTION_STORAGE_COLUMNS: [&str; 4] = [
        PROJECTION_ID_COL,
        PAYLOAD_COL,
        CREATED_AT_COL,
        LAST_UPDATED_AT_COL,
    ];
    static PROJECTION_STORAGE_COLUMNS_REP: Lazy<String> =
        Lazy::new(|| PROJECTION_STORAGE_COLUMNS.join(", "));
    static PROJECTION_STORAGE_VALUES_REP: Lazy<String> = Lazy::new(|| {
        let values = (1..=PROJECTION_STORAGE_COLUMNS.len())
            .map(|i| format!("${i}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("( {values} )")
    });

    const CURRENT_OFFSET_COL: &str = "current_offset";
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

    pub struct ProjectionStorageSqlQueryFactory {
        projection_storage_table: SmolStr,
        offset_storage_table: SmolStr,
        select_view: OnceCell<String>,
        update_or_insert_view: OnceCell<String>,
        select_all_offsets: OnceCell<String>,
        select_offset: OnceCell<String>,
        update_or_insert_offset: OnceCell<String>,
        where_projection_id: OnceCell<String>,
        where_projection_and_aggregate: OnceCell<String>,
    }

    impl fmt::Debug for ProjectionStorageSqlQueryFactory {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("ProjectionStorageSqlQueryFactory").finish()
        }
    }

    impl ProjectionStorageSqlQueryFactory {
        pub fn new(projection_storage_table: &str, offset_storage_table: &str) -> Self {
            Self {
                projection_storage_table: SmolStr::new(projection_storage_table),
                offset_storage_table: SmolStr::new(offset_storage_table),
                select_view: OnceCell::new(),
                update_or_insert_view: OnceCell::new(),
                select_all_offsets: OnceCell::new(),
                select_offset: OnceCell::new(),
                update_or_insert_offset: OnceCell::new(),
                where_projection_id: OnceCell::new(),
                where_projection_and_aggregate: OnceCell::new(),
            }
        }

        fn where_projection_id(&self) -> &str {
            self.where_projection_id.get_or_init(|| {
                format!(
                    "{projection_id} = $1",
                    projection_id = self.projection_id_column()
                )
            })
        }

        fn where_projection_and_aggregate(&self) -> &str {
            self.where_projection_and_aggregate.get_or_init(|| {
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

        #[allow(dead_code, clippy::missing_const_for_fn)]
        #[inline]
        pub fn payload_column(&self) -> &str {
            PAYLOAD_COL
        }

        #[inline]
        pub fn select_latest_view(&self) -> &str {
            self.select_view.get_or_init(|| {
                sql::Select::new()
                    .select(&PROJECTION_STORAGE_COLUMNS_REP)
                    .from(self.projection_storage_table.as_str())
                    .where_clause(self.where_projection_id())
                    .order_by(format!("{LAST_UPDATED_AT_COL} desc").as_str())
                    .limit("1")
                    .to_string()
            })
        }

        #[inline]
        pub fn update_or_insert_view(&self) -> &str {
            self.update_or_insert_view.get_or_init(|| {
                let payload_col = self.payload_column();
                let update_clause = sql::Update::new()
                    .set(format!("{payload_col} = EXCLUDED.{payload_col}, last_updated_at = EXCLUDED.last_updated_at").as_str())
                    .to_string();

                let conflict_clause = format!("( {primary_key} ) DO UPDATE {update_clause}", primary_key = self.projection_id_column());

                sql::Insert::new()
                    .insert_into(
                        format!(
                            "{table} ( {columns} )",
                            table = self.projection_storage_table, columns = PROJECTION_STORAGE_COLUMNS_REP.as_str(),
                        )
                        .as_str(),
                    )
                    .values(PROJECTION_STORAGE_VALUES_REP.as_str())
                    .on_conflict(&conflict_clause)
                    .to_string()
            })
        }

        #[inline]
        pub fn select_all_offsets(&self) -> &str {
            self.select_all_offsets.get_or_init(|| {
                sql::Select::new()
                    .select(&OFFSET_COLUMNS_REP)
                    .from(self.offset_storage_table.as_str())
                    .where_clause(self.where_projection_id())
                    .order_by(format!("{CURRENT_OFFSET_COL} desc").as_str())
                    .to_string()
            })
        }

        #[inline]
        pub fn select_offset(&self) -> &str {
            self.select_offset.get_or_init(|| {
                sql::Select::new()
                    .select(&OFFSET_COLUMNS_REP)
                    .from(self.offset_storage_table.as_str())
                    .where_clause(self.where_projection_and_aggregate())
                    .order_by(format!("{CURRENT_OFFSET_COL} desc").as_str())
                    .limit("1")
                    .to_string()
            })
        }

        #[inline]
        pub fn update_or_insert_offset(&self) -> &str {
            self.update_or_insert_offset.get_or_init(|| {
                let offset_col = self.current_offset_column();
                let update_clause = sql::Update::new()
                    .set(format!("{offset_col} = EXCLUDED.{offset_col}, last_updated_at = EXCLUDED.last_updated_at").as_str())
                    .to_string();

                let conflict_clause = format!(
                    "( {projection}, {persistence} ) DO UPDATE {update_clause}",
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
