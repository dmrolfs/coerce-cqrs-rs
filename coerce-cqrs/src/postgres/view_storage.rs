use crate::postgres::config;
use crate::postgres::view_storage::actor::PostgresViewStorageActor;
use crate::postgres::{PostgresStorageConfig, PostgresStorageError};
use crate::projection::{
    ProjectionError, ProjectionId, ViewStorage, META_PROJECTION_ID, META_TABLE,
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
pub struct ViewEntry {
    pub bytes: Arc<Vec<u8>>,
}

/// A Postgres-backed view storage for use in a GenericViewProcessor.
#[derive(Debug)]
pub struct PostgresViewStorage<V> {
    view_name: SmolStr,
    view_storage_table: SmolStr,
    storage: LocalActorRef<PostgresViewStorageActor>,
    _marker: PhantomData<V>,
}

impl<V> PostgresViewStorage<V> {
    #[instrument(level = "trace", skip(config, system,))]
    pub async fn new(
        view_name: &str,
        view_storage_table: &str,
        config: &PostgresStorageConfig,
        system: &ActorSystem,
    ) -> Result<Self, PostgresStorageError> {
        static POSTGRES_VIEW_STORAGE_COUNTER: AtomicU32 = AtomicU32::new(1);
        let view_name = SmolStr::new(view_name);
        let view_storage_table = SmolStr::new(view_storage_table);
        let connection_pool = config::connect_with(config);
        let storage = PostgresViewStorageActor::new(connection_pool, &view_storage_table)
            .into_actor(
                Some(format!(
                    "postgres-view-storage-{}",
                    POSTGRES_VIEW_STORAGE_COUNTER.fetch_add(1, Ordering::Relaxed)
                )),
                system,
            )
            .await?;

        Ok(Self {
            view_name,
            view_storage_table,
            storage,
            _marker: PhantomData,
        })
    }
}

const META_VIEW_NAME: &str = "view_name";

#[async_trait]
impl<V> ViewStorage for PostgresViewStorage<V>
where
    V: Serialize + DeserializeOwned + Debug + Default + Clone + Send + Sync,
{
    type View = V;

    fn view_name(&self) -> &str {
        self.view_name.as_str()
    }

    #[instrument(
        level = "debug",
        skip(self),
        fields(view_name=%self.view_name, view_storage_table=%self.view_storage_table)
    )]
    async fn load_view(
        &self,
        view_id: &ProjectionId,
    ) -> Result<Option<Self::View>, ProjectionError> {
        let entry = actor::protocol::load_view(&self.storage, view_id)
            .await
            .map_err(|err| ProjectionError::Storage {
                cause: err.into(),
                meta: maplit::hashmap! {
                    META_TABLE.to_string() => self.view_storage_table.to_string(),
                    META_PROJECTION_ID.to_string() => view_id.to_string(),
                    META_VIEW_NAME.to_string() => self.view_name.to_string(),
                },
            })?;

        let view = entry
            .as_ref()
            .map(|e| {
                bincode::serde::decode_from_slice(e.bytes.as_slice(), bincode::config::standard())
                    .map(|(view, _size)| view)
            })
            .transpose()?;

        debug!(?entry, ?view, "loaded view {view_id}");
        Ok(view)
    }

    #[instrument(
        level = "debug",
        skip(self),
        fields(view_name=%self.view_name, view_storage_table=%self.view_storage_table)
    )]
    async fn save_view(
        &self,
        view_id: &ProjectionId,
        view: Self::View,
    ) -> Result<(), ProjectionError> {
        let bytes = bincode::serde::encode_to_vec(view, bincode::config::standard())?;
        let bytes = Arc::new(bytes);
        let entry = ViewEntry { bytes };
        let query_result = actor::protocol::save_view(&self.storage, view_id, entry)
            .await
            .map_err(|err| ProjectionError::Storage {
                cause: err.into(),
                meta: maplit::hashmap! {
                    META_TABLE.to_string() => self.view_storage_table.to_string(),
                    META_PROJECTION_ID.to_string() => view_id.to_string(),
                },
            })?;
        debug!(?query_result, "save_view completed");
        Ok(())
    }
}

mod actor {
    pub mod protocol {
        use crate::postgres::view_storage::actor::PostgresViewStorageActor;
        use crate::postgres::view_storage::ViewEntry;
        use crate::postgres::PostgresStorageError;
        use crate::projection::ProjectionId;
        use coerce::actor::message::Message;
        use coerce::actor::LocalActorRef;
        use sqlx::postgres::PgQueryResult;

        #[instrument(level = "debug", name = "protocol::load_view")]
        pub async fn load_view(
            actor: &LocalActorRef<PostgresViewStorageActor>,
            view_id: &ProjectionId,
        ) -> Result<Option<ViewEntry>, PostgresStorageError> {
            let command = LoadView::new(view_id.clone());
            actor.send(command).await?
        }

        #[instrument(level = "debug", name = "protocol::save_view")]
        pub async fn save_view(
            actor: &LocalActorRef<PostgresViewStorageActor>,
            view_id: &ProjectionId,
            entry: ViewEntry,
        ) -> Result<PgQueryResult, PostgresStorageError> {
            let command = SaveView::new(view_id.clone(), entry);
            actor.send(command).await?
        }

        #[derive(Debug)]
        pub struct LoadView {
            pub view_id: ProjectionId,
        }

        impl LoadView {
            pub const fn new(view_id: ProjectionId) -> Self {
                Self { view_id }
            }
        }

        impl Message for LoadView {
            type Result = Result<Option<ViewEntry>, PostgresStorageError>;
        }

        #[derive(Debug)]
        pub struct SaveView {
            pub view_id: ProjectionId,
            pub entry: ViewEntry,
        }

        impl SaveView {
            pub const fn new(view_id: ProjectionId, entry: ViewEntry) -> Self {
                Self { view_id, entry }
            }
        }

        impl Message for SaveView {
            type Result = Result<PgQueryResult, PostgresStorageError>;
        }
    }

    use super::sql_query::ViewStorageSqlQueryFactory;
    use crate::postgres::view_storage::actor::protocol::{LoadView, SaveView};
    use crate::postgres::view_storage::ViewEntry;
    use crate::postgres::PostgresStorageError;
    use coerce::actor::context::ActorContext;
    use coerce::actor::message::Handler;
    use coerce::actor::Actor;
    use smol_str::SmolStr;
    use sqlx::postgres::{PgQueryResult, PgRow};
    use sqlx::{PgPool, Row};
    use std::sync::Arc;

    #[derive(Debug)]
    pub struct PostgresViewStorageActor {
        pool: PgPool,
        sql_query: ViewStorageSqlQueryFactory,
    }

    impl PostgresViewStorageActor {
        pub fn new(pool: PgPool, view_storage_table: &str) -> Self {
            let view_storage_table = SmolStr::new(view_storage_table);
            let sql_query = ViewStorageSqlQueryFactory::new(&view_storage_table);
            Self { pool, sql_query }
        }
    }

    #[async_trait]
    impl Actor for PostgresViewStorageActor {}

    impl PostgresViewStorageActor {
        fn entry_from_row(&self, row: PgRow) -> ViewEntry {
            let bytes = Arc::new(row.get(self.sql_query.payload_column()));
            ViewEntry { bytes }
        }
    }

    #[async_trait]
    impl Handler<LoadView> for PostgresViewStorageActor {
        #[instrument(level = "debug", skip(self, _ctx))]
        async fn handle(
            &mut self,
            message: LoadView,
            _ctx: &mut ActorContext,
        ) -> Result<Option<ViewEntry>, PostgresStorageError> {
            sqlx::query(self.sql_query.select_latest_view())
                .bind(message.view_id.as_ref())
                .map(|row| self.entry_from_row(row))
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| err.into())
        }
    }

    #[async_trait]
    impl Handler<SaveView> for PostgresViewStorageActor {
        #[instrument(level = "debug", skip(self, _ctx))]
        async fn handle(
            &mut self,
            message: SaveView,
            _ctx: &mut ActorContext,
        ) -> Result<PgQueryResult, PostgresStorageError> {
            let view_id = message.view_id.as_ref();

            let mut tx = sqlx::Acquire::begin(&self.pool).await?;

            let now = crate::now_timestamp();

            let query_result = sqlx::query(self.sql_query.update_or_insert_view())
                .bind(view_id)
                .bind(message.entry.bytes.to_vec())
                .bind(now)
                .bind(now)
                .execute(&mut tx)
                .await
                .map_err(|err| err.into());

            if let Err(error) = tx.commit().await {
                error!(%view_id, "postgres view storage failed to commit save view transaction: {error:?}");
                return Err(error.into());
            }

            debug!("save_view completed: {query_result:?}");
            query_result
        }
    }
}

mod sql_query {
    use once_cell::sync::{Lazy, OnceCell};
    use smol_str::SmolStr;
    use sql_query_builder as sql;
    use std::fmt;

    const VIEW_ID_COL: &str = "view_id";
    const PAYLOAD_COL: &str = "payload";
    const CREATED_AT_COL: &str = "created_at";
    const LAST_UPDATED_AT_COL: &str = "last_updated_at";

    const VIEW_STORAGE_COLUMNS: [&str; 4] = [
        VIEW_ID_COL,
        PAYLOAD_COL,
        CREATED_AT_COL,
        LAST_UPDATED_AT_COL,
    ];
    static VIEW_STORAGE_COLUMNS_REP: Lazy<String> = Lazy::new(|| VIEW_STORAGE_COLUMNS.join(", "));
    static VIEW_STORAGE_VALUES_REP: Lazy<String> = Lazy::new(|| {
        let values = (1..=VIEW_STORAGE_COLUMNS.len())
            .map(|i| format!("${i}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("( {values} )")
    });

    pub struct ViewStorageSqlQueryFactory {
        view_storage_table: SmolStr,
        select: OnceCell<String>,
        update_or_insert: OnceCell<String>,
        where_view_id: OnceCell<String>,
    }

    impl fmt::Debug for ViewStorageSqlQueryFactory {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("ViewStorageSqlQueryFactory").finish()
        }
    }

    impl ViewStorageSqlQueryFactory {
        pub fn new(view_storage_table: &str) -> Self {
            Self {
                view_storage_table: SmolStr::new(view_storage_table),
                select: OnceCell::new(),
                update_or_insert: OnceCell::new(),
                where_view_id: OnceCell::new(),
            }
        }

        fn where_view_id(&self) -> &str {
            self.where_view_id
                .get_or_init(|| format!("{view_id} = $1", view_id = self.view_id_column()))
        }

        #[allow(dead_code, clippy::missing_const_for_fn)]
        #[inline]
        pub fn view_id_column(&self) -> &str {
            VIEW_ID_COL
        }

        #[allow(dead_code, clippy::missing_const_for_fn)]
        #[inline]
        pub fn payload_column(&self) -> &str {
            PAYLOAD_COL
        }

        #[inline]
        pub fn select_latest_view(&self) -> &str {
            self.select.get_or_init(|| {
                sql::Select::new()
                    .select(&VIEW_STORAGE_COLUMNS_REP)
                    .from(self.view_storage_table.as_str())
                    .where_clause(self.where_view_id())
                    .order_by(format!("{LAST_UPDATED_AT_COL} desc").as_str())
                    .limit("1")
                    .to_string()
            })
        }

        #[inline]
        pub fn update_or_insert_view(&self) -> &str {
            self.update_or_insert.get_or_init(|| {
                let payload_col = self.payload_column();
                let update_clause = sql::Update::new()
                    .set(format!("{payload_col} = EXCLUDED.{payload_col}, last_updated_at = EXCLUDED.last_updated_at").as_str())
                    .to_string();

                let conflict_clause = format!("( {} ) DO UPDATE {}", self.view_id_column(), update_clause);

                sql::Insert::new()
                    .insert_into(
                        format!(
                            "{table} ( {columns} )",
                            table = self.view_storage_table, columns = VIEW_STORAGE_COLUMNS_REP.as_str(),
                        )
                        .as_str(),
                    )
                    .values(VIEW_STORAGE_VALUES_REP.as_str())
                    .on_conflict(&conflict_clause)
                    .to_string()
            })
        }
    }
}
