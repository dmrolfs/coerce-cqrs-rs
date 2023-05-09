

#[derive(Debug, Clone)]
pub struct OffsetEntry {
    pub current_offset: String,

}

pub struct PostgresOffsetStorage;

mod actor {
    use coerce::actor::Actor;
    use smol_str::SmolStr;
    use sqlx::PgPool;
    use sqlx::postgres::PgRow;
    use crate::postgres::offset::OffsetEntry;
    use crate::postgres::offset::sql_query::OffsetStorageSqlQueryFactory;

    pub mod protocol {
        use coerce::actor::LocalActorRef;
        use coerce::actor::message::Message;
        use futures::SinkExt;
        use sqlx::postgres::PgQueryResult;
        use crate::postgres::offset::OffsetEntry;
        use crate::postgres::PostgresStorageError;
        use crate::projection::{ProjectionId, ProjectionName};

        #[instrument(level="debug", name="protocol::load_offset")]
        pub async fn load_offset(
            actor: &LocalActorRef<PostgresOffsetStorageActor>,
            projection_name: &ProjectionName,
            projection_id: &ProjectionId,
        ) -> Result<Option<OffsetEntry>, PostgresStorageError> {
            let command = LoadOffset::new(projection_name.clone(), projection_id.clone());
            actor.send(command).await?
        }

        #[instrument(level="debug", name="protocol::save_offset")]
        pub async fn save_offset(
            actor: &LocalActorRef<PostgresOffsetStorageActor>,
            projection_name: &ProjectionName,
            projection_id: &ProjectionId,
            entry: OffsetEntry,
        ) -> Result<PgQueryResult, PostgresStorageError> {
            let command = SaveOffset::new(projection_name.clone(), projection_id.clone(), entry);
            actor.send(command).await?
        }

        #[derive(Debug)]
        pub struct LoadOffset {
            pub name: ProjectionName,
            pub id: ProjectionId,
            pub entry: OffsetEntry,
        }

        impl Message for LoadOffset {
            type Result = Result<Option<OffsetEntry>, PostgresStorageError>;
        }

        #[derive(Debug)]
        pub struct SaveOffset {
            pub name: ProjectionName,
            pub id: ProjectionId,
            pub entry: OffsetEntry,
        }

        impl SaveOffset {
            pub const fn new(name: ProjectionName, id: ProjectionId, entry: OffsetEntry) -> Self {
                Self { name, id, entry, }
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
            Self { pool, sql_query, }
        }
    }

    #[async_trait]
    impl Actor for PostgresOffsetStorageActor {}

    impl PostgresOffsetStorageActor {
        fn entry_from_row(&self row: PgRow) -> OffsetEntry {
            l
        }
    }
}

mod sql_query {
    use once_cell::sync::{Lazy, OnceCell};
    use smol_str::SmolStr;
    use sql_query_builder as sql;
    use std::fmt;

    const PROJECTION_NAME_COL: &str = "projection_name";
    const PROJECTION_KEY_COL: &str = "projection_key";
    const CURRENT_OFFSET_COL: &str = "current_offset";
    const LAST_UPDATED_AT_COL: &str = "last_updated_at";

    const OFFSET_COLUMNS: [&str; 4] = [PROJECTION_NAME_COL, PROJECTION_KEY_COL, CURRENT_OFFSET_COL, LAST_UPDATED_AT_COL];
    static OFFSET_COLUMNS_REP: Lazy<String> = Lazy::new(|| OFFSET_COLUMNS.join(", "));
    static OFFSET_VALUES_REP: Lazy<String> = Lazy::new(|| {
        let values = (1..OFFSET_COLUMNS.len())
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
            f.debug_struct("OffsetStorageSqlFactory").finish()
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
            self.where_projection
                .get_or_init(|| format!(
                    "{name} = $1 AND {key} = $2",
                    name = self.projection_name_column(),
                    key = self.projection_key_column(),
                ))
        }

        #[allow(dead_code, clippy::missing_const_for_fn)]
        #[inline]
        pub fn projection_name_column(&self) -> &str { PROJECTION_NAME_COL }

        #[allow(dead_code, clippy::missing_const_for_fn)]
        #[inline]
        pub fn projection_key_column(&self) -> &str { PROJECTION_KEY_COL }

        #[allow(dead_code, clippy::missing_const_for_fn)]
        #[inline]
        pub fn current_offset_column(&self) -> &str { CURRENT_OFFSET_COL }

        #[inline]
        pub fn select_offset(&self) -> &str {
            self.select.get_or_init(|| {
                sql::Select::new()
                    .select(&OFFSET_COLUMNS_REP)
                    .from(self.offset_storage_table.as_str())
                    .where_clause(self.where_projection())
                    .order_by(format!("{LAST_UPDATED_AT_COL} desc").as_str())
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
                    "( {name}, {key} ) DO UPDATE {update_clause}",
                    name = self.projection_name_column(), key = self.projection_key_column(),
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