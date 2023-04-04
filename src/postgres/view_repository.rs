// use crate::projection::{ProjectionError, ViewContext};
// use serde::de::DeserializeOwned;
// use serde::Serialize;
// use sql_query_builder as sql;
// use sqlx::{PgPool, Row};
// use std::fmt::Debug;
// use std::marker::PhantomData;

// /// A postgres-backed view repository for use in a `GenericProcessor`
// #[derive(Debug)]
// pub struct PostgresViewRepository<V>
// where
//     V: Debug + Serialize + DeserializeOwned + Send + Sync,
// {
//     insert_sql: String,
//     update_sql: String,
//     select_sql: String,
//     pool: PgPool,
//     _marker: PhantomData<V>,
// }

// impl<V> PostgresViewRepository<V>
// where
//     V: Debug + Serialize + DeserializeOwned + Send + Sync,
// {
//     /// Creates a new `PostgresViewRepository` that will store serialized views in a Postgres table
//     /// named identically to the `view_name` value provided. This table should be created by the
//     /// user before using this query repository (see `/db/init.sql` sql initialization file).
//     ///
//     /// ```
//     /// # use coerce_cqrs::doc::MyAggregate;
//     /// # use coerce_cqrs::doc::persist::MyView;
//     /// use sqlx::{Pool, Postgres};
//     /// use coerce_cqrs::postgres::PostgresViewRepository;
//     ///
//     /// fn configure_view_repo(pool: Pool<Postgres>) -> PostgresViewRepository<MyView> {
//     ///     PostgresViewRepository::new("my_view_table", pool)
//     /// }
//     /// ```
//     pub fn new(view_name: &str, pool: PgPool) -> Self {
//         let insert_sql = sql::Insert::new()
//             .insert_into(format!("{view_name} (payload, version, view_id)").as_str())
//             .values("( $1, $2, $3 )")
//             .as_string();
//         // format!("INSERT INTO {} (payload, version, view_id) VALUES ( $1, $2, $3 )", view_name);
//
//         let update_sql = sql::Update::new()
//             .update(view_name)
//             .set("payload = $1, version = $2")
//             .where_clause("view_id = $3")
//             .as_string();
//         // format!("UPDATE {} SET payload= $1 , version= $2 WHERE view_id= $3", view_name);
//
//         let select_sql = sql::Select::new()
//             .select("version, payload")
//             .from(view_name)
//             .where_clause("view_id = $1")
//             .as_string();
//         // format!("SELECT version,payload FROM {} WHERE view_id= $1", view_name);
//
//         Self {
//             insert_sql,
//             update_sql,
//             select_sql,
//             pool,
//             _marker: PhantomData,
//         }
//     }
// }
//
// const VIEW_PAYLOAD_FIELD: &str = "payload";
// const VIEW_VERSION_FIELD: &str = "version";
//
// #[async_trait]
// impl<V> ViewRepository for PostgresViewRepository<V>
// where
//     V: Debug + Serialize + DeserializeOwned + Send + Sync,
// {
//     type View = V;
//
//     #[instrument(level = "debug", skip(self))]
//     async fn load(&self, view_id: &str) -> Result<Option<Self::View>, ProjectionError> {
//         sqlx::query(&self.select_sql)
//             .bind(view_id)
//             .fetch_optional(&self.pool)
//             .await?
//             .map(|row| {
//                 let view = serde_json::from_value(row.get(VIEW_PAYLOAD_FIELD))?;
//                 Ok(Some(view))
//             })
//             .unwrap_or(Ok(None))
//     }
//
//     #[instrument(level = "debug", skip(self))]
//     async fn load_with_context(
//         &self,
//         view_id: &str,
//     ) -> Result<Option<(Self::View, ViewContext)>, ProjectionError> {
//         sqlx::query(&self.select_sql)
//             .bind(view_id)
//             .fetch_optional(&self.pool)
//             .await?
//             .map(|row| {
//                 let version = row.get(VIEW_VERSION_FIELD);
//                 let view = serde_json::from_value(row.get(VIEW_PAYLOAD_FIELD))?;
//                 let view_context = ViewContext::new(view_id, version);
//                 Ok(Some((view, view_context)))
//             })
//             .unwrap_or(Ok(None))
//     }
//
//     #[instrument(level = "debug", skip(self))]
//     async fn update_view(
//         &self,
//         view: Self::View,
//         context: ViewContext,
//     ) -> Result<(), ProjectionError> {
//         let sql = if context.version == 0 {
//             self.insert_sql.as_str()
//         } else {
//             self.update_sql.as_str()
//         };
//
//         let version = context.version + 1;
//         let payload = serde_json::to_value(&view)?;
//
//         sqlx::query(sql)
//             .bind(payload)
//             .bind(version)
//             .bind(context.view_instance_id)
//             .execute(&self.pool)
//             .await?;
//         Ok(())
//     }
// }
//
// #[cfg(test)]
// mod test {
//     use super::*;
// }
