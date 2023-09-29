mod actor;
mod config;
mod provider;

#[doc(hidden)]
pub mod doc;
mod projection_storage;
mod sql_query;

pub use config::PostgresStorageConfig;
pub use projection_storage::{BinaryProjection, PostgresProjectionStorage};
pub use provider::{PostgresJournalStorage, PostgresStorageProvider};

use crate::projection::PersistenceId;
use anyhow::anyhow;
use lazy_regex::lazy_regex;
use nutype::nutype;
use regex::Regex;
use sqlx::database::{HasArguments, HasValueRef};
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use std::fmt::Debug;
use std::str::FromStr;
use strum_macros::{Display, EnumString, EnumVariantNames, IntoStaticStr};
use thiserror::Error;

#[nutype(
    sanitize(trim, lowercase)
    validate(not_empty)
)]
#[derive(
    Debug,
    Clone,
    Deref,
    Borrow,
    FromStr,
    Into,
    TryFrom,
    AsRef,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
)]
pub struct TableName(String);

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[nutype(
    sanitize(trim, lowercase)
    validate(not_empty)
)]
#[derive(
    Debug,
    Clone,
    Deref,
    Borrow,
    FromStr,
    TryFrom,
    AsRef,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
)]
pub struct TableColumn(String);

impl std::fmt::Display for TableColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

static STORAGE_KEY_REGEX: lazy_regex::Lazy<Regex> = lazy_regex!(
    r"(?x)
      ^
      (([^:]+):)?             # First part (optional)
      (([^:]+)::([^:]+)):     # Second part with two subparts
      ([^:]+)                 # Third part
    $"
);

#[nutype(
    sanitize(trim)
    validate(not_empty, regex = STORAGE_KEY_REGEX)
)]
#[derive(
    Debug,
    Clone,
    FromStr,
    TryFrom,
    AsRef,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
)]
pub(in crate::postgres) struct StorageKey(String);

impl std::fmt::Display for StorageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[allow(clippy::fallible_impl_from)]
impl From<PersistenceId> for StorageKey {
    fn from(pid: PersistenceId) -> Self {
        Self::new(pid.to_string()).unwrap()
    }
}

impl<DB> sqlx::Type<DB> for StorageKey
where
    DB: sqlx::Database,
    String: sqlx::Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <String as sqlx::Type<DB>>::type_info()
    }

    fn compatible(ty: &DB::TypeInfo) -> bool {
        <String as sqlx::Type<DB>>::compatible(ty)
    }
}

impl<'r, DB> sqlx::Decode<'r, DB> for StorageKey
where
    DB: sqlx::Database,
    &'r str: sqlx::Decode<'r, DB>,
{
    fn decode(
        value: <DB as sqlx::database::HasValueRef<'r>>::ValueRef,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let id = <&str as sqlx::Decode<DB>>::decode(value)?;
        Self::new(id).map_err(|err| err.into())
    }
}

impl<'r, DB> sqlx::Encode<'r, DB> for StorageKey
where
    DB: sqlx::Database,
    String: sqlx::Encode<'r, DB>,
{
    fn encode_by_ref(&self, buf: &mut <DB as HasArguments<'r>>::ArgumentBuffer) -> IsNull {
        <String as sqlx::Encode<'r, DB>>::encode(self.to_string(), buf)
    }
}

impl<DB> sqlx::Type<DB> for PersistenceId
where
    DB: sqlx::Database,
    String: sqlx::Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <String as sqlx::Type<DB>>::type_info()
    }

    fn compatible(ty: &DB::TypeInfo) -> bool {
        <String as sqlx::Type<DB>>::compatible(ty)
    }
}

impl<'r, DB> sqlx::Decode<'r, DB> for PersistenceId
where
    DB: sqlx::Database,
    &'r str: sqlx::Decode<'r, DB>,
{
    fn decode(value: <DB as HasValueRef<'r>>::ValueRef) -> Result<Self, BoxDynError> {
        let id = <&str as sqlx::Decode<DB>>::decode(value)?;
        let persistence_id = Self::from_str(id)?;
        Ok(persistence_id)
    }
}

#[derive(
    Debug, Copy, Clone, PartialEq, Eq, Hash, Display, IntoStaticStr, EnumString, EnumVariantNames,
)]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
pub enum EntryType {
    Journal,
    Snapshot,
}

pub type StorageKeyParts = (Option<String>, PersistenceId, EntryType);

pub(in crate::postgres) trait StorageKeyCodec: Debug + Send + Sync {
    fn key_into_parts(&self, key: StorageKey) -> Result<StorageKeyParts, PostgresStorageError>;

    fn key_from_parts(&self, persistence_id: &str, entry_type: &str) -> StorageKey;

    fn key_from_persistence_parts(
        &self,
        persistence_id: &PersistenceId,
        entry_type: EntryType,
    ) -> StorageKey {
        self.key_from_parts(
            persistence_id.as_persistence_id().as_str(),
            entry_type.into(),
        )
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SimpleStorageKeyCodec {
    key_prefix: Option<String>,
}

impl SimpleStorageKeyCodec {
    pub fn with_prefix(key_prefix: impl Into<String>) -> Self {
        let prefix = key_prefix.into();
        let key_prefix = if prefix.is_empty() {
            None
        } else {
            Some(prefix)
        };
        Self { key_prefix }
    }
}

impl StorageKeyCodec for SimpleStorageKeyCodec {
    #[instrument(level = "debug")]
    fn key_into_parts(&self, key: StorageKey) -> Result<StorageKeyParts, PostgresStorageError> {
        let decomposed_captures = STORAGE_KEY_REGEX.captures(key.as_ref());
        // debug!("DMR: decomposed_captures: {decomposed_captures:?}");
        let result = if let Some(captures) = decomposed_captures {
            let prefix = captures.get(2).map(|c2| c2.as_str().to_string());
            // debug!("DMR: prefix = {prefix:?}");
            let aggregate_name = captures.get(4).unwrap().as_str();
            // debug!("DMR: aggregate_name = {aggregate_name}");
            let aggregate_id = captures.get(5).unwrap().as_str();
            // debug!("DMR: aggregate_id = {aggregate_id}");
            let persistence_id = PersistenceId::from_parts(aggregate_name, aggregate_id);
            // debug!("DMR: persistence_id = {persistence_id}");
            let entry_type = EntryType::from_str(captures.get(6).unwrap().as_str())
                .map_err(|err| PostgresStorageError::Storage(err.into()))?;
            // debug!("DMR: entry_type = {entry_type}");
            Ok((prefix, persistence_id, entry_type))
        } else {
            Err(PostgresStorageError::Storage(anyhow!(format!(
                "failed to decompose storage key: {key}"
            ))))
        };

        // debug!("DMR: storage key parts = {result:?}");
        result
    }

    fn key_from_parts(&self, persistence_id: &str, entry_type: &str) -> StorageKey {
        self.key_prefix.as_ref().map_or_else(
            || format!("{persistence_id}:{entry_type}").try_into().unwrap(),
            |key_prefix| {
                format!("{key_prefix}:{persistence_id}:{entry_type}")
                    .try_into()
                    .unwrap()
            },
        )
    }
}

#[derive(Debug, Error)]
pub enum PostgresStorageError {
    #[error("{0}")]
    ActorRef(#[from] coerce::actor::ActorRefErr),

    #[error("{0}")]
    ActorReply(#[from] tokio::sync::oneshot::error::RecvError),

    #[error("{0}")]
    Sql(#[from] sqlx::Error),

    #[error("{0}")]
    Storage(anyhow::Error),
}

#[cfg(test)]
mod tests {
    use crate::postgres::{EntryType, SimpleStorageKeyCodec, StorageKey, StorageKeyCodec};
    use crate::projection::PersistenceId;
    use claim::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_storage_key_codec() -> anyhow::Result<()> {
        let codec = SimpleStorageKeyCodec::default();

        let _ = assert_err!(StorageKey::new("WAZ558:journal"));

        let storage_key = assert_ok!(StorageKey::new("LocationZone::WAZ558:journal"));
        let (actual_prefix, actual_persistence_id, actual_entry_type) =
            assert_ok!(codec.key_into_parts(storage_key.clone()));

        assert_none!(actual_prefix);
        assert_eq!(
            actual_persistence_id,
            PersistenceId::from_parts("LocationZone", "WAZ558")
        );
        assert_eq!(actual_entry_type, EntryType::Journal);

        Ok(())
    }
}
