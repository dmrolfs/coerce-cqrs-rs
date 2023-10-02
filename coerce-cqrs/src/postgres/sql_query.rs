use crate::postgres::{TableColumn, TableName};
use crate::projection::processor::EntryPayloadTypes;
use itertools::Itertools;
use once_cell::sync::Lazy;
use once_cell::sync::OnceCell;
use sql_query_builder as sql;
use std::clone::Clone;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

static PROJECTION_ID_COL: Lazy<TableColumn> =
    Lazy::new(|| TableColumn::new("projection_id").unwrap());
static PERSISTENCE_ID_COL: Lazy<TableColumn> =
    Lazy::new(|| TableColumn::new("persistence_id").unwrap());
static SEQUENCE_NR_COL: Lazy<TableColumn> =
    Lazy::new(|| TableColumn::new("sequence_number").unwrap());
static EVENT_MANIFEST_COL: Lazy<TableColumn> =
    Lazy::new(|| TableColumn::new("event_manifest").unwrap());
static EVENT_PAYLOAD_COL: Lazy<TableColumn> =
    Lazy::new(|| TableColumn::new("event_payload").unwrap());
static CURRENT_OFFSET_COL: Lazy<TableColumn> =
    Lazy::new(|| TableColumn::new("current_offset").unwrap());
static SNAPSHOT_MANIFEST_COL: Lazy<TableColumn> =
    Lazy::new(|| TableColumn::new("snapshot_manifest").unwrap());
static SNAPSHOT_PAYLOAD_COL: Lazy<TableColumn> =
    Lazy::new(|| TableColumn::new("snapshot_payload").unwrap());

static EVENT_COLUMNS: Lazy<[TableColumn; 7]> = Lazy::new(|| {
    [
        PERSISTENCE_ID_COL.clone(),
        SEQUENCE_NR_COL.clone(),
        TableColumn::new("is_deleted").unwrap(),
        EVENT_MANIFEST_COL.clone(),
        EVENT_PAYLOAD_COL.clone(),
        TableColumn::new("meta_payload").unwrap(),
        TableColumn::new("created_at").unwrap(),
    ]
});

static EVENT_COLUMNS_REP: Lazy<String> = Lazy::new(|| EVENT_COLUMNS.join(", "));
static EVENT_VALUES_REP: Lazy<String> = Lazy::new(|| {
    let values = (1..=EVENT_COLUMNS.len())
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("( {values} )")
});

static SNAPSHOTS_COLUMNS: Lazy<[TableColumn; 7]> = Lazy::new(|| {
    [
        PERSISTENCE_ID_COL.clone(),
        SEQUENCE_NR_COL.clone(),
        SNAPSHOT_MANIFEST_COL.clone(),
        SNAPSHOT_PAYLOAD_COL.clone(),
        TableColumn::new("meta_payload").unwrap(),
        TableColumn::new("created_at").unwrap(),
        TableColumn::new("last_updated_at").unwrap(),
    ]
});
static SNAPSHOTS_COLUMNS_REP: Lazy<String> = Lazy::new(|| SNAPSHOTS_COLUMNS.join(", "));
static SNAPSHOTS_VALUES_REP: Lazy<String> = Lazy::new(|| {
    let values = (1..=SNAPSHOTS_COLUMNS.len())
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("( {values} )")
});

pub struct SqlQueryFactory {
    event_journal_table: TableName,
    snapshots_table: Option<TableName>,
    offsets_table: TableName,

    offsets_projection_id_column: TableColumn,
    offsets_persistence_id_column: TableColumn,
    offsets_current_offset_column: TableColumn,

    persistence_id_column: TableColumn,
    sequence_nr_column: TableColumn,

    event_manifest_column: TableColumn,
    event_payload_column: TableColumn,

    snapshot_manifest_column: TableColumn,
    snapshot_payload_column: TableColumn,

    where_persistence_id: OnceCell<String>,

    select_persistence_ids: HashMap<EntryPayloadTypes, Arc<str>>,
    select_event: OnceCell<String>,
    select_events_range: OnceCell<String>,
    select_latest_events: OnceCell<String>,
    append_event: OnceCell<String>,
    delete_event: OnceCell<String>,
    delete_events_range: OnceCell<String>,
    delete_latest_events: OnceCell<String>,
    select_snapshot: OnceCell<String>,
    update_or_insert: OnceCell<String>,
    delete_snapshot: OnceCell<String>,
    clear_aggregate_events: OnceCell<String>,
}

impl fmt::Debug for SqlQueryFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlQueryFactory")
            .field("event_journal_table", &self.event_journal_table)
            .field("snapshots_table", &self.snapshots_table)
            .field("offsets_table", &self.offsets_table)
            .finish()
    }
}

impl SqlQueryFactory {
    pub fn new(event_journal_table: TableName, offsets_table: TableName) -> Self {
        Self {
            event_journal_table,
            snapshots_table: None,
            offsets_table,

            offsets_projection_id_column: PROJECTION_ID_COL.clone(),
            offsets_persistence_id_column: PERSISTENCE_ID_COL.clone(),
            offsets_current_offset_column: CURRENT_OFFSET_COL.clone(),
            persistence_id_column: PERSISTENCE_ID_COL.clone(),
            sequence_nr_column: SEQUENCE_NR_COL.clone(),
            event_manifest_column: EVENT_MANIFEST_COL.clone(),
            event_payload_column: EVENT_PAYLOAD_COL.clone(),
            snapshot_manifest_column: SNAPSHOT_MANIFEST_COL.clone(),
            snapshot_payload_column: SNAPSHOT_PAYLOAD_COL.clone(),
            where_persistence_id: OnceCell::new(),
            select_persistence_ids: HashMap::new(),
            select_event: OnceCell::new(),
            select_events_range: OnceCell::new(),
            select_latest_events: OnceCell::new(),
            append_event: OnceCell::new(),
            delete_event: OnceCell::new(),
            delete_events_range: OnceCell::new(),
            delete_latest_events: OnceCell::new(),
            select_snapshot: OnceCell::new(),
            update_or_insert: OnceCell::new(),
            delete_snapshot: OnceCell::new(),
            clear_aggregate_events: OnceCell::new(),
        }
    }

    #[allow(dead_code, clippy::missing_const_for_fn)]
    pub fn with_snapshots_table(self, snapshots_table: TableName) -> Self {
        Self {
            snapshots_table: Some(snapshots_table),
            ..self
        }
    }

    #[allow(dead_code, clippy::missing_const_for_fn)]
    pub fn with_persistence_id_column(self, persistence_id_column: TableColumn) -> Self {
        Self {
            persistence_id_column,
            ..self
        }
    }

    #[allow(dead_code, clippy::missing_const_for_fn)]
    pub fn with_sequence_nr_column(self, sequence_nr_column: TableColumn) -> Self {
        Self {
            sequence_nr_column,
            ..self
        }
    }

    #[allow(dead_code, clippy::missing_const_for_fn)]
    pub fn with_event_manifest_column(self, event_manifest_column: TableColumn) -> Self {
        Self {
            event_manifest_column,
            ..self
        }
    }

    #[allow(dead_code, clippy::missing_const_for_fn)]
    pub fn with_event_payload_column(self, event_payload_column: TableColumn) -> Self {
        Self {
            event_payload_column,
            ..self
        }
    }

    #[allow(dead_code, clippy::missing_const_for_fn)]
    pub fn with_snapshot_manifest_column(self, snapshot_manifest_column: TableColumn) -> Self {
        Self {
            snapshot_manifest_column,
            ..self
        }
    }

    #[allow(dead_code, clippy::missing_const_for_fn)]
    pub fn with_snapshot_payload_column(self, snapshot_payload_column: TableColumn) -> Self {
        Self {
            snapshot_payload_column,
            ..self
        }
    }

    #[inline]
    pub const fn event_journal_table(&self) -> &TableName {
        &self.event_journal_table
    }

    #[inline]
    pub fn snapshots_table(&self) -> &TableName {
        self.snapshots_table
            .as_ref()
            .expect("No snapshots_table provided")
    }

    #[inline]
    pub const fn persistence_id_column(&self) -> &TableColumn {
        &self.persistence_id_column
    }

    #[inline]
    pub const fn sequence_number_column(&self) -> &TableColumn {
        &self.sequence_nr_column
    }

    #[inline]
    pub const fn event_manifest_column(&self) -> &TableColumn {
        &self.event_manifest_column
    }

    #[inline]
    pub const fn event_payload_column(&self) -> &TableColumn {
        &self.event_payload_column
    }

    #[inline]
    pub const fn snapshot_manifest_column(&self) -> &TableColumn {
        &self.snapshot_manifest_column
    }

    #[inline]
    pub const fn snapshot_payload_column(&self) -> &TableColumn {
        &self.snapshot_payload_column
    }

    #[inline]
    fn where_persistence_id(&self) -> &str {
        self.where_persistence_id
            .get_or_init(|| format!("{} = $1", self.persistence_id_column()))
    }

    #[inline]
    pub fn select_persistence_ids(&mut self, entry_types: &EntryPayloadTypes) -> Arc<str> {
        if let Some(query_sql) = self.select_persistence_ids.get(entry_types) {
            return query_sql.clone();
        }

        let select_sql = sql::Select::new()
            .select(format!("DISTINCT {}", self.persistence_id_column()).as_str())
            .from(self.event_journal_table());

        let select_sql = match entry_types {
            EntryPayloadTypes::All => select_sql,
            EntryPayloadTypes::Set(known_types) => {
                let known_entry_types = format!(
                    "{event_manifest} LIKE ({k_types})",
                    event_manifest = self.event_manifest_column(),
                    k_types = known_types.iter().map(|e| format!("'{e}'")).join(",")
                );
                select_sql.where_clause(&known_entry_types)
            }
            EntryPayloadTypes::Single(known_type) => {
                let known_clause = format!(
                    "{event_manifest} = '{known_type}'",
                    event_manifest = self.event_manifest_column()
                );
                select_sql.where_clause(&known_clause)
            }
        };

        self.select_persistence_ids
            .entry(entry_types.clone())
            .or_insert_with(|| select_sql.to_string().into())
            .clone()
    }

    #[inline]
    pub fn select_event(&self) -> &str {
        self.select_event.get_or_init(|| {
            sql::Select::new()
                .select(&EVENT_COLUMNS_REP)
                .from(self.event_journal_table())
                .where_clause(self.where_persistence_id())
                .where_clause(format!("{} = $2", self.sequence_number_column()).as_str())
                .limit("1")
                .to_string()
        })
    }

    #[inline]
    pub fn select_events_range(&self) -> &str {
        self.select_events_range.get_or_init(|| {
            let sequence_nr_col = self.sequence_number_column();

            sql::Select::new()
                .select(&EVENT_COLUMNS_REP)
                .from(self.event_journal_table())
                .where_clause(self.where_persistence_id())
                .where_clause("is_deleted = FALSE")
                .where_clause(format!("$2 <= {sequence_nr_col}").as_str())
                .where_clause(format!("{sequence_nr_col} < $3").as_str())
                .order_by(sequence_nr_col)
                .to_string()
        })
    }

    #[inline]
    pub fn select_latest_events(&self) -> &str {
        self.select_latest_events.get_or_init(|| {
            let sequence_nr_col = self.sequence_number_column();

            sql::Select::new()
                .select(&EVENT_COLUMNS_REP)
                .from(self.event_journal_table())
                .where_clause(self.where_persistence_id())
                .where_clause("is_deleted = FALSE")
                .where_clause(format!("$2 <= {sequence_nr_col}").as_str())
                .order_by(sequence_nr_col)
                .to_string()
        })
    }

    #[inline]
    pub fn append_event(&self) -> &str {
        self.append_event.get_or_init(|| {
            sql::Insert::new()
                .insert_into(
                    format!(
                        "{} ( {} )",
                        self.event_journal_table(),
                        EVENT_COLUMNS_REP.as_str(),
                    )
                    .as_str(),
                )
                .values(&EVENT_VALUES_REP)
                .to_string()
        })
    }

    #[inline]
    pub fn delete_event(&self) -> &str {
        self.delete_event.get_or_init(|| {
            sql::Update::new()
                .update(self.event_journal_table())
                .set("is_deleted = true")
                .where_clause(self.where_persistence_id())
                .where_clause(format!("{} = $2", self.sequence_number_column()).as_str())
                .to_string()
        })
    }

    #[inline]
    pub fn delete_events_range(&self) -> &str {
        self.delete_events_range.get_or_init(|| {
            let sequence_nr_col = self.sequence_number_column();

            sql::Update::new()
                .update(self.event_journal_table())
                .set("is_deleted = true")
                .where_clause(self.where_persistence_id())
                .where_clause(format!("$2 <= {sequence_nr_col}").as_str())
                .where_clause(format!("{sequence_nr_col} < $3").as_str())
                .to_string()
        })
    }

    #[inline]
    pub fn delete_latest_events(&self) -> &str {
        self.delete_latest_events.get_or_init(|| {
            sql::Update::new()
                .update(self.event_journal_table())
                .set("is_deleted = true")
                .where_clause(self.where_persistence_id())
                .where_clause(format!("$2 <= {}", self.sequence_number_column()).as_str())
                .to_string()
        })
    }

    #[inline]
    pub fn select_snapshot(&self) -> &str {
        self.select_snapshot.get_or_init(|| {
            sql::Select::new()
                .select(&SNAPSHOTS_COLUMNS_REP)
                .from(self.snapshots_table())
                .where_clause(self.where_persistence_id())
                .order_by(format!("{} desc", self.sequence_number_column()).as_str())
                .limit("1")
                .to_string()
        })
    }

    #[inline]
    pub fn update_or_insert_snapshot(&self) -> &str {
        self.update_or_insert.get_or_init(|| {
            let table = self.snapshots_table.as_ref().expect("No snapshot table set");
            let payload_col = self.snapshot_payload_column();
            let sequence_col = self.sequence_number_column();

            let update_clause = sql::Update::new()
                .set(format!("{payload_col} = EXCLUDED.{payload_col}, {sequence_col} = EXCLUDED.{sequence_col}, last_updated_at = EXCLUDED.last_updated_at").as_str())
                .to_string();

            let conflict_clause = format!(
                "( {primary_key} ) DO UPDATE {update_clause}",
                primary_key = self.persistence_id_column(),
            );

            sql::Insert::new()
                .insert_into(format!("{table} ( {columns} )", columns = SNAPSHOTS_COLUMNS_REP.as_str()).as_str())
                .values(&SNAPSHOTS_VALUES_REP)
                .on_conflict(&conflict_clause)
                .to_string()
        })
    }

    #[allow(dead_code)]
    #[inline]
    pub fn delete_snapshot(&self) -> &str {
        self.delete_snapshot.get_or_init(|| {
            sql::Delete::new()
                .delete_from(self.snapshots_table())
                .where_clause(self.where_persistence_id())
                .to_string()
        })
    }

    #[allow(dead_code)]
    #[inline]
    pub fn clear_aggregate_events(&self) -> &str {
        self.clear_aggregate_events.get_or_init(|| {
            sql::Delete::new()
                .delete_from(self.event_journal_table())
                .where_clause(self.where_persistence_id())
                .to_string()
        })
    }
}

// #[inline]
// pub fn select_all_latest_events(&self) -> &str {
//     // SELECT offsets.projection_id, journal.*
//     // FROM public.event_journal AS journal
//     // LEFT JOIN public.projection_offset AS offsets ON journal.persistence_id = offsets.persistence_id
//     // WHERE offsets.projection_id = $1 AND journal.sequence_number >= COALESCE(offsets.current_offset, 0);
//
//     // SELECT *
//     // FROM public.event_journal
//     // WHERE persistence_id == $
//
//     self.select_all_latest_events.get_or_init(|| {
//         sql::Select::new()
//
//
//
//
//         let ej_columns = EVENT_COLUMNS
//             .iter()
//             .map(|c| format!("journal.{c}"))
//             .collect::<Vec<_>>();
//
//         let mut columns = vec![format!("offsets.{}", self.offsets_projection_id_column)];
//         columns.extend(ej_columns);
//         let columns = columns.join(", ");
//
//         sql::Select::new()
//             .select(&columns)
//             .from(&format!("{} AS journal", self.event_journal_table()))
//             .left_join(&format!(
//                 "{} AS offsets ON journal.{} = offsets.{}",
//                 self.offsets_table,
//                 self.persistence_id_column,
//                 self.offsets_persistence_id_column
//             ))
//             .where_clause(&format!(
//                 "offsets.{} = $1 AND journal.{} >= COALESCE(offsets.{}, 0)",
//                 self.offsets_projection_id_column,
//                 self.sequence_number_column(),
//                 self.offsets_current_offset_column
//             ))
//             .to_string()
//     })
// }
