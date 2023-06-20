use once_cell::sync::Lazy;
use once_cell::sync::OnceCell;
use smol_str::SmolStr;
use sql_query_builder as sql;
use std::fmt;

const PROJECTION_ID_COL: &str = "projection_id";
const PERSISTENCE_ID_COL: &str = "persistence_id";
const SEQUENCE_NR_COL: &str = "sequence_number";
const EVENT_MANIFEST_COL: &str = "event_manifest";
const EVENT_PAYLOAD_COL: &str = "event_payload";
const CURRENT_OFFSET_COL: &str = "current_offset";
const SNAPSHOT_MANIFEST_COL: &str = "snapshot_manifest";
const SNAPSHOT_PAYLOAD_COL: &str = "snapshot_payload";

const EVENT_COLUMNS: [&str; 7] = [
    PERSISTENCE_ID_COL,
    SEQUENCE_NR_COL,
    "is_deleted",
    EVENT_MANIFEST_COL,
    EVENT_PAYLOAD_COL,
    "meta_payload",
    "created_at",
];

static EVENT_COLUMNS_REP: Lazy<String> = Lazy::new(|| EVENT_COLUMNS.join(", "));
static EVENT_VALUES_REP: Lazy<String> = Lazy::new(|| {
    let values = (1..=EVENT_COLUMNS.len())
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("( {values} )")
});

const SNAPSHOTS_COLUMNS: [&str; 7] = [
    PERSISTENCE_ID_COL,
    SEQUENCE_NR_COL,
    SNAPSHOT_MANIFEST_COL,
    SNAPSHOT_PAYLOAD_COL,
    "meta_payload",
    "created_at",
    "last_updated_at",
];
static SNAPSHOTS_COLUMNS_REP: Lazy<String> = Lazy::new(|| SNAPSHOTS_COLUMNS.join(", "));
static SNAPSHOTS_VALUES_REP: Lazy<String> = Lazy::new(|| {
    let values = (1..=SNAPSHOTS_COLUMNS.len())
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("( {values} )")
});

pub struct SqlQueryFactory {
    event_journal_table: SmolStr,
    snapshots_table: Option<SmolStr>,
    offsets_table: SmolStr,

    offsets_projection_id_column: SmolStr,
    offsets_persistence_id_column: SmolStr,
    offsets_current_offset_column: SmolStr,

    persistence_id_column: SmolStr,
    sequence_nr_column: SmolStr,

    event_manifest_column: SmolStr,
    event_payload_column: SmolStr,

    snapshot_manifest_column: SmolStr,
    snapshot_payload_column: SmolStr,

    where_persistence_id: OnceCell<String>,

    select_persistence_ids: OnceCell<String>,
    select_event: OnceCell<String>,
    select_events_range: OnceCell<String>,
    // select_all_latest_events: OnceCell<String>,
    select_latest_events: OnceCell<String>,
    append_event: OnceCell<String>,
    delete_event: OnceCell<String>,
    delete_events_range: OnceCell<String>,
    delete_latest_events: OnceCell<String>,
    select_snapshot: OnceCell<String>,
    update_or_insert: OnceCell<String>,
    // insert_snapshot: OnceCell<String>,
    // update_snapshot: OnceCell<String>,
    delete_snapshot: OnceCell<String>,
    clear_aggregate_events: OnceCell<String>,
}

impl fmt::Debug for SqlQueryFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlQueryFactory")
            .field("event_journal_table", &self.event_journal_table)
            .field("snapshots_table", &self.snapshots_table)
            .finish()
    }
}

impl SqlQueryFactory {
    pub fn new(event_journal_table: &str, offsets_table: &str) -> Self {
        Self {
            event_journal_table: SmolStr::new(event_journal_table),
            snapshots_table: None,
            offsets_table: SmolStr::new(offsets_table),

            offsets_projection_id_column: SmolStr::new(PROJECTION_ID_COL),
            offsets_persistence_id_column: SmolStr::new(PERSISTENCE_ID_COL),
            offsets_current_offset_column: SmolStr::new(CURRENT_OFFSET_COL),
            persistence_id_column: SmolStr::new(PERSISTENCE_ID_COL),
            sequence_nr_column: SmolStr::new(SEQUENCE_NR_COL),
            event_manifest_column: SmolStr::new(EVENT_MANIFEST_COL),
            event_payload_column: SmolStr::new(EVENT_PAYLOAD_COL),
            snapshot_manifest_column: SmolStr::new(SNAPSHOT_MANIFEST_COL),
            snapshot_payload_column: SmolStr::new(SNAPSHOT_PAYLOAD_COL),
            where_persistence_id: OnceCell::new(),
            select_persistence_ids: OnceCell::new(),
            select_event: OnceCell::new(),
            select_events_range: OnceCell::new(),
            // select_all_latest_events: OnceCell::new(),
            select_latest_events: OnceCell::new(),
            append_event: OnceCell::new(),
            delete_event: OnceCell::new(),
            delete_events_range: OnceCell::new(),
            delete_latest_events: OnceCell::new(),
            select_snapshot: OnceCell::new(),
            update_or_insert: OnceCell::new(),
            // insert_snapshot: OnceCell::new(),
            // update_snapshot: OnceCell::new(),
            delete_snapshot: OnceCell::new(),
            clear_aggregate_events: OnceCell::new(),
        }
    }

    #[allow(dead_code)]
    pub fn with_snapshots_table(self, snapshots_table: &str) -> Self {
        Self {
            snapshots_table: Some(SmolStr::new(snapshots_table)),
            ..self
        }
    }

    #[allow(dead_code)]
    pub fn with_persistence_id_column(self, persistence_id_column: &str) -> Self {
        Self {
            persistence_id_column: SmolStr::new(persistence_id_column),
            ..self
        }
    }

    #[allow(dead_code)]
    pub fn with_sequence_nr_column(self, sequence_nr_column: &str) -> Self {
        Self {
            sequence_nr_column: SmolStr::new(sequence_nr_column),
            ..self
        }
    }

    #[allow(dead_code)]
    pub fn with_event_manifest_column(self, event_manifest_column: &str) -> Self {
        Self {
            event_manifest_column: SmolStr::new(event_manifest_column),
            ..self
        }
    }

    #[allow(dead_code)]
    pub fn with_event_payload_column(self, event_payload_column: &str) -> Self {
        Self {
            event_payload_column: SmolStr::new(event_payload_column),
            ..self
        }
    }

    #[allow(dead_code)]
    pub fn with_snapshot_manifest_column(self, snapshot_manifest_column: &str) -> Self {
        Self {
            snapshot_manifest_column: SmolStr::new(snapshot_manifest_column),
            ..self
        }
    }

    #[allow(dead_code)]
    pub fn with_snapshot_payload_column(self, snapshot_payload_column: &str) -> Self {
        Self {
            snapshot_payload_column: SmolStr::new(snapshot_payload_column),
            ..self
        }
    }

    pub fn event_journal_table(&self) -> &str {
        self.event_journal_table.as_str()
    }

    pub fn snapshots_table(&self) -> &str {
        self.snapshots_table
            .as_ref()
            .expect("No snapshots_table provided")
    }

    pub fn persistence_id_column(&self) -> &str {
        self.persistence_id_column.as_str()
    }

    pub fn sequence_number_column(&self) -> &str {
        self.sequence_nr_column.as_str()
    }

    pub fn event_manifest_column(&self) -> &str {
        self.event_manifest_column.as_str()
    }

    pub fn event_payload_column(&self) -> &str {
        self.event_payload_column.as_str()
    }

    pub fn snapshot_manifest_column(&self) -> &str {
        self.snapshot_manifest_column.as_str()
    }

    pub fn snapshot_payload_column(&self) -> &str {
        self.snapshot_payload_column.as_str()
    }

    fn where_persistence_id(&self) -> &str {
        self.where_persistence_id
            .get_or_init(|| format!("{} = $1", self.persistence_id_column()))
    }

    #[inline]
    pub fn select_persistence_ids(&self) -> &str {
        self.select_persistence_ids.get_or_init(|| {
            sql::Select::new()
                .select(format!("DISTINCT {}", self.persistence_id_column()).as_str())
                .from(self.event_journal_table())
                .to_string()
        })
    }

    #[inline]
    pub fn select_event(&self) -> &str {
        self.select_event.get_or_init(|| {
            sql::Select::new()
                .select(&EVENT_COLUMNS_REP)
                .from(self.event_journal_table())
                .where_clause(self.where_persistence_id())
                .and(format!("{} = $2", self.sequence_number_column()).as_str())
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
                .and("is_deleted = FALSE")
                .and(format!("$2 <= {sequence_nr_col}").as_str())
                .and(format!("{sequence_nr_col} < $3").as_str())
                .order_by(sequence_nr_col)
                .to_string()
        })
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

    #[inline]
    pub fn select_latest_events(&self) -> &str {
        self.select_latest_events.get_or_init(|| {
            let sequence_nr_col = self.sequence_number_column();

            sql::Select::new()
                .select(&EVENT_COLUMNS_REP)
                .from(self.event_journal_table())
                .where_clause(self.where_persistence_id())
                .and("is_deleted = FALSE")
                .and(format!("$2 <= {sequence_nr_col}").as_str())
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
                .values(EVENT_VALUES_REP.as_str())
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
                .and(format!("{} = $2", self.sequence_number_column()).as_str())
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
                .and(format!("$2 <= {sequence_nr_col}").as_str())
                .and(format!("{sequence_nr_col} < $3").as_str())
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
                .and(format!("$2 <= {}", self.sequence_number_column()).as_str())
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
            let table = self.snapshots_table.as_ref().expect("No snapshot table set").as_str();
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
                .values(SNAPSHOTS_VALUES_REP.as_str())
                .on_conflict(&conflict_clause)
                .to_string()
        })
    }

    // #[inline]
    // pub fn insert_snapshot(&self) -> &str {
    //     self.insert_snapshot.get_or_init(|| {
    //         sql::Insert::new()
    //             .insert_into(
    //                 format!(
    //                     "{} ( {} )",
    //                     self.snapshots_table(),
    //                     SNAPSHOTS_COLUMNS_REP.as_str(),
    //                 )
    //                 .as_str(),
    //             )
    //             .values(SNAPSHOTS_VALUES_REP.as_str())
    //             .to_string()
    //     })
    // }
    //
    // #[allow(dead_code)]
    // #[inline]
    // pub fn update_snapshot(&self) -> &str {
    //     self.update_snapshot.get_or_init(|| {
    //         sql::Update::new()
    //             .update(self.snapshots_table())
    //             .set(
    //                 format!(
    //                     "{} = $2, {} = $3, created_at = $4",
    //                     self.sequence_number_column(),
    //                     self.snapshot_payload_column()
    //                 )
    //                 .as_str(),
    //             )
    //             .where_clause(self.where_persistence_id())
    //             .to_string()
    //     })
    // }

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
