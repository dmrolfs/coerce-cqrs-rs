use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::journal::storage::{JournalEntry, JournalStorage, JournalStorageRef};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
struct ActorJournal {
    snapshots: Vec<JournalEntry>,
    messages: Vec<JournalEntry>,
}

impl ActorJournal {
    pub fn from_snapshot(entry: JournalEntry) -> Self {
        Self {
            snapshots: vec![entry],
            messages: vec![],
        }
    }

    pub fn from_message(entry: JournalEntry) -> Self {
        Self {
            snapshots: vec![],
            messages: vec![entry],
        }
    }
}

#[derive(Debug, Default)]
pub struct InMemoryJournalStorage {
    store: RwLock<HashMap<String, ActorJournal>>,
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryStorageProvider {
    store: Arc<InMemoryJournalStorage>,
}

impl InMemoryStorageProvider {
    pub fn new() -> Self {
        Self::default()
    }
}

impl StorageProvider for InMemoryStorageProvider {
    fn journal_storage(&self) -> Option<JournalStorageRef> {
        Some(self.store.clone())
    }
}

#[async_trait]
impl JournalStorage for InMemoryJournalStorage {
    #[instrument(level = "trace", skip(self))]
    async fn write_snapshot(
        &self,
        persistence_id: &str,
        entry: JournalEntry,
    ) -> anyhow::Result<()> {
        let mut store = self.store.write();
        if let Some(journal) = store.get_mut(persistence_id) {
            journal.snapshots.push(entry);
        } else {
            store.insert(
                persistence_id.to_string(),
                ActorJournal::from_snapshot(entry),
            );
        }

        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    async fn write_message(&self, persistence_id: &str, entry: JournalEntry) -> anyhow::Result<()> {
        let mut store = self.store.write();
        if let Some(journal) = store.get_mut(persistence_id) {
            journal.messages.push(entry);
        } else {
            store.insert(
                persistence_id.to_string(),
                ActorJournal::from_message(entry),
            );
        }

        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    async fn read_latest_snapshot(
        &self,
        persistence_id: &str,
    ) -> anyhow::Result<Option<JournalEntry>> {
        let store = self.store.read();

        Ok(store
            .get(persistence_id)
            .and_then(|j| j.snapshots.last().cloned()))
    }

    #[instrument(level = "trace", skip(self))]
    async fn read_latest_messages(
        &self,
        persistence_id: &str,
        from_sequence: i64,
    ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
        let store = self.store.read();
        Ok(store.get(persistence_id).map(|journal| {
            let messages = match from_sequence {
                0 => journal.messages.clone(),
                from_sequence => {
                    let starting_message = journal
                        .messages
                        .iter()
                        .enumerate()
                        .find(|(_index, j)| j.sequence > from_sequence)
                        .map(|(index, _j)| index);

                    starting_message.map_or_else(Vec::new, |starting_index| {
                        journal.messages[starting_index..].to_vec()
                    })
                }
            };

            trace!(
                "storage found {} messages for persistence_id={}, from_sequence={}",
                messages.len(),
                persistence_id,
                from_sequence
            );

            messages
        }))
    }

    #[instrument(level = "trace", skip(self))]
    async fn delete_all(&self, persistence_id: &str) -> anyhow::Result<()> {
        let mut store = self.store.write();
        store.remove(persistence_id);
        Ok(())
    }
}
