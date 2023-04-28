use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::journal::storage::{JournalEntry, JournalStorage, JournalStorageRef};
use parking_lot::RwLock;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

//todo: remove in favor of coerce::persistent::journal::inmemory
//todo: copy retained here only until unit tests suite completed
//      for crate persistence and projection parts.

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

    pub fn from_messages(entries: Vec<JournalEntry>) -> Self {
        Self {
            snapshots: vec![],
            messages: entries,
        }
    }
}

#[derive(Debug, Default)]
pub struct InMemoryJournalStorage {
    store: RwLock<HashMap<String, ActorJournal>>,
}

#[derive(Debug, Default)]
pub struct InMemoryStorageProvider {
    store: Arc<InMemoryJournalStorage>,
}

#[allow(dead_code)]
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
    #[instrument(level = "debug")]
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

    #[instrument(level = "debug")]
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

    #[instrument(level = "debug")]
    async fn write_message_batch(
        &self,
        persistence_id: &str,
        mut entries: Vec<JournalEntry>,
    ) -> anyhow::Result<()> {
        let mut store = self.store.write();
        if let Some(journal) = store.get_mut(persistence_id) {
            journal.messages.append(&mut entries);
        } else {
            store.insert(
                persistence_id.to_string(),
                ActorJournal::from_messages(entries),
            );
        }

        Ok(())
    }

    #[instrument(level = "debug")]
    async fn read_latest_snapshot(
        &self,
        persistence_id: &str,
    ) -> anyhow::Result<Option<JournalEntry>> {
        let store = self.store.read();

        Ok(store
            .get(persistence_id)
            .and_then(|j| j.snapshots.last().cloned()))
    }

    #[instrument(level = "debug")]
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
                        .map(|(index, j)| {
                            info!(%index, sequence=%j.sequence, %from_sequence, "DMR: found starting message");
                            index
                        });

                    starting_message.map_or_else(Vec::new, |starting_index| {
                        journal.messages[starting_index..].to_vec()
                    })
                }
            };

            debug!(
                "storage found {} messages for persistence_id={}, from_sequence={}",
                messages.len(),
                persistence_id,
                from_sequence
            );

            messages
        }))
    }

    #[instrument(level = "debug")]
    async fn read_message(
        &self,
        persistence_id: &str,
        sequence_id: i64,
    ) -> anyhow::Result<Option<JournalEntry>> {
        let store = self.store.read();
        let message = store.get(persistence_id).and_then(|journal| {
            journal
                .messages
                .iter()
                .find(|entry| entry.sequence == sequence_id)
                .cloned()
        });
        Ok(message)
    }

    async fn read_messages(
        &self,
        persistence_id: &str,
        from_sequence: i64,
        to_sequence: i64,
    ) -> anyhow::Result<Option<Vec<JournalEntry>>> {
        let store = self.store.read();
        match store.get(persistence_id) {
            None => Ok(None),
            Some(journal) if journal.messages.is_empty() => Ok(None),
            Some(journal) => {
                let first_seq = journal.messages.first().map(|m| m.sequence).unwrap();
                let final_seq = journal.messages.last().map(|m| m.sequence).unwrap();

                if final_seq <= to_sequence {
                    if from_sequence <= first_seq {
                        Ok(Some(journal.messages.clone()))
                    } else {
                        let starting_message = Self::find_starting_message(journal, from_sequence);

                        starting_message.map_or_else(
                            || Ok(Some(vec![])),
                            |starting_index| Ok(Some(journal.messages[starting_index..].to_vec())),
                        )
                    }
                } else if from_sequence <= first_seq {
                    let ending_message = Self::find_ending_message(journal, to_sequence);

                    ending_message.map_or_else(
                        || Ok(Some(vec![])),
                        |ending_index| Ok(Some(journal.messages[..ending_index].to_vec())),
                    )
                } else {
                    let starting_message = Self::find_starting_message(journal, from_sequence);
                    let ending_message = Self::find_ending_message(journal, to_sequence);

                    starting_message.zip(ending_message).map_or_else(
                        || Ok(Some(vec![])),
                        |(starting_index, ending_index)| {
                            Ok(Some(
                                journal.messages[starting_index..ending_index].to_vec(),
                            ))
                        },
                    )
                }
            }
        }
    }

    async fn delete_messages_to(
        &self,
        persistence_id: &str,
        to_sequence: i64,
    ) -> anyhow::Result<()> {
        let mut store = self.store.write();
        if let Entry::Occupied(mut journal) = store.entry(persistence_id.to_string()) {
            let journal: &mut ActorJournal = journal.get_mut();

            fn get_messages_to(to_sequence: i64, journal: &mut ActorJournal) -> Vec<JournalEntry> {
                let starting_message =
                    InMemoryJournalStorage::find_ending_message(journal, to_sequence);
                starting_message.map_or_else(Vec::new, |m| journal.messages.split_off(m))
            }

            let messages = if let Some(newest_msg) = journal.messages.last() {
                if newest_msg.sequence < to_sequence {
                    vec![]
                } else {
                    get_messages_to(to_sequence, journal)
                }
            } else {
                get_messages_to(to_sequence, journal)
            };

            *journal = ActorJournal {
                snapshots: std::mem::take(&mut journal.snapshots),
                messages,
            };
        }

        Ok(())
    }

    #[instrument(level = "debug")]
    async fn delete_all(&self, persistence_id: &str) -> anyhow::Result<()> {
        let mut store = self.store.write();
        store.remove(persistence_id);
        Ok(())
    }
}

impl InMemoryJournalStorage {
    fn find_starting_message(journal: &ActorJournal, from_sequence: i64) -> Option<usize> {
        journal.messages.iter().enumerate().find_map(|(index, j)| {
            if from_sequence < j.sequence {
                Some(index)
            } else {
                None
            }
        })
    }

    fn find_ending_message(journal: &ActorJournal, to_sequence: i64) -> Option<usize> {
        journal
            .messages
            .iter()
            .enumerate()
            .rev()
            .find_map(|(index, j)| {
                if j.sequence <= to_sequence {
                    Some(index)
                } else {
                    None
                }
            })
    }
}