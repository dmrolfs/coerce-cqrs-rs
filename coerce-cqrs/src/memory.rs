use crate::projection::processor::{
    AggregateEntries, AggregateSequences, EntryPayloadTypes, ProcessorSource,
    ProcessorSourceProvider, ProcessorSourceRef,
};
use crate::projection::PersistenceId;
use anyhow::Context;
use coerce::persistent::journal::provider::StorageProvider;
use coerce::persistent::journal::storage::{JournalEntry, JournalStorage, JournalStorageRef};
use parking_lot::RwLock;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

//todo: remove in favor of coerce::persistent::journal::inmemory
//todo: copy retained here only until unit tests suite completed
//      for crate persistence and projection parts.

#[derive(Debug)]
pub struct InMemoryStorageProvider {
    store: Arc<InMemoryJournalStorage>,
}

impl Default for InMemoryStorageProvider {
    fn default() -> Self {
        Self {
            store: Arc::new(InMemoryJournalStorage::default()), //storage_key_codec)),
        }
    }
}

impl StorageProvider for InMemoryStorageProvider {
    fn journal_storage(&self) -> Option<JournalStorageRef> {
        Some(self.store.clone())
    }
}

impl ProcessorSourceProvider for InMemoryStorageProvider {
    fn processor_source(&self) -> Option<ProcessorSourceRef> {
        Some(self.store.clone())
    }
}

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

#[async_trait]
impl ProcessorSource for InMemoryJournalStorage {
    async fn read_persistence_ids(
        &self,
        entry_types: EntryPayloadTypes,
    ) -> anyhow::Result<Vec<PersistenceId>> {
        let store = self.store.read();
        store
            .iter()
            .filter_map(|(pid_rep, actor_journal)| {
                let known_entry = actor_journal
                    .messages
                    .iter()
                    .any(|entry| entry_types.is_known(&entry.payload_type));
                if known_entry {
                    Some(
                        PersistenceId::from_str(pid_rep.as_str()).with_context(|| {
                            format!("failed to parse persistence_id from {pid_rep}")
                        }),
                    )
                } else {
                    None
                }
            })
            .collect()
    }

    #[instrument(level = "debug")]
    async fn read_bulk_latest_messages(
        &self,
        sequences: AggregateSequences,
    ) -> anyhow::Result<Option<AggregateEntries>> {
        let mut result = HashMap::with_capacity(sequences.len());
        for (persistence_id, sequence) in sequences {
            let latest_entries = self
                .read_latest_messages(&persistence_id.as_persistence_id(), sequence.unwrap_or(0))
                .await?;

            debug!("{persistence_id}[{sequence:?}] latest messages: {latest_entries:?}");

            if let Some(entries) = latest_entries {
                result.insert(persistence_id, entries);
            }
        }

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
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

    #[instrument(level = "debug", skip(entries))]
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
        let entry = self
            .store
            .read()
            .get(persistence_id)
            .and_then(|j| j.snapshots.last().cloned());
        Ok(entry)
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
                        .find(|(_index, j)| from_sequence < j.sequence) //todo: test off by one
                        .map(|(index, j)| {
                            debug!(sequence=%j.sequence, %from_sequence, "found starting message: {index}");
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
        let message = self.store.read().get(persistence_id).and_then(|journal| {
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
        #[allow(clippy::significant_drop_in_scrutinee)]
        match self.store.read().get(persistence_id) {
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
        self.store.write().remove(persistence_id);
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
