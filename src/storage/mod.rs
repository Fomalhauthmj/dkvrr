use crate::utils::{TtoU8, U8toT};
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot, SnapshotMetadata};
use raft::Storage;
use rocksdb::DB;
use slog::Logger;
use std::cmp;
use std::convert::TryInto;
use std::error::Error;
use std::path::Path;
use std::sync::{Arc, RwLock};
/// RaftState
const RAFT_STATE_HARD_STATE: &str = "RaftStateHardState";
const RAFT_STATE_CONF_STATE: &str = "RaftStateConfState";
/// RaftEntry
const RAFT_ENTRY_PREFIX: &str = "RaftEntry";
const RAFT_ENTRY_EMPTY_FLAG: &str = "RaftEntryEmptyFlag";
const RAFT_FIRST_ENTRY_INDEX: &str = "RaftFirstEntryIndex";
const RAFT_LAST_ENTRY_INDEX: &str = "RaftLastEntryIndex";
/// Snapshot
const SNAPSHOT_METADATA: &str = "SnapshotMetadata";
const SNAPSHOT_DATA: &str = "SnapshotData";
struct RocksDBStorageCore {
    db: DB,
    logger: Logger,
}
impl RocksDBStorageCore {
    fn new(path: &Path, logger: Logger) -> Self {
        let db = DB::open_default(path).expect("RocksDB open error");
        RocksDBStorageCore { db, logger }
    }
}
impl RocksDBStorageCore{
    /// Saves the current HardState.
    fn set_hard_state(&self, hs: HardState) {
        self.db.put(RAFT_STATE_HARD_STATE, hs.t_to_u8()).unwrap();
    }

    /// Get the hard state.
    fn get_hard_state(&self) -> HardState {
        HardState::u8_to_t(self.db.get(RAFT_STATE_HARD_STATE).unwrap().unwrap())
    }

    /// Saves the current ConfState.
    fn set_conf_state(&self, hs: ConfState) {
        self.db.put(RAFT_STATE_CONF_STATE, hs.t_to_u8()).unwrap();
    }

    /// Get the conf state.
    fn get_conf_state(&self) -> ConfState {
        ConfState::u8_to_t(self.db.get(RAFT_STATE_CONF_STATE).unwrap().unwrap())
    }
}
impl RocksDBStorageCore{
    fn first_index(&self)->u64{

    }
    fn last_index(&self)->u64{

    }
    fn set_entry(){

    }
    fn get_entry(){

    }
    fn delete_entry(){

    }
    fn clear_entries(&self,flag:bool){
        self.db.put(RAFT_ENTRY_EMPTY_FLAG, flag.to_string());
        let flag=String::from_utf8(self.db.get(RAFT_ENTRY_EMPTY_FLAG).unwrap().unwrap()).unwrap().as_str();
    }
    fn entries_is_empty(&self,)->bool{
        let flag=String::from_utf8(self.db.get(RAFT_ENTRY_EMPTY_FLAG).unwrap().unwrap()).unwrap().as_str();
    }
}
impl RocksDBStorageCore{
    fn set_snapshot_metadata(){

    }
    fn get_snapshot_metadata(){
        
    }
}
impl RocksDBStorageCore{
    /// Commit to an index.
    ///
    /// # Panics
    ///
    /// Panics if there is no such entry in raft logs.
    pub fn commit_to(&mut self, index: u64) -> Result<(), Box<dyn Error>> {
        assert!(
            self.has_entry_at(index),
            "commit_to {} but the entry does not exist",
            index
        );
        let first_entry_index = u64::from_be_bytes(
            self.db
                .get(RAFT_FIRST_ENTRY_INDEX)
                .unwrap()
                .unwrap()
                .try_into()
                .unwrap(),
        );
        let diff = (index - first_entry_index) as usize;
        let hs = self.mut_hard_state().clone();
        hs.commit = index;
        let key = RAFT_ENTRY_PREFIX.to_string() + &diff.to_string();
        let e = Entry::u8_to_t(self.db.get(key).unwrap().unwrap());
        hs.term = e.term;
        self.set_hard_state(hs);
        Ok(())
    }

    #[inline]
    fn has_entry_at(&self, index: u64) -> bool {
        let empty_flag = u64::from_be_bytes(
            self.db
                .get(RAFT_ENTRY_EMPTY_FLAG)
                .unwrap()
                .unwrap()
                .try_into()
                .unwrap(),
        );
        empty_flag == 0 && index >= self.first_index() && index <= self.last_index()
    }

    fn first_index(&self) -> u64 {
        let empty_flag = u64::from_be_bytes(
            self.db
                .get(RAFT_ENTRY_EMPTY_FLAG)
                .unwrap()
                .unwrap()
                .try_into()
                .unwrap(),
        );
        if empty_flag == 1 {
            SnapshotMetadata::u8_to_t(self.db.get(SNAPSHOT_METADATA).unwrap().unwrap()).index + 1
        } else {
            u64::from_be_bytes(
                self.db
                    .get(RAFT_FIRST_ENTRY_INDEX)
                    .unwrap()
                    .unwrap()
                    .try_into()
                    .unwrap(),
            )
        }
    }

    fn last_index(&self) -> u64 {
        let empty_flag = u64::from_be_bytes(
            self.db
                .get(RAFT_ENTRY_EMPTY_FLAG)
                .unwrap()
                .unwrap()
                .try_into()
                .unwrap(),
        );
        if empty_flag == 1 {
            SnapshotMetadata::u8_to_t(self.db.get(SNAPSHOT_METADATA).unwrap().unwrap()).index
        } else {
            u64::from_be_bytes(
                self.db
                    .get(RAFT_LAST_ENTRY_INDEX)
                    .unwrap()
                    .unwrap()
                    .try_into()
                    .unwrap(),
            )
        }
    }

    /// Overwrites the contents of this Storage object with those of the given snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the snapshot index is less than the storage's first index.
    pub fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> Result<(), Box<dyn Error>> {
        let mut meta = snapshot.take_metadata();
        let index = meta.index;

        if self.first_index() > index {
            return Box::new(Err(
                "the snapshot index is less than the storage's first index",
            ))
            .unwrap();
        }
        self.db.put(SNAPSHOT_METADATA, meta.t_to_u8()).unwrap();
        let hs = self.mut_hard_state();
        hs.term = cmp::max(hs.term, meta.term);
        hs.commit = index;
        self.set_hard_state(hs.clone());
        let first_entry_index = u64::from_be_bytes(
            self.db
                .get(RAFT_FIRST_ENTRY_INDEX)
                .unwrap()
                .unwrap()
                .try_into()
                .unwrap(),
        );
        let last_entry_index = u64::from_be_bytes(
            self.db
                .get(RAFT_LAST_ENTRY_INDEX)
                .unwrap()
                .unwrap()
                .try_into()
                .unwrap(),
        );
        for i in first_entry_index..=last_entry_index {
            let key = RAFT_ENTRY_PREFIX.to_string() + &i.to_string();
            self.db.delete(key).unwrap();
        }
        // Update conf states.
        self.set_conf_state(meta.take_conf_state());
        Ok(())
    }

    fn snapshot(&self) -> Snapshot {
        let mut snapshot = Snapshot::default();

        // We assume all entries whose indexes are less than `hard_state.commit`
        // have been applied, so use the latest commit index to construct the snapshot.
        // TODO: This is not true for async ready.
        let meta = snapshot.mut_metadata();
        meta.index = self.raft_state.hard_state.commit;
        meta.term = match meta.index.cmp(&self.snapshot_metadata.index) {
            cmp::Ordering::Equal => self.snapshot_metadata.term,
            cmp::Ordering::Greater => {
                let offset = self.entries[0].index;
                self.entries[(meta.index - offset) as usize].term
            }
            cmp::Ordering::Less => {
                panic!(
                    "commit {} < snapshot_metadata.index {}",
                    meta.index, self.snapshot_metadata.index
                );
            }
        };

        meta.set_conf_state(self.raft_state.conf_state.clone());
        snapshot
    }

    /// Discards all log entries prior to compact_index.
    /// It is the application's responsibility to not attempt to compact an index
    /// greater than RaftLog.applied.
    ///
    /// # Panics
    ///
    /// Panics if `compact_index` is higher than `Storage::last_index(&self) + 1`.
    pub fn compact(&mut self, compact_index: u64) -> Result<(), Box<dyn Error>> {
        if compact_index <= self.first_index() {
            // Don't need to treat this case as an error.
            return Ok(());
        }

        if compact_index > self.last_index() + 1 {
            panic!(
                "compact not received raft logs: {}, last index: {}",
                compact_index,
                self.last_index()
            );
        }
        let empty_flag = u64::from_be_bytes(
            self.db
                .get(RAFT_ENTRY_EMPTY_FLAG)
                .unwrap()
                .unwrap()
                .try_into()
                .unwrap(),
        );
        if empty_flag == 0 {
            let offset = compact_index - self.first_index();
            for i in diff..last_entry_index {
                let key = RAFT_ENTRY_PREFIX.to_string() + &i.to_string();
                self.db.delete(key).unwrap();
            }
            self.entries.drain(..offset as usize);
        }
        Ok(())
    }

    /// Append the new entries to storage.
    ///
    /// # Panics
    ///
    /// Panics if `ents` contains compacted entries, or there's a gap between `ents` and the last
    /// received entry in the storage.
    pub fn append(&mut self, ents: &[Entry]) -> Result<(), Box<dyn Error>> {
        if ents.is_empty() {
            return Ok(());
        }
        if self.first_index() > ents[0].index {
            panic!(
                "overwrite compacted raft logs, compacted: {}, append: {}",
                self.first_index() - 1,
                ents[0].index,
            );
        }
        if self.last_index() + 1 < ents[0].index {
            panic!(
                "raft logs should be continuous, last index: {}, new appended: {}",
                self.last_index(),
                ents[0].index,
            );
        }

        // Remove all entries overwritten by `ents`.
        let mut diff = ents[0].index - self.first_index();
        let last_entry_index = u64::from_be_bytes(
            self.db
                .get(RAFT_LAST_ENTRY_INDEX)
                .unwrap()
                .unwrap()
                .try_into()
                .unwrap(),
        );
        for i in diff..=last_entry_index {
            let key = RAFT_ENTRY_PREFIX.to_string() + &i.to_string();
            self.db.delete(key).unwrap();
        }
        for e in ents {
            let key = RAFT_ENTRY_PREFIX.to_string() + &diff.to_string();
            diff += 1;
            self.db.put(key, e.t_to_u8()).unwrap();
        }
        Ok(())
    }
}
pub struct RocksDBStorage {
    core: Arc<RwLock<RocksDBStorageCore>>,
}
impl RocksDBStorage {
    pub fn new(path: &Path, logger: Logger) -> Self {
        RocksDBStorage {
            core: Arc::new(RwLock::new(RocksDBStorageCore::new(path, logger))),
        }
    }
}
impl Storage for RocksDBStorage {
    fn initial_state(&self) -> raft::Result<raft::RaftState> {
        todo!()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<Entry>> {
        todo!()
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        todo!()
    }

    fn first_index(&self) -> raft::Result<u64> {
        todo!()
    }

    fn last_index(&self) -> raft::Result<u64> {
        todo!()
    }

    fn snapshot(&self, request_index: u64) -> raft::Result<Snapshot> {
        todo!()
    }
}
