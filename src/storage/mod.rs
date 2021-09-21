use crate::utils::{TtoU8, U8toT};
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot, SnapshotMetadata};
use raft::util::limit_size;
use raft::{RaftState, Storage, StorageError};
use rocksdb::DB;
use slog::Logger;
use std::cmp;
use std::convert::TryInto;
use std::error::Error;
use std::path::Path;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
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
pub struct RocksDBStorageCore {
    db: DB,
    logger: Logger,
}
impl RocksDBStorageCore {
    fn new(path: &Path, logger: Logger) -> Self {
        let db = DB::open_default(path).expect("RocksDB open error");
        RocksDBStorageCore { db, logger }
    }
}
impl RocksDBStorageCore {
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

    fn get_raft_state(&self) -> RaftState {
        RaftState {
            hard_state: self.get_hard_state(),
            conf_state: self.get_conf_state(),
        }
    }
}
impl RocksDBStorageCore {
    fn first_index(&self) -> u64 {
        match self.entries_is_empty() {
            true => self.get_snapshot_metadata().index + 1,
            false => self.get_first_entry_index(),
        }
    }

    fn last_index(&self) -> u64 {
        match self.entries_is_empty() {
            true => self.get_snapshot_metadata().index,
            false => self.get_last_entry_index(),
        }
    }

    fn set_first_entry_index(&self, index: u64) {
        self.db
            .put(RAFT_FIRST_ENTRY_INDEX, index.to_be_bytes())
            .unwrap()
    }

    fn get_first_entry_index(&self) -> u64 {
        u64::from_be_bytes(
            self.db
                .get(RAFT_FIRST_ENTRY_INDEX)
                .unwrap()
                .unwrap()
                .try_into()
                .unwrap(),
        )
    }

    fn set_last_entry_index(&self, index: u64) {
        self.db
            .put(RAFT_LAST_ENTRY_INDEX, index.to_be_bytes())
            .unwrap()
    }

    fn get_last_entry_index(&self) -> u64 {
        u64::from_be_bytes(
            self.db
                .get(RAFT_LAST_ENTRY_INDEX)
                .unwrap()
                .unwrap()
                .try_into()
                .unwrap(),
        )
    }

    #[inline]
    fn has_entry_at(&self, index: u64) -> bool {
        !self.entries_is_empty() && index >= self.first_index() && index <= self.last_index()
    }

    // maintain first/last index of entries
    fn set_entry_at_back(&self, e: Entry) {
        if self.entries_is_empty() {
            self.set_first_entry_index(e.index);
            self.set_last_entry_index(e.index);
            self.set_entry_empty_flag(false);
        } else {
            self.set_last_entry_index(e.index);
        }
        let key = RAFT_ENTRY_PREFIX.to_string() + &e.index.to_string();
        self.db.put(key, e.t_to_u8()).unwrap();
    }

    fn get_entry(&self, index: u64) -> Entry {
        let key = RAFT_ENTRY_PREFIX.to_string() + &index.to_string();
        Entry::u8_to_t(self.db.get(key).unwrap().unwrap())
    }

    // maintain first/last index of entries
    fn delete_entry_from_front(&self, index: u64) {
        if index == self.get_first_entry_index() {
            self.set_first_entry_index(index + 1);
        }
        if self.get_first_entry_index()>self.get_last_entry_index(){
            self.set_entry_empty_flag(true);
        }
        let key = RAFT_ENTRY_PREFIX.to_string() + &index.to_string();
        self.db.delete(key).unwrap();
    }
    fn delete_entry_from_back(&self, index: u64) {
        if index == self.get_last_entry_index() {
            self.set_last_entry_index(index - 1);
        }
        if self.get_first_entry_index()>self.get_last_entry_index(){
            self.set_entry_empty_flag(true);
        }
        let key = RAFT_ENTRY_PREFIX.to_string() + &index.to_string();
        self.db.delete(key).unwrap();
    }

    fn clear_entries(&self) {
        if self.entries_is_empty() {
            return;
        }
        let first = self.get_first_entry_index();
        let last = self.get_last_entry_index();
        for i in (first..=last).rev() {
            self.delete_entry_from_back(i);
        }
    }

    fn set_entry_empty_flag(&self, flag: bool) {
        self.db
            .put(RAFT_ENTRY_EMPTY_FLAG, flag.to_string())
            .unwrap();
    }

    // equal to get_entry_empty_flag,just alias
    fn entries_is_empty(&self) -> bool {
        match String::from_utf8(self.db.get(RAFT_ENTRY_EMPTY_FLAG).unwrap().unwrap())
            .unwrap()
            .as_str()
        {
            "true" => true,
            "false" => false,
            _ => false,
        }
    }
}
impl RocksDBStorageCore {
    fn set_snapshot_metadata(&self, meta: SnapshotMetadata) {
        self.db.put(SNAPSHOT_METADATA, meta.t_to_u8()).unwrap();
    }
    fn get_snapshot_metadata(&self) -> SnapshotMetadata {
        SnapshotMetadata::u8_to_t(self.db.get(SNAPSHOT_METADATA).unwrap().unwrap())
    }
    fn set_snapshot_data(&self, data: Vec<u8>) {
        self.db.put(SNAPSHOT_DATA, data).unwrap();
    }
    fn get_snapshot_data(&self) -> Vec<u8> {
        self.db.get(SNAPSHOT_DATA).unwrap().unwrap()
    }
}
impl RocksDBStorageCore {
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
        let diff = index - self.get_first_entry_index();
        let mut hs = self.get_hard_state();
        hs.commit = index;
        hs.term = self.get_entry(diff).term;
        self.set_hard_state(hs);
        Ok(())
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

        self.set_snapshot_metadata(meta.clone());
        self.set_snapshot_data(snapshot.data);
        let mut hs = self.get_hard_state();
        hs.term = cmp::max(hs.term, meta.term);
        hs.commit = index;
        self.clear_entries();

        self.set_hard_state(hs);
        // Update conf states.
        self.set_conf_state(meta.take_conf_state());
        Ok(())
    }

    fn snapshot(&self) -> Snapshot {
        let mut snapshot = Snapshot::default();
        //TODO just guess
        snapshot.set_data(self.get_snapshot_data());

        // We assume all entries whose indexes are less than `hard_state.commit`
        // have been applied, so use the latest commit index to construct the snapshot.
        // TODO: This is not true for async ready.
        let meta = snapshot.mut_metadata();
        let hs = self.get_hard_state();
        let snapshot_metadata = self.get_snapshot_metadata();
        meta.index = hs.commit;
        meta.term = match meta.index.cmp(&snapshot_metadata.index) {
            cmp::Ordering::Equal => snapshot_metadata.term,
            cmp::Ordering::Greater => {
                let offset = self.get_first_entry_index();
                self.get_entry(meta.index - offset).term
            }
            cmp::Ordering::Less => {
                panic!(
                    "commit {} < snapshot_metadata.index {}",
                    meta.index, snapshot_metadata.index
                );
            }
        };

        meta.set_conf_state(self.get_conf_state());
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
        // compact_index between [self.first_index()+1,self.last_index()+1]
        let first_entry_index = self.get_first_entry_index();
        let offset = compact_index - first_entry_index;
        // we need remove [0,offset-1] -> ..offset
        for i in 0..offset {
            self.delete_entry_from_front(first_entry_index + i);
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
        let diff = ents[0].index - self.first_index();
        // need remove [diff,last_entry_index] -> diff..
        let last_entry_index = self.get_last_entry_index();
        for i in (diff..=last_entry_index).rev() {
            self.delete_entry_from_back(i);
        }
        for e in ents {
            self.set_entry_at_back(e.clone());
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

    /// Opens up a read lock on the storage and returns a guard handle. Use this
    /// with functions that don't require mutation.
    pub fn rl(&self) -> RwLockReadGuard<'_, RocksDBStorageCore> {
        self.core.read().unwrap()
    }

    /// Opens up a write lock on the storage and returns guard handle. Use this
    /// with functions that take a mutable reference to self.
    pub fn wl(&self) -> RwLockWriteGuard<'_, RocksDBStorageCore> {
        self.core.write().unwrap()
    }
}
impl Storage for RocksDBStorage {
    fn initial_state(&self) -> raft::Result<raft::RaftState> {
        Ok(self.rl().get_raft_state())
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<Entry>> {
        let max_size = max_size.into();
        let core = self.rl();
        if low < core.first_index() {
            return Err(raft::Error::Store(StorageError::Compacted));
        }

        if high > core.last_index() + 1 {
            panic!(
                "index out of bound (last: {}, high: {})",
                core.last_index() + 1,
                high
            );
        }

        let mut ents: Vec<Entry> = Vec::new();
        for i in low..high {
            ents.push(core.get_entry(i))
        }
        limit_size(&mut ents, max_size);
        Ok(ents)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let core = self.rl();
        let snapshot_metadata = core.get_snapshot_metadata();
        if idx == snapshot_metadata.index {
            return Ok(snapshot_metadata.term);
        }

        let offset = core.first_index();
        if idx < offset {
            return Err(raft::Error::Store(StorageError::Compacted));
        }

        if idx > core.last_index() {
            return Err(raft::Error::Store(StorageError::Unavailable));
        }
        Ok(core.get_entry(idx).term)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.rl().first_index())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.rl().last_index())
    }

    fn snapshot(&self, request_index: u64) -> raft::Result<Snapshot> {
        let core = self.wl();

        let mut snap = core.snapshot();
        if snap.get_metadata().index < request_index {
            snap.mut_metadata().index = request_index;
        }
        Ok(snap)
    }
}
