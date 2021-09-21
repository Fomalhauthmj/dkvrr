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

    fn set_raft_state(&self, rs: RaftState) {
        self.set_hard_state(rs.hard_state);
        self.set_conf_state(rs.conf_state);
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
        if self.get_first_entry_index() > self.get_last_entry_index() {
            self.set_entry_empty_flag(true);
        }
        let key = RAFT_ENTRY_PREFIX.to_string() + &index.to_string();
        self.db.delete(key).unwrap();
    }
    fn delete_entry_from_back(&self, index: u64) {
        if index == self.get_last_entry_index() {
            self.set_last_entry_index(index - 1);
        }
        if self.get_first_entry_index() > self.get_last_entry_index() {
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

    fn set_entries(&self,ents: &[Entry]){
        for e in ents{
            self.set_entry_at_back(e.clone());
        }
    }

    fn set_entry_empty_flag(&self, flag: bool) {
        self.db
            .put(RAFT_ENTRY_EMPTY_FLAG, flag.to_string())
            .unwrap();
    }

    // equal to get_entry_empty_flag,just alias
    fn entries_is_empty(&self) -> bool {
        match self.db.get(RAFT_ENTRY_EMPTY_FLAG).unwrap() {
            Some(data) => match String::from_utf8(data).unwrap().as_str() {
                "true" => true,
                "false" => false,
                _ => panic!("unexpected value"),
            },
            None => {
                self.db
                    .put(RAFT_ENTRY_EMPTY_FLAG, true.to_string())
                    .unwrap();
                true
            }
        }
    }
}
impl RocksDBStorageCore {
    fn set_snapshot_metadata(&self, meta: SnapshotMetadata) {
        self.db.put(SNAPSHOT_METADATA, meta.t_to_u8()).unwrap();
    }
    fn get_snapshot_metadata(&self) -> SnapshotMetadata {
        match self.db.get(SNAPSHOT_METADATA).unwrap() {
            Some(data) => SnapshotMetadata::u8_to_t(data),
            None => {
                let default = SnapshotMetadata::default();
                self.set_snapshot_metadata(default.clone());
                default
            }
        }
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
        let mut hs = self.get_hard_state();
        hs.commit = index;
        hs.term = self.get_entry(index).term;
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
            cmp::Ordering::Greater => self.get_entry(meta.index).term,
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
        // need remove [ents[0].index,last_entry_index]
        let last_entry_index = self.get_last_entry_index();
        for i in (ents[0].index..=last_entry_index).rev() {
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

#[cfg(test)]
mod test {
    use std::panic::{self, AssertUnwindSafe};

    use protobuf::Message as PbMessage;

    use raft::eraftpb::{ConfState, Entry, Snapshot};

    use raft::{Error as RaftError, RaftState, StorageError};
    use slog::{o, Logger};

    use super::{RocksDBStorage, Storage};

    fn temp_rocksdb() -> RocksDBStorage {
        let mut temp_path = std::env::temp_dir();
        temp_path.push("temp_RocksDB".to_string() + &rand::random::<u64>().to_string());
        RocksDBStorage::new(
            &temp_path,
            Logger::root(slog::Discard, o!("from"=>"temp_RocksDB")),
        )
    }

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::default();
        e.term = term;
        e.index = index;
        e
    }

    fn size_of<T: PbMessage>(m: &T) -> u32 {
        m.compute_size() as u32
    }

    fn new_snapshot(index: u64, term: u64, voters: Vec<u64>) -> Snapshot {
        let mut s = Snapshot::default();
        s.mut_metadata().index = index;
        s.mut_metadata().term = term;
        s.mut_metadata().mut_conf_state().voters = voters;
        s
    }

    #[test]
    fn test_storage_term() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (2, Err(RaftError::Store(StorageError::Compacted))),
            (3, Ok(3)),
            (4, Ok(4)),
            (5, Ok(5)),
            (6, Err(RaftError::Store(StorageError::Unavailable))),
        ];

        for (i, (idx, wterm)) in tests.drain(..).enumerate() {
            let storage = temp_rocksdb();
            storage.wl().set_entries(&ents);

            let t = storage.term(idx);
            if t != wterm {
                panic!("#{}: expect res {:?}, got {:?}", i, wterm, t);
            }
        }
    }

    #[test]
    fn test_storage_entries() {
        let ents = vec![
            new_entry(3, 3),
            new_entry(4, 4),
            new_entry(5, 5),
            new_entry(6, 6),
        ];
        let max_u64 = u64::max_value();
        let mut tests = vec![
            (
                2,
                6,
                max_u64,
                Err(RaftError::Store(StorageError::Compacted)),
            ),
            (3, 4, max_u64, Ok(vec![new_entry(3, 3)])),
            (4, 5, max_u64, Ok(vec![new_entry(4, 4)])),
            (4, 6, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
            (
                4,
                7,
                max_u64,
                Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
            // even if maxsize is zero, the first entry should be returned
            (4, 7, 0, Ok(vec![new_entry(4, 4)])),
            // limit to 2
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2])),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) / 2),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) - 1),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            // all
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3])),
                Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
        ];
        for (i, (lo, hi, maxsize, wentries)) in tests.drain(..).enumerate() {
            let storage = temp_rocksdb();
            storage.wl().set_entries(&ents);
            let e = storage.entries(lo, hi, maxsize);
            if e != wentries {
                panic!("#{}: expect entries {:?}, got {:?}", i, wentries, e);
            }
        }
    }

    #[test]
    fn test_storage_last_index() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let storage = temp_rocksdb();
        storage.wl().set_entries(&ents);

        let wresult = Ok(5);
        let result = storage.last_index();
        if result != wresult {
            panic!("want {:?}, got {:?}", wresult, result);
        }

        storage.wl().append(&[new_entry(6, 5)]).unwrap();
        let wresult = Ok(6);
        let result = storage.last_index();
        if result != wresult {
            panic!("want {:?}, got {:?}", wresult, result);
        }
    }

    #[test]
    fn test_storage_first_index() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let storage = temp_rocksdb();
        storage.wl().set_entries(&ents);

        assert_eq!(storage.first_index(), Ok(3));
        storage.wl().compact(4).unwrap();
        assert_eq!(storage.first_index(), Ok(4));
    }

    #[test]
    fn test_storage_compact() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![(2, 3, 3, 3), (3, 3, 3, 3), (4, 4, 4, 2), (5, 5, 5, 1)];
        for (i, (idx, windex, wterm, wlen)) in tests.drain(..).enumerate() {
            let storage = temp_rocksdb();
            storage.wl().set_entries(&ents);

            storage.wl().compact(idx).unwrap();
            let index = storage.first_index().unwrap();
            if index != windex {
                panic!("#{}: want {}, index {}", i, windex, index);
            }
            let term = if let Ok(v) = storage.entries(index, index + 1, 1) {
                v.first().map_or(0, |e| e.term)
            } else {
                0
            };
            if term != wterm {
                panic!("#{}: want {}, term {}", i, wterm, term);
            }
            let last = storage.last_index().unwrap();
            let len = storage.entries(index, last + 1, 100).unwrap().len();
            if len != wlen {
                panic!("#{}: want {}, term {}", i, wlen, len);
            }
        }
    }

    #[test]
    fn test_storage_create_snapshot() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let nodes = vec![1, 2, 3];
        let mut conf_state = ConfState::default();
        conf_state.voters = nodes.clone();
        /*
        let unavailable = Err(RaftError::Store(
            StorageError::SnapshotTemporarilyUnavailable,
        ));
        */
        let mut tests = vec![
            (4, Ok(new_snapshot(4, 4, nodes.clone())), 0),
            (5, Ok(new_snapshot(5, 5, nodes.clone())), 5),
            (5, Ok(new_snapshot(6, 5, nodes)), 6),
            //(5, unavailable, 6),
        ];
        for (i, (idx, wresult, windex)) in tests.drain(..).enumerate() {
            let storage = temp_rocksdb();
            storage.wl().set_entries(&ents);

            let mut raft_state = RaftState::default();
            raft_state.hard_state.commit = idx;
            raft_state.hard_state.term = idx;
            raft_state.conf_state = conf_state.clone();

            storage.wl().set_raft_state(raft_state);

            /*
            if wresult.is_err() {
                storage.wl().trigger_snap_unavailable();
            }
            */

            let result = storage.snapshot(windex);
            if result != wresult {
                panic!("#{}: want {:?}, got {:?}", i, wresult, result);
            }
        }
    }

    #[test]
    fn test_storage_append() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (
                vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
                Some(vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
                Some(vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)]),
            ),
            (
                vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ],
                Some(vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ]),
            ),
            // overwrite compacted raft logs is not allowed
            (
                vec![new_entry(2, 3), new_entry(3, 3), new_entry(4, 5)],
                None,
            ),
            // truncate the existing entries and append
            (
                vec![new_entry(4, 5)],
                Some(vec![new_entry(3, 3), new_entry(4, 5)]),
            ),
            // direct append
            (
                vec![new_entry(6, 6)],
                Some(vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 6),
                ]),
            ),
        ];
        for (i, (entries, wentries)) in tests.drain(..).enumerate() {
            let storage = temp_rocksdb();
            storage.wl().set_entries(&ents);
            let res = panic::catch_unwind(AssertUnwindSafe(|| storage.wl().append(&entries)));
            if let Some(wentries) = wentries {
                assert!(res.is_ok());
                let e = &storage
                    .entries(
                        storage.first_index().unwrap(),
                        storage.last_index().unwrap() + 1,
                        u64::MAX,
                    )
                    .unwrap();
                if *e != wentries {
                    panic!("#{}: want {:?}, entries {:?}", i, wentries, e);
                }
            } else {
                assert!(res.is_err());
            }
        }
    }

    #[test]
    fn test_storage_apply_snapshot() {
        let nodes = vec![1, 2, 3];
        let storage = temp_rocksdb();

        // Apply snapshot successfully
        let snap = new_snapshot(4, 4, nodes.clone());
        assert!(storage.wl().apply_snapshot(snap).is_ok());

        // Apply snapshot fails due to StorageError::SnapshotOutOfDate
        let snap = new_snapshot(3, 3, nodes);
        assert!(storage.wl().apply_snapshot(snap).is_err());
    }
}
