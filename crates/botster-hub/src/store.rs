//! The spawn operation store (design section 4a; steward S2-D4, R-120, R-121): durable spawn operation records (HC A.7b
//! `spawn` kind, EN-7 durable operation) in RT-2 `store/ops/`.
//!
//! - The owner thread owns the index and every reservation. The `store` thread only writes and deletes files.
//! - A write is atomic and durable: `.tmp-<record_key>`, `fsync`, `rename`, then `fsync` of `store/ops/`.
//! - The owner is the only sender of jobs, and it sends a job only while fewer than `store_queue` are in flight, so a send
//!   never blocks. Completions return on a channel with capacity `store_queue + 1`, then wake the owner.
//! - A terminal state is never refused: it waits in its record's pending slot for a job slot.
//! - Caller-known operation ids (EN-7b) are deferred: every `operation_id` is Hub-minted.

use crate::failure::{FailCode, StartFailure};
use crate::fsutil::{self, ensure_dir, WriteError};
use crate::phase::{Ended, Wake};
use crate::start::{ensure_durable_dir, random_id};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError};
use std::sync::Arc;
use std::thread::JoinHandle;

/// The record format version.
pub const RECORD_VERSION: u32 = 1;

/// The A.7b `spawn` states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpawnState {
    Starting,
    Installed,
    Failed,
    Cancelled,
}

/// The `failed` reasons that the Hub writes (A.7b; R-120 adds `interrupted` and `internal`; R-121 adds `start_failed`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailReason {
    ExecFailed,
    CwdMissing,
    WorkerFailed,
    /// A start failure whose Core reason this Hub does not know (R-121, S2-HD-12). Not a Hub fault.
    StartFailed,
    Interrupted,
    Internal,
}

impl FailReason {
    /// The word of the reason in the record and on the wire (A.7b).
    pub fn as_str(self) -> &'static str {
        match self {
            FailReason::ExecFailed => "exec_failed",
            FailReason::CwdMissing => "cwd_missing",
            FailReason::WorkerFailed => "worker_failed",
            FailReason::StartFailed => "start_failed",
            FailReason::Interrupted => "interrupted",
            FailReason::Internal => "internal",
        }
    }
}

/// One spawn operation record (design section 4a).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub version: u32,
    pub record_key: String,
    pub operation_id: String,
    pub session_id: String,
    pub state: SpawnState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<FailReason>,
    /// R-122 U3: the row is the Hub's to remove. It stays `true` until the row is proved gone (Sol S2-HD-11), and the
    /// record is not swept while it is `true`.
    #[serde(default)]
    pub remove_row: bool,
    pub admitted_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_at_ms: Option<u64>,
}

/// A terminal state for [`SpawnStore::finish_spawn`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Terminal {
    Installed,
    Failed(FailReason),
    Cancelled,
}

/// The store's parameters (small test values; HP LM-5 rule).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreConfig {
    pub max_records: usize,
    pub store_queue: usize,
    pub retention_ms: u64,
    pub max_record_bytes: u64,
    pub turn_sweeps: usize,
}

/// How a job ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobResult {
    Done,
    /// Proved: the record did not change (the error came before the `rename`).
    NotWritten,
    /// The record holds the old or the new state, or is present or absent for a delete; nobody can tell which.
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobKind {
    Create,
    Finish,
    Delete,
    /// R-122 U1, U2: the delete of a `starting` record before the answer.
    Forget,
}

#[derive(Debug)]
enum Job {
    /// A `Create` write first makes the session directory (design section 4a, "The session directory").
    Write { kind: JobKind, record: Record },
    /// A `Forget` delete names the session: its directory goes after the record.
    Delete {
        kind: JobKind,
        key: String,
        session: Option<String>,
    },
    /// The session directory of a `Released` row.
    DeleteDir { session: String },
}

#[derive(Debug)]
enum StoreMsg {
    Done {
        kind: JobKind,
        key: String,
        result: JobResult,
    },
    DirDone,
    Gone(Ended),
}

/// The directories of the store thread.
struct Dirs {
    /// `<data root>/store/ops`.
    ops: PathBuf,
    /// `<data root>/sessions` (HC RT-2).
    sessions: PathBuf,
}

/// What a turn's completions mean for the owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreEvent {
    /// The `starting` record is durable: the owner may begin `Create` (the order rule).
    CreateDurable { operation_id: String },
    /// The `starting` record was proved not written: the request gets `internal`, `effect: none`.
    CreateNotWritten { operation_id: String },
    /// The `starting` record is durably deleted (R-122 U1, U2): the owner answers now.
    Forgotten { operation_id: String },
    /// A fault (design section 5, "Store fault"): the Hub stops with cause `store_failed`.
    Fault(String),
}

/// Why `create_spawn` refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateRefused {
    /// No free record or no free job slot. Retryable, `effect: none`.
    Busy,
    /// The store no longer admits (the stop, or a fault).
    Closed,
    /// The session id is not a safe path component (an owner bug: session ids are Hub-minted).
    BadSession,
}

/// The answer of `get_spawn`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lookup {
    /// Recovery runs (EN-7b): never `not_found`.
    Busy,
    NotFound,
    Found(Record),
}

#[derive(Debug)]
struct Entry {
    /// The durable state (for a reservation: the state being written).
    record: Record,
    /// The `starting` write is durable.
    written: bool,
    /// The terminal record that waits for its job. A later terminal state replaces it until the job is sent.
    pending: Option<Record>,
    /// The terminal record whose job is in flight: exactly the payload that the job writes. A terminal state that comes
    /// meanwhile goes to `pending`, and is written by a job of its own after this one completes.
    sent: Option<Record>,
    /// A job for this record is in flight.
    busy: bool,
    /// A forget (R-122 U1, U2) waits for a job slot, or is in flight.
    forget: bool,
    /// The marker clear came while a write of the marker was in flight: write the record again after it.
    clear_after: bool,
}

impl Entry {
    /// A terminal write or a forget waits for a job slot.
    fn waits(&self) -> bool {
        (self.pending.is_some() || self.forget) && !self.busy
    }
}

/// The owner side of the store.
pub struct SpawnStore {
    dir: PathBuf,
    config: StoreConfig,
    index: HashMap<String, Entry>,
    keys: HashMap<String, String>,
    /// Terminal records by `(terminal_at_ms, operation_id)`, for the sweep.
    expiry: BTreeSet<(u64, String)>,
    jobs: Option<SyncSender<Job>>,
    done: Receiver<StoreMsg>,
    in_flight: usize,
    thread: Option<JoinHandle<()>>,
    recovering: bool,
    admitting: bool,
    /// The stop's quiesce began: only pending terminal states are still written (no directory deletion, no sweep).
    stopping: bool,
    /// A fault was seen: no job is sent again (the Hub stops with cause `store_failed`).
    faulted: bool,
    /// The session directories of `Released` rows that wait for a job slot.
    dir_deletes: VecDeque<String>,
    gone: Option<Ended>,
    /// Files that are not the Hub's, counted for `diagnostics()`.
    pub foreign_files: usize,
}

fn unusable(path: &Path, cause: impl std::fmt::Display) -> StartFailure {
    StartFailure::new(
        FailCode::DataRootUnusable,
        format!("{}: {cause}", path.display()),
    )
}

fn record_path(dir: &Path, key: &str) -> PathBuf {
    dir.join(format!("{key}.json"))
}

/// A session id that can name `sessions/<id>/`: not empty, not `.` or `..`, no `/` or NUL, at most 128 bytes (Core's
/// default `max_session_id_bytes`). Session ids are Hub-minted, so a refusal is an owner bug.
pub fn is_session_dir_name(session_id: &str) -> bool {
    !session_id.is_empty()
        && session_id.len() <= 128
        && session_id != "."
        && session_id != ".."
        && !session_id.contains(['/', '\0'])
}

/// Makes `sessions/`, `sessions/<id>/` and `sessions/<id>/uploads/` (0700) and syncs them. A path step that exists and is
/// not a directory (a symlink included) is an error, so nothing is made through a symlink.
fn make_session_dir(sessions: &Path, session_id: &str) -> io::Result<()> {
    let session = sessions.join(session_id);
    let uploads = session.join("uploads");
    for dir in [sessions, session.as_path(), uploads.as_path()] {
        ensure_dir(dir)?;
        if !fs::symlink_metadata(dir)?.is_dir() {
            return Err(io::Error::other(format!(
                "{} is not a directory",
                dir.display()
            )));
        }
    }
    fsutil::sync_dir(&session)?;
    fsutil::sync_dir(sessions)?;
    match sessions.parent() {
        Some(root) => fsutil::sync_dir(root),
        None => Ok(()),
    }
}

/// Deletes `sessions/<id>/` with no-follow: a missing entry is done, a symlink or a file is an error and is kept.
pub fn remove_session_dir(sessions: &Path, session_id: &str) -> io::Result<()> {
    // The sessions root is not followed either: a symlinked root would lead the deletion out of the data root.
    match fs::symlink_metadata(sessions) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
        Ok(meta) if !meta.is_dir() => {
            return Err(io::Error::other(format!(
                "{} is not a directory",
                sessions.display()
            )))
        }
        Ok(_) => {}
    }
    let session = sessions.join(session_id);
    match fs::symlink_metadata(&session) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
        Ok(meta) if !meta.is_dir() => {
            return Err(io::Error::other(format!(
                "{} is not a directory",
                session.display()
            )))
        }
        Ok(_) => fs::remove_dir_all(&session)?,
    }
    fsutil::sync_dir(sessions)
}

fn tmp_path(dir: &Path, key: &str) -> PathBuf {
    dir.join(format!(".tmp-{key}"))
}

/// A record key or a Hub-minted id: 26 characters of lowercase base32.
fn is_id(text: &str) -> bool {
    text.len() == 26
        && text
            .bytes()
            .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
}

impl SpawnStore {
    /// Loads `<data root>/store/ops/` strictly (design section 4a, "Load at start") and starts the store thread. Only the
    /// lock holder calls it.
    ///
    /// # Errors
    /// `data_root_unusable store/ops/<file>: <cause>` for a record-named file that does not decode, has an unknown version,
    /// is too large or names another key; for two records with one `operation_id`; for more than `max_records` records.
    pub fn open(
        data_root: &Path,
        config: StoreConfig,
        owner: Arc<dyn Wake>,
    ) -> Result<SpawnStore, StartFailure> {
        Self::open_with(data_root, config, owner, fsutil::sync_dir)
    }

    /// [`SpawnStore::open`] with the directory sync as a parameter, so that a sync failure can be tested.
    fn open_with(
        data_root: &Path,
        config: StoreConfig,
        owner: Arc<dyn Wake>,
        sync: impl Fn(&Path) -> io::Result<()>,
    ) -> Result<SpawnStore, StartFailure> {
        let dir = data_root.join("store").join("ops");
        // Each directory entry is made durable through the sync of its parent, at every open (a retry included): the data root
        // holds `store/`, `store/` holds `ops/`, and a record write syncs `ops/` itself.
        ensure_durable_dir(&data_root.join("store"), &sync).map_err(|e| unusable(&dir, e))?;
        ensure_durable_dir(&dir, &sync).map_err(|e| unusable(&dir, e))?;
        let mut index = HashMap::new();
        let mut keys = HashMap::new();
        let mut expiry = BTreeSet::new();
        let mut foreign_files = 0;
        for entry in fs::read_dir(&dir).map_err(|e| unusable(&dir, e))? {
            let entry = entry.map_err(|e| unusable(&dir, e))?;
            let path = entry.path();
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                foreign_files += 1;
                continue;
            };
            if let Some(key) = name.strip_prefix(".tmp-").filter(|k| is_id(k)) {
                // A write that never reached its `rename`: no record changed.
                let _ = key;
                fs::remove_file(&path).map_err(|e| unusable(&path, e))?;
                continue;
            }
            let Some(key) = name.strip_suffix(".json").filter(|k| is_id(k)) else {
                foreign_files += 1;
                continue;
            };
            let meta = fs::symlink_metadata(&path).map_err(|e| unusable(&path, e))?;
            if !meta.is_file() {
                return Err(unusable(&path, "not a regular file"));
            }
            if meta.len() > config.max_record_bytes {
                return Err(unusable(
                    &path,
                    format!("{} bytes is over the record limit", meta.len()),
                ));
            }
            let bytes = fs::read(&path).map_err(|e| unusable(&path, e))?;
            let record: Record = serde_json::from_slice(&bytes).map_err(|e| unusable(&path, e))?;
            if record.version != RECORD_VERSION {
                return Err(unusable(
                    &path,
                    format!("unknown version {}", record.version),
                ));
            }
            if record.record_key != key {
                return Err(unusable(&path, "the record names another key"));
            }
            if index.contains_key(&record.operation_id) {
                return Err(unusable(&path, "a second record of one operation_id"));
            }
            if index.len() >= config.max_records {
                return Err(unusable(
                    &dir,
                    format!("more than {} records", config.max_records),
                ));
            }
            if let (Some(at), false) = (record.terminal_at_ms, record.remove_row) {
                expiry.insert((at, record.operation_id.clone()));
            }
            keys.insert(record.record_key.clone(), record.operation_id.clone());
            index.insert(
                record.operation_id.clone(),
                Entry {
                    record,
                    written: true,
                    pending: None,
                    sent: None,
                    busy: false,
                    forget: false,
                    clear_after: false,
                },
            );
        }
        let (jobs, job_rx) = sync_channel(config.store_queue);
        let (done_tx, done) = sync_channel(config.store_queue + 1);
        let thread_dirs = Dirs {
            ops: dir.clone(),
            sessions: data_root.join("sessions"),
        };
        let thread = std::thread::Builder::new()
            .name("store".into())
            .spawn(move || store_thread(&thread_dirs, &job_rx, &done_tx, owner.as_ref()))
            .map_err(|e| StartFailure::new(FailCode::Internal, e.to_string()))?;
        Ok(SpawnStore {
            dir,
            config,
            index,
            keys,
            expiry,
            jobs: Some(jobs),
            done,
            in_flight: 0,
            thread: Some(thread),
            recovering: true,
            admitting: true,
            stopping: false,
            faulted: false,
            dir_deletes: VecDeque::new(),
            gone: None,
            foreign_files,
        })
    }

    /// The records loaded at start that are still `starting`, for step 6b.
    pub fn starting(&self) -> Vec<Record> {
        self.index
            .values()
            .filter(|e| e.written && e.record.state == SpawnState::Starting)
            .map(|e| e.record.clone())
            .collect()
    }

    /// Step 6b ended: `get_spawn` answers from the index from now on.
    pub fn end_recovery(&mut self) {
        self.recovering = false;
    }

    /// Index entries, reservations included.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight
    }

    fn pending_terminals(&self) -> bool {
        self.index.values().any(Entry::waits)
    }

    fn send(&mut self, job: Job) -> bool {
        if self.faulted {
            return false;
        }
        match &self.jobs {
            Some(jobs) if self.in_flight < self.config.store_queue => match jobs.try_send(job) {
                Ok(()) => {
                    self.in_flight += 1;
                    true
                }
                Err(_) => false,
            },
            _ => false,
        }
    }

    /// Reserves an index entry, then queues the `starting` write (design section 4a, API 1). The owner begins `Create` only
    /// at [`StoreEvent::CreateDurable`].
    ///
    /// # Errors
    /// [`CreateRefused::Busy`] with no free record, no free job slot, or a pending terminal state (which goes first);
    /// [`CreateRefused::Closed`] after the stop or a fault. Nothing is reserved then.
    pub fn create_spawn(&mut self, session_id: &str, now_ms: u64) -> Result<String, CreateRefused> {
        if !self.admitting || self.jobs.is_none() {
            return Err(CreateRefused::Closed);
        }
        if !is_session_dir_name(session_id) {
            return Err(CreateRefused::BadSession);
        }
        if self.index.len() >= self.config.max_records
            || self.in_flight >= self.config.store_queue
            || self.pending_terminals()
        {
            return Err(CreateRefused::Busy);
        }
        let mint = || random_id().map_err(|_| CreateRefused::Busy);
        let operation_id = mint()?;
        let record_key = mint()?;
        let record = Record {
            version: RECORD_VERSION,
            record_key: record_key.clone(),
            operation_id: operation_id.clone(),
            session_id: session_id.to_owned(),
            state: SpawnState::Starting,
            reason: None,
            remove_row: false,
            admitted_at_ms: now_ms,
            terminal_at_ms: None,
        };
        if !self.send(Job::Write {
            kind: JobKind::Create,
            record: record.clone(),
        }) {
            return Err(CreateRefused::Busy);
        }
        self.keys.insert(record_key, operation_id.clone());
        self.index.insert(
            operation_id.clone(),
            Entry {
                record,
                written: false,
                pending: None,
                sent: None,
                busy: true,
                forget: false,
                clear_after: false,
            },
        );
        Ok(operation_id)
    }

    /// Puts the terminal state in the record's pending slot (design section 4a, API 2). It is never refused; the job goes
    /// when a slot is free. A later terminal state for the same record replaces a pending one that has no job in flight.
    ///
    /// # Errors
    /// The operation is unknown, or its `starting` write is not durable (an owner bug: `Create` is begun only after it).
    pub fn finish_spawn(
        &mut self,
        operation_id: &str,
        terminal: Terminal,
        now_ms: u64,
    ) -> Result<(), String> {
        self.finish_with(operation_id, terminal, now_ms, false)
    }

    /// R-122 U3: `failed { internal }` with the cleanup obligation (`remove_row: true`).
    ///
    /// # Errors
    /// As [`SpawnStore::finish_spawn`].
    pub fn finish_with_cleanup(&mut self, operation_id: &str, now_ms: u64) -> Result<(), String> {
        self.finish_with(
            operation_id,
            Terminal::Failed(FailReason::Internal),
            now_ms,
            true,
        )
    }

    fn finish_with(
        &mut self,
        operation_id: &str,
        terminal: Terminal,
        now_ms: u64,
        remove_row: bool,
    ) -> Result<(), String> {
        let Some(entry) = self.index.get_mut(operation_id) else {
            return Err(format!("no spawn operation {operation_id}"));
        };
        if !entry.written || entry.forget {
            return Err(format!(
                "the spawn operation {operation_id} is not durable yet, or is forgotten"
            ));
        }
        let (state, reason) = match terminal {
            Terminal::Installed => (SpawnState::Installed, None),
            Terminal::Failed(reason) => (SpawnState::Failed, Some(reason)),
            Terminal::Cancelled => (SpawnState::Cancelled, None),
        };
        entry.pending = Some(Record {
            state,
            reason,
            remove_row,
            terminal_at_ms: Some(now_ms),
            ..entry.record.clone()
        });
        Ok(())
    }

    /// The row of a U3 record is proved gone (Sol S2-HD-11): the record is written again with `remove_row: false`, by the
    /// rules of `finish_spawn` (never refused). After that write, the record is swept after its retention.
    ///
    /// # Errors
    /// The operation is unknown.
    pub fn clear_remove_row(&mut self, operation_id: &str) -> Result<(), String> {
        let Some(entry) = self.index.get_mut(operation_id) else {
            return Err(format!("no spawn operation {operation_id}"));
        };
        let latest = entry
            .pending
            .as_ref()
            .or(entry.sent.as_ref())
            .unwrap_or(&entry.record);
        if !latest.remove_row {
            return Ok(());
        }
        if entry.pending.is_none() && entry.sent.is_some() {
            // The write of the marker is in flight: its completion queues the clear.
            entry.clear_after = true;
        } else {
            entry.pending = Some(Record {
                remove_row: false,
                ..latest.clone()
            });
        }
        Ok(())
    }

    /// The U3 records whose rows the Hub must still remove: `(operation_id, session_id)`.
    pub fn remove_rows(&self) -> Vec<(String, String)> {
        self.index
            .iter()
            .filter(|(_, e)| {
                !e.clear_after
                    && e.pending
                        .as_ref()
                        .or(e.sent.as_ref())
                        .unwrap_or(&e.record)
                        .remove_row
            })
            .map(|(id, e)| (id.clone(), e.record.session_id.clone()))
            .collect()
    }

    /// R-122 U1, U2: deletes a durable `starting` record. [`StoreEvent::Forgotten`] follows; a delete error is a fault.
    /// It is queued like a terminal state: never refused, and it goes before new `create_spawn` jobs.
    ///
    /// # Errors
    /// The operation is unknown, not durable, or not a plain `starting` record.
    pub fn forget_spawn(&mut self, operation_id: &str) -> Result<(), String> {
        let Some(entry) = self.index.get_mut(operation_id) else {
            return Err(format!("no spawn operation {operation_id}"));
        };
        if !entry.written
            || entry.busy
            || entry.forget
            || entry.pending.is_some()
            || entry.record.state != SpawnState::Starting
        {
            return Err(format!(
                "the spawn operation {operation_id} cannot be forgotten"
            ));
        }
        entry.forget = true;
        Ok(())
    }

    /// Answers from the index: the durable state only (design section 4a, API 3).
    pub fn get_spawn(&self, operation_id: &str) -> Lookup {
        if self.recovering {
            return Lookup::Busy;
        }
        match self.index.get(operation_id) {
            Some(entry) if entry.written && !entry.forget => Lookup::Found(entry.record.clone()),
            _ => Lookup::NotFound,
        }
    }

    /// One turn of store work: queue pending terminal states first, then at most `turn_sweeps` deletes of records whose
    /// retention has passed (EN-7b). A pending record is never deleted.
    pub fn pump(&mut self, now_ms: u64) {
        let ready: Vec<String> = self
            .index
            .iter()
            .filter(|(_, e)| e.waits())
            .map(|(id, _)| id.clone())
            .collect();
        for id in ready {
            let Some(entry) = self.index.get(&id) else {
                continue;
            };
            let job = if entry.forget {
                Job::Delete {
                    kind: JobKind::Forget,
                    key: entry.record.record_key.clone(),
                    session: Some(entry.record.session_id.clone()),
                }
            } else if let Some(record) = entry.pending.clone() {
                Job::Write {
                    kind: JobKind::Finish,
                    record,
                }
            } else {
                continue;
            };
            let sent = match &job {
                Job::Write { record, .. } => Some(record.clone()),
                _ => None,
            };
            if !self.send(job) {
                return;
            }
            if let Some(entry) = self.index.get_mut(&id) {
                entry.busy = true;
                if sent.is_some() {
                    // The payload now belongs to the job; a later terminal state goes to a new `pending`.
                    entry.pending = None;
                    entry.sent = sent;
                }
            }
        }
        if self.stopping {
            return;
        }
        while let Some(session) = self.dir_deletes.front().cloned() {
            if !self.send(Job::DeleteDir { session }) {
                break;
            }
            self.dir_deletes.pop_front();
        }
        let mut swept = 0;
        while swept < self.config.turn_sweeps {
            let Some((at, id)) = self.expiry.first().cloned() else {
                break;
            };
            if at.saturating_add(self.config.retention_ms) > now_ms {
                break;
            }
            let Some(entry) = self.index.get(&id) else {
                self.expiry.remove(&(at, id));
                continue;
            };
            // A key that is not the current durable record's expiry is stale: it never deletes the record.
            if entry.record.terminal_at_ms != Some(at) {
                self.expiry.remove(&(at, id));
                continue;
            }
            if entry.busy || entry.pending.is_some() || entry.forget || entry.record.remove_row {
                break;
            }
            let key = entry.record.record_key.clone();
            if !self.send(Job::Delete {
                kind: JobKind::Delete,
                key,
                session: None,
            }) {
                break;
            }
            self.expiry.remove(&(at, id.clone()));
            if let Some(entry) = self.index.get_mut(&id) {
                entry.busy = true;
            }
            swept += 1;
        }
    }

    /// Takes every completion that waits, applies it to the index, and returns what it means for the owner.
    pub fn poll(&mut self) -> Vec<StoreEvent> {
        let mut events = Vec::new();
        loop {
            match self.done.try_recv() {
                Ok(message) => self.apply(message, &mut events),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    if self.gone.is_none() {
                        self.apply(StoreMsg::Gone(Ended::Panicked), &mut events);
                    }
                    break;
                }
            }
        }
        events
    }

    fn apply(&mut self, message: StoreMsg, events: &mut Vec<StoreEvent>) {
        match message {
            StoreMsg::Gone(ended) => {
                self.gone = Some(ended);
                self.in_flight = 0;
                if ended == Ended::Panicked {
                    self.fault();
                    events.push(StoreEvent::Fault("the store thread panicked".into()));
                }
            }
            StoreMsg::DirDone => self.in_flight -= 1,
            StoreMsg::Done { kind, key, result } => {
                self.in_flight -= 1;
                let Some(id) = self.keys.get(&key).cloned() else {
                    return;
                };
                match (kind, result) {
                    (JobKind::Create, JobResult::Done) => {
                        if let Some(entry) = self.index.get_mut(&id) {
                            entry.written = true;
                            entry.busy = false;
                        }
                        events.push(StoreEvent::CreateDurable { operation_id: id });
                    }
                    (JobKind::Create, JobResult::NotWritten) => {
                        self.index.remove(&id);
                        self.keys.remove(&key);
                        events.push(StoreEvent::CreateNotWritten { operation_id: id });
                    }
                    (JobKind::Finish, JobResult::Done) => {
                        if let Some(entry) = self.index.get_mut(&id) {
                            entry.busy = false;
                            // The record is the payload that the job wrote, not a terminal state queued meanwhile.
                            if let Some(record) = entry.sent.take() {
                                // The record's earlier expiry key is superseded by the one of the record just written.
                                if let Some(old) = entry.record.terminal_at_ms {
                                    self.expiry.remove(&(old, id.clone()));
                                }
                                if let (Some(at), false) =
                                    (record.terminal_at_ms, record.remove_row)
                                {
                                    self.expiry.insert((at, id.clone()));
                                }
                                entry.record = record;
                            }
                            if std::mem::take(&mut entry.clear_after) {
                                let base =
                                    entry.pending.take().unwrap_or_else(|| entry.record.clone());
                                entry.pending = Some(Record {
                                    remove_row: false,
                                    ..base
                                });
                            }
                        }
                    }
                    (JobKind::Delete, JobResult::Done) => {
                        self.index.remove(&id);
                        self.keys.remove(&key);
                    }
                    (JobKind::Forget, JobResult::Done) => {
                        self.index.remove(&id);
                        self.keys.remove(&key);
                        events.push(StoreEvent::Forgotten { operation_id: id });
                    }
                    (kind, _) => {
                        if let Some(entry) = self.index.get_mut(&id) {
                            entry.busy = false;
                            entry.sent = None;
                        }
                        self.fault();
                        events.push(StoreEvent::Fault(format!(
                            "a {kind:?} write of spawn operation {id} failed"
                        )));
                    }
                }
            }
        }
    }

    /// A `Released` row: its session directory is deleted by a store job, after the terminal states (design section 4a,
    /// "The session directory"; Core A6-3; Sol S2-HD-14).
    /// - Until a job slot is free, the deletion waits in this handle, at most `max_records` entries. Over the bound, it
    ///   is dropped, and the orphan sweep at the next start removes the directory.
    /// - A deletion that was sent is a store job like any other: it finishes before the store thread is joined. One that
    ///   was not sent when the stop starts is dropped for the orphan sweep.
    /// - An id that cannot name a directory has none.
    pub fn release_session(&mut self, session_id: &str) {
        if is_session_dir_name(session_id) && self.dir_deletes.len() < self.config.max_records {
            self.dir_deletes.push_back(session_id.to_owned());
        }
    }

    /// The deletions that wait for a job slot.
    pub fn dir_deletes(&self) -> usize {
        self.dir_deletes.len()
    }

    /// A fault: no new spawn is admitted and no job is sent again. The jobs in flight complete.
    fn fault(&mut self) {
        self.faulted = true;
        self.admitting = false;
    }

    /// Stop step 2 (design section 5): no new `create_spawn`; every pending terminal state is written as slots free; then
    /// the job sender is dropped (the close), the store finishes its queue and ends, and the owner joins it. No deadline: a
    /// write after the lock release could break RT-4. Returns the faults seen meanwhile.
    pub fn quiesce(&mut self) -> Vec<StoreEvent> {
        self.admitting = false;
        self.stopping = true;
        let mut events = Vec::new();
        // A job in flight can queue a later terminal state when it completes: wait for it too.
        while self.gone.is_none() && (self.pending_terminals() || self.in_flight > 0) {
            self.pump(0);
            // After a fault no job goes; with no job in flight, no completion can come.
            if self.in_flight == 0 {
                break;
            }
            match self.done.recv() {
                Ok(message) => self.apply(message, &mut events),
                Err(_) => self.apply(StoreMsg::Gone(Ended::Panicked), &mut events),
            }
            if events.iter().any(|e| matches!(e, StoreEvent::Fault(_))) {
                break;
            }
        }
        events.extend(self.close());
        events
    }

    /// The fault path and the end of the quiesce: drop the job sender at once, take the completions of the queued jobs
    /// until the store thread ends, and join it. Pending terminal states that are not written stay as they are on disk.
    pub fn close(&mut self) -> Vec<StoreEvent> {
        self.admitting = false;
        self.jobs = None;
        let mut events = Vec::new();
        while self.gone.is_none() {
            match self.done.recv() {
                Ok(message) => self.apply(message, &mut events),
                Err(_) => self.apply(StoreMsg::Gone(Ended::Panicked), &mut events),
            }
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        events
    }

    /// The directory of the records.
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// Why the live flow did not write `installed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallBlocked {
    /// `installed` needs the row's `ready_reached` (A26 candidate 5). There is no interim rule for Core `v1` (lead ruling
    /// 2026-10-12): the spawn stays `starting` until the A26 pin.
    NeedsA26,
}

/// Spawn flow step 4 (design section 4a; R-122): `installed` when the row's `ready_reached` becomes `true`. Marked A26: a
/// typed stub until the Core pin carries A26. It changes nothing.
///
/// # Errors
/// Always [`InstallBlocked::NeedsA26`].
pub fn install_on_ready(
    _store: &mut SpawnStore,
    _operation_id: &str,
) -> Result<(), InstallBlocked> {
    Err(InstallBlocked::NeedsA26)
}

/// Why step 6b did not run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreBlocked {
    /// Step 6b is built on the Core pin that carries A26 (candidate 5: `start_failure` and `ready_reached` in the row), and
    /// R-121 forbids an interim rule for Core `v1`. The records stay as they are on disk until that pin.
    NeedsA26 { starting: usize },
}

/// Step 6b, recover the spawn operations (design section 5; R-120, R-121 at contracts main a11b6178). Marked A26: a typed
/// stub until the Core pin carries A26. It changes nothing; recovery then ends, so `get_spawn` answers the records' durable
/// states.
///
/// # Errors
/// [`RestoreBlocked::NeedsA26`] when `starting` records wait for the restore.
pub fn restore_spawns(store: &mut SpawnStore) -> Result<(), RestoreBlocked> {
    let starting = store.starting().len();
    store.end_recovery();
    if starting == 0 {
        Ok(())
    } else {
        Err(RestoreBlocked::NeedsA26 { starting })
    }
}

fn store_thread(dirs: &Dirs, jobs: &Receiver<Job>, done: &SyncSender<StoreMsg>, owner: &dyn Wake) {
    let ended = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        while let Ok(job) = jobs.recv() {
            let message = run_job(dirs, job);
            // The channel holds one slot per job in flight plus `Gone`, so this send never blocks.
            let _ = done.send(message);
            owner.wake();
        }
    })) {
        Ok(()) => Ended::Clean,
        Err(_) => Ended::Panicked,
    };
    let _ = done.send(StoreMsg::Gone(ended));
    owner.wake();
}

fn run_job(dirs: &Dirs, job: Job) -> StoreMsg {
    let dir = dirs.ops.as_path();
    match job {
        Job::Write { kind, record } => {
            let key = record.record_key.clone();
            // The directory goes first, so a record never names a missing directory. An error here changed no record.
            if kind == JobKind::Create {
                if let Err(error) = make_session_dir(&dirs.sessions, &record.session_id) {
                    eprintln!(
                        "botster-hub: store: the session directory of {}: {error}",
                        record.session_id
                    );
                    return StoreMsg::Done {
                        kind,
                        key,
                        result: JobResult::NotWritten,
                    };
                }
            }
            let result = match serde_json::to_vec(&record) {
                Err(_) => JobResult::NotWritten,
                Ok(bytes) => match fsutil::write_atomic(
                    &record_path(dir, &key),
                    &tmp_path(dir, &key),
                    &bytes,
                ) {
                    Ok(()) => JobResult::Done,
                    Err(WriteError::NotWritten(_)) => JobResult::NotWritten,
                    Err(WriteError::Unknown(_)) => JobResult::Unknown,
                },
            };
            StoreMsg::Done { kind, key, result }
        }
        Job::Delete { kind, key, session } => {
            let removed: io::Result<()> =
                fs::remove_file(record_path(dir, &key)).and_then(|()| fsutil::sync_dir(dir));
            // The directory goes after the record. Its error is logged only; the orphan sweep retries it.
            if let (Ok(()), Some(session)) = (&removed, session) {
                if let Err(error) = remove_session_dir(&dirs.sessions, &session) {
                    eprintln!("botster-hub: store: the session directory of {session}: {error}");
                }
            }
            StoreMsg::Done {
                kind,
                key,
                result: if removed.is_ok() {
                    JobResult::Done
                } else {
                    JobResult::Unknown
                },
            }
        }
        Job::DeleteDir { session } => {
            if let Err(error) = remove_session_dir(&dirs.sessions, &session) {
                eprintln!("botster-hub: store: the session directory of {session}: {error}");
            }
            StoreMsg::DirDone
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::phase::tests::Count;
    use std::os::unix::fs::PermissionsExt;

    const CONFIG: StoreConfig = StoreConfig {
        max_records: 3,
        store_queue: 2,
        retention_ms: 1000,
        max_record_bytes: 4096,
        turn_sweeps: 1,
    };

    fn root() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("bh-")
            .tempdir_in("/tmp")
            .unwrap()
    }

    fn open(dir: &Path) -> SpawnStore {
        SpawnStore::open(dir, CONFIG, Arc::new(Count::default())).unwrap()
    }

    /// Polls until `n` events arrived (the store thread is real; each completion arrives within a few writes).
    fn events(store: &mut SpawnStore, n: usize) -> Vec<StoreEvent> {
        let mut out = Vec::new();
        for _ in 0..5000 {
            out.extend(store.poll());
            if out.len() >= n && store.in_flight() == 0 {
                return out;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("events: {out:?}, in flight {}", store.in_flight());
    }

    fn settle(store: &mut SpawnStore, now_ms: u64) -> Vec<StoreEvent> {
        let mut out = Vec::new();
        for _ in 0..5000 {
            store.pump(now_ms);
            out.extend(store.poll());
            if store.in_flight() == 0 && !store.pending_terminals() {
                return out;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("not settled: {out:?}");
    }

    fn files(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir.join("store/ops"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    /// API 1 and 3: the reservation is written durably as `starting`, mode 0600; `get_spawn` is `busy` during recovery and
    /// answers the durable state after it; an unknown id is `not_found`.
    #[test]
    fn create_is_durable_then_found() {
        let tmp = root();
        let mut store = open(tmp.path());
        let id = store.create_spawn("s1", 5).unwrap();
        assert_eq!(store.get_spawn(&id), Lookup::Busy);
        assert_eq!(
            events(&mut store, 1),
            vec![StoreEvent::CreateDurable {
                operation_id: id.clone()
            }]
        );
        store.end_recovery();
        let Lookup::Found(record) = store.get_spawn(&id) else {
            panic!("not found");
        };
        assert_eq!(record.state, SpawnState::Starting);
        assert_eq!(record.session_id, "s1");
        assert_eq!(record.admitted_at_ms, 5);
        let path = record_path(&tmp.path().join("store/ops"), &record.record_key);
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            store.get_spawn("aaaaaaaaaaaaaaaaaaaaaaaaaa"),
            Lookup::NotFound
        );
        assert!(store.close().is_empty());
    }

    /// Bounds: the reservation counts against `max_records`, a full job queue refuses `busy`, and nothing is reserved then.
    #[test]
    fn reservations_count_against_the_bounds() {
        let tmp = root();
        let mut store = open(tmp.path());
        store.create_spawn("a", 0).unwrap();
        store.create_spawn("b", 0).unwrap();
        // Two jobs in flight fill the queue (store_queue 2).
        assert_eq!(store.create_spawn("c", 0), Err(CreateRefused::Busy));
        assert_eq!(store.len(), 2);
        events(&mut store, 2);
        store.create_spawn("c", 0).unwrap();
        events(&mut store, 1);
        // Three records fill the index (max_records 3).
        assert_eq!(store.create_spawn("d", 0), Err(CreateRefused::Busy));
        assert_eq!(store.len(), 3);
        store.close();
    }

    /// API 2: a terminal state is never refused; it waits for a slot, goes before a new create, and shows only after its
    /// completion (the durable state).
    #[test]
    fn finish_waits_for_a_slot_and_shows_when_durable() {
        let tmp = root();
        let mut store = open(tmp.path());
        store.end_recovery();
        let a = store.create_spawn("a", 0).unwrap();
        let b = store.create_spawn("b", 0).unwrap();
        events(&mut store, 2);
        store.finish_spawn(&a, Terminal::Installed, 10).unwrap();
        store
            .finish_spawn(&b, Terminal::Failed(FailReason::ExecFailed), 11)
            .unwrap();
        // A pending terminal state goes first: a new create is refused until it is queued.
        assert_eq!(store.create_spawn("c", 0), Err(CreateRefused::Busy));
        let Lookup::Found(before) = store.get_spawn(&a) else {
            panic!()
        };
        assert_eq!(before.state, SpawnState::Starting);
        assert!(settle(&mut store, 0).is_empty());
        let Lookup::Found(after) = store.get_spawn(&a) else {
            panic!()
        };
        assert_eq!(after.state, SpawnState::Installed);
        assert_eq!(after.terminal_at_ms, Some(10));
        let Lookup::Found(failed) = store.get_spawn(&b) else {
            panic!()
        };
        assert_eq!(
            (failed.state, failed.reason),
            (SpawnState::Failed, Some(FailReason::ExecFailed))
        );
        assert!(store.finish_spawn("nope", Terminal::Cancelled, 0).is_err());
        store.close();
    }

    /// A terminal state of a reservation that is not durable is an owner bug, refused.
    #[test]
    fn finish_before_durable_is_refused() {
        let tmp = root();
        let mut store = open(tmp.path());
        let a = store.create_spawn("a", 0).unwrap();
        assert!(store.finish_spawn(&a, Terminal::Installed, 1).is_err());
        events(&mut store, 1);
        store.close();
    }

    /// EN-7b: a terminal record is deleted only after the retention; a `starting` record never.
    #[test]
    fn sweep_deletes_after_retention_only() {
        let tmp = root();
        let mut store = open(tmp.path());
        store.end_recovery();
        let a = store.create_spawn("a", 0).unwrap();
        let b = store.create_spawn("b", 0).unwrap();
        events(&mut store, 2);
        store.finish_spawn(&a, Terminal::Cancelled, 100).unwrap();
        settle(&mut store, 0);
        settle(&mut store, 1099);
        assert!(matches!(store.get_spawn(&a), Lookup::Found(_)));
        settle(&mut store, 1100);
        assert_eq!(store.get_spawn(&a), Lookup::NotFound);
        settle(&mut store, u64::MAX);
        assert!(
            matches!(store.get_spawn(&b), Lookup::Found(_)),
            "a starting record is never swept"
        );
        assert_eq!(files(tmp.path()).len(), 1);
        store.close();
    }

    /// Load at start: the records come back; a temporary file is deleted; a foreign name is counted and kept.
    #[test]
    fn load_restores_records_and_cleans_temporaries() {
        let tmp = root();
        let mut store = open(tmp.path());
        let a = store.create_spawn("a", 7).unwrap();
        events(&mut store, 1);
        store.close();
        let ops = tmp.path().join("store/ops");
        fs::write(ops.join(".tmp-aaaaaaaaaaaaaaaaaaaaaaaaaa"), b"partial").unwrap();
        fs::write(ops.join("notes.txt"), b"").unwrap();
        let mut store = open(tmp.path());
        assert_eq!(store.foreign_files, 1);
        assert_eq!(store.starting().len(), 1);
        store.end_recovery();
        assert!(matches!(store.get_spawn(&a), Lookup::Found(r) if r.admitted_at_ms == 7));
        assert!(!ops.join(".tmp-aaaaaaaaaaaaaaaaaaaaaaaaaa").exists());
        assert!(ops.join("notes.txt").exists());
        store.close();
    }

    /// Files of a load case: `(record key, bytes)`.
    type Files = Vec<(String, Vec<u8>)>;

    /// Load at start is strict: an undecodable record, an unknown version, a key that differs from the name, an oversized
    /// file, a duplicate `operation_id` and too many records each fail the start `data_root_unusable`.
    #[test]
    fn load_is_strict() {
        let record = |key: &str, op: &str, version: u32| {
            serde_json::to_vec(&Record {
                version,
                record_key: key.into(),
                operation_id: op.into(),
                session_id: "s".into(),
                state: SpawnState::Starting,
                reason: None,
                remove_row: false,
                admitted_at_ms: 0,
                terminal_at_ms: None,
            })
            .unwrap()
        };
        let k = |c: char| c.to_string().repeat(26);
        let cases: Vec<(&str, Files)> = vec![
            ("undecodable", vec![(k('a'), b"{".to_vec())]),
            ("version", vec![(k('a'), record(&k('a'), "op", 2))]),
            ("key", vec![(k('a'), record(&k('b'), "op", 1))]),
            ("size", vec![(k('a'), vec![b' '; 4097])]),
            (
                "duplicate",
                vec![
                    (k('a'), record(&k('a'), "op", 1)),
                    (k('b'), record(&k('b'), "op", 1)),
                ],
            ),
            (
                "count",
                (['a', 'b', 'c', 'd'])
                    .iter()
                    .map(|&c| (k(c), record(&k(c), &format!("op{c}"), 1)))
                    .collect(),
            ),
        ];
        for (name, files) in cases {
            let tmp = root();
            let ops = tmp.path().join("store/ops");
            fs::create_dir_all(&ops).unwrap();
            for (key, bytes) in files {
                fs::write(ops.join(format!("{key}.json")), bytes).unwrap();
            }
            let failure = SpawnStore::open(tmp.path(), CONFIG, Arc::new(Count::default())).err();
            assert_eq!(
                failure.map(|f| f.code),
                Some(FailCode::DataRootUnusable),
                "{name}"
            );
        }
    }

    /// Stop step 2: no new create; the pending terminal states are written before the close; then the store thread ends.
    #[test]
    fn quiesce_writes_pending_terminals_then_closes() {
        let tmp = root();
        let mut store = open(tmp.path());
        let a = store.create_spawn("a", 0).unwrap();
        let b = store.create_spawn("b", 0).unwrap();
        events(&mut store, 2);
        store.finish_spawn(&a, Terminal::Installed, 1).unwrap();
        store.finish_spawn(&b, Terminal::Cancelled, 1).unwrap();
        assert!(store.quiesce().is_empty());
        assert_eq!(store.create_spawn("c", 0), Err(CreateRefused::Closed));
        let mut reopened = open(tmp.path());
        reopened.end_recovery();
        assert!(
            matches!(reopened.get_spawn(&a), Lookup::Found(r) if r.state == SpawnState::Installed)
        );
        assert!(
            matches!(reopened.get_spawn(&b), Lookup::Found(r) if r.state == SpawnState::Cancelled)
        );
        reopened.close();
    }

    /// A terminal state that comes while an older one is in flight is written by a job of its own: memory, the file and a
    /// reopen all agree with what was written (SPAWN-3).
    #[test]
    fn a_terminal_state_queued_during_a_write_does_not_pass_for_the_one_written() {
        let tmp = root();
        let mut store = open(tmp.path());
        let a = store.create_spawn("a", 0).unwrap();
        events(&mut store, 1);
        store.end_recovery();
        store.finish_spawn(&a, Terminal::Cancelled, 1).unwrap();
        store.pump(1);
        assert_eq!(store.in_flight(), 1);
        // Before the completion is taken, a later terminal state arrives.
        store.finish_spawn(&a, Terminal::Installed, 2).unwrap();
        let mut waited = 0;
        while store.in_flight() > 0 && waited < 5000 {
            store.poll();
            std::thread::sleep(std::time::Duration::from_millis(1));
            waited += 1;
        }
        let key = store.index[&a].record.record_key.clone();
        let on_disk: Record = serde_json::from_slice(
            &fs::read(record_path(&tmp.path().join("store/ops"), &key)).unwrap(),
        )
        .unwrap();
        assert_eq!(on_disk.state, SpawnState::Cancelled);
        assert!(
            matches!(store.get_spawn(&a), Lookup::Found(r) if r.state == SpawnState::Cancelled),
            "memory says what the file says"
        );
        // The later state is still pending, and the quiesce writes it.
        assert!(store.quiesce().is_empty());
        let mut reopened = open(tmp.path());
        reopened.end_recovery();
        assert!(
            matches!(reopened.get_spawn(&a), Lookup::Found(r) if r.state == SpawnState::Installed)
        );
        reopened.close();
    }

    /// The retention of a record runs from its current durable terminal state: an older write's expiry key is gone, so it
    /// cannot delete the record early (SPAWN-5).
    #[test]
    fn retention_follows_the_latest_durable_terminal_state() {
        let tmp = root();
        let mut store = open(tmp.path());
        let a = store.create_spawn("a", 0).unwrap();
        events(&mut store, 1);
        store.end_recovery();
        store.finish_spawn(&a, Terminal::Cancelled, 10).unwrap();
        store.pump(10);
        store.finish_spawn(&a, Terminal::Installed, 20).unwrap();
        settle(&mut store, 20);
        assert_eq!(
            store.expiry.len(),
            1,
            "one key for one record: {:?}",
            store.expiry
        );
        assert_eq!(
            store.expiry.first().map(|(at, _)| *at),
            Some(20),
            "the key is the latest write's"
        );
        // Past the first write's retention but not the second's: the record stays.
        let early = 10 + CONFIG.retention_ms;
        settle(&mut store, early);
        assert!(matches!(store.get_spawn(&a), Lookup::Found(_)));
        // Past the second's: it goes.
        settle(&mut store, 20 + CONFIG.retention_ms);
        assert!(matches!(store.get_spawn(&a), Lookup::NotFound));
    }

    /// The deletion of a session directory follows no symlink, the sessions root included (SPAWN-1).
    #[test]
    fn a_symlinked_sessions_root_is_not_followed() {
        let tmp = root();
        let outside = tmp.path().join("outside");
        fs::create_dir_all(outside.join("s1")).unwrap();
        fs::write(outside.join("s1/data"), "x").unwrap();
        let sessions = tmp.path().join("sessions");
        std::os::unix::fs::symlink(&outside, &sessions).unwrap();
        assert!(remove_session_dir(&sessions, "s1").is_err());
        assert!(outside.join("s1/data").exists());
        // A real root still works, and a missing one is done.
        let real = tmp.path().join("real");
        fs::create_dir_all(real.join("s1/uploads")).unwrap();
        remove_session_dir(&real, "s1").unwrap();
        assert!(!real.join("s1").exists());
        remove_session_dir(&tmp.path().join("none"), "s1").unwrap();
    }

    /// Opening the store syncs `store/`'s parent (the data root) and `ops/`'s parent (`store/`), at every open; a sync
    /// failure fails the open (SPAWN-2).
    #[test]
    fn open_syncs_the_parents_of_the_store_directories_and_a_sync_failure_fails() {
        let tmp = root();
        let synced = std::cell::RefCell::new(Vec::new());
        let record = |dir: &Path| {
            synced.borrow_mut().push(dir.to_path_buf());
            Ok(())
        };
        for _ in 0..2 {
            let mut store =
                SpawnStore::open_with(tmp.path(), CONFIG, Arc::new(Count::default()), record)
                    .unwrap();
            store.close();
        }
        let root = tmp.path().to_path_buf();
        let store_dir = root.join("store");
        assert_eq!(
            *synced.borrow(),
            vec![root.clone(), store_dir.clone(), root, store_dir]
        );
        let other = self::root();
        let failure =
            SpawnStore::open_with(other.path(), CONFIG, Arc::new(Count::default()), |_| {
                Err(io::Error::other("injected"))
            })
            .err()
            .unwrap();
        assert_eq!(failure.code, FailCode::DataRootUnusable);
    }

    /// A write that fails before the `rename` is `NotWritten`: the reservation is removed and nothing is on disk. A write
    /// with an unknown result is a fault.
    #[test]
    fn write_errors() {
        let tmp = root();
        let mut store = open(tmp.path());
        let ops = tmp.path().join("store/ops");
        // A read-only directory refuses the temporary file: not written.
        fs::set_permissions(&ops, fs::Permissions::from_mode(0o500)).unwrap();
        let a = store.create_spawn("a", 0).unwrap();
        assert_eq!(
            events(&mut store, 1),
            vec![StoreEvent::CreateNotWritten {
                operation_id: a.clone()
            }]
        );
        assert_eq!(store.len(), 0);
        fs::set_permissions(&ops, fs::Permissions::from_mode(0o700)).unwrap();
        // A directory in the record's place makes the `rename` fail: unknown, a fault.
        let b = store.create_spawn("b", 0).unwrap();
        let key = store.index[&b].record.record_key.clone();
        store.end_recovery();
        events(&mut store, 1);
        store.finish_spawn(&b, Terminal::Installed, 1).unwrap();
        fs::remove_file(record_path(&ops, &key)).unwrap();
        fs::create_dir(record_path(&ops, &key)).unwrap();
        fs::write(record_path(&ops, &key).join("x"), b"").unwrap();
        let faults = settle_until_fault(&mut store);
        assert!(matches!(&faults[..], [StoreEvent::Fault(_)]), "{faults:?}");
        // After a fault no job goes again: the failed write is not resent, and no spawn is admitted.
        store.pump(2);
        assert_eq!(store.in_flight(), 0);
        assert_eq!(store.create_spawn("c", 0), Err(CreateRefused::Closed));
        assert!(
            store.quiesce().is_empty(),
            "the quiesce does not wait for a job"
        );
    }

    /// Design section 4a, API 3 (R-122 U1, U2): `forget_spawn` deletes a durable `starting` record, then reports
    /// `Forgotten`. Only a plain durable `starting` record can be forgotten.
    #[test]
    fn forget_deletes_a_starting_record() {
        let tmp = root();
        let mut store = open(tmp.path());
        store.end_recovery();
        let a = store.create_spawn("a", 0).unwrap();
        assert!(store.forget_spawn(&a).is_err(), "not durable yet");
        events(&mut store, 1);
        store.forget_spawn(&a).unwrap();
        assert!(store.forget_spawn(&a).is_err(), "a forget waits already");
        assert_eq!(store.get_spawn(&a), Lookup::NotFound);
        assert!(store.finish_spawn(&a, Terminal::Installed, 1).is_err());
        assert_eq!(
            settle(&mut store, 0),
            vec![StoreEvent::Forgotten {
                operation_id: a.clone()
            }]
        );
        assert!(files(tmp.path()).is_empty());
        assert_eq!(store.len(), 0);
        let b = store.create_spawn("b", 0).unwrap();
        events(&mut store, 1);
        store.finish_spawn(&b, Terminal::Cancelled, 1).unwrap();
        assert!(store.forget_spawn(&b).is_err(), "a terminal state waits");
        settle(&mut store, 0);
        assert!(store.forget_spawn(&b).is_err(), "not starting");
        assert!(store.forget_spawn("nope").is_err());
        store.close();
    }

    /// Sol S2-HD-11: a record with `remove_row: true` is not swept and survives a restart; after `clear_remove_row` it is
    /// written again with `false`, and the sweep then deletes it after its retention.
    #[test]
    fn remove_row_blocks_the_sweep_until_cleared() {
        let tmp = root();
        let mut store = open(tmp.path());
        store.end_recovery();
        let a = store.create_spawn("a", 0).unwrap();
        events(&mut store, 1);
        store.finish_with_cleanup(&a, 100).unwrap();
        settle(&mut store, u64::MAX);
        settle(&mut store, u64::MAX);
        assert!(
            matches!(store.get_spawn(&a), Lookup::Found(r) if r.remove_row && r.reason == Some(FailReason::Internal))
        );
        assert_eq!(store.remove_rows(), vec![(a.clone(), "a".to_string())]);
        store.close();
        let mut store = open(tmp.path());
        store.end_recovery();
        assert_eq!(store.remove_rows(), vec![(a.clone(), "a".to_string())]);
        settle(&mut store, u64::MAX);
        assert!(matches!(store.get_spawn(&a), Lookup::Found(_)));
        store.clear_remove_row(&a).unwrap();
        assert!(store.remove_rows().is_empty());
        settle(&mut store, 0);
        assert!(matches!(store.get_spawn(&a), Lookup::Found(r) if !r.remove_row));
        settle(&mut store, 1099);
        assert!(matches!(store.get_spawn(&a), Lookup::Found(_)));
        settle(&mut store, 1100);
        assert_eq!(store.get_spawn(&a), Lookup::NotFound);
        store.close();
    }

    /// The session directory: the `starting` job makes it (0700) before the record; a symlink in its place fails the job
    /// as `NotWritten`, and nothing is made through the link.
    #[test]
    fn the_starting_job_makes_the_session_directory_first() {
        let tmp = root();
        let mut store = open(tmp.path());
        let a = store.create_spawn("a", 0).unwrap();
        assert_eq!(
            events(&mut store, 1),
            vec![StoreEvent::CreateDurable { operation_id: a }]
        );
        let uploads = tmp.path().join("sessions/a/uploads");
        assert_eq!(
            fs::symlink_metadata(&uploads).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let target = tmp.path().join("elsewhere");
        fs::create_dir(&target).unwrap();
        std::os::unix::fs::symlink(&target, tmp.path().join("sessions/b")).unwrap();
        let b = store.create_spawn("b", 0).unwrap();
        assert_eq!(
            events(&mut store, 1),
            vec![StoreEvent::CreateNotWritten { operation_id: b }]
        );
        assert!(
            !target.join("uploads").exists(),
            "nothing made through the link"
        );
        assert_eq!(files(tmp.path()).len(), 1);
        assert_eq!(
            store.create_spawn("../c", 0),
            Err(CreateRefused::BadSession)
        );
        store.close();
    }

    /// `remove_session_dir` deletes a directory, and keeps a symlink and its target.
    #[test]
    fn remove_session_dir_never_follows_a_symlink() {
        let tmp = root();
        let sessions = tmp.path().join("sessions");
        fs::create_dir_all(sessions.join("a/uploads")).unwrap();
        fs::write(sessions.join("a/uploads/f"), b"x").unwrap();
        remove_session_dir(&sessions, "a").unwrap();
        assert!(!sessions.join("a").exists());
        remove_session_dir(&sessions, "a").unwrap();
        let target = tmp.path().join("t");
        fs::create_dir(&target).unwrap();
        fs::write(target.join("keep"), b"x").unwrap();
        std::os::unix::fs::symlink(&target, sessions.join("l")).unwrap();
        assert!(remove_session_dir(&sessions, "l").is_err());
        assert!(target.join("keep").exists());
        assert!(fs::symlink_metadata(sessions.join("l")).is_ok());
    }

    /// Sol S2-HD-14: release deletions wait in the handle, at most `max_records`; a sent one finishes before the join.
    #[test]
    fn release_deletions_are_bounded_and_finish_before_the_join() {
        let tmp = root();
        let mut store = open(tmp.path());
        store.end_recovery();
        let sessions = tmp.path().join("sessions");
        for name in ["a", "b", "c", "d"] {
            fs::create_dir_all(sessions.join(name)).unwrap();
            store.release_session(name);
        }
        store.release_session("..");
        assert_eq!(store.dir_deletes(), CONFIG.max_records);
        // Two job slots: two deletions are sent; the stop joins them and drops the third.
        store.pump(0);
        assert_eq!(store.in_flight(), 2);
        assert!(store.quiesce().is_empty());
        assert!(!sessions.join("a").exists() && !sessions.join("b").exists());
        assert!(sessions.join("c").exists(), "left for the orphan sweep");
        assert!(sessions.join("d").exists(), "over the bound");
    }

    /// A marker clear while the marker's own write is in flight is written after it, so the last durable state is
    /// `remove_row: false`.
    #[test]
    fn clear_during_the_marker_write_is_kept() {
        let tmp = root();
        let mut store = open(tmp.path());
        store.end_recovery();
        let a = store.create_spawn("a", 0).unwrap();
        events(&mut store, 1);
        store.finish_with_cleanup(&a, 100).unwrap();
        store.pump(0);
        assert_eq!(store.in_flight(), 1);
        store.clear_remove_row(&a).unwrap();
        assert!(store.remove_rows().is_empty());
        settle(&mut store, 0);
        assert!(matches!(store.get_spawn(&a), Lookup::Found(r) if !r.remove_row));
        store.close();
        let mut store = open(tmp.path());
        assert!(store.remove_rows().is_empty());
        store.close();
    }

    /// Step 6b is a typed A26 stub: it changes no record, reports the `starting` records, and ends recovery.
    #[test]
    fn restore_is_a_typed_a26_stub() {
        let tmp = root();
        let mut store = open(tmp.path());
        assert_eq!(restore_spawns(&mut store), Ok(()));
        let a = store.create_spawn("a", 0).unwrap();
        events(&mut store, 1);
        store.close();
        let mut store = open(tmp.path());
        assert_eq!(store.get_spawn(&a), Lookup::Busy);
        assert_eq!(
            restore_spawns(&mut store),
            Err(RestoreBlocked::NeedsA26 { starting: 1 })
        );
        assert!(matches!(store.get_spawn(&a), Lookup::Found(r) if r.state == SpawnState::Starting));
        store.close();
    }

    fn settle_until_fault(store: &mut SpawnStore) -> Vec<StoreEvent> {
        for _ in 0..5000 {
            store.pump(0);
            let events = store.poll();
            if !events.is_empty() {
                return events;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("no fault");
    }
}
