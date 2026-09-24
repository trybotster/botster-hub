//! Append-only evidence for one File publication at a time.
//!
//! The startup phase scans this file before it creates or updates Hub state.
//! Host writes an intent before the first effect and a completion only after
//! the state document and all named effects have confirmed synchronization.

use std::fmt;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::fs::MetadataExt;

use sha2::{Digest, Sha256};

use super::state_directory::{StateDirectoryError, StateDirectoryOwnership};

const MAGIC: &[u8; 8] = b"BSTRJNL1";
const VERSION: u8 = 2;
const INTENT: u8 = 1;
const COMPLETION: u8 = 2;
const HEADER_LEN: usize = 26;
const INTENT_PREFIX_LEN: u64 = 42;
const CHECKSUM_LEN: u64 = 32;
const STREAM_BUFFER_LEN: usize = 8192;

#[derive(Debug)]
pub(crate) enum JournalError {
    Directory(StateDirectoryError),
    Io(io::Error),
    Malformed,
    ExistingStateWithoutJournal,
    EmptyJournalWithState,
    StateMissingWithJournal,
    Unresolved(u64),
    Quarantined,
    SequenceExhausted,
    LengthOverflow,
    PriorMismatch,
    JournalChanged,
}

impl fmt::Display for JournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Directory(error) => write!(formatter, "recovery journal directory: {error}"),
            Self::Io(error) => write!(formatter, "recovery journal I/O: {error}"),
            Self::Malformed => formatter.write_str("recovery journal frame is malformed"),
            Self::ExistingStateWithoutJournal => {
                formatter.write_str("existing Hub state has no recovery journal")
            }
            Self::EmptyJournalWithState => {
                formatter.write_str("existing Hub state has an empty recovery journal")
            }
            Self::StateMissingWithJournal => {
                formatter.write_str("recovery journal exists without Hub state")
            }
            Self::Unresolved(sequence) => {
                write!(
                    formatter,
                    "recovery journal intent {sequence} is unresolved"
                )
            }
            Self::Quarantined => formatter.write_str("recovery journal writes are quarantined"),
            Self::SequenceExhausted => {
                formatter.write_str("recovery journal sequence is exhausted")
            }
            Self::LengthOverflow => formatter.write_str("recovery journal length overflow"),
            Self::PriorMismatch => formatter.write_str("recovery journal prior file mismatch"),
            Self::JournalChanged => formatter.write_str("recovery journal file changed"),
        }
    }
}

impl JournalError {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Directory(_) => "recovery_directory_error",
            Self::Io(_) => "recovery_journal_io",
            Self::Malformed => "recovery_journal_malformed",
            Self::ExistingStateWithoutJournal => "recovery_journal_missing_for_state",
            Self::EmptyJournalWithState => "recovery_journal_empty_for_state",
            Self::StateMissingWithJournal => "recovery_state_missing_for_journal",
            Self::Unresolved(_) => "recovery_intent_unresolved",
            Self::Quarantined => "recovery_journal_quarantined",
            Self::SequenceExhausted => "recovery_sequence_exhausted",
            Self::LengthOverflow => "recovery_journal_length_overflow",
            Self::PriorMismatch => "recovery_prior_mismatch",
            Self::JournalChanged => "recovery_journal_changed",
        }
    }

    pub(crate) fn sequence(&self) -> Option<u64> {
        match self {
            Self::Unresolved(sequence) => Some(*sequence),
            _ => None,
        }
    }
}

impl std::error::Error for JournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Directory(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<StateDirectoryError> for JournalError {
    fn from(value: StateDirectoryError) -> Self {
        Self::Directory(value)
    }
}

impl From<io::Error> for JournalError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

/// Only a confirmed write-ahead append can construct this effect token.
#[derive(Debug)]
#[must_use]
pub(crate) struct DurableIntentReceipt {
    sequence: u64,
}

impl DurableIntentReceipt {
    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }
}

pub(crate) struct JournalIntent<'a> {
    pub(crate) metadata: &'a [u8],
    pub(crate) candidate_bytes: &'a [u8],
    pub(crate) prior_exists: bool,
    pub(crate) external: Option<JournalExternal<'a>>,
}

pub(crate) struct JournalExternal<'a> {
    pub(crate) prior: Option<&'a [u8]>,
    pub(crate) candidate: &'a [u8],
}

/// This index retains one sequence and one pending intent, never document bytes.
pub(crate) struct RecoveryJournal {
    directory: StateDirectoryOwnership,
    journal_identity: Option<(u64, u64)>,
    journal_len: u64,
    last_sequence: u64,
    pending: Option<u64>,
    quarantined: bool,
}

impl RecoveryJournal {
    /// Run in startup before initialization, adoption, or package refresh.
    pub(crate) fn scan(
        directory: StateDirectoryOwnership,
        state_exists: bool,
    ) -> Result<Self, JournalError> {
        let Some(mut file) = directory.open_recovery_journal()? else {
            return if state_exists {
                Err(JournalError::ExistingStateWithoutJournal)
            } else {
                Ok(Self {
                    directory,
                    journal_identity: None,
                    journal_len: 0,
                    last_sequence: 0,
                    pending: None,
                    quarantined: false,
                })
            };
        };
        let metadata = file.metadata()?;
        let identity = (metadata.dev(), metadata.ino());
        let file_len = metadata.len();
        let mut offset = 0_u64;
        let mut last_sequence = 0_u64;
        let mut pending = None;
        let mut saw_frame = false;
        while offset < file_len {
            let remaining = file_len - offset;
            if remaining < HEADER_LEN as u64 + CHECKSUM_LEN {
                return Err(JournalError::Malformed);
            }
            let mut header = [0_u8; HEADER_LEN];
            file.read_exact(&mut header)
                .map_err(|_| JournalError::Malformed)?;
            if &header[..8] != MAGIC || header[8] != VERSION {
                return Err(JournalError::Malformed);
            }
            let kind = header[9];
            let sequence = u64::from_le_bytes(header[10..18].try_into().unwrap());
            let payload_len = u64::from_le_bytes(header[18..26].try_into().unwrap());
            let frame_len = (HEADER_LEN as u64)
                .checked_add(payload_len)
                .and_then(|value| value.checked_add(CHECKSUM_LEN))
                .ok_or(JournalError::Malformed)?;
            offset = offset
                .checked_add(frame_len)
                .ok_or(JournalError::Malformed)?;
            if offset > file_len {
                return Err(JournalError::Malformed);
            }
            let mut checksum = Sha256::new();
            checksum.update(header);
            match kind {
                INTENT => {
                    if pending.is_some()
                        || sequence
                            != last_sequence
                                .checked_add(1)
                                .ok_or(JournalError::Malformed)?
                        || payload_len < INTENT_PREFIX_LEN
                    {
                        return Err(JournalError::Malformed);
                    }
                    let mut prefix = [0_u8; INTENT_PREFIX_LEN as usize];
                    file.read_exact(&mut prefix)
                        .map_err(|_| JournalError::Malformed)?;
                    checksum.update(prefix);
                    let metadata_len = u64::from_le_bytes(prefix[..8].try_into().unwrap());
                    let prior_flag = prefix[8];
                    let prior_len = u64::from_le_bytes(prefix[9..17].try_into().unwrap());
                    let candidate_len = u64::from_le_bytes(prefix[17..25].try_into().unwrap());
                    let external_kind = prefix[25];
                    let external_prior_len = u64::from_le_bytes(prefix[26..34].try_into().unwrap());
                    let external_candidate_len =
                        u64::from_le_bytes(prefix[34..42].try_into().unwrap());
                    if prior_flag > 1 || (prior_flag == 0 && prior_len != 0) {
                        return Err(JournalError::Malformed);
                    }
                    if external_kind > 2
                        || (external_kind != 2 && external_prior_len != 0)
                        || (external_kind == 0 && external_candidate_len != 0)
                    {
                        return Err(JournalError::Malformed);
                    }
                    let expected = INTENT_PREFIX_LEN
                        .checked_add(metadata_len)
                        .and_then(|value| value.checked_add(prior_len))
                        .and_then(|value| value.checked_add(candidate_len))
                        .and_then(|value| value.checked_add(external_prior_len))
                        .and_then(|value| value.checked_add(external_candidate_len))
                        .ok_or(JournalError::Malformed)?;
                    if expected != payload_len {
                        return Err(JournalError::Malformed);
                    }
                    hash_stream(&mut file, payload_len - INTENT_PREFIX_LEN, &mut checksum)?;
                    last_sequence = sequence;
                    pending = Some(sequence);
                }
                COMPLETION => {
                    if payload_len != 0 || pending != Some(sequence) {
                        return Err(JournalError::Malformed);
                    }
                    pending = None;
                }
                _ => return Err(JournalError::Malformed),
            }
            let mut actual = [0_u8; CHECKSUM_LEN as usize];
            file.read_exact(&mut actual)
                .map_err(|_| JournalError::Malformed)?;
            let expected: [u8; CHECKSUM_LEN as usize] = checksum.finalize().into();
            if actual != expected {
                return Err(JournalError::Malformed);
            }
            saw_frame = true;
        }
        directory.ensure_recovery_journal(&file)?;
        if file.metadata()?.len() != file_len {
            return Err(JournalError::JournalChanged);
        }
        if state_exists && !saw_frame {
            return Err(JournalError::EmptyJournalWithState);
        }
        if let Some(sequence) = pending {
            return Err(JournalError::Unresolved(sequence));
        }
        if !state_exists && saw_frame {
            return Err(JournalError::StateMissingWithJournal);
        }
        Ok(Self {
            directory,
            journal_identity: Some(identity),
            journal_len: file_len,
            last_sequence,
            pending: None,
            quarantined: false,
        })
    }

    /// Append and sync exact prior document bytes before any named effect.
    pub(crate) fn begin(
        &mut self,
        intent: JournalIntent<'_>,
    ) -> Result<DurableIntentReceipt, JournalError> {
        if self.quarantined || self.pending.is_some() {
            return Err(JournalError::Quarantined);
        }
        let sequence = self
            .last_sequence
            .checked_add(1)
            .ok_or(JournalError::SequenceExhausted)?;
        let mut prior = self.directory.open_document_bytes()?;
        if prior.is_some() != intent.prior_exists {
            return Err(JournalError::PriorMismatch);
        }
        let prior_len = prior
            .as_ref()
            .map(|file| file.metadata().map(|metadata| metadata.len()))
            .transpose()?
            .unwrap_or(0);
        let metadata_len =
            u64::try_from(intent.metadata.len()).map_err(|_| JournalError::LengthOverflow)?;
        let candidate_len = u64::try_from(intent.candidate_bytes.len())
            .map_err(|_| JournalError::LengthOverflow)?;
        let external_kind = match intent.external.as_ref() {
            None => 0_u8,
            Some(external) if external.prior.is_none() => 1_u8,
            Some(_) => 2_u8,
        };
        let external_prior_len = intent
            .external
            .as_ref()
            .and_then(|external| external.prior)
            .map(|prior| u64::try_from(prior.len()))
            .transpose()
            .map_err(|_| JournalError::LengthOverflow)?
            .unwrap_or(0);
        let external_candidate_len = intent
            .external
            .as_ref()
            .map(|external| u64::try_from(external.candidate.len()))
            .transpose()
            .map_err(|_| JournalError::LengthOverflow)?
            .unwrap_or(0);
        let payload_len = INTENT_PREFIX_LEN
            .checked_add(metadata_len)
            .and_then(|value| value.checked_add(prior_len))
            .and_then(|value| value.checked_add(candidate_len))
            .and_then(|value| value.checked_add(external_prior_len))
            .and_then(|value| value.checked_add(external_candidate_len))
            .ok_or(JournalError::LengthOverflow)?;
        let frame_len = (HEADER_LEN as u64)
            .checked_add(payload_len)
            .and_then(|value| value.checked_add(CHECKSUM_LEN))
            .ok_or(JournalError::LengthOverflow)?;
        let next_len = self
            .journal_len
            .checked_add(frame_len)
            .ok_or(JournalError::LengthOverflow)?;
        let file_result = (|| match self.journal_identity {
            Some(identity) => {
                let file = self.directory.open_recovery_journal_append()?;
                if file_identity(&file)? != identity {
                    Err(JournalError::JournalChanged)
                } else {
                    Ok(file)
                }
            }
            None => self.directory.create_recovery_journal().map_err(Into::into),
        })();
        let mut file = match file_result {
            Ok(file) => file,
            Err(error) => {
                self.quarantined = true;
                return Err(error);
            }
        };
        if file.metadata()?.len() != self.journal_len {
            self.quarantined = true;
            return Err(JournalError::JournalChanged);
        }
        let created = self.journal_identity.is_none();
        let result = (|| {
            let header = frame_header(INTENT, sequence, payload_len);
            let mut checksum = Sha256::new();
            write_hashed(&mut file, &header, &mut checksum)?;
            write_hashed(&mut file, &metadata_len.to_le_bytes(), &mut checksum)?;
            write_hashed(&mut file, &[u8::from(intent.prior_exists)], &mut checksum)?;
            write_hashed(&mut file, &prior_len.to_le_bytes(), &mut checksum)?;
            write_hashed(&mut file, &candidate_len.to_le_bytes(), &mut checksum)?;
            write_hashed(&mut file, &[external_kind], &mut checksum)?;
            write_hashed(&mut file, &external_prior_len.to_le_bytes(), &mut checksum)?;
            write_hashed(
                &mut file,
                &external_candidate_len.to_le_bytes(),
                &mut checksum,
            )?;
            write_hashed(&mut file, intent.metadata, &mut checksum)?;
            if let Some(prior) = prior.as_mut() {
                copy_hashed(prior, &mut file, prior_len, &mut checksum)?;
                if prior.read(&mut [0_u8; 1])? != 0 {
                    return Err(JournalError::PriorMismatch);
                }
                self.directory.ensure_document_bytes(prior)?;
            }
            write_hashed(&mut file, intent.candidate_bytes, &mut checksum)?;
            if let Some(external) = intent.external.as_ref() {
                if let Some(prior) = external.prior {
                    write_hashed(&mut file, prior, &mut checksum)?;
                }
                write_hashed(&mut file, external.candidate, &mut checksum)?;
            }
            let digest: [u8; CHECKSUM_LEN as usize] = checksum.finalize().into();
            file.write_all(&digest)?;
            file.sync_all()?;
            if created {
                self.directory.sync_directory()?;
            }
            self.directory.ensure_recovery_journal(&file)?;
            Ok::<_, JournalError>(())
        })();
        if let Err(error) = result {
            self.quarantined = true;
            return Err(error);
        }
        self.journal_identity = Some(match file_identity(&file) {
            Ok(identity) => identity,
            Err(error) => {
                self.quarantined = true;
                return Err(error);
            }
        });
        self.last_sequence = sequence;
        self.pending = Some(sequence);
        self.journal_len = next_len;
        Ok(DurableIntentReceipt { sequence })
    }

    /// Only a caller with confirmed state and effect sync may call this method.
    pub(crate) fn complete(&mut self, receipt: &DurableIntentReceipt) -> Result<(), JournalError> {
        self.complete_inner(receipt, false)
    }

    #[cfg(test)]
    fn complete_with_sync_failure_for_test(
        &mut self,
        receipt: &DurableIntentReceipt,
    ) -> Result<(), JournalError> {
        self.complete_inner(receipt, true)
    }

    fn complete_inner(
        &mut self,
        receipt: &DurableIntentReceipt,
        inject_sync_failure: bool,
    ) -> Result<(), JournalError> {
        if self.quarantined || self.pending != Some(receipt.sequence) {
            return Err(JournalError::Quarantined);
        }
        let mut file = match self.directory.open_recovery_journal_append() {
            Ok(file) => file,
            Err(error) => {
                self.quarantined = true;
                return Err(error.into());
            }
        };
        if Some(file_identity(&file)?) != self.journal_identity {
            self.quarantined = true;
            return Err(JournalError::JournalChanged);
        }
        if file.metadata()?.len() != self.journal_len {
            self.quarantined = true;
            return Err(JournalError::JournalChanged);
        }
        let next_len = self
            .journal_len
            .checked_add(HEADER_LEN as u64 + CHECKSUM_LEN)
            .ok_or(JournalError::LengthOverflow)?;
        let result = (|| {
            let header = frame_header(COMPLETION, receipt.sequence, 0);
            let digest: [u8; CHECKSUM_LEN as usize] = Sha256::digest(header).into();
            file.write_all(&header)?;
            file.write_all(&digest)?;
            if inject_sync_failure {
                return Err(JournalError::Io(io::Error::other(
                    "injected journal completion sync failure",
                )));
            }
            file.sync_all()?;
            self.directory.ensure_recovery_journal(&file)?;
            Ok::<_, JournalError>(())
        })();
        if let Err(error) = result {
            self.quarantined = true;
            return Err(error);
        }
        self.pending = None;
        self.journal_len = next_len;
        Ok(())
    }
}

fn frame_header(kind: u8, sequence: u64, payload_len: u64) -> [u8; HEADER_LEN] {
    let mut bytes = [0_u8; HEADER_LEN];
    bytes[..8].copy_from_slice(MAGIC);
    bytes[8] = VERSION;
    bytes[9] = kind;
    bytes[10..18].copy_from_slice(&sequence.to_le_bytes());
    bytes[18..26].copy_from_slice(&payload_len.to_le_bytes());
    bytes
}

fn file_identity(file: &File) -> Result<(u64, u64), JournalError> {
    let metadata = file.metadata()?;
    Ok((metadata.dev(), metadata.ino()))
}

fn write_hashed(file: &mut File, bytes: &[u8], checksum: &mut Sha256) -> Result<(), JournalError> {
    file.write_all(bytes)?;
    checksum.update(bytes);
    Ok(())
}

fn copy_hashed(
    source: &mut File,
    destination: &mut File,
    length: u64,
    checksum: &mut Sha256,
) -> Result<(), JournalError> {
    let mut remaining = length;
    let mut buffer = [0_u8; STREAM_BUFFER_LEN];
    while remaining != 0 {
        let take = remaining.min(buffer.len() as u64) as usize;
        source.read_exact(&mut buffer[..take])?;
        write_hashed(destination, &buffer[..take], checksum)?;
        remaining -= take as u64;
    }
    Ok(())
}

fn hash_stream(source: &mut File, length: u64, checksum: &mut Sha256) -> Result<(), JournalError> {
    let mut remaining = length;
    let mut buffer = [0_u8; STREAM_BUFFER_LEN];
    while remaining != 0 {
        let take = remaining.min(buffer.len() as u64) as usize;
        source
            .read_exact(&mut buffer[..take])
            .map_err(|_| JournalError::Malformed)?;
        checksum.update(&buffer[..take]);
        remaining -= take as u64;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_directory(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir()
            .join("botster-recovery-journal")
            .join(format!("{name}-{unique}"));
        fs::create_dir_all(&path).expect("create isolated journal directory");
        path
    }

    fn fresh(directory: &PathBuf) -> RecoveryJournal {
        RecoveryJournal::scan(
            StateDirectoryOwnership::acquire(directory).expect("own directory"),
            false,
        )
        .expect("fresh journal")
    }

    fn intent<'a>(candidate_bytes: &'a [u8], prior_exists: bool) -> JournalIntent<'a> {
        JournalIntent {
            metadata: br#"{"kind":"state_write"}"#,
            candidate_bytes,
            prior_exists,
            external: None,
        }
    }

    #[test]
    fn synced_intent_survives_interruption_before_effect() {
        let path = test_directory("intent-before-effect");
        let unrelated = path.join("unrelated");
        fs::write(&unrelated, b"keep").expect("write unrelated file");
        let mut journal = fresh(&path);
        let receipt = journal
            .begin(intent(br#"{"state":1}"#, false))
            .expect("intent");
        assert_eq!(receipt.sequence(), 1);
        drop(journal);
        let refusal = RecoveryJournal::scan(
            StateDirectoryOwnership::acquire(&path).expect("reopen directory"),
            false,
        );
        assert!(matches!(refusal, Err(JournalError::Unresolved(1))));
        assert!(!path.join("hub-state.json").exists());
        assert_eq!(fs::read(unrelated).expect("read unrelated file"), b"keep");
        fs::remove_dir_all(path).expect("remove isolated directory");
    }

    #[test]
    fn external_intent_retains_exact_file_bytes_before_effect() {
        let path = test_directory("external-before-effect");
        let mut journal = fresh(&path);
        let prior = b"prior repo bytes\n";
        let candidate = b"candidate repo bytes\n";
        let receipt = journal
            .begin(JournalIntent {
                metadata: br#"{"kind":"state_and_external_file_write","path":"repo"}"#,
                candidate_bytes: br#"{"state":1}"#,
                prior_exists: false,
                external: Some(JournalExternal {
                    prior: Some(prior),
                    candidate,
                }),
            })
            .expect("external intent");
        assert_eq!(receipt.sequence(), 1);
        drop(journal);
        let bytes = fs::read(path.join("hub-recovery.log")).expect("read durable intent");
        assert!(bytes.windows(prior.len()).any(|part| part == prior));
        assert!(bytes.windows(candidate.len()).any(|part| part == candidate));
        assert!(matches!(
            RecoveryJournal::scan(
                StateDirectoryOwnership::acquire(&path).expect("reopen directory"),
                false,
            ),
            Err(JournalError::Unresolved(1))
        ));
        assert!(!path.join("hub-state.json").exists());
        fs::remove_dir_all(path).expect("remove isolated directory");
    }

    #[test]
    fn mismatched_state_prior_refuses_before_the_external_file_effect() {
        let path = test_directory("missing-state-prior");
        let repo_file = path.join("repo-session-types.json");
        fs::write(&repo_file, b"prior repo bytes").expect("write prior repo file");
        let mut journal = fresh(&path);
        assert!(matches!(
            journal.begin(JournalIntent {
                metadata: br#"{"kind":"state_and_external_file_write"}"#,
                candidate_bytes: br#"{"state":1}"#,
                prior_exists: true,
                external: Some(JournalExternal {
                    prior: Some(b"prior repo bytes"),
                    candidate: b"candidate repo bytes",
                }),
            }),
            Err(JournalError::PriorMismatch)
        ));
        assert_eq!(
            fs::read(&repo_file).expect("read unchanged repo file"),
            b"prior repo bytes"
        );
        assert!(!path.join("hub-state.json").exists());
        assert!(!path.join("hub-recovery.log").exists());
        drop(journal);
        fs::remove_dir_all(path).expect("remove isolated directory");
    }

    #[test]
    fn two_synced_operations_keep_exact_prior_bytes() {
        let path = test_directory("two-operations");
        let mut journal = fresh(&path);
        let first_bytes = br#"{"compact":1}"#;
        let first = journal
            .begin(intent(first_bytes, false))
            .expect("first intent");
        assert!(matches!(
            journal.directory.write_document(first_bytes),
            Ok(super::super::state_directory::StateDocumentCommit::Synced)
        ));
        journal.complete(&first).expect("first completion");
        let second_bytes = br#"{"compact":2}"#;
        let second = journal
            .begin(intent(second_bytes, true))
            .expect("second intent");
        assert_eq!(second.sequence(), 2);
        assert!(matches!(
            journal.directory.write_document(second_bytes),
            Ok(super::super::state_directory::StateDocumentCommit::Synced)
        ));
        journal.complete(&second).expect("second completion");
        drop(journal);
        let reloaded = RecoveryJournal::scan(
            StateDirectoryOwnership::acquire(&path).expect("reopen directory"),
            true,
        )
        .expect("both operations completed");
        assert_eq!(reloaded.last_sequence, 2);
        let journal_bytes = fs::read(path.join("hub-recovery.log")).expect("read journal");
        assert!(
            journal_bytes
                .windows(first_bytes.len())
                .any(|part| part == first_bytes)
        );
        assert!(
            journal_bytes
                .windows(second_bytes.len())
                .any(|part| part == second_bytes)
        );
        drop(reloaded);
        fs::remove_dir_all(path).expect("remove isolated directory");
    }

    #[test]
    fn full_completion_with_failed_sync_is_safe_on_reload() {
        let path = test_directory("completion-sync");
        let mut journal = fresh(&path);
        let bytes = br#"{"state":1}"#;
        let receipt = journal.begin(intent(bytes, false)).expect("intent");
        assert!(matches!(
            journal.directory.write_document(bytes),
            Ok(super::super::state_directory::StateDocumentCommit::Synced)
        ));
        assert!(matches!(
            journal.complete_with_sync_failure_for_test(&receipt),
            Err(JournalError::Io(_))
        ));
        assert!(journal.quarantined);
        drop(journal);
        let reloaded = RecoveryJournal::scan(
            StateDirectoryOwnership::acquire(&path).expect("reopen directory"),
            true,
        )
        .expect("complete visible frame follows synced state");
        assert_eq!(reloaded.last_sequence, 1);
        drop(reloaded);
        fs::remove_dir_all(path).expect("remove isolated directory");
    }

    #[test]
    fn malformed_tail_and_legacy_state_refuse_writes() {
        let path = test_directory("malformed-tail");
        let ownership = StateDirectoryOwnership::acquire(&path).expect("own directory");
        let mut file = ownership.create_recovery_journal().expect("create journal");
        file.write_all(b"BSTR").expect("write partial header");
        file.sync_all().expect("sync partial header");
        ownership.sync_directory().expect("sync journal name");
        drop(file);
        drop(ownership);
        assert!(matches!(
            RecoveryJournal::scan(
                StateDirectoryOwnership::acquire(&path).expect("reopen directory"),
                false
            ),
            Err(JournalError::Malformed)
        ));
        fs::remove_dir_all(path).expect("remove malformed directory");

        let legacy = test_directory("legacy-state");
        let ownership = StateDirectoryOwnership::acquire(&legacy).expect("own directory");
        assert!(matches!(
            ownership.write_document(br#"{"legacy":true}"#),
            Ok(super::super::state_directory::StateDocumentCommit::Synced)
        ));
        drop(ownership);
        assert!(matches!(
            RecoveryJournal::scan(
                StateDirectoryOwnership::acquire(&legacy).expect("reopen directory"),
                true
            ),
            Err(JournalError::ExistingStateWithoutJournal)
        ));
        fs::remove_dir_all(legacy).expect("remove legacy directory");
    }

    #[test]
    fn sequence_overflow_refuses_before_creating_a_journal() {
        let path = test_directory("sequence-overflow");
        let mut journal = fresh(&path);
        journal.last_sequence = u64::MAX;
        assert!(matches!(
            journal.begin(intent(br#"{"state":1}"#, false)),
            Err(JournalError::SequenceExhausted)
        ));
        assert!(!path.join("hub-recovery.log").exists());
        drop(journal);
        fs::remove_dir_all(path).expect("remove isolated directory");
    }
}
