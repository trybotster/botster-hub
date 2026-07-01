//! Hub-owned credential provider policy and browser trust metadata checks.
//!
//! `botster-core` owns the reusable credential-store and identity contracts.
//! The hub owns concrete provider selection, key id references, durable public
//! browser trust metadata, and fail-closed validation against persisted
//! references.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use botster_core::{
    CredentialRecord, CredentialStore, CredentialStoreError, DeviceFingerprint, device_fingerprint,
    verify_device_fingerprint,
};
use serde::{Deserialize, Serialize};

use crate::persistence::{
    BootstrapGrantRecord, CredentialKeyReference, HubState, TrustedBrowserIdentity,
};

const KEYCHAIN_SERVICE: &str = "botster-hub";

/// Concrete credential provider selected by the hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialProviderKind {
    /// Production OS credential storage.
    OsKeychain,
    /// Explicit test/dev file store. Production startup never selects this.
    TestFile,
}

impl fmt::Display for CredentialProviderKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OsKeychain => formatter.write_str("os_keychain"),
            Self::TestFile => formatter.write_str("test_file"),
        }
    }
}

/// Stable hub credential key purpose labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialKeyPurpose {
    /// Hub-owned long-lived identity material.
    HubIdentity,
    /// Browser identity or session secret material.
    BrowserIdentity,
    /// Bootstrap grant secret material.
    BootstrapGrant,
}

impl fmt::Display for CredentialKeyPurpose {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HubIdentity => formatter.write_str("hub_identity"),
            Self::BrowserIdentity => formatter.write_str("browser_identity"),
            Self::BootstrapGrant => formatter.write_str("bootstrap_grant"),
        }
    }
}

/// Production OS-keychain credential store adapter.
#[derive(Debug, Clone, Default)]
pub struct OsKeychainCredentialStore;

impl OsKeychainCredentialStore {
    /// Build the default OS-keychain provider.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    fn entry(key: &str) -> Result<keyring::Entry, CredentialStoreError> {
        keyring::Entry::new(KEYCHAIN_SERVICE, key).map_err(keyring_error)
    }
}

impl CredentialStore for OsKeychainCredentialStore {
    fn get(&self, key: &str) -> Result<Option<CredentialRecord>, CredentialStoreError> {
        match Self::entry(key)?.get_secret() {
            Ok(secret) => Ok(Some(CredentialRecord::new(secret))),
            Err(error) if keyring_error_is_not_found(&error) => Ok(None),
            Err(error) => Err(keyring_error(error)),
        }
    }

    fn set(&mut self, key: &str, record: CredentialRecord) -> Result<(), CredentialStoreError> {
        Self::entry(key)?
            .set_secret(record.as_bytes())
            .map_err(keyring_error)
    }

    fn delete(&mut self, key: &str) -> Result<(), CredentialStoreError> {
        match Self::entry(key)?.delete_credential() {
            Ok(()) => Ok(()),
            Err(error) if keyring_error_is_not_found(&error) => Err(CredentialStoreError::NotFound),
            Err(error) => Err(keyring_error(error)),
        }
    }
}

fn keyring_error(error: keyring::Error) -> CredentialStoreError {
    CredentialStoreError::Rejected(format!("{error}"))
}

fn keyring_error_is_not_found(error: &keyring::Error) -> bool {
    matches!(error, keyring::Error::NoEntry)
}

/// Explicit test/dev file credential store.
///
/// This store intentionally exists for deterministic tests and local fixtures
/// only. Production startup uses [`OsKeychainCredentialStore`] and has no
/// plaintext file fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestFileCredentialStore {
    path: PathBuf,
}

impl TestFileCredentialStore {
    /// Build an explicit test/dev file store.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    fn read_records(&self) -> Result<BTreeMap<String, Vec<u8>>, CredentialStoreError> {
        match fs::read(&self.path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|error| CredentialStoreError::Rejected(error.to_string())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
            Err(error) => Err(CredentialStoreError::Rejected(error.to_string())),
        }
    }

    fn write_records(
        &self,
        records: &BTreeMap<String, Vec<u8>>,
    ) -> Result<(), CredentialStoreError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                CredentialStoreError::Rejected(format!("create credential directory: {error}"))
            })?;
        }
        let bytes = serde_json::to_vec_pretty(records)
            .map_err(|error| CredentialStoreError::Rejected(error.to_string()))?;
        fs::write(&self.path, bytes)
            .map_err(|error| CredentialStoreError::Rejected(error.to_string()))
    }

    /// Return the explicit test/dev file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl CredentialStore for TestFileCredentialStore {
    fn get(&self, key: &str) -> Result<Option<CredentialRecord>, CredentialStoreError> {
        Ok(self.read_records()?.remove(key).map(CredentialRecord::new))
    }

    fn set(&mut self, key: &str, record: CredentialRecord) -> Result<(), CredentialStoreError> {
        let mut records = self.read_records()?;
        records.insert(key.to_string(), record.as_bytes().to_vec());
        self.write_records(&records)
    }

    fn delete(&mut self, key: &str) -> Result<(), CredentialStoreError> {
        let mut records = self.read_records()?;
        if records.remove(key).is_some() {
            self.write_records(&records)
        } else {
            Err(CredentialStoreError::NotFound)
        }
    }
}

/// Credential policy validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialPolicyError {
    /// Persisted metadata refers to a different concrete provider.
    ProviderMismatch {
        key_id: String,
        expected: CredentialProviderKind,
        actual: CredentialProviderKind,
    },
    /// Persisted metadata refers to credential material that is absent.
    MissingCredential { key_id: String },
    /// The backing credential provider rejected a required lookup.
    StoreRejected { key_id: String, message: String },
    /// Browser public key and stored fingerprint do not match.
    FingerprintMismatch { browser_id: String },
}

impl fmt::Display for CredentialPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProviderMismatch {
                key_id,
                expected,
                actual,
            } => write!(
                formatter,
                "credential key {key_id} expects provider {expected}, got {actual}"
            ),
            Self::MissingCredential { key_id } => {
                write!(formatter, "credential key {key_id} is missing")
            }
            Self::StoreRejected { key_id, message } => {
                write!(
                    formatter,
                    "credential provider rejected key {key_id}: {message}"
                )
            }
            Self::FingerprintMismatch { browser_id } => {
                write!(
                    formatter,
                    "browser identity {browser_id} fingerprint mismatch"
                )
            }
        }
    }
}

impl Error for CredentialPolicyError {}

/// Build the stable hub credential key id for a purpose and local identifier.
#[must_use]
pub fn credential_key_id(host_id: &str, purpose: CredentialKeyPurpose, local_id: &str) -> String {
    format!("hub/{host_id}/{purpose}/{local_id}")
}

/// Validate persisted credential references and public browser identity anchors.
///
/// An available-but-empty state is valid for first boot. Fail-closed behavior
/// applies once hub-state contains credential references or browser identities.
pub fn validate_hub_credentials(
    state: &HubState,
    provider_kind: CredentialProviderKind,
    store: &impl CredentialStore,
) -> Result<(), CredentialPolicyError> {
    let mut validated_key_ids = BTreeSet::new();
    for key in &state.credential_keys {
        validate_key_reference(key, provider_kind, store)?;
        validated_key_ids.insert(key.key_id.clone());
    }

    for browser in &state.trusted_browser_identities {
        if !browser.fingerprint_matches_public_key() {
            return Err(CredentialPolicyError::FingerprintMismatch {
                browser_id: browser.browser_id.clone(),
            });
        }
        if let Some(key_id) = &browser.credential_key_id {
            ensure_validated_key_id(&validated_key_ids, key_id)?;
        }
    }

    for grant in &state.bootstrap_grants {
        if let Some(key_id) = &grant.credential_key_id {
            ensure_validated_key_id(&validated_key_ids, key_id)?;
        }
    }

    Ok(())
}

fn ensure_validated_key_id(
    validated_key_ids: &BTreeSet<String>,
    key_id: &str,
) -> Result<(), CredentialPolicyError> {
    if validated_key_ids.contains(key_id) {
        Ok(())
    } else {
        Err(CredentialPolicyError::MissingCredential {
            key_id: key_id.to_string(),
        })
    }
}

fn validate_key_reference(
    key: &CredentialKeyReference,
    provider_kind: CredentialProviderKind,
    store: &impl CredentialStore,
) -> Result<(), CredentialPolicyError> {
    if key.provider != provider_kind {
        return Err(CredentialPolicyError::ProviderMismatch {
            key_id: key.key_id.clone(),
            expected: key.provider,
            actual: provider_kind,
        });
    }

    match store.get(&key.key_id) {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(CredentialPolicyError::MissingCredential {
            key_id: key.key_id.clone(),
        }),
        Err(error) => Err(CredentialPolicyError::StoreRejected {
            key_id: key.key_id.clone(),
            message: error.to_string(),
        }),
    }
}

impl TrustedBrowserIdentity {
    /// Build a trusted browser identity from public key bytes.
    #[must_use]
    pub fn trusted(
        browser_id: impl Into<String>,
        public_key: Vec<u8>,
        trusted_at_unix_ms: u64,
        audit_reason: impl Into<String>,
    ) -> Self {
        let fingerprint = device_fingerprint(&public_key).0;
        Self {
            browser_id: browser_id.into(),
            public_key,
            fingerprint,
            credential_key_id: None,
            trusted_at_unix_ms,
            expires_at_unix_ms: None,
            revoked_at_unix_ms: None,
            audit_reason: audit_reason.into(),
        }
    }

    /// Return whether public key bytes match the persisted fingerprint.
    #[must_use]
    pub fn fingerprint_matches_public_key(&self) -> bool {
        verify_device_fingerprint(
            &self.public_key,
            &DeviceFingerprint(self.fingerprint.clone()),
        )
    }

    /// Return whether this browser identity is usable at `now_unix_ms`.
    #[must_use]
    pub fn is_trusted_at(&self, now_unix_ms: u64) -> bool {
        self.fingerprint_matches_public_key()
            && self.revoked_at_unix_ms.is_none()
            && self
                .expires_at_unix_ms
                .is_none_or(|expires_at| expires_at > now_unix_ms)
    }
}

impl BootstrapGrantRecord {
    /// Return whether this grant can still be redeemed at `now_unix_ms`.
    #[must_use]
    pub fn is_redeemable_at(&self, now_unix_ms: u64) -> bool {
        self.revoked_at_unix_ms.is_none()
            && self.redeemed_at_unix_ms.is_none()
            && self.expires_at_unix_ms > now_unix_ms
    }
}
