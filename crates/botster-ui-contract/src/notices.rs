//! Bounded package-owned client notice reaction vocabulary.
//!
//! Hub admits authored declarations. Clients consume projected descriptors.
//! Text resolution belongs here so no generic client reimplements the pointer
//! or the 512-byte bound.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Maximum UTF-8 byte length of a resolved notice string.
pub const NOTICE_TEXT_MAX_BYTES: usize = 512;
/// Inclusive lower bound for declared notice TTL, in milliseconds.
pub const NOTICE_TTL_MIN_MS: u32 = 1_000;
/// Inclusive upper bound for declared notice TTL, in milliseconds.
pub const NOTICE_TTL_MAX_MS: u32 = 60_000;

/// Authored in `botster-package.json`. Owner is optional and must equal the package name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageNoticeReactionDeclaration {
    /// Optional owner. When present it must equal the admitting package name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// Exact emitted event name this notice reacts to.
    pub name: String,
    /// Version-one subject scope. Session only.
    pub subject_scope: PackageNoticeSubjectScope,
    /// One top-level RFC 6901 pointer to the notice string property.
    pub text_pointer: String,
    /// Client display lifetime in milliseconds.
    pub ttl_ms: u32,
    /// Transient notice severity.
    pub severity: PackageNoticeSeverity,
}

/// Projected to every client. Owner is required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageNoticeReactionDescriptor {
    /// Admitted package name that owns the event.
    pub owner: String,
    /// Exact emitted event name this notice reacts to.
    pub name: String,
    /// Version-one subject scope. Session only.
    pub subject_scope: PackageNoticeSubjectScope,
    /// One top-level RFC 6901 pointer to the notice string property.
    pub text_pointer: String,
    /// Client display lifetime in milliseconds.
    pub ttl_ms: u32,
    /// Transient notice severity.
    pub severity: PackageNoticeSeverity,
}

/// Version-one notice subject scope. Session only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageNoticeSubjectScope {
    /// Match `payload.subject` against the subscribed session subject.
    Session,
}

/// Transient notice severity vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageNoticeSeverity {
    /// Informational notice.
    Info,
    /// Warning notice.
    Warning,
    /// Error notice.
    Error,
}

impl PackageNoticeReactionDeclaration {
    /// Project this declaration to the public client descriptor.
    ///
    /// This is the only construction path for [`PackageNoticeReactionDescriptor`].
    /// It always sets `owner` to the admitted package name.
    #[must_use]
    pub fn into_descriptor(self, package_name: &str) -> PackageNoticeReactionDescriptor {
        PackageNoticeReactionDescriptor {
            owner: package_name.to_string(),
            name: self.name,
            subject_scope: self.subject_scope,
            text_pointer: self.text_pointer,
            ttl_ms: self.ttl_ms,
            severity: self.severity,
        }
    }
}

/// Invalid notice reaction declaration.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PackageNoticeReactionValidationError {
    /// Event name is empty or contains a wildcard.
    #[error("notice reaction name `{name}` is not an exact name")]
    InvalidName {
        /// Rejected name.
        name: String,
    },
    /// Optional owner is empty or contains a wildcard.
    #[error("notice reaction owner `{owner}` is not an exact owner")]
    InvalidOwner {
        /// Rejected owner.
        owner: String,
    },
    /// `text_pointer` is not one top-level RFC 6901 pointer.
    #[error("{0}")]
    Pointer(#[from] NoticePointerError),
    /// TTL is outside `1_000..=60_000`.
    #[error("notice reaction `{name}` ttl_ms {ttl_ms} is outside 1000..=60000")]
    TtlOutOfRange {
        /// Reaction name.
        name: String,
        /// Rejected TTL.
        ttl_ms: u32,
    },
    /// Two declarations resolve to the same `(owner, name)` pair.
    #[error("duplicate notice reaction `{owner}/{name}`")]
    DuplicateReaction {
        /// Resolved owner, empty when omitted.
        owner: String,
        /// Duplicate event name.
        name: String,
    },
}

/// Invalid one-segment RFC 6901 pointer.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NoticePointerError {
    /// Pointer is empty or does not start with `/`.
    #[error("notice text pointer `{pointer}` must start with `/`")]
    MissingLeadingSlash {
        /// Rejected pointer.
        pointer: String,
    },
    /// Pointer contains a second raw `/` before decoding.
    #[error("notice text pointer `{pointer}` must be one top-level segment")]
    MultiSegment {
        /// Rejected pointer.
        pointer: String,
    },
    /// Pointer ends with a bare `~`.
    #[error("notice text pointer `{pointer}` has a trailing `~`")]
    TrailingTilde {
        /// Rejected pointer.
        pointer: String,
    },
    /// Pointer contains `~` not followed by `0` or `1`.
    #[error("notice text pointer `{pointer}` has an unknown `~` escape")]
    UnknownEscape {
        /// Rejected pointer.
        pointer: String,
    },
    /// Pointer decodes to an empty property name.
    #[error("notice text pointer `{pointer}` decodes to an empty property name")]
    EmptyPropertyName {
        /// Rejected pointer.
        pointer: String,
    },
}

/// Typed failure while resolving notice text from an event payload.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NoticeTextError {
    /// The pointer is not a valid one-segment RFC 6901 pointer.
    #[error("{0}")]
    Pointer(#[from] NoticePointerError),
    /// The named property is absent.
    #[error("notice text property `{property}` is missing")]
    Missing {
        /// Decoded property name.
        property: String,
    },
    /// The named property is present but is not a JSON string.
    #[error("notice text property `{property}` is not a string")]
    NotString {
        /// Decoded property name.
        property: String,
    },
    /// The string is empty.
    #[error("notice text property `{property}` is empty")]
    Empty {
        /// Decoded property name.
        property: String,
    },
    /// The string exceeds [`NOTICE_TEXT_MAX_BYTES`].
    #[error(
        "notice text property `{property}` is {bytes} bytes; maximum is {NOTICE_TEXT_MAX_BYTES}"
    )]
    Oversized {
        /// Decoded property name.
        property: String,
        /// Measured UTF-8 byte length.
        bytes: usize,
    },
}

/// Validate the shared shape of authored notice reaction declarations.
pub fn validate_package_notice_reactions(
    declarations: &[PackageNoticeReactionDeclaration],
) -> Result<(), PackageNoticeReactionValidationError> {
    let mut seen = BTreeSet::new();
    for declaration in declarations {
        if !is_exact_identity(&declaration.name) {
            return Err(PackageNoticeReactionValidationError::InvalidName {
                name: declaration.name.clone(),
            });
        }
        if let Some(owner) = &declaration.owner
            && !is_exact_identity(owner)
        {
            return Err(PackageNoticeReactionValidationError::InvalidOwner {
                owner: owner.clone(),
            });
        }
        decode_notice_text_pointer(&declaration.text_pointer)?;
        if !(NOTICE_TTL_MIN_MS..=NOTICE_TTL_MAX_MS).contains(&declaration.ttl_ms) {
            return Err(PackageNoticeReactionValidationError::TtlOutOfRange {
                name: declaration.name.clone(),
                ttl_ms: declaration.ttl_ms,
            });
        }
        let owner_key = declaration.owner.clone().unwrap_or_default();
        if !seen.insert((owner_key.clone(), declaration.name.clone())) {
            return Err(PackageNoticeReactionValidationError::DuplicateReaction {
                owner: owner_key,
                name: declaration.name.clone(),
            });
        }
    }
    Ok(())
}

/// Decode one top-level RFC 6901 pointer into a JSON object property name.
///
/// Raw `/` separators are counted before `~1`/`~0` decoding so an escaped `/`
/// inside the property name remains valid.
pub fn decode_notice_text_pointer(pointer: &str) -> Result<String, NoticePointerError> {
    if !pointer.starts_with('/') {
        return Err(NoticePointerError::MissingLeadingSlash {
            pointer: pointer.to_string(),
        });
    }
    let raw = &pointer[1..];
    if raw.contains('/') {
        return Err(NoticePointerError::MultiSegment {
            pointer: pointer.to_string(),
        });
    }

    let mut decoded = String::new();
    let mut chars = raw.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '~' {
            decoded.push(ch);
            continue;
        }
        match chars.next() {
            Some('0') => decoded.push('~'),
            Some('1') => decoded.push('/'),
            Some(_) => {
                return Err(NoticePointerError::UnknownEscape {
                    pointer: pointer.to_string(),
                });
            }
            None => {
                return Err(NoticePointerError::TrailingTilde {
                    pointer: pointer.to_string(),
                });
            }
        }
    }
    if decoded.is_empty() {
        return Err(NoticePointerError::EmptyPropertyName {
            pointer: pointer.to_string(),
        });
    }
    Ok(decoded)
}

/// Resolve notice text from a payload using one validated top-level pointer.
///
/// Measures the decoded JSON string as UTF-8 bytes. Does not trim or truncate.
pub fn resolve_notice_text<'a>(
    payload: &'a Value,
    pointer: &str,
) -> Result<&'a str, NoticeTextError> {
    let property = decode_notice_text_pointer(pointer)?;
    let Some(value) = payload.get(&property) else {
        return Err(NoticeTextError::Missing { property });
    };
    let Some(text) = value.as_str() else {
        return Err(NoticeTextError::NotString { property });
    };
    let bytes = text.len();
    if bytes == 0 {
        return Err(NoticeTextError::Empty { property });
    }
    if bytes > NOTICE_TEXT_MAX_BYTES {
        return Err(NoticeTextError::Oversized { property, bytes });
    }
    Ok(text)
}

fn is_exact_identity(value: &str) -> bool {
    !value.trim().is_empty() && !value.contains('*') && !value.contains('?') && value != "*"
}
