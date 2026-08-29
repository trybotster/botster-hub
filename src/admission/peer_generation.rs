//! Existing peer-instance ownership identity.
//!
//! Hub peer ownership is the grant id already carried on attach owners and
//! WebRTC peer state. This module records that identity. It does not mint a
//! counter or a wire field.

/// Compare an owner's grant id with a candidate grant id.
///
/// A match requires both sides to be present and equal. Missing grant ids do
/// not match. Client-id matching stays in attach_routes.
pub(crate) fn grant_ids_match(
    owner_grant_id: Option<&str>,
    candidate_grant_id: Option<&str>,
) -> bool {
    match (owner_grant_id, candidate_grant_id) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}
