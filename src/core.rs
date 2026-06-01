//! Boundary between `botster-hub` product policy and `botster-core` mechanisms.
//!
//! `botster-core` remains the reusable local engine layer. The hub composes
//! core mechanics and owns policy around where they run, which clients may
//! attach, and which providers are admitted.

/// Hub-facing role for the embedded core dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddedCoreRole {
    /// Session spawning, PTY/process mechanics, lifecycle, and activity.
    LocalEngineMechanics,
    /// Transport-neutral primitives the hub adapts for clients and providers.
    PrimitiveContracts,
}
