//! Boundary between `botster-hub` profile policy and `botster-core` mechanisms.
//!
//! `botster-core` remains the reusable local engine layer. The first-party host
//! profile composes core mechanics and owns policy around where they run, which
//! clients may attach, and which providers are admitted.

/// Hub-facing role for the embedded core dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddedCoreRole {
    /// Session spawning, PTY/process mechanics, lifecycle, and activity through
    /// the default local-runtime-backed engine facade.
    LocalEngineMechanics,
    /// Transport-neutral primitives the hub adapts for clients and providers.
    PrimitiveContracts,
}
