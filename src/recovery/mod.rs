//! Hub recovery records. Host workers own persistence and effect execution.
//!
//! This module does not start effects, inspect the filesystem, or select policy.
//! Runtime integration must use the existing single Hub mutation owner.

pub(crate) mod journal;
pub(crate) mod record;
pub(crate) mod state_directory;
pub(crate) mod store;

#[cfg(test)]
mod tests;
