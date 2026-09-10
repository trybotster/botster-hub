//! Hub-owned admission for Lua states and Rust values crossing the Lua ABI.
//!
//! This module deliberately contains no production limit values. The host must
//! construct one account with explicit policy before using the bounded loader.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Explicit memory limits shared by every Lua plugin loaded by one Hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LuaMemoryLimits {
    pub(crate) per_vm_bytes: usize,
    pub(crate) total_vm_bytes: usize,
    pub(crate) per_callback_bytes: usize,
    pub(crate) total_callback_bytes: usize,
}

impl LuaMemoryLimits {
    pub(crate) fn validate(self) -> Result<Self, LuaMemoryLimitError> {
        if self.per_vm_bytes == 0
            || self.total_vm_bytes == 0
            || self.per_callback_bytes == 0
            || self.total_callback_bytes == 0
        {
            return Err(LuaMemoryLimitError::Zero);
        }
        if self.per_vm_bytes > self.total_vm_bytes {
            return Err(LuaMemoryLimitError::PerVmExceedsTotal);
        }
        if self.per_callback_bytes > self.total_callback_bytes {
            return Err(LuaMemoryLimitError::PerCallbackExceedsTotal);
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LuaMemoryLimitError {
    Zero,
    PerVmExceedsTotal,
    PerCallbackExceedsTotal,
}

/// One Hub-wide account. Reservations are lock-free and released by ownership.
#[derive(Debug)]
pub(crate) struct LuaMemoryAccount {
    limits: LuaMemoryLimits,
    vm_bytes: AtomicUsize,
    callback_bytes: AtomicUsize,
}

impl LuaMemoryAccount {
    pub(crate) fn new(limits: LuaMemoryLimits) -> Result<Arc<Self>, LuaMemoryLimitError> {
        let limits = limits.validate()?;
        Ok(Arc::new(Self {
            limits,
            vm_bytes: AtomicUsize::new(0),
            callback_bytes: AtomicUsize::new(0),
        }))
    }

    pub(crate) const fn limits(&self) -> LuaMemoryLimits {
        self.limits
    }

    /// Reserve a complete Lua-state allowance before constructing the state.
    pub(crate) fn reserve_vm(self: &Arc<Self>) -> Result<LuaVmCharge, LuaMemoryCapacityError> {
        reserve(
            &self.vm_bytes,
            self.limits.per_vm_bytes,
            self.limits.total_vm_bytes,
            LuaMemoryClass::Vm,
        )?;
        Ok(LuaVmCharge {
            account: Arc::clone(self),
        })
    }

    /// Reserve the callback ceiling before constructing or cloning its result.
    pub(crate) fn reserve_callback(
        self: &Arc<Self>,
    ) -> Result<LuaCallbackCharge, LuaMemoryCapacityError> {
        self.reserve_callback_bytes(self.limits.per_callback_bytes)
    }

    /// Reserve known Rust allocation requests before constructing their storage.
    pub(crate) fn reserve_callback_bytes(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<LuaCallbackCharge, LuaMemoryCapacityError> {
        if bytes > self.limits.per_callback_bytes {
            return Err(LuaMemoryCapacityError {
                class: LuaMemoryClass::Callback,
                requested: bytes,
                available: self.limits.per_callback_bytes,
            });
        }
        reserve(
            &self.callback_bytes,
            bytes,
            self.limits.total_callback_bytes,
            LuaMemoryClass::Callback,
        )?;
        Ok(LuaCallbackCharge {
            account: Arc::clone(self),
            bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn usage(&self) -> (usize, usize) {
        (
            self.vm_bytes.load(Ordering::Acquire),
            self.callback_bytes.load(Ordering::Acquire),
        )
    }
}

fn reserve(
    used: &AtomicUsize,
    requested: usize,
    capacity: usize,
    class: LuaMemoryClass,
) -> Result<(), LuaMemoryCapacityError> {
    let mut current = used.load(Ordering::Acquire);
    loop {
        let next = current
            .checked_add(requested)
            .filter(|next| *next <= capacity)
            .ok_or(LuaMemoryCapacityError {
                class,
                requested,
                available: capacity.saturating_sub(current),
            })?;
        match used.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(()),
            Err(observed) => current = observed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LuaMemoryClass {
    Vm,
    Callback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LuaMemoryCapacityError {
    pub(crate) class: LuaMemoryClass,
    pub(crate) requested: usize,
    pub(crate) available: usize,
}

impl fmt::Display for LuaMemoryCapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Lua {} memory capacity exhausted: requested {} bytes, {} available",
            match self.class {
                LuaMemoryClass::Vm => "VM",
                LuaMemoryClass::Callback => "callback",
            },
            self.requested,
            self.available
        )
    }
}

pub(crate) struct LuaVmCharge {
    account: Arc<LuaMemoryAccount>,
}

impl fmt::Debug for LuaVmCharge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LuaVmCharge")
            .finish_non_exhaustive()
    }
}

impl Drop for LuaVmCharge {
    fn drop(&mut self) {
        self.account
            .vm_bytes
            .fetch_sub(self.account.limits.per_vm_bytes, Ordering::AcqRel);
    }
}

/// This non-cloneable charge owns one Rust operation allowance.
/// Lua results use the separate Lua-state allowance after Rust conversion ends.
pub(crate) struct LuaCallbackCharge {
    account: Arc<LuaMemoryAccount>,
    bytes: usize,
}

impl fmt::Debug for LuaCallbackCharge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LuaCallbackCharge")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl Drop for LuaCallbackCharge {
    fn drop(&mut self) {
        self.account
            .callback_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account() -> Arc<LuaMemoryAccount> {
        LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 4,
            total_vm_bytes: 8,
            per_callback_bytes: 3,
            total_callback_bytes: 6,
        })
        .expect("valid limits")
    }

    #[test]
    fn overlapping_vm_generations_reserve_before_replacement() {
        let account = account();
        let old = account.reserve_vm().expect("old VM");
        let candidate = account.reserve_vm().expect("reload candidate");
        let error = account.reserve_vm().expect_err("third VM is refused");
        assert_eq!(error.available, 0);
        assert_eq!(account.usage(), (8, 0));
        drop(candidate);
        assert!(account.reserve_vm().is_ok());
        drop(old);
    }

    #[test]
    fn callback_charge_survives_moves_and_releases_on_every_terminal_drop() {
        let account = account();
        let first = account.reserve_callback().expect("first callback");
        let second = account.reserve_callback().expect("second callback");
        assert!(account.reserve_callback().is_err());
        assert_eq!(account.usage(), (0, 6));
        let moved = Some(first);
        drop(second);
        assert_eq!(account.usage(), (0, 3));
        drop(moved);
        assert_eq!(account.usage(), (0, 0));
    }

    #[test]
    fn sized_callback_charges_refuse_at_the_exact_byte_boundary() {
        let account = account();
        assert!(account.reserve_callback_bytes(4).is_err());
        assert_eq!(account.usage(), (0, 0));
        let first = account.reserve_callback_bytes(2).unwrap();
        let second = account.reserve_callback_bytes(3).unwrap();
        let error = account.reserve_callback_bytes(2).unwrap_err();
        assert_eq!(error.available, 1);
        assert_eq!(account.usage(), (0, 5));
        let last = account.reserve_callback_bytes(1).unwrap();
        assert!(account.reserve_callback_bytes(1).is_err());
        assert_eq!(account.usage(), (0, 6));
        drop((first, second, last));
        assert_eq!(account.usage(), (0, 0));
    }

    #[test]
    fn sized_callback_charges_retain_more_items_than_full_allowances() {
        let account = account();
        let mut retained = Vec::new();
        for _ in 0..6 {
            retained.push(account.reserve_callback_bytes(1).unwrap());
        }
        assert!(
            retained.len()
                > account.limits().total_callback_bytes / account.limits().per_callback_bytes
        );
        assert_eq!(account.usage(), (0, 6));
        assert!(account.reserve_callback_bytes(1).is_err());
        drop(retained);
        assert_eq!(account.usage(), (0, 0));
    }

    #[test]
    fn invalid_limit_relationships_are_rejected() {
        assert_eq!(
            LuaMemoryAccount::new(LuaMemoryLimits {
                per_vm_bytes: 2,
                total_vm_bytes: 1,
                per_callback_bytes: 1,
                total_callback_bytes: 1,
            })
            .expect_err("invalid VM limits"),
            LuaMemoryLimitError::PerVmExceedsTotal
        );
    }
}
