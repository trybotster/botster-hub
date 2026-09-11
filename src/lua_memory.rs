//! Hub-owned admission for Lua states and Rust values crossing the Lua ABI.
//!
//! This module deliberately contains no production limit values. The host must
//! construct one account with explicit policy before using the bounded loader.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) mod layout;

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

    /// Admit one complete callback allowance before dividing its ownership.
    pub(crate) fn reserve_callback_total(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<LuaCallbackCharge, LuaCallbackAdmissionError> {
        if bytes > self.limits.per_callback_bytes {
            return Err(LuaCallbackAdmissionError::Quota);
        }
        reserve(
            &self.callback_bytes,
            bytes,
            self.limits.total_callback_bytes,
            LuaMemoryClass::Callback,
        )
        .map_err(LuaCallbackAdmissionError::Capacity)?;
        Ok(LuaCallbackCharge {
            account: Arc::clone(self),
            bytes,
        })
    }

    /// Reserve only instance-owned storage that outlives individual callbacks.
    /// Callback-owned storage must use aggregate callback admission instead.
    #[allow(dead_code)] // paused hook-error funding; keep the shared-storage interface
    pub(crate) fn reserve_shared_callback_storage(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<LuaCallbackCharge, LuaMemoryCapacityError> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LuaCallbackAdmissionError {
    Quota,
    Capacity(LuaMemoryCapacityError),
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

impl LuaCallbackCharge {
    /// Transfer disjoint bytes without changing the shared account's usage.
    pub(crate) fn split(&mut self, bytes: usize) -> Option<Self> {
        if bytes > self.bytes {
            return None;
        }
        self.bytes -= bytes;
        Some(Self {
            account: Arc::clone(&self.account),
            bytes,
        })
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
}

/// Shared storage keeps its charge until the last endpoint releases the storage.
#[derive(Debug)]
pub(crate) struct LuaCallbackStorageLease {
    charge: Option<Arc<LuaCallbackCharge>>,
}

impl LuaCallbackStorageLease {
    pub(crate) fn new(charge: LuaCallbackCharge) -> Self {
        Self {
            charge: Some(Arc::new(charge)),
        }
    }
}

impl Clone for LuaCallbackStorageLease {
    fn clone(&self) -> Self {
        Self {
            charge: self.charge.clone(),
        }
    }
}

impl Drop for LuaCallbackStorageLease {
    fn drop(&mut self) {
        if let Some(charge) = self.charge.take() {
            // No weak handle escapes. Free the Arc allocation before the charge.
            drop(Arc::into_inner(charge));
        }
    }
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
    fn storage_lease_keeps_the_charge_until_the_last_endpoint_drops() {
        let account = account();
        let sender = LuaCallbackStorageLease::new(account.reserve_callback().unwrap());
        let receiver = sender.clone();
        let retained_sender = sender.clone();
        drop(sender);
        drop(receiver);
        assert_eq!(account.usage(), (0, 3));
        drop(retained_sender);
        assert_eq!(account.usage(), (0, 0));
    }

    #[test]
    fn storage_lease_releases_the_charge_after_concurrent_endpoint_drops() {
        let account = account();
        let sender = LuaCallbackStorageLease::new(account.reserve_callback().unwrap());
        let receiver = sender.clone();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let other_barrier = Arc::clone(&barrier);
        let thread = std::thread::spawn(move || {
            other_barrier.wait();
            drop(sender);
        });
        barrier.wait();
        drop(receiver);
        thread.join().unwrap();
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

    #[test]
    fn callback_total_checks_quota_before_shared_capacity() {
        let account = account();
        assert_eq!(
            account.reserve_callback_total(4).unwrap_err(),
            LuaCallbackAdmissionError::Quota
        );
        assert_eq!(account.usage(), (0, 0));
        let first = account.reserve_callback_total(3).unwrap();
        let second = account.reserve_callback_total(3).unwrap();
        assert_eq!(
            account.reserve_callback_total(4).unwrap_err(),
            LuaCallbackAdmissionError::Quota
        );
        assert_eq!(
            account.reserve_callback_total(1).unwrap_err(),
            LuaCallbackAdmissionError::Capacity(LuaMemoryCapacityError {
                class: LuaMemoryClass::Callback,
                requested: 1,
                available: 0,
            })
        );
        assert_eq!(account.usage(), (0, 6));
        drop((first, second));
        assert_eq!(account.usage(), (0, 0));
    }

    #[test]
    fn callback_split_preserves_usage_and_releases_each_piece() {
        for original_first in [false, true] {
            let account = account();
            let mut original = account.reserve_callback_total(3).unwrap();
            let piece = original.split(1).unwrap();
            assert!(Arc::ptr_eq(&original.account, &piece.account));
            assert_eq!((original.bytes, piece.bytes), (2, 1));
            assert_eq!(account.usage(), (0, 3));
            if original_first {
                drop(original);
                assert_eq!(account.usage(), (0, 1));
                drop(piece);
            } else {
                drop(piece);
                assert_eq!(account.usage(), (0, 2));
                drop(original);
            }
            assert_eq!(account.usage(), (0, 0));
        }
    }

    #[test]
    fn callback_split_refuses_excess_without_changing_ownership() {
        let account = account();
        let mut original = account.reserve_callback_total(3).unwrap();
        assert!(original.split(4).is_none());
        assert_eq!(original.bytes, 3);
        assert_eq!(account.usage(), (0, 3));
        drop(original);
        assert_eq!(account.usage(), (0, 0));
    }

    #[test]
    fn callback_split_handles_zero_and_the_full_remaining_charge() {
        for original_first in [false, true] {
            let account = account();
            let mut original = account.reserve_callback_total(3).unwrap();
            let zero = original.split(0).unwrap();
            assert_eq!((original.bytes, zero.bytes), (3, 0));
            drop(zero);
            assert_eq!(account.usage(), (0, 3));
            let full = original.split(3).unwrap();
            assert_eq!((original.bytes, full.bytes), (0, 3));
            assert!(original.split(1).is_none());
            assert_eq!(account.usage(), (0, 3));
            if original_first {
                drop(original);
                assert_eq!(account.usage(), (0, 3));
                drop(full);
            } else {
                drop(full);
                assert_eq!(account.usage(), (0, 0));
                drop(original);
            }
            assert_eq!(account.usage(), (0, 0));
        }
    }

    #[test]
    fn callback_split_piece_survives_until_the_last_storage_endpoint() {
        let account = account();
        let mut original = account.reserve_callback_total(3).unwrap();
        let sender = LuaCallbackStorageLease::new(original.split(2).unwrap());
        let receiver = sender.clone();
        assert_eq!(account.usage(), (0, 3));
        drop(original);
        assert_eq!(account.usage(), (0, 2));
        drop(sender);
        assert_eq!(account.usage(), (0, 2));
        drop(receiver);
        assert_eq!(account.usage(), (0, 0));
    }

    #[test]
    fn shared_callback_storage_uses_total_capacity_without_callback_quota() {
        let account = account();
        let storage = account.reserve_shared_callback_storage(4).unwrap();
        assert_eq!(account.usage(), (0, 4));
        assert_eq!(
            account.reserve_callback_total(4).unwrap_err(),
            LuaCallbackAdmissionError::Quota
        );
        assert_eq!(
            account.reserve_shared_callback_storage(3).unwrap_err(),
            LuaMemoryCapacityError {
                class: LuaMemoryClass::Callback,
                requested: 3,
                available: 2,
            }
        );
        assert_eq!(account.usage(), (0, 4));
        let callback = account.reserve_callback_total(2).unwrap();
        assert_eq!(account.usage(), (0, 6));
        assert!(account.reserve_shared_callback_storage(1).is_err());
        drop(callback);
        assert_eq!(account.usage(), (0, 4));
        drop(storage);
        assert_eq!(account.usage(), (0, 0));
    }

    #[test]
    fn shared_callback_storage_releases_after_the_final_owner() {
        let account = account();
        let owner =
            LuaCallbackStorageLease::new(account.reserve_shared_callback_storage(6).unwrap());
        let retained = owner.clone();
        drop(owner);
        assert_eq!(account.usage(), (0, 6));
        assert!(account.reserve_shared_callback_storage(1).is_err());
        drop(retained);
        assert_eq!(account.usage(), (0, 0));
        let replacement = account.reserve_shared_callback_storage(6).unwrap();
        assert_eq!(account.usage(), (0, 6));
        drop(replacement);
        assert_eq!(account.usage(), (0, 0));
    }
}
