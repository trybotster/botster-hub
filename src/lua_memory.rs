//! Hub-owned admission for Lua states and Rust values crossing the Lua ABI.
//!
//! This module deliberately contains no production limit values. The host must
//! construct one account with explicit policy before using the bounded loader.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) mod charged_collection;
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
            growth: ChargeGrowth::Open {
                ceiling: self.limits.per_callback_bytes,
            },
        })
    }

    /// Reserve only instance-owned storage that outlives individual callbacks.
    /// Callback-owned storage must use aggregate callback admission instead.
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
            growth: ChargeGrowth::Sealed,
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
            growth: ChargeGrowth::Sealed,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LuaCallbackGrowthError {
    Sealed,
    Quota,
    Capacity(LuaMemoryCapacityError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChargeGrowth {
    Open { ceiling: usize },
    Sealed,
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
    growth: ChargeGrowth,
}

impl LuaCallbackCharge {
    /// Admit bytes before allocation, within the parent's remaining growth ceiling.
    /// Callers must not admit another parent for the same live operation.
    pub(crate) fn grow(&mut self, additional: usize) -> Result<(), LuaCallbackGrowthError> {
        let ChargeGrowth::Open { ceiling } = self.growth else {
            return Err(LuaCallbackGrowthError::Sealed);
        };
        let next = self
            .bytes
            .checked_add(additional)
            .filter(|next| *next <= ceiling)
            .ok_or(LuaCallbackGrowthError::Quota)?;
        reserve(
            &self.account.callback_bytes,
            additional,
            self.account.limits.total_callback_bytes,
            LuaMemoryClass::Callback,
        )
        .map_err(LuaCallbackGrowthError::Capacity)?;
        self.bytes = next;
        Ok(())
    }

    /// Release bytes after their payload is destroyed. Preserve growth authority.
    /// Return false without mutation if the requested size is larger.
    pub(crate) fn shrink_to(&mut self, next_bytes: usize) -> bool {
        let Some(released) = self.bytes.checked_sub(next_bytes) else {
            return false;
        };
        self.account
            .callback_bytes
            .fetch_sub(released, Ordering::AcqRel);
        self.bytes = next_bytes;
        true
    }

    /// Transfer a fixed allowance and permanently deduct it from the growth ceiling.
    /// The child is sealed. Dropping it does not restore the parent's ceiling.
    /// Return None without mutation for a sealed parent or insufficient allowance.
    pub(crate) fn split_fixed(&mut self, bytes: usize) -> Option<Self> {
        let ChargeGrowth::Open { ceiling } = self.growth else {
            return None;
        };
        let remaining_bytes = self.bytes.checked_sub(bytes)?;
        let remaining_ceiling = ceiling.checked_sub(bytes)?;
        self.bytes = remaining_bytes;
        self.growth = ChargeGrowth::Open {
            ceiling: remaining_ceiling,
        };
        Some(Self {
            account: Arc::clone(&self.account),
            bytes,
            growth: ChargeGrowth::Sealed,
        })
    }

    /// Transfer disjoint bytes without changing the shared account's usage.
    /// Every successful split seals both outputs, including a zero-byte split.
    pub(crate) fn split(&mut self, bytes: usize) -> Option<Self> {
        if bytes > self.bytes {
            return None;
        }
        self.bytes -= bytes;
        self.growth = ChargeGrowth::Sealed;
        Some(Self {
            account: Arc::clone(&self.account),
            bytes,
            growth: ChargeGrowth::Sealed,
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
    pub(crate) fn new(mut charge: LuaCallbackCharge) -> Self {
        charge.growth = ChargeGrowth::Sealed;
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
mod fixed_deduction_tests {
    use super::*;

    fn account() -> Arc<LuaMemoryAccount> {
        LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes: 10,
            total_callback_bytes: 20,
        })
        .unwrap()
    }

    #[test]
    fn multiple_fixed_partitions_preserve_the_exact_remaining_ceiling() {
        let memory = account();
        let mut parent = memory.reserve_callback_total(8).unwrap();
        let mut first = parent.split_fixed(2).unwrap();
        let mut second = parent.split_fixed(3).unwrap();
        assert_eq!(parent.bytes(), 3);
        assert_eq!(parent.growth, ChargeGrowth::Open { ceiling: 5 });
        assert_eq!(memory.usage().1, 8);
        parent.grow(2).unwrap();
        assert_eq!(parent.bytes() + first.bytes() + second.bytes(), 10);
        assert_eq!(memory.usage().1, 10);
        assert_eq!(parent.grow(1), Err(LuaCallbackGrowthError::Quota));
        assert_eq!(first.grow(1), Err(LuaCallbackGrowthError::Sealed));
        assert_eq!(second.grow(1), Err(LuaCallbackGrowthError::Sealed));
        assert!(first.split_fixed(0).is_none());
        assert!(second.split_fixed(1).is_none());
        drop((parent, first, second));
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn zero_fixed_partition_preserves_growth_but_seals_its_child() {
        let memory = account();
        let mut parent = memory.reserve_callback_total(0).unwrap();
        let mut child = parent.split_fixed(0).unwrap();
        assert_eq!(parent.growth, ChargeGrowth::Open { ceiling: 10 });
        assert_eq!(child.bytes(), 0);
        assert_eq!(child.grow(0), Err(LuaCallbackGrowthError::Sealed));
        assert!(child.split_fixed(0).is_none());
        parent.grow(10).unwrap();
        drop(child);
        assert_eq!(memory.usage().1, 10);
        assert_eq!(parent.grow(1), Err(LuaCallbackGrowthError::Quota));
        drop(parent);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn fixed_partition_refusals_preserve_bytes_ceiling_and_account() {
        let memory = account();
        let mut parent = memory.reserve_callback_total(4).unwrap();
        for requested in [5, usize::MAX] {
            assert!(parent.split_fixed(requested).is_none());
            assert_eq!(parent.bytes(), 4);
            assert_eq!(parent.growth, ChargeGrowth::Open { ceiling: 10 });
            assert_eq!(memory.usage().1, 4);
        }
        parent.grow(1).unwrap();
        let mut child = parent.split(2).unwrap();
        for charge in [&mut parent, &mut child] {
            let before = charge.bytes();
            for requested in [0, 1, usize::MAX] {
                assert!(charge.split_fixed(requested).is_none());
                assert_eq!(charge.bytes(), before);
                assert_eq!(charge.growth, ChargeGrowth::Sealed);
                assert_eq!(memory.usage().1, 5);
            }
        }
        drop((parent, child));
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn fixed_descendant_destruction_never_restores_the_parent_ceiling() {
        for child_first in [false, true] {
            let memory = account();
            let mut parent = memory.reserve_callback_total(7).unwrap();
            let mut child = parent.split_fixed(4).unwrap();
            let mut grandchild = child.split(2).unwrap();
            assert_eq!(grandchild.grow(1), Err(LuaCallbackGrowthError::Sealed));
            assert!(grandchild.split_fixed(0).is_none());
            if child_first {
                drop(child);
                assert_eq!(memory.usage().1, 5);
                drop(grandchild);
            } else {
                drop(grandchild);
                assert_eq!(memory.usage().1, 5);
                drop(child);
            }
            assert_eq!(memory.usage().1, 3);
            assert_eq!(parent.growth, ChargeGrowth::Open { ceiling: 6 });
            parent.grow(3).unwrap();
            assert_eq!(parent.grow(1), Err(LuaCallbackGrowthError::Quota));
            assert_eq!(memory.usage().1, 6);
            drop(parent);
            assert_eq!(memory.usage().1, 0);
        }
    }

    #[test]
    fn fixed_deduction_survives_shrink_regrow_and_aggregate_refusal() {
        let memory = account();
        let mut parent = memory.reserve_callback_total(10).unwrap();
        let mut fixed = parent.split_fixed(3).unwrap();
        assert!(parent.shrink_to(2));
        assert!(fixed.shrink_to(1));
        assert_eq!(memory.usage().1, 3);
        let other = memory.reserve_shared_callback_storage(17).unwrap();
        assert!(matches!(
            parent.grow(5),
            Err(LuaCallbackGrowthError::Capacity(_))
        ));
        assert_eq!(parent.bytes(), 2);
        assert_eq!(parent.growth, ChargeGrowth::Open { ceiling: 7 });
        assert_eq!(memory.usage().1, 20);
        drop(other);
        parent.grow(5).unwrap();
        assert_eq!(memory.usage().1, 8);
        drop(fixed);
        assert_eq!(parent.grow(1), Err(LuaCallbackGrowthError::Quota));
        assert!(parent.shrink_to(0));
        parent.grow(7).unwrap();
        drop(parent);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn terminal_split_and_lease_seal_a_parent_after_fixed_partition() {
        let memory = account();
        let mut parent = memory.reserve_callback_total(8).unwrap();
        let mut fixed = parent.split_fixed(3).unwrap();
        let mut terminal = parent.split(2).unwrap();
        for charge in [&mut parent, &mut fixed, &mut terminal] {
            assert_eq!(charge.grow(0), Err(LuaCallbackGrowthError::Sealed));
            assert!(charge.split_fixed(0).is_none());
        }
        drop((parent, fixed, terminal));
        assert_eq!(memory.usage().1, 0);
        let mut parent = memory.reserve_callback_total(10).unwrap();
        let fixed = parent.split_fixed(10).unwrap();
        parent.grow(0).unwrap();
        assert_eq!(parent.grow(1), Err(LuaCallbackGrowthError::Quota));
        let mut lease = LuaCallbackStorageLease::new(parent);
        let parent = Arc::get_mut(lease.charge.as_mut().unwrap()).unwrap();
        assert!(parent.split_fixed(0).is_none());
        assert_eq!(parent.grow(0), Err(LuaCallbackGrowthError::Sealed));
        drop(fixed);
        drop(lease);
        assert_eq!(memory.usage().1, 0);
    }
}

#[cfg(test)]
mod charge_phase_tests {
    use super::*;

    fn account(per_callback_bytes: usize, total_callback_bytes: usize) -> Arc<LuaMemoryAccount> {
        LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: 1,
            total_vm_bytes: 1,
            per_callback_bytes,
            total_callback_bytes,
        })
        .unwrap()
    }

    #[test]
    fn split_seals_parent_child_and_grandchild_in_every_drop_order() {
        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            let memory = account(8, 16);
            let mut parent = memory.reserve_callback_total(6).unwrap();
            let mut child = parent.split(2).unwrap();
            let grandchild = child.split(1).unwrap();
            let mut charges = [Some(parent), Some(child), Some(grandchild)];
            for charge in charges.iter_mut().flatten() {
                assert_eq!(charge.grow(1), Err(LuaCallbackGrowthError::Sealed));
                assert_eq!(charge.grow(0), Err(LuaCallbackGrowthError::Sealed));
            }
            assert_eq!(memory.usage().1, 6);
            let mut remaining = 6;
            for index in order {
                let charge = charges[index].take().unwrap();
                remaining -= charge.bytes();
                drop(charge);
                assert_eq!(memory.usage().1, remaining);
                for survivor in charges.iter_mut().flatten() {
                    assert_eq!(survivor.grow(1), Err(LuaCallbackGrowthError::Sealed));
                }
            }
        }
    }

    #[test]
    fn failed_split_preserves_open_growth() {
        let memory = account(8, 16);
        let mut parent = memory.reserve_callback_total(3).unwrap();
        assert!(parent.split(4).is_none());
        assert_eq!(parent.bytes(), 3);
        assert_eq!(memory.usage().1, 3);
        parent.grow(2).unwrap();
        assert_eq!(parent.bytes(), 5);
        assert_eq!(memory.usage().1, 5);
        drop(parent);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn zero_and_full_splits_seal_both_outputs() {
        for bytes in [0, 3] {
            for parent_first in [false, true] {
                let memory = account(8, 16);
                let mut parent = memory.reserve_callback_total(3).unwrap();
                let mut child = parent.split(bytes).unwrap();
                assert_eq!((parent.bytes(), child.bytes()), (3 - bytes, bytes));
                assert_eq!(parent.grow(1), Err(LuaCallbackGrowthError::Sealed));
                assert_eq!(child.grow(1), Err(LuaCallbackGrowthError::Sealed));
                assert_eq!(memory.usage().1, 3);
                if parent_first {
                    drop(parent);
                    assert_eq!(memory.usage().1, bytes);
                    assert_eq!(child.grow(1), Err(LuaCallbackGrowthError::Sealed));
                    drop(child);
                } else {
                    drop(child);
                    assert_eq!(memory.usage().1, 3 - bytes);
                    assert_eq!(parent.grow(1), Err(LuaCallbackGrowthError::Sealed));
                    drop(parent);
                }
                assert_eq!(memory.usage().1, 0);
            }
        }
    }

    #[test]
    fn shrink_preserves_open_state_across_phase_growth() {
        let memory = account(8, 16);
        let mut charge = memory.reserve_callback_total(5).unwrap();
        for (retained, additional, expected) in [(3, 4, 7), (4, 2, 6), (0, 8, 8)] {
            assert!(charge.shrink_to(retained));
            assert_eq!(memory.usage().1, retained);
            assert!(charge.shrink_to(retained));
            assert!(!charge.shrink_to(retained + 1));
            assert_eq!(charge.bytes(), retained);
            assert_eq!(memory.usage().1, retained);
            charge.grow(additional).unwrap();
            assert_eq!(charge.bytes(), expected);
            assert_eq!(memory.usage().1, expected);
        }
        charge.grow(0).unwrap();
        drop(charge);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn shrink_does_not_open_a_sealed_charge_or_release_its_sibling() {
        let memory = account(8, 16);
        let mut parent = memory.reserve_callback_total(6).unwrap();
        let mut child = parent.split(3).unwrap();
        assert!(child.shrink_to(1));
        assert_eq!((parent.bytes(), child.bytes()), (3, 1));
        assert_eq!(memory.usage().1, 4);
        assert_eq!(child.grow(1), Err(LuaCallbackGrowthError::Sealed));
        assert_eq!(parent.grow(1), Err(LuaCallbackGrowthError::Sealed));
        assert!(child.shrink_to(0));
        drop(child);
        assert_eq!(memory.usage().1, 3);
        drop(parent);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn grow_refuses_quota_and_capacity_without_mutation_then_retries() {
        let memory = account(8, 10);
        let mut charge = memory.reserve_callback_total(4).unwrap();
        let other = memory.reserve_callback_total(6).unwrap();
        assert_eq!(charge.grow(5), Err(LuaCallbackGrowthError::Quota));
        assert_eq!(
            charge.grow(1),
            Err(LuaCallbackGrowthError::Capacity(LuaMemoryCapacityError {
                class: LuaMemoryClass::Callback,
                requested: 1,
                available: 0,
            }))
        );
        assert_eq!(charge.bytes(), 4);
        assert_eq!(memory.usage().1, 10);
        drop(other);
        charge.grow(4).unwrap();
        assert_eq!(charge.bytes(), 8);
        assert_eq!(memory.usage().1, 8);
        drop(charge);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn grow_refuses_arithmetic_overflow_before_account_mutation() {
        let memory = account(usize::MAX, usize::MAX);
        let mut charge = memory.reserve_callback_total(1).unwrap();
        assert_eq!(charge.grow(usize::MAX), Err(LuaCallbackGrowthError::Quota));
        assert_eq!(charge.bytes(), 1);
        assert_eq!(memory.usage().1, 1);
        charge.grow(1).unwrap();
        assert_eq!(memory.usage().1, 2);
        drop(charge);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn only_total_admission_opens_growth_and_lease_sharing_seals_it() {
        let memory = account(8, 32);
        for mut charge in [
            memory.reserve_callback().unwrap(),
            memory.reserve_callback_bytes(1).unwrap(),
            memory.reserve_shared_callback_storage(1).unwrap(),
        ] {
            assert_eq!(charge.grow(0), Err(LuaCallbackGrowthError::Sealed));
            assert_eq!(charge.grow(1), Err(LuaCallbackGrowthError::Sealed));
        }
        assert_eq!(memory.usage().1, 0);
        let mut charge = memory.reserve_callback_total(0).unwrap();
        charge.grow(0).unwrap();
        charge.grow(2).unwrap();
        let mut lease = LuaCallbackStorageLease::new(charge);
        let shared = lease.clone();
        assert_eq!(lease.charge.as_ref().unwrap().growth, ChargeGrowth::Sealed);
        drop(shared);
        let charge = Arc::get_mut(lease.charge.as_mut().unwrap()).unwrap();
        assert_eq!(charge.grow(1), Err(LuaCallbackGrowthError::Sealed));
        assert_eq!(memory.usage().1, 2);
        drop(lease);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn payload_destruction_precedes_phase_reduction() {
        struct Payload(Arc<LuaMemoryAccount>);
        impl Drop for Payload {
            fn drop(&mut self) {
                assert_eq!(self.0.usage().1, 5);
            }
        }
        let memory = account(8, 16);
        let mut charge = memory.reserve_callback_total(5).unwrap();
        let payload = Payload(Arc::clone(&memory));
        drop(payload);
        assert!(charge.shrink_to(2));
        assert_eq!(memory.usage().1, 2);
        charge.grow(4).unwrap();
        drop(charge);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn lease_layout_includes_charge_state_and_arc_counters() {
        let charge = std::alloc::Layout::new::<LuaCallbackCharge>();
        let counters = std::alloc::Layout::array::<AtomicUsize>(2).unwrap();
        let (inner, _) = counters.extend(charge).unwrap();
        assert_eq!(layout::lease_bytes(), inner.pad_to_align().size());
        assert!(layout::lease_bytes() >= charge.size() + counters.size());
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
