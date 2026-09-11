//! Collection-capacity charges for Vec and VecDeque of accounted values.
//!
//! Slot bytes belong to the collection's capacity charge, not to each entry.
//! Growth charges the new capacity while the old charge is still held, then
//! replaces it. Pop releases only the entry.

use std::collections::VecDeque;
use std::mem::size_of;
use std::sync::{Arc, Mutex};

use super::{LuaCallbackCharge, LuaMemoryAccount, LuaMemoryCapacityError, LuaMemoryClass};

const PINNED_EXACT_GROWTH: &str =
    "pinned rustc 1.97.0 (2d8144b78) exact-growth contract: capacity must equal requested new_cap";

pub(crate) struct ChargedVecDeque<T> {
    buf: VecDeque<T>,
    capacity_charge: Option<LuaCallbackCharge>,
    account: Arc<LuaMemoryAccount>,
    #[cfg(test)]
    charge_after_grow: bool,
    #[cfg(test)]
    release_capacity_on_pop: bool,
    #[cfg(test)]
    skip_capacity_check: bool,
    #[cfg(test)]
    always_grow: bool,
    #[cfg(test)]
    take_without_charge: bool,
}

impl<T> ChargedVecDeque<T> {
    pub(crate) fn new(account: Arc<LuaMemoryAccount>) -> Self {
        const { assert!(size_of::<T>() > 0) };
        Self {
            buf: VecDeque::new(),
            capacity_charge: None,
            account,
            #[cfg(test)]
            charge_after_grow: false,
            #[cfg(test)]
            release_capacity_on_pop: false,
            #[cfg(test)]
            skip_capacity_check: false,
            #[cfg(test)]
            always_grow: false,
            #[cfg(test)]
            take_without_charge: false,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.buf.len()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub(crate) fn try_push_back(&mut self, item: T) -> Result<(), LuaMemoryCapacityError> {
        #[cfg(test)]
        if self.skip_capacity_check {
            self.buf.push_back(item);
            return Ok(());
        }
        self.prepare_push()?;
        self.buf.push_back(item);
        Ok(())
    }

    pub(crate) fn front(&self) -> Option<&T> {
        self.buf.front()
    }

    pub(crate) fn front_mut(&mut self) -> Option<&mut T> {
        self.buf.front_mut()
    }

    pub(crate) fn pop_front(&mut self) -> Option<T> {
        let item = self.buf.pop_front();
        #[cfg(test)]
        if self.release_capacity_on_pop {
            self.capacity_charge = None;
        }
        item
    }

    pub(crate) fn take(&mut self) -> Self {
        let capacity_charge = {
            #[cfg(test)]
            if self.take_without_charge {
                None
            } else {
                self.capacity_charge.take()
            }
            #[cfg(not(test))]
            self.capacity_charge.take()
        };
        Self {
            buf: std::mem::take(&mut self.buf),
            capacity_charge,
            account: Arc::clone(&self.account),
            #[cfg(test)]
            charge_after_grow: self.charge_after_grow,
            #[cfg(test)]
            release_capacity_on_pop: self.release_capacity_on_pop,
            #[cfg(test)]
            skip_capacity_check: self.skip_capacity_check,
            #[cfg(test)]
            always_grow: self.always_grow,
            #[cfg(test)]
            take_without_charge: self.take_without_charge,
        }
    }

    pub(crate) fn prepare_push(&mut self) -> Result<(), LuaMemoryCapacityError> {
        #[cfg(test)]
        if self.always_grow {
            return self.grow();
        }
        if self.buf.len() < self.buf.capacity() {
            return Ok(());
        }
        self.grow()
    }

    fn grow(&mut self) -> Result<(), LuaMemoryCapacityError> {
        grow_exact_std::<T, _>(
            &self.account,
            &mut self.capacity_charge,
            self.buf.len(),
            self.buf.capacity(),
            |additional| {
                self.buf
                    .try_reserve_exact(additional)
                    .map(|_| self.buf.capacity())
                    .map_err(|_| ())
            },
            #[cfg(test)]
            self.charge_after_grow,
        )
    }

    #[cfg(any(test, feature = "allocation-oracle"))]
    pub(crate) fn charge_bytes(&self) -> usize {
        charge_bytes(self.capacity_charge.as_ref())
    }

    #[cfg(test)]
    pub(crate) fn set_charge_after_grow(&mut self, enabled: bool) {
        self.charge_after_grow = enabled;
    }

    #[cfg(test)]
    pub(crate) fn set_release_capacity_on_pop(&mut self, enabled: bool) {
        self.release_capacity_on_pop = enabled;
    }

    #[cfg(test)]
    pub(crate) fn set_skip_capacity_check(&mut self, enabled: bool) {
        self.skip_capacity_check = enabled;
    }

    #[cfg(test)]
    pub(crate) fn set_always_grow(&mut self, enabled: bool) {
        self.always_grow = enabled;
    }

    #[cfg(test)]
    pub(crate) fn set_take_without_charge(&mut self, enabled: bool) {
        self.take_without_charge = enabled;
    }
}

pub(crate) struct ChargedVec<T> {
    buf: Vec<T>,
    capacity_charge: Option<LuaCallbackCharge>,
    account: Arc<LuaMemoryAccount>,
    reserved: usize,
    #[cfg(test)]
    charge_after_grow: bool,
    #[cfg(test)]
    release_capacity_on_pop: bool,
    #[cfg(test)]
    skip_capacity_check: bool,
    #[cfg(test)]
    always_grow: bool,
    #[cfg(test)]
    skip_reserved: bool,
}

impl<T> ChargedVec<T> {
    pub(crate) fn new(account: Arc<LuaMemoryAccount>) -> Self {
        const { assert!(size_of::<T>() > 0) };
        Self {
            buf: Vec::new(),
            capacity_charge: None,
            account,
            reserved: 0,
            #[cfg(test)]
            charge_after_grow: false,
            #[cfg(test)]
            release_capacity_on_pop: false,
            #[cfg(test)]
            skip_capacity_check: false,
            #[cfg(test)]
            always_grow: false,
            #[cfg(test)]
            skip_reserved: false,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.buf.len()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    pub(crate) fn try_push(&mut self, item: T) -> Result<(), LuaMemoryCapacityError> {
        #[cfg(test)]
        if self.skip_capacity_check {
            self.buf.push(item);
            return Ok(());
        }
        self.prepare_push()?;
        self.buf.push(item);
        Ok(())
    }

    pub(crate) fn swap_remove(&mut self, index: usize) -> T {
        let item = self.buf.swap_remove(index);
        #[cfg(test)]
        if self.release_capacity_on_pop {
            self.capacity_charge = None;
        }
        item
    }

    pub(crate) fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.buf.get_mut(index)
    }

    pub(crate) fn prepare_push(&mut self) -> Result<(), LuaMemoryCapacityError> {
        #[cfg(test)]
        if self.always_grow {
            return self.grow();
        }
        if self.buf.len() + self.reserved < self.buf.capacity() {
            return Ok(());
        }
        self.grow()
    }

    pub(crate) fn reserve_slot(&mut self) -> Result<(), LuaMemoryCapacityError> {
        self.prepare_push()?;
        #[cfg(test)]
        if self.skip_reserved {
            return Ok(());
        }
        self.reserved += 1;
        Ok(())
    }

    fn push_reserved(&mut self, item: T) {
        #[cfg(test)]
        if self.skip_reserved {
            self.buf.push(item);
            return;
        }
        assert!(self.reserved > 0);
        self.reserved -= 1;
        self.buf.push(item);
    }

    fn cancel_slot(&mut self) {
        if self.reserved > 0 {
            self.reserved -= 1;
        }
    }

    fn grow(&mut self) -> Result<(), LuaMemoryCapacityError> {
        grow_exact_std::<T, _>(
            &self.account,
            &mut self.capacity_charge,
            self.buf.len(),
            self.buf.capacity(),
            |additional| {
                self.buf
                    .try_reserve_exact(additional)
                    .map(|_| self.buf.capacity())
                    .map_err(|_| ())
            },
            #[cfg(test)]
            self.charge_after_grow,
        )
    }

    #[cfg(any(test, feature = "allocation-oracle"))]
    pub(crate) fn charge_bytes(&self) -> usize {
        charge_bytes(self.capacity_charge.as_ref())
    }

    #[cfg(test)]
    pub(crate) fn reserved(&self) -> usize {
        self.reserved
    }

    #[cfg(test)]
    pub(crate) fn set_charge_after_grow(&mut self, enabled: bool) {
        self.charge_after_grow = enabled;
    }

    #[cfg(test)]
    pub(crate) fn set_release_capacity_on_pop(&mut self, enabled: bool) {
        self.release_capacity_on_pop = enabled;
    }

    #[cfg(test)]
    pub(crate) fn set_skip_capacity_check(&mut self, enabled: bool) {
        self.skip_capacity_check = enabled;
    }

    #[cfg(test)]
    pub(crate) fn set_always_grow(&mut self, enabled: bool) {
        self.always_grow = enabled;
    }

    #[cfg(test)]
    pub(crate) fn set_skip_reserved(&mut self, enabled: bool) {
        self.skip_reserved = enabled;
    }
}

pub(crate) struct SlotReservation<'a, T> {
    collection: &'a Mutex<ChargedVec<T>>,
    armed: bool,
}

impl<'a, T> SlotReservation<'a, T> {
    pub(crate) fn try_reserve(
        collection: &'a Mutex<ChargedVec<T>>,
    ) -> Result<Self, LuaMemoryCapacityError> {
        let mut guard = collection.lock().map_err(|_| reserve_failed::<T>(0))?;
        guard.reserve_slot()?;
        drop(guard);
        Ok(Self {
            collection,
            armed: true,
        })
    }

    pub(crate) fn insert(mut self, item: T) {
        let mut guard = self
            .collection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.push_reserved(item);
        self.armed = false;
    }
}

impl<T> Drop for SlotReservation<'_, T> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut guard = self
            .collection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.cancel_slot();
        self.armed = false;
    }
}

fn grow_exact_std<T, F>(
    account: &Arc<LuaMemoryAccount>,
    capacity_charge: &mut Option<LuaCallbackCharge>,
    len: usize,
    old_cap: usize,
    reserve: F,
    #[cfg(test)] charge_after_grow: bool,
) -> Result<(), LuaMemoryCapacityError>
where
    F: FnOnce(usize) -> Result<usize, ()>,
{
    const { assert!(size_of::<T>() > 0) };
    let new_cap = next_capacity(old_cap);
    let additional = new_cap.saturating_sub(len);
    let new_bytes = charged_bytes::<T>(new_cap)?;
    #[cfg(test)]
    if charge_after_grow {
        let actual = reserve(additional).map_err(|_| reserve_failed::<T>(new_cap))?;
        assert_eq!(actual, new_cap, "{PINNED_EXACT_GROWTH}");
        *capacity_charge = Some(account.reserve_shared_callback_storage(new_bytes)?);
        return Ok(());
    }
    let new_charge = account.reserve_shared_callback_storage(new_bytes)?;
    let actual = reserve(additional).map_err(|_| reserve_failed::<T>(new_cap))?;
    assert_eq!(actual, new_cap, "{PINNED_EXACT_GROWTH}");
    *capacity_charge = Some(new_charge);
    Ok(())
}

fn next_capacity(current: usize) -> usize {
    if current == 0 {
        1
    } else {
        current.saturating_mul(2)
    }
}

#[cfg(any(test, feature = "allocation-oracle"))]
fn charge_bytes(charge: Option<&LuaCallbackCharge>) -> usize {
    charge.map(LuaCallbackCharge::bytes).unwrap_or(0)
}

fn charged_bytes<T>(capacity: usize) -> Result<usize, LuaMemoryCapacityError> {
    capacity
        .checked_mul(size_of::<T>())
        .ok_or(LuaMemoryCapacityError {
            class: LuaMemoryClass::Callback,
            requested: usize::MAX,
            available: 0,
        })
}

fn reserve_failed<T>(new_cap: usize) -> LuaMemoryCapacityError {
    LuaMemoryCapacityError {
        class: LuaMemoryClass::Callback,
        requested: size_of::<T>().saturating_mul(new_cap),
        available: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lua_memory::LuaMemoryLimits;

    fn account(total: usize) -> Arc<LuaMemoryAccount> {
        LuaMemoryAccount::new(LuaMemoryLimits {
            per_vm_bytes: total.max(1),
            total_vm_bytes: total.max(1),
            per_callback_bytes: total.max(1),
            total_callback_bytes: total.max(1),
        })
        .unwrap()
    }

    fn slot() -> usize {
        size_of::<u8>()
    }

    #[test]
    fn charge_tracks_logical_capacity_through_growth_steps() {
        let memory = account(16 * slot());
        let mut q = ChargedVecDeque::<u8>::new(Arc::clone(&memory));
        let mut v = ChargedVec::<u8>::new(Arc::clone(&memory));
        for cap in [1, 2, 4] {
            while q.len() < cap {
                q.try_push_back(0).unwrap();
            }
            while v.len() < cap {
                v.try_push(0).unwrap();
            }
            assert_eq!(q.capacity(), cap);
            assert_eq!(v.capacity(), cap);
            assert_eq!(q.charge_bytes(), cap * slot());
            assert_eq!(v.charge_bytes(), cap * slot());
        }
    }

    #[test]
    fn deque_growth_overlap_refuses_when_old_plus_new_does_not_fit() {
        let memory = account(2 * slot());
        let mut q = ChargedVecDeque::<u8>::new(Arc::clone(&memory));
        q.try_push_back(1).unwrap();
        assert_eq!(q.capacity(), 1);
        assert_eq!(q.charge_bytes(), slot());
        assert_eq!(memory.usage().1, slot());
        assert!(q.try_push_back(2).is_err());
        assert_eq!(q.len(), 1);
        assert_eq!(q.capacity(), 1);
        assert_eq!(memory.usage().1, slot());
        drop(q);
        let memory = account(1 * slot() + 2 * slot());
        let mut q = ChargedVecDeque::<u8>::new(Arc::clone(&memory));
        q.try_push_back(1).unwrap();
        q.try_push_back(2).unwrap();
        assert_eq!(q.capacity(), 2);
        assert_eq!(q.charge_bytes(), 2 * slot());
        assert_eq!(memory.usage().1, 2 * slot());
    }

    #[test]
    fn deque_growth_overlap_ablation_charges_after_grow() {
        let memory = account(2 * slot());
        let mut q = ChargedVecDeque::<u8>::new(Arc::clone(&memory));
        q.set_charge_after_grow(true);
        q.try_push_back(1).unwrap();
        assert!(q.try_push_back(2).is_err());
        assert_eq!(
            q.capacity(),
            2,
            "ablation reallocates before the charge so overlap is not held"
        );
        assert_eq!(q.len(), 1);
        assert_eq!(q.charge_bytes(), slot());
    }

    #[test]
    fn deque_drain_keeps_capacity_charge() {
        let memory = account(8 * slot());
        let mut q = ChargedVecDeque::<u8>::new(Arc::clone(&memory));
        q.try_push_back(1).unwrap();
        q.try_push_back(2).unwrap();
        assert_eq!(q.capacity(), 2);
        let _ = q.pop_front();
        let _ = q.pop_front();
        assert_eq!(q.len(), 0);
        assert_eq!(q.capacity(), 2);
        assert_eq!(q.charge_bytes(), 2 * slot());
        assert_eq!(memory.usage().1, 2 * slot());
    }

    #[test]
    fn deque_drain_ablation_releases_capacity_on_pop() {
        let memory = account(8 * slot());
        let mut q = ChargedVecDeque::<u8>::new(Arc::clone(&memory));
        q.set_release_capacity_on_pop(true);
        q.try_push_back(1).unwrap();
        q.try_push_back(2).unwrap();
        let _ = q.pop_front();
        let _ = q.pop_front();
        assert_eq!(q.charge_bytes(), 0);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn deque_reuse_does_not_recharge() {
        let memory = account(8 * slot());
        let mut q = ChargedVecDeque::<u8>::new(Arc::clone(&memory));
        q.try_push_back(1).unwrap();
        q.try_push_back(2).unwrap();
        let cap = q.capacity();
        let charge = q.charge_bytes();
        let _ = q.pop_front();
        let _ = q.pop_front();
        q.try_push_back(3).unwrap();
        q.try_push_back(4).unwrap();
        assert_eq!(q.capacity(), cap);
        assert_eq!(q.charge_bytes(), charge);
        assert_eq!(memory.usage().1, charge);
    }

    #[test]
    fn deque_saturation_refuses_when_total_is_full() {
        let memory = account(slot());
        let mut q = ChargedVecDeque::<u8>::new(Arc::clone(&memory));
        q.try_push_back(1).unwrap();
        assert!(q.try_push_back(2).is_err());
        assert_eq!(q.len(), 1);
        assert_eq!(memory.usage().1, slot());
    }

    #[test]
    fn deque_saturation_ablation_skips_capacity_check() {
        let memory = account(slot());
        let mut q = ChargedVecDeque::<u8>::new(Arc::clone(&memory));
        q.set_skip_capacity_check(true);
        q.try_push_back(1).unwrap();
        q.try_push_back(2).unwrap();
        assert_eq!(q.len(), 2);
        assert_eq!(q.charge_bytes(), 0);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn deque_reuse_ablation_always_grows() {
        let memory = account(8 * slot());
        let mut q = ChargedVecDeque::<u8>::new(Arc::clone(&memory));
        q.try_push_back(1).unwrap();
        q.try_push_back(2).unwrap();
        let _ = q.pop_front();
        let _ = q.pop_front();
        q.set_always_grow(true);
        q.try_push_back(3).unwrap();
        assert_eq!(q.capacity(), 4);
        assert_eq!(q.charge_bytes(), 4 * slot());
    }

    #[test]
    fn deque_take_releases_capacity_only_when_taken_buffer_drops() {
        let memory = account(8 * slot());
        let mut q = ChargedVecDeque::<u8>::new(Arc::clone(&memory));
        q.try_push_back(1).unwrap();
        q.try_push_back(2).unwrap();
        let taken = q.take();
        assert_eq!(q.len(), 0);
        assert_eq!(q.charge_bytes(), 0);
        assert_eq!(taken.charge_bytes(), 2 * slot());
        assert_eq!(memory.usage().1, 2 * slot());
        drop(taken);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn deque_take_ablation_leaves_charge_on_source() {
        let memory = account(8 * slot());
        let mut q = ChargedVecDeque::<u8>::new(Arc::clone(&memory));
        q.set_take_without_charge(true);
        q.try_push_back(1).unwrap();
        q.try_push_back(2).unwrap();
        let taken = q.take();
        assert_eq!(taken.charge_bytes(), 0);
        assert_eq!(q.charge_bytes(), 2 * slot());
        drop(taken);
        assert_eq!(memory.usage().1, 2 * slot());
        drop(q);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn vec_growth_overlap_refuses_when_old_plus_new_does_not_fit() {
        let memory = account(2 * slot());
        let mut v = ChargedVec::<u8>::new(Arc::clone(&memory));
        v.try_push(1).unwrap();
        assert_eq!(v.capacity(), 1);
        assert!(v.try_push(2).is_err());
        assert_eq!(v.len(), 1);
        assert_eq!(v.capacity(), 1);
        assert_eq!(memory.usage().1, slot());
        drop(v);
        let memory = account(1 * slot() + 2 * slot());
        let mut v = ChargedVec::<u8>::new(Arc::clone(&memory));
        v.try_push(1).unwrap();
        v.try_push(2).unwrap();
        assert_eq!(v.capacity(), 2);
        assert_eq!(v.charge_bytes(), 2 * slot());
        assert_eq!(memory.usage().1, 2 * slot());
    }

    #[test]
    fn vec_growth_overlap_ablation_charges_after_grow() {
        let memory = account(2 * slot());
        let mut v = ChargedVec::<u8>::new(Arc::clone(&memory));
        v.set_charge_after_grow(true);
        v.try_push(1).unwrap();
        assert!(v.try_push(2).is_err());
        assert_eq!(
            v.capacity(),
            2,
            "ablation reallocates before the charge so overlap is not held"
        );
        assert_eq!(v.len(), 1);
        assert_eq!(v.charge_bytes(), slot());
    }

    #[test]
    fn vec_drain_keeps_capacity_charge() {
        let memory = account(8 * slot());
        let mut v = ChargedVec::<u8>::new(Arc::clone(&memory));
        v.try_push(1).unwrap();
        v.try_push(2).unwrap();
        v.swap_remove(0);
        v.swap_remove(0);
        assert_eq!(v.len(), 0);
        assert_eq!(v.capacity(), 2);
        assert_eq!(v.charge_bytes(), 2 * slot());
        assert_eq!(memory.usage().1, 2 * slot());
    }

    #[test]
    fn vec_drain_ablation_releases_capacity_on_pop() {
        let memory = account(8 * slot());
        let mut v = ChargedVec::<u8>::new(Arc::clone(&memory));
        v.set_release_capacity_on_pop(true);
        v.try_push(1).unwrap();
        v.try_push(2).unwrap();
        v.swap_remove(0);
        v.swap_remove(0);
        assert_eq!(v.charge_bytes(), 0);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn vec_reuse_does_not_recharge() {
        let memory = account(8 * slot());
        let mut v = ChargedVec::<u8>::new(Arc::clone(&memory));
        v.try_push(1).unwrap();
        v.try_push(2).unwrap();
        let cap = v.capacity();
        let charge = v.charge_bytes();
        v.swap_remove(0);
        v.swap_remove(0);
        v.try_push(3).unwrap();
        v.try_push(4).unwrap();
        assert_eq!(v.capacity(), cap);
        assert_eq!(v.charge_bytes(), charge);
        assert_eq!(memory.usage().1, charge);
    }

    #[test]
    fn vec_reuse_ablation_always_grows() {
        let memory = account(8 * slot());
        let mut v = ChargedVec::<u8>::new(Arc::clone(&memory));
        v.try_push(1).unwrap();
        v.try_push(2).unwrap();
        v.swap_remove(0);
        v.swap_remove(0);
        v.set_always_grow(true);
        v.try_push(3).unwrap();
        assert_eq!(v.capacity(), 4);
        assert_eq!(v.charge_bytes(), 4 * slot());
    }

    #[test]
    fn vec_saturation_refuses_when_total_is_full() {
        let memory = account(slot());
        let mut v = ChargedVec::<u8>::new(Arc::clone(&memory));
        v.try_push(1).unwrap();
        assert!(v.try_push(2).is_err());
        assert_eq!(v.len(), 1);
        assert_eq!(memory.usage().1, slot());
    }

    #[test]
    fn vec_saturation_ablation_skips_capacity_check() {
        let memory = account(slot());
        let mut v = ChargedVec::<u8>::new(Arc::clone(&memory));
        v.set_skip_capacity_check(true);
        v.try_push(1).unwrap();
        v.try_push(2).unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v.charge_bytes(), 0);
        assert_eq!(memory.usage().1, 0);
    }

    #[test]
    fn exact_growth_contract_names_the_pinned_rustc() {
        assert_eq!(
            env!("BOTSTER_RUSTC_VERSION"),
            "rustc 1.97.0 (2d8144b78 2026-07-07)"
        );
    }

    #[test]
    fn slot_reservation_cancels_without_growth() {
        let memory = account(8 * slot());
        let collection = Mutex::new(ChargedVec::<u8>::new(Arc::clone(&memory)));
        {
            let mut guard = collection.lock().unwrap();
            guard.try_push(1).unwrap();
            guard.try_push(2).unwrap();
            guard.swap_remove(0);
        }
        let cap = collection.lock().unwrap().capacity();
        let reserved = SlotReservation::try_reserve(&collection).unwrap();
        assert_eq!(collection.lock().unwrap().reserved(), 1);
        drop(reserved);
        assert_eq!(collection.lock().unwrap().reserved(), 0);
        let reserved = SlotReservation::try_reserve(&collection).unwrap();
        assert_eq!(collection.lock().unwrap().capacity(), cap);
        reserved.insert(3);
        assert_eq!(collection.lock().unwrap().len(), 2);
        assert_eq!(collection.lock().unwrap().reserved(), 0);
    }

    fn race_one_free_slot(collection: &Mutex<ChargedVec<u8>>) -> usize {
        let accepted = std::sync::atomic::AtomicUsize::new(0);
        let start = std::sync::Barrier::new(2);
        let reserved = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            for _ in 0..2 {
                scope.spawn(|| {
                    start.wait();
                    let slot = SlotReservation::try_reserve(collection);
                    reserved.wait();
                    if let Ok(slot) = slot {
                        accepted.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                        slot.insert(9);
                    }
                });
            }
        });
        accepted.load(std::sync::atomic::Ordering::Acquire)
    }

    #[test]
    fn two_threads_one_free_slot_refuses_one_before_insert() {
        let memory = account(3 * slot());
        let collection = Mutex::new(ChargedVec::<u8>::new(Arc::clone(&memory)));
        {
            let mut guard = collection.lock().unwrap();
            guard.try_push(1).unwrap();
            guard.try_push(2).unwrap();
            guard.swap_remove(0);
        }
        let leftover = memory.limits().total_callback_bytes - memory.usage().1;
        let _hold = memory.reserve_shared_callback_storage(leftover).unwrap();
        assert_eq!(race_one_free_slot(&collection), 1);
        assert_eq!(collection.lock().unwrap().len(), 2);
        assert_eq!(collection.lock().unwrap().reserved(), 0);
    }

    #[test]
    fn two_threads_one_free_slot_ablation_skips_reserved() {
        let memory = account(3 * slot());
        let collection = Mutex::new(ChargedVec::<u8>::new(Arc::clone(&memory)));
        {
            let mut guard = collection.lock().unwrap();
            guard.try_push(1).unwrap();
            guard.try_push(2).unwrap();
            guard.swap_remove(0);
            guard.set_skip_reserved(true);
        }
        let leftover = memory.limits().total_callback_bytes - memory.usage().1;
        let _hold = memory.reserve_shared_callback_storage(leftover).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert_eq!(race_one_free_slot(&collection), 1);
            assert_eq!(collection.lock().unwrap().len(), 2);
        }));
        assert!(
            result.is_err(),
            "skip-reserved ablation must fail the race helper"
        );
    }
}
