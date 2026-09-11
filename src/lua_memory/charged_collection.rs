//! Collection-capacity charges for Vec and VecDeque of accounted values.
//!
//! Slot bytes belong to the collection's capacity charge, not to each entry.
//! Growth charges the new capacity while the old charge is still held, then
//! replaces it. Pop releases only the entry.

use std::alloc::{Layout, alloc};
use std::collections::VecDeque;
use std::mem::size_of;
use std::sync::Arc;

use super::{LuaCallbackCharge, LuaMemoryAccount, LuaMemoryCapacityError, LuaMemoryClass};

pub(crate) struct ChargedVecDeque<T> {
    buf: VecDeque<T>,
    capacity_charges: Vec<LuaCallbackCharge>,
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
        Self {
            buf: VecDeque::new(),
            capacity_charges: Vec::new(),
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
            self.capacity_charges.clear();
        }
        item
    }

    pub(crate) fn take(&mut self) -> Self {
        let capacity_charges = {
            #[cfg(test)]
            if self.take_without_charge {
                Vec::new()
            } else {
                std::mem::take(&mut self.capacity_charges)
            }
            #[cfg(not(test))]
            std::mem::take(&mut self.capacity_charges)
        };
        Self {
            buf: std::mem::take(&mut self.buf),
            capacity_charges,
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
        self.ensure_charged()?;
        if self.buf.len() < self.buf.capacity() {
            return Ok(());
        }
        self.grow()
    }

    fn ensure_charged(&mut self) -> Result<(), LuaMemoryCapacityError> {
        ensure_charges::<T>(
            &self.account,
            &mut self.capacity_charges,
            self.buf.capacity(),
        )
    }

    fn grow(&mut self) -> Result<(), LuaMemoryCapacityError> {
        let old_cap = self.buf.capacity();
        let new_cap = next_capacity(old_cap);
        let new_bytes = charged_bytes::<T>(new_cap)?;
        #[cfg(test)]
        let additional = new_cap.saturating_sub(self.buf.len());
        #[cfg(test)]
        if self.charge_after_grow {
            self.buf
                .try_reserve_exact(additional)
                .map_err(|_| reserve_failed::<T>(new_cap))?;
            return retain_grown_charges::<T>(
                &self.account,
                &mut self.capacity_charges,
                self.account.reserve_shared_callback_storage(new_bytes)?,
                new_cap,
                self.buf.capacity(),
            );
        }
        grow_exact(
            &self.account,
            &mut self.capacity_charges,
            new_cap,
            new_bytes,
            || replace_deque_exact(&mut self.buf, new_cap),
        )
    }

    #[cfg(any(test, feature = "allocation-oracle"))]
    pub(crate) fn charge_bytes(&self) -> usize {
        charge_sum(&self.capacity_charges)
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
    capacity_charges: Vec<LuaCallbackCharge>,
    account: Arc<LuaMemoryAccount>,
    #[cfg(test)]
    charge_after_grow: bool,
    #[cfg(test)]
    release_capacity_on_pop: bool,
    #[cfg(test)]
    skip_capacity_check: bool,
    #[cfg(test)]
    always_grow: bool,
}

impl<T> ChargedVec<T> {
    pub(crate) fn new(account: Arc<LuaMemoryAccount>) -> Self {
        Self {
            buf: Vec::new(),
            capacity_charges: Vec::new(),
            account,
            #[cfg(test)]
            charge_after_grow: false,
            #[cfg(test)]
            release_capacity_on_pop: false,
            #[cfg(test)]
            skip_capacity_check: false,
            #[cfg(test)]
            always_grow: false,
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
            self.capacity_charges.clear();
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
        self.ensure_charged()?;
        if self.buf.len() < self.buf.capacity() {
            return Ok(());
        }
        self.grow()
    }

    fn ensure_charged(&mut self) -> Result<(), LuaMemoryCapacityError> {
        ensure_charges::<T>(
            &self.account,
            &mut self.capacity_charges,
            self.buf.capacity(),
        )
    }

    fn grow(&mut self) -> Result<(), LuaMemoryCapacityError> {
        let old_cap = self.buf.capacity();
        let new_cap = next_capacity(old_cap);
        let new_bytes = charged_bytes::<T>(new_cap)?;
        #[cfg(test)]
        let additional = new_cap.saturating_sub(self.buf.len());
        #[cfg(test)]
        if self.charge_after_grow {
            self.buf
                .try_reserve_exact(additional)
                .map_err(|_| reserve_failed::<T>(new_cap))?;
            return retain_grown_charges::<T>(
                &self.account,
                &mut self.capacity_charges,
                self.account.reserve_shared_callback_storage(new_bytes)?,
                new_cap,
                self.buf.capacity(),
            );
        }
        grow_exact(
            &self.account,
            &mut self.capacity_charges,
            new_cap,
            new_bytes,
            || replace_vec_exact(&mut self.buf, new_cap),
        )
    }

    #[cfg(any(test, feature = "allocation-oracle"))]
    pub(crate) fn charge_bytes(&self) -> usize {
        charge_sum(&self.capacity_charges)
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
}

fn next_capacity(current: usize) -> usize {
    if current == 0 {
        1
    } else {
        current.saturating_mul(2)
    }
}

fn charge_sum(charges: &[LuaCallbackCharge]) -> usize {
    charges.iter().map(LuaCallbackCharge::bytes).sum()
}

fn ensure_charges<T>(
    account: &Arc<LuaMemoryAccount>,
    charges: &mut Vec<LuaCallbackCharge>,
    actual_cap: usize,
) -> Result<(), LuaMemoryCapacityError> {
    let needed = charged_bytes::<T>(actual_cap)?;
    let have = charge_sum(charges);
    if needed <= have {
        return Ok(());
    }
    charges.push(account.reserve_shared_callback_storage(needed - have)?);
    Ok(())
}

fn grow_exact<F>(
    account: &Arc<LuaMemoryAccount>,
    charges: &mut Vec<LuaCallbackCharge>,
    _new_cap: usize,
    new_bytes: usize,
    replace: F,
) -> Result<(), LuaMemoryCapacityError>
where
    F: FnOnce() -> Result<(), LuaMemoryCapacityError>,
{
    let new_charge = account.reserve_shared_callback_storage(new_bytes)?;
    replace()?;
    *charges = vec![new_charge];
    Ok(())
}

fn exact_vec<T>(cap: usize) -> Result<Vec<T>, LuaMemoryCapacityError> {
    if cap == 0 {
        return Ok(Vec::new());
    }
    if size_of::<T>() == 0 {
        return Err(reserve_failed::<T>(cap));
    }
    let layout = Layout::array::<T>(cap).map_err(|_| reserve_failed::<T>(cap))?;
    let ptr = unsafe { alloc(layout) };
    if ptr.is_null() {
        return Err(reserve_failed::<T>(cap));
    }
    Ok(unsafe { Vec::from_raw_parts(ptr.cast::<T>(), 0, cap) })
}

fn replace_vec_exact<T>(buf: &mut Vec<T>, new_cap: usize) -> Result<(), LuaMemoryCapacityError> {
    let next = exact_vec::<T>(new_cap)?;
    let mut old = std::mem::replace(buf, next);
    buf.append(&mut old);
    debug_assert_eq!(buf.capacity(), new_cap);
    Ok(())
}

fn replace_deque_exact<T>(
    buf: &mut VecDeque<T>,
    new_cap: usize,
) -> Result<(), LuaMemoryCapacityError> {
    let mut next = exact_vec::<T>(new_cap)?;
    next.extend(buf.drain(..));
    let old = std::mem::replace(buf, VecDeque::from(next));
    drop(old);
    debug_assert_eq!(buf.capacity(), new_cap);
    Ok(())
}

#[cfg(test)]
fn retain_grown_charges<T>(
    account: &Arc<LuaMemoryAccount>,
    charges: &mut Vec<LuaCallbackCharge>,
    new_charge: LuaCallbackCharge,
    new_cap: usize,
    actual: usize,
) -> Result<(), LuaMemoryCapacityError> {
    let mut next = vec![new_charge];
    if actual > new_cap {
        let extra = charged_bytes::<T>(actual - new_cap)?;
        match account.reserve_shared_callback_storage(extra) {
            Ok(charge) => next.push(charge),
            Err(error) => {
                *charges = next;
                return Err(error);
            }
        }
    } else if actual < new_cap {
        *charges = next;
        return Err(reserve_failed::<T>(new_cap));
    }
    *charges = next;
    Ok(())
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
}
