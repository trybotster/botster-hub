//! Allocation layouts from the pinned Rust 1.97 sources.
//! These records describe allocation requests. They do not construct channel storage.

use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, Mutex};

use super::LuaCallbackCharge;

#[allow(dead_code)]
#[repr(C, align(2))]
struct ArcInner<T> {
    strong: AtomicUsize,
    weak: AtomicUsize,
    data: T,
}

pub(crate) const fn arc_bytes<T>() -> usize {
    Layout::new::<ArcInner<T>>().size()
}

pub(crate) const fn lease_bytes() -> usize {
    arc_bytes::<LuaCallbackCharge>()
}

#[allow(dead_code)]
#[cfg_attr(any(target_arch = "x86_64", target_arch = "aarch64", target_arch = "powerpc64"), repr(align(128)))]
#[cfg_attr(any(target_arch = "arm", target_arch = "mips", target_arch = "mips32r6", target_arch = "mips64", target_arch = "mips64r6"), repr(align(32)))]
#[cfg_attr(target_arch = "s390x", repr(align(256)))]
#[cfg_attr(not(any(target_arch = "x86_64", target_arch = "aarch64", target_arch = "powerpc64", target_arch = "arm", target_arch = "mips", target_arch = "mips32r6", target_arch = "mips64", target_arch = "mips64r6", target_arch = "s390x")), repr(align(64)))]
struct CachePadded<T> {
    value: T,
}

#[allow(dead_code)]
struct SelectorEntry {
    operation: usize,
    packet: *mut (),
    context: Arc<()>,
}

#[allow(dead_code)]
struct Waker {
    selectors: Vec<SelectorEntry>,
    observers: Vec<SelectorEntry>,
}

#[allow(dead_code)]
struct SyncWaker {
    inner: Mutex<Waker>,
    is_empty: AtomicBool,
}

#[allow(dead_code)]
struct ArraySlot<T> {
    stamp: AtomicUsize,
    message: UnsafeCell<MaybeUninit<T>>,
}

#[allow(dead_code)]
struct ArrayChannel<T> {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    buffer: Box<[ArraySlot<T>]>,
    capacity: usize,
    one_lap: usize,
    mark_bit: usize,
    senders: SyncWaker,
    receivers: SyncWaker,
}

#[allow(dead_code)]
struct Counter<C> {
    senders: AtomicUsize,
    receivers: AtomicUsize,
    destroy: AtomicBool,
    channel: C,
}

/// The pthread mutex implementation allocates one native mutex on first use.
pub(crate) const fn lazy_mutex_bytes() -> usize {
    #[cfg(all(
        target_family = "unix",
        not(any(target_os = "linux", target_os = "android", target_os = "freebsd",
            target_os = "openbsd", target_os = "dragonfly", target_os = "fuchsia"))
    ))]
    {
        std::mem::size_of::<libc::pthread_mutex_t>()
    }
    #[cfg(not(all(
        target_family = "unix",
        not(any(target_os = "linux", target_os = "android", target_os = "freebsd",
            target_os = "openbsd", target_os = "dragonfly", target_os = "fuchsia"))
    )))]
    {
        0
    }
}

/// Storage for one single-send channel and its final-endpoint lease.
pub(crate) fn single_reply_bytes<T>(receiver_waits: bool) -> Option<usize> {
    let mutexes = if receiver_waits { 2usize } else { 1usize };
    // One receiver can register one selector. Vec's first allocation has four entries.
    let selectors = if receiver_waits {
        4usize.checked_mul(Layout::new::<SelectorEntry>().size())?
    } else {
        0
    };
    Layout::new::<Counter<ArrayChannel<T>>>().size()
        .checked_add(Layout::new::<ArraySlot<T>>().size())?
        .checked_add(mutexes.checked_mul(lazy_mutex_bytes())?)?
        .checked_add(selectors)?
        .checked_add(lease_bytes())
}

pub(crate) const fn lua_reference_bytes() -> usize {
    arc_bytes::<std::ffi::c_int>()
}
