//! Layout.size() versus charged collection capacity on rustc 1.97.0.

use std::alloc::{GlobalAlloc, Layout, System};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use botster_hub::test_internals::charged_collection::{InflightQueue, PendingQueue};

struct Recorder;

const BUFFER_BYTES: usize = 64;

static RECORD: AtomicBool = AtomicBool::new(false);
static MAX_LAYOUT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Recorder {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if RECORD.load(Ordering::Acquire) && layout.size() >= BUFFER_BYTES {
            MAX_LAYOUT.fetch_max(layout.size(), Ordering::AcqRel);
            let live = LIVE.fetch_add(layout.size(), Ordering::AcqRel) + layout.size();
            PEAK.fetch_max(live, Ordering::AcqRel);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if RECORD.load(Ordering::Acquire) && layout.size() >= BUFFER_BYTES {
            LIVE.fetch_sub(layout.size(), Ordering::AcqRel);
        }
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Recorder = Recorder;

fn grow_pending() -> Result<(), String> {
    LIVE.store(0, Ordering::Release);
    let slot = PendingQueue::slot();
    let mut q = PendingQueue::new();
    for cap in [1usize, 2, 4, 8] {
        MAX_LAYOUT.store(0, Ordering::Release);
        PEAK.store(LIVE.load(Ordering::Acquire), Ordering::Release);
        let old_charge = q.charge_bytes();
        RECORD.store(true, Ordering::Release);
        while q.capacity() < cap {
            q.try_push_uninit()?;
        }
        RECORD.store(false, Ordering::Release);
        let layout = MAX_LAYOUT.load(Ordering::Acquire);
        let charge = q.charge_bytes();
        let peak = PEAK.load(Ordering::Acquire);
        println!(
            "pending cap={} max_layout={} charge={} peak={} size_of*cap={}",
            q.capacity(),
            layout,
            charge,
            peak,
            slot * cap
        );
        if q.capacity() != cap {
            return Err(format!("pending capacity {} != {cap}", q.capacity()));
        }
        if charge != slot * cap {
            return Err(format!("pending charge {charge} != {}", slot * cap));
        }
        if layout != slot * cap {
            return Err(format!("pending max layout {layout} != {}", slot * cap));
        }
        if peak > old_charge + charge {
            return Err(format!(
                "pending peak {peak} > old+new {}",
                old_charge + charge
            ));
        }
    }
    Ok(())
}

fn grow_inflight() -> Result<(), String> {
    LIVE.store(0, Ordering::Release);
    let slot = InflightQueue::slot();
    let mut v = InflightQueue::new();
    for cap in [1usize, 2, 4, 8] {
        MAX_LAYOUT.store(0, Ordering::Release);
        PEAK.store(LIVE.load(Ordering::Acquire), Ordering::Release);
        let old_charge = v.charge_bytes();
        RECORD.store(true, Ordering::Release);
        while v.capacity() < cap {
            v.try_push_uninit()?;
        }
        RECORD.store(false, Ordering::Release);
        let layout = MAX_LAYOUT.load(Ordering::Acquire);
        let charge = v.charge_bytes();
        let peak = PEAK.load(Ordering::Acquire);
        println!(
            "inflight cap={} max_layout={} charge={} peak={} size_of*cap={}",
            v.capacity(),
            layout,
            charge,
            peak,
            slot * cap
        );
        if v.capacity() != cap {
            return Err(format!("inflight capacity {} != {cap}", v.capacity()));
        }
        if charge != slot * cap {
            return Err(format!("inflight charge {charge} != {}", slot * cap));
        }
        if layout != slot * cap {
            return Err(format!("inflight max layout {layout} != {}", slot * cap));
        }
        if peak > old_charge + charge {
            return Err(format!(
                "inflight peak {peak} > old+new {}",
                old_charge + charge
            ));
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    println!(
        "pending size={} inflight size={}",
        PendingQueue::slot(),
        InflightQueue::slot()
    );
    match grow_pending().and_then(|_| grow_inflight()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
