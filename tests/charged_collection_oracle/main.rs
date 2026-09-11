//! Layout.size() versus charged collection capacity on rustc 1.97.0.

use std::alloc::{GlobalAlloc, Layout, System};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};

use botster_hub::test_internals::charged_collection::{InflightQueue, PendingQueue};
use botster_hub::test_internals::lua_json;

struct Recorder;

const MAX_EVENTS: usize = 16;
const KIND_ALLOC: u8 = 1;
const KIND_DEALLOC: u8 = 2;

static RECORD: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static EV_N: AtomicUsize = AtomicUsize::new(0);
static EV_KIND: [AtomicU8; MAX_EVENTS] = [const { AtomicU8::new(0) }; MAX_EVENTS];
static EV_SIZE: [AtomicUsize; MAX_EVENTS] = [const { AtomicUsize::new(0) }; MAX_EVENTS];

fn record_event(kind: u8, size: usize) {
    if !RECORD.load(Ordering::Acquire) {
        return;
    }
    let index = EV_N.fetch_add(1, Ordering::AcqRel);
    if index < MAX_EVENTS {
        EV_KIND[index].store(kind, Ordering::Release);
        EV_SIZE[index].store(size, Ordering::Release);
    }
}

unsafe impl GlobalAlloc for Recorder {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if RECORD.load(Ordering::Acquire) {
            record_event(KIND_ALLOC, layout.size());
            let live = LIVE.fetch_add(layout.size(), Ordering::AcqRel) + layout.size();
            PEAK.fetch_max(live, Ordering::AcqRel);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if RECORD.load(Ordering::Acquire) {
            record_event(KIND_DEALLOC, layout.size());
            LIVE.fetch_sub(layout.size(), Ordering::AcqRel);
        }
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if RECORD.load(Ordering::Acquire) {
            record_event(KIND_DEALLOC, layout.size());
            record_event(KIND_ALLOC, new_size);
            let live = LIVE.load(Ordering::Acquire);
            PEAK.fetch_max(live.saturating_add(new_size), Ordering::AcqRel);
            LIVE.store(
                live.saturating_sub(layout.size()).saturating_add(new_size),
                Ordering::Release,
            );
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: Recorder = Recorder;

fn events() -> Vec<(u8, usize)> {
    let n = EV_N.load(Ordering::Acquire).min(MAX_EVENTS);
    (0..n)
        .map(|index| {
            (
                EV_KIND[index].load(Ordering::Acquire),
                EV_SIZE[index].load(Ordering::Acquire),
            )
        })
        .collect()
}

fn begin_record() {
    EV_N.store(0, Ordering::Release);
    PEAK.store(LIVE.load(Ordering::Acquire), Ordering::Release);
    RECORD.store(true, Ordering::Release);
}

fn end_record() {
    RECORD.store(false, Ordering::Release);
}

fn check_growth(
    label: &str,
    old_cap: usize,
    new_cap: usize,
    slot: usize,
    charge: usize,
    old_charge: usize,
) -> Result<(), String> {
    let recorded = events();
    let allocs: Vec<usize> = recorded
        .iter()
        .filter(|(kind, _)| *kind == KIND_ALLOC)
        .map(|(_, size)| *size)
        .collect();
    let deallocs: Vec<usize> = recorded
        .iter()
        .filter(|(kind, _)| *kind == KIND_DEALLOC)
        .map(|(_, size)| *size)
        .collect();
    let peak = PEAK.load(Ordering::Acquire);
    println!(
        "{label} cap={new_cap} allocs={allocs:?} deallocs={deallocs:?} charge={charge} peak={peak} size_of*cap={}",
        slot * new_cap
    );
    if allocs != [slot * new_cap] {
        return Err(format!(
            "{label} cap={new_cap}: expected one buffer alloc {}, got {allocs:?}",
            slot * new_cap
        ));
    }
    if old_cap == 0 {
        if !deallocs.is_empty() {
            return Err(format!(
                "{label} cap={new_cap}: expected no dealloc, got {deallocs:?}"
            ));
        }
    } else if deallocs != [slot * old_cap] {
        return Err(format!(
            "{label} cap={new_cap}: expected one dealloc {}, got {deallocs:?}",
            slot * old_cap
        ));
    }
    if charge != slot * new_cap {
        return Err(format!("{label} charge {charge} != {}", slot * new_cap));
    }
    if peak > old_charge + charge {
        return Err(format!(
            "{label} peak {peak} > old+new {}",
            old_charge + charge
        ));
    }
    Ok(())
}

fn grow_queue<Q: Queue>(label: &str, mut q: Q) -> Result<(), String> {
    let slot = Q::slot();
    let mut old_cap = 0;
    for cap in [1usize, 2, 4, 8] {
        let old_charge = q.charge_bytes();
        begin_record();
        while q.capacity() < cap {
            q.try_push_uninit()?;
        }
        end_record();
        if q.capacity() != cap {
            return Err(format!("{label} capacity {} != {cap}", q.capacity()));
        }
        check_growth(label, old_cap, cap, slot, q.charge_bytes(), old_charge)?;
        old_cap = cap;
    }
    Ok(())
}

trait Queue {
    fn slot() -> usize;
    fn capacity(&self) -> usize;
    fn charge_bytes(&self) -> usize;
    fn try_push_uninit(&mut self) -> Result<(), String>;
}

impl Queue for PendingQueue {
    fn slot() -> usize {
        PendingQueue::slot()
    }
    fn capacity(&self) -> usize {
        self.capacity()
    }
    fn charge_bytes(&self) -> usize {
        self.charge_bytes()
    }
    fn try_push_uninit(&mut self) -> Result<(), String> {
        self.try_push_uninit()
    }
}

impl Queue for InflightQueue {
    fn slot() -> usize {
        InflightQueue::slot()
    }
    fn capacity(&self) -> usize {
        self.capacity()
    }
    fn charge_bytes(&self) -> usize {
        self.charge_bytes()
    }
    fn try_push_uninit(&mut self) -> Result<(), String> {
        self.try_push_uninit()
    }
}

fn main() -> ExitCode {
    if PendingQueue::type_size() != PendingQueue::uninit_size()
        || PendingQueue::type_align() != PendingQueue::uninit_align()
    {
        eprintln!(
            "pending MaybeUninit layout mismatch size {}/{} align {}/{}",
            PendingQueue::type_size(),
            PendingQueue::uninit_size(),
            PendingQueue::type_align(),
            PendingQueue::uninit_align()
        );
        return ExitCode::FAILURE;
    }
    if InflightQueue::type_size() != InflightQueue::uninit_size()
        || InflightQueue::type_align() != InflightQueue::uninit_align()
    {
        eprintln!(
            "inflight MaybeUninit layout mismatch size {}/{} align {}/{}",
            InflightQueue::type_size(),
            InflightQueue::uninit_size(),
            InflightQueue::type_align(),
            InflightQueue::uninit_align()
        );
        return ExitCode::FAILURE;
    }
    println!(
        "commit={} pending size={} align={} inflight size={} align={}",
        option_env!("BOTSTER_EMBEDDED_BUILD_REVISION").unwrap_or("unspecified"),
        PendingQueue::type_size(),
        PendingQueue::type_align(),
        InflightQueue::type_size(),
        InflightQueue::type_align()
    );
    LIVE.store(0, Ordering::Release);
    match grow_queue("pending", PendingQueue::new())
        .and_then(|_| {
            LIVE.store(0, Ordering::Release);
            grow_queue("inflight", InflightQueue::new())
        })
        .and_then(|_| measure_lua_json())
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn measure_lua_json() -> Result<(), String> {
    for (label, source) in [
        (
            "wide-object-8",
            r#"
            local t = {}
            for i = 1, 8 do
                t['k' .. i] = 'v' .. i
            end
            return t
            "#,
        ),
        (
            "wide-object",
            r#"
            local t = {}
            for i = 1, 32 do
                t['k' .. i] = 'v' .. i
            end
            return t
            "#,
        ),
        (
            "wide-object-64",
            r#"
            local t = {}
            for i = 1, 64 do
                t['k' .. i] = 'v' .. i
            end
            return t
            "#,
        ),
        (
            "nested-depth-limit",
            r#"
            local t = 'leaf'
            for _ = 1, 128 do
                t = { n = t }
            end
            return t
            "#,
        ),
        (
            "array-heavy",
            r#"
            local t = {}
            for i = 1, 64 do
                t[i] = 'item' .. i
            end
            return t
            "#,
        ),
    ] {
        let prepared = lua_json::prepare(source);
        let admitted = lua_json::admitted(&prepared);
        LIVE.store(0, Ordering::Release);
        begin_record();
        let built = lua_json::build(&prepared);
        end_record();
        let peak = PEAK.load(Ordering::Acquire);
        let ratio = if admitted == 0 {
            0.0
        } else {
            peak as f64 / admitted as f64
        };
        let live_refs = lua_json::live_refs_peak(&prepared);
        println!(
            "{label} admitted={admitted} peak={peak} ratio={ratio:.6} live_refs_peak={live_refs}"
        );
        if peak > admitted {
            return Err(format!(
                "{label}: counted peak {peak} > admitted {admitted}"
            ));
        }
        let _ = built;
    }
    Ok(())
}
