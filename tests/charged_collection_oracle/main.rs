//! Layout.size() versus charged collection capacity on rustc 1.97.0.

use std::alloc::{GlobalAlloc, Layout, System};
use std::os::raw::c_int;
use std::process::ExitCode;
use std::sync::Arc;
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
static EV_ALIGN: [AtomicUsize; MAX_EVENTS] = [const { AtomicUsize::new(0) }; MAX_EVENTS];
static XRC_SIZE: AtomicUsize = AtomicUsize::new(0);
static XRC_ALIGN: AtomicUsize = AtomicUsize::new(0);
static XRC_LIVE: AtomicUsize = AtomicUsize::new(0);
static XRC_PEAK: AtomicUsize = AtomicUsize::new(0);
static XRC_OVERFLOW: AtomicBool = AtomicBool::new(false);
static ALLOC_SUM: AtomicUsize = AtomicUsize::new(0);
static DEALLOC_SUM: AtomicUsize = AtomicUsize::new(0);
const MAX_XRC: usize = 512;
static XRC_PTRS: [AtomicUsize; MAX_XRC] = [const { AtomicUsize::new(0) }; MAX_XRC];
const MAX_LIVE: usize = 4096;
static LIVE_PTR: [AtomicUsize; MAX_LIVE] = [const { AtomicUsize::new(0) }; MAX_LIVE];
static LIVE_SZ: [AtomicUsize; MAX_LIVE] = [const { AtomicUsize::new(0) }; MAX_LIVE];
static LIVE_ALIGN: [AtomicUsize; MAX_LIVE] = [const { AtomicUsize::new(0) }; MAX_LIVE];
static WINDOW_OVERFLOW: AtomicBool = AtomicBool::new(false);
/// mlua-sys 0.6.8: 16 on aarch64/x86_64.
const SYS_MIN_ALIGN: usize = 16;

fn record_event(kind: u8, layout: Layout) {
    if !RECORD.load(Ordering::Acquire) {
        return;
    }
    let index = EV_N.fetch_add(1, Ordering::AcqRel);
    if index < MAX_EVENTS {
        EV_KIND[index].store(kind, Ordering::Release);
        EV_SIZE[index].store(layout.size(), Ordering::Release);
        EV_ALIGN[index].store(layout.align(), Ordering::Release);
    }
}

fn is_xrc_layout(layout: Layout) -> bool {
    let size = XRC_SIZE.load(Ordering::Acquire);
    size != 0 && layout.size() == size && layout.align() == XRC_ALIGN.load(Ordering::Acquire)
}

fn xrc_note_alloc(ptr: *mut u8) {
    let addr = ptr as usize;
    if addr == 0 {
        return;
    }
    for slot in &XRC_PTRS {
        if slot
            .compare_exchange(0, addr, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let live = XRC_LIVE.fetch_add(1, Ordering::AcqRel) + 1;
            XRC_PEAK.fetch_max(live, Ordering::AcqRel);
            return;
        }
    }
    XRC_OVERFLOW.store(true, Ordering::Release);
}

fn window_alloc(ptr: *mut u8, size: usize, align: usize) {
    let addr = ptr as usize;
    if addr == 0 {
        return;
    }
    for ((slot, bytes), slot_align) in LIVE_PTR
        .iter()
        .zip(LIVE_SZ.iter())
        .zip(LIVE_ALIGN.iter())
    {
        if slot
            .compare_exchange(0, addr, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            bytes.store(size, Ordering::Release);
            slot_align.store(align, Ordering::Release);
            ALLOC_SUM.fetch_add(size, Ordering::AcqRel);
            return;
        }
    }
    WINDOW_OVERFLOW.store(true, Ordering::Release);
}

fn window_dealloc(ptr: *mut u8) {
    let addr = ptr as usize;
    if addr == 0 {
        return;
    }
    for ((slot, bytes), slot_align) in LIVE_PTR
        .iter()
        .zip(LIVE_SZ.iter())
        .zip(LIVE_ALIGN.iter())
    {
        if slot
            .compare_exchange(addr, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let size = bytes.swap(0, Ordering::AcqRel);
            slot_align.store(0, Ordering::Release);
            DEALLOC_SUM.fetch_add(size, Ordering::AcqRel);
            return;
        }
    }
}

fn xrc_note_dealloc(ptr: *mut u8) {
    let addr = ptr as usize;
    if addr == 0 {
        return;
    }
    for slot in &XRC_PTRS {
        if slot
            .compare_exchange(addr, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            XRC_LIVE.fetch_sub(1, Ordering::AcqRel);
            return;
        }
    }
}

unsafe impl GlobalAlloc for Recorder {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if RECORD.load(Ordering::Acquire) {
            record_event(KIND_ALLOC, layout);
            let live = LIVE.load(Ordering::Acquire).saturating_add(layout.size());
            LIVE.store(live, Ordering::Release);
            PEAK.fetch_max(live, Ordering::AcqRel);
            if !ptr.is_null() {
                window_alloc(ptr, layout.size(), layout.align());
            }
            if !ptr.is_null() && is_xrc_layout(layout) {
                xrc_note_alloc(ptr);
            }
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if RECORD.load(Ordering::Acquire) {
            record_event(KIND_DEALLOC, layout);
            let live = LIVE.load(Ordering::Acquire).saturating_sub(layout.size());
            LIVE.store(live, Ordering::Release);
            window_dealloc(ptr);
            if is_xrc_layout(layout) {
                xrc_note_dealloc(ptr);
            }
        }
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_layout = Layout::from_size_align(new_size, layout.align()).unwrap_or(layout);
        if RECORD.load(Ordering::Acquire) && is_xrc_layout(layout) {
            xrc_note_dealloc(ptr);
        }
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if RECORD.load(Ordering::Acquire) {
            record_event(KIND_DEALLOC, layout);
            record_event(KIND_ALLOC, new_layout);
            let live = LIVE.load(Ordering::Acquire);
            PEAK.fetch_max(live.saturating_add(new_size), Ordering::AcqRel);
            LIVE.store(
                live.saturating_sub(layout.size()).saturating_add(new_size),
                Ordering::Release,
            );
            window_dealloc(ptr);
            if !new_ptr.is_null() {
                window_alloc(new_ptr, new_size, new_layout.align());
            }
            if !new_ptr.is_null() && is_xrc_layout(new_layout) {
                xrc_note_alloc(new_ptr);
            }
        }
        new_ptr
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
    for slot in &XRC_PTRS {
        slot.store(0, Ordering::Release);
    }
    for ((slot, bytes), slot_align) in LIVE_PTR
        .iter()
        .zip(LIVE_SZ.iter())
        .zip(LIVE_ALIGN.iter())
    {
        slot.store(0, Ordering::Release);
        bytes.store(0, Ordering::Release);
        slot_align.store(0, Ordering::Release);
    }
    WINDOW_OVERFLOW.store(false, Ordering::Release);
    XRC_LIVE.store(0, Ordering::Release);
    XRC_PEAK.store(0, Ordering::Release);
    XRC_OVERFLOW.store(false, Ordering::Release);
    ALLOC_SUM.store(0, Ordering::Release);
    DEALLOC_SUM.store(0, Ordering::Release);
    RECORD.store(true, Ordering::Release);
}

fn capture_xrc_layout() -> Layout {
    begin_record();
    let probe = Arc::new(0 as c_int);
    end_record();
    let size = EV_SIZE[0].load(Ordering::Acquire);
    let align = EV_ALIGN[0].load(Ordering::Acquire);
    drop(probe);
    XRC_SIZE.store(size, Ordering::Release);
    XRC_ALIGN.store(align, Ordering::Release);
    Layout::from_size_align(size, align).expect("ArcInner<c_int> layout")
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
    let xrc = capture_xrc_layout();
    println!(
        "xrc_layout size={} align={} (from Arc::<c_int>, not a hard-coded 24); other same-layout allocations can only raise measured_xrc_peak",
        xrc.size(),
        xrc.align()
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
            "wide-object-tables-8",
            r#"
            local t = {}
            for i = 1, 8 do
                t['k' .. i] = {}
            end
            return t
            "#,
        ),
        (
            "wide-object-tables",
            r#"
            local t = {}
            for i = 1, 32 do
                t['k' .. i] = {}
            end
            return t
            "#,
        ),
        (
            "wide-object-tables-64",
            r#"
            local t = {}
            for i = 1, 64 do
                t['k' .. i] = {}
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
        let model = lua_json::live_refs_peak(&prepared);
        let admitted = lua_json::admitted(&prepared);
        LIVE.store(0, Ordering::Release);
        begin_record();
        let built = lua_json::build(&prepared);
        end_record();
        let peak = PEAK.load(Ordering::Acquire);
        let measured = XRC_PEAK.load(Ordering::Acquire);
        let ratio = if admitted == 0 {
            0.0
        } else {
            peak as f64 / admitted as f64
        };
        println!(
            "{label} admitted={admitted} peak={peak} ratio={ratio:.6} live_refs_peak_model={model} measured_xrc_peak={measured}"
        );
        if peak > admitted {
            return Err(format!(
                "{label}: counted peak {peak} > admitted {admitted}"
            ));
        }
        if XRC_OVERFLOW.load(Ordering::Acquire) {
            return Err(format!(
                "{label}: XRc pointer table overflowed (MAX_XRC={MAX_XRC})"
            ));
        }
        if measured > model {
            return Err(format!(
                "{label}: measured XRc peak {measured} > model live_refs_peak {model}"
            ));
        }
        let _ = built;
    }
    for n in [250, 1000, 4000] {
        measure_raise_storm(&format!("publish-capacity-raises-{n}"), n, |storm| {
            storm.retain_publish_errors(n)
        })?;
        measure_raise_storm(&format!("drain-capacity-raises-{n}"), n, |storm| {
            storm.retain_drain_errors(n)
        })?;
    }
    for n in [8, 32, 128] {
        measure_hook_storm(n)?;
    }
    Ok(())
}

fn measure_raise_storm(
    label: &str,
    n: u32,
    run: impl FnOnce(&botster_hub::test_internals::CapacityRaiseStorm) -> Result<(), String>,
) -> Result<(), String> {
    let storm = botster_hub::test_internals::prepare_capacity_raise_storm();
    let lua_before = storm.used_memory();
    LIVE.store(0, Ordering::Release);
    begin_record();
    run(&storm).map_err(|error| format!("{label}: {error}"))?;
    end_record();
    reconcile(label, n, lua_before, storm.used_memory())
}

fn measure_hook_storm(n: u32) -> Result<(), String> {
    let storm = botster_hub::test_internals::prepare_hook_raise_storm()?;
    let lua_before = storm.used_memory();
    LIVE.store(0, Ordering::Release);
    begin_record();
    storm
        .retain_errors(n)
        .map_err(|error| format!("hook-budget-raises-{n}: {error}"))?;
    end_record();
    reconcile(
        &format!("hook-budget-raises-{n}"),
        n,
        lua_before,
        storm.used_memory(),
    )
}

fn reconcile(label: &str, n: u32, lua_before: usize, lua_after: usize) -> Result<(), String> {
    let alloc = ALLOC_SUM.load(Ordering::Acquire);
    let dealloc = DEALLOC_SUM.load(Ordering::Acquire);
    let net = alloc as i128 - dealloc as i128;
    let lua_delta = lua_after as i128 - lua_before as i128;
    let rust_only = net as i128 - lua_delta;
    let per_raise = if n == 0 {
        0.0
    } else {
        rust_only as f64 / n as f64
    };
    println!(
        "{label} n={n} alloc_sum={alloc} dealloc_sum={dealloc} net={net} lua_delta={lua_delta} rust_only={rust_only} rust_only_per_raise={per_raise:.4}"
    );
    print_non_sys_min_align_histogram(label);
    if WINDOW_OVERFLOW.load(Ordering::Acquire) {
        return Err(format!("{label}: live-pointer table overflowed"));
    }
    Ok(())
}

fn print_non_sys_min_align_histogram(label: &str) {
    let mut buckets: Vec<(usize, usize, usize, usize)> = Vec::new();
    for i in 0..MAX_LIVE {
        if LIVE_PTR[i].load(Ordering::Acquire) == 0 {
            continue;
        }
        let size = LIVE_SZ[i].load(Ordering::Acquire);
        let align = LIVE_ALIGN[i].load(Ordering::Acquire);
        if align == 0 || align == SYS_MIN_ALIGN {
            continue;
        }
        match buckets
            .iter_mut()
            .find(|(s, a, _, _)| *s == size && *a == align)
        {
            Some((_, _, count, bytes)) => {
                *count += 1;
                *bytes += size;
            }
            None => buckets.push((size, align, 1, size)),
        }
    }
    buckets.sort_by_key(|b| (b.1, b.0));
    print!("{label} non_SYS_MIN_ALIGN_live SYS_MIN_ALIGN={SYS_MIN_ALIGN}");
    if buckets.is_empty() {
        println!(" (none)");
        return;
    }
    println!();
    for (size, align, count, bytes) in buckets {
        println!(
            "{label} hist size={size} align={align} count={count} live_bytes={bytes}"
        );
    }
}
