//! The only allocator override for the isolated C1 allocation oracle.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use botster_hub::test_internals::allocation_oracle::{self, Phase, Scenario};

#[cfg(not(all(target_arch = "aarch64", target_os = "macos", target_vendor = "apple")))]
compile_error!("The C1 allocation oracle supports only aarch64-apple-darwin.");

const COMMIT: &str = "2d8144b7880597b6e6d3dfd63a9a9efae3f533d3";
const TARGET: &str = "aarch64-apple-darwin";
// Estimate for 4,224 registrations: 16.9k markers + 8.4k register/take Vec events
// each + tree events is about 40k events. This is not a proved event maximum.
// The recorder fails on overflow even if this estimate is wrong.
const EVENT_CAPACITY: usize = 65_536;
const MARK: usize = 0;
const ALLOC: usize = 1;
const ZEROED: usize = 2;
const DEALLOC: usize = 3;
const REALLOC: usize = 4;

struct EventSlot {
    kind: AtomicUsize,
    phase: AtomicUsize,
    operation: AtomicUsize,
    pointer: AtomicUsize,
    previous: AtomicUsize,
    size: AtomicUsize,
    old_size: AtomicUsize,
    align: AtomicUsize,
}

impl EventSlot {
    const fn new() -> Self {
        Self {
            kind: AtomicUsize::new(0),
            phase: AtomicUsize::new(0),
            operation: AtomicUsize::new(0),
            pointer: AtomicUsize::new(0),
            previous: AtomicUsize::new(0),
            size: AtomicUsize::new(0),
            old_size: AtomicUsize::new(0),
            align: AtomicUsize::new(0),
        }
    }
}

static EVENTS: [EventSlot; EVENT_CAPACITY] = [const { EventSlot::new() }; EVENT_CAPACITY];
static RECORDING: AtomicBool = AtomicBool::new(false);
static START_WORKER: AtomicBool = AtomicBool::new(false);
static NEXT: AtomicUsize = AtomicUsize::new(0);
static PHASE: AtomicUsize = AtomicUsize::new(0);
static OPERATION: AtomicUsize = AtomicUsize::new(0);
static OVERFLOW: AtomicBool = AtomicBool::new(false);
static ALLOCATION_FAILED: AtomicBool = AtomicBool::new(false);

struct Recorder;

#[global_allocator]
static ALLOCATOR: Recorder = Recorder;

// This function uses only static atomics. It cannot allocate or call the allocator again.
fn record(
    kind: usize,
    pointer: usize,
    previous: usize,
    size: usize,
    old_size: usize,
    align: usize,
) {
    if !RECORDING.load(Ordering::Acquire) {
        return;
    }
    let index = NEXT.fetch_add(1, Ordering::Relaxed);
    if index >= EVENT_CAPACITY {
        OVERFLOW.store(true, Ordering::Relaxed);
        return;
    }
    let slot = &EVENTS[index];
    slot.kind.store(kind, Ordering::Relaxed);
    slot.phase
        .store(PHASE.load(Ordering::Relaxed), Ordering::Relaxed);
    slot.operation
        .store(OPERATION.load(Ordering::Relaxed), Ordering::Relaxed);
    slot.pointer.store(pointer, Ordering::Relaxed);
    slot.previous.store(previous, Ordering::Relaxed);
    slot.size.store(size, Ordering::Relaxed);
    slot.old_size.store(old_size, Ordering::Relaxed);
    slot.align.store(align, Ordering::Relaxed);
    if matches!(kind, ALLOC | ZEROED | REALLOC) && pointer == 0 {
        ALLOCATION_FAILED.store(true, Ordering::Relaxed);
    }
}

// SAFETY: Each method delegates the original pointer and Layout to System exactly once.
// Recording accesses only preallocated atomic slots and does not inspect allocated memory.
unsafe impl GlobalAlloc for Recorder {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        record(ALLOC, pointer as usize, 0, layout.size(), 0, layout.align());
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        record(
            ZEROED,
            pointer as usize,
            0,
            layout.size(),
            0,
            layout.align(),
        );
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        record(
            DEALLOC,
            pointer as usize,
            0,
            layout.size(),
            0,
            layout.align(),
        );
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let replacement = unsafe { System.realloc(pointer, layout, new_size) };
        record(
            REALLOC,
            replacement as usize,
            pointer as usize,
            new_size,
            layout.size(),
            layout.align(),
        );
        replacement
    }
}

fn mark(phase: Phase, operation: usize) {
    PHASE.store(phase as usize, Ordering::Relaxed);
    OPERATION.store(operation, Ordering::Relaxed);
    record(MARK, 0, 0, 0, 0, 0);
}

#[derive(Clone, Copy)]
struct Allocation {
    size: usize,
    align: usize,
}

fn report(scenario: Scenario, output: &mut impl Write) -> Result<(), String> {
    let count = NEXT.load(Ordering::Acquire);
    let mut live = BTreeMap::<usize, Allocation>::new();
    let mut seen = BTreeSet::new();
    let mut layouts = BTreeMap::<(usize, usize, usize, usize), usize>::new();
    let mut live_bytes = 0usize;
    let mut peak_bytes = 0usize;
    let mut foreign_deallocations = 0usize;
    let mut wait_allocations = 0usize;
    let mut retained_at_end = 0usize;
    let mut faults = Vec::new();
    writeln!(output, "scenario,{scenario:?},events,{count}").map_err(|e| e.to_string())?;
    for (index, event) in EVENTS.iter().take(count.min(EVENT_CAPACITY)).enumerate() {
        let kind = event.kind.load(Ordering::Relaxed);
        let phase = event.phase.load(Ordering::Relaxed);
        let operation = event.operation.load(Ordering::Relaxed);
        let pointer = event.pointer.load(Ordering::Relaxed);
        let previous = event.previous.load(Ordering::Relaxed);
        let size = event.size.load(Ordering::Relaxed);
        let old_size = event.old_size.load(Ordering::Relaxed);
        let align = event.align.load(Ordering::Relaxed);
        writeln!(output, "event,{index},{kind},{phase},{operation},{pointer:#x},{previous:#x},{size},{old_size},{align}").map_err(|e| e.to_string())?;
        if kind == MARK {
            if phase == Phase::End as usize {
                retained_at_end = live.len();
            }
            writeln!(
                output,
                "checkpoint,{phase},{operation},live,{},bytes,{live_bytes}",
                live.len()
            )
            .map_err(|e| e.to_string())?;
            continue;
        }
        *layouts.entry((phase, kind, size, align)).or_default() += 1;
        if kind == DEALLOC || kind == REALLOC {
            let old_pointer = if kind == DEALLOC { pointer } else { previous };
            let expected_size = if kind == DEALLOC { size } else { old_size };
            if let Some(old) = live.remove(&old_pointer) {
                if old.size != expected_size || old.align != align {
                    faults.push(format!(
                        "event {index}: deallocation Layout differs from allocation"
                    ));
                }
                live_bytes -= old.size;
            } else {
                // Thread startup happens before capture. Its teardown can release those blocks.
                if seen.contains(&old_pointer) {
                    faults.push(format!(
                        "event {index}: a captured pointer has no live allocation"
                    ));
                }
                foreign_deallocations += 1;
            }
        }
        if matches!(kind, ALLOC | ZEROED | REALLOC) && pointer != 0 {
            if phase == Phase::Wait as usize {
                wait_allocations += 1;
            }
            seen.insert(pointer);
            if live.insert(pointer, Allocation { size, align }).is_some() {
                faults.push(format!(
                    "event {index}: the allocator returned a live pointer"
                ));
            }
            live_bytes = live_bytes
                .checked_add(size)
                .ok_or("live byte count overflow")?;
            peak_bytes = peak_bytes.max(live_bytes);
        }
    }
    for ((phase, kind, size, align), occurrences) in layouts {
        writeln!(output, "layout,{phase},{kind},{size},{align},{occurrences}")
            .map_err(|e| e.to_string())?;
    }
    writeln!(output, "summary,{scenario:?},peak_live_requested_bytes,{peak_bytes},remaining_allocations,{},remaining_bytes,{live_bytes},preexisting_deallocations,{foreign_deallocations}", live.len()).map_err(|e| e.to_string())?;
    if OVERFLOW.load(Ordering::Acquire) {
        faults.push("the fixed event buffer overflowed".into());
    }
    if ALLOCATION_FAILED.load(Ordering::Acquire) {
        faults.push("System refused an allocation".into());
    }
    if matches!(
        scenario,
        Scenario::BlockingWait | Scenario::CallbackReplyWaitTimeout
    ) && (wait_allocations == 0 || retained_at_end == 0)
    {
        faults.push("the wait scenario did not observe retained thread-local allocation".into());
    }
    for (pointer, allocation) in &live {
        writeln!(
            output,
            "unreleased,{pointer:#x},{},{}",
            allocation.size, allocation.align
        )
        .map_err(|e| e.to_string())?;
    }
    if !live.is_empty() {
        faults.push("captured allocations remain after thread exit; source evidence must distinguish a scenario leak from process-lifetime initialization".into());
    }
    for fault in &faults {
        writeln!(output, "failure,{fault}").map_err(|e| e.to_string())?;
    }
    output.flush().map_err(|e| e.to_string())?;
    if faults.is_empty() {
        Ok(())
    } else {
        Err(faults.join("; "))
    }
}

fn run_one(scenario: Scenario, output: &mut impl Write) -> Result<(), String> {
    START_WORKER.store(false, Ordering::Release);
    let worker = std::thread::Builder::new()
        .name("c1-allocation-oracle".into())
        .spawn(move || {
            while !START_WORKER.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            allocation_oracle::run(scenario, mark);
        })
        .map_err(|e| e.to_string())?;
    NEXT.store(0, Ordering::Relaxed);
    OVERFLOW.store(false, Ordering::Relaxed);
    ALLOCATION_FAILED.store(false, Ordering::Relaxed);
    PHASE.store(Phase::Startup as usize, Ordering::Relaxed);
    OPERATION.store(0, Ordering::Relaxed);
    RECORDING.store(true, Ordering::Release);
    START_WORKER.store(true, Ordering::Release);
    while !worker.is_finished() {
        std::thread::yield_now();
    }
    // Join includes thread-local destruction. Capture stays active through that destruction.
    let outcome = worker.join();
    RECORDING.store(false, Ordering::Release);
    report(scenario, output)?;
    if outcome.is_err() {
        return Err(format!("scenario {scenario:?} panicked"));
    }
    Ok(())
}

fn main() -> ExitCode {
    if option_env!("C1_ORACLE_RUSTC_COMMIT") != Some(COMMIT)
        || option_env!("C1_ORACLE_TARGET") != Some(TARGET)
    {
        eprintln!("The oracle must use the reviewed build script and pinned compiler/target.");
        return ExitCode::FAILURE;
    }
    let stdout = io::stdout();
    let mut output = io::BufWriter::new(stdout.lock());
    if writeln!(output, "oracle,rust_commit,{COMMIT},target,{TARGET},event_capacity,{EVENT_CAPACITY}\nevent_columns,index,kind,phase,operation,pointer,previous_pointer,size,old_size,align").is_err() {
        return ExitCode::FAILURE;
    }
    for phase in Phase::ALL {
        if writeln!(
            output,
            "phase,{},{}",
            phase as usize,
            format_args!("{phase:?}")
        )
        .is_err()
        {
            return ExitCode::FAILURE;
        }
    }
    for (name, size, align) in allocation_oracle::type_layouts() {
        if writeln!(output, "hub_type,{name},{size},{align}").is_err() {
            return ExitCode::FAILURE;
        }
    }
    for scenario in Scenario::ALL {
        if let Err(error) = run_one(scenario, &mut output) {
            eprintln!("Allocation oracle failed: {error}");
            return ExitCode::FAILURE;
        }
    }
    if writeln!(output, "complete,{}", Scenario::ALL.len())
        .and_then(|_| output.flush())
        .is_err()
    {
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
