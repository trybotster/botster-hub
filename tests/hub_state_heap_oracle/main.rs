//! Isolated 1.97 clone-heap oracle for G2. Not the Hub process allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use botster_hub::config::{DataDirectoryOption, HubStartupOptions, RuntimeEnvironment};
use botster_hub::test_internals::hub_state_heap::walk_hub_state;
use botster_hub::persistence::HubState;

struct Counter;

static RECORDING: AtomicBool = AtomicBool::new(false);
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if RECORDING.load(Ordering::Acquire) {
            ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counter = Counter;

fn main() -> ExitCode {
    let config = HubStartupOptions {
        data_directory: DataDirectoryOption::Explicit(
            std::path::PathBuf::from("/private/tmp/hub-state-heap-oracle"),
        ),
        ..HubStartupOptions::default()
    }
    .build_config_for_environment(&RuntimeEnvironment::from_values(None, None))
    .expect("config");
    let state = HubState::from_config(&config);
    let walk = walk_hub_state(&state);
    ALLOCATED.store(0, Ordering::Relaxed);
    RECORDING.store(true, Ordering::Release);
    let _clone = state.clone();
    RECORDING.store(false, Ordering::Release);
    let counted = ALLOCATED.load(Ordering::Relaxed);
    if counted > walk.clone_heap {
        eprintln!(
            "oracle: counted {counted} > walk.clone_heap {}",
            walk.clone_heap
        );
        return ExitCode::from(1);
    }
    println!(
        "oracle: counted {counted} <= walk.clone_heap {}",
        walk.clone_heap
    );
    ExitCode::SUCCESS
}
