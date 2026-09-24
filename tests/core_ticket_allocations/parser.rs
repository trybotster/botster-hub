//! Parser fixtures use the existing recorder without changing its allocator.

use super::*;
use botster_hub::test_internals::parser_probe::{
    definition_layout, environment_node_bound, Case, PreparedProbe, ProbePhase,
};
use std::path::Path;

fn mark_parser(phase: ProbePhase) {
    PHASE.store(phase as usize, Ordering::Relaxed);
    OPERATION.store(0, Ordering::Relaxed);
    record(MARK, 0, 0, 0, 0, 0);
}

fn report_parser(
    case: Case,
    equal: bool,
    model_peak: Result<usize, &'static str>,
    output: &mut impl Write,
) -> Result<(), String> {
    let count = NEXT.load(Ordering::Acquire);
    let overflow = OVERFLOW.load(Ordering::Acquire);
    let allocation_failed = ALLOCATION_FAILED.load(Ordering::Acquire);
    let mut live = BTreeMap::<usize, Allocation>::new();
    let mut live_bytes = 0usize;
    let mut peak_bytes = 0usize;
    let mut growth_funding = 0usize;
    let mut parse_growth_funding = 0usize;
    let mut moved = 0usize;
    let mut in_place = 0usize;
    let mut faults = Vec::new();
    writeln!(output, "fixture,{},events,{count},event_capacity,{EVENT_CAPACITY},overflow,{overflow},allocation_failed,{allocation_failed},production_equal,{equal}", case.name())
        .map_err(|error| error.to_string())?;
    for (index, event) in EVENTS.iter().take(count.min(EVENT_CAPACITY)).enumerate() {
        let kind = event.kind.load(Ordering::Relaxed);
        let phase = event.phase.load(Ordering::Relaxed);
        let pointer = event.pointer.load(Ordering::Relaxed);
        let previous = event.previous.load(Ordering::Relaxed);
        let size = event.size.load(Ordering::Relaxed);
        let old_size = event.old_size.load(Ordering::Relaxed);
        let align = event.align.load(Ordering::Relaxed);
        writeln!(
            output,
            "event,{index},{kind},{phase},{pointer:#x},{previous:#x},{size},{old_size},{align}"
        )
        .map_err(|error| error.to_string())?;
        if kind == MARK {
            writeln!(
                output,
                "checkpoint,{phase},live,{},bytes,{live_bytes}",
                live.len()
            )
            .map_err(|error| error.to_string())?;
            continue;
        }
        if matches!(kind, ALLOC | ZEROED | REALLOC) && pointer != 0 {
            // A replacement needs funding before the previous charge can end.
            growth_funding = growth_funding.max(
                live_bytes
                    .checked_add(size)
                    .ok_or("growth funding overflow")?,
            );
            if phase == ProbePhase::Parse as usize {
                parse_growth_funding = parse_growth_funding.max(
                    live_bytes
                        .checked_add(size)
                        .ok_or("parser growth funding overflow")?,
                );
            }
        }
        if kind == DEALLOC || kind == REALLOC {
            let old_pointer = if kind == DEALLOC { pointer } else { previous };
            let expected_size = if kind == DEALLOC { size } else { old_size };
            if kind == REALLOC && pointer == 0 {
                continue;
            }
            if let Some(old) = live.remove(&old_pointer) {
                if old.size != expected_size || old.align != align {
                    faults.push(format!("event {index}: the allocation layout changed"));
                }
                live_bytes = live_bytes
                    .checked_sub(old.size)
                    .ok_or("live byte underflow")?;
            } else {
                faults.push(format!(
                    "event {index}: no captured allocation exists for the old pointer"
                ));
            }
        }
        if matches!(kind, ALLOC | ZEROED | REALLOC) && pointer != 0 {
            if live.insert(pointer, Allocation { size, align }).is_some() {
                faults.push(format!(
                    "event {index}: the allocator returned a live pointer"
                ));
            }
            live_bytes = live_bytes.checked_add(size).ok_or("live byte overflow")?;
            peak_bytes = peak_bytes.max(live_bytes);
            if kind == REALLOC {
                if pointer == previous {
                    in_place += 1;
                } else {
                    moved += 1;
                }
            }
        }
    }
    writeln!(output, "summary,{},peak_live_requested_bytes,{peak_bytes},growth_funding_bytes,{growth_funding},moved_reallocations,{moved},in_place_reallocations,{in_place},remaining_allocations,{},remaining_bytes,{live_bytes},physical_allocator_peak,unmeasured", case.name(), live.len())
        .map_err(|error| error.to_string())?;
    writeln!(
        output,
        "parser_model,{},counted_peak,{model_peak:?},measured_parse_growth_funding,{parse_growth_funding}",
        case.name()
    )
    .map_err(|error| error.to_string())?;
    match model_peak {
        Ok(bound) if bound >= parse_growth_funding => {}
        Ok(bound) => faults.push(format!(
            "parser model {bound} is below measured parse growth {parse_growth_funding}"
        )),
        Err(error) => faults.push(format!("parser model failed: {error}")),
    }
    if count == 0 || parse_growth_funding == 0 {
        faults.push("the parser allocation recorder captured no Parse growth".into());
    }
    if overflow {
        faults.push("the fixed event buffer overflowed".into());
    }
    if allocation_failed {
        faults.push("System refused an allocation".into());
    }
    if !equal {
        faults.push("the fixture differs from the production result".into());
    }
    if !live.is_empty() {
        faults.push("captured allocations remain after the parser result was dropped".into());
    }
    for fault in &faults {
        writeln!(output, "failure,{fault}").map_err(|error| error.to_string())?;
    }
    output.flush().map_err(|error| error.to_string())?;
    if faults.is_empty() {
        Ok(())
    } else {
        Err(faults.join("; "))
    }
}

fn run_cases(directory: &Path, output: &mut impl Write) -> Result<(), String> {
    let (size, align) = definition_layout();
    writeln!(output, "parser_oracle,rust_commit,{COMMIT},target,{TARGET},event_capacity,{EVENT_CAPACITY}\ndefinition_layout,{size},{align}\nevent_columns,index,kind,phase,pointer,previous_pointer,size,old_size,align")
        .map_err(|error| error.to_string())?;
    for phase in ProbePhase::ALL {
        writeln!(output, "phase,{},{phase:?}", phase as usize)
            .map_err(|error| error.to_string())?;
    }
    for entries in [0, 1, 2, 8, 64] {
        writeln!(
            output,
            "environment_node_bound,{entries},{:?}",
            environment_node_bound(entries)
        )
        .map_err(|error| error.to_string())?;
    }
    for case in Case::ALL {
        let fixture_directory = directory.join(case.name());
        // Fixture construction and the production control occur outside capture.
        let probe = PreparedProbe::prepare(case, &fixture_directory)?;
        let model_peak = probe.counted_parser_peak();
        std::fs::write(
            fixture_directory.join("expected.json"),
            serde_json::to_vec_pretty(&probe.expected_json()).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        NEXT.store(0, Ordering::Relaxed);
        OVERFLOW.store(false, Ordering::Relaxed);
        ALLOCATION_FAILED.store(false, Ordering::Relaxed);
        PHASE.store(ProbePhase::Parse as usize, Ordering::Relaxed);
        OPERATION.store(0, Ordering::Relaxed);
        RECORDING.store(true, Ordering::Release);
        let equal = probe.run(mark_parser);
        RECORDING.store(false, Ordering::Release);
        report_parser(case, equal, model_peak, output)?;
    }
    writeln!(output, "complete,{}", Case::ALL.len()).map_err(|error| error.to_string())?;
    output.flush().map_err(|error| error.to_string())
}

pub(super) fn run(directory: &Path) -> ExitCode {
    let stdout = io::stdout();
    let mut output = io::BufWriter::new(stdout.lock());
    if let Err(error) = run_cases(directory, &mut output) {
        eprintln!("Parser allocation oracle failed: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
