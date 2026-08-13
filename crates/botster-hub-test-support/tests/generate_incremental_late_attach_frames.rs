//! Generate opaque incremental GHOSTSNP frames for hub-test-support.
//!
//! Run with:
//! `GENERATE_LATE_ATTACH_FRAMES=1 cargo test -p botster-hub-test-support --test generate_incremental_late_attach_frames -- --ignored --nocapture`

use std::fs;
use std::path::PathBuf;

use botster_core::TerminalScreenSize;
use botster_terminal_ghostty::{GhosttyAdapterConfig, GhosttySnapshotFrameKind, GhosttyTerminal};

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/ghostsnp")
}

fn write_frame(name: &str, bytes: &[u8]) {
    let path = fixture_dir().join(name);
    fs::write(&path, bytes).unwrap_or_else(|error| panic!("write {}: {error}", path.display()));
    eprintln!("wrote {} ({} bytes)", path.display(), bytes.len());
}

#[test]
#[ignore]
fn generate_incremental_late_attach_frames() {
    assert_eq!(
        std::env::var("GENERATE_LATE_ATTACH_FRAMES").as_deref(),
        Ok("1"),
        "set GENERATE_LATE_ATTACH_FRAMES=1 to write fixtures"
    );

    let history_size = TerminalScreenSize::new(2, 215);
    let mut history = GhosttyTerminal::with_config(
        history_size,
        GhosttyAdapterConfig::with_max_scrollback_bytes(512 * 1024),
    )
    .expect("history terminal");
    for index in 0..1000 {
        history.write_output_bytes(format!("history-{index:04}\r\n").as_bytes());
    }
    history.write_output_bytes(b"history-before-live");

    let mut history_frames = Vec::new();
    history
        .export_snapshot_frames(|frame| {
            history_frames.push(frame);
            true
        })
        .expect("export history frames");
    assert_eq!(
        history_frames.first().map(|frame| frame.kind),
        Some(GhosttySnapshotFrameKind::Ready)
    );
    assert!(
        history_frames
            .iter()
            .any(|frame| frame.kind == GhosttySnapshotFrameKind::History)
    );
    assert_eq!(
        history_frames.last().map(|frame| frame.kind),
        Some(GhosttySnapshotFrameKind::Finish)
    );
    write_frame(
        "late-attach-history-ready-v2.ghostsnp",
        &history_frames[0].bytes,
    );
    let page = history_frames
        .iter()
        .find(|frame| frame.kind == GhosttySnapshotFrameKind::History)
        .expect("PAGE");
    write_frame("late-attach-history-page-v2.ghostsnp", &page.bytes);
    write_frame(
        "late-attach-history-finish-v2.ghostsnp",
        &history_frames.last().expect("FINISH").bytes,
    );

    let blank = GhosttyTerminal::with_config(
        TerminalScreenSize::new(24, 80),
        GhosttyAdapterConfig::with_max_scrollback_bytes(512 * 1024),
    )
    .expect("blank terminal");
    let mut blank_frames = Vec::new();
    blank
        .export_snapshot_frames(|frame| {
            blank_frames.push(frame);
            true
        })
        .expect("export blank frames");
    assert_eq!(blank_frames.len(), 2);
    write_frame(
        "late-attach-blank-ready-v2.ghostsnp",
        &blank_frames[0].bytes,
    );
    write_frame(
        "late-attach-blank-finish-v2.ghostsnp",
        &blank_frames[1].bytes,
    );
}
