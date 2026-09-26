//! One key=value line to the Hub's local log (the daemon's stderr).
//!
//! Local only: these lines may carry full error text and paths that client
//! responses must not. Tests read them back from a process-wide capture,
//! because Core closures log from the data-plane thread.

/// Write one formatted line to the Hub log.
macro_rules! hub_log {
    ($($arg:tt)*) => {{
        let line = format!($($arg)*);
        #[cfg(test)]
        $crate::hub_log::capture(&line);
        eprintln!("{line}");
    }};
}
pub(crate) use hub_log;

#[cfg(test)]
static CAPTURED: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
pub(crate) fn capture(line: &str) {
    CAPTURED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(line.to_owned());
}

/// Captured lines containing every needle. Tests run in parallel, so each
/// test matches on an identifier only it uses.
#[cfg(test)]
pub(crate) fn captured_matching(needles: &[&str]) -> Vec<String> {
    CAPTURED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .filter(|line| needles.iter().all(|needle| line.contains(needle)))
        .cloned()
        .collect()
}
