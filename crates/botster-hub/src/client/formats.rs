//! `welcome.terminal_format` (HC HS-3; steward R-124): the first entry of the client's list that **every** current
//! worker supports. A current worker is a session whose worker has announced its formats: a non-empty
//! `snapshot_formats(session)`. A starting session (no `Launched` and no adoption yet) is left out; it does not count as
//! supporting nothing. With no announced session, the field is absent.
//!
//! The owner computes the common set when a session changes state and stores it here; `io` reads it at `hello`.

use botster_core::contract::prelude::{CoreError, SnapshotFormat};
use std::sync::Mutex;

/// The formats that every announced worker supports, by Core's `terminal_format` names; `None` when no worker has
/// announced its formats.
#[derive(Debug, Default)]
pub struct CommonFormats(Mutex<Option<Vec<String>>>);

impl CommonFormats {
    pub fn set(&self, common: Option<Vec<String>>) {
        *self.0.lock().unwrap_or_else(|p| p.into_inner()) = common;
    }

    /// The first of `wanted` that every announced worker supports.
    pub fn pick(&self, wanted: &[String]) -> Option<String> {
        let common = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let common = common.as_ref()?;
        wanted.iter().find(|name| common.contains(name)).cloned()
    }
}

/// The intersection of the announced sessions' formats. A session whose read fails or is empty is left out.
pub fn common_formats(
    announced: impl Iterator<Item = Result<Vec<SnapshotFormat>, CoreError>>,
) -> Option<Vec<String>> {
    let mut common: Option<Vec<String>> = None;
    for formats in announced.filter_map(Result::ok).filter(|f| !f.is_empty()) {
        let names: Vec<String> = formats
            .iter()
            .map(botster_core_link::route::terminal_format)
            .collect();
        common = Some(match common {
            None => names,
            Some(set) => set.into_iter().filter(|n| names.contains(n)).collect(),
        });
    }
    common
}

#[cfg(test)]
mod tests {
    use super::*;
    use botster_core::contract::prelude::ErrorCode;

    fn format(name: &str, version: u32) -> SnapshotFormat {
        SnapshotFormat {
            name: name.into(),
            version,
        }
    }

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn no_announced_session_gives_no_format() {
        assert_eq!(common_formats(std::iter::empty()), None);
        let starting = vec![Ok(vec![])];
        assert_eq!(common_formats(starting.into_iter()), None);
        let shared = CommonFormats::default();
        assert_eq!(shared.pick(&names(&["ghostsnp/1+raw"])), None);
    }

    #[test]
    fn a_starting_session_does_not_empty_the_set_of_an_announced_one() {
        let sessions = vec![
            Ok(vec![format("GHOSTSNP", 1), format("GHOSTSNP", 2)]),
            Ok(vec![]),
            Err(CoreError::new(ErrorCode::UnknownSession, "gone")),
        ];
        let common = common_formats(sessions.into_iter());
        assert_eq!(common, Some(names(&["ghostsnp/1+raw", "ghostsnp/2+raw"])));
        let shared = CommonFormats::default();
        shared.set(common);
        assert_eq!(
            shared.pick(&names(&["vt/1+raw", "ghostsnp/2+raw", "ghostsnp/1+raw"])),
            Some("ghostsnp/2+raw".into()),
            "the client's order decides"
        );
    }

    #[test]
    fn every_announced_worker_must_support_the_pick() {
        let sessions = vec![
            Ok(vec![format("GHOSTSNP", 1), format("GHOSTSNP", 2)]),
            Ok(vec![format("GHOSTSNP", 1)]),
        ];
        let shared = CommonFormats::default();
        shared.set(common_formats(sessions.into_iter()));
        assert_eq!(
            shared.pick(&names(&["ghostsnp/2+raw", "ghostsnp/1+raw"])),
            Some("ghostsnp/1+raw".into())
        );
        let disjoint = vec![
            Ok(vec![format("GHOSTSNP", 2)]),
            Ok(vec![format("GHOSTSNP", 1)]),
        ];
        shared.set(common_formats(disjoint.into_iter()));
        assert_eq!(
            shared.pick(&names(&["ghostsnp/2+raw", "ghostsnp/1+raw"])),
            None
        );
    }
}
