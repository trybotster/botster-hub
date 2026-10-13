//! `cargo xtask lists`: the conformance lists of the minimum Hub (Stage 2 brief; lead 2026-10-12).
//!
//! - `conformance/minimum-hub.txt`: the selected ids, one row each (contract, id, clause, story, mark, reason).
//! - `conformance/hub-pending.txt`: the minimum rows that do not run yet (contract, id).
//! - `conformance/ledger-waits.txt`: the rows that the pinned contracts ledger does not hold with their contract yet, each
//!   with the amendment that fixes it.
//!
//! Each minimum row must be in the ledger of the pinned contracts tag with its contract, or be a listed wait, which the
//! check reports by name: never a pass, never a silent skip. A wait that the ledger already matches is refused, so it goes
//! when its amendment freezes. A row is its contract and id: the same id may be a row of two contracts (the ledger keys by bare id,
//! so HC and HP both name `conf::hs_1_hello_first`), but a row appears once. Every pending row must be a minimum row.

use crate::contracts;
use anyhow::{Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub const MINIMUM: &str = "conformance/minimum-hub.txt";
pub const PENDING: &str = "conformance/hub-pending.txt";
pub const WAITS: &str = "conformance/ledger-waits.txt";
/// The ledger, relative to the root of the pinned botster-contracts checkout.
pub const LEDGER: &str = "conformance/ledger.json";

/// A row of a tab-separated list: its contract and id, with its 1-based line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub line: usize,
    pub contract: String,
    pub id: String,
}

/// The data lines of a list: neither blank nor a `#` comment, with their 1-based line numbers.
fn data_lines(text: &str) -> impl Iterator<Item = (usize, &str)> {
    text.lines()
        .enumerate()
        .map(|(index, line)| (index + 1, line))
        .filter(|(_, line)| !line.trim().is_empty() && !line.starts_with('#'))
}

/// The rows of a list whose lines have exactly `columns` tab-separated fields, contract and id first.
///
/// # Errors
/// A line with another number of fields, or an empty contract or id.
pub fn parse_rows(name: &str, text: &str, columns: usize) -> Result<Vec<Row>, String> {
    data_lines(text)
        .map(|(line, text)| {
            let fields: Vec<&str> = text.split('\t').collect();
            if fields.len() != columns || fields[0].is_empty() || fields[1].is_empty() {
                return Err(format!(
                    "{name}:{line}: expected {columns} tab-separated fields with a contract and an id: {text:?}"
                ));
            }
            Ok(Row {
                line,
                contract: fields[0].to_string(),
                id: fields[1].to_string(),
            })
        })
        .collect()
}

/// The ledger's contract of each id (`{"ids": [{"id", "contract", ..}, ..]}`).
///
/// # Errors
/// The text is not such a document.
pub fn parse_ledger(json: &str) -> Result<BTreeMap<String, String>, String> {
    let doc: serde_json::Value =
        serde_json::from_str(json).map_err(|e| format!("the ledger is not JSON: {e}"))?;
    let ids = doc["ids"]
        .as_array()
        .ok_or("the ledger has no `ids` array")?;
    ids.iter()
        .map(
            |entry| match (entry["id"].as_str(), entry["contract"].as_str()) {
                (Some(id), Some(contract)) => Ok((id.to_string(), contract.to_string())),
                _ => Err(format!("a ledger entry has no id or contract: {entry}")),
            },
        )
        .collect()
}

/// Whether the ledger's contract `ledger` is the list's contract `listed`. The route codec (`codec`) is the extract of
/// the hub-client contract, so an HC row may be a codec id.
pub fn same_contract(listed: &str, ledger: &str) -> bool {
    match listed {
        "HP" => ledger == "hp",
        "HC" => ledger == "hc" || ledger == "codec",
        "LB" => ledger == "lb",
        "CLI" => ledger == "cli",
        "UI" => ledger == "ui",
        _ => false,
    }
}

/// The report of the lists: `Ok` with the summary lines (the waits named), or `Err` with every problem.
///
/// # Errors
/// Every problem that the check found, one a line.
pub fn verdict(
    minimum: &[Row],
    pending: &[Row],
    waits: &[Row],
    ledger: &BTreeMap<String, String>,
) -> Result<Vec<String>, Vec<String>> {
    let mut problems = Vec::new();
    let mut seen = BTreeSet::new();
    for row in minimum {
        if !seen.insert((row.contract.as_str(), row.id.as_str())) {
            problems.push(format!(
                "{MINIMUM}:{}: {} {} is listed twice",
                row.line, row.contract, row.id
            ));
        }
    }
    let mut reports = Vec::new();
    for row in minimum {
        let matched = ledger
            .get(&row.id)
            .is_some_and(|contract| same_contract(&row.contract, contract));
        let wait = waits
            .iter()
            .find(|w| w.id == row.id && w.contract == row.contract);
        match (matched, wait) {
            (true, None) => {}
            (true, Some(wait)) => problems.push(format!(
                "{WAITS}:{}: {} {} is in the ledger now: remove the wait",
                wait.line, wait.contract, wait.id
            )),
            (false, Some(wait)) => reports.push(format!(
                "lists: waits: {} {} ({WAITS}:{})",
                wait.contract, wait.id, wait.line
            )),
            (false, None) => problems.push(format!(
                "{MINIMUM}:{}: {} {} is not in the contracts ledger with that contract",
                row.line, row.contract, row.id
            )),
        }
    }
    for wait in waits {
        if !minimum
            .iter()
            .any(|row| row.id == wait.id && row.contract == wait.contract)
        {
            problems.push(format!(
                "{WAITS}:{}: {} {} is not a minimum row",
                wait.line, wait.contract, wait.id
            ));
        }
    }
    let mut pending_seen = BTreeSet::new();
    for row in pending {
        let key = (row.contract.as_str(), row.id.as_str());
        if !pending_seen.insert(key) {
            problems.push(format!(
                "{PENDING}:{}: {} {} is listed twice",
                row.line, row.contract, row.id
            ));
        }
        if !seen.contains(&key) {
            problems.push(format!(
                "{PENDING}:{}: {} {} is not a minimum row",
                row.line, row.contract, row.id
            ));
        }
    }
    if !problems.is_empty() {
        return Err(problems);
    }
    let runs = minimum.len() - pending_seen.len();
    reports.insert(
        0,
        format!(
            "lists: minimum {}, pending {}, running {runs}, waiting for an amendment {}",
            minimum.len(),
            pending_seen.len(),
            waits.len()
        ),
    );
    Ok(reports)
}

/// The check over the repository's lists and the pinned ledger.
///
/// # Errors
/// A file could not be read, or the check found a problem.
pub fn command(root: &Path, args: &[String]) -> Result<()> {
    if let Some(arg) = args.first() {
        anyhow::bail!("unknown argument '{arg}'");
    }
    let read = |path: &Path| {
        std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))
    };
    let ledger_path = contracts::root(root)?.join(LEDGER);
    let ledger = parse_ledger(&read(&ledger_path)?).map_err(anyhow::Error::msg)?;
    let minimum =
        parse_rows(MINIMUM, &read(&root.join(MINIMUM))?, 6).map_err(anyhow::Error::msg)?;
    let waits = parse_rows(WAITS, &read(&root.join(WAITS))?, 4).map_err(anyhow::Error::msg)?;
    let pending =
        parse_rows(PENDING, &read(&root.join(PENDING))?, 2).map_err(anyhow::Error::msg)?;
    match verdict(&minimum, &pending, &waits, &ledger) {
        Ok(lines) => {
            for line in lines {
                println!("{line}");
            }
            Ok(())
        }
        Err(problems) => anyhow::bail!(
            "{}\n{} problem(s) in the conformance lists",
            problems.join("\n"),
            problems.len()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(line: usize, contract: &str, id: &str) -> Row {
        Row {
            line,
            contract: contract.to_string(),
            id: id.to_string(),
        }
    }

    fn ledger() -> BTreeMap<String, String> {
        [
            ("conf::a", "hp"),
            ("conf::b", "hc"),
            ("conf::c", "codec"),
            ("conf::w", "hc"),
        ]
        .into_iter()
        .map(|(id, contract)| (id.to_string(), contract.to_string()))
        .collect()
    }

    fn pending(list: &[(&str, &str)]) -> Vec<Row> {
        list.iter()
            .enumerate()
            .map(|(index, (contract, id))| row(index + 1, contract, id))
            .collect()
    }

    #[test]
    fn rows_have_the_given_columns_and_skip_comments_and_blank_lines() {
        let text = "# head\n\nHP\tconf::a\tX-1\t0\t-\twhy\nHC\tconf::b\tX-2\t1\tW\twhy\n";
        assert_eq!(
            parse_rows("m", text, 6),
            Ok(vec![row(3, "HP", "conf::a"), row(4, "HC", "conf::b")])
        );
        for bad in [
            "HP\tconf::a\tX-1\t0\t-\n",
            "HP\tconf::a\tX-1\t0\t-\twhy\textra\n",
            "\tconf::a\tX-1\t0\t-\twhy\n",
            "HP\t\tX-1\t0\t-\twhy\n",
        ] {
            let error = parse_rows("m", bad, 6).unwrap_err();
            assert!(error.starts_with("m:1: expected 6 "), "{error}");
        }
        assert_eq!(
            parse_rows("p", "# c\nHP\tconf::a\n\nHC\tconf::b\n", 2),
            Ok(vec![row(2, "HP", "conf::a"), row(4, "HC", "conf::b")])
        );
    }

    #[test]
    fn the_ledger_maps_each_id_to_its_contract() {
        let json = r#"{"version": 1, "ids": [{"id": "conf::a", "contract": "hp", "clause": "X"}, {"id": "conf::c", "contract": "codec"}]}"#;
        let ledger = parse_ledger(json).unwrap();
        assert_eq!(ledger.len(), 2);
        assert_eq!(ledger["conf::a"], "hp");
        assert_eq!(ledger["conf::c"], "codec");
        assert!(parse_ledger("{")
            .unwrap_err()
            .starts_with("the ledger is not JSON"));
        assert_eq!(
            parse_ledger(r#"{"version": 1}"#),
            Err("the ledger has no `ids` array".to_string())
        );
        assert!(parse_ledger(r#"{"ids": [{"id": "conf::a"}]}"#)
            .unwrap_err()
            .starts_with("a ledger entry has no id or contract"));
    }

    #[test]
    fn each_listed_contract_names_its_ledger_contracts() {
        for (listed, ledger) in [
            ("HP", "hp"),
            ("HC", "hc"),
            ("HC", "codec"),
            ("LB", "lb"),
            ("CLI", "cli"),
            ("UI", "ui"),
        ] {
            assert!(same_contract(listed, ledger), "{listed} {ledger}");
        }
        for (listed, ledger) in [
            ("HP", "hc"),
            ("HC", "hp"),
            ("LB", "hp"),
            ("CLI", "ui"),
            ("UI", "cli"),
            ("hp", "hp"),
            ("CORE", "core"),
        ] {
            assert!(!same_contract(listed, ledger), "{listed} {ledger}");
        }
    }

    #[test]
    fn a_full_list_passes_and_names_each_wait() {
        let minimum = [
            row(1, "HP", "conf::a"),
            row(2, "HC", "conf::b"),
            row(3, "HC", "conf::c"),
            row(4, "HP", "conf::w"),
        ];
        let waits = [row(5, "HP", "conf::w")];
        assert_eq!(
            verdict(&minimum, &pending(&[("HP", "conf::w")]), &waits, &ledger()),
            Ok(vec![
                "lists: minimum 4, pending 1, running 3, waiting for an amendment 1".to_string(),
                format!("lists: waits: HP conf::w ({WAITS}:5)"),
            ])
        );
    }

    #[test]
    fn every_problem_is_reported() {
        let minimum = [
            row(1, "HP", "conf::a"),
            row(2, "HP", "conf::a"),
            row(3, "HP", "conf::b"),
            row(4, "HC", "conf::missing"),
            row(5, "HP", "conf::w"),
        ];
        // conf::a matches the ledger, so its wait is stale; the second wait names no minimum row.
        let waits = [row(7, "HP", "conf::a"), row(8, "HC", "conf::z")];
        let problems = verdict(
            &minimum,
            &pending(&[
                ("HP", "conf::a"),
                ("HP", "conf::a"),
                ("HP", "conf::nope"),
                ("HC", "conf::a"),
            ]),
            &waits,
            &ledger(),
        )
        .unwrap_err();
        assert_eq!(
            problems,
            [
                format!("{MINIMUM}:2: HP conf::a is listed twice"),
                format!("{WAITS}:7: HP conf::a is in the ledger now: remove the wait"),
                format!("{WAITS}:7: HP conf::a is in the ledger now: remove the wait"),
                format!("{MINIMUM}:3: HP conf::b is not in the contracts ledger with that contract"),
                format!("{MINIMUM}:4: HC conf::missing is not in the contracts ledger with that contract"),
                format!("{MINIMUM}:5: HP conf::w is not in the contracts ledger with that contract"),
                format!("{WAITS}:8: HC conf::z is not a minimum row"),
                format!("{PENDING}:2: HP conf::a is listed twice"),
                format!("{PENDING}:3: HP conf::nope is not a minimum row"),
                format!("{PENDING}:4: HC conf::a is not a minimum row"),
            ]
        );
    }

    /// The repository's own lists parse, every pending row is a minimum row, and each wait names a minimum row (the
    /// ledger part runs in the gate step, which has the pinned contracts checkout). The synthetic ledger keys by bare id,
    /// as the real one does.
    #[test]
    fn the_repository_lists_are_consistent() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let read = |name: &str| std::fs::read_to_string(root.join(name)).unwrap();
        let minimum = parse_rows(MINIMUM, &read(MINIMUM), 6).unwrap();
        let waits = parse_rows(WAITS, &read(WAITS), 4).unwrap();
        let pending = parse_rows(PENDING, &read(PENDING), 2).unwrap();
        // A ledger that holds every row but the waits: then only the waits are reported.
        let ledger: BTreeMap<String, String> = minimum
            .iter()
            .filter(|row| {
                !waits
                    .iter()
                    .any(|w| w.id == row.id && w.contract == row.contract)
            })
            .map(|row| {
                let contract = if row.contract == "HC" {
                    "hc"
                } else {
                    &row.contract
                };
                (row.id.clone(), contract.to_lowercase())
            })
            .collect();
        let report = verdict(&minimum, &pending, &waits, &ledger).unwrap();
        assert_eq!(report.len(), 1 + waits.len(), "{report:?}");
        assert_eq!(minimum.len(), 127);
    }
}
