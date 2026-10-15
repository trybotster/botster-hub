//! `cargo xtask zig-packages`: the Zig packages that ci/zig-prefetch/build.zig.zon lists are the ones that botster-core's
//! pin needs (orchestrator, 2026-10-15). The public fetch reads that list and not botster-core's checkout (the pool runs the
//! private and the public fetch independently), so a botster-core bump that changes the packages must change the list in
//! the same PR. This step has the checkout: cargo fetched it into `CARGO_HOME`. It fails when
//!
//! - the set of hashes differs from `ZIG_PACKAGES` in botster-terminal-ghostty's `build_data.rs`, or
//! - a package that a `build.zig.zon` of the Ghostty source names (`ZIG_PACKAGES_IN_ZON`) has another URL there.
//!
//! The nested packages are named only inside the package archives; Zig checks each download against its hash, so a wrong
//! URL fails the fetch.

use anyhow::{bail, Context, Result};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The file of the Hub's list, relative to the repository root.
const LIST: &str = "ci/zig-prefetch/build.zig.zon";

/// The `(url, hash)` pairs of a `build.zig.zon` text: each `.url = "..."` line followed by its `.hash = "..."` line.
pub fn packages(zon: &str) -> Vec<(String, String)> {
    let quoted = |line: &str, key: &str| {
        line.trim()
            .strip_prefix(key)?
            .trim_start()
            .strip_prefix('=')?
            .trim_start()
            .strip_prefix('"')?
            .split('"')
            .next()
            .map(str::to_owned)
    };
    let mut out = Vec::new();
    let mut url = None;
    for line in zon.lines() {
        if let Some(u) = quoted(line, ".url") {
            url = Some(u);
        } else if let Some(hash) = quoted(line, ".hash") {
            if let Some(url) = url.take() {
                out.push((url, hash));
            }
        }
    }
    out
}

/// The quoted entries of `pub const <name>: &[&str] = &[ ... ];` in a Rust source.
pub fn const_list(source: &str, name: &str) -> Vec<String> {
    let mut lines = source
        .lines()
        .skip_while(|line| !line.contains(&format!("pub const {name}:")));
    lines.next();
    lines
        .take_while(|line| !line.starts_with("];"))
        .filter_map(|line| {
            line.trim()
                .strip_prefix('"')?
                .strip_suffix("\",")
                .map(str::to_owned)
        })
        .collect()
}

/// The problems of the Hub's list against botster-core's data: `ours` is the Hub's `(url, hash)` pairs, `build_data` is
/// `build_data.rs`, and `ghostty_zon` is Ghostty's `build.zig.zon`.
pub fn problems(ours: &[(String, String)], build_data: &str, ghostty_zon: &str) -> Vec<String> {
    let needed: BTreeSet<String> = const_list(build_data, "ZIG_PACKAGES").into_iter().collect();
    let listed: BTreeSet<String> = ours.iter().map(|(_, hash)| hash.clone()).collect();
    let mut out = Vec::new();
    if needed.is_empty() {
        out.push("build_data.rs lists no ZIG_PACKAGES".to_owned());
    }
    for hash in needed.difference(&listed) {
        out.push(format!(
            "{hash} is needed by botster-core and not in {LIST}"
        ));
    }
    for hash in listed.difference(&needed) {
        out.push(format!(
            "{hash} is in {LIST} and not needed by botster-core"
        ));
    }
    let theirs = packages(ghostty_zon);
    for hash in const_list(build_data, "ZIG_PACKAGES_IN_ZON") {
        let ours_url = ours.iter().find(|(_, h)| *h == hash).map(|(u, _)| u);
        let their_url = theirs.iter().find(|(_, h)| *h == hash).map(|(u, _)| u);
        match (ours_url, their_url) {
            (Some(a), Some(b)) if a == b => {}
            (Some(a), Some(b)) => out.push(format!("{hash}: {LIST} has {a}, Ghostty has {b}")),
            (_, None) => out.push(format!("{hash} is not in Ghostty's build.zig.zon")),
            (None, _) => {} // reported above as a missing hash
        }
    }
    out
}

/// The commits of botster-core that Cargo.lock names for the crate botster-terminal-ghostty, the crate that builds
/// libghostty. (botster-gate-kit comes from another commit of the same repository; its Zig packages do not matter.)
pub fn core_commits(lock: &str) -> BTreeSet<String> {
    const SOURCE: &str = "source = \"git+https://github.com/trybotster/botster-core?rev=";
    let mut out = BTreeSet::new();
    for package in lock.split("[[package]]") {
        if !package
            .lines()
            .any(|l| l.trim() == "name = \"botster-terminal-ghostty\"")
        {
            continue;
        }
        for line in package.lines() {
            let Some(rest) = line.strip_prefix(SOURCE) else {
                continue;
            };
            if let Some(sha) = rest.split('#').nth(1).map(|s| s.trim_end_matches('"')) {
                if sha.len() >= 7 && sha.bytes().all(|b| b.is_ascii_hexdigit()) {
                    out.insert(sha.to_owned());
                }
            }
        }
    }
    out
}

/// Every `build.zig.zon` under `dir`, as one text: Ghostty's source names the packages in several of them.
fn zon_texts(dir: &Path) -> Result<String> {
    let mut text = String::new();
    let mut todo = vec![dir.to_path_buf()];
    while let Some(dir) = todo.pop() {
        for entry in std::fs::read_dir(&dir).with_context(|| format!("read {}", dir.display()))? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                todo.push(path);
            } else if entry.file_name() == "build.zig.zon" {
                text.push_str(&std::fs::read_to_string(&path)?);
                text.push('\n');
            }
        }
    }
    Ok(text)
}

/// The crate directories of botster-terminal-ghostty in cargo's checkouts of these commits.
fn ghostty_dirs(cargo_home: &Path, commits: &BTreeSet<String>) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(checkouts) = std::fs::read_dir(cargo_home.join("git/checkouts")) else {
        return out;
    };
    for repo in checkouts.flatten() {
        if !repo
            .file_name()
            .to_string_lossy()
            .starts_with("botster-core-")
        {
            continue;
        }
        for sha in commits {
            let dir = repo
                .path()
                .join(&sha[..7])
                .join("crates/botster-terminal-ghostty");
            if dir.join("build_data.rs").is_file() {
                out.push(dir);
            }
        }
    }
    out
}

/// The check over the repository: the list against the botster-terminal-ghostty crate of each botster-core commit that
/// Cargo.lock names and that cargo has checked out. With no checkout the step fails: a check that cannot read its input has
/// not run.
///
/// # Errors
/// A file could not be read, no checkout was found, or the list differs.
pub fn command(root: &Path, args: &[String]) -> Result<()> {
    if let Some(arg) = args.first() {
        bail!("unknown argument '{arg}'");
    }
    let read = |path: &Path| {
        std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))
    };
    let ours = packages(&read(&root.join(LIST))?);
    if ours.is_empty() {
        bail!("{LIST} lists no package");
    }
    let commits = core_commits(&read(&root.join("Cargo.lock"))?);
    let cargo_home = std::env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cargo")))
        .context("neither CARGO_HOME nor HOME is set")?;
    let dirs = ghostty_dirs(&cargo_home, &commits);
    if dirs.is_empty() {
        bail!(
            "no cargo checkout of botster-terminal-ghostty for the botster-core commits of Cargo.lock under {}",
            cargo_home.display()
        );
    }
    let mut all = Vec::new();
    for dir in &dirs {
        let build_data = read(&dir.join("build_data.rs"))?;
        let zon = zon_texts(&dir.join("vendor/ghostty"))?;
        for problem in problems(&ours, &build_data, &zon) {
            all.push(format!("{}: {problem}", dir.display()));
        }
    }
    if !all.is_empty() {
        bail!(
            "{LIST} differs from botster-core's pin:\n{}",
            all.join("\n")
        );
    }
    println!(
        "zig-packages: {} packages match botster-core at {} checkout(s)",
        ours.len(),
        dirs.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ZON: &str = r#"
        .a = .{
            .url = "https://x/a.tar.gz",
            .hash = "a-1",
        },
        .b = .{
            .url = "https://x/b.tar.gz",
            .hash = "b-1",
        },
"#;

    const DATA: &str = "pub const ZIG_PACKAGES: &[&str] = &[\n    \"a-1\",\n    \"b-1\",\n];\n\npub const ZIG_PACKAGES_IN_ZON: &[&str] = &[\n    \"a-1\",\n];\n";

    #[test]
    fn a_zon_gives_its_url_and_hash_pairs() {
        assert_eq!(
            packages(ZON),
            [
                ("https://x/a.tar.gz".to_owned(), "a-1".to_owned()),
                ("https://x/b.tar.gz".to_owned(), "b-1".to_owned())
            ]
        );
        assert_eq!(const_list(DATA, "ZIG_PACKAGES"), ["a-1", "b-1"]);
        assert_eq!(const_list(DATA, "ZIG_PACKAGES_IN_ZON"), ["a-1"]);
    }

    #[test]
    fn a_matching_list_has_no_problem() {
        assert_eq!(problems(&packages(ZON), DATA, ZON), Vec::<String>::new());
    }

    #[test]
    fn a_missing_extra_or_changed_package_is_named() {
        let ours = packages(ZON);
        let more = format!("{DATA}\n").replace("\"b-1\",", "\"b-1\",\n    \"c-1\",");
        assert_eq!(
            problems(&ours, &more, ZON),
            ["c-1 is needed by botster-core and not in ci/zig-prefetch/build.zig.zon"]
        );
        let fewer = DATA.replace("    \"b-1\",\n", "");
        assert_eq!(
            problems(&ours, &fewer, ZON),
            ["b-1 is in ci/zig-prefetch/build.zig.zon and not needed by botster-core"]
        );
        let moved = ZON.replace("https://x/a.tar.gz", "https://y/a.tar.gz");
        assert_eq!(
            problems(&ours, DATA, &moved),
            ["a-1: ci/zig-prefetch/build.zig.zon has https://x/a.tar.gz, Ghostty has https://y/a.tar.gz"]
        );
        assert_eq!(
            problems(&ours, DATA, ""),
            ["a-1 is not in Ghostty's build.zig.zon"]
        );
        assert_eq!(
            problems(&ours, "", ZON),
            [
                "build_data.rs lists no ZIG_PACKAGES",
                "a-1 is in ci/zig-prefetch/build.zig.zon and not needed by botster-core",
                "b-1 is in ci/zig-prefetch/build.zig.zon and not needed by botster-core"
            ]
        );
    }

    #[test]
    fn cargo_lock_names_the_commit_of_the_ghostty_crate_only() {
        let lock = "[[package]]\nname = \"botster-gate-kit\"\nsource = \"git+https://github.com/trybotster/botster-core?rev=28a29d32d191604221eb4cbeeb02e47371fa3daf#28a29d32d191604221eb4cbeeb02e47371fa3daf\"\n\n[[package]]\nname = \"botster-terminal-ghostty\"\nsource = \"git+https://github.com/trybotster/botster-core?rev=644f632f194f62ad7fe14e66482ee01083706e30#644f632f194f62ad7fe14e66482ee01083706e30\"\n";
        assert_eq!(
            core_commits(lock).into_iter().collect::<Vec<_>>(),
            ["644f632f194f62ad7fe14e66482ee01083706e30"]
        );
    }

    /// The repository's own list passes against the checkout that this machine has (the same input as the gate step).
    #[test]
    fn the_repository_list_matches_the_pinned_botster_core() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        command(root, &[]).unwrap();
    }
}
