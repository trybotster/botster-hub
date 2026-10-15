//! Repo tooling of botster-hub: `cargo xtask <command>`. The gate is `cargo xtask ci`.

mod budget;
mod ci;
mod contracts;
mod forbid_unsafe;
mod lists;
mod taint;
mod zig_packages;

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};

type CommandFn = fn(&Path, &[String]) -> Result<()>;

/// The commands, by name.
const COMMANDS: [(&str, CommandFn); 8] = [
    ("ci", ci::command),
    ("lists", lists::command),
    ("zig-packages", zig_packages::command),
    ("taint", taint::command),
    ("unsafe-code", forbid_unsafe::command),
    ("high-tier", botster_gate_kit::high_tier::command),
    ("test-budget", budget::command),
    ("slow", |root, _| {
        budget::command(root, &["--slow".to_string()])
    }),
];

/// The command that the arguments name, with its arguments.
///
/// # Errors
/// No argument, or an unknown name: the usage.
fn choose(args: &[String]) -> Result<(CommandFn, &[String]), String> {
    let usage = || {
        let names: Vec<&str> = COMMANDS.iter().map(|(name, _)| *name).collect();
        format!("usage: cargo xtask <{}>", names.join("|"))
    };
    let (name, rest) = args.split_first().ok_or_else(usage)?;
    COMMANDS
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, run)| (*run, rest))
        .ok_or_else(usage)
}

/// The repository root: the parent of the xtask manifest directory.
fn repo_root() -> Result<PathBuf> {
    let manifest =
        std::env::var("CARGO_MANIFEST_DIR").context("run xtask through `cargo xtask`")?;
    match Path::new(&manifest).parent() {
        Some(root) => Ok(root.to_path_buf()),
        None => bail!("no parent of {manifest}"),
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (run, rest) = choose(&args).map_err(anyhow::Error::msg)?;
    run(&repo_root()?, rest)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(words: &[&str]) -> Vec<String> {
        words.iter().map(|w| w.to_string()).collect()
    }

    #[test]
    fn a_name_picks_its_command_with_the_rest_of_the_arguments() {
        let given = args(&["test-budget", "--slow"]);
        let (run, rest) = choose(&given).unwrap();
        assert_eq!(run as usize, budget::command as CommandFn as usize);
        assert_eq!(rest, ["--slow".to_string()]);
        for (name, command) in COMMANDS {
            let given = args(&[name]);
            assert_eq!(
                choose(&given).unwrap().0 as usize,
                command as usize,
                "{name}"
            );
        }
    }

    #[test]
    fn no_name_or_an_unknown_one_gives_the_usage() {
        let usage = "usage: cargo xtask <ci|lists|zig-packages|taint|unsafe-code|high-tier|test-budget|slow>";
        assert_eq!(choose(&[]).err(), Some(usage.to_string()));
        assert_eq!(choose(&args(&["nope"])).err(), Some(usage.to_string()));
    }
}
