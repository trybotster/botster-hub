//! `botster-plugin-test --plugin <dir> <spec.lua>...`
//!
//! Runs Lua spec files against the real Hub plugin runtime. It prints one
//! TAP line per test and exits non-zero when any test fails.

use std::path::PathBuf;
use std::process::ExitCode;

use botster_plugin_test_kit::spec;

const USAGE: &str = "usage: botster-plugin-test [--plugin <dir>] <spec.lua>...";

fn main() -> ExitCode {
    let mut plugin_directory = PathBuf::from(".");
    let mut specs = Vec::new();
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--plugin" => match arguments.next() {
                Some(directory) => plugin_directory = PathBuf::from(directory),
                None => {
                    eprintln!("--plugin needs a directory\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--help" | "-h" => {
                println!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            flag if flag.starts_with("--") => {
                eprintln!("unknown option {flag}\n{USAGE}");
                return ExitCode::from(2);
            }
            _ => specs.push(PathBuf::from(argument)),
        }
    }
    if specs.is_empty() {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    }
    let mut failed = 0usize;
    let mut total = 0usize;
    for spec_path in &specs {
        for outcome in spec::run_spec_file(&plugin_directory, spec_path) {
            total += 1;
            match outcome.failure {
                None => println!("ok {total} - {}", outcome.name),
                Some(message) => {
                    failed += 1;
                    println!("not ok {total} - {}", outcome.name);
                    for line in message.lines() {
                        println!("  # {line}");
                    }
                }
            }
        }
    }
    println!("1..{total}");
    if failed == 0 {
        ExitCode::SUCCESS
    } else {
        eprintln!("{failed} of {total} tests failed");
        ExitCode::FAILURE
    }
}
