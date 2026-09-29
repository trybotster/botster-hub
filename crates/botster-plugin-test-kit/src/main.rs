//! `botster-plugin-test --plugin <dir> <spec.lua>...`
//!
//! Runs Lua spec files against the real Hub plugin runtime. It prints one
//! TAP line per test and exits non-zero when any test fails.

use std::path::PathBuf;
use std::process::ExitCode;

use botster_plugin_test_kit::e2e;
use botster_plugin_test_kit::spec::{self, Mode};

const USAGE: &str = "usage: botster-plugin-test [--e2e] [--plugin <dir>] <spec.lua>...\n       botster-plugin-test --conformance\n\n--e2e          run against a real botster-hub daemon process (needs BOTSTER_HUB_BIN,\n               BOTSTER_SESSION_WORKER_BIN, and BOTSTER_CANDIDATE_MANIFEST)\n--conformance  run the Hub's plugin contract matrix conformance on a real daemon";

fn main() -> ExitCode {
    let mut plugin_directory = PathBuf::from(".");
    let mut specs = Vec::new();
    let mut mode = Mode::InProcess;
    let mut conformance = false;
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
            "--e2e" => mode = Mode::E2e,
            "--conformance" => conformance = true,
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
    if conformance {
        return match e2e::run_conformance() {
            Ok(report) => {
                println!("{report}");
                ExitCode::SUCCESS
            }
            Err(message) => {
                eprintln!("{message}");
                ExitCode::FAILURE
            }
        };
    }
    if specs.is_empty() {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    }
    let mut failed = 0usize;
    let mut total = 0usize;
    for spec_path in &specs {
        for outcome in spec::run_spec_file_in(mode, &plugin_directory, spec_path) {
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
