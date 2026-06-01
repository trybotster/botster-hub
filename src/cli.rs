//! Minimal smoke/dev command adapter for `botster-hub`.
//!
//! Parsing stays deliberately small and dependency-free. Runtime and config
//! policy remain in the hub library modules this adapter calls.

use std::ffi::OsString;
use std::path::PathBuf;

use crate::config::{DataDirectoryOption, HubStartupOptions, RuntimeEnvironment};
use crate::core::{RunOneSmokeRequest, run_one_smoke};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliCommand {
    Version,
    Help,
    CheckConfig {
        data_dir: PathBuf,
    },
    RunOne {
        data_dir: PathBuf,
        working_directory: PathBuf,
        executable: String,
        arguments: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliOutput {
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
}

impl CliOutput {
    fn success(stdout: impl Into<String>) -> Self {
        Self {
            status: 0,
            stdout: stdout.into(),
            stderr: String::new(),
        }
    }

    fn failure(stderr: impl Into<String>) -> Self {
        Self {
            status: 1,
            stdout: String::new(),
            stderr: stderr.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliError {
    message: String,
}

impl CliError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CliError {}

pub fn run<I>(args: I, environment: &RuntimeEnvironment) -> CliOutput
where
    I: IntoIterator<Item = OsString>,
{
    match parse(args) {
        Ok(CliCommand::Version) => CliOutput::success(format!(
            "{} {}\n",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION")
        )),
        Ok(CliCommand::Help) => CliOutput::success(usage()),
        Ok(CliCommand::CheckConfig { data_dir }) => check_config(data_dir, environment),
        Ok(CliCommand::RunOne {
            data_dir,
            working_directory,
            executable,
            arguments,
        }) => run_one(
            data_dir,
            working_directory,
            executable,
            arguments,
            environment,
        ),
        Err(error) => CliOutput::failure(format!("{error}\n\n{}", usage())),
    }
}

pub fn parse<I>(args: I) -> Result<CliCommand, CliError>
where
    I: IntoIterator<Item = OsString>,
{
    let args: Vec<String> = args
        .into_iter()
        .map(|arg| {
            arg.into_string()
                .map_err(|_| CliError::new("arguments must be valid UTF-8"))
        })
        .collect::<Result<_, _>>()?;

    match args.first().map(String::as_str) {
        None | Some("--help") | Some("-h") | Some("help") => Ok(CliCommand::Help),
        Some("version") => parse_no_extra(&args, CliCommand::Version),
        Some("check-config") => parse_check_config(&args[1..]),
        Some("run-one") => parse_run_one(&args[1..]),
        Some(command) => Err(CliError::new(format!("unknown command: {command}"))),
    }
}

fn parse_no_extra(args: &[String], command: CliCommand) -> Result<CliCommand, CliError> {
    if args.len() == 1 {
        Ok(command)
    } else {
        Err(CliError::new(format!(
            "{} does not accept options or arguments",
            args[0]
        )))
    }
}

fn parse_check_config(args: &[String]) -> Result<CliCommand, CliError> {
    let mut parser = OptionParser::new(args);
    let data_dir = parser.required_path("--data-dir")?;
    parser.reject_remaining()?;

    Ok(CliCommand::CheckConfig { data_dir })
}

fn parse_run_one(args: &[String]) -> Result<CliCommand, CliError> {
    let separator = args
        .iter()
        .position(|arg| arg == "--")
        .ok_or_else(|| CliError::new("run-one requires -- before the executable"))?;
    let (option_args, command_args) = args.split_at(separator);
    let command_args = &command_args[1..];

    let mut parser = OptionParser::new(option_args);
    let data_dir = parser.required_path("--data-dir")?;
    let working_directory = parser
        .optional_path("--working-directory")?
        .unwrap_or_else(|| PathBuf::from("."));
    parser.reject_remaining()?;

    let executable = command_args
        .first()
        .cloned()
        .ok_or_else(|| CliError::new("run-one requires an executable after --"))?;
    let arguments = command_args[1..].to_vec();

    Ok(CliCommand::RunOne {
        data_dir,
        working_directory,
        executable,
        arguments,
    })
}

fn check_config(data_dir: PathBuf, environment: &RuntimeEnvironment) -> CliOutput {
    let options = HubStartupOptions {
        data_directory: DataDirectoryOption::Explicit(data_dir),
        ..HubStartupOptions::default()
    };

    match options.build_config_for_environment(environment) {
        Ok(config) => CliOutput::success(format!(
            "botster-hub config ok: host={} plugins={} providers={}\n",
            config.host.id,
            config.plugin_directories.len(),
            config.provider_directories.len()
        )),
        Err(error) => CliOutput::failure(format!("botster-hub config error: {error}\n")),
    }
}

fn run_one(
    data_dir: PathBuf,
    working_directory: PathBuf,
    executable: String,
    arguments: Vec<String>,
    environment: &RuntimeEnvironment,
) -> CliOutput {
    let options = HubStartupOptions {
        data_directory: DataDirectoryOption::Explicit(data_dir),
        ..HubStartupOptions::default()
    };

    if let Err(error) = options.build_config_for_environment(environment) {
        return CliOutput::failure(format!("botster-hub config error: {error}\n"));
    }

    match run_one_smoke(RunOneSmokeRequest {
        working_directory,
        executable,
        arguments,
    }) {
        Ok(report) => CliOutput::success(format!(
            "botster-hub run-one ok: spawned={} attached={} drained_bytes={} shutdown={}\n",
            report.spawned, report.attached, report.drained_bytes, report.shutdown
        )),
        Err(error) => CliOutput::failure(format!("botster-hub run-one error: {error}\n")),
    }
}

fn usage() -> String {
    format!(
        "\
Usage:
  {0} version
  {0} check-config --data-dir <path>
  {0} run-one --data-dir <path> [--working-directory <path>] -- <executable> [args...]
",
        env!("CARGO_PKG_NAME")
    )
}

struct OptionParser<'a> {
    args: &'a [String],
    consumed: Vec<bool>,
}

impl<'a> OptionParser<'a> {
    fn new(args: &'a [String]) -> Self {
        Self {
            args,
            consumed: vec![false; args.len()],
        }
    }

    fn required_path(&mut self, name: &str) -> Result<PathBuf, CliError> {
        self.optional_path(name)?
            .ok_or_else(|| CliError::new(format!("{name} is required")))
    }

    fn optional_path(&mut self, name: &str) -> Result<Option<PathBuf>, CliError> {
        let Some(position) = self.args.iter().position(|arg| arg == name) else {
            return Ok(None);
        };
        if self.consumed[position] {
            return Err(CliError::new(format!("{name} was provided more than once")));
        }
        let value_position = position + 1;
        let value = self
            .args
            .get(value_position)
            .ok_or_else(|| CliError::new(format!("{name} requires a value")))?;
        if value.starts_with("--") {
            return Err(CliError::new(format!("{name} requires a value")));
        }

        self.consumed[position] = true;
        self.consumed[value_position] = true;

        if self
            .args
            .iter()
            .enumerate()
            .any(|(index, arg)| index != position && !self.consumed[index] && arg == name)
        {
            return Err(CliError::new(format!("{name} was provided more than once")));
        }

        Ok(Some(PathBuf::from(value)))
    }

    fn reject_remaining(&self) -> Result<(), CliError> {
        if let Some((_, arg)) = self
            .args
            .iter()
            .enumerate()
            .find(|(index, _)| !self.consumed[*index])
        {
            Err(CliError::new(format!("unknown option or argument: {arg}")))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_args(args: &[&str]) -> Result<CliCommand, CliError> {
        parse(args.iter().map(OsString::from))
    }

    #[test]
    fn parse_version_command() {
        assert_eq!(
            parse_args(&["version"]).expect("parse"),
            CliCommand::Version
        );
    }

    #[test]
    fn parse_check_config_requires_explicit_data_dir() {
        assert!(parse_args(&["check-config"]).is_err());
        assert_eq!(
            parse_args(&["check-config", "--data-dir", "/tmp/hub"]).expect("parse"),
            CliCommand::CheckConfig {
                data_dir: PathBuf::from("/tmp/hub")
            }
        );
    }

    #[test]
    fn parse_run_one_requires_separator_and_executable() {
        assert!(parse_args(&["run-one", "--data-dir", "/tmp/hub", "sh"]).is_err());
        assert!(parse_args(&["run-one", "--data-dir", "/tmp/hub", "--"]).is_err());
        assert_eq!(
            parse_args(&[
                "run-one",
                "--data-dir",
                "/tmp/hub",
                "--",
                "sh",
                "-c",
                "printf ok"
            ])
            .expect("parse"),
            CliCommand::RunOne {
                data_dir: PathBuf::from("/tmp/hub"),
                working_directory: PathBuf::from("."),
                executable: "sh".to_string(),
                arguments: vec!["-c".to_string(), "printf ok".to_string()],
            }
        );
    }

    #[test]
    fn parse_rejects_unknown_command_or_option() {
        assert!(parse_args(&["unknown"]).is_err());
        assert!(parse_args(&["version", "--verbose"]).is_err());
        assert!(parse_args(&["check-config", "--data-dir", "/tmp/hub", "--x"]).is_err());
    }

    #[test]
    fn help_prints_usage_only() {
        let output = run(
            [OsString::from("--help")],
            &RuntimeEnvironment::from_values(None, None, None),
        );

        assert_eq!(output.status, 0);
        assert!(output.stdout.contains("Usage:"));
        assert!(!output.stdout.contains("/Users/"));
        assert!(output.stderr.is_empty());
    }

    #[test]
    fn version_command_reports_only_package_metadata() {
        let output = run(
            [OsString::from("version")],
            &RuntimeEnvironment::from_values(None, None, None),
        );

        assert_eq!(output.status, 0);
        assert_eq!(
            output.stdout,
            format!("{} {}\n", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
        );
        assert_scrubbed(&output.stdout);
        assert!(output.stderr.is_empty());
    }

    #[test]
    fn check_config_uses_explicit_data_dir_without_home() {
        let output = run(
            [
                OsString::from("check-config"),
                OsString::from("--data-dir"),
                OsString::from("/tmp/botster-hub-test-data"),
            ],
            &RuntimeEnvironment::from_values(None, None, None),
        );

        assert_eq!(output.status, 0);
        assert!(output.stdout.contains("botster-hub config ok"));
        assert_scrubbed(&output.stdout);
        assert!(output.stderr.is_empty());
    }

    #[test]
    fn check_config_missing_data_dir_does_not_fallback_to_home() {
        let output = run(
            [OsString::from("check-config")],
            &RuntimeEnvironment::from_values(None, None, Some(PathBuf::from("/tmp/home"))),
        );

        assert_ne!(output.status, 0);
        assert!(output.stderr.contains("--data-dir is required"));
        assert!(output.stdout.is_empty());
    }

    #[test]
    fn no_pii_output_for_parser_errors() {
        let output = run(
            [OsString::from("not-a-command")],
            &RuntimeEnvironment::from_values(
                Some(PathBuf::from("/tmp/data")),
                None,
                Some(PathBuf::from("/Users/example")),
            ),
        );

        assert_ne!(output.status, 0);
        assert_scrubbed(&output.stderr);
        assert!(output.stdout.is_empty());
    }

    fn assert_scrubbed(output: &str) {
        assert!(!output.contains("HOME"));
        assert!(!output.contains("/Users/"));
        assert!(!output.contains(env!("CARGO_MANIFEST_DIR")));
        assert!(!output.contains("BOTSTER_HUB_DATA_DIR"));
        assert!(!output.contains("XDG_DATA_HOME"));
    }
}
