use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::thread;

use signal_hook::consts::signal::SIGINT;
use signal_hook::iterator::{Handle as SignalHandle, Signals};

use crate::CommandOutcome;

pub(crate) const PROMPT: &str = "botster-hub> ";

const SIGNAL_MODE_IDLE: usize = 0;
const SIGNAL_MODE_INLINE: usize = 1;
const SIGNAL_MODE_FOREGROUND: usize = 2;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CommandMode {
    Inline,
    Foreground,
    ResolveApp(String),
    Stop,
    ExternalOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ConsoleAction {
    Empty,
    Help,
    Exit,
    Command(CommandMode),
}

pub(crate) struct ConsoleSignals {
    interrupted: Arc<AtomicBool>,
    mode: Arc<AtomicUsize>,
    foreground_child_pid: Arc<AtomicU32>,
    handle: SignalHandle,
    thread: Option<thread::JoinHandle<()>>,
}

pub(crate) struct ForegroundChildRegistration<'a> {
    signals: &'a ConsoleSignals,
    pid: u32,
}

impl ConsoleSignals {
    pub(crate) fn install() -> io::Result<Self> {
        let mut signals = Signals::new([SIGINT])?;
        let handle = signals.handle();
        let interrupted = Arc::new(AtomicBool::new(false));
        let mode = Arc::new(AtomicUsize::new(SIGNAL_MODE_IDLE));
        let foreground_child_pid = Arc::new(AtomicU32::new(0));
        let thread_interrupted = Arc::clone(&interrupted);
        let thread_mode = Arc::clone(&mode);
        let thread_foreground_child_pid = Arc::clone(&foreground_child_pid);
        let thread = thread::spawn(move || {
            for _ in signals.forever() {
                thread_interrupted.store(true, Ordering::SeqCst);
                let mode = thread_mode.load(Ordering::SeqCst);
                if mode == SIGNAL_MODE_INLINE {
                    eprintln!("\ninterrupt requested; finishing safely");
                } else if mode == SIGNAL_MODE_FOREGROUND {
                    forward_interrupt_to_foreground_child(&thread_foreground_child_pid);
                }
            }
        });
        Ok(Self {
            interrupted,
            mode,
            foreground_child_pid,
            handle,
            thread: Some(thread),
        })
    }

    pub(crate) fn begin_startup_or_inline(&self) {
        self.interrupted.store(false, Ordering::SeqCst);
        self.mode.store(SIGNAL_MODE_INLINE, Ordering::SeqCst);
    }

    fn begin_foreground(&self) {
        self.interrupted.store(false, Ordering::SeqCst);
        self.foreground_child_pid.store(0, Ordering::SeqCst);
        self.mode.store(SIGNAL_MODE_FOREGROUND, Ordering::SeqCst);
    }

    pub(crate) fn begin_idle(&self) {
        self.interrupted.store(false, Ordering::SeqCst);
        self.mode.store(SIGNAL_MODE_IDLE, Ordering::SeqCst);
    }

    fn take_interrupt(&self) -> bool {
        self.interrupted.swap(false, Ordering::SeqCst)
    }

    pub(crate) fn register_foreground_child(&self, pid: u32) -> ForegroundChildRegistration<'_> {
        self.foreground_child_pid.store(pid, Ordering::SeqCst);
        if self.interrupted.load(Ordering::SeqCst) {
            forward_interrupt_to_foreground_child(&self.foreground_child_pid);
        }
        ForegroundChildRegistration { signals: self, pid }
    }
}

impl Drop for ForegroundChildRegistration<'_> {
    fn drop(&mut self) {
        let _ = self.signals.foreground_child_pid.compare_exchange(
            self.pid,
            0,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }
}

fn forward_interrupt_to_foreground_child(foreground_child_pid: &AtomicU32) {
    let pid = foreground_child_pid.load(Ordering::SeqCst);
    if pid <= 1 {
        return;
    }
    unsafe {
        // SAFETY: the registered PID belongs to the foreground child and remains unreaped while
        // registered. SIGINT is idempotent with the terminal's normal foreground-group delivery.
        libc::kill(pid as libc::pid_t, libc::SIGINT);
    }
}

impl Drop for ConsoleSignals {
    fn drop(&mut self) {
        self.handle.close();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub(crate) fn parse_line(line: &str) -> Result<Vec<String>, shell_words::ParseError> {
    shell_words::split(line)
}

pub(crate) fn command_action(words: &[String]) -> ConsoleAction {
    let Some(command) = words.first().map(String::as_str) else {
        return ConsoleAction::Empty;
    };
    match command {
        "help" | "--help" | "-h" => ConsoleAction::Help,
        "exit" => ConsoleAction::Exit,
        "down" | "shutdown" => ConsoleAction::Command(CommandMode::Stop),
        "start" | "mcp-serve" | "inspect" | "run-one" => {
            ConsoleAction::Command(CommandMode::ExternalOnly)
        }
        "sessions" if words.get(1).map(String::as_str) == Some("attach") => {
            ConsoleAction::Command(CommandMode::ExternalOnly)
        }
        "open" if words.get(1).map(String::as_str) == Some("web") => ConsoleAction::Command(
            CommandMode::ResolveApp("botster-web/web-client".to_string()),
        ),
        "open" if words.get(1).map(String::as_str) == Some("tui") => {
            ConsoleAction::Command(CommandMode::ResolveApp("botster-tui".to_string()))
        }
        "open" => ConsoleAction::Command(CommandMode::Inline),
        "apps" if words.get(1).map(String::as_str) == Some("open") && words.get(2).is_some() => {
            ConsoleAction::Command(CommandMode::ResolveApp(words[2].clone()))
        }
        "up" | "doctor" | "smoke" | "status" | "sessions" | "session-templates"
        | "spawn-targets" | "context" | "reload" | "apps" | "packages" | "providers" => {
            ConsoleAction::Command(CommandMode::Inline)
        }
        _ => ConsoleAction::Command(CommandMode::ExternalOnly),
    }
}

pub(crate) fn contains_data_dir_override(words: &[String]) -> bool {
    words
        .iter()
        .take_while(|word| word.as_str() != "--")
        .any(|word| word == "--data-dir")
}

pub(crate) fn exact_external_invocation(
    words: &[String],
    canonicalize: impl FnOnce(&str, Vec<String>) -> Result<Vec<String>, String>,
) -> String {
    let Some(command) = words.first() else {
        return "botster-hub".to_string();
    };
    let args = canonicalize(command, words[1..].to_vec()).unwrap_or_else(|_| words[1..].to_vec());
    std::iter::once("botster-hub".to_string())
        .chain(std::iter::once(command.clone()))
        .chain(args)
        .map(|word| shell_words::quote(&word).into_owned())
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn print_intro(
    data_directory: &Path,
    daemon_state: &str,
    packages: &[crate::ConsolePackageState],
) {
    println!("data_dir=resolved:{}", data_directory.display());
    println!("daemon={daemon_state}");
    for package_name in ["botster-web", "botster-tui"] {
        match packages
            .iter()
            .find(|package| package.package_name == package_name)
        {
            Some(package) => println!("prerequisite {package_name}={}", package.state),
            None => {
                println!("prerequisite {package_name}=missing");
                println!(
                    "  packages install --path /path/to/{package_name}; packages enable {package_name}"
                );
            }
        }
    }
    print_help();
}

pub(crate) fn print_help() {
    println!(
        "commands: status doctor packages ... apps ... sessions ... up down help exit\n\
         external-only: start, mcp-serve, sessions attach, inspect, run-one"
    );
}

pub(crate) fn run(
    data_directory: PathBuf,
    signals: &ConsoleSignals,
    mut resolve_app_mode: impl FnMut(&str) -> Result<CommandMode, String>,
    mut canonicalize: impl FnMut(&str, Vec<String>) -> Result<Vec<String>, String>,
    mut dispatch: impl FnMut(&str, Vec<String>, &ConsoleSignals) -> Result<CommandOutcome, String>,
) -> io::Result<()> {
    let stdin = io::stdin();
    let stdin_fd = stdin.as_raw_fd();
    let mut line = Vec::new();

    signals.begin_idle();
    print_prompt()?;
    loop {
        if signals.take_interrupt() {
            line.clear();
            println!();
            print_prompt()?;
        }

        let mut poll_fd = libc::pollfd {
            fd: stdin_fd,
            events: libc::POLLIN | libc::POLLHUP,
            revents: 0,
        };
        let poll_result = unsafe {
            // SAFETY: poll_fd points to one initialized pollfd for the duration of this call.
            libc::poll(&mut poll_fd, 1, 50)
        };
        if poll_result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if poll_result == 0 {
            continue;
        }

        let mut byte = 0_u8;
        let read_result = unsafe {
            // SAFETY: byte is a valid one-byte output buffer and stdin_fd is borrowed, not closed.
            libc::read(stdin_fd, (&mut byte as *mut u8).cast(), 1)
        };
        if read_result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if read_result == 0 {
            println!("detached=daemon_running");
            return Ok(());
        }
        if byte != b'\n' {
            line.push(byte);
            continue;
        }

        let input = String::from_utf8_lossy(&line).into_owned();
        line.clear();
        let words = match parse_line(&input) {
            Ok(words) => words,
            Err(error) => {
                eprintln!("console parse error: {error}");
                print_prompt()?;
                continue;
            }
        };
        if words.first().map(String::as_str) == Some("botster-hub") {
            eprintln!("console error: omit the repeated `botster-hub` prefix");
            print_prompt()?;
            continue;
        }
        if contains_data_dir_override(&words) {
            eprintln!(
                "console error: this console is pinned to {}; run an explicit botster-hub command for another data directory",
                data_directory.display()
            );
            print_prompt()?;
            continue;
        }

        let action = command_action(&words);
        match action {
            ConsoleAction::Empty => {
                print_prompt()?;
                continue;
            }
            ConsoleAction::Help => {
                print_help();
                print_prompt()?;
                continue;
            }
            ConsoleAction::Exit => {
                println!("detached=daemon_running");
                return Ok(());
            }
            ConsoleAction::Command(CommandMode::ExternalOnly) => {
                let invocation = exact_external_invocation(&words, |command, args| {
                    canonicalize_with_pinned_data_dir(
                        command,
                        args,
                        &data_directory,
                        &mut canonicalize,
                    )
                });
                eprintln!(
                    "console error: command owns an external runtime or stdin; run `{invocation}` outside the console"
                );
                print_prompt()?;
                continue;
            }
            ConsoleAction::Command(mode) => {
                let command = words[0].clone();
                let mode = match mode {
                    CommandMode::ResolveApp(selector) => match resolve_app_mode(&selector) {
                        Ok(mode) => mode,
                        Err(error) => {
                            eprintln!("botster-hub {command} error: {error}");
                            print_prompt()?;
                            continue;
                        }
                    },
                    other => other,
                };
                let args = match canonicalize_with_pinned_data_dir(
                    &command,
                    words[1..].to_vec(),
                    &data_directory,
                    &mut canonicalize,
                ) {
                    Ok(args) => args,
                    Err(error) => {
                        eprintln!("botster-hub {command} error: {error}");
                        print_prompt()?;
                        continue;
                    }
                };
                if mode == CommandMode::Foreground {
                    signals.begin_foreground();
                } else {
                    signals.begin_startup_or_inline();
                }
                let mut saved_terminal_mode = if mode == CommandMode::Foreground {
                    Some(SavedTerminalMode::capture(stdin_fd)?)
                } else {
                    None
                };
                let result = dispatch(&command, args, signals);
                if let Some(saved_terminal_mode) = saved_terminal_mode.as_mut() {
                    saved_terminal_mode.restore()?;
                }
                signals.begin_idle();
                match result {
                    Ok(CommandOutcome::Completed) => {}
                    Ok(CommandOutcome::DaemonStopped) => return Ok(()),
                    Ok(CommandOutcome::ForegroundAppExited { description, .. }) => {
                        eprintln!("botster-hub {command} error: foreground app {description}");
                    }
                    Err(error) => eprintln!("botster-hub {command} error: {error}"),
                }
                print_prompt()?;
            }
        }
    }
}

struct SavedTerminalMode {
    fd: libc::c_int,
    original: Option<libc::termios>,
}

impl SavedTerminalMode {
    fn capture(fd: libc::c_int) -> io::Result<Self> {
        if unsafe {
            // SAFETY: isatty only inspects the borrowed file descriptor.
            libc::isatty(fd)
        } != 1
        {
            return Ok(Self { fd, original: None });
        }

        let mut original = std::mem::MaybeUninit::<libc::termios>::uninit();
        if unsafe {
            // SAFETY: original points to writable termios storage and fd remains borrowed.
            libc::tcgetattr(fd, original.as_mut_ptr())
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            fd,
            original: Some(unsafe {
                // SAFETY: tcgetattr initialized original after returning success.
                original.assume_init()
            }),
        })
    }

    fn restore(&mut self) -> io::Result<()> {
        let Some(original) = self.original.as_ref() else {
            return Ok(());
        };
        if unsafe {
            // SAFETY: original is initialized and fd remains borrowed by the console.
            libc::tcsetattr(self.fd, libc::TCSADRAIN, original)
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        self.original = None;
        Ok(())
    }
}

impl Drop for SavedTerminalMode {
    fn drop(&mut self) {
        if let Some(original) = self.original.as_ref() {
            unsafe {
                // SAFETY: best-effort unwind restoration uses initialized attributes and a
                // borrowed descriptor that remains open for the lifetime of the console.
                libc::tcsetattr(self.fd, libc::TCSADRAIN, original);
            }
        }
    }
}

fn canonicalize_with_pinned_data_dir(
    command: &str,
    mut args: Vec<String>,
    data_directory: &Path,
    canonicalize: &mut impl FnMut(&str, Vec<String>) -> Result<Vec<String>, String>,
) -> Result<Vec<String>, String> {
    args.insert(0, data_directory.to_string_lossy().into_owned());
    args.insert(0, "--data-dir".to_string());
    canonicalize(command, args)
}

fn print_prompt() -> io::Result<()> {
    print!("{PROMPT}");
    io::stdout().flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn shell_words_preserve_quoted_and_escaped_paths() {
        assert_eq!(
            parse_line(r#"packages install --path "/tmp/My Package""#).expect("parse quoted path"),
            words(&["packages", "install", "--path", "/tmp/My Package"])
        );
        assert_eq!(
            parse_line(r#"packages install --path /tmp/My\ Package"#).expect("parse escaped path"),
            words(&["packages", "install", "--path", "/tmp/My Package"])
        );
        assert!(parse_line(r#"packages install --path "unterminated"#).is_err());
    }

    #[test]
    fn every_top_level_dispatch_family_has_an_explicit_console_mode() {
        let cases = [
            ("start", CommandMode::ExternalOnly),
            ("up", CommandMode::Inline),
            ("down", CommandMode::Stop),
            ("doctor", CommandMode::Inline),
            ("smoke", CommandMode::Inline),
            ("status", CommandMode::Inline),
            ("sessions list", CommandMode::Inline),
            ("sessions attach abc", CommandMode::ExternalOnly),
            ("session-templates list", CommandMode::Inline),
            ("spawn-targets list", CommandMode::Inline),
            ("context", CommandMode::Inline),
            ("shutdown", CommandMode::Stop),
            ("mcp-serve", CommandMode::ExternalOnly),
            (
                "open web",
                CommandMode::ResolveApp("botster-web/web-client".to_string()),
            ),
            (
                "open tui",
                CommandMode::ResolveApp("botster-tui".to_string()),
            ),
            ("reload pkg", CommandMode::Inline),
            ("apps list", CommandMode::Inline),
            (
                "apps open package/app",
                CommandMode::ResolveApp("package/app".to_string()),
            ),
            ("packages list", CommandMode::Inline),
            ("providers list", CommandMode::Inline),
            ("inspect", CommandMode::ExternalOnly),
            ("run-one", CommandMode::ExternalOnly),
            ("future-command", CommandMode::ExternalOnly),
        ];
        for (line, expected) in cases {
            let parsed = parse_line(line).expect("parse classification fixture");
            assert_eq!(
                command_action(&parsed),
                ConsoleAction::Command(expected),
                "{line}"
            );
        }
    }

    #[test]
    fn data_dir_override_is_rejected_only_before_operand_separator() {
        assert!(contains_data_dir_override(&words(&[
            "sessions",
            "list",
            "--data-dir",
            "/tmp/other"
        ])));
        assert!(!contains_data_dir_override(&words(&[
            "sessions",
            "send-input",
            "--",
            "--data-dir"
        ])));
    }

    #[test]
    fn external_only_commands_render_exact_explicit_invocations() {
        for (line, expected) in [
            ("start", "botster-hub start"),
            ("mcp-serve", "botster-hub mcp-serve"),
            (
                "sessions attach sentinel",
                "botster-hub sessions attach sentinel",
            ),
            ("inspect sentinel", "botster-hub inspect sentinel"),
            ("run-one --once", "botster-hub run-one --once"),
        ] {
            let parsed = parse_line(line).expect("parse external-only command");
            assert_eq!(
                exact_external_invocation(&parsed, |_command, args| Ok(args)),
                expected,
                "{line}"
            );
        }
    }
}
