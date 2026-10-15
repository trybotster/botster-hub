//! `botster`: the command line of the v1 Hub. `botster hub ...` manages the Hub, and `botster tui` launches the TUI,
//! which connects to the Hub (CLI-AT2; the subcommand is `tui`, user 2026-10-12). Neither is built yet.

use std::process::ExitCode;

/// What the arguments (without the program) ask for.
#[derive(Debug, PartialEq, Eq)]
enum Command {
    Hub,
    Tui,
    Usage,
}

const USAGE: &str = "usage: botster <hub|tui>";

fn parse(args: &[String]) -> Command {
    match args.first().map(String::as_str) {
        Some("hub") => Command::Hub,
        Some("tui") => Command::Tui,
        _ => Command::Usage,
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match parse(&args) {
        Command::Hub => eprintln!("botster hub: not built yet (botster-hub v1 bootstrap)"),
        Command::Tui => eprintln!("botster tui: not built yet (botster-hub v1 bootstrap)"),
        Command::Usage => eprintln!("{USAGE}"),
    }
    ExitCode::from(2)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(words: &[&str]) -> Vec<String> {
        words.iter().map(|w| w.to_string()).collect()
    }

    #[test]
    fn the_subcommands_are_hub_and_tui() {
        assert_eq!(parse(&args(&["hub", "start"])), Command::Hub);
        assert_eq!(parse(&args(&["tui"])), Command::Tui);
        assert_eq!(parse(&args(&["attach"])), Command::Usage);
        assert_eq!(parse(&args(&[])), Command::Usage);
    }
}
