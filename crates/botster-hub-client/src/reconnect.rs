//! The UI's reconnect after a Hub restart (CLI-AT4, which cites CL-48: 10 attempts, 1 s apart; lead ruling Q4). The CLI
//! does not reconnect. A bounded retry on a failed connect is not state polling (HC P8, lead ruling).
//!
//! The wait is passed in, so a test runs the attempts with no clock; the values themselves are fixed here.

use std::time::Duration;

/// Attempts in all (CLI-AT4).
pub const RECONNECT_ATTEMPTS: u32 = 10;

/// The time between two attempts (CLI-AT4).
pub const RECONNECT_SPACING: Duration = Duration::from_secs(1);

/// Runs `attempt` up to [`RECONNECT_ATTEMPTS`] times, calling `wait(RECONNECT_SPACING)` between two attempts (never
/// before the first or after the last). It returns the first success, or the last error.
pub fn reconnect<T, E>(
    mut attempt: impl FnMut(u32) -> Result<T, E>,
    mut wait: impl FnMut(Duration),
) -> Result<T, E> {
    let mut n = 1;
    loop {
        match attempt(n) {
            Ok(value) => return Ok(value),
            Err(error) if n >= RECONNECT_ATTEMPTS => return Err(error),
            Err(_) => {
                wait(RECONNECT_SPACING);
                n += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ten_failed_attempts_one_second_apart_return_the_last_error() {
        let mut waits = Vec::new();
        let mut tried = Vec::new();
        let result: Result<(), u32> = reconnect(
            |n| {
                tried.push(n);
                Err(n)
            },
            |d| waits.push(d),
        );
        assert_eq!(result, Err(10));
        assert_eq!(tried, (1..=10).collect::<Vec<_>>());
        assert_eq!(waits, vec![Duration::from_secs(1); 9]);
    }

    #[test]
    fn the_first_success_ends_the_attempts_with_no_wait_after_it() {
        let mut waits = 0;
        let mut tried = 0;
        let result: Result<&str, ()> = reconnect(
            |n| {
                tried += 1;
                if n == 3 {
                    Ok("welcome")
                } else {
                    Err(())
                }
            },
            |_| waits += 1,
        );
        assert_eq!(result, Ok("welcome"));
        assert_eq!((tried, waits), (3, 2));
    }

    #[test]
    fn a_first_success_needs_no_wait() {
        let mut waits = 0;
        let result: Result<u8, ()> = reconnect(|_| Ok(1), |_| waits += 1);
        assert_eq!((result, waits), (Ok(1), 0));
    }
}
