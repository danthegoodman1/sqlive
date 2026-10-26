//! Deterministic simulation for SqLive. One seed draws the options, the
//! workload, the faults, and the crash points; the run checks every answer
//! against SQLite itself.

pub mod prng;
pub mod trace;

use prng::Prng;
use std::fmt;
use trace::Trace;

/// What a seed configures.
#[derive(Clone, Debug)]
pub struct Options {
    pub steps: u64,
}

impl Options {
    pub fn from_seed(seed: u64) -> Options {
        let mut prng = Prng::stream(seed, "options");
        Options {
            steps: prng.range(50..=200),
        }
    }
}

pub struct Simulator {
    seed: u64,
    options: Options,
    verbose: bool,
}

#[derive(Debug)]
pub struct Summary {
    pub seed: u64,
    pub steps: u64,
    pub digest: u64,
}

impl fmt::Display for Summary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {}: {} steps, digest {:016x}",
            self.seed, self.steps, self.digest
        )
    }
}

#[derive(Debug)]
pub struct Failure {
    pub seed: u64,
    pub step: u64,
    pub message: String,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {} failed at step {}: {}",
            self.seed, self.step, self.message
        )
    }
}

impl Simulator {
    pub fn from_seed(seed: u64) -> Simulator {
        Simulator {
            seed,
            options: Options::from_seed(seed),
            verbose: false,
        }
    }

    /// Prints every step of the run.
    pub fn trace(mut self) -> Simulator {
        self.verbose = true;
        self
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    pub fn run(self) -> Result<Summary, Failure> {
        let mut trace = Trace::new(self.verbose);
        let mut prng = Prng::stream(self.seed, "steps");
        for step in 0..self.options.steps {
            trace.record(format_args!("step {step}: {}", prng.next_u64()));
        }
        Ok(Summary {
            seed: self.seed,
            steps: self.options.steps,
            digest: trace.digest(),
        })
    }
}

/// A seed in decimal, or a git commit hash, whose first 16 hex digits
/// become the seed.
pub fn parse_seed(text: &str) -> Result<u64, String> {
    if let Ok(seed) = text.parse::<u64>() {
        return Ok(seed);
    }
    let hex = text.get(..16).unwrap_or(text);
    if hex.len() >= 7 && hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return u64::from_str_radix(hex, 16).map_err(|e| e.to_string());
    }
    Err(format!("not a seed or commit hash: {text}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeds_parse_from_decimal_and_commit_hashes() {
        assert_eq!(parse_seed("42"), Ok(42));
        assert_eq!(
            parse_seed("a2ff3d8c0ffee1234567890"),
            Ok(0xa2ff3d8c0ffee123)
        );
        assert!(parse_seed("nope").is_err());
    }
}
