//! Deterministic simulation for SqLive. One seed draws the options, the
//! workload, the faults, and the crash points; the run checks every answer
//! against SQLite itself.

pub mod prng;
pub mod storage;
pub mod trace;
pub mod vfs;

use std::fmt;

/// Which workload a seed runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Harness {
    /// Plain SQLite on the simulated VFS, checked against a row model.
    Storage,
}

pub struct Simulator {
    seed: u64,
    harness: Harness,
    verbose: bool,
}

#[derive(Debug)]
pub struct Summary {
    pub seed: u64,
    pub harness: Harness,
    pub text: String,
    pub digest: u64,
}

impl fmt::Display for Summary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "seed {} ({:?}): {}", self.seed, self.harness, self.text)
    }
}

#[derive(Debug)]
pub struct Failure {
    pub seed: u64,
    pub harness: Harness,
    pub message: String,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {} ({:?}) failed: {}",
            self.seed, self.harness, self.message
        )
    }
}

impl Simulator {
    pub fn from_seed(seed: u64) -> Simulator {
        Simulator {
            seed,
            harness: Harness::Storage,
            verbose: false,
        }
    }

    pub fn harness(mut self, harness: Harness) -> Simulator {
        self.harness = harness;
        self
    }

    /// Prints every step of the run.
    pub fn trace(mut self) -> Simulator {
        self.verbose = true;
        self
    }

    pub fn options(&self) -> String {
        match self.harness {
            Harness::Storage => format!("{:?}", storage::StorageOptions::from_seed(self.seed)),
        }
    }

    pub fn run(self) -> Result<Summary, Failure> {
        vfs::configure_sqlite();
        // SAFETY: reseeds SQLite's PRNG, which picks WAL salts, so the run
        // replays byte for byte; no connection is passed.
        unsafe {
            rusqlite::ffi::sqlite3_test_control(
                rusqlite::ffi::SQLITE_TESTCTRL_PRNG_SEED,
                self.seed as std::ffi::c_int,
                std::ptr::null_mut::<rusqlite::ffi::sqlite3>(),
            );
        }
        let failure = |message| Failure {
            seed: self.seed,
            harness: self.harness,
            message,
        };
        match self.harness {
            Harness::Storage => storage::run(self.seed, self.verbose)
                .map(|summary| Summary {
                    seed: self.seed,
                    harness: self.harness,
                    text: summary.to_string(),
                    digest: summary.digest,
                })
                .map_err(failure),
        }
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
