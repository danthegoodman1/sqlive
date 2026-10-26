//! A run's trace: every observable step, folded into a digest. Two runs of
//! one seed must produce the same digest.

use crate::prng::fnv1a;
use std::fmt::Display;

#[derive(Debug, Default)]
pub struct Trace {
    digest: u64,
    lines: u64,
    verbose: bool,
}

impl Trace {
    pub fn new(verbose: bool) -> Trace {
        Trace {
            digest: 0,
            lines: 0,
            verbose,
        }
    }

    pub fn record(&mut self, line: impl Display) {
        let text = line.to_string();
        self.digest = fnv1a(text.as_bytes()) ^ self.digest.rotate_left(5);
        self.lines += 1;
        if self.verbose {
            eprintln!("{text}");
        }
    }

    pub fn digest(&self) -> u64 {
        self.digest
    }

    pub fn lines(&self) -> u64 {
        self.lines
    }
}
