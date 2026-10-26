//! The simulator's PRNG. It lives in this crate so a recorded seed replays
//! the same run after any dependency upgrade.

use std::ops::RangeInclusive;

/// xoshiro256**, seeded through SplitMix64.
#[derive(Clone, Debug)]
pub struct Prng {
    state: [u64; 4],
}

impl Prng {
    pub fn new(seed: u64) -> Prng {
        let mut mix = seed;
        Prng {
            state: std::array::from_fn(|_| splitmix64(&mut mix)),
        }
    }

    /// An independent stream for one purpose, so drawing more numbers for
    /// one purpose leaves the others' draws unchanged.
    pub fn stream(seed: u64, purpose: &str) -> Prng {
        Prng::new(seed ^ fnv1a(purpose.as_bytes()))
    }

    pub fn next_u64(&mut self) -> u64 {
        let [s0, s1, s2, s3] = &mut self.state;
        let result = s1.wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = *s1 << 17;
        *s2 ^= *s0;
        *s3 ^= *s1;
        *s1 ^= *s2;
        *s0 ^= *s3;
        *s2 ^= t;
        *s3 = s3.rotate_left(45);
        result
    }

    /// A uniform value in `0..bound`, which must be nonempty.
    pub fn below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "below(0)");
        // Lemire's method: reject the few products that would bias the result.
        let threshold = bound.wrapping_neg() % bound;
        loop {
            let product = u128::from(self.next_u64()) * u128::from(bound);
            if product as u64 >= threshold {
                return (product >> 64) as u64;
            }
        }
    }

    pub fn range(&mut self, range: RangeInclusive<u64>) -> u64 {
        range.start() + self.below(range.end() - range.start() + 1)
    }

    pub fn index(&mut self, len: usize) -> usize {
        self.below(len as u64) as usize
    }

    /// True with probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        p > 0.0 && (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64) < p
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.index(items.len())]
    }

    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }

    /// An independent generator drawn from this one.
    pub fn fork(&mut self) -> Prng {
        Prng::new(self.next_u64())
    }
}

pub fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// FNV-1a: a fixed hash for stream purposes and trace digests.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xCBF2_9CE4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01B3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_seed_always_draws_the_same_numbers() {
        let mut prng = Prng::new(1);
        let draws: Vec<u64> = (0..3).map(|_| prng.next_u64()).collect();
        assert_eq!(
            draws,
            [
                0xB3F2_AF6D_0FC7_10C5,
                0x853B_5596_4736_4CEA,
                0x92F8_9756_082A_4514
            ]
        );
    }

    #[test]
    fn streams_with_different_purposes_differ() {
        let mut a = Prng::stream(7, "schema");
        let mut b = Prng::stream(7, "writes");
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn drawing_from_one_stream_leaves_another_unchanged() {
        let mut first = Prng::stream(7, "writes");
        let expected: Vec<u64> = (0..4).map(|_| first.next_u64()).collect();
        let mut other = Prng::stream(7, "schema");
        for _ in 0..100 {
            other.next_u64();
        }
        let mut second = Prng::stream(7, "writes");
        let actual: Vec<u64> = (0..4).map(|_| second.next_u64()).collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn below_stays_in_bounds() {
        let mut prng = Prng::new(3);
        for bound in 1..50 {
            for _ in 0..20 {
                assert!(prng.below(bound) < bound);
            }
        }
    }
}
