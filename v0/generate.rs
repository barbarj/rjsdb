use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaCha8Rng,
};

use crate::DbFloat;

pub struct RNG {
    rng: ChaCha8Rng,
}
impl RNG {
    /// Creates a new generator using a random seed.
    pub fn new() -> Self {
        let seed: u64 = rand::random();
        RNG::from_seed(seed)
    }

    /// Creates a new generator using the provided seed
    pub fn from_seed(seed: u64) -> Self {
        let rng = ChaCha8Rng::seed_from_u64(seed);
        RNG { rng }
    }

    pub fn next_value(&mut self) -> u32 {
        self.rng.next_u32()
    }
}
impl Default for RNG {
    fn default() -> Self {
        RNG::new()
    }
}

pub trait Generate {
    fn generate(rng: &mut RNG) -> Self;
}

impl Generate for i32 {
    fn generate(rng: &mut RNG) -> Self {
        let num = rng.next_value();
        num as i32
    }
}
impl Generate for f32 {
    fn generate(rng: &mut RNG) -> Self {
        let num = rng.next_value();
        f32::from_bits(num)
    }
}
impl Generate for f64 {
    fn generate(rng: &mut RNG) -> Self {
        let num = u64::generate(rng);
        f64::from_bits(num)
    }
}
impl Generate for usize {
    fn generate(rng: &mut RNG) -> Self {
        u64::generate(rng) as usize
    }
}
impl Generate for u64 {
    fn generate(rng: &mut RNG) -> Self {
        let mut bytes = [0u8; 8];
        bytes[0..4].copy_from_slice(rng.next_value().to_le_bytes().as_slice());
        bytes[4..8].copy_from_slice(rng.next_value().to_le_bytes().as_slice());
        u64::from_le_bytes(bytes)
    }
}
impl Generate for i64 {
    fn generate(rng: &mut RNG) -> Self {
        let bytes = u64::generate(rng).to_le_bytes();
        i64::from_le_bytes(bytes)
    }
}

impl Generate for DbFloat {
    fn generate(rng: &mut RNG) -> Self {
        let mut f = f64::generate(rng);
        while !f.is_finite() {
            f = f64::generate(rng);
        }
        DbFloat::new(f)
    }
}

const STRING_GEN_LENGTH_MAX: u32 = 100;
impl Generate for String {
    /// Generates a string of a random length with random, valid characters
    ///
    /// # Panics
    /// - If a u32->usize conversion fails.
    /// - If a u32->char conversion, which would already been proven to be valid, fails.
    fn generate(rng: &mut RNG) -> Self {
        let length = rng.next_value() % STRING_GEN_LENGTH_MAX;

        let mut output = String::with_capacity(length.try_into().unwrap());
        for _ in 0..length {
            let ch = char::generate(rng);
            output.push(ch);
        }

        output
    }
}

// const CHAR_GEN_UNICODE_CLAMP: u32 = 0x00ff; // Limits us to only latin characters
const CHAR_GEN_UNICODE_CLAMP: u32 = 0x007f; // Limits us to only latin characters
impl Generate for char {
    fn generate(rng: &mut RNG) -> Self {
        let mut x = rng.next_value() % CHAR_GEN_UNICODE_CLAMP;
        while to_useful_char(x).is_none() {
            x = rng.next_value() % CHAR_GEN_UNICODE_CLAMP;
        }
        char::from_u32(x)
            .expect("Failed a u32->char conversion that should have already been proven to work.")
    }
}
const DISSALOWED_CHARS: [char; 9] = ['*', ',', ';', '=', '(', ')', '<', '>', '\''];
fn to_useful_char(n: u32) -> Option<char> {
    let ch = char::from_u32(n)?;
    if ch.is_control() || DISSALOWED_CHARS.contains(&ch) {
        None
    } else {
        Some(ch)
    }
}
