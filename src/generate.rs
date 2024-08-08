use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaCha8Rng,
};

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
        num as f32
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
        while let None = to_useful_char(x) {
            x = rng.next_value() % CHAR_GEN_UNICODE_CLAMP;
        }
        char::from_u32(x)
            .expect("Failed a u32->char conversion that should have already been proven to work.")
    }
}
fn to_useful_char(n: u32) -> Option<char> {
    let ch = char::from_u32(n)?;
    if ch.is_control() {
        None
    } else {
        Some(ch)
    }
}
