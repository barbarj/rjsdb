use rjsdb::generate::{Generate, RNG};

fn main() {
    let mut rng = RNG::new();

    println!("Ints!");
    for _ in 0..10 {
        println!("{}", i32::generate(&mut rng));
    }
    println!("Floats!");
    for _ in 0..10 {
        println!("{}", f32::generate(&mut rng));
    }
    println!("Strings!");
    for _ in 0..10 {
        println!("\"{}\"", String::generate(&mut rng));
    }
}
