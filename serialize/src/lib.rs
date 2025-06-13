pub mod de;
pub mod error;
pub mod ser;

pub use de::{from_reader, Deserializer};
pub use error::{Error, Result};
pub use ser::{to_bytes, to_writer, Serializer};

#[cfg(not(target_pointer_width = "64"))]
compile_error!("This serialization format is only supported on 64-bit systems");

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use serde::{Deserialize, Serialize};

    use crate::{from_reader, to_bytes};

    fn assert_value_serdes_correctly<'a, T>(input: T)
    where
        T: Serialize + Deserialize<'a> + PartialEq + Debug,
    {
        let bytes = to_bytes(&input).unwrap();
        println!("bytes: {bytes:?}");
        let output: T = from_reader(&bytes[..]).unwrap();
        assert_eq!(input, output);
    }

    fn assert_str_serdes_correctly(input: &str) {
        let bytes = to_bytes(&input).unwrap();
        let output: String = from_reader(&bytes[..]).unwrap();
        assert_eq!(input, &output);
    }

    fn assert_byte_slice_serdes_correctly(input: &[u8]) {
        let bytes = to_bytes(&input).unwrap();
        let output: Vec<u8> = from_reader(&bytes[..]).unwrap();
        assert_eq!(input, &output);
    }

    #[test]
    fn basic_types() {
        // unsigned
        assert_value_serdes_correctly(42u8);
        assert_value_serdes_correctly(42u16);
        assert_value_serdes_correctly(42u32);
        assert_value_serdes_correctly(42u64);
        assert_value_serdes_correctly(42u128);
        assert_value_serdes_correctly(42usize);
        // signed
        assert_value_serdes_correctly(42i8);
        assert_value_serdes_correctly(42i16);
        assert_value_serdes_correctly(42i32);
        assert_value_serdes_correctly(42i64);
        assert_value_serdes_correctly(42i128);
        // floats
        assert_value_serdes_correctly(42.42f32);
        assert_value_serdes_correctly(42.42f64);
        // others
        assert_value_serdes_correctly(String::from("foobar"));
        assert_str_serdes_correctly("foobar");
        assert_value_serdes_correctly('f');
        assert_value_serdes_correctly(vec![31, 32, 33]);
        assert_value_serdes_correctly([31u8, 32u8, 33u8]);
        let byte_slice = "foooo".as_bytes();
        assert_byte_slice_serdes_correctly(byte_slice);
    }

    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct BasicStruct {
        a: usize,
        b: u32,
    }

    #[test]
    fn basic_struct() {
        assert_value_serdes_correctly(BasicStruct { a: 1382, b: 12329 });
    }

    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    enum UnitEnum {
        Foo,
        Bar,
        Baz,
    }

    #[test]
    fn unit_enum() {
        assert_value_serdes_correctly(UnitEnum::Foo);
        assert_value_serdes_correctly(UnitEnum::Bar);
        assert_value_serdes_correctly(UnitEnum::Baz);
    }

    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct NestedTypes {
        a: u16,
        b: i32,
        c: UnitEnum,
        d: BasicStruct,
    }

    #[test]
    fn nested_types() {
        let input = NestedTypes {
            a: 1230,
            b: -1239,
            c: UnitEnum::Bar,
            d: BasicStruct {
                a: 41231415,
                b: 1231,
            },
        };
        assert_value_serdes_correctly(input)
    }
}
