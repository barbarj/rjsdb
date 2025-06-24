use std::io::Read;

use crate::error::{Error, Result};
use serde::de::{EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};
use serde::{de, Deserialize};

pub struct Deserializer<R: Read> {
    reader: R,
}
impl<R: Read> Deserializer<R> {
    fn from_reader(reader: R) -> Self {
        Deserializer { reader }
    }
}

pub fn from_reader<'de, R, T>(reader: R) -> Result<T>
where
    R: Read,
    T: Deserialize<'de>,
{
    let mut deserializer = Deserializer::from_reader(reader);
    T::deserialize(&mut deserializer)
}

impl<R: Read> Deserializer<R> {
    fn parse_bool(&mut self) -> Result<bool> {
        let mut buf = [0; 1];
        self.reader.read_exact(&mut buf)?;
        let byte = buf[0];
        if byte == 0 {
            Ok(false)
        } else if byte == 1 {
            Ok(true)
        } else {
            Err(Error::ExpectedBool)
        }
    }

    fn parse_i8(&mut self) -> Result<i8> {
        let mut buf = [0; 1];
        self.reader.read_exact(&mut buf)?;
        Ok(i8::from_be_bytes(buf))
    }

    fn parse_i16(&mut self) -> Result<i16> {
        let mut buf = [0; 2];
        self.reader.read_exact(&mut buf)?;
        Ok(i16::from_be_bytes(buf))
    }

    fn parse_i32(&mut self) -> Result<i32> {
        let mut buf = [0; 4];
        self.reader.read_exact(&mut buf)?;
        Ok(i32::from_be_bytes(buf))
    }

    fn parse_i64(&mut self) -> Result<i64> {
        let mut buf = [0; 8];
        self.reader.read_exact(&mut buf)?;
        Ok(i64::from_be_bytes(buf))
    }

    fn parse_i128(&mut self) -> Result<i128> {
        let mut buf = [0; 16];
        self.reader.read_exact(&mut buf)?;
        Ok(i128::from_be_bytes(buf))
    }

    fn parse_u8(&mut self) -> Result<u8> {
        let mut buf = [0; 1];
        self.reader.read_exact(&mut buf)?;
        Ok(u8::from_be_bytes(buf))
    }

    fn parse_u16(&mut self) -> Result<u16> {
        let mut buf = [0; 2];
        self.reader.read_exact(&mut buf)?;
        Ok(u16::from_be_bytes(buf))
    }

    fn parse_u32(&mut self) -> Result<u32> {
        let mut buf = [0; 4];
        self.reader.read_exact(&mut buf)?;
        Ok(u32::from_be_bytes(buf))
    }

    fn parse_u64(&mut self) -> Result<u64> {
        let mut buf = [0; 8];
        self.reader.read_exact(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    }

    fn parse_u128(&mut self) -> Result<u128> {
        let mut buf = [0; 16];
        self.reader.read_exact(&mut buf)?;
        Ok(u128::from_be_bytes(buf))
    }

    fn parse_f32(&mut self) -> Result<f32> {
        let mut buf = [0; 4];
        self.reader.read_exact(&mut buf)?;
        Ok(f32::from_be_bytes(buf))
    }

    fn parse_f64(&mut self) -> Result<f64> {
        let mut buf = [0; 8];
        self.reader.read_exact(&mut buf)?;
        Ok(f64::from_be_bytes(buf))
    }

    fn parse_byte_slice(&mut self) -> Result<Vec<u8>> {
        let len: usize = self.parse_u64()? as usize;
        let mut buf = vec![0; len];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn parse_string(&mut self) -> Result<String> {
        let buf = self.parse_byte_slice()?;
        match String::from_utf8(buf) {
            Ok(s) => Ok(s),
            Err(err) => Err(Error::ExpectedUtf8String(err)),
        }
    }
}

impl<'de, 'a, R: Read> de::Deserializer<'de> for &'a mut Deserializer<R> {
    type Error = Error;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!(
            "This format is not self describing, therefore deserialize_any is not supported"
        );
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_bool(self.parse_bool()?)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i8(self.parse_i8()?)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i16(self.parse_i16()?)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i32(self.parse_i32()?)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i64(self.parse_i64()?)
    }

    fn deserialize_i128<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i128(self.parse_i128()?)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u8(self.parse_u8()?)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u16(self.parse_u16()?)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u32(self.parse_u32()?)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u64(self.parse_u64()?)
    }

    fn deserialize_u128<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u128(self.parse_u128()?)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f32(self.parse_f32()?)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f64(self.parse_f64()?)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_str(&self.parse_string()?)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_string(self.parse_string()?)
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let char = match char::from_u32(self.parse_u32()?) {
            Some(c) => c,
            None => return Err(Error::ExpectedChar),
        };
        visitor.visit_char(char)
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_byte_buf(visitor)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_byte_buf(self.parse_byte_slice()?)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let variant = self.parse_u8()?;
        if variant == 0 {
            visitor.visit_none()
        } else if variant == 1 {
            visitor.visit_some(self)
        } else {
            Err(Error::ExpectedOption)
        }
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let len = self.parse_u64()?;
        visitor.visit_seq(SequenceWithLength::new(self, len))
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(SequenceWithLength::new(self, len as u64))
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let len = self.parse_u64()?;
        visitor.visit_map(SequenceWithLength::new(self, len))
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(SequenceWithLength::new(self, fields.len() as u64))
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_enum(Enum::new(self))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_u32(visitor)
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!(
            "This format is not self describing, therefore deserialize_ignored_any is not supported"
        );
    }
}

struct SequenceWithLength<'a, R: Read> {
    de: &'a mut Deserializer<R>,
    items_left: u64,
}
impl<'a, R: Read> SequenceWithLength<'a, R> {
    fn new(de: &'a mut Deserializer<R>, length: u64) -> Self {
        SequenceWithLength {
            de,
            items_left: length,
        }
    }
}
impl<'a, 'de, R: Read> SeqAccess<'de> for SequenceWithLength<'a, R> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: de::DeserializeSeed<'de>,
    {
        if self.items_left == 0 {
            Ok(None)
        } else {
            let value = seed.deserialize(&mut *self.de)?;
            self.items_left -= 1;
            Ok(Some(value))
        }
    }
}
impl<'a, 'de, R: Read> MapAccess<'de> for SequenceWithLength<'a, R> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: de::DeserializeSeed<'de>,
    {
        if self.items_left == 0 {
            Ok(None)
        } else {
            let value = seed.deserialize(&mut *self.de)?;
            self.items_left -= 1;
            Ok(Some(value))
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: de::DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self.de)
    }
}

struct Enum<'a, R: Read> {
    de: &'a mut Deserializer<R>,
}
impl<'a, R: Read> Enum<'a, R> {
    fn new(de: &'a mut Deserializer<R>) -> Self {
        Enum { de }
    }
}
impl<'a, 'de, R: Read> EnumAccess<'de> for Enum<'a, R> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: de::DeserializeSeed<'de>,
    {
        let v = seed.deserialize(&mut *self.de)?;
        Ok((v, self))
    }
}
impl<'a, 'de, R: Read> VariantAccess<'de> for Enum<'a, R> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: de::DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self.de)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        de::Deserializer::deserialize_tuple(&mut *self.de, len, visitor)
    }

    fn struct_variant<V>(self, fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        de::Deserializer::deserialize_struct(&mut *self.de, "", fields, visitor)
    }
}
