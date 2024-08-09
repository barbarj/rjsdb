use core::str;

use serde::{
    de::{self, EnumAccess, MapAccess, SeqAccess, VariantAccess},
    Deserialize,
};

use super::SerdeError;

// TODO: Write some basic tests for all of this

type Result<T> = std::result::Result<T, SerdeError>;

pub struct Deserializer<'de> {
    input: &'de [u8],
}
impl<'de> Deserializer<'de> {
    pub fn from_bytes(input: &'de [u8]) -> Self {
        Deserializer { input }
    }
}

pub fn from_bytes<'a, T>(bytes: &'a [u8]) -> Result<T>
where
    T: Deserialize<'a>,
{
    let mut deserializer = Deserializer::from_bytes(bytes);
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.input.is_empty() {
        Ok(t)
    } else {
        Err(SerdeError::TrailingBytes)
    }
}

impl<'de> Deserializer<'de> {
    fn next_bytes(&mut self, num: usize) -> Option<&[u8]> {
        if self.input.len() < num {
            return None;
        }
        let bytes = &self.input[0..num];
        self.input = &self.input[num..];
        Some(bytes)
    }

    fn next_byte(&mut self) -> Option<&'de u8> {
        let byte = self.input.get(0)?;
        self.input = &self.input[1..];
        Some(byte)
    }

    fn parse_bool(&mut self) -> Result<bool> {
        match self.next_byte() {
            Some(1) => Ok(true),
            Some(0) => Ok(false),
            None => Err(SerdeError::Eof),
            Some(_) => Err(SerdeError::UnparseableValue),
        }
    }

    fn parse_i8(&mut self) -> Result<i8> {
        let bytes = match self.next_bytes(1) {
            Some(b) => b
                .try_into()
                .expect("byteslice did not have the number of bytes we just verified it does have"),
            None => return Err(SerdeError::Eof),
        };

        Ok(i8::from_le_bytes(bytes))
    }

    fn parse_i16(&mut self) -> Result<i16> {
        let bytes = match self.next_bytes(2) {
            Some(b) => b
                .try_into()
                .expect("byteslice did not have the number of bytes we just verified it does have"),
            None => return Err(SerdeError::Eof),
        };

        Ok(i16::from_le_bytes(bytes))
    }

    fn parse_i32(&mut self) -> Result<i32> {
        let bytes = match self.next_bytes(4) {
            Some(b) => b
                .try_into()
                .expect("byteslice did not have the number of bytes we just verified it does have"),
            None => return Err(SerdeError::Eof),
        };

        Ok(i32::from_le_bytes(bytes))
    }

    fn parse_i64(&mut self) -> Result<i64> {
        let bytes = match self.next_bytes(8) {
            Some(b) => b
                .try_into()
                .expect("byteslice did not have the number of bytes we just verified it does have"),
            None => return Err(SerdeError::Eof),
        };

        Ok(i64::from_le_bytes(bytes))
    }

    fn parse_i128(&mut self) -> Result<i128> {
        let bytes = match self.next_bytes(16) {
            Some(b) => b
                .try_into()
                .expect("byteslice did not have the number of bytes we just verified it does have"),
            None => return Err(SerdeError::Eof),
        };

        Ok(i128::from_le_bytes(bytes))
    }

    fn parse_u8(&mut self) -> Result<u8> {
        let bytes = match self.next_bytes(1) {
            Some(b) => b
                .try_into()
                .expect("byteslice did not have the number of bytes we just verified it does have"),
            None => return Err(SerdeError::Eof),
        };

        Ok(u8::from_le_bytes(bytes))
    }

    fn parse_u16(&mut self) -> Result<u16> {
        let bytes = match self.next_bytes(2) {
            Some(b) => b
                .try_into()
                .expect("byteslice did not have the number of bytes we just verified it does have"),
            None => return Err(SerdeError::Eof),
        };

        Ok(u16::from_le_bytes(bytes))
    }

    fn parse_u32(&mut self) -> Result<u32> {
        let bytes = match self.next_bytes(4) {
            Some(b) => b
                .try_into()
                .expect("byteslice did not have the number of bytes we just verified it does have"),
            None => return Err(SerdeError::Eof),
        };

        Ok(u32::from_le_bytes(bytes))
    }

    fn parse_u64(&mut self) -> Result<u64> {
        let bytes = match self.next_bytes(8) {
            Some(b) => b
                .try_into()
                .expect("byteslice did not have the number of bytes we just verified it does have"),
            None => return Err(SerdeError::Eof),
        };

        Ok(u64::from_le_bytes(bytes))
    }

    fn parse_u128(&mut self) -> Result<u128> {
        let bytes = match self.next_bytes(16) {
            Some(b) => b
                .try_into()
                .expect("byteslice did not have the number of bytes we just verified it does have"),
            None => return Err(SerdeError::Eof),
        };

        Ok(u128::from_le_bytes(bytes))
    }

    fn parse_f32(&mut self) -> Result<f32> {
        let bytes = match self.next_bytes(4) {
            Some(b) => b
                .try_into()
                .expect("byteslice did not have the number of bytes we just verified it does have"),
            None => return Err(SerdeError::Eof),
        };

        Ok(f32::from_le_bytes(bytes))
    }

    fn parse_f64(&mut self) -> Result<f64> {
        let bytes = match self.next_bytes(8) {
            Some(b) => b
                .try_into()
                .expect("byteslice did not have the number of bytes we just verified it does have"),
            None => return Err(SerdeError::Eof),
        };

        Ok(f64::from_le_bytes(bytes))
    }

    fn parse_bytes(&mut self) -> Result<&[u8]> {
        let len = self.parse_u64()?;
        match self.next_bytes(len as usize) {
            None => Err(SerdeError::Eof),
            Some(bytes) => Ok(bytes),
        }
    }

    fn parse_str(&mut self) -> Result<&str> {
        let bytes = self.parse_bytes()?;
        let str = str::from_utf8(bytes)?;
        Ok(str)
    }

    // NOTE: Parsing functions go here
}
impl<'a, 'de> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = SerdeError;

    fn deserialize_any<V>(self, _visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!();
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_bool(self.parse_bool()?)
    }

    fn deserialize_i8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_i8(self.parse_i8()?)
    }

    fn deserialize_i16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_i16(self.parse_i16()?)
    }

    fn deserialize_i32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_i32(self.parse_i32()?)
    }

    fn deserialize_i64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_i64(self.parse_i64()?)
    }

    fn deserialize_i128<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_i128(self.parse_i128()?)
    }

    fn deserialize_u8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_u8(self.parse_u8()?)
    }

    fn deserialize_u16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_u16(self.parse_u16()?)
    }

    fn deserialize_u32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_u32(self.parse_u32()?)
    }

    fn deserialize_u64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_u64(self.parse_u64()?)
    }

    fn deserialize_u128<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_u128(self.parse_u128()?)
    }

    fn deserialize_f32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_f32(self.parse_f32()?)
    }

    fn deserialize_f64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_f64(self.parse_f64()?)
    }

    fn deserialize_char<V>(self, _visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!();
    }

    fn deserialize_str<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_str(self.parse_str()?)
    }

    fn deserialize_string<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_bytes(self.parse_bytes()?)
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!();
    }

    fn deserialize_option<V>(self, _visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!();
    }

    fn deserialize_unit<V>(self, _visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!();
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!();
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_seq(Seq::build(self)?)
    }

    fn deserialize_tuple<V>(
        self,
        _len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_map(Seq::build(self)?)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_seq(Seq::build(self)?)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_enum(Enum::new(self))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_u32(visitor)
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!();
    }
}

struct Seq<'a, 'de: 'a> {
    len: u64,
    de: &'a mut Deserializer<'de>,
}
impl<'a, 'de> Seq<'a, 'de> {
    fn build(de: &'a mut Deserializer<'de>) -> Result<Self> {
        let len = u64::deserialize(&mut *de)?;
        Ok(Seq { len, de })
    }
}
impl<'de, 'a> SeqAccess<'de> for Seq<'a, 'de> {
    type Error = SerdeError;

    fn next_element_seed<T>(
        &mut self,
        seed: T,
    ) -> std::result::Result<Option<T::Value>, Self::Error>
    where
        T: de::DeserializeSeed<'de>,
    {
        if self.len == 0 {
            return Ok(None);
        }
        let value = seed.deserialize(&mut *self.de)?;
        self.len -= 1;
        Ok(Some(value))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.len as usize)
    }
}
impl<'de, 'a> MapAccess<'de> for Seq<'a, 'de> {
    type Error = SerdeError;

    fn next_key_seed<K>(&mut self, seed: K) -> std::result::Result<Option<K::Value>, Self::Error>
    where
        K: de::DeserializeSeed<'de>,
    {
        if self.len == 0 {
            return Ok(None);
        }
        let value = seed.deserialize(&mut *self.de)?;
        Ok(Some(value))
    }

    fn next_value_seed<V>(&mut self, seed: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        let value = seed.deserialize(&mut *self.de)?;
        self.len -= 1;
        Ok(value)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.len as usize)
    }
}

struct Enum<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
}
impl<'a, 'de> Enum<'a, 'de> {
    fn new(de: &'a mut Deserializer<'de>) -> Self {
        Enum { de }
    }
}
impl<'de, 'a> EnumAccess<'de> for Enum<'a, 'de> {
    type Error = SerdeError;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> std::result::Result<(V::Value, Self::Variant), Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        let val = seed.deserialize(&mut *self.de)?;
        Ok((val, self))
    }
}
impl<'de, 'a> VariantAccess<'de> for Enum<'a, 'de> {
    type Error = SerdeError;

    fn unit_variant(self) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> std::result::Result<T::Value, Self::Error>
    where
        T: de::DeserializeSeed<'de>,
    {
        seed.deserialize(self.de)
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        de::Deserializer::deserialize_seq(self.de, visitor)
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        de::Deserializer::deserialize_seq(self.de, visitor)
    }
}
