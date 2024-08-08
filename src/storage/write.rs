use serde::{
    ser::{self, SerializeSeq},
    Serialize,
};
use std::io;

use super::SerdeError;

struct Serializer<'w, T: io::Write> {
    writer: &'w mut T,
}
impl<'w, T: io::Write> Serializer<'w, T> {
    fn build(writer: &'w mut T) -> Self {
        Serializer { writer }
    }
}
impl<'a, 'w, T: io::Write> ser::Serializer for &'a mut Serializer<'w, T> {
    type Ok = ();
    type Error = SerdeError;
    type SerializeMap = Self;
    type SerializeSeq = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        let output = if v { 1 } else { 0 };
        self.writer.write([output].as_slice())?;
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        let mut seq = self.serialize_seq(Some(v.len()))?;
        for b in v {
            seq.serialize_element(b)?;
        }
        seq.end()
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(v.to_string().as_ref())
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        let bytes = v.to_le_bytes();
        self.writer.write(&bytes[..])?;
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        let bytes = v.to_le_bytes();
        self.writer.write(&bytes[..])?;
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        let bytes = v.to_le_bytes();
        self.writer.write(&bytes[..])?;
        Ok(())
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        let bytes = v.to_le_bytes();
        self.writer.write(&bytes[..])?;
        Ok(())
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        let bytes = v.to_le_bytes();
        self.writer.write(&bytes[..])?;
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        let bytes = v.to_le_bytes();
        self.writer.write(&bytes[..])?;
        Ok(())
    }

    fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
        let bytes = v.to_le_bytes();
        self.writer.write(&bytes[..])?;
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        let bytes = v.to_le_bytes();
        self.writer.write(&bytes[..])?;
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        let bytes = v.to_le_bytes();
        self.writer.write(&bytes[..])?;
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        let bytes = v.to_le_bytes();
        self.writer.write(&bytes[..])?;
        Ok(())
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        let bytes = v.to_le_bytes();
        self.writer.write(&bytes[..])?;
        Ok(())
    }

    fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
        let bytes = v.to_le_bytes();
        self.writer.write(&bytes[..])?;
        Ok(())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.serialize_bytes(v.as_bytes())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!();
    }

    fn serialize_some<U>(self, _value: &U) -> Result<Self::Ok, Self::Error>
    where
        U: ?Sized + Serialize,
    {
        unimplemented!();
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        unimplemented!();
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        unimplemented!();
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_u32(variant_index)
    }

    fn serialize_newtype_struct<U>(
        self,
        _name: &'static str,
        value: &U,
    ) -> Result<Self::Ok, Self::Error>
    where
        U: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<U>(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        value: &U,
    ) -> Result<Self::Ok, Self::Error>
    where
        U: ?Sized + Serialize,
    {
        variant_index.serialize(&mut *self)?;
        value.serialize(&mut *self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let len = match len {
            Some(l) => l as u64,
            None => 0,
        };
        self.serialize_u64(len)?;
        Ok(self)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.serialize_u32(variant_index)?;
        self.serialize_seq(Some(len))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        self.serialize_seq(len)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.serialize_u32(variant_index)?;
        self.serialize_seq(Some(len))
    }
}
impl<'a, 'w, T: io::Write> ser::SerializeSeq for &'a mut Serializer<'w, T> {
    type Ok = ();
    type Error = SerdeError;

    fn serialize_element<U>(&mut self, value: &U) -> Result<(), Self::Error>
    where
        U: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}
impl<'a, 'w, T: io::Write> ser::SerializeTuple for &'a mut Serializer<'w, T> {
    type Ok = ();
    type Error = SerdeError;

    fn serialize_element<U>(&mut self, value: &U) -> Result<(), Self::Error>
    where
        U: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}
impl<'a, 'w, T: io::Write> ser::SerializeTupleStruct for &'a mut Serializer<'w, T> {
    type Ok = ();
    type Error = SerdeError;

    fn serialize_field<U>(&mut self, value: &U) -> Result<(), Self::Error>
    where
        U: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}
impl<'a, 'w, T: io::Write> ser::SerializeTupleVariant for &'a mut Serializer<'w, T> {
    type Ok = ();
    type Error = SerdeError;

    fn serialize_field<U>(&mut self, value: &U) -> Result<(), Self::Error>
    where
        U: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}
impl<'a, 'w, T: io::Write> ser::SerializeMap for &'a mut Serializer<'w, T> {
    type Ok = ();
    type Error = SerdeError;

    fn serialize_key<U>(&mut self, key: &U) -> Result<(), Self::Error>
    where
        U: ?Sized + Serialize,
    {
        key.serialize(&mut **self)
    }

    fn serialize_value<U>(&mut self, value: &U) -> Result<(), Self::Error>
    where
        U: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}
impl<'a, 'w, T: io::Write> ser::SerializeStruct for &'a mut Serializer<'w, T> {
    type Ok = ();
    type Error = SerdeError;

    fn serialize_field<U>(&mut self, _key: &'static str, value: &U) -> Result<(), Self::Error>
    where
        U: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}
impl<'a, 'w, T: io::Write> ser::SerializeStructVariant for &'a mut Serializer<'w, T> {
    type Ok = ();
    type Error = SerdeError;

    fn serialize_field<U>(&mut self, _key: &'static str, value: &U) -> Result<(), Self::Error>
    where
        U: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

pub fn to_writer<'w, W, T>(writer: &'w mut W, value: &T) -> Result<(), SerdeError>
where
    W: io::Write,
    T: Serialize,
{
    let mut serializer = Serializer::build(writer);
    value.serialize(&mut serializer)
}
