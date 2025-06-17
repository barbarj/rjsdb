use serde::{ser, Serialize};

use crate::error::{Error, Result};

pub struct SerializedSize {
    size: usize,
}
impl SerializedSize {
    fn new() -> Self {
        SerializedSize { size: 0 }
    }
}

pub fn serialized_size<T: Serialize>(value: &T) -> usize {
    let mut ser = SerializedSize::new();
    value
        .serialize(&mut ser)
        .expect("Determining serialized size should always work");
    ser.size
}

impl<'a> ser::Serializer for &'a mut SerializedSize {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, _v: bool) -> Result<()> {
        self.size += 1;
        Ok(())
    }

    fn serialize_i8(self, _v: i8) -> Result<()> {
        self.size += 1;
        Ok(())
    }

    fn serialize_i16(self, _v: i16) -> Result<()> {
        self.size += 2;
        Ok(())
    }

    fn serialize_i32(self, _v: i32) -> Result<()> {
        self.size += 4;
        Ok(())
    }

    fn serialize_i64(self, _v: i64) -> Result<()> {
        self.size += 8;
        Ok(())
    }

    fn serialize_i128(self, _v: i128) -> Result<()> {
        self.size += 16;
        Ok(())
    }

    fn serialize_u8(self, _v: u8) -> Result<()> {
        self.size += 1;
        Ok(())
    }

    fn serialize_u16(self, _v: u16) -> Result<()> {
        self.size += 2;
        Ok(())
    }

    fn serialize_u32(self, _v: u32) -> Result<()> {
        self.size += 4;
        Ok(())
    }

    fn serialize_u64(self, _v: u64) -> Result<()> {
        self.size += 8;
        Ok(())
    }

    fn serialize_u128(self, _v: u128) -> Result<()> {
        self.size += 16;
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        self.size += 4;
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.size += 8;
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<()> {
        self.serialize_u32(v.into())
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        self.serialize_bytes(v.as_bytes())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        self.size += 8 + v.len();
        Ok(())
    }

    fn serialize_none(self) -> Result<()> {
        self.size += 1;
        Ok(())
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + Serialize,
    {
        self.size += 1;
        value.serialize(self)
    }

    /// This is a no-op for now. That might be the wrong choice and later I might need
    /// to come encode something for this
    fn serialize_unit(self) -> Result<()> {
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        self.serialize_u32(variant_index)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.serialize_u32(variant_index)?;
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        let len = len.expect("Only sequences with known lengths are supported");
        self.serialize_u64(len as u64)?;
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.serialize_u32(variant_index)?;
        self.serialize_seq(Some(len))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        let len = len.expect("Only maps with known lengths are supported");
        self.serialize_u64(len as u64)?;
        Ok(self)
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        self.serialize_u32(variant_index)?;
        Ok(self)
    }
}

impl<'a> ser::SerializeSeq for &'a mut SerializedSize {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeTuple for &'a mut SerializedSize {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeTupleStruct for &'a mut SerializedSize {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut SerializedSize {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeMap for &'a mut SerializedSize {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeStruct for &'a mut SerializedSize {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeStructVariant for &'a mut SerializedSize {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}
