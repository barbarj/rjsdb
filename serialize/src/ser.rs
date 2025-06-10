use std::io::Write;

use serde::{ser, Serialize};

use crate::error::{Error, Result};

#[cfg(not(target_pointer_width = "64"))]
compile_error!("This serialization format is only supported on 64-bit systems");

struct Serializer<W: Write> {
    writer: W,
}
impl<W: Write> Serializer<W> {
    fn new(writer: W) -> Self {
        Serializer { writer }
    }
}

pub fn to_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes: Vec<u8> = Vec::new();
    let mut ser = Serializer::new(&mut bytes);
    value.serialize(&mut ser)?;
    Ok(bytes)
}

pub fn to_writer<W: Write, T: Serialize>(writer: W, value: &T) -> Result<()> {
    let mut ser = Serializer::new(writer);
    value.serialize(&mut ser)
}

impl<'a, W: Write> ser::Serializer for &'a mut Serializer<W> {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<()> {
        match v {
            true => self.serialize_u8(1),
            false => self.serialize_u8(0),
        }
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        self.serialize_bytes(&v.to_le_bytes())
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.serialize_bytes(&v.to_le_bytes())
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.serialize_bytes(&v.to_le_bytes())
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.serialize_bytes(&v.to_le_bytes())
    }

    fn serialize_i128(self, v: i128) -> Result<()> {
        self.serialize_bytes(&v.to_le_bytes())
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.serialize_bytes(&v.to_le_bytes())
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.serialize_bytes(&v.to_le_bytes())
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        self.serialize_bytes(&v.to_le_bytes())
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.serialize_bytes(&v.to_le_bytes())
    }

    fn serialize_u128(self, v: u128) -> Result<()> {
        self.serialize_bytes(&v.to_le_bytes())
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        self.serialize_bytes(&v.to_le_bytes())
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.serialize_bytes(&v.to_le_bytes())
    }

    fn serialize_char(self, v: char) -> Result<()> {
        self.serialize_u32(v.into())
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        self.serialize_u64(v.len() as u64)?;
        self.serialize_bytes(v.as_bytes())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        self.writer.write_all(v)?;
        Ok(())
    }

    fn serialize_none(self) -> Result<()> {
        self.serialize_unit()
    }

    // TODO: Figure out how to meaningfully distinguish between arbitrary some/none values
    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + Serialize,
    {
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

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
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

impl<'a, W: Write> ser::SerializeSeq for &'a mut Serializer<W> {
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

impl<'a, W: Write> ser::SerializeTuple for &'a mut Serializer<W> {
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

impl<'a, W: Write> ser::SerializeTupleStruct for &'a mut Serializer<W> {
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

impl<'a, W: Write> ser::SerializeTupleVariant for &'a mut Serializer<W> {
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

impl<'a, W: Write> ser::SerializeMap for &'a mut Serializer<W> {
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

impl<'a, W: Write> ser::SerializeStruct for &'a mut Serializer<W> {
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

impl<'a, W: Write> ser::SerializeStructVariant for &'a mut Serializer<W> {
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
