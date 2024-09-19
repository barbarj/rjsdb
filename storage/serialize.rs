use std::{
    error::Error,
    fmt::Display,
    io::{self, Read, Write},
    rc::Rc,
    string,
};

use crate::{Char, DbType, DbValue, NumericValue, NumericValueSign, Row, Schema, Timestamp};

#[derive(Debug)]
pub enum SerdeError {
    IoError(Box<dyn Error>),
    InvalidInputs,
}
impl Display for SerdeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IoError(err) => err.fmt(f),
            Self::InvalidInputs => f.write_str("Invalid input"),
        }
    }
}
impl Error for SerdeError {
    fn cause(&self) -> Option<&dyn Error> {
        match self {
            Self::IoError(err) => Some(err.as_ref()),
            Self::InvalidInputs => Some(self),
        }
    }
}
impl From<io::Error> for SerdeError {
    fn from(value: io::Error) -> Self {
        Self::IoError(Box::new(value))
    }
}
impl From<string::FromUtf8Error> for SerdeError {
    fn from(value: string::FromUtf8Error) -> Self {
        Self::IoError(Box::new(value))
    }
}

type Result<T> = std::result::Result<T, SerdeError>;

trait Serialize {
    fn write_to_bytes(&self, dest: &mut impl Write) -> Result<()>;
}
trait Deserialize
where
    Self: Sized,
{
    type ExtraInfo;
    fn from_bytes(from: &mut impl Read, extra: &Self::ExtraInfo) -> Result<Self>;
}

impl Serialize for u16 {
    fn write_to_bytes(&self, dest: &mut impl Write) -> Result<()> {
        dest.write_all(&self.to_be_bytes())?;
        Ok(())
    }
}
impl Deserialize for u16 {
    type ExtraInfo = ();
    fn from_bytes(from: &mut impl Read, _extra: &()) -> Result<Self> {
        let mut buf = [0; 2];
        from.read_exact(&mut buf)?;
        Ok(u16::from_be_bytes(buf))
    }
}

impl Serialize for u64 {
    fn write_to_bytes(&self, dest: &mut impl Write) -> Result<()> {
        dest.write_all(&self.to_be_bytes())?;
        Ok(())
    }
}
impl Deserialize for u64 {
    type ExtraInfo = ();
    fn from_bytes(from: &mut impl Read, _extra: &()) -> Result<Self> {
        let mut buf = [0; 8];
        from.read_exact(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    }
}

impl Serialize for i32 {
    fn write_to_bytes(&self, dest: &mut impl Write) -> Result<()> {
        dest.write_all(&self.to_be_bytes())?;
        Ok(())
    }
}
impl Deserialize for i32 {
    type ExtraInfo = ();
    fn from_bytes(from: &mut impl Read, _extra: &()) -> Result<Self> {
        let mut buf = [0; 4];
        from.read_exact(&mut buf)?;
        Ok(i32::from_be_bytes(buf))
    }
}

impl Serialize for f64 {
    fn write_to_bytes(&self, dest: &mut impl Write) -> Result<()> {
        dest.write_all(&self.to_be_bytes())?;
        Ok(())
    }
}
impl Deserialize for f64 {
    type ExtraInfo = ();
    fn from_bytes(from: &mut impl Read, _extra: &()) -> Result<Self> {
        let mut buf = [0; 8];
        from.read_exact(&mut buf)?;
        Ok(f64::from_be_bytes(buf))
    }
}

impl Serialize for String {
    fn write_to_bytes(&self, dest: &mut impl Write) -> Result<()> {
        (self.len() as u64).write_to_bytes(dest)?;
        dest.write_all(self.as_bytes())?;

        Ok(())
    }
}
impl Deserialize for String {
    type ExtraInfo = ();
    fn from_bytes(from: &mut impl Read, _extra: &()) -> Result<Self> {
        let size = u64::from_bytes(from, &())? as usize;
        let mut buf = vec![0; size];
        from.read_exact(&mut buf)?;
        let res = String::from_utf8(buf)?;
        Ok(res)
    }
}

impl Serialize for NumericValueSign {
    fn write_to_bytes(&self, dest: &mut impl Write) -> Result<()> {
        let byte = match self {
            Self::Positive => 1,
            Self::Negative => 0,
            Self::NaN => 2,
        };
        dest.write_all(&[byte])?;
        Ok(())
    }
}
impl Deserialize for NumericValueSign {
    type ExtraInfo = ();
    fn from_bytes(from: &mut impl Read, _extra: &()) -> Result<Self> {
        let mut buf = [0; 1];
        from.read_exact(&mut buf)?;
        match NumericValueSign::from_number(buf[0]) {
            Some(sign) => Ok(sign),
            None => Err(SerdeError::InvalidInputs),
        }
    }
}

impl Serialize for NumericValue {
    fn write_to_bytes(&self, dest: &mut impl Write) -> Result<()> {
        self.total_digits.write_to_bytes(dest)?;
        self.first_group_weight.write_to_bytes(dest)?;
        self.sign.write_to_bytes(dest)?;
        for digit_group in self.digits.iter() {
            digit_group.write_to_bytes(dest)?;
        }
        Ok(())
    }
}
impl Deserialize for NumericValue {
    type ExtraInfo = ();
    fn from_bytes(from: &mut impl Read, _extra: &()) -> Result<Self> {
        let total_digits = u16::from_bytes(from, &())?;
        let first_group_weight = u16::from_bytes(from, &())?;
        let sign = NumericValueSign::from_bytes(from, &())?;
        let mut digits = Vec::with_capacity(total_digits.into());

        let digit_groups_count = if total_digits % 4 == 0 {
            total_digits / 4
        } else {
            (total_digits / 4) + 1
        };
        let mut buf = [0; 2];
        for _ in 0..digit_groups_count {
            from.read_exact(&mut buf)?;
            digits.push(u16::from_be_bytes(buf));
        }
        Ok(NumericValue {
            total_digits,
            first_group_weight,
            sign,
            digits,
        })
    }
}

impl Serialize for Char {
    fn write_to_bytes(&self, dest: &mut impl Write) -> Result<()> {
        dest.write_all(self.v.as_bytes())?;
        Ok(())
    }
}
impl Deserialize for Char {
    type ExtraInfo = u32; // string size
    fn from_bytes(from: &mut impl Read, size: &u32) -> Result<Self> {
        let mut buf = vec![0; *size as usize];
        from.read_exact(&mut buf)?;
        let res = String::from_utf8(buf)?;
        Ok(Char { v: res })
    }
}

impl Serialize for Timestamp {
    fn write_to_bytes(&self, dest: &mut impl Write) -> Result<()> {
        self.v.write_to_bytes(dest)?;
        Ok(())
    }
}
impl Deserialize for Timestamp {
    type ExtraInfo = ();
    fn from_bytes(from: &mut impl Read, _extra: &()) -> Result<Self> {
        let mut buf = [0; 8];
        from.read_exact(&mut buf)?;
        let v = u64::from_be_bytes(buf);
        Ok(Timestamp { v })
    }
}

impl Serialize for DbValue {
    fn write_to_bytes(&self, dest: &mut impl Write) -> Result<()> {
        match self {
            Self::Numeric(nv) => nv.write_to_bytes(dest),
            Self::Integer(i) => i.write_to_bytes(dest),
            Self::Varchar(s) => s.write_to_bytes(dest),
            Self::Char(c) => c.write_to_bytes(dest),
            Self::Double(d) => d.write_to_bytes(dest),
            Self::Timestamp(t) => t.write_to_bytes(dest),
        }
    }
}
impl Deserialize for DbValue {
    type ExtraInfo = DbType;
    fn from_bytes(from: &mut impl Read, expected_type: &DbType) -> Result<Self> {
        match expected_type {
            DbType::Numeric(_) => Ok(DbValue::Numeric(NumericValue::from_bytes(from, &())?)),
            DbType::Integer => Ok(DbValue::Integer(i32::from_bytes(from, &())?)),
            DbType::Varchar => Ok(DbValue::Varchar(String::from_bytes(from, &())?)),
            DbType::Char(size) => Ok(DbValue::Char(Char::from_bytes(from, size)?)),
            DbType::Double => Ok(DbValue::Double(f64::from_bytes(from, &())?)),
            DbType::Timestamp => Ok(DbValue::Timestamp(Timestamp::from_bytes(from, &())?)),
        }
    }
}

impl Serialize for Row {
    fn write_to_bytes(&self, dest: &mut impl Write) -> Result<()> {
        // item count and types assumed known on read from schema
        for v in self.data.iter() {
            v.write_to_bytes(dest)?;
        }
        Ok(())
    }
}
impl Deserialize for Row {
    type ExtraInfo = Rc<Schema>;
    fn from_bytes(from: &mut impl Read, schema: &Rc<Schema>) -> Result<Self> {
        let mut data = Vec::with_capacity(schema.len());
        for db_type in schema.iter() {
            data.push(DbValue::from_bytes(from, db_type)?);
        }
        Ok(Row {
            data,
            schema: schema.clone(),
        })
    }
}

#[cfg(test)]
mod serde_tests {
    use crate::{generate::RNG, NumericCfg};

    use super::*;

    #[test]
    fn integer_serde() {
        let input = DbValue::Integer(553239);
        let mut bytes = Vec::new();
        input.write_to_bytes(&mut bytes).unwrap();
        let mut reader = &bytes[..];
        let read = DbValue::from_bytes(&mut reader, &DbType::Integer).unwrap();
        assert_eq!(input, read);
    }

    #[test]
    fn varchar_serde() {
        let input = DbValue::Varchar("This is a String".to_string());
        let mut bytes = Vec::new();
        input.write_to_bytes(&mut bytes).unwrap();
        let mut reader = &bytes[..];
        let read = DbValue::from_bytes(&mut reader, &DbType::Varchar).unwrap();
        assert_eq!(input, read);
    }

    #[test]
    fn char_serde() {
        let input = DbValue::Char(Char {
            v: "foobar".to_string(),
        });
        let mut bytes = Vec::new();
        input.write_to_bytes(&mut bytes).unwrap();
        let mut reader = &bytes[..];
        let read = DbValue::from_bytes(&mut reader, &DbType::Char(6)).unwrap();
        assert_eq!(input, read);
    }

    #[test]
    fn rows_serde() {
        let mut rng = RNG::from_seed(42);
        let numeric_cfg = Rc::new(NumericCfg {
            max_precision: 10,
            max_scale: 5,
        });
        let char_size = 5;
        let schema = Rc::new(vec![
            DbType::Numeric(numeric_cfg),
            DbType::Integer,
            DbType::Varchar,
            DbType::Char(char_size),
            DbType::Double,
            DbType::Timestamp,
        ]);
        let mut rows = Vec::with_capacity(3);
        for _ in 0..3 {
            let row = Row {
                data: schema
                    .iter()
                    .map(|t| t.as_generated_value(&mut rng))
                    .collect(),
                schema: schema.clone(),
            };
            rows.push(row);
        }

        let mut bytes = Vec::new();
        for row in rows.iter() {
            row.write_to_bytes(&mut bytes).unwrap();
        }
        let mut reader = &bytes[..];
        let read_rows: Vec<Row> = (0..3)
            .map(|_| Row::from_bytes(&mut reader, &schema).unwrap())
            .collect();

        assert_eq!(rows, read_rows);
    }
}
