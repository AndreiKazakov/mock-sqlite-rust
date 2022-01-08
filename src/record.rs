use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt::{Display, Formatter};

use anyhow::{bail, Result};

use crate::varint::parse_varint;

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    I8(i8),
    I16(i16),
    I24(i32),
    I32(i32),
    I48(i64),
    I64(i64),
    F(f64),
    Blob(Vec<u8>),
    Text(String),
}

impl Value {
    pub fn get_numeric_value(&self) -> Result<f64> {
        match self {
            Value::I8(n) => Ok(*n as f64),
            Value::I16(n) => Ok(*n as f64),
            Value::I24(n) => Ok(*n as f64),
            Value::I32(n) => Ok(*n as f64),
            Value::I48(n) => Ok(*n as f64),
            Value::I64(n) => Ok(*n as f64),
            Value::F(n) => Ok(*n),
            _ => bail!("No numeric value"),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if let (Ok(s), Ok(o)) = (self.get_numeric_value(), other.get_numeric_value()) {
            s.partial_cmp(&o)
        } else {
            match (self, other) {
                (Value::Null, Value::Null) => Some(Ordering::Equal),
                (Value::Text(s), Value::Text(o)) => s.partial_cmp(o),
                _ => None,
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        if let (Ok(s), Ok(o)) = (self.get_numeric_value(), other.get_numeric_value()) {
            s == o
        } else {
            match (self, other) {
                (Value::Null, Value::Null) => true,
                (Value::Text(a), Value::Text(b)) => a == b,
                _ => false,
            }
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::I8(v) => write!(f, "{}", v),
            Value::I16(v) => write!(f, "{}", v),
            Value::I24(v) => write!(f, "{}", v),
            Value::I32(v) => write!(f, "{}", v),
            Value::I48(v) => write!(f, "{}", v),
            Value::I64(v) => write!(f, "{}", v),
            Value::F(v) => write!(f, "{}", v),
            Value::Blob(v) => write!(f, "{}", String::from_utf8_lossy(v).to_string()),
            Value::Text(v) => write!(f, "{}", v),
        }
    }
}

/// Reads SQLite's "Record Format" as mentioned here:
/// [record_format](https://www.sqlite.org/fileformat.html#record_format)
pub fn parse_record(stream: &[u8], column_count: usize) -> Result<Vec<Value>> {
    // Parse number of bytes in header, and use bytes_read as offset
    let (_, mut offset) = parse_varint(stream);

    // Read each varint into serial types and modify the offset
    let mut serial_types = vec![];
    for _ in 0..column_count {
        let (varint, read_bytes) = parse_varint(&stream[offset..]);
        offset += read_bytes;
        serial_types.push(varint);
    }

    // Parse each serial type as column into record and modify the offset
    let mut record = vec![];
    for serial_type in serial_types {
        let (column, column_len) = parse_column_value(&stream[offset..], serial_type as usize)?;
        offset += column_len;
        record.push(column);
    }

    Ok(record)
}

fn parse_column_value(stream: &[u8], serial_type: usize) -> Result<(Value, usize)> {
    let (column_value, offset) = match serial_type {
        0 => (Value::Null, 0),
        // 8 bit twos-complement integer
        1 => (Value::I8(i8::from_be_bytes([stream[0]])), 1),
        2 => (Value::I16(i16::from_be_bytes(stream[0..2].try_into()?)), 2),
        3 => (
            Value::I24(i32::from_be_bytes([0, stream[0], stream[1], stream[2]])),
            3,
        ),
        4 => (Value::I32(i32::from_be_bytes(stream[0..4].try_into()?)), 4),
        9 => (Value::I8(1), 0),
        // Text encoding
        n if serial_type >= 13 && serial_type % 2 == 1 => {
            let n_bytes = (n - 13) / 2;
            let bytes = stream[0..n_bytes as usize].to_vec();
            (
                Value::Text(String::from_utf8_lossy(&bytes).to_string()),
                bytes.len(),
            )
        }
        n if serial_type >= 12 && serial_type % 2 == 0 => {
            let n_bytes = (n - 12) / 2;
            let bytes = stream[0..n_bytes as usize].to_vec();
            let len = bytes.len();
            (Value::Blob(bytes), len)
        }
        _ => bail!("Invalid serial_type: {}", serial_type),
    };
    Ok((column_value, offset))
}
