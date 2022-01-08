use anyhow::Result;

use crate::record::{parse_record, Value};
use crate::varint::parse_varint;

pub struct TableLeafCell<'a> {
    rowid: usize,
    payload: &'a [u8],
}
impl<'a> TableLeafCell<'a> {
    pub fn parse(stream: &'a [u8]) -> Self {
        let (payload_size, payload_size_len) = parse_varint(stream);
        let (rowid, rowid_len) = parse_varint(&stream[payload_size_len..]);
        let offset = payload_size_len + rowid_len;
        let payload = &stream[offset..offset + payload_size];
        Self { rowid, payload }
    }

    pub fn get_record(&self, column_count: usize) -> Result<Vec<Value>> {
        parse_record(self.payload, column_count).map(|mut v| {
            if v[0] == Value::Null {
                v[0] = Value::I64(self.rowid as i64);
            }
            v
        })
    }
}

pub struct TableInteriorCell {
    pub left_child_page: usize,
    pub key: usize,
}

impl TableInteriorCell {
    pub fn parse(stream: &[u8]) -> Self {
        let left_child_page =
            u32::from_be_bytes([stream[0], stream[1], stream[2], stream[3]]) as usize;
        let (key, _read_bytes) = parse_varint(&stream[4..]);
        Self {
            left_child_page,
            key,
        }
    }
}

pub struct IndexLeafCell<'a> {
    payload: &'a [u8],
}
impl<'a> IndexLeafCell<'a> {
    pub fn parse(stream: &'a [u8]) -> Self {
        let (payload_size, payload_size_bytes) = parse_varint(stream);
        let payload = &stream[payload_size_bytes..payload_size_bytes + payload_size];
        Self { payload }
    }

    pub fn get_record(&self, column_count: usize) -> Result<Vec<Value>> {
        parse_record(self.payload, column_count)
    }
}

pub struct IndexInteriorCell<'a> {
    pub left_child_page: usize,
    payload: &'a [u8],
}

impl<'a> IndexInteriorCell<'a> {
    pub fn parse(stream: &'a [u8]) -> Self {
        let left_child_page =
            u32::from_be_bytes([stream[0], stream[1], stream[2], stream[3]]) as usize;
        let (payload_size, key_bytes) = parse_varint(&stream[4..]);
        Self {
            left_child_page,
            payload: &stream[4 + key_bytes..4 + key_bytes + payload_size],
        }
    }
    pub fn get_record(&self, column_count: usize) -> Result<Vec<Value>> {
        parse_record(self.payload, column_count)
    }
}
