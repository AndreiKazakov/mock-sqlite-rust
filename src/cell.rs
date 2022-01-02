use crate::record::{parse_record, Value};
use crate::varint::parse_varint;
use anyhow::Result;

pub struct TableLeafCell<'a> {
    _payload_size: usize,
    rowid: usize,
    payload: &'a [u8],
}
impl<'a> TableLeafCell<'a> {
    pub fn parse(stream: &'a [u8]) -> Self {
        let (payload_size, payload_size_bytes) = parse_varint(stream);
        let (rowid, rowid_bytes) = parse_varint(&stream[payload_size_bytes..]);
        Self {
            _payload_size: payload_size,
            rowid,
            payload: &stream[payload_size_bytes + rowid_bytes..],
        }
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

pub struct TableIndexCell {
    pub left_child_page: usize,
    _key: usize,
}

impl TableIndexCell {
    pub fn parse(stream: &[u8]) -> Self {
        let left_child_page =
            u32::from_be_bytes([stream[0], stream[1], stream[2], stream[3]]) as usize;
        let (key, _read_bytes) = parse_varint(&stream[4..]);
        Self {
            left_child_page,
            _key: key,
        }
    }
}
