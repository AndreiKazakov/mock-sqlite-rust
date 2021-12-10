use anyhow::Result;
use std::convert::TryInto;

#[derive(Debug)]
pub struct DBHeader {
    pub page_size: usize,
}

impl DBHeader {
    /// Parses a database header stream into a database header
    pub fn parse(stream: &[u8]) -> Result<Self> {
        let page_size = u16::from_be_bytes(stream[16..18].try_into()?) as usize;
        let header = DBHeader { page_size };
        Ok(header)
    }
}
