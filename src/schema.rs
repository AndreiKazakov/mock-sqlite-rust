use crate::record::Value;
use anyhow::{bail, Result};

#[derive(Debug)]
pub struct Schema {
    pub kind: String,
    name: String,
    pub table_name: String,
    pub root_page: u8,
    sql: String,
}

impl Schema {
    /// Parses a record into a schema
    pub fn parse(record: Vec<Value>) -> Result<Self> {
        let mut items = record.into_iter();
        match (
            items.next(),
            items.next(),
            items.next(),
            items.next(),
            items.next(),
        ) {
            (
                Some(Value::Text(kind)),
                Some(Value::Text(name)),
                Some(Value::Text(table_name)),
                Some(Value::I8(root_page)),
                Some(Value::Text(sql)),
            ) => Ok(Self {
                kind,
                name,
                table_name,
                root_page: root_page as u8,
                sql,
            }),
            _ => bail!("Wrong schema format"),
        }
    }
}
