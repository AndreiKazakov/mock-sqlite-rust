use anyhow::{bail, Error, Result};

use crate::record::Value;
use crate::sql::{Column, CreateTable};

#[derive(Debug)]
pub struct Schema {
    pub kind: String,
    pub name: String,
    pub table_name: String,
    pub root_page: u8,
    pub sql: Option<CreateTable>,
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
                sql: Some(CreateTable::parse(sql)?),
            }),
            (
                Some(Value::Text(kind)),
                Some(Value::Text(name)),
                Some(Value::Text(table_name)),
                Some(Value::I8(root_page)),
                Some(Value::Null),
            ) => Ok(Self {
                kind,
                name,
                table_name,
                root_page: root_page as u8,
                sql: None,
            }),
            cols => bail!("Wrong schema format: {:?}", cols),
        }
    }

    pub fn columns(&self) -> Result<&Vec<Column>> {
        self.sql
            .as_ref()
            .map(|ct| &ct.columns)
            .ok_or_else(|| Error::msg("sqlite_schema.sql is NULL"))
    }
}
