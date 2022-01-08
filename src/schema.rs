use anyhow::{bail, Error, Result};

use crate::record::Value;
use crate::sql::CreateStatement;

#[derive(Debug)]
pub struct Schema {
    pub kind: String,
    pub name: String,
    pub table_name: String,
    pub root_page: usize,
    pub sql: Option<CreateStatement>,
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
                root_page: root_page as usize,
                sql: Some(CreateStatement::parse(sql)?),
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
                root_page: root_page as usize,
                sql: None,
            }),
            cols => bail!("Wrong schema format: {:?}", cols),
        }
    }

    pub fn columns(&self) -> Result<Vec<&String>> {
        match self
            .sql
            .as_ref()
            .ok_or_else(|| Error::msg("No create statement found"))?
        {
            CreateStatement::CreateTable { columns, .. } => {
                Ok(columns.iter().map(|c| &c.name).collect())
            }
            CreateStatement::CreateIndex { columns, .. } => Ok(columns.iter().collect()),
        }
    }
}
