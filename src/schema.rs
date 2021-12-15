use anyhow::{bail, Error, Result};
use nom::bytes::complete::{is_not, tag, take_until};
use nom::character::complete::{alphanumeric1, multispace0, one_of};
use nom::multi::{many1, separated_list1};
use nom::sequence::{delimited, preceded, terminated};
use nom::{error, Err};

use crate::record::Value;

#[derive(Debug)]
pub struct Schema {
    pub kind: String,
    name: String,
    pub table_name: String,
    pub root_page: u8,
    pub sql: Option<String>,
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
                sql: Some(sql),
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

    pub fn columns(&self) -> Result<Vec<String>> {
        let sql = self
            .sql
            .as_ref()
            .ok_or_else(|| Error::msg("sqlite_schema.sql is NULL"))?;
        preceded(
            take_until("("),
            delimited(
                terminated(tag("("), multispace0),
                separated_list1(
                    many1(one_of(",\t\n ")),
                    terminated(alphanumeric1, is_not(",)")),
                ),
                preceded(multispace0, tag(")")),
            ),
        )(&**sql)
        .map(|(_, res)| res.iter().map(|&s| s.to_owned()).collect())
        .map_err(|err: Err<error::Error<&str>>| Error::msg(err.to_string()))
    }
}
