use anyhow::{Error, Result};
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, tag_no_case};
use nom::character::complete::{multispace0, multispace1};
use nom::combinator::opt;
use nom::multi::separated_list1;
use nom::number::complete::double;
use nom::sequence::{delimited, preceded, separated_pair, terminated, tuple};
use nom::{error, Err, Parser};

use crate::record::Value;
use crate::sql::CreateStatement::CreateIndex;

pub struct Select<'a> {
    pub columns: Vec<&'a str>,
    pub table: &'a str,
    pub filter: Option<(&'a str, Value)>,
}

impl<'a> Select<'a> {
    pub fn parse_select(query: &'a str) -> Result<Self> {
        let (_, (columns, table, filter)) = tuple((
            preceded(
                tag_no_case("select"),
                delimited(
                    multispace0,
                    separated_list1(
                        tag(","),
                        delimited(multispace0, is_not(" \t\r\n,"), multispace0),
                    ),
                    multispace0,
                ),
            ),
            preceded(
                tag_no_case("from"),
                delimited(multispace0, is_not(" \t\r\n,"), multispace0),
            ),
            opt(preceded(
                tag_no_case("where"),
                delimited(
                    multispace0,
                    separated_pair(
                        is_not(" \t\r\n="),
                        delimited(multispace0, tag("="), multispace0),
                        alt((
                            double.map(Value::F),
                            delimited(tag("'"), is_not("'"), tag("'"))
                                .map(|v: &str| Value::Text(v.to_string())),
                        )),
                    ),
                    multispace0,
                ),
            )),
        ))(query)
        .map_err(|err: Err<error::Error<&str>>| Error::msg(err.to_string()))?;
        Ok(Self {
            columns,
            table,
            filter,
        })
    }
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub data_type: Option<String>,
    pub is_primary_key: bool,
}

#[derive(Debug)]
pub enum CreateStatement {
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
    },
}

impl CreateStatement {
    pub fn parse(query: String) -> Result<Self> {
        if query.to_lowercase().starts_with("create table") {
            Self::parse_create_table(query)
        } else {
            Self::parse_create_index(query)
        }
    }
    pub fn parse_create_index(query: String) -> Result<Self> {
        let (_, (name, table, columns)) = tuple((
            preceded(
                tag_no_case("create index "),
                terminated(is_not(" \t\r\n,)("), multispace0),
            ),
            preceded(
                tag_no_case("on "),
                terminated(is_not(" \t\r\n,)("), multispace0),
            ),
            delimited(
                terminated(tag("("), multispace0),
                separated_list1(
                    delimited(multispace0, tag(","), multispace0),
                    is_not(" \t\r\n,)("),
                ),
                terminated(tag(")"), multispace0),
            ),
        ))(&*query)
        .map_err(|err: Err<error::Error<&str>>| Error::msg(err.to_string()))?;
        Ok(CreateIndex {
            name: name.to_string(),
            table: table.to_string(),
            columns: columns.into_iter().map(|s| s.to_string()).collect(),
        })
    }

    pub fn parse_create_table(query: String) -> Result<Self> {
        let (_, (name, cols)) = tuple((
            preceded(
                tag_no_case("create table "),
                terminated(is_not(" \t\r\n,)("), multispace0),
            ),
            delimited(
                terminated(tag("("), multispace0),
                separated_list1(
                    delimited(multispace0, tag(","), multispace0),
                    tuple((
                        alt((
                            is_not(" \t\r\n\",)"),
                            delimited(tag("\""), is_not("\""), tag("\"")),
                        )),
                        opt(preceded(multispace1, is_not(" \t\r\n,)"))),
                        opt(preceded(multispace1, tag_no_case("not null"))),
                        opt(preceded(
                            multispace1,
                            terminated(tag_no_case("primary key"), opt(tag(" autoincrement"))),
                        )),
                    )),
                ),
                preceded(multispace0, tag(")")),
            ),
        ))(&*query)
        .map_err(|err: Err<error::Error<&str>>| Error::msg(err.to_string()))?;

        let columns = cols
            .into_iter()
            .map(|(name, data_type, _not_null, pk)| Column {
                name: name.to_owned(),
                data_type: data_type.map(|s| s.to_owned()),
                is_primary_key: pk.is_some(),
            })
            .collect();

        Ok(Self::CreateTable {
            name: name.to_owned(),
            columns,
        })
    }
}
