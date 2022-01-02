use crate::record::Value;
use anyhow::{Error, Result};
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, tag_no_case};
use nom::character::complete::multispace0;
use nom::combinator::opt;
use nom::multi::separated_list1;
use nom::number::complete::double;
use nom::sequence::{delimited, preceded, separated_pair, tuple};
use nom::{error, Err, Parser};

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
