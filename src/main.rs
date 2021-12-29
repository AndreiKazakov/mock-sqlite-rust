use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::File;
use std::io::Read;
use std::iter;

use anyhow::{bail, Context, Error, Result};
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, tag_no_case};
use nom::character::complete::{alphanumeric1, multispace0};
use nom::combinator::opt;
use nom::multi::separated_list1;
use nom::number::complete::double;
use nom::sequence::{delimited, preceded, separated_pair, tuple};
use nom::{error, Err, Parser};

use sqlite_starter_rust::db_header::DBHeader;
use sqlite_starter_rust::page_header::BTreePage;
use sqlite_starter_rust::record::Value;
use sqlite_starter_rust::{
    page_header::PageHeader, record::parse_record, schema::Schema, varint::parse_varint,
};

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Read database file into database
    let mut file = File::open(&args[1])?;
    let mut database = Vec::new();
    file.read_to_end(&mut database)?;

    // Parse command and act accordingly
    let command = &args[2];
    let db_header = DBHeader::parse(&database)?;
    let schemas = get_schemas(&database[..], db_header.page_size)?;

    match command.as_str() {
        ".dbinfo" => {
            println!("number of tables: {}", schemas.len());
        }
        ".tables" => {
            println!(
                "{}",
                schemas
                    .into_iter()
                    .filter(|s| s.kind == "table" && s.table_name != "sqlite_sequence")
                    .map(|s| s.table_name)
                    .collect::<Vec<String>>()
                    .join(" ")
            );
        }
        query if query.to_lowercase().starts_with("select count") => {
            let table = query.split(' ').last().unwrap();
            let page_address = match schemas.into_iter().find(|s| s.table_name == table) {
                None => bail!("Table {} not found", table),
                Some(schema) => (schema.root_page as usize - 1) * db_header.page_size,
            };
            let page_header = PageHeader::parse(&database[page_address..])?;
            println!("{}", page_header.number_of_cells)
        }
        query if query.to_lowercase().starts_with("select") => {
            let (_, (cols, table, filter)) = tuple((
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
                    delimited(multispace0, alphanumeric1, multispace0),
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

            let schema = schemas
                .iter()
                .find(|&s| s.table_name == table)
                .ok_or_else(|| Error::msg(format!("Table {} not found", table)))?;

            let columns = schema.columns()?;
            let indices: HashMap<&String, usize> =
                columns.iter().enumerate().map(|(i, v)| (v, i)).collect();
            let col_indices = cols
                .iter()
                .map(|&c| {
                    indices
                        .get(&c.to_owned())
                        .copied()
                        .ok_or_else(|| Error::msg(format!("Column {} not found", c)))
                })
                .collect::<Result<Vec<usize>>>()?;

            if col_indices.is_empty() {
                bail!("Columns {} not found", cols.join(","))
            }

            let payload = get_payload(
                &database[..],
                columns.len(),
                db_header.page_size,
                schema.root_page as usize,
            )?;

            let filter_index = if let Some((col, val)) = filter {
                let i = indices
                    .get(&col.to_owned())
                    .copied()
                    .ok_or_else(|| Error::msg(format!("Column {} not found", col)))?;
                Some((i, val))
            } else {
                None
            };

            let values: Vec<String> = payload
                .into_iter()
                .filter(|row| match &filter_index {
                    None => true,
                    Some((col, val)) => &row[*col] == val,
                })
                .map(|row| {
                    col_indices
                        .iter()
                        .map(|&i| row[i].to_string())
                        .collect::<Vec<String>>()
                        .join("|")
                })
                .collect();

            println!("{}", values.join("\n"))
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

fn get_schemas(database: &[u8], page_size: usize) -> Result<Vec<Schema>> {
    Ok(get_payload(database, 5, page_size, 1)?
        .into_iter()
        .map(|record| Schema::parse(record).expect("Invalid record"))
        .collect::<Vec<_>>())
}

fn get_payload(
    database: &[u8],
    column_count: usize,
    page_size: usize,
    page_number: usize,
) -> Result<Vec<Vec<Value>>> {
    let db_header_offset = if page_number == 1 { 100 } else { 0 };
    let page_address = (page_number as usize - 1) * page_size;

    // Parse page header from database
    let page_header = PageHeader::parse(&database[db_header_offset + page_address..])?;

    if page_header.page_type == BTreePage::LeafTable {
        collect_cell_pointers(
            &database[db_header_offset + page_address + page_header.size()..],
            page_header.number_of_cells.into(),
        )
        .into_iter()
        .map(|cell_pointer| {
            let stream = &database[page_address + cell_pointer as usize..];
            let (_, offset) = parse_varint(stream);
            let (rowid, read_bytes) = parse_varint(&stream[offset..]);
            parse_record(&stream[offset + read_bytes..], column_count).map(|mut v| {
                if v[0] == Value::Null {
                    v[0] = Value::I64(rowid as i64);
                }
                v
            })
        })
        .collect::<Result<Vec<_>>>()
    } else {
        let right_most_pointer = page_header
            .right_most_pointer
            .context("Right most pointer not found")?;
        let cells = &database[db_header_offset + page_address + page_header.size()..];
        collect_cell_pointers(cells, page_header.number_of_cells.into())
            .into_iter()
            .map(|cell_pointer| {
                let stream = &database[page_address + cell_pointer as usize..];
                let page = u32::from_be_bytes([stream[0], stream[1], stream[2], stream[3]]);
                let (_rowid, _read_bytes) = parse_varint(&stream[4..]);
                page
            })
            .chain(iter::once(right_most_pointer))
            .map(|page| get_payload(database, column_count, page_size, page as usize))
            .collect::<Result<Vec<_>>>()
            .map(|contents| contents.into_iter().flatten().collect::<Vec<_>>())
    }
}

fn collect_cell_pointers(database: &[u8], number_of_cells: usize) -> Vec<u16> {
    database
        .chunks_exact(2)
        .take(number_of_cells)
        .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>()
}
