use std::convert::TryInto;
use std::fs::File;
use std::io::prelude::*;

use anyhow::{bail, Error, Result};
use nom::character::complete::{alphanumeric1, multispace0};
use nom::sequence::{delimited, pair, preceded};
use nom::{bytes::complete::tag_no_case, error, Err};

use sqlite_starter_rust::db_header::DBHeader;
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
    let schemas = get_schemas(&database[..])?;

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
            // let payload = get_payload(
            //     &database[page_address..page_address + db_header.page_size],
            //     3, // hard-coded for now
            //     false,
            // )?;
            let page_header = PageHeader::parse(&database[page_address..page_address + 8])?;
            println!("{}", page_header.number_of_cells)
        }
        query if query.to_lowercase().starts_with("select") => {
            let (_, (col, table)) = pair(
                preceded(
                    tag_no_case("select"),
                    delimited(multispace0, alphanumeric1, multispace0),
                ),
                preceded(
                    tag_no_case("from"),
                    delimited(multispace0, alphanumeric1, multispace0),
                ),
            )(query)
            .map_err(|err: Err<error::Error<&str>>| Error::msg(err.to_string()))?;

            let schema = schemas
                .iter()
                .find(|&s| s.table_name == table)
                .ok_or_else(|| Error::msg(format!("Table {} not found", table)))?;

            let columns = schema.columns()?;
            let col_index = columns
                .iter()
                .position(|c| c == col)
                .ok_or_else(|| Error::msg(format!("Column {} not found", col)))?;

            let page_address = (schema.root_page as usize - 1) * db_header.page_size;

            let payload = get_payload(
                &database[page_address..page_address + db_header.page_size],
                columns.len(),
                false,
            )?;

            let values: Vec<String> = payload
                .into_iter()
                .map(|row| format!("{}", row[col_index]))
                .collect();

            println!("{}", values.join("\n"))
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

fn get_schemas(database: &[u8]) -> Result<Vec<Schema>> {
    Ok(get_payload(database, 5, true)?
        .into_iter()
        .map(|record| Schema::parse(record).expect("Invalid record"))
        .collect::<Vec<_>>())
}

fn get_payload(
    database: &[u8],
    column_count: usize,
    is_first_page: bool,
) -> Result<Vec<Vec<Value>>> {
    let offset = if is_first_page { 100 } else { 0 };
    // Parse page header from database
    let page_header = PageHeader::parse(&database[offset..offset + 8])?;

    // Obtain all cell pointers
    database[offset + 8..]
        .chunks_exact(2)
        .take(page_header.number_of_cells.into())
        .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()))
        .map(|cell_pointer| {
            let stream = &database[cell_pointer as usize..];
            let (_, offset) = parse_varint(stream);
            let (_rowid, read_bytes) = parse_varint(&stream[offset..]);
            parse_record(&stream[offset + read_bytes..], column_count)
        })
        .collect::<Result<Vec<_>>>()
}
