use anyhow::{bail, Result};
use sqlite_starter_rust::{
    header::PageHeader, record::parse_record, schema::Schema, varint::parse_varint,
};
use std::convert::TryInto;
use std::fs::File;
use std::io::prelude::*;

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
    match command.as_str() {
        ".dbinfo" => {
            let schemas = get_schemas(&database)?;
            println!("number of tables: {}", schemas.len());
        }
        ".tables" => {
            let schemas = get_schemas(&database)?;
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
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

fn get_schemas(database: &[u8]) -> Result<Vec<Schema>> {
    // Parse page header from database
    let page_header = PageHeader::parse(&database[100..108])?;

    // Obtain all cell pointers
    database[108..]
        .chunks_exact(2)
        .take(page_header.number_of_cells.into())
        .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()))
        .map(|cell_pointer| {
            let stream = &database[cell_pointer as usize..];
            let (_, offset) = parse_varint(stream);
            let (_rowid, read_bytes) = parse_varint(&stream[offset..]);
            parse_record(&stream[offset + read_bytes..], 5)
                .map(|record| Schema::parse(record).expect("Invalid record"))
        })
        .collect::<Result<Vec<_>>>()
}
