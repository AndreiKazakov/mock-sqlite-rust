use anyhow::{bail, Result};
use sqlite_starter_rust::db_header::DBHeader;
use sqlite_starter_rust::record::Value;
use sqlite_starter_rust::{
    page_header::PageHeader, record::parse_record, schema::Schema, varint::parse_varint,
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
        query if query.starts_with("SELECT") => {
            let table = query.split(' ').last().unwrap();
            let page_address = match schemas.into_iter().find(|s| s.table_name == table) {
                None => bail!("Table {} not found", table),
                Some(schema) => (schema.root_page as usize - 1) * db_header.page_size,
            };
            let payload = get_payload(
                &database[page_address..page_address + db_header.page_size],
                3, // hard-coded for now
                false,
            )?;
            println!("{}", payload.len())
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
