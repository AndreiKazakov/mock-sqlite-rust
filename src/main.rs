use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

use anyhow::{bail, Error, Result};

use sqlite_starter_rust::db_header::DBHeader;
use sqlite_starter_rust::page::{parse_table_interior, parse_table_leaf};
use sqlite_starter_rust::page_header::BTreePage;
use sqlite_starter_rust::record::Value;
use sqlite_starter_rust::sql::Select;
use sqlite_starter_rust::{page_header::PageHeader, schema::Schema};

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
            let page_address = match schemas.into_iter().find(|s| s.name == table) {
                None => bail!("Table {} not found", table),
                Some(schema) => (schema.root_page as usize - 1) * db_header.page_size,
            };
            let page_header =
                PageHeader::parse(&database[page_address..page_address + db_header.page_size])?;
            println!("{}", page_header.number_of_cells)
        }
        query if query.to_lowercase().starts_with("select") => {
            let select = Select::parse_select(query)?;

            let schema = schemas
                .iter()
                .find(|&s| s.name == select.table)
                .ok_or_else(|| Error::msg(format!("Table {} not found", select.table)))?;

            let columns = schema.columns()?;
            let indices: HashMap<&String, usize> =
                columns.iter().enumerate().map(|(i, v)| (v, i)).collect();
            let col_indices = select
                .columns
                .iter()
                .map(|&c| {
                    indices
                        .get(&c.to_owned())
                        .copied()
                        .ok_or_else(|| Error::msg(format!("Column {} not found", c)))
                })
                .collect::<Result<Vec<usize>>>()?;

            if col_indices.is_empty() {
                bail!("Columns {} not found", select.columns.join(","))
            }

            let payload = get_payload(
                &database[..],
                columns.len(),
                db_header.page_size,
                schema.root_page as usize,
            )?;

            let filter_index = if let Some((col, val)) = select.filter {
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
    let page_header = PageHeader::parse(&database[db_header_offset + page_address..])?;

    if page_header.page_type == BTreePage::LeafTable {
        parse_table_leaf(
            &database[page_address..],
            db_header_offset,
            page_header,
            column_count,
        )
    } else {
        parse_table_interior(&database[page_address..], db_header_offset, page_header)?
            .into_iter()
            .map(|page| get_payload(database, column_count, page_size, page))
            .collect::<Result<Vec<_>>>()
            .map(|contents| contents.into_iter().flatten().collect::<Vec<_>>())
    }
}
