use crate::cell::{TableIndexCell, TableLeafCell};
use crate::page_header::PageHeader;
use crate::record::Value;
use anyhow::{Context, Result};
use core::iter;
use std::convert::TryInto;

pub fn parse_table_leaf(
    stream: &[u8],
    db_header_offset: usize,
    page_header: PageHeader,
    column_count: usize,
) -> Result<Vec<Vec<Value>>> {
    collect_cell_pointers(
        &stream[db_header_offset + page_header.size()..],
        page_header.number_of_cells.into(),
    )
    .into_iter()
    .map(|cell_pointer| {
        let stream = &stream[cell_pointer as usize..];
        TableLeafCell::parse(stream).get_record(column_count)
    })
    .collect::<Result<Vec<_>>>()
}

pub fn parse_table_interior(
    stream: &[u8],
    db_header_offset: usize,
    page_header: PageHeader,
) -> Result<Vec<usize>> {
    let right_most_pointer = page_header
        .right_most_pointer
        .context("Right most pointer not found")?;
    let cells = &stream[db_header_offset + page_header.size()..];
    let pages = collect_cell_pointers(cells, page_header.number_of_cells.into())
        .into_iter()
        .map(|cell_pointer| {
            let stream = &stream[cell_pointer as usize..];
            TableIndexCell::parse(stream).left_child_page
        })
        .chain(iter::once(right_most_pointer as usize))
        .collect::<Vec<_>>();
    Ok(pages)
}

fn collect_cell_pointers(database: &[u8], number_of_cells: usize) -> Vec<u16> {
    database
        .chunks_exact(2)
        .take(number_of_cells)
        .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>()
}
