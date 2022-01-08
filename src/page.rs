use core::iter;
use std::convert::TryInto;

use anyhow::{Context, Result};

use crate::cell::{IndexInteriorCell, IndexLeafCell, TableInteriorCell, TableLeafCell};
use crate::page_header::PageHeader;
use crate::record::Value;

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
    .map(|ptr| TableLeafCell::parse(&stream[ptr as usize..]).get_record(column_count))
    .collect::<Result<Vec<_>>>()
}

pub struct TableBTree {
    pub left: Vec<TableInteriorCell>,
    pub right: usize,
}

impl TableBTree {
    pub fn pages(&self) -> Vec<usize> {
        self.left
            .iter()
            .map(|cell| cell.left_child_page)
            .chain(iter::once(self.right))
            .collect()
    }
}

pub fn parse_table_interior(
    stream: &[u8],
    db_header_offset: usize,
    page_header: PageHeader,
) -> Result<TableBTree> {
    let right = page_header
        .right_most_pointer
        .context("Right most pointer not found")? as usize;
    let cells = &stream[db_header_offset + page_header.size()..];
    let left = collect_cell_pointers(cells, page_header.number_of_cells.into())
        .into_iter()
        .map(|cell_pointer| TableInteriorCell::parse(&stream[cell_pointer as usize..]))
        .collect::<Vec<_>>();
    Ok(TableBTree { left, right })
}

pub fn parse_index_leaf(
    stream: &[u8],
    page_header: PageHeader,
    column_count: usize,
) -> Result<Vec<Vec<Value>>> {
    collect_cell_pointers(
        &stream[page_header.size()..],
        page_header.number_of_cells.into(),
    )
    .into_iter()
    .map(|ptr| IndexLeafCell::parse(&stream[ptr as usize..]).get_record(column_count))
    .collect::<Result<Vec<_>>>()
}

pub struct IndexBTree {
    pub left: Vec<(usize, Vec<Value>)>,
    pub right: usize,
}

pub fn parse_index_interior(
    stream: &[u8],
    page_header: PageHeader,
    column_count: usize,
) -> Result<IndexBTree> {
    let right = page_header
        .right_most_pointer
        .context("Right most pointer not found")? as usize;
    let cells = &stream[page_header.size()..];
    let cell_pointers = collect_cell_pointers(cells, page_header.number_of_cells.into());
    let mut left = Vec::with_capacity(cell_pointers.len());

    for pointer in cell_pointers {
        let cell = IndexInteriorCell::parse(&stream[pointer as usize..]);
        left.push((cell.left_child_page, cell.get_record(column_count + 1)?));
    }
    Ok(IndexBTree { left, right })
}

fn collect_cell_pointers(database: &[u8], number_of_cells: usize) -> Vec<u16> {
    database
        .chunks_exact(2)
        .take(number_of_cells)
        .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>()
}
