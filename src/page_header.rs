use std::convert::TryInto;

use anyhow::{bail, Result};

#[derive(Debug, Eq, PartialEq)]
pub enum BTreePage {
    InteriorIndex = 2,
    InteriorTable = 5,
    LeafIndex = 10,
    LeafTable = 13,
}

#[derive(Debug)]
pub struct PageHeader {
    pub page_type: BTreePage,
    pub first_free_block_start: u16,
    pub number_of_cells: u16,
    pub start_of_content_area: u16,
    pub fragmented_free_bytes: u8,
    pub right_most_pointer: Option<u32>,
}

impl PageHeader {
    /// Parses a page header stream into a page header
    pub fn parse(stream: &[u8]) -> Result<Self> {
        let page_type = match stream[0] {
            2 => BTreePage::InteriorIndex,
            5 => BTreePage::InteriorTable,
            10 => BTreePage::LeafIndex,
            13 => BTreePage::LeafTable,
            x => bail!("Invalid page value encountered: {}", x),
        };
        let first_free_block_start = u16::from_be_bytes(stream[1..3].try_into()?);
        let number_of_cells = u16::from_be_bytes(stream[3..5].try_into()?);
        let start_of_content_area = u16::from_be_bytes(stream[5..7].try_into()?);
        let fragmented_free_bytes = stream[7];
        let right_most_pointer = match page_type {
            BTreePage::InteriorTable | BTreePage::InteriorIndex => {
                Some(u32::from_be_bytes(stream[8..12].try_into()?))
            }
            _ => None,
        };
        let header = PageHeader {
            page_type,
            first_free_block_start,
            number_of_cells,
            start_of_content_area,
            fragmented_free_bytes,
            right_most_pointer,
        };
        Ok(header)
    }

    pub fn size(&self) -> usize {
        match self.right_most_pointer {
            Some(_) => 12,
            None => 8,
        }
    }
}
