use crate::db_header::DBHeader;
use std::fs::File;
use std::os::unix::fs::FileExt;

use crate::page::{parse_index_interior, parse_index_leaf, parse_table_interior, parse_table_leaf};
use crate::page_header::{BTreePage, PageHeader};
use crate::record::Value;
use crate::schema::Schema;
use crate::sql::Select;
use anyhow::{bail, Error, Result};
use std::collections::HashMap;

pub struct DB {
    file: File,
    header: DBHeader,
}

impl DB {
    pub fn new(file_name: &str) -> Result<Self> {
        let file = File::open(file_name)?;
        let db_header_stream = &mut [0u8; 100];
        file.read_exact_at(db_header_stream, 0)?;
        let header = DBHeader::parse(db_header_stream)?;
        Ok(Self { file, header })
    }

    pub fn tables(&self) -> Result<Vec<String>> {
        Ok(self
            .get_schemas()?
            .into_iter()
            .filter(|s| s.kind == "table" && s.table_name != "sqlite_sequence")
            .map(|s| s.table_name)
            .collect())
    }

    pub fn count(&self, table: &str) -> Result<usize> {
        let page_number = match self.get_schemas()?.iter().find(|s| s.name == table) {
            None => bail!("Table {} not found", table),
            Some(schema) => schema.root_page,
        };
        let (_, page_header, _) = self.read_page(page_number)?;
        Ok(page_header.number_of_cells as usize)
    }

    pub fn select(&self, select: Select) -> Result<Vec<Vec<Value>>> {
        let schemas = self.get_schemas()?;
        let index = find_applicable_index(&select, &schemas)?;

        let schema = schemas
            .iter()
            .find(|s| s.name == select.table)
            .ok_or_else(|| Error::msg(format!("Table {} not found", select.table)))?;

        let columns = schema.columns()?;
        let indices: HashMap<&String, usize> =
            columns.iter().enumerate().map(|(i, &v)| (v, i)).collect();
        let selected_indices = get_selected_column_indices(&select, &indices)?;

        let rows = match (select.filter, index) {
            (None, _) => self.get_payload(columns.len(), schema.root_page, None)?,
            (Some((_, val)), Some(ind)) => {
                let cols_in_index = ind.columns()?.len();
                let keys = self.search_in_index(cols_in_index, ind.root_page, &val, vec![])?;
                self.get_payload(columns.len(), schema.root_page, Some(keys))?
            }
            (Some((col, val)), None) => {
                let filter_index = indices
                    .get(&col.to_owned())
                    .copied()
                    .ok_or_else(|| Error::msg(format!("Column {} not found", col)))?;

                self.get_payload(columns.len(), schema.root_page, None)?
                    .into_iter()
                    .filter(|row| row[filter_index] == val)
                    .collect()
            }
        };

        Ok(rows
            .into_iter()
            .map(|row| {
                selected_indices
                    .iter()
                    .map(|&i| row[i].clone())
                    .collect::<Vec<_>>()
            })
            .collect())
    }

    fn search_in_index(
        &self,
        column_count: usize,
        page_number: usize,
        value: &Value,
        mut buffer: Vec<usize>,
    ) -> Result<Vec<usize>> {
        let (_, page_header, page) = self.read_page(page_number)?;

        match page_header.page_type {
            BTreePage::InteriorIndex => {
                let index_btree = parse_index_interior(&page, page_header, column_count)?;
                let branch = index_btree.left.into_iter().find(|(_, vs)| value <= &vs[0]);
                if let Some((_, vs)) = &branch {
                    buffer.push(vs[vs.len() - 1].get_numeric_value().unwrap() as usize)
                }
                let page = branch.map(|(page, _)| page).unwrap_or(index_btree.right);
                self.search_in_index(column_count, page, value, buffer)
            }
            BTreePage::LeafIndex => {
                let res: Vec<usize> = parse_index_leaf(&page, page_header, column_count + 1)?
                    .into_iter()
                    .filter(|row| &row[0] == value)
                    .map(|row| row[column_count].get_numeric_value().unwrap() as usize)
                    .collect();
                buffer.extend(res);
                Ok(buffer)
            }
            _ => bail!("This is a table, not an index"),
        }
    }

    fn get_schemas(&self) -> Result<Vec<Schema>> {
        Ok(self
            .get_payload(5, 1, None)?
            .into_iter()
            .map(|record| Schema::parse(record).expect("Invalid record"))
            .collect::<Vec<_>>())
    }

    fn get_payload(
        &self,
        column_count: usize,
        page_number: usize,
        keys: Option<Vec<usize>>,
    ) -> Result<Vec<Vec<Value>>> {
        let (offset, page_header, page) = self.read_page(page_number)?;

        match (&page_header.page_type, keys) {
            (BTreePage::LeafTable, None) => {
                parse_table_leaf(&page, offset, page_header, column_count)
            }
            (BTreePage::InteriorTable, None) => parse_table_interior(&page, offset, page_header)?
                .pages()
                .iter()
                .map(|&page| self.get_payload(column_count, page, None))
                .collect::<Result<Vec<_>>>()
                .map(|contents| contents.into_iter().flatten().collect::<Vec<_>>()),
            (BTreePage::LeafTable, Some(pks)) => {
                let res = parse_table_leaf(&page, offset, page_header, column_count)?
                    .into_iter()
                    .filter(|vs| pks.contains(&(vs[0].get_numeric_value().unwrap() as usize)))
                    .collect();
                Ok(res)
            }
            (BTreePage::InteriorTable, Some(pks)) => {
                let tree = parse_table_interior(&page, offset, page_header)?;
                let mut pages_and_keys = HashMap::new();

                for key in pks {
                    let page = tree
                        .left
                        .iter()
                        .find(|cell| key <= cell.key)
                        .map(|cell| cell.left_child_page)
                        .unwrap_or(tree.right);
                    pages_and_keys
                        .entry(page)
                        .or_insert_with(Vec::new)
                        .push(key)
                }

                pages_and_keys
                    .into_iter()
                    .map(|(p, page_keys)| self.get_payload(column_count, p, Some(page_keys)))
                    .collect::<Result<Vec<_>>>()
                    .map(|contents| contents.into_iter().flatten().collect::<Vec<_>>())
            }
            _ => bail!("Cannot read from index"),
        }
    }

    fn read_page(&self, page_number: usize) -> Result<(usize, PageHeader, Vec<u8>)> {
        let db_header_offset = if page_number == 1 { 100 } else { 0 };
        let page_address = ((page_number - 1) * self.header.page_size) as u64;

        let mut page = vec![0; self.header.page_size];
        self.file.read_exact_at(&mut page, page_address)?;
        let page_header = PageHeader::parse(&page[db_header_offset..])?;
        Ok((db_header_offset, page_header, page))
    }
}

fn find_applicable_index<'a, 'b>(
    select: &'a Select,
    schemas: &'b [Schema],
) -> Result<Option<&'b Schema>> {
    match select.filter {
        Some((k, _)) => {
            let mut res = None;
            for s in schemas.iter() {
                let is_index_applicable = match s.columns() {
                    Ok(cols) => cols.contains(&&k.to_string()),
                    _ => false,
                };
                if s.kind == "index" && s.table_name == select.table && is_index_applicable {
                    res = Some(s);
                    break;
                }
            }
            Ok(res)
        }
        None => Ok(None),
    }
}

fn get_selected_column_indices(
    select: &Select,
    indices: &HashMap<&String, usize>,
) -> Result<Vec<usize>> {
    let selected_indices = select
        .columns
        .iter()
        .map(|&c| {
            indices
                .get(&c.to_owned())
                .copied()
                .ok_or_else(|| Error::msg(format!("Column {} not found", c)))
        })
        .collect::<Result<Vec<usize>>>()?;

    if selected_indices.is_empty() {
        bail!("Columns {} not found", select.columns.join(","))
    }

    Ok(selected_indices)
}
