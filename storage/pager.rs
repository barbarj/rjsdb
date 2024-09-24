use std::{cell::Cell, io::Write, path::Iter};

use crate::serialize::{Deserialize, SerdeError, Serialize};

const PAGE_SIZE: u32 = 4096; //4 KB
const PAGE_HEADER_SIZE: u32 = 16;
const PAGE_DATA_SIZE: usize = (PAGE_SIZE - PAGE_HEADER_SIZE) as usize;

// flags
const COMPACTIBLE_FLAG: u8 = 0b1;

enum PageKind {
    Data,
}

/// free_start and free_end are relative to the end of the header, not the beginning of the page
struct PageHeader {
    id: u32,
    free_start: u32, // starts at 0.
    free_end: u32,
    kind: PageKind,
    flags: u8,
}

struct Page {
    header: PageHeader,
    data: [u8; PAGE_DATA_SIZE],
}
impl Page {
    fn new(kind: PageKind, id: u32) -> Self {
        Page {
            header: PageHeader {
                id,
                free_start: 0,
                free_end: PAGE_DATA_SIZE as u32,
                kind,
                flags: 0,
            },
            data: [0; PAGE_DATA_SIZE],
        }
    }

    fn add_cell_pointer(&mut self, ptr: CellPointer) {
        let ptr_start = self.header.free_start as usize;
        let ptr_end = ptr_start + CELL_POINTER_SIZE as usize;
        let mut ptr_writer = &mut self.data[ptr_start..ptr_end];
        ptr.write_to_bytes(&mut ptr_writer).unwrap();
        self.header.free_start += CELL_POINTER_SIZE;
    }

    fn get_cell_pointer(&self, offset: u32) -> Result<CellPointer, SerdeError> {
        let reader_start = offset as usize;
        let reader_end = reader_start + CELL_POINTER_SIZE as usize;
        let mut reader = &self.data[reader_start..reader_end];
        CellPointer::from_bytes(&mut reader, &())
    }

    /// Returns Some(()) if the cell is successfully added
    /// or None if the data will not fit in this page
    fn add_cell(&mut self, data: &[u8]) -> Option<()> {
        let data_len: u32 = data.len().try_into().unwrap();
        let free_space = self.header.free_end - self.header.free_start;
        if (data_len + CELL_POINTER_SIZE) > free_space {
            return None;
        }
        let location = self.header.free_end - data_len;

        // add pointer
        let ptr = CellPointer {
            location,
            size: data_len,
        };
        self.add_cell_pointer(ptr);

        // add cell
        let cell_start = location as usize;
        let cell_end = cell_start + data.len();
        let mut cell_writer = &mut self.data[cell_start..cell_end];
        cell_writer.write_all(data).unwrap();
        self.header.free_end -= data_len;

        Some(())
    }

    fn get_cell(&self, ptr: &CellPointer) -> &[u8] {
        let cell_start = ptr.location as usize;
        let cell_end = cell_start + ptr.size as usize;
        &self.data[cell_start..cell_end]
    }

    fn is_cell_ptr_null(ptr: &CellPointer) -> bool {
        ptr.location == 0 && ptr.size == 0
    }

    fn mark_as_compactible(&mut self) {
        self.header.flags &= COMPACTIBLE_FLAG;
    }

    /// Removes all cells found matching the given predicate.
    fn remove_cell(&mut self, predicate: impl Fn(&[u8]) -> bool) {
        let mut ptr = PAGE_HEADER_SIZE as usize;
        while ptr < self.header.free_start as usize {
            let mut cell_ptr_bytes = &self.data[ptr..ptr + CELL_POINTER_SIZE as usize];
            let cell_ptr = CellPointer::from_bytes(&mut cell_ptr_bytes, &()).unwrap();
            if !Page::is_cell_ptr_null(&cell_ptr) && predicate(self.get_cell(&cell_ptr)) {
                let mut cell_ptr_bytes = &mut self.data[ptr..ptr + CELL_POINTER_SIZE as usize];
                cell_ptr_bytes
                    .write_all(&[0; CELL_POINTER_SIZE as usize])
                    .unwrap();
                self.mark_as_compactible();
            }
            ptr += CELL_POINTER_SIZE as usize;
        }
    }

    fn cell_iter(&self) -> PageCellIterator {
        PageCellIterator::new(&self)
    }
}

struct PageCellIterator<'a> {
    page: &'a Page,
    offset: u32,
}
impl<'a> PageCellIterator<'a> {
    fn new(page: &'a Page) -> Self {
        PageCellIterator { page, offset: 0 }
    }
}
impl<'a> Iterator for PageCellIterator<'a> {
    type Item = Result<&'a [u8], SerdeError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.page.header.free_start {
            return None;
        }
        let ptr = match self.page.get_cell_pointer(self.offset) {
            Ok(ptr) => ptr,
            Err(err) => return Some(Err(err)),
        };
        self.offset += CELL_POINTER_SIZE;
        if Page::is_cell_ptr_null(&ptr) {
            self.next()
        } else {
            Some(Ok(self.page.get_cell(&ptr)))
        }
    }
}

const CELL_POINTER_SIZE: u32 = 8;
#[derive(Debug, PartialEq)]
struct CellPointer {
    location: u32,
    size: u32,
}
impl CellPointer {
    fn new(location: u32, size: u32) -> Self {
        CellPointer { location, size }
    }
}
impl Serialize for CellPointer {
    fn write_to_bytes(&self, dest: &mut impl std::io::Write) -> Result<(), SerdeError> {
        self.location.write_to_bytes(dest)?;
        self.size.write_to_bytes(dest)?;
        Ok(())
    }
}
impl Deserialize for CellPointer {
    type ExtraInfo = ();
    fn from_bytes(
        from: &mut impl std::io::Read,
        _extra: &Self::ExtraInfo,
    ) -> Result<Self, SerdeError> {
        let location = u32::from_bytes(from, &())?;
        let size = u32::from_bytes(from, &())?;
        Ok(CellPointer::new(location, size))
    }
}

#[cfg(test)]
mod pager_tests {
    use std::rc::Rc;

    use crate::{generate::RNG, DbType, NumericCfg, Row, Schema};

    use super::*;

    fn gen_rows(seed: u64, count: usize) -> (Vec<Row>, Rc<Schema>) {
        let mut rng = RNG::from_seed(seed);
        let numeric_cfg = Rc::new(NumericCfg {
            max_precision: 10,
            max_scale: 5,
        });
        let char_size = 5;
        let schema = Rc::new(vec![
            DbType::Numeric(numeric_cfg),
            DbType::Integer,
            DbType::Varchar,
            DbType::Char(char_size),
            DbType::Double,
            DbType::Timestamp,
        ]);
        let mut rows = Vec::with_capacity(3);
        for _ in 0..count {
            let row = Row {
                data: schema
                    .iter()
                    .map(|t| t.as_generated_value(&mut rng))
                    .collect(),
                schema: schema.clone(),
            };
            rows.push(row);
        }
        (rows, schema)
    }

    #[test]
    fn cell_pointer_serde_with_large_buffer() {
        let mut buf = [0u8; 32];
        let cell_pointer = CellPointer {
            location: 42,
            size: 420,
        };
        let mut writer = &mut buf[..];
        cell_pointer.write_to_bytes(&mut writer).unwrap();

        let mut reader = &buf[..];
        let read_pointer = CellPointer::from_bytes(&mut reader, &()).unwrap();
        assert_eq!(cell_pointer, read_pointer);
    }

    #[test]
    fn read_and_write_multiple_rows() {
        let seed = rand::random();
        eprintln!("read_and_write_multiple_rows seed: {seed}");
        let (rows, schema) = gen_rows(seed, 3);

        let mut page = Page::new(PageKind::Data, 0);
        for row in rows.iter() {
            let mut bytes = Vec::new();
            row.write_to_bytes(&mut bytes).unwrap();
            page.add_cell(&bytes).unwrap();
        }

        let read_rows: Vec<Row> = page
            .cell_iter()
            .map(|bytes| {
                let mut reader = bytes.unwrap();
                Row::from_bytes(&mut reader, &schema).unwrap()
            })
            .collect();
        assert_eq!(rows, read_rows, "Failed with seed: {seed}");
    }
}
