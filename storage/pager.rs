use std::io::Write;

use crate::serialize::{Deserialize, SerdeError, Serialize};

const PAGE_SIZE: u32 = 1024;
const PAGE_HEADER_SIZE: u32 = 0;
const PAGE_DATA_SIZE: usize = (PAGE_SIZE - PAGE_HEADER_SIZE) as usize;

// flags
const COMPACTIBLE_FLAG: u8 = 0b1;

enum PageKind {
    Data,
}

struct PageHeader {
    id: u32,
    free_start: u32,
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
                free_start: PAGE_HEADER_SIZE,
                free_end: PAGE_SIZE,
                kind,
                flags: 0,
            },
            data: [0; PAGE_DATA_SIZE],
        }
    }

    /// Returns Some(()) if the cell is successfully added
    /// or None if the data will not fit in this page
    fn add_cell(&mut self, data: &[u8]) -> Option<()> {
        let data_len: u32 = data.len().try_into().unwrap();
        let free_space = self.header.free_end - self.header.free_start;
        if data_len > free_space {
            return None;
        }
        let location = self.header.free_end - data_len;

        // add pointer
        let ptr = CellPointer {
            location,
            size: data_len,
        };
        let ptr_start = self.header.free_start as usize;
        let ptr_end = ptr_start + CELL_POINTER_SIZE as usize;
        let mut ptr_writer = &mut self.data[ptr_start..ptr_end];
        ptr.write_to_bytes(&mut ptr_writer).unwrap();
        self.header.free_start += CELL_POINTER_SIZE;

        // add cell
        let cell_start = location as usize;
        let cell_end = cell_start + data.len();
        let mut cell_writer = &mut self.data[cell_start..cell_end];
        cell_writer.write_all(data).unwrap();

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
}

const CELL_POINTER_SIZE: u32 = 8;
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
        let mut buf = [0; 4];
        from.read_exact(&mut buf)?;
        let location = u32::from_bytes(from, &())?;
        from.read_exact(&mut buf)?;
        let size = u32::from_bytes(from, &())?;
        Ok(CellPointer::new(location, size))
    }
}
