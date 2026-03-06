"""
Pager — manages the physical .db file.
Translates between page_id (logical) and byte offset (physical).

  page_id 0  →  bytes 0     to 4095
  page_id 1  →  bytes 4096  to 8191
  page_id 2  →  bytes 8192  to 12287
  ...

Pager knows nothing about records or trees — just raw pages.
"""
import os
import struct
from page import Page, PAGE_SIZE


class Pager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        if not os.path.exists(db_path):
            open(db_path, "wb").close()
        self.file      = open(db_path, "r+b")
        self.num_pages = os.path.getsize(db_path) // PAGE_SIZE

    def get_page(self, page_id: int) -> bytearray:
        """Read raw bytes of a page from disk."""
        if page_id >= self.num_pages:
            return bytearray(PAGE_SIZE)   # blank page (never written yet)
        self.file.seek(page_id * PAGE_SIZE)
        data = self.file.read(PAGE_SIZE)
        return bytearray(data.ljust(PAGE_SIZE, b'\x00'))

    def write_page(self, page_id: int, data: bytearray):
        """Write raw bytes of a page to disk."""
        assert len(data) == PAGE_SIZE
        self.file.seek(page_id * PAGE_SIZE)
        self.file.write(data)
        self.file.flush()
        if page_id >= self.num_pages:
            self.num_pages = page_id + 1

    def allocate_page(self) -> int:
        """Reserve a new blank page at end of file, return its page_id."""
        new_id = self.num_pages
        self.write_page(new_id, bytearray(PAGE_SIZE))
        return new_id

    def close(self):
        self.file.close()