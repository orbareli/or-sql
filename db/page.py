"""
A Page is a fixed 4096-byte block — the unit of I/O.
The DB never reads individual records from disk, always full pages.

Layout inside a data page:
  [num_records: 2 bytes | free_ptr: 2 bytes | record0 | record1 | ...]
"""
import struct

PAGE_SIZE     = 4096
HEADER_FORMAT = "!HH"
HEADER_SIZE   = struct.calcsize(HEADER_FORMAT)  # 4 bytes
DELETED_MARKER = bytes([0xFF, 0xFF, 0xFF, 0xFF])

class Page:
    def __init__(self, data: bytearray = None):
        if data:
            self.data = bytearray(data)
            self.num_records, self.free_ptr = struct.unpack(
                HEADER_FORMAT, self.data[:HEADER_SIZE]
            )
        else:
            self.data        = bytearray(PAGE_SIZE)
            self.num_records = 0
            self.free_ptr    = HEADER_SIZE
            self._flush_header()
    def delete_record(self, slot_id: int, record_size: int) -> bool:
        """
        Mark a record as deleted by writing a tombstone over its first 4 bytes.
        We don't erase the whole record — just mark it so select_all skips it.
        """
        offset = HEADER_SIZE + slot_id * record_size
        if offset + record_size > self.free_ptr:
            return False  # slot doesn't exist
        self.data[offset : offset + 4] = DELETED_MARKER
        return True

    def is_deleted(self, slot_id: int, record_size: int) -> bool:
        """Check if a record has been tombstoned."""
        offset = HEADER_SIZE + slot_id * record_size
        return bytes(self.data[offset : offset + 4]) == DELETED_MARKER

    def _flush_header(self):
        struct.pack_into(HEADER_FORMAT, self.data, 0, self.num_records, self.free_ptr)

    def free_space(self) -> int:
        return PAGE_SIZE - self.free_ptr

    def add_record(self, record_bytes: bytes) -> int:
        """Returns slot_id of the inserted record."""
        if self.free_space() < len(record_bytes):
            raise PageFullError("Page is full")
        slot_id = self.num_records
        self.data[self.free_ptr : self.free_ptr + len(record_bytes)] = record_bytes
        self.free_ptr    += len(record_bytes)
        self.num_records += 1
        self._flush_header()
        return slot_id

    def get_record(self, slot_id: int, record_size: int) -> bytes:
        offset = HEADER_SIZE + slot_id * record_size
        return bytes(self.data[offset : offset + record_size])


class PageFullError(Exception):
    pass