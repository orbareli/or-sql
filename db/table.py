"""
Table — the top-level API. This is what your application talks to.

Coordinates:
  - Pager:    to store actual records (data pages)
  - BPlusTree: to store the index (index pages, separate file)

Flow for INSERT:
  1. Pack record into bytes
  2. Write bytes into a data page via Pager  → get back (page_id, slot_id)
  3. Insert (user_id, page_id, slot_id) into BPlusTree

Flow for SELECT by id:
  1. Ask BPlusTree for key=user_id  → get back (page_id, slot_id)
  2. Ask Pager for page_id          → get back raw page bytes
  3. Read slot_id from page         → get back record bytes
  4. Unpack and return
"""
import os
import struct
from page import Page, PageFullError, PAGE_SIZE, HEADER_SIZE
from pager import Pager
from btree import BPlusTree

RECORD_FORMAT = "<I20sI"   # user_id(4) | name(20) | age(4) = 28 bytes
RECORD_SIZE   = struct.calcsize(RECORD_FORMAT)


def pack(user_id, name, age) -> bytes:
    name_bytes = name.encode("utf-8")[:20].ljust(20, b'\x00')
    return struct.pack(RECORD_FORMAT, user_id, name_bytes, age)


def unpack(data: bytes) -> dict:
    user_id, name_bytes, age = struct.unpack(RECORD_FORMAT, data)
    return {"id": user_id, "name": name_bytes.decode("utf-8").strip('\x00'), "age": age}


class Table:
    def __init__(self, db_path: str):
        self.db_path  = db_path
        self.pager    = Pager(db_path)
        self.tree     = BPlusTree(db_path + ".idx")
        self.next_id  = self._load_next_id()

    # ---------------------------------------------------------------- #
    #  Metadata                                                         #
    # ---------------------------------------------------------------- #

    def _meta_path(self):
        return self.db_path + ".meta"

    def _load_next_id(self) -> int:
        if os.path.exists(self._meta_path()):
            with open(self._meta_path(), "rb") as f:
                return struct.unpack("<I", f.read(4))[0]
        return 1

    def _save_next_id(self):
        with open(self._meta_path(), "wb") as f:
            f.write(struct.pack("<I", self.next_id))

    # ---------------------------------------------------------------- #
    #  Helpers                                                          #
    # ---------------------------------------------------------------- #

    def _find_writable_page(self) -> tuple[int, Page]:
        """Return (page_id, Page) that has room for one more record."""
        for page_id in range(self.pager.num_pages):
            page = Page(self.pager.get_page(page_id))
            if page.free_space() >= RECORD_SIZE:
                return page_id, page
        # All pages full or no pages exist — allocate new one
        page_id = self.pager.allocate_page()
        return page_id, Page()

    # ---------------------------------------------------------------- #
    #  Public API                                                       #
    # ---------------------------------------------------------------- #

    def insert(self, name: str, age: int) -> int:
        """Insert a record. Returns the auto-assigned user_id."""
        user_id = self.next_id

        # 1. Pack record into bytes
        record = pack(user_id, name, age)

        # 2. Write into a data page
        page_id, page = self._find_writable_page()
        slot_id = page.add_record(record)
        self.pager.write_page(page_id, page.data)

        # 3. Insert into index
        self.tree.insert(user_id, page_id, slot_id)

        # 4. Persist next_id
        self.next_id += 1
        self._save_next_id()

        return user_id

    def delete(self, user_id: int) -> bool:
        location = self.tree.search(user_id)
        if location is None:
            return False
        page_id, slot_id = location
        raw = self.pager.get_page(page_id)
        print(f"DEBUG raw page size: {len(raw)}")   # ← should be 4096
        page = Page(raw)
        print(f"DEBUG page.data size: {len(page.data)}")  # ← should be 4096
        page.delete_record(slot_id, RECORD_SIZE)
        print(f"DEBUG after delete size: {len(page.data)}")  # ← still 4096?
        self.pager.write_page(page_id, page.data)
        self.tree.delete(user_id)
        return True

    def select_all(self) -> list[dict]:
        """Full scan — now skips tombstoned records."""
        results = []
        for page_id in range(self.pager.num_pages):
            page = Page(self.pager.get_page(page_id))
            for slot_id in range(page.num_records):
                # Skip deleted records
                if page.is_deleted(slot_id, RECORD_SIZE):
                    continue
                record = page.get_record(slot_id, RECORD_SIZE)
                results.append(unpack(record))
        return results

    def select_by_id(self, user_id: int) -> dict | None:
        """B+ Tree lookup — also check tombstone just to be safe."""
        location = self.tree.search(user_id)
        if location is None:
            return None

        page_id, slot_id = location
        page = Page(self.pager.get_page(page_id))

        if page.is_deleted(slot_id, RECORD_SIZE):
            return None  # was deleted, B+ Tree hasn't caught up yet

        record = page.get_record(slot_id, RECORD_SIZE)
        return unpack(record)

    def close(self):
        self.pager.close()
        self.tree.pager.close()


# ------------------------------------------------------------------ #
#  Usage                                                               #
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    DB = "my_database2.db"

    # Clean slate for testing
    for ext in ["", ".idx", ".meta"]:
        if os.path.exists(DB + ext): os.remove(DB + ext)

    db = Table(DB)

    id1 = db.insert("Alice", 30)
    id2 = db.insert("Bob", 25)
    id3 = db.insert("Charlie", 35)
    id4 = db.insert("Diana", 28)
    id5 = db.insert("Eve", 22)

    print("--- select by id ---")
    print(db.select_by_id(1))   # Alice
    print(db.select_by_id(3))   # Charlie
    print(db.select_by_id(99))  # None

    print("\n--- select all ---")
    for row in db.select_all():
        print(row)

    db.close()

    # Reopen — next_id should continue from 6, index should survive
    print("\n--- after restart ---")
    db2 = Table(DB)
    id6 = db2.insert("Frank", 40)
    print(f"Frank got id={id6}")           # should be 6
    print(db2.select_by_id(id6))           # Frank
    db2.close()
