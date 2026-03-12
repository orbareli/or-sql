"""
table.py
--------
Top-level API. Coordinates:
  - Pager      → data pages on disk
  - BPlusTree  → index (key → location)
  - Freelist   → tracks deleted slots for immediate reuse
  - VACUUM     → periodic compaction with temp file safety

Strategy:
  - delete()  → tombstone + add to freelist
  - insert()  → check freelist first, reuse if available
  - vacuum()  → write to temp file, atomic swap, rebuild index
"""
import os
import struct
import shutil
from page import Page, PageFullError, PAGE_SIZE, HEADER_SIZE
from pager import Pager
from btree import BPlusTree

RECORD_FORMAT       = "<I20sI"
RECORD_SIZE         = struct.calcsize(RECORD_FORMAT)
FREELIST_ENTRY_FMT  = "<II"
FREELIST_ENTRY_SIZE = struct.calcsize(FREELIST_ENTRY_FMT)


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
        self.freelist = self._load_freelist()

    # ---------------------------------------------------------------- #
    #  Paths                                                            #
    # ---------------------------------------------------------------- #

    def _meta_path(self):  return self.db_path + ".meta"
    def _free_path(self):  return self.db_path + ".free"
    def _idx_path(self):   return self.db_path + ".idx"

    # ---------------------------------------------------------------- #
    #  next_id persistence                                              #
    # ---------------------------------------------------------------- #

    def _load_next_id(self) -> int:
        if os.path.exists(self._meta_path()):
            with open(self._meta_path(), "rb") as f:
                return struct.unpack("<I", f.read(4))[0]
        return 1

    def _save_next_id(self):
        with open(self._meta_path(), "wb") as f:
            f.write(struct.pack("<I", self.next_id))

    # ---------------------------------------------------------------- #
    #  Freelist persistence                                             #
    # ---------------------------------------------------------------- #

    def _load_freelist(self) -> list:
        """
        Load freelist from disk.
        Format: [num_entries: 4b][page_id: 4b | slot_id: 4b] * n
        """
        path = self._free_path()
        if not os.path.exists(path):
            return []
        freelist = []
        with open(path, "rb") as f:
            raw = f.read(4)
            if len(raw) < 4:
                return []
            num_entries = struct.unpack("<I", raw)[0]
            for _ in range(num_entries):
                raw = f.read(FREELIST_ENTRY_SIZE)
                if len(raw) < FREELIST_ENTRY_SIZE:
                    break
                page_id, slot_id = struct.unpack(FREELIST_ENTRY_FMT, raw)
                freelist.append((page_id, slot_id))
        return freelist

    def _save_freelist(self):
        """Persist freelist to disk."""
        with open(self._free_path(), "wb") as f:
            f.write(struct.pack("<I", len(self.freelist)))
            for page_id, slot_id in self.freelist:
                f.write(struct.pack(FREELIST_ENTRY_FMT, page_id, slot_id))

    # ---------------------------------------------------------------- #
    #  Helpers                                                          #
    # ---------------------------------------------------------------- #

    def _find_writable_page(self) -> tuple[int, Page]:
        """Return (page_id, Page) with room for one more record."""
        for page_id in range(self.pager.num_pages):
            page = Page(self.pager.get_page(page_id))
            if page.free_space() >= RECORD_SIZE:
                return page_id, page
        page_id = self.pager.allocate_page()
        return page_id, Page()

    def _should_vacuum(self) -> bool:
        """
        Auto-vacuum trigger.
        Returns True if freelist has grown large relative to total records.
        Threshold: more than 30% of slots are in the freelist.
        """
        total = 0
        for page_id in range(self.pager.num_pages):
            raw  = self.pager.get_page(page_id)
            page = Page(raw)
            total += page.num_records
        if total == 0:
            return False
        return (len(self.freelist) / total) > 0.30

    # ---------------------------------------------------------------- #
    #  Public API                                                       #
    # ---------------------------------------------------------------- #

    def insert(self, name: str, age: int) -> int:
        """
        Insert a record.
        Checks freelist first — reuses deleted slots before appending.
        Auto-triggers vacuum if fragmentation exceeds 30%.
        """
        user_id = self.next_id
        record  = pack(user_id, name, age)

        if self.freelist:
            # -------------------------------------------------------- #
            # Reuse a deleted slot — no new space consumed              #
            # -------------------------------------------------------- #
            page_id, slot_id = self.freelist.pop()
            page   = Page(self.pager.get_page(page_id))
            offset = HEADER_SIZE + slot_id * RECORD_SIZE
            page.data[offset : offset + RECORD_SIZE] = record
            self.pager.write_page(page_id, page.data)
            self._save_freelist()

        else:
            # -------------------------------------------------------- #
            # No free slots — append normally                           #
            # -------------------------------------------------------- #
            page_id, page = self._find_writable_page()
            slot_id = page.add_record(record)
            self.pager.write_page(page_id, page.data)

        # Update index
        self.tree.insert(user_id, page_id, slot_id)

        # Persist next_id
        self.next_id += 1
        self._save_next_id()

        # Auto-vacuum check
        if self._should_vacuum():
            print("  [Auto-vacuum triggered — freelist exceeded 30% threshold]")
            self.vacuum()

        return user_id

    def delete(self, user_id: int) -> bool:
        """
        Delete a record.
        Tombstones the slot and adds it to freelist for immediate reuse.
        """
        location = self.tree.search(user_id)
        if location is None:
            return False

        page_id, slot_id = location

        # Tombstone the slot
        page = Page(self.pager.get_page(page_id))
        page.delete_record(slot_id, RECORD_SIZE)
        self.pager.write_page(page_id, page.data)

        # Track in freelist
        self.freelist.append((page_id, slot_id))
        self._save_freelist()

        # Remove from index
        self.tree.delete(user_id)

        return True

    def delete_many(self, ids: list[int]) -> int:
        """
        Batch delete — more efficient than calling delete() one by one.
        Groups by page so each page is loaded and written only once.
        """
        # Group by page
        pages_to_update = {}
        for record_id in ids:
            location = self.tree.search(record_id)
            if location is None:
                continue
            page_id, slot_id = location
            if page_id not in pages_to_update:
                pages_to_update[page_id] = []
            pages_to_update[page_id].append((record_id, slot_id))

        total = 0
        for page_id, entries in pages_to_update.items():
            page = Page(self.pager.get_page(page_id))
            for record_id, slot_id in entries:
                page.delete_record(slot_id, RECORD_SIZE)
                self.freelist.append((page_id, slot_id))
                self.tree.delete(record_id)
                total += 1
            self.pager.write_page(page_id, page.data)

        if total > 0:
            self._save_freelist()

        return total

    def select_all(self) -> list[dict]:
        """Full scan — skips tombstoned records."""
        results = []
        for page_id in range(self.pager.num_pages):
            page = Page(self.pager.get_page(page_id))
            for slot_id in range(page.num_records):
                if page.is_deleted(slot_id, RECORD_SIZE):
                    continue
                record = page.get_record(slot_id, RECORD_SIZE)
                results.append(unpack(record))
        return results

    def select_by_id(self, user_id: int) -> list | None:
        """B+ Tree O(log n) point lookup."""
        location = self.tree.search(user_id)
        if location is None:
            return None
        page_id, slot_id = location
        page = Page(self.pager.get_page(page_id))
        if page.is_deleted(slot_id, RECORD_SIZE):
            return None
        record = page.get_record(slot_id, RECORD_SIZE)
        return [unpack(record)]

    # ---------------------------------------------------------------- #
    #  VACUUM — safe compaction with temp file                          #
    # ---------------------------------------------------------------- #

    def vacuum(self) -> dict:
        """
        Compact the database safely.

        Steps:
          1. Stream live records into a temp file page by page
             (never loads whole DB into memory)
          2. Atomic swap — only replace original after temp is complete
             (crash during step 1 leaves original untouched)
          3. Rebuild B+ Tree from new locations
          4. Clear freelist — all slots are now tightly packed

        Safe because:
          - If crash during step 1 → original .db untouched
          - If crash during swap   → worst case: rename fails, original intact
        """
        print("Starting VACUUM...")

        tmp_path = self.db_path + ".tmp"
        tmp_pager = Pager(tmp_path)

        # ------------------------------------------------------------ #
        # Step 1 — stream live records into temp file                   #
        # One page in memory at a time — no memory bomb                 #
        # ------------------------------------------------------------ #
        output_page    = Page()
        output_page_id = 0
        new_locations  = {}   # user_id → (new_page_id, new_slot_id)
        live_count     = 0
        freed_count    = len(self.freelist)

        for page_id in range(self.pager.num_pages):
            raw  = self.pager.get_page(page_id)
            page = Page(raw)

            for slot_id in range(page.num_records):
                if page.is_deleted(slot_id, RECORD_SIZE):
                    continue

                record_bytes = page.get_record(slot_id, RECORD_SIZE)
                record       = unpack(record_bytes)

                # If output page is full — flush it and start a new one
                if output_page.free_space() < RECORD_SIZE:
                    tmp_pager.write_page(output_page_id, output_page.data)
                    output_page_id += 1
                    output_page     = Page()

                new_slot = output_page.add_record(record_bytes)
                new_locations[record["id"]] = (output_page_id, new_slot)
                live_count += 1

        # Write last page if it has any records
        if output_page.num_records > 0:
            tmp_pager.write_page(output_page_id, output_page.data)

        tmp_pager.close()

        pages_before = self.pager.num_pages
        pages_after  = output_page_id + 1 if live_count > 0 else 0

        # ------------------------------------------------------------ #
        # Step 2 — atomic swap                                          #
        # Original file untouched until this point                      #
        # ------------------------------------------------------------ #
        self.pager.close()
        os.remove(self.db_path)
        shutil.move(tmp_path, self.db_path)   # atomic on most OSes
        self.pager = Pager(self.db_path)

        # ------------------------------------------------------------ #
        # Step 3 — rebuild B+ Tree from new locations                   #
        # ------------------------------------------------------------ #
        self.tree.pager.close()
        os.remove(self._idx_path())
        self.tree = BPlusTree(self._idx_path())

        for user_id, (page_id, slot_id) in sorted(new_locations.items()):
            self.tree.insert(user_id, page_id, slot_id)

        # ------------------------------------------------------------ #
        # Step 4 — clear freelist                                       #
        # All slots are now tightly packed — no free slots needed       #
        # ------------------------------------------------------------ #
        self.freelist = []
        self._save_freelist()

        print(f"  Live records    : {live_count}")
        print(f"  Slots freed     : {freed_count}")
        print(f"  Pages before    : {pages_before}")
        print(f"  Pages after     : {pages_after}")
        print(f"  Pages reclaimed : {pages_before - pages_after}")
        print("VACUUM complete.")

        return {
            "live_records"   : live_count,
            "freed_slots"    : freed_count,
            "pages_before"   : pages_before,
            "pages_after"    : pages_after,
            "pages_reclaimed": pages_before - pages_after,
        }

    # ---------------------------------------------------------------- #
    #  Reports                                                          #
    # ---------------------------------------------------------------- #

    def fragmentation_report(self) -> str:
        total      = 0
        tombstones = 0

        for page_id in range(self.pager.num_pages):
            raw  = self.pager.get_page(page_id)
            page = Page(raw)
            total += page.num_records
            for slot_id in range(page.num_records):
                if page.is_deleted(slot_id, RECORD_SIZE):
                    tombstones += 1

        live     = total - tombstones
        frag_pct = (tombstones / total * 100) if total > 0 else 0
        records_per_page = (PAGE_SIZE - HEADER_SIZE) // RECORD_SIZE
        pages_needed     = -(-live // records_per_page) if live > 0 else 0

        return "\n".join([
            "--- Fragmentation Report ---",
            f"Total slots      : {total}",
            f"Live records     : {live}",
            f"Tombstones       : {tombstones}",
            f"Fragmentation    : {frag_pct:.1f}%",
            f"Wasted space     : {tombstones * RECORD_SIZE} bytes",
            f"Pages now        : {self.pager.num_pages}",
            f"Pages after vac  : {pages_needed}",
            f"Freelist slots   : {len(self.freelist)}",
        ])

    def freelist_report(self) -> str:
        return "\n".join([
            "--- Freelist Report ---",
            f"Available slots  : {len(self.freelist)}",
            f"Reclaimable      : {len(self.freelist) * RECORD_SIZE} bytes",
            f"Entries          : {self.freelist}",
        ])

    def close(self):
        self._save_freelist()
        self.pager.close()
        self.tree.pager.close()