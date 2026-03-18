"""Tests for the Page class."""
import struct
from page import Page, PageFullError, PAGE_SIZE, HEADER_SIZE, DELETED_MARKER


RECORD_FORMAT = "<I20sI"
RECORD_SIZE = struct.calcsize(RECORD_FORMAT)


def make_record(user_id, name, age):
    name_bytes = name.encode("utf-8")[:20].ljust(20, b'\x00')
    return struct.pack(RECORD_FORMAT, user_id, name_bytes, age)


class TestPageBasics:
    def test_new_page_is_empty(self):
        p = Page()
        assert p.num_records == 0
        assert p.free_ptr == HEADER_SIZE

    def test_page_size(self):
        p = Page()
        assert len(p.data) == PAGE_SIZE

    def test_add_record(self):
        p = Page()
        record = make_record(1, "Alice", 30)
        slot = p.add_record(record)
        assert slot == 0
        assert p.num_records == 1

    def test_add_multiple_records(self):
        p = Page()
        for i in range(3):
            slot = p.add_record(make_record(i + 1, f"User{i}", 20 + i))
            assert slot == i
        assert p.num_records == 3

    def test_get_record(self):
        p = Page()
        record = make_record(1, "Alice", 30)
        slot = p.add_record(record)
        retrieved = p.get_record(slot, RECORD_SIZE)
        assert retrieved == record

    def test_free_space_decreases(self):
        p = Page()
        initial_space = p.free_space()
        p.add_record(make_record(1, "Alice", 30))
        assert p.free_space() == initial_space - RECORD_SIZE


class TestPageFull:
    def test_page_full_raises(self):
        p = Page()
        max_records = (PAGE_SIZE - HEADER_SIZE) // RECORD_SIZE
        for i in range(max_records):
            p.add_record(make_record(i + 1, f"U{i}", i))
        # Next one should raise
        import pytest
        with pytest.raises(PageFullError):
            p.add_record(make_record(999, "Overflow", 0))


class TestTombstones:
    def test_delete_marks_tombstone(self):
        p = Page()
        p.add_record(make_record(1, "Alice", 30))
        result = p.delete_record(0, RECORD_SIZE)
        assert result is True
        assert p.is_deleted(0, RECORD_SIZE) is True

    def test_non_deleted_not_tombstoned(self):
        p = Page()
        p.add_record(make_record(1, "Alice", 30))
        assert p.is_deleted(0, RECORD_SIZE) is False

    def test_delete_preserves_other_records(self):
        p = Page()
        rec1 = make_record(1, "Alice", 30)
        rec2 = make_record(2, "Bob", 25)
        p.add_record(rec1)
        p.add_record(rec2)
        p.delete_record(0, RECORD_SIZE)
        assert p.is_deleted(0, RECORD_SIZE) is True
        assert p.is_deleted(1, RECORD_SIZE) is False
        assert p.get_record(1, RECORD_SIZE) == rec2

    def test_delete_invalid_slot(self):
        p = Page()
        result = p.delete_record(999, RECORD_SIZE)
        assert result is False


class TestPageRoundTrip:
    def test_serialize_deserialize(self):
        p1 = Page()
        rec = make_record(1, "Alice", 30)
        p1.add_record(rec)
        # Simulate reading from disk
        p2 = Page(p1.data)
        assert p2.num_records == 1
        assert p2.get_record(0, RECORD_SIZE) == rec
