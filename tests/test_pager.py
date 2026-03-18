"""Tests for the Pager class."""
from pager import Pager, PAGE_SIZE


class TestPagerBasics:
    def test_new_file_has_zero_pages(self, tmp_db):
        p = Pager(tmp_db)
        assert p.num_pages == 0
        p.close()

    def test_allocate_page(self, tmp_db):
        p = Pager(tmp_db)
        page_id = p.allocate_page()
        assert page_id == 0
        assert p.num_pages == 1
        p.close()

    def test_allocate_multiple_pages(self, tmp_db):
        p = Pager(tmp_db)
        id0 = p.allocate_page()
        id1 = p.allocate_page()
        assert id0 == 0
        assert id1 == 1
        assert p.num_pages == 2
        p.close()


class TestPagerReadWrite:
    def test_write_and_read_roundtrip(self, tmp_db):
        p = Pager(tmp_db)
        data = bytearray(PAGE_SIZE)
        data[0:5] = b"hello"
        p.write_page(0, data)
        read_back = p.get_page(0)
        assert read_back[0:5] == bytearray(b"hello")
        p.close()

    def test_read_unwritten_page_returns_blank(self, tmp_db):
        p = Pager(tmp_db)
        data = p.get_page(999)
        assert data == bytearray(PAGE_SIZE)
        p.close()

    def test_persistence_across_reopen(self, tmp_db):
        p = Pager(tmp_db)
        data = bytearray(PAGE_SIZE)
        data[10] = 42
        p.write_page(0, data)
        p.close()

        p2 = Pager(tmp_db)
        assert p2.num_pages == 1
        read_back = p2.get_page(0)
        assert read_back[10] == 42
        p2.close()
