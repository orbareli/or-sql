"""Tests for the B+ Tree index."""
import os
from btree import BPlusTree, ORDER


class TestBTreeBasics:
    def test_insert_and_search(self, tmp_path):
        idx = str(tmp_path / "test.idx")
        tree = BPlusTree(idx)
        tree.insert(1, 0, 0)
        assert tree.search(1) == (0, 0)
        tree.pager.close()

    def test_search_missing_key(self, tmp_path):
        idx = str(tmp_path / "test.idx")
        tree = BPlusTree(idx)
        assert tree.search(999) is None
        tree.pager.close()

    def test_multiple_inserts(self, tmp_path):
        idx = str(tmp_path / "test.idx")
        tree = BPlusTree(idx)
        for i in range(1, 10):
            tree.insert(i, i, i * 10)
        for i in range(1, 10):
            assert tree.search(i) == (i, i * 10)
        tree.pager.close()


class TestBTreeSplits:
    def test_leaf_split(self, tmp_path):
        """Insert more than ORDER keys to force a leaf split."""
        idx = str(tmp_path / "test.idx")
        tree = BPlusTree(idx)
        # ORDER is 4, so inserting 5+ keys triggers a split
        for i in range(1, ORDER + 3):
            tree.insert(i, 0, i)
        # All keys should still be findable
        for i in range(1, ORDER + 3):
            assert tree.search(i) == (0, i)
        tree.pager.close()

    def test_many_inserts(self, tmp_path):
        """Insert enough keys to trigger multiple levels of splits."""
        idx = str(tmp_path / "test.idx")
        tree = BPlusTree(idx)
        n = 50
        for i in range(1, n + 1):
            tree.insert(i, i % 5, i)
        for i in range(1, n + 1):
            result = tree.search(i)
            assert result is not None
            assert result == (i % 5, i)
        tree.pager.close()

    def test_reverse_order_inserts(self, tmp_path):
        """Insert keys in reverse order to stress split logic."""
        idx = str(tmp_path / "test.idx")
        tree = BPlusTree(idx)
        for i in range(20, 0, -1):
            tree.insert(i, 0, i)
        for i in range(1, 21):
            assert tree.search(i) == (0, i)
        tree.pager.close()


class TestBTreeDelete:
    def test_delete_existing(self, tmp_path):
        idx = str(tmp_path / "test.idx")
        tree = BPlusTree(idx)
        tree.insert(1, 0, 0)
        tree.insert(2, 0, 1)
        assert tree.delete(1) is True
        assert tree.search(1) is None
        assert tree.search(2) == (0, 1)
        tree.pager.close()

    def test_delete_nonexistent(self, tmp_path):
        idx = str(tmp_path / "test.idx")
        tree = BPlusTree(idx)
        tree.insert(1, 0, 0)
        assert tree.delete(999) is False
        tree.pager.close()

    def test_delete_all_keys(self, tmp_path):
        idx = str(tmp_path / "test.idx")
        tree = BPlusTree(idx)
        for i in range(1, 6):
            tree.insert(i, 0, i)
        for i in range(1, 6):
            tree.delete(i)
        for i in range(1, 6):
            assert tree.search(i) is None
        tree.pager.close()
