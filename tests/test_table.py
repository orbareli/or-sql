"""Tests for the Table class (full storage layer)."""


class TestInsertSelect:
    def test_insert_returns_id(self, table):
        id1 = table.insert({"name": "Alice", "age": 30})
        assert id1 == 1

    def test_auto_increment(self, table):
        id1 = table.insert({"name": "Alice", "age": 30})
        id2 = table.insert({"name": "Bob", "age": 25})
        assert id2 == id1 + 1

    def test_select_all(self, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        rows = table.select_all()
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[1]["name"] == "Bob"

    def test_select_by_id(self, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        result = table.select_by_id(1)
        assert result is not None
        assert len(result) == 1
        assert result[0]["name"] == "Alice"

    def test_select_by_id_not_found(self, table):
        assert table.select_by_id(999) is None

    def test_insert_roundtrip(self, table):
        table.insert({"name": "Charlie", "age": 42})
        rows = table.select_all()
        assert len(rows) == 1
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "Charlie"
        assert rows[0]["age"] == 42


class TestDelete:
    def test_delete_by_id(self, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        result = table.delete(1)
        assert result is True
        assert table.select_by_id(1) is None
        rows = table.select_all()
        assert len(rows) == 1
        assert rows[0]["name"] == "Bob"

    def test_delete_nonexistent(self, table):
        result = table.delete(999)
        assert result is False

    def test_delete_many(self, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        table.insert({"name": "Charlie", "age": 35})
        count = table.delete_many([1, 3])
        assert count == 2
        rows = table.select_all()
        assert len(rows) == 1
        assert rows[0]["name"] == "Bob"

    def test_delete_many_with_missing(self, table):
        table.insert({"name": "Alice", "age": 30})
        count = table.delete_many([1, 999])
        assert count == 1


class TestUpdate:
    def test_update_name(self, table):
        table.insert({"name": "Alice", "age": 30})
        result = table.update(1, {"name": "Bob", "age": 30})
        assert result is True
        rows = table.select_by_id(1)
        assert rows[0]["name"] == "Bob"

    def test_update_age(self, table):
        table.insert({"name": "Alice", "age": 30})
        table.update(1, {"name": "Alice", "age": 99})
        rows = table.select_by_id(1)
        assert rows[0]["age"] == 99

    def test_update_nonexistent(self, table):
        result = table.update(999, {"name": "Ghost", "age": 0})
        assert result is False

    def test_update_preserves_other_records(self, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        table.update(1, {"name": "Charlie", "age": 40})
        rows = table.select_all()
        assert rows[0]["name"] == "Charlie"
        assert rows[1]["name"] == "Bob"


class TestFreelistReuse:
    def test_freelist_reuse(self, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        table.delete(1)
        assert len(table.freelist) == 1
        table.insert({"name": "Charlie", "age": 35})
        assert len(table.freelist) == 0
        rows = table.select_all()
        names = sorted([r["name"] for r in rows])
        assert names == ["Bob", "Charlie"]


class TestVacuum:
    def test_vacuum_compacts(self, table):
        for i in range(10):
            table.insert({"name": f"User{i}", "age": 20 + i})
        for i in range(1, 11, 2):
            table.delete(i)
        stats = table.vacuum()
        assert stats["live_records"] == 5
        assert stats["freed_slots"] > 0
        rows = table.select_all()
        assert len(rows) == 5

    def test_vacuum_empty_db(self, table):
        stats = table.vacuum()
        assert stats["live_records"] == 0

    def test_data_integrity_after_vacuum(self, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        table.insert({"name": "Charlie", "age": 35})
        table.delete(2)
        table.vacuum()
        rows = table.select_all()
        names = sorted([r["name"] for r in rows])
        assert names == ["Alice", "Charlie"]
        assert table.select_by_id(1) is not None
        assert table.select_by_id(2) is None
        assert table.select_by_id(3) is not None
