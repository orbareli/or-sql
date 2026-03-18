"""Integration tests for multi-table support (CREATE TABLE, DROP TABLE, SHOW TABLES)."""
import os
import pytest
from catalog import Catalog
from executor import Executor


@pytest.fixture
def empty_catalog(tmp_path):
    """Return a Catalog with no tables pre-registered."""
    return Catalog(str(tmp_path))


@pytest.fixture
def ex(empty_catalog):
    """Return an Executor backed by an empty catalog."""
    e = Executor(empty_catalog)
    yield e
    e.close()


class TestCreateAndSelect:
    def test_create_and_insert_select(self, ex):
        result = ex.run("CREATE TABLE products (name TEXT(30), price INTEGER)")
        assert "created" in result.lower()

        result = ex.run('INSERT INTO products (name, price) VALUES ("Widget", 99)')
        assert "Inserted" in result
        assert "id=1" in result

        result = ex.run("SELECT * FROM products")
        assert len(result) == 1
        assert result[0]["name"] == "Widget"
        assert result[0]["price"] == 99
        assert result[0]["id"] == 1

    def test_create_and_multiple_inserts(self, ex):
        ex.run("CREATE TABLE items (label TEXT(20), count INTEGER)")
        ex.run('INSERT INTO items (label, count) VALUES ("Apples", 10)')
        ex.run('INSERT INTO items (label, count) VALUES ("Bananas", 5)')
        ex.run('INSERT INTO items (label, count) VALUES ("Cherries", 20)')

        result = ex.run("SELECT * FROM items ORDER BY count DESC")
        assert len(result) == 3
        assert result[0]["label"] == "Cherries"
        assert result[2]["label"] == "Bananas"


class TestTwoIndependentTables:
    def test_data_isolation(self, ex):
        ex.run("CREATE TABLE alpha (val INTEGER)")
        ex.run("CREATE TABLE beta (val INTEGER)")

        ex.run("INSERT INTO alpha (val) VALUES (100)")
        ex.run("INSERT INTO alpha (val) VALUES (200)")
        ex.run("INSERT INTO beta (val) VALUES (999)")

        alpha_rows = ex.run("SELECT * FROM alpha")
        beta_rows = ex.run("SELECT * FROM beta")

        assert len(alpha_rows) == 2
        assert len(beta_rows) == 1
        assert alpha_rows[0]["val"] == 100
        assert beta_rows[0]["val"] == 999

    def test_delete_in_one_table_no_effect_on_other(self, ex):
        ex.run("CREATE TABLE t1 (x INTEGER)")
        ex.run("CREATE TABLE t2 (x INTEGER)")

        ex.run("INSERT INTO t1 (x) VALUES (1)")
        ex.run("INSERT INTO t2 (x) VALUES (2)")
        ex.run("DELETE FROM t1 WHERE id = 1")

        assert ex.run("SELECT * FROM t1") == []
        assert len(ex.run("SELECT * FROM t2")) == 1


class TestShowTables:
    def test_show_no_tables(self, ex):
        result = ex.run("SHOW TABLES")
        assert result == "No tables."

    def test_show_tables_lists_all(self, ex):
        ex.run("CREATE TABLE zebra (z INTEGER)")
        ex.run("CREATE TABLE apple (a INTEGER)")
        result = ex.run("SHOW TABLES")
        names = [r["table_name"] for r in result]
        assert names == ["apple", "zebra"]


class TestCreateDuplicateError:
    def test_duplicate_table(self, ex):
        ex.run("CREATE TABLE items (val INTEGER)")
        result = ex.run("CREATE TABLE items (val INTEGER)")
        assert "Error" in result
        assert "already exists" in result


class TestDropTable:
    def test_drop_removes_from_show(self, ex):
        ex.run("CREATE TABLE temp (val INTEGER)")
        ex.run("INSERT INTO temp (val) VALUES (42)")
        result = ex.run("DROP TABLE temp")
        assert "dropped" in result.lower()

        show = ex.run("SHOW TABLES")
        assert show == "No tables."

    def test_drop_nonexistent(self, ex):
        result = ex.run("DROP TABLE nope")
        assert "Error" in result
        assert "does not exist" in result


class TestInsertNonexistentTable:
    def test_insert_into_missing_table(self, ex):
        result = ex.run('INSERT INTO ghost (name) VALUES ("boo")')
        assert "Error" in result
        assert "does not exist" in result


class TestAllColumnTypes:
    def test_integer_text_float_boolean_roundtrip(self, ex):
        ex.run("CREATE TABLE mixed (label TEXT(10), count INTEGER, score FLOAT, active BOOLEAN)")
        # Note: BOOLEAN and FLOAT values are parsed as integers by our parser
        # so we test with integer values that represent them
        ex.run('INSERT INTO mixed (label, count, score, active) VALUES ("test", 7, 3, 1)')

        result = ex.run("SELECT * FROM mixed")
        assert len(result) == 1
        row = result[0]
        assert row["label"] == "test"
        assert row["count"] == 7
        assert row["score"] == 3.0
        assert row["active"] is True


class TestDropCleansFiles:
    def test_files_removed_on_drop(self, ex, empty_catalog):
        ex.run("CREATE TABLE cleanup (val INTEGER)")
        ex.run("INSERT INTO cleanup (val) VALUES (42)")

        base = empty_catalog.db_path("cleanup")
        assert os.path.exists(base)

        ex.run("DROP TABLE cleanup")

        for ext in ("", ".idx", ".meta", ".free"):
            assert not os.path.exists(base + ext), f"File {base + ext} should be deleted"


class TestUpdateOnCustomTable:
    def test_update_custom_table(self, ex):
        ex.run("CREATE TABLE scores (name TEXT(20), points INTEGER)")
        ex.run('INSERT INTO scores (name, points) VALUES ("Alice", 100)')
        ex.run('INSERT INTO scores (name, points) VALUES ("Bob", 200)')

        result = ex.run('UPDATE scores SET points = 150 WHERE name = "Alice"')
        assert "Updated 1 record(s)" in result

        rows = ex.run("SELECT * FROM scores WHERE id = 1")
        assert rows[0]["points"] == 150


class TestDeleteOnCustomTable:
    def test_delete_with_where(self, ex):
        ex.run("CREATE TABLE logs (level INTEGER, msg TEXT(30))")
        ex.run('INSERT INTO logs (level, msg) VALUES (1, "info")')
        ex.run('INSERT INTO logs (level, msg) VALUES (3, "error")')
        ex.run('INSERT INTO logs (level, msg) VALUES (2, "warn")')

        result = ex.run("DELETE FROM logs WHERE level >= 3")
        assert "Deleted 1 record(s)" in result

        rows = ex.run("SELECT * FROM logs")
        assert len(rows) == 2


class TestAggregatesOnCustomTable:
    def test_count_and_sum(self, ex):
        ex.run("CREATE TABLE sales (product TEXT(20), amount INTEGER)")
        ex.run('INSERT INTO sales (product, amount) VALUES ("Widget", 100)')
        ex.run('INSERT INTO sales (product, amount) VALUES ("Gadget", 200)')
        ex.run('INSERT INTO sales (product, amount) VALUES ("Widget", 50)')

        result = ex.run("SELECT COUNT(*) FROM sales")
        assert result[0]["COUNT(*)"] == 3

        result = ex.run("SELECT SUM(amount) FROM sales")
        assert result[0]["SUM(amount)"] == 350
