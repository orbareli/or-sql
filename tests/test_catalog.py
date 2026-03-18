"""Tests for the Catalog system."""
import os
import pytest
from catalog import Catalog, CatalogError
from schema import Schema, Column, ColumnType


def make_schema(name="test_table"):
    return Schema(name, [
        Column("label", ColumnType.TEXT, 20),
        Column("value", ColumnType.INTEGER),
    ])


class TestCatalogBasic:
    def test_create_table(self, tmp_path):
        cat = Catalog(str(tmp_path))
        result = cat.create_table(make_schema("items"))
        assert result == "items"
        assert cat.table_exists("items")

    def test_list_tables(self, tmp_path):
        cat = Catalog(str(tmp_path))
        cat.create_table(make_schema("b_table"))
        cat.create_table(make_schema("a_table"))
        assert cat.list_tables() == ["a_table", "b_table"]

    def test_get_schema(self, tmp_path):
        cat = Catalog(str(tmp_path))
        cat.create_table(make_schema("items"))
        schema = cat.get_schema("items")
        assert schema.table_name == "items"
        assert len(schema.columns) == 2

    def test_db_path(self, tmp_path):
        cat = Catalog(str(tmp_path))
        path = cat.db_path("users")
        assert path.endswith("users.db")

    def test_table_not_exists(self, tmp_path):
        cat = Catalog(str(tmp_path))
        assert cat.table_exists("nope") is False


class TestCatalogErrors:
    def test_create_duplicate(self, tmp_path):
        cat = Catalog(str(tmp_path))
        cat.create_table(make_schema("items"))
        with pytest.raises(CatalogError, match="already exists"):
            cat.create_table(make_schema("items"))

    def test_get_nonexistent(self, tmp_path):
        cat = Catalog(str(tmp_path))
        with pytest.raises(CatalogError, match="does not exist"):
            cat.get_schema("nope")

    def test_drop_nonexistent(self, tmp_path):
        cat = Catalog(str(tmp_path))
        with pytest.raises(CatalogError, match="does not exist"):
            cat.drop_table("nope")


class TestCatalogPersistence:
    def test_reload(self, tmp_path):
        cat1 = Catalog(str(tmp_path))
        cat1.create_table(make_schema("items"))

        cat2 = Catalog(str(tmp_path))
        assert cat2.table_exists("items")
        schema = cat2.get_schema("items")
        assert schema.table_name == "items"
        assert len(schema.columns) == 2


class TestCatalogDrop:
    def test_drop_table(self, tmp_path):
        cat = Catalog(str(tmp_path))
        cat.create_table(make_schema("items"))
        cat.drop_table("items")
        assert cat.table_exists("items") is False
        assert "items" not in cat.list_tables()

    def test_drop_cleans_files(self, tmp_path):
        cat = Catalog(str(tmp_path))
        cat.create_table(make_schema("items"))
        # Create the data files by opening a Table
        from table import Table
        schema = cat.get_schema("items")
        t = Table(cat.db_path("items"), schema)
        t.insert({"label": "test", "value": 42})
        t.close()

        # Verify files exist
        base = cat.db_path("items")
        assert os.path.exists(base)

        cat.drop_table("items")
        for ext in ("", ".idx", ".meta", ".free"):
            assert not os.path.exists(base + ext)

    def test_drop_persists(self, tmp_path):
        cat1 = Catalog(str(tmp_path))
        cat1.create_table(make_schema("items"))
        cat1.drop_table("items")

        cat2 = Catalog(str(tmp_path))
        assert cat2.table_exists("items") is False
