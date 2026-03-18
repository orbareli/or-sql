"""
Shared fixtures for all tests.
Adds the db/ directory to sys.path so imports work.
Provides temp directory fixtures for database files.
"""
import sys
import os
import pytest

# Add db/ to path so we can import modules directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'db'))


@pytest.fixture
def tmp_db(tmp_path):
    """Return a path for a temporary database file."""
    return str(tmp_path / "test.db")


@pytest.fixture
def users_schema():
    """Return the classic users schema: (name TEXT(20), age INTEGER)."""
    from schema import Schema, Column, ColumnType
    return Schema("users", [
        Column("name", ColumnType.TEXT, 20),
        Column("age", ColumnType.INTEGER),
    ])


@pytest.fixture
def table(tmp_db, users_schema):
    """Return a Table instance backed by a temp file."""
    from table import Table
    t = Table(tmp_db, users_schema)
    yield t
    t.close()


@pytest.fixture
def catalog(tmp_path):
    """Return a Catalog instance in a temp directory with a pre-registered users table."""
    from catalog import Catalog
    from schema import Schema, Column, ColumnType
    cat = Catalog(str(tmp_path))
    users_schema = Schema("users", [
        Column("name", ColumnType.TEXT, 20),
        Column("age", ColumnType.INTEGER),
    ])
    cat.create_table(users_schema)
    return cat


@pytest.fixture
def executor(catalog):
    """Return an Executor instance backed by a Catalog with a users table."""
    from executor import Executor
    ex = Executor(catalog)
    yield ex
    ex.close()


@pytest.fixture
def join_executor(tmp_path):
    """Return an Executor with users + orders tables for JOIN tests."""
    from catalog import Catalog
    from schema import Schema, Column, ColumnType
    from executor import Executor

    cat = Catalog(str(tmp_path))
    users_schema = Schema("users", [
        Column("name", ColumnType.TEXT, 20),
        Column("age", ColumnType.INTEGER),
    ])
    orders_schema = Schema("orders", [
        Column("user_id", ColumnType.INTEGER),
        Column("amount", ColumnType.INTEGER),
    ])
    cat.create_table(users_schema)
    cat.create_table(orders_schema)

    ex = Executor(cat)
    yield ex
    ex.close()
