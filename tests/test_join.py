"""Tests for JOIN support — INNER JOIN, LEFT JOIN, qualified column names."""
import pytest


# ---------------------------------------------------------------- #
#  Helper to seed data                                              #
# ---------------------------------------------------------------- #

def seed_data(ex):
    """Seed users and orders tables with test data."""
    ex.run('INSERT INTO users (name, age) VALUES ("Alice", 30)')
    ex.run('INSERT INTO users (name, age) VALUES ("Bob", 25)')
    ex.run('INSERT INTO users (name, age) VALUES ("Charlie", 35)')
    ex.run('INSERT INTO orders (user_id, amount) VALUES (1, 100)')
    ex.run('INSERT INTO orders (user_id, amount) VALUES (1, 200)')
    ex.run('INSERT INTO orders (user_id, amount) VALUES (2, 50)')


# ---------------------------------------------------------------- #
#  INNER JOIN                                                       #
# ---------------------------------------------------------------- #

class TestInnerJoin:
    def test_simple_inner_join(self, join_executor):
        seed_data(join_executor)
        result = join_executor.run(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
        )
        assert len(result) == 3  # Alice x2, Bob x1

    def test_no_matches(self, join_executor):
        join_executor.run('INSERT INTO users (name, age) VALUES ("Alice", 30)')
        join_executor.run('INSERT INTO orders (user_id, amount) VALUES (99, 100)')
        result = join_executor.run(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
        )
        assert result == []

    def test_one_to_many(self, join_executor):
        seed_data(join_executor)
        result = join_executor.run(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
        )
        alice_rows = [r for r in result if r.get("name") == "Alice"]
        assert len(alice_rows) == 2

    def test_join_without_inner_keyword(self, join_executor):
        seed_data(join_executor)
        result = join_executor.run(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
        )
        assert len(result) == 3

    def test_result_columns_from_both_tables(self, join_executor):
        seed_data(join_executor)
        result = join_executor.run(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
        )
        row = result[0]
        assert "name" in row
        assert "amount" in row

    def test_column_conflict_stays_qualified(self, join_executor):
        seed_data(join_executor)
        result = join_executor.run(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
        )
        row = result[0]
        assert "users.id" in row
        assert "orders.id" in row

    def test_unique_columns_unqualified(self, join_executor):
        seed_data(join_executor)
        result = join_executor.run(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
        )
        row = result[0]
        assert "name" in row
        assert "age" in row
        assert "user_id" in row
        assert "amount" in row

    def test_inner_join_with_where(self, join_executor):
        seed_data(join_executor)
        result = join_executor.run(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id WHERE amount > 50"
        )
        assert len(result) == 2
        for r in result:
            assert r["amount"] > 50


# ---------------------------------------------------------------- #
#  LEFT JOIN                                                        #
# ---------------------------------------------------------------- #

class TestLeftJoin:
    def test_all_match(self, join_executor):
        join_executor.run('INSERT INTO users (name, age) VALUES ("Alice", 30)')
        join_executor.run('INSERT INTO orders (user_id, amount) VALUES (1, 100)')
        result = join_executor.run(
            "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id"
        )
        assert len(result) == 1
        assert result[0]["amount"] == 100

    def test_no_match_fills_nulls(self, join_executor):
        join_executor.run('INSERT INTO users (name, age) VALUES ("Alice", 30)')
        # No orders for Alice
        result = join_executor.run(
            "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id"
        )
        assert len(result) == 1
        assert result[0]["name"] == "Alice"
        assert result[0]["amount"] is None

    def test_partial_match(self, join_executor):
        seed_data(join_executor)
        result = join_executor.run(
            "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id"
        )
        # Alice=2, Bob=1, Charlie=1 (null)
        assert len(result) == 4
        charlie_rows = [r for r in result if r.get("name") == "Charlie"]
        assert len(charlie_rows) == 1
        assert charlie_rows[0]["amount"] is None

    def test_empty_right_table(self, join_executor):
        join_executor.run('INSERT INTO users (name, age) VALUES ("Alice", 30)')
        join_executor.run('INSERT INTO users (name, age) VALUES ("Bob", 25)')
        result = join_executor.run(
            "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id"
        )
        assert len(result) == 2
        for r in result:
            assert r["amount"] is None

    def test_left_join_with_where(self, join_executor):
        seed_data(join_executor)
        result = join_executor.run(
            "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE amount > 50"
        )
        # Only Alice's 100 and 200
        assert len(result) == 2


# ---------------------------------------------------------------- #
#  Multiple JOINs                                                   #
# ---------------------------------------------------------------- #

class TestMultipleJoins:
    def _setup_three_tables(self, tmp_path):
        from catalog import Catalog
        from schema import Schema, Column, ColumnType
        from executor import Executor

        cat = Catalog(str(tmp_path))
        cat.create_table(Schema("users", [
            Column("name", ColumnType.TEXT, 20),
        ]))
        cat.create_table(Schema("orders", [
            Column("user_id", ColumnType.INTEGER),
            Column("product_id", ColumnType.INTEGER),
        ]))
        cat.create_table(Schema("products", [
            Column("name", ColumnType.TEXT, 30),
            Column("price", ColumnType.INTEGER),
        ]))
        ex = Executor(cat)
        ex.run('INSERT INTO users (name) VALUES ("Alice")')
        ex.run('INSERT INTO products (name, price) VALUES ("Widget", 10)')
        ex.run('INSERT INTO orders (user_id, product_id) VALUES (1, 1)')
        return ex

    def test_three_table_chain(self, tmp_path):
        ex = self._setup_three_tables(tmp_path)
        result = ex.run(
            "SELECT * FROM users "
            "INNER JOIN orders ON users.id = orders.user_id "
            "INNER JOIN products ON orders.product_id = products.id"
        )
        assert len(result) == 1
        assert result[0]["price"] == 10
        ex.close()

    def test_left_then_inner(self, tmp_path):
        ex = self._setup_three_tables(tmp_path)
        ex.run('INSERT INTO users (name) VALUES ("Bob")')  # No orders
        result = ex.run(
            "SELECT * FROM users "
            "LEFT JOIN orders ON users.id = orders.user_id "
            "INNER JOIN products ON orders.product_id = products.id"
        )
        # Only Alice matches (Bob has NULL order, can't match product)
        assert len(result) == 1
        ex.close()

    def test_multiple_joins_with_where(self, tmp_path):
        ex = self._setup_three_tables(tmp_path)
        result = ex.run(
            "SELECT * FROM users "
            "INNER JOIN orders ON users.id = orders.user_id "
            "INNER JOIN products ON orders.product_id = products.id "
            "WHERE price > 5"
        )
        assert len(result) == 1
        ex.close()


# ---------------------------------------------------------------- #
#  Qualified column names in SELECT, WHERE, ORDER BY               #
# ---------------------------------------------------------------- #

class TestQualifiedColumns:
    def test_select_specific_qualified_columns(self, join_executor):
        seed_data(join_executor)
        result = join_executor.run(
            "SELECT users.name, orders.amount FROM users "
            "INNER JOIN orders ON users.id = orders.user_id"
        )
        assert len(result) == 3
        row = result[0]
        # Unambiguous qualified names get simplified in output
        assert "name" in row
        assert "amount" in row
        assert len(row) == 2  # only the requested columns

    def test_where_with_qualified_column(self, join_executor):
        seed_data(join_executor)
        result = join_executor.run(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id "
            "WHERE users.name = \"Alice\""
        )
        assert len(result) == 2
        for r in result:
            assert r["name"] == "Alice"

    def test_order_by_qualified_column(self, join_executor):
        seed_data(join_executor)
        result = join_executor.run(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id "
            "ORDER BY orders.amount DESC"
        )
        amounts = [r["amount"] for r in result]
        assert amounts == [200, 100, 50]

    def test_join_with_limit_offset(self, join_executor):
        seed_data(join_executor)
        result = join_executor.run(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id "
            "ORDER BY orders.amount LIMIT 2 OFFSET 1"
        )
        assert len(result) == 2

    def test_count_on_joined_results(self, join_executor):
        seed_data(join_executor)
        result = join_executor.run(
            "SELECT COUNT(*) FROM users INNER JOIN orders ON users.id = orders.user_id"
        )
        assert result == [{"COUNT(*)": 3}]

    def test_sum_on_joined_results(self, join_executor):
        seed_data(join_executor)
        result = join_executor.run(
            "SELECT SUM(amount) FROM users INNER JOIN orders ON users.id = orders.user_id"
        )
        assert result == [{"SUM(amount)": 350}]


# ---------------------------------------------------------------- #
#  Edge cases                                                       #
# ---------------------------------------------------------------- #

class TestJoinEdgeCases:
    def test_empty_left_table(self, join_executor):
        join_executor.run('INSERT INTO orders (user_id, amount) VALUES (1, 100)')
        result = join_executor.run(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
        )
        assert result == []

    def test_empty_right_table_inner(self, join_executor):
        join_executor.run('INSERT INTO users (name, age) VALUES ("Alice", 30)')
        result = join_executor.run(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
        )
        assert result == []

    def test_cache_invalidation_after_insert(self, join_executor):
        join_executor.run('INSERT INTO users (name, age) VALUES ("Alice", 30)')
        result1 = join_executor.run(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
        )
        assert result1 == []
        # Insert an order and re-query
        join_executor.run('INSERT INTO orders (user_id, amount) VALUES (1, 100)')
        result2 = join_executor.run(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
        )
        assert len(result2) == 1
