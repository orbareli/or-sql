"""Tests for the Executor (full SQL -> result pipeline)."""
import pytest


@pytest.fixture
def table(executor):
    """Get the users table from the executor's catalog."""
    return executor._get_table("users")


class TestSelectExecution:
    def test_select_all_empty(self, executor):
        result = executor.run("SELECT * FROM users")
        assert result == []

    def test_select_all(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        result = executor.run("SELECT * FROM users")
        assert len(result) == 2
        assert result[0]["name"] == "Alice"

    def test_select_specific_columns(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        result = executor.run("SELECT name FROM users")
        assert len(result) == 1
        assert "name" in result[0]
        assert "id" not in result[0]

    def test_select_where_id(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        result = executor.run("SELECT * FROM users WHERE id = 1")
        assert len(result) == 1
        assert result[0]["name"] == "Alice"

    def test_select_where_gt(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        result = executor.run("SELECT * FROM users WHERE age > 27")
        assert len(result) == 1
        assert result[0]["name"] == "Alice"

    def test_select_where_lt(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        result = executor.run("SELECT * FROM users WHERE age < 27")
        assert len(result) == 1
        assert result[0]["name"] == "Bob"

    def test_select_no_match(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        result = executor.run("SELECT * FROM users WHERE id = 999")
        assert result == "No record found."


class TestInsertExecution:
    def test_insert_basic(self, executor):
        result = executor.run('INSERT INTO users (name, age) VALUES ("Alice", 30)')
        assert "Inserted" in result
        assert "id=1" in result

    def test_insert_multiple(self, executor):
        executor.run('INSERT INTO users (name, age) VALUES ("Alice", 30)')
        result = executor.run('INSERT INTO users (name, age) VALUES ("Bob", 25)')
        assert "id=2" in result

    def test_insert_missing_name(self, executor):
        result = executor.run('INSERT INTO users (age) VALUES (30)')
        assert "Error" in result

    def test_insert_missing_age(self, executor):
        result = executor.run('INSERT INTO users (name) VALUES ("Alice")')
        assert "Error" in result


class TestDeleteExecution:
    def test_delete_by_id(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        result = executor.run("DELETE FROM users WHERE id = 1")
        assert "Deleted" in result
        assert table.select_all() == []

    def test_delete_nonexistent(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        result = executor.run("DELETE FROM users WHERE id = 999")
        assert "No record" in result

    def test_delete_with_filter(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        table.insert({"name": "Charlie", "age": 35})
        result = executor.run("DELETE FROM users WHERE age > 28")
        assert "Deleted 2 record(s)" in result
        rows = table.select_all()
        assert len(rows) == 1
        assert rows[0]["name"] == "Bob"

    def test_delete_all(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        result = executor.run("DELETE FROM users")
        assert "Deleted 2 record(s)" in result
        assert table.select_all() == []


class TestUpdateExecution:
    def test_update_by_id(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        result = executor.run('UPDATE users SET name = "Bob" WHERE id = 1')
        assert "Updated 1 record(s)" in result
        rows = executor.run("SELECT * FROM users WHERE id = 1")
        assert rows[0]["name"] == "Bob"

    def test_update_multiple_fields(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        result = executor.run('UPDATE users SET name = "Bob", age = 99 WHERE id = 1')
        assert "Updated 1 record(s)" in result
        rows = executor.run("SELECT * FROM users WHERE id = 1")
        assert rows[0]["name"] == "Bob"
        assert rows[0]["age"] == 99

    def test_update_nonexistent(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        result = executor.run('UPDATE users SET name = "Bob" WHERE id = 999')
        assert "No record found" in result

    def test_update_without_where(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        result = executor.run('UPDATE users SET age = 50')
        assert "Updated 2 record(s)" in result
        rows = executor.run("SELECT * FROM users")
        assert all(r["age"] == 50 for r in rows)

    def test_update_with_filter(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        result = executor.run('UPDATE users SET age = 99 WHERE age > 27')
        assert "Updated 1 record(s)" in result
        rows = executor.run("SELECT * FROM users WHERE id = 1")
        assert rows[0]["age"] == 99


class TestNewOperators:
    def test_lte(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        table.insert({"name": "Charlie", "age": 35})
        result = executor.run("SELECT * FROM users WHERE age <= 30")
        assert len(result) == 2

    def test_gte(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        table.insert({"name": "Charlie", "age": 35})
        result = executor.run("SELECT * FROM users WHERE age >= 30")
        assert len(result) == 2

    def test_neq(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        result = executor.run("SELECT * FROM users WHERE age != 30")
        assert len(result) == 1
        assert result[0]["name"] == "Bob"


class TestAndOr:
    def test_and(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        table.insert({"name": "Charlie", "age": 35})
        result = executor.run("SELECT * FROM users WHERE age > 20 AND age < 32")
        assert len(result) == 2
        names = sorted([r["name"] for r in result])
        assert names == ["Alice", "Bob"]

    def test_or(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        table.insert({"name": "Charlie", "age": 35})
        result = executor.run("SELECT * FROM users WHERE age = 25 OR age = 35")
        assert len(result) == 2

    def test_and_no_match(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        result = executor.run("SELECT * FROM users WHERE age > 40 AND age < 50")
        assert result == []

    def test_delete_with_and(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        table.insert({"name": "Charlie", "age": 35})
        result = executor.run("DELETE FROM users WHERE age >= 30 AND age <= 35")
        assert "Deleted 2 record(s)" in result


class TestOrderBy:
    def test_order_by_asc(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        table.insert({"name": "Charlie", "age": 35})
        result = executor.run("SELECT * FROM users ORDER BY age ASC")
        assert result[0]["name"] == "Bob"
        assert result[2]["name"] == "Charlie"

    def test_order_by_desc(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        table.insert({"name": "Charlie", "age": 35})
        result = executor.run("SELECT * FROM users ORDER BY age DESC")
        assert result[0]["name"] == "Charlie"
        assert result[2]["name"] == "Bob"

    def test_order_by_name(self, executor, table):
        table.insert({"name": "Charlie", "age": 35})
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        result = executor.run("SELECT * FROM users ORDER BY name ASC")
        assert result[0]["name"] == "Alice"


class TestLimitOffset:
    def test_limit(self, executor, table):
        for i in range(5):
            table.insert({"name": f"User{i}", "age": 20 + i})
        result = executor.run("SELECT * FROM users LIMIT 3")
        assert len(result) == 3

    def test_offset(self, executor, table):
        for i in range(5):
            table.insert({"name": f"User{i}", "age": 20 + i})
        result = executor.run("SELECT * FROM users LIMIT 2 OFFSET 2")
        assert len(result) == 2
        assert result[0]["name"] == "User2"

    def test_order_by_limit(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        table.insert({"name": "Charlie", "age": 35})
        result = executor.run("SELECT * FROM users ORDER BY age DESC LIMIT 2")
        assert len(result) == 2
        assert result[0]["name"] == "Charlie"
        assert result[1]["name"] == "Alice"


class TestAggregates:
    def test_count_star(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        result = executor.run("SELECT COUNT(*) FROM users")
        assert result[0]["COUNT(*)"] == 2

    def test_count_empty(self, executor):
        result = executor.run("SELECT COUNT(*) FROM users")
        assert result[0]["COUNT(*)"] == 0

    def test_sum(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        result = executor.run("SELECT SUM(age) FROM users")
        assert result[0]["SUM(age)"] == 55

    def test_avg(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 20})
        result = executor.run("SELECT AVG(age) FROM users")
        assert result[0]["AVG(age)"] == 25.0

    def test_min_max(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        table.insert({"name": "Charlie", "age": 35})
        result = executor.run("SELECT MIN(age) FROM users")
        assert result[0]["MIN(age)"] == 25
        result = executor.run("SELECT MAX(age) FROM users")
        assert result[0]["MAX(age)"] == 35

    def test_count_with_where(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        table.insert({"name": "Charlie", "age": 35})
        result = executor.run("SELECT COUNT(*) FROM users WHERE age > 27")
        assert result[0]["COUNT(*)"] == 2


class TestGroupBy:
    def test_group_by_count(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 30})
        table.insert({"name": "Charlie", "age": 25})
        result = executor.run("SELECT age, COUNT(*) FROM users GROUP BY age")
        assert len(result) == 2
        by_age = {r["age"]: r["COUNT(*)"] for r in result}
        assert by_age[30] == 2
        assert by_age[25] == 1

    def test_group_by_sum(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 30})
        table.insert({"name": "Charlie", "age": 25})
        result = executor.run("SELECT age, SUM(age) FROM users GROUP BY age")
        by_age = {r["age"]: r["SUM(age)"] for r in result}
        assert by_age[30] == 60
        assert by_age[25] == 25

    def test_group_by_order(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 30})
        table.insert({"name": "Charlie", "age": 25})
        result = executor.run("SELECT age, COUNT(*) FROM users GROUP BY age ORDER BY age ASC")
        assert result[0]["age"] == 25
        assert result[1]["age"] == 30


class TestLike:
    def test_like_percent(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Alex", "age": 25})
        table.insert({"name": "Bob", "age": 35})
        result = executor.run('SELECT * FROM users WHERE name LIKE "Al%"')
        assert len(result) == 2

    def test_like_underscore(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        result = executor.run('SELECT * FROM users WHERE name LIKE "Bo_"')
        assert len(result) == 1
        assert result[0]["name"] == "Bob"

    def test_like_exact(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        table.insert({"name": "Bob", "age": 25})
        result = executor.run('SELECT * FROM users WHERE name LIKE "Alice"')
        assert len(result) == 1

    def test_like_no_match(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        result = executor.run('SELECT * FROM users WHERE name LIKE "Z%"')
        assert result == []


class TestParseErrors:
    def test_bad_sql(self, executor):
        result = executor.run("FOOBAR whatever")
        assert "Parse error" in result

    def test_missing_table(self, executor):
        result = executor.run("SELECT * FROM")
        assert "error" in result.lower()


class TestCacheIntegration:
    def test_cache_hit(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        result1 = executor.run("SELECT * FROM users")
        result2 = executor.run("SELECT * FROM users")
        assert result1 == result2
        assert executor.cache.hits >= 1

    def test_cache_invalidated_on_insert(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        executor.run("SELECT * FROM users")
        executor.run('INSERT INTO users (name, age) VALUES ("Bob", 25)')
        result = executor.run("SELECT * FROM users")
        assert len(result) == 2

    def test_cache_invalidated_on_delete(self, executor, table):
        table.insert({"name": "Alice", "age": 30})
        executor.run("SELECT * FROM users")
        executor.run("DELETE FROM users WHERE id = 1")
        result = executor.run("SELECT * FROM users")
        assert result == []
