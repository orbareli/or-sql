"""Tests for the SQL parser."""
import pytest
from ast_sql.lexer import Lexer
from ast_sql.parser import Parser, ParseError


def parse(sql):
    tokens = Lexer(sql).tokenizer()
    return Parser(tokens).parse()


class TestSelect:
    def test_select_star(self):
        ast = parse("SELECT * FROM users")
        assert ast["action"] == "SELECT"
        assert ast["columns"] == ["*"]
        assert ast["table"] == "users"
        assert ast["where"] is None

    def test_select_columns(self):
        ast = parse("SELECT name, age FROM users")
        assert ast["columns"] == ["name", "age"]

    def test_select_single_column(self):
        ast = parse("SELECT name FROM users")
        assert ast["columns"] == ["name"]

    def test_select_where_eq(self):
        ast = parse("SELECT * FROM users WHERE id = 3")
        assert ast["where"] == ("id", "=", 3)

    def test_select_where_lt(self):
        ast = parse("SELECT * FROM users WHERE age < 30")
        assert ast["where"] == ("age", "<", 30)

    def test_select_where_gt(self):
        ast = parse("SELECT * FROM users WHERE age > 25")
        assert ast["where"] == ("age", ">", 25)

    def test_select_where_string(self):
        ast = parse('SELECT * FROM users WHERE name = "Alice"')
        assert ast["where"] == ("name", "=", "Alice")


class TestInsert:
    def test_insert_basic(self):
        ast = parse('INSERT INTO users (name, age) VALUES ("Alice", 30)')
        assert ast["action"] == "INSERT"
        assert ast["table"] == "users"
        assert ast["columns"] == ["name", "age"]
        assert ast["values"] == ["Alice", 30]

    def test_insert_single_value(self):
        ast = parse('INSERT INTO users (name) VALUES ("Bob")')
        assert ast["columns"] == ["name"]
        assert ast["values"] == ["Bob"]

    def test_insert_mismatched_columns_values(self):
        with pytest.raises(ParseError, match="Column count"):
            parse('INSERT INTO users (name, age) VALUES ("Alice")')


class TestDelete:
    def test_delete_where_id(self):
        ast = parse("DELETE FROM users WHERE id = 5")
        assert ast["action"] == "DELETE"
        assert ast["table"] == "users"
        assert ast["where"] == ("id", "=", 5)

    def test_delete_no_where(self):
        ast = parse("DELETE FROM users")
        assert ast["where"] is None


class TestUpdate:
    def test_update_single_assignment(self):
        ast = parse('UPDATE users SET name = "Bob" WHERE id = 1')
        assert ast["action"] == "UPDATE"
        assert ast["table"] == "users"
        assert ast["assignments"] == [("name", "Bob")]
        assert ast["where"] == ("id", "=", 1)

    def test_update_multiple_assignments(self):
        ast = parse('UPDATE users SET name = "Bob", age = 35 WHERE id = 1')
        assert ast["assignments"] == [("name", "Bob"), ("age", 35)]

    def test_update_no_where(self):
        ast = parse('UPDATE users SET age = 99')
        assert ast["where"] is None


class TestNewOperators:
    def test_lte(self):
        ast = parse("SELECT * FROM users WHERE age <= 30")
        assert ast["where"] == ("age", "<=", 30)

    def test_gte(self):
        ast = parse("SELECT * FROM users WHERE age >= 25")
        assert ast["where"] == ("age", ">=", 25)

    def test_neq(self):
        ast = parse("SELECT * FROM users WHERE age != 30")
        assert ast["where"] == ("age", "!=", 30)


class TestAndOr:
    def test_and(self):
        ast = parse("SELECT * FROM users WHERE age > 20 AND age < 30")
        w = ast["where"]
        assert w["op"] == "AND"
        assert w["left"] == ("age", ">", 20)
        assert w["right"] == ("age", "<", 30)

    def test_or(self):
        ast = parse("SELECT * FROM users WHERE age < 20 OR age > 30")
        w = ast["where"]
        assert w["op"] == "OR"
        assert w["left"] == ("age", "<", 20)
        assert w["right"] == ("age", ">", 30)

    def test_chained_and(self):
        ast = parse("SELECT * FROM users WHERE age > 20 AND age < 30 AND id = 1")
        w = ast["where"]
        # Left-associative: ((age>20 AND age<30) AND id=1)
        assert w["op"] == "AND"
        assert w["right"] == ("id", "=", 1)
        assert w["left"]["op"] == "AND"


class TestOrderBy:
    def test_order_by_asc(self):
        ast = parse("SELECT * FROM users ORDER BY age ASC")
        assert ast["order_by"] == {"column": "age", "direction": "ASC"}

    def test_order_by_desc(self):
        ast = parse("SELECT * FROM users ORDER BY age DESC")
        assert ast["order_by"] == {"column": "age", "direction": "DESC"}

    def test_order_by_default_asc(self):
        ast = parse("SELECT * FROM users ORDER BY age")
        assert ast["order_by"] == {"column": "age", "direction": "ASC"}


class TestLimitOffset:
    def test_limit(self):
        ast = parse("SELECT * FROM users LIMIT 10")
        assert ast["limit"] == 10
        assert ast["offset"] is None

    def test_limit_offset(self):
        ast = parse("SELECT * FROM users LIMIT 10 OFFSET 5")
        assert ast["limit"] == 10
        assert ast["offset"] == 5

    def test_order_limit(self):
        ast = parse("SELECT * FROM users ORDER BY age LIMIT 5")
        assert ast["order_by"]["column"] == "age"
        assert ast["limit"] == 5


class TestAggregates:
    def test_count_star(self):
        ast = parse("SELECT COUNT(*) FROM users")
        assert ast["columns"] == [{"func": "COUNT", "arg": "*"}]

    def test_count_column(self):
        ast = parse("SELECT COUNT(name) FROM users")
        assert ast["columns"] == [{"func": "COUNT", "arg": "name"}]

    def test_sum(self):
        ast = parse("SELECT SUM(age) FROM users")
        assert ast["columns"] == [{"func": "SUM", "arg": "age"}]

    def test_avg(self):
        ast = parse("SELECT AVG(age) FROM users")
        assert ast["columns"] == [{"func": "AVG", "arg": "age"}]

    def test_mixed_columns(self):
        ast = parse("SELECT name, COUNT(*) FROM users")
        assert ast["columns"][0] == "name"
        assert ast["columns"][1] == {"func": "COUNT", "arg": "*"}


class TestGroupBy:
    def test_group_by(self):
        ast = parse("SELECT age, COUNT(*) FROM users GROUP BY age")
        assert ast["group_by"] == "age"

    def test_group_by_with_order(self):
        ast = parse("SELECT age, COUNT(*) FROM users GROUP BY age ORDER BY age")
        assert ast["group_by"] == "age"
        assert ast["order_by"]["column"] == "age"


class TestLike:
    def test_like_in_where(self):
        ast = parse('SELECT * FROM users WHERE name LIKE "A%"')
        assert ast["where"] == ("name", "LIKE", "A%")


class TestCreateTable:
    def test_create_basic(self):
        ast = parse("CREATE TABLE items (name TEXT, value INTEGER)")
        assert ast["action"] == "CREATE_TABLE"
        assert ast["table"] == "items"
        assert len(ast["columns"]) == 2
        assert ast["columns"][0] == {"name": "name", "type": "TEXT"}
        assert ast["columns"][1] == {"name": "value", "type": "INTEGER"}

    def test_create_with_size(self):
        ast = parse("CREATE TABLE items (name TEXT(50), value INTEGER)")
        assert ast["columns"][0] == {"name": "name", "type": "TEXT", "size": 50}

    def test_create_all_types(self):
        ast = parse("CREATE TABLE t (a INTEGER, b TEXT(10), c FLOAT, d BOOLEAN)")
        types = [c["type"] for c in ast["columns"]]
        assert types == ["INTEGER", "TEXT", "FLOAT", "BOOLEAN"]

    def test_create_single_column(self):
        ast = parse("CREATE TABLE t (id INTEGER)")
        assert len(ast["columns"]) == 1


class TestDropTable:
    def test_drop_basic(self):
        ast = parse("DROP TABLE items")
        assert ast["action"] == "DROP_TABLE"
        assert ast["table"] == "items"


class TestShowTables:
    def test_show_tables(self):
        ast = parse("SHOW TABLES")
        assert ast["action"] == "SHOW_TABLES"


class TestJoinParsing:
    def test_inner_join(self):
        ast = parse("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id")
        assert ast["table"] == "users"
        assert len(ast["joins"]) == 1
        join = ast["joins"][0]
        assert join["type"] == "INNER"
        assert join["table"] == "orders"

    def test_left_join(self):
        ast = parse("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id")
        assert ast["joins"][0]["type"] == "LEFT"

    def test_join_shorthand(self):
        ast = parse("SELECT * FROM users JOIN orders ON users.id = orders.user_id")
        assert ast["joins"][0]["type"] == "INNER"

    def test_multiple_joins(self):
        ast = parse(
            "SELECT * FROM users "
            "INNER JOIN orders ON users.id = orders.user_id "
            "INNER JOIN products ON orders.product_id = products.id"
        )
        assert len(ast["joins"]) == 2

    def test_join_with_where(self):
        ast = parse(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id "
            'WHERE users.name = "Alice"'
        )
        assert ast["joins"] is not None
        assert ast["where"] is not None

    def test_join_with_order_by(self):
        ast = parse(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id "
            "ORDER BY orders.amount DESC"
        )
        assert ast["order_by"]["column"] == "orders.amount"


class TestQualifiedNames:
    def test_qualified_in_select(self):
        ast = parse("SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id")
        assert ast["columns"] == ["users.name", "orders.amount"]

    def test_qualified_in_where(self):
        ast = parse('SELECT * FROM users WHERE users.name = "Alice"')
        assert ast["where"][0] == "users.name"

    def test_on_condition_has_column_ref(self):
        from ast_sql.parser import ColumnRef
        ast = parse("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id")
        on = ast["joins"][0]["on"]
        assert on[0] == "users.id"
        assert on[2] == ColumnRef("orders.user_id")


class TestJoinBackwardCompat:
    def test_select_without_join_has_none(self):
        ast = parse("SELECT * FROM users")
        assert ast["joins"] is None

    def test_existing_select_still_works(self):
        ast = parse("SELECT name, age FROM users WHERE age > 20 ORDER BY age LIMIT 10")
        assert ast["joins"] is None
        assert ast["columns"] == ["name", "age"]
        assert ast["order_by"]["column"] == "age"


class TestErrors:
    def test_unexpected_token(self):
        with pytest.raises(ParseError):
            parse("FOOBAR something")

    def test_missing_from(self):
        with pytest.raises(ParseError):
            parse("SELECT * users")

    def test_bad_where_operator(self):
        with pytest.raises(ParseError, match="comparison operator"):
            parse("SELECT * FROM users WHERE id name 3")

    def test_missing_value_in_where(self):
        with pytest.raises(ParseError, match="Expected a value"):
            parse("SELECT * FROM users WHERE id = FROM")
