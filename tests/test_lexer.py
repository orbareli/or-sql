"""Tests for the SQL lexer."""
from ast_sql.lexer import Lexer, TokenType


def tokenize(sql):
    return Lexer(sql).tokenizer()


def token_types(sql):
    return [t.type for t in tokenize(sql)]


def token_values(sql):
    return [(t.type, t.value) for t in tokenize(sql)]


class TestSelectTokens:
    def test_select_star(self):
        types = token_types("SELECT * FROM users")
        assert types == [TokenType.SELECT, TokenType.STAR, TokenType.FROM, TokenType.IDENTIFIER, TokenType.EOF]

    def test_select_columns(self):
        types = token_types("SELECT name, age FROM users")
        assert types == [
            TokenType.SELECT, TokenType.IDENTIFIER, TokenType.COMMA,
            TokenType.IDENTIFIER, TokenType.FROM, TokenType.IDENTIFIER, TokenType.EOF,
        ]

    def test_select_where_eq(self):
        types = token_types("SELECT * FROM users WHERE id = 3")
        assert types == [
            TokenType.SELECT, TokenType.STAR, TokenType.FROM, TokenType.IDENTIFIER,
            TokenType.WHERE, TokenType.IDENTIFIER, TokenType.EQ, TokenType.NUMBER, TokenType.EOF,
        ]

    def test_select_where_lt(self):
        tokens = tokenize("SELECT * FROM users WHERE age < 30")
        ops = [t for t in tokens if t.type == TokenType.LT]
        assert len(ops) == 1
        assert ops[0].value == "<"

    def test_select_where_gt(self):
        tokens = tokenize("SELECT * FROM users WHERE age > 25")
        ops = [t for t in tokens if t.type == TokenType.GT]
        assert len(ops) == 1
        assert ops[0].value == ">"


class TestInsertTokens:
    def test_insert_basic(self):
        sql = 'INSERT INTO users (name, age) VALUES ("Alice", 30)'
        types = token_types(sql)
        assert types == [
            TokenType.INSERT, TokenType.INTO, TokenType.IDENTIFIER,
            TokenType.LPAREN, TokenType.IDENTIFIER, TokenType.COMMA, TokenType.IDENTIFIER, TokenType.RPAREN,
            TokenType.VALUES,
            TokenType.LPAREN, TokenType.STRING, TokenType.COMMA, TokenType.NUMBER, TokenType.RPAREN,
            TokenType.EOF,
        ]

    def test_string_value(self):
        tokens = tokenize('INSERT INTO t (name) VALUES ("Bob")')
        string_tokens = [t for t in tokens if t.type == TokenType.STRING]
        assert len(string_tokens) == 1
        assert string_tokens[0].value == "Bob"

    def test_single_quotes(self):
        tokens = tokenize("INSERT INTO t (name) VALUES ('Eve')")
        string_tokens = [t for t in tokens if t.type == TokenType.STRING]
        assert string_tokens[0].value == "Eve"


class TestDeleteTokens:
    def test_delete_where(self):
        types = token_types("DELETE FROM users WHERE id = 5")
        assert types == [
            TokenType.DELETE, TokenType.FROM, TokenType.IDENTIFIER,
            TokenType.WHERE, TokenType.IDENTIFIER, TokenType.EQ, TokenType.NUMBER,
            TokenType.EOF,
        ]

    def test_delete_no_where(self):
        types = token_types("DELETE FROM users")
        assert types == [TokenType.DELETE, TokenType.FROM, TokenType.IDENTIFIER, TokenType.EOF]


class TestUpdateTokens:
    def test_update_set(self):
        types = token_types('UPDATE users SET name = "Bob" WHERE id = 1')
        assert types == [
            TokenType.UPDATE, TokenType.IDENTIFIER, TokenType.SET,
            TokenType.IDENTIFIER, TokenType.EQ, TokenType.STRING,
            TokenType.WHERE, TokenType.IDENTIFIER, TokenType.EQ, TokenType.NUMBER,
            TokenType.EOF,
        ]


class TestNewOperators:
    def test_lte(self):
        tokens = tokenize("SELECT * FROM users WHERE age <= 30")
        ops = [t for t in tokens if t.type == TokenType.LTE]
        assert len(ops) == 1
        assert ops[0].value == "<="

    def test_gte(self):
        tokens = tokenize("SELECT * FROM users WHERE age >= 25")
        ops = [t for t in tokens if t.type == TokenType.GTE]
        assert len(ops) == 1
        assert ops[0].value == ">="

    def test_neq(self):
        tokens = tokenize("SELECT * FROM users WHERE age != 30")
        ops = [t for t in tokens if t.type == TokenType.NEQ]
        assert len(ops) == 1
        assert ops[0].value == "!="

    def test_bang_without_eq(self):
        import pytest
        with pytest.raises(ValueError, match="Expected '=' after '!'"):
            tokenize("SELECT * FROM users WHERE age ! 30")


class TestLogicalOperators:
    def test_and_keyword(self):
        types = token_types("SELECT * FROM users WHERE age > 20 AND age < 30")
        assert TokenType.AND in types

    def test_or_keyword(self):
        types = token_types("SELECT * FROM users WHERE age < 20 OR age > 30")
        assert TokenType.OR in types


class TestOrderByTokens:
    def test_order_by_asc(self):
        types = token_types("SELECT * FROM users ORDER BY age ASC")
        assert TokenType.ORDER in types
        assert TokenType.BY in types
        assert TokenType.ASC in types

    def test_limit_offset(self):
        types = token_types("SELECT * FROM users LIMIT 10 OFFSET 5")
        assert TokenType.LIMIT in types
        assert TokenType.OFFSET in types


class TestGroupByTokens:
    def test_group_by(self):
        types = token_types("SELECT age FROM users GROUP BY age")
        assert TokenType.GROUP in types
        assert TokenType.BY in types


class TestLikeTokens:
    def test_like_keyword(self):
        types = token_types('SELECT * FROM users WHERE name LIKE "A%"')
        assert TokenType.LIKE in types


class TestDDLTokens:
    def test_create_table(self):
        types = token_types("CREATE TABLE items (name TEXT, value INTEGER)")
        assert types == [
            TokenType.CREATE, TokenType.TABLE, TokenType.IDENTIFIER,
            TokenType.LPAREN,
            TokenType.IDENTIFIER, TokenType.TEXT_TYPE, TokenType.COMMA,
            TokenType.IDENTIFIER, TokenType.INTEGER_TYPE,
            TokenType.RPAREN, TokenType.EOF,
        ]

    def test_create_table_with_size(self):
        types = token_types("CREATE TABLE items (name TEXT(50), value INTEGER)")
        assert TokenType.TEXT_TYPE in types
        assert TokenType.NUMBER in types

    def test_drop_table(self):
        types = token_types("DROP TABLE items")
        assert types == [TokenType.DROP, TokenType.TABLE, TokenType.IDENTIFIER, TokenType.EOF]

    def test_show_tables(self):
        types = token_types("SHOW TABLES")
        assert types == [TokenType.SHOW, TokenType.TABLES, TokenType.EOF]

    def test_float_type(self):
        types = token_types("CREATE TABLE t (score FLOAT)")
        assert TokenType.FLOAT_TYPE in types

    def test_boolean_type(self):
        types = token_types("CREATE TABLE t (active BOOLEAN)")
        assert TokenType.BOOLEAN_TYPE in types

    def test_case_insensitive_ddl(self):
        types = token_types("create table items (name text)")
        assert TokenType.CREATE in types
        assert TokenType.TABLE in types
        assert TokenType.TEXT_TYPE in types


class TestJoinTokens:
    def test_inner_join_tokens(self):
        types = token_types("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id")
        assert types == [
            TokenType.SELECT, TokenType.STAR, TokenType.FROM, TokenType.IDENTIFIER,
            TokenType.INNER, TokenType.JOIN, TokenType.IDENTIFIER, TokenType.ON,
            TokenType.IDENTIFIER, TokenType.DOT, TokenType.IDENTIFIER,
            TokenType.EQ,
            TokenType.IDENTIFIER, TokenType.DOT, TokenType.IDENTIFIER,
            TokenType.EOF,
        ]

    def test_left_join_tokens(self):
        types = token_types("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id")
        assert TokenType.LEFT in types
        assert TokenType.JOIN in types
        assert TokenType.ON in types

    def test_join_alone_tokens(self):
        types = token_types("SELECT * FROM users JOIN orders ON users.id = orders.user_id")
        assert TokenType.JOIN in types
        assert TokenType.INNER not in types

    def test_dot_in_qualified_name(self):
        tokens = tokenize("users.id")
        assert [(t.type, t.value) for t in tokens] == [
            (TokenType.IDENTIFIER, "users"),
            (TokenType.DOT, None),
            (TokenType.IDENTIFIER, "id"),
            (TokenType.EOF, None),
        ]

    def test_multiple_qualified_names(self):
        types = token_types("t1.col1 = t2.col2")
        assert types == [
            TokenType.IDENTIFIER, TokenType.DOT, TokenType.IDENTIFIER,
            TokenType.EQ,
            TokenType.IDENTIFIER, TokenType.DOT, TokenType.IDENTIFIER,
            TokenType.EOF,
        ]

    def test_case_insensitive_join(self):
        types = token_types("select * from users inner join orders on users.id = orders.user_id")
        assert TokenType.INNER in types
        assert TokenType.JOIN in types
        assert TokenType.ON in types


class TestEdgeCases:
    def test_extra_whitespace(self):
        types = token_types("  SELECT   *   FROM   users  ")
        assert types == [TokenType.SELECT, TokenType.STAR, TokenType.FROM, TokenType.IDENTIFIER, TokenType.EOF]

    def test_number_value(self):
        tokens = tokenize("SELECT * FROM t WHERE id = 42")
        num = [t for t in tokens if t.type == TokenType.NUMBER]
        assert num[0].value == 42

    def test_case_insensitive_keywords(self):
        types = token_types("select * from users")
        assert types == [TokenType.SELECT, TokenType.STAR, TokenType.FROM, TokenType.IDENTIFIER, TokenType.EOF]

    def test_identifier_with_underscore(self):
        tokens = tokenize("SELECT first_name FROM users")
        ids = [t for t in tokens if t.type == TokenType.IDENTIFIER]
        assert ids[0].value == "first_name"

    def test_empty_input(self):
        tokens = tokenize("")
        assert len(tokens) == 1
        assert tokens[0].type == TokenType.EOF

    def test_unexpected_character(self):
        import pytest
        with pytest.raises(ValueError, match="Unexpected character"):
            tokenize("SELECT @invalid")
