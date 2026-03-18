"""
parser.py
---------
Stage 2 of the pipeline.
Takes a list of Token objects (from lexer.py) and builds an AST (dict).

The parser knows the grammar of SQL — which token sequences are valid.
"""
from ast_sql.lexer import TokenType


class ParseError(Exception):
    pass


class ColumnRef:
    """Marks a condition value as a column reference, not a literal."""
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return isinstance(other, ColumnRef) and self.name == other.name

    def __repr__(self):
        return f"ColumnRef({self.name!r})"


COMPARISON_OPS = {
    TokenType.EQ, TokenType.LT, TokenType.GT,
    TokenType.LTE, TokenType.GTE, TokenType.NEQ,
    TokenType.LIKE,
}

AGGREGATE_FUNCS = {"COUNT", "SUM", "AVG", "MIN", "MAX"}

TYPE_TOKENS = {
    TokenType.INTEGER_TYPE,
    TokenType.TEXT_TYPE,
    TokenType.FLOAT_TYPE,
    TokenType.BOOLEAN_TYPE,
}

TYPE_TOKEN_TO_NAME = {
    TokenType.INTEGER_TYPE: "INTEGER",
    TokenType.TEXT_TYPE: "TEXT",
    TokenType.FLOAT_TYPE: "FLOAT",
    TokenType.BOOLEAN_TYPE: "BOOLEAN",
}


class Parser:
    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0

    def peek(self):
        """Return the current token without advancing."""
        return self.tokens[self.pos]

    def consume(self, expected_type):
        """Consume and return the current token if it matches expected_type."""
        token = self.peek()
        if token.type == expected_type:
            self.pos += 1
            return token
        raise ParseError(f"Expected {expected_type}, got {token.type}")

    def parse_columns(self, element_parser_fn):
        """Parse a comma-separated list using the given element parser."""
        results = []
        results.append(element_parser_fn())
        while self.peek().type == TokenType.COMMA:
            self.consume(TokenType.COMMA)
            results.append(element_parser_fn())
        return results

    def _parse_identifier(self):
        """Parse a single identifier (column/table name)."""
        return self.consume(TokenType.IDENTIFIER).value

    def _parse_qualified_or_plain(self):
        """Parse IDENTIFIER or IDENTIFIER.IDENTIFIER (qualified name)."""
        name = self.consume(TokenType.IDENTIFIER).value
        if self.peek().type == TokenType.DOT:
            self.consume(TokenType.DOT)
            col = self.consume(TokenType.IDENTIFIER).value
            return f"{name}.{col}"
        return name

    def _parse_value(self):
        """Consume and return a single value -- number or string."""
        token = self.peek()
        if token.type in (TokenType.NUMBER, TokenType.STRING):
            self.pos += 1
            return token.value
        raise ParseError(f"Expected a value (number or string) but got {token.type!r}")

    def _parse_condition(self):
        """Parse a single condition: column op value (or column op column_ref)."""
        left = self._parse_qualified_or_plain()
        op_token = self.peek()
        if op_token.type in COMPARISON_OPS:
            token = self.consume(op_token.type)
            op = token.value if token.value is not None else token.type
        else:
            raise ParseError(f"Expected comparison operator, got {op_token.type}")
        # Right side: if it's an identifier, treat as column reference
        if self.peek().type == TokenType.IDENTIFIER:
            right = ColumnRef(self._parse_qualified_or_plain())
        else:
            right = self._parse_value()
        return (left, op, right)

    def _parse_where(self):
        """Parse optional WHERE clause with AND/OR support."""
        if self.peek().type != TokenType.WHERE:
            return None

        self.consume(TokenType.WHERE)
        result = self._parse_condition()

        while self.peek().type in (TokenType.AND, TokenType.OR):
            logic_op = self.consume(self.peek().type).type  # "AND" or "OR"
            right = self._parse_condition()
            result = {"op": logic_op, "left": result, "right": right}

        return result

    def _parse_select_column(self):
        """Parse a single select column — either identifier or aggregate(col)."""
        token = self.peek()
        if token.type == TokenType.IDENTIFIER and token.value.upper() in AGGREGATE_FUNCS:
            func_name = token.value.upper()
            self.consume(TokenType.IDENTIFIER)
            self.consume(TokenType.LPAREN)
            if self.peek().type == TokenType.STAR:
                self.consume(TokenType.STAR)
                arg = "*"
            else:
                arg = self._parse_qualified_or_plain()
            self.consume(TokenType.RPAREN)
            return {"func": func_name, "arg": arg}
        return self._parse_qualified_or_plain()

    def _parse_join_clause(self):
        """Parse a single JOIN clause: [INNER|LEFT] JOIN table ON condition."""
        join_type = "INNER"
        if self.peek().type == TokenType.INNER:
            self.consume(TokenType.INNER)
        elif self.peek().type == TokenType.LEFT:
            self.consume(TokenType.LEFT)
            join_type = "LEFT"
        self.consume(TokenType.JOIN)
        table_name = self.consume(TokenType.IDENTIFIER).value
        self.consume(TokenType.ON)
        on_condition = self._parse_condition()
        return {"type": join_type, "table": table_name, "on": on_condition}

    def parse_select(self):
        self.consume(TokenType.SELECT)
        columns = []
        if self.peek().type == TokenType.STAR:
            self.consume(TokenType.STAR)
            columns = ["*"]
        else:
            columns = self.parse_columns(self._parse_select_column)

        self.consume(TokenType.FROM)
        table_name = self.consume(TokenType.IDENTIFIER).value

        # JOIN clauses
        joins = []
        while self.peek().type in (TokenType.JOIN, TokenType.INNER, TokenType.LEFT):
            joins.append(self._parse_join_clause())

        where = self._parse_where()

        # GROUP BY
        group_by = None
        if self.peek().type == TokenType.GROUP:
            self.consume(TokenType.GROUP)
            self.consume(TokenType.BY)
            group_by = self._parse_qualified_or_plain()

        # ORDER BY
        order_by = None
        if self.peek().type == TokenType.ORDER:
            self.consume(TokenType.ORDER)
            self.consume(TokenType.BY)
            col = self._parse_qualified_or_plain()
            direction = "ASC"
            if self.peek().type in (TokenType.ASC, TokenType.DESC):
                direction = self.consume(self.peek().type).type
            order_by = {"column": col, "direction": direction}

        # LIMIT / OFFSET
        limit = None
        offset = None
        if self.peek().type == TokenType.LIMIT:
            self.consume(TokenType.LIMIT)
            limit = self.consume(TokenType.NUMBER).value
            if self.peek().type == TokenType.OFFSET:
                self.consume(TokenType.OFFSET)
                offset = self.consume(TokenType.NUMBER).value

        self.consume(TokenType.EOF)
        return {
            "action": "SELECT",
            "columns": columns,
            "table": table_name,
            "joins": joins or None,
            "where": where,
            "group_by": group_by,
            "order_by": order_by,
            "limit": limit,
            "offset": offset,
        }

    def parse_insert(self):
        self.consume(TokenType.INSERT)
        self.consume(TokenType.INTO)
        table_name = self.consume(TokenType.IDENTIFIER).value
        self.consume(TokenType.LPAREN)
        columns = self.parse_columns(self._parse_identifier)
        self.consume(TokenType.RPAREN)
        self.consume(TokenType.VALUES)
        self.consume(TokenType.LPAREN)
        values = self.parse_columns(self._parse_value)
        self.consume(TokenType.RPAREN)
        self.consume(TokenType.EOF)
        if len(columns) != len(values):
            raise ParseError(
                f"Column count ({len(columns)}) doesn't match value count ({len(values)})"
            )
        return {
            "action": "INSERT",
            "table": table_name,
            "columns": columns,
            "values": values,
        }

    def parse_delete(self):
        self.consume(TokenType.DELETE)
        self.consume(TokenType.FROM)
        table_name = self.consume(TokenType.IDENTIFIER).value
        where = self._parse_where()
        self.consume(TokenType.EOF)
        return {
            "action": "DELETE",
            "table": table_name,
            "where": where,
        }

    def parse_update(self):
        self.consume(TokenType.UPDATE)
        table_name = self.consume(TokenType.IDENTIFIER).value
        self.consume(TokenType.SET)

        # Parse assignments: col = val [, col = val]*
        assignments = []
        col = self._parse_identifier()
        self.consume(TokenType.EQ)
        val = self._parse_value()
        assignments.append((col, val))
        while self.peek().type == TokenType.COMMA:
            self.consume(TokenType.COMMA)
            col = self._parse_identifier()
            self.consume(TokenType.EQ)
            val = self._parse_value()
            assignments.append((col, val))

        where = self._parse_where()
        self.consume(TokenType.EOF)
        return {
            "action": "UPDATE",
            "table": table_name,
            "assignments": assignments,
            "where": where,
        }

    def _parse_column_def(self):
        """Parse a column definition: name TYPE or name TYPE(size)."""
        name = self._parse_identifier()
        token = self.peek()
        if token.type not in TYPE_TOKENS:
            raise ParseError(f"Expected column type, got {token.type}")
        type_name = TYPE_TOKEN_TO_NAME[token.type]
        self.consume(token.type)
        size = None
        if self.peek().type == TokenType.LPAREN:
            self.consume(TokenType.LPAREN)
            size = self.consume(TokenType.NUMBER).value
            self.consume(TokenType.RPAREN)
        col = {"name": name, "type": type_name}
        if size is not None:
            col["size"] = size
        return col

    def parse_create_table(self):
        self.consume(TokenType.CREATE)
        self.consume(TokenType.TABLE)
        table_name = self._parse_identifier()
        self.consume(TokenType.LPAREN)
        columns = self.parse_columns(self._parse_column_def)
        self.consume(TokenType.RPAREN)
        self.consume(TokenType.EOF)
        return {
            "action": "CREATE_TABLE",
            "table": table_name,
            "columns": columns,
        }

    def parse_drop_table(self):
        self.consume(TokenType.DROP)
        self.consume(TokenType.TABLE)
        table_name = self._parse_identifier()
        self.consume(TokenType.EOF)
        return {
            "action": "DROP_TABLE",
            "table": table_name,
        }

    def parse_show_tables(self):
        self.consume(TokenType.SHOW)
        self.consume(TokenType.TABLES)
        self.consume(TokenType.EOF)
        return {
            "action": "SHOW_TABLES",
        }

    def parse(self):
        """Entry point -- dispatches to the correct statement parser."""
        if self.peek().type == TokenType.SELECT:
            return self.parse_select()
        if self.peek().type == TokenType.INSERT:
            return self.parse_insert()
        if self.peek().type == TokenType.DELETE:
            return self.parse_delete()
        if self.peek().type == TokenType.UPDATE:
            return self.parse_update()
        if self.peek().type == TokenType.CREATE:
            return self.parse_create_table()
        if self.peek().type == TokenType.DROP:
            return self.parse_drop_table()
        if self.peek().type == TokenType.SHOW:
            return self.parse_show_tables()
        raise ParseError(f"Unexpected token: {self.peek().type}")


if __name__ == "__main__":
    from ast_sql.lexer import Lexer

    queries = [
        'SELECT * FROM users WHERE id = 3',
        'DELETE FROM users WHERE id = 5',
        "INSERT INTO table1 (a,b) VALUES (1,2)",
        'UPDATE users SET name = "Bob" WHERE id = 1',
    ]

    for q in queries:
        print(f"\nInput:  {q}")
        p = Parser(Lexer(q).tokenizer())
        print("AST:", p.parse())
