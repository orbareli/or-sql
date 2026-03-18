"""
lexer.py
--------
Stage 1 of the parser pipeline.
Turns a raw SQL string into a flat list of Token objects.

The lexer has no idea what the tokens MEAN -- it just labels them.
That's the parser's job.
"""


class TokenType:
    # Keywords
    SELECT = "SELECT"
    INSERT = "INSERT"
    DELETE = "DELETE"
    UPDATE = "UPDATE"
    SET    = "SET"
    INTO   = "INTO"
    FROM   = "FROM"
    WHERE  = "WHERE"
    VALUES = "VALUES"
    AND    = "AND"
    OR     = "OR"
    ORDER  = "ORDER"
    BY     = "BY"
    ASC    = "ASC"
    DESC   = "DESC"
    LIMIT  = "LIMIT"
    OFFSET = "OFFSET"
    GROUP  = "GROUP"
    LIKE   = "LIKE"

    # JOIN Keywords
    JOIN   = "JOIN"
    INNER  = "INNER"
    LEFT   = "LEFT"
    ON     = "ON"

    # DDL Keywords
    CREATE = "CREATE"
    DROP   = "DROP"
    TABLE  = "TABLE"
    SHOW   = "SHOW"
    TABLES = "TABLES"

    # Type Keywords
    INTEGER_TYPE = "INTEGER_TYPE"
    TEXT_TYPE    = "TEXT_TYPE"
    FLOAT_TYPE   = "FLOAT_TYPE"
    BOOLEAN_TYPE = "BOOLEAN_TYPE"

    # Literals
    NUMBER     = "NUMBER"      # 42
    STRING     = "STRING"      # "Alice"
    IDENTIFIER = "IDENTIFIER"  # table/column name

    # Symbols
    DOT    = "DOT"     # .
    STAR   = "STAR"    # *
    EQ     = "EQ"      # =
    LT     = "LT"      # <
    GT     = "GT"      # >
    LTE    = "LTE"     # <=
    GTE    = "GTE"     # >=
    NEQ    = "NEQ"     # !=
    COMMA  = "COMMA"   # ,
    LPAREN = "LPAREN"  # (
    RPAREN = "RPAREN"  # )

    # Control
    EOF = "EOF"  # end of input


KEYWORDS = {
    "SELECT": TokenType.SELECT,
    "INSERT": TokenType.INSERT,
    "DELETE": TokenType.DELETE,
    "UPDATE": TokenType.UPDATE,
    "SET":    TokenType.SET,
    "FROM":   TokenType.FROM,
    "WHERE":  TokenType.WHERE,
    "INTO":   TokenType.INTO,
    "VALUES": TokenType.VALUES,
    "AND":    TokenType.AND,
    "OR":     TokenType.OR,
    "ORDER":  TokenType.ORDER,
    "BY":     TokenType.BY,
    "ASC":    TokenType.ASC,
    "DESC":   TokenType.DESC,
    "LIMIT":  TokenType.LIMIT,
    "OFFSET": TokenType.OFFSET,
    "GROUP":  TokenType.GROUP,
    "LIKE":    TokenType.LIKE,
    "JOIN":    TokenType.JOIN,
    "INNER":   TokenType.INNER,
    "LEFT":    TokenType.LEFT,
    "ON":      TokenType.ON,
    "CREATE":  TokenType.CREATE,
    "DROP":    TokenType.DROP,
    "TABLE":   TokenType.TABLE,
    "SHOW":    TokenType.SHOW,
    "TABLES":  TokenType.TABLES,
    "INTEGER": TokenType.INTEGER_TYPE,
    "TEXT":    TokenType.TEXT_TYPE,
    "FLOAT":   TokenType.FLOAT_TYPE,
    "BOOLEAN": TokenType.BOOLEAN_TYPE,
}


class Token:
    def __init__(self, type_: str, value=None):
        self.type  = type_
        self.value = value

    def __repr__(self):
        if self.value is not None:
            return f"Token({self.type}, {self.value!r})"
        return f"Token({self.type})"


class Lexer:
    def __init__(self, text: str):
        self.text = text
        self.pos  = 0

    def _advance(self):
        """Move one character forward."""
        self.pos += 1

    def tokenizer(self):
        tokens = []
        while self.pos < len(self.text):
            char = self.text[self.pos]

            # Skip whitespace
            if char.isspace():
                self._advance()
                continue

            # Numbers
            if char.isdigit():
                start = self.pos
                while self.pos < len(self.text) and self.text[self.pos].isdigit():
                    self._advance()
                value = int(self.text[start:self.pos])
                tokens.append(Token(TokenType.NUMBER, value))
                continue

            # Words (keywords and identifiers)
            if char.isalpha() or char == '_':
                start = self.pos
                while self.pos < len(self.text) and (self.text[self.pos].isalnum() or self.text[self.pos] == '_'):
                    self._advance()
                value = self.text[start:self.pos]
                token_type = KEYWORDS.get(value.upper(), TokenType.IDENTIFIER)
                if token_type == TokenType.IDENTIFIER:
                    tokens.append(Token(token_type, value))
                else:
                    tokens.append(Token(token_type))
                continue

            # Quoted strings
            if char == '"' or char == "'":
                quote_type = char
                self._advance()
                start = self.pos
                while self.pos < len(self.text) and self.text[self.pos] != quote_type:
                    self._advance()
                value = self.text[start:self.pos]
                self._advance()
                tokens.append(Token(TokenType.STRING, value))
                continue

            # Symbols
            if char == ".":
                tokens.append(Token(TokenType.DOT))
                self._advance()
                continue

            if char == "*":
                tokens.append(Token(TokenType.STAR))
                self._advance()
            elif char == "=":
                tokens.append(Token(TokenType.EQ, "="))
                self._advance()
            elif char == "<":
                self._advance()
                if self.pos < len(self.text) and self.text[self.pos] == "=":
                    self._advance()
                    tokens.append(Token(TokenType.LTE, "<="))
                else:
                    tokens.append(Token(TokenType.LT, "<"))
            elif char == ">":
                self._advance()
                if self.pos < len(self.text) and self.text[self.pos] == "=":
                    self._advance()
                    tokens.append(Token(TokenType.GTE, ">="))
                else:
                    tokens.append(Token(TokenType.GT, ">"))
            elif char == "!":
                self._advance()
                if self.pos < len(self.text) and self.text[self.pos] == "=":
                    self._advance()
                    tokens.append(Token(TokenType.NEQ, "!="))
                else:
                    raise ValueError(f"Expected '=' after '!'")
            elif char == ",":
                tokens.append(Token(TokenType.COMMA))
                self._advance()
            elif char == "(":
                tokens.append(Token(TokenType.LPAREN))
                self._advance()
            elif char == ")":
                tokens.append(Token(TokenType.RPAREN))
                self._advance()
            else:
                raise ValueError(f"Unexpected character: {char!r}")

        tokens.append(Token(TokenType.EOF))
        return tokens


if __name__ == "__main__":
    queries = [
        'SELECT * FROM users',
        'SELECT * FROM users WHERE id = 3',
        'INSERT INTO users (name, age) VALUES ("Alice", 30)',
        'DELETE FROM users WHERE id = 5',
    ]

    for q in queries:
        print(f"\nInput:  {q}")
        print("Tokens:", Lexer(q).tokenizer())
