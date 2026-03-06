"""
lexer.py
--------
Stage 1 of the parser pipeline.
Turns a raw SQL string into a flat list of Token objects.

The lexer has no idea what the tokens MEAN — it just labels them.
That's the parser's job.
"""


class TokenType:
    # Keywords
    SELECT = "SELECT"
    INSERT = "INSERT"
    DELETE = "DELETE"
    INTO   = "INTO"
    FROM   = "FROM"
    WHERE  = "WHERE"
    VALUES = "VALUES"

    # Literals
    NUMBER     = "NUMBER"      # 42
    STRING     = "STRING"      # "Alice"
    IDENTIFIER = "IDENTIFIER"  # table/column name

    # Symbols
    STAR   = "STAR"    # *
    EQ     = "EQ"      # =
    LT     = "LT"      # 
    GT     = "GT"      # >
    COMMA  = "COMMA"   # ,
    LPAREN = "LPAREN"  # (
    RPAREN = "RPAREN"  # )

    # Control
    EOF = "EOF"  # end of input


# ------------------------------------------------------------------ #
#  Token                                                               #
# ------------------------------------------------------------------ #
KEYWORDS = {
    "SELECT": TokenType.SELECT,
    "INSERT": TokenType.INSERT,
    "FROM":   TokenType.FROM,
    "WHERE":  TokenType.WHERE,
    "INTO":   TokenType.INTO,
    "VALUES": TokenType.VALUES,
    "DELETE": TokenType.DELETE,
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
        self.text = text        # the full input string
        self.pos  = 0           # current position in the string
    def _advance(self):
        """Move one character forward."""
        self.pos += 1
    def tokenizer(self):
        tokens = []
        while self.pos < len(self.text):
            char = self.text[self.pos]

            # 1. דילוג על רווחים
            if char.isspace():
                self._advance()
                continue

            # 2. מספרים - שים לב לשיטת ה-"חיתוך" (Slicing)
            if char.isdigit():
                start = self.pos
                while self.pos < len(self.text) and self.text[self.pos].isdigit():
                    self._advance()
                value = int(self.text[start:self.pos]) # המרה למספר
                tokens.append(Token(TokenType.NUMBER, value))
                continue
            # 3. מילים (SELECT, users, וכו')
            if char.isalpha():

                start = self.pos
                while self.pos < len(self.text) and (self.text[self.pos].isalnum() or self.text[self.pos] == '_'):
                    self._advance()
                value = self.text[start:self.pos] 
                token_type = KEYWORDS.get(value.upper(), TokenType.IDENTIFIER)
                if token_type == TokenType.IDENTIFIER:
                    tokens.append(Token(token_type, value))
                else:

                    tokens.append(Token(token_type))
                continue # חוזרים לראש הלולאה בלי לקדם את pos שוב
           #4 מרכאות
            if char == '"' or char == "'":
                quote_type = char # שומרים אם זה " או '

                self._advance()
                start = self.pos
                while self.pos < len(self.text) and self.text[self.pos] != quote_type:
                    self._advance()
                value = self.text[start:self.pos] 
                self._advance()
                token_type = KEYWORDS.get(value.upper(), TokenType.IDENTIFIER)
                tokens.append(Token(TokenType.STRING, value))
                continue # חוזרים לראש הלולאה בלי לקדם את pos שוב
            # 5. סימנים
            elif char == "*":
                tokens.append(Token(TokenType.STAR))
                self._advance()

            elif char == "=":
                tokens.append(Token(TokenType.EQ, "="))
                self._advance()

            elif char == "<":
                tokens.append(Token(TokenType.LT, "<"))
                self._advance()

            elif char == ">":
                tokens.append(Token(TokenType.GT, ">"))
                self._advance()

            elif char == ",":
                tokens.append(Token(TokenType.COMMA))
                self._advance()

            elif char == "(":
                tokens.append(Token(TokenType.LPAREN))
                self._advance()

            elif char == ")":
                tokens.append(Token(TokenType.RPAREN))
                self._advance()
# בסוף הכל, מוסיפים טוקן EOF (End Of File)
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
