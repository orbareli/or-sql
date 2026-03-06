from ast_sql.lexer import TokenType, Lexer
from table import Table
class Parser:
    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0

    def peek(self):
        # מחזיר את הטוקן הנוכחי בלי לקדם
        return self.tokens[self.pos]

    def consume(self, expected_type):
        token = self.peek()
        if token.type == expected_type:
            self.pos += 1
            return token
        raise Exception(f"Expected {expected_type}, got {token.type}")


    def parse_columns(self, element_parser_fn):
        results = []
        results.append(element_parser_fn()) # קורא לפונקציה שמנתחת איבר בודד
        #results.append(self.consume(TokenType.IDENTIFIER).value)
            
            # לולאה לבדיקה אם יש עוד עמודות אחרי פסיק
        """while self.peek().type == TokenType.COMMA:
            if action == "INSERT":
                results.append(self._parse_value())
            elif action == "SELECT":

                self.consume(TokenType.COMMA)
                results.append(self.consume(TokenType.IDENTIFIER).value)"""
        while self.peek().type == TokenType.COMMA:
            self.consume(TokenType.COMMA)
            results.append(element_parser_fn())
        return results
    def _parse_identifier(self):
        """עוזרת קטנה לשמות עמודות/טבלאות"""
        return self.consume(TokenType.IDENTIFIER).value
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
            "values": values
        }
    def _parse_where(self):
        if self.peek().type == TokenType.WHERE:
            self.consume(TokenType.WHERE)
            left = self._parse_identifier()
            op_token = self.peek()
            if op_token.type in (TokenType.EQ, TokenType.LT, TokenType.GT):
                op = self.consume(op_token.type).value # בלקסר הגדרנו value לסימנים האלו
            else:
                raise ParseError(f"Expected comparison operator, got {op_token.type}")
            right = self._parse_value()
        
            return (left, op, right)
        return None
    def parse_delete(self):
        self.consume(TokenType.DELETE)
        self.consume(TokenType.FROM)
        table_name = self.consume(TokenType.IDENTIFIER).value
        where = self._parse_where()
        self.consume(TokenType.EOF)
        return {
            "action": "DELETE",
            "table": table_name,
            "where": where
        }


    def parse_select(self):
        self.consume(TokenType.SELECT)
        # 2. עכשיו אנחנו מחפשים מה לבחור (עמודות או *)
        columns = []
        if self.peek().type == TokenType.STAR:
            self.consume(TokenType.STAR)
            columns = ["*"]  # נסמן שרוצים את הכל
        else:
            # אם זו לא כוכבית, זו כנראה רשימת עמודות (למשל: name, age)
            columns = self.parse_columns(self._parse_identifier)
        self.consume(TokenType.FROM)
        table_name = self.consume(TokenType.IDENTIFIER).value
        # 5. מחזירים את ה"תוצאה" של הניתוח
        where = self._parse_where()
        self.consume(TokenType.EOF)
        return {
            "action": "SELECT",
            "columns": columns,
            "table": table_name,
            "where": where
        }
    def _parse_value(self):
        """Consume and return a single value — number or string."""
        token = self.peek()
        if token.type in (TokenType.NUMBER, TokenType.STRING):
            self.pos += 1 # מקדם ידנית במקום consume כי אנחנו כבר יודעים את הסוג
            return token.value
        raise ParseError(f"Expected a value (number or string) but got {token.type!r}")
            

    def parse(self):
        # נקודת ההתחלה של הכל
        if self.peek().type == TokenType.SELECT:
            return self.parse_select()
        if self.peek().type == TokenType.INSERT:
            return self.parse_insert()
        if self.peek().type == TokenType.DELETE:
            return self.parse_delete()
class ParseError(Exception):
    pass
        
        # כאן נוסיף בעתיד parse_insert וכו'

"""
executor.py
-----------
Stage 3 of the pipeline.
Takes an AST dict (from parser.py) and calls the right Table methods.

This is the only file that knows about both the SQL layer and the storage layer.
"""
class Executor:
    def __init__(self, table: Table):
        self.table = table
    # ---------------------------------------------------------------- #
    #  Entry point                                                       #
    # ---------------------------------------------------------------- #

    def run(self, sql: str):
        """
        The only method the CLI calls.
        
        Takes a raw SQL string, parses it, executes it, returns a result string.
        All errors are caught here so the CLI never crashes.
        """
        try:
            lexer = Lexer(sql)
            tokens = lexer.tokenizer()
            ast = Parser(tokens).parse()
        except ParseError as e:
            return f"Parse error: {e}"
        except Exception as e:
            return f"Unexpected error during parsing: {e}"

        handlers = {
            "SELECT": self._execute_select,
            "INSERT": self._execute_insert,
            "DELETE": self._execute_delete,
        }

        handler = handlers.get(ast["action"])
        if handler is None:
            return f"Unsupported statement: {ast['action']}"

        try:
            return handler(ast)
        except Exception as e:
            return f"Execution error: {e}"
    def _execute_delete(self, ast):
        print(f"DEBUG ast: {ast}")          # בדוק שה-AST נכון
        
        where = ast["where"]
        print(f"DEBUG where: {where}")      # בדוק שה-WHERE נכון
        
        all_records = self.table.select_all()
        # Case 1 — no WHERE, return everything
        if where is None:
            results = self._delete_records(all_records)
            return results
        column, op, value = where
        print(f"DEBUG column={column}, op={op}, value={value}, type={type(value)}")
        # מסלול מהיר — DELETE WHERE id = X
        if column == "id" and op == "=":
            existing = self.table.select_by_id(value)
            if existing is None:
                return f"No record with id={value}."
            try:
                result = self.table.delete(value)
                print(f"DEBUG delete result: {result}")
            except Exception as e:
                print(f"DEBUG delete crashed: {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()   # prints the full crash with line numbers
            return f"Deleted record with id={value}."

        # מסלול איטי — DELETE WHERE other_col op val
        all_records = self.table.select_all()
        filtered = self._apply_filter(op, all_records, column, value)
        if not filtered:
            return "No matching records found."
        return self._delete_records(filtered)
    def _delete_records(self, records):
        for r in records:
            self.table.delete(r["id"])  
        return f"Deleted {len(records)} record(s)."
    def _execute_insert(self,ast):
        values =ast["values"]
        columns =ast["columns"]
        data = dict(zip(columns, values))
        if "name" not in data:
            return "Error: INSERT requires a 'name' field."
        if "age" not in data:
            return "Error: INSERT requires a 'age' field."
        name = data["name"]
        age = data["age"]

        # בדיקה 1: סוג הנתון
        if not isinstance(age, int):
            return "Error: 'age' must be an integer."

        # בדיקה 2: טווח הגיוני (גם גיל שלילי זה לא תקין)
        if age < 0 or age > 4294967295:
            return "Error: 'age' is out of range (0 to 4,294,967,295)."
        id = self.table.insert(name, age)
        return f"Inserted record with id={id}."
    def _apply_columns(self, records: list, columns: list) -> list:
        """
        מסנן את השדות שהמשתמש ביקש.

        SELECT * → מחזיר הכל
        SELECT name, age → מחזיר רק את השדות האלה

        לפני: [{"id":1, "name":"Alice", "age":30}]
        אחרי SELECT name: [{"name":"Alice"}]
        """
        # אם ביקשו * — מחזירים הכל בלי שינוי
        if columns == ["*"]:
            return records

        # אחרת — בונים dict חדש עם רק העמודות המבוקשות
        filtered = []
        for record in records:
            filtered_record = {}
            for col in columns:
                if col in record:
                    filtered_record[col] = record[col]
                else:
                    # העמודה לא קיימת ברשומה
                    filtered_record[col] = f"<unknown column: {col}>"
            filtered.append(filtered_record)
        print ("fil", filtered)
        return filtered
    def _execute_select(self, ast: dict) -> str:
        """
        Three cases:
          1. No WHERE clause      → full table scan
          2. WHERE id = X         → fast B+ Tree lookup O(log n)
          3. WHERE other_col op X → full scan + filter in Python
        """
        where = ast["where"]  # either None or a tuple (column, op, value)

        # Case 1 — no WHERE, return everything
        if where is None:
            results = self.table.select_all()
            return self._format(results)
        column, op, value = where
        if column == "id" and op == "=":
            columns = []
            columns.append(column)
            results = self.table.select_by_id(value)
            if results is None:
                return "No record found."
            filresults = self._apply_columns(results, columns)
            print("sadasdada:   ", filresults)
            return self._format(filresults)
        all_records = self.table.select_all()
        filtered = self._apply_filter(op, all_records, column, value)
        print("DEBUG - Filtered records:", results)
            # שלב 2 — סנן עמודות
        filtered_results = self._apply_columns(filtered, column)
        return self._format(filtered_results)
    def _format(self, records: list) -> str:
        """
        Turn a list of record dicts into a human-readable string.
        
        Output example:
          {'id': 1, 'name': 'Alice', 'age': 30}
          {'id': 2, 'name': 'Bob', 'age': 28}
          (2 rows)
        """
        if not records:
            return "No records found."

        lines = [str(r) for r in records]
        count = len(records)
        lines.append(f"({count} row{'s' if count != 1 else ''})")
        return "\\n".join(lines)
    """def _format(self, records: list) -> str:

        return records"""
    def _apply_filter(self,op, records:list, column:str, value):
        """
        Pure Python filtering for columns without an index.
        
        Example:
          records = [{"id":1,"name":"Alice","age":30}, {"id":2,"name":"Bob","age":22}]
          column="age", op=">", value=25
          → returns only Alice's record
        
        We use a dict of lambdas instead of if/elif chains
        so adding a new operator later is just one line.
        """
        ops = {
            "=": lambda a, b: a == b,
            "<": lambda a, b: a <  b,
            ">": lambda a, b: a >  b,
        }

        compare = ops.get(op)
        if compare is None:
            raise ValueError(f"Unsupported operator: {op!r}")
        return [r for r in records if column in r and compare(r[column], value)]

        # Keep only records where the column value passes the comparison





if __name__ == "__main__":
    queries = [
        'SELECT * FROM users where id = "hello',
        'delete FROM users where id = "hello', 
        "INSERT INTO table1 (a,b) VALUES (1,2)"
    ]
    

    for q in queries:
        print(f"\nInput:  {q}")
        p = Parser(Lexer(q).tokenizer())
        print("Tokens:", p.parse())