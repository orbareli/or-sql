"""
executor.py
-----------
Stage 3 of the pipeline.
Takes an AST dict (from parser.py) and calls the right Table methods.

This is the only file that knows about both the SQL layer and the storage layer.
"""
import re
from collections import defaultdict
from ast_sql.lexer import Lexer
from ast_sql.parser import Parser, ParseError, ColumnRef
from table import Table
from catalog import Catalog, CatalogError
from schema import Schema, Column, ColumnType
from query_cache import QueryCache

TYPE_NAME_TO_COL_TYPE = {
    "INTEGER": ColumnType.INTEGER,
    "TEXT": ColumnType.TEXT,
    "FLOAT": ColumnType.FLOAT,
    "BOOLEAN": ColumnType.BOOLEAN,
}


class Executor:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog
        self._tables = {}
        self.cache = QueryCache(max_size=100, ttl_seconds=60)

    def _get_table(self, table_name: str) -> Table:
        if table_name in self._tables:
            return self._tables[table_name]
        schema = self.catalog.get_schema(table_name)
        t = Table(self.catalog.db_path(table_name), schema)
        self._tables[table_name] = t
        return t

    def close(self):
        for t in self._tables.values():
            t.close()
        self._tables.clear()

    def run(self, sql: str, ai=None):
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
            error_msg = f"Parse error: {e}"
            if ai:
                explanation = ai.explain_error(sql, str(e))
                return f"{error_msg}\n\nAI: {explanation}"
            return error_msg
        except Exception as e:
            error_msg = f"Parse error: {e}"
            if ai:
                explanation = ai.explain_error(sql, str(e))
                return f"{error_msg}\n\nAI: {explanation}"
            return error_msg

        handlers = {
            "SELECT":       self._execute_select,
            "INSERT":       self._execute_insert,
            "DELETE":       self._execute_delete,
            "UPDATE":       self._execute_update,
            "CREATE_TABLE": self._execute_create_table,
            "DROP_TABLE":   self._execute_drop_table,
            "SHOW_TABLES":  self._execute_show_tables,
        }

        handler = handlers.get(ast["action"])
        if handler is None:
            return f"Unsupported statement: {ast['action']}"

        try:
            return handler(ast, sql)
        except CatalogError as e:
            return f"Error: {e}"
        except Exception as e:
            error_msg = f"Execution error: {e}"
            if ai:
                explanation = ai.explain_error(sql, str(e))
                return f"{error_msg}\n\nAI: {explanation}"
            return error_msg

    # ---------------------------------------------------------------- #
    #  WHERE evaluation                                                 #
    # ---------------------------------------------------------------- #

    def _resolve_column(self, record, col_name):
        """Resolve a column name against a record, supporting qualified names."""
        if col_name in record:
            return record[col_name]
        # Try suffix match for unqualified name against qualified keys
        matches = [k for k in record if k.endswith(f".{col_name}")]
        if len(matches) == 1:
            return record[matches[0]]
        if len(matches) > 1:
            raise ValueError(f"Ambiguous column '{col_name}' — matches: {matches}")
        return None

    def _has_column(self, record, col_name):
        """Check if a column exists in the record."""
        if col_name in record:
            return True
        matches = [k for k in record if k.endswith(f".{col_name}")]
        return len(matches) > 0

    def _eval_where(self, record, where):
        """Evaluate a WHERE clause against a record.

        where can be:
          - tuple: (column, op, value) -- simple comparison
          - dict:  {"op": "AND"/"OR", "left": ..., "right": ...} -- logical
        """
        if isinstance(where, tuple):
            column, op, value = where
            if not self._has_column(record, column):
                return False
            record_val = self._resolve_column(record, column)
            # If value is a ColumnRef, resolve it against the record
            if isinstance(value, ColumnRef):
                value = self._resolve_column(record, value.name)
            return self._compare(record_val, op, value)

        # dict -- AND/OR
        logic_op = where["op"]
        left = self._eval_where(record, where["left"])
        if logic_op == "AND":
            return left and self._eval_where(record, where["right"])
        else:  # OR
            return left or self._eval_where(record, where["right"])

    def _compare(self, record_val, op, value):
        if record_val is None or value is None:
            # NULL comparisons: only = and != work with NULL
            if op == "=":
                return record_val == value
            if op == "!=":
                return record_val != value
            return False
        ops = {
            "=":    lambda a, b: a == b,
            "<":    lambda a, b: a < b,
            ">":    lambda a, b: a > b,
            "<=":   lambda a, b: a <= b,
            ">=":   lambda a, b: a >= b,
            "!=":   lambda a, b: a != b,
            "LIKE": self._like_match,
        }
        compare = ops.get(op)
        if compare is None:
            raise ValueError(f"Unsupported operator: {op!r}")
        return compare(record_val, value)

    def _like_match(self, record_val, pattern):
        """Convert SQL LIKE pattern to regex and match."""
        regex = ""
        for ch in pattern:
            if ch == "%":
                regex += ".*"
            elif ch == "_":
                regex += "."
            else:
                regex += re.escape(ch)
        return re.fullmatch(regex, str(record_val)) is not None

    def _is_simple_id_eq(self, where):
        """Check if where is a simple `id = X` condition."""
        return isinstance(where, tuple) and where[0] == "id" and where[1] == "="

    def _filter_records(self, records, where):
        """Filter records using the WHERE clause."""
        return [r for r in records if self._eval_where(r, where)]

    # ---------------------------------------------------------------- #
    #  JOIN engine                                                      #
    # ---------------------------------------------------------------- #

    def _qualify_rows(self, rows, table_name):
        """Prefix all keys with table_name."""
        return [{f"{table_name}.{k}": v for k, v in row.items()} for row in rows]

    def _null_row(self, table_name):
        """All-None row for LEFT JOIN misses."""
        schema = self.catalog.get_schema(table_name)
        row = {f"{table_name}.id": None}
        for col in schema.columns:
            row[f"{table_name}.{col.name}"] = None
        return row

    def _nested_loop_join(self, left_rows, right_table_name, on_condition, join_type):
        """Execute a nested loop join."""
        right_table = self._get_table(right_table_name)
        right_rows = self._qualify_rows(right_table.select_all(), right_table_name)
        results = []
        for left_row in left_rows:
            matched = False
            for right_row in right_rows:
                merged = {**left_row, **right_row}
                if self._eval_where(merged, on_condition):
                    results.append(merged)
                    matched = True
            if not matched and join_type == "LEFT":
                results.append({**left_row, **self._null_row(right_table_name)})
        return results

    def _simplify_row_keys(self, rows):
        """Drop table prefix on unambiguous column names."""
        if not rows:
            return rows
        # Collect all keys
        all_keys = list(rows[0].keys())
        # Find bare names and check for conflicts
        bare_names = {}
        for key in all_keys:
            if "." in key:
                bare = key.split(".", 1)[1]
            else:
                bare = key
            bare_names.setdefault(bare, []).append(key)

        # Build rename map
        rename = {}
        for bare, keys in bare_names.items():
            if len(keys) == 1:
                rename[keys[0]] = bare
            else:
                for k in keys:
                    rename[k] = k  # keep qualified

        return [{rename.get(k, k): v for k, v in row.items()} for row in rows]

    # ---------------------------------------------------------------- #
    #  SELECT                                                           #
    # ---------------------------------------------------------------- #

    def _execute_select(self, ast: dict, sql: str):
        cache_key = sql.strip().lower()
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached

        joins = ast.get("joins")
        table = self._get_table(ast["table"])
        where = ast["where"]
        columns = ast["columns"]

        if joins:
            # JOIN path: qualify base rows, execute joins, then filter
            results = self._qualify_rows(table.select_all(), ast["table"])
            for join in joins:
                results = self._nested_loop_join(
                    results, join["table"], join["on"], join["type"]
                )
            # WHERE after join
            if where:
                results = self._filter_records(results, where)

            # GROUP BY + Aggregates
            group_by = ast.get("group_by")
            has_aggregates = any(isinstance(c, dict) for c in columns)
            if group_by:
                results = self._apply_group_by(results, columns, group_by)
            elif has_aggregates:
                results = self._apply_aggregates(results, columns)
            else:
                results = self._apply_columns(results, columns)

            # ORDER BY
            order_by = ast.get("order_by")
            if order_by:
                col = order_by["column"]
                reverse = order_by["direction"] == "DESC"
                results = sorted(
                    results,
                    key=lambda r: (r.get(col, 0) is None, r.get(col, 0)),
                    reverse=reverse,
                )

            # OFFSET / LIMIT
            offset = ast.get("offset")
            if offset:
                results = results[offset:]
            limit = ast.get("limit")
            if limit is not None:
                results = results[:limit]

            # Simplify keys for output
            results = self._simplify_row_keys(results)
        else:
            # Non-join path (original logic)
            if where is None:
                results = table.select_all()
            elif self._is_simple_id_eq(where):
                results = table.select_by_id(where[2])
                if results is None:
                    return "No record found."
            else:
                results = self._filter_records(table.select_all(), where)

            # GROUP BY + Aggregates
            group_by = ast.get("group_by")
            has_aggregates = any(isinstance(c, dict) for c in columns)

            if group_by:
                results = self._apply_group_by(results, columns, group_by)
            elif has_aggregates:
                results = self._apply_aggregates(results, columns)
            else:
                results = self._apply_columns(results, columns)

            # ORDER BY
            order_by = ast.get("order_by")
            if order_by:
                col = order_by["column"]
                reverse = order_by["direction"] == "DESC"
                results = sorted(results, key=lambda r: r.get(col, 0), reverse=reverse)

            # OFFSET
            offset = ast.get("offset")
            if offset:
                results = results[offset:]

            # LIMIT
            limit = ast.get("limit")
            if limit is not None:
                results = results[:limit]

        self.cache.set(sql, results)
        return results

    # ---------------------------------------------------------------- #
    #  INSERT                                                           #
    # ---------------------------------------------------------------- #

    def _execute_insert(self, ast: dict, sql: str):
        table = self._get_table(ast["table"])
        values = ast["values"]
        columns = ast["columns"]

        ok, msg = table.schema.validate_values(columns, values)
        if not ok:
            return f"Error: {msg}"

        data = dict(zip(columns, values))
        record_id = table.insert(data)
        self.cache.invalidate()
        return f"Inserted record with id={record_id}."

    # ---------------------------------------------------------------- #
    #  DELETE                                                           #
    # ---------------------------------------------------------------- #

    def _execute_delete(self, ast: dict, sql: str):
        table = self._get_table(ast["table"])
        where = ast["where"]

        if where is None:
            all_records = table.select_all()
            return self._delete_records(table, all_records)

        # Fast path: DELETE WHERE id = X
        if self._is_simple_id_eq(where):
            value = where[2]
            existing = table.select_by_id(value)
            if existing is None:
                return f"No record with id={value}."
            table.delete(value)
            self.cache.invalidate()
            return f"Deleted record with id={value}."

        # Slow path: full scan + filter
        all_records = table.select_all()
        filtered = self._filter_records(all_records, where)
        if not filtered:
            return "No matching records found."
        self.cache.invalidate()
        return self._delete_records(table, filtered)

    def _delete_records(self, table, records):
        ids = [r["id"] for r in records]
        table.delete_many(ids)
        return f"Deleted {len(records)} record(s)."

    # ---------------------------------------------------------------- #
    #  UPDATE                                                           #
    # ---------------------------------------------------------------- #

    def _execute_update(self, ast: dict, sql: str):
        table = self._get_table(ast["table"])
        where = ast["where"]
        assignments = ast["assignments"]

        if where is None:
            records = table.select_all()
        elif self._is_simple_id_eq(where):
            records = table.select_by_id(where[2])
            if records is None:
                return "No record found."
        else:
            records = self._filter_records(table.select_all(), where)

        if not records:
            return "No matching records found."

        count = 0
        for record in records:
            # Build new values dict from existing record, applying assignments
            new_values = {col.name: record[col.name] for col in table.schema.columns}
            for col, val in assignments:
                new_values[col] = val
            table.update(record["id"], new_values)
            count += 1

        self.cache.invalidate()
        return f"Updated {count} record(s)."

    # ---------------------------------------------------------------- #
    #  DDL: CREATE TABLE, DROP TABLE, SHOW TABLES                       #
    # ---------------------------------------------------------------- #

    def _execute_create_table(self, ast: dict, sql: str):
        columns = []
        for col_def in ast["columns"]:
            col_type = TYPE_NAME_TO_COL_TYPE.get(col_def["type"])
            if col_type is None:
                return f"Error: Unknown type '{col_def['type']}'"
            columns.append(Column(col_def["name"], col_type, col_def.get("size")))
        schema = Schema(ast["table"], columns)
        self.catalog.create_table(schema)
        return f"Table '{ast['table']}' created."

    def _execute_drop_table(self, ast: dict, sql: str):
        table_name = ast["table"]
        # Close the table if it's open
        if table_name in self._tables:
            self._tables[table_name].close()
            del self._tables[table_name]
        self.catalog.drop_table(table_name)
        self.cache.invalidate()
        return f"Table '{table_name}' dropped."

    def _execute_show_tables(self, ast: dict, sql: str):
        tables = self.catalog.list_tables()
        if not tables:
            return "No tables."
        return [{"table_name": t} for t in tables]

    # ---------------------------------------------------------------- #
    #  Column projection                                                #
    # ---------------------------------------------------------------- #

    def _apply_columns(self, records: list, columns: list) -> list:
        if columns == ["*"]:
            return records
        filtered = []
        for record in records:
            filtered_record = {}
            for col in columns:
                if self._has_column(record, col):
                    filtered_record[col] = self._resolve_column(record, col)
                else:
                    filtered_record[col] = f"<unknown column: {col}>"
            filtered.append(filtered_record)
        return filtered

    # ---------------------------------------------------------------- #
    #  Aggregates                                                       #
    # ---------------------------------------------------------------- #

    def _apply_aggregates(self, records: list, columns: list) -> list:
        """Compute aggregate functions over the result set. Returns single row."""
        row = {}
        for col in columns:
            if isinstance(col, dict):
                key = f"{col['func']}({col['arg']})"
                row[key] = self._compute_aggregate(col["func"], col["arg"], records)
            else:
                if records:
                    row[col] = records[0].get(col)
                else:
                    row[col] = None
        return [row]

    def _compute_aggregate(self, func, arg, records):
        if func == "COUNT":
            if arg == "*":
                return len(records)
            return sum(1 for r in records if self._resolve_column(r, arg) is not None)
        values = [self._resolve_column(r, arg) for r in records
                  if self._has_column(r, arg) and self._resolve_column(r, arg) is not None]
        if not values:
            return 0 if func in ("COUNT", "SUM") else None
        if func == "SUM":
            return sum(values)
        if func == "AVG":
            return sum(values) / len(values)
        if func == "MIN":
            return min(values)
        if func == "MAX":
            return max(values)
        raise ValueError(f"Unknown aggregate: {func}")

    # ---------------------------------------------------------------- #
    #  GROUP BY                                                         #
    # ---------------------------------------------------------------- #

    def _apply_group_by(self, records: list, columns: list, group_key: str) -> list:
        buckets = defaultdict(list)
        for r in records:
            buckets[self._resolve_column(r, group_key)].append(r)

        results = []
        for key_val, group_records in buckets.items():
            row = {}
            for col in columns:
                if isinstance(col, dict):
                    col_key = f"{col['func']}({col['arg']})"
                    row[col_key] = self._compute_aggregate(col["func"], col["arg"], group_records)
                elif col == group_key:
                    row[col] = key_val
                else:
                    if group_records:
                        row[col] = self._resolve_column(group_records[0], col)
                    else:
                        row[col] = None
            results.append(row)
        return results
