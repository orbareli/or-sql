"""
schema.py
---------
Dynamic schema system for multi-table support.

Each table has a Schema that defines its columns and types.
The schema handles struct packing/unpacking, validation, and serialization.

The 'id' column is always implicit — a 4-byte unsigned int auto-increment
prepended to every record.
"""
from __future__ import annotations

import struct
from typing import Optional


class ColumnType:
    INTEGER = "INTEGER"    # struct "I" (4 bytes)
    TEXT = "TEXT"           # struct "<n>s" (n bytes, null-padded)
    FLOAT = "FLOAT"        # struct "d" (8 bytes)
    BOOLEAN = "BOOLEAN"    # struct "?" (1 byte)


class Column:
    def __init__(self, name: str, col_type: str, size: Optional[int] = None):
        self.name = name
        self.col_type = col_type
        self.size = size
        if col_type == ColumnType.TEXT and size is None:
            self.size = 255

    def struct_code(self) -> str:
        if self.col_type == ColumnType.INTEGER:
            return "I"
        if self.col_type == ColumnType.TEXT:
            return f"{self.size}s"
        if self.col_type == ColumnType.FLOAT:
            return "d"
        if self.col_type == ColumnType.BOOLEAN:
            return "?"
        raise ValueError(f"Unknown column type: {self.col_type}")

    def byte_size(self) -> int:
        return struct.calcsize(self.struct_code())

    def to_dict(self) -> dict:
        d = {"name": self.name, "type": self.col_type}
        if self.col_type == ColumnType.TEXT:
            d["size"] = self.size
        return d

    @staticmethod
    def from_dict(d: dict) -> "Column":
        return Column(d["name"], d["type"], d.get("size"))


class Schema:
    def __init__(self, table_name: str, columns: list[Column]):
        self.table_name = table_name
        self.columns = columns
        self._format = "<I" + "".join(c.struct_code() for c in columns)
        self._record_size = struct.calcsize(self._format)

    @property
    def record_size(self) -> int:
        return self._record_size

    @property
    def format_string(self) -> str:
        return self._format

    def column_names(self) -> list[str]:
        return ["id"] + [c.name for c in self.columns]

    def pack(self, record_id: int, values: dict) -> bytes:
        args = [record_id]
        for col in self.columns:
            val = values[col.name]
            if col.col_type == ColumnType.TEXT:
                if isinstance(val, str):
                    val = val.encode("utf-8")[:col.size].ljust(col.size, b'\x00')
                args.append(val)
            elif col.col_type == ColumnType.BOOLEAN:
                args.append(bool(val))
            elif col.col_type == ColumnType.FLOAT:
                args.append(float(val))
            else:
                args.append(int(val))
        return struct.pack(self._format, *args)

    def unpack(self, data: bytes) -> dict:
        values = struct.unpack(self._format, data)
        result = {"id": values[0]}
        for i, col in enumerate(self.columns):
            val = values[i + 1]
            if col.col_type == ColumnType.TEXT:
                val = val.decode("utf-8").strip('\x00')
            result[col.name] = val
        return result

    def validate_values(self, col_names: list[str], values: list) -> tuple[bool, str]:
        schema_col_names = [c.name for c in self.columns]
        for name in col_names:
            if name not in schema_col_names:
                return False, f"Unknown column: '{name}'"
        data = dict(zip(col_names, values))
        for col in self.columns:
            if col.name not in data:
                return False, f"Missing required column: '{col.name}'"
            val = data[col.name]
            if col.col_type == ColumnType.INTEGER:
                if not isinstance(val, int):
                    return False, f"Column '{col.name}' expects INTEGER, got {type(val).__name__}"
                if val < 0 or val > 4294967295:
                    return False, f"Column '{col.name}' value out of range (0 to 4,294,967,295)"
            elif col.col_type == ColumnType.TEXT:
                if not isinstance(val, str):
                    return False, f"Column '{col.name}' expects TEXT, got {type(val).__name__}"
            elif col.col_type == ColumnType.FLOAT:
                if not isinstance(val, (int, float)):
                    return False, f"Column '{col.name}' expects FLOAT, got {type(val).__name__}"
            elif col.col_type == ColumnType.BOOLEAN:
                if not isinstance(val, (bool, int)):
                    return False, f"Column '{col.name}' expects BOOLEAN, got {type(val).__name__}"
        return True, ""

    def to_dict(self) -> dict:
        return {
            "table_name": self.table_name,
            "columns": [c.to_dict() for c in self.columns],
        }

    @staticmethod
    def from_dict(d: dict) -> "Schema":
        columns = [Column.from_dict(c) for c in d["columns"]]
        return Schema(d["table_name"], columns)
