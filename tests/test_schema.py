"""Tests for the Schema system."""
import struct
from schema import Schema, Column, ColumnType


def make_users_schema():
    """Create the classic users schema: (name TEXT(20), age INTEGER)."""
    return Schema("users", [
        Column("name", ColumnType.TEXT, 20),
        Column("age", ColumnType.INTEGER),
    ])


class TestColumn:
    def test_integer_struct_code(self):
        col = Column("age", ColumnType.INTEGER)
        assert col.struct_code() == "I"
        assert col.byte_size() == 4

    def test_text_struct_code(self):
        col = Column("name", ColumnType.TEXT, 20)
        assert col.struct_code() == "20s"
        assert col.byte_size() == 20

    def test_float_struct_code(self):
        col = Column("score", ColumnType.FLOAT)
        assert col.struct_code() == "d"
        assert col.byte_size() == 8

    def test_boolean_struct_code(self):
        col = Column("active", ColumnType.BOOLEAN)
        assert col.struct_code() == "?"
        assert col.byte_size() == 1

    def test_text_default_size(self):
        col = Column("bio", ColumnType.TEXT)
        assert col.size == 255

    def test_to_from_dict(self):
        col = Column("name", ColumnType.TEXT, 50)
        d = col.to_dict()
        col2 = Column.from_dict(d)
        assert col2.name == "name"
        assert col2.col_type == ColumnType.TEXT
        assert col2.size == 50


class TestSchema:
    def test_record_size_matches_old_format(self):
        schema = make_users_schema()
        old_format = "<I20sI"
        assert schema.record_size == struct.calcsize(old_format)

    def test_format_string_matches_old(self):
        schema = make_users_schema()
        assert schema.format_string == "<I20sI"

    def test_column_names(self):
        schema = make_users_schema()
        assert schema.column_names() == ["id", "name", "age"]

    def test_pack_unpack_roundtrip(self):
        schema = make_users_schema()
        packed = schema.pack(1, {"name": "Alice", "age": 30})
        unpacked = schema.unpack(packed)
        assert unpacked == {"id": 1, "name": "Alice", "age": 30}

    def test_text_truncation(self):
        schema = make_users_schema()
        long_name = "A" * 100
        packed = schema.pack(1, {"name": long_name, "age": 25})
        unpacked = schema.unpack(packed)
        assert len(unpacked["name"]) == 20

    def test_text_padding(self):
        schema = make_users_schema()
        packed = schema.pack(1, {"name": "Hi", "age": 25})
        unpacked = schema.unpack(packed)
        assert unpacked["name"] == "Hi"
        assert len(packed) == schema.record_size

    def test_to_from_dict_roundtrip(self):
        schema = make_users_schema()
        d = schema.to_dict()
        schema2 = Schema.from_dict(d)
        assert schema2.table_name == "users"
        assert schema2.record_size == schema.record_size
        assert schema2.format_string == schema.format_string
        assert len(schema2.columns) == 2

    def test_all_types_schema(self):
        schema = Schema("mixed", [
            Column("label", ColumnType.TEXT, 10),
            Column("count", ColumnType.INTEGER),
            Column("score", ColumnType.FLOAT),
            Column("active", ColumnType.BOOLEAN),
        ])
        packed = schema.pack(42, {
            "label": "test",
            "count": 7,
            "score": 3.14,
            "active": True,
        })
        unpacked = schema.unpack(packed)
        assert unpacked["id"] == 42
        assert unpacked["label"] == "test"
        assert unpacked["count"] == 7
        assert abs(unpacked["score"] - 3.14) < 0.001
        assert unpacked["active"] is True


class TestValidation:
    def test_valid_values(self):
        schema = make_users_schema()
        ok, msg = schema.validate_values(["name", "age"], ["Alice", 30])
        assert ok is True
        assert msg == ""

    def test_missing_column(self):
        schema = make_users_schema()
        ok, msg = schema.validate_values(["name"], ["Alice"])
        assert ok is False
        assert "Missing required column" in msg

    def test_unknown_column(self):
        schema = make_users_schema()
        ok, msg = schema.validate_values(["name", "age", "foo"], ["Alice", 30, "bar"])
        assert ok is False
        assert "Unknown column" in msg

    def test_wrong_type_integer(self):
        schema = make_users_schema()
        ok, msg = schema.validate_values(["name", "age"], ["Alice", "thirty"])
        assert ok is False
        assert "expects INTEGER" in msg

    def test_wrong_type_text(self):
        schema = make_users_schema()
        ok, msg = schema.validate_values(["name", "age"], [123, 30])
        assert ok is False
        assert "expects TEXT" in msg

    def test_integer_out_of_range(self):
        schema = make_users_schema()
        ok, msg = schema.validate_values(["name", "age"], ["Alice", -1])
        assert ok is False
        assert "out of range" in msg
