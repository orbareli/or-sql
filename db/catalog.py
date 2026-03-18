"""
catalog.py
----------
Manages the registry of tables and their schemas.

Persists table metadata to catalog.json in the database directory.
Each table gets its own .db, .idx, .meta, and .free files.
"""
from __future__ import annotations

import json
import os
from schema import Schema


class CatalogError(Exception):
    pass


class Catalog:
    def __init__(self, db_dir: str):
        self.db_dir = db_dir
        os.makedirs(db_dir, exist_ok=True)
        self._catalog_path = os.path.join(db_dir, "catalog.json")
        self._schemas = {}
        self._load()

    def _load(self):
        if os.path.exists(self._catalog_path):
            with open(self._catalog_path, "r") as f:
                data = json.load(f)
            for entry in data.get("tables", []):
                schema = Schema.from_dict(entry)
                self._schemas[schema.table_name] = schema

    def _save(self):
        data = {"tables": [s.to_dict() for s in self._schemas.values()]}
        with open(self._catalog_path, "w") as f:
            json.dump(data, f, indent=2)

    def create_table(self, schema: Schema) -> str:
        if schema.table_name in self._schemas:
            raise CatalogError(f"Table '{schema.table_name}' already exists.")
        self._schemas[schema.table_name] = schema
        self._save()
        return schema.table_name

    def drop_table(self, name: str):
        if name not in self._schemas:
            raise CatalogError(f"Table '{name}' does not exist.")
        del self._schemas[name]
        self._save()
        # Clean up data files
        base = self.db_path(name)
        for ext in ("", ".idx", ".meta", ".free"):
            path = base + ext
            if os.path.exists(path):
                os.remove(path)

    def get_schema(self, name: str) -> Schema:
        if name not in self._schemas:
            raise CatalogError(f"Table '{name}' does not exist.")
        return self._schemas[name]

    def table_exists(self, name: str) -> bool:
        return name in self._schemas

    def list_tables(self) -> list:
        return sorted(self._schemas.keys())

    def db_path(self, table_name: str) -> str:
        return os.path.join(self.db_dir, f"{table_name}.db")
