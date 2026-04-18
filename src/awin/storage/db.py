from __future__ import annotations

import sqlite3
from pathlib import Path

from awin.storage.schema import CREATE_TABLE_STATEMENTS, INDEX_STATEMENTS, MIGRATION_ADD_COLUMNS


class ManagedConnection(sqlite3.Connection):
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            return super().__exit__(exc_type, exc_val, exc_tb)
        finally:
            self.close()


def connect_sqlite(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path, factory=ManagedConnection)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _table_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]) for row in rows}


def _ensure_table_columns(connection: sqlite3.Connection, table_name: str) -> None:
    existing_columns = _table_columns(connection, table_name)
    for column_definition in MIGRATION_ADD_COLUMNS.get(table_name, []):
        column_name = column_definition.split()[0]
        if column_name in existing_columns:
            continue
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_definition}")


def init_db(db_path: Path) -> None:
    with connect_sqlite(db_path) as connection:
        for statement in CREATE_TABLE_STATEMENTS:
            connection.execute(statement)
        for table_name in MIGRATION_ADD_COLUMNS:
            _ensure_table_columns(connection, table_name)
        for statement in INDEX_STATEMENTS:
            connection.execute(statement)
        connection.commit()
