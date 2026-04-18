from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from awin.adapters.contracts import SnapshotSourceBundle, SourceHealth
from awin.config import DbConfig


@dataclass(frozen=True)
class SnapshotRequest:
    trade_date: str
    snapshot_time: str
    analysis_snapshot_ts: str


class ReadOnlyAdapter:
    source_name = "base"

    def health(self) -> SourceHealth:
        return SourceHealth(source_name=self.source_name, source_status="missing")


class SnapshotBundleAdapter(ReadOnlyAdapter):
    def load_snapshot_bundle(self, request: SnapshotRequest) -> SnapshotSourceBundle:
        return SnapshotSourceBundle(source_health=[self.health()])


class FileBackedAdapter(ReadOnlyAdapter):
    def __init__(self, root: Path) -> None:
        self.root = root


class DbBackedAdapter(ReadOnlyAdapter):
    def __init__(self, db_config: DbConfig, dsn_label: str | None = None) -> None:
        self.db_config = db_config
        self.dsn_label = dsn_label

    def _connection_params(self) -> dict[str, Any] | None:
        try:
            import psycopg  # noqa: F401
        except ImportError as exc:
            raise RuntimeError("psycopg driver is not installed") from exc
        return {
            "host": self.db_config.host,
            "port": self.db_config.port,
            "dbname": self.db_config.dbname,
            "user": self.db_config.user,
            "password": self.db_config.password,
        }

    def _connect(self):
        params = self._connection_params()
        import psycopg  # type: ignore

        return psycopg.connect(**params)

    def _connect_with_error(self):
        try:
            connection = self._connect()
            return connection, None
        except Exception as exc:  # pragma: no cover - exercised through caller behavior
            return None, str(exc)

    def _query_rows(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]] | None:
        connection, _ = self._connect_with_error()
        if connection is None:
            return None
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                result = cursor.fetchall()
                columns = [desc.name for desc in cursor.description]
        return [dict(zip(columns, row)) for row in result]
