"""SQLite persistence for ingestion runs."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


@dataclass(slots=True)
class IngestionLogEntry:
    run_id: str
    source_id: str
    bank: str
    country: str
    regulator: str
    url: str
    format: str
    frequency: str
    local_path: str
    checksum: str
    record_count: int
    status: str
    error: str | None
    started_at: datetime
    completed_at: datetime
    metadata: Dict[str, Any]


class IngestionStore:
    """Utility wrapper around SQLite for ingestion logging."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS ingestion_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    bank TEXT NOT NULL,
                    country TEXT NOT NULL,
                    regulator TEXT NOT NULL,
                    url TEXT NOT NULL,
                    format TEXT NOT NULL,
                    frequency TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    record_count INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    metadata TEXT NOT NULL
                )
                """
            )

    def record(self, entry: IngestionLogEntry) -> None:
        payload = entry.metadata or {}
        metadata = json.dumps(payload, ensure_ascii=False)
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                """
                INSERT INTO ingestion_log (
                    run_id,
                    source_id,
                    bank,
                    country,
                    regulator,
                    url,
                    format,
                    frequency,
                    local_path,
                    checksum,
                    record_count,
                    status,
                    error,
                    started_at,
                    completed_at,
                    metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry.run_id,
                    entry.source_id,
                    entry.bank,
                    entry.country,
                    entry.regulator,
                    entry.url,
                    entry.format,
                    entry.frequency,
                    entry.local_path,
                    entry.checksum,
                    entry.record_count,
                    entry.status,
                    entry.error,
                    entry.started_at.isoformat(),
                    entry.completed_at.isoformat(),
                    metadata,
                ),
            )
