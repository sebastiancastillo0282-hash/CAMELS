"""SQLite schema management for normalization artifacts."""
from __future__ import annotations

import sqlite3
from pathlib import Path


class NormalizationSchema:
    """Ensure the SQLite database contains the normalization tables."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def ensure(self) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS banks (
                    bank_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    country TEXT NOT NULL,
                    regulator TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS indicators (
                    indicator_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    pillar TEXT NOT NULL,
                    unit TEXT NOT NULL,
                    description TEXT,
                    min_value REAL,
                    max_value REAL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS indicator_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bank_id TEXT NOT NULL REFERENCES banks(bank_id),
                    indicator_id TEXT NOT NULL REFERENCES indicators(indicator_id),
                    period TEXT NOT NULL,
                    period_start TEXT,
                    period_end TEXT,
                    value REAL,
                    unit TEXT NOT NULL,
                    raw_value TEXT,
                    source_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    ingested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    metadata TEXT,
                    UNIQUE(bank_id, indicator_id, period, source_id, run_id)
                );

                CREATE TABLE IF NOT EXISTS normalization_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    bank_id TEXT NOT NULL,
                    indicator_id TEXT NOT NULL,
                    period TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_indicator_history_lookup
                    ON indicator_history (bank_id, indicator_id, period);

                CREATE INDEX IF NOT EXISTS idx_indicator_history_source
                    ON indicator_history (source_id, run_id);

                CREATE INDEX IF NOT EXISTS idx_normalization_log_run
                    ON normalization_log (run_id);
                """
            )


__all__ = ["NormalizationSchema"]
