from __future__ import annotations

"""Helpers to retrieve ingestion outputs for normalization."""

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Sequence

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class IngestionRecord:
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
    started_at: datetime
    completed_at: datetime
    metadata: Dict[str, object]

    @property
    def is_success(self) -> bool:
        return self.status.lower() == "success"


class IngestionRepository:
    """Read ingestion_log entries produced during phase 1."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def fetch(self, *, run_ids: Sequence[str] | None = None) -> List[IngestionRecord]:
        try:
            with sqlite3.connect(self.path) as connection:
                connection.row_factory = sqlite3.Row
                if run_ids:
                    placeholders = ",".join("?" for _ in run_ids)
                    query = f"SELECT * FROM ingestion_log WHERE run_id IN ({placeholders})"
                    rows = connection.execute(query, tuple(run_ids)).fetchall()
                else:
                    rows = connection.execute("SELECT * FROM ingestion_log").fetchall()
        except sqlite3.OperationalError as exc:
            logger.warning("Ingestion log not available yet: %s", exc)
            return []

        records: List[IngestionRecord] = []
        for row in rows:
            metadata_raw = row["metadata"] or "{}"
            try:
                metadata = json.loads(metadata_raw)
            except json.JSONDecodeError:
                metadata = {}
            record = IngestionRecord(
                run_id=row["run_id"],
                source_id=row["source_id"],
                bank=row["bank"],
                country=row["country"],
                regulator=row["regulator"],
                url=row["url"],
                format=row["format"],
                frequency=row["frequency"],
                local_path=row["local_path"],
                checksum=row["checksum"],
                record_count=row["record_count"],
                status=row["status"],
                started_at=datetime.fromisoformat(row["started_at"]),
                completed_at=datetime.fromisoformat(row["completed_at"]),
                metadata=metadata,
            )
            records.append(record)
        return records

    def latest_successful(self) -> Dict[str, IngestionRecord]:
        """Return the most recent successful record per source."""

        records = [record for record in self.fetch() if record.is_success]
        results: Dict[str, IngestionRecord] = {}
        for record in sorted(records, key=lambda item: item.completed_at):
            results[record.source_id] = record
        return results


__all__ = ["IngestionRecord", "IngestionRepository"]
