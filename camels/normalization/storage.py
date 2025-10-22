from __future__ import annotations

"""Persistence utilities for normalized indicators."""

import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from .transformers import NormalizedRecord

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class NormalizationSummary:
    inserted: int
    updated: int


class NormalizedStore:
    """Store normalized records and validation metadata in SQLite."""

    KEY_FIELDS = (
        "bank_id",
        "indicator_id",
        "period",
        "source_id",
        "run_id",
    )

    def __init__(self, path: Path) -> None:
        self.path = path

    def _key_tuple(self, record: NormalizedRecord) -> Tuple[str, str, str, str, str]:
        return (
            record.bank_id,
            record.indicator_id,
            record.period,
            record.source_id,
            record.run_id,
        )

    def upsert(self, records: Iterable[NormalizedRecord]) -> NormalizationSummary:
        inserted = 0
        updated = 0
        items = list(records)
        if not items:
            return NormalizationSummary(inserted=0, updated=0)
        with sqlite3.connect(self.path) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            for record in items:
                metadata = json.dumps(record.metadata, ensure_ascii=False)
                key = self._key_tuple(record)
                existing = connection.execute(
                    """
                    SELECT 1 FROM indicator_history
                    WHERE bank_id=? AND indicator_id=? AND period=? AND source_id=? AND run_id=?
                    """,
                    key,
                ).fetchone()
                if existing:
                    logger.warning(
                        "Duplicate record detected for %s/%s %s from source %s; updating existing entry.",
                        record.bank_id,
                        record.indicator_id,
                        record.period,
                        record.source_id,
                    )
                    connection.execute(
                        """
                        UPDATE indicator_history
                        SET
                            period_start=?,
                            period_end=?,
                            value=?,
                            unit=?,
                            raw_value=?,
                            metadata=?,
                            ingested_at=CURRENT_TIMESTAMP
                        WHERE bank_id=? AND indicator_id=? AND period=? AND source_id=? AND run_id=?
                        """,
                        (
                            record.period_start,
                            record.period_end,
                            record.value,
                            record.unit,
                            record.raw_value,
                            metadata,
                            *key,
                        ),
                    )
                    updated += 1
                else:
                    connection.execute(
                        """
                        INSERT INTO indicator_history (
                            bank_id,
                            indicator_id,
                            period,
                            period_start,
                            period_end,
                            value,
                            unit,
                            raw_value,
                            source_id,
                            run_id,
                            metadata
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            record.bank_id,
                            record.indicator_id,
                            record.period,
                            record.period_start,
                            record.period_end,
                            record.value,
                            record.unit,
                            record.raw_value,
                            record.source_id,
                            record.run_id,
                            metadata,
                        ),
                    )
                    inserted += 1
        return NormalizationSummary(inserted=inserted, updated=updated)

    def log_event(
        self,
        *,
        run_id: str,
        source_id: str,
        bank_id: str,
        indicator_id: str,
        period: str,
        status: str,
        message: str | None = None,
    ) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                """
                INSERT INTO normalization_log (
                    run_id,
                    source_id,
                    bank_id,
                    indicator_id,
                    period,
                    status,
                    message
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, source_id, bank_id, indicator_id, period, status, message),
            )

    def coverage(self) -> List[Dict[str, object]]:
        with sqlite3.connect(self.path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                SELECT bank_id, indicator_id, COUNT(DISTINCT period) AS periods
                FROM indicator_history
                GROUP BY bank_id, indicator_id
                """
            ).fetchall()
        return [dict(row) for row in rows]


__all__ = ["NormalizationSummary", "NormalizedStore"]
