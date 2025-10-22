from __future__ import annotations

"""Data access helpers for the scoring engine."""

import json
import logging
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List

from .models import BankProfile, IndicatorSnapshot

logger = logging.getLogger(__name__)


class IndicatorRepository:
    """Read normalized indicator values for scoring."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def bank_profiles(self) -> List[BankProfile]:
        try:
            with sqlite3.connect(self.path) as connection:
                connection.row_factory = sqlite3.Row
                rows = connection.execute(
                    "SELECT bank_id, name, country, regulator FROM banks ORDER BY bank_id"
                ).fetchall()
        except sqlite3.OperationalError as exc:
            logger.warning("Bank registry not available for scoring: %s", exc)
            return []
        return [
            BankProfile(
                bank_id=row["bank_id"],
                name=row["name"],
                country=row["country"],
                regulator=row["regulator"],
            )
            for row in rows
        ]

    def latest_snapshots(self) -> Dict[str, Dict[str, IndicatorSnapshot]]:
        try:
            with sqlite3.connect(self.path) as connection:
                connection.row_factory = sqlite3.Row
                rows = connection.execute(
                    """
                    WITH latest_period AS (
                        SELECT bank_id, indicator_id, MAX(period) AS period
                        FROM indicator_history
                        GROUP BY bank_id, indicator_id
                    ),
                    latest_row AS (
                        SELECT ih.bank_id, ih.indicator_id, MAX(ih.id) AS row_id
                        FROM indicator_history ih
                        JOIN latest_period lp
                            ON ih.bank_id = lp.bank_id
                           AND ih.indicator_id = lp.indicator_id
                           AND ih.period = lp.period
                        GROUP BY ih.bank_id, ih.indicator_id
                    )
                    SELECT ih.bank_id,
                           ih.indicator_id,
                           ih.period,
                           ih.value,
                           ih.unit,
                           ih.source_id,
                           ih.run_id,
                           ih.metadata,
                           i.pillar
                    FROM indicator_history ih
                    JOIN latest_row lr ON ih.id = lr.row_id
                    JOIN indicators i ON i.indicator_id = ih.indicator_id
                    ORDER BY ih.bank_id, ih.indicator_id
                    """
                ).fetchall()
        except sqlite3.OperationalError as exc:
            logger.warning("Indicator history unavailable for scoring: %s", exc)
            return {}

        snapshots: Dict[str, Dict[str, IndicatorSnapshot]] = {}
        for row in rows:
            metadata_raw = row["metadata"] or "{}"
            try:
                metadata = json.loads(metadata_raw)
            except json.JSONDecodeError:
                metadata = {"raw": metadata_raw}
            snapshot = IndicatorSnapshot(
                bank_id=row["bank_id"],
                indicator_id=row["indicator_id"],
                pillar=row["pillar"],
                period=row["period"],
                value=row["value"],
                unit=row["unit"],
                source_id=row["source_id"],
                normalization_run_id=row["run_id"],
                metadata=metadata,
            )
            snapshots.setdefault(snapshot.bank_id, {})[snapshot.indicator_id] = snapshot
        return snapshots

    def periods_for_bank(self, bank_id: str) -> List[str]:
        try:
            with sqlite3.connect(self.path) as connection:
                rows = connection.execute(
                    "SELECT DISTINCT period FROM indicator_history WHERE bank_id=? ORDER BY period",
                    (bank_id,),
                ).fetchall()
        except sqlite3.OperationalError:
            return []
        return [row[0] for row in rows if row[0] is not None]

    def indicator_ids(self) -> Iterable[str]:
        try:
            with sqlite3.connect(self.path) as connection:
                rows = connection.execute("SELECT indicator_id FROM indicators").fetchall()
        except sqlite3.OperationalError:
            return []
        return [row[0] for row in rows]


__all__ = ["IndicatorRepository"]
