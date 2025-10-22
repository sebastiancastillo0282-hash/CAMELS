from __future__ import annotations

"""Persistence utilities for scoring outputs."""

import json
import sqlite3
from pathlib import Path
from typing import Iterable

from .models import CompositeScore, IndicatorScore, PillarScore


class ScoringStore:
    """Write scoring results to SQLite."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    bank_id TEXT NOT NULL REFERENCES banks(bank_id),
                    score REAL NOT NULL,
                    rating TEXT NOT NULL,
                    period TEXT,
                    calculated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    details TEXT
                );

                CREATE TABLE IF NOT EXISTS pillar_scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    bank_id TEXT NOT NULL REFERENCES banks(bank_id),
                    pillar TEXT NOT NULL,
                    score REAL NOT NULL,
                    rating TEXT NOT NULL,
                    weight REAL NOT NULL,
                    period TEXT,
                    calculated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    details TEXT
                );

                CREATE TABLE IF NOT EXISTS indicator_scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    bank_id TEXT NOT NULL REFERENCES banks(bank_id),
                    indicator_id TEXT NOT NULL,
                    pillar TEXT NOT NULL,
                    score REAL NOT NULL,
                    rating TEXT NOT NULL,
                    weight REAL NOT NULL,
                    value REAL,
                    period TEXT,
                    unit TEXT,
                    source_id TEXT,
                    normalization_run_id TEXT,
                    calculated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    details TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_scores_run ON scores(run_id);
                CREATE INDEX IF NOT EXISTS idx_pillar_scores_run ON pillar_scores(run_id);
                CREATE INDEX IF NOT EXISTS idx_indicator_scores_run ON indicator_scores(run_id);
                """
            )

    def persist(self, run_id: str, scores: Iterable[CompositeScore]) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("DELETE FROM scores WHERE run_id=?", (run_id,))
            connection.execute("DELETE FROM pillar_scores WHERE run_id=?", (run_id,))
            connection.execute("DELETE FROM indicator_scores WHERE run_id=?", (run_id,))

            for composite in scores:
                self._insert_composite(connection, run_id, composite)

    def _insert_composite(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        composite: CompositeScore,
    ) -> None:
        composite_details = json.dumps(composite.metadata or {}, ensure_ascii=False)
        connection.execute(
            """
            INSERT INTO scores (
                run_id, bank_id, score, rating, period, details
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                composite.bank_id,
                composite.score,
                composite.rating,
                composite.period,
                composite_details,
            ),
        )
        for pillar in composite.pillars:
            self._insert_pillar(connection, run_id, composite.bank_id, pillar)

    def _insert_pillar(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        bank_id: str,
        pillar: PillarScore,
    ) -> None:
        pillar_details = json.dumps(pillar.metadata or {}, ensure_ascii=False)
        connection.execute(
            """
            INSERT INTO pillar_scores (
                run_id, bank_id, pillar, score, rating, weight, period, details
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                bank_id,
                pillar.pillar,
                pillar.score,
                pillar.rating,
                pillar.weight,
                pillar.period,
                pillar_details,
            ),
        )
        for indicator in pillar.indicators:
            self._insert_indicator(connection, run_id, bank_id, pillar.pillar, indicator)

    def _insert_indicator(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        bank_id: str,
        pillar: str,
        indicator: IndicatorScore,
    ) -> None:
        indicator_details = json.dumps(indicator.metadata or {}, ensure_ascii=False)
        connection.execute(
            """
            INSERT INTO indicator_scores (
                run_id,
                bank_id,
                indicator_id,
                pillar,
                score,
                rating,
                weight,
                value,
                period,
                unit,
                source_id,
                normalization_run_id,
                details
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                bank_id,
                indicator.indicator_id,
                pillar,
                indicator.score,
                indicator.rating,
                indicator.weight,
                indicator.value,
                indicator.period,
                indicator.unit,
                indicator.source_id,
                indicator.normalization_run_id,
                indicator_details,
            ),
        )


__all__ = ["ScoringStore"]
