"""Audit trail persistence helpers."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


@dataclass(slots=True)
class AuditRecord:
    """Dictionary-style representation of a row in ``audit_trail``."""

    run_id: str
    stage: str
    bank_id: Optional[str]
    pillar: Optional[str]
    indicator_id: Optional[str]
    source_id: Optional[str]
    period: Optional[str]
    artifact_path: Optional[str]
    url: Optional[str]
    checksum: Optional[str]
    rating: Optional[str]
    status: Optional[str]
    ingestion_run_id: Optional[str]
    normalization_run_id: Optional[str]
    recorded_at: str
    metadata: Dict[str, object]


@dataclass(slots=True)
class ExportedAudit:
    """Summary of generated audit artifacts."""

    records: int
    files: List[Path]


class AuditStore:
    """Read and write audit trail data stored in SQLite."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._ensure_schema()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def prepare_stage(self, run_id: str, stage: str) -> None:
        """Remove previous audit entries for *run_id* and *stage*."""

        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "DELETE FROM audit_trail WHERE run_id=? AND stage=?",
                (run_id, stage),
            )

    def record_ingestions(
        self,
        run_id: str,
        entries: Sequence["IngestionLogEntry"],
        *,
        pipeline_version: str,
        command: str,
        workspace: Path,
    ) -> int:
        """Persist audit entries for ingestion log *entries*."""

        if not entries:
            return 0
        with sqlite3.connect(self.path) as connection:
            payloads = [
                self._build_ingestion_payload(
                    entry,
                    pipeline_version=pipeline_version,
                    command=command,
                    workspace=workspace,
                )
                for entry in entries
            ]
            self._bulk_insert(connection, payloads)
        return len(entries)

    def record_scores(
        self,
        run_id: str,
        scores: Iterable["CompositeScore"],
        *,
        pipeline_version: str,
    ) -> int:
        """Persist audit entries for scored indicators."""

        items = list(scores)
        if not items:
            return 0

        with sqlite3.connect(self.path) as connection:
            connection.row_factory = sqlite3.Row
            payloads = self._build_score_payloads(
                connection,
                run_id,
                items,
                pipeline_version=pipeline_version,
            )
            self._bulk_insert(connection, payloads)
        return len(payloads)

    def records(
        self,
        *,
        run_id: str | None = None,
        stage: str | None = None,
    ) -> List[AuditRecord]:
        """Return audit records filtered by *run_id* and/or *stage*."""

        query = [
            "SELECT run_id, stage, bank_id, pillar, indicator_id, source_id,",
            "       period, artifact_path, url, checksum, rating, status,",
            "       ingestion_run_id, normalization_run_id, recorded_at, metadata",
            "  FROM audit_trail",
        ]
        clauses: List[str] = []
        params: List[str] = []
        if run_id:
            clauses.append("run_id=?")
            params.append(run_id)
        if stage:
            clauses.append("stage=?")
            params.append(stage)
        if clauses:
            query.append(" WHERE " + " AND ".join(clauses))
        query.append(" ORDER BY recorded_at")

        with sqlite3.connect(self.path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute("\n".join(query), tuple(params)).fetchall()

        results: List[AuditRecord] = []
        for row in rows:
            metadata_raw = row["metadata"] or "{}"
            try:
                metadata = json.loads(metadata_raw)
            except json.JSONDecodeError:
                metadata = {"raw": metadata_raw}
            results.append(
                AuditRecord(
                    run_id=row["run_id"],
                    stage=row["stage"],
                    bank_id=row["bank_id"],
                    pillar=row["pillar"],
                    indicator_id=row["indicator_id"],
                    source_id=row["source_id"],
                    period=row["period"],
                    artifact_path=row["artifact_path"],
                    url=row["url"],
                    checksum=row["checksum"],
                    rating=row["rating"],
                    status=row["status"],
                    ingestion_run_id=row["ingestion_run_id"],
                    normalization_run_id=row["normalization_run_id"],
                    recorded_at=row["recorded_at"],
                    metadata=metadata,
                )
            )
        return results

    def export_records(
        self,
        *,
        run_id: str,
        output_dir: Path,
    ) -> ExportedAudit:
        """Persist the audit records for *run_id* into JSON/CSV artifacts."""

        records = self.records(run_id=run_id)
        if not records:
            return ExportedAudit(records=0, files=[])

        output_dir.mkdir(parents=True, exist_ok=True)
        json_path = output_dir / f"audit_trail_{run_id}.json"
        csv_path = output_dir / f"audit_trail_{run_id}.csv"

        with json_path.open("w", encoding="utf-8") as handle:
            json.dump(
                [self._record_to_dict(record) for record in records],
                handle,
                indent=2,
                ensure_ascii=False,
            )

        self._write_csv(csv_path, records)

        return ExportedAudit(records=len(records), files=[json_path, csv_path])

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_schema(self) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS audit_trail (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    bank_id TEXT,
                    pillar TEXT,
                    indicator_id TEXT,
                    source_id TEXT,
                    period TEXT,
                    artifact_path TEXT,
                    url TEXT,
                    checksum TEXT,
                    rating TEXT,
                    status TEXT,
                    ingestion_run_id TEXT,
                    normalization_run_id TEXT,
                    recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    metadata TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_audit_trail_run
                    ON audit_trail(run_id, stage);

                CREATE INDEX IF NOT EXISTS idx_audit_trail_source
                    ON audit_trail(source_id, ingestion_run_id);
                """
            )

    def _bulk_insert(
        self,
        connection: sqlite3.Connection,
        payloads: Sequence[Mapping[str, object]],
    ) -> None:
        if not payloads:
            return
        connection.executemany(
            """
            INSERT INTO audit_trail (
                run_id,
                stage,
                bank_id,
                pillar,
                indicator_id,
                source_id,
                period,
                artifact_path,
                url,
                checksum,
                rating,
                status,
                ingestion_run_id,
                normalization_run_id,
                metadata
            ) VALUES (
                :run_id,
                :stage,
                :bank_id,
                :pillar,
                :indicator_id,
                :source_id,
                :period,
                :artifact_path,
                :url,
                :checksum,
                :rating,
                :status,
                :ingestion_run_id,
                :normalization_run_id,
                :metadata
            )
            """,
            tuple(payloads),
        )

    def _build_ingestion_payload(
        self,
        entry: "IngestionLogEntry",
        *,
        pipeline_version: str,
        command: str,
        workspace: Path,
    ) -> Dict[str, object]:
        metadata = {
            "bank": entry.bank,
            "country": entry.country,
            "regulator": entry.regulator,
            "frequency": entry.frequency,
            "format": entry.format,
            "record_count": entry.record_count,
            "started_at": entry.started_at.isoformat(),
            "completed_at": entry.completed_at.isoformat(),
            "command": command,
            "workspace": str(workspace),
            "pipeline_version": pipeline_version,
            "ingestion_metadata": entry.metadata,
        }
        return {
            "run_id": entry.run_id,
            "stage": "ingest",
            "bank_id": None,
            "pillar": None,
            "indicator_id": None,
            "source_id": entry.source_id,
            "period": None,
            "artifact_path": entry.local_path or None,
            "url": entry.url,
            "checksum": entry.checksum or None,
            "rating": entry.status,
            "status": entry.status,
            "ingestion_run_id": entry.run_id,
            "normalization_run_id": None,
            "metadata": json.dumps(metadata, ensure_ascii=False),
        }

    def _build_score_payloads(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        scores: Sequence["CompositeScore"],
        *,
        pipeline_version: str,
    ) -> List[Mapping[str, object]]:
        ingestion_lookup, latest_by_source = self._load_ingestions(
            connection,
            self._sources_from_scores(scores),
        )
        bank_lookup = self._load_banks(connection)
        payloads: List[Mapping[str, object]] = []

        for composite in scores:
            bank_info = bank_lookup.get(composite.bank_id, {})
            for pillar in composite.pillars:
                for indicator in pillar.indicators:
                    source_meta = {}
                    if indicator.metadata:
                        source_meta = indicator.metadata.get("source_metadata", {}) or {}
                    ingestion_run_id = source_meta.get("source_run")
                    key = (indicator.source_id, ingestion_run_id)
                    ingestion_info = ingestion_lookup.get(key)
                    if ingestion_info is None and indicator.source_id:
                        ingestion_info = latest_by_source.get(indicator.source_id)

                    checksum = None
                    if source_meta.get("checksum"):
                        checksum = str(source_meta["checksum"])
                    if ingestion_info and ingestion_info.get("checksum"):
                        checksum = ingestion_info["checksum"]

                    payload_metadata = {
                        "pipeline_version": pipeline_version,
                        "composite_score": composite.score,
                        "composite_rating": composite.rating,
                        "pillar_score": pillar.score,
                        "pillar_rating": pillar.rating,
                        "indicator_value": indicator.value,
                        "indicator_unit": indicator.unit,
                        "indicator_weight": indicator.weight,
                        "bank_name": bank_info.get("name"),
                        "country": bank_info.get("country"),
                        "regulator": bank_info.get("regulator"),
                        "indicator_metadata": indicator.metadata,
                        "ingestion": ingestion_info,
                        "ingestion_run_id": ingestion_run_id,
                    }

                    payloads.append(
                        {
                            "run_id": run_id,
                            "stage": "score",
                            "bank_id": composite.bank_id,
                            "pillar": pillar.pillar,
                            "indicator_id": indicator.indicator_id,
                            "source_id": indicator.source_id,
                            "period": indicator.period,
                            "artifact_path": (ingestion_info or {}).get("local_path"),
                            "url": (ingestion_info or {}).get("url"),
                            "checksum": checksum,
                            "rating": indicator.rating,
                            "status": "scored",
                            "ingestion_run_id": ingestion_run_id,
                            "normalization_run_id": indicator.normalization_run_id,
                            "metadata": json.dumps(payload_metadata, ensure_ascii=False),
                        }
                    )
        return payloads

    def _load_ingestions(
        self,
        connection: sqlite3.Connection,
        sources: Sequence[str],
    ) -> Tuple[Dict[Tuple[str, str], Dict[str, object]], Dict[str, Dict[str, object]]]:
        if not sources:
            return {}, {}
        unique_sources = sorted({source for source in sources if source})
        if not unique_sources:
            return {}, {}
        placeholders = ",".join("?" for _ in unique_sources)
        rows = connection.execute(
            f"""
            SELECT run_id, source_id, url, local_path, checksum, status, completed_at
              FROM ingestion_log
             WHERE source_id IN ({placeholders})
            """,
            tuple(unique_sources),
        ).fetchall()
        lookup: Dict[Tuple[str, str], Dict[str, object]] = {}
        latest: Dict[str, Dict[str, object]] = {}
        for row in rows:
            info = {
                "run_id": row["run_id"],
                "source_id": row["source_id"],
                "url": row["url"],
                "local_path": row["local_path"],
                "checksum": row["checksum"],
                "status": row["status"],
                "completed_at": row["completed_at"],
            }
            lookup[(row["source_id"], row["run_id"])] = info
            latest_entry = latest.get(row["source_id"])
            if (
                latest_entry is None
                or str(row["completed_at"]) > str(latest_entry.get("completed_at"))
            ):
                latest[row["source_id"]] = info
        return lookup, latest

    def _load_banks(
        self, connection: sqlite3.Connection
    ) -> Dict[str, Dict[str, object]]:
        rows = connection.execute(
            "SELECT bank_id, name, country, regulator FROM banks"
        ).fetchall()
        return {
            row["bank_id"]: {
                "name": row["name"],
                "country": row["country"],
                "regulator": row["regulator"],
            }
            for row in rows
        }

    def _sources_from_scores(
        self, scores: Sequence["CompositeScore"]
    ) -> List[str]:
        sources: List[str] = []
        for composite in scores:
            for pillar in composite.pillars:
                for indicator in pillar.indicators:
                    if indicator.source_id:
                        sources.append(indicator.source_id)
        return sources

    def _record_to_dict(self, record: AuditRecord) -> Dict[str, object]:
        return {
            "run_id": record.run_id,
            "stage": record.stage,
            "bank_id": record.bank_id,
            "pillar": record.pillar,
            "indicator_id": record.indicator_id,
            "source_id": record.source_id,
            "period": record.period,
            "artifact_path": record.artifact_path,
            "url": record.url,
            "checksum": record.checksum,
            "rating": record.rating,
            "status": record.status,
            "ingestion_run_id": record.ingestion_run_id,
            "normalization_run_id": record.normalization_run_id,
            "recorded_at": record.recorded_at,
            "metadata": record.metadata,
        }

    def _write_csv(self, path: Path, records: Sequence[AuditRecord]) -> None:
        import csv

        headers = [
            "run_id",
            "stage",
            "bank_id",
            "pillar",
            "indicator_id",
            "source_id",
            "period",
            "artifact_path",
            "url",
            "checksum",
            "rating",
            "status",
            "ingestion_run_id",
            "normalization_run_id",
            "recorded_at",
            "metadata",
        ]
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            for record in records:
                writer.writerow(
                    {
                        "run_id": record.run_id,
                        "stage": record.stage,
                        "bank_id": record.bank_id,
                        "pillar": record.pillar,
                        "indicator_id": record.indicator_id,
                        "source_id": record.source_id,
                        "period": record.period,
                        "artifact_path": record.artifact_path,
                        "url": record.url,
                        "checksum": record.checksum,
                        "rating": record.rating,
                        "status": record.status,
                        "ingestion_run_id": record.ingestion_run_id,
                        "normalization_run_id": record.normalization_run_id,
                        "recorded_at": record.recorded_at,
                        "metadata": json.dumps(record.metadata, ensure_ascii=False),
                    }
                )


__all__ = ["AuditRecord", "AuditStore", "ExportedAudit"]


# Type checking imports -------------------------------------------------------
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - used for type checking only
    from camels.ingestion.storage import IngestionLogEntry
    from camels.scoring.models import CompositeScore
