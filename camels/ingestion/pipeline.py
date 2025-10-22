"""Ingestion pipeline implementation."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List

from camels.core.stage import StageContext

from .catalog import CatalogError, load_catalog
from .download import DownloadError, download_source
from .parsers import ParsedDataset, parse_file
from .storage import IngestionLogEntry, IngestionStore

logger = logging.getLogger(__name__)


def _raw_directory(settings_dir: Path, timestamp: datetime) -> Path:
    return settings_dir / "raw" / timestamp.strftime("%Y%m%d")


def _summarize(dataset: ParsedDataset) -> dict:
    return {
        "rows": dataset.row_count,
        "metadata": dataset.metadata,
    }


def run_pipeline(context: StageContext) -> List[IngestionLogEntry]:
    """Execute the ingestion pipeline using the provided *context*."""

    context.settings.ensure_directories()
    try:
        sources = load_catalog()
    except CatalogError as exc:
        logger.error("Unable to load source catalog: %s", exc)
        raise

    run_id = context.run_id
    store = IngestionStore(context.settings.sqlite_path)
    raw_dir = _raw_directory(context.settings.data_dir, context.timestamp)
    raw_dir.mkdir(parents=True, exist_ok=True)
    results: List[IngestionLogEntry] = []

    logger.info("Loaded %d sources from catalog", len(sources))
    for source in sources:
        logger.info(
            "Processing source %s for bank %s (%s)",
            source.id,
            source.bank,
            source.country,
        )
        started = datetime.utcnow()
        try:
            download = download_source(source, raw_dir)
            parsed = parse_file(download.path, source)
            metadata = {
                "indicators": list(source.indicators),
                "content_type": download.content_type,
                "size_bytes": download.size_bytes,
                "parse_summary": _summarize(parsed),
            }
            entry = IngestionLogEntry(
                run_id=run_id,
                source_id=source.id,
                bank=source.bank,
                country=source.country,
                regulator=source.regulator,
                url=source.url,
                format=source.format,
                frequency=source.frequency,
                local_path=str(download.path),
                checksum=download.sha256,
                record_count=parsed.row_count,
                status="success",
                error=None,
                started_at=started,
                completed_at=datetime.utcnow(),
                metadata=metadata,
            )
        except (DownloadError, ValueError) as exc:
            logger.exception("Failed to process source %s: %s", source.id, exc)
            metadata = {
                "indicators": list(source.indicators),
            }
            entry = IngestionLogEntry(
                run_id=run_id,
                source_id=source.id,
                bank=source.bank,
                country=source.country,
                regulator=source.regulator,
                url=source.url,
                format=source.format,
                frequency=source.frequency,
                local_path="",
                checksum="",
                record_count=0,
                status="failed",
                error=str(exc),
                started_at=started,
                completed_at=datetime.utcnow(),
                metadata=metadata,
            )
        store.record(entry)
        results.append(entry)
        logger.info(
            "Recorded ingestion for %s with status %s", entry.source_id, entry.status
        )
    logger.info("Ingestion pipeline complete for run %s", run_id)
    logger.debug("Run summary: %s", json.dumps([entry.metadata for entry in results]))
    return results
